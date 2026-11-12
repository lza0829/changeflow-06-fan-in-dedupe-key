from app.db import connect

LEDGER_DDL = """
        CREATE TABLE IF NOT EXISTS change_ledger(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER,
            batch_id INTEGER,
            entry_id INTEGER NOT NULL DEFAULT 0,
            seq INTEGER NOT NULL,
            merge_seq INTEGER,
            apply_seq INTEGER,
            op TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            disposition TEXT NOT NULL,
            UNIQUE(stream_id, seq, run_id, entry_id)
        );
"""

SCHEMA = """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS streams(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            fixture_path TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            apply_cursor INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS apply_runs(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            loaded_count INTEGER NOT NULL,
            applied_count INTEGER NOT NULL,
            quarantined_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            cursor_before INTEGER NOT NULL,
            cursor_after INTEGER NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sink_records(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            last_seq INTEGER NOT NULL,
            last_event_ts TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(stream_id, entity_key)
        );
        CREATE TABLE IF NOT EXISTS quarantine(
            id INTEGER PRIMARY KEY,
            stream_id INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            batch_id INTEGER,
            candidate_id INTEGER,
            seq INTEGER NOT NULL,
            entity_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS applied_seqs(
            stream_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            run_id INTEGER NOT NULL,
            applied_at TEXT NOT NULL,
            PRIMARY KEY(stream_id, seq)
        );
        CREATE TABLE IF NOT EXISTS change_entries(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            fixture_path TEXT NOT NULL,
            stream_id INTEGER NOT NULL,
            pull_cursor INTEGER NOT NULL DEFAULT 0,
            consumed_cursor INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS fan_in_batches(
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            target_stream_id INTEGER NOT NULL,
            entry_ids_json TEXT NOT NULL,
            dedupe_rule TEXT NOT NULL,
            max_per_entry INTEGER NOT NULL DEFAULT 0,
            candidate_total INTEGER NOT NULL DEFAULT 0,
            retained_count INTEGER NOT NULL DEFAULT 0,
            dropped_count INTEGER NOT NULL DEFAULT 0,
            applied_count INTEGER NOT NULL DEFAULT 0,
            quarantined_count INTEGER NOT NULL DEFAULT 0,
            blocked_count INTEGER NOT NULL DEFAULT 0,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS fan_in_candidates(
            id INTEGER PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            entry_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            op TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT NOT NULL,
            merge_seq INTEGER,
            apply_seq INTEGER,
            dedupe_status TEXT NOT NULL,
            dedupe_reason TEXT,
            retained_candidate_id INTEGER,
            apply_status TEXT NOT NULL DEFAULT 'pending',
            ledger_id INTEGER,
            quarantine_id INTEGER,
            replayed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(batch_id, entry_id, seq)
        );
        CREATE INDEX IF NOT EXISTS idx_fan_in_candidates_batch
            ON fan_in_candidates(batch_id, entry_id, seq);
"""+LEDGER_DDL

CHANNEL_STREAM_NAME = "Partner Merge CDC"
CHANNEL_NOTE = "汇入通道（虚拟）：多入口汇入后共享本通道 Sink / 账本；请从「汇入去重」页触发"


def _ensure_column(conn, table: str, column: str, decl: str) -> None:
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def _migrate_ledger(conn) -> None:
    """老库的账本没有批次/入口归属列：重建表以保留原有行并扩 uniqueness。

    普通流的账本行 entry_id=0，原唯一约束语义不变；汇入批次的同一入口 seq
    可与其他入口重复，因此唯一键加入 entry_id。
    """
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(change_ledger)").fetchall()]
    if "entry_id" in cols:
        return
    conn.execute("ALTER TABLE change_ledger RENAME TO change_ledger_legacy")
    conn.execute(LEDGER_DDL)
    conn.execute(
        """
        INSERT INTO change_ledger(
            id, stream_id, run_id, batch_id, entry_id, seq, merge_seq, apply_seq,
            op, entity_key, event_ts, payload_json, disposition
        )
        SELECT id, stream_id, run_id, NULL, 0, seq, NULL, NULL,
               op, entity_key, event_ts, payload_json, disposition
        FROM change_ledger_legacy
        """
    )
    conn.execute("DROP TABLE change_ledger_legacy")


def _ensure_channel_stream(conn) -> int:
    row = conn.execute("SELECT id FROM streams WHERE name=?", (CHANNEL_STREAM_NAME,)).fetchone()
    if row:
        return int(row["id"])
    cur = conn.execute(
        """
        INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
        VALUES (?,?,?,?,?,?)
        """,
        (CHANNEL_STREAM_NAME, "", "order", 0, 0, CHANNEL_NOTE),
    )
    return int(cur.lastrowid)


def _ensure_fan_in_entries(conn, stream_id: int) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO change_entries(
            name, fixture_path, stream_id, pull_cursor, consumed_cursor, enabled, note
        ) VALUES (?,?,?,0,0,1,?)
        """,
        [
            (
                "Partner A 分片",
                "partner_a_cdc.jsonl",
                stream_id,
                "本机夹具入口 A：ORD-3001(revision 2) / ORD-3002(revision 2) / ORD-3003(旧 event_ts)",
            ),
            (
                "Partner B 分片",
                "partner_b_cdc.jsonl",
                stream_id,
                "本机夹具入口 B：与 A 同键；seq 4 源侧缺口，保留条 seq 5 将在链路阶段隔离",
            ),
        ],
    )


def init_db():
    conn = connect()
    conn.executescript(SCHEMA)
    _migrate_ledger(conn)
    _ensure_column(conn, "quarantine", "batch_id", "INTEGER")
    _ensure_column(conn, "quarantine", "candidate_id", "INTEGER")
    _ensure_column(conn, "fan_in_candidates", "replayed", "INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_change_ledger_batch ON change_ledger(batch_id)"
    )

    if conn.execute("SELECT COUNT(*) c FROM streams").fetchone()["c"] == 0:
        conn.executemany(
            """
            INSERT INTO streams(name, fixture_path, entity_type, apply_cursor, enabled, note)
            VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    "Orders CDC",
                    "orders_cdc.jsonl",
                    "order",
                    0,
                    1,
                    "clean：seq 1→5 连续；含一次 delete",
                ),
                (
                    "Inventory CDC",
                    "inventory_cdc.jsonl",
                    "inventory",
                    0,
                    1,
                    "clean：seq 1→3 连续库存变更",
                ),
            ],
        )
    conn.execute(
        "INSERT OR IGNORE INTO settings(key, value) VALUES ('project_label', 'Changeflow')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO settings(key, value) VALUES ('gap_policy', 'stop_quarantine')"
    )

    target_stream_id = _ensure_channel_stream(conn)
    _ensure_fan_in_entries(conn, target_stream_id)
    conn.commit()
    conn.close()
