from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------- entries

def list_entries(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, fixture_path, stream_id, pull_cursor, consumed_cursor, enabled, note
        FROM change_entries ORDER BY id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def get_entry(conn: sqlite3.Connection, entry_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, name, fixture_path, stream_id, pull_cursor, consumed_cursor, enabled, note
        FROM change_entries WHERE id=?
        """,
        (entry_id,),
    ).fetchone()
    return dict(row) if row else None


def set_entry_cursors(
    conn: sqlite3.Connection,
    entry_id: int,
    *,
    pull_cursor: int,
    consumed_cursor: int,
) -> None:
    conn.execute(
        "UPDATE change_entries SET pull_cursor=?, consumed_cursor=? WHERE id=?",
        (pull_cursor, consumed_cursor, entry_id),
    )


def reset_entry_cursors(conn: sqlite3.Connection, entry_ids: list[int] | None = None) -> None:
    if entry_ids:
        conn.executemany(
            "UPDATE change_entries SET pull_cursor=0, consumed_cursor=0 WHERE id=?",
            [(i,) for i in entry_ids],
        )
    else:
        conn.execute("UPDATE change_entries SET pull_cursor=0, consumed_cursor=0")


# ---------------------------------------------------------------- batches

def create_batch(
    conn: sqlite3.Connection,
    *,
    target_stream_id: int,
    entry_ids: list[int],
    dedupe_rule: str,
    max_per_entry: int = 0,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO fan_in_batches(
            status, target_stream_id, entry_ids_json, dedupe_rule, max_per_entry,
            report_json, created_at
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (
            "pulling",
            target_stream_id,
            json.dumps(entry_ids),
            dedupe_rule,
            max_per_entry,
            "{}",
            _now(),
        ),
    )
    return int(cur.lastrowid)


def update_batch(
    conn: sqlite3.Connection,
    batch_id: int,
    *,
    status: str | None = None,
    candidate_total: int | None = None,
    retained: int | None = None,
    dropped: int | None = None,
    applied: int | None = None,
    quarantined: int | None = None,
    blocked: int | None = None,
    report: dict[str, Any] | None = None,
    finished: bool = False,
) -> None:
    fields: list[tuple[str, Any]] = []
    if status is not None:
        fields.append(("status", status))
    for name, value in (
        ("candidate_total", candidate_total),
        ("retained_count", retained),
        ("dropped_count", dropped),
        ("applied_count", applied),
        ("quarantined_count", quarantined),
        ("blocked_count", blocked),
    ):
        if value is not None:
            fields.append((name, value))
    if report is not None:
        fields.append(("report_json", json.dumps(report, ensure_ascii=False)))
    if finished:
        fields.append(("finished_at", _now()))
    if not fields:
        return
    sets = ", ".join(f"{name}=?" for name, _ in fields)
    conn.execute(
        f"UPDATE fan_in_batches SET {sets} WHERE id=?",
        [value for _, value in fields] + [batch_id],
    )


def _batch_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    item["entry_ids"] = json.loads(item.pop("entry_ids_json") or "[]")
    item["report"] = json.loads(item.pop("report_json") or "{}")
    return item


def list_batches(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, status, target_stream_id, entry_ids_json, dedupe_rule, max_per_entry,
               candidate_total, retained_count, dropped_count, applied_count,
               quarantined_count, blocked_count, report_json, created_at, finished_at
        FROM fan_in_batches ORDER BY id DESC LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_batch_dict(r) for r in rows]


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT id, status, target_stream_id, entry_ids_json, dedupe_rule, max_per_entry,
               candidate_total, retained_count, dropped_count, applied_count,
               quarantined_count, blocked_count, report_json, created_at, finished_at
        FROM fan_in_batches WHERE id=?
        """,
        (batch_id,),
    ).fetchone()
    return _batch_dict(row) if row else None


# ---------------------------------------------------------------- candidates

CANDIDATE_FIELDS = (
    "id, batch_id, entry_id, seq, op, entity_key, event_ts, revision, payload_json, "
    "merge_seq, apply_seq, dedupe_status, dedupe_reason, retained_candidate_id, "
    "apply_status, ledger_id, quarantine_id, replayed"
)


def insert_candidate(conn: sqlite3.Connection, *, batch_id: int, candidate: dict[str, Any]) -> int:
    cur = conn.execute(
        """
        INSERT INTO fan_in_candidates(
            batch_id, entry_id, seq, op, entity_key, event_ts, revision, payload_json,
            merge_seq, dedupe_status, dedupe_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            batch_id,
            int(candidate["entry_id"]),
            int(candidate["seq"]),
            candidate["op"],
            candidate["entity_key"],
            candidate["event_ts"],
            int(candidate.get("revision") or 0),
            json.dumps(candidate.get("payload") or {}, ensure_ascii=False),
            int(candidate.get("merge_seq") or 0),
            candidate["dedupe_status"],
            candidate.get("dedupe_reason"),
        ),
    )
    return int(cur.lastrowid)


def set_candidate_dedupe(
    conn: sqlite3.Connection,
    candidate_id: int,
    *,
    status: str,
    reason: str | None,
    retained_candidate_id: int | None,
) -> None:
    conn.execute(
        """
        UPDATE fan_in_candidates
        SET dedupe_status=?, dedupe_reason=?, retained_candidate_id=?
        WHERE id=?
        """,
        (status, reason, retained_candidate_id, candidate_id),
    )


def set_candidate_apply(
    conn: sqlite3.Connection,
    candidate_id: int,
    *,
    status: str,
    ledger_id: int | None = None,
    quarantine_id: int | None = None,
) -> None:
    conn.execute(
        """
        UPDATE fan_in_candidates
        SET apply_status=?, ledger_id=COALESCE(?, ledger_id),
            quarantine_id=COALESCE(?, quarantine_id)
        WHERE id=?
        """,
        (
            status,
            ledger_id,
            quarantine_id,
            candidate_id,
        ),
    )


def set_candidate_apply_seq(conn: sqlite3.Connection, candidate_id: int, apply_seq: int) -> None:
    conn.execute("UPDATE fan_in_candidates SET apply_seq=? WHERE id=?", (apply_seq, candidate_id))


def find_applied_by_source(
    conn: sqlite3.Connection, *, stream_id: int, entry_id: int, seq: int,
    entity_key: str, event_ts: str,
) -> dict[str, Any] | None:
    """共享通道上已应用过的同源事件（跨批次）。"""
    row = conn.execute(
        """
        SELECT id, apply_seq FROM change_ledger
        WHERE stream_id=? AND entry_id=? AND seq=? AND entity_key=? AND event_ts=?
          AND disposition='applied'
        ORDER BY id LIMIT 1
        """,
        (stream_id, entry_id, seq, entity_key, event_ts),
    ).fetchone()
    return dict(row) if row else None


def find_quarantined_by_source(
    conn: sqlite3.Connection, *, stream_id: int, entry_id: int, seq: int, entity_key: str
) -> dict[str, Any] | None:
    """共享通道上已隔离过的同源事件（跨批次，经 candidate_id 关联入口）。"""
    row = conn.execute(
        """
        SELECT q.id AS quarantine_id, (
            SELECT l.id FROM change_ledger l
            WHERE l.batch_id = q.batch_id AND l.entry_id = c.entry_id AND l.seq = q.seq
              AND l.entity_key = q.entity_key AND l.disposition = 'quarantine_gap'
            ORDER BY l.id LIMIT 1
        ) AS ledger_id
        FROM quarantine q
        JOIN fan_in_candidates c ON c.id=q.candidate_id
        WHERE q.stream_id=? AND c.entry_id=? AND q.seq=? AND q.entity_key=?
        ORDER BY q.id LIMIT 1
        """,
        (stream_id, entry_id, seq, entity_key),
    ).fetchone()
    return dict(row) if row else None


def set_candidate_replay(
    conn: sqlite3.Connection,
    candidate_id: int,
    *,
    status: str,
    ledger_id: int | None = None,
    quarantine_id: int | None = None,
    apply_seq: int | None = None,
) -> None:
    conn.execute(
        """
        UPDATE fan_in_candidates
        SET apply_status=?, replayed=1,
            ledger_id=COALESCE(?, ledger_id),
            quarantine_id=COALESCE(?, quarantine_id),
            apply_seq=COALESCE(?, apply_seq)
        WHERE id=?
        """,
        (status, ledger_id, quarantine_id, apply_seq, candidate_id),
    )


def set_candidate_ledger(conn: sqlite3.Connection, candidate_id: int, ledger_id: int) -> None:
    conn.execute("UPDATE fan_in_candidates SET ledger_id=? WHERE id=?", (ledger_id, candidate_id))


def list_candidates(conn: sqlite3.Connection, batch_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"SELECT {CANDIDATE_FIELDS} FROM fan_in_candidates WHERE batch_id=? "
        "ORDER BY entry_id, seq, id",
        (batch_id,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        out.append(item)
    return out
