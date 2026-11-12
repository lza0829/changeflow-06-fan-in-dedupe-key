from __future__ import annotations

import json
import sqlite3
from typing import Any


def insert_ledger(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    run_id: int,
    event: dict[str, Any],
    disposition: str,
    batch_id: int | None = None,
    entry_id: int = 0,
    merge_seq: int | None = None,
    apply_seq: int | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO change_ledger(
            stream_id, run_id, batch_id, entry_id, seq, merge_seq, apply_seq,
            op, entity_key, event_ts, payload_json, disposition
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            run_id,
            batch_id,
            int(entry_id),
            int(event["seq"]),
            merge_seq,
            apply_seq,
            event["op"],
            event["entity_key"],
            event["event_ts"],
            json.dumps(event.get("payload") or {}, ensure_ascii=False),
            disposition,
        ),
    )
    return int(cur.lastrowid)


def list_ledger(
    conn: sqlite3.Connection, limit: int = 200, batch_id: int | None = None
) -> list[dict[str, Any]]:
    where = "WHERE batch_id=?" if batch_id is not None else ""
    params: tuple[Any, ...] = (batch_id, limit) if batch_id is not None else (limit,)
    rows = conn.execute(
        f"""
        SELECT id, stream_id, run_id, batch_id, entry_id, seq, merge_seq, apply_seq,
               op, entity_key, event_ts, payload_json, disposition
        FROM change_ledger {where} ORDER BY id DESC LIMIT ?
        """,
        params,
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        out.append(item)
    return out


def count_by_disposition(conn: sqlite3.Connection, batch_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT disposition, COUNT(*) c FROM change_ledger WHERE batch_id=? GROUP BY disposition",
        (batch_id,),
    ).fetchall()
    return {str(r["disposition"]): int(r["c"]) for r in rows}


def count_for_batch(conn: sqlite3.Connection, batch_id: int) -> int:
    return int(
        conn.execute("SELECT COUNT(*) c FROM change_ledger WHERE batch_id=?", (batch_id,)).fetchone()[
            "c"
        ]
    )
