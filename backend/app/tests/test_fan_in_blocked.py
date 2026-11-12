import importlib
import json

import pytest


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import app.config
    import app.db
    import app.seed

    importlib.reload(app.config)
    importlib.reload(app.db)
    importlib.reload(app.seed)
    app.seed.init_db()

    # 入口 A（id=3 缺口入口）：seq 2 缺失，seq 3 隔离、seq 4 被缺口阻塞；
    # 入口 B（id=1 干净入口）：partner_a_cdc.jsonl。
    fixture = tmp_path / "entry_gap.jsonl"
    fixture.write_text(
        "\n".join(
            [
                json.dumps({"seq": 1, "op": "upsert", "entity_key": "K-1",
                            "event_ts": "2026-11-03T10:00:00Z", "revision": 1,
                            "payload": {"v": 1}}),
                json.dumps({"seq": 3, "op": "upsert", "entity_key": "K-3",
                            "event_ts": "2026-11-03T10:02:00Z", "revision": 1,
                            "payload": {"v": 3}}),
                json.dumps({"seq": 4, "op": "upsert", "entity_key": "K-4",
                            "event_ts": "2026-11-03T10:03:00Z", "revision": 1,
                            "payload": {"v": 4}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    import app.services.fan_in_service as svc

    importlib.reload(svc)
    from app.db import connect

    conn = connect()
    conn.execute(
        "INSERT INTO change_entries(name, fixture_path, stream_id, note) VALUES (?,?,?,?)",
        ("Gap Entry", str(fixture), 3, "缺口测试入口"),
    )
    conn.execute(
        "INSERT INTO change_entries(name, fixture_path, stream_id, note) VALUES (?,?,?,?)",
        ("Decoy Entry", "partner_a_cdc.jsonl", 3, "干净入口"),
    )
    conn.commit()
    conn.close()
    return svc


def test_gap_blocks_retained_candidates_without_silent_loss(env):
    svc = env
    detail = svc.run_fan_in([1, 3])
    report = detail["report"]
    counts = report["counts"]
    manifest = {(c["entry_id"], c["seq"]): c for c in detail["manifest"]}

    assert manifest[(3, 1)]["apply_status"] == "applied"
    assert manifest[(3, 3)]["dedupe_status"] == "retained"
    assert manifest[(3, 3)]["apply_status"] == "quarantined"
    assert manifest[(3, 4)]["dedupe_status"] == "retained"
    assert manifest[(3, 4)]["apply_status"] == "blocked_gap"
    assert counts["quarantined"] == 1 and counts["blocked"] == 1

    # blocked_gap 未提交：无账本行、无隔离行、未占 apply 序；但已在清单中留痕，
    # 因此 pull 游标会越过它（不会静默丢事件），consumed 游标停在缺口前。
    assert manifest[(3, 4)]["ledger_id"] is None
    assert manifest[(3, 4)]["quarantine_id"] is None
    assert manifest[(3, 4)]["apply_seq"] is None
    entries = {e["entry_id"]: e for e in detail["entries"]}
    assert entries[3]["pull_cursor"] == 4
    assert entries[3]["consumed_cursor"] == 1
    assert all(check["ok"] for check in report["invariants"]), report["invariants"]
