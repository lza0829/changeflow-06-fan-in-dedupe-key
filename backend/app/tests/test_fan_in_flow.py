import importlib

import pytest


@pytest.fixture()
def app_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import app.config
    import app.db

    importlib.reload(app.config)
    importlib.reload(app.db)
    import app.seed
    importlib.reload(app.seed)
    app.seed.init_db()
    import app.services.fan_in_service as svc

    importlib.reload(svc)
    return svc


def _batch(svc, entry_ids=None):
    detail = svc.run_fan_in(entry_ids)
    return detail


def test_fan_in_dedupe_apply_reconciles(app_env):
    svc = app_env
    detail = _batch(svc)
    report = detail["report"]
    counts = report["counts"]

    assert counts["candidate_total"] == 7
    assert counts["retained"] == 4
    assert counts["dropped"] == 3
    assert counts["applied"] == 3
    assert counts["quarantined"] == 1
    assert counts["blocked"] == 0
    assert counts["ledger_rows"] == 4
    assert all(check["ok"] for check in report["invariants"]), report["invariants"]

    manifest = {(c["entry_id"], c["seq"]): c for c in detail["manifest"]}
    # 同键 ORD-3001：B 的 revision 3 优先，A 的 revision 2 在汇入阶段丢弃。
    assert manifest[(1, 1)]["dedupe_status"] == "dropped"
    assert manifest[(2, 1)]["dedupe_status"] == "retained"
    assert manifest[(2, 1)]["apply_status"] == "applied"
    # ORD-3003：revision 相同，event_ts 新者保留。
    assert manifest[(2, 3)]["dedupe_status"] == "retained"
    assert manifest[(1, 3)]["dedupe_reason"] == "dedupe_older_event_ts"

    # 保留条因源侧缺口进隔离，与“汇入阶段去重”是两种状态。
    assert manifest[(2, 5)]["dedupe_status"] == "retained"
    assert manifest[(2, 5)]["apply_status"] == "quarantined"

    # 丢弃条不得写账本、不得占 apply 序、不得静默进 Sink。
    dropped = [c for c in detail["manifest"] if c["dedupe_status"] == "dropped"]
    assert all(c["ledger_id"] is None and c["apply_seq"] is None for c in dropped)
    sink_channels = {(row["entity_key"], row["payload"].get("channel")) for row in detail["sink_rows"]}
    assert ("ORD-3001", "B") in sink_channels
    assert all(channel != "A" or key == "ORD-3002" for key, channel in sink_channels)

    entries = {e["entry_id"]: e for e in detail["entries"]}
    assert entries[1]["pull_cursor"] == 3 and entries[1]["consumed_cursor"] == 3
    assert entries[2]["pull_cursor"] == 5 and entries[2]["consumed_cursor"] == 3

    # 批次持久化：重新打开后清单与对账数字仍在。
    again = svc.get_batch_detail(detail["id"])
    assert [c["id"] for c in again["manifest"]] == [c["id"] for c in detail["manifest"]]
    assert again["report"]["counts"] == counts


def test_fan_in_second_batch_is_no_change(app_env):
    svc = app_env
    first = _batch(svc)
    second = _batch(svc)
    assert second["status"] == "no_change"
    assert second["report"]["counts"]["candidate_total"] == 0
    assert first["id"] != second["id"]
    assert any(b["id"] == first["id"] for b in svc.list_batches())


def test_fan_in_requires_two_entries(app_env):
    svc = app_env
    with pytest.raises(ValueError):
        svc.run_fan_in([1])


def test_channel_replay_after_entry_reset_is_idempotent(app_env):
    svc = app_env
    first = _batch(svc)
    assert first["report"]["counts"]["ledger_rows"] == 4

    svc.reset_entry_cursors([1, 2])
    second = _batch(svc)
    counts = second["report"]["counts"]
    assert counts["candidate_total"] == 7
    assert counts["applied"] == 3 and counts["quarantined"] == 1
    assert counts["ledger_rows"] == 0  # 全部复用既有账本/隔离行
    assert counts["replayed"] == 4
    assert all(check["ok"] for check in second["report"]["invariants"]), second["report"][
        "invariants"
    ]
    # Sink 不被重放改写：last_seq 仍是首批的 apply 序。
    assert {row["entity_key"]: row["last_seq"] for row in second["sink_rows"]} == {
        "ORD-3001": 1,
        "ORD-3002": 2,
        "ORD-3003": 3,
    }
