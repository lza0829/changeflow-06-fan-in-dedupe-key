from app.engines.fan_in_rules import (
    DEDUPE_RULE,
    dedupe_by_entity_key,
    drop_reason,
    gap_decision,
    merge_rank,
)


def _cand(entry_id, seq, key, ts, rev, op="upsert", ref=None, channel="X"):
    return {
        "entry_id": entry_id,
        "seq": seq,
        "entity_key": key,
        "event_ts": ts,
        "revision": rev,
        "op": op,
        "payload": {"channel": channel},
        "_ref": ref if ref is not None else f"{entry_id}-{seq}",
    }


def test_dedupe_prefers_higher_revision():
    a = _cand(1, 1, "K", "2026-11-02T09:00:00Z", 2)
    b = _cand(2, 1, "K", "2026-11-02T09:03:00Z", 3)
    retained, dropped = dedupe_by_entity_key([a, b])
    assert retained == [b]
    assert len(dropped) == 1
    assert dropped[0]["dedupe_status"] == "dropped"
    assert dropped[0]["dedupe_reason"] == "dedupe_lower_revision"
    assert dropped[0]["retained_candidate_ref"] == b["_ref"]


def test_dedupe_event_ts_breaks_revision_tie():
    a = _cand(1, 3, "K", "2026-11-02T09:02:00Z", 2, channel="A")
    b = _cand(2, 3, "K", "2026-11-02T09:05:00Z", 2, channel="B")
    retained, dropped = dedupe_by_entity_key([a, b])
    assert retained == [b]
    assert drop_reason(dropped[0], b) == "dedupe_older_event_ts"


def test_dedupe_leaves_input_untouched_for_winners_and_marks_losers():
    a = _cand(1, 1, "K", "2026-11-02T09:00:00Z", 2)
    b = _cand(2, 1, "K", "2026-11-02T09:03:00Z", 3)
    retained, dropped = dedupe_by_entity_key([a, b])
    assert retained[0] is b
    assert b["dedupe_status"] == "retained"
    assert a["dedupe_status"] == "dropped"


def test_merge_order_is_event_time_then_entry_then_seq():
    a = _cand(2, 5, "A", "2026-11-02T09:06:00Z", 1)
    b = _cand(1, 3, "B", "2026-11-02T09:02:00Z", 1)
    c = _cand(1, 2, "C", "2026-11-02T09:02:00Z", 1)
    assert sorted([a, b, c], key=merge_rank)[0]["entity_key"] == "C"
    assert sorted([a, b, c], key=merge_rank)[-1]["entity_key"] == "A"


def test_gap_decision_on_retained_candidate():
    assert gap_decision(entry_consumed_cursor=1, seq=2, gap_policy="stop_quarantine") == "apply"
    assert (
        gap_decision(entry_consumed_cursor=1, seq=3, gap_policy="stop_quarantine")
        == "quarantine_gap"
    )
    assert gap_decision(entry_consumed_cursor=3, seq=2, gap_policy="stop_quarantine") in (
        "skipped_duplicate",
    )
    assert gap_decision(entry_consumed_cursor=1, seq=3, gap_policy="skip_gap") == "apply"


def test_rule_string_is_explicit():
    assert "revision" in DEDUPE_RULE and "event_ts" in DEDUPE_RULE
