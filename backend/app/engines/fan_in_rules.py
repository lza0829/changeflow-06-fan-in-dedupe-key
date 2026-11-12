from __future__ import annotations

from typing import Any, Iterable

# 固定裁决规则（UI 与报告同源展示）：
# 1) revision 高者优先；
# 2) revision 相同则 event_ts 新者优先；
# 3) 仍相同则入口登记 id 小者优先（先注册的入口优先）；
# 4) 最后按源 seq 大者优先（同一入口内更靠后的变更视为更新）。
DEDUPE_RULE = "revision_desc > event_ts_desc > entry_id_asc > seq_desc"


def _dedupe_rank(candidate: dict[str, Any]) -> tuple[int, str, int, int]:
    # 元组越大优先级越高：revision 高 > event_ts 新 > 入口登记 id 小 > 源 seq 大。
    return (
        int(candidate.get("revision") or 0),
        str(candidate["event_ts"]),
        -int(candidate["entry_id"]),
        int(candidate["seq"]),
    )


def merge_rank(candidate: dict[str, Any]) -> tuple[str, int, int]:
    """汇入后的处理顺序：先按 event_ts，再按入口登记顺序，再按源 seq。"""
    return (str(candidate["event_ts"]), int(candidate["entry_id"]), int(candidate["seq"]))


def drop_reason(loser: dict[str, Any], winner: dict[str, Any]) -> str:
    if int(loser.get("revision") or 0) < int(winner.get("revision") or 0):
        return "dedupe_lower_revision"
    if str(loser["event_ts"]) < str(winner["event_ts"]):
        return "dedupe_older_event_ts"
    if int(loser["entry_id"]) > int(winner["entry_id"]):
        return "dedupe_tie_entry_order"
    if int(loser["seq"]) < int(winner["seq"]):
        return "dedupe_tie_seq"
    return "dedupe_tie_break"


def dedupe_by_entity_key(
    candidates: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按 entity_key 分组裁决：每组仅保留一条（原对象就地标记）。"""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for cand in candidates:
        grouped.setdefault(str(cand["entity_key"]), []).append(cand)

    retained: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for entity_key, group in grouped.items():
        winner = max(group, key=_dedupe_rank)
        retained.append(winner)
        for loser in group:
            if loser is winner:
                continue
            loser["dedupe_status"] = "dropped"
            loser["dedupe_reason"] = drop_reason(loser, winner)
            loser["retained_candidate_ref"] = winner["_ref"]
            dropped.append(loser)
    for winner in retained:
        winner["dedupe_status"] = "retained"
        winner["dedupe_reason"] = None
    return retained, dropped


def gap_decision(*, entry_consumed_cursor: int, seq: int, gap_policy: str) -> str:
    """汇入保留条在共享应用通道上的链路裁决。

    每个入口按自身源 seq 连续消费；保留条越过期望 seq 视为源侧缺口。
    返回 apply / quarantine_gap / skipped_duplicate。
    """
    expected = int(entry_consumed_cursor) + 1
    if seq < expected:
        return "skipped_duplicate"
    if seq > expected:
        if gap_policy == "skip_gap":
            return "apply"
        return "quarantine_gap"
    return "apply"
