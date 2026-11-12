from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.fan_in_rules import (
    DEDUPE_RULE,
    dedupe_by_entity_key,
    gap_decision,
    merge_rank,
)
from app.engines.parse_change import parse_change_fixture
from app.repositories import cursors as cursor_repo
from app.repositories import fan_in as fan_in_repo
from app.repositories import ledger as ledger_repo
from app.repositories import quarantine as quarantine_repo
from app.repositories import runs as runs_repo
from app.repositories import settings as settings_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo


def _pull_entry_events(entry: dict[str, Any], max_per_entry: int = 0) -> list[dict[str, Any]]:
    """从本机夹具按入口 pull_cursor 拉取候选事件（仅本机夹具，不触网）。"""
    path = FIXTURES_DIR / entry["fixture_path"]
    events = parse_change_fixture(path)
    after = int(entry["pull_cursor"] or 0)
    pending = [e for e in events if int(e["seq"]) > after]
    if max_per_entry and max_per_entry > 0:
        pending = pending[:max_per_entry]
    return pending


def run_fan_in(
    entry_ids: list[int] | None = None, max_per_entry: int = 0
) -> dict[str, Any]:
    conn = connect()
    try:
        all_entries = [
            e
            for e in fan_in_repo.list_entries(conn)
            if e["enabled"] or (entry_ids and e["id"] in entry_ids)
        ]
        selected = (
            [e for e in all_entries if e["id"] in entry_ids]
            if entry_ids
            else [e for e in all_entries if e["enabled"]]
        )
        if len(selected) < 2:
            raise ValueError("至少需要两个启用/选中的汇入入口")
        target_stream_id = int(selected[0]["stream_id"])
        if any(int(e["stream_id"]) != target_stream_id for e in selected):
            raise ValueError("本次入口必须共享同一汇入通道")
        stream = streams_repo.get_stream(conn, target_stream_id)
        if not stream:
            raise ValueError("target stream not found")

        # 先全部解析成功，再开始写批次，避免留下半成品清单。
        pulled_by_entry: dict[int, list[dict[str, Any]]] = {}
        for entry in selected:
            pulled_by_entry[int(entry["id"])] = _pull_entry_events(entry, max_per_entry)
        selected_ids = [int(e["id"]) for e in selected]
        batch_id = fan_in_repo.create_batch(
            conn,
            target_stream_id=target_stream_id,
            entry_ids=selected_ids,
            dedupe_rule=DEDUPE_RULE,
            max_per_entry=int(max_per_entry or 0),
        )

        cursor_before = int(stream["apply_cursor"] or 0)
        gap_policy = settings_repo.get(conn, "gap_policy", "stop_quarantine") or "stop_quarantine"
        run_id = runs_repo.create_run(conn, target_stream_id, cursor_before)
        sink_before = sink_repo.count_for_stream(conn, target_stream_id)

        # 1) 汇入候选：所有入口的待处理事件进入同一集合（先全部 pending）。
        candidates: list[dict[str, Any]] = []
        for entry in selected:
            for event in pulled_by_entry[int(entry["id"])]:
                candidates.append(
                    {
                        "entry_id": int(entry["id"]),
                        "entry_name": entry["name"],
                        "seq": int(event["seq"]),
                        "op": event["op"],
                        "entity_key": event["entity_key"],
                        "event_ts": event["event_ts"],
                        "revision": int(event.get("revision") or 0),
                        "payload": event.get("payload") or {},
                        "dedupe_status": "pending",
                        "dedupe_reason": None,
                    }
                )
        for index, cand in enumerate(candidates, start=1):
            cand["_ref"] = index
            cand["merge_seq"] = 0  # 待整体汇入排序后回填
        ordered = sorted(candidates, key=merge_rank)
        for index, cand in enumerate(ordered, start=1):
            cand["merge_seq"] = index
        for cand in ordered:
            cand["id"] = fan_in_repo.insert_candidate(
                conn, batch_id=batch_id, candidate=cand
            )

        # 2) 同实体键裁决：每组只保留一条。
        retained, dropped = dedupe_by_entity_key(candidates)
        id_by_ref = {cand["_ref"]: cand["id"] for cand in candidates}
        for cand in retained:
            fan_in_repo.set_candidate_dedupe(
                conn,
                cand["id"],
                status="retained",
                reason=None,
                retained_candidate_id=cand["id"],
            )
        for cand in dropped:
            winner_id = id_by_ref[cand["retained_candidate_ref"]]
            fan_in_repo.set_candidate_dedupe(
                conn,
                cand["id"],
                status="dropped",
                reason=cand["dedupe_reason"],
                retained_candidate_id=winner_id,
            )

        # 3) 链路裁决：保留条仍按入口源序走连续序规则；入口缺口 → 隔离；
        #    去重丢弃视为源侧已消费，不制造假缺口；缺口后同入口后续保留条 blocked。
        entry_state: dict[int, dict[str, Any]] = {}
        for entry in selected:
            entry_state[int(entry["id"])] = {
                "consumed": int(entry["consumed_cursor"] or 0),
                "blocked": False,
                "actions": [],
            }
        candidates_by_entry: dict[int, list[dict[str, Any]]] = {}
        for cand in candidates:
            candidates_by_entry.setdefault(cand["entry_id"], []).append(cand)
        for entry_id, rows in candidates_by_entry.items():
            state = entry_state[entry_id]
            for cand in sorted(rows, key=lambda c: c["seq"]):
                if cand["dedupe_status"] == "dropped":
                    state["consumed"] = max(state["consumed"], cand["seq"])
                    state["actions"].append((cand, "dedupe_dropped"))
                    continue
                if state["blocked"]:
                    state["actions"].append((cand, "blocked_gap"))
                    continue
                decision = gap_decision(
                    entry_consumed_cursor=state["consumed"],
                    seq=int(cand["seq"]),
                    gap_policy=gap_policy,
                )
                if decision == "quarantine_gap":
                    state["actions"].append((cand, "quarantine_gap"))
                    state["blocked"] = True
                elif decision == "skipped_duplicate":
                    state["actions"].append((cand, "skipped_duplicate"))
                else:
                    state["consumed"] = cand["seq"]
                    state["actions"].append((cand, "apply"))

        # 4) 按汇入顺序执行 Apply：只有保留且链路过关的条写账本与 Sink。
        actions = [action for state in entry_state.values() for action in state["actions"]]
        actions.sort(key=lambda item: merge_rank(item[0]))
        apply_seq = cursor_before
        for cand, action in actions:
            if action == "dedupe_dropped":
                fan_in_repo.set_candidate_apply(
                    conn, cand["id"], status="dropped_at_fan_in"
                )
                continue
            if action == "blocked_gap":
                fan_in_repo.set_candidate_apply(
                    conn, cand["id"], status="blocked_gap"
                )
                continue
            if action == "skipped_duplicate":
                fan_in_repo.set_candidate_apply(
                    conn, cand["id"], status="skipped_duplicate"
                )
                continue
            if action == "quarantine_gap":
                replayed_quarantine = fan_in_repo.find_quarantined_by_source(
                    conn,
                    stream_id=target_stream_id,
                    entry_id=cand["entry_id"],
                    seq=int(cand["seq"]),
                    entity_key=cand["entity_key"],
                )
                if replayed_quarantine:
                    fan_in_repo.set_candidate_replay(
                        conn,
                        cand["id"],
                        status="quarantined",
                        quarantine_id=int(replayed_quarantine["quarantine_id"]),
                        ledger_id=int(replayed_quarantine["ledger_id"])
                        if replayed_quarantine["ledger_id"]
                        else None,
                    )
                    continue
                ledger_id = ledger_repo.insert_ledger(
                    conn,
                    stream_id=target_stream_id,
                    run_id=run_id,
                    event=cand,
                    disposition="quarantine_gap",
                    batch_id=batch_id,
                    entry_id=cand["entry_id"],
                    merge_seq=cand["merge_seq"],
                )
                quarantine_id = quarantine_repo.insert(
                    conn,
                    stream_id=target_stream_id,
                    run_id=run_id,
                    seq=int(cand["seq"]),
                    entity_key=cand["entity_key"],
                    reason="source_seq_gap_after_fan_in",
                    payload=cand.get("payload") or {},
                    batch_id=batch_id,
                    candidate_id=cand["id"],
                )
                fan_in_repo.set_candidate_apply(
                    conn,
                    cand["id"],
                    status="quarantined",
                    ledger_id=ledger_id,
                    quarantine_id=quarantine_id,
                )
                continue

            # 共享通道幂等：入口游标重置后重放同一源事件，不重复写账本/Sink，
            # 复用既有处置并在清单标记 replayed=1。
            replayed_ledger = fan_in_repo.find_applied_by_source(
                conn,
                stream_id=target_stream_id,
                entry_id=cand["entry_id"],
                seq=int(cand["seq"]),
                entity_key=cand["entity_key"],
                event_ts=cand["event_ts"],
            )
            if replayed_ledger:
                fan_in_repo.set_candidate_replay(
                    conn,
                    cand["id"],
                    status="applied",
                    ledger_id=int(replayed_ledger["id"]),
                    apply_seq=int(replayed_ledger["apply_seq"] or 0) or None,
                )
                continue

            apply_seq += 1
            if cand["op"] == "upsert":
                sink_repo.upsert(
                    conn,
                    stream_id=target_stream_id,
                    entity_key=cand["entity_key"],
                    payload=cand.get("payload") or {},
                    seq=apply_seq,
                    event_ts=cand["event_ts"],
                )
            else:
                sink_repo.delete(
                    conn, stream_id=target_stream_id, entity_key=cand["entity_key"]
                )
            cursor_repo.mark_applied(conn, target_stream_id, apply_seq, run_id)
            streams_repo.set_cursor(conn, target_stream_id, apply_seq)
            ledger_id = ledger_repo.insert_ledger(
                conn,
                stream_id=target_stream_id,
                run_id=run_id,
                event=cand,
                disposition="applied",
                batch_id=batch_id,
                entry_id=cand["entry_id"],
                merge_seq=cand["merge_seq"],
                apply_seq=apply_seq,
            )
            fan_in_repo.set_candidate_apply_seq(conn, cand["id"], apply_seq)
            fan_in_repo.set_candidate_ledger(conn, cand["id"], ledger_id)
            fan_in_repo.set_candidate_apply(conn, cand["id"], status="applied")

        # 5) 游标：
        #    pull_cursor     = 已拉取到的最大源 seq；下一批次从它之后继续拉。
        #                      本批所有候选（含 blocked_gap）都已在清单中有明确状态，
        #                      所以批次终态时统一推进，不做静默丢弃。
        #    consumed_cursor = 本入口已应用到共享通道的最大源 seq（去重丢弃视为
        #                      源侧已消费，也推进）；隔离/阻塞不推进，因此它小于
        #                      pull_cursor 的部分正是“未进入通道”的事件。
        #    源侧缺口补齐或需要重放时，用入口游标重置接口（重置后由共享通道幂等
        #    去重复用既有账本/隔离行，标记 replayed=1）。
        for entry in selected:
            entry_id = int(entry["id"])
            pulled = pulled_by_entry[entry_id]
            if not pulled:
                continue
            new_pull = max(
                int(entry["pull_cursor"] or 0), max(int(e["seq"]) for e in pulled)
            )
            fan_in_repo.set_entry_cursors(
                conn,
                entry_id,
                pull_cursor=new_pull,
                consumed_cursor=max(
                    int(entry["consumed_cursor"] or 0), entry_state[entry_id]["consumed"]
                ),
            )

        # 6) 报告数字全部从库内同源数据回读，不做内存计数自证。
        report = _build_report(
            conn,
            batch_id=batch_id,
            run_id=run_id,
            target_stream_id=target_stream_id,
            entries=selected,
            entry_state=entry_state,
            pulled_by_entry=pulled_by_entry,
            sink_before=sink_before,
            cursor_before=cursor_before,
        )
        report["invariants"] = _check_invariants(conn, batch_id, target_stream_id, report)
        status = (
            "partial"
            if (report["counts"]["quarantined"] or report["counts"]["blocked"])
            else "succeeded"
        )
        if report["counts"]["candidate_total"] == 0:
            status = "no_change"
        cursor_after = int(streams_repo.get_stream(conn, target_stream_id)["apply_cursor"] or 0)

        fan_in_repo.update_batch(
            conn,
            batch_id,
            status=status,
            candidate_total=report["counts"]["candidate_total"],
            retained=report["counts"]["retained"],
            dropped=report["counts"]["dropped"],
            applied=report["counts"]["applied"],
            quarantined=report["counts"]["quarantined"],
            blocked=report["counts"]["blocked"],
            report=report,
            finished=True,
        )
        runs_repo.finish_run(
            conn,
            run_id,
            status="partial" if status == "partial" else "succeeded",
            loaded=report["counts"]["candidate_total"],
            applied=report["counts"]["applied"],
            quarantined=report["counts"]["quarantined"],
            skipped=report["counts"]["dropped"]
            + report["counts"]["blocked"]
            + report["counts"]["skipped_duplicate"],
            cursor_after=cursor_after,
            report={
                "kind": "fan_in",
                "batch_id": batch_id,
                "entry_ids": [int(e["id"]) for e in selected],
                "dedupe_rule": DEDUPE_RULE,
                **report,
            },
        )
        conn.commit()
        return get_batch_detail(batch_id, conn=conn) or {}
    except ValueError:
        conn.rollback()
        raise
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        raise ValueError(f"fan-in failed: {exc}") from exc
    finally:
        conn.close()


def _build_report(
    conn,
    *,
    batch_id: int,
    run_id: int,
    target_stream_id: int,
    entries: list[dict[str, Any]],
    entry_state: dict[int, dict[str, Any]],
    pulled_by_entry: dict[int, list[dict[str, Any]]],
    sink_before: int,
    cursor_before: int,
) -> dict[str, Any]:
    counts = _counts(conn, batch_id)
    candidates = fan_in_repo.list_candidates(conn, batch_id)
    entry_reports = []
    for entry in entries:
        entry_id = int(entry["id"])
        rows = [c for c in candidates if c["entry_id"] == entry_id]
        entry_reports.append(
            {
                "entry_id": entry_id,
                "name": entry["name"],
                "fixture_path": entry["fixture_path"],
                "pulled_count": len(pulled_by_entry[entry_id]),
                "candidate_count": len(rows),
                "retained": sum(1 for c in rows if c["dedupe_status"] == "retained"),
                "dropped": sum(1 for c in rows if c["dedupe_status"] == "dropped"),
                "applied": sum(1 for c in rows if c["apply_status"] == "applied"),
                "quarantined": sum(1 for c in rows if c["apply_status"] == "quarantined"),
                "blocked": sum(1 for c in rows if c["apply_status"] == "blocked_gap"),
                "pull_cursor_before": int(entry["pull_cursor"] or 0),
                "pull_cursor_after": int(
                    (fan_in_repo.get_entry(conn, entry_id) or entry)["pull_cursor"] or 0
                ),
                "consumed_cursor_before": int(entry["consumed_cursor"] or 0),
                "consumed_cursor_after": int(entry_state[entry_id]["consumed"]),
            }
        )
    ledger_dispositions = ledger_repo.count_by_disposition(conn, batch_id)
    stream_row = streams_repo.get_stream(conn, target_stream_id)
    sink_after = sink_repo.count_for_stream(conn, target_stream_id)
    sink_rows = sink_repo.list_sink_for_keys(
        conn, target_stream_id, sorted({c["entity_key"] for c in candidates})
    )
    dropped_reasons: dict[str, int] = {}
    for cand in candidates:
        if cand["dedupe_status"] == "dropped":
            dropped_reasons[cand["dedupe_reason"] or "unknown"] = (
                dropped_reasons.get(cand["dedupe_reason"] or "unknown", 0) + 1
            )
    return {
        "batch_id": batch_id,
        "run_id": run_id,
        "target_stream_id": target_stream_id,
        "dedupe_rule": DEDUPE_RULE,
        "gap_policy": settings_repo.get(conn, "gap_policy", "stop_quarantine"),
        "counts": counts,
        "entries": entry_reports,
        "dropped_by_reason": dropped_reasons,
        "ledger_by_disposition": ledger_dispositions,
        "ledger_rows": ledger_repo.count_for_batch(conn, batch_id),
        "sink": {
            "stream_id": target_stream_id,
            "before": sink_before,
            "after": sink_after,
            "delta": sink_after - sink_before,
            "keys_in_batch": sorted({c["entity_key"] for c in candidates}),
            "rows": sink_rows,
        },
        "cursor": {
            "stream_apply_cursor_before": cursor_before,
            "stream_apply_cursor_after": int(stream_row["apply_cursor"] or 0) if stream_row else None,
            "entries": entry_reports,
        },
    }


def _counts(conn, batch_id: int) -> dict[str, int]:
    candidates = fan_in_repo.list_candidates(conn, batch_id)
    ledger_dispositions = ledger_repo.count_by_disposition(conn, batch_id)
    return {
        "candidate_total": len(candidates),
        "retained": sum(1 for c in candidates if c["dedupe_status"] == "retained"),
        "dropped": sum(1 for c in candidates if c["dedupe_status"] == "dropped"),
        "pending_dedupe": sum(1 for c in candidates if c["dedupe_status"] == "pending"),
        "applied": sum(1 for c in candidates if c["apply_status"] == "applied"),
        "quarantined": sum(1 for c in candidates if c["apply_status"] == "quarantined"),
        "blocked": sum(1 for c in candidates if c["apply_status"] == "blocked_gap"),
        "skipped_duplicate": sum(
            1 for c in candidates if c["apply_status"] == "skipped_duplicate"
        ),
        "replayed": sum(1 for c in candidates if c["replayed"]),
        "replayed_applied": sum(
            1 for c in candidates if c["replayed"] and c["apply_status"] == "applied"
        ),
        "replayed_quarantined": sum(
            1 for c in candidates if c["replayed"] and c["apply_status"] == "quarantined"
        ),
        # 本批新写入的行；replayed 条复用既有账本/隔离行，不计入本批新增。
        "ledger_rows": ledger_repo.count_for_batch(conn, batch_id),
        "ledger_applied": ledger_dispositions.get("applied", 0),
        "ledger_quarantined": ledger_dispositions.get("quarantine_gap", 0),
        "new_applied": sum(
            1 for c in candidates if c["apply_status"] == "applied" and not c["replayed"]
        ),
        "new_quarantined": sum(
            1 for c in candidates if c["apply_status"] == "quarantined" and not c["replayed"]
        ),
    }


def _check_invariants(
    conn, batch_id: int, target_stream_id: int, report: dict[str, Any]
) -> list[dict[str, Any]]:
    counts = report["counts"]
    candidates = fan_in_repo.list_candidates(conn, batch_id)
    sink_rows = {r["entity_key"]: r for r in sink_repo.list_sink_for_stream(conn, target_stream_id)}
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    add(
        "候选总数 = 保留 + 丢弃",
        counts["candidate_total"] == counts["retained"] + counts["dropped"],
        f"{counts['candidate_total']} = {counts['retained']} + {counts['dropped']}",
    )
    add(
        "保留条 = 已应用 + 隔离 + 缺口阻塞 + 链路重复",
        counts["retained"]
        == counts["applied"] + counts["quarantined"] + counts["blocked"]
        + counts["skipped_duplicate"],
        f"{counts['retained']} = {counts['applied']} + {counts['quarantined']} + "
        f"{counts['blocked']} + {counts['skipped_duplicate']}",
    )
    add(
        "本批账本行 = 本批新增 applied + 新增隔离",
        counts["ledger_rows"] == counts["new_applied"] + counts["new_quarantined"],
        f"ledger {counts['ledger_rows']} = new applied {counts['new_applied']} + "
        f"new quarantine {counts['new_quarantined']} "
        f"（重放 applied {counts['replayed_applied']} / quarantine "
        f"{counts['replayed_quarantined']} 复用既有行）",
    )
    add(
        "账本处置与清单一致",
        counts["ledger_applied"] == counts["new_applied"]
        and counts["ledger_quarantined"] == counts["new_quarantined"],
        f"applied {counts['ledger_applied']} = new {counts['new_applied']} · quarantine "
        f"{counts['ledger_quarantined']} = new {counts['new_quarantined']}",
    )

    dropped = [c for c in candidates if c["dedupe_status"] == "dropped"]
    add(
        "丢弃条不写账本/不占 apply 序",
        all(
            c["ledger_id"] is None
            and c["apply_seq"] is None
            and c["quarantine_id"] is None
            for c in dropped
        ),
        f"{len(dropped)} 条丢弃候选均无 ledger_id / quarantine_id / apply_seq",
    )

    retained_rows = [c for c in candidates if c["dedupe_status"] == "retained"]
    retained_keys = {c["entity_key"] for c in retained_rows}
    candidate_keys = {c["entity_key"] for c in candidates}
    add(
        "每个实体键恰有一条保留条",
        len(retained_rows) == len(retained_keys) == len(candidate_keys),
        f"保留 {len(retained_rows)} 条 · 实体键 {len(retained_keys)} 个 · "
        f"候选键 {len(candidate_keys)} 个",
    )

    # 由清单推导本批应在 Sink 留下的最终态：按 apply 序重放 applied 条
    # （upsert 覆盖、delete 移除），再与 Sink 实际行逐字段比对。
    applied_rows = sorted(
        (c for c in candidates if c["apply_status"] == "applied"),
        key=lambda c: int(c["apply_seq"]),
    )
    expected_sink: dict[str, dict[str, Any]] = {}
    for cand in applied_rows:
        if cand["op"] == "upsert":
            expected_sink[cand["entity_key"]] = cand
        else:
            expected_sink.pop(cand["entity_key"], None)
    batch_keys = {c["entity_key"] for c in candidates}
    observed = {k: v for k, v in sink_rows.items() if k in batch_keys}
    sink_ok = set(observed) == set(expected_sink) and all(
        int(observed[key]["last_seq"]) == int(expected_sink[key]["apply_seq"])
        and observed[key]["last_event_ts"] == expected_sink[key]["event_ts"]
        and observed[key]["payload"] == expected_sink[key]["payload"]
        for key in expected_sink
    )
    add(
        "Sink 终态可由保留条重放推导（entity/last_seq/event_ts/payload 对齐）",
        sink_ok,
        f"{len(applied_rows)} 条已应用候选 → {len(expected_sink)} 行 Sink；"
        f"实际本批相关 {len(observed)} 行",
    )

    dropped_payloads = {(c["entity_key"], c["event_ts"], c["payload"].get("channel")) for c in dropped}
    sink_from_dropped = [
        r
        for r in sink_rows.values()
        if (r["entity_key"], r["last_event_ts"], r["payload"].get("channel")) in dropped_payloads
    ]
    add(
        "丢弃条未静默写入 Sink",
        len(sink_from_dropped) == 0,
        f"0 行 Sink 可追溯到丢弃候选（检查 {len(sink_rows)} 行）",
    )

    add(
        "拉取总数 = 候选总数",
        sum(e["pulled_count"] for e in report["entries"]) == counts["candidate_total"],
        f"pulled {sum(e['pulled_count'] for e in report['entries'])} = candidates {counts['candidate_total']}",
    )

    # replayed 条复用既有批次的账本行，需按 id 全库定位而非只查本批。
    ledger_candidates = [c for c in candidates if c["apply_status"] in ("applied", "quarantined")]
    missing_links = [
        c["id"]
        for c in ledger_candidates
        if c["ledger_id"] is None
        or conn.execute(
            "SELECT 1 FROM change_ledger WHERE id=? AND entity_key=? AND seq=?",
            (c["ledger_id"], c["entity_key"], int(c["seq"])),
        ).fetchone()
        is None
    ]
    entry_ok = True
    entry_detail = []
    for entry_report in report["entries"]:
        entry_retained = entry_report["retained"]
        terminal = (
            entry_report["applied"]
            + entry_report["quarantined"]
            + entry_report["blocked"]
        )
        ok = (
            entry_report["candidate_count"]
            == entry_retained + entry_report["dropped"]
            and entry_retained == terminal
            and entry_report["pulled_count"] == entry_report["candidate_count"]
        )
        entry_ok = entry_ok and ok
        entry_detail.append(
            f"{entry_report['entry_id']}: 拉取 {entry_report['pulled_count']} = 候选 "
            f"{entry_report['candidate_count']} = 保留 {entry_retained} + 丢弃 "
            f"{entry_report['dropped']}；保留 = {terminal}"
        )
    add("各入口拉取/候选/保留/丢弃逐口对账", entry_ok, " · ".join(entry_detail))

    add(
        "清单与账本逐条可追溯",
        not missing_links,
        f"{len(ledger_candidates)} 条清单候选可定位到账本行"
        + (f"（缺失 {missing_links}）" if missing_links else ""),
    )
    return checks


def _entry_view(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "entry_id": int(entry["id"]),
        "name": entry["name"],
        "fixture_path": entry["fixture_path"],
        "stream_id": int(entry["stream_id"]),
        "enabled": bool(entry["enabled"]),
        "pull_cursor": int(entry["pull_cursor"] or 0),
        "consumed_cursor": int(entry["consumed_cursor"] or 0),
        "note": entry.get("note"),
    }


def get_batch_detail(batch_id: int, conn=None) -> dict[str, Any] | None:
    own = conn is None
    if own:
        conn = connect()
    try:
        batch = fan_in_repo.get_batch(conn, batch_id)
        if not batch:
            return None
        candidates = fan_in_repo.list_candidates(conn, batch_id)
        entries = {
            int(e["id"]): e for e in fan_in_repo.list_entries(conn) if int(e["id"]) in batch["entry_ids"]
        }
        ledger_rows = ledger_repo.list_ledger(conn, limit=100000, batch_id=batch_id)
        quarantine_ids = {c["quarantine_id"] for c in candidates if c["quarantine_id"]}
        quarantine_rows = [
            q for q in quarantine_repo.list_quarantine(conn) if q["id"] in quarantine_ids
        ]
        sink_keys = sorted({c["entity_key"] for c in candidates})
        sink_rows = sink_repo.list_sink_for_keys(conn, batch["target_stream_id"], sink_keys)
        return {
            **batch,
            "manifest": candidates,
            "sink_rows": sink_rows,
            "ledger_rows": ledger_rows,
            "quarantine_rows": quarantine_rows,
            "entries": [_entry_view(entries[i]) for i in batch["entry_ids"] if i in entries],
        }
    finally:
        if own:
            conn.close()


def list_batches(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return fan_in_repo.list_batches(conn, limit=limit)
    finally:
        conn.close()


def list_entries() -> list[dict[str, Any]]:
    conn = connect()
    try:
        entries = fan_in_repo.list_entries(conn)
        rows = conn.execute(
            """
            SELECT entry_id, COUNT(*) c, MAX(batch_id) last_batch_id
            FROM fan_in_candidates GROUP BY entry_id
            """
        ).fetchall()
        stats = {int(r["entry_id"]): dict(r) for r in rows}
        for entry in entries:
            stat = stats.get(int(entry["id"]), {})
            entry["candidate_count_all_batches"] = int(stat.get("c", 0))
            entry["last_batch_id"] = stat.get("last_batch_id")
        return entries
    finally:
        conn.close()


def patch_entry(entry_id: int, *, enabled: bool | None = None) -> dict[str, Any]:
    conn = connect()
    try:
        entry = fan_in_repo.get_entry(conn, entry_id)
        if not entry:
            raise ValueError("entry not found")
        if enabled is not None:
            conn.execute(
                "UPDATE change_entries SET enabled=? WHERE id=?",
                (1 if enabled else 0, entry_id),
            )
        conn.commit()
        return fan_in_repo.get_entry(conn, entry_id) or {}
    finally:
        conn.close()


def reset_entry_cursors(entry_ids: list[int] | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        fan_in_repo.reset_entry_cursors(conn, entry_ids)
        conn.commit()
        return fan_in_repo.list_entries(conn)
    finally:
        conn.close()
