"""Bounded serial forecast of FinLab debt plus newly due source checks.

No SDK, account call, file scan or frame construction. A heap tracks arrivals
and the same stage priority as the collector. Unknown work stays unknown.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import heapq
import math

from stockagent.data.finlab_acquisition_contract import (
    STAGE_PRIORITY, queue_priority, quota_cycle_start, utc_time, DISPATCH_INTERVAL_SECONDS,
)

MIB = 1024**2
STAGE_LABELS = {
    "priority_updates": "到期市場行情（含收盤價）",
    "history": "尚未取得的歷史表",
    "updates": "其他到期歷史／特徵追新",
    "metadata": "來源與市場標籤",
    "source_issues": "來源全空／權限／錯誤待修復",
    "tick": "全市場 Tick（剩餘配額）",
}


def _number(value, *, positive=False):
    return (value if isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0 and (not positive or value > 0) else None)


def forecast(rows: list[dict], *, now: datetime, quota: dict, reserve_mb: float,
             next_run: datetime | None, duration_factor=1.0, quota_share=1.0,
             opening_policy=None, refresh_days=1, horizon_days=14, max_jobs=20000) -> dict:
    """Finish today's actionable debt, allowing due recurring work to preempt.

    The first future arrival uses the bound SDK expiry. Beyond that evidence,
    daily quota-cycle checks are conservative, not a fabricated release cron.
    Stage completion is cumulative elapsed time, not independent stopwatches.
    """
    targets = {r["key"] for r in rows if r["needs_refresh"] and not r["blocked_reason"]}
    remaining_by_stage = {stage: sum(r["key"] in targets and r["queue_role"] == stage for r in rows)
                          for stage in STAGE_PRIORITY}
    stage_finishes = {stage: now.isoformat() for stage, count in remaining_by_stage.items() if not count}
    completions = {}
    stages_started = {}
    work = waits = queue_wait = 0.0
    resets = arrivals = processed = 0
    cursor = max(now, next_run or now) if targets else now
    queue_wait = (cursor - now).total_seconds()

    def result(state, finish=None):
        return {"state": state, "finish_at_utc": finish,
                "remaining_seconds": round((cursor - now).total_seconds(), 1) if finish else None,
                "processing_seconds": round(work, 1), "total_wait_seconds": round(waits + queue_wait, 1),
                "quota_opening_wait_seconds": round(waits, 1), "queue_wait_seconds": round(queue_wait, 1),
                "quota_resets": resets, "newly_due_checks": arrivals, "simulated_checks": processed,
                "quota_share": quota_share, "duration_factor": duration_factor,
                "key_finish_at_utc": completions, "stage_finish_at_utc": stage_finishes,
                "stage_start_at_utc": stages_started}

    observed = utc_time(quota.get("observed_at_utc"))
    reset = utc_time(quota.get("reset_at_utc"))
    limit, room = _number(quota.get("limit_mb"), positive=True), _number(quota.get("remaining_mb"))
    if (limit is None or room is None or room > limit or not reset or reset <= now
            or not observed or not timedelta(0) <= now - observed <= timedelta(minutes=3)):
        return result("quota_unverified")
    gross_budget = limit * MIB * quota_share
    reserve_bytes = reserve_mb * MIB * quota_share
    budget = max(0, gross_budget - reserve_bytes)
    room *= MIB * quota_share
    if budget <= 0 and not any(r.get("incremental_quota_exempt") is True for r in rows):
        return result("no_daily_budget")
    if not targets:
        return result("complete", now.isoformat())
    future, ready = [], []
    by_key = {r["key"]: dict(r, queue_order=r.get("queue_order", i))
              for i, r in enumerate(rows) if not r["blocked_reason"]}

    def arrive(key, due, *, initial=False):
        heapq.heappush(future, (due, key, initial))

    def initial_slot(row):
        return max(now, utc_time(row.get("retry_at_utc")) or now,
                   utc_time(row.get("ready_at_utc")) or now)

    for key, row in by_key.items():
        if key in targets:
            arrive(key, initial_slot(row), initial=True)
        else:
            arrive(key, max(now, utc_time(row.get("next_source_check_at_utc")) or
                            quota_cycle_start(now) + timedelta(days=1)))
    horizon = now + timedelta(days=horizon_days)
    daily_capacity = {}
    for priority in STAGE_PRIORITY.values():
        higher = [r for r in by_key.values() if STAGE_PRIORITY[r["queue_role"]] < priority]
        costs = [_number(r.get("transfer_bytes"), positive=True) for r in higher]
        # Before every higher-priority key's first observed future slot has
        # passed, there can still be an earlier tail window. Do not prematurely
        # declare starvation from a catalog-size average.
        last_first_slot = max((initial_slot(r) if r["key"] in targets else
                               max(now, utc_time(r.get("next_source_check_at_utc")) or
                                   quota_cycle_start(now) + timedelta(days=1)) for r in higher), default=now)
        daily_capacity[priority] = (sum(c or 0 for c in costs) if all(c is not None for c in costs) else 0,
                                    last_first_slot)
    while targets:
        if cursor >= horizon or processed >= max_jobs:
            return result("capacity_constrained")
        reset_changed = False
        while cursor >= reset:
            room = gross_budget
            reset += timedelta(days=1)
            resets += 1
            reset_changed = True
        if reset_changed and ready:
            ready = [(queue_priority(by_key[key]["queue_role"], checked=age, now=cursor),
                      age, sequence, key, initial) for _, age, sequence, key, initial in ready]
            heapq.heapify(ready)
        target_priority = min(queue_priority(by_key[key]["queue_role"],
                                            checked=utc_time(by_key[key].get("source_checked_at_utc")), now=cursor)
                              for key in targets)
        recurring_bytes, last_first_slot = daily_capacity[target_priority]
        first_targets = [by_key[key] for key in targets if queue_priority(by_key[key]["queue_role"],
                         checked=utc_time(by_key[key].get("source_checked_at_utc")), now=cursor) == target_priority]
        smallest_target = min(max(_number(r.get("transfer_bytes")) or 0,
                                  _number(r.get("admission_bytes")) or 0) for r in first_targets)
        exempt_target = any(r.get("incremental_quota_exempt") is True for r in first_targets)
        target_budget = gross_budget if exempt_target else budget
        target_room = room if exempt_target else max(0, room - reserve_bytes)
        ready_exempt_target = any(
            by_key[key].get("incremental_quota_exempt") is True
            and max(_number(by_key[key].get("transfer_bytes")) or 0,
                    _number(by_key[key].get("admission_bytes")) or 0) <= room
            and initial_slot(by_key[key]) <= cursor for key in targets)
        if (refresh_days == 1 and recurring_bytes and recurring_bytes + smallest_target > target_budget
                and cursor >= last_first_slot and target_room < smallest_target and not ready_exempt_target):
            # In this conservative daily recurrence model a complete higher
            # priority cycle alone consumes the budget. No finite lower-stage
            # finish exists: avoid replaying thousands of identical cycles.
            return result("capacity_constrained")
        while future and future[0][0] <= cursor:
            due, key, initial = heapq.heappop(future)
            row = by_key[key]
            # Stalest checks first within the same stage, matching acquisition.
            age = utc_time(row.get("source_checked_at_utc")) or datetime.min.replace(tzinfo=UTC)
            heapq.heappush(ready, (queue_priority(row["queue_role"], checked=age, now=cursor),
                                   age, row["queue_order"], key, initial))
            arrivals += not initial
        if not ready:
            wake = min(future[0][0], reset) if future else reset
            queue_wait += (wake - cursor).total_seconds()
            cursor = wake
            continue
        priority, age, sequence, key, initial = ready[0]
        row = by_key[key]
        cost = _number(row.get("transfer_bytes"), positive=True)
        seconds = _number(row.get("fetch_seconds"), positive=True)
        if cost is None or seconds is None:
            return result("insufficient_samples")
        admission = max(cost, _number(row.get("admission_bytes")) or 0)
        exempt = row.get("incremental_quota_exempt") is True
        available = room if exempt else max(0, room - reserve_bytes)
        daily_budget = gross_budget if exempt else budget
        # Match the collector's quota-deferred prefix scan. A bounded-history
        # admission cannot force an otherwise admissible SDK check to wait.
        if admission > daily_budget or admission > available:
            alternatives = []
            for item in ready:
                candidate = by_key[item[3]]
                candidate_cost = _number(candidate.get("transfer_bytes"), positive=True)
                candidate_seconds = _number(candidate.get("fetch_seconds"), positive=True)
                if (candidate.get("incremental_quota_exempt") is True and candidate_cost is not None
                        and candidate_seconds is not None
                        and max(candidate_cost, _number(candidate.get("admission_bytes")) or 0) <= room):
                    alternatives.append(item)
            if alternatives:
                chosen = min(alternatives)
                # The normal heap still ranks the quota-deferred prefix first;
                # remove that one selected item explicitly before processing.
                priority, age, sequence, key, initial = chosen
                row = by_key[key]
                cost = _number(row.get("transfer_bytes"), positive=True)
                seconds = _number(row.get("fetch_seconds"), positive=True)
                admission = max(cost, _number(row.get("admission_bytes")) or 0)
                exempt = True
                available, daily_budget = room, gross_budget
        if admission > available:
            # A new SDK release/retry may become ready before the account
            # reset. Do not jump past that usable residual-capacity window.
            earlier = []
            for due, candidate_key, _ in future:
                candidate = by_key[candidate_key]
                candidate_cost = _number(candidate.get("transfer_bytes"), positive=True)
                if (cursor < due < reset and candidate.get("incremental_quota_exempt") is True
                        and candidate_cost is not None
                        and _number(candidate.get("fetch_seconds"), positive=True) is not None
                        and max(candidate_cost, _number(candidate.get("admission_bytes")) or 0) <= room):
                    earlier.append(due)
            if earlier:
                wake = min(earlier)
                waits += (wake - cursor).total_seconds()
                cursor = wake
                continue
        if daily_budget <= 0:
            return result("no_daily_budget")
        if admission > daily_budget:
            return result("object_exceeds_daily_budget")
        if admission > available:
            waits += (reset - cursor).total_seconds() + 5
            cursor = reset + timedelta(seconds=5)
            continue  # re-admit release-window work before selecting a key
        if opening_policy:
            shifted = opening_policy(cursor)
            if shifted > cursor:
                waits += (shifted - cursor).total_seconds()
                cursor = shifted
                continue
        selected = (priority, age, sequence, key, initial)
        if ready[0] == selected:
            heapq.heappop(ready)
        else:
            ready.remove(selected)
            heapq.heapify(ready)
        if key in targets:
            stages_started.setdefault(row["queue_role"], cursor.isoformat())
        duration = seconds * duration_factor
        cursor += timedelta(seconds=duration)
        work += duration
        room -= cost
        processed += 1
        if key in targets:
            targets.remove(key)
            completions[key] = cursor.isoformat()
            stage = row["queue_role"]
            remaining_by_stage[stage] -= 1
            if not remaining_by_stage[stage]:
                stage_finishes[stage] = cursor.isoformat()
        row = {**row, "source_checked_at_utc": cursor.isoformat()}
        by_key[key] = row
        planned = utc_time(row.get("next_source_check_at_utc"))
        next_due = planned if planned and planned > cursor else (
            quota_cycle_start(cursor) + timedelta(days=1) if refresh_days == 1
            else cursor + timedelta(days=refresh_days))
        row["next_source_check_at_utc"] = None
        arrive(key, next_due)
    return result("conditional", cursor.isoformat())


def stage_summary(rows: list[dict], scenarios: dict, *, now: datetime, tick: dict) -> list[dict]:
    stages = []
    for stage, label in STAGE_LABELS.items():
        selected = [r for r in rows if r["queue_role"] == stage]
        pending = [r for r in selected if r["needs_refresh"]]
        blocked = [r for r in selected if r["blocked_reason"]]
        known = [r for r in pending if r.get("transfer_bytes") is not None]
        records = [{"unit": unit, "count": sum(r["record_count"] for r in pending
                                               if r.get("record_unit") == unit and r.get("record_count") is not None)}
                   for unit in ("wide_values", "event_rows", "metadata_values")
                   if any(r.get("record_unit") == unit and r.get("record_count") is not None for r in pending)]
        next_checks = [utc_time(r.get("next_source_check_at_utc")) for r in selected if not r["needs_refresh"]]
        next_checks = [t for t in next_checks if t and t > now]
        retry_checks = [utc_time(r.get("retry_at_utc")) for r in pending]
        retry_checks = [t for t in retry_checks if t and t > now]
        state = "blocked" if blocked else "pending" if pending else "not_required" if not selected else "awaiting_release"
        entry = {"id": stage, "label": label, "state": state, "total_keys": len(selected),
                 "incremental_quota_exempt_keys": sum(r.get("incremental_quota_exempt") is True for r in selected),
                 "pending_keys": len(pending), "blocked_keys": len(blocked),
                 "completed_keys": len(selected) - len(pending),
                 "remaining_bytes_estimate": sum(r["transfer_bytes"] for r in known) if known else (0 if not pending else None),
                 "unknown_transfer_keys": sum(r.get("transfer_bytes") is None for r in pending),
                 "remaining_work_seconds_estimate": sum(r["fetch_seconds"] for r in pending
                                                       if r.get("fetch_seconds") is not None)
                    if any(r.get("fetch_seconds") is not None for r in pending) or not pending else None,
                 "unknown_time_keys": sum(r.get("fetch_seconds") is None for r in pending),
                 "next_check_at_utc": min(next_checks).isoformat() if next_checks else None,
                 "next_retry_at_utc": min(retry_checks).isoformat() if retry_checks else None,
                 "cooldown_keys": sum(bool(utc_time(r.get("retry_at_utc"))
                                           and utc_time(r["retry_at_utc"]) > now) for r in pending),
                 "records": records, "scenarios": {}}
        weights = [r.get("source_weight_bytes", r.get("transfer_bytes")) for r in selected]
        total_weight = sum(w or 0 for w in weights)
        done_weight = sum(r.get("source_weight_bytes", r.get("transfer_bytes")) or 0
                          for r in selected if not r["needs_refresh"])
        entry.update(source_weight_total_bytes=total_weight,
                     source_weight_completed_bytes=done_weight,
                     progress_ratio=done_weight / total_weight
                         if total_weight and all(w is not None for w in weights) and not blocked else None)
        for name, scenario in scenarios.items():
            finish = scenario.get("stage_finish_at_utc", {}).get(stage)
            entry["scenarios"][name] = {
                "state": "blocked" if blocked else "not_required" if not selected else
                    "conditional" if pending and finish else "complete" if not pending else scenario["state"],
                "start_at_utc": scenario.get("stage_start_at_utc", {}).get(stage),
                "finish_at_utc": None if blocked or not selected else finish,
            }
        if stage == "source_issues":
            entry["reason"] = "錯誤仍須重試；全空不是完成，不捏造成功時間或數值。"
        if stage == "metadata":
            promoted = sum(queue_priority(stage, checked=utc_time(r.get("source_checked_at_utc")), now=now)
                           < STAGE_PRIORITY[stage] for r in pending)
            entry["promoted_keys"] = promoted
            entry["reason"] = (f"{promoted} 鍵已跨完整配額週期未查核，提升至最舊特徵修補順序，避免永遠被新到期工作擠掉。"
                               if promoted else "標籤低於必要行情與缺失歷史；逾期會自動提升優先序，避免飢餓。")
        if stage == "tick":
            entry.update(state="unmeasured", completed_keys=None, pending_keys=None,
                         remaining_bytes_estimate=None, remaining_work_seconds_estimate=None,
                         next_check_at_utc=None, scenarios={}, reason=tick.get("reason"))
        elif entry["incremental_quota_exempt_keys"]:
            note = "已驗證 SDK 增量追新不受本機回補保留量阻擋；來源實際限額、發布到期與重試守門仍有效。"
            entry["reason"] = " ".join(filter(None, (entry.get("reason"), note)))
        stages.append(entry)
    return stages


def next_release_waves(rows: list[dict], *, now: datetime, quota: dict, reserve_mb: float,
                       next_run: datetime | None, opening_policy=None, refresh_days=1,
                       dispatch_interval_seconds=DISPATCH_INTERVAL_SECONDS) -> dict:
    """Forecast each stage's first *observed* future expiry wave, not all future data.

    This is distinct from today's completion. All other stages can still
    preempt according to the same quota/priority rules. A small bounded horizon
    protects the minute sampler; unknown source arrivals remain unknown.
    """
    result = {}
    for stage in STAGE_PRIORITY:
        selected = [r for r in rows if r["queue_role"] == stage and not r["needs_refresh"]
                    and not r["blocked_reason"] and (utc_time(r.get("next_source_check_at_utc")) or now) > now]
        if not selected:
            continue
        first = min(utc_time(r["next_source_check_at_utc"]) for r in selected)
        wave = [r for r in selected if utc_time(r["next_source_check_at_utc"]) == first]
        keys = {r["key"] for r in wave}
        targets = [dict(r, needs_refresh=r["needs_refresh"] or r["key"] in keys,
                        ready_at_utc=first.isoformat() if r["key"] in keys else None) for r in rows]
        estimates = {}
        for name, factor, share in (("fast", .75, 1), ("reference", 1, 1), ("slow", 1.5, .5)):
            lag = 0 if name == "fast" else dispatch_interval_seconds * (.5 if name == "reference" else 1)
            dispatched = [dict(r, ready_at_utc=(first+timedelta(seconds=lag)).isoformat())
                          if r["key"] in keys else r for r in targets]
            estimate = forecast(dispatched, now=now, quota=quota, reserve_mb=reserve_mb,
                                next_run=next_run, duration_factor=factor, quota_share=share,
                                opening_policy=opening_policy, refresh_days=refresh_days,
                                horizon_days=14, max_jobs=4000)
            finish = estimate.get("stage_finish_at_utc", {}).get(stage)
            estimates[name] = {"state": "conditional" if finish else estimate["state"],
                               "start_at_utc": estimate.get("stage_start_at_utc", {}).get(stage),
                               "finish_at_utc": finish}
        result[stage] = {"check_at_utc": first.isoformat(), "keys": len(wave),
                         "dispatch_interval_seconds": dispatch_interval_seconds,
                         "transfer_bytes_estimate": sum(r["transfer_bytes"] or 0 for r in wave)
                            if all(r["transfer_bytes"] is not None for r in wave) else None,
                         "scenarios": estimates,
                         "basis": "first_observed_sdk_expiry_wave_not_publication_or_all_future_history"}
    return result
