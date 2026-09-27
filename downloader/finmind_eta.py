"""Conditional completion scenarios for one shared FinMind account.

Known, currently registered work is finite; discovering all historical symbols
or guaranteeing a provider's future availability is not. Request-start rates
are explicitly dispatch proxies, not measured success rates or confidence bands.
"""
from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
import math
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_json


TAIPEI = ZoneInfo('Asia/Taipei')
SCOPE_LABEL = 'FinMind 已排程可執行工作（含次要校驗）'


def _number(value: Any) -> float | None:
    return float(value) if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def scheduled_finish(now: datetime, active_seconds: float, *,
                     day_is_protected: Callable[[date], bool]) -> tuple[datetime, int]:
    """Convert useful service time to wall time with canonical opening breaks."""
    if now.tzinfo is None or not math.isfinite(active_seconds) or active_seconds < 0:
        raise ValueError('aware timestamp and finite nonnegative duration required')
    if active_seconds > 366 * 86400:
        raise ValueError('estimate exceeds one-year projection horizon')
    remaining = active_seconds
    cursor = now.astimezone(TAIPEI)
    pause = 0.0
    for _ in range(3 * 366 + 4):
        if remaining <= 0:
            return cursor.astimezone(UTC), math.ceil(pause)
        day = cursor.date()
        next_day = datetime.combine(day + timedelta(days=1), time(), TAIPEI)
        start = datetime.combine(day, time(8, 20), TAIPEI)
        end = datetime.combine(day, time(9, 10), TAIPEI)
        protected = day_is_protected(day)
        if protected and start <= cursor < end:
            pause += (end - cursor).total_seconds()
            cursor = end
            continue
        boundary = start if protected and cursor < start else next_day
        available = (boundary - cursor).total_seconds()
        if remaining <= available:
            return (cursor + timedelta(seconds=remaining)).astimezone(UTC), math.ceil(pause)
        remaining -= available
        cursor = boundary
    raise ValueError('calendar projection did not converge')


def estimate_completion(workload: dict[str, Any], telemetry: dict[str, Any], now: datetime, *,
                        day_is_protected: Callable[[date], bool] | None = None) -> dict[str, Any]:
    if now.tzinfo is None:
        raise ValueError('now must be timezone-aware')
    now = now.astimezone(UTC)
    summary = workload.get('summary', {})
    quota = telemetry.get('quota', {})
    rates = telemetry.get('traffic', {}).get('rate_distributions', {}).get('24h', {})
    planned = _number(summary.get('current_plan_requests'))
    unbatched = _number(summary.get('unbatched_requests'))
    cap = _number(quota.get('paced_requests_per_hour'))
    official = _number(quota.get('official_requests_per_hour'))
    reserved = _number(quota.get('reserved_requests_per_hour'))
    cap = max(0.0, min(cap, official) - reserved) if None not in (cap, official, reserved) else None
    fastest_rate = cap
    observed_middle, observed_slow = _number(rates.get('p50')), _number(rates.get('p10'))
    central_rate = min(cap, max(0.0, observed_middle - reserved)) if None not in (cap, observed_middle, reserved) else None
    slow_rate = min(central_rate, max(0.0, observed_slow - reserved)) if None not in (central_rate, observed_slow, reserved) else None
    counts = {key: summary.get(key) for key in ('required_requests', 'validation_requests', 'unbatched_requests',
              'blocked_tasks', 'unscheduled_datasets', 'unknown_datasets', 'inflight_tasks', 'local_derived_tasks', 'batch_savings')}
    counts['planned_requests'] = int(planned) if planned is not None else None
    blockers: list[dict[str, Any]] = []
    for field, reason in (
        ('blocked_tasks', '權限、參數或端點阻塞不包含在有限工時中。'),
        ('unscheduled_datasets', '未排程來源沒有工作分母，不能聲稱全來源完工。'),
        ('unknown_datasets', '尚有未知工作範圍；預估不含未發現的商品與歷史。'),
    ):
        count = _number(summary.get(field))
        if count:
            blockers.append({'code': field, 'count': int(count), 'reason': reason})
    if summary.get('validation_requests'):
        blockers.append({'code': 'secondary_admission', 'count': summary['validation_requests'],
                         'reason': '次要校驗須等必要抓取完成及跨來源准入條件通過。'})
    tail = sum(_number(summary.get(key)) or 0 for key in ('inflight_tasks', 'local_derived_tasks'))
    if tail:
        blockers.append({'code': 'unmeasured_tail', 'count': int(tail),
                         'reason': '假設在途回應與本機衍生能於網路佇列清空前完成；未量測的尾端處理可能延長。'})
    estimate = {
        'schema_version': 1, 'state': 'conditional', 'scope_label': SCOPE_LABEL,
        'observed_at_utc': now.isoformat(), 'valid_until_utc': (now + timedelta(minutes=5)).isoformat(),
        'is_guaranteed': False, 'absolute_slowest_seconds': None,
        'workload': counts, 'scenarios': {}, 'blockers': blockers,
        'basis': '合批後工作量 ÷ 共用帳號可用請求吞吐；加計開盤保護時段。',
        'assumptions': [
            '最快為共用額度與現行限速可達的理想情境；不是保證能達到的速度。',
            '中間／最慢採近 24 小時完整且非零的 15 分鐘窗，換算每小時發出速度的 P50／P10；不是完成機率。',
            '最慢另假設退回逐任務請求；它是保守情境，不是不可超過的期限。',
            '所有情境均假設請求成功、權限有效、持續運行及次要校驗准入；額外重試與停機會延長。',
            '範圍固定為當前已登記可執行佇列；預留本次追新需求，不含未來新公告或新商品工作。',
        ],
        'rate_evidence': {'official_requests_per_hour': official, 'paced_requests_per_hour': quota.get('paced_requests_per_hour'),
                          'reserved_requests_per_hour': reserved, 'observed_active_bins': rates.get('active_bin_count'),
                          'observed_idle_bins': rates.get('idle_bin_count'),
                          'dispatch_rate_p50': observed_middle, 'dispatch_rate_p10': observed_slow,
                          'success_rate': None, 'success_rate_basis': 'no_complete_request_outcome_ledger'},
    }
    if rates.get('idle_bin_count'):
        estimate['assumptions'].append(
            f"觀察窗有 {rates['idle_bin_count']} 個完全閒置區間未納入活躍速度；持續停機不在有限估時內。")
    budget = quota.get('current_budget')
    if isinstance(budget, dict) and budget.get('allowed') is False:
        estimate['blockers'].append({'code': 'quota_pause', 'count': 1,
                                    'reason': '目前等待共用額度；以下情境假設恢復服務，未知等待時間仍須順延。'})
    if day_is_protected is None:
        # Pure tests can pass a calendar; runtime always supplies the same
        # receipt/schedule-backed decision as the existing worker.
        day_is_protected = lambda day: day.weekday() < 5
        estimate['assumptions'].append('未提供交易日曆；暫按平日保護時段估算。')
    ready = (workload.get('state') in {'observed', 'partial'} and planned is not None
             and unbatched is not None and quota.get('account_fresh') is True)
    if workload.get('state') == 'partial':
        estimate['scope_label'] = 'FinMind 已知可盤點佇列小計（部分來源缺少證據）'
    cooldown = _number(summary.get('max_retry_wait_seconds')) or 0
    for name, requests, rate, basis in (
        ('fastest', planned, fastest_rate, '現行合批／限速上限，扣除追新預留'),
        ('central', planned, central_rate, '現行合批／近 24h 活躍 15 分鐘窗 P50 發出吞吐'),
        ('slowest', max(unbatched, planned) if None not in (unbatched, planned) else None,
         slow_rate, '逐任務請求／近 24h 活躍 15 分鐘窗 P10 發出吞吐'),
    ):
        scenario = {'state': 'unknown', 'remaining_seconds': None, 'estimated_complete_at_utc': None,
                    'request_count': int(requests) if requests is not None else None,
                    'effective_requests_per_hour': rate, 'basis': basis}
        if ready and requests is not None and (requests == 0 or rate is not None and rate > 0):
            duration = requests * 3600 / rate if requests else 0
            # Already-issued requests/derivations still need accepted receipts;
            # zero additional requests never implies these completed instantly.
            if requests == 0 and (summary.get('inflight_tasks') or summary.get('local_derived_tasks')):
                scenario['basis'] = '無額外網路請求，但仍等待在途回應／本機衍生收據'
            else:
                try:
                    # Cooling can overlap other work; the conservative scenario
                    # waits first. No scenario can finish before the last known
                    # deferred task becomes eligible and receives service.
                    start = now + timedelta(seconds=cooldown if name == 'slowest' else 0)
                    finish, pause = scheduled_finish(start, duration, day_is_protected=day_is_protected)
                    if requests and cooldown:
                        retry_finish, _ = scheduled_finish(now + timedelta(seconds=cooldown),
                                                          3600 / rate, day_is_protected=day_is_protected)
                        finish = max(finish, retry_finish)
                except ValueError:
                    scenario['basis'] = '超過一年推估範圍；需更長期吞吐與日曆證據'
                else:
                    scenario.update(state='estimated', remaining_seconds=math.ceil((finish-now).total_seconds()),
                                    estimated_complete_at_utc=finish.isoformat(), opening_pause_seconds=pause)
        estimate['scenarios'][name] = scenario
    if not ready:
        estimate['state'] = 'unavailable'
        estimate['basis'] = '佇列或帳戶額度證據缺少／過期，不將未知工作當成零。'
    elif planned == 0 and all(x['remaining_seconds'] == 0 for x in estimate['scenarios'].values()):
        estimate['state'] = 'conditional' if blockers else 'current'
    elif any(x['state'] == 'unknown' for x in estimate['scenarios'].values()):
        estimate['state'] = 'warming_up'
    return estimate


def snapshot_finmind_estimate(root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    from downloader.finmind_eta_work import build_finmind_workload
    from downloader.finmind_eta_telemetry import build_finmind_eta_telemetry
    from stockagent.live.market_status import tw_stock_day_decision

    now = now or datetime.now(UTC)
    workload = build_finmind_workload(root, now)
    telemetry = build_finmind_eta_telemetry(root, now)
    public_root = Path(__file__).resolve().parents[1] / 'data_tw_public'
    calendar: dict[str, str] = {}
    def protected(day: date) -> bool:
        if day.isoformat() not in calendar:
            decision = tw_stock_day_decision(day, parquet_root=public_root, observed=now)
            calendar[day.isoformat()] = decision.status
        state = calendar[day.isoformat()]
        return state != 'closed' and (state in {'actual_open', 'scheduled_open'} or day.weekday() < 5)
    estimate = estimate_completion(workload, telemetry, now, day_is_protected=protected)
    estimate['calendar_states'] = calendar
    if 'unknown' in calendar.values():
        estimate['assumptions'].append('部分日期缺少官方日曆證據，依現行下載器以平日套用開盤保護。')
    evidence = {'schema_version': 1, 'estimate': estimate, 'workload': workload, 'telemetry': telemetry}
    atomic_write_json(root / 'eta_status.json', evidence)
    return estimate
