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
SNAPSHOT_CONTRACT_VERSION = 12  # Independent retry lanes and standalone stage cost evidence.


def _number(value: Any) -> float | None:
    return float(value) if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def scheduled_finish(now: datetime, active_seconds: float, *,
                     day_is_protected: Callable[[date], bool]) -> tuple[datetime, int]:
    """Convert useful service time to wall time with canonical opening breaks."""
    if now.tzinfo is None or not math.isfinite(active_seconds) or active_seconds < 0:
        raise ValueError('aware timestamp and finite nonnegative duration required')
    if active_seconds > 10 * 366 * 86400:
        raise ValueError('estimate exceeds ten-year projection horizon')
    remaining = active_seconds
    cursor = now.astimezone(TAIPEI)
    pause = 0.0
    for _ in range(3 * 10 * 366 + 4):
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
                        day_is_protected: Callable[[date], bool] | None = None,
                        secondary_admission: dict[str, Any] | None = None,
                        _project_calendar: bool = True) -> dict[str, Any]:
    if now.tzinfo is None:
        raise ValueError('now must be timezone-aware')
    now = now.astimezone(UTC)
    summary = workload.get('summary', {})
    quota = telemetry.get('quota', {})
    distributions = telemetry.get('traffic', {}).get('rate_distributions', {})
    # Same trailing (now - 60m, now] population as the dashboard, including
    # idle time. Aligned-bin medians are NOT a rolling-hour request count.
    recent = distributions.get('1h', {})
    window = '1h' if recent.get('complete_bin_count', 0) == 4 else '24h'
    rates = distributions.get(window, {})
    rolling = telemetry.get('traffic', {}).get('windows', {}).get('1h', {})
    planned = _number(summary.get('current_plan_requests'))
    unbatched = _number(summary.get('unbatched_requests'))
    cap = _number(quota.get('paced_requests_per_hour'))
    official = _number(quota.get('official_requests_per_hour'))
    reserved = _number(quota.get('forecast_recurring_requests_per_hour', quota.get('reserved_requests_per_hour')))
    cap = max(0.0, min(cap, official) - reserved) if None not in (cap, official, reserved) else None
    fastest_rate = cap
    observed_middle = (_number(rolling.get('wall_requests_per_hour'))
                       if rolling.get('complete_window') is True else None)
    observed_slow = _number(rates.get('wall_p10'))
    central_rate = min(cap, max(0.0, observed_middle - reserved)) if None not in (cap, observed_middle, reserved) else None
    slow_rate = min(central_rate, max(0.0, observed_slow - reserved)) if None not in (central_rate, observed_slow, reserved) else None
    processing = telemetry.get('request_processing_rates')
    if processing is not None:
        # A signed whole-day file can take seconds/minutes per single quota
        # grant. Old JSON call rates are not evidence of that transfer speed.
        measured = processing.get('rates', {})
        if measured.get('fastest') is not None and fastest_rate is not None:
            fastest_rate = min(fastest_rate, measured['fastest'])
        central_rate = (min(central_rate, measured['central'])
                        if central_rate is not None and measured.get('central') is not None else None)
        slow_rate = (min(slow_rate, measured['slowest'])
                     if slow_rate is not None and measured.get('slowest') is not None else None)
    counts = {key: summary.get(key) for key in ('required_requests', 'validation_requests', 'unbatched_requests',
              'blocked_tasks', 'unscheduled_datasets', 'unknown_datasets', 'inflight_tasks', 'local_derived_tasks',
              'batch_savings', 'calendar_wait_tasks', 'retry_tasks', 'retry_exhausted_tasks', 'candidate_requests',
              'independent_retry_tasks', 'independent_retry_requests', 'independent_inflight_tasks')}
    counts['planned_requests'] = int(planned) if planned is not None else None
    blockers: list[dict[str, Any]] = []
    for field, reason in (
        ('blocked_tasks', '權限、參數或端點阻塞不包含在有限工時中。'),
        ('unscheduled_datasets', '未排程來源沒有工作分母，不能聲稱全來源完工。'),
        ('unknown_datasets', '尚有未知工作範圍；預估不含未發現的商品與歷史。'),
        ('retry_exhausted_tasks', '重試已達上限，保留資料與失敗紀錄；不再自動重試，也不算抓取完成。此估時只涵蓋可自動排程工作。'),
    ):
        count = _number(summary.get(field))
        if count:
            blockers.append({'code': field, 'count': int(count), 'reason': reason})
    admission_blocked = bool(summary.get('validation_requests') and
                             (secondary_admission or {}).get('allowed') is not True)
    if admission_blocked:
        blockers.append({'code': 'secondary_admission', 'count': summary['validation_requests'],
                         'reason': '次要校驗尚未放行；先完成適用範圍的必要抓取。放行時間未知，不提供日曆完工時間。'})
    tail = sum(_number(summary.get(key)) or 0 for key in ('inflight_tasks', 'local_derived_tasks'))
    if tail:
        blockers.append({'code': 'unmeasured_tail', 'count': int(tail),
                         'reason': '假設在途回應與本機衍生能於網路佇列清空前完成；未量測的尾端處理可能延長。'})
    estimate = {
        'schema_version': 3, 'state': 'conditional', 'scope_label': SCOPE_LABEL,
        'observed_at_utc': now.isoformat(), 'valid_until_utc': (now + timedelta(minutes=5)).isoformat(),
        'is_guaranteed': False, 'absolute_slowest_seconds': None,
        'workload': counts, 'scenarios': {}, 'blockers': blockers,
        'basis': '固定截止日剩餘工作量 ÷ 共用吞吐扣除未來週期性增量；當前追新欠帳僅計入分子一次。',
        'assumptions': [
            '最快為共用額度與現行限速可達的理想情境；不是保證能達到的速度。',
            '中間採同一取樣時點的滾動 60 分鐘實發請求數，含閒置時間；未滿一小時不外推成完整樣本。',
            f'保守採近 {window} 完整 15 分鐘窗的 P10（含零），且不快於中間；不是完成機率。',
            '最慢另假設退回逐任務請求；它是保守情境，不是不可超過的期限。',
            '所有情境均假設請求成功、權限有效、持續運行及次要校驗准入；額外重試與停機會延長。',
            '當前追新欠帳只計入工作量一次；未來追新是模型負載，不把當下額度保留量再扣一次。',
            '全量容量尚未驗證；估時假設磁碟空間足夠，既有 25 GiB 剩餘空間守門不會被略過。',
        ],
        'rate_evidence': {'request_processing': processing,
                          'official_requests_per_hour': official, 'paced_requests_per_hour': quota.get('paced_requests_per_hour'),
                          'reserved_requests_per_hour': reserved, 'observed_active_bins': rates.get('active_bin_count'),
                          'observed_idle_bins': rates.get('idle_bin_count'),
                          'dispatch_rate_p50': _number(rates.get('p50')), 'dispatch_rate_p10': observed_slow,
                          'observation_window': 'rolling_60m', 'conservative_window': window,
                          'rolling_requests_60m': _number(rolling.get('attempts')),
                          'rolling_complete_window': rolling.get('complete_window') is True,
                          'rolling_window_start_at_utc': rolling.get('started_at_utc'),
                          'rolling_window_end_at_utc': rolling.get('ended_at_utc', now.isoformat()),
                          'gross_requests_per_hour': observed_middle,
                          'effective_requests_per_hour': central_rate,
                          'current_reserved_requests': _number(quota.get('reserved_requests_per_hour')),
                          'future_recurring_requests_per_hour': reserved,
                          'success_rate': None, 'success_rate_basis': 'no_complete_request_outcome_ledger'},
    }
    if rates.get('idle_bin_count'):
        estimate['assumptions'].append(
            f"保守觀察窗含 {rates['idle_bin_count']} 個完全閒置區間；若淨速度為零，不能給有限完成日。")
    budget = quota.get('current_budget')
    quota_blocked = (isinstance(budget, dict) and budget.get('allowed') is False
                     and not (summary.get('incremental_requests', 0) > 0 and budget.get('remaining', 0) > 0))
    if quota_blocked:
        estimate['blockers'].append({'code': 'quota_pause', 'count': 1,
                                    'reason': '目前等待共用額度；恢復時間尚未驗證，不提供日曆完工時間。'})
    estimate['admission'] = {
        'allowed': (secondary_admission or {}).get('allowed') is True,
        'reason': str((secondary_admission or {}).get('reason', 'not_observed'))[:120],
    }
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
    retry_pending = bool(_number(summary.get('retry_tasks')))
    if retry_pending:
        estimate['blockers'].append({
            'code': 'retry_tasks', 'count': summary['retry_tasks'],
            'reason': '失敗／部分回應仍待重試；請求發出速度不等於成功消化速度。重試成功前不提供完工倒數。',
        })
    estimate['retry_wait_seconds'] = cooldown if retry_pending else None
    for name, requests, rate, basis in (
        ('fastest', planned, fastest_rate, '現行合批／限速上限，扣除追新預留'),
        ('central', planned, central_rate, '現行合批／滾動 60 分鐘實發吞吐扣除本階段未來追新'),
        ('slowest', max(unbatched, planned) if None not in (unbatched, planned) else None,
         slow_rate, f'逐任務請求／近 {window} 完整 15 分鐘窗 P10（含閒置）扣除追新'),
    ):
        scenario = {'state': 'unknown', 'remaining_seconds': None, 'estimated_complete_at_utc': None,
                    'active_work_seconds': None,
                    'request_count': int(requests) if requests is not None else None,
                    'effective_requests_per_hour': rate, 'basis': basis}
        if ready and requests is not None and (requests == 0 or rate is not None and rate > 0):
            duration = requests * 3600 / rate if requests else 0
            # Already-issued requests/derivations still need accepted receipts;
            # zero additional requests never implies these completed instantly.
            if requests == 0 and (summary.get('inflight_tasks') or summary.get('local_derived_tasks')):
                scenario['basis'] = '無額外網路請求，但仍等待在途回應／本機衍生收據'
            else:
                scenario['active_work_seconds'] = math.ceil(duration)
                if admission_blocked or quota_blocked or retry_pending:
                    scenario.update(state='waiting_admission' if admission_blocked else
                                    'waiting_quota' if quota_blocked else 'waiting_retry',
                                    basis=basis + '；僅為成功後有效工時，不含未知等待與額外重試')
                    estimate['scenarios'][name] = scenario
                    continue
                if requests and not _project_calendar:
                    # Ordered stages own the calendar cursor. Computing an
                    # independent date here and throwing it away would repeat
                    # years of date walking for every stage and retry model.
                    scenario['state'] = 'work_ready'
                    estimate['scenarios'][name] = scenario
                    continue
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
                    scenario['basis'] = '超過十年日曆投影範圍；保留有效工時，不捏造遠期完成日'
                else:
                    scenario.update(state='estimated', remaining_seconds=math.ceil((finish-now).total_seconds()),
                                    estimated_complete_at_utc=finish.isoformat(), opening_pause_seconds=pause)
        estimate['scenarios'][name] = scenario
    if not ready:
        estimate['state'] = 'unavailable'
        estimate['basis'] = '佇列或帳戶額度證據缺少／過期，不將未知工作當成零。'
    elif admission_blocked or quota_blocked or retry_pending:
        estimate['state'] = ('waiting_admission' if admission_blocked else
                             'waiting_quota' if quota_blocked else 'waiting_retry')
        estimate['basis'] = '工作尚未放行或尚未成功重試；有效工時與未知等待分列，不能由現在起算完工。'
    elif planned == 0 and all(x['remaining_seconds'] == 0 for x in estimate['scenarios'].values()):
        estimate['state'] = 'conditional' if blockers else 'current'
    elif any(x['state'] == 'unknown' for x in estimate['scenarios'].values()):
        estimate['state'] = ('conditional' if all(x['active_work_seconds'] is not None for x in estimate['scenarios'].values())
                             else 'warming_up')
    return estimate


def milestone_workloads(workload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Logical coverage subsets, not dispatch-order completion deadlines.

    The ordered ETA owns actual predecessor time (US minutes follow ticks).
    This legacy helper remains for callers inspecting non-tick coverage only.
    """
    from downloader.finmind_supplemental import SOURCES
    from downloader.finmind_eta_work import COUNT_FIELDS

    result = {}
    for key, label, cutoff in (
        ('core', '日資料／財報／總經／新聞（不含分鐘、分點與 tick）', 8),
        ('non_tick', '全部非 tick 資料（含分鐘與分點）', 10),
        ('all', '全部已知資料（含最低優先 tick）', 100),
    ):
        rows = [row for row in workload['datasets']
                if row['dataset'] not in SOURCES or SOURCES[row['dataset']].priority < cutoff]
        summary = {name: sum(row.get(name) or 0 for row in rows) for name in COUNT_FIELDS}
        summary.update(unknown_datasets=sum(row['state'] == 'unknown' for row in rows),
                       unscheduled_datasets=sum(row['state'] == 'unscheduled' for row in rows),
                       max_retry_wait_seconds=max((row.get('max_retry_wait_seconds', 0) for row in rows), default=0))
        result[key] = {'label': label, 'summary': summary,
                       'state': 'partial' if summary['unknown_datasets'] else 'observed'}
    return result


def snapshot_finmind_estimate(root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    from downloader.finmind_eta_stages import ordered_estimate
    from downloader.finmind_eta_work import build_finmind_workload
    from downloader.finmind_eta_telemetry import build_finmind_eta_telemetry
    from stockagent.live.market_status import tw_stock_day_decision
    from downloader.acquisition_policy import evaluate_finmind_secondary_admission

    now = now or datetime.now(UTC)
    workload = build_finmind_workload(root, now)
    telemetry = build_finmind_eta_telemetry(root, now)
    public_root = Path(__file__).resolve().parents[1] / 'data_tw_public'
    calendar: dict[str, str] = {}
    def protected(day: date) -> bool:
        if day > now.astimezone(TAIPEI).date() + timedelta(days=366):
            # No long-range official calendar is available. O(1) conservative
            # weekday breaks avoid thousands of disk-backed calendar reads.
            calendar['beyond_one_year'] = 'weekday_projection'
            return day.weekday() < 5
        if day.isoformat() not in calendar:
            decision = tw_stock_day_decision(day, parquet_root=public_root, observed=now)
            calendar[day.isoformat()] = decision.status
        state = calendar[day.isoformat()]
        return state != 'closed' and (state in {'actual_open', 'scheduled_open'} or day.weekday() < 5)
    admission = evaluate_finmind_secondary_admission(root=root, now=now)
    estimate = ordered_estimate(workload, telemetry, now, day_is_protected=protected,
                                secondary_admission=admission)
    estimate['assumptions'].append('台股現貨僅排除已驗證的休市日；上市存續期間及其他市場未知日曆仍保留為搜尋工作量上側投影，不是缺失 K 棒數。')
    estimate['assumptions'].append('超過一年的未公布日曆僅按平日保護時段投影；持續新增資料另依週期負載預留。')
    estimate['calendar_states'] = calendar
    if 'unknown' in calendar.values():
        estimate['assumptions'].append('部分日期缺少官方日曆證據，依現行下載器以平日套用開盤保護。')
    evidence = {'schema_version': 1, 'snapshot_contract_version': SNAPSHOT_CONTRACT_VERSION,
                'estimate': estimate, 'workload': workload, 'telemetry': telemetry,
                'secondary_admission': admission}
    atomic_write_json(root / 'eta_status.json', evidence)
    return estimate
