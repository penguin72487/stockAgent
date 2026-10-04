"""TEJ acquisition scenarios from bounded metadata, never from source values.

The unit of service is a complete desktop query (including empty responses),
not an exported row. Lazy queries are real work; superseded encodings are not.
Unseen axes are modeled separately and never certify history completeness.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
import hashlib
import json
import math

CONTRACT = "tej_staged_query_scenarios_v1"
SCENARIOS = ("fast", "middle", "slow")
QUANTILES = (.10, .50, .90)
PHASES = ("P1", "P2", "P3")


def quantile(values: list[float], fraction: float) -> float | None:
    """Linear empirical quantile; these are scenarios, not confidence bounds."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    return ordered[lower] + (ordered[math.ceil(position)] - ordered[lower]) * (position - lower)


def query_geometry(companies: int, dates: int, fields: int, keys: int,
                   config: dict) -> tuple[int, int]:
    """Closed-form count of the existing rectangular planner, without its JSON.

    Only for undiscovered scenarios: verified lazy plans supply exact counts.
    No arrays, Cartesian products, source calls, or assumed native rows.
    """
    if min(companies, fields) < 1 or keys not in (1, 2, 3) or dates < 0:
        return 0, 0
    if keys != 1 and dates == 0:
        return 0, 0
    width = 30 - keys
    full, tail = divmod(fields, width)
    from downloader.tej_planning import TILING_CONTRACT, company_batch_size, rectangle_queries

    queries = 0
    for batch_width, batches in ((width, full), (tail, bool(tail))):
        if not batches:
            continue
        rows = min(config["max_rows_per_export"], config["max_cells_per_export"] // (batch_width + keys) - 1)
        size = company_batch_size(companies, dates, rows, config["max_companies_per_export"], keys,
                                  optimize=config.get('query_tiling_contract') == TILING_CONTRACT)
        queries += int(batches) * rectangle_queries(companies, dates, rows, size, keys)
    return queries, math.ceil(fields / width) * companies * (1 if keys == 1 else dates)


def _pool(target: dict, records: list[dict], *, axis: str | None = None) -> tuple[list[dict], str]:
    if axis == "companies":
        choices = (("same_universe", "query_type"), ("same_category", "category"))
    else:
        choices = (("same_frequency", "frequency"),)
    for label, key in choices:
        selected = [r for r in records if r.get(key) == target.get(key)]
        if selected:
            return selected, label
    return records, "cross_table_fallback"


def _metrics(samples: list[dict], overhead: float, *, start_interval: bool = False) -> dict | None:
    # Repeated small exports from one table must not dominate all other tables.
    grouped = defaultdict(list)
    for sample in samples:
        if (sample.get("timing_basis") == "fresh_end_to_end"
                and isinstance(sample.get("seconds"), (int, float))
                and math.isfinite(sample["seconds"]) and sample["seconds"] > 0):
            grouped[sample["table_id"]].append(sample)
    if not grouped:
        return None
    durations = {scenario: quantile([(max(quantile([s['seconds'] for s in members], q),overhead)
                                      if start_interval else quantile([s['seconds'] for s in members], q) + overhead)
                                    for members in grouped.values()], q)
                 for scenario, q in zip(SCENARIOS, QUANTILES)}
    density, byte_cost = [], []
    for members in grouped.values():
        measured = [s for s in members if s.get("expected_rows", 0) and s.get("actual_rows") is not None]
        if measured:
            # Ratio of sums, not an unweighted mean of batch densities.
            denominator = sum(s["expected_rows"] for s in measured)
            density.append(sum(s["actual_rows"] for s in measured) / denominator)
            byte_cost.append(sum(s["actual_bytes"] or 0 for s in measured) / len(measured))
    return {"seconds_per_query": durations, "samples": sum(map(len, grouped.values())),
            "tables": len(grouped),
            "rows_per_work_row": dict(zip(SCENARIOS, (quantile(density, q) for q in QUANTILES))),
            "bytes_per_query": dict(zip(SCENARIOS, (quantile(byte_cost, q) for q in QUANTILES)))}


def _sum(rows: list[dict], key: str, scenario: str) -> float | None:
    values = [r[key][scenario] for r in rows]
    return None if any(v is None for v in values) else sum(values)


def _milestone(needed: list[dict], all_work: list[dict], barrier: str | int, scenario: str,
               *, ordering_key: str = 'phase') -> float | None:
    """Round-robin table milestone under the canonical single-owner scheduler.

    Earlier owner phases drain first. In the final owner phase, each table
    receives one query per round and leaves when drained. Discoveries are
    conservatively charged before those rounds; the real worker interleaves.
    We do not iterate millions of virtual queries or assume they run parallel.
    """
    if not needed:
        return 0
    if ordering_key == 'collection_priority' and all(
            r['remaining_queries'][scenario] == 0 and r['discovery_seconds'][scenario] == 0 for r in needed):
        return 0
    final = [r for r in needed if r[ordering_key] == barrier]
    rounds = [r['remaining_queries'][scenario] for r in final]
    if any(n is None for n in rounds):
        return None
    horizon = max(rounds, default=0)
    seconds = 0
    for row in all_work:
        if row[ordering_key] > barrier:
            continue
        queries = row['remaining_queries'][scenario]
        discovery = row['discovery_seconds'][scenario]
        if discovery is None:
            return None
        seconds += discovery
        if row[ordering_key] < barrier:
            service = row['download_seconds'][scenario]
        elif horizon == 0:
            service = 0
        elif queries is None or row['download_seconds'][scenario] is None:
            return None
        else:
            service = min(queries, horizon) / queries * row['download_seconds'][scenario] if queries else 0
        if service is None:
            return None
        seconds += service
    return seconds


def build_staged_eta(tables: list[dict], samples: list[dict], config: dict,
                     *, observed: datetime, cutoff: str | None, alive: bool,
                     interface_blocked: bool = False) -> dict:
    """Project query workloads and stage dependency barriers, without mutation."""
    overhead = config.get("minimum_export_interval_seconds", 0)
    interval_contract = config.get('query_interval_contract','minimum_completion_gap_v1')
    if interval_contract not in ('minimum_completion_gap_v1','minimum_query_start_interval_v1'):
        raise ValueError('Unreviewed TEJ query interval contract')
    start_interval = interval_contract == 'minimum_query_start_interval_v1'
    samples = [s for s in samples if s.get('timing_basis') == 'fresh_end_to_end'
               and isinstance(s.get('seconds'), (int, float)) and math.isfinite(s['seconds']) and s['seconds'] > 0]
    downloads = [s for s in samples if s.get("kind") == "download"]
    discoveries = [s for s in samples if s.get("kind") == "discover"]
    global_download = _metrics(downloads, overhead, start_interval=start_interval)
    global_discovery = _metrics(discoveries, overhead, start_interval=start_interval)
    per_table_samples = defaultdict(list)
    for sample in downloads:
        per_table_samples[sample['table_id']].append(sample)
    frequency_models = {}
    known_axes = [t for t in tables if t.get("grid_rows") is not None and t.get("fields", 0) > 0]
    company_axes = [t for t in known_axes if t.get("universe_count", 0) and t["universe_count"] > 0]
    date_axes = [t for t in known_axes if t.get("grid_dates", 0) and t["grid_dates"] > 0]
    geometry_available = all(type(config.get(k)) is int and config[k] > 0 for k in
                             ("max_rows_per_export", "max_cells_per_export", "max_companies_per_export"))
    table_forecasts = []
    for table in tables:
        own = per_table_samples[table['table_id']]
        frequency = table.get('frequency')
        if table.get("fields", 0) == 0:
            remaining_queries, remaining_rows = dict.fromkeys(SCENARIOS, 0), dict.fromkeys(SCENARIOS, 0)
            geometry_basis = "empty_field_menu_excluded_not_certified"
        elif table.get("grid_rows") is not None:
            count = table.get("remaining_download_queries")
            rows = max(0, table["grid_rows"] - table["resolved_grid_rows"])
            remaining_queries, remaining_rows = dict.fromkeys(SCENARIOS, count), dict.fromkeys(SCENARIOS, rows)
            geometry_basis = "verified_plan_and_live_queue"
        else:
            companies, company_basis = _pool(table, company_axes, axis="companies")
            periods, date_basis = _pool(table, date_axes, axis="dates")
            remaining_queries, remaining_rows = dict.fromkeys(SCENARIOS), dict.fromkeys(SCENARIOS)
            geometry_basis = company_basis + "/" + date_basis + "/assumed_key2_until_discovery"
            if geometry_available and companies and periods:
                for scenario, q in zip(SCENARIOS, QUANTILES):
                    count = max(1, round(quantile([t["universe_count"] for t in companies], q)))
                    dates = max(1, round(quantile([t["grid_dates"] for t in periods], q)))
                    # No weekly axis yet: don't mislabel a daily axis as weekly.
                    if date_basis == "cross_table_fallback":
                        geometry_basis += "/frequency_unverified"
                    remaining_queries[scenario], remaining_rows[scenario] = query_geometry(
                        count, dates, table["fields"], table.get("source_key_mode") or 2, config)
        typical_rows = (remaining_rows['middle'] / remaining_queries['middle']
                        if remaining_queries['middle'] and remaining_rows['middle'] is not None else None)
        # Pilot queries of one company/day are not representative of a nearly
        # 10,000-row bulk query. Match scope magnitude before pooling timings
        # or byte costs; don't multiply their per-row receipt overhead.
        magnitude = math.floor(math.log2(max(1, typical_rows))) if typical_rows is not None else None
        key = (frequency, magnitude)
        if key not in frequency_models:
            lower, upper = ((2 ** magnitude / 4, 2 ** (magnitude + 1) * 4)
                            if magnitude is not None else (0, math.inf))
            matched = [s for s in downloads if lower <= (s.get('expected_rows') or 0) <= upper]
            pool, basis = _pool(table, matched or downloads)
            frequency_models[key] = (_metrics(pool, overhead, start_interval=start_interval) or global_download,
                                     basis + ('/similar_scope' if matched else '/scope_unverified'), lower, upper)
        model, timing_basis, lower, upper = frequency_models[key]
        matched_own = [s for s in own if lower <= (s.get('expected_rows') or 0) <= upper]
        if len(matched_own) >= 2:
            model, timing_basis = _metrics(matched_own, overhead, start_interval=start_interval), 'same_table/similar_scope'
        discovery_count = table.get("remaining_discovery_tasks", 0)
        seconds, result_rows, result_bytes, download_times, discovery_times = {}, {}, {}, {}, {}
        for scenario in SCENARIOS:
            queries, work_rows = remaining_queries[scenario], remaining_rows[scenario]
            cost = model["seconds_per_query"][scenario] if model else None
            discovery_cost = global_discovery["seconds_per_query"][scenario] if global_discovery else None
            download_seconds = 0 if queries == 0 else queries * cost if queries is not None and cost is not None else None
            discovery_seconds = 0 if discovery_count == 0 else discovery_count * discovery_cost if discovery_cost is not None else None
            download_times[scenario], discovery_times[scenario] = download_seconds, discovery_seconds
            seconds[scenario] = (download_seconds + discovery_seconds
                                 if download_seconds is not None and discovery_seconds is not None else None)
            density = model["rows_per_work_row"][scenario] if model else None
            byte_cost = model["bytes_per_query"][scenario] if model else None
            result_rows[scenario] = (0 if work_rows == 0 else work_rows * density
                                     if work_rows is not None and density is not None else None)
            result_bytes[scenario] = (0 if queries == 0 else queries * byte_cost
                                      if queries is not None and byte_cost is not None else None)
        table_forecasts.append({"table_id": table["table_id"], "phase": table["phase"],
            "collection_priority": table.get('collection_priority'),
            "field_phase_counts": table["field_phase_counts"], "axis_verified": table.get("grid_rows") is not None,
            "remaining_queries": remaining_queries, "remaining_work_rows": remaining_rows,
            "remaining_seconds": seconds, "remaining_export_rows": result_rows, "remaining_local_bytes": result_bytes,
            "download_seconds": download_times, "discovery_seconds": discovery_times,
            "geometry_basis": geometry_basis, "timing_basis": timing_basis,
            "timing_samples": model["samples"] if model else 0,
            "discovery_tasks": discovery_count, "blocked_tasks": table.get("blocked_tasks", 0),
            "deferred_tasks": table.get('deferred_tasks',0),
            "resolved_grid_rows": table["resolved_grid_rows"], "exported_rows": table["exported_rows"],
            "recorded_bytes": table["recorded_bytes"]})
    scenarios = {scenario: {"remaining_seconds": _sum(table_forecasts, "remaining_seconds", scenario),
                            "remaining_queries": _sum(table_forecasts, "remaining_queries", scenario),
                            "remaining_work_rows": _sum(table_forecasts, "remaining_work_rows", scenario),
                            "remaining_export_rows": _sum(table_forecasts, "remaining_export_rows", scenario),
                            "remaining_local_bytes": _sum(table_forecasts, "remaining_local_bytes", scenario)}
                 for scenario in SCENARIOS}
    phases = []
    value_order = bool(table_forecasts) and all(type(r['collection_priority']) is int for r in table_forecasts)
    for phase in PHASES:
        own = [r for r in table_forecasts if r["phase"] == phase]
        needed = [r for r in table_forecasts if r["field_phase_counts"].get(phase, 0) > 0]
        # Scheduler completes earlier phases first. Bundled fields depend on
        # their owner phase, not a second download or zero-time completion.
        barrier = max((r["phase"] for r in needed), default=phase)
        priority_barrier = max((r['collection_priority'] for r in needed), default=0) if value_order else None
        phase_scenarios = {}
        for scenario in SCENARIOS:
            remaining = _milestone(needed, table_forecasts, priority_barrier if value_order else barrier, scenario,
                                   ordering_key='collection_priority' if value_order else 'phase')
            phase_scenarios[scenario] = {
                "additional_queries": _sum(own, "remaining_queries", scenario),
                "additional_seconds": _sum(own, "remaining_seconds", scenario),
                "dependency_remaining_seconds": remaining,
                "if_started_now_complete_at_utc": (observed + timedelta(seconds=remaining)).isoformat()
                    if remaining is not None and 0 <= remaining < 100 * 365.25 * 86400 else None,
                "dependency_remaining_work_rows": _sum(needed, "remaining_work_rows", scenario),
                "dependency_remaining_local_bytes": _sum(needed, "remaining_local_bytes", scenario),
                "dependency_remaining_export_rows": _sum(needed, "remaining_export_rows", scenario),
            }
        phases.append({"phase": phase, "candidate_fields": sum(r["field_phase_counts"].get(phase, 0) for r in needed),
            "milestone_order_basis": "local_gap_value_priority" if value_order else "legacy_phase_priority",
            "priority_dependency_barrier": priority_barrier,
            "owner_tables": len(own), "dependency_tables": len(needed), "parent_phase_barrier": barrier,
            "bundled_into_earlier_tables": sum(r["phase"] < phase for r in needed),
            "undiscovered_dependency_tables": sum(not r["axis_verified"] for r in needed),
            "blocked_dependency_tasks": sum(r["blocked_tasks"] for r in needed),
            "deferred_dependency_tasks": sum(r['deferred_tasks'] for r in needed),
            "dependency_resolved_grid_rows": sum(r['resolved_grid_rows'] for r in needed),
            "dependency_exported_rows": sum(r['exported_rows'] for r in needed),
            "dependency_recorded_bytes": sum(r['recorded_bytes'] for r in needed),
            "completion_scope": "validation_candidates_only_not_cross_source_validation" if phase == "P3" else "query_scope_not_native_history",
            "cross_source_validation_remaining_seconds": None if phase == "P3" else 0,
            "scheduled_complete_at_utc": None, "scenarios": phase_scenarios})
    known = [r for r in table_forecasts if r["axis_verified"]]
    discovery_pending = sum(t.get("remaining_discovery_tasks", 0) for t in tables)
    provenance = {"contract": CONTRACT, "cutoff": cutoff, "tables": tables, "samples": samples,
                  "bounds": {k: config.get(k) for k in ("max_rows_per_export", "max_cells_per_export",
                              "max_companies_per_export", "minimum_export_interval_seconds")}}
    provenance['bounds']['query_tiling_contract'] = config.get('query_tiling_contract')
    provenance['bounds']['query_interval_contract'] = interval_contract
    fingerprint = hashlib.sha256(json.dumps(provenance, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()).hexdigest()
    return {"contract": CONTRACT, "as_of_utc": observed.isoformat(), "history_cutoff": cutoff,
            "input_sha256": fingerprint, "execution_state": "finite_batch_running" if alive else "not_running",
            "scheduled_complete_at_utc": None, "desktop_parallelism": 1,
            "global_scenarios": scenarios, "phases": phases, "table_forecasts": table_forecasts,
            "known_remaining_queries": _sum(known, "remaining_queries", "middle"),
            "known_remaining_seconds": {s: _sum(known, "remaining_seconds", s) for s in SCENARIOS},
            "discovery_remaining_tasks": discovery_pending,
            "discovery_only_seconds": {s: discovery_pending * global_discovery["seconds_per_query"][s]
                                       if global_discovery else None for s in SCENARIOS},
            "modeled_axis_tables": sum(not r["axis_verified"] and bool(t.get("fields")) for r, t in zip(table_forecasts, tables)),
            "download_timing_samples": global_download["samples"] if global_download else 0,
            "download_timing_tables": global_download["tables"] if global_download else 0,
            "discovery_timing_samples": global_discovery["samples"] if global_discovery else 0,
            "blocked_tasks": sum(t.get("blocked_tasks", 0) for t in tables),
            "interface_blocked": interface_blocked, "official_quota_wait_seconds": None,
            "scenario_quantiles": dict(zip(SCENARIOS, QUANTILES)), "statistical_confidence_interval": False,
            "assumptions": ["單一桌面持有者，若從現在連續 24 小時執行；有限批次並非持續排程",
                "較快／中間／較慢是同表或相近表實測情境，不是保證或統計信賴區間",
                "未清點歷史軸以同 universe／頻率推估；缺同類證據則跨表外推，Key 暫假設 2",
                ("依本機缺口與資料價值優先序、同價值逐表輪轉推算；P1/P2/P3是覆蓋分類而非執行先後，清點工時前置計入" if value_order else
                 "依既有階段順序與逐表輪轉推算最後依賴表完成；清點工時前置計入，實際仍交錯執行"),
                "含完整查詢、驗證、保存、收據與工作間隔；不把恢復舊結果當下載速度",
                "假設來源故障已修復；官方配額、桌面空檔與外部修復等待未知，未計入",
                "筆數與容量是稀疏樣本外推，本機含原始匯出＋Parquet，不是網路流量",
                "只估固定 cutoff 的既有範圍；原生歷史完整、精度、訓練可用與多源校驗另需驗證"]}
