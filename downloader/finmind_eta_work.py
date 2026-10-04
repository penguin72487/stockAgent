"""Read-only request inventory for a finite, already registered FinMind plan.

An HTTP request is not a storage partition: verified range endpoints can combine
partitions, derived tables cost no requests, and provider-owner aliases must not
be added together. This module observes queues; it never migrates, claims, reads
credentials, scans Parquet, or calls a provider. Counts are planning evidence,
not independent data-completeness proofs or a promise about unknown universes.
"""

from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import json
from itertools import islice
from pathlib import Path
import sqlite3
from typing import Any

from downloader.finmind_batching import RANGE_CONTRACTS, coalesce_pending_tasks
from downloader.finmind_scheduling import TAIPEI
from downloader.finmind_runtime import idle_heartbeat


EXCLUDED_STATES = frozenset({"non_session", "not_observation_date", "deprecated_query_shape",
                             "outside_documented_range", "disabled", "delegated", "calendar_wait", "identifier_alias"})
DONE_STATES = frozenset({"complete", "observed_empty"})
PENDING_STATES = frozenset({"pending", "failed"})
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_RANGE_TASKS = 20000
FREE_STATUS_MAX_AGE = timedelta(minutes=30)
COUNT_FIELDS = ("required_requests", "incremental_requests", "backfill_requests", "validation_requests",
                "unbatched_requests", "fastest_requests", "current_plan_requests", "batch_savings",
                "completed_tasks", "pending_tasks", "blocked_tasks", "cooling_tasks", "inflight_tasks",
                "inflight_requests", "local_derived_tasks", "excluded_tasks", "uncertain_requests",
                "calendar_wait_tasks", "retry_tasks")


def _registry() -> dict[str, dict[str, Any]]:
    # One existing catalog already reconciles worker aliases and query contracts.
    # Import lazily: importing this read-only helper does not import worker SDKs.
    from scripts.audit_finmind_query_ranges import registry
    return registry()


def _json(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        body = stream.read(MAX_JSON_BYTES + 1)
    if len(body) > MAX_JSON_BYTES:
        raise ValueError("metadata_size_limit")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("metadata_not_object")
    return value


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else None
    except ValueError:
        return None


def _row(dataset: str, owner: str, contract: dict[str, Any]) -> dict[str, Any]:
    return {"dataset": dataset, "owner": owner, "query_shape": contract["query_shape"],
            "state": "observed", "basis": "readonly_primary_owner_queue_not_content_audit",
            "historical_universe_verified_complete": contract.get("historical_universe_verified_complete"),
            "earliest_retry_at_utc": None, "max_retry_wait_seconds": 0,
            **dict.fromkeys(COUNT_FIELDS, 0)}


def _work_class(state: str, priority: int, due: bool) -> str:
    if state in EXCLUDED_STATES:
        return "excluded"
    if state == "inflight":
        return "inflight"
    if state in DONE_STATES:
        # Recurring historical refreshes are operating load, not history holes.
        return "incremental" if priority == 0 and due else "completed"
    if state in PENDING_STATES:
        return "validation" if priority >= 8 else "incremental" if priority == 0 else "backfill"
    return "blocked"


def _queue(path: Path, owner: str, now: datetime) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if not path.is_file():
        return {}, {"state": "missing_queue"}
    aggregate: dict[str, dict[str, Any]] = {}
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.15)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("PRAGMA busy_timeout=150")
            conn.execute("BEGIN")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
            required = {"dataset", "data_id", "partition", "kind", "priority", "state", "next_attempt_at_utc"}
            if not required <= columns:
                raise ValueError("queue_schema_missing_columns")
            # SQL groups the large day/ID queues; only the small range subset is
            # materialized below. One snapshot keeps aggregates and grouping aligned.
            query = """SELECT dataset,kind,priority,state,
                CASE WHEN (state='pending' AND next_attempt_at_utc IS NULL)
                    OR next_attempt_at_utc<=? THEN 1 ELSE 0 END,
                count(*),min(next_attempt_at_utc),max(next_attempt_at_utc)
                FROM tasks GROUP BY dataset,kind,priority,state,5"""
            for dataset, kind, priority, state, due, count, first, last in conn.execute(query, (now.isoformat(),)):
                if any(value is not None and _stamp(value) is None for value in (first, last)):
                    raise ValueError("invalid_queue_retry_timestamp")
                item = aggregate.setdefault(dataset, {"classes": Counter(), "range_classes": Counter(),
                                                      "states": Counter(), "derived": 0, "cooling": 0,
                                                      "inflight_network": 0, "retry_first": None, "retry_last": None,
                                                      "retries_by_class": Counter(), "wait_by_class": {}})
                item["states"][state] += count
                work = _work_class(state, priority, bool(due))
                if work == 'validation' and owner == 'complement':
                    from downloader.finmind_supplemental import SOURCES as SUPPLEMENTAL_SOURCES
                    if dataset in SUPPLEMENTAL_SOURCES:
                        work = 'backfill'  # Low-priority tick is acquisition, not duplicate validation.
                if work in {"incremental", "backfill", "validation"}:
                    if state == 'failed':
                        item['retries_by_class'][work] += count
                    if kind == "derived":
                        item["derived"] += count
                    else:
                        item["classes"][work] += count
                        if kind in {"year", "month"}:
                            item["range_classes"][work] += count
                    if not due:
                        item["cooling"] += count
                        if _stamp(first):
                            item["retry_first"] = min(item["retry_first"] or first, first)
                        if _stamp(last):
                            item["retry_last"] = max(item["retry_last"] or last, last)
                            item['wait_by_class'][work] = max(item['wait_by_class'].get(work, 0),
                                                              (_stamp(last) - now).total_seconds())
                elif work == "inflight" and kind != "derived":
                    item["inflight_network"] += count
            tables = {value[0] for value in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if owner == 'complement' and 'finmind_priority_tasks' in tables:
                # The worker's finite, user-selected priority override precedes
                # normal background work. Count it in the SAME queue snapshot,
                # not as extra work or a whole-dataset priority promotion.
                for dataset, kind, state, count, last, has_id in conn.execute('''
                    SELECT t.dataset,t.kind,t.state,count(*),max(t.next_attempt_at_utc),t.data_id!=''
                    FROM tasks t WHERE t.priority!=0 AND EXISTS (SELECT 1 FROM finmind_priority_tasks p
                        WHERE p.dataset=t.dataset AND p.data_id=t.data_id AND p.partition=t.partition)
                    GROUP BY t.dataset,t.kind,t.state,6'''):
                    item = aggregate[dataset].setdefault('priority_override', {
                        'requests': 0, 'inflight_tasks': 0, 'blocked_tasks': 0,
                        'unsupported_tasks': 0, 'max_retry_wait_seconds': 0, 'retry_tasks': 0})
                    if state in PENDING_STATES:
                        from downloader.finmind_supplemental import SOURCES as SUPPLEMENTAL_SOURCES
                        source = SUPPLEMENTAL_SOURCES.get(dataset)
                        # The worker dispatches supplemental queries by the
                        # source contract, not the old queue's kind spelling.
                        # Both legacy `day` and current `id_day` cost one
                        # per-ID/day call. Priority zero was already counted
                        # as incremental work and must not move a second time.
                        if (source and source.grain == 'day' and kind in {'day', 'id_day'}
                                and bool(has_id) == (source.universe != 'market')):
                            item['requests'] += count
                            item['retry_tasks'] += count if state == 'failed' else 0
                            retry = _stamp(last)
                            item['max_retry_wait_seconds'] = max(item['max_retry_wait_seconds'],
                                                                 (retry - now).total_seconds() if retry else 0)
                        else:
                            item['unsupported_tasks'] += count
                    elif state == 'inflight':
                        item['inflight_tasks'] += count
                    elif state not in EXCLUDED_STATES | DONE_STATES:
                        item['blocked_tasks'] += count
            if owner == 'complement' and 'finmind_source_frontiers' in tables:
                from downloader.finmind_supplemental import frontier_status
                for dataset, frontier in frontier_status(conn).items():
                    if dataset in aggregate:
                        aggregate[dataset]['frontier'] = frontier
                        aggregate[dataset]['classes']['backfill'] += frontier['unseeded_partition_candidates']
            disabled = (set(value[0] for value in conn.execute("SELECT dataset FROM request_batch_policy"))
                        if "request_batch_policy" in tables else set())
            table = "request_batch_limits" if owner == "sponsor" else "complement_year_batch_policy"
            limits = dict(conn.execute(f"SELECT dataset,max_partitions FROM {table}")) if table in tables else {}
            eligible = list(conn.execute("""SELECT dataset,data_id,partition,kind,priority,state,next_attempt_at_utc
                FROM tasks WHERE kind IN ('year','month') AND data_id='' AND priority>=0 AND priority<8
                AND (state IN ('pending','failed') OR (priority=0 AND state IN ('complete','observed_empty')
                    AND next_attempt_at_utc<=?)) LIMIT ?""", (now.isoformat(), MAX_RANGE_TASKS + 1)))
            if len(eligible) > MAX_RANGE_TASKS:
                raise ValueError("range_inventory_size_limit")
            range_rows: dict[str, list[tuple[Any, ...]]] = {}
            for values in eligible:
                range_rows.setdefault(values[0], []).append(values)
            for dataset, values in range_rows.items():
                item = aggregate[dataset]
                item["central_range_classes"] = _range_groups(values, owner, now, limits.get(dataset), dataset in disabled)
                item["fast_range_classes"] = _range_groups(values, owner, now, None, dataset in disabled)
            # In-flight annual partitions share one already-issued request ID.
            if owner == "complement" and "complement_year_batch_claims" in tables:
                for dataset, parts, requests in conn.execute(
                    "SELECT c.dataset,count(*),count(DISTINCT c.request_id) FROM complement_year_batch_claims c "
                    "JOIN tasks t ON t.dataset=c.dataset AND t.partition=c.partition AND t.data_id='' "
                    "WHERE t.state='inflight' GROUP BY c.dataset"
                ):
                    aggregate[dataset]["inflight_network"] -= parts - requests
            return aggregate, {"state": "observed", "basis": "readonly_sqlite_snapshot",
                               "learned_batch_limits": limits, "batching_disabled": sorted(disabled)}
    except (sqlite3.Error, OSError, ValueError, TypeError) as error:
        return {}, {"state": "queue_unreadable", "error_type": type(error).__name__}


def _range_groups(rows: list[tuple[Any, ...]], owner: str, now: datetime,
                  limit: int | None, disabled: bool) -> Counter[str]:
    """Simulate only documented batching; deferred tasks cannot bridge due gaps."""
    from downloader.download_finmind_complement import BULK_GLOBAL_HISTORY, Task
    counts: Counter[str] = Counter()
    ready: dict[str, Task] = {}
    for dataset, data_id, partition, kind, priority, state, next_attempt in rows:
        due = next_attempt is None or (_stamp(next_attempt) is not None and _stamp(next_attempt) <= now)
        work = _work_class(state, priority, due)
        if work not in {"incremental", "backfill"}:
            continue
        if not due:
            counts[work] += 1
        else:
            ready[partition] = Task(dataset, data_id, partition, kind, priority, state)
    today = now.astimezone(TAIPEI).date()
    while ready:
        # Sponsor rotates datasets, but its order *within* a dataset is priority
        # then newest. Complement's batch spans both sides of its oldest seed.
        priority = min(task.priority for task in ready.values())
        candidates = [task for task in ready.values() if task.priority == priority]
        seed = (max if owner == "sponsor" else min)(candidates, key=lambda task: task.partition)
        selected = (seed,)
        if not disabled and owner == "sponsor" and seed.dataset in RANGE_CONTRACTS:
            batch = coalesce_pending_tasks(seed, ready.values(), today=today, max_years=limit, max_months=limit,
                                           include_due_refresh=True)
            if batch is not None:
                selected = batch.tasks
        elif not disabled and owner == "complement" and seed.dataset in BULK_GLOBAL_HISTORY:
            # Same consecutive-year rule as _claim_bulk_years, without claiming.
            eligible = {int(part): task for part, task in ready.items() if len(part) == 4 and part.isdigit()}
            low = high = int(seed.partition)
            bound = limit or len(eligible)
            while low - 1 in eligible and high - low + 1 < bound:
                low -= 1
            while high + 1 in eligible and high - low + 1 < bound:
                high += 1
            selected = tuple(eligible[year] for year in range(low, high + 1))
        counts["incremental" if min(task.priority for task in selected) == 0 else "backfill"] += 1
        for task in selected:
            ready.pop(task.partition)
    return counts


def _observed_row(dataset: str, owner: str, contract: dict[str, Any], item: dict[str, Any],
                  now: datetime) -> dict[str, Any]:
    row = _row(dataset, owner, contract)
    unbatched = item["classes"]
    central = unbatched.copy()
    fast = unbatched.copy()
    if "central_range_classes" in item:
        central.subtract(item["range_classes"])
        central.update(item["central_range_classes"])
        fast.subtract(item["range_classes"])
        fast.update(item["fast_range_classes"])
    row.update({f"{name}_requests": central[name] for name in ("incremental", "backfill", "validation")})
    row["required_requests"] = central["incremental"] + central["backfill"]
    row['unbatched_incremental_requests'] = unbatched['incremental']
    row['fastest_incremental_requests'] = fast['incremental']
    row["current_plan_requests"] = sum(central.values())
    row["fastest_requests"] = sum(fast.values())
    row["unbatched_requests"] = sum(unbatched.values())
    row["batch_savings"] = row["unbatched_requests"] - row["current_plan_requests"]
    row["uncertain_requests"] = row["unbatched_requests"] - row["fastest_requests"]
    states = item["states"]
    row.update(completed_tasks=sum(states[state] for state in DONE_STATES),
               pending_tasks=sum(states[state] for state in PENDING_STATES),
               blocked_tasks=sum(count for state, count in states.items()
                                 if state not in EXCLUDED_STATES | DONE_STATES | PENDING_STATES | {"inflight"}),
               excluded_tasks=sum(states[state] for state in EXCLUDED_STATES),
               calendar_wait_tasks=states['calendar_wait'],
               inflight_tasks=states["inflight"], inflight_requests=item["inflight_network"],
               local_derived_tasks=item["derived"], cooling_tasks=item["cooling"],
               retry_tasks=sum(item['retries_by_class'].values()),
               retry_tasks_by_class=dict(item['retries_by_class']),
               retry_wait_seconds_by_class=item['wait_by_class'],
               earliest_retry_at_utc=item["retry_first"], state_counts=dict(states))
    if item.get('frontier'):
        row['historical_frontier'] = item['frontier']
        row['request_estimate_basis'] = 'includes_unseeded_calendar_candidates_not_verified_instrument_lifetimes'
    if item.get('priority_override'):
        row['priority_override'] = item['priority_override']
    latest = _stamp(item["retry_last"])
    if latest:
        row["max_retry_wait_seconds"] = max(0, (latest - now).total_seconds())
    if row["blocked_tasks"]:
        row["state"] = "partially_blocked"
    return row


def _free_rows(root: Path, now: datetime, catalog: dict[str, dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    from downloader.download_finmind_free import CALENDAR_DATASET, MASTER_DATASET, SESSION_DATASETS
    result: dict[str, Any] = {}
    try:
        status = _json(root / "status.json")
        observed = _stamp(status.get("observed_at_utc"))
        idle_proof = idle_heartbeat(root, status, now)
        if (observed is None or observed > now + timedelta(minutes=1)
                or (now - observed > FREE_STATUS_MAX_AGE and not idle_proof['alive'])):
            raise ValueError("stale_free_status")
        for dataset in SESSION_DATASETS:
            item = status["series"][dataset]
            total, complete, deferred = (item.get(key, 0) for key in ("total", "complete", "deferred"))
            if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (total, complete, deferred)):
                raise ValueError("invalid_free_counts")
            if complete + deferred > total:
                raise ValueError("inconsistent_free_counts")
            row = _row(dataset, "free", catalog[dataset]["owner_contracts"]["free"])
            pending = total - complete
            active = status.get("active_task") or {}
            inflight = int(pending > 0 and active.get("dataset") == dataset)
            pending -= inflight
            row.update(required_requests=pending, backfill_requests=pending, current_plan_requests=pending,
                       fastest_requests=pending, unbatched_requests=pending, completed_tasks=complete,
                       pending_tasks=pending, cooling_tasks=deferred, inflight_requests=inflight,
                       inflight_tasks=inflight, basis="fresh_worker_status_not_receipt_rescan")
            # The worker already visited these receipt heads. Reuse its small
            # projection instead of rescanning every historical file for ETA.
            retries = item.get('retry_tasks')
            if type(retries) is int and 0 <= retries <= pending + inflight:
                row['retry_tasks'] = retries
                row['retry_tasks_by_class'] = {'backfill': retries}
            earliest, latest = (_stamp(item.get(key)) for key in ('earliest_retry_at_utc', 'latest_retry_at_utc'))
            row['earliest_retry_at_utc'] = earliest.isoformat() if earliest else None
            row['max_retry_wait_seconds'] = max(0, (latest - now).total_seconds()) if latest else 0
            row['retry_wait_seconds_by_class'] = {'backfill': row['max_retry_wait_seconds']}
            result[dataset] = row
        local = now.astimezone(TAIPEI)
        for dataset in (CALENDAR_DATASET, MASTER_DATASET):
            row = _row(dataset, "free", catalog[dataset]["owner_contracts"]["free"])
            row["basis"] = "bounded_local_snapshot_receipt_not_content_audit"
            if dataset == CALENDAR_DATASET:
                receipt = _json(root / "calendar.json")
                stamp = _stamp(receipt.get("observed_at_utc"))
                if receipt.get("source_dataset") != dataset or not receipt.get("dates") or stamp is None:
                    raise ValueError("invalid_calendar_receipt")
                due = now - stamp >= timedelta(hours=20)
            else:
                directory = root / "receipts" / dataset
                current = directory / f"{local.date()}.json"
                paths = list(islice(directory.glob("*.json"), 4001))
                if len(paths) > 4000:
                    raise ValueError("master_receipt_inventory_size_limit")
                path = current if current.is_file() else max(paths, default=None)
                if path is None:
                    raise ValueError("missing_master_receipt")
                receipt = _json(path)
                if receipt.get("dataset") != dataset or receipt.get("status") != "complete":
                    raise ValueError("invalid_master_receipt")
                due = local.hour >= 14 and not current.is_file()
            row.update(completed_tasks=1, incremental_requests=int(due), required_requests=int(due),
                       current_plan_requests=int(due), unbatched_requests=int(due), fastest_requests=int(due))
            result[dataset] = row
        return result, {"state": "observed", "observed_at_utc": status["observed_at_utc"],
                        "basis": "fresh_status_or_matching_idle_lease_plus_two_snapshot_heads",
                        "idle_heartbeat": idle_proof}
    except (OSError, ValueError, KeyError, TypeError) as error:
        return {}, {"state": "free_status_unreadable_or_stale", "error_type": type(error).__name__}


def build_finmind_workload(root: Path, now: datetime | None = None) -> dict[str, Any]:
    """Count unique-owner network work without altering a source or a queue.

    The finite horizon is the currently seeded registry, not all future refreshes
    or unknown provider products. Missing/unreadable sources yield unknown rows;
    ``summary`` is then explicitly a known partial subtotal, never completion.
    In-flight requests have already spent quota and are listed separately.
    """
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(UTC)
    root = Path(root)
    catalog = _registry()
    observations, sources = {}, {}
    for owner in ("sponsor", "complement"):
        observations[owner], sources[owner] = _queue(root / owner / "queue.sqlite3", owner, now)
    free, sources["free"] = _free_rows(root, now, catalog)
    rows = []
    for dataset, spec in sorted(catalog.items()):
        owner = spec["primary_owner"]
        contract = spec["owner_contracts"][owner]
        row = _row(dataset, owner, contract)
        if owner in {"unscheduled", "disabled_policy"}:
            row.update(state=owner, basis=contract.get("unscheduled_reason", contract.get("disabled_reason")))
            for key in ("required_requests", "current_plan_requests", "fastest_requests", "unbatched_requests"):
                row[key] = None
        elif owner == "free" and dataset in free:
            row = free[dataset]
        elif owner in observations and dataset in observations[owner]:
            row = _observed_row(dataset, owner, contract, observations[owner][dataset], now)
        elif contract['query_shape'] == 'derived_no_api':
            row.update(state='awaiting_parent', basis='derived_after_parent_receipt_no_additional_api')
        else:
            row.update(state="unknown", basis="missing_unseeded_or_unreadable_primary_owner")
            row.update(dict.fromkeys(COUNT_FIELDS))
        row["owner_aliases_not_added"] = [alias for alias in spec["owners"] if alias != owner]
        rows.append(row)
    summary = {key: sum(row[key] or 0 for row in rows) for key in COUNT_FIELDS}
    summary.update(unknown_datasets=sum(row["state"] == "unknown" for row in rows),
                   unscheduled_datasets=sum(row["state"] == "unscheduled" for row in rows),
                   disabled_datasets=sum(row["state"] == "disabled_policy" for row in rows),
                   missing_source_owners=[owner for owner, value in sources.items() if value["state"] != "observed"],
                   max_retry_wait_seconds=max((row["max_retry_wait_seconds"] for row in rows), default=0))
    summary["count_basis"] = "known_partial_subtotal" if summary["unknown_datasets"] else "registered_primary_owner_current_queue"
    return {"schema_version": 1, "observed_at_utc": now.isoformat(),
            "state": "partial" if summary["unknown_datasets"] else "observed",
            "scope": "current_registered_history", "api_requests": 0, "queue_writes": 0, "parquet_scans": 0,
            "inflight_basis": "already_issued_not_added_to_remaining_quota_calls",
            "uncertainty_basis": "known_batch_split_spread_not_unknown_provider_universe",
            "all_provider_history_complete_claim": False,
            "summary": summary, "datasets": rows, "sources": sources}
