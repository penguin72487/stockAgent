"""Finite source-bound priorities on the canonical TEJ queue; no desktop owner."""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_history import (
    CONTRACT_VERSION, SOURCE_SCOPE_CONTRACT, compact_request, company_code,
    connect, expand_request, normalize_period, stable_id,
)
from downloader.tej_key_layout import KEY1_CONTRACT, key_count

CONTRACT = "finite_source_bound_priority_requests_v1"
SOURCE_RECHECK_CONTRACT = "completed_native_value_conflict_recheck_v1"


class PriorityQueueChanged(RuntimeError):
    """A concurrent queue mutation invalidated the read-only priority plan."""


def _prior_rows(con, table_id: str, *, batch_size: int = 64):
    """Release SQLite's read lock between bounded pages of retained tasks."""
    # Fetch the small identity list once. Sorting and rescanning the entire
    # table for every page would make a 1M-task queue quadratic in practice.
    ids = [row['task_id'] for row in con.execute(
        "SELECT task_id FROM tasks WHERE table_id=? AND kind='download' "
        "AND state IN ('complete','pending','running','blocked') ORDER BY task_id",
        (table_id,)).fetchall()]
    for begin in range(0, len(ids), batch_size):
        page = ids[begin:begin + batch_size]
        placeholders = ','.join('?' for _ in page)
        rows = con.execute(f"SELECT * FROM tasks WHERE task_id IN ({placeholders}) ORDER BY task_id",
                           page).fetchall()
        for row in rows:
            yield row


def _source_recheck_parents(root: Path, item: dict, request: dict, prior: list) -> set[str]:
    """Permit one separately retained query of a proved conflicting native cell.

    Completed observations stay immutable. This is source correction research,
    not a retry of an unknown desktop outcome or an unbounded refresh policy.
    The new request carries the conflict and parent identities into its own
    durable task hash and export receipt.
    """
    proof = request.get("source_recheck")
    if proof is None:
        return set()
    companies, periods = query_axes(request)
    if (not isinstance(proof, dict)
            or set(proof) != {"contract", "conflict_evidence", "parent_task_ids"}
            or proof["contract"] != SOURCE_RECHECK_CONTRACT or key_count(request) != 2
            or len(companies) != 1 or len(periods) != 1):
        raise ValueError("A source-conflict recheck requires one exact native daily cell")
    evidence_ref = proof["conflict_evidence"]
    if (not isinstance(evidence_ref, dict) or set(evidence_ref) != {"path", "sha256"}
            or sha256_file(Path(evidence_ref["path"])) != evidence_ref["sha256"]):
        raise ValueError("Source-conflict evidence SHA mismatch")
    evidence = json.loads(Path(evidence_ref["path"]).read_text())
    if (evidence.get("contract") != "native_source_value_conflict_v1"
            or evidence.get("table_id") != item["table_id"]
            or evidence.get("company") != next(iter(companies))
            or evidence.get("period") != next(iter(periods))
            or not evidence.get("reason") or len(evidence.get("sources", [])) < 2
            or {s.get("kind") for s in evidence["sources"]}
               != {"native_original", "independent_original"}):
        raise ValueError("Exact native and independent conflicting sources required")
    paths, native_paths = set(), set()
    for source in evidence["sources"]:
        path = Path(source["path"]).resolve()
        if sha256_file(path) != source["sha256"]:
            raise ValueError("Conflicting source original SHA mismatch")
        paths.add(path)
        if source["kind"] == "native_original":
            native_paths.add(path)
    if len(paths) < 2:
        raise ValueError("Independent source originals must be distinct")
    scope, names = query_axes(request), set(request["fields"])
    parents, retained_raw_paths = set(), set()
    for done, existing, old_scope in prior:
        if not _scopes_overlap(scope, old_scope) or not names.intersection(existing["fields"]):
            continue
        if (done["state"] != "complete" or done["scope_contract"] != SOURCE_SCOPE_CONTRACT
                or existing.get("source_recheck") is not None or not done.get("receipt_path")):
            raise ValueError("Only a completed original cell can receive one conflict recheck")
        receipt_path = (root / done["receipt_path"]).resolve()
        if not receipt_path.is_relative_to(root.resolve()):
            raise ValueError("Source parent receipt escapes canonical owner")
        receipt = json.loads(receipt_path.read_text())
        if (receipt.get("task_id") != done["task_id"] or receipt.get("table_id") != item["table_id"]
                or receipt.get("source_values_retained") is not True
                or receipt.get("training_auto_injected") is not False):
            raise ValueError("Retained native parent receipt required")
        for field in ("raw", "parquet"):
            path = (root / receipt[field + "_path"]).resolve()
            if (not path.is_relative_to(root.resolve())
                    or sha256_file(path) != receipt[field + "_sha256"]):
                raise ValueError("Native parent original SHA mismatch")
            if field == "raw":
                retained_raw_paths.add(path)
        parents.add(done["task_id"])
    if not parents or proof["parent_task_ids"] != sorted(parents):
        raise ValueError("Source recheck must bind every overlapping completed parent")
    if not native_paths or not native_paths.issubset(retained_raw_paths):
        raise ValueError("Conflicting native source must be a retained parent original")
    return parents


def pack_gap_rectangles(wanted: dict[str, set[str]], *, forbidden: set[tuple[str, str]],
                        max_companies: int, max_rows: int, expansion: float = 2.0) -> list[dict]:
    """Coalesce sparse needs with a bounded number of native control cells.

    Extra cells are real requested observations, never imputed prices. The
    heuristic reduces desktop actions; it makes no global-optimum or vendor
    bandwidth claim. Captured and unresolved query cells cannot be repeated.
    """
    from collections import defaultdict
    if (type(max_companies) is not int or type(max_rows) is not int
            or not 0 < max_companies <= 1024 or max_rows < 1
            or not 1 <= expansion <= 4):
        raise ValueError("Invalid bounded sparse-query geometry")
    grouped = defaultdict(list)
    for company, periods in sorted(wanted.items()):
        if not periods or any((company, day) in forbidden for day in periods):
            raise ValueError("Sparse needs are empty or overlap captured scope")
        grouped[tuple(sorted(periods))].append(company)
    rectangles = []
    for dates, companies in grouped.items():
        for begin in range(0, len(companies), min(max_companies, max_rows)):
            own = set(companies[begin:begin + min(max_companies, max_rows)])
            for offset in range(0, len(dates), max_rows // len(own)):
                selected = set(dates[offset:offset + max_rows // len(own)])
                rectangles.append({"companies": own, "dates": selected,
                                   "needed_rows": len(own) * len(selected)})
    while True:
        best = None
        for i, left in enumerate(rectangles):
            for j in range(i + 1, len(rectangles)):
                right = rectangles[j]
                if left["companies"] & right["companies"]:
                    # Keep each company's original disjoint date pieces apart.
                    continue
                companies = left["companies"] | right["companies"]
                dates = left["dates"] | right["dates"]
                rows = len(companies) * len(dates)
                needed = left["needed_rows"] + right["needed_rows"]
                if (len(companies) > max_companies or rows > min(max_rows, expansion * needed)
                        or any((company, day) in forbidden for company in companies for day in dates)
                        or any(companies & other["companies"] and dates & other["dates"]
                               for k, other in enumerate(rectangles) if k not in (i, j))):
                    continue
                score = (rows - len(left["companies"]) * len(left["dates"])
                         - len(right["companies"]) * len(right["dates"]), rows, i, j)
                if best is None or score < best[0]:
                    best = (score, {"companies": companies, "dates": dates, "needed_rows": needed})
        if best is None:
            break
        (_, _, i, j), merged = best
        rectangles = [r for index, r in enumerate(rectangles) if index not in (i, j)] + [merged]
    return [{"companies": sorted(r["companies"]), "dates": sorted(r["dates"]),
             "needed_rows": r["needed_rows"], "requested_grid_rows": len(r["companies"]) * len(r["dates"])}
            for r in sorted(rectangles, key=lambda r: (min(r["dates"]), min(r["companies"])))]


def query_axes(request: dict) -> tuple[frozenset[str], frozenset[str | None]]:
    """Keep rectangular native scopes factored instead of retaining C*D cells."""
    companies = frozenset(company_code(label) for label in request["company_labels"])
    periods = (frozenset({None}) if key_count(request) == 1 else
               frozenset(normalize_period(period) for period in request["date_labels"]))
    return companies, periods


def _scopes_overlap(left, right) -> bool:
    return not left[0].isdisjoint(right[0]) and not left[1].isdisjoint(right[1])


def query_keys(request: dict) -> set[tuple[str, str | None]]:
    companies, periods = query_axes(request)
    return {(company, period) for company in companies for period in periods}


def retained_request(row: dict, definition: dict) -> dict | None:
    """Legacy scopes are forbidden query cells, never upgraded observations.

    Some old operator records named a menu-only plan 'download'. An explicit
    plan action has no Preview scope. A legacy data action with captured axes
    is conservatively treated as queried across all fields, even without an
    admissible receipt. Ambiguous data actions still require reconciliation.
    """
    encoded = json.loads(row["request_json"])
    if row["scope_contract"] == SOURCE_SCOPE_CONTRACT:
        return expand_request(encoded, definition, definition["table_id"])
    if encoded.get("action") == "plan":
        return None
    if (encoded.get("action") != "download" or not encoded.get("company_labels")
            or not encoded.get("date_labels")):
        raise ValueError("Unverified prior query scope requires reconciliation")
    return {**encoded, "fields": json.loads(definition["fields_json"])}


def finite_axis_dependency_pins(root: Path, path: Path, proof: dict) -> dict[str, str]:
    """List every immutable dependency of an independently audited observation."""
    attempt = proof['query_attempt_id']
    observation = json.loads(path.read_text(encoding='utf-8-sig'))
    claim_path = root / 'finite_axis_capture_intents' / 'claimed' / (attempt + '.json')
    claim = json.loads(claim_path.read_text(encoding='utf-8-sig'))
    candidate_path = Path(claim['candidate_manifest_path']).resolve(strict=True)
    candidate = json.loads(candidate_path.read_text(encoding='utf-8-sig'))
    from downloader.tej_history import DesktopBridge
    request_path = root / 'requests' / (attempt + '.json')
    release, release_sha = DesktopBridge.bound_script_release(root, attempt, request_path)
    if (sha256_file(path) != proof.get('observation_sha256')
            or sha256_file(claim_path) != proof.get('claimed_intent_sha256')
            or observation.get('intent_sha256') != proof.get('claimed_intent_sha256')
            or sha256_file(request_path) != observation.get('prepared_request_sha256')
            or release_sha != observation.get('bridge_script_sha256')
            or claim.get('candidate_manifest_sha256') != observation.get('candidate_manifest_sha256')):
        raise ValueError('Finite source evidence changed after independent audit')
    required = {str(path.resolve(strict=True)): proof['observation_sha256'],
                str(claim_path.resolve()): proof['claimed_intent_sha256'],
                str(request_path.resolve()): observation['prepared_request_sha256'], str(Path(release).resolve()): release_sha,
                str(Path(claim['snapshot_manifest_path']).resolve()): claim['snapshot_manifest_sha256'],
                str(candidate_path): claim['candidate_manifest_sha256'],
                str(Path(candidate['data_path']).resolve()): candidate['data_sha256']}
    for category in ('source_pins', 'implementation_sha256'):
        for name, digest in candidate.get(category, {}).items():
            resolved = str(Path(name).resolve(strict=True))
            if resolved in required and required[resolved] != digest:
                raise ValueError('Finite source evidence has contradictory fingerprints')
            required[resolved] = digest
    return required


def _finite_axis_plan(root: Path, con, definition: dict, reference: dict,
                      source_pins: dict[str, str], config: dict) -> tuple[dict, dict, Path]:
    """Bind an exact partial native observation without installing discovery.

    This permits only the verified company/date rectangle and the canonical
    30-column field planner.  A source menu is not a market observation.
    """
    if (not isinstance(reference, dict) or set(reference) != {'path', 'sha256'}
            or not isinstance(reference['path'], str) or not isinstance(reference['sha256'], str)):
        raise ValueError('Exact finite source observation reference required')
    path = Path(reference['path']).resolve(strict=True)
    if sha256_file(path) != reference['sha256']:
        raise ValueError('Finite source observation fingerprint changed')
    from scripts.audit_tej_finite_option_axis_capture import audit
    proof = audit(root, path)
    if (proof.get('market_observations_obtained') is not False
            or proof.get('full_axis_admitted_to_plan') is not False
            or proof.get('training_input_admitted') is not False
            or proof.get('source_key_mode') != 2 or proof.get('date_menu_contains_day') is not True
            or any(proof.get(key) != definition[value] for key, value in
                   (('type', 'query_type'), ('smart_id', 'smart_id'), ('table', 'name')))):
        raise ValueError('Finite Key=2 native company/date observation is not verified')
    observation = json.loads(path.read_text(encoding='utf-8-sig'))
    required = finite_axis_dependency_pins(root, path, proof)
    if any(source_pins.get(name) != digest for name, digest in required.items()):
        raise ValueError('Finite priority plan does not pin every source dependency')
    task = con.execute('SELECT * FROM tasks WHERE task_id=?', (proof['task_id'],)).fetchone()
    if (task is None or task['kind'] != 'discover' or task['table_id'] != definition['table_id']
            or task['scope_contract'] != SOURCE_SCOPE_CONTRACT):
        raise ValueError('Finite source observation is not bound to the registered discovery')
    from downloader.tej_history import task_request
    request = task_request(root, dict(task), connection=con)
    from downloader.tej_planning import build_plan
    axes = {'company_labels': observation['company_labels'], 'date_labels': observation['date_labels']}
    plan = build_plan(request, axes['company_labels'], axes['date_labels'], config, [])
    return axes, plan, path


def register_priority_requests(root: Path, plan_path: Path, receipt_path: Path, *,
                               repair_pending_bounds: bool = False) -> dict:
    """Short metadata transaction, safe between or during an unrelated query.

    The caller explicitly invokes this action. It never retries a source action,
    changes the global plan, clears quarantine, or acquires a second desktop.
    Terminal negative responses count as queried scopes, just like nonempty ones.
    An explicit single-cell source-conflict recheck has its own retained task;
    it never resets or overwrites a completed parent.
    """
    if type(repair_pending_bounds) is not bool:
        raise ValueError("Explicit pending-bound repair mode required")
    plan = json.loads(plan_path.read_text())
    if (plan.get("contract") != CONTRACT or not isinstance(plan.get("requests"), list)
            or not 1 <= len(plan["requests"]) <= 1000 or not plan.get("sources")):
        raise ValueError("Invalid finite priority plan")
    source_pins = {}
    for source in plan["sources"]:
        path = Path(source["path"]).resolve(strict=True)
        if sha256_file(path) != source["sha256"]:
            raise ValueError("Priority plan source SHA mismatch")
        if str(path) in source_pins and source_pins[str(path)] != source['sha256']:
            raise ValueError('Priority plan contains contradictory source fingerprints')
        source_pins[str(path)] = source['sha256']
    if receipt_path.exists():
        raise ValueError("Priority receipt already exists; preserve the previous operation")
    result = {"contract": CONTRACT, "plan_path": str(plan_path),
              "plan_sha256": sha256_file(plan_path), "tasks": [],
              "global_plan_changed": False, "desktop_actions": 0,
              "unknown_outcomes_retried": 0,
              "priority_lifetime": "finite_tasks_only_original_plan_resumes_on_completion"}
    with closing(connect(root)) as con:
        # Scan in short read pages without holding the single desktop owner's
        # writer lock. The same connection's data_version changes on any other
        # connection's commit, so a concurrent edit invalidates the plan.
        queue_version = con.execute("PRAGMA data_version").fetchone()[0]
        try:
            config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
            definitions, prior, planned = {}, {}, {}
            mutations = []
            for item in plan["requests"]:
                tid, request = item["table_id"], item["request"]
                priority = item["priority"]
                if type(priority) is not int or not -99 <= priority < 0 or not item.get("reason"):
                    raise ValueError("Explicit bounded priority and purpose required")
                if tid not in definitions:
                    row = con.execute("SELECT * FROM tables WHERE table_id=?", (tid,)).fetchone()
                    if row is None:
                        raise ValueError("Priority table has no verified source axes")
                    definitions[tid] = dict(row)
                    finite_ref = item.get('finite_axis_observation')
                    if row['discovery_path']:
                        if finite_ref is not None:
                            raise ValueError('Use the complete discovery when already installed')
                        discovery = (root / row["discovery_path"]).resolve()
                        if not discovery.is_relative_to((root / "raw").resolve()):
                            raise ValueError("Discovery source escapes raw root")
                        axes = json.loads(discovery.read_text())
                        if (axes.get("contract_version") != CONTRACT_VERSION or axes.get("action") != "plan"
                                or axes.get("smart_id") != row["smart_id"] or axes.get("table") != row["name"]
                                or axes.get("type") != row["query_type"]
                                or axes.get("fields") != json.loads(row["fields_json"])):
                            raise ValueError("Captured discovery belongs to another source/schema")
                    else:
                        axes, finite_plan, discovery = _finite_axis_plan(
                            root, con, definitions[tid], finite_ref, source_pins, config)
                        definitions[tid]['_priority_field_plan'] = finite_plan
                        definitions[tid]['_finite_axis_reference'] = finite_ref
                    definitions[tid]["_axes"] = axes
                    definitions[tid]["_axis_path"] = discovery
                    definitions[tid]["_axis_sha256"] = sha256_file(discovery)
                    prior[tid] = []
                    # All overlap checks below concern this finite plan only.
                    # Decode and validate every prior scope as before, but do
                    # not retain thousands of unrelated, potentially huge
                    # native rectangles throughout the write transaction.
                    target_scopes = [query_axes(other["request"]) for other in plan["requests"]
                                     if other["table_id"] == tid]
                    for done in _prior_rows(con, tid):
                        existing = retained_request(dict(done), dict(row))
                        if existing is not None:
                            old_scope = query_axes(existing)
                            if any(_scopes_overlap(old_scope, scope) for scope in target_scopes):
                                prior[tid].append((dict(done), existing, old_scope))
                    planned[tid] = []
                definition = definitions[tid]
                if item.get('finite_axis_observation') != definition.get('_finite_axis_reference'):
                    raise ValueError('Priority requests must share the exact same source observation')
                names = json.loads(definition["fields_json"])
                from scripts.build_tej_smart_wizard_inventory import table_identity
                if table_identity(definition["smart_id"], definition["name"], names) != (tid, definition["schema_sha256"]):
                    raise ValueError("Registered priority schema content changed")
                if (item.get("discovery_sha256") != definition["_axis_sha256"]
                        or request.get("contract_version") != CONTRACT_VERSION
                        or request.get("action") != "download"
                        or request.get("smart_id") != definition["smart_id"]
                        or request.get("table") != definition["name"]
                        or request.get("type") != definition["query_type"]
                        or request.get("catalog_fields") != names):
                    raise ValueError("Priority schema/source scope mismatch")
                fields = request.get("fields")
                partition = request.get("field_partition")
                if partition is None:
                    if fields != names:
                        raise ValueError("Priority schema/source scope mismatch")
                else:
                    # A wide table may use an existing lazy plan's exact
                    # contiguous field batch.  This proves the catalog and
                    # work denominator; arbitrary subsets remain inadmissible.
                    from downloader.tej_planning import CONTRACT as FIELD_CONTRACT
                    if "_priority_field_plan" not in definition:
                        field_row = con.execute("SELECT contract,plan_json FROM download_plans WHERE table_id=?", (tid,)).fetchone()
                        if field_row is None or field_row["contract"] != FIELD_CONTRACT:
                            raise ValueError("Priority field partition requires an existing native plan")
                        field_plan = json.loads(field_row["plan_json"])
                        core = {k: v for k, v in field_plan.items() if k != "fingerprint"}
                        digest = hashlib.sha256(json.dumps(core, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                        if field_plan.get("contract") != FIELD_CONTRACT or field_plan.get("fingerprint") != digest:
                            raise ValueError("Priority field plan fingerprint differs")
                        definition["_priority_field_plan"] = field_plan
                    field_plan = definition["_priority_field_plan"]
                    if (not isinstance(partition, dict) or key_count(request) != 2
                            or request.get("frequency") != "daily"
                            or partition.get("plan_fingerprint") != field_plan["fingerprint"]
                            or field_plan.get("request", {}).get("fields") != names
                            or partition.get("field_range") not in [segment["field_range"] for segment in field_plan["segments"]]):
                        raise ValueError("Priority field partition differs from the native plan")
                    compact_request(request, definition)  # Validate schema, exact slice and Preview limit.
                companies, dates = request.get("company_labels"), request.get("date_labels")
                axes = definition["_axes"]
                if (not isinstance(companies, list) or not companies or len(set(companies)) != len(companies)
                        or not set(companies) <= set(axes["company_labels"])
                        or not isinstance(dates, list) or len(set(dates)) != len(dates)):
                    raise ValueError("Priority selection differs from captured native axes")
                symbols = [company_code(label) for label in companies]
                if len(set(symbols)) != len(symbols):
                    raise ValueError("Priority company aliases are ambiguous")
                mode = key_count(request)
                if mode == 1:
                    if (definition.get("source_key_mode") != 1 or request.get("key_layout_contract") != KEY1_CONTRACT
                            or request.get("frequency") != "snapshot" or dates):
                        raise ValueError("Snapshot must retain Key=1 and have no history dates")
                elif mode != 2 or not dates or not set(dates) <= set(axes["date_labels"]):
                    raise ValueError("Priority requires verified daily Key=2 or static Key=1")
                else:
                    if request.get("frequency") != "daily" or any(len(normalize_period(d)) != 10 for d in dates):
                        raise ValueError("Priority daily observation dates required")
                    periods = [normalize_period(d) for d in dates]
                    if min(periods) < request["start"] or max(periods) > request["end"]:
                        raise ValueError("Priority dates outside exact requested scope")
                rows = len(companies) * (1 if mode == 1 else len(dates))
                columns = mode + len(fields)
                if (type(request.get("max_rows")) is not int or type(request.get("max_cells")) is not int
                        or not 0 < rows <= request["max_rows"] <= config["max_rows_per_export"]
                        or not 100 <= request["max_cells"] <= 400000
                        or not (rows + 1) * columns <= request["max_cells"] <= config["max_cells_per_export"]
                        or len(companies) > config["max_companies_per_export"] or columns > 30):
                    raise ValueError("Priority exceeds native export bounds")
                scope = query_axes(request)
                for old_scope, old_fields in planned[tid]:
                    if _scopes_overlap(scope, old_scope) and set(fields) & old_fields:
                        raise ValueError("Overlapping requests within priority plan")
                planned[tid].append((scope, set(fields)))
                key = stable_id([tid, request])
                reused = None
                reused_request = None
                recheck_parents = _source_recheck_parents(root, item, request, prior[tid])
                for done, existing, old_scope in prior[tid]:
                    if not _scopes_overlap(scope, old_scope) or not set(fields) & set(existing["fields"]):
                        continue
                    if done["task_id"] in recheck_parents:
                        continue
                    if (done["scope_contract"] == SOURCE_SCOPE_CONTRACT
                            and done["state"] == "pending" and scope == old_scope
                            and existing["fields"] == fields and reused is None):
                        reused = done
                        reused_request = existing
                    else:
                        raise ValueError("Priority overlaps captured, in-flight, blocked or partial pending scope")
                replace_bound = reused is not None and reused_request["max_cells"] < 100
                previous_request_sha = None
                if replace_bound:
                    # Preserve the never-attempted invalid request under its
                    # old identity. The replacement has identical source axes,
                    # fields and row budget; only the bridge's minimum changes.
                    if (not repair_pending_bounds or request["max_cells"] != 100
                            or {**reused_request, "max_cells": 100} != request
                            or any(reused[k] is not None for k in ("attempted_at_utc", "completed_at_utc",
                                "actual_rows", "receipt_path", "output_path", "active_attempt_id"))
                            or con.execute("SELECT 1 FROM desktop_attempts WHERE task_id=? LIMIT 1",
                                           (reused["task_id"],)).fetchone()):
                        raise ValueError("Exact never-attempted minimum-bound repair required")
                    encoded = json.dumps(compact_request(request, definition), ensure_ascii=False, separators=(",", ":"))
                    if con.execute("SELECT 1 FROM tasks WHERE task_id=?", (key,)).fetchone():
                        raise ValueError("Replacement task identity already retained")
                    previous_request_sha = hashlib.sha256(reused["request_json"].encode()).hexdigest()
                    mutations.append(("supersede_bound", reused, priority))
                    mutations.append(("insert", (key, tid, definition["phase"], encoded, rows), min(priority, reused["priority"])))
                elif reused is not None:
                    mutations.append(("promote", reused, priority))
                    key = reused["task_id"]
                else:
                    encoded = json.dumps(compact_request(request, definition), ensure_ascii=False, separators=(",", ":"))
                    if con.execute("SELECT 1 FROM tasks WHERE task_id=?", (key,)).fetchone():
                        raise ValueError("Priority task identity is retained in another lifecycle state")
                    mutations.append(("insert", (key, tid, definition["phase"], encoded, rows), priority))
                result["tasks"].append({"task_id": key, "table_id": tid, "priority": priority,
                                        "requested_grid_rows": rows, "reason": item["reason"],
                                        "operation": ("replace_unattempted_invalid_bound" if replace_bound else
                                                      "promote_pending" if reused else
                                                      "new_source_conflict_recheck" if recheck_parents else
                                                      "new_finite_task"),
                                        "source_recheck_parent_task_ids": sorted(recheck_parents),
                                        "previous_task_id": reused["task_id"] if replace_bound else None,
                                        "previous_request_sha256": previous_request_sha,
                                        "previous_priority": reused["priority"] if reused else None})
            # No write lock until the whole batch has passed semantic checks.
            # Once acquired, recheck the optimistic queue version and every
            # local source before committing any task or priority change.
            con.execute("BEGIN IMMEDIATE")
            if con.execute("PRAGMA data_version").fetchone()[0] != queue_version:
                raise PriorityQueueChanged("TEJ queue changed during priority planning")
            for source in plan["sources"]:
                if sha256_file(Path(source["path"])) != source["sha256"]:
                    raise ValueError("Priority plan source changed during planning")
            for definition in definitions.values():
                if sha256_file(definition['_axis_path']) != definition["_axis_sha256"]:
                    raise ValueError("Priority discovery changed during planning")
            for action, values, priority in mutations:
                if action == "supersede_bound":
                    con.execute("UPDATE tasks SET state='superseded_priority_capacity_v1' WHERE task_id=? AND state='pending'",
                                (values["task_id"],))
                elif action == "promote":
                    con.execute("UPDATE tasks SET priority=? WHERE task_id=? AND state='pending'",
                                (min(priority, values["priority"]), values["task_id"]))
                else:
                    key, tid, phase, encoded, rows = values
                    con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,"
                                "work_expected_rows,scope_contract) VALUES(?,?,'download',?,?,?,?,?,?)",
                                (key, tid, phase, priority, encoded, rows, rows, SOURCE_SCOPE_CONTRACT))
            result["registered_at_utc"] = datetime.now(UTC).isoformat()
            # The audit and task mutation commit together. A crash before the
            # final public receipt is recoverable from this exact metadata.
            audit_key = "finite_priority_plan:" + result["plan_sha256"]
            if con.execute("SELECT 1 FROM meta WHERE key=?", (audit_key,)).fetchone():
                raise ValueError("This finite priority plan was already registered")
            con.execute("INSERT INTO meta VALUES(?,?)", (audit_key, json.dumps(result, ensure_ascii=False)))
            from downloader.tej_priority_coverage import index_new_priority_tasks
            index_new_priority_tasks(con, result)
            con.commit()
        except BaseException:
            con.rollback()
            raise
    atomic_write_json(receipt_path, result)
    return result
