"""Source-labelled period keys, distinct from requested analytical grid dates.

Smart Wizard can render a YYYYMM key as the last trading day of its month,
including a later day than a partial-month query's end. This is a period label,
not evidence that data was published on that rendered day. Only the literal,
captured YYYYMM header permits month semantics; ordinary Date keys stay strict.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import uuid

CONTRACT = "literal_source_yyyymm_period_key_v1"
BANK_MONTH_CONTRACT = "literal_source_data_yymm_period_key_v1"
HEADER_CONTRACTS = {'YYYYMM':CONTRACT, 'Data YYMM':BANK_MONTH_CONTRACT}
MONTH_CONTRACTS = frozenset(HEADER_CONTRACTS.values())


def union_scope_weights(request: dict, scopes: list[tuple], by_month: dict[str,str]) -> list[tuple[int,str]]:
    """Credit each covered field-partition/company/month once, not twice.

    Month ranges can overlap when adjacent old daily chunks both selected
    part of one month. Store merged index intervals, never a dense historical
    Cartesian grid. Only whole new field partitions qualify for coverage.
    """
    from downloader.tej_key_layout import key_count
    fields=request['fields'];width=30-key_count(request)
    month_index={month:index for index,month in enumerate(sorted(by_month))}
    covered: dict[tuple[int,str],list[tuple[int,int]]] = {}
    weights=[]
    for done,req,months in scopes:
        if not months <= month_index.keys():
            raise ValueError('Accepted month scope absent from new verified source axis')
        ranges=[]
        for index in sorted(month_index[month] for month in months):
            if ranges and ranges[-1][1]==index:ranges[-1]=(ranges[-1][0],index+1)
            else:ranges.append((index,index+1))
        selected=set(req['fields']);weight=0
        for begin in range(0,len(fields),width):
            if not set(fields[begin:begin+width]) <= selected:continue
            for company in req['company_labels']:
                key=(begin,company);previous=covered.get(key,[]);merged=[]
                for left,right in sorted(previous+ranges):
                    if merged and left<=merged[-1][1]:merged[-1]=(merged[-1][0],max(merged[-1][1],right))
                    else:merged.append((left,right))
                weight+=sum(right-left for left,right in merged)-sum(right-left for left,right in previous)
                covered[key]=merged
        weights.append((weight,done['task_id']))
    return weights


def month_key(value, *, system=None) -> str:
    from downloader.tej_history import normalize_period

    period = normalize_period(value, system=system)
    if len(period) not in (7, 10):
        raise ValueError("YYYYMM source key requires an unambiguous month or date")
    return period[:7]


def month_header_contract(payload: dict) -> str | None:
    """Only reviewed literal headers; never guess grain from a rendered date.

    Data YYMM is the observed Bankstat month key. Values still require a
    four-digit year; this label does not authorize guessing a two-digit year.
    Keep its ABI separate from the earlier literal YYYYMM contract.
    """
    headers = payload.get("source_key_headers")
    valid = (payload.get("contract_version") == 4
            and payload.get("capture_method") == "native_msaa_preview_full"
            and isinstance(headers, list) and len(headers) in (2, 3)
            and isinstance(headers[1],str) and headers[1] in HEADER_CONTRACTS
            and isinstance(payload.get("cells"), list) and bool(payload["cells"])
            and isinstance(payload['cells'][0],list)
            and payload["cells"][0][:len(headers)] == headers)
    return HEADER_CONTRACTS[headers[1]] if valid else None


def literal_month_header(payload: dict) -> bool:
    return month_header_contract(payload) is not None


def source_month_keys(payload: dict) -> bool:
    # Existing v4 receipts keep their historical interpretation. A source
    # schema alone is not permission to change an already-published data ABI.
    contract=month_header_contract(payload)
    return contract is not None and payload.get('source_period_key_contract') == contract


def interpreted_payload(root: Path, task: dict, payload: dict) -> dict:
    """Apply an exact, audited interpretation to saved raw bytes, not rewrite them."""
    path = root/'period_key_interpretations'/(task['task_id']+'.json')
    if not path.is_file():
        return payload
    if path.stat().st_size > 8192:
        raise ValueError('Bounded exact period interpretation required')
    proof = json.loads(path.read_text(encoding='utf-8-sig'))
    attempt = task.get('active_attempt_id')
    raw = root/'raw'/(str(attempt)+'.json')
    contract=month_header_contract(payload)
    if (contract is None or proof.get('contract') != contract or proof.get('task_id') != task['task_id']
            or not attempt or proof.get('query_attempt_id') != attempt
            or payload.get('query_attempt_id') != attempt
            or proof.get('raw_path') != str(raw.relative_to(root))
            or not raw.is_file() or raw.stat().st_size > 64*1024**2
            or proof.get('raw_sha256') != hashlib.sha256(raw.read_bytes()).hexdigest()
            or proof.get('source_bytes_changed') is not False
            or proof.get('source_header') != payload['source_key_headers'][1]):
        raise ValueError('Period interpretation does not match the exact original capture')
    return {**payload, 'source_period_key_contract': contract,
            'source_period_interpretation_path': str(path.relative_to(root)),
            'source_period_interpretation_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def approve_saved_month_interpretation(root: Path, task_id: str, output: Path) -> dict:
    """Operator recovery of one rejected raw capture; no source action or replay."""
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import connect, task_request, validate_download_evidence
    from downloader.tej_desktop_attempts import validate_active_attempt

    with closing(connect(root)) as con:
        row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    if (row is None or row['kind'] != 'download' or row['state'] != 'blocked'
            or row['last_error_code'] != 'source_validation_failed'):
        raise ValueError('One exact rejected source capture is required')
    task = dict(row)
    expected = (root/'raw'/(str(task['active_attempt_id'])+'.json')).resolve()
    if output.resolve() != expected or not output.is_file() or output.stat().st_size > 64*1024**2:
        raise ValueError('Exact bounded active-attempt response is required')
    payload = json.loads(output.read_text(encoding='utf-8-sig'))
    validate_active_attempt(root, task, payload)
    contract=month_header_contract(payload)
    if contract is None or (root/'receipts'/(task_id+'.json')).exists():
        raise ValueError('Do not reinterpret an adopted receipt or an ambiguous source header')
    _, _, profile = validate_download_evidence(task_request(root, task),
                                             {**payload, 'source_period_key_contract': contract})
    proof = {'contract':contract, 'task_id':task_id, 'query_attempt_id':task['active_attempt_id'],
             'raw_path':str(output.resolve().relative_to(root.resolve())),
             'raw_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
             'observed_at_utc':datetime.now(UTC).isoformat(), 'source_bytes_changed':False,
             'source_dates_backdated':False,'data_query_repeated':False,
             'source_header':payload['source_key_headers'][1],'source_period_scope_rows':profile['source_period_scope_rows']}
    path = root/'period_key_interpretations'/(task_id+'.json')
    if path.exists():
        existing = json.loads(path.read_text())
        if any(existing.get(key) != proof.get(key) for key in proof if key != 'observed_at_utc'):
            raise ValueError('Saved period interpretation differs; never overwrite it')
    else:
        atomic_write_json(path, proof)
        path.chmod(0o600)
    return {'state':'saved_month_interpretation_verified', 'source_period_key_contract':contract,
            'source_bytes_changed':False,'data_query_repeated':False}


def period_scope(request: dict, payload: dict) -> tuple[set[str], dict]:
    from downloader.tej_history import normalize_period

    original = {normalize_period(label) for label in request["date_labels"]}
    declared=request.get('source_period_key_contract')
    if declared is not None and declared not in MONTH_CONTRACTS:
        raise ValueError("Unreviewed source period-key contract")
    if declared is not None and (not source_month_keys(payload) or declared!=month_header_contract(payload)):
        raise ValueError("Planned month-period source header changed; do not guess its grain")
    if not source_month_keys(payload):
        if payload.get('source_period_key_contract') is not None:
            raise ValueError('Declared month-period header contract does not match the exact captured source')
        return original, {}
    months = {month_key(label) for label in request["date_labels"]}
    if (request.get("start") and min(months) < month_key(request["start"])
            or request.get("end") and max(months) > month_key(request["end"])):
        raise ValueError("Source month scope outside requested bounds")
    return months, {
        "source_period_key_contract": month_header_contract(payload),
        "source_period_grain": "monthly",
        "source_period_basis": "literal_"+payload['source_key_headers'][1]+"_header_not_rendered_day_or_publication_time",
        "requested_grid_periods": len(original),
        "source_period_scope_periods": len(months),
        "grid_to_source_period_mapping": "selected_grid_months_no_daily_value_expansion",
        "source_observation_cadence_verified": False,
    }


def replan_month_periods(root: Path, task_id: str, bridge) -> dict:
    """Use one metadata-only Monthly readback and preserve accepted query scope.

    Caller owns the canonical lock. Never change a prepared source request,
    invent monthly menu labels, replay Preview or rewrite successful receipts.
    A failed readback leaves the old plan intact and visibly requires recovery.
    """
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import (connect, task_request, validate_download_evidence,
                                        validate_source_scope, compact_request, stable_id, SOURCE_SCOPE_CONTRACT)
    from downloader.tej_planning import build_plan, refill_ready_plans, CONTRACT as PLAN_CONTRACT

    with closing(connect(root)) as con:
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                       "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError("Unknown/running Preview blocks source period migration")
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None or row['kind'] != 'download' or row['state'] != 'complete':
            raise ValueError("One completed captured YYYYMM task is required")
        task = dict(row)
        definition = dict(con.execute("SELECT * FROM tables WHERE table_id=?", (task['table_id'],)).fetchone())
        stored = con.execute("SELECT * FROM download_plans WHERE table_id=?", (task['table_id'],)).fetchone()
        if stored is None or stored['contract'] != PLAN_CONTRACT:
            raise ValueError("Compatible canonical lazy plan required")
        old = json.loads(stored['plan_json'])
        if old['request'].get('source_period_key_contract') in MONTH_CONTRACTS:
            return {'state': 'source_period_plan_current', 'provider_queries_sent': 0}
        if old['request'].get('frequency') != 'daily':
            raise ValueError('Only redundant daily month-key grids are automatically coarsened; retain quarterly/yearly cadence')
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        completed = [dict(r) for r in con.execute(
            "SELECT * FROM tasks WHERE table_id=? AND kind='download' AND state='complete' AND scope_contract=?",
            (task['table_id'], SOURCE_SCOPE_CONTRACT))]
        completed.sort(key=lambda done:(done.get('completed_at_utc') or '',done['task_id']))
    # Revalidate the saved request and exact source evidence; a forged receipt
    # cannot turn an ordinary Date table into a monthly table.
    scopes = []; contract=None;legacy_scope_proofs=[]
    for done in completed:
        req = task_request(root, done)
        raw = (root / done['output_path']).resolve()
        receipt_file = (root / done['receipt_path']).resolve()
        if (raw.parent != (root/'raw').resolve() or not raw.is_file() or raw.stat().st_size > 64*1024**2
                or receipt_file.parent != (root/'receipts').resolve() or receipt_file.stat().st_size > 1024**2):
            raise ValueError("Bounded exact saved monthly source evidence required")
        receipt = json.loads(receipt_file.read_text(encoding='utf-8-sig'))
        if receipt.get('raw_sha256') != hashlib.sha256(raw.read_bytes()).hexdigest():
            raise ValueError("Saved monthly source evidence hash changed")
        payload = interpreted_payload(root, done, json.loads(raw.read_text(encoding='utf-8-sig')))
        if receipt.get('source_outcome') == 'explicit_empty_scope':
            # An old daily-grid empty reply proves neither monthly history nor
            # emptiness of this different native query mode. Keep its receipt.
            continue
        _, original_rows, profile = validate_download_evidence(req, payload)
        captured_contract=profile.get('source_period_key_contract');legacy_profile=False
        if (captured_contract is None and receipt.get('source_period_key_contract') is None
                and payload.get('source_period_key_contract') is None and req.get('frequency')=='daily'
                and month_header_contract(payload) is not None):
            # Planning-only interpretation of a previously adopted day ABI.
            # Keep its source, receipt and Parquet untouched. Validate both
            # the original ABI and exact literal month header/unique keys.
            from downloader.tej_key_layout import parquet_relative
            parquet=(root/receipt['parquet_path']).resolve()
            if (parquet!=(root/parquet_relative(done,receipt)).resolve() or not parquet.is_file()
                    or parquet.stat().st_size>64*1024**2
                    or receipt.get('parquet_sha256')!=hashlib.sha256(parquet.read_bytes()).hexdigest()
                    or receipt.get('exported_rows')!=len(original_rows)
                    or receipt.get('exported_non_null_cells')!=sum(profile['non_null_counts'])):
                raise ValueError('Legacy planning coverage lacks exact original artifact/count proof')
            captured_contract=month_header_contract(payload)
            _,_,profile=validate_download_evidence(req,{**payload,'source_period_key_contract':captured_contract})
            legacy_profile=True
            legacy_scope_proofs.append({'task_id':done['task_id'],'original_period_contract':None,
                'planning_period_contract':captured_contract,'raw_sha256':receipt['raw_sha256'],
                'parquet_sha256':receipt['parquet_sha256'],
                'receipt_sha256':hashlib.sha256(receipt_file.read_bytes()).hexdigest(),
                'source_and_published_abi_changed':False})
        if (captured_contract not in MONTH_CONTRACTS
                or not legacy_profile and receipt.get('source_period_key_contract')!=captured_contract
                or payload.get('task_id') != done['task_id']):
            raise ValueError("Completed non-monthly scope requires separate explicit migration")
        if contract is not None and contract!=captured_contract:
            raise ValueError('Mixed source month-key contracts require an explicit migration')
        contract=captured_contract
        scopes.append((done, req, {month_key(label) for label in req['date_labels']}))
    if not any(done['task_id'] == task_id for done, _, _ in scopes):
        raise ValueError("Trigger task lacks verified nonempty YYYYMM evidence")
    request = {**old['request'], 'action': 'plan', 'frequency': 'monthly',
               'source_period_key_contract': contract}
    metadata_id = stable_id([task['table_id'], old['fingerprint'], contract, 'monthly_source_axis'])
    metadata_task = {**task, 'task_id': metadata_id, 'kind': 'discover',
                     'request_json': json.dumps(request, ensure_ascii=False)}
    with closing(connect(root)) as con, con:
        con.execute("INSERT OR REPLACE INTO meta VALUES ('source_period_replan_required',?)", (task_id,))
    attempted_at = datetime.now(UTC).isoformat()
    payload, output, metadata_seconds = bridge.execute(root, metadata_task)
    validate_source_scope(request, payload, 'plan')
    if (payload.get('task_id') != metadata_id or payload.get('frequency') != 'monthly'
            or payload.get('frequency_selection_readback_contract') != 'owned_checked_frequency_button_v1'):
        raise ValueError("Exact selected Monthly source control readback required")
    companies, dates = payload.get('company_labels'), payload.get('date_labels')
    if (not isinstance(companies, list) or not isinstance(dates, list) or not companies or not dates
            or any(not isinstance(label, str) or not label for label in companies + dates)
            or len(set(companies)) != len(companies) or len(set(dates)) != len(dates)):
        raise ValueError("Monthly source menus are empty, duplicated or invalid")
    by_month = {month_key(label): label for label in dates}
    if (len(by_month) != len(dates) or min(by_month) < month_key(request['start'])
            or max(by_month) > month_key(request['end'])):
        raise ValueError("Monthly source grid is not one bounded label per month")
    coverage = []
    for done, req, months in scopes:
        if not months <= by_month.keys():
            raise ValueError("Accepted month scope absent from new verified source axis")
        coverage.append({**req, 'date_labels': [by_month[month] for month in sorted(months)]})
    weights=union_scope_weights(request,scopes,by_month)
    plan = build_plan(request, companies, dates, config, coverage)
    if sum(weight for weight, _ in weights) != plan['completed_work_rows']:
        raise ValueError("Monthly coverage union differs from the canonical plan")
    audit_file = root/'period_key_repairs'/(task_id+'-'+uuid.uuid4().hex+'.json')
    audit = {'contract': contract, 'task_id': task_id, 'table_id': task['table_id'],
             'observed_at_utc': datetime.now(UTC).isoformat(), 'original_plan': old,
             'old_remaining_queries': stored['total_queries']-stored['next_query'],
             'new_remaining_queries': plan['total_queries'], 'new_plan_fingerprint': plan['fingerprint'],
             'metadata_readback_path': str(output.relative_to(root)),
             'metadata_readback_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
             'completed_month_scope_rows_preserved': plan['completed_work_rows'],
             'planning_only_legacy_month_coverage':legacy_scope_proofs,
             'completed_scope_union_weights':dict((task_id,weight) for weight,task_id in weights),
             'data_query_repeated': False, 'source_files_deleted': False,
             'source_dates_backdated': False, 'monthly_values_expanded_to_daily': False}
    atomic_write_json(audit_file, audit)
    with closing(connect(root)) as con, con:
        current = con.execute("SELECT * FROM download_plans WHERE table_id=?", (task['table_id'],)).fetchone()
        if dict(current) != dict(stored):
            raise ValueError("Canonical plan changed during metadata readback")
        con.execute("UPDATE tasks SET state='superseded_month_period_grid_v1' WHERE table_id=? AND kind='download' "
                    "AND scope_contract=? AND state='pending'", (task['table_id'], SOURCE_SCOPE_CONTRACT))
        con.execute("UPDATE download_plans SET plan_json=?,next_query=0,total_queries=? WHERE table_id=?",
                    (json.dumps(plan, ensure_ascii=False, separators=(',', ':')), plan['total_queries'], task['table_id']))
        con.execute("UPDATE tables SET frequency='monthly',universe_count=?,grid_dates=?,grid_rows=?,work_grid_rows=?,"
                    "field_batches=?,first_available_query_period=?,last_available_query_period=?,discovery_path=?,"
                    "axis_profile_basis='verified_Monthly_menu_with_literal_month_period_key_not_release_time',"
                    "state='backfilling',last_error_code=NULL WHERE table_id=?",
                    (len(companies), len(dates), len(companies)*len(dates), plan['total_work_rows'], plan['field_batches'],
                     min(by_month), max(by_month), str(output.relative_to(root)), task['table_id']))
        con.execute("UPDATE tasks SET work_expected_rows=0 WHERE table_id=? AND kind='download' AND state='complete' "
                    "AND scope_contract=?", (task['table_id'], SOURCE_SCOPE_CONTRACT))
        con.executemany("UPDATE tasks SET work_expected_rows=? WHERE task_id=?", weights)
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,actual_bytes,seconds,"
                    "attempted_at_utc,completed_at_utc,output_path,timing_basis,scope_contract) "
                    "VALUES(?,?,'discover',?,?,?,'complete',?,?,?,?,?,'fresh_period_axis_metadata',?)",
                    (metadata_id, task['table_id'], task['phase'], int(task['phase'][1])*100+1,
                     json.dumps(compact_request(request, definition), ensure_ascii=False, separators=(',', ':')),
                     output.stat().st_size, metadata_seconds, attempted_at, datetime.now(UTC).isoformat(),
                     str(output.relative_to(root)), SOURCE_SCOPE_CONTRACT))
        con.execute("DELETE FROM meta WHERE key='source_period_replan_required' AND value=?", (task_id,))
        refill_ready_plans(con)
    return {'state': 'source_period_plan_replanned', 'source_period_key_contract': contract,
            'frequency': 'monthly', 'old_remaining_queries': audit['old_remaining_queries'],
            'new_remaining_queries': plan['total_queries'], 'completed_month_scope_rows_preserved': plan['completed_work_rows'],
            'data_query_repeated': False, 'source_files_deleted': False}
