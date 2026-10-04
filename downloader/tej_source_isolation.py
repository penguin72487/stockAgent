"""Quarantine a fully captured invalid TEJ response without blocking other tables.

An invalid observation is never adopted or retried. Unknown source outcomes,
wrong bindings, missing attempts and malformed captures remain global barriers.
Caller owns the canonical dataset lock; this module never operates the desktop.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json

CONTRACT = 'completed_native_response_table_isolation_v1'
STATE = 'source_validation_failed_deferred'


def isolate_invalid_response(root: Path, task_id: str) -> dict:
    from downloader.tej_desktop_attempts import prepared_request_matches, query_stage, validate_active_attempt
    from downloader.tej_header_mapping import validate_headers
    from downloader.tej_history import connect, task_request, validate_preview, validate_source_scope
    from downloader.tej_key_layout import key_count

    with closing(connect(root)) as con, con:
        row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if row is None:
            raise ValueError('Unknown invalid-response task')
        task = dict(row)
        active = task['active_attempt_id']
        if (task['kind'] != 'download' or task['state'] != 'blocked'
                or task['last_error_code'] not in ('source_validation_failed', STATE)
                or not isinstance(active, str) or not re.fullmatch(re.escape(task_id)+r'-[0-9a-f]{32}', active)
                or any(task.get(k) is not None for k in ('actual_rows','receipt_path','completed_at_utc'))):
            raise ValueError('Only a current unadopted invalid source response may be isolated')
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                       "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError('Unknown or inflight source action remains a global barrier')
        if con.execute("SELECT 1 FROM meta WHERE key IN ('desktop_interface_recovery_required','source_period_replan_required')").fetchone():
            raise ValueError('Shared source interface must be reconciled first')
        attempt = con.execute('SELECT * FROM desktop_attempts WHERE attempt_id=?', (active,)).fetchone()
        if (attempt is None or attempt['task_id'] != task_id or attempt['finished_at_utc'] is None
                or attempt['state'] not in ('response_received_not_yet_adopted','validated_response_quarantined')):
            raise ValueError('Exact finished native response required')
        output = root / 'raw' / (active + '.json')
        if (output.resolve().parent != (root / 'raw').resolve() or not output.is_file()
                or output.stat().st_size > 64 * 1024**2):
            raise ValueError('Exact bounded immutable native response required')
        body = output.read_bytes()
        payload = json.loads(body.decode('utf-8-sig'))
        if not isinstance(payload,dict):
            raise ValueError('Native response root must be an object')
        request = task_request(root, task)
        prepared = root / 'requests' / (active + '.json')
        stage, stage_path = query_stage(root, task, request, prepared)
        if not prepared_request_matches(json.loads(prepared.read_text(encoding='utf-8-sig')), request, task):
            raise ValueError('Prepared capacity or source scope differs')
        validate_active_attempt(root, task, payload)
        validate_source_scope(request, payload, 'download')
        cells = payload.get('cells')
        keys = key_count(request)
        columns = len(request['fields']) + keys
        if (payload.get('task_id') != task_id or payload.get('fresh_preview_transition_verified') is not True
                or payload.get('capture_method') != 'native_msaa_preview_full'
                or payload.get('source_value_representation') != 'vendor_display_strings_not_underlying_excel_values'
                or payload.get('source_outcome') not in (None,'exported_query_grid')
                or payload.get('query_comments_read') is not False
                or not isinstance(cells, list) or len(cells) < 2
                or payload.get('source_grid_rows') != len(cells)-1
                or payload.get('source_grid_columns') != columns
                or len(cells)-1 > request['max_rows'] or len(cells)*columns > request['max_cells']
                or any(not isinstance(r,list) or len(r)!=columns for r in cells)):
            raise ValueError('Source outcome is not a fully captured bounded response')
        headers = payload.get('source_key_headers')
        if (not isinstance(headers,list) or len(headers)!=keys
                or any(not isinstance(h,str) or not h or len(h)>128 for h in headers)):
            raise ValueError('Exact native key headers required')
        validate_headers(request, payload, headers)
        validate_preview(cells[0], cells, payload.get('preview'), date_system=payload.get('date_system'), keys_count=keys)
        try:
            clocks = [datetime.fromisoformat(v.replace('Z','+00:00')) for v in
                      (attempt['started_at_utc'],stage['observed_at_utc'],payload['observed_at_utc'],attempt['finished_at_utc'])]
        except (KeyError,TypeError,AttributeError,ValueError) as exc:
            raise ValueError('Exact native response timestamps required') from exc
        if any(t.tzinfo is None for t in clocks) or not clocks[0] <= clocks[1] <= clocks[2] <= clocks[3]:
            raise ValueError('Native response is outside its exact attempt clock')
        evidence = {'response_sha256':hashlib.sha256(body).hexdigest(),
                    'prepared_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
                    'query_stage_sha256':hashlib.sha256(stage_path.read_bytes()).hexdigest()}
        audit_path = root / 'source_isolation' / (active + '.json')
        if task['last_error_code'] == STATE:
            previous = json.loads(audit_path.read_text())
            if previous.get('evidence') != evidence or previous.get('contract') != CONTRACT:
                raise ValueError('Quarantined evidence changed; preserve safety barrier')
            return {**previous['result'],'already_isolated':True}
        result = {'state':STATE,'task_id':task_id,'table_id':task['table_id'],
                  'provider_queries_sent':0,'data_query_repeated':False,'source_rows_adopted':False,
                  'table_blocked':True,'other_tables_may_continue':True,
                  'audit':str(audit_path.relative_to(root))}
        audit = {'contract':CONTRACT,'state':'prepared','observed_at_utc':datetime.now(UTC).isoformat(),
                 'task_id':task_id,'attempt_id':active,'original_error':'source_validation_failed',
                 'evidence':evidence,'source_files_retained':True,'validation_relaxed':False,'result':result}
        atomic_write_json(audit_path,audit,durable=True)
        con.execute('UPDATE tasks SET last_error_code=? WHERE task_id=?', (STATE,task_id))
        con.execute("UPDATE tables SET state='needs_review',last_error_code=? WHERE table_id=?", (STATE,task['table_id']))
        con.execute("UPDATE desktop_attempts SET state='validated_response_quarantined' WHERE attempt_id=?", (active,))
    audit['state'] = 'committed'
    # The durable intent precedes classification. A failed final marker does
    # not undo quarantine or authorize another source query.
    try:
        atomic_write_json(audit_path,audit,durable=True)
    except OSError:
        result = {**result,'audit_commit_marker_pending':True}
    return result
