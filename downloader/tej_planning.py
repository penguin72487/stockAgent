"""Bounded TEJ Preview field partitions and on-demand history tasks.

Only verified native company/date menus are inputs. A logical query plan is
not a native observation count. Existing completed query scopes are excluded;
we never fabricate rows for the omitted/sparse source response.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from calendar import monthrange
import hashlib
import json
import math

CONTRACT = 'preview_30_columns_lazy_fields_v1'
MAX_PREVIEW_COLUMNS = 30
TILING_CONTRACT = 'minimum_uniform_company_date_rectangles_v1'
CAPACITY_CONTRACT = 'native_row_capacity_replanning_v1'
OPERATOR_GEOMETRY_CONTRACT = 'operator_unknown_record_geometry_prior_v1'
LOCAL_CAPACITY_CONTRACT = 'native_preview_full_local_capacity_v1'
SELECTOR_CONTRACT = 'bounded_source_selector_geometry_v1'
MAX_SELECTOR_LABELS = 2000  # ExactListIndices / SelectListBatch / TrySelectListBulk.
COMPANY_MEMBERSHIP_CONTRACT = 'exact_current_company_membership_v1'
COMPANY_MEMBERSHIP_DEFERRED = 'source_company_labels_absent_fragment'
COMPANY_HINT_CONTRACT = 'prior_negative_company_membership_scheduling_only_v1'


def rectangle_queries(companies: int, dates: int, rows: int, size: int, keys: int,
                      date_cap: int | None = None) -> int:
    groups, tail = divmod(companies, size)
    def count(width):
        return 1 if keys == 1 else math.ceil(dates / min(rows // width, date_cap or rows))
    return groups * count(size) + (count(tail) if tail else 0)


def company_batch_size(companies: int, dates: int, rows: int, cap: int, keys: int,
                       *, optimize: bool = False, date_cap: int | None = None) -> int:
    """Minimum call count among bounded uniform rectangular tilings.

    This does not claim the unrestricted global packing optimum. Enumerating
    at most the configured company cap avoids a Cartesian work queue or a
    large dynamic program, and never increases the existing row/cell bounds.
    """
    maximum = min(cap, rows)
    if type(maximum) is not int or maximum < 1 or maximum > 1024:
        raise ValueError('Invalid bounded company tiling cap')
    if date_cap is not None and (type(date_cap) is not int or not 1 <= date_cap <= MAX_SELECTOR_LABELS):
        raise ValueError('Invalid bounded source date selector cap')
    if not optimize or keys == 1 or companies == 0:
        return maximum
    return min(range(1, min(maximum, companies) + 1),
               key=lambda size: (rectangle_queries(companies, dates, rows, size, keys, date_cap), -size))


def _ranges(indices: list[int]) -> list[list[int]]:
    result=[]
    for index in indices:
        if result and result[-1][1] == index:
            result[-1][1]+=1
        else:
            result.append([index,index+1])
    return result


def build_plan(request: dict, companies: list[str], dates: list[str], config: dict,
               completed: list[dict]) -> dict:
    fields=request['fields']; limit=config.get('preview_max_columns',MAX_PREVIEW_COLUMNS)
    selector = config.get('query_selector_contract')
    if selector not in (None, SELECTOR_CONTRACT):
        raise ValueError('Unreviewed source selector geometry contract')
    tiling = config.get('query_tiling_contract')
    if tiling not in (None, TILING_CONTRACT):
        raise ValueError('Unreviewed company/date tiling contract')
    if isinstance(limit,bool) or limit != MAX_PREVIEW_COLUMNS:
        raise ValueError('Preview cap is exactly the reviewed 30 total columns')
    from downloader.tej_key_layout import key_count
    keys=key_count(request)
    density=request.get('native_record_density_hint',1)
    if (type(density) is not int or not 1 <= density <= 1000000
            or request.get('record_capacity_contract') not in (None,CAPACITY_CONTRACT,OPERATOR_GEOMETRY_CONTRACT)
            or density!=1 and (keys!=3 or request.get('record_capacity_contract') not in (CAPACITY_CONTRACT,OPERATOR_GEOMETRY_CONTRACT))):
        raise ValueError('Unverified native record capacity hint')
    width=limit-keys
    if not fields or len(fields) > 2000 or len(set(companies)) != len(companies) or len(set(dates)) != len(dates):
        raise ValueError('Unverified field/company/date plan')
    company_index={label:i for i,label in enumerate(companies)}
    date_index={label:i for i,label in enumerate(dates)}
    if keys==1:
        if dates or completed:
            raise ValueError('Snapshot plans never inherit historical date coverage')
        segments=[]; queries=0
        for begin in range(0,len(fields),width):
            stop=min(len(fields),begin+width)
            rows=min(request['max_rows'],request['max_cells']//(stop-begin+1)-1)
            size=min(config['max_companies_per_export'],rows)
            if type(size) is not int or size<1:
                raise ValueError('Snapshot exceeds reviewed company/cell bound')
            for index in range(0,len(companies),size):
                queries+=1
                segments.append({'field_range':[begin,stop],'companies':list(range(index,min(index+size,len(companies)))),
                                 'date_ranges':[],'dates_per_task':0,'queries':1,'query_end':queries})
        total_rows=math.ceil(len(fields)/width)*len(companies)
        core={'contract':CONTRACT,'request':request,'companies':companies,'dates':[],
              'segments':segments,'total_queries':queries,'remaining_work_rows':total_rows,
              'total_work_rows':total_rows,'completed_work_rows':0,
              'field_batches':math.ceil(len(fields)/width),'preview_max_columns':limit}
        if selector:
            core['query_selector_contract'] = selector
        return {**core,'fingerprint':hashlib.sha256(json.dumps(core,sort_keys=True,ensure_ascii=False).encode()).hexdigest()}
    coverage=[]
    for done in completed:
        if (key_count(done) != keys
                or any(done.get(k) != request.get(k) for k in ('type','smart_id','table'))
                or not set(done['fields']) <= set(fields)
                or not set(done['company_labels']) <= company_index.keys()
                or not set(done['date_labels']) <= date_index.keys()):
            raise ValueError('Completed query proof is outside the verified plan')
        coverage.append((set(done['fields']),set(done['company_labels']),{date_index[x] for x in done['date_labels']}))
    segments=[]; query_count=0; remaining_rows=0
    missing_cache={}
    for begin in range(0,len(fields),width):
        stop=min(len(fields),begin+width); selected=set(fields[begin:stop]); columns=stop-begin+keys
        rows=min(request['max_rows'],request['max_cells']//columns-1)
        # Key=3 means several native records may share a company/period.
        # The hint bounds query geometry, not actual observation counts.
        rows //= density
        if rows < 1:
            raise ValueError('Preview partition exceeds output bound')
        company_size=company_batch_size(len(companies),len(dates),rows,config['max_companies_per_export'],keys,
                                        optimize=tiling == TILING_CONTRACT,
                                        date_cap=MAX_SELECTOR_LABELS if selector else None)
        if isinstance(company_size,bool) or company_size < 1:
            raise ValueError('Invalid local company batch bound')
        for c in range(0,len(companies),company_size):
            grouped=defaultdict(list)
            for index in range(c,min(len(companies),c+company_size)):
                covered=set()
                for names,labels,periods in coverage:
                    if selected <= names and companies[index] in labels:
                        covered.update(periods)
                # Compact ranges keep century-sized date axes stored ONCE.
                grouped[frozenset(covered)].append(index)
            for covered,indices in grouped.items():
                if covered not in missing_cache:
                    missing=[i for i in range(len(dates)) if i not in covered]
                    missing_cache[covered]=(len(missing),_ranges(missing))
                missing_count,ranges=missing_cache[covered]
                if not missing_count:
                    continue
                date_size=max(1,rows//len(indices))
                if selector:
                    date_size=min(date_size,MAX_SELECTOR_LABELS)
                chunks=math.ceil(missing_count/date_size)
                segments.append({'field_range':[begin,stop],'companies':indices,
                                 'date_ranges':ranges,'dates_per_task':date_size,
                                 'queries':chunks,'query_end':query_count+chunks})
                query_count+=chunks; remaining_rows+=len(indices)*missing_count
    total_rows=math.ceil(len(fields)/width)*len(companies)*len(dates)
    core={'contract':CONTRACT,'request':request,'companies':companies,'dates':dates,
          'segments':segments,'total_queries':query_count,'remaining_work_rows':remaining_rows,
          'total_work_rows':total_rows,'completed_work_rows':total_rows-remaining_rows,
          'field_batches':math.ceil(len(fields)/width),'preview_max_columns':limit}
    if tiling:
        core['query_tiling_contract'] = tiling
    if selector:
        core['query_selector_contract'] = selector
    return {**core,'fingerprint':hashlib.sha256(json.dumps(core,sort_keys=True,ensure_ascii=False).encode()).hexdigest()}


def next_request(plan: dict, ordinal: int) -> dict:
    from downloader.tej_history import normalize_period

    if plan.get('contract') != CONTRACT or not 0 <= ordinal < plan['total_queries']:
        raise ValueError('Exhausted or incompatible Preview plan')
    core={k:v for k,v in plan.items() if k!='fingerprint'}
    if hashlib.sha256(json.dumps(core,sort_keys=True,ensure_ascii=False).encode()).hexdigest() != plan.get('fingerprint'):
        raise ValueError('Download plan fingerprint differs; reject changed scope')
    # Weave field batches rather than finishing decades of the first 28
    # features before ever sampling feature 29. Different widths can fit
    # different date counts; use exact counts, not a guessed modulo.
    field_counts=[]; fields=[]
    for segment in plan['segments']:
        if not fields or fields[-1]!=segment['field_range']:
            fields.append(segment['field_range']);field_counts.append(0)
        field_counts[-1]+=segment['queries']
    lo,hi=0,max(field_counts)
    while lo+1<hi:
        middle=(lo+hi)//2
        if sum(min(middle,n) for n in field_counts)<=ordinal:
            lo=middle
        else:
            hi=middle
    remainder=ordinal-sum(min(lo,n) for n in field_counts);prefix=0
    for count in field_counts:
        if count>lo:
            if remainder==0:
                ordinal=prefix+lo;break
            remainder-=1
        prefix+=count
    offset=0
    for segment in plan['segments']:
        if ordinal < segment['query_end']:
            break
        offset=segment['query_end']
    start=(ordinal-offset)*segment['dates_per_task']; stop=start+segment['dates_per_task']
    # Slice only one bounded date group, never an all-history Cartesian list.
    selected=[]; skipped=0
    for begin,end in segment['date_ranges']:
        if skipped+end-begin > start and skipped < stop:
            lo=begin+max(0,start-skipped); hi=begin+min(end-begin,stop-skipped)
            selected.extend(plan['dates'][lo:hi])
        skipped+=end-begin
        if skipped >= stop:
            break
    first,last=segment['field_range']; base=plan['request']
    query={**base,'action':'download','fields':base['fields'][first:last],
           'company_labels':[plan['companies'][i] for i in segment['companies']], 'date_labels':selected,
           'field_partition':{'contract':CONTRACT,'field_range':[first,last],
                              'catalog_fields':len(base['fields']),'plan_fingerprint':plan['fingerprint']}}
    periods=[normalize_period(p) for p in selected]
    if periods and all(len(p)==10 for p in periods):
        query.update(start=min(periods),end=max(periods))
    elif periods and all(len(p)==7 for p in periods):
        earliest,latest=min(periods),max(periods); year,month=map(int,latest.split('-'))
        lower=(date.fromisoformat(earliest+'-01')-timedelta(days=1)).isoformat()
        query.update(start=max(base['start'],lower),end=min(base['end'],f'{latest}-{monthrange(year,month)[1]:02d}'))
    return query


def install_plan(con, definition: dict, request: dict, payload: dict, config: dict) -> dict:
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, expand_request

    existing=con.execute('SELECT * FROM download_plans WHERE table_id=?',(definition['table_id'],)).fetchone()
    if existing:
        if existing['contract'] != CONTRACT:
            raise ValueError('Incompatible durable download plan')
        return {'logical_queries':existing['total_queries'],'already_installed':True}
    completed=[]; completed_tasks=[]
    for row in con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND state='complete' AND scope_contract=?",(definition['table_id'],SOURCE_SCOPE_CONTRACT)):
        completed.append(expand_request(json.loads(row['request_json']),definition,definition['table_id']))
        completed_tasks.append(dict(row))
    if payload.get('source_key_mode') in (1,3):
        from downloader.tej_key_layout import KEY1_CONTRACT, KEY3_CONTRACT
        mode=payload['source_key_mode']
        request={**request,'source_key_mode':mode,'key_layout_contract':KEY1_CONTRACT if mode==1 else KEY3_CONTRACT}
        if mode==1:
            request['frequency']='snapshot'
    if config.get('query_capacity_contract') == LOCAL_CAPACITY_CONTRACT:
        # Discovery captures menus, not market observations. The next data
        # request may use the explicitly upgraded output memory budget.
        request={**request,'max_rows':config['max_rows_per_export'],'max_cells':config['max_cells_per_export']}
    plan=build_plan(request,payload['company_labels'],payload['date_labels'],config,completed)
    weights=[]
    for done,task in zip(completed,completed_tasks):
        weight=(plan['field_batches'] if done['fields']==request['fields'] else 1)
        weights.append((weight*task['expected_rows'],task['task_id']))
    if sum(weight for weight,_ in weights) != plan['completed_work_rows']:
        raise ValueError('Overlapping or unaligned completed field scopes; do not refetch silently')
    from downloader.tej_value_priority import installed_priority
    priority = installed_priority(con, definition['table_id'], int(definition['phase'][1])*100)
    con.execute('INSERT INTO download_plans VALUES(?,?,?,?,?,?,?)',
                (definition['table_id'],CONTRACT,json.dumps(plan,ensure_ascii=False,separators=(',',':')),
                 0,plan['total_queries'],definition['phase'],priority))
    con.executemany('UPDATE tasks SET work_expected_rows=? WHERE task_id=?',weights)
    # Source evidence and failed receipts remain; this is a queue encoding /
    # field-partition upgrade, not deletion or relabelling successful data.
    con.execute("UPDATE tasks SET state='superseded_preview_partition_v1' WHERE table_id=? AND kind='download' AND scope_contract=? AND (state='pending' OR (state='blocked' AND last_error_code='preview_column_limit_repartition_required'))",
                (definition['table_id'],SOURCE_SCOPE_CONTRACT))
    con.execute('UPDATE tables SET work_grid_rows=?,field_batches=?,last_error_code=CASE WHEN last_error_code=\'preview_column_limit_repartition_required\' THEN NULL ELSE last_error_code END WHERE table_id=?',
                (plan['total_work_rows'],plan['field_batches'],definition['table_id']))
    if not con.execute("SELECT 1 FROM tasks WHERE table_id=? AND state='blocked' AND scope_contract=? LIMIT 1",(definition['table_id'],SOURCE_SCOPE_CONTRACT)).fetchone():
        con.execute("UPDATE tables SET state='backfilling' WHERE table_id=? AND grid_rows>0",(definition['table_id'],))
    return {'logical_queries':plan['total_queries'],'field_batches':plan['field_batches'],
            'completed_work_rows_preserved':plan['completed_work_rows'],'already_installed':False}


def refill_ready_plans(con) -> int:
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, compact_request, stable_id
    from downloader.tej_value_priority import installed_priority

    generated=0
    plans=con.execute("SELECT p.table_id,p.next_query,p.phase,p.priority FROM download_plans p WHERE p.next_query<p.total_queries AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.table_id=p.table_id AND t.kind='download' AND t.scope_contract=? AND t.state IN ('pending','running','blocked') AND NOT(t.state='pending' AND COALESCE(t.last_error_code,'')=?)) ORDER BY p.priority,p.table_id",(SOURCE_SCOPE_CONTRACT,COMPANY_MEMBERSHIP_DEFERRED)).fetchall()
    for row in plans:
        definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(row['table_id'],)).fetchone())
        encoded=con.execute('SELECT plan_json FROM download_plans WHERE table_id=?',(row['table_id'],)).fetchone()[0]
        plan=json.loads(encoded); query=next_request(plan,row['next_query'])
        from downloader.tej_key_layout import key_count
        key=stable_id([row['table_id'],query]); expected=len(query['company_labels'])*(1 if key_count(query)==1 else len(query['date_labels']))
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,scope_contract,work_expected_rows) VALUES(?,?,?,?,?,?,?,?,?)',
                    (key,row['table_id'],'download',row['phase'],installed_priority(con,row['table_id'],row['priority']),
                     json.dumps(compact_request(query,definition),ensure_ascii=False,separators=(',',':')),
                     expected,SOURCE_SCOPE_CONTRACT,expected))
        con.execute('UPDATE download_plans SET next_query=next_query+1 WHERE table_id=?',(row['table_id'],))
        generated+=1
    return generated


def repartition_selector_pending(con, root, row) -> bool:
    """Split only an unsubmitted pending fragment, before native UI work.

    Reuse the canonical rectangular planner. Original plans/failed attempts and
    every field/company/date remain; no successful or unknown scope is reset.
    The selector's reviewed bound is not a provider rate limit or row count.
    """
    from datetime import UTC, datetime
    import uuid
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, expand_request, compact_request, stable_id
    from downloader.tej_key_layout import key_count
    if row['kind'] != 'download' or row['state'] != 'pending' or row['scope_contract'] != SOURCE_SCOPE_CONTRACT:
        return False
    definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(row['table_id'],)).fetchone())
    request=expand_request(json.loads(row['request_json']),definition,row['table_id'])
    if max(len(request['company_labels']),len(request['date_labels'])) <= MAX_SELECTOR_LABELS:
        return False
    if row['active_attempt_id']:
        from downloader.tej_desktop_attempts import proved_unsent_prequery
        proved_unsent_prequery(root,con,dict(row),finished=True)
    elif row['attempted_at_utc'] is not None:
        raise ValueError('Previously attempted selector scope requires an exact unsent proof')
    if any(row[k] is not None for k in ('actual_rows','completed_at_utc','receipt_path','output_path')):
        raise ValueError('Selector repartition cannot replace observed source data')
    if len(request['fields'])+key_count(request)>MAX_PREVIEW_COLUMNS:
        raise ValueError('Selector geometry cannot substitute for field partitioning')
    config={'preview_max_columns':MAX_PREVIEW_COLUMNS,'query_tiling_contract':TILING_CONTRACT,
        'query_selector_contract':SELECTOR_CONTRACT,
        'max_companies_per_export':min(len(request['company_labels']),1024)}
    plan=build_plan(request,request['company_labels'],request['date_labels'],config,[])
    children=[]
    for index in range(plan['total_queries']):
        selected=next_request(plan,index)
        part={**request,**{k:selected[k] for k in ('company_labels','date_labels','start','end')},
            'selector_partition':{'contract':SELECTOR_CONTRACT,'parent_task_id':row['task_id'],
                'plan_fingerprint':plan['fingerprint'],'max_labels_per_axis':MAX_SELECTOR_LABELS}}
        expected=len(part['company_labels'])*(1 if key_count(part)==1 else len(part['date_labels']))
        children.append((stable_id([row['table_id'],part]),part,expected))
    work=sum(count for _,_,count in children)
    if (len(children)<2 or work!=row['expected_rows'] or work!=(row['work_expected_rows'] or row['expected_rows'])
            or any(con.execute('SELECT 1 FROM tasks WHERE task_id=?',(identifier,)).fetchone()
                   for identifier,_,_ in children)):
        raise ValueError('Exact new disjoint selector work and unchanged denominator required')
    retry=con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?',(row['table_id'],)).fetchone()
    if retry is not None and retry['task_id'] != row['task_id']:
        raise ValueError('Another original fragment owns the table retry window')
    audit_path=root/'selector_replanning'/(row['task_id']+'-'+uuid.uuid4().hex+'.json')
    audit={'contract':SELECTOR_CONTRACT,'state':'prepared','observed_at_utc':datetime.now(UTC).isoformat(),
        'original_task':dict(row),'original_request_sha256':hashlib.sha256(json.dumps(request,sort_keys=True).encode()).hexdigest(),
        'plan':plan,'replacement_task_ids':[identifier for identifier,_,_ in children],
        'work_rows_before':row['work_expected_rows'] or row['expected_rows'],'work_rows_after':work,
        'provider_queries_sent':0,'source_rows_adopted':False,'original_attempt_preserved':True,
        'retry_budget_reset':False,'table_retry_window_preserved':dict(retry) if retry else None}
    atomic_write_json(audit_path,audit,durable=True)
    con.execute("UPDATE tasks SET state='superseded_selector_geometry_v1' WHERE task_id=?",(row['task_id'],))
    for identifier,part,expected in children:
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,scope_contract,'
            'work_expected_rows,safe_prequery_retries,next_attempt_at_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
            (identifier,row['table_id'],'download',row['phase'],row['priority'],
             json.dumps(compact_request(part,definition),ensure_ascii=False,separators=(',',':')),
             expected,SOURCE_SCOPE_CONTRACT,expected,row['safe_prequery_retries'],row['next_attempt_at_utc']))
    if retry is not None:
        con.execute('UPDATE desktop_retry_windows SET task_id=? WHERE table_id=?',(children[0][0],row['table_id']))
    return True  # Caller commits the queue; durable intent is not a download receipt.


def company_membership_failure(root, task: dict, verified: dict) -> dict | None:
    """Validate a COMPLETE native lookup; absence is not a missing observation.

    The ordinary finished negative-attempt proof must be checked first. A
    missing sidecar, timeout, duplicate label or changed source never licenses
    partitioning. The sidecar contains positions, not guessed replacement IDs.
    """
    from datetime import datetime
    import re
    from downloader.tej_history import DesktopBridge
    from downloader.tej_operation_clock import source_operation_clock
    digest = verified['outcome'].get('company_membership_sha256')
    if digest is None:
        return None
    if not isinstance(digest,str) or not re.fullmatch(r'[0-9a-f]{64}',digest):
        raise ValueError('Exact company-membership fingerprint required')
    path = root/'raw'/(verified['attempt_id']+'.json.company-membership.json')
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise ValueError('Bounded original company-membership readback required')
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest()!=digest:
        raise ValueError('Company-membership readback changed')
    value = json.loads(body.decode('utf-8-sig'))
    labels = verified['request']['company_labels']
    if not isinstance(value,dict):
        raise ValueError('Company-membership readback object required')
    positions = value.get('absent_positions')
    if (value.get('contract')!=COMPANY_MEMBERSHIP_CONTRACT
            or value.get('task_id')!=task['task_id']
            or value.get('query_attempt_id')!=verified['attempt_id']
            or value.get('prepared_request_sha256')!=verified['prepared_request_sha256']
            or type(value.get('requested_count')) is not int or value['requested_count']!=len(labels)
            or type(value.get('available_count')) is not int or not 1<=value['available_count']<=150000
            or not isinstance(positions,list) or not positions or len(positions)>len(labels)
            or any(type(i) is not int or not 0<=i<len(labels) for i in positions)
            or positions!=sorted(set(positions)) or len(set(labels))!=len(labels)
            or not 1<=len(labels)<=MAX_SELECTOR_LABELS
            or any(value.get(k) is not True for k in
                   ('complete_lookup_verified','source_binding_verified','source_list_count_stable'))
            or value.get('market_data_query_submitted') is not False
            or verified['outcome']['error_code']!='local_list_selection_failed_before_preview'):
        raise ValueError('Complete exact unsent company-membership evidence required')
    window=verified['source_clock_evidence']
    source_operation_clock(value['observed_at_utc'],
        datetime.fromisoformat(window['local_window_started_at_utc']),
        datetime.fromisoformat(window['local_window_finished_at_utc']))
    release, script_digest=DesktopBridge.bound_script_release(root,verified['attempt_id'],verified['prepared_path'])
    if COMPANY_MEMBERSHIP_CONTRACT.encode() not in release.read_bytes():
        raise ValueError('Original bridge does not implement company-membership evidence')
    return {'absent_positions':positions,'sha256':digest,'path':str(path.relative_to(root)),
            'bridge_script_sha256':script_digest,'available_count':value['available_count']}


def repartition_missing_companies(con, root, task: dict, verified: dict, membership: dict,
                                 *, due: str, failures: int, audit_path, scheduling_hint: dict | None = None) -> dict:
    """Preserve ALL work; defer only absent labels, unblock the present labels.

    Called only after the canonical fresh owned-interface acknowledgement.
    One atomic queue transaction and a preceding durable intent preserve the
    original failed attempt, lazy-plan cursor, denominator and retry history.
    No source value is adopted and no company name is silently substituted.
    """
    from datetime import UTC, datetime
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, compact_request, stable_id
    from downloader.tej_key_layout import key_count
    request=verified['request']; labels=request['company_labels']
    absent=set(membership['absent_positions'])
    groups=[([label for i,label in enumerate(labels) if i not in absent],False),
            ([label for i,label in enumerate(labels) if i in absent],True)]
    expected=len(labels)*(1 if key_count(request)==1 else len(request['date_labels']))
    if (expected!=task['expected_rows'] or expected!=(task['work_expected_rows'] or expected)
            or any(task.get(k) is not None for k in ('actual_rows','actual_bytes','receipt_path','output_path','completed_at_utc'))):
        raise ValueError('Company partition must preserve the exact unadopted work denominator')
    definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(task['table_id'],)).fetchone())
    children=[]
    contract=COMPANY_HINT_CONTRACT if scheduling_hint is not None else COMPANY_MEMBERSHIP_CONTRACT
    if groups[0][0]:
        for selected,missing in groups:
            part={**request,'company_labels':selected,'company_membership_partition':{
                'contract':contract,'parent_task_id':task['task_id'],
                'membership_sha256':membership['sha256'],
                **({'delayed_by_prior_absence_hint':missing} if scheduling_hint is not None else
                   {'currently_absent':missing})}}
            count=len(selected)*(1 if key_count(part)==1 else len(part['date_labels']))
            identifier=stable_id([task['table_id'],part])
            if con.execute('SELECT 1 FROM tasks WHERE task_id=?',(identifier,)).fetchone():
                raise ValueError('Company partition child already exists; reconcile the original intent')
            children.append((identifier,part,count,missing))
    missing_task_id=children[-1][0] if children else task['task_id']
    audit={'contract':contract,'state':'prepared',
        'observed_at_utc':datetime.now(UTC).isoformat(),'original_task':task,
        'membership':membership,'prepared_request_sha256':verified['prepared_request_sha256'],
        'prequery_outcome_sha256':verified['prequery_outcome_sha256'],
        'interface_readback_path':verified['interface_readback_path'],
        'interface_readback_sha256':verified['interface_readback_sha256'],
        'replacement_task_ids':[child[0] for child in children],
        'present_companies':len(groups[0][0]),'deferred_companies':len(groups[1][0]),
        'work_rows_before':expected,'work_rows_after':sum(child[2] for child in children) if children else expected,
        'provider_queries_sent':0,'source_rows_adopted':False,'original_attempt_preserved':True,
        'retry_budget_reset':False,'next_attempt_at_utc':due,'consecutive_failures':failures}
    if scheduling_hint is not None:
        audit['scheduling_hint']=scheduling_hint
        audit['fresh_source_membership_claimed']=False
        audit['source_failure_count_increased']=False
    if audit['work_rows_before']!=audit['work_rows_after']:
        raise ValueError('Company partition lost original scope')
    atomic_write_json(audit_path,audit,durable=True)
    if children:
        con.execute('UPDATE tasks SET state=? WHERE task_id=?',
            ('superseded_company_hint_v1' if scheduling_hint is not None else 'superseded_company_membership_v1',task['task_id']))
        for identifier,part,count,missing in children:
            con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,scope_contract,'
                'work_expected_rows,safe_prequery_retries,next_attempt_at_utc,last_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                (identifier,task['table_id'],'download',task['phase'],task['priority'],
                 json.dumps(compact_request(part,definition),ensure_ascii=False,separators=(',',':')),
                 count,SOURCE_SCOPE_CONTRACT,count,task['safe_prequery_retries'],due if missing else None,
                 COMPANY_MEMBERSHIP_DEFERRED if missing else None))
    else:
        con.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=?,last_error_code=? WHERE task_id=?",
                    (due,COMPANY_MEMBERSHIP_DEFERRED,task['task_id']))
    if scheduling_hint is None:
        con.execute('INSERT OR REPLACE INTO desktop_retry_windows VALUES(?,?,?,?,?,?,?)',
            (task['table_id'],missing_task_id,verified['attempt_id'],failures,due,
             COMPANY_MEMBERSHIP_DEFERRED,str(audit_path.relative_to(root))))
    con.execute("UPDATE tables SET state='backfilling',last_error_code=? WHERE table_id=?",
                (COMPANY_MEMBERSHIP_DEFERRED,task['table_id']))
    return {'state':'selector_scope_repartitioned' if children else 'prequery_failure_deferred',
        'task_id':task['task_id'],'table_id':task['table_id'],'membership':membership,
        'present_companies':len(groups[0][0]),'deferred_companies':len(groups[1][0]),
        'work_rows_before':expected,'work_rows_after':expected,'replacement_task_ids':[c[0] for c in children],
        'next_attempt_at_utc':due,'consecutive_failures':failures,'provider_queries_sent':0,
        'source_rows_adopted':False,'data_query_repeated':False,'audit':str(audit_path.relative_to(root))}


def defer_planned_company_hint(con, root, task: dict, *, now=None) -> bool:
    """Skip repeated FAILED preparation, never skip or certify source data.

    A recent exact negative lookup may delay the same display labels in a
    DIFFERENT, wholly unattempted date/field tile until the ORIGINAL due time.
    Due all-missing fragments may share a newer verified lookup's due time,
    rather than repeating it once per date. An older attempt must independently
    prove its own finished, complete all-absent, never-submitted outcome first.
    Every field/date/company remains queued. Healthy companies still undergo full
    fresh native selection and source validation when actually queried. The
    old proof is scheduling metadata, NOT a new no-query outcome or UI grant.
    """
    from datetime import UTC, datetime
    import uuid
    from downloader.tej_desktop_attempts import proved_unsent_prequery
    from downloader.tej_history import compact_request,expand_request
    deferred=task.get('last_error_code')==COMPANY_MEMBERSHIP_DEFERRED
    attempted=any(task.get(k) is not None for k in ('attempted_at_utc','active_attempt_id'))
    if (task['kind']!='download' or task['state']!='pending'
            or task.get('last_error_code') not in (None,COMPANY_MEMBERSHIP_DEFERRED)
            or attempted and (not deferred or any(task.get(k) is None for k in ('attempted_at_utc','active_attempt_id')))
            or any(task.get(k) is not None for k in ('actual_rows','actual_bytes',
                'receipt_path','output_path','completed_at_utc'))):
        return False
    previous=con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?',(task['table_id'],)).fetchone()
    if previous is None or previous['error_code']!=COMPANY_MEMBERSHIP_DEFERRED:
        return False
    try:
        now=now or datetime.now(UTC);due=datetime.fromisoformat(previous['next_attempt_at_utc'])
        if now.tzinfo is None or due.tzinfo is None or not 0<(due-now).total_seconds()<=900:
            return False
        if deferred:
            old_due=datetime.fromisoformat(task['next_attempt_at_utc'])
            if old_due.tzinfo is None or old_due>now:
                return False  # Never renew a live cooldown or extend it on each loop.
        own_negative=None
        if attempted:
            own=proved_unsent_prequery(root,con,task,finished=True)
            own_membership=company_membership_failure(root,task,own)
            if own_membership is None or len(own_membership['absent_positions'])!=len(own['request']['company_labels']):
                return False
            own_negative={'attempt_id':own['attempt_id'],
                'prepared_request_sha256':own['prepared_request_sha256'],
                'prequery_outcome_sha256':own['prequery_outcome_sha256'],
                'company_membership_sha256':own_membership['sha256']}
        path=root/previous['audit_path']
        if (path.is_symlink() or path.resolve().parent!=(root/'selector_replanning').resolve()
                or not path.is_file() or path.stat().st_size>2*1024**2):
            return False
        audit=json.loads(path.read_text());prior=audit['original_task']
        if audit.get('contract')!=COMPANY_MEMBERSHIP_CONTRACT or prior['table_id']!=task['table_id']:
            return False
        observed=datetime.fromisoformat(audit['observed_at_utc'])
        if observed.tzinfo is None or not 0<=(now-observed).total_seconds()<=900:
            return False
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(prior['task_id'],)).fetchone()
        if row is None or row['active_attempt_id']!=prior['active_attempt_id']:
            return False
        proof=proved_unsent_prequery(root,con,dict(row),finished=True)
        membership=company_membership_failure(root,dict(row),proof)
        if (membership!=audit['membership'] or previous['attempt_id']!=proof['attempt_id']
                or proof['prepared_request_sha256']!=audit['prepared_request_sha256']
                or proof['prequery_outcome_sha256']!=audit['prequery_outcome_sha256']):
            return False
        definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(task['table_id'],)).fetchone())
        request=expand_request(json.loads(task['request_json']),definition,task['table_id'])
        if any(request.get(k)!=proof['request'].get(k) for k in
                ('action','type','smart_id','table','catalog_fields','source_key_mode','key_layout_contract')):
            return False
        fields=request.get('fields');catalog=json.loads(definition['fields_json'])
        if (not isinstance(fields,list) or not fields or len(set(fields))!=len(fields)
                or any(field not in catalog for field in fields) or request.get('catalog_fields')!=catalog):
            return False
        compact_request(request,definition)  # Exact registered field partition, even across field batches.
        absent={proof['request']['company_labels'][i] for i in membership['absent_positions']}
        positions=[i for i,label in enumerate(request['company_labels']) if label in absent]
        if not positions or deferred and len(positions)!=len(request['company_labels']):
            return False
        hint={'contract':COMPANY_HINT_CONTRACT,'prior_task_id':prior['task_id'],
            'prior_attempt_id':proof['attempt_id'],'prior_partition_path':str(path.relative_to(root)),
            'prior_partition_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'prior_lookup_observed_at_utc':audit['observed_at_utc'],
            'valid_until_utc':due.isoformat(),'current_request_has_no_desktop_attempt':not attempted,
            'original_finished_all_absent_attempt':own_negative,
            'previously_deferred_fragment':deferred,
            'previous_fragment_due_at_utc':task.get('next_attempt_at_utc'),
            'prior_negative_proof_applies_only_to_prior_request':True}
        current={'request':request,'prepared_request_sha256':None,'prequery_outcome_sha256':None,
            'interface_readback_path':None,'interface_readback_sha256':None}
        projected={**membership,'absent_positions':positions}
        repartition_missing_companies(con,root,task,current,projected,due=due.isoformat(),
            failures=previous['consecutive_failures'],
            audit_path=root/'selector_replanning'/(task['task_id']+'-'+uuid.uuid4().hex+'-company-hint.json'),
            scheduling_hint=hint)
        return True
    except (OSError,ValueError,KeyError,TypeError,AttributeError):
        return False  # A damaged/stale hint cannot suppress normal fresh preparation.


def upgrade_query_tiling(root, config: dict, *, capacity_upgrade: bool = False) -> dict:
    """Explicit, locked upgrade preserving completed scopes and original plans.

    Caller owns the canonical dataset lock. No GUI or provider request occurs;
    no source receipt or former queue encoding is deleted. Unknown outcomes,
    blocked downloads, and active workers remain hard migration barriers for
    the legacy operation. An explicit capacity upgrade defers invalid tables,
    but still rejects every inflight/unknown source action globally.
    """
    from contextlib import closing
    from datetime import UTC, datetime
    import shutil
    import uuid

    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect, expand_request

    if config.get('query_tiling_contract') != TILING_CONTRACT:
        raise ValueError('Explicit reviewed company/date tiling configuration required')
    contract = LOCAL_CAPACITY_CONTRACT if capacity_upgrade else TILING_CONTRACT
    if capacity_upgrade and (config.get('query_capacity_contract') != LOCAL_CAPACITY_CONTRACT
            or type(config.get('max_cells_per_export')) is not int or not 100 <= config['max_cells_per_export'] <= 400000
            or type(config.get('max_companies_per_export')) is not int or not 1 <= config['max_companies_per_export'] <= 1024
            or type(config.get('max_rows_per_export')) is not int or not 1 <= config['max_rows_per_export'] <= 10000):
        raise ValueError('Explicit reviewed local Preview capacity required')
    with closing(connect(root)) as con, con:
        saved = dict(con.execute('SELECT key,value FROM meta'))
        deferred_ids = set(json.loads(saved.get('query_tiling_deferred_tables','[]')))
        original_config = json.loads(saved['config'])
        if (saved.get('query_tiling_contract') == TILING_CONTRACT and not deferred_ids
                and (not capacity_upgrade or saved.get('query_capacity_contract') == LOCAL_CAPACITY_CONTRACT
                     and all(config[k]==original_config[k] for k in
                             ('max_rows_per_export','max_cells_per_export','max_companies_per_export')))):
            return {'contract':contract,'already_installed':True,'provider_queries_sent':0}
        if saved.get('preview_planning_contract') != CONTRACT:
            raise ValueError('Upgrade the canonical Preview planner first')
        barrier_sql = ("SELECT 1 FROM tasks WHERE state='running' OR "
                       "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1"
                       if capacity_upgrade else
                       "SELECT 1 FROM tasks WHERE state='running' OR (kind='download' AND state='blocked') LIMIT 1")
        if con.execute(barrier_sql).fetchone():
            raise ValueError('Unresolved source action blocks tiling migration')
        if capacity_upgrade and con.execute("SELECT 1 FROM meta WHERE key IN ('desktop_interface_recovery_required','source_period_replan_required')").fetchone():
            raise ValueError('Unresolved shared source interface blocks capacity migration')
        if shutil.disk_usage(root).free < config.get('minimum_free_disk_bytes',5*1024**3)+2*(root/'queue.sqlite3').stat().st_size:
            raise OSError('Insufficient headroom for recoverable tiling upgrade')
        for field in ('max_rows_per_export','max_cells_per_export','max_companies_per_export'):
            if not capacity_upgrade and config[field] != original_config[field]:
                raise ValueError('Tiling cannot silently change source capacity bounds')
        new_config = {**original_config,'query_tiling_contract':TILING_CONTRACT}
        if capacity_upgrade:
            new_config.update({k:config[k] for k in ('max_rows_per_export','max_cells_per_export','max_companies_per_export',
                                                   'query_capacity_contract')})
            new_config['query_interval_contract'] = config.get('query_interval_contract','minimum_completion_gap_v1')
        migration = root/'planning_migrations'/uuid.uuid4().hex
        audit = {'contract':contract,'state':'prepared','provider_queries_sent':0,
                 'observed_at_utc':datetime.now(UTC).isoformat(),'tables':[], 'deferred_tables':[],
                 'source_files_deleted':False,'completed_scopes_preserved':True,
                 'former_bounds':{k:original_config[k] for k in ('max_rows_per_export','max_cells_per_export','max_companies_per_export')},
                 'new_bounds':{k:new_config[k] for k in ('max_rows_per_export','max_cells_per_export','max_companies_per_export')}}
        prepared = []
        for row in con.execute('SELECT * FROM download_plans ORDER BY table_id').fetchall():
            if not capacity_upgrade and saved.get('query_tiling_contract') == TILING_CONTRACT and row['table_id'] not in deferred_ids:
                continue
            previous = json.loads(row['plan_json'])
            core = {k:v for k,v in previous.items() if k!='fingerprint'}
            if hashlib.sha256(json.dumps(core,sort_keys=True,ensure_ascii=False).encode()).hexdigest() != previous['fingerprint']:
                raise ValueError('Original plan fingerprint differs')
            if capacity_upgrade and con.execute("SELECT 1 FROM tasks WHERE table_id=? AND kind='download' AND state='blocked' LIMIT 1",
                                                (row['table_id'],)).fetchone():
                audit['deferred_tables'].append({'table_id':row['table_id'],'reason':'blocked_table_scope_unchanged'})
                continue
            from downloader.tej_key_layout import key_count
            if key_count(previous['request']) == 1:
                if capacity_upgrade:
                    audit['deferred_tables'].append({'table_id':row['table_id'],'reason':'snapshot_axis_scope_unchanged'})
                continue  # Snapshot batching is already maximal; no dated migration.
            definition = dict(con.execute('SELECT * FROM tables WHERE table_id=?',(row['table_id'],)).fetchone())
            finished = [dict(r) for r in con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND state='complete' AND scope_contract=?",(row['table_id'],SOURCE_SCOPE_CONTRACT))]
            completed = [expand_request(json.loads(t['request_json']),definition,row['table_id']) for t in finished]
            base = {**previous['request'],'max_rows':new_config['max_rows_per_export'],
                    'max_cells':new_config['max_cells_per_export']} if capacity_upgrade else previous['request']
            try:
                plan = build_plan(base,previous['companies'],previous['dates'],new_config,completed)
            except ValueError:
                if not capacity_upgrade:
                    raise
                audit['deferred_tables'].append({'table_id':row['table_id'],'reason':'completed_key_scope_requires_reconciliation'})
                continue
            resolved = sum(t['work_expected_rows'] if t['work_expected_rows'] is not None else t['expected_rows'] for t in finished)
            if (plan['total_work_rows'] != previous['total_work_rows'] or plan['completed_work_rows'] != resolved):
                raise ValueError('Completed scope coverage changed or overlapped; reject migration')
            pending = [dict(r) for r in con.execute("SELECT task_id,request_json FROM tasks WHERE table_id=? AND kind='download' AND state='pending' AND scope_contract=?",(row['table_id'],SOURCE_SCOPE_CONTRACT))]
            native_scopes = []
            for task in pending:
                request = json.loads(task['request_json'])
                if 'field_partition' not in request:
                    expanded = expand_request(request,definition,row['table_id'])
                    # Older bounded native probes can coexist with the lazy
                    # plan. Validate their exact source axes, but never drop
                    # them or pretend that an unsent scope was completed.
                    if expanded.get('fields') != previous['request']['fields']:
                        raise ValueError('Pending native query differs from the former schema')
                    build_plan(previous['request'],previous['companies'],previous['dates'],new_config,[expanded])
                    native_scopes.append(task['task_id'])
                elif request['field_partition'].get('plan_fingerprint') != previous['fingerprint']:
                    raise ValueError('Pending query is outside the exact former plan')
            if native_scopes:
                audit['deferred_tables'].append({'table_id':row['table_id'],
                    'reason':'preserve_pending_native_scopes_until_completed',
                    'pending_native_tasks':native_scopes})
                continue
            remaining = row['total_queries'] - row['next_query'] + len(pending)
            if plan['total_queries'] >= remaining:
                continue  # Do not rewrite unchanged geometry, or increase calls after holes.
            backup = migration/'plans'/(row['table_id']+'.json')
            atomic_write_json(backup,dict(row),durable=True)
            item = {'table_id':row['table_id'],'before_remaining_queries':remaining,
                    'after_remaining_queries':plan['total_queries'],'preserved_work_rows':resolved,
                    'unchanged_total_work_rows':plan['total_work_rows'],
                    'former_fingerprint':previous['fingerprint'],'new_fingerprint':plan['fingerprint'],
                    'original_plan_backup':str(backup.relative_to(root)),
                    'original_plan_backup_sha256':hashlib.sha256(backup.read_bytes()).hexdigest(),
                    'pending_tasks_retained':len(pending)}
            audit['tables'].append(item)
            prepared.append((row['table_id'],plan,pending))
        # Durable intent and original plans precede the atomic DB commit.
        atomic_write_json(migration/'audit.json',audit,durable=True)
        for table_id, plan, pending in prepared:
            con.executemany("UPDATE tasks SET state='superseded_query_tiling_v1' WHERE task_id=? AND state='pending'",
                            [(task['task_id'],) for task in pending])
            con.execute('UPDATE download_plans SET plan_json=?,next_query=0,total_queries=? WHERE table_id=?',
                        (json.dumps(plan,ensure_ascii=False,separators=(',',':')),plan['total_queries'],table_id))
        con.execute("INSERT OR REPLACE INTO meta VALUES('query_tiling_contract',?)",(TILING_CONTRACT,))
        if capacity_upgrade:
            con.execute("INSERT OR REPLACE INTO meta VALUES('query_capacity_contract',?)",(LOCAL_CAPACITY_CONTRACT,))
        con.execute("INSERT OR REPLACE INTO meta VALUES('query_tiling_deferred_tables',?)",
                    (json.dumps([r['table_id'] for r in audit['deferred_tables']]),))
        con.execute("UPDATE meta SET value=? WHERE key='config'",(json.dumps(new_config),))
    audit['state']='committed'
    atomic_write_json(migration/'audit.json',audit,durable=True)
    return {'contract':contract,'already_installed':False,'tables_optimized':len(prepared),
            'queries_saved':sum(r['before_remaining_queries']-r['after_remaining_queries'] for r in audit['tables']),
            'provider_queries_sent':0,'completed_scopes_preserved':True,
            'deferred_tables':audit['deferred_tables'],
            'audit':str((migration/'audit.json').relative_to(root))}
