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


def rectangle_queries(companies: int, dates: int, rows: int, size: int, keys: int) -> int:
    groups, tail = divmod(companies, size)
    return groups * (1 if keys == 1 else math.ceil(dates / (rows // size))) + (
        (1 if keys == 1 else math.ceil(dates / (rows // tail))) if tail else 0)


def company_batch_size(companies: int, dates: int, rows: int, cap: int, keys: int,
                       *, optimize: bool = False) -> int:
    """Minimum call count among bounded uniform rectangular tilings.

    This does not claim the unrestricted global packing optimum. Enumerating
    at most the configured company cap avoids a Cartesian work queue or a
    large dynamic program, and never increases the existing row/cell bounds.
    """
    maximum = min(cap, rows)
    if type(maximum) is not int or maximum < 1 or maximum > 1024:
        raise ValueError('Invalid bounded company tiling cap')
    if not optimize or keys == 1 or companies == 0:
        return maximum
    return min(range(1, min(maximum, companies) + 1),
               key=lambda size: (rectangle_queries(companies, dates, rows, size, keys), -size))


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
                                        optimize=tiling == TILING_CONTRACT)
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
                date_size=max(1,rows//len(indices)); chunks=math.ceil(missing_count/date_size)
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
    plans=con.execute("SELECT p.table_id,p.next_query,p.phase,p.priority FROM download_plans p WHERE p.next_query<p.total_queries AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.table_id=p.table_id AND t.kind='download' AND t.scope_contract=? AND t.state IN ('pending','running','blocked')) ORDER BY p.priority,p.table_id",(SOURCE_SCOPE_CONTRACT,)).fetchall()
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
