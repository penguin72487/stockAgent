"""Timing metadata and no-query Windows readback boundary checks."""
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from downloader.tej_history import SOURCE_SCOPE_CONTRACT
from scripts.benchmark_tej_preview_readback import completed_source, run
from scripts.profile_tej_acquisition import PROFILE_CONTRACT, profile

ROOT = Path(__file__).resolve().parents[1]
BRIDGE = (ROOT/'scripts/tej_smart_wizard_bridge.ps1').read_text()
BENCHMARK = (ROOT/'scripts/benchmark_tej_preview_readback.ps1').read_text()


@pytest.fixture
def queue(tmp_path):
    root = tmp_path/'data_tej'
    root.mkdir()
    with sqlite3.connect(root/'queue.sqlite3') as con:
        con.executescript('''
            CREATE TABLE tables(table_id TEXT PRIMARY KEY,name TEXT);
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY,table_id TEXT,kind TEXT,state TEXT,
                scope_contract TEXT,active_attempt_id TEXT,actual_rows INTEGER,seconds REAL,
                attempted_at_utc TEXT,completed_at_utc TEXT,timing_basis TEXT,
                output_path TEXT,receipt_path TEXT,last_error_code TEXT);
            CREATE INDEX tasks_scope_timing ON tasks(scope_contract,state,kind,timing_basis,completed_at_utc DESC);
            INSERT INTO tables VALUES('table','Fixture');
        ''')
        con.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    ('a'*24,'table','download','complete',SOURCE_SCOPE_CONTRACT,
                     'a'*24+'-'+'b'*32,2,30,'2026-10-03T16:00:00+00:00',
                     '2026-10-03T16:00:30+00:00','fresh_end_to_end',
                     'raw/'+'a'*24+'-'+'b'*32+'.json','receipts/'+'a'*24+'.json',None))
    for name in ('requests','raw','receipts'):
        (root/name).mkdir()
    attempt = 'a'*24+'-'+'b'*32
    (root/'requests'/(attempt+'.json')).write_text(json.dumps(
        {'fields':['PRIVATE_FIELD'],'company_labels':['PRIVATE_COMPANY'],'date_labels':['20261004']}))
    (root/'raw'/(attempt+'.json.stage.json')).write_text(json.dumps(
        {'task_id':'a'*24,'stage':'prepreview_verified','observed_at_utc':'2026-10-03T16:00:18+00:00'}))
    raw = root/'raw'/(attempt+'.json')
    raw.write_text('{"cells":[["PRIVATE_VALUE"]]}')
    (root/'receipts'/('a'*24+'.json')).write_text(json.dumps(
        {'task_id':'a'*24,'raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest()}))
    return root


def test_profile_indexed_complete_end_to_end_metadata_only(queue,tmp_path):
    report = profile(queue,tmp_path/'profile.json')
    assert report['contract'] == PROFILE_CONTRACT
    assert report['all']['tasks'] == 1
    assert report['nonempty']['median_end_to_end_seconds'] == 30
    assert report['median_before_submission_seconds'] == 18
    assert report['samples'][0]['remaining_including_query_readback_validation_seconds'] == 12
    text = json.dumps(report)
    assert not any(value in text for value in ('PRIVATE_VALUE','PRIVATE_COMPANY','PRIVATE_FIELD'))
    assert report['queue_modified'] is False and report['provider_queries_sent'] == 0
    assert any('tasks_scope_timing' in r['detail'] for r in report['selection_plan'])
    with pytest.raises(FileExistsError):
        profile(queue,tmp_path/'profile.json')


def test_scope_timings_only_expose_known_finite_numeric_phases(queue,tmp_path):
    stage = next((queue/'raw').glob('*.stage.json'))
    evidence = json.loads(stage.read_text())
    evidence.update(
        scope_preparation_timings_contract='monotonic_complete_scope_stages_v1',
        scope_preparation_seconds={
            'context_and_source_binding':2.5, 'company_selection_and_verification':0,
            'field_selection':'PRIVATE_VALUE', 'date_selection_and_verification':True,
            'company_universe_and_lookup':-1, 'date_model_and_lookup':901,
            'final_source_and_preview_guards':float('inf'), 'PRIVATE_FIELD':1,
        })
    stage.write_text(json.dumps(evidence))
    report = profile(queue,tmp_path/'timings.json')
    assert report['samples'][0]['scope_preparation_seconds'] == {
        'context_and_source_binding':2.5, 'company_selection_and_verification':0,
    }
    assert 'PRIVATE_' not in json.dumps(report)


def test_optional_full_windows_timing_is_bounded_exact_attempt_metadata_only(queue,tmp_path):
    attempt = 'a'*24+'-'+'b'*32
    flow = queue/'raw'/(attempt+'.json.flow.json')
    flow.write_text(json.dumps({
        'contract':'tej_monotonic_windows_full_flow_v1','task_id':'a'*24,
        'query_attempt_id':attempt,'action':'download',
        'phases':{'initialization_compile_seconds':0.5,'full_readback_seconds':7.2,
                  'query_and_response_seconds':True,'PRIVATE_FIELD':1,
                  'serialize_and_save_seconds':float('nan')},
        'windows_bridge_seconds':14,'windows_cpu_seconds':3,'PRIVATE_VALUE':'secret',
    }))
    report=profile(queue,tmp_path/'flow.json')
    assert report['samples'][0]['windows_flow']=={
        'phases':{'initialization_compile_seconds':0.5,'full_readback_seconds':7.2},
        'windows_bridge_seconds':14,'windows_cpu_seconds':3,
    }
    assert 'PRIVATE_' not in json.dumps(report)


@pytest.mark.parametrize('error',['attempt','task','contract','action','invalid_json'])
def test_optional_timing_failure_or_other_attempt_never_affects_data_profile(queue,tmp_path,error):
    attempt='a'*24+'-'+'b'*32
    value={'contract':'tej_monotonic_windows_full_flow_v1','task_id':'a'*24,
           'query_attempt_id':attempt,'action':'download','phases':{},'windows_bridge_seconds':14}
    if error=='invalid_json':text='{'
    else:
        key={'attempt':'query_attempt_id','task':'task_id','contract':'contract','action':'action'}[error]
        value[key]='different';text=json.dumps(value)
    (queue/'raw'/(attempt+'.json.flow.json')).write_text(text)
    report=profile(queue,tmp_path/'flow-invalid.json')
    assert report['all']['tasks']==1
    assert 'windows_flow' not in report['samples'][0]


@pytest.mark.parametrize('limit',[0,257,True])
def test_profile_rejects_unbounded_samples(queue,tmp_path,limit):
    with pytest.raises(ValueError):
        profile(queue,tmp_path/'bad.json',limit=limit)


def test_exact_completed_source_requires_saved_result_digest(queue):
    prepared,raw = completed_source(queue,'a'*24)
    assert prepared.is_file() and raw.is_file()
    raw.write_text('CHANGED')
    with pytest.raises(ValueError,match='digest'):
        completed_source(queue,'a'*24)


def test_preview_readback_benchmark_still_requires_nonempty_source(queue):
    with sqlite3.connect(queue/'queue.sqlite3') as con:
        con.execute('UPDATE tasks SET actual_rows=0 WHERE task_id=?',('a'*24,))
    with pytest.raises(ValueError,match='nonempty'):
        completed_source(queue,'a'*24)


@pytest.mark.parametrize('state,error',[('running',None),('blocked','unknown_outcome_no_auto_retry')])
def test_benchmark_preserves_unknown_and_active_owner(queue,state,error):
    with sqlite3.connect(queue/'queue.sqlite3') as con:
        con.execute('INSERT INTO tasks(task_id,kind,state,scope_contract,last_error_code) VALUES(?,?,?,?,?)',
                    ('c'*24,'download',state,SOURCE_SCOPE_CONTRACT,error))
    with pytest.raises(ValueError,match='Unresolved'):
        completed_source(queue,'a'*24)


@pytest.mark.parametrize('fixture,task_id',[(True,'a'*24),(False,None)])
def test_benchmark_requires_exactly_one_mode(queue,tmp_path,fixture,task_id):
    baseline = tmp_path/'baseline.ps1'
    baseline.write_text('fixture baseline')
    with pytest.raises(ValueError,match='Select either'):
        run(queue,baseline,tmp_path/'result.json',fixture=fixture,task_id=task_id)


def test_candidate_retains_full_validation_and_independent_sample():
    candidate = BRIDGE.split('public static object[][] CaptureFullPreview(',1)[1].split(
        'private static object[][] FullPreviewRows(',1)[0]
    assert 'SourceRowsBatched(grid,maxRows)' in candidate
    assert 'sample=PreviewRows(sourceRows,maxColumns)' in candidate
    assert 'FullPreviewRows(' in candidate and 'finally {ReleaseRows(sourceRows);}' in candidate
    core = BRIDGE.split('private static object[][] FullPreviewRows(',1)[1].split("'@ -ReferencedAssemblies",1)[0]
    assert 'if(batchedVerification)VerifyPreviewRowStability(grid,maxRows,count)' in core
    assert 'Source grid changed during readback' in core
    assert 'for(int c=skip+1;c<=columns;c++)' in core
    assert 'Interlocked.Add(ref totalCharacters,value.Length)>64000000' in core
    assert 'catch(COMException error)' in core
    assert '0x80020003' in core and '0x80070057' in core


def test_stability_rechecks_every_current_role_and_exact_counts_without_skipping_validation():
    stable=BRIDGE.split('private static void VerifyPreviewRowStability(',1)[1].split(
        'private static object[] PreviewRows(',1)[0]
    for proof in ('count=a.accChildCount','count>maxRows+4','for(int i=1;i<=count;i++)',
                  'a.get_accRole(i)','if(!(value is int))','role==28','role!=3',
                  'a.accChildCount!=count||rows!=expectedRows','Source grid changed during readback',
                  'finally {ReleaseAccessible(a);}'):
        assert proof in stable
    # Unsupported child IDs alone may use the original FULL role/row read.
    # RPC/disconnection/errors, unknown roles or changed counts must still stop.
    assert 'if(unsupported)' in stable and 'var verifiedRows=SourceRowsBatched(grid,maxRows)' in stable
    assert '0x80020003' in stable and '0x80070057' in stable and '))throw;' in stable
    assert 'verifiedRows.Length!=expectedRows' in stable and 'ReleaseRows(verifiedRows)' in stable
    assert 'owned_mta4_shared_row_complete_stability_v7' in BRIDGE


def test_complete_readback_worker_owns_its_mta_interfaces_and_bounded_join():
    wrapper=BRIDGE.split('public static object[][] CaptureFullPreview(',1)[1].split(
        'private static object[][] CaptureFullPreviewCore(',1)[0]
    assert 'new System.Threading.Thread(' in wrapper
    assert 'worker.SetApartmentState(System.Threading.ApartmentState.MTA)' in wrapper
    assert 'worker.IsBackground=true' in wrapper and 'worker.Join(300000)' in wrapper
    assert 'preserve source attempt, no query repeat' in wrapper
    assert 'if(error!=null)throw' in wrapper
    # Only primitive inputs and managed result arrays cross the apartment.
    assert 'SourceRowsBatched(' not in wrapper and 'Accessibility.IAccessible' not in wrapper
    core=BRIDGE.split('private static object[][] CaptureFullPreviewCore(',1)[1].split(
        'private static object[][] FullPreviewRows(',1)[0]
    assert 'SourceRowsBatched(grid,maxRows)' in core
    assert 'sample=PreviewRows(sourceRows,maxColumns)' in core
    assert 'finally {ReleaseRows(sourceRows);}' in core


def test_bounded_parallel_readback_has_one_value_contract_and_complete_ordered_commit():
    serial=BRIDGE.split('private static object[][] FullPreviewRows(',1)[1].split(
        'private static object[][] ParallelPreviewRows(',1)[0]
    parallel=BRIDGE.split('private static object[][] ParallelPreviewRows(',1)[1].split(
        'private static object[] ReadPreviewRow(',1)[0]
    reader=BRIDGE.split('private static object[] ReadPreviewRow(',1)[1].split("'@ -ReferencedAssemblies",1)[0]
    assert 'batchedVerification&&sourceRows.Length>=512' in serial
    assert 'GetApartmentState()==System.Threading.ApartmentState.MTA' in serial
    assert 'ReadPreviewRow(sourceRows[i-1],i,maxColumns' in serial
    assert 'ReadPreviewRow(sourceRows[i],i+1,maxColumns' in parallel
    assert 'MaxDegreeOfParallelism=4' in parallel
    assert 'Unreviewed readback apartment; no partial adoption' in parallel
    for proof in ('if(i!=count-1||!blank)result[i]=cells',
                  'Interlocked.Increment(ref completed)',
                  'if(completed!=count-1)throw',
                  'VerifyPreviewRowStability(grid,maxRows,count)',
                  'for(int i=0;i<count;i++)',
                  'Missing complete parallel row; no partial adoption',
                  'progressClock.ElapsedMilliseconds>=2000', 'lock(progressGuard)'):
        assert proof in parallel
    assert 'skip<0||skip>1' in reader and 'rowNumber==1' in reader
    assert 'for(int c=skip+1;c<=columns;c++)' in reader
    assert 'Interlocked.Add(ref totalCharacters,value.Length)>64000000' in reader
    assert 'Interlocked.Increment(ref fallback)' in reader
    assert 'cells[c-skip-1]=value' in reader
    assert reader.count('finally {ReleaseAccessible(cell);}')==2
    assert 'catch(COMException error)' in reader
    assert '0x80020003' in reader and '0x80070057' in reader and '))throw;' in reader
    for forbidden in ('Parse(', 'Convert.ToDouble', 'Round(', 'BeginPreviewDefaultAction',
                      'accDoDefaultAction', 'SelectListBatch', 'SendKeys', 'SetForegroundWindow'):
        assert forbidden not in parallel+reader


def test_company_reuse_requires_fresh_binding_selected_list_and_full_all_source_names():
    guard=BRIDGE.split('$companySelectionAlreadyMatches=',1)[1].split(
        'if(-not $companySelectionAlreadyMatches)',1)[0]
    assert '$bindingAlreadyMatches' in guard
    assert 'Items $companyLists[3].Current.NativeWindowHandle' in guard
    assert '$requestDoc.company_labels.Count -ne $availableCompanyCount' in guard
    assert 'Items $companyLists[2].Current.NativeWindowHandle' in guard
    assert guard.count('[TejBridgeNative]::SameItems(')==2
    assert guard.count('[string[]]$requestDoc.company_labels')==2
    block=BRIDGE.split('if(-not $companySelectionAlreadyMatches)',1)[1].split(
        "Stamp-ScopeTiming 'company_selection_and_verification'",1)[0]
    assert "Click-Button $company 'Clear All'" in block
    assert "Click-Button $company 'Select All'" in block
    assert '$selectedCompanies=Items $companyLists[3].Current.NativeWindowHandle' in block
    assert 'SameItems([string[]]$selectedCompanies,[string[]]$requestDoc.company_labels)' in block
    assert '$stage.fresh_company_selection_reused=$companySelectionAlreadyMatches' in BRIDGE


def test_header_shape_sample_and_value_fallback_release_their_exact_com_acquisitions():
    for name,following in [('PreviewHeader','PreviewShape'),('PreviewShape','PreviewRowLowerBound')]:
        part=BRIDGE.split('public static '+('string[] ' if name=='PreviewHeader' else 'int[] ')+name+'(',1)[1].split(
            'public static '+('int[] ' if following=='PreviewShape' else 'int ')+following+'(',1)[0]
        assert 'finally {ReleaseAccessible(a);}' in part
        assert 'ReleaseAccessible(row)' in part or 'ReleaseAccessible(header)' in part
    sample=BRIDGE.split('private static object[] PreviewRows(',1)[1].split('private static string lastProgressPath',1)[0]
    assert 'finally {ReleaseAccessible(cell);}' in sample
    core=BRIDGE.split('private static object[][] FullPreviewRows(',1)[1].split("'@ -ReferencedAssemblies",1)[0]
    assert core.count('finally {ReleaseAccessible(cell);}')==2


def test_batch_enumeration_is_bounded_exact_and_releases_interfaces():
    source = BRIDGE.split('private static Accessibility.IAccessible[] SourceRowsBatched(',1)[1].split(
        'public static object[] Preview(',1)[0]
    assert 'count>maxRows+4' in source and 'obtained!=count' in source
    assert 'AccessibleChildren(a,0,count,children,out obtained)' in source
    assert 'Marshal.ThrowExceptionForHR(hr)' in source
    assert 'child.get_accRole(0)' in source and 'role==28' in source and 'role!=3' in source
    assert 'a.accChildCount!=count' in source
    assert 'ReleaseAccessible(a)' in source and 'ReleaseRows(rows.ToArray())' in source


def test_source_benchmark_never_submits_or_changes_desktop_state():
    for action in ('BeginPreviewDefaultAction','accDoDefaultAction','SelectTab(',
                   'SetForegroundWindow','SendKeys.Send','Clip'+'board','Set-Checkbox','Select-Combo'):
        assert action not in BENCHMARK
    for proof in ('StockAgentTEJSmartWizardOwner','Exact idle query scope unavailable',
                  'Exact full-grid or object-path sample parity failed',
                  'Current Preview differs from the exact saved source result',
                  'source_signature_unchanged=$true','provider_queries_sent=0'):
        assert proof in BENCHMARK


def test_complete_capture_fault_checks_can_only_mutate_the_owned_fixture():
    guard="if($CheckStability -and -not $Fixture){throw 'Mutation checks may only modify the owned fixture'}"
    assert guard in BENCHMARK and BENCHMARK.index(guard) < BENCHMARK.index('Add-Type')
    block=BENCHMARK.split('if($CheckStability) {',1)[1].split('$receipt=@',1)[0]
    assert '[TejReadbackBenchmark]::ChangeFixtureColumns($true)' in block
    assert 'finally {[TejReadbackBenchmark]::ChangeFixtureColumns($false)}' in block
    assert 'changed_schema_full_capture_rejected=$true' in block
    assert 'destroyed_window_full_capture_rejected=$true' in block
    assert block.count('[TejBridgeNative]::CaptureFullPreview(') == 2
    assert block.index('[TejReadbackBenchmark]::Close()') < block.index('destroyed_window_full_capture_rejected=$true')


@pytest.mark.parametrize('rows,columns',[(0,30),(3001,30),(True,30),(12,2),(12,31),(12,False)])
def test_fixture_dimensions_rejected_before_windows_admission(queue,tmp_path,rows,columns):
    baseline=tmp_path/'baseline.ps1';baseline.write_text('owned baseline')
    with pytest.raises(ValueError,match='fixture dimensions'):
        run(queue,baseline,tmp_path/'never.json',fixture=True,fixture_rows=rows,fixture_columns=columns)
    assert not (tmp_path/'never.json').exists()


def test_mutations_can_never_target_a_real_source(queue,tmp_path):
    baseline=tmp_path/'baseline.ps1';baseline.write_text('owned baseline')
    with pytest.raises(ValueError,match='Mutation checks'):
        run(queue,baseline,tmp_path/'never.json',task_id='a'*24,check_stability=True)
    assert BENCHMARK.index("if($CheckStability -and -not $Fixture)")<BENCHMARK.index('Add-Type')


def test_fair_baseline_uses_same_capture_when_available():
    assert "[TejBridgeBaseline].GetMethod('CaptureFullPreview')" in BENCHMARK
    assert '[TejBridgeBaseline]::CaptureFullPreview(' in BENCHMARK
    assert '[TejBridgeBaseline]::FullPreview(' in BENCHMARK  # Preserved legacy experiment path.


def test_benchmark_uses_canonical_durable_interactive_admission(queue,tmp_path,monkeypatch):
    from types import SimpleNamespace
    import scripts.benchmark_tej_preview_readback as module
    baseline=tmp_path/'baseline.ps1';baseline.write_text('owned baseline')
    candidate=tmp_path/'candidate.ps1';candidate.write_text('owned candidate')
    output=tmp_path/'benchmark'/'accepted.json'
    monkeypatch.setattr(module,'interactive_transport',lambda root: {
        'windows_session_id':3,'interop_socket':'/run/WSL/123_interop'})
    seen=[]
    def launch(command,**kwargs):
        seen.append(kwargs)
        assert kwargs['windows_session_id']==3 and kwargs['interop_socket']=='/run/WSL/123_interop'
        request=json.loads(kwargs['request'].read_text())
        assert request['candidate_sha256']==hashlib.sha256(candidate.read_bytes()).hexdigest()
        assert request['baseline_sha256']==hashlib.sha256(baseline.read_bytes()).hexdigest()
        assert request['action']=='read_only_readback_benchmark' and request['fixture'] is True
        assert 'candidate.ps1' in command and '-FixtureRows 64' in command
        output.write_text(json.dumps({'accepted':True,'provider_queries_sent':0,
            'source_values_exposed':False,'queue_modified':False}))
        return SimpleNamespace(returncode=0,stderr=b'')
    monkeypatch.setattr(module,'run_guarded_windows',launch)
    result=run(queue,baseline,output,fixture=True,candidate=candidate,fixture_rows=64)
    assert result['accepted'] and len(seen)==1
