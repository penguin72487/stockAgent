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
    assert 'SourceRowsBatched(grid,maxRows):SourceRows(grid,maxRows)' in core
    assert 'Source grid changed during readback' in core
    assert 'for(int c=skip+1;c<=columns;c++)' in core
    assert 'totalCharacters>64000000' in core
    assert 'catch(COMException error)' in core
    assert '0x80020003' in core and '0x80070057' in core


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
