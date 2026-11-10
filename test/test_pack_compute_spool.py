"""Closed-pack compute scopes are independent; none is a recovery authority."""
import json
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import tempfile
import threading
import time

import pytest

from scripts import manage_pack_compute_spool as spool


def configuration():
    return {'contract': spool.CONTRACT, 'enabled': True, 'maximum_jobs_per_cycle': 2,
        'cold_root': '/srv/stockagent-packed/.local-state/private-history',
        'dataset': 'local-tw-index-futures', 'installed_since_utc': '2026-11-01T02:56:00+00:00',
        'additional_sources': [{'cold_root': '/srv/stockagent-packed', 'dataset': 'tw-index-futures',
                               'installed_since_utc': '2026-11-01T12:28:00+00:00'}]}


def test_original_and_shared_roots_keep_distinct_stable_contexts():
    cfg = configuration()
    private, shared = spool.enrolled_sources(cfg)
    assert spool.scope_context(private) != spool.scope_context(shared)
    assert spool.scope_context(private) == spool.scope_context(spool.enrolled_sources({
        **cfg, 'additional_sources': []})[0])


@pytest.mark.parametrize('change', ['wrong_root', 'wrong_dataset', 'duplicate', 'naive', 'jobs_bool'])
def test_scope_enrollment_rejects_implicit_or_mismatched_source_authority(change):
    cfg = configuration()
    if change == 'wrong_root': cfg['additional_sources'][0]['cold_root'] = '/root'
    elif change == 'wrong_dataset': cfg['additional_sources'][0]['dataset'] = cfg['dataset']
    elif change == 'duplicate': cfg['additional_sources'][0] = {key: cfg[key] for key in
        ('cold_root', 'dataset', 'installed_since_utc')}
    elif change == 'naive': cfg['installed_since_utc'] = '2026-11-01T02:56:00'
    else: cfg['maximum_jobs_per_cycle'] = True
    with pytest.raises(ValueError): spool.enrolled_sources(cfg)


def test_one_scope_failure_is_recorded_without_starving_its_neighbor(tmp_path, monkeypatch):
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    visited = []
    def run(scope, cfg, *, progress):
        visited.append(scope['dataset'])
        if scope['dataset'].startswith('local-'): raise OSError('preserve original error')
        progress({'context_sha256': spool.scope_context(scope), 'dataset': scope['dataset'],
                  'state': 'waiting_for_new_sealed_packs', 'completed_items': 2, 'total_items': 2})
    monkeypatch.setattr(spool, 'run_scope', run)
    cfg = configuration()
    value = spool.run_sources(cfg, spool.enrolled_sources(cfg))
    assert visited == ['local-tw-index-futures', 'tw-index-futures']
    assert value['scopes'][0]['error_type'] == 'OSError'
    assert value['state'] == 'blocked' and value['completed_items'] == 2
    assert value['source_retirement'] is False and value['nas_acceptance'] is False


@pytest.mark.parametrize('previous_kind', ['valid', 'wrong_source', 'wrong_worker', 'wrong_boot'])
def test_legacy_checkpoint_reuses_only_exact_generations_and_remains_unchanged(
        tmp_path, monkeypatch, previous_kind):
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    source = tmp_path/'closed.zip'
    source.write_bytes(b'closed test payload')
    cfg = configuration()
    scope = spool.enrolled_sources(cfg)[0]
    context = spool.scope_context(scope)
    record = {'object_bytes': source.stat().st_size, 'source_generation': list(spool.signature(source.stat())),
              'worker_sha256': spool.VERIFIER_SHA, 'cache_worker_sha256': spool.CACHE_SHA}
    if previous_kind == 'wrong_source': record['source_generation'][-1] += 1
    if previous_kind == 'wrong_worker': record['worker_sha256'] = 'superseded'
    previous = {'context_sha256': context, 'remote_boot_id': 'other' if previous_kind == 'wrong_boot' else 'boot',
                'objects': {source.stem: record}}
    raw = json.dumps(previous).encode()
    (tmp_path/'state.json').write_bytes(raw)
    monkeypatch.setattr(spool, 'configuration', lambda *_: {'remote_spool': True, 'remote_workers': 2})
    monkeypatch.setattr(spool, 'candidates', lambda *_: [(source, source.stat())])
    monkeypatch.setattr(spool, 'NativeUpload', lambda *_: SimpleNamespace(close=lambda:None, abort=lambda:None,
        started=None, finished=None))
    visits = []
    class Remote:
        boot_id = 'boot'
        def __init__(self, *_): pass
        def stage(self, payload, item, **kwargs):
            visits.append(item)
            return {'object_bytes': item['bytes'], 'worker_sha256': spool.VERIFIER_SHA,
                    'cache_worker_sha256': spool.CACHE_SHA}
        def close(self): return {}
    monkeypatch.setattr(spool, 'RemotePacks', Remote)
    reports = []
    spool.run_scope(scope, cfg, progress=lambda row:reports.append(dict(row)))
    assert len(visits) == (0 if previous_kind == 'valid' else 1)
    assert reports[0]['completed_items'] == (1 if previous_kind == 'valid' else 0)
    assert reports[-1]['completed_items'] == 1
    assert reports[-1]['state'] == 'waiting_for_new_sealed_packs'
    assert (tmp_path/'state.json').read_bytes() == raw
    assert (tmp_path/f'scope-{context}.json').is_file()
    assert all('objects' not in row for row in reports)


def test_discovery_error_closes_remote_without_releasing_another_owner_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    monkeypatch.setattr(spool, 'configuration', lambda *_: {'remote_spool': True})
    monkeypatch.setattr(spool, 'candidates', lambda *_: (_ for _ in ()).throw(OSError('offline')))
    closed = []
    monkeypatch.setattr(spool, 'RemotePacks', lambda *_: SimpleNamespace(boot_id='boot',close=lambda:closed.append(True)))
    cfg = configuration()
    with pytest.raises(OSError, match='offline'):
        spool.run_scope(spool.enrolled_sources(cfg)[0], cfg, progress=lambda _:None)
    assert closed == [True]


def test_capacity_recovery_marks_only_reported_cache_and_resumes_queue(tmp_path, monkeypatch):
    from stockagent.data_sync import pack_compute_pressure_control as pressure
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    cfg = {**configuration(), 'pressure_recovery_enabled':True}
    scope = spool.enrolled_sources(cfg)[0]
    paths = [tmp_path/(f'{number:064x}.zip') for number in (1, 2)]
    for path in paths: path.write_bytes(b'closed')
    first, second = paths
    context = spool.scope_context(scope)
    record = {'object_bytes':first.stat().st_size, 'source_generation':list(spool.signature(first.stat())),
              'worker_sha256':spool.VERIFIER_SHA, 'cache_worker_sha256':spool.CACHE_SHA}
    (tmp_path/f'scope-{context}.json').write_text(json.dumps({'context_sha256':context,
        'remote_boot_id':'boot', 'objects':{first.stem:record}}))
    monkeypatch.setattr(spool, 'configuration', lambda *_: {'remote_spool':True,'remote_workers':1,
        'remote_spool_reserve_bytes':1024**3})
    monkeypatch.setattr(spool, 'candidates', lambda *_:[(path,path.stat()) for path in paths])
    values = iter([0, 64*1024**3])
    monkeypatch.setattr(spool, 'remote_spool_available', lambda *_: next(values))
    monkeypatch.setattr(spool, 'NativeUpload', lambda *_: SimpleNamespace(close=lambda:None, abort=lambda:None,
        started=None, finished=None))
    class Remote:
        boot_id='boot'
        def __init__(self,*_): pass
        def stage(self, payload, item, **_):
            return {'object_bytes':item['bytes'], 'worker_sha256':spool.VERIFIER_SHA,
                    'cache_worker_sha256':spool.CACHE_SHA}
        def close(self): return {}
    monkeypatch.setattr(spool, 'RemotePacks', Remote)
    def relieve(_scope, _current, _completed, _options, _remote, _state, *, available, progress):
        assert available == 0
        progress({'phase':'verify_fixed_d_originals', 'done_bytes':3, 'total_bytes':6})
        return {'state':'partial','removed':[{'object_sha256':first.stem,'released_allocated_bytes':4096}]}
    monkeypatch.setattr(pressure, 'relieve', relieve)
    scans = []
    monkeypatch.setattr(pressure, 'retry_vast_syncthing_scan', lambda _options: scans.append(True) or 'requested')
    reports = []
    spool.run_scope(scope, cfg, progress=lambda row:reports.append(dict(row)))
    stored=json.loads((tmp_path/f'scope-{context}.json').read_bytes())
    assert stored['objects'][first.stem]['payload_cache_retired'] is True
    assert 'payload_cache_retired' not in stored['objects'][second.stem]
    assert stored['pressure_reclaimed_bytes']==4096
    assert stored['pressure_reclaimed_total_bytes']==4096 and stored['pressure_reclaimed_total_items']==1
    assert stored['pressure_sync_state']=='requested' and scans == [True]
    assert stored['completed_items']==2 and stored['state']=='waiting_for_new_sealed_packs'
    assert any(row.get('pressure_done_bytes')==3 for row in reports)


def test_no_progress_pressure_retry_is_bounded_and_does_not_rehash(tmp_path, monkeypatch):
    from stockagent.data_sync import pack_compute_pressure as pressure_worker
    from stockagent.data_sync import pack_compute_pressure_control as pressure
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    cfg = {**configuration(), 'pressure_recovery_enabled':True}
    scope = spool.enrolled_sources(cfg)[0]
    paths = [tmp_path/(f'{number:064x}.zip') for number in (1, 2)]
    for path in paths: path.write_bytes(b'closed')
    first, second = paths
    context = spool.scope_context(scope)
    rec = {'object_bytes':first.stat().st_size, 'source_generation':list(spool.signature(first.stat())),
           'worker_sha256':spool.VERIFIER_SHA, 'cache_worker_sha256':spool.CACHE_SHA}
    retry_at = time.time() + 240
    previous = {'context_sha256':context, 'remote_boot_id':'boot', 'objects':{first.stem:rec},
        'pressure_state':'partial','pressure_retry_after_epoch':retry_at,
        'pressure_retry_worker_sha256':pressure_worker.SOURCE_SHA256,
        'retry_failures':{second.stem:{'source_generation':list(spool.signature(second.stat())),
            'retry_after_epoch':time.time()+120, 'attempts':1}}}
    (tmp_path/f'scope-{context}.json').write_text(json.dumps(previous))
    monkeypatch.setattr(spool, 'configuration', lambda *_:{'remote_spool':True, 'remote_workers':1,
        'remote_spool_reserve_bytes':1024**3})
    monkeypatch.setattr(spool, 'candidates', lambda *_:[(path,path.stat()) for path in paths])
    monkeypatch.setattr(spool, 'remote_spool_available', lambda *_:0)
    monkeypatch.setattr(spool, 'RemotePacks', lambda *_:SimpleNamespace(boot_id='boot',close=lambda:{}))
    monkeypatch.setattr(pressure, 'relieve', lambda *_a, **_kw:pytest.fail('must not rehash before retry'))
    reports=[]
    spool.run_scope(scope,cfg,progress=lambda row:reports.append(dict(row)))
    assert reports[-1]['pressure_state']=='partial'
    assert reports[-1]['pressure_retry_after_epoch']==retry_at
    assert reports[-1]['capacity_wait'] is True


def test_failed_syncthing_retry_continues_even_after_pack_queue_finishes(tmp_path, monkeypatch):
    from stockagent.data_sync import pack_compute_pressure_control as pressure
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    cfg = {**configuration(), 'pressure_recovery_enabled':True}
    scope = spool.enrolled_sources(cfg)[0]
    path = tmp_path/(f'{1:064x}.zip')
    path.write_bytes(b'closed')
    context = spool.scope_context(scope)
    record = {'object_bytes':path.stat().st_size, 'source_generation':list(spool.signature(path.stat())),
              'worker_sha256':spool.VERIFIER_SHA, 'cache_worker_sha256':spool.CACHE_SHA}
    previous = {'context_sha256':context, 'remote_boot_id':'boot', 'objects':{path.stem:record},
                'pressure_sync_state':'failed'}
    (tmp_path/f'scope-{context}.json').write_text(json.dumps(previous))
    monkeypatch.setattr(spool, 'configuration', lambda *_:{'remote_spool':True,'remote_workers':1})
    monkeypatch.setattr(spool, 'candidates', lambda *_:[(path,path.stat())])
    monkeypatch.setattr(spool, 'remote_spool_available', lambda *_:64*1024**3)
    monkeypatch.setattr(spool, 'RemotePacks', lambda *_:SimpleNamespace(boot_id='boot',close=lambda:{}))
    calls=[]
    monkeypatch.setattr(pressure, 'retry_vast_syncthing_scan', lambda _options:calls.append(True) or 'not_required')
    reports=[]
    spool.run_scope(scope,cfg,progress=lambda row:reports.append(dict(row)))
    assert calls == [True] and reports[-1]['pressure_sync_state']=='not_required'
    assert reports[-1]['state']=='waiting_for_new_sealed_packs'


def test_aborted_remote_session_stops_turn_and_failed_pack_retries_without_starving_others(tmp_path, monkeypatch):
    monkeypatch.setattr(spool, 'STATE', tmp_path)
    cfg = {**configuration(), 'maximum_jobs_per_cycle': 8}
    scope = spool.enrolled_sources(cfg)[0]
    paths = [tmp_path / (f'{i:064x}.zip') for i in range(8)]
    for path in paths:
        path.write_bytes(b'test pack')
    monkeypatch.setattr(spool, 'configuration', lambda *_: {'remote_spool': True, 'remote_workers': 1})
    monkeypatch.setattr(spool, 'candidates', lambda *_: [(path, path.stat()) for path in paths])
    monkeypatch.setattr(spool, 'NativeUpload', lambda *_: SimpleNamespace(close=lambda:None, abort=lambda:None,
        started=None, finished=None))
    clock = [1000.0]
    monkeypatch.setattr(spool.time, 'time', lambda: clock[0])
    visits = []
    fail = [True]
    class Remote:
        boot_id = 'boot'
        def __init__(self, *_): self.sessions = [SimpleNamespace(closed=False)]
        def stage(self, payload, item, **kwargs):
            visits.append(item['sha256'])
            if fail[0]:
                self.sessions[0].closed = True
                raise TimeoutError('simulated disconnected worker')
            return {'object_bytes': item['bytes'], 'worker_sha256': spool.VERIFIER_SHA,
                    'cache_worker_sha256': spool.CACHE_SHA}
        def close(self): return {}
    monkeypatch.setattr(spool, 'RemotePacks', Remote)
    reports = []
    spool.run_scope(scope, cfg, progress=lambda row: reports.append(dict(row)))
    assert len(visits) == 1
    assert reports[-1]['state'] == 'blocked' and reports[-1]['retry_deferred_items'] == 1
    assert all('retry_failures' not in row for row in reports)
    fail[0] = False
    spool.run_scope(scope, cfg, progress=lambda row: reports.append(dict(row)))
    assert visits.count(paths[0].stem) == 1
    assert reports[-1]['completed_items'] == 7
    clock[0] += 61
    spool.run_scope(scope, cfg, progress=lambda row: reports.append(dict(row)))
    assert visits.count(paths[0].stem) == 2
    assert reports[-1]['completed_items'] == 8
    assert reports[-1]['state'] == 'waiting_for_new_sealed_packs'


def test_cancelled_native_upload_stops_blocked_pipe_and_releases_reader_lock(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io as native
    monkeypatch.setattr(native, 'windows_path', lambda _: 'fixture')
    processes = []
    def process(*_args, **_kwargs):
        child = subprocess.Popen([sys.executable, '-c',
            "import os; block=b'x'*65536\nwhile True: os.write(1,block)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        processes.append(child)
        return child, tempfile.TemporaryFile()
    monkeypatch.setattr(native, '_process', process)
    lock = threading.Lock()
    source = spool.NativeUpload(tmp_path / 'fixture', 64 * 1024**2, lock)
    source.seek(0)
    assert source.read(4096)
    started = time.monotonic()
    source.abort()
    assert time.monotonic() - started < 10
    assert processes[0].poll() is not None
    assert lock.acquire(blocking=False)
    lock.release()


def test_native_upload_requires_exact_bytes_and_eof_before_success(tmp_path, monkeypatch):
    from contextlib import contextmanager
    import io
    from stockagent.data_sync import windows_cold_io as native
    outcomes = []
    @contextmanager
    def reader(*_args, **_kwargs):
        try:
            yield io.BytesIO(b'abc')
        except RuntimeError:
            outcomes.append('aborted')
            raise
        else:
            outcomes.append('verified')
    monkeypatch.setattr(native, 'binary_reader', reader)
    source = spool.NativeUpload(tmp_path / 'fixture', 3, threading.Lock())
    source.seek(0)
    assert source.read(3) == b'abc'
    with pytest.raises(ValueError, match='exact source and EOF'):
        source.close()
    assert outcomes == ['aborted']
    source = spool.NativeUpload(tmp_path / 'fixture', 3, threading.Lock())
    source.seek(0)
    assert source.read(3) == b'abc' and source.read(1) == b''
    source.close()
    assert outcomes == ['aborted', 'verified']


def test_remote_capacity_denial_never_opens_a_native_source_or_locks(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io as native
    monkeypatch.setattr(native, 'binary_reader', lambda *_a, **_k: pytest.fail('source must remain unopened'))
    lock = threading.Lock()
    source = spool.NativeUpload(tmp_path / 'fixture', 100, lock, 1000, 950)
    with pytest.raises(spool.RemoteCapacityWait):
        source.seek(0)
    assert lock.acquire(blocking=False)
    lock.release()


def test_ready_turns_continue_without_timer_gap_and_reload_enrollment(monkeypatch):
    loaded, turns = [], []
    def load():
        loaded.append(True)
        return configuration()
    def turn(*_):
        turns.append(True)
        return {'scopes': [{'state': 'queued' if len(turns) == 1 else 'waiting_for_new_sealed_packs',
                            'computed_this_turn': 2}]}
    monkeypatch.setattr(spool, 'run_sources', turn)
    result = spool.drain_sources(load)
    assert len(loaded) == len(turns) == 2
    assert result['scopes'][0]['state'] == 'waiting_for_new_sealed_packs'


@pytest.mark.parametrize('state', ['blocked', 'queued'])
def test_no_progress_yields_to_the_original_timer_without_a_busy_loop(monkeypatch, state):
    turns = []
    def turn(*_):
        turns.append(True)
        return {'scopes': [{'state': state, 'computed_this_turn': 0}]}
    monkeypatch.setattr(spool, 'run_sources', turn)
    spool.drain_sources(configuration)
    assert len(turns) == 1
