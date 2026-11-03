"""Closed-pack compute scopes are independent; none is a recovery authority."""
import json
from pathlib import Path
from types import SimpleNamespace

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
    monkeypatch.setattr(spool, 'NativeUpload', lambda *_: SimpleNamespace(close=lambda:None,
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
