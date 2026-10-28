"""Original-owner cutovers retain payloads and reconcile partial metadata writes."""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import publish_data_releases as publisher
from scripts import rollout_lakehouse_source_policy as rollout
from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync import retained_source as retained
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from test_dataset_migration import setup


@pytest.fixture
def cutover(setup, monkeypatch):
    repo, parent, entries, _, policy = setup
    source = repo / 'data_fred_crypto_macro'
    (source / 'raw').mkdir(parents=True)
    (source / 'raw/original.gz').write_bytes(b'exact original bytes')
    (source / 'observations.parquet').write_bytes(b'original necessary view')
    cold = migration.private_history_root(parent, policy)
    fixed = retained.packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1)
    original = retained.packed.fetch_packed_snapshot(cold, repo / 'materialized', fixed)
    (source / 'observations.parquet').write_bytes(b'current necessary view')
    (source / 'raw/increment.gz').write_bytes(b'new observed increment')
    pins = repo / 'pins'
    monkeypatch.setattr(retained, 'DEFAULT_PIN_ROOT', pins)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        binding = retained.prepare_retained_source_binding(source, cold, pins, fixed,
            source_lock_fd=owner, fixed_materialized_path=original)
    candidate = repo / 'reviewed-candidate.json'
    atomic_write_json(candidate, {'binding': binding})
    entry = {**entries[1], 'dataset': 'fred-crypto-macro', 'source': str(source)}
    catalog = {'schema_version': 1, 'datasets': [entries[0], entry],
        'publication_families': [], 'unrelated_original_field': {'unchanged': True}}
    path = repo / rollout.CATALOG_RELATIVE
    path.parent.mkdir(parents=True)
    atomic_write_json(path, catalog)
    configuration = {'cold_root': str(parent)}
    before = {str(path.relative_to(source)): path.read_bytes()
              for path in source.rglob('*') if path.is_file()}
    return repo, source, candidate, path, configuration, before


def prepare(fixture):
    repo, _, candidate, _, configuration, _ = fixture
    return rollout.prepare_retained_cutover(repo, configuration, candidate)


def test_cutover_uses_original_collectors_full_cold_recovery_and_readback_without_retirement(cutover):
    repo, source, _, catalog_path, _, before = cutover
    original_catalog = catalog_path.read_bytes()
    reviewed = prepare(cutover)
    assert catalog_path.read_bytes() == original_catalog and not (source / retained.BINDING).exists()
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        result = rollout.commit_retained_cutover(reviewed, owner, repo / 'intent.json')
    assert result['state'] == 'installed_and_fixed_raw_readback_verified'
    assert result['raw_files_verified'] == 1 and result['source_files_deleted'] == 0
    assert not result['retirement_authorized'] and not result['production_incremental_round_verified']
    catalog = json.loads(catalog_path.read_bytes())
    assert catalog['unrelated_original_field'] == {'unchanged': True}
    assert catalog['datasets'][0] == json.loads(original_catalog)['datasets'][0]
    retained.validate_catalog(publisher._load_catalog(catalog_path)[1])
    assert all((source / name).read_bytes() == raw for name, raw in before.items())
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=reviewed['cold'],
                                        pin_root=retained.DEFAULT_PIN_ROOT)
    assert state.get('raw/original.gz') == b'exact original bytes'
    assert state.get('raw/increment.gz') is None  # Current increment remains in its original workspace.
    intent = json.loads((repo / 'intent.json').read_bytes())
    assert intent['catalog_before'] == json.loads(original_catalog)
    assert intent['binding']['manifest_sha256'] == result['manifest_sha256']


@pytest.mark.parametrize('fault', ['source', 'cold', 'retirement', 'fixed-manifest', 'current-raw', 'catalog-drift', 'owner'])
def test_invalid_cutover_never_installs_or_loses_source_data(cutover, fault):
    repo, source, candidate, catalog, _, before = cutover
    if fault in ('source', 'cold', 'retirement'):
        document = json.loads(candidate.read_bytes())
        key, value = {'source': ('source', str(repo / 'wrong')),
            'cold': ('cold_root', str(repo / 'wrong')),
            'retirement': ('retirement_authorized_by_binding', True)}[fault]
        document['binding'][key] = value
        atomic_write_json(candidate, document)
        with pytest.raises(ValueError):
            prepare(cutover)
    else:
        reviewed = prepare(cutover)
        if fault == 'fixed-manifest':
            reviewed['candidate']['manifest_sha256'] = 'f' * 64
        elif fault == 'current-raw':
            (source / 'raw/original.gz').write_bytes(b'corrected current observation')
        elif fault == 'catalog-drift':
            updated = json.loads(catalog.read_bytes())
            updated['independent_edit'] = True
            atomic_write_json(catalog, updated)
        observed_catalog = catalog.read_bytes()
        with retained.collector_owner(source, 'fred-crypto-macro') as owner:
            with pytest.raises((ValueError, SnapshotError)):
                rollout.commit_retained_cutover(reviewed, -1 if fault == 'owner' else owner, repo / 'intent.json')
        assert catalog.read_bytes() == observed_catalog
    assert not (source / retained.BINDING).exists()
    assert all((source / name).exists() for name in before)
    assert (source / 'raw/increment.gz').read_bytes() == b'new observed increment'


def test_interrupted_binding_installation_retains_catalog_support_and_can_resume(cutover, monkeypatch):
    repo, source, _, catalog, _, before = cutover
    reviewed = prepare(cutover)
    native = rollout.atomic_write_json
    def failed(path, value, **options):
        if path == source / retained.BINDING:
            raise OSError('interrupted binding metadata write')
        return native(path, value, **options)
    monkeypatch.setattr(rollout, 'atomic_write_json', failed)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        with pytest.raises(OSError, match='interrupted'):
            rollout.commit_retained_cutover(reviewed, owner, repo / 'first-intent.json')
    assert not (source / retained.BINDING).exists()
    assert json.loads(catalog.read_bytes())['datasets'][1]['retained_source_contract'] == retained.CONTRACT
    assert all((source / name).read_bytes() == raw for name, raw in before.items())
    old_intent = (repo / 'first-intent.json').read_bytes()
    monkeypatch.setattr(rollout, 'atomic_write_json', native)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        result = rollout.commit_retained_cutover(prepare(cutover), owner, repo / 'second-intent.json')
    assert result['state'] == 'installed_and_fixed_raw_readback_verified'
    assert (repo / 'first-intent.json').read_bytes() == old_intent


def test_existing_cutover_is_preserved_and_never_overwritten(cutover):
    repo, source, _, _, _, _ = cutover
    reviewed = prepare(cutover)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        rollout.commit_retained_cutover(reviewed, owner, repo / 'intent.json')
    before = (source / retained.BINDING).read_bytes()
    with pytest.raises(ValueError, match='already installed'):
        prepare(cutover)
    assert (source / retained.BINDING).read_bytes() == before


@pytest.mark.parametrize('extra', [[]])
def test_retained_cutover_requires_a_natural_code_only_handoff_before_service_access(tmp_path, monkeypatch, extra):
    monkeypatch.setattr(rollout, 'configuration', lambda: pytest.fail('unscoped source cutover reached production'))
    with pytest.raises(SystemExit) as error:
        rollout.main(['--output', str(tmp_path / 'receipt'), '--retained-source-candidate',
                      str(tmp_path / 'candidate'), *extra])
    assert error.value.code == 2


def test_worker_identity_can_qualify_future_catalog_bytes_without_installation(monkeypatch):
    from scripts import manage_lakehouse as worker
    changed = {}
    def digest(path):
        return changed.get(str(path.relative_to(worker.ROOT)), 'a' * 64)
    monkeypatch.setattr(worker, 'digest', digest)
    original = worker.worker_code_identity()
    raw = b'reviewed future catalog bytes'
    expected = worker.worker_code_identity(source_overrides={rollout.CATALOG_RELATIVE: raw})
    assert expected != original and worker.worker_code_identity() == original
    changed[rollout.CATALOG_RELATIVE] = hashlib.sha256(raw).hexdigest()
    assert worker.worker_code_identity() == expected
    with pytest.raises(ValueError):
        worker.worker_code_identity(source_overrides={'foreign.json': raw})
    with pytest.raises(ValueError):
        worker.worker_code_identity(source_overrides={rollout.CATALOG_RELATIVE: 'not bytes'})


@pytest.mark.parametrize('active_restart', [False, True])
def test_handoff_holds_all_original_owners_and_loads_the_reviewed_future_catalog(cutover, monkeypatch, active_restart):
    import fcntl
    import subprocess
    repo, source, candidate, catalog, c, before = cutover
    state = repo / 'state'
    state.mkdir()
    c.update(state_root=str(state), producer_device_id='producer', receiver_device_id='receiver',
             source_replication={})
    ready, private = repo / 'readiness.json', repo / 'private.json'
    atomic_write_json(ready, {'producer_device_id': 'producer', 'receiver_device_id': 'receiver',
                             'ingress_free_bytes': 1})
    private.write_bytes(b'{"original_private_policy":true}')
    monkeypatch.setattr(rollout, 'ROOT', repo)
    monkeypatch.setattr(rollout, 'RECEIVER_READINESS', ready)
    monkeypatch.setattr(rollout, 'PRIVATE_CONFIGURATION', private)
    monkeypatch.setattr(rollout, 'configuration', lambda: c)
    monkeypatch.setattr(rollout, 'guard', lambda *_: None)
    monkeypatch.setattr(rollout, 'source_wave_policy', lambda *_: None)
    def identity(**options):
        raw = options.get('source_overrides', {}).get(rollout.CATALOG_RELATIVE, catalog.read_bytes())
        return hashlib.sha256(raw).hexdigest()
    monkeypatch.setattr(rollout, 'worker_code_identity', identity)
    processes, events = {'worker': '100'}, []
    monkeypatch.setattr(rollout, 'pid', lambda unit: processes['worker'] if unit == rollout.OWNER else unit)
    async def workflows(_):
        return {name: {'status': 'RUNNING', 'first_run_id': name, 'run_id': name}
                for name in (*rollout.IDS, *rollout.OPTIONAL_IDS)}
    monkeypatch.setattr(rollout, 'workflows', workflows)
    def run(command, **options):
        assert command[:2] in (['systemctl', 'stop'], ['systemctl', 'start'])
        events.append(command[1])
        if command[1] == 'stop':
            if active_restart:
                assert (repo / 'handoff-before.json').is_file()
            else:
                for lock in (state / 'source-replication-owner.lock', state / 'catalog-owner.lock', source / retained.LOCK):
                    with lock.open('a') as other:
                        with pytest.raises(BlockingIOError):
                            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert not (source / retained.BINDING).exists()
            processes['worker'] = '0'
        else:
            processes['worker'] = '200'
            atomic_write_json(state / 'worker-ready.json', {'code_identity_sha256': identity()})
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(rollout.subprocess, 'run', run)
    output = repo / 'handoff.json'
    native_commit = rollout.commit_retained_cutover
    def commit(prepared, descriptor, intent):
        for lock in (state / 'source-replication-owner.lock', state / 'catalog-owner.lock', source / retained.LOCK):
            with lock.open('a') as other:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert processes['worker'] == '0'
        return native_commit(prepared, descriptor, intent)
    monkeypatch.setattr(rollout, 'commit_retained_cutover', commit)
    rollout.main(['--output', str(output), '--reload-code-only', '--retained-source-candidate', str(candidate),
                  *(['--restart-active-owner'] if active_restart else [])])
    receipt = json.loads(output.read_bytes())
    assert receipt['state'] == 'accepted' and receipt['authorized_active_owner_restart'] == active_restart
    assert receipt['source_cutover']['raw_files_verified'] == 1
    assert receipt['source_cutover']['source_files_deleted'] == 0
    assert receipt['loaded_code_identity_sha256'] == identity() == receipt['candidate_code_identity_sha256']
    assert receipt['postgres_and_temporal_server_not_restarted'] and receipt['workflow_chains_preserved']
    assert all((source / name).read_bytes() == raw for name, raw in before.items())
    assert private.read_bytes() == b'{"original_private_policy":true}'
    assert events == ['stop', 'start', 'start']  # The final idempotent start also protects failures.
