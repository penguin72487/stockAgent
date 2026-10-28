"""Native collector cache parity and fixed-history failure isolation."""
from datetime import datetime, timezone
from dataclasses import replace
import fcntl
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader import download_fred_crypto_macro_vintages as fred
from downloader.http_transport import HttpResponse
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import retained_source as retained
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


PAYLOAD = b'{"observations":[{"date":"2020-01-01","realtime_start":"2020-01-02","value":"-3.5"}]}'


class Transport:
    def request_bytes(self, *_args, **_kwargs):
        return HttpResponse(200, PAYLOAD, {}, 1)


def fixture(tmp_path, *, private=False):
    source, cold, pins = tmp_path / 'source', tmp_path / 'cold', tmp_path / 'pin-owner'
    if private:
        from stockagent.data_sync.dataset_migration import private_history_root
        packed.initialize_packed_layout(cold, node_id='test')
        cold = private_history_root(cold, {'private_history': {'enabled': True,
            'relative_root': '.local-state/private-history', 'shared_export': False,
            'nas_export': False, 'source_retirement': False}})
    window = fred.FredWindow('DGS10', '2020-01-01', '2021-01-01')
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        fetched = fred._fetch_window(source, window, api_key='fixture', observation_start='2020-01-01',
            observation_end='2026-01-01', max_retries=0, transport=Transport())
        assert fetched.status == 'complete'
        (source / 'observations.parquet').write_bytes(b'necessary service view')
        release = packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1)
        binding = retained.prepare_retained_source_binding(source, cold, pins, release, source_lock_fd=owner)
        assert not (source / retained.BINDING).exists()  # Preparation never activates/deletes.
        atomic_write_json(source / retained.BINDING, binding)  # Isolated fixture activation only.
    return source, cold, pins, release, window, fetched, binding


def old_fixed_materialization(tmp_path):
    """A genuine pre-owner release; current views/increments have moved on."""
    source, cold, pins = (tmp_path / name for name in ('source', 'cold', 'pins'))
    (source / 'raw').mkdir(parents=True)
    (source / 'raw/original.gz').write_bytes(b'exact original encoded bytes')
    (source / 'observations.parquet').write_bytes(b'old necessary view')
    release = packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1)
    original = packed.fetch_packed_snapshot(cold, tmp_path / 'fixed-materialized', release)
    (source / 'raw/increment.gz').write_bytes(b'new original increment')
    (source / 'observations.parquet').write_bytes(b'current necessary view')
    return source, cold, pins, release, original


def test_initial_binding_verifies_old_complete_materialization_and_current_raw_separately(tmp_path):
    source, cold, pins, release, original = old_fixed_materialization(tmp_path)
    before = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob('*') if p.is_file()}
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        with pytest.raises(SnapshotError, match='materialized'):
            retained.prepare_retained_source_binding(source, cold, pins, release, source_lock_fd=owner)
        binding = retained.prepare_retained_source_binding(source, cold, pins, release,
            source_lock_fd=owner, fixed_materialized_path=original)
        assert binding['recovery']['materialized_verified']
        assert binding['recovery']['independently_reconstructed_files'] == 2
        workspace = binding['workspace_raw_verification']
        assert workspace['source'] == str(source) and workspace['fixed_materialization'] == str(original)
        assert workspace['files_verified'] == 1 and len(workspace['raw_files_sha256']) == 64
        assert set(binding['raw_files']) == {'raw/original.gz'}
        assert not binding['retirement_authorized_by_binding'] and not (source / retained.BINDING).exists()
        assert all((source / name).read_bytes() == raw for name, raw in before.items())
        atomic_write_json(source / retained.BINDING, binding)  # Isolated fixture activation.
        state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)
        assert state.get('raw/original.gz') == b'exact original encoded bytes'


@pytest.mark.parametrize('fault', ['current-correction', 'current-missing', 'fixed-corruption',
    'fixed-extra', 'fixed-redirect', 'current-hardlink'])
def test_separate_materialization_preserves_every_original_recovery_and_current_raw_gate(tmp_path, fault):
    source, cold, pins, release, original = old_fixed_materialization(tmp_path)
    path = source / 'raw/original.gz'
    if fault == 'current-correction':
        path.write_bytes(b'corrected original')
    elif fault == 'current-missing':
        path.unlink()
    elif fault == 'fixed-corruption':
        (original / 'observations.parquet').write_bytes(b'corrupt fixed view')
    elif fault == 'fixed-extra':
        (original / 'unknown').write_bytes(b'unknown fixed materialization member')
    elif fault == 'fixed-redirect':
        redirect = tmp_path / 'redirect'
        redirect.symlink_to(original, target_is_directory=True)
        original = redirect
    else:
        (tmp_path / 'hardlink').hardlink_to(path)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        with pytest.raises(SnapshotError):
            retained.prepare_retained_source_binding(source, cold, pins, release,
                source_lock_fd=owner, fixed_materialized_path=original)
    assert not (source / retained.BINDING).exists() and not pins.exists()
    assert (source / 'raw/increment.gz').read_bytes() == b'new original increment'


def test_current_raw_change_during_complete_separate_recovery_rejects_before_pin(tmp_path, monkeypatch):
    source, cold, pins, release, original = old_fixed_materialization(tmp_path)
    verify = packed.verify_packed_snapshot
    def changed(*args, **kwargs):
        proof = verify(*args, **kwargs)
        (source / 'raw/original.gz').write_bytes(b'concurrently changed raw')
        return proof
    monkeypatch.setattr(packed, 'verify_packed_snapshot', changed)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        with pytest.raises(SnapshotError, match='generation changed'):
            retained.prepare_retained_source_binding(source, cold, pins, release,
                source_lock_fd=owner, fixed_materialized_path=original)
    assert not pins.exists() and not (source / retained.BINDING).exists()


@pytest.mark.parametrize('field', ['source', 'fixed_materialization', 'files_verified', 'raw_files_sha256'])
def test_loader_rejects_changed_exact_workspace_proof(tmp_path, field):
    source, cold, pins, release, original = old_fixed_materialization(tmp_path)
    with retained.collector_owner(source, 'fred-crypto-macro') as owner:
        binding = retained.prepare_retained_source_binding(source, cold, pins, release,
            source_lock_fd=owner, fixed_materialized_path=original)
        binding['workspace_raw_verification'][field] = 'wrong scope'
        atomic_write_json(source / retained.BINDING, binding)
    with pytest.raises(SnapshotError, match='workspace proof'):
        retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)


def test_retired_raw_cache_and_refetch_preserve_values_pit_and_first_observation(tmp_path):
    source, cold, pins, release, window, fetched, _ = fixture(tmp_path)
    raw_path = Path(fetched.raw_path)
    original = raw_path.read_bytes()
    raw_path.unlink()
    with retained.collector_owner(source, 'fred-crypto-macro'):
        state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)
        assert state.get(raw_path.relative_to(source).as_posix()) == original
        cached = fred._read_cached_window(source, window, observation_start='2020-01-01',
            end_date='2026-01-01', recheck_days=30, now=datetime.now(timezone.utc), retained_raw=state)
        assert cached.rows == fetched.rows
        assert cached.rows[0]['value'] == -3.5
        assert cached.rows[0]['available_at_utc'] == '2020-01-03T00:00:00+00:00'
        assert cached.retrieved_at_utc == fetched.retrieved_at_utc
        assert cached.raw_locator['manifest_sha256'] == release.manifest_sha256
        refreshed = fred._fetch_window(source, window, api_key='fixture', observation_start='2020-01-01',
            observation_end='2026-01-01', max_retries=0, transport=Transport(), retained_raw=state)
        assert refreshed.rows == fetched.rows
        assert refreshed.retrieved_at_utc == fetched.retrieved_at_utc
        assert refreshed.raw_locator == cached.raw_locator
        assert not raw_path.exists()  # Same API payload never creates a second raw authority.
        # A genuinely new vintage stays a current C increment until its own full
        # publication/recovery/retirement batch; no change to the fixed binding.
        new_window = fred.FredWindow('DGS10', '2021-01-02', '2026-01-01')
        new = fred._fetch_window(source, new_window, api_key='fixture', observation_start='2020-01-01',
            observation_end='2026-01-01', max_retries=0, transport=Transport(), retained_raw=state)
        assert Path(new.raw_path).is_file() and new.raw_locator is None
        current = packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1,
            retained_source_files=state.request, source_guard=state.check_generation)
    restored = packed.fetch_packed_snapshot(cold, tmp_path / 'restored', current)
    assert (restored / raw_path.relative_to(source)).read_bytes() == original
    assert (restored / Path(new.raw_path).relative_to(source)).read_bytes() == Path(new.raw_path).read_bytes()
    assert packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1,
        retained_source_files=state.request, source_guard=state.check_generation).manifest_sha256 == current.manifest_sha256


@pytest.mark.parametrize('problem', ['dataset', 'cold-root', 'binding-sha', 'inventory-sha', 'raw-sha',
                                   'proof', 'pin-missing', 'redirect', 'duplicate-field', 'raw-kind'])
def test_bad_binding_or_cold_identity_never_becomes_a_cache_miss(tmp_path, problem):
    source, cold, pins, _, _, _, binding = fixture(tmp_path)
    if problem == 'dataset':
        binding['dataset'] = 'other'
    elif problem == 'cold-root':
        binding['cold_root'] = str(tmp_path / 'another-cold')
    elif problem == 'binding-sha':
        binding['manifest_sha256'] = '0' * 64
    elif problem == 'inventory-sha':
        binding['inventory_sha256'] = '0' * 64
    elif problem == 'raw-sha':
        next(iter(binding['raw_files'].values()))['sha256'] = '0' * 64
    elif problem == 'proof':
        binding['recovery']['materialized_verified'] = False
    elif problem == 'pin-missing':
        next(pins.rglob('*.pin.json')).unlink()
    elif problem == 'raw-kind':
        binding['raw_files'] = {'state/collector.lock': {'bytes': 0, 'sha256': '0' * 64}}
    marker = source / retained.BINDING
    atomic_write_json(marker, binding)
    if problem == 'redirect':
        moved = marker.with_suffix('.moved')
        marker.rename(moved)
        marker.symlink_to(moved.name)
    elif problem == 'duplicate-field':
        marker.write_text('{"contract":"first",' + marker.read_text()[1:])
    with pytest.raises(SnapshotError):
        retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)


def test_binding_and_pin_generation_stay_fixed_for_the_complete_collector_transaction(tmp_path):
    source, cold, pins, _, _, _, binding = fixture(tmp_path)
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)
    atomic_write_json(source / retained.BINDING, binding)
    with pytest.raises(SnapshotError, match='changed'):
        state.check_generation()
    with pytest.raises(SnapshotError, match='changed'):
        state.check_binding_generation()


def test_unbound_collector_does_not_access_cold_or_create_a_binding(tmp_path, monkeypatch):
    tmp_path.mkdir(exist_ok=True)
    monkeypatch.setattr(packed, 'resolve_packed_pin', lambda *a, **k: pytest.fail('unbound collector opened cold'))
    assert retained.load_retained_source(tmp_path, 'fred-crypto-macro') is None
    assert not (tmp_path / retained.BINDING).exists()


def test_collector_owner_matches_publisher_and_cannot_be_borrowed_unlocked(tmp_path):
    with retained.collector_owner(tmp_path, 'fred-crypto-macro') as descriptor:
        with (tmp_path / retained.LOCK).open('rb') as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(SnapshotError, match='exclusive'):
                retained.require_collector_owner(tmp_path, other.fileno())
        retained.require_collector_owner(tmp_path, descriptor)


def test_malformed_binding_stops_before_any_credentials_or_network_call(tmp_path, monkeypatch):
    (tmp_path / 'state').mkdir()
    atomic_write_json(tmp_path / retained.BINDING, {'invalid': 'binding'})
    monkeypatch.setattr(fred.sys, 'argv', ['fred', '--output-dir', str(tmp_path)])
    monkeypatch.setattr(fred, '_configured_api_key', lambda *a: pytest.fail('invalid binding reached credentials'))
    monkeypatch.setattr(fred, '_resolve_end_date', lambda *a, **k: pytest.fail('invalid binding reached provider'))
    with pytest.raises(SnapshotError):
        fred.main()


def test_original_private_publisher_combines_retained_history_and_current_workspace(tmp_path, monkeypatch):
    from scripts import publish_data_releases as publisher
    from stockagent.data_sync import dataset_migration as migration
    source, cold, pins, base, _, fetched, _ = fixture(tmp_path, private=True)
    parent = cold.parent.parent
    policy = {'file_chunk_bytes': 1024, 'private_history': {'enabled': True,
        'relative_root': '.local-state/private-history', 'shared_export': False,
        'nas_export': False, 'source_retirement': False}}
    entry = {'dataset': 'fred-crypto-macro', 'source': str(source), 'publish': False,
        'note': 'isolated native collector/publication fixture',
        'role': 'local-analytics-and-point-in-time-macro', 'pack_buckets': 1, 'loose_threshold_mib': 8,
        'retained_source_contract': retained.CONTRACT,
        'source_coordination_lock_scope': 'source_parent',
        'source_coordination_lock': f'{source.name}/{retained.LOCK}'}
    args = SimpleNamespace(node_id=None, migration_state=tmp_path / 'journal')
    monkeypatch.setattr(publisher, 'REPO_ROOT', tmp_path)
    loader = retained.load_retained_source
    monkeypatch.setattr(retained, 'load_retained_source',
        lambda *a, **kw: loader(*a, **kw, pin_root=pins))
    raw_path = Path(fetched.raw_path)
    original = raw_path.read_bytes()
    raw_path.unlink()
    increment = source / 'raw/new.gz'
    increment.write_bytes(b'genuine new raw increment')
    current, status = migration._private_publish(tmp_path, entry, args, parent, policy, [entry])
    assert status['state'] == 'published'
    assert current.manifest['source']['selection']['retained_source_base']['manifest_sha256'] == base.manifest_sha256
    assert not (parent / 'heads/fred-crypto-macro').exists()
    restored = packed.fetch_packed_snapshot(cold, tmp_path / 'hot', current)
    assert (restored / raw_path.relative_to(source)).read_bytes() == original
    assert (restored / 'raw/new.gz').read_bytes() == increment.read_bytes()
    again, _ = migration._private_publish(tmp_path, entry, args, parent, policy, [entry])
    assert again.manifest_sha256 == current.manifest_sha256
    without_contract = {key: value for key, value in entry.items() if key != 'retained_source_contract'}
    with pytest.raises(SnapshotError, match='catalogued'):
        migration._private_publish(tmp_path, without_contract, args, parent, policy, [without_contract])


def test_publisher_reuses_only_this_live_verified_read_and_keeps_the_collectors_originals(tmp_path, monkeypatch):
    source, cold, pins, _, _, fetched, _ = fixture(tmp_path)
    original_path = Path(fetched.raw_path)
    raw = original_path.read_bytes()
    original_path.unlink()
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)
    assert state.request._verified_read is not None
    monkeypatch.setattr(packed, '_read_packed_files_verified',
        lambda *a, **k: pytest.fail('same collector/publisher transaction repeated its original decode'))
    saved = packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1,
        retained_source_files=state.request, source_guard=state.check_generation)
    assert state.get(original_path.relative_to(source).as_posix()) == raw
    assert packed.verify_packed_snapshot(cold, saved, reconstruct_all=True)['independently_reconstructed_files'] == saved.manifest['source']['files']


@pytest.mark.parametrize('fault', ['metadata', 'manifest', 'byte-bound', 'fake-proof', 'cold-generation'])
def test_verified_read_transfer_rechecks_metadata_fixed_identity_admission_and_cold_generation(tmp_path, fault):
    source, cold, pins, base, _, _, _ = fixture(tmp_path)
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)
    request = state.request
    if fault == 'metadata':
        request._verified_read.entries[0]['sha256'] = '0' * 64
    elif fault == 'manifest':
        request = replace(request, release=replace(request.release,
            manifest={**request.release.manifest, 'metadata': {'invented': 'manifest'}}))
    elif fault == 'byte-bound':
        request = replace(request, maximum_bytes=1)
    elif fault == 'fake-proof':
        request = replace(request, _verified_read=object())
    elif fault == 'cold-generation':
        path = cold / base.manifest['archive']['objects'][0]['relpath']
        replacement = path.with_suffix('.changed')
        replacement.write_bytes(path.read_bytes())
        replacement.replace(path)
    with pytest.raises(SnapshotError, match='proof|identity|generation'):
        packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1,
            retained_source_files=request, source_guard=state.check_binding_generation)
    assert packed.resolve_latest_packed(cold, 'fred-crypto-macro').manifest_sha256 == base.manifest_sha256


def test_binding_fence_is_separate_from_the_original_cold_payload_fence(tmp_path, monkeypatch):
    source, cold, pins, _, _, _, _ = fixture(tmp_path)
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins)
    def cold_unavailable(*_args):
        raise RuntimeError('cold authority unavailable')
    monkeypatch.setattr(packed, '_check_packed_read_authority', cold_unavailable)
    state.check_binding_generation()  # Mutable controls need no redundant D walk.
    with pytest.raises(RuntimeError, match='cold authority'):
        state.check_generation()  # Full collector acceptance still fences D.


def test_concurrent_deferred_reads_decode_once_and_transfer_the_same_live_proof(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    source, cold, pins, _, _, fetched, _ = fixture(tmp_path)
    path = Path(fetched.raw_path)
    original = path.read_bytes()
    path.unlink()  # Owned fixture only.
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins,
        read_originals=False, defer_originals=True)
    count = []
    actual = packed._read_packed_files_verified
    def counted(*args, **kwargs):
        count.append(1)
        return actual(*args, **kwargs)
    monkeypatch.setattr(packed, '_read_packed_files_verified', counted)
    assert state.get('raw/not-in-this-release.gz') is None and not count
    with ThreadPoolExecutor(max_workers=4) as pool:
        values = list(pool.map(state.get, [path.relative_to(source).as_posix()] * 4))
    assert values == [original] * 4 and len(count) == 1
    request = state.publication_request()
    assert request._verified_read is not None
    packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=1,
        retained_source_files=request, source_guard=state.check_binding_generation)
    assert len(count) == 1
