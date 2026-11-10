import hashlib
import json
from pathlib import Path

import pytest

from scripts import install_isolated_edge_reader as installer
from scripts import run_edge_reader_recovery as worker
from stockagent.data_sync import edge_reader_bundle_preflight as preflight, edge_reader_recovery as recovery
from stockagent.data_sync.desync_snapshots import SnapshotError


def sha(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def case(tmp_path):
    repo, site, workspace = (tmp_path/name for name in ('repo', 'site', 'workspace'))
    repo.mkdir(); site.mkdir(); workspace.mkdir()
    source = {'scripts/reader.py': b'code\n'}
    vendor = {'vendor/ijson/__init__.py': b'dependency\n'}
    for name, body in source.items():
        path = repo/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
    for name, body in vendor.items():
        path = site/name[len('vendor/'):]; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
    source_map = {name: sha(body) for name, body in source.items()}
    config = repo/'configs/data_sync'; config.mkdir(parents=True)
    (config/'edge_reader_v3_source_sha.json').write_text(json.dumps(source_map))
    manifest = installer._manifest({**source, **vendor})
    (config/'edge_reader_v3_manifest.sha256').write_bytes(manifest)
    wrapper, deployer = b'wrapper\n', b'installer\n'
    (repo/'scripts/run_data_cache.sh').write_bytes(wrapper)
    (repo/'scripts/install_isolated_edge_reader.py').write_bytes(deployer)
    value = {'contract': recovery.CONTRACT, 'enabled': True,
             'source_identity_sha256': sha(json.dumps(source_map, sort_keys=True, separators=(',', ':')).encode()),
             'reader_manifest_sha256': sha(manifest), 'wrapper_sha256': sha(wrapper),
             'installer_sha256': sha(deployer), 'baseline_wrapper_sha256': 'a'*64,
             'dataset': 'fixture'}
    return repo, site, workspace, value


def test_bundle_is_exact_private_and_idempotent(case):
    repo, site, workspace, value = case
    result = recovery.build(repo, workspace, site, value)
    bundle = Path(result['bundle'])
    assert result['state'] == 'built' and result['reader_file_count'] == 2
    assert not result['market_payload_included'] and not result['credentials_included']
    assert bundle.stat().st_mode & 0o777 == 0o700
    assert recovery.verify(bundle, value)['reader_manifest_sha256'] == value['reader_manifest_sha256']
    assert recovery.build(repo, workspace, site, value)['state'] == 'already_built'


def test_bundle_rejects_changed_input_and_retains_existing_release(case):
    repo, site, workspace, value = case
    result = recovery.build(repo, workspace, site, value)
    bundle = Path(result['bundle'])
    (repo/'scripts/reader.py').write_text('changed\n')
    with pytest.raises(SnapshotError, match='input changed'):
        recovery.build(repo, workspace, site, value)
    assert recovery.verify(bundle, value)['market_payload_included'] is False


def test_bundle_rejects_modified_or_extra_vendor_member(case):
    repo, site, workspace, value = case
    bundle = Path(recovery.build(repo, workspace, site, value)['bundle'])
    (bundle/'source/vendor/ijson/extra.py').write_text('unexpected\n')
    with pytest.raises(SnapshotError, match='members differ'):
        recovery.verify(bundle, value)


def test_bundle_build_stops_if_workspace_generation_changes(case, monkeypatch, tmp_path):
    repo, site, workspace, value = case
    original = Path.write_bytes
    moved = tmp_path/'old-workspace'
    changed = []

    def write(path, body):
        result = original(path, body)
        if '.reader-bundle-' in str(path) and not changed:
            changed.append(True)
            workspace.rename(moved)
            workspace.mkdir()
        return result

    monkeypatch.setattr(Path, 'write_bytes', write)
    with pytest.raises(SnapshotError, match='workspace changed'):
        recovery.build(repo, workspace, site, value)
    assert changed and not (workspace/'migration').exists()
    assert any((moved/'migration/edge-reader-recovery-v1').iterdir())


def test_policy_must_match_existing_reader_enrollment(case):
    repo, _, _, value = case
    path, activation = repo/'recovery.json', repo/'activation.json'
    path.write_text(json.dumps(value))
    enrollment = {'contract': 'stockagent.edge_reader_activation_policy.v1',
                  'enabled': True,
                  'remote_reader_manifest_sha256': value['reader_manifest_sha256'],
                  'remote_wrapper_sha256': value['wrapper_sha256'], 'probe_dataset': 'fixture'}
    activation.write_text(json.dumps(enrollment))
    assert recovery.policy(path, activation) == value
    enrollment['remote_wrapper_sha256'] = '0'*64
    activation.write_text(json.dumps(enrollment))
    with pytest.raises(SnapshotError, match='does not match'):
        recovery.policy(path, activation)


def test_staged_bundle_checks_all_bytes_before_any_remote_execution(case):
    repo, site, workspace, value = case
    bundle = Path(recovery.build(repo, workspace, site, value)['bundle'])
    request = {'root': str(bundle), **{key: value[key] for key in
               ('source_identity_sha256', 'reader_manifest_sha256', 'wrapper_sha256', 'installer_sha256')}}
    assert preflight.inspect(request, enforce_temporary_scope=False)['state'] == 'verified'
    (bundle/'source/vendor/ijson/__init__.py').write_text('different code\n')
    with pytest.raises(ValueError, match='manifest differs'):
        preflight.inspect(request, enforce_temporary_scope=False)


def test_staged_bundle_refuses_redirected_members_and_unscoped_tmp(case):
    repo, site, workspace, value = case
    bundle = Path(recovery.build(repo, workspace, site, value)['bundle'])
    request = {'root': str(bundle), **{key: value[key] for key in
               ('source_identity_sha256', 'reader_manifest_sha256', 'wrapper_sha256', 'installer_sha256')}}
    with pytest.raises(ValueError, match='request differs'):
        preflight.inspect(request)
    path = bundle/'source/vendor/ijson/__init__.py'
    path.unlink(); path.symlink_to(repo/'scripts/reader.py')
    with pytest.raises(ValueError, match='redirected'):
        preflight.inspect(request, enforce_temporary_scope=False)


def _ready(value):
    return {'state': 'ready', 'reader_manifest_sha256': value['reader_manifest_sha256'],
            'wrapper_sha256': value['wrapper_sha256'], 'schema_version': 3,
            'probe_dataset': value['dataset'],
            'probe_dataset_manifest_sha256': 'd'*64, 'payload_fetched': False, 'payload_verified': False}


def test_recovery_owner_does_not_modify_a_healthy_remote(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True, probe=lambda _: _ready(value),
        preflight=lambda _: pytest.fail('healthy reader must not be reinstalled'),
        deploy=lambda *_: pytest.fail('healthy reader must not be reinstalled'))
    assert result['state'] == 'ready' and not result['remote_payload_fetched']
    saved = json.loads((workspace/'migration/edge-reader-recovery-v1/status.json').read_text())
    assert saved['state'] == 'ready' and saved['NAS_accepted'] is False


def _during_trading():
    raise SnapshotError(worker.TRADING_DEFERRED)


def test_healthy_reader_is_observed_during_trading_without_mutation(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        mutation_guard=_during_trading, probe=lambda _: _ready(value),
        preflight=lambda _: pytest.fail('healthy reader needs no recovery'),
        deploy=lambda *_: pytest.fail('trading must not deploy code'))
    assert result['state'] == 'ready'
    assert json.loads((workspace/'migration/edge-reader-recovery-v1/status.json').read_text())['state'] == 'ready'


def test_missing_reader_waits_for_trading_before_deployment(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        mutation_guard=_during_trading, probe=lambda _: (_ for _ in ()).throw(OSError('offline')),
        preflight=lambda _: {'state': 'eligible'},
        deploy=lambda *_: pytest.fail('trading must not deploy code'))
    assert result['state'] == 'waiting_trading'
    assert not result['remote_payload_fetched'] and not result['NAS_accepted']


def test_missing_bundle_waits_for_trading_before_rebuild(case):
    repo, site, workspace, value = case
    result = recovery.build(repo, workspace, site, value)
    import shutil
    shutil.rmtree(result['bundle'])
    observed = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        mutation_guard=_during_trading, rebuild=lambda: pytest.fail('trading must not rebuild bundle'),
        probe=lambda _: pytest.fail('unavailable bundle cannot be validated'))
    assert observed['state'] == 'waiting_trading'


def test_recovery_defers_if_trading_starts_during_install(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    observed = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        probe=lambda _: (_ for _ in ()).throw(OSError('offline')),
        preflight=lambda _: {'state': 'eligible'}, deploy=lambda *_: _during_trading())
    assert observed['state'] == 'waiting_trading'


def test_remote_install_checks_trading_again_before_selecting_reader(monkeypatch):
    from contextlib import contextmanager
    calls = []
    monkeypatch.setattr(worker, '_expected_dataset', lambda *_: 'd'*64)
    @contextmanager
    def staged(*_args, **_kwargs):
        yield [], 'host', '/repo', '/tmp/bundle', {}
    monkeypatch.setattr(worker, 'staged_bundle', staged)
    monkeypatch.setattr(worker, '_stage_remote', lambda *_: calls.append('stage'))
    monkeypatch.setattr(worker, '_activate_remote', lambda *_: pytest.fail('must not change reader during trading'))
    def guard():
        calls.append('guard')
        if calls.count('guard') == 3:
            _during_trading()
    with pytest.raises(SnapshotError, match='day-trade priority'):
        worker.remote_deploy(Path('/bundle'), Path('/cold'), {'dataset': 'fixture'}, mutation_guard=guard)
    assert calls == ['guard', 'guard', 'stage', 'guard']


def test_recovery_status_is_not_written_after_workspace_guard_fails(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    def missing_workspace():
        raise SnapshotError('workspace vanished')
    with pytest.raises(SnapshotError, match='workspace vanished'):
        worker.run(workspace, repo, value, {'enabled': True}, apply=True,
                   probe=lambda _: _ready(value), ensure_workspace=missing_workspace)
    assert not (workspace/'migration/edge-reader-recovery-v1/status.json').exists()


def test_disabling_original_reader_enrollment_disables_recovery(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    result = worker.run(workspace, repo, value, {'enabled': False}, apply=True,
        probe=lambda _: pytest.fail('disabled policy must not contact remote'),
        preflight=lambda _: pytest.fail('disabled policy must not contact remote'),
        deploy=lambda *_: pytest.fail('disabled policy must not install remote code'))
    assert result['state'] == 'disabled'


def test_missing_bundle_rebuilds_only_through_pinned_builder(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    bundle = workspace/'migration/edge-reader-recovery-v1'/value['reader_manifest_sha256']
    import shutil
    shutil.rmtree(bundle)
    calls = []
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        rebuild=lambda: calls.append('verified-builder') or recovery.build(repo, workspace, site, value),
        probe=lambda _: _ready(value), preflight=lambda _: pytest.fail('reader already healthy'))
    assert calls == ['verified-builder'] and result['state'] == 'ready'
    assert recovery.verify(bundle, value)['credentials_included'] is False


@pytest.mark.parametrize('reason', ['base_not_enrolled', 'control_mismatch', 'wrapper_mismatch',
                                    'reader_intact', 'release_mismatch'])
def test_recovery_owner_preserves_unrecognized_remote_state(case, reason):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        probe=lambda _: (_ for _ in ()).throw(OSError('offline')),
        preflight=lambda _: {'state': reason}, deploy=lambda *_: pytest.fail('must not replace other state'))
    assert result['state'] == reason and not result['market_payload_deleted']


def test_recovery_owner_requires_post_install_probe(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    observations = iter([OSError('missing'), _ready(value)])
    def probe(_):
        value = next(observations)
        if isinstance(value, Exception):
            raise value
        return value
    calls = []
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True, probe=probe,
        preflight=lambda _: {'state': 'eligible'},
        deploy=lambda *args: calls.append(args) or {'state': 'installed', 'payload_fetched': False})
    assert len(calls) == 1 and result['state'] == 'recovered'
    assert result['probe_dataset_manifest_sha256'] == 'd'*64
    assert not result['remote_payload_fetched'] and not result['NAS_accepted']
    later = worker.run(workspace, repo, value, {'enabled': True}, apply=True, probe=lambda _: _ready(value))
    assert later['state'] == 'ready'
    assert later['last_recovered_at_epoch'] == result['last_recovered_at_epoch']


def test_recovery_owner_does_not_claim_success_on_failed_post_probe(case):
    repo, site, workspace, value = case
    recovery.build(repo, workspace, site, value)
    result = worker.run(workspace, repo, value, {'enabled': True}, apply=True,
        probe=lambda _: (_ for _ in ()).throw(OSError('unavailable')),
        preflight=lambda _: {'state': 'eligible'},
        deploy=lambda *_: {'state': 'installed', 'payload_fetched': False})
    assert result['state'] == 'retrying' and 'probe_dataset_manifest_sha256' not in result
