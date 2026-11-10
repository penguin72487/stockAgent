import json
from pathlib import Path
import tarfile

import pytest

from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_workspace_mount_enqueues_original_lifecycle_without_forcing_restart():
    repo = Path(__file__).resolve().parents[1]
    unit = (repo/'deploy/systemd/stockagent-e-workspace-mount.service.in').read_text()
    lifecycle = (repo/'deploy/systemd/stockagent-storage-lifecycle.service.in').read_text()
    assert 'Wants=stockagent-storage-lifecycle.service' in unit
    assert 'Before=stockagent-storage-lifecycle.service stockagent-backup-transport-mount.service' in unit
    assert 'Upholds=' not in unit and 'OnSuccess=' not in unit
    assert 'Requires=stockagent-d-cold-mount.service stockagent-backup-transport-mount.service' in lifecycle
    assert 'Restart=on-failure' in lifecycle


@pytest.mark.parametrize('initial,expected_calls', [
    ('rw,relatime,discard', 1),
    ('rw,relatime', 0),
])
def test_workspace_mount_disables_online_discard_once(monkeypatch, initial, expected_calls):
    from scripts import manage_workspace_volume as manager
    from types import SimpleNamespace
    state = {'options': initial, 'mounts': []}

    def run(args, **kwargs):
        if args[0] == 'findmnt':
            assert args[-2:] == ['--mountpoint', '/enrolled-e']
            return SimpleNamespace(stdout=state['options'] + '\n')
        assert args == ['mount', '-o', 'remount,nodiscard', '/enrolled-e']
        state['mounts'].append(args)
        state['options'] = 'rw,relatime'
        return SimpleNamespace(stdout='')

    monkeypatch.setattr(manager.subprocess, 'run', run)
    manager.disable_workspace_online_discard(Path('/enrolled-e'))
    assert len(state['mounts']) == expected_calls


@pytest.mark.parametrize('observed', ['', 'rw,discard\nrw', 'rw,discard'])
def test_workspace_discard_policy_rejects_unknown_or_unchanged_mount(monkeypatch, observed):
    from scripts import manage_workspace_volume as manager
    from types import SimpleNamespace
    monkeypatch.setattr(manager.subprocess, 'run',
                        lambda *args, **kwargs: SimpleNamespace(stdout=observed))
    with pytest.raises(ValueError, match='options|discard'):
        manager.disable_workspace_online_discard(Path('/enrolled-e'))


def test_workspace_mount_checks_e_identity_before_changing_discard(tmp_path, monkeypatch):
    from scripts import manage_workspace_volume as manager
    shared, target = tmp_path/'shared', tmp_path/'target'
    shared.mkdir()
    target.mkdir()
    enrollment = tmp_path/'enrollment.json'
    enrollment.write_text('{}')
    c = {'shared_root': str(shared), 'mount_point': str(target), 'distribution': 'StockAgentData'}
    (shared/volume.MARKER).write_text(json.dumps({'fixed': True}))
    events = []
    monkeypatch.setattr(manager, 'ENROLLMENT', enrollment)
    monkeypatch.setattr(manager, 'native_identity', lambda _: {'registration': True})
    monkeypatch.setattr(manager, 'wsl_command', lambda *args, **kwargs: None)
    monkeypatch.setattr(manager, '_mount_info', lambda _: {'fstype': 'ext4'})
    monkeypatch.setattr(manager, 'marker', lambda _: {'fixed': True})
    monkeypatch.setattr(Path, 'is_mount', lambda path: path in (shared, target))
    monkeypatch.setattr(manager, 'check', lambda **kwargs: events.append('verified') or {'state': 'mounted'})
    monkeypatch.setattr(manager, 'disable_workspace_online_discard',
                        lambda path: events.append(('nodiscard', path)))
    assert manager.mount(c) == {'state': 'mounted'}
    assert events == ['verified', ('nodiscard', shared)]


def test_work_volume_does_not_relocate_cold_authority(tmp_path):
    c = volume.configuration()
    assert c["physical_volume"] == "E:"
    assert c["cold_physical_volume"] == "D:"
    c["cold_root"] = "/mnt/e/cold"
    invalid = tmp_path / "config.json"
    invalid.write_text(json.dumps(c))
    with pytest.raises(SnapshotError, match="configuration"):
        volume.configuration(invalid)


def test_different_or_unregistered_workspace_is_not_admitted(tmp_path, monkeypatch):
    c = volume.configuration()
    target, shared = tmp_path / "target", tmp_path / "shared"
    target.mkdir()
    shared.mkdir()
    c.update(mount_point=str(target), shared_root=str(shared))
    monkeypatch.setattr(volume, "_mount_info", lambda p: {"target": str(target), "fstype": "ext4", "uuid": "one"})
    with pytest.raises(SnapshotError, match="enrolled shared"):
        volume.check(c=c)
    shared.rmdir()
    c["shared_root"] = str(target)
    (target / volume.MARKER).write_text(json.dumps(volume.marker(c)))
    enrollment = tmp_path / "enrollment.json"
    monkeypatch.setattr(volume, "ENROLLMENT", enrollment)
    with pytest.raises(SnapshotError, match="no fixed local enrollment"):
        volume.check(c=c)
    enrollment.write_text(json.dumps({"marker": volume.marker(c), "filesystem_uuid": "other"}))
    with pytest.raises(SnapshotError, match="UUID differs"):
        volume.check(c=c)
    enrollment.write_text(json.dumps({"marker": volume.marker(c), "filesystem_uuid": "one"}))
    assert volume.check(c=c)["state"] == "mounted"


def test_native_e_identity_requires_original_disk_and_location(monkeypatch):
    c = volume.configuration()
    info = {"volume_id": "\\\\?\\Volume{" + c["volume_id"] + "}\\",
            "disk_unique_id": c["disk_unique_id"], "filesystem": "NTFS", "free_bytes": 10**12,
            "registration": {"base_path": "\\\\?\\" + c["install_location"], "version": 2}}
    monkeypatch.setattr(volume, "native", lambda *a, **k: info)
    assert volume.native_identity(c)["registration"]["version"] == 2
    info["registration"]["base_path"] = "C:\\wrong"
    with pytest.raises(SnapshotError, match="fixed E location"):
        volume.native_identity(c)
    info["registration"] = None
    info["disk_unique_id"] = "another disk"
    with pytest.raises(SnapshotError, match="NVMe identity"):
        volume.native_identity(c)


def test_finlab_futures_workspace_must_be_the_enrolled_mounted_alias(tmp_path, monkeypatch):
    c = volume.configuration()
    c['mount_point'] = str(tmp_path / 'e')
    source = Path(c['mount_point']) / volume.FINLAB_FUTURES_ROLE
    target = tmp_path / 'objects/futures-workspace-v1'
    source.mkdir(parents=True)
    target.mkdir(parents=True)
    monkeypatch.setattr(volume, 'FINLAB_FUTURES_ALIAS', target)
    monkeypatch.setattr(volume, 'check', lambda **kwargs: {'state': 'mounted'})
    identity = {'contract': volume.FINLAB_FUTURES_CONTRACT, 'volume_id': c['volume_id'],
                'source': str(source), 'target': str(target)}
    (source / volume.FINLAB_FUTURES_MARKER).write_text(json.dumps(identity))
    with pytest.raises(SnapshotError, match='absent or redirected'):
        volume.check_finlab_futures(c=c)
    original_mount, original_samefile = Path.is_mount, Path.samefile
    # Same-filesystem binds are real mounts even though Path.is_mount is false.
    monkeypatch.setattr(Path, 'is_mount', lambda p: False if p == target else original_mount(p))
    monkeypatch.setattr(volume, '_mount_info', lambda p: {'target': str(target)})
    monkeypatch.setattr(Path, 'samefile', lambda p, other: True if (p, other) == (target, source)
                        else original_samefile(p, other))
    assert volume.check_finlab_futures(c=c)['target'] == str(target)
    identity['volume_id'] = 'different-volume'
    (source / volume.FINLAB_FUTURES_MARKER).write_text(json.dumps(identity))
    with pytest.raises(SnapshotError, match='identity differs'):
        volume.check_finlab_futures(c=c)


def test_finlab_futures_mount_never_hides_existing_objects(tmp_path, monkeypatch):
    from scripts import manage_workspace_volume as manager
    c = volume.configuration()
    c['mount_point'] = str(tmp_path / 'e')
    source_parent = Path(c['mount_point']) / 'downloads'
    target = tmp_path / 'intraday/objects/futures-workspace-v1'
    source_parent.mkdir(parents=True)
    target.mkdir(parents=True)
    (target / 'existing.parquet').write_bytes(b'preserve')
    monkeypatch.setattr(manager, 'FINLAB_FUTURES_ALIAS', target)
    monkeypatch.setattr(manager, 'admit', lambda **kwargs: {'state': 'mounted'})
    monkeypatch.setattr(manager.subprocess, 'run', lambda *a, **k: pytest.fail('must not mount'))
    with pytest.raises(ValueError, match='refuse to hide'):
        manager.mount_finlab_futures(c)
    assert (target / 'existing.parquet').read_bytes() == b'preserve'


def test_finlab_futures_mount_requires_registered_workspace_identity(tmp_path, monkeypatch):
    from scripts import manage_workspace_volume as manager
    c = volume.configuration()
    c['mount_point'] = str(tmp_path / 'e')
    source = Path(c['mount_point']) / manager.FINLAB_FUTURES_ROLE
    target = tmp_path / 'intraday/objects/futures-workspace-v1'
    source.mkdir(parents=True)
    target.parent.mkdir(parents=True)
    monkeypatch.setattr(manager, 'FINLAB_FUTURES_ALIAS', target)
    monkeypatch.setattr(manager, 'admit', lambda **kwargs: {'state': 'mounted'})
    monkeypatch.setattr(manager.subprocess, 'run', lambda *a, **k: pytest.fail('must not mount'))
    with pytest.raises(ValueError, match='unrecognized'):
        manager.mount_finlab_futures(c)
    assert not target.exists()


def test_data_rootfs_contains_no_application_services_or_credentials(tmp_path):
    from scripts.manage_workspace_volume import rootfs
    archive = tmp_path / "data.tar"
    binaries = rootfs(volume.configuration(), archive)
    assert "/bin/bash" in binaries
    with tarfile.open(archive) as stream:
        names = stream.getnames()
        assert all(not name.startswith(("root/", "var/", "home/")) for name in names)
        assert "bin/sh" in names
        assert "workspace/" + volume.MARKER in names
        assert b"systemd=false" in stream.extractfile("etc/wsl.conf").read()
        assert not any(".ssh" in name or ".env" in name or "systemd/system" in name for name in names)


def test_monitor_work_volume_is_nested_and_never_added_to_host_capacity():
    from stockagent.data_sync import storage_monitor as monitor
    observed=monitor.utc()
    cap={'state':'measured','observed_at_utc':observed,'source':'statvfs',
         'total_bytes':1000,'used_bytes':100,'free_bytes':900,'available_bytes':850,'reserved_free_bytes':50}
    raw={'contract':monitor.CONTRACT,'volumes':[
        {'id':'penguin-e','capacity':cap},
        {'id':'penguin-workspace','capacity':cap,'contained_in':'penguin-e'}]}
    result=monitor.public_status(raw)
    row=next(v for n in result['nodes'] for v in n['volumes'] if v['id']=='penguin-workspace')
    assert row['contained_in']=='penguin-e'
    assert result['aggregate_capacity_bytes'] is None


def test_relocation_monitor_keeps_inventory_unknown_and_does_not_publish_private_paths(tmp_path):
    from stockagent.data_sync import storage_progress as progress
    import time
    now=time.time()
    (tmp_path/'intent.json').write_text(json.dumps({'started_at_epoch':now,'source':'/private','files':4,'logical_bytes':100}))
    (tmp_path/'progress.json').write_text(json.dumps({'state':'inventory','entries':8,'observed_at_epoch':now}))
    row,=progress.workspace_transport_tasks(tmp_path)
    assert row['total_items'] is None
    assert progress.estimate(row,{},now=now)[0]['completion_at_utc'] is None
    value=progress.public_status({'contract':progress.CONTRACT,'observed_at_utc':progress.iso(now),'tasks':[row]},now=now)
    assert value['tasks'][0]['kind']=='workspace_transport'
    assert '/private' not in json.dumps(value)


def test_relocation_failure_stays_visible_until_real_progress_resumes(tmp_path):
    from stockagent.data_sync import storage_progress as progress
    (tmp_path/'intent.json').write_text(json.dumps({'started_at_epoch':100,'files':4,'logical_bytes':100}))
    snapshot={'state':'copying_and_independent_sha_readback','bytes_completed':20,
              'observed_at_epoch':110}
    (tmp_path/'progress.json').write_text(json.dumps(snapshot))
    (tmp_path/'attempt.json').write_text(json.dumps({'state':'relocation_failed',
        'observed_at_epoch':120,'reason_code':'retirement_validation','private_error':'/secret'}))
    row,=progress.workspace_transport_tasks(tmp_path)
    assert row['state']=='blocked' and row['observed_at_utc']==progress.iso(120)
    assert row['completed_bytes']==20
    assert '/secret' not in json.dumps(row)
    snapshot['observed_at_epoch']=130
    (tmp_path/'progress.json').write_text(json.dumps(snapshot))
    row,=progress.workspace_transport_tasks(tmp_path)
    assert row['state']=='running' and row['reason'] is None
