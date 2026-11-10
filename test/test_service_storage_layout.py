"""Service placement preserves one state owner and exact live file inodes."""
import os
from pathlib import Path

import pytest

from scripts.organize_service_storage import audit_tree, organize
from stockagent.service_layout import canonical_service_path, service_moves, service_path


def checkout(tmp_path):
    (tmp_path / ".git").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/runtime_env.sh").touch()
    (tmp_path / "artifacts/live").mkdir(parents=True)
    return tmp_path


def test_live_placement_preserves_inode_fd_internal_link_and_legacy_reader(tmp_path):
    root = checkout(tmp_path)
    source = root / "artifacts/live"
    value = source / "ledger.json"
    value.write_bytes(b'{"fills":[{"quantity":1000}]}')
    (source / "head").symlink_to("ledger.json")
    info = value.stat()
    dry = organize(root, ["live"])
    assert dry["rows"][0]["state"] == "planned"
    assert not source.is_symlink()
    with value.open("rb") as held:
        applied = organize(root, ["live"], apply=True)
        assert held.read() == value.read_bytes()
    target = root / "service_data/live"
    assert applied["rows"][0]["state"] == "placed"
    assert source.is_symlink() and os.readlink(source) == str(target)
    assert value.stat().st_ino == (target / "ledger.json").stat().st_ino == info.st_ino
    assert (target / "head").read_bytes() == value.read_bytes()
    assert canonical_service_path(value, root) == target / "ledger.json"
    assert service_path("ledger.json", root=root) == target / "ledger.json"
    assert organize(root, ["live"], apply=True)["rows"][0]["state"] == "already_placed"
    assert applied["source_files_deleted"] == applied["payload_bytes_copied"] == 0


def test_no_empty_second_ledger_before_cutover_or_on_collision(tmp_path):
    root = checkout(tmp_path)
    assert service_path("ledger", root=root) == root / "artifacts/live/ledger"
    (root / "service_data/live").mkdir(parents=True)
    with pytest.raises(ValueError, match="independent service"):
        service_path("ledger", root=root)
    with pytest.raises(FileExistsError, match="independent destination"):
        organize(root, ["live"], apply=True)
    assert not (root / "artifacts/live").is_symlink()


def test_new_node_uses_service_namespace_and_rejects_escape(tmp_path):
    assert service_path("new/ledger", root=tmp_path) == tmp_path / "service_data/live/new/ledger"
    for suffix in ("../ledger", "/absolute"):
        with pytest.raises(ValueError):
            service_path(suffix, root=tmp_path)


def test_remote_archive_guard_allows_only_registered_root_alias(tmp_path, monkeypatch):
    from stockagent import service_layout
    from stockagent.data_sync.remote_legacy_return import real
    from stockagent.data_sync.desync_snapshots import SnapshotError
    root = checkout(tmp_path)
    source = root / 'artifacts/live'
    value = source / 'ledger'
    value.write_bytes(b'original ledger')
    before = value.stat()
    organize(root, ['live'], apply=True)
    target = root / 'service_data/live'
    monkeypatch.setattr(service_layout, 'service_moves', lambda _root: {source: target})
    assert real(value) == target / 'ledger'
    assert real(value).stat().st_ino == before.st_ino
    (target / 'leaf-alias').symlink_to('ledger')
    with pytest.raises(SnapshotError, match='redirected'):
        real(source / 'leaf-alias')
    unrelated = root / 'unregistered'
    unrelated.symlink_to(target)
    with pytest.raises(SnapshotError, match='redirected'):
        real(unrelated / 'ledger')


def test_redirected_compatibility_alias_and_escaping_relative_link_refused(tmp_path):
    root = checkout(tmp_path)
    source = root / "artifacts/live"
    (source / "external").symlink_to("../other")
    with pytest.raises(ValueError, match="relative link escapes"):
        audit_tree(source)
    (source / "external").unlink()
    source.rmdir()
    outside = root / "other"
    outside.mkdir()
    source.symlink_to(outside)
    with pytest.raises(ValueError, match="unregistered"):
        canonical_service_path(source / "ledger", root)


def test_active_control_owner_is_deferred_without_audit_or_mutation(tmp_path, monkeypatch):
    root = checkout(tmp_path)
    source = root / "artifacts/operations"
    source.mkdir()
    from stockagent.data_sync import storage_monitor
    monkeypatch.setattr(storage_monitor, "local_census_priority_reason", lambda *_: "dataset_version_owner_active")
    value = organize(root, ["operations"], apply=True)
    assert value["rows"][0]["state"] == "deferred"
    assert value["rows"][0]["reason"] == "dataset_version_owner_active"
    assert source.is_dir() and not source.is_symlink()


def test_busy_retirement_owner_does_not_block_service_start(tmp_path, monkeypatch, capsys):
    import fcntl
    import json
    import sys
    import scripts.organize_service_storage as placement
    lock = tmp_path / 'original-owner.lock'
    with lock.open('w') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_open = os.open
        def local_open(path, flags, mode=0o777):
            assert str(path) == '/run/lock/stockagent-wsl-retirement-wave-scheduler.lock'
            return original_open(lock, flags, mode)
        monkeypatch.setattr(placement.os, 'open', local_open)
        monkeypatch.setattr(sys, 'argv', ['organize_service_storage.py', '--apply', '--boot-handoff'])
        monkeypatch.setattr(placement, 'organize', lambda *_args, **_kwargs: pytest.fail('busy owner was bypassed'))
        placement.main()
    result = json.loads(capsys.readouterr().out)
    assert result['state'] == 'deferred'
    assert result['reason'] == 'original_retirement_owner_active'
    assert result['source_files_deleted'] == result['payload_bytes_copied'] == 0


def test_unavailable_native_bind_rolls_back_exact_original_names(tmp_path, monkeypatch):
    import subprocess
    import scripts.organize_service_storage as placement
    root = checkout(tmp_path)
    source = root / "artifacts/operations"
    source.mkdir()
    ledger = source / "original.json"
    ledger.write_bytes(b'original control journal')
    before = ledger.stat()
    monkeypatch.setattr(placement, "owner_gate", lambda *_args, **_kwargs: None)
    def unavailable(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, ["mount"])
    monkeypatch.setattr(placement.subprocess, "run", unavailable)
    value = organize(root, ["operations"], apply=True, boot_handoff=True)
    assert value["rows"][0]["state"] == "rolled_back_bind_unavailable"
    assert source.is_dir() and not source.is_symlink()
    assert ledger.read_bytes() == b'original control journal'
    assert ledger.stat().st_ino == before.st_ino
    assert not (root / "service_data/operations").exists()


def test_native_control_placement_needs_no_descendant_scan(tmp_path, monkeypatch):
    from scripts.organize_service_storage import native_control_audit
    source = tmp_path / "control"
    source.mkdir()
    (source / "relative").symlink_to("../other")
    (source / "journal").write_bytes(b'preserve by directory inode')
    monkeypatch.setattr(os, "scandir", lambda *_: (_ for _ in ()).throw(AssertionError("unexpected descendant scan")))
    value = native_control_audit(source)
    assert value["inode"] == source.stat().st_ino
    assert value["descendants_scanned"] is False and value["payload_recovery_claim"] is False


def test_startup_installer_preserves_existing_units_and_unrelated_dropins(tmp_path, monkeypatch):
    from scripts.install_service_storage_layout import install, UNIT
    root = checkout(tmp_path)
    template = Path(__file__).resolve().parents[1] / "deploy/systemd" / (UNIT + ".in")
    destination = root / "deploy/systemd" / template.name
    destination.parent.mkdir(parents=True)
    destination.write_bytes(template.read_bytes())
    units = root / "systemd"
    units.mkdir()
    (units / "stockagent-example.service").write_text("[Service]\nExecStart=/bin/true\n")
    dropins = units / "stockagent-example.service.d"
    dropins.mkdir()
    (dropins / "other.conf").write_text("[Service]\nNice=-1\n")
    calls = []
    import scripts.install_service_storage_layout as installer
    monkeypatch.setattr(installer.subprocess, "run", lambda args, **_: calls.append(args))
    assert install(root, units)["ordered_units"] == 1
    assert not (units / UNIT).exists()
    install(root, units, apply=True)
    assert (dropins / "other.conf").read_text() == "[Service]\nNice=-1\n"
    assert (units / "stockagent-example.service").read_text() == "[Service]\nExecStart=/bin/true\n"
    assert all("restart" not in call for call in calls)
    install(root, units, apply=True)  # no duplicate or conflicting unit


def enrolled_control(root):
    import json
    source = root / "artifacts/operations"
    source.mkdir()
    target = root / "service_data/operations"
    target.mkdir(parents=True)
    slots = root / "service_data/.compatibility"
    slots.mkdir()
    slot = slots / "operations"
    slot.symlink_to(target)
    row = {"source": str(source), "target": str(target), "area": "operations",
           "audit": {"device": target.stat().st_dev, "inode": target.stat().st_ino},
           "binding": {"slot": str(slot), "mountpoint_inode": source.stat().st_ino}}
    (root / "service_data/.placement-operations.json").write_text(json.dumps(row))
    return source, target, row


def test_enrolled_replay_ignores_unrelated_target_readers(tmp_path, monkeypatch):
    import scripts.organize_service_storage as placement
    from stockagent.data_sync import materialized_cache
    root = checkout(tmp_path)
    source, target, row = enrolled_control(root)
    monkeypatch.setattr(placement, "owner_gate", lambda *_a, **_k: pytest.fail("initial-move gate used"))
    observed = []
    monkeypatch.setattr(materialized_cache, "process_references_many",
                        lambda roots, **_: observed.append(roots) or [])
    value = organize(root, ["operations"], boot_handoff=True)
    assert value["rows"][0]["state"] == "planned_bind_restore"
    assert observed == [[source]] and target.stat().st_ino == row["audit"]["inode"]
    assert placement.required_bind_failures(root, ["operations"]) == ["operations"]


def test_enrolled_mountpoint_reader_and_shadow_payload_block_replay(tmp_path, monkeypatch):
    import scripts.organize_service_storage as placement
    from stockagent.data_sync import materialized_cache
    root = checkout(tmp_path)
    source, target, row = enrolled_control(root)
    monkeypatch.setattr(materialized_cache, "process_references_many", lambda *_a, **_k: [{"pid": 123}])
    value = organize(root, ["operations"], boot_handoff=True)
    assert value["rows"][0]["reason"] == "control_mountpoint_process_reference"
    (source / "new-control.json").write_bytes(b"preserve interrupted-boot evidence")
    value = organize(root, ["operations"], apply=True, boot_handoff=True)
    assert value["rows"][0]["reason"] == "control_mountpoint_changed_requires_recovery"
    assert (source / "new-control.json").read_bytes() == b"preserve interrupted-boot evidence"
    assert placement.required_bind_failures(root, ["operations"]) == ["operations"]


def test_already_bound_identity_needs_no_owner_gate_but_rejects_wrong_target(tmp_path, monkeypatch):
    import scripts.organize_service_storage as placement
    root = checkout(tmp_path)
    source, target, row = enrolled_control(root)
    monkeypatch.setattr(placement, "owner_gate", lambda *_a, **_k: pytest.fail("already bound owner gate"))
    assert placement.enrolled_bind_gate(target, target, row, root, boot_handoff=True) is None
    wrong = {**row, "audit": {"inode": source.stat().st_ino}}
    with pytest.raises(ValueError, match="target identity"):
        placement.enrolled_bind_gate(target, target, wrong, root, boot_handoff=True)


def test_busy_retirement_cannot_report_success_for_unrestored_control(tmp_path, monkeypatch, capsys):
    import fcntl
    import json
    import sys
    import scripts.organize_service_storage as placement
    root = checkout(tmp_path)
    enrolled_control(root)
    lock = tmp_path / "owner.lock"
    with lock.open("w") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_open = os.open
        monkeypatch.setattr(placement.os, "open", lambda _path, flags, mode=0o777: original_open(lock, flags, mode))
        monkeypatch.setattr(sys, "argv", ["organize_service_storage.py", "--root", str(root),
                                        "--areas", "operations", "--apply", "--boot-handoff"])
        assert placement.main() == 75
    value = json.loads(capsys.readouterr().out)
    assert value["startup_control_paths_ready"] is False
    assert value["required_bind_failures"] == ["operations"]
    assert value["source_files_deleted"] == 0


def test_installer_upgrades_only_exact_previous_managed_unit(tmp_path, monkeypatch):
    from scripts.install_service_storage_layout import install, UNIT
    import scripts.install_service_storage_layout as installer
    root = checkout(tmp_path)
    template = Path(__file__).resolve().parents[1] / "deploy/systemd" / (UNIT + ".in")
    destination = root / "deploy/systemd" / template.name
    destination.parent.mkdir(parents=True)
    destination.write_bytes(template.read_bytes())
    units = root / "systemd"
    units.mkdir()
    current = template.read_text().replace("@REPO_ROOT@", str(root)).encode()
    prior = current.replace(b"StartLimitIntervalSec=0\n", b"").replace(b"Restart=on-failure\nRestartSec=5s\n", b"")
    (units / UNIT).write_bytes(prior)
    calls = []
    monkeypatch.setattr(installer.subprocess, "run", lambda args, **_: calls.append(args))
    install(root, units, apply=True)
    assert (units / UNIT).read_bytes() == current
    assert all("restart" not in call for call in calls)
    (units / UNIT).write_bytes(prior + b"# unrelated local policy\n")
    with pytest.raises(ValueError, match="unrelated"):
        install(root, units, apply=True)


def test_private_reader_namespace_waits_for_enrolled_data_mount_attempts(tmp_path, monkeypatch):
    from scripts.install_service_storage_layout import install, UNIT, DATA_MOUNTS
    import scripts.install_service_storage_layout as installer
    root = checkout(tmp_path)
    template = Path(__file__).resolve().parents[1] / "deploy/systemd" / (UNIT + ".in")
    destination = root / "deploy/systemd" / template.name
    destination.parent.mkdir(parents=True)
    destination.write_bytes(template.read_bytes())
    units = root / "systemd"
    units.mkdir()
    reader = "stockagent-tw-day-trade-simulation.service"
    (units / reader).write_text("[Service]\nPrivateMounts=yes\nExecStart=/bin/true\n")
    calls = []
    monkeypatch.setattr(installer.subprocess, "run", lambda args, **_: calls.append(args))
    assert install(root, units)["data_readers_ordered"] == 0
    for name in DATA_MOUNTS:
        (units / name).touch()
    value = install(root, units, apply=True)
    ordering = (units / (reader + ".d") / "35-service-data-volume-order.conf").read_text()
    assert value["data_readers_ordered"] == 1
    assert "Wants=" in ordering and "After=" in ordering
    assert all(name in ordering for name in DATA_MOUNTS)
    # Broker quotes and the accepted C-backed service cache remain available
    # if an E mount attempt fails. A missing E volume is not a source fallback.
    assert "Requires=" not in ordering and "RequiresMountsFor=" not in ordering
    assert all("restart" not in call for call in calls)
