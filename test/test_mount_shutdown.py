"""A stop may detach only its enrolled mount, without forcing busy readers."""
import os
from pathlib import Path
import shlex
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('name', ['mount_packed_d_cold.sh', 'mount_backup_transport.sh'])
@pytest.mark.parametrize('mounted', [True, False])
def test_unrelated_mount_is_never_detached(tmp_path, name, mounted):
    findmnt = tmp_path / 'findmnt'
    findmnt.write_text(
        '#!/usr/bin/env bash\n'
        'case "$3" in\n'
        'TARGET) if [[ "$TEST_MOUNT_PRESENT" == yes ]]; then printf "%s\\n" "$5"; else printf "/\\n"; fi ;;\n'
        'SOURCE) printf "C:\\n" ;;\n'
        'FSTYPE) printf "9p\\n" ;;\n'
        'esac\n'
    )
    findmnt.chmod(0o755)
    trace = tmp_path / 'detach-calls'
    umount = tmp_path / 'umount'
    umount.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$TEST_MOUNT_TRACE"\nexit 99\n')
    umount.chmod(0o755)
    env = {**os.environ, 'PATH': str(tmp_path) + ':' + os.environ['PATH'],
           'TEST_MOUNT_PRESENT': 'yes' if mounted else 'no', 'TEST_MOUNT_TRACE': str(trace)}
    script = ROOT / 'scripts' / name
    if name == 'mount_backup_transport.sh':
        # Exercise the legacy D branch independently of the machine's active
        # transport placement, which may already have moved to E.
        (tmp_path/'configs/data_sync').mkdir(parents=True)
        (tmp_path/'configs/data_sync/backup_transport_volume.json').write_text('{"physical_volume":"D:"}')
        source = script.read_text().replace(
            'backup_mount_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
            'backup_mount_repo='+shlex.quote(str(tmp_path)), 1)
        script = tmp_path/name
        script.write_text(source)
    result = subprocess.run(['bash', str(script), '--unmount'], env=env, text=True, capture_output=True)
    assert result.returncode == (2 if mounted else 0)
    assert not trace.exists()


@pytest.mark.parametrize('valid', [False, True])
def test_e_transport_unmount_requires_current_owner_and_volume_check(tmp_path, monkeypatch, valid):
    import sys
    from scripts import manage_backup_transport_mount as manager
    root, alias = tmp_path/'e-transport', tmp_path/'alias'
    root.mkdir()
    alias.mkdir()
    (root/'.transport-owner.json').write_bytes(b'expected owner')
    monkeypatch.setattr(manager, 'backing', lambda: root)
    monkeypatch.setattr(manager, 'placement', lambda: ({}, b'expected owner'))
    monkeypatch.setattr(manager, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(manager, 'CANONICAL', alias)
    monkeypatch.setattr(Path, 'is_mount', lambda path: path == alias)
    monkeypatch.setattr(sys, 'argv', ['manage_backup_transport_mount.py', '--unmount'])
    def validate(**_kwargs):
        if not valid:
            raise ValueError('foreign or redirected transport')
    monkeypatch.setattr(manager, 'check_transport', validate)
    calls = []
    monkeypatch.setattr(manager.subprocess, 'run', lambda command, **_kwargs: calls.append(command))
    if valid:
        manager.main()
        assert calls == [['umount', str(alias)]]
    else:
        with pytest.raises(ValueError, match='foreign or redirected'):
            manager.main()
        assert not calls
