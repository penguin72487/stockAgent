"""A stop may detach only its enrolled mount, without forcing busy readers."""
import os
from pathlib import Path
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
    result = subprocess.run(['bash', str(ROOT / 'scripts' / name), '--unmount'], env=env, text=True, capture_output=True)
    assert result.returncode == (2 if mounted else 0)
    assert not trace.exists()
