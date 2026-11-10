import json
import os
from pathlib import Path
import shlex
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def mounts(tmp_path):
    volume, canonical = tmp_path/'d-volume', tmp_path/'canonical'
    primary = volume/'stockagent-cold-primary/packed'
    for root in (primary, canonical):
        (root/'.local-state').mkdir(parents=True)
        (root/'.stockagent-d-primary').write_text(json.dumps({
            'schema_version': 1, 'volume_id': '9ba6ab87-3889-40e7-90e4-9597dc88abaf',
            'backing': 'drvfs_msize8192', 'authority_node_id': 'penguin', 'resilience': 'single_d_volume'}))
        (root/'.local-state/node-id').write_text('penguin\n')
    (primary/'.stfolder').mkdir()
    (primary/'.stignore').write_text('')
    (volume/'stockagent-cold-primary/volume.json').write_text(json.dumps({
        'schema_version': 1, 'volume_id': '9ba6ab87-3889-40e7-90e4-9597dc88abaf',
        'authority_node_id': 'penguin', 'purpose': 'stockagent-d-cold-primary'}))
    script = (ROOT/'scripts/mount_packed_d_cold.sh').read_text()
    script = script.replace('volume=/srv/stockagent-d-volume', 'volume='+shlex.quote(str(volume)))
    script = script.replace('canonical=/srv/stockagent-packed', 'canonical='+shlex.quote(str(canonical)))
    script_path = tmp_path/'mount.sh'
    script_path.write_text(script)
    findmnt = tmp_path/'findmnt'
    findmnt.write_text('''#!/usr/bin/env bash
case "$3" in
  TARGET) printf '%s\\n' "$5" ;;
  SOURCE) if [[ "$5" == "$TEST_CANONICAL" ]]; then printf '%s\\n' "$TEST_CANONICAL_SOURCE"; else printf 'D:\\n'; fi ;;
  FSTYPE) printf '9p\\n' ;;
  OPTIONS) if [[ "$5" == "$TEST_CANONICAL" ]]; then printf '%s\\n' "$TEST_CANONICAL_OPTIONS"; else printf '%s\\n' "$TEST_VOLUME_OPTIONS"; fi ;;
esac
''')
    findmnt.chmod(0o755)
    stat = tmp_path/'stat'
    stat.write_text('''#!/usr/bin/env bash
if [[ "$3" == "$TEST_CANONICAL" ]]; then printf '%s\\n' "$TEST_CANONICAL_INO"; else printf '42\\n'; fi
''')
    stat.chmod(0o755)
    env = {**os.environ, 'PATH': str(tmp_path)+':'+os.environ['PATH'], 'TEST_CANONICAL': str(canonical),
           'TEST_CANONICAL_SOURCE': 'D:[/stockagent-cold-primary/packed]',
           'TEST_CANONICAL_OPTIONS': 'rw,msize=8192', 'TEST_VOLUME_OPTIONS': 'rw,msize=8192',
           'TEST_CANONICAL_INO': '42'}
    def check(**changes):
        return subprocess.run(['bash', str(script_path), '--check'], env={**env, **changes},
                              text=True, capture_output=True)
    return check, canonical


def test_independent_d_mount_namespaces_allow_same_enrolled_inode(mounts):
    check, _ = mounts
    assert check().returncode == 0


@pytest.mark.parametrize('change', [
    {'TEST_CANONICAL_INO': '43'},
    {'TEST_CANONICAL_SOURCE': 'D:[/different]'},
    {'TEST_CANONICAL_SOURCE': 'C:[/stockagent-cold-primary/packed]'},
    {'TEST_CANONICAL_OPTIONS': 'rw,msize=65536'},
    {'TEST_VOLUME_OPTIONS': 'rw,msize=65536'},
])
def test_different_inode_source_or_request_size_is_rejected(mounts, change):
    check, _ = mounts
    assert check(**change).returncode != 0


def test_different_canonical_authority_marker_is_rejected(mounts):
    check, canonical = mounts
    (canonical/'.stockagent-d-primary').write_text('{}')
    assert check().returncode != 0
