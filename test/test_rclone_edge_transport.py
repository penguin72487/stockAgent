"""Real fixed remote verifier: full SHA, atomic promotion and demand fences."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from stockagent.data_sync.rclone_edge_transport import REMOTE_ADAPTER


def run(payload):
    return subprocess.run([sys.executable, '-c', REMOTE_ADAPTER], input=json.dumps(payload),
                          text=True, capture_output=True, timeout=10)


@pytest.fixture
def remote(tmp_path):
    root, state = tmp_path / 'packed', tmp_path / 'state'
    root.mkdir(); state.mkdir()
    (state / 'state.json').write_text('{"schema_version":1,"mode":"index-only"}')
    (root / '.stignore-edge').write_text('exact demand')
    raw = b'actual source object bytes'
    sha = hashlib.sha256(raw).hexdigest()
    relative = 'objects/blobs/' + sha[:2] + '/' + sha + '.blob'
    base = {'remote_root': str(root), 'remote_state_root': str(state),
            'state_sha256': hashlib.sha256((state / 'state.json').read_bytes()).hexdigest(),
            'ignore_sha256': hashlib.sha256((root / '.stignore-edge').read_bytes()).hexdigest(),
            'producer_device_id': 'penguin', 'receiver_device_id': 'vast',
            'files': {relative: {'sha256': sha, 'bytes': len(raw)}}, 'transfer_identity_sha256': 'a' * 64}
    return root, state, raw, relative, base


def test_real_prepare_verify_promote_and_cached_noop(remote):
    root, _, raw, relative, base = remote
    observed = json.loads(run({**base, 'mode': 'observe'}).stdout)
    assert observed['missing'] == [relative]
    prepared = run({**base, 'mode': 'prepare'})
    assert prepared.returncode == 0, prepared.stderr
    stage = Path(json.loads(prepared.stdout)['staging'])
    file = stage / relative
    file.parent.mkdir(parents=True);file.write_bytes(raw)
    result = run({**base, 'mode': 'finish'})
    assert result.returncode == 0, result.stderr
    proof = json.loads(result.stdout)
    assert (root / relative).read_bytes() == raw and (root / relative).stat().st_nlink == 1
    assert not stage.exists()
    cached = run({**base, 'mode': 'observe', 'cached': proof['files']})
    assert cached.returncode == 0 and json.loads(cached.stdout)['missing'] == []
    assert json.loads(cached.stdout)['files'] == proof['files']


@pytest.mark.parametrize('alteration', ['existing_corrupt', 'staging_corrupt', 'unknown', 'demand_changed'])
def test_bad_bytes_unknown_staging_and_changed_demand_preserve_final(remote, alteration):
    root, state, raw, relative, base = remote
    prepared = run({**base, 'mode': 'prepare'})
    stage = Path(json.loads(prepared.stdout)['staging'])
    file = stage / relative
    file.parent.mkdir(parents=True);file.write_bytes(raw)
    if alteration == 'existing_corrupt':
        final = root / relative
        final.parent.mkdir(parents=True);final.write_bytes(b'x' * len(raw))
    elif alteration == 'staging_corrupt':
        file.write_bytes(b'x' * len(raw))
    elif alteration == 'unknown':
        (stage / 'unique').write_text('preserved')
    else:
        (state / 'state.json').write_text('{"schema_version":1,"mode":"index-only","changed":true}')
    result = run({**base, 'mode': 'finish'})
    assert result.returncode != 0 and stage.exists()
    if alteration == 'existing_corrupt':
        assert (root / relative).read_bytes() == b'x' * len(raw)
    else:
        assert not (root / relative).exists()
