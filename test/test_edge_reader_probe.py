import hashlib
import json
from types import SimpleNamespace

import pytest

from stockagent.data_sync import edge_reader_probe as probe


def test_probe_binds_control_code_and_metadata_without_claiming_payload(tmp_path):
    repo = tmp_path/'repo'
    (repo/'scripts').mkdir(parents=True)
    wrapper = repo/'scripts/run_data_cache.sh'
    wrapper.write_bytes(b'fixed wrapper')
    manifest = b'fixed release manifest'
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    release = repo/'artifacts/code_releases'/('stockagent-edge-reader-v3-'+manifest_sha[:16])
    release.mkdir(parents=True)
    (release/'reader-manifest.sha256').write_bytes(manifest)
    control = tmp_path/'control'
    control.write_text(f'{release} {manifest_sha} {repo}\n')
    control.chmod(0o600)
    request = {'repo': str(repo), 'manifest_sha256': manifest_sha,
               'wrapper_sha256': hashlib.sha256(wrapper.read_bytes()).hexdigest(), 'dataset': 'fixture'}
    calls = []
    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout=json.dumps({'mode': 'index-only-reader-status', 'dataset': 'fixture',
            'schema_version': 3, 'manifest_sha256': 'a'*64, 'payload_fetched': False, 'payload_verified': False}))
    result = probe.probe(request, control_path=control, runner=runner)
    assert result['state'] == 'ready' and result['payload_verified'] is False
    assert calls[0][0] == ['bash', str(wrapper), 'reader-status', 'fixture']
    control.chmod(0o644)
    with pytest.raises(ValueError, match='permissions'):
        probe.probe(request, control_path=control, runner=runner)
    assert len(calls) == 1


def test_probe_rejects_a_schema_two_status(tmp_path):
    repo = tmp_path/'repo'
    (repo/'scripts').mkdir(parents=True)
    wrapper = repo/'scripts/run_data_cache.sh'
    wrapper.write_bytes(b'wrapper')
    manifest_sha = hashlib.sha256(b'manifest').hexdigest()
    release = repo/'artifacts/code_releases'/('stockagent-edge-reader-v3-'+manifest_sha[:16])
    release.mkdir(parents=True)
    (release/'reader-manifest.sha256').write_bytes(b'manifest')
    control = tmp_path/'control'
    control.write_text(f'{release} {manifest_sha} {repo}\n')
    control.chmod(0o600)
    request = {'repo': str(repo), 'manifest_sha256': manifest_sha,
               'wrapper_sha256': hashlib.sha256(wrapper.read_bytes()).hexdigest(), 'dataset': 'fixture'}
    def runner(*_args, **_kwargs):
        return SimpleNamespace(stdout=json.dumps({'mode': 'index-only-reader-status', 'dataset': 'fixture',
            'schema_version': 2, 'manifest_sha256': 'a'*64, 'payload_fetched': False, 'payload_verified': False}))
    with pytest.raises(ValueError, match='metadata status'):
        probe.probe(request, control_path=control, runner=runner)
