import hashlib
from pathlib import Path

import pytest

from stockagent.data_sync import edge_reader_recovery_probe as probe


@pytest.fixture
def case(tmp_path):
    repo, state = tmp_path/'repo', tmp_path/'state'
    for path in (repo/'configs', repo/'scripts', repo/'artifacts/code_releases', state):
        path.mkdir(parents=True)
    (state/'state.json').write_text('{}')
    (state/'operation.lock').touch()
    wrapper = b'old wrapper\n'
    (repo/'scripts/run_data_cache.sh').write_bytes(wrapper)
    request = {'repo': str(repo), 'baseline_wrapper_sha256': hashlib.sha256(wrapper).hexdigest(),
               'wrapper_sha256': 'b'*64, 'reader_manifest_sha256': 'c'*64}
    return repo, state, request


def test_missing_control_with_enrolled_owner_is_eligible(case):
    _repo, state, request = case
    result = probe.inspect(request, state_root=state)
    assert result['state'] == 'eligible' and result['control_present'] is False


def test_matching_existing_control_can_rebuild_an_absent_release(case):
    repo, state, request = case
    path = state/'reader-release.control'
    path.write_text(f"{repo}/artifacts/code_releases/stockagent-edge-reader-v3-{'c'*16} {'c'*64} {repo}\n")
    path.chmod(0o600)
    assert probe.inspect(request, state_root=state)['control_present'] is True


def test_matching_intact_release_waits_for_runtime_instead_of_reinstallation(case):
    repo, state, request = case
    body = b'fixed reader\n'
    manifest = f'{hashlib.sha256(body).hexdigest()}  scripts/reader.py\n'.encode()
    request['reader_manifest_sha256'] = hashlib.sha256(manifest).hexdigest()
    release = repo/'artifacts/code_releases'/('stockagent-edge-reader-v3-'+request['reader_manifest_sha256'][:16])
    release.mkdir(mode=0o700); (release/'scripts').mkdir()
    (release/'scripts/reader.py').write_bytes(body)
    (release/'reader-manifest.sha256').write_bytes(manifest)
    (release/'configs').symlink_to(repo/'configs')
    control = state/'reader-release.control'
    control.write_text(f"{release} {request['reader_manifest_sha256']} {repo}\n"); control.chmod(0o600)
    assert probe.inspect(request, state_root=state)['state'] == 'reader_intact'
    (release/'scripts/reader.py').write_text('changed\n')
    assert probe.inspect(request, state_root=state)['state'] == 'release_mismatch'


def test_different_control_and_unapproved_wrapper_are_not_replaced(case):
    repo, state, request = case
    path = state/'reader-release.control'; path.write_text('another release\n'); path.chmod(0o600)
    assert probe.inspect(request, state_root=state)['state'] == 'control_mismatch'
    path.unlink(); (repo/'scripts/run_data_cache.sh').write_text('custom wrapper\n')
    assert probe.inspect(request, state_root=state)['state'] == 'wrapper_mismatch'


def test_missing_original_edge_owner_is_not_created(case):
    _repo, state, request = case
    (state/'operation.lock').unlink()
    assert probe.inspect(request, state_root=state)['state'] == 'base_not_enrolled'
    assert not (state/'operation.lock').exists()


def test_redirected_control_is_not_replaced(case):
    _repo, state, request = case
    (state/'reader-release.control').symlink_to(state/'state.json')
    assert probe.inspect(request, state_root=state)['state'] == 'control_mismatch'
