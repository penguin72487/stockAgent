import fcntl
import hashlib
import json
from pathlib import Path

import pytest

from scripts import install_isolated_edge_reader as installer


def digest(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def release_case(tmp_path, monkeypatch):
    source, repo, state = (tmp_path/name for name in ('source', 'repo', 'state'))
    source.mkdir()
    (repo/'configs').mkdir(parents=True)
    (repo/'artifacts/code_releases').mkdir(parents=True)
    (repo/'scripts').mkdir()
    state.mkdir()
    (state/'state.json').write_text('{}')
    (state/'operation.lock').touch()
    baseline = b'#!/usr/bin/env bash\nexit 0\n'
    (repo/'scripts/run_data_cache.sh').write_bytes(baseline)
    (repo/'scripts/run_data_cache.sh').chmod(0o764)
    (repo/'scripts/runtime_env.sh').write_text('run_fintech_python() { python3 "$@"; }\n')
    dataset_sha = 'a'*64
    reader = ('import json\nprint(json.dumps({'
              '"mode":"index-only-reader-status","dataset":"fixture",'
              '"schema_version":3,"manifest_sha256":"'+dataset_sha+'",'
              '"payload_fetched":False,"payload_verified":False}))\n').encode()
    members = {'scripts/manage_packed_edge.py': reader, 'scripts/verify_isolated_edge_reader.py': b'pass\n',
               'stockagent/data_sync/packed_snapshots.py': b'pass\n',
               'stockagent/data_sync/packed_alias_v3.py': b'pass\n',
               'stockagent/data_sync/packed_edge_cache.py': b'pass\n'}
    for name, body in members.items():
        path = source/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    (source/'vendor/ijson').mkdir(parents=True)
    (source/'vendor/ijson/__init__.py').write_text('version = "fixture"\n')
    hashes = {name: digest(body) for name, body in members.items()}
    (source/'edge-source-sha.json').write_text(json.dumps(hashes))
    identity = digest(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode())
    monkeypatch.setattr(installer, '_smoke', lambda *_args: {'state': 'passed'})
    staged = installer.stage(source, repo, identity)
    release = Path(staged['release_root'])
    wrapper_source = tmp_path/'new-wrapper.sh'
    wrapper_source.write_bytes((Path(__file__).resolve().parents[1]/'scripts/run_data_cache.sh').read_bytes())
    return {'source': source, 'repo': repo, 'state': state, 'identity': identity,
            'release': release, 'manifest': staged['manifest_sha256'],
            'wrapper_source': wrapper_source, 'wrapper_sha': digest(wrapper_source.read_bytes()),
            'baseline': baseline, 'dataset_sha': dataset_sha}


def activate(case, *, dataset_sha=None):
    return installer.activate(case['repo'], case['state'], case['release'], case['manifest'],
                              case['wrapper_source'], case['wrapper_sha'], digest(case['baseline']),
                              dataset='fixture', expected_dataset_sha=dataset_sha or case['dataset_sha'])


def test_release_is_versioned_pinned_and_idempotent(release_case):
    case = release_case
    again = installer.stage(case['source'], case['repo'], case['identity'])
    assert Path(again['release_root']) == case['release'] and again['pointer_changed'] is False
    (case['release']/'scripts/manage_packed_edge.py').write_text('tampered\n')
    with pytest.raises(ValueError, match='member differs'):
        installer.stage(case['source'], case['repo'], case['identity'])
    assert not (case['state']/'reader-release.control').exists()


def test_activation_uses_canonical_wrapper_and_existing_edge_owner(release_case):
    case = release_case
    result = activate(case)
    assert result['state'] == 'active' and result['reader_status']['schema_version'] == 3
    assert result['payload_fetched'] is False
    control = case['state']/'reader-release.control'
    assert control.stat().st_mode & 0o777 == 0o600
    assert str(case['release']) in control.read_text()
    backup = case['state']/'reader-release-history'/(digest(case['baseline'])+'.sh')
    assert backup.read_bytes() == case['baseline']
    assert activate(case)['state'] == 'active'


def test_failed_reader_acceptance_rolls_back_pointer_and_preserves_original_wrapper(release_case):
    case = release_case
    with pytest.raises(ValueError, match='status differs'):
        activate(case, dataset_sha='b'*64)
    assert not (case['state']/'reader-release.control').exists()
    backup = case['state']/'reader-release-history'/(digest(case['baseline'])+'.sh')
    assert backup.read_bytes() == case['baseline']


def test_busy_edge_owner_defers_without_switching(release_case):
    case = release_case
    with (case['state']/'operation.lock').open('r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            activate(case)
    assert not (case['state']/'reader-release.control').exists()
    assert (case['repo']/'scripts/run_data_cache.sh').read_bytes() == case['baseline']
