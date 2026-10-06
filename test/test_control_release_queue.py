from __future__ import annotations

import json
from pathlib import Path

import pytest

from stockagent.control import release_queue as queue
from stockagent.control.contracts import WorkSpec


def write_private(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    return path


@pytest.fixture
def policy(tmp_path, monkeypatch):
    state = tmp_path / 'state'
    state.mkdir(mode=0o700)
    lock = write_private(tmp_path / 'runtime.json', {'sha256': 'accepted'})
    monkeypatch.setattr(queue, 'runtime_identity', lambda: {'sha256': 'accepted'})
    monkeypatch.setattr(queue, 'validate_runtime_lock', lambda expected, actual: [] if expected == actual else ['different'])
    value = {'schema_version': 1, 'node_id': 'node', 'state_root': str(state),
             'runtime_lock': str(lock), 'lease_seconds': 120, 'maximum_jobs_per_cycle': 8}
    path = write_private(tmp_path / 'policy.json', value)
    return path, value


def test_private_registry_rejects_redirects_public_permissions_and_duplicate_keys(tmp_path):
    path = write_private(tmp_path / 'binding.json', {'state': 'fixed'})
    alias = tmp_path / 'alias.json'
    alias.symlink_to(path)
    with pytest.raises(OSError): queue.private_json(alias)
    path.chmod(0o644)
    with pytest.raises(ValueError): queue.private_json(path)
    path.chmod(0o600)
    path.write_text('{"state":"one","state":"two"}')
    with pytest.raises(ValueError, match='duplicate'): queue.private_json(path)


def test_runtime_or_policy_change_rejected_before_work_access(policy):
    path, value = policy
    assert queue.load_policy(path) == value
    write_private(Path(value['runtime_lock']), {'sha256': 'changed'})
    with pytest.raises(ValueError, match='runtime differs'): queue.load_policy(path)
    write_private(Path(value['runtime_lock']), {'sha256': 'accepted'})
    write_private(path, {**value, 'command': 'arbitrary shell'})
    with pytest.raises(ValueError, match='unsupported'): queue.load_policy(path)


def test_owner_contention_defers_without_removing_lock_or_touching_bindings(policy):
    _, value = policy
    with queue.owner(value):
        with pytest.raises(BlockingIOError):
            with queue.owner(value): pytest.fail('duplicate owner admitted')
    assert (Path(value['state_root']) / 'owner.lock').is_file()


def test_binding_cannot_redirect_source_or_claim_different_receipt(tmp_path):
    source = tmp_path / 'frozen'
    source.mkdir()
    receipt = write_private(tmp_path / 'release.json', {})
    inputs = {'receipt_sha256': 'a' * 64, 'source_sha256': 'b' * 64}
    spec = WorkSpec('code-release-' + 'a' * 64, inputs)
    value = {'schema_version': 1, 'receipt': str(receipt), 'source_root': str(source), 'work_spec': spec.as_dict()}
    assert queue.binding_spec(value) == spec
    alias = tmp_path / 'redirect'
    alias.symlink_to(source)
    with pytest.raises(ValueError, match='redirected'): queue.binding_spec({**value, 'source_root': str(alias)})
    value['work_spec'] = WorkSpec('unrelated-key', inputs).as_dict()
    with pytest.raises(ValueError, match='exact immutable'): queue.binding_spec(value)


def test_changed_frozen_receipt_never_claims_or_consumes_attempt(policy, monkeypatch, tmp_path):
    _, value = policy
    source = tmp_path / 'frozen'
    source.mkdir()
    receipt = write_private(tmp_path / 'release.json', {'changed': True})
    spec = WorkSpec('code-release-' + 'a' * 64, {'receipt_sha256': 'a' * 64, 'source_sha256': 'b' * 64})
    write_private(Path(value['state_root']) / (spec.key + '.json'), {
        'schema_version': 1, 'receipt': str(receipt), 'source_root': str(source), 'work_spec': spec.as_dict()})
    monkeypatch.setattr(queue, 'ControlStore', lambda *args, **kwargs: pytest.fail('DB admission after receipt changed'))
    with pytest.raises(ValueError, match='receipt changed'): queue.cycle(value, 'unused-private-dsn')
