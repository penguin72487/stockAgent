"""A work claim cannot change source identity, bypass dependencies or revive an expired owner."""

from dataclasses import replace

import pytest

from stockagent.control.contracts import WorkSpec


def spec(**kwargs):
    return WorkSpec('release-check', {'receipt_sha256': 'a'*64, 'source_sha256': 'b'*64}, **kwargs)


def test_identity_retains_caller_values_and_is_immutable():
    inputs = {'receipt_sha256': 'a'*64, 'source_sha256': 'b'*64}
    work = WorkSpec('release-check', inputs)
    before = work.identity_sha256
    inputs['source_sha256'] = 'c'*64
    assert work.identity_sha256 == before
    with pytest.raises(TypeError):
        work.inputs['source_sha256'] = 'd'*64
    output = work.as_dict()
    output['inputs']['source_sha256'] = 'e'*64
    assert work.identity_sha256 == before
    assert WorkSpec.from_dict(work.as_dict()) == work


@pytest.mark.parametrize('change', [
    {'priority': True}, {'priority': -1}, {'priority': 101},
    {'max_attempts': 0}, {'max_attempts': 11}, {'cpu_slots': 0},
    {'memory_bytes': 0}, {'scratch_bytes': -1}, {'kind': 'run-arbitrary-command'},
    {'dependencies': ('release-check',)}, {'dependencies': ('same','same')},
])
def test_invalid_work_is_rejected_before_queue_access(change):
    with pytest.raises(ValueError): spec(**change)


@pytest.mark.parametrize('inputs', [
    {'receipt_sha256': 'a'*64},
    {'receipt_sha256': 'a'*64, 'source_sha256': 'B'*64},
    {'receipt_sha256': 'a'*64, 'source_sha256': 'b'*64, 'command': 'train.py'},
])
def test_only_exact_allowlisted_inputs_are_work(inputs):
    with pytest.raises(ValueError): WorkSpec('release-check', inputs)


def test_source_resource_or_retry_change_requires_new_identity():
    original = spec()
    assert replace(original, inputs={'receipt_sha256':'a'*64,'source_sha256':'c'*64}).identity_sha256 != original.identity_sha256
    assert replace(original, cpu_slots=2).identity_sha256 != original.identity_sha256
    assert replace(original, max_attempts=3).identity_sha256 != original.identity_sha256


@pytest.mark.parametrize('value', [True, 2])
def test_schema_cannot_be_bool_or_unknown(value):
    body = spec().as_dict(); body['schema_version'] = value
    with pytest.raises(ValueError): WorkSpec.from_dict(body)
