import json

import pytest

from downloader.artifact_io import sha256_file
from downloader.tej_capture_claims import (
    find_capture_claim, require_reconciled_failures, verified_unsent_failure,
)
from downloader.tej_history import CONTRACT_VERSION, DesktopBridge


def test_only_exact_intent_is_selected_among_retained_attempts(tmp_path):
    task = 'a' * 24
    old = tmp_path / (task + '-' + 'b' * 32 + '.json')
    current = tmp_path / (task + '-' + 'c' * 32 + '.json')
    old.write_text('previous intent')
    current.write_text('current intent')
    assert find_capture_claim(tmp_path, task, sha256_file(current)) == current
    assert find_capture_claim(tmp_path, task, 'f' * 64) is None
    assert old.read_text() == 'previous intent'


def test_duplicate_or_invalid_claim_is_not_accepted(tmp_path):
    task = 'a' * 24
    first = tmp_path / (task + '-' + 'b' * 32 + '.json')
    second = tmp_path / (task + '-' + 'c' * 32 + '.json')
    first.write_text('same intent')
    second.write_text('same intent')
    with pytest.raises(ValueError, match='multiple claims'):
        find_capture_claim(tmp_path, task, sha256_file(first))
    second.unlink()
    second.symlink_to(first)
    with pytest.raises(ValueError, match='Invalid retained'):
        find_capture_claim(tmp_path, task, sha256_file(first))


def test_current_attempt_cannot_consume_a_different_intent(tmp_path):
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    (tmp_path / (attempt + '.json')).write_text('different')
    with pytest.raises(ValueError, match='different intent'):
        find_capture_claim(tmp_path, task, 'f' * 64, attempt)


@pytest.fixture(params=['finite', 'large'])
def previous_failure(tmp_path, monkeypatch, request):
    family = request.param
    root = tmp_path / 'owner'
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    folder, contract, suffix = {
        'finite': ('finite_axis_capture_intents', 'tej_finite_company_date_axis_capture_once_v1',
                   '.finite-axis.json'),
        'large': ('large_axis_capture_intents', 'tej_large_company_axis_capture_once_v1',
                  '.company-axis.snapshot.json'),
    }[family]
    for name in ('raw', 'requests', folder + '/claimed'):
        (root / name).mkdir(parents=True)
    native = {'task_id': task, 'action': 'plan', 'type': 'Option (ALL)',
              'smart_id': 'TEJ Derivatives', 'table': 'source'}
    request_path = root / 'requests' / (attempt + '.json')
    request_path.write_text(json.dumps(native))
    intent = {'contract': contract, **{k: native[k] for k in ('task_id', 'type', 'smart_id', 'table')},
              'prepared_request_sha256': sha256_file(request_path), 'bridge_script_sha256': 'c' * 64,
              'market_data_query_authorized': False, 'provider_queries_by_registration': 0}
    claim = root / folder / 'claimed' / (attempt + '.json')
    claim.write_text(json.dumps(intent))
    outcome = {**native, 'contract_version': CONTRACT_VERSION, 'provider': 'tej_smart_wizard',
               'market_data_query_submission_possible': False,
               'error_code': 'metadata_preparation_failed_before_preview'}
    outcome_path = root / 'raw' / (attempt + '.json.outcome.json')
    monkeypatch.setattr(DesktopBridge, 'bound_script_release', lambda *args: (None, 'c' * 64))
    return root, task, claim, family, outcome_path, outcome, suffix


def test_only_verified_unsent_failure_can_be_retried(previous_failure):
    root, task, claim, family, outcome_path, outcome, suffix = previous_failure
    assert verified_unsent_failure(root, claim, family) is None
    with pytest.raises(ValueError, match='unknown'):
        require_reconciled_failures(root, claim.parent, task, family)
    outcome_path.write_text(json.dumps(outcome))
    assert verified_unsent_failure(root, claim, family) == outcome_path
    require_reconciled_failures(root, claim.parent, task, family)
    assert claim.is_file()  # Original claim remains in its provenance path.
    outcome['market_data_query_submission_possible'] = True
    outcome_path.write_text(json.dumps(outcome))
    with pytest.raises(ValueError, match='pre-Preview'):
        require_reconciled_failures(root, claim.parent, task, family)


def test_successful_or_changed_prior_capture_cannot_be_retried(previous_failure):
    root, task, claim, family, outcome_path, outcome, suffix = previous_failure
    outcome_path.write_text(json.dumps(outcome))
    success = root / 'raw' / (claim.stem + '.json' + suffix)
    success.write_text('{}')
    with pytest.raises(ValueError, match='readback and reuse'):
        require_reconciled_failures(root, claim.parent, task, family)
    success.unlink()
    intent = json.loads(claim.read_text())
    intent['prepared_request_sha256'] = 'f' * 64
    claim.write_text(json.dumps(intent))
    with pytest.raises(ValueError, match='pre-Preview'):
        require_reconciled_failures(root, claim.parent, task, family)
