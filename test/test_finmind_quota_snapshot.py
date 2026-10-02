import json
from pathlib import Path

import pytest

from scripts import snapshot_finmind_quota as sampler


@pytest.mark.parametrize('message,code', [
    ('priority_override_query_grain_unverified', 'priority_override_query_grain_unverified'),
    ('private-test-token https://private.example/raw', 'eta_snapshot_failed'),
])
def test_estimator_failure_has_separate_sanitized_health_and_keeps_old_evidence(tmp_path, monkeypatch, capsys,
                                                                            message, code):
    previous = b'{"previous":"evidence"}'
    (tmp_path / 'eta_status.json').write_bytes(previous)

    def fail(_root):
        raise ValueError(message)

    monkeypatch.setattr(sampler, 'snapshot_finmind_estimate', fail)
    assert sampler.sample_local_eta(tmp_path) is None
    health = json.loads((tmp_path / 'eta_refresh_status.json').read_text())
    assert health['state'] == 'failed'
    assert health['error_code'] == code
    assert health['exception_type'] == 'ValueError'
    assert health['elapsed_seconds'] >= 0
    assert 'private-test-token' not in json.dumps(health) + capsys.readouterr().out
    assert (tmp_path / 'eta_status.json').read_bytes() == previous


def test_success_replaces_failure_health_and_bootstraps_without_existing_estimate(tmp_path, monkeypatch):
    (tmp_path / 'eta_refresh_status.json').write_text('{"state":"failed"}')
    estimate = {'state': 'conditional', 'observed_at_utc': '2026-10-01T08:00:00+00:00'}
    monkeypatch.setattr(sampler, 'snapshot_finmind_estimate', lambda _root: estimate)
    assert sampler.sample_local_eta(tmp_path) == estimate
    health = json.loads((tmp_path / 'eta_refresh_status.json').read_text())
    assert health['state'] == 'ok' and health['error_code'] is None
    assert health['estimate_observed_at_utc'] == estimate['observed_at_utc']


def test_normal_sampler_still_refreshes_eta_if_previous_file_missing(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(sampler, '__file__', str(tmp_path / 'scripts' / 'snapshot_finmind_quota.py'))
    monkeypatch.setattr(sampler, 'load_env_file', lambda *_a, **_kw: None)
    monkeypatch.setattr(sampler, 'verified_account', lambda *_a: {
        'tier': 'Sponsor', 'provider_used_in_hour': 100, 'official_requests_per_hour': 6000})
    monkeypatch.setattr(sampler, 'sample_local_eta', lambda root: calls.append(root))
    monkeypatch.setenv('FINMIND_TOKEN', 'private-test-token')
    monkeypatch.setattr('sys.argv', ['snapshot_finmind_quota'])
    assert sampler.main() == 0
    assert calls == [tmp_path / 'data_finmind']
