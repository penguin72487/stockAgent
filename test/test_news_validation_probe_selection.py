"""Clock/scope diagnostics use existing exact-head, read-only metadata grants."""
import pytest

import test_news_failure_probe_selection as existing
from test_news_fulltext import config


queue = existing.queue
CODES = ('invalid_bulk_publication_clock', 'article_rights_or_scope_excluded', 'article_license_not_verified')


@pytest.mark.parametrize('code', CODES)
def test_due_exact_validation_failure_is_read_only_not_recovery_or_body_grant(queue, code):
    existing.test_attribution_failure_requires_explicit_exact_code_and_keeps_original_head(queue, code)


@pytest.mark.parametrize('code', CODES)
@pytest.mark.parametrize('changes', [
    {'state': 'complete'}, {'body_sha256': 'a' * 64}, {'state': 'pending'}, {'state': 'inflight'},
    {'due': 1e12}, {'lease_owner': 'other'}, {'lease_until': 1e12},
    {'error_code': 'http_403'}, {'error_code': 'http_429'}, {'error_code': 'paywall_or_login'},
    {'last_checked_at_utc': None},
])
def test_validation_probe_never_fetches_saved_refused_future_or_leased_work(queue, changes, code):
    existing.test_attribution_failure_diagnosis_never_grants_saved_refused_future_or_leased_work(queue, changes, code)


@pytest.mark.parametrize('code', CODES)
@pytest.mark.parametrize('flags', [[], ['--retain-reviewed-response', '--require-current-missing-body-failure']])
def test_validation_cli_requires_current_head_and_never_allows_raw_body(queue, monkeypatch, flags, code):
    existing.test_attribution_cli_requires_pinned_current_head_and_metadata_only_before_any_http(
        queue, monkeypatch, flags, code)


@pytest.mark.parametrize('code', CODES)
def test_validation_probe_one_request_preserves_head_and_never_accepts_body(queue, monkeypatch, capsys, code):
    existing.test_explicit_author_diagnosis_uses_one_request_and_never_accepts_or_retains_body(
        queue, monkeypatch, capsys, code)


@pytest.mark.parametrize('code', CODES)
def test_future_observation_is_not_a_diagnostic_request_grant(queue, code):
    existing.test_future_failure_observation_cannot_grant_a_request_even_with_due_zero(queue, code)
