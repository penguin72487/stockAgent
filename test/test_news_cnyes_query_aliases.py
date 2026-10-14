"""Observed original query pages: same-response proof, never query stripping."""
import hashlib
import json
import time
from urllib.parse import urlsplit

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import cnyes_supported_native_alias_view
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_html import BODY, CLOCK, URL, source
from test_news_cnyes_unknown_body_rechecks import config, legacy, queue, row

QUERIES = ('amp;utm_medium=RSS&amp;utm_campaign=relate', 'defer_action=auth')


@pytest.mark.parametrize('query', QUERIES)
@pytest.mark.parametrize('renderer', [mobile_page, streamed_mobile_page])
def test_exact_observed_queries_require_the_original_response_proof(cnyes_config, tmp_path, query, renderer):
    original = MOBILE + '?' + query
    article = extract_article(renderer(body=BODY * 8), source(cnyes_config, tmp_path), page_url=original)
    assert article['body'] == BODY * 8 and article['published_at_raw'] == CLOCK
    assert article['declared_canonical_url'] == URL
    assert f'observed_{query.replace("=", "_")}_request_variant_with_full_current_article_proof_v1' in article['article_identity_basis']
    assert cnyes_supported_native_alias_view(urlsplit(original))
    assert not cnyes_supported_native_alias_view(urlsplit(URL + '?' + query))


@pytest.mark.parametrize('query', QUERIES)
@pytest.mark.parametrize('changes,schema', [
    ({'payment': 1}, {}), ({'payment': False}, {}), ({'newsId': 43}, {}),
    ({'author': '別篇作者'}, {}), ({'publishAt': 1388534401}, {}),
    ({}, {'isAccessibleForFree': False}), ({}, {'articleBody': '另一段正文'}),
])
def test_query_name_does_not_grant_access_identity_author_clock_or_body(cnyes_config, tmp_path, query, changes, schema):
    with pytest.raises(NewsError):
        extract_article(mobile_page(changes=changes, schema_changes=schema),
                        source(cnyes_config, tmp_path), page_url=MOBILE + '?' + query)


@pytest.mark.parametrize('query', [
    'defer_action=login', 'defer_action=auth&token=secret', 'defer_action=%61uth',
    'DEFER_ACTION=auth', 'defer_action=auth&defer_action=auth',
    'utm_medium=RSS&utm_campaign=relate', 'amp;utm_medium=RSS&amp;utm_campaign=other',
    'amp;utm_medium=RSS&amp;utm_campaign=relate&token=secret',
])
def test_nearby_or_credential_queries_are_not_inferred_as_observed(cnyes_config, tmp_path, query):
    assert not cnyes_supported_native_alias_view(urlsplit(MOBILE + '?' + query))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=MOBILE + '?' + query)


def test_query_capture43_does_not_change_bulk_or_source_permission(cnyes_config, tmp_path, monkeypatch):
    current, original_sha = source(cnyes_config, tmp_path), hashlib.sha256

    def version42(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original_sha(value)
        if isinstance(payload, dict) and 'mobile_canonical_html_contract_version' in payload:
            assert payload['mobile_canonical_html_contract_version'] == 43
            payload['mobile_canonical_html_contract_version'] = 42
            return original_sha(json.dumps(payload, sort_keys=True).encode())
        return original_sha(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', version42)
    previous = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != previous.data_fingerprint
    assert current.authorization_fingerprint == previous.authorization_fingerprint
    assert current.cnyes_bulk_data_fingerprint == previous.cnyes_bulk_data_fingerprint


@pytest.mark.parametrize('query', QUERIES)
def test_old_query_identity_failure_rechecks_once_without_resetting_evidence(queue, query):
    url, due = MOBILE + '?' + query, time.time() + 9000
    before = legacy(queue, url=url, state='article_identity_not_verified',
                    error='article_identity_not_verified', age=5, due=due)
    queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'unknown_cnyes_identity_rechecks_v1:%'")
    queue.db.commit()
    queue.migrate_unknown_cnyes_identity_rechecks()
    after = row(queue, url)
    assert after['state'] == 'retry_cooldown' and after['due'] == due
    for key in ('error_code', 'attempts', 'last_checked_at_utc', 'body_sha256', 'captured_at_utc', 'metadata_date_raw'):
        assert after[key] == before[key]
    changes = queue.db.total_changes
    queue.migrate_unknown_cnyes_identity_rechecks()
    assert queue.db.total_changes == changes


@pytest.mark.parametrize('query', QUERIES)
@pytest.mark.parametrize('change', [
    {'body': 'verified-existing-body'}, {'error': 'http_403'}, {'lease': 'live-owner'},
    {'state': 'paywall_or_login'}, {'state': 'source_not_found'},
])
def test_query_repair_never_overrides_saved_heads_leases_or_source_refusals(queue, query, change):
    options = {'url': MOBILE + '?' + query, 'state': 'article_identity_not_verified',
               'error': 'article_identity_not_verified', **change}
    before = legacy(queue, **options)
    queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'unknown_cnyes_identity_rechecks_v1:%'")
    queue.db.commit()
    queue.migrate_unknown_cnyes_identity_rechecks()
    assert row(queue, options['url']) == before
