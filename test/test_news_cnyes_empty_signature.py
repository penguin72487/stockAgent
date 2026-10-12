"""Named current declarations do not make an explicitly blank credit visible."""
from dataclasses import replace
import json
import time

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT
from test_news_cnyes_canonical_alias import AUTHOR, BODY, CLOCK, MOBILE, URL, mobile_page, source, streamed_mobile_page
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_unknown_body_rechecks import config, legacy, queue, row


def empty_signature(payload):
    return payload.replace(f'鉅亨網記者{AUTHOR} 台北'.encode(), b'')


@pytest.mark.parametrize('page', [mobile_page, streamed_mobile_page])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
def test_current_named_declarations_accept_explicit_empty_visible_signature(cnyes_config, tmp_path, page, url):
    result = extract_article(empty_signature(page()), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 and result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['attribution_status'] == 'source_declared_author_visible_signature_empty'
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['visible_byline_raw'] == '' and detail['visible_byline_explicitly_empty'] is True
    assert detail['schema_authors_raw'] == [AUTHOR] and detail['current_data_author_raw'] == AUTHOR
    assert detail['named_author_invented'] is False
    assert 'explicit_empty_visible_signature_v1' in result['article_identity_basis']


@pytest.mark.parametrize('old,new', [
    ('<span class="signature">', '<span class="signature" hidden>'),
    ('<span class="signature">', '<span class="signature" aria-label="unknown credit">'),
    ('<span class="signature">', '<span class="signature" title="unknown credit">'),
    ('<span class="signature">', '<span class="signature" aria-labelledby="another-credit">'),
    ('<span class="signature">', '<span class="signature hidden">'),
    ('<span class="signature"></span>', '<span class="signature"><i></i></span>'),
    ('<span class="signature"></span>', '<span class="signature"><!--unknown--></span>'),
    ('<span class="signature"></span>', ''),
    ('<span class="signature"></span>', '<span class="signature"></span><span class="signature"></span>'),
])
def test_empty_visible_credit_does_not_accept_unknown_hidden_or_ambiguous_witnesses(cnyes_config, tmp_path, old, new):
    payload = empty_signature(streamed_mobile_page()).replace(old.encode(), new.encode())
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('changes', [{'author': '另一篇作者'}, {'author': ''}, {'newsId': 99}, {'payment': 1}])
def test_empty_credit_still_requires_current_named_author_identity_and_free_body(cnyes_config, tmp_path, changes):
    with pytest.raises(NewsError):
        extract_article(empty_signature(mobile_page(changes=changes)), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_empty_credit_never_replaces_a_missing_or_conflicting_visible_body(cnyes_config, tmp_path):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(empty_signature(mobile_page(visible='另一篇不相符正文')),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


def test_pinned_repair_requeues_only_due_terminal_native_author_failures_once(queue):
    url = URL + '?exp=a'
    before = legacy(queue, url=url, state='article_attribution_not_verified', error='article_attribution_not_verified')
    queue.recheck_cnyes_native_empty_signature(CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT)
    after = row(queue, url)
    assert after['state'] == 'retry' and after['due'] == 0
    for key in ('error_code', 'attempts', 'last_checked_at_utc', 'body_sha256', 'url'):
        assert after[key] == before[key]
    marker = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_native_v15_empty_signature_repair_v1:%'").fetchone()
    proof = json.loads(marker[0])
    assert proof['url_count'] == 1 and proof['accepted_as_bodies'] is False
    assert proof['source_refusals_stored_heads_and_future_deadlines_reset'] is False
    changes = queue.db.total_changes
    queue.recheck_cnyes_native_empty_signature(CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT)
    assert queue.db.total_changes == changes


@pytest.mark.parametrize('change', [
    {'state': 'http_403', 'error': 'http_403'},
    {'state': 'article_identity_not_verified', 'error': 'article_identity_not_verified'},
    {'error': 'http_429'}, {'due': time.time() + 10000}, {'lease': 'another-owner'}, {'body': 'stored-sha'},
    {'url': URL}, {'url': URL + '?exp=unknown'}, {'url': URL.replace('news.', 'amp-news.')},
])
def test_pinned_author_repair_preserves_refusals_heads_leases_other_routes_and_deadlines(queue, change):
    options = {'url': URL + '?exp=a', 'state': 'article_attribution_not_verified', 'error': 'article_attribution_not_verified', **change}
    before = legacy(queue, **options)
    queue.recheck_cnyes_native_empty_signature(CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT)
    assert row(queue, options['url']) == before


@pytest.mark.parametrize('reason', ['unrelated_contract', 'same_contract', 'source_disabled'])
def test_pinned_author_repair_needs_a_known_changed_allowed_contract(queue, reason):
    url = URL + '?exp=a'
    before = legacy(queue, url=url, state='article_attribution_not_verified', error='article_attribution_not_verified')
    source_policy = queue.registry.sources['cnyes']
    previous = CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT
    if reason == 'unrelated_contract':
        previous = '0' * 64
    elif reason == 'same_contract':
        queue.registry.sources['cnyes'] = replace(source_policy, data_fingerprint=previous)
    else:
        queue.registry.sources['cnyes'] = replace(source_policy, allowed=False)
    queue.recheck_cnyes_native_empty_signature(previous)
    assert row(queue, url) == before
