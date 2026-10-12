from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_bulk import cnyes_records
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_cnyes_story_identity import audit
from test_cnyes_news_bulk import cfg as config, payload, response
from test_news_cnyes_canonical_alias import BODY, TITLE, URL, MOBILE, mobile_page
from test_news_fulltext import FakeResponse, http_fixture


def accepted_pair(config, tmp_path, *, changed_api_title=False):
    http, _ = http_fixture(config, tmp_path, {
        'https://m.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        MOBILE: FakeResponse(mobile_page())})
    queue = NewsQueue(tmp_path, http.registry)
    queue.add_articles([{'url': URL}, {'url': MOBILE}])
    rows, errors = cnyes_records(response(payload(ids=(42,), content='<p>' + BODY * 8 + '</p>')),
                               SimpleNamespace(registry=http.registry), http.registry.sources['cnyes'])
    assert not errors
    rows[0].update(raw_response_body=response(payload(ids=(42,), content='<p>' + BODY * 8 + '</p>')).body,
                   source_response_article_id=rows[0]['article_id'])
    if not changed_api_title:
        # Preserve the actual API title; the mobile fixture has a different
        # title for the same declared story and must never overwrite it.
        assert rows[0]['title'] != TITLE
    else:
        rows[0]['title'] = 'not the retained API title'
    queue.accept(rows)
    queue.accept([fetch_article(http, queue.claim_url(MOBILE, 300))])
    return queue, http


def test_same_story_can_have_different_metadata_without_field_copy_or_http(config, tmp_path, monkeypatch):
    queue, http = accepted_pair(config, tmp_path)
    try:
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.get',
                            lambda *a, **k: pytest.fail('readonly proof attempted HTTP'))
        before = queue.db.total_changes
        result = audit(tmp_path, [MOBILE], http.registry)
        assert result['accepted'] and result['publisher_requests'] == 0
        assert result['story_links'][0]['publication_instant_equal']
        assert not result['story_links'][0]['original_authors_equal']
        assert not result['story_links'][0]['observations_interchangeable']
        assert result['story_links'][0]['exact_body_digest_equal']
        assert result['selected_head_proof']['batch_proof']['current_heads_verified'] == 2
        assert queue.db.total_changes == before
        assert not result['fields_copied'] and not result['historical_completeness_verified']
    finally:
        queue.close()
        http.close()


def test_self_consistent_receipt_does_not_replace_original_api_field_proof(config, tmp_path):
    queue, http = accepted_pair(config, tmp_path, changed_api_title=True)
    try:
        result = audit(tmp_path, [MOBILE], http.registry)
        assert not result['accepted']
        assert not result['story_links'][0]['publisher_declared_story_identity_verified']
        assert result['selected_head_proof']['errors']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('urls', [[], [MOBILE] * 2, [MOBILE + '?exp=c'], [MOBILE + '?exp=%61'],
                                 [MOBILE + '#fragment'], [MOBILE.replace('https:', 'http:')],
                                 [URL], [MOBILE + '?exp=a&token=private']])
def test_unknown_views_are_not_stripped_or_linked(tmp_path, urls):
    with pytest.raises(ValueError):
        audit(tmp_path, urls, None)
