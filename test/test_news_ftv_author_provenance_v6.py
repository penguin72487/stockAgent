"""Explicit canonical syndication cannot inherit a generic FTV newsroom credit."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_public_originals import ftv_declared_public_canonical, verify_resolution_response
from downloader.news_fulltext_queue import NewsQueue, ftv_native_parser_repair, utc_now
from scripts.audit_news_selected_articles import audit
from scripts.verify_news_recent_batches import verify_recent_batches
from test_news_fulltext import FakeResponse, http_fixture
from test_news_ftv_paired_quotes import page as quote_page, registry
from test_news_ftv_quote_edges import edge_page
from test_news_ftv_literal_tabs_v4 import payload as tab_page
from test_news_ftv_subsite_headline_v5 import page as scoped_page, SUBSITE
from test_news_public_originals import CLOCK, FTV_URL, TEXT, ftv_page, ftv_subsite_page

EXTERNAL = '外稿｜引新聞 - 民視新聞網'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('author', [EXTERNAL, '外稿 | 引新聞 - 民視新聞網',
    '轉載／來源新聞 - 民視新聞網', '外稿：來源新聞 - 民視新聞網'])
@pytest.mark.parametrize('credit', ['即時中心／綜合報導', '（財經中心／綜合報導）緊接完整正文。'])
def test_explicit_external_author_overrides_generic_or_unrecognized_newsroom_credit(parser, author, credit):
    raw = ftv_page(name=author, credit=credit)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract_article(raw, registry(6, parser).sources['ftvnews'], page_url=FTV_URL)
    if credit == '即時中心／綜合報導':
        # Demonstrate the actual defect: the old suffix-only test accepts an
        # explicitly external author when another field says newsroom.
        old = extract_article(raw, registry(5, parser).sources['ftvnews'], page_url=FTV_URL)
        assert json.loads(old['authors_json']) == [author]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_legacy_originals_all_values_and_resolution_proofs_remain_identical(parser):
    old, new = (registry(version, parser).sources['ftvnews'] for version in (5, 6))
    for raw in (ftv_page(), str(quote_page()).encode(), str(edge_page()).encode(), tab_page()):
        assert extract_article(raw, new, page_url=FTV_URL) == extract_article(raw, old, page_url=FTV_URL)
    for host in ('finance', 'sport', 'health'):
        url = FTV_URL.replace('www.', host + '.')
        raw = ftv_subsite_page(url)
        assert ftv_declared_public_canonical(raw, new, page_url=url) == \
               ftv_declared_public_canonical(raw, old, page_url=url)
    raw = str(scoped_page(parser, body_first=True)).encode()
    assert ftv_declared_public_canonical(raw, new, page_url=SUBSITE) == \
           ftv_declared_public_canonical(raw, old, page_url=SUBSITE)


@pytest.mark.parametrize('author', ['即時中心 - 民視新聞網', '陳外稿 - 民視新聞網',
                                    '外稿研究員 - 民視新聞網'])
def test_no_substring_inference_or_body_footer_scope_guess(author):
    raw = ftv_page(name=author, extra='<footer>外稿｜引新聞</footer>').replace(
        TEXT.encode(), (TEXT + ' 本文討論「外稿｜引新聞」的署名格式。').encode())
    values = [extract_article(raw, registry(v).sources['ftvnews'], page_url=FTV_URL) for v in (5, 6)]
    assert values[0] == values[1] and json.loads(values[1]['authors_json']) == [author]


@pytest.mark.parametrize('kwargs,error', [
    ({'canonical': FTV_URL + 'OTHER'}, 'article_identity_not_verified'),
    ({'og': FTV_URL + 'OTHER'}, 'article_identity_not_verified'),
    ({'schema_url': FTV_URL + 'OTHER'}, 'article_identity_not_verified'),
    ({'free': False}, 'paywall_or_login'),
    ({'attrs': 'hidden'}, 'article_identity_not_verified'),
])
def test_wrong_article_or_refused_document_never_becomes_a_provenance_conclusion(kwargs, error):
    with pytest.raises(NewsError, match='^' + error + '$'):
        extract_article(ftv_page(name=EXTERNAL, **kwargs), registry(6).sources['ftvnews'], page_url=FTV_URL)


def test_one_explicit_external_coauthor_is_binding():
    raw = ftv_page()
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(raw, 'lxml')
    node = soup.find('script', type='application/ld+json')
    schema = json.loads(node.string)
    schema['author'] = [schema['author'], {'name': EXTERNAL}]
    node.string = json.dumps(schema, ensure_ascii=False)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract_article(str(soup).encode(), registry(6).sources['ftvnews'], page_url=FTV_URL)


def transport(tmp_path, *, external=False):
    config = deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    config['providers'] = [entry for entry in config['providers'] if entry['id'] == 'ftvnews']
    config['providers'][0].update(embedded_article_layout='ftv_visible_article_v6', interval_seconds=0.001)
    config['exclusive_http_owner'] = False
    return http_fixture(config, tmp_path, {
        'https://finance.ftvnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://www.ftvnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        SUBSITE: FakeResponse(str(scoped_page(body_first=True)).encode()),
        FTV_URL: FakeResponse(ftv_page(name=EXTERNAL if external else '即時中心 - 民視新聞網',
            credit='論壇中心／綜合報導'))})


def test_current_canonical_original_writer_still_preserves_both_responses_and_receipts(tmp_path):
    http, session = transport(tmp_path)
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': SUBSITE}])
            record = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (SUBSITE,)).fetchone()))
            queue.accept([record])
            assert record['published_at_raw'] == CLOCK and TEXT in record['body']
            assert verify_resolution_response(record) == len(record['url_resolution_response_body'])
            assert verify_recent_batches(tmp_path, 1)['accepted']
            assert audit(tmp_path, [SUBSITE], registry=http.registry)['accepted']
            assert [url for url, _ in session.calls] == ['https://finance.ftvnews.com.tw/robots.txt',
                SUBSITE, 'https://www.ftvnews.com.tw/robots.txt', FTV_URL]
    finally:
        http.close()


def test_external_canonical_fails_normally_without_retaining_a_body_or_raw_response(tmp_path):
    http, _ = transport(tmp_path, external=True)
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': SUBSITE}])
            queue.sync_finite_cohorts()
            row = queue.claim_url(SUBSITE, 300)
            assert row is not None
            with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$') as outcome:
                fetch_article(http, row)
            queue.failed(row, outcome.value, 6)
            current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (SUBSITE,)).fetchone())
            assert current['state'] == current['error_code'] == 'article_rights_or_scope_excluded'
            assert current['attempts'] == 1 and current['last_checked_at_utc']
            assert current['body_sha256'] is None and current['parquet_path'] is None
            assert current['lease_owner'] is None and current['lease_until'] is None
            assert not list(tmp_path.rglob('*.parquet'))
    finally:
        http.close()


def test_capture_abi_changes_without_new_authorization_capacity_or_positive_retry_grant():
    old, prior, new = (registry(version).sources['ftvnews'] for version in (1, 5, 6))
    assert new.data_fingerprint != prior.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == prior.data_fingerprint
    assert new.authorization_fingerprint == prior.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint == old.data_fingerprint
    assert not ftv_native_parser_repair(new, prior.data_fingerprint)


def test_author_provenance_upgrade_does_not_replay_healthy_failed_refused_leased_or_undue_heads(tmp_path):
    states = ['complete', 'article_attribution_not_verified', 'article_identity_not_verified',
              'http_403', 'http_429', 'retry']
    with closing(NewsQueue(tmp_path, registry(5))) as queue:
        for index, state in enumerate(states):
            url = FTV_URL + str(index)
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=?,'
                'last_checked_at_utc=?,body_sha256=?,parquet_path=?,lease_owner=?,lease_until=? WHERE url=?',
                (state, state, 9000000000 if state == 'retry' else 1234, utc_now(),
                 'a' * 64 if state == 'complete' else None, 'accepted.parquet' if state == 'complete' else None,
                 'other-owner' if state == 'retry' else None, 9000000000 if state == 'retry' else None, url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ftvnews:20261018','2')")
        queue.db.commit()
        before = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        budgets = dict(queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'failure_%'"))
    for _ in range(2):
        with closing(NewsQueue(tmp_path, registry(6))) as queue:
            assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
            assert dict(queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'failure_%'")) == budgets
            assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                ('source_capture_fingerprint:ftvnews',)).fetchone()[0] == registry(6).sources['ftvnews'].data_fingerprint
