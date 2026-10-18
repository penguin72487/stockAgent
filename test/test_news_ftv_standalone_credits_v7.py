"""Native byline-only paragraphs: source breaks, spaces, reporter and recovery."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_public_originals import _ftv_newsroom_credit, ftv_declared_public_canonical
from downloader.news_fulltext_queue import NewsQueue, ftv_native_parser_repair, utc_now, verify_bodies
from scripts.audit_news_selected_articles import audit
from scripts.verify_news_recent_batches import verify_recent_batches
from test_news_fulltext import FakeResponse, http_fixture
from test_news_ftv_paired_quotes import registry, page as quote_page
from test_news_ftv_quote_edges import edge_page
from test_news_ftv_literal_tabs_v4 import payload as tab_page
from test_news_ftv_subsite_headline_v5 import page as scoped_page, SUBSITE
from test_news_public_originals import CLOCK, FTV_URL, TEXT, ftv_page, ftv_subsite_page

URLS = ('https://www.ftvnews.com.tw/news/detail/2021309W0100',
        'https://www.ftvnews.com.tw/news/detail/2021316W0196')
CREDIT = '（民視新聞網／梁博超 台北報導）'


def page(credit=CREDIT, *, breaks=1, **kwargs):
    return ftv_page(credit='<br/>' * breaks + credit, **kwargs)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('credit,breaks', [(CREDIT, 1), (CREDIT, 2),
    ('(民視新聞 /梁博超 宋記者 台北報導)', 0), ('（民視新聞　／梁博超 台北報導）', 0)])
def test_native_standalone_byline_preserves_raw_credit_body_clocks_and_dom(parser, credit, breaks):
    raw = page(credit, breaks=breaks)
    soup = BeautifulSoup(raw, parser)
    body = soup.select_one('#contentarea')
    before, notices = str(body), []
    assert _ftv_newsroom_credit(body, standalone_credits=True, syntax_notices=notices) == credit
    assert str(body) == before and notices[0]['paragraph_leading_br_count'] == breaks
    value = extract_article(raw, registry(7, parser).sources['ftvnews'], page_url=FTV_URL)
    assert TEXT in value['body'] and value['published_at_raw'] == CLOCK
    assert value['authors_json'] == '["梁博超（即時中心） - 民視新聞網"]'
    assert json.loads(value['author_attribution_detail_json'])['visible_newsroom_credit_raw'] == credit
    proof = json.loads(value['verified_article_notices_json'])[-1]
    assert proof['contract_version'] == 7 and proof['declared_reporter_names_matched']
    assert proof['visible_newsroom_credit_raw'] == credit and not proof['original_values_modified']
    assert value['article_identity_basis'].endswith('_v7')
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(raw, registry(6, parser).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('credit', ['（民視新聞網／不是這位記者 台北報導）',
    '（民視新聞網／梁博 台北報導）', '（民視新聞網／大梁博超 台北報導）'])
def test_new_path_needs_literal_declared_reporter_not_a_substring_or_editor_guess(credit):
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(page(credit), registry(7).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('credit', ['（民視新聞網／梁博超 台北報導）後面還有正文',
    '這是正文，內引（民視新聞網／梁博超 台北報導）', '（民視新聞網／梁博超 台北報導)',
    '(民視新聞網／梁博超 台北報導）', '(民視新聞網／梁博超 台北報導)<br/>同段正文',
    '<figure>' + CREDIT + '</figure>', '<blockquote><p>' + CREDIT + '</p></blockquote>'])
def test_no_inline_prefix_mismatched_parentheses_quotation_or_caption_acceptance(credit):
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(page(credit), registry(7).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('wrapper', ['footer', 'header', 'blockquote', 'aside', 'nav', 'figure', 'figcaption'])
def test_credit_belongs_to_current_body_not_unrelated_roles(wrapper):
    raw = ftv_page(credit='未經證實的署名').replace(b'<p class="aphorism">',
        (f'<{wrapper}><p><br/>{CREDIT}</p></{wrapper}><p class="aphorism">').encode())
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(raw, registry(7).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('credit', ['<span hidden>' + CREDIT + '</span>',
                                   '<span style="display:none">' + CREDIT + '</span>'])
def test_hidden_native_credit_is_not_visible_provenance(credit):
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(page(credit), registry(7).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('credit', ['（中央社）', 'CNEWS匯流新聞網記者／台北報導', '圖、文／CTWANT'])
def test_competing_syndication_after_a_leading_break_still_excludes_the_body(credit):
    raw = page().replace(b'<p class="aphorism">', ('<p><br/>' + credit + '</p><p class="aphorism">').encode())
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract_article(raw, registry(7).sources['ftvnews'], page_url=FTV_URL)


def test_explicit_external_schema_guard_v6_is_not_lost_in_v7():
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract_article(page(name='外稿｜引新聞 - 民視新聞網'), registry(7).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('kwargs,error', [({'free': False}, 'paywall_or_login'),
    ({'canonical': FTV_URL + 'OTHER'}, 'article_identity_not_verified'),
    ({'og': FTV_URL + 'OTHER'}, 'article_identity_not_verified'),
    ({'schema_url': FTV_URL + 'OTHER'}, 'article_identity_not_verified')])
def test_source_identity_and_access_still_precede_credit(kwargs, error):
    with pytest.raises(NewsError, match='^' + error + '$'):
        extract_article(page(**kwargs), registry(7).sources['ftvnews'], page_url=FTV_URL)


def test_conflicting_visible_reporter_lines_never_collapse_to_the_first_one():
    raw = page().replace(b'<p class="aphorism">',
        '<p><br/>（民視新聞網／另一記者 台北報導）</p><p class="aphorism">'.encode())
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(raw, registry(7).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_legacy_originals_and_all_subsite_proofs_stay_identical(parser):
    old, new = (registry(v, parser).sources['ftvnews'] for v in (6, 7))
    for raw in (ftv_page(), str(quote_page()).encode(), str(edge_page()).encode(), tab_page(),
                page(breaks=0)):
        assert extract_article(raw, old, page_url=FTV_URL) == extract_article(raw, new, page_url=FTV_URL)
    for host in ('finance', 'sport', 'health'):
        url = FTV_URL.replace('www.', host + '.')
        assert ftv_declared_public_canonical(ftv_subsite_page(url), new, page_url=url) == \
               ftv_declared_public_canonical(ftv_subsite_page(url), old, page_url=url)
    raw = str(scoped_page(parser, body_first=True)).encode()
    assert ftv_declared_public_canonical(raw, new, page_url=SUBSITE) == \
           ftv_declared_public_canonical(raw, old, page_url=SUBSITE)


def test_canonical_capture_durable_batch_and_selected_current_source_readback(tmp_path):
    config = deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    config['providers'] = [entry for entry in config['providers'] if entry['id'] == 'ftvnews']
    config['providers'][0].update(embedded_article_layout='ftv_visible_article_v7', interval_seconds=0.001)
    config['exclusive_http_owner'] = False
    raw = page()
    http, session = http_fixture(config, tmp_path, {
        'https://www.ftvnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        FTV_URL: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': FTV_URL}])
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (FTV_URL,)).fetchone())
            record = fetch_article(http, row)
            assert record['raw_response_body'] == raw
            queue.accept([record])
            assert verify_bodies(tmp_path)['accepted'] and verify_recent_batches(tmp_path, 1)['accepted']
            assert audit(tmp_path, [FTV_URL], registry=http.registry)['accepted']
            assert [url for url, _ in session.calls] == ['https://www.ftvnews.com.tw/robots.txt', FTV_URL]
    finally:
        http.close()


def test_exact_v6_prior_changes_only_capture_not_authorization_or_capacity():
    old, prior, new = (registry(v).sources['ftvnews'] for v in (1, 6, 7))
    assert new.data_fingerprint != prior.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == prior.data_fingerprint
    assert new.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == prior.authorization_fingerprint == old.authorization_fingerprint
    assert ftv_native_parser_repair(new, prior.data_fingerprint)
    assert not ftv_native_parser_repair(new, registry(5).sources['ftvnews'].data_fingerprint)


def test_only_two_proven_missing_heads_requeue_once_preserving_observations_and_budgets(tmp_path):
    with closing(NewsQueue(tmp_path, registry(6))) as queue:
        for url in (*URLS, FTV_URL):
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',error_code='article_attribution_not_verified',"
                'attempts=4,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ftvnews:20261018','2')")
        queue.db.commit()
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, registry(7))) as queue:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == \
            {url: {**row, 'state': 'retry'} if url in URLS else row for url, row in before.items()}
        marker = 'ftv_standalone_credit_recheck_v7:' + registry(7).sources['ftvnews'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        assert [row['url'] for row in json.loads(receipt)['rows']] == list(URLS)
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        queue.db.execute('UPDATE articles SET state=error_code')
        queue.db.commit()
    with closing(NewsQueue(tmp_path, registry(7))) as queue:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v6'])
def test_saved_leased_refused_future_or_unproved_heads_never_requeue(tmp_path, boundary):
    with closing(NewsQueue(tmp_path, registry(5 if boundary == 'skipped_v6' else 6))) as queue:
        queue.add_articles([{'url': URLS[0]}])
        queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',error_code='article_attribution_not_verified',"
            'attempts=4,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), URLS[0]))
        changes = {'body': ('body_sha256', 'b' * 64), 'parquet': ('parquet_path', 'immutable.parquet'),
            'lease_owner': ('lease_owner', 'someone'), 'lease_until': ('lease_until', 9000000000),
            'future_due': ('due', 9000000000), 'state': ('state', 'http_403'), 'error': ('error_code', 'http_429'),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-14T00:00:00'),
            'future_clock': ('last_checked_at_utc', '2299-01-01T00:00:00+00:00'), 'invalid_clock': ('last_checked_at_utc', 'invalid')}
        if boundary == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value='unknown' WHERE key='source_capture_fingerprint:ftvnews'")
        elif boundary != 'skipped_v6':
            field, value = changes[boundary]
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URLS[0]))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    with closing(NewsQueue(tmp_path, registry(7))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before
