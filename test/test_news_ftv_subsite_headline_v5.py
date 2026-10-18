"""Native FTV headers and body subheadings have different source roles."""
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_public_originals import ftv_declared_public_canonical, verify_resolution_response
from downloader.news_fulltext_queue import NewsQueue, ftv_native_parser_repair, utc_now, verify_bodies
from scripts.audit_news_selected_articles import audit
from scripts.verify_news_recent_batches import verify_recent_batches
from test_news_fulltext import FakeResponse, http_fixture
from test_news_ftv_paired_quotes import page as quote_page, registry
from test_news_ftv_quote_edges import edge_page
from test_news_ftv_literal_tabs_v4 import payload as tab_page, URLS as COMPLETED_V4
from test_news_public_originals import CLOCK, FTV_URL, TITLE, TEXT, ftv_page, ftv_subsite_page

URL = 'https://finance.ftvnews.com.tw/news/detail/2026610W0400'
SUBSITE = FTV_URL.replace('www.', 'finance.')


def page(parser='lxml', *, body_first=False):
    soup = BeautifulSoup(ftv_subsite_page(SUBSITE), parser)
    article = soup.select_one('#__nuxt article')
    header = article.h1.extract()
    content = soup.new_tag('div', id='newscontent')
    for node in list(article.contents):
        content.append(node.extract())
    for title in ('正文小標題，不是主標題', '數值 -12.5 與比率 3.25% 均保留'):
        node = soup.new_tag('h1')
        node.string = title
        content.append(node)
    for node in (content, header) if body_first else (header, content):
        article.append(node)
    return soup


def proof(soup, *, version=5, parser='lxml'):
    return ftv_declared_public_canonical(str(soup).encode(), registry(version, parser).sources['ftvnews'], page_url=SUBSITE)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('body_first', [False, True])
def test_only_native_header_outside_newscontent_is_selected_never_the_first_h1(parser, body_first):
    soup = page(parser, body_first=body_first)
    before = str(soup)
    value = proof(soup, parser=parser)
    assert str(soup) == before and value['headline'] == TITLE
    assert value['subsite_published_at_raw'].endswith('+00:00') and value['clock_not_accepted_or_rewritten']
    assert value['visible_newsroom_credit_raw'] == '論壇中心／綜合報導'
    assert value['basis'].endswith('_v5')
    assert value['headline_selection'] == {'contract_version': 5, 'native_body_id': 'newscontent',
        'total_h1_count': 3, 'body_h1_count': 2,
        'basis': 'single_current_article_h1_outside_unique_native_newscontent'}
    for version in (1, 2, 3, 4):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            proof(soup, version=version, parser=parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_single_header_and_all_old_canonical_outputs_stay_unchanged(parser):
    for host in ('finance', 'sport', 'health'):
        url = FTV_URL.replace('www.', host + '.')
        assert ftv_declared_public_canonical(ftv_subsite_page(url), registry(4, parser).sources['ftvnews'], page_url=url) == \
               ftv_declared_public_canonical(ftv_subsite_page(url), registry(5, parser).sources['ftvnews'], page_url=url)
    for raw in (ftv_page(), str(quote_page()).encode(), str(edge_page()).encode(), tab_page()):
        assert extract_article(raw, registry(4, parser).sources['ftvnews'], page_url=FTV_URL) == \
               extract_article(raw, registry(5, parser).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('change,error', [
    ('no_content', 'article_identity_not_verified'), ('duplicate_content', 'article_identity_not_verified'),
    ('wrong_content_tag', 'article_identity_not_verified'),
    ('duplicate_article', 'article_identity_not_verified'), ('outside_header', 'article_identity_not_verified'),
    ('header_inside_content', 'article_identity_not_verified'), ('two_headers', 'article_identity_not_verified'),
    ('related_header', 'article_identity_not_verified'), ('hidden_header', 'article_identity_not_verified'),
    ('hidden_content', 'article_identity_not_verified'), ('hidden_article', 'article_identity_not_verified'),
    ('wrong_canonical', 'article_identity_not_verified'), ('wrong_og', 'article_identity_not_verified'),
    ('wrong_schema', 'article_identity_not_verified'), ('wrong_title', 'article_identity_not_verified'),
    ('no_credit', 'article_attribution_not_verified'), ('third_party', 'article_rights_or_scope_excluded'),
    ('wrong_author', 'article_attribution_not_verified'), ('clock', 'invalid_bulk_publication_clock'),
    ('paid', 'paywall_or_login'), ('login', 'paywall_or_login'), ('challenge', 'access_challenge'),
])
def test_scoped_header_never_waives_identity_access_byline_or_clock(change, error):
    soup = page()
    content, article = soup.select_one('#newscontent'), soup.select_one('article')
    header = article.select_one(':scope > h1')
    script = soup.find('script', type='application/ld+json')
    schema = json.loads(script.string)
    if change == 'no_content':
        del content['id']
    elif change == 'wrong_content_tag':
        content.name = 'section'
    elif change == 'duplicate_content':
        article.append(soup.new_tag('div', id='newscontent'))
    elif change == 'duplicate_article':
        soup.select_one('#__nuxt').append(soup.new_tag('article'))
    elif change == 'outside_header':
        soup.select_one('#__nuxt').append(header.extract())
    elif change == 'header_inside_content':
        content.append(header.extract())
    elif change in {'two_headers', 'related_header'}:
        node = soup.new_tag('h1')
        node.string = '相關新聞標題不可借用'
        (article if change == 'two_headers' else soup.select_one('#__nuxt')).append(node)
    elif change.startswith('hidden_'):
        {'hidden_header': header, 'hidden_content': content, 'hidden_article': article}[change]['hidden'] = ''
    elif change == 'wrong_canonical':
        soup.select_one('link[rel=canonical]')['href'] = FTV_URL + 'X'
    elif change == 'wrong_og':
        soup.select_one('meta[property="og:url"]')['content'] = SUBSITE + 'X'
    elif change == 'wrong_schema':
        schema['url'] = schema['mainEntityOfPage'] = FTV_URL + 'X'
    elif change == 'wrong_title':
        header.string = TITLE + '錯誤數值'
    elif change in {'no_credit', 'third_party'}:
        content.select_one(':scope > p').string = '（中央社）' if change == 'third_party' else '沒有原創署名'
    elif change == 'wrong_author':
        schema['author'] = {'name': '其他媒體'}
    elif change == 'clock':
        schema['datePublished'] = CLOCK
    elif change == 'paid':
        schema['isAccessibleForFree'] = False
    elif change == 'login':
        soup.body.append(BeautifulSoup('<form action="/login"><input type="password"></form>', 'lxml').form)
    else:
        soup.title.string = 'Just a moment'
    script.string = json.dumps(schema, ensure_ascii=False)
    with pytest.raises(NewsError, match=error):
        proof(soup)


def transport(tmp_path):
    config = deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    config['providers'] = [entry for entry in config['providers'] if entry['id'] == 'ftvnews']
    config['providers'][0].update(embedded_article_layout='ftv_visible_article_v5', interval_seconds=0.001)
    config['exclusive_http_owner'] = False
    return http_fixture(config, tmp_path, {
        'https://finance.ftvnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://www.ftvnews.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        SUBSITE: FakeResponse(str(page(body_first=True)).encode()),
        FTV_URL: FakeResponse(ftv_page(credit='論壇中心／綜合報導'))})


def test_full_canonical_writer_receipts_selected_audit_and_both_response_clocks(tmp_path):
    http, session = transport(tmp_path)
    with closing(NewsQueue(tmp_path, http.registry)) as queue:
        queue.add_articles([{'url': SUBSITE}])
        record = fetch_article(http, dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (SUBSITE,)).fetchone()))
        queue.accept([record])
        assert record['requested_url'] == SUBSITE and record['final_url'] == FTV_URL
        assert record['published_at_raw'] == CLOCK and TEXT in record['body']
        evidence = json.loads(record['url_resolution_evidence_json'])
        assert evidence['declared_clock_conflict'] and evidence['subsite_published_at_raw'].endswith('+00:00')
        assert verify_resolution_response(record) == len(record['url_resolution_response_body'])
        assert verify_bodies(tmp_path)['accepted'] and verify_recent_batches(tmp_path, 1)['accepted']
        selected = audit(tmp_path, [SUBSITE], registry=http.registry)
        assert selected['accepted'] and selected['articles'][0]['canonical_resolution_source_replay_verified']
        assert [url for url, _ in session.calls] == ['https://finance.ftvnews.com.tw/robots.txt', SUBSITE,
            'https://www.ftvnews.com.tw/robots.txt', FTV_URL]
    http.close()


@pytest.mark.parametrize('change', ['count', 'float_contract', 'headline', 'credit', 'native_body', 'extra_header'])
def test_v5_retained_source_proof_is_reconstructed_not_just_trusted_metadata(tmp_path, change):
    http, _ = transport(tmp_path)
    record = fetch_article(http, {'url': SUBSITE, 'id': bytes.fromhex('11' * 32)})
    evidence = json.loads(record['url_resolution_evidence_json'])
    if change == 'count':
        evidence['headline_selection']['body_h1_count'] = 99
    elif change == 'float_contract':
        evidence['headline_selection']['contract_version'] = 5.0
    elif change == 'headline':
        evidence['headline'] += '另一文章'
    elif change == 'credit':
        evidence['visible_newsroom_credit_raw'] = '其他署名'
    else:
        soup = BeautifulSoup(record['url_resolution_response_body'], 'lxml')
        if change == 'native_body':
            del soup.select_one('#newscontent')['id']
        else:
            node = soup.new_tag('h1')
            node.string = '不可借用的額外標題'
            soup.select_one('article').append(node)
        raw = str(soup).encode()
        record['url_resolution_response_body'] = raw
        record['url_resolution_response_sha256'] = hashlib.sha256(raw).hexdigest()
        record['http_total_response_bytes'] = record['http_response_bytes'] + len(raw)
    record['url_resolution_evidence_json'] = json.dumps(evidence, sort_keys=True)
    with pytest.raises(ValueError, match='canonical_resolution'):
        verify_resolution_response(record)
    http.close()


def test_v5_requires_exact_v4_prior_and_preserves_v1_http_and_all_authorization_identities():
    old, prior, new = (registry(v).sources['ftvnews'] for v in (1, 4, 5))
    assert new.data_fingerprint != prior.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == prior.data_fingerprint
    assert new.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == prior.authorization_fingerprint == old.authorization_fingerprint
    assert ftv_native_parser_repair(new, prior.data_fingerprint)
    assert not ftv_native_parser_repair(new, registry(3).sources['ftvnews'].data_fingerprint)


def test_only_one_proven_missing_head_retries_once_without_resetting_any_observation_or_budget(tmp_path):
    with closing(NewsQueue(tmp_path, registry(4))) as queue:
        for url in (URL, *COMPLETED_V4):
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                'attempts=4,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ftvnews:20261018','2')")
        queue.db.commit()
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, registry(5))) as queue:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after == {url: {**r, 'state': 'retry'} if url == URL else r for url, r in before.items()}
        marker = 'ftv_subsite_headline_recheck_v5:' + registry(5).sources['ftvnews'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        assert [r['url'] for r in json.loads(receipt)['rows']] == [URL]
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (URL,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, registry(5))) as queue:
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v4'])
def test_saved_leased_refused_undue_or_unproved_head_never_requeues(tmp_path, boundary):
    with closing(NewsQueue(tmp_path, registry(3 if boundary == 'skipped_v4' else 4))) as queue:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            'attempts=4,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), URL))
        fields = {'body': ('body_sha256', 'b' * 64), 'parquet': ('parquet_path', 'immutable.parquet'),
            'lease_owner': ('lease_owner', 'someone'), 'lease_until': ('lease_until', 9000000000),
            'future_due': ('due', 9000000000), 'state': ('state', 'http_403'), 'error': ('error_code', 'http_429'),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-14T00:00:00'),
            'future_clock': ('last_checked_at_utc', '2299-01-01T00:00:00+00:00'), 'invalid_clock': ('last_checked_at_utc', 'invalid')}
        if boundary == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value='unknown' WHERE key='source_capture_fingerprint:ftvnews'")
        elif boundary != 'skipped_v4':
            field, value = fields[boundary]
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    with closing(NewsQueue(tmp_path, registry(5))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('change', [('interval_seconds', 3), ('parallelism', 2),
    ('authorization', {**registry(4).sources['ftvnews'].authorization, 'reviewed_on': '2026-10-18'})])
def test_changed_source_or_authorization_never_inherits_the_native_retry_grant(change):
    assert not ftv_native_parser_repair(registry(5, change=change).sources['ftvnews'],
        registry(4).sources['ftvnews'].data_fingerprint)
