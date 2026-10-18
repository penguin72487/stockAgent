"""FTV JSON value TABs are preserved, never identity/access/date repairs."""
from contextlib import closing
from copy import deepcopy
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_public_originals import _ftv_schema_json, ftv_declared_public_canonical
from downloader.news_fulltext_queue import NewsQueue, ftv_native_parser_repair, ftv_paired_quote_parser_repair, utc_now
from test_news_ftv_paired_quotes import page, registry, URL as V2_URL
from test_news_ftv_quote_edges import edge_page, URLS as V3_URLS
from test_news_public_originals import CLOCK, FTV_URL, TEXT, ftv_page, ftv_subsite_page

URLS = tuple('https://finance.ftvnews.com.tw/news/detail/' + value for value in (
    '2026501W0287', '2026507W0200', '2026510W0129'))
OTHER = 'https://finance.ftvnews.com.tw/news/detail/2026610W0400'
AUTHOR = '研究記者（內容中心）\t - 民視新聞網'


def payload(*, change=None, parser='lxml'):
    soup = BeautifulSoup(ftv_page(), parser)
    script = soup.find('script', type='application/ld+json')
    schema = json.loads(script.string)
    schema['author'] = {'@type': 'Person', 'name': AUTHOR}
    if change:
        change(soup, schema)
    script.string = json.dumps(schema, ensure_ascii=False)
    return str(soup).encode().replace(b'\\t', b'\t')


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_only_observed_literal_tab_decoding_changes_with_all_values_and_clocks_preserved(parser):
    raw = payload(parser=parser)
    before = raw
    value = extract_article(raw, registry(4, parser).sources['ftvnews'], page_url=FTV_URL)
    assert raw == before and TEXT in value['body'] and '-12.5' in value['body']
    assert value['published_at_raw'] == CLOCK and json.loads(value['authors_json']) == [AUTHOR]
    assert json.loads(value['author_attribution_detail_json'])['schema_authors_raw'] == [AUTHOR]
    notice, = json.loads(value['verified_article_notices_json'])
    assert notice == {'kind': 'ftv_literal_json_value_tabs', 'contract_version': 4,
        'literal_tab_count': 1, 'decoded_values_modified': False,
        'raw_evidence': 'shared_original_response_receipt'}
    assert value['article_identity_basis'].endswith('lossless_json_tabs_v4')
    assert not value['historical_point_in_time'] and not value['per_article_license_verified']
    for version in (1, 2, 3):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract_article(raw, registry(version, parser).sources['ftvnews'], page_url=FTV_URL)


def test_actual_source_tab_has_identical_dual_html_parser_result():
    raw = payload()
    values = [extract_article(raw, registry(4, parser).sources['ftvnews'], page_url=FTV_URL)
              for parser in ('lxml', 'html.parser')]
    assert values[0] == values[1]


@pytest.mark.parametrize('codepoint', range(32))
@pytest.mark.parametrize('field', ['headline', 'description', 'keywords'])
def test_bounded_stdlib_decoding_changes_no_value_and_only_accepts_literal_value_tab(codepoint, field):
    value = {field: '原始值"\\ ' + chr(codepoint) + ' 股'}
    encoded = json.dumps(value, ensure_ascii=False)
    raw = encoded.replace(json.dumps(chr(codepoint))[1:-1], chr(codepoint))
    assert _ftv_schema_json(encoded) == (value, 0)
    if codepoint == 9:
        assert _ftv_schema_json(raw) == (value, 1)
    else:
        with pytest.raises(ValueError):
            _ftv_schema_json(raw)


@pytest.mark.parametrize('raw', [
    '{"bad\tkey":"value"}', '\x02{"name":"source\tvalue"}',
    '{"name":"source\tvalue"}\x08', '{"name":"source\\\tvalue"}',
    '{"name":"source\tvalue",}', '{"name":"unclosed\t}',
    '{"name":"source\tvalue"//unreviewed\n}', '{"name":"source\tvalue" /*unreviewed*/}',
    '{"name":"source\tvalue"}eval(1)', '{"name":"source\tvalue", "other":"raw\nline"}',
    '{"name":"source\tvalue", "bad\\tkey":"value"}',
])
def test_tabs_never_repair_controls_in_keys_broken_escapes_or_other_json_grammar(raw):
    with pytest.raises(ValueError):
        _ftv_schema_json(raw)


def test_strict_valid_fast_path_preserves_existing_values_and_all_old_article_outputs():
    for raw in (ftv_page(), str(page()).encode(), str(edge_page()).encode()):
        assert extract_article(raw, registry(3).sources['ftvnews'], page_url=FTV_URL) == \
               extract_article(raw, registry(4).sources['ftvnews'], page_url=FTV_URL)
    value = {'name': 'literal:\\t actual:\t escaped quote:" slash:\\'}
    assert _ftv_schema_json(json.dumps(value)) == (value, 0)


@pytest.mark.parametrize('change,error', [
    ('id', 'article_identity_not_verified'), ('headline', 'article_identity_not_verified'),
    ('canonical', 'article_identity_not_verified'), ('og', 'article_identity_not_verified'),
    ('hidden', 'article_identity_not_verified'), ('paid', 'paywall_or_login'),
    ('clock', 'invalid_bulk_publication_clock'), ('byline', 'article_attribution_not_verified'),
    ('third_party', 'article_rights_or_scope_excluded'), ('author', 'article_rights_or_scope_excluded'),
    ('comment', 'article_identity_not_verified'), ('many_scripts', 'article_identity_not_verified'),
    ('huge_schema', 'article_identity_not_verified'),
])
def test_lossless_syntax_does_not_waive_complete_article_semantics(change, error):
    def mutate(soup, schema):
        if change == 'id':
            schema['url'] = schema['mainEntityOfPage'] = V2_URL
        elif change == 'headline':
            schema['headline'] += '另一文章'
        elif change == 'canonical':
            soup.select_one('link[rel=canonical]')['href'] = V2_URL
        elif change == 'og':
            soup.select_one('meta[property="og:url"]')['content'] = V2_URL
        elif change == 'hidden':
            soup.select_one('#contentarea')['hidden'] = ''
        elif change == 'paid':
            schema['isAccessibleForFree'] = False
        elif change == 'clock':
            schema['datePublished'] = '2026-10-08T17:24:43+08:00'
        elif change in {'byline', 'third_party'}:
            soup.select_one('#contentarea > div > p').string = '（中央社）' if change == 'third_party' else '未署名'
        elif change == 'author':
            schema['author']['name'] = '另一媒體\t - 其他新聞網'
        elif change in {'comment', 'many_scripts'}:
            for _ in range(16 if change == 'many_scripts' else 1):
                node = soup.new_tag('script', type='application/ld+json')
                node.string = '{"name":"value" /*unreviewed*/}' if change == 'comment' else '{}'
                soup.head.append(node)
        else:
            schema['description'] = 'x' * 262144
    with pytest.raises(NewsError, match=error):
        extract_article(payload(change=mutate), registry(4).sources['ftvnews'], page_url=FTV_URL)


@pytest.mark.parametrize('host', ['finance', 'sport', 'health'])
def test_v4_subsite_resolution_requires_unchanged_strict_independent_source_proof(host):
    url = FTV_URL.replace('www.', host + '.')
    values = [ftv_declared_public_canonical(ftv_subsite_page(url), registry(v).sources['ftvnews'], page_url=url)
              for v in (3, 4)]
    assert values[0] == values[1] and values[0]['clock_not_accepted_or_rewritten']


def test_exact_v3_capture_and_original_http_and_authorization_identity_are_required():
    old, prior, new = (registry(v).sources['ftvnews'] for v in (1, 3, 4))
    assert new.data_fingerprint != prior.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == prior.data_fingerprint
    assert new.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == prior.authorization_fingerprint == old.authorization_fingerprint
    assert ftv_native_parser_repair(new, prior.data_fingerprint)
    assert not ftv_paired_quote_parser_repair(new, prior.data_fingerprint)
    assert not ftv_native_parser_repair(new, old.data_fingerprint)


def test_only_three_proved_missing_heads_retry_once_without_touching_any_other_field_or_budget(tmp_path):
    with closing(NewsQueue(tmp_path, registry(3))) as queue:
        for url in (*URLS, OTHER, V2_URL, *V3_URLS):
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code=state,"
                'attempts=3,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
            queue.db.execute("UPDATE articles SET error_code=state WHERE url=?", (url,))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ftvnews:20261017','2')")
        queue.db.commit()
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, registry(4))) as queue:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after == {url: {**r, 'state': 'retry'} if url in URLS else r for url, r in before.items()}
        marker = 'ftv_literal_tab_recheck_v4:' + registry(4).sources['ftvnews'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        assert {r['url'] for r in json.loads(receipt)['rows']} == set(URLS)
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        for url in URLS:
            queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (url,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, registry(4))) as queue:
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v3'])
def test_saved_leased_refused_undue_or_unproved_heads_never_requeue(tmp_path, boundary):
    with closing(NewsQueue(tmp_path, registry(2 if boundary == 'skipped_v3' else 3))) as queue:
        url = URLS[0]
        queue.add_articles([{'url': url}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            'attempts=4,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        fields = {'body': ('body_sha256', 'b' * 64), 'parquet': ('parquet_path', 'immutable.parquet'),
            'lease_owner': ('lease_owner', 'someone'), 'lease_until': ('lease_until', 9000000000),
            'future_due': ('due', 9000000000), 'state': ('state', 'http_403'), 'error': ('error_code', 'http_429'),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-14T00:00:00'),
            'future_clock': ('last_checked_at_utc', '2299-01-01T00:00:00+00:00'), 'invalid_clock': ('last_checked_at_utc', 'invalid')}
        if boundary == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value='unknown' WHERE key='source_capture_fingerprint:ftvnews'")
        elif boundary != 'skipped_v3':
            field, value = fields[boundary]
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, url))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, registry(4))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('change', [('interval_seconds', 3), ('parallelism', 2),
    ('authorization', {**registry(3).sources['ftvnews'].authorization, 'reviewed_on': '2026-10-18'})])
def test_changed_source_or_authorization_never_inherits_the_native_retry_grant(change):
    assert not ftv_native_parser_repair(registry(4, change=change).sources['ftvnews'],
        registry(3).sources['ftvnews'].data_fingerprint)
