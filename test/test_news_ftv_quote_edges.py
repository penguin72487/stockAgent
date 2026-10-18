"""Source-proved quote edges without re-fetching healthy FTV heads."""
from contextlib import closing
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_public_originals import ftv_declared_public_canonical
from downloader.news_fulltext_queue import NewsQueue, ftv_paired_quote_parser_repair, utc_now
from test_news_ftv_paired_quotes import page, registry, URL as V2_URL
from test_news_public_originals import CLOCK, FTV_URL, TEXT, ftv_page, ftv_subsite_page

URLS = tuple('https://www.ftvnews.com.tw/news/detail/' + value for value in (
    '2024730W0063', '2025829W0721', '2025903W0076'))
OTHER = 'https://www.ftvnews.com.tw/news/detail/2024711W0074'


def edge_page(title="測試股好''妖''！成交7.5萬張", parser='lxml'):
    soup = page(parser)
    soup.select_one('.scroll-header h1').string = title
    soup.title.string = title + ' - 民視新聞網'
    soup.select_one('meta[property="og:title"]')['content'] = title.split("''")[0]
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.string)
    data['headline'] = title.replace("''", '')
    script.string = json.dumps(data, ensure_ascii=False)
    return soup


def extract(soup, *, parser='lxml', version=3, url=FTV_URL):
    return extract_article(str(soup).encode(), registry(version, parser).sources['ftvnews'], page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('title', ["測試股好''妖''！成交7.5萬張", "''妖股''出關第二天 股價12.5元", "''妖''出關第二天"])
def test_only_one_cjk_quote_pair_changes_with_full_document_schema_id_and_explicit_og(parser, title):
    soup = edge_page(title, parser)
    value = extract(soup, parser=parser)
    assert value['title'] == title and TEXT in value['body']
    assert value['published_at_raw'] == CLOCK and '-12.5' in value['body']
    notice, = json.loads(value['verified_article_notices_json'])
    assert notice['contract_version'] == 3 and notice['kind'] == 'ftv_unescaped_paired_label_quotes'
    assert notice['visible_headline_raw'] == title
    assert notice['schema_headline_raw'] == title.replace("''", '')
    assert notice['og_title_raw'] == title.split("''")[0] and not notice['original_values_modified']
    assert value['article_identity_basis'].endswith('_v3')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser=parser, version=2)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_old_normal_and_v2_quote_outputs_are_unchanged_bit_for_bit(parser):
    for payload in (ftv_page(), str(page(parser)).encode()):
        assert extract_article(payload, registry(2, parser).sources['ftvnews'], page_url=FTV_URL) == \
               extract_article(payload, registry(3, parser).sources['ftvnews'], page_url=FTV_URL)
    assert json.loads(extract(page(parser), parser=parser)['verified_article_notices_json'])[0]['contract_version'] == 2


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('title', ["測試股好''妖''！成交7.5萬張", "''妖股''出關第二天"])
def test_real_unescaped_attribute_shapes_have_equal_dual_parser_results(parser, title):
    soup = edge_page(title, parser)
    soup.select_one('meta[property="og:title"]').decompose()
    raw = str(soup).replace('</head>', f"<meta property='og:title' content='{title}'></head>").encode()
    values = [extract_article(raw, registry(3, p).sources['ftvnews'], page_url=FTV_URL) for p in ('lxml', 'html.parser')]
    assert values[0] == values[1] and values[0]['title'] == title


@pytest.mark.parametrize('change,error', [
    ('missing_og', 'article_identity_not_verified'), ('wrong_og', 'article_identity_not_verified'),
    ('og_attribute_missing', 'article_identity_not_verified'), ('duplicate_og', 'article_identity_not_verified'),
    ('wrong_document', 'article_identity_not_verified'), ('no_document', 'article_identity_not_verified'),
    ('wrong_schema_id', 'article_identity_not_verified'), ('wrong_canonical', 'article_identity_not_verified'),
    ('schema_value', 'article_identity_not_verified'), ('wrong_tail', 'article_identity_not_verified'),
    ('hidden', 'article_identity_not_verified'), ('paid', 'paywall_or_login'),
    ('clock', 'invalid_bulk_publication_clock'), ('no_credit', 'article_attribution_not_verified'),
    ('third_party', 'article_rights_or_scope_excluded'),
])
def test_quote_edges_never_bypass_other_identity_scope_or_value_checks(change, error):
    soup = edge_page("''妖股''出關第二天 股價12.5元")
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.string)
    if change == 'missing_og':
        soup.select_one('meta[property="og:title"]').decompose()
    elif change == 'wrong_og':
        soup.select_one('meta[property="og:title"]')['content'] = '別篇'
    elif change == 'og_attribute_missing':
        del soup.select_one('meta[property="og:title"]')['content']
    elif change == 'duplicate_og':
        soup.head.append(soup.new_tag('meta', property='og:title', content=''))
    elif change == 'wrong_document':
        soup.title.string = soup.title.string.replace('12.5', '12.6')
    elif change == 'no_document':
        soup.title.decompose()
    elif change == 'wrong_schema_id':
        data['url'] = data['mainEntityOfPage'] = V2_URL
    elif change == 'wrong_canonical':
        soup.select_one('link[rel=canonical]')['href'] = V2_URL
    elif change == 'schema_value':
        data['headline'] = data['headline'].replace('12.5', '12.6')
    elif change == 'wrong_tail':
        data['headline'] = data['headline'].replace('出關', '')
    elif change == 'hidden':
        soup.select_one('#contentarea')['hidden'] = ''
    elif change == 'paid':
        data['isAccessibleForFree'] = False
    elif change == 'clock':
        soup.select_one('meta[property="article:published_time"]')['content'] = '2026-10-08T17:24:43+08:00'
    else:
        soup.select_one('#contentarea > div > p').string = '（中央社）' if change == 'third_party' else '未署名'
    script.string = json.dumps(data, ensure_ascii=False)
    with pytest.raises(NewsError, match=error):
        extract(soup)


@pytest.mark.parametrize('title', ["''妖股''", "測試股''妖股''", "''1234''出關", "''妖股'出關", "'妖股'出關",
    "''妖股''''另一股''出關", "''妖''股''出關", "''研究公司研究公司研究公司研究公司研究公司研究''出關"])
def test_unproved_quote_shapes_are_not_normalized(title):
    soup = edge_page(title)
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.string)
    # Exercise a real mismatch. Equal single-quoted titles are ordinary
    # previously accepted input and must not be rejected by this repair.
    data['headline'] = title.replace("'", '')
    script.string = json.dumps(data, ensure_ascii=False)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


def test_v3_uses_exact_v2_prior_capture_and_original_v1_http_identity():
    old, prior, new = (registry(v).sources['ftvnews'] for v in (1, 2, 3))
    assert len({s.data_fingerprint for s in (old, prior, new)}) == 3
    assert new.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == prior.data_fingerprint
    assert new.authorization_fingerprint == prior.authorization_fingerprint == old.authorization_fingerprint
    assert ftv_paired_quote_parser_repair(new, prior.data_fingerprint)
    assert not ftv_paired_quote_parser_repair(new, old.data_fingerprint)


@pytest.mark.parametrize('host', ['finance', 'sport', 'health'])
def test_v3_subsite_still_needs_independent_provenance_and_source_clock(host):
    url = FTV_URL.replace('www.', host + '.')
    values = [ftv_declared_public_canonical(ftv_subsite_page(url), registry(v).sources['ftvnews'], page_url=url)
              for v in (2, 3)]
    assert values[0] == values[1] and values[0]['clock_not_accepted_or_rewritten']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(edge_page(), url=url)


def test_only_three_proved_missing_heads_retry_once_with_all_other_fields_and_budgets_unchanged(tmp_path):
    old, new = registry(2), registry(3)
    with closing(NewsQueue(tmp_path, old)) as queue:
        for url in (*URLS, OTHER, V2_URL):
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                'attempts=3,due=1234,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ftvnews:20261017','2')")
        queue.db.commit()
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert after == {url: {**row, 'state': 'retry'} if url in URLS else row for url, row in before.items()}
        marker = 'ftv_paired_quote_recheck_v3:' + new.sources['ftvnews'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        assert json.loads(receipt)['contract_version'] == 3
        assert {r['url'] for r in json.loads(receipt)['rows']} == set(URLS)
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        for url in URLS:
            queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (url,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['body', 'parquet', 'lease_owner', 'lease_until', 'future_due', 'state',
    'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v2'])
def test_v3_never_requeues_retained_leased_undue_refused_or_unobserved_heads(tmp_path, boundary):
    with closing(NewsQueue(tmp_path, registry(1 if boundary == 'skipped_v2' else 2))) as queue:
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
        elif boundary != 'skipped_v2':
            field, value = fields[boundary]
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, url))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, registry(3))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('change', [('interval_seconds', 3), ('parallelism', 2),
    ('authorization', {**registry(2).sources['ftvnews'].authorization, 'reviewed_on': '2026-10-18'})])
def test_changed_source_cannot_inherit_v3_retry_grant(change):
    assert not ftv_paired_quote_parser_repair(registry(3, change=change).sources['ftvnews'],
        registry(2).sources['ftvnews'].data_fingerprint)
