"""Exact FTV quote mismatch; immutable values and single missing-head recovery."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_public_originals import ftv_declared_public_canonical
from downloader.news_fulltext_queue import NewsQueue, ftv_paired_quote_parser_repair, utc_now
from test_news_public_originals import CLOCK, FTV_URL, TEXT, ftv_page, ftv_subsite_page

URL = 'https://www.ftvnews.com.tw/news/detail/2026A13W0651'
TITLE = "公開資訊：設備廠''研究公司''前3季EPS 10.67元創新高"


def registry(version=2, parser='lxml', *, change=None):
    cfg = load_config(Path('configs/news_fulltext.json'))[0]
    entry = deepcopy(next(row for row in cfg['providers'] if row['id'] == 'ftvnews'))
    entry['embedded_article_layout'] = f'ftv_visible_article_v{version}'
    entry['html_parser'] = parser
    if change:
        key, value = change
        entry[key] = value
    return PolicyRegistry({**cfg, 'providers': [entry]}, base=Path.cwd())


def page(parser='lxml'):
    soup = BeautifulSoup(ftv_page(title=TITLE), parser)
    soup.title.string = TITLE + ' - 民視新聞網'
    tag = soup.new_tag('meta', property='og:title', content=TITLE.split("''")[0])
    soup.head.append(tag)
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.string)
    data['headline'] = TITLE.replace("''", '')
    script.string = json.dumps(data, ensure_ascii=False)
    return soup


def extract(soup, *, version=2, parser='lxml', url=FTV_URL):
    return extract_article(str(soup).encode(), registry(version, parser).sources['ftvnews'], page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_only_four_ascii_quotes_differ_and_original_titles_values_clocks_remain(parser):
    soup = page(parser)
    result = extract(soup, parser=parser)
    assert result['title'] == TITLE
    assert TEXT in result['body'] and result['published_at_raw'] == CLOCK
    assert '10.67' in result['title'] and '-12.5' in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert len(notices) == 1 and notices[0]['visible_headline_raw'] == TITLE
    assert notices[0]['schema_headline_raw'] == TITLE.replace("''", '')
    assert notices[0]['og_title_raw'] == TITLE.split("''")[0]
    assert not notices[0]['original_values_modified'] and not result['historical_point_in_time']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, version=1, parser=parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_normal_ftv_outputs_are_unchanged_bit_for_bit(parser):
    payload = ftv_page()
    old, new = (extract_article(payload, registry(v, parser).sources['ftvnews'], page_url=FTV_URL) for v in (1, 2))
    assert new == old


def test_actual_unescaped_og_attribute_shape_has_identical_dual_parser_output():
    soup = page()
    soup.select_one('meta[property="og:title"]').decompose()
    malformed = f"<meta property='og:title' content='{TITLE}'>"
    payload = str(soup).replace('</head>', malformed + '</head>').encode()
    values = [extract_article(payload, registry(parser=parser).sources['ftvnews'], page_url=FTV_URL)
        for parser in ('lxml', 'html.parser')]
    assert values[0] == values[1] and values[0]['title'] == TITLE


@pytest.mark.parametrize('host', ['finance', 'sport', 'health'])
def test_unchanged_subsite_resolution_proof_is_still_required_by_v2(host):
    url = FTV_URL.replace('www.', host + '.')
    values = [ftv_declared_public_canonical(ftv_subsite_page(url), registry(v).sources['ftvnews'],
        page_url=url) for v in (1, 2)]
    assert values[0] == values[1] and values[0]['declared_canonical_url'] == FTV_URL
    assert values[0]['clock_not_accepted_or_rewritten']


@pytest.mark.parametrize('change', ['value', 'missing_word', 'unbalanced', 'multiple_pairs',
    'numeric_quotes', 'single_quotes', 'smart_quotes', 'wrong_document', 'no_document',
    'duplicate_document', 'wrong_og', 'no_og', 'duplicate_og', 'other_canonical',
    'other_schema', 'other_og_url', 'conflicting_schema', 'subsite'])
def test_not_fuzzy_matching_or_another_article_or_unproved_punctuation(change):
    soup = page()
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.string)
    url = FTV_URL
    if change == 'value':
        data['headline'] = data['headline'].replace('10.67', '10.68')
    elif change == 'missing_word':
        data['headline'] = data['headline'].replace('設備廠', '')
    elif change in {'unbalanced', 'multiple_pairs', 'numeric_quotes', 'single_quotes', 'smart_quotes'}:
        bad = {'unbalanced': TITLE.replace("''研究公司''", "''研究公司'"),
            'multiple_pairs': TITLE + "''另一公司''", 'numeric_quotes': TITLE.replace('研究公司', '1234'),
            'single_quotes': TITLE.replace("''", "'"), 'smart_quotes': TITLE.replace("''研究公司''", '「研究公司」')}[change]
        soup.select_one('.scroll-header h1').string = bad
        soup.title.string = bad + ' - 民視新聞網'
        data['headline'] = bad.replace("'", '').replace('「', '').replace('」', '')
    elif change == 'wrong_document':
        soup.title.string = TITLE.replace('10.67', '10.68') + ' - 民視新聞網'
    elif change == 'no_document':
        soup.title.decompose()
    elif change == 'duplicate_document':
        soup.head.append(BeautifulSoup('<title>另一篇</title>', 'lxml').title)
    elif change == 'wrong_og':
        soup.select_one('meta[property="og:title"]')['content'] = '另一篇'
    elif change == 'no_og':
        soup.select_one('meta[property="og:title"]').decompose()
    elif change == 'duplicate_og':
        soup.head.append(soup.new_tag('meta', property='og:title', content=TITLE.split("''")[0]))
    elif change == 'other_canonical':
        soup.select_one('link[rel=canonical]')['href'] = URL
    elif change == 'other_schema':
        data['url'] = data['mainEntityOfPage'] = URL
    elif change == 'other_og_url':
        soup.select_one('meta[property="og:url"]')['content'] = URL
    elif change == 'conflicting_schema':
        other = deepcopy(data)
        other['headline'] = other['headline'].replace('10.67', '10.68')
        tag = soup.new_tag('script', type='application/ld+json')
        tag.string = json.dumps(other, ensure_ascii=False)
        soup.head.append(tag)
    elif change == 'subsite':
        url = FTV_URL.replace('www.', 'finance.')
    script.string = json.dumps(data, ensure_ascii=False)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, url=url)


@pytest.mark.parametrize('change,error', [('third_party', 'article_rights_or_scope_excluded'),
    ('clock', 'invalid_bulk_publication_clock'), ('hidden', 'article_identity_not_verified'),
    ('paid', 'paywall_or_login'), ('missing_credit', 'article_attribution_not_verified')])
def test_quotes_do_not_override_rights_access_visibility_or_own_release_clock(change, error):
    soup = page()
    if change in {'third_party', 'missing_credit'}:
        soup.select_one('#contentarea > div > p').string = '（中央社）' if change == 'third_party' else '無可核對署名'
    elif change == 'clock':
        soup.select_one('meta[property="article:published_time"]')['content'] = '2026-10-08T17:24:43+08:00'
    elif change == 'hidden':
        soup.select_one('#contentarea')['hidden'] = ''
    else:
        script = soup.find('script', type='application/ld+json')
        data = json.loads(script.string)
        data['isAccessibleForFree'] = False
        script.string = json.dumps(data)
    with pytest.raises(NewsError, match=error):
        extract(soup)


def test_exact_v1_capture_and_http_refusal_identity_preserved():
    old, new = registry(1).sources['ftvnews'], registry().sources['ftvnews']
    assert old.data_fingerprint != new.data_fingerprint
    assert new.http_capacity_source_fingerprint == (old.http_capacity_source_fingerprint or old.data_fingerprint) == old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert ftv_paired_quote_parser_repair(new, old.data_fingerprint)
    assert not ftv_paired_quote_parser_repair(new, 'unknown')


@pytest.mark.parametrize('change', [('interval_seconds', 3), ('parallelism', 2),
    ('authorization', {**registry(1).sources['ftvnews'].authorization, 'reviewed_on': '2026-10-18'})])
def test_source_or_authorization_change_cannot_inherit_old_retry_grant(change):
    assert not ftv_paired_quote_parser_repair(registry(change=change).sources['ftvnews'],
        registry(1).sources['ftvnews'].data_fingerprint)


def test_only_one_proved_missing_head_retries_once_without_resetting_any_other_field(tmp_path):
    old, new = registry(1), registry()
    cases = [(URL, 'article_identity_not_verified'), (FTV_URL, 'article_attribution_not_verified'),
        (FTV_URL + 'X', 'body_not_found'), (FTV_URL + 'Y', 'http_403'), (FTV_URL + 'Z', 'complete')]
    with closing(NewsQueue(tmp_path, old)) as queue:
        for url, code in cases:
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=1234,last_checked_at_utc=? WHERE url=?',
                (code, code, utc_now(), url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('diagnostic-test-budget','spent')")
        queue.db.commit()
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert after == {url: {**row, 'state': 'retry'} if url == URL else row for url, row in before.items()}
        marker = 'ftv_paired_quote_recheck_v2:' + new.sources['ftvnews'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        assert len(json.loads(receipt)['rows']) == 1
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='diagnostic-test-budget'").fetchone()[0] == 'spent'
        queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (URL,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before[URL]
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior'])
def test_missing_head_migration_skips_retained_leased_undue_or_unobserved_rows(tmp_path, boundary):
    with closing(NewsQueue(tmp_path, registry(1))) as queue:
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
        else:
            field, value = fields[boundary]
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    with closing(NewsQueue(tmp_path, registry())) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
