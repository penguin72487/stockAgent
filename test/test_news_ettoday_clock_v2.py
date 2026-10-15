"""Independent native release/edit clocks, preserving v1 and retry evidence."""
from contextlib import closing
from datetime import datetime, timedelta
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_clock_only_parser_repair, utc_now
import test_news_ettoday as native


def config(version=2):
    cfg = native.config()
    cfg['providers'][0]['embedded_article_layout'] = f'ettoday_visible_article_v{version}'
    return cfg


def registry(version=2, base=None):
    return PolicyRegistry(config(version), base=base or Path.cwd())


def before_publication(host, parser, seconds):
    soup = native.page(host, parser)
    published = datetime.fromisoformat(native.PUB)
    modified = (published - timedelta(seconds=seconds)).isoformat()
    soup.select_one('meta[name=lastmod]')['content'] = modified
    native.update_schema(soup, dateModified=modified)
    # lxml's Script node class is restored at the response parse boundary.
    # Reparse the fixture before a second mutation; do not test an artificial
    # in-memory NavigableString as though it were publisher response bytes.
    return BeautifulSoup(str(soup), parser), modified


@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('seconds', [1, 7, 23, 65, 86400])
def test_distinct_declared_clocks_do_not_invent_order_availability_or_new_source_values(host, parser, seconds):
    soup, modified = before_publication(host, parser, seconds)
    payload = str(soup).encode()
    source = registry().sources['ettoday']
    result = extract_article(payload, source, page_url=native.URLS[host == 'www'])
    assert result['published_at_raw'] == native.PUB and result['modified_at_raw'] == modified
    assert native.PROSE in result['body'] and not result['historical_point_in_time']
    notice = [n for n in json.loads(result['verified_article_notices_json'])
        if n['kind'] == 'source_modification_precedes_publication']
    assert notice == [{'kind': 'source_modification_precedes_publication',
        'declared_publication': native.PUB, 'declared_modification': modified,
        'availability_backdated_to_modification': False, 'source_reason_inferred': False}]
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract_article(payload, registry(1).sources['ettoday'], page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_all_original_positive_values_outputs_and_raw_precision_stay_exact(host, parser):
    raw = str(native.page(host, parser)).encode()
    outputs = [extract_article(raw, registry(v).sources['ettoday'], page_url=native.URLS[host == 'www']) for v in (1, 2)]
    assert outputs[0] == outputs[1]


@pytest.mark.parametrize('change', ['epoch_placeholder', 'invalid_modification', 'naive_modification',
    'schema_disagrees', 'publication_disagrees', 'hidden_clock', 'other_article', 'missing_byline', 'paid'])
def test_independent_clocks_never_waive_clock_identity_attribution_or_access_checks(change):
    soup, modified = before_publication('finance', 'lxml', 23)
    if change in {'epoch_placeholder', 'invalid_modification', 'naive_modification'}:
        value = {'epoch_placeholder': '1970-01-01T08:00:00+08:00',
            'invalid_modification': '2026-99-99T22:18:00+08:00', 'naive_modification': '2026-10-13T22:17:37'}[change]
        soup.select_one('meta[name=lastmod]')['content'] = value
        native.update_schema(soup, dateModified=value)
    elif change == 'schema_disagrees': native.update_schema(soup, dateModified=native.MOD)
    elif change == 'publication_disagrees': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'hidden_clock': soup.select_one('time')['hidden'] = ''
    elif change == 'other_article': soup.select_one('meta[property="og:url"]')['content'] = native.URLS[0] + '0'
    elif change == 'missing_byline': soup.select_one('#credit').decompose()
    else: native.update_schema(soup, isAccessibleForFree=False)
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), registry().sources['ettoday'], page_url=native.URLS[0])


def test_only_exact_old_capture_receives_clock_retry_and_retains_http_identity():
    old, new = registry(1).sources['ettoday'], registry(2).sources['ettoday']
    assert old.data_fingerprint != new.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert ettoday_clock_only_parser_repair(new, old.data_fingerprint)
    assert not ettoday_clock_only_parser_repair(new, new.data_fingerprint)
    assert not ettoday_clock_only_parser_repair(new, 'unknown-prior-capture')
    cfg = config()
    cfg['providers'][0]['authorization']['scope'] += '; changed source scope'
    assert not ettoday_clock_only_parser_repair(PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday'], old.data_fingerprint)


def test_only_unleased_missing_clock_failures_retry_once_and_keep_all_original_evidence(tmp_path):
    old, new = registry(1, tmp_path), registry(2, tmp_path)
    previous = {}
    states = ['invalid_bulk_publication_clock', 'body_not_found', 'article_attribution_not_verified',
        'article_identity_not_verified', 'article_rights_or_scope_excluded', 'http_403', 'http_429',
        'paywall_or_login', 'source_not_found', 'complete']
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, state in enumerate(states, 1):
            url = 'https://finance.ettoday.net/news/' + str(number)
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            previous[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in previous.items():
            expected = {**row, 'state': 'retry'} if url.endswith('/1') else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        marker = 'ettoday_independent_release_revision_recheck_v2:' + new.sources['ettoday'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        evidence = json.loads(receipt)
        assert len(evidence['rows']) == 1 and evidence['rows'][0]['attempts'] == 3
        assert evidence['rows'][0]['due'] == 9000000000 and not evidence['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock' WHERE url=?", (next(iter(previous)),))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (next(iter(previous)),)).fetchone()[0] == 'invalid_bulk_publication_clock'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['stored_hash', 'stored_path', 'lease_owner', 'lease_until',
    'missing_observation', 'different_error', 'unknown_prior_capture', 'unknown_host', 'query'])
def test_clock_repair_never_reopens_unproved_saved_leased_or_other_source_heads(tmp_path, change):
    old, new = registry(1, tmp_path), registry(2, tmp_path)
    url = ('https://house.ettoday.net/news/11' if change == 'unknown_host' else
        'https://finance.ettoday.net/news/11?foo=1' if change == 'query' else 'https://finance.ettoday.net/news/11')
    columns = {'stored_hash': ('body_sha256', 'a' * 64), 'stored_path': ('parquet_path', 'bodies/old.parquet'),
        'lease_owner': ('lease_owner', 'other-owner'), 'lease_until': ('lease_until', 9000000000),
        'missing_observation': ('last_checked_at_utc', None), 'different_error': ('error_code', 'http_403')}
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock',error_code='invalid_bulk_publication_clock',"
            'attempts=4,due=9000000000,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        if change in columns:
            column, value = columns[change]
            queue.db.execute('UPDATE articles SET ' + column + '=? WHERE url=?', (value, url))
        elif change == 'unknown_prior_capture':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('b' * 64, 'source_capture_fingerprint:ettoday'))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before
