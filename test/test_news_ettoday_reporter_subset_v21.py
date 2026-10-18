"""Schema authors corroborate, never replace, the whole visible signature."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from itertools import combinations
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V21_PROVED_REPORTER_SUBSET_URLS
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_reporter_subset_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


NAMES = ['原始記者', '研究記者', '測試記者', '查證記者']


def page(parser='lxml', *, names=None, authors=None):
    names = names or NAMES[:2]
    soup = native.page('www', parser)
    soup.select_one('#credit').string = '記者' + '、'.join(names) + '／台北報導'
    native.update_schema(soup, author=authors if authors is not None else
        [{'@type': 'Person', 'name': name} for name in names[:1]])
    # Restore each parser's Script node class before a second fixture edit.
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=21):
    return extract_article(str(soup).encode(), replace(clocks.registry(version).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[1])


SUBSETS = [(count, indices) for count in range(2, 5) for size in range(1, count)
    for indices in combinations(range(count), size)]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('count,indices', SUBSETS)
def test_exact_ordered_subsets_keep_every_visible_reporter_and_all_newsroom_values(parser, count, indices):
    names = NAMES[:count]
    authors = [{'@type': 'Person', 'name': names[i]} for i in indices]
    soup = page(parser, names=names, authors=authors)
    original_dom = str(soup)
    actual = extract(soup, parser)
    assert str(soup) == original_dom
    baseline = extract(page(parser, names=names, authors=[{'@type': 'Person', 'name': n} for n in names]), parser, 20)
    assert {k: v for k, v in actual.items() if k != 'verified_article_notices_json'} == {
        k: v for k, v in baseline.items() if k != 'verified_article_notices_json'}
    assert json.loads(actual['authors_json']) == names
    notes = json.loads(actual['verified_article_notices_json'])
    if count == 2 and indices == (1,):
        # Retain the already accepted last-of-two notice byte-for-byte.
        assert actual == extract(soup, parser, 20)
        assert not any(n['kind'] == 'schema_declares_ordered_subset_of_native_reporters' for n in notes)
    else:
        notice, = (n for n in notes if n['kind'] == 'schema_declares_ordered_subset_of_native_reporters')
        assert notice == {'kind': 'schema_declares_ordered_subset_of_native_reporters',
            'schema_declared_reporters': [names[i] for i in indices], 'native_reporter_indices': list(indices),
            'reporters_taken_from_whole_visible_signature': True,
            'schema_promoted_to_additional_reporter': False, 'source_values_rewritten': False}
        assert [n for n in notes if n['kind'] != notice['kind']] == json.loads(baseline['verified_article_notices_json'])
        with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
            extract(soup, parser, 20)


@pytest.mark.parametrize('version', range(1, 21))
def test_old_versions_do_not_silently_accept_the_new_schema_shape(version):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(page(), version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('authors', [[], None, {}, '原始記者', {'@type': 'Person'},
    {'@type': 'Person', 'name': '其他記者'}, {'@type': 'Organization', 'name': '原始記者'},
    [{'@type': 'Person', 'name': '原始記者'}, {'@type': 'Person', 'name': '原始記者'}],
    [{'@type': 'Person', 'name': '研究記者'}, {'@type': 'Person', 'name': '原始記者'}],
    [{'@type': 'Person', 'name': '原始記者'}, {'@type': 'Organization', 'name': '研究記者'}],
    {'@type': 'Person', 'name': ['原始記者']}, {'@type': 'Person', 'name': {'name': '原始記者'}}])
def test_unknown_empty_mixed_duplicate_reordered_or_nonliteral_names_remain_rejected(parser, authors):
    soup = page(parser, names=NAMES)
    native.update_schema(soup, author=authors)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['canonical', 'headline', 'clock', 'paid', 'missing_credit', 'hidden_credit',
    'foreign_credit', 'duplicate_credit_names', 'body_missing', 'foreign_publisher'])
def test_a_subset_never_waives_article_access_identity_clocks_or_visible_attribution(parser, change):
    soup = page(parser)
    if change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '1'
    elif change == 'headline': native.update_schema(soup, headline='別篇新聞')
    elif change == 'clock': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'missing_credit': soup.select_one('#credit').decompose()
    elif change == 'hidden_credit': soup.select_one('#credit')['hidden'] = ''
    elif change == 'foreign_credit': soup.select_one('#credit').string = '中央社記者／台北報導'
    elif change == 'duplicate_credit_names': soup.select_one('#credit').string = '記者原始記者、原始記者／台北報導'
    elif change == 'body_missing': soup.select_one('.story').decompose()
    else: native.update_schema(soup, publisher={'@type': 'Organization', 'name': '中央社'})
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_previously_accepted_values_and_notices_remain_identical(parser, host):
    raw = str(native.page(host, parser)).encode()
    assert extract_article(raw, replace(clocks.registry(20).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[host == 'www']) == extract_article(raw,
        replace(clocks.registry(21).sources['ettoday'], html_parser=parser), page_url=native.URLS[host == 'www'])


def test_capture_ancestry_changes_without_resetting_access_or_capacity():
    old, new = (clocks.registry(v).sources['ettoday'] for v in (20, 21))
    assert ettoday_reporter_subset_parser_repair(new, old.data_fingerprint)
    assert not ettoday_reporter_subset_parser_repair(new, clocks.registry(19).sources['ettoday'].data_fingerprint)
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    assert new.parser_repair_capture_history[-1] == old.data_fingerprint
    changed = clocks.config(21)
    changed['providers'][0]['authorization']['scope'] += '; changed source scope'
    assert not ettoday_reporter_subset_parser_repair(PolicyRegistry(changed, base=Path.cwd()).sources['ettoday'], old.data_fingerprint)
    with pytest.raises(ValueError):
        clocks.registry(clocks.future_version())


@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'lease_owner', 'lease_until', 'has_body', 'has_partition', 'other_url', 'other_failure', 'wrong_error', 'complete'])
def test_repair_is_once_and_only_changes_the_proved_missing_head_state(tmp_path, excluded):
    old, new = clocks.registry(20), clocks.registry(21)
    url = ETTODAY_V21_PROVED_REPORTER_SUBSET_URLS[0]
    if excluded == 'other_url': url = url.replace('2002764', '2002765')
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}, {'url': native.URLS[0]}])
        values = {'state': 'article_identity_not_verified', 'error_code': 'article_identity_not_verified',
            'attempts': 3, 'due': 99, 'last_checked_at_utc': utc_now()}
        if excluded == 'future_due': values['due'] = (datetime.now(timezone.utc) + timedelta(days=1)).timestamp()
        elif excluded == 'unobserved': values['last_checked_at_utc'] = None
        elif excluded == 'future_observation': values['last_checked_at_utc'] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        elif excluded == 'naive_observation': values['last_checked_at_utc'] = '2026-10-18T00:00:00'
        elif excluded == 'lease_owner': values['lease_owner'] = 'other_owner'
        elif excluded == 'lease_until': values['lease_until'] = 1
        elif excluded == 'has_body': values['body_sha256'] = 'a' * 64
        elif excluded == 'has_partition': values['parquet_path'] = 'source.parquet'
        elif excluded == 'other_failure': values['state'] = 'article_attribution_not_verified'
        elif excluded == 'wrong_error': values['error_code'] = 'different_error'
        elif excluded == 'complete': values['state'] = 'complete'
        with queue.db:
            queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in values) + ' WHERE url=?', (*values.values(), url))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after == {u: {**row, 'state': 'retry' if excluded == 'none' and u == url else row['state']}
            for u, row in before.items()}
        marker = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('ettoday_reporter_subset_recheck_v21:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_reporter_subset(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
