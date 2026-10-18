"""Different native section/channel labels are not article identity conflicts."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V22_PROVED_3C_CHANNEL_URLS
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_3c_channel_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


def page(parser='lxml', host='www'):
    soup = native.page(host, parser)
    soup.select_one('meta[name=subsection]')['content'] = '3C'
    soup.select_one('meta[property="og:title"]')['content'] = native.TITLE + ' | ETtoday3C家電新聞 | ETtoday新聞雲'
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=22, host='www'):
    return extract_article(str(soup).encode(), replace(clocks.registry(version).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_exact_literal_native_pair_keeps_all_original_values_and_distinct_labels(parser):
    soup = page(parser)
    original = str(soup)
    actual = extract(soup, parser)
    expected = extract(native.page('www', parser), parser, 21)
    assert str(soup) == original
    assert {k: v for k, v in actual.items() if k != 'verified_article_notices_json'} == {
        k: v for k, v in expected.items() if k != 'verified_article_notices_json'}
    notes = json.loads(actual['verified_article_notices_json'])
    label, = (n for n in notes if n['kind'] == 'native_subsection_and_social_channel_labels_distinct')
    assert label == {'kind': 'native_subsection_and_social_channel_labels_distinct',
        'native_subsection': '3C', 'native_social_channel': '3C家電',
        'native_social_title': native.TITLE + ' | ETtoday3C家電新聞 | ETtoday新聞雲',
        'identity_basis': 'same_literal_canonical_og_url_unique_current_schema_and_visible_reporter_story',
        'source_labels_rewritten': False, 'additional_source_host_admitted': False}
    schema, = (n for n in notes if n['kind'] == 'native_variant_current_schema_headline')
    assert schema['matches'] == 'native_headline' and schema['literal_article_url_and_clocks_verified']
    assert [n for n in notes if n not in (schema, label)] == json.loads(expected['verified_article_notices_json'])


@pytest.mark.parametrize('version', range(1, 22))
def test_old_contracts_do_not_silently_admit_new_section_labels(version):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(), version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('section,channel', [('3c', '3C家電'), ('3C ', '3C家電'), (' 3C', '3C家電'),
    ('3Ｃ', '3C家電'), ('３C', '3C家電'), ('3C家電', '3C家電'), ('3D', '3C家電'),
    ('3C', '3C'), ('3C', '消費'), ('好康資訊', '消費'), ('3C', '3C家電雲'), ('', '3C家電')])
def test_no_fuzzy_category_crosswalk_or_other_taxonomy_is_inferred(parser, section, channel):
    soup = page(parser)
    soup.select_one('meta[name=subsection]')['content'] = section
    soup.select_one('meta[property="og:title"]')['content'] = native.TITLE + f' | ETtoday{channel}新聞 | ETtoday新聞雲'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['missing_schema', 'extra_schema', 'missing_section', 'extra_section',
    'canonical', 'og_url', 'foreign_reporter', 'no_reporter', 'hidden_reporter', 'hidden_body', 'second_story',
    'schema_headline', 'schema_url', 'schema_clock', 'schema_author', 'schema_publisher', 'paid', 'schema_full_body'])
def test_native_pair_never_waives_unique_schema_identity_clocks_access_or_reporters(parser, change):
    from copy import deepcopy
    soup = page(parser)
    if change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'extra_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'missing_section': soup.select_one('meta[name=subsection]').decompose()
    elif change == 'extra_section': soup.head.append(deepcopy(soup.select_one('meta[name=subsection]')))
    elif change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '0'
    elif change == 'og_url': soup.select_one('meta[property="og:url"]')['content'] += '0'
    elif change == 'foreign_reporter': soup.select_one('#credit').string = '中央社記者／台北報導'
    elif change == 'no_reporter': soup.select_one('#credit').decompose()
    elif change == 'hidden_reporter': soup.select_one('#credit')['hidden'] = ''
    elif change == 'hidden_body': soup.select_one('.story')['hidden'] = ''
    elif change == 'second_story': soup.body.append(deepcopy(soup.select_one('.story')))
    else:
        field, value = {'schema_headline': ('headline', '別篇文章'), 'schema_url': ('url', native.URLS[1] + '0'),
            'schema_clock': ('datePublished', native.MOD), 'schema_author': ('author', {'@type': 'Person', 'name': '他人'}),
            'schema_publisher': ('publisher', {'@type': 'Organization', 'name': '別家出版社'}),
            'paid': ('isAccessibleForFree', False), 'schema_full_body': ('articleBody', native.PROSE)}[change]
        native.update_schema(soup, **{field: value})
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_the_new_pair_is_not_a_finance_host_or_subdomain_scope_expansion(parser):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(parser, 'finance'), parser, host='finance')
    with pytest.raises(NewsError):
        extract_article(str(page(parser)).encode(), clocks.registry(22).sources['ettoday'],
            page_url='https://house.ettoday.net/news/3254110')


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_every_previously_valid_value_and_notice_is_identical(parser, host):
    raw = str(native.page(host, parser)).encode()
    assert extract_article(raw, replace(clocks.registry(21).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[host == 'www']) == extract_article(raw,
        replace(clocks.registry(22).sources['ettoday'], html_parser=parser), page_url=native.URLS[host == 'www'])


def test_capture_ancestry_changes_without_resetting_scope_or_capacity():
    old, new = (clocks.registry(v).sources['ettoday'] for v in (21, 22))
    assert ettoday_3c_channel_parser_repair(new, old.data_fingerprint)
    assert not ettoday_3c_channel_parser_repair(new, clocks.registry(20).sources['ettoday'].data_fingerprint)
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_capture_history[-1] == old.data_fingerprint
    for field in ('scope', 'capacity'):
        cfg = clocks.config(22)
        if field == 'scope': cfg['providers'][0]['authorization']['scope'] += '; different scope'
        else: cfg['providers'][0]['interval_seconds'] += 1
        assert not ettoday_3c_channel_parser_repair(PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday'], old.data_fingerprint)
    with pytest.raises(ValueError):
        clocks.registry(clocks.future_version())


@pytest.mark.parametrize('url', ETTODAY_V22_PROVED_3C_CHANNEL_URLS)
@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'lease_owner', 'lease_until', 'has_body', 'has_partition', 'other_url', 'other_failure', 'wrong_error', 'complete'])
def test_only_two_proved_missing_heads_receive_once_only_state_retry(tmp_path, url, excluded):
    old, new = clocks.registry(21), clocks.registry(22)
    if excluded == 'other_url': url = url.replace('.htm', '0.htm')
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
            ('ettoday_3c_channel_recheck_v22:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        assert marker['attempts_due_error_and_observation_preserved']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_3c_channel(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
