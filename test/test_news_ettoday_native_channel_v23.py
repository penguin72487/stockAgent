"""Article subsection and social channel are source fields, not join keys."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V23_PROVED_NATIVE_CHANNEL_URLS
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_native_channel_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_3c_channel_v22 as previous


def page(parser='lxml', section='民生消費', channel='消費'):
    soup = native.page('www', parser)
    soup.select_one('meta[name=subsection]')['content'] = section
    soup.select_one('meta[property="og:title"]')['content'] = native.TITLE + f' | ETtoday{channel}新聞 | ETtoday新聞雲'
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=23):
    return extract_article(str(soup).encode(), replace(clocks.registry(version).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('section,channel', [('汽機車', '消費'), ('民生消費', '消費'), ('教育', '生活'),
    ('好康資訊', '消費'), ('美食', '名家'), ('3C', '消費')])
def test_source_properties_are_preserved_independently_not_corrected_or_crosswalked(parser, section, channel):
    soup = page(parser, section, channel)
    original = str(soup)
    actual = extract(soup, parser)
    baseline = extract(native.page('www', parser), parser, 22)
    assert str(soup) == original
    assert {k: v for k, v in actual.items() if k != 'verified_article_notices_json'} == {
        k: v for k, v in baseline.items() if k != 'verified_article_notices_json'}
    notes = json.loads(actual['verified_article_notices_json'])
    label, = [n for n in notes if n['kind'] == 'native_subsection_and_social_channel_labels_distinct']
    assert label == {'kind': 'native_subsection_and_social_channel_labels_distinct',
        'native_subsection': section, 'native_social_channel': channel,
        'native_social_title': native.TITLE + f' | ETtoday{channel}新聞 | ETtoday新聞雲',
        'identity_basis': 'same_literal_canonical_og_url_unique_current_schema_and_visible_reporter_story',
        'source_labels_rewritten': False, 'additional_source_host_admitted': False}
    witness, = [n for n in notes if n['kind'] == 'native_variant_current_schema_headline']
    assert witness['matches'] == 'native_headline' and witness['literal_article_url_and_clocks_verified']
    assert [n for n in notes if n not in (label, witness)] == json.loads(baseline['verified_article_notices_json'])
    assert not actual['historical_point_in_time'] and not actual['per_article_license_verified']


@pytest.mark.parametrize('version', range(1, 23))
def test_old_versions_retain_original_acceptance_and_do_not_gain_new_channel_reading(version):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(), version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('soup_factory', [lambda p: native.page('www', p), previous.page])
def test_existing_accepted_values_and_v22_3c_notices_are_bit_identical(parser, soup_factory):
    soup = soup_factory(parser)
    assert extract(soup, parser, 22) == extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('section,channel', [('3c', '消費'), ('3Ｃ', '消費'), ('', '消費'),
    ('民生消費 ', '消費'), ('民生消費', ''), ('民生消費', '3C'), ('民生消費', '來源' * 9),
    ('民生消費', '消費 News'), ('民生消費', '消費\n'), ('民生消費', '<script>消費</script>')])
def test_unobserved_label_syntax_is_not_fuzzily_fixed(parser, section, channel):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(parser, section, channel), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['missing_schema', 'extra_schema', 'missing_section', 'extra_section',
    'canonical', 'og_url', 'foreign_reporter', 'no_reporter', 'hidden_reporter', 'hidden_body', 'second_story',
    'schema_headline', 'schema_url', 'schema_clock', 'schema_author', 'schema_publisher', 'paid', 'schema_full_body'])
def test_distinct_labels_never_waive_article_identity_source_clocks_visibility_access_or_reporter(parser, change):
    soup = page(parser)
    if change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'extra_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'missing_section': soup.select_one('meta[name=subsection]').decompose()
    elif change == 'extra_section': soup.head.append(deepcopy(soup.select_one('meta[name=subsection]')))
    elif change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '0'
    elif change == 'og_url': soup.select_one('meta[property="og:url"]')['content'] += '0'
    elif change == 'foreign_reporter': soup.select_one('#credit').string = '消費中心／綜合報導'
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
def test_no_finance_or_other_host_scope_extension(parser):
    soup = native.page('finance', parser)
    soup.select_one('meta[property="og:title"]')['content'] = native.TITLE + ' | ETtoday消費新聞 | ETtoday新聞雲'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(soup).encode(), clocks.registry(23).sources['ettoday'], page_url=native.URLS[0])
    with pytest.raises(NewsError):
        extract_article(str(page(parser)).encode(), clocks.registry(23).sources['ettoday'],
            page_url='https://house.ettoday.net/news/3254110')


def test_only_capture_ancestry_changes_without_resetting_source_scope_or_capacity():
    old, new = (clocks.registry(v).sources['ettoday'] for v in (22, 23))
    assert ettoday_native_channel_parser_repair(new, old.data_fingerprint)
    assert not ettoday_native_channel_parser_repair(new, clocks.registry(21).sources['ettoday'].data_fingerprint)
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_capture_history[-1] == old.data_fingerprint
    for field in ('scope', 'capacity'):
        cfg = clocks.config(23)
        if field == 'scope': cfg['providers'][0]['authorization']['scope'] += '; different scope'
        else: cfg['providers'][0]['interval_seconds'] += 1
        assert not ettoday_native_channel_parser_repair(
            PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday'], old.data_fingerprint)
    with pytest.raises(ValueError):
        clocks.registry(clocks.future_version())


@pytest.mark.parametrize('url', ETTODAY_V23_PROVED_NATIVE_CHANNEL_URLS)
@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'lease_owner', 'lease_until', 'has_body', 'has_partition', 'other_url', 'other_failure', 'wrong_error', 'complete'])
def test_only_four_proved_missing_heads_receive_once_only_state_retry(tmp_path, url, excluded):
    old, new = clocks.registry(22), clocks.registry(23)
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
            ('ettoday_native_channel_recheck_v23:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        assert marker['attempts_due_error_and_observation_preserved']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_native_channel(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
