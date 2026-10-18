"""Reviewed house admission is not a brand-wide host or body permission."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_ettoday import ettoday_request_identity, ettoday_source_redirect_verified
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks

URL = 'https://house.ettoday.net/news/3254747'
POLICY = 'https://www.ettoday.net/member/clause_copyright.php'


def config(version=24, reviewed=True):
    cfg = clocks.config(version)
    if reviewed:
        cfg['providers'][0]['reviewed_article_subsites'] = ['house.ettoday.net']
    return cfg


def registry(version=24, reviewed=True):
    return PolicyRegistry(config(version, reviewed), base=Path.cwd())


def page(parser='lxml'):
    soup = native.page('finance', parser)
    soup.select_one('link[rel=canonical]')['href'] = URL
    soup.select_one('meta[property="og:url"]')['content'] = URL
    soup.select_one('meta[property="og:title"]')['content'] = native.TITLE + ' | ETtoday房產雲 | ETtoday新聞雲'
    soup.select_one('meta[name=subsection]')['content'] = '房產'
    soup.select_one('time')['datetime'] = native.PUB
    native.update_schema(soup, url=URL, mainEntityOfPage=URL)
    scope = soup.select_one('.subject_article')
    article = soup.new_tag('article')
    scope.wrap(article)
    note = soup.select_one('p.note')
    link = soup.new_tag('a', href=POLICY)
    link.string = '[ETtoday著作權聲明]'
    note.append(link)
    # Observed house image-only wrapper, not article prose.
    image = soup.new_tag('p')
    strong = soup.new_tag('strong')
    strong.append(soup.new_tag('img', src='//cdn2.ettoday.net/images/1/original.jpg'))
    strong.append(soup.new_tag('br', type='_moz'))
    image.append(strong)
    soup.select_one('.story').append(image)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', source=None):
    return extract_article(str(soup).encode(), replace(source or registry().sources['ettoday'], html_parser=parser),
        page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_house_literal_original_preserves_values_clocks_caption_rights_and_reporter(parser):
    soup = page(parser)
    original = str(soup)
    actual = extract(soup, parser)
    baseline = extract_article(str(native.page('finance', parser)).encode(),
        replace(clocks.registry(23).sources['ettoday'], html_parser=parser), page_url=native.URLS[0])
    assert actual['body'] == baseline['body'] and actual['body_sha256'] == baseline['body_sha256']
    assert actual['authors_json'] == baseline['authors_json']
    assert actual['published_at_raw'] == native.PUB and actual['modified_at_raw'] == native.MOD
    assert json.loads(actual['verified_image_captions_json']) == json.loads(baseline['verified_image_captions_json'])
    assert any(n['kind'] == 'native_article_rights_notice' for n in json.loads(actual['verified_article_notices_json']))
    assert not actual['per_article_license_verified'] and not actual['historical_point_in_time']
    assert original == str(soup)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['canonical', 'og_url', 'section', 'suffix', 'no_schema', 'duplicate_schema',
    'schema_url', 'schema_author', 'schema_clock', 'schema_publisher', 'paid', 'missing_credit', 'foreign_credit',
    'hidden_story', 'hidden_reporter', 'second_story', 'wrong_parent', 'wrong_time', 'short_datetime',
    'missing_rights', 'foreign_rights', 'duplicate_rights', 'unknown_script'])
def test_house_never_waives_original_identity_reporter_source_time_access_or_scope(parser, change):
    soup = page(parser)
    if change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '0'
    elif change == 'og_url': soup.select_one('meta[property="og:url"]')['content'] += '0'
    elif change == 'section': soup.select_one('meta[name=subsection]')['content'] = '財經'
    elif change == 'suffix': soup.select_one('meta[property="og:title"]')['content'] = native.TITLE + ' | ETtoday財經雲 | ETtoday新聞雲'
    elif change == 'no_schema': soup.select_one('#report').decompose()
    elif change == 'duplicate_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'missing_credit': soup.select_one('#credit').decompose()
    elif change == 'foreign_credit': soup.select_one('#credit').string = '中央社／綜合報導'
    elif change == 'hidden_story': soup.select_one('.story')['hidden'] = ''
    elif change == 'hidden_reporter': soup.select_one('#credit')['hidden'] = ''
    elif change == 'second_story': soup.body.append(deepcopy(soup.select_one('.story')))
    elif change == 'wrong_parent': soup.select_one('article').name = 'section'
    elif change == 'wrong_time': soup.select_one('time').string = '2026-10-13 22:19'
    elif change == 'short_datetime': soup.select_one('time')['datetime'] = '31'
    elif change == 'missing_rights': soup.select_one('p.note').decompose()
    elif change == 'foreign_rights': soup.select_one('p.note a')['href'] = 'https://other.example/policy'
    elif change == 'duplicate_rights': soup.select_one('.subject_article').append(deepcopy(soup.select_one('p.note')))
    elif change == 'unknown_script': soup.select_one('.story').append(soup.new_tag('script', src='/unknown.js'))
    else:
        key, value = {'schema_url': ('url', URL + '0'), 'schema_author': ('author', {'@type': 'Person', 'name': '別人'}),
            'schema_clock': ('datePublished', native.MOD), 'schema_publisher': ('publisher', {'@type': 'Organization', 'name': '他社'}),
            'paid': ('isAccessibleForFree', False)}[change]
        native.update_schema(soup, **{key: value})
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('host', ['esg', 'forum', 'star', 'sports', 'travel', 'house.evil'])
def test_no_other_brand_or_same_numeric_id_grants_capture(host):
    assert ettoday_request_identity(URL.replace('house', host), allow_house=True) is None
    with pytest.raises(NewsError):
        extract_article(str(page()).encode(), registry().sources['ettoday'], page_url=URL.replace('house', host))


def test_house_requires_machine_review_flag_and_v24_not_only_domain_or_layout():
    for source in (registry(reviewed=False).sources['ettoday'],
                   replace(clocks.registry(23).sources['ettoday'], reviewed_article_subsites=('house.ettoday.net',))):
        with pytest.raises(NewsError):
            extract(page(), source=source)
    assert ettoday_request_identity(URL) is None
    assert ettoday_request_identity(URL + '?redirect=1', allow_house=True, allow_finance_redirect_query=True) is None
    assert ettoday_request_identity(URL, allow_house=True) == ('house.ettoday.net', '/news/3254747')
    for version in (1, 23, clocks.future_version()):
        with pytest.raises(ValueError): registry(version)


@pytest.mark.parametrize('value', [True, 'house.ettoday.net', ['esg.ettoday.net'], ['house.ettoday.net'] * 2,
    ['house.ettoday.net', 'forum.ettoday.net']])
def test_unreviewed_or_malformed_subsite_config_is_rejected(value):
    cfg = config()
    cfg['providers'][0]['reviewed_article_subsites'] = value
    with pytest.raises(ValueError): PolicyRegistry(cfg, base=Path.cwd())


def test_literal_house_transport_and_connected_observed_redirect_not_alias_guess():
    response = Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode())
    source_registry = registry()
    row = {'id': b'house-fixture', 'url': URL, 'provider': 'ettoday', 'metadata_date_raw': None}
    record = fetch_article(SimpleNamespace(registry=source_registry, get=lambda *a, **kw: response), row)
    assert record['requested_url'] == URL and record['final_url'] == URL
    initial = native.URLS[0]
    hop = RedirectHop(initial, 302, URL, URL, utc_now(), 'a' * 64, 0)
    redirected = replace(response, redirects=(hop,))
    assert not ettoday_source_redirect_verified(initial, redirected)
    assert ettoday_source_redirect_verified(initial, redirected, allow_house=True)
    assert not ettoday_source_redirect_verified(initial, response, allow_house=True)
    assert not ettoday_source_redirect_verified(initial + '0', redirected, allow_house=True)


@pytest.mark.parametrize('excluded', ['none', 'has_body', 'partition', 'future_due', 'unobserved', 'naive',
    'future_observation', 'lease_owner', 'lease_until', 'other_host', 'query', 'other_failure', 'wrong_error', 'no_attempt'])
def test_only_pre_http_missing_house_path_rejections_retry_once_without_resetting_evidence(tmp_path, excluded):
    url = URL.replace('house', 'esg') if excluded == 'other_host' else URL + '?x=1' if excluded == 'query' else URL
    old, new = registry(23, reviewed=False), registry()
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}, {'url': native.URLS[0]}])
        values = {'state': 'unsupported_article_path', 'error_code': 'unsupported_article_path', 'attempts': 3,
            'due': 99, 'last_checked_at_utc': utc_now()}
        if excluded == 'has_body': values['body_sha256'] = 'a' * 64
        elif excluded == 'partition': values['parquet_path'] = 'source.parquet'
        elif excluded == 'future_due': values['due'] = (datetime.now(timezone.utc) + timedelta(days=1)).timestamp()
        elif excluded == 'unobserved': values['last_checked_at_utc'] = None
        elif excluded == 'naive': values['last_checked_at_utc'] = '2026-10-18T00:00:00'
        elif excluded == 'future_observation': values['last_checked_at_utc'] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        elif excluded == 'lease_owner': values['lease_owner'] = 'other_owner'
        elif excluded == 'lease_until': values['lease_until'] = 1
        elif excluded == 'other_failure': values['state'] = values['error_code'] = 'http_403'
        elif excluded == 'wrong_error': values['error_code'] = 'http_403'
        elif excluded == 'no_attempt': values['attempts'] = 0
        with queue.db:
            queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in values) + ' WHERE url=?', (*values.values(), url))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after == {u: {**row, 'state': 'retry' if excluded == 'none' and u == url else row['state']}
            for u, row in before.items()}
        marker = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('ettoday_house_path_recheck_v24:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        assert marker['attempts_due_error_and_observation_preserved']
        with queue.db: queue.db.execute("UPDATE articles SET state='unsupported_article_path' WHERE url=?", (url,))
        queue.recheck_ettoday_house_paths(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'unsupported_article_path'


@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_previously_accepted_extractor_fields_are_identical_not_reinterpreted(host, parser):
    raw = str(native.page(host, parser)).encode()
    old = extract_article(raw, replace(clocks.registry(23).sources['ettoday'], html_parser=parser), page_url=native.URLS[host == 'www'])
    new = extract_article(raw, replace(registry().sources['ettoday'], html_parser=parser), page_url=native.URLS[host == 'www'])
    assert old == new
