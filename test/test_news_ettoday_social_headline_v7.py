"""Social/display labels are distinct; literal current article witnesses remain strict."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_social_headline_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


SOCIAL = '社群使用不同的原始標題：-12.5%、123456股與比率0.75'


def page(parser='lxml', host='finance', witness='social'):
    soup = native.page(host, parser)
    suffix = soup.select_one('meta[property="og:title"]')['content'][len(native.TITLE):]
    soup.select_one('meta[property="og:title"]')['content'] = SOCIAL + suffix
    if witness == 'social':
        native.update_schema(soup, headline=SOCIAL)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', host='finance', version=7):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('witness', ['native', 'social'])
def test_variant_needs_same_article_schema_witness_and_preserves_every_original_field(parser, host, witness):
    soup = page(parser, host, witness)
    original = str(soup)
    result = extract(soup, parser, host)
    baseline = extract(native.page(host, parser), parser, host, 6)
    for key in ('title', 'body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
                'verified_image_captions_json', 'author_attribution_detail_json'):
        assert result[key] == baseline[key]
    assert str(soup) == original and result['title'] == native.TITLE
    notices = json.loads(result['verified_article_notices_json'])
    assert next(n for n in notices if n['kind'] == 'native_variant_current_schema_headline')['matches'] == (
        'native_headline' if witness == 'native' else 'social_title')
    variant = next(n for n in notices if n['kind'].startswith('native_social_headline_differs'))
    assert variant['native_headline'] == native.TITLE
    assert variant['social_title'] == soup.select_one('meta[property="og:title"]')['content']
    assert not variant['source_reason_inferred'] and not variant['source_titles_rewritten']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    for old_version in range(1, 7):
        with pytest.raises(NewsError, match='^article_identity_not_verified$'):
            extract(soup, parser, host, old_version)


@pytest.mark.parametrize('change', [
    'no_schema', 'ambiguous_schema', 'third_headline', 'schema_url', 'schema_mainentity',
    'canonical', 'ogurl', 'missing_og', 'empty_social_prefix', 'unknown_publisher_suffix',
    'schema_body', 'schema_publication', 'schema_modification', 'schema_free_false',
    'unknown_schema_author', 'native_credit', 'hidden_heading', 'hidden_body',
    'multiple_story', 'unknown_section', 'duplicate_og_title',
])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_social_title_does_not_waive_identity_clock_access_author_or_current_body(change, parser):
    soup = page(parser)
    schema = soup.select_one('script[type="application/ld+json"]')
    if change == 'no_schema':
        schema.decompose()
    elif change == 'ambiguous_schema':
        soup.head.append(BeautifulSoup(str(schema), parser).select_one('script'))
    elif change.startswith('schema_') or change == 'third_headline':
        updates = {
            'third_headline': {'headline': '不是原生或社群的第三個標題'},
            'schema_url': {'url': native.URLS[0] + '1'},
            'schema_mainentity': {'mainEntityOfPage': native.URLS[0] + '1'},
            'schema_body': {'articleBody': '不能用JSON正文替代當篇原生正文'},
            'schema_publication': {'datePublished': native.MOD},
            'schema_modification': {'dateModified': native.PUB},
            'schema_free_false': {'isAccessibleForFree': False},
        }
        native.update_schema(soup, **updates[change])
    elif change == 'unknown_schema_author':
        native.update_schema(soup, author={'@type': 'Person', 'name': '未知第三方'})
    elif change == 'canonical':
        soup.select_one('link[rel=canonical]')['href'] += '1'
    elif change == 'ogurl':
        soup.select_one('meta[property="og:url"]')['content'] += '1'
    elif change == 'missing_og':
        soup.select_one('meta[property="og:title"]').decompose()
    elif change == 'empty_social_prefix':
        soup.select_one('meta[property="og:title"]')['content'] = ' | ETtoday財經雲 | ETtoday新聞雲'
    elif change == 'unknown_publisher_suffix':
        soup.select_one('meta[property="og:title"]')['content'] = SOCIAL + ' | 未核實媒體'
    elif change == 'native_credit':
        soup.select_one('#credit').string = '本文來源：未核實第三方'
    elif change == 'hidden_heading':
        soup.select_one('.subject_article h1')['hidden'] = ''
    elif change == 'hidden_body':
        soup.select_one('.story')['style'] = 'display:none'
    elif change == 'multiple_story':
        soup.select_one('.subject_article').append(BeautifulSoup('<div class="story" itemprop="articleBody"></div>', parser).div)
    elif change == 'unknown_section':
        soup.select_one('meta[name=subsection]')['content'] = 'unknown-section'
    elif change == 'duplicate_og_title':
        soup.head.append(BeautifulSoup('<meta property="og:title" content="unknown">', parser).meta)
    else:
        raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_matching_original_headline_has_unchanged_v6_semantics(parser, host):
    soup = native.page(host, parser)
    assert extract(soup, parser, host) == extract(soup, parser, host, 6)


@pytest.mark.parametrize('prior_version', [1, 2, 3, 4, 5, 6])
def test_prior_capture_is_exact_v6_and_capacity_and_authorization_do_not_reset(prior_version):
    old = clocks.registry(prior_version).sources['ettoday']
    new = clocks.registry(7).sources['ettoday']
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert (old.http_capacity_source_fingerprint or old.data_fingerprint) == new.http_capacity_source_fingerprint
    assert ettoday_social_headline_parser_repair(new, old.data_fingerprint) == (prior_version == 6)


@pytest.mark.parametrize('ancestry', ['exact', 'unknown', 'different_scope'])
def test_only_observed_missing_unleased_literal_routes_requeue_once_and_preserve_deadlines(tmp_path, ancestry):
    old = clocks.registry(6, tmp_path)
    new = clocks.registry(7, tmp_path)
    if ancestry == 'unknown':
        prior = old.sources['ettoday']
        old.sources['ettoday'] = replace(prior, data_fingerprint='f' * 64,
            parser_repair_capture_fingerprint='f' * 64)
    elif ancestry == 'different_scope':
        cfg = clocks.config(6)
        cfg['providers'][0]['authorization']['scope'] += '; a different source scope'
        old = type(old)(cfg, base=tmp_path)
    cases = ['eligible', 'complete', 'saved_body', 'saved_path', 'leased_owner', 'leased_until',
        'unobserved', 'bad_clock', 'naive_clock', 'future_clock', 'wrong_error', 'star_host',
        'unknown_path', 'http_403', 'http_429', 'paywall_or_login', 'invalid_bulk_publication_clock',
        'article_attribution_not_verified', 'source_not_found', 'body_not_found', 'retry']
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for index, case in enumerate(cases):
            url = f'https://finance.ettoday.net/news/{7700000 + index}'
            if case == 'star_host':
                url = url.replace('finance.', 'star.')
            elif case == 'unknown_path':
                url += '/unknown'
            queue.add_articles([{'url': url}])
            state = case if case in {'complete', 'http_403', 'http_429', 'paywall_or_login',
                'invalid_bulk_publication_clock', 'article_attribution_not_verified', 'source_not_found',
                'body_not_found', 'retry'} else 'article_identity_not_verified'
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=?,last_checked_at_utc=? WHERE url=?',
                (state, state, 9000000000 + index, utc_now(), url))
            if case == 'saved_body':
                queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a' * 64, url))
            elif case == 'saved_path':
                queue.db.execute("UPDATE articles SET parquet_path='old.parquet' WHERE url=?", (url,))
            elif case in {'leased_owner', 'leased_until'}:
                queue.db.execute('UPDATE articles SET ' + ('lease_owner=?' if case == 'leased_owner' else 'lease_until=?') + ' WHERE url=?',
                    ('owner' if case == 'leased_owner' else 9000000000, url))
            elif case in {'unobserved', 'bad_clock', 'naive_clock', 'future_clock'}:
                clock = {'unobserved': None, 'bad_clock': 'unknown', 'naive_clock': '2026-01-01T00:00:00',
                    'future_clock': (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}[case]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (clock, url))
            elif case == 'wrong_error':
                queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (url,))
            before[url] = (case, dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()))
        queue.db.commit()
    marker = 'ettoday_social_headline_recheck_v1:' + new.sources['ettoday'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, (case, previous) in before.items():
            expected = {**previous, 'state': 'retry'} if case == 'eligible' and ancestry == 'exact' else previous
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        proof = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if ancestry != 'exact':
            assert proof is None
            return
        journal = json.loads(proof[0])
        assert len(journal['rows']) == 1 and journal['rows'][0]['attempts'] == 4
        assert journal['attempts_due_error_and_observation_preserved'] and not journal['accepted_as_bodies']
        url = next(url for url, (case, _) in before.items() if case == 'eligible')
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
