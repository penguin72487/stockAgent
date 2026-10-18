"""Observed direct BR prefix repair, not inferred attribution or prose slicing."""
from dataclasses import replace
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import _caption_break_credit
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_break_credit_parser_repair, utc_now
from downloader.news_fulltext_ettoday import ETTODAY_V8_PROVED_BREAK_CREDIT_URLS
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_social_headline_v7 as social


CAPTION = '▲原始圖片出處與數值123456股。'
CREDIT = '記者研究記者／台北報導'


def page(parser='lxml', host='finance'):
    soup = native.page(host, parser)
    story = soup.select_one('.story')
    # Keep the source image, replace only the synthetic caption/credit/prose.
    for node in list(story.find_all('p', recursive=False))[1:]:
        node.decompose()
    combined = BeautifulSoup('<p id="combined">' + CAPTION + '<br><br>' + CREDIT
        + '<br><br>' + native.PROSE + '<br><a href="/raw">-12.5%、123456股與0.75</a>'
        + '<br>原始尾段，不得遺漏。</p>', parser).find('p')
    story.find('p').insert_after(combined)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', host='finance', version=8):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_only_native_prefix_removed_and_all_prose_source_values_preserved(parser):
    soup = page(parser)
    original = str(soup)
    result = extract(soup, parser)
    assert str(soup) == original
    assert native.PROSE in result['body'] and '原始尾段，不得遺漏。' in result['body']
    assert result['body'].index(native.PROSE) < result['body'].index('原始尾段，不得遺漏。')
    assert '-12.5%、123456股與0.75' in result['body']
    assert CREDIT not in result['body'] and CAPTION not in result['body']
    assert json.loads(result['verified_image_captions_json']) == [CAPTION]
    assert json.loads(result['authors_json']) == ['研究記者']
    assert json.loads(result['author_attribution_detail_json'])[0]['native_credit'] == CREDIT
    assert (result['published_at_raw'], result['modified_at_raw']) == (native.PUB, native.MOD)
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    node = soup.select_one('#combined')
    old_suffix = str(node).split(CREDIT + '<br/><br/>', 1)[1]
    names, credit, caption = _caption_break_credit(node)
    assert str(node) == '<p id="combined"><br/>' + old_suffix
    assert (names, credit, caption) == (['研究記者'], CREDIT, CAPTION)


@pytest.mark.parametrize('version', range(1, 8))
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_old_versions_do_not_silently_change_acceptance(version, parser):
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(page(parser), parser, version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('variant', ['native', 'social'])
def test_already_verified_v7_layouts_have_identical_v8_output(parser, host, variant):
    soup = native.page(host, parser) if variant == 'native' else social.page(parser, host)
    assert extract(soup, parser, host, 7) == extract(soup, parser, host, 8)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['www', 'no_image', 'no_caption', 'prior_prose', 'duplicate',
    'embedded', 'wire', 'hidden', 'nested_br', 'block', 'script', 'two_captions', 'no_prose',
    'tagged_credit', 'identity', 'clock', 'duplicate_names'])
def test_unreviewed_attribution_visibility_scope_identity_and_clocks_stay_rejected(parser, case):
    host = 'www' if case == 'www' else 'finance'
    soup = page(parser, host)
    node = soup.select_one('#combined')
    text = str(node)
    if case == 'no_image':
        soup.select_one('.story img').decompose()
    elif case == 'no_caption':
        text = text.replace(CAPTION, '這是署名前的正文。')
    elif case == 'prior_prose':
        text = text.replace(CREDIT, '這是署名前的正文。<br>' + CREDIT)
    elif case == 'duplicate':
        text = text.replace(native.PROSE, CREDIT + '<br>' + native.PROSE)
    elif case == 'embedded':
        text = text.replace(CREDIT, CREDIT + '：與正文不能拼成署名')
    elif case == 'wire':
        text = text.replace(native.PROSE, '來源：中央社<br>' + native.PROSE)
    elif case == 'hidden':
        text = text.replace('id="combined"', 'id="combined" style="display:none"')
    elif case == 'nested_br':
        text = text.replace(CREDIT, '<span>' + CREDIT + '<br></span>')
    elif case == 'block':
        text = text.replace(native.PROSE, '<div>' + native.PROSE + '</div>')
    elif case == 'script':
        text = text.replace(native.PROSE, '<script>not_body</script>' + native.PROSE)
    elif case == 'two_captions':
        text = text.replace(CREDIT, '▲另一段圖說<br>' + CREDIT)
    elif case == 'no_prose':
        text = '<p id="combined">' + CAPTION + '<br>' + CREDIT + '</p>'
    elif case == 'tagged_credit':
        text = text.replace(CREDIT, '<strong>' + CREDIT + '</strong>')
    elif case == 'identity':
        soup.select_one('meta[property="og:url"]')['content'] += '0'
    elif case == 'clock':
        soup.select_one('meta[name="pubdate"]')['content'] = native.MOD
    elif case == 'duplicate_names':
        text = text.replace(CREDIT, '記者研究記者、研究記者／台北報導')
    node.replace_with(BeautifulSoup(text, parser).find('p'))
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser, host)


@pytest.mark.parametrize('prior_version', range(1, 8))
def test_only_capture_version_changes_not_authorization_capacity_or_old_parsers(prior_version):
    old, new = clocks.registry(prior_version).sources['ettoday'], clocks.registry(8).sources['ettoday']
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert (old.http_capacity_source_fingerprint or old.data_fingerprint) == new.http_capacity_source_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == clocks.registry(7).sources['ettoday'].data_fingerprint
    assert ettoday_break_credit_parser_repair(new, old.data_fingerprint) == (prior_version == 7)


@pytest.mark.parametrize('ancestry', ['exact', 'unknown', 'different_scope'])
@pytest.mark.parametrize('case', ['eligible', 'complete', 'saved_body', 'saved_path', 'leased_owner',
    'leased_until', 'unobserved', 'bad_clock', 'naive_clock', 'future_clock', 'wrong_error',
    'http_403', 'http_429', 'paywall_or_login', 'invalid_bulk_publication_clock',
    'article_identity_not_verified', 'retry'])
def test_one_proved_url_only_preserves_complete_row_and_is_one_time(tmp_path, ancestry, case):
    old, new = clocks.registry(7, tmp_path), clocks.registry(8, tmp_path)
    if ancestry == 'unknown':
        original = old.sources['ettoday']
        old.sources['ettoday'] = replace(original, data_fingerprint='f' * 64,
            parser_repair_capture_fingerprint='f' * 64)
    elif ancestry == 'different_scope':
        cfg = clocks.config(7)
        cfg['providers'][0]['authorization']['scope'] += '; changed grant'
        old = type(old)(cfg, base=tmp_path)
    url = ETTODAY_V8_PROVED_BREAK_CREDIT_URLS[0]
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}, {'url': 'https://finance.ettoday.net/news/3254171'}])
        queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',error_code=state,"
            'attempts=4,due=9000000000,last_checked_at_utc=?', (utc_now(),))
        queue.db.execute("UPDATE articles SET error_code='article_attribution_not_verified'")
        if case in {'complete', 'http_403', 'http_429', 'paywall_or_login',
                'invalid_bulk_publication_clock', 'article_identity_not_verified', 'retry'}:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (case, case, url))
        elif case == 'saved_body':
            queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a' * 64, url))
        elif case == 'saved_path':
            queue.db.execute("UPDATE articles SET parquet_path='old.parquet' WHERE url=?", (url,))
        elif case in {'leased_owner', 'leased_until'}:
            field, value = ('lease_owner', 'owner') if case == 'leased_owner' else ('lease_until', 9000000000)
            queue.db.execute('UPDATE articles SET ' + field + '=? WHERE url=?', (value, url))
        elif case in {'unobserved', 'bad_clock', 'naive_clock', 'future_clock'}:
            value = {'unobserved': None, 'bad_clock': 'unknown', 'naive_clock': '2026-01-01T00:00:00',
                'future_clock': (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}[case]
            queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, url))
        elif case == 'wrong_error':
            queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (url,))
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        queue.db.commit()
    marker = 'ettoday_break_credit_recheck_v8:' + new.sources['ettoday'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for item, previous in before.items():
            expected = {**previous, 'state': 'retry'} if item == url and case == 'eligible' and ancestry == 'exact' else previous
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (item,)).fetchone()) == expected
        proof = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if ancestry != 'exact':
            assert proof is None
            return
        journal = json.loads(proof[0])
        assert len(journal['rows']) == int(case == 'eligible')
        assert journal['attempts_due_error_and_observation_preserved'] and not journal['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='article_attribution_not_verified' WHERE url=?", (url,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_attribution_not_verified'
