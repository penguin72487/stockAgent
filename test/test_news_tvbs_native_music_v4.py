"""Source-declared two-level category, exact ancestry, no broad failure replay."""
from contextlib import closing
import json
from pathlib import Path
import re

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, tvbs_native_v4_parser_repair, utc_now
from downloader.news_fulltext_tvbs import (
    TVBS_HOSTS, TVBS_V4_PATH_PATTERN, TVBS_V4_PROVED_MUSIC_URLS,
    tvbs_request_identity, tvbs_validation_metadata,
)
import test_news_tvbs as native
import test_news_tvbs_native_v2 as v2

DECLARED = 'https://news.tvbs.com.tw/entertainment/music/4035845'
REQUESTED = 'https://news.tvbs.com.tw/entertainment/4035845'


def registry(version=4, parser='lxml'):
    entry = native.recipe(parser)
    entry['embedded_article_layout'] = f'tvbs_visible_article_v{version}'
    if version == 4:
        entry['article_path_pattern'] = TVBS_V4_PATH_PATTERN
    return PolicyRegistry(v2.config(entry), base=Path.cwd())


def page(parser='lxml'):
    value = str(native.page(parser=parser)).replace(native.URLS[0], DECLARED)
    value = value.replace('%2Fmoney%2F4035845', '%2Fentertainment%2Fmusic%2F4035845')
    return BeautifulSoup(value, parser)


def extract(soup, *, version=4, parser='lxml', url=REQUESTED):
    return extract_article(str(soup).encode(), registry(version, parser).sources['tvbs'], page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('direct', [False, True])
def test_native_music_canonical_keeps_whole_report_clocks_and_declared_url(parser, direct):
    soup = page(parser)
    original = str(soup)
    result = extract(soup, parser=parser, url=DECLARED if direct else REQUESTED)
    assert result['title'] == native.TITLE
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert json.loads(result['authors_json']) == ['研究記者']
    assert result['published_at_raw'] == '2026-10-14T08:29:26.000Z'
    assert result['modified_at_raw'] == '2026-10-14T08:45:53.000Z'
    assert str(soup) == original
    facts = tvbs_validation_metadata(soup, page_url=DECLARED if direct else REQUESTED, adapter_contract_version=4)
    assert facts['schema_valid'] and not facts['raw_scope_or_article_acceptance_granted']
    if not direct:
        notice = next(n for n in json.loads(result['verified_article_notices_json']) if n['kind']=='native_same_story_category_canonical')
        assert notice['response_url'] == REQUESTED and notice['declared_canonical'] == DECLARED
        assert not notice['alternate_url_requested']
    for old in (1, 2, 3):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract(soup, version=old, parser=parser, url=DECLARED if direct else REQUESTED)


@pytest.mark.parametrize('change', ['different_id', 'another_nested_category', 'third_level', 'foreign_host',
    'cars_host', 'canonical_og_conflict', 'canonical_schema_conflict', 'http_canonical', 'query_canonical',
    'duplicate_canonical', 'hidden_headline', 'missing_reporter', 'external_reporter', 'hidden_body', 'headline_conflict'])
def test_nested_route_never_replaces_native_identity_visibility_or_author_scope(change):
    soup = page()
    canonical = soup.select_one('link[rel~="canonical"]')
    url = REQUESTED
    if change == 'different_id':
        url = url.replace('4035845', '4035846')
    elif change in {'another_nested_category', 'third_level', 'foreign_host', 'cars_host', 'http_canonical', 'query_canonical'}:
        replacements = {'another_nested_category': DECLARED.replace('/music/', '/unknown/'),
            'third_level': DECLARED.replace('/music/', '/music/archive/'),
            'foreign_host': DECLARED.replace('news.tvbs.com.tw', 'foreign.invalid'),
            'cars_host': DECLARED.replace('news.tvbs.com.tw', 'cars.tvbs.com.tw'),
            'http_canonical': DECLARED.replace('https:', 'http:'), 'query_canonical': DECLARED+'?from=known'}
        canonical['href'] = replacements[change]
    elif change == 'canonical_og_conflict':
        soup.select_one('meta[property="og:url"]')['content'] = REQUESTED
    elif change == 'canonical_schema_conflict':
        v2.update_schema(soup, mainEntityOfPage={'@type':'WebPage','@id':REQUESTED})
    elif change == 'duplicate_canonical':
        soup.head.append(BeautifulSoup(str(canonical), 'lxml').link)
    elif change == 'hidden_headline':
        soup.h1['hidden'] = ''
    elif change in {'missing_reporter', 'external_reporter'}:
        for label in soup.select('header ul > li:first-child'):
            label.string = '' if change == 'missing_reporter' else '記者：中央社'
    elif change == 'hidden_body':
        soup.select_one('.article-editor-content')['hidden'] = ''
    else:
        soup.h1.string = '鄰篇報導不是本篇原文'
    with pytest.raises(NewsError):
        extract(soup, url=url)


@pytest.mark.parametrize('host', TVBS_HOSTS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_existing_three_host_all_fields_stay_equal_to_v3(host, parser):
    soup = native.page(host, parser=parser)
    url = native.URLS[TVBS_HOSTS.index(host)]
    assert extract(soup, version=3, parser=parser, url=url) == extract(soup, parser=parser, url=url)


def test_exact_v3_capture_and_original_http_identity_despite_native_path_admission():
    old, new = registry(3).sources['tvbs'], registry().sources['tvbs']
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint != new.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == registry(1).sources['tvbs'].data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert tvbs_native_v4_parser_repair(new, old.data_fingerprint)
    assert not tvbs_native_v4_parser_repair(new, registry(2).sources['tvbs'].data_fingerprint)
    assert re.fullmatch(new.article_path_pattern, '/entertainment/music/4035845')
    assert tvbs_request_identity(DECLARED, adapter_contract_version=4) == ('news.tvbs.com.tw','/entertainment/music/4035845')
    assert tvbs_request_identity(DECLARED, adapter_contract_version=3) is None
    assert tvbs_request_identity(DECLARED.replace('news.', 'cars.'), adapter_contract_version=4) is None


def test_only_due_proved_missing_head_retries_once_without_erasing_evidence(tmp_path):
    url = TVBS_V4_PROVED_MUSIC_URLS[0]
    other = 'https://news.tvbs.com.tw/money/123'
    old, new = registry(3), registry()
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}, {'url': other}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=7,due=0,last_checked_at_utc=?", (utc_now(),))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_test:tvbs:spent','11')")
        queue.db.commit()
        before = {row['url']:dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = {row['url']:dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert after == {key: {**row,'state':'retry'} if key==url else row for key,row in before.items()}
        marker = 'tvbs_native_music_recheck_v4:' + new.sources['tvbs'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',(marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert [row['url'] for row in proof['rows']] == [url] and not proof['accepted_as_bodies']
        assert all(before[url][key] == value for key,value in proof['rows'][0].items())
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_test:tvbs:spent'").fetchone()[0] == '11'
        queue.db.execute('UPDATE articles SET state=error_code WHERE url=?',(url,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone()) == before[url]
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',(marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['saved_body','saved_parquet','lease_owner','lease_until','future_due',
    'different_state','different_error','missing_clock','naive_clock','future_clock','bad_clock','unknown_prior','older_prior','changed_grant'])
def test_proved_url_never_retries_changed_saved_refused_unobserved_or_not_due_head(tmp_path,boundary):
    old,new = registry(3),registry()
    url = TVBS_V4_PROVED_MUSIC_URLS[0]
    with closing(NewsQueue(tmp_path,old)) as queue:
        queue.add_articles([{'url':url}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=6,due=0,last_checked_at_utc=?",(utc_now(),))
        changes = {'saved_body':('body_sha256','b'*64),'saved_parquet':('parquet_path','unchanged.parquet'),
            'lease_owner':('lease_owner','another-owner'),'lease_until':('lease_until',9000000000),
            'future_due':('due',9000000000),'different_state':('state','http_403'),
            'different_error':('error_code','http_429'),'missing_clock':('last_checked_at_utc',None),
            'naive_clock':('last_checked_at_utc','2026-10-18T00:00:00'),
            'future_clock':('last_checked_at_utc','2299-01-01T00:00:00+00:00'),
            'bad_clock':('last_checked_at_utc','invalid')}
        if boundary in changes:
            column,value = changes[boundary]
            queue.db.execute(f'UPDATE articles SET {column}=? WHERE url=?',(value,url))
        elif boundary in {'unknown_prior','older_prior'}:
            prior = 'f'*64 if boundary=='unknown_prior' else registry(2).sources['tvbs'].data_fingerprint
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:tvbs'",(prior,))
        else:
            entry=native.recipe()
            entry.update(embedded_article_layout='tvbs_visible_article_v4',article_path_pattern=TVBS_V4_PATH_PATTERN)
            entry['authorization']={**entry['authorization'],'reviewed_on':'2026-10-18'}
            new=PolicyRegistry(v2.config(entry),base=Path.cwd())
        queue.db.commit()
        before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
    with closing(NewsQueue(tmp_path,new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone()) == before
