"""Witnessed calendar-label grammars are not publication clock substitutions."""
from contextlib import closing
from pathlib import Path
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, pocket_native_parser_repair, utc_now
from test_news_pocket_original import (settings, page, parse, versioned,
    POCKET_AUTHOR, POCKET_ORIGIN, RELEASE, REVISION)


def labeled(value):
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('.article-meta').string = value
    return str(soup)


FORMS = [
    ('更新日期：2023年2月15日 '+POCKET_AUTHOR, 'update_date', '2023-02-15'),
    ('資料截至 2023 年 2 月 15 日收盤 ｜ '+POCKET_AUTHOR, 'data_as_of_close_date', '2023-02-15'),
    ('更新於 2023/02/15 ｜ '+POCKET_AUTHOR, 'update_date', '2023-02-15'),
    ('資料截至 2023-02-15', 'data_as_of_date', '2023-02-15'),
    ('口袋觀點｜資料截至 2023 年 2 月 15 日', 'data_as_of_date', '2023-02-15'),
    ('更新於 2023-02-14 ｜ '+POCKET_AUTHOR, 'update_date', '2023-02-14'),
]


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('label,role,date', FORMS)
def test_native_calendar_roles_preserve_body_and_schema_clocks(settings, engine, label, role, date):
    config = versioned(settings, 3)
    config['providers'][0]['html_parser'] = engine
    article = parse(config, labeled(label))
    baseline = parse(versioned(settings, 2))
    assert article['body'] == baseline['body'] and article['body_sha256'] == baseline['body_sha256']
    assert article['published_at_raw'] == RELEASE and article['modified_at_raw'] == REVISION
    notice = next(n for n in json.loads(article['verified_article_notices_json'])
                  if n['kind'] == 'native_inner_calendar_label')
    assert notice['text'] == label and notice['clock_role'] == role and notice['calendar_date'] == date
    assert notice['not_promoted_to_release_or_revision_clock']
    assert notice['close_session_does_not_supply_a_wall_clock_time'] == (role == 'data_as_of_close_date')
    assert notice['matches_outer_visible_update_day'] == (date == '2023-02-15')
    assert not article['historical_point_in_time'] and not article['per_article_license_verified']
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(versioned(settings, 2), labeled(label))


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('nested', [False, True])
def test_all_accepted_v2_outputs_are_unchanged(settings, engine, nested):
    old, new = versioned(settings, 2), versioned(settings, 3)
    old['providers'][0]['html_parser'] = new['providers'][0]['html_parser'] = engine
    assert parse(old, page(nested=nested)) == parse(new, page(nested=nested))


@pytest.mark.parametrize('label', [
    '資料截至 2023-02-30', '資料截至 2023-00-15', '資料截至 0000-02-15',
    '更新日期：2023年2月29日 '+POCKET_AUTHOR,
    '資料截至 2023 年 13 月 15 日收盤 ｜ '+POCKET_AUTHOR,
    '更新於 2023/2/15 ｜ '+POCKET_AUTHOR,
    '更新於 2023/02-15 ｜ '+POCKET_AUTHOR,
    '資料截至 2023-02-15 另有未核對正文',
    '資料截至 2023 年 2 月 15 日收盤 ｜ 外部投稿',
    '更新日期：2023年2月15日 未核對作者',
    '資料截至昨天', '資料截至 2023-02-15T13:30:00+08:00',
])
def test_unknown_or_invalid_calendar_labels_are_not_discarded_or_inferred(settings, label):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(versioned(settings, 3), labeled(label))


@pytest.mark.parametrize('attribute,value', [('hidden',''), ('aria-hidden','true'), ('style','display:none')])
def test_hidden_inner_label_cannot_supply_an_observation(settings, attribute, value):
    soup = BeautifulSoup(labeled(FORMS[0][0]), 'lxml')
    soup.select_one('.article-meta')[attribute] = value
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 3), str(soup))


def test_as_of_period_different_from_release_is_retained_not_promoted(settings):
    label = '資料截至 2022-12-30'
    article = parse(versioned(settings, 3), labeled(label))
    notice = next(n for n in json.loads(article['verified_article_notices_json'])
                  if n['kind'] == 'native_inner_calendar_label')
    assert notice['calendar_date'] == '2022-12-30' and not notice['matches_outer_visible_update_day']
    assert notice['not_promoted_to_release_or_revision_clock']
    assert article['published_at_raw'] == RELEASE and article['modified_at_raw'] == REVISION


def test_previously_accepted_repeated_label_keeps_exact_v2_output(settings):
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('.article-meta')['style'] = 'display:none'
    assert parse(versioned(settings, 2), str(soup)) == parse(versioned(settings, 3), str(soup))


def test_new_hidden_label_child_is_not_promoted_to_visible_fact(settings):
    soup = BeautifulSoup(page(), 'lxml')
    meta = soup.select_one('.article-meta')
    meta.clear()
    child = soup.new_tag('span', attrs={'aria-hidden':'true'})
    child.string = FORMS[0][0]
    meta.append(child)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 3), str(soup))


def test_v3_predecessor_and_transport_identity_remain_separate(settings):
    old = PolicyRegistry(versioned(settings, 2), base=Path.cwd()).sources['pocket']
    original = PolicyRegistry(settings, base=Path.cwd()).sources['pocket']
    new = PolicyRegistry(versioned(settings, 3), base=Path.cwd()).sources['pocket']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == original.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert pocket_native_parser_repair(new, old.data_fingerprint)
    assert not pocket_native_parser_repair(new, original.data_fingerprint)
    changed = versioned(settings, 3)
    changed['providers'][0]['authorization']['scope'] += '; changed'
    assert not pocket_native_parser_repair(PolicyRegistry(changed,base=Path.cwd()).sources['pocket'], old.data_fingerprint)


@pytest.mark.parametrize('obstruction', [None, 'stored', 'lease', 'missing_observation', 'wrong_predecessor'])
def test_v3_retries_only_unstored_clock_failures_once_not_identity_or_permissions(settings, tmp_path, obstruction):
    old = PolicyRegistry(versioned(settings, 2), base=tmp_path)
    new = PolicyRegistry(versioned(settings, 3), base=tmp_path)
    states = ['invalid_bulk_publication_clock', 'article_identity_not_verified',
              'article_rights_or_scope_excluded', 'http_403', 'http_429', 'complete', 'unsupported_article_path']
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for i, state in enumerate(states, 1):
            url = POCKET_ORIGIN+f'/school/articles/{i}/'
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,last_checked_at_utc=? WHERE url=?',
                             (state,state,utc_now(),url))
            if obstruction == 'stored':
                queue.db.execute("UPDATE articles SET body_sha256='existing-body' WHERE url=?", (url,))
            elif obstruction == 'lease':
                queue.db.execute("UPDATE articles SET lease_owner='other-owner' WHERE url=?", (url,))
            elif obstruction == 'missing_observation':
                queue.db.execute('UPDATE articles SET last_checked_at_utc=NULL WHERE url=?', (url,))
            before[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        if obstruction == 'wrong_predecessor':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:pocket'", ('f'*64,))
        queue.db.commit()
    marker = 'pocket_native_parser_recheck_v3:'+new.sources['pocket'].data_fingerprint
    with closing(NewsQueue(tmp_path,new)) as queue:
        for url,row in before.items():
            eligible = obstruction is None and row['state'] == 'invalid_bulk_publication_clock'
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == (
                {**row,'state':'retry'} if eligible else row)
        evidence = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        assert (evidence is None) == (obstruction == 'wrong_predecessor')
        if evidence:
            assert len(json.loads(evidence[0])['rows']) == (1 if obstruction is None else 0)
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock' WHERE state='retry'")
        queue.db.commit()
    with closing(NewsQueue(tmp_path,new)) as queue:
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='retry'").fetchone()[0] == 0
