"""Native title whitespace and leading-image credits, without widening rights."""
from copy import deepcopy
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ftnn import ftnn_visible_article
from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_capacity import PublisherCapacity
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, ftnn_native_v2_parser_repair, utc_now, verify_bodies
from test_news_ftnn import URL, TITLE, PROSE, NOTICE, page, policy_config, update_schema
from test_news_ftnn_release_clocks import revised_page
from test_news_fulltext import FakeResponse, http_fixture


def candidate_config(parser='lxml'):
    cfg = policy_config(parser)
    cfg['providers'][0]['embedded_article_layout'] = 'ftnn_visible_article_v2'
    return cfg


def candidate_policy(parser='lxml'):
    return PolicyRegistry(candidate_config(parser), base=Path.cwd()).sources['ftnn']


def extract(soup, parser='lxml'):
    return extract_article(str(soup).encode(), candidate_policy(parser), page_url=URL)


def breadcrumb(soup, **changes):
    script = soup.select('script[type="application/ld+json"]')[1]
    declared = json.loads(script.string)
    declared['itemListElement'][-1]['item'].update(changes)
    script.string = json.dumps(declared, ensure_ascii=False)


def leading_photo(soup):
    body = soup.select_one('.news-body')
    photo = body.select_one('figure.image')
    body.insert(0, photo.extract())
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('space', [' ', '　', '\r\n', '\t', '\u00a0'])
def test_outer_whitespace_is_only_identity_formatting_not_changed_title_or_body(parser, space):
    soup = page(parser)
    breadcrumb(soup, name=space + TITLE + space)
    update_schema(soup, 'headline', space + TITLE + space)
    before = str(soup)
    result = extract(soup, parser)
    assert result['title'] == TITLE
    assert PROSE in result['body'] and NOTICE in result['body']
    assert 'outer_whitespace_breadcrumb' in result['article_identity_basis']
    assert str(soup) == before
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('changes', [
    {'name': TITLE + '!'}, {'name': TITLE.replace('股數', '股 數')}, {'name': None}, {'name': ''},
    {'@id': URL + '0'}, {'@id': URL + '?x=1'}, {'@id': {'@id': URL}},
    {'name': TITLE, 'unreviewed': 'extra identity'},
])
def test_outer_whitespace_does_not_ignore_changed_inner_title_url_type_or_keys(changes):
    soup = page()
    breadcrumb(soup, **changes)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('intern,dual_clock', [(False, False), (True, False), (False, True), (True, True)])
def test_first_native_photo_does_not_hide_whole_report_credit_caption_or_clock(parser, intern, dual_clock):
    soup = revised_page(parser) if dual_clock else page(parser)
    leading_photo(soup)
    if intern:
        soup.select_one('.news-body > p:first-of-type').string = '實習記者黃詩雯／綜合報導'
    before = str(soup)
    result = extract(soup, parser)
    assert result['authors_json'] == '["黃詩雯"]'
    assert PROSE in result['body'] and NOTICE in result['body']
    assert '記者黃詩雯' not in result['body'] and '獨立照片說明' not in result['body']
    assert json.loads(result['verified_image_captions_json']) == ['獨立照片說明／原始出處']
    assert json.loads(result['author_attribution_detail_json'])['native_byline'].startswith('實習記者' if intern else '記者')
    assert result['published_at_raw'] == ('2024-07-02T08:51:00+08:00' if dual_clock else '2026-10-14T22:00:15+08:00')
    assert str(soup) == before


@pytest.mark.parametrize('credit', ['黃詩雯', '中央社記者黃詩雯', '編譯黃詩雯', '黃詩雯表示',
    '記者其他作者', '記者黃詩雯表示', '記者黃詩雯／不明來源', '特約記者黃詩雯', '記者 黃詩雯'])
def test_unproved_credit_variants_quotes_organizations_or_unknown_roles_are_not_accepted(credit):
    soup = leading_photo(page())
    soup.select_one('.news-body > p:first-of-type').string = credit
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


@pytest.mark.parametrize('change', ['two_photos', 'nonimage_figure', 'missing_image', 'figure_prose',
    'unknown_prefix', 'literal_prefix', 'reporter_in_later_paragraph', 'hidden_photo', 'hidden_credit',
    'script', 'video', 'third_party', 'schema_author', 'foreign_account', 'embedded_license'])
def test_new_layout_never_uses_a_later_quote_unknown_prefix_or_unauthorized_credit(change):
    soup = leading_photo(page())
    photo = soup.select_one('figure.image')
    body = soup.select_one('.news-body')
    credit = body.select_one(':scope > p:first-of-type')
    if change == 'two_photos':
        photo.insert_after(deepcopy(photo))
    elif change == 'nonimage_figure':
        photo['class'] = ['other']
    elif change == 'missing_image':
        photo.find('img').decompose()
    elif change == 'figure_prose':
        photo.append('不得被省略的未知正文')
    elif change == 'unknown_prefix':
        body.insert(0, soup.new_tag('div'))
    elif change == 'literal_prefix':
        body.insert(0, '不得被省略的前文')
    elif change == 'reporter_in_later_paragraph':
        credit.insert_before(BeautifulSoup('<p>這段前文不可誤當署名或刪除</p>', 'html.parser').p)
    elif change == 'hidden_photo':
        photo['hidden'] = ''
    elif change == 'hidden_credit':
        credit['aria-hidden'] = 'true'
    elif change in ('script', 'video', 'embedded_license'):
        node = soup.new_tag({'script': 'script', 'video': 'iframe', 'embedded_license': 'a'}[change])
        if change == 'embedded_license':
            node['rel'] = 'license'
        photo.append(node)
    elif change == 'third_party':
        credit.string = '中央社記者黃詩雯／綜合報導'
    elif change == 'schema_author':
        update_schema(soup, 'author', {'@type': 'Person', 'name': '其他作者', 'url': 'https://www.ftnn.com.tw/search?author=a1'})
    else:
        update_schema(soup, 'author', {'@type': 'Person', 'name': '黃詩雯', 'url': 'https://foreign.invalid/author'})
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('change', ['canonical', 'paid', 'clock', 'hidden_body', 'duplicate_schema'])
def test_title_and_credit_repairs_do_not_waive_other_existing_boundaries(change):
    soup = leading_photo(page())
    breadcrumb(soup, name=TITLE + '　')
    if change == 'canonical':
        soup.select_one('link[rel=canonical]')['href'] = URL + '0'
    elif change == 'paid':
        update_schema(soup, 'isAccessibleForFree', False)
    elif change == 'clock':
        update_schema(soup, 'datePublished', '2026-10-14T22:02:00+08:00')
    elif change == 'hidden_body':
        soup.select_one('.news-body')['style'] = 'display:none'
    else:
        script = soup.select_one('script')
        script.insert_after(deepcopy(script))
    with pytest.raises(NewsError):
        extract(soup)


def test_versioned_capture_changes_but_original_scope_authorization_and_capacity_do_not():
    cfg = policy_config()
    old = PolicyRegistry(cfg, base=Path.cwd()).sources['ftnn']
    new = candidate_policy()
    assert old.data_fingerprint != new.data_fingerprint
    assert old.data_fingerprint == new.parser_repair_prior_capture_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint


def test_installed_source_cutover_changes_only_ftnn_capture_not_any_authorization_or_http(monkeypatch):
    import test_news_fulltext as shared
    config, baseline, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    for entry in candidate['providers']:
        if entry['id'] == 'ftnn':
            entry['embedded_article_layout'] = 'ftnn_visible_article_v2'
    registry = PolicyRegistry(candidate, base=repo)
    for provider, previous in baseline.sources.items():
        current = registry.sources[provider]
        assert previous.authorization_fingerprint == current.authorization_fingerprint
        assert previous.http_capacity_source_fingerprint == current.http_capacity_source_fingerprint
        if provider != 'ftnn':
            assert previous.data_fingerprint == current.data_fingerprint
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def test_old_adapter_remains_reproducible_and_dual_parser_values_match():
    soup = leading_photo(page())
    breadcrumb(soup, name=TITLE + '　')
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), PolicyRegistry(policy_config(), base=Path.cwd()).sources['ftnn'], page_url=URL)
    values = [extract(soup, parser) for parser in ('lxml', 'html.parser')]
    assert values[0] == values[1]


@pytest.mark.parametrize('version', [0, 7, None, True, '2'])
def test_unknown_adapter_contract_is_not_a_permissive_alias(version):
    with pytest.raises(NewsError):
        ftnn_visible_article(page(), page_url=URL, adapter_contract_version=version)


def test_only_exact_old_source_can_reopen_a_cohort_not_current_unknown_or_changed_scope(tmp_path):
    old = PolicyRegistry(policy_config(), base=tmp_path).sources['ftnn']
    new = PolicyRegistry(candidate_config(), base=tmp_path).sources['ftnn']
    assert ftnn_native_v2_parser_repair(new, old.data_fingerprint)
    assert not ftnn_native_v2_parser_repair(new, new.data_fingerprint)
    assert not ftnn_native_v2_parser_repair(new, 'b' * 64)
    assert not ftnn_native_v2_parser_repair(old, old.data_fingerprint)
    changed = candidate_config()
    changed['providers'][0]['interval_seconds'] = 3
    assert not ftnn_native_v2_parser_repair(PolicyRegistry(changed, base=tmp_path).sources['ftnn'], old.data_fingerprint)
    assert not ftnn_native_v2_parser_repair(replace(new, allowed=False), old.data_fingerprint)


def test_retry_cohort_preserves_all_fields_but_state_and_does_not_replay_after_restart(tmp_path):
    before = {}
    credit_url = 'https://www.ftnn.com.tw/news/512507'
    states = ['article_identity_not_verified', 'article_attribution_not_verified',
              'article_rights_or_scope_excluded', 'body_not_found', 'invalid_bulk_publication_clock',
              'http_403', 'http_429', 'paywall_or_login', 'source_not_found', 'complete']
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        for number, value in enumerate(states, 1):
            url = 'https://www.ftnn.com.tw/news/' + str(number)
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=4102444800,last_checked_at_utc=? WHERE url=?',
                             (value, value, utc_now(), url))
        queue.add_articles([{'url': credit_url}])
        queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',error_code='article_attribution_not_verified',"
                         'attempts=3,due=4102444800,last_checked_at_utc=? WHERE url=?', (utc_now(), credit_url))
        queue.db.commit()
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    registry = PolicyRegistry(candidate_config(), base=tmp_path)
    selected = {'https://www.ftnn.com.tw/news/1'}
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for row in queue.db.execute('SELECT * FROM articles'):
            prior = before[row['url']]
            assert dict(row) == ({**prior, 'state': 'retry'} if row['url'] in selected else prior)
        marker = 'ftnn_native_title_recheck_v2:' + registry.sources['ftnn'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert {row['url'] for row in proof['rows']} == selected
        assert proof['attempts_due_error_and_observation_preserved'] and not proof['accepted_as_bodies']
        assert not proof['all_failures_assumed_repairable']
        for url in selected:
            queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (url,))
        queue.db.commit()
        after = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == after
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['stored_hash', 'stored_path', 'lease_owner', 'lease_until',
    'missing_observation', 'naive_observation', 'future_observation', 'wrong_error', 'unknown_capture',
    'changed_http', 'query_url'])
def test_unsafe_or_unrelated_old_failure_is_not_reopened_or_generic_reset(tmp_path, change):
    url = URL + ('?x=1' if change == 'query_url' else '')
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        queue.add_articles([{'url': url}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                         'attempts=9,due=4102444800,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        columns = {'stored_hash': ('body_sha256', 'a' * 64), 'stored_path': ('parquet_path', 'bodies/old.parquet'),
            'lease_owner': ('lease_owner', 'other-owner'), 'lease_until': ('lease_until', 4102444800),
            'missing_observation': ('last_checked_at_utc', None),
            'naive_observation': ('last_checked_at_utc', '2026-10-17T00:00:00'),
            'future_observation': ('last_checked_at_utc', '2099-10-17T00:00:00+00:00'),
            'wrong_error': ('error_code', 'http_403')}
        if change in columns:
            column, value = columns[change]
            queue.db.execute('UPDATE articles SET ' + column + '=? WHERE url=?', (value, url))
        elif change == 'unknown_capture':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('b' * 64, 'source_capture_fingerprint:ftnn'))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    cfg = candidate_config()
    if change == 'changed_http':
        cfg['providers'][0]['interval_seconds'] = 3
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


def test_learned_capacity_and_real_429_refusal_survive_native_repair(tmp_path):
    cfg = policy_config()
    cfg['adaptive_rate'] = {'enabled': False}
    old_http, old_session = http_fixture(cfg, tmp_path, {})
    try:
        old = PublisherCapacity(old_http, cfg)
        old_http.set_client_capacity('ftnn', .25, 2)
        with pytest.raises(NewsError, match='http_429'):
            old_http._response_status('ftnn', 429, {'retry-after': '120'})
        old.states['ftnn'].update(interval_seconds=.5, parallelism=2, state='stable_pipeline_capacity')
        atomic_write_json(old.path, old.snapshot(), durable=True)
        refusal = (tmp_path / 'cooldowns/ftnn.json').read_bytes()
        assert not old_session.calls
    finally:
        old_http.close()
    new_cfg = deepcopy(cfg)
    new_cfg['providers'][0]['embedded_article_layout'] = 'ftnn_visible_article_v2'
    new_http, session = http_fixture(new_cfg, tmp_path, {})
    try:
        new = PublisherCapacity(new_http, new_cfg)
        assert new.fingerprint == old.fingerprint and new.provider_fingerprints == old.provider_fingerprints
        assert new.states['ftnn']['state'] == 'stable_pipeline_capacity'
        assert new.states['ftnn']['interval_seconds'] == .5 and new.states['ftnn']['parallelism'] == 2
        assert new_http.rate_refusal_intervals['ftnn'] == .5
        assert (tmp_path / 'cooldowns/ftnn.json').read_bytes() == refusal
        assert not session.calls
    finally:
        new_http.close()


def test_actual_native_fetch_accept_receipt_parquet_and_restart(tmp_path):
    soup = leading_photo(page())
    soup.select_one('.news-body > p:first-of-type').string = '記者黃詩雯／綜合報導'
    breadcrumb(soup, name=TITLE + '　')
    cfg = candidate_config()
    http, _ = http_fixture(cfg, tmp_path, {
        'https://www.ftnn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(str(soup).encode())})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': URL}])
            queue.accept([fetch_article(http, queue.claim_url(URL, 300))])
            head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            assert head['state'] == 'complete' and head['body_sha256'] and head['parquet_path']
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == head
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
