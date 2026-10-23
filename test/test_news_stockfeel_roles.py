"""Source67 is a publisher declaration, not an author, license or retry grant."""
from copy import deepcopy
import json
from pathlib import Path
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_candidate_diagnostics import _key
from downloader.news_fulltext_candidates import native_candidate_source
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_repairs as repair
from downloader.news_fulltext_stockfeel import (
    STOCKFEEL_V2_HTTP_BINDING, STOCKFEEL_V2_PRIOR_SCOPE_CUTOFF, STOCKFEEL_V2_REPAIR_COHORT,
    STOCKFEEL_V3_HTTP_BINDING, STOCKFEEL_V4_HTTP_BINDING, STOCKFEEL_V3_PRIOR_SCOPE_CUTOFF,
    STOCKFEEL_V4_CREDIT_NOTE, STOCKFEEL_V5_CREDIT_NOTE, STOCKFEEL_V6_CREDIT_NOTE, STOCKFEEL_V7_CREDIT_NOTE,
    stockfeel_role_repair_source, stockfeel_writer_profile, stockfeel_writer_repair_source,
)
from test_news_fulltext import config
from test_news_stockfeel_original import AUTHOR, PROFILE, URL, html, settings
from test_news_reviewed_repairs import failure, head


def role_html(html):
    soup = BeautifulSoup(html, 'lxml')
    source = soup.select('.sin-content-header div[data-aid]')[1]
    source['data-aid'] = '67'
    source.a['href'] = 'https://www.stockfeel.com.tw/author/stockfeel'
    source.a.string = '股感知識庫'
    return str(soup)


def extract(settings, html, version):
    selected = deepcopy(settings)
    selected['providers'][0]['embedded_article_layout'] = f'stockfeel_visible_original_v{version}'
    return extract_article(html.encode(), PolicyRegistry(selected, base='.').sources['stockfeel'], page_url=URL)


def test_old427_pair_keeps_every_field_under_v2(settings, html):
    assert extract(settings, html, 1) == extract(settings, html, 2)


def test_source67_kept_separate_from_declared_author_and_license(settings, html):
    original = role_html(html)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(settings, original, 1)
    record = extract(settings, original, 2)
    assert json.loads(record['authors_json']) == [AUTHOR]
    author, source = json.loads(record['author_attribution_detail_json'])['source_header_declarations']
    assert (author['data_aid'], author['profile_url'], author['name']) == ('427', PROFILE, AUTHOR)
    assert (source['label'], source['data_aid'], source['name']) == ('文章來源', '67', '股感知識庫')
    assert 'not_copyright_ownership_or_license' in record['article_identity_basis']
    assert not record['historical_point_in_time']
    assert record['published_at_raw'] == '2026-10-15T07:07:51+00:00'
    assert '-1,234.50' in record['body'] and '原始合成段落。' * 25 in record['body']


@pytest.mark.parametrize('selector,attribute,value', [
    ('.sin-content-header div[data-aid]:first-child', 'data-aid', '67'),
    ('.sin-content-header div[data-aid]:last-child', 'data-aid', '999'),
    ('.sin-content-header div[data-aid]:last-child a', 'href', 'https://other.example/author/stockfeel'),
    ('.sin-content-header div[data-aid]:last-child a', 'text', '其他來源'),
    ('head > meta[name=author]', 'content', '股感知識庫'),
    ('.user-avatar', 'href', 'https://www.stockfeel.com.tw/author/stockfeel'),
])
def test_role_mismatch_never_becomes_an_arbitrary_byline(settings, html, selector, attribute, value):
    soup = BeautifulSoup(role_html(html), 'lxml')
    node = soup.select_one(selector)
    assert node is not None
    if attribute == 'text':
        node.string = value
    else:
        node[attribute] = value
    with pytest.raises(NewsError):
        extract(settings, str(soup), 2)


def production_settings(version):
    loaded, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = deepcopy(next(entry for entry in loaded['providers'] if entry['id'] == 'stockfeel'))
    entry['note'] = entry['note'].replace(STOCKFEEL_V7_CREDIT_NOTE, STOCKFEEL_V6_CREDIT_NOTE)
    entry['note'] = entry['note'].replace(STOCKFEEL_V6_CREDIT_NOTE, STOCKFEEL_V5_CREDIT_NOTE)
    entry['note'] = entry['note'].replace(STOCKFEEL_V5_CREDIT_NOTE, STOCKFEEL_V4_CREDIT_NOTE)
    # Normalize any installed reviewed version; do not require a local artifact.
    entry['authorization']['scope'] = entry['authorization']['scope'].replace(
        'matching native author/header/avatar declarations with exact StockFeel article-source67/stockfeel, plus the unchanged427/news pair; declaration is not ownership',
        'native first-party427 author/source pair')
    entry['note'] = entry['note'].replace(
        'Author name, numeric ID, same-origin literal profile, avatar and head declaration must agree independently of exact source67/stockfeel. Source427/news remains the old427 author only. External credits, reprints and unproved identities remain excluded.',
        'Current article/source must independently match native StockFeel427.')
    entry['authorization']['scope'] = entry['authorization']['scope'].replace(
        'native author427/news and independent article-source427/news or67/stockfeel declarations',
        'native first-party427 author/source pair')
    entry['note'] = entry['note'].replace(
        'The native427/news author and independent article-source427/news or67/stockfeel must match their exact declarations; other credits remain unreviewed.',
        'Current article/source must independently match native StockFeel427.')
    entry['adapter_verification'] = 'native_UTF8_canonical_current_heading_original_427_credit_pair_OG_clocks_v1_offline_verified_20261022'
    entry['embedded_article_layout'] = 'stockfeel_visible_original_v1'
    if version in {2, 3, 4}:
        entry['embedded_article_layout'] = 'stockfeel_visible_original_v2'
        entry['authorization']['scope'] = entry['authorization']['scope'].replace('native first-party427 author/source pair',
            'native author427/news and independent article-source427/news or67/stockfeel declarations')
        entry['note'] = entry['note'].replace('Current article/source must independently match native StockFeel427.',
            'The native427/news author and independent article-source427/news or67/stockfeel must match their exact declarations; other credits remain unreviewed.')
        entry['adapter_verification'] = 'native_UTF8_canonical_current_heading_author427_independent_source427_or67_OG_clocks_v2_scoped_20261022'
    if version in {3, 4}:
        entry['embedded_article_layout'] = 'stockfeel_visible_original_v3'
        entry['authorization']['scope'] = entry['authorization']['scope'].replace(
            'native author427/news and independent article-source427/news or67/stockfeel declarations',
            'matching native author/header/avatar declarations with exact StockFeel article-source67/stockfeel, plus the unchanged427/news pair; declaration is not ownership')
        entry['note'] = entry['note'].replace(
            'The native427/news author and independent article-source427/news or67/stockfeel must match their exact declarations; other credits remain unreviewed.',
            'Author name, numeric ID, same-origin literal profile, avatar and head declaration must agree independently of exact source67/stockfeel. Source427/news remains the old427 author only. External credits, reprints and unproved identities remain excluded.')
        entry['adapter_verification'] = 'native_UTF8_canonical_exact_source67_independent_matching_writer_profile_avatar_OG_clocks_v3_scoped_20261022'
    if version == 4:
        entry['embedded_article_layout'] = 'stockfeel_visible_original_v4'
        entry['adapter_verification'] = 'native_UTF8_canonical_exact_source67_matching_writer_literal_dotted_profile_avatar_OG_clocks_v4_scoped_20261022'
    loaded['providers'] = [entry]
    return loaded, repo


def test_exact_new_capture_inherits_only_previous_transport_identity():
    settings, repo = production_settings(2)
    source = PolicyRegistry(settings, base=repo).sources['stockfeel']
    assert stockfeel_role_repair_source(source)
    assert (source.data_fingerprint, source.authorization_fingerprint) == STOCKFEEL_V2_HTTP_BINDING[1:3]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == STOCKFEEL_V2_HTTP_BINDING[3:]
    assert source.parser_repair_prior_capture_fingerprint == STOCKFEEL_V2_HTTP_BINDING[0]
    changed = deepcopy(settings)
    changed['providers'][0]['interval_seconds'] = 3
    changed_source = PolicyRegistry(changed, base=repo).sources['stockfeel']
    assert not stockfeel_role_repair_source(changed_source)
    assert not changed_source.http_capacity_source_fingerprint
    assert not changed_source.http_capacity_authorization_fingerprint


def test_cutover_preserves_all_heads_then_mailbox_repairs_only_proved_one(tmp_path):
    old_settings, repo = production_settings(1)
    old = NewsQueue(tmp_path, PolicyRegistry(old_settings, base=repo))
    selected, = STOCKFEEL_V2_REPAIR_COHORT
    observed_at = '2026-10-21T20:30:00+00:00'
    failure(old, selected, state='article_rights_or_scope_excluded', error_code='article_rights_or_scope_excluded',
        last_checked_at_utc=observed_at)
    for index, state in enumerate(('awaiting_authorization', 'article_identity_not_verified', 'paywall_or_login',
            'http_403', 'http_429', 'body_not_found', 'article_attribution_not_verified', 'article_rights_or_scope_excluded')):
        failure(old, f'https://www.stockfeel.com.tw/other-{index}/', state=state, error_code=state,
            last_checked_at_utc=observed_at)
    before = {row['url']: dict(row) for row in old.db.execute('SELECT * FROM articles')}
    old.close()
    settings, repo = production_settings(2)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before
        result = repair.submit_repairs(tmp_path, queue.registry, before, reason='One reviewed native source role')
        assert result['submitted_targets'] == 4  #3 unchanged generic semantic failures plus the one scope witness.
        # Submit only the witnessed scope head operationally; this multi-input
        # test also proves every other scope/refusal/auth head remains excluded.
        request = repair._load(Path(result['request_path']))
        scope = [row for row in request['targets'] if row['state'] == 'article_rights_or_scope_excluded']
        assert [row['url'] for row in scope] == [selected]
        assert not result['queue_mutations'] and head(queue, selected) == before[selected]
        assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 4}
        assert head(queue, selected) == dict(before[selected], state='retry')
        for url, row in before.items():
            if row['state'] in {'awaiting_authorization', 'paywall_or_login', 'http_403', 'http_429'} or url.endswith('other-7/'):
                assert head(queue, url) == row
        assert repair.submit_repairs(tmp_path, queue.registry, [selected], reason='No duplicate replay')['submitted_targets'] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('changes', [
    {'last_checked_at_utc': STOCKFEEL_V2_PRIOR_SCOPE_CUTOFF}, {'last_checked_at_utc': None},
    {'body_sha256': 'a' * 64}, {'lease_owner': 'other'}, {'due': time.time() + 86400},
    {'state': 'http_403', 'error_code': 'http_403'},
])
def test_reviewed_scope_question_never_clears_fresh_failures_or_refusals(tmp_path, changes):
    settings, repo = production_settings(2)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        selected, = STOCKFEEL_V2_REPAIR_COHORT
        values = {'state': 'article_rights_or_scope_excluded',
            'error_code': 'article_rights_or_scope_excluded', 'last_checked_at_utc': '2026-10-21T20:30:00+00:00'}
        values.update(changes)
        before = failure(queue, selected, **values)
        assert repair.submit_repairs(tmp_path, queue.registry, [selected], reason='Bounded native role question')['submitted_targets'] == 0
        assert head(queue, selected) == before
    finally:
        queue.close()


def test_v14_completed_question_key_does_not_rearm_after_role_upgrade():
    target = {'provider': 'stockfeel', 'id': 'a' * 64}
    assert _key(target, STOCKFEEL_V2_HTTP_BINDING[0], evidence_contract_version=14) == _key(
        target, STOCKFEEL_V2_HTTP_BINDING[1], evidence_contract_version=14)
    settings, repo = production_settings(2)
    registry = PolicyRegistry(settings, base=repo)
    assert native_candidate_source(settings, registry, repo, 'stockfeel_visible_original_v2') == registry.sources['stockfeel']
    with pytest.raises(ValueError, match='reviewed_current_stockfeel_source_required'):
        native_candidate_source(settings, registry, repo, 'stockfeel_visible_original_v1')


def test_current_v2_metadata_mailbox_loads_without_new_question_version(tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    settings, repo = production_settings(2)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        selected, = STOCKFEEL_V2_REPAIR_COHORT
        failure(queue, selected, state='article_rights_or_scope_excluded',
            error_code='article_rights_or_scope_excluded', last_checked_at_utc='2026-10-21T20:30:00+00:00')
        result = diagnostic.submit_candidate(tmp_path, settings, queue.registry, selected,
            'stockfeel_visible_original_v2', reason='Only an unanswered fixed native boundary', evidence_contract_version=14)
        assert result['submitted'] and result['publisher_requests'] == 0
        request = diagnostic._load(diagnostic._inbox(tmp_path) / (result['request_id'] + '.json'))
        assert request['evidence_contract_version'] == 14
        assert request['observation_key'] == _key(request['target'], STOCKFEEL_V2_HTTP_BINDING[0], evidence_contract_version=14)
    finally:
        queue.close()


def writer_html(html, *, profile='https://www.stockfeel.com.tw/author/合成作者'):
    """Synthetic native writer, not a captured article or a real person fixture."""
    soup = BeautifulSoup(role_html(html), 'lxml')
    author = soup.select('.sin-content-header div[data-aid]')[0]
    author['data-aid'] = '98765'
    author.a.string = '合成研究員甲'
    author.a['href'] = profile
    soup.select_one('head > meta[name=author]')['content'] = '合成研究員甲'
    soup.select_one('.user-avatar')['href'] = profile
    return str(soup)


@pytest.mark.parametrize('original', [True, False])
def test_v3_preserves_every_old427_field(settings, html, original):
    original_html = html if original else role_html(html)
    assert extract(settings, original_html, 2) == extract(settings, original_html, 3)


@pytest.mark.parametrize('profile', [
    'https://www.stockfeel.com.tw/author/合成作者',
    'https://www.stockfeel.com.tw/author/%E5%90%88%E6%88%90%E4%BD%9C%E8%80%85/',
    'https://www.stockfeel.com.tw/author/synthetic-writer_123',
])
def test_native_writer_and_exact_publisher_are_separate_without_person_or_license_claim(settings, html, profile):
    raw = writer_html(html, profile=profile)
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(settings, raw, 2)
    record = extract(settings, raw, 3)
    assert json.loads(record['authors_json']) == ['合成研究員甲']
    attribution = json.loads(record['author_attribution_detail_json'])
    writer, source = attribution['source_header_declarations']
    assert writer['profile_url'] == profile and writer['data_aid'] == '98765'
    assert (source['data_aid'], source['name']) == ('67', '股感知識庫')
    assert attribution['meta_author_raw'] == writer['name']
    assert attribution['declared_writer_not_independently_verified_person']
    assert not attribution['human_reporter_identity_verified']
    assert not attribution['rendered_byline_visibility_verified']
    assert 'not_copyright_ownership_or_license' in record['article_identity_basis']
    assert record['body'] == extract(settings, role_html(html), 2)['body']


@pytest.mark.parametrize('profile', [
    'https://other.example/author/合成作者', 'http://www.stockfeel.com.tw/author/name',
    'https://www.stockfeel.com.tw.evil.example/author/name',
    'https://www.stockfeel.com.tw@other.example/author/name',
    'https://www.stockfeel.com.tw:443/author/name',
    'https://www.stockfeel.com.tw/author/name?', 'https://www.stockfeel.com.tw/author/name#',
    'https://www.stockfeel.com.tw/author/name/extra', 'https://www.stockfeel.com.tw/author//',
    'https://www.stockfeel.com.tw/author/.', 'https://www.stockfeel.com.tw/author/..',
    'https://www.stockfeel.com.tw/author/%2Fname', 'https://www.stockfeel.com.tw/author/%5cname',
    'https://www.stockfeel.com.tw/author/%252Fname', 'https://www.stockfeel.com.tw/author/%FF',
    'https://www.stockfeel.com.tw/author/%', 'https://www.stockfeel.com.tw/author/%00',
    'https://www.stockfeel.com.tw/author/na\u200bme',
    'https://www.stockfeel.com.tw/author/na%20me',
    'https://www.stockfeel.com.tw/author/name\\other',
])
def test_native_writer_profile_does_not_expand_origin_or_invent_alias(settings, html, profile):
    assert stockfeel_writer_profile(profile) is None
    with pytest.raises(NewsError):
        extract(settings, writer_html(html, profile=profile), 3)


@pytest.mark.parametrize('selector,attribute,value', [
    ('.sin-content-header div[data-aid]:first-child', 'data-aid', '427'),
    ('.sin-content-header div[data-aid]:first-child', 'data-aid', '67'),
    ('.sin-content-header div[data-aid]:first-child', 'data-aid', 'not-numeric'),
    ('.sin-content-header div[data-aid]:first-child', 'style', 'display:none'),
    ('.sin-content-header div[data-aid]:last-child', 'data-aid', '427'),
    ('.sin-content-header div[data-aid]:last-child', 'data-aid', '999'),
    ('.sin-content-header div[data-aid]:last-child a', 'text', '外部來源'),
    ('.sin-content-header div[data-aid]:last-child a', 'href', 'https://other.example/author/stockfeel'),
    ('head > meta[name=author]', 'content', '另一位作者'),
    ('.user-avatar', 'href', 'https://www.stockfeel.com.tw/author/另一位作者'),
    ('.user-avatar', 'href', 'https://www.stockfeel.com.tw/author/%E5%90%88%E6%88%90%E4%BD%9C%E8%80%85'),
])
def test_v3_requires_all_independent_native_declarations(settings, html, selector, attribute, value):
    soup = BeautifulSoup(writer_html(html), 'lxml')
    node = soup.select_one(selector)
    assert node is not None
    if attribute == 'text':
        node.string = value
    else:
        node[attribute] = value
    with pytest.raises(NewsError):
        extract(settings, str(soup), 3)


def test_v3_does_not_accept_reprint_or_embedded_access_boundary(settings, html):
    for markup, error in [('<p>本文轉載自其他媒體。</p>', 'article_rights_or_scope_excluded'),
            ('<iframe src="https://other.example/embed"></iframe>', 'paywall_or_login')]:
        soup = BeautifulSoup(writer_html(html), 'lxml')
        soup.select_one('.sin-content-original').append(BeautifulSoup(markup, 'lxml').body.contents[0])
        with pytest.raises(NewsError, match=error):
            extract(settings, str(soup), 3)


def test_v3_exact_transport_binding_and_existing_v14_key_are_not_reset():
    settings, repo = production_settings(3)
    source = PolicyRegistry(settings, base=repo).sources['stockfeel']
    assert stockfeel_writer_repair_source(source)
    assert (source.data_fingerprint, source.authorization_fingerprint) == STOCKFEEL_V3_HTTP_BINDING[1:3]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == STOCKFEEL_V2_HTTP_BINDING[3:]
    assert source.parser_repair_prior_capture_fingerprint == STOCKFEEL_V2_HTTP_BINDING[1]
    target = {'provider': 'stockfeel', 'id': 'b' * 64}
    assert _key(target, STOCKFEEL_V2_HTTP_BINDING[1], evidence_contract_version=14) == _key(
        target, source.data_fingerprint, evidence_contract_version=14)
    changed = deepcopy(settings)
    changed['providers'][0]['parallelism'] += 1
    other = PolicyRegistry(changed, base=repo).sources['stockfeel']
    assert not stockfeel_writer_repair_source(other)
    assert not other.http_capacity_source_fingerprint and not other.http_capacity_authorization_fingerprint


def test_v3_keeps_existing_heads_and_only_explicitly_reconsiders_old_semantic_exclusion(tmp_path):
    old_settings, repo = production_settings(2)
    old = NewsQueue(tmp_path, PolicyRegistry(old_settings, base=repo))
    eligible = 'https://www.stockfeel.com.tw/another-native-writer/'
    failure(old, eligible, state='article_rights_or_scope_excluded', error_code='article_rights_or_scope_excluded',
        last_checked_at_utc='2026-10-21T20:30:00+00:00', attempts=3)
    protected = []
    for index, state in enumerate(('awaiting_authorization', 'http_403', 'http_429', 'paywall_or_login',
            'robots_disallowed', 'article_rights_not_verified', 'article_rights_or_scope_excluded')):
        url = f'https://www.stockfeel.com.tw/protected-{index}/'
        failure(old, url, state=state, error_code=state, last_checked_at_utc=STOCKFEEL_V3_PRIOR_SCOPE_CUTOFF)
        protected.append(url)
    before = {row['url']: dict(row) for row in old.db.execute('SELECT * FROM articles')}
    old.close()
    settings, repo = production_settings(3)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before
        result = repair.submit_repairs(tmp_path, queue.registry, before, reason='Once under strict source67/native-writer correction')
        assert result['submitted_targets'] == 1
        assert head(queue, eligible) == before[eligible]
        assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 1}
        assert head(queue, eligible) == dict(before[eligible], state='retry')
        assert all(head(queue, url) == before[url] for url in protected)
        assert repair.submit_repairs(tmp_path, queue.registry, [eligible], reason='No repeated retry')['submitted_targets'] == 0
    finally:
        queue.close()


def test_current_v3_metadata_loader_keeps_the_fixed_question(tmp_path):
    from downloader import news_fulltext_candidate_diagnostics as diagnostic
    settings, repo = production_settings(3)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        selected = 'https://www.stockfeel.com.tw/another-native-writer/'
        failure(queue, selected, state='article_rights_or_scope_excluded', error_code='article_rights_or_scope_excluded',
            last_checked_at_utc='2026-10-21T20:30:00+00:00')
        result = diagnostic.submit_candidate(tmp_path, settings, queue.registry, selected,
            'stockfeel_visible_original_v3', reason='Synthetic unanswered question only', evidence_contract_version=14)
        request = diagnostic._load(diagnostic._inbox(tmp_path) / (result['request_id'] + '.json'))
        assert request['evidence_contract_version'] == 14 and result['publisher_requests'] == 0
        assert request['observation_key'] == _key(request['target'], STOCKFEEL_V2_HTTP_BINDING[0], evidence_contract_version=14)
    finally:
        queue.close()


def test_v4_accepts_literal_dotted_author_profile_without_guessing_a_route(settings, html):
    profile = 'https://www.stockfeel.com.tw/author/synthetic.writer'
    assert stockfeel_writer_profile(profile, adapter_contract_version=3) is None
    assert stockfeel_writer_profile(profile, adapter_contract_version=4) == profile
    raw = writer_html(html, profile=profile)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(settings, raw, 3)
    result = extract(settings, raw, 4)
    writer, source = json.loads(result['author_attribution_detail_json'])['source_header_declarations']
    assert writer['profile_url'] == profile and source['data_aid'] == '67'
    assert result['body'] == extract(settings, role_html(html), 2)['body']
    assert 'source67_v4;' in result['article_identity_basis']
    assert extract(settings, role_html(html), 2) == extract(settings, role_html(html), 4)
    assert extract(settings, html, 2) == extract(settings, html, 4)


@pytest.mark.parametrize('profile', [
    'https://www.stockfeel.com.tw/author/.', 'https://www.stockfeel.com.tw/author/..',
    'https://www.stockfeel.com.tw/author/../synthetic.writer',
    'https://www.stockfeel.com.tw/author/synthetic.writer/../another',
    'https://www.stockfeel.com.tw/author/synthetic%2F.writer',
    'https://www.stockfeel.com.tw/author/synthetic.writer?x=1',
    'https://www.stockfeel.com.tw/author/synthetic.writer#bio',
    'https://other.example/author/synthetic.writer',
    'https://www.stockfeel.com.tw/author/....',
])
def test_v4_dot_does_not_allow_traversal_or_another_identity(settings, html, profile):
    assert stockfeel_writer_profile(profile, adapter_contract_version=4) is None
    with pytest.raises(NewsError):
        extract(settings, writer_html(html, profile=profile), 4)


def test_v4_inherits_exact_old_http_and_does_not_rearm_v14():
    settings, repo = production_settings(4)
    source = PolicyRegistry(settings, base=repo).sources['stockfeel']
    assert stockfeel_writer_repair_source(source)
    assert (source.data_fingerprint, source.authorization_fingerprint) == STOCKFEEL_V4_HTTP_BINDING[1:3]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == STOCKFEEL_V2_HTTP_BINDING[3:]
    assert source.parser_repair_prior_capture_fingerprint == STOCKFEEL_V3_HTTP_BINDING[1]
    target = {'provider': 'stockfeel', 'id': 'c' * 64}
    assert _key(target, source.data_fingerprint, evidence_contract_version=14) == _key(
        target, STOCKFEEL_V2_HTTP_BINDING[0], evidence_contract_version=14)
