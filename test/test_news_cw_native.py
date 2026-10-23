"""Synthetic CW static split-body contract; no copied article fixture or HTTP."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest
import pyarrow.parquet as pq

from downloader.news_fulltext_cw import (
    CW_AUTHOR_SELECTOR, CW_BODY_SELECTOR, CW_EDITOR, CW_EXCLUSIONS,
    CW_LAYOUT, CW_ORIGIN, CW_PATH_PATTERN,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = CW_ORIGIN+'/article/12345'
PUBLISHED = '2026-10-05T11:10:00+08:00'
MODIFIED = '2026-10-05T11:19:14+08:00'
TEXT = '自行撰寫的合成文字，保留ETF原始數字1000、價格12.34、比例2.5%與負數-5，不是來源新聞。' * 4


def entry():
    return {'id': 'cw', 'domains': ['www.cw.com.tw'], 'enabled': True,
        'acquisition_mode': 'finite', 'finite_article_urls': [URL], 'html_parser': 'lxml',
        'interval_seconds': 10, 'parallelism': 1, 'embedded_article_layout': CW_LAYOUT,
        'article_selectors': [CW_BODY_SELECTOR], 'article_path_pattern': CW_PATH_PATTERN,
        'title_selector': 'h1', 'require_configured_container': True,
        'require_author_attribution': True, 'author_selectors': [CW_AUTHOR_SELECTOR],
        'permitted_author_names': [CW_EDITOR], 'exclude_selectors': list(CW_EXCLUSIONS),
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-22',
            'evidence_url': CW_ORIGIN+'/copyright',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'original_author_scope_evidence_urls': [URL],
            'scope': 'Exact finite private reading only. No sitewide, paid, training or publication grant.'}}


def policy(candidate=None):
    return PolicyRegistry({'contract_version': 1, 'publish': False,
        'remote_training_export': False, 'collection_purpose': 'personal_research',
        'providers': [candidate or entry()]}, base=Path('.')).sources['cw']


def page():
    profile = CW_ORIGIN+'/author/30'
    article = {'@type': 'NewsArticle', '@id': URL+'#article', 'url': URL,
        'mainEntityOfPage': {'@type': 'WebPage', '@id': URL}, 'headline': '合成測試標題',
        'publisher': {'@id': CW_ORIGIN+'/#organization'}, 'isAccessibleForFree': True,
        'datePublished': PUBLISHED, 'dateModified': MODIFIED,
        'author': [{'@type': 'Person', 'name': CW_EDITOR, '@id': profile+'#person', 'url': profile,
            'worksFor': {'@type': 'Organization', '@id': CW_ORIGIN+'/#organization'}}]}
    return BeautifulSoup(f'''<html><head><link rel="canonical" href="{URL}">
      <meta property="og:url" content="{URL}"><meta property="og:title" content="合成測試標題｜天下雜誌">
      <meta property="article:modified_time" content="{MODIFIED}">
      <script type="application/ld+json">{json.dumps(article,ensure_ascii=False)}</script></head><body>
      <nav>NOT_ARTICLE_NAV</nav><main><article><div class="article__head"><h1>合成測試標題</h1></div>
      <div class="article__body"><div class="article__detail"><ul class="article__author"><li>
      <span>整理</span><div class="author--item"><a href="{profile}">{CW_EDITOR}</a></div></li></ul>
      <div class="pt10"><time>更新時間：2026-10-05</time></div><time>瀏覽數：100</time></div>
      <div class="article__content"><div id="article-preview" class="article-preview"><p>合成前段必須保留。</p></div>
      <div id="article-full-content" class="article-content"><h2>合成小標題</h2><p>{TEXT}</p>
      <section class="ad text-center ad--300by250 cw__advertising"><span>廣告</span><div class="google-ads" id="div-gpt-ad-1587109258916-0"></div></section>
      <table><thead><tr><th>股數</th></tr></thead><tbody><tr><td>1000</td></tr></tbody></table>
      <div class="accordion__group--article"><div class="accordion__item"><div class="accordion__item__header">合成問題？</div>
      <div class="accordion__item__panel"><p>合成完整回答。</p></div></div></div></div></div>
      <aside>NOT_ARTICLE_RECOMMENDATION</aside></div></article></main><footer>NOT_ARTICLE_FOOTER</footer>
      <div hidden>會員登入收藏功能，不是本文。</div></body></html>''','lxml')


def extract(soup, *, url=URL):
    return extract_article(str(soup).encode(), policy(), page_url=url)


def mutate_schema(soup, **updates):
    node = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(node.get_text()); value.update(updates)
    node.string = json.dumps(value, ensure_ascii=False)


def test_public_preview_full_table_and_static_FAQ_preserve_values_roles_and_clocks():
    result = extract(page())
    assert all(text in result['body'] for text in ('合成前段必須保留。', TEXT, '股數', '1000', '合成完整回答。'))
    assert 'NOT_ARTICLE_' not in result['body'] and '廣告' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_credit_role'] == 'publisher_declared_editor_compilation'
    assert detail['schema_author_type_raw'] == 'Person' and not detail['human_reporter_identity_verified']
    assert result['attribution_status'] == 'source_declared_corporate_author_not_verified_person'
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['public_static_preview_and_full_both_required']
    assert not notices['media_or_scripts_requested_or_executed'] and not notices['old_metadata_used_as_publication']


@pytest.mark.parametrize('updates,error', [
    ({'isAccessibleForFree': False}, 'paywall_or_login'),
    ({'isAccessibleForFree': 'true'}, 'paywall_or_login'),
    ({'url': URL+'0'}, 'article_identity_not_verified'),
    ({'@id': URL+'0#article'}, 'article_identity_not_verified'),
    ({'headline': '別篇'}, 'article_identity_not_verified'),
    ({'author': [{'@type': 'Person','name': '第三方來源'}]}, 'article_rights_or_scope_excluded'),
    ({'datePublished': '2026-10-05'}, 'invalid_bulk_publication_clock'),
    ({'dateModified': '2026-10-05T11:09:00+08:00'}, 'invalid_bulk_publication_clock'),
])
def test_schema_cannot_override_article_scope_access_author_or_clock(updates,error):
    soup = page(); mutate_schema(soup, **updates)
    with pytest.raises(NewsError,match=error):
        extract(soup)


@pytest.mark.parametrize('failure,error', [
    ('empty_full', 'body_not_found'), ('missing_preview', 'article_identity_not_verified'),
    ('third_party_credit', 'article_rights_or_scope_excluded'),
    ('wrong_visible_date', 'invalid_bulk_publication_clock'),
    ('hidden_body', 'article_identity_not_verified'), ('duplicate_body', 'article_identity_not_verified'),
    ('hidden_FAQ', 'article_identity_not_verified'), ('script', 'article_identity_not_verified'),
    ('ad_prose', 'article_identity_not_verified'),
])
def test_public_native_body_rejects_incomplete_unreviewed_or_hidden_representations(failure,error):
    soup = page()
    if failure == 'empty_full':
        soup.select_one('#article-full-content').clear()
        mutate_schema(soup,articleBody=TEXT*20)
    elif failure == 'missing_preview': soup.select_one('#article-preview').decompose()
    elif failure == 'third_party_credit':
        soup.select_one('.article__author').append(BeautifulSoup('<li><span>資料來源</span><a>第三方</a></li>','lxml').li)
    elif failure == 'wrong_visible_date': soup.select_one('.pt10 > time').string='更新時間：2026-05-13'
    elif failure == 'hidden_body': soup.select_one('#article-full-content')['style']='display: none'
    elif failure == 'duplicate_body': soup.select_one('.article__content').append(deepcopy(soup.select_one('#article-full-content')))
    elif failure == 'hidden_FAQ': soup.select_one('.accordion__item__panel')['aria-hidden']='true'
    elif failure == 'script': soup.select_one('#article-full-content').append(soup.new_tag('script'))
    elif failure == 'ad_prose': soup.select_one('.cw__advertising span').string=TEXT
    with pytest.raises(NewsError,match=error): extract(soup)


@pytest.mark.parametrize('field,value', [
    ('domains',['cw.com.tw']), ('acquisition_mode','continuous'), ('finite_article_urls',[]),
    ('allow_reviewed_supplements',True), ('require_author_attribution',False),
    ('article_selectors',['body']), ('exclude_selectors',[]), ('permitted_author_names',['第三方']),
    ('feeds',[CW_ORIGIN+'/feed']), ('wordpress_apis',[CW_ORIGIN+'/api/']),
])
def test_finite_registration_cannot_expand_source_or_body_contract(field,value):
    candidate=entry(); candidate[field]=value
    with pytest.raises(ValueError,match='unreviewed embedded article parser|bulk text endpoint requires an explicit permission basis'):
        policy(candidate)


@pytest.mark.parametrize('url',[URL+'?member=1',CW_ORIGIN+'/article/999','https://csr.cw.com.tw/article/12345'])
def test_canonical_identity_is_literal_not_guessed_or_cross_subsite(url):
    with pytest.raises(NewsError,match='article_identity_not_verified'): extract(page(),url=url)


def test_literal_NUL_is_not_silently_removed_by_HTML_parser():
    payload = str(page()).encode().replace('天下編輯部'.encode(), '天下\x00編輯部'.encode())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, policy(), page_url=URL)


@pytest.fixture
def finite_original(tmp_path):
    from downloader.artifact_io import atomic_write_json
    from test_news_retained_native_replay import approved
    config = {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research', 'providers': [entry()]}
    original_auth = deepcopy(entry()['authorization'])
    original_auth.pop('original_author_scope_evidence_urls')
    original_auth['scope'] = 'Original finite public diagnostic, not accepted body.'
    result = approved(tmp_path, config, urls=(URL,), provider='cw', original_auth=original_auth,
        raw_factory=lambda **kwargs: str(page()).encode())
    queue, _, manifest_path, manifest = result
    queue.add_articles([{'url': URL, 'date': '2026-05-13 09:08:34'}])
    queue.add_articles([{'url': URL.replace('12345', '98765')}])
    queue.sync_finite_cohorts()
    directory = manifest_path.parent/'private-source-diagnostics'
    directory.chmod(0o700)
    for path in directory.iterdir():
        path.chmod(0o600)
    manifest.update(contract_version=3, scope_kind='reviewed_finite_native_materialization_v3',
        original_author_scope_evidence_addition_reviewed=True, diagnostic_directory=directory.name)
    atomic_write_json(manifest_path, manifest, durable=True)
    yield result
    queue.close()


def test_CW_finite_original_uses_canonical_receipts_once_and_preserves_outside_scope(finite_original, monkeypatch):
    from downloader.artifact_io import sha256_file
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import head
    queue, registry, path, manifest = finite_original
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('offline native acceptance must not create an HTTP owner'))
    outside = URL.replace('12345', '98765')
    before = head(queue, outside)
    result = materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)
    assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
    current = head(queue, URL)
    saved = pq.ParquetFile(queue.root/current['parquet_path']).read().to_pylist()[0]
    assert saved['captured_at_utc'] == saved['source_observed_at_utc'] == manifest['entries'][0]['captured_at_utc']
    assert saved['published_at_raw'] == PUBLISHED and not saved['historical_point_in_time']
    assert saved['metadata_date_raw'] == '2026-05-13 09:08:34'
    assert json.loads(saved['original_diagnostic_authorization_json']) == manifest['entries'][0]['original_authorization']
    assert json.loads(saved['authorization_json']) == registry.sources['cw'].authorization
    assert sha256_file(queue.root/saved['replay_source_parquet']) == manifest['entries'][0]['diagnostic_sha256']
    assert head(queue, outside) == before and queue.claim_url(outside, 300) is None
    assert materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)['materialized_bodies'] == 0
    assert head(queue, URL) == current


@pytest.mark.parametrize('changes', [
    {'provider': 'other'}, {'domains': ('crossing.cw.com.tw',)},
    {'embedded_article_layout': 'gvm_public_native_article_v1'}, {'acquisition_mode': 'continuous'},
    {'finite_article_urls': ()}, {'allow_reviewed_supplements': True},
    {'authorization': {'kind': 'explicit_permission', 'evidence_url': CW_ORIGIN+'/copyright'}},
    {'authorization': {'kind': 'personal_research', 'evidence_url': CW_ORIGIN+'/copyright',
        'original_author_scope_evidence_urls': [URL+'0']}},
])
def test_finite_native_scope_is_not_a_generic_provider_or_subsite_grant(changes):
    from downloader.news_fulltext_retained_native import _reviewed_source_scope
    assert _reviewed_source_scope(policy(), 3)
    assert not _reviewed_source_scope(replace(policy(), **changes), 3)


def test_CW_durable_receipt_recovers_after_worker_loss_without_refetch(finite_original, monkeypatch):
    from downloader.news_fulltext_queue import NewsQueue
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import head
    queue, registry, path, _ = finite_original
    def crash(*args):
        raise RuntimeError('durable body before head commit')
    monkeypatch.setattr(queue, '_accept_records', crash)
    with pytest.raises(RuntimeError, match='durable body before head commit'):
        materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)
    root = queue.root
    queue.close()
    recovered = NewsQueue(root, registry)
    try:
        assert head(recovered, URL)['state'] == 'complete'
        assert head(recovered, URL)['body_sha256']
        assert recovered.claim_url(URL.replace('12345', '98765'), 300) is None
    finally:
        recovered.close()


def test_HTTP_identity_inheritance_requires_the_exact_capture_and_authorization(monkeypatch):
    # Production has its own closed binding. A synthetic or altered candidate
    # must not inherit it merely because its provider/layout has the same name.
    source = policy()
    assert not source.http_capacity_source_fingerprint
    assert not source.http_capacity_authorization_fingerprint
    binding = (source.data_fingerprint, source.authorization_fingerprint, 'a'*64, 'b'*64)
    monkeypatch.setattr('downloader.news_fulltext_cw.CW_FINITE_HTTP_BINDING', binding)
    inherited = policy()
    assert inherited.http_capacity_source_fingerprint == binding[2]
    assert inherited.http_capacity_authorization_fingerprint == binding[3]
    for change in ('url', 'authorization', 'pacing'):
        candidate = entry()
        if change == 'url':
            candidate['finite_article_urls'] = [URL+'0']
            candidate['authorization']['original_author_scope_evidence_urls'] = [URL+'0']
        elif change == 'authorization':
            candidate['authorization']['scope'] += ' Changed scope must not inherit controls.'
        else:
            candidate['interval_seconds'] = 11
        altered = policy(candidate)
        assert not altered.http_capacity_source_fingerprint
        assert not altered.http_capacity_authorization_fingerprint


def test_activating_one_CW_original_preserves_other_capture_heads_and_GVM_scope(tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_retained_native_replay import head
    from test_news_gvm_native import entry as gvm_entry
    before = {'contract_version': 1, 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [gvm_entry()]}
    outside = URL.replace('12345', '98765')
    root = tmp_path/'queue'
    old_registry = PolicyRegistry(before, base=tmp_path)
    old = NewsQueue(root, old_registry)
    try:
        with old.db:
            old.add_articles([{'url': URL, 'date': '2026-05-13 09:08:34'}, {'url': outside}])
        prior = head(old, outside)
    finally:
        old.close()
    current_config = deepcopy(before)
    current_config['providers'].append(entry())
    registry = PolicyRegistry(current_config, base=tmp_path)
    assert registry.sources['gvm'] == old_registry.sources['gvm']
    current = NewsQueue(root, registry)
    try:
        assert head(current, URL)['state'] == 'pending'
        assert head(current, URL)['metadata_date_raw'] == '2026-05-13 09:08:34'
        migrated = head(current, outside)
        assert migrated == {**prior, 'provider': 'cw'}
        assert current.claim_url(outside, 300) is None
    finally:
        current.close()
