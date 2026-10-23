"""Synthetic native stream proofs; no publisher body, credentials or HTTP."""
from copy import deepcopy
from html import escape
import json
from pathlib import Path
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_taiwannews import (
    TAIWANNEWS_BODY_SELECTOR, TAIWANNEWS_LAYOUT, TAIWANNEWS_PATH_PATTERN,
    TAIWANNEWS_V2_LAYOUT, TAIWANNEWS_V2_VERIFICATION, TAIWANNEWS_VERIFICATION,
    TAIWANNEWS_V2_HTTP_BINDING,
)


URL = 'https://www.taiwannews.com.tw/zh/news/1234567'
TITLE = '合成研究資料：原始價格與成交數字'
CLOCK = 'May. 12, 2026 15:43'
PARAGRAPHS = ['這是合成研究文字，不是網站原文。價格10.8元、比率5.57%、數量4,183,651與負值-2都必須保持。' * 3,
              '另一段只供解析測試。不能把支持連結、圖片說明或其他文章當作當篇正文。']
RS = '$RS=function(a,b){a=document.getElementById(a);b=document.getElementById(b);for(a.parentNode.removeChild(a);a.firstChild;)b.parentNode.insertBefore(a.firstChild,b);b.parentNode.removeChild(b)}'
RC = '$RC=function(b,c,e){c=document.getElementById(c);c.parentNode.removeChild(c);var a=document.getElementById(b);if(a){b=a.previousSibling;if(e)b.data="$!",a.setAttribute("data-dgst",e);else{e=b.parentNode;a=b.nextSibling;var f=0;do{if(a&&8===a.nodeType){var d=a.data;if("/$"===d)if(0===f)break;else f--;else"$"!==d&&"$?"!==d&&"$!"!==d||f++}d=a.nextSibling;e.removeChild(a);a=d}while(a);for(;c.firstChild;)e.insertBefore(c.firstChild,a);b.data="$"}b._reactRetry&&b._reactRetry()}};$RC("B:0","S:0")'


def entry():
    return {'id': 'taiwannews', 'domains': ['www.taiwannews.com.tw'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'html_parser': 'lxml', 'interval_seconds': 2, 'parallelism': 1,
        'embedded_article_layout': TAIWANNEWS_LAYOUT, 'article_path_pattern': TAIWANNEWS_PATH_PATTERN,
        'article_selectors': [TAIWANNEWS_BODY_SELECTOR], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'exclude_selectors': [], 'author_selectors': [],
        'authorization': {'kind': 'personal_research',
            'evidence_url': 'https://www.taiwannews.com.tw/page/about-us',
            'reviewed_on': '2026-10-22', 'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Synthetic private known-URL original test; no public reuse or training/export.'}}


def config(source=None):
    return {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research', 'providers': [source or entry()]}


def source(tmp_path, version=1):
    selected = entry()
    selected['embedded_article_layout'] = f'taiwannews_completed_public_original_v{version}'
    return PolicyRegistry(config(selected), base=tmp_path).sources['taiwannews']


def page(*, nested=False, body=None, label='台灣英文新聞－編輯', name='合成編輯組', clock=CLOCK):
    writer = '3' if nested else '2'
    article_segment = '2' if nested else '1'
    body = ''.join('<p>' + escape(value) + '</p>' for value in PARAGRAPHS) if body is None else body
    headline = ('<div class="w-full max-w-4xl mx-auto"><div class="max-w-2xl w-full mx-auto">'
        '<h1 class="text-head-semibold">' + TITLE + '</h1><p>合成副標</p>'
        '<p class="text-right text-subtle-regular">' + clock + '</p></div>'
        '<section><p>圖／中央社，圖片不下載</p></section><template id="P:' + writer + '"></template></div>')
    article = ('<section>' + headline + '<div class="grid"><section><div itemprop="articleBody">'
        '<div class="editor app">' + body + '</div></div><a>支持我們</a></section>'
        '<section>其他文章不是正文</section></div></section>')
    card = ('<section><div class="flex gap-x-3"><a href="/zh/journalist/20"><img alt="' + name + '"></a>'
        '<div class="flex-1"><p class="text-small-regular"><a href="/zh/journalist/20">' + name + '</a></p>'
        '<p class="text-gray-3 text-subtle-large">' + label + '</p></div></div></section>')
    segments = {'1': '<div><template id="P:2"></template></div>'} if nested else {}
    segments.update({article_segment: article, writer: card})
    streams = '<div hidden="" id="S:0"><template id="P:1"></template></div>'
    for index, (key, content) in enumerate(segments.items()):
        streams += ('<div hidden="" id="S:' + key + '">' + content + '</div><script>'
            + (RS + ';' if index == 0 else '') + '$RS("S:' + key + '","P:' + key + '")</script>')
    return ('<!doctype html><html><head><title>' + TITLE + ' | Taiwan News | ' + clock + '</title>'
        '<meta property="og:url" content="https://taiwannews.com.tw/zh/news/1234567">'
        '<meta property="og:title" content="' + TITLE + ' | Taiwan News | ' + clock + '">'
        '<meta property="article:published_time" content="' + clock + '"></head><body>'
        '<main><!--$?--><template id="B:0"></template>載入中<!--/$--></main>' + streams
        + '<script>' + RC + '</script></body></html>').encode()


@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('label', ['台灣英文新聞－編輯', '台灣英文新聞－記者', '台灣英文新聞－時評'])
def test_whole_prose_native_roles_and_minute_without_invented_offset(tmp_path, nested, label):
    result = extract_article(page(nested=nested, label=label), source(tmp_path), page_url=URL)
    assert result['body'] == '\n'.join(PARAGRAPHS)
    assert result['title'] == TITLE and result['published_at_raw'] == CLOCK
    assert result['modified_at_raw'] is None and not result['historical_point_in_time']
    assert 'timezone_and_historical_release_observation_not_verified' in result['published_time_basis']
    assert result['declared_canonical_url'] is None
    credit = json.loads(result['author_attribution_detail_json'])
    assert credit['native_credit_role'] == label and credit['native_profile_href'] == '/zh/journalist/20'
    assert not credit['human_reporter_identity_verified'] and not credit['original_ownership_verified']
    assert credit['editor_or_desk_is_not_inferred_reporter']
    assert '中央社' not in result['body'] and '其他文章不是正文' not in result['body'] and '支持我們' not in result['body']


@pytest.mark.parametrize('old,new', [
    ('$RS("S:2","P:2")', '$RS("S:2","P:1")'),
    ('$RS("S:2","P:2")', '$RS("S:2","P:2");evil()'),
    ('b.parentNode.removeChild(b)', 'b.remove()'),
    ('$RC("B:0","S:0")', '$RC("B:0","S:0","error")'),
    ('$RC("B:0","S:0")', '$RC("B:1","S:0")'),
    ('<!--/$-->', ''), ('<!--$?-->', '<!--$!-->'),
    ('<template id="P:2"></template>', '<template id="P:2">unexpected</template>'),
    ('<div class="editor app">', '<div class="editor app" hidden>'),
    ('<main>', '<main style="display:none">'),
    ('<template id="P:2"></template>', '<template id="P:2"></template><template id="P:2"></template>'),
    ('<div class="flex-1">', '<div class="flex-1" hidden>'),
    ('https://taiwannews.com.tw/zh/news/1234567', 'https://taiwannews.com.tw/zh/news/1234568'),
    ('https://taiwannews.com.tw/zh/news/1234567', 'https://different.example/zh/news/1234567'),
    ('<img alt="合成編輯組">', '<img alt="別的署名">'),
    ('<p>合成副標</p>', '<h1>另一篇</h1>'),
    ('合成編輯組</a>', '合成編輯\x00組</a>'),
])
def test_unbound_hidden_ambiguous_or_changed_renderer_stays_gap(tmp_path, old, new):
    payload = page().replace(old.encode(), new.encode())
    assert payload != page()
    with pytest.raises(NewsError):
        extract_article(payload, source(tmp_path), page_url=URL)


@pytest.mark.parametrize('url', [URL.replace('/zh/', '/ch/'), URL.replace('www.', ''), URL + '?x=1', URL + '#other'])
def test_no_inferred_redirect_transport_host_or_query_alias(tmp_path, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page(), source(tmp_path), page_url=url)


@pytest.mark.parametrize('credit', ['中央社報導', '本文轉載', '路透社', 'PRNewswire'])
def test_explicit_wire_in_prose_is_not_publisher_original(tmp_path, credit):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(page(body='<p>' + credit + '</p>'), source(tmp_path), page_url=URL)


def test_partner_credit_not_granted_by_same_host_or_native_profile(tmp_path):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(page(label='美通社'), source(tmp_path), page_url=URL)


@pytest.mark.parametrize('clock', ['May. 32, 2026 15:43', 'May. 12, 2026 25:43', 'May 12, 2026', '2026-05-12'])
def test_invalid_or_changed_clock_is_not_inferred(tmp_path, clock):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract_article(page(clock=clock), source(tmp_path), page_url=URL)


@pytest.mark.parametrize('markup', ['<iframe></iframe>', '<script>hidden()</script>',
    '<form>member</form>', '<span hidden>not public</span>', '<span class="hidden">not visible</span>',
    '<img alt="body picture">'])
def test_unknown_active_hidden_or_media_body_is_not_transcribed(tmp_path, markup):
    with pytest.raises(NewsError):
        extract_article(page(body='<p>公開合成文字</p>' + markup), source(tmp_path), page_url=URL)


@pytest.mark.parametrize('key,value', [('domains', ['taiwannews.com.tw']),
    ('require_author_attribution', False), ('article_selectors', ['article']),
    ('html_parser', 'html.parser'), ('exclude_selectors', ['.ad']),
    ('author_selectors', ['a']), ('allow_short_configured_body', True),
    ('sitemaps', ['https://www.taiwannews.com.tw/sitemap.xml'])])
def test_exact_reviewed_scope_cannot_expand_silently(tmp_path, key, value):
    selected = deepcopy(entry())
    selected[key] = value
    with pytest.raises(ValueError):
        PolicyRegistry(config(selected), base=tmp_path)


@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('role', ['台灣英文新聞－編輯', '台灣英文新聞－記者', '台灣英文新聞－時評'])
def test_v2_static_order_keeps_all_existing_fields_except_versioned_identity(tmp_path, nested, role):
    original = page(nested=nested, label=role)
    old = extract_article(original, source(tmp_path), page_url=URL)
    new = extract_article(original, source(tmp_path, 2), page_url=URL)
    assert {k: v for k, v in old.items() if k != 'article_identity_basis'} == {
        k: v for k, v in new.items() if k != 'article_identity_basis'}
    assert 'synchronous_move_and_root_completion_order_verified' in new['article_identity_basis']


def reordered_page(kind):
    soup = BeautifulSoup(page(), 'lxml')
    if kind == 'destination_after_move':
        # Final graph is unchanged, but P:2 has not yet been parsed when the
        # first synchronous move runs. No JavaScript is executed by this test.
        root, article, credit = [soup.find(id=key).extract() for key in ('S:0', 'S:1', 'S:2')]
        for script in list(soup.find_all('script')):
            script.extract()
        soup.body.append(root)
        soup.body.append(credit)
        first = soup.new_tag('script'); first.string = RS + ';$RS("S:2","P:2")'
        soup.body.append(first)
        soup.body.append(article)
        second = soup.new_tag('script'); second.string = '$RS("S:1","P:1")'
        soup.body.append(second)
        final = soup.new_tag('script'); final.string = RC
        soup.body.append(final)
    elif kind == 'destination_after_completion':
        soup.body.append(soup.main.extract())
    elif kind == 'destination_after_root_before_completion':
        soup.find_all('script')[-1].insert_before(soup.main.extract())
    else:
        raise AssertionError('unknown synthetic ordering')
    return str(soup).encode()


@pytest.mark.parametrize('kind', ['destination_after_move', 'destination_after_completion'])
def test_v2_rejects_future_targets_without_silently_rewriting_v1(tmp_path, kind):
    original = reordered_page(kind)
    assert extract_article(original, source(tmp_path), page_url=URL)['body'] == '\n'.join(PARAGRAPHS)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(original, source(tmp_path, 2), page_url=URL)


def test_v2_requires_targets_before_calls_not_unnecessary_order_between_targets(tmp_path):
    original = reordered_page('destination_after_root_before_completion')
    assert extract_article(original, source(tmp_path, 2), page_url=URL)['body'] == '\n'.join(PARAGRAPHS)


def test_future_version_or_lossy_NUL_never_inherits_completed_stream_permission(tmp_path):
    with pytest.raises(ValueError):
        source(tmp_path, 4)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page().replace(TITLE.encode(), (TITLE+'\x00').encode()), source(tmp_path, 2), page_url=URL)


def production_settings(version):
    from downloader.news_fulltext import load_config
    loaded, _, repo = load_config(Path('configs/news_fulltext.json'))
    settings = deepcopy(loaded)
    selected = deepcopy(next(row for row in settings['providers'] if row['id'] == 'taiwannews'))
    selected['embedded_article_layout'] = TAIWANNEWS_LAYOUT if version == 1 else TAIWANNEWS_V2_LAYOUT
    selected['adapter_verification'] = TAIWANNEWS_VERIFICATION if version == 1 else TAIWANNEWS_V2_VERIFICATION
    settings['providers'] = [selected]
    settings.pop('provider_catalog', None)
    return settings, repo


def test_v2_exact_capture_preserves_HTTP_and_authorization_but_fences_other_changes():
    settings, repo = production_settings(2)
    selected = PolicyRegistry(settings, base=repo).sources['taiwannews']
    assert (selected.data_fingerprint, selected.authorization_fingerprint) == TAIWANNEWS_V2_HTTP_BINDING[1:3]
    assert (selected.http_capacity_source_fingerprint, selected.http_capacity_authorization_fingerprint) == TAIWANNEWS_V2_HTTP_BINDING[3:]
    assert selected.parser_repair_prior_capture_fingerprint == TAIWANNEWS_V2_HTTP_BINDING[0]
    for key, value in [('interval_seconds', 3), ('parallelism', 2), ('capacity_max_parallelism', 2),
                       ('adapter_verification', 'unattested-other-capture')]:
        changed = deepcopy(settings); changed['providers'][0][key] = value
        other = PolicyRegistry(changed, base=repo).sources['taiwannews']
        assert not other.http_capacity_source_fingerprint and not other.http_capacity_authorization_fingerprint
    changed = deepcopy(settings); changed['providers'][0]['authorization']['scope'] += ' Other scope.'
    other = PolicyRegistry(changed, base=repo).sources['taiwannews']
    assert not other.http_capacity_source_fingerprint and not other.http_capacity_authorization_fingerprint


def test_v2_negative_only_upgrade_preserves_every_old_head_attempt_deadline_and_failure_budget(tmp_path, monkeypatch):
    from downloader.news_fulltext_queue import NewsQueue
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **kw: pytest.fail('zero HTTP'))
    settings, repo = production_settings(1)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        states = ['complete', 'pending', 'retry', 'article_identity_not_verified', 'article_title_not_verified',
            'article_attribution_not_verified', 'article_rights_or_scope_excluded', 'paywall_or_login',
            'awaiting_authorization', 'http_403', 'http_429', 'retry_cooldown']
        for index, state in enumerate(states):
            url = 'https://www.taiwannews.com.tw/zh/news/'+str(1234567+index)
            with queue.db:
                queue.add_articles([{'url': url}])
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=? WHERE url=?',
                    (state, state, time.time()+900, '2026-10-21T23:00:00+00:00', url))
                if state == 'complete':
                    queue.db.execute('UPDATE articles SET body_sha256=?,parquet_path=? WHERE url=?', ('a'*64, 'unchanged.parquet', url))
        with queue.db:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('failure_attempts:taiwannews', '17'))
        before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        following, _ = production_settings(2)
        queue.registry = PolicyRegistry(following, base=repo)
        queue.reconcile_policy()
        queue.reconcile_policy()
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', ('failure_attempts:taiwannews',)).fetchone()[0] == '17'
    finally:
        queue.close()
