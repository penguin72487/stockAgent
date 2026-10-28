"""Observed written wrappers preserve all prose; media cannot hide extra prose."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_contentplatform as native
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_contentplatform import URL, RAW, page, source
from test_news_contentplatform_native_scope import entry as v4_entry


def entry():
    row = v4_entry()
    row.update(embedded_article_layout=native.CONTENTPLATFORM_WRAPPED_NATIVE_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V5_VERIFICATION)
    return row


def wrapped(*, extra='', figure_attrs='class="wp-block-image size-full"', figure_body=None):
    raw = page(credit='文／另一位原作者', extra=extra)
    # Change only the fixture's original figure; preserve explicitly supplied
    # extra media so tests exercise its declared geometry.
    raw = raw.replace(b'class="wp-caption"', figure_attrs.encode(), 1)
    soup = BeautifulSoup(raw, 'html.parser')
    body = soup.select_one('.tdb_single_content > .tdb-block-inner')
    if figure_body is not None:
        figure = body.select_one('figure')
        figure.clear()
        for node in list(BeautifulSoup(figure_body, 'html.parser').contents):
            figure.append(node.extract())
    wrapper = soup.new_tag('div')
    for node in list(body.contents):
        wrapper.append(node.extract())
    body.append(wrapper)
    return str(soup).encode()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_native_static_wrappers_and_block_images_keep_every_written_value(parser):
    raw = wrapped(extra='<div><p>額外原始值 -0.12、2,678股</p><div>原本在同一正文容器的相關連結<a href="/not-requested">原文字</a></div></div>')
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(raw, source(v4_entry()), page_url=URL)
    record = extract_article(raw, replace(source(entry()), html_parser=parser), page_url=URL)
    assert RAW in record['body'] and '文／另一位原作者' in record['body']
    assert '額外原始值 -0.12、2,678股' in record['body'] and '相關連結' in record['body']
    assert '照片說明' not in record['body'] and '推薦文不能混入' not in record['body']
    document = BeautifulSoup(raw, 'html.parser')
    written = document.select_one('article .tdb_single_content > .tdb-block-inner')
    for figure in list(written.select('figure')):
        figure.decompose()
    assert ''.join(''.join(written.stripped_strings).split()) == ''.join(record['body'].split())
    assert record['published_at_raw'] == '2026-10-14T10:43:17+08:00'
    assert record['modified_at_raw'] == '2026-10-14T10:43:19+08:00'


@pytest.mark.parametrize('changes', [
    {'figure_body': '<img src="/not-fetched"><p>正文不能當成圖片丟掉</p>'},
    {'figure_body': '<img src="/not-fetched">圖片旁的文字不能被刪掉'},
    {'figure_body': '<img src="/one"><img src="/two"><figcaption>未檢查的圖片組</figcaption>'},
    {'figure_body': '<img src="/not-fetched"><iframe src="/not-fetched"></iframe>'},
    {'figure_body': '<img src="/not-fetched"><figcaption hidden>隱藏圖說</figcaption>'},
    {'figure_attrs': 'class="unknown-media"'},
    {'extra': '<div hidden><p>隱藏正文</p></div>'},
    {'extra': '<section>尚未觀察的正文結構</section>'},
    {'extra': '<div><script>unknown_script()</script></div>'},
    {'extra': '<div><!--unknown--></div>'},
])
def test_unknown_or_hidden_media_prose_is_never_silently_dropped(changes):
    with pytest.raises(NewsError):
        extract_article(wrapped(**changes), source(entry()), page_url=URL)


def test_publisher_access_gate_still_precedes_geometry_acceptance():
    with pytest.raises(NewsError, match='^paywall_or_login$'):
        extract_article(wrapped(extra='<div class="paywall">登入閱讀</div>'), source(entry()), page_url=URL)


def test_only_attested_v4_predecessor_keeps_HTTP_refusals_for_v5():
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    candidate = next(p for p in config['providers'] if p['id'] == 'contentplatform')
    candidate.update(embedded_article_layout=native.CONTENTPLATFORM_WRAPPED_NATIVE_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V5_VERIFICATION)
    policy = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert native.contentplatform_known_native_source(policy)
    assert policy.parser_repair_prior_capture_fingerprint == native.CONTENTPLATFORM_V4_HTTP_BINDING[0]
    assert policy.authorization_fingerprint == native.CONTENTPLATFORM_V4_HTTP_BINDING[1]
    assert (policy.http_capacity_source_fingerprint, policy.http_capacity_authorization_fingerprint) == native.CONTENTPLATFORM_V4_HTTP_BINDING[2:4]
    candidate['adapter_verification'] += '_unreviewed'
    unreviewed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert unreviewed.http_capacity_source_fingerprint != policy.http_capacity_source_fingerprint
    candidate['adapter_verification'] = native.CONTENTPLATFORM_V5_VERIFICATION
    # Per-provider capture policy is the source fingerprint boundary. The
    # process-wide User-Agent is a separate HTTP/runtime contract.
    candidate['interval_seconds'] += 1
    changed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert changed.http_capacity_source_fingerprint != policy.http_capacity_source_fingerprint


def media_entry():
    row = entry()
    row.update(embedded_article_layout=native.CONTENTPLATFORM_WRAPPED_MEDIA_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V6_VERIFICATION)
    return row


def written_html_entry():
    row = media_entry()
    row.update(embedded_article_layout=native.CONTENTPLATFORM_WRITTEN_HTML_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V7_VERIFICATION)
    return row


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('media', [
    '<div><img class="wp-image-538855" src="/never-fetched.jpg"><p>保留後面的原始值 -2.5%</p></div>',
    '<div><div><figure class="wp-caption"><img src="/never-fetched.jpg"><figcaption>照片說明</figcaption></figure></div></div>',
    '<div>圖片前原文<img class="wp-image-42" src="/never-fetched.jpg">圖片後原文</div>',
    '<p>段落前<strong>原始值<img class="wp-image-42" src="/never-fetched.jpg">-0.23股</strong>段落後</p>',
    '<div><a href="/not-followed"><img class="wp-image-42" src="/never-fetched.jpg"></a><p>不是另一次抓取</p></div>',
])
def test_v6_media_placement_preserves_all_surrounding_written_text(parser, media):
    raw = wrapped(extra=media)
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(raw, source(entry()), page_url=URL)
    record = extract_article(raw, replace(source(media_entry()), html_parser=parser), page_url=URL)
    document = BeautifulSoup(raw, 'html.parser')
    written = document.select_one('article .tdb_single_content > .tdb-block-inner')
    for node in list(written.select('figure')):
        node.decompose()
    for node in list(written.select('img')):
        node.decompose()
    assert ''.join(''.join(written.stripped_strings).split()) == ''.join(record['body'].split())
    assert RAW in record['body'] and '文／另一位原作者' in record['body']
    assert '照片說明' not in record['body'] and '推薦文不能混入' not in record['body']
    assert record['published_at_raw'] == '2026-10-14T10:43:17+08:00'
    assert record['modified_at_raw'] == '2026-10-14T10:43:19+08:00'
    assert not record['historical_point_in_time'] and not record['per_article_license_verified']


@pytest.mark.parametrize('media', [
    '<div><img src="/unknown.jpg"></div>',
    '<div><img class="wp-image-0" src="/unknown.jpg"></div>',
    '<section><img class="wp-image-42" src="/never-fetched.jpg"></section>',
    '<div hidden><img class="wp-image-42" src="/never-fetched.jpg"></div>',
    '<div><img hidden class="wp-image-42" src="/never-fetched.jpg"></div>',
    '<div><figure hidden class="wp-caption"><img src="/never-fetched.jpg"></figure></div>',
    '<div><figure class="wp-caption"><img src="/never-fetched.jpg"><p>不能默默刪掉的段落</p></figure></div>',
    '<div><figure class="wp-caption"><img src="/never-fetched.jpg">不能默默刪掉的文字</figure></div>',
    '<div><figure class="unknown"><img src="/never-fetched.jpg"></figure></div>',
    '<div><figure class="wp-caption"><img src="/never-fetched.jpg"><script>unknown()</script></figure></div>',
    '<div><figure class="wp-caption"><img src="/never-fetched.jpg"><img src="/two.jpg"></figure></div>',
    '<div><figure class="wp-caption"><img src="/never-fetched.jpg"><figcaption hidden>隱藏圖說</figcaption></figure></div>',
    '<div><!--unknown--><img class="wp-image-42" src="/never-fetched.jpg"></div>',
])
def test_v6_media_does_not_make_unknown_or_hidden_subtrees_transparent(media):
    with pytest.raises(NewsError):
        extract_article(wrapped(extra=media), source(media_entry()), page_url=URL)


@pytest.mark.parametrize('changes,error', [
    ({'canonical': URL.replace('533649', '429527')}, 'article_identity_not_verified'),
    ({'entity': URL.replace('533649', '429527')}, 'article_identity_not_verified'),
    ({'published': '2026-10-14T10:43:17'}, 'invalid_bulk_publication_clock'),
    ({'modified': '2026-10-13T10:43:19+08:00'}, 'invalid_bulk_publication_clock'),
    ({'profile': 'https://example.org/author/'}, 'article_attribution_not_verified'),
    ({'credit': '中央社記者'}, 'article_rights_or_scope_excluded'),
    ({'extra': '<div class="paywall">登入閱讀</div>'}, 'paywall_or_login'),
])
@pytest.mark.parametrize('make_entry', [media_entry, written_html_entry])
def test_media_revisions_preserve_identity_clocks_attribution_and_scope_gates(changes, error, make_entry):
    with pytest.raises(NewsError) as caught:
        extract_article(page(**changes), source(make_entry()), page_url=URL)
    assert caught.value.code == error


def test_v6_only_exact_v5_predecessor_inherits_existing_HTTP_identity():
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    row = next(p for p in config['providers'] if p['id'] == 'contentplatform')
    row.update(embedded_article_layout=native.CONTENTPLATFORM_WRAPPED_MEDIA_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V6_VERIFICATION)
    current = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert native.contentplatform_known_native_source(current)
    assert current.parser_repair_prior_capture_fingerprint == native.CONTENTPLATFORM_V5_ORIGINAL_BINDING[0]
    assert current.authorization_fingerprint == native.CONTENTPLATFORM_V4_HTTP_BINDING[1]
    assert (current.http_capacity_source_fingerprint,
        current.http_capacity_authorization_fingerprint) == native.CONTENTPLATFORM_V4_HTTP_BINDING[2:4]
    row['adapter_verification'] += '_unknown'
    changed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert changed.http_capacity_source_fingerprint != current.http_capacity_source_fingerprint
    assert not changed.parser_repair_prior_capture_fingerprint
    row['adapter_verification'] = native.CONTENTPLATFORM_V6_VERIFICATION
    row['interval_seconds'] += 1
    changed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert changed.http_capacity_source_fingerprint != current.http_capacity_source_fingerprint
    assert not changed.parser_repair_prior_capture_fingerprint


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('extra', [
    '<div><img src="/not-fetched.jpg"><p>沒有CSS類別依然保留 -2.5%</p></div>',
    '<div>圖片前<img class="non-wordpress-image" src="/not-fetched.jpg">圖片後</div>',
    '<div><figure><img src="/not-fetched.jpg"><figcaption>照片說明</figcaption></figure></div>',
    '<section><figure class="another-styling-class"><a href="/not-followed"><img src="/not-fetched.jpg"></a><figcaption>照片說明</figcaption></figure><p>正文 -62.5股</p></section>',
    '<div><h1>正文小標</h1><hr><pre><code>原始值 -62.5</code></pre><dl><dt>單位</dt><dd>股</dd></dl><small>原始備註</small></div>',
    '<div><p><mark>原文</mark><abbr title="not copied">公司</abbr><cite>資料來源</cite><del>-6</del><ins>2</ins><wbr></p><table><caption>原始表頭</caption><colgroup><col></colgroup><tbody><tr><th>股數</th><td>-62.5</td></tr></tbody></table></div>',
])
def test_v7_static_written_HTML_and_media_do_not_depend_on_styling_class(parser, extra):
    raw = wrapped(extra=extra)
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(raw, source(media_entry()), page_url=URL)
    record = extract_article(raw, replace(source(written_html_entry()), html_parser=parser), page_url=URL)
    document = BeautifulSoup(raw, 'html.parser')
    written = document.select_one('article .tdb_single_content > .tdb-block-inner')
    for node in list(written.select('figure')):
        node.decompose()
    for node in list(written.select('img')):
        node.decompose()
    assert ''.join(''.join(written.stripped_strings).split()) == ''.join(record['body'].split())
    assert RAW in record['body'] and '照片說明' not in record['body']


@pytest.mark.parametrize('extra', [
    '<div><figure><img src="/not-fetched.jpg"><p>不能當圖說刪掉的正文</p></figure></div>',
    '<figure class="wp-caption"><img src="/not-fetched.jpg">不能沿用舊版寬鬆媒體判定而刪掉</figure>',
    '<div><figure><img src="/not-fetched.jpg"><figcaption hidden>不能丟棄的未知內容</figcaption></figure></div>',
    '<section hidden><p>隱藏正文</p></section>',
    '<div><script>unknown()</script></div>',
    '<div><iframe src="/not-followed"></iframe></div>',
    '<div><form><input value="hidden data"></form></div>',
    '<div><unknown-element>未確認元素</unknown-element></div>',
    '<div><!--unknown--></div>',
])
def test_v7_unknown_active_or_hidden_content_is_not_silently_removed(extra):
    with pytest.raises(NewsError):
        extract_article(wrapped(extra=extra), source(written_html_entry()), page_url=URL)


def test_v7_only_exact_v6_predecessor_inherits_existing_HTTP_identity():
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    row = next(p for p in config['providers'] if p['id'] == 'contentplatform')
    row.update(embedded_article_layout=native.CONTENTPLATFORM_WRITTEN_HTML_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V7_VERIFICATION)
    current = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert current.parser_repair_prior_capture_fingerprint == native.CONTENTPLATFORM_V6_CAPTURE
    assert current.authorization_fingerprint == native.CONTENTPLATFORM_V4_HTTP_BINDING[1]
    assert (current.http_capacity_source_fingerprint,
        current.http_capacity_authorization_fingerprint) == native.CONTENTPLATFORM_V4_HTTP_BINDING[2:4]
    row['adapter_verification'] += '_unknown'
    changed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert not changed.parser_repair_prior_capture_fingerprint
    assert changed.http_capacity_source_fingerprint != current.http_capacity_source_fingerprint
    row['adapter_verification'] = native.CONTENTPLATFORM_V7_VERIFICATION
    row['interval_seconds'] += 1
    changed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert changed.http_capacity_source_fingerprint != current.http_capacity_source_fingerprint
    assert not changed.parser_repair_prior_capture_fingerprint
