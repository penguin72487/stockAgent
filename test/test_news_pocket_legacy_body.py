"""Synthetic witnessed CMS shapes, no site text, media, aliases or requests."""
from copy import deepcopy
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError
from test_news_pocket_original import settings, page, versioned, POCKET_AUTHOR
from test_news_pocket_native_blocks import parse, flat
from test_news_pocket_own_account_cta import raw_action, nonterminal_cta


STYLE = 'color: rgb(122, 128, 135); font-size: 14px;'


def inline_label():
    soup = BeautifulSoup(page(), 'lxml')
    root = soup.select_one('article.pocket-article')
    root.select_one('.article-meta').decompose()
    label = BeautifulSoup(f'<p><span style="{STYLE}">更新於 2026-09-11 </span>'
        f'<span aria-hidden="true" style="{STYLE}">｜</span>'
        f'<span style="{STYLE}"> {POCKET_AUTHOR}</span></p>', 'lxml').p
    root.insert(0, label)
    return soup


def context_summary():
    soup = BeautifulSoup(page(), 'lxml')
    root = soup.select_one('article.pocket-article')
    root.select_one('.article-meta').decompose()
    content = BeautifulSoup('''<div>
        <p class="article-meta">資料說明：保留 -12.50、0 及 2.8，不推定發布日。</p>
        <p class="article-meta">研究用途說明仍是原生文字，不是另一個時鐘。</p>
        <aside aria-label="重點速覽" class="key-takeaways"><strong>1個重點速覽</strong>
          <ul><li>原始摘要數值 -7.25，僅出現一次。</li></ul></aside>
        <nav aria-label="文章目錄" class="toc-box"><strong>文章目錄</strong>
          <ol><li><a href="#native-section">章節索引不是第二份正文</a></li></ol></nav>
        <h2 id="native-section">保留章節與正文</h2></div>''', 'lxml').div
    for child in reversed(list(content.contents)):
        root.insert(0, child.extract())
    return soup


def illustrated():
    soup = BeautifulSoup(page(), 'lxml')
    root = soup.select_one('article.pocket-article')
    figure = BeautifulSoup('<figure style="margin:28px 0;">'
        '<img src="/unfetched.webp" alt="不是正文的圖中文字"/>'
        '<figcaption>原生圖說保留數值 -8.79 與 0。</figcaption></figure>', 'lxml').figure
    root.append(figure)
    return soup


def index_cta():
    soup = BeautifulSoup(raw_action(), 'lxml')
    soup.select_one('#section-cta').string = '掌握富時指數調整，在口袋證券自行追蹤台股'
    soup.select_one('a[href*="/openaccountonline/"]').string = '線上5分鐘快速申請'
    return soup


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('factory', [inline_label, context_summary, illustrated, index_cta])
def test_v9_actual_boundary_shapes_preserve_values_and_clocks(settings, engine, factory):
    raw = str(factory())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 8), raw)
    config = versioned(settings, 9);config['providers'][0]['html_parser'] = engine
    result = parse(config, raw)
    reference = parse(versioned(settings, 8), page())
    assert result['published_at_raw'] == reference['published_at_raw']
    assert result['modified_at_raw'] == reference['modified_at_raw']
    assert result['authors_json'] == reference['authors_json']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    for i in range(4):
        assert result['body'].count(f'第{i}段') == 1
    notices = json.loads(result['verified_article_notices_json'])
    if factory is inline_label:
        assert '更新於' not in result['body'] and '｜' not in result['body']
        note = next(n for n in notices if n['kind'].startswith('native_inline_update_label'))
        assert note['calendar_date'] == '2026-09-11' and not note['hidden_prose_made_visible']
    elif factory is context_summary:
        assert result['body'].count('原始摘要數值 -7.25') == 1
        assert '資料說明：保留 -12.50、0 及 2.8' in result['body']
        assert '研究用途說明仍是原生文字' in result['body']
        assert result['body'].index('資料說明') < result['body'].index('原始摘要') < result['body'].index('第0段')
        assert '章節索引不是第二份正文' not in result['body']
        note = next(n for n in notices if n['kind'].startswith('native_article_meta_paragraphs'))
        assert note['paragraph_count'] == 2 and note['not_promoted_to_release_or_revision_clock']
    elif factory is illustrated:
        assert result['body'].count('原生圖說保留數值 -8.79 與 0') == 1
        assert '不是正文的圖中文字' not in result['body']
        assert all(not n['media_requested'] for n in notices if n['kind'].startswith('unfetched_inline'))
    else:
        assert result['body'] == reference['body']
        assert '自行追蹤' not in result['body'] and '線上5分鐘' not in result['body']


@pytest.mark.parametrize('raw', [page(), page(nested=False), flat(), raw_action(), str(nonterminal_cta())])
def test_v9_previous_entire_output_unchanged(settings, raw):
    assert parse(versioned(settings, 9), raw) == parse(versioned(settings, 8), raw)


@pytest.mark.parametrize('mutation', ['glyph', 'hidden_prose', 'date', 'style', 'extra', 'hidden_label', 'wrong_author'])
def test_inline_label_does_not_make_unknown_hidden_text_visible(settings, mutation):
    soup = inline_label();label = soup.select_one('article.pocket-article > p')
    marker = label.select_one('[aria-hidden]')
    if mutation == 'glyph':marker.string = '-12.50'
    elif mutation == 'hidden_prose':marker.append(soup.new_tag('b'))
    elif mutation == 'date':label.span.string = '更新於 2026-02-30 '
    elif mutation == 'style':marker['style'] = STYLE + 'display:none;'
    elif mutation == 'extra':label.append('不得被吞掉的文字')
    elif mutation == 'hidden_label':label['hidden'] = ''
    else:label.find_all('span')[2].string = '外部來源'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 9), str(soup))


@pytest.mark.parametrize('mutation', ['hidden_context', 'nonparagraph', 'hidden_summary', 'count',
    'unknown_aside', 'nested_summary', 'hidden_caption', 'orphan_caption', 'competing_caption',
    'figure_prose', 'hidden_figure', 'unknown_toc', 'bad_toc_target', 'hidden_toc'])
def test_only_reviewed_containers_and_visible_context_are_admitted(settings, mutation):
    soup = illustrated() if 'caption' in mutation or 'figure' in mutation else context_summary()
    if mutation == 'hidden_context':soup.select_one('p.article-meta')['aria-hidden'] = 'true'
    elif mutation == 'nonparagraph':soup.select_one('p.article-meta').name = 'div'
    elif mutation == 'hidden_summary':soup.select_one('.key-takeaways li')['hidden'] = ''
    elif mutation == 'count':soup.select_one('.key-takeaways strong').string = '2個重點速覽'
    elif mutation == 'unknown_aside':soup.select_one('aside')['class'] = ['unknown']
    elif mutation == 'nested_summary':soup.select_one('aside').wrap(soup.new_tag('div'))
    elif mutation == 'hidden_caption':soup.figcaption['aria-hidden'] = 'true'
    elif mutation == 'orphan_caption':soup.figure.insert_after(soup.figcaption.extract())
    elif mutation == 'competing_caption':soup.figure.append(deepcopy(soup.figcaption))
    elif mutation == 'figure_prose':soup.figure.append('不得漏掉的文字')
    elif mutation == 'hidden_figure':soup.figure['hidden'] = ''
    elif mutation == 'unknown_toc':soup.select_one('article.pocket-article nav')['class'] = ['unknown']
    elif mutation == 'bad_toc_target':soup.select_one('nav.toc-box a')['href'] = '#missing'
    else:soup.select_one('nav.toc-box')['hidden'] = ''
    with pytest.raises(NewsError):
        parse(versioned(settings, 9), str(soup))


@pytest.mark.parametrize('mutation', ['heading', 'action', 'host', 'duplicate', 'hidden'])
def test_further_cta_shape_does_not_expand_actions_or_scope(settings, mutation):
    soup = index_cta();heading = soup.select_one('#section-cta')
    action = soup.select_one('a[href*="/openaccountonline/"]')
    if mutation == 'heading':heading.string = '未核對的標題'
    elif mutation == 'action':action.string = '未核對的動作'
    elif mutation == 'host':action['href'] = action['href'].replace('www.pocket.tw', 'unknown.example.org')
    elif mutation == 'duplicate':action.insert_after(deepcopy(action))
    else:action['aria-hidden'] = 'true'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(versioned(settings, 9), str(soup))
