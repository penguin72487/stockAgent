"""Synthetic native NCHU structure; no copied publisher body or live requests."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_nchu import NCHU_REVIEWED_URL
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from stockagent.data.news_source_catalog import load_source_review_ledger


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [entry for entry in config['providers'] if entry['id'] == 'nchu']
    return config


@pytest.fixture
def page():
    title = '【公關組】合成的產學合作測試'
    prose = '<br><br>'.join(f'第{i}段是專門為測試產生的內容，<strong>公司原始值</strong>不應遺漏。' * 3 for i in range(5))
    return f'''<html><head><title>{title} - 國立中興大學(National Chung Hsing University)</title>
      <meta property="og:title" content="{title}"><meta property="og:url" content="{NCHU_REVIEWED_URL}"></head>
      <body><header>不是正文</header><main id="content"><div id="news"><div class="page-wrap">
      <section class="intro-wrap"><div class="detail"><h1 class="info-title">{title}</h1><div class="article">
      <div class="article-head"><div class="part1">
        <div class="date">更新時間：2023-06-30 11:40:24 / 張貼時間：2023-06-30 11:37:14</div>
        <div class="postBy">興新聞張貼者</div><div class="unit"><span>單位</span>秘書室</div>
        <div class="source"><span>新聞來源</span>秘書室媒體公關組</div>
      </div></div><div class="news-detail"><div class="editor"><!--開頭--><div class="spec-skin">{prose}
        <br><strong><a href="https://sec.video.nchu.edu.tw/media/1248">活動相簿</a></strong>
        <div class="figure-group"><div class="item"><h3>不是正文的照片說明</h3><img src="/not-requested.png"></div></div>
      </div><!--結束--></div><a class="btn-back">回列表</a></div></div></div></section></div></div></main>
      <footer>不是正文的頁尾</footer></body></html>'''


def parse(settings, page, url=NCHU_REVIEWED_URL):
    source = PolicyRegistry(settings, base=Path(__file__).resolve().parents[1]).sources['nchu']
    return extract_article(page.encode(), source, page_url=url)


def test_native_original_clocks_organization_and_complete_prose(settings, page):
    article = parse(settings, page)
    assert article['published_at_raw'] == '2023-06-30 11:37:14'
    assert article['modified_at_raw'] == '2023-06-30 11:40:24'
    assert 'UTC_offset_not_declared_or_inferred' in article['published_time_basis']
    assert json.loads(article['authors_json']) == ['秘書室媒體公關組']
    detail = json.loads(article['author_attribution_detail_json'])
    assert detail['named_person_author'] is None
    assert detail['posting_account_label'] == '興新聞張貼者'
    for i in range(5):
        assert f'第{i}段' in article['body']
    assert article['body'].count('公司原始值') == 15
    for excluded in ('活動相簿', '照片說明', '不是正文', '回列表', '興新聞張貼者', '更新時間'):
        assert excluded not in article['body']


@pytest.mark.parametrize('url', [NCHU_REVIEWED_URL.replace('55817', '54287'),
                                NCHU_REVIEWED_URL + '?copy=1',
                                NCHU_REVIEWED_URL.replace('www2.', 'www.')])
def test_no_inferred_alias_or_syndicated_url(settings, page, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page, url)


@pytest.mark.parametrize('old,new,error', [
    ('【公關組】', '【媒體報導】', 'article_rights_or_scope_excluded'),
    ('秘書室媒體公關組', '外部通訊社', 'article_rights_or_scope_excluded'),
    ('<span>單位</span>秘書室', '<span>單位</span>別的單位', 'article_rights_or_scope_excluded'),
    ('2023-06-30 11:40:24', '2023-06-30 11:30:24', 'invalid_bulk_publication_clock'),
    ('2023-06-30 11:37:14', '2023-02-30 11:37:14', 'invalid_bulk_publication_clock'),
    ('張貼時間：', '活動時間：', 'invalid_bulk_publication_clock'),
    ('2023-06-30 11:37:14', '2023-06-30T11:37:14+08:00', 'invalid_bulk_publication_clock'),
    ('class="date"', 'class="date" hidden', 'article_identity_not_verified'),
    ('<span>新聞來源</span>', '<span hidden>新聞來源</span>', 'article_identity_not_verified'),
    ('興新聞張貼者', '猜測的人名', 'article_attribution_not_verified'),
    ('<strong>公司原始值</strong>', '<em>未知的編輯內容</em>', 'article_identity_not_verified'),
    ('<div class="figure-group">', '<div class="changed-gallery">', 'article_identity_not_verified'),
    ('media/1248', 'media/1249', 'article_identity_not_verified'),
    ('class="spec-skin"', 'class="spec-skin" hidden', 'article_identity_not_verified'),
])
def test_source_drift_fails_closed(settings, page, old, new, error):
    with pytest.raises(NewsError, match=error):
        parse(settings, page.replace(old, new))


@pytest.mark.parametrize('declaration', [
    '<link rel="canonical" href="https://elsewhere.test/article">',
    f'<meta property="og:url" content="{NCHU_REVIEWED_URL}">',
])
def test_conflicting_or_duplicate_identity_is_not_fallback(settings, page, declaration):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page.replace('</head>', declaration + '</head>'))


@pytest.mark.parametrize('field,value', [
    ('acquisition_mode', 'continuous'), ('domains', ['www.nchu.edu.tw']),
    ('article_path_pattern', '^/news-detail/id/[0-9]+$'),
    ('article_selectors', ['body']), ('exclude_selectors', ['strong']),
    ('require_author_attribution', False), ('require_configured_container', False),
])
def test_reviewed_parser_cannot_expand_scope(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(settings, base=Path(__file__).resolve().parents[1])


def test_installed_scope_stays_finite_private():
    # The installed recipe remains finite/private and never enables export.
    config, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['nchu']
    assert source.allowed and source.acquisition_mode == 'finite'
    assert source.authorization['kind'] == 'personal_research'
    assert source.authorization.get('license') is None
    assert not config['publish'] and not config['remote_training_export']


def test_native_and_pre_registration_review_share_current_source():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    assert reviews['nchu'] == reviews['unregistered:www2.nchu.edu.tw']
    assert reviews['nchu']['capture_decision'] == 'reviewed_existing_scope'
    assert reviews['nchu']['prior_reviews'][0]['capture_decision'] == 'review_incomplete'
