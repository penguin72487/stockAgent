"""Canonical observed image-only posts never supply OCR, alt or teaser bodies."""
from contextlib import closing
import html
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, _objects, extract_article
from downloader.news_fulltext_publishers import cnyes_verified_image_only_body
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGE = 'http://cimg.cnyes.cool/prod/news/42/o/' + '0' * 32 + '.jpg'
ALT = '來源圖片說明，不是圖片內的完整正文。'
PARAGRAPH = f'<p align="center"><img src="{IMAGE}" alt="{ALT}"></p>'


def page(*, current=PARAGRAPH, visible=PARAGRAPH, streamed=False, schema_missing=False,
         schema=None, changes=None, desk=False):
    builder = streamed_mobile_page if streamed else mobile_page
    payload = builder(body='', changes={'content': html.escape(current), 'video': '', **(changes or {})},
                      schema_changes=schema)
    payload = payload.replace(b'<div id="article-container"><p></p></div>',
        ('<div id="article-container"><section style="margin-top:30px">' + visible + '</section><p></p></div>').encode())
    if schema_missing:
        payload = payload.replace(b'"articleBody": "", ', b'', 1)
    if desk:
        payload = payload.replace(('鉅亨網記者' + AUTHOR + ' 台北').encode(), '鉅亨網新聞中心'.encode())
    return payload


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [URL, URL + '?exp=a', MOBILE, MOBILE + '?exp=a', MOBILE + '?exp=b'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('schema_missing', [False, True])
@pytest.mark.parametrize('desk', [False, True])
def test_same_current_visible_source_image_is_explicit_nonbody_not_empty(cnyes_config, tmp_path, parser, url, streamed, schema_missing, desk):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='source_body_image_only') as caught:
        extract_article(page(streamed=streamed, schema_missing=schema_missing, desk=desk),
                        source(cnyes_config, tmp_path), page_url=url)
    assert caught.value.code == 'source_body_image_only' and not caught.value.retry


@pytest.mark.parametrize('visible', [
    PARAGRAPH.replace('0' * 32, '1' * 32),
    PARAGRAPH.replace(ALT, '另一張圖片說明'),
    PARAGRAPH.replace('align="center"', 'align="left"'),
    PARAGRAPH.replace('<img ', '<img hidden '),
    PARAGRAPH.replace('<p ', '<p hidden '),
    PARAGRAPH.replace('</p>', '<span>未知正文123</span></p>'),
    PARAGRAPH.replace('<img ', '<!--unknown--><img '),
    PARAGRAPH + PARAGRAPH,
    PARAGRAPH + '<iframe src="https://external.test/video"></iframe>',
    PARAGRAPH.replace('<img ', '<img data-unreviewed="true" '),
    '<div>' + PARAGRAPH + '</div>',
])
def test_different_hidden_unknown_or_mixed_representation_remains_gap(cnyes_config, tmp_path, visible):
    with pytest.raises(NewsError) as caught:
        extract_article(page(visible=visible), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('changes', [{'payment': 1}, {'payment': False}, {'newsId': 99},
    {'publishAt': 1388534401}, {'author': '另一篇作者'}, {'video': 'external-video'},
    {'url': URL.replace('/42', '/43')}])
def test_classification_never_waives_identity_free_author_clock_or_video(cnyes_config, tmp_path, changes):
    with pytest.raises(NewsError) as caught:
        extract_article(page(changes=changes), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('schema', [{'articleBody': None}, {'datePublished': '2014-01-01T00:00:00'},
    {'datePublished': '2014-01-01T00:00:01.000Z'}, {'isAccessibleForFree': False},
    {'author': [{'name': '其他作者'}]}, {'mainEntityOfPage': {'@id': URL.replace('/42', '/43')}}])
def test_unknown_or_conflicting_schema_cannot_be_called_source_image_only(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError) as caught:
        extract_article(page(schema=schema), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('mutate', [
    lambda p: p.replace('<section style="margin-top:30px">', '<section style="margin-top:30px" hidden>'),
    lambda p: p.replace('<div id="article-container">', '<div id="article-container" hidden>'),
    lambda p: p.replace('<span class="signature">', '<span class="signature" hidden>'),
    lambda p: p.replace('鉅亨網記者' + AUTHOR + ' 台北', '無法驗證的另一篇作者'),
    lambda p: p.replace('</section><p></p>', '</section><p>未知正文789</p>'),
    lambda p: p.replace('</section><p></p>', '</section><p><!--unknown--></p>'),
    lambda p: p.replace(IMAGE, 'https://unreviewed.test/image.jpg'),
    lambda p: p.replace(IMAGE, IMAGE + '?private-token=unknown'),
    lambda p: p.replace('<p align="center">', '<p align="center">正文'),
])
def test_renderer_wrappers_credits_and_primary_image_scope_are_not_guessed(cnyes_config, tmp_path, mutate):
    payload = mutate(page().decode()).encode()
    with pytest.raises(NewsError) as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


def test_source_outcome_is_persisted_without_an_article_or_accepted_receipt(cnyes_config, tmp_path):
    policy = source(cnyes_config, tmp_path)
    from downloader.news_fulltext_policy import PolicyRegistry
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': URL}])
        row = queue.claim_url(URL, 120)
        try:
            extract_article(page(), policy, page_url=URL)
        except NewsError as error:
            queue.failed(row, error, 6)
        stored = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()
        assert tuple(stored) == ('source_body_image_only', 'source_body_image_only', None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert not list((tmp_path / 'bodies').rglob('*.parquet'))


def test_unknown_mixed_schema_text_is_not_discarded_as_image_only(cnyes_config, tmp_path):
    payload = page(schema={'articleBody': BODY * 8})
    soup = BeautifulSoup(payload, 'html.parser')
    article = next(node for script in soup.find_all('script', type='application/ld+json')
                   for node in _objects(json.loads(script.get_text())) if node.get('@type') == 'NewsArticle')
    assert not cnyes_verified_image_only_body(soup, source(cnyes_config, tmp_path), article, page_url=URL)

