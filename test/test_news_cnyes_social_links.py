"""Three source-social references need exact ordered, visible full-body proof."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGE = 'https://cimg.cnyes.cool/test-original-social-image.jpg'
LINKS = ('https://t.me/imoney168', 'https://www.facebook.com/groups/istock168',
         'https://line.me/R/ti/p/%40laz1344x')
FIGURE = (f'<figure><img src="{IMAGE}"><p>TG：<a href="{LINKS[0]}">{LINKS[0]}</a></p>'
          f'<p>FB 社團：<a href="{LINKS[1]}">{LINKS[1]}</a></p><p>新龍捲風操盤戰隊 Line@生活圈</p>'
          f'<p><a href="{LINKS[2]}">{LINKS[2]}</a></p></figure>')
TEXT = BeautifulSoup(FIGURE, 'html.parser').figure.get_text('\n', strip=True)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_social_supplement_all_text_links_images_preserved_not_author_or_narrative(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    value = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
        schema_caption=TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert value['body'] == BODY * 8 and value['published_at_raw'] == CLOCK
    assert json.loads(value['authors_json']) == [AUTHOR] and value['modified_at_raw'] is None
    assert value['verified_image_captions_json'] is None
    assert json.loads(value['verified_article_notices_json']) == [{
        'text': TEXT, 'kind': 'same_article_image_figure_social_links', 'image_sources': [IMAGE],
        'source_links': [{'href': link, 'text': link} for link in LINKS],
    }]
    assert 'source_current_image_figure_social_links_comparison_v1' in value['article_identity_basis']
    assert 'social_link_supplement_preserved_separately' in value['body_completeness']
    assert value['historical_point_in_time'] is False


MUTATIONS = [
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>'), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure class="unknown">'), id='unknown-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unknown-->'), id='comment'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden '), id='hidden-image'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" '), id='image-event'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<a ', '<a hidden ', 1), id='hidden-link'),
    pytest.param(lambda f: f.replace('<a ', '<a target="_blank" ', 1), id='unknown-link-attribute'),
    pytest.param(lambda f: f.replace('TG：', '未知：'), id='unknown-prefix'),
    pytest.param(lambda f: f.replace('FB 社團：', 'FB：'), id='missing-group-label'),
    pytest.param(lambda f: f.replace('操盤戰隊', '投資報酬123.45'), id='new-words'),
    pytest.param(lambda f: f.replace(LINKS[0], 'https://t.me/imoney168?unknown=1'), id='query'),
    pytest.param(lambda f: f.replace(LINKS[0], 'https://t.me/imoney168#unknown'), id='fragment'),
    pytest.param(lambda f: f.replace(LINKS[0], 'https://t.me.evil.test/imoney168'), id='foreign-host'),
    pytest.param(lambda f: f.replace(LINKS[0], 'https://user:secret@t.me/imoney168'), id='credential'),
    pytest.param(lambda f: f.replace(LINKS[0], 'javascript:unknown()'), id='script-url'),
    pytest.param(lambda f: f.replace(LINKS[0], 'http://[invalid'), id='malformed'),
    pytest.param(lambda f: f.replace('>https://t.me/imoney168<', '>another label<'), id='changed-link-text'),
    pytest.param(lambda f: f.replace('>https://t.me/imoney168<', '><span>https://t.me/imoney168</span><'), id='nested-link'),
    pytest.param(lambda f: f.replace('</figure>', '<p>additional source value -12.5%</p></figure>'), id='extra-narrative'),
    pytest.param(lambda f: f.replace('</figure>', '<p></p></figure>'), id='extra-placeholder'),
    pytest.param(lambda f: f.replace('<p>新龍', '<p><span>新龍').replace('Line@生活圈</p>', 'Line@生活圈</span></p>'), id='nested-label'),
]


@pytest.mark.parametrize('mutate', MUTATIONS)
@pytest.mark.parametrize('representation', ['visible', 'current', 'both'])
def test_matching_schema_does_not_authorize_unknown_shapes_or_hidden_words(cnyes_config, tmp_path, mutate, representation):
    changed = mutate(FIGURE)
    text = BeautifulSoup(changed, 'html.parser').figure.get_text('\n', strip=True) if representation == 'both' else TEXT
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=changed if representation in {'current', 'both'} else FIGURE,
            visible_figure=changed if representation in {'visible', 'both'} else FIGURE, schema_caption=text),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [TEXT[:-1], TEXT[::-1], TEXT + ' -12.5%', TEXT.replace(LINKS[0], LINKS[1])])
def test_independent_whole_schema_never_waived_for_social_supplement(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE, schema_caption=schema),
            source(cnyes_config, tmp_path), page_url=MOBILE)


def test_healthy_narrative_without_schema_supplement_keeps_old_fastpath_not_new_notice_proof(cnyes_config, tmp_path):
    value = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE, schema_caption=''),
        source(cnyes_config, tmp_path), page_url=MOBILE)
    assert value['body'] == BODY * 8 and value['verified_article_notices_json'] is None
    assert 'source_current_image_figure_social_links_comparison_v1' not in value['article_identity_basis']


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_social_witness_nonmutating_includes_exact_ordered_forest(parser):
    node = BeautifulSoup(FIGURE, parser).figure
    before = str(node)
    key = _cnyes_structured_figure_identity(node)
    assert key == (TEXT, ((IMAGE,), tuple((link, link) for link in LINKS),
        hashlib.sha256(before.encode()).hexdigest()), 'same_article_image_figure_social_links')
    assert str(node) == before
