"""Source completeness evidence is independent of body success or length."""
from copy import deepcopy
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page, source
from test_news_cnyes_image_outcomes import IMAGE, page as image_page


QUOTES = (
    '520.78  -2.73(-0.52%)：(AAPL), NFLX, LNKD, BIDU, AMZN.',
    '(AEGR) 26.64 -', '(CLNY) 20.95', '(JDSU) 13.47', '(OTT) 2.39', '(NSPR) 1.97 : C',
    '((INTL) 16.40 -0.09 :', '(TSRO) 19.09 : .....', '907.57  20.47(2.31%)',
    '25.24：(ITT)', '50.37  5.37(11.93%)：SolarCitySolarCity', '18.31  1.34(7.90%)',
    '2.21  0.21(10.50%)：(ATEC)', '(VIPS) 28.94 : 22, 2013.',
    '229.80  15.81(7.39%)：Netflix (NFLX)',
)
EMPTY = ('<div>\n<div></div>\n</div>', '<p>&nbsp;</p>\n<div></div>')
BORDER_STYLE = ('BORDER-BOTTOM: #000000 1px solid; BORDER-LEFT: #000000 1px solid; '
                'BORDER-TOP: #000000 1px solid; BORDER-RIGHT: #000000 1px solid')


def image_markup(count, attributes='alt=""', paragraph='align="center"'):
    return '\n'.join(f'<p {paragraph}><img src="{IMAGE.replace("0" * 32, format(n, "032x"))}" {attributes}></p>'
                     for n in range(count))


IMAGES = (
    image_markup(1, 'border="0"', 'style="text-align: center;"'),
    image_markup(8),
    image_markup(20, f'border="1" alt="1" style="{BORDER_STYLE}"') + '<p align="center"></p>',
    image_markup(4),
    image_markup(1, 'alt="图片说明" border="0" width="590" height="725"', 'class="p_center"'),
)
AD = ('<div class="i1y9ycug"><div class="a1mzdm1v" id="div-gpt-ad-cnyes_news_article_middle_300*250" '
      'style="--a1mzdm1v-0:300px;--a1mzdm1v-1:250px;--a1mzdm1v-2:300px;--a1mzdm1v-3:250px;'
      '--a1mzdm1v-4:0;--a1mzdm1v-5:0;--a1mzdm1v-6:auto;--a1mzdm1v-7:auto">'
      '<span aria-busy="true" aria-live="polite"><span class="react-loading-skeleton" '
      'style="border-radius:6px;width:100%;height:100%">\u200c</span><br></span></div></div>')


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('body', QUOTES)
def test_entire_literal_quote_values_are_source_outcomes_not_prose(cnyes_config, tmp_path, parser, body):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='^source_body_quote_only$') as error:
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)
    assert not error.value.retry


@pytest.mark.parametrize('body', QUOTES)
@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'publishAt': 1388534401}},
    {'change': {'content': '<p>different source</p>'}}, {'change': {'video': 'source video'}},
    {'visible': 'unrelated prose'}, {'schema_change': {'articleBody': 'unrelated prose'}},
    {'canonical': URL.replace('/42', '/43')}, {'conflict': True}, {'duplicate_key': True},
])
def test_new_quotes_preserve_free_identity_clock_and_whole_source_evidence(cnyes_config, tmp_path, body, options):
    with pytest.raises(NewsError) as error:
        extract_article(short_page(body=body, **options), source(cnyes_config, tmp_path), page_url=URL)
    assert error.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('body', QUOTES)
@pytest.mark.parametrize('extra', [' useful prose', ' 中文正文', ' 123.45', ' (OTHER) (EXTRA)', '::'])
def test_new_quote_grammar_never_discards_extra_source_words_or_values(cnyes_config, tmp_path, body, extra):
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    soup = BeautifulSoup(short_page(body=body + extra), 'lxml')
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, body+extra, page_url=URL)


@pytest.mark.parametrize('body', QUOTES)
@pytest.mark.parametrize('media', ['img', 'video', 'audio', 'iframe', 'object', 'embed', 'table', 'script'])
def test_new_quote_view_does_not_erase_media_tables_or_script_content(cnyes_config, tmp_path, body, media):
    payload = short_page(body=body).replace(b'</p></div>', f'</p><{media}>unknown</{media}></div>'.encode())
    with pytest.raises(NewsError) as error:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert error.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('content', EMPTY)
def test_structural_blank_is_verified_absence_not_missing_prose(cnyes_config, tmp_path, parser, content):
    cnyes_config['providers'][0]['html_parser'] = parser
    payload = image_page(current=content, visible=content)
    with pytest.raises(NewsError, match='^source_article_empty_template$') as error:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert not error.value.retry


@pytest.mark.parametrize('content', [
    '<div><div hidden></div></div>', '<div><!--unknown--><div></div></div>',
    '<div><script></script></div>', '<div><iframe></iframe></div>',
    '<div><p>real text</p></div>', '<div><img alt="not prose"></div>',
    '<div><div data-unknown="missing"></div></div>', '<div><svg></svg></div>',
    '<div>' * 33 + '</div>' * 33,
])
def test_blank_classifier_keeps_unknown_hidden_media_and_text_as_gaps(cnyes_config, tmp_path, content):
    with pytest.raises(NewsError) as error:
        extract_article(short_page(body='', visible='', change={'content': content}), source(cnyes_config, tmp_path), page_url=URL)
    assert error.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('markup', IMAGES)
def test_whole_same_native_image_forest_is_not_a_text_download(cnyes_config, tmp_path, parser, markup):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='^source_body_image_only$') as error:
        extract_article(image_page(current=markup, visible=markup), source(cnyes_config, tmp_path), page_url=URL)
    assert not error.value.retry


@pytest.mark.parametrize('markup', IMAGES)
@pytest.mark.parametrize('mutate', [
    lambda text: text.replace('/42/', '/43/'),
    lambda text: text.replace('<img ', '<img hidden '),
    lambda text: text.replace('<img ', '<img data-unknown="true" '),
    lambda text: text + '<p>real narrative must be kept</p>',
    lambda text: text + '<iframe></iframe>',
    lambda text: text + '<!--unknown-->',
    lambda text: text.replace('cimg.cnyes.cool', 'different.test'),
    lambda text: text.replace('border="0"', 'border="2"'),
])
def test_new_image_forest_keeps_unknown_changes_and_order(cnyes_config, tmp_path, markup, mutate):
    changed = mutate(markup)
    if changed == markup:
        return  # Only border-0 shapes carry that field.
    with pytest.raises(NewsError) as error:
        extract_article(image_page(current=markup, visible=changed), source(cnyes_config, tmp_path), page_url=URL)
    assert error.value.code != 'source_body_image_only'


@pytest.mark.parametrize('count', [32, 33])
def test_image_count_and_whole_forest_are_bounded(cnyes_config, tmp_path, count):
    markup = image_markup(count)
    with pytest.raises(NewsError) as error:
        extract_article(image_page(current=markup, visible=markup), source(cnyes_config, tmp_path), page_url=URL)
    assert (error.value.code == 'source_body_image_only') == (count == 32)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('markup', IMAGES)
def test_actual_named_blank_ad_does_not_break_whole_image_evidence(cnyes_config, tmp_path, parser, markup):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='^source_body_image_only$'):
        extract_article(image_page(current=markup, visible=markup+AD), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('mutate', [
    lambda value:value.replace('article_middle_300*250','article_different'),
    lambda value:value.replace('class="i1y9ycug"','class="other"'),
    lambda value:value.replace('\u200c','real text'),
    lambda value:value.replace('\u200c','\u200c<!--unknown-->'),
    lambda value:value.replace('<div class="i1y9ycug"','<div hidden class="i1y9ycug"'),
    lambda value:value.replace('aria-busy="true"','aria-busy="false"'),
    lambda value:value.replace('<br>','<br><iframe></iframe>'),
    lambda value:value.replace('border-radius:6px','border-radius:5px'),
])
def test_unknown_ad_like_container_never_erases_source_fields(cnyes_config, tmp_path, mutate):
    markup = IMAGES[1]
    with pytest.raises(NewsError) as error:
        extract_article(image_page(current=markup, visible=markup+mutate(AD)), source(cnyes_config, tmp_path), page_url=URL)
    assert error.value.code != 'source_body_image_only'


@pytest.mark.parametrize('parser', ['lxml','html.parser'])
def test_all_blank_chunks_correspond_to_entire_source_forest(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    payload = image_page(current=EMPTY[1],visible='<p>&nbsp;</p>').replace(
        b'</section><p></p></div>', b'</section><p></p><section style="margin-top:30px">\n</section>'
        b'<section style="margin-top:30px"><div></div></section></div>')
    with pytest.raises(NewsError,match='^source_article_empty_template$'):
        extract_article(payload,source(cnyes_config,tmp_path),page_url=URL)


@pytest.mark.parametrize('extra', ['<section style="margin-top:30px"><p></p></section>',
    '<section style="margin-top:30px"><script></script></section>',
    '<section style="margin-top:30px"><p>actual text</p></section>',
    '<section hidden style="margin-top:30px"></section>', '<div></div>'])
def test_blank_chunks_do_not_hide_changed_forest_or_unsupported_renderer(cnyes_config,tmp_path,extra):
    payload = image_page(current=EMPTY[0],visible=EMPTY[0]).replace(
        b'</section><p></p></div>', ('</section><p></p>'+extra+'</div>').encode())
    with pytest.raises(NewsError) as error:
        extract_article(payload,source(cnyes_config,tmp_path),page_url=URL)
    assert error.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('field,now,before', [
    ('verified_empty_html_outcome_contract_version', 8, 7),
    ('verified_native_quote_only_outcome_contract_version', 7, 6),
    ('verified_native_image_only_outcome_contract_version', 3, 2),
])
def test_failed_outcome_versions_do_not_rewrite_bulk_or_permissions(cnyes_config, tmp_path, monkeypatch, field, now, before):
    current = source(cnyes_config, tmp_path)
    original = hashlib.sha256

    def previous(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and field in payload:
            payload = deepcopy(payload)
            assert payload[field] == now
            payload[field] = before
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', previous)
    older = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != older.data_fingerprint
    assert current.authorization_fingerprint == older.authorization_fingerprint
    assert current.cnyes_bulk_data_fingerprint == older.cnyes_bulk_data_fingerprint
