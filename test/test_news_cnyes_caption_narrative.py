"""A narrative paragraph wrapped in a caption remains ordered article prose."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, factset_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGES = ['https://cimg.cnyes.cool/caption-narrative-1.jpg',
          'https://cimg.cnyes.cool/caption-narrative-2.jpg']
CAPTIONS = ['原始照片圖說：2018 年，150 人。', '原始照片圖說：左 2、右 3。']
TAIL = '原始正文 -12.50%，成交量 1,234 股，營收 22 億；保留每一個來源數字。' * 4
AFTER = '最後原始正文 567 元，不改成圖說或風險聲明。' * 4
LEAF = f'<figure><img src="{IMAGES[1]}" alt="alt 999 不是正文"><figcaption>{CAPTIONS[1]}</figcaption></figure>'
FOREST = (f'<figure><img src="{IMAGES[0]}" alt="alt 888 不是正文"><figcaption><p>{CAPTIONS[0]}</p>'
          + LEAF + f'<p>{TAIL}</p></figcaption></figure>')


def article_markup(*, forest=FOREST, wrapped_only=False):
    return forest if wrapped_only else f'<p>{BODY * 8}</p>' + forest + f'<p>{AFTER}</p>'


def schema(markup):
    return BeautifulSoup(markup, 'html.parser').get_text('\n', strip=True)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('wrapped_only', [False, True])
def test_original_narrative_stays_in_order_and_captions_stay_separate(cnyes_config, tmp_path, parser, url, streamed, wrapped_only):
    cnyes_config['providers'][0]['html_parser'] = parser
    markup = article_markup(wrapped_only=wrapped_only)
    result = extract_article(factset_page(markup, schema=schema(markup), streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    expected = TAIL if wrapped_only else BODY * 8 + '\n' + TAIL + '\n' + AFTER
    assert result['body'] == expected
    assert all(value in result['body'] for value in ('-12.50%', '1,234 股', '22 億'))
    assert '999' not in result['body'] and '888' not in result['body'] and '原始照片' not in result['body']
    assert json.loads(result['verified_image_captions_json']) == [
        {'text': text, 'image_src': image,
         'structure_basis': 'same_article_visible_nested_captions_and_source_image_order'}
        for text, image in zip(CAPTIONS, IMAGES, strict=True)]
    assert result['verified_article_notices_json'] is None
    assert result['published_at_raw'] == CLOCK and result['modified_at_raw'] is None
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert 'source_current_caption_wrapped_narrative_body_comparison_v1' in result['article_identity_basis']
    assert result['body_completeness'] == ('verified_current_visible_caption_wrapped_narrative_body; '
        'all_source_prose_preserved_in_order; original_image_captions_preserved_separately')


CHANGES = [
    pytest.param(lambda f: f.replace('-12.50%', '12.50%'), id='changed-sign'),
    pytest.param(lambda f: f.replace('1,234', '1,235'), id='changed-number'),
    pytest.param(lambda f: f.replace(IMAGES[1], IMAGES[0]), id='duplicate-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'https://cimg.cnyes.cool/other.jpg'), id='changed-image'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figcaption>', '<figcaption hidden>', 1), id='hidden-caption'),
    pytest.param(lambda f: f.replace('<p>' + TAIL, '<p style="display:none">' + TAIL), id='hidden-narrative'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" ', 1), id='image-event'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unreviewed-->', 1), id='comment'),
    pytest.param(lambda f: f.replace(TAIL, '<strong>' + TAIL + '</strong>'), id='unknown-narrative-markup'),
    pytest.param(lambda f: f.replace(LEAF, '<figure>' + LEAF + '</figure>'), id='unknown-depth'),
    pytest.param(lambda f: f.replace(LEAF, ''), id='missing-leaf'),
    pytest.param(lambda f: f.replace(LEAF, LEAF + LEAF), id='duplicate-leaf'),
    pytest.param(lambda f: f.replace('<p>' + TAIL + '</p>', '<p></p>'), id='empty-tail'),
    pytest.param(lambda f: f.replace('<p>' + TAIL + '</p>', '<p> </p>'), id='whitespace-tail'),
    pytest.param(lambda f: f.replace(LEAF + '<p>' + TAIL, '<p>' + TAIL + '</p>' + LEAF + '<p>extra'), id='changed-order'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'https://user:secret@example.org/photo'), id='credential-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'javascript:unknown()'), id='script-image'),
    pytest.param(lambda f: f.replace('alt 888', 'changed alt'), id='changed-image-metadata'),
]


@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('side', ['current', 'visible'])
def test_full_dom_and_schema_cannot_hide_one_sided_mutation(cnyes_config, tmp_path, change, side):
    markup = article_markup()
    changed = article_markup(forest=change(FOREST))
    with pytest.raises(NewsError):
        extract_article(factset_page(changed if side == 'visible' else markup,
            current=changed if side == 'current' else markup, schema=schema(markup)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('change', CHANGES[2:3] + CHANGES[4:11] + CHANGES[12:18])
def test_matching_hidden_or_unknown_dom_is_not_a_schema_waiver(cnyes_config, tmp_path, change):
    markup = article_markup(forest=change(FOREST))
    with pytest.raises(NewsError):
        extract_article(factset_page(markup, schema=schema(markup)), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_plain_single_image_caption_does_not_gain_a_narrative_promotion(cnyes_config, tmp_path):
    # Without the nested leaf this is an existing single-image rich caption,
    # not the new observed narrative shape. Preserve the older caption role;
    # do not reject valid old semantics just to make a negative test green.
    markup = article_markup(forest=FOREST.replace(LEAF, ''))
    result = extract_article(factset_page(markup, schema=schema(markup)),
                             source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8 + '\n' + AFTER
    assert 'source_current_caption_wrapped_narrative_body_comparison_v1' not in result['article_identity_basis']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_simple_and_nested_caption_pairs_keep_whole_source_order(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    before = '<figure><img src="https://cimg.cnyes.cool/before.jpg"><figcaption>前方原始圖說</figcaption></figure>'
    after = '<figure><img src="https://cimg.cnyes.cool/after.jpg"><figcaption>後方原始圖說</figcaption></figure>'
    markup = article_markup(forest=before + FOREST + after)
    result = extract_article(factset_page(markup, schema=schema(markup)),
                             source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8 + '\n' + TAIL + '\n' + AFTER
    captions = json.loads(result['verified_image_captions_json'])
    assert [(row['text'], row['image_src']) for row in captions] == [
        ('前方原始圖說', 'https://cimg.cnyes.cool/before.jpg'),
        *list(zip(CAPTIONS, IMAGES, strict=True)),
        ('後方原始圖說', 'https://cimg.cnyes.cool/after.jpg')]


@pytest.mark.parametrize('declared', [schema(article_markup()).replace('-12.50%', '12.50%'),
                                    schema(article_markup()).replace(TAIL, ''),
                                    schema(article_markup()) + '\nextra 123', TAIL])
def test_whole_schema_not_one_caption_or_tail_required(cnyes_config, tmp_path, declared):
    with pytest.raises(NewsError):
        extract_article(factset_page(article_markup(), schema=declared), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_full_forest_witness_is_nonmutating_and_typed(parser):
    figure = BeautifulSoup(FOREST, parser).figure
    before = str(figure)
    witness = _cnyes_structured_figure_identity(figure)
    assert witness[2] == 'same_article_caption_gallery_narrative_tail'
    assert witness[1][2] == hashlib.sha256(before.encode()).hexdigest()
    assert witness[1][3] == TAIL and str(figure) == before
