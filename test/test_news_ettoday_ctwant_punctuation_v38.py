"""An explicit native source label is an exclusion, not a body permission."""
import pytest

from downloader.news_fulltext_ettoday import _finance_third_party_prelude
from downloader.news_fulltext_policy import NewsError, extract_article
import test_news_ettoday as native
import test_news_ettoday_bare_signature_v37 as previous
import test_news_ettoday_third_party_prelude_v34 as external


def source(parser='lxml', version=38):
    return previous.source(parser=parser, version=version)


def page(parser='lxml'):
    soup = external.page(parser)
    soup.select_one('#credit').string = '圖、文／CTWANT'
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_literal_source_punctuation_only_changes_classification_in_new_contract(parser):
    soup = page(parser)
    original = str(soup)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract_article(original.encode(), source(parser), page_url=native.URLS[0])
    for version in (33, 34, 35, 36, 37):
        with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
            extract_article(original.encode(), source(parser, version), page_url=native.URLS[0])
    assert str(soup) == original


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_credit', 'hidden_caption', 'hidden_image', 'nested_credit',
    'linked_credit', 'caption_credit', 'extra_text', 'wrong_source', 'different_punctuation',
    'later_credit', 'own_credit_first', 'mixed_image', 'nonparagraph', 'nested_caption'])
def test_quoted_hidden_mixed_late_or_unknown_source_is_not_guessed(parser, change):
    soup = page(parser)
    body, credit = soup.select_one('.story'), soup.select_one('#credit')
    if change.startswith('hidden_'):
        target = {'hidden_credit': credit, 'hidden_caption': body.select_one('strong'),
                  'hidden_image': body.select_one('img')}[change]
        target['hidden'] = ''
    elif change == 'nested_credit': credit.string.wrap(soup.new_tag('strong'))
    elif change == 'linked_credit': credit.string.wrap(soup.new_tag('a', href='https://example.test/'))
    elif change == 'caption_credit':
        body.select_one('strong').string = '▲正文引用圖、文／CTWANT，不是署名。'
        credit.string = '記者研究記者／台北報導'
    elif change == 'extra_text': credit.append('不能刪掉的原始正文。')
    elif change == 'wrong_source': credit.string = '圖、文／其他來源'
    elif change == 'different_punctuation': credit.string = '圖，文／CTWANT'
    elif change == 'later_credit':
        paragraph = soup.new_tag('p'); paragraph.string = native.PROSE
        credit.insert_before(paragraph)
    elif change == 'own_credit_first':
        paragraph = soup.new_tag('p'); paragraph.string = '記者研究記者／台北報導'
        body.insert(0, paragraph)
    elif change == 'mixed_image': body.select_one('img').insert_after('原始文字不得刪去。')
    elif change == 'nonparagraph': body.select_one('p').name = 'div'
    elif change == 'nested_caption': body.select_one('strong').string.wrap(soup.new_tag('span'))
    else: raise AssertionError(change)
    before = str(soup)
    try:
        assert not _finance_third_party_prelude(body, reviewed_external_credits=True,
            reviewed_editor_padding=True, reviewed_ctwant_punctuation=True)
    except NewsError as error:
        assert change.startswith('hidden_') and error.code != 'article_rights_or_scope_excluded'
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_www_does_not_inherit_finance_exclusion_and_valid_reporters_keep_all_fields(parser):
    soup = native.page('www', parser)
    soup.select_one('#credit').string = '圖、文／CTWANT'
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(str(soup).encode(), source(parser), page_url=native.URLS[1])
    for host in ('finance', 'www'):
        for specimen in (native.page(host, parser), previous.page(host, parser)):
            raw = str(specimen).encode()
            url = native.URLS[host == 'www']
            assert extract_article(raw, source(parser), page_url=url) == extract_article(
                raw, source(parser, 37), page_url=url)
    # The previous literal remains an exclusion under both contracts.
    for version in (37, 38):
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
            extract_article(str(external.page(parser)).encode(), source(parser, version), page_url=native.URLS[0])


def test_only_capture_identity_changes_and_prior_capture_is_exact():
    old, new = source(version=37), source()
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
