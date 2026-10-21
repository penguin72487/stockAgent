"""Reuse the already reviewed native _moz padding, not arbitrary BR markup."""
import pytest

from downloader.news_fulltext_ettoday import _finance_third_party_prelude
from downloader.news_fulltext_policy import NewsError
import test_news_ettoday as native
import test_news_ettoday_external_credit_v35 as prior


def page(parser='lxml'):
    soup = prior.page('weekly_strong', parser)
    soup.select_one('.story strong br')['type'] = '_moz'
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_terminal_native_editor_padding_has_versioned_exclusion_only(parser):
    soup = page(parser)
    before = str(soup)
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        prior.extract(soup, 35, parser)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        prior.extract(soup, 36, parser)
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['other_type', 'extra_attribute', 'hidden_break', 'hidden_credit',
    'mid_caption_text', 'nested_padding', 'second_break', 'nonempty_break'])
def test_padding_does_not_hide_or_discard_source_content(change, parser):
    soup = page(parser)
    body, br = soup.select_one('.story'), soup.select_one('.story strong br')
    if change == 'other_type': br['type'] = 'other'
    elif change == 'extra_attribute': br['onclick'] = 'unexpected()'
    elif change == 'hidden_break': br['hidden'] = ''
    elif change == 'hidden_credit': soup.select_one('#credit strong')['aria-hidden'] = 'true'
    elif change == 'mid_caption_text': br.insert_after('不是尾端空白')
    elif change == 'nested_padding': br.wrap(soup.new_tag('span'))
    elif change == 'second_break': br.insert_after(soup.new_tag('br'))
    elif change == 'nonempty_break': br.append('原始字元')
    else: raise AssertionError(change)
    try:
        assert not _finance_third_party_prelude(body, reviewed_external_credits=True, reviewed_editor_padding=True)
    except NewsError as error:
        assert change.startswith('hidden_') and error.code != 'article_rights_or_scope_excluded'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v35_valid_values_and_unverified_source_typo_stay_unchanged(parser):
    soup = native.page(parser=parser)
    assert prior.extract(soup, 35, parser) == prior.extract(soup, 36, parser)
    soup.select_one('#credit').string = '記埋研究記者／台北報導'
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        prior.extract(soup, 36, parser)
    for kind in prior.KINDS:
        for version in (35, 36):
            with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
                prior.extract(prior.page(kind, parser), version, parser)


def test_new_capture_does_not_reset_authorization_or_http_identity():
    before, after = prior.source(35), prior.source(36)
    assert before.data_fingerprint != after.data_fingerprint
    assert after.parser_repair_prior_capture_fingerprint == before.data_fingerprint
    assert before.authorization_fingerprint == after.authorization_fingerprint
    assert before.http_capacity_source_fingerprint == after.http_capacity_source_fingerprint
