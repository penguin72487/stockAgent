"""Native external-header topology observed by the v143 metadata-only probe.

The structural ownership follows that source evidence; prose and writer names
below are synthetic. Passing these tests is not acceptance of native articles.
"""
from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_globalvoices_untitled import BODY, CLOCK, URL, page, source


def external_header_page():
    soup = BeautifulSoup(page(), 'lxml')
    soup.body['class'] = ['single-post', 'postid-164959']
    header = soup.select_one('.post-header').extract()
    container = soup.select_one('.post-container').extract()
    scope = soup.new_tag('div', id='page-content')
    scope['class'] = ['page-content']
    header_wrapper = soup.new_tag('div')
    header_wrapper['class'] = ['post-header-container']
    header_wrapper.append(header)
    body_wrapper = soup.new_tag('div')
    body_wrapper['class'] = ['main-wrapper']
    body_wrapper.append(container)
    scope.append(header_wrapper)
    scope.append(body_wrapper)
    soup.body.append(scope)
    return soup


def test_native_external_header_binds_one_numeric_post_without_inventing_a_title():
    result = extract_article(str(external_header_page()).encode(), source(), page_url=URL)
    assert result['body'] == BODY and result['title'] == ''
    assert result['authors_json'] == '["Writer"]' and result['published_at_raw'] == CLOCK
    assert result['per_article_license_verified']
    assert result['title_basis'] == 'source_visible_title_empty_verified_numeric_article'
    assert 'same_single_page_content_external_header_and_body_post_id' in result['article_identity_basis']


@pytest.mark.parametrize('change', [
    'wrong_body_post_id', 'missing_single_post_class', 'missing_body_post_id',
    'duplicate_page_content', 'missing_page_content', 'body_in_different_page_content',
    'title_in_different_header', 'credit_in_different_header', 'multiple_headers',
    'missing_header_wrapper', 'wrong_numeric_post', 'entry_outside_numeric_post',
    'footer_writer_outside_numeric_post', 'license_outside_numeric_post',
    'wrong_canonical', 'missing_clock', 'unaware_clock', 'different_footer_author',
    'foreign_profile', 'hidden_header', 'hidden_body_scope',
])
def test_external_header_does_not_borrow_related_credits_or_bypass_source_binding(change):
    soup = external_header_page()
    scope = soup.select_one('#page-content')
    if change == 'wrong_body_post_id': soup.body['class'] = ['single-post', 'postid-164881']
    elif change == 'missing_single_post_class': soup.body['class'] = ['postid-164959']
    elif change == 'missing_body_post_id': soup.body['class'] = ['single-post']
    elif change == 'duplicate_page_content': soup.body.append(soup.new_tag('div', id='page-content'))
    elif change == 'missing_page_content': scope['id'] = 'unbound-content'
    elif change == 'body_in_different_page_content': soup.body.append(soup.select_one('.post-container').extract())
    elif change in {'title_in_different_header', 'credit_in_different_header'}:
        other = soup.new_tag('div'); other['class'] = ['other-story']
        node = soup.select_one('.post-title' if change.startswith('title') else '#post-header-credit')
        other.append(node.extract()); scope.append(other)
    elif change == 'multiple_headers':
        extra = soup.new_tag('div'); extra['class'] = ['post-header']; soup.select_one('.post-header-container').append(extra)
    elif change == 'missing_header_wrapper': soup.select_one('.post-header-container')['class'] = ['unknown-header']
    elif change == 'wrong_numeric_post': soup.select_one('.post')['class'] = ['post', 'post-164881']
    elif change == 'entry_outside_numeric_post': soup.select_one('.post-container').append(soup.select_one('.entry').extract())
    elif change == 'footer_writer_outside_numeric_post':
        soup.select_one('.post-container').append(soup.select_one('.postfooter-credits').extract())
    elif change == 'license_outside_numeric_post':
        extra = soup.new_tag('div'); extra['class'] = ['post-credit-container']
        extra.append(soup.select_one('[rel="license"]').extract()); soup.select_one('.post-container').append(extra)
    elif change == 'wrong_canonical': soup.select_one('[rel="canonical"]')['href'] = URL.replace('164959', '164881')
    elif change == 'missing_clock': soup.select_one('[property="article:published_time"]').decompose()
    elif change == 'unaware_clock': soup.select_one('[property="article:published_time"]')['content'] = '2010-09-24T21:02:07'
    elif change == 'different_footer_author': soup.select_one('.contributor-name a').string = 'Different'
    elif change == 'foreign_profile': soup.select('#post-header-credit a.user-link')[-1]['href'] = 'https://foreign.test/author/writer/'
    elif change == 'hidden_header': soup.select_one('.post-header')['hidden'] = ''
    elif change == 'hidden_body_scope': soup.select_one('.main-wrapper')['hidden'] = ''
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), source(), page_url=URL)
