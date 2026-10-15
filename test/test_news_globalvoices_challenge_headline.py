"""A source-observed news headline is not proof of a provider denial.

Title and native external-header topology follow the metadata-only v144
observation. Prose and writer below are synthetic, not native acceptance.
"""
from dataclasses import replace
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_globalvoices_external_header import external_header_page
from test_news_globalvoices_untitled import BODY, source


URL = 'https://globalvoices.org/2010/08/23/st-vincent-the-grenadines-haiti-access-denied/'
TITLE = 'St. Vincent & the Grenadines, Haiti: Access Denied'
CLOCK = '2010-08-23T17:31:59+0000'


def native_headline_page(title=TITLE):
    soup = external_header_page()
    soup.select_one('[rel="canonical"]')['href'] = URL
    soup.select_one('.post-title').string = title
    soup.select_one('[property="article:published_time"]')['content'] = CLOCK
    soup.body['class'] = ['single-post', 'postid-158298']
    soup.select_one('.post')['class'] = ['post', 'post-158298', 'type-post', 'status-publish']
    document_title = soup.new_tag('title')
    document_title.string = title + ' · Global Voices'
    soup.head.append(document_title)
    return soup


def native_comment_widget_page():
    """Observed public template ownership; all prose and form values synthetic."""
    soup = native_headline_page()
    respond = soup.new_tag('div', id='respond')
    respond['class'] = ['respond']
    content = soup.new_tag('div')
    content['class'] = ['respond-content']
    form = soup.new_tag('form', id='commentform')
    captcha = soup.new_tag('h-captcha')
    captcha['class'] = ['h-captcha']
    captcha['data-sitekey'] = 'synthetic-public-comment-widget'
    form.append(captcha); content.append(form); respond.append(content)
    soup.select_one('.main-wrapper').append(respond)
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_verified_native_news_with_comment_only_captcha_is_not_a_read_access_challenge(parser):
    policy = replace(source(), html_parser=parser)
    result = extract_article(str(native_comment_widget_page()).encode(), policy, page_url=URL)
    assert result['title'] == TITLE and result['body'] == BODY
    assert result['authors_json'] == '["Writer"]' and result['published_at_raw'] == CLOCK
    assert result['per_article_license_verified']


@pytest.mark.parametrize('change', ['entry_sitekey', 'inside_post_container', 'different_widget',
    'not_comment_form', 'form_not_direct', 'respond_not_direct', 'outside_page_content',
    'duplicate_respond', 'duplicate_commentform', 'duplicate_page_content', 'second_sitekey',
    'explicit_challenge', 'marker_title_mismatch'])
def test_comment_widget_exception_never_exempts_gated_or_ambiguous_article_access(change):
    soup = native_comment_widget_page()
    widget, form, respond = soup.select_one('[data-sitekey]'), soup.select_one('#commentform'), soup.select_one('#respond')
    if change == 'entry_sitekey': soup.select_one('.entry').append(widget.extract())
    elif change == 'inside_post_container': soup.select_one('.post-container').append(respond.extract())
    elif change == 'different_widget': widget.name = 'div'
    elif change == 'not_comment_form': form['id'] = 'loginform'
    elif change == 'form_not_direct':
        extra = soup.new_tag('div'); extra.append(widget.extract()); form.append(extra)
    elif change == 'respond_not_direct': soup.select_one('.main-wrapper').append(form.extract())
    elif change == 'outside_page_content': soup.body.append(respond.extract())
    elif change == 'duplicate_respond': soup.body.append(soup.new_tag('div', id='respond'))
    elif change == 'duplicate_commentform': soup.body.append(soup.new_tag('form', id='commentform'))
    elif change == 'duplicate_page_content': soup.body.append(soup.new_tag('div', id='page-content'))
    elif change == 'second_sitekey': form.append(widget.__copy__())
    elif change == 'explicit_challenge': soup.body.append(soup.new_tag('form', id='challenge-form'))
    elif change == 'marker_title_mismatch': soup.title.string = 'Access Denied · Global Voices'
    with pytest.raises(NewsError, match='access_challenge'):
        extract_article(str(soup).encode(), source(), page_url=URL)


def test_observed_suffix_headline_is_not_a_provider_wide_refusal():
    result = extract_article(str(native_headline_page()).encode(), source(), page_url=URL)
    assert result['title'] == TITLE and result['body'] == BODY
    assert json.loads(result['authors_json']) == ['Writer']
    assert result['published_at_raw'] == CLOCK and result['per_article_license_verified']


@pytest.mark.parametrize('title', ['Access denied: source-authored news',
    'An original headline about access denied to public records', TITLE])
def test_title_word_position_does_not_replace_the_verified_article_identity(title):
    result = extract_article(str(native_headline_page(title)).encode(), source(), page_url=URL)
    assert result['title'] == title and result['body'] == BODY


@pytest.mark.parametrize('change', ['wrong_canonical', 'duplicate_canonical', 'missing_request',
    'mismatched_document_title', 'challenge_form', 'challenge_class', 'sitekey', 'other_marker',
    'duplicate_title', 'hidden_title', 'hidden_entry', 'missing_body', 'missing_header_author',
    'missing_footer_author', 'wrong_footer_author', 'foreign_profile', 'missing_clock',
    'unaware_clock', 'duplicate_clock', 'missing_article_license', 'other_provider'])
def test_a_news_headline_never_grants_access_to_a_challenge_or_unbound_article(change):
    soup, policy, url = native_headline_page(), source(), URL
    if change == 'wrong_canonical': soup.select_one('[rel="canonical"]')['href'] = URL + 'other/'
    elif change == 'duplicate_canonical': soup.head.append(soup.select_one('[rel="canonical"]').__copy__())
    elif change == 'missing_request': url = None
    elif change == 'mismatched_document_title': soup.title.string = 'Access Denied · Global Voices'
    elif change == 'challenge_form': soup.body.append(soup.new_tag('form', id='challenge-form'))
    elif change == 'challenge_class':
        challenge = soup.new_tag('div'); challenge['class'] = ['cf-challenge']; soup.body.append(challenge)
    elif change == 'sitekey':
        challenge = soup.new_tag('div'); challenge['data-sitekey'] = 'synthetic-key'; soup.body.append(challenge)
    elif change == 'other_marker': soup.title.string = 'Verify you are human ' + soup.title.string
    elif change == 'duplicate_title':
        soup.select_one('.post-header').append(soup.select_one('.post-title').__copy__())
        assert len(soup.select(policy.title_selector)) == 2
    elif change == 'hidden_title': soup.select_one('.post-title')['hidden'] = ''
    elif change == 'hidden_entry': soup.select_one('.entry')['hidden'] = ''
    elif change == 'missing_body': soup.select_one('.entry').clear()
    elif change == 'missing_header_author': soup.select_one('#post-header-credit').clear()
    elif change == 'missing_footer_author': soup.select_one('.contributor-name').clear()
    elif change == 'wrong_footer_author': soup.select_one('.contributor-name a').string = 'Other'
    elif change == 'foreign_profile': soup.select_one('.contributor-name a')['href'] = 'https://foreign.test/author/writer/'
    elif change == 'missing_clock': soup.select_one('[property="article:published_time"]').decompose()
    elif change == 'unaware_clock': soup.select_one('[property="article:published_time"]')['content'] = '2010-08-23T17:31:59'
    elif change == 'duplicate_clock': soup.head.append(soup.select_one('[property="article:published_time"]').__copy__())
    elif change == 'missing_article_license': soup.select_one('[rel="license"]').decompose()
    elif change == 'other_provider': policy = replace(policy, provider='not-reviewed')
    with pytest.raises(NewsError, match='access_challenge'):
        extract_article(str(soup).encode(), policy, page_url=url)


def test_a_verified_news_headline_does_not_exempt_real_member_content():
    soup = native_headline_page()
    soup.select_one('.entry')['data-paywall'] = 'true'
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(str(soup).encode(), source(), page_url=URL)


def test_headline_repair_changes_only_GV_HTML_not_RSS_authorization_or_other_sources(monkeypatch):
    from pathlib import Path
    from downloader.news_fulltext import load_config
    import downloader.news_fulltext_policy as policy

    current = load_config(Path('configs/news_fulltext.json'))[1].sources
    monkeypatch.setattr(policy, '_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION', 8)
    previous = load_config(Path('configs/news_fulltext.json'))[1].sources
    assert {provider for provider in current if current[provider].data_fingerprint != previous[provider].data_fingerprint} == {'globalvoices'}
    for provider in current:
        assert current[provider].authorization_fingerprint == previous[provider].authorization_fingerprint
        assert current[provider].allowed == previous[provider].allowed
        assert current[provider].fulltext_feed_data_fingerprint == previous[provider].fulltext_feed_data_fingerprint
