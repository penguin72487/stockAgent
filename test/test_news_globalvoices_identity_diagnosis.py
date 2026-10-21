"""Observe actual binding constraints without raw prose or automatic requests."""
from dataclasses import replace
import json

from bs4 import BeautifulSoup
import pytest

from scripts.probe_news_sources import globalvoices_public_identity_shape
from test_news_globalvoices_untitled import BODY, CLOCK, URL, page, source


def test_identity_diagnosis_reports_exact_roles_but_no_body_or_acceptance():
    result = globalvoices_public_identity_shape(BeautifulSoup(page(), 'lxml'), source(), page_url=URL)
    assert result['numeric_requested_post_id'] == '164959'
    assert result['counts'] == {'canonical': 1, 'container': 1, 'title': 1, 'header_credit': 1,
        'published_meta': 1, 'entry': 1, 'numeric_post': 1, 'footer_writer': 1, 'article_license': 1}
    assert result['nodes']['canonical'][0]['matches_requested_url']
    assert result['nodes']['title'][0]['visible_text_characters'] == 0
    assert result['nodes']['title'][0]['inside_single_numeric_post']
    assert result['nodes']['published_meta'][0]['source_clock_raw'] == CLOCK
    assert result['nodes']['published_meta'][0]['aware_clock']
    assert result['nodes']['header_credit'][0]['public_profiles'][1]['public_byline'] == 'Writer'
    assert result['current_exception_identity_verified']
    assert not result['article_durably_accepted'] and not result['raw_response_retained']
    assert result['automatic_profile_requests'] == 0 and not result['body_text_exported']
    assert BODY not in json.dumps(result)


def test_header_outside_numeric_post_is_reported_not_silently_assumed():
    soup = BeautifulSoup(page(), 'lxml')
    head = soup.select_one('.post-header').extract()
    soup.select_one('.post-container').insert(0, head)
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    assert result['nodes']['title'][0]['inside_single_container']
    assert not result['nodes']['title'][0]['inside_single_numeric_post']
    assert not result['nodes']['header_credit'][0]['inside_single_numeric_post']
    assert result['nodes']['entry'][0]['inside_single_numeric_post']
    assert not result['current_exception_identity_verified']
    assert BODY not in json.dumps(result)


@pytest.mark.parametrize('clock', ['', 'invalid', '2010-09-24', '2010-09-24T21:02:07'])
def test_unverified_source_clock_is_not_inferred_from_date_url(clock):
    soup = BeautifulSoup(page().replace(CLOCK.encode(), clock.encode()), 'lxml')
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    assert result['nodes']['published_meta'][0]['source_clock_raw'] == clock
    assert not result['nodes']['published_meta'][0]['aware_clock']
    assert not result['current_exception_identity_verified']


def test_exact_counts_and_bounded_samples_ancestors_and_public_attributes():
    raw = page().replace(b'</head>', (b'<link rel="canonical" href="'+URL.encode()+b'">')*15+b'</head>')
    soup = BeautifulSoup(raw, 'lxml')
    soup.select_one('.entry')['class'] = ['x'*500]*15 + ['entry']
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    assert result['counts']['canonical'] == 16 and len(result['nodes']['canonical']) == 8
    assert not result['current_exception_identity_verified']
    assert len(result['nodes']['entry'][0]['classes']) == 8
    assert all(len(value) <= 80 for value in result['nodes']['entry'][0]['classes'])
    assert all(len(row['ancestors']) <= 8 for rows in result['nodes'].values() for row in rows)
    assert BODY not in json.dumps(result)


def test_public_url_shape_never_exports_query_values_or_userinfo():
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('link[rel="canonical"]')['href'] = (
        'https://private-name:private-password@globalvoices.org/2010/09/24/164959/?token=private-token')
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    row = result['nodes']['canonical'][0]
    assert row['query_present'] and row['credentials_present']
    assert not row['matches_requested_url']
    assert 'private-' not in json.dumps(result)


def test_unreviewed_provider_cannot_use_native_GV_diagnostic_contract():
    with pytest.raises(ValueError, match='globalvoices_native_identity_metadata_scope_required'):
        globalvoices_public_identity_shape(BeautifulSoup(page(), 'lxml'),
            replace(source(), provider='unreviewed'), page_url=URL)


def empty_entry_page():
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('.entry').clear()
    return soup


@pytest.mark.parametrize('whitespace', ['', ' \n\t '])
def test_bound_empty_native_response_is_not_a_body_or_an_exhausted_archive(whitespace):
    soup = empty_entry_page()
    soup.select_one('.entry').append(whitespace)
    before = str(soup)
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    shape = result['native_body_observation']
    assert result['contract_version'] == 2
    assert result['current_exception_identity_verified']
    assert shape['verified_empty_native_container']
    assert not shape['all_authorized_alternatives_exhausted']
    assert not shape['queue_mutation_possible'] and not result['article_durably_accepted']
    assert str(soup) == before
    assert BODY not in json.dumps(result)


@pytest.mark.parametrize('markup', [
    '<p></p>', '<br>', '<img src="https://media.test/photo">',
    '<iframe src="https://media.test/embed"></iframe>',
    '<script>const privateText = "not a body";</script>',
    '<!-- unknown loading directive -->', '<template>Unknown body</template>',
    '<p hidden>Invisible source text</p>', 'Unknown source text', '\u200b',
])
def test_textless_rendered_widgets_comments_or_hidden_content_are_not_verified_empty(markup):
    soup = empty_entry_page()
    fragment = BeautifulSoup(markup, 'html.parser')
    soup.select_one('.entry').extend(list(fragment.contents))
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    assert not result['native_body_observation']['verified_empty_native_container']
    assert not result['article_durably_accepted']
    assert 'privateText' not in json.dumps(result)


@pytest.mark.parametrize('change', [
    'wrong_canonical', 'missing_license', 'unknown_author', 'naive_clock',
    'duplicate_entry', 'hidden_entry', 'loading_attribute', 'unknown_class', 'paywall',
])
def test_empty_shape_requires_same_visible_article_identity_and_known_plain_container(change):
    soup = empty_entry_page()
    entry = soup.select_one('.entry')
    if change == 'wrong_canonical':
        soup.select_one('link[rel=canonical]')['href'] = URL + '?different=1'
    elif change == 'missing_license':
        soup.select_one('[rel=license]').decompose()
    elif change == 'unknown_author':
        soup.select_one('#post-header-credit').clear()
    elif change == 'naive_clock':
        soup.select_one('meta[property="article:published_time"]')['content'] = '2010-09-24T12:00:00'
    elif change == 'duplicate_entry':
        entry.parent.append(BeautifulSoup('<div class="entry"></div>', 'html.parser').div)
    elif change == 'hidden_entry':
        entry['hidden'] = ''
    elif change == 'loading_attribute':
        entry['data-loading'] = 'unknown'
    elif change == 'unknown_class':
        entry['class'].append('unknown')
    elif change == 'paywall':
        entry.parent['class'] = ['paywall']
    result = globalvoices_public_identity_shape(soup, source(), page_url=URL)
    assert not result['native_body_observation']['verified_empty_native_container']
