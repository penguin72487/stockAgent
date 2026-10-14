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
