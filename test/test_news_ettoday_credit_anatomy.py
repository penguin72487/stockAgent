"""Attribution leads never become accepted author/body data or prose exports."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import ettoday_credit_anatomy, ettoday_validation_metadata
from test_news_ettoday import URLS, page


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_nested_late_whitespace_and_embedded_signatures_are_shapes_not_acceptance(parser):
    story = BeautifulSoup('<div class="story">' + '<p>Private original prose.</p>' * 9
        + '<section><p>記者研究記者／台北報導</p></section>'
        + '<p>記者 研究記者 ／ 台北 報導</p><p>【記者原始記者／台北報導】</p></div>', parser).div
    facts = ettoday_credit_anatomy(story)
    assert facts['paragraphs_examined'] == 12 and facts['candidate_paragraphs_examined'] == 3
    assert not facts['paragraph_scan_truncated'] and not facts['candidate_rows_truncated']
    nested, spaced, embedded = facts['paragraph_shapes'][-3:]
    assert nested['exact_reporter_signature'] and not nested['direct_child']
    assert nested['direct_paragraph_index'] is None
    assert spaced['whitespace_stripped_signature'] and not spaced['exact_reporter_signature']
    assert embedded['embedded_reporter_signature'] and not embedded['embedded_signature_starts_at_zero']
    assert not facts['body_or_author_text_exported'] and not facts['attribution_verified_by_diagnostic']
    encoded = json.dumps(facts, ensure_ascii=False)
    assert all(value not in encoded for value in ('Private original prose', '研究記者', '原始記者', '台北報導'))


def test_hidden_byline_is_a_visible_failure_not_an_authorization_witness():
    story = BeautifulSoup('<div><p hidden>記者研究記者／台北報導</p></div>', 'lxml').div
    row = ettoday_credit_anatomy(story)['paragraph_shapes'][0]
    assert row['exact_reporter_signature'] and not row['visible']


def test_scan_and_output_bounds_are_explicit_and_do_not_claim_whole_story_absence():
    story = BeautifulSoup('<div>' + '<p>記者研究記者／台北報導</p>' * 200 + '</div>', 'lxml').div
    facts = ettoday_credit_anatomy(story)
    assert facts['paragraphs_examined'] == 128 and facts['paragraph_scan_truncated']
    assert facts['candidate_paragraphs_examined'] == 128
    assert facts['candidate_rows_truncated'] and len(facts['paragraph_shapes']) == 32


def test_missing_credit_sample_does_not_assert_absence_beyond_its_bound():
    story = BeautifulSoup('<div>' + '<p>No credit here.</p>' * 128
        + '<p>記者研究記者／台北報導</p></div>', 'lxml').div
    facts = ettoday_credit_anatomy(story)
    assert facts['candidate_paragraphs_examined'] == 0 and facts['paragraph_scan_truncated']
    assert not facts['attribution_verified_by_diagnostic']


def test_existing_metadata_is_additive_and_never_substitutes_organization_for_reporter():
    facts = ettoday_validation_metadata(page(), page_url=URLS[0], adapter_contract_version=7)
    assert facts['contract_version'] == 3 and facts['native_credit_shapes'][0]['native_reporter_signature_shape']
    assert facts['declared_author_types'] == [['Organization']]
    anatomy = facts['story_credit_anatomy'][0]
    # The prose fixture also begins with 記者, but is not a byline. A broad
    # diagnostic lead is deliberately not an exact signature or acceptance.
    assert anatomy['candidate_paragraphs_examined'] == 2
    assert sum(r['exact_reporter_signature'] for r in anatomy['paragraph_shapes']) == 1
    assert any(r['credit_prefix'] and not r['exact_reporter_signature']
        for r in anatomy['paragraph_shapes'])
    assert not anatomy['attribution_verified_by_diagnostic']
    assert '研究記者' not in json.dumps(facts, ensure_ascii=False)
