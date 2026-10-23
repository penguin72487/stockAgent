"""Native v3 adds literal organisational credit, never an inferred reporter."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_taiwannews import (
    TAIWANNEWS_V2_HTTP_BINDING, TAIWANNEWS_V2_LAYOUT, TAIWANNEWS_V2_VERIFICATION,
    TAIWANNEWS_V3_HTTP_BINDING, TAIWANNEWS_V3_LAYOUT, TAIWANNEWS_V3_VERIFICATION,
    taiwannews_evidence_source, taiwannews_resident_source,
)
from test_news_taiwannews_original import CLOCK, PARAGRAPHS, URL, config, entry, page, source
from test_news_taiwannews_private_original import inline_page


def test_inline_editor_is_a_literal_organisation_not_an_invented_person(tmp_path):
    result = extract_article(inline_page(), source(tmp_path, 3), page_url=URL)
    assert result['body'] == '\n'.join(PARAGRAPHS)
    assert json.loads(result['authors_json']) == ['台灣英文新聞－編輯']
    assert result['attribution_status'] == 'source_declared_corporate_author_not_verified_person'
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['native_credit_name'] is None and detail['native_profile_href'] is None
    assert detail['native_profile_id'] is None and not detail['human_reporter_identity_verified']
    assert not detail['original_ownership_verified']
    assert detail['source_declared_editor_group'] == '台灣英文新聞－編輯'
    assert result['published_at_raw'] == CLOCK and not result['historical_point_in_time']
    assert 'without_offset' in result['published_time_basis']


@pytest.mark.parametrize('version', [1, 2])
def test_legacy_layouts_do_not_silently_admit_new_inline_body(tmp_path, version):
    with pytest.raises(NewsError):
        extract_article(inline_page(), source(tmp_path, version), page_url=URL)


@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('label', ['台灣英文新聞－編輯', '台灣英文新聞－記者', '台灣英文新聞－時評'])
def test_existing_card_every_semantic_field_stays_equal(tmp_path, nested, label):
    raw = page(nested=nested, label=label)
    old = extract_article(raw, source(tmp_path, 2), page_url=URL)
    new = extract_article(raw, source(tmp_path, 3), page_url=URL)
    assert {k: v for k, v in new.items() if k != 'article_identity_basis'} == {
        k: v for k, v in old.items() if k != 'article_identity_basis'}


@pytest.mark.parametrize('markup,expected', [
    ('10.<wbr>8', '10.8'), ('負值<strong>-2</strong>', '負值-2'),
    ('hello <a href="https://example.com">world</a> again', 'hello world again'),
    ('1,<span>234</span>.56', '1,234.56'), ('5.<wbr/>57%', '5.57%'),
    ('a<!--not written-->b', 'ab'), ('a<br>b', 'a\nb'),
    ('<ul><li>A<strong>B</strong></li><li>C</li></ul>', 'AB\nC'),
])
def test_literal_inline_adjacency_and_block_boundaries(tmp_path, markup, expected):
    raw = inline_page().replace(PARAGRAPHS[1].encode(), markup.encode())
    result = extract_article(raw, source(tmp_path, 3), page_url=URL)
    assert result['body'] == PARAGRAPHS[0]+'\n'+expected


@pytest.mark.parametrize('prefix', ['台灣英文新聞', '台灣英文新聞－記者', '中央社', ''])
def test_bare_publisher_and_other_credits_are_not_inferred(tmp_path, prefix):
    with pytest.raises(NewsError):
        extract_article(inline_page(prefix), source(tmp_path, 3), page_url=URL)


@pytest.mark.parametrize('markup', [
    '<wbr class="other">', '<wbr onclick="evil()">', '<wbr hidden>',
    '<span hidden>prose</span>', '<span style="display:none">prose</span>',
    '<iframe>prose</iframe>', '<div>unknown block</div>', '<p>中央社提供</p>',
    '<a href="javascript:evil()">prose</a>',
])
def test_unknown_hidden_active_wire_and_word_break_shapes_remain_refused(tmp_path, markup):
    raw = inline_page().replace(PARAGRAPHS[1].encode(), markup.encode())
    with pytest.raises(NewsError):
        extract_article(raw, source(tmp_path, 3), page_url=URL)


def test_old_evidence_is_not_rearmed_and_http_auth_identity_stays_exact():
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    settings = deepcopy(settings)
    item = next(item for item in settings['providers'] if item['id'] == 'taiwannews')
    item.update(embedded_article_layout=TAIWANNEWS_V2_LAYOUT, adapter_verification=TAIWANNEWS_V2_VERIFICATION)
    old = PolicyRegistry(settings, base=repo).sources['taiwannews']
    item.update(embedded_article_layout=TAIWANNEWS_V3_LAYOUT, adapter_verification=TAIWANNEWS_V3_VERIFICATION)
    new = PolicyRegistry(settings, base=repo).sources['taiwannews']
    assert taiwannews_evidence_source(old) and not taiwannews_evidence_source(new)
    assert taiwannews_resident_source(new)
    assert (old.data_fingerprint, new.data_fingerprint, new.authorization_fingerprint,
        new.http_capacity_source_fingerprint, new.http_capacity_authorization_fingerprint) == TAIWANNEWS_V3_HTTP_BINDING
    assert new.authorization == old.authorization
    for field, value in [('authorization_fingerprint', 'changed'), ('data_fingerprint', 'changed'),
            ('http_capacity_source_fingerprint', 'changed'), ('domains', ('taiwannews.com.tw',))]:
        assert not taiwannews_resident_source(replace(new, **{field: value}))
