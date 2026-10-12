"""Original legacy references may differ only by their verified comment view."""
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


REFS = (('article_table', '0.table'), ('article_image', '20160202_38_0.jpg'))


def reference_page(*, references=REFS, current=None, visible=None, schema=None, body=BODY * 8,
                   streamed=False):
    current = current if current is not None else ''.join(f'<!--<{kind}>{label}</{kind}> -->' for kind, label in references)
    visible = visible if visible is not None else ''.join(f'<{kind}>{label}</{kind}> -->' for kind, label in references)
    schema = schema if schema is not None else body + ' ' + ' '.join(label + ' -->' for _, label in references)
    content = '<p>' + html.escape(body) + current + '</p>'
    page = streamed_mobile_page if streamed else mobile_page
    payload = page(body=body, changes={'content': html.escape(content)}, schema_changes={'articleBody': schema})
    return payload.replace(b'</p></div></article>', (visible + '</p></div></article>').encode())


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_original_values_and_references_preserved_without_asset_invention(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    body = BODY * 8 + ' 原始成交量2330股，年增率-12.5%。'
    result = extract_article(reference_page(body=body, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'].startswith(body)
    assert all(label in result['body'] for _, label in REFS)
    assert json.loads(result['authors_json']) == [AUTHOR] and result['published_at_raw'] == CLOCK
    assert json.loads(result['verified_article_notices_json']) == [{'text': label + ' -->',
        'kind': 'same_article_legacy_template_reference', 'marker_kind': kind, 'source_reference': label,
        'referenced_asset_or_values_reconstructed': False} for kind, label in REFS]
    assert result['verified_image_captions_json'] is None and result['historical_point_in_time'] is False
    assert 'source_current_legacy_template_reference_comparison_v1' in result['article_identity_basis']
    assert 'source_current_figure_notice_schema_comparison_v1' not in result['article_identity_basis']
    assert 'original_references_preserved_without_asset_or_value_reconstruction' in result['body_completeness']


@pytest.mark.parametrize('change', [
    {'current': '<!--<article_table>99.table</article_table> --><!--<article_image>20160202_38_0.jpg</article_image> -->'},
    {'current': '<!--<article_image>20160202_38_0.jpg</article_image> --><!--<article_table>0.table</article_table> -->'},
    {'current': '<!--unverified omitted prose123.45-->'},
    {'current': '<!--<article_table>0.table</article_table> --><!--<article_image>20160202_38_0.jpg</article_image> --><!--unknown-->'},
    {'visible': '<article_table hidden>0.table</article_table> --><article_image>20160202_38_0.jpg</article_image> -->'},
    {'visible': '<article_table onclick="unknown()">0.table</article_table> --><article_image>20160202_38_0.jpg</article_image> -->'},
    {'visible': '<article_table><span>0.table</span></article_table> --><article_image>20160202_38_0.jpg</article_image> -->'},
    {'visible': '<article_table>0.table</article_table> --x<article_image>20160202_38_0.jpg</article_image> -->'},
    {'visible': '<article_table>0.table</article_table> --><article_image>20160202_38_0.jpg</article_image> -->new value123.45'},
    {'schema': BODY * 8},
    {'schema': BODY * 8 + ' 99.table --> 20160202_38_0.jpg -->'},
    {'references': (('article_image', 'https://example.org/x.jpg'),)},
    {'references': (('article_table', 'arbitrary123.45'),)},
    {'references': (('article_image', '20160202_38_0.png'),)},
    {'references': REFS * 9},
])
@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_schema_or_matching_hidden_markup_does_not_relax_whole_source_proof(cnyes_config, tmp_path, parser, change):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError):
        extract_article(reference_page(**change), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_source_comments_cannot_supply_missing_article(cnyes_config, tmp_path):
    with pytest.raises(NewsError):
        extract_article(reference_page(body=''), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_healthy_identical_body_never_enters_new_comparison(cnyes_config, tmp_path, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_publishers._cnyes_legacy_template_reference_witness',
        lambda *args: pytest.fail('healthy body entered mismatch-only witness'))
    result = extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['verified_article_notices_json'] is None
