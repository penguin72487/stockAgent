"""Exact retained Office markers are current source emptiness, not prose."""
from copy import deepcopy
import hashlib
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, empty_page, source


MARKERS = ('<!--StartFragment -->', '<!--[if gte mso 9]>      <![endif]-->')


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('marker', MARKERS)
@pytest.mark.parametrize('encoded', [False, True])
def test_exact_office_marker_and_whole_empty_native_container_are_not_downloaded_bodies(
        cnyes_config, tmp_path, parser, marker, encoded):
    cnyes_config['providers'][0]['html_parser'] = parser
    payload = empty_page(change={'content': html.escape(marker) if encoded else marker, 'video': ''})
    with pytest.raises(NewsError, match='^source_article_empty_template$') as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert not caught.value.retry


@pytest.mark.parametrize('marker', MARKERS)
@pytest.mark.parametrize('change', [
    {'payment': 1}, {'payment': False}, {'payment': '0'}, {'newsId': 43},
    {'publishAt': 1388534401}, {'publishAt': True}, {'url': URL.replace('/42', '/43')},
    {'video': 'a source video'}, {'title': 'another source title'},
])
def test_marker_never_waives_source_identity_free_flag_clock_or_video(cnyes_config, tmp_path, marker, change):
    with pytest.raises(NewsError) as caught:
        extract_article(empty_page(change={'content': marker, 'video': '', **change}),
                        source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('marker', MARKERS)
@pytest.mark.parametrize('native', [
    '<!--unknown-->', '<p></p>', '<p> </p>', '<p hidden>source text</p>', '<img src="source.png">',
    '<video></video>', '<iframe></iframe>', '<span>actual text</span>',
])
def test_marker_does_not_hide_any_other_native_content(cnyes_config, tmp_path, marker, native):
    payload = empty_page(change={'content': marker, 'video': ''}).replace(
        b'<div id="article-container">', ('<div id="article-container">' + native).encode())
    with pytest.raises(NewsError) as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('content', [
    '<!--StartFragment-->', '<!--StartFragment --> ', '<!--StartFragment -->\n',
    '<!--StartFragment -->hidden text', '<!--StartFragment -->\n<p></p>',
    '<!--[if gte mso 9]>SECRET<![endif]-->', '<!--[if gte mso 9]>     <![endif]-->',
    '<!--unknown-->', '<!--<p>original prose</p>-->',
])
def test_unknown_conditional_or_comment_text_is_not_cleaned_to_empty(cnyes_config, tmp_path, content):
    with pytest.raises(NewsError) as caught:
        extract_article(empty_page(change={'content': content, 'video': ''}),
                        source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('mutation', [
    lambda payload: payload.replace(b'<div id="article-container">', b'<div id="article-container" hidden>'),
    lambda payload: payload.replace(b'<div id="article-container">', b'<div id="article-container" style="display:none">'),
    lambda payload: payload.replace(b'<h1>', b'<h1 hidden>'),
])
def test_hidden_renderer_cannot_prove_source_absence(cnyes_config, tmp_path, mutation):
    with pytest.raises(NewsError) as caught:
        extract_article(mutation(empty_page(change={'content': MARKERS[0], 'video': ''})),
                        source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


def test_outcome_contract_changes_html_parser_identity_not_bulk_authorization_or_pacing(
        cnyes_config, tmp_path, monkeypatch):
    current = source(cnyes_config, tmp_path)
    original = hashlib.sha256

    def before(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and 'verified_office_comment_empty_outcome_contract_version' in payload:
            payload = deepcopy(payload)
            assert payload.pop('verified_office_comment_empty_outcome_contract_version') == 1
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', before)
    old = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != old.data_fingerprint
    assert current.parser_repair_fingerprint != old.parser_repair_fingerprint
    assert current.cnyes_bulk_data_fingerprint == old.cnyes_bulk_data_fingerprint
    assert current.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert current.authorization_fingerprint == old.authorization_fingerprint
