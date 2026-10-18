"""Long native prose keeps real word/number boundaries, never inferred text."""
import hashlib
import html
import json
import re
from copy import deepcopy
from dataclasses import replace

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, URL, short_page, source


PROSE = ('Original quarterly research report: the company recorded revenue of US$138.1 million, '
         'an increase of 7.25% in 2021, while operating costs decreased by -12.5 million. '
         'The source retained its original forecast and all reported observations.')
FLAT = ''.join(PROSE.split())


def page(*, fragment=None, schema=FLAT, visible=None, **options):
    fragment = fragment if fragment is not None else '<p>' + html.escape(PROSE) + '</p>'
    payload = short_page(body=PROSE, schema_change={'articleBody': schema,
        **options.pop('schema_change', {})},
        change={'content': html.escape(fragment), **options.pop('change', {})}, **options)
    return payload.replace(('<p>' + html.escape(PROSE) + '</p>').encode(),
        (visible if visible is not None else fragment).encode()).replace(b'<head>', b'<head><meta charset="utf-8">')


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('fragment', [PROSE, '<p>' + PROSE + '</p>', '<div>' + PROSE + '</div>',
    '<section><p>' + PROSE + '</p></section>',
    '<p>' + PROSE.replace('while operating', 'while<br>operating') + '</p>'])
def test_only_whole_visible_and_current_long_prose_can_restore_spaces(cnyes_config, tmp_path, parser, fragment):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(page(fragment=fragment), source(cnyes_config, tmp_path), page_url=URL)
    assert re.sub(r'\s+', ' ', result['body']).strip() == PROSE
    assert result['extraction_method'] == 'cnyes_visible_prose_spacing_v1'
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == ['來源宣告的測試作者']
    assert result['body_completeness'].startswith('verified_publisher_body;')
    assert 'whole_visible_container_match_full_content' in result['article_identity_basis']
    for value in ('US$138.1', '7.25%', '2021', '-12.5'):
        assert value in result['body']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'payment': False}},
    {'change': {'publishAt': 1388534401}}, {'change': {'newsId': 43}},
    {'change': {'url': URL.replace('/42', '/43')}},
    {'change': {'content': '<p>' + PROSE.replace('138.1', '139.1') + '</p>'}},
    {'change': {'title': 'another source title'}}, {'conflict': True}, {'duplicate_key': True},
    {'canonical': URL.replace('/42', '/43')}, {'schema': FLAT.replace('2021', '2022')},
    {'visible': '<p>' + PROSE.replace('138.1', '139.1') + '</p>'},
    {'visible': '<p>' + PROSE + '</p><p>additional original observation 7.25</p>'},
    {'fragment': '<p hidden>' + PROSE + '</p>'},
    {'fragment': '<p aria-hidden="true">' + PROSE + '</p>'},
    {'fragment': '<p style="display:none">' + PROSE + '</p>'},
    {'schema_change': {'datePublished': '2014-01-01T00:00:00'}},
])
def test_other_words_numbers_identity_payment_and_hidden_content_stay_rejected(cnyes_config, tmp_path, parser, options):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError):
        extract_article(page(**options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('extra', [
    '<img src="https://source.example/image">', '<video></video>', '<audio></audio>',
    '<iframe src="https://source.example/video"></iframe>', '<object></object>', '<embed>',
    '<table><tr><td>12</td><td>138.1</td></tr></table>',
    '<figure><figcaption>another body</figcaption></figure>',
])
def test_new_spacing_branch_cannot_drop_media_tables_or_captions(cnyes_config, tmp_path, parser, extra):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError):
        extract_article(page(fragment='<p>' + PROSE + '</p>' + extra), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('body', ['X' * 150, '1' * 150, '(AB)20.19+1.08(5.65%):' * 8])
def test_unobserved_word_boundaries_never_supply_prose(cnyes_config, tmp_path, body):
    with pytest.raises(NewsError):
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_current_content_without_visible_or_declared_body_is_not_downloaded(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    payload = page(schema='').replace(('<p>' + html.escape(PROSE) + '</p>').encode(), b'')
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('body', [PROSE, '原始數據與數值保持完全一致。' * 15])
def test_already_valid_long_bodies_keep_the_original_hot_path(cnyes_config, tmp_path, parser, body, monkeypatch):
    from downloader import news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, 'cnyes_verified_prose_spacing',
        lambda *args, **kwargs: pytest.fail('healthy body must not parse native stream again'))
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)
    assert result['body'] == body
    assert result['extraction_method'] == 'jsonld_article_body'


def test_only_html_abi_changes_not_bulk_index_or_authorization(cnyes_config, tmp_path, monkeypatch):
    new = source(cnyes_config, tmp_path)
    original_sha = hashlib.sha256

    def old_fingerprint(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original_sha(value)
        if isinstance(payload, dict) and 'verified_native_prose_spacing_contract_version' in payload:
            assert payload.pop('verified_native_prose_spacing_contract_version') == 1
            # Reconstruct the actual pre-spacing HTTP identity, not a newer
            # capture with the independently versioned source-test outcome.
            assert payload.pop('verified_source_test_article_outcome_contract_version') == 1
            assert payload.pop('visible_headline_body_ownership_contract_version') == 1
            assert payload.pop('verified_office_comment_empty_outcome_contract_version') == 1
            # Present in capture, deliberately absent from parser-replay ABI.
            if 'verified_native_degenerate_outcome_contract_version' in payload:
                assert payload.pop('verified_native_degenerate_outcome_contract_version') == 1
            assert payload.pop('verified_untitled_no_events_bulletin_contract_version') == 1
            assert payload.pop('verified_source_signature_image_outcome_contract_version') == 1
            return original_sha(json.dumps(payload, sort_keys=True).encode())
        return original_sha(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', old_fingerprint)
    old = source(cnyes_config, tmp_path)
    assert new.data_fingerprint != old.data_fingerprint
    assert new.cnyes_bulk_data_fingerprint == old.cnyes_bulk_data_fingerprint
    assert new.cnyes_index_data_fingerprint == old.cnyes_index_data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_unrelated_schema_cannot_use_new_spacing_recovery(cnyes_config, tmp_path, parser):
    from downloader.news_fulltext_policy import public_article_document
    from downloader.news_fulltext_publishers import cnyes_verified_prose_spacing
    cnyes_config['providers'][0]['html_parser'] = parser
    policy = source(cnyes_config, tmp_path)
    payload = page(schema_change={'url': URL.replace('/42', '/43'),
        'mainEntityOfPage': {'@id': URL.replace('/42', '/43')}})
    soup = public_article_document(payload, policy, page_url=URL)
    declared = json.loads(soup.select_one('script[type="application/ld+json"]').get_text())['@graph'][0]
    assert cnyes_verified_prose_spacing(soup, policy, declared, FLAT, page_url=URL) is None
    # The pre-existing configured CSS-body route has its own scope. This
    # branch must not silently relabel that route as a matched-schema proof.
    result = extract_article(payload, policy, page_url=URL)
    assert result['extraction_method'] == 'css:#article-container'


def capacity_config(config):
    value = deepcopy(config)
    value['adaptive_rate'] = {'enabled': False}
    value['providers'][0].update(interval_seconds=1.0, parallelism=1, capacity_max_parallelism=2,
        capacity_transient_backoff_max_seconds=10.0)
    return value


def test_exact_prior_http_capacity_and_429_deadline_survive_parser_upgrade(cnyes_config, tmp_path):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_capacity import PublisherCapacity
    from test_news_fulltext import http_fixture
    config = capacity_config(cnyes_config)
    old_http, old_session = http_fixture(config, tmp_path, {})
    new_source = old_http.registry.sources['cnyes']
    old_http.registry.sources['cnyes'] = replace(new_source,
        data_fingerprint=new_source.http_capacity_source_fingerprint, http_capacity_source_fingerprint='')
    try:
        old = PublisherCapacity(old_http, config)
        old_http.set_client_capacity('cnyes', .25, 2)
        with pytest.raises(NewsError, match='http_429'):
            old_http._response_status('cnyes', 429, {'retry-after': '120'})
        state = old.states['cnyes']
        state.update(interval_seconds=.5, parallelism=2, state='stable_pipeline_capacity')
        atomic_write_json(old.path, old.snapshot(), durable=True)
        saved = json.loads((tmp_path / 'cooldowns/cnyes.json').read_text())
        assert not old_session.calls
    finally:
        old_http.close()
    new_http, new_session = http_fixture(config, tmp_path, {})
    try:
        new = PublisherCapacity(new_http, config)
        assert new.fingerprint == old.fingerprint
        assert new.provider_fingerprints == old.provider_fingerprints
        assert new.states['cnyes']['state'] == 'stable_pipeline_capacity'
        assert new.states['cnyes']['interval_seconds'] == .5
        assert new.states['cnyes']['parallelism'] == 2
        assert new_http.rate_refusal_intervals['cnyes'] == .5
        assert json.loads((tmp_path / 'cooldowns/cnyes.json').read_text()) == saved
        assert not new_session.calls
    finally:
        new_http.close()


@pytest.mark.parametrize('change', [{'interval_seconds': 2.0}, {'html_parser': 'html.parser'}])
def test_other_source_policy_changes_still_invalidate_capacity(cnyes_config, tmp_path, change):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_capacity import PublisherCapacity
    from test_news_fulltext import http_fixture
    config = capacity_config(cnyes_config)
    old_http, _ = http_fixture(config, tmp_path, {})
    try:
        old = PublisherCapacity(old_http, config)
        old.states['cnyes'].update(interval_seconds=.5, parallelism=2, state='stable_pipeline_capacity')
        atomic_write_json(old.path, old.snapshot(), durable=True)
    finally:
        old_http.close()
    changed = deepcopy(config)
    changed['providers'][0].update(change)
    new_http, session = http_fixture(changed, tmp_path, {})
    try:
        new = PublisherCapacity(new_http, changed)
        assert new.fingerprint != old.fingerprint
        assert new.provider_fingerprints['cnyes'] != old.provider_fingerprints['cnyes']
        assert new.states['cnyes']['state'] == 'warming'
        assert not session.calls
    finally:
        new_http.close()
