"""WP public shape diagnostics are not byline guesses or a second collector."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from scripts.probe_news_sources import probe_provider, wordpress_public_metadata_shape


def post():
    return {'id': 101, 'author': 72, 'link': 'https://publisher.test/2026/10/13/article/',
        'content': {'rendered': '<p>ARTICLE BODY NEVER PRINTED</p>', 'protected': False},
        'title': {'rendered': 'TITLE NEVER PRINTED'},
        '_links': {'author': [{'href': 'https://publisher.test/wp-json/wp/v2/users/72', 'embeddable': True}]},
        '_embedded': {'author': [{'id': 72, 'name': 'Original writer', 'slug': 'not-a-name',
                                  'email': 'SECRET EMAIL', 'private': 'SECRET VALUE'}]},
        'meta': {'token': 'SECRET TOKEN'}, 'unknown_credit': {'private': 'SECRET CREDIT'},
        'yoast_head_json': {'author': 'Declared writer', 'password': 'SECRET PASSWORD'}}


def test_native_declared_metadata_is_not_a_complete_article_or_profile_request():
    value = post()
    before = deepcopy(value)
    result = wordpress_public_metadata_shape([value], {'x-wp-total': '1234', 'x-wp-totalpages': '13'})
    assert value == before
    assert result['rows'] == result['object_rows'] == result['posts_with_embedded_author_name'] == 1
    assert result['declared_total_posts'] == 1234 and result['declared_total_pages'] == 13
    sample = result['sampled_posts'][0]
    assert sample['author_id'] == 72 and sample['embedded_authors'][0]['name'] == 'Original writer'
    assert sample['author_link_embeddable'] == [True]
    assert sample['yoast_declared_author'] == 'Declared writer'
    assert result['article_body_accepted'] is result['raw_response_retained'] is False
    assert result['automatic_profile_requests'] == 0
    encoded = json.dumps(result)
    assert 'NEVER PRINTED' not in encoded and 'SECRET ' not in encoded and 'not-a-name' not in encoded
    assert 'wp/v2/users' not in encoded


@pytest.mark.parametrize('embedded', [None, False, [], 'wrong', {}, {'author': None},
    {'author': 'unknown'}, {'author': [{'code': 'rest_user_cannot_view', 'message': 'PRIVATE MESSAGE'}]}])
def test_missing_or_rejected_embeds_do_not_make_up_an_author(embedded):
    value = post()
    value['_embedded'] = embedded
    result = wordpress_public_metadata_shape([value], {})
    assert result['posts_with_embedded_author_name'] == 0
    sample = result['sampled_posts'][0]
    assert all(author.get('name') is None for author in sample['embedded_authors'])
    assert 'PRIVATE MESSAGE' not in json.dumps(result)
    assert result['article_body_accepted'] is False


def test_count_whole_bounded_page_but_sample_only_four_posts():
    values = [{'id': i, '_embedded': {'author': [{'name': 'Writer'}]}} for i in range(100)]
    result = wordpress_public_metadata_shape(values, {})
    assert result['rows'] == result['posts_with_embedded_author_name'] == 100
    assert len(result['sampled_posts']) == 4
    assert result['samples_are_not_all_post_byline_proof'] is True


@pytest.mark.parametrize('data', [{}, None, False, 'body', [{}] * 101])
def test_unbounded_or_wrong_envelope_is_explicitly_rejected(data):
    with pytest.raises(NewsError, match='bulk_api_shape_invalid'):
        wordpress_public_metadata_shape(data, {})


@pytest.mark.parametrize('value', [None, True, 1, '-1', 'nan', '1.5', '10000001', '1' * 2000, '\u0662'])
def test_unverified_totals_remain_unknown_not_zero(value):
    result = wordpress_public_metadata_shape([], {'x-wp-total': value, 'x-wp-totalpages': value})
    assert result['declared_total_posts'] is result['declared_total_pages'] is None


def test_malformed_post_metadata_is_diagnosed_without_aborting_other_posts():
    result = wordpress_public_metadata_shape([None, False, 3, {'content': 'bad', 'title': None,
        'author': True, '_links': 'bad', 'yoast_head_json': []}], {})
    assert result['rows'] == 4 and result['object_rows'] == 1
    assert result['sampled_posts'][3]['author_id'] is None
    assert result['sampled_posts'][3]['content_characters'] is None


def test_diagnostic_fields_and_public_name_lengths_are_bounded():
    value = post()
    value['_embedded']['author'] = [{'id': 1, 'name': 'W' * 300, 'code': 'C' * 200}] * 16
    value['meta'] = {str(i): 'SECRET' * 100 for i in range(1000)}
    result = wordpress_public_metadata_shape([value], {})
    sample = result['sampled_posts'][0]
    assert len(sample['embedded_authors']) == 8 and len(sample['meta_keys']) == 32
    assert len(sample['embedded_authors'][0]['name']) == 200
    assert len(sample['embedded_authors'][0]['error_code']) == 100
    assert 'SECRET' not in json.dumps(result)


def test_shared_probe_uses_one_explicit_request_and_does_not_keep_WP_raw(tmp_path, capsys):
    url = 'https://publisher.test/wp-json/wp/v2/posts?per_page=100&_embed=author'
    calls = []
    source = SimpleNamespace(provider='publisher', cnyes_apis=())
    response = Response(url, 200, {'content-type': 'application/json', 'x-wp-total': '1'},
                        json.dumps([post()]).encode())
    def get(target):
        calls.append(target)
        return response
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda target: source),
                           provider_not_before={}, robot_delays={}, get=get)
    results = probe_provider(http, [url], retain=True, output_root=tmp_path)
    assert calls == [url] and list(tmp_path.iterdir()) == []
    assert results[0]['wordpress_public_metadata']['posts_with_embedded_author_name'] == 1
    assert 'SECRET ' not in capsys.readouterr().out


@pytest.mark.parametrize('payload', [[], [None], [False], [1], ['not a mapping'], [{'_embedded': []}]])
def test_generic_JSON_diagnosis_no_longer_crashes_on_first_nonobject(payload, capsys):
    url = 'https://publisher.test/metadata'
    source = SimpleNamespace(provider='publisher', cnyes_apis=())
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda target: source), provider_not_before={},
        robot_delays={}, get=lambda target: Response(url, 200, {'content-type': 'application/json'}, json.dumps(payload).encode()))
    result = probe_provider(http, [url])[0]
    assert result['status'] == 200 and result['json_first_keys'] in ([], ['_embedded'])
    assert result['embedded_author_keys'] == [] and 'wordpress_public_metadata' not in result


def test_wrong_WP_envelope_is_recorded_and_next_explicit_probe_runs(capsys):
    bad, good = 'https://publisher.test/wp-json/wp/v2/posts', 'https://publisher.test/metadata'
    calls = []
    source = SimpleNamespace(provider='publisher', cnyes_apis=())
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'application/json'}, b'{}' if url == bad else b'[]')
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda target: source), provider_not_before={},
                           robot_delays={}, get=get)
    result = probe_provider(http, [bad, good])
    assert calls == [bad, good] and result[0]['error'] == 'bulk_api_shape_invalid'
    assert result[1]['status'] == 200


@pytest.mark.parametrize('bad_body', [b'not JSON SECRET REJECTED TEXT', b'{"truncated":', b'\xff'])
def test_bad_JSON_does_not_abort_another_endpoint_or_log_rejected_text(bad_body, capsys):
    bad, good = 'https://publisher.test/wp-json/wp/v2/posts', 'https://publisher.test/metadata'
    calls = []
    source = SimpleNamespace(provider='publisher', cnyes_apis=())
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'application/json'}, bad_body if url == bad else b'[]')
    http = SimpleNamespace(registry=SimpleNamespace(require=lambda target: source), provider_not_before={},
                           robot_delays={}, get=get)
    results = probe_provider(http, [bad, good])
    assert calls == [bad, good] and results[0]['error'] == 'bulk_api_invalid_json'
    assert results[0]['cause_chain'] == [] and results[1]['status'] == 200
    assert 'SECRET REJECTED TEXT' not in capsys.readouterr().out
