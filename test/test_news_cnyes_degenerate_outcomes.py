"""Native placeholder failures are not recovered bodies or bulk replays."""
from copy import deepcopy
import hashlib
import html
import json
import time

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import BODY, URL, empty_page, short_page, source


def token_page(token='b', **options):
    return short_page(body=token, **options).replace(
        f'<div id="article-container"><p>{token}</p>'.encode(),
        f'<div id="article-container"><section style="margin-top:30px"><p>{token}</p></section><p></p>'.encode())


def require_placeholder(payload, policy):
    with pytest.raises(NewsError, match='^source_article_empty_template$') as caught:
        extract_article(payload, policy, page_url=URL)
    assert not caught.value.retry


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('token', ['b', '-'])
def test_exact_whole_native_tokens_are_failure_outcomes_not_bodies(cnyes_config, tmp_path, parser, token):
    cnyes_config['providers'][0]['html_parser'] = parser
    require_placeholder(token_page(token), source(cnyes_config, tmp_path))


@pytest.mark.parametrize('token', ['b', '-'])
@pytest.mark.parametrize('options', [
    {'change': {'content': '<p>source prose</p>'}}, {'change': {'payment': 1}},
    {'change': {'payment': False}}, {'change': {'newsId': 43}},
    {'change': {'publishAt': 1388534401}}, {'change': {'video': 'source video'}},
    {'change': {'title': 'another title'}}, {'canonical': URL.replace('/42', '/43')},
    {'change': {'url': URL.replace('/42', '/43')}}, {'conflict': True}, {'duplicate_key': True},
    {'schema_change': {'datePublished': '2014-01-01T00:00:00'}},
])
def test_tokens_never_waive_same_article_free_clock_or_native_content(cnyes_config, tmp_path, token, options):
    with pytest.raises(NewsError) as caught:
        extract_article(token_page(token, **options), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('token', ['b', '-'])
@pytest.mark.parametrize('mutation', [
    lambda p: p.replace(b'<section style="margin-top:30px">', b'<section style="margin-top:30px" hidden>'),
    lambda p: p.replace(b'<section style="margin-top:30px">', b'<section style="margin-top:31px">'),
    lambda p: p.replace(b'<section style="margin-top:30px">', b'<section style="margin-top:30px"><!--unknown-->'),
    lambda p: p.replace(b'<section style="margin-top:30px"><p>', b'<section style="margin-top:30px"><p hidden>'),
    lambda p: p.replace(b'</section><p></p>', b'</section><p><img src="original.png"></p>'),
    lambda p: p.replace(b'</section><p></p>', b'</section><p></p><p hidden>source prose</p>'),
    lambda p: p.replace(b'</section><p></p>', b'</section><p></p><!--unknown-->'),
    lambda p: p.replace(b'<h1>', b'<h1 hidden>'),
])
def test_no_hidden_extra_or_unknown_renderer_content_is_cleaned_away(cnyes_config, tmp_path, parser, token, mutation):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError) as caught:
        extract_article(mutation(token_page(token)), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('token', ['b', '-', 'B', '--', 'nullish'])
def test_tokens_do_not_generalize_to_arbitrary_short_dom_or_short_body_acceptance(cnyes_config, tmp_path, token):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(short_page(body=token), source(cnyes_config, tmp_path), page_url=URL)
    assert extract_article(short_page(), source(cnyes_config, tmp_path), page_url=URL)['body'] == BODY


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('encoded', [False, True])
def test_only_pinned_entire_style_comment_can_prove_current_empty_view(cnyes_config, tmp_path, monkeypatch, parser, encoded):
    import downloader.news_fulltext_publishers as publishers

    assert publishers._CNYES_EMPTY_STYLE_COMMENT == (
        12866, 'ff6ff7680b73b558829e927516186534d5c588d3daa5fbda80f0889c5dc07922')
    # No publisher body/boilerplate fixture is kept in Git. The private native
    # receipt proof independently checks the actual production hash and length.
    marker = '<!--font-family:synthetic;-->'
    cnyes_config['providers'][0]['html_parser'] = parser
    policy = source(cnyes_config, tmp_path)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(empty_page(change={'content': marker}), policy, page_url=URL)
    monkeypatch.setattr(publishers, '_CNYES_EMPTY_STYLE_COMMENT', (len(marker), hashlib.sha256(marker.encode()).hexdigest()))
    require_placeholder(empty_page(change={'content': html.escape(marker) if encoded else marker}), policy)
    for changed in (marker + ' ', marker + '\n', marker.replace('synthetic', 'different'), marker + '<p></p>'):
        with pytest.raises(NewsError) as caught:
            extract_article(empty_page(change={'content': changed}), policy, page_url=URL)
        assert caught.value.code != 'source_article_empty_template'
    for extra in ('<!--unknown-->', '<p></p>', '<p hidden>source prose</p>', '<img src="source.png">'):
        payload = empty_page(change={'content': marker}).replace(b'<div id="article-container">',
            ('<div id="article-container">' + extra).encode())
        with pytest.raises(NewsError) as caught:
            extract_article(payload, policy, page_url=URL)
        assert caught.value.code != 'source_article_empty_template'


def previous_registry(config, root, monkeypatch):
    original = hashlib.sha256

    def without_outcome(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and 'verified_native_degenerate_outcome_contract_version' in payload:
            payload = deepcopy(payload)
            assert payload.pop('verified_native_degenerate_outcome_contract_version') == 1
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)

    with monkeypatch.context() as patch:
        patch.setattr('downloader.news_fulltext_policy.hashlib.sha256', without_outcome)
        return PolicyRegistry(config, base=root)


def test_only_outcome_capture_changes_not_body_replay_bulk_authorization_or_pacing(cnyes_config, tmp_path, monkeypatch):
    current = source(cnyes_config, tmp_path)
    before = previous_registry(cnyes_config, tmp_path, monkeypatch).sources['cnyes']
    assert current.data_fingerprint != before.data_fingerprint
    for field in ('parser_repair_fingerprint', 'cnyes_bulk_data_fingerprint',
                  'http_capacity_source_fingerprint', 'authorization_fingerprint',
                  'cnyes_index_data_fingerprint'):
        assert getattr(current, field) == getattr(before, field)


def test_outcome_rollout_and_restart_preserve_all_existing_heads_and_deadlines(cnyes_config, tmp_path, monkeypatch):
    old = NewsQueue(tmp_path, previous_registry(cnyes_config, tmp_path, monkeypatch))
    states = ['complete', 'pending', 'retry', 'retry_cooldown', 'http_403', 'http_429',
              'source_article_empty_template', 'unsupported_article_path', 'article_attribution_not_verified']
    with old.db:
        for index, state in enumerate(states, 1):
            url = URL.rsplit('/', 1)[0] + '/' + str(index)
            old.add_articles([{'url': url}])
            old.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=?,body_sha256=? WHERE url=?',
                (state, 'body_not_found' if state in ('retry', 'retry_cooldown') else state,
                 time.time() + 6000, '2026-10-14T04:00:00+00:00', 'a' * 64 if state == 'complete' else None, url))
    before = [dict(r) for r in old.db.execute('SELECT * FROM articles ORDER BY url')]
    old.close()
    for _ in range(2):
        new = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
        try:
            assert [dict(r) for r in new.db.execute('SELECT * FROM articles ORDER BY url')] == before
        finally:
            new.close()
