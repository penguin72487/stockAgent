"""Diagnose an unknown layout without accepting its body or resetting budgets."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import sha256_file
from downloader.news_fulltext_failure_focus import configure_focus
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page
from test_news_failure_diagnostics import fail, reports
from test_news_failure_focus import spec
from test_news_failure_raw_scope_observation import context


def unknown_page(**options):
    return short_page(**options).replace(b'id="article-container"', b'id="unreviewed-body-layout"')


def source(config, tmp_path):
    return PolicyRegistry(config, base=tmp_path).sources['cnyes']


@pytest.mark.parametrize('flags', [{}, {'isAccessibleForFree': True}, {'isAccessibleForFree': 'true'}])
def test_unknown_body_layout_has_independent_private_identity_not_article_acceptance(cnyes_config, tmp_path, flags):
    policy = source(cnyes_config, tmp_path)
    raw = unknown_page(schema_change=flags)
    response = Response(URL, 200, {'content-type': 'text/html'}, raw)
    observed = {}
    assert not diagnostics._public_cnyes_scope(context(response, policy), policy, observed)
    assert observed['first_failed_check'] == 'visible_article_layout_not_verified'
    assert diagnostics._public_cnyes_scope(context(response, policy), policy, observed, require_body_layout=False)
    assert observed == {'contract_version': 2, 'raw_scope_proved': True, 'first_failed_check': None,
        'proof_basis': 'canonical_current_public_identity_without_body_layout_proved',
        'original_layout_check': 'visible_article_layout_not_verified', 'independent_identity_only': True}
    with pytest.raises(NewsError) as caught:
        extract_article(raw, policy, page_url=URL)
    assert caught.value.code == 'body_not_found'


@pytest.mark.parametrize('case', ['canonical', 'multiple_canonicals', 'different_current_id', 'conflicting_current',
    'paid', 'boolean_payment', 'epoch_type', 'wrong_epoch', 'empty_title', 'wrong_current_url',
    'wrong_heading', 'missing_heading', 'duplicate_heading', 'excluded', 'schema_missing',
    'malformed_schema', 'duplicate_schema_key', 'multiple_current_schema', 'wrong_schema_title',
    'wrong_schema_clock', 'naive_schema_clock'])
def test_independent_identity_rejects_conflicts_and_malformed_declarations(cnyes_config, tmp_path, case):
    changes = {'different_current_id': {'newsId': 43}, 'paid': {'payment': 1},
        'boolean_payment': {'payment': False}, 'epoch_type': {'publishAt': '1388534400'},
        'wrong_epoch': {'publishAt': 1388534401}, 'empty_title': {'title': ''},
        'wrong_current_url': {'url': URL + '0'}}
    schemas = {'schema_missing': {'@type': 'ItemList'}, 'wrong_schema_title': {'headline': 'another story'},
        'wrong_schema_clock': {'datePublished': '2014-01-02T00:00:00.000Z'},
        'naive_schema_clock': {'datePublished': CLOCK.removesuffix('Z')}}
    raw = unknown_page(change=changes.get(case), schema_change=schemas.get(case),
        conflict=case == 'conflicting_current', canonical=URL + '0' if case == 'canonical' else URL)
    substitutions = {
        'multiple_canonicals': (b'</head>', f'<link rel="canonical" href="{URL}"></head>'.encode()),
        'wrong_heading': (('<h1>' + TITLE + '</h1>').encode(), b'<h1>another story</h1>'),
        'missing_heading': (b'<h1>', b'<h2>'),
        'duplicate_heading': (b'</h1>', b'</h1><h1>another story</h1>'),
        'malformed_schema': (b'{"@graph":', b'{"@graph":bad-private-json,'),
        'duplicate_schema_key': (b'"@type": "NewsArticle"', b'"@type":"ItemList","@type": "NewsArticle"'),
        'multiple_current_schema': (b'</head>', raw[raw.index(b'<script type="application/ld+json">'):raw.index(b'</head>')] + b'</head>'),
    }
    if case in substitutions:
        raw = raw.replace(*substitutions[case], 1)
    policy = source(cnyes_config, tmp_path)
    if case == 'excluded':
        policy = replace(policy, excluded_article_title_pattern='測試')
    observed = {}
    assert not diagnostics._public_cnyes_scope(context(Response(URL, 200, {'content-type': 'text/html'}, raw), policy),
        policy, observed, require_body_layout=False)
    assert observed['contract_version'] == 2 and not observed['raw_scope_proved']
    assert TITLE not in json.dumps(observed, ensure_ascii=False) and 'bad-private' not in json.dumps(observed)


@pytest.mark.parametrize('value', [False, None, 0, 1, 'false', 'FALSE', 'TRUE', 'yes', {}, []])
def test_independent_schema_access_is_not_truthiness(cnyes_config, tmp_path, value):
    policy = source(cnyes_config, tmp_path)
    raw = unknown_page(schema_change={'isAccessibleForFree': value})
    observed = {}
    assert not diagnostics._public_cnyes_scope(context(Response(URL, 200, {'content-type': 'text/html'}, raw), policy),
        policy, observed, require_body_layout=False)
    assert observed['first_failed_check'] == 'schema_clock_or_title_or_access_not_verified'


@pytest.mark.parametrize('attribute', ['hidden', 'inert', 'aria-hidden="true"',
    'style="display:none"', 'style="display: none !important"', 'style="visibility:hidden"',
    'style="content-visibility:hidden"', 'style="opacity:0.00"',
    'class="hidden"', 'class="sr-only"', 'class="d-none"', 'class="visually-hidden"'])
@pytest.mark.parametrize('where', ['heading', 'ancestor', 'child'])
def test_hidden_heading_text_cannot_witness_identity(cnyes_config, tmp_path, attribute, where):
    raw = unknown_page()
    if where == 'heading':
        raw = raw.replace(b'<h1>', ('<h1 ' + attribute + '>').encode())
    elif where == 'ancestor':
        raw = raw.replace(b'<section>', ('<section ' + attribute + '>').encode())
    else:
        raw = raw.replace(('<h1>' + TITLE + '</h1>').encode(),
                          ('<h1><span ' + attribute + '>' + TITLE + '</span></h1>').encode())
    policy = source(cnyes_config, tmp_path)
    observed = {}
    assert not diagnostics._public_cnyes_scope(context(Response(URL, 200, {'content-type': 'text/html'}, raw), policy),
        policy, observed, require_body_layout=False)
    assert observed['first_failed_check'] == 'diagnostic_heading_identity_not_verified'


@pytest.mark.parametrize('tag', ['template', 'noscript', 'nav', 'footer', 'aside', 'dialog'])
def test_non_article_heading_container_cannot_witness_identity(cnyes_config, tmp_path, tag):
    raw = unknown_page().replace(b'<section>', f'<{tag}>'.encode()).replace(b'</section>', f'</{tag}>'.encode())
    policy = source(cnyes_config, tmp_path)
    assert not diagnostics._public_cnyes_scope(context(Response(URL, 200, {'content-type': 'text/html'}, raw), policy),
        policy, require_body_layout=False)


@pytest.mark.parametrize('url', [URL.replace('https:', 'http:'), URL.replace('news.', 'm.'),
    URL.replace('news.cnyes.com', 'news.cnyes.com:443'), URL + '?exp=a', URL + '?token=private-key', URL + '#fragment'])
def test_separate_diagnostic_proof_does_not_expand_alias_or_secret_url_scope(cnyes_config, tmp_path, url):
    policy = source(cnyes_config, tmp_path)
    observed = {}
    assert not diagnostics._public_cnyes_scope(context(Response(url, 200, {'content-type': 'text/html'}, unknown_page()),
        policy, url), policy, observed, require_body_layout=False)
    assert not observed['raw_scope_proved'] and 'private-key' not in json.dumps(observed)


@pytest.mark.parametrize('marker,code', [(b'<title>Just a moment</title>', 'access_challenge'),
    (b'<div class="paywall"></div>', 'paywall_or_login'),
    (b'<form action="/login"></form>', 'paywall_or_login')])
def test_shared_access_gate_precedes_independent_raw_retention(cnyes_config, tmp_path, marker, code):
    raw = unknown_page().replace(b'</head>', marker + b'</head>')
    policy = source(cnyes_config, tmp_path)
    with pytest.raises(NewsError, match=code):
        diagnostics._public_cnyes_scope(context(Response(URL, 200, {'content-type': 'text/html'}, raw), policy),
            policy, require_body_layout=False)


@pytest.mark.parametrize('paid', [False, True])
def test_canonical_owner_keeps_private_pair_and_original_failure_not_accepted_body(cnyes_config, tmp_path, paid):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    raw = unknown_page(change={'payment': 1} if paid else None)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie'}, raw))
        paths, rows = reports(tmp_path)
        assert len(rows) == 1
        receipt = json.loads(paths[0].with_suffix('.receipt.json').read_bytes())
        assert receipt['sha256'] == sha256_file(paths[0])
        assert receipt['response_sha256'] == hashlib.sha256(raw).hexdigest()
        assert receipt['raw_scope_observation'] == rows[0]['raw_scope_observation']
        assert receipt['raw_scope_observation']['contract_version'] == 2
        assert receipt['raw_response_retained'] is not paid
        assert rows[0]['raw_response_body'] == (None if paid else raw)
        assert all(receipt[key] is False for key in ('accepted_as_article', 'publish', 'remote_training_export'))
        assert 'private-cookie' not in json.dumps(receipt)
        assert tuple(queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()) == ('retry', 'body_not_found', None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT reserved_responses FROM failure_evidence_focus_cohorts WHERE cohort_id=?',
                               (spec()['cohort_id'],)).fetchone()[0] == 1


def test_scope_fallback_never_runs_for_duplicate_reserved_url_even_in_new_cohort(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, unknown_page()))
        before = queue.db.execute('SELECT reserved_responses FROM failure_evidence_focus_cohorts WHERE cohort_id=?',
                                  (spec()['cohort_id'],)).fetchone()[0]
        hashes = [(p, sha256_file(p)) for p in tmp_path.rglob('*.receipt.json')]
        configure_focus(queue, {**spec(), 'cohort_id': 'new-cohort-does-not-reset-url'})
        monkeypatch.setattr(diagnostics, '_public_cnyes_scope', lambda *_a, **_k: pytest.fail('duplicate reparsed'))
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, unknown_page()))
        assert queue.db.execute('SELECT reserved_responses FROM failure_evidence_focus_cohorts WHERE cohort_id=?',
                               ('new-cohort-does-not-reset-url',)).fetchone()[0] == 0
        assert len(reports(tmp_path)[0]) == 1
        assert all(sha256_file(p) == digest for p, digest in hashes)
        assert queue.db.execute('SELECT reserved_responses FROM failure_evidence_focus_cohorts WHERE cohort_id=?', (spec()['cohort_id'],)).fetchone()[0] == before
