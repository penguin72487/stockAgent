"""Public identity is private evidence, never hidden-body acceptance or a retry reset."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext import load_config
from downloader.news_fulltext_failure_focus import configure_focus
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page
from test_news_failure_diagnostics import fail, reports
from test_news_failure_focus import spec, receipt_path
from test_news_failure_raw_scope_observation import context


def page(**options):
    # Unknown body layout and no rendered headline: metadata is not a body.
    return short_page(**options).replace(b'id="article-container"', b'id="unknown-layout"').replace(
        ('<h1>' + TITLE + '</h1>').encode(), b'<span>unrendered heading</span>')


def response(**options):
    return Response(URL, 200, {'content-type': 'text/html'}, page(**options))


def private_scope(raw, policy, observation):
    return diagnostics._public_cnyes_scope(context(raw, policy, raw.url), policy, observation,
        require_body_layout=False, require_heading_identity=False)


def ledger(queue):
    return [tuple(row) for row in queue.db.execute('SELECT * FROM failure_evidence_focus_attempts '
        'ORDER BY provider,url_sha256,attempt_number')]


def pair_hashes(root):
    return {str(p): sha256_file(p) for p in (root / 'diagnostics').rglob('*') if p.is_file()}


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('heading', ['absent', 'hidden', 'duplicate', 'wrong'])
def test_public_metadata_can_diagnose_rendering_failure_but_not_accept_body(cnyes_config, tmp_path, parser, heading):
    policy = replace(PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes'], html_parser=parser)
    raw = page()
    insert = {'absent': '', 'hidden': '<h1 hidden>' + TITLE + '</h1>',
        'duplicate': '<h1>' + TITLE + '</h1><h1>navigation heading</h1>',
        'wrong': '<h1>not the current article</h1>'}[heading].encode()
    raw = raw.replace(b'<section>', b'<section>' + insert)
    observed = {}
    assert private_scope(Response(URL, 200, {'content-type': 'text/html'}, raw), policy, observed)
    assert observed == {'contract_version': 3, 'raw_scope_proved': True, 'first_failed_check': None,
        'proof_basis': 'canonical_current_public_metadata_scope_proved',
        'original_layout_check': 'visible_article_layout_not_verified', 'independent_identity_only': True,
        'original_heading_check': 'diagnostic_heading_identity_not_verified', 'body_visibility_verified': False}
    assert TITLE not in json.dumps(observed, ensure_ascii=False)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(raw, policy, page_url=URL)


@pytest.mark.parametrize('case', ['paid', 'boolean_payment', 'missing_current', 'conflicting_current',
    'duplicate_current_key', 'string_epoch', 'wrong_epoch', 'empty_title', 'wrong_url',
    'wrong_canonical', 'duplicate_canonical', 'schema_missing', 'schema_malformed', 'duplicate_schema_key',
    'duplicate_schema', 'wrong_schema_title', 'wrong_schema_clock', 'naive_schema_clock',
    'access_false', 'access_string_false', 'access_one', 'access_null'])
def test_metadata_identity_requires_all_independent_positive_checks(cnyes_config, tmp_path, case):
    changes = {'paid': {'payment': 1}, 'boolean_payment': {'payment': False},
        'missing_current': {'newsId': 43}, 'string_epoch': {'publishAt': '1388534400'},
        'wrong_epoch': {'publishAt': 1388534401}, 'empty_title': {'title': ''}, 'wrong_url': {'url': URL + '0'}}
    schemas = {'schema_missing': {'@type': 'ItemList'}, 'wrong_schema_title': {'headline': 'not this article'},
        'wrong_schema_clock': {'datePublished': '2014-01-02T00:00:00Z'},
        'naive_schema_clock': {'datePublished': CLOCK.removesuffix('Z')},
        'access_false': {'isAccessibleForFree': False}, 'access_string_false': {'isAccessibleForFree': 'false'},
        'access_one': {'isAccessibleForFree': 1}, 'access_null': {'isAccessibleForFree': None}}
    raw = page(change=changes.get(case), schema_change=schemas.get(case),
        conflict=case == 'conflicting_current', duplicate_key=case == 'duplicate_current_key',
        canonical=URL + '0' if case == 'wrong_canonical' else URL)
    substitutions = {'duplicate_canonical': (b'</head>', f'<link rel="canonical" href="{URL}"></head>'.encode()),
        'schema_malformed': (b'{"@graph":', b'{"@graph":invalid,'),
        'duplicate_schema_key': (b'"@type": "NewsArticle"', b'"@type":"ItemList","@type": "NewsArticle"'),
        'duplicate_schema': (b'</head>', raw[raw.index(b'<script type="application/ld+json">'):raw.index(b'</head>')] + b'</head>')}
    if case in substitutions:
        raw = raw.replace(*substitutions[case], 1)
    policy = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    observed = {}
    assert not private_scope(Response(URL, 200, {'content-type': 'text/html'}, raw), policy, observed)
    assert observed['contract_version'] == 3 and not observed['raw_scope_proved']
    assert observed['body_visibility_verified'] is False and observed['first_failed_check']
    assert TITLE not in json.dumps(observed, ensure_ascii=False)


@pytest.mark.parametrize('marker,code', [(b'<title>Just a moment</title>', 'access_challenge'),
    (b'<div class="paywall"></div>', 'paywall_or_login'), (b'<form action="/login"></form>', 'paywall_or_login')])
def test_access_gate_precedes_private_metadata_identity(cnyes_config, tmp_path, marker, code):
    policy = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    raw = Response(URL, 200, {'content-type': 'text/html'}, page().replace(b'</head>', marker + b'</head>'))
    with pytest.raises(NewsError, match=code):
        private_scope(raw, policy, {})


@pytest.mark.parametrize('url', [URL + '?exp=a', URL + '?token=private-key', URL + '#fragment',
    URL.replace('news.', 'm.'), URL.replace('https:', 'http:'), URL.replace('news.cnyes.com', 'news.cnyes.com:443')])
def test_metadata_only_proof_does_not_expand_request_scope(cnyes_config, tmp_path, url):
    policy = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    observed = {}
    assert not private_scope(Response(url, 200, {'content-type': 'text/html'}, page()), policy, observed)
    assert not observed['raw_scope_proved'] and 'private-key' not in json.dumps(observed)


def test_metadata_proof_cannot_supply_visible_body_proof(cnyes_config, tmp_path):
    policy = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    with pytest.raises(ValueError, match='private_diagnostics_only'):
        diagnostics._public_cnyes_scope(context(response(), policy), policy, {}, require_heading_identity=False)


def test_first_failure_without_heading_can_retain_public_raw_without_extra_requests(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        row = reports(tmp_path)[1][0]
        assert row['contract_version'] == 7 and row['raw_response_body'] == response().body
        assert row['raw_scope_observation']['contract_version'] == 3
        assert row['retention_basis'] == 'canonical_current_public_metadata_identity_for_private_diagnostic_only'
        assert queue.failure_diagnostic_focus.progress()['publisher_requests_added'] == 0
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'raw_saved': 1}
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def first_metadata(queue):
    fail(queue, response(change={'payment': 1}))
    assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'metadata_saved': 1}
    return ledger(queue), pair_hashes(queue.root)


def test_explicit_positive_upgrade_appends_once_and_preserves_old_pair_and_counters(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        old_ledger, old_files = first_metadata(queue)
        fail(queue, response())
        paths, rows = reports(tmp_path)
        assert len(rows) == 2 and ledger(queue)[0] == old_ledger[0]
        assert all(sha256_file(p) == digest for p, digest in old_files.items())
        upgraded = next(row for row in rows if row['contract_version'] == 8)
        old_receipt = next(p for p in paths if json.loads(p.with_suffix('.receipt.json').read_bytes())['contract_version'] == 7).with_suffix('.receipt.json')
        assert upgraded['failure_diagnostic_metadata_identity_upgrade_from_receipt_path'] == str(old_receipt.relative_to(tmp_path))
        assert upgraded['failure_diagnostic_metadata_identity_upgrade_from_receipt_sha256'] == sha256_file(old_receipt)
        assert upgraded['failure_diagnostic_focus_attempt_number'] == 2
        assert upgraded['raw_response_body'] == response().body and upgraded['raw_response_retained'] is True
        assert upgraded['raw_scope_observation']['proof_basis'] == 'canonical_current_public_metadata_scope_proved'
        assert all(upgraded[key] is False for key in ('accepted_as_article', 'publish', 'remote_training_export'))
        progress = queue.failure_diagnostic_focus.progress()
        assert progress['outcomes'] == {'metadata_saved': 1, 'raw_saved': 1}
        assert progress['raw_bytes_reserved'] == len(response(change={'payment': 1}).body) + len(response().body)
        assert progress['write_attempts_reserved'] == progress['maximum_write_attempts'] == 2
        assert progress['maximum_raw_bytes'] == 256 << 20 and progress['publisher_requests_added'] == 0
        all_files = pair_hashes(tmp_path)
        for _ in range(3):
            fail(queue, response())
        assert pair_hashes(tmp_path) == all_files
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert tuple(queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()) == ('retry', 'body_not_found', None)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        assert queue.failure_diagnostic_focus.progress() == progress
        fail(queue, response())
        assert pair_hashes(tmp_path) == all_files


@pytest.mark.parametrize('enabled,new_cohort', [(False, False), (True, True)])
def test_disabled_or_cross_cohort_upgrade_cannot_reset_old_metadata(cnyes_config, tmp_path, monkeypatch, enabled, new_cohort):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        old_ledger, files = first_metadata(queue)
        configure_focus(queue, spec(cohort_id='no-cross-cohort-reset') if new_cohort else spec(), metadata_identity_upgrade=enabled)
        monkeypatch.setattr(diagnostics, '_public_cnyes_scope', lambda *_a, **_k: pytest.fail('suppressed duplicate parsed'))
        fail(queue, response())
        assert ledger(queue) == old_ledger and pair_hashes(tmp_path) == files


@pytest.mark.parametrize('changes', [{'payment': 1}, {'payment': False}, {'publishAt': 1}, {'url': URL + '0'}])
def test_unproved_upgrade_does_not_consume_second_attempt_or_change_first(cnyes_config, tmp_path, changes):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        before, files = first_metadata(queue)
        progress = queue.failure_diagnostic_focus.progress()
        fail(queue, response(change=changes))
        assert ledger(queue) == before and pair_hashes(tmp_path) == files
        assert queue.failure_diagnostic_focus.progress() == progress


def test_upgrade_does_not_overshoot_original_byte_cap(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(maximum_raw_bytes=len(response(change={'payment': 1}).body)), metadata_identity_upgrade=True)
        before, files = first_metadata(queue)
        fail(queue, response())
        assert ledger(queue) == before and pair_hashes(tmp_path) == files


def test_lost_upgrade_finish_recovers_from_same_pair_even_after_flag_disabled(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        first_metadata(queue)
        def broken(*_a):
            raise OSError('lost owner finish')
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'finish', broken)
        fail(queue, response())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'metadata_saved': 1, 'reserved': 1}
        files = pair_hashes(tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'metadata_saved': 1, 'raw_saved': 1}
        assert pair_hashes(tmp_path) == files
        fail(queue, response())
        assert pair_hashes(tmp_path) == files


@pytest.mark.parametrize('failure', ['no_files', 'partial'])
def test_upgrade_storage_failure_never_gets_a_third_attempt(cnyes_config, tmp_path, monkeypatch, failure):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        first_metadata(queue)
        original = diagnostics.atomic_write_parquet
        def broken(path, table, **kwargs):
            if failure == 'partial':
                original(path, table, **kwargs)
            raise OSError('interrupted storage')
        monkeypatch.setattr(diagnostics, 'atomic_write_parquet', broken)
        fail(queue, response())
        files = pair_hashes(tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        expected = 'write_failed' if failure == 'no_files' else 'reserved'
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'metadata_saved': 1, expected: 1}
        monkeypatch.setattr(diagnostics, '_public_cnyes_scope', lambda *_a, **_k: pytest.fail('exhausted URL parsed'))
        fail(queue, response())
        assert pair_hashes(tmp_path) == files and queue.failure_diagnostic_focus.progress()['write_attempts_reserved'] == 2


@pytest.mark.parametrize('mutation', ['missing_pointer', 'wrong_pointer', 'wrong_old_sha', 'missing_scope',
    'false_scope', 'wrong_scope_version', 'body_visibility_true', 'not_raw', 'original_pair_corrupt'])
def test_corrupt_upgrade_receipt_stays_unknown_without_overwrite(cnyes_config, tmp_path, monkeypatch, mutation):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        first_metadata(queue)
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'finish', lambda *_a: (_ for _ in ()).throw(OSError('lost finish')))
        fail(queue, response())
        path, = [p for p in reports(tmp_path)[0] if json.loads(p.with_suffix('.receipt.json').read_bytes())['contract_version'] == 8]
        receipt = json.loads(path.with_suffix('.receipt.json').read_bytes())
        if mutation == 'missing_pointer':
            receipt.pop('failure_diagnostic_metadata_identity_upgrade_from_receipt_path')
        elif mutation == 'wrong_pointer':
            receipt['failure_diagnostic_metadata_identity_upgrade_from_receipt_path'] = 'diagnostics/not-owned.receipt.json'
        elif mutation == 'wrong_old_sha':
            receipt['failure_diagnostic_metadata_identity_upgrade_from_receipt_sha256'] = 'a' * 64
        elif mutation == 'missing_scope':
            receipt['raw_scope_observation'] = None
        elif mutation == 'false_scope':
            receipt['raw_scope_observation']['raw_scope_proved'] = False
        elif mutation == 'wrong_scope_version':
            receipt['raw_scope_observation']['contract_version'] = 2
        elif mutation == 'body_visibility_true':
            receipt['raw_scope_observation']['body_visibility_verified'] = True
        elif mutation == 'not_raw':
            receipt['raw_response_retained'] = False
        else:
            atomic_write_json(tmp_path / receipt['failure_diagnostic_metadata_identity_upgrade_from_receipt_path'], {'corrupt': True})
        # Keep the candidate pair internally consistent, so rejection proves
        # the new origin/scope contract rather than a generic Parquet mismatch.
        identity = {k: v for k, v in receipt.items() if k not in {'path', 'sha256'}}
        atomic_write_parquet(path, table_from_records([{**identity, 'raw_response_body': response().body}]))
        receipt['sha256'] = sha256_file(path)
        atomic_write_json(path.with_suffix('.receipt.json'), receipt)
        files = pair_hashes(tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'metadata_saved': 1, 'reserved': 1}
        fail(queue, response())
        assert pair_hashes(tmp_path) == files


def test_upgrade_reservation_requires_explicit_boolean_proof(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(), metadata_identity_upgrade=True)
        before, files = first_metadata(queue)
        focus = queue.failure_diagnostic_focus
        for value in (False, 1, 'true', None):
            assert focus.reserve(URL, len(response().body), hashlib.sha256(response().body).hexdigest(),
                receipt_path(focus), metadata_identity_proved=value) is None
        assert ledger(queue) == before and pair_hashes(tmp_path) == files


@pytest.mark.parametrize('value', [1, 'true', None])
def test_upgrade_enable_is_not_truthiness(cnyes_config, tmp_path, value):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        with pytest.raises(ValueError):
            configure_focus(queue, spec(), metadata_identity_upgrade=value)


def test_upgrade_requires_explicit_finite_focus(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        with pytest.raises(ValueError, match='finite_focus'):
            configure_focus(queue, None, metadata_identity_upgrade=True)


@pytest.mark.parametrize('value,focus,valid', [(True, True, True), (False, False, True),
    (True, False, False), (1, True, False), ('true', True, False), (None, True, False)])
def test_config_upgrade_is_explicit_finite_not_ignored(cnyes_config, tmp_path, value, focus, valid):
    config = {**cnyes_config, 'failure_diagnostic_focus': spec() if focus else None,
        'failure_diagnostic_metadata_identity_upgrade': value,
        # This fixture tests metadata upgrade independently, not the paired
        # production replay opt-in which requires its own finite focus.
        'retained_cnyes_failure_replay_enabled': False}
    config.pop('provider_catalog', None)
    path = tmp_path / 'news.json'
    atomic_write_json(path, config)
    if valid:
        loaded, _, _ = load_config(path, repo=Path.cwd())
        assert loaded['failure_diagnostic_metadata_identity_upgrade'] is value
    else:
        with pytest.raises(ValueError, match='metadata identity upgrade'):
            load_config(path, repo=Path.cwd())
