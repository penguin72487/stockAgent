"""Missing native originals share the owner, pacing and once-only mailbox."""
from copy import deepcopy
from dataclasses import replace
import json
import time
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    PendingOriginal, private_contentplatform_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_ORIGINAL_LAYOUT, CONTENTPLATFORM_ORIGINAL_QUESTION
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_contentplatform import URL, RAW, entry, page
from test_news_fulltext import config


@pytest.fixture
def owner(config, tmp_path):
    settings = deepcopy(config)
    native = entry()
    native['embedded_article_layout'] = CONTENTPLATFORM_ORIGINAL_LAYOUT
    settings.update(root=str(tmp_path), providers=[native])
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=2,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
            ('failure_diagnostic_attempts:v1:contentplatform:20261024', '2'))
    yield queue, settings
    queue.close()


def head(queue): return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def submit(queue, settings, reason='One still-missing signed native geometry'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, CONTENTPLATFORM_ORIGINAL_LAYOUT,
        reason=reason, evidence_contract_version=43)


def response(**changes):
    return replace(Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'SECRET COOKIE'},
        page(extra='<div id="SECRET_VALUE">Unreviewed geometry</div>')), **changes)


def http_stub(raw):
    calls = []
    def get(url): calls.append(url); return raw
    return SimpleNamespace(get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda _: (), blocked_robots_origins=lambda _: ())


@pytest.mark.parametrize('version', [1, 40, 41, 42, 43, 44, 45, 46, True])
def test_current_source_and_fixed_question_only(owner, version):
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    assert candidate_evidence_allowed(source, source, version) == (type(version) is int and version in {43, 44, 45})
    assert not candidate_evidence_allowed(source, replace(source, data_fingerprint='a'*64), version)
    assert not candidate_evidence_allowed(replace(source, embedded_article_layout='contentplatform_native_article_v1'), source, version)


@pytest.mark.parametrize('change', [{'allowed': False}, {'known_url_only': False}, {'domains': ('example.org',)},
    {'require_author_attribution': False}, {'selectors': ('body',)},
    {'wordpress_apis': ('https://www.contentplatform.info/wp-json/wp/v2/posts',)}])
def test_diagnosis_does_not_expand_capture_scope(owner, change):
    queue, _ = owner
    source = replace(queue.registry.sources['contentplatform'], **change)
    assert not candidate_evidence_allowed(source, source, 43)


def test_once_only_main_owner_witness_keeps_heads_and_old_daily_charges(owner, monkeypatch):
    queue, settings = owner
    before = head(queue)
    charges = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no second HTTP owner'))
    request = submit(queue, settings)
    assert submit(queue, settings, reason='Other words')['request_id'] == request['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(response())
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    assert not http.calls
    assert tuple(queue.db.execute('SELECT state,owner_id FROM native_candidate_jobs WHERE request_id=?',
        (request['request_id'],)).fetchone()) == ('reserved', queue.owner)
    original = diagnostic.fetch_candidate(http, job)
    assert isinstance(original, PendingOriginal) and http.calls == [URL]
    assert original.permit['question'] == CONTENTPLATFORM_ORIGINAL_QUESTION
    assert RAW not in json.dumps(original.facts) and 'SECRET' not in json.dumps(original.facts)
    diagnostic.finish_candidate(queue, job, result=original)
    status = diagnostic.candidate_status(queue.root, request['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained']
    ref = status['result_json']['private_original']
    record, = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    assert record['raw_response_body'] == response().body and 'SECRET' not in record['headers_json']
    assert (queue.root/ref['path']).stat().st_mode & 0o077 == 0
    assert head(queue) == before and list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == charges
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not submit(queue, settings)['submitted']


def test_parser_transition_keeps_completed_original_question_closed(owner):
    from downloader.news_fulltext_candidates import native_candidate_source
    queue, settings = owner
    request = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(response())
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    closed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
    settings['providers'][0]['embedded_article_layout'] = 'contentplatform_native_article_v3'
    queue.registry = PolicyRegistry(settings, base=queue.root)
    with pytest.raises(ValueError, match='reviewed_current_contentplatform_source_required'):
        native_candidate_source(settings, queue.registry, queue.root, CONTENTPLATFORM_ORIGINAL_LAYOUT)
    layout = 'contentplatform_native_article_v3'
    assert native_candidate_source(settings, queue.registry, queue.root, layout) == queue.registry.sources['contentplatform']
    duplicate = diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, layout,
        reason='A parser version cannot re-arm the closed same question', evidence_contract_version=43)
    assert duplicate['request_id'] == request['request_id'] and not duplicate['submitted']
    diagnostic.pump_candidates(queue)
    assert diagnostic.claim_candidate(queue, http, settings, 'contentplatform') is None
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone()) == closed
    assert http.calls == [URL]


@pytest.fixture
def retained(owner, monkeypatch, tmp_path):
    from downloader.artifact_io import atomic_write_json, sha256_file
    from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
    from downloader.news_fulltext_policy import extract_article
    from downloader.news_fulltext_queue import utc_now
    from downloader.news_fulltext_retained_native import _article_digest
    queue, settings = owner
    before = head(queue)
    requested = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    raw_response = response(body=page(extra='<h3 data-start="421" data-end="442">'
        '<img class="alignnone size-full wp-image-378186" src="/never-fetched.png"></h3>'))
    http = http_stub(raw_response)
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    completed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
    ref = json.loads(completed['result_json'])['private_original']
    raw, = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    old = queue.registry.sources['contentplatform']
    settings['providers'][0]['embedded_article_layout'] = 'contentplatform_native_article_v3'
    queue.registry = PolicyRegistry(settings, base=queue.root)
    source = queue.registry.sources['contentplatform']
    monkeypatch.setattr('downloader.news_fulltext_contentplatform.CONTENTPLATFORM_V2_ORIGINAL_BINDING',
        (old.data_fingerprint, old.authorization_fingerprint))
    article = extract_article(raw['raw_response_body'], source, page_url=URL)
    item = {'url': URL, 'request_id': requested['request_id'],
        'diagnostic_path': ref['path'], 'diagnostic_receipt_path': ref['receipt_path'],
        'diagnostic_sha256': ref['sha256'], 'diagnostic_receipt_sha256': sha256_file(queue.root/ref['receipt_path']),
        'response_sha256': ref['response_sha256'], 'response_bytes': ref['response_bytes'],
        'captured_at_utc': raw['observed_at_utc'], 'candidate_job_sha256': _article_digest(completed),
        'article_keys': sorted(article), 'article_fields_sha256': _article_digest(article),
        'expected_missing_head': {k: before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{k: article[k] for k in ('body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw')}}
    report = tmp_path/'review'/'observed.json'
    atomic_write_json(report, {'private_originals': [{'request_id': requested['request_id'], 'url': URL,
        'final_url': URL, 'reference': ref, 'receipt_sha256': item['diagnostic_receipt_sha256'],
        'accepted_as_body': False}]})
    approval = {'contract_version': 2, 'scope_kind': 'reviewed_resident_native_materialization_v2',
        'provider': source.provider, 'data_fingerprint': source.data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint, 'approved_at_utc': utc_now(),
        'diagnostic_scope_unchanged': True, 'parser_transition_reviewed': True, 'publish': False,
        'remote_training_export': False, 'observation_report': report.name,
        'observation_report_sha256': sha256_file(report), 'entries': [item]}
    path = report.parent/'approval.json'
    atomic_write_json(path, approval)
    return queue, path, approval, raw, ref


def test_resident_signed_original_reuses_raw_once_and_preserves_source_clocks(retained, monkeypatch):
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue, path, approval, raw, ref = retained
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('Retained repair cannot start HTTP'))
    jobs = list(map(tuple, queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')))
    result = materialize_reviewed_native(queue, [URL], path, lease_seconds=300)
    assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
    saved, = pq.ParquetFile(queue.root/head(queue)['parquet_path']).read().to_pylist()
    assert saved['body'] == f'【許家源記者 / 綜合報導】\n{RAW}\n新聞來源:原公司'
    assert saved['raw_response_body'] == raw['raw_response_body'] and saved['replay_source_parquet'] == ref['path']
    assert saved['captured_at_utc'] == saved['source_observed_at_utc'] == raw['observed_at_utc']
    assert saved['http_duration_seconds'] is None and saved['original_diagnostic_authorization_json'] is None
    assert saved['original_diagnostic_authorization_fingerprint'] == raw['authorization_fingerprint']
    assert saved['original_transport_redirects_stored'] is False and not saved['historical_point_in_time']
    assert not saved['remote_training_export'] and not saved['training_injection_enabled']
    assert list(map(tuple, queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id'))) == jobs
    complete = head(queue)
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies'] == 0
    assert head(queue) == complete


@pytest.mark.parametrize('field', ['candidate_job_sha256', 'diagnostic_receipt_sha256', 'response_sha256', 'article_fields_sha256'])
def test_retained_signed_original_revalidates_approval_before_claim(retained, field):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue, path, approval, *_ = retained
    approval['entries'][0][field] = '0'*64
    atomic_write_json(path, approval)
    before = head(queue)
    with pytest.raises(ValueError):
        materialize_reviewed_native(queue, [URL], path, lease_seconds=300)
    assert head(queue) == before


@pytest.mark.parametrize('change', [
    {'state': 'http_403', 'error_code': 'http_403'}, {'state': 'http_429', 'error_code': 'http_429'},
    {'due': time.time()+3600}, {'lease_owner': 'other', 'lease_until': time.time()+3600}, {'attempts': 99},
])
def test_retained_signed_original_preserves_newer_or_refused_heads(retained, change):
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue, path, *_ = retained
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?', (*change.values(), URL))
    before = head(queue)
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies'] == 0
    assert head(queue) == before


@pytest.mark.parametrize('change', [
    {'credit': '中央社記者'}, {'canonical': URL+'?alias=1'}, {'body_attrs': 'hidden'},
    {'published': '2026-10-24T10:00:00'}, {'extra': '<iframe src="/unknown"></iframe>'},
])
def test_unverified_native_original_keeps_only_failure_location(owner, change):
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    raw = response(body=page(**change))
    with pytest.raises(NewsError): private_contentplatform_original_permit(raw, source, URL)
    facts = candidate_response_metadata(raw, source, source, evidence_contract_version=43, requested_url=URL)
    assert 'private_original_permit' not in facts and not facts['raw_or_names_or_body_retained']
    assert 'private_original_scope_error' in facts and 'private_original_scope_failure_location' in facts
    assert RAW not in json.dumps(facts) and 'SECRET' not in json.dumps(facts)


@pytest.mark.parametrize('change', [{'status': 403}, {'status': 429}, {'body': b''},
    {'body': b'x'*(2**21+1)}, {'url': URL+'?other=1'}, {'headers': {'content-type': 'application/json'}}])
def test_rejected_or_changed_transport_never_retains_original(owner, change):
    queue, _ = owner
    with pytest.raises(NewsError):
        private_contentplatform_original_permit(response(**change), queue.registry.sources['contentplatform'], URL)


@pytest.mark.parametrize('blocked', ['deadline', 'gateway', 'robots', 'uncertain'])
def test_source_gates_or_unknown_prior_operation_cannot_dispatch(owner, blocked):
    queue, settings = owner
    request = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(response())
    if blocked == 'deadline': http.provider_not_before['contentplatform'] = time.time()+3600
    elif blocked == 'gateway': http.blocked_gateway_hosts = lambda _: ('www.contentplatform.info',)
    elif blocked == 'robots': http.blocked_robots_origins = lambda _: ('https://www.contentplatform.info',)
    else:
        with queue.db:
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (request['request_id'],))
    assert diagnostic.claim_candidate(queue, http, settings, 'contentplatform') is None and not http.calls
    assert not submit(queue, settings)['submitted']


def test_retention_error_is_terminal_without_retry_or_article_mutation(owner, monkeypatch):
    queue, settings = owner
    before = head(queue)
    request = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(response())
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    original = diagnostic.fetch_candidate(http, job)
    def broken(*args, **kwargs): raise OSError('SECRET local path')
    monkeypatch.setattr(diagnostic, 'retain_original', broken)
    diagnostic.finish_candidate(queue, job, result=original)
    status = diagnostic.candidate_status(queue.root, request['request_id'])
    assert status['state'] == 'observed_error' and status['raw_retention_state'] == 'unknown'
    assert status['raw_or_names_or_body_retained'] is None and head(queue) == before
    assert 'SECRET' not in json.dumps(status) and not submit(queue, settings)['submitted']
    assert http.calls == [URL]


@pytest.mark.parametrize('changes', [
    {'state': 'complete', 'body_sha256': 'a'*64},
    {'state': 'retry_cooldown', 'due': time.time()+3600},
    {'state': 'article_rights_or_scope_excluded', 'error_code': 'article_rights_or_scope_excluded'},
])
def test_completed_denied_or_out_of_question_heads_cannot_submit(owner, changes):
    queue, settings = owner
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(key+'=?' for key in changes)+' WHERE url=?',
            (*changes.values(), URL))
    before = head(queue)
    with pytest.raises(ValueError): submit(queue, settings)
    assert head(queue) == before and not (queue.root/'reviewed_repairs/candidate_diagnostics/pending').exists()


def test_owner_revalidates_exact_immutable_bytes_before_writing(owner):
    queue, settings = owner
    submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(response())
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    original = diagnostic.fetch_candidate(http, job)
    forged = replace(original, response=response(body=page(extra='<div>Different received original</div>')))
    with pytest.raises(ValueError, match='private_original_permit_mismatch'):
        retain_original(queue, job, forged)
    assert not list((queue.root/'reviewed_repairs/candidate_diagnostics/originals').glob('*'))


def test_private_original_cli_submits_without_http_owner(owner, monkeypatch, tmp_path):
    from scripts import probe_news_sources as cli
    queue, settings = owner
    monkeypatch.setattr(cli, 'load_config', lambda _: (settings, queue.registry, tmp_path))
    monkeypatch.setattr(cli, 'require_current_missing_body_failures', lambda *a, **k: {})
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('CLI cannot own HTTP'))
    calls = []
    def submitter(*args, **kwargs): calls.append((args, kwargs)); return {'submitted': True, 'request_id': 'a'*64}
    monkeypatch.setattr(diagnostic, 'submit_candidate', submitter)
    args = ['--probe', URL, '--candidate-evidence-contract', '43', '--candidate-embedded-layout', CONTENTPLATFORM_ORIGINAL_LAYOUT,
        '--require-current-missing-body-failure', '--failure-code', 'article_identity_not_verified', '--output', str(tmp_path/'result.json')]
    with pytest.raises(SystemExit) as failure: cli.main(args)
    assert failure.value.code == 2 and not calls
    assert cli.main([*args, '--enqueue-candidate']) == 0
    assert len(calls) == 1 and calls[0][1]['evidence_contract_version'] == 43
