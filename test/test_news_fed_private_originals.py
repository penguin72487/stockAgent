"""Synthetic bounded Board PDF witnesses, not original publisher documents."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext import load_config
from downloader.news_fulltext_candidate_originals import PendingOriginal, retain_original
from downloader.news_fulltext_candidates import candidate_evidence_allowed, native_candidate_source
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_pdf import (
    BOARD_PDF_ORIGINAL_LAYOUT as LAYOUT, BOARD_PDF_ORIGINAL_QUESTION as QUESTION,
    board_pdf_original_source, private_board_pdf_permit,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_candidate_diagnostics import http_stub

URL = 'https://www.federalreserve.gov/monetarypolicy/files/FOMC20000516material.pdf'
# An original can be malformed. Decoding/quality classification is a separate
# next action; its bytes must not be lost or labelled a full article here.
RAW = b'%PDF-1.7\nSynthetic undecoded original -12.50 0 3.90 percent\n%%EOF'


@pytest.fixture
def settings(tmp_path):
    config = deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    config['providers'] = [p for p in config['providers'] if p['id'] == 'fed_board']
    config.pop('provider_catalog', None)
    config.update(root=str(tmp_path), min_free_disk_bytes=1, workers=2, exclusive_http_owner=False,
        adaptive_rate={'enabled': False})
    return config


@pytest.fixture
def queue(settings, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    value.add_articles([{'url': URL}])
    with value.db:
        value.db.execute("UPDATE articles SET state='pdf_image_only_or_body_not_found',"
            "error_code='pdf_image_only_or_body_not_found',attempts=3,due=0,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (URL,))
    yield value
    value.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def response(**changes):
    return replace(Response(URL, 200, {'content-type': 'application/pdf'}, RAW), **changes)


def submit(queue, settings, **options):
    return jobs.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=options.pop('reason', 'Original Board PDF failure witness only; no body admission'),
        evidence_contract_version=options.pop('evidence', 12), **options)


def pump(queue):
    with patch.object(jobs.time, 'monotonic', return_value=0.0):
        jobs.pump_candidates(queue)


def claim(queue, settings, http):
    submitted = submit(queue, settings)
    pump(queue)
    return submitted, jobs.claim_candidate(queue, http, settings, 'fed_board')


def test_current_source_is_not_a_new_parser_or_permission(settings, queue):
    source = queue.registry.sources['fed_board']
    assert native_candidate_source(settings, queue.registry, queue.registry.base, LAYOUT) is source
    assert candidate_evidence_allowed(source, source, 12)
    for version in [*range(1, 12), 13, 0, True, None]:
        assert not candidate_evidence_allowed(source, source, version)
    assert jobs._failure_states(LAYOUT, 12) == {'pdf_image_only_or_body_not_found'}
    assert jobs._failure_states('bastillepost_visible_article_v7', 12) == set()
    with pytest.raises(ValueError, match='reviewed_candidate_evidence_contract_required'):
        submit(queue, settings, evidence=1)


def test_one_owner_retains_complete_bytes_and_clock_without_article_acceptance(settings, queue, monkeypatch):
    before, settings_before = head(queue), list(queue.db.execute('SELECT * FROM queue_settings'))
    client = http_stub(queue, response=response())
    monkeypatch.setattr('downloader.news_fulltext_pdf.extract_board_pdf', lambda *a, **k: pytest.fail('not a body replay'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no second HTTP owner'))
    requested, job = claim(queue, settings, client)
    assert jobs.candidate_status(queue.root, requested['request_id'])['raw_retention_state'] == 'unknown'
    original = jobs.fetch_candidate(client, job)
    assert isinstance(original, PendingOriginal) and original.permit['question'] == QUESTION
    assert not original.facts['PDF_decoded'] and not original.facts['image_only_or_malformed_verified']
    jobs.finish_candidate(queue, job, result=original); pump(queue)
    status = jobs.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'observed' and status['raw_retention_state'] == 'retained_verified'
    assert status['raw_or_names_or_body_retained'] and not status['accepted_as_article']
    facts = status['result_json']; reference = facts['private_original']
    path = queue.root/reference['path']; receipt_path = queue.root/reference['receipt_path']
    row, = pq.ParquetFile(path).read().to_pylist()
    assert row['raw_response_body'] == RAW and row['request_url'] == row['final_url'] == row['source_document_url'] == URL
    assert row['response_sha256'] == hashlib.sha256(RAW).hexdigest() and json.loads(row['transport_redirects_json']) == []
    assert row['observed_at_utc'] and not any(k in row for k in ('published_at_raw', 'body', 'pdf_metadata_raw_json'))
    for key in ('accepted_as_article', 'publish', 'historical_point_in_time', 'per_article_license_verified',
                'training_injection_enabled', 'remote_training_export'):
        assert row[key] is False
    assert row['diagnostic_only'] and json.loads(receipt_path.read_bytes())['permit'] == original.permit
    assert reference['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert all(not p.stat().st_mode & 0o077 for p in (path, receipt_path, path.parent))
    assert head(queue) == before and list(queue.db.execute('SELECT * FROM queue_settings')) == settings_before
    assert client.calls == [URL] and not (queue.root/'bodies').exists()
    assert not submit(queue, settings, reason='A new reason cannot rearm the question')['submitted']
    assert jobs.claim_candidate(queue, client, settings, 'fed_board') is None


@pytest.mark.parametrize('change', [
    {'provider': 'publisher'}, {'allowed': False}, {'domains': ('federalreserve.gov',)},
    {'legacy_release_layout': 'fed_table_release_v9'}, {'legacy_release_layout': 'fed_table_release_v11'},
    {'authorization': {'kind': 'personal_research'}}, {'pdf_path_pattern': ''},
    {'pdf_max_response_bytes': 0}, {'pdf_max_response_bytes': True}, {'pdf_max_response_bytes': 33 << 20},
])
def test_independent_source_scope_gate(queue, change):
    source = replace(queue.registry.sources['fed_board'], **change)
    assert not board_pdf_original_source(source)
    with pytest.raises(NewsError, match='private_pdf_original_scope_not_verified'):
        private_board_pdf_permit(response(), source, URL)


@pytest.mark.parametrize('literal', [
    URL.replace('https:', 'http:'), URL+'?download=1', URL+'#page=1',
    URL.replace('www.federalreserve.gov', 'evil.test'), URL.replace('www.', ''),
    URL.replace('www.federalreserve.gov', 'user@www.federalreserve.gov'),
    URL.replace('www.federalreserve.gov', 'www.federalreserve.gov:443'),
    'https://www.federalreserve.gov/unregistered.pdf',
    'https://www.federalreserve.gov/monetarypolicy/fomcpresconf20110427.htm', None, 123,
])
def test_exact_registered_pdf_url_not_inferred_transport(queue, literal):
    with pytest.raises(NewsError, match='private_pdf_original_scope_not_verified'):
        private_board_pdf_permit(response(), queue.registry.sources['fed_board'], literal)


@pytest.mark.parametrize('changes', [
    {'status': 304}, {'status': 403}, {'status': 429}, {'status': 404},
    {'headers': {'content-type': 'text/html'}}, {'headers': {'content-type': 'application/octet-stream'}},
    {'body': b'<html>Not a PDF</html>'}, {'body': b''}, {'body': bytearray(RAW)},
    {'url': URL.replace('20000516', '20001219')},
    {'redirects': (RedirectHop(URL, 301, URL, URL, '2026-01-01T00:00:00Z', 'a'*64, 0),)},
])
def test_response_status_mime_signature_and_actual_transport(queue, changes):
    with pytest.raises(NewsError, match='private_pdf_original_scope_not_verified'):
        private_board_pdf_permit(response(**changes), queue.registry.sources['fed_board'], URL)


def test_document_bound_exceeds_html_limit_but_respects_registered_limit(settings, queue):
    source = queue.registry.sources['fed_board']; raw = b'%PDF-1.7\n'+b'x'*(2 << 20)
    permit = private_board_pdf_permit(response(body=raw), source, URL)
    assert permit['response_sha256'] == hashlib.sha256(raw).hexdigest()
    with pytest.raises(NewsError):
        private_board_pdf_permit(response(body=RAW), replace(source, pdf_max_response_bytes=len(RAW)-1), URL)


@pytest.mark.parametrize('state', ['complete', 'source_document_landing_page', 'http_403', 'paywall_or_login',
    'robots_disallowed', 'article_rights_or_scope_excluded', 'body_not_found', 'pending'])
def test_only_unstored_pdf_parser_failure_is_eligible(settings, queue, state):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert not (queue.root/'reviewed_repairs').exists()


def test_fixed_question_key_and_future_provider_contracts(settings, queue):
    target = jobs._head(head(queue))
    original = jobs._key(target, 'a'*64, evidence_contract_version=12)
    assert original == jobs._key(dict(target, attempts=999, due=0), 'b'*64, evidence_contract_version=12)
    for provider, evidence in [('bastillepost', 12), ('pocket', 12), ('fed_board', 13)]:
        with pytest.raises(ValueError):
            jobs._key(dict(target, provider=provider), 'a'*64, evidence_contract_version=evidence)


def test_source_gate_pause_keeps_original_ready_request(settings, queue):
    client = http_stub(queue, response=response())
    requested = submit(queue, settings); pump(queue)
    client.blocked_robots_origins = lambda p: ('https://www.federalreserve.gov',)
    assert jobs.claim_candidate(queue, client, settings, 'fed_board') is None
    assert jobs.candidate_status(queue.root, requested['request_id'])['state'] == 'ready'
    assert client.calls == []


def test_unknown_owner_loss_never_retries_original(settings, queue):
    requested, job = claim(queue, settings, http_stub(queue, response=response()))
    queue.native_candidate_schema_ready = False; queue.owner = 'new-owner'
    pump(queue)
    status = jobs.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'uncertain_outcome' and status['raw_retention_state'] == 'unknown'
    assert status['raw_or_names_or_body_retained'] is None
    assert not submit(queue, settings)['submitted']
    assert jobs.claim_candidate(queue, http_stub(queue), settings, 'fed_board') is None


def test_write_failure_is_uncertain_evidence_not_rearm_or_owner_crash(settings, queue, monkeypatch):
    requested, job = claim(queue, settings, http_stub(queue, response=response()))
    original = jobs.fetch_candidate(http_stub(queue, response=response()), job)
    monkeypatch.setattr(jobs, 'retain_original', lambda *a: (_ for _ in ()).throw(OSError('synthetic write failure')))
    jobs.finish_candidate(queue, job, result=original)
    status = jobs.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'observed_error' and status['raw_retention_state'] == 'unknown'
    assert status['raw_or_names_or_body_retained'] is None and not submit(queue, settings)['submitted']


def test_owner_rechecks_raw_permit_before_any_private_write(settings, queue):
    requested, job = claim(queue, settings, http_stub(queue, response=response()))
    original = jobs.fetch_candidate(http_stub(queue, response=response()), job)
    forged = replace(original, response=response(body=RAW+b'tampered'))
    with pytest.raises(ValueError, match='private_original_permit_mismatch'):
        retain_original(queue, job, forged)
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()


def test_legacy_unknown_check_is_retained_not_invented_and_does_not_prevent_new_original(settings, queue):
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=NULL WHERE url=?', (URL,))
    before = head(queue); client = http_stub(queue, response=response())
    assert jobs._head_problem(before, jobs.time.time(), allowed_failure_states={'pdf_image_only_or_body_not_found'}) == 'missing_or_invalid_check'
    requested, job = claim(queue, settings, client)
    original = jobs.fetch_candidate(client, job)
    jobs.finish_candidate(queue, job, result=original); pump(queue)
    status = jobs.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'observed' and status['raw_retention_state'] == 'retained_verified'
    archived = queue.root/'reviewed_repairs/candidate_diagnostics/processed'/(requested['request_id']+'.json')
    assert json.loads(archived.read_bytes())['target']['last_checked_at_utc'] is None
    assert head(queue) == before and client.calls == [URL] and not submit(queue, settings)['submitted']


@pytest.mark.parametrize('clock', ['', 'not-a-clock', '2026-01-01', '2099-01-01T00:00:00+00:00'])
def test_malformed_or_future_legacy_check_still_rejects_before_http(settings, queue, clock):
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (clock, URL))
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert not (queue.root/'reviewed_repairs').exists()


@pytest.mark.parametrize('changes', [
    {'lease_owner': 'other-owner', 'lease_until': 9999999999},
    {'body_sha256': 'a'*64, 'body_bytes': 1}, {'due': 9999999999},
    {'state': 'http_403', 'error_code': 'http_403'},
    {'state': 'source_document_landing_page', 'error_code': 'source_document_landing_page'},
])
def test_legacy_null_never_overrides_stored_lease_refusal_or_future_guards(settings, queue, changes):
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=NULL,'+','.join(k+'=?' for k in changes)+' WHERE url=?',
            (*changes.values(), URL))
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert not (queue.root/'reviewed_repairs').exists()
