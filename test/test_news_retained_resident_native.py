"""ABI2 reuses candidate17 receipts through the one native writer, zero HTTP."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_policy import PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
from downloader.news_fulltext_retained_native import _article_digest, materialize_reviewed_native
from downloader.news_fulltext_taiwannews import TAIWANNEWS_V3_LAYOUT, TAIWANNEWS_V3_VERIFICATION
from test_news_taiwannews_original import URL
from test_news_taiwannews_private_original import (
    diagnostic, http_stub, inline_page, previous_inline_observation, response, submit_inline,
)
from test_news_taiwannews_original import production_settings
from downloader.news_fulltext_http import RedirectHop


def rewrite(path, payload):
    atomic_write_json(path, payload, durable=True)


def head(queue, url):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


@pytest.fixture
def approved(tmp_path):
    settings, _ = production_settings(2)
    settings.update(root=str(tmp_path/'news'), min_free_disk_bytes=1, exclusive_http_owner=False)
    settings['adaptive_rate'] = {'enabled': False}
    requested = URL.replace('/zh/', '/ch/')
    old = NewsQueue(tmp_path/'news', PolicyRegistry(settings, base='.'))
    previous_inline_observation(old, settings, requested=requested)
    submitted = submit_inline(old, settings, requested)
    diagnostic.pump_candidates(old)
    hops = (RedirectHop(requested, 301, '/zh/news/1234567', URL,
        '2026-10-01T00:00:00+00:00', 'a'*64, 1),)
    http = http_stub(old, response=response(inline_page(), redirects=hops))
    job = diagnostic.claim_candidate(old, http, settings, 'taiwannews')
    diagnostic.finish_candidate(old, job, result=diagnostic.fetch_candidate(http, job))
    completed = dict(old.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (submitted['request_id'],)).fetchone())
    result = json.loads(completed['result_json'])
    ref = result['private_original']
    raw, = pq.ParquetFile(old.root/ref['path']).read().to_pylist()
    old_jobs = [tuple(row) for row in old.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')]
    before = head(old, requested)
    old.close()
    item = next(item for item in settings['providers'] if item['id'] == 'taiwannews')
    item.update(embedded_article_layout=TAIWANNEWS_V3_LAYOUT, adapter_verification=TAIWANNEWS_V3_VERIFICATION)
    registry = PolicyRegistry(settings, base='.')
    queue = NewsQueue(tmp_path/'news', registry)
    assert head(queue, requested) == before  # Registry change requeues nothing.
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')] == old_jobs
    source = registry.sources['taiwannews']
    article = extract_article(raw['raw_response_body'], source, page_url=raw['final_url'])
    entry = {'url': requested, 'request_id': submitted['request_id'],
        'diagnostic_path': ref['path'], 'diagnostic_receipt_path': ref['receipt_path'],
        'diagnostic_sha256': ref['sha256'], 'diagnostic_receipt_sha256': sha256_file(queue.root/ref['receipt_path']),
        'response_sha256': ref['response_sha256'], 'response_bytes': ref['response_bytes'],
        'captured_at_utc': raw['observed_at_utc'], 'candidate_job_sha256': _article_digest(completed),
        'article_keys': sorted(article), 'article_fields_sha256': _article_digest(article),
        'expected_missing_head': {k: before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{key: article[key] for key in ('body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw')}}
    report_path = tmp_path/'review'/'readback.json'
    rewrite(report_path, {'private_originals': [{'request_id': submitted['request_id'], 'url': requested,
        'final_url': raw['final_url'], 'reference': ref, 'receipt_sha256': entry['diagnostic_receipt_sha256'],
        'accepted_as_body': False}]})
    manifest = {'contract_version': 2, 'scope_kind': 'reviewed_resident_native_materialization_v2',
        'provider': source.provider, 'data_fingerprint': source.data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint, 'approved_at_utc': utc_now(),
        'diagnostic_scope_unchanged': True, 'parser_transition_reviewed': True,
        'publish': False, 'remote_training_export': False, 'observation_report': report_path.name,
        'observation_report_sha256': sha256_file(report_path), 'entries': [entry]}
    path = report_path.parent/'approved.json'; rewrite(path, manifest)
    yield queue, requested, path, manifest, ref, raw, old_jobs
    queue.close()


def replay(approved):
    queue, requested, path, *_ = approved
    return materialize_reviewed_native(queue, [requested], path, lease_seconds=300)


def test_resident_same_ID_actual_redirect_retains_clock_and_accepts_once_without_http(approved, monkeypatch):
    queue, requested, path, manifest, ref, raw, old_jobs = approved
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('retained evidence cannot construct an HTTP owner'))
    original_hashes = {key: sha256_file(queue.root/ref[key]) for key in ('path', 'receipt_path')}
    first = replay(approved)
    assert first['contract_version'] == 2 and first['materialized_bodies'] == 1
    assert first['publisher_requests_this_command'] == 0 and not first['all_publisher_history_complete']
    after = head(queue, requested)
    assert after['state'] == 'complete' and after['body_sha256'] == manifest['entries'][0]['body_sha256']
    saved, = pq.ParquetFile(queue.root/after['parquet_path']).read().to_pylist()
    assert saved['raw_response_body'] == raw['raw_response_body']
    assert saved['requested_url'] == requested and saved['final_url'] == URL
    assert json.loads(saved['transport_redirects_json']) == json.loads(raw['transport_redirects_json'])
    assert saved['captured_at_utc'] == saved['source_observed_at_utc'] == raw['observed_at_utc']
    assert saved['http_duration_seconds'] is None and not saved['historical_point_in_time']
    assert saved['original_diagnostic_authorization_json'] is None
    assert saved['original_diagnostic_authorization_fingerprint'] == raw['authorization_fingerprint']
    assert saved['replay_source_parquet'] == ref['path']
    assert not list((queue.root/'reviewed_native_sources').glob('*.parquet'))
    assert queue.db.execute('SELECT 1 FROM articles WHERE url=?', (URL,)).fetchone() is None
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')] == old_jobs
    assert original_hashes == {key: sha256_file(queue.root/ref[key]) for key in original_hashes}
    assert replay(approved)['materialized_bodies'] == 0 and head(queue, requested) == after


@pytest.mark.parametrize('change', [
    {'state': 'http_403', 'error_code': 'http_403'}, {'state': 'http_429', 'error_code': 'http_429'},
    {'state': 'paywall_or_login', 'error_code': 'paywall_or_login'},
    {'due': time.time()+3600}, {'lease_owner': 'other', 'lease_until': time.time()+3600},
    {'last_checked_at_utc': '2026-10-21T23:59:59+00:00'}, {'attempts': 99},
])
def test_newer_or_refused_head_is_never_reset_or_overwritten(approved, change):
    queue, requested, *_ = approved
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?', (*change.values(), requested))
    before = head(queue, requested)
    assert replay(approved)['materialized_bodies'] == 0
    assert head(queue, requested) == before


@pytest.mark.parametrize('field,value', [
    ('diagnostic_path', '../escape.parquet'), ('response_sha256', '0'*64),
    ('diagnostic_receipt_sha256', '0'*64), ('candidate_job_sha256', '0'*64),
    ('article_fields_sha256', '0'*64), ('captured_at_utc', '2020-01-01T00:00:00+00:00'),
])
def test_entire_batch_preflight_fails_before_head_or_evidence_write(approved, field, value):
    queue, requested, path, manifest, *_ = approved
    manifest['entries'][0][field] = value; rewrite(path, manifest)
    before = head(queue, requested)
    with pytest.raises((ValueError, KeyError)):
        replay(approved)
    assert head(queue, requested) == before
    assert not (queue.root/'reviewed_native_sources').exists()


def test_actual_closed_candidate_cannot_be_changed_into_unknown_or_replayed(approved):
    queue, requested, path, manifest, *_ = approved
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",
            (manifest['entries'][0]['request_id'],))
    before = head(queue, requested)
    with pytest.raises(ValueError, match='completed candidate changed'):
        replay(approved)
    assert head(queue, requested) == before


def test_recovery_does_not_promote_a_durable_receipt_over_a_new_refusal(approved, monkeypatch):
    queue, requested, *_ = approved
    original = queue._accept_records
    def refusal(records, relative, receipt):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403',lease_owner=NULL,lease_until=NULL WHERE url=?", (requested,))
        return original(records, relative, receipt)
    monkeypatch.setattr(queue, '_accept_records', refusal)
    result = replay(approved)
    assert result['materialized_bodies'] == 0
    assert head(queue, requested)['state'] == 'http_403' and head(queue, requested)['body_sha256'] is None


def test_generic_exact_claim_cannot_bypass_terminal_failure_without_approval(approved):
    queue, requested, *_ = approved
    before = head(queue, requested)
    with pytest.raises(ValueError, match='exact missing-head claim fence'):
        queue.claim_url(requested, 300, expected_missing_head={k: before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS})
    assert head(queue, requested) == before


def test_receipt_or_raw_replacement_is_not_an_unvalidated_local_replay(approved):
    queue, requested, path, manifest, ref, *_ = approved
    original = queue.root/ref['path']
    original.write_bytes(original.read_bytes()+b'changed')
    before = head(queue, requested)
    with pytest.raises(ValueError, match='diagnostic changed'):
        replay(approved)
    assert head(queue, requested) == before and not (queue.root/'reviewed_native_sources').exists()


def test_batch_with_one_invalid_entry_never_claims_the_other_valid_head(approved):
    queue, requested, path, manifest, *_ = approved
    bad = deepcopy(manifest['entries'][0]); bad['url'] = requested.replace('1234567', '1234568')
    manifest['entries'].append(bad); rewrite(path, manifest)
    report_path = path.parent/manifest['observation_report']
    report = json.loads(report_path.read_bytes())
    extra = deepcopy(report['private_originals'][0]); extra['url'] = bad['url']
    report['private_originals'].append(extra); rewrite(report_path, report)
    manifest['observation_report_sha256'] = sha256_file(report_path); rewrite(path, manifest)
    before = head(queue, requested)
    with pytest.raises(ValueError):
        materialize_reviewed_native(queue, [requested, bad['url']], path, lease_seconds=300)
    assert head(queue, requested) == before and not (queue.root/'reviewed_native_sources').exists()


def test_durable_resident_receipt_recovers_only_its_own_owner_restart_witness(approved, monkeypatch):
    from downloader.news_fulltext_retained_native import reviewed_native_head_matches
    queue, requested, *_ = approved
    original = queue._accept_records
    def restarted(records, relative, receipt):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,error_code='owner_restart',lease_owner=NULL,lease_until=NULL WHERE url=?", (requested,))
        assert reviewed_native_head_matches(queue, records[0])
        return original(records, relative, receipt)
    monkeypatch.setattr(queue, '_accept_records', restarted)
    assert replay(approved)['materialized_bodies'] == 1
    assert head(queue, requested)['state'] == 'complete'


def test_racing_refusal_before_exact_claim_is_not_consumed(approved, monkeypatch):
    queue, requested, *_ = approved
    original = queue.claim_url
    def refusal(url, lease_seconds, **kwargs):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='http_429',error_code='http_429',due=? WHERE url=?", (time.time()+3600, requested))
        return original(url, lease_seconds, **kwargs)
    before = head(queue, requested)
    monkeypatch.setattr(queue, 'claim_url', refusal)
    assert replay(approved)['materialized_bodies'] == 0
    after = head(queue, requested)
    assert after['state'] == 'http_429' and after['attempts'] == before['attempts'] and after['body_sha256'] is None
