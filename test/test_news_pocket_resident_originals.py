"""Finite resident witnesses: no duplicate HTTP, no body/head admission."""
from dataclasses import replace
import hashlib
import json
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal, private_original_permit
from downloader.news_fulltext_candidates import native_candidate_source, candidate_evidence_allowed
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_pocket_original import settings, page, versioned, URL
from test_news_pocket_resident_diagnostics import http, head


LAYOUT = 'pocket_visible_article_v5'


@pytest.fixture
def config(settings, tmp_path):
    result = versioned(settings, 5)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, workers=2, exclusive_http_owner=False)
    result['adaptive_rate'] = {'enabled': False}
    return result


@pytest.fixture
def queue(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    queue.add_articles([{'url': URL}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified',attempts=2,due=0,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (URL,))
    yield queue
    queue.close()


def submit(queue, config):
    return diagnostic.submit_candidate(queue.root, config, queue.registry, URL, LAYOUT,
        reason='One private current native failure witness; never body admission', evidence_contract_version=4)


def claim(queue, config, client):
    submitted = submit(queue, config)
    # These tests exercise retention, not the resident's wall-time budget.
    # Schema/fsync scheduling can legitimately exhaust the first 50 ms tick.
    with patch.object(diagnostic.time, 'monotonic', return_value=0.0):
        diagnostic.pump_candidates(queue)
    return submitted, diagnostic.claim_candidate(queue, client, config, 'pocket')


def test_budget_exhaustion_defers_same_request_until_next_tick(config, queue):
    before, client = head(queue), http(queue, body=page().encode())
    submitted = submit(queue, config)
    with patch.object(diagnostic.time, 'monotonic', side_effect=[0.0, 0.1]):
        diagnostic.pump_candidates(queue)
    assert diagnostic.has_unadmitted_candidates(queue)
    assert diagnostic.claim_candidate(queue, client, config, 'pocket') is None
    assert head(queue) == before and client.calls == []
    with patch.object(diagnostic.time, 'monotonic', return_value=0.0):
        diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, client, config, 'pocket')
    assert job['request_id'] == submitted['request_id']
    result = diagnostic.fetch_candidate(client, job)
    diagnostic.finish_candidate(queue, job, result=result)
    assert not diagnostic.candidate_status(queue.root, submitted['request_id'])['raw_or_names_or_body_retained']
    assert head(queue) == before and client.calls == [URL]
    assert not submit(queue, config)['submitted']


def test_owner_retains_exact_private_original_and_receipt_without_accepting_body(config, queue):
    before, client = head(queue), http(queue)
    submitted, job = claim(queue, config, client)
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['raw_retention_state'] == 'unknown'
    observation = diagnostic.fetch_candidate(client, job)
    assert isinstance(observation, PendingOriginal)
    diagnostic.finish_candidate(queue, job, result=observation)
    diagnostic.pump_candidates(queue)
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed' and state['raw_or_names_or_body_retained'] is True
    assert state['raw_retention_state'] == 'retained_verified' and not state['accepted_as_article']
    result = state['result_json']
    assert result['current_error'] == 'article_identity_not_verified'
    reference = result['private_original']
    path = queue.root/reference['path']
    receipt = json.loads((queue.root/reference['receipt_path']).read_bytes())
    restored = pq.ParquetFile(path).read().to_pylist()
    assert len(restored) == 1 and restored[0]['raw_response_body'] == observation.response.body
    assert reference['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert receipt['permit'] == observation.permit and receipt['original'] == reference
    assert restored[0]['diagnostic_only'] and not restored[0]['publish']
    assert not restored[0]['historical_point_in_time'] and not restored[0]['training_injection_enabled']
    assert not path.stat().st_mode & 0o077 and not path.parent.stat().st_mode & 0o077
    archive = path.parent.parent/'processed'/(job['request_id']+'.outcome.json')
    assert json.loads(archive.read_bytes())['raw_or_names_or_body_retained'] is True
    assert head(queue) == before and client.calls == [URL] and not (queue.root/'bodies').exists()
    assert not submit(queue, config)['submitted']
    assert diagnostic.claim_candidate(queue, client, config, 'pocket') is None


@pytest.mark.parametrize('old,new', [
    ('"isAccessibleForFree": true', '"isAccessibleForFree": false'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": null'),
    ('口袋學堂編輯團隊</span>', '外部供稿</span>'),
    ('"name": "口袋學堂編輯團隊"', '"name": "外部供稿"'),
    ('"name": "口袋學堂"', '"name": "其他出版商"'),
    ('合成資料的原生研究</h1>', '不一致標題</h1>'),
    ('rel="canonical"', 'rel="alternate"'),
    ('第0段', '來源：外部 第0段'),
    ('class="cms-article-scope"', 'class="unknown-body"'),
    ('class="cms-article-scope"', 'class="cms-article-scope" hidden'),
    ('"@type": "Article"', '"@type": "Article", "articleBody": "other body"'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": true, "isAccessibleForFree": true'),
])
def test_independent_retention_gate_rejects_unverified_identity_rights_or_scope(queue, old, new):
    raw = page().replace(old, new)
    assert raw != page()
    with pytest.raises(NewsError):
        private_original_permit(Response(URL, 200, {'content-type':'text/html'}, raw.encode()),
                                queue.registry.sources['pocket'])


@pytest.mark.parametrize('change', ['missing_free_declaration', 'extended_organization', 'literal_true'])
def test_public_native_scope_does_not_invent_a_free_declaration_or_reject_profile_metadata(queue, change):
    raw = page()
    if change == 'missing_free_declaration':
        raw = raw.replace(', "isAccessibleForFree": true', '')
    elif change == 'literal_true':
        raw = raw.replace('"isAccessibleForFree": true', '"isAccessibleForFree": "true"')
    else:
        raw = raw.replace('"author": {"@type": "Organization",',
            '"author": {"@type": "Organization", "description": "own profile", "knowsAbout": ["education"],')
    assert raw != page()
    permit = private_original_permit(Response(URL,200,{'content-type':'text/html'},raw.encode()),
                                    queue.registry.sources['pocket'])
    assert permit['scope']=='reviewed_current_public_native_own_organization_private_original_only'
    assert 'isAccessibleForFree' not in permit


@pytest.mark.parametrize('change', ['source', 'auth', 'version', 'provider'])
def test_source_binding_and_fixed_current_layout(config, queue, change):
    source = queue.registry.sources['pocket']
    candidate = native_candidate_source(config, queue.registry, queue.registry.base, LAYOUT)
    assert source is candidate and candidate_evidence_allowed(source, candidate, 4)
    if change == 'source':
        candidate = replace(candidate, data_fingerprint='a'*64)
    elif change == 'auth':
        candidate = replace(candidate, authorization_fingerprint='b'*64)
    elif change == 'version':
        candidate = replace(candidate, embedded_article_layout='pocket_visible_article_v4')
    else:
        candidate = replace(candidate, provider='another')
    assert not candidate_evidence_allowed(source, candidate, 4)


def test_same_question_key_survives_parser_promotions_and_changing_failure_counters(queue):
    target = diagnostic._head(queue.db.execute('SELECT * FROM articles').fetchone())
    assert diagnostic._key(target, 'a'*64, evidence_contract_version=4) == diagnostic._key(
        dict(target, attempts=20, due=1), 'b'*64, evidence_contract_version=4)
    assert diagnostic._key(target, 'a'*64, evidence_contract_version=3) != diagnostic._key(
        target, 'b'*64, evidence_contract_version=3)


def test_write_failure_is_terminal_truthful_and_does_not_kill_the_owner(config, queue, monkeypatch):
    before, client = head(queue), http(queue)
    submitted, job = claim(queue, config, client)
    observation = diagnostic.fetch_candidate(client, job)
    def fail(*args):
        raise OSError('private path must not leak to status')
    monkeypatch.setattr(diagnostic, 'retain_original', fail)
    diagnostic.finish_candidate(queue, job, result=observation)
    diagnostic.pump_candidates(queue)
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed_error' and state['raw_retention_state'] == 'unknown'
    assert state['raw_or_names_or_body_retained'] is None
    assert state['result_json']['error'] == 'worker_OSError' and 'private path' not in json.dumps(state)
    assert head(queue) == before and client.calls == [URL] and not submit(queue, config)['submitted']
    assert queue.db.execute('SELECT 1').fetchone()[0] == 1


def test_plain_result_cannot_forge_raw_retention_permission(config, queue):
    client = http(queue)
    submitted, job = claim(queue, config, client)
    diagnostic.finish_candidate(queue, job, result={'accepted_as_article':False,
        'raw_or_names_or_body_retained':True, 'private_original':{'path':'fake'}})
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed_error' and not state['raw_or_names_or_body_retained']
    assert state['result_json']['error'] == 'invalid_candidate_result' and client.calls == []


def test_no_raw_if_current_parser_already_succeeds(config, queue):
    before, client = head(queue), http(queue, body=page().encode())
    submitted, job = claim(queue, config, client)
    result = diagnostic.fetch_candidate(client, job)
    assert isinstance(result, dict) and result['current_error'] is None
    diagnostic.finish_candidate(queue, job, result=result)
    assert not diagnostic.candidate_status(queue.root, submitted['request_id'])['raw_or_names_or_body_retained']
    assert head(queue) == before and client.calls == [URL]
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()


def test_unknown_outcome_is_not_replayed_or_misrepresented_as_no_retention(config, queue):
    submitted, job = claim(queue, config, http(queue))
    assert job
    queue.close()
    restarted = NewsQueue(queue.root, queue.registry)
    try:
        diagnostic.pump_candidates(restarted)
        state = diagnostic.candidate_status(restarted.root, submitted['request_id'])
        assert state['state'] == 'uncertain_outcome' and state['raw_retention_state'] == 'unknown'
        assert state['raw_or_names_or_body_retained'] is None
        assert diagnostic.claim_candidate(restarted, http(restarted), config, 'pocket') is None
        assert not submit(restarted, config)['submitted']
    finally:
        restarted.close()


def test_owner_rejects_stale_completion_before_any_raw_write(config, queue):
    client = http(queue)
    submitted, job = claim(queue, config, client)
    observation = diagnostic.fetch_candidate(client, job)
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET owner_id='other' WHERE request_id=?",
                         (submitted['request_id'],))
    with pytest.raises(ValueError, match='candidate_completion_owner_mismatch'):
        diagnostic.finish_candidate(queue, job, result=observation)
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()
