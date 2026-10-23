"""One literal-canonical question; completed identity evidence stays closed."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_candidates as candidates
from downloader import news_fulltext_policy as policy
from downloader import news_fulltext_publishers as publishers
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from test_news_candidate_diagnostics import http_stub
from test_news_cmnews_resident_diagnostics import settings, queue, head, pump, response, submit, LAYOUT
from test_news_cmnews_scope import URL, UUID, AUTHOR, TEXT, page

DECLARED = 'https://cmnews.com.tw/article/another-author-' + UUID


def finished_identity(queue, settings):
    submitted = submit(queue, settings); pump(queue)
    client = http_stub(queue, response=response(page(canonical=DECLARED, schema_url=URL)))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews')
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job)); pump(queue)
    return submitted['request_id']


def relation(queue, settings, **options):
    return submit(queue, settings, evidence_contract_version=19, **options)


def stored_job(queue, request_id):
    return dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (request_id,)).fetchone())


def saved_literal_witness(queue, **changes):
    from downloader.artifact_io import atomic_write_json
    source = queue.registry.sources['cmnews']; target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    value = {'contract_version': 1, 'provider': 'cmnews', 'data_fingerprint': source.data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint, 'request_url': URL, 'final_url': URL,
        'request_url_sha256': target['id'], 'final_url_sha256': target['id'], 'http_status': 200,
        'error_code': 'article_identity_not_verified', 'observed_at_utc': '2025-12-31T23:59:59+00:00',
        'diagnostic_only': True, 'raw_response_retained': False, 'accepted_as_article': False,
        'facts': {'canonical': {'node_count': 1, 'truncated': False, 'values': [
            {'display_url': DECLARED, 'original_value_sha256': hashlib.sha256(DECLARED.encode()).hexdigest()}]}}}
    value.update(changes)
    path = queue.root/'identity_metadata/20251231/cmnews/20251231T235959000000Z-01234567.json'
    path.parent.mkdir(parents=True, mode=0o700)
    atomic_write_json(path, value, durable=True); path.chmod(0o600)
    return path


def test_new_question_is_separate_fixed_and_provider_bound(queue, settings):
    finished_identity(queue, settings)
    source = queue.registry.sources['cmnews']; target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    assert candidates.candidate_evidence_allowed(source, source, 19)
    assert not candidates.candidate_evidence_allowed(replace(source, provider='other'), replace(source, provider='other'), 19)
    assert not candidates.candidate_evidence_allowed(source, replace(source, data_fingerprint='a'*64), 19)
    assert jobs._failure_states(LAYOUT, 19) == {'article_identity_not_verified'}
    assert jobs._failure_states('watchmedia_visible_article_v3', 19) == set()
    key = jobs._key(target, 'a'*64, evidence_contract_version=19)
    assert key != jobs._key(target, 'a'*64)
    assert key == jobs._key(dict(target, attempts=90, due=100), 'b'*64, evidence_contract_version=19)
    assert relation(queue, settings)['submitted']


def test_no_prior_witness_cannot_buy_another_get(queue, settings):
    with pytest.raises(ValueError, match='prior_completed_cmnews_canonical_mismatch_required'):
        relation(queue, settings)
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/pending').exists()


def test_completed_literal_evidence_from_original_failure_prevents_duplicate_question(queue, settings):
    finished_identity(queue, settings); witness = saved_literal_witness(queue)
    target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    result = jobs.recorded_cmnews_canonical(queue.db, queue.registry.sources['cmnews'], target)
    assert result['canonical_url'] == DECLARED and result['metadata_sha256'] == hashlib.sha256(witness.read_bytes()).hexdigest()
    before = witness.read_bytes()
    with pytest.raises(ValueError, match='cmnews_literal_canonical_already_recorded'):
        relation(queue, settings)
    assert witness.read_bytes() == before and not result['article_or_alias_accepted']


@pytest.mark.parametrize('changes', [
    {'data_fingerprint': 'a'*64}, {'authorization_fingerprint': 'b'*64}, {'request_url': DECLARED},
    {'final_url': DECLARED}, {'request_url_sha256': 'a'*64}, {'diagnostic_only': False},
    {'raw_response_retained': True}, {'accepted_as_article': True}, {'http_status': 404},
    {'observed_at_utc': '2026-01-02T00:00:00+00:00'},
])
def test_unattested_or_different_saved_metadata_is_not_a_completed_literal(queue, settings, changes):
    finished_identity(queue, settings); saved_literal_witness(queue, **changes)
    assert relation(queue, settings)['submitted']


@pytest.mark.parametrize('change', ['truncated', 'hash', 'multiple', 'query_redacted'])
def test_truncated_redacted_or_conflicting_prior_value_is_still_unanswered(queue, settings, change):
    finished_identity(queue, settings); path = saved_literal_witness(queue); value = json.loads(path.read_bytes())
    canonical = value['facts']['canonical']
    if change == 'truncated':
        canonical['truncated'] = True
    elif change == 'hash':
        canonical['values'][0]['original_value_sha256'] = 'a'*64
    elif change == 'multiple':
        canonical['node_count'] = 2
    else:
        canonical['values'][0]['display_url'] += '?redacted'
    from downloader.artifact_io import atomic_write_json
    atomic_write_json(path, value, durable=True); path.chmod(0o600)
    assert relation(queue, settings)['submitted']


def test_new_claim_rechecks_if_normal_metadata_answer_arrived_after_submission(queue, settings):
    finished_identity(queue, settings); request = relation(queue, settings); pump(queue)
    saved_literal_witness(queue)
    client = http_stub(queue, response=response())
    assert jobs.claim_candidate(queue, client, settings, 'cmnews') is None and not client.calls
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'skipped'


@pytest.mark.parametrize('state', ['ready', 'reserved', 'uncertain_outcome', 'observed_error', 'skipped'])
def test_unfinished_or_failed_prior_is_not_a_canonical_witness(queue, settings, state):
    prior = finished_identity(queue, settings)
    with queue.db:
        queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?', (state, prior))
    with pytest.raises(ValueError, match='prior_completed_cmnews_canonical_mismatch_required'):
        relation(queue, settings)


@pytest.mark.parametrize('field,value', [
    ('canonical_count', 2), ('matching_canonical_count', 1), ('matching_native_scope_count', 0),
    ('native_slug_matches', False), ('native_UUID_container_matches', False),
    ('native_title_count', 2), ('nonempty_native_title_count', 0), ('native_body_container_count', 0),
])
def test_only_exact_completed_native_mismatch_witness_is_eligible(queue, settings, field, value):
    prior = finished_identity(queue, settings)
    result = json.loads(stored_job(queue, prior)['result_json']); result['native_identity'][field] = value
    with queue.db:
        queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?', (json.dumps(result), prior))
    with pytest.raises(ValueError, match='prior_completed_cmnews_canonical_mismatch_required'):
        relation(queue, settings)


@pytest.mark.parametrize('change', ['head', 'source', 'question', 'old_version'])
def test_changed_head_source_or_different_question_is_not_permission(queue, settings, change):
    prior = finished_identity(queue, settings); old = stored_job(queue, prior)
    request = json.loads(old['request_json']); result = json.loads(old['result_json'])
    with queue.db:
        if change == 'head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
        elif change == 'source':
            request['source']['data_fingerprint'] = 'a'*64
        elif change == 'question':
            result['question'] = 'unreviewed_question'
        else:
            request['evidence_contract_version'] = 19
        queue.db.execute('UPDATE native_candidate_jobs SET request_json=?,result_json=? WHERE request_id=?',
            (json.dumps(request), json.dumps(result), prior))
    with pytest.raises(ValueError, match='prior_completed_cmnews_canonical_mismatch_required'):
        relation(queue, settings)


def test_same_owner_records_literal_urls_not_bodies_or_names_and_keeps_old_question(queue, settings, monkeypatch):
    prior = finished_identity(queue, settings); old = stored_job(queue, prior); before = head(queue)
    settings_before = list(queue.db.execute('SELECT * FROM queue_settings'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no second HTTP owner'))
    submitted = relation(queue, settings); pump(queue)
    client = http_stub(queue, response=response(page(canonical=DECLARED, schema_url=URL)))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews'); result = jobs.fetch_candidate(client, job)
    jobs.finish_candidate(queue, job, result=result); pump(queue)
    assert client.calls == [URL]
    assert result['question'] == publishers.CMNEWS_CANONICAL_RELATION_QUESTION
    facts = result['canonical_relation']
    assert facts['canonical_locators'][0]['literal'] == DECLARED
    assert facts['canonical_locators'][0]['article_UUID'] == UUID
    assert facts['requested']['absolute'] == URL and facts['native_UUID_matches_requested']
    assert facts['matching_headline_schema'][0]['identity_locator']['absolute'] == URL
    assert facts['byline_profile_locators'][0]['literal'] == '/forumnews'
    assert not result['accepted_as_article'] and not result['identity_or_alias_accepted']
    assert not facts['article_or_alias_accepted'] and not result['body_completeness_verified']
    assert head(queue) == before and stored_job(queue, prior) == old
    assert list(queue.db.execute('SELECT * FROM queue_settings')) == settings_before
    assert AUTHOR not in json.dumps(result, ensure_ascii=False) and TEXT not in json.dumps(result, ensure_ascii=False)
    assert not (queue.root/'bodies').exists() and not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()
    assert not submit(queue, settings, reason='Closed identity question stays closed')['submitted']
    assert not relation(queue, settings, reason='Changing a reason cannot rearm canonical relation')['submitted']
    assert jobs.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'


def test_new_relation_parses_once_and_does_not_repeat_the_old_validator(queue, monkeypatch):
    source = queue.registry.sources['cmnews']; calls = []; original = policy.public_article_document
    def document(*args, **kwargs):
        calls.append(1); return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'public_article_document', document)
    monkeypatch.setattr(publishers, 'cmnews_visible_article', lambda *a, **k: pytest.fail('identity-v1 already answered'))
    result = candidates.candidate_response_metadata(response(page(canonical=DECLARED)), source, source, evidence_contract_version=19)
    assert len(calls) == 1 and 'current_native_error' not in result
    assert result['canonical_relation']['canonical_count'] == 1


def test_new_question_never_bypasses_public_access_challenge(queue):
    source = queue.registry.sources['cmnews']
    result = candidates.candidate_response_metadata(response(b'<title>Verify you are human</title><p>private challenge</p>'),
        source, source, evidence_contract_version=19)
    assert result['current_native_error'] == 'access_challenge' and 'canonical_relation' not in result
    assert not result['raw_or_names_or_body_retained'] and not result['accepted_as_article']


def test_public_paid_metadata_is_not_fulltext_permission(queue):
    source = queue.registry.sources['cmnews']
    result = candidates.candidate_response_metadata(response(page(paid=True)), source, source, evidence_contract_version=19)
    assert result['canonical_relation']['matching_headline_schema'][0]['explicitly_paid'] is True
    assert not result['raw_or_names_or_body_retained'] and not result['accepted_as_article']
    assert not result['body_completeness_verified'] and not result['identity_or_alias_accepted']


@pytest.mark.parametrize('declared', [
    'https://secret:password@cmnews.com.tw/article/else-' + UUID,
    'https://cmnews.com.tw/article/else-' + UUID + '?token=private',
    'javascript:private', '/article/' + 'a'*2100,
])
def test_unreviewed_or_sensitive_locators_are_not_exported(queue, declared):
    source = queue.registry.sources['cmnews']
    result = candidates.candidate_response_metadata(response(page(canonical=declared)), source, source, evidence_contract_version=19)
    assert result['canonical_relation']['canonical_locators'] == [{'literal_locator_retained': False}]
    assert declared not in json.dumps(result) and not result['identity_or_alias_accepted']


def test_declared_different_uuid_is_preserved_as_conflict_not_accepted(queue):
    source = queue.registry.sources['cmnews']; other = DECLARED.replace(UUID, '00000000-0000-0000-0000-000000000000')
    result = candidates.candidate_response_metadata(response(page(canonical=other)), source, source, evidence_contract_version=19)
    assert result['canonical_relation']['canonical_locators'][0]['article_UUID'] != UUID
    assert not result['identity_or_alias_accepted']


def test_literal_foreign_canonical_is_metadata_not_permission_to_follow_or_capture(queue):
    source = queue.registry.sources['cmnews']; other = 'https://other.example/article/else-' + UUID
    result = candidates.candidate_response_metadata(response(page(canonical=other)), source, source, evidence_contract_version=19)
    assert result['canonical_relation']['canonical_locators'][0]['literal'] == other
    assert not result['canonical_relation']['canonical_locators'][0]['same_publisher_host']
    assert not result['identity_or_alias_accepted'] and not result['raw_or_names_or_body_retained']


@pytest.mark.parametrize('declared', ['https://cmnews.com.tw', '/', 'https://cmnews.com.tw/category/news'])
def test_declared_nonarticle_target_is_preserved_not_turned_into_an_article(queue, declared):
    source = queue.registry.sources['cmnews']
    result = candidates.candidate_response_metadata(response(page(canonical=declared)), source, source, evidence_contract_version=19)
    locator = result['canonical_relation']['canonical_locators'][0]
    assert locator['literal'] == declared and locator['article_UUID'] is None
    assert not result['identity_or_alias_accepted'] and not result['accepted_as_article']


def test_actual_already_retained_percent_encoded_original_does_not_hit_artificial_short_path_cap(queue):
    from downloader.news_fulltext_cmoneylearning import CMONEY_LEARNING_ORIGIN, CMONEY_LEARNING_SUPPLEMENT_PATHS
    declared = CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_SUPPLEMENT_PATHS[1]
    assert len(CMONEY_LEARNING_SUPPLEMENT_PATHS[1]) > 180
    source = queue.registry.sources['cmnews']
    result = candidates.candidate_response_metadata(response(page(canonical=declared)), source, source, evidence_contract_version=19)
    assert result['canonical_relation']['canonical_locators'][0]['literal'] == declared
    assert not result['identity_or_alias_accepted']


def test_claim_rechecks_exact_prior_witness_before_http(queue, settings):
    prior = finished_identity(queue, settings); request = relation(queue, settings); pump(queue)
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (prior,))
    client = http_stub(queue, response=response())
    assert jobs.claim_candidate(queue, client, settings, 'cmnews') is None and not client.calls
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'skipped'


def test_unknown_new_reservation_is_not_retried_on_restart(queue, settings):
    prior = finished_identity(queue, settings); old = stored_job(queue, prior)
    request = relation(queue, settings); pump(queue); client = http_stub(queue, response=response())
    assert jobs.claim_candidate(queue, client, settings, 'cmnews')
    root, registry = queue.root, queue.registry; queue.close()
    with closing(NewsQueue(root, registry)) as replacement:
        pump(replacement)
        assert stored_job(replacement, prior) == old
        assert jobs.candidate_status(root, request['request_id'])['state'] == 'uncertain_outcome'
        assert not relation(replacement, settings)['submitted']
        assert jobs.claim_candidate(replacement, client, settings, 'cmnews') is None and not client.calls


def test_new_relation_still_requires_literal_transport(queue, settings):
    finished_identity(queue, settings); relation(queue, settings); pump(queue)
    client = http_stub(queue, response=response(url=DECLARED))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews')
    with pytest.raises(NewsError, match='candidate_literal_response_not_verified'):
        jobs.fetch_candidate(client, job)
    assert client.calls == [URL]
