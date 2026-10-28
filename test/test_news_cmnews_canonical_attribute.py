"""Retain an unanswered URI/IRI declaration without replaying closed work."""
from dataclasses import replace
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_candidates as candidates
from downloader import news_fulltext_policy as policy
from downloader import news_fulltext_publishers as publishers
from downloader.news_fulltext_queue import NewsQueue
from test_news_candidate_diagnostics import http_stub
from test_news_cmnews_canonical_relation import (
    finished_identity, relation, saved_literal_witness, stored_job,
)
from test_news_cmnews_resident_diagnostics import settings, queue, head, pump, response, submit, LAYOUT
from test_news_cmnews_scope import URL, UUID, AUTHOR, TEXT, page

DECLARED = 'https://www.cmoney.tw/learn/台股 原文-' + UUID + '/'


def attribute(queue, settings, **options):
    return submit(queue, settings, evidence_contract_version=54, **options)


def closed_relation(queue, settings, *, declared=DECLARED):
    identity_id = finished_identity(queue, settings)
    submitted = relation(queue, settings); pump(queue)
    client = http_stub(queue, response=response(page(canonical=declared, schema_url=URL)))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews')
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job)); pump(queue)
    return identity_id, submitted['request_id']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('literal', [DECLARED, '/article/中文 原文-' + UUID, '//www.cmoney.tw/learn/中文/'])
def test_exact_native_attribute_is_preserved_without_encoding_or_acceptance(parser, literal):
    soup = BeautifulSoup(page(canonical=literal), parser)
    facts = publishers.cmnews_canonical_attribute_metadata(soup, page_url=URL)
    locator = facts['canonical_attribute']
    assert locator['literal'] == literal
    assert locator['literal_sha256'] == hashlib.sha256(literal.encode()).hexdigest()
    assert locator['contains_non_ASCII'] and locator['source_declaration_only']
    assert not locator['safe_to_request_assumed'] and facts['automatic_requests'] == 0
    assert not facts['source_permission_granted'] and not facts['article_or_alias_accepted']
    assert not facts['raw_or_names_or_body_retained']
    assert publishers.cmnews_canonical_relation_metadata(soup, page_url=URL)[
        'canonical_locators'] == [{'literal_locator_retained': False}]


@pytest.mark.parametrize('literal,reason', [
    ('https://key:secret@example.org/private', 'unsupported_HTTP_authority_shape'),
    ('https://example.org/private?token=secret', 'query_or_fragment_not_retained'),
    ('https://example.org/private#secret', 'query_or_fragment_not_retained'),
    ('javascript:secret', 'unsupported_HTTP_authority_shape'),
    ('https://example.org/\u202esecret', 'control_or_edge_whitespace'),
    (' https://example.org/private', 'control_or_edge_whitespace'),
    ('/article/' + 'x'*4096, 'missing_or_overlong_attribute'),
])
def test_sensitive_or_unbounded_declarations_are_not_exported(literal, reason):
    facts = publishers.cmnews_canonical_attribute_metadata(
        BeautifulSoup(page(canonical=literal), 'lxml'), page_url=URL)
    assert facts['canonical_attribute'] is None and facts['not_retained_reason'] == reason
    assert literal not in json.dumps(facts, ensure_ascii=False)


def test_ambiguous_or_wrong_native_scope_does_not_supply_an_attribute():
    soup = BeautifulSoup(page(canonical=DECLARED, container_id='other-container'), 'lxml')
    facts = publishers.cmnews_canonical_attribute_metadata(soup, page_url=URL)
    assert facts['canonical_attribute'] is None and not facts['native_scope_verified']


def test_first_unanswered_native_attribute_can_use_existing_owner(queue, settings, monkeypatch):
    before = head(queue); charges = list(queue.db.execute('SELECT * FROM queue_settings'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('no new HTTP owner'))
    request = attribute(queue, settings); pump(queue)
    client = http_stub(queue, response=response(page(canonical=DECLARED)))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews')
    assert job is not None
    result = jobs.fetch_candidate(client, job)
    jobs.finish_candidate(queue, job, result=result); pump(queue)
    assert client.calls == [URL]
    assert result['requested_url'] == result['final_url'] == URL
    assert result['question'] == publishers.CMNEWS_CANONICAL_ATTRIBUTE_QUESTION
    assert result['canonical_attribute']['canonical_attribute']['literal'] == DECLARED
    assert not result['accepted_as_article'] and not result['body_completeness_verified']
    assert not result['identity_or_alias_accepted'] and not result['raw_or_names_or_body_retained']
    assert AUTHOR not in json.dumps(result, ensure_ascii=False) and TEXT not in json.dumps(result, ensure_ascii=False)
    assert head(queue) == before and list(queue.db.execute('SELECT * FROM queue_settings')) == charges
    assert not (queue.root/'bodies').exists()
    assert not (queue.root/'reviewed_repairs/candidate_diagnostics/originals').exists()
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert not attribute(queue, settings, reason='Reason changes cannot rearm the same question')['submitted']


def test_closed_unretained_ascii_observation_is_preserved(queue, settings):
    old_ids = closed_relation(queue, settings)
    old = {key: stored_job(queue, key) for key in old_ids}
    before = head(queue)
    submitted = attribute(queue, settings); pump(queue)
    client = http_stub(queue, response=response(page(canonical=DECLARED)))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews')
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job)); pump(queue)
    assert old == {key: stored_job(queue, key) for key in old_ids} and head(queue) == before
    assert not relation(queue, settings)['submitted']
    assert not submit(queue, settings)['submitted']
    assert jobs.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'


def test_retained_closed_locator_prevents_new_attribute_get(queue, settings):
    closed_relation(queue, settings, declared='https://www.cmoney.tw/learn/original/')
    with pytest.raises(ValueError, match='prior_unretained_cmnews_canonical_required'):
        attribute(queue, settings)


def test_reuses_normal_failure_literal_before_new_diagnostic(queue, settings):
    saved_literal_witness(queue)
    with pytest.raises(ValueError, match='cmnews_literal_canonical_already_recorded'):
        attribute(queue, settings)


@pytest.mark.parametrize('change', ['request', 'head', 'clock', 'state', 'scope', 'retained', 'native_shape', 'relation_shape'])
def test_prior_result_must_be_exact_closed_unretained_native_witness(queue, settings, change):
    _, prior = closed_relation(queue, settings)
    old = stored_job(queue, prior); request = json.loads(old['request_json']); result = json.loads(old['result_json'])
    with queue.db:
        if change == 'request':
            request['source']['authorization_fingerprint'] = 'a'*64
        elif change == 'head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
        elif change == 'clock':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?", (prior,))
        elif change == 'state':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (prior,))
        elif change == 'scope':
            result['native_identity']['native_UUID_container_matches'] = False
        elif change == 'native_shape':
            result['native_identity'] = None
        elif change == 'relation_shape':
            result['canonical_relation'] = []
        else:
            result['canonical_relation']['canonical_locators'] = [{'literal': DECLARED}]
        queue.db.execute('UPDATE native_candidate_jobs SET request_json=?,result_json=? WHERE request_id=?',
            (json.dumps(request, ensure_ascii=False), json.dumps(result, ensure_ascii=False), prior))
    with pytest.raises(ValueError, match='prior_unretained_cmnews_canonical_required'):
        attribute(queue, settings)


def test_normal_receipt_arriving_after_submission_is_rechecked_before_http(queue, settings):
    request = attribute(queue, settings); pump(queue); saved_literal_witness(queue)
    client = http_stub(queue, response=response())
    assert jobs.claim_candidate(queue, client, settings, 'cmnews') is None and not client.calls
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'skipped'


def test_pending_old_observation_blocks_submission(queue, settings):
    submit(queue, settings); pump(queue)
    with pytest.raises(ValueError, match='prior_observation_not_terminal'):
        attribute(queue, settings)


def test_distinct_question_is_source_bound_and_once_key_ignores_reason_or_parser(queue):
    source = queue.registry.sources['cmnews']; target = jobs._head(head(queue))
    assert candidates.candidate_evidence_allowed(source, source, 54)
    assert not candidates.candidate_evidence_allowed(source, replace(source, authorization={'kind': 'other'}), 54)
    assert jobs._key(target, 'a'*64, evidence_contract_version=54) != jobs._key(target, 'a'*64, evidence_contract_version=19)
    assert jobs._key(target, 'a'*64, evidence_contract_version=54) == jobs._key(
        dict(target, attempts=9, due=100), 'b'*64, evidence_contract_version=54)
    assert jobs._failure_states(LAYOUT, 54) == {'article_identity_not_verified'}


def test_one_public_parse_does_not_replay_old_identity_or_locator_question(queue, monkeypatch):
    source = queue.registry.sources['cmnews']; calls = []; original = policy.public_article_document
    def parse(*args, **kwargs):
        calls.append(1); return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'public_article_document', parse)
    monkeypatch.setattr(publishers, 'cmnews_visible_article', lambda *a, **k: pytest.fail('closed identity'))
    monkeypatch.setattr(publishers, 'cmnews_canonical_relation_metadata', lambda *a, **k: pytest.fail('closed ASCII locator'))
    result = candidates.candidate_response_metadata(response(page(canonical=DECLARED)), source, source,
        requested_url=URL, evidence_contract_version=54)
    assert calls == [1] and result['canonical_attribute']['native_scope_verified']


def test_source_cooldown_blocks_dispatch(queue, settings):
    attribute(queue, settings); pump(queue); client = http_stub(queue, response=response())
    client.provider_not_before['cmnews'] = jobs.time.time()+600
    assert jobs.claim_candidate(queue, client, settings, 'cmnews') is None and not client.calls


def test_unknown_reservation_survives_restart_without_repeating_get(queue, settings):
    submitted = attribute(queue, settings); pump(queue); client = http_stub(queue, response=response())
    assert jobs.claim_candidate(queue, client, settings, 'cmnews')
    root, registry = queue.root, queue.registry; queue.close()
    replacement = NewsQueue(root, registry)
    try:
        pump(replacement)
        assert not attribute(replacement, settings)['submitted']
        assert jobs.candidate_status(root, submitted['request_id'])['state'] == 'uncertain_outcome'
        assert jobs.claim_candidate(replacement, client, settings, 'cmnews') is None and not client.calls
    finally:
        replacement.close()
