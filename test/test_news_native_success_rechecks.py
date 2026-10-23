"""Successful current source observations recover once, without parser resets."""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_native_rechecks as witness
from downloader import news_fulltext_repairs as repairs
from downloader.news_fulltext_http import Response
from test_news_candidate_diagnostics import http_stub
from test_news_fulltext import config
from test_news_upmedia_native import URL, single_byline_page
from test_news_upmedia_resident_diagnostics import current_queue, head, pump
from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V2


@pytest.fixture
def prepared(config, tmp_path):
    with closing(current_queue(config, tmp_path)) as queue:
        first = repairs.submit_repairs(queue.root, queue.registry, [URL], reason='First current parser repair')
        assert repairs.consume_repairs(queue)['outcomes'] == {'enqueued': 1}
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
                "error_code='article_identity_not_verified',attempts=2,last_checked_at_utc='2026-01-02T00:00:00+00:00'")
        observation = jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V2,
            reason='Current unresolved native boundary', evidence_contract_version=20)
        pump(queue)
        client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(single_byline_page()).encode()))
        job = jobs.claim_candidate(queue, client, config, 'upmedia')
        result = jobs.fetch_candidate(client, job)
        assert result['current_native_error'] is None
        jobs.finish_candidate(queue, job, result=result)
        value = witness.reference(queue.root, observation['request_id'])
        yield queue, value, first


def submit(queue, value):
    return repairs.submit_repairs(queue.root, queue.registry, [URL],
        reason='A new successful source witness, not a changed parser or blanket retry', native_success_witnesses={URL: value})


def test_actual_native_success_admits_only_state_once_and_keeps_old_marker_and_evidence(prepared):
    queue, value, first = prepared
    before = head(queue)
    old_markers = [dict(r) for r in queue.db.execute('SELECT * FROM reviewed_repair_items')]
    old_questions = [dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    ordinary = repairs.submit_repairs(queue.root, queue.registry, [URL], reason='Ordinary duplicate stays denied')
    assert ordinary['excluded'] == [{'url': URL, 'reason': 'already_admitted_for_parser'}]
    request = submit(queue, value)
    assert request['contract_version'] == witness.VERSION
    assert request['submitted'] and request['submitted_targets'] == 1 and not request['queue_mutations'], request
    assert head(queue) == before
    loaded = repairs._load(Path(request['request_path']))
    assert loaded['contract_version'] == witness.VERSION and loaded['kind'] == witness.KIND
    assert repairs.consume_repairs(queue)['outcomes'] == {witness.OUTCOME: 1}
    assert head(queue) == dict(before, state='retry')
    assert [dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')] == old_questions
    assert [dict(r) for r in queue.db.execute('SELECT * FROM reviewed_repair_items WHERE request_id=?', (first['request_id'],))] == old_markers
    receipt = json.loads((queue.root / 'reviewed_repairs/processed' / (request['request_id'] + '.outcome.json')).read_bytes())
    assert receipt['admitted_targets'] == 1 and receipt['body_acceptances'] == 0
    assert receipt['previous_parser_repair_marker_preserved'] and receipt['full_body_still_requires_normal_capture']
    assert not (queue.root / 'bodies').exists()
    with queue.db: queue.db.execute("UPDATE articles SET state='article_identity_not_verified'")
    assert submit(queue, value)['excluded'] == [{'url': URL, 'reason': 'already_admitted_for_parser'}]
    queue.reviewed_repair_schema_ready = False
    repairs._schema(queue)
    assert not submit(queue, value)['submitted']


@pytest.mark.parametrize('change', ['failed', 'uncertain', 'reserved', 'wrong_url', 'redirect', 'status', 'native_error',
    'missing_error', 'raw_retained', 'body_accepted', 'wrong_question', 'wrong_source', 'changed_head', 'hash',
    'future_clock', 'before_failure', 'missing_prior_marker', 'malformed_json', 'no_provider', 'empty_response'])
def test_invalid_or_stale_witness_does_not_admit_or_write_a_new_request(prepared, change):
    queue, value, _ = prepared
    row = queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone()
    facts = json.loads(row['result_json'])
    request = json.loads(row['request_json'])
    if change in {'failed', 'uncertain', 'reserved'}:
        with queue.db: queue.db.execute('UPDATE native_candidate_jobs SET state=?', ({'failed':'observed_error', 'uncertain':'uncertain_outcome', 'reserved':'reserved'}[change],))
    elif change == 'wrong_url': facts['final_url'] = URL + '1'
    elif change == 'redirect': facts['redirect_count'] = 1
    elif change == 'status': facts['status'] = 403
    elif change == 'native_error': facts['current_native_error'] = 'article_attribution_not_verified'
    elif change == 'missing_error': del facts['current_native_error']
    elif change == 'raw_retained': facts['raw_or_names_or_body_retained'] = True
    elif change == 'body_accepted': facts['accepted_as_article'] = True
    elif change == 'wrong_question': facts['question'] = 'new-reason-is-not-proof'
    elif change == 'wrong_source': request['source']['data_fingerprint'] = 'f' * 64
    elif change == 'changed_head':
        with queue.db: queue.db.execute('UPDATE articles SET attempts=3')
    elif change == 'future_clock':
        with queue.db: queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00'")
    elif change == 'before_failure':
        with queue.db: queue.db.execute("UPDATE native_candidate_jobs SET reserved_at_utc='2026-01-01T00:00:00+00:00'")
    elif change == 'missing_prior_marker':
        with queue.db: queue.db.execute('DELETE FROM reviewed_repair_items')
    elif change == 'no_provider': facts['provider'] = 'other'
    elif change == 'empty_response': facts['response_bytes'] = 0
    with queue.db:
        queue.db.execute('UPDATE native_candidate_jobs SET result_json=?,request_json=?',
            (json.dumps(facts, sort_keys=True) if change != 'malformed_json' else '{',
             row['request_json'] if change != 'wrong_source' else json.dumps(request)))
    value = witness.reference(queue.root, value['request_id'])
    if change == 'hash': value['result_sha256'] = 'f' * 64
    before = head(queue)
    result = submit(queue, value)
    assert not result['submitted'] and result['excluded']
    assert head(queue) == before and not list((queue.root / 'reviewed_repairs/pending').glob('*.json'))


@pytest.mark.parametrize('change', ['leased', 'future_due', 'stored', '403', 'complete'])
def test_source_success_does_not_override_head_lease_deadline_saved_body_or_refusal(prepared, change):
    queue, value, _ = prepared
    mutations = {'leased': {'lease_owner':'other'}, 'future_due': {'due': 9e12},
        'stored': {'body_sha256': 'a' * 64}, '403': {'state':'http_403','error_code':'http_403'},
        'complete': {'state':'complete','error_code':None}}[change]
    with queue.db: queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in mutations), tuple(mutations.values()))
    before = head(queue)
    assert not submit(queue, value)['submitted'] and head(queue) == before


def test_owner_revalidates_observed_evidence_after_submission_and_never_consumes_uncertainty(prepared):
    queue, value, _ = prepared
    result = submit(queue, value); before = head(queue)
    assert result['submitted'], result
    with queue.db: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome'")
    assert repairs.consume_repairs(queue)['outcomes'] == {'native_success_evidence_invalid': 1}
    assert head(queue) == before
    assert queue.db.execute("SELECT count(*) FROM reviewed_repair_items WHERE outcome=?", (witness.OUTCOME,)).fetchone()[0] == 0


def test_native_success_request_does_not_accept_untyped_extra_targets_or_retained_body_modes(prepared):
    queue, value, _ = prepared
    for wrong in [None, {}, {**value, 'extra':'not-evidence'}, dict(value, request_id='no'), dict(value, result_sha256=True)]:
        with pytest.raises(ValueError): submit(queue, wrong)
    with pytest.raises(ValueError):
        repairs.submit_repairs(queue.root, queue.registry, [URL], reason='No combined admission modes',
            native_success_witnesses={URL:value}, retained_originals={URL:{}})


def test_committed_native_marker_survives_archive_error_without_repeating_admission(prepared, monkeypatch):
    queue, value, _ = prepared
    request = submit(queue, value); before = head(queue); archive = repairs._archive
    def unavailable(*_a, **_k): raise OSError('Synthetic archive failure after the SQL commit')
    monkeypatch.setattr(repairs, '_archive', unavailable)
    assert repairs.consume_repairs(queue)['outcomes'][witness.OUTCOME] == 1
    assert head(queue) == dict(before, state='retry') and Path(request['request_path']).exists()
    monkeypatch.setattr(repairs, '_archive', archive)
    queue.reviewed_repair_schema_ready = False
    assert repairs.consume_repairs(queue)['outcomes'] == {}
    assert not Path(request['request_path']).exists()
    assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items WHERE outcome=?', (witness.OUTCOME,)).fetchone()[0] == 1
    assert dict(queue.db.execute('SELECT key,value FROM counters'))['reviewed_native_success_rechecks_admitted'] == 1


def test_owner_rejects_source_auth_change_after_witness_submission(prepared):
    queue, value, _ = prepared
    submit(queue, value); before = head(queue)
    queue.registry.sources['upmedia'] = replace(queue.registry.sources['upmedia'], authorization_fingerprint='e' * 64)
    assert repairs.consume_repairs(queue)['outcomes'] == {'source_identity_changed': 1}
    assert head(queue) == before


@pytest.mark.parametrize('change', ['kind', 'reference', 'provider', 'state', 'targets', 'version'])
def test_tampered_native_recheck_envelope_is_quarantined_before_admission(prepared, change):
    from test_news_reviewed_repairs import rewrite
    queue, value, _ = prepared
    submitted = submit(queue, value); before = head(queue)
    path = Path(submitted['request_path']); payload = repairs._load(path)
    if change == 'kind': payload['kind'] = 'reviewed_missing_body_repair'
    elif change == 'reference': payload['native_successes'][next(iter(payload['native_successes']))]['result_sha256'] = False
    elif change == 'provider': payload['targets'][0]['provider'] = 'other'
    elif change == 'state': payload['targets'][0].update(state='article_rights_or_scope_excluded',error_code='article_rights_or_scope_excluded')
    elif change == 'targets': payload['native_successes'] = {}
    elif change == 'version': payload['contract_version'] = True
    rewrite(path, payload)
    assert repairs.consume_repairs(queue)['outcomes'] == {'invalid_request': 1}
    assert head(queue) == before
