"""A new, one-time private original question; no article or HTTP bypass."""
from copy import deepcopy
from contextlib import closing
from dataclasses import asdict, replace
import hashlib
import json
import stat
import time

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_http import RedirectHop
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_taiwannews import (
    TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION, TAIWANNEWS_V3_LAYOUT, TAIWANNEWS_V3_VERIFICATION,
    private_taiwannews_single_root_original_permit,
)
from test_news_candidate_diagnostics import http_stub
from test_news_taiwannews_original import CLOCK, PARAGRAPHS, RC, URL, production_settings
from test_news_taiwannews_private_original import (
    failed, inline_page, previous_inline_observation, response, submit_inline,
)


def single_root_page():
    document = BeautifulSoup(inline_page(), 'lxml')
    target = document.find(id='P:1')
    for node in list(document.find(id='S:1').contents):
        target.insert_before(node.extract())
    target.extract()
    for node in list(document.select('[id^="S:"]')):
        if node['id'] != 'S:0':
            node.extract()
    for node in list(document.select('[id^="P:"]')):
        node.extract()
    for node in list(document.find_all('script')):
        if '$RS' in node.get_text():
            node.extract()
    assert [node.get_text() for node in document.find_all('script')] == [RC]
    return document.encode()


def settings(tmp_path, version=3):
    result, _ = production_settings(2)
    if version == 3:
        result['providers'][0].update(embedded_article_layout=TAIWANNEWS_V3_LAYOUT,
            adapter_verification=TAIWANNEWS_V3_VERIFICATION)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, exclusive_http_owner=False)
    result['adaptive_rate'] = {'enabled': False}
    return result


def source(tmp_path, version=3):
    return PolicyRegistry(settings(tmp_path, version), base='.').sources['taiwannews']


def submit(queue, config, requested=URL, **changes):
    return diagnostic.submit_candidate(queue.root, config, queue.registry, requested, TAIWANNEWS_V3_LAYOUT,
        reason=changes.pop('reason', 'New completed single-root scope; never rearm completed17'),
        evidence_contract_version=18, **changes)


def hops(requested):
    return () if requested == URL else (RedirectHop(requested, 301, '/zh/news/1234567', URL,
        '2026-10-01T00:00:00+00:00', 'a'*64, 1),)


def seed_prior(tmp_path, requested=URL):
    config = settings(tmp_path, 2)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base='.'))) as queue:
        old = previous_inline_observation(queue, config, requested=requested)
        previous = submit_inline(queue, config, requested)
        diagnostic.pump_candidates(queue)
        http = http_stub(queue, response=response(single_root_page(), redirects=hops(requested)))
        job = diagnostic.claim_candidate(queue, http, config, 'taiwannews')
        facts = diagnostic.fetch_candidate(http, job)
        assert not isinstance(facts, PendingOriginal)
        assert facts['current_native_error'] == 'article_identity_not_verified'
        diagnostic.finish_candidate(queue, job, result=facts)
        assert http.calls == [requested]
        return old, previous


def test_scope_is_private_and_does_not_broaden_registered_article_parser(tmp_path):
    current = source(tmp_path)
    assert candidate_evidence_allowed(current, current, 18)
    assert not candidate_evidence_allowed(current, current, 17)
    assert not candidate_evidence_allowed(source(tmp_path, 2), source(tmp_path, 2), 18)
    raw = single_root_page()
    permit = private_taiwannews_single_root_original_permit(response(raw), current, URL)
    assert permit['question'] == TAIWANNEWS_SINGLE_ROOT_ORIGINAL_QUESTION
    assert permit['response_sha256'] == hashlib.sha256(raw).hexdigest()
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(raw, current, page_url=URL)
    facts = candidate_response_metadata(response(raw), current, current,
        evidence_contract_version=18, requested_url=URL)
    assert facts['private_original_permit'] == permit
    assert not facts['accepted_as_article'] and not facts['body_completeness_verified']
    assert PARAGRAPHS[0] not in json.dumps(facts, ensure_ascii=False)
    target = {'provider': 'taiwannews', 'id': hashlib.sha256(URL.encode()).hexdigest()}
    assert diagnostic._key(target, 'old', evidence_contract_version=18) == diagnostic._key(
        target, 'new-hash-cannot-rearm', evidence_contract_version=18)
    assert diagnostic._key(target, 'old', evidence_contract_version=18) != diagnostic._key(
        target, 'old', evidence_contract_version=17)


@pytest.mark.parametrize('kind', ['unknown_rc', 'no_rc', 'boundary_not_closed', 'root_after_rc',
    'destination_after_rc', 'dangling_segment', 'dangling_placeholder', 'duplicate_root',
    'hidden_destination', 'hidden_body', 'wire', 'media', 'bare_publisher', 'hidden_credit',
    'title_mismatch', 'clock_mismatch', 'old_source', 'changed_auth', 'two_scripts'])
def test_single_root_keeps_exact_transport_stream_visibility_clock_and_scope(tmp_path, kind):
    document = BeautifulSoup(single_root_page(), 'lxml')
    current = source(tmp_path)
    if kind == 'unknown_rc':
        document.script.string = RC+';unknown()'
    elif kind == 'no_rc':
        document.script.extract()
    elif kind == 'boundary_not_closed':
        document.find(id='B:0').next_sibling.next_sibling.extract()
    elif kind == 'root_after_rc':
        document.body.append(document.find(id='S:0').extract())
    elif kind == 'destination_after_rc':
        document.body.append(document.main.extract())
    elif kind in {'dangling_segment', 'dangling_placeholder', 'duplicate_root'}:
        node = document.new_tag('div' if kind != 'dangling_placeholder' else 'template')
        node['id'] = {'dangling_segment':'S:1', 'dangling_placeholder':'P:1', 'duplicate_root':'S:0'}[kind]
        document.body.append(node)
    elif kind == 'hidden_destination':
        document.main['hidden'] = ''
    elif kind == 'hidden_body':
        document.select_one('.editor')['hidden'] = ''
    elif kind in {'wire', 'media'}:
        node = document.new_tag('p' if kind == 'wire' else 'iframe')
        node.string = '中央社提供' if kind == 'wire' else 'not prose'
        document.select_one('.editor').append(node)
    elif kind in {'bare_publisher', 'hidden_credit', 'clock_mismatch'}:
        clock = document.select_one('h1 ~ p.text-right')
        if kind == 'bare_publisher':
            clock.string = '台灣英文新聞 '+CLOCK
        elif kind == 'hidden_credit':
            clock['hidden'] = ''
        else:
            clock.string = '台灣英文新聞－編輯 May. 13, 2026 15:43'
    elif kind == 'title_mismatch':
        document.title.string = 'Unrelated article'
    elif kind == 'old_source':
        current = source(tmp_path, 2)
    elif kind == 'changed_auth':
        current = replace(current, authorization_fingerprint='f'*64)
    elif kind == 'two_scripts':
        document.body.append(deepcopy(document.script))
    with pytest.raises(NewsError):
        private_taiwannews_single_root_original_permit(response(document.encode()), current, URL)


@pytest.mark.parametrize('changes', [{'status':403}, {'status':429}, {'headers':{'content-type':'application/json'}},
    {'body':b'\x00'}, {'body':b'x'*((2 << 20)+1)}, {'url':URL+'?alias=1'},
    {'url':URL.replace('1234567','1234568')}])
def test_transport_is_not_bypassed(tmp_path, changes):
    with pytest.raises(NewsError):
        private_taiwannews_single_root_original_permit(response(single_root_page(), **changes), source(tmp_path), URL)


@pytest.mark.parametrize('legacy', [False, True])
def test_same_owner_writes_once_with_independent_readback_and_no_article_mutation(tmp_path, monkeypatch, legacy):
    requested = URL.replace('/zh/','/ch/') if legacy else URL
    seed_prior(tmp_path, requested)
    config = settings(tmp_path)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base='.'))) as queue:
        closed = [tuple(row) for row in queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')]
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (requested,)).fetchone())
        budgets = [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')]
        submitted = submit(queue, config, requested)
        diagnostic.pump_candidates(queue)
        raw = single_root_page()
        http = http_stub(queue, response=response(raw, redirects=hops(requested)))
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a, **k: pytest.fail('reuse resident HTTP owner'))
        job = diagnostic.claim_candidate(queue, http, config, 'taiwannews')
        assert job is not None
        result = diagnostic.fetch_candidate(http, job)
        assert isinstance(result, PendingOriginal)
        diagnostic.finish_candidate(queue, job, result=result)
        status = diagnostic.candidate_status(queue.root, submitted['request_id'])
        assert status['state'] == 'observed' and status['raw_or_names_or_body_retained'] is True
        facts = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',
            (submitted['request_id'],)).fetchone()[0])
        reference = facts['private_original']
        path, receipt = [queue.root/reference[key] for key in ('path','receipt_path')]
        assert stat.S_IMODE(path.stat().st_mode) == stat.S_IMODE(receipt.stat().st_mode) == 0o600
        stored, = pq.ParquetFile(path).read().to_pylist()
        assert stored['raw_response_body'] == raw and stored['request_url'] == requested
        assert stored['source_document_url'] == URL and json.loads(stored['transport_redirects_json']) == [
            asdict(hop) for hop in hops(requested)]
        assert not stored['accepted_as_article'] and not stored['publish'] and not stored['remote_training_export']
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id<>? ORDER BY request_id',
            (submitted['request_id'],))] == closed
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (requested,)).fetchone()) == before
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings')] == budgets
        assert not list(queue.root.glob('bodies/**/*.parquet'))
        assert not submit(queue, config, requested, reason='new reason still completed')['submitted']
        assert diagnostic.claim_candidate(queue, http, config, 'taiwannews') is None and http.calls == [requested]


@pytest.mark.parametrize('kind', ['missing', 'uncertain', 'reserved', 'retained', 'wrong_hash',
    'wrong_question', 'changed_source', 'changed_head', 'future', 'unrelated_failure'])
def test_only_actual_closed17_single_root_witness_with_unchanged_failed_head_is_eligible(tmp_path, kind):
    _, previous = seed_prior(tmp_path)
    config = settings(tmp_path)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base='.'))) as queue:
        row = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (previous['request_id'],)).fetchone()
        request, facts = json.loads(row['request_json']), json.loads(row['result_json'])
        with queue.db:
            if kind == 'missing':
                queue.db.execute('DELETE FROM native_candidate_jobs WHERE request_id=?', (previous['request_id'],))
            elif kind in {'uncertain','reserved'}:
                queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?',
                    ('uncertain_outcome' if kind == 'uncertain' else kind, previous['request_id']))
            elif kind in {'changed_head','future','unrelated_failure'}:
                if kind == 'changed_head':
                    queue.db.execute('UPDATE articles SET attempts=attempts+1')
                elif kind == 'future':
                    queue.db.execute('UPDATE articles SET due=?', (time.time()+3600,))
                else:
                    queue.db.execute("UPDATE articles SET state='http_429',error_code='http_429'")
            else:
                if kind == 'retained':
                    facts['raw_or_names_or_body_retained'] = True
                elif kind == 'wrong_hash':
                    facts['taiwannews_native_boundary']['stream_scripts']['sha256'] = ['f'*64]
                elif kind == 'wrong_question':
                    facts['question'] = 'another-question'
                elif kind == 'changed_source':
                    request['source']['authorization_fingerprint'] = 'f'*64
                queue.db.execute('UPDATE native_candidate_jobs SET request_json=?,result_json=? WHERE request_id=?',
                    (json.dumps(request),json.dumps(facts),previous['request_id']))
        with pytest.raises(ValueError):
            submit(queue, config)
        assert diagnostic.claim_candidate(queue, http_stub(queue), config, 'taiwannews') is None


def test_prior_witness_is_rechecked_on_claim_and_original_scope_on_write(tmp_path):
    _, previous = seed_prior(tmp_path)
    config = settings(tmp_path)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base='.'))) as queue:
        submitted = submit(queue, config)
        diagnostic.pump_candidates(queue)
        with queue.db:
            queue.db.execute("UPDATE native_candidate_jobs SET state='observed_error' WHERE request_id=?", (previous['request_id'],))
        http = http_stub(queue, response=response(single_root_page()))
        assert diagnostic.claim_candidate(queue, http, config, 'taiwannews') is None and not http.calls
        assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'skipped'


def test_forged_single_root_permit_is_rejected_by_independent_writer(tmp_path):
    seed_prior(tmp_path)
    config = settings(tmp_path)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base='.'))) as queue:
        submitted = submit(queue, config)
        diagnostic.pump_candidates(queue)
        http = http_stub(queue, response=response(single_root_page()))
        job = diagnostic.claim_candidate(queue, http, config, 'taiwannews')
        result = diagnostic.fetch_candidate(http, job)
        forged = replace(result, response=response(single_root_page().replace('台灣英文新聞－編輯'.encode(), '中央社'.encode())))
        diagnostic.finish_candidate(queue, job, result=forged)
        assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed_error'
        assert not list(queue.root.glob('candidate_diagnostics/originals/**/*.parquet'))
