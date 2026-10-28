"""A source declaration permits a normal GET, never a fabricated raw replay."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_cmoneylearning as original
from downloader import news_fulltext_cohorts as cohorts
from downloader import news_fulltext_url_metadata as intake
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_repairs import _bytes
from test_news_candidate_diagnostics import http_stub
from test_news_cmnews_resident_diagnostics import (
    settings as parent_settings, pump, response, submit,
)
from test_news_cmnews_scope import URL as PARENT, page as parent_page
from test_news_cmoneylearning_original import (
    settings as original_settings, page as original_page,
)
from test_news_fulltext import config

URL = original.CMONEY_LEARNING_ORIGIN + original.CMONEY_LEARNING_TWELVE_DECLARED_20261026_PATHS[0]


def version7(entry):
    entry = deepcopy(entry)
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v7',
        article_path_pattern=original.CMONEY_LEARNING_V7_PATH_PATTERN, allow_reviewed_supplements=True,
        adapter_verification='native_canonical_BlogPosting_editor_clock_v7_twelve_declared_20261026',
        note=original.CMONEY_LEARNING_V7_NOTE)
    entry['authorization'].update(reviewed_on='2026-10-26', scope=original.CMONEY_LEARNING_V7_AUTH_SCOPE)
    return entry


@pytest.fixture
def settings(parent_settings, original_settings):
    parent_settings['providers'].append(version7(original_settings['providers'][0]))
    return parent_settings


@pytest.fixture
def queue(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    with owner.db:
        owner.add_articles([{'url': PARENT}] + [
            {'url': original.CMONEY_LEARNING_ORIGIN + p} for p in original.CMONEY_LEARNING_PATHS])
        owner.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (PARENT,))
    owner.sync_finite_cohorts()
    yield owner
    owner.close()


def head(queue, url=URL):
    row = queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    return dict(row) if row else None


def declared(queue, settings):
    submitted = submit(queue, settings, evidence_contract_version=54); pump(queue)
    client = http_stub(queue, response=response(parent_page(canonical=URL, schema_url=PARENT)))
    job = jobs.claim_candidate(queue, client, settings, 'cmnews')
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job)); pump(queue)
    row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (submitted['request_id'],)).fetchone())
    return {'url': URL, 'declared_original': {'request_id': submitted['request_id'],
        'job_sha256': hashlib.sha256(_bytes(row)).hexdigest()}}


def consume(queue):
    return intake.consume_urls(queue, max_seconds=.1)


def test_scope_admission_uses_same_writer_and_is_not_a_body(queue, settings, monkeypatch):
    entry = declared(queue, settings)
    parent_before = head(queue, PARENT)
    old_jobs = [dict(row) for row in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    old_cohort = [dict(row) for row in queue.db.execute('SELECT * FROM finite_cohorts')]
    changes = queue.db.total_changes
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('no second HTTP owner'))
    submitted = intake.submit_declared_urls(queue.root, queue.registry, [entry, entry])
    assert submitted['submitted_urls'] == 1 and submitted['contract_version'] == 2
    assert submitted['http_requests'] == submitted['queue_writes'] == 0 and queue.db.total_changes == changes
    assert head(queue) is None
    assert consume(queue)['outcomes'] == {'declared_original_acquisition_registered': 1}
    captured = head(queue)
    assert captured['state'] == 'pending' and not captured['body_sha256'] and captured['attempts'] == 0
    assert not list(queue.root.glob('bodies/**/*.parquet'))
    assert head(queue, PARENT) == parent_before
    assert [dict(row) for row in queue.db.execute('SELECT * FROM native_candidate_jobs')] == old_jobs
    assert [dict(row) for row in queue.db.execute('SELECT * FROM finite_cohorts')] == old_cohort
    assert cohorts.validate_supplements(queue.root, queue.db)[0]['integrity_verified']
    with pytest.raises(ValueError, match='no retained original'):
        cohorts.materialize_reviewed_supplements(queue, [URL], lease_seconds=300)
    intake.submit_declared_urls(queue.root, queue.registry, [entry])
    assert consume(queue)['outcomes'] == {'existing_head_preserved': 1} and head(queue) == captured
    receipt = json.loads((Path(submitted['pending_path']).parent.parent/'completed'/
        (submitted['request_id']+'.receipt.json')).read_bytes())
    assert receipt['contract_version'] == 2 and not receipt['body_acceptance_proven'] and receipt['http_requests'] == 0


@pytest.mark.parametrize('change', ['hash', 'head', 'source', 'state', 'clock', 'url', 'body', 'literal', 'request', 'native'])
def test_unverified_or_changed_declaration_is_not_enqueued(queue, settings, change):
    entry = declared(queue, settings)
    witness = entry['declared_original']
    row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (witness['request_id'],)).fetchone())
    facts = json.loads(row['result_json'])
    if change == 'hash':
        witness['job_sha256'] = '0'*64
    elif change == 'head':
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (PARENT,))
    elif change == 'source':
        altered = deepcopy(settings); altered['providers'][0]['authorization']['reviewed_on'] = '2026-10-25'
        queue.registry = PolicyRegistry(altered, base=queue.registry.base)
    elif change == 'url':
        entry['url'] = original.CMONEY_LEARNING_ORIGIN + original.CMONEY_LEARNING_PATHS[0]
    else:
        if change == 'state':
            row['state'] = 'uncertain_outcome'
        elif change == 'clock':
            row['completed_at_utc'] = '2099-01-01T00:00:00+00:00'
        elif change == 'request':
            req = json.loads(row['request_json']); req['reason'] = 'changed without request hash'
            row['request_json'] = _bytes(req).decode()
        elif change == 'body':
            facts['accepted_as_article'] = True
        elif change == 'native':
            facts['native_identity']['native_UUID_container_matches'] = False
        else:
            facts['canonical_attribute']['canonical_attribute']['literal'] += 'guessed/'
        row['result_json'] = _bytes(facts).decode()
        with queue.db:
            queue.db.execute('UPDATE native_candidate_jobs SET state=?,completed_at_utc=?,request_json=?,result_json=? WHERE request_id=?',
                (row['state'],row['completed_at_utc'],row['request_json'],row['result_json'],row['request_id']))
        witness['job_sha256'] = hashlib.sha256(_bytes(row)).hexdigest()
    before = queue.db.total_changes
    with pytest.raises(ValueError, match='invalid_closed_declared_original_witness'):
        intake.submit_declared_urls(queue.root, queue.registry, [entry])
    assert queue.db.total_changes == before and head(queue) is None
    assert not (queue.root/'reviewed_repairs/url_metadata').exists()


def test_owner_rechecks_a_changed_witness_without_writing_a_frontier(queue, settings):
    entry = declared(queue, settings)
    intake.submit_declared_urls(queue.root, queue.registry, [entry])
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (PARENT,))
    assert consume(queue)['outcomes'] == {'declared_original_witness_rejected': 1}
    assert head(queue) is None and not cohorts.validate_supplements(queue.root, queue.db)


@pytest.mark.parametrize('state', ['complete', 'http_403', 'http_429', 'retry', 'inflight'])
def test_existing_bodies_failures_and_leases_are_not_rearmed(queue, settings, state):
    entry = declared(queue, settings)
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute('UPDATE articles SET state=?,attempts=3,due=?,lease_owner=?,lease_until=? WHERE url=?',
            (state,time.time()+50000,'existing-owner',time.time()+600,URL))
    before = head(queue)
    intake.submit_declared_urls(queue.root, queue.registry, [entry])
    assert consume(queue)['outcomes'] == {'existing_head_preserved': 1}
    assert head(queue) == before and not cohorts.validate_supplements(queue.root, queue.db)


def test_waiting_metadata_can_be_approved_then_downloaded_with_normal_durable_acceptance(queue, settings):
    entry = declared(queue, settings)
    intake.submit_urls(queue.root, queue.registry, [URL]); consume(queue)
    assert head(queue)['state'] == 'waiting_cohort'
    intake.submit_declared_urls(queue.root, queue.registry, [entry]); consume(queue)
    raw = original_page(URL.split(original.CMONEY_LEARNING_ORIGIN,1)[1]).encode()
    client = http_stub(queue, response=Response(URL, 200, {'content-type':'text/html; charset=UTF-8'}, raw))
    get = client.get; client.get = lambda url, **kwargs: get(url)
    row = queue.claim_url(URL, 300)
    record = fetch_article(client, row)
    queue.accept([record])
    assert client.calls == [URL] and head(queue)['state'] == 'complete' and head(queue)['body_sha256']
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert verify_bodies(queue.root)['accepted']
    root, registry = queue.root, queue.registry
    queue.close()
    restarted = NewsQueue(root, registry)
    try:
        assert head(restarted)['state'] == 'complete'
        before = head(restarted)
        intake.submit_declared_urls(root, registry, [entry]); consume(restarted)
        assert head(restarted) == before and cohorts.validate_supplements(root, restarted.db)
    finally:
        restarted.close()


def test_declared_scope_rejects_corrupt_immutable_proof(queue, settings):
    entry = declared(queue, settings)
    intake.submit_declared_urls(queue.root, queue.registry, [entry]); consume(queue)
    header = queue.db.execute('SELECT * FROM finite_capture_supplements').fetchone()
    receipt = json.loads((queue.root/header['receipt_path']).read_bytes())
    (queue.root/receipt['source_evidence'][0]['private_source_path']).write_bytes(b'corrupted')
    with pytest.raises(ValueError, match='immutable source digest'):
        cohorts.validate_supplements(queue.root, queue.db)


def test_interruption_rolls_back_membership_with_cursor_and_resumes_without_http(queue, settings, monkeypatch):
    entry = declared(queue, settings)
    intake.submit_declared_urls(queue.root, queue.registry, [entry])
    approve = cohorts.approve_declared_original
    def interrupted(*args):
        approve(*args)
        raise OSError('interruption after membership, before cursor commit')
    with monkeypatch.context() as patch:
        patch.setattr(cohorts, 'approve_declared_original', interrupted)
        with pytest.raises(OSError, match='interruption'):
            consume(queue)
    assert head(queue) is None and not cohorts.validate_supplements(queue.root, queue.db)
    root, registry = queue.root, queue.registry
    queue.close()
    restarted = NewsQueue(root, registry)
    try:
        assert intake.has_pending(restarted)
        assert consume(restarted)['outcomes'] == {'declared_original_acquisition_registered': 1}
        assert head(restarted)['state'] == 'pending' and not head(restarted)['body_sha256']
        assert len(cohorts.validate_supplements(root, restarted.db)) == 1
    finally:
        restarted.close()


def test_owner_rechecks_original_source_generation_after_submission(queue, settings):
    entry = declared(queue, settings)
    intake.submit_declared_urls(queue.root, queue.registry, [entry])
    changed = deepcopy(settings)
    changed['providers'][1]['authorization']['reviewed_on'] = '2026-10-25'
    queue.registry = PolicyRegistry(changed, base=queue.registry.base)
    assert consume(queue)['outcomes'] == {'source_contract_changed': 1}
    assert head(queue) is None and not cohorts.validate_supplements(queue.root, queue.db)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('path', original.CMONEY_LEARNING_V6_PATHS)
def test_version7_preserves_completed_original_semantics(original_settings, path, parser):
    entry = original_settings['providers'][0]
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v6',
        article_path_pattern=original.CMONEY_LEARNING_V6_PATH_PATTERN, allow_reviewed_supplements=True,
        html_parser=parser)
    entry['authorization']['scope'] = original.CMONEY_LEARNING_V6_AUTH_SCOPE
    before = PolicyRegistry(original_settings, base=Path.cwd()).sources['cmoneylearning']
    original_settings['providers'][0] = version7(entry)
    after = PolicyRegistry(original_settings, base=Path.cwd()).sources['cmoneylearning']
    a, b = (extract_article(original_page(path).encode(), s, page_url=original.CMONEY_LEARNING_ORIGIN+path)
        for s in (before, after))
    assert a.pop('article_identity_basis') != b.pop('article_identity_basis')
    assert a == b


def test_exact_installed_candidate_keeps_the_prior_http_budget_identity():
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    i = next(i for i, p in enumerate(settings['providers']) if p['id'] == 'cmoneylearning')
    settings['providers'][i] = version7(settings['providers'][i])
    source = PolicyRegistry(settings, base=repo).sources['cmoneylearning']
    assert (source.data_fingerprint,source.authorization_fingerprint,source.http_capacity_source_fingerprint,
        source.http_capacity_authorization_fingerprint) == original.CMONEY_LEARNING_V7_HTTP_BINDING
    settings['providers'][i]['parallelism'] += 1
    changed = PolicyRegistry(settings, base=repo).sources['cmoneylearning']
    assert changed.http_capacity_source_fingerprint != source.http_capacity_source_fingerprint
