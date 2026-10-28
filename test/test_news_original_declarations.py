"""Literal original acquisition, transaction recovery and unchanged raw values."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from downloader import news_fulltext_cmoneylearning as original
from downloader import news_fulltext_cohorts as cohorts
from downloader import news_fulltext_original_declarations as declarations
from downloader import news_fulltext_url_metadata as intake
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_candidate_diagnostics import recorded_cmnews_canonical
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_repairs import _bytes, _head
from test_news_candidate_diagnostics import http_stub
from test_news_cmnews_resident_diagnostics import settings as parent_settings, response
from test_news_cmnews_scope import URL as PARENT, page as parent_page
from test_news_cmoneylearning_original import settings as original_settings, page as original_page
from test_news_declared_original_acquisition import version7
from test_news_fulltext import config

URL = original.CMONEY_LEARNING_ORIGIN + '/new-observed-original-20261027/'


def version8(entry):
    entry = deepcopy(entry)
    entry.update(embedded_article_layout='cmoneylearning_visible_article_v8',
        article_path_pattern=original.CMONEY_LEARNING_V8_PATH_PATTERN,
        adapter_verification='native_canonical_BlogPosting_editor_clock_v8_source_bound_declarations',
        allow_reviewed_supplements=True, note=original.CMONEY_LEARNING_V8_NOTE)
    entry['authorization'].update(reviewed_on='2026-10-27', scope=original.CMONEY_LEARNING_V8_AUTH_SCOPE)
    return entry


@pytest.fixture
def settings(parent_settings, original_settings):
    parent_settings['providers'].append(version8(original_settings['providers'][0]))
    return parent_settings


@pytest.fixture
def queue(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    with queue.db:
        queue.add_articles([{'url': PARENT}] + [{'url': original.CMONEY_LEARNING_ORIGIN + p}
            for p in original.CMONEY_LEARNING_PATHS])
    queue.sync_finite_cohorts()
    yield queue
    queue.close()


def head(queue, url):
    row = queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    return dict(row) if row else None


def failure(queue, *, old_metadata_only=False):
    client = http_stub(queue, response=response(parent_page(canonical=URL, schema_url=PARENT)))
    get = client.get
    client.get = lambda url, **kwargs: get(url)
    row = queue.claim_url(PARENT, 300)
    assert row is not None
    with pytest.raises(NewsError, match='article_identity_not_verified') as failure:
        fetch_article(client, row)
    error = failure.value
    assert error.cmnews_original_declaration['canonical_attribute']['literal'] == URL
    if old_metadata_only:
        del error.cmnews_original_declaration
    queue.failed(row, error, 3)
    assert client.calls == [PARENT]
    return error


def consume(queue):
    return intake.consume_urls(queue, max_seconds=.1)


def normal_witness(queue):
    parent = queue.registry.sources['cmnews']
    target = _head(queue.db.execute('SELECT * FROM articles WHERE url=?', (PARENT,)).fetchone())
    saved = recorded_cmnews_canonical(queue.db, parent, target)
    assert saved and saved['canonical_url'] == URL
    return dict(kind=declarations.NORMAL_KIND, parent_url=PARENT, parent_head=target,
        metadata_path=saved['metadata_path'], metadata_sha256=saved['metadata_sha256'])


def test_same_response_is_atomic_acquisition_not_alias_or_body(queue):
    failure(queue)
    parent = head(queue, PARENT)
    assert parent['state'] == 'article_identity_not_verified' and parent['body_sha256'] is None
    assert head(queue, URL) is None
    outbox = queue.db.execute('SELECT * FROM original_declaration_outbox').fetchone()
    assert outbox['state'] == 'pending'
    assert hashlib.sha256(outbox['payload'].encode()).hexdigest() == outbox['id']
    assert consume(queue)['outcomes'] == {'declared_original_acquisition_registered': 1}
    assert head(queue, PARENT) == parent and head(queue, URL)['state'] == 'pending'
    assert queue.db.execute('SELECT count(*) FROM finite_supplement_members').fetchone()[0] == 1
    assert queue.db.execute('SELECT count(*) FROM finite_cohort_members').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    assert consume(queue)['processed'] == 0
    assert cohorts.validate_supplements(queue.root, queue.db)


def test_pending_declaration_resumes_after_owner_restart_without_another_get(queue):
    failure(queue)
    before = head(queue, PARENT)
    root, registry = queue.root, queue.registry
    queue.close()
    restarted = NewsQueue(root, registry)
    try:
        assert consume(restarted)['processed'] == 1
        assert head(restarted, PARENT) == before
        assert head(restarted, URL)['state'] == 'pending'
        assert consume(restarted)['processed'] == 0
    finally:
        restarted.close()


def test_normal_original_requires_actual_http_and_durable_acceptance(queue):
    failure(queue)
    consume(queue)
    parent = head(queue, PARENT)
    from downloader.news_fulltext_http import Response
    raw = original_page('/new-observed-original-20261027/').encode()
    client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html; charset=UTF-8'}, raw))
    get = client.get
    client.get = lambda url, **kwargs: get(url)
    record = fetch_article(client, queue.claim_url(URL, 300))
    queue.accept([record])
    assert client.calls == [URL] and head(queue, URL)['state'] == 'complete'
    assert head(queue, PARENT) == parent and verify_bodies(queue.root)['accepted']
    assert consume(queue)['processed'] == 0


def test_failure_rollback_cannot_leave_a_declaration_without_its_parent(queue, monkeypatch):
    real = declarations.record_failure
    def fail_after_record(*args):
        real(*args)
        raise RuntimeError('simulated transaction interruption')
    monkeypatch.setattr(declarations, 'record_failure', fail_after_record)
    with pytest.raises(RuntimeError, match='simulated transaction'):
        failure(queue)
    assert head(queue, PARENT)['state'] == 'inflight'
    exists = queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='original_declaration_outbox'").fetchone()
    assert not exists or queue.db.execute('SELECT count(*) FROM original_declaration_outbox').fetchone()[0] == 0
    assert head(queue, URL) is None


def test_membership_rollback_reuses_immutable_proof_and_pending_outbox(queue, monkeypatch):
    failure(queue)
    real = cohorts.approve_declared_original
    def interrupted(*args):
        real(*args)
        raise OSError('simulated durable write interruption')
    monkeypatch.setattr(cohorts, 'approve_declared_original', interrupted)
    deferred = consume(queue)
    assert deferred['processed'] == 0
    assert deferred['outcomes'] == {'original_declaration_storage_deferred': 1}
    assert head(queue, URL) is None
    assert queue.db.execute('SELECT state FROM original_declaration_outbox').fetchone()[0] == 'pending'
    monkeypatch.setattr(cohorts, 'approve_declared_original', real)
    assert consume(queue)['outcomes'] == {'declared_original_acquisition_registered': 1}
    assert cohorts.validate_supplements(queue.root, queue.db)


def test_current_normal_failure_metadata_needs_no_extra_canonical_get(queue):
    failure(queue, old_metadata_only=True)
    witness = normal_witness(queue)
    request = intake.submit_declared_urls(queue.root, queue.registry,
        [{'url': URL, 'declared_original': witness}], contract_version=3)
    assert request['http_requests'] == 0 and request['contract_version'] == 3
    assert consume(queue)['outcomes'] == {'declared_original_acquisition_registered': 1}
    assert head(queue, PARENT)['body_sha256'] is None and head(queue, URL)['state'] == 'pending'


@pytest.mark.parametrize('change', ['sha', 'path', 'parent', 'due', 'bool_attempt', 'source', 'canonical'])
def test_observed_witness_rejects_corruption_and_changed_source(queue, change):
    failure(queue, old_metadata_only=True)
    witness = normal_witness(queue)
    url = URL
    if change == 'sha': witness['metadata_sha256'] = '0' * 64
    elif change == 'path': witness['metadata_path'] += '.missing'
    elif change == 'parent': witness['parent_url'] += '?other=1'
    elif change == 'due': witness['parent_head']['due'] += 1
    elif change == 'bool_attempt': witness['parent_head']['attempts'] = True
    elif change == 'canonical': url = URL.replace('new-observed', 'not-declared')
    else:
        with queue.db:
            queue.db.execute('UPDATE articles SET state=\'complete\' WHERE url=?', (PARENT,))
    with pytest.raises((ValueError, NewsError)):
        cohorts.validate_declared_original(queue.db, queue.registry, url, witness)
    assert head(queue, URL) is None


def test_ordinary_url_shape_alone_does_not_admit_a_supplement(queue):
    intake.submit_urls(queue.root, queue.registry, [URL])
    consume(queue)
    assert head(queue, URL)['state'] == 'waiting_cohort'
    assert queue.claim_url(URL, 300) is None
    assert queue.db.execute('SELECT count(*) FROM finite_supplement_members').fetchone()[0] == 0


@pytest.mark.parametrize('url', ['https://evil.test/post/', original.CMONEY_LEARNING_ORIGIN + '/',
    original.CMONEY_LEARNING_ORIGIN + '/author/person/', original.CMONEY_LEARNING_ORIGIN + '/bad%xy/',
    URL + '?token=secret', URL + '#fragment', URL.replace('https:', 'http:'), URL.replace('/new-', '/a.b-')])
def test_unreviewed_transport_shape_is_not_an_original(url):
    assert not original.cmoneylearning_literal_original(url)


@pytest.mark.parametrize('path', original.CMONEY_LEARNING_V7_PATHS)
def test_version8_preserves_previous_raw_article_semantics(original_settings, path):
    previous, current = deepcopy(original_settings), deepcopy(original_settings)
    previous['providers'][0] = version7(previous['providers'][0])
    current['providers'][0] = version8(current['providers'][0])
    raw = original_page(path).encode()
    url = original.CMONEY_LEARNING_ORIGIN + path
    a, b = (PolicyRegistry(value, base=Path('.')).require(url) for value in (previous, current))
    old, new = (extract_article(raw, source, page_url=url) for source in (a, b))
    assert old.pop('article_identity_basis').replace('_v7;', '_v8;') == new.pop('article_identity_basis')
    assert old == new


@pytest.mark.parametrize('change', ['digest', 'parent', 'source', 'missing_parent_policy'])
def test_stale_outbox_is_closed_without_a_request_or_original(queue, settings, change):
    failure(queue)
    if change == 'digest':
        with queue.db:
            queue.db.execute("UPDATE original_declaration_outbox SET payload=payload||' '")
    elif change == 'parent':
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (PARENT,))
    else:
        altered = deepcopy(settings)
        if change == 'source':
            altered['providers'][1]['authorization']['reviewed_on'] = '2026-10-28'
        else:
            altered['providers'] = altered['providers'][1:]
        queue.registry = PolicyRegistry(altered, base=queue.registry.base)
    assert consume(queue)['outcomes'] == {'declared_original_witness_rejected': 1}
    assert head(queue, URL) is None
    assert queue.db.execute('SELECT state FROM original_declaration_outbox').fetchone()[0] == 'rejected'
    assert consume(queue)['processed'] == 0


def test_closed_body_is_not_reopened_by_pending_declaration(queue):
    failure(queue)
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=?,attempts=2 WHERE url=?", ('a'*64, URL))
    before = head(queue, URL)
    assert consume(queue)['outcomes'] == {'existing_head_preserved': 1}
    assert head(queue, URL) == before
    assert queue.db.execute('SELECT count(*) FROM finite_supplement_members').fetchone()[0] == 0


def test_reviewed_candidate_inherits_capacity_only_for_exact_identity():
    from downloader.news_fulltext import load_config
    repo = Path(__file__).resolve().parents[1]
    cfg, previous, base = load_config(repo / 'configs/news_fulltext.json')
    cfg = deepcopy(cfg)
    cfg.pop('provider_catalog', None)
    entry = next(p for p in cfg['providers'] if p['id'] == 'cmoneylearning')
    entry.update(version8(entry))
    current = PolicyRegistry(cfg, base=base).sources['cmoneylearning']
    assert (current.data_fingerprint, current.authorization_fingerprint,
        current.http_capacity_source_fingerprint, current.http_capacity_authorization_fingerprint) == original.CMONEY_LEARNING_V8_HTTP_BINDING
    assert current.http_capacity_source_fingerprint == previous.sources['cmoneylearning'].http_capacity_source_fingerprint
    entry['authorization']['reviewed_on'] = '2026-10-28'
    changed = PolicyRegistry(cfg, base=base).sources['cmoneylearning']
    assert changed.http_capacity_authorization_fingerprint != current.http_capacity_authorization_fingerprint
