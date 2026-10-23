"""Current registered Pocket paths use the resident once-only original lane."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal, private_original_permit
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_repairs import _head_problem
from test_news_pocket_original import settings, page, URL, OLD
from test_news_pocket_opaque_routes import opaque_routes, OPAQUE
from test_news_pocket_numeric_routes import redirect, routes

LEGACY = 'https://www.pocket.tw/school/report/newflash/12345'
LAYOUT = 'pocket_visible_article_v11'


def setup(settings, tmp_path, kind='opaque', state=None):
    config = opaque_routes(settings)
    config.update(root=str(tmp_path), min_free_disk_bytes=1, workers=2, exclusive_http_owner=False,
        adaptive_rate={'enabled': False})
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    url = OPAQUE if kind == 'opaque' else LEGACY
    state = state or ('article_identity_not_verified' if kind == 'opaque' else 'unsupported_article_path')
    queue.add_articles([{'url': url}])
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=2,due=0,last_checked_at_utc=?',
            (state, state, '2026-01-01T00:00:00+00:00'))
    raw = page().replace(URL, url) if kind == 'opaque' else page()
    raw = (raw.replace('第2段', '<unreviewed>第2段</unreviewed>') if kind == 'opaque'
        else raw.replace('更新於 2023-02-15 ｜', '更新於 malformed label ｜'))
    hops = () if kind == 'opaque' else (redirect(url, OLD, 301), redirect(OLD, URL))
    response = Response(url if kind == 'opaque' else URL, 200, {'content-type': 'text/html'}, raw.encode(), hops)
    calls = []
    client = SimpleNamespace(registry=queue.registry, get=lambda url: (calls.append(url), response)[1],
        calls=calls, provider_not_before={}, blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())
    return config, queue, url, response, client


def submit(config, queue, url, kind):
    return diagnostic.submit_candidate(queue.root, config, queue.registry, url, LAYOUT,
        reason='One exact current native original, never body admission or normal requeue',
        evidence_contract_version=4 if kind == 'opaque' else 5)


def claim(config, queue, url, kind, client):
    request = submit(config, queue, url, kind)
    with patch.object(diagnostic.time, 'monotonic', return_value=0.0): diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, client, config, 'pocket')
    assert job and job['request_id'] == request['request_id']
    return request, job


@pytest.mark.parametrize('kind,state', [('opaque', 'article_identity_not_verified'),
    ('legacy', 'unsupported_article_path'), ('legacy', 'invalid_bulk_publication_clock'),
    ('legacy', 'article_identity_not_verified')])
def test_one_current_private_original_keeps_heads_clocks_actual_transport_and_fixed_key(settings, tmp_path, kind, state):
    config, queue, url, response, client = setup(settings, tmp_path, kind, state)
    try:
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        # A diagnostic-only accepted failure never broadens normal repair.
        if state != 'article_identity_not_verified': assert _head_problem(before, 1e12) == 'not_semantic_parser_failure'
        request, job = claim(config, queue, url, kind, client)
        pending = diagnostic.fetch_candidate(client, job)
        assert isinstance(pending, PendingOriginal)
        assert pending.facts['current_error'] == ('article_identity_not_verified' if kind == 'opaque' else 'invalid_bulk_publication_clock')
        diagnostic.finish_candidate(queue, job, result=pending)
        with patch.object(diagnostic.time, 'monotonic', return_value=0.0): diagnostic.pump_candidates(queue)
        status = diagnostic.candidate_status(queue.root, request['request_id'])
        assert status['state'] == 'observed' and status['raw_retention_state'] == 'retained_verified'
        reference = status['result_json']['private_original']
        path = queue.root / reference['path']
        row = pq.ParquetFile(path).read().to_pylist()[0]
        assert row['raw_response_body'] == response.body
        assert row['request_url'] == url and row['final_url'] == response.url
        assert hashlib.sha256(response.body).hexdigest() == row['response_sha256'] == reference['response_sha256']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == reference['sha256']
        assert not path.stat().st_mode & 0o077
        assert all(row[k] is False for k in ('accepted_as_article', 'publish', 'historical_point_in_time',
            'per_article_license_verified', 'training_injection_enabled', 'remote_training_export'))
        if kind == 'legacy':
            assert row['source_document_url'] == URL and len(json.loads(row['transport_redirects_json'])) == 2
            assert status['result_json']['legacy_transport']['same_ID_transport_verified']
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before
        assert not submit(config, queue, url, kind)['submitted'] and client.calls == [url]
        assert queue.db.execute('SELECT COUNT(*) FROM accepted_batches').fetchone()[0] == 0
    finally: queue.close()


@pytest.mark.parametrize('kind', ['opaque', 'legacy'])
@pytest.mark.parametrize('obstruction', ['body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'missing_clock', 'future_clock', 'naive_clock', 'refusal', 'wrong_error'])
def test_protected_current_heads_do_not_even_enter_the_mailbox(settings, tmp_path, kind, obstruction):
    config, queue, url, _, client = setup(settings, tmp_path, kind)
    try:
        with queue.db:
            if obstruction == 'body': queue.db.execute('UPDATE articles SET body_sha256=?', ('a' * 64,))
            elif obstruction == 'parquet': queue.db.execute('UPDATE articles SET parquet_path=?', ('old.parquet',))
            elif obstruction in ('lease_owner', 'lease_until'):
                queue.db.execute('UPDATE articles SET ' + obstruction + '=?', ('other' if obstruction == 'lease_owner' else 1e12,))
            elif obstruction == 'future_due': queue.db.execute('UPDATE articles SET due=1e12')
            elif obstruction in ('missing_clock', 'future_clock', 'naive_clock'):
                value = {'missing_clock': None, 'future_clock': '2999-01-01T00:00:00+00:00', 'naive_clock': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=?', (value,))
            elif obstruction == 'refusal': queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403'")
            else: queue.db.execute("UPDATE articles SET error_code='http_403'")
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        with pytest.raises(ValueError): submit(config, queue, url, kind)
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before and not client.calls
        assert not (queue.root / 'reviewed_repairs').exists()
    finally: queue.close()


@pytest.mark.parametrize('kind', ['opaque', 'legacy'])
def test_unknown_current_original_outcome_is_never_replayed_after_restart(settings, tmp_path, kind):
    config, queue, url, _, client = setup(settings, tmp_path, kind)
    try:
        request, _ = claim(config, queue, url, kind, client)
        registry = queue.registry
        queue.close()
        queue = NewsQueue(tmp_path, registry)
        with patch.object(diagnostic.time, 'monotonic', return_value=0.0): diagnostic.pump_candidates(queue)
        state = diagnostic.candidate_status(tmp_path, request['request_id'])
        assert state['state'] == 'uncertain_outcome' and state['raw_or_names_or_body_retained'] is None
        assert not submit(config, queue, url, kind)['submitted']
        assert diagnostic.claim_candidate(queue, client, config, 'pocket') is None and not client.calls
    finally: queue.close()


@pytest.mark.parametrize('mutation', ['query', 'fragment', 'different_id', 'missing_chain', 'location', 'paid', 'foreign_credit'])
def test_received_legacy_original_cannot_bypass_actual_transport_or_public_native_scope(settings, tmp_path, mutation):
    config, queue, url, response, client = setup(settings, tmp_path, 'legacy')
    try:
        if mutation == 'query': response = replace(response, url=URL + '?x=1')
        elif mutation == 'fragment': response = replace(response, url=URL + '#x')
        elif mutation == 'different_id': response = replace(response, url=URL.replace('12345', '54321'))
        elif mutation == 'missing_chain': response = replace(response, redirects=())
        elif mutation == 'location': response = replace(response, redirects=(replace(response.redirects[0], location='/wrong/'), response.redirects[1]))
        elif mutation == 'paid': response = replace(response, body=response.body.replace(b'"isAccessibleForFree": true', b'"isAccessibleForFree": false'))
        else: response = replace(response, body=response.body.replace('口袋學堂編輯團隊'.encode(), '外部供稿'.encode()))
        client.get = lambda url: (client.calls.append(url), response)[1]
        _, job = claim(config, queue, url, 'legacy', client)
        result = diagnostic.fetch_candidate(client, job)
        assert isinstance(result, dict) and not result['raw_or_names_or_body_retained']
        assert client.calls == [url] and not (tmp_path / 'reviewed_repairs/candidate_diagnostics/originals').exists()
    finally: queue.close()


def test_v11_opaque_native_permit_does_not_relax_older_ABI_or_forge_numeric_identity(settings, tmp_path):
    config, queue, url, response, _ = setup(settings, tmp_path)
    try:
        permit = private_original_permit(response, queue.registry.sources['pocket'])
        assert permit['url'] == url
        old = PolicyRegistry(routes(settings), base=Path.cwd()).sources['pocket']
        with pytest.raises(NewsError): private_original_permit(response, old)
        for changed in (replace(response, redirects=[]), replace(response, url=URL),
                replace(response, body=response.body.replace(url.encode(), URL.encode()))):
            with pytest.raises(NewsError): private_original_permit(changed, queue.registry.sources['pocket'])
    finally: queue.close()
