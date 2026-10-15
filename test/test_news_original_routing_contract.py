"""Routing eligibility is separate from parser acceptance; batch lookup parity."""
from copy import deepcopy
from dataclasses import replace
import json
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_urls as routing
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_original_queue import alias


KEY = 'google_literal_url_routing_v1'


def checkpoint(queue):
    return json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0])


def finish(queue):
    while routing.sync_original_url_routes(queue):
        pass
    return checkpoint(queue)


@pytest.mark.parametrize('field,value', [
    ('selectors', ('article .new-container',)), ('html_parser', 'lxml'),
    ('data_fingerprint', 'e' * 64), ('parallelism', 7), ('interval_seconds', 5.),
    ('cnyes_apis', ('https://publisher.test/api',)),
])
def test_parser_and_transport_inputs_cannot_reset_routing_identity(config, tmp_path, field, value):
    registry = PolicyRegistry(config, base=tmp_path)
    before = routing.routing_fingerprint(registry)
    registry.sources['publisher'] = replace(registry.sources['publisher'], **{field: value})
    assert routing.routing_fingerprint(registry) == before


@pytest.mark.parametrize('field,value', [
    ('allowed', False), ('domains', ('changed.test',)), ('provider', 'other'),
    ('authorization_fingerprint', 'e' * 64), ('article_path_pattern', '^/limited/'),
    ('acquisition_mode', 'finite'),
])
def test_routing_eligibility_and_permission_inputs_change_identity(config, tmp_path, field, value):
    registry = PolicyRegistry(config, base=tmp_path)
    before = routing.routing_fingerprint(registry)
    registry.sources['publisher'] = replace(registry.sources['publisher'], **{field: value})
    assert routing.routing_fingerprint(registry) != before


def test_parser_upgrade_keeps_done_cursor_but_new_metadata_still_routes(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    item = alias('https://publisher.test/a')
    try:
        with q.db:
            q.add_articles([item])
        routing.backfill_original_urls(q)
        before = finish(q)
        assert before['after'] == 'done'
        old_policy = q.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()[0]
    finally:
        q.close()
    changed = deepcopy(config)
    changed['providers'][0]['selectors'] = ['.different-body-selector']
    q = NewsQueue(tmp_path, PolicyRegistry(changed, base=tmp_path))
    try:
        assert q.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()[0] != old_policy
        assert checkpoint(q) == before and routing.sync_original_url_routes(q) == 0
        with q.db:
            q.add_articles([{**item, 'title': 'New metadata, not accepted text', 'date': '2026-10-11 08:00:00'}])
        row = q.db.execute("SELECT title,metadata_date_raw,body_sha256 FROM articles WHERE url='https://publisher.test/a'").fetchone()
        assert tuple(row) == ('New metadata, not accepted text', '2026-10-11 08:00:00', None)
    finally:
        q.close()


@pytest.mark.parametrize('after', ['', 'done', 'b' * 64])
@pytest.mark.parametrize('matches', [True, False])
def test_legacy_cursor_carries_only_proven_current_policy_and_archives_original(config, tmp_path, after, matches):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        policy = q.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()[0]
        old = {'policy_fingerprint': policy if matches else 'unknown-old-policy', 'after': after}
        raw = json.dumps(old)
        with q.db:
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (KEY, raw))
        result = routing._routing_checkpoint(q)
        assert result['after'] == (after if matches else '')
        proof = json.loads(q.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'original_url_routing_upgrade_v2:%'").fetchone()[0])
        assert proof['previous_checkpoint'] == old and proof['replacement_checkpoint'] == result
        assert proof['legacy_current_policy_matches'] == matches
        assert proof['article_states_deadlines_leases_or_bodies_changed'] is False
        assert routing._routing_checkpoint(q) == result
        assert q.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'original_url_routing_upgrade_v2:%'").fetchone()[0] == 1
    finally:
        q.close()


def test_legacy_upgrade_evidence_and_replacement_rollback_together(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        policy = q.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()[0]
        old = json.dumps({'policy_fingerprint': policy, 'after': 'done'})
        with q.db:
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (KEY, old))
            q.db.execute("CREATE TRIGGER abort_routing_upgrade BEFORE INSERT ON queue_settings "
                         "WHEN NEW.key='google_literal_url_routing_v1' BEGIN SELECT RAISE(ABORT,'upgrade failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='upgrade failure'):
            routing._routing_checkpoint(q)
        assert q.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0] == old
        assert q.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'original_url_routing_upgrade_v2:%'").fetchone()[0] == 0
    finally:
        q.close()


def seed(queue, monkeypatch):
    # Canonical retained URL proofs, without pretending these controlled
    # fixture heads are real publisher receipts or downloaded historical text.
    with queue.db:
        queue.add_articles([
            {**alias(f'https://publisher.test/a{i % 4}'), 'url': alias(f'https://publisher.test/a{i % 4}')['url'] + f'&hl={i}',
             'title': '' if i % 5 == 0 else f'Metadata {i}', 'date': f'2014-02-{i + 1:02} 08:00:00'}
            for i in range(12)] + [alias(f'https://unreviewed.test/u{i}') for i in range(12)], origin_id=27)
        queue.add_articles([{'url': 'https://publisher.test/a0'}, {'url': 'https://unreviewed.test/u0'}])
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=9999999999,error_code='http_403',"
                         "lease_owner='other',lease_until=9999999999,body_sha256=?,body_bytes=9 "
                         "WHERE url='https://publisher.test/a0'", ('a' * 64,))
    with monkeypatch.context() as isolated:
        isolated.setattr(routing, '_route_literal', lambda *args: False)
        while routing.backfill_original_urls(queue)['examined']:
            pass


def snapshot(queue):
    return {table: [tuple(row) for row in queue.db.execute('SELECT * FROM ' + table + ' ORDER BY 1,2')]
            for table in ('articles', 'source_links', 'original_url_resolutions', 'original_url_metrics',
                          'original_url_host_metrics', 'state_metrics', 'stored_body_metrics', 'hosts')}


def test_grouped_lookup_matches_ordered_oracle_with_duplicates_rights_and_failures(config, tmp_path, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_queue.utc_now', lambda: '2026-10-11T06:30:00+00:00')
    reference = NewsQueue(tmp_path / 'reference', PolicyRegistry(config, base=tmp_path))
    candidate = NewsQueue(tmp_path / 'candidate', PolicyRegistry(config, base=tmp_path))
    try:
        seed(reference, monkeypatch)
        seed(candidate, monkeypatch)
        assert snapshot(reference) == snapshot(candidate)
        traces = {'reference': [], 'candidate': []}
        for name, q in [('reference', reference), ('candidate', candidate)]:
            q.db.set_trace_callback(traces[name].append)
        with reference.db:
            for row in reference.db.execute('SELECT r.*,a.title,a.metadata_date_raw FROM original_url_resolutions r '
                'JOIN articles a ON a.id=r.alias_id WHERE r.contract_version=1 AND r.outcome=\'literal_original\' ORDER BY r.alias_id').fetchall():
                routing._route_literal(reference, row, row)
        assert routing.sync_original_url_routes(candidate, max_seconds=.1) == 24
        assert snapshot(reference) == snapshot(candidate)
        assert sum(s.startswith('SELECT 1 FROM articles WHERE id=') for s in traces['reference']) == 24
        assert sum(s.startswith('SELECT id FROM articles WHERE id IN (') for s in traces['candidate']) == 1
        protected = candidate.db.execute("SELECT state,due,error_code,lease_owner,lease_until,body_sha256 FROM articles "
                                         "WHERE url='https://publisher.test/a0'").fetchone()
        assert tuple(protected) == ('retry_cooldown', 9999999999., 'http_403', 'other', 9999999999., 'a' * 64)
        assert candidate.db.execute('SELECT count(*) FROM source_links').fetchone()[0] == 24
    finally:
        reference.close()
        candidate.close()


def test_stopped_slice_never_advances_or_changes_target(config, tmp_path, monkeypatch):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        seed(q, monkeypatch)
        before, cursor = snapshot(q), checkpoint(q)
        stop = threading.Event()
        stop.set()
        assert routing.sync_original_url_routes(q, stop=stop) == 0
        assert snapshot(q) == before and checkpoint(q) == cursor
    finally:
        q.close()


def test_grouped_target_update_rolls_back_alias_upserts_and_cursor(config, tmp_path, monkeypatch):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        seed(q, monkeypatch)
        before, cursor = snapshot(q), checkpoint(q)
        with q.db:
            q.db.execute("CREATE TRIGGER abort_route_target BEFORE UPDATE OF target_queued ON original_url_resolutions "
                         "BEGIN SELECT RAISE(ABORT,'route target failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='route target failure'):
            routing.sync_original_url_routes(q, max_seconds=.1)
        assert snapshot(q) == before and checkpoint(q) == cursor
    finally:
        q.close()


@pytest.mark.parametrize('raw,version,valid', [
    ('{broken', None, False), ('{}', None, False),
    ('{"policy_fingerprint":"legacy","after":"done"}', 1, True),
    ('{"contract_version":2,"routing_fingerprint":"' + 'a' * 64 + '","after":"done"}', 2, True),
])
def test_readonly_progress_uses_durable_marker_not_owner_cache(config, tmp_path, raw, version, valid):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (KEY, raw))
        changes = q.db.total_changes
        view = SimpleNamespace(db=q.db, _original_url_started=q._original_url_started,
                               _original_url_start_count=q._original_url_start_count)
        progress = routing.original_url_progress(view)
        assert progress['routing_contract_version'] == version and progress['routing_checkpoint_valid'] is valid
        assert progress['routing_fingerprint'] == ('a' * 64 if version == 2 else None)
        assert q.db.total_changes == changes
        assert q.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0] == raw
    finally:
        q.close()


def test_large_metadata_slice_respects_bind_bound_and_preserves_unreviewed_missing_targets(config, tmp_path, monkeypatch):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([alias(f'https://unreviewed.test/u{i}') for i in range(801)])
        while routing.backfill_original_urls(q)['examined']:
            pass
        statements = []
        q.db.set_trace_callback(statements.append)
        # Explicit virtual clock for batch-size semantics, not a speed claim.
        monkeypatch.setattr(routing.time, 'monotonic', lambda: 0.)
        assert routing.sync_original_url_routes(q, limit=1024, max_seconds=.1) == 801
        seeks = [s for s in statements if s.startswith('SELECT id FROM articles WHERE id IN (')]
        assert len(seeks) == 3 and all(s.count("x'") <= 400 for s in seeks)
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 801
        assert q.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 0
        assert routing.original_url_progress(q)['body_acceptance_proven'] is False
    finally:
        q.close()
