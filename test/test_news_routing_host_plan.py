"""Exact eligibility deltas, interrupted scopes, migration and idle cost."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import sqlite3
import threading

import pytest

from downloader import news_fulltext_routing as plan
from downloader import news_fulltext_urls as routing
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_original_queue import alias


KEY = 'google_literal_url_routing_v1'


def finish(queue, **kwargs):
    examined = 0
    for _ in range(100):
        examined += routing.sync_original_url_routes(queue, **kwargs)
        if json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0])['after'] == 'done':
            return examined
    pytest.fail('host routing failed to converge')


def retained(queue, urls):
    with queue.db:
        queue.add_articles([alias(url) for url in urls])
    while routing.backfill_original_urls(queue)['examined']:
        pass


def state(queue):
    return {table: [tuple(r) for r in queue.db.execute('SELECT * FROM ' + table + ' ORDER BY 1,2')]
            for table in ('articles', 'original_url_resolutions', 'source_links', 'original_url_host_metrics',
                          'original_url_metrics', plan.TABLE)}


def change(queue, provider='publisher', **fields):
    queue.registry.sources[provider] = replace(queue.registry.sources[provider], **fields)
    routing.initialize_original_urls(queue)


def test_completed_unaffected_hosts_are_not_revisited(config, tmp_path, monkeypatch):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://publisher.test/b',
                     'https://unregistered.test/x', 'https://other.unregistered.test/y'])
        assert finish(q) == 4
        with q.db:
            q.db.execute("UPDATE articles SET state='retry_cooldown',due=9999999999,error_code='http_403',"
                         "lease_owner='live-other',lease_until=9999999999,body_sha256=?,body_bytes=17 "
                         "WHERE url='https://publisher.test/a'", ('a' * 64,))
        protected = q.db.execute("SELECT state,due,error_code,lease_owner,lease_until,body_sha256,body_bytes "
                                 "FROM articles WHERE url='https://publisher.test/a'").fetchone()[:]
        change(q, authorization_fingerprint='c' * 64)
        statements = []
        q.db.set_trace_callback(statements.append)
        monkeypatch.setattr(routing, 'resolve_google_literal_url', lambda _: pytest.fail('cached envelope redecoded'))
        assert finish(q) == 2
        seeks = [s for s in statements if s.startswith('SELECT r.*,a.title')]
        assert seeks and all("r.original_host='publisher.test'" in s for s in seeks)
        assert all('INDEXED BY original_url_host' in s for s in seeks)
        assert q.db.execute("SELECT state,due,error_code,lease_owner,lease_until,body_sha256,body_bytes "
                            "FROM articles WHERE url='https://publisher.test/a'").fetchone()[:] == protected
        assert q.db.execute('SELECT count(*) FROM source_links').fetchone()[0] == 0
        assert routing.original_url_progress(q)['body_acceptance_proven'] is False
    finally:
        q.close()


@pytest.mark.parametrize('field,value', [('selectors', ('.changed',)), ('parallelism', 4),
                                       ('data_fingerprint', 'd' * 64)])
def test_body_or_transport_changes_do_not_create_eligibility_work(config, tmp_path, field, value):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://outside.test/x'])
        finish(q)
        change(q, **{field: value})
        assert finish(q) == 0
    finally:
        q.close()


def test_new_provider_routes_only_its_existing_originals(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://new.test/one', 'https://new.test/two',
                     'https://untouched.test/no'])
        finish(q)
        q.registry.sources['new'] = replace(q.registry.sources['publisher'], provider='new', domains=('new.test',))
        # The real restart builds this registry index from current config.
        del q.registry._domain_providers
        routing.initialize_original_urls(q)
        assert finish(q) == 2
        assert q.db.execute("SELECT count(*) FROM articles WHERE provider='new'").fetchone()[0] == 2
        assert q.db.execute("SELECT count(*) FROM articles WHERE host='untouched.test'").fetchone()[0] == 0
    finally:
        q.close()


def test_domain_child_reassignment_uses_exact_longest_domain_match(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://child.publisher.test/b',
                     'https://deeper.child.publisher.test/c', 'https://unrelated.test/x'])
        finish(q)
        q.registry.sources['child'] = replace(q.registry.sources['publisher'], provider='child',
                                               domains=('child.publisher.test',), allowed=False)
        del q.registry._domain_providers
        routing.initialize_original_urls(q)
        assert finish(q) == 2
        rows = q.db.execute('SELECT original_host,complete FROM ' + plan.TABLE + ' ORDER BY original_host').fetchall()
        assert len(rows) == 4 and all(r[1] == 1 for r in rows)
    finally:
        q.close()


def test_extra_domain_does_not_replay_unchanged_effective_source(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://new-domain.test/b', 'https://unrelated.test/c'])
        finish(q)
        change(q, domains=('publisher.test', 'new-domain.test'))
        del q.registry._domain_providers
        assert finish(q) == 1
        assert q.db.execute("SELECT state FROM articles WHERE url='https://new-domain.test/b'").fetchone()[0] == 'pending'
    finally:
        q.close()


@pytest.mark.parametrize('field,value', [('allowed', False), ('authorization_fingerprint', 'a' * 64),
                                       ('article_path_pattern', '^/a$'), ('acquisition_mode', 'finite')])
def test_effective_source_eligibility_changes_replay_only_that_host(config, tmp_path, field, value):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://publisher.test/b', 'https://unrelated.test/x'])
        finish(q)
        change(q, **{field: value})
        assert finish(q) == 2
    finally:
        q.close()


def test_partial_host_resume_and_midpass_change_keep_unaffected_prefix(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    try:
        retained(q, [f'https://publisher.test/a{i}' for i in range(8)] + ['https://other.test/z'])
        assert routing.sync_original_url_routes(q, limit=3, max_seconds=.1) == 3
        before = json.loads(q.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0])
    finally:
        q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert json.loads(q.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0]) == before
        assert all(r['after'].hex() == before['after'] for r in q.db.execute('SELECT * FROM ' + plan.TABLE))
        # The pending publisher prefix is unaffected by an unrelated source.
        q.registry.sources['irrelevant'] = replace(q.registry.sources['publisher'], provider='irrelevant',
                                                  domains=('unused.test',))
        del q.registry._domain_providers
        routing.initialize_original_urls(q)
        assert finish(q, limit=2, max_seconds=.1) == 6
    finally:
        q.close()


@pytest.mark.parametrize('matching', [True, False])
@pytest.mark.parametrize('position', ['prefix', 'done'])
def test_unpartitioned_v2_migration_inherits_only_matching_global_coverage(config, tmp_path, matching, position):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, [f'https://publisher.test/a{i}' for i in range(5)] + [f'https://other.test/b{i}' for i in range(4)])
        proofs = q.db.execute('SELECT alias_id FROM original_url_resolutions ORDER BY alias_id').fetchall()
        after = proofs[3][0].hex() if position == 'prefix' else 'done'
        fingerprint = routing.routing_fingerprint(q.registry) if matching else 'f' * 64
        old = {'contract_version': 2, 'routing_fingerprint': fingerprint, 'after': after}
        with q.db:
            q.db.execute('DELETE FROM ' + plan.TABLE)
            q.db.execute('DELETE FROM queue_settings WHERE key=?', (plan.INPUTS,))
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (KEY, json.dumps(old)))
        routing.initialize_original_urls(q)
        expected = (0 if position == 'done' else 5) if matching else 9
        assert finish(q, max_seconds=.1) == expected
        transitions = [json.loads(r[0]) for r in q.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'original_url_route_transition_v1:%'")]
        assert any(r['previous_marker'] == old and r['legacy_global_prefix_inherited'] is matching for r in transitions)
    finally:
        q.close()


@pytest.mark.parametrize('raw', ['{broken', '{}', '[]', '{"sources":[["p",[{}],true,"x","",[]]]}'])
def test_corrupt_plan_inputs_replay_and_preserve_exact_bad_evidence(config, tmp_path, raw):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://unregistered.test/x'])
        finish(q)
        with q.db:
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (plan.INPUTS, raw))
        routing.initialize_original_urls(q)
        assert finish(q) == 2
        assert q.db.execute('SELECT original_value FROM original_url_checkpoint_recoveries WHERE setting_key=?',
                            (plan.INPUTS,)).fetchone()[0] == raw
    finally:
        q.close()


@pytest.mark.parametrize('corrupt_inputs', [False, True])
def test_manifest_and_host_cursor_replacement_are_atomic(config, tmp_path, corrupt_inputs):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://publisher.test/b', 'https://unregistered.test/x'])
        finish(q)
        if corrupt_inputs:
            with q.db:
                q.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('{broken', plan.INPUTS))
        before = state(q)
        settings = [tuple(r) for r in q.db.execute('SELECT * FROM queue_settings ORDER BY key')]
        with q.db:
            q.db.execute('CREATE TRIGGER abort_host_plan BEFORE INSERT ON ' + plan.TABLE +
                         " WHEN NEW.original_host='publisher.test' BEGIN SELECT RAISE(ABORT,'host plan failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='host plan failure'):
            change(q, authorization_fingerprint='e' * 64)
        assert state(q) == before
        assert [tuple(r) for r in q.db.execute('SELECT * FROM queue_settings ORDER BY key')] == settings
        assert q.db.execute('SELECT count(*) FROM original_url_checkpoint_recoveries').fetchone()[0] == 0
    finally:
        q.close()


def test_failed_route_rolls_back_new_host_cursors_and_metadata_together(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://publisher.test/b'])
        # Materialize the new host plan, but do not examine a routing slice.
        routing.initialize_original_urls(q)
        before = state(q)
        setting = q.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0]
        with q.db:
            q.db.execute('UPDATE original_url_resolutions SET target_queued=0')
        before = state(q)
        with q.db:
            q.db.execute("CREATE TRIGGER abort_route_host_target BEFORE UPDATE OF target_queued ON original_url_resolutions "
                         "BEGIN SELECT RAISE(ABORT,'route target failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='route target failure'):
            routing.sync_original_url_routes(q)
        assert state(q) == before
        assert q.db.execute('SELECT value FROM queue_settings WHERE key=?', (KEY,)).fetchone()[0] == setting
    finally:
        q.close()


@pytest.mark.parametrize('corruption', ['missing_row', 'invalid_prefix', 'wrong_source'])
def test_restart_repairs_only_unproven_host_coverage(config, tmp_path, corruption):
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    try:
        retained(q, ['https://publisher.test/a', 'https://unrelated.test/b'])
        finish(q)
        with q.db:
            if corruption == 'missing_row':
                q.db.execute('DELETE FROM ' + plan.TABLE + " WHERE original_host='publisher.test'")
            elif corruption == 'invalid_prefix':
                q.db.execute('UPDATE ' + plan.TABLE + " SET after=x'01' WHERE original_host='publisher.test'")
            else:
                q.db.execute('UPDATE ' + plan.TABLE + " SET source_fingerprint='unproven' WHERE original_host='publisher.test'")
    finally:
        q.close()
    q = NewsQueue(tmp_path, registry)
    try:
        assert finish(q) == 1
    finally:
        q.close()


def test_new_host_incidence_routes_but_same_host_new_metadata_does_not_replan(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a'])
        finish(q)
        retained(q, ['https://publisher.test/b'])
        assert q.db.execute('SELECT value FROM queue_settings WHERE key=?', (plan.DIRTY,)).fetchone()[0] == '0'
        statements = []
        q.db.set_trace_callback(statements.append)
        assert finish(q) == 0
        assert not any('FROM original_url_host_metrics' in s for s in statements)
        retained(q, ['https://unregistered.test/x'])
        assert q.db.execute('SELECT value FROM queue_settings WHERE key=?', (plan.DIRTY,)).fetchone()[0] == '1'
        assert finish(q) == 1
        assert q.db.execute("SELECT state FROM articles WHERE url='https://publisher.test/b'").fetchone()[0] == 'pending'
    finally:
        q.close()


def test_stop_never_advances_partitioned_coverage(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a'])
        before = state(q)
        stop = threading.Event()
        stop.set()
        assert routing.sync_original_url_routes(q, stop=stop) == 0
        assert state(q) == before
    finally:
        q.close()


def test_host_lookup_plan_is_an_index_range_not_global_scan_or_sort(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(q, ['https://publisher.test/a', 'https://unregistered.test/x'])
        rows = q.db.execute('EXPLAIN QUERY PLAN SELECT r.*,a.title,a.metadata_date_raw FROM original_url_resolutions r '
            'INDEXED BY original_url_host JOIN articles a ON a.id=r.alias_id WHERE r.original_host=? '
            'AND r.original_host IS NOT NULL AND r.contract_version=? AND r.alias_id>? '
            "AND r.outcome='literal_original' ORDER BY r.alias_id LIMIT ?", ('publisher.test', 1, b'', 256)).fetchall()
        assert any('original_url_host (original_host=? AND contract_version=? AND alias_id>?)' in r[3] for r in rows)
        assert all('TEMP B-TREE' not in r[3] and 'SCAN r' not in r[3] for r in rows)
    finally:
        q.close()


def test_host_plan_keeps_exact_legacy_v2_eligibility_fingerprint(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    inputs = [(s.provider, tuple(sorted(s.domains)), s.allowed, s.authorization_fingerprint,
               s.article_path_pattern, s.acquisition_mode)
              for s in sorted(registry.sources.values(), key=lambda s: s.provider)]
    legacy = hashlib.sha256(json.dumps([2, 1, inputs], separators=(',', ':')).encode()).hexdigest()
    assert routing.routing_fingerprint(registry) == legacy


def new_source_behind_large_partial_host(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    retained(queue, [f'https://publisher.test/large{i}' for i in range(24)] + ['https://zz-new.test/one'])
    assert routing.sync_original_url_routes(queue, limit=1, max_seconds=.1) == 1
    queue.registry.sources['new'] = replace(queue.registry.sources['publisher'], provider='new', domains=('zz-new.test',))
    del queue.registry._domain_providers
    routing.initialize_original_urls(queue)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (plan.GEOMETRY,)).fetchone()[0] == 'hosts'
    return queue


@pytest.mark.parametrize('restart', [False, True])
def test_small_new_source_gets_next_host_turn_before_large_neighbour_finishes(config, tmp_path, restart):
    queue = new_source_behind_large_partial_host(config, tmp_path)
    try:
        assert routing.sync_original_url_routes(queue, limit=1, max_seconds=.1) == 1
        turn = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (plan.FAIR_CURSOR,)).fetchone()[0])
        assert turn == {'contract_version': 1, 'after_host': 'publisher.test'}
        registry = queue.registry
        if restart:
            queue.close()
            queue = NewsQueue(tmp_path, registry)
        assert routing.sync_original_url_routes(queue, limit=1, max_seconds=.1) == 1
        assert queue.db.execute("SELECT target_queued FROM original_url_resolutions WHERE original_host='zz-new.test'").fetchone()[0] == 1
        assert queue.db.execute('SELECT complete FROM ' + plan.TABLE + " WHERE original_host='publisher.test'").fetchone()[0] == 0
        assert queue.db.execute("SELECT state,body_sha256 FROM articles WHERE url='https://zz-new.test/one'").fetchone()[:] == ('pending', None)
        remaining = finish(queue, limit=1, max_seconds=.1)
        # One global prefix and two fair turns are already covered; no replay.
        assert remaining == 22
        assert queue.db.execute('SELECT sum(target_queued) FROM original_url_resolutions').fetchone()[0] == 25
        assert queue.db.execute('SELECT count(*) FROM source_links').fetchone()[0] == 0
    finally:
        queue.close()


def test_fair_host_turn_and_coverage_roll_back_with_target_admission(config, tmp_path):
    queue = new_source_behind_large_partial_host(config, tmp_path)
    try:
        assert routing.sync_original_url_routes(queue, limit=1, max_seconds=.1) == 1
        before = state(queue)
        settings = [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')]
        with queue.db:
            queue.db.execute("CREATE TRIGGER abort_new_fair_target BEFORE UPDATE OF target_queued ON original_url_resolutions "
                             "WHEN NEW.original_host='zz-new.test' BEGIN SELECT RAISE(ABORT,'fair target failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='fair target failure'):
            routing.sync_original_url_routes(queue, limit=1, max_seconds=.1)
        assert state(queue) == before
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')] == settings
    finally:
        queue.close()


@pytest.mark.parametrize('raw', ['{broken', '{"contract_version":true,"after_host":"x"}',
                               '{"contract_version":1,"after_host":123}'])
def test_corrupt_fair_turn_archives_value_without_claiming_coverage(config, tmp_path, raw):
    queue = new_source_behind_large_partial_host(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (plan.FAIR_CURSOR, raw))
        assert finish(queue, limit=2, max_seconds=.1) == 24
        assert queue.db.execute('SELECT original_value FROM original_url_checkpoint_recoveries WHERE setting_key=?',
                                (plan.FAIR_CURSOR,)).fetchone()[0] == raw
        assert all(r[0] == 1 for r in queue.db.execute('SELECT complete FROM ' + plan.TABLE))
    finally:
        queue.close()


def test_fair_host_selection_uses_partial_index_range_without_offset_or_sort(config, tmp_path):
    queue = new_source_behind_large_partial_host(config, tmp_path)
    try:
        rows = queue.db.execute('EXPLAIN QUERY PLAN SELECT h.original_host,h.after FROM ' + plan.TABLE +
            ' h INDEXED BY original_route_pending JOIN original_url_host_metrics m '
            'ON m.contract_version=h.contract_version AND m.original_host=h.original_host '
            'WHERE h.contract_version=? AND h.complete=0 AND m.metadata_links>0 '
            'AND h.original_host>? ORDER BY h.original_host LIMIT 1', (1, 'publisher.test')).fetchall()
        assert any('original_route_pending (contract_version=? AND original_host>?)' in r[3] for r in rows)
        assert all('TEMP B-TREE' not in r[3] and 'SCAN h' not in r[3] for r in rows)
    finally:
        queue.close()
