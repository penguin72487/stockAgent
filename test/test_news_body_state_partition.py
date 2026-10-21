"""Synthetic joint state/body counts: no source requests or invented bodies."""
from contextlib import closing
import json
import sqlite3

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_status import initialize_article_body_states
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import NOW, _seed, _save
from test_news_fulltext import config


def assert_joint_counts(db):
    expected = [tuple(row) for row in db.execute('''SELECT provider,state,count(*),
        coalesce(sum(body_bytes),0),sum(body_sha256 IS NOT NULL) FROM articles
        GROUP BY provider,state ORDER BY provider,state''')]
    actual = [tuple(row) for row in db.execute('''SELECT provider,state,articles,body_bytes,stored_body_urls
        FROM state_metrics WHERE articles>0 ORDER BY provider,state''')]
    assert actual == expected


def test_state_and_body_mutations_share_source_transactions_and_rollback(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/{index}'} for index in range(4)])
            queue.db.execute("UPDATE articles SET body_sha256='synthetic',body_bytes=0 WHERE url LIKE '%/0'")
            queue.db.execute("UPDATE articles SET body_sha256='other',body_bytes=9 WHERE url LIKE '%/1'")
        assert_joint_counts(queue.db)
        # A body revision is not a new URL head. A rejected recheck retains it.
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',body_sha256='revision',due=9999999999 "
                             "WHERE url LIKE '%/0'")
            queue.db.execute("UPDATE articles SET provider='peer',state='retry',body_bytes=NULL WHERE url LIKE '%/1'")
        assert_joint_counts(queue.db)
        status = queue.status(activity='running')
        assert status['provider_body_state_counter_contract_version'] == 1
        assert status['providers']['publisher']['body_state_counts'] == {
            'missing': {'pending': 2}, 'stored': {'article_identity_not_verified': 1}}
        assert status['providers']['peer']['body_state_counts'] == {'missing': {}, 'stored': {'retry': 1}}
        assert status['eligible_missing_body_articles'] == 2
        assert status['eligible_stored_body_recheck_articles'] == 1
        before = [tuple(row) for row in queue.db.execute('SELECT * FROM state_metrics ORDER BY provider,state')]
        with pytest.raises(RuntimeError, match='synthetic rollback'):
            with queue.db:
                queue.db.execute("UPDATE articles SET body_sha256=NULL,state='body_not_found'")
                assert_joint_counts(queue.db)
                raise RuntimeError('synthetic rollback')
        assert before == [tuple(row) for row in queue.db.execute('SELECT * FROM state_metrics ORDER BY provider,state')]
        assert_joint_counts(queue.db)
        with queue.db:
            queue.db.execute("UPDATE articles SET body_sha256=NULL WHERE provider='peer'")
            queue.db.execute("DELETE FROM articles WHERE url LIKE '%/0'")
            queue.db.execute("UPDATE articles SET body_bytes=12 WHERE provider='peer'")
        assert_joint_counts(queue.db)
        assert queue.db.execute('SELECT sum(stored_body_urls) FROM state_metrics').fetchone()[0] == 0


def legacy_db():
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE articles(id INTEGER PRIMARY KEY,provider TEXT,state TEXT,body_bytes INTEGER,body_sha256 TEXT);
        CREATE TABLE state_metrics(provider TEXT,state TEXT,articles INTEGER,body_bytes INTEGER,PRIMARY KEY(provider,state)) WITHOUT ROWID;
        CREATE TABLE queue_settings(key TEXT PRIMARY KEY,value TEXT);
        INSERT INTO articles VALUES (1,'a','pending',0,NULL),(2,'a','retry',4,'sha'),(3,'b','body_not_found',NULL,'old');
        INSERT INTO state_metrics VALUES ('a','pending',1,0),('a','retry',1,4),('b','body_not_found',1,0);''')
    return db


def migration_snapshot(db):
    return {'schema': list(db.execute("SELECT name,sql FROM sqlite_master ORDER BY name")),
            'counts': list(db.execute('SELECT * FROM state_metrics ORDER BY provider,state')),
            'source': list(db.execute('SELECT * FROM articles ORDER BY id')),
            'settings': list(db.execute('SELECT * FROM queue_settings ORDER BY key'))}


@pytest.mark.parametrize('failure_after', ['ALTER TABLE', 'CREATE TRIGGER metrics_update',
    'INSERT INTO state_metrics(provider,state,articles,body_bytes,stored_body_urls)\n            SELECT',
    'INSERT OR REPLACE INTO queue_settings'])
def test_legacy_column_trigger_seed_and_marker_are_one_crash_atomic_generation(failure_after):
    with closing(legacy_db()) as db:
        before = migration_snapshot(db)

        class InterruptedConnection:
            def execute(self, statement, *args):
                result = db.execute(statement, *args)
                if statement.startswith(failure_after):
                    raise OSError('synthetic migration interruption')
                return result

            def __enter__(self):
                db.__enter__()
                return self

            def __exit__(self, *args):
                return db.__exit__(*args)

        with pytest.raises(OSError, match='synthetic migration interruption'):
            initialize_article_body_states(InterruptedConnection())
        assert migration_snapshot(db) == before
        initialize_article_body_states(db)
        assert_joint_counts(db)
        assert list(db.execute('SELECT * FROM articles ORDER BY id')) == before['source']


def test_restart_and_heartbeat_never_rescan_history_to_partition_body_states(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/one'}])
            queue.db.execute("UPDATE articles SET body_sha256='synthetic',state='body_not_found'")
        statements = []
        queue.db.set_trace_callback(statements.append)
        initialize_article_body_states(queue.db)
        queue.status()
        assert not any('FROM articles' in sql or 'INTO state_metrics' in sql for sql in statements)
        queue.db.set_trace_callback(None)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as restarted:
        assert_joint_counts(restarted.db)
        assert restarted.status()['providers']['publisher']['body_state_counts'] == {
            'missing': {}, 'stored': {'body_not_found': 1}}


def test_whole_status_readonly_verifier_checks_native_counts_without_restarting_owner(config, tmp_path, monkeypatch):
    from downloader.news_fulltext_http import NewsHTTP
    from scripts.benchmark_news_status_projection import profile_current_status
    from downloader.artifact_io import sha256_file
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/one'}])
            queue.db.execute("UPDATE articles SET body_sha256='synthetic',state='body_not_found'")
        queue.status(activity='running')
        before = migration_snapshot(queue.db)
        status_sha = sha256_file(tmp_path / 'status.json')

        def no_owner(*args, **kwargs):
            raise AssertionError('read-only verifier cannot start a source owner')

        monkeypatch.setattr(NewsQueue, '__init__', no_owner)
        monkeypatch.setattr(NewsHTTP, '__init__', no_owner)
        result = profile_current_status(tmp_path, queue.registry, 3, verify_body_states=True)
        assert result['accepted'] and result['body_state_verification']['accepted']
        assert result['body_state_verification']['live_transactional_counters_verified']
        assert result['body_state_verification']['source_scans'] == 1
        assert result['body_state_verification']['known_url_heads'] == result['body_state_verification']['stored_body_url_heads'] == 1
        assert result['publisher_requests'] == 0 and not result['status_published']
        assert not result['body_state_verification']['schema_migrated']
        assert migration_snapshot(queue.db) == before
        assert sha256_file(tmp_path / 'status.json') == status_sha
        with queue.db:
            queue.db.execute('UPDATE state_metrics SET stored_body_urls=0')
        with pytest.raises(ValueError, match='article_body_state_counters_disagree'):
            profile_current_status(tmp_path, queue.registry, 3, verify_body_states=True)


def partition_snapshot(root):
    status = _seed(root)
    status['provider_stored_body_counter_contract_version'] = 1
    status['provider_body_state_counter_contract_version'] = 1
    source = status['providers']['cnyes']
    source.update(states={'complete': 2, 'pending': 4, 'article_identity_not_verified': 13},
                  stored_body_urls=17, body_state_counts={
                      'missing': {'pending': 2},
                      'stored': {'complete': 2, 'pending': 2, 'article_identity_not_verified': 13}})
    status['providers']['unregistered:example.org'].update(stored_body_urls=0, body_state_counts={
        'missing': {'awaiting_authorization': 4}, 'stored': {}})
    status.update(unique_articles=23, articles_with_stored_body=17,
                  states={**source['states'], 'awaiting_authorization': 4})
    status['performance']['providers']['cnyes']['counts']['durably_accepted_new_url_heads'] = 12
    return status


def test_retained_identity_failures_are_not_missing_and_eta_excludes_rechecks(tmp_path):
    status = partition_snapshot(tmp_path)
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent']
    source = next(item for item in result['providers'] if item['id'] == 'cnyes')
    coverage = source['body_coverage']
    assert source['remaining_urls'] == 4  # Retained legacy request-work count.
    assert coverage['missing_body_urls'] == coverage['queued_missing_body_urls'] == 2
    assert coverage['stored_body_urls'] == 17 and coverage['queued_stored_body_recheck_urls'] == 2
    assert coverage['retained_body_noncomplete_urls'] == 15
    assert coverage['missing_body_outcomes']['categories']['repair_required'] == 0
    assert coverage['retained_body_outcomes']['categories']['repair_required'] == 13
    assert source['conditional_eta_seconds'] == 20 and source['conditional_eta_scope'] == 'queued_missing_body_urls'
    assert result['body_coverage']['missing_body_urls'] == 6
    for key, total in (('missing_body_outcomes', 6), ('retained_body_outcomes', 17)):
        assert sum(result['body_coverage'][key]['categories'].values()) == total
    assert result['acquisition']['progress_ratio'] == 17 / 23
    assert all(secret not in json.dumps(result) for secret in ('PRIVATE_TOKEN', 'PRIVATE_HTML', str(tmp_path)))
    from scripts.verify_news_dashboard import body_state_inventory_check
    assert body_state_inventory_check(result)['provider_keys'] == 2


@pytest.mark.parametrize('change', [
    lambda status: status.update(provider_body_state_counter_contract_version=True),
    lambda status: status.update(provider_body_state_counter_contract_version=2),
    lambda status: status.update(provider_stored_body_counter_contract_version=None),
    lambda status: status['providers']['cnyes'].pop('body_state_counts'),
    lambda status: status['providers']['cnyes']['body_state_counts']['stored'].update(pending=3),
    lambda status: status['providers']['cnyes']['body_state_counts']['missing'].update(pending=True),
    lambda status: status['providers']['cnyes']['body_state_counts']['stored'].update(article_identity_not_verified=-1),
    lambda status: status['providers']['cnyes']['body_state_counts'].update(private='PRIVATE_BODY'),
])
def test_unreconciled_joint_counts_fail_closed_without_false_eta_or_zero(tmp_path, change):
    status = partition_snapshot(tmp_path)
    change(status)
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['health'] == 'degraded' and not result['snapshot_consistent']
    assert result['body_coverage'] is None
    assert result['acquisition']['eligible_eta_seconds'] is result['acquisition']['progress_ratio'] is None
    assert all(source['conditional_eta_seconds'] is None for source in result['providers'])
    assert 'PRIVATE_BODY' not in json.dumps(result)


def test_legacy_marginal_counts_cannot_reconstruct_joint_missingness(tmp_path):
    status = partition_snapshot(tmp_path)
    del status['provider_body_state_counter_contract_version']
    for source in status['providers'].values():
        del source['body_state_counts']
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent'] and result['body_coverage'] is None
    assert all(source['body_coverage'] is None for source in result['providers'])
    from scripts.verify_news_dashboard import body_state_inventory_check
    with pytest.raises(AssertionError):
        body_state_inventory_check(result)


def test_private_unknown_states_are_grouped_not_dropped_or_exposed(tmp_path):
    status = partition_snapshot(tmp_path)
    source = status['providers']['cnyes']
    source['states']['PRIVATE_STATE_NAME'] = 3
    source['body_state_counts']['missing']['PRIVATE_STATE_NAME'] = 1
    source['body_state_counts']['stored']['PRIVATE_STATE_NAME'] = 2
    source['stored_body_urls'] += 2
    status['unique_articles'] += 3
    status['articles_with_stored_body'] += 2
    status['states']['PRIVATE_STATE_NAME'] = 3
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent']
    assert result['body_coverage']['missing_body_states']['other'] == 1
    assert result['body_coverage']['retained_body_states']['other'] == 2
    assert 'PRIVATE_STATE_NAME' not in json.dumps(result)


@pytest.mark.parametrize('blocked_missing', [False, True])
def test_only_retained_rechecks_do_not_become_missing_work_or_false_completion(tmp_path, blocked_missing):
    status = partition_snapshot(tmp_path)
    source = status['providers']['cnyes']
    source['states']['pending'] = 2
    source['body_state_counts']['missing'] = {'body_not_found': 2} if blocked_missing else {}
    if blocked_missing:
        source['states']['body_not_found'] = 2
    else:
        status['unique_articles'] -= 2
    status['states'] = {**source['states'], 'awaiting_authorization': 4}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    projected = next(item for item in result['providers'] if item['id'] == 'cnyes')
    assert result['snapshot_consistent']
    assert projected['state'] == 'rechecking'
    assert projected['body_coverage']['queued_missing_body_urls'] == 0
    assert projected['conditional_eta_seconds'] == (None if blocked_missing else 0)


def test_configured_empty_and_literal_only_sources_are_valid_zero_not_missing(tmp_path):
    status = partition_snapshot(tmp_path)
    configuration = json.loads((tmp_path / 'configs/news_fulltext.json').read_bytes())
    configuration['providers'].append({'id': 'empty', 'name': 'Empty', 'enabled': False})
    atomic_write_json(tmp_path / 'configs/news_fulltext.json', configuration)
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['snapshot_consistent']
    empty = next(item for item in result['providers'] if item['id'] == 'empty')
    assert empty['body_coverage']['missing_body_urls'] == empty['body_coverage']['stored_body_urls'] == 0
    assert empty['body_coverage']['missing_body_states'] == empty['body_coverage']['retained_body_states'] == {}
