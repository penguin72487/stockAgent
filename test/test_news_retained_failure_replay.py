"""Finite original-response reuse shares durable acceptance, never HTTP."""
from contextlib import closing
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_failure_focus import configure_focus
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_replay import replay_retained_cnyes_failures
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, short_page
from test_news_failure_diagnostics import fail, reports
from test_news_failure_focus import spec


def prepare(queue, monkeypatch, *, payload=None, advance=True):
    configure_focus(queue, spec())
    fail(queue, Response(URL, 200, {'content-type': 'text/html'}, payload or short_page()))
    original = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    ledger = queue.failure_diagnostic_focus.progress()
    if advance:
        original_time = time.time
        monkeypatch.setattr(time, 'time', lambda: original_time() + 4000)
    return original, ledger


def test_due_original_is_materialized_once_without_http_or_new_diagnostic_budget(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        original, ledger = prepare(queue, monkeypatch)
        paths, evidence = reports(tmp_path)
        immutable = {str(path): sha256_file(path) for path in (*paths, *(p.with_suffix('.receipt.json') for p in paths))}
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == result['responses_checked'] == 1 and result['http_requests'] == 0
        head = queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()
        assert head['state'] == 'complete' and head['captured_at_utc'] == evidence[0]['captured_at_utc']
        assert head['last_checked_at_utc'] == original['last_checked_at_utc']
        record = pq.read_table(tmp_path / head['parquet_path']).to_pylist()[0]
        assert record['acquisition_transport'] == 'retained_raw_reparse'
        assert record['source_observed_at_utc'] == evidence[0]['captured_at_utc']
        assert record['http_duration_seconds'] is None and not record['historical_point_in_time']
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert all(sha256_file(path) == digest for path, digest in immutable.items())
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 0
        status = queue.status()
        assert status['provider_stored_body_counter_contract_version'] == 1
        assert status['providers']['cnyes']['stored_body_urls'] == status['articles_with_stored_body'] == 1
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=? WHERE url=?", (time.time() + 4000, URL))
        assert queue.status()['providers']['cnyes']['stored_body_urls'] == 1
        assert queue.status()['providers']['cnyes']['states'] == {'retry': 1}
        assert verify_bodies(tmp_path)['accepted']
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        configure_focus(restarted, spec())
        assert replay_retained_cnyes_failures(restarted)['accepted_heads'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1


def test_future_due_is_not_reset_or_checkpointed(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, ledger = prepare(queue, monkeypatch, advance=False)
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0


@pytest.mark.parametrize('state', ['inflight', 'source_not_found', 'paywall_or_login', 'article_identity_not_verified'])
def test_newer_lease_or_terminal_source_outcome_is_never_overwritten(cnyes_config, tmp_path, monkeypatch, state):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original


def test_newer_http_check_blocks_stale_original_even_after_due(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 0 and result['responses_checked'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.db.execute('SELECT state FROM retained_failure_replays').fetchone()[0] == 'failed_validation'


def test_unresolved_local_parser_does_not_add_retry_attempt_or_reset_error(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, _ = prepare(queue, monkeypatch, payload=short_page(visible='source text does not match schema'))
        result = replay_retained_cnyes_failures(queue)
        assert result['responses_checked'] == 1 and result['accepted_heads'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        assert queue.db.execute('SELECT state FROM retained_failure_replays').fetchone()[0] == 'parse_unresolved'


def test_corrupt_pair_keeps_original_proof_and_failure_head(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, _ = prepare(queue, monkeypatch)
        path = reports(tmp_path)[0][0].with_suffix('.receipt.json')
        value = json.loads(path.read_bytes())
        value['sha256'] = '0' * 64
        path.write_text(json.dumps(value))  # Synthetic corruption injection only.
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 0 and result['responses_checked'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.db.execute('SELECT state FROM retained_failure_replays').fetchone()[0] == 'failed_validation'


def test_durable_receipt_recovers_after_commit_failure_under_same_head(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        prepare(queue, monkeypatch)
        def fail_commit(*args):
            raise OSError('synthetic SQLite commit interruption')
        monkeypatch.setattr(queue, '_accept_records', fail_commit)
        with pytest.raises(OSError):
            replay_retained_cnyes_failures(queue)
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0
        assert queue.db.execute('SELECT state,body_sha256 FROM articles').fetchone()[:] == ('inflight', None)
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        assert restarted.db.execute('SELECT state FROM articles').fetchone()[0] == 'complete'
        assert verify_bodies(tmp_path)['accepted']


def test_recovered_receipt_cannot_overwrite_later_source_removal(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        prepare(queue, monkeypatch)
        def fail_commit(*args):
            raise OSError('synthetic SQLite commit interruption')
        monkeypatch.setattr(queue, '_accept_records', fail_commit)
        with pytest.raises(OSError):
            replay_retained_cnyes_failures(queue)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='source_body_removed',error_code='source_body_removed',lease_owner=NULL,lease_until=NULL")
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        row = restarted.db.execute('SELECT state,body_sha256 FROM articles').fetchone()
        assert row[:] == ('source_body_removed', None)
        assert dict(restarted.db.execute('SELECT key,value FROM counters'))['retained_failure_superseded_at_commit'] == 1


@pytest.mark.parametrize('options', [{'maximum_responses': True}, {'maximum_responses': 17},
    {'maximum_wall_seconds': float('nan')}, {'maximum_wall_seconds': True}, {'maximum_raw_bytes': True},
    {'maximum_raw_bytes': 17 << 20}])
def test_replay_has_explicit_resource_guards(cnyes_config, tmp_path, monkeypatch, options):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with pytest.raises(ValueError):
            replay_retained_cnyes_failures(queue, **options)


@pytest.mark.parametrize('value', [True, False, 'true', None, 1])
def test_runtime_opt_in_is_typed_and_bound_to_reviewed_focus(cnyes_config, tmp_path, value):
    cnyes_config.pop('provider_catalog', None)
    cnyes_config.update(failure_diagnostic_focus=spec(), failure_diagnostic_metadata_identity_upgrade=False,
                        retained_cnyes_failure_replay_enabled=value)
    path = tmp_path / 'candidate.json'
    atomic_write_json(path, cnyes_config)
    if type(value) is bool:
        config, _, _ = load_config(path)
        assert config['retained_cnyes_failure_replay_enabled'] is value
    else:
        with pytest.raises(ValueError, match='retained_cnyes_failure_replay_enabled'):
            load_config(path)
    if value is True:
        cnyes_config['failure_diagnostic_focus'] = None
        atomic_write_json(path, cnyes_config)
        with pytest.raises(ValueError, match='retained failure replay requires'):
            load_config(path)
