"""Finite Anue failure evidence is not an HTTP budget or article acceptance."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.artifact_io import table_from_records
from downloader.news_fulltext import load_config
from downloader.news_fulltext_failure_focus import configure_focus, validate_focus, FailureEvidenceFocus
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, short_page
from test_news_failure_diagnostics import fail, reports


def spec(urls=None, **changes):
    return {'contract_version': 1, 'cohort_id': 'owned-fixture-v1', 'provider': 'cnyes',
            'urls': [URL] if urls is None else urls, 'maximum_raw_bytes': 256 << 20, **changes}


def response(url=URL, **changes):
    return Response(url, 200, {'content-type': 'text/html'}, short_page(change=changes or None))


def receipt_path(focus):
    return f'diagnostics/20261012/cnyes/focus/{focus.id}/20261012T000000000000Z-abcdefab.receipt.json'


def test_disabled_focus_has_no_tables_or_writes(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, None)
        assert queue.failure_diagnostic_focus is None
        assert not queue.db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'failure_evidence_focus_%'").fetchone()
        fail(queue, response())
        assert len(reports(tmp_path)[0]) == 1 and reports(tmp_path)[1][0]['contract_version'] == 6


def test_one_free_raw_with_independent_receipt_no_article_and_no_daily_charge(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        for _ in range(4):
            fail(queue, response())
        paths, rows = reports(tmp_path)
        assert len(rows) == 1 and rows[0]['raw_response_body'] == response().body
        assert rows[0]['contract_version'] == 7 and rows[0]['budget_period_kind'] == 'finite_cohort'
        assert rows[0]['failure_diagnostic_focus_contract_version'] == 1
        assert rows[0]['budget_contract_version'] == 3 and rows[0]['daily_bucket_attempt_limit'] is None
        assert not rows[0]['accepted_as_article'] and not rows[0]['publish'] and not rows[0]['remote_training_export']
        assert paths[0].with_suffix('.receipt.json').exists()
        assert sha256_file(paths[0]) == json.loads(paths[0].with_suffix('.receipt.json').read_bytes())['sha256']
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()[0] is None
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_%attempts:%'").fetchone()
        progress = queue.failure_diagnostic_focus.progress()
        assert progress['outcomes'] == {'raw_saved': 1} and progress['write_attempts_reserved'] == 1
        assert progress['publisher_requests_added'] == 0
        assert queue.status()['failure_diagnostic_focus']['outcomes'] == {'raw_saved': 1}


def test_opt_in_reservation_survives_boot_and_changed_source_parser(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        before = queue.failure_diagnostic_focus.progress()
    registry.sources['cnyes'] = replace(registry.sources['cnyes'], data_fingerprint='new-parser-not-storage-budget')
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        assert queue.failure_diagnostic_focus.progress() == before
        assert len(reports(tmp_path)[0]) == 1


@pytest.mark.parametrize('changes', [{'payment': 1}, {'payment': False}, {'publishAt': 1}])
def test_focus_never_grants_missing_public_scope(cnyes_config, tmp_path, changes):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        fail(queue, response(**changes))
        row = reports(tmp_path)[1][0]
        assert row['raw_response_body'] is None and row['raw_response_retained'] is False
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'metadata_saved': 1}


@pytest.mark.parametrize('changes', [{'contract_version': True}, {'contract_version': 2},
    {'cohort_id': '../escape'}, {'cohort_id': ''}, {'provider': 'globalvoices'},
    {'urls': []}, {'urls': [URL, URL]}, {'urls': [False]}, {'urls': 'url'},
    {'maximum_raw_bytes': True}, {'maximum_raw_bytes': 0}, {'maximum_raw_bytes': 1 << 30},
    {'ignored_setting': 1}])
def test_unbounded_or_ignored_config_rejected(cnyes_config, tmp_path, changes):
    with pytest.raises(ValueError):
        validate_focus(spec(**changes), PolicyRegistry(cnyes_config, base=tmp_path))


@pytest.mark.parametrize('url', [URL + '?exp=b', URL + '?secret=abc', URL + '#section',
    URL.replace('https:', 'http:'), URL.replace('news.cnyes.com', 'amp-news.cnyes.com'),
    URL.replace('news.cnyes.com', 'example.com'), URL.replace('/42', '/042'),
    URL.replace('news.cnyes.com', 'u:p@news.cnyes.com'), URL + '/', URL + '?exp=%61'])
def test_unreviewed_route_not_finite_retention_permission(cnyes_config, tmp_path, url):
    with pytest.raises(ValueError):
        validate_focus(spec([url]), PolicyRegistry(cnyes_config, base=tmp_path))


def test_manifest_cannot_be_replaced_to_reset_same_generation(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        with pytest.raises(ValueError, match='cannot be overwritten'):
            configure_focus(queue, spec(maximum_raw_bytes=1024))
        assert len(reports(tmp_path)[0]) == 1


def test_new_generation_cannot_save_same_url_twice(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        configure_focus(queue, spec(cohort_id='owned-fixture-v2'))
        fail(queue, response())
        assert len(reports(tmp_path)[0]) == 1
        assert queue.failure_diagnostic_focus.progress()['write_attempts_reserved'] == 0


def test_raw_byte_budget_cannot_overshoot(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(maximum_raw_bytes=len(response().body) - 1))
        fail(queue, response())
        assert not reports(tmp_path)[0] and queue.failure_diagnostic_focus.progress()['write_attempts_reserved'] == 0
        assert queue.db.execute('SELECT error_code FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'body_not_found'


def test_known_storage_failure_has_only_two_normal_response_attempts(cnyes_config, tmp_path, monkeypatch):
    calls = []
    def broken(*args, **kwargs):
        calls.append(True)
        raise OSError('owned storage sink failure')
    monkeypatch.setattr(diagnostics, 'atomic_write_parquet', broken)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        for _ in range(4):
            fail(queue, response())
        assert len(calls) == 2 and not reports(tmp_path)[0]
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'write_failed': 2}
        assert queue.failure_diagnostic_focus.progress()['raw_bytes_reserved'] == 2 * len(response().body)


def test_absent_interrupted_write_reconciles_without_http_then_bounded_retry(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        focus = queue.failure_diagnostic_focus
        token = focus.reserve(URL, len(response().body), hashlib.sha256(response().body).hexdigest(), receipt_path(focus))
        assert token and focus.progress()['outcomes'] == {'reserved': 1}
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'write_failed': 1}
        fail(queue, response())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'write_failed': 1, 'raw_saved': 1}
        assert len(reports(tmp_path)[0]) == 1


def test_partial_write_remains_unknown_unchanged(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        focus = queue.failure_diagnostic_focus
        focus.reserve(URL, len(response().body), hashlib.sha256(response().body).hexdigest(), receipt_path(focus))
        path = focus.receipt_path(receipt_path(focus))
        atomic_write_json(path, {'partial': 'owned evidence'})
        before = sha256_file(path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'reserved': 1}
        assert sha256_file(path) == before and not reports(tmp_path)[0]


def test_durable_pair_lost_finish_is_reconciled_not_overwritten(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        def broken(token, outcome):
            raise OSError('owned lost ledger finish')
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'finish', broken)
        fail(queue, response())
        paths, _ = reports(tmp_path)
        assert len(paths) == 1 and queue.failure_diagnostic_focus.progress()['outcomes'] == {'reserved': 1}
        before = sha256_file(paths[0])
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'raw_saved': 1}
        fail(queue, response())
        assert sha256_file(paths[0]) == before and len(reports(tmp_path)[0]) == 1


def test_duplicate_returns_before_scope_parse_or_file_io(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        monkeypatch.setattr(diagnostics, '_public_cnyes_scope', lambda *_: pytest.fail('duplicate parsed again'))
        monkeypatch.setattr(diagnostics, 'atomic_write_parquet', lambda *_a, **_kw: pytest.fail('duplicate written again'))
        fail(queue, response())
        assert len(reports(tmp_path)[0]) == 1


def test_reservation_counters_and_write_atomicity(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        focus = queue.failure_diagnostic_focus
        queue.db.execute("CREATE TRIGGER deny_focus_counter BEFORE UPDATE ON failure_evidence_focus_cohorts "
            "BEGIN SELECT RAISE(ABORT,'owned counter failure'); END")
        with pytest.raises(Exception, match='owned counter failure'):
            focus.reserve(URL, 1, 'a' * 64, receipt_path(focus))
        assert queue.db.execute('SELECT count(*) FROM failure_evidence_focus_attempts').fetchone()[0] == 0
        assert focus.progress()['write_attempts_reserved'] == 0


def test_only_Cnyes_measurement_identity_changes_source_contracts_unchanged(cnyes_config, tmp_path):
    from pathlib import Path
    from downloader.news_fulltext_capacity import PublisherCapacity
    from downloader.news_fulltext_http import NewsHTTP
    from test_news_fulltext import FakeSession, FakeLimiter
    cfg, registry, _ = load_config(Path('configs/news_fulltext.json'))
    http = NewsHTTP(registry, cfg, tmp_path, session_factory=lambda: FakeSession({}),
                    address_check=lambda url: None, limiter_factory=FakeLimiter)
    try:
        previous = PublisherCapacity(http, cfg)
        current = PublisherCapacity(http, {**cfg, 'failure_diagnostic_focus': spec()})
        assert previous.provider_fingerprints['cnyes'] != current.provider_fingerprints['cnyes']
        assert all(previous.provider_fingerprints[p] == current.provider_fingerprints[p]
                   for p in previous.provider_fingerprints if p != 'cnyes')
        assert {p: (s.data_fingerprint, s.authorization_fingerprint) for p, s in registry.sources.items()} == {
            p: (s.data_fingerprint, s.authorization_fingerprint) for p, s in http.registry.sources.items()}
    finally:
        http.close()


@pytest.mark.parametrize('field,value', [
    ('path', 'diagnostics/wrong.parquet'), ('request_url_sha256', 'b' * 64),
    ('failure_diagnostic_focus_contract_version', 2), ('failure_diagnostic_focus_manifest_sha256', 'c' * 64),
    ('failure_diagnostic_focus_attempt_number', 2), ('failure_diagnostic_focus_maximum_raw_bytes', 1024),
    ('failure_diagnostic_focus_maximum_write_attempts_per_url', 3),
    ('budget_period', 'wrong-cohort'), ('budget_period_kind', 'daily'), ('status', 403),
    ('error_code', 'paywall_or_login'), ('raw_response_retained', False),
    ('diagnostic_only', False), ('accepted_as_article', True), ('publish', True),
    ('remote_training_export', True), ('response_bytes', 1), ('response_sha256', 'd' * 64)])
def test_corrupt_finished_pair_never_reconciles_or_retries(cnyes_config, tmp_path, monkeypatch, field, value):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'finish',
                            lambda *_: (_ for _ in ()).throw(OSError('owned lost finish')))
        fail(queue, response())
        path = reports(tmp_path)[0][0].with_suffix('.receipt.json')
        receipt = json.loads(path.read_bytes())
        receipt[field] = value
        atomic_write_json(path, receipt)
        original = sha256_file(path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, response())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'reserved': 1}
        assert len(reports(tmp_path)[0]) == 1 and sha256_file(path) == original


def test_missing_parquet_identity_cannot_be_hidden_by_new_checksum(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'finish',
                            lambda *_: (_ for _ in ()).throw(OSError('owned lost finish')))
        fail(queue, response())
        paths, rows = reports(tmp_path)
        row = rows[0]
        row.pop('publish')
        atomic_write_parquet(paths[0], table_from_records([row]), compression='zstd')
        path = paths[0].with_suffix('.receipt.json')
        receipt = json.loads(path.read_bytes())
        receipt['sha256'] = sha256_file(paths[0])
        atomic_write_json(path, receipt)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'reserved': 1}


@pytest.mark.parametrize('relative', ['diagnostics/../outside', '/tmp/outside',
    'diagnostics//20261012/cnyes/focus/owned-fixture-v1/20261012T000000000000Z-abcdefab.receipt.json'])
def test_planned_destination_rejects_noncanonical_namespace(cnyes_config, tmp_path, relative):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        with pytest.raises(ValueError):
            queue.failure_diagnostic_focus.receipt_path(relative)


def test_planned_destination_rejects_existing_symlink_escape(cnyes_config, tmp_path):
    root, outside = tmp_path / 'owner', tmp_path / 'outside'
    outside.mkdir()
    with closing(NewsQueue(root, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        (root / 'diagnostics').symlink_to(outside, target_is_directory=True)
        with pytest.raises(ValueError, match='escapes'):
            queue.failure_diagnostic_focus.receipt_path(receipt_path(queue.failure_diagnostic_focus))
        assert not list(outside.iterdir())


def test_nonfocused_failures_keep_same_daily_budget(cnyes_config, tmp_path):
    other = URL.replace('/42', '/43')
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec())
        for _ in range(4):
            fail(queue, response(other))
        _, rows = reports(tmp_path)
        assert len(rows) == 3 and all(row['contract_version'] == 6 for row in rows)
        assert queue.failure_diagnostic_focus.progress()['write_attempts_reserved'] == 0
        fail(queue, response())
        assert queue.failure_diagnostic_focus.progress()['outcomes'] == {'raw_saved': 1}


@pytest.mark.parametrize('pid,state,timer,owner,activity,exit_status,accepted', [
    ('0', 'inactive', 'inactive', 'original', 'complete', '0', True),
    ('0', 'failed', 'inactive', 'original', 'configuration_reload_requested', '75', True),
    ('12', 'inactive', 'inactive', 'original', 'configuration_reload_requested', '75', False),
    ('0', 'failed', 'active', 'original', 'configuration_reload_requested', '75', False),
    ('0', 'failed', 'inactive', 'new', 'configuration_reload_requested', '75', False),
    ('0', 'failed', 'inactive', 'original', 'interrupted', '75', False),
    ('0', 'failed', 'inactive', 'original', 'configuration_reload_requested', '1', False),
    ('0', 'deactivating', 'inactive', 'original', 'configuration_reload_requested', '75', False)])
def test_expected_config75_drain_distinct_from_unknown_failure(pid, state, timer, owner, activity, exit_status, accepted):
    from artifacts.data_quality.news_goal_stage01_20261009.remaining_v114.deploy_verified import planned_drained
    assert planned_drained({'MainPID': pid, 'ActiveState': state}, timer,
        {'owner_id': owner, 'activity': activity}, 'original', exit_status) is accepted
