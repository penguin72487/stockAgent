"""Static stage evidence cannot be starved by generic failures or reset by boot."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, short_page
from test_news_failure_diagnostics import fail, reports
from test_news_failure_diagnostics import globalvoices_registry
from test_news_fulltext import config as cfg
from test_news_fulltext import http_fixture
from test_news_globalvoices_small_body import URL as GV_URL, page as gv_page


def raw():
    return Response(URL, 200, {'content-type': 'text/html'}, short_page(visible='不一致的原件測試'))


def day():
    return datetime.now(timezone.utc).strftime('%Y%m%d')


def legacy(queue, receipts):
    today = day()
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
            (f'failure_diagnostic_attempts:v1:cnyes:{today}', '2'))
    paths = []
    for number, updates in enumerate(receipts):
        path = queue.root / 'diagnostics' / today / 'cnyes' / f'legacy-{number}.receipt.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(path, {'provider': 'cnyes', 'contract_version': 4,
            'error_code': 'body_not_found', 'diagnostic_only': True, 'accepted_as_article': False,
            'captured_at_utc': datetime.now(timezone.utc).isoformat(), **updates})
        paths.append(path)
    return paths


def test_all_static_stages_and_general_have_independent_bounded_storage(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        for _ in range(3):
            fail(queue, raw())
        for stage in sorted(diagnostics.CNYES_NATIVE_BODY_FAILURE_STAGES):
            for _ in range(3):
                fail(queue, raw(), stage=stage)
        paths, rows = reports(tmp_path)
        assert len(rows) == (diagnostics.DAILY_PROVIDER_ATTEMPTS + diagnostics.HOURLY_BUCKET_ATTEMPTS) * (
            1 + len(diagnostics.CNYES_NATIVE_BODY_FAILURE_STAGES)) == 18
        daily = [row for row in rows if row['budget_period_kind'] == 'utc_day']
        hourly = [row for row in rows if row['budget_period_kind'] == 'utc_hour']
        assert len(daily) == 12 and len(hourly) == 6
        assert sum(row['budget_bucket'] == 'provider_general' for row in daily) == 2
        assert all(sum(row['budget_bucket'] == stage for row in daily) == 2 for stage in diagnostics.CNYES_NATIVE_BODY_FAILURE_STAGES)
        assert all(row['budget_attempt_limit'] == 1 for row in hourly)
        assert all(row['budget_attempt_limit'] == 2 for row in daily)
        assert all(row['budget_contract_version'] == 3 and row['daily_bucket_attempt_limit'] == 2
                   and not row['accepted_as_article'] and row['raw_response_body'] == raw().body for row in rows)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()[0] is None
        for path in paths:
            assert json.loads(path.with_suffix('.receipt.json').read_bytes())['sha256'] == sha256_file(path)


def test_known_legacy_stage_is_charged_once_other_stage_not_starved(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        old_stage, rare = 'schema_comparison_mismatch', 'table_grid_mismatch'
        paths = legacy(queue, [{'native_body_failure_stage': old_stage}] * 2)
        hashes = [sha256_file(path) for path in paths]
        fail(queue, raw(), stage=old_stage)
        assert len(reports(tmp_path)[0]) == 1 and reports(tmp_path)[1][0]['budget_period_kind'] == 'utc_hour'
        for _ in range(3):
            fail(queue, raw(), stage=rare)
        assert len(reports(tmp_path)[0]) == 4
        assert hashes == [sha256_file(path) for path in paths]
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            (f'failure_diagnostic_attempts:v1:cnyes:{day()}',)).fetchone()[0] == '2'


def test_missing_or_unclassified_legacy_attempts_are_not_invented_as_a_stage(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        legacy(queue, [{'native_body_failure_stage': None}])
        fail(queue, raw(), stage='table_grid_mismatch')
        assert len(reports(tmp_path)[0]) == 1
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            (f'failure_diagnostic_attempts:v1:cnyes:{day()}',)).fetchone()[0] == '2'


@pytest.mark.parametrize('bad', ['json', 'oversized', 'many', 'provider', 'clock', 'contract', 'accepted'])
def test_unverified_legacy_receipt_blocks_new_buckets_once_without_stopping_source(cnyes_config, tmp_path, bad):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        changes = {'provider': {'provider': 'other'}, 'clock': {'captured_at_utc': 'naive'},
                   'contract': {'contract_version': True}, 'accepted': {'accepted_as_article': True}}
        paths = legacy(queue, [changes.get(bad, {})] * (3 if bad == 'many' else 1))
        if bad in {'json', 'oversized'}:
            atomic_write_json(paths[0], {'oversized': 'x' * 65536} if bad == 'oversized' else [])
        for _ in range(3):
            fail(queue, raw(), stage='table_grid_mismatch')
        assert not reports(tmp_path)[0]
        counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert counts['failure_diagnostic_legacy_budget_seed_errors'] == 1
        assert queue.db.execute('SELECT error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()[:] == ('body_not_found', None)


def test_native_reservation_survives_restart_parser_upgrade_and_storage_outage(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    calls = []
    def broken(*args, **kwargs):
        calls.append(True)
        raise OSError('owned fixture storage outage')
    monkeypatch.setattr(diagnostics, 'atomic_write_parquet', broken)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for _ in range(3):
            fail(queue, raw(), stage='table_grid_mismatch')
    registry.sources['cnyes'] = replace(registry.sources['cnyes'], data_fingerprint='new-owned-parser')
    with closing(NewsQueue(tmp_path, registry)) as queue:
        fail(queue, raw(), stage='table_grid_mismatch')
    assert len(calls) == 3 and not reports(tmp_path)[0]


def test_other_provider_cannot_borrow_native_stage_buckets(cfg, tmp_path):
    response = Response('https://publisher.test/a', 200, {'content-type': 'text/html'}, b'<html>owned fixture</html>')
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        for stage in diagnostics.CNYES_NATIVE_BODY_FAILURE_STAGES:
            fail(queue, response, stage=stage)
        assert len(reports(tmp_path)[0]) == 2
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_stage_attempts:%'").fetchone()


def test_new_utc_day_has_new_budget_without_resetting_previous_reservations(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        for _ in range(3):
            fail(queue, raw(), stage='table_grid_mismatch')
        today = day()
        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y%m%d')
        # Budget seam only; not a fake publisher observation or backdated body.
        with queue.db:
            diagnostics._seed_native_budgets(queue, 'cnyes', tomorrow)
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            (diagnostics._stage_budget_key('cnyes', today, 'table_grid_mismatch'),)).fetchone()[0] == '2'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            (diagnostics._stage_budget_key('cnyes', tomorrow, 'table_grid_mismatch'),)).fetchone()[0] == '0'


@pytest.mark.parametrize('provider', ['cnyes', 'globalvoices'])
def test_fixed_hour_budget_advances_without_resetting_daily_or_restarted_hour(cnyes_config, tmp_path, provider):
    registry = (PolicyRegistry(cnyes_config, base=tmp_path) if provider == 'cnyes'
                else globalvoices_registry(tmp_path))
    response = raw() if provider == 'cnyes' else Response(GV_URL, 200, {'content-type': 'text/html'}, gv_page(''))
    root = tmp_path / 'store'
    # An owned diagnostic clock seam, not a reconstructed publication date.
    hour = datetime.now(timezone.utc).replace(hour=10, minute=20, second=0, microsecond=0)
    with closing(NewsQueue(root, registry)) as queue:
        for _ in range(8):
            fail(queue, response, received_at_utc=hour.isoformat())
        _, rows = reports(root)
        assert len(rows) == 3
        assert sorted(row['budget_period_kind'] for row in rows) == ['utc_day', 'utc_day', 'utc_hour']
    registry.sources[provider] = replace(registry.sources[provider], data_fingerprint='owned-upgraded-parser')
    with closing(NewsQueue(root, registry)) as queue:
        fail(queue, response, received_at_utc=hour.astimezone(timezone(timedelta(hours=8))).isoformat())
        assert len(reports(root)[0]) == 3
        for _ in range(8):
            fail(queue, response, received_at_utc=(hour + timedelta(hours=1)).isoformat())
        _, rows = reports(root)
        assert len(rows) == 4 and sum(row['budget_period_kind'] == 'utc_day' for row in rows) == 2
        assert all(not row['accepted_as_article'] and row['raw_response_body'] == response.body for row in rows)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            (f'failure_diagnostic_attempts:v1:{provider}:{hour:%Y%m%d}',)).fetchone()[0] == '2'


def test_spent_fixed_hour_returns_before_public_scope_parse_or_storage(cnyes_config, tmp_path, monkeypatch):
    calls = []
    original = diagnostics._public_cnyes_scope
    def observed(*args):
        calls.append(True)
        return original(*args)
    monkeypatch.setattr(diagnostics, '_public_cnyes_scope', observed)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        for _ in range(20):
            fail(queue, raw(), stage='table_grid_mismatch')
        assert len(calls) == 3 and len(reports(tmp_path)[0]) == 3
        assert queue.metrics.snapshot()['providers']['cnyes']['counts']['failure_diagnostic_budget_exhausted'] == 17


@pytest.mark.parametrize('stage', [None, 'table_grid_mismatch'])
@pytest.mark.parametrize('marker', ['blocked_unverified_legacy_budget', 'unknown-owned-marker'])
def test_closed_legacy_seed_cannot_be_reopened_by_hourly_bucket(cnyes_config, tmp_path, stage, marker):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        key = diagnostics._stage_budget_key('cnyes', day(), stage) if stage else f'failure_diagnostic_attempts:v1:cnyes:{day()}'
        with queue.db:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, '2'))
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                             (f'failure_diagnostic_stage_seed:v1:cnyes:{day()}', marker))
        fail(queue, raw(), stage=stage)
        assert not reports(tmp_path)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_hourly_attempts:%'").fetchone()
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == '2'


@pytest.mark.parametrize('value', ['-1', '3', 'not-an-integer'])
def test_corrupt_daily_charge_cannot_mint_hourly_evidence(cnyes_config, tmp_path, value):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        key = f'failure_diagnostic_attempts:v1:cnyes:{day()}'
        with queue.db:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, value))
        fail(queue, raw())
        assert not reports(tmp_path)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_hourly_attempts:%'").fetchone()
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == value
        assert queue.db.execute('SELECT error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()[:] == ('body_not_found', None)


@pytest.mark.parametrize('change', [{'payment': 1}, {'payment': False}, {'publishAt': 1}])
def test_hourly_sample_does_not_expand_raw_capture_permissions(cnyes_config, tmp_path, change):
    response = Response(URL, 200, {'content-type': 'text/html'}, short_page(change=change))
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        for _ in range(8):
            fail(queue, response)
        _, rows = reports(tmp_path)
        assert len(rows) == 3 and sum(row['budget_period_kind'] == 'utc_hour' for row in rows) == 1
        assert all(row['raw_response_body'] is None and not row['raw_response_retained'] for row in rows)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


@pytest.mark.parametrize('clock', ['not-a-clock', '2026-10-11T01:02:03'])
def test_invalid_or_naive_observation_clock_cannot_reserve_hour(cnyes_config, tmp_path, clock):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        fail(queue, raw(), received_at_utc=clock)
        assert not reports(tmp_path)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_diagnostic_%attempts:%'").fetchone()


def test_static_day_upper_bound_not_per_url_or_fingerprint(cnyes_config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        stages = [None, *sorted(diagnostics.CNYES_NATIVE_BODY_FAILURE_STAGES)]
        reservations = []
        for hour in range(24):
            clock = start + timedelta(hours=hour)
            for stage in stages:
                for _ in range(6):
                    reserved = diagnostics._reserve_evidence(queue, 'cnyes', clock, stage)
                    if reserved:
                        reservations.append(reserved)
        assert len(reservations) == (2 + 24) * len(stages) == 156
        assert sum(row['budget_period_kind'] == 'utc_day' for row in reservations) == 12
        assert sum(row['budget_period_kind'] == 'utc_hour' for row in reservations) == 144
        assert not reports(tmp_path)[0]  # Budget-only seam performs no parser/storage/HTTP.


def test_only_hourly_evidence_measurement_identities_change_not_source_capture(cfg, tmp_path, monkeypatch):
    from copy import deepcopy
    from downloader import news_fulltext_capacity as capacity
    from downloader.news_fulltext import load_config
    from pathlib import Path

    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    native = deepcopy(next(p for p in config['providers'] if p['id'] == 'cnyes'))
    native.pop('cnyes_endpoint_inventory', None)
    native['cnyes_apis'] = []
    cfg['providers'].append(native)
    globalvoices = deepcopy(next(p for p in config['providers'] if p['id'] == 'globalvoices'))
    globalvoices.update(feeds=[], sitemaps=[], wordpress_apis=[], index_pages=[])
    cfg['providers'].append(globalvoices)
    taiwanhot = deepcopy(next(p for p in config['providers'] if p['id'] == 'taiwanhot'))
    cfg['providers'].append(taiwanhot)
    cfg['providers'][0]['interval_seconds'] = .01
    http, session = http_fixture(cfg, tmp_path, {})
    try:
        before_sources = {p: (s.data_fingerprint, s.authorization_fingerprint)
                          for p, s in http.registry.sources.items()}
        current = capacity.PublisherCapacity(http, cfg)
        monkeypatch.setattr(capacity, 'FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION', 1)
        previous = capacity.PublisherCapacity(http, cfg)
        assert current.fingerprint != previous.fingerprint
        for provider in diagnostics.HOURLY_EVIDENCE_PROVIDERS:
            assert current.provider_fingerprints[provider] != previous.provider_fingerprints[provider]
        assert {p: v for p, v in current.provider_fingerprints.items() if p not in diagnostics.HOURLY_EVIDENCE_PROVIDERS} == {
            p: v for p, v in previous.provider_fingerprints.items() if p not in diagnostics.HOURLY_EVIDENCE_PROVIDERS}
        assert before_sources == {p: (s.data_fingerprint, s.authorization_fingerprint)
                                  for p, s in http.registry.sources.items()}
        assert session.calls == []
    finally:
        http.close()
