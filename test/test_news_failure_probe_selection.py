"""A stale raw failure never licenses another request for a saved body."""
from datetime import datetime, timezone
from dataclasses import replace
import json
import time

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.probe_news_sources import require_current_missing_body_failures, select_current_failure_urls, main
from test_news_fulltext import config


URL = 'https://publisher.test/article/123'


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with value.db:
        value.add_articles([{'url': URL}])
        value.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',last_checked_at_utc=? WHERE url=?",
                         (datetime.now(timezone.utc).isoformat(), URL))
    yield value
    value.close()


def test_due_failed_missing_body_gets_pinned_read_only_grant(queue):
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    proof = require_current_missing_body_failures(queue.root, [URL], queue.registry)
    assert len(proof) == 1 and proof[0]['body_sha256'] is None
    assert proof[0]['last_checked_at_utc'] == before['last_checked_at_utc']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('column,value', [('body_sha256', 'a' * 64), ('state', 'complete'),
    ('state', 'pending'), ('state', 'inflight'), ('state', 'waiting_cohort'),
    ('state', 'source_article_empty_template'), ('error_code', 'http_403'),
    ('error_code', 'http_429'), ('error_code', 'paywall_or_login'),
    ('last_checked_at_utc', None), ('lease_owner', 'other'), ('lease_until', 1e12), ('provider', 'other')])
def test_saved_refused_leased_or_unobserved_heads_cancel_probe(queue, column, value):
    with queue.db:
        queue.db.execute(f'UPDATE articles SET {column}=? WHERE url=?', (value, URL))
    with pytest.raises(ValueError, match='probe_no_longer_a_due_failed_missing_body'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)


@pytest.mark.parametrize('clock', ['invalid', '2026-10-10', '2026-10-10T00:00:00'])
def test_probe_cannot_invent_the_previous_failure_clock(queue, clock):
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (clock, URL))
    with pytest.raises(ValueError, match='probe_failure_observation_clock_not_verified'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)


def test_future_per_url_deadline_is_not_overridden_by_failure_probe(queue):
    deadline = time.time() + 600
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=? WHERE url=?", (deadline, URL))
    with pytest.raises(ValueError, match='probe_no_longer_a_due_failed_missing_body'):
        require_current_missing_body_failures(queue.root, [URL], queue.registry)
    assert queue.db.execute('SELECT due FROM articles WHERE url=?', (URL,)).fetchone()[0] == deadline


def test_missing_or_oversized_selection_cannot_make_source_requests(queue):
    for urls in ([], [URL] * 17, [URL, URL], ['https://publisher.test/not-registered']):
        with pytest.raises(ValueError):
            require_current_missing_body_failures(queue.root, urls, queue.registry)


def test_dynamic_selection_reads_all_failure_lanes_without_changing_queue(queue):
    clock = datetime.now(timezone.utc).isoformat()
    with queue.db:
        for lane in ('retry', 'retry_cooldown', 'body_not_found'):
            for number in range(3):
                url = f'https://publisher.test/article/{lane}-{number}'
                queue.add_articles([{'url': url}])
                queue.db.execute('UPDATE articles SET state=?,error_code=?,last_checked_at_utc=?,due=? WHERE url=?',
                                 (lane, 'body_not_found', clock, 1 + number, url))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    selected = select_current_failure_urls(queue.root, 'publisher', queue.registry, maximum=16)
    proof = require_current_missing_body_failures(queue.root, selected, queue.registry)
    assert len(selected) == 6 and len(set(selected)) == 6
    assert all(sum(row['state'] == lane for row in proof) == 2
               for lane in ('retry', 'retry_cooldown', 'body_not_found'))
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


@pytest.mark.parametrize('maximum', [1, 2, 4])
def test_dynamic_selection_honors_its_bound(queue, maximum):
    selected = select_current_failure_urls(queue.root, 'publisher', queue.registry, maximum=maximum)
    assert 1 <= len(selected) <= maximum and selected == [URL]


@pytest.mark.parametrize('maximum', [0, 17, 1.0, True, None])
def test_invalid_dynamic_probe_bounds_cancel_before_http(queue, maximum):
    with pytest.raises(ValueError, match='bounded_enabled_ongoing_failure_provider_required'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry, maximum=maximum)


@pytest.mark.parametrize('change', [{'allowed': False}, {'acquisition_mode': 'finite'}])
def test_dynamic_selection_cannot_expand_disabled_or_finite_scope(queue, change):
    queue.registry.sources['publisher'] = replace(queue.registry.sources['publisher'], **change)
    with pytest.raises(ValueError, match='bounded_enabled_ongoing_failure_provider_required'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry)


@pytest.mark.parametrize('changes', [{'state': 'complete'}, {'body_sha256': 'a' * 64},
    {'due': 1e12}, {'lease_owner': 'other'}, {'error_code': 'http_403'}, {'error_code': 'http_429'}])
def test_dynamic_selection_excludes_saved_refused_not_due_or_leased_work(queue, changes):
    with queue.db:
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    with pytest.raises(ValueError, match='no_due_failed_missing_body_to_probe'):
        select_current_failure_urls(queue.root, 'publisher', queue.registry)


def test_dynamic_index_seek_does_not_scan_the_saved_or_pending_frontier(queue):
    plan = ' '.join(row[3] for row in queue.db.execute(
        "EXPLAIN QUERY PLAN SELECT url FROM articles INDEXED BY dispatch WHERE provider=? AND state=? "
        "AND error_code='body_not_found' AND body_sha256 IS NULL AND due<=? AND last_checked_at_utc IS NOT NULL "
        "AND lease_owner IS NULL AND lease_until IS NULL ORDER BY due LIMIT ?",
        ('publisher', 'retry', time.time(), 2)))
    assert 'SEARCH articles USING INDEX dispatch' in plan
    assert 'SCAN articles' not in plan and 'TEMP B-TREE' not in plan


@pytest.mark.parametrize('changes', [{'state': 'complete'}, {'due': 1e12},
    {'error_code': 'http_403'}, {'error_code': 'http_429'}, {'lease_owner': 'other'}])
def test_no_due_dynamic_cli_saves_explicit_skip_without_http_or_mutation(queue, monkeypatch, changes):
    from scripts import probe_news_sources as probes
    with queue.db:
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('empty selection created HTTP owner'))
    output = queue.root / 'probe-result.json'
    assert main(['--current-failure-provider', 'publisher', '--output', str(output)]) == 0
    result = json.loads(output.read_text())
    assert result['diagnostic_status_contract_version'] == 1
    assert result['state'] == 'skipped_no_due_failed_missing_body' and result['probes'] == []
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['all_history_complete'] and not result['publish']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


@pytest.mark.parametrize('maximum', ['0', '17'])
def test_no_due_cli_does_not_mask_invalid_scope_or_bounds(queue, monkeypatch, maximum):
    from scripts import probe_news_sources as probes
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('invalid selection created HTTP owner'))
    with pytest.raises(ValueError, match='bounded_enabled_ongoing_failure_provider_required'):
        main(['--current-failure-provider', 'publisher', '--maximum-failure-probes', maximum,
              '--output', str(queue.root / 'invalid-probe.json')])
    assert not (queue.root / 'invalid-probe.json').exists()


@pytest.mark.parametrize('changes', [{'state': 'complete'}, {'due': 1e12},
    {'error_code': 'http_403'}, {'error_code': 'http_429'}, {'lease_owner': 'other'}])
def test_explicit_handoff_race_records_skip_without_request_or_reset(queue, monkeypatch, changes):
    from scripts import probe_news_sources as probes
    with queue.db:
        for field, value in changes.items():
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('canceled explicit grant created HTTP owner'))
    output = queue.root / 'explicit-skip.json'
    assert main(['--probe', URL, '--require-current-missing-body-failure', '--output', str(output)]) == 0
    result = json.loads(output.read_text())
    assert result['state'] == 'skipped_no_longer_due_failed_missing_body'
    assert result['explicit_selected_urls'] == [URL] and result['probes'] == []
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['all_history_complete']
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


def test_explicit_skip_does_not_mask_bad_clocks_or_duplicate_requests(queue, monkeypatch):
    from scripts import probe_news_sources as probes
    monkeypatch.setattr(probes, 'load_config', lambda _path: ({'root': '.', 'workers': 1}, queue.registry, queue.root))
    monkeypatch.setattr(probes, 'NewsHTTP', lambda *_a, **_k: pytest.fail('invalid grant created HTTP owner'))
    output = queue.root / 'invalid-explicit-skip.json'
    with pytest.raises(ValueError, match='bounded_failure_probe_selection_required'):
        main(['--probe', URL, '--probe', URL, '--require-current-missing-body-failure', '--output', str(output)])
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', ('invalid', URL))
    with pytest.raises(ValueError, match='probe_failure_observation_clock_not_verified'):
        main(['--probe', URL, '--require-current-missing-body-failure', '--output', str(output)])
    assert not output.exists()
