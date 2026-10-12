"""Heartbeat bounds cannot discard authoritative gaps, recovery or ETA evidence."""
from copy import deepcopy
import json

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_status import (
    BULK_GAP_STATUS_SAMPLE_LIMIT, CAPACITY_STATUS_HISTORY_LIMIT,
    summarize_bulk_window_gaps,
)
from stockagent.live.news_dashboard import _completion_forecast
from test_news_capacity import controller_fixture
from test_news_completion_forecast import samples
from test_news_dashboard import NOW
from test_news_fulltext import config


def test_compact_json_keeps_exact_values_and_legacy_default(tmp_path):
    value = {'原文': '負值 -12.50%\n\u00a0', 'counts': [1, 0, None, False], 'nested': {'clock': 1234.5}}
    legacy, compact = tmp_path / 'legacy.json', tmp_path / 'compact.json'
    atomic_write_json(legacy, value)
    atomic_write_json(compact, value, compact=True)
    assert legacy.read_text() == json.dumps(value, ensure_ascii=False, indent=2) + '\n'
    assert compact.read_text() == json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n'
    assert json.loads(compact.read_bytes()) == json.loads(legacy.read_bytes()) == value
    assert compact.stat().st_size < legacy.stat().st_size
    assert not list(tmp_path.glob('.*.tmp'))


@pytest.mark.parametrize('count', [0, 1, 64, 65, 1025])
def test_gap_sample_is_bounded_exact_summary_and_no_mutation(config, tmp_path, count):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.db.executemany('INSERT INTO bulk_window_gaps VALUES (?,?,?,?)', [
                (f'https://publisher.test/scopes/{index:06d}', 'publisher',
                 'page_cap' if index % 2 else 'source_gap', f'2026-10-12T01:{index % 60:02d}:00+00:00')
                for index in range(count)])
        before = [dict(row) for row in queue.db.execute('SELECT * FROM bulk_window_gaps ORDER BY scope_url')]
        changes = queue.db.total_changes
        projected, summary = summarize_bulk_window_gaps(queue.db)
        assert projected == before[:BULK_GAP_STATUS_SAMPLE_LIMIT]
        assert summary['total_gaps'] == count
        assert summary['sample_rows'] == min(count, 64)
        assert summary['sample_truncated'] == (count > 64)
        assert summary['all_gap_details_preserved'] and not summary['historical_completeness_verified']
        assert sum(row['scopes'] for row in summary['groups']) == count
        for group in summary['groups']:
            exact = [row for row in before if row['provider'] == group['provider'] and row['error_code'] == group['error_code']]
            assert group['scopes'] == len(exact)
            assert group['first_observed_at_utc'] == min(row['observed_at_utc'] for row in exact)
            assert group['latest_observed_at_utc'] == max(row['observed_at_utc'] for row in exact)
        assert changes == queue.db.total_changes
        assert before == [dict(row) for row in queue.db.execute('SELECT * FROM bulk_window_gaps ORDER BY scope_url')]
        status = queue.status()
        assert status['status_projection_contract_version'] == 2
        assert status['bulk_gap_projection'] == summary and status['bulk_historical_window_gaps'] == projected
        assert json.loads((tmp_path / 'status.json').read_bytes()) == status
        assert (tmp_path / 'status.json').stat().st_size < len(json.dumps(status, ensure_ascii=False, indent=2).encode())
    finally:
        queue.close()


@pytest.mark.parametrize('seconds', [10, 30, 60, 600])
def test_capacity_heartbeat_preserves_all_forecast_time_failures_and_durable_history(config, tmp_path, seconds):
    http, controller, _ = controller_fixture(config, tmp_path)
    try:
        counts = tuple(0 if index % 3 else 5 for index in range(128))
        history = samples(counts, seconds=seconds)
        state = controller.states['publisher']
        state.update(history=history, last_window=history[-1])
        original = deepcopy(state)
        full = controller.snapshot()
        projected = controller.snapshot(for_status=True)
        assert projected['providers']['publisher']['history'] == history[-CAPACITY_STATUS_HISTORY_LIMIT:]
        assert len(full['providers']['publisher']['history']) == 128
        assert projected['providers']['publisher']['last_window'] == projected['providers']['publisher']['history'][-1]
        assert state == original
        assert projected['heartbeat_projection']['omitted_history_windows']['publisher'] == 64
        assert projected['heartbeat_projection']['full_history_preserved']
        metrics = {'owner_id': 'a' * 32, 'snapshot_clock_contract_version': 1,
                   'snapshot_at_utc': NOW.isoformat(), 'owner_elapsed_seconds': 128 * seconds + 1}

        def forecast(payload):
            return _completion_forecast(metrics, {
                'measurement_contract_version': payload['measurement_contract_version'],
                'completion_window_owner_contract_version': payload['completion_window_owner_contract_version'],
                **payload['providers']['publisher']}, sum(counts), NOW, NOW, True, current_owner='a' * 32)

        assert forecast(full) == forecast(projected)
        assert http.capacity_snapshot()['calibration']['providers']['publisher']['history'] == history[-64:]
        projected['providers']['publisher']['history'][-1]['new_stored_url_heads'] = 123456
        assert state == original  # No returned object aliases controller/recovery state.
        atomic_write_json(controller.path, full)
        assert len(json.loads(controller.path.read_bytes())['providers']['publisher']['history']) == 128
        assert 'heartbeat_projection' not in full
    finally:
        http.close()
