"""Safe, versioned aggregate diagnostics for retained-source watchdog retries."""
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_status import cnyes_metadata_large_replay
from stockagent.live.news_dashboard import _cnyes_metadata_large_replay
from test_news_dashboard import metadata_large_sample


def test_retry_causes_are_aggregated_by_current_source_without_exposing_errors():
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE retained_metadata_large_retries '
                   '(source_fingerprint TEXT,not_before_epoch REAL,last_error TEXT,original_path TEXT)')
        errors = ['worker_progress_unknown_timeout', 'worker_progress_unknown_timeout',
                  'worker_progress_unchanged_timeout', 'worker_stalled', 'PRIVATE_ERROR']
        db.executemany('INSERT INTO retained_metadata_large_retries VALUES (?,?,?,?)',
                       [('active', 1_800_000_000, error, 'PRIVATE_PATH') for error in errors]
                       + [('another-source', 1_800_000_000, 'worker_progress_unknown_timeout', 'PRIVATE_PATH')])
        before = db.total_changes
        result = cnyes_metadata_large_replay(db, SimpleNamespace(data_fingerprint='active'), None)
        assert result['retry_records'] == 5
        assert result['retry_causes_contract_version'] == 1
        assert result['retry_cause_counts'] == {
            'progress_unknown_timeout': 2, 'counters_unchanged_timeout': 1, 'other': 2}
        assert db.total_changes == before and 'PRIVATE' not in str(result)
        public = _cnyes_metadata_large_replay(result)
        assert public['retry_cause_counts'] == result['retry_cause_counts']
        assert 'PRIVATE' not in str(public)


def test_legacy_snapshot_does_not_fabricate_zero_cause_counts():
    public = _cnyes_metadata_large_replay(metadata_large_sample())
    assert public['retry_records'] == 2
    assert 'retry_cause_counts' not in public


@pytest.mark.parametrize('patch', [
    {'retry_causes_contract_version': True},
    {'retry_causes_contract_version': None},
    {'retry_cause_counts': None},
    {'retry_cause_counts': {'progress_unknown_timeout': -1, 'counters_unchanged_timeout': 1, 'other': 2}},
    {'retry_cause_counts': {'progress_unknown_timeout': True, 'counters_unchanged_timeout': 1, 'other': 0}},
    {'retry_cause_counts': {'progress_unknown_timeout': 0, 'counters_unchanged_timeout': 0, 'other': 1}},
    {'retry_cause_counts': {'progress_unknown_timeout': 1, 'counters_unchanged_timeout': 1, 'other': 0, 'PRIVATE': 9}},
])
def test_invalid_cause_breakdown_is_unknown_without_losing_the_valid_parent_snapshot(patch):
    raw = {**metadata_large_sample(), 'retry_causes_contract_version': 1,
           'retry_cause_counts': {'progress_unknown_timeout': 1, 'counters_unchanged_timeout': 1, 'other': 0}}
    public = _cnyes_metadata_large_replay({**raw, **patch})
    assert public['retry_records'] == 2
    assert 'retry_cause_counts' not in public
    assert 'PRIVATE' not in str(public)
