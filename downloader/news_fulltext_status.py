"""Bounded heartbeat views, not deletion of acquisition diagnostic evidence.

Public readers never query SQLite. The canonical owner publishes a bounded
sample and exact grouped counts; full gap rows and calibration windows retain
their existing authority and recovery contracts.
"""
from __future__ import annotations


STATUS_PROJECTION_CONTRACT_VERSION = 2
CAPACITY_STATUS_HISTORY_LIMIT = 64
BULK_GAP_STATUS_SAMPLE_LIMIT = 64


def summarize_bulk_window_gaps(db):
    """One grouped SQL pass; never build every historical gap as Python rows."""
    groups = [dict(row) for row in db.execute(
        'SELECT provider,error_code,count(*) AS scopes,min(observed_at_utc) AS first_observed_at_utc,'
        'max(observed_at_utc) AS latest_observed_at_utc FROM bulk_window_gaps '
        'GROUP BY provider,error_code ORDER BY provider,error_code')]
    samples = [dict(row) for row in db.execute(
        'SELECT * FROM bulk_window_gaps ORDER BY scope_url LIMIT ?', (BULK_GAP_STATUS_SAMPLE_LIMIT,))]
    total = sum(row['scopes'] for row in groups)
    return samples, {
        'contract_version': 1, 'total_gaps': total, 'groups': groups,
        'sample_limit': BULK_GAP_STATUS_SAMPLE_LIMIT, 'sample_rows': len(samples),
        'sample_truncated': total > len(samples), 'sample_order': 'scope_url',
        'all_gap_details_preserved': True, 'detail_authority': 'SQLite bulk_window_gaps',
        'historical_completeness_verified': False,
    }
