"""Explicit live-index schedules, independent of body and discovery-parser identity.

Do not infer hourly archive polling from a provider name or a homepage-shaped
URL. Only declared, already registered continuous indexes have this schedule.
The canonical queue owner corrects known successful legacy daily deadlines once,
using their original aware observation clock. Refusals and unknown work stay put.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import time


CONTRACT_VERSION = 1
LEGACY_INDEX_INTERVAL_SECONDS = 86400
LEGACY_DUE_TOLERANCE_SECONDS = 2


def schedule_fingerprint(source):
    payload = {'version': CONTRACT_VERSION, 'provider': source.provider,
               'interval_seconds': source.discovery_interval_seconds,
               'incremental_index_pages': sorted(source.incremental_index_pages)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def receipt_key(source):
    return 'incremental_index_schedule:' + source.provider + ':' + schedule_fingerprint(source)


def reconcile_incremental_indexes(queue, source, parser_fingerprint):
    """PK lookups for a finite explicit scope, in the caller's transaction.

    A daily success was incorrectly scheduled regardless of its configured
    interval. Recognize that exact old rule, not arbitrary future deadlines.
    Marker and due correction commit/roll back together; no source request,
    publication/capture clock, validator, error or attempts is rewritten.
    """
    if not source.allowed or source.acquisition_mode != 'continuous' or not source.incremental_index_pages:
        return []
    key = receipt_key(source)
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (key,)).fetchone():
        return []
    corrected, skipped = [], []
    now = time.time()
    for url in source.incremental_index_pages:
        row = queue.db.execute('SELECT provider,kind,due,attempts,error_code,last_checked_at_utc,parser_fingerprint '
                               'FROM discovery WHERE url=?', (url,)).fetchone()
        if (not row or row['provider'] != source.provider or row['kind'] != 'index'
                or row['error_code'] is not None or row['attempts'] != 0
                or row['parser_fingerprint'] != parser_fingerprint or not row['last_checked_at_utc']):
            continue
        try:
            checked = datetime.fromisoformat(row['last_checked_at_utc'].replace('Z', '+00:00'))
            due = row['due']
            if (checked.utcoffset() is None or not math.isfinite(checked.timestamp())
                    or checked.timestamp() > now+LEGACY_DUE_TOLERANCE_SECONDS
                    or type(due) not in {int,float} or not math.isfinite(due)):
                raise ValueError('unverifiable schedule clock')
        except (ValueError, TypeError, AttributeError, OverflowError):
            skipped.append({'url': url, 'reason': 'unverifiable_successful_schedule_clock'})
            continue
        expected = checked.timestamp()+source.discovery_interval_seconds
        legacy = checked.timestamp()+LEGACY_INDEX_INTERVAL_SECONDS
        if abs(due-legacy) > LEGACY_DUE_TOLERANCE_SECONDS or due <= expected:
            continue
        queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (expected,url))
        corrected.append({'url': url, 'original_due': due, 'corrected_due': expected,
                          'original_last_checked_at_utc': row['last_checked_at_utc']})
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key,json.dumps({
        'contract_version': CONTRACT_VERSION, 'provider': source.provider,
        'schedule_fingerprint': schedule_fingerprint(source), 'parser_fingerprint': parser_fingerprint,
        'inspected_at_utc': datetime.now(timezone.utc).isoformat(), 'corrected': corrected, 'skipped': skipped,
        'publisher_requests': 0, 'source_observation_or_validators_changed': False,
    },sort_keys=True)))
    if corrected:
        queue.counter('incremental_index_schedules_corrected', len(corrected))
        queue.metrics.count(source.provider, 'incremental_index_schedules_corrected', len(corrected))
    return corrected
