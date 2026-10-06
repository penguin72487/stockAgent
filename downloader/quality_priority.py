"""Finite audited rechecks in canonical collectors, without a second scheduler.

Due incremental collection keeps its normal priority. Historical runs dispatch
verified repair intents first. Source checks are not claimed as gap fills; a
failed or empty recheck never blocks unrelated symbols indefinitely.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import re

try:
    from downloader.artifact_io import atomic_write_json
except ModuleNotFoundError:
    from artifact_io import atomic_write_json

CONTRACT = 'canonical_source_quality_priority_v1'
MAX_ATTEMPTS = 3
MAX_BYTES = 8 * 1024**2
REASONS = {'invalid_values', 'gap_candidate', 'structural_integrity_failure', 'timestamp_keys'}
_WARNED_INVALID_PLANS = set()


def _time(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(UTC) if parsed.tzinfo else None
    except (AttributeError, TypeError, ValueError):
        return None


def load_plan(root: Path) -> dict:
    path = root / 'quality_priority_requests.json'
    if not path.exists():
        return {'contract': CONTRACT, 'requests': []}
    if path.is_symlink():
        raise ValueError('unsafe quality intent path')
    with path.open('rb') as stream:
        captured = stream.read(MAX_BYTES + 1)
    if len(captured) > MAX_BYTES:
        raise ValueError('quality intent size bound exceeded')
    plan = json.loads(captured)
    if (not isinstance(plan, dict) or plan.get('contract') != CONTRACT
            or not isinstance(plan.get('requests'), list) or len(plan['requests']) > 4096):
        raise ValueError('invalid quality intent contract')
    ids = set()
    for row in plan['requests']:
        code = row.get('code') if isinstance(row, dict) else None
        if (not isinstance(code, str) or not code or code in {'.', '..'} or '/' in code or '\\' in code
                or any(ord(c) < 32 for c in code) or row.get('reason') not in REASONS
                or not isinstance(row.get('request_id'), str) or re.fullmatch(r'[a-f0-9]{64}', row['request_id']) is None
                or row['request_id'] in ids or not isinstance(row.get('evidence_sha256'), str)
                or re.fullmatch(r'[a-f0-9]{64}', row['evidence_sha256']) is None
                or _time(row.get('expires_at_utc')) is None
                or type(row.get('attempts', 0)) is not int or not 0 <= row.get('attempts', 0) <= MAX_ATTEMPTS):
            raise ValueError('invalid quality intent identity')
        ids.add(row['request_id'])
    return plan


def pending_requests(root: Path, *, now=None) -> list[dict]:
    now = now or datetime.now(UTC)
    try:
        plan = load_plan(root)
    except (OSError, ValueError):
        if str(root) not in _WARNED_INVALID_PLANS:
            print('[source-quality] invalid priority intent; normal collection continues', flush=True)
            _WARNED_INVALID_PLANS.add(str(root))
        return []
    return [r for r in plan['requests'] if r.get('state', 'pending') in {'pending', 'retryable', 'running'}
            and r.get('attempts', 0) < MAX_ATTEMPTS and _time(r['expires_at_utc']) > now]


def prioritize_records(records, root: Path, *, tail_only: bool):
    if tail_only:
        return records  # Live refresh must not wait behind historical repairs.
    codes = {r['code'] for r in pending_requests(root)}
    return sorted(records, key=lambda record: record.code not in codes)


class Attempt:
    def __init__(self, request_ids):
        self.request_ids, self.pages, self.failed = request_ids, 0, False

    def observe_page(self):
        self.pages += 1


@contextmanager
def quality_attempt(root: Path, code: str, *, tail_only: bool):
    rows = [] if tail_only else [r for r in pending_requests(root) if r['code'] == code]
    attempt = Attempt({r['request_id'] for r in rows})
    failed = False
    try:
        yield attempt
    except BaseException:
        failed = True
        raise
    finally:
        if attempt.request_ids:
            # Collector dataset locks already serialize upstream work. This
            # small lock also protects concurrently arriving audit intents.
            try:
                with (root / '.quality_priority.lock').open('a') as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX)
                    plan = load_plan(root)
                    for row in plan['requests']:
                        if row['request_id'] not in attempt.request_ids:
                            continue
                        row['attempts'] = min(MAX_ATTEMPTS, row.get('attempts', 0) + 1)
                        attempt_failed = failed or attempt.failed
                        checked = not attempt_failed and attempt.pages > 0
                        row['state'] = ('source_checked_not_fill_proof' if checked else
                                        'retry_exhausted' if row['attempts'] >= MAX_ATTEMPTS else 'retryable')
                        row.setdefault('history', []).append({'observed_at_utc': datetime.now(UTC).isoformat(),
                            'request_pages': attempt.pages, 'failed': attempt_failed,
                            'gap_filled': None, 'state': row['state']})
                    atomic_write_json(root / 'quality_priority_requests.json', plan)
                    (root / 'quality_priority_requests.json').chmod(0o600)
            except (OSError, ValueError):
                # Optional audit metadata must not mask the upstream exception
                # or turn a successful source commit into a failed download.
                # The unacknowledged intent remains unverified, not completed.
                print('[source-quality] attempt receipt unavailable; source result preserved', flush=True)


def collect_with_quality_priority(call, *, root: Path, code: str, tail_only: bool, page_observer):
    """Wrap the existing worker without altering provider quota or transport."""
    with quality_attempt(root, code, tail_only=tail_only) as attempt:
        def page(*args):
            attempt.observe_page()
            return page_observer(*args)
        result = call(page)
        attempt.failed = str(getattr(result, 'status', '')).startswith('failed')
        return result
