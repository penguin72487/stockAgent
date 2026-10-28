"""Admit a verified, bounded option-product query through the existing owner.

The reduced acquisition-only reserve is limited to six explicit product-daily
history keys. It does not apply to whole-day tick downloads or other queues.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import sha256_file
from downloader.finmind_product_query_verification import _keys, _read, permits
from downloader.provider_priority import inspect
from scripts.check_acquisition_capacity import priority_dispatch_status


MAX_OPTION_KEYS = 6  # Per dispatch batch; the registered demand may be larger.
ORDINARY_CAPACITY_BLOCKED = 'finite_option_ordinary_physical_capacity_deferred'
# Base reserve is supplied by the existing policy. Keep another 24 GiB for
# concurrent processes, plus one maximum 64 MiB response per finite key.
ADDITIONAL_HEADROOM_BYTES = 24 * 1024**3 + MAX_OPTION_KEYS * 64 * 1024**2


def verified_request(verification_path: Path, priority_path: Path) -> tuple[str, tuple, dict]:
    verification, _ = _read(verification_path.absolute())
    keys = _keys(verification)
    if not keys or any(dataset != 'TaiwanOptionDaily' for dataset, _, _ in keys):
        raise ValueError('option daily product checks are required')
    priority, _ = _read(priority_path.absolute())
    if (priority.get('contract') != 'provider_priority_query_v1' or priority.get('provider') != 'finmind'
            or not isinstance(priority.get('items'), list)):
        raise ValueError('an explicit FinMind priority plan is required')
    priority_keys = tuple((item.get('dataset'), item.get('data_id'), item.get('partition'))
                          for item in priority['items'] if isinstance(item, dict))
    if (len(priority_keys) != len(priority['items']) or len(priority_keys) != len(keys)
            or set(priority_keys) != set(keys)):
        raise ValueError('priority and source verification keys differ')
    created = datetime.fromisoformat(priority['created_at_utc'].replace('Z', '+00:00'))
    expires = datetime.fromisoformat(priority['expires_at_utc'].replace('Z', '+00:00'))
    if (created.tzinfo is None or expires.tzinfo is None or expires <= created
            or (expires - created).total_seconds() > 7 * 86400):
        raise ValueError('priority lifetime must be explicit and at most seven days')
    evidence = verification.get('source_evidence')
    if not isinstance(evidence, list) or not evidence:
        raise ValueError('source evidence is required')
    for item in evidence:
        path = Path(item['path'])
        if not path.is_absolute() or sha256_file(path) != item['sha256']:
            raise ValueError('product check source evidence changed')
    digest = hashlib.sha256(json.dumps(priority, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return digest, keys, priority


def validate_owner_request(root: Path, verification: Path, priority: Path, keys: tuple,
                           max_requests: int, *, now: datetime | None = None) -> None:
    """Require an active registered plan and its exact existing owner tasks."""
    request_id, planned, _ = verified_request(verification, priority)
    if (not keys or len(keys) > MAX_OPTION_KEYS or len(keys) != len(set(keys)) or not set(keys) <= set(planned)
            or not 0 < max_requests <= len(keys)):
        raise ValueError('finite priority must select bounded plan keys')
    status = inspect(root, request_id, now=now or datetime.now(timezone.utc))
    if (status.get('provider') != 'finmind' or status.get('request_id') != request_id
            or not status.get('priority_active')):
        raise ValueError('finite option priority is not active')
    uri = (root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=5) as connection:
        for key in keys:
            row = connection.execute('SELECT kind FROM tasks WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
            if row is None or row[0] != 'id_history' or not permits(connection, key):
                raise ValueError('finite option priority requires the registered product-history task')


def capacity_probe(root: Path, verification: Path, priority: Path, keys: tuple, max_requests: int) -> dict:
    """Recheck identity, active authorization and physical capacity on each probe."""
    validate_owner_request(root, verification, priority, keys, max_requests)
    result = priority_dispatch_status(root, additional_headroom_bytes=ADDITIONAL_HEADROOM_BYTES)
    if result.get('state') == 'not_enrolled':
        return {'state': 'deferred', 'reason': 'finite_priority_requires_enrolled_physical_capacity'}
    # This basis is returned only when the same physical observation proved the
    # ordinary 64-GiB preflight blocked, while the stricter finite headroom
    # passed. Preserve the distinction in a sampler-supported field.
    if (result.get('state') == 'admitted'
            and result.get('backing_volume', {}).get('observation_basis') == 'same_dispatch_physical_probe'):
        result = {**result, 'guard_kind': ORDINARY_CAPACITY_BLOCKED}
    return result


def capacity_deferred_priority_allowed(admission: dict, budget: dict, reservation: dict) -> bool:
    """Allow this small exact query only while ordinary dispatch cannot run.

    Due and upcoming requests remain in the quota reserve; this does not reset
    or complete them. Unknown, stale, unenrolled or recovered capacity yields.
    The owner calls this only after validating the finite plan and task keys.
    """
    return bool(admission.get('state') == 'admitted'
                and admission.get('guard_kind') == ORDINARY_CAPACITY_BLOCKED
                and type(admission.get('sample_age_seconds')) in (int, float)
                and 0 <= admission['sample_age_seconds'] <= 30
                and budget.get('priority_wait') is True
                and budget.get('schedule_verified') is True
                and budget.get('basis') == 'provider_observation_plus_local_starts'
                and type(budget.get('remaining')) is int and type(budget.get('reserve')) is int
                and budget['remaining'] > budget['reserve']
                and reservation.get('queue_errors') == []
                and type(reservation.get('ready_requests')) is int
                and reservation['ready_requests'] > 0)
