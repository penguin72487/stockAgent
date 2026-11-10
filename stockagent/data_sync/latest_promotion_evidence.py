"""Bounded fixed-generation comparison evidence shared by storage owners."""
from __future__ import annotations

import collections
import gzip
import hashlib
import io
import json
from pathlib import Path

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError

ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT/'configs/data_sync/fast_latest_generation_migration.json'
MAX_WORKLIST_MEMBERS = 2_000_000
MAX_WORKLIST_LINE = 65_536
MAX_WORKLIST_TEXT_CHARS = 512 * 1024**2
MAX_WORKLIST_COMPRESSED_BYTES = 64 * 1024**2


def validate_worklist(plan: dict, raw: bytes) -> None:
    """Check complete path-level differences, without reading either payload."""
    if hashlib.sha256(raw).hexdigest() != plan.get('worklist', {}).get('sha256'):
        raise SnapshotError('latest fixed selection worklist SHA differs')
    expected = plan.get('counters', {})
    decisions = ('conflict', 'private_only', 'public_only', 'private_excluded')
    if (not isinstance(expected, dict) or any(type(expected.get(k)) is not int or expected[k] < 0 for k in decisions)
            or sum(expected[k] for k in decisions) > MAX_WORKLIST_MEMBERS):
        raise SnapshotError('latest fixed selection worklist count exceeds its bound')
    expected_members = sum(expected[k] for k in decisions)
    counts, categories, previous = collections.Counter(), collections.Counter(), None
    processed, decoded_chars = 0, 0
    with gzip.open(io.BytesIO(raw), 'rt', encoding='utf-8') as source:
        while line := source.readline(MAX_WORKLIST_LINE+1):
            processed += 1
            decoded_chars += len(line)
            if (len(line) > MAX_WORKLIST_LINE or decoded_chars > MAX_WORKLIST_TEXT_CHARS
                    or processed > expected_members):
                raise SnapshotError('latest fixed selection worklist exceeds its bound')
            item = json.loads(line)
            name, decision = item.get('path'), item.get('decision')
            if (not isinstance(name, str) or packed._safe_relative_path(name, 'comparison path').as_posix() != name
                    or previous is not None and name <= previous or decision not in decisions):
                raise SnapshotError('latest fixed selection worklist identity differs')
            previous = name
            counts[decision] += 1
            if decision == 'conflict':
                categories[item.get('category')] += 1
                if (not isinstance(item.get('public'), dict) or not isinstance(item.get('private'), dict)
                        or item['public'].get('kind') != 'file' or item['private'].get('kind') != 'file'):
                    raise SnapshotError('latest fixed selection changes an original path type')
    if (any(counts[key] != expected[key] for key in decisions)
            or dict(categories) != plan.get('conflict_categories')):
        raise SnapshotError('latest fixed selection worklist counts differ')
