import gzip
import hashlib
import json

import pytest

from scripts import promote_latest_fixed_generation as latest
from stockagent.data_sync.desync_snapshots import SnapshotError


def worklist(row):
    return gzip.compress((json.dumps(row)+'\n').encode())


def plan(raw):
    return {'worklist': {'sha256': hashlib.sha256(raw).hexdigest()},
            'counters': {'conflict': 1, 'private_only': 0, 'public_only': 0, 'private_excluded': 0},
            'conflict_categories': {'parquet': 1}}


def test_latest_selection_checks_every_changed_path_and_source_kind():
    row = {'path': '1m/BTC.parquet', 'decision': 'conflict', 'category': 'parquet',
           'public': {'kind': 'file'}, 'private': {'kind': 'file'}}
    raw = worklist(row)
    latest.validate_worklist(plan(raw), raw)
    with pytest.raises(SnapshotError, match='SHA'):
        latest.validate_worklist(plan(raw), raw+b'drift')
    bad = worklist({**row, 'private': {'kind': 'directory'}})
    with pytest.raises(SnapshotError, match='path type'):
        latest.validate_worklist(plan(bad), bad)
    bad = worklist({**row, 'path': '../outside'})
    with pytest.raises(SnapshotError):
        latest.validate_worklist(plan(bad), bad)
    bad_plan = plan(raw); bad_plan['counters']['conflict'] = 2
    with pytest.raises(SnapshotError, match='counts'):
        latest.validate_worklist(bad_plan, raw)


def test_latest_worklist_streams_more_than_legacy_4096_names_and_rejects_duplicates():
    rows = [{'path':f'us_stocks/{i:05}.parquet', 'decision':'conflict', 'category':'parquet',
             'public':{'kind':'file'}, 'private':{'kind':'file'}} for i in range(12_000)]
    raw = gzip.compress(''.join(json.dumps(row)+'\n' for row in rows).encode())
    expected = {'worklist':{'sha256':hashlib.sha256(raw).hexdigest()},
        'counters':{'conflict':len(rows), 'private_only':0, 'public_only':0, 'private_excluded':0},
        'conflict_categories':{'parquet':len(rows)}}
    latest.validate_worklist(expected, raw)
    rows[-1] = rows[-2]
    raw = gzip.compress(''.join(json.dumps(row)+'\n' for row in rows).encode())
    expected['worklist']['sha256'] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(SnapshotError, match='identity'):
        latest.validate_worklist(expected, raw)
