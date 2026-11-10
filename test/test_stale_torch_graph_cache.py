import os
import time

import pytest

from scripts import reclaim_stale_torch_graph_cache as cleanup
from stockagent.data_sync.desync_snapshots import SnapshotError


def cache_file(root, bucket, *, age_days):
    directory = root/bucket
    directory.mkdir(parents=True)
    path = directory/'compiled-graph'
    path.write_bytes(b'rebuildable cache')
    stamp = time.time()-age_days*86400
    os.utime(path, (stamp, stamp))
    os.utime(directory, (stamp, stamp))
    return path


def test_only_stale_reproducible_graph_cache_is_removed(tmp_path):
    root = tmp_path/'fxgraph'
    old = cache_file(root, 'old', age_days=9)
    recent = cache_file(root, 'recent', age_days=1)
    journal = tmp_path/'journals'/'retirement.json'
    plan = cleanup.reclaim(root, references=lambda _paths: [])
    assert plan['candidate_files'] == 1 and old.exists() and recent.exists()
    result = cleanup.reclaim(root, apply=True, journal=journal, references=lambda _paths: [])
    assert result['state'] == 'completed' and result['removed_files'] == 1
    assert result['source_data_deleted'] is False and not old.exists() and recent.exists()


def test_active_or_changed_cache_is_not_removed(tmp_path):
    root = tmp_path/'fxgraph'
    old = cache_file(root, 'old', age_days=9)
    with pytest.raises(SnapshotError, match='running process'):
        cleanup.reclaim(root, references=lambda _paths: ['active'])
    calls = 0
    def changed(_paths):
        nonlocal calls
        calls += 1
        if calls == 2:
            old.write_bytes(b'new active generation')
        return []
    with pytest.raises(SnapshotError, match='changed'):
        cleanup.reclaim(root, apply=True, journal=tmp_path/'journals'/'status.json', references=changed)
    assert old.read_bytes() == b'new active generation'
