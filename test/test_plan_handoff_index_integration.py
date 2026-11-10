import json
from pathlib import Path

import pytest

from scripts import plan_handoff_index_integration as planner, publish_handoff_index_archive as publisher
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync import packed_snapshots
from test_publish_handoff_index_archive import setup_cli


def test_bybit_public_comparison_accepts_only_the_exact_live_catalog_alias(tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_source_handoff as handoff

    physical = tmp_path / 'service_data/stockagent-live/data_bybit'
    physical.mkdir(parents=True)
    alias = tmp_path / 'data_bybit'
    alias.symlink_to(physical, target_is_directory=True)
    entry = {'dataset': 'bybit', 'source': 'data_bybit', 'publish': True, 'excluded_subtrees': ['cache']}
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    monkeypatch.setattr(planner.source_cli, 'ENROLLED', {handoff.BYBIT_NAME: ('bybit', [], True)})
    monkeypatch.setattr(planner, '_load_catalog', lambda _path: [entry])
    monkeypatch.setattr(planner, '_source_path', lambda _entry: alias)
    monkeypatch.setattr(planner.p, 'sha256_file', lambda _path: 'a' * 64)
    assert planner.policy('bybit') == (('cache',), 'a' * 64)
    alias.unlink()
    other = tmp_path / 'other'
    other.mkdir()
    alias.symlink_to(other, target_is_directory=True)
    with pytest.raises(SnapshotError, match='another source root'):
        planner.policy('bybit')


def test_integration_plan_uses_D_receipts_and_does_not_change_public_head(tmp_path, monkeypatch):
    public, private, stage, _status, old = setup_cli(tmp_path, monkeypatch)
    publisher.publish('fixture')
    replacement = tmp_path/'new-public-source'
    replacement.mkdir()
    (replacement/'changed.parquet').write_bytes(b'public-current')
    current = packed_snapshots.publish_packed_snapshot(public, 'fixture', replacement, node_id='test')
    monkeypatch.setattr(planner, 'COLD', public)
    monkeypatch.setattr(planner, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(planner, 'private_history_root', lambda *_a, **_kw: private)
    monkeypatch.setattr(planner, 'version_policy', lambda *_a: {})
    monkeypatch.setattr(planner, 'policy', lambda *_a: ((), 'b'*64))
    result = planner.plan('fixture')
    assert result['state'] == 'metadata_compared' and result['counters']['public_only'] == 1
    assert not result['semantic_merge_complete'] and not result['source_payload_read']
    assert packed_snapshots.resolve_latest_packed(public, 'fixture').manifest_sha256 == current.manifest_sha256
    assert json.loads((stage.parent.parent/'fixture.diff-status.json').read_text())['catalog_sha256'] == 'b'*64
