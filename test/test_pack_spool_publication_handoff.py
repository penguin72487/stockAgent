"""A compute rollout cannot discard hours of an uncommitted source publisher."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import rollout_lakehouse_source_policy as rollout


def setup(tmp_path, monkeypatch, row):
    from stockagent.data_sync import packed_snapshots
    (tmp_path/'progress.json').write_text(json.dumps({'datasets': {'source':row}}))
    checkpoint = {'operation_directory': str(tmp_path), 'plan_sha256': 'a'*64}
    monkeypatch.setattr(rollout, 'migration_checkpoint', lambda _: checkpoint)
    monkeypatch.setattr(rollout, 'worker_code_identity', lambda:'fixed-code')
    monkeypatch.setattr(packed_snapshots, 'resolve_packed_snapshot_id',
                        lambda *a,**k:SimpleNamespace(manifest_sha256='b'*64))
    return tmp_path/'waiting.json'


def test_committed_exact_fixed_version_can_reach_the_original_restart_boundary(tmp_path, monkeypatch):
    output = setup(tmp_path,monkeypatch,{'snapshot_id':'source-fixed','manifest_sha256':'b'*64})
    result = rollout.wait_for_committed_publication(tmp_path,'source',0,'fixed-code',output)
    assert result['snapshot_id'] == 'source-fixed'
    assert result['publication_only_not_recovery_acceptance'] is True


def test_uncommitted_publisher_times_out_without_any_restart(tmp_path, monkeypatch):
    output = setup(tmp_path,monkeypatch,{'state':'processing'})
    with pytest.raises(TimeoutError,match='publication'):
        rollout.wait_for_committed_publication(tmp_path,'source',0,'fixed-code',output)


def test_changed_code_cannot_restart_an_active_publisher(tmp_path, monkeypatch):
    output = setup(tmp_path,monkeypatch,{'snapshot_id':'source-fixed','manifest_sha256':'b'*64})
    with pytest.raises(ValueError,match='candidate changed'):
        rollout.wait_for_committed_publication(tmp_path,'source',0,'other-code',output)


def test_unresolved_or_different_manifest_never_reaches_restart(tmp_path, monkeypatch):
    output = setup(tmp_path,monkeypatch,{'snapshot_id':'source-fixed','manifest_sha256':'c'*64})
    with pytest.raises(TimeoutError):
        rollout.wait_for_committed_publication(tmp_path,'source',0,'fixed-code',output)
