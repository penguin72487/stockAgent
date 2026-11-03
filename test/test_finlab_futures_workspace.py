from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest

from scripts import download_finlab_intraday as intraday
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError


KEY = "tw_futures_minute:I5F_201611"
DAY = date(2016, 11, 8)


def frame():
    result = pd.DataFrame({
        "stock_id": ["I5F_201611"], "trade_date": ["2016-11-08"],
        "timestamp": pd.to_datetime(["2016-11-08T08:45:00+08:00"]),
        "session": ["regular"], "open": [100.], "high": [100.],
        "low": [100.], "close": [100.], "volume": [1],
    })
    result.attrs["volume_scope"] = "outright_only"
    return result


def configure(tmp_path, monkeypatch):
    alias = tmp_path / "intraday/objects/futures-workspace-v1"
    alias.mkdir(parents=True)
    monkeypatch.setattr(workspace_volume, "FINLAB_FUTURES_ALIAS", alias)
    monkeypatch.setattr(workspace_volume, "check_finlab_futures", lambda: {"target": str(alias)})
    return alias


def test_futures_objects_use_new_relative_receipt_namespace(tmp_path, monkeypatch):
    alias = configure(tmp_path, monkeypatch)
    receipt = intraday.persist_partition_frame(tmp_path, KEY, DAY, frame(), futures_workspace=True)
    target = tmp_path / receipt["parquet_path"]
    assert target.parent == alias and target.is_file()
    assert receipt["storage_contract"] == "finlab_futures_workspace_objects_v1"
    assert receipt["publication_time_status"] == "not_verified_for_training"
    path, _ = intraday._partition_paths(tmp_path, KEY, DAY)
    assert intraday._stored_receipt(path, tmp_path, KEY, DAY, verify_hash=True) == receipt


def test_missing_mount_stops_before_provider_and_no_receipt(tmp_path, monkeypatch):
    import finlab.data

    configure(tmp_path, monkeypatch)
    monkeypatch.setattr(workspace_volume, "check_finlab_futures",
                        lambda: (_ for _ in ()).throw(SnapshotError("mount missing")))
    monkeypatch.setattr(finlab.data, "get", lambda *a, **k: pytest.fail("provider must not be called"))
    with pytest.raises(intraday.FuturesWorkspaceUnavailableBeforeSource):
        intraday.fetch_partition(tmp_path, KEY, DAY, now=datetime.now(UTC), futures_workspace=True)
    assert not intraday._partition_paths(tmp_path, KEY, DAY)[0].exists()


def test_lost_mount_after_object_write_never_publishes_receipt(tmp_path, monkeypatch):
    alias = configure(tmp_path, monkeypatch)
    calls = []
    def check():
        calls.append(1)
        if len(calls) > 1:
            raise SnapshotError("mount disappeared")
        return {"target": str(alias)}
    monkeypatch.setattr(workspace_volume, "check_finlab_futures", check)
    with pytest.raises(SnapshotError, match="mount disappeared"):
        intraday.persist_partition_frame(tmp_path, KEY, DAY, frame(), futures_workspace=True)
    assert len(list(alias.glob("*.parquet"))) == 1  # retained, never silently reclassified
    assert not intraday._partition_paths(tmp_path, KEY, DAY)[0].exists()


def test_futures_workspace_rejects_stock_or_another_root(tmp_path, monkeypatch):
    configure(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="canonical futures"):
        intraday._futures_workspace_objects(tmp_path, "tw_minute:2330")
    with pytest.raises(ValueError, match="canonical futures"):
        intraday._futures_workspace_objects(tmp_path / "other", KEY)


def test_existing_workspace_receipt_refuses_unmounted_authority_namespace(tmp_path, monkeypatch):
    root = tmp_path / 'data_finlab'
    configure(root, monkeypatch)
    monkeypatch.setattr(intraday, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(intraday.socket, 'gethostname', lambda: 'penguin')
    intraday.persist_partition_frame(root, KEY, DAY, frame(), futures_workspace=True)
    monkeypatch.setattr(workspace_volume, 'check_finlab_futures',
                        lambda: (_ for _ in ()).throw(SnapshotError('missing mount')))
    with pytest.raises(SnapshotError, match='missing mount'):
        intraday._stored_receipt(intraday._partition_paths(root, KEY, DAY)[0], root, KEY, DAY)


@pytest.mark.parametrize('error', [ValueError('bad marker JSON'), OSError('unreadable marker')])
def test_existing_workspace_receipt_never_treats_unreadable_identity_as_missing(
        tmp_path, monkeypatch, error):
    root = tmp_path / 'data_finlab'
    configure(root, monkeypatch)
    monkeypatch.setattr(intraday, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(intraday.socket, 'gethostname', lambda: 'penguin')
    receipt = intraday.persist_partition_frame(root, KEY, DAY, frame(), futures_workspace=True)
    receipt_path, _ = intraday._partition_paths(root, KEY, DAY)
    before = receipt_path.read_bytes()

    def fail_identity():
        raise error

    monkeypatch.setattr(workspace_volume, 'check_finlab_futures', fail_identity)
    with pytest.raises(SnapshotError, match='identity could not be verified'):
        intraday._stored_receipt(receipt_path, root, KEY, DAY)
    assert receipt_path.read_bytes() == before
    assert (root / receipt['parquet_path']).is_file()


def test_workspace_identity_checked_before_malformed_receipt_path(tmp_path, monkeypatch):
    root = tmp_path / 'data_finlab'
    configure(root, monkeypatch)
    monkeypatch.setattr(intraday, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(intraday.socket, 'gethostname', lambda: 'penguin')
    receipt_path, _ = intraday._partition_paths(root, KEY, DAY)
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(
        '{"dataset":"tw_futures_minute:I5F_201611","trade_date":"2016-11-08",'
        '"status":"downloaded_unverified_for_pit",'
        '"storage_contract":"finlab_futures_workspace_objects_v1"}'
    )
    monkeypatch.setattr(workspace_volume, 'check_finlab_futures',
                        lambda: (_ for _ in ()).throw(OSError('unreadable marker')))
    with pytest.raises(SnapshotError, match='identity could not be verified'):
        intraday._stored_receipt(receipt_path, root, KEY, DAY)


def test_dispatch_requires_both_control_headroom_and_the_actual_workspace(tmp_path, monkeypatch):
    from scripts import check_acquisition_capacity as capacity

    root = tmp_path / 'data_finlab'
    root.mkdir()
    monkeypatch.setattr(capacity, 'ROOT', tmp_path)
    states = []
    monkeypatch.setattr(capacity, 'priority_dispatch_status', lambda *a, **kw:
                        states.append(kw['additional_headroom_bytes']) or {'state': 'admitted'})
    monkeypatch.setattr(workspace_volume, 'check_finlab_futures', lambda: {'target': '/enrolled/alias'})
    monkeypatch.setattr(workspace_volume, 'admit', lambda: {'filesystem_available_bytes': 100})
    assert capacity.futures_workspace_dispatch_status(root)['state'] == 'admitted'
    assert states == [24 * 1024**3]
    monkeypatch.setattr(workspace_volume, 'admit',
                        lambda: (_ for _ in ()).throw(SnapshotError('missing workspace')))
    assert capacity.futures_workspace_dispatch_status(root)['reason'] == 'workspace_unavailable'
    monkeypatch.setattr(capacity, 'priority_dispatch_status', lambda *a, **kw: {'state': 'deferred'})
    assert capacity.futures_workspace_dispatch_status(root)['reason'] == 'control_capacity_unavailable'
