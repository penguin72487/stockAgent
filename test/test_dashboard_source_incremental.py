"""Every monitor shares per-source invalidation, never stale health evidence."""
from concurrent.futures import ThreadPoolExecutor
import json
import marshal
import os
from pathlib import Path

import pytest

from stockagent.live import dashboard_updates as snapshots


@pytest.fixture(autouse=True)
def isolated_cache():
    with snapshots._JSON_SNAPSHOT_LOCK:
        snapshots._JSON_SNAPSHOTS.clear()
        snapshots._JSON_RESIDENT_BYTES = 0
    yield
    with snapshots._JSON_SNAPSHOT_LOCK:
        snapshots._JSON_SNAPSHOTS.clear()
        snapshots._JSON_RESIDENT_BYTES = 0


def test_only_changed_daily_source_is_decoded_and_values_are_isolated(tmp_path):
    paths = [tmp_path / f"day-{day}.json" for day in range(100)]
    for number, path in enumerate(paths):
        path.write_text(json.dumps({"day": number, "value": [None, -0.0, 1.123456789]}))
    before = snapshots.json_snapshot_metrics()
    first = [snapshots.read_json_snapshot(path) for path in paths]
    first[0]["value"].append("caller mutation")
    assert [snapshots.read_json_snapshot(path) for path in paths][0]["value"] == [None, -0.0, 1.123456789]
    paths[-1].write_text('{"day": 99, "value": [2]}')
    second = [snapshots.read_json_snapshot(path) for path in paths]
    assert second[-1]["value"] == [2]
    after = snapshots.json_snapshot_metrics()
    assert after["decodes"] - before["decodes"] == 101
    assert after["hits"] - before["hits"] == 199


@pytest.mark.parametrize("change", ["replace", "same_size_mtime", "truncate", "remove", "partial", "symlink"])
def test_each_source_generation_invalidates_without_resurrecting_old_data(tmp_path, change):
    path = tmp_path / "receipt.json"
    path.write_text('{"value":1}')
    assert snapshots.read_json_snapshot(path) == {"value": 1}
    original = path.stat()
    if change == "replace":
        pending = tmp_path / "pending.json"
        pending.write_text('{"value":2}')
        pending.replace(path)
    elif change == "same_size_mtime":
        path.write_text('{"value":2}')
        os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    elif change == "symlink":
        target = tmp_path / "other.json"
        target.write_text('{"value":2}')
        path.unlink()
        path.symlink_to(target)
    elif change == "truncate":
        path.write_text("null")
    elif change == "remove":
        path.unlink()
    else:
        path.write_text('{"value":')
    result = snapshots.read_json_snapshot(path, "missing")
    assert result == ({"value": 2} if change in {"replace", "same_size_mtime", "symlink"}
                      else None if change == "truncate" else "missing")
    if change == "partial":
        path.write_text('{"value":3}')
        assert snapshots.read_json_snapshot(path) == {"value": 3}


def test_source_single_flight_and_caller_size_limit(tmp_path):
    path = tmp_path / "receipt.json"
    path.write_text('{"nested":[1,2,3]}')
    before = snapshots.json_snapshot_metrics()
    with ThreadPoolExecutor(max_workers=16) as pool:
        result = list(pool.map(lambda _: snapshots.read_json_snapshot(path), range(64)))
    assert all(value == {"nested": [1, 2, 3]} for value in result)
    assert snapshots.json_snapshot_metrics()["decodes"] - before["decodes"] == 1
    assert snapshots.read_json_snapshot(path, "oversize", max_bytes=3) == "oversize"


def test_eviction_keeps_budget_and_nonregular_files_never_open(tmp_path, monkeypatch):
    per_empty = snapshots._json_resident_charge(marshal.dumps({}, 4))
    monkeypatch.setattr(snapshots, "_JSON_MAX_RESIDENT_BYTES", 2 * per_empty)
    for number in range(5):
        path = tmp_path / f"{number}.json"
        path.write_text("{}")
        assert snapshots.read_json_snapshot(path) == {}
    assert snapshots.json_snapshot_metrics()["entries"] == 2
    assert snapshots.json_snapshot_metrics()["resident_charge_bytes"] <= 2 * per_empty
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    assert snapshots.read_json_snapshot(fifo, "not regular") == "not regular"


def test_pretty_status_inventory_fits_real_container_budget_without_scan_thrashing(tmp_path):
    payload = {"rows": [{"symbol": str(i), "value": i / 7, "missing": None} for i in range(512)]}
    paths = [tmp_path / f"{i}.json" for i in range(64)]
    for path in paths:
        path.write_text(json.dumps(payload, indent=2))
    before = snapshots.json_snapshot_metrics()
    for _ in range(2):
        for path in paths:
            assert snapshots.read_json_snapshot(path) == payload
    after = snapshots.json_snapshot_metrics()
    assert after["decodes"] - before["decodes"] == 64
    assert after["hits"] - before["hits"] == 64
    assert after["resident_charge_bytes"] <= snapshots._JSON_MAX_RESIDENT_BYTES


def test_all_monitor_adapters_share_one_changed_source_owner(tmp_path):
    from stockagent.live import (finlab_dashboard, finmind_dashboard, news_dashboard,
        tej_dashboard, openbb_archive_dashboard, data_monitor_dashboard)
    path = tmp_path / "source.json"
    path.write_text('{"health":"degraded","count":42}')
    readers = [lambda p: finlab_dashboard._read_json(p, None), finmind_dashboard._read_json,
               news_dashboard._read, tej_dashboard._json,
               openbb_archive_dashboard._read_json_object, data_monitor_dashboard._read_json]
    before = snapshots.json_snapshot_metrics()
    for reader in readers:
        assert reader(path) == {"health": "degraded", "count": 42}
    assert snapshots.json_snapshot_metrics()["decodes"] - before["decodes"] == 1
    path.write_text('{"health":"critical","count":43}')
    assert all(reader(path) == {"health": "critical", "count": 43} for reader in readers)
    assert snapshots.json_snapshot_metrics()["decodes"] - before["decodes"] == 2
