from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path

import pytest

from stockagent.live import data_monitor_feature_shards as shards
from stockagent.live import data_monitor_inventory as inventory
from stockagent.live.data_monitor_dashboard import build_data_monitor_feature_inventory
from stockagent.live.data_monitor_feature_pages import feature_page_projections


def _write(path: Path, *, values=(1.0, None)) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"date": [date(2026, 9, 30), date(2026, 10, 1)],
                            "close": list(values)}), path)


@pytest.fixture
def setup(tmp_path):
    first = tmp_path / "data_yahoo/us_stocks/A_features.parquet"
    second = tmp_path / "data_yahoo/crypto/B_features.parquet"
    _write(first)
    _write(second)
    status = {
        "generated_at_utc": "2026-10-02T12:00:00+00:00",
        "sources": [
            {"id": "yahoo:us_stocks", "title": "美股", "provider": "Yahoo", "market_category": "us_stock"},
            {"id": "yahoo:crypto", "title": "Crypto", "provider": "Yahoo", "market_category": "crypto"},
        ],
    }
    return tmp_path, first, second, status, tmp_path / "artifacts/live/data_monitor/feature_inventory.json"


def _snapshot(root):
    frame = inventory.InventorySnapshot(root)
    inventory.build_record_inventory(root, refresh=True, snapshot=frame)
    return frame


def _publish(root, output, status, frame=None, **kwargs):
    frame = frame or _snapshot(root)
    result = shards.publish_feature_shards(root, output, snapshot=frame, monitor_status=status, **kwargs)
    full = build_data_monitor_feature_inventory(
        root, monitor_status=status,
        inventory=inventory.build_feature_inventory(root, snapshot=frame),
    )
    assert output.read_bytes() == shards._encode(full) + b"\n"
    assert (result.preview, result.source_pages) == feature_page_projections(full)
    assert result.fields == len(full["rows"])
    assert result.source_observation_root == inventory.build_feature_inventory(root, snapshot=frame)["source_observation_root"]
    return result


def test_cold_exact_public_bytes_and_warm_reuse(setup, monkeypatch):
    root, _, _, status, output = setup
    cold = _publish(root, output, status)
    assert cold.reused_datasets == 0
    frame = _snapshot(root)
    assert frame.changed_dataset_ids == frozenset()
    calls = []
    original = inventory.FeatureDataset.rows

    def tracked(self, **kwargs):
        calls.append(self.dataset_id)
        return original(self, **kwargs)

    monkeypatch.setattr(inventory.FeatureDataset, "rows", tracked)
    warm = shards.publish_feature_shards(root, output, snapshot=frame, monitor_status=status)
    assert warm.rebuilt_datasets == 0
    assert warm.reused_datasets == cold.rebuilt_datasets
    assert calls == []  # verification still happened; only reductions are skipped


def test_changed_owner_only_and_labels_invalidate_locally(setup):
    root, first, second, status, output = setup
    _publish(root, output, status)
    _write(second, values=(2.0, 3.0))
    frame = _snapshot(root)
    assert frame.changed_dataset_ids == frozenset({"yahoo:crypto"})
    assert all(isinstance(pair, list) for schema in frame.payload["schemas"].values() for pair in schema)
    changed = _publish(root, output, status, frame)
    assert changed.rebuilt_datasets == 1
    assert changed.reused_datasets > 0
    status["sources"][0]["title"] = "新標籤"
    labels = _publish(root, output, status)
    assert labels.rebuilt_datasets == 1
    assert first.exists()


def test_source_disappears_and_recovers_without_cache_revision_change(setup):
    root, first, _, status, output = setup
    _publish(root, output, status)
    frame = _snapshot(root)
    hidden = first.with_suffix(".hidden")
    first.rename(hidden)
    try:
        partial = _publish(root, output, status, frame)
        assert partial.rebuilt_datasets == 1
        assert json.loads(output.read_bytes())["summary"]["state"] == "partial"
    finally:
        hidden.rename(first)
    recovered = _publish(root, output, status)
    assert recovered.rebuilt_datasets == 1
    assert json.loads(output.read_bytes())["summary"]["state"] == "complete"


@pytest.mark.parametrize("change", ["base", "unknown_delta", "abi", "corrupt_manifest"])
def test_unproven_generation_rebuilds_all(setup, monkeypatch, change):
    root, _, _, status, output = setup
    cold = _publish(root, output, status)
    frame = _snapshot(root)
    if change == "base":
        frame.previous_cache_signature = [0, 0, 0, 0, 0]
    elif change == "unknown_delta":
        frame.changed_dataset_ids = None
    elif change == "abi":
        monkeypatch.setattr(shards, "projection_abi", lambda: "new-abi")
    else:
        store = shards.ProjectionStore(output)
        (store.root / "current.json").write_bytes(b"{torn")
    result = _publish(root, output, status, frame)
    assert result.reused_datasets == 0
    assert result.rebuilt_datasets == cold.rebuilt_datasets


def test_bad_object_recovers_by_canonical_cold_rebuild(setup):
    root, _, _, status, output = setup
    _publish(root, output, status)
    old = output.read_bytes()
    store = shards.ProjectionStore(output)
    owner = store.manifest()["datasets"]["yahoo:crypto"]
    (store.objects / f"{owner['sha256']}.rows").write_bytes(b"torn")
    result = _publish(root, output, status)
    assert result.reused_datasets == 0
    assert "object_recovery_total" in result.timing_ms
    assert output.read_bytes() == old
    assert not list(output.parent.glob("*.shards.*.tmp"))


def test_interrupted_atomic_publication_keeps_previous_json(setup, monkeypatch):
    root, _, second, status, output = setup
    _publish(root, output, status)
    old = output.read_bytes()
    _write(second, values=(2.0, 3.0))

    def interrupted(*args, **kwargs):
        raise OSError("injected before atomic rename")

    monkeypatch.setattr(shards, "durable_replace", interrupted)
    with pytest.raises(OSError, match="injected"):
        shards.publish_feature_shards(root, output, snapshot=_snapshot(root), monitor_status=status)
    assert output.read_bytes() == old
    assert not list(output.parent.glob(".*.shards.*.tmp"))


def test_cache_moves_between_quick_frame_and_decode_cannot_reuse_empty_delta(setup):
    root, _, second, status, output = setup
    _publish(root, output, status)
    old_frame = _snapshot(root)
    assert old_frame.payload is None
    _write(second, values=(2.0, 3.0))
    _snapshot(root)
    result = _publish(root, output, status, old_frame)
    assert result.reused_datasets == 0
    assert old_frame.changed_dataset_ids is None


def test_native_integer_reductions_are_not_float_or_uint_overflow():
    count = 2**63 + 13
    owner = inventory.FeatureDataset(
        "dataset:large", 2, 2, "0" * 32,
        {"schema": [{"count": count, "non_null": [count]}, {"count": count, "non_null": [count]}]},
        {"schema": [["x", "uint64"]]}, {},
    )
    row = owner.rows()[0]
    assert type(row["non_null_count"]) is int
    assert row["non_null_count"] == row["rows_with_field"] == count * 2


@pytest.mark.parametrize("prefer_shards", [True, False])
def test_shared_producer_publishes_trusted_receipt_and_complete_pages(setup, monkeypatch, prefer_shards):
    from scripts import snapshot_data_refresh_services as producer
    from stockagent.live.data_monitor_feature_receipt import trusted_feature_snapshot
    from stockagent.live.data_monitor_dashboard import feature_source_metadata_sha256

    root, _, _, status, output = setup
    frame = _snapshot(root)
    expected = build_data_monitor_feature_inventory(
        root, monitor_status=status, inventory=inventory.build_feature_inventory(root, snapshot=frame),
    )
    monkeypatch.setattr(producer, "REPO_ROOT", root)
    result = producer.publish_feature_inventory_snapshot(
        root, output, snapshot=frame, public_status=status,
        feature_revision=frame.payload["feature_revision"],
        source_metadata_sha256=feature_source_metadata_sha256(status, frame.payload["datasets"]),
        prefer_shards=prefer_shards,
    )
    assert json.loads(output.read_bytes()) == expected
    assert trusted_feature_snapshot(output, source_stat=output.stat(), body=output.read_bytes())
    assert result["projection_cache"]["state"] == ("published" if prefer_shards else "full_build")
    assert frame.payload is None and frame.selected is None


def test_shared_producer_falls_back_without_corrupting_public_contract(setup, monkeypatch):
    from scripts import snapshot_data_refresh_services as producer
    from stockagent.live.data_monitor_dashboard import feature_source_metadata_sha256

    root, _, _, status, output = setup
    frame = _snapshot(root)
    expected = build_data_monitor_feature_inventory(
        root, monitor_status=status, inventory=inventory.build_feature_inventory(root, snapshot=frame),
    )

    def unavailable(*args, **kwargs):
        raise OSError("injected cache unavailable")

    monkeypatch.setattr(shards, "publish_feature_shards", unavailable)
    result = producer.publish_feature_inventory_snapshot(
        root, output, snapshot=frame, public_status=status,
        feature_revision=frame.payload["feature_revision"],
        source_metadata_sha256=feature_source_metadata_sha256(status, frame.payload["datasets"]),
    )
    assert result["projection_cache"]["state"] == "full_fallback"
    assert result["projection_cache"]["error_type"] == "OSError"
    assert json.loads(output.read_bytes()) == expected


def test_complete_source_and_receipt_publication_share_lock(setup, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    from scripts import snapshot_data_refresh_services as producer

    root, _, _, status, output = setup
    entered = threading.Event()
    second_started = threading.Event()
    release = threading.Event()
    active = 0
    maximum = 0
    calls = 0

    def publisher(*args, **kwargs):
        nonlocal active, maximum, calls
        active += 1
        maximum = max(maximum, active)
        calls += 1
        entered.set()
        assert release.wait(3)
        active -= 1
        return {"fields": 4}

    def invoke(second=False):
        if second:
            second_started.set()
        return producer.publish_feature_inventory_snapshot(
            root, output, snapshot=inventory.InventorySnapshot(root),
            public_status=status, feature_revision=None, source_metadata_sha256="test",
        )

    monkeypatch.setattr(producer, "_publish_feature_inventory_snapshot", publisher)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(invoke)
        assert entered.wait(3)
        second = pool.submit(invoke, True)
        assert second_started.wait(3)
        assert not second.done()
        release.set()
        assert first.result(timeout=3) == second.result(timeout=3) == {"fields": 4}
    assert calls == 2 and maximum == 1


def test_publication_lock_refuses_symlink_without_overwriting_target(setup):
    from scripts import snapshot_data_refresh_services as producer

    root, _, _, status, output = setup
    output.parent.mkdir(parents=True, exist_ok=True)
    target = root / "protected-source.txt"
    target.write_bytes(b"unique source")
    output.with_name(f".{output.name}.publication.lock").symlink_to(target)
    with pytest.raises(OSError):
        producer.publish_feature_inventory_snapshot(
            root, output, snapshot=inventory.InventorySnapshot(root),
            public_status=status, feature_revision=None, source_metadata_sha256="test",
        )
    assert target.read_bytes() == b"unique source"


def test_capacity_failure_keeps_public_source_and_does_not_grow_unbounded(setup):
    root, _, _, status, output = setup
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"original public source")
    with pytest.raises(ValueError, match="capacity exhausted"):
        shards.publish_feature_shards(root, output, snapshot=_snapshot(root), monitor_status=status, max_cache_bytes=64)
    assert output.read_bytes() == b"original public source"
    store = shards.ProjectionStore(output)
    assert sum(path.stat().st_size for path in store.objects.iterdir()) <= 64


def test_object_eviction_scope_keeps_unknown_shared_and_tampered_bytes(setup):
    root, _, _, status, output = setup
    _publish(root, output, status)
    store = shards.ProjectionStore(output)
    orphan = b"generated orphan"
    digest = hashlib.sha256(orphan).hexdigest()
    safe = store.objects / f"{digest}.rows"
    safe.write_bytes(orphan)
    unknown = store.objects / "do-not-delete.parquet"
    unknown.write_bytes(b"unique source")
    corrupt = store.objects / f"{'a' * 64}.rows"
    corrupt.write_bytes(b"unmatched content")
    shared = store.objects / f"{hashlib.sha256(b'shared').hexdigest()}.rows"
    shared.write_bytes(b"shared")
    (root / "shared-reference").hardlink_to(shared)
    with store.locked():
        store.prune()
    assert not safe.exists()
    assert unknown.exists() and corrupt.exists() and shared.exists()


def test_incomplete_scan_is_not_publishable_and_single_use(setup):
    root, _, _, _, _ = setup
    scan = inventory.FeatureInventoryScan(root, snapshot=_snapshot(root))
    iterator = iter(scan)
    next(iterator)
    with pytest.raises(ValueError, match="incomplete"):
        scan.summary(datasets_with_schema=0)
    with pytest.raises(ValueError, match="single-use"):
        next(iter(scan))
