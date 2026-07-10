from __future__ import annotations

import os
import numpy as np
import pytest
import threading
import time

import stockagent.data.panel_cache as panel_cache
from stockagent.data.panel import PanelData, _compute_source_hash
from stockagent.data.panel_cache import (
    load_panel_cache_v2,
    panel_cache_v2_is_valid,
    save_panel_cache_v2,
)


def test_panel_cache_v2_round_trips_memmap_payload(tmp_path) -> None:
    source = tmp_path / "AAA_features.parquet"
    source.write_bytes(b"parquet-placeholder")
    features = np.arange(5 * 3 * 2, dtype=np.float32).reshape(5, 3, 2)
    returns = np.linspace(-0.02, 0.02, 15, dtype=np.float32).reshape(5, 3)
    masks = np.ones((5, 3), dtype=bool)
    force_exit = np.zeros((5, 3), dtype=bool)
    force_exit[3, 1] = True
    panel = PanelData(
        dates=np.arange(5).astype("datetime64[D]"),
        symbols=["AAA", "BBB", "CCC"],
        feature_names=["f0", "f1"],
        features=features,
        returns_1d=returns,
        tradable_mask=masks,
        can_buy_mask=masks.copy(),
        can_sell_mask=masks.copy(),
        force_exit_mask=force_exit,
        alive_mask=masks.copy(),
        benchmark_returns=returns.mean(axis=1),
        close_prices=np.ones((5, 3), dtype=np.float32),
    )

    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="hash-v1",
        backend_key="pyarrow|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
    )

    assert panel_cache_v2_is_valid(
        tmp_path,
        source_hash="hash-v1",
        backend_key="pyarrow|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
        source_paths=[source],
    )
    payload = load_panel_cache_v2(tmp_path, mmap_mode="r")

    assert isinstance(payload["features"], np.memmap)
    assert payload["symbols"] == panel.symbols
    assert payload["feature_names"] == panel.feature_names
    assert np.array_equal(payload["features"], panel.features)
    assert np.array_equal(payload["returns_1d"], panel.returns_1d)
    assert np.array_equal(payload["tradable_mask"], panel.tradable_mask)
    assert np.array_equal(payload["force_exit_mask"], force_exit)


def test_source_hash_invalidates_same_size_replacement_with_preserved_mtime(tmp_path) -> None:
    source = tmp_path / "AAA_features.parquet"
    source.write_bytes(b"old-payload")
    original_stat = source.stat()
    original_hash = _compute_source_hash([source])

    source.write_bytes(b"new-payload")
    os.utime(source, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    assert source.stat().st_size == original_stat.st_size
    assert source.stat().st_mtime_ns == original_stat.st_mtime_ns
    assert _compute_source_hash([source]) != original_hash


def test_panel_cache_v2_invalidates_on_backend_key(tmp_path) -> None:
    source = tmp_path / "AAA_features.parquet"
    source.write_bytes(b"parquet-placeholder")
    masks = np.ones((2, 1), dtype=bool)
    panel = PanelData(
        dates=np.arange(2).astype("datetime64[D]"),
        symbols=["AAA"],
        feature_names=["f0"],
        features=np.zeros((2, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((2, 1), dtype=np.float32),
        tradable_mask=masks,
        can_buy_mask=masks.copy(),
        can_sell_mask=masks.copy(),
        alive_mask=masks.copy(),
        benchmark_returns=np.zeros((2,), dtype=np.float32),
        close_prices=np.ones((2, 1), dtype=np.float32),
    )
    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="hash-v1",
        backend_key="pyarrow|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
    )

    payload = load_panel_cache_v2(tmp_path, mmap_mode="r")
    assert not np.asarray(payload["force_exit_mask"]).any()

    assert not panel_cache_v2_is_valid(
        tmp_path,
        source_hash="hash-v1",
        backend_key="polars|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
        source_paths=[source],
    )


def test_failed_cache_refresh_keeps_previous_generation_readable(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "AAA_features.parquet"
    source.write_bytes(b"parquet-placeholder")
    masks = np.ones((2, 1), dtype=bool)
    old_features = np.asarray([[[1.0]], [[2.0]]], dtype=np.float32)
    panel = PanelData(
        dates=np.arange(2).astype("datetime64[D]"),
        symbols=["AAA"],
        feature_names=["f0"],
        features=old_features,
        returns_1d=np.zeros((2, 1), dtype=np.float32),
        tradable_mask=masks,
        can_buy_mask=masks.copy(),
        can_sell_mask=masks.copy(),
        alive_mask=masks.copy(),
        benchmark_returns=np.zeros((2,), dtype=np.float32),
        close_prices=np.ones((2, 1), dtype=np.float32),
    )
    backend_key = "pyarrow|atomic-generation-test"
    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="hash-v1",
        backend_key=backend_key,
        version=123,
    )

    original_save_array = panel_cache._save_array

    def fail_during_second_array(cache_dir, name, array):
        if name == "returns_1d":
            raise OSError("simulated interrupted cache rebuild")
        return original_save_array(cache_dir, name, array)

    monkeypatch.setattr(panel_cache, "_save_array", fail_during_second_array)
    panel.features = np.asarray([[[9.0]], [[9.0]]], dtype=np.float32)
    with pytest.raises(OSError, match="interrupted cache rebuild"):
        save_panel_cache_v2(
            tmp_path,
            panel,
            source_hash="hash-v1",
            backend_key=backend_key,
            version=123,
        )

    assert panel_cache_v2_is_valid(
        tmp_path,
        source_hash="hash-v1",
        backend_key=backend_key,
        version=123,
        source_paths=[source],
    )
    payload = load_panel_cache_v2(tmp_path, mmap_mode="r")
    assert np.array_equal(payload["features"], old_features)


def test_reader_retries_when_sampled_generation_is_reclaimed(
    tmp_path,
    monkeypatch,
) -> None:
    source = tmp_path / "AAA_features.parquet"
    source.write_bytes(b"parquet-placeholder")
    masks = np.ones((2, 1), dtype=bool)

    def make_panel(value: float) -> PanelData:
        return PanelData(
            dates=np.arange(2).astype("datetime64[D]"),
            symbols=["AAA"],
            feature_names=["f0"],
            features=np.full((2, 1, 1), value, dtype=np.float32),
            returns_1d=np.zeros((2, 1), dtype=np.float32),
            tradable_mask=masks,
            can_buy_mask=masks.copy(),
            can_sell_mask=masks.copy(),
            alive_mask=masks.copy(),
            benchmark_returns=np.zeros((2,), dtype=np.float32),
            close_prices=np.ones((2, 1), dtype=np.float32),
        )

    backend_key = "pyarrow|reader-generation-race"
    save_panel_cache_v2(
        tmp_path,
        make_panel(1.0),
        source_hash="hash-v1",
        backend_key=backend_key,
        version=123,
    )
    stale_meta = panel_cache.read_panel_cache_v2_meta(tmp_path)
    assert stale_meta is not None
    original_read_meta = panel_cache.read_panel_cache_v2_meta
    read_count = 0

    def stale_meta_then_latest(parquet_root):
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            save_panel_cache_v2(
                tmp_path,
                make_panel(9.0),
                source_hash="hash-v1",
                backend_key=backend_key,
                version=123,
            )
            return stale_meta
        return original_read_meta(parquet_root)

    monkeypatch.setattr(
        panel_cache,
        "read_panel_cache_v2_meta",
        stale_meta_then_latest,
    )
    payload = load_panel_cache_v2(tmp_path, mmap_mode="r")

    assert read_count >= 2
    assert np.array_equal(
        payload["features"],
        np.full((2, 1, 1), 9.0, dtype=np.float32),
    )


def test_cache_writer_lock_serializes_two_writers(tmp_path, monkeypatch) -> None:
    first_entered = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    call_order: list[str] = []
    failures: list[BaseException] = []

    def fake_locked_save(
        parquet_root,
        panel_like,
        *,
        source_hash,
        backend_key,
        version,
    ):
        del panel_like, backend_key, version
        call_order.append(source_hash)
        if source_hash == "writer-a":
            first_entered.set()
            if not release_first.wait(timeout=5.0):
                raise TimeoutError("writer-a was not released")
        return panel_cache.panel_cache_v2_dir(parquet_root)

    monkeypatch.setattr(
        panel_cache,
        "_save_panel_cache_v2_locked",
        fake_locked_save,
    )

    def run_writer(name: str) -> None:
        if name == "writer-b":
            second_started.set()
        try:
            save_panel_cache_v2(
                tmp_path,
                object(),
                source_hash=name,
                backend_key="lock-test",
                version=123,
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    writer_a = threading.Thread(target=run_writer, args=("writer-a",))
    writer_b = threading.Thread(target=run_writer, args=("writer-b",))
    writer_a.start()
    assert first_entered.wait(timeout=5.0)
    writer_b.start()
    assert second_started.wait(timeout=5.0)
    time.sleep(0.05)
    assert call_order == ["writer-a"]
    release_first.set()
    writer_a.join(timeout=5.0)
    writer_b.join(timeout=5.0)

    assert not writer_a.is_alive()
    assert not writer_b.is_alive()
    assert failures == []
    assert call_order == ["writer-a", "writer-b"]
