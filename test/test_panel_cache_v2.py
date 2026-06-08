from __future__ import annotations

import numpy as np

from stockagent.data.panel import PanelData
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
    panel = PanelData(
        dates=np.arange(5).astype("datetime64[D]"),
        symbols=["AAA", "BBB", "CCC"],
        feature_names=["f0", "f1"],
        features=features,
        returns_1d=returns,
        tradable_mask=masks,
        can_buy_mask=masks.copy(),
        can_sell_mask=masks.copy(),
        alive_mask=masks.copy(),
        benchmark_returns=returns.mean(axis=1),
        close_prices=np.ones((5, 3), dtype=np.float32),
    )

    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="hash-v1",
        backend_key="pandas|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
    )

    assert panel_cache_v2_is_valid(
        tmp_path,
        source_hash="hash-v1",
        backend_key="pandas|benchmark=test|usd_only=False|tradable_mode=tradable",
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
        backend_key="pandas|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
    )

    assert not panel_cache_v2_is_valid(
        tmp_path,
        source_hash="hash-v1",
        backend_key="polars|benchmark=test|usd_only=False|tradable_mode=tradable",
        version=123,
        source_paths=[source],
    )
