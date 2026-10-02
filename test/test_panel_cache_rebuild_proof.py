import json
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import verify_panel_cache_rebuild as proof
from stockagent.config import external_panel_data_kwargs, load_config
from stockagent.data.panel import build_panel


def fixture_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = Path("data_bybit/perpetual_daily")
    source.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "date": ["2026-09-20", "2026-09-21", "2026-09-22", "2026-09-23"],
                "Open": [100.0, 101.0, 102.0, 103.0],
                "High": [102.0, 103.0, 104.0, 105.0],
                "Low": [99.0, 100.0, 101.0, 102.0],
                "Close": [101.0, 102.0, 103.0, 104.0],
                "Adj Close": [101.0, 102.0, 103.0, 104.0],
                "Trading_Volume": [1000.0, 1001.0, 1002.0, 1003.0],
            }
        ),
        source / "BTCUSDT_features.parquet",
    )
    config = Path("fixture.yaml")
    config.write_text("""experiment_name: cache-proof-fixture
environment:
  device: cpu
  use_tensor_cores: false
  amp_dtype: fp32
trading:
  frequency: daily
  buy_fee_rate: 0.0005
  sell_fee_rate: 0.0005
  long_only: false
training:
  non_blocking_transfer: false
data:
  parquet_root: data_bybit/perpetual_daily
  panel_cache_root: artifacts/cache/test_bybit
  benchmark_name: BTCUSDT
  use_tw_public_features: false
  use_tw_public_rules: false
  use_external_features: false
  security_filter: none
  panel_backend: polars_lazy
  trading_volume_policy: required
""")
    cfg = load_config(config)
    build_panel(
        cfg.data.parquet_root,
        benchmark_name=cfg.data.benchmark_name,
        panel_cache_root=cfg.data.panel_cache_root,
        panel_backend=cfg.data.panel_backend,
        trading_volume_policy=cfg.data.trading_volume_policy,
        **external_panel_data_kwargs(cfg.data),
    )
    return config, source, Path("artifacts/cache/test_bybit/panel_cache_v2/meta.json")


def test_full_rebuild_matches_all_arrays_without_writing_another_cache(
    tmp_path, monkeypatch
):
    config, _, meta = fixture_config(tmp_path, monkeypatch)
    before = meta.read_bytes()
    generations = {p.name for p in (meta.parent / "generations").iterdir()}
    result = proof.verify(config, tmp_path / "receipt.json")
    assert result["exact_rebuild_verified"]
    assert not result["eviction_authorized"]
    assert meta.read_bytes() == before
    assert {p.name for p in (meta.parent / "generations").iterdir()} == generations


def test_changed_source_blocks_before_running_builder(tmp_path, monkeypatch):
    config, source, _ = fixture_config(tmp_path, monkeypatch)
    (source / "BTCUSDT_features.parquet").write_bytes(b"source changed")
    with patch.object(proof.panels, "build_panel") as builder:
        with pytest.raises(ValueError, match="exact source identity"):
            proof.verify(config, tmp_path / "receipt.json")
    builder.assert_not_called()


def test_old_abi_is_not_called_rebuildable(tmp_path, monkeypatch):
    config, _, meta = fixture_config(tmp_path, monkeypatch)
    row = json.loads(meta.read_text())
    row["version"] -= 1
    meta.write_text(json.dumps(row))
    with pytest.raises(ValueError, match="old panel ABI"):
        proof.verify(config, tmp_path / "receipt.json")
