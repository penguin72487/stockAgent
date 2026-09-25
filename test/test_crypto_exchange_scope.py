from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from scripts.build_bybit_venue_daily_features import build
from stockagent.config import load_config
from stockagent.data.crypto_exchange_scope import validate_crypto_exchange_scope


@pytest.mark.parametrize("name,venue", [
    ("bybit_perpetual_daily_0005_historical_pit_v1.yaml", "bybit"),
    ("okx_1m_venue_only_v1.yaml", "okx"),
    ("binance_1m_venue_only_v1.yaml", "binance"),
])
def test_single_venue_configs_are_scoped(name: str, venue: str) -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/markets" / name)
    assert config.data.crypto_exchange_scope == venue
    validate_crypto_exchange_scope(config.data, repo_root=root, check_schema=True)


def test_scope_rejects_cross_venue_path_and_feature(tmp_path: Path) -> None:
    source = tmp_path / "data_okx/1m"
    source.mkdir(parents=True)
    data = {
        "crypto_exchange_scope": "okx", "parquet_root": str(source),
        "use_external_features": False, "external_feature_path": "",
        "use_tw_public_features": False, "use_tw_public_rules": False,
        "feature_include": ["close_logret_1d", "crypto_binance_funding_rate"],
        "feature_availability_indicators": [],
    }
    with pytest.raises(ValueError, match="out-of-scope"):
        validate_crypto_exchange_scope(data, repo_root=tmp_path)
    data["feature_include"] = ["*"]
    with pytest.raises(ValueError, match="out-of-scope"):
        validate_crypto_exchange_scope(data, repo_root=tmp_path)
    data["feature_include"] = ["close_logret_1d"]
    data["parquet_root"] = str(tmp_path / "data_binance/1m")
    with pytest.raises(ValueError, match="parquet_root"):
        validate_crypto_exchange_scope(data, repo_root=tmp_path)


def test_scope_rejects_hidden_foreign_external_schema(tmp_path: Path) -> None:
    source = tmp_path / "data_bybit/perpetual_daily"
    source.mkdir(parents=True)
    external = tmp_path / "data_bybit/public_features/bybit_venue_daily.parquet"
    external.parent.mkdir(parents=True)
    pl.DataFrame({
        "date": ["2024-01-01"], "symbol": ["BTCUSDT"],
        "crypto_bybit_funding": [0.01], "crypto_okx_hidden": [0.02],
    }).write_parquet(external)
    data = {
        "crypto_exchange_scope": "bybit", "parquet_root": str(source),
        "use_external_features": True, "external_feature_path": str(external),
        "use_tw_public_features": False, "use_tw_public_rules": False,
        "feature_include": ["close_logret_1d", "crypto_bybit_funding"],
        "feature_availability_indicators": ["crypto_bybit_funding"],
    }
    with pytest.raises(ValueError, match="out-of-scope columns"):
        validate_crypto_exchange_scope(data, repo_root=tmp_path, check_schema=True)


def test_bybit_venue_builder_outputs_only_own_funding(tmp_path: Path) -> None:
    daily = tmp_path / "data_bybit/perpetual_daily"
    daily.mkdir(parents=True)
    (daily / "materialize_summary.json").write_text(
        json.dumps({"contract_version": 6, "failed_symbols": []}), encoding="utf-8"
    )
    pl.DataFrame({
        "date": ["2024-01-01"],
        "funding_rate_sum_previous_session": [0.001],
        "funding_cashflow_coefficient_previous_session": [0.001],
        "funding_last_rate_previous_session": [0.001],
        "funding_age_hours_at_decision": [2.0],
        "funding_event_count_previous_session": [3],
        "crypto_okx_hidden": [10.0],
    }).write_parquet(daily / "BTCUSDT_features.parquet")
    output = tmp_path / "data_bybit/public_features/bybit_venue_daily.parquet"
    result = build(daily, output)
    assert result["exchange_scope"] == "bybit"
    assert result["output_rows"] == 1
    assert set(pl.read_parquet(output).columns) == {
        "date", "symbol", "crypto_bybit_funding_rate_sum_1d",
        "crypto_bybit_funding_realized_annualized",
        "crypto_bybit_funding_cashflow_coefficient_1d",
        "crypto_bybit_funding_last_rate", "crypto_bybit_funding_age_hours",
        "crypto_bybit_funding_event_count_1d", "crypto_bybit_funding_available",
    }
