from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path

import polars as pl

from downloader.common import load_env_file, provider_rate_limit
from downloader.download_crypto_keyed_context import (
    _coingecko_catalog_frame,
    _coingecko_market_frame,
    _dataset_due,
)
from stockagent.live.data_monitor_dashboard import build_data_monitor_public_status


ROOT = Path(__file__).resolve().parents[1]


def test_crypto_registry_has_unique_first_principles_fact_contracts() -> None:
    registry = json.loads(
        (ROOT / "configs/crypto_data_acquisition.json").read_text(encoding="utf-8")
    )
    datasets = registry["datasets"]
    ids = [item["id"] for item in datasets]
    scope = registry["active_scope"]

    assert len(datasets) >= 30
    assert scope["canonical_intraday_market_granularity"] == "1m"
    assert scope["active_exchange_venues"] == ["Binance", "OKX", "Bybit"]
    assert scope["download_daily_candles"] is False
    assert scope["preserve_existing_event_data"] is True
    assert len(ids) == len(set(ids))
    assert {item["priority"] for item in datasets} == {"P0", "P1", "P2"}
    assert sum(item["priority"] == "P0" for item in datasets) >= 15
    for item in datasets:
        assert 0 <= item["score"] <= 10
        assert item["canonical_owners"]
        assert item["dedup_key"]
        assert item["native_granularity"]
        assert item["mechanism"]
    assert registry["identity_contract"]["derived_rule"].startswith(
        "Daily bars, returns"
    )
    rejected = {item["id"]: item for item in registry["rejected_or_fallback_only"]}
    assert rejected["yahoo_crypto_1m_bulk"]["action"] == (
        "disabled_by_default_manual_fallback_only"
    )
    assert rejected["aggregator_technical_indicators"]["action"] == "derive_locally"
    by_id = {item["id"]: item for item in datasets}
    for fact_id in scope["deferred_fact_ids"]:
        assert by_id[fact_id]["implementation"] == "deferred_by_user_1m_only"
        assert by_id[fact_id]["acquisition_enabled"] is False
    venue_fact_ids = {
        fact_id
        for fact_id in ids
        if fact_id.startswith("venue_") or fact_id == "options_chain_greeks_iv"
    }
    inactive_venue_tokens = {
        "coinbase",
        "kraken",
        "bitfinex",
        "hyperliquid",
        "deribit",
    }
    for fact_id in venue_fact_ids:
        owners = " ".join(by_id[fact_id]["canonical_owners"]).lower()
        assert not any(token in owners for token in inactive_venue_tokens)
    assert by_id["venue_spot_ohlcv_1m"]["implementation"].startswith(
        "implemented_partial_binance_archive"
    )
    assert by_id["aggregate_asset_identity"]["implementation"].startswith(
        "implemented_coingecko"
    )
    assert by_id["dex_derivatives_options_volume"]["implementation"].startswith(
        "implemented_dex_options"
    )
    assert by_id["protocol_fees_revenue"]["implementation"].startswith(
        "implemented_fees"
    )


def test_crypto_source_allocation_only_assigns_registered_facts() -> None:
    facts = json.loads(
        (ROOT / "configs/crypto_data_acquisition.json").read_text(encoding="utf-8")
    )
    allocation = json.loads(
        (ROOT / "configs/crypto_source_allocation.json").read_text(encoding="utf-8")
    )
    fact_ids = {item["id"] for item in facts["datasets"]}
    provider_ids = [item["id"] for item in allocation["providers"]]
    deferred = set(allocation["execution_scope"]["deferred_fact_ids"])

    assert len(provider_ids) == len(set(provider_ids))
    assert allocation["execution_scope"]["canonical_intraday_market_granularity"] == "1m"
    assert deferred == set(facts["active_scope"]["deferred_fact_ids"])
    assert set(allocation["provider_parallel_groups"]) == set(provider_ids)
    for provider in allocation["providers"]:
        assert set(provider["assigned_facts"]) <= fact_ids
        assert provider["limit_profiles"]
        assert provider["parallel_group"] == provider["id"]
    coingecko = next(item for item in allocation["providers"] if item["id"] == "coingecko")
    cmc = next(item for item in allocation["providers"] if item["id"] == "coinmarketcap")
    assert coingecko["role"] == "configured_aggregate_primary"
    assert cmc["role"] == "configured_fallback_and_validation"


def test_keyed_crypto_provider_limits_match_free_plan_buckets() -> None:
    expected = {
        "coingecko_demo": 100 / 60,
        "coinmarketcap_basic": 50 / 60,
        "coinglass_keyed": 30 / 60,
        "etherscan_free": 3.0,
        "dune_free_low": 15 / 60,
        "dune_free_high": 40 / 60,
        "blockscout_ethereum_public": 10 / 60,
        "binance_public_archive": 10.0,
        "binance_public_listing": 10.0,
    }
    for name, requests_per_second in expected.items():
        assert provider_rate_limit(name).requests_per_second == requests_per_second


def test_coingecko_normalization_uses_provider_id_not_ambiguous_symbol() -> None:
    catalog = _coingecko_catalog_frame(
        [
            {
                "id": "asset-one",
                "symbol": "dup",
                "name": "One",
                "platforms": {"ethereum": "0x1"},
            },
            {
                "id": "asset-two",
                "symbol": "dup",
                "name": "Two",
                "platforms": {"base": "0x2"},
            },
        ]
    )
    markets = _coingecko_market_frame(
        [
            {
                "id": "asset-one",
                "symbol": "dup",
                "name": "One",
                "market_cap_rank": 1,
                "current_price": 10,
                "market_cap": 100,
                "last_updated": "2026-08-16T01:02:03Z",
            },
            {
                "id": "asset-two",
                "symbol": "dup",
                "name": "Two",
                "market_cap_rank": 2,
                "current_price": 20,
                "market_cap": 200,
                "last_updated": "2026-08-16T01:02:03Z",
            },
        ]
    )

    assert catalog.height == 2
    assert markets.height == 2
    assert set(catalog["asset_id"].to_list()) == {"asset-one", "asset-two"}
    assert catalog.filter(pl.col("asset_id") == "asset-one")["platforms_json"].item() == (
        '{"ethereum": "0x1"}'
    )


def test_cadence_receipt_prevents_duplicate_requests(tmp_path: Path) -> None:
    state = tmp_path / "state/datasets"
    state.mkdir(parents=True)
    (state / "fixture.json").write_text(
        json.dumps(
            {
                "last_success_at_utc": (
                    datetime.now(UTC) + timedelta(hours=1)
                ).isoformat()
            }
        ),
        encoding="utf-8",
    )

    # The helper uses wall clock; a future receipt is conservatively current.
    assert _dataset_due(tmp_path, "fixture", 3600, force=False) is False
    assert _dataset_due(tmp_path, "fixture", 3600, force=True) is True


def test_keyed_downloader_loads_only_allowlisted_env_without_override(
    tmp_path: Path, monkeypatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "COINGECKO_DEMO_API_KEY=from-file\n"
        "export DUNE_API_KEY='dune-file'\n"
        "UNRELATED_SECRET=must-not-load\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("COINGECKO_DEMO_API_KEY", "from-process")
    monkeypatch.delenv("DUNE_API_KEY", raising=False)
    monkeypatch.delenv("UNRELATED_SECRET", raising=False)

    loaded = load_env_file(
        env_file,
        allowed_names={"COINGECKO_DEMO_API_KEY", "DUNE_API_KEY"},
    )

    assert loaded == {"DUNE_API_KEY"}
    assert os.environ["COINGECKO_DEMO_API_KEY"] == "from-process"
    assert os.environ["DUNE_API_KEY"] == "dune-file"
    assert "UNRELATED_SECRET" not in os.environ


def test_crypto_fact_registry_is_exposed_on_read_only_dashboard(tmp_path: Path) -> None:
    config = tmp_path / "configs"
    (config / "data_sync").mkdir(parents=True)
    (config / "data_sync/packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "crypto-reference",
                        "source": "data_crypto_reference",
                        "role": "analytics",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (config / "crypto_data_acquisition.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "id": "aggregate_asset_identity",
                        "title": "asset identity",
                        "mechanism": "stable join identity",
                        "native_granularity": "daily_snapshot",
                        "score": 9,
                        "priority": "P0",
                        "canonical_owners": ["CoinGecko Demo"],
                        "fallbacks": ["CoinMarketCap"],
                        "dedup_key": ["asset_id", "observed_at"],
                        "implementation": "implemented_coingecko_full_catalog",
                        "credential_id": "coingecko",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_crypto_reference"
    data.mkdir()
    (data / "source_status.json").write_text(
        json.dumps(
            {
                "generated_at_utc": "2026-08-16T05:00:00+00:00",
                "secret_values_included": False,
                "providers": [
                    {
                        "credential_id": "coingecko",
                        "credential_state": "configured",
                        "operational_state": "operational",
                        "message": "ok",
                    }
                ],
                "datasets": [
                    {
                        "dataset": "coingecko_asset_catalog",
                        "status": "updated",
                        "rows": 100,
                        "observed_at_utc": "2026-08-16T05:00:00+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, 5, 1, tzinfo=UTC),
        refresh_services={},
        shioaji_status={"pipelines": []},
        openbb_status={},
    )

    assert payload["summary"]["crypto_fact_families"] == 1
    fact = next(row for row in payload["sources"] if row["scope"] == "crypto_fact_family")
    assert fact["operation_state"] == "complete"
    assert fact["rows"] == 100
    assert fact["dedup_key"] == ["asset_id", "observed_at"]
    assert "secret" not in json.dumps(payload).lower()
