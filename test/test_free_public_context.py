from __future__ import annotations

from pathlib import Path

import polars as pl

from downloader import download_free_public_context as public


def _spec(adapter: str, dataset: str = "test") -> public.DatasetSpec:
    return public.DatasetSpec(
        dataset=dataset,
        source="source",
        provider_profile="defillama_public",
        method="GET",
        url="https://example.invalid",
        body=None,
        adapter=adapter,
    )


def test_targeted_refresh_preserves_unselected_manifest_receipts() -> None:
    previous = {
        "results": [
            {"dataset": "defillama_chains", "status": "updated"},
            {"dataset": "blockscout_ethereum_gas", "status": "updated"},
            {"dataset": "not_registered", "status": "updated"},
        ]
    }
    current = [
        {"dataset": "blockscout_ethereum_gas", "status": "failed"},
    ]

    merged = public._merge_manifest_results(previous, current)

    assert [item["dataset"] for item in merged] == [
        "defillama_chains",
        "blockscout_ethereum_gas",
    ]
    assert merged[-1]["status"] == "failed"


def test_snapshot_available_at_is_local_observation_not_backfilled_event() -> None:
    observed = "2026-08-16T05:00:00+00:00"
    row = public._observation(
        _spec("defillama_chains"),
        observed,
        "a" * 64,
        entity="Ethereum",
        metric="tvl_usd",
        value=100.0,
        event_ts="2020-01-01T00:00:00+00:00",
    )

    assert row is not None
    assert row["event_ts_utc"] == "2020-01-01T00:00:00+00:00"
    assert row["observed_at_utc"] == observed
    assert row["available_at_utc"] == observed


def test_defillama_stablecoin_adapter_keeps_current_and_lagged_supply() -> None:
    rows = public._adapt_defillama_stablecoins(
        _spec("defillama_stablecoins", "defillama_stablecoins"),
        {
            "peggedAssets": [
                {
                    "symbol": "USDT",
                    "pegType": "peggedUSD",
                    "circulating": {"peggedUSD": 100.0},
                    "circulatingPrevDay": {"peggedUSD": 99.0},
                    "circulatingPrevWeek": {"peggedUSD": 95.0},
                    "circulatingPrevMonth": {"peggedUSD": 90.0},
                    "price": 1.0,
                    "pegMechanism": "fiat-backed",
                }
            ]
        },
        "2026-08-16T05:00:00+00:00",
        "b" * 64,
    )
    values = {row["metric"]: row for row in rows}

    assert values["circulating"]["value_float"] == 100.0
    assert values["circulating_prev_day"]["value_float"] == 99.0
    assert values["peg_mechanism"]["value_text"] == "fiat-backed"


def test_defillama_overview_separates_history_from_protocol_snapshot() -> None:
    observed = "2026-08-16T05:00:00+00:00"
    rows = public._adapt_defillama_dex_volume(
        _spec("defillama_dex_volume", "defillama_dex_volume"),
        {
            "totalDataChart": [[1_692_230_400, 123.0]],
            "protocols": [
                {
                    "id": "1",
                    "slug": "uniswap",
                    "displayName": "Uniswap",
                    "category": "Dexs",
                    "chains": ["Ethereum", "Base"],
                    "total24h": 12.0,
                    "total7d": 70.0,
                    "change_1d": 3.0,
                }
            ],
        },
        observed,
        "9" * 64,
    )
    values = {(row["entity"], row["metric"]): row for row in rows}

    history = values[("all", "daily_dex_volume_usd")]
    assert history["event_ts_utc"].startswith("2023-")
    assert history["available_at_utc"] == observed
    assert values[("uniswap", "total_24h_usd")]["value_float"] == 12.0
    assert values[("uniswap", "chains_json")]["value_text"] == '["Ethereum","Base"]'


def test_hyperliquid_predicted_funding_is_available_before_future_settlement() -> None:
    observed = "2026-08-16T05:00:00+00:00"
    rows = public._adapt_hyperliquid_predicted_fundings(
        _spec("hyperliquid_predicted_fundings"),
        [
            [
                "BTC",
                [
                    [
                        "HlPerp",
                        {
                            "fundingRate": "0.0001",
                            "nextFundingTime": 1_786_867_200_000,
                            "fundingIntervalHours": 1,
                        },
                    ]
                ],
            ]
        ],
        observed,
        "c" * 64,
    )
    values = {row["metric"]: row for row in rows}

    assert values["predicted_funding_rate"]["available_at_utc"] == observed
    assert values["next_funding_time_utc"]["value_text"].startswith("2026-")


def test_append_observations_is_idempotent_for_same_observation_vintage(
    tmp_path: Path,
) -> None:
    path = tmp_path / "observations.parquet"
    row = public._observation(
        _spec("defillama_chains"),
        "2026-08-16T05:00:00+00:00",
        "d" * 64,
        entity="Ethereum",
        metric="tvl_usd",
        value=100.0,
    )
    assert row is not None

    first_added, first_total = public._append_observations(path, [row])
    second_added, second_total = public._append_observations(path, [row])

    assert (first_added, first_total) == (1, 1)
    assert (second_added, second_total) == (0, 1)
    stored = pl.read_parquet(path)
    assert stored.columns == list(public.OBSERVATION_COLUMNS)


def test_append_observations_keeps_distinct_events_in_one_retrieval(
    tmp_path: Path,
) -> None:
    rows = [
        public._observation(
            _spec("alternative_me_fear_greed"),
            "2026-08-16T05:00:00+00:00",
            "1" * 64,
            entity="BTC",
            metric="fear_greed_index",
            value=value,
            event_ts=event_ts,
        )
        for value, event_ts in (
            (20, "2026-08-14T00:00:00+00:00"),
            (30, "2026-08-15T00:00:00+00:00"),
        )
    ]
    assert all(row is not None for row in rows)

    added, total = public._append_observations(tmp_path / "observations.parquet", rows)
    assert (added, total) == (2, 2)


def test_bitfinex_adapter_separates_trading_and_funding_layouts() -> None:
    rows = public._adapt_bitfinex_tickers(
        _spec("bitfinex_tickers"),
        [
            ["tBTCUSD", 10, 2, 11, 3, 1, 0.1, 10.5, 100, 12, 9, 1_786_867_200_000],
            [
                "fUSD",
                0.0003,
                0.0002,
                30,
                1000,
                0.0004,
                2,
                2000,
                0.00001,
                0.1,
                0.0003,
                3000,
                0.0005,
                0.0001,
                None,
                None,
                4000,
                1_786_867_200_000,
            ],
        ],
        "2026-08-16T05:00:00+00:00",
        "e" * 64,
    )
    values = {(row["entity"], row["metric"]): row for row in rows}

    assert values[("tBTCUSD", "bid")]["value_float"] == 10
    assert ("tBTCUSD", "flash_return_rate") not in values
    assert values[("fUSD", "flash_return_rate")]["value_float"] == 0.0003
    assert values[("fUSD", "available_amount")]["value_float"] == 4000
    assert ("fUSD", "bid") not in values


def test_historical_public_archive_remains_conservative_until_observed() -> None:
    observed = "2026-08-16T05:00:00+00:00"
    rows = public._adapt_alternative_me_fear_greed(
        _spec("alternative_me_fear_greed"),
        {
            "data": [
                {
                    "value": "40",
                    "value_classification": "Fear",
                    "timestamp": "1551157200",
                }
            ]
        },
        observed,
        "f" * 64,
    )
    values = {row["metric"]: row for row in rows}

    assert values["fear_greed_index"]["value_float"] == 40
    assert values["classification"]["value_text"] == "Fear"
    assert values["fear_greed_index"]["available_at_utc"] == observed


def test_mempool_hashrate_keeps_event_time_but_not_false_availability() -> None:
    observed = "2026-08-16T05:00:00+00:00"
    rows = public._adapt_mempool_hashrate(
        _spec("mempool_hashrate"),
        {
            "currentHashrate": 200,
            "currentDifficulty": 300,
            "hashrates": [{"timestamp": 1_692_230_400, "avgHashrate": 100}],
            "difficulty": [{"time": 1_692_724_599, "height": 804384, "difficulty": 90}],
        },
        observed,
        "0" * 64,
    )
    history = [
        row
        for row in rows
        if row["entity"] == "bitcoin:network" and row["metric"] == "hashrate"
    ][0]

    assert history["event_ts_utc"].startswith("2023-")
    assert history["available_at_utc"] == observed


def test_blockscout_adapters_keep_gas_snapshot_and_block_event_times() -> None:
    observed = "2026-08-16T11:01:00+00:00"
    gas_rows = public._adapt_blockscout_ethereum_gas(
        _spec("blockscout_ethereum_gas"),
        {
            "gas_price_updated_at": "2026-08-16T11:00:12Z",
            "gas_prices": {"slow": 0.07, "average": 0.17, "fast": 1.6},
            "network_utilization_percentage": 48.4,
        },
        observed,
        "7" * 64,
    )
    block_rows = public._adapt_blockscout_ethereum_latest_block(
        _spec("blockscout_ethereum_latest_block"),
        {
            "items": [
                {
                    "height": 25767180,
                    "timestamp": "2026-08-16T10:59:59Z",
                    "base_fee_per_gas": "43888577",
                    "gas_used": "4844517",
                    "gas_limit": "60000000",
                }
            ]
        },
        observed,
        "8" * 64,
    )
    gas = {row["metric"]: row for row in gas_rows}
    block = {row["metric"]: row for row in block_rows}

    assert gas["gas_price_average_gwei"]["value_float"] == 0.17
    assert gas["gas_price_average_gwei"]["available_at_utc"] == observed
    assert block["base_fee_per_gas_wei"]["value_float"] == 43888577
    assert block["base_fee_per_gas_gwei"]["value_float"] == 0.043888577
    assert block["block_height"]["event_ts_utc"] == "2026-08-16T10:59:59Z"


def test_every_public_dataset_has_an_adapter_and_rate_profile() -> None:
    assert len(public.DATASETS) == 26
    for spec in public.DATASETS:
        assert spec.adapter in public.ADAPTERS
        assert (
            public.provider_rate_limit(spec.provider_profile).requests_per_second <= 10
        )
