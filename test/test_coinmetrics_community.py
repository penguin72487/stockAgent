from __future__ import annotations

from pathlib import Path

import polars as pl

from downloader import download_coinmetrics_community as coinmetrics


def test_metric_columns_are_stable_and_collision_checked() -> None:
    assert coinmetrics._metric_column("PriceUSD") == "coinmetrics_PriceUSD"
    assert coinmetrics._metric_column("foo/bar") == "coinmetrics_foo_bar"


def test_fresh_frame_keeps_metric_status_and_builds_vintage_rows() -> None:
    fresh, vintages = coinmetrics._fresh_frame_for_asset(
        "btc",
        ("PriceUSD", "SplyCur"),
        [
            {
                "asset": "btc",
                "time": "2026-08-15T00:00:00Z",
                "PriceUSD": "63000",
                "PriceUSD-status": "reviewed",
                "PriceUSD-status-time": "2026-08-16T01:00:00Z",
                "SplyCur": "20000000",
            }
        ],
    )

    assert fresh["coinmetrics_PriceUSD"].item() == 63000.0
    assert fresh["coinmetrics_PriceUSD_status"].item() == "reviewed"
    assert {row["metric"] for row in vintages} == {"PriceUSD", "SplyCur"}


def test_merge_wide_replaces_overlap_without_erasing_unreturned_metrics() -> None:
    existing = pl.DataFrame(
        {
            "date": ["2026-08-15"],
            "coinmetrics_PriceUSD": [62000.0],
            "coinmetrics_SplyCur": [20_000_000.0],
        }
    )
    fresh = pl.DataFrame(
        {
            "date": ["2026-08-15"],
            "coinmetrics_PriceUSD": [63000.0],
        }
    )

    merged = coinmetrics._merge_wide(existing, fresh)

    assert merged["coinmetrics_PriceUSD"].item() == 63000.0
    assert merged["coinmetrics_SplyCur"].item() == 20_000_000.0


def test_vintage_append_retains_revisions_and_is_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "btc_vintages.parquet"
    rows = [
        {
            "asset": "btc",
            "date": "2026-08-15",
            "metric": "PriceUSD",
            "value": 63000.0,
            "status": "reviewed",
            "status_time_utc": "2026-08-16T01:00:00Z",
        }
    ]
    observed = "2026-08-16T05:00:00+00:00"

    first = coinmetrics._append_vintages(
        path, rows, observed_at=observed, raw_sha256="a" * 64
    )
    second = coinmetrics._append_vintages(
        path, rows, observed_at=observed, raw_sha256="a" * 64
    )
    third = coinmetrics._append_vintages(
        path,
        [{**rows[0], "value": 63100.0}],
        observed_at="2026-08-17T05:00:00+00:00",
        raw_sha256="b" * 64,
    )

    assert (first, second, third) == (1, 0, 1)
    stored = pl.read_parquet(path)
    assert stored.height == 2
    assert stored["available_at_utc"].to_list() == [
        observed,
        "2026-08-17T05:00:00+00:00",
    ]


def test_batching_groups_same_metric_signature_across_listing_dates(
    tmp_path: Path,
) -> None:
    specs = [
        coinmetrics.AssetSpec(
            asset=f"asset{index}",
            metrics=("PriceUSD",),
            catalog_start=f"202{index}-01-01",
            catalog_end="2026-08-16",
            query_start=f"202{index}-01-01",
        )
        for index in range(3)
    ]

    batches = coinmetrics._build_batches(
        specs,
        tmp_path,
        refresh=False,
        batch_assets=50,
    )

    assert len(batches) == 1
    assert {item.query_start for item in batches[0]} == {
        "2020-01-01",
        "2021-01-01",
        "2022-01-01",
    }
