from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import sys

import polars as pl
import pytest

DOWNLOADER = Path(__file__).resolve().parents[1] / "downloader"
if str(DOWNLOADER) not in sys.path:
    sys.path.insert(0, str(DOWNLOADER))

import download_bybit_funding_history as funding  # noqa: E402
from download_bybit_funding_history import (  # noqa: E402
    FUNDING_CONTRACT_VERSION,
    HOUR_MS,
    _download_symbol,
)
from scripts.report_crypto_training_features import _source_status  # noqa: E402


def test_historical_feature_status_distinguishes_sparse_and_prospective() -> None:
    assert _source_status(
        selected=True,
        contract="historical_completed_bar",
        finite_rows=1,
        last_date="2026-09-01",
        daily_last="2026-09-20",
    ) == "exchange_aux_tail_gap"
    assert _source_status(
        selected=False,
        contract="prospective_observed_vintage",
        finite_rows=5,
        last_date="2026-09-20",
        daily_last="2026-09-20",
    ) == "prospective_only"
    assert _source_status(
        selected=True,
        contract="prospective_first_observed",
        finite_rows=5,
        last_date="2026-09-20",
        daily_last="2026-09-20",
    ) == "prospective_selected"
    assert _source_status(
        selected=True,
        contract="historical_completed_bar",
        finite_rows=0,
        last_date=None,
        daily_last="2026-09-20",
    ) == "no_eligible_value"


def test_funding_incremental_reconciles_overlap_without_losing_old_rows(
    tmp_path: Path, monkeypatch,
) -> None:
    day_ms = 24 * HOUR_MS
    start_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    times = [start_ms + offset * day_ms for offset in (0, 1, 2)]
    old = pl.DataFrame(
        {
            "funding_time_utc": [funding._ms_to_date_string(value) for value in times],
            "funding_timestamp_ms": times,
            "funding_rate": [0.001, 0.002, 0.003],
            "symbol": ["BTCUSDT"] * 3,
            "category": ["linear"] * 3,
            "download_snapshot_utc": [funding._ms_to_date_string(start_ms + 2 * day_ms)] * 3,
            "funding_mark_price": [100.0, 101.0, 102.0],
            "funding_mark_price_source": ["bybit_hourly_mark_kline_open"] * 3,
            "bybit_funding_contract_version": [FUNDING_CONTRACT_VERSION] * 3,
            "funding_prefix_quarantined_events": [0] * 3,
            "funding_coverage_start_utc": [funding._ms_to_date_string(start_ms)] * 3,
        }
    )
    old.write_parquet(tmp_path / "BTCUSDT_funding.parquet")
    latest = times[-1] + day_ms

    class Client:
        calls = 0

        def get(self, endpoint, params):
            self.calls += 1
            return {
                "result": {
                    "list": [
                        {"fundingRateTimestamp": str(latest), "fundingRate": "0.004"},
                        {"fundingRateTimestamp": str(times[-1]), "fundingRate": "0.03"},
                        {"fundingRateTimestamp": str(times[-2]), "fundingRate": "0.002"},
                        {"fundingRateTimestamp": str(times[0]), "fundingRate": "0.001"},
                    ]
                }
            }

    monkeypatch.setattr(
        funding,
        "_funding_mark_prices",
        lambda client, record, event_times: {value: 100.0 for value in event_times},
    )
    client = Client()
    result = _download_symbol(
        client,
        SimpleNamespace(code="BTCUSDT", launch_time=None, bybit_symbol="BTCUSDT"),
        requested_start_ms=start_ms,
        snapshot_ms=latest + HOUR_MS,
        output_dir=tmp_path,
        refresh=False,
    )
    output = pl.read_parquet(tmp_path / "BTCUSDT_funding.parquet")
    assert client.calls == 1
    assert result.rows == 4
    assert output["funding_timestamp_ms"].to_list() == [*times, latest]
    assert output.filter(pl.col("funding_timestamp_ms") == times[-1])["funding_rate"][0] == 0.03
    assert output["funding_coverage_start_utc"].min() == funding._ms_to_date_string(start_ms)


def test_funding_incremental_rejects_non_overlapping_history(
    tmp_path: Path, monkeypatch,
) -> None:
    start_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    previous_last = start_ms + 10 * 24 * HOUR_MS
    old = pl.DataFrame(
        {
            "funding_timestamp_ms": [previous_last],
            "funding_time_utc": [funding._ms_to_date_string(previous_last)],
            "funding_rate": [0.001],
            "funding_mark_price": [100.0],
            "funding_mark_price_source": ["bybit_hourly_mark_kline_open"],
            "funding_prefix_quarantined_events": [0],
            "funding_coverage_start_utc": [funding._ms_to_date_string(start_ms)],
            "download_snapshot_utc": [funding._ms_to_date_string(previous_last)],
            "bybit_funding_contract_version": [FUNDING_CONTRACT_VERSION],
            "symbol": ["BTCUSDT"],
            "category": ["linear"],
        }
    )
    path = tmp_path / "BTCUSDT_funding.parquet"
    old.write_parquet(path)

    class Client:
        def get(self, endpoint, params):
            return {"result": {"list": []}}

    with pytest.raises(RuntimeError, match="did not overlap"):
        _download_symbol(
            Client(),
            SimpleNamespace(code="BTCUSDT", launch_time=None, bybit_symbol="BTCUSDT"),
            requested_start_ms=start_ms,
            snapshot_ms=previous_last + 2 * 24 * HOUR_MS,
            output_dir=tmp_path,
            refresh=False,
        )
    assert pl.read_parquet(path).equals(old)
