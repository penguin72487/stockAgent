from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from downloader import binance_historical_features as binance
from downloader import download_okx_perp_daily as okx_downloader
from downloader import okx_historical_features as okx
from downloader.common import PersistentProgress
from downloader.feature_stage_timing import (
    feature_run_summary_path,
    stage_latency_summary,
)
from downloader.ohlcv_hot_tail import hot_tail_path


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"test")


def test_feature_latency_summary_rejects_invalid_worker_samples() -> None:
    results = [
        SimpleNamespace(stage_elapsed_seconds_json='{"fetch": 2, "write": 0}'),
        SimpleNamespace(stage_elapsed_seconds_json='{"fetch": -1, "write": "nan"}'),
        SimpleNamespace(stage_elapsed_seconds_json='{"fetch": 3, "write": true}'),
        SimpleNamespace(stage_elapsed_seconds_json="not-json"),
    ]
    assert stage_latency_summary(results) == {
        "fetch": {
            "samples": 2,
            "p50_seconds": 2.0,
            "p95_seconds": 3.0,
            "aggregate_worker_seconds": 5.0,
        },
        "write": {
            "samples": 1,
            "p50_seconds": 0.0,
            "p95_seconds": 0.0,
            "aggregate_worker_seconds": 0.0,
        },
    }


def test_feature_summary_path_survives_candle_only_refresh(tmp_path: Path) -> None:
    feature_path = feature_run_summary_path(tmp_path, features_enabled=True)
    candle_path = feature_run_summary_path(tmp_path, features_enabled=False)
    assert feature_path != candle_path
    assert feature_path.name == "download_summary.historical_features.json"
    assert candle_path.name == "download_summary.candles_only.json"


def test_okx_limiter_activity_aggregates_instrument_scoped_grants() -> None:
    class Limiter:
        interval_seconds = 0.2

        def __init__(self, grants: int) -> None:
            self.grants = grants

        def grant_activity(self) -> dict[str, int]:
            return {
                "grants_total": self.grants,
                "grants_last_60s": self.grants,
                "pending_claim_observations": 0,
            }

    client = okx_downloader.OkxClient(0.1, 0, 0.1)
    client._limiters = {
        "okx_funding_rate_history:BTC": Limiter(2),
        "okx_funding_rate_history:ETH": Limiter(3),
    }
    activity = client.limiter_activity()
    assert activity["okx_funding_rate_history"]["grants_total"] == 5
    assert activity["okx_funding_rate_history"]["interval_seconds"] == 0.2
    assert activity["okx_funding_rate_history"]["limiter_count"] == 2
    assert activity["okx_funding_rate_history"]["sharing_scope"] == "per_instrument"


def test_binance_tail_feature_run_targets_hot_tail_without_rewriting_base(
    tmp_path: Path, monkeypatch
) -> None:
    base = tmp_path / "BTCUSDT_features.parquet"
    tail = hot_tail_path(base)
    _touch(base)
    _touch(tail)
    called: list[Path] = []

    def fake_enrich(_client, record, output_path, **_kwargs):
        called.append(output_path)
        return binance.HistoricalFeatureResult(
            record.code, record.binance_symbol, "unchanged", 1,
            str(output_path), False, "{}", "{}", "{}",
        )

    monkeypatch.setattr(binance, "enrich_symbol_historical_features", fake_enrich)
    record = SimpleNamespace(code="BTCUSDT", binance_symbol="BTCUSDT")
    results = binance.run_historical_feature_downloads(
        object(), [record], tmp_path, start_ms=0, end_ms=1,
        workers=1, tail_only=True,
    )
    assert called == [tail]
    assert results[0].output_path == str(tail)
    assert base.read_bytes() == b"test"


def test_okx_tail_feature_run_targets_hot_tail_without_rewriting_base(
    tmp_path: Path, monkeypatch
) -> None:
    base = tmp_path / "BTCUSDTSWAP_features.parquet"
    tail = hot_tail_path(base)
    _touch(base)
    _touch(tail)
    called: list[Path] = []

    def fake_enrich(_client, record, output_path, **_kwargs):
        called.append(output_path)
        return okx.HistoricalFeatureResult(
            record.code, record.okx_symbol, "unchanged", 1,
            str(output_path), False, "{}", "{}", "{}",
        )

    monkeypatch.setattr(okx, "enrich_symbol_historical_features", fake_enrich)
    record = SimpleNamespace(code="BTCUSDTSWAP", okx_symbol="BTC-USDT-SWAP")
    results = okx.run_historical_feature_downloads(
        object(), [record], tmp_path, start_ms=0, end_ms=1,
        workers=1, include_funding_archive=False, tail_only=True,
    )
    assert called == [tail]
    assert results[0].output_path == str(tail)
    assert base.read_bytes() == b"test"


def test_progress_revises_feature_denominator_after_candle_stage(tmp_path: Path) -> None:
    progress = PersistentProgress(
        tmp_path / "progress.json", label="test", total=12, unit="stage",
        basis="completed stages",
    )
    progress.update("candles", "updated", count=2)
    progress.revise_total(5, phase="historical-features")
    payload = json.loads((tmp_path / "progress.json").read_text())
    assert payload["current"] == 2
    assert payload["total"] == 5
    assert payload["ratio"] == 0.4
