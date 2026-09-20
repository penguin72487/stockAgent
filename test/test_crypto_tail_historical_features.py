from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from downloader import binance_historical_features as binance
from downloader import okx_historical_features as okx
from downloader.common import PersistentProgress
from downloader.ohlcv_hot_tail import hot_tail_path


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"test")


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
