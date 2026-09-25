from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.okx_historical_features import HistoricalFeatureResult


def _record(record_type, **values):
    return record_type(
        **{field.name: values.get(field.name) for field in fields(record_type)}
    )


def _run_serially(items, worker, **_kwargs):
    return [worker(item) for item in items]


@pytest.mark.parametrize("feature_status, expected_state", [
    ("partial", "failed"), ("updated", "complete"),
])
def test_okx_feature_result_matches_exit_and_archived_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    feature_status: str, expected_state: str,
) -> None:
    output = tmp_path / "latest"
    archive = tmp_path / "run-1" / "okx_source"
    symbol = _record(
        okx.SymbolRecord,
        code="BTC",
        okx_symbol="BTC-USDT-SWAP",
        market="crypto_okx_perp",
        inst_family="BTC-USDT",
    )
    args = SimpleNamespace(
        output_dir=str(output), mode="incremental", start_date="2026-09-01",
        end_date="2026-09-01", workers=1, feature_workers=1,
        refresh=False, tail_only=False, limit=None, request_interval=None,
        max_retries=0, retry_base=0.1, skip_historical_features=False,
        skip_funding_archive=True, archive_report_dir=str(archive),
    )

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def limiter_activity(self):
            return {}

    def source_result(_client, record, output_dir, *_args, **_kwargs):
        (output_dir / f"{record.code}_features.parquet").touch()
        return okx.DownloadResult(
            asset_class="crypto_okx_perp", code=record.code,
            okx_symbol=record.okx_symbol, market=record.market,
            status="updated", rows=1, output_path="synthetic",
        )

    def feature_results(_client, _symbols, _output, *, stage_progress_callback, **_kwargs):
        for stage in okx.FEATURE_STAGE_IDS:
            stage_progress_callback("BTC", stage, feature_status)
        return [HistoricalFeatureResult(
            code="BTC", okx_symbol="BTC-USDT-SWAP", status=feature_status,
            rows=1, output_path="synthetic", changed=False,
            stage_status_json="{}", coverage_json="{}", errors_json="{}",
        )]

    monkeypatch.setattr(okx, "parse_args", lambda: args)
    monkeypatch.setattr(okx, "OkxClient", FakeClient)
    monkeypatch.setattr(okx, "_fetch_swap_symbols", lambda *_args, **_kwargs: [symbol])
    monkeypatch.setattr(okx, "_download_symbol_1m", source_result)
    monkeypatch.setattr(okx, "run_parallel_tasks", _run_serially)
    monkeypatch.setattr(okx, "run_historical_feature_downloads", feature_results)

    if expected_state == "failed":
        with pytest.raises(RuntimeError, match="1 historical features"):
            okx.main()
    else:
        okx.main()

    assert json.loads((output / "progress.json").read_text())["state"] == expected_state
    assert json.loads((archive / "download_summary.json").read_text())[
        "historical_feature_status_counts"
    ] == {feature_status: 1}
    assert "BTC-USDT-SWAP" in (archive / "download_report.csv").read_text()
    (output / "download_report.csv").write_text("replaced\n")
    assert "BTC-USDT-SWAP" in (archive / "download_report.csv").read_text()


@pytest.mark.parametrize("status, expected_state", [
    ("failed", "failed"), ("repair_required", "failed"), ("updated", "complete"),
])
def test_bybit_source_result_matches_exit_and_archived_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str,
    expected_state: str,
) -> None:
    output = tmp_path / "latest"
    archive = tmp_path / "run-1" / "bybit_source"
    symbol = _record(
        bybit.SymbolRecord,
        code="BTC", bybit_symbol="BTCUSDT", category="linear",
        market="crypto_bybit_perp",
    )
    args = SimpleNamespace(
        output_dir=str(output), mode="incremental", start_date="2026-09-01",
        end_date="2026-09-01", workers=1, refresh=False, tail_only=False,
        categories=["linear"], symbols=None, limit=None, request_interval=None,
        max_retries=0, retry_base=0.1, archive_report_dir=str(archive),
    )

    class FakeClient:
        request_interval = 0.0

        def __init__(self, **_kwargs):
            pass

    def source_result(_client, record, *_args, **_kwargs):
        return bybit.DownloadResult(
            asset_class="crypto_bybit_perp", code=record.code,
            bybit_symbol=record.bybit_symbol, market=record.market,
            status=status, rows=0, output_path=None,
        )

    monkeypatch.setattr(bybit, "parse_args", lambda: args)
    monkeypatch.setattr(bybit, "BybitClient", FakeClient)
    monkeypatch.setattr(bybit, "_fetch_perp_symbols", lambda *_args, **_kwargs: [symbol])
    monkeypatch.setattr(bybit, "_download_symbol_1m", source_result)
    monkeypatch.setattr(bybit, "run_parallel_tasks", _run_serially)
    monkeypatch.setattr(bybit, "_stored_parquet_inventory", lambda *_args, **_kwargs: {})

    if expected_state == "failed":
        with pytest.raises(RuntimeError, match="1 source symbols"):
            bybit.main()
    else:
        bybit.main()

    assert json.loads((output / "progress.json").read_text())["state"] == expected_state
    assert json.loads((archive / "download_summary.json").read_text())[
        "status_counts"
    ] == {status: 1}
    assert "BTCUSDT" in (archive / "download_report.csv").read_text()
