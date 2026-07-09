from __future__ import annotations

from downloader.status import (
    command_summary_paths,
    download_counts_failure_reason,
    first_download_failure,
)


def test_download_counts_failure_reason_rejects_all_failed() -> None:
    assert download_counts_failure_reason({"failed": 2307}) == "2307 failed, 0 productive"


def test_download_counts_failure_reason_accepts_productive_download() -> None:
    assert download_counts_failure_reason({"repaired": 2000, "failed": 10}) is None


def test_download_counts_failure_reason_accepts_unchanged_download() -> None:
    assert download_counts_failure_reason({"unchanged": 2000, "failed": 10}) is None


def test_command_summary_paths_prefers_asset_output_summary(tmp_path) -> None:
    output_root = tmp_path / "data_yahoo"
    command = [
        "python",
        "downloader/download_yahoo_ohlcv.py",
        "--mode",
        "daily-update",
        "--asset",
        "tw_stocks",
        "--output-root",
        str(output_root),
    ]

    assert command_summary_paths(command) == [
        output_root / "tw_stocks" / "download_summary.json",
        output_root / "daily_update_summary.json",
    ]


def test_command_summary_paths_maps_cboe_us_command(tmp_path) -> None:
    output_root = tmp_path / "data_yahoo"
    command = [
        "python",
        "downloader/download_cboe_us_ohlcv.py",
        "--mode",
        "daily-update",
        "--output-root",
        str(output_root),
    ]

    assert command_summary_paths(command) == [
        output_root / "us_stocks" / "download_summary.json",
        output_root / "daily_update_summary.json",
    ]


def test_first_download_failure_uses_asset_output_summary_not_other_root_summary(tmp_path) -> None:
    output_root = tmp_path / "data_yahoo"
    tw_dir = output_root / "tw_stocks"
    tw_dir.mkdir(parents=True)
    (tw_dir / "download_summary.json").write_text(
        '{"asset_class":"tw_stocks","symbol_count":2307,"row_count":0,"status_counts":{"failed":2307}}',
        encoding="utf-8",
    )
    (output_root / "daily_update_summary.json").write_text(
        '{"forex":{"schema_mismatch":38,"failed":38}}',
        encoding="utf-8",
    )
    command = [
        "python",
        "downloader/download_yahoo_ohlcv.py",
        "--mode",
        "daily-update",
        "--asset",
        "tw_stocks",
        "--output-root",
        str(output_root),
    ]

    failure = first_download_failure(command=command, market="tw", market_type="tw")

    assert failure is not None
    path, reason = failure
    assert path == tw_dir / "download_summary.json"
    assert reason == "2307 failed, 0 productive"
