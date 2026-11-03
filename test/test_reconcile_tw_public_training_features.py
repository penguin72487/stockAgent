from __future__ import annotations

import json
import subprocess
from contextlib import nullcontext
from datetime import date, time
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from scripts import reconcile_tw_public_training_features as reconcile
from scripts import rebuild_tw_public_data_layer as rebuild
from scripts.refresh_tw_public_release_archives import _source_update_lock as archive_update_lock
from stockagent.data.tw_public_features import build_tw_public_training_features


def test_reconcile_skips_only_receipt_compatible_current_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    (input_dir / "download_summary.json").write_text(json.dumps({
        "coverage_complete": True, "blocking_failed_count": 0,
        "end_date": "2026-09-16",
    }))
    output = tmp_path / "features.parquet"
    output.touch()
    output.with_suffix(".summary.json").write_text("{}")
    monkeypatch.setattr(reconcile, "_source_content_receipts", lambda _root: [])
    monkeypatch.setattr(reconcile, "_symbol_universe_receipt", lambda _root: {})
    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(reconcile, "parquet_footer_stats", lambda _path: {"last": "2026-09-16"})
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (False, "2026-09-16", False)
    monkeypatch.setattr(reconcile, "parquet_footer_stats", lambda _path: {"last": "2026-09-15"})
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (True, "2026-09-16", False)
    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", lambda *_args, **_kwargs: False)
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (True, "2026-09-16", False)
    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", lambda *_args, **_kwargs: True)
    output.with_suffix(".summary.json").write_text('{"requested_end_date":"2026-09-16"}')
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (False, "2026-09-16", False)


def test_reconcile_refuses_incomplete_close_receipt(tmp_path: Path) -> None:
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    (input_dir / "download_summary.json").write_text(json.dumps({
        "coverage_complete": False, "blocking_failed_count": 1,
        "end_date": "2026-09-16",
    }))
    with pytest.raises(RuntimeError, match="complete close-source coverage"):
        reconcile.needs_rebuild(input_dir, tmp_path, tmp_path / "features.parquet")


def test_reconcile_uses_accepted_close_and_never_regresses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import polars as pl

    input_dir = tmp_path / "data"
    input_dir.mkdir()
    (input_dir / "download_summary.json").write_text(json.dumps({
        "coverage_complete": True, "blocking_failed_count": 0,
        "end_date": "2026-09-16",
    }))
    for name in ("twse_daily_ohlcv", "tpex_daily_ohlcv"):
        pl.DataFrame({"date": [date(2026, 9, 17)]}).write_parquet(input_dir / f"{name}.parquet")
    output = tmp_path / "features.parquet"
    pl.DataFrame({"date": [date(2026, 9, 17)]}).write_parquet(output)
    output.with_suffix(".summary.json").write_text('{"requested_end_date":"2026-09-17"}')
    monkeypatch.setattr(reconcile, "_source_content_receipts", lambda _root: [])
    monkeypatch.setattr(reconcile, "_symbol_universe_receipt", lambda _root: {})
    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(reconcile, "REPO_ROOT", tmp_path)

    with pytest.raises(RuntimeError, match="would regress"):
        reconcile.needs_rebuild(input_dir, tmp_path, output)

    receipt = tmp_path / "artifacts/data_refresh/tw_public/publications/close_initial/latest.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({
        "status": "ok", "phase": "close_initial", "live_root": str(input_dir),
        "selected_datasets": ["twse_daily_ohlcv", "tpex_daily_ohlcv"],
        "download_summary": {
            "coverage_complete": True, "daily_close_ready": True,
            "blocking_failed_count": 0, "incomplete_count": 0,
            "end_date": "2026-09-17",
        },
    }))
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (True, "2026-09-17", True)

    receipt.write_text(json.dumps({
        "status": "ok", "phase": "close_initial", "live_root": str(tmp_path / "wrong"),
        "selected_datasets": ["twse_daily_ohlcv", "tpex_daily_ohlcv"],
        "download_summary": {
            "coverage_complete": True, "daily_close_ready": True,
            "blocking_failed_count": 0, "incomplete_count": 0,
            "end_date": "2026-09-17",
        },
    }))
    with pytest.raises(RuntimeError, match="would regress"):
        reconcile.needs_rebuild(input_dir, tmp_path, output)


def test_reconcile_keeps_accepted_close_when_only_publication_lag_remains(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    import polars as pl

    input_dir = tmp_path / "data"
    input_dir.mkdir()
    (input_dir / "download_summary.json").write_text(json.dumps({
        "coverage_complete": True, "blocking_failed_count": 0,
        "end_date": "2026-09-22",
    }))
    for name in ("twse_daily_ohlcv", "tpex_daily_ohlcv"):
        pl.DataFrame({"date": [date(2026, 9, 23)]}).write_parquet(
            input_dir / f"{name}.parquet"
        )
    output = tmp_path / "features.parquet"
    pl.DataFrame({"date": [date(2026, 9, 23)]}).write_parquet(output)
    output.with_suffix(".summary.json").write_text(json.dumps({
        "requested_end_date": "2026-09-23",
    }))
    receipt = tmp_path / "artifacts/data_refresh/tw_public/publications/close_final/latest.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({
        "status": "ok", "phase": "close_final", "live_root": str(input_dir),
        "selected_datasets": ["twse_daily_ohlcv", "tpex_daily_ohlcv"],
        "download_summary": {
            "coverage_complete": False, "daily_close_ready": True,
            "blocking_failed_count": 0, "incomplete_count": 0,
            "publication_lag_datasets": ["tpex_daily_valuation"],
            "end_date": "2026-09-23",
        },
    }))
    monkeypatch.setattr(reconcile, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(reconcile, "_source_content_receipts", lambda _root: [])
    monkeypatch.setattr(reconcile, "_symbol_universe_receipt", lambda _root: {})
    seen: list[bool] = []

    def compatible(*_args: object, **kwargs: object) -> bool:
        seen.append(bool(kwargs["allow_daily_publication_lag"]))
        return True

    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", compatible)
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (
        False, "2026-09-23", True
    )
    assert seen == [True]

    receipt.write_text(json.dumps({
        "status": "ok", "phase": "close_final", "live_root": str(input_dir),
        "selected_datasets": ["twse_daily_ohlcv", "tpex_daily_ohlcv"],
        "download_summary": {
            "daily_close_ready": False, "blocking_failed_count": 0,
            "incomplete_count": 0, "end_date": "2026-09-23",
        },
    }))
    with pytest.raises(RuntimeError, match="would regress"):
        reconcile.needs_rebuild(input_dir, tmp_path, output)

def test_timer_market_hours_catch_up_gate() -> None:
    assert not reconcile._inside_taiwan_market_hours(time(8, 59, 59))
    assert reconcile._inside_taiwan_market_hours(time(9, 0))
    assert reconcile._inside_taiwan_market_hours(time(13, 29, 59))
    assert not reconcile._inside_taiwan_market_hours(time(13, 30))


def test_reconcile_uses_canonical_archive_producer_lock(tmp_path: Path) -> None:
    root = tmp_path / "data_tw_public"
    root.mkdir()
    with archive_update_lock(root):
        with pytest.raises(RuntimeError, match="source update is active"):
            with reconcile._source_update_lock(root):
                pass
    with reconcile._source_update_lock(root):
        pass


@pytest.mark.parametrize("owner", ["reconcile", "daily_data_layer"])
@pytest.mark.parametrize("change", ["non_feature", "dated_and_non_feature", "unknown"])
def test_feature_owners_execute_proven_incremental_path_and_full_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: str, owner: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    symbols = tmp_path / "absent-symbols"
    output = tmp_path / "features.parquet"
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]

    def write_daily(closes: list[str]) -> None:
        pl.DataFrame({
            "date": days, "證券代號": ["2330"] * 3,
            "收盤價": closes, "最高價": closes, "最低價": closes,
            "成交股數": ["1000"] * 3, "成交金額": ["100000"] * 3,
            "成交筆數": ["10"] * 3,
        }).write_parquet(source / "twse_daily_ohlcv.parquet")

    write_daily(["100", "101", "102"])
    unrelated = source / "tpex_api_tpex50_constituents.parquet"
    unknown = source / "unknown_dependency.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(unrelated)
    pl.DataFrame({"value": [1]}).write_parquet(unknown)
    (source / "download_summary.json").write_text(json.dumps({
        "coverage_complete": True, "blocking_failed_count": 0,
        "end_date": "2024-01-04",
    }))
    common = dict(input_dir=source, symbols_root=symbols, end_date=date(2024, 1, 4),
                  allow_daily_publication_lag=owner == "daily_data_layer")
    build_tw_public_training_features(**common, output_path=output, incremental_auto=True)
    old_output = output.stat()
    pl.DataFrame({"value": [2]}).write_parquet(unrelated)
    if change == "dated_and_non_feature":
        write_daily(["100", "101", "108"])
    elif change == "unknown":
        pl.DataFrame({"value": [2]}).write_parquet(unknown)

    if owner == "reconcile":
        monkeypatch.setattr(reconcile.sys, "argv", [
            "reconcile_tw_public_training_features.py", "--input-dir", str(source),
            "--symbols-root", str(symbols), "--output-path", str(output),
        ])
        assert reconcile.main() == 0
    else:
        monkeypatch.setattr(rebuild, "_daily_baseline_ready", lambda **_kwargs: True)
        monkeypatch.setattr(rebuild, "_acquire_data_layer_lock", lambda _path: None)
        monkeypatch.setattr(rebuild, "_preflight", lambda **_kwargs: None)

        def run_feature(_runner, name, command, **_kwargs):
            if name == "build_public_features":
                subprocess.run(command, cwd=rebuild.REPO_ROOT, check=True)

        monkeypatch.setattr(rebuild.RebuildRunner, "run", run_feature)
        monkeypatch.setattr(rebuild.sys, "argv", [
            "rebuild_tw_public_data_layer.py", "--mode", "daily", "--dry-run",
            "--public-dir", str(source), "--stock-root", str(symbols),
            "--public-feature-path", str(output), "--stage-root", str(tmp_path / "stage"),
            "--end-date", "2024-01-04", "--skip-symbol-build",
            "--skip-corporate-actions", "--skip-short-rules",
        ])
        rebuild.main()
    summary = json.loads(output.with_suffix(".summary.json").read_text())
    if change == "non_feature":
        assert summary["build_mode"] == "unchanged_verified"
        current_output = output.stat()
        assert (current_output.st_ino, current_output.st_size,
                current_output.st_mtime_ns, current_output.st_ctime_ns) == (
            old_output.st_ino, old_output.st_size,
            old_output.st_mtime_ns, old_output.st_ctime_ns,
        )
    elif change == "dated_and_non_feature":
        assert summary["build_mode"] == "incremental_tail"
        assert summary["incremental_start_date"] == "2024-01-04"
    else:
        assert summary["build_mode"] == "full"

    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(pl.read_parquet(output), pl.read_parquet(reference))
    if owner == "reconcile":
        assert reconcile.needs_rebuild(source, symbols, output) == (False, "2024-01-04", False)


def test_research_reconcile_refreshes_macro_before_wide_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(reconcile, "needs_rebuild", lambda *_args: (False, "2026-09-17", False))
    monkeypatch.setattr(reconcile, "_source_update_lock", lambda _root: nullcontext())
    monkeypatch.setattr(
        reconcile.subprocess, "run",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(reconcile.sys, "argv", [
        "reconcile_tw_public_training_features.py",
        "--input-dir", str(tmp_path),
        "--research-output-path", str(tmp_path / "wide.parquet"),
        "--research-macro-events-path", str(tmp_path / "macro.parquet"),
    ])
    assert reconcile.main() == 0
    assert len(commands) == 2
    assert commands[0][1].endswith("build_tw_public_provisional_macro.py")
    assert commands[1][1].endswith("build_tw_public_research_features.py")


def test_research_reconcile_refreshes_all_features_after_taifex(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(reconcile, "needs_rebuild", lambda *_args: (False, "2026-09-17", False))
    monkeypatch.setattr(reconcile, "_source_update_lock", lambda _root: nullcontext())
    monkeypatch.setattr(
        reconcile.subprocess, "run",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(reconcile.sys, "argv", [
        "reconcile_tw_public_training_features.py",
        "--input-dir", str(tmp_path),
        "--research-output-path", str(tmp_path / "wide.parquet"),
        "--research-taifex-output-path", str(tmp_path / "taifex.parquet"),
    ])
    assert reconcile.main() == 0
    assert [Path(command[1]).name for command in commands] == [
        "build_tw_public_provisional_macro.py",
        "build_tw_public_research_features.py",
        "build_tw_public_research_taifex.py",
        "build_tw_public_research_all_features.py",
    ]
    assert commands[-1][-1] == str(tmp_path / "tw_public_research_all_2014_v3.parquet")
