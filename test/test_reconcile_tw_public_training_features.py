from __future__ import annotations

import json
from contextlib import nullcontext
from datetime import date, time
from pathlib import Path

import pytest

from scripts import reconcile_tw_public_training_features as reconcile
from scripts.refresh_tw_public_release_archives import _source_update_lock as archive_update_lock


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
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (False, "2026-09-16")
    monkeypatch.setattr(reconcile, "parquet_footer_stats", lambda _path: {"last": "2026-09-15"})
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (True, "2026-09-16")
    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", lambda *_args, **_kwargs: False)
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (True, "2026-09-16")
    monkeypatch.setattr(reconcile, "_incremental_base_is_compatible", lambda *_args, **_kwargs: True)
    output.with_suffix(".summary.json").write_text('{"requested_end_date":"2026-09-16"}')
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (False, "2026-09-16")


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
            "coverage_complete": True, "blocking_failed_count": 0,
            "end_date": "2026-09-17",
        },
    }))
    assert reconcile.needs_rebuild(input_dir, tmp_path, output) == (True, "2026-09-17")

    receipt.write_text(json.dumps({
        "status": "ok", "phase": "close_initial", "live_root": str(tmp_path / "wrong"),
        "selected_datasets": ["twse_daily_ohlcv", "tpex_daily_ohlcv"],
        "download_summary": {
            "coverage_complete": True, "blocking_failed_count": 0,
            "end_date": "2026-09-17",
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


def test_research_reconcile_refreshes_macro_before_wide_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(reconcile, "needs_rebuild", lambda *_args: (False, "2026-09-17"))
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
