from __future__ import annotations

from datetime import date
import builtins
import json
from pathlib import Path
import sys

import pytest
import polars as pl

from scripts.recheck_tw_day_trade_0901_local_price_gaps import recheck


@pytest.mark.parametrize("override", [False, True])
def test_cli_default_paths_do_not_load_the_replay_engine(tmp_path, monkeypatch, capsys, override):
    from scripts import recheck_tw_day_trade_0901_local_price_gaps as command
    from stockagent.live.minute_source_paths import DEFAULT_LOCAL_MINUTE_ROOTS

    monkeypatch.chdir(tmp_path)
    original_import = builtins.__import__
    def guarded(name, *args, **kwargs):
        if name == "scripts.rebuild_tw_day_trade_open_price_replay":
            raise AssertionError("reading default source paths loaded a replay engine")
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", guarded)
    seen = []
    def report(audit, roots, **kwargs):
        seen.append((audit, roots))
        return {"claim": "metadata_paths_only"}
    monkeypatch.setattr(command, "recheck", report)
    monkeypatch.setattr(sys, "argv", ["recheck", "--audit", "audit.json",
                                      *(["--minute-data-root", "custom"] if override else [])])
    assert command.main() is None
    roots = (tmp_path / "custom",) if override else tuple(tmp_path / root for root in DEFAULT_LOCAL_MINUTE_ROOTS)
    assert seen == [(Path("audit.json"), roots)]
    assert json.loads(capsys.readouterr().out)["claim"] == "metadata_paths_only"


def _audit(path: Path, symbols: list[str]) -> None:
    path.write_text(json.dumps({
        "schema_version": 1,
        "claim": "receipt_verified_minute_capacity_only_not_fills_or_replay_promotion",
        "missing_0901_price_symbols_by_date": {"2026-02-25": symbols},
    }), encoding="utf-8")


def test_recheck_separates_new_local_evidence_from_unresolved_gap(tmp_path: Path) -> None:
    source = tmp_path / "audit.json"
    _audit(source, ["0050", "0056"])

    def resolve(_roots: tuple[Path, ...], symbols: list[str], *, trading_date: date):
        assert symbols == ["0050", "0056"]
        assert trading_date == date(2026, 2, 25)
        return {"0050": {"execution_price_0901": 100.0}}, {"error_counts": {}}

    report = recheck(source, (tmp_path,), resolver=resolve)
    assert report["totals"] == {
        "requested_symbol_pairs": 2,
        "locally_resolved_symbol_pairs": 1,
        "still_missing_symbol_pairs": 1,
        "dates_with_source_errors": 0,
    }
    assert report["by_date"]["2026-02-25"]["still_missing_symbols"] == ["0056"]
    assert report["claim"] == "local_0901_source_recheck_only_not_fills_or_replay_promotion"


def test_recheck_rejects_duplicate_symbol_manifest(tmp_path: Path) -> None:
    source = tmp_path / "audit.json"
    _audit(source, ["0050", "0050"])
    with pytest.raises(ValueError, match="invalid missing symbol set"):
        recheck(source, (tmp_path,))


def test_recheck_distinguishes_later_bars_from_exact_0901_reader_gap(tmp_path: Path) -> None:
    source = tmp_path / "audit.json"
    _audit(source, ["0050", "0056", "2330"])
    root = tmp_path / "research_dataset"
    partition = root / "trade_date=2026-02-25" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["0050", "0050", "0056"],
        "minutes_from_open": [1, 2, 3],
    }).write_parquet(partition)

    def unresolved(_roots: tuple[Path, ...], _symbols: list[str], *, trading_date: date):
        assert trading_date == date(2026, 2, 25)
        return {}, {"error_counts": {}}

    report = recheck(source, (tmp_path,), resolver=unresolved, research_root=root)
    diagnostic = report["by_date"]["2026-02-25"]["first_minute_diagnostic"]
    assert diagnostic["histogram"] == {
        "1": 1, "3": 1, "absent_symbol_or_partition": 1,
    }
    assert diagnostic["exact_0901_but_unresolved_symbols"] == ["0050"]
    assert report["totals"]["first_minute_exact_0901_but_unresolved_pairs"] == 1
    assert report["first_minute_histogram_for_still_missing"] == diagnostic["histogram"]


def test_recheck_hft_overlap_is_candidate_not_resolved_fill(tmp_path: Path) -> None:
    source = tmp_path / "audit.json"
    _audit(source, ["0050", "0056"])
    hft_root = tmp_path / "hft_dataset"
    partition = hft_root / "trade_date=2026-02-25" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame({"code": ["0050", "2330"]}).write_parquet(partition)

    def unresolved(_roots: tuple[Path, ...], _symbols: list[str], *, trading_date: date):
        return {}, {"error_counts": {}}

    report = recheck(source, (tmp_path,), resolver=unresolved, hft_root=hft_root)
    assert report["totals"]["still_missing_symbol_pairs"] == 2
    assert report["totals"]["hft_universe_overlap_pairs"] == 1
    assert report["by_date"]["2026-02-25"]["hft_universe_diagnostic"][
        "overlap_symbols_require_raw_tick_validation"
    ] == ["0050"]
