from __future__ import annotations

import json
import math
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from scripts.audit_tw_public_data_layer import (
    audit_delisted_universe_coverage,
    audit_feature_lineage_registry,
    audit_non_vintage_archive_contract,
    audit_official_symbol_build,
    audit_panel_contract,
    audit_quote_source_files,
    audit_return_price_provenance,
    audit_source_receipts,
    audit_snapshot_contract,
    audit_walk_forward_availability,
)
from scripts.rebuild_tw_public_data_layer import (
    RebuildRunner,
    _promote_one,
    _rollback_promoted_tree,
)
from scripts.build_tw_official_symbol_parquets import (
    MIXED_FALLBACK_SOURCE_NAME,
    _receipt,
    _write_official_quote_parquet,
)
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_public_features import _file_content_receipt


def _panel(dates: list[str]) -> PanelData:
    rows = len(dates)
    tradable = np.ones((rows, 1), dtype=bool)
    returns = np.zeros((rows, 1), dtype=np.float32)
    if rows > 1:
        returns[0, 0] = np.float32(math.log(1.01))
    return PanelData(
        dates=np.asarray(dates, dtype="datetime64[ns]"),
        symbols=["2330"],
        feature_names=["body_ratio"],
        features=np.zeros((rows, 1, 1), dtype=np.float32),
        returns_1d=returns,
        tradable_mask=tradable,
        can_buy_mask=tradable.copy(),
        can_sell_mask=tradable.copy(),
        can_short_open_mask=tradable.copy(),
        force_short_cover_mask=np.zeros_like(tradable),
        force_exit_mask=np.zeros_like(tradable),
        alive_mask=tradable.copy(),
        benchmark_returns=returns[:, 0].copy(),
        close_prices=np.full((rows, 1), 100.0, dtype=np.float32),
        daily_volumes=np.full((rows, 1), 1000.0, dtype=np.float32),
    )


def test_snapshot_only_cumulative_revenue_is_quarantined() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    findings = audit_snapshot_contract(Path("/does/not/need/to/exist"), config)
    cumulative = next(
        item for item in findings if item.item == "twpub_cumulative_revenue_yoy"
    )
    assert cumulative.severity == "low"
    assert "all_zero_filled=True" in cumulative.evidence

    config.data.feature_zero_fill = [
        pattern
        for pattern in config.data.feature_zero_fill
        if pattern != "twpub_cumulative_revenue_yoy"
    ]
    findings = audit_snapshot_contract(Path("/does/not/need/to/exist"), config)
    cumulative = next(
        item for item in findings if item.item == "twpub_cumulative_revenue_yoy"
    )
    assert cumulative.severity == "critical"


def test_single_vintage_macro_archives_are_quarantined() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    findings = audit_non_vintage_archive_contract(Path("/not/required"), config)
    dgbas = next(item for item in findings if item.item == "twpub_dgbas_*")
    mof = next(item for item in findings if item.item == "twpub_mof_*")
    assert dgbas.severity == "low"
    assert mof.severity == "low"
    assert audit_feature_lineage_registry(config) == []

    config.data.feature_zero_fill = [
        pattern for pattern in config.data.feature_zero_fill if pattern != "twpub_dgbas_*"
    ]
    findings = audit_non_vintage_archive_contract(Path("/not/required"), config)
    dgbas = next(item for item in findings if item.item == "twpub_dgbas_*")
    assert dgbas.severity == "critical"


def test_rule_receipt_is_required_and_machine_checked(tmp_path: Path) -> None:
    config = load_config("configs/markets/tw_public.yaml")
    (tmp_path / "download_summary.json").write_text(
        json.dumps({"mode": "full", "failed_count": 0}),
        encoding="utf-8",
    )

    _, findings = audit_source_receipts(tmp_path, config)
    assert any(item.code == "missing_short_rule_receipt" for item in findings)

    (tmp_path / "tw_short_sale_download_report.json").write_text(
        json.dumps(
            {
                "requests_complete": True,
                "failure_count": 0,
                "unparseable": 0,
                "data_output_written": True,
                "archive_cohort_coverage_complete": True,
                "data_quality": {
                    "rows": 2,
                    "symbols_nonempty_rows": 2,
                    "composite_key_duplicate_rows": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    _, findings = audit_source_receipts(tmp_path, config)
    assert findings == []


def test_panel_contract_accepts_zero_terminal_label_and_valid_destination() -> None:
    panel = _panel(["2024-01-02", "2024-01-03"])
    summary, findings = audit_panel_contract(
        panel,
        np.asarray(["2024-01-02", "2024-01-03"], dtype="datetime64[D]"),
    )

    assert findings == []
    assert summary["destination_missing_quote_label_violations"] == 0
    assert summary["terminal_nonzero_labels"] == 0


def test_panel_contract_rejects_non_benchmark_time_axis_rows() -> None:
    panel = _panel(["2024-01-02", "2024-01-03", "2024-01-04"])
    _, findings = audit_panel_contract(
        panel,
        np.asarray(["2024-01-02", "2024-01-04"], dtype="datetime64[D]"),
    )

    assert any(item.code == "extra_non_benchmark_dates" for item in findings)


def test_delisted_universe_requires_pre_delisting_history(tmp_path: Path) -> None:
    parquet_root = tmp_path / "stocks"
    public_dir = tmp_path / "public"
    parquet_root.mkdir()
    public_dir.mkdir()
    pl.DataFrame(
        {
            "symbol": ["1111", "2222"],
            "date": [date(2024, 6, 1), date(2024, 6, 1)],
            "company_name": ["covered", "relisted"],
        }
    ).write_parquet(public_dir / "twse_delisted_company.parquet")
    pl.DataFrame(
        schema={"symbol": pl.String, "date": pl.Date, "company_name": pl.String}
    ).write_parquet(public_dir / "tpex_delisted_company.parquet")
    for symbol, history_date in (("1111", date(2024, 5, 31)), ("2222", date(2025, 1, 2))):
        pl.DataFrame({"date": [history_date], "close": [10.0]}).write_parquet(
            parquet_root / f"{symbol}_features.parquet"
        )

    profiles, missing, findings = audit_delisted_universe_coverage(
        parquet_root,
        public_dir,
        np.asarray(["2024-01-02", "2025-12-31"], dtype="datetime64[D]"),
    )

    twse = next(item for item in profiles if item["market"] == "twse")
    assert twse["canonical_symbol_histories"] == 1
    assert twse["missing_symbol_histories"] == 1
    assert missing[0]["symbol"] == "2222"
    assert missing[0]["missing_reason"] == "no_history_on_or_before_delisting"
    assert any(item.code == "delisted_universe_coverage" for item in findings)


def test_quote_source_audit_detects_duplicates_and_impossible_bars(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 2)],
            "open": [10.0, 10.0],
            "max": [11.0, 9.0],
            "min": [9.0, 8.0],
            "close": [10.5, 10.0],
            "adjclose": [10.0, 10.1],
            "Trading_Volume": [1000.0, -1.0],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    profiles, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02"], dtype="datetime64[D]"),
        workers=1,
    )

    assert profiles[0]["duplicate_dates"] == 1
    assert summary["invalid_ohlc_geometry"] == 1
    assert summary["negative_volume"] == 1
    codes = {item.code for item in findings}
    assert {"duplicate_quote_dates", "invalid_ohlc_geometry", "negative_quote_volume"} <= codes


def test_quote_source_audit_separates_raw_corporate_actions_from_adjusted_jumps(
    tmp_path: Path,
) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "open": [10.0, 1.0, 1.0],
            "max": [10.0, 1.0, 1.0],
            "min": [10.0, 1.0, 1.0],
            "close": [10.0, 1.0, 1.0],
            "adjclose": [10.0, 10.1, 40.4],
            "Trading_Volume": [1000.0, 1000.0, 1000.0],
        }
    ).write_parquet(tmp_path / "1234_features.parquet")

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02", "2024-01-03", "2024-01-04"], dtype="datetime64[D]"),
        workers=1,
    )

    assert summary["raw_close_jumps_gt_2x"] == 1
    assert summary["adjusted_index_jumps_gt_3x"] == 1
    assert any(item.code == "extreme_adjusted_index_jump" for item in findings)
    assert not any(item.code == "extreme_source_price_jump" for item in findings)


def test_quote_source_audit_rejects_non_stock_non_etf_security(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2)],
            "open": [10.0],
            "max": [10.0],
            "min": [10.0],
            "close": [10.0],
            "adjclose": [10.0],
            "Trading_Volume": [1000.0],
        }
    ).write_parquet(tmp_path / "01001T_features.parquet")

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2024-01-02"], dtype="datetime64[D]"),
        workers=1,
    )

    assert summary["unsupported_security_files"] == 1
    assert any(item.code == "unsupported_tw_security_type" for item in findings)


def test_quote_source_audit_accepts_audited_yahoo_fallback_lineage(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2004, 2, 11)],
            "open": [10.0, 20.0],
            "max": [10.5, 21.0],
            "min": [9.5, 19.0],
            "close": [10.0, 20.0],
            "Trading_Volume": [1000.0, 2000.0],
            "data_source": ["yahoo_fallback", "twse_official"],
            "adjustment_source": ["yahoo_fallback", "twse_official"],
            "adjclose": [10.0, 10.5],
        }
    )
    _write_official_quote_parquet(
        frame,
        tmp_path / "2330_features.parquet",
        checked_through="2004-02-11",
        source=MIXED_FALLBACK_SOURCE_NAME,
    )

    _, summary, findings = audit_quote_source_files(
        tmp_path,
        np.asarray(["2000-01-04", "2004-02-11"], dtype="datetime64[D]"),
        workers=1,
        require_official=True,
    )

    assert summary["unapproved_source_files"] == 0
    assert summary["yahoo_fallback_rows"] == 1
    assert summary["yahoo_fallback_adjustment_rows"] == 1
    assert summary["source_lineage_mismatch"] == 0
    assert not any("source" in finding.code for finding in findings)


def test_official_symbol_build_audit_accepts_relocated_yahoo_receipt(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "data_tw_public"
    parquet_root = public_dir / "stocks"
    fallback_dir = public_dir / "fallback"
    parquet_root.mkdir(parents=True)
    fallback_dir.mkdir()
    for name in (
        "twse_daily_ohlcv.parquet",
        "tpex_daily_ohlcv.parquet",
        "tw_corporate_action_reference.parquet",
    ):
        pl.DataFrame({"marker": [name]}).write_parquet(public_dir / name)

    fallback = fallback_dir / "yahoo_tw_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4)],
            "symbol": ["2330"],
            "name": ["TSMC"],
            "market": ["twse"],
            "open": [100.0],
            "max": [101.0],
            "min": [99.0],
            "close": [100.0],
            "Trading_Volume": [1000.0],
            "source_adjclose": [50.0],
            "source_factor": [None],
            "quote_source": ["yahoo_fallback"],
        }
    ).write_parquet(fallback)
    fallback_receipt = _receipt(fallback)
    stale_fallback_receipt = {
        **fallback_receipt,
        "path": "/old/staged/data_tw_public/fallback/yahoo_tw_ohlcv.parquet",
    }
    fallback.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "source": "yahoo_fallback",
                "start_date": "2000-01-01",
                "failed_symbol_count": 0,
                "output_receipt": stale_fallback_receipt,
            }
        ),
        encoding="utf-8",
    )

    _write_official_quote_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4)],
                "open": [100.0],
                "max": [101.0],
                "min": [99.0],
                "close": [100.0],
                "Trading_Volume": [1000.0],
                "data_source": ["yahoo_fallback"],
                "adjustment_source": ["yahoo_fallback"],
                "adjclose": [10.0],
            }
        ),
        parquet_root / "2330_features.parquet",
        checked_through="2000-01-04",
        source=MIXED_FALLBACK_SOURCE_NAME,
    )
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["TSMC"],
            "market": ["twse"],
            "security_type": ["stock"],
            "source": [MIXED_FALLBACK_SOURCE_NAME],
        }
    ).write_csv(parquet_root / "symbols.csv")
    source_paths = [
        public_dir / "twse_daily_ohlcv.parquet",
        public_dir / "tpex_daily_ohlcv.parquet",
        public_dir / "tw_corporate_action_reference.parquet",
    ]
    (parquet_root / "official_symbol_build_summary.json").write_text(
        json.dumps(
            {
                "source": MIXED_FALLBACK_SOURCE_NAME,
                "adjusted_price_method": "first=10; test",
                "missing_adjustment_rows": 0,
                "source_receipts": [_file_content_receipt(path) for path in source_paths],
                "legacy_source_name": "",
                "legacy_source_receipts": [],
                "fallback_source_name": "yahoo_fallback",
                "fallback_source_receipts": [stale_fallback_receipt],
                "fallback_rows": 1,
                "fallback_adjustment_rows": 0,
                "fallback_symbols": 1,
                "symbols": 1,
            }
        ),
        encoding="utf-8",
    )

    summary, findings = audit_official_symbol_build(parquet_root, public_dir)

    assert summary["valid"] is True
    assert findings == []


def test_walk_forward_audit_fails_when_configured_fold_is_unavailable() -> None:
    config = load_config("configs/markets/tw_public.yaml")
    config.runner.start_fold = 3
    config.walk_forward.expected_first_year = 2004
    panel = _panel(["2004-01-02", "2005-01-03", "2006-01-03"])

    summary, findings = audit_walk_forward_availability(panel, config)

    assert summary["fold_count"] == 2
    assert summary["last_fold"] == 2
    assert summary["target_folds"][0]["fold_id"] == 3
    assert summary["target_folds"][0]["available"] is False
    assert [item.code for item in findings] == ["configured_fold_unavailable"]


def test_raw_close_provenance_blocks_model_safe_audit(tmp_path: Path) -> None:
    (tmp_path / "1234_features.parquet").touch()
    (tmp_path / "return_price_provenance.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "symbols": {
                    "1234": {
                        "kind": "official_raw_close",
                        "source": "official",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    summary, findings = audit_return_price_provenance(tmp_path)

    assert summary["raw_close_symbols"] == 1
    assert findings[0].code == "unadjusted_delisted_return_history"
    assert findings[0].severity == "high"


def test_rebuild_resume_requires_matching_command_and_sha256(tmp_path: Path) -> None:
    output = tmp_path / "output.txt"
    manifest = tmp_path / "manifest.json"
    write_v1 = [
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(output)!r}).write_text('v1')",
    ]
    runner = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    runner.run("example", write_v1, outputs=[output])
    first_mtime = output.stat().st_mtime_ns

    resumed = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    resumed.run("example", write_v1, outputs=[output])
    assert output.stat().st_mtime_ns == first_mtime

    output.write_text("tampered", encoding="utf-8")
    repaired = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    repaired.run("example", write_v1, outputs=[output])
    assert output.read_text(encoding="utf-8") == "v1"

    write_v2 = [
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(output)!r}).write_text('v2')",
    ]
    changed_command = RebuildRunner(manifest_path=manifest, resume=True, dry_run=False)
    changed_command.run("example", write_v2, outputs=[output])
    assert output.read_text(encoding="utf-8") == "v2"


def test_single_promotion_restores_old_data_when_new_move_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staged = tmp_path / "stage" / "new"
    production = tmp_path / "production" / "current"
    backup = tmp_path / "backup" / "old"
    staged.mkdir(parents=True)
    production.mkdir(parents=True)
    (staged / "marker").write_text("new", encoding="utf-8")
    (production / "marker").write_text("old", encoding="utf-8")

    original_replace = os.replace

    def fail_new_move(source, destination):
        if Path(source) == staged and Path(destination) == production:
            raise OSError("simulated staged move failure")
        return original_replace(source, destination)

    monkeypatch.setattr("scripts.rebuild_tw_public_data_layer.os.replace", fail_new_move)
    with pytest.raises(OSError, match="simulated staged move failure"):
        _promote_one(staged, production, backup)

    assert (production / "marker").read_text(encoding="utf-8") == "old"
    assert (staged / "marker").read_text(encoding="utf-8") == "new"
    assert not backup.exists()


def test_single_public_tree_promotion_can_roll_back(tmp_path: Path) -> None:
    staged = tmp_path / "stage" / "data_tw_public"
    production = tmp_path / "production" / "data_tw_public"
    backup = tmp_path / "backup" / "data_tw_public"
    staged.mkdir(parents=True)
    production.mkdir(parents=True)
    (staged / "marker").write_text("new", encoding="utf-8")
    (production / "marker").write_text("old", encoding="utf-8")

    _promote_one(staged, production, backup)
    _rollback_promoted_tree(staged=staged, production=production, backup=backup)

    assert (production / "marker").read_text(encoding="utf-8") == "old"
    assert (staged / "marker").read_text(encoding="utf-8") == "new"
