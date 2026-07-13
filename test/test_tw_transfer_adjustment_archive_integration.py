from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts.build_tw_yahoo_fallback_archive import (
    _file_receipt,
    _load_verified_transfer_adjustments,
    _read_symbol_fallback,
    main as build_yahoo_fallback_archive,
)
from scripts.rebuild_tw_public_data_layer import (
    RebuildRunner,
    _transfer_adjustment_command,
    _validate_transfer_adjustment_reference,
)


def _write_yahoo_source(path: Path) -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5)],
            "open": [100.0, 110.0],
            "max": [101.0, 111.0],
            "min": [99.0, 109.0],
            "close": [100.0, 110.0],
            "adjclose": [50.0, 55.0],
            "Trading_Volume": [1000.0, 1200.0],
        }
    )
    table = frame.to_arrow().replace_schema_metadata(
        {
            b"stockagent.source": b"yahoo",
            b"stockagent.asset_class": b"tw_stocks",
            b"stockagent.yahoo_requested_start": b"2000-01-01",
            b"stockagent.yahoo_checked_through": b"2000-01-31",
        }
    )
    pq.write_table(table, path)


def _write_transfer_artifact(
    path: Path,
    *,
    row_date: date,
    input_path: Path,
    coverage_complete: bool = True,
    unresolved_count: int = 0,
) -> Path:
    artifact = pl.DataFrame(
        {
            "date": [row_date],
            "symbol": ["2330"],
            "adjustment_factor": [0.8],
            "official_reference_price": [125.0],
            "reconstructed_close": [100.0],
        }
    )
    table = artifact.to_arrow().replace_schema_metadata(
        {
            b"stockagent.dataset": b"tw_transfer_adjustment_reference",
            b"stockagent.schema_version": b"1",
            b"stockagent.source": b"TWSE_TPEx_official_verified_Yahoo_bridge",
        }
    )
    pq.write_table(table, path)
    candidate_keys = [f"{row_date.isoformat()}|2330"]
    summary_path = path.with_suffix(".summary.json")
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "tw_transfer_adjustment_reference",
                "start_date": "2000-01-01",
                "end_date": "2000-01-31",
                "coverage_complete": coverage_complete,
                "replacement_promoted": coverage_complete,
                "unresolved_count": unresolved_count,
                "rows": 1,
                "candidate_count": 1,
                "required_candidate_count": 1,
                "candidate_keys": candidate_keys,
                "candidate_keys_sha256": hashlib.sha256(
                    "\n".join(candidate_keys).encode("utf-8")
                ).hexdigest(),
                "output_receipt": _file_receipt(path),
                "input_receipts": [_file_receipt(input_path)],
                "raw_receipts": [_file_receipt(input_path)],
            }
        ),
        encoding="utf-8",
    )
    return summary_path


def _archive_fixture(tmp_path: Path, *, transfer_date: date) -> tuple[Path, Path]:
    yahoo_dir = tmp_path / "yahoo"
    official_dir = tmp_path / "official"
    output = tmp_path / "fallback" / "yahoo_tw_ohlcv.parquet"
    yahoo_dir.mkdir()
    official_dir.mkdir()
    output.parent.mkdir()
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["TSMC"],
            "market": ["TWSE"],
            "yahoo_symbol": ["2330.TW"],
        }
    ).write_csv(yahoo_dir / "symbols.csv")
    _write_yahoo_source(yahoo_dir / "2330_features.parquet")
    input_path = official_dir / "official_receipt_input.json"
    input_path.write_text('{"source":"TWSE"}\n', encoding="utf-8")
    transfer_path = official_dir / "tw_transfer_adjustment_reference.parquet"
    _write_transfer_artifact(
        transfer_path,
        row_date=transfer_date,
        input_path=input_path,
    )
    return output, transfer_path


def _run_archive(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    output: Path,
    transfer_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_tw_yahoo_fallback_archive.py",
            "--input-dir",
            str(tmp_path / "yahoo"),
            "--official-input-dir",
            str(tmp_path / "official"),
            "--output-path",
            str(output),
            "--start-date",
            "2000-01-01",
            "--end-date",
            "2000-01-31",
            "--workers",
            "1",
            "--transfer-adjustment-reference",
            str(transfer_path),
        ],
    )
    build_yahoo_fallback_archive()


def test_archive_applies_verified_transfer_factor_only_to_matching_first_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output, transfer_path = _archive_fixture(
        tmp_path,
        transfer_date=date(2000, 1, 4),
    )

    _run_archive(monkeypatch, tmp_path, output, transfer_path)

    archive = pl.read_parquet(output)
    assert archive.get_column("source_factor").to_list() == [0.8, 1.1]
    summary = json.loads(output.with_suffix(".summary.json").read_text())
    inputs = json.loads(output.with_suffix(".inputs.json").read_text())
    assert summary["transfer_adjustment_reference_rows"] == 1
    assert summary["transfer_adjustment_candidate_count"] == 1
    assert summary["transfer_adjustment_required_candidate_count"] == 1
    assert summary["transfer_adjustment_applied_rows"] == 1
    assert summary["transfer_adjustment_unmatched_rows"] == 0
    assert summary["transfer_adjustment_accounting_complete"] is True
    assert summary["transfer_adjustment_reference_receipt"] == _file_receipt(
        transfer_path
    )
    assert inputs["transfer_adjustment_reference_receipt"] == _file_receipt(
        transfer_path
    )
    assert inputs["transfer_adjustment_summary_receipt"] == _file_receipt(
        transfer_path.with_suffix(".summary.json")
    )
    assert summary["transfer_adjustment_input_receipt_count"] == 1
    assert summary["transfer_adjustment_raw_receipt_count"] == 1


def test_archive_fails_when_transfer_key_is_not_a_source_first_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output, transfer_path = _archive_fixture(
        tmp_path,
        transfer_date=date(2000, 1, 5),
    )

    with pytest.raises(RuntimeError, match="did not map 1:1"):
        _run_archive(monkeypatch, tmp_path, output, transfer_path)

    assert not output.exists()
    summary = json.loads(output.with_suffix(".summary.json").read_text())
    assert summary["transfer_adjustment_reference_rows"] == 1
    assert summary["transfer_adjustment_applied_rows"] == 0
    assert summary["transfer_adjustment_unmatched_rows"] == 1
    assert summary["transfer_adjustment_accounting_complete"] is False


def test_venue_alias_first_row_cannot_consume_canonical_transfer_factor(
    tmp_path: Path,
) -> None:
    source = tmp_path / "2330_TW_features.parquet"
    _write_yahoo_source(source)

    result, frame = _read_symbol_fallback(
        "2330",
        [(source, "TW")],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
        transfer_adjustments={("2330", date(2000, 1, 4)): 0.8},
    )

    assert result.status == "ok"
    assert result.transfer_adjustment_rows == 0
    assert frame is not None
    assert frame.item(0, "source_factor") is None


def test_transfer_artifact_validation_is_fail_closed_on_coverage_and_receipts(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text("{}\n", encoding="utf-8")
    transfer_path = tmp_path / "tw_transfer_adjustment_reference.parquet"
    summary_path = _write_transfer_artifact(
        transfer_path,
        row_date=date(2000, 1, 4),
        input_path=input_path,
        coverage_complete=False,
    )

    with pytest.raises(RuntimeError, match="coverage_complete"):
        _load_verified_transfer_adjustments(
            transfer_path,
            start=date(2000, 1, 1),
            end=date(2000, 1, 31),
        )

    payload = json.loads(summary_path.read_text())
    payload["coverage_complete"] = True
    summary_path.write_text(json.dumps(payload), encoding="utf-8")
    input_path.write_text('{"tampered":true}\n', encoding="utf-8")
    with pytest.raises(RuntimeError, match="input_receipts"):
        _load_verified_transfer_adjustments(
            transfer_path,
            start=date(2000, 1, 1),
            end=date(2000, 1, 31),
        )

    input_path.write_text("{}\n", encoding="utf-8")
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4)],
            "symbol": ["2330"],
            "adjustment_factor": [0.9],
        }
    ).write_parquet(transfer_path)
    with pytest.raises(RuntimeError, match="output_receipt"):
        _load_verified_transfer_adjustments(
            transfer_path,
            start=date(2000, 1, 1),
            end=date(2000, 1, 31),
        )


def test_historical_transfer_artifact_cannot_self_certify_as_empty(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text("{}\n", encoding="utf-8")
    transfer_path = tmp_path / "tw_transfer_adjustment_reference.parquet"
    empty = pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "adjustment_factor": pl.Float64,
        }
    )
    pq.write_table(
        empty.to_arrow().replace_schema_metadata(
            {
                b"stockagent.dataset": b"tw_transfer_adjustment_reference",
                b"stockagent.schema_version": b"1",
                b"stockagent.source": b"TWSE_TPEx_official_verified_Yahoo_bridge",
            }
        ),
        transfer_path,
    )
    transfer_path.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "tw_transfer_adjustment_reference",
                "start_date": "2000-01-01",
                "end_date": "2026-07-12",
                "coverage_complete": True,
                "replacement_promoted": True,
                "unresolved_count": 0,
                "rows": 0,
                "candidate_count": 0,
                "required_candidate_count": 0,
                "candidate_keys": [],
                "candidate_keys_sha256": hashlib.sha256(b"").hexdigest(),
                "output_receipt": _file_receipt(transfer_path),
                "input_receipts": [_file_receipt(input_path)],
                "raw_receipts": [_file_receipt(input_path)],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="historical_candidates_nonempty"):
        _load_verified_transfer_adjustments(
            transfer_path,
            start=date(2000, 1, 1),
            end=date(2026, 7, 12),
        )


def test_rebuild_transfer_stage_command_uses_official_provider_limits() -> None:
    args = argparse.Namespace(
        mode="repair",
        fallback_start_date="2000-01-01",
        end_date="2026-07-12",
        public_workers=3,
        request_interval=None,
        timeout=45,
        retries=5,
        resume=True,
    )
    command = _transfer_adjustment_command(
        args,
        public_dir=Path("stage/data_tw_public"),
        yahoo_source_dir=Path("stage/data_tw_public/fallback/yahoo_tw_stocks"),
        output_path=Path(
            "stage/data_tw_public/tw_transfer_adjustment_reference.parquet"
        ),
    )

    assert command[1] == "downloader/download_tw_transfer_adjustments.py"
    assert command[command.index("--mode") + 1] == "repair"
    assert command[command.index("--official-input-dir") + 1] == (
        "stage/data_tw_public"
    )
    assert command[command.index("--yahoo-source-dir") + 1].endswith(
        "fallback/yahoo_tw_stocks"
    )
    assert command[command.index("--end-date") + 1] == "2026-07-12"
    assert command[command.index("--workers") + 1] == "3"
    assert "--request-interval" not in command
    assert command[-1] == "--resume"

    args.request_interval = 0.2
    limited = _transfer_adjustment_command(
        args,
        public_dir=Path("stage/data_tw_public"),
        yahoo_source_dir=Path("stage/data_tw_public/fallback/yahoo_tw_stocks"),
        output_path=Path(
            "stage/data_tw_public/tw_transfer_adjustment_reference.parquet"
        ),
    )
    assert limited[limited.index("--request-interval") + 1] == "0.2"


def test_rebuild_resume_skip_revalidates_incomplete_transfer_summary(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text("{}\n", encoding="utf-8")
    transfer_path = tmp_path / "tw_transfer_adjustment_reference.parquet"
    summary_path = _write_transfer_artifact(
        transfer_path,
        row_date=date(2000, 1, 4),
        input_path=input_path,
        coverage_complete=False,
    )
    manifest_path = tmp_path / "rebuild_manifest.json"
    command = [sys.executable, "-c", "pass"]
    outputs = [transfer_path, summary_path]

    legacy = RebuildRunner(
        manifest_path=manifest_path,
        resume=True,
        dry_run=False,
    )
    legacy.run("tw_transfer_adjustment_reference", command, outputs=outputs)

    resumed = RebuildRunner(
        manifest_path=manifest_path,
        resume=True,
        dry_run=False,
    )
    with pytest.raises(RuntimeError, match="coverage_complete"):
        resumed.run(
            "tw_transfer_adjustment_reference",
            command,
            outputs=outputs,
            validate_outputs=lambda: _validate_transfer_adjustment_reference(
                transfer_path,
                start=date(2000, 1, 1),
                end=date(2000, 1, 31),
            ),
        )

    stages = json.loads(manifest_path.read_text())["stages"]
    assert [stage["status"] for stage in stages] == ["complete", "failed"]
    assert "coverage_complete" in stages[-1]["message"]
