from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from scripts.build_tw_minute_daily_guidance import build_guidance
from stockagent.data.tw_minute import (
    MINUTE_DAILY_GUIDANCE_CONTRACT,
    _load_minute_daily_guidance,
    minute_daily_guidance_manifest_path,
)


def _write_daily_fold(
    root: Path,
    *,
    fold_id: int,
    dates: list[str],
    requested_weights: np.ndarray,
) -> None:
    fold = root / f"fold_{fold_id:02d}"
    fold.mkdir(parents=True)
    (fold / "checkpoint_best.pt").write_bytes(f"fold-{fold_id}".encode())
    date_values = np.asarray(dates, dtype="datetime64[D]")
    np.savez(
        fold / "test_backtest.npz",
        execution_mode=np.asarray("tw_day_trade"),
        dates=date_values,
        requested_weights_history=requested_weights.astype(np.float32),
    )
    pl.DataFrame(
        {
            "date": date_values,
            "0050": np.zeros(len(dates), dtype=np.float32),
            "2330": np.zeros(len(dates), dtype=np.float32),
        }
    ).write_parquet(fold / "daily_weights.parquet")


def test_build_and_load_strict_oof_daily_guidance(tmp_path: Path) -> None:
    daily_root = tmp_path / "daily"
    daily_root.mkdir()
    summary = [
        {
            "fold_id": 1,
            "train_years": [2018],
            "val_years": [2019],
            "test_years": [2020, 2021],
        },
        {
            "fold_id": 2,
            "train_years": [2018, 2019],
            "val_years": [2020],
            "test_years": [2021],
        },
    ]
    (daily_root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    _write_daily_fold(
        daily_root,
        fold_id=1,
        dates=["2020-01-02", "2021-01-04"],
        requested_weights=np.asarray([[0.60, -0.40], [0.50, -0.50]]),
    )
    _write_daily_fold(
        daily_root,
        fold_id=2,
        dates=["2021-01-04"],
        requested_weights=np.asarray([[-0.20, 0.80]]),
    )
    minute_manifest = tmp_path / "minute_manifest.json"
    minute_manifest.write_text(
        json.dumps(
            {
                "symbols": ["0050", "2330"],
                "dates": ["2020-01-02", "2021-01-04"],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "oof_requested_weights.parquet"

    manifest = build_guidance(
        daily_output_dirs=(daily_root,),
        minute_manifest_path=minute_manifest,
        output_path=output,
    )
    guidance = _load_minute_daily_guidance(
        output,
        minute_symbols=("0050", "2330"),
        minute_dates=np.asarray(["2020-01-02", "2021-01-04"], dtype="datetime64[D]"),
    )

    assert manifest["contract"] == MINUTE_DAILY_GUIDANCE_CONTRACT
    assert [row["fold_id"] for row in manifest["year_owners"]] == [1, 2]
    np.testing.assert_allclose(
        guidance.weights,
        [[0.60, -0.40], [-0.20, 0.80]],
    )
    assert len(guidance.fingerprint) == 64


def test_daily_guidance_loader_rejects_noncausal_year_owner(
    tmp_path: Path,
) -> None:
    daily_root = tmp_path / "daily"
    daily_root.mkdir()
    (daily_root / "summary.json").write_text(
        json.dumps(
            [
                {
                    "fold_id": 1,
                    "train_years": [2018],
                    "val_years": [2019],
                    "test_years": [2020],
                }
            ]
        ),
        encoding="utf-8",
    )
    _write_daily_fold(
        daily_root,
        fold_id=1,
        dates=["2020-01-02"],
        requested_weights=np.asarray([[0.60, -0.40]]),
    )
    minute_manifest = tmp_path / "minute_manifest.json"
    minute_manifest.write_text(
        json.dumps({"symbols": ["0050", "2330"], "dates": ["2020-01-02"]}),
        encoding="utf-8",
    )
    output = tmp_path / "oof_requested_weights.parquet"
    build_guidance(
        daily_output_dirs=(daily_root,),
        minute_manifest_path=minute_manifest,
        output_path=output,
    )
    sidecar = minute_daily_guidance_manifest_path(output)
    manifest = json.loads(sidecar.read_text(encoding="utf-8"))
    manifest["year_owners"][0]["val_years"] = [2020]
    sidecar.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RuntimeError, match="not strict walk-forward OOF"):
        _load_minute_daily_guidance(
            output,
            minute_symbols=("0050", "2330"),
            minute_dates=np.asarray(["2020-01-02"], dtype="datetime64[D]"),
        )
