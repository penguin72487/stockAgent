#!/usr/bin/env python3
"""Build strict walk-forward daily targets for the one-minute policy.

Each minute session is owned by the most recent daily fold whose complete train
and validation horizon precedes that session's year, whose test horizon contains
the year, and whose artifact contains that exact date.  Date-level ownership is
necessary because split-local daily lookback deliberately removes the first
``lookback - 1`` test targets from every fold.  An older causal fold supplies
only those warmup-gap dates; no final checkpoint may leak future years into
historical minute inputs.  The output stores the daily model's requested signed
target weights; minute execution rules and fills are applied later by the
canonical tw_minute ledger.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Sequence

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_minute import (  # noqa: E402
    MINUTE_DAILY_GUIDANCE_CONTRACT,
    minute_trainable_session_summaries,
    minute_daily_guidance_manifest_path,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _causal_candidates(
    sources: Sequence[tuple[Path, list[dict[str, Any]]]],
    target_years: Sequence[int],
) -> dict[int, list[tuple[Path, dict[str, Any]]]]:
    candidates: dict[int, list[tuple[Path, dict[str, Any]]]] = {
        int(year): [] for year in target_years
    }
    scored_identities: dict[int, dict[tuple[int, int], tuple[Path, int]]] = {
        int(year): {} for year in target_years
    }
    for root, summary in sources:
        for row in summary:
            train_years = [int(value) for value in row.get("train_years", [])]
            val_years = [int(value) for value in row.get("val_years", [])]
            test_years = [int(value) for value in row.get("test_years", [])]
            if not train_years or not val_years or not test_years:
                raise RuntimeError("daily summary contains an incomplete fold row")
            information_cutoff = max((*train_years, *val_years))
            for target_year in target_years:
                if target_year not in test_years or information_cutoff >= target_year:
                    continue
                score = (max(val_years), max(train_years))
                identity = (root.resolve(), int(row["fold_id"]))
                previous = scored_identities[target_year].get(score)
                if previous is not None and previous != identity:
                    previous_root, previous_fold = previous
                    raise RuntimeError(
                        "daily guidance has ambiguous equally recent causal "
                        f"owners for {target_year}: "
                        f"{previous_root}/fold_{previous_fold:02d} and "
                        f"{root}/fold_{int(row['fold_id']):02d}"
                    )
                scored_identities[target_year][score] = identity
                candidates[target_year].append((root, row))
    for year, rows in candidates.items():
        rows.sort(
            key=lambda item: (
                max(int(value) for value in item[1]["val_years"]),
                max(int(value) for value in item[1]["train_years"]),
            ),
            reverse=True,
        )
    return candidates


def build_guidance(
    *,
    daily_output_dirs: Sequence[Path],
    minute_manifest_path: Path,
    output_path: Path,
    allow_zero_fill_missing: bool = False,
) -> dict[str, Any]:
    minute_manifest = _read_json(minute_manifest_path)
    minute_symbols = tuple(sorted(str(value) for value in minute_manifest["symbols"]))
    if minute_manifest.get("partitions"):
        usable_date_strings, _, excluded_unusable_dates = (
            minute_trainable_session_summaries(minute_manifest)
        )
    else:
        # Lightweight synthetic manifests used by focused tests predate the
        # receipt-backed partition contract. Production manifests always take
        # the validated branch above.
        usable_date_strings = [str(value) for value in minute_manifest["dates"]]
        excluded_unusable_dates = ()
    minute_dates = np.asarray(usable_date_strings, dtype="datetime64[D]")
    if not minute_symbols or minute_dates.size == 0:
        raise RuntimeError("minute manifest has no symbols or dates")
    if not bool(np.all(minute_dates[1:] > minute_dates[:-1])):
        raise RuntimeError("minute manifest dates are not strictly increasing")

    resolved_daily_roots = tuple(Path(value).resolve() for value in daily_output_dirs)
    if not resolved_daily_roots:
        raise RuntimeError("at least one daily output directory is required")
    daily_sources: list[tuple[Path, list[dict[str, Any]]]] = []
    summary_receipts: list[dict[str, str]] = []
    for daily_root in resolved_daily_roots:
        summary_path = daily_root / "summary.json"
        summary = _read_json(summary_path)
        if not isinstance(summary, list) or not summary:
            raise RuntimeError(f"daily summary is missing or empty: {summary_path}")
        daily_sources.append((daily_root, summary))
        summary_receipts.append(
            {
                "daily_output_dir": str(daily_root),
                "summary_sha256": _sha256(summary_path),
            }
        )
    target_years = [
        int(value)
        for value in sorted(
            set(minute_dates.astype("datetime64[Y]").astype(int) + 1970)
        )
    ]
    candidates = _causal_candidates(daily_sources, target_years)
    missing_owner_years = [year for year in target_years if not candidates[year]]
    if missing_owner_years:
        raise RuntimeError(
            "daily outputs lack a causal OOF fold covering each minute "
            f"year: {missing_owner_years}"
        )

    output_weights = np.zeros(
        (int(minute_dates.size), len(minute_symbols)), dtype=np.float32
    )
    output_symbol_lookup = {
        symbol: index for index, symbol in enumerate(minute_symbols)
    }
    source_cache: dict[tuple[Path, int], dict[str, Any]] = {}
    owner_segments: list[dict[str, Any]] = []
    zero_filled_dates: list[str] = []

    def load_source(
        daily_output_dir: Path,
        owner: dict[str, Any],
    ) -> dict[str, Any]:
        fold_id = int(owner["fold_id"])
        cache_key = (daily_output_dir.resolve(), fold_id)
        cached = source_cache.get(cache_key)
        if cached is not None:
            return cached
        fold_dir = daily_output_dir / f"fold_{fold_id:02d}"
        checkpoint_path = fold_dir / "checkpoint_best.pt"
        backtest_path = fold_dir / "test_backtest.npz"
        weights_table_path = fold_dir / "daily_weights.parquet"
        for required in (checkpoint_path, backtest_path, weights_table_path):
            if not required.is_file() or required.stat().st_size <= 0:
                raise RuntimeError(f"daily guidance source is incomplete: {required}")

        weight_schema = pl.read_parquet_schema(weights_table_path)
        source_symbols = tuple(name for name in weight_schema if name != "date")
        source_symbol_lookup = {
            symbol: index for index, symbol in enumerate(source_symbols)
        }
        missing_symbols = [
            symbol for symbol in minute_symbols if symbol not in source_symbol_lookup
        ]
        if missing_symbols and not allow_zero_fill_missing:
            raise RuntimeError(
                f"daily fold {fold_id} lacks minute symbols: {missing_symbols[:20]}"
            )
        matched_output_indices = np.asarray(
            [
                index
                for index, symbol in enumerate(minute_symbols)
                if symbol in source_symbol_lookup
            ],
            dtype=np.int64,
        )
        matched_source_indices = np.asarray(
            [
                source_symbol_lookup[minute_symbols[index]]
                for index in matched_output_indices
            ],
            dtype=np.int64,
        )

        with np.load(backtest_path, allow_pickle=False) as payload:
            execution_mode = str(payload["execution_mode"].item())
            if execution_mode != "tw_day_trade":
                raise RuntimeError(
                    f"daily fold {fold_id} execution_mode={execution_mode!r}; "
                    "expected 'tw_day_trade'"
                )
            source_dates = payload["dates"].astype("datetime64[D]")
            requested = payload["requested_weights_history"].astype(
                np.float32, copy=False
            )
        if requested.shape != (int(source_dates.size), len(source_symbols)):
            raise RuntimeError(
                f"daily fold {fold_id} requested target shape disagrees with symbols"
            )
        table_dates = (
            pl.read_parquet(weights_table_path, columns=["date"])["date"]
            .to_numpy()
            .astype("datetime64[D]")
        )
        if not np.array_equal(table_dates, source_dates):
            raise RuntimeError(
                f"daily fold {fold_id} weights table and backtest dates disagree"
            )
        if not bool(np.isfinite(requested).all()):
            raise RuntimeError(f"daily fold {fold_id} requested targets are non-finite")
        if source_dates.size == 0 or not bool(
            np.all(source_dates[1:] > source_dates[:-1])
        ):
            raise RuntimeError(
                f"daily fold {fold_id} backtest dates are not sorted and unique"
            )
        cached = {
            "fold_id": fold_id,
            "source_dates": source_dates,
            "requested": requested,
            "matched_output_indices": matched_output_indices,
            "matched_source_indices": matched_source_indices,
            "missing_symbols": missing_symbols,
            "checkpoint_sha256": _sha256(checkpoint_path),
            "backtest_sha256": _sha256(backtest_path),
            "weights_table_sha256": _sha256(weights_table_path),
        }
        source_cache[cache_key] = cached
        return cached

    for target_year in target_years:
        target_rows = np.flatnonzero(
            minute_dates.astype("datetime64[Y]").astype(int) + 1970 == target_year
        )
        target_dates = minute_dates[target_rows]
        unresolved = np.ones(target_rows.size, dtype=np.bool_)
        for daily_output_dir, owner in candidates[target_year]:
            source = load_source(daily_output_dir, owner)
            source_dates = source["source_dates"]
            positions = np.searchsorted(source_dates, target_dates)
            exact = positions < source_dates.size
            exact &= (
                source_dates[np.clip(positions, 0, source_dates.size - 1)]
                == target_dates
            )
            fill = unresolved & exact
            fill_rows = np.flatnonzero(fill)
            if fill_rows.size == 0:
                continue
            matched_output_indices = source["matched_output_indices"]
            matched_source_indices = source["matched_source_indices"]
            if matched_output_indices.size > 0:
                destination_rows = target_rows[fill_rows]
                output_weights[np.ix_(destination_rows, matched_output_indices)] = (
                    source["requested"][positions[fill_rows]][
                        :, matched_source_indices
                    ]
                )
            unresolved[fill_rows] = False

            run_boundaries = np.flatnonzero(np.diff(fill_rows) > 1) + 1
            for run in np.split(fill_rows, run_boundaries):
                owner_segments.append(
                    {
                        "target_year": int(target_year),
                        "date_start": str(target_dates[int(run[0])]),
                        "date_end": str(target_dates[int(run[-1])]),
                        "target_rows": int(run.size),
                        "daily_output_dir": str(daily_output_dir),
                        "fold_id": int(source["fold_id"]),
                        "train_years": [
                            int(value) for value in owner["train_years"]
                        ],
                        "val_years": [int(value) for value in owner["val_years"]],
                        "test_years": [
                            int(value) for value in owner["test_years"]
                        ],
                        "checkpoint_sha256": source["checkpoint_sha256"],
                        "backtest_sha256": source["backtest_sha256"],
                        "weights_table_sha256": source["weights_table_sha256"],
                        "zero_filled_missing_symbol_count": len(
                            source["missing_symbols"]
                        ),
                        "zero_filled_missing_symbols": source[
                            "missing_symbols"
                        ][:20],
                    }
                )
            if not bool(unresolved.any()):
                break
        if bool(unresolved.any()):
            missing_dates = [str(value) for value in target_dates[unresolved]]
            if not allow_zero_fill_missing:
                raise RuntimeError(
                    "daily outputs lack an exact causal OOF row for minute dates: "
                    f"{missing_dates[:20]}"
                )
            zero_filled_dates.extend(missing_dates)

    owner_segments.sort(key=lambda row: (row["date_start"], row["fold_id"]))

    if not bool(np.isfinite(output_weights).all()):
        raise RuntimeError("built daily guidance contains non-finite values")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".parquet", dir=output_path.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        frame = pl.DataFrame(
            {
                "date": minute_dates,
                **{
                    symbol: output_weights[:, output_symbol_lookup[symbol]]
                    for symbol in minute_symbols
                },
            }
        )
        frame.write_parquet(
            temporary,
            compression="zstd",
            compression_level=7,
            statistics=True,
        )
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)

    gross = np.abs(output_weights.astype(np.float64)).sum(axis=1)
    manifest = {
        "schema_version": 2,
        "contract": MINUTE_DAILY_GUIDANCE_CONTRACT,
        "source_execution_mode": "tw_day_trade",
        "source_tensor": "test_backtest.npz/requested_weights_history",
        "decision_clock": "observed_session_open",
        "minute_execution_clock": "completed_right_labelled_1m_bar_to_next_1m_open",
        "daily_sources": summary_receipts,
        "allow_zero_fill_missing": bool(allow_zero_fill_missing),
        "minute_manifest": str(minute_manifest_path.resolve()),
        "minute_manifest_sha256": _sha256(minute_manifest_path),
        "parquet": str(output_path.resolve()),
        "parquet_sha256": _sha256(output_path),
        "date_start": str(minute_dates[0]),
        "date_end": str(minute_dates[-1]),
        "date_count": int(minute_dates.size),
        "excluded_unusable_dates": list(excluded_unusable_dates),
        "symbol_count": len(minute_symbols),
        "gross_min": float(gross.min()),
        "gross_mean": float(gross.mean()),
        "gross_max": float(gross.max()),
        "owner_segments": owner_segments,
        "zero_filled_missing_date_count": len(zero_filled_dates),
        "zero_filled_missing_dates": zero_filled_dates[:100],
    }
    manifest_path = minute_daily_guidance_manifest_path(output_path)
    _atomic_json(manifest_path, manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--daily-output-dir",
        type=Path,
        action="append",
        required=True,
        help=(
            "Daily training output root; repeat to form a causal checkpoint "
            "catalog. The latest exact-date, pre-year train/validation "
            "cutoff wins."
        ),
    )
    parser.add_argument(
        "--minute-manifest",
        type=Path,
        default=Path("data_tw_minute/research_dataset/manifest.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data_tw_minute/daily_guidance/oof_requested_weights.parquet"),
    )
    parser.add_argument(
        "--allow-zero-fill-missing",
        action="store_true",
        help=(
            "Fail closed to zero for audited source-symbol/date gaps instead "
            "of rejecting the guide build."
        ),
    )
    args = parser.parse_args()
    manifest = build_guidance(
        daily_output_dirs=[value.resolve() for value in args.daily_output_dir],
        minute_manifest_path=args.minute_manifest.resolve(),
        output_path=args.output.resolve(),
        allow_zero_fill_missing=bool(args.allow_zero_fill_missing),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
