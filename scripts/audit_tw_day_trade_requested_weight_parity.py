#!/usr/bin/env python3
"""Compare a private daily signal candidate with an original research artifact.

Compare both the model output and the executor-preview target with the saved
research request. The latter is prepared by the research backtest, so neither
comparison alone proves that all model inputs or preparation stages match.
This is not an execution, point-in-time, or publication authorization audit.
Candidate paths and hashes come from the isolated backfill receipt.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any

import numpy as np
import polars as pl


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def audit(receipt_path: Path, research_path: Path, *, epsilon: float = 1e-10) -> dict[str, Any]:
    if not math.isfinite(epsilon) or epsilon < 0:
        raise ValueError("epsilon must be finite and nonnegative")
    receipt_path, research_path = receipt_path.resolve(strict=True), research_path.resolve(strict=True)
    receipt_raw = receipt_path.read_bytes()
    receipt_hash = hashlib.sha256(receipt_raw).hexdigest()
    receipt = json.loads(receipt_raw)
    if receipt.get("status") != "source_hashes_unchanged_candidate_not_yet_accepted":
        raise ValueError("candidate receipt is incomplete or not an isolated source-bound backfill")
    sessions = receipt.get("sessions")
    if not isinstance(sessions, list) or not sessions:
        raise ValueError("candidate receipt has no sessions")
    by_date: dict[str, dict[str, Any]] = {}
    for row in sessions:
        if not isinstance(row, dict) or not isinstance(row.get("session_date"), str):
            raise ValueError("invalid candidate session")
        day = row["session_date"]
        if day in by_date:
            raise ValueError(f"duplicate candidate session: {day}")
        by_date[day] = row

    research_hash = _sha256(research_path)
    with np.load(research_path, allow_pickle=False) as artifact:
        dates = artifact["dates"]
        universe = artifact["carry_universe"]
        weights = artifact["requested_weights_history"]
        if (dates.ndim != 1 or universe.ndim != 1 or weights.ndim != 2
                or weights.shape != (len(dates), len(universe))
                or dates.dtype.kind != "M" or universe.dtype.kind not in "US"
                or weights.dtype.kind not in "fi" or not np.isfinite(weights).all()):
            raise ValueError("invalid original research target axes or values")
        date_keys = dates.astype("datetime64[D]").astype(str).tolist()
        symbols = universe.astype(str).tolist()
        if ("NaT" in date_keys or len(set(date_keys)) != len(date_keys)
                or any(not s for s in symbols) or len(set(symbols)) != len(symbols)):
            raise ValueError("duplicate or invalid research dates/symbols")
        weights = np.asarray(weights, dtype=np.float64).copy()
    if _sha256(research_path) != research_hash:
        raise ValueError("original research artifact changed during read")
    symbol_index = {symbol: i for i, symbol in enumerate(symbols)}
    details = []
    for index, day in enumerate(date_keys):
        if day not in by_date:
            continue
        session = by_date[day]
        summary_path = Path(session["summary_path"]).resolve(strict=True)
        weights_path = Path(session["weights_path"]).resolve(strict=True)
        if weights_path != summary_path.with_name("target_weights.parquet"):
            raise ValueError(f"candidate weights are outside their signal directory: {day}")
        summary_raw = summary_path.read_bytes()
        if hashlib.sha256(summary_raw).hexdigest() != session.get("summary_sha256"):
            raise ValueError(f"candidate summary hash changed: {day}")
        summary = json.loads(summary_raw)
        if (summary.get("signal_id") != session.get("signal_id")
                or summary.get("counterfactual_signal_regeneration") is not True
                or summary.get("simulation_only") is not True
                or summary.get("production_order_possible") is not False):
            raise ValueError(f"candidate signal identity or replay boundary invalid: {day}")
        expected = (summary.get("counterfactual_artifact_proofs") or {}).get("weights_sha256")
        if not isinstance(expected, str) or _sha256(weights_path) != expected:
            raise ValueError(f"candidate weight hash changed: {day}")
        frame = pl.read_parquet(weights_path, columns=["symbol", "model_weight", "target_weight"])
        if not frame.height:
            raise ValueError(f"candidate has no target rows: {day}")
        candidates = {key: np.zeros(len(symbols), dtype=np.float64)
                      for key in ("model_weight", "target_weight")}
        seen: set[str] = set()
        extras: dict[str, dict[str, float]] = {key: {} for key in candidates}
        for symbol, model_value, target_value in frame.iter_rows():
            if not isinstance(symbol, str) or not symbol or symbol in seen:
                raise ValueError(f"duplicate or invalid candidate symbol: {day}")
            seen.add(symbol)
            for key, value in (("model_weight", model_value), ("target_weight", target_value)):
                if value is None or not math.isfinite(float(value)):
                    raise ValueError(f"invalid candidate {key}: {day}/{symbol}")
                value = float(value)
                if symbol in symbol_index:
                    candidates[key][symbol_index[symbol]] = value
                elif abs(value) > epsilon:
                    extras[key][symbol] = value
        if _sha256(weights_path) != expected or hashlib.sha256(summary_path.read_bytes()).hexdigest() != session["summary_sha256"]:
            raise ValueError(f"candidate changed during read: {day}")
        original = weights[index]
        comparison = {}
        for key, candidate in candidates.items():
            delta = np.abs(candidate - original)
            comparison[key] = {
                "l1_weight_difference": float(delta.sum() + sum(abs(v) for v in extras[key].values())),
                "max_absolute_weight_difference": max(float(delta.max(initial=0)), max((abs(v) for v in extras[key].values()), default=0.0)),
                "nonzero_membership_differences": int(np.count_nonzero((np.abs(candidate) > epsilon) != (np.abs(original) > epsilon))) + len(extras[key]),
                "candidate_nonzero_outside_original_universe": sorted(extras[key]),
            }
        details.append({"session_date": day, "comparisons": comparison})
    if not details:
        raise ValueError("candidate and original research have no common sessions")
    if (hashlib.sha256(receipt_path.read_bytes()).hexdigest() != receipt_hash
            or _sha256(research_path) != research_hash):
        raise ValueError("candidate receipt or original research changed during comparison")
    return {
        "contract": "private_counterfactual_requested_weight_parity_v2",
        "receipt_path": str(receipt_path), "receipt_sha256": receipt_hash,
        "original_research_path": str(research_path), "original_research_sha256": research_hash,
        "epsilon": epsilon, "compared_sessions": len(details),
        "original_sessions_without_candidate": sorted(set(date_keys) - set(by_date)),
        "candidate_sessions_without_original": sorted(set(by_date) - set(date_keys)),
        "comparison_semantics": {
            "original": "research requested_weights_history after research input preparation",
            "model_weight": "candidate model output before execution preview constraints",
            "target_weight": "candidate execution preview target after known constraints",
        },
        "comparisons": {key: {
            "sessions_with_numeric_differences": sum(row["comparisons"][key]["max_absolute_weight_difference"] > epsilon for row in details),
            "sessions_with_membership_differences": sum(row["comparisons"][key]["nonzero_membership_differences"] > 0 for row in details),
            "max_absolute_weight_difference": max(row["comparisons"][key]["max_absolute_weight_difference"] for row in details),
            "median_l1_weight_difference": float(np.median([row["comparisons"][key]["l1_weight_difference"] for row in details])),
        } for key in ("model_weight", "target_weight")},
        "sessions": details,
        "historical_point_in_time_proven": False, "historical_revision_sequence_proven": False,
        "execution_or_performance_equivalence_proven": False, "publication_authorized": False,
    }


def _write_new_json(path: Path, payload: dict[str, Any]) -> None:
    """Publish one complete private receipt without replacing an existing file."""
    path = path.absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)  # Atomic and fails if an earlier receipt exists.
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--research-artifact", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--epsilon", type=float, default=1e-10)
    args = parser.parse_args()
    result = audit(args.receipt, args.research_artifact, epsilon=args.epsilon)
    _write_new_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k != "sessions"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
