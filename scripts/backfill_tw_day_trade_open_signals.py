#!/usr/bin/env python3
"""Regenerate explicitly counterfactual TW day-trade open signals.

The generated signals are simulation-only.  They use only a completed prior
panel row plus the selected session's official open and pre-open price limits.
The real generation timestamp is preserved; ``replay_effective_signal_at`` is
separate evidence for the historical 09:00 paper replay.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
import json
import math
import os
from pathlib import Path
import shutil
import sys
import time as wall_time
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import polars as pl
import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.rebuild_tw_day_trade_open_price_replay import (  # noqa: E402
    DEFAULT_TPEX_DAILY_OHLCV_PATH,
    DEFAULT_TWSE_DAILY_OHLCV_PATH,
    _atomic_json,
    _load_price_limits,
    _official_aggregate_daily_rows,
    _sha256,
)
from stockagent.config import load_config  # noqa: E402
from stockagent.live.market_config import (  # noqa: E402
    load_market_config,
    resolved_live_output_dir,
)
from stockagent.live.signal_engine import _build_panel, _resolve_checkpoint, generate_live_signal  # noqa: E402
from stockagent.live.signal_engine import live_tw_public_source_read_guard  # noqa: E402
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")


def _with_source_wait(operation, *, seconds: float):
    deadline = wall_time.monotonic() + seconds
    while True:
        try:
            return operation()
        except TWPublicSourceUpdatePending:
            if wall_time.monotonic() >= deadline:
                raise
            print(json.dumps({"status": "waiting_source", "reason": "canonical_producer_holds_read_guard",
                              "model_executed": False}), flush=True)
            wall_time.sleep(min(2.0, max(0.0, deadline-wall_time.monotonic())))


def _snapshot_sources(root: Path, sources: dict[str, Path], *, references: dict[str, Path] | None = None) -> dict[str, Path]:
    """Retain a finite, byte-identical source set for a long isolated replay.

    Caller holds the canonical read guard. This is not a collector or a new
    data generation: producers may continue publishing after the copy.
    """
    root.mkdir(parents=True, exist_ok=True)
    receipt_path = root / "source_snapshot_receipt.json"
    references = references or {}
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
        if (receipt.get("contract") != "immutable_byte_identical_replay_inputs_v1"
                or receipt.get("status") != "verified"
                or set(receipt.get("sources") or {}) != set(sources)
                or set(receipt.get("references") or {}) != set(references)):
            raise ValueError("immutable replay source membership differs")
        retained = {}
        for name, source in sources.items():
            proof = receipt["sources"][name]
            target = Path(proof["retained"])
            if (proof["source"] != str(source.resolve()) or not target.resolve().is_relative_to(root.resolve())
                    or _sha256(target) != proof["sha256"] or target.stat().st_size != proof["bytes"]):
                raise ValueError(f"retained replay source differs: {target}")
            retained[name] = target
        for proof in receipt.get("references", {}).values():
            if _sha256(Path(proof["path"])) != proof["sha256"]:
                raise ValueError("immutable replay reference changed")
        return retained
    output, proofs = {}, {}
    for name, source in sources.items():
        digest = _sha256(source)
        target = root / f"{name}-{digest}.parquet"
        if not target.exists():
            pending = target.with_suffix(f".pending.{os.getpid()}")
            shutil.copyfile(source, pending)
            if _sha256(pending) != digest or _sha256(source) != digest:
                raise ValueError(f"source changed during immutable replay copy: {source}")
            with pending.open("rb") as stream:
                os.fsync(stream.fileno())
            os.replace(pending, target)
        if _sha256(target) != digest:
            raise ValueError(f"retained replay source differs: {target}")
        output[name] = target
        proofs[name] = {"source": str(source.resolve()), "retained": str(target.resolve()),
                        "sha256": digest, "bytes": target.stat().st_size}
    _atomic_json(receipt_path, {
        "contract": "immutable_byte_identical_replay_inputs_v1", "status": "verified",
        "captured_at": datetime.now(TAIPEI).isoformat(), "sources": proofs,
        "references": {name: {"path": str(path.resolve()), "sha256": _sha256(path)}
                       for name, path in references.items()},
        "source_publication_claim": False})
    return output


def _resume_summary(path: Path, *, identity: dict[str, str], signal_id: str,
                    provenance: dict[str, Any]) -> dict[str, Any] | None:
    """Reuse only complete, byte-bound counterfactual paper artifacts."""
    if not path.exists():
        return None
    summary = json.loads(path.read_text())
    weights = path.parent / "target_weights.parquet"
    proof = summary.get("counterfactual_artifact_proofs") or {}
    if (summary.get("signal_id") != signal_id
            or summary.get("counterfactual_signal_regeneration") is not True
            or summary.get("simulation_only") is not True
            or summary.get("production_order_possible") is not False
            or proof.get("recipe_identity") != identity
            or summary.get("counterfactual_open_provenance") != provenance
            or not weights.is_file()
            or proof.get("weights_sha256") != _sha256(weights)):
        raise ValueError(f"existing candidate cannot be resumed without exact artifact proof: {path}")
    return summary


def _previous_official_session_date(
    twse_path: Path,
    tpex_path: Path,
    trading_date: date,
) -> date:
    """Resolve a prior exchange session, never a weekend publication row."""

    session_frames = [
        pl.scan_parquet(path)
        # The retained venue archive contains one malformed legacy date.  It
        # is unrelated to the requested modern session and must not make the
        # entire official calendar unreadable.  Invalid values remain null
        # and therefore cannot become a previous-session candidate.
        .select(pl.col("date").cast(pl.Date, strict=False).alias("date"))
        .filter(pl.col("date").is_not_null())
        .filter(pl.col("date") < trading_date)
        .select(pl.col("date").max())
        .collect()
        for path in (twse_path, tpex_path)
    ]
    candidates = [frame.item() for frame in session_frames if frame.item() is not None]
    value = max(candidates) if candidates else None
    if value is None:
        raise ValueError(
            f"official venue data has no completed session before {trading_date}"
        )
    return value


def _open_input_rows(
    *,
    official_rows: Mapping[str, Mapping[str, Any]],
    limits: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    missing_limits = 0
    for symbol in sorted(official_rows):
        official = official_rows[symbol]
        try:
            opening = float(official.get("open"))
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(opening) and opening > 0.0):
            continue
        limit = limits.get(symbol)
        if limit is None:
            missing_limits += 1
            limit = {}
        rows.append(
            {
                "symbol": symbol,
                "price": opening,
                "open_price": opening,
                "upper_limit_price": limit.get("upper_limit_price"),
                "lower_limit_price": limit.get("lower_limit_price"),
                "reference_price": limit.get("reference_price"),
            }
        )
    return rows, missing_limits


def _write_open_input(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing empty official-open input: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    pl.DataFrame(rows).write_csv(temporary)
    temporary.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-config", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--live-output-dir", type=Path, help="Isolated candidate signal root; never overwrite accepted live artifacts.")
    parser.add_argument("--resume-verified", action="store_true",
                        help="Resume an isolated candidate only after full recipe/input/weights SHA validation.")
    parser.add_argument("--source-snapshot-dir", type=Path,
                        help="Retain byte-identical canonical aggregates/features for a long isolated replay; no producer pause.")
    parser.add_argument("--source-wait-seconds", type=float, default=0,
                        help="Bounded wait for the canonical producer before history inference; corruption is never retried.")
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="Verified candidate model root; requires --live-output-dir and never changes the market selector.",
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_counterfactual_open_inputs"),
    )
    parser.add_argument(
        "--price-limit-dir",
        type=Path,
        default=Path("artifacts/live/tw_price_limits"),
    )
    parser.add_argument(
        "--twse-daily-ohlcv-path",
        type=Path,
        default=DEFAULT_TWSE_DAILY_OHLCV_PATH,
    )
    parser.add_argument(
        "--tpex-daily-ohlcv-path",
        type=Path,
        default=DEFAULT_TPEX_DAILY_OHLCV_PATH,
    )
    parser.add_argument(
        "--live-tail-panel-rows",
        type=int,
        default=256,
        help=(
            "Build one reusable causal panel tail large enough to contain every "
            "requested prior-session row. This is a backfill-only override and "
            "does not change the deployment config."
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    if not 0 <= args.source_wait_seconds <= 1800:
        raise ValueError("source wait must be between zero and 1800 seconds")

    market_config_path = args.market_config.resolve()
    market_config = load_market_config(market_config_path)
    if args.resume_verified and args.live_output_dir is None:
        raise ValueError("--resume-verified requires an isolated --live-output-dir")
    if args.source_snapshot_dir and args.live_output_dir is None:
        raise ValueError("source snapshots require an isolated --live-output-dir")
    candidate_artifact_root = args.artifact_root.resolve() if args.artifact_root else None
    if candidate_artifact_root is not None:
        if args.live_output_dir is None:
            raise ValueError("--artifact-root requires an isolated --live-output-dir")
        candidate_live_root = args.live_output_dir.resolve()
        accepted_live_root = resolved_live_output_dir(market_config).resolve()
        if (
            candidate_live_root == accepted_live_root
            or candidate_live_root in accepted_live_root.parents
            or accepted_live_root in candidate_live_root.parents
        ):
            raise ValueError("candidate live output must not overlap the accepted live output")
        if market_config.fold_id is None:
            raise ValueError("--artifact-root requires a pinned market fold_id")
        checkpoint = candidate_artifact_root / f"fold_{int(market_config.fold_id):02d}" / "checkpoint_best.pt"
        if not checkpoint.is_file():
            raise FileNotFoundError(checkpoint)
    experiment = load_config(market_config.config_path)
    if str(experiment.trading.execution_mode) != "tw_day_trade":
        raise ValueError("market config must resolve to execution_mode=tw_day_trade")
    feature_path = Path(experiment.data.tw_public_feature_path).resolve()
    if not feature_path.is_file():
        raise FileNotFoundError(feature_path)
    twse_path = args.twse_daily_ohlcv_path.resolve()
    tpex_path = args.tpex_daily_ohlcv_path.resolve()
    for source_path in (twse_path, tpex_path):
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
    if args.source_snapshot_dir:
        def capture():
            with live_tw_public_source_read_guard(experiment.data.parquet_root):
                references = ({"factorized_manifest": Path(experiment.data.factorized_feature_manifest).resolve(strict=True)}
                              if experiment.data.factorized_feature_manifest else {})
                return _snapshot_sources(args.source_snapshot_dir.resolve(),
                    {"features": feature_path, "twse": twse_path, "tpex": tpex_path}, references=references)
        retained = _with_source_wait(capture, seconds=args.source_wait_seconds)
        if experiment.data.factorized_feature_manifest:
            # Immutable generation identity, never a moving current symlink.
            snapshot = json.loads((args.source_snapshot_dir / "source_snapshot_receipt.json").read_text())
            experiment.data.factorized_feature_manifest = snapshot["references"]["factorized_manifest"]["path"]
        feature_path, twse_path, tpex_path = retained["features"], retained["twse"], retained["tpex"]
        experiment.data.tw_public_feature_path = str(feature_path)
    requested_tail_rows = int(args.live_tail_panel_rows)
    if requested_tail_rows < int(experiment.training.lookback) + 1:
        raise ValueError(
            "--live-tail-panel-rows must exceed the configured model lookback"
        )
    # A historical batch must not rebuild the same 2,700-symbol panel once per
    # session.  Build one sufficiently long, point-in-time panel and let the
    # canonical signal generator select the requested prior date from it.  The
    # loaded deployment object is process-local; the YAML remains untouched.
    experiment.data.live_tail_panel_rows = max(
        int(experiment.data.live_tail_panel_rows or 0), requested_tail_rows
    )
    panel, panel_cache_hit, panel_cache_tier = _with_source_wait(
        lambda: _build_panel(experiment, live_tail=True), seconds=args.source_wait_seconds)

    input_root = args.input_root.resolve() / market_config.market
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "market": market_config.market,
        "simulation_only": True,
        "production_order_possible": False,
        "contract": (
            "prior completed panel plus official session open; actual generation "
            "time is never backdated"
        ),
        "panel": {
            "dates": int(panel.num_dates),
            "symbols": int(panel.num_symbols),
            "requested_live_tail_panel_rows": requested_tail_rows,
            "memory_cache_hit": bool(panel_cache_hit),
            "cache_tier": str(panel_cache_tier),
        },
        "market_config_path": str(market_config_path),
        "market_config_sha256": _sha256(market_config_path),
        "candidate_artifact_root": str(candidate_artifact_root) if candidate_artifact_root else None,
        "sessions": [],
        "skipped": [],
    }
    _, checkpoint_path = _resolve_checkpoint(
        candidate_artifact_root or market_config.output_dir or experiment.runner.output_dir,
        market_config.fold_id,
        (candidate_artifact_root / f"fold_{int(market_config.fold_id):02d}" / "checkpoint_best.pt"
         if candidate_artifact_root is not None else market_config.checkpoint_path),
    )
    recipe_identity = {"generator_sha256": _sha256(Path(__file__)),
                       "signal_engine_sha256": _sha256(REPO_ROOT / "stockagent/live/signal_engine.py"),
                       "model_implementation_sha256": _sha256(REPO_ROOT / "stockagent/models/financial_transformer.py"),
                       "market_config_sha256": _sha256(market_config_path),
                       "config_sha256": _sha256(Path(market_config.config_path)),
                       "checkpoint_sha256": _sha256(checkpoint_path)}
    if experiment.data.factorized_feature_manifest:
        recipe_identity["factorized_manifest_sha256"] = _sha256(Path(experiment.data.factorized_feature_manifest))
    source_hashes = {"feature_path_sha256": _sha256(feature_path),
                     "twse_daily_ohlcv_sha256": _sha256(twse_path),
                     "tpex_daily_ohlcv_sha256": _sha256(tpex_path)}
    receipt.update(recipe_identity=recipe_identity, source_hashes=source_hashes)
    if args.source_snapshot_dir:
        receipt["immutable_source_snapshot_receipt"] = str(args.source_snapshot_dir.resolve() / "source_snapshot_receipt.json")

    current = start
    while current <= end:
        if current.weekday() >= 5:
            receipt["skipped"].append(
                {"session_date": current.isoformat(), "reason": "weekend"}
            )
            current += timedelta(days=1)
            continue
        official_rows = _official_aggregate_daily_rows(
            twse_path, tpex_path, current
        )
        if not official_rows:
            receipt["skipped"].append(
                {
                    "session_date": current.isoformat(),
                    "reason": "no_official_session_rows",
                }
            )
            current += timedelta(days=1)
            continue
        limit_path = (args.price_limit_dir / f"{current.isoformat()}.parquet").resolve()
        if not limit_path.is_file():
            raise FileNotFoundError(limit_path)
        limits = _load_price_limits(limit_path)
        input_rows, missing_limit_rows = _open_input_rows(
            official_rows=official_rows,
            limits=limits,
        )
        input_path = input_root / current.isoformat() / "official_open.csv"
        _write_open_input(input_path, input_rows)
        input_sha256 = _sha256(input_path)
        previous_panel_date = _previous_official_session_date(
            twse_path, tpex_path, current
        )
        effective_at = datetime.combine(current, time(9, 0), tzinfo=TAIPEI)
        signal_id = (
            f"{market_config.market}-counterfactual-official-open-"
            f"{current.isoformat()}"
        )
        kwargs = market_config.signal_kwargs(
            live_output_dir=str(args.live_output_dir.resolve() if args.live_output_dir else resolved_live_output_dir(market_config)),
            panel_date=previous_panel_date.isoformat(),
            asof_date=effective_at.isoformat(timespec="seconds"),
            price_source="csv",
            prices_csv=str(input_path),
            signal_id=signal_id,
            ensure_previous_signal=False,
            previous_signal_backfill_limit=0,
            market_notice=(
                "此為歷史官方開盤價的反事實紙上訊號重建；不是當日 09:00 "
                "即時產出的訊號，也不可用於正式委託。"
            ),
            write=True,
        )
        if candidate_artifact_root is not None:
            fold_dir = candidate_artifact_root / f"fold_{int(market_config.fold_id):02d}"
            kwargs.update(
                output_dir=str(candidate_artifact_root),
                checkpoint_path=str(fold_dir / "checkpoint_best.pt"),
                weights_path=str(fold_dir / "daily_weights.parquet"),
            )
        provenance = {
            "schema_version": 1,
            "source": "official_daily_session_open",
            "input_path": str(input_path),
            "input_sha256": input_sha256,
            "input_rows": len(input_rows),
            "missing_price_limit_rows": missing_limit_rows,
            "price_limit_path": str(limit_path),
            "price_limit_sha256": _sha256(limit_path),
            "twse_daily_ohlcv_path": str(twse_path),
            "twse_daily_ohlcv_sha256": source_hashes["twse_daily_ohlcv_sha256"],
            "tpex_daily_ohlcv_path": str(tpex_path),
            "tpex_daily_ohlcv_sha256": source_hashes["tpex_daily_ohlcv_sha256"],
            "feature_path": str(feature_path),
            "feature_path_sha256": source_hashes["feature_path_sha256"],
            "feature_cutoff_date": previous_panel_date.isoformat(),
        }
        expected_dir = Path(kwargs["live_output_dir"]) / effective_at.isoformat(timespec="seconds") / signal_id
        summary_path = expected_dir / "summary.json"
        summary = (_resume_summary(summary_path, identity=recipe_identity, signal_id=signal_id,
                                   provenance=provenance) if args.resume_verified else None)
        resumed = summary is not None
        if summary is None:
            # WSL/WDDM needs driver headroom for a 4.84-GiB dense input plus
            # the original encoder's FP32 workspaces. Inactive cached blocks
            # are not financial/model state; release them between wide calls.
            if experiment.data.factorized_feature_manifest and torch.cuda.is_initialized():
                torch.cuda.synchronize()
                torch.cuda.empty_cache()
            kwargs["_panel_override"] = panel
            result = generate_live_signal(**kwargs)
            if not result.output_dir:
                raise RuntimeError(f"{current}: signal writer returned no output_dir")
            if not bool(result.summary.get("live_session_open_feature_applied")):
                raise RuntimeError(f"{current}: session-open feature was not applied")
            summary_path = Path(result.output_dir) / "summary.json"
            summary = dict(result.summary)
            summary["counterfactual_artifact_proofs"] = {
                "recipe_identity": recipe_identity,
                "weights_sha256": _sha256(summary_path.parent / "target_weights.parquet"),
            }
        summary.update(
            {
                "counterfactual_signal_regeneration": True,
                "simulation_only": True,
                "production_order_possible": False,
                "counterfactual_generated_at": summary.get("generated_at"),
                "replay_effective_signal_at": effective_at.isoformat(
                    timespec="seconds"
                ),
                "counterfactual_open_provenance": provenance,
                "counterfactual_lookahead_warning": (
                    "The official daily aggregate was retrieved after the session "
                    "and is used only to reconstruct the observed open. This artifact "
                    "does not prove the signal existed at 09:00."
                ),
            }
        )
        _atomic_json(summary_path, summary)
        receipt["sessions"].append(
            {
                "session_date": current.isoformat(),
                "previous_panel_date": previous_panel_date.isoformat(),
                "replay_effective_signal_at": effective_at.isoformat(
                    timespec="seconds"
                ),
                "actual_generated_at": summary.get("generated_at"),
                "signal_id": summary.get("signal_id"),
                "summary_path": str(summary_path),
                "summary_sha256": _sha256(summary_path),
                "weights_path": str(summary_path.parent / "target_weights.parquet"),
                "resumed_verified_artifact": resumed,
                "input_rows": len(input_rows),
                "missing_price_limit_rows": missing_limit_rows,
                "opening_price_available_count": summary.get(
                    "opening_price_available_count"
                ),
                "target_gross": summary.get("target_gross"),
            }
        )
        _atomic_json(input_root / "backfill_receipt.json", receipt)
        print(
            json.dumps(receipt["sessions"][-1], ensure_ascii=False, sort_keys=True),
            flush=True,
        )
        current += timedelta(days=1)

    if not receipt["sessions"]:
        raise RuntimeError("no trading sessions were regenerated")
    if source_hashes != {"feature_path_sha256": _sha256(feature_path),
                         "twse_daily_ohlcv_sha256": _sha256(twse_path),
                         "tpex_daily_ohlcv_sha256": _sha256(tpex_path)}:
        raise RuntimeError("source files changed during the backfill; candidate is not accepted")
    _atomic_json(input_root / "backfill_receipt.json", receipt)
    print(
        json.dumps(
            {
                "market": market_config.market,
                "sessions": len(receipt["sessions"]),
                "receipt": str(input_root / "backfill_receipt.json"),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
