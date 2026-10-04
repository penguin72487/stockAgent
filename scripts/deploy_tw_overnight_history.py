#!/usr/bin/env python3
"""Publish audited overnight counterfactual history beside the live ledger.

The live paper engine remains authoritative for real-time signal and auction
evidence.  This deployment adds immutable, explicitly counterfactual history
tables that the same public API merges read-only.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import uuid
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.live.tw_day_trade_simulation import TAIPEI


SIGNAL_COLUMNS = (
    "action",
    "ask",
    "bid",
    "execution_price",
    "exchange_quote_at",
    "filled_shares",
    "lower_limit",
    "market",
    "model_trained_for_overnight",
    "name",
    "order_limit_price",
    "quote_at",
    "raw_score",
    "reason",
    "requested_shares",
    "score",
    "session_date",
    "side",
    "signal_at",
    "signal_id",
    "signal_source_path",
    "simtrade",
    "simulation_replay",
    "sizing_capital_twd",
    "sizing_open_price",
    "sizing_price_at_13_25",
    "source_signal_at",
    "status",
    "symbol",
    "target_weight",
    "temporary_day_trade_model_adapter",
    "upper_limit",
)

EVENT_COLUMNS = (
    "recorded_at",
    "fill_at",
    "quote_at",
    "session_date",
    "market",
    "symbol",
    "side",
    "purpose",
    "order_type",
    "price",
    "quantity",
    "requested_quantity",
    "remaining_quantity",
    "filled_quantity",
    "unfilled_quantity",
    "status",
    "gross_pnl_twd",
    "net_pnl_twd",
    "fee_and_tax_twd",
    "gross_fee_and_tax_twd",
    "commission_rebate_accrued_twd",
    "simulation_only",
    "fill_contract",
    "depth_assumption",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_parquet(frame: pl.DataFrame, target: Path) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        digest = _sha256(temporary)
        os.replace(temporary, target)
        return digest
    finally:
        temporary.unlink(missing_ok=True)


def _selected_frame(path: Path, columns: tuple[str, ...]) -> pl.DataFrame:
    schema = pl.scan_ndjson(path).collect_schema()
    available = set(schema.names())
    missing = {"session_date", "market", "symbol"} - available
    if missing:
        raise ValueError(f"historical ledger {path} misses {sorted(missing)}")
    selected = [name for name in columns if name in available]
    return pl.scan_ndjson(path).select(selected).collect()


def _event_frame(ledger: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for filename, kind in (("orders.jsonl", "order"), ("fills.jsonl", "fill")):
        path = ledger / filename
        if not path.is_file() or path.stat().st_size == 0:
            continue
        frame = _selected_frame(path, EVENT_COLUMNS).with_columns(
            pl.lit(kind).alias("event_kind"),
            pl.lit(True).alias("simulation_replay"),
        )
        frames.append(frame)
    if not frames:
        return pl.DataFrame(
            schema={
                "session_date": pl.String,
                "market": pl.String,
                "symbol": pl.String,
                "event_kind": pl.String,
                "simulation_replay": pl.Boolean,
            }
        )
    return pl.concat(frames, how="diagonal_relaxed")


def _write_position_release(
    *, ledger: Path, state_dir: Path, release_id: str
) -> tuple[Path, int]:
    releases = state_dir / "overnight_position_history_releases"
    candidate = releases / release_id
    if candidate.exists():
        raise FileExistsError(candidate)
    candidate.mkdir(parents=True)
    count = 0

    def write_payload(payload: dict[str, Any]) -> None:
        nonlocal count
        day = str(payload.get("session_date") or "")[:10]
        market = str(payload.get("market") or "")
        if not day or not market:
            raise ValueError("historical position snapshot lacks identity")
        positions = []
        for raw in payload.get("positions") or ():
            if not isinstance(raw, dict):
                continue
            positions.append({**raw, "counterfactual_overnight_replay": True})
        output = {
            **payload,
            "positions": positions,
            "counterfactual_overnight_replay": True,
            "production_order_possible": False,
            "simulation_only": True,
        }
        destination = candidate / day / f"{market}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(destination, output)
        count += len(positions)

    for path in sorted((ledger / "position_history").glob("*/*.json")):
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            raise TypeError(f"invalid position snapshot: {path}")
        write_payload(payload)

    state = json.loads((ledger / "state.json").read_text())
    for market, mode in (state.get("modes") or {}).items():
        if not isinstance(mode, dict) or not mode.get("session_date"):
            continue
        write_payload(
            {
                "schema_version": 1,
                "session_date": str(mode["session_date"])[:10],
                "market": str(market),
                "signal_id": mode.get("signal_id"),
                "archived_at": state.get("updated_at"),
                "positions": list((mode.get("positions") or {}).values()),
            }
        )

    link = state_dir / "overnight_position_history"
    temporary_link = state_dir / f".overnight_position_history.{uuid.uuid4().hex}.tmp"
    relative = candidate.relative_to(state_dir)
    os.symlink(relative, temporary_link)
    os.replace(temporary_link, link)
    return candidate, count


def deploy(source: Path, state_dir: Path) -> dict[str, Any]:
    source = source.resolve()
    state_dir = state_dir.resolve()
    result_path = source / "result.json"
    plan_path = source / "plan.json"
    result = json.loads(result_path.read_text())
    plan = json.loads(plan_path.read_text())
    if not result.get("computation_complete") or not result.get("dashboard_history_ready"):
        raise ValueError("overnight history did not pass dashboard acceptance")
    if result.get("start_date") != "2026-02-25" or result.get("end_date") != plan.get("end_date"):
        raise ValueError("overnight history range is not the requested immutable plan")
    for name, expected in (result.get("outputs") or {}).items():
        path = source / name
        if not path.is_file() or _sha256(path) != expected:
            raise ValueError(f"historical output hash mismatch: {name}")
    ledger = (source / str(result.get("ledger_relative_path") or "")).resolve()
    if not ledger.is_relative_to(source) or not (ledger / "state.json").is_file():
        raise ValueError("historical replay ledger escapes source release")

    current_path = state_dir / "overnight_history.json"
    if current_path.is_file():
        current = json.loads(current_path.read_text())
        if str(current.get("end_date") or "") > str(result["end_date"]):
            raise ValueError("refusing overnight history date regression")

    marks_frame = pl.read_parquet(source / "equity_history.parquet").sort(
        ["minute", "market"]
    )
    expected_marks = int(result["sessions"]) * len(result["markets"]) * 2
    if marks_frame.height != expected_marks:
        raise ValueError("overnight auction-event coverage is incomplete")
    marks = []
    for raw in marks_frame.to_dicts():
        row = dict(raw)
        for key in ("minute",):
            value = row.get(key)
            if isinstance(value, datetime):
                row[key] = value.isoformat(timespec="minutes")
        row.update(
            {
                "recorded_at": row.get("minute"),
                "historical_counterfactual_replay": True,
                "historical_minute_replay": True,
                "valuation_executable": False,
                "valuation_source": "official_daily_open_close_counterfactual",
                "minute_valuation_contract": "overnight_auction_events_no_interpolation",
            }
        )
        marks.append(row)

    signal_frame = _selected_frame(ledger / "signals.jsonl", SIGNAL_COLUMNS).with_columns(
        pl.lit(True).alias("counterfactual_overnight_replay"),
        pl.lit(True).alias("simulation_replay"),
    )
    if signal_frame.select(pl.col("session_date").n_unique()).item() != int(result["sessions"]):
        raise ValueError("historical signal sessions are incomplete")
    event_frame = _event_frame(ledger)

    state_dir.mkdir(parents=True, exist_ok=True)
    signal_digest = _atomic_parquet(
        signal_frame, state_dir / "overnight_signal_history.parquet"
    )
    event_digest = _atomic_parquet(
        event_frame, state_dir / "overnight_event_history.parquet"
    )
    release_id = (
        f"{result['end_date'].replace('-', '')}-{_sha256(result_path)[:12]}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    position_release, position_count = _write_position_release(
        ledger=ledger,
        state_dir=state_dir,
        release_id=release_id,
    )
    stale_markets = sum(bool(row.get("valuation_stale")) for row in result["markets"])
    counts = result.get("decision_price_source_counts") or {}
    history = {
        "schema_version": 1,
        "product": "tw_overnight",
        "status": (
            "ready_with_stale_unresolved_position" if stale_markets else "ready"
        ),
        "generated_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "session_count": int(result["sessions"]),
        "market_count": len(result["markets"]),
        "mark_count": len(marks),
        "signal_count": signal_frame.height,
        "event_count": event_frame.height,
        "position_count": position_count,
        "missing_1325_count": int(counts.get("missing") or 0),
        "close_fallback_count": int(counts.get("same_session_close") or 0),
        "valuation_stale_market_count": stale_markets,
        "unresolved_prior_position_count": len(result.get("unresolved_positions") or ()),
        "blocked_close_signal_count": int(result.get("blocked_close_signal_count") or 0),
        "simulation_only": True,
        "production_order_possible": False,
        "counterfactual": True,
        "model_scope": plan.get("model_scope"),
        "history_lineage_fingerprint": (
            plan.get("history_lineage") or {}
        ).get("fingerprint_sha256"),
        "observation_contract": (
            "13:25 when source-backed; same-session close fallback when missing"
        ),
        "auction_contract": plan.get("auction_contract"),
        "funding_contract": result.get("funding_contract"),
        "marks": marks,
    }
    atomic_write_json(current_path, history)
    receipt = {
        "schema_version": 1,
        "status": "deployed",
        "deployed_at": history["generated_at"],
        "source": str(source),
        "source_result_sha256": _sha256(result_path),
        "source_plan_sha256": _sha256(plan_path),
        "history_lineage_fingerprint": (
            plan.get("history_lineage") or {}
        ).get("fingerprint_sha256"),
        "end_date": result["end_date"],
        "history_sha256": _sha256(current_path),
        "signal_history_sha256": signal_digest,
        "event_history_sha256": event_digest,
        "position_release": str(position_release.relative_to(state_dir)),
        "position_count": position_count,
        "simulation_only": True,
        "production_order_possible": False,
    }
    atomic_write_json(state_dir / "overnight_history_deployment.json", receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_overnight_simulation"),
    )
    args = parser.parse_args()
    print(json.dumps(deploy(args.source, args.state_dir), ensure_ascii=False))


if __name__ == "__main__":
    main()
