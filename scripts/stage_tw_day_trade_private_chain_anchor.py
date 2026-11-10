#!/usr/bin/env python3
"""Stage an auditable *private* next-day anchor from a flat paper candidate.

This projection copies source evidence into a new E workspace and never
modifies the source, produces a broker order, or authorizes publication.
By default only fully observed prior marks are accepted. An explicit private
diagnostic option can carry disclosed unknown gaps, never publication authority.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import shutil
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.download_shioaji_tw_minute_kbars import minute_receipt_valid
from scripts import audit_tw_day_trade_flat_snapshot_anchor as snapshot_audit
from scripts import audit_tw_day_trade_historical_anchor as historical_anchor
from scripts import audit_tw_day_trade_provisional_source as provisional_audit
from scripts import repair_tw_day_trade_flat_opening as owner
from scripts.plan_tw_day_trade_historical_revisions import _write_new_plan
from scripts.prepare_finmind_tw_day_trade_repair_source import verify as verify_finmind_source
from scripts.rebuild_tw_day_trade_minute_curves import _atomic_jsonl
from scripts.verify_tw_day_trade_scoped_repair import verify_minute_source_coverage
from stockagent.data_sync import workspace_volume
from stockagent.live.paper_journal import open_paper_journal


class UnprovenZeroTradeSource(RuntimeError):
    """No receipt-backed zero row is available; this is not a source conflict."""


def _check_private_receipt(receipt: dict, *, allow_observed_zero_proof: bool = False) -> None:
    coverage = receipt.get("minute_source_coverage") or {}
    if (receipt.get("contract") != "private_historical_progressive_candidate_v1"
            or receipt.get("status") != "private_candidate_verified_not_applied"
            or receipt.get("simulation_only") is not True or receipt.get("broker_orders_created") != 0
            or receipt.get("live_ledger_modified") is not False or receipt.get("publication_authorized") is not False
            or receipt.get("subsequent_sessions_replayed") is not False
            or coverage.get("minute_points") != 270 or coverage.get("duplicate_minute_points") != 0
            or not isinstance(coverage.get("unknown_carried_minute_symbols"), dict)
            or coverage.get("unsourced_held_price_minutes") != len(coverage["unknown_carried_minute_symbols"])
            or (coverage["unknown_carried_minute_symbols"] and not allow_observed_zero_proof)):
        raise ValueError("a private candidate with fully proved held-price coverage is required")


def _verified_zero_trade_sources(receipt: dict, *, day: date) -> tuple[list[dict], list[dict]]:
    """Resolve missing bars only from receipt-pinned provider zero rows.

    Positive-minute OHLC/share-volume must match the already pinned FinMind
    projection exactly. This is provider-observed no trade, not proof that a
    quote was executable, a complete exchange tape or a broker fill.
    """
    unknown = receipt["minute_source_coverage"]["unknown_carried_minute_symbols"]
    if not unknown:
        return [], []
    if any(not isinstance(minute, str) or not minute.startswith(str(day) + "T")
           or not isinstance(symbols, list) or not symbols
           or any(not isinstance(s, str) or not s or Path(s).name != s for s in symbols)
           for minute, symbols in unknown.items()):
        raise ValueError("invalid unknown-minute source scope")
    manifests = {Path(item["path"]).resolve() for item in receipt["source_pins"]
                 if Path(item["path"]).name == "finmind_replay_source.json"}
    if len(manifests) != 1:
        raise RuntimeError("one exact FinMind minute projection is required")
    verified = verify_finmind_source(next(iter(manifests)).parent)
    if verified.get("session_date") != str(day):
        raise RuntimeError("FinMind projection has a different session")
    finmind = pl.read_parquet(verified["output"]["path"])
    symbols = sorted({symbol for items in unknown.values() for symbol in items})
    files, pins = [], []
    columns = ["ts", "Open", "High", "Low", "Close", "volume_shares"]
    for symbol in symbols:
        candidates = set()
        for root in owner.DEFAULT_MINUTE_DATA_ROOTS:
            directory = ROOT / root / "minute_chunks" / symbol
            for path in sorted(directory.glob("*.parquet")):
                try:
                    left, right = path.stem.split("_")
                    start, end = date.fromisoformat(left), date.fromisoformat(right)
                except ValueError:
                    continue
                if start <= day <= end and path.with_suffix(".receipt.json").is_file():
                    candidates.add((path.resolve(strict=True), start, end))
        if not candidates:
            raise UnprovenZeroTradeSource(f"no receipt-backed Shioaji minute chunk: {symbol}")
        comparison = finmind.filter(pl.col("symbol") == symbol).select(columns).sort("ts")
        confirmed = False
        valid_but_incomplete = False
        for raw, start, end in sorted(candidates):
            native_receipt = raw.with_suffix(".receipt.json")
            before = {str(path): owner._sha256(path) for path in (raw, native_receipt)}
            native = owner._object(native_receipt)
            if (native.get("underlying_data_method") != "provider_kbars"
                    or native.get("status") != "ok" or str(day) not in native.get("returned_dates", [])
                    or Path(str((native.get("output_receipt") or {}).get("path") or "")).resolve() != raw
                    or not minute_receipt_valid(native_receipt, symbol=symbol, start=start, end=end,
                                                required_dates={day})):
                continue
            frame = pl.read_parquet(raw)
            if "ts" not in frame.columns:
                raise RuntimeError(f"invalid Shioaji minute identity: {symbol}")
            frame = frame.filter(pl.col("ts").dt.date() == day)
            if (not {"symbol", "ts", "Volume", "Amount", "volume_shares", *columns} <= set(frame.columns)
                    or frame.filter(pl.col("symbol") != symbol).height
                    or frame.select(pl.col("ts").is_duplicated().any()).item()):
                raise RuntimeError(f"invalid Shioaji minute identity: {symbol}")
            positive = frame.filter(pl.col("volume_shares") > 0).select(columns).sort("ts")
            if not positive.equals(comparison):
                raise RuntimeError(f"provider positive-minute data differ: {symbol}")
            zero = {row["ts"].isoformat()[:16] for row in frame.filter(
                (pl.col("volume_shares") == 0) & (pl.col("Volume") == 0) & (pl.col("Amount") == 0)
            ).select("ts").to_dicts()}
            if any(owner._sha256(Path(path)) != digest for path, digest in before.items()):
                raise RuntimeError(f"Shioaji minute source changed: {symbol}")
            pins.extend({"path": path, "sha256": digest} for path, digest in before.items())
            if any(minute not in zero for minute, values in unknown.items() if symbol in values):
                valid_but_incomplete = True
                continue
            files.append({"path": str(raw), "sha256": before[str(raw)]})
            confirmed = True
        if not confirmed:
            if valid_but_incomplete:
                raise UnprovenZeroTradeSource(f"missing explicit zero-volume minute: {symbol}")
            raise UnprovenZeroTradeSource(f"no valid provider-backed Shioaji receipt: {symbol}")
    return files, pins


def _jsonl(path: Path) -> list[dict]:
    with open_paper_journal(path) as stream:
        rows = []
        for line in stream:
            if not line.endswith(b"\n"):
                raise RuntimeError(f"incomplete candidate journal: {path}")
            rows.append(json.loads(line))
        return rows


def _provisional_coverage(receipt: dict, *, day: date, mode: dict, marks: list[dict]) -> dict:
    """Reproduce disclosed gaps from the exact original projection, not zero-fill."""
    manifests = {Path(item["path"]).resolve() for item in receipt["source_pins"]
                 if Path(item["path"]).name == "finmind_replay_source.json"}
    if len(manifests) != 1:
        raise RuntimeError("one pinned FinMind source is required for provisional diagnostics")
    verified = verify_finmind_source(next(iter(manifests)).parent)
    if verified.get("session_date") != str(day):
        raise RuntimeError("provisional minute source belongs to another session")
    symbols = {str(p["symbol"]) for p in mode["positions"].values() if p.get("session_date") == str(day)}
    bars, bar_proof = owner._minute_bar_rows((next(iter(manifests)).parent,), trading_date=day, symbols=symbols)
    pins = {(str(Path(item["path"]).resolve()), item["sha256"]) for item in receipt["source_pins"]}
    if any((str(Path(item["path"]).resolve()), item["sha256"]) not in pins
           for item in bar_proof.get("source_files", [])):
        raise RuntimeError("provisional minute source was not pinned by the candidate")
    if set(bars) != symbols:
        raise RuntimeError("provisional positions lack their original observed minute source")
    coverage = verify_minute_source_coverage(mode, marks, market=receipt["market"], day=str(day),
        source_files=[], allow_unverified_gaps=True, bars=bars)
    if coverage != receipt["minute_source_coverage"]:
        raise RuntimeError("provisional minute gaps differ from the candidate receipt")
    provisional_audit.audit_marks(mode, marks, bars, day=str(day))
    return coverage


def _inherited_gaps(prior: dict, *, allowed: bool) -> list[dict]:
    gaps = prior.get("inherited_unverified_minute_gaps") or []
    if not isinstance(gaps, list):
        raise ValueError("invalid inherited private data-gap disclosure")
    if gaps and not allowed:
        raise RuntimeError("inherited unverified gaps require explicit private diagnostic mode")
    return gaps


def _stage_coverage(receipt: dict, *, day: date, mode: dict, marks: list[dict],
                    allow_unverified_private_gaps: bool) -> tuple[dict, list[dict], bool]:
    """Prefer verified zeros; source conflicts always fail instead of downgrading."""
    try:
        sources, pins = _verified_zero_trade_sources(receipt, day=day)
    except UnprovenZeroTradeSource:
        if not allow_unverified_private_gaps:
            raise
        return _provisional_coverage(receipt, day=day, mode=mode, marks=marks), [], True
    coverage = verify_minute_source_coverage(mode, marks, market=receipt["market"], day=str(day),
                                              source_files=sources)
    return coverage, pins, False


def stage(*, candidate_receipt: Path, target_day: date, output_root: Path,
          allow_unverified_private_gaps: bool = False) -> dict:
    if type(allow_unverified_private_gaps) is not bool:
        raise ValueError("private diagnostic option must be an explicit boolean")
    workspace = workspace_volume.configuration()
    root = output_root.resolve()
    build_root = Path(workspace["mount_point"]) / "build"
    if not root.is_relative_to(build_root) or root == build_root or root.exists():
        raise ValueError("a new isolated E build directory is required")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    receipt_sha = owner._sha256(candidate_receipt)
    receipt = owner._object(candidate_receipt)
    _check_private_receipt(receipt, allow_observed_zero_proof=True)
    market, day = receipt["market"], date.fromisoformat(receipt["session_date"])
    candidate = Path(receipt["candidate_path"]).resolve(strict=True)
    if candidate.parent != candidate_receipt.parent or not candidate.is_relative_to(build_root):
        raise ValueError("candidate must belong to its exact private receipt")
    expected = receipt.get("candidate_file_sha256") or {}
    actual_names = {path.name for path in candidate.iterdir() if path.is_file()}
    if set(expected) != actual_names or not {"state.json", "marks.jsonl", "fills.jsonl", "events.jsonl"} <= actual_names:
        raise RuntimeError("private candidate file set changed")
    for name, digest in expected.items():
        if owner._sha256(candidate / name) != digest:
            raise RuntimeError(f"private candidate file changed: {name}")
    for name, digest in receipt.get("implementation_sha256", {}).items():
        if owner._sha256(Path(name)) != digest:
            raise RuntimeError("private candidate implementation changed")
    owner._assert_source_pins(receipt["source_pins"])
    state = owner._object(candidate / "state.json")
    mode = state.get("modes", {}).get(market)
    if (not isinstance(mode, dict) or mode.get("session_date") != str(day)
            or type(mode.get("open_position_count")) is not int or mode["open_position_count"] != 0):
        raise RuntimeError("private candidate did not end flat")
    marks = _jsonl(candidate / "marks.jsonl")
    coverage, extra_source_pins, unverified = _stage_coverage(receipt, day=day, mode=mode, marks=marks,
        allow_unverified_private_gaps=allow_unverified_private_gaps)
    archive = candidate / "position_history" / str(day) / f"{market}.json"
    archive_exists = archive.is_file()
    archive_data = (owner._object(archive) if archive_exists else {
        "market": market, "session_date": str(day), "signal_id": mode.get("signal_id"),
        "config_fingerprint": mode.get("config_fingerprint"),
        "checkpoint_fingerprint": mode.get("checkpoint_fingerprint"),
        "simulation_only": True, "production_order_possible": False,
        "positions": list(mode.get("positions", {}).values()),
        "private_projection_from_candidate_state": True,
    })
    if (archive_data.get("market") != market or archive_data.get("session_date") != str(day)
            or archive_data.get("signal_id") != mode.get("signal_id")
            or archive_data.get("simulation_only") is not True
            or archive_data.get("production_order_possible") is not False
            or historical_anchor._positions(archive_data.get("positions")) !=
               historical_anchor._positions(list(mode.get("positions", {}).values()))):
        raise RuntimeError("private position archive differs from candidate state")
    archive_sha = owner._sha256(archive) if archive_exists else None
    prior_receipt = Path(receipt["anchor_receipt"]).resolve(strict=True)
    if owner._sha256(prior_receipt) != receipt["anchor_receipt_sha256"]:
        raise RuntimeError("prior anchor receipt changed")
    prior = owner._object(prior_receipt)
    inherited_gaps = _inherited_gaps(prior, allowed=allow_unverified_private_gaps)
    if unverified:
        inherited_gaps = [*inherited_gaps, {"session_date": str(day),
            "candidate_receipt": str(candidate_receipt), "candidate_receipt_sha256": receipt_sha,
            "unknown_carried_minute_symbols": coverage["unknown_carried_minute_symbols"],
            "missing_is_not_zero_and_fills_are_not_invented": True}]
    previous_day = date.fromisoformat(prior["anchor_session"])
    source_mark = prior["source_pins"]["marks"]
    source_marks = Path(source_mark["path"]).resolve(strict=True)
    if owner._sha256(source_marks) != source_mark["sha256"]:
        raise RuntimeError("prior closing mark source changed")
    previous_mark = historical_anchor._close_mark(source_marks, day=str(previous_day), market=market)
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    staged_ledger = root / "private_ledger_projection"
    staged_archive = staged_ledger / "position_history" / str(day) / f"{market}.json"
    staged_archive.parent.mkdir(mode=0o700, parents=True)
    if archive_exists:
        shutil.copyfile(archive, staged_archive)
    else:
        owner._atomic_json(staged_archive, archive_data)
    for name in ("events.jsonl", "fills.jsonl"):
        shutil.copyfile(candidate / name, staged_ledger / name)
    _atomic_jsonl(staged_ledger / "marks.jsonl", [previous_mark, *marks])
    snapshot = root / "private_state_projection.json"
    owner._atomic_json(snapshot, {**state, "simulation_only": True, "production_order_possible": False,
        "private_chain_projection": {"candidate_receipt": str(candidate_receipt),
                                     "candidate_receipt_sha256": receipt_sha,
                                     "inherited_unverified_minute_gaps": inherited_gaps,
                                     "publication_authorized": False}})
    proof = snapshot_audit.audit(snapshot=snapshot, live_dir=staged_ledger, markets=[market],
        previous_day=previous_day, anchor_day=day, target_day=target_day, calendar_root=ROOT / "data_tw_public")
    owner._assert_source_pins(extra_source_pins)
    if (owner._sha256(candidate_receipt) != receipt_sha or owner._sha256(prior_receipt) != receipt["anchor_receipt_sha256"]
            or owner._sha256(source_marks) != source_mark["sha256"]
            or (archive_exists and owner._sha256(archive) != archive_sha)
            or any(owner._sha256(candidate / name) != digest for name, digest in expected.items())):
        raise RuntimeError("source candidate or original closing mark changed during staging")
    proof.update({"private_chain_projection": True, "private_candidate_receipt": str(candidate_receipt),
                  "private_candidate_receipt_sha256": receipt_sha,
                  "previous_original_marks_sha256": source_mark["sha256"],
                  "source_candidate_position_archive_sha256": archive_sha,
                  "position_archive_basis": "source_candidate_archive" if archive_exists
                    else "derived_private_projection_from_pinned_candidate_state",
                  "strict_minute_coverage": None if unverified else coverage,
                  "provisional_minute_coverage": coverage if unverified else None,
                  "inherited_unverified_minute_gaps": inherited_gaps,
                  "observed_zero_trade_source_pins": extra_source_pins,
                  "zero_trade_source_basis": (None if unverified else
                    "shioaji_provider_zero_volume_and_amount_cross_checked_with_finmind_positive_bars"),
                  "zero_trade_is_not_an_execution_quote": True, "publication_authorized": False})
    proof["private_staging_implementation_sha256"] = owner._sha256(Path(__file__).resolve())
    if unverified:
        proof["provisional_auditor_sha256"] = owner._sha256(Path(provisional_audit.__file__).resolve())
    _write_new_plan(root / "snapshot_anchor.json", proof)
    return {"status": "private_chain_anchor_audited_not_applied", "market": market,
            "anchor_session": str(day), "target_session": str(target_day), "root": str(root),
            "minute_points": coverage["minute_points"], "live_ledger_modified": False,
            "inherited_unverified_session_count": len(inherited_gaps),
            "production_order_possible": False, "publication_authorized": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-receipt", type=Path, required=True)
    parser.add_argument("--target-day", type=date.fromisoformat, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--allow-unverified-private-gaps", action="store_true",
                        help="stage a disclosed, unpublished diagnostic with unknown minute gaps")
    args = parser.parse_args()
    print(json.dumps(stage(candidate_receipt=args.candidate_receipt.resolve(strict=True),
                           target_day=args.target_day, output_root=args.output_root,
                           allow_unverified_private_gaps=args.allow_unverified_private_gaps), ensure_ascii=False))


if __name__ == "__main__":
    main()
