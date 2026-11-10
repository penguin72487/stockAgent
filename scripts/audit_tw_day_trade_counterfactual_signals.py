#!/usr/bin/env python3
"""Audit an isolated historical signal batch; never promote or execute it."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import math
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.backfill_tw_day_trade_open_signals import (  # noqa: E402
    _open_input_rows, _resume_summary, _sha256,
)
from scripts.rebuild_tw_day_trade_open_price_replay import (  # noqa: E402
    _load_price_limits, _official_aggregate_daily_rows_batch,
)
from scripts.switch_tw_day_trade_strategy import _latest_completed_session  # noqa: E402
from stockagent.live.tw_day_trade_simulation import TAIPEI  # noqa: E402


def _inside(path: str, root: Path) -> Path:
    resolved = Path(path).resolve(strict=True)
    if not resolved.is_file() or not resolved.is_relative_to(root.resolve(strict=True)):
        raise ValueError(f"candidate artifact outside its isolated root: {resolved}")
    return resolved


def _same_number(actual: object, expected: object) -> bool:
    if actual is None or expected is None:
        return actual is None and expected is None
    try:
        a, b = float(actual), float(expected)
    except (TypeError, ValueError):
        return False
    return (math.isnan(a) and math.isnan(b)) or a == b


def audit(*, receipt_path: Path, signal_root: Path, input_root: Path,
          expected_sessions: list[date], limit_root: Path) -> dict:
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get("status") != "source_hashes_unchanged_candidate_not_yet_accepted"
            or receipt.get("simulation_only") is not True
            or receipt.get("production_order_possible") is not False
            or receipt.get("candidate_accepted") is not False):
        raise ValueError("candidate batch is unfinished or has an invalid paper contract")
    sessions = receipt.get("sessions") or []
    observed = [date.fromisoformat(row["session_date"]) for row in sessions]
    if observed != expected_sessions or len(observed) != len(set(observed)):
        raise ValueError("candidate sessions differ from the official completed calendar")
    if not sessions:
        raise ValueError("empty candidate batch")

    identity = receipt["recipe_identity"]
    source_hashes = receipt["source_hashes"]
    snapshot_path = Path(receipt["immutable_source_snapshot_receipt"]).resolve(strict=True)
    snapshot = json.loads(snapshot_path.read_text())
    if snapshot.get("status") != "verified" or snapshot.get("source_publication_claim") is not False:
        raise ValueError("immutable source snapshot is not verified")
    for key, digest_name in (("twse", "twse_daily_ohlcv_sha256"),
                             ("tpex", "tpex_daily_ohlcv_sha256"),
                             ("features", "feature_path_sha256")):
        proof = snapshot["sources"][key]
        path = _inside(proof["retained"], snapshot_path.parent)
        if (_sha256(path) != proof["sha256"] or proof["sha256"] != source_hashes[digest_name]
                or path.stat().st_size != proof["bytes"]):
            raise ValueError(f"retained source changed: {key}")
    factorized = snapshot.get("references", {}).get("factorized_manifest")
    if factorized and (_sha256(Path(factorized["path"])) != factorized["sha256"]
                       or factorized["sha256"] != identity.get("factorized_manifest_sha256")):
        raise ValueError("factorized source manifest changed")
    twse = Path(snapshot["sources"]["twse"]["retained"])
    tpex = Path(snapshot["sources"]["tpex"]["retained"])

    missing_limits, missing_opens = [], []
    target_count = 0
    batches = (sessions[i:i + 32] for i in range(0, len(sessions), 32))
    for batch in batches:
        rows_by_day = _official_aggregate_daily_rows_batch(
            twse, tpex, (date.fromisoformat(row["session_date"]) for row in batch))
        for row in batch:
            day = date.fromisoformat(row["session_date"])
            summary_path = _inside(row["summary_path"], signal_root)
            weights_path = _inside(row["weights_path"], signal_root)
            if (summary_path.parent != weights_path.parent
                    or _sha256(summary_path) != row["summary_sha256"]):
                raise ValueError(f"{day}: summary or weights path changed")
            summary = json.loads(summary_path.read_text())
            provenance = summary.get("counterfactual_open_provenance") or {}
            if (_resume_summary(summary_path, identity=identity, signal_id=row["signal_id"],
                                provenance=provenance) is None
                    or provenance.get("feature_cutoff_date") != row["previous_panel_date"]
                    or summary.get("counterfactual_open_clock_known") is not False
                    or summary.get("counterfactual_historical_revision_history_proven") is not False
                    or summary.get("opening_live_source_coverage_proven") is not False
                    or summary.get("price_timestamp") is not None
                    or str(summary.get("replay_effective_signal_at") or "")[:10] != str(day)
                    or date.fromisoformat(str(summary["generated_at"])[:10]) <= day):
                raise ValueError(f"{day}: counterfactual identity or time claims invalid")
            input_path = _inside(provenance["input_path"], input_root)
            limit_path = _inside(provenance["price_limit_path"], limit_root)
            if (limit_path.name != f"{day}.parquet"
                    or _sha256(input_path) != provenance["input_sha256"]
                    or _sha256(limit_path) != provenance["price_limit_sha256"]):
                raise ValueError(f"{day}: official input or limit snapshot changed")
            limits = _load_price_limits(limit_path)
            expected, missing = _open_input_rows(official_rows=rows_by_day[day], limits=limits)
            actual = pl.read_csv(input_path, infer_schema_length=0).to_dicts()
            if len(actual) != len(expected) or missing != provenance["missing_price_limit_rows"]:
                raise ValueError(f"{day}: official open input coverage differs")
            for saved, source in zip(actual, expected):
                if (saved.get("symbol") != source["symbol"]
                        or any(not _same_number(saved.get(key), source[key]) for key in (
                            "price", "open_price", "reference_price", "upper_limit_price", "lower_limit_price"))):
                    raise ValueError(f"{day}: official open input differs from pinned source")
            weights = pl.read_parquet(weights_path, columns=["symbol", "target_weight", "open_price"])
            if (weights.select(pl.col("symbol").is_duplicated().any()).item()
                    or weights.filter(pl.col("target_weight").is_null() | ~pl.col("target_weight").is_finite()).height):
                raise ValueError(f"{day}: duplicate symbol or invalid target weight")
            for target in weights.filter(pl.col("target_weight") != 0).iter_rows(named=True):
                target_count += 1
                symbol = str(target["symbol"])
                opening = target["open_price"]
                if opening is None or not math.isfinite(float(opening)) or float(opening) <= 0:
                    missing_opens.append({"date": str(day), "symbol": symbol})
                limit = limits.get(symbol) or {}
                try:
                    ref, up, down = (float(limit[key]) for key in
                                     ("reference_price", "upper_limit_price", "lower_limit_price"))
                    valid = all(math.isfinite(x) for x in (ref, up, down)) and 0 < down <= ref <= up
                except (KeyError, TypeError, ValueError):
                    valid = False
                if not valid:
                    missing_limits.append({"date": str(day), "symbol": symbol})
    return {
        "contract": "isolated_counterfactual_signals_source_audit_v1",
        "status": "source_gaps_remain" if missing_limits or missing_opens else "source_inputs_audited_not_accepted",
        "audited_at": datetime.now(TAIPEI).isoformat(), "market": receipt["market"],
        "receipt_path": str(receipt_path.resolve()), "receipt_sha256": _sha256(receipt_path),
        "sessions": len(sessions), "start_date": str(expected_sessions[0]),
        "end_date": str(expected_sessions[-1]), "nonzero_targets": target_count,
        "targets_without_official_open": missing_opens, "targets_without_valid_limits": missing_limits,
        "historical_first_print_time_proven": False, "historical_revision_sequence_proven": False,
        "historical_point_in_time_proven": False, "simulation_only": True,
        "broker_orders_created": 0, "paper_fills_created": 0,
        "ledger_modified": False, "publication_authorized": False,
    }


def write_limit_scopes(*, result: dict, audit_path: Path, directory: Path) -> list[Path]:
    """Create source-audit-bound finite price-limit scopes, never ledger data."""
    if (result.get("contract") != "isolated_counterfactual_signals_source_audit_v1"
            or result.get("status") != "source_gaps_remain"
            or result.get("ledger_modified") is not False
            or result.get("publication_authorized") is not False):
        raise ValueError("a completed private source-gap audit is required")
    if directory.exists():
        raise ValueError("price-limit scope directory must be new")
    grouped: dict[str, set[str]] = {}
    for item in result.get("targets_without_valid_limits") or []:
        day, symbol = str(item["date"]), str(item["symbol"])
        date.fromisoformat(day)
        if not symbol or symbol in grouped.setdefault(day, set()):
            raise ValueError("invalid or duplicate missing-limit target")
        grouped[day].add(symbol)
    if not grouped:
        raise ValueError("no finite missing-limit scope")
    if json.loads(audit_path.read_text()) != result:
        raise ValueError("price-limit scope audit readback differs")
    audit_hash = _sha256(audit_path)
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    paths = []
    for day, symbols in sorted(grouped.items()):
        path = directory / f"{day}.json"
        atomic_write_json(path, {
            "contract": "counterfactual_signal_price_limit_scope_v1",
            "session_date": day, "minute_symbols": sorted(symbols),
            "official_no_trade_symbols": [], "ledger_modified": False,
            "simulation_only": True, "publication_authorized": False,
            "signal_source_audit_path": str(audit_path.resolve()),
            "signal_source_audit_sha256": audit_hash,
            "scope_notice": "nonzero historical model targets, not proof of fills or minute availability",
        })
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--signal-root", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--tw-public-dir", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    parser.add_argument("--limit-root", type=Path, default=ROOT / "artifacts/live/tw_price_limits")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit-scopes-root", type=Path,
                        help="Create new private per-day price-limit scopes from this audit.")
    args = parser.parse_args()
    if args.start_date > args.end_date:
        raise ValueError("start date must not be after end date")
    latest, sessions, calendar_sha = _latest_completed_session(
        tw_public_dir=args.tw_public_dir, start_date=args.start_date)
    if args.end_date > latest:
        raise ValueError("end date is not an officially completed session")
    sessions = [day for day in sessions if day <= args.end_date]
    if not sessions or sessions[0] != args.start_date or sessions[-1] != args.end_date:
        raise ValueError("requested boundaries are not completed official sessions")
    output = args.output.resolve()
    if output.exists() or not output.parent.resolve().is_relative_to(args.input_root.resolve()):
        raise ValueError("audit output must be a new file inside the isolated input root")
    result = audit(receipt_path=args.receipt.resolve(strict=True), signal_root=args.signal_root,
                   input_root=args.input_root, expected_sessions=sessions, limit_root=args.limit_root)
    result["official_calendar_sha256"] = calendar_sha
    atomic_write_json(output, result)
    if args.limit_scopes_root is not None:
        scope_root = args.limit_scopes_root.resolve()
        if not scope_root.is_relative_to(args.input_root.resolve()) or scope_root == args.input_root.resolve():
            raise ValueError("limit scopes must be inside the isolated input root")
        scopes = write_limit_scopes(result=result, audit_path=output, directory=scope_root)
        result = {**result, "limit_scope_files_created": len(scopes)}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
