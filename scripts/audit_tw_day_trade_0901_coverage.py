#!/usr/bin/env python3
"""Inventory local 09:01 observations for an isolated signal candidate.

This is a triage receipt, not validation that the candidate was executable.
In particular, an absent bar is not evidence of zero exchange volume.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time as datetime_time, timedelta
import json
import math
from pathlib import Path
import sys
import time
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.backfill_tw_day_trade_open_signals import _sha256  # noqa: E402
from scripts.rebuild_tw_day_trade_open_price_replay import (  # noqa: E402
    DEFAULT_MINUTE_DATA_ROOTS, _local_0901_vwap_rows,
)
from stockagent.live.tw_day_trade_simulation import TAIPEI  # noqa: E402


def _file_inside(path: str | Path, root: Path) -> Path:
    result = Path(path).resolve(strict=True)
    if not result.is_file() or not result.is_relative_to(root.resolve(strict=True)):
        raise ValueError(f"candidate artifact outside isolated root: {result}")
    return result


def _load_targets(row: dict[str, Any], *, signal_root: Path, identity: dict) -> list[str]:
    summary_path = _file_inside(row["summary_path"], signal_root)
    weights_path = _file_inside(row["weights_path"], signal_root)
    if (summary_path.parent != weights_path.parent
            or _sha256(summary_path) != row["summary_sha256"]):
        raise ValueError("signal summary changed or weights path has changed")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    proof = summary.get("counterfactual_artifact_proofs") or {}
    if (proof.get("recipe_identity") != identity
            or proof.get("weights_sha256") != _sha256(weights_path)
            or summary.get("counterfactual_official_open_session") != row["session_date"]):
        raise ValueError("target weights or counterfactual identity changed")
    frame = pl.read_parquet(weights_path, columns=["symbol", "target_weight"])
    if (frame.select(pl.col("symbol").is_null().any()).item()
            or frame.select(pl.col("symbol").is_duplicated().any()).item()
            or frame.filter(pl.col("target_weight").is_null()
                            | ~pl.col("target_weight").is_finite()).height):
        raise ValueError("invalid or duplicate targets")
    return sorted(str(symbol) for symbol in frame.filter(
        pl.col("target_weight") != 0).get_column("symbol"))


def _finlab_observation(root: Path, *, symbol: str, day: date) -> dict[str, Any]:
    """Verify stored FinLab bytes; never download or derive a new partition."""
    from scripts.derive_finlab_minute import derived_receipt_path, stored_derived_receipt
    from scripts.download_finlab_intraday import _partition_paths, _stored_receipt, _validate_frame

    key = f"tw_minute:{symbol}"
    receipt_path = _partition_paths(root, key, day)[0]
    receipt = _stored_receipt(receipt_path, root, key, day, verify_hash=True)
    tick_proof = None
    if receipt is None:
        tick_key = f"tw_tick:{symbol}"
        tick_path = _partition_paths(root, tick_key, day)[0]
        tick = _stored_receipt(tick_path, root, tick_key, day, verify_hash=True)
        if tick is not None and tick.get("sha256"):
            receipt = stored_derived_receipt(root, symbol, day, tick["sha256"])
            if receipt is not None:
                receipt_path = derived_receipt_path(root, symbol, day)
                tick_proof = {"source_tick_receipt": str(tick_path.resolve()),
                              "source_tick_receipt_sha256": _sha256(tick_path),
                              "source_tick_sha256": tick["sha256"]}
    if receipt is None:
        return {"status": "no_verified_local_finlab_receipt"}
    receipt_hash = _sha256(receipt_path)
    status = receipt.get("status")
    if status in {"verified_closed_date", "verified_no_trade", "derived_no_regular_trades"}:
        return {"status": "provider_reports_no_regular_trades_or_closed",
                "receipt_path": str(receipt_path.resolve()), "receipt_sha256": receipt_hash,
                "provider_status": status, "exchange_no_trade_proven": False, **(tick_proof or {})}
    if status not in {"downloaded_unverified_for_pit", "derived_unverified_for_pit"}:
        raise ValueError(f"unsupported FinLab minute status: {status}")
    if receipt.get("source_grain") != "stock_trade_day_left_labelled_minute":
        raise ValueError("FinLab minute label convention is not proven")
    relative = Path(str(receipt.get("parquet_path") or ""))
    if relative.is_absolute() or ".." in relative.parts or relative.parts[:2] != ("intraday", "objects"):
        raise ValueError("FinLab object is outside the canonical object directory")
    source = root / relative
    if not source.is_file() or _sha256(source) != receipt.get("sha256"):
        raise ValueError("FinLab object SHA differs from its native receipt")
    frame = pl.read_parquet(source)
    if _validate_frame(frame, key, day) != "downloaded_unverified_for_pit" or "session" not in frame.columns:
        raise ValueError("FinLab partition failed schema or regular-session verification")
    opening = frame.filter(
        (pl.col("session") == "regular")
        & (pl.col("timestamp").dt.convert_time_zone("Asia/Taipei").dt.time() == datetime_time(9, 0)))
    result: dict[str, Any] = {
        "status": "source_has_no_0900_left_label_bar" if opening.is_empty() else "opening_bar_requires_execution_validation",
        "receipt_path": str(receipt_path.resolve()), "receipt_sha256": receipt_hash,
        "object_path": str(source.resolve()), "object_sha256": receipt["sha256"],
        "source_grain": receipt["source_grain"], "source_publication_time_verified": False,
        "exchange_no_trade_proven": False, **(tick_proof or {}),
    }
    if not opening.is_empty():
        row = opening.row(0, named=True)
        quantity = row.get("volume_shares")
        result["positive_volume_candidate"] = (
            receipt.get("canonical_volume_unit") == "shares"
            and isinstance(quantity, (int, float)) and not isinstance(quantity, bool)
            and math.isfinite(quantity) and quantity > 0)
    if _sha256(receipt_path) != receipt_hash or _sha256(source) != receipt["sha256"]:
        raise ValueError("FinLab source changed while reading")
    return result


def audit(*, receipt_path: Path, source_audit_path: Path, signal_root: Path,
          minute_roots: tuple[Path, ...], year: int, finlab_root: Path | None = None) -> dict[str, Any]:
    """Inspect only nonzero target/date pairs from an already audited batch."""
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt_hash = _sha256(receipt_path)
    audit_hash = _sha256(source_audit_path)
    source_audit = json.loads(source_audit_path.read_text(encoding="utf-8"))
    if (receipt.get("status") != "source_hashes_unchanged_candidate_not_yet_accepted"
            or receipt.get("candidate_accepted") is not False
            or receipt.get("simulation_only") is not True
            or receipt.get("production_order_possible") is not False
            or source_audit.get("contract") != "isolated_counterfactual_signals_source_audit_v1"
            or source_audit.get("status") not in {"source_gaps_remain", "source_inputs_audited_not_accepted"}
            or source_audit.get("receipt_sha256") != receipt_hash
            or source_audit.get("market") != receipt.get("market")
            or source_audit.get("ledger_modified") is not False
            or source_audit.get("publication_authorized") is not False):
        raise ValueError("completed private source audit bound to the batch is required")
    rows = receipt.get("sessions") or []
    days = [date.fromisoformat(row["session_date"]) for row in rows]
    if (not days or days != sorted(set(days)) or any(day.year != year for day in days)
            or len(days) != source_audit.get("sessions")
            or str(days[0]) != source_audit.get("start_date")
            or str(days[-1]) != source_audit.get("end_date")):
        raise ValueError("session calendar or requested year does not match the source audit")

    begin = time.perf_counter()
    results, missing = [], []
    totals = {"nonzero_targets": 0, "observed_positive_volume_candidates": 0,
              "missing_or_nonpositive_volume_candidates": 0, "no_local_price_observation": 0}
    for row, day in zip(rows, days):
        targets = _load_targets(row, signal_root=signal_root, identity=receipt["recipe_identity"])
        books, local = _local_0901_vwap_rows(
            minute_roots=minute_roots, symbols=targets, trading_date=day)
        categories: dict[str, list[str]] = {
            "observed_positive_volume_candidates": [],
            "missing_or_nonpositive_volume_candidates": [],
            "no_local_price_observation": [],
        }
        for symbol in targets:
            book = books.get(symbol)
            if not book:
                key = "no_local_price_observation"
            else:
                volume, price = book.get("tick_volume_units_0901"), book.get("execution_price_0901")
                valid = (book.get("observed_volume_known_0901") is True
                         and isinstance(volume, (int, float)) and not isinstance(volume, bool)
                         and math.isfinite(volume) and volume > 0
                         and isinstance(price, (int, float)) and not isinstance(price, bool)
                         and math.isfinite(price) and price > 0)
                key = ("observed_positive_volume_candidates" if valid
                       else "missing_or_nonpositive_volume_candidates")
            categories[key].append(symbol)
            if key != "observed_positive_volume_candidates":
                item = {"session_date": str(day), "symbol": symbol, "reason": key}
                if finlab_root is not None:
                    item["finlab"] = _finlab_observation(finlab_root, symbol=symbol, day=day)
                missing.append(item)
        totals["nonzero_targets"] += len(targets)
        for key, symbols in categories.items():
            totals[key] += len(symbols)
        results.append({"session_date": str(day), "target_count": len(targets),
                        **categories, "local_loader_errors": local.get("error_counts") or {},
                        "local_price_sources": local.get("source_counts") or {}})

    if totals["nonzero_targets"] != source_audit.get("nonzero_targets"):
        raise ValueError("target count differs from source audit")
    if _sha256(receipt_path) != receipt_hash or _sha256(source_audit_path) != audit_hash:
        raise ValueError("batch receipt or source audit changed during local coverage scan")
    return {
        "contract": "counterfactual_local_0901_coverage_triage_v1",
        "status": "local_source_gaps_or_unverified" if missing else "local_candidates_not_verified",
        "audited_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "elapsed_seconds": round(time.perf_counter() - begin, 6),
        "market": receipt["market"], "year": year, "sessions": len(days),
        "receipt_path": str(receipt_path), "receipt_sha256": receipt_hash,
        "source_audit_path": str(source_audit_path), "source_audit_sha256": audit_hash,
        "minute_roots": [str(root) for root in minute_roots],
        "finlab_root": str(finlab_root) if finlab_root is not None else None,
        "totals": totals, "sessions_detail": results, "unresolved_target_days": missing,
        "source_bytes_independently_verified": False,
        "missing_means_no_trade": False, "positive_volume_proves_fill": False,
        "historical_intraday_completeness_proven": False,
        "ledger_modified": False, "broker_orders_created": 0,
        "paper_fills_created": 0, "publication_authorized": False,
    }


def write_finlab_priority_plan(*, result: dict, output: Path) -> int:
    """Queue only exact target-days without a native source receipt."""
    if result.get("contract") != "counterfactual_local_0901_coverage_triage_v1":
        raise ValueError("private 09:01 coverage triage is required")
    items = sorted({(row["symbol"], row["session_date"]) for row in result["unresolved_target_days"]
                    if (row.get("finlab") or {}).get("status") == "no_verified_local_finlab_receipt"})
    if not items:
        raise ValueError("no exact FinLab target-days need a local receipt")
    now = datetime.now(UTC)
    atomic_write_json(output, {
        "contract": "provider_priority_query_v1", "provider": "finlab",
        "reason": "200萬研究模擬今年非零訊號，查驗本機缺少收據的分鐘來源；不得推論成交",
        "created_at_utc": now.isoformat(), "expires_at_utc": (now + timedelta(days=6)).isoformat(),
        "items": [{"dataset": f"tw_minute:{symbol}", "partition": day} for symbol, day in items],
    })
    return len(items)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--source-audit", type=Path, required=True)
    parser.add_argument("--signal-root", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minute-root", type=Path, action="append")
    parser.add_argument("--finlab-root", type=Path, help="Inspect existing native receipts without provider calls.")
    parser.add_argument("--finlab-priority-plan", type=Path, help="Create an isolated finite plan for missing receipts.")
    args = parser.parse_args()
    input_root = args.input_root.resolve(strict=True)
    receipt = _file_inside(args.receipt, input_root)
    source_audit = _file_inside(args.source_audit, input_root)
    output = args.output.resolve()
    if output.exists() or not output.parent.is_relative_to(input_root):
        raise ValueError("output must be a new file inside the private input root")
    roots = tuple(path.resolve() for path in (args.minute_root or DEFAULT_MINUTE_DATA_ROOTS))
    finlab_root = args.finlab_root.resolve(strict=True) if args.finlab_root is not None else None
    result = audit(receipt_path=receipt, source_audit_path=source_audit,
                   signal_root=args.signal_root, minute_roots=roots, year=args.year, finlab_root=finlab_root)
    atomic_write_json(output, result)
    if args.finlab_priority_plan is not None:
        plan = args.finlab_priority_plan.resolve()
        if finlab_root is None or plan.exists() or not plan.parent.is_relative_to(input_root):
            raise ValueError("priority plan requires a FinLab root and a new private output file")
        result["finlab_priority_items"] = write_finlab_priority_plan(result=result, output=plan)
    print(json.dumps({key: result[key] for key in (
        "status", "year", "sessions", "totals", "elapsed_seconds", "publication_authorized")},
        ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
