#!/usr/bin/env python3
"""Compare selected official option events with one FinMind calendar-day object.

This read-only audit uses explicit, date-bound candidate session windows. It
does not establish listing history, executable quotes, or training admission.
Official decimals are cast to the stored FinMind float32 representation for
comparison; that deliberately cannot prove original-decimal price equality.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip


def _hash(path: Path) -> str:
    return sha256_file(path)


def _compare_multisets(left: pl.DataFrame, right: pl.DataFrame, keys: list[str]) -> tuple[int, int, list, list]:
    """Compare native side-row multiplicities without materializing Python tuples."""
    a = left.group_by(keys).len(name="official")
    b = right.group_by(keys).len(name="finmind")
    joined = a.join(b, on=keys, how="full", coalesce=True).with_columns(
        pl.col("official").fill_null(0).cast(pl.Int64),
        pl.col("finmind").fill_null(0).cast(pl.Int64),
    ).with_columns(
        (pl.col("official") - pl.col("finmind")).clip(lower_bound=0).alias("missing"),
        (pl.col("finmind") - pl.col("official")).clip(lower_bound=0).alias("extra"),
    )
    totals = joined.select(pl.col("missing").sum(), pl.col("extra").sum()).row(0)
    missing = joined.filter(pl.col("missing") > 0).sort("missing", descending=True).head(5).to_dicts()
    extra = joined.filter(pl.col("extra") > 0).sort("extra", descending=True).head(5).to_dicts()
    return int(totals[0] or 0), int(totals[1] or 0), missing, extra


def audit(session: date, policy_path: Path, tick_root: Path, finmind_root: Path) -> dict:
    policy_bytes = policy_path.read_bytes()
    policy = json.loads(policy_bytes)
    if policy.get("trading_date") != session.isoformat():
        raise ValueError("session policy date does not match requested trading date")
    if not policy.get("research_candidate_only"):
        raise ValueError("this audit requires a research-only candidate policy")
    raw_windows = policy["products"]
    if not raw_windows:
        raise ValueError("no products in session policy")
    windows = {
        product: (tuple(item["day_minutes"]), None if item["night_minutes"] is None else tuple(item["night_minutes"]))
        for product, item in raw_windows.items()
    }
    manifest_path = tick_root / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    entries = [x for x in manifest["raw_downloads"]
               if x["kind"] == "options" and x["trading_date"] == session.isoformat()]
    if len(entries) != 1:
        raise ValueError("official source is absent or ambiguous in tick manifest")
    official_path = Path(entries[0]["path"])
    if _hash(official_path) != entries[0]["sha256"]:
        raise ValueError("official source SHA mismatch")
    receipt_path = finmind_root / "receipts/TaiwanOptionTick/all" / f"{session.isoformat()}.json"
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    if receipt.get("status") != "complete" or receipt.get("partition") != session.isoformat():
        raise ValueError("FinMind source receipt is not complete for requested date")
    parquet_path = finmind_root / receipt["parquet_path"]
    if _hash(parquet_path) != receipt["sha256"]:
        raise ValueError("FinMind source SHA mismatch")

    parsed = _parse_zip(official_path, kind="options", trading_date=session,
                        source_sha256=entries[0]["sha256"],
                        option_products=tuple(windows), option_session_windows=windows)
    parsed = parsed.filter((pl.col("event_date") == session) & (pl.col("session") == "day"))
    financial = pl.scan_parquet(parquet_path).filter(
        pl.col("option_id").is_in(list(windows))
        & pl.col("date").str.starts_with(session.isoformat())
    ).collect()

    keys = ["clock", "product", "series", "strike", "right", "price", "side_quantity"]
    result = []
    for product, (day, _) in windows.items():
        official = parsed.filter(pl.col("product") == product)
        first = f"{day[0] // 60:02d}:{day[0] % 60:02d}:00"
        last = f"{day[1] // 60:02d}:{day[1] % 60:02d}:00"
        vendor = financial.filter(
            (pl.col("option_id") == product)
            & (pl.col("date").str.slice(11, 8) >= first)
            & (pl.col("date").str.slice(11, 8) <= last)
        )
        a = official.select(
            pl.col("event_ts").dt.strftime("%H:%M:%S").alias("clock"),
            pl.col("product"), pl.col("delivery_month_week").alias("series"),
            pl.col("strike_price").cast(pl.Float32).alias("strike"),
            pl.col("option_right").alias("right"), pl.col("price").cast(pl.Float32),
            pl.col("reported_side_quantity").cast(pl.Int64).alias("side_quantity"),
        )
        b = vendor.select(
            pl.col("date").str.slice(11, 8).alias("clock"),
            pl.col("option_id").alias("product"), pl.col("contract_date").alias("series"),
            pl.col("ExercisePrice").cast(pl.Float32).alias("strike"),
            pl.col("PutCall").alias("right"), pl.col("price").cast(pl.Float32),
            pl.col("volume").cast(pl.Int64).alias("side_quantity"),
        )
        missing, extra, missing_examples, extra_examples = _compare_multisets(a, b, keys)
        changed_price = official.select(
            (pl.col("price") != pl.col("price").cast(pl.Float32).cast(pl.Float64)).sum()
        ).item()
        changed_strike = official.select(
            (pl.col("strike_price") != pl.col("strike_price").cast(pl.Float32).cast(pl.Float64)).sum()
        ).item()
        result.append({
            "product": product, "official_side_rows": official.height,
            "finmind_side_rows": vendor.height,
            "official_distinct_contracts": official.select("delivery_month_week", "strike_price", "option_right").unique().height,
            "finmind_distinct_contracts": vendor.select("contract_date", "ExercisePrice", "PutCall").unique().height,
            "matched_volume_equivalent": float(official["matched_quantity_equivalent"].sum()),
            "official_price_values_changed_by_float32": changed_price,
            "official_strike_values_changed_by_float32": changed_strike,
            "source_rows_missing_from_finmind": missing,
            "source_rows_extra_in_finmind": extra,
            "float32_multiset_equal": bool(official.height and vendor.height and not missing and not extra),
            "missing_examples": missing_examples, "extra_examples": extra_examples,
        })
    if (_hash(official_path) != entries[0]["sha256"] or _hash(parquet_path) != receipt["sha256"]
            or manifest_path.read_bytes() != manifest_bytes or receipt_path.read_bytes() != receipt_bytes
            or policy_path.read_bytes() != policy_bytes):
        raise ValueError("source or session policy changed during audit")
    parser_path = Path(__file__).resolve().parent / "download_taifex_recent_index_derivatives_ticks.py"
    return {
        "contract": "taifex_finmind_option_contract_event_sample_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "session_date": session.isoformat(),
        "policy": str(policy_path.resolve()), "policy_sha256": hashlib.sha256(policy_bytes).hexdigest(),
        "official_source": str(official_path.resolve()), "official_source_sha256": entries[0]["sha256"],
        "official_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "finmind_source": str(parquet_path.resolve()), "finmind_source_sha256": receipt["sha256"],
        "finmind_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "parser_sha256": _hash(parser_path), "audit_sha256": _hash(Path(__file__)),
        "products": result,
        "limitations": [
            "Candidate session windows are not dated contract-rule or historical-listing verification.",
            "FinMind stores price and strike as float32; matching after that cast is not original decimal equality.",
            "Source side rows are not independent matched trades, executable quotes, or fills.",
            "A one-day comparison does not prove other dates, expiry rules, complete history, or training readiness.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--policy", required=True, type=Path)
    parser.add_argument("--tick-root", type=Path, default=Path("data_tw_index_derivatives_ticks"))
    parser.add_argument("--finmind-root", type=Path, default=Path("data_finmind/complement"))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = audit(args.date, args.policy, args.tick_root, args.finmind_root)
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "products": result["products"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
