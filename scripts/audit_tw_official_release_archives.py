#!/usr/bin/env python3
"""Verify original-release archive bytes, receipts, and coverage separately.

Integrity of locally saved bytes does not imply complete history or prove that
an official page was never revised before our first observation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

import polars as pl


ARCHIVES = ("dgbas_release_vintages", "cbc_fx_reserve_release_vintages",
            "cbc_money_release_vintages")


def _digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def audit_one(root: Path, name: str) -> dict[str, object]:
    root = root.resolve()
    state_path = root / "state" / f"{name}.json"
    parquet_path = root / f"{name}.parquet"
    errors: list[str] = []
    verified_files: set[str] = set()
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise ValueError("state must be a JSON object")
    except (OSError, ValueError) as exc:
        return {"dataset": name, "integrity_ok": False, "coverage_complete": False,
                "value_history_complete": False,
                "errors": [f"state unreadable: {type(exc).__name__}: {exc}"]}

    def verify(path_value: object, hash_value: object, label: str) -> None:
        if not isinstance(path_value, str) or not isinstance(hash_value, str):
            errors.append(f"{label}: missing path or SHA-256")
            return
        try:
            receipt_path = Path(path_value)
            if ".." in receipt_path.parts:
                raise ValueError("receipt path contains parent traversal")
            candidate = receipt_path if receipt_path.is_absolute() else root / receipt_path
            if receipt_path.is_absolute() and not receipt_path.is_relative_to(root):
                # Receipts were written in the mutable producer workspace.
                # Relocate only the dataset's raw subtree inside this audited
                # release; never follow the old absolute path back to live.
                anchors = [
                    index for index, part in enumerate(receipt_path.parts[:-1])
                    if part == "raw" and receipt_path.parts[index + 1] == name
                ]
                if len(anchors) != 1:
                    raise ValueError("receipt path has no unique archive anchor")
                candidate = root.joinpath(*receipt_path.parts[anchors[0]:])
                if label == "parquet":
                    raise ValueError("parquet receipt cannot be relocated")
            path = candidate.resolve(strict=True)
            if not path.is_relative_to(root):
                raise ValueError("receipt path escapes the archive root")
            if label != "parquet" and receipt_path.is_absolute() and not receipt_path.is_relative_to(root):
                if not path.is_relative_to(root / "raw" / name):
                    raise ValueError("relocated receipt escapes its dataset archive")
            if not path.is_file():
                raise ValueError("receipt is not a file")
            if len(hash_value) != 64 or _digest(path) != hash_value:
                raise ValueError("SHA-256 differs from receipt")
            verified_files.add(str(path))
        except (OSError, ValueError) as exc:
            errors.append(f"{label}: {type(exc).__name__}: {exc}")

    verify(str(parquet_path), state.get("parquet_sha256"), "parquet")
    try:
        frame = pl.read_parquet(parquet_path)
    except Exception as exc:
        errors.append(f"parquet unreadable: {type(exc).__name__}: {exc}")
        frame = pl.DataFrame()
    if not frame.is_empty():
        declared = state.get("saved_releases")
        actual_releases = (
            frame["release_url"].n_unique()
            if name == "cbc_money_release_vintages" and "release_url" in frame.columns
            else frame.height
        )
        if declared != actual_releases:
            errors.append(f"saved_releases={declared!r} differs from parquet releases={actual_releases}")
        if {"metric", "period"} <= set(frame.columns):
            usable = frame.filter(pl.col("metric").is_not_null() & pl.col("period").is_not_null())
            if name == "cbc_fx_reserve_release_vintages":
                duplicates = usable.group_by("period").len().filter(pl.col("len") > 1)
                if not duplicates.is_empty():
                    errors.append(f"CBC has {duplicates.height} duplicate subject periods")
                periods = sorted(str(value) for value in usable["period"].to_list())
                if any(re.fullmatch(r"\d{4}-(?:0[1-9]|1[0-2])", value) is None for value in periods):
                    errors.append("CBC archive contains malformed monthly periods")
                elif periods:
                    start_year, start_month = map(int, periods[0].split("-"))
                    end_year, end_month = map(int, periods[-1].split("-"))
                    expected = {
                        f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"
                        for ordinal in range(
                            start_year * 12 + start_month - 1,
                            end_year * 12 + end_month,
                        )
                    }
                    missing = sorted(expected - set(periods))
                    if missing:
                        errors.append(f"CBC archive has {len(missing)} missing intervening months")
                    if state.get("earliest_period") not in {None, periods[0]}:
                        errors.append("CBC earliest_period differs from parquet")
                    if state.get("latest_period") not in {None, periods[-1]}:
                        errors.append("CBC latest_period differs from parquet")
                    if state.get("distinct_periods") not in {None, len(set(periods))}:
                        errors.append("CBC distinct_periods differs from parquet")
            if state.get("headline_values") != usable.height and isinstance(state.get("headline_values"), int):
                errors.append("headline_values differs from nonnull parquet metric rows")
    elif state.get("saved_releases", 0):
        errors.append("state declares saved releases but parquet is empty")

    list_receipts = state.get("listing_receipts")
    if not isinstance(list_receipts, list) or not list_receipts:
        errors.append("official index listing receipts absent")
    else:
        for index, receipt in enumerate(list_receipts):
            if not isinstance(receipt, dict):
                errors.append(f"listing[{index}]: malformed receipt")
                continue
            verify(receipt.get("path"), receipt.get("sha256"), f"listing[{index}]")

    for index, row in enumerate(frame.iter_rows(named=True)):
        if row.get("html_path") is not None:
            verify(row.get("html_path"), row.get("html_sha256"), f"row[{index}].html")
        elif name != "dgbas_release_vintages" or row.get("release_kind") != "direct_attachment":
            errors.append(f"row[{index}]: original HTML receipt absent")
        if name == "dgbas_release_vintages":
            try:
                attachments = json.loads(row.get("attachment_receipts") or "[]")
                if not isinstance(attachments, list):
                    raise ValueError("attachment_receipts is not a list")
            except (TypeError, ValueError) as exc:
                errors.append(f"row[{index}]: malformed attachments: {exc}")
                continue
            if row.get("release_kind") == "direct_attachment" and not attachments:
                errors.append(f"row[{index}]: direct attachment receipt absent")
            for attachment_index, attachment in enumerate(attachments):
                if not isinstance(attachment, dict):
                    errors.append(f"row[{index}].attachment[{attachment_index}]: malformed receipt")
                    continue
                verify(attachment.get("path"), attachment.get("sha256"),
                       f"row[{index}].attachment[{attachment_index}]")

    failures = state.get("failures")
    failed_releases = len(failures) if isinstance(failures, list) else None
    coverage_complete = bool(state.get("complete")) and state.get("status") == "complete"
    if coverage_complete and failed_releases not in {0, None}:
        errors.append("complete state contradicts nonempty failure list")
    if coverage_complete and not frame.height:
        errors.append("complete state has no saved releases")
    actual_releases = (
        frame["release_url"].n_unique()
        if name == "cbc_money_release_vintages" and "release_url" in frame.columns
        else frame.height
    )
    if coverage_complete and state.get("registered_releases") != actual_releases:
        errors.append("complete state has uncollected registered releases")
    missing_periods = state.get("missing_periods")
    has_missing_periods = (
        any(bool(periods) for periods in missing_periods.values())
        if isinstance(missing_periods, dict)
        else bool(missing_periods)
    )
    if coverage_complete and has_missing_periods and name != "cbc_money_release_vintages":
        errors.append("complete state has missing periods")
    if name == "cbc_money_release_vintages":
        if {"release_url", "metric"} <= set(frame.columns):
            identities = frame.select("release_url", "metric").unique()
            if identities.height != frame.height:
                errors.append("duplicate CBC money release metric identity")
        if not {"period", "metric", "value_pct", "published_on"} <= set(frame.columns):
            errors.append("CBC money archive lacks required value or posting-date columns")
        else:
            values = frame.filter(pl.col("metric").is_not_null())
            if values.filter(pl.col("value_pct").is_null()).height:
                errors.append("CBC money named metric lacks a value")
            duplicates = values.group_by("period", "metric").agg(
                pl.len().alias("rows"),
                pl.col("value_pct").n_unique().alias("distinct_values"),
                pl.col("published_on").n_unique().alias("distinct_posting_days"),
            ).filter(pl.col("rows") > 1)
            conflicting_duplicates = duplicates.filter(
                (pl.col("distinct_values") != 1) | (pl.col("distinct_posting_days") != 1)
            )
            if conflicting_duplicates.height:
                errors.append(f"CBC money has {conflicting_duplicates.height} conflicting period metrics")
            metrics_by_period = values.group_by("period").agg(
                pl.col("metric").unique().sort().alias("metrics")
            )
            expected_pair = ["m1b_yoy_pct", "m2_yoy_pct"]
            if any(metrics != expected_pair for metrics in metrics_by_period["metrics"].to_list()):
                errors.append("CBC money has an incomplete or unexpected metric pair")
            periods = sorted(str(value) for value in metrics_by_period["period"].to_list())
            if periods and any(re.fullmatch(r"\d{4}-(?:0[1-9]|1[0-2])", period) is None
                               for period in periods):
                errors.append("CBC money has malformed monthly periods")
            elif periods:
                first_year, first_month = map(int, periods[0].split("-"))
                last_year, last_month = map(int, periods[-1].split("-"))
                expected = {
                    f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"
                    for ordinal in range(first_year * 12 + first_month - 1,
                                         last_year * 12 + last_month)
                }
                actual_missing = sorted(expected - set(periods))
                if state.get("missing_periods") != actual_missing:
                    errors.append("CBC money missing_periods differs from original value rows")
                if state.get("value_history_complete") is not (not actual_missing):
                    errors.append("CBC money value-history claim differs from original value rows")
                if state.get("value_releases") != len(periods):
                    errors.append("CBC money value_releases differs from original value rows")
                if state.get("earliest_period") != periods[0] or state.get("latest_period") != periods[-1]:
                    errors.append("CBC money period bounds differ from original value rows")
    if coverage_complete and name == "cbc_fx_reserve_release_vintages":
        period_rows = int(frame["period"].is_not_null().sum()) if "period" in frame.columns else 0
        if period_rows != frame.height:
            errors.append("complete CBC archive has releases without a period")
        usable_metric_rows = int(frame["metric"].is_not_null().sum()) if "metric" in frame.columns else 0
        if usable_metric_rows != frame.height:
            errors.append("complete CBC archive has releases without a metric")
        usable_values = int(frame["value"].is_not_null().sum()) if "value" in frame.columns else 0
        if usable_values != frame.height:
            errors.append("complete CBC archive has releases without a usable value")
    return {
        "dataset": name,
        "integrity_ok": not errors,
        "coverage_complete": coverage_complete and not errors,
        "value_history_complete": (
            state.get("value_history_complete") is True
            if name == "cbc_money_release_vintages" else coverage_complete and not errors
        ),
        "saved_releases": actual_releases,
        "registered_releases": state.get("registered_releases"),
        "value_rows": int(frame["metric"].is_not_null().sum()) if "metric" in frame.columns else 0,
        "verified_files": len(verified_files),
        "failed_releases": failed_releases,
        "missing_periods": state.get("missing_periods"),
        "generated_at_utc": state.get("generated_at_utc"),
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--require-complete", action="store_true",
                        help="Also fail when any archive is incomplete.")
    parser.add_argument("--require-value-history-complete", action="store_true",
                        help="Also require every monthly value period in each audited archive.")
    args = parser.parse_args()
    results = [audit_one(args.root, name) for name in ARCHIVES]
    print(json.dumps(results, ensure_ascii=False, indent=2))
    if any(not row["integrity_ok"] for row in results):
        return 1
    if args.require_complete and any(not row["coverage_complete"] for row in results):
        return 2
    if args.require_value_history_complete and any(not row["value_history_complete"] for row in results):
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
