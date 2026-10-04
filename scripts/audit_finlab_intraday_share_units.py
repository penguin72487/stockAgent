#!/usr/bin/env python3
"""Read-only local audit of receipt-backed FinLab intraday stock-share units."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.finlab_volume_units import (  # noqa: E402
    DEFAULT_REFERENCE_ROOT, VOLUME_UNIT_CONTRACT_VERSION, _reference, reference_revision,
)


def audit(root: Path, reference_root: Path, *, expected_receipts: int | None = None) -> dict:
    root, reference_root = root.resolve(), reference_root.resolve()
    paths = sorted((root / "intraday/receipts").glob("*/*.json"))
    paths += sorted((root / "intraday/derived_minute/receipts").glob("*/*.json"))
    signatures: dict[Path, tuple[int, int, int]] = {}
    hashes: dict[Path, str] = {}
    references: dict[tuple[str, date], dict | None] = {}
    revisions: dict[tuple[str, date], str] = {}
    totals: dict[str, Counter] = {}
    records, problems = [], []

    def checked_hash(path: Path) -> str:
        path = path.resolve()
        stat = path.stat()
        signature = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        if path in signatures and signatures[path] != signature:
            raise ValueError(f"source changed during audit: {path}")
        signatures[path] = signature
        if path not in hashes:
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            hashes[path] = digest.hexdigest()
        return hashes[path]

    def read_receipt(path: Path) -> dict:
        checked_hash(path)
        return json.loads(path.read_text(encoding="utf-8"))

    def object_path(receipt: dict) -> Path:
        relative = Path(receipt["parquet_path"])
        if relative.is_absolute() or ".." in relative.parts or relative.parts[:2] != ("intraday", "objects"):
            raise ValueError("unsafe intraday object path")
        target = (root / relative).resolve()
        if not target.is_relative_to(root / "intraday/objects"):
            raise ValueError("intraday object escapes source root")
        return target

    def checked_frame(receipt: dict) -> pl.DataFrame:
        target = object_path(receipt)
        if checked_hash(target) != receipt["sha256"]:
            raise ValueError("Parquet SHA-256 mismatch")
        if target.stat().st_size != receipt["parquet_size_bytes"]:
            raise ValueError("Parquet byte-size mismatch")
        frame = pl.read_parquet(target)
        if frame.height != receipt["rows"]:
            raise ValueError("Parquet row-count mismatch")
        return frame

    for path in paths:
        record = {"receipt": str(path.relative_to(root)), "issues": []}
        records.append(record)
        try:
            receipt = read_receipt(path)
            family, symbol = receipt["dataset"].split(":", 1)
            family = "derived_minute" if "derived_minute" in path.parts else family
            if family not in {"tw_tick", "tw_minute", "derived_minute"}:
                raise ValueError("unexpected dataset family")
            day = date.fromisoformat(receipt["trade_date"])
            counters = totals.setdefault(family, Counter())
            counters["receipts"] += 1
            record.update(dataset=receipt["dataset"], family=family, trade_date=str(day))
            frame = checked_frame(receipt)
            if not {"volume", "volume_shares", "session"}.issubset(frame.columns):
                raise ValueError("raw/canonical volume or session column missing")
            if list(receipt["fields"]) != frame.columns:
                raise ValueError("receipt field list disagrees with Parquet")
            if receipt.get("canonical_volume_scope") != "regular_session_only":
                raise ValueError("canonical volume scope is not regular-session-only")
            if receipt.get("volume_unit_contract_version") != VOLUME_UNIT_CONTRACT_VERSION:
                raise ValueError("stale volume-unit contract")
            if receipt.get("canonical_volume_unit") != "shares":
                raise ValueError("canonical stock-share unit is unresolved")
            if receipt.get("publication_time_status") != "not_verified_for_training":
                raise ValueError("unexpected publication/PIT claim")
            if frame["session"].null_count():
                raise ValueError("null session")
            key = symbol, day
            if key not in references:
                references[key] = _reference(symbol, day, reference_root)
                revisions[key] = reference_revision(symbol, day, reference_root)
            reference = references[key]
            if reference is None or reference != receipt.get("volume_unit_reference"):
                raise ValueError("direct KBar evidence is absent or changed")
            if (reference.get("reference_kind") != "provider_kbars_direct_amount"
                    or receipt.get("unit_reference_revision") != revisions[key]):
                raise ValueError("direct KBar provenance or reference revision mismatch")
            ref_path = reference_root / reference["reference_receipt"]
            ref = read_receipt(ref_path)
            ref_object = ref_path.with_suffix("").with_suffix(".parquet")
            if (checked_hash(ref_object) != reference["reference_sha256"]
                    or pl.scan_parquet(ref_object).select(pl.len()).collect().item() != ref["rows"]):
                raise ValueError("reference KBar hash or rows mismatch")
            factor = receipt["volume_multiplier"]
            if factor != reference["multiplier"]:
                raise ValueError("share multiplier differs from direct KBar evidence")
            raw, shares = pl.col("volume"), pl.col("volume_shares")
            if frame.filter(raw.is_null() | ~raw.is_finite() | (raw < 0) | (raw != raw.round(0))).height:
                raise ValueError("invalid provider raw volume")
            regular = frame.filter(pl.col("session") == "regular")
            other = frame.filter(pl.col("session") != "regular")
            if regular.filter(shares.is_null() | ~shares.is_finite() | (shares != raw * factor)).height:
                raise ValueError("regular-session shares are not exactly raw times verified factor")
            if other["volume_shares"].null_count() != other.height:
                raise ValueError("nonregular session exposes unverified stock shares")
            if regular["volume"].sum() != reference["regular_raw_volume"]:
                raise ValueError("regular raw total differs from direct KBar total")
            if family == "derived_minute":
                source_relative = Path(receipt["source_tick_receipt"])
                if (source_relative.is_absolute() or ".." in source_relative.parts
                        or source_relative.parts[:2] != ("intraday", "receipts")):
                    raise ValueError("unsafe source tick receipt path")
                source = read_receipt(root / source_relative)
                if (source["sha256"] != receipt["source_tick_sha256"]
                        or source["dataset"] != f"tw_tick:{symbol}"
                        or source["trade_date"] != str(day)):
                    raise ValueError("derived-minute source tick identity or hash mismatch")
                ticks = checked_frame(source).filter(pl.col("session") == "regular")
                if (ticks["volume"].sum() != regular["volume"].sum()
                        or ticks["volume_shares"].sum() != regular["volume_shares"].sum()):
                    raise ValueError("derived-minute raw/share totals differ from source ticks")
                counters["source_tick_links_verified"] += 1
            else:
                # Unit-only migrations retain source_checked_at_utc. Compare
                # their archived raw values; independent provider revisions
                # can legitimately change the same historical source date.
                version_dir = root / "intraday/versions" / path.parent.name / str(day)
                for old_path in sorted(version_dir.glob("*.json")):
                    old = read_receipt(old_path)
                    if old.get("source_checked_at_utc") != receipt.get("source_checked_at_utc"):
                        continue
                    original = checked_frame(old)
                    fields = [name for name in original.columns if name != "volume_shares"]
                    if not original.select(fields).equals(frame.select(fields)):
                        raise ValueError("unit migration changed retained raw provider fields")
                    counters["retained_raw_versions_verified"] += 1
            counters.update(rows=frame.height, regular_rows=regular.height, nonregular_rows=other.height,
                            parquet_bytes=receipt["parquet_size_bytes"], verified=1)
            counters[f"factor_{factor}_receipts"] += 1
            record.update(rows=frame.height, regular_rows=regular.height, nonregular_rows=other.height,
                          sha256=receipt["sha256"], volume_multiplier=factor)
        except (OSError, ValueError, TypeError, KeyError, pl.exceptions.PolarsError) as exc:
            record["issues"].append(f"{type(exc).__name__}: {exc}")
            problems.append({"receipt": record["receipt"], "error": record["issues"][-1]})

    if expected_receipts is not None and len(paths) != expected_receipts:
        problems.append({"error": f"expected {expected_receipts} receipts; found {len(paths)}"})
    if not paths:
        problems.append({"error": "no current intraday receipts"})
    for path, expected in signatures.items():
        try:
            stat = path.stat()
            if (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != expected:
                problems.append({"error": f"source changed during audit: {path}"})
        except OSError as exc:
            problems.append({"error": f"source disappeared during audit: {path}: {exc}"})
    for (symbol, day), revision in revisions.items():
        if reference_revision(symbol, day, reference_root) != revision:
            problems.append({"error": f"reference receipt revision changed: {symbol}/{day}"})
    return {
        "schema_version": 1, "checked_at_utc": datetime.now(UTC).isoformat(),
        "source_root": str(root), "reference_root": str(reference_root),
        "contract_version": VOLUME_UNIT_CONTRACT_VERSION,
        "passed": not problems, "current_receipts": len(paths),
        "families": {name: dict(value) for name, value in sorted(totals.items())},
        "unique_symbol_days": len(references), "checked_files": len(signatures),
        "errors": problems, "partitions": records,
        "scope": "local quantity and retained-source integrity only; not PIT or training-readiness proof",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--reference-root", type=Path, default=DEFAULT_REFERENCE_ROOT)
    parser.add_argument("--expected-receipts", type=int)
    parser.add_argument("--output", type=Path, help="Optional audit artifact outside both source roots.")
    args = parser.parse_args()
    if args.output is not None and any(
        args.output.resolve().is_relative_to(source.resolve())
        for source in (args.root, args.reference_root)
    ):
        parser.error("audit output must be outside the source roots")
    result = audit(args.root, args.reference_root, expected_receipts=args.expected_receipts)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({name: value for name, value in result.items() if name != "partitions"},
                     ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
