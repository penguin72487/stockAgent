from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import polars as pl

try:
    from scripts.build_shioaji_tw_minute_dataset import (
        FEATURE_STATISTICS_CONTRACT,
        SCHEMA_VERSION,
    )
except ModuleNotFoundError:
    # Direct ``python scripts/...py`` execution puts ``scripts`` rather than
    # the repository root on sys.path.
    from build_shioaji_tw_minute_dataset import (  # type: ignore[no-redef]
        FEATURE_STATISTICS_CONTRACT,
        SCHEMA_VERSION,
    )


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a non-destructive schema-v4 minute dataset view with "
            "Float64 minutes_from_open statistics. Parquet payloads are hard-linked."
        )
    )
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return payload


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_partition(root: Path, summary: dict[str, Any]) -> Path:
    relative = Path(str(summary.get("output", "")))
    candidates = (
        root / relative,
        root / f"trade_date={summary.get('trade_date')}" / "data.parquet",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise RuntimeError(f"minute partition is missing: {candidates[-1]}")


def _repair_partition(
    input_root: Path,
    summary: dict[str, Any],
) -> tuple[str, Path, dict[str, Any], bool]:
    trade_date = str(summary.get("trade_date", ""))
    source = _source_partition(input_root, summary)
    row = (
        pl.scan_parquet(source)
        .filter(pl.col("feature_valid").fill_null(False))
        .select(
            pl.col("minutes_from_open").count().alias("count"),
            pl.col("minutes_from_open").cast(pl.Float64).sum().alias("sum"),
            (
                pl.col("minutes_from_open").cast(pl.Float64)
                * pl.col("minutes_from_open").cast(pl.Float64)
            )
            .sum()
            .alias("sum_square"),
        )
        .collect()
        .row(0, named=True)
    )
    repaired = deepcopy(summary)
    repaired["schema_version"] = SCHEMA_VERSION
    repaired["feature_statistics_contract"] = FEATURE_STATISTICS_CONTRACT
    repaired["output"] = f"trade_date={trade_date}/data.parquet"
    old_square = float(repaired["feature_sum_squares"]["minutes_from_open"])
    repaired["feature_counts"]["minutes_from_open"] = int(row["count"] or 0)
    repaired["feature_sums"]["minutes_from_open"] = float(row["sum"] or 0.0)
    repaired["feature_sum_squares"]["minutes_from_open"] = float(
        row["sum_square"] or 0.0
    )
    return (
        trade_date,
        source,
        repaired,
        old_square != repaired["feature_sum_squares"]["minutes_from_open"],
    )


def main() -> None:
    args = _args()
    input_root = args.input_root.resolve()
    output_root = args.output_root.resolve()
    if output_root.exists():
        raise RuntimeError(
            f"refusing to overwrite an existing repaired dataset: {output_root}"
        )
    if int(args.workers) < 1:
        raise ValueError("workers must be positive")
    manifest_path = input_root / "manifest.json"
    manifest = _read_json(manifest_path)
    if manifest.get("schema_version") not in {3, SCHEMA_VERSION}:
        raise RuntimeError("unsupported minute source dataset schema")
    partitions = list(manifest.get("partitions", []))
    if not partitions:
        raise RuntimeError("minute source dataset contains no partitions")

    staging = output_root.with_name(f".{output_root.name}.building-{os.getpid()}")
    if staging.exists():
        raise RuntimeError(f"staging directory already exists: {staging}")
    staging.mkdir(parents=True)
    repaired_by_date: dict[str, dict[str, Any]] = {}
    changed = 0
    with ThreadPoolExecutor(max_workers=int(args.workers)) as executor:
        futures = [
            executor.submit(_repair_partition, input_root, summary)
            for summary in partitions
        ]
        for position, future in enumerate(futures, start=1):
            trade_date, source, repaired, was_changed = future.result()
            partition_root = staging / f"trade_date={trade_date}"
            partition_root.mkdir(parents=True)
            target = partition_root / "data.parquet"
            try:
                os.link(source, target)
            except OSError as exc:
                raise RuntimeError(
                    "feature-statistics repair requires same-filesystem hard links "
                    f"to avoid copying the immutable parquet payload: {source}"
                ) from exc
            _atomic_json(partition_root / "summary.json", repaired)
            repaired_by_date[trade_date] = repaired
            changed += int(was_changed)
            if position == 1 or position % 100 == 0 or position == len(futures):
                print(
                    f"[tw-minute-stats-repair] partitions={position}/{len(futures)} "
                    f"changed={changed} last_date={trade_date}",
                    flush=True,
                )

    repaired_manifest = deepcopy(manifest)
    repaired_manifest["schema_version"] = SCHEMA_VERSION
    repaired_manifest["feature_statistics_contract"] = FEATURE_STATISTICS_CONTRACT
    repaired_manifest["partitions"] = [
        repaired_by_date[str(date)] for date in manifest["dates"]
    ]
    repaired_manifest["source_manifest_sha256"] = _sha256(manifest_path)
    repaired_manifest["statistics_repair"] = {
        "method": "recompute_minutes_from_open_float64_and_hardlink_payloads",
        "source_root": str(input_root),
        "changed_partitions": changed,
        "repaired_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    }
    _atomic_json(staging / "manifest.json", repaired_manifest)
    os.replace(staging, output_root)
    print(
        f"[tw-minute-stats-repair] complete output={output_root} "
        f"partitions={len(partitions)} changed={changed}",
        flush=True,
    )


if __name__ == "__main__":
    main()
