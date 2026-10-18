#!/usr/bin/env python3
"""Independently compare complete frozen-source feature-builder trials.

This is engineering acceptance, not publication, source completeness or model
readiness. Read every output byte and preserve NULLs, signed zeros and NaN bits.
Physical Parquet equality is the fast proof; different encodings require every
ordered value of every column, in bounded Arrow batches, not sampled checksums.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.benchmark_tw_public_feature_assembly import _verified_fixture, file_hash
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
from stockagent.data_sync.physical_cache_reuse import _metadata


def _fixture(root: Path) -> dict:
    fixture = _verified_fixture(root)
    expected = [item["name"] for item in fixture["source_files"]]
    if (len(set(expected)) != len(expected)
            or set(expected) != {path.relative_to(root / "source").as_posix()
                                for path in (root / "source").rglob("*.parquet")}):
        raise ValueError("frozen source membership changed")
    if fixture["source_bytes"] != sum(item["size"] for item in fixture["source_files"]):
        raise ValueError("frozen source byte inventory differs")
    for name in expected:
        member = root / "source" / name
        if member.resolve(strict=True) != member:
            raise ValueError("frozen source must not redirect its members")
    return fixture


def ordered_column_bits(path: Path, *, batch_rows: int = 65_536) -> dict[str, Any]:
    """Complete logical bits, invariant to Parquet row groups/batch boundaries.

    Null slots' backing bytes are unspecified and are not observations. Hash
    validity separately from nonnull values, without converting floats or
    equating -0 with +0. The builder's Date/String/Float schema is explicit;
    unsupported nested/extension types reject rather than weaken the oracle.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    if type(batch_rows) is not int or batch_rows < 1:
        raise ValueError("batch_rows must be a positive integer")
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    columns = []
    for field in schema:
        validity, values = hashlib.sha256(), hashlib.sha256()
        rows = nulls = 0
        for batch in parquet.iter_batches(batch_size=batch_rows, columns=[field.name], use_threads=False):
            array = batch.column(0)
            valid = array.is_valid().to_numpy(zero_copy_only=False)
            validity.update(valid.view(np.uint8))
            rows += len(array)
            nulls += array.null_count
            dtype = array.type
            if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
                for value in array.to_pylist():
                    if value is not None:
                        encoded = value.encode("utf-8")
                        values.update(len(encoded).to_bytes(8, "little"))
                        values.update(encoded)
            elif pa.types.is_boolean(dtype):
                # Arrow bools are bit-packed; canonicalize only their encoding.
                values.update(np.asarray(array.to_pylist(), dtype=bool)[valid].view(np.uint8))
            elif pa.types.is_null(dtype):
                continue
            elif (pa.types.is_integer(dtype) or pa.types.is_floating(dtype)
                  or pa.types.is_temporal(dtype) or pa.types.is_fixed_size_binary(dtype)):
                width = dtype.byte_width if pa.types.is_fixed_size_binary(dtype) else dtype.bit_width // 8
                raw = np.frombuffer(array.buffers()[1], dtype=np.uint8,
                                    count=len(array) * width, offset=array.offset * width)
                values.update(np.ascontiguousarray(raw.reshape(len(array), width)[valid]))
            else:
                raise ValueError(f"unsupported exact-bit feature type: {dtype}")
        if rows != parquet.metadata.num_rows:
            raise ValueError(f"incomplete feature column read: {field.name}")
        columns.append({"name": field.name, "type": str(field.type), "rows": rows,
                        "nulls": nulls, "validity_sha256": validity.hexdigest(),
                        "nonnull_value_bits_sha256": values.hexdigest()})
    return {"rows": parquet.metadata.num_rows, "schema": schema.to_string(show_field_metadata=True),
            "columns": columns}


def _output_proof(trial: Path, receipt: dict) -> tuple[Path, dict]:
    output = trial / "features.parquet"
    if output.resolve(strict=True) != output or not output.is_file():
        raise ValueError("trial output must be its own regular, unredirected file")
    before = payload_info(output)
    proof = {"name": output.name, "size": before["size"], "sha256": stable_payload_hash(output, before)}
    if proof != receipt.get("output_receipt"):
        raise ValueError("trial output changed or differs from its committed receipt")
    return output, proof


def verify_trials(fixture_root: Path, trials: list[str]) -> dict:
    import pyarrow.parquet as pq

    root = fixture_root.absolute()
    if root.resolve(strict=True) != root:
        raise ValueError("fixture root must not be redirected")
    if len(trials) < 2 or len(set(trials)) != len(trials):
        raise ValueError("at least two distinct complete trials required")
    fixture_before = _fixture(root)
    manifest_sha = file_hash(root / "fixture.json")
    measured, counts, receipts, paths = [], defaultdict(int), [], []
    for name in trials:
        if not name or Path(name).name != name or name in {".", "..", "source"}:
            raise ValueError("trial must be a direct, named fixture child")
        trial = root / name
        if trial.resolve(strict=True) != trial:
            raise ValueError("trial cannot redirect outside its frozen fixture")
        receipt_path = trial / "receipt.json"
        if receipt_path.resolve(strict=True) != receipt_path or payload_info(receipt_path)["size"] > 4 * 1024**2:
            raise ValueError("trial receipt exceeds its bounded metadata size")
        receipt, receipt_sha = _metadata(receipt_path)
        if (receipt.get("state") != "completed_trial" or receipt.get("build_mode") != "full"
                or receipt.get("original_code_sha256") != fixture_before["original_code_sha256"]
                or receipt.get("symbol_universe_receipt") != fixture_before["symbol_universe"]
                or type(receipt.get("provider_calls")) is not int or receipt["provider_calls"] != 0
                or receipt.get("model_runtime_imported") is not False
                or receipt.get("production_writes") is not False
                or receipt.get("variant") not in {"original", "streaming", "eager"}):
            raise ValueError(f"not an admitted complete private trial: {name}")
        if not receipt.get("source_receipts"):
            raise ValueError("missing complete builder source proofs")
        if any(type(receipt.get(field)) is not int or receipt[field] < 0
               for field in ("rows", "stock_rows", "market_rows", "features")):
            raise ValueError("invalid complete trial row/feature counts")
        if receipts:
            for field in ("source_receipts", "symbol_universe_receipt", "rows", "stock_rows", "market_rows",
                          "features", "pool_profile", "polars_threads", "arrow_threads", "harness_code_sha256"):
                if receipt.get(field) != receipts[0].get(field):
                    raise ValueError(f"trial scope/runtime mismatch: {field}")
        output, output_proof = _output_proof(trial, receipt)
        parquet = pq.ParquetFile(output)
        if (parquet.metadata.num_rows != receipt.get("rows") or receipt["rows"] <= 0
                or receipt["rows"] != receipt["stock_rows"] + receipt["market_rows"]
                or receipt["features"] <= 0):
            raise ValueError("trial row/feature coverage is incomplete")
        for field in ("wall_seconds", "cpu_seconds", "rss_peak_bytes"):
            value = receipt.get(field)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"invalid trial measurement: {field}")
        measured.append({"trial": name, "variant": receipt["variant"],
                         "wall_seconds": receipt["wall_seconds"], "cpu_seconds": receipt["cpu_seconds"],
                         "rss_peak_bytes": receipt["rss_peak_bytes"], "output": output_proof,
                         "receipt_sha256": receipt_sha})
        receipts.append(receipt)
        paths.append(output)
        counts[receipt["variant"]] += 1
    if not counts["original"] or not any(counts[key] for key in ("streaming", "eager")):
        raise ValueError("both an original control and a candidate are required")
    physical_equal = len({item["output"]["sha256"] for item in measured}) == 1
    bits = None
    if not physical_equal:
        bits = ordered_column_bits(paths[0])
        for path in paths[1:]:
            if ordered_column_bits(path) != bits:
                raise ValueError("complete ordered feature bits differ from the control")
    # Recheck payloads/receipts and the complete frozen source after the oracle.
    for path, receipt, item in zip(paths, receipts, measured, strict=True):
        if (_output_proof(path.parent, receipt)[1] != item["output"]
                or file_hash(path.parent / "receipt.json") != item["receipt_sha256"]):
            raise ValueError("trial changed during independent verification")
    if _fixture(root) != fixture_before or file_hash(root / "fixture.json") != manifest_sha:
        raise ValueError("frozen source changed during independent verification")
    aggregates = {}
    for variant, count in counts.items():
        if not count:
            continue
        subset = [row for row in measured if row["variant"] == variant]
        aggregates[variant] = {"count": count, **{
            f"mean_{field}": statistics.mean(row[field] for row in subset)
            for field in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}}
    return {"contract": "tw_public_complete_builder_exact_parity_v1", "state": "accepted_exact_output",
            "scope": "complete_frozen_source_builder_not_production_or_formal_model_acceptance",
            "verified_at_utc": datetime.now(UTC).isoformat(), "fixture": str(root),
            "fixture_sha256": manifest_sha, "source_files": len(fixture_before["source_files"]),
            "source_bytes": fixture_before["source_bytes"], "rows": receipts[0]["rows"],
            "feature_count": receipts[0]["features"], "exact_output_parity": True,
            "proof_method": "complete_parquet_bytes" if physical_equal else "all_ordered_column_bits_and_validity",
            "column_bits": bits, "measurements": measured, "aggregates": aggregates,
            "repeated_per_variant": all(count >= 2 for count in counts.values() if count),
            "provider_calls": 0, "model_runtime_imported": False, "production_writes": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--trial", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.absolute()
    if (output.exists() or output.is_symlink() or output.resolve(strict=False) != output
            or (args.fixture.absolute() / "source") in output.parents):
        parser.error("verification receipt needs a fresh unredirected output outside frozen source")
    result = verify_trials(args.fixture, args.trial)
    from downloader.artifact_io import atomic_write_json
    atomic_write_json(output, result)
    print(json.dumps({key: result[key] for key in ("state", "rows", "feature_count", "proof_method", "aggregates")}))


if __name__ == "__main__":
    main()
