#!/usr/bin/env python3
"""Read-only proof of streaming per-dataset feature rows versus full JSON write.

The trusted producer source is frozen in memory before any trial. Shard files
and outputs are temporary; this does not enable incremental publication.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.dashboard_updates import metadata_signature  # noqa: E402
from stockagent.live.data_monitor_feature_receipt import trusted_feature_snapshot  # noqa: E402


def _encode(payload: Any) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"),
        sort_keys=False, allow_nan=False,
    ).encode("utf-8")


def _trusted_body(path: Path) -> tuple[bytes, os.stat_result]:
    before = path.stat()
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        body = stream.read()
        closed = os.fstat(stream.fileno())
    if not (
        metadata_signature(before)
        == metadata_signature(opened)
        == metadata_signature(closed)
        == metadata_signature(path.stat())
    ):
        raise ValueError("feature source changed during read")
    if not trusted_feature_snapshot(path, source_stat=before, body=body):
        raise ValueError("feature source receipt is not trusted")
    return body, before


def _row_blocks(rows: list[dict[str, Any]]) -> list[tuple[str, bytes]]:
    """Keep the producer's existing order; refuse non-contiguous datasets."""

    blocks: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    current_dataset: str | None = None
    current_rows: list[bytes] = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("dataset_id"), str):
            raise ValueError("feature row has no dataset ID")
        dataset = row["dataset_id"]
        if dataset != current_dataset:
            if current_dataset is not None:
                blocks.append((current_dataset, b",".join(current_rows)))
            if dataset in seen:
                raise ValueError("dataset rows are not contiguous")
            seen.add(dataset)
            current_dataset = dataset
            current_rows = []
        current_rows.append(_encode(row))
    if current_dataset is not None:
        blocks.append((current_dataset, b",".join(current_rows)))
    return blocks


def _write_streamed(path: Path, prefix: bytes, shard_paths: list[Path]) -> None:
    with path.open("wb") as output:
        output.write(prefix)
        for index, shard in enumerate(shard_paths):
            if index:
                output.write(b",")
            with shard.open("rb") as source:
                for chunk in iter(lambda: source.read(1 << 20), b""):
                    output.write(chunk)
        output.write(b"]}\n")


def benchmark(source: Path, *, temp_root: Path) -> dict[str, Any]:
    source_body, source_stat = _trusted_body(source)

    def reject_nonfinite(value: str) -> None:
        raise ValueError(f"non-finite feature JSON: {value}")

    payload = json.loads(source_body, parse_constant=reject_nonfinite)
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or payload.get("read_only") is not True
        or payload.get("production_control_possible") is not False
        or not isinstance(payload.get("rows"), list)
        or list(payload)[-1] != "rows"
    ):
        raise ValueError("feature source contract or order changed")
    shard_build_started = time.perf_counter()
    blocks = _row_blocks(payload["rows"])
    shard_encode_ms = round((time.perf_counter() - shard_build_started) * 1_000, 3)
    header = {key: value for key, value in payload.items() if key != "rows"}
    prefix = _encode(header)[:-1] + b',"rows":['
    source_digest = hashlib.sha256(source_body).hexdigest()
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="feature-shard-proof-", dir=temp_root) as temporary:
        directory = Path(temporary)
        shard_paths = []
        shard_write_started = time.perf_counter()
        for index, (_dataset, body) in enumerate(blocks):
            path = directory / f"{index:04d}.rows.json"
            path.write_bytes(body)
            shard_paths.append(path)
        shard_write_ms = round((time.perf_counter() - shard_write_started) * 1_000, 3)
        output = directory / "trial.json"
        trials = []
        for variant in ("full", "sharded", "sharded", "full"):
            started = time.perf_counter()
            if variant == "full":
                output.write_bytes(_encode(payload) + b"\n")
            else:
                _write_streamed(output, prefix, shard_paths)
            elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)
            body = output.read_bytes()
            digest = hashlib.sha256(body).hexdigest()
            trials.append({
                "variant": variant,
                "elapsed_ms": elapsed_ms,
                "output_sha256": digest,
                "exact_source_bytes": body == source_body,
            })
    return {
        "state": "verified" if all(row["exact_source_bytes"] for row in trials) else "mismatch",
        "source_sha256": source_digest,
        "source_signature": list(metadata_signature(source_stat)),
        "source_bytes": len(source_body),
        "rows": len(payload["rows"]),
        "dataset_shards": len(blocks),
        "shard_bytes": sum(len(body) for _dataset, body in blocks),
        "shard_encode_ms": shard_encode_ms,
        "shard_write_ms": shard_write_ms,
        "trials": trials,
        "claim_boundary": (
            "Frozen trusted full snapshot; isolated serialization and local temporary I/O. "
            "The sharded steady-state trials assume previously encoded immutable shards; "
            "a first build also pays shard_encode_ms and shard_write_ms. "
            "No old-generation verification, source scan, footer projection, "
            "atomic publication, gateway read, or crash recovery is measured."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path,
        default=REPO_ROOT / "artifacts/live/data_monitor/feature_inventory.json",
    )
    parser.add_argument("--temp-root", type=Path, default=REPO_ROOT / "artifacts/benchmarks")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = benchmark(args.source.resolve(), temp_root=args.temp_root.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0 if result["state"] == "verified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
