#!/usr/bin/env python3
"""Compare copied and read-only traffic history on one frozen source snapshot.

Copies only two recent anonymous JSONL files to a temporary directory. The
production history, gateway process, and its cache are never changed.
"""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.serve_public_dashboards import PublicTrafficObserver


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _signature(path: Path) -> tuple[int, int, int, int]:
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _output_sha256(payload: dict[str, object]) -> str:
    stable = dict(payload)
    stable.pop("generated_at_utc", None)
    return hashlib.sha256(
        json.dumps(stable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def benchmark(root: Path, *, repeats: int) -> dict[str, object]:
    if not 1 <= repeats <= 100:
        raise ValueError("repeats must be 1..100")
    observed = datetime.now(UTC)
    source_paths = [
        root / f"{(observed.date() - timedelta(days=offset)).isoformat()}.jsonl"
        for offset in (1, 0)
    ]
    source_paths = [path for path in source_paths if path.is_file()]
    if not source_paths:
        raise FileNotFoundError("no recent public traffic history source")

    with tempfile.TemporaryDirectory(prefix="stockagent-traffic-history-benchmark-") as directory:
        frozen_root = Path(directory)
        sources = []
        for source in source_paths:
            before = _signature(source)
            frozen = frozen_root / source.name
            shutil.copyfile(source, frozen)
            if _signature(source) != before:
                raise RuntimeError(f"traffic history source changed during copy: {source.name}")
            sources.append({"name": source.name, "bytes": frozen.stat().st_size,
                            "sha256": _sha256(frozen)})

        loaded_at = time.perf_counter()
        observer = PublicTrafficObserver(history_root=frozen_root)
        load_seconds = time.perf_counter() - loaded_at
        store = observer._history_store
        if store is None:
            observer.close()
            raise RuntimeError("isolated traffic history store did not initialize")
        samples: dict[str, list[float]] = {"copied": [], "readonly_view": []}
        digests: dict[str, list[str]] = {"copied": [], "readonly_view": []}
        try:
            for index in range(repeats):
                order = ("copied", "readonly_view") if index % 2 == 0 else (
                    "readonly_view", "copied"
                )
                for mode in order:
                    context = (
                        patch.object(store, "rows_since_readonly", store.rows_since)
                        if mode == "copied" else nullcontext()
                    )
                    with context:
                        started = time.perf_counter()
                        result = observer.history_snapshot("24h")
                        samples[mode].append(round((time.perf_counter() - started) * 1000, 3))
                        digests[mode].append(_output_sha256(result))
            all_digests = {digest for values in digests.values() for digest in values}
            return {
                "schema_version": 1,
                "measured_at_utc": observed.isoformat(),
                "boundary": "isolated copied source; not live HTTP, browser paint, or cold process startup",
                "source_files": sources,
                "source_loaded_seconds": round(load_seconds, 3),
                "resident_rows": store.status().get("resident_rows"),
                "repeats": repeats,
                "timings_ms": {
                    mode: {
                        "median": round(statistics.median(values), 3),
                        "samples": values,
                    }
                    for mode, values in samples.items()
                },
                "output_sha256": {mode: values for mode, values in digests.items()},
                "outputs_identical": len(all_digests) == 1,
            }
        finally:
            observer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--history-root", type=Path,
        default=Path("/var/lib/stockagent-public-dashboards/performance"),
    )
    parser.add_argument("--repeats", type=int, default=6)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = benchmark(args.history_root, repeats=args.repeats)
    atomic_write_json(args.output, result)
    print(json.dumps({
        "receipt": str(args.output),
        "outputs_identical": result["outputs_identical"],
        "timings_ms": result["timings_ms"],
    }, ensure_ascii=False))
    return 0 if result["outputs_identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
