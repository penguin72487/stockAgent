#!/usr/bin/env python3
"""Fixed complete private catalog-cache transaction, including exact readback.

Runs the production lock/read/decode/change/write path on disposable SSD copies.
Only the decoder differs. A cycle increment forces a complete cache publication;
provider builders, source checks and public status generation are outside scope.
No production cache or service is modified. The fixed original seed is retained.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import snapshot_data_refresh_services as producer
from stockagent import private_json


def write_new(path, body):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
        stream.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("preserve existing results")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pins = [Path(__file__).resolve(), Path(producer.__file__), Path(private_json.__file__)]
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in pins}
    with args.cache.open("rb") as stream:
        stat_before = os.fstat(stream.fileno())
        raw = stream.read()
        stat_after = os.fstat(stream.fileno())
    signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if signature(stat_before) != signature(stat_after):
        raise ValueError("held source descriptor changed")
    seed = args.output.with_name(args.output.stem + "_seed.json")
    write_new(seed, raw)
    current = json.loads(raw)
    if not isinstance(current, dict) or type(current.get("cycle")) is not int:
        raise ValueError("benchmark requires the complete versioned bar cache")
    current["cycle"] += 1
    expected = (json.dumps(current, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    expected_sha = hashlib.sha256(expected).hexdigest()
    expected_cycle = current["cycle"]
    del current

    def update(root, *, previous, now, audit_seconds):
        # Exercise a full real cache transaction without doing provider work.
        updated = {**previous, "cycle": previous["cycle"] + 1}
        return {"cycle": updated["cycle"]}, updated

    trials = []
    for variant in "ABBABAAB":
        with tempfile.TemporaryDirectory(prefix="catalog-cache-", dir=args.output.parent) as directory:
            output = Path(directory) / "public_status.json"
            cache = output.with_name("bar_inventory_cache.json")
            cache.write_bytes(raw)  # Equal fixed starting generation, outside timing.
            decoder = SimpleNamespace(loads=json.loads) if variant == "A" else private_json
            cpu = time.process_time()
            started = time.perf_counter()
            with patch.object(producer, "private_json", decoder):
                inventory = producer._catalog_snapshot(output, name="bar", builder=update,
                    observed=datetime.now(UTC), audit_seconds=2.)
            observed = cache.read_bytes()
            assert inventory == {"cycle": expected_cycle}
            assert observed == expected
            digest = hashlib.sha256(observed).hexdigest()
            assert digest == expected_sha
            trials.append({"variant": variant, "wall_s": time.perf_counter() - started,
                           "cpu_s": time.process_time() - cpu, "complete_output_sha256": digest,
                           "output_bytes": len(observed)})
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in pins}
    means = {label: {metric: statistics.mean(row[metric] for row in trials if row["variant"] == label)
                     for metric in ["wall_s", "cpu_s"]} for label in ["A", "B"]}
    result = {"schema_version": 1, "scope": __doc__, "seed": str(seed), "input_bytes": len(raw),
              "input_sha256": hashlib.sha256(raw).hexdigest(), "held_source_generation": signature(stat_before),
              "source_pins_before": before, "source_pins_after": after, "source_stable": before == after,
              "native_available": private_json.native_available(), "trials": trials, "means": means,
              "wall_ratio": means["B"]["wall_s"] / means["A"]["wall_s"],
              "cpu_ratio": means["B"]["cpu_s"] / means["A"]["cpu_s"],
              "all_complete_output_bytes_equal": True}
    write_new(args.output, json.dumps(result, ensure_ascii=False, allow_nan=False).encode())
    print(json.dumps({k: result[k] for k in ["input_bytes", "means", "wall_ratio", "cpu_ratio", "source_stable"]}))
    if before != after:
        raise ValueError("source changed during complete experiment")


if __name__ == "__main__":
    main()
