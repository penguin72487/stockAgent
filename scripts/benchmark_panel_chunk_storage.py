#!/usr/bin/env python3
"""Compare complete Panel persistence/append/readback on a fixed real receipt.

The last real source session supplies the append: no fabricated observation or
provider request. This is storage engineering, not a historical PIT rebuild.
Keep all outputs and proofs; byte-identical final files share verified readonly
links after (never during) each measured trial. Never modify the source cache.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import replace
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np

from stockagent.data.panel import _panel_from_cache_payload
from stockagent.data.panel_cache import (
    ARRAY_NAMES, load_panel_cache_v2, load_panel_cache_v2_manifest,
    _cache_writer_lock, panel_cache_v2_dir, read_panel_cache_v2_meta, save_panel_cache_v2,
)
from stockagent.data import panel_chunks
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.data_sync.immutable_cache_files import (
    link_immutable_payload, payload_info, stable_payload_hash,
)


def _file_proof(path: Path) -> dict:
    before = payload_info(path)
    return {"path": str(path), "metadata": before,
            "sha256": stable_payload_hash(path, before)}


def _array_hash(array) -> str:
    raw = np.asarray(array)
    if not raw.flags.c_contiguous:
        raise ValueError("this benchmark requires original contiguous source arrays")
    return hashlib.sha256(memoryview(raw.view(np.uint8).reshape(-1))).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--chunk-mib", type=int, default=8)
    parser.add_argument("--minimum-mib", type=int, default=8)
    parser.add_argument("--reuse-evidence", type=Path, action="append", default=[])
    args = parser.parse_args()
    if any(value <= 0 or value & (value - 1) for value in (args.chunk_mib, args.minimum_mib)):
        parser.error("chunk and minimum sizes must be positive powers of two")
    panel_chunks.CHUNK_BYTES = args.chunk_mib * 1024**2
    panel_chunks.MIN_CHUNKED_BYTES = args.minimum_mib * 1024**2
    source_code = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in (Path(__file__), ROOT / "stockagent/data/panel_chunks.py",
                                ROOT / "stockagent/data/panel_cache.py")}
    output = args.output.absolute()
    allowed = (ROOT / "artifacts/benchmarks").resolve()
    if not output.resolve().is_relative_to(allowed) or output.exists():
        parser.error("use a new private root below artifacts/benchmarks")
    manifest = args.manifest.resolve(strict=True)
    original_meta = json.loads(manifest.read_bytes())
    source_root = manifest.parent.parent if manifest.parent.name == "variants" else manifest.parent
    files = [manifest, source_root / original_meta["symbols_file"],
             source_root / original_meta["feature_names_file"]]
    for item in original_meta["arrays"].values():
        files.extend(panel_chunks.array_paths(source_root, item))
    source_proofs = [_file_proof(path) for path in sorted(set(files))]
    loaded = load_panel_cache_v2_manifest(
        manifest, expected_generation=original_meta["generation"],
        expected_version=original_meta["version"],
        expected_source_hash=original_meta["source_hash"],
    )
    full = _panel_from_cache_payload(loaded)
    if full.num_dates < 3:
        parser.error("need at least three real sessions")
    prefix = replace(full, **{
        name: getattr(full, name)[:-1] for name in ARRAY_NAMES
        if isinstance(getattr(full, name, None), np.ndarray)
    }, content_fingerprints=None)
    expected = {name: _array_hash(getattr(full, name)) for name in ARRAY_NAMES
                if getattr(full, name, None) is not None}
    expected_specs = {name: {"shape": list(getattr(full, name).shape),
                             "dtype": getattr(full, name).dtype.str} for name in expected}
    output.mkdir(parents=True)
    records, shared = [], {}
    # Reuse trial evidence within this private benchmark only. Linking from a
    # live source without its writer lease would change its inode metadata.
    old_setting = os.environ.get("STOCKAGENT_PANEL_CHUNK_STORAGE")
    donor_leases = ExitStack()
    try:
        for previous in args.reuse_evidence:
            previous = previous.absolute()
            if previous.resolve() != previous or not previous.is_relative_to(allowed):
                raise ValueError("benchmark evidence donor must be a real private benchmark root")
            accepted = json.loads((previous / "acceptance.json").read_bytes())
            if accepted.get("state") != "accepted_complete_panel_storage_workflow":
                raise ValueError("benchmark donor has no complete acceptance")
            for index, record in enumerate(accepted["records"]):
                if record.get("complete_array_hashes") != expected or record.get("kind") not in ("whole", "chunks"):
                    raise ValueError("benchmark donor values differ from this selected source")
                cache = panel_cache_v2_dir(previous / f"{index}-{record['kind']}")
                donor_leases.enter_context(_cache_writer_lock(cache))
                for item in read_panel_cache_v2_meta(cache.parent)["arrays"].values():
                    for path in panel_chunks.array_paths(cache, item):
                        if path.suffix not in (".npy", ".bin"):
                            continue
                        info = payload_info(path)
                        if info["mode"] & 0o222:
                            continue
                        digest = stable_payload_hash(path, info)
                        shared.setdefault((digest, info["size"]), path)
        for index, kind in enumerate(("whole", "chunks", "chunks", "whole")):
            root = output / f"{index}-{kind}"
            os.environ["STOCKAGENT_PANEL_CHUNK_STORAGE"] = "1" if kind == "chunks" else "0"
            stages = {}
            for stage, value in (("initial", prefix), ("append", full)):
                before = resource.getrusage(resource.RUSAGE_SELF)
                start = time.perf_counter()
                save_panel_cache_v2(root, value, source_hash=f"engineering-{stage}",
                                   backend_key="same-fixed-real-observations", version=original_meta["version"])
                elapsed = time.perf_counter() - start
                after = resource.getrusage(resource.RUSAGE_SELF)
                meta = read_panel_cache_v2_meta(root)
                stages[stage] = {"wall_seconds": elapsed, "cpu_seconds": (
                    after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
                ), "storage_build": meta["storage_build"]}
            start = time.perf_counter()
            result = load_panel_cache_v2(root, mmap_mode="c")
            load_seconds = time.perf_counter() - start
            start = time.perf_counter()
            actual = {name: _array_hash(result[name]) for name in expected}
            actual_specs = {name: {"shape": list(result[name].shape),
                                   "dtype": result[name].dtype.str} for name in expected}
            if (actual != expected or actual_specs != expected_specs
                    or result["symbols"] != full.symbols or result["feature_names"] != full.feature_names):
                raise AssertionError("complete Panel values or axes differ from source")
            verification_seconds = time.perf_counter() - start
            record = {"kind": kind, "stages": stages, "load_seconds": load_seconds,
                      "full_readback_verification_seconds": verification_seconds,
                      "complete_array_hashes": actual, "complete_axes_equal": True,
                      "complete_array_specs": actual_specs,
                      "features_memmap": isinstance(result["features"], np.memmap),
                      "peak_process_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
            # Reuse final immutable evidence only after the measured workflow.
            linked = 0
            cache = panel_cache_v2_dir(root)
            for item in read_panel_cache_v2_meta(root)["arrays"].values():
                for path in panel_chunks.array_paths(cache, item):
                    if path.suffix not in (".npy", ".bin"):
                        continue
                    info = payload_info(path)
                    digest = stable_payload_hash(path, info)
                    key = (digest, info["size"])
                    donor = shared.get(key)
                    if donor is not None and donor != path:
                        linked += int(link_immutable_payload(donor, path, sha256=digest, size=info["size"]))
                    else:
                        shared[key] = path
            record["post_measurement_exact_links"] = linked
            records.append(record)
            atomic_write_json(output / "progress.json", {"records": records})
            print(json.dumps({"trial": index, "kind": kind,
                              "append_seconds": stages["append"]["wall_seconds"],
                              "append_storage": stages["append"]["storage_build"],
                              "load_seconds": load_seconds}), flush=True)
            del result
    finally:
        donor_leases.close()
        if old_setting is None:
            os.environ.pop("STOCKAGENT_PANEL_CHUNK_STORAGE", None)
        else:
            os.environ["STOCKAGENT_PANEL_CHUNK_STORAGE"] = old_setting
    for proof in source_proofs:
        current = _file_proof(Path(proof["path"]))
        if current != proof:
            raise AssertionError("original source receipt or payload changed")
    receipt = {"state": "accepted_complete_panel_storage_workflow",
               "observed_at_utc": datetime.now(UTC).isoformat(),
               "source_manifest": str(manifest), "source_proofs": source_proofs,
               "dates": full.num_dates, "symbols": full.num_symbols,
               "features": len(full.feature_names), "order": "ABBA",
               "chunk_bytes": panel_chunks.CHUNK_BYTES,
               "minimum_chunked_bytes": panel_chunks.MIN_CHUNKED_BYTES,
               "source_code": source_code,
               "records": records, "original_values_receipts_preserved": True,
               "provider_calls": 0, "production_cache_mutations": 0,
               "reader_rollout_required_before_writer_opt_in": True}
    atomic_write_json(output / "acceptance.json", receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
