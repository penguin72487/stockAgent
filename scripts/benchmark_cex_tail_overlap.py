"""Offline full stored-universe comparison of exact CEX tail overlap facts.

Pin all mutable tails; retain read-only base references behind generation guards.
Do not copy 176 GB of unchanged historical values just to inspect their footers.
All original information loaders and error/interval rules remain the oracle.
The prototype replaces only the round-trip date block, in memory, before any
production caller is changed. Each case publishes both complete inventories.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import asdict
from datetime import UTC, datetime
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "downloader"))

from downloader.artifact_io import atomic_write_json
from downloader.ohlcv_hot_tail import hot_tail_path, tail_newer_summary

PROVIDERS = ("bybit", "okx", "binance")


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity(path: Path) -> list[int]:
    stat = path.stat()
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def load_reference(path: Path, provider: str):
    name = f"cex_overlap_reference_{provider}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("missing reference module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def prototype(module, function):
    """Reuse the full old function, with exactly its discarded block replaced."""
    source = inspect.getsource(function)
    start = source.index("    dates = ")
    end = source.index("    contiguous = ", start)
    if source.count("base.rows + newer.height") != 1:
        raise ValueError("unexpected reference overlap contract")
    source = source[:start] + (
        "    newer_count, first_new = tail_newer_summary(\n"
        "        _read_overlap_dates(tail_path), base_latest_ms=base.latest_ms,\n"
        "        normalize=_normalize_date_frame,\n"
        "    )\n"
    ) + source[end:]
    source = source.replace("base.rows + newer.height", "base.rows + newer_count")
    namespace = dict(module.__dict__)
    namespace["tail_newer_summary"] = tail_newer_summary
    namespace["_read_overlap_dates"] = (
        module._read_date_column if hasattr(module, "_read_date_column") else
        lambda path: module.pl.from_arrow(module.pq.read_table(path, columns=["date"], memory_map=True))
    )
    exec(compile(source, f"<exact_overlap_prototype_{module.__name__}>", "exec"), namespace)
    return namespace[function.__name__]


def benchmark(reference_dir: Path, output: Path) -> dict:
    reference_dir, output = reference_dir.resolve(), output.resolve()
    roots = {provider: ROOT / f"data_{provider}" / "1m" for provider in PROVIDERS}
    if output.exists() or not output.parent.is_dir():
        raise ValueError("fresh output and existing parent required")
    for root in roots.values():
        resolved = root.resolve()
        if output == resolved or resolved in output.parents or output in resolved.parents:
            raise ValueError("private benchmark must not overlap source")
    code_paths = [Path(__file__), ROOT / "downloader/ohlcv_hot_tail.py", *(
        reference_dir / f"reference_{provider}.py" for provider in PROVIDERS)]
    code_hashes = {str(path): sha(path) for path in code_paths}
    native_paths = {p: sorted(root.glob("*_features.parquet")) for p, root in roots.items()}
    if any(not paths for paths in native_paths.values()):
        raise ValueError("all three complete source universes required")
    tail_bytes = sum(hot_tail_path(path).stat().st_size for paths in native_paths.values()
                     for path in paths if hot_tail_path(path).is_file())
    if shutil.disk_usage(output.parent).free < 32 * 1024**3 + 2 * tail_bytes:
        raise RuntimeError("private tail snapshot exceeds disk headroom")
    output.mkdir()
    preparation = time.monotonic()
    context, base_guards, private_roots = {}, {}, {}
    for provider, paths in native_paths.items():
        private = output / provider
        private.mkdir()
        (private / "_hot_tail").mkdir()
        private_roots[provider] = private
        scope = []
        for path in paths:
            base_before = identity(path)
            # Symlinks do not mutate source inode/link counts or ctime. Native
            # collectors atomically replace files; before/after guards reject
            # any base generation drift. Never write through these references.
            os.symlink(path.resolve(), private / path.name)
            base_guards[str(path)] = base_before
            row = {"name": path.name, "base_generation": base_before}
            native_tail, pinned_tail = hot_tail_path(path), hot_tail_path(private / path.name)
            row["tail_present"] = native_tail.is_file()
            if row["tail_present"]:
                tail_before = identity(native_tail)
                shutil.copyfile(native_tail, pinned_tail)
                if identity(native_tail) != tail_before:
                    raise OSError("tail changed while pinning exact input")
                row.update(tail_generation=tail_before, pinned_tail_sha256=sha(pinned_tail))
            if identity(path) != base_before:
                raise OSError("base changed while pinning exact input")
            scope.append(row)
        context[provider] = scope
    atomic_write_json(output / "fixed-inputs.json", context, durable=True)
    preparation_seconds = time.monotonic() - preparation
    modules = {p: load_reference(reference_dir / f"reference_{p}.py", p) for p in PROVIDERS}
    references, candidates = {}, {}
    for provider, module in modules.items():
        name = "_load_logical_existing_info" if provider == "binance" else "_load_logical_existing_candle_info"
        references[provider] = getattr(module, name)
        candidates[provider] = prototype(module, references[provider])
    measurements, expected = [], None
    with ExitStack() as guards:
        import requests
        guards.enter_context(patch.object(requests.Session, "request", side_effect=AssertionError("provider forbidden")))
        for module in modules.values():
            guards.enter_context(patch.object(module, "urlopen", side_effect=AssertionError("provider forbidden")))
            guards.enter_context(patch.object(module, "_write_parquet", side_effect=AssertionError("source write forbidden"))
                                 if hasattr(module, "_write_parquet") else patch.object(
                                     module, "atomic_write_parquet", side_effect=AssertionError("source write forbidden")))
        for number, variant in enumerate(("reference", "candidate", "candidate", "reference")):
            for text, expected_generation in base_guards.items():
                if identity(Path(text)) != expected_generation:
                    raise OSError("native base changed during complete comparison")
            functions = references if variant == "reference" else candidates
            started, cpu = time.monotonic(), time.process_time()
            inventories, timings = [], []
            for phase in ("initial", "final"):
                whole = {}
                for provider, function in functions.items():
                    begin = time.monotonic()
                    rows = []
                    for path in sorted(private_roots[provider].glob("*_features.parquet")):
                        kwargs = {"require_contiguous": False} if provider == "bybit" else {}
                        rows.append({"name": path.name, **asdict(function(path, **kwargs))})
                    whole[provider] = rows
                    timings.append({"provider": provider, "phase": phase, "wall_seconds": time.monotonic() - begin})
                atomic_write_json(output / f"inventory-{number}-{phase}.json", whole, durable=True)
                inventories.append(whole)
            row = {"variant": variant, "wall_seconds": time.monotonic() - started,
                   "cpu_seconds": time.process_time() - cpu,
                   "output_sha256": digest(inventories), "provider_timings": timings}
            if expected is None:
                expected = row["output_sha256"]
            elif row["output_sha256"] != expected:
                raise AssertionError("complete source-information output differs")
            measurements.append(row)
            print(json.dumps(row), flush=True)
    for text, expected_generation in base_guards.items():
        if identity(Path(text)) != expected_generation:
            raise OSError("source base changed after complete comparison")
    for provider, scope in context.items():
        for row in scope:
            if row["tail_present"] and sha(private_roots[provider] / "_hot_tail" / row["name"]) != row["pinned_tail_sha256"]:
                raise OSError("pinned tail input changed")
    if {str(path): sha(path) for path in code_paths} != code_hashes:
        raise OSError("benchmark code changed during comparison")
    medians = {variant: {axis: statistics.median(row[axis] for row in measurements if row["variant"] == variant)
                        for axis in ("wall_seconds", "cpu_seconds")} for variant in ("reference", "candidate")}
    result = {"contract": "complete_cex_tail_information_roundtrip_v1", "state": "accepted_output_parity",
              "observed_at_utc": datetime.now(UTC).isoformat(), "measurements": measurements, "medians": medians,
              "workflow_speedup": medians["reference"]["wall_seconds"] / medians["candidate"]["wall_seconds"],
              "output_sha256": expected, "code_sha256": code_hashes, "input_context_sha256": digest(context),
              "symbols_by_provider": {p: len(rows) for p, rows in context.items()},
              "tail_bytes_pinned": tail_bytes, "preparation_seconds": preparation_seconds,
              "provider_calls": 0, "native_source_writes": 0, "all_base_generations_checked": True,
              "all_historical_values_rehashed": False, "complete_api_workflow_measured": False,
              "full_native_atomic_snapshot_claim": False, "os_page_cache_cleared": False, "exclusive_host": False}
    atomic_write_json(output / "comparison.json", result, durable=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = benchmark(args.reference_dir, args.output)
    print(json.dumps({k: result[k] for k in ("state", "symbols_by_provider", "medians", "workflow_speedup")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
