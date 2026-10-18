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
import fcntl
import importlib.util
import inspect
import json
import os
from pathlib import Path
import shutil
import stat
import statistics
import sys
import time
from unittest.mock import patch

import pyarrow as pa
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "downloader"))

from downloader.artifact_io import atomic_write_json
from downloader.ohlcv_hot_tail import canonical_date_frame, hot_tail_path, tail_newer_summary
from stockagent.data_sync.benchmark_fixtures import CONTRACT as FIXTURE_CONTRACT, RECEIPT as FIXTURE_RECEIPT, ImmutableFixtureInputs
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash

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


def normalization_prototype(module, provider: str):
    """Preserve Binance repair priority and all noncanonical legacy behavior."""
    original = module._normalize_date_frame

    def normalize(frame):
        if provider != "binance":
            reused = canonical_date_frame(frame)
            if reused is not None:
                return reused
        return original(frame)

    return normalize


def frame_digest(frame) -> str:
    """Hash typed values, not string-view buffers or undefined NULL payloads.

    Keep every row's validity plus all *valid* value bits. Arrow permits garbage
    in null slots and unused bitmap bits, so hashing IPC storage is not a value
    oracle. Valid float NaN payloads and signed zero are deliberately unchanged.
    Unsupported nested/extension types fail closed instead of losing schema.
    """
    table = frame.rechunk().to_arrow()
    result = hashlib.sha256()

    def add(data):
        result.update(len(data).to_bytes(8, "little"))
        result.update(data)

    add(json.dumps([(name, str(dtype)) for name, dtype in frame.schema.items()]).encode())
    for column in table.columns:
        array = column.combine_chunks()
        add(array.is_valid().to_numpy(zero_copy_only=False).tobytes())
        values = array.drop_null()
        dtype = values.type
        if not len(values) or pa.types.is_null(dtype):
            add(b"")
        elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
            width = 8 if pa.types.is_large_string(dtype) or pa.types.is_large_binary(dtype) else 4
            offsets = np.frombuffer(values.buffers()[1], dtype=f"<i{width}", count=len(values) + 1,
                                    offset=values.offset * width)
            first, last = int(offsets[0]), int(offsets[-1])
            add((offsets - first).tobytes())
            add(memoryview(values.buffers()[2])[first:last] if last > first else b"")
        elif pa.types.is_boolean(dtype):
            add(values.to_numpy(zero_copy_only=False).tobytes())
        elif (pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_temporal(dtype)
              or pa.types.is_decimal(dtype) or pa.types.is_fixed_size_binary(dtype)):
            width = dtype.byte_width if pa.types.is_fixed_size_binary(dtype) else dtype.bit_width // 8
            start = values.offset * width
            add(memoryview(values.buffers()[1])[start:start + len(values) * width])
        else:
            raise TypeError(f"unsupported complete value-digest dtype: {dtype}")
    return result.hexdigest()


def benchmark_normalization(reference_dir: Path, output: Path, *, input_fixture: Path | None = None) -> dict:
    """Read/normalize/merge/hash every stored native tail, with identical inputs.

    Freeze mutable files using the shared immutable-fixture owner; never link
    the running writer's inode. Historical bases are inventoried, not copied:
    this phase only merges recent tails. All value columns participate in the
    comparison. Fresh overlaps are existing observed terminal rows, not fake
    candles. This is not timing provider calls or whole collector lifecycles.
    """
    reference_dir, output = reference_dir.resolve(), output.resolve()
    roots = {p: ROOT / f"data_{p}" / "1m" for p in PROVIDERS}
    if output.exists() or not output.parent.is_dir():
        raise ValueError("fresh output and existing parent required")
    for root in roots.values():
        resolved = root.resolve()
        if output == resolved or resolved in output.parents or output in resolved.parents:
            raise ValueError("private benchmark must not overlap source")
    paths = {p: sorted(root.glob("*_features.parquet")) for p, root in roots.items()}
    if any(not members for members in paths.values()):
        raise ValueError("all three complete source universes required")
    tails = {p: [hot_tail_path(path) for path in members if hot_tail_path(path).is_file()]
             for p, members in paths.items()}
    total_bytes = sum(path.stat().st_size for members in tails.values() for path in members)
    if shutil.disk_usage(output.parent).free < 32 * 1024**3 + (2 * total_bytes if input_fixture is None else 0):
        raise RuntimeError("private tail snapshot exceeds disk headroom")
    code_paths = [Path(__file__), ROOT / "downloader/ohlcv_hot_tail.py", *(
        reference_dir / f"reference_{p}.py" for p in PROVIDERS)]
    code_hashes = {str(path): sha(path) for path in code_paths}
    output.mkdir(mode=0o700)
    started = time.monotonic()
    if input_fixture is None:
        input_fixture = output
        with ImmutableFixtureInputs(output) as capture:
            for provider, members in tails.items():
                for path in members:
                    capture.copy(path, f"{provider}/{path.name}")
            inputs = capture.commit()
    else:
        input_fixture = input_fixture.absolute()
        if input_fixture.resolve(strict=True) != input_fixture:
            raise ValueError("immutable input fixture must not be redirected")
        receipt = input_fixture / FIXTURE_RECEIPT
        if receipt.is_symlink() or receipt.stat().st_size > 8 * 1024**2:
            raise ValueError("bounded committed fixture required")
        inputs = json.loads(receipt.read_text())
        expected_names = {f"{p}/{path.name}" for p, members in tails.items() for path in members}
        actual_names = [item["name"] for item in inputs.get("files", [])]
        if (inputs.get("contract") != FIXTURE_CONTRACT or inputs.get("state") != "committed_inputs"
                or len(set(actual_names)) != len(actual_names) or set(actual_names) != expected_names):
            raise ValueError("exact complete current stored-tail member set required")
    total_bytes = sum(item["size"] for item in inputs["files"])
    preparation_seconds = time.monotonic() - started
    modules = {p: load_reference(reference_dir / f"reference_{p}.py", p) for p in PROVIDERS}
    originals = {p: module._normalize_date_frame for p, module in modules.items()}
    candidates = {p: normalization_prototype(module, p) for p, module in modules.items()}
    measurements, expected = [], None
    with ExitStack() as guards:
        # Reuse a sealed capture after a harness failure without copying live
        # tails again or overwriting any old trial result. Hold its shared lock.
        descriptor = os.open(input_fixture / ".immutable-inputs.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        lock = guards.enter_context(os.fdopen(descriptor, "rb"))
        lock_info = os.fstat(descriptor)
        if not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1:
            raise ValueError("unsafe immutable input lock")
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        input_receipt_sha = sha(input_fixture / FIXTURE_RECEIPT)
        if json.loads((input_fixture / FIXTURE_RECEIPT).read_text()) != inputs:
            raise ValueError("immutable input receipt changed before admission")
        input_guards = {}
        for item in inputs["files"]:
            path = input_fixture / item["name"]
            if path.resolve(strict=True) != path:
                raise ValueError("redirected immutable input")
            info = payload_info(path)
            if info["mode"] & 0o222 or info["size"] != item["size"] or stable_payload_hash(path, info) != item["sha256"]:
                raise ValueError("immutable input differs from its complete receipt")
            input_guards[str(path)] = info
        import requests
        guards.enter_context(patch.object(requests.Session, "request", side_effect=AssertionError("provider forbidden")))
        for module in modules.values():
            guards.enter_context(patch.object(module, "urlopen", side_effect=AssertionError("provider forbidden")))
            writer = "_write_parquet" if hasattr(module, "_write_parquet") else "_write_parquet_atomic"
            guards.enter_context(patch.object(module, writer, side_effect=AssertionError("source write forbidden")))
        for number, variant in enumerate(("reference", "candidate", "candidate", "reference")):
            wall, cpu = time.monotonic(), time.process_time()
            inventory, timings = {}, {}
            for provider, module in modules.items():
                start = time.monotonic()
                rows = []
                normalizer = originals[provider] if variant == "reference" else candidates[provider]
                with patch.object(module, "_normalize_date_frame", normalizer):
                    for path in tails[provider]:
                        frame = module._read_parquet(input_fixture / provider / path.name)
                        # Input preparation deliberately uses the original rule
                        # for both arms; no candidate can change the test data.
                        fresh = originals[provider](frame.tail(3))
                        if fresh.height:
                            cutoff = int(datetime.strptime(fresh["date"][0], "%Y-%m-%d %H:%M:%S")
                                         .replace(tzinfo=UTC).timestamp() * 1000)
                        else:
                            cutoff = 0
                        merged, changed = module._merge_existing_with_fresh(frame, fresh, cutoff)
                        rows.append({"name": path.name, "rows": merged.height, "changed": changed,
                                     "complete_schema_values_sha256": frame_digest(merged)})
                inventory[provider] = rows
                timings[provider] = time.monotonic() - start
                print(json.dumps({"stage": "complete_provider_tail_merge", "variant": variant,
                                  "provider": provider, "files": len(rows),
                                  "wall_seconds": timings[provider]}), flush=True)
            row = {"variant": variant, "wall_seconds": time.monotonic() - wall,
                   "cpu_seconds": time.process_time() - cpu, "provider_wall_seconds": timings,
                   "output_sha256": digest(inventory)}
            if expected is None:
                expected = row["output_sha256"]
            elif expected != row["output_sha256"]:
                atomic_write_json(output / f"rejected-inventory-{number}.json", inventory, durable=True)
                raise AssertionError("complete merged values/schema/changed output differs")
            atomic_write_json(output / f"merge-inventory-{number}.json", inventory, durable=True)
            measurements.append(row)
            print(json.dumps(row), flush=True)
        for item in inputs["files"]:
            path = input_fixture / item["name"]
            if payload_info(path) != input_guards[str(path)] or sha(path) != item["sha256"]:
                raise OSError("pinned complete tail changed")
        if sha(input_fixture / FIXTURE_RECEIPT) != input_receipt_sha:
            raise OSError("immutable input receipt changed")
    if {str(path): sha(path) for path in code_paths} != code_hashes:
        raise OSError("benchmark code changed during comparison")
    medians = {variant: {axis: statistics.median(row[axis] for row in measurements if row["variant"] == variant)
                        for axis in ("wall_seconds", "cpu_seconds")} for variant in ("reference", "candidate")}
    result = {"contract": "complete_cex_stored_tail_merge_normalization_v3", "state": "accepted_output_parity",
              "value_digest_format": "typed_validity_and_exact_valid_value_bits_v1",
              "observed_at_utc": datetime.now(UTC).isoformat(), "measurements": measurements, "medians": medians,
              "workflow_speedup": medians["reference"]["wall_seconds"] / medians["candidate"]["wall_seconds"],
              "output_sha256": expected, "code_sha256": code_hashes,
              "stored_symbols_by_provider": {p: len(members) for p, members in paths.items()},
              "symbols_by_provider": {p: len(members) for p, members in tails.items()},
              "tail_bytes_pinned": total_bytes, "preparation_seconds": preparation_seconds,
              "immutable_inputs_sha256": digest(inputs), "source_writes": 0, "provider_calls": 0,
              "input_fixture": str(input_fixture), "input_receipt_sha256": input_receipt_sha,
              "binance_normalizer_unchanged": True, "complete_api_workflow_measured": False,
              "os_page_cache_cleared": False, "exclusive_host": False}
    atomic_write_json(output / "comparison.json", result, durable=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--operation", choices=("overlap", "normalization"), default="overlap")
    parser.add_argument("--input-fixture", type=Path, help="Reuse a complete committed normalization input capture, read-only.")
    args = parser.parse_args()
    if args.input_fixture is not None and args.operation != "normalization":
        parser.error("--input-fixture requires --operation normalization")
    result = (benchmark_normalization(args.reference_dir, args.output, input_fixture=args.input_fixture)
              if args.operation == "normalization" else benchmark(args.reference_dir, args.output))
    print(json.dumps({k: result[k] for k in ("state", "symbols_by_provider", "medians", "workflow_speedup")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
