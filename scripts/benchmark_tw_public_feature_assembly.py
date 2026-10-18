#!/usr/bin/env python3
"""Private full TW-public builder trials, without providers or publication.

Retain a coherent byte-for-byte source copy and the original builder. Candidate
merge plans are isolated here until whole-output parity and full-workflow timing
justify a production change. This is not remote training derivation or storage
retirement evidence. Never publishes, deletes originals, or disables source gates.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time
import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def _candidate_merge(module, engine: str):
    """Keep first-frame values, all keys, source normalization and raw dtypes."""
    import polars as pl

    original = module._merge_feature_frames

    def merge(frames, *, stage_seconds=None, stage_prefix="join"):
        frames = list(frames)
        # Leave the canonical small/sparse path unchanged. Do not introduce
        # suffix or key-preservation differences into full outer joins.
        nonempty = [frame for frame in frames if frame is not None and not frame.is_empty()]
        if len(nonempty) < 3 or nonempty[0].height < 100_000:
            return original(frames, stage_seconds=stage_seconds, stage_prefix=stage_prefix)
        cleaned = []
        for index, frame in enumerate(frames):
            if frame is None or frame.is_empty():
                continue
            started = time.perf_counter()
            prepared = module._finalize_feature_frame(frame)
            if stage_seconds is not None:
                stage_seconds[f"{stage_prefix}_prepare_{index}"] = round(time.perf_counter()-started, 3)
            if not prepared.is_empty():
                cleaned.append(prepared)
        if len(cleaned) < 3 or cleaned[0].height < 100_000:
            return original(frames, stage_seconds=stage_seconds, stage_prefix=stage_prefix)
        # Do not silently accept a schema that the original plan rejects.
        # A third repetition can collide with an existing *_right column.
        original_columns = set(module.KEY_COLUMNS)
        for frame in cleaned:
            for name in frame.columns:
                if name in module.KEY_COLUMNS:
                    continue
                result_name = f"{name}_right" if name in original_columns else name
                if result_name in original_columns:
                    return original(frames, stage_seconds=stage_seconds, stage_prefix=stage_prefix)
                original_columns.add(result_name)
        started = time.perf_counter()
        keys = pl.concat([frame.select(list(module.KEY_COLUMNS)) for frame in cleaned]).unique(
            subset=list(module.KEY_COLUMNS)
        )
        if stage_seconds is not None:
            stage_seconds[f"{stage_prefix}_keys"] = round(time.perf_counter()-started, 3)
        merged = keys.lazy() if engine == "streaming" else keys
        seen = set(module.KEY_COLUMNS)
        for index, frame in enumerate(cleaned):
            columns = [name for name in frame.columns if name in module.OUTPUT_COLUMNS and name not in seen]
            # The union already retained this frame's keys. Every repeated
            # value column is ignored by the original final select, even if
            # the earlier value is null or this frame has a previously unseen
            # key. Avoid allocating discarded *_right buffers in both cases.
            if not columns:
                continue
            joined_at = time.perf_counter()
            selected = frame.select([*module.KEY_COLUMNS, *columns])
            merged = merged.join(
                selected.lazy() if engine == "streaming" else selected,
                on=list(module.KEY_COLUMNS), how="left", coalesce=True,
            )
            seen.update(columns)
            if stage_seconds is not None:
                suffix = "plan" if engine == "streaming" else "join"
                stage_seconds[f"{stage_prefix}_{suffix}_{index}"] = round(time.perf_counter()-joined_at, 3)
        if engine == "streaming":
            started = time.perf_counter()
            merged = merged.collect(engine="streaming")
            if stage_seconds is not None:
                stage_seconds[f"{stage_prefix}_materialize"] = round(time.perf_counter()-started, 3)
        return merged

    return merge


def capture(output: Path, source: Path, symbols: Path) -> None:
    from downloader.artifact_io import atomic_write_json
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
    from stockagent.data.tw_public_features import _symbol_universe_receipt
    from stockagent.data_sync.benchmark_fixtures import ImmutableFixtureInputs

    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("capture requires a new private artifacts/benchmarks directory")
    paths = sorted(source.glob("*.parquet"))
    supplemental = source / "supplemental/cbc_overnight_official_pages.parquet"
    if supplemental.is_file():
        paths.append(supplemental)
    size = sum(path.stat().st_size for path in paths)
    if not paths or shutil.disk_usage(output.parent).free < size + 15 * 1024**3:
        raise ValueError("insufficient private scratch for retained source and full trials")
    copied = []
    with live_tw_public_source_read_guard(source / "stocks"):
        # Busy native producers must not create another abandoned fixture root.
        output.mkdir(mode=0o700)
        with ImmutableFixtureInputs(output) as inputs:
            names_before = _symbol_universe_receipt(symbols)
            current_paths = sorted(source.glob("*.parquet"))
            if supplemental.is_file():
                current_paths.append(supplemental)
            if current_paths != paths:
                raise RuntimeError("source inventory changed before coherent capture")
            for path in paths:
                relative = path.relative_to(source)
                target = output / "source" / relative
                item = inputs.copy(path, f"source/{relative.as_posix()}")
                before = item["sha256"]
                if before != file_hash(target) or before != file_hash(path):
                    raise RuntimeError("source changed or private copy differs")
                copied.append({"name": relative.as_posix(), "size": target.stat().st_size, "sha256": before})
            if names_before != _symbol_universe_receipt(symbols):
                raise RuntimeError("symbol universe changed during coherent capture")
            original = ROOT / "stockagent/data/tw_public_features.py"
            shutil.copyfile(original, output / "original_features.py")
            (output / "original_features.py").chmod(0o400)
            if file_hash(original) != file_hash(output / "original_features.py"):
                raise RuntimeError("builder changed during capture")
            storage = inputs.commit()
    atomic_write_json(output / "fixture.json", {
        "contract": "tw_public_private_full_builder_fixture_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_files": copied, "source_bytes": size, "original_code_sha256": file_hash(original),
        "symbols_root": str(symbols.resolve()), "symbol_universe": names_before,
        "production_writes": False, "providers_called": 0,
        "storage_build": {key: value for key, value in storage.items() if key != "files"},
    })
    print(json.dumps({"event": "private_source_captured", "files": len(copied), "bytes": size}), flush=True)


def _verified_fixture(root: Path):
    fixture = json.loads((root / "fixture.json").read_bytes())
    if fixture.get("contract") != "tw_public_private_full_builder_fixture_v1":
        raise ValueError("unrecognized private source fixture")
    for item in fixture["source_files"]:
        path = root / "source" / item["name"]
        if not path.resolve().is_relative_to((root / "source").resolve()):
            raise ValueError("fixture path escapes private source")
        if path.stat().st_size != item["size"] or file_hash(path) != item["sha256"]:
            raise RuntimeError("private source generation changed")
    if file_hash(root / "original_features.py") != fixture["original_code_sha256"]:
        raise RuntimeError("original builder changed")
    return fixture


def worker(root: Path, output: Path, variant: str, end: date, threads: int) -> None:
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    pools = configure_cpu_source_runtime(threads)
    import pyarrow as pa
    import polars as pl
    import requests
    from downloader.artifact_io import atomic_write_json
    from stockagent.data.tw_public_features import _symbol_universe_receipt
    pa.set_cpu_count(pools["effective_threads"])
    fixture = _verified_fixture(root)
    symbols = Path(fixture["symbols_root"])
    if _symbol_universe_receipt(symbols) != fixture["symbol_universe"]:
        raise RuntimeError("live symbol-name filter differs from the captured universe")
    if output.exists():
        raise ValueError("trial requires a fresh output directory")
    output.mkdir(mode=0o700)
    code = (root / "original_features.py").read_bytes()
    module = types.ModuleType("stockagent.data._tw_public_features_private_original")
    module.__file__ = str(root / "original_features.py")
    sys.modules[module.__name__] = module
    exec(compile(code, module.__file__, "exec"), module.__dict__)
    if variant != "original":
        module._merge_feature_frames = _candidate_merge(module, variant)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("private builder reached a provider")

    requests.Session.request = forbidden
    import resource
    cpu_before = time.process_time()
    started = time.perf_counter()
    result = module.build_tw_public_training_features(
        input_dir=root / "source", output_path=output / "features.parquet", symbols_root=symbols,
        summary_path=output / "features.summary.json", end_date=end,
    )
    wall, cpu = time.perf_counter()-started, time.process_time()-cpu_before
    peak = next(int(line.split()[1])*1024 for line in Path("/proc/self/status").read_text().splitlines()
                if line.startswith("VmHWM:"))
    if "torch" in sys.modules or _symbol_universe_receipt(symbols) != fixture["symbol_universe"]:
        raise RuntimeError("model runtime imported or universe changed during full build")
    _verified_fixture(root)
    receipt = {
        "state": "completed_trial", "variant": variant, "wall_seconds": wall, "cpu_seconds": cpu,
        "rss_peak_bytes": peak, "rusage_informational_maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "rows": result.rows, "stock_rows": result.stock_rows, "market_rows": result.market_rows,
        "features": result.feature_count, "build_mode": result.build_mode,
        "output_receipt": result.output_receipt, "source_receipts": result.source_receipts,
        "symbol_universe_receipt": result.symbol_universe_receipt,
        "stage_elapsed_seconds": result.stage_elapsed_seconds,
        "polars_threads": pl.thread_pool_size(), "arrow_threads": pa.cpu_count(),
        "pool_profile": pools, "original_code_sha256": fixture["original_code_sha256"],
        "harness_code_sha256": file_hash(Path(__file__)), "model_runtime_imported": False,
        "provider_calls": 0, "production_writes": False,
    }
    atomic_write_json(output / "receipt.json", receipt)
    print(json.dumps({key: receipt[key] for key in ("variant", "wall_seconds", "cpu_seconds", "rss_peak_bytes", "rows", "features")}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--source", type=Path, default=ROOT / "data_tw_public")
    parser.add_argument("--symbols", type=Path, default=ROOT / "data_tw_public/stocks")
    parser.add_argument("--variant", choices=("original", "eager", "streaming"), default="original")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    if args.capture:
        capture(args.fixture.absolute(), args.source.resolve(), args.symbols.resolve())
        return
    if args.output is None or args.end_date is None:
        parser.error("trial requires --output and --end-date")
    output = args.output.absolute()
    if output.exists() or not output.resolve().is_relative_to(args.fixture.resolve()) or output.resolve().is_relative_to((args.fixture / "source").resolve()):
        parser.error("trial requires a new output below fixture, outside its source")
    worker(args.fixture.resolve(), output, args.variant, args.end_date, args.threads)


if __name__ == "__main__":
    main()
