"""Compare bounded tail-read candidates on the five-mode canonical BASE panel.

Freeze all stock files, external data and corporate-action receipt dependencies.
Workers call the existing live base-panel owner, including memory reuse and
private disk-cache publication. Factorized attachment, checkpoint/model/quotes,
signals, orders and CUDA are explicitly outside this CPU-only denominator.
Candidates change a private reader copy or reuse lossless raw Arrow tables;
neither mutates the live reader. The original complete oracle is retained.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from dataclasses import asdict, fields
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file  # noqa: E402
from scripts.benchmark_ohlcv_tail_reader import READER, _generation, threshold_candidate  # noqa: E402
from scripts.benchmark_project_data_processing import load_baseline, measured  # noqa: E402
from stockagent.remote_build import observe_node  # noqa: E402

CONTRACT = "five_mode_complete_live_base_panel_cutoff_v1"
SOURCE_CACHE = ROOT / "downloader/live_tail_source_cache.py"
ORIGINAL_CALLERS_HELPER = ROOT / "scripts/canonical_live_tail_cache_oracle.py"
MODE_CONFIGS = {
    "v8": "configs/deployments/tw_day_trade_v8_annual_log_cash_fold10.yaml",
    "multi22": "configs/deployments/tw_day_trade_multi_basis_22_fold11.yaml",
    "multi": "configs/deployments/tw_day_trade_multi_basis_fold11.yaml",
    "100m": "configs/deployments/tw_day_trade_100m_fold11.yaml",
    "factorized_base": "configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_live_v1.yaml",
}
CODE = [READER, Path(__file__), ROOT / "scripts/benchmark_ohlcv_tail_reader.py",
        ROOT / "scripts/benchmark_project_data_processing.py", ROOT / "stockagent/config.py",
        ROOT / "stockagent/data/panel.py", ROOT / "stockagent/data/panel_cache.py",
        ROOT / "stockagent/data/panel_numba.py", ROOT / "stockagent/data/crypto_lifecycle.py",
        ROOT / "stockagent/live/signal_engine.py", ROOT / "stockagent/live/tw_public_source_guard.py",
        SOURCE_CACHE]


def new_path(path: Path) -> Path:
    path = path.absolute()
    if (path.exists() or path.is_symlink()
            or not path.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve())):
        raise ValueError("new private evidence under artifacts/benchmarks required")
    return path


def recipes() -> list[dict]:
    from stockagent.config import load_config
    return [{"mode": mode, "data": asdict(config.data), "lookback": config.training.lookback,
             "strict_no_fallback": config.training.strict_no_fallback}
            for mode, relative in MODE_CONFIGS.items() for config in [load_config(ROOT / relative)]]


def source_members(definitions: list[dict]) -> tuple[Path, list[Path]]:
    from stockagent.config import DataConfig, external_panel_data_kwargs
    from stockagent.data.panel import _resolve_corporate_action_reference_paths
    roots = {Path(row["data"]["parquet_root"]).resolve() for row in definitions}
    if len(roots) != 1:
        raise ValueError("five modes no longer share one canonical stock source")
    stocks = roots.pop()
    root = stocks.parent
    if root.name != "data_tw_public" or stocks.name != "stocks":
        raise ValueError("preserve official TW path semantics in the frozen layout")
    members = set(stocks.glob("*_features.parquet"))
    if not members:
        raise ValueError("missing full stock universe")
    members.update((stocks / "_hot_tail").glob("*_features.parquet"))
    if (stocks / "symbols.csv").is_file():
        members.add(stocks / "symbols.csv")
    for row in definitions:
        data = DataConfig(**row["data"])
        if data.security_filter not in {None, "none"} or data.overnight_1325_root:
            raise ValueError("new security/overnight dependencies require explicit fixture support")
        kwargs = external_panel_data_kwargs(data)
        external = Path(str(kwargs["external_feature_path"])).resolve()
        if not external.is_relative_to(root):
            raise ValueError("external data belongs to a different source tree")
        members.add(external)
        summary = external.with_suffix(".summary.json")
        if summary.is_file():
            members.add(summary)
        reference = _resolve_corporate_action_reference_paths(
            external, include_rules=bool(kwargs["external_include_rules"]))
        if reference is not None:
            members.update(getattr(reference, field.name) for field in fields(reference)
                           if getattr(reference, field.name) is not None)
            if reference.entitlements_summary is not None:
                receipt = json.loads(reference.entitlements_summary.read_bytes())
                raw = (reference.entitlements_summary.parent /
                       receipt["raw_receipt_manifest"]["relative_path"]).resolve()
                if not raw.is_relative_to(reference.entitlements_summary.parent.resolve()):
                    raise ValueError("raw entitlement manifest escapes its canonical receipt root")
                members.add(raw)
    if any(not path.is_file() or not path.resolve().is_relative_to(root) for path in members):
        raise ValueError("unresolved source or receipt dependency")
    return root, sorted(members)


def prepare(fixture: Path) -> dict:
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
    fixture = new_path(fixture)
    codes = {str(path): sha256_file(path) for path in CODE}
    definitions = recipes()
    root, members = source_members(definitions)
    size = sum(path.stat().st_size for path in members)
    if shutil.disk_usage(fixture.parent).free < size + 30 * 1024**3:
        raise ValueError("preserve 30 GiB reserve after freezing the full workload")
    fixture.mkdir(mode=0o700)
    reference = fixture / "baseline.py"
    atomic_write_bytes(reference, READER.read_bytes(), mode=0o400)
    proofs = []
    with live_tw_public_source_read_guard(root / "stocks"):
        before = {str(path): _generation(path) for path in members}
        for path in members:
            relative = path.relative_to(root)
            copy = fixture / "data_tw_public" / relative
            copy.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(path, copy)
            digest = sha256_file(copy)
            if sha256_file(path) != digest:
                raise ValueError("canonical source changed during full fixture copy")
            copy.chmod(0o400)
            proofs.append({"path": str(copy), "source": str(path), "bytes": copy.stat().st_size,
                           "sha256": digest, "generation": before[str(path)]})
        if (before != {str(path): _generation(path) for path in members}
                or source_members(definitions) != (root, members)):
            raise ValueError("source or dependency membership changed while freezing")
    if definitions != recipes() or codes != {str(path): sha256_file(path) for path in CODE}:
        raise ValueError("effective config or code changed during full fixture capture")
    frozen = fixture / "data_tw_public"
    for row in definitions:
        data = row["data"]
        data["parquet_root"] = str(frozen / "stocks")
        for name in ("tw_public_feature_path", "external_feature_path"):
            if data.get(name) and Path(data[name]).resolve().is_relative_to(root):
                data[name] = str(frozen / Path(data[name]).resolve().relative_to(root))
    value = {"contract": CONTRACT, "state": "prepared_full_canonical_base_panel_fixture",
             "observed_at_utc": datetime.now(UTC).isoformat(), "recipes": definitions,
             "members": proofs, "source_bytes": size, "code_sha256": codes,
             "baseline_sha256": sha256_file(reference),
             "stock_files": sum(Path(row["path"]).parent == frozen / "stocks"
                                and row["path"].endswith("_features.parquet") for row in proofs)}
    atomic_write_json(fixture / "fixture.json", value)
    return value


def panel_identity(panel) -> dict:
    import numpy as np
    from stockagent.data.panel_cache import array_content_fingerprint
    value = {}
    for field in fields(panel):
        item = getattr(panel, field.name)
        value[field.name] = array_content_fingerprint(item) if isinstance(item, np.ndarray) else item
    # Unknown lazy objects must fail, not quietly disappear from the equality oracle.
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(payload).hexdigest(), "fields": value,
            "dates": panel.num_dates, "symbols": panel.num_symbols,
            "features": len(panel.feature_names)}


def fence(fixture: Path, manifest: dict) -> dict:
    if sha256_file(fixture / "baseline.py") != manifest["baseline_sha256"]:
        raise ValueError("original decoder reference changed")
    values = {row["path"]: sha256_file(Path(row["path"])) for row in manifest["members"]}
    if values != {row["path"]: row["sha256"] for row in manifest["members"]}:
        raise ValueError("complete source/receipt fixture changed")
    stocks = fixture / "data_tw_public/stocks"
    actual = set(stocks.glob("*_features.parquet")) | set((stocks / "_hot_tail").glob("*_features.parquet"))
    expected = {Path(row["path"]) for row in manifest["members"]
                if Path(row["path"]).parent in {stocks, stocks / "_hot_tail"}
                and row["path"].endswith("_features.parquet")}
    if actual != expected:
        raise ValueError("full frozen stock/hot-tail membership changed")
    return values


def child(config: dict) -> dict:
    import torch
    import polars as pl
    import pyarrow as pa
    from downloader import ohlcv_hot_tail as reader
    from stockagent.config import DataConfig
    def forbidden(*_args, **_kwargs):
        raise AssertionError("CPU base-panel benchmark must not call CUDA")

    for name in ("_lazy_init", "is_available", "synchronize", "empty_cache"):
        setattr(torch.cuda, name, forbidden)
    from stockagent.live import signal_engine as engine
    original_callers = config.get("original_callers")
    if original_callers:
        from scripts.canonical_live_tail_cache_oracle import original_callers_identity, original_stable_tail_builder
        if original_callers_identity(Path(original_callers)) != config["original_callers_identity"]:
            raise ValueError("original canonical callers changed before worker")
        if config["mode"] == "baseline":
            engine._build_panel_from_stable_source = original_stable_tail_builder(Path(original_callers))
    os.environ["STOCKAGENT_LIVE_RAW_TAIL_CACHE_BYTES"] = (
        str(config["source_cache_bytes"]) if config["mode"] == "canonical_table_cache_candidate" else "0")
    fixture = Path(config["fixture"])
    if sha256_file(fixture / "fixture.json") != config["fixture_sha256"]:
        raise ValueError("full workload manifest changed before worker")
    manifest = json.loads((fixture / "fixture.json").read_bytes())
    before = fence(fixture, manifest)
    baseline = load_baseline(fixture / "baseline.py")
    if config["mode"] == "cutoff_candidate":
        threshold_candidate(baseline, config["threshold"])
    reader._read_tail_row_groups = baseline._read_tail_row_groups
    source_cache = None
    if config["mode"] == "table_cache_candidate":
        from downloader.live_tail_source_cache import LiveTailSourceCache
        from stockagent.data import panel as panel_module
        source_cache = LiveTailSourceCache(max_bytes=config["source_cache_bytes"])
        def cached_symbol_tail(path, *, tail_rows, tradable_mode="tradable",
                               trading_volume_policy="auto", raw_input_features=False, raw_tail_reader=None):
            table = source_cache.read(path, tail_rows=tail_rows)
            return panel_module._symbol_arrays_from_arrow_table(table, path,
                tradable_mode=tradable_mode, trading_volume_policy=trading_volume_policy,
                raw_input_features=raw_input_features)
        panel_module._load_symbol_arrays_pyarrow_tail = cached_symbol_tail
    pa.set_cpu_count(config["threads"])
    pa.set_io_thread_count(config["threads"])
    cache = new_path(Path(config["cache"]))
    cache.mkdir(mode=0o700)
    os.environ["STOCKAGENT_LIVE_PANEL_CACHE_ROOT"] = str(cache)
    os.environ["STOCKAGENT_LIVE_PANEL_DISK_CACHE"] = "1"
    os.environ["STOCKAGENT_LIVE_PANEL_LOAD_WORKERS"] = str(config["workers"])
    engine.clear_live_panel_memory_cache()
    if config["mode"] == "canonical_table_cache_candidate":
        source_cache = engine._live_raw_tail_source_cache()

    def execute():
        results = []
        for row in manifest["recipes"]:
            recipe = SimpleNamespace(data=DataConfig(**row["data"]), training=SimpleNamespace(
                lookback=row["lookback"], strict_no_fallback=row["strict_no_fallback"]))
            panel, hit, tier = engine._build_panel_from_stable_source(recipe, live_tail=True)
            results.append({"mode": row["mode"], "cache_hit": hit, "tier": tier,
                            "panel": panel_identity(panel)})
        return results

    with redirect_stdout(sys.stderr):
        result = measured(execute)
    if (torch.cuda.is_initialized() or fence(fixture, manifest) != before
            or sha256_file(fixture / "fixture.json") != config["fixture_sha256"]):
        raise ValueError("GPU initialization or source drift in CPU-only benchmark")
    if original_callers and original_callers_identity(Path(original_callers)) != config["original_callers_identity"]:
        raise ValueError("original canonical callers changed during worker")
    result.update(polars_threads=pl.thread_pool_size(), arrow_threads=pa.cpu_count(),
                  panel_workers=config["workers"], cuda_initialized=False,
                  source_cache_stats=None if source_cache is None else source_cache.stats())
    return result


def independent_reference(receipt: Path, fixture: Path, manifest: dict, codes: dict,
                          original_callers: Path | None = None) -> dict:
    """Reuse the original full oracle after a benchmark-only extension.

    The saved original controller is bound to its accepted four-trial report.
    Only this controller and the new cache owner may differ; every canonical
    source/recipe/reader/panel/engine implementation remains the original one.
    """
    capture = json.loads(receipt.read_bytes())
    control_path, saved = Path(capture["control"]), Path(capture["saved"])
    control = json.loads(control_path.read_bytes())
    self_name = str(Path(__file__))
    if (capture.get("state") != "captured_accepted_base_panel_controller"
            or sha256_file(control_path) != capture["control_sha256"]
            or sha256_file(saved) != capture["saved_sha256"]
            or capture["source"] != self_name
            or capture["saved_sha256"] != control["code_sha256"][self_name]
            or control.get("state") != "accepted_complete_base_panel_only"
            or control.get("all_panel_fields_equal") is not True or len(control["trials"]) != 4
            or control["fixture"] != str(fixture)
            or control["fixture_sha256"] != sha256_file(fixture / "fixture.json")
            or manifest["code_sha256"] != control["code_sha256"]):
        raise ValueError("original complete panel controller/source/oracle identity changed")
    callers = None
    extensions = {str(SOURCE_CACHE)}
    allowed = {self_name}
    if original_callers is not None:
        from scripts.canonical_live_tail_cache_oracle import original_callers_identity, verify_unmodified_helpers
        callers = original_callers_identity(original_callers)
        verify_unmodified_helpers(callers)
        extensions.add(str(ORIGINAL_CALLERS_HELPER))
        allowed.update(callers["proofs"])
        allowed.add(str(SOURCE_CACHE))  # Scoped cache owner/comment identity is separately fenced.
    if set(codes) != set(control["code_sha256"]) | extensions:
        raise ValueError("unexpected code extension outside the scoped cache owner")
    if any(codes[name] != digest for name, digest in control["code_sha256"].items()
           if name not in allowed):
        raise ValueError("canonical computation changed since independent full oracle")
    expected = control["trials"][0]["result"]
    if any(trial["result"] != expected for trial in control["trials"]):
        raise ValueError("independent full original oracle is inconsistent")
    return {"receipt_sha256": sha256_file(receipt), "control": str(control_path),
            "control_sha256": capture["control_sha256"], "saved": str(saved),
            "saved_sha256": capture["saved_sha256"], "expected": expected,
            "original_callers": callers}


def benchmark(args) -> dict:
    output = new_path(args.output)
    fixture = args.fixture.resolve(strict=True)
    manifest = json.loads((fixture / "fixture.json").read_bytes())
    if manifest.get("contract") != CONTRACT or manifest.get("state") != "prepared_full_canonical_base_panel_fixture":
        raise ValueError("requires complete canonical base-panel fixture")
    callers = getattr(args, "original_callers", None)
    measured_code = CODE + ([ORIGINAL_CALLERS_HELPER] if callers is not None else [])
    codes = {str(path): sha256_file(path) for path in measured_code}
    reference = (independent_reference(args.reference, fixture, manifest, codes, callers)
                 if args.reference else None)
    if ((reference is None and manifest["code_sha256"] != codes)
            or codes[str(READER)] != manifest["baseline_sha256"]):
        raise ValueError("measured source changed since independent original capture")
    source_fence = fence(fixture, manifest)
    value = {"state": "running", "contract": CONTRACT, "fixture": str(fixture),
             "fixture_sha256": sha256_file(fixture / "fixture.json"), "code_sha256": codes,
             "stock_files": manifest["stock_files"], "source_bytes": manifest["source_bytes"],
             "threads": args.threads, "workers": args.workers, "threshold": args.threshold,
             "candidate": args.candidate, "source_cache_bytes": args.source_cache_bytes,
             "independent_reference": reference,
             "node_before": observe_node(fixture, output.parent), "trials": [],
             "provider_calls": 0, "services_changed": False, "production_writes": False,
             "cuda_calls": 0, "live_owner_adoption_verified": False,
             "scope": "five-mode complete canonical BASE panel preparation, memory reuse, private disk publication and all-field oracle; excludes factorized attachment/checkpoint/model/quotes/signals/trading",
             "cache_state": "fresh process and private disk panel cache; OS cache uncontrolled; nice 19 on a live host"}
    atomic_write_json(output, value)
    expected = None if reference is None else reference["expected"]
    candidate = {"table_cache": "table_cache_candidate", "canonical_table_cache": "canonical_table_cache_candidate",
                 "cutoff": "cutoff_candidate"}[args.candidate]
    try:
        for number, mode in enumerate(("baseline", candidate, candidate, "baseline")):
            config = {"fixture": str(fixture), "mode": mode, "threshold": args.threshold,
                      "threads": args.threads, "workers": args.workers,
                      "fixture_sha256": value["fixture_sha256"],
                      "source_cache_bytes": args.source_cache_bytes,
                      "cache": str(fixture / f"{output.stem}-trial-{number}-cache")}
            if callers is not None:
                config.update(original_callers=str(callers.resolve(strict=True)),
                              original_callers_identity=reference["original_callers"])
            process_started = time.perf_counter()
            cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            result = subprocess.run([sys.executable, str(Path(__file__)), "--child", json.dumps(config)],
                capture_output=True, text=True, timeout=600, cwd=ROOT,
                env={**os.environ, "POLARS_MAX_THREADS": str(args.threads),
                     "OMP_NUM_THREADS": str(args.threads), "OPENBLAS_NUM_THREADS": str(args.threads),
                     "MKL_NUM_THREADS": str(args.threads)})
            log = new_path(fixture / f"{output.stem}-trial-{number}.log")
            atomic_write_bytes(log, result.stderr.encode(), mode=0o600)
            if result.returncode:
                raise RuntimeError(f"complete base-panel worker {number} failed; retained private log")
            trial = json.loads(result.stdout)
            usage = resource.getrusage(resource.RUSAGE_CHILDREN)
            trial.update(process_wall_seconds=time.perf_counter() - process_started,
                         process_cpu_seconds=(usage.ru_utime + usage.ru_stime
                                              - cpu_before.ru_utime - cpu_before.ru_stime))
            if expected is None:
                expected = trial["result"]
            if trial["result"] != expected:
                raise ValueError("complete panel fields/order/masks/float bits or cache reuse differ")
            trial.update(mode=mode, trial=number)
            value["trials"].append(trial)
            atomic_write_json(output, value)
            print({"mode": mode, "trial": number, "wall": trial["workflow_seconds"],
                   "cpu": trial["cpu_seconds"], "peak_rss": trial["peak_rss_bytes"]}, flush=True)
        if (codes != {str(path): sha256_file(path) for path in measured_code}
                or fence(fixture, manifest) != source_fence
                or sha256_file(fixture / "fixture.json") != value["fixture_sha256"]):
            raise ValueError("code or complete workload changed during ABBA")
        value.update(state="accepted_complete_base_panel_only", all_panel_fields_equal=True,
            medians={mode: {field: statistics.median(trial[field] for trial in value["trials"]
                if trial["mode"] == mode) for field in ("workflow_seconds", "cpu_seconds", "peak_rss_bytes",
                                                        "process_wall_seconds", "process_cpu_seconds")}
                for mode in ("baseline", candidate)}, node_after=observe_node(fixture, output.parent))
    except Exception as exc:
        value.update(state="failed", error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        atomic_write_json(output, value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--threads", type=int, choices=range(1, 33), default=2)
    parser.add_argument("--workers", type=int, choices=range(1, 17), default=8)
    parser.add_argument("--threshold", type=int, default=256)
    parser.add_argument("--candidate", choices=("cutoff", "table_cache", "canonical_table_cache"), default="cutoff")
    parser.add_argument("--source-cache-bytes", type=int, default=64 * 1024**2)
    parser.add_argument("--reference", type=Path, help="retained accepted original controller/oracle")
    parser.add_argument("--original-callers", type=Path, help="immutable actual original engine/panel caller receipt")
    parser.add_argument("--child")
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(args.child))))
    elif args.fixture is None or args.threshold < 0 or args.source_cache_bytes < 0:
        parser.error("explicit fixture and nonnegative cutoff required")
    elif args.prepare:
        value = prepare(args.fixture)
        print({"state": value["state"], "stock_files": value["stock_files"], "bytes": value["source_bytes"]})
    elif args.output is None:
        parser.error("new output required")
    elif args.candidate in {"table_cache", "canonical_table_cache"} and args.reference is None:
        parser.error("raw cache probe requires the independent original full oracle")
    elif args.candidate == "canonical_table_cache" and args.original_callers is None:
        parser.error("canonical raw cache probe requires the saved actual original callers")
    else:
        print(json.dumps(benchmark(args)["medians"]))


if __name__ == "__main__":
    main()
