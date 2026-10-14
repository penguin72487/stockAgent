"""Freeze and benchmark the whole canonical local factorized preparation owner.

This companion includes source receipt checks, all normalization, executable
base build, causal projection, validation and private atomic publication. It
reuses saved pre-edit implementations and never updates the live projection.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, timedelta
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    # systemd's agent launcher intentionally forwards only selected runtime
    # variables. Apply the measured worker profile HERE, before numpy/polars
    # imports, instead of assuming exported shell pool settings reach a worker.
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    configure_cpu_source_runtime(2)

from downloader.artifact_io import atomic_write_bytes, atomic_write_json  # noqa: E402
from scripts.benchmark_factorized_live_projection import (  # noqa: E402
    candidate_sources, new_private, private_cache_path, private_copy, verify_fixture,
)
from scripts.benchmark_live_base_panel_cutoff import panel_identity, source_members  # noqa: E402
from scripts.benchmark_project_data_processing import load_baseline, measured  # noqa: E402
from stockagent.data.factorized_panel import file_sha256, verify_factorized_members  # noqa: E402

CONTRACT = "complete_factorized_live_owner_benchmark_v1"
SHARED_CODE = [ROOT / path for path in (
    "scripts/prepare_tw_day_trade_factorized_panel.py", "scripts/prepare_tw_day_trade_factorized_live.py",
    "scripts/benchmark_factorized_live_projection.py", "scripts/benchmark_factorized_live_owner.py",
    "scripts/benchmark_live_base_panel_cutoff.py", "scripts/benchmark_project_data_processing.py",
    "scripts/build_tw_release_schedule_dataset.py", "stockagent/config.py", "train.py",
    "stockagent/data/panel.py", "stockagent/data/panel_cache.py", "stockagent/data/panel_numba.py",
    "stockagent/data/factorized_panel.py", "stockagent/data/tw_native_panel_features.py",
    "stockagent/data/annual_event_cache.py",
    "stockagent/data/annual_cache_admission.py",
    "stockagent/data/source_processing_runtime.py",
    "stockagent/data/tw_price_rules.py", "stockagent/data/tw_listing_admission.py",
    "stockagent/data/finlab_acquisition_contract.py", "stockagent/data/tw_day_trade_mixed_frequency.py",
    "stockagent/data/tw_public_release_schedule.py", "stockagent/data/tw_public_features.py",
    "stockagent/live/market_status.py", "stockagent/live/tw_public_source_guard.py",
    "stockagent/training/checkpoint_contract.py", "stockagent/storage_layout.py",
    "stockagent/remote_build.py", "downloader/artifact_io.py", "scripts/benchmark_columnar_query_engines.py",
)]


def canonical_cache_path(path: Path) -> Path:
    """Explicit local derived cache only; never a source/trading output path."""
    path = path.absolute()
    if path.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        return path
    approved = ROOT / "artifacts/cache/tw_factorized_live/annual"
    if path.is_symlink() or path != approved or path.resolve() != approved.absolute():
        raise ValueError("explicit canonical derived-cache root or private benchmark cache required")
    return path


def shared_identity() -> dict:
    return {str(path): file_sha256(path) for path in SHARED_CODE}


def freeze(*, fixture: Path, causal_fixture: Path, config_path: Path,
           native_source: Path, public: Path, finlab: Path) -> dict:
    from stockagent.config import load_config
    from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source, safe_stem
    from stockagent.data.tw_day_trade_mixed_frequency import rule_from_spec
    from stockagent.live.market_status import TW_HOLIDAY_SCHEDULE_NAME
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard

    causal_fixture = causal_fixture.resolve(strict=True)
    public, native_source, finlab = (path.resolve(strict=True) for path in (public, native_source, finlab))
    core = verify_fixture(causal_fixture)
    config = load_config(config_path)
    # The owner overrides these exact fields too; this only enumerates its
    # complete executable-base/source receipt dependencies before freezing.
    recipe = {"data": {**asdict(config.data), "parquet_root": str(public / "stocks"),
                       "tw_public_feature_path": str(public / "features/tw_public_stock_daily.parquet")}}
    _, base_members = source_members([recipe])
    members = sorted(set(base_members) | {public / name for name in (
        "features/tw_public_stock_daily.summary.json", TW_HOLIDAY_SCHEDULE_NAME,
        "twse_taiex_ohlc.parquet", "twse_taiex_ohlc.summary.json")})
    dictionary = json.loads((causal_fixture / "feature_dictionary.json").read_bytes())
    by_name = {d["feature"]: d for d in dictionary["features"]}
    definitions = [by_name[name] for name in core["individual"] + core["common"]]
    captured = {}
    for definition in definitions:
        if definition.get("source") == "FinLab":
            key = definition["dataset"]
            if key not in captured:
                receipt_path = finlab / "receipts" / f"{safe_stem(key)}.json"
                captured[key] = (*read_receipt_bound_source(finlab, key, receipt_path), receipt_path)
    source_manifest = native_source / "source_manifest.json"
    native = json.loads(source_manifest.read_bytes())
    native_members = [source_manifest, native_source / "finlab/known_uploads.parquet"]
    bounds = native.get("source_repairs", {}).get("publication_lower_bounds")
    if bounds:
        native_members.append(native_source / bounds)
    source_bytes = (sum(path.stat().st_size for path in members)
                    + sum(original.stat().st_size for original, _, _ in captured.values())
                    + sum(path.stat().st_size for path in native_members if path.is_file())
                    + core["frozen_bytes"])
    if shutil.disk_usage(ROOT).free < source_bytes + 30 * 1024**3:
        raise ValueError("retain 30 GiB disk reserve after whole-owner capture")
    fixture = new_private(fixture)
    proofs = []
    with live_tw_public_source_read_guard(public / "stocks"):
        for path in members:
            proofs.append(private_copy(path, fixture / "data_tw_public" / path.relative_to(public)))
        if source_members([recipe])[1] != base_members:
            raise ValueError("public source dependency membership changed while freezing")
    for path in native_members:
        if path.is_file():
            proofs.append(private_copy(path, fixture / "native_source" / path.relative_to(native_source)))
    for key, (original, receipt, _receipt_path) in captured.items():
        proof = private_copy(original, fixture / "finlab" / receipt["parquet_path"])
        if proof["sha256"] != receipt["sha256"]:
            raise ValueError("captured FinLab immutable object changed")
        proofs.append(proof)
        # Preserve the captured body, not a later atomic head. Its provenance
        # remains original; only the private root is different.
        path = fixture / "finlab/receipts" / f"{safe_stem(key)}.json"
        atomic_write_json(path, receipt)
        path.chmod(0o400)
        proofs.append({"path": str(path), "sha256": file_sha256(path), "bytes": path.stat().st_size})
    parent = json.loads((causal_fixture / "parent_manifest.json").read_bytes())
    horizon = max(max(rule_from_spec(d).carry_days, rule_from_spec(d).older_carry_days) for d in definitions)
    years = set(range((date.fromisoformat(core["dates"][0]) - timedelta(days=horizon)).year,
                      date.fromisoformat(parent["dates"][-1]).year + 1))
    for path, target in [(causal_fixture / "original_owner.py", fixture / "original_owner.py"),
                         (causal_fixture / "original_sweep.py", fixture / "original_sweep.py"),
                         (causal_fixture / "original_writer.py", fixture / "original_writer.py"),
                         (causal_fixture / "parent_manifest.json", fixture / "parent/factorized_manifest.json"),
                         (causal_fixture / "feature_dictionary.json", fixture / "parent/feature_dictionary.json"),
                         (config_path, fixture / "config.json")]:
        proofs.append(private_copy(path, target))
    for year in sorted(years):
        proofs.append(private_copy(causal_fixture / "events" / f"{year}.parquet", fixture / "parent" / f"{year}.parquet"))
    evidence = {"contract": CONTRACT, "status": "frozen", "proofs": proofs,
                "causal_fixture": str(causal_fixture), "causal_fixture_sha256": file_sha256(causal_fixture / "fixture.json"),
                "start_date": core["dates"][0], "source_bytes": source_bytes,
                "config_sha256": file_sha256(config_path),
                "finlab_tables": len(captured), "public_members": len(members),
                "no_provider_calls": True, "production_publication": False}
    atomic_write_json(fixture / "fixture.json", evidence)
    return {key: evidence[key] for key in ("status", "source_bytes", "finlab_tables", "public_members")}


def worker(*, fixture: Path, output: Path, variant: str, cache_root: Path | None = None) -> dict:
    import polars as pl
    import pyarrow as pa
    import torch
    import requests
    import urllib.request
    from stockagent.remote_build import observe_node

    def forbidden(*_args, **_kwargs):
        raise AssertionError("offline source owner must not call provider or CUDA")

    if torch.cuda.is_initialized():
        raise ValueError("CPU worker already owns a CUDA context")
    for name in ("_lazy_init", "is_available", "synchronize", "empty_cache"):
        setattr(torch.cuda, name, forbidden)
    requests.sessions.Session.request = forbidden
    urllib.request.urlopen = forbidden

    codes = shared_identity()
    fixture = fixture.resolve(strict=True)
    data = json.loads((fixture / "fixture.json").read_bytes())
    fixture_sha = file_sha256(fixture / "fixture.json")
    if data["contract"] != CONTRACT:
        raise ValueError("whole-owner fixture contract mismatch")

    def fence():
        for proof in data["proofs"]:
            path = Path(proof["path"])
            if not path.resolve().is_relative_to(fixture) or file_sha256(path) != proof["sha256"]:
                raise ValueError("frozen whole-owner dependency changed")
        if file_sha256(fixture / "fixture.json") != fixture_sha:
            raise ValueError("complete workload manifest changed")

    fence()
    if torch.cuda.is_initialized():
        raise ValueError("whole-owner benchmark initialized CUDA")
    output = new_private(output)
    pa.set_cpu_count(int(os.environ.get("POLARS_MAX_THREADS", "2")))
    if variant == "canonical":
        if cache_root is None:
            raise ValueError("canonical verification requires an explicit derived-cache root")
        sweep_path = ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py"
        owner_path = ROOT / "scripts/prepare_tw_day_trade_factorized_live.py"
        writer_path = ROOT / "stockagent/data/factorized_panel.py"
        sweep_text, writer_text = sweep_path.read_text(), writer_path.read_text()
        private_copy(owner_path, output / "canonical_owner_reference.py")
        for path in SHARED_CODE:
            proof = private_copy(path, output / "shared-code" / path.relative_to(ROOT))
            if proof["sha256"] != codes[str(path)]:
                raise ValueError("canonical dependency changed while preserving source code")
    else:
        sweep_text, writer_text = candidate_sources((fixture / "original_sweep.py").read_text(),
                                                   (fixture / "original_writer.py").read_text(), variant)
        owner_path = fixture / "original_owner.py"
    atomic_write_bytes(output / "candidate_sweep.py", sweep_text.encode(), mode=0o400)
    atomic_write_bytes(output / "candidate_writer.py", writer_text.encode(), mode=0o400)
    sweep = load_baseline(output / "candidate_sweep.py")
    if variant.startswith("prepared"):
        sweep.PREPARED_ANNUAL_CACHE_ROOT = private_cache_path(cache_root or output / "annual-cache")
    writer = load_baseline(output / "candidate_writer.py")
    owner = load_baseline(owner_path)
    # Private root localizes the existing owner's base cache and source-code
    # identity; actual causal/writer functions are independently saved copies.
    owner.ROOT = output / "code-root"
    atomic_write_bytes(owner.ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py", sweep_text.encode(), mode=0o400)
    if variant != "canonical":
        owner.AnnualObservationEvents = sweep.AnnualObservationEvents
        owner.causal_state_blocks = sweep.causal_state_blocks
    # Canonical specialization inherits the real shared class. Keep its real
    # causal function too, rather than mixing a copied module's class identity
    # with a current resource-admitted subclass. All files are SHA-fenced.
    owner.write_array_block = writer.write_array_block
    original_subset = owner.subset_panel_symbols
    base_oracles = []
    oracle_seconds = []

    def record_base(*args, **kwargs):
        panel = original_subset(*args, **kwargs)
        started = time.perf_counter()
        base_oracles.append(panel_identity(panel))
        oracle_seconds.append(time.perf_counter() - started)
        return panel

    owner.subset_panel_symbols = record_base
    preparation = dict(
        config_path=fixture / "config.json", parent_path=fixture / "parent/factorized_manifest.json",
        native_source=fixture / "native_source", public=fixture / "data_tw_public", finlab=fixture / "finlab",
        output=output / "projection", start_date=data["start_date"])
    if variant == "canonical":
        preparation["annual_cache_root"] = canonical_cache_path(cache_root)
    metrics = measured(lambda: owner.prepare(**preparation))
    manifest_path = Path(metrics["result"]["manifest"])
    manifest = json.loads(manifest_path.read_bytes())
    verify_factorized_members(manifest_path.parent, manifest)
    fence()
    if torch.cuda.is_initialized() or len(base_oracles) != 1 or codes != shared_identity():
        raise ValueError("whole-owner CUDA/base-oracle boundary changed")
    evidence = {"contract": CONTRACT, "variant": variant, "fixture_sha256": fixture_sha,
                "original_owner_sha256": file_sha256(fixture / "original_owner.py"),
                "metrics": metrics, "manifest_sha256": file_sha256(manifest_path),
                "node": observe_node(fixture, output), "scope": "complete canonical preparation including all upstream steps",
                "all_symbols": len(manifest["symbols"]), "all_dates": len(manifest["dates"]),
                "all_channels": manifest["logical_model_channels"], "provider_calls": 0,
                "complete_base_oracles": base_oracles, "base_oracle_seconds": sum(oracle_seconds),
                "delta_sha256": file_sha256(manifest_path.parent / "delta_events.parquet"),
                "shared_code_sha256": codes,
                "production_publication": False, "cuda_requested": False, "cuda_initialized": False}
    evidence["processing_pools"] = {
        "polars_observed_threads": pl.thread_pool_size(), "arrow_observed_threads": pa.cpu_count(),
        "requested_environment": {key: os.environ.get(key) for key in (
            "POLARS_MAX_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")}}
    if variant.startswith("prepared"):
        evidence["annual_cache"] = {"root": str(sweep.PREPARED_ANNUAL_CACHE_ROOT),
            "instances": [cache.stats() for cache in sweep.PREPARED_ANNUAL_CACHE_INSTANCES],
            "code_sha256": file_sha256(ROOT / "stockagent/data/annual_event_cache.py")}
    if variant == "canonical":
        evidence["annual_cache"] = {"root": str(preparation["annual_cache_root"]),
            "instances": list(metrics["result"]["annual_cache"].values()),
            "code_sha256": file_sha256(ROOT / "stockagent/data/annual_event_cache.py")}
        evidence["canonical_owner_sha256"] = file_sha256(owner_path)
        evidence["canonical_cache_written"] = True
        if len(evidence["annual_cache"]["instances"]) != 2:
            raise ValueError("both canonical annual event owners must expose their cache evidence")
    atomic_write_json(output / "receipt.json", evidence)
    return evidence


def output_contract(manifest: dict) -> dict:
    """Ignore only measured timing and the declared code/generation identity."""
    value = deepcopy(manifest)
    value.pop("build_wall_seconds", None)
    receipt = value["paper_inference"]
    receipt.pop("built_at_utc", None)
    receipt["source_identity"].pop("sweep_sha", None)
    return value


def canonical_output_contract(manifest: dict, fixture: Path) -> dict:
    """Normalize only independently VERIFIED implementation metadata changes.

    Source values, definitions, axes, PIT/clocks, permissions, masks and every
    output member remain in the original oracle. New config/publication hashes
    must equal the frozen fixture; none are unconditionally ignored.
    """
    from scripts.prepare_tw_day_trade_factorized_live import _native_context_identity
    from scripts.prepare_tw_day_trade_factorized_panel import annual_preparation_identity

    value = deepcopy(manifest)
    receipt = value["paper_inference"]
    identity = receipt["source_identity"]
    owner = ROOT / "scripts/prepare_tw_day_trade_factorized_live.py"
    expected = {
        "builder_sha": file_sha256(owner),
        "sweep_sha": file_sha256(ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py"),
        "annual_preparation": annual_preparation_identity(),
        "writer_sha": file_sha256(ROOT / "stockagent/data/factorized_panel.py"),
        "config_sha": file_sha256(fixture / "config.json"),
        "native_context": _native_context_identity(
            fixture / "native_source",
            json.loads((fixture / "native_source/source_manifest.json").read_bytes())),
    }
    if (receipt["source_update_owner"] != str(owner)
            or any(identity.get(key) != proof for key, proof in expected.items())):
        raise ValueError("canonical code/configuration/publication fingerprints differ")
    for key in ("annual_preparation", "writer_sha", "config_sha", "native_context"):
        identity.pop(key)
    identity["builder_sha"] = file_sha256(fixture / "original_owner.py")
    receipt["source_update_owner"] = str(fixture / "original_owner.py")
    return output_contract(value)


def verify_canonical(*, fixture: Path, reference: Path, candidate: Path, output: Path) -> dict:
    """Accept a complete canonical cold/warm worker against a saved original."""
    fixture = fixture.resolve(strict=True)
    original = json.loads(reference.read_bytes())
    proposed = json.loads(candidate.read_bytes())
    fixture_sha = file_sha256(fixture / "fixture.json")
    if (original["variant"] != "baseline" or proposed["variant"] != "canonical"
            or any(row["fixture_sha256"] != fixture_sha or row["provider_calls"] != 0
                   or row["cuda_initialized"] or row["production_publication"] for row in (original, proposed))
            or proposed["shared_code_sha256"] != shared_identity()):
        raise ValueError("independent original/canonical source boundaries differ")
    original_path, proposed_path = [Path(row["metrics"]["result"]["manifest"]) for row in (original, proposed)]
    original_manifest, proposed_manifest = [json.loads(path.read_bytes()) for path in (original_path, proposed_path)]
    for path, manifest, row in zip((original_path, proposed_path), (original_manifest, proposed_manifest),
                                   (original, proposed), strict=True):
        verify_factorized_members(path.parent, manifest)
        if (file_sha256(path) != row["manifest_sha256"]
                or file_sha256(path.parent / "delta_events.parquet") != row["delta_sha256"]):
            raise ValueError("complete owner manifest/delta proof changed")
    if (canonical_output_contract(proposed_manifest, fixture) != output_contract(original_manifest)
            or proposed["complete_base_oracles"] != original["complete_base_oracles"]
            or proposed["delta_sha256"] != original["delta_sha256"]):
        raise ValueError("canonical all-base/all-shard/delta/clock/permission oracle mismatch")
    value = {"contract": "canonical_complete_live_owner_acceptance_v1", "status": "accepted_exact_parity",
             "fixture_sha256": fixture_sha, "reference_receipt_sha256": file_sha256(reference),
             "candidate_receipt_sha256": file_sha256(candidate), "candidate": str(candidate),
             "all_dates": proposed["all_dates"], "all_symbols": proposed["all_symbols"],
             "all_channels": proposed["all_channels"], "metrics": proposed["metrics"],
             "annual_cache": proposed["annual_cache"], "shared_code_sha256": shared_identity(),
             "provider_calls": 0, "cuda_initialized": False, "production_projection_published": False}
    output = new_private(output)
    atomic_write_json(output / "receipt.json", value)
    return {key: value[key] for key in ("status", "all_dates", "all_symbols", "all_channels")}


def benchmark(*, fixture: Path, output: Path, variant: str, cache_root: Path | None = None) -> dict:
    from stockagent.remote_build import observe_node

    if variant == "baseline":
        raise ValueError("a distinct candidate is required")
    if (variant.startswith("prepared") or variant == "canonical") and (cache_root is None or not cache_root.is_dir()):
        raise ValueError("warm whole-owner benchmark requires a separately built private cache")
    fixture = fixture.resolve(strict=True)
    output = new_private(output)
    fixture_sha = file_sha256(fixture / "fixture.json")
    codes = shared_identity()
    env = {**os.environ, "CUDA_VISIBLE_DEVICES": "", "POLARS_MAX_THREADS": "2",
           "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"}
    receipts = []
    reference = None
    node = observe_node(fixture, output)
    for index, selected in enumerate(("baseline", variant, variant, "baseline")):
        if codes != shared_identity() or fixture_sha != file_sha256(fixture / "fixture.json"):
            raise ValueError("shared computation/workload changed before worker")
        destination = output / f"{index}-{selected}"
        command = [sys.executable, str(Path(__file__).resolve()), "worker", "--fixture", str(fixture),
                   "--output", str(destination), "--variant", selected]
        if selected.startswith("prepared") or selected == "canonical":
            command.extend(["--cache-root", str(cache_root)])
        started = time.perf_counter()
        with (output / f"{index}-{selected}.log").open("x") as stream:
            child = subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=1800)
        if child.returncode:
            raise RuntimeError(f"complete owner worker {index} failed: inspect {output / f'{index}-{selected}.log'}")
        receipt = json.loads((destination / "receipt.json").read_bytes())
        if (selected.startswith("prepared") or selected == "canonical") and any(row["builds"] or row["rejections"] for row in receipt["annual_cache"]["instances"]):
            raise ValueError("cold/repaired cache cannot be compared as a warm workflow")
        if (receipt["processing_pools"]["polars_observed_threads"] != 2
                or receipt["processing_pools"]["arrow_observed_threads"] != 2):
            raise ValueError("requested pool settings were not applied to the actual worker")
        manifest = json.loads(Path(receipt["metrics"]["result"]["manifest"]).read_bytes())
        contract = {"manifest": (canonical_output_contract(manifest, fixture) if selected == "canonical"
                                 else output_contract(manifest)), "delta_sha256": receipt["delta_sha256"],
                    "complete_base_oracles": receipt["complete_base_oracles"]}
        if reference is None:
            reference = contract
        if (contract != reference or receipt["fixture_sha256"] != fixture_sha
                or codes != shared_identity() or fixture_sha != file_sha256(fixture / "fixture.json")):
            raise ValueError("complete base/normalization/projection/source/code contract differs")
        receipt["child_wall_seconds"] = time.perf_counter() - started
        receipt["complete_output_parity"] = True
        receipts.append(receipt)
        atomic_write_json(output / "progress.json", {"completed": len(receipts), "receipts": receipts})
        print(f"complete owner {index+1}/4 {selected}: {receipt['metrics']['workflow_seconds']:.3f}s", flush=True)
    summary = {}
    for metric in ("workflow_seconds", "cpu_seconds", "peak_rss_bytes"):
        baseline = statistics.mean(row["metrics"][metric] for row in receipts if row["variant"] == "baseline")
        candidate = statistics.mean(row["metrics"][metric] for row in receipts if row["variant"] == variant)
        summary[metric] = {"baseline": baseline, "candidate": candidate, "change_fraction": candidate / baseline - 1}
    value = {"contract": CONTRACT, "status": "measured_complete_owner_exact_parity", "variant": variant,
             "fixture_sha256": fixture_sha, "shared_code_sha256": codes, "receipts": receipts, "summary": summary,
             "node_before": node, "node_after": observe_node(fixture, output),
             "scope": "complete canonical preparation, all base fields, all normalized delta events and every external shard",
             "baseline_source": "immutable saved original owner/sweep/writer", "provider_calls": 0,
             "model_inference_included": False, "production_publication": False,
             "base_oracle_cost_included": True, "os_cache_controlled": False}
    atomic_write_json(output / "receipt.json", value)
    return {"status": value["status"], "summary": summary}


def main():
    os.nice(19)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("freeze")
    for name in ("fixture", "causal-fixture", "config-path", "native-source", "public", "finlab"):
        capture.add_argument(f"--{name}", type=Path, required=True)
    verify = commands.add_parser("verify-canonical")
    for name in ("fixture", "reference", "candidate", "output"):
        verify.add_argument(f"--{name}", type=Path, required=True)
    for name in ("worker", "benchmark"):
        run = commands.add_parser(name)
        run.add_argument("--fixture", type=Path, required=True)
        run.add_argument("--output", type=Path, required=True)
        run.add_argument("--variant", choices=("baseline", "take", "slices", "slices_take", "prepared", "prepared_causal", "canonical"), required=True)
        run.add_argument("--cache-root", type=Path)
    args = vars(parser.parse_args())
    command = args.pop("command")
    print(json.dumps({"result": {"freeze": freeze, "worker": worker, "benchmark": benchmark,
                                  "verify-canonical": verify_canonical}[command](**args)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
