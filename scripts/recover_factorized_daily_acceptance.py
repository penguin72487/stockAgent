"""Resume only a capacity-deferred final diagnostic, preserving the failed run.

No new source capture, inference, publication or service actions. The original
owner/code/source proofs must still match. Recheck every projection cell and
the independent BASE oracle; never promote a progress file alone to acceptance.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if __name__ == "__main__":
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    configure_cpu_source_runtime(2)

from downloader.artifact_io import atomic_write_json
from scripts import benchmark_factorized_daily_owner as original
from scripts.benchmark_dashboard_daily_incremental import _bounded_json_receipt, verify_real_factorized_pair
from stockagent.data.factorized_panel import FactorizedPanelFeatures, file_sha256, verify_factorized_members
from stockagent.data_sync.cache_capacity import load_capacity_policy, load_prepared_capacity_policy, reserve_cache_write
from stockagent.data_sync.immutable_cache_files import payload_info


def compare_complete_bounded(first, second):
    """Same dense all-cell bit oracle/hash, without cyclic block-cache eviction.

    A date block is visited for several slabs. Keep its decoded symbol blocks
    together (bounded at 2 GiB per side), not a 512 MiB cache which can evict
    the beginning before reaching the end. Hash a buffer, not a second copy.
    """
    import numpy as np
    paths = [Path(value["manifest"]) for value in (first, second)]
    raw = [path.read_bytes() for path in paths]
    manifests = [json.loads(value) for value in raw]
    for key in ("dates", "symbols", "base_feature_names", "individual_channels", "common_channels", "model_channel_policy"):
        if manifests[0][key] != manifests[1][key]:
            raise ValueError(f"daily/full axes differ: {key}")
    n, s, b = (len(manifests[0][key]) for key in ("dates", "symbols", "base_feature_names"))
    base = np.zeros((n, s, b), dtype=np.float32)
    arrays = []
    for path, manifest in zip(paths, manifests, strict=True):
        verify_factorized_members(path.parent, manifest)
        arrays.append(FactorizedPanelFeatures(path.parent, manifest, base, cache_bytes=2*1024**3))
    digest = hashlib.sha256()
    for begin in range(0, n, 4):
        a, c = (array[begin:min(begin+4, n)] for array in arrays)
        if not np.array_equal(a.view(np.uint32), c.view(np.uint32)):
            raise ValueError(f"all-cell daily/full bit oracle failed at {begin}")
        digest.update(memoryview(a).cast("B"))
        if begin % 200 == 0:
            print(json.dumps({"oracle_completed_dates": min(begin+4, n), "dates": n}), flush=True)
        del a, c
    for path, before, manifest in zip(paths, raw, manifests, strict=True):
        if path.read_bytes() != before:
            raise ValueError("Factorized manifest changed during complete oracle")
        verify_factorized_members(path.parent, manifest)
    return {"every_individual_common_value_bit_equal": True, "dates": n, "symbols": s,
            "individual_common_channels": len(manifests[0]["individual_channels"]) + len(manifests[0]["common_channels"]),
            "base_channels_zeroed_for_projection_only_oracle": True, "all_cell_sha256": digest.hexdigest()}


def recover(output, run_path, progress_path, independent_path):
    import requests
    import torch
    import urllib.request
    output = Path(output).absolute()
    if (output.exists() or output.resolve() != output
            or not output.is_relative_to(ROOT / "artifacts/benchmarks")):
        raise ValueError("fresh private recovery output required")
    failed, run_proof = _bounded_json_receipt(run_path)
    prior, progress_proof = _bounded_json_receipt(progress_path)
    if failed.get("state") != "failed" or failed.get("exit_code") != 1:
        raise ValueError("only a finished failed diagnostic may be resumed")
    argv = failed.get("argv", [])
    if (len(argv) != 6 or argv[:2] != ["run_fintech_python", "scripts/benchmark_factorized_daily_owner.py"]
            or argv[2] != "--output" or argv[4] != "--capture-root"
            or (ROOT / argv[3]).resolve() != Path(progress_proof["path"]).parent):
        raise ValueError("failed run does not own the exact diagnostic progress")
    log = Path(run_proof["path"]).with_name("run.log").read_text()
    if not log.rstrip().endswith("CapacityDeferred: completed-forward capacity deferred: filesystem_headroom_exhausted"):
        raise ValueError("only the original capacity deferral can be recovered")
    stages = prior["stages"]
    required = {"reuse_captured_real_sources", "retrospective_prior_fixture", "prior_full_owner",
                "restore_latest_real_fixture", "new_session_complete_owner", "fresh_full_latest_owner",
                "complete_independent_full_output_oracle"}
    if set(stages) != required or prior.get("contract") != "complete_factorized_daily_owner_benchmark_v1":
        raise ValueError("all original stages except final unchanged check are required")
    if stages["complete_independent_full_output_oracle"]["result"].get("every_individual_common_value_bit_equal") is not True:
        raise ValueError("original complete output oracle did not pass")
    own_sha = file_sha256(Path(__file__))
    snapshot = stages["reuse_captured_real_sources"]["result"]
    captured = (ROOT / argv[5]).resolve(strict=True)
    original_root = Path(progress_proof["path"]).parent

    def verify():
        for proof in (run_proof, progress_proof):
            if file_sha256(Path(proof["path"])) != proof["sha256"]:
                raise ValueError("failed run/progress evidence changed")
        if any(file_sha256(Path(path)) != sha for path, sha in prior["code_sha256"].items()):
            raise ValueError("original owner code changed; recovery requires original ABI")
        if file_sha256(Path(__file__)) != own_sha:
            raise ValueError("recovery implementation changed")
        for proof in snapshot["proofs"]:
            path = Path(proof["path"])
            info = payload_info(path)
            if (path.resolve(strict=True) != path or not path.is_relative_to(captured)
                    or info["mode"] & 0o222 or info["uid"] != __import__("os").geteuid()
                    or info["size"] != proof["bytes"] or file_sha256(path) != proof["sha256"]):
                raise ValueError("retained original capture changed")

    verify()
    if torch.cuda.is_initialized():
        raise ValueError("recovery must not use CUDA")
    def forbidden(*_args, **_kwargs):
        raise AssertionError("recovery cannot call providers or CUDA")
    requests.sessions.Session.request = forbidden
    urllib.request.urlopen = forbidden
    torch.cuda._lazy_init = forbidden
    output.mkdir(parents=True, mode=0o700)
    options = dict(config_path=captured / "config.json", parent_path=captured / "parent/factorized_manifest.json",
        native_source=captured / "native", public=original_root / "public_work", finlab=captured / "finlab",
        start_date=snapshot["original_start"], base_cache_root=original_root / "base_cache",
        annual_cache_root=original_root / "annual_cache", output=original_root / "incremental")
    started, cpu = time.perf_counter(), time.process_time()
    with reserve_cache_write(original_root, key=hashlib.sha256(str(options["output"]).encode()).hexdigest(),
            maximum_growth_bytes=load_prepared_capacity_policy()["maximum_generation_bytes"],
            family="prepared", policy=load_capacity_policy()):
        value = original.owner.prepare(**options)
    if value.get("manifest") != stages["new_session_complete_owner"]["result"]["manifest"]:
        raise ValueError("unchanged owner produced a different generation")
    stages["unchanged_full_integrity_owner"] = {"wall_seconds": time.perf_counter()-started,
        "cpu_seconds": time.process_time()-cpu, "result": value}
    verify()
    recovered = {**prior, "status": "accepted_real_source_daily_owner_not_pit_live_or_inference_readiness",
        "recovery": {"contract": "capacity_deferred_final_factorized_diagnostic_recovery_v1",
            "failed_run": run_proof, "retained_progress": progress_proof,
            "recovered_at_utc": datetime.now(UTC).isoformat(), "verifier_sha256": own_sha,
            "original_failure_preserved": True}}
    atomic_write_json(output / "daily_receipt.json", recovered)
    paired = verify_real_factorized_pair(output / "pair", output / "daily_receipt.json", independent_path,
        projection_verifier=compare_complete_bounded)
    verify()
    atomic_write_json(output / "receipt.json", {"status": paired["status"], "recovery": recovered["recovery"],
        "pair_receipt": {"path": str(output / "pair/receipt.json"), "sha256": file_sha256(output / "pair/receipt.json")},
        "provider_calls": 0, "production_writes": False, "service_restarts": 0})
    return paired


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--failed-run", type=Path, required=True)
    parser.add_argument("--progress", type=Path, required=True)
    parser.add_argument("--independent-base-receipt", type=Path, required=True)
    args = parser.parse_args()
    result = recover(args.output, args.failed_run, args.progress, args.independent_base_receipt)
    print(json.dumps({"status": result["status"]}), flush=True)


if __name__ == "__main__":
    main()
