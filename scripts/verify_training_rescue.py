#!/usr/bin/env python3
"""Acceptance of real Penguin D preservation, not a new Vast GPU launch."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.training_rescue import Rescue, load_policy  # noqa: E402
from stockagent.training_rescue_worker import atomic_json, checkpoint_summary, digest, file_hash, relative  # noqa: E402
from stockagent.data_sync.windows_cold_io import binary_reader  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise ValueError("acceptance needs a new evidence path; never overwrite an older receipt")
    rescue = Rescue(load_policy(ROOT / "configs/training_rescue.json"))
    with rescue.lock(wait_seconds=120):
        rescue.guard()
        spec, state = rescue.spec(args.run), rescue.journal(args.run)
        commit_id = state["latest"]["commit_id"]
        commit = json.loads((rescue.cold / "commits" / args.run / (commit_id + ".json")).read_bytes())
        bootstrap_id = state["bootstrap"]["id"]
        bootstrap = json.loads((rescue.cold / "bootstrap" / args.run / (bootstrap_id + ".json")).read_bytes())
        source_id = state["input_verification"]["id"]
        source = json.loads((rescue.cold / "source-verification" / args.run / (source_id + ".json")).read_bytes())
        if (digest(commit) != commit_id or digest(bootstrap) != bootstrap_id or digest(source) != source_id
                or any(v["spec_sha256"] != digest(spec) for v in (commit, bootstrap, source))
                or state["input_verification"].get("level") != "all_original_file_SHA_decoded"
                or bootstrap.get("source_runtime_stability", {}).get("state") != "source_runtime_still_matches_closed_capsule"):
            raise ValueError("recovery evidence is incomplete or differs from this enrolled run")
        rows = [*commit["inventory"]["files"], *bootstrap["files"],
                bootstrap["runtime_archive"], bootstrap["runtime_archive_proof"]]
        unique = {r["sha256"]: r for r in rows}
        for row in unique.values():
            path = rescue.object_path(row["sha256"])
            if path.stat().st_size != row["bytes"] or rescue.object_hash(path) != row["sha256"]:
                raise ValueError("D object failed fresh independent acceptance readback")
        checkpoint_proofs = {}
        # Bounded transient validation, not a training view or a C cold copy.
        with tempfile.TemporaryDirectory(prefix="stockagent-rescue-cpu-proof-") as temp:
            root = Path(temp)
            validation_spec = {**spec, "output_root": str(root)}
            for row in commit["inventory"]["files"]:
                if row["relative"] not in commit["inventory"]["checkpoints"]:
                    continue
                path = root / relative(row["relative"]); path.parent.mkdir(parents=True)
                with binary_reader(rescue.object_path(row["sha256"])) as src, path.open("xb") as dst:
                    shutil.copyfileobj(src, dst, 4 * 1024**2)
                if file_hash(path) != row["sha256"]:
                    raise ValueError("actual full checkpoint failed cold reconstruction")
                proof = checkpoint_summary(path, validation_spec)
                proof.pop("signature", None)
                checkpoint_proofs[row["relative"]] = proof
        for action in ("is-enabled", "is-active"):
            subprocess.run(["systemctl", action, "stockagent-training-rescue.timer"],
                           check=True, capture_output=True)
        body = {"contract": spec["contract"], "state": "accepted_prepared_preservation_not_new_GPU_resume",
            "run_id": args.run, "observed_at": time.time(), "spec_sha256": digest(spec),
            "D_commit_id": commit_id, "D_bootstrap_id": bootstrap_id, "source_proof_id": source_id,
            "checkpoint_context_files": len(commit["inventory"]["files"]),
            "checkpoint_context_bytes": sum(r["bytes"] for r in commit["inventory"]["files"]),
            "runtime_compressed_bytes": bootstrap["runtime_archive"]["bytes"],
            "unique_D_payload_objects_verified": len(unique), "checkpoint_CPU_reconstruction": checkpoint_proofs,
            "original_source_files_SHA_verified": sum(r["verification"]["independently_reconstructed_files"] for r in source["sources"]),
            "preservation_timer_enabled_active": True, "source_lifecycle_state": state.get("source_lifecycle_state"),
            "real_new_vast_resume_verified": False, "cross_hardware_bitwise_claim": False,
            "automatic_destination_selection": False, "provider_Stop_or_Destroy_called": False,
            "training_started": False, "original_source_files_deleted": False,
            "prepared_views_retained_on_penguin": False, "temporary_CPU_validation_copy_retired": True,
            "code_sha256": {str(path.relative_to(ROOT)): file_hash(path) for path in
                [ROOT / "stockagent/training_rescue.py", ROOT / "stockagent/training_rescue_worker.py",
                 ROOT / "scripts/training_rescue.py", ROOT / "scripts/run_training_rescue.sh"]}}
        atomic_json(args.output, body)
        print(json.dumps(body, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
