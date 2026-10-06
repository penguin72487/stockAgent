#!/usr/bin/env python3
"""Measure complete parallel workflows on this relay's real guarded NAS.

Uses four bounded samples of received source files and a fresh, isolated NAS
repository. Keeps private evidence and the experimental repository; never
prunes, deletes ingress or changes the production repository.
"""
import argparse
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
from statistics import median
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))

from scripts.backup_delivery_receipt import DISPATCH, atomic_public, read_json, signed, validate_dispatch
from scripts.verify_backup_delivery import CONTRACT, regular
from stockagent.data_sync.backup_relay_pipeline import run_cycle, validate_configuration
from stockagent.data_sync.nas_target import NasTarget
from stockagent.data_sync.offhost_backup import ResticBackup, private_json
from stockagent.data_sync.packed_backup import signature


def benchmark(configuration, evidence):
    validate_configuration(configuration)
    evidence = evidence.absolute()
    if evidence.exists() or any(p.is_symlink() for p in (evidence, *evidence.parents)):
        raise ValueError("use fresh private benchmark evidence")
    started = time.perf_counter()
    evidence.mkdir(mode=0o700, parents=True)
    c = deepcopy(configuration)
    # This preparation also takes the existing owner. Subsequent measured
    # cycles use it themselves; an unrelated worker can never overlap NAS IO.
    with Path(c["owner_lock"]).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        samples = []
        for path in sorted((Path(c["ingress_root"]) / "tools/dispatch").glob("*.json"), reverse=True):
            dispatch = read_json(path)
            validate_dispatch(dispatch)
            if any(dispatch[k] != c[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")):
                raise ValueError("benchmark source dispatch belongs to another pair")
            batch = Path(c["ingress_root"]) / dispatch["batch_relative"]
            if not (batch / "READY").is_file() or not (batch / "backup-envelope.json").is_file():
                continue
            envelope = read_json(batch / "backup-envelope.json")
            if (hashlib.sha256((batch / "backup-envelope.json").read_bytes()).hexdigest() != dispatch["envelope_file_sha256"]
                    or (batch / "READY").read_text().strip() != dispatch["envelope_identity_sha256"]):
                raise ValueError("benchmark source envelope differs from its pinned dispatch")
            for row in envelope["files"]:
                if row["bytes"] < 1024**2:
                    continue
                try:
                    source = regular(batch, row["relative"])
                    before = signature(source)
                    if source.stat().st_size != row["bytes"]:
                        continue
                    with source.open("rb") as reader:
                        raw = reader.read(16*1024**2)
                    if signature(source) != before:
                        raise ValueError("benchmark source changed during sampling")
                except FileNotFoundError:
                    continue
                samples.append({"raw": raw, "source_envelope_identity_sha256": dispatch["envelope_identity_sha256"],
                    "source_relative": row["relative"], "source_full_file_sha256": row["sha256"],
                    "sample_offset": 0, "sample_bytes": len(raw), "sample_sha256": hashlib.sha256(raw).hexdigest()})
                if len(samples) == 4:
                    break
            if len(samples) == 4:
                break
        if len(samples) != 4:
            raise ValueError("wait for four real source-file samples; do not benchmark fabricated history")
        profile = read_json(Path(c["nas_configuration"]))
        experiment="stockagent-parallel-measurement-" + uuid.uuid4().hex
        profile["nas"]["repository_directory"] = experiment
        profile_path = evidence / "nas-benchmark.private.json"
        private_json(profile_path, profile)
        target = NasTarget(profile_path)
        mount = target.check(additional_bytes=sum(s["sample_bytes"] for s in samples))
        c["nas_configuration"] = str(profile_path)
        private_json(evidence / "samples.private.json", [{k:v for k,v in s.items() if k != "raw"} for s in samples])
        for key in ("ingress_root", "receipt_root", "scratch_root"):
            c[key] = str(evidence / key)
            Path(c[key]).mkdir(mode=0o700)
        for n, sample in enumerate(samples):
            relative = f"measured-source-samples/{n}.bin"
            envelope = signed({"contract": CONTRACT, "source_plan_identity_sha256": c["installed_handoff_identity_sha256"],
                "files": [{"relative": relative, "bytes": sample["sample_bytes"], "sha256": sample["sample_sha256"]}],
                "selection": "bounded_real_source_file_prefix_measurement", "all_history_verified": False,
                "unpublished_sources_included": False})
            key = envelope["identity_sha256"]
            batch = Path(c["ingress_root"]) / ("batch-" + key)
            (batch / relative).parent.mkdir(mode=0o700, parents=True)
            with (batch / relative).open("xb") as stream:
                stream.write(sample["raw"])
                stream.flush()
                os.fsync(stream.fileno())
            private_json(batch / "backup-envelope.json", envelope)
            (batch / "READY").write_text(key + "\n")
            dispatch = signed({"contract": DISPATCH, **{k:c[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
                "batch_relative": batch.name, "envelope_identity_sha256": key,
                "envelope_file_sha256": hashlib.sha256((batch / "backup-envelope.json").read_bytes()).hexdigest(),
                "complete_files": 3, "complete_bytes": sample["sample_bytes"] + sum((batch / p).stat().st_size for p in ("backup-envelope.json", "READY"))})
            # This initial paired dispatch is private benchmark preparation;
            # each trial repins it to that trial's newly initialized repository.
            atomic_public(Path(c["ingress_root"]) / "tools/dispatch" / (key + ".json"), dispatch)
    rounds = []
    policies = ((1,1,1), (2,1,1), (4,2,2), (4,4,4))
    for n, (jobs,backup,restore) in enumerate((*policies, *reversed(policies))):
        # A reused repository would deduplicate all later input and measure
        # metadata overhead instead of new NAS writes. Every trial uses a
        # fresh repository; initialization is outside the recurring timer cost.
        with Path(c["owner_lock"]).open("a") as owner:
            fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
            profile["nas"]["repository_directory"]=experiment+"-"+str(n)
            profile_path=evidence/("nas-trial-"+str(n)+".private.json")
            private_json(profile_path,profile)
            c["nas_configuration"]=str(profile_path)
            target=NasTarget(profile_path)
            mount=target.check(additional_bytes=sum(s["sample_bytes"] for s in samples))
            backend=ResticBackup(Path(c["restic"]),str(target.repository),Path(c["password_file"]),cache=evidence/("init-cache-"+str(n)))
            c["repository_id"]=backend.initialize()["id"]
            target.unchanged(mount)
            for path in (Path(c["ingress_root"])/"tools/dispatch").glob("*.json"):
                dispatch=read_json(path)
                dispatch["repository_id"]=c["repository_id"]
                dispatch=signed({k:v for k,v in dispatch.items() if k != "identity_sha256"})
                atomic_public(path,dispatch,replace=True)
        c["state_root"] = str(evidence / ("state-" + str(n)))
        c["receipt_root"] = str(evidence / ("receipts-" + str(n)))
        Path(c["receipt_root"]).mkdir(mode=0o700)
        c.update(job_workers=jobs, backup_workers=backup, restore_workers=restore, maximum_jobs_per_cycle=4)
        trial = run_cycle(c)
        if len(trial["jobs"]) != 4 or any(j["state"] != "accepted" for j in trial["jobs"]):
            raise ValueError("a measured NAS workflow failed; preserve its private evidence")
        rounds.append({"policy":f"{jobs}/{backup}/{restore}", "job_workers":jobs,"backup_workers":backup,
            "restore_workers":restore,"seconds":trial["complete_cycle_seconds"],"all_four_fixed_snapshot_full_sha_verified":True,
            "fresh_repository_id":c["repository_id"]})
        private_json(evidence / "rounds.private.json", rounds)
    medians = {f"{j}/{b}/{r}":median(s["seconds"] for s in rounds if s["policy"] == f"{j}/{b}/{r}") for j,b,r in policies}
    # The requested pipeline must overlap stages. Compare the fully serialized
    # baseline honestly, but choose among actually measured parallel policies.
    selected = min((k for k in medians if k != "1/1/1"),key=medians.get)
    jobs,backup,restore = map(int,selected.split("/"))
    result = {"state": "real_nas_complete_pipeline_measured", "samples": rounds, "median_seconds": medians,
        "selected_job_workers": jobs,"selected_backup_workers":backup,"selected_restore_workers":restore,
        "speedup_over_single_job":medians["1/1/1"]/medians[selected],
        "measured_payload_bytes": sum(s["sample_bytes"] for s in samples), "cache_flushed": False,
        "scope": "four real source-file prefix samples; input SHA, encrypted NAS backup, exclusive structural check, fixed snapshot restore, full SHA, ACK and exact scratch cleanup",
        "production_repository_modified":False,"experimental_repositories_preserved":True,"fresh_repository_each_trial":True,
        "repository_initialization_included_in_trial_seconds":False,
        "automatic_pruning": False, "complete_benchmark_seconds": time.perf_counter()-started}
    private_json(evidence / "measured-result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configuration", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(benchmark(read_json(args.configuration), args.evidence)))


if __name__ == "__main__":
    main()
