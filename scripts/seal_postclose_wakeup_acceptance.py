"""Seal bounded native cache-wakeup acceptance; never starts a service/model."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts import audit_managed_gpu_handles as gpu
from scripts import discord_postclose_wakeup as wakeup
from scripts import verify_discord_artifact_wakeup_runtime as native


def seal(args):
    if args.output.exists():
        raise FileExistsError("never replace previous acceptance")
    proofs = {name: json.loads(getattr(args, name).read_bytes())
              for name in ("benchmark", "readback", "reuse")}
    benchmark, readback, reuse = (proofs[name] for name in ("benchmark", "readback", "reuse"))
    if benchmark.get("state") != "verified" or not benchmark.get("complete_decision_parity"):
        raise ValueError("full probe benchmark not verified")
    for path, digest in benchmark["code_sha256"].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
            raise ValueError("measured code changed")
    if readback.get("state") != "native_wakeup_and_worker_readback_verified":
        raise ValueError("native worker readback not accepted")
    if native.code() != readback["code_sha256"] or native.protected() != readback["protected"]:
        raise ValueError("native code or protected owners changed")
    expected = readback["units"][wakeup.OWNER_UNIT]
    worker = native.show(wakeup.OWNER_UNIT)
    fields = ("ActiveState", "Result", "InvocationID", "ExecMainStartTimestampMonotonic",
              "ExecMainExitTimestampMonotonic", "CPUUsageNSec")
    if any(worker[field] != expected[field] for field in fields):
        raise ValueError("cache worker restarted or is not stable terminal")
    accepted = wakeup.read_json(wakeup.accepted_path(ROOT))
    current = wakeup.observe(ROOT, previous=accepted)
    if current != readback["wakeup"] or not wakeup.artifacts_unchanged(accepted):
        raise ValueError("source/code/config/artifacts changed since native readback")
    markets = {row["market"] for row in reuse["markets"]}
    if (reuse.get("state") != "accepted" or reuse.get("model_forward_calls") != 0
            or reuse.get("provider_calls") != 0 or reuse.get("cuda_initialized") is not False
            or reuse.get("trading_writes") is not False or markets != set(current["markets"])):
        raise ValueError("complete canonical consumer reuse not verified")
    since = datetime.fromisoformat(readback["observed_at_utc"]).strftime("%Y-%m-%d %H:%M:%S UTC")
    child = subprocess.run(["journalctl", "-u", native.UNITS[0], "--since", since,
                            "-n", "200", "--no-pager", "-o", "json"],
                           capture_output=True, text=True, check=True, timeout=10)
    polls = {}
    for line in child.stdout.splitlines():
        entry = json.loads(line)
        try:
            message = json.loads(entry["MESSAGE"])
        except (KeyError, TypeError, ValueError):
            continue
        if (message.get("status") == "postclose_wakeup_idle"
                and message.get("reason") == "unchanged_verified_wakeup_generation"
                and message.get("requested") is False and message.get("model_runtime_imported") is False
                and int(entry["__MONOTONIC_TIMESTAMP"]) > int(worker["ExecMainExitTimestampMonotonic"])):
            invocation = entry.get("_SYSTEMD_INVOCATION_ID")
            if invocation:
                polls[invocation] = {"invocation_id": invocation,
                                     "timestamp_us": entry["__REALTIME_TIMESTAMP"], **message}
    if len(polls) < 2:
        raise ValueError("need two distinct real native idle timer invocations after readback")
    handles = gpu.inventory()
    result = {"state": "accepted_bounded_postclose_wakeup", "contract": "postclose_wakeup_acceptance_v1",
              "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "proofs": {name: {"path": str(getattr(args, name)), "sha256": hashlib.sha256(
                  getattr(args, name).read_bytes()).hexdigest()} for name in proofs},
              "native_idle_polls": list(polls.values()), "worker": worker,
              "protected_owners_unchanged": True, "canonical_cpu_reuse_markets": sorted(markets),
              "managed_gpu_sample": handles, "all_services_optimized": False,
              "future_publication_or_opening_tested": False, "all_windows_gpu_memory_attributed": False}
    atomic_write_json(args.output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("benchmark", "readback", "reuse", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    result = seal(parser.parse_args())
    print(json.dumps({"state": result["state"], "idle_poll_count": len(result["native_idle_polls"]),
                      "gpu_sample_owners": result["managed_gpu_sample"]["gpu_owner_count"]}))


if __name__ == "__main__":
    main()
