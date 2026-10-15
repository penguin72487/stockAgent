"""Read back the native postclose fallback and protected process identities.

No service control, providers, source writes, trading writes or Torch imports.
Independent canonical signal-reuse verification remains a separate proof.
"""
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

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from scripts import discord_postclose_wakeup as wakeup

UNITS = ("stockagent-discord-artifact-retry.service", "stockagent-discord-artifact-retry.timer",
         wakeup.OWNER_UNIT)
PROTECTED = ("stockagent-discord-bot.service", "stockagent-tw-day-trade-simulation.service",
             "stockagent-shioaji-taifex-bidask.service", "stockagent-shioaji-taifex-dashboard.service",
             "stockagent-tw-public-source-events.service", "stockagent-finmind-complement.service")


def show(unit: str) -> dict:
    fields = ("ActiveState", "SubState", "Result", "MainPID", "InvocationID", "ExecMainStatus",
              "ExecMainStartTimestampMonotonic", "ExecMainExitTimestampMonotonic", "CPUUsageNSec",
              "MemoryPeak", "ExecStart", "ExecCondition", "DropInPaths", "FragmentPath")
    child = subprocess.run(["systemctl", "show", unit,
                            *(arg for field in fields for arg in ("-p", field))],
                           check=True, capture_output=True, text=True, timeout=5)
    return dict(line.split("=", 1) for line in child.stdout.splitlines() if "=" in line)


def protected() -> dict:
    return {unit: {key: show(unit)[key] for key in ("ActiveState", "MainPID", "InvocationID")}
            for unit in PROTECTED}


def code() -> dict:
    paths = ("scripts/discord_postclose_wakeup.py", "scripts/run_discord_artifact_maintenance.py",
             "scripts/install_discord_bot_service.sh", *(f"deploy/systemd/{unit}.in" for unit in UNITS))
    return {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths}


def run(args) -> dict:
    if args.output.exists():
        raise FileExistsError("never replace previous operational evidence")
    result = {"contract": "postclose_native_wakeup_readback_v1", "state": "observed",
              "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "units": {unit: show(unit) for unit in UNITS}, "protected": protected(),
              "code_sha256": code(), "observer_source_writes": 0, "observer_service_controls": 0,
              "observer_model_forward_calls": 0, "canonical_consumer_reuse_verified": False,
              "path": show("stockagent-discord-artifact-maintenance.path")}
    if args.baseline:
        for unit in UNITS:
            path = Path("/etc/systemd/system") / unit
            content = path.read_bytes()
            backup = args.output.parent / f"{args.output.stem}-units-before" / unit
            if backup.exists():
                raise FileExistsError("do not overwrite unit rollback copy")
            atomic_write_bytes(backup, content)
        result["state"] = "baseline_preserved"
    else:
        previous = json.loads(args.control.read_bytes())
        if previous["protected"] != result["protected"]:
            raise ValueError("protected source/gateway/execution owner changed")
        if previous["code_sha256"] != result["code_sha256"]:
            raise ValueError("wakeup code/templates changed after baseline")
        for unit in UNITS:
            state = result["units"][unit]
            if state["DropInPaths"]:
                raise ValueError("unverified native unit drop-ins")
            expected = (ROOT / f"deploy/systemd/{unit}.in").read_text().replace(
                "@REPO_ROOT@", str(ROOT)).replace("@SERVICE_HOME@", "/root").replace(
                "@SERVICE_USER@", "root").replace("@SERVICE_GROUP@", "root")
            if Path(state["FragmentPath"]).read_text() != expected:
                raise ValueError("installed unit does not match canonical template")
        timer = result["units"][UNITS[1]]
        if timer["ActiveState"] != "active":
            raise ValueError("native receipt-probe timer is not active")
        worker = result["units"][wakeup.OWNER_UNIT]
        if worker["ActiveState"] != "inactive" or worker["Result"] != "success" or worker["ExecMainStatus"] != "0":
            raise ValueError("native cache owner has not completed successfully")
        accepted = wakeup.read_json(wakeup.accepted_path(ROOT))
        snapshot = wakeup.observe(ROOT, previous=accepted)
        if (accepted.get("status") != "canonical_artifacts_reusable"
                or accepted.get("fingerprint") != snapshot["fingerprint"]
                or not wakeup.artifacts_unchanged(accepted)):
            raise ValueError("no current worker-confirmed generation/artifacts")
        result.update(state="native_wakeup_and_worker_readback_verified", wakeup=snapshot,
                      worker_acceptance=accepted)
    if "torch" in sys.modules:
        raise ValueError("operational observer imported model runtime")
    atomic_write_json(args.output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--control", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not args.baseline and args.control is None:
        parser.error("readback requires preserved baseline")
    result = run(args)
    print(json.dumps({"state": result["state"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
