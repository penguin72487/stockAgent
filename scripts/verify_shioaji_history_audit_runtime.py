"""Read back natural adoption of the canonical single-pass history audit.

This verifier never starts/restarts a service, imports the broker SDK, calls a
provider, or writes source data. A waiting result is not successful adoption.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.verify_public_feature_index_runtime import identity, sha, PROTECTED, UNIT  # noqa: E402

OWNER = "stockagent-shioaji-historical-market-data.service"
RUN_ROOT = ROOT / "artifacts/data_repair/shioaji_historical_market_data"
LOG = RUN_ROOT / "run.log"
SUMMARY = ROOT / "data_tw_shioaji_history/summary.json"
CODE = (
    Path(__file__), ROOT / "downloader/download_shioaji_historical_market_data.py",
    ROOT / "downloader/shioaji_history_repair.py", ROOT / "downloader/download_shioaji_tw_kbars.py",
    ROOT / "downloader/common.py", ROOT / "downloader/artifact_io.py",
    ROOT / "scripts/run_shioaji_historical_market_data.sh",
    ROOT / "scripts/shioaji_history_runner_common.sh",
    ROOT / "stockagent/live/shioaji_schedule.py", ROOT / "stockagent/live/shioaji_traffic_ledger.py",
    Path("/etc/systemd/system") / OWNER,
)


def running_identity(unit):
    value = identity(unit)
    if value.get("ActiveState") != "active" or not value.get("InvocationID") or int(value.get("MainPID", 0)) <= 0:
        raise ValueError(f"native_owner_not_running:{unit}")
    return value


def code_proofs():
    return {str(path): sha(path) for path in CODE}


def log_position():
    stamp = LOG.stat()
    return {"device": stamp.st_dev, "inode": stamp.st_ino, "offset": stamp.st_size}


def recent_audit(control):
    before, after = control["log_position"], log_position()
    if (after["device"], after["inode"]) != (before["device"], before["inode"]) or after["offset"] < before["offset"]:
        raise ValueError("native_log_rotated_or_truncated; capture a new baseline")
    if after["offset"] - before["offset"] > 2 * 2**20:
        raise ValueError("native_log_readback_exceeds_2MiB_bound")
    with LOG.open("rb") as handle:
        handle.seek(before["offset"])
        content = handle.read(after["offset"] - before["offset"]).decode("utf-8", errors="replace")
    started = re.findall(r"^\[shioaji-history\] provider=.*summary_strategy=single_pass_receipt_v1$", content, re.M)
    audits = re.findall(
        r"^\[shioaji-history\] summary_audit query=(\d+) kind=final coverage_passes=1 elapsed_seconds=([\d.]+)$",
        content, re.M,
    )
    return {"position": after, "startup_markers": len(started),
            "final_audits": [{"queries": int(count), "seconds": float(seconds)} for count, seconds in audits]}


def readback(control, result):
    if control.get("state") != "ready_for_natural_batch":
        raise ValueError("not_a_valid_native_adoption_baseline")
    for key in ("owner", "protected", "code_sha256"):
        if result[key] != control[key]:
            raise ValueError(f"native_adoption_control_changed:{key}")
    audit = recent_audit(control)
    result["native_audit"] = audit
    result["state"] = "waiting_next_natural_batch"
    if not audit["startup_markers"] or not audit["final_audits"]:
        return
    summary = json.loads(SUMMARY.read_bytes())
    scheduler = json.loads((RUN_ROOT / "scheduler.json").read_bytes())
    if summary.get("source") != "shioaji_historical_market_data_v2" or summary.get("schema_version") != 2:
        raise ValueError("unexpected_canonical_summary_contract")
    written = datetime.fromisoformat(summary["written_at_utc"].replace("Z", "+00:00"))
    observed = datetime.fromisoformat(scheduler["observed_at_utc"].replace("Z", "+00:00"))
    baseline_time = datetime.fromisoformat(control["observed_at_utc"])
    if written <= baseline_time or observed < written or scheduler.get("state") != "waiting":
        return  # Native batch has not yet returned to its original scheduler.
    if scheduler.get("reason") not in {"await_catalog_or_retry_deadline", "next_repair_batch", "yield_to_waiting_tx_history"}:
        return
    if summary.get("methods") != ["kbars", "ticks"] or int(summary["contracts"]) <= 0:
        raise ValueError("unexpected_native_collection_scope")
    result.update(state="accepted_native_audit_adoption", summary=summary,
                  summary_sha256=sha(SUMMARY), scheduler=scheduler,
                  all_source_data_complete=summary.get("coverage_state") == "observed_data"
                  and summary.get("state") == "complete")


def verify(control=None):
    result = {
        "contract": "shioaji_single_pass_natural_adoption_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "owner": running_identity(OWNER),
        "protected": {unit: running_identity(unit) for unit in (*PROTECTED, UNIT)},
        "code_sha256": code_proofs(), "log_position": log_position(),
        "services_started_or_restarted": False, "source_files_written": 0,
        "provider_requests": 0, "gpu_calls": 0, "provider_throughput_measured": False,
        "state": "ready_for_natural_batch",
    }
    if control is not None:
        readback(control, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--control", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.baseline == (args.control is not None):
        parser.error("choose exactly one of --baseline or --control")
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError("preserve previous native runtime evidence")
    if not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        raise ValueError("runtime evidence must remain under artifacts/operations")
    result = {"state": "failed"}
    try:
        result = verify(None if args.baseline else json.loads(args.control.read_bytes()))
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        raise
    finally:
        atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("state", "owner", "provider_requests", "gpu_calls")}))


if __name__ == "__main__":
    main()
