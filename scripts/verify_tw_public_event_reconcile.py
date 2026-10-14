"""Read-only native event-owner and derived-input admission/adoption evidence.

Never calls providers, builds, CUDA or service actions. Baseline requires an
idle serial source owner. Control observes that one owner was replaced with
the reviewed runtime contract and accepted the actual derived generation.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts import finalize_tw_public_completed_session as completed  # noqa: E402
from scripts.verify_public_feature_index_runtime import identity, sha  # noqa: E402
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard  # noqa: E402

UNIT = "stockagent-tw-public-source-events.service"
PROTECTED = (
    "stockagent-discord-bot.service", "stockagent-tw-day-trade-simulation.service",
    "stockagent-shioaji-taifex-bidask.service", "stockagent-shioaji-historical-market-data.service",
    "stockagent-finmind-complement.service", "stockagent-public-dashboards.service",
)
CODE = (
    ROOT / "scripts/watch_tw_public_source_events.py",
    ROOT / "scripts/run_tw_public_0830_check.py",
    ROOT / "scripts/finalize_tw_public_completed_session.py",
    ROOT / "stockagent/data/tw_public_source_receipts.py",
    ROOT / "stockagent/live/tw_public_source_guard.py",
    Path(__file__),
)
STATE = ROOT / "artifacts/data_refresh/tw_public/events/latest.json"
LIVE = Path("/srv/stockagent-live/data_tw_public")
PROC_ROOT = Path("/proc")
CGROUP_ROOT = Path("/sys/fs/cgroup/system.slice")


def forbidden(*_args, **_kwargs):
    raise AssertionError("native event verification may not compute/download/build")


def observe(*, control: dict | None = None) -> dict:
    import torch
    import requests

    owner = identity(UNIT)
    if owner.get("ActiveState") != "active" or not owner.get("InvocationID"):
        raise RuntimeError("event owner not active")
    pid = int(owner["MainPID"])
    proc = PROC_ROOT / str(pid)
    selected_runtime = Path(sys.executable).resolve()
    loaded_runtime = str((proc / "exe").readlink())
    runtime_is_current = (proc / "exe").samefile(selected_runtime)
    if (b"scripts/watch_tw_public_source_events.py" not in (proc / "cmdline").read_bytes().split(b"\0")
            or (proc / "cwd").resolve() != ROOT
            or loaded_runtime not in {str(selected_runtime), str(selected_runtime) + " (deleted)"}
            or control is not None and not runtime_is_current):
        raise RuntimeError("unexpected event owner/runtime")
    loaded_runtime_identity = {
        "path": loaded_runtime, "matches_current_inode": runtime_is_current,
        "loaded_sha256": sha(proc / "exe"), "selected_sha256": sha(selected_runtime),
    }
    processes = [int(value) for value in (CGROUP_ROOT / UNIT / "cgroup.procs").read_text().split()]
    if processes != [pid]:
        raise RuntimeError("source owner has active children; do not restart or double-build")
    state = json.loads(STATE.read_bytes())
    updated = datetime.fromisoformat(state["updated_at_taipei"])
    if not 0 <= (datetime.now(timezone.utc) - updated).total_seconds() <= 120:
        raise RuntimeError("event heartbeat not current")
    if state.get("refresh_in_progress") or state.get("completed_session_pending"):
        raise RuntimeError("serial source/finalizer work still pending")
    protected = {unit: identity(unit) for unit in PROTECTED}
    code = {str(path): sha(path) for path in CODE}
    if torch.cuda.is_initialized():
        raise RuntimeError("verification must start CUDA-cold")
    with ExitStack() as stack:
        stack.enter_context(patch.object(requests.Session, "request", forbidden))
        stack.enter_context(patch.object(completed, "_build_commands", forbidden))
        stack.enter_context(patch.object(completed.subprocess, "Popen", forbidden))
        for name in ("init", "is_available", "synchronize", "empty_cache"):
            stack.enter_context(patch.object(torch.cuda, name, forbidden))
        with live_tw_public_source_read_guard(LIVE / "stocks"):
            expected = completed._latest_completed_taiex_session(
                LIVE, observed=datetime.now(completed.TAIPEI)
            )
            derived = completed._derived_state(
                LIVE, expected_date=expected,
                session_date=datetime.now(completed.TAIPEI).date().isoformat(),
            )
    if code != {str(path): sha(path) for path in CODE} or owner != identity(UNIT):
        raise RuntimeError("code or native owner changed during verification")
    if protected != {unit: identity(unit) for unit in PROTECTED}:
        raise RuntimeError("protected native owners changed during verification")
    if control is not None:
        if control["code_sha256"] != code or control["protected_owners"] != protected:
            raise RuntimeError("reviewed code/protected owners differ from admission")
        if owner["InvocationID"] == control["owner"]["InvocationID"]:
            raise RuntimeError("native owner has not loaded the reviewed change")
        if (state.get("runtime_contract") != "completed_dependency_reconcile_v2"
                or state.get("runtime_source_sha256") != code[str(CODE[0])]):
            raise RuntimeError("new owner did not publish reviewed runtime identity")
        dispatched = state.get("completed_session_finalize") or {}
        if dispatched.get("status") != "ok" or dispatched.get("expected_date") != expected or not derived["current"]:
            raise RuntimeError("actual derived generation is not accepted")
    return {
        "state": "accepted_native_event_adoption" if control is not None else "admitted_idle_event_owner",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "owner": owner, "protected_owners": protected, "code_sha256": code,
        "owner_runtime": loaded_runtime_identity,
        "expected_date": expected, "derived_state": derived,
        "runtime_contract": state.get("runtime_contract"),
        "native_finalization": state.get("completed_session_finalize"),
        "provider_calls": 0, "cuda_initialized": torch.cuda.is_initialized(),
        "service_actions": False, "source_writes": False,
        "scope": "serial_event_owner_and_current_derived_receipts_not_model_or_full_history",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        parser.error("use a new private engineering receipt")
    control = json.loads(args.control.read_bytes()) if args.control else None
    payload = observe(control=control)
    atomic_write_json(args.output, payload)
    print(json.dumps({key: payload[key] for key in ("state", "expected_date", "owner", "derived_state", "provider_calls", "cuda_initialized")}, sort_keys=True))


if __name__ == "__main__":
    main()
