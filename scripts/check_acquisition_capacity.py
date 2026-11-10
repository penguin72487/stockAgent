#!/usr/bin/env python3
"""Check the canonical acquisition reserve before dispatch; 75 means retry later."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync.cache_capacity import (
    _admit_backing_volume,
    acquisition_dispatch_status,
    load_capacity_policy,
)
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

# Concurrent host writes previously varied by about 18 GiB for reasons not yet
# identified. Moving payloads to E does not establish that this risk ended.
FUTURES_WORKSPACE_CONTROL_HEADROOM_BYTES = 24 * 1024**3


def futures_workspace_dispatch_status(workspace: Path) -> dict:
    """Check the actual E payload target and the remaining small C controls.

    This admits only the separately bounded, serial futures-workspace mode;
    it is a dispatch observation, not a reservation against other writers.
    """
    from stockagent.data_sync.desync_snapshots import SnapshotError
    from stockagent.data_sync.workspace_volume import admit, check_finlab_futures

    value = {"contract": "finlab_futures_workspace_dispatch_v1", "path": str(workspace),
             "guard_kind": "finite_priority_preflight_not_write_reservation"}
    try:
        if workspace.resolve(strict=True) != (ROOT / "data_finlab").resolve(strict=True):
            raise ValueError("futures workspace is limited to the canonical source root")
        control = priority_dispatch_status(workspace,
            additional_headroom_bytes=FUTURES_WORKSPACE_CONTROL_HEADROOM_BYTES)
        if control.get("state") != "admitted":
            return {**value, "state": "deferred", "reason": "control_capacity_unavailable",
                    "control": control, "retryable": True}
        mount = check_finlab_futures()
        payload = admit()
    except (SnapshotError, OSError, ValueError, KeyError, TypeError, RuntimeError,
            subprocess.SubprocessError) as error:
        return {**value, "state": "deferred", "reason": "workspace_unavailable",
                "error_type": type(error).__name__, "retryable": True}
    return {**value, "state": "admitted", "control": control,
            "workspace": payload, "workspace_alias": mount["target"]}


def check_futures_workspace_dispatch(workspace: Path, *, report_admitted: bool = False) -> int:
    result = futures_workspace_dispatch_status(workspace)
    if report_admitted or result["state"] != "admitted":
        print(json.dumps(result), flush=True)
    return 0 if result["state"] == "admitted" else 75


def priority_dispatch_status(workspace: Path, *, additional_headroom_bytes: int = 0) -> dict:
    """Use the normal guard except for its additional verification reserve.

    Finite priority requests may consume the acquisition-only headroom, but
    never the common filesystem/ledger reserve or an unmeasurable backing disk.
    This is still a dispatch check, not a reservation for arbitrary growth.
    """
    if type(additional_headroom_bytes) is not int or not 0 <= additional_headroom_bytes < 2**63:
        raise ValueError("invalid additional priority headroom")
    result = acquisition_dispatch_status(workspace)
    if result.get("state") == "not_enrolled":
        return result
    if (result.get("state") == "deferred" and result.get("reason") not in
            {"filesystem_reserve", "physical_backing_reserve"}):
        return result
    policy = load_capacity_policy()
    if socket.gethostname() not in policy["nodes"]:
        return result
    minimum = policy["minimum_free_bytes"] + additional_headroom_bytes
    if minimum >= 2**63:
        raise ValueError("priority headroom exceeds bounded byte range")
    if (result.get("state") == "admitted" and
            type(result.get("required_bytes")) is int and result["required_bytes"] >= minimum):
        return result
    if result.get("state") not in {"admitted", "deferred"}:
        return result
    if workspace.resolve(strict=True) != workspace or not workspace.is_dir():
        raise CapacityDeferred("redirected_acquisition_workspace")
    stat = os.statvfs(workspace)
    available = stat.f_bavail * stat.f_frsize
    priority = {"contract": "acquisition_dispatch_capacity_v1", "path": str(workspace),
                "guard_kind": "priority_dispatch_preflight_not_write_reservation",
                "data_paths_removed": 0, "available_bytes": available,
                "required_bytes": minimum, "minimum_free_bytes": minimum,
                "reserve_class": "finite_priority_control_and_ledger"}
    if available < minimum:
        return {**priority, "state": "deferred", "reason": "filesystem_reserve", "retryable": True}
    # The ordinary guard has just measured the same physical backing volume.
    # Reuse that observation only when its complete identity and reserve are
    # present and consistent; otherwise retain the independent backing probe.
    # This is one dispatch observation, not a cache or a write reservation.
    physical = result.get("physical_free_bytes")
    previous_required = result.get("required_bytes_including_reserve")
    if (result.get("state") == "deferred" and result.get("reason") == "physical_backing_reserve"
            and result.get("path") == str(workspace.absolute())
            and type(physical) is int and physical >= 0
            and type(previous_required) is int and previous_required >= minimum
            and previous_required == result.get("required_bytes") and physical < previous_required):
        if physical < minimum:
            return {**priority, "state": "deferred", "reason": "physical_backing_reserve",
                    "retryable": True, "allocation_admitted": False,
                    "physical_free_bytes": physical, "required_bytes_including_reserve": minimum}
        return {**priority, "state": "admitted", "backing_volume": {
            "physical_free_bytes": physical, "required_bytes": minimum,
            "observation_basis": "same_dispatch_physical_probe"}}
    try:
        backing = _admit_backing_volume(workspace, minimum)
    except CapacityDeferred as error:
        return {**error.details, **priority, "state": "deferred", "retryable": True}
    return {**priority, "state": "admitted", "backing_volume": backing}


def check_dispatch(path: Path, *, report_admitted: bool = True, priority: bool = False,
                   additional_priority_headroom_bytes: int = 0) -> int:
    """Check a new acquisition batch without touching source state or retries.

    A first download may not have created its output directory yet. Inspect
    its nearest existing ancestor, without following or creating a new link.
    The canonical guard still validates the actual workspace and backing disk.
    """
    try:
        workspace = path.absolute()
        try:
            workspace.lstat()
        except FileNotFoundError:
            # A missing child must not make a registered E provider silently
            # fall back to the checkout's C filesystem during handoff/boot.
            from stockagent.data_sync.desync_snapshots import SnapshotError
            from stockagent.data_sync.provider_workspace import check_path
            try:
                check_path(workspace)
            except (OSError, ValueError, SnapshotError):
                result = {'contract': 'acquisition_dispatch_capacity_v1', 'state': 'deferred',
                          'reason': 'provider_workspace_unavailable', 'retryable': True,
                          'data_paths_removed': 0}
                print(json.dumps(result), flush=True)
                return 75
        while True:
            try:
                workspace.lstat()
                break
            except FileNotFoundError:
                workspace = workspace.parent
        if not priority and additional_priority_headroom_bytes:
            raise ValueError("additional priority headroom requires the priority guard")
        result = (priority_dispatch_status(workspace, additional_headroom_bytes=additional_priority_headroom_bytes)
                  if priority else acquisition_dispatch_status(workspace))
    except CapacityDeferred as error:
        result = {"contract": "acquisition_dispatch_capacity_v1", "state": "deferred",
                  "reason": error.details["reason"], "retryable": True, "data_paths_removed": 0}
    except (OSError, ValueError) as error:
        print(json.dumps({"state": "error", "error_type": type(error).__name__, "allocation_admitted": False}))
        return 2
    # The explicit exception is only safe with a measured, enrolled backing
    # volume. Ordinary legacy downloads retain their not-enrolled behaviour.
    if priority and additional_priority_headroom_bytes and result.get("state") != "admitted":
        if result.get("state") != "deferred":
            result = {"contract": "acquisition_dispatch_capacity_v1", "state": "deferred",
                      "reason": "priority_backing_unverified", "retryable": True,
                      "data_paths_removed": 0}
    if report_admitted or result["state"] == "deferred":
        print(json.dumps(result), flush=True)
    return 75 if result["state"] == "deferred" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    return check_dispatch(args.path)


if __name__ == "__main__":
    raise SystemExit(main())
