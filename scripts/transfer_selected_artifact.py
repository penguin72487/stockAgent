#!/usr/bin/env python3
"""Run the registered Syncthing ingress; SSH carries code/control only.

Source the existing remote-cold-artifact-ingress environment with export enabled.
This command never rsyncs/scps artifacts, publishes cold heads, or activates a
model. Receive requires a pinned package and fresh two-sided convergence.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.configure_artifact_ingress_syncthing import run as local_syncthing
from stockagent.data_sync.artifact_transport import receive_package
from stockagent.data_sync.desync_snapshots import atomic_write_json


def remote_control(config, *, action, role="producer"):
    if action == "build":
        module = REPO_ROOT / "stockagent/data_sync/artifact_transport.py"
        arguments = ["artifact_transport", "--config-json", json.dumps(config), "build"]
    elif action in {"configure", "scan", "status"}:
        module = REPO_ROOT / "scripts/configure_artifact_ingress_syncthing.py"
        arguments = [
            "syncthing_control",
            "--config-json",
            json.dumps(config),
            "--role",
            role,
            action,
        ]
    else:
        raise ValueError("unsupported remote control action")
    # Only audited repository CODE and a bounded scope configuration cross SSH.
    # Model/source/object contents are never opened here.
    program = (
        "import sys\nsys.path.insert(0, '/root/stockAgent')\nsys.argv = "
        + repr(arguments)
        + "\n"
    )
    program += module.read_text().replace("from __future__ import annotations", "")
    command = [
        "ssh",
        "-i",
        os.environ["COLD_ARTIFACT_INGRESS_IDENTITY_FILE"],
        "-p",
        os.environ["COLD_ARTIFACT_INGRESS_SSH_PORT"],
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=15",
        os.environ["COLD_ARTIFACT_INGRESS_SSH_TARGET"],
        "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -",
    ]
    result = subprocess.run(
        command,
        input=program,
        text=True,
        capture_output=True,
        timeout=1200 if action == "build" else 180,
    )
    if result.returncode:
        # Do not dump arbitrary supervisor banners, raw config or command lines.
        raise RuntimeError(
            f"remote {action} failed with exit {result.returncode}; inspect the bounded remote operation"
        )
    output = json.loads(result.stdout)
    state = Path(config["state_root"])
    state.mkdir(parents=True, exist_ok=True)
    atomic_write_json(state / f"remote-{action}.json", output)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("configure", "build", "status", "receive"))
    parser.add_argument(
        "--config",
        type=Path,
        default=REPO_ROOT / "configs/data_sync/artifact_ingress.json",
    )
    parser.add_argument("--package-id")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if args.action == "configure":
        result = {
            "receiver": local_syncthing(config, "receiver", "configure"),
            "producer": remote_control(config, action="configure"),
        }
    elif args.action == "build":
        # Validate this SSH target's existing Syncthing identity and exact
        # folder scope before asking it to read any artifact source.
        remote_control(config, action="status")
        result = remote_control(config, action="build")
        remote_control(config, action="scan")
    else:
        result = {
            "receiver": local_syncthing(config, "receiver", "status"),
            "producer": remote_control(config, action="status"),
        }
        if args.action == "receive":
            if not args.package_id:
                parser.error(
                    "receive requires --package-id; latest is not a stable identity"
                )
            if not all(row["ok"] for row in result.values()):
                print(json.dumps(result, sort_keys=True))
                raise SystemExit(
                    "Syncthing is not converged; keep waiting, do not activate"
                )
            result["quarantine"] = receive_package(config, args.package_id)
            atomic_write_json(
                Path(config["state_root"]) / f"accepted-{args.package_id}.json", result
            )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
