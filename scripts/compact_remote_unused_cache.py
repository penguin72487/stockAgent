#!/usr/bin/env python3
"""Private-SSH one-shot compaction; never install code into remote Git work."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from stockagent.data_sync.desync_snapshots import atomic_write_json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventory", required=True, type=Path)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    args = p.parse_args()
    inventory = json.loads(args.inventory.read_text())
    body = "import sys,types\n"
    for name, relative in (
        ("stockagent.data_sync.artifact_dedup", "stockagent/data_sync/artifact_dedup.py"),
        ("stockagent.data_sync.legacy_artifact_archive", "stockagent/data_sync/legacy_artifact_archive.py"),
        ("stockagent.data_sync.remote_legacy_return", "stockagent/data_sync/remote_legacy_return.py"),
        ("scripts.deduplicate_inactive_panel_caches", "scripts/deduplicate_inactive_panel_caches.py"),
    ):
        body += f"m=types.ModuleType({name!r});m.__file__={'/root/stockAgent/' + relative!r};sys.modules[{name!r}]=m\n"
        body += f"exec({(ROOT / relative).read_text()!r},m.__dict__)\n"
    body += "import json\nfrom pathlib import Path\n"
    body += "from scripts.deduplicate_inactive_panel_caches import compact_current_inventory\n"
    body += f"inventory=json.loads({json.dumps(inventory)!r})\n"
    body += f"result=compact_current_inventory(inventory,apply={args.apply!r},receipt_dir=Path('/var/lib/stockagent-legacy-return/cache-compaction'))\n"
    body += "print(json.dumps(result),flush=True)\n"
    command = [*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target),
               "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"]
    completed = subprocess.run(command, input=body, text=True, capture_output=True, timeout=3600)
    receipt = args.inventory.parent / ("cache-compaction-apply.json" if args.apply else "cache-compaction-dry-run.json")
    if completed.returncode:
        atomic_write_json(receipt, {"state": "compaction_failed_source_preserved",
                                   "exit_code": completed.returncode,
                                   "stderr": completed.stderr, "observed_at_epoch": time.time()})
        raise RuntimeError("cache compaction stopped; private failure receipt retained")
    value = json.loads(completed.stdout.splitlines()[-1])
    atomic_write_json(receipt, value)
    print(json.dumps(value), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
