#!/usr/bin/env python3
"""Record the user's bounded waiver of unavailable historical recovery."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync.backup_history import apply_disposition, build_disposition  # noqa: E402
from stockagent.data_sync.backup_stream import BackupStream, capture_catalog  # noqa: E402
from stockagent.data_sync.offhost_backup import private_json  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--authorization", required=True)
    parser.add_argument("--evidence", type=Path, action="append", required=True)
    parser.add_argument("--apply-plan", type=Path)
    parser.add_argument("--expected-plan-sha256")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve a fresh disposition/dry-run receipt")
    config = json.loads(args.config.read_bytes())
    queue = BackupStream(config)
    queue.storage_guard()
    with (queue.state / "owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        catalog = capture_catalog(queue.backup_read_root(), Path(config["publication_catalog"]))
        evidence = [{"name": p.name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in args.evidence]
        proposal = build_disposition(catalog, authorization=args.authorization, evidence=evidence)
        if args.apply_plan:
            plan = json.loads(args.apply_plan.read_bytes())
            if (args.expected_plan_sha256 != plan["identity_sha256"]
                    or proposal["identity_sha256"] != plan["identity_sha256"]):
                parser.error("history or evidence changed; inspect a fresh dry run")
            private_json(args.output, proposal)
            active = apply_disposition({k: v for k, v in catalog.items() if k not in ["identity_sha256", "observed_at_utc"]}, args.output)
            print(json.dumps({"state": "unavailable_history_recovery_abandoned", "identity_sha256": proposal["identity_sha256"],
                              "abandoned_releases": len(active["abandoned_releases"]),
                              "abandoned_unavailable_objects": len(active["abandoned_missing_objects"]),
                              "source_objects_deleted": 0, "manifests_deleted": 0, "recovered_bytes": 0}))
        else:
            private_json(args.output, proposal)
            print(json.dumps({"state": "history_disposition_dry_run", "identity_sha256": proposal["identity_sha256"],
                              "releases": len(proposal["releases"]), "unavailable_objects": len(proposal["unavailable_objects"]),
                              "existing_source_or_metadata_deleted": False}))


if __name__ == "__main__":
    main()
