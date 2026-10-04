#!/usr/bin/env python3
"""Real local encrypted recovery of the staged code/control delivery.

This acceptance exercises full ZIP file hashes and a real isolated SQL restore;
NAS provenance, lab203 deployment and independent USB custody stay separate.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.verify_backup_delivery import verify  # noqa: E402
from stockagent.control.recovery import restore_control  # noqa: E402
from stockagent.data_sync.backup_auxiliary import verified_working_tree  # noqa: E402
from stockagent.data_sync.offhost_backup import ResticBackup, private_json  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--password-file", type=Path, required=True)
    parser.add_argument("--restic", type=Path, required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.output.exists():
        parser.error("preserve previous local acceptance receipts")
    args.output.mkdir(mode=0o700, parents=True)
    configuration = json.loads(args.config.read_bytes())
    ledger = json.loads((Path(configuration["state_root"]) / "ledger.json").read_bytes())
    pending = ledger["auxiliary_pending"]
    source = Path(configuration["transport_root"]) / pending["staging_relative"]
    envelope_id = pending["export"]["envelope_identity_sha256"]
    initial = verify(source, envelope_id)
    started = time.perf_counter()
    client = ResticBackup(args.restic, args.repository, args.password_file, cache=args.output / "cache")
    backup = client.run(["backup", "--json", "--tag", "local-code-control-acceptance", str(source)])
    summaries = [json.loads(line) for line in backup.stdout.splitlines() if line and json.loads(line).get("message_type") == "summary"]
    if len(summaries) != 1:
        raise ValueError("expected one fixed complete local snapshot")
    snapshot_id = summaries[0]["snapshot_id"]
    client.run(["check", "--read-data"])
    client.restore(snapshot_id, args.output / "restored")
    restored = args.output / "restored" / source.as_posix().lstrip("/")
    recovered = verify(restored, envelope_id)
    if (recovered["bytes_verified"], recovered["files_verified"]) != (initial["bytes_verified"], initial["files_verified"]):
        raise ValueError("independent restored transport differs")
    code = verified_working_tree(restored / "code")
    sql = restore_control(restored / "control", Path("/var/lib") / ("stockagent-backup-restore-" + uuid.uuid4().hex[:12]), pg_bin=args.pg_bin)
    result = {"state": "local_encrypted_code_and_control_recovery_verified", "restic_snapshot_id": snapshot_id,
        "envelope_identity_sha256": envelope_id, "transport": recovered, "code": code, "control": sql,
        "all_command_exit_codes_zero": True, "check_read_data_verified": True,
        "nas_restore_verified": False, "independent_usb_key_custody_verified": False,
        "complete_workflow_seconds": time.perf_counter() - started}
    private_json(args.output / "acceptance.json", result)
    print(json.dumps({k: result[k] for k in ("state", "restic_snapshot_id", "complete_workflow_seconds", "nas_restore_verified")}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
