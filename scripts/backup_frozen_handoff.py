#!/usr/bin/env python3
"""Submit sealed receiver handoffs to the existing paired NAS backup owner."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.data_sync.backup_stream import BackupStream
from stockagent.data_sync.frozen_handoff_backup import publish_handoff_backups


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/data_sync/backup_stream.json")
    parser.add_argument("--handoff", nargs=2, metavar=("NAME", "ENVELOPE_SHA256"), action="append", required=True)
    parser.add_argument("--wait-for-owner", action="store_true")
    args = parser.parse_args()
    queue = BackupStream(json.loads(args.config.read_bytes()))
    try:
        result = publish_handoff_backups(queue, [tuple(item) for item in args.handoff],
                                         wait_for_owner=args.wait_for_owner)
    except BlockingIOError:
        print(json.dumps({"state": "existing_source_owner_busy", "nas_backup_verified": False}))
        return 75
    print(json.dumps(result, ensure_ascii=False))
    try:
        notification = queue.notify_transport(wait_for_owner=False)
    except (OSError, ValueError, RuntimeError) as error:
        # Publication is already journalled; the canonical timer may retry
        # the transport notification independently of this command.
        notification = {"state": "transport_notification_deferred", "error_type": type(error).__name__,
                        "retry_by_existing_timer": True}
    print(json.dumps({"transport_notification": notification, "nas_backup_verified_by_notification": False},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
