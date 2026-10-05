#!/usr/bin/env python3
"""Use only the fixed local immutable NAS relay policy."""
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.immutable_replication import relay_cycle

try:
    result = relay_cycle(json.loads(Path('/etc/lab203-backup/lake-relay.json').read_bytes()))
except BlockingIOError:
    print(json.dumps({"state": "existing_backup_owner_busy"}))
    raise SystemExit(75)
print(json.dumps(result))
raise SystemExit(0 if result["state"] == "ready" else 1)
