#!/usr/bin/env python3
"""One bounded recovery-gated temporary wave; primary D/NAS are preserved."""
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import acceptance, configuration, guard, retire_accepted_transport


def main():
    c = configuration();guard(c)
    base = Path(c['state_root'])
    with (base / 'transport-gc-owner.lock').open('a') as owner:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 75
        started = time.perf_counter()
        ledger_path = base / 'source-replication-ledger.json'
        ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {'deliveries': {}}
        result = {'state': 'no_accepted_transport_candidates', 'observed_at_utc': datetime.now(timezone.utc).isoformat()}
        for identity, row in ledger['deliveries'].items():
            if row.get('nas_acceptance', {}).get('state') != 'nas_archive_file_recovery_verified':
                continue
            path = base / ('cache-retirement-' + identity + '.json')
            if path.exists() and json.loads(path.read_bytes()).get('state') == 'retired':
                continue
            result.update(state='retiring_exact_accepted_transport', delivery_identity_sha256=identity)
            atomic_write_json(base / 'transport-gc-status.json', result)
            proof = acceptance(c, identity)
            result['retirement'] = retire_accepted_transport(c, identity, proof)
            result['state'] = result['retirement']['state']
            break  # One wave per invocation, independent of new-source copying.
        result.update(observed_at_utc=datetime.now(timezone.utc).isoformat(),
                      complete_workflow_seconds=time.perf_counter() - started,
                      source_files_deleted=0, nas_archive_files_deleted=0)
        atomic_write_json(base / 'transport-gc-status.json', result)
        print(json.dumps(result))
        return 75 if result['state'] == 'retirement_deferred' else 0


if __name__ == '__main__':
    raise SystemExit(main())
