#!/usr/bin/env python3
"""Interactive scheduled-task entrypoint for the canonical TEJ desktop/queue."""
import argparse
from contextlib import nullcontext
import json
import os
import signal
from pathlib import Path
import sys
from threading import Event

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.dataset_lock import DatasetLockTimeout, exclusive_dataset_lock
from downloader.tej_startup import bootstrap, drain_interactive_transport


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT/'data_tej')
    parser.add_argument('--config', type=Path, default=ROOT/'configs/tej_history.json')
    parser.add_argument('--windows-session-id', type=int, required=True)
    parser.add_argument('--watch', action='store_true', help='Keep the logged-in WSL relay alive and recover every minute')
    args = parser.parse_args()
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: stop.set())
    lease = (exclusive_dataset_lock(args.root/'.desktop-startup.lock', provider='tej_interactive_startup', timeout_seconds=0)
             if args.watch else nullcontext())
    failures = 0
    owns_supervisor = False
    try:
        with lease:
            owns_supervisor = True
            try:
                while not stop.is_set():
                    relay = os.environ.get('WSL_INTEROP')
                    if args.watch and relay and not Path(relay).is_socket():
                        return 1  # Windows logoff/task termination destroyed this exact relay.
                    try:
                        with exclusive_dataset_lock(args.root/'.download.lock', provider='tej_smart_wizard', timeout_seconds=0):
                            result = bootstrap(args.root.resolve(), ROOT, args.windows_session_id,
                                               config=json.loads(args.config.read_text()))
                    except DatasetLockTimeout:
                        result = {'state': 'existing_canonical_writer_preserved', 'provider_queries_sent': 0}
                    print(json.dumps(result), flush=True)
                    failures = failures+1 if result['state']=='interactive_windows_transport_unavailable' else 0
                    if failures >= 3:
                        return 1  # Let the Interactive scheduled task reacquire a fresh relay.
                    if not args.watch:
                        break
                    stop.wait(60)
            finally:
                if args.watch:
                    print(json.dumps(drain_interactive_transport(args.root.resolve())), flush=True)
    except DatasetLockTimeout:
        print(json.dumps({'state': 'interactive_relay_drain_deadline_exceeded' if owns_supervisor else
                          'existing_interactive_supervisor_preserved', 'provider_queries_sent':0}))
        if owns_supervisor:
            return 1  # Original source state remains unknown; never imply a healthy handoff.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
