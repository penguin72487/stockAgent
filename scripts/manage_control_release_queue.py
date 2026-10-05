#!/usr/bin/env python3
"""Use the production PostgreSQL queue for registered frozen code releases."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.control.release_queue import cycle, enroll, load_policy, private_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', type=Path, default=Path('/etc/stockagent/control-release-verification.json'))
    sub = parser.add_subparsers(dest='action', required=True)
    sub.add_parser('cycle')
    sub.add_parser('status')
    registration = sub.add_parser('enroll')
    registration.add_argument('receipt', type=Path)
    registration.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    policy = load_policy(args.policy)
    if args.action == 'status':
        result = private_json(Path(policy['state_root']) / 'status.json')
    else:
        dsn = os.environ.get('CONTROL_PLANE_DSN')
        if not dsn:
            raise ValueError('private control role environment required')
        result = (enroll(policy, dsn, args.receipt, args.root) if args.action == 'enroll'
                  else cycle(policy, dsn))
    print(json.dumps(result, ensure_ascii=False))
    return 1 if result.get('state') == 'degraded' else 75 if result.get('state') in {'working', 'waiting_registration'} else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except BlockingIOError:
        print(json.dumps({'state': 'owner_busy', 'verification_completed': False}))
        raise SystemExit(75)
    except Exception as error:
        # Do not expose credentials, local exception args or arbitrary process
        # output through logs/receipts. Failed bindings and attempts are retained.
        print(json.dumps({'state': 'control_verification_error', 'error_type': type(error).__name__}))
        raise SystemExit(1)
