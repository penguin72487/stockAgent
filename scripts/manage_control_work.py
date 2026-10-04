#!/usr/bin/env python3
"""Opt-in shared engineering work; the first handler only verifies frozen code."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import socket
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.control.contracts import WorkSpec  # noqa: E402
from stockagent.control.postgres import ControlStore  # noqa: E402
from stockagent.control.worker import execute_one, node_observation  # noqa: E402


def json_value(value):
    if isinstance(value, (datetime, uuid.UUID)):
        return str(value)
    raise TypeError(type(value).__name__)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema', default='stockagent_control')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('init')
    commands.add_parser('status')
    commands.add_parser('expire-leases')
    sub = commands.add_parser('submit')
    sub.add_argument('spec', type=Path)
    sub = commands.add_parser('register-node')
    sub.add_argument('--node', default=socket.gethostname())
    sub.add_argument('--root', type=Path, default=ROOT)
    sub = commands.add_parser('worker-once')
    sub.add_argument('--node', default=socket.gethostname())
    sub.add_argument('--worker', default=f'{socket.gethostname()}-{os.getpid()}')
    sub.add_argument('--receipt', required=True, type=Path)
    sub.add_argument('--root', required=True, type=Path)
    sub.add_argument('--output', required=True, type=Path)
    sub.add_argument('--lease-seconds', type=int, default=60)
    args = parser.parse_args(argv)
    dsn = os.environ.get('CONTROL_PLANE_DSN')
    if not dsn:
        raise ValueError('CONTROL_PLANE_DSN is required in a private role environment')
    if args.command == 'worker-once':
        result = execute_one(dsn, schema=args.schema, node_id=args.node, worker_id=args.worker,
                             receipt=args.receipt, root=args.root, output=args.output,
                             lease_seconds=args.lease_seconds)
    else:
        with ControlStore(dsn, schema=args.schema) as store:
            if args.command == 'init':
                store.initialize(); result = {'state': 'initialized', 'schema': args.schema}
            elif args.command == 'status':
                result = store.snapshot()
            elif args.command == 'expire-leases':
                result = {'expired_attempts': store.expire_leases()}
            elif args.command == 'register-node':
                store.register_node(args.node, **node_observation(args.root))
                result = {'node_id': args.node, 'state': 'registered'}
            else:
                result = store.submit(WorkSpec.from_dict(json.loads(args.spec.read_text())))
    print(json.dumps(result, ensure_ascii=False, default=json_value))
    return 1 if result.get('state') in {'attempt_failed', 'lease_lost'} else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({'state': 'control_error', 'error_type': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1)
