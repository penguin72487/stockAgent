#!/usr/bin/env python3
"""Queue finite priority reads in the canonical FinLab/FinMind/TEJ owner; no API calls."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.provider_priority import _connect, inspect, register
from contextlib import closing

ROOTS = {"finlab": "data_finlab", "finmind": "data_finmind/complement",
         "tej": "data_tej", "tej_api": "data_tej/api_trial_v1"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--plan", type=Path, required=True)
    submit.add_argument("--root", type=Path)
    status = commands.add_parser("status")
    status.add_argument("--provider", choices=ROOTS, required=True)
    status.add_argument("--request-id", required=True)
    status.add_argument("--root", type=Path)
    listing = commands.add_parser('list')
    listing.add_argument('--provider', choices=ROOTS, required=True)
    listing.add_argument('--root', type=Path)
    listing.add_argument('--dataset')
    listing.add_argument('--data-id')
    listing.add_argument('--table-id')
    listing.add_argument('--match', default='')
    listing.add_argument('--state', choices=('pending', 'retry', 'failed', 'running', 'complete', 'observed_empty', 'blocked', 'all'), default='pending')
    listing.add_argument('--limit', type=int, default=32)
    args = parser.parse_args()
    if args.command == "submit":
        if args.plan.is_symlink() or not args.plan.is_file():
            parser.error("A regular plan file is required")
        with args.plan.open(encoding='utf-8') as stream:
            plan = json.load(stream)
        root = args.root or ROOT / ROOTS[plan["provider"]]
        result = register(root, plan)
    elif args.command == 'status':
        root = args.root or ROOT / ROOTS[args.provider]
        result = inspect(root, args.request_id)
        if result["provider"] != args.provider:
            parser.error("Provider/receipt mismatch")
    else:
        if args.limit < 0:
            parser.error('--limit must be nonnegative; 0 lists all matches')
        root = args.root or ROOT / ROOTS[args.provider]
        if args.provider == 'finlab':
            keys = json.loads((root / 'catalog/discovery.json').read_text())['keys']
            matched = [k for k in keys if args.match in k]
            result = {'items': matched[:args.limit] if args.limit else matched}
        else:
            where, params = ['1=1'], []
            if args.state != 'all':
                where.append('state=?'); params.append(args.state)
            else:
                where.append("state NOT LIKE 'superseded%'")
            fields = 'task_id,table_id,state,next_attempt_at_utc' if args.provider == 'tej' else 'id,table_id,state,next_attempt_utc'
            if args.provider == 'finmind':
                fields = 'dataset,data_id,partition,kind,priority,state,next_attempt_at_utc'
                for name, value in (('dataset', args.dataset), ('data_id', args.data_id)):
                    if value is not None:
                        where.append(name + '=?'); params.append(value)
                if args.table_id:
                    parser.error('FinMind uses --dataset/--data-id')
            elif args.table_id is not None:
                where.append('table_id=?'); params.append(args.table_id)
            # Read native IDs and optional filters directly. All matching
            # rows are returned only when the caller explicitly uses --limit 0.
            with closing(_connect(root)) as con:
                result = {'items': [dict(r) for r in con.execute(
                    'SELECT ' + fields + ' FROM tasks WHERE ' + ' AND '.join(where) + ' LIMIT ?',
                    (*params, args.limit or -1))]}
        result.update(provider=args.provider, root=str(root), provider_calls=0)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
