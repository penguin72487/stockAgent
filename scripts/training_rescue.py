#!/usr/bin/env python3
"""Confirmation-gated Vast recovery; no rental, Destroy or automatic failover."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.training_rescue import Rescue, RescueError, load_policy  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--policy", type=Path, default=ROOT / "configs/training_rescue.json")
    p.add_argument("--owner-wait-seconds", type=int, default=0,
                   help="Bounded 0..1200s admission wait; default skips a busy owner")
    commands = p.add_subparsers(dest="action", required=True)
    commands.add_parser("status", help="Read preserved progress; never starts a job")
    commands.add_parser("cycle", help="Monitor enrolled runs and preserve committed checkpoints")
    r = commands.add_parser("register", help="Enroll an explicit immutable recovery contract")
    r.add_argument("--spec", type=Path, required=True)
    for name in ("capture", "seal-bootstrap", "verify-sources"):
        s = commands.add_parser(name)
        s.add_argument("--run", required=True)
    fence = commands.add_parser("fence-provider", help="Read-only proof that the exact old Vast instance is Stopped; never calls Stop")
    fence.add_argument("--run", required=True)
    fence.add_argument("--api-key-file", type=Path, help="0600 file; otherwise use VAST_API_KEY environment")
    fence = commands.add_parser("fence-guardian", help="Stop only the enrolled managed run and verify its descendants are gone")
    fence.add_argument("--run", required=True)
    k = commands.add_parser("key", help="Issue an incident-only public SSH key; private key stays local")
    k.add_argument("--hours", type=int, default=24)
    revoke = commands.add_parser("revoke-key", help="Revoke only this incident key, not ongoing run supervision")
    revoke.add_argument("--key-id", required=True)
    plan = commands.add_parser("plan", help="Pin a user-selected SSH destination; no target writes/login")
    plan.add_argument("--run", required=True)
    plan.add_argument("--host", required=True)
    plan.add_argument("--port", type=int, required=True)
    plan.add_argument("--user", default="root")
    plan.add_argument("--key-id", required=True)
    plan.add_argument("--host-key-file", type=Path, required=True)
    recover = commands.add_parser("recover", help="Execute only the exact user-confirmed plan")
    recover.add_argument("--plan-id", required=True)
    recover.add_argument("--confirm", required=True)
    recover.add_argument("--old-node-stopped-receipt", type=Path, required=True)
    args = p.parse_args()
    os.umask(0o077)
    rescue = Rescue(load_policy(args.policy))
    if args.action == "status":
        result = rescue.status()
    else:
        with rescue.lock(wait_seconds=args.owner_wait_seconds):
            if args.action == "register":
                result = rescue.register(json.loads(args.spec.read_bytes()))
            elif args.action == "capture":
                result = rescue.capture(args.run)
            elif args.action == "seal-bootstrap":
                result = rescue.seal_bootstrap(args.run)
            elif args.action == "verify-sources":
                result = rescue.verify_sources(args.run)
            elif args.action == "cycle":
                result = rescue.cycle()
            elif args.action == "fence-provider":
                result = rescue.fence_provider(args.run, api_key_file=args.api_key_file)
            elif args.action == "fence-guardian":
                result = rescue.fence_guardian(args.run)
            elif args.action == "key":
                result = rescue.issue_key(hours=args.hours)
            elif args.action == "revoke-key":
                result = rescue.revoke_key(args.key_id)
            elif args.action == "plan":
                result = rescue.plan(args.run, host=args.host, port=args.port, user=args.user,
                    key_id=args.key_id, host_key_line=args.host_key_file.read_text())
            else:
                result = rescue.recover(args.plan_id, confirm=args.confirm,
                                       old_node_stopped_receipt=args.old_node_stopped_receipt)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BlockingIOError:
        print(json.dumps({"state": "owner_busy_no_duplicate_operation", "retry_later": True}))
        raise SystemExit(75)
    except (RescueError, OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"state": "rescue_rejected", "error_type": type(exc).__name__,
                          "reason": str(exc)[:400]}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(65)
