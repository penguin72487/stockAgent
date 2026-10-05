#!/usr/bin/env python3
"""Merge custom Codex skills with an SSH peer; default plans and writes receipts."""
from __future__ import annotations

import argparse
import base64
import fcntl
import json
from pathlib import Path
import shlex
import subprocess
import sys
import uuid
from datetime import datetime, timezone

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.remote_ssh import ssh_base, validate_ssh_target, validate_remote_absolute
from stockagent.skill_sync import CONTRACT, SkillSyncError, content, inventory, install, merge_files
from stockagent.data_sync.desync_snapshots import atomic_write_json


def validate_entrypoints(files: dict) -> None:
    names = set()
    for key, item in files.items():
        scope, name, member = key.split("/", 2)
        if member != "SKILL.md":
            continue
        text = content(item).decode("utf-8")
        if not text.startswith("---\n") or len(text.split("---", 2)) != 3:
            raise SkillSyncError(f"invalid skill frontmatter: {key}")
        try:
            meta = yaml.safe_load(text.split("---", 2)[1])
        except yaml.YAMLError as exc:
            raise SkillSyncError(f"invalid skill YAML: {key}") from exc
        if (not isinstance(meta, dict) or meta.get("name") != name
                or not isinstance(meta.get("description"), str) or not meta["description"].strip()):
            raise SkillSyncError(f"skill identity or description invalid: {key}")
        if name in names:
            raise SkillSyncError(f"duplicate skill identity across scopes: {name}")
        names.add(name)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    p.add_argument("--local-user-root", default=str(Path.home() / ".codex/skills"))
    p.add_argument("--local-repo-root", default=str(ROOT / ".agents/skills"))
    p.add_argument("--remote-user-root", default="/root/.codex/skills")
    p.add_argument("--remote-repo-root", default="/root/stockAgent/.agents/skills")
    p.add_argument("--state-root", type=Path, default=Path.home() / ".codex/skill-sync/vastai1T")
    p.add_argument("--output", type=Path, required=True, help="new receipt directory")
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    target = validate_ssh_target(args.ssh_target)
    local_roots = {"user": args.local_user_root, "repo": args.local_repo_root}
    remote_roots = {"user": validate_remote_absolute(args.remote_user_root, "remote user skills"),
                    "repo": validate_remote_absolute(args.remote_repo_root, "remote repo skills")}
    args.output.mkdir(parents=True, exist_ok=False)
    args.state_root.mkdir(parents=True, exist_ok=True)
    with (args.state_root / "owner.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        peer = {"target": target, "port": args.ssh_port, "local_roots": local_roots, "remote_roots": remote_roots}
        baseline = args.state_root / "base.json"
        state = json.loads(baseline.read_text()) if baseline.exists() else {}
        if state and (state.get("contract") != CONTRACT or state.get("peer") != peer):
            raise SkillSyncError("baseline belongs to another peer or path mapping")
        # This fixed, stdlib-only implementation runs on the peer without installing code or dependencies.
        source = base64.b64encode((ROOT / "stockagent/skill_sync.py").read_bytes()).decode()
        program = f"import base64;exec(base64.b64decode({source!r}));rpc()"
        command = "python3 -c " + shlex.quote(program)
        ssh = [*ssh_base(args.identity_file, args.ssh_port), "-o", "StrictHostKeyChecking=yes", target, command]

        def call(action: str, **values) -> dict:
            request = {"contract": CONTRACT, "action": action, "roots": remote_roots, **values}
            r = subprocess.run(ssh, input=json.dumps(request), text=True, capture_output=True, timeout=60)
            if r.returncode:
                raise SkillSyncError(f"peer action failed: {r.stderr[-2000:]}")
            # Some Vast login wrappers print a welcome banner before the one JSON response.
            lines = [line for line in r.stdout.splitlines() if line.startswith('{"contract":')]
            if len(lines) != 1:
                raise SkillSyncError("missing or ambiguous peer response")
            result = json.loads(lines[0])
            if result.get("contract") != CONTRACT:
                raise SkillSyncError("peer response contract mismatch")
            return result

        local = inventory(local_roots)
        remote = call("inventory")["files"]
        atomic_write_json(args.output / "before.json", {"peer": peer, "local": local, "remote": remote, "base": state.get("files", {})})
        merged, conflicts = merge_files(local, remote, state.get("files", {}))
        changes = {"local": [k for k in merged if merged[k] != local.get(k)],
                   "remote": [k for k in merged if merged[k] != remote.get(k)]}
        receipt = {"contract": CONTRACT, "peer": peer, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                   "apply": args.apply, "members": len(merged), "skills": sorted({"/".join(k.split('/')[:2]) for k in merged}),
                   "changes": changes, "conflicts": conflicts, "state": "conflict" if conflicts else "planned"}
        atomic_write_json(args.output / "receipt.json", receipt)
        if conflicts:
            print(json.dumps(receipt, ensure_ascii=False, indent=2))
            return 2
        validate_entrypoints(merged)
        if args.apply:
            if inventory(local_roots) != local:
                raise SkillSyncError("local skills changed since planning")
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
            remote_result = call("install", expected=remote, desired=merged, run_id=run_id)
            receipt["remote_install"] = {k: v for k, v in remote_result.items() if k != "files"}
            atomic_write_json(args.output / "receipt.json", receipt | {"state": "remote_applied"})
            local_result = install(local_roots, local, merged, run_id)
            receipt["local_install"] = {k: v for k, v in local_result.items() if k != "files"}
            after_local, after_remote = inventory(local_roots), call("inventory")["files"]
            if after_local != merged or after_remote != merged:
                raise SkillSyncError("post-install members differ; baseline not advanced")
            atomic_write_json(args.output / "after.json", {"local": after_local, "remote": after_remote})
            atomic_write_json(baseline, {"contract": CONTRACT, "peer": peer, "files": merged})
            receipt["state"] = "synchronized"
            receipt["baseline"] = str(baseline)
            atomic_write_json(args.output / "receipt.json", receipt)
        print(json.dumps(receipt, ensure_ascii=False, indent=2))
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SkillSyncError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
