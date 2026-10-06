#!/usr/bin/env python3
"""Collect primary-user excerpts from penguin and Vast; sources remain read-only."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.prompt_history import CONTRACT, collect, deduplicate
from stockagent.remote_ssh import ssh_base, validate_ssh_target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new private evidence directory")
    parser.add_argument("--penguin-baseline", type=Path)
    parser.add_argument("--vast-baseline", type=Path)
    parser.add_argument("--ssh-target", default="root@114.32.64.6")
    parser.add_argument("--ssh-port", type=int, default=40032)
    parser.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    roots = ["/root/.codex/sessions", "/root/.codex/archived_sessions"]

    def baseline(path: Path | None) -> dict:
        return json.loads(path.read_text()) if path else {}

    penguin = collect("penguin", roots, baseline(args.penguin_baseline))
    source = base64.b64encode((ROOT / "stockagent/prompt_history.py").read_bytes()).decode()
    program = f"import base64;exec(base64.b64decode({source!r}));rpc()"
    command = "python3 -c " + shlex.quote(program)
    request = {"contract": CONTRACT, "node": "Vastai1T", "roots": roots,
               "baseline": baseline(args.vast_baseline)}
    results = {"penguin": penguin}
    peer_error = None
    try:
        response = subprocess.run([*ssh_base(args.identity_file, args.ssh_port), "-o", "StrictHostKeyChecking=yes",
                                   validate_ssh_target(args.ssh_target), command],
                                  input=json.dumps(request), text=True, capture_output=True, timeout=120)
        if response.returncode:
            raise ValueError(f"peer extraction failed: {response.stderr[-1000:]}")
        lines = [line for line in response.stdout.splitlines() if line.startswith('{"contract":')]
        if len(lines) != 1:
            raise ValueError("missing or ambiguous peer history response")
        vast = json.loads(lines[0])
        if vast.get("contract") != CONTRACT or vast.get("node") != "Vastai1T":
            raise ValueError("unexpected peer history contract or node")
        results["Vastai1T"] = vast
    except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
        peer_error = str(exc)
    all_candidates = []
    for node, result in results.items():
        all_candidates.extend(result.pop("candidates"))
        atomic_write_json(args.output / f"{node}_session_inventory.json", result)
    candidates = deduplicate(all_candidates)
    candidate_file = args.output / "private_user_candidates.jsonl"
    with candidate_file.open("x") as stream:
        for item in candidates:
            stream.write(json.dumps(item, ensure_ascii=False) + "\n")
    candidate_file.chmod(0o600)
    summary = {"contract": CONTRACT, "created_at_utc": datetime.now(timezone.utc).isoformat(),
               "nodes": {node: r["counts"] for node, r in results.items()},
               "deduplicated_candidates": len(candidates), "peer_error": peer_error,
               "parse_errors": {node: r["parse_errors"] for node, r in results.items()},
               "coverage": "role extraction and index, not semantic review"}
    atomic_write_json(args.output / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 2 if peer_error or any(r["parse_errors"] for r in results.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
