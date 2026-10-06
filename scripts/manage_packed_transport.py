#!/usr/bin/env python3
"""Expose exact edge-cache requests while keeping penguin's full D authority.

This manages transport ignores only. It never deletes objects, materialized
data, source files or edge state. The existing edge hydration/GC owns those.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.configure_artifact_ingress_syncthing import credentials, request
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from scripts.manage_packed_edge import _allowed_relpaths
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import private_json
from stockagent.data_sync.packed_edge_cache import render_edge_ignore
from stockagent.data_sync.syncthing_scan import _scan_request_groups
from stockagent.data_sync.rclone_edge_transport import configuration as rclone_configuration, deliver as rclone_deliver

BEGIN = "// BEGIN stockagent exact edge transport"
END = "// END stockagent exact edge transport"
LEGACY = "// Transport to the sole index-only Vast peer; full authoritative payload retained on D."
EXCLUSIONS = ["/objects/blobs/*/*", "/objects/packs/*/*"]
REMOTE_READER = r'''from pathlib import Path
import hashlib,json
paths=[Path('/var/lib/stockagent-packed-edge/state.json'),Path('/srv/stockagent-packed/.stignore-edge')]
payloads=[];signatures=[]
for p in paths:
    if any(q.is_symlink() for q in (p,*p.parents)) or not p.is_file():
        raise ValueError('edge demand must be a regular file without symlink parents')
    s=p.stat()
    if s.st_size>1024*1024:raise ValueError('edge demand exceeds bounded metadata size')
    payloads.append(p.read_bytes());signatures.append((s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns))
for p,before in zip(paths,signatures):
    s=p.stat()
    if before!=(s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns):raise ValueError('edge demand changed while reading')
print(json.dumps({'state':json.loads(payloads[0]),'ignore':payloads[1].decode().splitlines(),
    'state_sha256':hashlib.sha256(payloads[0]).hexdigest(),'ignore_sha256':hashlib.sha256(payloads[1]).hexdigest()}))
'''


def source_ignores(current: list[str], allowed: set[str], *, full_replica=False) -> list[str]:
    """Replace only our owned suffix, preserving every unrelated rule."""
    lines = list(current)
    if BEGIN in lines or END in lines:
        if lines.count(BEGIN) != 1 or lines.count(END) != 1:
            raise SnapshotError("ambiguous owned transport ignore block")
        start, end = lines.index(BEGIN), lines.index(END)
        if end < start:
            raise SnapshotError("reversed owned transport ignore block")
        lines = lines[:start] + lines[end + 1:]
    elif LEGACY in lines:
        if lines.count(LEGACY) != 1 or lines[lines.index(LEGACY):] != [LEGACY, *EXCLUSIONS]:
            raise SnapshotError("legacy transport suffix was edited; preserve it")
        lines = lines[:lines.index(LEGACY)]
    if any(line in EXCLUSIONS or line.startswith("!/objects/") for line in lines):
        raise SnapshotError("unowned payload filter needs review")
    if full_replica:
        return lines
    rendered = render_edge_ignore(allowed).decode().splitlines()
    exceptions = [line for line in rendered if line.startswith("!/")]
    block = [BEGIN, *exceptions, *EXCLUSIONS, END]
    # Syncthing's first matching rule wins, including rules from #include.
    # The producer may retain a local edge include from an earlier enrollment;
    # appending exceptions after it leaves new requested objects silently
    # excluded even while both peers report idle/100% completion.
    include = "#include .stignore-edge"
    position = lines.index(include) if include in lines else len(lines)
    return [*lines[:position], *block, *lines[position:]]


def requested_payloads(sync_root: Path, demand: dict) -> set[str]:
    """Resolve the edge request through source manifests, never remote paths."""
    state = demand["state"]
    if state.get("schema_version") != 1 or state.get("mode") != "index-only":
        raise SnapshotError("consumer has not enrolled as an index-only edge")
    if not isinstance(state.get("hydrating", {}), dict):
        raise SnapshotError("invalid edge hydration state")
    allowed = _allowed_relpaths(sync_root, state)
    # A state/ignore transition is retried. Do not serve arbitrary negations,
    # clear an in-flight request, or guess the requested release's identity.
    expected = render_edge_ignore(allowed).decode().splitlines()
    if demand["ignore"] != expected:
        raise SnapshotError("edge state and exact ignore request are not yet consistent")
    return allowed


def observe_remote(args) -> dict:
    command = [*_ssh_base(args.identity_file, args.ssh_port),
               _validate_ssh_target(args.ssh_target),
               "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"]
    result = subprocess.run(command, input=REMOTE_READER, capture_output=True,
                            text=True, timeout=35, check=True)
    return json.loads(next(line for line in reversed(result.stdout.splitlines()) if line.startswith("{")))


def reconcile(args, api, demand_reader=observe_remote, *, rclone_policy=None, payload_delivery=rclone_deliver) -> dict:
    subprocess.run(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"],
                   capture_output=True, text=True, timeout=30, check=True)
    folder = api("/rest/config/folders/stockagent-packed")
    identity = api("/rest/system/status")["myID"]
    devices = api("/rest/config/devices")
    own = [d for d in devices if d.get("name") == "penguin"]
    peer = [d for d in devices if d.get("name") == "vastai1T"]
    if (len(own) != 1 or own[0]["deviceID"] != identity or len(peer) != 1
            or folder.get("path") != str(args.sync_root) or folder.get("paused")):
        raise SnapshotError("transport role, root or configured peer changed")
    peer_id = peer[0]["deviceID"]
    if rclone_policy and (rclone_policy['producer_device_id'] != identity or rclone_policy['receiver_device_id'] != peer_id):
        raise SnapshotError('rclone transport is not enrolled to these exact peers')
    paired = {d["deviceID"] for d in folder["devices"]}
    current = api("/rest/db/ignores", {"folder": "stockagent-packed"})["ignore"]
    full_replica = paired != {identity, peer_id}
    if rclone_policy and full_replica:
        raise SnapshotError('immutable edge SFTP requires its sole index-only peer')
    demand = None
    if full_replica:
        allowed = set()
    else:
        connection = api("/rest/system/connections")["connections"].get(peer_id, {})
        if not connection.get("connected") or not connection.get("crypto"):
            raise SnapshotError("edge is not connected over its authenticated transport")
        demand = demand_reader(args)
        allowed = requested_payloads(args.sync_root, demand)
    # Metadata/index keeps the existing private channel. Payloads have exactly
    # one transport writer: source Syncthing exceptions are removed before
    # this owner publishes any rclone object at the edge.
    wanted = source_ignores(current, set() if rclone_policy else allowed, full_replica=full_replica)
    changed = wanted != current
    pending_path = args.state_root / "pending-scan.json"
    pending = json.loads(pending_path.read_text()) if pending_path.is_file() else None
    if changed and args.apply:
        # GET again immediately before mutation. A different concurrent editor
        # remains visible and is never silently overwritten.
        if api("/rest/db/ignores", {"folder": "stockagent-packed"})["ignore"] != current:
            raise SnapshotError("source ignores changed during reconciliation")
        previous = {line[2:] for line in current if line.startswith("!/objects/")}
        paths = previous | allowed | set(pending.get("paths", []) if pending else [])
        # Durable intent precedes mutation: a POST timeout or a killed scan
        # cannot make the next unchanged-allowlist attempt skip its scans.
        pending = {"schema_version": 1, "paths": sorted(paths),
                   "full_scan": full_replica or bool(pending and pending.get("full_scan")),
                   "wanted_ignore": wanted}
        private_json(pending_path, pending)
        api("/rest/db/ignores", {"folder": "stockagent-packed"},
            method="POST", payload={"ignore": wanted}, timeout=120)
    scan_requests = 0
    if pending and args.apply:
        if (pending.get("schema_version") != 1 or pending.get("wanted_ignore") != wanted
                or api("/rest/db/ignores", {"folder": "stockagent-packed"})["ignore"] != wanted):
            raise SnapshotError("pending transport scan does not match the current source policy")
        if pending["full_scan"]:
            api("/rest/db/scan", {"folder": "stockagent-packed"}, method="POST", timeout=120)
            scan_requests = 1
        else:
            # Reuse the canonical bounded object groups, without scanning the
            # metadata paths: these immutable releases are already published.
            groups = _scan_request_groups("edge-transport", tuple(pending["paths"]),
                full_objects_scan=False, batch_object_paths=True)[:-2]
            for group in groups:
                query = urllib.parse.urlencode({"folder": "stockagent-packed", "sub": group}, doseq=True)
                api("/rest/db/scan?" + query, method="POST", timeout=120)
                scan_requests += 1
        pending_path.unlink()
    payload = payload_delivery(args, rclone_policy, demand, allowed) if rclone_policy else None
    return {"state": "immutable_rclone_edge_transport" if rclone_policy else "full_replica_transport" if full_replica else "exact_edge_transport",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "applied": args.apply, "changed": changed,
        "scan_requests_acknowledged": scan_requests,
        "requested_payload_count": len(allowed), "allowed_payload_paths": sorted(allowed),
        "remote_state_sha256": demand.get("state_sha256") if demand else None,
        "remote_ignore_sha256": demand.get("ignore_sha256") if demand else None,
        "before_ignore_sha256": hashlib.sha256(json.dumps(current).encode()).hexdigest(),
        "after_ignore_sha256": hashlib.sha256(json.dumps(wanted).encode()).hexdigest(),
        "authoritative_payload_deleted": False, "remote_state_modified": False,
        "payload_transport": payload,
        "peer_delivery_verified": payload.get('all_requested_objects_sha256_verified') is True if payload else False}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ssh-target", default=os.environ.get("COLD_ARTIFACT_INGRESS_SSH_TARGET"))
    parser.add_argument("--ssh-port", type=int, default=int(os.environ.get("COLD_ARTIFACT_INGRESS_SSH_PORT", "22")))
    parser.add_argument("--identity-file", type=Path,
        default=Path(os.environ["COLD_ARTIFACT_INGRESS_IDENTITY_FILE"]) if os.environ.get("COLD_ARTIFACT_INGRESS_IDENTITY_FILE") else None)
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--state-root", type=Path, default=Path("/var/lib/stockagent-packed-transport"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    os.umask(0o077)
    args.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (args.state_root / "owner.lock").open("a+b") as owner:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if not args.ssh_target or args.identity_file is None:
                raise SnapshotError("reuse the configured private ingress SSH identity")
            base, key = credentials()
            def api(path, query=None, **kw):
                return request(base, key, path, query, **kw)
            result = reconcile(args, api, rclone_policy=rclone_configuration())
            private_json(args.state_root / "latest.json", result)
            print(json.dumps({k: v for k, v in result.items() if k != "allowed_payload_paths"}))
            return 0
        except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as exc:
            result = {"state": "transport_update_pending_retry", "error_type": type(exc).__name__,
                      "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                      "source_ignore_outcome": "not_reconfirmed_after_error",
                      "authoritative_payload_deleted": False}
            private_json(args.state_root / "latest.json", result)
            print(json.dumps(result), file=sys.stderr)
            return 75


if __name__ == "__main__":
    raise SystemExit(main())
