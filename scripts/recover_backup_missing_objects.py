#!/usr/bin/env python3
"""Audit/recover exact missing packed objects from the existing Vast peer.

No lab203 SSH, remote mutations, new resources, head changes or deletions.
Transfers are accepted only when the complete bytes equal retained source refs.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.common import load_env_file  # noqa: E402
from stockagent.data_sync.offhost_backup import private_file, private_json  # noqa: E402
from stockagent.data_sync.packed_backup import object_descriptor, safe_path  # noqa: E402
from stockagent.data_sync.packed_snapshots import _install_immutable_object  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--remote-env", type=Path, default=Path("/etc/stockagent/remote-cold-artifact-ingress.env"))
    parser.add_argument("--cold-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve previous recovery receipts")
    os.umask(0o077)
    refs = json.loads(args.catalog.read_bytes())["missing_objects"]
    for row in refs:
        if object_descriptor(row["relative"]) != row["sha256"]:
            raise ValueError("missing object descriptor differs")
    load_env_file(private_file(args.remote_env), allowed_names=[
        "COLD_ARTIFACT_INGRESS_SSH_TARGET", "COLD_ARTIFACT_INGRESS_SSH_PORT", "COLD_ARTIFACT_INGRESS_IDENTITY_FILE"], override=True)
    port = os.environ["COLD_ARTIFACT_INGRESS_SSH_PORT"]
    key = private_file(Path(os.environ["COLD_ARTIFACT_INGRESS_IDENTITY_FILE"]))
    target = os.environ["COLD_ARTIFACT_INGRESS_SSH_TARGET"]
    if not port.isdecimal() or target.startswith("-") or any(c in target for c in "\n\r "):
        raise ValueError("trusted Vast SSH configuration is invalid")
    flags = ["-oBatchMode=yes", "-oStrictHostKeyChecking=yes", "-oConnectTimeout=10", "-i", str(key)]
    payload = base64.b64encode(json.dumps(refs).encode()).decode()
    script = "cd /root/stockAgent\nsource scripts/runtime_env.sh\nrun_fintech_python - <<'PY'\n" + f"""import base64,hashlib,json,os,stat
from pathlib import Path
os.nice(10)
refs=json.loads(base64.b64decode({payload!r}))
root=Path('/srv/stockagent-packed')
node=(root/'.local-state/node-id').read_text().strip()
if node!='vastai1T':raise SystemExit('unexpected existing remote cold role')
matched=[];different=[];missing=[]
for row in refs:
 path=root/row['relative']
 try:
  if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('redirected object')
  before=path.stat()
  if not stat.S_ISREG(before.st_mode) or before.st_size!=row['bytes']:raise ValueError('size differs')
  with path.open('rb') as handle:digest=hashlib.file_digest(handle,'sha256').hexdigest()
  after=path.stat()
  if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns) or digest!=row['sha256']:raise ValueError('content differs')
  matched.append(row)
 except FileNotFoundError:missing.append(row['relative'])
 except ValueError:different.append(row['relative'])
print(json.dumps({{'state':'existing_vast_missing_object_audit','node_id':node,'matched':matched,'different':different,'missing':missing,'remote_mutations':False}}))
PY
"""
    result = subprocess.run(["ssh", *flags, "-p", port, target, "bash -s"], input=script,
                            capture_output=True, text=True, timeout=180)
    if result.returncode:
        private_json(args.output, {"state": "existing_vast_recovery_audit_failed", "exit_code": result.returncode,
                                   "source_objects_changed": False, "remote_mutations": False})
        raise SystemExit(1)
    audit = json.loads(result.stdout)
    audit["catalog_identity_sha256"] = json.loads(args.catalog.read_bytes())["identity_sha256"]
    audit["applied"] = args.apply
    audit["restored"] = []
    if args.apply:
        subprocess.run(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"], check=True, capture_output=True)
        scratch_root = args.cold_root / ".local-state" / "exact-object-recovery"
        scratch_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        for row in audit["matched"]:
            destination = safe_path(args.cold_root, row["relative"])
            if destination.exists():
                continue
            with tempfile.TemporaryDirectory(dir=scratch_root) as work:
                scratch = Path(work) / "object.partial"
                copied = subprocess.run(["scp", *flags, "-P", port, "-q",
                    target + ":" + shlex.quote("/srv/stockagent-packed/" + row["relative"]), str(scratch)],
                    capture_output=True, timeout=180)
                if copied.returncode:
                    raise RuntimeError("exact-object transfer failed; no remote/source deletion")
                with scratch.open("rb") as handle:
                    digest = hashlib.file_digest(handle, "sha256").hexdigest()
                if digest != row["sha256"] or scratch.stat().st_size != row["bytes"]:
                    raise ValueError("transferred bytes differ from the exact retained source reference")
                _install_immutable_object(args.cold_root, scratch, destination, expected_sha256=digest)
                audit["restored"].append(row)
        subprocess.run(["bash", str(ROOT / "scripts/mount_packed_d_cold.sh"), "--check"], check=True, capture_output=True)
    audit["source_heads_changed"] = False
    private_json(args.output, audit)
    print(json.dumps({"state": audit["state"], "matched": len(audit["matched"]), "restored": len(audit["restored"]),
                      "missing": len(audit["missing"]), "different": len(audit["different"]), "remote_mutations": False}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
