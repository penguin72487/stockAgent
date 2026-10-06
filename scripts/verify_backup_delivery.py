#!/usr/bin/env python3
"""Portable, dependency-free byte verification of a closed backup delivery.

Runs on native Windows or Linux. This verifies the transport envelope and every
file; canonical packed decoding and PostgreSQL restoration keep their owners.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import stat

CONTRACT = "closed_packed_backup_delivery_v1"
HASH = re.compile(r"[0-9a-f]{64}\Z")


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def is_redirected(path: Path) -> bool:
    item = path.lstat()
    return stat.S_ISLNK(item.st_mode) or bool(getattr(item, "st_file_attributes", 0) & 0x400)


def regular(root: Path, relative: str) -> Path:
    part = PurePosixPath(relative)
    if (part.is_absolute() or not part.parts or ".." in part.parts
            or any(c in relative for c in "\\:\r\n\0")):
        raise ValueError("unsafe backup delivery path")
    current = root
    for name in part.parts:
        current /= name
        if is_redirected(current):
            raise ValueError("backup delivery must not traverse symlinks or junctions")
    if not stat.S_ISREG(current.lstat().st_mode):
        raise ValueError("backup delivery member is not a regular file")
    return current


def verify(root: Path, expected_identity: str, *, require_ready: bool = True,
           include_file_signatures: bool = False, workers: int = 1) -> dict:
    if type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("delivery verification needs 1 to 32 bounded workers")
    if not HASH.fullmatch(expected_identity):
        raise ValueError("a source-pinned full envelope identity is required")
    root = root.absolute()
    if any(is_redirected(p) for p in (root, *root.parents)):
        raise ValueError("backup delivery root is redirected")
    envelope = json.loads(regular(root, "backup-envelope.json").read_bytes())
    body = {k: v for k, v in envelope.items() if k != "identity_sha256"}
    if (envelope.get("contract") != CONTRACT or identity(body) != expected_identity
            or envelope.get("identity_sha256") != expected_identity
            or (require_ready and regular(root, "READY").read_text().strip() != expected_identity)):
        raise ValueError("backup delivery contract, READY or pinned identity differs")
    expected = {"backup-envelope.json", *({"READY"} if require_ready else set())}
    for row in envelope["files"]:
        if (row["relative"] in expected or not HASH.fullmatch(row["sha256"])
                or type(row["bytes"]) is not int or row["bytes"] < 0):
            raise ValueError("invalid or duplicate backup delivery member")
        expected.add(row["relative"])

    def verify_member(row):
        path = regular(root, row["relative"])
        before = path.stat()
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(4 * 1024**2), b""):
                digest.update(chunk)
        after = path.stat()
        signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if signature(before) != signature(after) or before.st_size != row["bytes"] or digest.hexdigest() != row["sha256"]:
            raise ValueError("backup delivery member changed or its content differs")
        return row["relative"], row["bytes"], list(signature(after))

    if workers == 1:
        results = [verify_member(row) for row in envelope["files"]]
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="backup-sha") as pool:
            results = list(pool.map(verify_member, envelope["files"]))
    total = sum(row[1] for row in results)
    signatures = {row[0]: row[2] for row in results} if include_file_signatures else {}
    observed = set()
    def walk_error(error):
        raise error
    for directory, subdirs, names in os.walk(root, followlinks=False, onerror=walk_error):
        for name in subdirs:
            if is_redirected(Path(directory) / name):
                raise ValueError("backup delivery contains a symlink/junction directory")
        observed.update((Path(directory) / name).relative_to(root).as_posix() for name in names)
    if observed != expected:
        raise ValueError("backup delivery file set differs from the closed envelope")
    return {"state": "transport_bytes_verified", "envelope_identity_sha256": expected_identity,
            "files_verified": len(envelope["files"]), "bytes_verified": total,
            "source_plan_identity_sha256": envelope["source_plan_identity_sha256"],
            "canonical_reconstruction_verified": False, "control_database_restore_verified": False,
            "durable_off_host_backup_verified": False, "host": platform.node(),
            **({"file_signatures": signatures} if include_file_signatures else {}),
            "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--expected-envelope-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    output = args.output.absolute()
    if output.is_relative_to(args.root.absolute()) or output.exists():
        parser.error("save a fresh receipt outside the closed backup delivery")
    os.umask(0o077)
    result = verify(args.root, args.expected_envelope_sha256, workers=args.workers)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    # On Windows run inside the ACL-protected receipt directory created by
    # prepare_lab203_backup.ps1; POSIX mode bits do not establish NTFS privacy.
    if os.name == "posix":
        output.chmod(0o600)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
