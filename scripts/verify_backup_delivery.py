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
import itertools
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import stat
import threading

CONTRACT = "closed_packed_backup_delivery_v1"
HASH = re.compile(r"[0-9a-f]{64}\Z")
# On closed Penguin D-to-NAS deliveries, the 1.4 KiB and 1 MiB samples were
# faster to hash via DrvFS than to start native PowerShell batches. Route the
# intervening small-file range the same way; retain native I/O for large files.
PORTABLE_HASH_MAX_BYTES = 1024**2
NATIVE_HASH_BATCH_MAX_BYTES = 16 * 1024**3


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def is_redirected(path: Path) -> bool:
    item = path.lstat()
    return stat.S_ISLNK(item.st_mode) or bool(getattr(item, "st_file_attributes", 0) & 0x400)


def _regular_with_info(root: Path, relative: str):
    part = PurePosixPath(relative)
    if (part.is_absolute() or not part.parts or ".." in part.parts
            or any(c in relative for c in "\\:\r\n\0")):
        raise ValueError("unsafe backup delivery path")
    current = root
    for name in part.parts:
        current /= name
        item = current.lstat()
        if stat.S_ISLNK(item.st_mode) or bool(getattr(item, "st_file_attributes", 0) & 0x400):
            raise ValueError("backup delivery must not traverse symlinks or junctions")
    if not stat.S_ISREG(item.st_mode):
        raise ValueError("backup delivery member is not a regular file")
    return current, item


def regular(root: Path, relative: str) -> Path:
    return _regular_with_info(root, relative)[0]


def _guard_direct_d_delivery(root: Path):
    """Pin the same enrolled D authority when bypassing the native hasher."""
    from stockagent.data_sync.windows_cold_io import ALIASES, _configuration_on_checked_mount
    alias = next((alias for alias, _ in ALIASES if root.is_relative_to(alias)), None)
    if alias is None:
        return None
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    # The envelope resides in the same closed delivery as its members. The
    # canonical adapter checks the registered D volume and ingress alias;
    # no native process or write is launched for this guard.
    _configuration_on_checked_mount(root / 'backup-envelope.json', mode='hash', expected_bytes=0)
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_dev != alias.stat().st_dev:
        raise ValueError('backup delivery does not reside on its enrolled D volume')
    return info.st_dev, info.st_ino


def _descriptor_identity(info, *, windows: bool | None = None):
    """Compare path and fd identities without mixing Windows ctime meanings.

    On Windows, lstat and fstat can report different ctime values for the
    same open file. Each clock is still compared with its own later reading.
    """
    if windows is None:
        windows = os.name == 'nt'
    common = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
              info.st_mode, info.st_nlink)
    return common if windows else (*common, info.st_ctime_ns)


def _native_hash_groups(rows):
    """Respect both the adapter's member count and total byte bounds."""
    groups, group, total = [], [], 0
    for row in rows:
        if group and (len(group) == 16 or total + row['bytes'] > NATIVE_HASH_BATCH_MAX_BYTES):
            groups.append(group)
            group, total = [], 0
        group.append(row)
        total += row['bytes']
    if group:
        groups.append(group)
    return groups


def verify(root: Path, expected_identity: str, *, require_ready: bool = True,
           include_file_signatures: bool = False, workers: int = 1,
           native_windows_io: bool = False) -> dict:
    if type(workers) is not int or not 1 <= workers <= 32:
        raise ValueError("delivery verification needs 1 to 32 bounded workers")
    if type(native_windows_io) is not bool:
        raise ValueError("native delivery I/O must be an explicit boolean")
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
    direct_d_identity = None
    if native_windows_io and any(row['bytes'] <= PORTABLE_HASH_MAX_BYTES for row in envelope['files']):
        direct_d_identity = _guard_direct_d_delivery(root)

    def verify_member(row):
        path, before = _regular_with_info(root, row["relative"])
        signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        stable = lambda s: (*signature(s), s.st_mode, s.st_nlink)
        if native_windows_io and row['bytes'] >= 8 * 1024**2:
            # Optional Penguin adapter; the portable lab203/Windows verifier
            # keeps its dependency-free default. The enrolled adapter checks
            # the actual D volume and stable inode while hashing every byte.
            from stockagent.data_sync.windows_cold_io import hash_file
            digest_hex = hash_file(path)
        else:
            digest = hashlib.sha256()
            # Pin the opened inode as well as the path generation, and do not
            # follow a final-component symlink substituted after regular().
            flags = (os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
                     | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
                     | getattr(os, "O_BINARY", 0))
            fd = os.open(path, flags)
            with os.fdopen(fd, "rb") as stream:
                opened = os.fstat(stream.fileno())
                if not stat.S_ISREG(opened.st_mode) or _descriptor_identity(before) != _descriptor_identity(opened):
                    raise ValueError("backup delivery member changed or its content differs")
                for chunk in iter(lambda: stream.read(4 * 1024**2), b""):
                    digest.update(chunk)
                if stable(opened) != stable(os.fstat(stream.fileno())):
                    raise ValueError("backup delivery member changed or its content differs")
            digest_hex = digest.hexdigest()
        _, after = _regular_with_info(root, row["relative"])
        if stable(before) != stable(after) or before.st_size != row["bytes"] or digest_hex != row["sha256"]:
            raise ValueError("backup delivery member changed or its content differs")
        return row["relative"], row["bytes"], list(signature(after))

    native_groups, portable = [], []
    if native_windows_io:
        from stockagent.data_sync.windows_cold_io import windows_path
        eligible = []
        def classify(row):
            if row['bytes'] <= PORTABLE_HASH_MAX_BYTES:
                # verify_member will perform the same no-follow path and full
                # SHA checks; avoid redundant classification I/O for these.
                return row, False
            path = regular(root, row["relative"])
            return row, windows_path(path) is not None and row['bytes'] <= 8 * 1024**3
        # Classification performs real no-follow path checks. On DrvFS those
        # metadata reads used to serialize the entire native hash pipeline.
        # Keep every check and overlap them within the same worker budget.
        if workers == 1:
            classified = map(classify, envelope['files'])
            for row, native in classified:
                (eligible if native else portable).append(row)
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='backup-path') as pool:
                for row, native in pool.map(classify, envelope['files']):
                    (eligible if native else portable).append(row)
        native_groups = _native_hash_groups(eligible)
    else:
        portable = envelope["files"]

    def verify_native_group(rows, cancel_event=None):
        # The enrolled adapter hashes all original bytes on Windows and
        # returns one proof per file. Batch startup/mount checks are bounded;
        # exact path, physical volume and stable inode checks remain intact.
        from stockagent.data_sync.windows_cold_io import hash_many
        paths = [regular(root, row["relative"]) for row in rows]
        before = [path.stat() for path in paths]
        hashes = (hash_many(paths) if cancel_event is None
                  else hash_many(paths, cancel_event=cancel_event))
        result = []
        signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        for row, path, prior in zip(rows, paths, before):
            after = path.stat()
            if (signature(prior) != signature(after) or prior.st_size != row["bytes"]
                    or hashes[path] != row["sha256"]):
                raise ValueError("backup delivery member changed or its content differs")
            result.append((row["relative"], row["bytes"], list(signature(after))))
        return result

    if workers == 1:
        results = [item for group in native_groups for item in verify_native_group(group)]
        results.extend(verify_member(row) for row in portable)
    else:
        cancel_event = threading.Event()
        first_failure = []
        failure_lock = threading.Lock()
        def verify_task(task):
            try:
                if cancel_event.is_set():
                    raise RuntimeError('Verification cancelled after a peer failed')
                native, value = task
                return verify_native_group(value, cancel_event) if native else [verify_member(value)]
            except Exception as error:
                with failure_lock:
                    if not first_failure:
                        first_failure.append(error)
                        cancel_event.set()
                raise
        # Queue both kinds under the same worker budget. A slow final native
        # batch should not leave the other workers idle while portable files
        # wait for the whole native phase to finish. Preserve result order.
        tasks = itertools.chain(((True, group) for group in native_groups),
                                ((False, row) for row in portable))
        try:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="backup-sha") as pool:
                results = [item for group in pool.map(verify_task, tasks) for item in group]
        except Exception:
            # map yields in submission order, so a cancelled earlier peer can
            # surface first. Preserve the actual error that triggered it.
            if first_failure:
                raise first_failure[0]
            raise
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
    if direct_d_identity is not None and _guard_direct_d_delivery(root) != direct_d_identity:
        raise ValueError("backup delivery D mount or root identity changed during verification")
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
