"""Reuse sealed benchmark inputs without linking a mutable source or database.

Each experiment still owns its logical input names. A small committed receipt
indexes readonly inputs in sibling experiments; the index itself is not proof.
The existing immutable-payload publisher verifies complete bytes before linking.
There is no payload pool, service, automatic deletion or cold-recovery claim.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory
from stockagent.data_sync.immutable_cache_files import (
    link_immutable_payload, payload_info, stable_payload_hash,
)

CONTRACT = "immutable_benchmark_inputs_v1"
RECEIPT = "immutable-inputs.json"
MAX_METADATA_BYTES = 8 * 1024**2
MAX_DONOR_ROOTS = 1024


def _completed_comparison(fixture: dict, accepted: dict, objects: dict) -> bool:
    """Recognize producer receipts, not an assumed equality of contract names.

    The canonical OpenBB selector benchmark deliberately uses a fixture ABI and
    a different comparison ABI. Bind that exact pair to its original sidecars;
    do not admit arbitrary mismatched contracts or infer raw-payload validity.
    """
    fixture_contract = "openbb_private_full_manifest_source_selector_v1"
    comparison_contract = "openbb_full_manifest_unassigned_source_abba_v1"
    if (fixture.get("contract") != fixture_contract
            and accepted.get("contract") != comparison_contract):
        return (isinstance(accepted.get("state"), str)
                and accepted["state"].startswith("accepted_")
                and isinstance(fixture.get("contract"), str) and bool(fixture["contract"])
                and accepted.get("contract") == fixture["contract"]
                and isinstance(accepted.get("trials"), list) and bool(accepted["trials"])
                and all(isinstance(row, dict) and row.get("state") == "completed_trial"
                        for row in accepted["trials"]))

    def sha256(value):
        return (isinstance(value, str) and len(value) == 64
                and all(character in "0123456789abcdef" for character in value))

    if (fixture.get("contract") != fixture_contract
            or accepted.get("contract") != comparison_contract
            or accepted.get("state") != "accepted_bounded_phase_comparison"
            or not all(sha256(fixture.get(name))
                       for name in ("manifest_sha256", "selector_sha256", "module_sha256"))
            or type(fixture.get("tasks")) is not int or fixture["tasks"] <= 0
            or type(accepted.get("tasks")) is not int or accepted["tasks"] != fixture["tasks"]
            or accepted.get("fixture_manifest_sha256") != fixture["manifest_sha256"]
            or accepted.get("exact_full_ordered_source_parity") is not True
            or any(type(row.get("provider_calls")) is not int or row["provider_calls"] != 0
                   or row.get("production_writes") is not False for row in (fixture, accepted))):
        return False
    names = ("01-original.json", "02-candidate.json", "03-candidate.json", "04-original.json")
    variants = ("original", "candidate", "candidate", "original")
    trials = accepted.get("trials")
    if not isinstance(trials, list) or len(trials) != len(names):
        return False
    parity = set()
    for name, variant, row in zip(names, variants, trials, strict=True):
        if (not isinstance(row, dict) or row.get("name") != name
                or {key: value for key, value in row.items() if key != "name"} != objects.get(name)
                or row.get("state") != "completed_trial" or row.get("variant") != variant
                or type(row.get("limit")) is not int or row["limit"] != 32768
                or row.get("fixture_manifest_sha256") != fixture["manifest_sha256"]
                or row.get("shared_code_sha256") != fixture["module_sha256"]
                or type(row.get("provider_calls")) is not int or row["provider_calls"] != 0
                or row.get("production_writes") is not False
                or row.get("model_runtime_imported") is not False
                or type(row.get("selected_files")) is not int or row["selected_files"] < 0
                or not sha256(row.get("full_ordered_source_contract_sha256"))):
            return False
        parity.add((row["selected_files"], row["full_ordered_source_contract_sha256"]))
    return len(parity) == 1


def completed_benchmark_evidence(source: Path, *, destination: Path | None = None) -> dict:
    """Bounded exact receipts for a completed case, never a numerical oracle.

    Shared by capture admission and local retirement: incomplete/unknown work
    cannot pass merely because a caller skipped the outer command's review.
    Full original/cold hashes and consumer gates are independent. Only small
    producer JSON is retained locally; mutable working DBs are never linked.
    """
    if source.resolve(strict=True) != source.absolute():
        raise SnapshotError('benchmark evidence source is redirected')
    records, objects, contents = [], {}, {}
    def unique(pairs):
        value = {}
        for key, child in pairs:
            if key in value:
                raise ValueError("duplicate benchmark evidence key")
            value[key] = child
        return value
    paths = sorted(source.glob("*.json"))
    if len(paths) > 128:
        raise SnapshotError('benchmark receipt count exceeds its bound')
    total = 0
    for path in paths:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(descriptor, 'rb') as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > 8 * 1024**2:
                raise SnapshotError('benchmark evidence is not bounded regular metadata')
            total += before.st_size
            if total > 32 * 1024**2:
                raise SnapshotError('benchmark evidence exceeds its total byte budget')
            raw = handle.read(8 * 1024**2 + 1)
            signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            if (len(raw) != before.st_size or signature(before) != signature(os.fstat(handle.fileno()))
                    or signature(before) != signature(path.lstat())):
                raise SnapshotError('benchmark evidence changed during capture')
        try:
            value = json.loads(raw, object_pairs_hook=unique)
        except (ValueError, RecursionError) as error:
            raise SnapshotError('benchmark evidence is not readable JSON') from error
        if not isinstance(value, dict):
            raise SnapshotError('benchmark evidence is not an object')
        objects[path.name] = value
        contents[path.name] = raw
        records.append({'path': path.name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    fixture, accepted = objects.get('fixture.json', {}), objects.get('abba-receipt.json', {})
    if not _completed_comparison(fixture, accepted, objects):
        raise SnapshotError('benchmark has no supported completed producer comparison receipt')
    if destination is not None:
        if destination.resolve(strict=False) != destination.absolute():
            raise SnapshotError('benchmark evidence destination is redirected')
        destination.mkdir(mode=0o700, parents=True, exist_ok=True)
        for name, raw in contents.items():
            target = destination / name
            if target.exists() or target.is_symlink():
                info = target.lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_size != len(raw)
                        or info.st_nlink != 1 or target.read_bytes() != raw):
                    raise SnapshotError('preserved benchmark evidence differs; reconcile the original receipt')
            else:
                atomic_write_bytes(target, raw, mode=0o600)
    return {'contract': 'retained_benchmark_evidence_v1', 'files': records, 'bytes': total,
            'producer_contract': fixture['contract'], 'producer_state': accepted['state'],
            'scope': 'producer_declared_completion_not_benchmark_reexecution'}


def _relative(value: str) -> Path:
    if not isinstance(value, str):
        raise ValueError("fixture member must be a relative path")
    parsed = PurePosixPath(value)
    if not value or parsed.is_absolute() or any(p in {"", ".", ".."} for p in value.split("/")):
        raise ValueError("fixture member escapes its experiment")
    return Path(*parsed.parts)


def _metadata(path: Path) -> dict:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_METADATA_BYTES:
            raise ValueError("unsafe fixture input receipt")
        raw = handle.read(MAX_METADATA_BYTES + 1)
        if len(raw) > MAX_METADATA_BYTES:
            raise ValueError("fixture input receipt grew beyond its bound")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("fixture input receipt is not an object")
    return value


class ImmutableFixtureInputs:
    """One fresh private capture; copied/linked members are immutable thereafter.

    Only source inputs are eligible. Mutable SQLite variants and trial outputs
    must never be passed to this owner. Unknown, busy, writable or corrupt donors
    are misses, not reasons to trust another experiment's acceptance result.
    """

    def __init__(self, root: Path):
        self.root = root.absolute()
        if self.root.resolve(strict=True) != self.root or not self.root.is_dir():
            raise ValueError("fixture root is redirected/not a directory")
        if (self.root / RECEIPT).exists():
            raise ValueError("preserve committed inputs; choose a fresh experiment")
        self.members: list[dict] = []
        self._names: set[str] = set()
        self._committed = False
        self.copied = self.reused = self.copied_bytes = self.reused_bytes = 0
        self.scanned_roots = 0
        self.scan_truncated = False
        self._donors = defaultdict(list)
        self._lock = None

    def __enter__(self):
        path = self.root / ".immutable-inputs.lock"
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        self._lock = os.fdopen(descriptor, "a+b")
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("unsafe fixture input lock")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._index_siblings()
            return self
        except BaseException:
            self._lock.close()
            self._lock = None
            raise

    def __exit__(self, *_exc):
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def _index_siblings(self):
        with os.scandir(self.root.parent) as entries:
            for entry in entries:
                if not entry.is_dir(follow_symlinks=False) or Path(entry.path) == self.root:
                    continue
                if self.scanned_roots >= MAX_DONOR_ROOTS:
                    self.scan_truncated = True
                    break
                self.scanned_roots += 1
                donor = Path(entry.path)
                try:
                    if donor.resolve(strict=True) != donor:
                        continue
                    legacy = not (donor / RECEIPT).exists()
                    if legacy:
                        # The existing full-public capture already sealed its
                        # sources readonly and verifies every byte in workers.
                        # Reuse those copies too; do not recopy merely to add
                        # this new storage receipt to an old experiment.
                        old = _metadata(donor / "fixture.json")
                        if old.get("contract") != "tw_public_private_full_builder_fixture_v1":
                            continue
                        value = {"files": [dict(item, name=f"source/{item['name']}")
                                           for item in old["source_files"]]}
                    else:
                        value = _metadata(donor / RECEIPT)
                        if value.get("contract") != CONTRACT or value.get("state") != "committed_inputs":
                            continue
                    for item in value["files"]:
                        name, size, digest = _relative(item["name"]), item["size"], item["sha256"]
                        if (type(size) is not int or size < 0 or not isinstance(digest, str)
                                or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)):
                            continue
                        path = donor / name
                        if path.resolve(strict=False) == path:
                            self._donors[(digest, size)].append((donor, path, legacy))
                except (OSError, ValueError, KeyError, TypeError):
                    continue

    @staticmethod
    @contextmanager
    def _donor_guard(donor: Path, legacy: bool):
        if legacy:
            yield  # The old capture never mutates its sealed input names.
            return
        descriptor = os.open(donor / ".immutable-inputs.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("unsafe donor input lock")
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            yield

    def _reuse(self, destination: Path, digest: str, size: int) -> bool:
        for donor, path, legacy in self._donors[(digest, size)]:
            try:
                with self._donor_guard(donor, legacy):
                    if link_immutable_payload(path, destination, sha256=digest, size=size):
                        return True
            except ValueError:
                continue
            except OSError as exc:
                if exc.errno not in {errno.ENOENT, errno.EACCES, errno.EPERM, errno.EAGAIN,
                                     errno.ELOOP, errno.ENOTDIR, errno.ESTALE}:
                    raise  # ENOSPC/EIO during publication must not become a cache hit/miss.
        return False

    def copy(self, source: Path, name: str, *, expected_sha256: str | None = None) -> dict:
        if self._lock is None or self._committed:
            raise RuntimeError("fixture capture is unlocked/already committed")
        relative = _relative(name)
        if name in self._names:
            raise ValueError("duplicate fixture input")
        target = self.root / relative
        if target.resolve(strict=False) != target or os.path.lexists(target):
            raise ValueError("fixture input already exists or is redirected")
        before = payload_info(source)
        digest = stable_payload_hash(source, before)
        if expected_sha256 is not None and digest != expected_sha256:
            raise ValueError("source differs from the selected fixture input")
        target.parent.mkdir(parents=True, exist_ok=True)
        reused = self._reuse(target, digest, before["size"])
        if not reused:
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.partial")
            try:
                with temporary.open("xb") as writer, source.open("rb") as reader:
                    shutil.copyfileobj(reader, writer, 8 * 1024**2)
                    writer.flush()
                    os.fchmod(writer.fileno(), 0o400)
                    os.fsync(writer.fileno())
                copied = payload_info(temporary)
                if copied["size"] != before["size"] or stable_payload_hash(temporary, copied) != digest:
                    raise ValueError("private fixture copy differs from its selected source")
                os.link(temporary, target, follow_symlinks=False)
                _fsync_directory(target.parent)
            finally:
                temporary.unlink(missing_ok=True)
        after = payload_info(source)
        if after != before:
            # An already frozen source may itself own the selected donor inode.
            # Adding its alias changes nlink/ctime only. Mutable sources retain
            # the strict complete signature fence, including ctime.
            linked = payload_info(target)
            if (not reused or before["mode"] & 0o222
                    or (after["device"], after["inode"]) != (linked["device"], linked["inode"])
                    or any(after[key] != before[key] for key in (
                        "device", "inode", "size", "mtime_ns", "mode", "uid", "gid", "xattrs"))
                    or stable_payload_hash(source, after) != digest):
                raise ValueError("source changed during fixture capture")
        item = {"name": name, "size": before["size"], "sha256": digest}
        self.members.append(item)
        self._names.add(name)
        if reused:
            self.reused += 1
            self.reused_bytes += before["size"]
        else:
            self.copied += 1
            self.copied_bytes += before["size"]
        return item

    def commit(self) -> dict:
        if self._lock is None or (self.root / RECEIPT).exists():
            raise ValueError("fixture capture is unlocked/already committed")
        # No partial source set can seed a later capture after a crash.
        for item in self.members:
            path = self.root / item["name"]
            info = payload_info(path)
            if info["mode"] & 0o222 or info["size"] != item["size"] or stable_payload_hash(path, info) != item["sha256"]:
                raise ValueError("fixture input changed before commit")
        result = {"contract": CONTRACT, "state": "committed_inputs", "files": self.members,
                  "copied_files": self.copied, "copied_bytes": self.copied_bytes,
                  "reused_files": self.reused, "reused_bytes": self.reused_bytes,
                  "donor_roots_scanned": self.scanned_roots,
                  "donor_scan_truncated": self.scan_truncated}
        atomic_write_json(self.root / RECEIPT, result, durable=True)
        self._committed = True
        return result
