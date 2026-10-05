"""One-shot compressed preservation, indexed by original content and metadata.

This is an inactive-legacy preservation exception, not a training-completion
release. Normal incremental source publication is unchanged. All payloads enter
the existing packed object store; original artifact paths are never hydrated.
"""
from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import tarfile
import tempfile
import time

from stockagent.data_sync.desync_snapshots import (
    SnapshotError, _safe_relative_path, _fsync_directory, atomic_write_json as _atomic_write_json, sha256_file,
)
from stockagent.data_sync.windows_cold_io import binary_reader, hash_file, metadata_path

CONTRACT = "one_shot_zstd_tar_preservation_v1"
ROLE = "legacy-compressed-preservation"
SYNC_ROOT = Path("/srv/stockagent-packed")
NATIVE_COLD = Path("/mnt/d/stockagent-cold-primary/packed")
INCOMING = Path("/mnt/d/stockagent-cold-primary/remote-artifact-incoming")
POLICY_PATH = Path(__file__).resolve().parents[2] / "configs/data_sync/vastai_bulk_preservation.json"


def atomic_write_json(path, value):
    return _atomic_write_json(metadata_path(path), value)


def load_policy():
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise SnapshotError("duplicate compressed preservation policy field")
            result[key] = value
        return result
    policy = json.loads(POLICY_PATH.read_text(), object_pairs_hook=unique)
    required = {"schema_version", "one_shot", "authority_node_id", "origin_node_id", "scopes",
                "transport_role", "minimum_stable_hours", "cold_root", "protected_cache_roots"}
    if policy.get("schema_version") == 2:
        required.add("shared_file_policy")
    if (set(policy) != required or policy["schema_version"] not in {1, 2} or policy["one_shot"] is not True
        or policy["authority_node_id"] != "penguin" or policy["origin_node_id"] != "vastai1T"
        or policy["scopes"] != ["markets", "ablations", "cache"]
        or policy["transport_role"] != ROLE or policy["minimum_stable_hours"] != 12
        or policy["cold_root"] != str(SYNC_ROOT)
        or policy["schema_version"] == 2 and policy["shared_file_policy"] != "unlink_preserved_names_only"
        or not isinstance(policy["protected_cache_roots"], list)
        or any(not isinstance(name, str) or Path(name).name != name or name in {".", ".."}
               for name in policy["protected_cache_roots"])):
        raise SnapshotError("compressed preservation policy differs from authorized scope")
    return policy


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def signature(path):
    s = path.lstat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_mode, s.st_nlink]


def safe_member(name):
    # Do not let PurePosixPath silently normalize a traversal or absolute path.
    name = name.rstrip("/")
    if not name or name.startswith("/") or any(p in {"", ".", ".."} for p in name.split("/")):
        raise SnapshotError("unsafe preservation archive member")
    return _safe_relative_path(name, "archive member").as_posix()


def mtime_ns(member):
    return int(Decimal(str(member.pax_headers.get("mtime", member.mtime))) * 1000000000)


def index_tar_stream(stream, *, scopes, maximum_original_bytes=2 * 1024**4):
    """Hash every decoded member, without extracting or loading an array.

    Hard links must point backwards to a verified regular member. Symlinks and
    unusual records are preserved, but their roots cannot be auto-retired.
    """
    rows, known, original_bytes = [], {}, 0
    with tarfile.open(fileobj=stream, mode="r|") as archive:
        for member in archive:
            name = safe_member(member.name)
            if name.split("/")[0] not in scopes or name in known:
                raise SnapshotError("archive member scope/uniqueness mismatch")
            row = {"path": name, "mode": member.mode & 0o7777,
                   "mtime_ns": mtime_ns(member), "uid": member.uid, "gid": member.gid}
            if member.isdir():
                row["kind"] = "directory"
            elif member.isfile():
                if member.size < 0 or original_bytes + member.size > maximum_original_bytes:
                    raise SnapshotError("archive original-byte budget exceeded")
                value = hashlib.sha256()
                size = 0
                reader = archive.extractfile(member)
                while chunk := reader.read(8 * 1024 * 1024):
                    value.update(chunk)
                    size += len(chunk)
                if size != member.size:
                    raise SnapshotError("truncated original archive member")
                row.update(kind="file", size=size, sha256=value.hexdigest())
                original_bytes += size
            elif member.islnk():
                target = safe_member(member.linkname)
                if target not in known or known[target]["kind"] != "file":
                    raise SnapshotError("unresolved or unsafe archive hard link")
                other = known[target]
                row.update(kind="file", size=other["size"], sha256=other["sha256"], hardlink_to=target)
                original_bytes += other["size"]
                if original_bytes > maximum_original_bytes:
                    raise SnapshotError("archive hard-link logical-byte budget exceeded")
            elif member.issym():
                row.update(kind="symlink", linkname=member.linkname)
            else:
                row.update(kind="unsupported", type=member.type.hex())
            rows.append(row)
            known[name] = row
        # Include tarfile's buffered tail, then drain the compressor to EOF.
        # A valid tar prefix must not hide a corrupt zstd frame or another tar.
        while chunk := archive.fileobj.read(8 * 1024 * 1024):
            if any(chunk):
                raise SnapshotError("non-padding bytes after preservation tar end")
    rows.sort(key=lambda r: r["path"])
    return {"schema_version": 1, "contract": CONTRACT, "origin_node_id": "vastai1T",
            "authority_node_id": "penguin", "deployable": False,
            "completion_claim": "not_checked", "rows": rows,
            "files": sum(r["kind"] == "file" for r in rows),
            "logical_bytes": original_bytes, "member_fingerprint_sha256": digest(rows)}


def index_zstd(payload, *, expected_sha256, scopes, progress=None):
    payload = metadata_path(payload)
    before = signature(payload)
    if not stat.S_ISREG(before[5]):
        raise SnapshotError("compressed preservation type mismatch")
    # Hash exactly the bytes consumed by zstd, in the same physical read.
    # Independent original decoding stays mandatory; eliminate a redundant
    # whole-archive disk pass rather than weakening the SHA/checksum gate.
    with subprocess.Popen(["zstd", "-d", "-q", "-c"], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as decoder:
        def feed():
            total, value = 0, hashlib.sha256()
            last = time.monotonic()
            with binary_reader(payload) as source:
                try:
                    while chunk := source.read(8 * 1024 * 1024):
                        decoder.stdin.write(chunk)
                        value.update(chunk)
                        total += len(chunk)
                        if progress and time.monotonic() - last >= 15:
                            progress({"compressed_read_bytes": total, "compressed_total_bytes": before[2]})
                            last = time.monotonic()
                finally:
                    decoder.stdin.close()
            return total, value.hexdigest()
        with ThreadPoolExecutor(max_workers=1) as workers:
            incoming = workers.submit(feed)
            try:
                result = index_tar_stream(decoder.stdout, scopes=scopes)
                total, actual = incoming.result(timeout=30)
                if decoder.wait(timeout=30) or total != before[2] or actual != expected_sha256:
                    raise SnapshotError("compressed preservation SHA/frame/checksum failed")
            except BaseException:
                decoder.terminate()
                decoder.wait(timeout=30)
                raise
    if signature(payload) != before:
        raise SnapshotError("compressed archive changed during full original decode")
    result.update(compressed_sha256=expected_sha256, compressed_bytes=before[2],
                  verified_at_epoch=time.time())
    return result


def root_records(index):
    roots = {}
    for row in index["rows"]:
        parts = PurePosixPath(row["path"]).parts
        if len(parts) < 2:
            continue
        relative = "/".join(parts[:2])
        roots.setdefault(relative, []).append(row)
    result = []
    for relative, rows in sorted(roots.items()):
        top = next((r for r in rows if r["path"] == relative), None)
        result.append({"relative_root": relative, "rows": rows,
                       "root_fingerprint_sha256": digest(rows),
                       "directory_root": bool(top and top["kind"] == "directory"),
                       "unsupported": any(r["kind"] not in {"file", "directory"} for r in rows),
                       "logical_bytes": sum(r.get("size", 0) for r in rows if r["kind"] == "file")})
    return result


def canonical_blobs(sync_root, resolved):
    """Resolve the two full blobs; never accept a same-named sidecar index."""
    from stockagent.data_sync.packed_snapshots import _load_inventory, _validate_inventory
    inventory = _load_inventory(sync_root, resolved.manifest)
    _validate_inventory(resolved.manifest, inventory)
    objects = {r["sha256"]: r for r in resolved.manifest["archive"]["objects"]}
    files = {}
    for row in inventory:
        if row["kind"] != "file":
            continue
        store = row["storage"]
        if store["kind"] != "blob":
            raise SnapshotError("compressed preservation requires bounded full blobs")
        files[row["path"]] = sync_root / objects[store["object_sha256"]]["relpath"]
    if set(files) != {"payload.tar.zst", "member_inventory.json"}:
        raise SnapshotError("unexpected compressed preservation cold member set")
    return files


@contextmanager
def decoded_stream(payload):
    """Keep ownership of both pipes; parsing failures cannot strand a feeder."""
    with subprocess.Popen(['zstd', '-d', '-q', '-c'], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as decoder:
        def feed():
            try:
                with binary_reader(payload) as source:
                    while block := source.read(4 * 1024 * 1024):
                        decoder.stdin.write(block)
            finally:
                decoder.stdin.close()
        with ThreadPoolExecutor(max_workers=1) as workers:
            incoming = workers.submit(feed)
            try:
                yield decoder.stdout
                incoming.result(timeout=30)
                if decoder.wait(timeout=30):
                    raise SnapshotError('Compressed frame failed after reconstruction')
            except BaseException:
                if decoder.poll() is None:
                    decoder.terminate()
                    decoder.wait(timeout=30)
                raise


def restore_original_root(payload, index, relative_root, destination):
    """Restore into an exclusively created private sibling, then expose once.

    Never extractall, follow an archived link, overwrite a destination or
    hydrate an artifact implicitly. External hard-link dependencies are bounded
    scratch, copied only when needed and removed after exact reconstruction.
    """
    relative_root = safe_member(relative_root)
    records = {r["relative_root"]: r for r in root_records(index)}
    record = records.get(relative_root)
    if not record or not record["directory_root"] or record["unsupported"]:
        raise SnapshotError("restore requires a supported complete original directory")
    destination = destination.absolute()
    if os.path.lexists(destination) or destination.parent.resolve() != destination.parent:
        raise SnapshotError("restore destination exists or its parent is redirected")
    lookup = {row["path"]: row for row in index["rows"]}
    selected = {r["path"]: r for r in record["rows"]}
    primaries = {}
    for name, row in selected.items():
        if row["kind"] != "file":
            continue
        target = name
        visited = set()
        while lookup[target].get("hardlink_to"):
            if target in visited:
                raise SnapshotError("cyclic preservation hard-link dependencies")
            visited.add(target)
            target = lookup[target]["hardlink_to"]
        primaries[name] = target
    dependencies = set(primaries.values()) - set(selected)
    needed = record["logical_bytes"] + sum(lookup[name]["size"] for name in dependencies)
    fs = os.statvfs(destination.parent)
    if fs.f_bavail * fs.f_frsize < needed + 32 * 1024**3:
        raise SnapshotError("explicit restore lacks full logical budget plus 32 GiB reserve")
    from stockagent.data_sync.training_return import admit_workspace
    admit_workspace(destination.parent, needed + 32 * 1024**3)
    stage = Path(tempfile.mkdtemp(prefix=".stockagent-bulk-restore-", dir=destination.parent))
    deps = Path(tempfile.mkdtemp(prefix=".stockagent-bulk-dependencies-", dir=destination.parent))
    dependency_paths, restored = {}, set()
    before = signature(payload)
    if hash_file(payload) != index["compressed_sha256"]:
        raise SnapshotError("cold compressed archive differs before explicit restore")
    try:
        with decoded_stream(payload) as decoded:
            with tarfile.open(fileobj=decoded, mode="r|") as container:
                for member in container:
                    name = safe_member(member.name)
                    if name not in selected and name not in dependencies:
                        continue
                    expected = lookup[name]
                    if expected["kind"] == "directory":
                        target = stage if name == relative_root else stage / name[len(relative_root) + 1:]
                        target.mkdir(parents=True, exist_ok=True, mode=0o700)
                        restored.add(name)
                        continue
                    if name in dependencies:
                        target = deps / hashlib.sha256(name.encode()).hexdigest()
                        dependency_paths[name] = target
                    else:
                        target = stage / name[len(relative_root) + 1:]
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    if member.islnk():
                        primary = primaries[name]
                        alias = dependency_paths[primary] if primary in dependencies else stage / primary[len(relative_root) + 1:]
                        if not alias.is_file() or alias.is_symlink():
                            raise SnapshotError("cold hard-link dependency was not decoded")
                        os.link(alias, target, follow_symlinks=False)
                    elif member.isfile():
                        size, value = 0, hashlib.sha256()
                        with target.open("xb") as writer:
                            reader = container.extractfile(member)
                            while chunk := reader.read(8 * 1024 * 1024):
                                writer.write(chunk)
                                value.update(chunk)
                                size += len(chunk)
                            writer.flush()
                            os.fsync(writer.fileno())
                        if size != expected["size"] or value.hexdigest() != expected["sha256"]:
                            raise SnapshotError("explicit original reconstruction hash/size differs")
                    else:
                        raise SnapshotError("unsupported original member during reconstruction")
                    os.chown(target, expected["uid"], expected["gid"], follow_symlinks=False)
                    os.chmod(target, expected["mode"], follow_symlinks=False)
                    os.utime(target, ns=(expected["mtime_ns"], expected["mtime_ns"]), follow_symlinks=False)
                    if name in selected:
                        restored.add(name)
                while chunk := container.fileobj.read(8 * 1024 * 1024):
                    if any(chunk):
                        raise SnapshotError("non-padding bytes at end of explicit restore")
        if restored != set(selected) or signature(payload) != before:
            raise SnapshotError("incomplete original reconstruction or changed cold object")
        for name, expected in selected.items():
            if expected["kind"] != "file":
                continue
            target = stage / name[len(relative_root) + 1:]
            if target.is_symlink() or sha256_file(target) != expected["sha256"]:
                raise SnapshotError("independent reconstructed original differs")
        # Only our exclusive, fully hashed dependency names may be removed.
        if {p.name for p in deps.iterdir()} != {p.name for p in dependency_paths.values()}:
            raise SnapshotError("unknown dependency scratch; preserve for audit")
        for name, path in dependency_paths.items():
            if path.is_symlink() or sha256_file(path) != lookup[name]["sha256"]:
                raise SnapshotError("dependency scratch changed; retain")
            path.unlink()
        deps.rmdir()
        for name, expected in sorted(selected.items(), key=lambda p: len(PurePosixPath(p[0]).parts), reverse=True):
            if expected["kind"] == "directory":
                target = stage if name == relative_root else stage / name[len(relative_root) + 1:]
                os.chown(target, expected["uid"], expected["gid"], follow_symlinks=False)
                os.chmod(target, expected["mode"], follow_symlinks=False)
                os.utime(target, ns=(expected["mtime_ns"], expected["mtime_ns"]), follow_symlinks=False)
                _fsync_directory(target)
        if os.path.lexists(destination):
            raise SnapshotError("restore destination appeared; private reconstruction retained")
        # Atomic no-replace: a racing user-created empty directory is not ours.
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        if rename(-100, os.fsencode(stage), -100, os.fsencode(destination), 1):
            number = ctypes.get_errno()
            raise OSError(number, os.strerror(number))
        _fsync_directory(destination.parent)
        return {"state": "original_root_restored_verified", "destination": str(destination),
                "relative_root": relative_root, "files": sum(r["kind"] == "file" for r in selected.values()),
                "logical_bytes": record["logical_bytes"], "deployable": False,
                "compressed_sha256": index["compressed_sha256"], "cold_deleted": False}
    except BaseException as error:
        raise SnapshotError(f"explicit restore failed; private scratch retained at {stage} and {deps}: {error}") from error


def native_guard():
    from stockagent.data_sync.cold_primary import _check_d_primary_mount, D_PRIMARY_MARKER
    load_policy()
    _check_d_primary_mount(SYNC_ROOT)
    if NATIVE_COLD.resolve() != NATIVE_COLD or NATIVE_COLD.is_symlink():
        raise SnapshotError("native D cold alias is redirected")
    mount = json.loads(subprocess.check_output(
        ["findmnt", "-J", "-T", str(NATIVE_COLD), "-o", "TARGET,SOURCE,FSTYPE"], text=True))
    records = mount.get("filesystems", [])
    if len(records) != 1 or records[0].get("source") != "D:\\" or records[0].get("fstype") != "9p":
        raise SnapshotError("native cold alias is not the actual D volume")
    if (NATIVE_COLD / D_PRIMARY_MARKER).read_bytes() != (SYNC_ROOT / D_PRIMARY_MARKER).read_bytes():
        raise SnapshotError("native D alias marker differs from canonical authority")


def install_native_blob(source, digest_value):
    """Use one NTFS inode, under the canonical publisher-retention lock.

    A lost pre-publication object can be recopied from the retained incoming
    archive. No source retirement is permitted before canonical commit/decode.
    """
    from stockagent.data_sync.desync_snapshots import _exclusive_lock
    from stockagent.data_sync.packed_snapshots import _object_relpath, _ensure_shared_packed_directory
    native_guard()
    source = metadata_path(source)
    # Windows-created files may otherwise inherit executable/world-write mode.
    # This changes only the private compressed carrier, never archived originals.
    os.chmod(source, 0o600, follow_symlinks=False)
    before = signature(source)
    if not stat.S_ISREG(before[5]) or hash_file(source) != digest_value:
        raise SnapshotError("immutable incoming blob differs from proof")
    with _exclusive_lock(SYNC_ROOT / ".local-state/locks/publish-retention-global.lock"):
        relative = _object_relpath("blobs", digest_value, ".blob")
        canonical = SYNC_ROOT / relative
        # Both incoming and canonical names use the same stable 9p mount for
        # metadata/link operations; bulk reads still use Windows FileStream.
        native = canonical
        _ensure_shared_packed_directory(SYNC_ROOT, canonical.parent)
        if canonical.exists():
            from stockagent.data_sync.cold_primary import d_primary_read_alias
            if hash_file(d_primary_read_alias(canonical)) != digest_value:
                raise SnapshotError("existing canonical blob differs from SHA")
        else:
            if signature(source) != before or source.stat().st_dev != native.parent.stat().st_dev:
                raise SnapshotError("incoming/native cold stability or volume differs")
            os.link(source, native, follow_symlinks=False)
            with native.open("rb") as reader:
                os.fsync(reader.fileno())
            _fsync_directory(canonical.parent)
            from stockagent.data_sync.cold_primary import d_primary_read_alias
            if not canonical.is_file() or hash_file(d_primary_read_alias(canonical)) != digest_value:
                raise SnapshotError("canonical D view differs after native inode installation")
    return canonical


def publish_preservation(directory, receipt, index, *, repo_root, progress=None, defer_recovery=False):
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
    native_guard()
    directory = metadata_path(directory)
    payload = metadata_path(receipt["payload"])
    if (payload.parent != directory or payload.resolve() != payload
        or directory.resolve() != directory or metadata_path(INCOMING) not in directory.parents):
        raise SnapshotError("preservation publication requires exact private D incoming root")
    scope = receipt["scope"]
    dataset = "legacy-vast-bulk-" + scope + "-" + index["compressed_sha256"][:24]
    stage = directory / ("publish-" + scope)
    stage.mkdir(mode=0o700, exist_ok=True)
    alias = stage / "payload.tar.zst"
    if not alias.exists():
        os.link(payload, alias, follow_symlinks=False)
    elif not alias.samefile(payload):
        raise SnapshotError("incoming payload alias is unexpected")
    manifest = stage / "member_inventory.json"
    # The canonical member inventory is deterministic; observations are stored
    # in local receipts rather than minting a new release for a retry.
    stable = {k: v for k, v in index.items() if k != "verified_at_epoch"}
    if manifest.exists():
        if json.loads(manifest.read_text()) != stable:
            raise SnapshotError("retained preservation index differs; keep evidence")
    else:
        atomic_write_json(manifest, stable)
    for source in (alias, manifest):
        install_native_blob(source, hash_file(source))
    metadata = {"transport_role": ROLE, "preservation_contract": CONTRACT,
                "deployable": "false", "completion_claim": "not_checked",
                "source_scope": scope, "origin_node_id": "vastai1T",
                "compressed_sha256": index["compressed_sha256"],
                "member_inventory_sha256": sha256_file(manifest),
                "preservation_policy_sha256": sha256_file(POLICY_PATH),
                "scope_completeness": "transport-clean" if receipt["producer_exit_code"] == 0 else "must-reconcile"}
    resolved = publish_packed_snapshot(SYNC_ROOT, dataset, stage, node_id="penguin",
                                      loose_file_threshold_bytes=1, pack_buckets=1,
                                      d_primary_native_blob_reads=True,
                                      defer_scan=True,
                                      metadata=metadata, repo_root=repo_root)
    from stockagent.data_sync.syncthing_scan import scan_after_publish
    scan_after_publish(SYNC_ROOT, dataset,
                       new_object_paths=tuple(o["relpath"] for o in resolved.manifest["archive"]["objects"]),
                       retry_full=True, batch_object_paths=True)
    publication = {"dataset": dataset, "snapshot_id": resolved.manifest["snapshot_id"],
                   "manifest_sha256": resolved.manifest_sha256, **metadata,
                   "cold_verified": False, "decoded_originals_verified": False}
    if defer_recovery:
        # A publication receipt cannot authorize deletion. The coordinator owns
        # this cohort while independent immutable-object recovery runs unlocked.
        return publication
    return verify_preservation(publication, index, progress=progress)


def verify_preservation(publication, index, *, progress=None):
    from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, verify_packed_snapshot
    native_guard()
    resolved = resolve_packed_snapshot_id(SYNC_ROOT, publication["dataset"], publication["snapshot_id"])
    metadata = resolved.manifest.get("metadata", {})
    if (resolved.manifest_sha256 != publication["manifest_sha256"]
            or metadata.get("preservation_contract") != CONTRACT or metadata.get("transport_role") != ROLE
            or metadata.get("deployable") != "false"
            or metadata.get("compressed_sha256") != index["compressed_sha256"]
            or any(publication.get(k) != v for k, v in metadata.items())):
        raise SnapshotError("independent recovery is not bound to the exact cold publication")
    files = canonical_blobs(SYNC_ROOT, resolved)
    from stockagent.data_sync.cold_primary import d_primary_read_alias
    aliases = {name: d_primary_read_alias(path) for name, path in files.items()}
    # Full canonical-format verification through the exact same physical D
    # objects. Manifest/locks/heads remain canonical; no shortcut skips hashes.
    verify_packed_snapshot(SYNC_ROOT, resolved, d_primary_native_blob_reads=True)
    if hash_file(aliases["member_inventory.json"]) != metadata["member_inventory_sha256"]:
        raise SnapshotError("canonical member inventory differs")
    cold_index = index_zstd(aliases["payload.tar.zst"], expected_sha256=index["compressed_sha256"],
                           scopes={r["path"].split("/")[0] for r in index["rows"]}, progress=progress)
    if cold_index["rows"] != index["rows"]:
        raise SnapshotError("independent canonical D original decode differs")
    return {"dataset": publication["dataset"], "snapshot_id": resolved.manifest["snapshot_id"],
            "manifest_sha256": resolved.manifest_sha256, **metadata,
            "cold_verified": True, "decoded_originals_verified": True,
            "verified_at_epoch": time.time(),
            "cold_blob_signature": signature(files["payload.tar.zst"]),
            "cold_index_signature": signature(files["member_inventory.json"])}


@contextmanager
def ingress_owner(*, wait=False):
    """Share the existing transaction owner; never signal an in-flight writer."""
    with Path("/run/lock/stockagent-remote-cold-artifact-ingress.lock").open("a+b") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
