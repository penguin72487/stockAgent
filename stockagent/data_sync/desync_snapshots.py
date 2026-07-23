from __future__ import annotations

import contextlib
import dataclasses
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Mapping, Sequence


SNAPSHOT_SCHEMA_VERSION = 1
HEAD_SCHEMA_VERSION = 1
DEFAULT_CHUNK_SIZE_KIB = "256:1024:4096"
DEFAULT_MAX_CLOCK_SKEW_SECONDS = 300
_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_STIGNORE = """// Local transactional state must never leave this node.
(?d).local-state
(?d).local-state/**
// Materialized trees are local caches, not replication inputs.
(?d)materialized
(?d)materialized/**
"""


class SnapshotError(RuntimeError):
    """A dataset snapshot could not be safely published or consumed."""


@dataclasses.dataclass(frozen=True, order=True)
class HLC:
    """A deterministic Hybrid Logical Clock stamp.

    The physical component gives last-write-wins behavior when clocks are
    healthy. The logical component preserves causal ordering when a publisher
    has already observed an equal or later remote stamp. ``node_id`` provides a
    stable final tie-break for concurrent writes.
    """

    physical_ns: int
    logical: int
    node_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "physical_ns": self.physical_ns,
            "logical": self.logical,
            "node_id": self.node_id,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "HLC":
        try:
            stamp = cls(
                physical_ns=int(value["physical_ns"]),
                logical=int(value["logical"]),
                node_id=validate_slug(str(value["node_id"]), "HLC node_id"),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotError(f"invalid HLC: {value!r}") from exc
        if stamp.physical_ns <= 0 or stamp.logical < 0:
            raise SnapshotError(f"invalid HLC values: {value!r}")
        return stamp


@dataclasses.dataclass(frozen=True)
class ResolvedSnapshot:
    manifest: dict[str, Any]
    manifest_path: Path
    manifest_sha256: str
    head_path: Path | None
    diagnostics: tuple[str, ...] = ()


def validate_slug(value: str, label: str) -> str:
    text = str(value).strip()
    if not _SLUG_RE.fullmatch(text) or text in {".", ".."}:
        raise SnapshotError(f"{label} must match {_SLUG_RE.pattern!r}; got {value!r}")
    return text


def _utc_iso_from_ns(timestamp_ns: int) -> str:
    seconds, nanos = divmod(int(timestamp_ns), 1_000_000_000)
    base = dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)
    return f"{base.strftime('%Y-%m-%dT%H:%M:%S')}.{nanos:09d}Z"


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def sha256_file(path: Path, *, block_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, content: bytes, *, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / (
        f".syncthing.{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        with temporary.open("xb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write_bytes(path, _canonical_json_bytes(value))


def write_immutable_json(path: Path, value: Any) -> str:
    content = _canonical_json_bytes(value)
    digest = hashlib.sha256(content).hexdigest()
    if path.exists():
        existing = path.read_bytes()
        if existing != content:
            raise SnapshotError(f"immutable metadata collision at {path}")
        return digest
    atomic_write_bytes(path, content)
    return digest


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SnapshotError(f"JSON root must be an object: {path}")
    return value


def _safe_relative_path(value: str, label: str) -> PurePosixPath:
    path = PurePosixPath(str(value))
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise SnapshotError(f"unsafe {label}: {value!r}")
    return path


def _path_under(root: Path, relative: str, label: str) -> Path:
    rel = _safe_relative_path(relative, label)
    return root.joinpath(*rel.parts)


def _contains_git_metadata(path: Path) -> Path | None:
    resolved = path.resolve()
    for candidate in (resolved, *resolved.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _paths_overlap(first: Path, second: Path) -> bool:
    left = first.resolve()
    right = second.resolve()
    return left == right or left in right.parents or right in left.parents


def default_node_id() -> str:
    host = re.sub(r"[^A-Za-z0-9._-]+", "-", socket.gethostname()).strip("-.")
    host = host or "node"
    machine_id = ""
    for candidate in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
        try:
            machine_id = candidate.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if machine_id:
            break
    suffix_source = machine_id or f"{host}-{uuid.getnode()}"
    suffix = hashlib.sha256(suffix_source.encode("utf-8")).hexdigest()[:12]
    return validate_slug(f"{host[:80]}-{suffix}", "node_id")


def initialize_layout(
    sync_root: Path,
    *,
    node_id: str | None = None,
    replace_ignore: bool = False,
    replace_node_id: bool = False,
) -> str:
    sync_root = sync_root.resolve()
    if git_root := _contains_git_metadata(sync_root):
        raise SnapshotError(
            f"sync root {sync_root} is inside Git worktree {git_root}; "
            "use a separate data-only directory"
        )
    resolved_node_id = validate_slug(node_id or default_node_id(), "node_id")
    for relative in (
        "heads",
        "indices",
        "manifests",
        "stores",
        ".local-state/locks",
        ".local-state/staging",
    ):
        (sync_root / relative).mkdir(parents=True, exist_ok=True)
    ignore_path = sync_root / ".stignore"
    if replace_ignore or not ignore_path.exists():
        atomic_write_bytes(ignore_path, _STIGNORE.encode("utf-8"))
    node_path = sync_root / ".local-state" / "node-id"
    if node_path.exists():
        existing = node_path.read_text(encoding="utf-8").strip()
        if existing:
            existing = validate_slug(existing, "stored node_id")
            if node_id is None:
                return existing
            if existing != resolved_node_id and not replace_node_id:
                raise SnapshotError(
                    f"node_id is already {existing}; refusing to change it to "
                    f"{resolved_node_id} without --replace-node-id"
                )
    atomic_write_bytes(node_path, f"{resolved_node_id}\n".encode("utf-8"), mode=0o600)
    return resolved_node_id


def resolve_node_id(sync_root: Path, explicit: str | None = None) -> str:
    requested = validate_slug(explicit, "node_id") if explicit else None
    if from_env := os.environ.get("STOCKAGENT_SYNC_NODE_ID"):
        env_node = validate_slug(from_env, "STOCKAGENT_SYNC_NODE_ID")
        if requested is not None and requested != env_node:
            raise SnapshotError(
                f"--node-id {requested} disagrees with STOCKAGENT_SYNC_NODE_ID {env_node}"
            )
        requested = env_node
    node_path = sync_root / ".local-state" / "node-id"
    try:
        stored = node_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SnapshotError(
            f"node identity is not initialized under {sync_root}; run init first"
        ) from exc
    stored = validate_slug(stored, "stored node_id")
    if requested is not None and requested != stored:
        raise SnapshotError(
            f"requested node_id {requested} disagrees with initialized node_id {stored}"
        )
    return stored


@contextlib.contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def scan_tree(root: Path) -> dict[str, Any]:
    """Return cheap portable and mutation-detection fingerprints.

    The portable fingerprint intentionally excludes timestamps and ownership so
    a ``desync tar --no-time`` extraction can be compared across machines. The
    stability fingerprint includes inode metadata and is used only to detect a
    live source changing while its archive is being produced. Chunk and index
    hashes remain the content-integrity authority.
    """

    root = root.resolve()
    if not root.is_dir():
        raise SnapshotError(f"snapshot source is not a directory: {root}")
    portable = hashlib.sha256()
    stability = hashlib.sha256()
    files = directories = symlinks = logical_bytes = 0

    def update_entry(
        relative: str, kind: str, info: os.stat_result, extra: str
    ) -> None:
        nonlocal files, directories, symlinks, logical_bytes
        path_bytes = relative.encode("utf-8", errors="surrogateescape")
        extra_bytes = extra.encode("utf-8", errors="surrogateescape")
        portable_size = info.st_size if kind == "F" else 0
        portable.update(kind.encode("ascii") + b"\0" + path_bytes + b"\0")
        portable.update(
            str(portable_size).encode("ascii") + b"\0" + extra_bytes + b"\n"
        )
        stability.update(kind.encode("ascii") + b"\0" + path_bytes + b"\0")
        stability.update(
            (
                f"{info.st_size}:{info.st_mode}:{info.st_mtime_ns}:"
                f"{info.st_ctime_ns}:{info.st_dev}:{info.st_ino}:"
            ).encode("ascii")
            + extra_bytes
            + b"\n"
        )
        if kind == "F":
            files += 1
            logical_bytes += info.st_size
        elif kind == "D":
            directories += 1
        elif kind == "L":
            symlinks += 1

    for current, dir_names, file_names in os.walk(
        root, topdown=True, followlinks=False
    ):
        dir_names.sort()
        file_names.sort()
        current_path = Path(current)
        for name in list(dir_names):
            path = current_path / name
            info = path.lstat()
            relative = path.relative_to(root).as_posix()
            if stat.S_ISLNK(info.st_mode):
                dir_names.remove(name)
                update_entry(relative, "L", info, os.readlink(path))
            elif stat.S_ISDIR(info.st_mode):
                update_entry(relative, "D", info, "")
            else:
                raise SnapshotError(f"unsupported directory entry type: {path}")
        for name in file_names:
            path = current_path / name
            info = path.lstat()
            relative = path.relative_to(root).as_posix()
            if stat.S_ISREG(info.st_mode):
                update_entry(relative, "F", info, "")
            elif stat.S_ISLNK(info.st_mode):
                update_entry(relative, "L", info, os.readlink(path))
            else:
                raise SnapshotError(f"special files are not snapshot-safe: {path}")

    return {
        "files": files,
        "directories": directories,
        "symlinks": symlinks,
        "logical_bytes": logical_bytes,
        "portable_fingerprint_sha256": portable.hexdigest(),
        "stability_fingerprint_sha256": stability.hexdigest(),
    }


def _resolve_desync_binary(explicit: str | None = None) -> Path:
    candidates: list[str] = []
    if explicit:
        candidates.append(explicit)
    if from_env := os.environ.get("DESYNC_BIN"):
        candidates.append(from_env)
    if found := shutil.which("desync"):
        candidates.append(found)
    candidates.append(str(Path.home() / ".local" / "bin" / "desync"))
    for candidate in candidates:
        path = Path(candidate).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return path.resolve()
    raise SnapshotError(
        "desync executable not found; run scripts/install_desync.sh or set DESYNC_BIN"
    )


def _run_desync(
    binary: Path,
    arguments: Sequence[str],
    *,
    capture_stdout: bool = False,
) -> str:
    command = [str(binary), *map(str, arguments)]
    try:
        completed = subprocess.run(
            command,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture_stdout else None,
            stderr=None,
        )
    except subprocess.CalledProcessError as exc:
        raise SnapshotError(
            f"desync command failed with exit code {exc.returncode}: {' '.join(command)}"
        ) from exc
    return completed.stdout or ""


def _desync_index_info(binary: Path, store: Path, index: Path) -> dict[str, Any]:
    output = _run_desync(
        binary,
        ["info", "--format=json", "--store", str(store), str(index)],
        capture_stdout=True,
    )
    try:
        info = json.loads(output)
    except json.JSONDecodeError as exc:
        raise SnapshotError(f"desync info returned invalid JSON for {index}") from exc
    if not isinstance(info, dict):
        raise SnapshotError(f"desync info returned an invalid object for {index}")
    unique = int(info.get("unique", -1))
    in_store = int(info.get("in-store", -1))
    if unique < 0 or in_store < 0:
        raise SnapshotError(f"desync info omitted chunk counts for {index}")
    if in_store != unique:
        raise SnapshotError(
            f"incomplete desync store for {index.name}: {in_store}/{unique} unique chunks"
        )
    return info


def _git_state(repo_root: Path | None) -> dict[str, Any] | None:
    if repo_root is None:
        return None
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                cwd=repo_root,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return {"commit": commit, "tracked_worktree_dirty": dirty}


def _validate_clock(stamp: HLC, *, now_ns: int, max_clock_skew_seconds: int) -> None:
    future_ns = stamp.physical_ns - now_ns
    limit_ns = int(max_clock_skew_seconds) * 1_000_000_000
    if future_ns > limit_ns:
        raise SnapshotError(
            f"head from {stamp.node_id} is {future_ns / 1e9:.3f}s in the future; "
            "repair NTP/clock synchronization before resolving latest"
        )


def next_hlc(
    observed: Sequence[HLC],
    *,
    node_id: str,
    now_ns: int | None = None,
) -> HLC:
    node_id = validate_slug(node_id, "node_id")
    physical_now = time.time_ns() if now_ns is None else int(now_ns)
    max_physical = max((stamp.physical_ns for stamp in observed), default=0)
    physical = max(physical_now, max_physical)
    if physical == max_physical:
        logical = (
            max(
                (stamp.logical for stamp in observed if stamp.physical_ns == physical),
                default=-1,
            )
            + 1
        )
    else:
        logical = 0
    return HLC(physical_ns=physical, logical=logical, node_id=node_id)


def _validate_manifest(manifest: Mapping[str, Any]) -> HLC:
    if int(manifest.get("schema_version", -1)) != SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotError("unsupported snapshot manifest schema")
    dataset = validate_slug(str(manifest.get("dataset", "")), "manifest dataset")
    snapshot_id = validate_slug(
        str(manifest.get("snapshot_id", "")), "manifest snapshot_id"
    )
    publisher = manifest.get("publisher")
    archive = manifest.get("archive")
    if not isinstance(publisher, Mapping) or not isinstance(archive, Mapping):
        raise SnapshotError(
            f"manifest {snapshot_id} is missing publisher/archive objects"
        )
    publisher_node = validate_slug(
        str(publisher.get("node_id", "")), "publisher node_id"
    )
    stamp = HLC.from_mapping(manifest.get("hlc", {}))
    if stamp.node_id != publisher_node:
        raise SnapshotError(f"manifest {snapshot_id} has inconsistent publisher HLC")
    _safe_relative_path(str(archive.get("index_relpath", "")), "index_relpath")
    _safe_relative_path(str(archive.get("store_relpath", "")), "store_relpath")
    index_sha = str(archive.get("index_sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", index_sha):
        raise SnapshotError(f"manifest {snapshot_id} has invalid index SHA-256")
    if not dataset or not snapshot_id:
        raise SnapshotError("manifest dataset/snapshot_id is empty")
    return stamp


def _head_candidates(
    sync_root: Path,
    dataset: str,
    *,
    now_ns: int,
    max_clock_skew_seconds: int,
    verify_index: bool,
) -> tuple[list[ResolvedSnapshot], list[str], list[tuple[HLC | None, str]]]:
    candidates: list[ResolvedSnapshot] = []
    diagnostics: list[str] = []
    incomplete_canonical_heads: list[tuple[HLC | None, str]] = []
    head_root = sync_root / "heads" / dataset
    for path in sorted(head_root.glob("*.json")):
        head_stamp: HLC | None = None
        canonical_head = False
        try:
            head = _load_json(path)
            if int(head.get("schema_version", -1)) != HEAD_SCHEMA_VERSION:
                raise SnapshotError("unsupported head schema")
            head_dataset = validate_slug(str(head.get("dataset", "")), "head dataset")
            node_id = validate_slug(str(head.get("node_id", "")), "head node_id")
            if head_dataset != dataset:
                raise SnapshotError(
                    f"head dataset is {head_dataset}, expected {dataset}"
                )
            canonical_head = path.name == f"{node_id}.json"
            if not canonical_head:
                raise SnapshotError(
                    "head filename does not match its unique node_id; duplicate node IDs "
                    "or Syncthing conflict copies are not valid publishers"
                )
            head_stamp = HLC.from_mapping(head.get("hlc", {}))
            _validate_clock(
                head_stamp,
                now_ns=now_ns,
                max_clock_skew_seconds=max_clock_skew_seconds,
            )
            manifest_path = _path_under(
                sync_root, str(head.get("manifest_relpath", "")), "manifest_relpath"
            )
            expected_manifest_sha = str(head.get("manifest_sha256", ""))
            if not re.fullmatch(r"[0-9a-f]{64}", expected_manifest_sha):
                raise SnapshotError("head has invalid manifest SHA-256")
            actual_manifest_sha = sha256_file(manifest_path)
            if actual_manifest_sha != expected_manifest_sha:
                raise SnapshotError(
                    f"manifest checksum mismatch: expected {expected_manifest_sha}, "
                    f"got {actual_manifest_sha}"
                )
            manifest = _load_json(manifest_path)
            manifest_stamp = _validate_manifest(manifest)
            if manifest_stamp != head_stamp:
                raise SnapshotError("head and manifest HLC differ")
            if str(manifest["snapshot_id"]) != str(head.get("snapshot_id", "")):
                raise SnapshotError("head and manifest snapshot IDs differ")
            if verify_index:
                archive = manifest["archive"]
                index_path = _path_under(
                    sync_root, str(archive["index_relpath"]), "index_relpath"
                )
                if sha256_file(index_path) != str(archive["index_sha256"]):
                    raise SnapshotError("index checksum mismatch")
            candidates.append(
                ResolvedSnapshot(
                    manifest=dict(manifest),
                    manifest_path=manifest_path,
                    manifest_sha256=actual_manifest_sha,
                    head_path=path,
                )
            )
        except (OSError, SnapshotError) as exc:
            message = f"{path.name}: {exc}"
            diagnostics.append(message)
            if canonical_head or (
                head_stamp is None and ".sync-conflict-" not in path.name
            ):
                incomplete_canonical_heads.append((head_stamp, message))
    return candidates, diagnostics, incomplete_canonical_heads


def resolve_latest(
    sync_root: Path,
    dataset: str,
    *,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    now_ns: int | None = None,
    verify_index: bool = True,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    dataset = validate_slug(dataset, "dataset")
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    candidates, diagnostics, incomplete_heads = _head_candidates(
        sync_root,
        dataset,
        now_ns=current_ns,
        max_clock_skew_seconds=max_clock_skew_seconds,
        verify_index=verify_index,
    )
    if not candidates:
        detail = "; ".join(diagnostics) if diagnostics else "no per-node heads found"
        raise SnapshotError(f"no valid snapshot for dataset {dataset}: {detail}")
    winner = max(
        candidates,
        key=lambda candidate: (
            HLC.from_mapping(candidate.manifest["hlc"]),
            str(candidate.manifest["snapshot_id"]),
        ),
    )
    winner_stamp = HLC.from_mapping(winner.manifest["hlc"])
    blocking = [
        message
        for stamp, message in incomplete_heads
        if stamp is None or stamp >= winner_stamp
    ]
    if blocking:
        raise SnapshotError(
            "newest canonical head is not locally complete; refusing to fall back "
            f"to {winner.manifest['snapshot_id']}: {'; '.join(blocking)}"
        )
    return dataclasses.replace(winner, diagnostics=tuple(diagnostics))


def resolve_snapshot_id(
    sync_root: Path, dataset: str, snapshot_id: str
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    dataset = validate_slug(dataset, "dataset")
    snapshot_id = validate_slug(snapshot_id, "snapshot_id")
    path = sync_root / "manifests" / dataset / f"{snapshot_id}.json"
    manifest_sha = sha256_file(path)
    manifest = _load_json(path)
    _validate_manifest(manifest)
    if manifest.get("dataset") != dataset or manifest.get("snapshot_id") != snapshot_id:
        raise SnapshotError(f"manifest identity mismatch: {path}")
    archive = manifest["archive"]
    index_path = _path_under(sync_root, archive["index_relpath"], "index_relpath")
    if sha256_file(index_path) != archive["index_sha256"]:
        raise SnapshotError(f"index checksum mismatch for {snapshot_id}")
    return ResolvedSnapshot(
        manifest=manifest,
        manifest_path=path,
        manifest_sha256=manifest_sha,
        head_path=None,
    )


def _observed_stamps(
    sync_root: Path,
    dataset: str,
    *,
    now_ns: int,
    max_clock_skew_seconds: int,
) -> list[HLC]:
    candidates, diagnostics, incomplete_heads = _head_candidates(
        sync_root,
        dataset,
        now_ns=now_ns,
        max_clock_skew_seconds=max_clock_skew_seconds,
        verify_index=False,
    )
    blocking_errors = [item for item in diagnostics if "in the future" in item]
    blocking_errors.extend(message for _, message in incomplete_heads)
    if blocking_errors:
        raise SnapshotError("; ".join(blocking_errors))
    return [HLC.from_mapping(candidate.manifest["hlc"]) for candidate in candidates]


def publish_snapshot(
    sync_root: Path,
    dataset: str,
    source: Path,
    *,
    node_id: str | None = None,
    desync_binary: str | None = None,
    chunk_size_kib: str = DEFAULT_CHUNK_SIZE_KIB,
    preserve_times: bool = False,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    metadata: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    source = source.resolve()
    if _paths_overlap(sync_root, source):
        raise SnapshotError(
            "snapshot source and sync root overlap; this can recursively archive the "
            "chunk store or expose live data to Syncthing"
        )
    dataset = validate_slug(dataset, "dataset")
    node_identity_path = sync_root / ".local-state" / "node-id"
    initialize_layout(
        sync_root,
        node_id=node_id if not node_identity_path.exists() else None,
    )
    publisher_node = resolve_node_id(sync_root, node_id)
    binary = _resolve_desync_binary(desync_binary)
    lock_path = (
        sync_root
        / ".local-state"
        / "locks"
        / f"publish-{dataset}-{publisher_node}.lock"
    )

    with _exclusive_lock(lock_path):
        before = scan_tree(source)
        staging_dir = sync_root / ".local-state" / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        temporary_index = staging_dir / f"{dataset}-{uuid.uuid4().hex}.caidx"
        store_relpath = PurePosixPath("stores") / f"{dataset}.castr"
        store_path = sync_root.joinpath(*store_relpath.parts)
        store_path.mkdir(parents=True, exist_ok=True)
        command = [
            "tar",
            "--index",
            "--one-file-system",
            "--store",
            str(store_path),
            "--chunk-size",
            chunk_size_kib,
        ]
        if not preserve_times:
            command.append("--no-time")
        command.extend([str(temporary_index), str(source)])
        _run_desync(binary, command)
        after = scan_tree(source)
        if (
            before["stability_fingerprint_sha256"]
            != after["stability_fingerprint_sha256"]
        ):
            raise SnapshotError(
                "source tree changed while desync archived it; publish from a frozen "
                "filesystem snapshot or under the downloader's dataset lock"
            )

        info = _desync_index_info(binary, store_path, temporary_index)
        index_sha = sha256_file(temporary_index)
        wall_time_ns = time.time_ns()
        observed = _observed_stamps(
            sync_root,
            dataset,
            now_ns=wall_time_ns,
            max_clock_skew_seconds=max_clock_skew_seconds,
        )
        stamp = next_hlc(observed, node_id=publisher_node, now_ns=wall_time_ns)
        seconds, subsecond_ns = divmod(stamp.physical_ns, 1_000_000_000)
        timestamp_slug = (
            dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).strftime(
                "%Y%m%dT%H%M%S"
            )
            + f"{subsecond_ns:09d}Z"
        )
        snapshot_id = validate_slug(
            f"{dataset[:40]}-{timestamp_slug}-l{stamp.logical}-"
            f"{publisher_node[:40]}-{index_sha[:16]}",
            "snapshot_id",
        )
        index_relpath = PurePosixPath("indices") / dataset / f"{snapshot_id}.caidx"
        index_path = sync_root.joinpath(*index_relpath.parts)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        if index_path.exists():
            if sha256_file(index_path) != index_sha:
                raise SnapshotError(f"immutable index collision at {index_path}")
            temporary_index.unlink()
        else:
            os.replace(temporary_index, index_path)
            _fsync_directory(index_path.parent)

        manifest: dict[str, Any] = {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "dataset": dataset,
            "snapshot_id": snapshot_id,
            "published_at": _utc_iso_from_ns(wall_time_ns),
            "hlc": stamp.to_dict(),
            "publisher": {"node_id": publisher_node},
            "source": {
                "snapshot_root_name": source.name,
                **{
                    key: value
                    for key, value in before.items()
                    if key != "stability_fingerprint_sha256"
                },
            },
            "archive": {
                "format": "desync-caidx-v1",
                "index_relpath": index_relpath.as_posix(),
                "index_sha256": index_sha,
                "store_relpath": store_relpath.as_posix(),
                "chunk_size_kib": chunk_size_kib,
                "total_chunks": int(info["total"]),
                "unique_chunks": int(info["unique"]),
                "archive_stream_bytes": int(info["size"]),
                "desync_binary_sha256": sha256_file(binary),
            },
            "metadata": dict(sorted((metadata or {}).items())),
        }
        if git_state := _git_state(repo_root):
            manifest["git"] = git_state
        manifest_relpath = PurePosixPath("manifests") / dataset / f"{snapshot_id}.json"
        manifest_path = sync_root.joinpath(*manifest_relpath.parts)
        manifest_sha = write_immutable_json(manifest_path, manifest)
        head = {
            "schema_version": HEAD_SCHEMA_VERSION,
            "dataset": dataset,
            "node_id": publisher_node,
            "snapshot_id": snapshot_id,
            "hlc": stamp.to_dict(),
            "updated_at": _utc_iso_from_ns(wall_time_ns),
            "manifest_relpath": manifest_relpath.as_posix(),
            "manifest_sha256": manifest_sha,
        }
        head_path = sync_root / "heads" / dataset / f"{publisher_node}.json"
        atomic_write_json(head_path, head)
        return ResolvedSnapshot(
            manifest=manifest,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha,
            head_path=head_path,
        )


def verify_snapshot(
    sync_root: Path,
    resolved: ResolvedSnapshot,
    *,
    desync_binary: str | None = None,
    materialized_path: Path | None = None,
) -> dict[str, Any]:
    sync_root = sync_root.resolve()
    binary = _resolve_desync_binary(desync_binary)
    manifest = resolved.manifest
    _validate_manifest(manifest)
    if sha256_file(resolved.manifest_path) != resolved.manifest_sha256:
        raise SnapshotError(
            f"manifest changed after resolution: {resolved.manifest_path}"
        )
    archive = manifest["archive"]
    index_path = _path_under(sync_root, archive["index_relpath"], "index_relpath")
    store_path = _path_under(sync_root, archive["store_relpath"], "store_relpath")
    actual_index_sha = sha256_file(index_path)
    if actual_index_sha != archive["index_sha256"]:
        raise SnapshotError(
            f"index checksum mismatch: expected {archive['index_sha256']}, "
            f"got {actual_index_sha}"
        )
    info = _desync_index_info(binary, store_path, index_path)
    result: dict[str, Any] = {
        "snapshot_id": manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256,
        "index_sha256": actual_index_sha,
        "chunks": {"unique": int(info["unique"]), "in_store": int(info["in-store"])},
        "materialized_verified": False,
    }
    if materialized_path is not None:
        inventory = scan_tree(materialized_path)
        expected = manifest["source"]["portable_fingerprint_sha256"]
        actual = inventory["portable_fingerprint_sha256"]
        if actual != expected:
            raise SnapshotError(
                f"materialized tree fingerprint mismatch: expected {expected}, got {actual}"
            )
        result["materialized_verified"] = True
        result["materialized"] = inventory
    return result


def fetch_snapshot(
    sync_root: Path,
    materialized_root: Path,
    resolved: ResolvedSnapshot,
    *,
    desync_binary: str | None = None,
) -> Path:
    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if _paths_overlap(sync_root, materialized_root):
        raise SnapshotError(
            "materialized root must be outside the Syncthing/desync sync root"
        )
    binary = _resolve_desync_binary(desync_binary)
    manifest = resolved.manifest
    dataset = validate_slug(str(manifest["dataset"]), "dataset")
    snapshot_id = validate_slug(str(manifest["snapshot_id"]), "snapshot_id")
    dataset_root = materialized_root / dataset
    target = dataset_root / snapshot_id
    ready_path = dataset_root / f".{snapshot_id}.READY.json"
    lock_path = materialized_root / ".locks" / f"fetch-{dataset}-{snapshot_id}.lock"

    with _exclusive_lock(lock_path):
        verification = verify_snapshot(
            sync_root,
            resolved,
            desync_binary=str(binary),
        )
        if target.exists():
            verify_snapshot(
                sync_root,
                resolved,
                desync_binary=str(binary),
                materialized_path=target,
            )
            if not ready_path.exists():
                atomic_write_json(
                    ready_path,
                    {
                        "snapshot_id": snapshot_id,
                        "manifest_sha256": resolved.manifest_sha256,
                        "verified_at": _utc_iso_from_ns(time.time_ns()),
                    },
                )
            return target

        dataset_root.mkdir(parents=True, exist_ok=True)
        staging = dataset_root / f".{snapshot_id}.partial.{uuid.uuid4().hex}"
        archive = manifest["archive"]
        index_path = _path_under(sync_root, archive["index_relpath"], "index_relpath")
        store_path = _path_under(sync_root, archive["store_relpath"], "store_relpath")
        _run_desync(
            binary,
            [
                "untar",
                "--index",
                "--no-same-owner",
                "--store",
                str(store_path),
                str(index_path),
                str(staging),
            ],
        )
        inventory = scan_tree(staging)
        expected = manifest["source"]["portable_fingerprint_sha256"]
        if inventory["portable_fingerprint_sha256"] != expected:
            raise SnapshotError(
                f"extracted snapshot fingerprint mismatch; partial tree retained at {staging}"
            )
        os.replace(staging, target)
        _fsync_directory(dataset_root)
        atomic_write_json(
            ready_path,
            {
                "snapshot_id": snapshot_id,
                "manifest_sha256": resolved.manifest_sha256,
                "index_sha256": verification["index_sha256"],
                "verified_at": _utc_iso_from_ns(time.time_ns()),
            },
        )
        return target


def write_pin(path: Path, resolved: ResolvedSnapshot) -> None:
    atomic_write_json(
        path,
        {
            "schema_version": 1,
            "resolved_at": _utc_iso_from_ns(time.time_ns()),
            "manifest_sha256": resolved.manifest_sha256,
            "manifest": resolved.manifest,
        },
    )
