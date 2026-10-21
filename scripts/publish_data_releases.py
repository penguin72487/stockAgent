#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from datetime import UTC, date, datetime, timedelta
import fcntl
import json
import math
import os
import re
import shlex
import stat
import sys
from pathlib import Path, PurePosixPath
import time
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    atomic_write_json,
    sha256_file,
)
from scripts.bybit_refresh_inputs import materialization_input_signature  # noqa: E402
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    _check_packed_read_authority,
    _normalize_included_root_files,
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
)

DEFAULT_CATALOG = REPO_ROOT / "configs" / "data_sync" / "packed_datasets.json"
DEFAULT_RETENTION_CONFIG = REPO_ROOT / "configs" / "data_sync" / "packed_retention.json"
SOURCE_LOCK_WAIT_SECONDS = 180.0


class SourceCoordinationLockTimeout(SnapshotError):
    """The bounded wait expired while another source owner held the lock."""


def _native_source_lock_entries(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Use the original producers' locks, without changing catalog/source scope.

    Process discovery cannot serialize a collector that starts after discovery.
    The CEX collector and daily materializer already share .download.lock;
    taking that same inode closes this race for both full and tail updates.
    Only known canonical locations participate, never discovered *.lock files.
    """
    source = _source_path(entry).absolute()
    dataset = str(entry.get("dataset", ""))
    locks: list[tuple[str, Path]] = []
    if dataset in {"okx", "bybit", "binance"} and source == REPO_ROOT / ("data_" + dataset):
        # The enrolled Bybit workspace is a whole-root compatibility alias.
        # Bind its canonical target once; redirected grains below that target
        # are still rejected, and no unrelated catalog path is admitted.
        original_root = source.resolve(strict=True)
        for grain in ("1m", "15m", "daily"):
            directory = original_root / grain
            if directory.exists():
                if directory.resolve() != directory or not directory.is_dir():
                    raise SnapshotError("canonical CEX writer directory is redirected")
                locks.append((grain + "/.download.lock", original_root))
    research_roots = {
        "tw-public-research-wide-2014": "data_tw_public_research_wide_2014_v1",
        "tw-public-research-all-observed-2014-v3": "data_tw_public_research_all_observed_2014_v3",
        "tw-public-research-finlab-2014-v4": "data_tw_public_research_finlab_2014_v4",
    }
    if (dataset in research_roots
            and source == Path("/srv/stockagent-live") / research_roots[dataset]):
        if dataset == "tw-public-research-finlab-2014-v4":
            from stockagent.data.finlab_acquisition_contract import FINLAB_RESEARCH_STAGE_LOCK
            locks.append((FINLAB_RESEARCH_STAGE_LOCK, source.parent))
        else:
            locks.append((".locks/tw-public-refresh.lock", source.parent))
    return [{**entry, "source": str(root / "source-owner-scope"),
             "source_coordination_lock_scope": "source_parent",
             "source_coordination_lock": relative}
            for relative, root in locks]


@contextmanager
def _publication_source_locks(entry: Mapping[str, Any], *, inherited_fd: int | None = None,
                              wait_seconds: float | None = None):
    """One bounded admission budget; release every source lock before cold recovery."""
    native = _native_source_lock_entries(entry)
    duration = SOURCE_LOCK_WAIT_SECONDS if wait_seconds is None else wait_seconds
    deadline = time.monotonic() + duration
    with ExitStack() as owners:
        primary = owners.enter_context(_source_coordination_lock(
            entry, inherited_fd=inherited_fd, wait_seconds=wait_seconds))
        native_fds = []
        for original_owner in native:
            native_fds.append(owners.enter_context(_source_coordination_lock(
                original_owner, wait_seconds=max(0.0, deadline - time.monotonic()))))
        yield {"primary_fd": primary, "native_lock_fds": tuple(native_fds)}


def _coordinated_native_waiters(entry, blockers, held_lock_fds):
    """Recognize only native collectors waiting behind our original lock.

    This is not a process-name override. Publication must already own every
    native lock. The same canonical collector must still have an open FD for
    one of those exact inodes. Its audited writer body is entirely within
    exclusive_dataset_lock, so it cannot concurrently write this workspace.
    Unknown writers, shell programs and unrelated/custom output locks remain
    blockers. Never use this proof after the publication context exits.
    """
    if not held_lock_fds or not blockers:
        return []
    scripts = {
        "binance": {"download_binance_perp_1m.py", "download_binance_perp_15m.py"},
        "okx": {"download_okx_perp_1m.py", "download_okx_perp_daily.py"},
        "bybit": {"download_bybit_perp_1m.py", "download_bybit_perp_daily.py"},
    }.get(entry["dataset"], set())
    native = _native_source_lock_entries(entry) if scripts else []
    if not native or len(native) != len(held_lock_fds):
        return []
    inodes = set()
    for owner, fd in zip(native, held_lock_fds, strict=True):
        if type(fd) is not int:
            return []
        path = _source_path(owner).parent / owner["source_coordination_lock"]
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(opened, path.lstat()):
            raise SnapshotError("native writer owner changed before source admission")
        inodes.add((opened.st_dev, opened.st_ino))
    waiters = []
    for blocker in blockers:
        if blocker["pattern"] not in scripts:
            continue
        proc = Path("/proc") / str(blocker["pid"])
        try:
            raw = (proc / "cmdline").read_bytes()
            argv = [part.decode("utf-8", errors="strict") for part in raw.split(b"\0") if part]
            if (not argv or shlex.join(argv) != blocker["command"]
                    or re.fullmatch(r"python(?:\d+(?:\.\d+)*)?", Path(argv[0]).name) is None):
                continue
            options = list(argv[1:])
            while options and options[0] in {"-u", "-B", "-s", "-E", "-I"}:
                options.pop(0)
            name = blocker["pattern"]
            if options[:2] == ["-m", "downloader." + name[:-3]]:
                canonical = True
            elif options and not options[0].startswith("-"):
                script = Path(options[0])
                if not script.is_absolute():
                    script = (proc / "cwd").resolve(strict=True) / script
                canonical = script.resolve(strict=True) == REPO_ROOT / "downloader" / name
            else:
                canonical = False
            if not canonical:
                continue
            observed = []
            for fd_path in (proc / "fd").iterdir():
                try:
                    info = fd_path.stat()
                    key = (info.st_dev, info.st_ino)
                    if stat.S_ISREG(info.st_mode) and key in inodes:
                        observed.append({"device": key[0], "inode": key[1]})
                except FileNotFoundError:
                    continue
            if observed and (proc / "cmdline").read_bytes() == raw:
                waiters.append({**blocker, "held_native_lock_inodes": observed,
                                "state": "excluded_by_original_native_writer_lock"})
        except (OSError, UnicodeError):
            continue  # Incomplete process evidence retains the original blocker.
    return waiters


def _expected_bybit_signature(value: object) -> dict[str, object]:
    if (
        not isinstance(value, dict)
        or set(value) != {"files", "metadata_sha256"}
        or type(value.get("files")) is not int
        or value["files"] < 0
        or not isinstance(value.get("metadata_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", value["metadata_sha256"]) is None
    ):
        raise SnapshotError("invalid expected Bybit input signature")
    return dict(value)


def _parse_expected_bybit_signature(text: str) -> dict[str, object]:
    try:
        return _expected_bybit_signature(json.loads(text))
    except (ValueError, SnapshotError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


@contextmanager
def _source_coordination_lock(
    entry: Mapping[str, Any],
    *,
    inherited_fd: int | None = None,
    wait_seconds: float | None = None,
):
    """Serialize a catalogued publisher with cooperative source writers.

    The packed publisher's stat/hash/fingerprint checks remain authoritative;
    this lock only closes the scheduled writer's check-then-start race. A
    subprocess may borrow the owner's open file description through pass_fds;
    it must neither unlock nor close that descriptor on context exit.
    """
    configured = entry.get("source_coordination_lock")
    if not configured:
        if inherited_fd is not None:
            raise SnapshotError("inherited source lock requires a catalogued lock")
        yield None
        return
    relative = Path(str(configured))
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotError(f"invalid source coordination lock: {configured!r}")
    scope = entry.get("source_coordination_lock_scope", "repo")
    if scope not in {"repo", "source_parent"}:
        raise SnapshotError(f"invalid source coordination lock scope: {scope!r}")
    lock_root = REPO_ROOT if scope == "repo" else _source_path(entry).resolve().parent
    requested = lock_root.absolute() / relative
    path = requested.resolve()
    if not path.is_relative_to(lock_root.resolve()):
        raise SnapshotError(f"source coordination lock leaves {scope}: {configured!r}")
    if path != requested:
        raise SnapshotError("source coordination lock is redirected")
    existing_forward_owner = entry.get("completed_forward_key") is not None

    def check_existing_owner(descriptor: int | None = None) -> None:
        if not existing_forward_owner:
            return
        source = _source_path(entry).absolute()
        requested = source / f"{entry['completed_forward_key']}.lock"
        current = requested.lstat()
        if (source.resolve() != source or path != requested or not stat.S_ISREG(current.st_mode)
                or current.st_nlink != 1 or current.st_uid != os.geteuid()):
            raise SnapshotError("completed-forward publication requires its existing original key owner")
        if descriptor is not None:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                raise SnapshotError("completed-forward key owner changed during publication")

    check_existing_owner()
    if inherited_fd is not None:
        if type(inherited_fd) is not int or inherited_fd < 3:
            raise SnapshotError("inherited source lock fd must be an integer >= 3")
        try:
            held = os.fstat(inherited_fd)
            expected = path.stat()
            if not stat.S_ISREG(held.st_mode) or not stat.S_ISREG(expected.st_mode):
                raise SnapshotError("inherited source lock must be a regular file")
            if (held.st_dev, held.st_ino) != (expected.st_dev, expected.st_ino):
                raise SnapshotError("inherited source lock does not match the catalog lock")
            # A distinct open file description held by another process must
            # fail immediately; the owner's inherited description is reentrant.
            fcntl.flock(inherited_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            current = path.stat()
            if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
                raise SnapshotError("catalog source lock changed while borrowing it")
        except OSError as exc:
            raise SnapshotError(f"cannot borrow source coordination lock: {exc}") from exc
        check_existing_owner(inherited_fd)
        try:
            yield inherited_fd
        finally:
            check_existing_owner(inherited_fd)
        return
    duration = SOURCE_LOCK_WAIT_SECONDS if wait_seconds is None else wait_seconds
    if (
        isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise SnapshotError("source coordination lock wait must be finite and nonnegative")
    if not existing_forward_owner:
        path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    deadline = started + duration
    flags = os.O_RDWR | os.O_NONBLOCK | os.O_NOFOLLOW
    handle = os.fdopen(os.open(path, flags | (0 if existing_forward_owner else os.O_CREAT), 0o600), "r+b")
    with handle:
        opened = os.fstat(handle.fileno())
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or opened.st_uid != os.geteuid()):
            raise SnapshotError("source coordination lock must be an owned single-link regular file")
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise SourceCoordinationLockTimeout(
                        f"timed out waiting for source coordination lock: {path}"
                    ) from None
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        check_existing_owner(handle.fileno())
        if not os.path.samestat(opened, path.lstat()):
            raise SnapshotError("source coordination lock inode changed during acquisition")
        print(
            f"source_coordination_lock dataset={entry['dataset']} "
            f"wait_seconds={time.monotonic() - started:.3f}",
            file=sys.stderr,
            flush=True,
        )
        # Close only our descriptor. Explicit LOCK_UN would also release a
        # surviving child's inherited open file description after owner failure.
        try:
            yield handle.fileno()
        finally:
            check_existing_owner(handle.fileno())
            if not os.path.samestat(opened, path.lstat()):
                raise SnapshotError("source coordination lock inode changed during publication")


def _load_catalog(path: Path, include_families: bool = False) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read data release catalog {path}: {exc}") from exc
    if not isinstance(value, dict) or int(value.get("schema_version", -1)) != 1:
        raise SnapshotError(f"unsupported data release catalog: {path}")
    datasets = value.get("datasets")
    if not isinstance(datasets, list):
        raise SnapshotError("catalog datasets must be a list")
    families = value.get("publication_families", [])
    if not isinstance(families, list):
        raise SnapshotError("catalog publication families must be a list")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate([*datasets, *families]):
        if not isinstance(raw, dict):
            raise SnapshotError("catalog dataset entry must be an object")
        dataset = str(raw.get("dataset", ""))
        if not dataset or dataset in seen:
            raise SnapshotError(f"duplicate or empty catalog dataset: {dataset!r}")
        if "loose_threshold_bytes" in raw:
            _loose_threshold_bytes(raw)
        if "file_chunk_bytes" in raw:
            from stockagent.data_sync.packed_snapshots import MAX_FILE_CHUNK_BYTES
            count = raw["file_chunk_bytes"]
            if (type(count) is not int or not 1 <= count <= MAX_FILE_CHUNK_BYTES
                    or raw.get("reader_contract") != "packed-v2-chunks"):
                raise SnapshotError("chunk publication requires bounded bytes and explicit packed-v2-chunks readers")
        is_family = index >= len(datasets)
        if is_family != ("publication_family" in raw):
            raise SnapshotError("catalog publication families must be separate from concrete source datasets")
        if is_family:
            from stockagent.data_sync.completed_forward_publication import validate_template
            validate_template(raw)
        elif "completed_forward_key" in raw:
            raise SnapshotError("completed-forward keys must be explicit CLI selections")
        if "included_root_files" in raw:
            if not isinstance(raw["included_root_files"], list):
                raise SnapshotError("catalog included root files must be an explicit list")
            _normalize_included_root_files(raw["included_root_files"])
            if raw.get("excluded_subtrees"):
                raise SnapshotError("catalog root files cannot be combined with subtree omissions")
        seen.add(dataset)
        if not is_family or include_families:
            result.append(raw)
    return result


def _loose_threshold_bytes(entry: Mapping[str, Any]) -> int:
    if "loose_threshold_bytes" not in entry:
        return int(entry["loose_threshold_mib"]) * 1024 * 1024
    value = entry["loose_threshold_bytes"]
    if "loose_threshold_mib" in entry or type(value) is not int or not 0 < value < 2**63:
        raise SnapshotError("catalog loose threshold requires one explicit positive integer byte count")
    return value


def _running_commands() -> list[tuple[int, str]]:
    commands: list[tuple[int, str]] = []
    own_pid = os.getpid()
    for path in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            pid = int(path.parent.name)
            if pid == own_pid:
                continue
            command = shlex.join([value.decode("utf-8", errors="replace")
                                  for value in path.read_bytes().split(b"\0") if value])
        except (OSError, ValueError):
            continue
        if command:
            commands.append((pid, command))
    return commands


def _blockers(entry: Mapping[str, Any], commands: list[tuple[int, str]]) -> list[dict[str, Any]]:
    patterns = [str(item) for item in entry.get("active_process_substrings", [])]
    def matches(command: str, pattern: str) -> bool:
        # A waiting shell wrapper may mention an already-finished downloader
        # inside its -c program. Match actual script argv, not quoted programs.
        if pattern.endswith(".py") and "/" not in pattern:
            try:
                argv = shlex.split(command)
                return any(Path(arg).name == pattern and not any(c.isspace() for c in arg)
                           for arg in argv) or any(
                    arg == '-m' and i + 1 < len(argv)
                    and argv[i + 1].rsplit('.', 1)[-1] == pattern[:-3]
                    for i, arg in enumerate(argv))
            except ValueError:
                return pattern in command  # malformed evidence fails closed
        return pattern in command
    return [
        {"pid": pid, "pattern": pattern, "command": command}
        for pid, command in commands
        for pattern in patterns
        if matches(command, pattern)
    ]


def _source_path(entry: Mapping[str, Any]) -> Path:
    source = Path(str(entry["source"]))
    return source if source.is_absolute() else REPO_ROOT / source


def _source_freshness(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    config = entry.get("freshness")
    if config is None:
        return None
    if not isinstance(config, Mapping):
        raise SnapshotError(f"dataset {entry['dataset']} freshness must be an object")
    relative = PurePosixPath(str(config.get("receipt", "")))
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotError(f"invalid freshness receipt path: {relative}")
    receipt_path = _source_path(entry).joinpath(*relative.parts)
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(
            f"cannot read freshness receipt {receipt_path}: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise SnapshotError(f"freshness receipt must be an object: {receipt_path}")
    field = str(config.get("field", ""))
    value = str(payload.get(field, ""))
    if config.get("format") != "iso-date":
        raise SnapshotError(f"unsupported freshness format for {entry['dataset']}")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise SnapshotError(
            f"freshness field {field} is not an ISO date: {value!r}"
        ) from exc
    required = config.get("required_values", {})
    if not isinstance(required, Mapping):
        raise SnapshotError("freshness required_values must be an object")
    mismatches = {
        str(key): {"expected": expected, "actual": payload.get(key)}
        for key, expected in required.items()
        if payload.get(key) != expected
    }
    if mismatches:
        raise SnapshotError(
            f"freshness receipt completion gate failed: {mismatches}"
        )
    if "max_receipt_age_hours" in config:
        try:
            max_hours = float(config["max_receipt_age_hours"])
            checked = datetime.fromisoformat(str(payload[config["timestamp_field"]]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotError("freshness receipt timestamp is missing or invalid") from exc
        if (not 0 < max_hours <= 8760 or checked.tzinfo is None
                or not timedelta(0) <= datetime.now(UTC) - checked.astimezone(UTC) <= timedelta(hours=max_hours)):
            raise SnapshotError("freshness receipt is outside its configured age window")
    if entry["dataset"] == "tw-public-research-finlab-2014-v4":
        from scripts.finlab_release_gate import CONTRACT as FINLAB_GATE_CONTRACT
        try:
            valid_until = datetime.fromisoformat(str(payload["catalog_valid_until_utc"]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotError("FinLab release lacks its canonical source-expiry proof") from exc
        if (payload.get("catalog_scope_contract") != FINLAB_GATE_CONTRACT
                or valid_until.tzinfo is None or datetime.now(UTC) >= valid_until.astimezone(UTC)):
            raise SnapshotError("FinLab general-history release proof has expired or changed scope")
    return {
        "field": field,
        "format": "iso-date",
        "receipt": relative.as_posix(),
        "receipt_sha256": sha256_file(receipt_path),
        "value": value,
    }


def _latest_cold_freshness(
    sync_root: Path | None, dataset: str
) -> dict[str, Any] | None:
    if sync_root is None:
        return None
    try:
        # Freshness non-regression is metadata-backed even while an object's
        # payload is missing. Missing bytes cannot erase a newer date gate.
        resolved = resolve_latest_packed(sync_root, dataset, require_objects=False)
    except SnapshotError:
        return None
    metadata = resolved.manifest.get("metadata", {})
    value = metadata.get("freshness_value") if isinstance(metadata, Mapping) else None
    if not value:
        return None
    return {
        "snapshot_id": resolved.manifest["snapshot_id"],
        "field": metadata.get("freshness_field"),
        "format": metadata.get("freshness_format"),
        "value": str(value),
    }


def _status(
    entry: Mapping[str, Any],
    commands: list[tuple[int, str]],
    *,
    sync_root: Path | None = None,
    held_native_lock_fds: tuple[int, ...] = (),
) -> dict[str, Any]:
    source = _source_path(entry)
    blockers = _blockers(entry, commands)
    waiters = _coordinated_native_waiters(entry, blockers, held_native_lock_fds)
    admitted = {(row["pid"], row["pattern"]) for row in waiters}
    blockers = [row for row in blockers if (row["pid"], row["pattern"]) not in admitted]
    freshness_error: str | None = None
    try:
        source_freshness = _source_freshness(entry)
    except SnapshotError as exc:
        source_freshness = None
        freshness_error = str(exc)
    cold_freshness = _latest_cold_freshness(
        sync_root, str(entry["dataset"])
    )
    non_regression = (
        source_freshness is None
        or cold_freshness is None
        or str(source_freshness["value"]) >= str(cold_freshness["value"])
    )
    selection_error = None
    selected = entry.get("included_root_files")
    requires_key = "publication_family" in entry and "completed_forward_key" not in entry
    names = None
    if "included_root_files" in entry:
        try:
            names = _normalize_included_root_files(selected)
            if names is None or entry.get("excluded_subtrees"):
                raise SnapshotError("invalid catalog root file selection")
            for name in names:
                if not stat.S_ISREG((source / name).lstat().st_mode):
                    raise SnapshotError(f"catalog root file is not regular: {name}")
        except (OSError, SnapshotError) as error:
            selection_error = str(error)
    return {
        "dataset": entry["dataset"],
        "role": entry["role"],
        "source": str(source),
        "source_exists": source.exists(),
        "source_resolved": str(source.resolve()) if source.exists() else None,
        "publish": bool(entry["publish"]),
        "publish_ready": bool(entry["publish"])
        and not requires_key
        and source.is_dir()
        and not blockers
        and freshness_error is None
        and selection_error is None
        and non_regression,
        "active_blockers": blockers,
        **({"coordinated_writer_waiters": waiters} if waiters else {}),
        "source_freshness": source_freshness,
        "latest_cold_freshness": cold_freshness,
        "freshness_non_regression": non_regression,
        "freshness_error": freshness_error,
        "note": entry["note"],
        **({"publication_family": entry["publication_family"],
            "requires_explicit_completed_forward_key": requires_key}
           if "publication_family" in entry else {}),
        **({"source_selection": {"scope": "explicit_root_files_only", "files": list(names or ())},
            "selection_error": selection_error} if "included_root_files" in entry else {}),
    }


def _publish_entry(
    entry: Mapping[str, Any], args: argparse.Namespace, sync_root: Path
) -> tuple[dict[str, Any], Any | None]:
    if "publication_family" in entry and "completed_forward_key" not in entry:
        return _status(entry, _running_commands(), sync_root=sync_root), None
    if entry.get("publish") is False:
        # A disabled source is not a writer transaction. Do not wait for its
        # lock, or let an unrelated busy provider delay the all-ready batch.
        return _status(entry, _running_commands(), sync_root=sync_root), None
    expected_inputs = getattr(args, "expected_bybit_input_signature", None)
    source_guard = None
    if expected_inputs is not None:
        expected_inputs = _expected_bybit_signature(expected_inputs)
        if entry["dataset"] != "bybit" or getattr(args, "source_lock_fd", None) is None:
            raise SnapshotError("expected Bybit inputs require Bybit publication under an inherited lock")
        if _source_path(entry).resolve() != (REPO_ROOT / "data_bybit").resolve():
            raise SnapshotError("Bybit input proof does not match the catalog source")

        def source_guard() -> None:
            try:
                current = materialization_input_signature(REPO_ROOT)
            except (OSError, RuntimeError) as exc:
                raise SnapshotError(f"cannot verify Bybit build inputs: {exc}") from exc
            if current != expected_inputs:
                raise SnapshotError("Bybit materialization inputs changed before publication commit")

    # Acquire before the final process/receipt check. A writer that starts
    # between an earlier status call and publication must not escape the gate.
    with _publication_source_locks(
        entry, inherited_fd=getattr(args, "source_lock_fd", None),
        **({"wait_seconds": 0} if getattr(args, "all_ready", False) else {}),
    ) as source_owners:
        # A standalone producer may let this canonical publisher acquire the
        # original owner instead of passing an inherited descriptor. Deferred
        # notification still requires both a real held source lock and the
        # caller's unique commit receipt; it never makes an unlocked source
        # publishable or turns queued scans into delivery acceptance.
        if (getattr(args, "defer_scan", False) and getattr(args, "dataset", None)
                and source_owners["primary_fd"] is None
                and not source_owners["native_lock_fds"]):
            raise SnapshotError("single deferred publication requires a held catalog/native source lock")
        status = _status(entry, _running_commands(), sync_root=sync_root,
                         held_native_lock_fds=source_owners["native_lock_fds"])
        if not status["publish_ready"]:
            return status, None
        from stockagent.data_sync.parquet_publication import ParquetPublicationSnapshot
        parquet_proof = ParquetPublicationSnapshot(_source_path(entry),
            dataset=entry["dataset"],
            excluded=entry.get("excluded_subtrees", ()),
            included_root_files=entry.get("included_root_files"))
        forward_proof = None
        forward_pin = None
        validation = None
        validation_guard = None
        freshness_guard = None
        if status["source_freshness"] is not None:
            def freshness_guard():
                if _source_freshness(entry) != status["source_freshness"]:
                    raise SnapshotError("source freshness receipt changed before publication commit")
        if entry.get('validation_gate'):
            from scripts.validate_training_sources import validate_catalog_source, verify_unchanged
            validation = validate_catalog_source(entry, repo_root=REPO_ROOT)
            validation_guard = lambda: verify_unchanged(entry, validation, repo_root=REPO_ROOT)
        if "completed_forward_key" in entry:
            from stockagent.data_sync.completed_forward_publication import (
                fixed_pin_path, reuse_fixed_pin, validate_source,
            )
            forward_proof = validate_source(entry, _source_path(entry))
            forward_pin = fixed_pin_path(entry, sync_root=sync_root,
                retention_config=args.retention_config, repo_root=REPO_ROOT)
            existing = reuse_fixed_pin(sync_root, forward_pin, entry, forward_proof)
            if existing is not None:
                if validation_guard is not None:
                    validation_guard()
                if freshness_guard is not None:
                    freshness_guard()
                status["fixed_pin"] = {"path": str(forward_pin), "reused": True,
                                       "snapshot_id": existing.manifest["snapshot_id"]}
                return status, existing
        previous_guard = source_guard
        if validation_guard is not None or forward_proof is not None or freshness_guard is not None or parquet_proof.applicable:
            def source_guard():
                if previous_guard is not None:
                    previous_guard()
                parquet_proof.guard()
                if validation_guard is not None:
                    validation_guard()
                if freshness_guard is not None:
                    freshness_guard()
                if forward_proof is not None:
                    forward_proof.guard()
        resolved = publish_packed_snapshot(
            sync_root,
            str(entry["dataset"]),
            _source_path(entry),
            node_id=args.node_id,
            loose_file_threshold_bytes=_loose_threshold_bytes(entry),
            pack_buckets=int(entry["pack_buckets"]),
            **_version_options(entry, args),
            excluded_subtrees=[
                str(value) for value in entry.get("excluded_subtrees", [])
            ],
            **({"included_root_files": entry["included_root_files"]}
               if "included_root_files" in entry else {}),
            metadata={
                "role": str(entry["role"]),
                "catalog_schema": "1",
                **parquet_proof.metadata,
                **(forward_proof.metadata if forward_proof is not None else {}),
                **({'source_validation_contract': str(validation['contract']),
                    'source_validation_identity': str(validation['identity']),
                    'source_validation_status': str(validation['result']['status']),
                    'source_validation_last_date': str(validation['result'].get('end_date',
                                                        validation['result'].get('last_date', ''))),
                    'all_source_coverage_complete_claim': str(validation['result'].get('coverage_complete', False)).lower()}
                   if validation is not None else {}),
                **({
                    "bybit_materialization_input_files": str(expected_inputs["files"]),
                    "bybit_materialization_input_metadata_sha256": str(expected_inputs["metadata_sha256"]),
                } if expected_inputs is not None else {}),
                **(
                    {
                        "freshness_field": str(status["source_freshness"]["field"]),
                        "freshness_format": str(status["source_freshness"]["format"]),
                        "freshness_receipt": str(status["source_freshness"]["receipt"]),
                        "freshness_receipt_sha256": str(
                            status["source_freshness"]["receipt_sha256"]
                        ),
                        "freshness_value": str(status["source_freshness"]["value"]),
                    }
                    if status["source_freshness"] is not None
                    else {}
                ),
            },
            repo_root=REPO_ROOT,
            **({"progress": args.publication_progress}
               if getattr(args, "publication_progress", None) is not None else {}),
            **({"source_guard": source_guard} if source_guard is not None else {}),
            **({"defer_scan": True} if getattr(args, "defer_scan", False) else {}),
            **({"publication_lock_nonblocking": True}
               if getattr(args, "publication_lock_nonblocking", False) else {}),
            **({"batch_directory_fsync": True}
               if getattr(args, "batch_directory_fsync", False) else {}),
            **({"recover_missing_base_objects": True} if args.recover_missing_base_objects else {}),
        )
        if forward_proof is not None:
            from stockagent.data_sync.completed_forward_publication import protect_fixed_pin
            protect_fixed_pin(sync_root, forward_pin, entry, forward_proof, resolved)
            status["fixed_pin"] = {"path": str(forward_pin), "reused": False,
                                   "snapshot_id": resolved.manifest["snapshot_id"]}
        return status, resolved


def _version_options(entry: Mapping[str, Any], args: argparse.Namespace) -> dict[str, int]:
    from stockagent.data_sync.dataset_version_layout import publication_version_options
    return publication_version_options(REPO_ROOT, entry,
        reader_contract=getattr(args, "reader_contract", None))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Publish catalogued canonical datasets through the packed Syncthing "
            "release layer. Downloader work shards are never selected implicitly."
        )
    )
    parser.add_argument("command", choices=("status", "publish", "layout", "migrate"))
    parser.add_argument("dataset", nargs="?")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--sync-root", type=Path)
    parser.add_argument("--node-id")
    parser.add_argument(
        "--reader-contract", choices=("packed-v1", "packed-v2-chunks"), default="packed-v1",
        help="Explicit source-format rollout assertion. V2 shares unchanged large-file chunks; upgrade consumers first.",
    )
    parser.add_argument("--layout-output", type=Path,
                        help="Write the read-only all-data-root layout inventory (layout command only).")
    parser.add_argument("--completed-forward-key", help="Explicit full v3 CPU forward key; never select the whole cache.")
    parser.add_argument("--retention-config", type=Path, default=DEFAULT_RETENTION_CONFIG,
                        help="Existing retention owner configuration for a completed-forward fixed pin.")
    parser.add_argument(
        "--defer-scan", action="store_true",
        help="Durably queue scans; requires a held catalog/native source lock and unique single-source receipt, or an all-ready batch progress receipt. The existing drain owns synchronization.",
    )
    parser.add_argument(
        "--source-lock-fd", type=int,
        help="Borrow a catalog-matched source lock descriptor for one dataset publication.",
    )
    parser.add_argument(
        "--expected-bybit-input-signature", type=_parse_expected_bybit_signature,
        help="Bind Bybit publication to the refresh input metadata proof (JSON).",
    )
    parser.add_argument(
        "--result-receipt", type=Path,
        help="Atomically write the successful single-dataset result to a new local path.",
    )
    parser.add_argument(
        "--batch-progress-receipt", type=Path,
        help="New local all-ready progress path, atomically checkpointed before and after every source. Not peer convergence or migration acceptance.",
    )
    parser.add_argument(
        "--after-dataset",
        help="Resume catalog-ordered all-ready attempts after this explicitly inspected dataset; requires a new batch progress receipt. Does not accept prior sources.",
    )
    parser.add_argument("--recover-missing-base-objects", action="store_true",
        help="Explicit repair: retain damaged history; repack current source members into verified objects before updating the head.")
    parser.add_argument(
        "--all-ready",
        action="store_true",
        help="Publish every catalog entry that is present, publishable and inactive.",
    )
    parser.add_argument("--source-roots-only", action="store_true",
                        help="Scope --all-ready to registered data_* roots; excludes artifact caches and grants no publication rights.")
    parser.add_argument("--migration-state", type=Path,
                        help="Durable operation directory for migrate; source gates and publication rights remain in force.")
    parser.add_argument("--watch", action="store_true", help="migrate: retry deferred sources through this one operation owner.")
    parser.add_argument("--retry-seconds", type=int, default=60)
    parser.add_argument("--maximum-passes", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "migrate":
            from stockagent.data_sync.dataset_migration import migrate_catalog
            if (args.dataset or args.all_ready or args.source_roots_only or args.sync_root is None
                    or args.migration_state is None or args.completed_forward_key
                    or args.source_lock_fd is not None or args.after_dataset
                    or args.result_receipt or args.batch_progress_receipt or args.layout_output):
                raise SnapshotError("migrate requires --sync-root and --migration-state, without publication selections")
            result = migrate_catalog(REPO_ROOT, args, _load_catalog(args.catalog.resolve()))
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result["state"] == "verified" else 75
        if args.migration_state is not None or args.watch:
            raise SnapshotError("--migration-state and --watch require migrate")
        if args.source_roots_only and (args.command != "publish" or not args.all_ready or args.dataset):
            raise SnapshotError("--source-roots-only requires publish --all-ready without an explicit dataset")
        if args.completed_forward_key is not None:
            from stockagent.data_sync.completed_forward_publication import validate_key
            validate_key(args.completed_forward_key)
            if not args.dataset or args.all_ready or args.sync_root is None:
                raise SnapshotError("completed-forward key requires one explicit dataset and sync root")
            _check_packed_read_authority(args.sync_root.absolute())
        single_deferred_owner = (
            args.command == "publish" and args.dataset and not args.all_ready
            and args.result_receipt is not None
        )
        batch_deferred_owner = (
            args.command == "publish" and args.all_ready and not args.dataset
            and args.batch_progress_receipt is not None
        )
        if args.defer_scan and not (single_deferred_owner or batch_deferred_owner):
            raise SnapshotError("--defer-scan requires one guarded publication with a unique result receipt, or an all-ready batch progress receipt")
        if args.batch_progress_receipt is not None:
            if args.command != "publish" or not args.all_ready or args.dataset:
                raise SnapshotError("--batch-progress-receipt requires publish --all-ready")
            if args.batch_progress_receipt.exists() or args.batch_progress_receipt.is_symlink():
                raise SnapshotError("batch progress receipt must be a new path")
        if args.after_dataset is not None and not batch_deferred_owner:
            raise SnapshotError("--after-dataset requires publish --all-ready and a new batch progress receipt")
        if args.expected_bybit_input_signature is not None:
            args.expected_bybit_input_signature = _expected_bybit_signature(
                args.expected_bybit_input_signature,
            )
            if (
                args.command != "publish" or args.dataset != "bybit"
                or args.all_ready or args.source_lock_fd is None
            ):
                raise SnapshotError(
                    "--expected-bybit-input-signature requires explicit Bybit publish with --source-lock-fd"
                )
        if args.result_receipt is not None:
            if args.command != "publish" or not args.dataset or args.all_ready:
                raise SnapshotError("--result-receipt requires one explicit publish dataset")
            if args.result_receipt.exists() or args.result_receipt.is_symlink():
                raise SnapshotError("publication result receipt must be a new path")
        if args.source_lock_fd is not None and (
            args.command != "publish" or not args.dataset or args.all_ready
            or args.source_lock_fd < 3
        ):
            raise SnapshotError(
                "--source-lock-fd requires one explicit publish dataset and an fd >= 3"
            )
        entries = _load_catalog(args.catalog.resolve(), True)
        if args.layout_output is not None and args.command != "layout":
            raise SnapshotError("--layout-output requires the layout command")
        if args.command == "layout":
            from stockagent.data_sync.dataset_version_layout import inventory_version_layout
            if args.all_ready or args.completed_forward_key or args.source_lock_fd is not None:
                raise SnapshotError("layout is metadata-only, not a publication or source transaction")
            result = inventory_version_layout(REPO_ROOT, entries,
                sync_root=args.sync_root.resolve() if args.sync_root else None)
            if args.dataset:
                selected_roots = [row for row in result["roots"] if row["root"] == args.dataset]
                if not selected_roots:
                    raise SnapshotError(f"unregistered data root: {args.dataset}")
                result = {**result, "roots": selected_roots, "root_count": len(selected_roots)}
            if args.layout_output is not None:
                atomic_write_json(args.layout_output, result)
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0 if result["state"] == "accepted" else 1
        if args.completed_forward_key is not None:
            from stockagent.data_sync.completed_forward_publication import select_entry
            selected_family = [entry for entry in entries if entry["dataset"] == args.dataset]
            if not selected_family:
                raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
            entries = [select_entry(selected_family[0], args.completed_forward_key)]
            args.dataset = str(entries[0]["dataset"])
        commands = _running_commands()
        status_sync_root = args.sync_root.resolve() if args.sync_root else None
        if args.command == "status":
            selected = entries
            if args.dataset:
                selected = [item for item in entries if item["dataset"] == args.dataset]
                if not selected:
                    raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "datasets": [
                            _status(item, commands, sync_root=status_sync_root)
                            for item in selected
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

        if args.sync_root is None:
            raise SnapshotError("publish requires --sync-root")
        if args.recover_missing_base_objects and args.all_ready:
            raise SnapshotError("missing-object recovery requires one explicit dataset")
        if bool(args.dataset) == bool(args.all_ready):
            raise SnapshotError("select exactly one dataset or --all-ready")
        selected = (
            [item for item in entries if item["dataset"] == args.dataset]
            if args.dataset
            else entries
        )
        if args.source_roots_only:
            from stockagent.data_sync.dataset_version_layout import source_root_entries
            selected = source_root_entries(REPO_ROOT, selected)
        if not selected:
            raise SnapshotError(f"unknown catalog dataset: {args.dataset}")
        receipt_source_scope = selected
        if args.after_dataset is not None:
            cursor = next((i for i, item in enumerate(selected) if item["dataset"] == args.after_dataset), None)
            if cursor is None:
                raise SnapshotError("resume cursor is not in the selected catalog scope")
            selected = selected[cursor + 1:]
        sync_root = args.sync_root.resolve()
        if args.batch_progress_receipt is not None:
            receipt_path = args.batch_progress_receipt.resolve()
            if receipt_path.is_relative_to(sync_root) or any(
                receipt_path.is_relative_to(_source_path(item).resolve()) for item in receipt_source_scope
            ):
                raise SnapshotError("batch progress receipt must stay outside cold and selected source trees")
        initialize_packed_layout(sync_root, node_id=args.node_id)
        results: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        def checkpoint_batch(state: str, active_dataset: str | None = None) -> None:
            if args.batch_progress_receipt is None:
                return
            atomic_write_json(args.batch_progress_receipt, {
                "contract": "catalog_publication_batch_progress_v1",
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "state": state, "selected_datasets": [item["dataset"] for item in selected],
                "resume_after_dataset": args.after_dataset,
                "prior_sources_accepted_by_this_receipt": False,
                "active_dataset": active_dataset,
                "active_outcome": "unknown_check_current_head" if active_dataset else None,
                "completed_attempts": len(results) + len(skipped),
                "published": results, "skipped": skipped,
                "scan_policy": "durably_queued" if args.defer_scan else "immediate",
                "peer_convergence": "not_checked",
                "release_restore_acceptance": "not_checked",
                "all_dataset_migration_complete": False,
            })

        checkpoint_batch("running")
        for entry in selected:
            # If interrupted here, the old head plus the durable pending scan
            # remain authoritative. A missing result is never "unpublished".
            checkpoint_batch("running", str(entry["dataset"]))
            try:
                status, resolved = _publish_entry(entry, args, sync_root)
            except SourceCoordinationLockTimeout as exc:
                if not args.all_ready:
                    raise
                skipped.append({"dataset": entry["dataset"], "publish_ready": False,
                                "source_lock_busy": str(exc), "retryable": True,
                                "committed_head_outcome": "not_started_source_lock_busy"})
                checkpoint_batch("running")
                continue
            except (SnapshotError, OSError, ValueError) as exc:
                if not args.all_ready:
                    raise
                skipped.append({"dataset": entry["dataset"], "publish_ready": False,
                                "validation_or_publication_error": str(exc),
                                "committed_head_outcome": "unknown_check_current_head"})
                checkpoint_batch("running")
                continue
            if resolved is None:
                if args.all_ready:
                    skipped.append(status)
                    checkpoint_batch("running")
                    continue
                raise SnapshotError(
                    f"dataset {entry['dataset']} is not publish-ready: "
                    f"publish={entry['publish']} source_exists={status['source_exists']} "
                    f"active_blockers={status['active_blockers']}"
                )
            results.append(
                {
                    "dataset": entry["dataset"],
                    **({"fixed_pin": status["fixed_pin"],
                        "completed_forward_key": entry["completed_forward_key"]}
                       if "fixed_pin" in status else {}),
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "scan_policy": "durably_queued" if args.defer_scan else "immediate",
                    "inventory_sha256": resolved.manifest["archive"]["inventory"]["sha256"],
                    "source_files": resolved.manifest["source"]["files"],
                    "source_bytes": resolved.manifest["source"]["logical_bytes"],
                    "sync_objects": resolved.manifest["archive"]["object_count"] + 1,
                    "sync_bytes": resolved.manifest["archive"]["stored_bytes"],
                    "base_snapshot_id": resolved.manifest["archive"].get(
                        "base_snapshot_id"
                    ),
                    "reused_files": resolved.manifest["archive"].get(
                        "reused_files", 0
                    ),
                    "changed_files": resolved.manifest["archive"].get(
                        "changed_files"
                    ),
                    "new_sync_objects": resolved.manifest["archive"].get(
                        "new_object_count"
                    ),
                    "new_sync_bytes": resolved.manifest["archive"].get(
                        "new_stored_bytes"
                    ),
                    **({"new_sync_objects": 0, "new_sync_bytes": 0, "changed_files": 0,
                        "reused_files": resolved.manifest["source"]["files"]}
                       if status.get("fixed_pin", {}).get("reused") is True else {}),
                }
            )
            checkpoint_batch("running")
        result_payload = {"published": results, "skipped": skipped}
        checkpoint_batch("finished")
        if args.result_receipt is not None:
            # A failure here can follow a committed head. Callers must treat a
            # missing receipt/nonzero exit as outcome unknown, not unpublished.
            if args.result_receipt.exists() or args.result_receipt.is_symlink():
                raise SnapshotError("publication result receipt must be a new path")
            atomic_write_json(args.result_receipt, result_payload)
        print(
            json.dumps(
                result_payload,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (OSError, SnapshotError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
