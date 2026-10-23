"""Resumable all-catalog rollout using the existing packed publisher and verifier.

This operation owns only its journal. Source locks, cold transactions, private
boundaries and NAS delivery remain with their original owners. A completed
attempt is never confused with a reconstructible version or a deletion proof.
"""
from __future__ import annotations

from contextlib import closing, contextmanager, nullcontext
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import time
from types import SimpleNamespace
from urllib.parse import quote
import uuid

from stockagent.data_sync.desync_snapshots import (
    SnapshotError, atomic_write_bytes, atomic_write_json, sha256_file,
)
from stockagent.data_sync.packed_snapshots import (
    PackedPublicationBusy, _check_packed_read_authority, _iter_inventory,
    _storage_extents,
    initialize_packed_layout, publish_packed_snapshot, resolve_latest_packed,
    resolve_packed_snapshot_id, verify_packed_snapshot,
)
from stockagent.data_sync.packed_metadata import MetadataRows

CONTRACT = "all_catalog_incremental_migration_v1"
PRIVATE_RELATIVE = ".local-state/private-history"
CONTROL_SCOPE = "recursive_mutable_controls_v17"
FROZEN_CONTROLS_INLINE_LIMIT = 4096


def _identity(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _now():
    return datetime.now(UTC).isoformat()


def migration_process_identity(pid=None):
    """Fence reused PIDs and rebooted workers without a time-based liveness guess."""
    pid = os.getpid() if pid is None else pid
    if type(pid) is not int or pid < 1:
        raise ValueError('invalid migration owner PID')
    fields = Path(f'/proc/{pid}/stat').read_text().rpartition(') ')[2].split()
    return {'pid': pid, 'start_ticks': int(fields[19]),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def migration_owner_observation(directory, owner):
    """Read-only owner check; never create, replace or unlock the owner inode."""
    path = directory / 'owner.lock'
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return {'lock_held': False, 'process_matches': False}
    try:
        held = False
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            held = True
        if os.fstat(descriptor).st_ino != path.lstat().st_ino:
            raise ValueError('migration owner lock inode changed during status')
        matches = False
        if owner:
            try:
                matches = migration_process_identity(owner['pid']) == {
                    key: owner[key] for key in ('pid', 'start_ticks', 'boot_id')}
            except (OSError, ValueError, KeyError, IndexError):
                pass
        return {'lock_held': held, 'process_matches': matches}
    finally:
        os.close(descriptor)


def migration_entries(repo: Path, catalog: list[dict], policy: dict) -> list[dict]:
    """Do not confuse catalogued subtrees/exclusions with full data-root coverage."""
    from scripts.publish_data_releases import _source_path
    entries = [dict(entry) for entry in catalog if "publication_family" not in entry]
    for name in policy["roots"]:
        root = (repo / name).resolve()
        bindings = [entry for entry in entries
                    if _source_path(entry).resolve() == root
                    or _source_path(entry).resolve().is_relative_to(root)]
        if not bindings:
            raise SnapshotError(f"data root lacks an original source owner: {name}")
        if any(_source_path(entry).resolve() == root and not entry.get("excluded_subtrees")
               and not entry.get("included_root_files") for entry in bindings):
            continue
        entries.append({"dataset": "local-" + name[5:].replace("_", "-").lower(), "source": str(root),
            "publish": False, "role": "host-private-whole-root-preservation",
            "note": "Includes original catalog omissions locally only; no export or retirement grant.",
            "pack_buckets": 64, "loose_threshold_mib": 8,
            "reader_contract": "packed-v2-chunks", "file_chunk_bytes": policy["file_chunk_bytes"],
            "active_process_substrings": sorted({pattern for entry in bindings
                                                 for pattern in entry.get("active_process_substrings", [])}),
            "migration_root": name})
    ids = [entry["dataset"] for entry in entries]
    if len(ids) != len(set(ids)):
        raise SnapshotError("migration root identity collides with the existing catalog")
    return entries


def _bootstrap_public_storage(repo: Path, entry: dict, parent: Path, cold: Path, catalog: list[dict],
                              *, file_sink: MetadataRows[dict] | None = None, progress=None):
    """Link already-public exact objects into the ignored security domain.

    Only public -> private is allowed, never the reverse. Existing immutable D
    objects keep their original names; no bytes or heads are removed. Missing,
    incompatible or cross-filesystem donors simply remain unused.
    """
    from scripts.publish_data_releases import _source_path
    files = file_sink if file_sink is not None else {}
    if (cold / "heads" / entry["dataset"]).is_dir():
        return files, {}, []
    source = _source_path(entry).resolve()
    objects, proofs = {}, []
    examined, last_progress = 0, time.monotonic()
    def report():
        if progress is not None:
            progress({"stage": "bootstrapping_public_objects", "acceptance": False,
                      "files_completed": 0, "files_total": None,
                      "logical_bytes_processed": 0, "logical_bytes_total": None,
                      "donor_rows_examined": examined, "file_hints_indexed": len(files),
                      "public_objects_available": len(objects),
                      "metadata_spilled": isinstance(files, MetadataRows) and files.spilled})
    report()
    for donor in catalog:
        root = _source_path(donor).resolve()
        if donor.get("publish") is not True or not root.is_relative_to(source):
            continue
        if not (parent / "heads" / donor["dataset"]).is_dir():
            continue
        selected = resolve_latest_packed(parent, donor["dataset"], require_objects=False)
        prefix = root.relative_to(source)
        descriptors = {obj["sha256"]: obj for obj in selected.manifest["archive"]["objects"]}
        admitted = {}
        for row in _iter_inventory(parent, selected.manifest):
            examined += 1
            if examined % 1024 == 0 and time.monotonic() - last_progress >= 5:
                last_progress = time.monotonic()
                report()
            if row["kind"] != "file" or (prefix.parts and row["storage"]["kind"] == "pack"):
                continue  # embedded member names must not be relabelled
            extents = _storage_extents(row["storage"], row["size"])
            ready = True
            for digest, _size in extents:
                if digest in admitted:
                    continue
                obj = descriptors[digest]
                original, alias = parent / obj["relpath"], cold / obj["relpath"]
                if original.is_symlink() or not original.is_file():
                    ready = False
                    break
                if not alias.exists():
                    before = original.lstat()
                    if before.st_size != obj["bytes"] or sha256_file(original) != digest:
                        raise SnapshotError("public bootstrap object is corrupt; preserve and repair through its owner")
                    alias.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        os.link(original, alias, follow_symlinks=False)
                    except FileExistsError:
                        pass
                    except OSError:
                        ready = False
                        break
                    after = original.lstat()
                    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                        raise SnapshotError("public bootstrap source object changed")
                    # Persist the new name before a private manifest can refer to it.
                    descriptor = os.open(alias.parent, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                if alias.is_symlink() or alias.stat().st_size != obj["bytes"]:
                    raise SnapshotError("private bootstrap object alias differs")
                admitted[digest] = obj
            if ready:
                name = (prefix / row["path"]).as_posix()
                hint = {**row, "path": name}
                if isinstance(files, MetadataRows):
                    files.upsert(hint)
                else:
                    files[name] = hint
                objects.update({digest: admitted[digest] for digest, _ in extents})
        proofs.append({"dataset": donor["dataset"], "snapshot_id": selected.manifest["snapshot_id"],
                       "manifest_sha256": selected.manifest_sha256})
    report()
    return files, objects, proofs


def private_history_root(parent: Path, policy: dict, *, initialize: bool = True) -> Path:
    """Keep restricted bytes below the already ignored original owner's state."""
    requested = parent.absolute()
    _check_packed_read_authority(requested)
    if requested.resolve() != requested:
        raise SnapshotError("private history parent must be the original non-redirected authority")
    settings = policy.get("private_history", {})
    if settings != {"enabled": True, "relative_root": PRIVATE_RELATIVE, "shared_export": False,
                    "nas_export": False, "source_retirement": False}:
        raise SnapshotError("private history needs its explicit local-only policy")
    ignore = (parent / ".stignore").read_text(encoding="utf-8").splitlines()
    if "(?d).local-state" not in ignore or "(?d).local-state/**" not in ignore:
        raise SnapshotError("original local-state Syncthing exclusion is not installed")
    root = parent / PRIVATE_RELATIVE
    if root.is_symlink() or root.resolve() != root:
        raise SnapshotError("private history is redirected")
    marker = {"contract": CONTRACT, "parent_authority": str(parent), "shared_export": False,
              "nas_export": False, "source_retirement": False}
    if not root.exists():
        if not initialize:
            raise SnapshotError("private history has not been initialized")
        root.mkdir(mode=0o700)
        atomic_write_json(root / "private-history.json", marker)
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700
            or json.loads((root / "private-history.json").read_text()) != marker):
        raise SnapshotError("private history owner/permissions/marker differs")
    return root


@contextmanager
def _operation_owner(directory: Path):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.resolve() != directory:
        raise SnapshotError("migration journal must not be redirected")
    descriptor = os.open(directory / "owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PackedPublicationBusy("the original migration operation is still active") from None
        yield
    finally:
        os.close(descriptor)


def _sqlite_databases(source: Path, excluded: list[str], *, progress=None) -> list[Path]:
    """Discover databases, not arbitrary state to silently omit from coverage."""
    result = []
    examined, last_progress = 0, time.monotonic()
    def report():
        if progress is not None:
            progress({"stage": "discovering_sqlite_states", "acceptance": False,
                      "files_completed": 0, "files_total": None,
                      "logical_bytes_processed": 0, "logical_bytes_total": None,
                      "files_examined": examined, "sqlite_states_discovered": len(result)})
    report()
    def walk_error(error):
        raise error
    for directory, names, files in os.walk(source, followlinks=False, onerror=walk_error):
        relative = Path(directory).relative_to(source)
        names[:] = sorted(name for name in names
                          if (relative / name).as_posix() not in excluded)
        for name in sorted(files):
            examined += 1
            if examined % 1024 == 0 and time.monotonic() - last_progress >= 5:
                last_progress = time.monotonic()
                report()
            # Reject by name first. A parquet/body does not need ANY file stat
            # merely to discover a handful of native SQLite databases.
            if not name.lower().endswith((".db", ".sqlite", ".sqlite3")):
                continue
            path = Path(directory) / name
            if path.is_symlink():
                continue
            if path.relative_to(source).as_posix() in excluded:
                continue
            with path.open("rb") as stream:
                if stream.read(16) == b"SQLite format 3\x00":
                    result.append(path)
    report()
    return result


def _snapshot_sqlite(database: Path, staging: Path, logical_path: str) -> dict:
    """SQLite's own read transaction, never copy a live main/WAL/SHM trio."""
    staging.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    marker = {"contract": "owned_sqlite_backup_intent_v1", "source": str(database),
              "source_relative": logical_path, "target": "snapshot.sqlite3"}
    if staging.exists():
        # A crash between mkdir and its first durable marker leaves no data.
        # Only that exact empty, owned directory may finish initialization.
        if (not staging.is_symlink() and staging.resolve() == staging and not any(staging.iterdir())
                and staging.stat().st_uid == os.geteuid() and stat.S_IMODE(staging.stat().st_mode) == 0o700):
            atomic_write_json(staging / "backup-intent.json", marker)
        if (staging.is_symlink() or staging.resolve() != staging
                or json.loads((staging / "backup-intent.json").read_text()) != marker
                or set(path.name for path in staging.iterdir()) - {
                    "backup-intent.json", "snapshot.sqlite3", "snapshot.sqlite3-journal",
                    "snapshot.sqlite3-wal", "snapshot.sqlite3-shm", "snapshot.json"}):
            raise SnapshotError("native SQLite staging differs from its durable owned intent")
        if (staging / "snapshot.json").exists():
            proof = json.loads((staging / "snapshot.json").read_text())
            payload = staging / "snapshot.sqlite3"
            if (payload.is_symlink() or payload.stat().st_size != proof["snapshot_bytes"]
                    or sha256_file(payload) != proof["snapshot_sha256"]):
                raise SnapshotError("completed native SQLite backup changed")
            return proof
    else:
        staging.mkdir(mode=0o700)
        atomic_write_json(staging / "backup-intent.json", marker)
    target = staging / "snapshot.sqlite3"
    if shutil.disk_usage(staging.parent).free < database.stat().st_size * 1.2 + 5 * 1024**3:
        raise SnapshotError("insufficient native snapshot scratch space; retain source and retry")
    if any(path.is_symlink() for path in staging.iterdir()):
        raise SnapshotError("native SQLite scratch is redirected")
    from stockagent.data_sync.materialized_cache import process_references_many
    if process_references_many([staging]):
        raise SnapshotError("native SQLite scratch is still used by another process")
    started = time.monotonic()
    def progress(_status, _remaining, _total):
        if time.monotonic() - started > 900:
            raise SnapshotError("consistent SQLite snapshot exceeded its bounded attempt")
    with closing(sqlite3.connect("file:" + quote(str(database), safe="/") + "?mode=ro", uri=True,
                                 timeout=5)) as original, closing(sqlite3.connect(target)) as snapshot:
        journal_mode = original.execute("PRAGMA journal_mode").fetchone()[0]
        if journal_mode == "wal":
            # Pin one WAL end mark before incremental backup. Otherwise busy
            # collectors can restart the entire copy after every 1024 pages.
            # WAL readers do not block writers; never hold this long read
            # transaction on a rollback-journal source.
            original.execute("BEGIN")
            original.execute("SELECT rootpage FROM sqlite_schema LIMIT 1").fetchall()
        original.backup(snapshot, pages=1024, progress=progress, sleep=0.05)
        # Only the owned backup destination is normalized. A portable closed
        # backup must not depend on live destination WAL/SHM files, and a
        # connection context manager alone does NOT close its connection.
        snapshot.commit()
        if snapshot.execute("PRAGMA journal_mode=DELETE").fetchone() != ("delete",):
            raise SnapshotError("native SQLite backup did not become a standalone database")
        if snapshot.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise SnapshotError("consistent SQLite snapshot failed its own integrity check")
    with target.open("rb") as stream:
        os.fsync(stream.fileno())
    proof = {"contract": "sqlite_backup_api_consistent_state_v1", "source_relative": logical_path,
             "snapshot_sha256": sha256_file(target), "snapshot_bytes": target.stat().st_size,
             "source_journal_mode": journal_mode,
             "source_read_snapshot_pinned": journal_mode == "wal",
             "destination_connections_closed": True,
             "raw_main_wal_shm_byte_copy": False}
    atomic_write_json(staging / "snapshot.json", proof)
    return proof


def _mutable_published_locations(source: Path) -> dict[str, tuple[str, ...] | None]:
    """Known closed/atomically replaced outputs, never arbitrary raw payloads.

    None means the direct Parquet children of this exact writer directory.
    OpenBB closes/checkpoints/validates its temporary catalog before replace;
    it is not a live in-place DuckDB backup. Native SQLite uses its own API.
    """
    if source.name in {'data_binance', 'data_bybit', 'data_okx'}:
        return {grain + '/_hot_tail': None for grain in ('1m', '15m', 'daily')}
    if source.name == 'data_taifex_public_history':
        return {'rules/normalized': None}
    if source.name == 'rules' and source.parent.name == 'data_taifex_public_history':
        # The existing rule-only catalog is a separate source from its whole
        # root complement. Both refer to the same original atomic publisher.
        return {'normalized': None}
    if source.name == 'data_finlab':
        # finlab_research_overlay closes and atomically replaces these exact
        # deterministic-token outputs, including existing build partitions.
        # A token is not immutable-inode evidence: later overlays can rewrite
        # it during a multi-hour baseline. Never probe raw descendants here.
        locations = {}
        normalized = source / 'normalized'
        if normalized.exists() or normalized.is_symlink():
            if normalized.resolve() != normalized or not normalized.is_dir():
                raise SnapshotError('FinLab normalized writer directory is redirected')
            locations['normalized'] = tuple(sorted(member.name for member in normalized.iterdir()
                if re.fullmatch(r'[0-9a-f]{24}\.parquet', member.name)))
        builds = normalized / 'builds'
        if builds.exists() or builds.is_symlink():
            if builds.resolve() != builds or not builds.is_dir():
                raise SnapshotError('FinLab normalized build directory is redirected')
            for directory in sorted(builds.iterdir()):
                if not re.fullmatch(r'[0-9a-f]{24}', directory.name):
                    continue
                if directory.resolve() != directory or not directory.is_dir():
                    raise SnapshotError('FinLab normalized build identity is redirected')
                locations['normalized/builds/' + directory.name] = tuple(sorted(
                    member.name for member in directory.iterdir()
                    if re.fullmatch(r'part-[0-9]{4}\.parquet', member.name)))
        return locations
    if source.name == 'data_openBB':
        # Reviewed catalog/monitor writers publish closed outputs. Arbitrary
        # raw/provider Parquet files do not inherit their mutable semantics.
        return {'.': ('openbb_l1.duckdb',), 'catalog': (
            'l1_compaction_status.parquet', 'l1_compaction_audit.parquet',
            'provider_eta.parquet', 'category_eta.parquet', 'provider_endpoint_eta.parquet',
            'coverage.parquet', 'equity_universe.parquet', 'currency_universe.parquet',
            'configured_credentials.parquet', 'provider_rate_limits.parquet',
            'completeness_contract_audit.parquet', 'compaction_summary.parquet',
            'compaction_audit.parquet')}
    if source.name == 'data_tw_public':
        # build_tw_public_training_features closes its temporary table and
        # checks source identities before os.replace. This is not permission
        # to copy arbitrary feature/raw Parquet files that change in place.
        return {'features': ('tw_public_stock_daily.parquet',
                             'tw_public_research_wide_2014_v1.parquet')}
    return {}


def _mutable_control_file_locations(source: Path) -> dict[str, tuple[str, ...]]:
    """Exact operational filenames owned by the existing collectors.

    Lock *contents* can hold a changing storage epoch; capturing those bytes
    never replaces/unlinks the live lock inode or claims a source read lease.
    Probe only contract directories on an upgrade, not their raw descendants.
    """
    locations = {name: ('download_report.csv',) for name in ('shioaji', 'shioaji_1m')}
    if source.name in {'shioaji', 'shioaji_1m'}:
        locations['.'] = ('download_report.csv',)
    if source.name in {'data_binance', 'data_bybit', 'data_okx'}:
        locations.update({grain: ('.download.lock', 'download_report.csv')
                          for grain in ('1m', '15m', 'daily')})
    if source.name in {'data_tw_futures', 'data_tw_index_futures'}:
        history = source / 'shioaji_history'
        if history.is_dir() and not history.is_symlink():
            for contract in sorted(history.iterdir()):
                if not contract.is_symlink() and contract.is_dir():
                    locations['shioaji_history/' + contract.name] = ('download.lock',)
    if source.name == 'data_tw_public':
        # Both original builders atomically publish these exact companions.
        # They are separate fixed observations, not a globally atomic pair.
        locations['features'] = ('tw_public_stock_daily.summary.json',
                                 'tw_public_research_wide_2014_v1.research.json')
        metadata = source / 'metadata'
        if metadata.is_dir() and not metadata.is_symlink():
            locations['metadata'] = tuple(member.name for member in metadata.iterdir()
                                          if member.name.endswith('_versions.jsonl'))
        journals = source / 'state/journals'
        if journals.is_dir() and not journals.is_symlink():
            # download_tw_public_data and download_tw_taiex_ohlc append these
            # exact per-dataset query receipts. Capture a complete stable file,
            # not a moving path or a truncated-to-last-newline prefix.
            locations['state/journals'] = tuple(member.name for member in journals.iterdir()
                                                if member.name.endswith('.jsonl'))
    if source.name == 'data_finlab':
        # download_finlab_history publishes this control before each API key.
        # Dated/raw response records must not inherit its mutable semantics.
        locations['runs'] = ('latest.json',)
        locations['catalog'] = ('discovery.json',)
        # The market collector atomically replaces its two queue frontiers,
        # shared provider backoff and bounded batch index. Calibration rewrites
        # the seed from original service/quota evidence. Dated batch_receipts
        # remain immutable payloads; none of these names imply a global clock.
        locations['intraday'] = ('minute_cursor.json', 'market_cursor.json',
                                 'source_backoff.json', 'batch_metrics.json',
                                 'batch_metrics_seed.json')
        # capture_empty_evidence rewrites the observation clock under the same
        # raw-content digest. Only these two reviewed producers are controls;
        # their immutable Feather evidence and arbitrary JSON remain strict.
        evidence = source / 'empty_evidence'
        if evidence.exists() or evidence.is_symlink():
            if evidence.resolve() != evidence or not evidence.is_dir():
                raise SnapshotError('FinLab empty evidence directory is redirected')
            from stockagent.data.finlab_acquisition_contract import safe_stem
            stems = tuple(safe_stem(key) + '-' for key in (
                'dividend_otc:權息', 'management_change_events:變更交易開始日'))
            locations['empty_evidence'] = tuple(sorted(member.name for member in evidence.iterdir()
                if any(member.name.startswith(stem)
                       and re.fullmatch(r'[0-9a-f]{64}\.json', member.name[len(stem):])
                       for stem in stems)))
    if source.name == 'data_finmind':
        # finmind_json_pipeline.run_cycle atomically replaces this bounded
        # summary after a batch. It is not one of the raw response records.
        locations.update({root: ('json_pipeline_last.json',)
                          for root in ('.', 'complement', 'sponsor')})
        # The announcement watcher replaces these two heads, while snapshots/
        # and checks/ retain the original dated evidence. They are not raw
        # financial observations or permission to freeze arbitrary plan files.
        locations['announcements'] = ('head.json', 'repair_plan.json')
    if source.name == 'data_openBB':
        # The priority queue and L1 compactor own these atomic control heads.
        # Probe exact names on retry instead of rescanning millions of shards.
        locations['_state'] = ('public_priority_cursors.json',
                               'public_priority_queue_event.json', 'public_priority_plan.json',
                               'l1_compaction_attempt_latest.json', 'l1_compaction_latest.json',
                               'last_run_summary.json', 'downloader_phase.json',
                               'catalog_followup_repair_latest.json', 'provider_scheduler.json',
                               'provider_cooldowns.json', 'monitor_latest.json', 'audit_latest.json',
                               'archive_end_date.txt', 'supervisor.pid', 'supervisor.lock',
                               'openbb_l1_compaction.lock', 'public_priority.lock')
        locations['catalog'] = ('completeness_contract_summary.json',)
    return locations


def _mutable_streamed_control_locations(source: Path) -> dict[str, tuple[str, ...]]:
    """Reviewed producer logs may exceed the small JSON control size bound.

    These are whole stable observations, not permissive append prefixes. An
    append/truncate during the bounded copy must retry before packing. Source
    logs remain intact and full original-byte recovery still gates acceptance.
    """
    if source.name == 'data_openBB':
        return {'logs': ('supervisor.log', 'supervisor.log.1',
                         'supervisor_launcher.log', 'supervisor-launch.log'),
                '_state': ('monitor_history.jsonl', 'monitor_dashboard_history.jsonl')}
    return {}


def _mutable_json_control_locations(source: Path) -> frozenset[str]:
    """Direct, named operational outputs; immutable article JSON is not a control.

    NewsHTTP atomically replaces provider cooldowns and per-origin robots
    policy caches under these directories. Their arbitrary provider/hash
    names cannot be recognized by generic status/progress filename suffixes.
    """
    if source.name == 'data_news_fulltext':
        return frozenset({'cooldowns', 'robots'})
    if source.name == 'data_tw_public':
        # The canonical collector atomically writes Data.gov catalog metadata
        # here; financial observations/raw responses are separate payloads.
        return frozenset({'metadata'})
    return frozenset()


def _validate_frozen_control(raw: bytes, *, json_control: bool, json_lines: bool) -> None:
    if json_control:
        json.loads(raw)
    elif json_lines:
        # Preserve the complete selected journal, never truncate a partial row
        # or substitute values. The collector appends complete JSON + newline.
        if raw and not raw.endswith(b'\n'):
            raise SnapshotError('public source journal has an incomplete final record')
        for line in raw.splitlines():
            json.loads(line)


def _freeze_streamed_observation(original: Path, target: Path, logical: str, *,
                                 capture_kind='closed_atomic_published_payload', json_lines=False) -> dict:
    """One bounded-memory stable-FD copy of a reviewed output or operational log."""
    from stockagent.data_sync.desync_snapshots import _fsync_directory, rename_without_overwrite
    from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash

    if target.exists():
        # Only a complete fsynced capture can acquire this final name. A crash
        # before saving the preparation must retain that observation, not copy
        # today's new catalog/tail over it.
        info = payload_info(target)
        return {'path': str(target), 'sha256': stable_payload_hash(target, info),
                'bytes': info['size'], 'capture_kind': capture_kind}
    signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    for attempt in range(3):
        temporary = target.with_name('.capture-' + uuid.uuid4().hex + '.partial')
        try:
            descriptor = os.open(original, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
            with os.fdopen(descriptor, 'rb') as source:
                before = os.fstat(source.fileno())
                if not stat.S_ISREG(before.st_mode):
                    raise SnapshotError(f'published mutable source is not regular: {logical}')
                if shutil.disk_usage(target.parent).free < before.st_size * 1.2 + 5 * 1024**3:
                    raise SnapshotError('published mutable capture would cross its 5 GiB scratch reserve')
                if original.name == 'openbb_l1.duckdb' and original.with_suffix('.duckdb.wal').exists():
                    raise SnapshotError('OpenBB published catalog has a WAL; native owner recovery required')
                digest, copied, oversized = hashlib.sha256(), 0, False
                output_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
                with os.fdopen(output_fd, 'wb') as output:
                    while block := (source.readline(64 * 1024**2 + 1) if json_lines else
                                    source.read(min(8 * 1024**2, before.st_size - copied + 1))):
                        if copied + len(block) > before.st_size:
                            oversized = True
                            break  # A growing in-place writer cannot make this copy unbounded.
                        if json_lines:
                            if len(block) > 64 * 1024**2 or not block.endswith(b'\n'):
                                raise SnapshotError(f'operational journal has an incomplete or oversized record: {logical}')
                            json.loads(block)
                        digest.update(block)
                        output.write(block)
                        copied += len(block)
                    after = os.fstat(source.fileno())
                    stable = not oversized and signature(before) == signature(after) and copied == before.st_size
                    if stable:
                        output.flush()
                        os.fsync(output.fileno())
            if not stable:
                if attempt == 2:
                    raise SnapshotError(f'published mutable source changed during capture: {logical}')
                continue
            rename_without_overwrite(temporary, target)
            _fsync_directory(target.parent)
            return {'path': str(target), 'sha256': digest.hexdigest(), 'bytes': copied,
                    'capture_kind': capture_kind}
        finally:
            temporary.unlink(missing_ok=True)  # This call's exact owned work file, never a source.
    raise AssertionError('bounded published capture did not return or reject')


def _freeze_controls(source: Path, staging: Path, excluded: list[str], *, existing=None,
                     progress=None, upgrade_from=None) -> dict:
    """Freeze bounded controls and known atomically published mutable outputs.

    Unknown in-place writers are NOT silently accepted: the canonical source
    fences still reject their selected payloads. These common metadata files
    are copied before the raw cohort census so always-on status writers do not
    invalidate a multi-hour baseline. Known closed catalog/tail/summary outputs
    get the same fixed-observation treatment, not a fabricated atomic workspace.
    """
    staging.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    intent_path = staging.with_suffix(".intent.json")
    intent = {"contract": CONTROL_SCOPE, "source": str(source), "excluded": sorted(excluded)}
    if intent_path.exists():
        previous_intent = json.loads(intent_path.read_bytes())
        if intent_path.is_symlink() or previous_intent not in tuple(
                {**intent, "contract": version} for version in
                (CONTROL_SCOPE, "recursive_mutable_controls_v2", "recursive_mutable_controls_v3",
                 "recursive_mutable_controls_v4", "recursive_mutable_controls_v5",
                 "recursive_mutable_controls_v6", "recursive_mutable_controls_v7",
                 "recursive_mutable_controls_v8", "recursive_mutable_controls_v9",
                 "recursive_mutable_controls_v10", "recursive_mutable_controls_v11",
                 "recursive_mutable_controls_v12", "recursive_mutable_controls_v13",
                 "recursive_mutable_controls_v14", "recursive_mutable_controls_v15",
                 "recursive_mutable_controls_v16")):
            raise SnapshotError("control freeze intent changed")
        if previous_intent != intent:
            # Expand the named control scope without replacing retained
            # captures or repeating a native SQLite backup.
            atomic_write_json(intent_path, intent)
    else:
        if staging.exists():
            raise SnapshotError("control freeze staging has no owned intent")
        atomic_write_json(intent_path, intent)
    staging.mkdir(mode=0o700, parents=True, exist_ok=True)
    if (staging.resolve() != staging or staging.stat().st_uid != os.geteuid()
            or stat.S_IMODE(staging.stat().st_mode) != 0o700):
        raise SnapshotError("control freeze staging is not private")
    frozen = dict(existing or {})
    examined = 0
    captured_bytes = sum(row["bytes"] for row in frozen.values())
    last_progress = time.monotonic()
    def report():
        if progress is not None:
            progress({"stage": "freezing_mutable_controls", "acceptance": False,
                      "files_completed": 0, "files_total": None,
                      "logical_bytes_processed": 0, "logical_bytes_total": None,
                      "files_examined": examined, "controls_frozen": len(frozen),
                      "control_bytes_frozen": captured_bytes})
    report()
    control_names = {"state.json", "manifest.json", "checkpoint.json", "cursor.json", "current.json",
                     "status.json", "progress.json", "history.json", "active_run.json", ".sync.lock"}
    published_locations = _mutable_published_locations(source)
    control_file_locations = _mutable_control_file_locations(source)
    streamed_control_locations = _mutable_streamed_control_locations(source)
    json_control_locations = _mutable_json_control_locations(source)
    def walk_error(error):
        raise error
    if upgrade_from in {'recursive_mutable_controls_v3', 'recursive_mutable_controls_v4',
                        'recursive_mutable_controls_v5', 'recursive_mutable_controls_v6',
                        'recursive_mutable_controls_v7', 'recursive_mutable_controls_v8',
                        'recursive_mutable_controls_v9', 'recursive_mutable_controls_v10',
                        'recursive_mutable_controls_v11', 'recursive_mutable_controls_v12',
                        'recursive_mutable_controls_v13', 'recursive_mutable_controls_v14',
                        'recursive_mutable_controls_v15', 'recursive_mutable_controls_v16', CONTROL_SCOPE}:
        # Completed recursive censuses only need named delta probes. v4 added
        # reports/atomic outputs; v5 added rule-only TAIFEX, v6 news controls.
        # v7 adds native CEX reports/lock epochs and the official feature table;
        # v8 repeats ONLY these named probes on retry; v9 adds the canonical
        # public catalog metadata/version journal directory; v10 adds exact
        # FinMind batch summaries and OpenBB public-priority queue controls;
        # v11 adds direct public query journals and OpenBB compactor heads.
        # v12 adds the FinLab run head and exact public feature companions.
        # v13 completes reviewed FinLab discovery/OpenBB operational producers.
        # v14 adds the FinLab intraday frontiers/backoff/batch summary, not the
        # immutable dated batch receipts or another SQLite snapshot.
        # v15 adds only the FinMind announcement watcher's two mutable heads.
        # v16 adds the FinLab overlay's exact token/year closed outputs.
        # v17 adds only the two reviewed mutable null-audit JSON reports.
        # Large receipt
        # trees are observed directly into cold packs, never millions of copies.
        # Never rewalk millions of raw files or replace retained observations.
        locations = [source / name for name in control_file_locations]
        locations.extend(source / name for name in published_locations)
        locations.extend(source / name for name in streamed_control_locations)
        locations.extend(source / name for name in sorted(json_control_locations))
        def delta_walks():
            for directory in dict.fromkeys(locations):
                location = directory.relative_to(source).as_posix()
                if (directory.resolve() != directory or not directory.is_dir()
                        or any(location == name or location.startswith(name + '/') for name in excluded)):
                    continue
                files = {name for name in control_file_locations.get(location, ())
                         if (directory / name).exists()}
                files.update(name for name in streamed_control_locations.get(location, ())
                             if (directory / name).exists())
                if location in published_locations:
                    named = published_locations[location]
                    files.update((member.name for member in directory.iterdir() if member.name.endswith('.parquet'))
                                 if named is None else (name for name in named if (directory / name).exists()))
                if location in json_control_locations:
                    files.update(member.name for member in directory.iterdir() if member.name.endswith('.json'))
                yield directory, [], sorted(files)
        walks = delta_walks()
    else:
        walks = os.walk(source, followlinks=False, onerror=walk_error)
    for directory, names, files in walks:
        relative = Path(directory).relative_to(source)
        names[:] = sorted(name for name in names
                          if (relative / name).as_posix() not in excluded)
        for name in sorted(files):
            examined += 1
            if (progress is not None and examined % 1024 == 0
                    and time.monotonic() - last_progress >= 5):
                last_progress = time.monotonic()
                report()
            # Classify names BEFORE constructing/stating millions of immutable
            # payload paths. Only candidate controls need a file-level stat.
            json_control = name.lower().endswith(".json")
            location = relative.as_posix()
            named_file_control = name in control_file_locations.get(location, ())
            streamed_control = name in streamed_control_locations.get(location, ())
            json_lines = (source.name == 'data_tw_public' and (
                (location == 'metadata' and name.endswith('_versions.jsonl'))
                or (location == 'state/journals' and name.endswith('.jsonl'))))
            published_payload = (location in published_locations and (
                name in published_locations[location] if published_locations[location] is not None
                else name.endswith('.parquet')))
            declared_json_control = json_control and location in json_control_locations
            if not (published_payload or streamed_control or named_file_control or declared_json_control) and (
                    (not json_control and name != ".sync.lock")
                    or (relative.parts and "latest" not in relative.parts and name not in control_names
                        and not name.endswith(("_status.json", "_progress.json", "_state.json")))):
                continue
            logical = (relative / name).as_posix()
            if logical in frozen or logical in excluded:
                continue
            original = Path(directory) / name
            before = original.lstat()
            if not stat.S_ISREG(before.st_mode) or (not (published_payload or streamed_control)
                                                   and before.st_size > 64 * 1024**2):
                if published_payload or streamed_control:
                    raise SnapshotError(f'published mutable payload exceeds its bounded capture: {logical}')
                continue
            target = staging / logical
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if target.resolve() != target or target.is_symlink():
                raise SnapshotError("control freeze target is redirected")
            if published_payload or streamed_control:
                streamed_jsonl = streamed_control and name.endswith('.jsonl')
                frozen[logical] = _freeze_streamed_observation(original, target, logical,
                    capture_kind=('stable_operational_jsonl' if streamed_jsonl else 'stable_operational_log')
                        if streamed_control else 'closed_atomic_published_payload',
                    json_lines=streamed_jsonl)
                captured_bytes += frozen[logical]['bytes']
                continue
            if target.exists():
                # A crash can leave an atomically installed complete capture.
                # Reuse that exact private observation, not today's new value.
                raw = target.read_bytes()
                if len(raw) > 64 * 1024**2:
                    raise SnapshotError("retained control capture is oversized")
                _validate_frozen_control(raw, json_control=json_control, json_lines=json_lines)
            else:
                for attempt in range(3):
                    descriptor = os.open(original, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
                    try:
                        src = os.fdopen(descriptor, "rb")
                    except BaseException:
                        os.close(descriptor)
                        raise
                    with src:
                        first = os.fstat(src.fileno())
                        if not stat.S_ISREG(first.st_mode) or first.st_size > 64 * 1024**2:
                            raise SnapshotError("control source exceeded its bounded regular-file scope")
                        raw = src.read(first.st_size + 1)
                        last = os.fstat(src.fileno())
                    # Atomic source replacement is harmless to the held FD.
                    # In-place writes still fail the size/mtime/content fences.
                    signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
                    if signature(first) == signature(last) and len(raw) == first.st_size:
                        _validate_frozen_control(raw, json_control=json_control, json_lines=json_lines)
                        break
                    if attempt == 2:
                        raise SnapshotError(f"control file changed during bounded freeze: {logical}")
                atomic_write_bytes(target, raw, mode=0o600, durable=True)
            frozen[logical] = {"path": str(target), "sha256": hashlib.sha256(raw).hexdigest(),
                               "bytes": len(raw)}
            captured_bytes += len(raw)
    report()
    return frozen


def _preparation_path(args, dataset):
    return args.migration_state.absolute() / "preparations" / (dataset + ".json")


def _private_preparation(entry: dict, args, source: Path, cold: Path, policy: dict) -> dict:
    """Persist SQLite bindings/control copies before attempting the main head.

    A busy cold writer or process restart must not copy a 10 GB database again.
    Retain the original prepared evidence until its exact recovery has passed.
    """
    path = _preparation_path(args, entry["dataset"])
    if path.exists():
        prepared = json.loads(path.read_text())
        if (prepared.get("contract") != CONTRACT or prepared.get("source") != str(source)
                or prepared.get("dataset") != entry["dataset"]):
            raise SnapshotError("private cohort preparation identity changed")
    else:
        prepared = {"contract": CONTRACT, "source": str(source), "dataset": entry["dataset"],
                    "created_at_utc": _now(), "sqlite_states": [], "state": "preparing"}
        atomic_write_json(path, prepared)
    excluded = list(entry.get("excluded_subtrees", []))
    known = {row["source_relative"] for row in prepared["sqlite_states"]}
    if prepared["state"] == "preparing":
        for database in _sqlite_databases(source, excluded,
                                         progress=getattr(args, "publication_progress", None)):
            relative = database.relative_to(source).as_posix()
            if relative in known:
                continue
            name = str(entry["dataset"]) + "-state-" + hashlib.sha256(relative.encode()).hexdigest()[:12]
            stage = args.migration_state.absolute() / "sqlite-snapshots" / ("sqlite-" + uuid.uuid4().hex)
            # Persist the exact native backup target BEFORE copying a byte.
            # Restarting an interrupted attempt reuses this SQLite destination
            # via its own backup API, never a growing pile of orphan 10 GB files.
            prepared["sqlite_states"].append({"source_relative": relative, "dataset": name,
                "retained_snapshot_staging": str(stage), "native_backup_state": "pending"})
            atomic_write_json(path, prepared)
        for row in prepared["sqlite_states"]:
            if "consistent_state" not in row:
                row["consistent_state"] = _snapshot_sqlite(source / row["source_relative"],
                    Path(row["retained_snapshot_staging"]), row["source_relative"])
                row["native_backup_state"] = "snapshot_ready"
                atomic_write_json(path, prepared)
            if "snapshot_id" in row:
                continue
            saved = publish_packed_snapshot(cold, row["dataset"], Path(row["retained_snapshot_staging"]),
                node_id=args.node_id, pack_buckets=1, file_chunk_bytes=policy["file_chunk_bytes"],
                batch_directory_fsync=True, publication_lock_nonblocking=True,
                metadata={"role": "private_consistent_sqlite_state", "source_relative": row["source_relative"],
                          "shared_export": "false", "nas_export": "false"})
            row.update(snapshot_id=saved.manifest["snapshot_id"], manifest_sha256=saved.manifest_sha256)
            atomic_write_json(path, prepared)
        prepared.update(state="prepared", prepared_at_utc=_now())
        atomic_write_json(path, prepared)
    # Repeat bounded named control probes, not the initial raw/database census.
    # Keep old captures; only controls first appearing since preparation append.
    if prepared.get("state") == "prepared":
        for row in prepared["sqlite_states"]:
            relative = row["source_relative"]
            excluded.extend((relative, relative + "-wal", relative + "-shm", relative + "-journal"))
        if "controls_staging" not in prepared:
            prepared["controls_staging"] = str(args.migration_state.absolute() / "controls"
                                               / ("controls-" + uuid.uuid4().hex))
            atomic_write_json(path, prepared)
        prepared["controls"] = _freeze_controls(source, Path(prepared["controls_staging"]), excluded,
                                               existing=prepared.get("controls", {}),
                                               progress=getattr(args, "publication_progress", None),
                                               upgrade_from=prepared.get("control_scope"))
        prepared["control_scope"] = CONTROL_SCOPE
        atomic_write_json(path, prepared)
    for row in prepared["sqlite_states"]:
        selected = resolve_packed_snapshot_id(cold, row["dataset"], row["snapshot_id"])
        if selected.manifest_sha256 != row["manifest_sha256"]:
            raise SnapshotError("prepared SQLite release identity changed")
    for row in prepared["controls"].values():
        original = Path(row["path"])
        if original.is_symlink() or original.stat().st_size != row["bytes"] or sha256_file(original) != row["sha256"]:
            raise SnapshotError("prepared private control bytes changed")
    return prepared


def _private_publish(repo: Path, entry: dict, args, parent: Path, policy: dict, catalog: list[dict]):
    from scripts.publish_data_releases import (
        _running_commands, _source_coordination_lock, _source_path, _status, _loose_threshold_bytes,
    )
    cold = private_history_root(parent, policy)
    initialize_packed_layout(cold, node_id=args.node_id)
    # Native news publication is an atomic completed-version boundary. Raw
    # observation preservation is read-only and uses file/SQLite fences instead
    # of waiting forever for an always-on downloader to stop.
    native_news = entry["dataset"] == "news-training-local"
    with (_source_coordination_lock(entry, wait_seconds=0) if native_news else nullcontext()), \
            MetadataRows(dict, dict) as bootstrap_index:
        status = _status(entry, _running_commands(), sync_root=cold)
        if not status["source_exists"] or (native_news and status["active_blockers"]):
            return None, {"state": "waiting_source_owner", "source_status": status}
        source = _source_path(entry).resolve()
        from stockagent.data_sync.private_source_links import (
            guard_source_links, prepare_source_links, source_link_metadata,
        )
        source_links = prepare_source_links(repo, entry, source, cold, args.migration_state.absolute(),
                                           progress=getattr(args, "publication_progress", None))
        inherited_files, inherited_objects, inherited_proofs = _bootstrap_public_storage(
            repo, entry, parent, cold, catalog, file_sink=bootstrap_index,
            progress=getattr(args, "publication_progress", None))
        excluded = list(entry.get("excluded_subtrees", []))
        excluded.extend(binding["name"] for binding in source_links)
        prepared = _private_preparation(entry, args, source, cold, policy)
        # Mutable operational controls may be frozen, but the new readonly
        # Parquet head and immutable members stay under the packed publisher's
        # whole-tree before/after fences. Do not resurrect a retained legacy
        # override after a format cutover.
        from stockagent.data_sync.parquet_publication import ParquetPublicationSnapshot
        parquet_proof = ParquetPublicationSnapshot(source, excluded=excluded,
            dataset=entry["dataset"],
            file_overrides=prepared["controls"])
        snapshots = prepared["sqlite_states"]
        for row in snapshots:
            relative = row["source_relative"]
            excluded.extend((relative, relative + "-wal", relative + "-shm", relative + "-journal"))
        bindings = [{key: value for key, value in row.items()
                     if key in {"source_relative", "dataset", "snapshot_id", "manifest_sha256"}}
                    for row in snapshots]
        staging_rules = ["artifact_io_atomic_sibling_v1"]
        if entry["dataset"] == "finmind-free" and source == (repo / "data_finmind").resolve():
            staging_rules.append("finmind_parquet_promote_v1")
        if source == (repo / 'data_finlab').resolve():
            staging_rules.append('finlab_atomic_sibling_v1')
            staging_rules.append('finlab_research_atomic_sibling_v1')
        if source == (repo / 'data_openBB').resolve():
            staging_rules.append('openbb_l1_atomic_sibling_v1')
            staging_rules.append('openbb_operational_atomic_sibling_v1')
        from stockagent.data_sync.packing_checkpoint import SealedPackHints
        hints = SealedPackHints(args.migration_state.absolute() / 'sealed-pack-checkpoints' / str(entry['dataset']),
                               cold, source, str(entry['dataset']))
        def source_guard():
            parquet_proof.guard()
            if source_links:
                guard_source_links(source, source_links)

        saved = publish_packed_snapshot(cold, str(entry["dataset"]), source, node_id=args.node_id,
            loose_file_threshold_bytes=(_loose_threshold_bytes(entry)
                                       if entry.get("loose_threshold_bytes") or entry.get("loose_threshold_mib") else 8 * 1024**2),
            pack_buckets=int(entry["pack_buckets"]) or 64,
            file_chunk_bytes=policy["file_chunk_bytes"], excluded_subtrees=excluded,
            batch_directory_fsync=True, publication_lock_nonblocking=True,
            d_primary_native_chunk_reuse=(cold == Path('/srv/stockagent-packed/.local-state/private-history')),
            d_primary_native_chunk_reuse_pipeline=(cold == Path('/srv/stockagent-packed/.local-state/private-history')),
            bootstrap_file_storage=inherited_files, bootstrap_objects=inherited_objects,
            private_observation_cohort=True,
            private_atomic_json_controls=source.name in {
                'data_finlab', 'data_finmind', 'data_crypto_reference', 'data_tej'},
            private_staging_file_rules=staging_rules,
            sealed_pack_hints=hints,
            file_overrides={name: Path(row["path"]) for name, row in prepared["controls"].items()},
            **({"source_guard": source_guard} if source_links or parquet_proof.applicable else {}),
            progress=getattr(args, "publication_progress", None),
            metadata={"role": str(entry["role"]), "shared_export": "false", "nas_export": "false",
                      **parquet_proof.metadata,
                      "consistent_sqlite_bindings": json.dumps(bindings, sort_keys=True),
                      "version_dependencies_sha256": _identity(bindings if not source_links else
                          {"sqlite_states": bindings, "source_links": source_links}),
                      **source_link_metadata(source_links),
                      "public_bootstrap_releases": json.dumps(inherited_proofs, sort_keys=True)})
        # The compound source keeps exact link provenance and immutable target
        # dependencies, not unsafe absolute links in a portable file inventory.
        control_evidence = ({'frozen_control_files': prepared['controls']} if len(prepared['controls']) <= FROZEN_CONTROLS_INLINE_LIMIT else
            {'frozen_controls_receipt': {'path': str(_preparation_path(args, entry['dataset'])),
                'sha256': sha256_file(_preparation_path(args, entry['dataset'])),
                'files': len(prepared['controls'])}})
        return saved, {"state": "published", "sqlite_states": snapshots, "shared_export": False,
                       "source_status": status, "consistency": "fixed_observed_files_not_atomic_workspace",
                       **control_evidence,
                       "sealed_pack_hint_files": list(hints.receipts.values()),
                       "sealed_pack_hints": dict(hints.statistics),
                       "source_links": source_links,
                       "nas_export": False, "source_files_deleted": 0}


def _recover(cold: Path, dataset: str, snapshot: str, receipt: Path, progress_path: Path,
             *, attempt_id: str | None = None) -> dict:
    retained = receipt.exists()
    # A new full verifier performs its own complete presence/size pass. Do not
    # stat every cold object once in each resolver and then a third time in
    # verification. Reusing a completed proof STILL checks current presence.
    selected = resolve_packed_snapshot_id(cold, dataset, snapshot, require_objects=retained)
    if retained:
        proof = json.loads(receipt.read_text())
        if (proof.get("manifest_sha256") != selected.manifest_sha256
                or proof.get("full_original_file_reconstruction_verified") is not True):
            raise SnapshotError("previous recovery receipt does not match the fixed migration release")
        return proof
    last = 0.0
    def progress(document):
        nonlocal last
        if time.monotonic() - last >= 5:
            last = time.monotonic()
            atomic_write_json(progress_path, {**document, "updated_at_utc": _now(),
                              "snapshot_id": snapshot, "attempt_id": attempt_id,
                              "acceptance": False}, durable=False)
    started = time.monotonic()
    primary = Path('/srv/stockagent-packed')
    native_pack_reads = cold in (primary, primary / '.local-state/private-history')
    proof = verify_packed_snapshot(cold, selected, reconstruct_all=True, progress=progress,
                                  d_primary_native_pack_reads=native_pack_reads,
                                  d_primary_native_chunk_reads=(cold in {
                                      primary, primary / '.local-state/private-history'}))
    proof.update(contract=CONTRACT, dataset=dataset, snapshot_id=snapshot,
                 manifest_sha256=selected.manifest_sha256,
                 full_original_file_reconstruction_verified=True, source_files=int(selected.manifest["source"]["files"]),
                 proof_scope="fixed_D_release_not_NAS_or_source_retirement", verified_at_utc=_now(),
                 complete_wall_seconds=time.monotonic() - started)
    atomic_write_json(receipt, proof)
    return proof


def _retire_owned_scratch(directory: Path, dataset: str, publication: dict, *, progress=None) -> dict:
    """Unlink only our reproducible freeze copies after all cold proofs passed.

    This never targets a data_* source, authoritative object, NAS file or an
    unknown staging member. An interrupted unlink is reconciled conservatively;
    only measured last-link releases are counted as reclaimed allocated bytes.
    """
    from stockagent.data_sync.materialized_cache import process_references_many
    receipt = directory / "scratch-retirement" / (dataset + ".json")
    previous = json.loads(receipt.read_text()) if receipt.exists() else {}
    controls = publication.get('frozen_control_files', {})
    if 'frozen_controls_receipt' in publication:
        binding = publication['frozen_controls_receipt']
        path = directory / 'preparations' / (dataset + '.json')
        if (binding['path'] != str(path) or path.is_symlink() or path.resolve() != path
                or sha256_file(path) != binding['sha256']):
            raise SnapshotError('frozen control receipt no longer matches its exact preparation')
        prepared = json.loads(path.read_bytes())
        if prepared['dataset'] != dataset or len(prepared['controls']) != binding['files']:
            raise SnapshotError('frozen control receipt dataset/count changed')
        controls = prepared['controls']
    targets = {row["path"]: row["sha256"] for row in controls.values()}
    for row in publication.get("sqlite_states", []):
        targets[str(Path(row["retained_snapshot_staging"]) / "snapshot.sqlite3")] = row["consistent_state"]["snapshot_sha256"]
    hints_root = directory / 'sealed-pack-checkpoints' / dataset
    for row in publication.get('sealed_pack_hint_files', []):
        if Path(row['path']).parent != hints_root:
            raise SnapshotError('sealed-pack hint retirement differs from its original operation')
        targets[row['path']] = row['sha256']
    result = {"contract": "owned_migration_scratch_retirement_v1", "dataset": dataset,
              "source_files_deleted": 0, "cold_objects_deleted": 0, "nas_files_deleted": 0,
              "files": previous.get("files", {})}
    permitted = (directory / "controls", directory / "sqlite-snapshots", hints_root)
    pending = []
    last_report = 0.0
    def report(stage, completed=0, *, force=False):
        nonlocal last_report
        if progress is not None and (force or time.monotonic() - last_report >= 5):
            last_report = time.monotonic()
            progress({'stage': stage, 'completed_items': completed, 'total_items': len(targets),
                      'completed_bytes': None, 'total_bytes': None, 'acceptance': False,
                      'source_files_deleted': 0, 'cold_objects_deleted': 0, 'nas_files_deleted': 0})
    report('verify-owned-preparation-scratch', force=True)
    signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
                              info.st_ctime_ns, info.st_nlink)
    for index, (name, expected_sha) in enumerate(targets.items()):
        if index % 128 == 0:
            report('verify-owned-preparation-scratch', index)
        path = Path(name)
        if path.resolve() != path or not any(path.is_relative_to(root) for root in permitted):
            raise SnapshotError("owned scratch retirement target escapes its operation")
        if result["files"].get(name, {}).get("state") in {"unlinked", "already_absent"}:
            if path.exists():
                raise SnapshotError("retired owned scratch name was unexpectedly recreated")
            continue
        if not path.exists():
            result["files"][name] = {"state": "already_absent", "allocated_bytes_reclaimed": 0}
            continue
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or sha256_file(path) != expected_sha:
            raise SnapshotError("owned scratch identity/link/hash changed; preserve it")
        if signature(before) != signature(path.lstat()):
            raise SnapshotError("owned scratch changed during final retirement check")
        pending.append((path, expected_sha, before))
    # One fresh /proc observation and one durable intent for this owned batch,
    # not a complete process scan plus an O(N) journal rewrite per small hint.
    # A positive reference conservatively holds the entire batch. The original
    # operation lock and per-file final identity fences remain mandatory.
    report('check-owned-scratch-process-references', len(targets), force=True)
    references = process_references_many([path for path, _, _ in pending]) if pending else []
    for path, expected_sha, _ in pending:
        result["files"][str(path)] = {"state": "waiting_process_reference" if references else "unlink_planned",
            "sha256": expected_sha, "allocated_bytes_reclaimed": 0,
            **({"references": references} if references else {})}
    if pending:
        atomic_write_json(receipt, result)
    changed_directories = set()
    report('unlink-owned-preparation-scratch', len(targets) - len(pending), force=True)
    try:
        if not references:
            for index, (path, _, before) in enumerate(pending):
                if index % 128 == 0:
                    report('unlink-owned-preparation-scratch', len(targets) - len(pending) + index)
                if path.resolve() != path or signature(before) != signature(path.lstat()):
                    raise SnapshotError("owned scratch changed during final retirement check")
                path.unlink()
                changed_directories.add(path.parent)
                result["files"][str(path)].update(state="unlinked",
                    allocated_bytes_reclaimed=before.st_blocks * 512)
    finally:
        for parent in changed_directories:
            descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    result.update(state="retired" if all(row["state"] in {"unlinked", "already_absent"}
                                         for row in result["files"].values()) else "waiting",
                  allocated_bytes_reclaimed=sum(row["allocated_bytes_reclaimed"] for row in result["files"].values()),
                  observed_at_utc=_now())
    atomic_write_json(receipt, result)
    report('owned-preparation-scratch-' + result['state'], len(targets) if result['state'] == 'retired'
           else len(targets) - len(pending), force=True)
    return result


def _committed_catalog_release(entry: dict, args, cold: Path):
    """Pin an already admitted v2 release; never republish stale/live source bytes.

    Migration proves recovery of a fixed historical version, not perpetual
    freshness of its source workspace. The canonical publisher's exact entry
    identity binds scope, rights and admission gates at commit. Legacy/unbound
    manifests still go through the original live publisher. In either case all
    objects and original files must pass _recover before migration acceptance.
    """
    from scripts.publish_data_releases import (
        CATALOG_PUBLICATION_CONTRACT, _source_path, _version_options,
        catalog_publication_identity,
    )
    if not entry.get('publish') or 'publication_family' in entry or 'completed_forward_key' in entry:
        return None
    # Missing heads are normal before first publication. Do not hide a corrupt
    # or redirected existing head behind a fallback to an older/live source.
    heads = cold / 'heads' / entry['dataset']
    if not heads.exists() and not heads.is_symlink():
        return None
    saved = resolve_latest_packed(cold, entry['dataset'], require_objects=False)
    manifest, expected = saved.manifest, catalog_publication_identity(entry)
    metadata = manifest.get('metadata', {})
    if (manifest['schema_version'] != 2
            or metadata.get('catalog_publication_contract') != CATALOG_PUBLICATION_CONTRACT
            or metadata.get('catalog_entry_sha256') != expected):
        return None
    source, archive = manifest['source'], manifest['archive']
    options = _version_options(entry, args)
    from stockagent.data_sync.packed_snapshots import (
        _normalize_excluded_subtrees, _normalize_included_root_files,
    )
    excluded = list(_normalize_excluded_subtrees(entry.get('excluded_subtrees', [])))
    included = (list(_normalize_included_root_files(entry['included_root_files']))
                if 'included_root_files' in entry else None)
    if (metadata.get('role') != str(entry['role']) or metadata.get('catalog_schema') != '1'
            or source['snapshot_root_name'] != _source_path(entry).name
            or source.get('excluded_subtrees', []) != excluded
            or source.get('selection', {}).get('included_root_files') != included
            or source.get('selection', {}).get('private_observation_cohort', False)
            or source.get('omitted_files_above_maximum', 0) != 0
            or archive.get('file_chunk_bytes') != options.get('file_chunk_bytes')):
        raise SnapshotError('committed catalog publication disagrees with its enrolled contract')
    return saved


def migrate_catalog(repo: Path, args, catalog: list[dict]) -> dict:
    """One resumable operation; all concrete catalog sources, not just repo links."""
    from scripts.publish_data_releases import _publish_entry, _source_path
    from stockagent.data_sync.dataset_version_layout import version_policy
    if not 1 <= args.retry_seconds <= 300 or not 1 <= args.maximum_passes <= 100000:
        raise SnapshotError("migration retry/pass bounds are invalid")
    budget = getattr(args, "maximum_completed_sources", None)
    if budget is not None and (type(budget) is not int or budget < 1):
        raise SnapshotError("migration completion budget must be positive")
    attempt_budget = getattr(args, "maximum_attempted_sources", None)
    if attempt_budget is not None and (type(attempt_budget) is not int or attempt_budget < 1):
        raise SnapshotError("migration attempt budget must be positive")
    parent, directory = args.sync_root.absolute(), args.migration_state.absolute()
    _check_packed_read_authority(parent)
    if (directory.is_relative_to(parent) or any(directory.is_relative_to(_source_path(e).resolve())
                                               for e in catalog)):
        raise SnapshotError("migration journal must remain outside cold and every source")
    policy = version_policy(repo)
    entries = migration_entries(repo, catalog, policy)
    identity = _identity({"catalog": entries, "policy": policy})
    run_args = SimpleNamespace(**vars(args))
    run_args.all_ready = True
    run_args.reader_contract = "packed-v2-chunks"
    run_args.defer_scan = (parent / ".stockagent-d-primary").is_file()
    run_args.publication_lock_nonblocking = True
    run_args.batch_directory_fsync = True
    state_path = directory / "progress.json"
    with _operation_owner(directory):
        if state_path.exists():
            result = json.loads(state_path.read_text())
            if result.get("contract") != CONTRACT or result.get("plan_sha256") != identity:
                raise SnapshotError("migration source/policy changed; retain old journal and use a new operation")
        else:
            result = {"contract": CONTRACT, "plan_sha256": identity, "state": "processing",
                "created_at_utc": _now(), "source_count": len(entries), "source_retirement": False,
                "nas_acceptance": "original_lifecycle_owner_not_this_operation",
                "datasets": {entry["dataset"]: {"state": "pending", "attempts": 0,
                    "domain": "shared" if entry["publish"] else "host_private",
                    "source": str(_source_path(entry))} for entry in entries}}
            atomic_write_json(directory / "plan.json", {"contract": CONTRACT, "plan_sha256": identity,
                "catalog": entries, "policy": policy, "source_retirement": False})
        # This exact operation's exclusive lock proves no previous activity is
        # still writing here. Preserve its attempt/proof and fixed snapshot;
        # do not leave a crashed process permanently displayed as processing.
        for dataset, row in result['datasets'].items():
            if row['state'] in ('processing', 'recovery'):
                interruption = {**{k: v for k, v in row.items() if k != 'last_interruption'},
                    'detected_at_utc': _now(),
                    'reason': 'previous_activity_released_exclusive_operation_owner',
                    'source_retirement': False}
                receipt = directory / 'interruptions' / dataset / (uuid.uuid4().hex + '.json')
                atomic_write_json(receipt, interruption)
                row.update(state='waiting_retry', retry_at_unix=0,
                    last_interruption={**{k: interruption[k] for k in
                        ('state', 'attempt_id', 'started_at_utc', 'snapshot_id', 'detected_at_utc', 'reason')
                        if k in interruption}, 'receipt': str(receipt)})
        activity = {**migration_process_identity(), 'activity_id': uuid.uuid4().hex,
                    'started_at_utc': _now(), 'active_dataset': None, 'active_attempt_id': None}
        result['runtime_owner'] = activity
        def save():
            states = [row["state"] for row in result["datasets"].values()]
            result.update(updated_at_utc=_now(), verified_datasets=states.count("verified"),
                          state="verified" if all(s == "verified" for s in states) else "processing")
            atomic_write_json(state_path, result)
        save()
        completed_this_call = 0
        attempted_this_call = 0
        for pass_number in range(args.maximum_passes):
            # Resume committed releases first, then untouched work. A repeatedly
            # changing multi-million-file source must not starve every pending
            # branch on each durable activity. Among retries, oldest attempt
            # goes first: historical failure counts are not waiting time.
            ordered = sorted(entries, key=lambda entry: (
                0 if "snapshot_id" in result["datasets"][entry["dataset"]] else
                1 if result["datasets"][entry["dataset"]]["state"] == "pending" else 2,
                result["datasets"][entry["dataset"]].get("started_at_utc", ""),
                result["datasets"][entry["dataset"]].get("attempts", 0)))
            for entry in ordered:
                row = result["datasets"][entry["dataset"]]
                if row["state"] == "verified":
                    continue
                if row.get("retry_at_unix", 0) > time.time():
                    continue
                if attempt_budget is not None and attempted_this_call >= attempt_budget:
                    return result
                attempted_this_call += 1
                if row.get("error"):
                    row["last_failure"] = {key: row[key] for key in
                        ("error", "error_type", "attempts", "attempt_id", "started_at_utc") if key in row}
                for key in ("error", "error_type", "retry_at_unix"):
                    row.pop(key, None)
                row.update(state="processing", attempts=row["attempts"] + 1,
                           attempt_id=uuid.uuid4().hex, started_at_utc=_now(),
                           owner_activity_id=activity['activity_id'])
                activity.update(active_dataset=entry['dataset'], active_attempt_id=row['attempt_id'])
                save()
                try:
                    progress_path = directory / "publication" / (entry["dataset"] + ".json")
                    run_args.publication_progress = lambda progress: atomic_write_json(progress_path,
                        {**progress, "dataset": entry["dataset"], "attempt_id": row["attempt_id"],
                         "observed_at_utc": _now()}, durable=False)
                    run_args.publication_progress({"stage": "preparing_source", "acceptance": False,
                        "files_completed": 0, "files_total": None,
                        "logical_bytes_processed": 0, "logical_bytes_total": None})
                    cold = parent if entry["publish"] else private_history_root(parent, policy)
                    # An interrupted publish might have committed. Keep its exact
                    # selected binding; never infer a failed/missing receipt meant no write.
                    if "snapshot_id" in row:
                        saved = resolve_packed_snapshot_id(cold, entry["dataset"], row["snapshot_id"],
                                                          require_objects=False)
                        extra = row.get("publication", {})
                    elif entry["publish"]:
                        saved = _committed_catalog_release(entry, run_args, parent)
                        if saved is None:
                            status, saved = _publish_entry(entry, run_args, parent)
                            extra = {"source_status": status}
                        else:
                            extra = {"state": "fixed_committed_catalog_release",
                                "catalog_entry_sha256": saved.manifest['metadata']['catalog_entry_sha256'],
                                "published_at": saved.manifest['published_at'],
                                "current_source_freshness_checked": False,
                                "all_current_source_history_complete_claim": False}
                    else:
                        saved, extra = _private_publish(repo, entry, run_args, parent, policy, catalog)
                    if saved is None:
                        row.update(state="waiting_source_owner", publication=extra,
                                   retry_at_unix=time.time() + args.retry_seconds)
                        save()
                        continue
                    if saved.manifest["schema_version"] != 2:
                        raise SnapshotError("source did not commit the required v2 reader contract")
                    row.update(state="recovery", snapshot_id=saved.manifest["snapshot_id"],
                        manifest_sha256=saved.manifest_sha256, publication=extra,
                        source_files=saved.manifest["source"]["files"],
                        logical_bytes=saved.manifest["source"]["logical_bytes"],
                        new_payload_bytes=saved.manifest["archive"].get("new_stored_bytes"))
                    save()
                    proof = directory / "recovery" / (entry["dataset"] + ".json")
                    _recover(cold, entry["dataset"], row["snapshot_id"], proof,
                             proof.with_suffix(".progress.json"), attempt_id=row["attempt_id"])
                    from stockagent.data_sync.private_source_links import manifest_source_links, source_link_dependencies
                    source_links = manifest_source_links(saved)
                    for link in source_link_dependencies(source_links):
                        dependency = resolve_packed_snapshot_id(cold, link["dataset"], link["snapshot_id"],
                                                               require_objects=False)
                        if dependency.manifest_sha256 != link["manifest_sha256"]:
                            raise SnapshotError("fixed source link dependency identity changed")
                        linkproof = directory / "recovery" / (link["dataset"] + ".json")
                        _recover(cold, link["dataset"], link["snapshot_id"], linkproof,
                                 linkproof.with_suffix(".progress.json"), attempt_id=row["attempt_id"])
                    if source_links:
                        link_receipt = directory / "recovery" / (entry["dataset"] + ".source-links.json")
                        atomic_write_json(link_receipt, {"contract": CONTRACT,
                            "snapshot_id": row["snapshot_id"], "manifest_sha256": saved.manifest_sha256,
                            "bindings": source_links, "all_fixed_dependencies_recovered": True,
                            "unsafe_absolute_links_materialized": False,
                            "source_files_deleted": 0, "verified_at_utc": _now()})
                        row["source_links_recovery_receipt"] = str(link_receipt)
                    for database in extra.get("sqlite_states", []):
                        dbproof = directory / "recovery" / (database["dataset"] + ".json")
                        _recover(cold, database["dataset"], database["snapshot_id"], dbproof,
                                 dbproof.with_suffix(".progress.json"), attempt_id=row["attempt_id"])
                    if getattr(args, "retire_staging", False) and not entry["publish"]:
                        def scratch_progress(document):
                            atomic_write_json(proof.with_suffix('.progress.json'),
                                {**document, 'updated_at_utc': _now(), 'snapshot_id': row['snapshot_id'],
                                 'attempt_id': row['attempt_id']}, durable=False)
                        scratch = _retire_owned_scratch(directory, entry["dataset"], extra,
                                                       progress=scratch_progress)
                        row['owned_scratch_retirement'] = {k: v for k, v in scratch.items() if k != 'files'}
                        row['owned_scratch_retirement'].update(files_checked=len(scratch['files']),
                            receipt=str(directory / 'scratch-retirement' / (entry['dataset'] + '.json')))
                        if scratch["state"] != "retired":
                            row.update(state="verified_scratch_retirement_pending",
                                       retry_at_unix=time.time() + args.retry_seconds)
                            save()
                            continue
                    row.update(state="verified", recovery_receipt=str(proof), verified_at_utc=_now())
                    completed_this_call += 1
                    row.pop("error", None)
                except (SnapshotError, OSError, ValueError, sqlite3.Error, ImportError) as error:
                    row.update(state="waiting_retry", error_type=type(error).__name__, error=str(error),
                               retry_at_unix=time.time() + args.retry_seconds)
                finally:
                    atomic_write_json(directory / "attempts" / entry["dataset"] / (row["attempt_id"] + ".json"),
                                      {**row, "finished_at_utc": _now()})
                    activity.update(active_dataset=None, active_attempt_id=None)
                    save()
                if budget is not None and completed_this_call >= budget:
                    return result
            if result["state"] == "verified" or not args.watch:
                break
            result["completed_passes"] = pass_number + 1
            save()
            time.sleep(args.retry_seconds)
        return result


def _inflight_lock_only_catalog(repo: Path, root: Path, current: dict, catalog: list[dict],
                                policy: dict) -> list[dict] | None:
    """Retain a fixed plan until a lock-mode-only change's normal next cycle.

    Do not relabel accepted manifests or apply the new mode to old receipts.
    Source paths, publication rights, validation/freshness gates and the entire
    version policy must still match. Other catalog changes keep their existing
    new-plan behavior. A corrupted/redirected saved plan is never a fallback.
    """
    operation = Path(current['operation_directory'])
    plan_path = operation / 'plan.json'
    if (operation.resolve() != operation or not operation.is_relative_to(root / 'operations')
            or plan_path.is_symlink() or plan_path.stat().st_size > 32 * 1024**2):
        raise SnapshotError('in-flight migration plan is redirected or oversized')
    saved = json.loads(plan_path.read_bytes())
    if (saved.get('contract') != CONTRACT or saved.get('plan_sha256') != current['plan_sha256']
            or _identity({'catalog': saved['catalog'], 'policy': saved['policy']}) != current['plan_sha256']):
        raise SnapshotError('in-flight migration plan identity differs')
    entries = migration_entries(repo, catalog, policy)
    def without_modes(rows):
        normalized = []
        for row in rows:
            if row.get('source_coordination_lock_mode', 'exclusive') not in ('exclusive', 'shared_read'):
                raise SnapshotError('invalid in-flight source coordination mode')
            normalized.append({key: value for key, value in row.items()
                               if key != 'source_coordination_lock_mode'})
        return normalized
    if saved['policy'] != policy or without_modes(saved['catalog']) != without_modes(entries):
        return None
    by_name = {entry['dataset']: entry for entry in saved['catalog']}
    frozen = [by_name[entry['dataset']] for entry in catalog]
    if migration_entries(repo, frozen, policy) != saved['catalog']:
        raise SnapshotError('in-flight catalog cannot reconstruct its original exact plan')
    return frozen


def advance_catalog_versions(repo: Path, configuration: dict, *, now: float | None = None) -> dict:
    """One bounded source step for the existing Temporal storage worker.

    No market API, new scheduler/service, external export or moving-latest
    recovery is introduced. The all-source plan is persisted before first I/O.
    After a fully verified cycle the next full check is daily, not every poll.
    """
    from scripts.publish_data_releases import _load_catalog
    from stockagent.data_sync.dataset_version_layout import version_policy
    document = json.loads((repo / "configs/columnar_storage.json").read_text())
    settings = document.get("dataset_version_lifecycle", {})
    if settings.get("enabled") is not True:
        return {"state": "disabled"}
    interval, retry = settings.get("completed_cycle_interval_seconds"), settings.get("busy_retry_seconds")
    if (type(interval) is not int or not 3600 <= interval <= 7 * 86400
            or type(retry) is not int or not 10 <= retry <= 300
            or settings.get("completed_source_budget_per_activity") != 1
            or settings.get("attempted_source_budget_per_activity") != 1
            or settings.get("state_directory") != "dataset-versions"):
        raise SnapshotError("invalid dataset version lifecycle bounds")
    cold = Path(configuration["cold_root"]).absolute()
    _check_packed_read_authority(cold)
    root = Path(configuration["state_root"]).absolute() / settings["state_directory"]
    catalog = _load_catalog(repo / "configs/data_sync/packed_datasets.json")
    policy = version_policy(repo)
    plan = _identity({"catalog": migration_entries(repo, catalog, policy), "policy": policy})
    observed = time.time() if now is None else now
    with _operation_owner(root):
        current_path = root / "current.json"
        current = json.loads(current_path.read_text()) if current_path.exists() else None
        # Coordination policy changes neither the accepted bytes nor their
        # source scope. Complete an in-flight original plan, and preserve a
        # completed cycle's deadline instead of immediately rereading every
        # source just because a reader's lock mode changed. At the deadline a
        # new exact plan adopts the revised catalog normally.
        if (current and current.get('plan_sha256') != plan
                and (current.get('state') != 'verified'
                     or observed < current['next_cycle_unix'])):
            frozen = _inflight_lock_only_catalog(repo, root, current, catalog, policy)
            if frozen is not None:
                if current.get('deferred_catalog_plan_sha256') != plan:
                    current['deferred_catalog_plan_sha256'] = plan
                    atomic_write_json(current_path, current)
                catalog, plan = frozen, current['plan_sha256']
        if (current is None or current.get("plan_sha256") != plan
                or (current.get("state") == "verified" and observed >= current["next_cycle_unix"])):
            current = {"contract": CONTRACT, "plan_sha256": plan, "state": "processing",
                "operation_directory": str(root / "operations" / (plan[:16] + "-" + uuid.uuid4().hex)),
                "created_at_utc": _now(), "shared_export_permissions_changed": False}
            atomic_write_json(current_path, current)
        elif current.get("state") == "verified":
            return {"state": "waiting_next_daily_cycle", "verified_datasets": current["verified_datasets"],
                    "source_count": current["source_count"], "next_cycle_unix": current["next_cycle_unix"],
                    **({'deferred_catalog_plan_sha256': current['deferred_catalog_plan_sha256']}
                       if 'deferred_catalog_plan_sha256' in current else {})}
        args = SimpleNamespace(sync_root=cold, migration_state=Path(current["operation_directory"]),
            node_id=None, retry_seconds=retry, maximum_passes=1, watch=False,
            maximum_completed_sources=1, retire_staging=True,
            maximum_attempted_sources=1,
            recover_missing_base_objects=False, source_lock_fd=None)
        result = migrate_catalog(repo, args, catalog)
        current.update(state=result["state"], verified_datasets=result["verified_datasets"],
                       source_count=result["source_count"], updated_at_utc=_now())
        if result["state"] == "verified":
            current["next_cycle_unix"] = observed + interval
        atomic_write_json(current_path, current)
        # Host paths and licensed source metadata stay in the local journal,
        # not in Temporal history or a public dashboard.
        # Hand off immediately when another source is eligible. Busy sources
        # have individual deadlines; never hot-poll them to remove a blanket
        # 30-second delay, or hold the operation lock while sleeping.
        pending = [row for row in result['datasets'].values() if row['state'] != 'verified']
        poll_clock = time.time()
        remaining = min((row.get('retry_at_unix', 0) - poll_clock for row in pending), default=0)
        delay = min(retry, max(0, math.ceil(remaining)))
        return {**{key: current[key] for key in ("state", "source_count", "verified_datasets", "plan_sha256")},
                'next_poll_seconds': delay}
