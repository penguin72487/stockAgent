"""Bounded NAS delivery queue over the existing immutable packed namespace.

Syncthing completion is transport evidence. Only a source-pinned acknowledgement
from the paired relay establishes NAS file recovery. Only explicitly enabled,
reconstructible cold transport copies may be retired after that proof. Canonical
source objects, code/SQL batches and NAS snapshots are preserved.
"""
from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import shutil
import stat
import time
import urllib.error
import uuid

from scripts.backup_delivery_receipt import (
    DISPATCH, HASH, atomic_public, read_json, signed, validate_ack, validate_readiness,
)
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes
from stockagent.data_sync.offhost_backup import (
    _regular, export_incremental_delivery, private_json,
)
from stockagent.data_sync.packed_backup import (
    PinnedObjectSignatures, mounted_volume, object_descriptor, safe_path, signature,
)
from stockagent.data_sync.packed_snapshots import PACKED_HEAD_SCHEMA_VERSION, _validate_manifest
from stockagent.data_sync.transport_capacity import (
    CONTRACT as TRANSPORT_CAPACITY_CONTRACT, transport_usage as measure_transport_usage,
)
from stockagent.runtime_identity import identity_sha256

CATALOG = "retained_packed_backup_catalog_v1"
LEDGER = "bounded_nas_backup_stream_ledger_v1"
TRANSPORT_SCAN = "backup_transport_scan_notification_v1"
MAX_TRANSPORT_SCAN_BATCHES = 64
# A changing working tree cannot be published as a frozen generation.  It is
# nevertheless a transient race: retry it once in the same owner cycle rather
# than waiting for another complete catalog capture. Other failures keep their
# existing fail-closed behaviour and every attempt repeats capacity admission.
_AUXILIARY_TRANSIENT_TREE_RACES = frozenset({
    "code changed during its frozen working-tree backup",
    "working tree changed during backup; READY withheld",
    "configuration changed during privacy classification",
})
# Encoding a small retained history through a reuse table cost more CPU in the
# measured complete capture. The large history crosses this work threshold;
# small catalogs keep the original stdlib encoder and perform all checks.
_CATALOG_REUSE_MIN_REFERENCES = 1_000_000
_CATALOG_OBSERVATION = 'stockagent.backup_catalog_observation.v1'


def transport_placement_pending(config: dict, *, evidence=None, now=None) -> bool:
    """Yield a new full inventory to a queued, enrolled sender cutover.

    Receipt ingestion/retirement still runs before this check. A fresh waiting
    observation gives the original placement owner its short lock boundary;
    no source inventory, signature or NAS acceptance is renewed by this hint.
    """
    if config.get('transport_namespace_handoff_contract') != 'stockagent.transport_namespace_handoff.v1':
        return False
    if evidence is None:
        from stockagent.data_sync.workspace_volume import ENROLLMENT
        evidence = ENROLLMENT.parent/'transport-relocation-v1'
    try:
        path = _regular(Path(evidence)/'attempt.json')
        if path.stat().st_size > 16384:
            return False
        waiting = json.loads(path.read_bytes())
        if not isinstance(waiting, dict):
            return False
        timestamp = waiting.get('observed_at_epoch')
        current = time.time() if now is None else now
        return (waiting.get('state') == 'waiting_for_original_owner_or_reader'
                and type(timestamp) in (int, float) and -5 <= current - timestamp <= 120)
    except (OSError, ValueError, KeyError, TypeError, SnapshotError):
        return False


def _catalog_file_signatures_identity(catalog: dict) -> str | None:
    """Bind the execution pins excluded from the normal catalog identity."""
    rows = catalog.get('files')
    if not isinstance(rows, list):
        return None
    digest = hashlib.sha256()
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get('relative'), str)
                or not isinstance(row.get('signature'), list)
                or len(row['signature']) != 5
                or any(type(part) is not int for part in row['signature'])):
            return None
        raw = json.dumps([row['relative'], row['signature']], ensure_ascii=True,
                         separators=(',', ':')).encode('ascii')
        digest.update(len(raw).to_bytes(8, 'big'))
        digest.update(raw)
    return digest.hexdigest()


def _private_catalog_generation(path: Path) -> list[int] | None:
    """Observe an owner-private, regular name without reading its contents."""
    try:
        if any(part.is_symlink() for part in path.parents):
            return None
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        try:
            opened = os.fstat(descriptor)
            named = os.stat(path, follow_symlinks=False)
        finally:
            os.close(descriptor)
        fields = lambda s: [s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns,
                            s.st_ctime_ns, s.st_uid, s.st_gid, s.st_nlink]
        if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.geteuid()
                or opened.st_mode & 0o077 or fields(opened) != fields(named)):
            return None
        return fields(opened)
    except OSError:
        return None


def _catalog_observation(path: Path) -> dict:
    generation = _private_catalog_generation(path)
    if generation is None or generation[3] > 16384:
        return {}
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(descriptor, 'rb') as stream:
            value = json.loads(stream.read(16385))
        return value if isinstance(value, dict) and _private_catalog_generation(path) == generation else {}
    except (OSError, ValueError, TypeError):
        return {}


def save_catalog_snapshot(state: Path, catalog: dict) -> dict:
    """Reuse only unchanged private snapshot bytes after a full fresh capture.

    Content identity intentionally excludes source file execution signatures;
    compare those separately and pin the saved file's actual generation. The
    small observation has its own clock and is never a recovery or NAS ACK.
    A missing/invalid observation simply retains the original full write.
    """
    target, marker = state / 'catalog.json', state / 'catalog-observation.json'
    identity = catalog.get('identity_sha256')
    signatures = _catalog_file_signatures_identity(catalog)
    reusable = isinstance(identity, str) and HASH.fullmatch(identity) and signatures is not None
    previous = _catalog_observation(marker) if reusable else {}
    generation = _private_catalog_generation(target) if previous else None
    reuse = bool(generation is not None and previous.get('contract') == _CATALOG_OBSERVATION
                 and previous.get('catalog_identity_sha256') == identity
                 and previous.get('file_signatures_sha256') == signatures
                 and previous.get('catalog_generation') == generation
                 and isinstance(previous.get('persisted_observed_at_utc'), str))
    if not reuse:
        private_json(target, catalog, compact=True)
        generation = _private_catalog_generation(target)
    persisted_at = previous['persisted_observed_at_utc'] if reuse else catalog.get('observed_at_utc')
    state_name = 'unchanged_snapshot_reused' if reuse else 'fresh_snapshot_written'
    if reusable and generation is not None and isinstance(persisted_at, str):
        try:
            private_json(marker, {'contract': _CATALOG_OBSERVATION,
                'catalog_identity_sha256': identity, 'file_signatures_sha256': signatures,
                'catalog_generation': generation, 'persisted_observed_at_utc': persisted_at,
                'observed_at_utc': catalog.get('observed_at_utc'), 'state': state_name,
                'NAS_backup_verified': False}, compact=True)
        except (OSError, ValueError, TypeError):
            # A missing optimization receipt never substitutes for the full
            # catalog or prevents the existing backup workflow from proceeding.
            state_name += '_observation_unavailable'
    return {'state': state_name, 'persisted_observed_at_utc': persisted_at}


def _owner_has_waiters(owner) -> bool:
    """Yield only when a kernel-queued publisher actually needs this owner.

    A missing/unreadable proc view falls back to yielding: lack of diagnostics
    must not turn a long sequence of verified retirements into starvation.
    """
    item = os.fstat(owner.fileno())
    expected = (os.major(item.st_dev), os.minor(item.st_dev), item.st_ino)
    try:
        with open("/proc/locks", encoding="ascii") as locks:
            for line in locks:
                parts = line.split()
                if len(parts) < 9 or parts[1:3] != ["->", "FLOCK"]:
                    continue
                device = parts[-3].split(":")
                if len(device) == 3 and (int(device[0], 16), int(device[1], 16), int(device[2])) == expected:
                    return True
    except (OSError, ValueError):
        return True
    return False


def _scan_error_details(error: Exception) -> dict:
    result = {"state": "retryable_scan_error", "error_type": type(error).__name__}
    if isinstance(error, urllib.error.HTTPError):
        from stockagent.data_sync.syncthing_scan import _http_error_reason
        result.update(http_status=error.code, reason=_http_error_reason(error))
    return result


def file_key(row: dict) -> str:
    return row["relative"] + "@" + row["sha256"]


def _catalog_release_bytes(encoder, member, eligible_keys, encoded_keys):
    """Encode repeated file keys once while retaining stdlib canonical bytes.

    The fast path accepts only plain string-keyed release objects whose key
    list contains plain strings. All other values and any nonstandard release
    retain the original JSONEncoder behavior. Cache entries are restricted to
    files and missing objects already present in this catalog, so temporary
    memory grows with distinct catalog objects, not historical references.
    """
    if type(member) is not dict or not set(map(type, member)).issubset({str}):
        return encoder.encode(member).encode("utf-8")
    references = member.get("required_file_keys")
    if (type(references) not in (list, tuple)
            or not set(map(type, references)).issubset({str})):
        return encoder.encode(member).encode("utf-8")
    fields = []
    for key in sorted(member):
        name = encoder.encode(key).encode("utf-8") + b":"
        if key != "required_file_keys":
            fields.append(name + encoder.encode(member[key]).encode("utf-8"))
            continue
        encoded = []
        for reference in references:
            body = encoded_keys.get(reference)
            if body is None:
                body = encoder.encode(reference).encode("utf-8")
                if reference in eligible_keys:
                    encoded_keys[reference] = body
            encoded.append(body)
        fields.append(name + b"[" + b",".join(encoded) + b"]")
    return b"{" + b",".join(fields) + b"}"


def catalog_identity(content: dict, *, omit_file_signatures: bool = False) -> str:
    """Hash the existing canonical JSON bytes without one catalog-sized buffer.

    The C JSON encoder still owns escaping, numeric spelling and nested ordering.
    Only outer record lists are framed here; temporary space is one record, not
    all versions and files. Filesystem signatures are execution pins, not the
    content identity, and may be omitted without copying every file descriptor.
    """
    if not isinstance(content, dict) or any(not isinstance(key, str) for key in content):
        raise ValueError("catalog content requires string field names")
    encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True,
                               separators=(",", ":"), allow_nan=False)
    eligible_keys = set()
    releases = content.get("releases")
    reference_count = 0
    if type(releases) in (list, tuple) and len(releases) > 1:
        for release in releases:
            references = release.get("required_file_keys") if type(release) is dict else None
            if type(references) in (list, tuple):
                reference_count += len(references)
    collections = (content.get("files"), content.get("missing_objects"))
    objects = sum(len(rows) for rows in collections if type(rows) in (list, tuple))
    if (reference_count >= _CATALOG_REUSE_MIN_REFERENCES
            and reference_count >= 2 * objects):
        for collection in collections:
            if type(collection) not in (list, tuple):
                continue
            for row in collection:
                if (type(row) is dict and type(row.get("relative")) is str
                        and type(row.get("sha256")) is str):
                    eligible_keys.add(row["relative"] + "@" + row["sha256"])
    encoded_keys = {}
    digest = hashlib.sha256(b"{")
    for index, key in enumerate(sorted(content)):
        if index:
            digest.update(b",")
        digest.update(encoder.encode(key).encode("utf-8"))
        digest.update(b":")
        value = content[key]
        if isinstance(value, (list, tuple)):
            digest.update(b"[")
            for member_index, member in enumerate(value):
                if member_index:
                    digest.update(b",")
                if omit_file_signatures and key == "files":
                    member = {k: v for k, v in member.items() if k != "signature"}
                if key == "releases" and eligible_keys:
                    digest.update(_catalog_release_bytes(encoder, member, eligible_keys, encoded_keys))
                else:
                    digest.update(encoder.encode(member).encode("utf-8"))
            digest.update(b"]")
        else:
            digest.update(encoder.encode(value).encode("utf-8"))
    digest.update(b"}")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class _CatalogManifest:
    """Validated head binding and shared object refs, never a recovery manifest."""

    dataset: str
    snapshot_id: str
    hlc: dict
    publisher_node_id: str
    source_fingerprint_sha256: str
    references: tuple[tuple[str, str, int], ...]


def _catalog_manifest(manifest: dict, references: dict, *, retain_objects: bool) -> _CatalogManifest:
    compact = []
    if retain_objects:
        archive = manifest["archive"]
        for ref in (archive["inventory"], *archive["objects"]):
            key = (ref["relpath"], ref["sha256"], ref["bytes"])
            # Invalid descriptors still reach the original later validation;
            # they must not collide with valid int sizes or fail during pooling.
            if type(key[0]) is str and type(key[1]) is str and type(key[2]) is int:
                key = references.setdefault(key, key)
            compact.append(key)
    return _CatalogManifest(manifest["dataset"], manifest["snapshot_id"], manifest["hlc"],
        manifest["publisher"]["node_id"], manifest["source"]["portable_fingerprint_sha256"], tuple(compact))


def _catalog_metadata_workers(cold: Path) -> int:
    """Only the enrolled D reader overlaps metadata; keep descriptor headroom."""
    from stockagent.data_sync.syncthing_scan import CANONICAL_ROOT
    if (cold not in (CANONICAL_ROOT, CANONICAL_ROOT / '.local-state/private-history')
            or not os.path.ismount(CANONICAL_ROOT)):
        return 1
    try:
        import resource
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != resource.RLIM_INFINITY and soft - len(os.listdir('/proc/self/fd')) < 192:
            return 1
    except (OSError, ValueError, ImportError):
        return 1
    return 4


def capture_catalog(cold: Path, publication_catalog: Path, history_disposition: Path | None = None,
                    *, include_unreferenced_objects: bool = False, testing_disposition: Path | None = None,
                    metadata_workers: int | None = None, private_backup_policy: dict | None = None) -> dict:
    """Fixed metadata inventory, with per-release missing-object diagnostics.

    Missing historical objects do not erase a historical manifest or obstruct
    unrelated valid deliveries. Catalog-restricted workspaces are not scanned.
    """
    cold = cold.absolute()
    if cold.resolve() != cold:
        raise SnapshotError("cold metadata root is redirected")
    if private_backup_policy is not None:
        from stockagent.data_sync.private_cold_backup import validate_private_root
        validate_private_root(cold, private_backup_policy)
    workers = _catalog_metadata_workers(cold) if metadata_workers is None else metadata_workers
    if type(workers) is not int or not 1 <= workers <= 4:
        raise SnapshotError("catalog metadata workers must be between one and four")
    with PinnedObjectSignatures(cold, parallel_signatures=workers > 1) as metadata_pins:
        result = _capture_catalog(cold, publication_catalog, history_disposition,
            include_unreferenced_objects=include_unreferenced_objects,
            testing_disposition=testing_disposition, metadata_pins=metadata_pins,
            metadata_workers=workers, private_backup_policy=private_backup_policy)
        metadata_pins.recheck()
        if private_backup_policy is not None:
            validate_private_root(cold, private_backup_policy)
        return result


def _capture_catalog(cold: Path, publication_catalog: Path, history_disposition: Path | None,
                     *, include_unreferenced_objects: bool, testing_disposition: Path | None,
                     metadata_pins: PinnedObjectSignatures, metadata_workers: int = 1,
                     private_backup_policy: dict | None = None) -> dict:
    cold = cold.absolute()
    entries = json.loads(publication_catalog.read_bytes())["datasets"]
    excluded = ({r["dataset"] for r in entries if r.get("publish") is not True}
                if private_backup_policy is None else set())
    rows: dict[str, dict] = {}
    releases = []
    head_snapshots = set()
    errors = []
    excluded_manifests = []
    metadata = {}
    shared_references = {}
    # Pass-local validation reuse only. Every original manifest is still fully
    # read/hashed, and shared objects are independently pinned and rechecked.
    object_validation_cache = set()
    all_referenced_objects = set()
    retained_referenced_objects = set()
    orphan_scan_blocked = False
    from stockagent.data_sync.testing_disposition import DEFAULT_POLICY, load_policy, manifest_discarded
    disposition_path = testing_disposition or DEFAULT_POLICY
    disposition = load_policy(disposition_path) if disposition_path.exists() else None
    discarded_testing_releases = []
    discarded_testing_metadata = {}
    discarded_testing_objects = set()

    def add_metadata(path: Path, observed: tuple[bytes, tuple[int, ...]] | None = None) -> tuple[bytes, dict]:
        relative = path.relative_to(cold).as_posix()
        raw, before = observed if observed is not None else metadata_pins.read_metadata(relative)
        row = {"relative": relative, "sha256": hashlib.sha256(raw).hexdigest(),
               "bytes": len(raw), "signature": list(before), "role": "cold_metadata"}
        rows[relative] = row
        return raw, row

    def observe_manifest(path: Path, observed: tuple[bytes, tuple[int, ...]] | None = None) -> bool:
        if path.parent.parent != cold / "manifests":
            raise SnapshotError("head references a non-manifest path")
        relative = path.relative_to(cold).as_posix()
        raw, row = add_metadata(path, observed)
        manifest = json.loads(raw)
        _validate_manifest(manifest, object_validation_cache=object_validation_cache)
        if manifest["dataset"] != path.parent.name or manifest["snapshot_id"] != path.stem:
            raise SnapshotError("retained manifest path identity differs")
        refs = [manifest["archive"]["inventory"], *manifest["archive"]["objects"]]
        all_referenced_objects.update(r["relpath"] for r in refs)
        if disposition and manifest_discarded(manifest, disposition):
            if not any(r["manifest_relative"] == relative for r in discarded_testing_releases):
                discarded_testing_releases.append({"dataset": manifest["dataset"], "snapshot_id": manifest["snapshot_id"],
                    "manifest_relative": relative, "manifest_sha256": row["sha256"],
                    "policy_identity_sha256": disposition["identity_sha256"], "recovery_claimed": False})
            discarded_testing_objects.update(r["relpath"] for r in refs)
            discarded_testing_metadata[relative] = (_catalog_manifest(
                manifest, shared_references, retain_objects=False), row)
            return False
        retained_referenced_objects.update(r["relpath"] for r in refs)
        metadata[relative] = (_catalog_manifest(manifest, shared_references, retain_objects=True), row)
        return True

    if private_backup_policy is not None:
        add_metadata(cold / 'private-history.json')
    def prefetch_manifest(path: Path):
        if path.parent.name in excluded:
            return None, None
        try:
            return metadata_pins.read_metadata(path.relative_to(cold).as_posix()), None
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            return None, error

    # Each leaf is independently opened without following links and its parent
    # FD remains pinned through the final name check. Only byte transport is
    # parallel: validation, error ordering, reductions and publication stay on
    # this thread. Limit read-ahead to the worker count, not the catalog size.
    manifest_pool = (ThreadPoolExecutor(max_workers=metadata_workers)
                     if metadata_workers > 1 else nullcontext(None))
    with manifest_pool as pool:
        paths = iter(sorted((cold / "manifests").glob("*/*.json")))
        while batch := list(itertools.islice(paths, metadata_workers)):
            fetched = pool.map(prefetch_manifest, batch) if pool is not None else map(prefetch_manifest, batch)
            for path, (observed, read_error) in zip(batch, fetched, strict=True):
                relative = path.relative_to(cold).as_posix()
                if path.parent.name in excluded:
                    excluded_manifests.append(relative)
                    if include_unreferenced_objects:
                        try:
                            manifest = json.loads(_regular(path).read_bytes())
                            _validate_manifest(manifest)
                            all_referenced_objects.update(r["relpath"] for r in [
                                manifest["archive"]["inventory"], *manifest["archive"]["objects"]])
                            retained_referenced_objects.update(r["relpath"] for r in [
                                manifest["archive"]["inventory"], *manifest["archive"]["objects"]])
                        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
                            orphan_scan_blocked = True
                            errors.append({"relative": relative, "reason": type(error).__name__})
                    continue
                try:
                    if read_error is not None:
                        raise read_error
                    observe_manifest(path, observed)
                except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
                    rows.pop(relative, None)
                    errors.append({"relative": relative, "reason": type(error).__name__})
    for path in sorted((cold / "heads").glob("*/*.json")):
        if path.parent.name in excluded:
            continue
        relative = path.relative_to(cold).as_posix()
        try:
            raw, row = add_metadata(path)
            head = json.loads(raw)
            if head.get("manifest_relpath") not in metadata and head.get("manifest_relpath") not in discarded_testing_metadata:
                # A publisher may commit a new immutable manifest after our
                # first scan and before the head scan. Capture those actual
                # fixed bytes and validate the same head SHA, without guessing
                # completeness or blocking unrelated publishers.
                observe_manifest(safe_path(cold, head["manifest_relpath"]))
            manifest, manifest_row = (metadata[head["manifest_relpath"]] if head["manifest_relpath"] in metadata
                                      else discarded_testing_metadata[head["manifest_relpath"]])
            if (head["schema_version"] != PACKED_HEAD_SCHEMA_VERSION
                    or head["dataset"] != path.parent.name or head["node_id"] != path.stem
                    or ".sync-conflict-" in path.name or head["manifest_sha256"] != manifest_row["sha256"]
                    or head["snapshot_id"] != manifest.snapshot_id or head["hlc"] != manifest.hlc
                    or head["node_id"] != manifest.publisher_node_id):
                raise SnapshotError("current head does not match its fixed retained manifest")
            if head["manifest_relpath"] in discarded_testing_metadata:
                continue
            head_snapshots.add((manifest.dataset, manifest.snapshot_id))
            row["captured_bytes_utf8"] = raw.decode("utf-8")
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            rows.pop(relative, None)
            errors.append({"relative": relative, "reason": type(error).__name__})

    # Previous head bytes are durable provenance, distinct from mutable heads.
    for path in sorted((cold / "head-history/heads").glob("*/*/*.json")):
        dataset, node = path.parent.parent.name, path.parent.name
        if dataset in excluded:
            continue
        relative = path.relative_to(cold).as_posix()
        try:
            raw, row = add_metadata(path)
            head = json.loads(raw)
            if head.get("manifest_relpath") not in metadata and head.get("manifest_relpath") not in discarded_testing_metadata:
                observe_manifest(safe_path(cold, head["manifest_relpath"]))
            manifest, manifest_row = (metadata[head["manifest_relpath"]] if head["manifest_relpath"] in metadata
                                      else discarded_testing_metadata[head["manifest_relpath"]])
            if (head["schema_version"] != PACKED_HEAD_SCHEMA_VERSION or head["dataset"] != dataset
                    or head["node_id"] != node or path.stem != row["sha256"]
                    or head["manifest_sha256"] != manifest_row["sha256"]
                    or head["snapshot_id"] != manifest.snapshot_id or head["hlc"] != manifest.hlc):
                raise SnapshotError("historical head does not match its fixed manifest")
            if head["manifest_relpath"] in discarded_testing_metadata:
                continue
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            rows.pop(relative, None)
            errors.append({"relative": relative, "reason": type(error).__name__})

    missing = {}
    object_file_keys = {}
    with PinnedObjectSignatures(cold, parallel_signatures=metadata_workers > 1) as pinned:
        for relative in sorted(metadata):
            manifest, manifest_row = metadata.pop(relative)
            required = [file_key(manifest_row)]
            for object_relative, expected_digest, expected_bytes in manifest.references:
                key_binding = object_file_keys.get(object_relative)
                if key_binding is None:
                    digest = object_descriptor(object_relative)
                    key_binding = object_file_keys[object_relative] = (
                        digest, object_relative + "@" + digest, expected_bytes)
                digest, key, declared_bytes = key_binding
                if digest != expected_digest or type(expected_bytes) is not int or expected_bytes < 0:
                    raise SnapshotError("retained object descriptor differs from its manifest")
                if declared_bytes != expected_bytes:
                    # A missing/short first object must not hide conflicting
                    # declarations in later versions or mark valid versions bad.
                    raise SnapshotError("retained manifests disagree on an immutable object")
                required.append(key)
            releases.append({"dataset": manifest.dataset, "snapshot_id": manifest.snapshot_id,
                "manifest_sha256": manifest_row["sha256"], "manifest_relative": relative,
                "source_fingerprint_sha256": manifest.source_fingerprint_sha256,
                "current_head": (manifest.dataset, manifest.snapshot_id) in head_snapshots,
                "required_file_keys": sorted(required), "missing_objects": []})
        # Version order interleaves hash-prefix directories and repeatedly
        # evicts the same directory FDs on DrvFs. Resolve all declarations
        # first, then visit each unique object in directory order. Every leaf
        # still receives a fresh no-follow stat, and every pinned ancestor is
        # rechecked below; no mtime or cross-pass cache certifies an object.
        def observe_object(object_relative):
            digest, _key, expected_bytes = object_file_keys[object_relative]
            try:
                # object_descriptor and all size/digest agreements were checked
                # above; avoid parsing that same descriptor a second time.
                observed = pinned.regular_file_signature(object_relative)
                if observed[2] != expected_bytes:
                    raise SnapshotError("retained object size differs")
                return False, {"relative": object_relative, "sha256": digest,
                    "bytes": expected_bytes, "signature": list(observed), "role": "packed_object"}
            except (OSError, SnapshotError) as error:
                return True, {"relative": object_relative, "sha256": digest,
                    "bytes": expected_bytes, "reason": type(error).__name__}
        # Bound submissions, preserve deterministic order and update the
        # catalog only on this thread. Each duplicated parent FD stays pinned
        # through its leaf stat; final ancestor/root rechecks still run below.
        pool_context = (ThreadPoolExecutor(max_workers=metadata_workers)
                        if metadata_workers > 1 else nullcontext(None))
        with pool_context as pool:
            objects = iter(sorted(object_file_keys))
            while batch := list(itertools.islice(objects, 32)):
                observed = pool.map(observe_object, batch) if pool is not None else map(observe_object, batch)
                for failed, row in observed:
                    (missing if failed else rows)[row['relative']] = row
        if missing:
            for release in releases:
                release["missing_objects"] = sorted(
                    relative for key in release["required_file_keys"]
                    if (relative := key.rsplit("@", 1)[0]) in missing
                )
        if include_unreferenced_objects and not orphan_scan_blocked:
            # Back up unique evidence in the enrolled cold object namespace;
            # lack of a manifest grants neither deletion nor release identity.
            # Objects belonging to restricted manifests remain excluded.
            for parent, dirs, names in os.walk(cold / "objects", followlinks=False):
                for name in dirs:
                    if (Path(parent) / name).is_symlink():
                        raise SnapshotError("cold object directory is redirected")
                for name in names:
                    relative = (Path(parent) / name).relative_to(cold).as_posix()
                    if relative in all_referenced_objects:
                        continue
                    try:
                        digest = object_descriptor(relative)
                        observed = pinned.signature(relative)
                        rows[relative] = {"relative": relative, "sha256": digest,
                            "bytes": observed[2], "signature": list(observed),
                            "role": "unreferenced_cold_object", "release_provenance_verified": False}
                    except (OSError, SnapshotError) as error:
                        errors.append({"relative": relative, "reason": type(error).__name__})
        pinned.recheck()
    if not releases:
        raise SnapshotError("no valid retained packed releases found")
    body = {"contract": CATALOG, "releases": releases,
            "files": sorted(rows.values(), key=lambda r: r["relative"]),
            "missing_objects": sorted(missing.values(), key=lambda r: r["relative"]),
            "metadata_errors": errors, "excluded_catalog_datasets": sorted(excluded),
            "excluded_manifests": excluded_manifests, "unpublished_sources_included": False,
            "unreferenced_cold_objects_included": include_unreferenced_objects and not orphan_scan_blocked,
            "scope": "all observed retained canonical manifests; current heads prioritized"}
    if private_backup_policy is not None:
        body.update(scope='all retained private immutable manifests; separate personal NAS backup authorization',
                    private_backup_policy_identity_sha256=identity_sha256(private_backup_policy))
    from stockagent.data_sync.backup_history import apply_disposition
    body = apply_disposition(body, history_disposition)
    if disposition:
        body["discarded_testing_releases"] = discarded_testing_releases
        body["discarded_testing_objects"] = sorted(discarded_testing_objects - retained_referenced_objects)
        body["testing_disposition_identity_sha256"] = disposition["identity_sha256"]
    # Filesystem signatures pin this execution, but are not content identity.
    return {**body, "identity_sha256": catalog_identity(body, omit_file_signatures=True),
            "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def empty_ledger() -> dict:
    return {"contract": LEDGER, "deliveries": {}, "reported_baseline": []}


def reported_baseline(delivery: Path, report_path: Path, repository_id: str) -> dict:
    """Bind a relayed pilot report to current exact source bytes, not an ACK.

    The original NAS receipts remain on lab203. Keep this provenance explicit
    so user-relayed acceptance cannot silently become machine verification.
    """
    from scripts.verify_backup_delivery import verify
    report_path = _regular(report_path)
    raw_report = report_path.read_bytes()
    report = json.loads(raw_report)
    if report.get("original_remote_receipts_received") is not False:
        raise SnapshotError("pilot baseline needs its explicit relayed provenance")
    if report.get("evidence_origin") == "user_relay_from_lab203_local_codex":
        acceptance = report["delivery"]
        key = acceptance["envelope_identity_sha256"]
        snapshot = acceptance["snapshot_id"]
        reported_repo = acceptance["repository_id"]
        required = ("all_command_exit_codes_zero", "handoff_ready_both_envelope_hashes_exact_set_verified",
                    "encrypted_backup_verified", "check_read_data_verified",
                    "fixed_snapshot_independent_restore_verified", "source_verifier_verified",
                    "all_401_file_sha256_verified")
        if any(acceptance.get(k) is not True for k in required):
            raise SnapshotError("relayed pilot acceptance is incomplete")
    elif report.get("evidence_kind") == "user_relayed_lab203_acceptance_summary":
        key = report["envelope_identity_sha256"]
        snapshot = report["nas_restic_snapshot_id"]
        reported_repo = report["nas_report"]["repository_id"]
        required = ("ready_and_pinned_envelope_identity", "complete_file_set", "nas_encrypted_backup",
                    "nas_check_read_all_data", "exact_full_snapshot_independent_restore",
                    "restored_source_verifier", "all_13_file_sha256_match")
        if any(report["reported_checks"].get(k) is not True for k in required):
            raise SnapshotError("relayed pilot acceptance is incomplete")
        rows = report["source_local_independent_checks"]["batch_files"]
        acceptance = {"envelope_file_sha256": next(r["sha256"] for r in rows if r["relative"] == "backup-envelope.json"),
                      "files": len(rows), "bytes": sum(r["bytes"] for r in rows)}
    else:
        raise SnapshotError("unknown pilot acceptance report")
    if (reported_repo != repository_id or not HASH.fullmatch(snapshot)
            or delivery.name != "batch-" + key):
        raise SnapshotError("pilot baseline repository, snapshot or batch identity differs")
    verified = verify(delivery, key)
    raw_envelope = (delivery / "backup-envelope.json").read_bytes()
    envelope = json.loads(raw_envelope)
    if (hashlib.sha256(raw_envelope).hexdigest() != acceptance["envelope_file_sha256"]
            or verified["files_verified"] + 2 != acceptance["files"]
            or verified["bytes_verified"] + len(raw_envelope) + (delivery / "READY").stat().st_size != acceptance["bytes"]):
        raise SnapshotError("pilot baseline source byte proof differs from its report")
    files = []
    for row in envelope["files"]:
        if row["relative"] == "source-plan.json":
            # Original full-release pilots include their fixed source plan;
            # it is byte-verified above but is not a canonical cold object.
            continue
        if not row["relative"].startswith("cold/"):
            raise SnapshotError("pilot baseline must contain only canonical cold inputs")
        relative = row["relative"][5:]
        safe_path(delivery / "cold", relative)
        if relative.startswith("objects/"):
            if object_descriptor(relative) != row["sha256"]:
                raise SnapshotError("pilot baseline object descriptor differs")
        elif not (relative.startswith(("manifests/", "heads/")) and relative.endswith(".json")):
            raise SnapshotError("pilot baseline has noncanonical cold metadata")
        files.append({**row, "relative": relative})
    return {"envelope_identity_sha256": key, "envelope_file_sha256": acceptance["envelope_file_sha256"],
        "batch_relative": delivery.name, "repository_id": repository_id, "snapshot_id": snapshot,
        "report_sha256": hashlib.sha256(raw_report).hexdigest(), "files": files,
        "evidence_origin": "user_relay_from_lab203_local_codex",
        "original_remote_receipts_received": False, "machine_acknowledged": False}


def _release_file_covered(release: dict, accepted: set[str]) -> bool:
    if release["missing_objects"]:
        return False
    keys = release["required_file_keys"]
    # Normal catalog references are plain strings.  Check membership directly
    # instead of rebuilding an often-repeated set for every historical version.
    # Fall back to the original conversion for unusual inputs: in particular,
    # it must still reject an unhashable key even after an earlier missing key.
    if type(keys) in (list, tuple) and all(type(key) is str for key in keys):
        return accepted.issuperset(keys)
    return set(keys) <= accepted


def coverage(catalog: dict, ledger: dict) -> dict:
    confirmed = set()
    reported = set()
    published = set()
    pending_bytes = 0
    for delivery in ledger["deliveries"].values():
        keys = {file_key(row) for row in delivery["files"]}
        published.update(keys)
        if delivery.get("acceptance"):
            confirmed.update(keys)
        else:
            pending_bytes += delivery["dispatch"]["complete_bytes"]
    for baseline in ledger.get("reported_baseline", []):
        reported.update(file_key(r) for r in baseline["files"])
    accepted = confirmed | reported
    available = {file_key(row) for row in catalog["files"]}
    current = [r for r in catalog["releases"] if r["current_head"]]
    recovered = lambda r: _release_file_covered(r, accepted)
    return {"available_file_count": len(available), "available_bytes": sum(r["bytes"] for r in catalog["files"]),
        "machine_acknowledged_files": len(available & confirmed),
        "user_reported_baseline_files": len(available & reported),
        "nas_covered_file_count": len(available & accepted),
        "nas_covered_bytes": sum(r["bytes"] for r in catalog["files"] if file_key(r) in accepted),
        "pending_delivery_count": sum(not d.get("acceptance") for d in ledger["deliveries"].values()),
        "pending_bytes": pending_bytes, "published_file_keys": published,
        "accepted_file_keys": accepted, "current_release_count": len(current),
        "current_releases_nas_file_covered": sum(recovered(r) for r in current),
        "retained_release_count": len(catalog["releases"]),
        "observed_release_count": len(catalog["releases"]) + len(catalog.get("abandoned_releases", [])),
        "abandoned_history_release_count": len(catalog.get("abandoned_releases", [])),
        "abandoned_unavailable_object_count": len(catalog.get("abandoned_missing_objects", [])),
        "retained_releases_nas_file_covered": sum(recovered(r) for r in catalog["releases"]),
        "missing_object_count": len(catalog["missing_objects"]),
        "blocked_retained_releases": sum(bool(r["missing_objects"]) for r in catalog["releases"]),
        "metadata_error_count": len(catalog["metadata_errors"]),
        "unreferenced_cold_object_count": sum(r["role"] == "unreferenced_cold_object" for r in catalog["files"]),
        "unreferenced_cold_object_bytes": sum(r["bytes"] for r in catalog["files"] if r["role"] == "unreferenced_cold_object"),
        "all_history_backup_verified": False, "canonical_reconstruction_verified": False,
        "unpublished_sources_included": False}


def select_wave(catalog: dict, covered: set[str], *, maximum_bytes: int,
                maximum_files: int = 2048) -> list[dict]:
    current_keys = {key for r in catalog["releases"] if r["current_head"] for key in r["required_file_keys"]}
    eligible = [r for r in catalog["files"] if file_key(r) not in covered]
    eligible.sort(key=lambda r: (r["role"] != "cold_metadata", file_key(r) not in current_keys,
                                 r["bytes"], r["relative"]))
    selected = []
    total = 0
    # Allow room for the JSON envelope, paths and dispatch bookkeeping.
    overhead = 1024 * 1024
    for row in eligible:
        if len(selected) >= maximum_files:
            break
        if total + row["bytes"] + overhead > maximum_bytes:
            continue
        selected.append(row)
        total += row["bytes"]
    return selected


def recovery_index(catalog: dict, ledger: dict, repository_id: str) -> dict:
    deliveries = []
    for item in ledger.get("reported_baseline", []):
        deliveries.append({k: item[k] for k in ("envelope_identity_sha256", "envelope_file_sha256",
            "batch_relative", "repository_id", "snapshot_id", "files", "evidence_origin", "report_sha256")})
    for delivery in ledger["deliveries"].values():
        ack = delivery.get("acceptance")
        if not ack:
            continue
        dispatch = delivery["dispatch"]
        deliveries.append({**{k: dispatch[k] for k in ("envelope_identity_sha256", "envelope_file_sha256",
            "batch_relative", "repository_id", "delivery_kind")}, "snapshot_id": ack["snapshot_id"],
            "acceptance_identity_sha256": ack["identity_sha256"], "evidence_origin": "paired_relay_machine_acknowledgement",
            "batch_retired_from_transport": delivery.get("cache_retirement", {}).get("state") == "retired",
            "files": [{k: row[k] for k in ("relative", "sha256", "bytes")} for row in delivery["files"]]})
    body = {"contract": "fixed_nas_snapshot_recovery_index_v1", "repository_id": repository_id,
        "catalog_identity_sha256": catalog["identity_sha256"], "deliveries": deliveries,
        "releases": [{k: row[k] for k in ("dataset", "snapshot_id", "manifest_sha256", "manifest_relative",
            "source_fingerprint_sha256", "current_head", "missing_objects")} for row in catalog["releases"]],
        "abandoned_releases": catalog.get("abandoned_releases", []),
        "nas_provenance_verified_by_index": False, "canonical_reconstruction_verified": False,
        "full_history_backup_verified": False}
    return {**body, "identity_sha256": identity_sha256(body)}


class BackupStream:
    def __init__(self, configuration: dict):
        self.config = configuration
        if type(configuration.get('publication_requires_receiver_ready', True)) is not bool:
            raise SnapshotError('receiver admission must be an explicit boolean')
        if type(configuration.get("include_unreferenced_cold_objects", False)) is not bool:
            raise SnapshotError("unreferenced cold inclusion must be an explicit boolean")
        if type(configuration.get("cold_object_replication_enabled", True)) is not bool:
            raise SnapshotError("cold object replication must be an explicit boolean")
        if type(configuration.get('cold_metadata_replication_enabled', False)) is not bool:
            raise SnapshotError('cold metadata replication must be an explicit boolean')
        cache = configuration.get("cold_transport_cache", {})
        if type(cache.get('allow_scanning_when_fully_converged', False)) is not bool:
            raise SnapshotError('scanning cache retirement must be an explicit boolean')
        retirement_turn = cache.get('maximum_retirements_per_cycle')
        if retirement_turn is not None and (type(retirement_turn) is not int or not 1 <= retirement_turn <= 1024):
            raise SnapshotError('transport retirement turn must contain 1..1024 complete transactions')
        if cache.get("enabled") is True and configuration.get("automatic_batch_deletion") is not True:
            raise SnapshotError("cold cache retirement must be reported as automatic batch deletion")
        if (configuration.get("schema_version") != 1 or configuration.get("automatic_pruning") is not False
                or (configuration.get("automatic_batch_deletion") is not False and not (
                    configuration.get("automatic_batch_deletion") is True and cache.get("enabled") is True
                    and cache.get("scope") == "machine_acknowledged_reconstructible_cold_transport"
                    and isinstance(cache.get("user_instruction"), str) and bool(cache["user_instruction"].strip())))
                or configuration.get("source_cleanup_authorized") is not False):
            raise SnapshotError("stream preserves source, batches and NAS snapshots")
        self.cold = Path(configuration["cold_root"]).absolute()
        self.transport = Path(configuration["transport_root"]).absolute()
        self.receipts = Path(configuration["receipt_root"]).absolute()
        self.state = Path(configuration["state_root"]).absolute()
        for root in (self.cold, self.transport, self.receipts, self.state):
            if any(p.is_symlink() for p in (root, *root.parents)):
                raise SnapshotError("stream roots must not traverse symlinks")
        if (self.transport == self.cold or self.transport.is_relative_to(self.cold)
                or self.state.is_relative_to(self.transport) or self.receipts.is_relative_to(self.transport)):
            raise SnapshotError("stream roots have conflicting authorities")
        self.state.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.ledger_path = self.state / "ledger.json"
        for key in ("maximum_batch_bytes", "maximum_batch_files", "maximum_pending_bytes",
                    "maximum_pending_deliveries", "maximum_retained_transport_bytes", "reserve_bytes",
                    "readiness_max_age_seconds"):
            if type(configuration.get(key)) is not int or configuration[key] <= 0:
                raise SnapshotError("stream capacity and age limits must be positive integers")
        if "maximum_staging_bytes" in configuration and (type(configuration["maximum_staging_bytes"]) is not int
                                                       or configuration["maximum_staging_bytes"] <= 0):
            raise SnapshotError("staging capacity must be a positive integer")
        self.private_backup_reserve_bytes = configuration['reserve_bytes']
        backup_capacity = configuration.get('private_backup_capacity')
        if backup_capacity is not None:
            private = configuration.get('private_history_backup', {})
            if (not isinstance(backup_capacity, dict)
                    or set(backup_capacity) != {'contract', 'use_backup_reserve', 'minimum_free_bytes', 'user_instruction'}
                    or backup_capacity.get('contract') != 'private_closed_wave_backup_reserve_v1'
                    or backup_capacity.get('use_backup_reserve') is not True
                    or type(backup_capacity.get('minimum_free_bytes')) is not int
                    or not 32 * 1024**3 <= backup_capacity['minimum_free_bytes'] <= min(configuration['reserve_bytes'], 64 * 1024**3)
                    or not isinstance(backup_capacity.get('user_instruction'), str)
                    or not backup_capacity['user_instruction'].strip()
                    or not isinstance(private, dict) or private.get('enabled') is not True
                    or private.get('private_backup_authorized') is not True
                    or private.get('public_publish_allowed') is not False):
                raise SnapshotError('private backup reserve requires an authorized bounded closed-wave policy')
            self.private_backup_reserve_bytes = backup_capacity['minimum_free_bytes']
        self.pipeline = configuration.get("pipeline", {})
        if self.pipeline:
            if set(self.pipeline) != {"maximum_waves_per_cycle", "copy_workers", "verify_workers", "retry_delays_seconds"}:
                raise SnapshotError("source pipeline needs an exact bounded policy")
            for key in ("maximum_waves_per_cycle", "copy_workers", "verify_workers"):
                if type(self.pipeline[key]) is not int or not 1 <= self.pipeline[key] <= 16:
                    raise SnapshotError("source pipeline workers and waves must be between 1 and 16")
            if not isinstance(self.pipeline["retry_delays_seconds"], list) or not self.pipeline["retry_delays_seconds"] or any(
                    type(n) is not int or not 1 <= n <= 86400 for n in self.pipeline["retry_delays_seconds"]):
                raise SnapshotError("source pipeline retry delays must be bounded")

    def storage_guard(self) -> None:
        from stockagent.data_sync.transport_placement import CANONICAL, on_e, check_transport
        e_transport = self.transport == CANONICAL and on_e()
        if e_transport:
            check_transport()
        for mount_name, expected_source in self.config.get("required_mounts", {}).items():
            if e_transport and Path(mount_name) == CANONICAL:
                continue  # E uses its pinned VHD/UUID/alias, not a drive-letter source.
            if mounted_volume(Path(mount_name))[2] != expected_source:
                raise SnapshotError("backup transport has the wrong physical storage volume")
        if self.config.get("require_transport_mount", True):
            mounted_volume(self.transport)
        if self.config.get("read_root"):
            raise SnapshotError("larger-request D aliases failed full-size validation; use the canonical 8 KiB mount")
        if not self.transport.is_dir() or not (self.transport / ".stfolder").is_dir():
            raise SnapshotError("backup transport marker is missing")
        ignore = _regular(self.transport / ".stignore").read_text().splitlines()
        if not {"(?d).staging", "(?d).staging/**"} <= set(ignore):
            raise SnapshotError("unready staging must be excluded from Syncthing")

    def backup_read_root(self) -> Path:
        """The authoritative, measured-stable D reader for formal backups."""
        return self.cold

    def load_ledger(self) -> dict:
        value = json.loads(self.ledger_path.read_bytes()) if self.ledger_path.exists() else empty_ledger()
        if value.get("contract") != LEDGER or not isinstance(value.get("deliveries"), dict):
            raise SnapshotError("unknown stream ledger")
        return value

    def _save_ledger(self, ledger: dict) -> None:
        # Private formatting only; fields, values, canonical identities and
        # the durable replace boundary stay unchanged for every owner path.
        private_json(self.ledger_path, ledger, compact=True)

    def register_baseline(self, report_path: Path, delivery: Path) -> dict:
        self.storage_guard()
        with (self.state / "owner.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            baseline = reported_baseline(delivery, report_path, self.config["repository_id"])
            ledger = self.load_ledger()
            existing = [r for r in ledger["reported_baseline"]
                        if r["envelope_identity_sha256"] == baseline["envelope_identity_sha256"]]
            if existing and existing != [baseline]:
                raise SnapshotError("preserve the previous relayed pilot baseline")
            if not existing:
                ledger["reported_baseline"].append(baseline)
                self._save_ledger(ledger)
            return {"state": "user_reported_pilot_baseline_registered", "envelope_identity_sha256": baseline["envelope_identity_sha256"],
                    "files": len(baseline["files"]), "machine_acknowledged": False}

    def refresh_auxiliary(self, *, wait_for_owner: bool = False) -> dict:
        """Retain a prior unsubmitted snapshot and freeze the current work."""
        self.storage_guard()
        if not self.config.get("auxiliary"):
            raise SnapshotError("no auxiliary snapshot scope is configured")
        with (self.state / "owner.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
            self.storage_guard()
            ledger = self.load_ledger()
            self.recover_publications(ledger)
            pending = ledger.get("auxiliary_pending")
            if pending:
                from stockagent.data_sync.backup_auxiliary import working_tree_inventory, control_input
                config = self.config["auxiliary"]
                tree = working_tree_inventory(Path(config["repository_root"]), documentation=config["include_documentation"],
                                              configs=config["include_configs"])
                control, _, _ = control_input(Path(config["control_receipt"]))
                if identity_sha256({"code": tree["identity_sha256"], "control": control}) == pending["export"]["content_identity_sha256"]:
                    return {"state": "current_auxiliary_snapshot_staged", "published_to_relay": False,
                            "pending": True, "source_or_staging_deleted": False}
                del ledger["auxiliary_pending"]
            if pending:
                ledger.setdefault("quarantined_auxiliary_snapshots", []).append({
                    "staging_relative": pending["staging_relative"], "reason": "explicit_current_working_tree_refresh"})
            ledger["auxiliary_last_capture_epoch"] = 0
            self._save_ledger(ledger)
            self.stage_auxiliary(ledger)
            self._save_ledger(ledger)
            return {"state": "current_auxiliary_snapshot_staged", "published_to_relay": False,
                    "pending": bool(ledger.get("auxiliary_pending")), "source_or_staging_deleted": False}

    def transport_usage(self) -> dict:
        # GC can rename/unlink an accepted wave while this read-only inventory
        # traverses it. Restart the complete scan; skipping vanished entries
        # could undercount a wave merely moved to another retained directory.
        authority = self.transport.stat()
        for _ in range(3):
            try:
                usage = self._transport_usage_once()
            except FileNotFoundError as error:
                after = self.transport.stat()
                if (authority.st_dev, authority.st_ino) != (after.st_dev, after.st_ino):
                    raise SnapshotError("transport authority changed during capacity inventory") from error
                continue
            after = self.transport.stat()
            if (authority.st_dev, authority.st_ino) != (after.st_dev, after.st_ino):
                raise SnapshotError("transport authority changed during capacity inventory")
            return usage
        raise SnapshotError("transport changed repeatedly during capacity inventory; retry cycle")

    def _transport_usage_once(self) -> dict:
        return measure_transport_usage(self.transport)
    def staging_room(self, usage: dict, *, reserve_bytes: int | None = None) -> int:
        return min(self.staging_bounds(usage, **({} if reserve_bytes is None else {'reserve_bytes': reserve_bytes})).values())

    def staging_bounds(self, usage: dict, *, reserve_bytes: int | None = None) -> dict:
        """Independent bounds, measured in the same units; never summed."""
        if reserve_bytes is not None and reserve_bytes != self.private_backup_reserve_bytes:
            raise SnapshotError('transport reserve differs from its enrolled private backup policy')
        return {
            "staging": self.config.get("maximum_staging_bytes", self.config["maximum_pending_bytes"]) - usage["retained_staging_bytes"],
            "retained_transport": self.config["maximum_retained_transport_bytes"] - usage["retained_transport_bytes"],
            "physical": shutil.disk_usage(self.transport).free - (self.config['reserve_bytes'] if reserve_bytes is None else reserve_bytes),
        }

    def ingest(self, ledger: dict) -> list[dict]:
        errors = []
        if not self.receipts.exists():
            return errors
        for path in sorted(self.receipts.glob("acceptance-*.json")):
            try:
                ack = read_json(path)
                key = ack["envelope_identity_sha256"]
                delivery = ledger["deliveries"].get(key)
                if not delivery:
                    # Pilot acknowledgements can coexist with the new stream.
                    continue
                if path.name != "acceptance-" + key + ".json":
                    raise ValueError("NAS receipt filename differs")
                validate_ack(ack, delivery["dispatch"])
                if delivery.get("acceptance") and delivery["acceptance"] != ack:
                    raise ValueError("immutable NAS acknowledgement changed")
                delivery["acceptance"] = ack
                delivery["state"] = "nas_file_restore_acknowledged"
            except (OSError, ValueError, KeyError, TypeError) as error:
                errors.append({"receipt": path.name, "reason": type(error).__name__})
        return errors

    def _publish_cycle_status(self, result: dict) -> None:
        """Publish the same bounded observation locally and to the receiver."""
        payload = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2).encode()
        atomic_write_bytes(self.state / "status.json", payload, mode=0o600)
        atomic_write_bytes(self.transport / "tools/source-status.json", payload, mode=0o600)

    def _catalog_capacity_wait(self) -> dict | None:
        """Do not recapture history when no public wave can be allocated.

        ACK ingestion and verified transport retirement run before this check.
        This is only a conservative no-write decision, never an allocation
        admission. A resumed wave still takes the complete catalog, transport
        census and native copy checks. Retained counters keep their own clock.
        """
        free = shutil.disk_usage(self.transport).free
        reserve = self.config['reserve_bytes']
        if free - reserve > 1024**2:
            return None
        path = self.state / 'status.json'
        previous = read_json(path) if path.exists() else {}
        checked = datetime.now(timezone.utc).isoformat()
        return {**previous, 'state': 'backpressure', 'activity_at_utc': checked,
            'observed_at_utc': previous.get('observed_at_utc'),
            'all_history_backup_verified': False, 'canonical_reconstruction_verified': False,
            'source_cleanup_authorized': False, 'unpublished_sources_included': False,
            'catalog_refresh': {
                'contract': 'stockagent.backup_catalog_refresh.v1',
                'state': 'deferred_physical_capacity', 'observed_at_utc': checked,
                'source_free_bytes': free, 'source_reserve_bytes': reserve,
                'source_available_bytes': free - reserve,
                'coverage_recomputed': False,
                'catalog_identity_sha256': previous.get('catalog_identity_sha256'),
                'catalog_observed_at_utc': previous.get('catalog_observed_at_utc')}}

    def cycle(self, *, publish: bool = True) -> dict:
        self.storage_guard()
        private_schedule = self.config.get('private_history_schedule', 'inline')
        if private_schedule not in {'inline', 'independent_timer'}:
            raise SnapshotError('unknown private backup schedule')
        # A read-only catalog pass yields the publication owner, but must not
        # admit another complete controller/capture in the meantime. This
        # guard never participates in independent closed-wave publication.
        lock = self.state / "cycle.lock"
        if lock.is_symlink():
            raise SnapshotError("stream cycle lock is redirected")
        with lock.open("a") as controller:
            try:
                fcntl.flock(controller, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise SnapshotError("another source stream cycle is active") from error
            private_backup = None
            if self.config.get('private_history_backup', {}).get('enabled') and private_schedule == 'inline':
                try:
                    private_backup = self.private_history_cycle(publish=publish)
                except (OSError, ValueError, RuntimeError) as error:
                    private_backup = {'state': 'private_scope_deferred', 'error_type': type(error).__name__,
                                      'reason': str(error)[:512], 'all_project_sources_NAS_verified': False}
            result = self._cycle(publish=publish)
            if self.config.get('private_history_backup', {}).get('enabled') and private_schedule == 'independent_timer':
                status_path = self.state / 'private-status.json'
                try:
                    private_backup = json.loads(_regular(status_path).read_bytes())
                    if not isinstance(private_backup, dict):
                        raise ValueError('private backup status is not an object')
                except (OSError, ValueError):
                    private_backup = {'state': 'private_timer_status_unavailable',
                                      'all_project_sources_NAS_verified': False}
                private_backup = {**private_backup, 'schedule': 'independent_timer'}
            if private_backup is not None:
                result['private_history_backup'] = private_backup
                self._publish_cycle_status(result)
            return result

    def private_history_cycle(self, *, publish: bool = True, wait_for_owner: bool = False) -> dict:
        """Use the original owner/ledger/receiver for authorized private cold files."""
        from stockagent.data_sync.private_cold_backup import cycle
        return cycle(self, publish=publish, wait_for_owner=wait_for_owner)

    def _cycle(self, *, publish: bool) -> dict:
        started = time.perf_counter()
        lock = self.state / "owner.lock"
        if lock.is_symlink():
            raise SnapshotError("stream owner lock is redirected")
        with lock.open("a") as owner:
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise SnapshotError("another source stream owner is active") from error
            ledger = self.load_ledger()
            self.recover_publications(ledger)
            receipt_errors = self.ingest(ledger)
            automatic_recovery = {"state": "disabled"}
            if publish and self.config.get("automatic_recovery", {}).get("enabled") is True:
                from stockagent.data_sync.recovery_queue import publish_requests
                try:
                    automatic_recovery = publish_requests(self.config, self.transport, ledger)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    automatic_recovery = {"state": "request_publication_failed", "error_type": type(error).__name__}
            cache_result = self.retire_transport_cache(ledger, owner=owner) if (
                publish and not receipt_errors and self.config.get("cold_transport_cache", {}).get("enabled")) else {
                "retired_deliveries": 0, "retired_logical_bytes": 0, "errors": [],
                "deferred": bool(receipt_errors) or not publish}
            if publish and transport_placement_pending(self.config):
                self._save_ledger(ledger)
                path = self.state / 'status.json'
                previous = read_json(path) if path.exists() else {}
                checked = datetime.now(timezone.utc).isoformat()
                result = {**previous, 'state': 'waiting_for_transport_placement',
                          'observed_at_utc': previous.get('observed_at_utc'),
                          'activity_at_utc': checked,
                          'receipt_errors': receipt_errors, 'cold_transport_cache': cache_result,
                          'automatic_recovery_requests': automatic_recovery,
                          'catalog_capture_started': False, 'prior_catalog_preserved': True,
                          'auxiliary_attempts': 0, 'auxiliary_deferred_reason': 'transport_placement',
                          'all_history_backup_verified': False, 'source_cleanup_authorized': False,
                          'catalog_refresh': {
                              'contract': 'stockagent.backup_catalog_refresh.v1',
                              'state': 'deferred_transport_placement', 'observed_at_utc': checked,
                              'coverage_recomputed': False,
                              'catalog_identity_sha256': previous.get('catalog_identity_sha256'),
                              'catalog_observed_at_utc': previous.get('catalog_observed_at_utc')},
                          'complete_cycle_seconds': time.perf_counter() - started}
                self._publish_cycle_status(result)
                return result
            capacity_wait = self._catalog_capacity_wait() if publish else None
            auxiliary_error = None
            auxiliary_error_detail = None
            auxiliary_attempts = 0
            auxiliary_retry_reasons = []
            if self.config.get("auxiliary") and capacity_wait is None:
                try:
                    for attempt in range(2):
                        auxiliary_attempts += 1
                        try:
                            self.stage_auxiliary(ledger)
                            break
                        except SnapshotError as error:
                            if attempt or str(error) not in _AUXILIARY_TRANSIENT_TREE_RACES:
                                raise
                            auxiliary_retry_reasons.append(str(error))
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    auxiliary_error = type(error).__name__
                    auxiliary_error_detail = {"error_type": auxiliary_error}
                    if isinstance(error, SnapshotError):
                        auxiliary_error_detail["reason"] = str(error)[:512]
            # Catalog capture is read-only and may take much longer than an
            # immutable publication. Persist prior intents before yielding;
            # after the full pinned capture, reload the journal under its
            # original owner so concurrent publications/ACKs are never lost.
            self._save_ledger(ledger)
            if publish and capacity_wait is None:
                # Auxiliary capture can itself consume space. A second small
                # observation still avoids an inadmissible full history pass.
                capacity_wait = self._catalog_capacity_wait()
            if capacity_wait is not None:
                # Keep the owner for the small status commit; there is no new
                # catalog, allocation or notification to wait for. The normal
                # timer retries against fresh physical capacity next turn.
                if capacity_wait.get('auxiliary_error') and 'last_auxiliary_failure' not in capacity_wait:
                    capacity_wait['last_auxiliary_failure'] = {
                        'error_type': capacity_wait['auxiliary_error'],
                        'detail': capacity_wait.get('auxiliary_error_detail'),
                        'observed_at_utc': capacity_wait.get('observed_at_utc')}
                capacity_wait.update(
                    receipt_errors=receipt_errors, cold_transport_cache=cache_result,
                    automatic_recovery_requests=automatic_recovery,
                    auxiliary_error=auxiliary_error, auxiliary_error_detail=auxiliary_error_detail,
                    auxiliary_attempts=auxiliary_attempts, auxiliary_retry_reasons=auxiliary_retry_reasons,
                    auxiliary_deferred_reason='physical_capacity',
                    auxiliary_pending=bool(ledger.get('auxiliary_pending')),
                    complete_cycle_seconds=time.perf_counter() - started)
                self._publish_cycle_status(capacity_wait)
                return capacity_wait
            fcntl.flock(owner, fcntl.LOCK_UN)
            try:
                catalog = capture_catalog(self.backup_read_root(), Path(self.config["publication_catalog"]),
                                          Path(self.config["history_disposition"]) if self.config.get("history_disposition") else None,
                                          include_unreferenced_objects=self.config.get("include_unreferenced_cold_objects", False))
            finally:
                fcntl.flock(owner, fcntl.LOCK_EX)
            self.storage_guard()
            ledger = self.load_ledger()
            self.recover_publications(ledger)
            receipt_errors.extend(self.ingest(ledger))
            catalog_storage = save_catalog_snapshot(self.state, catalog)
            covered = coverage(catalog, ledger)
            state = "inspected"
            readiness_error = None
            published_waves, export_errors = [], []
            scan_errors, scan_notifications = [], []
            retries = ledger.setdefault("export_retries", {})
            # Hashing/indexing batch N runs while the source exports batch N+1.
            # The single controller still owns every ledger mutation.
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="backup-scan") as scanner:
                scans = []
                for wave_index in range(self.pipeline.get("maximum_waves_per_cycle", 1) if publish else 0):
                    # NAS ACKs can arrive while the preceding wave is copied.
                    # Reconcile on this controller before applying admission.
                    if published_waves:
                        receipt_errors.extend(self.ingest(ledger))
                    if wave_index:
                        covered = coverage(catalog, ledger)
                    try:
                        ready = read_json(self.receipts / "readiness.json")
                        independent = not self.config.get('publication_requires_receiver_ready', True)
                        validate_readiness(ready, self.config, admission_only=independent)
                        reserve = max(self.config["reserve_bytes"], ready["reserve_bytes"])
                        limits = [self.config["maximum_batch_bytes"], ready["maximum_batch_bytes"],
                                  self.staging_room(self.transport_usage())]
                        if not independent:
                            limits.extend([ready["ingress_free_bytes"] - reserve - covered["pending_bytes"],
                                ready["nas_free_bytes"] - reserve - covered["pending_bytes"],
                                self.config["maximum_pending_bytes"] - covered["pending_bytes"]])
                        limit = min(limits)
                    except (OSError, ValueError, KeyError, TypeError) as error:
                        readiness_error = type(error).__name__
                        state = "waiting_receiver_readiness"
                        break
                    if (not independent and covered["pending_delivery_count"] >= self.config["maximum_pending_deliveries"]) or limit <= 1024**2:
                        state = "backpressure"
                        break
                    deferred = {key for key, item in retries.items() if item["next_attempt_epoch"] > time.time()}
                    auxiliary = ledger.get("auxiliary_pending")
                    metadata_only = (not self.config.get("cold_object_replication_enabled", True)
                                     and self.config.get("cold_metadata_replication_enabled", False))
                    selection_covered = covered['published_file_keys'] | covered['accepted_file_keys'] | deferred
                    if metadata_only:
                        from stockagent.data_sync.nas_coverage import restic_keys
                        # Pilot reports remain provenance, not machine restore
                        # receipts. Re-deliver their small metadata for a proof.
                        selection_covered = covered['published_file_keys'] | restic_keys(ledger) | deferred
                    selection_catalog = {**catalog, "files": [r for r in catalog["files"] if r["role"] == "cold_metadata"]} if metadata_only else catalog
                    selection = [] if ((not self.config.get("cold_object_replication_enabled", True) and not metadata_only)
                        or auxiliary and auxiliary["complete_bytes"] <= limit) else select_wave(
                        selection_catalog, selection_covered,
                        maximum_bytes=limit, maximum_files=self.config["maximum_batch_files"])
                    selection_identity = catalog['identity_sha256']
                    if (not selection and metadata_only and self.config.get('metadata_backfill_catalog')
                            and not (auxiliary and auxiliary['complete_bytes'] <= limit)):
                        fixed_path = _regular(Path(self.config['metadata_backfill_catalog']))
                        if fixed_path.stat().st_size > 128 * 1024**2:
                            raise SnapshotError('fixed metadata backfill inventory is oversized')
                        raw = fixed_path.read_bytes()
                        if hashlib.sha256(raw).hexdigest() != self.config.get('metadata_backfill_sha256'):
                            raise SnapshotError('fixed metadata backfill inventory changed')
                        fixed = json.loads(raw)
                        if fixed.get('contract') != CATALOG:
                            raise SnapshotError('fixed metadata backfill contract differs')
                        selection = select_wave({**fixed, 'files': [r for r in fixed['files'] if r['role'] == 'cold_metadata']},
                            selection_covered,
                            maximum_bytes=limit, maximum_files=self.config['maximum_batch_files'])
                        selection_identity = fixed['identity_sha256']
                    if auxiliary and auxiliary["complete_bytes"] <= limit:
                        staging = safe_path(self.transport, auxiliary["staging_relative"])
                        key = auxiliary["export"]["envelope_identity_sha256"]
                        self.publish_closed(ledger, auxiliary["files"], staging, key,
                                            catalog["identity_sha256"], "code_and_control_backup")
                        ledger["last_auxiliary_delivery"] = key
                        ledger.pop("auxiliary_pending", None)
                    elif selection:
                        staging = self.transport / ".staging" / uuid.uuid4().hex
                        try:
                            result = export_incremental_delivery(self.backup_read_root(), selection, staging,
                                catalog_identity=selection_identity,
                                copy_workers=self.pipeline.get("copy_workers", 1),
                                verify_workers=self.pipeline.get("verify_workers", 1))
                        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                            if not self.pipeline:
                                raise  # Preserve the existing diagnostic interface.
                            delays = self.pipeline["retry_delays_seconds"]
                            for row in selection:
                                key = file_key(row)
                                attempt = retries.get(key, {}).get("attempts", 0) + 1
                                retries[key] = {"attempts": attempt,
                                    "next_attempt_epoch": time.time() + delays[min(attempt - 1, len(delays) - 1)],
                                    "error_type": type(error).__name__,
                                    "staging_relative": staging.relative_to(self.transport).as_posix()}
                            export_errors.append({"error_type": type(error).__name__, "files": len(selection),
                                "staging_relative": staging.relative_to(self.transport).as_posix()})
                            self._save_ledger(ledger)
                            state = "export_requeued"
                            continue
                        key = result["envelope_identity_sha256"]
                        self.publish_closed(ledger, selection, staging, key,
                                            selection_identity, "incremental_cold_objects")
                        for row in selection:
                            retries.pop(file_key(row), None)
                    else:
                        if not self.config.get("cold_object_replication_enabled", True):
                            state = "cold_replication_delegated_to_immutable_lake"
                            break
                        unsent = [r for r in catalog["files"] if file_key(r) not in (
                            covered["published_file_keys"] | covered["accepted_file_keys"])]
                        state = ("waiting_export_retry" if any(file_key(r) in deferred for r in unsent)
                            else "waiting_object_exceeds_batch_budget" if unsent
                            else "available_cold_files_complete_with_history_gaps" if catalog["missing_objects"]
                            else "caught_up_with_available_cold_files")
                        break
                    self.recover_publications(ledger)
                    self._save_ledger(ledger)
                    # Snapshot the controller-owned ledger before handing it to
                    # the scan thread; worker threads never touch that journal.
                    one = {"deliveries": {key: json.loads(json.dumps(ledger["deliveries"][key]))}}
                    scans.append(scanner.submit(self.scan_transport, one))
                    published_waves.append(key)
                    state = "published_waiting_nas_receipt"
                # Published waves and their dispatches are already durable.
                # A Syncthing scan can wait minutes; it owns no ledger mutation
                # and must not block another producer's immutable publication.
                self._save_ledger(ledger)
                fcntl.flock(owner, fcntl.LOCK_UN)
                try:
                    for future in scans:
                        try:
                            notification = future.result()
                            if notification is not None:
                                scan_notifications.append(notification)
                                if notification.get("error_type"):
                                    scan_errors.append(notification["error_type"])
                        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                            scan_errors.append(type(error).__name__)
                finally:
                    fcntl.flock(owner, fcntl.LOCK_EX)
                    # Other owners can commit while scans wait. Reload before
                    # any recovery/write so their deliveries are never lost.
                    ledger = self.load_ledger()
                    retries = ledger.setdefault("export_retries", {})
                    receipt_errors.extend(self.ingest(ledger))
                    for notification in scan_notifications:
                        self.record_transport_scan(ledger, notification)
            if published_waves:
                state = "published_waiting_nas_receipt"
            self.recover_publications(ledger)
            self._save_ledger(ledger)
            covered = coverage(catalog, ledger)
            result = {"state": state, "catalog_identity_sha256": catalog["identity_sha256"],
                "transport_capacity_contract": TRANSPORT_CAPACITY_CONTRACT,
                "cold_object_replication_enabled": self.config.get("cold_object_replication_enabled", True),
                **self.transport_usage(),
                **{k: v for k, v in covered.items() if not k.endswith("file_keys")},
                "receipt_errors": receipt_errors, "readiness_error": readiness_error,
                "auxiliary_error": auxiliary_error,
                "auxiliary_error_detail": auxiliary_error_detail,
                "auxiliary_attempts": auxiliary_attempts,
                "auxiliary_retry_reasons": auxiliary_retry_reasons,
                "pipeline": {"published_waves": published_waves, "export_errors": export_errors,
                    "scan_errors": scan_errors, "scan_notifications": scan_notifications,
                    "deferred_files": sum(
                        item["next_attempt_epoch"] > time.time() for item in retries.values()),
                    "maximum_in_flight": self.config["maximum_pending_deliveries"] if self.config.get('publication_requires_receiver_ready', True) else None,
                    "copy_workers": self.pipeline.get("copy_workers", 1),
                    "verify_workers": self.pipeline.get("verify_workers", 1)},
                "automatic_recovery_requests": automatic_recovery,
                "last_auxiliary_delivery": ledger.get("last_auxiliary_delivery"),
                "auxiliary_pending": bool(ledger.get("auxiliary_pending")),
                "auxiliary_nas_acknowledged": bool(ledger.get("last_auxiliary_delivery") and
                    ledger["deliveries"][ledger["last_auxiliary_delivery"]].get("acceptance")),
                "cold_transport_cache": cache_result,
                "automatic_pruning": False, "automatic_batch_deletion": self.config["automatic_batch_deletion"],
                "automatic_batch_deletion_scope": self.config.get("cold_transport_cache", {}).get("scope"),
                "source_cleanup_authorized": False, "complete_cycle_seconds": time.perf_counter() - started,
                "catalog_observed_at_utc": catalog["observed_at_utc"],
                "catalog_storage": catalog_storage,
                "observed_at_utc": datetime.now(timezone.utc).isoformat()}
            from stockagent.data_sync.nas_recovery_acceptance import recovery_status
            if self.config.get('immutable_archive_state_root'):
                from stockagent.data_sync.nas_coverage import combined_coverage, read_ledger
                try:
                    result['combined_nas_coverage'] = combined_coverage(catalog, ledger,
                        read_ledger(Path(self.config['immutable_archive_state_root']) / 'source-replication-ledger.json'))
                except (OSError, ValueError, KeyError, TypeError) as error:
                    result['combined_nas_coverage'] = {'state': 'unavailable', 'error_type': type(error).__name__}
            try:
                result["independent_recovery"] = recovery_status(self.config, self.receipts)
            except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
                # Semantic recovery failure is visible separately; it cannot
                # erase successful file ACKs or stop ordinary backup transport.
                result["independent_recovery"] = {"state": "invalid_recovery_evidence", "error": type(error).__name__}
            if self.config.get("automatic_recovery", {}).get("enabled") is True:
                from stockagent.data_sync.recovery_queue import queue_status
                try:
                    result["automatic_recovery"] = queue_status(self.config, self.transport, self.receipts)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    result["automatic_recovery"] = {"state": "invalid_queue_evidence", "error_type": type(error).__name__}
            if self.pipeline:
                from stockagent.data_sync.backup_relay_pipeline import receiver_status
                try:
                    result["parallel_receiver"] = receiver_status(self.config, self.transport, self.receipts)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    result["parallel_receiver"] = {"state": "invalid_pipeline_evidence", "error_type": type(error).__name__}
            index_path = self.transport / "tools/recovery-index.json"
            raw_index = json.dumps(recovery_index(catalog, ledger, self.config["repository_id"]),
                                   ensure_ascii=False, sort_keys=True, indent=2).encode()
            if not index_path.exists() or index_path.read_bytes() != raw_index:
                atomic_write_bytes(index_path, raw_index, mode=0o600)
            if (not published_waves or scan_errors
                    or any(item["state"] != "scan_request_acknowledged" for item in scan_notifications)):
                self._publish_cycle_status(result)
                # The ledger is the durable notification work list. A scan
                # timeout must not erase completed publication or hold its
                # owner during a network wait. Rebase any concurrent commits.
                self._save_ledger(ledger)
                fcntl.flock(owner, fcntl.LOCK_UN)
                try:
                    notification = self.scan_transport(ledger)
                finally:
                    fcntl.flock(owner, fcntl.LOCK_EX)
                    self.storage_guard()
                    ledger = self.load_ledger()
                if notification is not None:
                    self.record_transport_scan(ledger, notification)
                    self._save_ledger(ledger)
                    result["transport_scan"] = notification
                    if notification.get("error_type"):
                        scan_errors.append(notification["error_type"])
            # The source catalog retains its own observation clock. This
            # terminal clock includes recovery-index work and any final scan,
            # including the path where an earlier wave scan already succeeded.
            result["complete_cycle_seconds"] = time.perf_counter() - started
            result["observed_at_utc"] = datetime.now(timezone.utc).isoformat()
            self._publish_cycle_status(result)
            return result

    def notify_transport(self, *, wait_for_owner: bool = False, idle_preflight: bool = False) -> dict:
        """Retry durable notifications without exporting or recapturing cold history."""
        if type(idle_preflight) is not bool:
            raise ValueError("notification idle preflight must be explicit")
        queue_busy = self.config.get("busy_scan_notification_enabled", False)
        if type(queue_busy) is not bool:
            raise SnapshotError("busy scan notification setting must be boolean")
        self.storage_guard()
        if idle_preflight and self.config.get("syncthing_folder_id"):
            from scripts.configure_artifact_ingress_syncthing import request
            timeout = self.config.get("scan_timeout_seconds", 30)
            if type(timeout) not in {int, float} or not 1 <= timeout <= 1800:
                raise SnapshotError("backup transport scan timeout must be bounded")
            base, key, folder_id = self.paired_transport()
            status = request(base, key, "/rest/db/status", {"folder": folder_id}, timeout=min(5, timeout))
            state = status.get("state") if isinstance(status, dict) else None
            if ((state == "scanning" and (not queue_busy or self._busy_scan_unconfirmed()))
                    or not isinstance(state, str) or not state):
                # A minute retry must not parse/save the large durable ledger
                # while the original scanner is busy. Unknown pending count is
                # intentionally null; this is neither scan nor NAS acceptance.
                notification = {"contract": TRANSPORT_SCAN,
                    "state": "deferred_existing_folder_scan" if state == "scanning"
                             else "deferred_invalid_folder_state",
                    "observed_folder_state": state, "idle_preflight_only": True,
                    "pending_batch_scans": None, "batch_scan_receipts": [],
                    "tools_scan_requested": False, "NAS_backup_verified": False,
                    "observed_at_utc": datetime.now(timezone.utc).isoformat()}
                private_json(self.state / "transport-scan-status.json", notification)
                return notification
        lock = self.state / "owner.lock"
        if lock.is_symlink():
            raise SnapshotError("stream owner lock is redirected")
        with lock.open("a") as owner:
            if wait_for_owner:
                private_json(self.state / "transport-scan-status.json", {
                    "contract": TRANSPORT_SCAN, "state": "waiting_source_publication_owner",
                    "pending_batch_scans": None, "batch_scan_receipts": [], "tools_scan_requested": False,
                    "NAS_backup_verified": False, "observed_at_utc": datetime.now(timezone.utc).isoformat()})
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
            self.storage_guard()
            ledger = self.load_ledger()
            self.recover_publications(ledger)
            self._save_ledger(ledger)
            fcntl.flock(owner, fcntl.LOCK_UN)
            try:
                notification = (self.scan_transport(ledger, queue_busy_scanner=True) if queue_busy
                                else self.scan_transport(ledger))
            finally:
                fcntl.flock(owner, fcntl.LOCK_EX)
                self.storage_guard()
                ledger = self.load_ledger()
            if notification is None:
                return {"state": "transport_scan_not_configured", "NAS_backup_verified": False}
            self.record_transport_scan(ledger, notification)
            self._save_ledger(ledger)
            private_json(self.state / "transport-scan-status.json", notification)
            return notification

    def publish_recovery_requests(self, *, wait_for_owner: bool = False) -> dict:
        """Publish data-only tasks without interrupting or duplicating a cycle."""
        from stockagent.data_sync.recovery_queue import publish_requests, queue_status
        self.storage_guard()
        with (self.state / "owner.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
            self.storage_guard()
            ledger = self.load_ledger()
            errors = self.ingest(ledger)
            self._save_ledger(ledger)
            if errors:
                return {"state": "waiting_valid_file_receipts", "receipt_errors": errors}
            request_proofs = {key: delivery.get("control_recovery_request_proof")
                              for key, delivery in ledger["deliveries"].items()}
            result = publish_requests(self.config, self.transport, ledger)
            if any(delivery.get("control_recovery_request_proof") != request_proofs[key]
                   for key, delivery in ledger["deliveries"].items()):
                # The normal cycle persists these proofs before capture. This
                # separate owner entrypoint must also retain new proofs before
                # yielding and reloading for a concurrent transport scan.
                self._save_ledger(ledger)
            fcntl.flock(owner, fcntl.LOCK_UN)
            try:
                notification = self.scan_transport(ledger)
            finally:
                fcntl.flock(owner, fcntl.LOCK_EX)
                self.storage_guard()
                ledger = self.load_ledger()
            if notification is not None:
                self.record_transport_scan(ledger, notification)
                self._save_ledger(ledger)
            return {**result, "transport_scan": notification,
                    "automatic_recovery": queue_status(self.config, self.transport, self.receipts)}


    def retire_transport_cache(self, ledger: dict, *, owner=None) -> dict:
        from stockagent.data_sync.backup_transport_cache import retire, retirement_failure_reason
        candidates = [(key, d) for key, d in ledger["deliveries"].items()
                      if d.get("acceptance") and d["dispatch"]["delivery_kind"] in (
                          "incremental_cold_objects", 'private_immutable_cold_backup')
                      and d.get("cache_retirement", {}).get("state") != "retired"]
        result = {"retired_deliveries": 0, "retired_logical_bytes": 0, "errors": [], "deferred": False}
        def report(state='running', reason=None):
            if not candidates:
                return
            done = {key for key, _ in candidates
                    if ledger['deliveries'].get(key, {}).get('cache_retirement', {}).get('state') == 'retired'}
            finished = len(done) == len(candidates) and not result['errors']
            observation = {
                'contract': 'stockagent.transport_cache_retirement_progress.v1',
                'scope': 'fixed_accepted_transport_candidates',
                'generation_sha256': identity_sha256(sorted(key for key, _ in candidates)),
                'state': 'completed' if finished else state,
                'reason_code': None if finished else reason or (
                    result['errors'][0].get('reason_code') if result['errors'] else None),
                'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'completed_items': len(done), 'total_items': len(candidates),
                'completed_bytes': sum(d['dispatch']['complete_bytes'] for key, d in candidates if key in done),
                'total_bytes': sum(d['dispatch']['complete_bytes'] for _, d in candidates),
                'all_candidate_retirements_verified': finished,
                'source_objects_deleted': 0, 'NAS_snapshots_deleted': 0,
                'logical_transport_bytes_not_allocated_WSL_reclamation': True,
                'blocked_items': len(result['errors']),
            }
            try:
                # Tiny derived observation; journal/ACK proofs stay authoritative.
                private_json(self.state / 'transport-cache-progress.json', observation, compact=True)
            except (OSError, ValueError, TypeError) as error:
                result['progress_observation_error'] = type(error).__name__
        unscanned = [(key, d) for key, d in ledger["deliveries"].items()
                     if d.get("cache_retirement", {}).get("state") == "retired"
                     and not d["cache_retirement"].get("deletion_scan_requested")]
        if not candidates and not unscanned:
            return result
        report()
        from scripts.configure_artifact_ingress_syncthing import request
        base, key, folder = self.paired_transport()
        def synchronized():
            status = request(base, key, "/rest/db/status", {"folder": folder})
            peer = request(base, key, "/rest/db/completion", {"folder": folder, "device": self.config["receiver_device_id"]})
            states = {'idle'}
            if self.config.get('cold_transport_cache', {}).get('allow_scanning_when_fully_converged') is True:
                # A scanner traversing other immutable names does not invalidate
                # this batch's paired full-restore ACK. All convergence/error
                # checks and exact source/member/FD proofs still apply in retire.
                states.add('scanning')
            return not (status["state"] not in states or any(status.get(k, 0) for k in (
                "errors", "pullErrors", "needBytes", "needTotalItems", "needDeletes"))
                or status.get("watchError") or status.get("error")
                or peer["completion"] != 100 or peer["remoteState"] != "valid"
                or any(peer.get(k, 0) for k in ("needBytes", "needItems", "needDeletes")))
        if not synchronized():
            result["deferred"] = True
            report('waiting', 'transport_not_converged')
            return result
        turn_limit = self.config.get('cold_transport_cache', {}).get('maximum_retirements_per_cycle')
        for envelope, _ in candidates:
            delivery = ledger["deliveries"].get(envelope)
            if (delivery is None or not delivery.get("acceptance")
                    or delivery["dispatch"]["delivery_kind"] not in (
                        "incremental_cold_objects", "private_immutable_cold_backup")
                    or delivery.get("cache_retirement", {}).get("state") == "retired"):
                continue
            retirement_committed = False
            try:
                plan = retire(self, ledger, envelope)
                retirement_committed = True
                result["retired_deliveries"] += 1
                result["retired_logical_bytes"] += plan["complete_bytes"]
            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                result["errors"].append({"envelope": envelope, "reason": type(error).__name__,
                    'reason_code': retirement_failure_reason(error), 'detail': str(error)[:512]})
            report()
            # Each retirement is an independently journalled transaction.
            # Let a kernel-queued publisher run between transactions; then
            # reload the owner ledger so neither side can overwrite new work.
            if retirement_committed and owner is not None and _owner_has_waiters(owner):
                self._save_ledger(ledger)
                fcntl.flock(owner, fcntl.LOCK_UN)
                try:
                    time.sleep(0.01)
                finally:
                    fcntl.flock(owner, fcntl.LOCK_EX)
                    self.storage_guard()
                    fresh = self.load_ledger()
                    self.recover_publications(fresh)
                    ledger.clear()
                    ledger.update(fresh)
                    self._save_ledger(ledger)
                # A concurrent publisher may have changed the transport.
                base, key, folder = self.paired_transport()
                if not synchronized():
                    result["deferred"] = True
                    report('waiting', 'transport_not_converged')
                    return result
            if turn_limit is not None and result['retired_deliveries'] >= turn_limit:
                # Finish each exact transaction, then let this same owner copy
                # the next wave. A large accepted backlog must not postpone
                # publication until every historical cache copy is retired.
                result['yielded_to_publication_turn'] = True
                break
        unscanned = [(key, d) for key, d in ledger["deliveries"].items()
                     if d.get("cache_retirement", {}).get("state") == "retired"
                     and not d["cache_retirement"].get("deletion_scan_requested")]
        if unscanned:
            # Never queue another scan behind a busy scanner while holding the
            # publication owner. Deleted-name intents remain in the journal;
            # an idle turn issues their existing exact-subpath notification.
            if request(base, key, '/rest/db/status', {'folder': folder})['state'] != 'idle':
                result.update(deferred=True, deletion_scan_deferred=True)
                report('waiting', 'deletion_scan_pending')
                return result
            # Syncthing supports scanning deleted paths. A full folder scan
            # needlessly walks every unrelated in-flight backup while holding
            # the publication owner. Validate each immutable identity first;
            # the API accepts repeated sub parameters in a single request.
            for envelope, delivery in unscanned:
                if (not HASH.fullmatch(envelope)
                        or delivery["dispatch"]["batch_relative"] != "batch-" + envelope):
                    raise SnapshotError("invalid retired backup batch scan path")
            timeout = self.config.get("scan_timeout_seconds", 30)
            if type(timeout) not in {int, float} or not 1 <= timeout <= 1800:
                raise SnapshotError("backup transport scan timeout must be bounded")
            deadline = time.monotonic() + timeout
            result["deletion_scan_batches_acknowledged"] = 0
            for offset in range(0, len(unscanned), 64):
                group = unscanned[offset:offset + 64]
                remaining = deadline - time.monotonic()
                try:
                    if remaining < 1:
                        raise TimeoutError("retired batch scan budget exhausted")
                    query = [("folder", folder)] + [
                        ("sub", delivery["dispatch"]["batch_relative"])
                        for _, delivery in group
                    ]
                    request(base, key, "/rest/db/scan", query, method="POST", timeout=remaining)
                except OSError as error:
                    # Previously acknowledged groups remain journalled; this
                    # group and later paths remain pending without re-unlink.
                    result["deferred"] = True
                    result["scan_notification"] = _scan_error_details(error)
                    report('waiting', 'deletion_scan_pending')
                    return result
                for _, delivery in group:
                    delivery["cache_retirement"]["deletion_scan_requested"] = True
                self._save_ledger(ledger)
                result["deletion_scan_batches_acknowledged"] += len(group)
        report('blocked' if result['errors'] else 'waiting')
        return result

    def paired_transport(self) -> tuple[str, str, str]:
        from scripts.configure_artifact_ingress_syncthing import credentials, request
        base, key = credentials()
        folder_id = self.config["syncthing_folder_id"]
        folder = request(base, key, "/rest/config/folders/" + folder_id,
                         timeout=self.config.get("scan_timeout_seconds", 30))
        if (folder["path"] != str(self.transport) or folder["type"] != "sendonly" or folder["paused"]
                or {d["deviceID"] for d in folder["devices"]} != {
                    self.config["producer_device_id"], self.config["receiver_device_id"]}):
            raise SnapshotError("backup transport no longer has the paired send-only scope")
        return base, key, folder_id

    @staticmethod
    def record_transport_scan(ledger: dict, notification: dict) -> None:
        """Record only scan acknowledgement, never a NAS recovery ACK."""
        if notification.get("contract") != TRANSPORT_SCAN:
            raise SnapshotError("unknown backup transport scan notification")
        for proof in notification["batch_scan_receipts"]:
            if proof.get("state") != "scan_request_acknowledged":
                raise SnapshotError("backup batch scan has no acknowledgement")
            delivery = ledger["deliveries"].get(proof["envelope"])
            if (delivery is not None and delivery["dispatch"]["identity_sha256"] == proof["dispatch_identity_sha256"]
                    and delivery["dispatch"]["batch_relative"] == proof["batch_relative"]):
                delivery["transport_scan_notification"] = proof

    def _busy_scan_unconfirmed(self) -> bool:
        path = self.state / "transport-busy-scan-intent.json"
        if path.is_symlink():
            raise SnapshotError("busy scan intent is redirected")
        if not path.exists():
            return False
        intent = read_json(path)
        if (intent.get("contract") != "backup_busy_scan_intent_v1"
                or intent.get("state") not in {"requesting", "waiting_idle", "acknowledged", "idle_observed"}):
            raise SnapshotError("invalid busy scan intent")
        return intent["state"] in {"requesting", "waiting_idle"}

    def _busy_transport_scan(self, pending, result, base, key, folder_id, deadline):
        """Queue one closed batch behind the existing scanner, never interrupt it.

        Syncthing keeps a queued scan even if its HTTP client times out. Persist
        that uncertainty before POST and do not enqueue another until idle is
        observed. A dedicated notification lock excludes concurrent notifiers;
        the publication owner is already released throughout the network wait.
        """
        from scripts.configure_artifact_ingress_syncthing import request
        lock = self.state / "transport-busy-scan.lock"
        if lock.is_symlink():
            raise SnapshotError("busy scan lock is redirected")
        result["state"] = "deferred_existing_folder_scan"
        with lock.open("a") as owner:
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return result
            path = self.state / "transport-busy-scan-intent.json"
            unconfirmed = self._busy_scan_unconfirmed()
            if result["observed_folder_state"] != "scanning":
                if unconfirmed:
                    if result["observed_folder_state"] != "idle":
                        result["state"] = "deferred_unconfirmed_scan_waiting_idle"
                        return result
                    private_json(path, {"contract": "backup_busy_scan_intent_v1", "state": "idle_observed",
                                       "observed_at_utc": datetime.now(timezone.utc).isoformat()})
                return None
            if unconfirmed or not pending:
                return result
            remaining = deadline - time.monotonic()
            if remaining < 1:
                result["state"] = "deferred_scan_budget"
                return result
            envelope, dispatch = pending[0]
            intent = {"contract": "backup_busy_scan_intent_v1", "state": "requesting",
                      "envelope": envelope, "dispatch_identity_sha256": dispatch["identity_sha256"],
                      "batch_relative": dispatch["batch_relative"], "NAS_backup_verified": False,
                      "observed_at_utc": datetime.now(timezone.utc).isoformat()}
            private_json(path, intent)
            private_json(self.state / "transport-scan-status.json", {
                **result, "state": "waiting_existing_scanner_queued_scan", "envelope": envelope,
                "observed_at_utc": datetime.now(timezone.utc).isoformat()})
            query = [("folder", folder_id), ("sub", "tools"), ("sub", dispatch["batch_relative"])]
            try:
                request(base, key, "/rest/db/scan", query, method="POST", timeout=remaining)
            except OSError:
                private_json(path, {**intent, "state": "waiting_idle"})
                raise
            private_json(path, {**intent, "state": "acknowledged"})
            result["tools_scan_requested"] = True
            result["batch_scan_receipts"] = [{"state": "scan_request_acknowledged", "envelope": envelope,
                "batch_relative": dispatch["batch_relative"], "dispatch_identity_sha256": dispatch["identity_sha256"],
                "observed_at_utc": datetime.now(timezone.utc).isoformat()}]
            result["pending_batch_scans"] -= 1
            result["state"] = "deferred_scan_budget" if result["pending_batch_scans"] else "scan_request_acknowledged"
            return result

    def scan_transport(self, ledger: dict, *, queue_busy_scanner: bool = False) -> dict | None:
        if type(queue_busy_scanner) is not bool:
            raise SnapshotError("busy scanner scheduling must be explicit")
        if not self.config.get("syncthing_folder_id"):
            return
        from scripts.configure_artifact_ingress_syncthing import request
        timeout = self.config.get("scan_timeout_seconds", 30)
        if type(timeout) not in {int, float} or not 1 <= timeout <= 1800:
            raise SnapshotError("backup transport scan timeout must be bounded")
        pending = []
        for envelope, delivery in ledger["deliveries"].items():
            dispatch = delivery["dispatch"]
            proof = delivery.get("transport_scan_notification", {})
            if not isinstance(proof, dict):
                raise SnapshotError("invalid backup batch scan receipt")
            if (delivery.get("acceptance") or (proof.get("state") == "scan_request_acknowledged"
                    and proof.get("dispatch_identity_sha256") == dispatch["identity_sha256"]
                    and proof.get("batch_relative") == dispatch["batch_relative"]
                    and proof.get("envelope") == envelope)):
                continue
            if not HASH.fullmatch(envelope) or dispatch["batch_relative"] != "batch-" + envelope:
                raise SnapshotError("invalid immutable backup batch scan path")
            pending.append((envelope, dispatch))
        result = {"contract": TRANSPORT_SCAN, "state": "scan_request_acknowledged",
            "tools_scan_requested": False, "batch_scan_receipts": [], "pending_batch_scans": len(pending),
            "NAS_backup_verified": False, "observed_at_utc": datetime.now(timezone.utc).isoformat()}
        deadline = time.monotonic() + timeout
        try:
            base, key, folder_id = self.paired_transport()
            remaining = min(timeout, deadline - time.monotonic())
            if remaining < 1:
                result["state"] = "deferred_scan_budget"
                return result
            status = request(base, key, "/rest/db/status", {"folder": folder_id}, timeout=remaining)
            folder_state = status.get("state") if isinstance(status, dict) else None
            result["observed_folder_state"] = folder_state
            if queue_busy_scanner and isinstance(folder_state, str) and folder_state:
                queued = self._busy_transport_scan(pending, result, base, key, folder_id, deadline)
                if queued is not None:
                    return queued
                result["state"] = "scan_request_acknowledged"
            if folder_state == "scanning":
                result["state"] = "deferred_existing_folder_scan"
                return result
            if not isinstance(folder_state, str) or not folder_state:
                result["state"] = "deferred_invalid_folder_state"
                return result
            paths = [(None, "tools")] + [(envelope, dispatch["batch_relative"])
                for envelope, dispatch in pending[:MAX_TRANSPORT_SCAN_BATCHES]]
            dispatches = dict(pending)
            for envelope, path in paths:
                remaining = min(timeout, deadline - time.monotonic())
                if remaining < 1:
                    result["state"] = "deferred_scan_budget"
                    return result
                request(base, key, "/rest/db/scan", {"folder": folder_id, "sub": path},
                        method="POST", timeout=remaining)
                if envelope is None:
                    result["tools_scan_requested"] = True
                else:
                    result["batch_scan_receipts"].append({"state": "scan_request_acknowledged",
                        "envelope": envelope, "batch_relative": path,
                        "dispatch_identity_sha256": dispatches[envelope]["identity_sha256"],
                        "observed_at_utc": datetime.now(timezone.utc).isoformat()})
                    result["pending_batch_scans"] -= 1
            if result["pending_batch_scans"]:
                result["state"] = "deferred_scan_budget"
        except (OSError, ValueError) as error:
            result.update(_scan_error_details(error))
        return result

    def publish_closed(self, ledger: dict, rows: list[dict], staging: Path, envelope_id: str,
                       catalog_identity: str, kind: str, *, native_windows_io: bool = False) -> None:
        from scripts.verify_backup_delivery import verify
        verified = verify(staging, envelope_id, include_file_signatures=kind in (
                              "incremental_cold_objects", 'private_immutable_cold_backup'),
                          workers=self.pipeline.get("verify_workers", 1), native_windows_io=native_windows_io)
        batch = self.transport / ("batch-" + envelope_id)
        if batch.exists():
            raise SnapshotError("unrecorded existing batch needs explicit recovery")
        raw = (staging / "backup-envelope.json").read_bytes()
        size = verified["bytes_verified"] + len(raw) + (staging / "READY").stat().st_size
        dispatch = signed({"contract": DISPATCH, "batch_relative": batch.name,
            "producer_device_id": self.config["producer_device_id"],
            "receiver_device_id": self.config["receiver_device_id"], "repository_id": self.config["repository_id"],
            "envelope_identity_sha256": envelope_id, "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
            "complete_files": verified["files_verified"] + 2, "complete_bytes": size,
            "catalog_identity_sha256": catalog_identity, "delivery_kind": kind,
            "complete_release_in_this_delivery": False})
        ledger["deliveries"][envelope_id] = {"state": "publication_prepared", "dispatch": dispatch,
            "files": rows, "staging_relative": staging.relative_to(self.transport).as_posix(),
            "transport_signatures": verified.get("file_signatures", {})}
        self._save_ledger(ledger)
        self.storage_guard()
        os.rename(staging, batch)
        directory = os.open(self.transport, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        atomic_public(self.transport / "tools/dispatch" / (envelope_id + ".json"), dispatch)
        ledger["deliveries"][envelope_id]["state"] = "published_waiting_nas_receipt"

    def stage_auxiliary(self, ledger: dict) -> None:
        from stockagent.data_sync.backup_auxiliary import (
            export_auxiliary, working_tree_inventory, control_input, PRIVACY_FILTER_VERSION,
        )
        if ledger.get("auxiliary_pending"):
            pending = ledger["auxiliary_pending"]
            if pending["export"].get("privacy_filter_version") == PRIVACY_FILTER_VERSION:
                return
            ledger.setdefault("quarantined_auxiliary_snapshots", []).append({
                "staging_relative": pending["staging_relative"], "reason": "superseded_privacy_filter_version"})
            del ledger["auxiliary_pending"]
            ledger["auxiliary_last_capture_epoch"] = 0
        config = self.config["auxiliary"]
        latest = Path(config["control_receipt"])
        receipt_sha = hashlib.sha256(_regular(latest).read_bytes()).hexdigest()
        if (time.time() - ledger.get("auxiliary_last_capture_epoch", 0) < config["interval_seconds"]
                and receipt_sha == ledger.get("auxiliary_source_control_receipt_sha256")):
            return
        tree = working_tree_inventory(Path(config["repository_root"]), documentation=config["include_documentation"],
                                      configs=config["include_configs"])
        control, _, _ = control_input(latest)
        content_identity = identity_sha256({"code": tree["identity_sha256"], "control": control})
        if content_identity == ledger.get("last_auxiliary_content_identity"):
            ledger["auxiliary_last_capture_epoch"] = time.time()
            ledger["auxiliary_source_control_receipt_sha256"] = control["source_receipt_sha256"]
            return
        # An unchanged logical snapshot creates no payload. Do not traverse all
        # retained transport files, or reject it for capacity needed only by an
        # export that will not happen. Changed content keeps the full admission.
        if self.staging_room(self.transport_usage()) < config["maximum_bytes"] + 1024**2:
            raise SnapshotError("retain failed staging; auxiliary capacity budget reached")
        staging = self.transport / ".staging" / ("auxiliary-" + uuid.uuid4().hex)
        result = export_auxiliary(Path(config["repository_root"]), staging, control_receipt=latest,
            documentation=config["include_documentation"], configs=config["include_configs"],
            maximum_bytes=config["maximum_bytes"])
        envelope = json.loads((staging / "backup-envelope.json").read_bytes())
        ledger["auxiliary_pending"] = {"staging_relative": staging.relative_to(self.transport).as_posix(),
            "export": result, "complete_bytes": sum(r["bytes"] for r in envelope["files"]) + sum(
                (staging / n).stat().st_size for n in ("READY", "backup-envelope.json")),
            "files": [{**r, "relative": "auxiliary/" + result["content_identity_sha256"] + "/" + r["relative"],
                       "role": "auxiliary"} for r in envelope["files"]]}
        ledger["auxiliary_last_capture_epoch"] = time.time()
        # The control receipt can change between preflight and export. Record
        # the exact bytes parsed into the closed delivery, not the old probe.
        ledger["auxiliary_source_control_receipt_sha256"] = result["control_source_receipt_sha256"]
        ledger["last_auxiliary_content_identity"] = result["content_identity_sha256"]
        self._save_ledger(ledger)

    def recover_publications(self, ledger: dict) -> None:
        from scripts.verify_backup_delivery import verify
        for key, delivery in ledger["deliveries"].items():
            if delivery["state"] != "publication_prepared":
                self.finish_auxiliary_handoff(ledger, key, delivery)
                continue
            target = self.transport / delivery["dispatch"]["batch_relative"]
            staging = safe_path(self.transport, delivery["staging_relative"])
            selected = target if target.exists() else staging
            verify(selected, key)
            if hashlib.sha256((selected / "backup-envelope.json").read_bytes()).hexdigest() != delivery["dispatch"]["envelope_file_sha256"]:
                raise SnapshotError("interrupted publication envelope differs")
            if not target.exists():
                os.rename(staging, target)
            atomic_public(self.transport / "tools/dispatch" / (key + ".json"), delivery["dispatch"])
            delivery["state"] = "published_waiting_nas_receipt"
            self.finish_auxiliary_handoff(ledger, key, delivery)

    @staticmethod
    def finish_auxiliary_handoff(ledger: dict, key: str, delivery: dict) -> None:
        pending = ledger.get("auxiliary_pending")
        if (delivery["state"] in {"published_waiting_nas_receipt", "nas_file_restore_acknowledged"}
                and delivery["dispatch"].get("delivery_kind") == "code_and_control_backup" and pending
                and pending["export"]["envelope_identity_sha256"] == key):
            ledger["last_auxiliary_delivery"] = key
            del ledger["auxiliary_pending"]
