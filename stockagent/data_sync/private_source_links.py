"""Fixed, lossless dependencies for the reviewed futures source interfaces.

An absolute compatibility link is not a portable packed symlink. Preserve its
original bytes as provenance and bind its data to an already recovered release,
instead of following arbitrary links or packing a second physical source copy.
This contract is local-only and does not authorize publication or retirement.
"""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
from itertools import zip_longest
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_metadata import MetadataRows
from stockagent.data_sync.packed_snapshots import (
    _collect_entries, _entry_from_record, _entry_record, _hash_source_entry,
    _iter_inventory, _validate_inventory, fetch_packed_snapshot, fetch_packed_subtree,
    resolve_packed_snapshot_id,
)

CONTRACT = "private_futures_source_links_v1"
NAMES = ("margin_sources", "margin_repair_pending", "margin_repair_inputs")
TARGET_DATASET = "local-tw-index-futures"
METADATA_KEY = "private_source_link_bindings"


def _sqlite_rows(binding: dict) -> list[dict]:
    rows = binding.get("sqlite_states", [])
    if (not isinstance(rows, list) or rows and binding.get("sqlite_capture_basis")
            != "original_fixed_native_backup_not_current_main_wal_bytes"):
        raise SnapshotError("invalid fixed source-interface SQLite state list")
    seen = set()
    prefix = binding["subtree"] + "/"
    for row in rows:
        relative = row.get("source_relative") if isinstance(row, dict) else None
        if (not isinstance(relative, str) or not relative.startswith(prefix)
                or ".." in PurePosixPath(relative).parts or relative in seen
                or row.get("dataset") != TARGET_DATASET + "-state-" + hashlib.sha256(relative.encode()).hexdigest()[:12]
                or not isinstance(row.get("snapshot_id"), str) or not row["snapshot_id"]
                or re.fullmatch(r"[0-9a-f]{64}", str(row.get("manifest_sha256"))) is None):
            raise SnapshotError("invalid or redirected fixed source-interface SQLite binding")
        seen.add(relative)
    return rows


def _sqlite_exclusions(binding: dict) -> tuple[str, ...]:
    prefix = binding["subtree"] + "/"
    return tuple(row["source_relative"][len(prefix):] + suffix
                 for row in _sqlite_rows(binding) for suffix in ("", "-wal", "-shm", "-journal"))


def source_link_dependencies(bindings: list[dict]):
    """Yield each exact main/SQLite dependency once, never follow moving heads."""
    seen = set()
    for binding in bindings:
        for row in (binding, *_sqlite_rows(binding)):
            key = (row["dataset"], row["snapshot_id"], row["manifest_sha256"])
            if key not in seen:
                seen.add(key)
                yield row


def _link_identity(path: Path) -> dict:
    info = path.lstat()
    if not stat.S_ISLNK(info.st_mode):
        raise SnapshotError(f"reviewed source interface is no longer a symlink: {path}")
    return {"target": os.readlink(path), "mode": stat.S_IMODE(info.st_mode),
            "device": info.st_dev, "inode": info.st_ino,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def _check_private(cold: Path) -> None:
    marker = cold / "private-history.json"
    if (cold.name != "private-history" or cold.parent.name != ".local-state"
            or marker.is_symlink() or marker.resolve() != marker.absolute()):
        raise SnapshotError("source link dependencies require the original private history")
    policy = json.loads(marker.read_text())
    if any(policy.get(key) is not False for key in ("shared_export", "nas_export", "source_retirement")):
        raise SnapshotError("source link dependencies cannot grant export or source retirement")


def _read_fixed_dependency(cold: Path, row: dict, recovery: dict):
    selected = resolve_packed_snapshot_id(cold, TARGET_DATASET, row["snapshot_id"])
    if (selected.manifest_sha256 != row.get("manifest_sha256")
            or recovery.get("manifest_sha256") != selected.manifest_sha256
            or recovery.get("snapshot_id") != row["snapshot_id"]
            or recovery.get("dataset") != TARGET_DATASET
            or recovery.get("full_original_file_reconstruction_verified") is not True):
        raise SnapshotError("cross-root source dependency lacks exact full cold recovery")
    return selected


def prepare_source_links(repo: Path, entry: dict, source: Path, cold: Path,
                         operation: Path, *, progress=None) -> list[dict]:
    if entry["dataset"] != "local-tw-futures":
        return []
    if source != repo / "data_tw_futures" or source.resolve() != source:
        raise SnapshotError("futures interface source differs from the canonical root")
    _check_private(cold)
    journal = json.loads((operation / "progress.json").read_text())
    target_row = journal["datasets"].get(TARGET_DATASET, {})
    if target_row.get("state") != "verified":
        raise SnapshotError("futures interface target must finish its original migration first")
    recovery = json.loads(Path(target_row["recovery_receipt"]).read_text())
    selected = _read_fixed_dependency(cold, target_row, recovery)
    sqlite_states = json.loads(selected.manifest.get("metadata", {}).get("consistent_sqlite_bindings", "[]"))
    if not isinstance(sqlite_states, list):
        raise SnapshotError("fixed target SQLite dependencies are invalid")
    bindings = []
    with ExitStack() as stack:
        expected = {}
        for name in NAMES:
            alias = source / name
            if not alias.is_symlink():
                if alias.exists():
                    raise SnapshotError("reviewed futures interface was replaced by another source")
                continue
            target = repo / "data_tw_index_futures/preparation_sources" / name
            identity = _link_identity(alias)
            if (identity["target"] != str(target) or target.resolve(strict=True) != target
                    or not target.is_dir()):
                raise SnapshotError("futures interface redirects outside its reviewed canonical target")
            binding = {"name": name, "original_link": identity, "original_target": str(target),
                "dataset": TARGET_DATASET, "snapshot_id": selected.manifest["snapshot_id"],
                "manifest_sha256": selected.manifest_sha256,
                "subtree": "preparation_sources/" + name}
            states = [row for row in sqlite_states if isinstance(row, dict)
                      and str(row.get("source_relative", "")).startswith(binding["subtree"] + "/")]
            if states:
                binding.update(sqlite_states=states,
                    sqlite_capture_basis="original_fixed_native_backup_not_current_main_wal_bytes")
                for row in _sqlite_rows(binding):
                    fixed = resolve_packed_snapshot_id(cold, row["dataset"], row["snapshot_id"])
                    proof = json.loads((operation / "recovery" / (row["dataset"] + ".json")).read_text())
                    if (fixed.manifest_sha256 != row["manifest_sha256"]
                            or fixed.manifest.get("metadata", {}).get("role") != "private_consistent_sqlite_state"
                            or fixed.manifest["metadata"].get("source_relative") != row["source_relative"]
                            or proof.get("manifest_sha256") != row["manifest_sha256"]
                            or proof.get("snapshot_id") != row["snapshot_id"]
                            or proof.get("dataset") != row["dataset"]
                            or proof.get("full_original_file_reconstruction_verified") is not True):
                        raise SnapshotError("source-interface SQLite state lacks exact full recovery")
                    relative = row["source_relative"]
                    if any(relative + suffix not in selected.manifest["source"]["excluded_subtrees"]
                           for suffix in ("", "-wal", "-shm", "-journal")):
                        raise SnapshotError("source-interface SQLite state is not a declared fixed target exclusion")
                    current = target / relative[len(binding["subtree"]) + 1:]
                    if current.resolve(strict=True) != current or not current.is_file():
                        raise SnapshotError("source-interface native SQLite input was redirected")
            bindings.append(binding)
            expected[name] = stack.enter_context(MetadataRows(dict, dict))

        def select_rows():
            for row in _iter_inventory(cold, selected.manifest):
                for binding in bindings:
                    prefix = binding["subtree"]
                    if row["path"] == prefix or row["path"].startswith(prefix + "/"):
                        local = row["path"][len(prefix):].lstrip("/")
                        expected[binding["name"]].append({**row, "path": local or "."})
                yield row
        # Validate the entire dependency inventory, not just its selected rows.
        _validate_inventory(selected.manifest, select_rows(), retain_object_members=False)
        for binding in bindings:
            target = Path(binding["original_target"])
            rows = iter(expected[binding["name"]])
            root = next(rows, None)
            if (root is None or root["path"] != "." or root["kind"] != "directory"
                    or root["mode"] != stat.S_IMODE(target.lstat().st_mode)):
                raise SnapshotError("fixed dependency does not contain the interface directory")
            root_identity = target.lstat()
            with MetadataRows(_entry_record, _entry_from_record) as current:
                _, summary = _collect_entries(target, entry_sink=current, progress=progress,
                                              excluded_subtrees=_sqlite_exclusions(binding))
                files = logical_bytes = 0
                for actual, wanted in zip_longest(current, rows):
                    if (actual is None or wanted is None or actual.path != wanted["path"]
                            or actual.kind != wanted["kind"] or actual.mode != wanted["mode"]):
                        actual_info = (actual.path, actual.kind, oct(actual.mode)) if actual else None
                        wanted_info = (wanted["path"], wanted["kind"], oct(wanted["mode"])) if wanted else None
                        raise SnapshotError("current futures interface differs from its fixed dependency set: "
                                            f"{binding['name']}: current={actual_info!r}, fixed={wanted_info!r}")
                    if actual.kind == "file":
                        if actual.size != wanted["size"]:
                            raise SnapshotError("current futures interface differs from its fixed source bytes")
                        files += 1
                        logical_bytes += actual.size
                    elif actual.kind == "symlink" and actual.target != wanted["target"]:
                        raise SnapshotError("current futures interface contains a changed nested symlink")
                # Reject changed path/type/mode/size sets before rereading GiB
                # of already known source bytes. MetadataRows remains bounded
                # and reiterable, including when its inventory spills to disk.
                wanted_files = (row for row in expected[binding["name"]] if row["kind"] == "file")
                for actual, wanted in zip((row for row in current if row.kind == "file"), wanted_files):
                    if _hash_source_entry(target, actual) != wanted["sha256"]:
                        raise SnapshotError("current futures interface differs from its fixed source bytes")
                _, after = _collect_entries(target, retain_entries=False,
                                             excluded_subtrees=_sqlite_exclusions(binding))
                if (summary != after or not os.path.samestat(root_identity, target.lstat())
                        or _link_identity(source / binding["name"]) != binding["original_link"]):
                    raise SnapshotError("futures interface changed during full source comparison")
            binding.update(files=files, logical_bytes=logical_bytes,
                           target_stability_sha256=summary["stability_fingerprint_sha256"])
    return bindings


def guard_source_links(source: Path, bindings: list[dict]) -> None:
    for binding in bindings:
        target = Path(binding["original_target"])
        if (target.resolve(strict=True) != target
                or _link_identity(source / binding["name"]) != binding["original_link"]):
            raise SnapshotError("futures source link changed before publication commit")
        _, current = _collect_entries(target, retain_entries=False,
                                       excluded_subtrees=_sqlite_exclusions(binding))
        if current["stability_fingerprint_sha256"] != binding["target_stability_sha256"]:
            raise SnapshotError("futures source dependency changed before publication commit")


def manifest_source_links(selected) -> list[dict]:
    raw = selected.manifest.get("metadata", {}).get(METADATA_KEY)
    if raw is None:
        return []
    document = json.loads(raw)
    if (not isinstance(document, dict) or document.get("contract") != CONTRACT
            or selected.manifest["dataset"] != "local-tw-futures"
            or document.get("shared_export") is not False or document.get("nas_export") is not False
            or document.get("source_retirement") is not False):
        raise SnapshotError("invalid private source link contract")
    bindings = document.get("links")
    if not isinstance(bindings, list) or len(bindings) > len(NAMES):
        raise SnapshotError("invalid private source link list")
    seen = set()
    for row in bindings:
        if (not isinstance(row, dict) or row.get("name") not in NAMES or row["name"] in seen
                or row.get("dataset") != TARGET_DATASET
                or row.get("subtree") != "preparation_sources/" + row["name"]
                or not isinstance(row.get("original_link"), dict)
                or row.get("original_target") != row.get("original_link", {}).get("target")
                or not isinstance(row.get("original_target"), str)
                or not PurePosixPath(row["original_target"]).is_absolute()
                or not isinstance(row.get("snapshot_id"), str) or not row["snapshot_id"]
                or re.fullmatch(r"[0-9a-f]{64}", str(row.get("manifest_sha256"))) is None
                or re.fullmatch(r"[0-9a-f]{64}", str(row.get("target_stability_sha256"))) is None
                or any(type(row.get(field)) is not int or row[field] < 0
                       for field in ("files", "logical_bytes"))
                or row["name"] not in selected.manifest["source"].get("excluded_subtrees", [])):
            raise SnapshotError("invalid or unbound private futures source interface")
        seen.add(row["name"])
        _sqlite_rows(row)
    return bindings


def source_link_metadata(bindings: list[dict]) -> dict:
    return {METADATA_KEY: json.dumps({"contract": CONTRACT, "links": bindings,
        "shared_export": False, "nas_export": False, "source_retirement": False}, sort_keys=True)} if bindings else {}


def _portable_overlay(source: Path, output: Path, overrides: dict[str, Path]) -> Path:
    """Only expand directories containing state overlays; link all other trees."""
    from stockagent.data_sync.desync_snapshots import _fsync_directory
    if output.is_symlink() or (output.exists() and not output.is_dir()):
        raise SnapshotError("private source-interface overlay is occupied or redirected")
    output.mkdir(parents=True, mode=0o700, exist_ok=True)
    children = {path.name: path for path in source.iterdir()}
    for relative, target in overrides.items():
        if "/" not in relative:
            children[relative] = target
        else:
            children.setdefault(relative.split("/", 1)[0], source / relative.split("/", 1)[0])
    if set(path.name for path in output.iterdir()) - set(children):
        raise SnapshotError("retain unknown private source-interface overlay members")
    for name, target in children.items():
        nested = {key[len(name) + 1:]: value for key, value in overrides.items() if key.startswith(name + "/")}
        path = output / name
        if nested:
            _portable_overlay(target, path, nested)
        else:
            relative = os.path.relpath(target, output)
            if path.is_symlink():
                if os.readlink(path) != relative or path.resolve(strict=True) != target.resolve(strict=True):
                    raise SnapshotError("private source-interface overlay binding differs")
            elif path.exists():
                raise SnapshotError("private source-interface overlay member is occupied")
            else:
                path.symlink_to(relative, target_is_directory=target.is_dir())
    _fsync_directory(output)
    return output


def fetch_private_source_view(cold: Path, materialized: Path, selected) -> Path:
    """Build a portable local view; preserve original absolute links in its receipt.

    Component READY proves only that component. VIEW.json is committed only
    after every fixed dependency has been fully fetched/verified. No link ever
    points at a live/original path, and no duplicate payload is produced by the view.
    """
    from stockagent.data_sync.desync_snapshots import _exclusive_lock, _fsync_directory
    _check_private(cold)
    bindings = manifest_source_links(selected)
    if not bindings:
        raise SnapshotError("selected source has no compound source interfaces")
    materialized = materialized.absolute()
    if materialized.resolve() != materialized or materialized.is_relative_to(cold):
        raise SnapshotError("private source view must remain outside cold and redirected roots")
    bundle = materialized / "source-views" / selected.manifest["dataset"] / selected.manifest["snapshot_id"]
    for path in (materialized / "source-views", bundle.parent, bundle, bundle / "source", bundle / "interfaces"):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise SnapshotError("private source view ancestor was redirected or occupied")
    with _exclusive_lock(materialized / ".locks" / ("source-view-" + selected.manifest["snapshot_id"] + ".lock")):
        # Fixed IDs only. The canonical readers verify full objects/originals.
        main = fetch_packed_snapshot(cold, materialized / "components", selected)
        targets = {path.name: path for path in main.iterdir()}
        for binding in bindings:
            dependency = resolve_packed_snapshot_id(cold, binding["dataset"], binding["snapshot_id"])
            if dependency.manifest_sha256 != binding["manifest_sha256"]:
                raise SnapshotError("source view dependency manifest differs")
            component = fetch_packed_subtree(
                cold, materialized / "components", dependency, binding["subtree"])
            overrides = {}
            for state in _sqlite_rows(binding):
                fixed = resolve_packed_snapshot_id(cold, state["dataset"], state["snapshot_id"])
                if fixed.manifest_sha256 != state["manifest_sha256"]:
                    raise SnapshotError("source view SQLite dependency manifest differs")
                restored = fetch_packed_snapshot(cold, materialized / "components", fixed)
                overrides[state["source_relative"][len(binding["subtree"]) + 1:]] = restored / "snapshot.sqlite3"
            targets[binding["name"]] = (_portable_overlay(component, bundle / "interfaces" / binding["name"], overrides)
                                         if overrides else component)
        source = bundle / "source"
        source.mkdir(parents=True, mode=0o700, exist_ok=True)
        if source.resolve() != source or not source.is_dir():
            raise SnapshotError("private source view directory was redirected")
        if set(path.name for path in source.iterdir()) - set(targets):
            raise SnapshotError("retain unknown private source view members")
        for name, target in targets.items():
            path = source / name
            relative = os.path.relpath(target, source)
            if path.is_symlink():
                if os.readlink(path) != relative or path.resolve(strict=True) != target.resolve(strict=True):
                    raise SnapshotError("private source view interface changed")
            elif path.exists():
                raise SnapshotError("private source view member is occupied")
            else:
                path.symlink_to(relative, target_is_directory=target.is_dir())
        _fsync_directory(source)
        atomic_write_json(bundle / "VIEW.json", {"contract": CONTRACT,
            "snapshot_id": selected.manifest["snapshot_id"], "manifest_sha256": selected.manifest_sha256,
            "source": str(source), "links": bindings, "all_components_verified": True,
            "original_absolute_links_preserved_as_provenance": True,
            "generated_links": "portable_local_readonly_component_view",
            "source_files_deleted": 0, "payload_copies_created_by_view": 0})
        return source
