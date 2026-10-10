"""Fixed, retryable same-filesystem moves; no payload copies or cold deletion."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import stat
import time

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.remote_legacy_return import (
    active_configuration_references_many, identity, real,
)
from scripts.deliver_vast_futures_preparation import rename_no_replace
from stockagent.storage_layout import ROLE_DIRECTORIES
from stockagent.storage_layout import relocate_configuration
from stockagent.data_sync.desync_snapshots import sha256_file


def refresh_launches(value: dict, completed: list[dict], root: Path) -> list[dict]:
    """Make mutable local launch copies from fixed completed-run configuration."""
    import yaml
    done = {r["source"] for r in completed if r["state"] == "moved"}
    moves = [r for r in value["moves"] if r["source"] in done]
    results = []
    for launch in value.get("launches", []):
        for name in ("manifest", "config", "receipt"):
            relative = Path(launch[name])
            if relative.is_absolute() or ".." in relative.parts:
                raise SnapshotError("local launch path escapes the enrolled root")
        if not launch["config"].startswith("artifacts/operations/training_launches/") or not launch["receipt"].startswith("artifacts/operations/training_launches/"):
            raise SnapshotError("local launch output is outside the operations namespace")
        source = real(root / launch["manifest"])
        if sha256_file(source) != launch["manifest_sha256"]:
            raise SnapshotError("original completed-run configuration changed")
        original = json.loads(source.read_text())
        config = relocate_configuration(original["configuration"], moves, root)
        config_path = real(root / launch["config"])
        config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # This new YAML has no dependency on retired engineering base configs.
        body = yaml.safe_dump(config, allow_unicode=True, sort_keys=False)
        temporary = config_path.with_suffix(".yaml.tmp")
        if temporary.is_symlink():
            raise SnapshotError("local config staging is redirected")
        temporary.write_text(body); os.replace(temporary, config_path)
        source_receipt = relocate_configuration(launch["source_receipt"], moves, root)
        waiting = [r["source"] for r in value["moves"] if r["source"] not in done
                   and (r["source"] in launch["source_receipt"] or r["source"] in launch["transform_cache"]
                        or any(str(root / r["source"]) in str(v) for v in original["configuration"]["data"].values()))]
        receipt = {"schema_version": 1, "state": "waiting-layout" if waiting else "ready",
                   "config": str(config_path), "config_sha256": sha256_file(config_path),
                   "source_receipt": source_receipt, "source_sha256": launch["source_sha256"],
                   "source_manifest": str(source), "source_manifest_sha256": launch["manifest_sha256"],
                   "original_configuration_fingerprint": original["configuration_fingerprint"],
                   "fold_ids": original["selected_fold_ids"],
                   "output_root": config["runner"]["output_dir"],
                   "transform_cache": relocate_configuration(launch["transform_cache"], moves, root),
                   "pending_inputs": waiting, "plan_identity": value["identity_sha256"]}
        atomic_write_json(real(root / launch["receipt"]), receipt)
        results.append({"receipt": launch["receipt"], "state": receipt["state"], "pending_inputs": waiting})
    return results


def inventory(path: Path) -> dict:
    """Include links without traversing them; bind all file/inode metadata."""
    device = path.lstat().st_dev
    rows = []
    paths = [path]
    if path.is_dir() and not path.is_symlink():
        for directory, dirs, files in os.walk(path, followlinks=False):
            paths.extend(Path(directory) / name for name in sorted(dirs + files))
    for member in paths:
        s = member.lstat()
        if s.st_dev != device:
            raise SnapshotError("layout move contains a mounted filesystem")
        kind = ("file" if stat.S_ISREG(s.st_mode) else "directory" if stat.S_ISDIR(s.st_mode)
                else "symlink" if stat.S_ISLNK(s.st_mode) else "unsupported")
        if kind == "unsupported":
            raise SnapshotError("layout move contains unsupported filesystem content")
        rows.append({"path": "." if member == path else member.relative_to(path).as_posix(),
                     "kind": kind, "dev": s.st_dev, "ino": s.st_ino, "size": s.st_size,
                     "mtime_ns": s.st_mtime_ns, "mode": s.st_mode, "links": s.st_nlink,
                     **({"target": os.readlink(member)} if kind == "symlink" else {})})
    rows.sort(key=lambda r: r["path"])
    return {"fingerprint": identity({"rows": rows}), "rows": rows,
            "files": sum(r["kind"] == "file" for r in rows),
            "logical_bytes": sum(r["size"] for r in rows if r["kind"] == "file")}


def same_relocated_tree(expected: list[dict], actual: list[dict]) -> bool:
    """Directory entry storage size can change on an overlayfs rename.

    Every name/kind/inode/mode/mtime/link and all regular-file sizes remain
    exact. Directory bookkeeping size is neither payload nor content identity.
    Keep the original full plan and the actual post-rename observation.
    """
    def normalize(rows):
        return [{k:v for k,v in row.items() if k != "size" or row["kind"] != "directory"}
                for row in rows]
    return normalize(expected) == normalize(actual)


def validate_moves(moves: list[dict]) -> None:
    if not isinstance(moves, list) or len(moves) > 1024:
        raise SnapshotError("layout requires a bounded exact move list")
    sources, destinations = [], []
    for row in moves:
        source, destination = Path(row["source"]), Path(row["destination"])
        role = row["role"]
        if (source.is_absolute() or destination.is_absolute() or ".." in source.parts
                or ".." in destination.parts or role not in ROLE_DIRECTORIES
                or source.parts[:2] != ("artifacts", "markets") or len(source.parts) < 3
                or destination.parts[:2] != tuple(Path(ROLE_DIRECTORIES[role]).parts)
                or role == "training" or row.get("action", "move") not in {"move", "remove-alias"}):
            raise SnapshotError("layout move leaves its exact source/role namespace")
        for existing in sources:
            if source == existing or source in existing.parents or existing in source.parents:
                raise SnapshotError("layout source moves overlap")
        if destination in destinations:
            raise SnapshotError("layout destination moves overlap")
        sources.append(source); destinations.append(destination)


def plan(root: Path, moves: list[dict]) -> dict:
    root = real(root)
    validate_moves(moves)
    rows = []
    for move in moves:
        source = root / move["source"]
        destination = root / move["destination"]
        # Parent paths must not be aliases. A single explicit alias removal
        # never follows the source or removes its canonical target.
        real(source.parent); real(destination)
        if not os.path.lexists(source):
            raise SnapshotError(f"layout source is absent: {move['source']}")
        if move.get("action") == "remove-alias":
            if not source.is_symlink() or source.resolve() != destination or not destination.exists():
                raise SnapshotError("layout alias does not resolve to its fixed canonical destination")
        elif os.path.lexists(destination):
            raise SnapshotError("layout destination is occupied; no overwrite")
        rows.append({**move, **inventory(source)})
    body = {"schema_version": 1, "root": str(root), "moves": rows}
    return {**body, "identity_sha256": identity(body)}


def reconcile_cache_generations(value: dict, done: dict, state: Path, *, maximum_moves: int) -> list[dict]:
    """A legacy frozen consumer can recreate a cache after its first move.

    Keep the canonical destination intact. Enroll each recreated generation
    in a private fixed child plan, with the same consumer and rename gates.
    Child journals independently resume an interrupted move; no payload is
    merged, overwritten or deleted.
    """
    caches = {m['source']: m for m in value['moves'] if m['role'] == 'cache' and m['source'] in done}
    registry_path = state / 'cache-generations.json'
    registry = json.loads(registry_path.read_text()) if registry_path.exists() else {
        'schema_version': 1, 'plan_identity': value['identity_sha256'], 'sources': {}}
    if (registry.get('schema_version') != 1 or registry.get('plan_identity') != value['identity_sha256']
            or not isinstance(registry.get('sources'), dict) or set(registry['sources']) - set(caches)):
        raise SnapshotError('cache generation registry changed its enrolled scope')
    root = real(Path(value['root'])); results = []
    for source, move in caches.items():
        record = registry['sources'].setdefault(source, {'serial': 0, 'active_serial': None})
        serial, active = record['serial'], record['active_serial']
        if (type(serial) is not int or not 0 <= serial <= 1_000_000
                or (active is not None and (type(active) is not int or active != serial))):
            raise SnapshotError('cache generation serial is invalid')
        if active is None and not os.path.lexists(root / source):
            continue
        group = real(state / 'cache-generations' / identity({'source': source}))
        group.mkdir(mode=0o700, parents=True, exist_ok=True)
        if active is None:
            active = serial + 1
            destination = Path(move['destination']).parent / (Path(move['destination']).name + '.generations') / f'{active:06d}'
            child_path = group / f'{active:06d}.plan.json'
            if not child_path.exists():
                child = plan(root, [{'source': source, 'destination': destination.as_posix(), 'role': 'cache'}])
                atomic_write_json(child_path, child)
            record.update(serial=active, active_serial=active)
            atomic_write_json(registry_path, registry)
        child_path = real(group / f'{active:06d}.plan.json')
        child = json.loads(child_path.read_text())
        destination = Path(move['destination']).parent / (Path(move['destination']).name + '.generations') / f'{active:06d}'
        if (child.get('root') != value['root'] or len(child.get('moves', [])) != 1
                or child['moves'][0]['source'] != source or child['moves'][0]['destination'] != destination.as_posix()
                or child['moves'][0]['role'] != 'cache'):
            raise SnapshotError('cache generation plan escapes its enrolled source')
        if maximum_moves <= 0:
            results.append({'source': source, 'destination': destination.as_posix(), 'serial': active,
                            'plan_identity': child['identity_sha256'], 'state': 'queued', 'pending': 1})
            continue
        child_result = apply_plan(child, group / f'{active:06d}.state', maximum_moves=max(1, maximum_moves),
                                  monitor_cache_generations=False)
        row = {'source': source, 'destination': destination.as_posix(), 'serial': active,
               'plan_identity': child['identity_sha256'], 'state': child_result['state'],
               'pending': child_result['pending'], 'rows': child_result['rows']}
        if child_result['state'] == 'complete':
            maximum_moves -= 1
            record['active_serial'] = None
            atomic_write_json(registry_path, registry)
            if os.path.lexists(root / source):
                row.update(state='queued', pending=1)
        results.append(row)
    return results


def apply_plan(value: dict, state: Path, *, maximum_moves: int = 32, monitor_cache_generations: bool = True) -> dict:
    """Retry busy paths independently, bind every successful rename to its plan."""
    body = {k: v for k, v in value.items() if k != "identity_sha256"}
    if value.get("schema_version") != 1 or identity(body) != value.get("identity_sha256"):
        raise SnapshotError("layout plan identity changed")
    validate_moves(value["moves"])
    root = real(Path(value["root"])); state = real(state)
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (state / "owner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = state / "status.json"
        previous = json.loads(journal.read_text()) if journal.exists() else {}
        if previous and previous.get("plan_identity") != value["identity_sha256"]:
            raise SnapshotError("layout owner already holds a different plan")
        done = {r["source"]: r for r in previous.get("rows", []) if r["state"] in {"moved", "alias-removed"}}
        intents = {r["source"]: r for r in previous.get("rows", []) if r["state"] == "renaming"}
        sources = [root / r["source"] for r in value["moves"] if r["source"] not in done]
        configs = active_configuration_references_many(sources, root) if sources else {}
        services = artifact_service_references(sources, root) if sources else {}
        rows = []; moved = 0
        for move in value["moves"]:
            if move["source"] in done:
                rows.append(done[move["source"]]); continue
            source, destination = root / move["source"], root / move["destination"]
            real(source.parent); real(destination)
            row = {k: move[k] for k in ("source", "destination", "role", "fingerprint", "logical_bytes", "files")}
            refs = [r for r in configs.get(str(source.resolve()), []) if not r.startswith("explicit-recovery-hold:")]
            refs += services.get(str(source.absolute()), [])
            refs += artifact_process_references(source, root / "artifacts/markets")
            if refs:
                row.update(state="waiting-consumer", references=refs[:20]); rows.append(row); continue
            if moved >= maximum_moves:
                row.update(state="queued"); rows.append(row); continue
            if not os.path.lexists(source):
                # Crash after rename, before completion receipt: verify the
                # fixed destination and resume the same intent, never guess.
                intent = intents.get(move["source"], move)
                expected_rows = intent.get("effective_rows", move["rows"])
                if move.get("action", "move") == "move" and destination.exists() and same_relocated_tree(expected_rows, inventory(destination)["rows"]):
                    row["fingerprint"] = intent["fingerprint"]
                    row.update(state="moved", recovered_interrupted_intent=True)
                    done[move["source"]] = row
                else:
                    row.update(state="source-absent-unverified")
                rows.append(row); continue
            current = inventory(source)
            minimum_idle = move.get("minimum_idle_seconds", 0)
            newest = max(r["mtime_ns"] for r in current["rows"])
            if minimum_idle and time.time_ns() - newest < minimum_idle * 1_000_000_000:
                row.update(state="waiting-owner-stability"); rows.append(row); continue
            if current["fingerprint"] != move["fingerprint"]:
                if move["role"] == "cache" or (move.get("refresh_when_idle") is True and move["role"] == "benchmark"):
                    # A mutable working cache, or the explicitly enrolled live
                    # benchmark, can gain files while busy. After its owner
                    # exits, freeze the complete new generation; preserve
                    # every file, with the initial and effective fingerprints.
                    row.update(enrollment_fingerprint=move["fingerprint"],
                               fingerprint=current["fingerprint"],
                               files=current["files"], logical_bytes=current["logical_bytes"])
                else:
                    row.update(state="source-changed"); rows.append(row); continue
            fresh = artifact_process_references(source, root / "artifacts/markets")
            fresh += [r for r in active_configuration_references_many([source], root).get(str(source.resolve()), [])
                      if not r.startswith("explicit-recovery-hold:")]
            if fresh:
                row.update(state="waiting-consumer", references=fresh[:20]); rows.append(row); continue
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            real(destination.parent)
            if source.lstat().st_dev != destination.parent.stat().st_dev:
                raise SnapshotError("layout rename would cross filesystems")
            atomic_write_json(journal, {"plan_identity": value["identity_sha256"],
                                      "rows": [*done.values(), {**row, "state": "renaming", "effective_rows":current["rows"]}]})
            if move.get("action") == "remove-alias":
                if not source.is_symlink() or source.resolve() != destination:
                    raise SnapshotError("layout alias target changed")
                source.unlink(); row.update(state="alias-removed")
            else:
                if os.path.lexists(destination):
                    raise SnapshotError("layout destination became occupied")
                rename_no_replace(source, destination)
                after = inventory(destination)
                if not same_relocated_tree(current["rows"], after["rows"]):
                    raise SnapshotError("renamed layout lost its inode/content identity")
                row.update(state="moved", destination_fingerprint=after["fingerprint"])
            for parent in (source.parent, destination.parent):
                fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
                try: os.fsync(fd)
                finally: os.close(fd)
            moved += 1; done[move["source"]] = row; rows.append(row)
            atomic_write_json(journal, {"plan_identity": value["identity_sha256"], "rows": list(done.values())})
        pending = [r for r in rows if r["state"] not in {"moved", "alias-removed"}]
        generations = reconcile_cache_generations(value, done, state, maximum_moves=maximum_moves - moved) if monitor_cache_generations else []
        pending_count = len(pending) + sum(r['pending'] for r in generations)
        result = {"schema_version": 1, "plan_identity": value["identity_sha256"],
                  "observed_at_epoch": time.time(), "state": "complete" if not pending_count else "pending",
                  "rows": rows, "pending": pending_count, "completed": len(rows)-len(pending),
                  "recreated_cache_generations": generations,
                  "payload_copies_created": 0, "cold_deleted": False, "training_results_deleted": False}
        result["launches"] = refresh_launches(value, rows, root)
        atomic_write_json(journal, result)
        return result
