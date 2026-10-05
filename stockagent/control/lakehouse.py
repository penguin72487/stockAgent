"""Penguin authority: DuckLake dataset versions and sealed replication exports."""
from __future__ import annotations
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import time
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.immutable_replication import digest, replicate, seal, verify
from stockagent.runtime_identity import identity_sha256, runtime_identity, validate_runtime_lock


def configuration(path=Path("/etc/stockagent/lakehouse-control.json")) -> dict:
    if path.is_symlink() or path.stat().st_mode & 0o077 or path.stat().st_uid != os.geteuid():
        raise ValueError("lakehouse control needs its private locally owned configuration")
    value = json.loads(path.read_bytes())
    if value.get("schema_version") != 1:
        raise ValueError("unsupported lakehouse configuration")
    if validate_runtime_lock(json.loads(Path(value["runtime_lock"]).read_bytes()), runtime_identity()):
        raise ValueError("lakehouse Mamba runtime changed")
    return value


def guard(c: dict):
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.packed_backup import mounted_volume
    _check_d_primary_mount(Path(c["cold_root"]))
    mount = Path("/srv/stockagent-d-volume")
    if mounted_volume(mount)[2] != "D:":
        raise ValueError("immutable lake must use the enrolled D volume")
    lake = Path(c["lake_root"])
    if (not lake.is_relative_to(mount) or lake.resolve() != lake
            or any(p.is_symlink() for p in (lake, *lake.parents))):
        raise ValueError("lake root is redirected or outside D")
    if lake.exists() and lake.stat().st_dev != mount.stat().st_dev:
        raise ValueError("lake root is on a different filesystem")


def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def connect(c: dict, *, database=None, data_root=None, read_only=False):
    import duckdb
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='512MB'")
    con.execute("SET extension_directory=" + sql_text(c["extensions"]))
    con.execute("LOAD postgres")
    con.execute("LOAD ducklake")
    os.environ["PGPASSWORD"] = c["catalog_password"]
    name = database or c["catalog_database"]
    path = "ducklake:postgres:dbname=" + name + " host=127.0.0.1 port=5432 user=" + c["catalog_role"]
    options = "DATA_PATH " + sql_text(data_root or Path(c["lake_root"]) / "data") + ", DATA_INLINING_ROW_LIMIT 0"
    if read_only:
        options += ", READ_ONLY, CREATE_IF_NOT_EXISTS false"
    else:
        options += ", OVERRIDE_DATA_PATH true"
    con.execute("ATTACH " + sql_text(path) + " AS lake (" + options + ")")
    return con


def provision_extensions(c: dict):
    import duckdb
    root = Path(c["extensions"])
    root.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute("SET extension_directory=" + sql_text(root))
        con.execute("INSTALL postgres")
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
        rows = con.execute("SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name IN ('postgres','ducklake')").fetchall()
    files = {p.relative_to(root).as_posix(): digest(p) for p in root.rglob("*") if p.is_file()}
    atomic_write_json(Path(c["state_root"]) / "extension-lock.json", {"extensions": rows, "files": files})


def notify_transport(c: dict, subpaths=()):
    from scripts.configure_artifact_ingress_syncthing import credentials, request
    state = Path(c["state_root"]) / "pending-transport-scan.json"
    paths = set(subpaths)
    if state.exists():
        previous = json.loads(state.read_bytes())
        if previous.get("state") == "retry_scan":
            paths.update(previous.get("paths", ("lakehouse",)))
    if not paths:
        return
    from stockagent.data_sync.immutable_replication import safe
    for path in paths:
        safe(Path(c["transport_root"]).parent, path)
        if path != "lakehouse" and not re.fullmatch(r"lakehouse/lake-[0-9a-f]{64}", path):
            raise ValueError("transport scan is outside its fixed delivery scope")
    pending = {"state": "retry_scan", "paths": sorted(paths), "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    atomic_write_json(state, pending)
    try:
        base, key = credentials()
        if request(base, key, "/rest/system/status")["myID"] != c["producer_device_id"]:
            raise ValueError("lake source transport identity differs")
        folder = "stockagent-backup-ingress-lab203"
        if request(base, key, "/rest/db/status", {"folder": folder}, timeout=5).get("state") == "scanning":
            atomic_write_json(state, {**pending, "reason": "existing_scan_in_progress"})
            return
        for path in sorted(paths):
            request(base, key, "/rest/db/scan", {"folder": folder, "sub": path}, method="POST", timeout=30)
            paths.remove(path)
            atomic_write_json(state, {**pending, "paths": sorted(paths)})
        atomic_write_json(state, {"state": "scan_requested", "paths": [], "observed_at_utc": datetime.now(timezone.utc).isoformat()})
    except Exception as error:
        atomic_write_json(state, {**pending, "paths": sorted(paths), "error_type": type(error).__name__})


def register(c: dict) -> dict:
    from stockagent.data_sync.backup_stream import capture_catalog
    import pyarrow as pa
    started = time.perf_counter()
    guard(c)
    state = Path(c["state_root"])
    with (state / "catalog-owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Immutable manifests and publication heads are the change boundary.
        # Bounded reuse avoids 20k HDD stats each minute; a full catalog recapture
        # remains mandatory hourly and for any unavailable source/recovery.
        paths = [Path("configs/data_sync/packed_datasets.json"), Path("configs/data_sync/backup_history_disposition_20261004.json"),
                 *sorted((Path(c["cold_root"]) / "heads").glob("*/*.json")),
                 *sorted((Path(c["cold_root"]) / "manifests").glob("*/*.json"))]
        include_orphans = c.get("include_unreferenced_cold_objects", False)
        if type(include_orphans) is not bool:
            raise ValueError("unreferenced source inclusion must be an explicit boolean")
        source_signature = lambda: identity_sha256([include_orphans, [(str(p), p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) for p in paths]])
        token = source_signature()
        cache = state / "catalog-input-signature.json"
        status_path = state / "catalog-status.json"
        if cache.exists() and status_path.exists():
            recorded = json.loads(cache.read_bytes())
            age = time.time() - recorded["full_capture_epoch"]
            status = json.loads(status_path.read_bytes())
            if (recorded.get('registry_contract') == 2 and recorded["input_signature_sha256"] == token and 0 <= age <= 3600 and status["unavailable_release_count"] == 0):
                con = connect(c, read_only=True)
                try:
                    snapshot = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
                    rows = con.execute("SELECT count(*) FROM lake.dataset_releases").fetchone()[0]
                    if snapshot == status["snapshot_id"] and rows == status["release_count"]:
                        result = {**status, "observed_at_utc": datetime.now(timezone.utc).isoformat(), "changed": False,
                                  "full_capture_observed_at_utc": recorded["full_capture_observed_at_utc"],
                                  "reuse_scope": "unchanged source metadata; hourly full inventory",
                                  "complete_workflow_seconds": time.perf_counter() - started}
                        atomic_write_json(status_path, result)
                        return result
                finally:
                    con.close()
        catalog = capture_catalog(Path(c["cold_root"]), Path("configs/data_sync/packed_datasets.json"),
                                  Path("configs/data_sync/backup_history_disposition_20261004.json"),
                                  include_unreferenced_objects=include_orphans)
        if catalog["metadata_errors"]:
            raise ValueError("source catalog metadata errors must remain explicit")
        con = connect(c)
        try:
            con.execute("CALL ducklake_set_option('lake', 'parquet_compression', 'zstd')")
            con.execute("CREATE TABLE IF NOT EXISTS lake.dataset_releases (dataset VARCHAR, source_snapshot_id VARCHAR, manifest_sha256 VARCHAR, source_fingerprint_sha256 VARCHAR, manifest_relative VARCHAR, available BOOLEAN, current_head BOOLEAN, missing_objects_json VARCHAR)")
            con.execute("CREATE TABLE IF NOT EXISTS lake.source_objects (relative VARCHAR, sha256 VARCHAR, bytes BIGINT, role VARCHAR)")
            con.execute("CREATE TABLE IF NOT EXISTS lake.catalog_captures (capture_sha256 VARCHAR, observed_at_utc VARCHAR, release_count BIGINT, available_bytes BIGINT)")
            con.execute("CREATE TABLE IF NOT EXISTS lake.source_metadata (relative VARCHAR, sha256 VARCHAR, bytes BIGINT, content_utf8 VARCHAR)")
            captured = con.execute("SELECT count(*) FROM lake.catalog_captures WHERE capture_sha256=?", [catalog["identity_sha256"]]).fetchone()[0]
            metadata = []
            for row in catalog['files']:
                if row['role'] != 'cold_metadata':
                    continue
                raw = row['captured_bytes_utf8'].encode() if 'captured_bytes_utf8' in row else (Path(c['cold_root']) / row['relative']).read_bytes()
                if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
                    raise ValueError('source metadata changed after catalog capture')
                metadata.append({'relative': row['relative'], 'sha256': row['sha256'], 'bytes': row['bytes'], 'content_utf8': raw.decode('utf-8')})
            con.register('incoming_metadata', pa.Table.from_pylist(metadata))
            missing_metadata = con.execute("SELECT count(*) FROM incoming_metadata i WHERE NOT EXISTS (SELECT 1 FROM lake.source_metadata o WHERE o.relative=i.relative AND o.sha256=i.sha256)").fetchone()[0]
            changed = not captured or bool(missing_metadata)
            if changed:
                releases = [{"dataset": r["dataset"], "source_snapshot_id": r["snapshot_id"], "manifest_sha256": r["manifest_sha256"],
                             "source_fingerprint_sha256": r["source_fingerprint_sha256"], "manifest_relative": r["manifest_relative"],
                             "available": not bool(r["missing_objects"]), "current_head": r["current_head"],
                             "missing_objects_json": json.dumps(r["missing_objects"], sort_keys=True)} for r in catalog["releases"]]
                objects = [{k: r[k] for k in ("relative", "sha256", "bytes", "role")} for r in catalog["files"]]
                con.register("incoming_releases", pa.Table.from_pylist(releases))
                con.register("incoming_objects", pa.Table.from_pylist(objects))
                con.execute("BEGIN")
                con.execute("DELETE FROM lake.dataset_releases")
                con.execute("INSERT INTO lake.dataset_releases SELECT * FROM incoming_releases")
                # Source objects are a persistent unique set. Mutable heads
                # have new SHA entries; they are sealed only in version exports.
                con.execute("INSERT INTO lake.source_objects SELECT i.* FROM incoming_objects i WHERE NOT EXISTS (SELECT 1 FROM lake.source_objects o WHERE o.relative=i.relative AND o.sha256=i.sha256)")
                con.execute("INSERT INTO lake.source_metadata SELECT i.* FROM incoming_metadata i WHERE NOT EXISTS (SELECT 1 FROM lake.source_metadata o WHERE o.relative=i.relative AND o.sha256=i.sha256)")
                if not captured:
                    con.execute("INSERT INTO lake.catalog_captures VALUES (?, ?, ?, ?)", [catalog["identity_sha256"], catalog["observed_at_utc"], len(releases), sum(r["bytes"] for r in objects)])
                con.execute("COMMIT")
            snapshot = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
            current_rows = con.execute("SELECT count(*) FROM lake.dataset_releases").fetchone()[0]
            if current_rows != len(catalog["releases"]):
                raise ValueError("DuckLake dataset version coverage differs")
            columns = con.execute("SELECT count(*) FROM lake.source_objects").fetchone()[0]
            result = {"state": "registered", "observed_at_utc": datetime.now(timezone.utc).isoformat(), "snapshot_id": snapshot,
                      "registry_contract": 2,
                      "source_catalog_sha256": catalog["identity_sha256"], "release_count": current_rows,
                      "source_object_versions": columns, "available_release_count": sum(not r["missing_objects"] for r in catalog["releases"]),
                      "unavailable_release_count": sum(bool(r["missing_objects"]) for r in catalog["releases"]),
                      "changed": changed, "complete_workflow_seconds": time.perf_counter() - started}
            atomic_write_json(state / "catalog-status.json", result)
            if token == source_signature():
                atomic_write_json(cache, {"input_signature_sha256": token, "full_capture_epoch": time.time(),
                                         "registry_contract": 2,
                                         "full_capture_observed_at_utc": result["observed_at_utc"]})
            return result
        finally:
            con.close()


def export(c: dict, expected_snapshot: int) -> dict:
    guard(c)
    state = Path(c["state_root"])
    journal = state / ("export-" + str(expected_snapshot) + ".json")
    with (state / "catalog-owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if journal.exists():
            result = json.loads(journal.read_bytes())
            if result["state"] == "published":
                verify(Path(result["delivery_root"]))
                return result
            return finish_export(c, result, journal)
        con = connect(c, read_only=True)
        try:
            actual = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
            if actual != expected_snapshot:
                return {'state': 'catalog_advanced', 'snapshot_id': actual, 'delivery_identity_sha256': None}
            totals = con.execute("SELECT count(*) FROM lake.dataset_releases").fetchone()[0]
            objects = con.execute("SELECT count(*) FROM lake.source_objects").fetchone()[0]
            # Old immutable snapshots remain independently exportable during
            # a registry migration; the next register invalidates its cache.
            tables = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='lake'").fetchall()}
            base_tables = {'dataset_releases', 'source_objects', 'catalog_captures'}
            if not base_tables <= tables or tables - (base_tables | {'source_metadata'}):
                raise ValueError('unexpected lakehouse registry schema')
            row_hashes = {table: identity_sha256(con.execute("SELECT * FROM lake." + table + " ORDER BY ALL").fetchall())
                          for table in sorted(tables)}
        finally:
            con.close()
        root = Path(c["lake_root"])
        staging = root / (".export-" + uuid.uuid4().hex)
        staging.mkdir(parents=True)
        shutil.copytree(root / "data", staging / "data")
        import psycopg
        from stockagent.control.backup import logical_database_state
        from stockagent.data_sync.backup_auxiliary import CONTROL
        with psycopg.connect(host="127.0.0.1", dbname=c["catalog_database"], user=c["catalog_role"], password=c["catalog_password"]) as database:
            database.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            snapshot = database.execute("SELECT pg_export_snapshot()").fetchone()[0]
            logical = logical_database_state(database)
            result = subprocess.run(["runuser", "-u", "postgres", "--", "pg_dump", "-Fc", "--no-owner", "--no-privileges",
                                     "--snapshot", snapshot, c["catalog_database"]], capture_output=True, timeout=120)
        if result.returncode:
            raise RuntimeError("DuckLake catalog logical export failed")
        control = staging / "control"
        control.mkdir()
        (control / "archive.backup").write_bytes(result.stdout)
        atomic_write_json(control / "logical-state.json", logical)
        atomic_write_json(control / "receipt.json", {"contract": CONTROL, "same_mvcc_snapshot_as_dump": True,
            "archive": "archive.backup", "logical_state_file": "logical-state.json", "sha256": digest(control / "archive.backup"),
            "logical_state_file_sha256": digest(control / "logical-state.json"), "logical_state_identity_sha256": logical["identity_sha256"],
            "table_count": logical["table_count"], "row_count": logical["row_count"], "source_observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "bytes": len(result.stdout), "control_database_restore_verified": False})
        atomic_write_json(staging / "catalog-state.json", {"snapshot_id": expected_snapshot, "dataset_release_count": totals, "source_object_versions": objects,
                                                        "table_row_sha256": row_hashes,
                                                        "catalog_database": c["catalog_database"], "catalog_role": c["catalog_role"]})
        manifest = seal(staging, {"kind": "ducklake_catalog_and_data", "ducklake_snapshot_id": expected_snapshot,
                                 "producer_device_id": c["producer_device_id"], "receiver_device_id": c["receiver_device_id"]})
        destination = root / "releases" / ("lake-" + manifest["identity_sha256"])
        destination.parent.mkdir(exist_ok=True)
        if destination.exists():
            raise ValueError("immutable export already exists without its journal")
        transport = Path(c["transport_root"]) / destination.name
        result = {"state": "publication_prepared", "snapshot_id": expected_snapshot, "delivery_identity_sha256": manifest["identity_sha256"],
                  "delivery_root": str(destination), "transport_root": str(transport), "staging_root": str(staging)}
        atomic_write_json(journal, result)
        return finish_export(c, result, journal)


def finish_export(c: dict, result: dict, journal: Path) -> dict:
    destination = Path(result["delivery_root"])
    transport = Path(result["transport_root"])
    identity = result["delivery_identity_sha256"]
    if (destination != Path(c["lake_root"]) / "releases" / ("lake-" + identity)
            or transport != Path(c["transport_root"]) / ("lake-" + identity)):
        raise ValueError("publication journal differs from enrolled roots")
    if not destination.exists():
        staging = Path(result["staging_root"])
        if staging.parent != Path(c["lake_root"]) or not staging.name.startswith(".export-"):
            raise ValueError("publication staging is outside this owner")
        if verify(staging)["identity_sha256"] != identity:
            raise ValueError("interrupted publication staging identity differs")
        staging.rename(destination)
    if verify(destination)["identity_sha256"] != identity:
        raise ValueError("interrupted publication destination identity differs")
    proof = replicate(destination, transport, Path(c["binaries"]) / "bin/rclone")
    finished = {**result, "state": "published", "replication": proof}
    atomic_write_json(journal, finished)
    notify_transport(c, ["lakehouse/" + transport.name])
    return finished


def acceptance(c: dict, delivery_id: str) -> dict:
    from stockagent.data_sync.immutable_replication import HASH
    from stockagent.data_sync.immutable_transport_cache import validate_archive_receipt, validate_sources
    if not HASH.fullmatch(delivery_id):
        raise ValueError("invalid lifecycle delivery identity")
    path = Path(c["receipt_root"]) / ("lake-" + delivery_id + ".json")
    if not path.exists():
        pending_scan = Path(c["state_root"]) / "pending-transport-scan.json"
        if pending_scan.exists() and json.loads(pending_scan.read_bytes())["state"] == "retry_scan":
            notify_transport(c)
        return {"state": "waiting_nas_archive_acceptance", "delivery_identity_sha256": delivery_id}
    source = Path(c["lake_root"]) / "releases" / ("lake-" + delivery_id)
    if not source.exists():
        source = Path(c["transport_root"]) / ("lake-" + delivery_id)
    if source.exists():
        manifest = verify(source)
        manifest_bytes = (source / "manifest.json").read_bytes()
    else:
        cached = json.loads((Path(c["state_root"]) / ("cache-retirement-" + delivery_id + ".json")).read_bytes())
        if cached["state"] not in ("prepared", "renamed", "retired"):
            raise ValueError("missing source wave without a recovery-gated cache journal")
        manifest_bytes = cached["manifest_bytes_utf8"].encode()
        manifest = json.loads(manifest_bytes)
        if manifest["identity_sha256"] != delivery_id or identity_sha256({k: v for k, v in manifest.items() if k != "identity_sha256"}) != delivery_id:
            raise ValueError("cached archive source manifest differs")
        validate_sources(c, delivery_id, manifest, cached["primary_signatures"])
    value = validate_archive_receipt(c, delivery_id, manifest, manifest_bytes)
    return {"state": "nas_archive_file_recovery_verified", "delivery_identity_sha256": delivery_id, "receipt_identity_sha256": value["identity_sha256"],
            "catalog_semantic_restore_verified": value.get("catalog_semantic_restore_verified", False)}


def retire_accepted_transport(c, delivery_id, proof):
    from stockagent.data_sync.immutable_transport_cache import retire
    if c.get("transport_cache_retirement") is not True:
        return {"state": "disabled"}
    try:
        guard(c)
        retire(c, delivery_id, proof, apply=False)
        guard(c)
        result = retire(c, delivery_id, proof, apply=True)
        notify_transport(c, ["lakehouse/lake-" + delivery_id])
        return {k: result[k] for k in ('state', 'reclaimed_bytes', 'primary_source_files_deleted', 'nas_archive_files_deleted')}
    except (OSError, ValueError) as error:
        return {"state": "retirement_deferred", "error_type": type(error).__name__}


def _publish_source_wave(c, chosen, physical, ledger_path, ledger):
    """Durable fixed wave, including recovery between copying/sealing/enrollment."""
    from downloader.artifact_io import atomic_write_text
    from stockagent.data_sync.immutable_replication import safe
    from stockagent.data_sync.offhost_backup import copy_verified_bytes
    from stockagent.data_sync.packed_backup import object_descriptor
    journal_path = Path(c['state_root']) / 'source-wave-current.json'
    journal = json.loads(journal_path.read_bytes()) if journal_path.exists() else {}
    if journal.get('state') in ('copying', 'sealed'):
        chosen = journal['files']
        body = journal['intent']
        if identity_sha256(body) != journal['intent_sha256'] or body['files'] != chosen:
            raise ValueError('interrupted source wave intent differs')
        if any(body[k] != c[k] for k in ('producer_device_id', 'receiver_device_id')):
            raise ValueError('interrupted source wave pairing differs')
    else:
        body = {'contract': 'durable_immutable_source_wave_v1', 'files': [list(r) for r in chosen],
                'producer_device_id': c['producer_device_id'], 'receiver_device_id': c['receiver_device_id']}
        journal = {'state': 'copying', 'intent': body, 'intent_sha256': identity_sha256(body), 'files': body['files']}
        atomic_write_json(journal_path, journal, durable=True)
    chosen = journal['files']
    source_paths, descriptors = {}, {}
    for relative, sha, count in chosen:
        if object_descriptor(relative) != sha or type(count) is not int or count < 0:
            raise ValueError('fixed source wave object identity differs')
        flat = 'payload/' + Path(relative).parts[1] + '-' + sha
        source_paths[flat] = relative
        descriptors[flat] = {'sha256': sha, 'bytes': count}
    context = {'kind': 'immutable_source_objects', 'producer_device_id': c['producer_device_id'],
               'receiver_device_id': c['receiver_device_id'], 'source_object_paths': source_paths,
               'transport_layout': 'flat-sha256-v1'}
    staging = physical / '.staging' / ('lake-raw-' + journal['intent_sha256'])
    delivery_id = journal.get('delivery_identity_sha256')
    published = physical / 'lakehouse' / ('lake-' + delivery_id) if delivery_id else None
    if published and published.exists():
        manifest = verify(published)
    else:
        staging.mkdir(parents=True, exist_ok=True)
        allowed = set(descriptors) | {r + '.partial' for r in descriptors} | {'manifest.json', 'READY'}
        for path in staging.rglob('*'):
            safe(staging, path.relative_to(staging).as_posix())
            if not path.is_dir() and (not path.is_file() or path.relative_to(staging).as_posix() not in allowed):
                raise ValueError('retain unknown interrupted source-wave member')
        for flat, spec in descriptors.items():
            source = safe(Path(c['cold_root']), source_paths[flat])
            destination = safe(staging, flat)
            partial = safe(staging, flat + '.partial')
            if destination.exists():
                if destination.stat().st_size != spec['bytes'] or digest(destination) != spec['sha256']:
                    raise ValueError('completed interrupted wave member conflicts')
                if partial.exists():
                    raise ValueError('retain ambiguous completed and partial members')
                continue
            if partial.exists():
                # This name is owned by a durable intent and never published.
                # Reuse completed bytes; recopy only an interrupted short write
                # after independently proving the unchanged authoritative bytes.
                if partial.stat().st_size > spec['bytes']:
                    raise ValueError('retain oversized interrupted wave member')
                if partial.stat().st_size != spec['bytes'] or digest(partial) != spec['sha256']:
                    if source.stat().st_size != spec['bytes'] or digest(source) != spec['sha256']:
                        raise ValueError('interrupted source is no longer recoverable')
                    from stockagent.data_sync.materialized_cache import process_references_many
                    if process_references_many([partial]):
                        raise ValueError('interrupted copy still has a process owner')
                    partial.unlink()
            if not partial.exists():
                copy_verified_bytes(source, partial, expected_sha256=spec['sha256'], expected_bytes=spec['bytes'])
            partial.rename(destination)
        if (staging / 'manifest.json').exists():
            manifest = verify(staging, require_ready=False)
            if not (staging / 'READY').exists():
                atomic_write_text(staging / 'READY', manifest['identity_sha256'] + '\n', durable=True)
        else:
            manifest = seal(staging, context)
        if manifest['context'] != context or manifest['files'] != descriptors:
            raise ValueError('interrupted wave sealed identity differs from its intent')
        delivery_id = manifest['identity_sha256']
        journal.update(state='sealed', delivery_identity_sha256=delivery_id)
        atomic_write_json(journal_path, journal, durable=True)
        published = physical / 'lakehouse' / ('lake-' + delivery_id)
        published.parent.mkdir(exist_ok=True)
        staging.rename(published)
    if manifest['context'] != context or manifest['files'] != descriptors:
        raise ValueError('published wave differs from its durable intent')
    size = sum(r[2] for r in chosen)
    ledger['deliveries'].setdefault(delivery_id, {'file_keys': [r[0] + '@' + r[1] for r in chosen], 'bytes': size})
    atomic_write_json(ledger_path, ledger, durable=True)
    atomic_write_json(journal_path, {**journal, 'state': 'enrolled'}, durable=True)
    return delivery_id, published


def stage_source_wave(c: dict) -> dict:
    """One bounded append-only wave over the existing authoritative CAS.

    No source/transport removal. Archive membership is counted only from exact
    paired NAS independent-restore receipts, not from transfer completion.
    """
    from stockagent.data_sync.immutable_replication import safe
    guard(c)
    state = Path(c["state_root"])
    ledger_path = state / "source-replication-ledger.json"
    with (state / "source-replication-owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"deliveries": {}}
        status_path = state / 'source-replication-status.json'
        previous_status = json.loads(status_path.read_bytes()) if status_path.exists() else {}
        atomic_write_json(status_path, {**previous_status, 'state': 'processing_immutable_increments',
                                      'observed_at_utc': datetime.now(timezone.utc).isoformat()})
        acknowledged = set()
        pending = []
        for delivery_id, row in ledger["deliveries"].items():
            proof = row.get("nas_acceptance") or acceptance(c, delivery_id)
            if proof["state"] == "nas_archive_file_recovery_verified":
                row["nas_acceptance"] = proof
                retired = state / ('cache-retirement-' + delivery_id + '.json')
                if retired.exists():
                    value = json.loads(retired.read_bytes())
                    row['transport_retirement'] = {k: value[k] for k in
                        ('state', 'reclaimed_bytes', 'primary_source_files_deleted', 'nas_archive_files_deleted')}
                # Archive coverage must survive a crash without waiting for
                # HDD unlink/parent audits. The bounded GC owner consumes the
                # same exact proof and canonical retirement implementation.
                atomic_write_json(ledger_path, ledger, durable=True)
                acknowledged.update(row["file_keys"])
            else:
                pending.append(delivery_id)
        con = connect(c, read_only=True)
        try:
            rows = con.execute("SELECT DISTINCT relative, sha256, bytes FROM lake.source_objects WHERE role IN ('packed_object','unreferenced_cold_object') ORDER BY bytes,relative,sha256").fetchall()
        finally:
            con.close()
        total = sum(row[2] for row in rows)
        covered = sum(row[2] for row in rows if row[0] + "@" + row[1] in acknowledged)
        result = {"state": "waiting_nas_archive_acceptance" if pending else "monitoring_increments",
                  "observed_at_utc": datetime.now(timezone.utc).isoformat(), "available_object_bytes": total,
                  "nas_archive_covered_bytes": covered, "available_object_count": len(rows),
                  "nas_archive_covered_objects": len(acknowledged), "pending_delivery_ids": pending,
                  "source_deletion_enabled": False, "transport_deletion_enabled": c.get('transport_cache_retirement') is True,
                  "transport_cache_reclaimed_bytes": sum(r.get('transport_retirement',{}).get('reclaimed_bytes',0) for r in ledger['deliveries'].values()
                                                          if r.get('transport_retirement',{}).get('state') == 'retired')}
        if not pending:
            chosen = []
            size = 0
            for row in rows:
                if row[0] + "@" + row[1] in acknowledged:
                    continue
                if chosen and (len(chosen) >= 1024 or size + row[2] > 1024**3):
                    break
                chosen.append(row)
                size += row[2]
            journal_path = state / 'source-wave-current.json'
            if journal_path.exists():
                journal = json.loads(journal_path.read_bytes())
                if (journal.get('state') in ('copying', 'sealed')
                        and journal.get('delivery_identity_sha256') not in ledger['deliveries']):
                    chosen = journal['files']
                    size = sum(r[2] for r in chosen)
                elif journal.get('delivery_identity_sha256') in ledger['deliveries']:
                    atomic_write_json(journal_path, {**journal, 'state': 'enrolled'}, durable=True)
            if chosen:
                physical = Path("/srv/stockagent-d-volume/stockagent-backup-ingress-lab203")
                transport_parent = Path(c["transport_root"]).parent
                if not physical.samefile(transport_parent) or shutil.disk_usage(physical).free < size + 64 * 1024**3:
                    raise ValueError("bounded source replication lacks the enrolled D transport/capacity")
                delivery_id, destination = _publish_source_wave(c, chosen, physical, ledger_path, ledger)
                result.update(state="waiting_nas_archive_acceptance", pending_delivery_ids=[delivery_id])
                notify_transport(c, ["lakehouse/" + destination.name])
        atomic_write_json(ledger_path, ledger)
        atomic_write_json(state / "source-replication-status.json", result)
        return result


def restore_lake_delivery(root: Path, scratch: Path, *, extensions: Path, pg_bin: Path, validate_restored=None) -> dict:
    """Canonical private PG restore plus native DuckLake queries on restored files."""
    from stockagent.control.recovery import restore_control
    manifest = verify(root)
    if manifest["context"].get("kind") != "ducklake_catalog_and_data":
        raise ValueError("semantic lake restore requires a fixed catalog/data delivery")
    expected = json.loads((root / "catalog-state.json").read_bytes())
    allowed = {'dataset_releases', 'source_objects', 'catalog_captures', 'source_metadata'}
    names = set(expected['table_row_sha256'])
    if (not allowed - {'source_metadata'} <= names or names - allowed
            or type(expected['snapshot_id']) is not int
            or expected['snapshot_id'] != manifest['context']['ducklake_snapshot_id']):
        raise ValueError('restored registry schema/snapshot is outside the fixed local validator')
    def validate(options):
        import duckdb
        con = duckdb.connect()
        try:
            con.execute("SET threads=2")
            con.execute("SET memory_limit='512MB'")
            con.execute("SET extension_directory=" + sql_text(extensions))
            con.execute("LOAD postgres")
            con.execute("LOAD ducklake")
            os.environ.pop("PGPASSWORD", None)
            path = "ducklake:postgres:dbname=" + options["dbname"] + " host=" + options["host"] + " port=" + str(options["port"]) + " user=" + options["user"]
            con.execute("ATTACH " + sql_text(path) + " AS lake (DATA_PATH " + sql_text(root / "data") + ", OVERRIDE_DATA_PATH true, DATA_INLINING_ROW_LIMIT 0)")
            snapshot = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
            actual = {table: identity_sha256(con.execute("SELECT * FROM lake." + table + " ORDER BY ALL").fetchall())
                      for table in expected["table_row_sha256"]}
            if actual != expected["table_row_sha256"] or snapshot != expected["snapshot_id"]:
                raise ValueError("restored native DuckLake version/rows differ")
            result = {"catalog_semantic_restore_verified": True, "ducklake_snapshot_id": snapshot,
                      "ducklake_all_registered_rows_verified": True}
            # Only a caller's fixed local validator can extend this proof.
            # No received JSON, program or callback is ever executed.
            extra = validate_restored(con) if validate_restored else {}
            if not isinstance(extra, dict) or result.keys() & extra.keys():
                raise ValueError('additional recovery proof cannot replace native catalog checks')
            return {**result, **extra}
        finally:
            con.close()
    return restore_control(root / "control", scratch, pg_bin=pg_bin, validator=validate)
