from __future__ import annotations

import fcntl
import io
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from scripts import publish_data_releases as publisher
from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import (
    PackedPublicationBusy, initialize_packed_layout, publish_packed_snapshot,
    resolve_latest_packed, fetch_packed_snapshot, _source_blocks,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('cold,native_packs', [
    ('/srv/stockagent-packed', True),
    ('/srv/stockagent-packed/.local-state/private-history', True),
    ('/elsewhere/cold', False),
])
def test_recovery_uses_full_verified_native_pack_reads_only_for_original_D(
        tmp_path, monkeypatch, cold, native_packs):
    selected = SimpleNamespace(manifest_sha256='a' * 64, manifest={'source': {'files': 31}})
    monkeypatch.setattr(migration, 'resolve_packed_snapshot_id', lambda *args: selected)
    calls = []
    def verify(root, release, **options):
        calls.append((root, release, options))
        return {'independently_reconstructed_files': 31}
    monkeypatch.setattr(migration, 'verify_packed_snapshot', verify)
    receipt = tmp_path / 'recovery.json'
    proof = migration._recover(Path(cold), 'dataset', 'fixed-release', receipt, tmp_path / 'progress.json')
    assert len(calls) == 1
    root, release, options = calls[0]
    assert root == Path(cold) and release is selected
    assert options['reconstruct_all'] is True
    assert options['d_primary_native_pack_reads'] is native_packs
    assert callable(options['progress'])
    assert 'd_primary_native_blob_reads' not in options
    assert proof['full_original_file_reconstruction_verified'] is True
    assert proof['manifest_sha256'] == selected.manifest_sha256
    assert json.loads(receipt.read_text()) == proof
    # The original fixed successful receipt remains reusable without another
    # read; enabling the faster reader does not rebind accepted releases.
    assert migration._recover(Path(cold), 'dataset', 'fixed-release', receipt,
                              tmp_path / 'progress.json') == proof
    assert len(calls) == 1


def test_native_sqlite_backup_pins_wal_snapshot_while_collectors_keep_writing(tmp_path, monkeypatch):
    from contextlib import closing
    original_connect = sqlite3.connect
    database = tmp_path / "live.sqlite3"
    stage = tmp_path / "private" / "snapshot"
    with closing(original_connect(database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE data (value INTEGER, payload BLOB)")
        writer.executemany("INSERT INTO data VALUES (?,zeroblob(4096))", ((number,) for number in range(2200)))
        writer.commit()
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        wrote_during_backup, remaining = [], []
        closed_connections = []

        class ObservedConnection(sqlite3.Connection):
            def backup(self, target, **options):
                callback = options["progress"]
                def progress(status, left, total):
                    remaining.append(left)
                    if not wrote_during_backup:
                        assert left > 0, "fixture must exercise multiple backup steps"
                        writer.execute("INSERT INTO data VALUES (-1,zeroblob(4096))")
                        writer.commit()  # Must not be blocked by the backup reader.
                        wrote_during_backup.append(True)
                    callback(status, left, total)
                return super().backup(target, **{**options, "progress": progress})

            def close(self):
                super().close()
                closed_connections.append(self)

        monkeypatch.setattr(migration.sqlite3, "connect", lambda *args, **kwargs:
                            original_connect(*args, **kwargs, factory=ObservedConnection))
        proof = migration._snapshot_sqlite(database, stage, "live.sqlite3")
        assert proof["source_read_snapshot_pinned"] is True
        assert proof["destination_connections_closed"] is True
        assert len(closed_connections) == 2
        assert remaining == sorted(remaining, reverse=True) and remaining[-1] == 0
        assert writer.execute("SELECT count(*) FROM data").fetchone() == (2201,)
    assert {path.name for path in stage.iterdir()} == {"backup-intent.json", "snapshot.sqlite3", "snapshot.json"}
    with closing(original_connect("file:" + str(stage / "snapshot.sqlite3") + "?immutable=1", uri=True)) as frozen:
        assert frozen.execute("SELECT count(*),min(value) FROM data").fetchone() == (2200, 0)
        assert frozen.execute("PRAGMA quick_check").fetchall() == [("ok",)]
        assert frozen.execute("PRAGMA journal_mode").fetchone() == ("delete",)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / "configs").mkdir(parents=True)
    policy = json.loads((ROOT / "configs/columnar_storage.json").read_text())
    policy["source_versioning"]["roots"] = {}
    (repo / "configs/columnar_storage.json").write_text(json.dumps(policy))
    monkeypatch.setattr(publisher, "REPO_ROOT", repo)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test")
    entries = []
    for name, public in (("public", True), ("private", False)):
        source = repo / name
        source.mkdir()
        (source / "raw.txt").write_text(name)
        entries.append({"dataset": name, "source": str(source), "publish": public,
            "role": "source", "note": "test", "pack_buckets": 1, "loose_threshold_mib": 8,
            "file_chunk_bytes": 8 * 1024**2, "reader_contract": "packed-v2-chunks"})
    args = SimpleNamespace(sync_root=cold, migration_state=tmp_path / "journal", node_id="test",
        retry_seconds=1, maximum_passes=1, watch=False, recover_missing_base_objects=False,
        source_lock_fd=None)
    return repo, cold, entries, args, policy["source_versioning"]


def test_all_sources_including_private_have_exact_verified_versions(setup):
    repo, cold, entries, args, policy = setup
    result = migration.migrate_catalog(repo, args, entries)
    assert result["state"] == "verified", result
    assert result["source_count"] == result["verified_datasets"] == 2
    assert entries[1]["publish"] is False
    assert not (cold / "heads/private").exists()
    private = migration.private_history_root(cold, policy, initialize=False)
    assert (private / "heads/private").is_dir()
    for row in result["datasets"].values():
        proof = json.loads(Path(row["recovery_receipt"]).read_text())
        assert proof["full_original_file_reconstruction_verified"] is True
        assert proof["source_files"] == 1
    for entry in entries:
        assert (Path(entry["source"]) / "raw.txt").read_text() == entry["dataset"]


def _futures_interfaces(setup):
    from stockagent.data_sync.private_source_links import NAMES
    repo, cold, entries, args, policy = setup
    target = repo / "data_tw_index_futures"
    source = repo / "data_tw_futures"
    target.mkdir()
    source.mkdir()
    (source / "prices.csv").write_bytes(b"price,volume\n-7,1000\n")
    for name in NAMES:
        directory = target / "preparation_sources" / name
        directory.mkdir(parents=True)
        (directory / "original.csv").write_bytes((name + ",raw=-7,NULL\n").encode())
        (source / name).symlink_to(directory, target_is_directory=True)
    catalog = [{**entries[1], "dataset": "local-tw-index-futures", "source": str(target)},
               {**entries[1], "dataset": "local-tw-futures", "source": str(source)}]
    return repo, cold, catalog, args, policy


def test_futures_cross_root_interfaces_bind_fixed_versions_without_payload_copy(setup, monkeypatch):
    from stockagent.data_sync import private_source_links as links
    from scripts import packed_snapshot as cli
    repo, cold, catalog, args, policy = _futures_interfaces(setup)
    native_index = links.MetadataRows
    monkeypatch.setattr(links, "MetadataRows", lambda *a, **k: native_index(*a, **k, memory_rows=1))
    result = migration.migrate_catalog(repo, args, catalog)
    assert result["verified_datasets"] == 2, result
    private = migration.private_history_root(cold, policy)
    source = repo / "data_tw_futures"
    selected = resolve_latest_packed(private, "local-tw-futures")
    bindings = links.manifest_source_links(selected)
    assert {row["name"] for row in bindings} == set(links.NAMES)
    assert selected.manifest["source"]["files"] == 1
    assert not (cold / "heads/local-tw-futures").exists()
    row = result["datasets"]["local-tw-futures"]
    assert json.loads(Path(row["source_links_recovery_receipt"]).read_text())["all_fixed_dependencies_recovered"]
    workspace = repo / "restore"
    assert cli.main(["fetch-source-view", "local-tw-futures", "--snapshot-id", selected.manifest["snapshot_id"],
                     "--sync-root", str(private), "--materialized-root", str(workspace)]) == 0
    view = links.fetch_private_source_view(private, workspace, selected)
    assert (view / "prices.csv").read_bytes() == (source / "prices.csv").read_bytes()
    for binding in bindings:
        alias = source / binding["name"]
        assert alias.is_symlink() and str(alias.resolve()) == binding["original_target"]
        assert (view / binding["name"] / "original.csv").read_bytes() == (alias / "original.csv").read_bytes()
        assert not (view / binding["name"]).readlink().is_absolute()
        assert (view / binding["name"]).resolve() != alias.resolve(), "restore must not depend on live originals"
    receipt = json.loads((view.parent / "VIEW.json").read_text())
    assert receipt["all_components_verified"] and receipt["payload_copies_created_by_view"] == 0
    assert receipt["source_files_deleted"] == 0


def test_futures_source_view_retains_original_consistent_sqlite_dependency(setup):
    from stockagent.data_sync import private_source_links as links
    repo, cold, catalog, args, policy = _futures_interfaces(setup)
    original = repo / "data_tw_index_futures/preparation_sources/margin_repair_inputs/probe/observations.sqlite3"
    original.parent.mkdir()
    with sqlite3.connect(original) as database:
        database.execute("CREATE TABLE observations(value REAL)")
        database.executemany("INSERT INTO observations VALUES(?)", [(-7,), (None,), (2.5,)])
    result = migration.migrate_catalog(repo, args, catalog)
    assert result["verified_datasets"] == 2, result
    private = migration.private_history_root(cold, policy)
    selected = resolve_latest_packed(private, "local-tw-futures")
    bindings = links.manifest_source_links(selected)
    state = next(row for row in bindings if row["name"] == "margin_repair_inputs")["sqlite_states"][0]
    dependencies = list(links.source_link_dependencies(bindings))
    assert len(dependencies) == 2 and dependencies[1]["dataset"] == state["dataset"]
    assert json.loads((args.migration_state / "recovery" / (state["dataset"] + ".json")).read_text())[
        "full_original_file_reconstruction_verified"] is True
    # A later live update is not allowed to rewrite the old fixed DB capture.
    with sqlite3.connect(original) as database:
        database.execute("INSERT INTO observations VALUES(999)")
    view = links.fetch_private_source_view(private, repo / "restored", selected)
    restored = view / "margin_repair_inputs/probe/observations.sqlite3"
    assert restored.is_symlink() and not restored.readlink().is_absolute()
    with sqlite3.connect(restored.as_uri() + "?mode=ro", uri=True) as database:
        assert database.execute("SELECT value FROM observations").fetchall() == [(-7.0,), (None,), (2.5,)]
    assert links.fetch_private_source_view(private, repo / "restored", selected) == view


@pytest.mark.parametrize("fault", ["target_changed", "redirected_alias", "unknown_alias", "missing_cold_proof"])
def test_futures_source_interfaces_refuse_inexact_or_unreviewed_dependencies(setup, fault):
    from stockagent.data_sync import private_source_links as links
    repo, cold, catalog, args, policy = _futures_interfaces(setup)
    args.maximum_completed_sources = 1
    first = migration.migrate_catalog(repo, args, catalog)
    assert first["datasets"][links.TARGET_DATASET]["state"] == "verified"
    alias = repo / "data_tw_futures" / links.NAMES[0]
    if fault == "target_changed":
        (alias / "original.csv").write_bytes(b"changed while retaining original fixed receipt")
    elif fault == "redirected_alias":
        alias.unlink()
        alias.symlink_to(repo / "private", target_is_directory=True)
    elif fault == "unknown_alias":
        (repo / "data_tw_futures/unknown-source").symlink_to(repo / "private", target_is_directory=True)
    else:
        proof = Path(first["datasets"][links.TARGET_DATASET]["recovery_receipt"])
        document = json.loads(proof.read_text())
        document["full_original_file_reconstruction_verified"] = False
        proof.write_text(json.dumps(document))
    result = migration.migrate_catalog(repo, args, catalog)
    assert result["datasets"]["local-tw-futures"]["state"] == "waiting_retry", result
    private = migration.private_history_root(cold, policy)
    assert not (private / "heads/local-tw-futures").exists()
    assert alias.is_symlink(), "rejection must never remove the original interface"


def test_futures_dependency_guard_rechecks_target_and_link_before_commit(setup, monkeypatch):
    from stockagent.data_sync import private_source_links as links
    repo, cold, catalog, args, policy = _futures_interfaces(setup)
    args.maximum_completed_sources = 1
    migration.migrate_catalog(repo, args, catalog)
    native_publish = migration.publish_packed_snapshot
    def change_target_before_commit(root, dataset, source, **options):
        if dataset == "local-tw-futures":
            (repo / "data_tw_index_futures/preparation_sources/margin_repair_inputs/original.csv").write_bytes(b"new revision")
        return native_publish(root, dataset, source, **options)
    monkeypatch.setattr(migration, "publish_packed_snapshot", change_target_before_commit)
    result = migration.migrate_catalog(repo, args, catalog)
    assert result["datasets"]["local-tw-futures"]["state"] == "waiting_retry", result
    assert "dependency changed" in result["datasets"]["local-tw-futures"]["error"]


def test_finmind_inflight_parquet_promotion_is_not_a_missing_observation(setup, monkeypatch):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / "data_finmind"
    source.mkdir()
    entry = {**entries[1], "dataset": "finmind-free", "source": str(source)}
    folder = source / "complement/parquet/JapanStockPrice/fac11183ced3/history"
    folder.mkdir(parents=True)
    final = folder / ("a" * 64 + ".parquet")
    final.write_bytes(b"committed source values")
    staged = folder / "latest.parquet"
    staged.write_bytes(b"uncommitted writer work")
    sibling = folder / ".latest.parquet.1234.5678.tmp"
    sibling.write_bytes(b"unclosed atomic write")
    raw = source / "raw"
    raw.mkdir()
    (raw / "latest.parquet").write_bytes(b"actual latest-named observation")
    (raw / "notes.tmp").write_bytes(b"real raw tmp-named payload")
    native = packed._write_pack
    def promote_while_packing(root, selected, temporary, **options):
        staged.unlink(missing_ok=True)
        sibling.unlink(missing_ok=True)
        return native(root, selected, temporary, **options)
    monkeypatch.setattr(packed, "_write_pack", promote_while_packing)
    result = migration.migrate_catalog(repo, args, [entry])
    assert result["state"] == "verified", result
    private = migration.private_history_root(cold, policy)
    saved = resolve_latest_packed(private, "finmind-free")
    restored = fetch_packed_snapshot(private, repo / "recovered", saved)
    assert (restored / final.relative_to(source)).read_bytes() == final.read_bytes()
    assert not (restored / staged.relative_to(source)).exists()
    assert not (restored / sibling.relative_to(source)).exists()
    assert (restored / "raw/latest.parquet").read_bytes() == b"actual latest-named observation"
    assert (restored / "raw/notes.tmp").read_bytes() == b"real raw tmp-named payload"
    assert saved.manifest["source"]["omitted_uncommitted_staging_files"] == 2
    assert saved.manifest["source"]["selection"]["private_staging_file_rules"] == [
        "artifact_io_atomic_sibling_v1", "finmind_parquet_promote_v1"]


def test_finlab_sync_lock_is_frozen_without_dropping_its_bytes(setup, monkeypatch):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]["source"])
    lock = source / ".sync.lock"
    lock.write_bytes(b"original lock state")
    native = packed._write_pack
    def new_sync_owner(root, selected, temporary, **options):
        lock.write_bytes(b"new sync owner")
        return native(root, selected, temporary, **options)
    monkeypatch.setattr(packed, "_write_pack", new_sync_owner)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    assert result["state"] == "verified", result
    private = migration.private_history_root(cold, policy)
    saved = resolve_latest_packed(private, entries[1]["dataset"])
    restored = fetch_packed_snapshot(private, repo / "recovered", saved)
    assert (restored / ".sync.lock").read_bytes() == b"original lock state"
    assert lock.read_bytes() == b"new sync owner"
    assert ".sync.lock" in saved.manifest["source"]["selection"]["frozen_control_files"]


def test_resume_does_not_republish_or_rehash_completed_sources(setup, monkeypatch):
    repo, cold, entries, args, _ = setup
    first = migration.migrate_catalog(repo, args, entries)
    monkeypatch.setattr(publisher, "_publish_entry", lambda *a: pytest.fail("completed source republished"))
    monkeypatch.setattr(migration, "_private_publish", lambda *a: pytest.fail("completed source republished"))
    again = migration.migrate_catalog(repo, args, entries)
    assert again["datasets"] == first["datasets"]


def test_plan_change_rejects_incompatible_resume(setup):
    repo, _, entries, args, _ = setup
    migration.migrate_catalog(repo, args, entries)
    entries[0]["publish"] = False
    with pytest.raises(SnapshotError, match="source/policy changed"):
        migration.migrate_catalog(repo, args, entries)


@pytest.mark.parametrize("fault", ["ignore", "permissions", "symlink", "policy", "marker"])
def test_private_history_fails_closed_on_privacy_drift(setup, fault):
    _, cold, _, _, policy = setup
    private = migration.private_history_root(cold, policy)
    if fault == "ignore":
        (cold / ".stignore").write_text("")
    elif fault == "permissions":
        private.chmod(0o755)
    elif fault == "symlink":
        (private / "private-history.json").unlink()
        private.rmdir()
        private.symlink_to(cold / ".local-state", target_is_directory=True)
    elif fault == "policy":
        policy["private_history"]["nas_export"] = True
    else:
        (private / "private-history.json").write_text("{}")
    with pytest.raises(SnapshotError):
        migration.private_history_root(cold, policy)


def test_busy_shared_publisher_does_not_block_private_progress(setup):
    repo, cold, entries, args, _ = setup
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    with owner.open("a+b") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = migration.migrate_catalog(repo, args, entries)
    assert result["datasets"]["public"]["state"] == "waiting_retry"
    assert result["datasets"]["public"]["error_type"] == "PackedPublicationBusy"
    assert result["datasets"]["private"]["state"] == "verified"


def test_sqlite_is_consistent_snapshot_not_raw_wal_copy(setup):
    repo, cold, entries, args, policy = setup
    database = Path(entries[1]["source"]) / "state.sqlite3"
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE observations(value)")
        db.execute("INSERT INTO observations VALUES(23)")
        db.commit()
        result = migration.migrate_catalog(repo, args, entries)
    assert result["state"] == "verified", result
    row = result["datasets"]["private"]
    assert row["source_files"] == 1
    snapshot = row["publication"]["sqlite_states"][0]
    assert snapshot["consistent_state"]["raw_main_wal_shm_byte_copy"] is False
    with sqlite3.connect(Path(snapshot["retained_snapshot_staging"]) / "snapshot.sqlite3") as db:
        assert db.execute("SELECT value FROM observations").fetchall() == [(23,)]
    private = migration.private_history_root(cold, policy)
    assert resolve_latest_packed(private, snapshot["dataset"]).manifest["schema_version"] == 2


def test_nonblocking_packed_lock_retains_same_owner_inode(setup):
    _, cold, entries, _, _ = setup
    lock = cold / ".local-state/locks/publish-retention-global.lock"
    with lock.open("a+b") as stream:
        before = lock.stat().st_ino
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(PackedPublicationBusy):
            publish_packed_snapshot(cold, "test", Path(entries[0]["source"]),
                                    publication_lock_nonblocking=True)
        assert lock.stat().st_ino == before


def test_active_private_collector_is_not_an_impossible_idle_gate(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    entries[1]["active_process_substrings"] = ["always-on-provider"]
    monkeypatch.setattr(publisher, "_running_commands", lambda: [(123, "always-on-provider")])
    result = migration.migrate_catalog(repo, args, entries)
    row = result["datasets"]["private"]
    assert row["state"] == "verified", row
    assert row["publication"]["source_status"]["active_blockers"]
    assert row["publication"]["consistency"] == "fixed_observed_files_not_atomic_workspace"


def test_private_cohort_freezes_controls_and_allows_new_files_not_selected_mutations(setup, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]["source"])
    (source / "status.json").write_text('{"cursor": 1}')
    original = packed._write_pack
    def while_hashing(root, selected, temporary, **options):
        (source / "arrived-later.txt").write_text("next version")
        (source / "status.json").write_text('{"cursor": 2}')
        return original(root, selected, temporary, **options)
    monkeypatch.setattr(packed, "_write_pack", while_hashing)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    row = result["datasets"]["private"]
    assert row["state"] == "verified", row
    private = migration.private_history_root(cold, policy)
    release = resolve_latest_packed(private, "private")
    restored = fetch_packed_snapshot(private, repo / "restored", release)
    assert (restored / "status.json").read_text() == '{"cursor": 1}'
    assert not (restored / "arrived-later.txt").exists()
    assert release.manifest["source"]["selection"]["private_observation_cohort"] is True


def test_private_cohort_never_accepts_selected_payload_change(setup, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    repo, _, entries, args, _ = setup
    original = packed._write_pack
    def mutate(root, selected, destination, **options):
        (root / "raw.txt").write_text("in-place corruption")
        return original(root, selected, destination, **options)
    monkeypatch.setattr(packed, "_write_pack", mutate)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    assert result["datasets"]["private"]["state"] == "waiting_retry"
    assert "changed while packing" in result["datasets"]["private"]["error"]


def test_observation_cohort_cannot_be_enabled_for_shared_store(setup):
    _, cold, entries, _, _ = setup
    with pytest.raises(SnapshotError, match="ignored private history"):
        publish_packed_snapshot(cold, "public", Path(entries[0]["source"]),
                                private_observation_cohort=True)


def test_recursive_mutable_controls_do_not_restart_the_entire_baseline(setup, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]['source'])
    controls = ['complement/receipts/EuropeStockPrice/id/history.json',
                'intraday/active_run.json', 'api_trial_v1/worker_status.json',
                'latest/etherscan_ethereum_state.json']
    for name in controls:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"value":1}')
    original = packed._write_pack
    def while_hashing(root, selected, temporary, **options):
        for name in controls:
            (source / name).write_text('{"value":2}')
        return original(root, selected, temporary, **options)
    monkeypatch.setattr(packed, '_write_pack', while_hashing)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert all((restored / name).read_text() == '{"value":1}' for name in controls)
    assert all((source / name).read_text() == '{"value":2}' for name in controls)


def test_control_freeze_accepts_atomic_replace_but_captures_the_original_fd(tmp_path, monkeypatch):
    import os
    source, stage = tmp_path / 'source', tmp_path / 'controls' / 'stage'
    source.mkdir()
    original = source / 'status.json'
    original.write_text('{"value":1}')
    replacement = tmp_path / 'replacement.json'
    replacement.write_text('{"value":2}')
    native, replaced = os.fstat, []
    def replace_after_open(descriptor):
        result = native(descriptor)
        if not replaced and os.readlink(f'/proc/self/fd/{descriptor}') == str(original):
            os.replace(replacement, original)
            replaced.append(True)
        return result
    monkeypatch.setattr(migration.os, 'fstat', replace_after_open)
    result = migration._freeze_controls(source, stage, [])
    assert replaced
    assert Path(result['status.json']['path']).read_text() == '{"value":1}'
    assert original.read_text() == '{"value":2}'
    assert migration._freeze_controls(source, stage, []) == result


def test_control_census_reports_progress_without_stating_immutable_payloads(tmp_path, monkeypatch):
    source, staging = tmp_path / 'source', tmp_path / 'controls/capture'
    source.mkdir()
    for index in range(2048):
        (source / f'{index}.parquet').write_bytes(b'raw')
    (source / 'status.json').write_text('{"value":1}')
    (source / '.sync.lock').write_bytes(b'lock state')
    native = Path.lstat
    def reject_unneeded_stat(path, *args, **options):
        if path.parent == source and path.suffix == '.parquet':
            pytest.fail('control discovery stated an immutable payload')
        return native(path, *args, **options)
    monkeypatch.setattr(Path, 'lstat', reject_unneeded_stat)
    ticks = iter(range(0, 1000, 6))
    monkeypatch.setattr(migration.time, 'monotonic', lambda: next(ticks))
    progress = []
    controls = migration._freeze_controls(source, staging, [], progress=progress.append)
    assert set(controls) == {'status.json', '.sync.lock'}
    assert len(progress) >= 3 and progress[-1]['files_examined'] == 2050
    assert progress[-1]['controls_frozen'] == 2
    assert progress[-1]['control_bytes_frozen'] == sum(row['bytes'] for row in controls.values())
    assert all(row['files_total'] is None and row['acceptance'] is False for row in progress)


def test_sqlite_discovery_avoids_every_non_database_stat_and_reports_progress(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    for index in range(2048):
        (source / f'{index}.parquet').write_bytes(b'raw')
    (source / 'history.SQLITE3').write_bytes(b'SQLite format 3\x00')
    (source / 'not-a-database.db').write_bytes(b'raw')
    (source / 'link.sqlite').symlink_to('history.SQLITE3')
    native = Path.lstat
    def reject_payload_stat(path, *args, **options):
        if path.parent == source and path.suffix == '.parquet':
            pytest.fail('database discovery stated an unrelated payload')
        return native(path, *args, **options)
    monkeypatch.setattr(Path, 'lstat', reject_payload_stat)
    ticks = iter(range(0, 1000, 6))
    monkeypatch.setattr(migration.time, 'monotonic', lambda: next(ticks))
    progress = []
    assert migration._sqlite_databases(source, [], progress=progress.append) == [source / 'history.SQLITE3']
    assert len(progress) >= 3 and progress[-1]['files_examined'] == 2051
    assert progress[-1]['sqlite_states_discovered'] == 1
    assert all(row['files_total'] is None and row['acceptance'] is False for row in progress)


@pytest.mark.parametrize('report_directory', ['shioaji', 'shioaji_1m'])
@pytest.mark.parametrize('old_scope', ['recursive_mutable_controls_v2', 'recursive_mutable_controls_v3'])
def test_named_shioaji_report_scope_upgrade_keeps_previous_captures_not_raw_csv(
        setup, monkeypatch, report_directory, old_scope):
    import stockagent.data_sync.packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]['source'])
    (source / report_directory).mkdir()
    logical_report = report_directory + '/download_report.csv'
    report = source / logical_report
    report.write_text('symbol,status\n2330,complete\n')
    (source / 'raw.csv').write_text('price,volume_shares\n123,1000\n')
    (source / 'status.json').write_text('{"old_observation":true}')
    stage = args.migration_state / 'controls/upgrade-fixture'
    controls = migration._freeze_controls(source, stage, [], progress=None)
    old = {name: row for name, row in controls.items() if name != logical_report}
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': old_scope, 'source': str(source), 'excluded': []}))
    (source / 'status.json').write_text('{"new_observation":true}')
    upgraded = migration._freeze_controls(source, stage, [], existing=old)
    assert set(upgraded) == {'status.json', logical_report}
    assert Path(upgraded['status.json']['path']).read_text() == '{"old_observation":true}'
    assert json.loads(stage.with_suffix('.intent.json').read_text())['contract'] == migration.CONTROL_SCOPE
    original = packed._write_pack
    def while_packing(root, selected, target, **options):
        report.write_text('symbol,status\n2330,newer\n')
        return original(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', while_packing)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert (restored / logical_report).read_text() == 'symbol,status\n2330,complete\n'
    assert (restored / 'raw.csv').read_text() == (source / 'raw.csv').read_text()


@pytest.mark.parametrize('source_name', ['shioaji', 'shioaji_1m'])
def test_report_freeze_handles_the_named_source_root_not_unrelated_csv(tmp_path, source_name):
    source = tmp_path / source_name
    (source / 'unrelated').mkdir(parents=True)
    (source / 'download_report.csv').write_text('symbol,status\n2330,complete\n')
    (source / 'unrelated/download_report.csv').write_text('raw,value\n1,2\n')
    result = migration._freeze_controls(source, tmp_path / 'controls/captured', [])
    assert set(result) == {'download_report.csv'}


@pytest.mark.parametrize('source_name,relative', [
    ('data_binance', '1m/download_report.csv'),
    ('data_bybit', '1m/download_report.csv'),
    ('data_okx', 'daily/download_report.csv'),
    ('data_bybit', '15m/.download.lock'),
    ('data_tw_futures', 'shioaji_history/FT1R1/download.lock'),
    ('data_tw_index_futures', 'shioaji_history/TXFR1/download.lock'),
])
def test_native_reports_and_epoch_bytes_survive_live_updates_without_replacing_locks(
        setup, monkeypatch, source_name, relative):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / source_name
    payload = source / relative
    payload.parent.mkdir(parents=True)
    raw = b'original operational observation\n'
    payload.write_bytes(raw)
    original_inode = payload.stat().st_ino
    (source / 'raw.csv').write_bytes(b'original prices,raw=-7,shares\n')
    native = packed._write_pack
    def update_control(root, selected, target, **options):
        payload.write_bytes(b'new live operational observation\n')
        return native(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', update_control)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert (restored / relative).read_bytes() == raw
    assert payload.read_bytes() == b'new live operational observation\n'
    assert payload.stat().st_ino == original_inode
    assert (restored / 'raw.csv').read_bytes() == (source / 'raw.csv').read_bytes()


@pytest.mark.parametrize('old_scope', [f'recursive_mutable_controls_v{version}' for version in range(3, 7)])
@pytest.mark.parametrize('source_name,relative,raw', [
    ('data_bybit', '1m/download_report.csv', b'symbol,status\nBTCUSDT,complete\n'),
    ('data_okx', 'daily/.download.lock', b'original lock owner bytes\n'),
    ('data_tw_futures', 'shioaji_history/FT1R1/download.lock', b'{"nonce":"original"}\n'),
    ('data_tw_public', 'features/tw_public_stock_daily.parquet', b'closed original feature table'),
])
def test_v7_named_delta_upgrade_does_not_rescan_raw_or_replace_older_captures(
        tmp_path, monkeypatch, old_scope, source_name, relative, raw):
    source = tmp_path / source_name
    source.mkdir()
    (source / 'status.json').write_text('{"cursor":1}')
    stage = tmp_path / 'controls/captured'
    captured = migration._freeze_controls(source, stage, [])
    (source / 'status.json').write_text('{"cursor":2}')
    payload = source / relative
    payload.parent.mkdir(parents=True)
    payload.write_bytes(raw)
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': old_scope, 'source': str(source), 'excluded': []}))
    def no_walk(*args, **kwargs):
        pytest.fail('v7 named control upgrade recursively scanned immutable originals')
    monkeypatch.setattr(migration.os, 'walk', no_walk)
    upgraded = migration._freeze_controls(source, stage, [], existing=captured, upgrade_from=old_scope)
    assert set(upgraded) == {'status.json', relative}
    assert Path(upgraded['status.json']['path']).read_text() == '{"cursor":1}'
    assert Path(upgraded[relative]['path']).read_bytes() == raw
    payload.write_bytes(b'newer current control')
    assert migration._freeze_controls(source, stage, [], existing=upgraded, upgrade_from=old_scope) == upgraded


@pytest.mark.parametrize('source_name,relative,excluded', [
    ('data_bybit', '1m/download_report.csv', ['1m']),
    ('data_tw_futures', 'shioaji_history/FT1R1/download.lock', ['shioaji_history']),
    ('data_tw_public', 'features/tw_public_stock_daily.parquet', ['features']),
])
def test_v7_named_controls_respect_exclusions(tmp_path, source_name, relative, excluded):
    source = tmp_path / source_name
    payload = source / relative
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b'preserve original bytes')
    for index, upgrade in enumerate((None, 'recursive_mutable_controls_v6')):
        result = migration._freeze_controls(source, tmp_path / f'controls/captured-{index}', excluded,
                                            upgrade_from=upgrade)
        assert result == {}


def test_arbitrary_lock_and_csv_names_do_not_gain_a_mutable_exception(tmp_path):
    source = tmp_path / 'data_bybit'
    (source / 'raw').mkdir(parents=True)
    for relative in ('raw/download.lock', 'raw/.download.lock', 'raw/download_report.csv',
                     '1m/source.lock', '1m/raw.csv'):
        payload = source / relative
        payload.parent.mkdir(exist_ok=True)
        payload.write_bytes(b'immutable observation')
    assert migration._freeze_controls(source, tmp_path / 'controls/captured', []) == {}


def test_v3_preparation_upgrade_probes_named_controls_without_rewalking_raw_files(setup, monkeypatch):
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]['source'])
    (source / 'status.json').write_text('{"old":true}')
    private = migration.private_history_root(cold, policy)
    prepared = migration._private_preparation(entries[1], args, source, private, policy)
    path = migration._preparation_path(args, 'private')
    prepared['control_scope'] = 'recursive_mutable_controls_v3'
    path.write_text(json.dumps(prepared))
    intent = Path(prepared['controls_staging']).with_suffix('.intent.json')
    value = json.loads(intent.read_text());value['contract'] = 'recursive_mutable_controls_v3'
    intent.write_text(json.dumps(value))
    (source / 'shioaji_1m').mkdir()
    (source / 'shioaji_1m/download_report.csv').write_text('symbol,status\n2330,complete\n')
    (source / 'status.json').write_text('{"new":true}')
    def reject_recursive_census(*args, **kwargs):
        pytest.fail('completed v3 controls were recursively scanned again for a named v4 addition')
    monkeypatch.setattr(migration.os, 'walk', reject_recursive_census)
    upgraded = migration._private_preparation(entries[1], args, source, private, policy)
    assert upgraded['control_scope'] == migration.CONTROL_SCOPE
    assert set(upgraded['controls']) == {'status.json', 'shioaji_1m/download_report.csv'}
    assert Path(upgraded['controls']['status.json']['path']).read_text() == '{"old":true}'


def test_ordinary_csv_still_fails_closed_on_mutation(setup, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    repo, _, entries, args, _ = setup
    source = Path(entries[1]['source'])
    (source / 'raw.csv').write_text('price\n123\n')
    original = packed._write_pack
    def while_packing(root, selected, target, **options):
        (source / 'raw.csv').write_text('price\n999\n')
        return original(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', while_packing)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    assert result['datasets']['private']['state'] == 'waiting_retry'
    assert 'changed while packing' in result['datasets']['private']['error']


@pytest.mark.parametrize('source_name,relative', [
    ('data_bybit', '1m/_hot_tail/0GUSDT_features.parquet'),
    ('data_okx', '1m/_hot_tail/ARUSDTSWAP_features.parquet'),
    ('data_binance', '1m/_hot_tail/BTCUSDT.parquet'),
    ('data_taifex_public_history', 'rules/normalized/document_links.parquet'),
    ('data_taifex_public_history/rules', 'normalized/document_links.parquet'),
    ('data_openBB', 'openbb_l1.duckdb'),
    ('data_openBB', 'catalog/l1_compaction_status.parquet'),
    ('data_tw_public', 'features/tw_public_stock_daily.parquet'),
])
def test_known_published_mutable_payload_keeps_exact_old_bytes_and_raw_files(
        setup, monkeypatch, source_name, relative):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / source_name
    source.mkdir(parents=True)
    payload = source / relative
    payload.parent.mkdir(parents=True, exist_ok=True)
    payload.write_bytes(b'closed published original bytes, raw=-7, NULL, shares\n')
    (source / 'raw.csv').write_bytes(b'unchanged original observations\n')
    entry = {**entries[1], 'source': str(source)}
    native = packed._write_pack
    def replace_while_packing(root, selected, target, **options):
        replacement = payload.with_name('.replaced')
        replacement.write_bytes(b'newer closed published version\n')
        replacement.replace(payload)
        return native(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', replace_while_packing)
    result = migration.migrate_catalog(repo, args, [entry])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    saved = resolve_latest_packed(private, 'private')
    restored = fetch_packed_snapshot(private, repo / 'restored', saved)
    assert (restored / relative).read_bytes() == b'closed published original bytes, raw=-7, NULL, shares\n'
    assert payload.read_bytes() == b'newer closed published version\n'
    assert (restored / 'raw.csv').read_bytes() == (source / 'raw.csv').read_bytes()
    publication = result['datasets']['private']['publication']
    assert publication['frozen_control_files'][relative]['capture_kind'] == 'closed_atomic_published_payload'


def test_v3_delta_payload_upgrade_is_named_not_recursive_and_keeps_exclusions(tmp_path, monkeypatch):
    source = tmp_path / 'data_bybit'
    folder = source / '1m/_hot_tail'
    folder.mkdir(parents=True)
    (folder / 'BTCUSDT.parquet').write_bytes(b'closed old bytes')
    (folder / 'notes.tmp').write_bytes(b'actual raw observation')
    stage = tmp_path / 'controls/captured'
    stage.parent.mkdir()
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v3', 'source': str(source), 'excluded': []}))
    def no_walk(*args, **kwargs):
        pytest.fail('v3 published-output upgrade rescanned immutable raw files')
    monkeypatch.setattr(migration.os, 'walk', no_walk)
    captured = migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v3')
    assert set(captured) == {'1m/_hot_tail/BTCUSDT.parquet'}
    excluded = migration._freeze_controls(source, tmp_path / 'excluded/captured', ['1m'],
                                          upgrade_from='recursive_mutable_controls_v3')
    assert not excluded


@pytest.mark.parametrize('old_scope', ['recursive_mutable_controls_v3', 'recursive_mutable_controls_v4',
                                       'recursive_mutable_controls_v5'])
def test_rule_only_delta_upgrade_preserves_frozen_controls_and_avoids_raw_census(tmp_path, monkeypatch, old_scope):
    source = tmp_path / 'data_taifex_public_history/rules'
    (source / 'normalized').mkdir(parents=True)
    (source / 'normalized/documents.parquet').write_bytes(b'closed official table')
    (source / 'status.json').write_text('{"cursor":1}')
    stage = tmp_path / 'controls/captured'
    captured = migration._freeze_controls(source, stage, [])
    (stage / 'normalized/documents.parquet').unlink()  # This test's pre-v5 fixture had no table capture.
    old = {'status.json': captured['status.json']}
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': old_scope, 'source': str(source), 'excluded': []}))
    (source / 'status.json').write_text('{"cursor":2}')
    def no_walk(*args, **kwargs):
        pytest.fail('named rule-only upgrade rescanned all historical originals')
    monkeypatch.setattr(migration.os, 'walk', no_walk)
    upgraded = migration._freeze_controls(source, stage, [], existing=old, upgrade_from=old_scope)
    assert set(upgraded) == {'status.json', 'normalized/documents.parquet'}
    assert Path(upgraded['status.json']['path']).read_text() == '{"cursor":1}'
    assert Path(upgraded['normalized/documents.parquet']['path']).read_bytes() == b'closed official table'
    # Unrelated directories called normalized do not gain this exception.
    assert not migration._mutable_published_locations(tmp_path / 'unrelated/rules')


@pytest.mark.parametrize('relative', ['cooldowns/cmoney.json', 'robots/' + 'a' * 64 + '.json'])
def test_news_operational_json_is_frozen_without_changing_live_cache(setup, monkeypatch, relative):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / 'data_news_fulltext'
    payload = source / relative
    payload.parent.mkdir(parents=True)
    original_bytes = b'{"deadline":123.456,"raw":-7,"unknown":null}'
    payload.write_bytes(original_bytes)
    (source / 'raw.csv').write_bytes(b'unchanged article observations\n')
    native = packed._write_pack
    def replace_while_packing(root, selected, target, **options):
        replacement = payload.with_name('.replacement')
        replacement.write_bytes(b'{"deadline":987.654}')
        replacement.replace(payload)
        return native(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', replace_while_packing)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    saved = resolve_latest_packed(migration.private_history_root(cold, policy), 'private')
    restored = fetch_packed_snapshot(migration.private_history_root(cold, policy), repo / 'restored', saved)
    assert (restored / relative).read_bytes() == original_bytes
    assert payload.read_bytes() == b'{"deadline":987.654}'
    assert (restored / 'raw.csv').read_bytes() == (source / 'raw.csv').read_bytes()


@pytest.mark.parametrize('old_scope', ['recursive_mutable_controls_v3', 'recursive_mutable_controls_v4',
                                       'recursive_mutable_controls_v5'])
def test_news_control_upgrade_is_named_preserves_observations_and_exclusions(tmp_path, monkeypatch, old_scope):
    source = tmp_path / 'data_news_fulltext'
    (source / 'cooldowns').mkdir(parents=True)
    (source / 'cooldowns/cmoney.json').write_text('{"deadline":1}')
    (source / 'robots').mkdir()
    (source / 'robots/origin.json').write_text('{"body":"User-agent: *"}')
    (source / 'status.json').write_text('{"cursor":1}')
    stage = tmp_path / 'controls/captured'
    stage.parent.mkdir()
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': old_scope, 'source': str(source), 'excluded': ['robots']}))
    def no_walk(*args, **kwargs):
        pytest.fail('news controls upgrade rescanned immutable article payloads')
    monkeypatch.setattr(migration.os, 'walk', no_walk)
    captured = migration._freeze_controls(source, stage, ['robots'], upgrade_from=old_scope)
    assert set(captured) == {'cooldowns/cmoney.json'}
    (source / 'cooldowns/cmoney.json').write_text('{"deadline":2}')
    repeated = migration._freeze_controls(source, stage, ['robots'], existing=captured, upgrade_from=old_scope)
    assert repeated == captured
    assert Path(repeated['cooldowns/cmoney.json']['path']).read_text() == '{"deadline":1}'
    assert not migration._mutable_json_control_locations(tmp_path / 'unrelated')


def test_news_article_json_is_not_silently_treated_as_a_mutable_control(setup, monkeypatch):
    from stockagent.data_sync import packed_snapshots as packed
    repo, _, entries, args, _ = setup
    source = repo / 'data_news_fulltext'
    (source / 'articles').mkdir(parents=True)
    payload = source / 'articles/immutable.json'
    payload.write_text('{"raw":-7}')
    native = packed._write_pack
    def mutate_article(root, selected, target, **options):
        payload.write_text('{"raw":-8}')
        return native(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', mutate_article)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['datasets']['private']['state'] == 'waiting_retry'
    assert 'changed while packing' in result['datasets']['private']['error']


@pytest.mark.parametrize('fault', ['symlink', 'wal', 'no_space'])
def test_published_mutable_capture_rejects_unsafe_backup_or_space(tmp_path, monkeypatch, fault):
    source = tmp_path / 'data_openBB'
    source.mkdir()
    database = source / 'openbb_l1.duckdb'
    database.write_bytes(b'closed catalog')
    if fault == 'symlink':
        other = source / 'other.duckdb'
        database.rename(other)
        database.symlink_to('other.duckdb')
    elif fault == 'wal':
        database.with_suffix('.duckdb.wal').write_bytes(b'uncheckpointed live write')
    else:
        monkeypatch.setattr(migration.shutil, 'disk_usage', lambda *_: SimpleNamespace(free=0))
    with pytest.raises(SnapshotError):
        migration._freeze_controls(source, tmp_path / 'controls/captured', [])
    assert database.exists()
    assert not (tmp_path / 'controls/captured/openbb_l1.duckdb').exists()


def test_published_capture_larger_than_64_mib_uses_bounded_memory_and_resumes_exact_copy(tmp_path):
    import tracemalloc
    source = tmp_path / 'data_openBB'
    source.mkdir()
    database = source / 'openbb_l1.duckdb'
    with database.open('wb') as stream:
        stream.truncate(70 * 1024**2)
    stage = tmp_path / 'controls/captured'
    tracemalloc.start()
    try:
        captured = migration._freeze_controls(source, stage, [])
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    row = captured['openbb_l1.duckdb']
    assert row['bytes'] == 70 * 1024**2 and peak < 32 * 1024**2
    assert migration.sha256_file(database) == row['sha256']
    assert migration.sha256_file(Path(row['path'])) == row['sha256']
    database.write_bytes(b'new live closed catalog')
    # Crash after final-name install but before saving the control row: keep
    # the complete first observation; do not read today's value over it.
    assert migration._freeze_controls(source, stage, []) == captured


def test_private_capture_atomic_no_replace_keeps_an_existing_observation(tmp_path):
    from stockagent.data_sync.desync_snapshots import rename_without_overwrite
    source, target = tmp_path / 'source', tmp_path / 'target'
    source.write_bytes(b'new observation')
    target.write_bytes(b'original observation')
    with pytest.raises(FileExistsError):
        rename_without_overwrite(source, target)
    assert source.read_bytes() == b'new observation' and target.read_bytes() == b'original observation'
    target.unlink()
    rename_without_overwrite(source, target)
    assert not source.exists() and target.read_bytes() == b'new observation'
    assert target.stat().st_nlink == 1


def test_mutable_cohort_benchmark_keeps_complete_recovery_and_equal_original_values():
    from scripts.benchmark_sealed_pack_recovery import mutable_trial
    reference = mutable_trial(32, False)
    fixed = mutable_trial(32, True)
    assert reference['attempts'] == 2 and fixed['attempts'] == 1
    assert reference['inventory_sha256'] == fixed['inventory_sha256']
    assert reference['full_original_readback'] is fixed['full_original_readback'] is True
    assert reference['owned_scratch_retired'] is fixed['owned_scratch_retired'] is True
    assert reference['production_source_payloads_deleted'] == fixed['production_source_payloads_deleted'] == 0


def test_retry_deadline_and_untouched_work_prevent_head_of_line_starvation(setup, monkeypatch):
    repo, cold, entries, args, _ = setup
    original = publisher._publish_entry
    def fail_public(*_):
        raise SnapshotError('injected busy source')
    monkeypatch.setattr(publisher, '_publish_entry', fail_public)
    args.maximum_completed_sources = 1
    first = migration.migrate_catalog(repo, args, entries)
    assert first['datasets']['public']['state'] == 'waiting_retry'
    assert first['datasets']['private']['state'] == 'verified'
    monkeypatch.setattr(publisher, '_publish_entry', original)
    path = args.migration_state / 'progress.json'
    document = json.loads(path.read_text())
    document['datasets']['public']['retry_at_unix'] = 10**20
    path.write_text(json.dumps(document))
    result = migration.migrate_catalog(repo, args, entries)
    assert result['datasets']['public']['attempts'] == 1
    document['datasets']['public']['retry_at_unix'] = 0
    path.write_text(json.dumps(document))
    result = migration.migrate_catalog(repo, args, entries)
    row = result['datasets']['public']
    assert row['state'] == 'verified'
    assert row['last_failure']['error'] == 'injected busy source'
    assert 'error' not in row and 'error_type' not in row and 'retry_at_unix' not in row
    progress = json.loads((args.migration_state / 'publication/public.json').read_text())
    assert progress['attempt_id'] == row['attempt_id']


def test_untouched_source_advances_before_an_eligible_failed_source(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    original = publisher._publish_entry
    monkeypatch.setattr(publisher, '_publish_entry', lambda *_: (_ for _ in ()).throw(SnapshotError('old failure')))
    args.maximum_completed_sources = 1
    first = migration.migrate_catalog(repo, args, entries)
    assert first['datasets']['private']['state'] == 'verified'
    path = args.migration_state / 'progress.json'
    document = json.loads(path.read_bytes())
    document['datasets']['public']['retry_at_unix'] = 0
    # Simulate an interrupted activity before its next untouched item, without
    # changing the enrolled plan. The old failed source is otherwise eligible.
    document['datasets']['private'].update(state='pending', attempts=0)
    for name in ('snapshot_id', 'manifest_sha256'):
        document['datasets']['private'].pop(name, None)
    path.write_text(json.dumps(document))
    monkeypatch.setattr(publisher, '_publish_entry', original)
    result = migration.migrate_catalog(repo, args, entries)
    assert result['datasets']['private']['state'] == 'verified'
    assert result['datasets']['public']['state'] == 'waiting_retry'
    assert result['datasets']['public']['attempts'] == 1


def test_source_owner_wait_has_a_durable_attempt_receipt(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    monkeypatch.setattr(publisher, '_publish_entry', lambda *_: ({'publish_ready': False}, None))
    result = migration.migrate_catalog(repo, args, [entries[0]])
    row = result['datasets']['public']
    path = args.migration_state / 'attempts/public' / (row['attempt_id'] + '.json')
    proof = json.loads(path.read_bytes())
    assert proof['state'] == 'waiting_source_owner'
    assert proof['attempt_id'] == row['attempt_id'] and proof['finished_at_utc']


def test_dependency_failure_is_per_source_and_does_not_starve_other_work(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    def missing_dependency(*_):
        raise ModuleNotFoundError("injected unavailable audit dependency")
    monkeypatch.setattr(publisher, '_publish_entry', missing_dependency)
    result = migration.migrate_catalog(repo, args, entries)
    row = result['datasets']['public']
    assert row['state'] == 'waiting_retry' and row['error_type'] == 'ModuleNotFoundError'
    assert result['datasets']['private']['state'] == 'verified'
    assert result['runtime_owner']['active_dataset'] is None
    assert json.loads((args.migration_state / 'attempts/public' / (row['attempt_id'] + '.json')).read_text())['error']


@pytest.mark.parametrize('stage', ['processing', 'recovery'])
def test_interrupted_activity_preserves_fixed_snapshot_and_attempt_before_resume(setup, monkeypatch, stage):
    repo, _, entries, args, _ = setup
    first = migration.migrate_catalog(repo, args, [entries[0]])
    path = args.migration_state / 'progress.json'
    document = json.loads(path.read_text())
    row = document['datasets']['public']
    old_attempt, snapshot = row['attempt_id'], row['snapshot_id']
    old_receipt = (args.migration_state / 'attempts/public' / (old_attempt + '.json')).read_bytes()
    row['state'] = stage
    path.write_text(json.dumps(document))
    monkeypatch.setattr(publisher, '_publish_entry', lambda *_: pytest.fail('committed release republished'))
    resumed = migration.migrate_catalog(repo, args, [entries[0]])
    current = resumed['datasets']['public']
    assert current['state'] == 'verified' and current['snapshot_id'] == snapshot
    assert current['attempt_id'] != old_attempt
    assert current['last_interruption']['attempt_id'] == old_attempt
    interruption = json.loads(Path(current['last_interruption']['receipt']).read_text())
    assert interruption['state'] == stage and interruption['snapshot_id'] == snapshot
    assert (args.migration_state / 'attempts/public' / (old_attempt + '.json')).read_bytes() == old_receipt
    assert first['datasets']['public']['state'] == 'verified'


def test_sqlite_backup_is_not_repeated_when_main_publication_retries(setup, monkeypatch):
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]["source"])
    with sqlite3.connect(source / "state.sqlite3") as db:
        db.execute("CREATE TABLE observations(value)")
        db.commit()
    private = migration.private_history_root(cold, policy)
    initialize_packed_layout(private, node_id="test")
    prepared = migration._private_preparation(entries[1], args, source, private, policy)
    monkeypatch.setattr(migration, "_snapshot_sqlite", lambda *a: pytest.fail("repeated SQLite backup"))
    again = migration._private_preparation(entries[1], args, source, private, policy)
    assert prepared == again


def test_old_preparation_extends_controls_without_repeating_sqlite_or_losing_old_values(setup, monkeypatch):
    repo, cold, entries, args, policy = setup
    source = Path(entries[1]['source'])
    (source / 'status.json').write_text('{"value":1}')
    with sqlite3.connect(source / 'state.sqlite3') as db:
        db.execute('CREATE TABLE data(value)')
    private = migration.private_history_root(cold, policy)
    first = migration._private_preparation(entries[1], args, source, private, policy)
    path = migration._preparation_path(args, 'private')
    old = json.loads(path.read_bytes())
    old.pop('control_scope')
    path.write_text(json.dumps(old))
    (source / 'status.json').write_text('{"value":2}')
    deep = source / 'complement/receipts/market/id/history.json'
    deep.parent.mkdir(parents=True)
    deep.write_text('{"value":3}')
    monkeypatch.setattr(migration, '_snapshot_sqlite', lambda *_: pytest.fail('repeated large SQLite snapshot'))
    upgraded = migration._private_preparation(entries[1], args, source, private, policy)
    assert upgraded['sqlite_states'] == first['sqlite_states']
    assert Path(upgraded['controls']['status.json']['path']).read_text() == '{"value":1}'
    assert 'complement/receipts/market/id/history.json' in upgraded['controls']
    assert migration._private_preparation(entries[1], args, source, private, policy) == upgraded


def test_sqlite_interrupted_proof_reuses_its_exact_owned_destination(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    database = Path(entries[1]['source']) / 'state.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE observations(value)')
        db.execute('INSERT INTO observations VALUES(37)')
        db.commit()
    staging = args.migration_state / 'sqlite-snapshots' / 'owned'
    original = migration.atomic_write_json
    def interrupted(path, document, **options):
        if path.name == 'snapshot.json':
            raise OSError('injected crash after native SQLite backup')
        return original(path, document, **options)
    monkeypatch.setattr(migration, 'atomic_write_json', interrupted)
    with pytest.raises(OSError, match='injected crash'):
        migration._snapshot_sqlite(database, staging, 'state.sqlite3')
    assert (staging / 'backup-intent.json').exists()
    assert not (staging / 'snapshot.json').exists()
    inode = (staging / 'snapshot.sqlite3').stat().st_ino
    monkeypatch.setattr(migration, 'atomic_write_json', original)
    result = migration._snapshot_sqlite(database, staging, 'state.sqlite3')
    assert result['raw_main_wal_shm_byte_copy'] is False
    assert (staging / 'snapshot.sqlite3').stat().st_ino == inode
    with sqlite3.connect(staging / 'snapshot.sqlite3') as db:
        assert db.execute('SELECT value FROM observations').fetchall() == [(37,)]
    with pytest.raises(SnapshotError, match='durable owned intent'):
        migration._snapshot_sqlite(database, staging, 'other.sqlite3')


def test_sqlite_crash_before_intent_accepts_only_empty_owned_stage(setup):
    _, _, entries, args, _ = setup
    database = Path(entries[1]['source']) / 'state.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE observations(value)')
        db.commit()
    staging = args.migration_state / 'sqlite-snapshots' / 'empty-owned'
    staging.mkdir(mode=0o700, parents=True)
    result = migration._snapshot_sqlite(database, staging, 'state.sqlite3')
    assert result['snapshot_bytes'] > 0 and (staging / 'backup-intent.json').exists()


def test_owned_scratch_is_retired_only_after_recovery_not_originals(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    source = Path(entries[1]["source"])
    (source / "status.json").write_text('{"cursor":1}')
    with sqlite3.connect(source / "state.sqlite3") as db:
        db.execute("CREATE TABLE observed(value)")
        db.execute("INSERT INTO observed VALUES(3)")
        db.commit()
    from stockagent.data_sync import materialized_cache
    monkeypatch.setattr(materialized_cache, "process_references_many", lambda *_: [])
    args.retire_staging = True
    result = migration.migrate_catalog(repo, args, [entries[1]])
    row = result["datasets"]["private"]
    assert row["state"] == "verified", row
    assert row["owned_scratch_retirement"]["state"] == "retired"
    assert (source / "status.json").exists() and (source / "state.sqlite3").exists()
    assert not any(Path(name).exists() for name in row["owned_scratch_retirement"]["files"])
    assert row['publication']['sealed_pack_hint_files']
    assert all(not Path(item['path']).exists() for item in row['publication']['sealed_pack_hint_files'])
    assert json.loads(Path(row["recovery_receipt"]).read_text())["full_original_file_reconstruction_verified"]


def test_failed_recovery_keeps_owned_hints_and_all_originals(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    args.retire_staging = True
    def rejected(*args, **options):
        raise SnapshotError('injected full original recovery failure')
    monkeypatch.setattr(migration, '_recover', rejected)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    row = result['datasets']['private']
    assert row['state'] == 'waiting_retry'
    assert row['publication']['sealed_pack_hint_files']
    assert all(Path(item['path']).exists() for item in row['publication']['sealed_pack_hint_files'])
    assert (Path(entries[1]['source']) / 'raw.txt').read_text() == 'private'
    assert not (args.migration_state / 'scratch-retirement/private.json').exists()


def test_completed_source_budget_keeps_the_remaining_plan_durable(setup):
    repo, _, entries, args, _ = setup
    args.maximum_completed_sources = 1
    first = migration.migrate_catalog(repo, args, entries)
    assert first["source_count"] == 2 and first["verified_datasets"] == 1
    assert first["datasets"]["private"]["state"] == "pending"
    assert migration.migrate_catalog(repo, args, entries)["state"] == "verified"


def test_existing_lifecycle_resumes_all_sources_then_sleeps_daily_not_per_poll(setup, monkeypatch):
    repo, cold, entries, args, _ = setup
    path = repo / "configs/columnar_storage.json"
    document = json.loads(path.read_text())
    document["dataset_version_lifecycle"] = json.loads((ROOT / "configs/columnar_storage.json").read_text())["dataset_version_lifecycle"]
    path.write_text(json.dumps(document))
    monkeypatch.setattr(publisher, "_load_catalog", lambda *_: entries)
    configuration = {"cold_root": str(cold), "state_root": str(args.migration_state.parent / "control")}
    first = migration.advance_catalog_versions(repo, configuration, now=100)
    assert first["verified_datasets"] == 1 and first["source_count"] == 2
    second = migration.advance_catalog_versions(repo, configuration, now=200)
    assert second["state"] == "verified"
    monkeypatch.setattr(migration, "migrate_catalog", lambda *a: pytest.fail("rehashed every poll"))
    sleeping = migration.advance_catalog_versions(repo, configuration, now=300)
    assert sleeping["state"] == "waiting_next_daily_cycle"
    assert sleeping["next_cycle_unix"] == 200 + 86400


def test_root_subtree_bindings_do_not_silently_omit_the_rest(setup):
    repo, _, entries, _, policy = setup
    root = repo / "data_example"
    (root / "published").mkdir(parents=True)
    entry = {**entries[0], "dataset": "partial", "source": str(root / "published")}
    policy["roots"] = {"data_example": {"native_layout": "source", "target_grain": "period"}}
    selected = migration.migration_entries(repo, [entry], policy)
    assert len(selected) == 2
    assert selected[1]["source"] == str(root)
    assert selected[1]["publish"] is False
    assert selected[1]["dataset"] == "local-example"


def test_public_to_private_reuse_shares_existing_bytes_but_never_shared_heads(setup):
    repo, cold, entries, args, policy = setup
    source = Path(entries[0]["source"])
    initial = publish_packed_snapshot(cold, "public", source, pack_buckets=1, file_chunk_bytes=1024)
    private_entry = {**entries[0], "publish": False, "dataset": "local-full"}
    private = migration.private_history_root(cold, policy)
    initialize_packed_layout(private, node_id="test")
    files, objects, proofs = migration._bootstrap_public_storage(repo, private_entry, cold, private, entries)
    assert files and objects and len(proofs) == 1
    for obj in objects.values():
        assert (cold / obj["relpath"]).stat().st_ino == (private / obj["relpath"]).stat().st_ino
    saved = publish_packed_snapshot(private, "local-full", source, pack_buckets=1, file_chunk_bytes=1024,
        bootstrap_file_storage=files, bootstrap_objects=objects)
    assert saved.manifest["archive"]["changed_files"] == 0
    assert saved.manifest["archive"]["reused_files"] == 1
    assert not (cold / "heads/local-full").exists()
    assert resolve_latest_packed(cold, "public").manifest_sha256 == initial.manifest_sha256


@pytest.mark.parametrize('limit', [2, 100])
def test_private_bootstrap_streams_overlapping_donors_into_one_consumed_index(setup, monkeypatch, limit):
    from stockagent.data_sync.packed_metadata import MetadataRows
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, _, policy = setup
    source = Path(entries[0]['source'])
    for index in range(5):
        (source / f'{index}.bin').write_bytes(bytes([index]) * 40)
    first = publish_packed_snapshot(cold, 'public', source, pack_buckets=1, file_chunk_bytes=1024)
    (source / 'raw.txt').write_bytes(b'revised source bytes')
    second = publish_packed_snapshot(cold, 'second-donor', source, pack_buckets=1, file_chunk_bytes=1024)
    donors = [entries[0], {**entries[0], 'dataset': 'second-donor'}]
    private = migration.private_history_root(cold, policy)
    initialize_packed_layout(private, node_id='test')
    monkeypatch.setattr(packed, '_load_inventory', lambda *_: pytest.fail('bootstrap retained a whole inventory'))
    progress = []
    with MetadataRows(dict, dict, memory_rows=limit) as index:
        files, objects, proofs = migration._bootstrap_public_storage(repo,
            {**entries[0], 'dataset': 'local-full'}, cold, private, donors,
            file_sink=index, progress=progress.append)
        assert files is index and len(index) == 6 and index.spilled is (limit == 2)
        assert progress[-1]['donor_rows_examined'] == 12
        assert progress[-1]['file_hints_indexed'] == 6
        assert len(proofs) == 2
        saved = publish_packed_snapshot(private, 'local-full', source, pack_buckets=1,
            file_chunk_bytes=1024, bootstrap_file_storage=index, bootstrap_objects=objects)
        assert not index._rows and index._db is None and index._scratch is None
    assert saved.manifest['archive']['changed_files'] == 0
    assert saved.manifest['archive']['reused_files'] == 6
    assert packed.verify_packed_snapshot(private, saved, reconstruct_all=True)['independently_reconstructed_files'] == 6
    restored = fetch_packed_snapshot(private, repo / 'restored', saved)
    assert all((restored / path.name).read_bytes() == path.read_bytes() for path in source.iterdir())
    assert resolve_latest_packed(cold, 'public').manifest_sha256 == first.manifest_sha256
    assert resolve_latest_packed(cold, 'second-donor').manifest_sha256 == second.manifest_sha256
    assert not (cold / 'heads/local-full').exists()


def test_dependency_change_creates_metadata_version_not_timestamp_only_noop(setup):
    _, cold, entries, _, _ = setup
    source = Path(entries[0]["source"])
    first = publish_packed_snapshot(cold, "dependencies", source, pack_buckets=1,
        file_chunk_bytes=1024, metadata={"version_dependencies_sha256": "a" * 64})
    second = publish_packed_snapshot(cold, "dependencies", source, pack_buckets=1,
        file_chunk_bytes=1024, metadata={"version_dependencies_sha256": "b" * 64})
    third = publish_packed_snapshot(cold, "dependencies", source, pack_buckets=1,
        file_chunk_bytes=1024, metadata={"version_dependencies_sha256": "b" * 64})
    assert first.manifest_sha256 != second.manifest_sha256 == third.manifest_sha256
    assert second.manifest["archive"]["new_stored_bytes"] == 0


@pytest.mark.parametrize("size", [0, 1, 63, 1024, 2048, 2049, 81937])
def test_native_cdc_is_streaming_bounded_and_preserves_every_byte(size):
    pytest.importorskip("fastcdc")
    import random
    payload = random.Random(23).randbytes(size)
    chunks = list(_source_blocks(io.BytesIO(payload), 1024, "fastcdc-v1"))
    assert b"".join(chunks) == payload
    assert all(0 < len(chunk) <= 1024 for chunk in chunks)


def test_native_cdc_survives_unaligned_insertion():
    pytest.importorskip("fastcdc")
    import random
    import hashlib
    raw = random.Random(23).randbytes(1024 * 1024)
    edited = raw[:87] + b"new source observation" + raw[87:]
    original = {hashlib.sha256(block).hexdigest() for block in _source_blocks(io.BytesIO(raw), 16384, "fastcdc-v1")}
    blocks = list(_source_blocks(io.BytesIO(edited), 16384, "fastcdc-v1"))
    reused = sum(len(block) for block in blocks if hashlib.sha256(block).hexdigest() in original)
    assert reused > 0.95 * len(raw)
