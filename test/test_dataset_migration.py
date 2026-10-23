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


def test_private_control_freeze_uses_observed_size_and_complete_original_bytes(tmp_path, monkeypatch):
    import os
    source = tmp_path / 'source'
    source.mkdir()
    expected = b'{"original":-7,"missing":null,"unit":"shares"}'
    (source / 'status.json').write_bytes(expected)
    native, reads = os.fdopen, []
    class ReadProbe:
        def __init__(self, stream):
            self.stream = stream
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def read(self, bound):
            reads.append(bound)
            return self.stream.read(bound)
    def observed(descriptor, mode, *args, **options):
        stream = native(descriptor, mode, *args, **options)
        return ReadProbe(stream) if mode == 'rb' else stream
    monkeypatch.setattr(migration.os, 'fdopen', observed)
    frozen = migration._freeze_controls(source, tmp_path / 'capture', [])
    assert reads == [len(expected) + 1]
    assert Path(frozen['status.json']['path']).read_bytes() == expected
    assert (source / 'status.json').read_bytes() == expected


def test_private_control_freeze_fdopen_error_closes_owned_descriptor(tmp_path, monkeypatch):
    import os
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'status.json').write_bytes(b'{"original":true}')
    native, descriptors = os.fdopen, []
    def fail_reader(descriptor, mode, *args, **options):
        if mode == 'rb':
            descriptors.append(descriptor)
            raise OSError('deliberate control stream construction failure')
        return native(descriptor, mode, *args, **options)
    monkeypatch.setattr(migration.os, 'fdopen', fail_reader)
    with pytest.raises(OSError, match='deliberate control stream construction failure'):
        migration._freeze_controls(source, tmp_path / 'capture', [])
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert not (tmp_path / 'capture/status.json').exists()


@pytest.mark.parametrize('cold,native_packs', [
    ('/srv/stockagent-packed', True),
    ('/srv/stockagent-packed/.local-state/private-history', True),
    ('/elsewhere/cold', False),
])
def test_recovery_uses_full_verified_native_pack_reads_only_for_original_D(
        tmp_path, monkeypatch, cold, native_packs):
    selected = SimpleNamespace(manifest_sha256='a' * 64, manifest={'source': {'files': 31}})
    resolutions = []
    def resolve(*args, **options):
        resolutions.append(options)
        return selected
    monkeypatch.setattr(migration, 'resolve_packed_snapshot_id', resolve)
    calls = []
    def verify(root, release, **options):
        calls.append((root, release, options))
        return {'independently_reconstructed_files': 31}
    monkeypatch.setattr(migration, 'verify_packed_snapshot', verify)
    receipt = tmp_path / 'recovery.json'
    proof = migration._recover(Path(cold), 'dataset', 'fixed-release', receipt, tmp_path / 'progress.json')
    assert len(calls) == 1
    assert resolutions == [{'require_objects': False}]
    root, release, options = calls[0]
    assert root == Path(cold) and release is selected
    assert options['reconstruct_all'] is True
    assert options['d_primary_native_pack_reads'] is native_packs
    assert options['d_primary_native_chunk_reads'] is native_packs
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
    assert resolutions == [{'require_objects': False}, {'require_objects': True}]


def test_full_recovery_checks_presence_once_and_cached_proof_still_rejects_missing_object(tmp_path, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'file.bin').write_bytes(b'a' * 64)
    cold = tmp_path / 'cold'
    release = packed.publish_packed_snapshot(cold, 'dataset', source, node_id='test',
        loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2)
    original = packed._validate_object_presence
    calls = []
    def presence(root, manifest):
        calls.append(manifest['snapshot_id'])
        return original(root, manifest)
    monkeypatch.setattr(packed, '_validate_object_presence', presence)
    receipt = tmp_path / 'proof.json'
    proof = migration._recover(cold, 'dataset', release.manifest['snapshot_id'], receipt, tmp_path / 'progress.json')
    assert proof['full_original_file_reconstruction_verified']
    assert proof['independently_reconstructed_bytes'] == 64
    assert calls == [release.manifest['snapshot_id']]
    migration._recover(cold, 'dataset', release.manifest['snapshot_id'], receipt, tmp_path / 'progress.json')
    assert len(calls) == 2  # Cached proof cannot bypass live presence checks.
    (cold / release.manifest['archive']['objects'][0]['relpath']).unlink()
    with pytest.raises(SnapshotError, match='not locally complete'):
        migration._recover(cold, 'dataset', release.manifest['snapshot_id'], receipt, tmp_path / 'progress.json')
    assert len(calls) == 3


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


def test_migration_recovers_already_admitted_fixed_catalog_release_without_live_republication(
        setup, monkeypatch):
    from datetime import UTC, datetime, timedelta
    repo, cold, entries, args, _ = setup
    entry = entries[0]
    entry['source_coordination_lock'] = 'public-owner.lock'
    entry['freshness'] = {'receipt': 'release.json', 'field': 'end_date', 'format': 'iso-date',
        'required_values': {'status': 'complete'}, 'max_receipt_age_hours': 1,
        'timestamp_field': 'checked_at_utc'}
    source = Path(entry['source'])
    initial = {'end_date': '2026-10-21', 'status': 'complete',
               'checked_at_utc': datetime.now(UTC).isoformat()}
    (source / 'release.json').write_text(json.dumps(initial))
    _, fixed = publisher._publish_entry(entry, args, cold)
    assert fixed.manifest['schema_version'] == 2
    assert fixed.manifest['metadata']['catalog_entry_sha256'] == publisher.catalog_publication_identity(entry)
    # A collector has started the next revision. Its expiry/pending state is
    # not permission to republish it, nor a reason to lose the prior D version.
    (source / 'release.json').write_text(json.dumps({**initial, 'status': 'pending',
        'checked_at_utc': (datetime.now(UTC) - timedelta(days=7)).isoformat()}))
    (source / 'raw.txt').write_text('new uncommitted observations')
    monkeypatch.setattr(publisher, '_publish_entry',
                        lambda *_a, **_k: pytest.fail('fixed-version recovery must not republish live bytes'))
    with publisher._source_coordination_lock(entry, wait_seconds=0):
        result = migration.migrate_catalog(repo, args, entries)
    row = result['datasets']['public']
    assert row['state'] == 'verified' and result['verified_datasets'] == 2
    assert row['snapshot_id'] == fixed.manifest['snapshot_id']
    assert row['manifest_sha256'] == fixed.manifest_sha256
    assert row['publication']['state'] == 'fixed_committed_catalog_release'
    assert row['publication']['current_source_freshness_checked'] is False
    assert row['publication']['all_current_source_history_complete_claim'] is False
    restored = fetch_packed_snapshot(cold, repo / 'restore', fixed)
    assert (restored / 'raw.txt').read_text() == 'public'
    assert json.loads((restored / 'release.json').read_text()) == initial
    assert (source / 'raw.txt').read_text() == 'new uncommitted observations'


@pytest.mark.parametrize('changed', ['role', 'source', 'excluded_subtrees', 'freshness', 'publish'])
def test_committed_catalog_reuse_never_admits_changed_scope_rights_or_gates(setup, changed):
    _, cold, entries, args, _ = setup
    entry = entries[0]
    _, saved = publisher._publish_entry(entry, args, cold)
    variants = {'role': 'different-role', 'source': str(Path(entry['source']).with_name('other')),
                'excluded_subtrees': ['raw'], 'freshness': {'field': 'other'}, 'publish': False}
    revised = {**entry, changed: variants[changed]}
    assert migration._committed_catalog_release(revised, args, cold) is None
    assert resolve_latest_packed(cold, entry['dataset']).manifest_sha256 == saved.manifest_sha256


def test_unbound_legacy_catalog_release_gets_one_contract_upgrade_not_payload_duplication(setup):
    _, cold, entries, args, _ = setup
    entry = entries[0]
    legacy = publish_packed_snapshot(cold, entry['dataset'], Path(entry['source']), node_id='test',
                                    file_chunk_bytes=entry['file_chunk_bytes'], pack_buckets=1)
    assert migration._committed_catalog_release(entry, args, cold) is None
    _, admitted = publisher._publish_entry(entry, args, cold)
    assert admitted.manifest['snapshot_id'] != legacy.manifest['snapshot_id']
    assert admitted.manifest['archive']['inventory']['sha256'] == legacy.manifest['archive']['inventory']['sha256']
    assert admitted.manifest['archive']['new_stored_bytes'] == 0
    reused = migration._committed_catalog_release(entry, args, cold)
    assert reused.manifest_sha256 == admitted.manifest_sha256
    _, unchanged = publisher._publish_entry(entry, args, cold)
    assert unchanged.manifest_sha256 == admitted.manifest_sha256


def test_committed_catalog_reuse_normalizes_the_original_selection_without_expanding_it(setup):
    _, cold, entries, args, _ = setup
    entry = {**entries[0], 'included_root_files': ['second.txt', 'raw.txt']}
    (Path(entry['source']) / 'second.txt').write_text('selected')
    (Path(entry['source']) / 'not-selected.txt').write_text('must remain local')
    _, admitted = publisher._publish_entry(entry, args, cold)
    reused = migration._committed_catalog_release(entry, args, cold)
    assert reused.manifest_sha256 == admitted.manifest_sha256
    assert reused.manifest['source']['files'] == 2


def test_a_matching_catalog_marker_cannot_hide_a_wrong_reader_chunk_contract(setup):
    _, cold, entries, args, _ = setup
    entry = entries[0]
    publish_packed_snapshot(cold, entry['dataset'], Path(entry['source']), node_id='test',
        file_chunk_bytes=16, pack_buckets=1, metadata={'catalog_schema': '1', 'role': entry['role'],
            'catalog_publication_contract': publisher.CATALOG_PUBLICATION_CONTRACT,
            'catalog_entry_sha256': publisher.catalog_publication_identity(entry)})
    with pytest.raises(SnapshotError, match='disagrees with its enrolled contract'):
        migration._committed_catalog_release(entry, args, cold)


def test_committed_catalog_identity_never_replaces_current_full_object_and_original_proof(setup):
    repo, cold, entries, args, _ = setup
    _, fixed = publisher._publish_entry(entries[0], args, cold)
    (cold / fixed.manifest['archive']['objects'][0]['relpath']).write_bytes(b'corrupt retained archive')
    result = migration.migrate_catalog(repo, args, entries)
    assert result['datasets']['public']['state'] == 'waiting_retry'
    assert result['datasets']['public']['snapshot_id'] == fixed.manifest['snapshot_id']
    assert result['datasets']['private']['state'] == 'verified'
    assert result['verified_datasets'] == 1
    assert not Path(args.migration_state / 'recovery/public.json').exists()


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


@pytest.mark.parametrize('memory_rows', [1, 10000])
@pytest.mark.parametrize('root,controls', [
    ('data_crypto_reference', ['state/providers/etherscan.json', 'state/datasets/supply.json']),
    ('data_finlab', ['receipts/prices.json', 'raw_receipts/after_market_fixed_price.json',
                     'attempts/prices.json',
                     'intraday/attempts/tw_minute-0103/2026-09-21.json',
                     'intraday/receipts/tw_minute-0103/2026-09-21.json',
                     'intraday/derived_minute/receipts/tw_tick-0103/2026-09-21.json']),
    ('data_finmind', ['receipts/TaiwanStockInfo/2026-10-19.json',
                      'complement/receipts/TaiwanStockNews/all/2026-10-19.json',
                      'sponsor/receipts/TaiwanStockPrice/all/2026-10-19.json']),
    ('data_tej', ['progress/task-attempt.json.progress.json']),
])
def test_named_provider_controls_stream_into_cold_without_millions_of_freeze_files(
        setup, monkeypatch, root, controls, memory_rows):
    from stockagent.data_sync import packed_snapshots as packed
    monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', memory_rows)
    repo, cold, entries, args, policy = setup
    source = repo / root
    Path(entries[1]['source']).rename(source)
    entry = {**entries[1], 'source': str(source)}
    for logical in controls:
        path = source / logical
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"value":1}')
    raw = source / 'raw/original.json'
    raw.parent.mkdir()
    raw.write_text('{"unmodified_original":true}')
    original = packed._write_pack
    def while_packing(root, selected, temporary, **options):
        for logical in controls:
            (source / logical).write_text('{"value":2000}')
        result = original(root, selected, temporary, **options)
        for logical in controls:
            (source / logical).write_text('{"value":3}')
        return result
    monkeypatch.setattr(packed, '_write_pack', while_packing)
    result = migration.migrate_catalog(repo, args, [entry])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    saved = resolve_latest_packed(private, 'private')
    assert saved.manifest['source']['selection']['atomic_json_observed_files'] == len(controls)
    restored = fetch_packed_snapshot(private, repo / 'restored', saved)
    assert all((restored / name).read_text() == '{"value":2000}' for name in controls)
    assert all((source / name).read_text() == '{"value":3}' for name in controls)
    assert saved.manifest['source']['logical_bytes'] == sum(p.stat().st_size for p in restored.rglob('*') if p.is_file())
    published = json.loads((args.migration_state / 'publication/private.json').read_text())
    assert published['logical_bytes_processed'] == published['logical_bytes_total'] == saved.manifest['source']['logical_bytes']
    preparation = json.loads((args.migration_state / 'preparations/private.json').read_bytes())
    assert not set(controls).intersection(preparation['controls'])
    monkeypatch.setattr(packed, '_write_pack', original)
    # Changed control bytes make one incremental release; unchanged bytes reuse it.
    progress = []
    revised = packed.publish_packed_snapshot(private, 'private', source, pack_buckets=1,
        file_chunk_bytes=policy['file_chunk_bytes'], private_observation_cohort=True,
        private_atomic_json_controls=True, progress=progress.append)
    assert revised.manifest_sha256 != saved.manifest_sha256
    assert progress[-1]['logical_bytes_processed'] == progress[-1]['logical_bytes_total'] == revised.manifest['source']['logical_bytes']
    packed.verify_packed_snapshot(private, revised, reconstruct_all=True)
    unchanged = packed.publish_packed_snapshot(private, 'private', source, pack_buckets=1,
        file_chunk_bytes=policy['file_chunk_bytes'], private_observation_cohort=True,
        private_atomic_json_controls=True)
    assert unchanged.manifest_sha256 == revised.manifest_sha256
    assert raw.read_text() == '{"unmodified_original":true}'


@pytest.mark.parametrize('source_name', ['data_finlab', 'data_finmind', 'data_tej', 'data_crypto_reference'])
def test_provider_control_scope_does_not_admit_raw_receipts_by_basename(tmp_path, source_name):
    source = tmp_path / source_name
    (source / 'raw/receipts').mkdir(parents=True)
    (source / 'raw/receipts/2026-10-19.json').write_text('{"price":-7}')
    capture = migration._freeze_controls(source, tmp_path / 'private/capture', [])
    assert not capture


@pytest.mark.parametrize('fault', ['none', 'atomic_replace', 'in_place_write', 'invalid_json', 'redirected'])
def test_inline_control_fd_observation_is_bounded_and_does_not_create_a_copy(tmp_path, monkeypatch, fault):
    import os
    from stockagent.data_sync import packed_snapshots as packed
    source = tmp_path / 'data_finmind'
    original = source / 'complement/receipts/market/all/current.json'
    original.parent.mkdir(parents=True)
    original.write_bytes(b'{"price":-7,"volume":null}')
    replacement = tmp_path / 'replacement.json'
    replacement.write_bytes(b'{"value":2}')
    native = os.fstat
    touched = []
    def change_after_open(descriptor):
        info = native(descriptor)
        if os.readlink(f'/proc/self/fd/{descriptor}') == str(original):
            if fault == 'atomic_replace' and not touched:
                os.replace(replacement, original)
                touched.append(True)
            elif fault == 'in_place_write':
                original.write_bytes(b'{"value":' + str(len(touched) + 1).encode() + b'}')
                touched.append(True)
        return info
    monkeypatch.setattr(packed.os, 'fstat', change_after_open)
    if fault == 'invalid_json':
        original.write_bytes(b'{"incomplete":')
    if fault == 'redirected':
        original.unlink()
        original.symlink_to(replacement)
    entry = packed._SourceEntry(path=original.relative_to(source).as_posix(),
                               kind='file', mode=0o644, atomic_json_control=True)
    if fault in {'invalid_json', 'redirected', 'in_place_write'}:
        with pytest.raises((SnapshotError, OSError, ValueError)):
            packed._read_atomic_json_control(source, entry)
    else:
        raw = packed._read_atomic_json_control(source, entry)
        assert raw == b'{"price":-7,"volume":null}'
        assert entry.size == len(raw)
        if fault == 'atomic_replace':
            assert original.read_bytes() == b'{"value":2}'
    assert list(original.parent.iterdir()) == [original]


def test_inline_controls_are_never_admitted_to_shared_publication(setup):
    _, cold, entries, _, _ = setup
    with pytest.raises(SnapshotError, match='private cohort'):
        publish_packed_snapshot(cold, 'public', Path(entries[0]['source']),
                                private_atomic_json_controls=True)


def test_inline_control_option_still_rejects_changed_raw_payload(setup, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    repo, _, entries, args, _ = setup
    source = repo / 'data_finmind'
    Path(entries[1]['source']).rename(source)
    raw = source / 'raw/receipts/original.json'
    raw.parent.mkdir(parents=True)
    raw.write_text('{"original":-7}')
    original = packed._write_pack
    def mutated(root, selected, temporary, **options):
        result = original(root, selected, temporary, **options)
        raw.write_text('{"original":-7000}')
        return result
    monkeypatch.setattr(packed, '_write_pack', mutated)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] != 'verified'
    assert result['datasets']['private']['state'] == 'waiting_retry'
    assert raw.read_text() == '{"original":-7000}'


def test_inline_control_rejects_oversized_file_before_reading(tmp_path):
    from stockagent.data_sync import packed_snapshots as packed
    source = tmp_path / 'data_finmind'
    path = source / 'receipts/oversized.json'
    path.parent.mkdir(parents=True)
    with path.open('wb') as stream:
        stream.truncate(64 * 1024**2 + 1)
    entry = packed._SourceEntry(path='receipts/oversized.json', kind='file',
                               mode=0o644, atomic_json_control=True)
    with pytest.raises(SnapshotError, match='bounded regular-file'):
        packed._read_atomic_json_control(source, entry)


def test_public_catalog_scope_upgrade_keeps_complete_jsonl_and_old_observations(tmp_path, monkeypatch):
    source = tmp_path / 'data_tw_public'
    (source / 'metadata').mkdir(parents=True)
    (source / 'raw').mkdir()
    (source / 'status.json').write_bytes(b'{"old":true}')
    stage = tmp_path / 'private/captures'
    old = migration._freeze_controls(source, stage, [])
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v8', 'source': str(source), 'excluded': []}))
    (source / 'status.json').write_bytes(b'{"new":true}')
    metadata = b'{"title":"official catalog","publishedDate":"2026-10-16"}'
    journal = b'{"value":-7,"missing":null}\n{"value":3}\n'
    (source / 'metadata/mof_customs_trade.json').write_bytes(metadata)
    (source / 'metadata/catalog_versions.jsonl').write_bytes(journal)
    (source / 'raw/catalog_versions.jsonl').write_bytes(b'not a control')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    frozen = migration._freeze_controls(source, stage, [], existing=old,
                                        upgrade_from='recursive_mutable_controls_v8')
    assert set(frozen) == {'status.json', 'metadata/mof_customs_trade.json', 'metadata/catalog_versions.jsonl'}
    assert Path(frozen['status.json']['path']).read_bytes() == b'{"old":true}'
    assert Path(frozen['metadata/mof_customs_trade.json']['path']).read_bytes() == metadata
    assert Path(frozen['metadata/catalog_versions.jsonl']['path']).read_bytes() == journal
    assert (source / 'raw/catalog_versions.jsonl').read_bytes() == b'not a control'


@pytest.mark.parametrize('source_name,controls', [
    ('data_finmind', ['json_pipeline_last.json', 'complement/json_pipeline_last.json',
                      'sponsor/json_pipeline_last.json']),
    ('data_openBB', ['_state/public_priority_cursors.json',
                     '_state/public_priority_queue_event.json', '_state/public_priority_plan.json']),
])
def test_v10_control_upgrade_probes_exact_atomic_producer_heads_only(tmp_path, monkeypatch,
                                                                  source_name, controls):
    source = tmp_path / source_name
    source.mkdir()
    (source / 'status.json').write_bytes(b'{"old":true}')
    stage = tmp_path / 'private/captures'
    captured = migration._freeze_controls(source, stage, [])
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v9', 'source': str(source), 'excluded': []}))
    (source / 'status.json').write_bytes(b'{"current":true}')
    for name in controls:
        path = source / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b'{"value":-7,"missing":null}')
    raw = source / 'raw/json_pipeline_last.json'
    raw.parent.mkdir()
    raw.write_bytes(b'{"original":true}')
    unknown = source / '_state/unknown_raw.json'
    unknown.parent.mkdir(exist_ok=True)
    unknown.write_bytes(b'{"original":true}')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    upgraded = migration._freeze_controls(source, stage, [], existing=captured,
                                         upgrade_from='recursive_mutable_controls_v9')
    assert set(upgraded) == {'status.json', *controls}
    assert Path(upgraded['status.json']['path']).read_bytes() == b'{"old":true}'
    assert all(Path(upgraded[name]['path']).read_bytes() == b'{"value":-7,"missing":null}'
               for name in controls)
    assert migration._freeze_controls(source, stage, [], existing=upgraded,
                                     upgrade_from=migration.CONTROL_SCOPE) == upgraded
    assert raw.read_bytes() == unknown.read_bytes() == b'{"original":true}'


@pytest.mark.parametrize('source_name,controls', [
    ('data_finmind', ['complement/json_pipeline_last.json']),
    ('data_openBB', ['_state/public_priority_cursors.json', '_state/public_priority_plan.json',
                     '_state/public_priority_queue_event.json']),
])
def test_v10_atomic_queue_updates_preserve_exact_cold_roundtrip(setup, monkeypatch,
                                                              source_name, controls):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / source_name
    Path(entries[1]['source']).rename(source)
    for name in controls:
        path = source / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b'{"value":-7,"missing":null}')
    original = packed._write_pack
    def updated(root, selected, temporary, **options):
        for name in controls:
            (source / name).write_bytes(b'{"current":true}')
        return original(root, selected, temporary, **options)
    monkeypatch.setattr(packed, '_write_pack', updated)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert all((restored / name).read_bytes() == b'{"value":-7,"missing":null}' for name in controls)
    assert all((source / name).read_bytes() == b'{"current":true}' for name in controls)


@pytest.mark.parametrize('source_name,controls', [
    ('data_openBB', {'_state/l1_compaction_attempt_latest.json': b'{"state":"running"}',
                     '_state/l1_compaction_latest.json': b'{"count":-7,"missing":null}'}),
    ('data_tw_public', {'state/journals/tpex_daily_ohlcv.jsonl':
                       b'{"rows":7,"missing":null}\n{"rows":-3}\n'}),
    ('data_finlab', {'runs/latest.json': b'{"state":"running","active_key":"price"}'}),
    ('data_tw_public', {'features/tw_public_stock_daily.summary.json': b'{"rows":7}',
                       'features/tw_public_research_wide_2014_v1.research.json': b'{"rows":8}',
                       'features/tw_public_research_wide_2014_v1.parquet': b'closed original table'}),
])
def test_v11_control_delta_keeps_old_capture_and_never_rewalks_payload(
        tmp_path, monkeypatch, source_name, controls):
    source = tmp_path / source_name
    source.mkdir()
    (source / 'status.json').write_bytes(b'{"old":true}')
    stage = tmp_path / 'private/captures'
    previous = migration._freeze_controls(source, stage, [])
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v10', 'source': str(source), 'excluded': []}))
    (source / 'status.json').write_bytes(b'{"current":true}')
    for name, raw in controls.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    unknown = source / 'raw/state/journals/tpex_daily_ohlcv.jsonl'
    unknown.parent.mkdir(parents=True)
    unknown.write_bytes(b'not a control')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    selected = migration._freeze_controls(source, stage, [], existing=previous,
                                         upgrade_from='recursive_mutable_controls_v10')
    assert set(selected) == {'status.json', *controls}
    assert Path(selected['status.json']['path']).read_bytes() == b'{"old":true}'
    for name, raw in controls.items():
        assert Path(selected[name]['path']).read_bytes() == raw
        (source / name).write_bytes(raw + (b'{"later":true}\n' if name.endswith('.jsonl') else b' '))
    assert migration._freeze_controls(source, stage, [], existing=selected,
                                     upgrade_from=migration.CONTROL_SCOPE) == selected
    assert unknown.read_bytes() == b'not a control'


@pytest.mark.parametrize('source_name,logical,payload,changed', [
    ('data_tw_public', 'state/journals/tpex_daily_ohlcv.jsonl',
     b'{"rows":-7,"missing":null}\n', b'{"rows":-7,"missing":null}\n{"rows":2}\n'),
    ('data_openBB', '_state/l1_compaction_attempt_latest.json',
     b'{"state":"running"}', b'{"state":"completed"}'),
    ('data_openBB', '_state/l1_compaction_latest.json',
     b'{"count":-7,"missing":null}', b'{"count":30}'),
    ('data_finlab', 'runs/latest.json',
     b'{"state":"running","active_key":"price"}', b'{"state":"finished"}'),
    ('data_tw_public', 'features/tw_public_stock_daily.summary.json',
     b'{"rows":7,"missing":null}', b'{"rows":8}'),
    ('data_tw_public', 'features/tw_public_research_wide_2014_v1.research.json',
     b'{"rows":7,"unit":"shares"}', b'{"rows":8}'),
    ('data_tw_public', 'features/tw_public_research_wide_2014_v1.parquet',
     b'closed original table', b'new closed table'),
])
def test_v11_producer_updates_roundtrip_complete_selected_original(
        setup, monkeypatch, source_name, logical, payload, changed):
    import hashlib
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / source_name
    Path(entries[1]['source']).rename(source)
    path = source / logical
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    original = packed._write_pack
    def while_packing(root, selected, temporary, **options):
        path.write_bytes(changed)
        return original(root, selected, temporary, **options)
    monkeypatch.setattr(packed, '_write_pack', while_packing)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    saved = resolve_latest_packed(private, 'private')
    restored = fetch_packed_snapshot(private, repo / 'restored', saved)
    assert (restored / logical).read_bytes() == payload
    assert path.read_bytes() == changed
    preparation = json.loads((args.migration_state / 'preparations/private.json').read_bytes())
    assert preparation['controls'][logical]['sha256'] == hashlib.sha256(payload).hexdigest()
    assert preparation['control_scope'] == migration.CONTROL_SCOPE


@pytest.mark.parametrize('source_name,logical', [
    ('data_finlab', 'runs/latest.json'),
    ('data_tw_public', 'features/tw_public_stock_daily.summary.json'),
])
def test_v12_upgrade_probes_only_named_paths_and_keeps_v11_observations(
        tmp_path, monkeypatch, source_name, logical):
    source = tmp_path / source_name
    source.mkdir()
    (source / 'status.json').write_bytes(b'{"prior":true}')
    stage = tmp_path / 'private/captures'
    old = migration._freeze_controls(source, stage, [])
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v11', 'source': str(source), 'excluded': []}))
    path = source / logical
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'{"current":true}')
    (source / 'status.json').write_bytes(b'{"prior":false}')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    upgraded = migration._freeze_controls(source, stage, [], existing=old,
                                         upgrade_from='recursive_mutable_controls_v11')
    assert set(upgraded) == {'status.json', logical}
    assert Path(upgraded['status.json']['path']).read_bytes() == b'{"prior":true}'
    assert Path(upgraded[logical]['path']).read_bytes() == b'{"current":true}'
    assert migration._freeze_controls(source, stage, [], existing=upgraded,
                                     upgrade_from=migration.CONTROL_SCOPE) == upgraded


@pytest.mark.parametrize('source_name,logical', [
    ('data_finlab', 'raw/runs/latest.json'),
    ('data_tw_public', 'raw/features/tw_public_stock_daily.summary.json'),
    ('foreign_source', 'runs/latest.json'),
    ('data_tw_public', 'features/foreign.summary.json'),
    ('data_tw_public', 'features/foreign.parquet'),
])
def test_v12_named_companions_do_not_admit_foreign_or_raw_lookalikes(tmp_path, source_name, logical):
    source = tmp_path / source_name
    path = source / logical
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"not_a_control":true}')
    assert logical not in migration._freeze_controls(source, tmp_path / 'private/captures', [])


@pytest.mark.parametrize('source_name,logical,payload', [
    ('data_finlab', 'catalog/discovery.json', b'{"keys":["price"],"missing":null}'),
    ('data_openBB', 'logs/supervisor.log', b'original running log\n'),
    ('data_openBB', 'logs/supervisor.log.1', b'previous rotated log\n'),
    ('data_openBB', 'logs/supervisor_launcher.log', b'launcher original\n'),
    ('data_openBB', 'logs/supervisor-launch.log', b'boot original\n'),
    ('data_openBB', '_state/monitor_history.jsonl', b'{"observed":-7,"missing":null}\n'),
    ('data_openBB', '_state/monitor_dashboard_history.jsonl', b'{"progress":0}\n'),
    ('data_openBB', '_state/provider_scheduler.json', b'{"quota":-7,"missing":null}'),
    ('data_openBB', '_state/provider_cooldowns.json', b'{"until":null}'),
    ('data_openBB', '_state/monitor_latest.json', b'{"complete":false}'),
    ('data_openBB', '_state/audit_latest.json', b'{"complete":false}'),
    ('data_openBB', '_state/archive_end_date.txt', b'2026-10-21\n'),
    ('data_openBB', '_state/supervisor.pid', b'4321\n'),
    ('data_openBB', '_state/openbb_l1_compaction.lock', b'{"epoch":1}'),
    ('data_openBB', 'catalog/completeness_contract_summary.json', b'{"passed":false}'),
    ('data_openBB', 'catalog/provider_eta.parquet', b'closed monitor projection'),
    ('data_openBB', 'catalog/coverage.parquet', b'closed catalog projection'),
    ('data_openBB', 'catalog/l1_compaction_audit.parquet', b'closed audit projection'),
])
def test_v13_producer_controls_survive_changes_and_full_original_cold_recovery(
        setup, monkeypatch, source_name, logical, payload):
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync import materialized_cache
    # These owned fixtures exercise byte preservation and retirement, not the
    # live host's unrelated /proc permissions. Incomplete scans are covered by
    # the explicit rejection test below; production keeps its fail-closed scan.
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [])
    repo, cold, entries, args, policy = setup
    args.retire_staging = True
    source = repo / source_name
    Path(entries[1]['source']).rename(source)
    path = source / logical
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    native = packed._write_pack
    def changed(root, selected, temporary, **options):
        result = native(root, selected, temporary, **options)
        path.write_bytes(payload + b'updated during long packing\n')
        return result
    monkeypatch.setattr(packed, '_write_pack', changed)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    row = result['datasets']['private']
    proof = json.loads(Path(row['recovery_receipt']).read_bytes())
    assert proof['full_original_file_reconstruction_verified'] is True
    assert row['owned_scratch_retirement']['state'] == 'retired'
    private = migration.private_history_root(cold, policy)
    selected = resolve_latest_packed(private, 'private')
    restored = fetch_packed_snapshot(private, repo / 'restored', selected)
    assert (restored / logical).read_bytes() == payload
    assert path.read_bytes() == payload + b'updated during long packing\n'


@pytest.mark.parametrize('source_name,logical,payload', [
    ('data_finlab', 'catalog/discovery.json', b'{"keys":[]}'),
    ('data_openBB', 'logs/supervisor.log', b'first log\n'),
    ('data_openBB', '_state/monitor_history.jsonl', b'{"first":true}\n'),
    ('data_openBB', 'catalog/provider_eta.parquet', b'closed projection'),
])
def test_v13_prepared_delta_never_recensuses_or_replaces_v12_captures(
        tmp_path, monkeypatch, source_name, logical, payload):
    source = tmp_path / source_name
    source.mkdir()
    (source / 'status.json').write_bytes(b'{"first":true}')
    stage = tmp_path / 'private/captures'
    old = migration._freeze_controls(source, stage, [])
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v12', 'source': str(source), 'excluded': []}))
    path = source / logical
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    (source / 'status.json').write_bytes(b'{"first":false}')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw tree walked again'))
    captured = migration._freeze_controls(source, stage, [], existing=old,
                                         upgrade_from='recursive_mutable_controls_v12')
    assert set(captured) == {'status.json', logical}
    assert Path(captured['status.json']['path']).read_bytes() == b'{"first":true}'
    assert Path(captured[logical]['path']).read_bytes() == payload
    path.write_bytes(b'new operational observation\n')
    assert migration._freeze_controls(source, stage, [], existing=captured,
                                      upgrade_from=migration.CONTROL_SCOPE) == captured


FINLAB_INTRADAY_CONTROLS = ('minute_cursor.json', 'market_cursor.json',
                           'source_backoff.json', 'batch_metrics.json',
                           'batch_metrics_seed.json')


@pytest.mark.parametrize('name', FINLAB_INTRADAY_CONTROLS)
def test_v14_finlab_intraday_controls_complete_original_recovery_and_retirement(
        setup, monkeypatch, name):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    args.retire_staging = True
    source = repo / 'data_finlab'
    Path(entries[1]['source']).rename(source)
    path = source / 'intraday' / name
    path.parent.mkdir()
    original = b'{"frontier":-7,"missing":null,"batches":[]}\n'
    path.write_bytes(original)
    native = packed._write_pack
    def changed(root, selected, temporary, **options):
        result = native(root, selected, temporary, **options)
        path.write_bytes(b'{"frontier":99,"batches":[1]}\n')
        return result
    monkeypatch.setattr(packed, '_write_pack', changed)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    row = result['datasets']['private']
    proof = json.loads(Path(row['recovery_receipt']).read_bytes())
    assert proof['full_original_file_reconstruction_verified'] is True
    assert row['owned_scratch_retirement']['state'] == 'retired'
    private = migration.private_history_root(cold, policy)
    selected = resolve_latest_packed(private, 'private')
    restored = fetch_packed_snapshot(private, repo / 'restored', selected)
    assert (restored / 'intraday' / name).read_bytes() == original
    assert path.read_bytes() == b'{"frontier":99,"batches":[1]}\n'


@pytest.mark.parametrize('old_scope', ['recursive_mutable_controls_v12',
                                      'recursive_mutable_controls_v13',
                                      'recursive_mutable_controls_v14'])
def test_v14_preparation_upgrade_retains_native_database_and_only_probes_names(setup, monkeypatch, old_scope):
    repo, cold, entries, args, policy = setup
    source = repo / 'data_finlab'
    Path(entries[1]['source']).rename(source)
    entry = {**entries[1], 'source': str(source)}
    (source / 'status.json').write_bytes(b'{"old":true}')
    database = sqlite3.connect(source / 'state.sqlite3')
    try:
        database.execute('CREATE TABLE observations(value REAL)')
        database.executemany('INSERT INTO observations VALUES (?)', [(-7.0,), (None,)])
        database.commit()
    finally:
        database.close()
    private = migration.private_history_root(cold, policy)
    prepared = migration._private_preparation(entry, args, source, private, policy)
    database_bindings = json.loads(json.dumps(prepared['sqlite_states']))
    assert database_bindings
    prepared['control_scope'] = old_scope
    migration._preparation_path(args, 'private').write_text(json.dumps(prepared))
    stage = Path(prepared['controls_staging'])
    intent_path = stage.with_suffix('.intent.json')
    intent = json.loads(intent_path.read_bytes())
    intent['contract'] = old_scope
    intent_path.write_text(json.dumps(intent))
    intraday = source / 'intraday'
    intraday.mkdir()
    for name in FINLAB_INTRADAY_CONTROLS:
        (intraday / name).write_bytes(b'{"first":-7,"missing":null}')
    (source / 'status.json').write_bytes(b'{"current":true}')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    monkeypatch.setattr(migration, '_snapshot_sqlite', lambda *_a, **_k: pytest.fail('native DB recopied'))
    upgraded = migration._private_preparation(entry, args, source, private, policy)
    assert upgraded['control_scope'] == migration.CONTROL_SCOPE
    assert upgraded['sqlite_states'] == database_bindings
    assert set(upgraded['controls']) == {'status.json', *(f'intraday/{name}' for name in FINLAB_INTRADAY_CONTROLS)}
    assert Path(upgraded['controls']['status.json']['path']).read_bytes() == b'{"old":true}'
    for name in FINLAB_INTRADAY_CONTROLS:
        (intraday / name).write_bytes(b'{"second":true}')
    retried = migration._private_preparation(entry, args, source, private, policy)
    assert retried == upgraded
    assert all(Path(retried['controls'][f'intraday/{name}']['path']).read_bytes()
               == b'{"first":-7,"missing":null}' for name in FINLAB_INTRADAY_CONTROLS)


@pytest.mark.parametrize('name', FINLAB_INTRADAY_CONTROLS)
def test_v14_finlab_intraday_capture_honors_exclusions_and_validates_json(tmp_path, name):
    source = tmp_path / 'data_finlab'
    path = source / 'intraday' / name
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"partial":')
    assert migration._freeze_controls(source, tmp_path / 'excluded/captures', ['intraday'],
                                     upgrade_from='recursive_mutable_controls_v13') == {}
    stage = tmp_path / 'private/captures'
    with pytest.raises(ValueError):
        migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v13')
    assert not (stage / 'intraday' / name).exists()
    assert path.read_bytes() == b'{"partial":'


@pytest.mark.parametrize('name', ['head.json', 'repair_plan.json'])
def test_v15_finmind_announcement_heads_upgrade_without_recopying_state(setup, monkeypatch, name):
    repo, cold, entries, args, policy = setup
    source = repo / 'data_finmind'
    Path(entries[1]['source']).rename(source)
    entry = {**entries[1], 'source': str(source)}
    (source / 'status.json').write_bytes(b'{"first":true}')
    with sqlite3.connect(source / 'state.sqlite3') as database:
        database.execute('CREATE TABLE observations(value REAL)')
        database.execute('INSERT INTO observations VALUES (-7)')
    private = migration.private_history_root(cold, policy)
    prepared = migration._private_preparation(entry, args, source, private, policy)
    bindings = json.loads(json.dumps(prepared['sqlite_states']))
    prepared['control_scope'] = 'recursive_mutable_controls_v14'
    migration._preparation_path(args, 'private').write_text(json.dumps(prepared))
    stage = Path(prepared['controls_staging'])
    intent_path = stage.with_suffix('.intent.json')
    intent = json.loads(intent_path.read_bytes())
    intent['contract'] = prepared['control_scope']
    intent_path.write_text(json.dumps(intent))
    head = source / 'announcements' / name
    head.parent.mkdir()
    original = b'{"requests":[],"value":-7,"missing":null}\n'
    head.write_bytes(original)
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    monkeypatch.setattr(migration, '_snapshot_sqlite', lambda *_a, **_k: pytest.fail('native DB recopied'))
    upgraded = migration._private_preparation(entry, args, source, private, policy)
    assert upgraded['control_scope'] == migration.CONTROL_SCOPE
    assert upgraded['sqlite_states'] == bindings
    logical = head.relative_to(source).as_posix()
    assert set(upgraded['controls']) == {'status.json', logical}
    assert Path(upgraded['controls'][logical]['path']).read_bytes() == original
    head.write_bytes(b'{"new":true}')
    assert migration._private_preparation(entry, args, source, private, policy) == upgraded


@pytest.mark.parametrize('name', ['head.json', 'repair_plan.json'])
def test_v15_finmind_announcement_heads_reject_incomplete_json_and_honor_exclusions(tmp_path, name):
    source = tmp_path / 'data_finmind'
    path = source / 'announcements' / name
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"partial":')
    assert migration._freeze_controls(source, tmp_path / 'excluded/captures', ['announcements'],
                                     upgrade_from='recursive_mutable_controls_v14') == {}
    stage = tmp_path / 'private/captures'
    with pytest.raises(ValueError):
        migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v14')
    assert not (stage / path.relative_to(source)).exists()
    assert path.read_bytes() == b'{"partial":'


@pytest.mark.parametrize('name', ['head.json', 'repair_plan.json'])
def test_v15_finmind_announcement_heads_keep_first_bytes_through_complete_recovery(setup, monkeypatch, name):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    args.retire_staging = True
    source = repo / 'data_finmind'
    Path(entries[1]['source']).rename(source)
    path = source / 'announcements' / name
    path.parent.mkdir()
    original = b'{"requests":[],"value":-7,"missing":null}\n'
    path.write_bytes(original)
    native = packed._write_pack
    def changed(root, selected, temporary, **options):
        result = native(root, selected, temporary, **options)
        path.write_bytes(b'{"requests":[1],"value":99}')
        return result
    monkeypatch.setattr(packed, '_write_pack', changed)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    row = result['datasets']['private']
    assert json.loads(Path(row['recovery_receipt']).read_bytes())['full_original_file_reconstruction_verified']
    assert row['owned_scratch_retirement']['state'] == 'retired'
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert (restored / path.relative_to(source)).read_bytes() == original
    assert path.read_bytes() == b'{"requests":[1],"value":99}'


@pytest.mark.parametrize('key', ['dividend_otc:權息', 'management_change_events:變更交易開始日'])
def test_v17_finlab_empty_evidence_keeps_original_control_and_raw_through_recovery(setup, monkeypatch, key):
    from stockagent.data.finlab_acquisition_contract import safe_stem
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    args.retire_staging = True
    source = repo / 'data_finlab'
    Path(entries[1]['source']).rename(source)
    directory = source / 'empty_evidence'
    directory.mkdir()
    name = safe_stem(key) + '-' + 'a' * 64
    report = directory / (name + '.json')
    raw = directory / (name + '.feather')
    original = b'{"raw_non_null_values":0,"usable_observations":false,"observed_at_utc":"first"}'
    report.write_bytes(original)
    raw.write_bytes(b'exact immutable null-source evidence')
    native = packed._write_pack
    def changed(root, selected, temporary, **options):
        result = native(root, selected, temporary, **options)
        report.write_bytes(b'{"raw_non_null_values":0,"observed_at_utc":"later"}')
        return result
    monkeypatch.setattr(packed, '_write_pack', changed)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    row = result['datasets']['private']
    assert row['owned_scratch_retirement']['state'] == 'retired'
    assert json.loads(Path(row['recovery_receipt']).read_bytes())['full_original_file_reconstruction_verified']
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert (restored / report.relative_to(source)).read_bytes() == original
    assert (restored / raw.relative_to(source)).read_bytes() == raw.read_bytes()
    assert report.read_bytes() != original


def test_v17_finlab_empty_evidence_upgrade_probes_exact_names_only(tmp_path, monkeypatch):
    from stockagent.data.finlab_acquisition_contract import safe_stem
    source = tmp_path / 'data_finlab'
    evidence = source / 'empty_evidence'
    evidence.mkdir(parents=True)
    valid = safe_stem('management_change_events:變更交易開始日') + '-' + 'b' * 64 + '.json'
    names = [valid, 'arbitrary.json', valid.replace('b' * 64, 'X' * 64),
             safe_stem('other:key') + '-' + 'b' * 64 + '.json', valid.replace('.json', '.feather')]
    for name in names:
        (evidence / name).write_bytes(b'{"value":-3,"missing":null}')
    stage = tmp_path / 'private/controls'
    stage.parent.mkdir()
    stage.with_suffix('.intent.json').write_text(json.dumps({
        'contract': 'recursive_mutable_controls_v16', 'source': str(source), 'excluded': []}))
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw/database census on upgrade'))
    captured = migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v16')
    assert set(captured) == {'empty_evidence/' + valid}
    original = (evidence / valid).read_bytes()
    (evidence / valid).write_bytes(b'{"new":true}')
    assert migration._freeze_controls(source, stage, [], existing=captured,
        upgrade_from='recursive_mutable_controls_v16') == captured
    assert Path(captured['empty_evidence/' + valid]['path']).read_bytes() == original
    assert migration._freeze_controls(source, tmp_path / 'excluded/controls', ['empty_evidence'],
        upgrade_from='recursive_mutable_controls_v16') == {}


def test_v17_finlab_empty_evidence_rejects_redirection_and_partial_json(tmp_path):
    from stockagent.data.finlab_acquisition_contract import safe_stem
    source = tmp_path / 'data_finlab'
    evidence = source / 'empty_evidence'
    evidence.mkdir(parents=True)
    name = safe_stem('dividend_otc:權息') + '-' + 'a' * 64 + '.json'
    (evidence / name).write_bytes(b'{"unfinished":')
    with pytest.raises(ValueError):
        migration._freeze_controls(source, tmp_path / 'partial/controls', [],
            upgrade_from='recursive_mutable_controls_v16')
    other = tmp_path / 'other'
    evidence.rename(other)
    evidence.symlink_to(other, target_is_directory=True)
    with pytest.raises(SnapshotError, match='empty evidence directory is redirected'):
        migration._freeze_controls(source, tmp_path / 'redirect/controls', [],
            upgrade_from='recursive_mutable_controls_v16')


def test_finlab_only_uncommitted_writer_siblings_may_disappear_during_full_recovery(setup, monkeypatch):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / 'data_finlab'
    Path(entries[1]['source']).rename(source)
    staging = ['datasets/.finlab-2c3a7_3z.parquet.tmp',
        'intraday/objects/.finlab-intraday-abcdefgh.parquet.tmp',
        'intraday/objects/.finlab-unit-abcdefgh.parquet.tmp',
        'intraday/objects/.finlab-derived-abcdefgh.parquet.tmp',
        'receipts/.finlab-abcdefgh.json.tmp',
        'intraday/batch_receipts/.finlab-abcdefgh.json.tmp',
        'intraday/receipts/symbol/.finlab-abcdefgh.json.tmp',
        'intraday/derived_minute/receipts/symbol/.finlab-abcdefgh.json.tmp',
        'intraday/derived_minute/versions/symbol/2026-10-22/.finlab-abcdefgh.json.tmp',
        'versions/symbol/.finlab-abcdefgh.json.tmp',
        'raw/tmpabcdefgh.feather.tmp', 'empty_evidence/tmpabcdefgh.tmp',
        'raw_receipts/.raw-abcdefgh', 'empty_evidence/.raw-abcdefgh']
    originals = {'datasets/price-sha.parquet': b'original parquet observation',
        'intraday/objects/symbol-day-sha.parquet': b'original intraday observation',
        'raw/original.feather': b'original raw response',
        'empty_evidence/original.feather': b'original null evidence',
        'raw_receipts/original.json': b'{"value":-7,"missing":null}',
        'raw/.finlab-abcdefgh.parquet.tmp': b'an unreviewed lookalike stays selected'}
    for logical, payload in {**dict.fromkeys(staging, b'uncommitted'), **originals}.items():
        target = source / logical
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    native = packed._write_pack
    def promoted(root, selected, temporary, **options):
        result = native(root, selected, temporary, **options)
        for logical in staging:
            (source / logical).unlink(missing_ok=True)  # Simulated original writer, not migration cleanup.
        return result
    monkeypatch.setattr(packed, '_write_pack', promoted)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    row = result['datasets']['private']
    assert row['source_files'] == len(originals) + 1  # The fixture's raw.txt also remains.
    assert json.loads(Path(row['recovery_receipt']).read_bytes())['full_original_file_reconstruction_verified']
    private = migration.private_history_root(cold, policy)
    selected = resolve_latest_packed(private, 'private')
    assert 'finlab_atomic_sibling_v1' in selected.manifest['source']['selection']['private_staging_file_rules']
    restored = fetch_packed_snapshot(private, repo / 'restored', selected)
    assert all((restored / logical).read_bytes() == payload for logical, payload in originals.items())
    assert all(not (restored / logical).exists() for logical in staging)
    assert all((source / logical).read_bytes() == payload for logical, payload in originals.items())


@pytest.mark.parametrize('source_name,logical', [
    ('data_finlab', 'raw/catalog/discovery.json'),
    ('foreign', 'catalog/discovery.json'),
    ('data_openBB', 'raw/logs/supervisor.log'),
    ('foreign', 'logs/supervisor.log'),
    ('data_openBB', 'logs/unreviewed.log'),
    ('data_openBB', '_state/unreviewed.jsonl'),
    ('data_openBB', 'catalog/unreviewed.parquet'),
    ('data_finlab', 'raw/intraday/batch_metrics.json'),
    ('data_finlab', 'intraday/unreviewed.json'),
    ('data_finlab', 'intraday/batch_receipts/fixed-batch.json'),
    ('foreign', 'intraday/batch_metrics.json'),
    ('data_finmind', 'raw/announcements/repair_plan.json'),
    ('data_finmind', 'announcements/checks/repair_plan.json'),
    ('data_finmind', 'announcements/unreviewed.json'),
    ('foreign', 'announcements/repair_plan.json'),
    ('data_finlab', 'datasets/final.parquet'),
    ('foreign', 'datasets/.finlab-2c3a7_3z.parquet.tmp'),
])
def test_v13_control_lookalikes_still_reject_raw_mutation(setup, monkeypatch, source_name, logical):
    from stockagent.data_sync import packed_snapshots as packed
    repo, _, entries, args, _ = setup
    source = repo / source_name
    Path(entries[1]['source']).rename(source)
    path = source / logical
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'original raw observation\n')
    native = packed._write_pack
    def changed(root, selected, temporary, **options):
        result = native(root, selected, temporary, **options)
        path.write_bytes(b'changed raw observation\n')
        return result
    monkeypatch.setattr(packed, '_write_pack', changed)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['datasets']['private']['state'] == 'waiting_retry'
    assert 'changed while packing' in result['datasets']['private']['error']


@pytest.mark.parametrize('payload', [b'{"partial":', b'{"complete":true}', b'not JSON\n',
                                     b'{"complete":true}\n{"partial":'])
def test_v13_streamed_journal_rejects_partial_records_and_keeps_source(tmp_path, payload):
    source = tmp_path / 'data_openBB'
    path = source / '_state/monitor_history.jsonl'
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    stage = tmp_path / 'private/captures'
    with pytest.raises((SnapshotError, ValueError)):
        migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v12')
    assert path.read_bytes() == payload
    assert not (stage / path.relative_to(source)).exists()
    assert not list(stage.rglob('*.partial'))


def test_v13_large_operational_log_has_bounded_memory_and_retains_first_capture(tmp_path):
    import tracemalloc
    source = tmp_path / 'data_openBB'
    path = source / 'logs/supervisor.log'
    path.parent.mkdir(parents=True)
    with path.open('wb') as stream:
        stream.truncate(70 * 1024**2)
    stage = tmp_path / 'private/captures'
    tracemalloc.start()
    try:
        captured = migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v12')
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    row = captured['logs/supervisor.log']
    assert row['bytes'] == 70 * 1024**2 and peak < 32 * 1024**2
    assert row['capture_kind'] == 'stable_operational_log'
    assert migration.sha256_file(path) == row['sha256']
    path.write_bytes(b'new log\n')
    # Recover an atomic installed capture after a crash before receipt save.
    assert migration._freeze_controls(source, stage, [], upgrade_from='recursive_mutable_controls_v12') == captured


@pytest.mark.parametrize('logical', ['logs/supervisor.log', '_state/monitor_history.jsonl',
                                     'catalog/discovery.json'])
def test_v13_named_observations_honor_excluded_trees(tmp_path, logical):
    source = tmp_path / ('data_finlab' if logical.startswith('catalog/') else 'data_openBB')
    path = source / logical
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"raw":true}\n')
    assert migration._freeze_controls(source, tmp_path / 'private/captures', [logical.split('/')[0]],
        upgrade_from='recursive_mutable_controls_v12') == {}


@pytest.mark.parametrize('large', [False, True])
def test_v13_nonblocking_regular_open_cannot_hang_on_substituted_fifo(tmp_path, monkeypatch, large):
    import os
    source = tmp_path / ('data_openBB' if large else 'data_finlab')
    path = source / ('logs/supervisor.log' if large else 'catalog/discovery.json')
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"first":true}\n')
    native = migration.os.open
    def replaced(target, flags, *args, **kwargs):
        if Path(target) == path:
            assert flags & os.O_NONBLOCK and flags & os.O_NOFOLLOW
            path.unlink()
            os.mkfifo(path)
        return native(target, flags, *args, **kwargs)
    monkeypatch.setattr(migration.os, 'open', replaced)
    with pytest.raises(SnapshotError, match='regular'):
        migration._freeze_controls(source, tmp_path / 'private/captures', [],
                                  upgrade_from='recursive_mutable_controls_v12')
    assert not list((tmp_path / 'private/captures').rglob('*.partial'))


@pytest.mark.parametrize('payload', [b'{"partial":', b'{"complete":true}', b'not JSON\n',
                                     b'{"complete":true}\n{"partial":'])
def test_v11_query_journal_rejects_partial_or_malformed_without_truncation(tmp_path, payload):
    source = tmp_path / 'data_tw_public'
    path = source / 'state/journals/tpex_daily_ohlcv.jsonl'
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    stage = tmp_path / 'private/captures'
    with pytest.raises((SnapshotError, ValueError)):
        migration._freeze_controls(source, stage, [])
    assert path.read_bytes() == payload
    assert not (stage / path.relative_to(source)).exists()


def test_v11_query_journal_delta_honors_excluded_subtree(tmp_path):
    source = tmp_path / 'data_tw_public'
    path = source / 'state/journals/tpex_daily_ohlcv.jsonl'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"rows":-7}\n')
    result = migration._freeze_controls(source, tmp_path / 'private/captures', ['state'],
                                        upgrade_from='recursive_mutable_controls_v10')
    assert not result


@pytest.mark.parametrize('source_name,logical', [
    ('data_tw_public', 'raw/state/journals/tpex_daily_ohlcv.jsonl'),
    ('unrelated', 'state/journals/tpex_daily_ohlcv.jsonl'),
    ('data_openBB', 'raw/l1_compaction_attempt_latest.json'),
])
def test_v11_similar_raw_or_foreign_paths_still_reject_payload_mutation(
        setup, monkeypatch, source_name, logical):
    from stockagent.data_sync import packed_snapshots as packed
    repo, _, entries, args, _ = setup
    source = repo / source_name
    Path(entries[1]['source']).rename(source)
    path = source / logical
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'{"original":-7}\n')
    original = packed._write_pack
    def changed(root, selected, temporary, **options):
        result = original(root, selected, temporary, **options)
        path.write_bytes(b'{"original":-7000}\n')
        return result
    monkeypatch.setattr(packed, '_write_pack', changed)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['datasets']['private']['state'] == 'waiting_retry'
    assert path.read_bytes() == b'{"original":-7000}\n'


def test_owned_scratch_progress_distinguishes_retirement_from_recovery(tmp_path, monkeypatch):
    import hashlib
    from stockagent.data_sync import materialized_cache
    operation = tmp_path / 'operation'
    capture = operation / 'controls/owned/status.json'
    capture.parent.mkdir(parents=True)
    raw = b'{"raw":-7,"missing":null}'
    capture.write_bytes(raw)
    observations = []
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [])
    result = migration._retire_owned_scratch(operation, 'fixture', {'frozen_control_files': {
        'status.json': {'path': str(capture), 'sha256': hashlib.sha256(raw).hexdigest()}}},
        progress=observations.append)
    assert result['state'] == 'retired'
    assert [row['stage'] for row in observations] == [
        'verify-owned-preparation-scratch', 'check-owned-scratch-process-references',
        'unlink-owned-preparation-scratch', 'owned-preparation-scratch-retired']
    assert observations[-1]['completed_items'] == observations[-1]['total_items'] == 1
    assert all(row['acceptance'] is False and row['source_files_deleted'] == 0
               and row['cold_objects_deleted'] == 0 and row['nas_files_deleted'] == 0
               for row in observations)
    assert not capture.exists()


@pytest.mark.parametrize('payload', [b'{"partial":', b'{"complete":true}', b'not JSON\n'])
def test_public_catalog_version_capture_never_silently_truncates_invalid_rows(tmp_path, payload):
    source = tmp_path / 'data_tw_public'
    path = source / 'metadata/catalog_versions.jsonl'
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    with pytest.raises((SnapshotError, ValueError)):
        migration._freeze_controls(source, tmp_path / 'private/captures', [])
    assert path.read_bytes() == payload


def test_unrelated_metadata_directory_is_not_a_public_catalog_control(tmp_path):
    source = tmp_path / 'unrelated'
    (source / 'metadata').mkdir(parents=True)
    (source / 'metadata/original.json').write_bytes(b'{"original":-7}')
    (source / 'metadata/original_versions.jsonl').write_bytes(b'{"original":-7}\n')
    assert not migration._freeze_controls(source, tmp_path / 'private/captures', [])


def test_public_catalog_updates_do_not_invalidate_complete_cold_roundtrip(setup, monkeypatch):
    from stockagent.data_sync import packed_snapshots as packed
    repo, cold, entries, args, policy = setup
    source = repo / 'data_tw_public'
    Path(entries[1]['source']).rename(source)
    (source / 'metadata').mkdir()
    originals = {'metadata/mof_customs_trade.json': b'{"publishedDate":"2026-10-16"}',
                 'metadata/catalog_versions.jsonl': b'{"value":-7,"missing":null}\n'}
    for name, raw in originals.items():
        (source / name).write_bytes(raw)
    original = packed._write_pack
    def updated(root, selected, temporary, **options):
        for name in originals:
            (source / name).write_bytes(b'{"newer":true}\n')
        return original(root, selected, temporary, **options)
    monkeypatch.setattr(packed, '_write_pack', updated)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['state'] == 'verified', result
    private = migration.private_history_root(cold, policy)
    restored = fetch_packed_snapshot(private, repo / 'restored', resolve_latest_packed(private, 'private'))
    assert all((restored / name).read_bytes() == raw for name, raw in originals.items())
    assert all((source / name).read_bytes() == b'{"newer":true}\n' for name in originals)


@pytest.mark.parametrize('tamper', [False, True])
def test_large_control_evidence_stays_out_of_progress_but_is_verified_before_retirement(setup, monkeypatch, tamper):
    repo, _, entries, args, _ = setup
    source = Path(entries[1]['source'])
    (source / 'status.json').write_bytes(b'{"value":-7}')
    monkeypatch.setattr(migration, 'FROZEN_CONTROLS_INLINE_LIMIT', 0)
    from stockagent.data_sync import materialized_cache
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [])
    original = migration._recover
    def recovered(*call_args, **options):
        result = original(*call_args, **options)
        if tamper:
            path = args.migration_state / 'preparations/private.json'
            path.write_bytes(path.read_bytes() + b'\n')
        return result
    monkeypatch.setattr(migration, '_recover', recovered)
    args.retire_staging = True
    result = migration.migrate_catalog(repo, args, [entries[1]])
    row = result['datasets']['private']
    assert 'frozen_control_files' not in row['publication']
    assert row['publication']['frozen_controls_receipt']['files'] == 1
    assert 'files' not in row.get('owned_scratch_retirement', {})
    captured = json.loads((args.migration_state / 'preparations/private.json').read_bytes())['controls']['status.json']
    assert Path(captured['path']).exists() is tamper
    assert row['state'] == ('waiting_retry' if tamper else 'verified')
    if tamper:
        assert 'exact preparation' in row['error']
    assert (source / 'status.json').read_bytes() == b'{"value":-7}'


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
    ('data_finlab', 'normalized/' + 'a' * 24 + '.parquet'),
    ('data_finlab', 'normalized/builds/' + 'b' * 24 + '/part-2026.parquet'),
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


def test_v16_finlab_overlay_upgrade_keeps_native_state_and_observations_without_raw_census(setup, monkeypatch):
    repo, cold, entries, args, policy = setup
    source = repo / 'data_finlab'
    source.mkdir()
    (source / 'status.json').write_text('{"original":true}')
    with sqlite3.connect(source / 'source.db') as db:
        db.execute('CREATE TABLE source (raw INTEGER)')
        db.execute('INSERT INTO source VALUES (-7)')
    entry = {**entries[1], 'source': str(source)}
    private = migration.private_history_root(cold, policy)
    prepared = migration._private_preparation(entry, args, source, private, policy)
    states = json.loads(json.dumps(prepared['sqlite_states']))
    old = 'recursive_mutable_controls_v15'
    prepared['control_scope'] = old
    migration._preparation_path(args, entry['dataset']).write_text(json.dumps(prepared))
    stage = Path(prepared['controls_staging'])
    intent_path = stage.with_suffix('.intent.json')
    intent = json.loads(intent_path.read_text())
    intent['contract'] = old
    intent_path.write_text(json.dumps(intent))
    names = ['normalized/' + 'a' * 24 + '.parquet',
             'normalized/builds/' + 'b' * 24 + '/part-2026.parquet']
    for name in [*names, 'normalized/unknown.parquet',
                 'normalized/builds/not-a-token/part-2026.parquet']:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'original closed bytes,-7,NULL\n')
    (source / 'status.json').write_text('{"newer":true}')
    monkeypatch.setattr(migration.os, 'walk', lambda *_a, **_k: pytest.fail('raw census repeated'))
    monkeypatch.setattr(migration, '_snapshot_sqlite', lambda *_a, **_k: pytest.fail('native DB recopied'))
    upgraded = migration._private_preparation(entry, args, source, private, policy)
    assert upgraded['sqlite_states'] == states
    assert upgraded['control_scope'] == migration.CONTROL_SCOPE
    assert set(upgraded['controls']) == {'status.json', *names}
    assert Path(upgraded['controls']['status.json']['path']).read_text() == '{"original":true}'
    for name in names:
        (source / name).write_bytes(b'newer live bytes')
    assert migration._private_preparation(entry, args, source, private, policy) == upgraded
    for name in names:
        assert Path(upgraded['controls'][name]['path']).read_bytes() == b'original closed bytes,-7,NULL\n'
    assert migration._mutable_published_locations(repo / 'other_finlab') == {}


@pytest.mark.parametrize('logical', ['normalized/unknown.parquet',
    'raw/' + 'a' * 24 + '.parquet', 'normalized/builds/not-a-token/part-2026.parquet'])
def test_unknown_finlab_payloads_do_not_gain_the_overlay_capture_exception(setup, monkeypatch, logical):
    from stockagent.data_sync import packed_snapshots as packed
    repo, _, entries, args, _ = setup
    source = repo / 'data_finlab'
    payload = source / logical
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b'original raw observation')
    native = packed._write_pack
    def mutate(root, selected, target, **options):
        replacement = payload.with_name('.replacement')
        replacement.write_bytes(b'new raw observation')
        replacement.replace(payload)
        return native(root, selected, target, **options)
    monkeypatch.setattr(packed, '_write_pack', mutate)
    result = migration.migrate_catalog(repo, args, [{**entries[1], 'source': str(source)}])
    assert result['datasets']['private']['state'] == 'waiting_retry'
    assert 'changed while packing' in result['datasets']['private']['error']


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


def test_operational_controls_benchmark_measures_complete_retry_and_equal_originals():
    from scripts.benchmark_sealed_pack_recovery import mutable_trial
    reference = mutable_trial(32, False, operational_controls=True)
    candidate = mutable_trial(32, True, operational_controls=True)
    assert reference['attempts'] == 2 and candidate['attempts'] == 1
    assert reference['inventory_sha256'] == candidate['inventory_sha256']
    for row in (reference, candidate):
        assert row['full_original_readback'] and row['owned_scratch_retired']
        assert row['original_control_files'] == 3 and row['operational_controls_workload']
        assert row['production_source_payloads_deleted'] == 0


def test_announcement_controls_benchmark_avoids_whole_retry_without_skipping_original_recovery():
    from scripts.benchmark_sealed_pack_recovery import mutable_trial
    reference = mutable_trial(32, False, announcement_controls=True)
    candidate = mutable_trial(32, True, announcement_controls=True)
    assert reference['attempts'] == 2 and candidate['attempts'] == 1
    assert reference['inventory_sha256'] == candidate['inventory_sha256']
    assert reference['source_census_calls'] > candidate['source_census_calls']
    assert reference['source_census_files'] > candidate['source_census_files']
    for row in (reference, candidate):
        assert row['full_original_readback'] and row['owned_scratch_retired']
        assert row['original_control_files'] == 2 and row['announcement_controls_workload']
        assert not row['operational_controls_workload']
        assert row['production_source_payloads_deleted'] == 0


def test_inventory_benchmark_changes_only_inventory_io_and_keeps_complete_lifecycle():
    from scripts.benchmark_sealed_pack_recovery import inventory_trial
    reference = inventory_trial(32, False)
    candidate = inventory_trial(32, True)
    assert reference['inventory_sha256'] == candidate['inventory_sha256']
    for row in (reference, candidate):
        assert row['sealed_pack_recovery_enabled_in_both_variants'] is True
        assert row['retry_full_source_hash_and_pack_recovery'] is True
        assert row['deliberate_crash_exit'] == 88
        assert row['first_files_independently_reconstructed'] == 32
        assert row['second_files_independently_reconstructed'] == 32
        assert row['old_version_all_original_readback'] is True
        assert len(row['inventory_writes']) == 2
        assert row['production_source_payloads_deleted'] == 0


def test_object_path_index_benchmark_preserves_complete_spilled_lifecycle():
    from scripts.benchmark_sealed_pack_recovery import object_path_trial
    reference = object_path_trial(32, False)
    candidate = object_path_trial(32, True)
    assert reference['inventory_sha256'] == candidate['inventory_sha256']
    assert reference['object_path_rows'] == candidate['object_path_rows']
    for row in (reference, candidate):
        assert row['deliberate_crash_exit'] == 88
        assert row['first_files_independently_reconstructed'] == 32
        assert row['second_files_independently_reconstructed'] == 32
        assert row['old_version_all_original_readback']
        assert row['retry_full_source_hash_and_pack_recovery']
        assert row['object_path_index_workload'] and row['object_path_lookups']
        assert all(lookup['spilled'] for lookup in row['object_path_lookups'])
        assert row['metadata_memory_rows'] == 8
        assert row['production_source_payloads_deleted'] == 0


def test_metadata_update_benchmark_keeps_full_spilled_crash_and_old_version_recovery():
    from scripts.benchmark_sealed_pack_recovery import metadata_update_trial
    reference = metadata_update_trial(32, False)
    candidate = metadata_update_trial(32, True)
    assert reference['inventory_sha256'] == candidate['inventory_sha256']
    assert reference['metadata_update_rows'] == candidate['metadata_update_rows']
    for row in (reference, candidate):
        assert row['deliberate_crash_exit'] == 88
        assert row['first_files_independently_reconstructed'] == 32
        assert row['second_files_independently_reconstructed'] == 32
        assert row['old_version_all_original_readback']
        assert row['retry_full_source_hash_and_pack_recovery']
        assert all(group['spilled'] for group in row['metadata_update_groups'])
        assert row['production_source_payloads_deleted'] == 0


def test_pack_digest_benchmark_preserves_archive_bytes_crash_and_old_version_recovery():
    from scripts.benchmark_sealed_pack_recovery import pack_digest_trial
    reference = pack_digest_trial(32, False)
    candidate = pack_digest_trial(32, True)
    assert reference['inventory_sha256'] == candidate['inventory_sha256']
    assert reference['pack_archive_sha256'] == candidate['pack_archive_sha256']
    assert reference['producer_pack_rehash_calls'] == len(reference['pack_writes']) > 0
    assert reference['producer_pack_rehash_bytes'] > 0
    assert candidate['producer_pack_rehash_calls'] == candidate['producer_pack_rehash_bytes'] == 0
    for row in (reference, candidate):
        assert row['same_digest_mode_in_crashed_child']
        assert row['deliberate_crash_exit'] == 88
        assert row['first_files_independently_reconstructed'] == 32
        assert row['second_files_independently_reconstructed'] == 32
        assert row['old_version_all_original_readback']
        assert row['retry_full_source_hash_and_pack_recovery']
        assert row['sealed_pack_recovery_enabled_in_both_variants']
        assert row['production_source_payloads_deleted'] == 0


@pytest.mark.parametrize('provider,root_name,logical,payload', [
    ('finlab', 'data_finlab', 'catalog/discovery.json', b'{"keys":[],"missing":null}'),
    ('openbb', 'data_openBB', '_state/monitor_history.jsonl', b'{"value":-7,"missing":null}\n'),
])
def test_live_operational_readback_verifies_only_isolated_copies(
        tmp_path, monkeypatch, provider, root_name, logical, payload):
    from scripts import benchmark_sealed_pack_recovery as benchmark
    root = tmp_path / root_name
    path = root / logical
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    # The canonical large catalog is explicitly excluded, never admitted just
    # because it shares a known operational directory.
    if provider == 'openbb':
        (root / 'openbb_l1.duckdb').write_bytes(b'not selected')
    monkeypatch.setattr(benchmark, 'ROOT', tmp_path)
    result = benchmark.live_operational_trial(provider)
    assert result['state'] == 'accepted_live_operational_observation_readback'
    assert result['files'] == 1 and result['logical_bytes'] == len(payload)
    assert result['full_original_readback'] and result['owned_capture_copies_retired']
    assert result['production_source_writes'] == result['production_cold_writes'] == 0
    assert result['provider_calls'] == 0 and not result['production_migration_claim']
    assert path.read_bytes() == payload


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


def test_retry_fairness_uses_waiting_time_not_lifetime_failure_count(setup, monkeypatch):
    repo, _, entries, args, _ = setup
    observed = []
    def fail_public(entry, *_a):
        observed.append(entry['dataset'])
        raise SnapshotError('busy public source')
    def fail_private(_repo, entry, *_a):
        observed.append(entry['dataset'])
        raise SnapshotError('busy private source')
    monkeypatch.setattr(publisher, '_publish_entry', fail_public)
    monkeypatch.setattr(migration, '_private_publish', fail_private)
    migration.migrate_catalog(repo, args, entries)
    path = args.migration_state / 'progress.json'
    document = json.loads(path.read_bytes())
    document['datasets']['public'].update(attempts=129, retry_at_unix=0,
        started_at_utc='2026-10-17T00:00:00+00:00')
    document['datasets']['private'].update(attempts=1, retry_at_unix=0,
        started_at_utc='2026-10-22T00:00:00+00:00')
    path.write_text(json.dumps(document))
    observed.clear()
    args.maximum_attempted_sources = 1
    first = migration.migrate_catalog(repo, args, entries)
    assert observed == ['public'] and first['datasets']['public']['attempts'] == 130
    assert first['datasets']['private']['attempts'] == 1
    # With an immediately due retry, the other older waiter must go next.
    document = json.loads(path.read_bytes())
    document['datasets']['public']['retry_at_unix'] = 0
    path.write_text(json.dumps(document))
    observed.clear()
    second = migration.migrate_catalog(repo, args, entries)
    assert observed == ['private'] and second['datasets']['private']['attempts'] == 2


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
    retirement = json.loads(Path(row['owned_scratch_retirement']['receipt']).read_bytes())
    assert not any(Path(name).exists() for name in retirement['files'])
    assert row['publication']['sealed_pack_hint_files']
    assert all(not Path(item['path']).exists() for item in row['publication']['sealed_pack_hint_files'])
    assert json.loads(Path(row["recovery_receipt"]).read_text())["full_original_file_reconstruction_verified"]


def test_incomplete_process_scan_keeps_owned_scratch_after_full_recovery(setup, monkeypatch):
    from stockagent.data_sync import materialized_cache
    repo, _, entries, args, _ = setup
    args.retire_staging = True
    source = Path(entries[1]['source'])
    (source / 'status.json').write_bytes(b'{"value":-7,"missing":null}')
    proof_path = args.migration_state / 'recovery/private.json'

    def incomplete(*_args, **_options):
        if proof_path.exists():
            raise SnapshotError('incomplete process-reference scan: injected permission error')
        return []

    monkeypatch.setattr(materialized_cache, 'process_references_many', incomplete)
    result = migration.migrate_catalog(repo, args, [entries[1]])
    row = result['datasets']['private']
    assert row['state'] == 'waiting_retry', row
    assert 'incomplete process-reference scan' in row['error']
    assert json.loads(proof_path.read_bytes())[
        'full_original_file_reconstruction_verified']
    assert row['publication']['sealed_pack_hint_files']
    assert all(Path(item['path']).is_file()
               for item in row['publication']['sealed_pack_hint_files'])
    assert (source / 'raw.txt').read_text() == 'private'
    assert (source / 'status.json').read_bytes() == b'{"value":-7,"missing":null}'
    assert not (args.migration_state / 'scratch-retirement/private.json').exists()
    fixed_release = row['snapshot_id'], row['manifest_sha256']
    original_proof = proof_path.read_bytes()
    retry_clock = row['retry_at_unix'] + 1
    monkeypatch.setattr(migration.time, 'time', lambda: retry_clock)
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [])
    resumed = migration.migrate_catalog(repo, args, [entries[1]])['datasets']['private']
    assert resumed['state'] == 'verified', resumed
    assert (resumed['snapshot_id'], resumed['manifest_sha256']) == fixed_release
    assert proof_path.read_bytes() == original_proof
    assert resumed['owned_scratch_retirement']['state'] == 'retired'
    assert (source / 'raw.txt').read_text() == 'private'
    assert (source / 'status.json').read_bytes() == b'{"value":-7,"missing":null}'


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


@pytest.mark.parametrize('outcome', ['failed', 'source_owner_busy', 'verified'])
def test_attempt_budget_yields_at_failed_busy_and_completed_source_boundaries(setup, monkeypatch, outcome):
    repo, _, entries, args, _ = setup
    args.maximum_attempted_sources = 1
    original = publisher._publish_entry
    def first_attempt(*call_args):
        if outcome == 'failed':
            raise SnapshotError('source remains changing')
        if outcome == 'source_owner_busy':
            return {'publish_ready': False}, None
        return original(*call_args)
    monkeypatch.setattr(publisher, '_publish_entry', first_attempt)
    first = migration.migrate_catalog(repo, args, entries)
    assert first['datasets']['public']['attempts'] == 1
    assert first['datasets']['private']['attempts'] == 0
    assert first['datasets']['private']['state'] == 'pending'
    monkeypatch.setattr(publisher, '_publish_entry', original)
    second = migration.migrate_catalog(repo, args, entries)
    assert second['datasets']['private']['state'] == 'verified'
    assert second['datasets']['public']['attempts'] == 1
    assert second['plan_sha256'] == first['plan_sha256']


@pytest.mark.parametrize('budget', [0, -1, True, 1.5])
def test_attempt_budget_rejects_invalid_scope_before_writing(setup, budget):
    repo, _, entries, args, _ = setup
    args.maximum_attempted_sources = budget
    with pytest.raises(SnapshotError, match='attempt budget'):
        migration.migrate_catalog(repo, args, entries)
    assert not args.migration_state.exists()


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
    assert first['next_poll_seconds'] == 0
    second = migration.advance_catalog_versions(repo, configuration, now=200)
    assert second["state"] == "verified"
    monkeypatch.setattr(migration, "migrate_catalog", lambda *a: pytest.fail("rehashed every poll"))
    sleeping = migration.advance_catalog_versions(repo, configuration, now=300)
    assert sleeping["state"] == "waiting_next_daily_cycle"
    assert sleeping["next_cycle_unix"] == 200 + 86400


def test_lock_mode_change_finishes_original_plan_before_new_catalog_cycle(setup, monkeypatch):
    repo, cold, entries, args, _ = setup
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    first = migration.advance_catalog_versions(repo, configuration, now=100)
    current_path = Path(configuration['state_root']) / 'dataset-versions/current.json'
    original = json.loads(current_path.read_bytes())
    progress_path = Path(original['operation_directory']) / 'progress.json'
    public = json.loads(progress_path.read_bytes())['datasets']['public']
    binding = {key: public[key] for key in ('snapshot_id', 'manifest_sha256', 'recovery_receipt', 'attempts')}
    entries[0] = {**entries[0], 'source_coordination_lock_mode': 'exclusive'}
    second = migration.advance_catalog_versions(repo, configuration, now=200)
    resumed = json.loads(current_path.read_bytes())
    assert second['state'] == 'verified' and second['verified_datasets'] == 2
    assert resumed['plan_sha256'] == first['plan_sha256']
    assert resumed['operation_directory'] == original['operation_directory']
    assert resumed['deferred_catalog_plan_sha256'] != first['plan_sha256']
    public_after = json.loads(progress_path.read_bytes())['datasets']['public']
    assert {key: public_after[key] for key in binding} == binding
    # Changing only reader coordination is not permission to repeat a completed
    # all-source cycle immediately. Preserve the original completion/deadline.
    third = migration.advance_catalog_versions(repo, configuration, now=300)
    assert third['state'] == 'waiting_next_daily_cycle'
    assert third['verified_datasets'] == 2
    assert third['next_cycle_unix'] == resumed['next_cycle_unix']
    assert third['deferred_catalog_plan_sha256'] == resumed['deferred_catalog_plan_sha256']
    assert json.loads(current_path.read_bytes())['operation_directory'] == original['operation_directory']
    # The new exact plan is adopted at the original daily boundary, not hidden
    # forever and not stamped onto the previously accepted manifests.
    fourth = migration.advance_catalog_versions(repo, configuration, now=resumed['next_cycle_unix'])
    assert fourth['plan_sha256'] == resumed['deferred_catalog_plan_sha256']
    assert json.loads(current_path.read_bytes())['operation_directory'] != original['operation_directory']


def test_completed_lock_only_change_defers_without_payload_or_recovery_io(setup, monkeypatch):
    repo, cold, entries, args, policy = setup
    entries[0] = {**entries[0], 'dataset': 'tw-public',
                  'source_coordination_lock': 'test-public-source.lock'}
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    migration.advance_catalog_versions(repo, configuration, now=100)
    migration.advance_catalog_versions(repo, configuration, now=200)
    current_path = Path(configuration['state_root']) / 'dataset-versions/current.json'
    original = json.loads(current_path.read_bytes())
    progress_path = Path(original['operation_directory']) / 'progress.json'
    accepted = progress_path.read_bytes()
    entries[0] = {**entries[0], 'source_coordination_lock_mode': 'shared_read'}
    revised = migration._identity({'catalog': migration.migration_entries(repo, entries, policy),
                                   'policy': policy})
    native = migration.migrate_catalog
    monkeypatch.setattr(migration, 'migrate_catalog', lambda *a: pytest.fail('lock change reread source payload'))
    for clock in (201, 300, original['next_cycle_unix'] - 1):
        result = migration.advance_catalog_versions(repo, configuration, now=clock)
        assert result['state'] == 'waiting_next_daily_cycle'
        assert result['deferred_catalog_plan_sha256'] == revised
        saved = json.loads(current_path.read_bytes())
        assert saved['next_cycle_unix'] == original['next_cycle_unix']
        assert saved['plan_sha256'] == original['plan_sha256']
        assert progress_path.read_bytes() == accepted
    monkeypatch.setattr(migration, 'migrate_catalog', native)
    due = migration.advance_catalog_versions(repo, configuration, now=original['next_cycle_unix'])
    assert due['plan_sha256'] == revised
    assert due['verified_datasets'] == 1


@pytest.mark.parametrize('change', ['path', 'rights', 'freshness', 'lock_path', 'policy'])
def test_completed_cycle_does_not_delay_material_catalog_changes(setup, monkeypatch, change):
    repo, cold, entries, args, policy = setup
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    migration.advance_catalog_versions(repo, configuration, now=100)
    migration.advance_catalog_versions(repo, configuration, now=200)
    current_path = Path(configuration['state_root']) / 'dataset-versions/current.json'
    original = json.loads(current_path.read_bytes())
    entries[0] = {**entries[0], 'source_coordination_lock_mode': 'shared_read'}
    if change == 'path':
        entries[0]['source'] = entries[1]['source']
    elif change == 'rights':
        entries[0]['publish'] = False
    elif change == 'freshness':
        entries[0]['freshness'] = {'receipt': 'new.json'}
    elif change == 'lock_path':
        entries[0]['source_coordination_lock'] = 'different.lock'
    else:
        from stockagent.data_sync import dataset_version_layout
        monkeypatch.setattr(dataset_version_layout, 'version_policy', lambda *_:
            {**policy, 'file_chunk_bytes': policy['file_chunk_bytes'] // 2})
    calls = []
    def inspect_new_cycle(repo, run_args, catalog):
        calls.append((run_args, catalog))
        return {'state': 'processing', 'verified_datasets': 0, 'source_count': 2,
                'datasets': {entry['dataset']: {'state': 'pending'} for entry in catalog}}
    monkeypatch.setattr(migration, 'migrate_catalog', inspect_new_cycle)
    migration.advance_catalog_versions(repo, configuration, now=201)
    revised = json.loads(current_path.read_bytes())
    assert len(calls) == 1
    assert revised['plan_sha256'] != original['plan_sha256']
    assert revised['operation_directory'] != original['operation_directory']


@pytest.mark.parametrize('change', ['path', 'rights', 'freshness', 'lock_path', 'policy'])
def test_lock_only_plan_resume_never_hides_changed_scope_or_rights(setup, monkeypatch, change):
    repo, cold, entries, args, policy = setup
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    migration.advance_catalog_versions(repo, configuration, now=100)
    root = Path(configuration['state_root']) / 'dataset-versions'
    original_raw = (root / 'current.json').read_bytes()
    original = json.loads(original_raw)
    revised = [dict(entry) for entry in entries]
    revised[0]['source_coordination_lock_mode'] = 'shared_read'
    if change == 'path':
        revised[0]['source'] = entries[1]['source']
    elif change == 'rights':
        revised[0]['publish'] = False
    elif change == 'freshness':
        revised[0]['freshness'] = {'receipt': 'new.json'}
    elif change == 'lock_path':
        revised[0]['source_coordination_lock'] = 'different.lock'
    else:
        policy = {**policy, 'file_chunk_bytes': policy['file_chunk_bytes'] // 2}
    assert migration._inflight_lock_only_catalog(repo, root, original, revised, policy) is None
    assert (root / 'current.json').read_bytes() == original_raw


@pytest.mark.parametrize('damage', ['identity', 'redirect'])
def test_lock_only_plan_resume_refuses_damaged_saved_evidence(setup, monkeypatch, damage):
    repo, cold, entries, args, policy = setup
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    migration.advance_catalog_versions(repo, configuration, now=100)
    root = Path(configuration['state_root']) / 'dataset-versions'
    original = json.loads((root / 'current.json').read_bytes())
    path = Path(original['operation_directory']) / 'plan.json'
    if damage == 'identity':
        value = json.loads(path.read_bytes())
        value['catalog'][0]['publish'] = False
        path.write_text(json.dumps(value))
    else:
        target = path.with_suffix('.retained.json')
        path.rename(target)
        path.symlink_to(target)
    with pytest.raises(SnapshotError, match='plan.*(identity|redirected)'):
        migration._inflight_lock_only_catalog(repo, root, original, entries, policy)


def test_lifecycle_schedules_ready_sources_without_delay_but_respects_each_busy_deadline(setup, monkeypatch):
    repo, cold, entries, args, _ = setup
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    monkeypatch.setattr(publisher, '_publish_entry', lambda *_: ({'publish_ready': False}, None))
    monkeypatch.setattr(migration, '_private_publish', lambda *_: (None, {'state': 'waiting_source_owner'}))
    monkeypatch.setattr(migration.time, 'time', lambda: 100)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    assert migration.advance_catalog_versions(repo, configuration)['next_poll_seconds'] == 0
    second = migration.advance_catalog_versions(repo, configuration)
    assert second['next_poll_seconds'] == 60
    current = json.loads((Path(configuration['state_root']) / 'dataset-versions/current.json').read_bytes())
    progress_path = Path(current['operation_directory']) / 'progress.json'
    before = json.loads(progress_path.read_bytes())
    assert all(row['attempts'] == 1 and row['retry_at_unix'] == 160 for row in before['datasets'].values())
    monkeypatch.setattr(publisher, '_publish_entry', lambda *_: pytest.fail('busy deadline was ignored'))
    monkeypatch.setattr(migration, '_private_publish', lambda *_: pytest.fail('busy deadline was ignored'))
    assert migration.advance_catalog_versions(repo, configuration)['next_poll_seconds'] == 60
    assert all(row['attempts'] == 1 for row in json.loads(progress_path.read_bytes())['datasets'].values())


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
