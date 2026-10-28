import copy
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import sqlite3
import subprocess
import sys
from threading import Event

import pytest

from stockagent.live import data_monitor_inventory as inventory
from stockagent.live import data_monitor_record_store as store


@pytest.fixture
def path(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "MIN_INCREMENTAL_FILES", 0)
    return tmp_path / "record_inventory_cache.json"


def seed(path):
    payload = {"version": 10, "schemas": {}, "datasets": {}, "feature_revision": "a" * 32,
               "files": {"a": {"stats": [1, None]}, "b": {"error": "invalid_parquet"}}}
    path.write_bytes(json.dumps(payload).encode())
    return payload


def migrate(path, payload, *, partitioned_metadata=False, factored_stats=False, interned_schemas=False):
    return store.write_record_delta(path, payload, payload,
        expected_signature=inventory._cache_signature(path), enable=True,
        partitioned_metadata=partitioned_metadata, factored_stats=factored_stats, interned_schemas=interned_schemas)


def test_legacy_and_versioned_store_keep_exact_values(path):
    payload = seed(path)
    payload["files"]["a"]["legacy"] = [2**100, -0.0, 5e-324, "台灣", None, False]
    path.write_bytes(json.dumps(payload).encode())
    assert store.read_record_cache(path) == payload
    observed = migrate(path, payload)
    assert observed["store_rebuilt"] and observed["rows_written"] == 2
    assert json.loads(path.read_bytes())["record_cache_storage"] == 1
    actual = store.read_record_cache(path)
    assert actual == payload
    assert math.copysign(1., actual["files"]["a"]["legacy"][1]) == -1.
    assert actual["files"]["a"]["legacy"][2] == 5e-324
    assert inventory._read_json(path) == payload
    assert not store.cache_file(path).with_name(store.cache_file(path).name + "-wal").exists()


def test_shared_new_stats_mutations_are_preserved_between_transactions(path):
    payload = seed(path)
    shared = _footer()
    payload["files"] = {str(i): {"size": i, "stats": shared} for i in range(12)}
    migrate(path, payload, factored_stats=True)
    assert store.read_record_cache(path) == payload
    previous = store.read_record_cache(path, share_stats=True)
    shared = _footer()
    shared["count"] += 1
    current = {**previous, "files": {key: {**value, "stats": shared}
                                     for key, value in previous["files"].items()}}
    store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert store.read_record_cache(path) == current
    shared["count"] += 1
    updated = {**current, "files": {key: dict(value) for key, value in current["files"].items()}}
    store.write_record_delta(path, updated, current, expected_signature=inventory._cache_signature(path))
    assert store.read_record_cache(path) == updated


def test_shared_stats_are_encoded_once_per_publication_and_not_across_mutations(path, monkeypatch):
    payload = seed(path)
    shared = _footer()
    payload["files"] = {str(i): {"size": i, "stats": shared} for i in range(12)}
    calls = []
    encode = store.encode_footer_stats
    def counted(value):
        calls.append(value)
        return encode(value)
    monkeypatch.setattr(store, "encode_footer_stats", counted)
    migrate(path, payload, factored_stats=True)
    assert len(calls) == 1
    previous = store.read_record_cache(path, share_stats=True)
    current = {**previous, "files": {key: {**value, "size": value["size"] + 1}
                                     for key, value in previous["files"].items()}}
    calls.clear()
    store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert len(calls) == 1
    assert store.read_record_cache(path) == current
    current["files"]["0"]["stats"]["non_null"][0] = 999
    updated = {**current, "files": {key: dict(value) for key, value in current["files"].items()}}
    calls.clear()
    store.write_record_delta(path, updated, current, expected_signature=inventory._cache_signature(path))
    assert len(calls) == 1
    assert store.read_record_cache(path) == updated


def test_equal_distinct_stats_keep_independent_type_proofs(path, monkeypatch):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}, "b": {"stats": _footer()}}
    calls = []
    encode = store.encode_footer_stats
    def counted(value):
        calls.append(value)
        return encode(value)
    monkeypatch.setattr(store, "encode_footer_stats", counted)
    migrate(path, payload, factored_stats=True)
    assert len(calls) == 2
    assert store.read_record_cache(path) == payload


def test_delta_updates_additions_deletions_and_metadata_in_one_generation(path):
    original = seed(path)
    migrate(path, original)
    before = store.read_record_cache(path)
    current = {**before, "files": dict(before["files"]), "feature_revision": "b" * 32}
    current["files"]["a"] = {"stats": [2, None]}
    del current["files"]["b"]
    current["files"]["c"] = {"stats": [3]}
    target, old_signature, descriptor = store.cache_file(path), inventory._cache_signature(path), path.read_bytes()
    observed = store.write_record_delta(path, current, before, expected_signature=old_signature)
    assert observed["rows_written"] == 2 and observed["rows_deleted"] == 1
    assert not observed["store_rebuilt"]
    assert store.read_record_cache(path) == current
    assert inventory._cache_signature(path) != old_signature
    assert path.read_bytes() == descriptor and store.cache_file(path) == target


def test_metadata_only_and_signed_zero_type_changes_are_not_lost(path):
    original = seed(path)
    original["files"]["a"] = {"zero": -0.0, "kind": False}
    migrate(path, original)
    previous = store.read_record_cache(path)
    current = {**previous, "feature_revision": "b" * 32}
    observed = store.write_record_delta(path, current, previous,
        expected_signature=inventory._cache_signature(path))
    assert observed["rows_written"] == 0
    previous = store.read_record_cache(path)
    current = {**previous, "files": dict(previous["files"])}
    current["files"]["a"] = {"zero": 0.0, "kind": 0}
    observed = store.write_record_delta(path, current, previous,
        expected_signature=inventory._cache_signature(path))
    actual = store.read_record_cache(path)["files"]["a"]
    assert observed["rows_written"] == 1
    assert math.copysign(1., actual["zero"]) == 1.
    assert type(actual["kind"]) is int


@pytest.mark.parametrize("storage_version", [1, 2, 3])
def test_failed_multirow_update_rolls_back_entire_payload(path, storage_version):
    original = seed(path)
    migrate(path, original, partitioned_metadata=storage_version >= 2, factored_stats=storage_version == 3)
    with sqlite3.connect(store.cache_file(path)) as connection:
        connection.execute("CREATE TRIGGER reject_bad BEFORE INSERT ON files WHEN NEW.path='bad' "
                           "BEGIN SELECT RAISE(ABORT,'injected failure'); END")
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"a": {"stats": [9]}, "bad": {"stats": [8]}},
               "feature_revision": "b" * 32}
    with pytest.raises(sqlite3.IntegrityError, match="injected failure"):
        store.write_record_delta(path, current, previous,
            expected_signature=inventory._cache_signature(path))
    assert store.read_record_cache(path) == original


def test_replaced_generation_cannot_receive_a_stale_delta(path):
    original = seed(path)
    migrate(path, original)
    old_signature = inventory._cache_signature(path)
    previous = store.read_record_cache(path)
    newer = copy.deepcopy(previous)
    newer["files"]["a"] = {"stats": [99]}
    store.write_record_delta(path, newer, previous, expected_signature=old_signature)
    current = {**previous, "feature_revision": "c" * 32}
    observed = store.write_record_delta(path, current, previous, expected_signature=old_signature)
    assert observed["store_rebuilt"]
    assert store.read_record_cache(path) == current


@pytest.mark.parametrize("storage_version", [1, 2, 3])
def test_reader_transaction_never_mixes_generations_during_commit(path, monkeypatch, storage_version):
    payload = seed(path)
    migrate(path, payload, partitioned_metadata=storage_version >= 2, factored_stats=storage_version == 3)
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"a": {"stats": [99]}}, "feature_revision": "b" * 32}
    signature = inventory._cache_signature(path)
    connect = sqlite3.connect
    committing = Event()
    def traced(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(lambda sql: committing.set() if sql == "COMMIT" else None)
        return connection
    reader = connect(store.cache_file(path))
    try:
        reader.execute("BEGIN")
        assert json.loads(reader.execute("SELECT body FROM metadata WHERE key='payload'").fetchone()[0])["feature_revision"] == "a" * 32
        monkeypatch.setattr(store.sqlite3, "connect", traced)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(store.write_record_delta, path, current, previous, expected_signature=signature)
            try:
                assert committing.wait(5.)
                assert not future.done()
                assert json.loads(reader.execute("SELECT body FROM files WHERE path='a'").fetchone()[0]) == payload["files"]["a"]
            finally:
                reader.rollback()
            assert future.result(timeout=5.)["rows_written"] == 1
        assert store.read_record_cache(path) == current
    finally:
        reader.close()


def test_offline_export_fences_database_even_when_descriptor_is_unchanged(path, monkeypatch):
    payload = seed(path)
    migrate(path, payload)
    descriptor = path.read_bytes()
    read = store.read_record_cache
    def advance(candidate):
        previous = read(candidate)
        current = {**previous, "feature_revision": "b" * 32}
        store.write_record_delta(candidate, current, previous,
            expected_signature=inventory._cache_signature(candidate))
        return previous
    monkeypatch.setattr(store, "read_record_cache", advance)
    with pytest.raises(ValueError, match="advanced"):
        store.stable_record_cache_bytes(path)
    assert path.read_bytes() == descriptor


def test_nonfinite_legacy_values_are_never_converted_to_null(path):
    payload = seed(path)
    payload["files"]["a"] = {"legacy": [float("inf"), float("-inf"), float("nan")]}
    migrate(path, payload)
    values = store.read_record_cache(path)["files"]["a"]["legacy"]
    assert values[:2] == [float("inf"), float("-inf")]
    assert math.isnan(values[2])


@pytest.mark.parametrize("database", ["../other.sqlite3", "/tmp/other.sqlite3", "invalid.sqlite3"])
def test_descriptor_cannot_redirect_outside_owned_namespace(path, database):
    path.write_text(json.dumps({"record_cache_storage": 1, "database": database}, separators=(",", ":")))
    with pytest.raises(ValueError, match="descriptor"):
        store.read_record_cache(path)
    assert inventory._read_json(path) is None and inventory._cache_signature(path) is None


def test_missing_symlinked_and_wrong_version_databases_are_rejected(path):
    payload = seed(path)
    migrate(path, payload)
    target = store.cache_file(path)
    with sqlite3.connect(target) as connection:
        connection.execute("PRAGMA user_version=99")
    assert inventory._read_json(path) is None
    held = target.with_suffix(".held")
    target.rename(held)
    assert inventory._read_json(path) is None
    target.symlink_to(held)
    assert inventory._read_json(path) is None and inventory._cache_signature(path) is None


@pytest.mark.parametrize("storage_version", [1, 2, 3])
def test_uncommitted_process_exit_recovers_without_partial_publication(path, storage_version):
    payload = seed(path)
    migrate(path, payload, partitioned_metadata=storage_version >= 2, factored_stats=storage_version == 3)
    target = store.cache_file(path)
    command = """import os,sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE')
c.execute(\"UPDATE files SET body=? WHERE path='a'\", (b'{\"stats\":[999]}',))
c.execute(\"UPDATE metadata SET body=? WHERE key='payload'\", (b'{\"version\":99}',))
os._exit(77)
"""
    result = subprocess.run([sys.executable, "-c", command, str(target)], check=False)
    assert result.returncode == 77
    # The canonical writer's next RW open performs SQLite hot-journal recovery.
    with sqlite3.connect(target) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.rollback()
    assert store.read_record_cache(path) == payload


def test_inventory_migration_change_and_feature_reader_keep_complete_semantics(path):
    from datetime import date
    import polars as pl
    root = path.parent
    source = root / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2025, 1, 1)], "close": [1.]}).write_parquet(source)
    frame = inventory.InventorySnapshot(root)
    first = inventory.build_record_inventory(root, refresh=True, incremental_store=True, snapshot=frame)
    assert first["cache_encoding_observation"]["store_rebuilt"]
    assert inventory.build_record_inventory(root, refresh=True)["fast_index_hit"]
    pl.DataFrame({"date": [date(2025, 1, 1), date(2025, 1, 2)], "close": [1., 2.]}).write_parquet(source)
    changed = inventory.build_record_inventory(root, refresh=True, snapshot=frame)
    assert changed["cache_encoding_observation"]["rows_written"] == 1
    assert not changed["cache_encoding_observation"]["store_rebuilt"]
    assert changed["datasets"]["yahoo:crypto"]["count"] == 2
    retained = inventory.build_feature_inventory(root, snapshot=frame)
    assert inventory.build_feature_inventory(root) == retained


def test_enable_migrates_even_when_the_old_quick_index_is_valid(path):
    from datetime import date
    import polars as pl
    root = path.parent
    source = root / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2025, 1, 1)], "close": [1.]}).write_parquet(source)
    first = inventory.build_record_inventory(root, refresh=True)
    migrated = inventory.build_record_inventory(root, refresh=True, incremental_store=True)
    assert migrated["cache_encoding_observation"]["store_rebuilt"]
    assert migrated["feature_revision"] == first["feature_revision"]
    assert migrated["source_observation_root"] == first["source_observation_root"]
    assert migrated["datasets"] == first["datasets"]
    assert inventory.build_record_inventory(root, refresh=True, incremental_store=True)["fast_index_hit"]


def test_existing_projection_capture_exports_a_complete_transactional_generation(path):
    from datetime import date
    import polars as pl
    from scripts.benchmark_data_monitor_projection_pipeline import capture
    root = path.parent
    source = root / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2025, 1, 1)], "close": [1.]}).write_parquet(source)
    inventory.build_record_inventory(root, refresh=True, incremental_store=True)
    status = root / "artifacts/live/data_monitor/public_status.json"
    status.write_text(json.dumps({"sources": []}))
    generation = capture(root)
    assert generation["payload"]["version"] == 10
    assert generation["payload"]["files"][str(source)]["stats"]["count"] == 1
    assert generation["signature"] == inventory._cache_signature(status.with_name("record_inventory_cache.json"))


def test_metadata_v2_upgrade_is_explicit_and_preserves_legacy_values(path):
    payload = seed(path)
    payload["schemas"] = {"quoted\"台灣": [["date", "date32"], ["a", "double"]]}
    payload["datasets"] = {"values": [2**100, -0.0, 5e-324, float("inf"), float("nan")]}
    migrate(path, payload)
    old_database = store.cache_file(path)
    previous = store.read_record_cache(path)
    upgraded = store.write_record_delta(path, previous, previous,
        expected_signature=inventory._cache_signature(path), partitioned_metadata=True)
    assert upgraded["store_rebuilt"] and upgraded["backend"] == "sqlite_metadata_v2"
    assert old_database.exists() and old_database != store.cache_file(path)
    assert json.loads(path.read_bytes())["record_cache_storage"] == 2
    actual = store.read_record_cache(path)
    assert actual["schemas"] == payload["schemas"]
    values = actual["datasets"]["values"]
    assert values[:3] == payload["datasets"]["values"][:3]
    assert math.copysign(1., values[1]) == -1. and math.isinf(values[3]) and math.isnan(values[4])
    assert json.loads(store.stable_record_cache_bytes(path))["files"] == payload["files"]
    # Existing v2 continues to update coherently even through callers whose
    # optional migration flag remains off; it never downgrades implicitly.
    result = store.write_record_delta(path, actual, actual, expected_signature=inventory._cache_signature(path))
    assert not result["store_rebuilt"] and result["backend"] == "sqlite_metadata_v2"


def test_metadata_v2_only_updates_changed_sections_and_exact_value_bytes(path):
    original = seed(path)
    original.update(schemas={"wide": [["date", "date32"], ["股票", "double"]]},
                    datasets={"one": {"zero": -0.0, "kind": False}, "remove": {}},
                    dataset_memberships={"one": "a"})
    migrate(path, original, partitioned_metadata=True)
    with sqlite3.connect(store.cache_file(path)) as connection:
        connection.execute("CREATE TRIGGER immutable_schema BEFORE UPDATE ON sections "
                           "WHEN OLD.section='schemas' BEGIN SELECT RAISE(ABORT,'unchanged schema rewritten'); END")
    previous = store.read_record_cache(path)
    current = {**previous, "schemas": copy.deepcopy(previous["schemas"]),
               "datasets": {"one": {"zero": 0.0, "kind": 0}}, "feature_revision": "b" * 32}
    result = store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert result["rows_written"] == 0
    assert result["metadata_rows_written"] == 2 and result["metadata_rows_deleted"] == 1
    actual = store.read_record_cache(path)
    assert actual == current
    assert math.copysign(1., actual["datasets"]["one"]["zero"]) == 1.
    assert type(actual["datasets"]["one"]["kind"]) is int
    assert store.write_record_delta(path, actual, actual,
        expected_signature=inventory._cache_signature(path))["metadata_rows_written"] == 0


def test_metadata_failure_rolls_back_files_and_root_together(path):
    original = seed(path)
    original["schemas"] = {"before": [["date", "date32"]]}
    migrate(path, original, partitioned_metadata=True)
    with sqlite3.connect(store.cache_file(path)) as connection:
        connection.execute("CREATE TRIGGER reject_schema BEFORE INSERT ON sections WHEN NEW.key='bad' "
                           "BEGIN SELECT RAISE(ABORT,'injected schema failure'); END")
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"a": {"stats": [999]}}, "schemas": {"bad": []},
               "feature_revision": "b" * 32}
    with pytest.raises(sqlite3.IntegrityError, match="schema failure"):
        store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert store.read_record_cache(path) == original


def test_metadata_v2_upgrades_a_valid_quick_index_without_changing_proofs(path):
    from datetime import date
    import polars as pl
    root = path.parent
    source = root / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2025, 1, 1)], "close": [1.]}).write_parquet(source)
    first = inventory.build_record_inventory(root, refresh=True, incremental_store=True)
    assert inventory.build_record_inventory(root, refresh=True)["fast_index_hit"]
    upgraded = inventory.build_record_inventory(root, refresh=True, incremental_store=True, incremental_metadata=True)
    assert upgraded["cache_encoding_observation"]["backend"] == "sqlite_metadata_v2"
    assert upgraded["cache_encoding_observation"]["store_rebuilt"]
    for key in ("feature_revision", "source_observation_root", "datasets"):
        assert upgraded[key] == first[key]
    assert inventory.build_record_inventory(root, refresh=True, incremental_store=True,
                                            incremental_metadata=True)["fast_index_hit"]


def _footer(count=7, **changes):
    return {"count": count, "first": "2026-01-02", "last": "2026-01-05",
            "time_column": "date", "schema_id": "one", "non_null": [count, None, 0], **changes}


def test_readonly_footer_header_publication_reuses_only_witnessed_blocks(path, monkeypatch):
    payload = seed(path)
    payload["files"] = {key: {"size": 10, "stats": _footer()} for key in ("a", "b")}
    migrate(path, payload, factored_stats=True)
    previous = store.read_record_cache(path, share_stats=True, immutable_stats=True)
    stats = previous["files"]["a"]["stats"]
    assert stats is previous["files"]["b"]["stats"]
    assert previous == payload
    assert json.loads(json.dumps(previous)) == payload
    for mutate in (
        lambda: stats.update(count=8), lambda: stats.setdefault("new", 1),
        lambda: stats.pop("count"), lambda: stats.clear(),
        lambda: stats.__setitem__("count", 8), lambda: stats.__ior__({"count": 8}),
        lambda: setattr(stats, "_encoded_block", ("wrong", b"wrong", 1)),
        lambda: stats["non_null"].append(1), lambda: stats["non_null"].clear(),
        lambda: stats["non_null"].__setitem__(0, 999),
        lambda: stats["non_null"].__iadd__([1]), lambda: stats["non_null"].__imul__(2),
        lambda: stats["non_null"].__init__([1]),
    ):
        with pytest.raises(TypeError, match="read-only"):
            mutate()
    calls = []
    encode = store.encode_footer_stats
    def counted(value):
        calls.append(value)
        return encode(value)
    monkeypatch.setattr(store, "encode_footer_stats", counted)
    current = {**previous, "files": {key: {**entry, "size": 11}
                                     for key, entry in previous["files"].items()}}
    store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert calls == []
    assert store.read_record_cache(path) == current
    current["files"]["a"]["stats"] = _footer(count=8)
    before = store.read_record_cache(path, share_stats=True, immutable_stats=True)
    store.write_record_delta(path, current, before, expected_signature=inventory._cache_signature(path))
    assert calls == [current["files"]["a"]["stats"]]
    normal = store.read_record_cache(path)
    normal["files"]["b"]["stats"]["non_null"][0] = 999
    assert store.read_record_cache(path) == current


def test_readonly_footer_keeps_legacy_mutability_and_stale_generation_rebuild(path):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()},
                        "b": {"stats": _footer(unknown={"values": [1]})}}
    migrate(path, payload, factored_stats=True)
    previous = store.read_record_cache(path, share_stats=True, immutable_stats=True)
    assert type(previous["files"]["b"]["stats"]) is dict
    previous["files"]["b"]["stats"]["unknown"]["values"].append(2)
    result = store.write_record_delta(path, previous, previous, expected_signature=None)
    assert result["store_rebuilt"]
    assert store.read_record_cache(path) == previous


def test_readonly_footer_edges_and_generation_are_checked_before_skipping_sql(path, monkeypatch):
    payload = seed(path)
    payload["files"] = {"a": {"size": 10, "stats": _footer()},
                        "b": {"size": 20, "stats": _footer(count=8)}}
    migrate(path, payload, factored_stats=True)
    previous = store.read_record_cache(path, share_stats=True, immutable_stats=True)
    statements = []
    connect = sqlite3.connect
    def observed(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection
    monkeypatch.setattr(sqlite3, "connect", observed)
    headers = {**previous, "files": {key: {**entry, "size": 11}
                                     for key, entry in previous["files"].items()}}
    store.write_record_delta(path, headers, previous, expected_signature=inventory._cache_signature(path))
    assert not any(sql.startswith(("SELECT stats_key FROM files WHERE", "SELECT body FROM footer_stats WHERE",
                                   "DELETE FROM footer_stats")) for sql in statements)
    # An old frame may supply exact bytes, but its edges cannot waive current
    # database checks after another transaction has changed the generation.
    stale = previous
    current = {**headers, "files": {"a": {"size": 12, "stats": stale["files"]["b"]["stats"]},
                                    "b": {**headers["files"]["b"], "size": 12}}}
    statements.clear()
    result = store.write_record_delta(path, current, headers, expected_signature=inventory._cache_signature(path))
    assert any(sql.startswith("SELECT body FROM footer_stats WHERE") for sql in statements)
    assert result["stats_rows_deleted"] == 1
    assert store.read_record_cache(path) == current
    fresh = store.read_record_cache(path, share_stats=True, immutable_stats=True)
    replaced = {**fresh, "files": {"b": {**fresh["files"]["b"], "stats": _footer(count=9)}}}
    store.write_record_delta(path, replaced, fresh, expected_signature=inventory._cache_signature(path))
    latest = store.read_record_cache(path, share_stats=True, immutable_stats=True)
    reassigned = {**latest, "files": {"b": {**latest["files"]["b"], "stats": stale["files"]["a"]["stats"]}}}
    result = store.write_record_delta(path, reassigned, latest, expected_signature=inventory._cache_signature(path))
    assert result["stats_rows_written"] == result["stats_rows_deleted"] == 1
    assert store.read_record_cache(path) == reassigned


def test_factored_upgrade_only_changes_headers_when_stats_remain_equal(path):
    payload = seed(path)
    payload["files"] = {"a": {"size": 10, "stats": _footer()}, "b": {"size": 20, "stats": _footer()}}
    migrate(path, payload, partitioned_metadata=True)
    old = store.cache_file(path)
    previous = store.read_record_cache(path)
    upgraded = store.write_record_delta(path, previous, previous,
        expected_signature=inventory._cache_signature(path), factored_stats=True)
    assert upgraded["store_rebuilt"] and upgraded["stats_rows_written"] == 1
    assert old.exists() and store.cache_file(path) != old
    with sqlite3.connect(store.cache_file(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert all("stats" not in json.loads(body) for (body,) in connection.execute("SELECT body FROM files"))
        connection.execute("CREATE TRIGGER no_stats_insert BEFORE INSERT ON footer_stats "
                           "BEGIN SELECT RAISE(ABORT,'unchanged stats encoded as new block'); END")
    previous = store.read_record_cache(path)
    current = {**previous, "files": {**previous["files"], "a": {"size": 11, "stats": _footer()}}}
    observed = store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert not observed["store_rebuilt"] and observed["rows_written"] == 1
    assert observed["stats_rows_written"] == observed["stats_rows_deleted"] == 0
    assert store.read_record_cache(path) == current
    assert json.loads(path.read_bytes())["record_cache_storage"] == 3


def test_factored_public_reads_keep_independent_mutable_vectors_and_owner_can_share(path):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}, "b": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    actual = store.read_record_cache(path)
    actual["files"]["a"]["stats"]["non_null"][0] = 999
    actual["files"]["a"]["stats"]["count"] = 999
    assert actual["files"]["b"]["stats"] == _footer()
    shared = store.read_record_cache(path, share_stats=True)
    assert shared == payload
    assert shared["files"]["a"]["stats"] is shared["files"]["b"]["stats"]


def test_interned_schemas_migration_delta_and_mutable_reader_isolation(path):
    payload = seed(path)
    payload["schemas"] = {"one": [["date", "date32"], ["股票", "double"]],
                          "two": [["股票", "double"], ["date", "date32"]],
                          "legacy": [["mixed", 123], [False, -0.0]]}
    payload["files"] = {"a": {"stats": _footer()}, "b": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    old_database = store.cache_file(path)
    previous = store.read_record_cache(path)
    result = store.write_record_delta(path, previous, previous,
        expected_signature=inventory._cache_signature(path), interned_schemas=True)
    assert result["store_rebuilt"] and result["backend"] == "sqlite_interned_v4"
    assert old_database.exists() and store.cache_file(path) != old_database
    with sqlite3.connect(store.cache_file(path)) as connection:
        assert connection.execute("SELECT count(*) FROM schema_fields").fetchone()[0] == 2
        assert connection.execute("SELECT codec FROM sections WHERE key='legacy'").fetchone()[0] == 0
        connection.execute("CREATE TRIGGER no_schema_update BEFORE UPDATE ON sections "
                           "WHEN OLD.section='schemas' AND OLD.key='one' "
                           "BEGIN SELECT RAISE(ABORT,'unchanged schema rewritten'); END")
    actual = store.read_record_cache(path)
    assert actual == payload
    actual["schemas"]["one"][0][0] = "changed"
    assert actual["schemas"]["two"][1] == ["date", "date32"]
    shared = store.read_record_cache(path, share_stats=True)
    assert shared == payload
    assert shared["schemas"]["one"][0] is shared["schemas"]["two"][1]
    current = {**shared, "schemas": {**shared["schemas"], "two": [["new", "int64"]]}}
    result = store.write_record_delta(path, current, shared, expected_signature=inventory._cache_signature(path))
    assert not result["store_rebuilt"] and result["metadata_rows_written"] >= 1
    assert store.read_record_cache(path) == current


def test_compact_schema_frame_survives_rollback_and_stale_generation_rebuild(path):
    payload = seed(path)
    payload["schemas"] = {"one": [["date", "date32"], ["a", "double"]],
                          "surrogate": [["\ud800", "string"]]}
    migrate(path, payload, interned_schemas=True)
    signature = inventory._cache_signature(path)
    previous = store.read_record_cache(path, share_stats=True, compact_schemas=True)
    assert isinstance(previous["schemas"]["one"], store.SharedSchema)
    assert len(previous["schemas"]["one"]) == 2
    assert list(previous["schemas"]["one"]) == payload["schemas"]["one"]
    with sqlite3.connect(store.cache_file(path)) as connection:
        connection.execute("CREATE TRIGGER reject_new_schema BEFORE INSERT ON sections "
                           "WHEN NEW.key='bad' BEGIN SELECT RAISE(ABORT,'injected v4 failure'); END")
    signature = inventory._cache_signature(path)
    current = {**previous, "schemas": {**previous["schemas"], "bad": [["new", "int64"]]}}
    with pytest.raises(sqlite3.IntegrityError, match="v4 failure"):
        store.write_record_delta(path, current, previous, expected_signature=signature)
    assert store.read_record_cache(path) == payload
    with sqlite3.connect(store.cache_file(path)) as connection:
        assert connection.execute("SELECT count(*) FROM schema_fields").fetchone() == (2,)
    old = store.cache_file(path)
    result = store.write_record_delta(path, previous, previous, expected_signature=None)
    assert result["store_rebuilt"] and old.exists() and old != store.cache_file(path)
    assert store.read_record_cache(path) == payload


@pytest.mark.parametrize("body", [b'[true]', b'[1.0]', b'[999999]', b'null', b'{}'])
def test_interned_schema_invalid_references_fail_closed(path, body):
    payload = seed(path)
    payload["schemas"] = {"one": [["date", "date32"]]}
    migrate(path, payload, interned_schemas=True)
    with sqlite3.connect(store.cache_file(path)) as connection:
        connection.execute("UPDATE sections SET body=? WHERE section='schemas'", (body,))
    with pytest.raises(ValueError, match="schema field reference"):
        store.read_record_cache(path)


def test_factored_legacy_and_numeric_types_remain_exact(path):
    payload = seed(path)
    payload["files"] = {
        "a": {"stats": _footer(count=2**100, non_null=[2**100, -0.0, False, None], unknown={"保留": [float("inf")]})},
        "b": {"stats": None}, "c": {"stats": [1, None]}, "d": {"stats": _footer(count=True)},
    }
    migrate(path, payload, factored_stats=True)
    actual = store.read_record_cache(path)
    assert actual == payload
    assert type(actual["files"]["d"]["stats"]["count"]) is bool
    assert math.copysign(1., actual["files"]["a"]["stats"]["non_null"][1]) == -1.
    before = store.read_record_cache(path)
    current = {**before, "files": {**before["files"], "d": {"stats": _footer(count=1)}}}
    store.write_record_delta(path, current, before, expected_signature=inventory._cache_signature(path))
    assert type(store.read_record_cache(path)["files"]["d"]["stats"]["count"]) is int


def test_factored_removes_only_unreferenced_blocks_in_same_transaction(path):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}, "b": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"a": {"stats": _footer(count=8)}, "b": previous["files"]["b"]}}
    observed = store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert observed["stats_rows_written"] == 1 and observed["stats_rows_deleted"] == 0
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"a": previous["files"]["a"]}}
    observed = store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert observed["stats_rows_deleted"] == 1
    with sqlite3.connect(store.cache_file(path)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM footer_stats").fetchone()[0] == 1
    assert store.read_record_cache(path) == current


def test_factored_late_failure_rolls_back_new_blocks_references_and_metadata(path):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    with sqlite3.connect(store.cache_file(path)) as connection:
        connection.execute("CREATE TRIGGER reject_late BEFORE INSERT ON sections WHEN NEW.key='bad' "
                           "BEGIN SELECT RAISE(ABORT,'late failure'); END")
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"b": {"stats": _footer(count=8)}}, "schemas": {"bad": []}}
    with pytest.raises(sqlite3.IntegrityError, match="late failure"):
        store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert store.read_record_cache(path) == payload
    with sqlite3.connect(store.cache_file(path)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM footer_stats").fetchone()[0] == 1


@pytest.mark.parametrize("damage", ["missing", "body", "codec"])
def test_factored_corrupt_or_missing_block_never_becomes_empty_inventory(path, damage):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    with sqlite3.connect(store.cache_file(path)) as connection:
        if damage == "missing":
            connection.execute("DELETE FROM footer_stats")
        elif damage == "body":
            connection.execute("UPDATE footer_stats SET body=?", (b'{}',))
        else:
            connection.execute("UPDATE footer_stats SET codec=99")
    with pytest.raises(ValueError):
        store.read_record_cache(path)
    assert inventory._read_json(path) is None


def test_factored_valid_quick_index_migration_and_feature_source_checks(path):
    from datetime import date
    import polars as pl
    root = path.parent
    source = root / "data_yahoo/crypto/A_features.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"date": [date(2025, 1, 1)], "close": [1.]}).write_parquet(source)
    first = inventory.build_record_inventory(root, refresh=True, incremental_store=True, incremental_metadata=True)
    assert inventory.build_record_inventory(root, refresh=True)["fast_index_hit"]
    frame = inventory.InventorySnapshot(root)
    upgraded = inventory.build_record_inventory(root, refresh=True, snapshot=frame,
        incremental_store=True, incremental_metadata=True, incremental_stats=True)
    assert upgraded["cache_encoding_observation"]["backend"] == "sqlite_factored_v3"
    for key in ("feature_revision", "source_observation_root", "datasets"):
        assert upgraded[key] == first[key]
    expected = inventory.build_feature_inventory(root, snapshot=frame)
    assert expected == inventory.build_feature_inventory(root)
    pl.DataFrame({"date": [date(2025, 1, 1), date(2025, 1, 2)], "close": [1., 2.]}).write_parquet(source)
    changed = inventory.build_record_inventory(root, refresh=True, snapshot=frame, incremental_stats=True)
    assert not changed["cache_encoding_observation"]["store_rebuilt"]
    assert inventory.build_feature_inventory(root, snapshot=frame)["rows"][0]["rows_with_field"] == 2


def test_factored_reader_and_writer_keep_working_without_optional_native_codec(path, monkeypatch):
    from stockagent.live import data_monitor_record_codec as codec
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    monkeypatch.setattr(codec, "_native", None)
    assert store.read_record_cache(path) == payload
    previous = store.read_record_cache(path)
    current = {**previous, "files": {"b": {"stats": _footer(count=2**100)}}}
    store.write_record_delta(path, current, previous, expected_signature=inventory._cache_signature(path))
    assert store.read_record_cache(path) == current


def test_factored_stale_snapshot_rebuilds_instead_of_rebinding_newer_blocks(path):
    payload = seed(path)
    payload["files"] = {"a": {"stats": _footer()}}
    migrate(path, payload, factored_stats=True)
    previous = store.read_record_cache(path)
    signature = inventory._cache_signature(path)
    newer = {**previous, "files": {"b": {"stats": _footer(count=8)}}}
    store.write_record_delta(path, newer, previous, expected_signature=signature)
    old_database = store.cache_file(path)
    restored = {**previous, "feature_revision": "c" * 32}
    observed = store.write_record_delta(path, restored, previous, expected_signature=signature)
    assert observed["store_rebuilt"]
    assert store.cache_file(path) != old_database and old_database.exists()
    assert store.read_record_cache(path) == restored
