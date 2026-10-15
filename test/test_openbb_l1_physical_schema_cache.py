from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import openbb_l1_view_schema as schemas


def footer(tmp_path: Path, name: str, table: pa.Table, **options):
    path = tmp_path / f"{name}.parquet"
    pq.write_table(table, path, **options)
    return pq.ParquetFile(path)


def test_cache_reuses_only_physical_reflection_and_is_bounded(tmp_path):
    cache = schemas.PhysicalSchemaIdentityCache()
    first = footer(tmp_path, "first", pa.table({"x": [1]}))
    second = footer(tmp_path, "second", pa.table({"x": [2, 3, 4]}), compression="gzip")
    for item in (first, second):
        assert schemas.schema_identities(item, physical_cache=cache) == schemas.schema_identities(item)
    assert first.metadata.num_rows == 1 and second.metadata.num_rows == 3
    assert cache.hits == 1 and cache.misses == 1
    assert cache.snapshot()["entries"] == 1
    assert 0 < cache.retained_serialized_bytes <= cache.max_serialized_bytes


@pytest.mark.parametrize("data_type", [pa.int32(), pa.decimal128(20, 4), pa.binary(16),
    pa.timestamp("us", tz="UTC"), pa.list_(pa.int32()),
    pa.struct([("nested", pa.list_(pa.string()))]), pa.map_(pa.string(), pa.int64()),
    pa.json_(), pa.uuid()])
def test_all_supported_descriptors_match_reference_on_hits(tmp_path, data_type):
    cache = schemas.PhysicalSchemaIdentityCache()
    table = pa.table({"x": pa.array([None], type=data_type)})
    for index in range(2):
        item = footer(tmp_path, str(index), table)
        assert schemas.schema_identities(item, physical_cache=cache) == schemas.schema_identities(item)
    assert cache.hits == 1 and cache.misses == 1


def test_cache_keeps_int96_metadata_group_ids_and_column_order_distinct(tmp_path):
    cache = schemas.PhysicalSchemaIdentityCache()
    timestamp = pa.table({"x": pa.array([1234567890123456789], pa.timestamp("ns"))})
    tables = [(timestamp, {}), (timestamp, {"use_deprecated_int96_timestamps": True})]
    for metadata in ({b"geo": b"first"}, {b"geo": b"second"}, {b"a": b"bc"}, {b"ab": b"c"}):
        tables.append((pa.table({"x": [1]}).replace_schema_metadata(metadata), {}))
    for field_id in (7, 8):
        field = pa.field("group", pa.struct([("leaf", pa.int32())]),
                         metadata={b"PARQUET:field_id": str(field_id).encode()})
        tables.append((pa.Table.from_arrays([pa.array([None], type=field.type)],
                                           schema=pa.schema([field])), {}))
    tables.extend([(pa.table({"a": [1], "b": [2]}), {}), (pa.table({"b": [2], "a": [1]}), {})])
    identities = []
    for index, (table, options) in enumerate(tables):
        item = footer(tmp_path, str(index), table, **options)
        observed = schemas.schema_identities(item, physical_cache=cache)
        assert observed == schemas.schema_identities(item)
        identities.append(observed[1])
    assert None not in identities and len(set(identities)) == len(identities)
    assert cache.hits == 0


def test_cache_does_not_hide_required_arrow_failures_after_hit(tmp_path):
    cache = schemas.PhysicalSchemaIdentityCache()
    item = footer(tmp_path, "first", pa.table({"x": [1]}))
    schemas.schema_identities(item, physical_cache=cache)

    class Broken:
        schema = item.schema
        metadata = item.metadata

        @property
        def schema_arrow(self):
            raise ValueError("required Arrow audit failed")

    with pytest.raises(ValueError, match="required Arrow"):
        schemas.schema_identities(Broken(), physical_cache=cache)
    assert cache.hits == 0


def test_cache_checks_current_arrow_digest_on_every_hit(tmp_path):
    cache = schemas.PhysicalSchemaIdentityCache()
    item = footer(tmp_path, "first", pa.table({"x": [1]}))
    schemas.schema_identities(item, physical_cache=cache)
    class Counted:
        reads = 0
        schema = item.schema
        metadata = item.metadata

        @property
        def schema_arrow(self):
            self.reads += 1
            return item.schema_arrow

    counted = Counted()
    assert schemas.schema_identities(counted, physical_cache=cache) == schemas.schema_identities(item)
    assert counted.reads == 1 and cache.hits == 1
    changed = SimpleNamespace(schema=item.schema, metadata=item.metadata,
                              schema_arrow=pa.schema([("x", pa.string())]))
    assert schemas.schema_identities(changed, physical_cache=cache) == schemas.schema_identities(changed)
    assert cache.hits == 1 and cache.misses == 2


def test_cache_evicts_lru_and_bypasses_oversize(tmp_path):
    cache = schemas.PhysicalSchemaIdentityCache(max_entries=1)
    first = footer(tmp_path, "first", pa.table({"x": [1]}))
    second = footer(tmp_path, "second", pa.table({"x": ["one"]}))
    for item in (first, second, first):
        assert schemas.schema_identities(item, physical_cache=cache) == schemas.schema_identities(item)
        assert cache.snapshot()["entries"] == 1
        assert cache.retained_serialized_bytes <= cache.max_serialized_bytes
    assert cache.hits == 0 and cache.misses == 3 and cache.evictions == 2
    disabled = schemas.PhysicalSchemaIdentityCache(max_serialized_bytes=1)
    assert schemas.schema_identities(first, physical_cache=disabled) == schemas.schema_identities(first)
    assert disabled.bypasses == 1 and disabled.snapshot()["entries"] == 0


def test_unknown_version_revision_and_memory_failure_never_reuse(tmp_path, monkeypatch):
    cache = schemas.PhysicalSchemaIdentityCache()
    item = footer(tmp_path, "first", pa.table({"x": [1]}))
    original = schemas.schema_identities(item, physical_cache=cache)
    monkeypatch.setattr(schemas, "SCHEMA_IDENTITY_REVISION", 99)
    changed = schemas.schema_identities(item, physical_cache=cache)
    assert changed == schemas.schema_identities(item) and changed[1] != original[1]
    assert cache.hits == 0
    monkeypatch.setattr(pa, "__version__", "unknown")
    assert schemas.schema_identities(item, physical_cache=cache) == (changed[0], None)
    assert cache.hits == 0
    monkeypatch.setattr(pa, "__version__", "25.0.0")
    def exhausted(_schema):
        raise MemoryError("injected schema cache failure")
    monkeypatch.setattr(schemas, "_schema_tree", exhausted)
    with pytest.raises(MemoryError, match="schema cache"):
        schemas.schema_identities(item, physical_cache=cache)


def test_native_descriptor_equality_is_required_even_for_identical_key(tmp_path, monkeypatch):
    cache = schemas.PhysicalSchemaIdentityCache()
    first = footer(tmp_path, "first", pa.table({"x": [1]}))
    second = footer(tmp_path, "second", pa.table({"x": ["one"]}))
    # The tree/Arrow/KV key alone must not be the authority. Inject a key
    # collision; native equality must miss and execute the complete reference.
    class SameArrow:
        schema = second.schema
        schema_arrow = first.schema_arrow
        metadata = SimpleNamespace(metadata=first.metadata.metadata,
                                   serialized_size=first.metadata.serialized_size)
    monkeypatch.setattr(schemas, "_schema_tree", lambda _schema: "same injected tree")
    schemas.schema_identities(first, physical_cache=cache)
    assert schemas.schema_identities(SameArrow(), physical_cache=cache) == schemas.schema_identities(SameArrow())
    assert cache.hits == 0 and cache.misses == 2


@pytest.mark.parametrize("broken", ["rows", "bytes", "arrow", "signature"])
def test_physical_hit_does_not_waive_per_file_proof_checks(tmp_path, monkeypatch, broken):
    from scripts import compact_openbb_l1 as compactor
    from scripts.benchmark_openbb_l1_footer_reader import reader_variant

    for index in range(2):
        pq.write_table(pa.table({"x": [1] * (index + 1)}), tmp_path / f"{index}.parquet")
    with reader_variant(compactor, "schema_cache") as cache:
        first = tmp_path / "0.parquet"
        item = pq.ParquetFile(first)
        compactor._view_source_proof(first, "first", item, compactor._view_file_signature(first),
            output_rows=1, output_bytes=first.stat().st_size,
            arrow_fingerprint=schemas.schema_identities(item)[0])
        second = tmp_path / "1.parquet"
        item = pq.ParquetFile(second)
        before = compactor._view_file_signature(second)
        if broken == "signature":
            monkeypatch.setattr(compactor, "_view_file_signature", lambda _path: (*before[:4], before[4] + 1))
        with pytest.raises(RuntimeError, match="receipt mismatch|changed during"):
            compactor._view_source_proof(second, "second", item, before,
                output_rows=1 if broken == "rows" else 2,
                output_bytes=before[2] + int(broken == "bytes"),
                arrow_fingerprint="wrong" if broken == "arrow" else schemas.schema_identities(item)[0])
        assert cache.hits == 1
