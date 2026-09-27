from __future__ import annotations

import hashlib
import json
from pathlib import Path
import struct
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import openbb_l1_view_schema as view_schema


def _arrow_fingerprint(parquet_file: pq.ParquetFile) -> str:
    return hashlib.sha256(
        parquet_file.schema_arrow.remove_metadata().serialize().to_pybytes()
    ).hexdigest()


def _write(tmp_path: Path, name: str, table: pa.Table, **options) -> Path:
    path = tmp_path / f"{name}.parquet"
    pq.write_table(table, path, **options)
    return path


def _metadata_proxy(parquet_file: pq.ParquetFile, metadata: object):
    return SimpleNamespace(
        schema=parquet_file.schema,
        schema_arrow=parquet_file.schema_arrow,
        metadata=SimpleNamespace(metadata=metadata),
    )


def test_identity_ignores_addresses_rows_compression_and_path(tmp_path: Path) -> None:
    first_path = _write(tmp_path, "first", pa.table({"x": [1]}))
    second_path = _write(
        tmp_path, "second", pa.table({"x": [2, 3]}), compression="gzip"
    )
    with pq.ParquetFile(first_path) as first, pq.ParquetFile(second_path) as second:
        first_identity = view_schema.physical_schema_identity(first)
        assert first_identity is not None and len(first_identity) == 64
        assert first_identity == view_schema.physical_schema_identity(second)


def test_identity_uses_existing_metadata_without_reopening(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        expected = view_schema.physical_schema_identity(parquet_file)

        def forbidden_open(*args, **kwargs):
            raise AssertionError("identity must not reopen a Parquet file")

        monkeypatch.setattr(pq, "ParquetFile", forbidden_open)
        assert expected is not None
        assert view_schema.physical_schema_identity(parquet_file) == expected


def test_joint_identity_serializes_each_footer_once(tmp_path: Path) -> None:
    path = _write(tmp_path, "joint", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        expected = (_arrow_fingerprint(parquet_file), view_schema.physical_schema_identity(parquet_file))

        class CountedFooter:
            reads = 0

            def __getattr__(self, name):
                return getattr(parquet_file, name)

            @property
            def schema_arrow(self):
                self.reads += 1
                if self.reads > 1:
                    raise AssertionError("one Arrow schema read per joint proof")
                return parquet_file.schema_arrow

        observed = CountedFooter()
        assert view_schema.schema_identities(observed) == expected
        assert observed.reads == 1


def test_joint_identity_does_not_cache_between_files(tmp_path: Path) -> None:
    first_path = _write(tmp_path, "integer", pa.table({"x": [1]}))
    second_path = _write(tmp_path, "string", pa.table({"x": ["one"]}))
    with pq.ParquetFile(first_path) as first, pq.ParquetFile(second_path) as second:
        first_arrow, first_physical = view_schema.schema_identities(first)
        second_arrow, second_physical = view_schema.schema_identities(second)
        assert first_arrow != second_arrow
        assert first_physical != second_physical


def test_joint_identity_retains_required_arrow_failure() -> None:
    class BrokenArrow:
        @property
        def schema_arrow(self):
            raise ValueError("broken Arrow footer")

    with pytest.raises(ValueError, match="broken Arrow footer"):
        view_schema.schema_identities(BrokenArrow())
    assert view_schema.physical_schema_identity(BrokenArrow()) is None


def test_joint_identity_unknown_version_still_audits_arrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write(tmp_path, "unknown", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        expected = _arrow_fingerprint(parquet_file)
        monkeypatch.setattr(pa, "__version__", "unknown")
        assert view_schema.schema_identities(parquet_file) == (expected, None)


def test_int96_and_nanoseconds_have_same_arrow_but_distinct_identity(
    tmp_path: Path,
) -> None:
    table = pa.table({"t": pa.array([1234567890123456789], pa.timestamp("ns"))})
    legacy_path = _write(
        tmp_path, "legacy", table, use_deprecated_int96_timestamps=True
    )
    modern_path = _write(tmp_path, "modern", table)
    with pq.ParquetFile(legacy_path) as legacy, pq.ParquetFile(modern_path) as modern:
        assert _arrow_fingerprint(legacy) == _arrow_fingerprint(modern)
        legacy_identity = view_schema.physical_schema_identity(legacy)
        modern_identity = view_schema.physical_schema_identity(modern)
        assert legacy_identity is not None and modern_identity is not None
        assert legacy_identity != modern_identity
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        paths = [str(legacy_path), str(modern_path)]
        original = database.execute(
            "SELECT epoch_ns(t) FROM read_parquet(?, union_by_name=true)", [paths]
        ).fetchall()
        unguarded = database.execute(
            "SELECT epoch_ns(t) FROM read_parquet(?, union_by_name=false)", [paths]
        ).fetchall()
        assert original == [(1234567890123456000,), (1234567890123456789,)]
        assert unguarded == [(1234567890123456000,), (1234567890123456000,)]


def test_geo_metadata_changes_duckdb_type_without_changing_physical_schema(
    tmp_path: Path,
) -> None:
    geo = {
        "version": "1.1.0",
        "primary_column": "geom",
        "columns": {
            "geom": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": None,
            }
        },
    }
    plain = pa.table({"geom": [struct.pack("<BIdd", 1, 1, 1.0, 2.0)]})
    plain_path = _write(tmp_path, "plain", plain)
    geo_path = _write(
        tmp_path,
        "geo",
        plain.replace_schema_metadata({b"geo": json.dumps(geo).encode()}),
    )
    with pq.ParquetFile(plain_path) as first, pq.ParquetFile(geo_path) as second:
        assert _arrow_fingerprint(first) == _arrow_fingerprint(second)
        assert view_schema._schema_tree(first.schema) == view_schema._schema_tree(
            second.schema
        )
        assert first.schema.equals(second.schema)
        assert view_schema.physical_schema_identity(first) is not None
        assert view_schema.physical_schema_identity(
            first
        ) != view_schema.physical_schema_identity(second)
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        database.execute("SET enable_geoparquet_conversion=true")
        plain_type = database.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(plain_path)]
        ).fetchone()[1]
        geo_type = database.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(geo_path)]
        ).fetchone()[1]
        assert plain_type == "BLOB"
        assert geo_type == "GEOMETRY"


def test_group_field_id_is_in_tree_even_when_leaf_equals_misses_it(
    tmp_path: Path,
) -> None:
    paths = []
    for field_id in (7, 8):
        field = pa.field(
            "outer",
            pa.struct([("inner", pa.int32())]),
            metadata={b"PARQUET:field_id": str(field_id).encode()},
        )
        table = pa.Table.from_arrays(
            [pa.array([{"inner": 1}], type=field.type)], schema=pa.schema([field])
        )
        paths.append(_write(tmp_path, str(field_id), table))
    with pq.ParquetFile(paths[0]) as first, pq.ParquetFile(paths[1]) as second:
        assert first.schema.equals(second.schema)
        # Isolate the tree contribution from the Arrow field metadata and KVs.
        second_proxy = _metadata_proxy(second, {})
        second_proxy.schema_arrow = first.schema_arrow
        first_identity = view_schema.physical_schema_identity(
            _metadata_proxy(first, {})
        )
        second_identity = view_schema.physical_schema_identity(second_proxy)
        assert first_identity is not None and second_identity is not None
        assert first_identity != second_identity


def test_all_metadata_values_are_framed_as_exact_bytes(tmp_path: Path) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        identities = [
            view_schema.physical_schema_identity(
                _metadata_proxy(parquet_file, metadata)
            )
            for metadata in (
                {b"a": b"bc"},
                {b"ab": b"c"},
                {b"a": b"bd"},
                {b"a": b"\xff\x00"},
                {b"a": b"\xfe\x00"},
                {b"ARROW:schema": b"one"},
                {b"ARROW:schema": b"two"},
                {},
            )
        ]
        assert None not in identities
        assert len(set(identities)) == len(identities)
        first = _metadata_proxy(parquet_file, {b"a": b"1", b"b": b"2"})
        second = _metadata_proxy(parquet_file, {b"b": b"2", b"a": b"1"})
        assert view_schema.physical_schema_identity(
            first
        ) == view_schema.physical_schema_identity(second)


class _SchemaProxy:
    def __init__(self, schema, representation=None, column_override=None):
        self.schema = schema
        self.representation = representation
        self.column_override = column_override

    def __str__(self):
        return str(self.schema) if self.representation is None else self.representation

    def __len__(self):
        return len(self.schema)

    def column(self, index):
        return (
            self.schema.column(index)
            if self.column_override is None
            else self.column_override
        )


@pytest.mark.parametrize(
    "representation",
    [
        "unknown schema format",
        "<unknown>\nrequired group field_id=-1 schema {\n}\n",
        "<pyarrow._parquet.ParquetSchema object at 0x1>\ntruncated",
        "<pyarrow._parquet.ParquetSchema object at 0x1>\nrequired group field_id=-1 schema {\n  unexpected;\n}\n",
        "<pyarrow._parquet.ParquetSchema object at 0x1>\nrequired group field_id=-1 schema {\n}\n}\n",
    ],
)
def test_unknown_schema_representation_returns_none(
    tmp_path: Path, representation: str
) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        proxy = _metadata_proxy(parquet_file, {})
        proxy.schema = _SchemaProxy(parquet_file.schema, representation)
        assert view_schema.physical_schema_identity(proxy) is None


@pytest.mark.parametrize("metadata", [{"text": b"value"}, {b"key": "text"}, [], 1])
def test_unknown_metadata_api_returns_none(tmp_path: Path, metadata: object) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        assert (
            view_schema.physical_schema_identity(
                _metadata_proxy(parquet_file, metadata)
            )
            is None
        )


def test_missing_column_api_returns_none(tmp_path: Path) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        proxy = _metadata_proxy(parquet_file, {})
        proxy.schema = _SchemaProxy(parquet_file.schema, column_override=object())
        assert view_schema.physical_schema_identity(proxy) is None


def test_unknown_arrow_version_returns_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        monkeypatch.setattr(pa, "__version__", "999.0.0")
        assert view_schema.physical_schema_identity(parquet_file) is None


def test_revision_is_bound_to_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        original = view_schema.physical_schema_identity(parquet_file)
        monkeypatch.setattr(view_schema, "SCHEMA_IDENTITY_REVISION", 999)
        assert original is not None
        assert original != view_schema.physical_schema_identity(parquet_file)


@pytest.mark.parametrize(
    "data_type",
    [
        pa.null(),
        pa.bool_(),
        pa.int8(),
        pa.uint64(),
        pa.float16(),
        pa.float64(),
        pa.decimal128(20, 4),
        pa.string(),
        pa.binary(),
        pa.binary(16),
        pa.date32(),
        pa.time64("us"),
        pa.timestamp("ns"),
        pa.timestamp("us", tz="UTC"),
        pa.list_(pa.int32()),
        pa.struct([("x", pa.list_(pa.string()))]),
        pa.map_(pa.string(), pa.int64()),
        pa.json_(),
        pa.uuid(),
    ],
)
def test_supported_leaf_and_group_representations(tmp_path: Path, data_type) -> None:
    path = _write(tmp_path, "typed", pa.table({"x": pa.array([None], type=data_type)}))
    with pq.ParquetFile(path) as parquet_file:
        assert view_schema.physical_schema_identity(parquet_file) is not None


def test_column_order_remains_part_of_identity(tmp_path: Path) -> None:
    first_path = _write(tmp_path, "first", pa.table({"a": [1], "b": [2]}))
    second_path = _write(tmp_path, "second", pa.table({"b": [2], "a": [1]}))
    with pq.ParquetFile(first_path) as first, pq.ParquetFile(second_path) as second:
        first_identity = view_schema.physical_schema_identity(first)
        second_identity = view_schema.physical_schema_identity(second)
        assert first_identity is not None and second_identity is not None
        assert first_identity != second_identity


def test_unknown_logical_json_returns_none(tmp_path: Path) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))
    with pq.ParquetFile(path) as parquet_file:
        proxy = _metadata_proxy(parquet_file, {})
        column = SimpleNamespace(logical_type=SimpleNamespace(to_json=lambda: "[]"))
        proxy.schema = _SchemaProxy(parquet_file.schema, column_override=column)
        assert view_schema.physical_schema_identity(proxy) is None


def test_memory_error_is_not_hidden_as_unsupported_schema(tmp_path: Path) -> None:
    path = _write(tmp_path, "first", pa.table({"x": [1]}))

    class OutOfMemorySchema:
        def __str__(self):
            raise MemoryError("injected")

    with pq.ParquetFile(path) as parquet_file:
        proxy = _metadata_proxy(parquet_file, {})
        proxy.schema = OutOfMemorySchema()
        with pytest.raises(MemoryError, match="injected"):
            view_schema.physical_schema_identity(proxy)


def _actual_grouped_query(database, output_dir: Path, tables):
    from scripts import compact_openbb_l1 as compaction

    paths = []
    proofs = {}
    for index, (table, options) in enumerate(tables):
        segment_id = f"{index:024x}"
        path = compaction._segment_output(
            output_dir, compaction.SCHEMA_GROUPED_ENDPOINT, segment_id
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path, **options)
        paths.append(path)
        before = compaction._view_file_signature(path)
        with pq.ParquetFile(path) as parquet_file:
            proofs[str(path)] = compaction._view_source_proof(
                path,
                segment_id,
                parquet_file,
                before,
                output_rows=table.num_rows,
                output_bytes=path.stat().st_size,
                arrow_fingerprint=_arrow_fingerprint(parquet_file),
            )
    candidate = compaction._schema_grouped_view(database, paths, output_dir, proofs)
    assert candidate is not None
    return compaction._parquet_view_query(paths), candidate[0]


def _assert_full_query_parity(database, original: str, candidate: str) -> None:
    assert (
        database.execute("DESCRIBE " + original).fetchall()
        == database.execute("DESCRIBE " + candidate).fetchall()
    )
    # SQL compares exact timestamps and duplicate multiplicities; fetching
    # Python datetime values would discard sub-microsecond evidence.
    for left, right in ((original, candidate), (candidate, original)):
        assert database.execute(
            f"SELECT count(*) FROM (({left}) EXCEPT ALL ({right}))"
        ).fetchone() == (0,)


def test_actual_grouped_view_preserves_int96_nanosecond_values(tmp_path: Path) -> None:
    table = pa.table({"t": pa.array([1234567890123456789], pa.timestamp("ns"))})
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        original, candidate = _actual_grouped_query(
            database,
            tmp_path,
            [(table, {"use_deprecated_int96_timestamps": True}), (table, {})],
        )
        assert candidate.count("UNION ALL BY NAME") == 1
        _assert_full_query_parity(database, original, candidate)
        assert database.execute(
            f"SELECT epoch_ns(t) FROM ({candidate}) ORDER BY 1"
        ).fetchall() == [(1234567890123456000,), (1234567890123456789,)]


def _geometry_table(*, with_geo: bool, bbox: list[float] | None = None) -> pa.Table:
    table = pa.table({"geom": [struct.pack("<BIdd", 1, 1, 1.0, 2.0)]})
    if not with_geo:
        return table
    column = {"encoding": "WKB", "geometry_types": ["Point"], "crs": None}
    if bbox is not None:
        column["bbox"] = bbox
    geo = {"version": "1.1.0", "primary_column": "geom", "columns": {"geom": column}}
    return table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})


def test_actual_grouped_view_keeps_geo_metadata_groups_and_full_rows(
    tmp_path: Path,
) -> None:
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        database.execute("SET enable_geoparquet_conversion=true")
        first = _geometry_table(with_geo=True)
        second = _geometry_table(with_geo=True, bbox=[1.0, 2.0, 1.0, 2.0])
        original, candidate = _actual_grouped_query(
            database, tmp_path, [(first, {}), (second, {}), (first, {})]
        )
        assert candidate.count("UNION ALL BY NAME") == 1
        _assert_full_query_parity(database, original, candidate)
        assert database.execute(f"SELECT count(*) FROM ({candidate})").fetchone() == (
            3,
        )


def test_actual_grouped_view_preserves_mixed_blob_geometry_read_failure(
    tmp_path: Path,
) -> None:
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        database.execute("SET enable_geoparquet_conversion=true")
        original, candidate = _actual_grouped_query(
            database,
            tmp_path,
            [
                (_geometry_table(with_geo=False), {}),
                (_geometry_table(with_geo=True), {}),
            ],
        )
        assert candidate.count("UNION ALL BY NAME") == 1
        assert (
            database.execute("DESCRIBE " + original).fetchall()
            == database.execute("DESCRIBE " + candidate).fetchall()
        )
        for query in (original, candidate):
            with pytest.raises(duckdb.ConversionException, match="BLOB -> GEOMETRY"):
                database.execute(query).fetchall()
