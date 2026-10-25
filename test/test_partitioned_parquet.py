"""Explicit immutable partitions preserve the entire canonical typed table."""
from datetime import date
import json
import os
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data import partitioned_parquet as parts
from stockagent.data.tw_public_incremental import table_content_sha256


@pytest.fixture(autouse=True)
def isolated_capacity_controls(tmp_path, monkeypatch):
    # systemd's private /tmp may be on a different device from /var/lib.
    # Exercise the real namespace/filesystem lease on the fixture's device.
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "capacity-controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})


def table(days=None, values=None):
    days = days or [date(2024, 1, 1), date(2024, 12, 31), date(2025, 1, 1)]
    return pa.table({"date": pa.array(days, type=pa.date32()),
        "symbol": ["2330"] * len(days), "value": pa.array(
            values if values is not None else [-0.0, None, float("nan")], type=pa.float64())})


def write(path, value, **kwargs):
    return parts.write_partitioned_table(value, path, source_identity={"input": table_content_sha256(value)}, **kwargs)


def same(left, right):
    assert left.schema.equals(right.schema, check_metadata=True)
    assert table_content_sha256(left) == table_content_sha256(right)


def test_legacy_reader_keeps_values_schema_and_identity(tmp_path):
    path = tmp_path / "2330_features.parquet"
    value = table().replace_schema_metadata({b"source": b"official"})
    pq.write_table(value, path)
    same(parts.read_table(path), value)
    assert parts.source_paths([path]) == [path]
    assert parts.discover_sources(tmp_path) == [path]


@pytest.mark.parametrize("rows", [False, True])
def test_same_snapshot_reuses_verified_footers_and_closes_owned_readers(tmp_path, monkeypatch, rows):
    path = tmp_path / "source.parquet"
    value = table().replace_schema_metadata({b"source": b"literal"})
    write(path, value, **(dict(period="rows", date_column=None, partition_rows=2) if rows else {}))
    original, observations = pq.ParquetFile, []

    class ObservedFile:
        def __init__(self, path, **kwargs):
            self.file = original(path, **kwargs)
            self.record = {"path": Path(path), "metadata": kwargs.get("metadata"), "closed": False}
            observations.append(self.record)

        def __getattr__(self, name):
            return getattr(self.file, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.file.close()
            self.record["closed"] = True

    monkeypatch.setattr(pq, "ParquetFile", ObservedFile)
    with parts.ParquetSource(path) as source:
        assert len(observations) == len(source.members)
        assert all(row["closed"] and row["metadata"] is None for row in observations)
        same(source.table(), value)
        same(source.table(columns=["value"]), value.select(["value"]))
        same(source.table(tail_rows=1), value.slice(value.num_rows - 1))
        stored = dict(zip(source.members, source._metadata, strict=True))
        reads = observations[len(source.members):]
        assert len(reads) == 2 * len(source.members) + 1
        assert all(row["metadata"] is stored[row["path"]] and row["closed"] for row in reads)
    # Returned Arrow buffers must survive closing every owned mmap reader.
    same(parts.read_table(path), value)


def test_partition_decode_failure_closes_the_reader_and_preserves_generation(tmp_path, monkeypatch):
    path = tmp_path / "source.parquet"
    write(path, table())
    with parts.ParquetSource(path) as source:
        original, closed = pq.ParquetFile, []

        class BrokenFile:
            def __init__(self, path, **kwargs):
                self.file = original(path, **kwargs)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self.file.close()
                closed.append(True)

            def read(self, **kwargs):
                raise OSError("independent decode failure")

        monkeypatch.setattr(pq, "ParquetFile", BrokenFile)
        before = source.fingerprints()
        with pytest.raises(OSError, match="independent decode failure"):
            source.table()
        assert closed == [True]
        assert source.fingerprints() == before


def _live_source_keys(root, *, external=None):
    from types import SimpleNamespace
    from stockagent.config import DataConfig
    from stockagent.live.completed_inference_cache import source_fingerprint
    from stockagent.live.signal_engine import _live_panel_cache_key

    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index",
        use_tw_public_features=external is not None, tw_public_feature_path=str(external or "")))
    kwargs = {"external_feature_path": str(external)} if external else {}
    return (source_fingerprint(config, rule_data_dir=root.parent),
            _live_panel_cache_key(config, live_tail_rows=48, panel_kwargs=kwargs))


@pytest.mark.parametrize("external", [False, True])
def test_live_source_keys_track_only_current_partition_members_and_logical_summary(tmp_path, external):
    root = tmp_path / "stocks"
    root.mkdir()
    path = tmp_path / "public.parquet" if external else root / "2330_features.parquet"
    write(path, table())
    # Explicit pointer and logical legacy spelling select the same summary.
    argument = parts.manifest_path(path) if external else None
    before = _live_source_keys(root, external=argument)
    archive = parts.manifest_path(path).with_name(parts.manifest_path(path).name + ".parts") / "unreferenced.parquet"
    archive.write_bytes(b"not an input or a valid Parquet file")
    (root / "status.json").write_text("heartbeat")
    assert _live_source_keys(root, external=argument) == before
    write(path, table([date(2025, 1, 2)], [4.]), replace_from=date(2025, 1, 2))
    after = _live_source_keys(root, external=argument)
    assert all(left != right for left, right in zip(before, after, strict=True))
    if external:
        path.with_suffix(".summary.json").write_text('{"updated": true}')
        latest = _live_source_keys(root, external=argument)
        assert all(left != right for left, right in zip(after, latest, strict=True))


@pytest.mark.parametrize("failure", ["corrupt_old_member", "missing_member", "ambiguous"])
def test_live_source_keys_fail_closed_for_partition_corruption_outside_tail(tmp_path, failure):
    from types import SimpleNamespace
    from stockagent.config import DataConfig
    from stockagent.live.completed_inference_cache import source_fingerprint
    from stockagent.live.signal_engine import _live_panel_cache_key

    root = tmp_path / "stocks"
    root.mkdir()
    path = root / "2330_features.parquet"
    write(path, table())
    _live_source_keys(root)
    with parts.ParquetSource(path) as source:
        old = source.members[0]
    if failure == "ambiguous":
        path.write_bytes(b"stale legacy")
    elif failure == "missing_member":
        old.unlink()
    else:
        before = old.stat()
        raw = old.read_bytes()
        old.chmod(0o644)
        old.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
        os.utime(old, ns=(before.st_atime_ns, before.st_mtime_ns))
        old.chmod(0o444)
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index"))
    for read in (lambda: source_fingerprint(config, rule_data_dir=root.parent),
                 lambda: _live_panel_cache_key(config, live_tail_rows=1, panel_kwargs={})):
        with pytest.raises((ValueError, FileNotFoundError)):
            read()


def test_live_panel_key_detects_legacy_in_place_repair_without_directory_change(tmp_path):
    root = tmp_path / "stocks"
    root.mkdir()
    path = root / "2330_features.parquet"
    path.write_bytes(b"before")
    before = _live_source_keys(root)
    directory_mtime, original = root.stat().st_mtime_ns, path.stat()
    path.write_bytes(b"repair")
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert root.stat().st_mtime_ns == directory_mtime
    assert all(left != right for left, right in zip(before, _live_source_keys(root), strict=True))


def test_generation_only_legacy_discovery_avoids_per_member_path_resolution(tmp_path, monkeypatch):
    from pathlib import Path
    from stockagent.live.completed_inference_cache import _parquet_source_generations

    paths = [tmp_path / f"{symbol}_features.parquet" for symbol in ("2330", "2317")]
    for path in paths:
        path.write_bytes(b"generation proof does not decode legacy data")
    generations = {str(path): (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                   for path in paths for s in [path.stat()]}
    def no_resolve(*_args, **_kwargs):
        pytest.fail("legacy directory discovery must not resolve every path")
    monkeypatch.setattr(Path, "resolve", no_resolve)
    assert dict(_parquet_source_generations(feature_roots=(tmp_path,))) == generations


def test_partition_roundtrip_projection_filter_tail_metadata(tmp_path):
    path = tmp_path / "2330_features.parquet"
    value = table().replace_schema_metadata({b"source": b"official", b"raw": b"\x00\xff"})
    result = write(path, value)
    assert result["written_partitions"] == 2
    assert not path.exists()
    assert parts.source_exists(path)
    same(parts.read_table(path), value)
    same(parts.read_table(path, columns=["date", "value"], filters=[("date", ">=", date(2024, 12, 31))]),
         value.select(["date", "value"]).slice(1))
    same(parts.read_table(path, tail_rows=1), value.slice(2))
    assert parts.discover_sources(tmp_path) == [parts.manifest_path(path)]
    assert parts.logical_name(parts.manifest_path(path)) == path.name
    with parts.ParquetSource(path) as source:
        assert source.scan().collect().height == 3
        assert len(source.source_paths()) == 3
    assert np.signbit(parts.read_table(path)["value"][0].as_py())


def test_append_rewrites_only_affected_year_and_retains_original_generation(tmp_path):
    path = tmp_path / "2330_features.parquet"
    original = table()
    write(path, original)
    before = json.loads(parts.manifest_path(path).read_bytes())
    extra = table([date(2025, 1, 2)], [1.234567890123456])
    result = write(path, extra, replace_from=date(2025, 1, 2))
    same(parts.read_table(path), pa.concat_tables([original, extra]))
    after = json.loads(parts.manifest_path(path).read_bytes())
    assert before["members"][0] == after["members"][0]
    assert before["members"][1] != after["members"][1]
    assert result["written_partitions"] == 1
    assert result["reused_partitions"] == 1
    for proof in before["members"]:
        assert (path.parent / proof["path"]).exists()
    assert len(list((tmp_path / (parts.manifest_path(path).name + ".parts")).glob("generation_*.json"))) == 2


def test_correction_and_deletion_rebuild_only_changed_members(tmp_path):
    path = tmp_path / "x.parquet"
    original = table()
    write(path, original)
    changed = original.set_column(2, "value", pa.array([-0.0, 5., float("nan")]))
    result = write(path, changed)
    assert result["written_partitions"] == 1 and result["reused_partitions"] == 1
    same(parts.read_table(path), changed)
    result = write(path, changed.slice(0, 2))
    assert result["written_partitions"] == 0
    same(parts.read_table(path), changed.slice(0, 2))


def _ordered_rows():
    # This is an ordered provider index, not a publication/trading clock.
    bits = np.array([0x8000000000000000, 0x7ff8000000000001,
                     0x3ff0000000000000, 0xbff0000000000000], dtype=np.uint64)
    return pa.table({"source_index": ["2024-Q4", "2024-Q4", "other", None],
        "raw": pa.array(bits.view(np.float64)),
        "optional": pa.array([None, 0, -1, 2], type=pa.int64())}).replace_schema_metadata(
            {b"source": b"original", b"raw-index": b"\x00\xff"})


def _write_rows(path, value, **kwargs):
    return write(path, value, period="rows", date_column=None, partition_rows=2, **kwargs)


def test_ordered_rows_reuse_closed_members_and_read_old_sealed_generation(tmp_path):
    path, original = tmp_path / "raw.parquet", _ordered_rows().slice(0, 3)
    first = _write_rows(path, original)
    before = json.loads(parts.manifest_path(path).read_bytes())
    closed = path.parent / before["members"][0]["path"]
    old_stat = closed.stat()
    result = _write_rows(path, _ordered_rows())
    assert result["written_partitions"] == result["reused_partitions"] == 1
    assert result["written_rows"] == result["reused_rows"] == 2
    assert (closed.stat().st_ino, closed.stat().st_mtime_ns) == (old_stat.st_ino, old_stat.st_mtime_ns)
    same(parts.read_table(path), _ordered_rows())
    same(parts.read_table(first["immutable_manifest"]), original)
    assert parts.is_partition_manifest(first["immutable_manifest"])
    with parts.ParquetSource(path) as source:
        same(source.table(columns=["source_index", "raw"], tail_rows=1),
             _ordered_rows().select(["source_index", "raw"]).slice(3))
        assert source.scan().collect().to_dicts()[0]["source_index"] == "2024-Q4"
        assert result["current_payload_bytes"] == sum(p.stat().st_size for p in source.members)


def test_ordered_rows_correction_shrink_and_noop_keep_exact_history(tmp_path):
    path, original = tmp_path / "raw.parquet", _ordered_rows()
    first = _write_rows(path, original)
    changed = original.set_column(2, "optional", pa.array([7, 0, -1, 2], type=pa.int64()))
    result = _write_rows(path, changed)
    assert result["written_partitions"] == result["reused_partitions"] == 1
    same(parts.read_table(path), changed)
    result = _write_rows(path, changed.slice(0, 2))
    assert result["written_partitions"] == 0 and result["reused_partitions"] == 1
    same(parts.read_table(path), changed.slice(0, 2))
    result = _write_rows(path, changed.slice(0, 2))
    assert result["written_partitions"] == 0
    same(parts.read_table(first["immutable_manifest"]), original)


@pytest.mark.parametrize("ordered", [False, True])
def test_verified_noop_preserves_an_already_open_reader_and_needs_no_write_capacity(
        tmp_path, monkeypatch, ordered):
    from stockagent.data_sync import cache_capacity

    path = tmp_path / "unchanged.parquet"
    value, writer = (_ordered_rows(), _write_rows) if ordered else (table(), write)
    first = writer(path, value)
    with parts.ParquetSource(path) as reader:
        before = {p: parts.payload_info(p) for p in [reader.path, *reader.members,
                  Path(first["immutable_manifest"])]}
        namespace_before = sorted(reader.path.parent.iterdir())

        def no_capacity(*_args, **_kwargs):
            pytest.fail("a complete unchanged version must not request new-write capacity")

        monkeypatch.setattr(cache_capacity, "reserve_cache_write", no_capacity)
        result = writer(path, value)
        assert result["manifest_changed"] is False
        assert result["written_payload_bytes"] == result["written_partitions"] == 0
        assert result["reused_rows"] == value.num_rows
        assert result["immutable_manifest"] == first["immutable_manifest"]
        assert {p: parts.payload_info(p) for p in before} == before
        assert sorted(reader.path.parent.iterdir()) == namespace_before
        same(reader.table(), value)


@pytest.mark.parametrize("change", ["identity", "metadata", "shrink"])
def test_reused_payload_still_admits_a_changed_manifest_and_retains_the_last_good(
        tmp_path, monkeypatch, change):
    from stockagent.data_sync import cache_capacity

    path, value = tmp_path / "source.parquet", _ordered_rows()
    first = _write_rows(path, value)
    before = parts.manifest_path(path).read_bytes()
    original_reserve = cache_capacity.reserve_cache_write
    reservations = []

    def observe(*args, **kwargs):
        reservations.append(kwargs["maximum_growth_bytes"])
        return original_reserve(*args, **kwargs)

    monkeypatch.setattr(cache_capacity, "reserve_cache_write", observe)
    changed = (value.replace_schema_metadata({b"source": b"new evidence"})
               if change == "metadata" else value.slice(0, 2) if change == "shrink" else value)
    kwargs = {"source_identity": {"new_observation": True}} if change == "identity" else {}
    result = parts.write_partitioned_table(changed, path, period="rows", date_column=None,
        partition_rows=2, **({"source_identity": {"input": table_content_sha256(changed)}} | kwargs))
    assert result["manifest_changed"] is True
    assert len(reservations) == 1
    assert result["written_payload_bytes"] == result["written_partitions"] == 0
    assert parts.manifest_path(path).read_bytes() != before
    same(parts.read_table(path), changed)
    same(parts.read_table(first["immutable_manifest"]), value)


@pytest.mark.parametrize("corrupt", ["member", "archive"])
def test_noop_still_rejects_corrupt_stored_evidence_before_capacity(tmp_path, monkeypatch, corrupt):
    from stockagent.data_sync import cache_capacity

    path, value = tmp_path / "corrupt.parquet", table()
    first = write(path, value)
    with parts.ParquetSource(path) as source:
        target = source.members[0] if corrupt == "member" else Path(first["immutable_manifest"])
    # Do not mutate the hardlinked pointer when exercising the archive proof.
    if corrupt == "archive":
        replacement = target.with_name("replacement.json")
        replacement.write_bytes(b'{"different": true}')
        replacement.chmod(0o444)
        replacement.replace(target)
    else:
        target.chmod(0o644)
        target.write_bytes(b"broken immutable original")
        target.chmod(0o444)

    def no_capacity(*_args, **_kwargs):
        pytest.fail("invalid prior evidence must fail before a new write reservation")

    monkeypatch.setattr(cache_capacity, "reserve_cache_write", no_capacity)
    with pytest.raises(ValueError, match="content proof|archive changed"):
        write(path, value)


def test_missing_archive_is_repaired_with_capacity_and_full_previous_proof(tmp_path, monkeypatch):
    from stockagent.data_sync import cache_capacity

    path, value = tmp_path / "missing-archive.parquet", table()
    first = write(path, value)
    Path(first["immutable_manifest"]).unlink()
    original_reserve = cache_capacity.reserve_cache_write
    reservations = []

    def observe(*args, **kwargs):
        reservations.append(kwargs["maximum_growth_bytes"])
        return original_reserve(*args, **kwargs)

    monkeypatch.setattr(cache_capacity, "reserve_cache_write", observe)
    result = write(path, value)
    assert result["manifest_changed"] is True
    assert len(reservations) == 1
    assert result["written_payload_bytes"] == 0
    same(parts.read_table(result["immutable_manifest"]), value)


def test_append_capacity_estimate_counts_new_partitions_without_rewriting_history(tmp_path, monkeypatch):
    from stockagent.data_sync import cache_capacity

    path = tmp_path / "sized.parquet"
    original = pa.table({"v": pa.array(np.arange(1024, dtype=np.float64))})
    options = dict(period="rows", date_column=None, partition_rows=1024,
                   source_identity={"source": "fixed"})
    parts.write_partitioned_table(original, path, **options)
    original_reserve = cache_capacity.reserve_cache_write
    reservations = []

    def observe(*args, **kwargs):
        reservations.append(kwargs["maximum_growth_bytes"])
        return original_reserve(*args, **kwargs)

    monkeypatch.setattr(cache_capacity, "reserve_cache_write", observe)
    appended = pa.concat_tables([original, pa.table({"v": [123.0]})])
    result = parts.write_partitioned_table(appended, path, **options)
    assert result["written_rows"] == 1 and result["reused_rows"] == original.num_rows
    assert reservations == [3 * 8 + 2 * parts.MAX_MANIFEST_BYTES + 512 * 1024]
    same(parts.read_table(path), appended)


@pytest.mark.parametrize("options", [
    {"partition_rows": 0}, {"partition_rows": True}, {"partition_rows": 1024**2 + 1},
    {"date_column": "source_index"}, {"replace_from": date(2024, 1, 1)},
    {"parquet_options": {"row_group_size": True}},
    {"parquet_options": {"compression": "invalid"}}, {"parquet_options": ["snappy"]},
])
def test_ordered_rows_invalid_contract_fails_before_payload_write(tmp_path, options):
    path = tmp_path / "raw.parquet"
    with pytest.raises(ValueError):
        write(path, _ordered_rows(), **{"period": "rows", "date_column": None,
            "partition_rows": 2, **options})
    assert not list(tmp_path.rglob("*.parquet"))
    assert not parts.manifest_path(path).exists()


@pytest.mark.parametrize("failure", ["row_start", "row_count", "archive_name", "archive_writable"])
def test_ordered_rows_rejects_incomplete_members_and_misidentified_archive(tmp_path, failure):
    path = tmp_path / "raw.parquet"
    result = _write_rows(path, _ordered_rows())
    target = parts.manifest_path(path)
    if failure in {"row_start", "row_count"}:
        manifest = json.loads(target.read_bytes())
        manifest["members"][1]["row_start" if failure == "row_start" else "rows"] += 1
        manifest["manifest_sha256"] = parts._digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
        target.unlink()  # Do not mutate the sealed archive's hard-linked inode.
        target.write_text(json.dumps(manifest))
    else:
        target = parts.Path(result["immutable_manifest"])
        if failure == "archive_name":
            wrong = target.with_name("generation_" + "0" * 64 + ".json")
            os.link(target, wrong)
            target = wrong
        else:
            target.chmod(0o644)
    with pytest.raises(ValueError):
        parts.read_table(target)


def test_period_archive_is_readable_without_following_new_pointer(tmp_path):
    path, original = tmp_path / "dated.parquet", table()
    first = write(path, original)
    write(path, table([date(2025, 1, 2)], [3.]), replace_from=date(2025, 1, 2))
    same(parts.read_table(first["immutable_manifest"]), original)


def test_ordered_rows_explicit_codec_preserves_original_raw_values(tmp_path):
    path, value = tmp_path / "raw.parquet", _ordered_rows()
    _write_rows(path, value, parquet_options={"compression": "zstd", "row_group_size": 2,
        "use_dictionary": False, "use_byte_stream_split": True})
    with parts.ParquetSource(path) as snapshot:
        same(snapshot.table(), value)
        assert all(pq.ParquetFile(member).metadata.row_group(0).column(1).compression == "ZSTD"
                   for member in snapshot.members)


def test_complete_shared_row_benchmark_keeps_two_cold_versions_and_all_readers(tmp_path):
    from scripts.benchmark_finlab_incremental_parquet import trial
    value = pa.table({'source_index': ['a', 'a', 'b', 'c'],
        'raw': pa.array([-0.0, None, 3., -1.], type=pa.float64())}).replace_schema_metadata(
            {b'original': b'complete schema'})
    result = trial(value, tmp_path / 'complete-case', None,
        partition_rows=2, native_key='price:收盤價', shared_manifest=True, verify_other_readers=True)
    assert result['cold_versions_fully_reconstructed'] == 2
    assert result['previous_version_readback_after_append'] is True
    assert result['readers_verified'] == ['pyarrow', 'polars', 'duckdb']
    assert result['stages'][1]['source_reused_files'] == 1
    assert result['stages'][1]['source_written_files'] == 1
    assert result['stages'][1]['writer_options'] == {'compression': 'zstd', 'row_group_size': None,
        'use_dictionary': True, 'use_byte_stream_split': False}


@pytest.mark.parametrize("fault", ["missing", "same_size_mtime", "writable", "symlink", "schema", "checksum", "row_count", "reorder"])
def test_corrupt_or_partial_generation_is_never_silently_read(tmp_path, fault):
    path = tmp_path / "x.parquet"
    write(path, table())
    manifest_path = parts.manifest_path(path)
    manifest = json.loads(manifest_path.read_bytes())
    member = path.parent / manifest["members"][0]["path"]
    if fault == "missing":
        member.unlink()
    elif fault == "same_size_mtime":
        before = member.stat()
        member.chmod(0o644)
        with member.open("r+b") as stream:
            stream.seek(20)
            stream.write(b"X")
        member.chmod(0o444)
        os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    elif fault == "writable":
        member.chmod(0o644)
    elif fault == "symlink":
        target = tmp_path / "outside.parquet"
        member.rename(target)
        member.symlink_to(target)
    else:
        if fault == "schema":
            manifest["schema"] = "bad"
        elif fault == "row_count":
            manifest["rows"] += 1
        elif fault == "reorder":
            manifest["members"].reverse()
        else:
            manifest["source_identity"] = {"wrong": True}
        if fault != "checksum":
            manifest["manifest_sha256"] = parts._digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
        manifest_path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, OSError, pa.ArrowException)):
        parts.read_table(path, filters=[("date", ">=", date(2025, 1, 1))])


def test_ambiguous_legacy_copy_cannot_hide_new_manifest(tmp_path):
    path = tmp_path / "x.parquet"
    write(path, table())
    pq.write_table(table(), path)
    with pytest.raises(ValueError, match="ambiguous"):
        parts.read_table(path)
    with pytest.raises(ValueError, match="ambiguous"):
        parts.resolve_source(parts.manifest_path(path))


def test_snapshot_fences_pointer_and_member_changes(tmp_path):
    path = tmp_path / "x.parquet"
    write(path, table())
    snapshot = parts.ParquetSource(path)
    write(path, table([date(2025, 2, 1)], [5.]), replace_from=date(2025, 2, 1))
    with pytest.raises(ValueError, match="pointer changed"):
        snapshot.table()


@pytest.mark.parametrize("fault", ["unordered", "null_date", "text_clock", "bad_boundary", "missing_base", "schema"])
def test_writer_rejects_ambiguous_input_without_overwriting_last_good(tmp_path, fault):
    path = tmp_path / "x.parquet"
    write(path, table())
    before = parts.manifest_path(path).read_bytes()
    value, kwargs = table(), {}
    if fault == "unordered":
        value = value.take(pa.array([2, 0, 1]))
    elif fault == "null_date":
        value = value.set_column(0, "date", pa.array([None] * 3, type=pa.date32()))
    elif fault == "text_clock":
        value = value.set_column(0, "date", value["date"].cast(pa.string()))
    elif fault == "bad_boundary":
        kwargs["replace_from"] = date(2025, 1, 2)
    elif fault == "missing_base":
        path = tmp_path / "missing.parquet"
        kwargs["replace_from"] = date(2024, 1, 1)
    else:
        value = value.append_column("other", pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        write(path, value, **kwargs)
    assert parts.manifest_path(tmp_path / "x.parquet").read_bytes() == before


def test_failed_publication_retains_last_good_and_new_payload_evidence(tmp_path, monkeypatch):
    path = tmp_path / "x.parquet"
    value = table()
    write(path, value)
    original = os.replace
    def fail_pointer(source, target):
        if target == parts.manifest_path(path):
            raise OSError("injected publication failure")
        return original(source, target)
    monkeypatch.setattr(os, "replace", fail_pointer)
    with pytest.raises(OSError, match="injected"):
        write(path, table([date(2025, 1, 2)], [7.]), replace_from=date(2025, 1, 2))
    same(parts.read_table(path), value)


def test_panel_and_hot_tail_readers_share_same_partition_values(tmp_path):
    from downloader.ohlcv_hot_tail import read_logical_parquet
    from stockagent.data.panel import _compute_source_hash, _load_external_feature_arrays, _symbol_name_from_path
    path = tmp_path / "2330_features.parquet"
    original = table().rename_columns(["date", "symbol", "twpub_value"])
    write(path, original)
    assert _symbol_name_from_path(parts.manifest_path(path)) == "2330"
    assert read_logical_parquet(path).height == 3
    first = _compute_source_hash([parts.manifest_path(path)])
    values = _load_external_feature_arrays(parts.manifest_path(path), date_start=date(2024, 12, 31))
    assert values.feature_names == ["twpub_value"]
    assert _compute_source_hash([parts.manifest_path(path)]) == first
    write(path, original.slice(0, 2))
    assert _compute_source_hash([parts.manifest_path(path)]) != first


@pytest.mark.parametrize("layout", ["file", "year"])
@pytest.mark.parametrize("case", ["append", "year_boundary", "episode", "revision", "delete", "last_missing", "new_missing", "reference", "zero"])
def test_base_yearly_producer_matches_full_canonical_output(tmp_path, case, layout):
    from scripts.build_tw_official_symbol_parquets import _write_symbol
    from test_tw_official_symbol_builder import _incremental_frame
    frame = _incremental_frame()
    if case == "year_boundary":
        frame = frame.with_columns(pl.Series("date", [date(2024, 12, 29), date(2024, 12, 30),
            date(2024, 12, 31), date(2025, 1, 1), date(2025, 1, 2)]))
    if case == "episode":
        frame = frame.with_columns(pl.when(pl.col("date") >= date(2026, 10, 15))
            .then(1).otherwise(0).alias("_lifecycle_episode_id"))
    if case in {"last_missing", "new_missing"}:
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][2 if case == "last_missing" else 4])
            .then(float("nan")).otherwise(pl.col("close")).alias("close"))
    if case == "reference":
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][4])
            .then(99.).otherwise(pl.col("reference_override")).alias("reference_override"))
    if case == "zero":
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][3])
            .then(0.).otherwise(pl.col("open")).alias("open"))
    output, reference = tmp_path / "parts", tmp_path / "full"
    output.mkdir()
    reference.mkdir()
    states = {}
    seed = _write_symbol(output, "2330", frame.head(3), requested_end_date=str(frame["date"][2]),
        dry_run=False, state_sink=states, output_layout=layout)
    assert seed.status == "created", seed.message
    prior = json.loads(json.dumps(states["2330"]))
    if case == "revision":
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][1])
            .then(2.).otherwise(pl.col("signed_change")).alias("signed_change"))
    elif case == "delete":
        frame = frame.filter(pl.col("date") != frame["date"][1])
    result = _write_symbol(output, "2330", frame, requested_end_date=str(frame["date"][-1]),
        dry_run=False, previous_state=prior, state_sink={}, output_layout=layout)
    full = _write_symbol(reference, "2330", frame, requested_end_date=str(frame["date"][-1]), dry_run=False)
    assert result.status == full.status == "created", (result.message, full.message)
    actual = parts.read_table(output / "2330_features.parquet")
    expected = pq.read_table(reference / "2330_features.parquet")
    assert table_content_sha256(actual) == table_content_sha256(expected)
    assert result.return_quarantined_rows == full.return_quarantined_rows
    for name in ("rows", "adjusted_rows", "missing_adjustment_rows", "first_date", "last_date", "source",
                 "fallback_rows", "fallback_adjustment_rows", "normalized_zero_ohlc_rows",
                 "previous_session_next_reference_adjustment_rows", "lifecycle_episode_quarantined_rows",
                 "listing_boundary_quarantined_rows", "unverified_extreme_quarantined_rows"):
        assert getattr(result, name) == getattr(full, name), name


def test_full_panel_all_arrays_and_persistent_rules_match_new_reader(tmp_path):
    from scripts.audit_tw_public_data_layer import _panel_kwargs
    from test_tw_public_audit_panel_projection import _fixture, _assert_panel_bits
    from stockagent.data.panel import build_panel
    stocks, public, config = _fixture(tmp_path)
    external = pl.read_parquet(public).with_columns(pl.lit(None, dtype=pl.Float64).alias("_twpub_delisted"))
    external = pl.concat([external, pl.DataFrame({"date": [date(2023, 12, 29)], "symbol": ["2330"],
        "_twpub_delisted": [1.]})], how="diagonal_relaxed").sort("date", "symbol")
    external.write_parquet(public)
    config.data.feature_include = ["open_raw", "close_raw", "next_session_open_gap_logret", "twpub_keep"]
    config.data.feature_exclude = []
    legacy = build_panel(stocks, **_panel_kwargs(config, public))
    target = tmp_path / "partitioned"
    target.mkdir()
    for path in stocks.glob("*_features.parquet"):
        write(target / path.name, pq.read_table(path))
    new_public = tmp_path / "partition_public.parquet"
    original_public = pq.read_table(public)
    import pyarrow.compute as pc
    order = pc.sort_indices(original_public, sort_keys=[("date", "ascending"), ("symbol", "ascending")])
    write(new_public, original_public.take(order), period="month")
    actual = build_panel(target, **_panel_kwargs(config, new_public))
    _assert_panel_bits(legacy, actual)
    from stockagent.data.panel import _load_external_feature_arrays
    payload = _load_external_feature_arrays(new_public, date_start=date(2024, 1, 1))
    days, values = payload.by_symbol_rules["2330"]
    row = list(days.astype("datetime64[D]")).index(np.datetime64("2023-12-29"))
    assert values[row, payload.rule_names.index("_twpub_delisted")] == 1.


def test_audit_and_stage_receipts_bind_every_partition(tmp_path):
    from scripts import audit_tw_public_data_layer as audit
    from scripts import rebuild_tw_public_data_layer as rebuild
    path = tmp_path / "stocks/2330_features.parquet"
    path.parent.mkdir()
    write(path, table())
    before = rebuild._file_receipt(path)
    assert len(before["members"]) == 2
    assert rebuild._outputs_exist([path])
    signatures = audit._audit_input_signatures(path.parent, tmp_path, tmp_path / "public.parquet")
    assert all(item["path"] in signatures for item in before["members"])
    write(path, table([date(2025, 2, 1)], [42.]), replace_from=date(2025, 2, 1))
    assert not rebuild._receipts_match({"output_receipts": [before]}, [path])


def test_unchanged_partition_stats_are_current_and_csv_report_is_scalar(tmp_path):
    from scripts import build_tw_official_symbol_parquets as builder
    from test_tw_official_symbol_builder import _incremental_frame
    frame, states = _incremental_frame(), {}
    first = builder._write_symbol(tmp_path, "2330", frame,
        requested_end_date="2026-10-16", dry_run=False, state_sink=states, output_layout="year")
    assert first.status == "created", first.message
    original_state = json.dumps(states, sort_keys=True)
    hit = builder._write_symbol(tmp_path, "2330", frame,
        requested_end_date="2026-10-16", dry_run=False, previous_state=states["2330"],
        state_sink={}, output_layout="year")
    assert hit.reused_output
    assert hit.partitioned_write["written_partitions"] == hit.partitioned_write["written_rows"] == 0
    assert hit.partitioned_write["reused_partitions"] == 1
    assert hit.partitioned_write["reused_rows"] == frame.height
    assert json.dumps(states, sort_keys=True) == original_state
    path = tmp_path / "report.csv"
    pl.DataFrame(builder._report_rows([hit])).write_csv(path)
    recorded = json.loads(pl.read_csv(path)["partitioned_write"][0])
    assert recorded == hit.partitioned_write


def test_stale_partitioned_universe_fails_before_any_other_output_is_written(tmp_path):
    from scripts import build_tw_official_symbol_parquets as builder
    from test_tw_official_symbol_builder import _incremental_frame
    path = tmp_path / "old_features.parquet"
    write(path, table())
    before = parts.manifest_path(path).read_bytes()
    with pytest.raises(ValueError, match="explicit retirement"):
        builder._write_symbol_batch(tmp_path, {"2330": _incremental_frame()},
            requested_end_date="2026-10-16", output_layout="year")
    assert not parts.manifest_path(tmp_path / "2330_features.parquet").exists()
    assert parts.manifest_path(path).read_bytes() == before
    legacy = tmp_path / "legacy_features.parquet"
    pq.write_table(table(), legacy)
    with pytest.raises(ValueError, match="explicit retirement"):
        builder._remove_stale_official_symbol_files(tmp_path, {"2330"}, dry_run=False)
    assert legacy.exists()


def test_public_table_audit_and_build_receipt_accept_explicit_manifest(tmp_path, monkeypatch):
    from dataclasses import asdict
    from scripts import audit_tw_public_data_layer as audit
    from stockagent.data.tw_public_features import FEATURE_COLUMNS, RULE_COLUMNS
    original = tmp_path / "legacy.parquet"
    value = table().rename_columns(["date", "symbol", FEATURE_COLUMNS[0]])
    for name in RULE_COLUMNS:
        value = value.append_column(name, pa.array([None] * value.num_rows, type=pa.float64()))
    pq.write_table(value, original)
    logical = tmp_path / "new.parquet"
    write(logical, value, period="month")
    expected = audit.audit_public_feature_table(original, [FEATURE_COLUMNS[0]], "__MARKET__")
    actual = audit.audit_public_feature_table(logical, [FEATURE_COLUMNS[0]], "__MARKET__")
    assert actual[:2] == expected[:2]
    # Paths are presentation only; all semantic findings must agree.
    assert [{k: v for k, v in asdict(item).items() if k != "item"} for item in actual[2]] == [
        {k: v for k, v in asdict(item).items() if k != "item"} for item in expected[2]]
    monkeypatch.setattr(audit, "_source_content_receipts", lambda *_: [])
    monkeypatch.setattr(audit, "_symbol_universe_receipt", lambda *_: {})
    manifest = parts.manifest_path(logical)
    summary = {"feature_columns": list(FEATURE_COLUMNS), "rule_columns": list(RULE_COLUMNS),
        "availability_contract_version": audit.TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION,
        "source_receipts": [], "symbol_universe_receipt": {}, "output_receipt": audit._file_content_receipt(manifest)}
    logical.with_suffix(".summary.json").write_text(json.dumps(summary))
    for path in (logical, manifest):
        result, findings = audit.audit_feature_build_receipt(path, tmp_path, tmp_path)
        assert result["valid"] and not findings
    member = logical.parent / json.loads(manifest.read_bytes())["members"][0]["path"]
    member.chmod(0o644)
    with pytest.raises(ValueError, match="content proof"):
        audit.audit_feature_build_receipt(manifest, tmp_path, tmp_path)


def test_stock_history_resolves_manifest_and_cached_prices_still_verify_members(tmp_path):
    from stockagent.live import stock_history as history
    legacy, target = tmp_path / "legacy", tmp_path / "partitioned"
    legacy.mkdir()
    target.mkdir()
    value = table(values=[-0., 2., 3.]).rename_columns(["date", "symbol", "close"])
    value = value.append_column("open", pa.array([1., 2., 3.]))
    pq.write_table(value, legacy / "2330_features.parquet")
    write(target / "2330_features.parquet", value)
    assert history._price_history_path(target, "") is None
    before, symbol, _ = history._read_price_symbol_series(legacy, "2330.TW")
    after, actual_symbol, path = history._read_price_symbol_series(target, "2330.TW")
    assert symbol == actual_symbol == "2330"
    same(before.to_arrow(), after.to_arrow())
    expected, _ = history._read_price_history_maps(legacy, "2330")
    actual, _ = history._read_price_history_maps(target, "2330")
    assert actual == expected
    assert history._read_price_history_maps(target, "2330")[0] == expected
    member = path.parent / json.loads(path.read_bytes())["members"][0]["path"]
    member.chmod(0o644)
    with pytest.raises(ValueError, match="content proof"):
        history._read_price_history_maps(target, "2330")


def test_stock_history_cache_rejects_same_size_mtime_replacement(tmp_path):
    from stockagent.live import stock_history as history
    path = tmp_path / "2330_features.parquet"
    value = table(values=[1., 2., 3.]).rename_columns(["date", "symbol", "close"])
    pq.write_table(value, path, compression=None)
    before = path.stat()
    first, _ = history._read_price_history_maps(tmp_path, "2330", price_columns=("close",))
    pq.write_table(value.set_column(2, "close", pa.array([4., 2., 3.])), path, compression=None)
    assert path.stat().st_size == before.st_size
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    second, _ = history._read_price_history_maps(tmp_path, "2330", price_columns=("close",))
    assert first["close"]["2024-01-01"] == 1.
    assert second["close"]["2024-01-01"] == 4.


def test_base_benchmark_isolates_suffix_from_batch_sync_and_proof_cost(tmp_path, monkeypatch):
    from scripts import benchmark_dashboard_daily_incremental as benchmark
    original = benchmark._write_symbol_batch
    observed = {}
    def record(folder, groups, **kwargs):
        observed[folder.name] = (kwargs["workers"], kwargs["batch_directory_fsync"],
                                kwargs["state_sink"] is not None)
        return original(folder, groups, **kwargs)
    monkeypatch.setattr(benchmark, "_write_symbol_batch", record)
    result = benchmark.official_symbols(tmp_path, 4, 2, workers=1)
    assert observed["symbol_full"] == (1, False, False)
    assert observed["symbol_full_batched"] == observed["symbol_incremental"] == (1, True, True)
    assert result["full_batched_directory_commits"] == result["incremental_directory_commits"] == 1
    assert result["full_batched_with_same_proofs"]["wall_ms"] >= 0
    assert result["all_columns_nulls_and_values_equal"]
    assert result["partitioned"]["all_columns_raw_bits_nulls_and_order_equal"]
