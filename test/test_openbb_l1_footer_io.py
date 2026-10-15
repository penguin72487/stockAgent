from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor as RealPool
import inspect
from pathlib import Path
from threading import Lock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import compact_openbb_l1 as compactor
from scripts import openbb_l1_footer_io as io
from scripts.benchmark_openbb_l1_footer_reader import prefetch_source
from scripts.openbb_l1_view_schema import schema_identities


@pytest.mark.parametrize("kind", ["plain", "nested", "int96", "timezone", "geo", "field_ids", "uuid", "empty"])
def test_metadata_only_preserves_all_actual_footer_properties(tmp_path, kind):
    table, options = pa.table({"value": [1, None]}), {}
    if kind == "nested":
        table = pa.table({"value": pa.array([None, [[1, None]]], pa.list_(pa.list_(pa.int64())))})
    elif kind == "int96":
        table = pa.table({"value": pa.array([1, None], pa.timestamp("ns"))})
        options["use_deprecated_int96_timestamps"] = True
    elif kind == "timezone":
        table = pa.table({"value": pa.array([1, None], pa.timestamp("ns", tz="Asia/Taipei"))})
    elif kind == "geo":
        table = table.replace_schema_metadata({b"geo": b"original", b"unrelated": b"\x00\xff"})
    elif kind == "field_ids":
        field = pa.field("group", pa.struct([("leaf", pa.int64())]), metadata={b"PARQUET:field_id": b"7"})
        table = pa.Table.from_arrays([pa.array([None], field.type)], schema=pa.schema([field]))
    elif kind == "uuid":
        table = pa.table({"value": pa.array([None], pa.uuid())})
    elif kind == "empty":
        table = pa.table({"value": pa.array([], pa.int64())})
    path = tmp_path / f"{kind}.parquet"
    pq.write_table(table, path, **options)
    opener = io.FooterOpener()
    assert opener.optimized is True
    with pq.ParquetFile(path) as reference, opener(path) as candidate:
        assert reference.metadata.equals(candidate.metadata)
        assert reference.schema.equals(candidate.schema)
        assert reference.schema_arrow.equals(candidate.schema_arrow, check_metadata=True)
        assert schema_identities(reference) == schema_identities(candidate)
        for name in ("read", "read_row_group", "read_row_groups", "iter_batches", "scan_contents"):
            with pytest.raises(RuntimeError, match="metadata-only"):
                getattr(candidate, name)()
    assert opener.snapshot()["actual_footer_opens"] == opener.snapshot()["metadata_only_opens"] == 1


def test_unknown_version_or_changed_hook_uses_canonical_reader(tmp_path, monkeypatch):
    path = tmp_path / "source.parquet"
    pq.write_table(pa.table({"x": [1]}), path)
    monkeypatch.setattr(pa, "__version__", "unreviewed")
    opener = io.FooterOpener()
    assert opener.optimized is False
    with opener(path) as reader:
        assert type(reader) is pq.ParquetFile
        assert reader.read().column("x").to_pylist() == [1]
    monkeypatch.setattr(pa, "__version__", "25.0.0")
    monkeypatch.setattr(pq.ParquetFile, "_build_nested_paths", lambda _self: {})
    assert io.FooterOpener().optimized is False


@pytest.mark.parametrize("kind", ["missing", "directory", "malformed"])
def test_original_reader_error_is_preserved(tmp_path, kind):
    path = tmp_path / kind
    if kind == "directory":
        path.mkdir()
    elif kind == "malformed":
        path.write_bytes(b"not parquet")
    with pytest.raises(Exception) as reference:
        pq.ParquetFile(path)
    opener = io.FooterOpener()
    with pytest.raises(type(reference.value)) as candidate:
        opener(path)
    assert str(candidate.value) == str(reference.value)
    assert opener.snapshot()["actual_footer_opens"] == 2
    assert opener.snapshot()["canonical_retries"] == 1


def test_memory_failure_never_retries():
    def exhausted(*_args, **_kwargs):
        raise MemoryError("original resource exhaustion")
    opener = io.FooterOpener(reader=exhausted)
    with pytest.raises(MemoryError, match="original resource"):
        opener("source")
    assert opener.snapshot()["actual_footer_opens"] == 1
    assert opener.snapshot()["canonical_retries"] == 0


@pytest.mark.parametrize("exception", [None, MemoryError("consumer memory"), ValueError("consumer error")])
def test_prefetch_is_ordered_bounded_and_releases_all_owned_readers(monkeypatch, exception):
    counters = {"submitted": 0, "retired": 0, "max_pending": 0, "opened": 0, "closed": 0}
    lock = Lock()
    class Reader:
        def __init__(self):
            with lock:
                counters["opened"] += 1
        def close(self):
            with lock:
                counters["closed"] += 1
    class CountedPool(RealPool):
        def submit(self, *args, **kwargs):
            counters["submitted"] += 1
            counters["max_pending"] = max(counters["max_pending"], counters["submitted"] - counters["retired"])
            future = super().submit(*args, **kwargs)
            return future
    monkeypatch.setattr(io, "ThreadPoolExecutor", CountedPool)
    rows = [{"id": index, "output_path": f"source-{index}", "endpoint": "ordinary"} for index in range(100)]
    observed = []
    def consume():
        with io.prefetched_footer_rows(rows, opener=lambda _path: Reader(), signature=lambda _path: None,
                                      grouped_endpoint="grouped", collect_proofs=True) as pending:
            for item in pending:
                observed.append(item.row["id"])
                counters["retired"] += 1
                assert item.before is None and item.error is None and item.attempted is True
                if exception:
                    raise exception
    if exception:
        with pytest.raises(type(exception), match="consumer"):
            consume()
        assert counters["submitted"] <= 4
    else:
        consume()
        assert observed == list(range(100))
    assert counters["max_pending"] <= 4
    assert counters["opened"] == counters["closed"]
    assert not any(t.name.startswith("openbb-footer") for t in __import__("threading").enumerate())


def test_prefetch_keeps_signature_errors_and_memory_failures_in_source_order():
    rows = [{"id": index, "output_path": f"source-{index}", "endpoint": "grouped"} for index in range(3)]
    def signature(path):
        if str(path).endswith("1"):
            raise FileNotFoundError("original missing")
        return (1, 2, 3, 4, 5)
    def opener(path):
        if path.endswith("2"):
            raise MemoryError("original allocation failure")
        class Reader:
            def close(self):
                pass
        return Reader()
    with io.prefetched_footer_rows(rows, opener=opener, signature=signature,
                                  grouped_endpoint="grouped", collect_proofs=True) as pending:
        first, missing, exhausted = list(pending)
    assert first.before == (1, 2, 3, 4, 5) and first.attempted
    assert isinstance(missing.error, FileNotFoundError) and not missing.attempted
    assert isinstance(exhausted.error, MemoryError) and exhausted.attempted


@pytest.mark.parametrize("failure_index", [0, 2])
def test_failing_close_still_joins_pool_and_releases_other_readers(failure_index):
    readers, lock = [], Lock()
    class Reader:
        closed = False
        def __init__(self, index):
            self.index = index
            with lock:
                readers.append(self)
        def close(self):
            self.closed = True
            if self.index == failure_index:
                raise RuntimeError("injected close failure")
    rows = [{"output_path": str(index), "endpoint": "ordinary"} for index in range(100)]
    with pytest.raises(RuntimeError, match="close failure"):
        with io.prefetched_footer_rows(rows, opener=lambda path: Reader(int(path)),
                                      signature=None, grouped_endpoint="grouped", collect_proofs=False) as pending:
            for _item in pending:
                pass
    assert readers and all(reader.closed for reader in readers)
    assert not any(t.name.startswith("openbb-footer") for t in __import__("threading").enumerate())


@pytest.mark.parametrize("workers,pending", [(0, 4), (3, 4), (True, 4), (2, 1), (2, 5), (2, True)])
def test_unbounded_or_invalid_queue_refused(workers, pending):
    with pytest.raises(ValueError), io.prefetched_footer_rows([], opener=None, signature=None,
                                                            grouped_endpoint="", collect_proofs=False,
                                                            workers=workers, max_pending=pending):
        pytest.fail("invalid pool started")


def test_prefetch_prototype_preserves_source_sql_and_stale_state_application():
    source = inspect.getsource(compactor._mark_stale_segments)
    candidate = prefetch_source(source)
    compile(candidate, "full_audit_prototype", "exec")
    start = source.index("    if metadata_audit is not None:")
    assert candidate[candidate.index("    if metadata_audit is not None:"):] == source[start:]
    prefix = source.index("    for row in connection.execute(\n", source.index("    footer_open_attempts = 0"))
    assert candidate[:prefix] == source[:prefix]
