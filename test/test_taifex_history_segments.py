from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os

import polars as pl
import pytest

from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_history_segments as segments
from stockagent.live import taifex_volatility_dashboard as dashboard
from test_taifex_history_column_cache import assert_exact, read, source as source
from test_taifex_volatility_dashboard import _fixture


@pytest.fixture(autouse=True)
def segmented_layout(monkeypatch):
    monkeypatch.setenv(segments.LAYOUT_ENV, "base_delta_v2")


def manifest(source):
    return next(source[2].glob("history-columns-v1/*.segments-v2/receipt.json"))


def append(path, ts):
    with path.open("ab") as stream:
        stream.write(json.dumps({"ts": ts, "value": -0.25 * ts, "name": "TX"}).encode() + b"\n")


def test_append_reuses_base_inode_and_bounds_delta_objects(source):
    path, schema, _ = source
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    receipt = manifest(source)
    original = json.loads(receipt.read_bytes())["parts"][0]
    base = receipt.parent / original["name"]
    identity = cache._generation(base.stat())
    original_bytes = base.read_bytes()
    for ts in (4, 5, 6):
        append(path, ts)
        assert_exact(read(source), pl.read_ndjson(path, schema=schema))
        value = json.loads(receipt.read_bytes())
        assert len(value["parts"]) == 2 and value["parts"][0] == original
        assert len(list(receipt.parent.glob("part-*.parquet"))) == 2
        assert cache._generation(base.stat()) == identity and base.read_bytes() == original_bytes
        assert value["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_tail_threshold_compacts_once_then_reuses_new_base(source, monkeypatch):
    path, schema, _ = source
    read(source)
    monkeypatch.setattr(segments, "MAX_TAIL_SOURCE_BYTES", 1)
    append(path, 4)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    receipt = manifest(source)
    assert len(json.loads(receipt.read_bytes())["parts"]) == 1
    assert len(list(receipt.parent.glob("*.parquet"))) == 1
    monkeypatch.setattr(segments, "MAX_TAIL_SOURCE_BYTES", 1024)
    append(path, 5)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert len(json.loads(receipt.read_bytes())["parts"]) == 2


def test_v1_migration_reuses_verified_payload_hardlink(source, monkeypatch):
    path, schema, root = source
    monkeypatch.setenv(segments.LAYOUT_ENV, "v1")
    read(source)
    payload = next(root.glob("history-columns-v1/*.parquet"))
    old_bytes, old_stat = payload.read_bytes(), payload.stat()
    old_receipt = next(root.glob("history-columns-v1/*.json")).read_bytes()
    monkeypatch.setenv(segments.LAYOUT_ENV, "base_delta_v2")
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    value = json.loads(manifest(source).read_bytes())
    base = manifest(source).parent / value["parts"][0]["name"]
    assert (base.stat().st_dev, base.stat().st_ino) == (old_stat.st_dev, old_stat.st_ino)
    assert payload.read_bytes() == old_bytes
    assert next(root.glob("history-columns-v1/*.json")).read_bytes() == old_receipt
    append(path, 4)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert payload.read_bytes() == old_bytes


def test_invalid_existing_manifest_is_repaired_during_legacy_adoption(source, monkeypatch):
    monkeypatch.setenv(segments.LAYOUT_ENV, "v1")
    read(source)
    monkeypatch.setenv(segments.LAYOUT_ENV, "base_delta_v2")
    read(source)
    receipt = manifest(source)
    receipt.write_bytes(b"invalid manifest")
    read(source)
    assert json.loads(receipt.read_bytes())["contract"] == segments.CONTRACT


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_legacy_receipt_is_bounded_regular_file_only(source, monkeypatch, kind):
    _, schema, root = source
    monkeypatch.setenv(segments.LAYOUT_ENV, "v1")
    read(source)
    receipt = next(root.glob("history-columns-v1/*.json"))
    receipt.unlink()
    victim = root / "private"
    victim.write_text('{"preserve":true}')
    receipt.symlink_to(victim) if kind == "symlink" else os.mkfifo(receipt)
    assert cache._receipt(receipt, schema) == {}
    assert victim.read_text() == '{"preserve":true}'


@pytest.mark.parametrize("mode", ["repair", "repair_append", "truncate", "replace"])
def test_changed_source_never_reuses_incorrect_historical_values(source, mode):
    path, schema, _ = source
    read(source)
    original = path.stat()
    if mode.startswith("repair"):
        path.write_bytes(path.read_bytes().replace(b'"ts": 1', b'"ts": 9'))
        os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
        if mode == "repair_append":
            append(path, 10)
    elif mode == "truncate":
        path.write_bytes(b'{"ts":99,"value":-100.0}\n')
    else:
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(b'{"ts":100,"value":-0.0}\n')
        os.replace(replacement, path)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))


@pytest.mark.parametrize("damage", ["payload", "symlink", "fifo", "receipt", "range", "traversal"])
def test_invalid_disposable_cache_recovers_without_following_other_paths(source, damage):
    path, schema, root = source
    read(source)
    receipt = manifest(source)
    value = json.loads(receipt.read_bytes())
    part = receipt.parent / value["parts"][0]["name"]
    victim = root / "never-change"
    victim.write_bytes(b"private original")
    if damage == "payload":
        part.write_bytes(b"corrupt")
    elif damage in {"symlink", "fifo"}:
        part.unlink()
        part.symlink_to(victim) if damage == "symlink" else os.mkfifo(part)
    elif damage == "receipt":
        receipt.write_bytes(b"{not-json}")
    else:
        value["parts"][0]["start" if damage == "range" else "name"] = 9 if damage == "range" else "../../never-change"
        receipt.write_text(json.dumps(value))
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert victim.read_bytes() == b"private original"


def test_failed_writer_race_removes_only_this_attempts_uncommitted_cache(source, monkeypatch):
    path, schema, root = source
    sink = pl.LazyFrame.sink_parquet

    def race(self, *args, **kwargs):
        result = sink(self, *args, **kwargs)
        append(path, 20)
        return result

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", race)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert not list(root.rglob("receipt.json"))
    assert not list(root.rglob("part-*.parquet"))


def test_one_source_race_retries_and_publishes_complete_generation(source, monkeypatch):
    path, schema, _ = source
    read(source)
    append(path, 4)
    original, calls = cache._source_digests, []

    def race(*args):
        result = original(*args)
        calls.append(1)
        if len(calls) == 1:
            append(path, 5)
        return result

    monkeypatch.setattr(cache, "_source_digests", race)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert len(calls) == 2 and json.loads(manifest(source).read_bytes())["rows"] == 5


def test_unchanged_process_proofs_are_reused_and_restart_reverifies(source, monkeypatch):
    path, schema, _ = source
    read(source)
    proofs = []
    original = cache._prefix_digest

    def count(fd, size):
        proofs.append(os.readlink(f"/proc/self/fd/{fd}"))
        return original(fd, size)

    monkeypatch.setattr(cache, "_prefix_digest", count)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert proofs == []
    cache._VERIFIED_PAYLOADS.clear()
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert len(proofs) == 2 and str(path) in proofs


def test_metadata_only_change_keeps_payloads_unchanged(source):
    path, schema, _ = source
    read(source)
    receipt = manifest(source)
    before = {p.name: p.read_bytes() for p in receipt.parent.glob("*.parquet")}
    info = path.stat()
    os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 100000))
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert {p.name: p.read_bytes() for p in receipt.parent.glob("*.parquet")} == before


def test_single_flight_readers_and_no_fd_leak(source, monkeypatch):
    calls, sink = [], pl.LazyFrame.sink_parquet

    def counted(self, *args, **kwargs):
        calls.append(1)
        return sink(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", counted)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: read(source), range(4)))
    assert len(calls) == 1
    for frame in results:
        assert_exact(frame, results[0])
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(10):
        read(source)
    assert len(os.listdir("/proc/self/fd")) == before


def test_receipt_publication_failure_falls_back_without_fd_leak(source, monkeypatch):
    path, schema, _ = source

    def fail(*args):
        raise OSError("injected receipt fsync failure")

    monkeypatch.setattr(cache, "_publish_receipt", fail)
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(3):
        assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert len(os.listdir("/proc/self/fd")) == before


def test_compaction_never_deletes_unregistered_files(source, monkeypatch):
    path, schema, _ = source
    read(source)
    unrelated = manifest(source).parent / "unrelated.parquet"
    unrelated.write_bytes(b"not owned by projection manifest")
    monkeypatch.setattr(segments, "MAX_TAIL_SOURCE_BYTES", 1)
    append(path, 4)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert unrelated.read_bytes() == b"not owned by projection manifest"


def test_source_parser_failure_is_not_successful_empty(source):
    path, _, _ = source
    read(source)
    path.write_bytes(b'{"ts":4,"value":"not-a-number"}\n')
    with pytest.raises(pl.exceptions.PolarsError):
        read(source)


def test_complete_seven_range_outputs_match_canonical_reader(tmp_path, monkeypatch):
    state_dir, _ = _fixture(tmp_path)
    root = tmp_path / "cache"
    root.mkdir()
    monkeypatch.setenv(cache.CACHE_ENV, str(root))
    monkeypatch.setattr(cache, "MIN_SOURCE_BYTES", 1)
    now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
    for range_key in dashboard.HISTORY_RANGE_SECONDS:
        monkeypatch.setenv(cache.ENABLE_ENV, "0")
        control = dashboard.build_dashboard_history_snapshot(state_dir=state_dir, now=now, range_key=range_key)
        monkeypatch.setenv(cache.ENABLE_ENV, "1")
        result = dashboard.build_dashboard_history_snapshot(state_dir=state_dir, now=now, range_key=range_key)
        assert result == control
