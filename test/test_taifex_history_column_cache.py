from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from datetime import datetime, timezone
import hashlib
import io
import json
import os

import polars as pl
import pytest

from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_volatility_dashboard as dashboard
from test_taifex_volatility_dashboard import _fixture


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "_VERIFIED_PAYLOADS", OrderedDict())
    root = tmp_path / "cache"
    root.mkdir()
    monkeypatch.setenv(cache.CACHE_ENV, str(root))
    monkeypatch.setenv(cache.ENABLE_ENV, "1")
    monkeypatch.setattr(cache, "MIN_SOURCE_BYTES", 1)
    path = tmp_path / "marks.jsonl"
    schema = {"ts": pl.Int64, "value": pl.Float64, "name": pl.String, "flag": pl.Boolean}
    path.write_text(''.join(json.dumps(row) + "\n" for row in (
        {"ts": 1, "value": -0.0, "name": "台股", "flag": True},
        {"ts": 2, "value": 1e-120, "flag": False},
        {"ts": 3, "name": "future", "flag": None},
    )))
    return path, schema, root


def read(source):
    path, schema, _ = source
    with cache.history_scan(path, schema=schema) as frame:
        return frame.collect()


def assert_exact(actual, expected):
    assert actual.schema == expected.schema and actual.rows() == expected.rows()
    assert actual["value"].to_numpy().tobytes() == expected["value"].to_numpy().tobytes()


def test_full_raw_projection_and_warm_cache_preserve_values_and_source(source):
    path, schema, root = source
    original = path.read_bytes()
    expected = pl.read_ndjson(path, schema=schema)
    for _ in range(2):
        assert_exact(read(source), expected)
    assert path.read_bytes() == original
    directory = root / "history-columns-v1"
    assert directory.stat().st_mode & 0o777 == 0o700
    assert all(item.stat().st_mode & 0o777 == 0o600 for item in directory.iterdir())
    receipt = json.loads(next(directory.glob("*.json")).read_bytes())
    assert receipt["rows"] == 3 and receipt["source_sha256"] == hashlib.sha256(original).hexdigest()


def test_append_verifies_prefix_and_only_parses_real_tail(source, monkeypatch):
    path, schema, _ = source
    read(source)
    tail = b'{"ts":4,"value":123.45678912345678,"name":"TX","flag":true}\n'
    with path.open("ab") as stream:
        stream.write(tail)
    scans = []
    raw_scan = pl.scan_ndjson

    def scan(value, **kwargs):
        scans.append(value.getvalue() if isinstance(value, io.BytesIO) else str(value))
        return raw_scan(value, **kwargs)

    monkeypatch.setattr(cache.pl, "scan_ndjson", scan)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert scans == [tail]


@pytest.mark.parametrize("append", [False, True])
def test_same_inode_prefix_repair_cannot_reuse_old_values(source, append):
    path, schema, _ = source
    read(source)
    original = path.stat()
    repaired = path.read_bytes().replace(b'"ts": 1', b'"ts": 9')
    path.write_bytes(repaired + (b'{"ts":10,"value":9.0}\n' if append else b""))
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))


@pytest.mark.parametrize("damage", ["payload", "source_hash", "receipt", "symlink", "fifo"])
def test_invalid_cache_rebuilds_from_source_without_touching_other_files(source, damage):
    path, schema, root = source
    read(source)
    directory = root / "history-columns-v1"
    payload = next(directory.glob("*.parquet"))
    receipt = next(directory.glob("*.json"))
    victim = root / "never-write.parquet"
    victim.write_bytes(b"preserve external bytes")
    if damage == "payload":
        payload.write_bytes(b"corrupt")
    elif damage == "source_hash":
        value = json.loads(receipt.read_text())
        value["source_sha256"] = "0" * 64
        receipt.write_text(json.dumps(value))
    elif damage == "symlink":
        payload.unlink()
        payload.symlink_to(victim)
    elif damage == "fifo":
        payload.unlink()
        os.mkfifo(payload)
    else:
        receipt.write_text('{"contract":"wrong"}')
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert victim.read_bytes() == b"preserve external bytes"


def test_concurrent_readers_share_one_complete_column_generation(source, monkeypatch):
    builds = []
    sink = pl.LazyFrame.sink_parquet

    def counted(self, *args, **kwargs):
        builds.append(True)
        return sink(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", counted)
    with ThreadPoolExecutor(max_workers=4) as pool:
        frames = list(pool.map(lambda _: read(source), range(4)))
    assert len(builds) == 1
    for frame in frames:
        assert_exact(frame, frames[0])


def test_disabled_or_small_sources_keep_canonical_raw_reader(source, monkeypatch):
    _, _, root = source
    monkeypatch.setenv(cache.ENABLE_ENV, "0")
    assert read(source).height == 3 and not list(root.iterdir())
    monkeypatch.setenv(cache.ENABLE_ENV, "1")
    monkeypatch.setattr(cache, "MIN_SOURCE_BYTES", 1024)
    assert read(source).height == 3
    assert not list(root.rglob("*.parquet"))


def test_cache_racing_source_append_is_not_committed(source, monkeypatch):
    path, schema, root = source
    sink = pl.LazyFrame.sink_parquet

    def append_after_sink(self, *args, **kwargs):
        result = sink(self, *args, **kwargs)
        with path.open("ab") as stream:
            stream.write(b'{"ts":4,"value":4.0}\n')
        return result

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", append_after_sink)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert not list(root.rglob("*.json")) and not list(root.rglob("*.parquet"))


def test_one_normal_append_race_reobserves_and_builds_only_verified_generation(source, monkeypatch):
    path, schema, root = source
    read(source)
    raw_digest = cache._source_digests
    attempts = []
    # Trigger a new source version, then one additional real writer append
    # during its prefix proof. The retry must include both actual rows.
    with path.open("ab") as stream:
        stream.write(b'{"ts":4,"value":4.0}\n')

    def race_once(*args):
        result = raw_digest(*args)
        attempts.append(True)
        if len(attempts) == 1:
            with path.open("ab") as stream:
                stream.write(b'{"ts":5,"value":5.0}\n')
        return result

    monkeypatch.setattr(cache, "_source_digests", race_once)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    receipt = json.loads(next(root.rglob("*.json")).read_bytes())
    assert len(attempts) == 2 and receipt["rows"] == 5
    assert receipt["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_all_range_curves_match_full_canonical_raw_workflow(tmp_path, monkeypatch):
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


def test_invalid_source_keeps_raw_parser_failure_instead_of_empty_success(source):
    path, schema, _ = source
    read(source)
    path.write_bytes(b'{"ts":4,"value":"not-a-number"}\n')
    with pytest.raises(pl.exceptions.PolarsError):
        read(source)


def _count_proof_reads(monkeypatch):
    """Count successful complete source/payload proofs, not parser reads."""
    calls = []
    prefix, source_digests = cache._prefix_digest, cache._source_digests

    def record_prefix(fd, size):
        value = prefix(fd, size)
        calls.append((os.readlink(f"/proc/self/fd/{fd}"), "verify", size))
        return value

    def record_source(fd, size, previous):
        value = source_digests(fd, size, previous)
        calls.append((os.readlink(f"/proc/self/fd/{fd}"), "build", size))
        return value

    monkeypatch.setattr(cache, "_prefix_digest", record_prefix)
    monkeypatch.setattr(cache, "_source_digests", record_source)
    return calls


@pytest.mark.parametrize("update", ["cold", "append", "metadata"])
def test_completed_source_proof_is_reused_without_skipping_new_payload_proof(source, monkeypatch, update):
    path, schema, _ = source
    if update != "cold":
        read(source)
        if update == "append":
            with path.open("ab") as stream:
                stream.write(b'{"ts":4,"value":4.0}\n')
        else:
            before = path.stat()
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1))
    calls = _count_proof_reads(monkeypatch)
    expected = pl.read_ndjson(path, schema=schema)
    assert_exact(read(source), expected)
    first_count = len(calls)
    for _ in range(3):
        assert_exact(read(source), expected)
    source_calls = [row for row in calls if row[0] == str(path)]
    assert source_calls == [(str(path), "build", path.stat().st_size)]
    # A new payload still receives an independent SHA proof after publication.
    if update != "metadata":
        assert any(row[0].endswith(".parquet") for row in calls[first_count:])


def test_restart_loses_memory_proofs_and_verifies_disk_receipt(source, monkeypatch):
    path, schema, _ = source
    read(source)
    monkeypatch.setattr(cache, "_VERIFIED_PAYLOADS", OrderedDict())
    calls = _count_proof_reads(monkeypatch)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert [row for row in calls if row[0] == str(path)] == [
        (str(path), "verify", path.stat().st_size)]
    assert any(row[0].endswith(".parquet") for row in calls)


def test_inode_replacement_cannot_use_seeded_source_proof(source, monkeypatch):
    path, schema, _ = source
    read(source)
    old = path.stat()
    replacement = path.with_suffix(".replacement")
    replacement.write_bytes(path.read_bytes().replace(b'"ts": 1', b'"ts": 8'))
    os.utime(replacement, ns=(old.st_atime_ns, old.st_mtime_ns))
    replacement.replace(path)
    calls = _count_proof_reads(monkeypatch)
    assert_exact(read(source), pl.read_ndjson(path, schema=schema))
    assert path.stat().st_ino != old.st_ino
    assert any(row[0] == str(path) and row[1] == "build" for row in calls)


def test_failed_publication_does_not_seed_a_source_receipt(source, monkeypatch):
    path, _, root = source

    def fail(*args, **kwargs):
        raise OSError("injected receipt publication failure")

    monkeypatch.setattr(cache, "_publish_receipt", fail)
    assert read(source).height == 3  # Canonical raw fallback, not empty success.
    assert not list(root.rglob("*.json"))
    assert not any(key[0] == str(path) for key in cache._VERIFIED_PAYLOADS)


def test_proof_memo_is_bounded_lru_and_retains_false_results(monkeypatch):
    monkeypatch.setattr(cache, "_VERIFIED_PAYLOADS", OrderedDict())
    generation = (1, 2, 3, 4, 5)
    for index in range(cache._VERIFIED_PAYLOAD_LIMIT):
        cache._remember_verified_payload(str(index), generation, "a" * 64, False)
    assert cache._verified_payload("0", generation, "a" * 64) is False
    cache._remember_verified_payload("new", generation, "a" * 64)
    assert len(cache._VERIFIED_PAYLOADS) == cache._VERIFIED_PAYLOAD_LIMIT
    assert ("0", generation, "a" * 64) in cache._VERIFIED_PAYLOADS
    assert ("1", generation, "a" * 64) not in cache._VERIFIED_PAYLOADS
