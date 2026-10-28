"""Lossless journal representation, verified ancestry and fail-closed recovery."""
from datetime import timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import shutil

import pytest

from stockagent.live import paper_journal as journal
from stockagent.live import tw_day_trade_simulation as simulation
from stockagent.live import tw_day_trade_dashboard as dashboard
from stockagent.live.tw_day_trade_service_sync import (
    PaperReadChanged, committed_paper_read_guard, paper_journal_generations,
)
from test_paper_mode_snapshot import CLOCK, state


def seed(root, raw=b'{"session_date":"2026-10-06","quantity":1000}\n', name="signals.jsonl"):
    path = root / name
    path.write_bytes(raw)
    pointer = journal.publish_snapshot(path, operation="seed")
    return path, pointer


@pytest.mark.parametrize("raw", [b"", b"\r\n", b'{"value":-0.0}\r\n[1,2]\nnull', "台積電\n".encode()])
def test_lossless_seekable_bytes_and_text(tmp_path, raw):
    path, pointer = seed(tmp_path, raw)
    with journal.open_paper_journal(path) as stream:
        assert stream.read() == raw
        assert journal.snapshot_of(stream).head == pointer["head"]
        assert os.fstat(stream.fileno()).st_size == len(raw)
        stream.seek(0)
        assert stream.read(3) == raw[:3]
        stream.seek(0, io.SEEK_END)
        assert stream.read() == b""
        if raw:
            stream.seek(-1, io.SEEK_END)
            assert stream.read() == raw[-1:]
        with pytest.raises(ValueError):
            stream.seek(-1)
    with journal.open_paper_journal(path, "r", encoding="utf-8", newline="") as stream:
        assert stream.read() == raw.decode()
        assert journal.snapshot_of(stream).head == pointer["head"]
    assert path.read_bytes() == raw
    assert list(tmp_path.rglob(".pending-*")) == []


def test_incompressible_large_chunks_and_cross_chunk_seeks(tmp_path):
    raw = os.urandom(journal.CHUNK_BYTES + 1024)
    path, _ = seed(tmp_path, raw)
    view = journal.JournalSnapshot(path)
    assert len(view.chunks) == 2
    assert view.chunks[0]["size_bytes"] > journal.MAX_HEAD_BYTES
    with journal.open_paper_journal(path) as stream:
        stream.seek(journal.CHUNK_BYTES - 19)
        assert stream.read(75) == raw[journal.CHUNK_BYTES - 19:journal.CHUNK_BYTES + 56]
        stream.seek(0)
        assert hashlib.file_digest(stream, "sha256").digest() == hashlib.sha256(raw).digest()


def test_file_only_projection_bridge_collects_exact_managed_columns(tmp_path):
    import polars as pl
    from scripts.deploy_tw_overnight_history import _selected_frame
    raw = (b'{"session_date":"2026-10-06","market":"one","symbol":"2330","shares":1000}\n'
           b'{"session_date":"2026-10-07","market":"one","symbol":"0050","shares":2000}\n')
    path, _ = seed(tmp_path, raw)
    expected = pl.read_ndjson(path)
    with journal.verified_journal_projection(path) as accepted:
        assert accepted != path and accepted.read_bytes() == raw
        assert pl.scan_ndjson(accepted).collect().equals(expected)
    assert _selected_frame(path, tuple(expected.columns)).equals(expected)
    assert path.read_bytes() == raw


def test_file_only_projection_bridge_does_not_enroll_legacy(tmp_path, monkeypatch):
    path = tmp_path / "signals.jsonl"
    path.write_bytes(b'{"quantity":1000}\n')
    def forbidden(*args, **kwargs):
        raise AssertionError("legacy input must not construct an immutable view")
    monkeypatch.setattr(journal, "JournalSnapshot", forbidden)
    with journal.verified_journal_projection(path) as accepted:
        assert accepted == path
    assert not (tmp_path / journal.HEADS).exists()


@pytest.mark.parametrize("controls", [("head",), ("marker",), ("head", "marker")])
def test_broken_control_symlinks_never_downgrade_to_legacy(tmp_path, controls):
    path, _ = seed(tmp_path)
    head = journal._head_path(path)
    marker = head.with_suffix(".enrolled")
    head.unlink()
    marker.unlink()
    for name in controls:
        (head if name == "head" else marker).symlink_to("missing-control-target")
    assert journal.managed(path)
    before = path.read_bytes()
    with pytest.raises(OSError):
        with journal.open_paper_journal(path) as stream:
            stream.read()
    with pytest.raises(OSError):
        with journal.journal_write_transaction(path, operation="append"):
            pytest.fail("damaged enrollment authorized a legacy write")
    assert path.read_bytes() == before


def test_file_only_projection_bridge_refuses_unverified_raw_bytes(tmp_path):
    path, pointer = seed(tmp_path, b'{"quantity":1000}\n')
    path.write_bytes(b'{"quantity":2000}\n')
    # Even a forged metadata binding cannot make the changed raw values the
    # accepted immutable history consumed by a file-only library.
    pointer["source_generation"] = list(journal._generation(path.stat()))
    journal.atomic_write_json(journal._head_path(path), pointer, durable=True)
    with pytest.raises(ValueError, match="differs from immutable"):
        with journal.verified_journal_projection(path):
            pytest.fail("unverified values reached the parser")


@pytest.mark.parametrize("change", ["projection", "head", "object"])
def test_file_only_projection_bridge_fences_changes_and_corruption(tmp_path, change):
    path, _ = seed(tmp_path)
    if change == "object":
        chunk = journal.JournalSnapshot(path).chunks[0]
        target = tmp_path / journal.DIRECTORY / (chunk["sha256"] + ".zst")
        target.chmod(0o600)
        target.write_bytes(b"x" * target.stat().st_size)
        with pytest.raises(ValueError, match="hash"):
            with journal.verified_journal_projection(path):
                pytest.fail("corrupt object reached the parser")
        return
    with pytest.raises(OSError, match="changed"):
        with journal.verified_journal_projection(path) as accepted:
            assert accepted.read_bytes() == path.read_bytes()
            if change == "projection":
                with path.open("ab") as stream:
                    stream.write(b'{"quantity":2000}\n')
            else:
                journal.publish_snapshot(path, operation="replace")


def test_unenrolled_canonical_writer_does_not_create_format(tmp_path):
    path = tmp_path / "signals.jsonl"
    simulation._append_jsonl(path, {"quantity": 1000})
    assert journal.managed(path) is False
    with journal.open_paper_journal(path) as stream:
        assert json.loads(stream.read())["quantity"] == 1000
    assert not (tmp_path / journal.DIRECTORY).exists()
    assert set(paper_journal_generations(tmp_path)) == set(journal.FILENAMES)


@pytest.mark.parametrize("name", journal.FILENAMES)
def test_all_six_canonical_append_owners_preserve_bytes_and_parent(tmp_path, name):
    path, first = seed(tmp_path, name=name)
    before = path.read_bytes()
    payload = {"session_date": "2026-10-07", "quantity": 2000, "signed": -0.0}
    simulation._append_jsonl(path, payload)
    expected = before + simulation.ledger_lines_bytes([payload])
    with journal.open_paper_journal(path) as stream:
        assert stream.read() == expected == path.read_bytes()
        assert journal.snapshot_of(stream).extends(first["head"]["sha256"], len(before))
    assert all((tmp_path / journal.DIRECTORY / (r["sha256"] + ".zst")).stat().st_mode & 0o222 == 0
               for r in journal.JournalSnapshot(path).chunks)


def test_explicit_replacement_retires_append_ancestry_but_preserves_old_objects(tmp_path):
    path, first = seed(tmp_path)
    prior_objects = set((tmp_path / journal.DIRECTORY).iterdir())
    with journal.journal_write_transaction(path, operation="replace"):
        path.write_bytes(b'{"repaired":true}\n')
    view = journal.JournalSnapshot(path)
    assert not view.extends(first["head"]["sha256"], first["source_generation"][2])
    with journal.open_paper_journal(path) as stream:
        assert stream.read() == b'{"repaired":true}\n'
    assert prior_objects.issubset(set((tmp_path / journal.DIRECTORY).iterdir()))


def test_unknown_same_inode_rewrite_and_growth_never_become_an_append(tmp_path):
    path, _ = seed(tmp_path)
    with path.open("r+b") as stream:
        stream.write(b"!")
        stream.seek(0, io.SEEK_END)
        stream.write(b"\n")
    before = path.read_bytes()
    with pytest.raises(OSError, match="outside accepted lineage"):
        with journal.open_paper_journal(path) as stream:
            stream.read()
    with pytest.raises(OSError, match="outside accepted lineage"):
        simulation._append_jsonl(path, {"must_not_write": True})
    assert path.read_bytes() == before


@pytest.mark.parametrize("damage", ["missing", "hash", "symlink", "fifo"])
def test_corrupt_immutable_chunk_never_becomes_empty_or_cached_good_bytes(tmp_path, damage):
    path, _ = seed(tmp_path)
    with journal.open_paper_journal(path) as stream:
        stream.read()  # Include a genuinely warm object-cache case.
    chunk = journal.JournalSnapshot(path).chunks[0]
    target = tmp_path / journal.DIRECTORY / (chunk["sha256"] + ".zst")
    before = target.read_bytes()
    target.unlink()
    if damage == "hash":
        target.write_bytes(bytes([before[0] ^ 1]) + before[1:])
    elif damage == "symlink":
        alternate = tmp_path / "alternate"
        alternate.write_bytes(before)
        target.symlink_to(alternate)
    elif damage == "fifo":
        os.mkfifo(target)
    with pytest.raises((OSError, ValueError)):
        with journal.open_paper_journal(path) as stream:
            stream.read()


def test_lost_head_keeps_permanent_enrollment_marker_and_fails_closed(tmp_path):
    path, _ = seed(tmp_path)
    journal._head_path(path).unlink()
    assert journal.managed(path)
    with pytest.raises(FileNotFoundError):
        with journal.open_paper_journal(path) as stream:
            stream.read()


def test_failed_head_publication_is_not_an_accepted_partial_book(tmp_path, monkeypatch):
    path, first = seed(tmp_path)
    original = journal.atomic_write_json
    def fail(target, *args, **kwargs):
        if target == journal._head_path(path):
            raise OSError("injected head publication failure")
        return original(target, *args, **kwargs)
    monkeypatch.setattr(journal, "atomic_write_json", fail)
    with pytest.raises(OSError, match="injected"):
        simulation._append_jsonl(path, {"quantity": 2000})
    assert journal._pointer(path) == first
    with pytest.raises(OSError, match="ahead of its accepted head"):
        paper_journal_generations(tmp_path)
    with pytest.raises(OSError, match="outside accepted lineage"):
        with journal.open_paper_journal(path) as stream:
            stream.read()
    # Explicit private-fixture recovery is a new epoch, not an invented append.
    monkeypatch.setattr(journal, "atomic_write_json", original)
    journal.publish_snapshot(path, operation="replace", expected_parent=first["head"]["sha256"])
    with journal.open_paper_journal(path) as stream:
        assert stream.read() == path.read_bytes()


def test_cached_tail_and_session_index_use_verified_parent_not_old_prefix_scan(tmp_path, monkeypatch):
    path, _ = seed(tmp_path)
    original = dashboard._tail(path, 100, verified_append=True)
    initial = dashboard._ledger_session_index(path)
    def no_prefix(*args):
        raise AssertionError("immutable ancestry should not rescan the old prefix")
    monkeypatch.setattr(dashboard, "_hash_jsonl_prefix", no_prefix)
    row = {"session_date": "2026-10-07", "quantity": 2000}
    simulation._append_jsonl(path, row)
    assert dashboard._tail(path, 100, verified_append=True) == [*original, row]
    later = dashboard._ledger_session_index(path)
    assert later.lineage == initial.lineage
    assert later.prefix_sha256 is None
    assert set(later.spans) == {"2026-10-06", "2026-10-07"}
    assert later.journal_head != initial.journal_head


def test_commit_binds_small_heads_not_only_unchanged_flat_projection(tmp_path):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path)
    engine.state = state()
    path, _ = seed(tmp_path)
    engine._persist(CLOCK)
    with committed_paper_read_guard(tmp_path):
        pass
    # This changes only representation, not a single financial byte.
    journal.publish_snapshot(path, operation="replace")
    with pytest.raises(PaperReadChanged):
        with committed_paper_read_guard(tmp_path):
            pass
    engine._persist(CLOCK + timedelta(seconds=1))
    with committed_paper_read_guard(tmp_path):
        pass


def test_source_mutation_during_seal_never_publishes_head(tmp_path, monkeypatch):
    path = tmp_path / "signals.jsonl"
    path.write_bytes(b"before\n")
    original = journal._put_object
    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        path.write_bytes(b"changed\n")
        return result
    monkeypatch.setattr(journal, "_put_object", mutate)
    with pytest.raises(OSError, match="changed"):
        journal.publish_snapshot(path, operation="seed")
    assert not journal._head_path(path).exists()


def test_live_reader_fences_concurrent_flat_mutation(tmp_path):
    path, _ = seed(tmp_path)
    stream = journal.open_paper_journal(path)
    path.write_bytes(b"changed\n")
    with pytest.raises(OSError, match="outside accepted lineage"):
        stream.read()
    with pytest.raises(OSError, match="outside accepted lineage"):
        stream.close()


def test_reusable_benchmark_keeps_full_bytes_and_charges_sealing_separately(tmp_path, monkeypatch):
    from scripts.benchmark_day_trade_persistence import benchmark_journal_snapshots
    from scripts import benchmark_day_trade_persistence as benchmark
    # Unit-sized fixtures do not depend on production capacity. Admission is
    # exercised explicitly below; no real benchmark bypasses the 8 GiB gate.
    usage = shutil.disk_usage(tmp_path)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: usage._replace(free=16 * 1024**3))
    path, _ = seed(tmp_path)
    before = path.read_bytes()
    report = benchmark_journal_snapshots(path, repeats=2, output=tmp_path / "report.json")
    assert report["all_retained_bytes_and_deltas_equal"]
    assert report["complete_readback_each_sample_outside_delta_timing"]
    assert report["state_commit_ack_and_public_transport_not_measured"]
    assert len(report["samples"]) == 4
    assert report["initial_preparation"]["immutable_verified_parent"]["initial_seal_ms"] > 0
    assert path.read_bytes() == before
    with pytest.raises(ValueError, match="overwrite"):
        benchmark_journal_snapshots(path, repeats=2, output=path)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: usage._replace(free=4 * 1024**3))
    with pytest.raises(RuntimeError, match="8 GiB"):
        benchmark_journal_snapshots(path, repeats=1, output=tmp_path / "insufficient.json")
    assert path.read_bytes() == before


def test_private_clone_requires_complete_readback_then_keeps_parent_and_source_unchanged(tmp_path):
    source, target = tmp_path / "source", tmp_path / "clone"
    source.mkdir()
    original, head = seed(source)
    simulation._append_jsonl(original, {"unchanged_clock": "2026-10-06T09:00:00+08:00", "quantity": 2000})
    accepted = journal._pointer(original)
    shutil.copytree(source, target)
    with pytest.raises(OSError, match="outside accepted lineage"):
        journal.open_paper_journal(target / original.name)
    proofs = journal.rebind_cloned_journals(source, target)
    assert proofs[original.name]["verified_bytes"] == original.stat().st_size
    assert proofs[original.name]["head_sha256"] == accepted["head"]["sha256"]
    assert proofs[original.name]["target_generation"] != proofs[original.name]["source_generation"]
    with journal.open_paper_journal(target / original.name) as stream:
        assert stream.read() == original.read_bytes()
        assert journal.snapshot_of(stream).extends(head["head"]["sha256"], head["source_generation"][2])
    simulation._append_jsonl(target / original.name, {"clone_only": True})
    assert journal._pointer(original) == accepted
    assert original.read_bytes() != (target / original.name).read_bytes()


@pytest.mark.parametrize("corruption", ["raw", "object", "head", "source"])
def test_clone_rebind_rejects_corruption_before_any_pointer_changes(tmp_path, corruption):
    source, target = tmp_path / "source", tmp_path / "clone"
    source.mkdir()
    original, _ = seed(source)
    seed(source, name="orders.jsonl")
    shutil.copytree(source, target)
    if corruption == "raw":
        (target / "orders.jsonl").write_bytes(b"x" * (source / "orders.jsonl").stat().st_size)
    elif corruption == "object":
        snapshot = journal.JournalSnapshot(target / "orders.jsonl")
        member = target / journal.DIRECTORY / (snapshot.chunks[0]["sha256"] + ".zst")
        member.chmod(0o600)
        member.write_bytes(b"x" * member.stat().st_size)
    elif corruption == "head":
        journal._head_path(target / "orders.jsonl").unlink()
    else:
        original.write_bytes(b"x" * original.stat().st_size)
    before = journal._head_path(target / "signals.jsonl").read_bytes()
    with pytest.raises((OSError, ValueError)):
        journal.rebind_cloned_journals(source, target)
    assert journal._head_path(target / "signals.jsonl").read_bytes() == before


def test_legacy_clone_remains_unenrolled_and_in_place_rebind_is_rejected(tmp_path):
    source, target = tmp_path / "source", tmp_path / "clone"
    source.mkdir()
    (source / "signals.jsonl").write_bytes(b"legacy\n")
    shutil.copytree(source, target)
    assert journal.rebind_cloned_journals(source, target) == {}
    assert not (target / journal.HEADS).exists()
    with pytest.raises(ValueError, match="distinct"):
        journal.rebind_cloned_journals(source, source)


@pytest.mark.parametrize("persistent", [False, True])
def test_line_count_never_treats_same_inode_rewrite_and_growth_as_append(tmp_path, monkeypatch, persistent):
    path = tmp_path / "signals.jsonl"
    raw, changed = b"aaaaa\nbbbbb\n", b"a\nb\nc\nd\ne\nf\nx\n"
    path.write_bytes(raw)
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, str(cache) if persistent else "")
    assert dashboard._line_count(path) == 2
    original_inode = path.stat().st_ino
    path.write_bytes(changed)
    assert path.stat().st_ino == original_inode
    if persistent:
        dashboard._LINE_COUNT_CACHE.clear()
    assert dashboard._line_count(path) == changed.count(b"\n")
    with path.open("ab") as stream:
        stream.write(b"final\r\nunterminated")
    assert dashboard._line_count(path) == changed.count(b"\n") + 1


def test_line_count_uses_immutable_ancestry_and_rejects_head_corruption(tmp_path, monkeypatch):
    path, _ = seed(tmp_path, raw=b"first\r\nsecond\nlast")
    assert dashboard._line_count(path) == 2
    monkeypatch.setattr(dashboard, "_hash_jsonl_prefix", lambda *_: pytest.fail("immutable old bytes rehashed"))
    simulation._append_jsonl(path, {"next": True})
    assert dashboard._line_count(path) == 3
    # Replacing content is a new epoch, even when it grows in place.
    with journal.journal_write_transaction(path, operation="replace"):
        path.write_bytes(b"a\nb\nc\nd\ne\n" * 10)
    assert dashboard._line_count(path) == 50
    journal._head_path(path).unlink()
    with pytest.raises(FileNotFoundError):
        dashboard._line_count(path)
