"""Expected-byte recovery never invents data or acknowledges financial state."""
import hashlib
import os
import shutil
import subprocess
import sys

import pytest

from stockagent.live import paper_journal as journal
from stockagent.live import tw_day_trade_simulation as simulation
from stockagent.live.tw_day_trade_service_sync import PaperReadChanged, committed_paper_read_guard
from test_paper_journal import seed
from test_paper_mode_snapshot import CLOCK, state


@pytest.mark.parametrize("name", journal.FILENAMES)
def test_durable_append_intent_and_idempotent_complete_readback(tmp_path, name):
    path, parent = seed(tmp_path, name=name)
    before = path.read_bytes()
    delta = b'{"quantity":2000,"price":-0.0,"big":9007199254740993}\n'
    journal.append_paper_journal(path, delta)
    intent, node = journal._read_append_intent(path)
    assert intent["parent"] == parent
    assert intent["append_sha256"] == hashlib.sha256(delta).hexdigest()
    assert node["base_size"] == len(before)
    first = journal.recover_paper_journal_append(path)
    assert first["already_sealed"] is True
    assert first["verified_bytes"] == len(before + delta)
    assert first["financial_commit_acknowledged"] is False
    assert journal.recover_paper_journal_append(path) == first
    assert path.read_bytes() == before + delta
    with journal.open_paper_journal(path) as stream:
        assert stream.read() == before + delta


@pytest.mark.parametrize("phase", ["objects", "intent", "raw", "head", "object_file", "object_directory",
                                   "intent_file", "intent_directory", "raw_before_fsync", "raw_after_fsync",
                                   "head_file", "head_directory"])
def test_actual_process_exit_at_durability_boundaries(tmp_path, phase):
    path, parent = seed(tmp_path)
    before = path.read_bytes()
    # No interpreter cleanup/exception rollback: the child exits at the actual
    # owner boundary. This is process-crash evidence, not physical power loss.
    script = r'''
import os, sys
from pathlib import Path
from stockagent.live import paper_journal as j
path, phase = Path(sys.argv[1]), sys.argv[2]
if phase == "objects":
    original = j.atomic_write_json
    def stop(target, *a, **kw):
        if target == j._intent_path(path): os._exit(81)
        return original(target, *a, **kw)
    j.atomic_write_json = stop
elif phase == "intent":
    j._append_bytes = lambda *a, **kw: os._exit(81)
elif phase == "raw":
    original = j.atomic_write_json
    def stop(target, *a, **kw):
        if target == j._head_path(path): os._exit(81)
        return original(target, *a, **kw)
    j.atomic_write_json = stop
elif phase == "head":
    original = j.atomic_write_json
    def stop(target, *a, **kw):
        result = original(target, *a, **kw)
        if target == j._head_path(path): os._exit(81)
        return result
    j.atomic_write_json = stop
else:
    original = os.fsync
    original_size = path.stat().st_size
    def stop(fd):
        target = Path(os.readlink(f"/proc/self/fd/{fd}"))
        kind = None
        if target == path:
            kind = "raw_before_fsync"
            if phase == "raw_after_fsync":
                original(fd)
                os._exit(81)
        elif target == path.parent / j.DIRECTORY:
            kind = "object_directory"
        elif target.parent == path.parent / j.DIRECTORY:
            kind = "object_file"
        elif target == path.parent / j.HEADS:
            kind = "intent_directory" if path.stat().st_size == original_size else "head_directory"
        elif target.parent == path.parent / j.HEADS:
            kind = "intent_file" if "append-intent" in target.name else "head_file"
        if kind == phase: os._exit(81)
        return original(fd)
    os.fsync = stop
j.append_paper_journal(path, b'{"quantity":2000}\n')
raise AssertionError("crash injection was not reached")
'''
    child = subprocess.run([sys.executable, "-c", script, str(path), phase], timeout=25, capture_output=True)
    assert child.returncode == 81, child.stderr.decode()
    if phase in {"objects", "intent", "object_file", "object_directory", "intent_file", "intent_directory"}:
        assert path.read_bytes() == before and journal._pointer(path) == parent
        # No execution byte was written. An ordinary new attempt is safe and
        # replaces the still-unused intent, without adopting orphan objects.
        journal.append_paper_journal(path, b'{"quantity":3000}\n')
        assert path.read_bytes() == before + b'{"quantity":3000}\n'
    else:
        expected = before + b'{"quantity":2000}\n'
        assert path.read_bytes() == expected
        recovered = journal.recover_paper_journal_append(path)
        assert recovered["already_sealed"] == (phase in {"head", "head_directory"})
        pointer = journal._pointer(path)
        assert journal.recover_paper_journal_append(path)["already_sealed"]
        assert journal._pointer(path) == pointer
        with journal.open_paper_journal(path) as stream:
            assert stream.read() == expected


@pytest.mark.parametrize("damage", ["partial", "prefix", "tail", "inode", "intent", "node", "object"])
def test_interrupted_append_refuses_unprovable_recovery(tmp_path, monkeypatch, damage):
    path, parent = seed(tmp_path)
    original = journal.atomic_write_json
    def stop(target, *args, **kwargs):
        if target == journal._head_path(path):
            raise OSError("injected head failure")
        return original(target, *args, **kwargs)
    monkeypatch.setattr(journal, "atomic_write_json", stop)
    with pytest.raises(OSError, match="injected"):
        journal.append_paper_journal(path, b'{"quantity":2000}\n')
    monkeypatch.setattr(journal, "atomic_write_json", original)
    if damage == "partial":
        with path.open("r+b") as stream:
            stream.truncate(path.stat().st_size - 1)
    elif damage in {"prefix", "tail"}:
        with path.open("r+b") as stream:
            stream.seek(0 if damage == "prefix" else parent["source_generation"][2])
            stream.write(b"!")
    elif damage == "inode":
        copied = tmp_path / "foreign"
        copied.write_bytes(path.read_bytes())
        copied.replace(path)
    else:
        intent, node = journal._read_append_intent(path)
        if damage == "intent":
            journal._intent_path(path).write_bytes(b'{"contract":"unknown"}')
        else:
            ref = intent["head"] if damage == "node" else node["chunks"][0]
            target = tmp_path / journal.DIRECTORY / (ref["sha256"] + (".json" if damage == "node" else ".zst"))
            target.chmod(0o600)
            target.write_bytes(b"x" * target.stat().st_size)
    raw, head = path.read_bytes(), journal._head_path(path).read_bytes()
    with pytest.raises((OSError, ValueError)):
        journal.recover_paper_journal_append(path)
    with pytest.raises((OSError, ValueError)):
        journal.append_paper_journal(path, b'{"must_not_duplicate":true}\n')
    assert path.read_bytes() == raw and journal._head_path(path).read_bytes() == head


def test_sealing_all_six_journals_is_not_financial_commit_or_ack(tmp_path, monkeypatch):
    engine = simulation.TwDayTradeSimulationEngine(tmp_path)
    engine.state = state()
    paths = [seed(tmp_path, name=name)[0] for name in journal.FILENAMES]
    engine._persist(CLOCK)
    with committed_paper_read_guard(tmp_path):
        pass
    original = journal.atomic_write_json
    def stop(target, *args, **kwargs):
        if target.name.endswith(".jsonl.json"):
            raise OSError("injected head failure")
        return original(target, *args, **kwargs)
    monkeypatch.setattr(journal, "atomic_write_json", stop)
    for path in paths:
        with pytest.raises(OSError, match="injected"):
            journal.append_paper_journal(path, b'{"quantity":2000}\n')
    monkeypatch.setattr(journal, "atomic_write_json", original)
    for path in paths:
        assert not journal.recover_paper_journal_append(path)["financial_commit_acknowledged"]
    with pytest.raises(PaperReadChanged):
        with committed_paper_read_guard(tmp_path):
            pytest.fail("representation recovery must not admit a mixed financial view")
    # Only the ordinary canonical financial commit may make this test book
    # readable again; there is no automatic financial state reconstruction.
    engine._persist(CLOCK)
    with committed_paper_read_guard(tmp_path):
        pass


def test_legacy_and_explicit_replace_remain_separate(tmp_path):
    legacy = tmp_path / "fills.jsonl"
    journal.append_paper_journal(legacy, b'{"quantity":1000}\n')
    assert not journal.managed(legacy) and not journal._intent_path(legacy).exists()
    with pytest.raises(ValueError, match="enrolled"):
        journal.recover_paper_journal_append(legacy)
    path, _parent = seed(tmp_path)
    journal.append_paper_journal(path, b'{"quantity":2000}\n')
    with journal.journal_write_transaction(path, operation="replace"):
        path.write_bytes(b'{"authorized_replacement":true}\n')
    # A stale old intent cannot authorize recovery over a new explicit epoch.
    with pytest.raises(OSError, match="compare-and-set"):
        journal.recover_paper_journal_append(path)
    journal.append_paper_journal(path, b'{"new_epoch":true}\n')
    assert journal.recover_paper_journal_append(path)["already_sealed"]


def test_startup_recovery_only_visits_changed_enrolled_heads(tmp_path, monkeypatch):
    path, _parent = seed(tmp_path)
    original = journal.recover_paper_journal_append
    calls = []
    def counted(target):
        calls.append(target)
        return original(target)
    monkeypatch.setattr(journal, "recover_paper_journal_append", counted)
    assert journal.recover_pending_journal_heads(tmp_path) == [] and not calls
    write = journal.atomic_write_json
    def stop(target, *args, **kwargs):
        if target == journal._head_path(path):
            raise OSError("injected head failure")
        return write(target, *args, **kwargs)
    monkeypatch.setattr(journal, "atomic_write_json", stop)
    with pytest.raises(OSError, match="injected"):
        journal.append_paper_journal(path, b'{"quantity":2000}\n')
    monkeypatch.setattr(journal, "atomic_write_json", write)
    assert len(journal.recover_pending_journal_heads(tmp_path)) == 1
    assert calls == [path]
    assert journal.recover_pending_journal_heads(tmp_path) == [] and calls == [path]


def test_startup_reestablishes_already_visible_head_directory_barrier(tmp_path, monkeypatch):
    path, _parent = seed(tmp_path)
    journal.append_paper_journal(path, b'{"quantity":2000}\n')
    original, synced = os.fsync, []
    def counted(fd):
        synced.append(os.readlink(f"/proc/self/fd/{fd}"))
        return original(fd)
    monkeypatch.setattr(os, "fsync", counted)
    assert journal.recover_pending_journal_heads(tmp_path) == []
    assert str(tmp_path / journal.HEADS) in synced


def test_benchmark_charges_complete_six_journal_commit_readback_and_restore(tmp_path, monkeypatch):
    from scripts.benchmark_day_trade_persistence import benchmark_journal_transaction
    from stockagent.data_sync import cache_capacity
    monkeypatch.setattr(cache_capacity, "acquisition_dispatch_status", lambda *a, **kw: {"state": "admitted", "scope": "tiny_test_fixture"})
    source = tmp_path / "source"
    source.mkdir()
    engine = simulation.TwDayTradeSimulationEngine(source)
    engine.state = state()
    for name in journal.FILENAMES:
        path = source / name
        path.write_bytes(b'{"unmodified_financial_fixture":true,"signed_zero":-0.0}\n')
    engine._persist(CLOCK)
    before = {path.name: path.read_bytes() for path in source.iterdir() if path.is_file()}
    usage = shutil.disk_usage(tmp_path)
    monkeypatch.setattr(shutil, "disk_usage", lambda _: usage._replace(free=16 * 1024**3))
    result = benchmark_journal_transaction(engine.state_path, repeats=2, output=tmp_path / "report.json")
    assert result["mode_count"] == 6
    assert result["complete_journal_bytes_equal"] and result["complete_financial_publication_equal"]
    assert result["complete_restore_equal"] and result["post_fsync_ack_verified_each_sample"]
    assert len(result["samples"]) == 4
    assert [row["variant"] for row in result["samples"]] == [
        "legacy_projection", "expected_append_intent", "expected_append_intent", "legacy_projection"]
    assert all(row["complete_workflow_ms"] >= row["append_ms"] + row["state_commit_ack_ms"] for row in result["samples"])
    assert before == {path.name: path.read_bytes() for path in source.iterdir() if path.is_file()}
    with pytest.raises(ValueError, match="source book"):
        benchmark_journal_transaction(engine.state_path, repeats=1, output=source / "must_not_write.json")
    monkeypatch.setattr(cache_capacity, "acquisition_dispatch_status", lambda *a, **kw: {"state": "deferred"})
    with pytest.raises(RuntimeError, match="physical backing"):
        benchmark_journal_transaction(engine.state_path, repeats=1, output=tmp_path / "physical_not_admitted.json")
    monkeypatch.setattr(shutil, "disk_usage", lambda _: usage._replace(free=4 * 1024**3))
    with pytest.raises(RuntimeError, match="8 GiB"):
        benchmark_journal_transaction(engine.state_path, repeats=1, output=tmp_path / "not_admitted.json")


@pytest.mark.parametrize("damage", ["symlink", "fifo"])
def test_intent_special_files_never_block_or_downgrade(tmp_path, damage):
    path, _parent = seed(tmp_path)
    target = journal._intent_path(path)
    if damage == "symlink":
        target.symlink_to(tmp_path / "missing")
    else:
        os.mkfifo(target)
    with pytest.raises((OSError, ValueError)):
        journal.append_paper_journal(path, b'{"must_not_write":true}\n')
