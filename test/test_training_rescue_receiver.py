"""Receiver commits include file, original permissions and namespace barriers.

Fault injection proves acknowledgement ordering, not physical power-loss or
remote training acceptance. No CUDA or data-provider access is required.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import pytest

from stockagent import training_rescue_worker as worker


PAYLOAD = b"closed optimizer and negative raw values: -2,0,3\n"
SHA = hashlib.sha256(PAYLOAD).hexdigest()


class NoInput:
    def read(self, *_):
        pytest.fail("an existing verified file must not consume input")


def incoming(monkeypatch, payload=PAYLOAD):
    monkeypatch.setattr(worker.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(payload)))


def test_commit_syncs_original_permissions_and_entire_namespace(tmp_path, monkeypatch):
    path = tmp_path / "new" / "nested" / "checkpoint.pt"
    incoming(monkeypatch)
    actual_sync = os.fsync
    calls = []

    def sync(descriptor):
        value = os.fstat(descriptor)
        calls.append((stat.S_ISDIR(value.st_mode), (value.st_dev, value.st_ino),
                      stat.S_IMODE(value.st_mode), path.exists()))
        return actual_sync(descriptor)

    monkeypatch.setattr(worker.os, "fsync", sync)
    proof = worker.receive_file(path, SHA, len(PAYLOAD), 0o644)
    assert proof == {"sha256": SHA, "bytes": len(PAYLOAD), "durable": True}
    assert path.read_bytes() == PAYLOAD and stat.S_IMODE(path.stat().st_mode) == 0o644
    assert calls[0][0] is False and calls[0][2:] == (0o644, False)
    expected = []
    for parent in (path.parent, *path.parent.parents):
        if parent.stat().st_dev != path.parent.stat().st_dev:
            break
        expected.append((parent.stat().st_dev, parent.stat().st_ino))
    assert [row[1] for row in calls[1:]] == expected
    assert all(row[0] and row[3] for row in calls[1:])
    assert not list(path.parent.glob("*.partial.*"))


def test_failed_file_barrier_preserves_partial_without_publishing(tmp_path, monkeypatch):
    path = tmp_path / "incoming" / "checkpoint.pt"
    incoming(monkeypatch)
    actual_sync = os.fsync

    def sync(descriptor):
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("injected file barrier failure")
        return actual_sync(descriptor)

    monkeypatch.setattr(worker.os, "fsync", sync)
    with pytest.raises(OSError, match="file barrier"):
        worker.receive_file(path, SHA, len(PAYLOAD), 0o644)
    assert not path.exists()
    partials = list(path.parent.glob("checkpoint.pt.partial.*"))
    assert len(partials) == 1 and partials[0].read_bytes() == PAYLOAD


def test_failed_directory_barrier_requires_full_retry_before_ack(tmp_path, monkeypatch):
    path = tmp_path / "incoming" / "checkpoint.pt"
    path.parent.mkdir()
    foreign = path.parent / "checkpoint.pt.partial.foreign"
    foreign.write_bytes(b"unknown source")
    identity = path.parent.stat()
    actual_sync = os.fsync
    incoming(monkeypatch)

    def sync(descriptor):
        value = os.fstat(descriptor)
        if (value.st_dev, value.st_ino) == (identity.st_dev, identity.st_ino):
            raise OSError("injected directory barrier failure")
        return actual_sync(descriptor)

    monkeypatch.setattr(worker.os, "fsync", sync)
    with pytest.raises(OSError, match="directory barrier"):
        worker.receive_file(path, SHA, len(PAYLOAD), 0o644)
    original_inode = path.stat().st_ino
    assert path.read_bytes() == PAYLOAD and foreign.read_bytes() == b"unknown source"
    monkeypatch.setattr(worker.os, "fsync", actual_sync)
    monkeypatch.setattr(worker.sys, "stdin", SimpleNamespace(buffer=NoInput()))
    proof = worker.receive_file(path, SHA, len(PAYLOAD), 0o644)
    assert proof["reused"] and proof["durable"] and path.stat().st_ino == original_inode
    assert foreign.read_bytes() == b"unknown source"
    path.write_bytes(b"x" * len(PAYLOAD))
    with pytest.raises(ValueError, match="never overwrite"):
        worker.receive_file(path, SHA, len(PAYLOAD), 0o644)


def test_reuse_keeps_inode_and_ctime_when_original_mode_already_matches(tmp_path, monkeypatch):
    path = tmp_path / "original"
    path.write_bytes(PAYLOAD)
    path.chmod(0o400)
    before = (path.stat().st_ino, path.stat().st_ctime_ns)
    monkeypatch.setattr(worker.sys, "stdin", SimpleNamespace(buffer=NoInput()))
    monkeypatch.setattr(worker.os, "fchmod", lambda *_: pytest.fail("unchanged permissions must not mutate ctime"))
    proof = worker.receive_file(path, SHA, len(PAYLOAD), 0o400)
    assert proof["reused"] and proof["durable"]
    assert (path.stat().st_ino, path.stat().st_ctime_ns) == before


def test_reuse_permission_change_is_flushed_before_ack(tmp_path, monkeypatch):
    path = tmp_path / "original"
    path.write_bytes(PAYLOAD)
    path.chmod(0o600)
    original_inode = path.stat().st_ino
    monkeypatch.setattr(worker.sys, "stdin", SimpleNamespace(buffer=NoInput()))
    actual_sync = os.fsync
    regular_modes = []

    def sync(descriptor):
        value = os.fstat(descriptor)
        if stat.S_ISREG(value.st_mode):
            regular_modes.append(stat.S_IMODE(value.st_mode))
        return actual_sync(descriptor)

    monkeypatch.setattr(worker.os, "fsync", sync)
    proof = worker.receive_file(path, SHA, len(PAYLOAD), 0o644)
    assert proof["reused"] and proof["durable"] and regular_modes == [0o644]
    assert path.stat().st_ino == original_inode and path.read_bytes() == PAYLOAD


@pytest.mark.parametrize("payload", [b"short", b"x" * len(PAYLOAD), PAYLOAD + b"extra"])
def test_bad_input_never_publishes_and_keeps_exact_new_partial(tmp_path, monkeypatch, payload):
    path = tmp_path / "new" / "original"
    incoming(monkeypatch, payload)
    with pytest.raises(ValueError, match="partial/corrupt|exceeded"):
        worker.receive_file(path, SHA, len(PAYLOAD))
    assert not path.exists()
    assert len(list(path.parent.glob("original.partial.*"))) == 1


@pytest.mark.parametrize("kind", ["file", "directory", "fifo", "symlink", "dangling"])
def test_foreign_target_is_preserved(tmp_path, monkeypatch, kind):
    path = tmp_path / "target"
    original = tmp_path / "foreign"
    original.write_bytes(b"foreign")
    if kind == "file":
        path.write_bytes(b"x" * len(PAYLOAD))
    elif kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.symlink_to(original if kind == "symlink" else tmp_path / "absent")
    before = path.lstat()
    monkeypatch.setattr(worker.sys, "stdin", SimpleNamespace(buffer=NoInput()))
    with pytest.raises((OSError, ValueError)):
        worker.receive_file(path, SHA, len(PAYLOAD))
    after = path.lstat()
    assert (before.st_ino, before.st_mode, before.st_ctime_ns) == (after.st_ino, after.st_mode, after.st_ctime_ns)
    assert original.read_bytes() == b"foreign"


def test_no_overwrite_when_foreign_file_arrives_before_link(tmp_path, monkeypatch):
    path = tmp_path / "original"
    incoming(monkeypatch)
    actual_link = os.link

    def link(source, target, **kwargs):
        path.write_bytes(b"concurrent foreign source")
        return actual_link(source, target, **kwargs)

    monkeypatch.setattr(worker.os, "link", link)
    with pytest.raises(FileExistsError):
        worker.receive_file(path, SHA, len(PAYLOAD))
    assert path.read_bytes() == b"concurrent foreign source"
    partials = list(tmp_path.glob("original.partial.*"))
    assert len(partials) == 1 and partials[0].read_bytes() == PAYLOAD


def test_reuse_cannot_change_permissions_through_another_hardlink(tmp_path, monkeypatch):
    path = tmp_path / "original"
    path.write_bytes(PAYLOAD)
    path.chmod(0o400)
    sibling = tmp_path / "other-consumer"
    os.link(path, sibling)
    monkeypatch.setattr(worker.sys, "stdin", SimpleNamespace(buffer=NoInput()))
    with pytest.raises(ValueError, match="shared target inode"):
        worker.receive_file(path, SHA, len(PAYLOAD), 0o644)
    assert stat.S_IMODE(sibling.stat().st_mode) == 0o400
    proof = worker.receive_file(path, SHA, len(PAYLOAD), 0o400)
    assert proof["durable"] and path.stat().st_ino == sibling.stat().st_ino


def test_directory_move_during_last_barrier_cannot_be_acknowledged(tmp_path, monkeypatch):
    path = tmp_path / "incoming" / "original"
    path.parent.mkdir()
    identity = path.parent.stat()
    relocated = tmp_path / "moved"
    actual_sync = os.fsync
    incoming(monkeypatch)
    moved = False

    def sync(descriptor):
        nonlocal moved
        value = os.fstat(descriptor)
        if not moved and (value.st_dev, value.st_ino) == (identity.st_dev, identity.st_ino):
            path.parent.rename(relocated)
            path.parent.mkdir()
            moved = True
        return actual_sync(descriptor)

    monkeypatch.setattr(worker.os, "fsync", sync)
    with pytest.raises(ValueError, match="directory moved"):
        worker.receive_file(path, SHA, len(PAYLOAD))
    assert not path.exists() and (relocated / path.name).read_bytes() == PAYLOAD


def test_nested_mount_does_not_require_syncing_an_unrelated_filesystem(monkeypatch):
    mount = Path("/dev/shm")
    if not mount.is_dir() or mount.stat().st_dev == Path("/").stat().st_dev:
        pytest.skip("requires an existing separate tmpfs mount")
    actual_sync = os.fsync
    calls = []
    with tempfile.TemporaryDirectory(prefix="stockagent-receiver-", dir=mount) as root:
        path = Path(root) / "new" / "nested" / "original"
        incoming(monkeypatch)

        def sync(descriptor):
            value = os.fstat(descriptor)
            assert value.st_dev == mount.stat().st_dev
            calls.append(value.st_ino)
            return actual_sync(descriptor)

        monkeypatch.setattr(worker.os, "fsync", sync)
        proof = worker.receive_file(path, SHA, len(PAYLOAD))
        assert proof["durable"] and path.read_bytes() == PAYLOAD
        assert mount.stat().st_ino in calls


@pytest.mark.parametrize("existing", [False, True])
def test_stream_construction_failure_closes_owned_descriptors(tmp_path, monkeypatch, existing):
    path = tmp_path / "incoming" / "original"
    if existing:
        path.parent.mkdir()
        path.write_bytes(PAYLOAD)
    incoming(monkeypatch)
    before = len(list(Path("/proc/self/fd").iterdir()))

    def fail(*_):
        raise OSError("injected stream construction failure")

    monkeypatch.setattr(worker.os, "fdopen", fail)
    with pytest.raises(OSError, match="stream construction"):
        worker.receive_file(path, SHA, len(PAYLOAD))
    assert len(list(Path("/proc/self/fd").iterdir())) == before


def test_cli_reuse_proves_full_hash_and_durable_original_mode(tmp_path):
    path = tmp_path / "new" / "nested" / "original"
    command = [sys.executable, str(Path(worker.__file__)), "receive", "--path", str(path),
               "--sha256", SHA, "--bytes", str(len(PAYLOAD)), "--mode", str(0o644)]
    first = subprocess.run(command, input=PAYLOAD, capture_output=True, check=True)
    second = subprocess.run(command, input=b"", capture_output=True, check=True)
    assert json.loads(first.stdout)["durable"]
    assert json.loads(second.stdout)["reused"] and json.loads(second.stdout)["durable"]
    path.write_bytes(b"x" * len(PAYLOAD))
    bad = subprocess.run(command, input=b"", capture_output=True)
    assert bad.returncode != 0 and b'"durable": true' not in bad.stdout


@pytest.mark.parametrize("target", ["new", "exact", "conflicting"])
@pytest.mark.parametrize("entrypoint", ["object", "cold_source"])
def test_push_early_receiver_close_finishes_the_owned_native_source(tmp_path, monkeypatch, target, entrypoint):
    """A native producer must reach EOF even when a target declines stdin.

    Use the real receiver and a payload larger than the copy and pipe buffers.
    Linux file handles hide this Windows producer shutdown boundary.
    """
    from contextlib import contextmanager
    from stockagent.training_rescue import RescueError
    from test_training_rescue_preparation import controller_fixture
    import stockagent.data_sync.windows_cold_io as native
    owner = controller_fixture(tmp_path, monkeypatch)
    payload = b"ordered negative raw values: -3,0,2\n" * 262144
    row = {"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}
    source = owner.object_path(row["sha256"])
    source.parent.mkdir(parents=True); source.write_bytes(payload)
    destination = tmp_path / "remote/original"
    if target != "new":
        destination.parent.mkdir()
        destination.write_bytes(payload if target == "exact" else b"foreign original")
    prior = destination.stat() if destination.exists() else None
    finished = []
    @contextmanager
    def native_source(path):
        assert path == source
        stream = io.BytesIO(payload)
        try:
            yield stream
        finally:
            assert stream.tell() == len(payload), "native producer left blocked on unread stdout"
            finished.append(True)
            stream.close()
    class ByteSSH:
        def command(self, argv):
            return [sys.executable, str(Path(worker.__file__)), *argv[2:]]
    monkeypatch.setattr(native, "binary_reader", native_source)
    def push():
        if entrypoint == "cold_source":
            return owner.push_cold_source(ByteSSH(), source, {"sha256": row["sha256"], "size": row["bytes"]}, destination)
        return owner.push_object(ByteSSH(), row, destination)
    if target == "conflicting":
        with pytest.raises(RescueError, match="target rejected"):
            push()
        assert destination.read_bytes() == b"foreign original"
    else:
        result = push()
        if entrypoint == "object":
            assert result["durable"] and result.get("reused", False) == (target == "exact")
        else:
            assert result is None  # Preserve the original cold-source API.
        assert destination.read_bytes() == payload
    assert finished == [True, True]  # Full preflight SHA read, then transfer read.
    if prior is not None:
        assert (destination.stat().st_ino, destination.stat().st_ctime_ns) == (prior.st_ino, prior.st_ctime_ns)
