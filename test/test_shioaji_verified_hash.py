import errno
import hashlib
import os
from pathlib import Path

import pytest

from downloader import download_shioaji_historical_market_data as exact
from downloader import download_shioaji_tx_futures_ticks as continuous
from downloader import shioaji_history_repair as repair
from downloader.artifact_io import atomic_write_json


@pytest.fixture(autouse=True)
def empty_hash_memo():
    repair._signature_sha.cache_clear()
    exact._valid_receipt_at_signature.cache_clear()
    yield
    repair._signature_sha.cache_clear()
    exact._valid_receipt_at_signature.cache_clear()


def test_regular_hash_matches_original_and_reuses_only_exact_identity(tmp_path, monkeypatch):
    path = tmp_path / "data"
    path.write_bytes(b"original value")
    original_digest = repair.hashlib.file_digest
    calls = []

    def counted(stream, algorithm):
        calls.append(stream.fileno())
        return original_digest(stream, algorithm)

    monkeypatch.setattr(repair.hashlib, "file_digest", counted)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    assert repair.verified_sha(path) == expected
    assert repair.verified_sha(path) == expected
    assert len(calls) == 1
    assert repair._signature_sha.cache_info().hits == 1
    previous = path.stat()
    path.write_bytes(b"replaced value")
    os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
    assert path.stat().st_size == previous.st_size
    assert repair.verified_sha(path) == hashlib.sha256(b"replaced value").hexdigest()
    assert len(calls) == 2


def test_hash_does_not_resolve_every_path_component(tmp_path, monkeypatch):
    path = tmp_path / "data"
    path.write_bytes(b"real bytes")

    def forbidden(*_args, **_kwargs):
        pytest.fail("digest lookup must not resolve path components")

    monkeypatch.setattr(Path, "resolve", forbidden)
    assert repair.verified_sha(path) == hashlib.sha256(b"real bytes").hexdigest()
    assert repair.verified_sha(path) == hashlib.sha256(b"real bytes").hexdigest()


def test_absolute_preserves_symlink_parent_dotdot_semantics(tmp_path, monkeypatch):
    nested = tmp_path / "actual" / "nested"
    nested.mkdir(parents=True)
    (nested.parent / "data").write_bytes(b"correct filesystem target")
    (tmp_path / "data").write_bytes(b"wrong lexical normalization target")
    (tmp_path / "alias").symlink_to(nested, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    assert repair.verified_sha(Path("alias/../data")) == hashlib.sha256(
        b"correct filesystem target"
    ).hexdigest()


def test_symlink_aba_between_stat_and_open_cannot_poison_memo(tmp_path, monkeypatch):
    a, b, alias = (tmp_path / name for name in ("a", "b", "alias"))
    a.write_bytes(b"original")
    b.write_bytes(b"different")
    alias.symlink_to(a)
    original_open = repair.os.open

    def open_other_identity(path, flags):
        alias.unlink()
        alias.symlink_to(b)
        descriptor = original_open(path, flags)
        alias.unlink()
        alias.symlink_to(a)
        return descriptor

    with monkeypatch.context() as patcher:
        patcher.setattr(repair.os, "open", open_other_identity)
        with pytest.raises(OSError, match="source changed"):
            repair.verified_sha(alias)
    assert repair._signature_sha.cache_info().currsize == 0
    assert repair.verified_sha(alias) == hashlib.sha256(b"original").hexdigest()


def test_cached_identity_is_checked_after_alias_switch(tmp_path, monkeypatch):
    a, b, alias = (tmp_path / name for name in ("a", "b", "alias"))
    a.write_bytes(b"original")
    b.write_bytes(b"different")
    alias.symlink_to(a)
    assert repair.verified_sha(alias) == hashlib.sha256(b"original").hexdigest()
    original_cache = repair._signature_sha

    def switch_after_cached_lookup(path, signature):
        result = original_cache(path, signature)
        alias.unlink()
        alias.symlink_to(b)
        return result

    with monkeypatch.context() as patcher:
        patcher.setattr(repair, "_signature_sha", switch_after_cached_lookup)
        with pytest.raises(OSError, match="source changed"):
            repair.verified_sha(alias)
    assert repair.verified_sha(alias) == hashlib.sha256(b"different").hexdigest()


@pytest.mark.parametrize("kind", ["in_place", "atomic_replace", "unlink"])
def test_mutation_during_hash_is_not_accepted_or_cached(tmp_path, monkeypatch, kind):
    path, replacement = tmp_path / "data", tmp_path / "replacement"
    path.write_bytes(b"original")
    replacement.write_bytes(b"different")
    before = path.stat()
    original_digest = repair.hashlib.file_digest

    def mutate_after_read(stream, algorithm):
        digest = original_digest(stream, algorithm)
        if kind == "in_place":
            path.write_bytes(b"replaced")
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        elif kind == "atomic_replace":
            os.replace(replacement, path)
        else:
            path.unlink()
        return digest

    with monkeypatch.context() as patcher:
        patcher.setattr(repair.hashlib, "file_digest", mutate_after_read)
        with pytest.raises(OSError):
            repair.verified_sha(path)
    # A digest from the original opened identity cannot become the new file's
    # proof, even if a platform allows the old identity to finish hashing.
    if path.exists():
        assert repair.verified_sha(path) == hashlib.sha256(path.read_bytes()).hexdigest()


def test_hash_failure_closes_owned_descriptor_and_does_not_memoize(tmp_path, monkeypatch):
    path = tmp_path / "data"
    path.write_bytes(b"original")
    original_open, descriptors = repair.os.open, []

    def tracked_open(*args):
        descriptor = original_open(*args)
        descriptors.append(descriptor)
        return descriptor

    def fail(*_args):
        raise OSError(errno.EIO, "injected read failure")

    monkeypatch.setattr(repair.os, "open", tracked_open)
    monkeypatch.setattr(repair.hashlib, "file_digest", fail)
    with pytest.raises(OSError, match="injected read failure"):
        repair.verified_sha(path)
    assert repair._signature_sha.cache_info().currsize == 0
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_nonregular_path_is_rejected_before_open(tmp_path):
    with pytest.raises(OSError, match="regular file"):
        repair.verified_sha(tmp_path)
    assert repair._signature_sha.cache_info().currsize == 0


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFO race")
def test_fifo_replacement_does_not_block_audit_open(tmp_path, monkeypatch):
    path, fifo = tmp_path / "data", tmp_path / "fifo"
    path.write_bytes(b"original")
    os.mkfifo(fifo)
    original_open = repair.os.open

    def replace_before_open(name, flags):
        assert flags & os.O_NONBLOCK
        os.replace(fifo, path)
        return original_open(name, flags)

    monkeypatch.setattr(repair.os, "open", replace_before_open)
    with pytest.raises(OSError, match="source changed"):
        repair.verified_sha(path)
    assert repair._signature_sha.cache_info().currsize == 0


@pytest.mark.parametrize("collector", [exact, continuous])
def test_transient_hash_failure_does_not_abort_or_poison_receipt_cache(tmp_path, monkeypatch, collector):
    from datetime import date

    day = date(2026, 10, 13)
    if collector is exact:
        partition = tmp_path / "start=2026-10-13_end=2026-10-13"
        partition.mkdir()
        data, receipt = partition / "data.parquet", partition / "receipt.json"
        check = lambda: exact._valid_receipt(receipt, data, method="kbars", code="TXFR1")
    else:
        tmp_path = tmp_path / "TXFR1"
        data, receipt = continuous._data_path(tmp_path, day), continuous._receipt_path(tmp_path, day)
        data.parent.mkdir(parents=True)
        check = lambda: continuous._valid_receipt(tmp_path, day)
    data.write_bytes(b"verified real bytes")
    payload = {"schema_version": collector.RECEIPT_SCHEMA_VERSION, "source": collector.SOURCE,
               "contract": "TXFR1", "method": "kbars", "status": "complete",
               "start": str(day), "end": str(day), "trading_date": str(day),
               "sha256": hashlib.sha256(data.read_bytes()).hexdigest()}
    atomic_write_json(receipt, payload)
    with monkeypatch.context() as patcher:
        def fail(_path):
            raise OSError(errno.EAGAIN, "source changed")
        patcher.setattr(collector, "verified_sha", fail)
        assert check() is None
    assert check() == payload
