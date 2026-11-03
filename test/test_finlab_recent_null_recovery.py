from datetime import UTC, datetime, timedelta
import importlib
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
from pyarrow import ipc
import pytest

from scripts.finlab_recent_null_recovery import recent_null_recovery
from scripts.finlab_incremental_refresh import checked_get


@pytest.fixture
def real_storage(tmp_path):
    """Actual SDK storage with a synthetic all-null historical field."""
    if version("finlab") != "2.1.1":
        pytest.skip("This recovery intentionally applies only to verified FinLab 2.1.1 internals")
    from finlab.data.storage import FileStorage

    storage = FileStorage(str(tmp_path), use_cache=False)
    name = "test:null_promotion"
    path = Path(storage._data_file_path(name, ".feather"))
    table = pa.table({"date": pa.array([pd.Timestamp("2020-01-01")], type=pa.timestamp("ns")),
                      "symbol_null": pa.nulls(1), "symbol_existing": pa.array(["old"])})
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)
    delta = pd.DataFrame({"date": [pd.Timestamp("2020-01-02")],
                          "symbol_null": ["new"], "symbol_existing": ["current"]})
    recent = importlib.import_module("finlab.data.recent_merge")
    sdk_get = importlib.import_module("finlab.data.get")
    expected = recent.merge_recent_df(table.to_pandas(), delta)
    return storage, name, path, delta, expected, sdk_get.hash(expected)


def test_sdk_verified_null_promotion_preserves_values(real_storage):
    storage, name, path, delta, expected, digest = real_storage
    expiry = datetime.now(UTC) + timedelta(days=1)
    with recent_null_recovery(storage, name) as evidence:
        assert storage.merge_recent_delta(name, delta, digest, expiry=expiry)
    assert evidence == {"recovered": True, "hash_mismatch": False}
    assert "merge_recent_delta" not in vars(storage)
    with pa.memory_map(str(path)) as source:
        actual = ipc.open_file(source).read_all().to_pandas()
    pd.testing.assert_frame_equal(actual, expected)
    assert storage.get_time_expired(name) == expiry
    assert not list(path.parent.glob(".finlab-null-*.tmp"))


def test_hash_mismatch_preserves_original_and_signals_sdk_fallback(real_storage):
    storage, name, path, delta, _, digest = real_storage
    before = path.read_bytes()
    wrong_hash = "0000000" if digest != "0000000" else "1111111"
    with recent_null_recovery(storage, name) as evidence:
        assert storage.merge_recent_delta(name, delta, wrong_hash) is False
    assert evidence == {"recovered": False, "hash_mismatch": True}
    assert path.read_bytes() == before
    assert "merge_recent_delta" not in vars(storage)
    assert not list(path.parent.glob(".finlab-null-*.tmp"))


def test_unrelated_key_does_not_get_repaired(real_storage):
    storage, name, path, delta, _, digest = real_storage
    before = path.read_bytes()
    with recent_null_recovery(storage, "other:key"):
        with pytest.raises(pa.ArrowNotImplementedError):
            storage.merge_recent_delta(name, delta, digest)
    assert path.read_bytes() == before
    assert "merge_recent_delta" not in vars(storage)


def test_successful_normal_merge_is_unchanged(real_storage):
    storage, name, path, _, _, _ = real_storage
    delta = pd.DataFrame({"date": [pd.Timestamp("2020-01-02")],
                          "symbol_null": [None], "symbol_existing": ["current"]})
    recent = importlib.import_module("finlab.data.recent_merge")
    sdk_get = importlib.import_module("finlab.data.get")
    with pa.memory_map(str(path)) as source:
        original = ipc.open_file(source).read_all().to_pandas()
    expected = recent.merge_recent_df(original, delta)
    with recent_null_recovery(storage, name) as evidence:
        assert storage.merge_recent_delta(name, delta, sdk_get.hash(expected))
    assert evidence == {"recovered": False, "hash_mismatch": False}
    with pa.memory_map(str(path)) as source:
        actual = ipc.open_file(source).read_all().to_pandas()
    pd.testing.assert_frame_equal(actual, expected)


def test_other_arrow_error_is_not_swallowed(real_storage, monkeypatch):
    storage, name, path, delta, _, digest = real_storage
    from finlab.data.storage import FileStorage
    before = path.read_bytes()

    def fail(*args, **kwargs):
        raise pa.ArrowNotImplementedError("different error")

    monkeypatch.setattr(FileStorage, "merge_recent_delta", fail)
    with recent_null_recovery(storage, name):
        with pytest.raises(pa.ArrowNotImplementedError, match="different error"):
            storage.merge_recent_delta(name, delta, digest)
    assert path.read_bytes() == before
    assert "merge_recent_delta" not in vars(storage)


def test_checked_get_reuses_sdk_publication_after_verified_recovery(real_storage):
    storage, name, path, delta, expected, digest = real_storage
    storage._publications = {}
    storage._created[name] = datetime.now(UTC)
    storage._expiry[name] = datetime.now(UTC) + timedelta(hours=1)
    ctx = SimpleNamespace(_storage=storage, _loaded_datasets={name},
                          prefer_local_if_exists=True, use_local_data_only=True,
                          force_cloud_download=True)
    calls = []

    def get(key, *, force_download, progress, progress_callback):
        calls.append((key, force_download))
        assert storage.merge_recent_delta(key, delta, digest, expiry=datetime.now(UTC) + timedelta(hours=1))
        storage._publications[key] = (storage.get_time_created(key), {"content_hash": digest})
        return expected

    sdk = SimpleNamespace(__name__="finlab.data", _default_context=ctx, get=get)
    frame, evidence = checked_get(name, sdk=sdk)
    assert calls == [(name, False)]
    assert evidence["source_check_mode"] == "upstream_incremental"
    assert evidence["provider_content_hash"] == digest
    assert evidence["recent_merge_recovery"]["sdk_hash_verified"] is True
    assert evidence["recent_merge_recovery"]["sdk_full_fallback_after_hash_mismatch"] is False
    assert ctx.prefer_local_if_exists and ctx.use_local_data_only and ctx.force_cloud_download
    assert "merge_recent_delta" not in vars(storage)
    pd.testing.assert_frame_equal(frame, expected)


def test_checked_get_records_native_full_fallback_after_hash_mismatch(real_storage):
    storage, name, path, delta, expected, digest = real_storage
    before = path.read_bytes()
    storage._publications = {}
    storage._created[name] = datetime.now(UTC)
    storage._expiry[name] = datetime.now(UTC) + timedelta(hours=1)
    ctx = SimpleNamespace(_storage=storage, _loaded_datasets={name},
                          prefer_local_if_exists=True, use_local_data_only=True,
                          force_cloud_download=True)
    wrong_hash = "0000000" if digest != "0000000" else "1111111"

    def get(key, *, force_download, progress, progress_callback):
        assert not force_download
        assert not storage.merge_recent_delta(key, delta, wrong_hash)
        # Simulate the SDK's existing full-source branch and publication marker.
        storage._publications[key] = (storage.get_time_created(key), {"content_hash": digest})
        return expected

    sdk = SimpleNamespace(__name__="finlab.data", _default_context=ctx, get=get)
    frame, evidence = checked_get(name, sdk=sdk)
    assert path.read_bytes() == before
    assert evidence["source_check_mode"] == "upstream_forced"
    assert evidence["recent_merge_recovery"]["sdk_hash_verified"] is False
    assert evidence["recent_merge_recovery"]["sdk_full_fallback_after_hash_mismatch"] is True
    assert "merge_recent_delta" not in vars(storage)
    pd.testing.assert_frame_equal(frame, expected)


def test_unverified_sdk_version_does_not_modify_storage(real_storage, monkeypatch):
    storage, name, path, delta, _, digest = real_storage
    before = path.read_bytes()
    monkeypatch.setattr("scripts.finlab_recent_null_recovery.version", lambda _: "9.0.0")
    with recent_null_recovery(storage, name) as evidence:
        with pytest.raises(pa.ArrowNotImplementedError):
            storage.merge_recent_delta(name, delta, digest)
    assert evidence == {"recovered": False, "hash_mismatch": False}
    assert path.read_bytes() == before
    assert "merge_recent_delta" not in vars(storage)
