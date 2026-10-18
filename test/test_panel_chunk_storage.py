"""Exercise the native producer/reader, including real mmap ownership and COW."""
from __future__ import annotations

from dataclasses import replace
import ctypes
import gc
import hashlib
import json
import os
import sys

import numpy as np
import pytest

from stockagent.data import panel_cache, panel_chunks
from stockagent.data.panel import PanelData


@pytest.fixture(autouse=True)
def small_chunks(monkeypatch):
    if sys.platform != "linux" or ctypes.sizeof(ctypes.c_void_p) != 8:
        pytest.skip("native 64-bit Linux chunk persistence contract")
    monkeypatch.setenv("STOCKAGENT_PANEL_CHUNK_STORAGE", "1")
    monkeypatch.setattr(panel_chunks, "MIN_CHUNKED_BYTES", 8192)
    monkeypatch.setattr(panel_chunks, "CHUNK_BYTES", 8192)


def panel(days=61):
    values = np.arange(days * 17 * 7, dtype=np.float32).reshape(days, 17, 7)
    # Preserve NaN payload, infinities, subnormal and signed-zero bits.
    values.view(np.uint32).flat[:6] = [0x80000000, 0x7FC00123, 0x7F800000,
                                      0xFF800000, 1, 0]
    mask = np.ones((days, 17), dtype=bool)
    return PanelData(
        dates=np.arange(days).astype("datetime64[D]"),
        symbols=[str(index) for index in range(17)],
        feature_names=[f"f{index}" for index in range(7)], features=values,
        returns_1d=np.zeros(mask.shape, dtype=np.float32), tradable_mask=mask,
        alive_mask=mask.copy(), benchmark_returns=np.zeros(days, dtype=np.float32),
        close_prices=np.ones(mask.shape, dtype=np.float32),
    )


def save(root, value, source="a", backend="same"):
    panel_cache.save_panel_cache_v2(root, value, source_hash=source,
                                   backend_key=backend, version=123)
    cache = panel_cache.panel_cache_v2_dir(root)
    meta = panel_cache.read_panel_cache_v2_meta(root)
    _, descriptor = panel_chunks.read_descriptor(cache, meta["arrays"]["features"])
    return cache, meta, descriptor


@pytest.mark.parametrize("mode", ["r", "c", None])
def test_contiguous_round_trip_all_bits_and_ownership(tmp_path, mode):
    original = panel()
    cache, meta, descriptor = save(tmp_path, original)
    result = panel_cache.load_panel_cache_v2(tmp_path, mmap_mode=mode)["features"]
    assert isinstance(result, np.memmap) == (mode is not None)
    assert result.flags.c_contiguous
    assert result.tobytes() == original.features.tobytes()
    assert panel_cache.array_content_fingerprint(result) == meta["arrays"]["features"]["content_fingerprint"]
    assert all((cache / chunk["file"]).stat().st_mode & 0o222 == 0
               for chunk in descriptor["chunks"])
    if mode == "r":
        with pytest.raises(ValueError):
            result.setflags(write=True)
    else:
        result[-1, 0, 0] = -997
        fresh = panel_cache.load_panel_cache_v2(tmp_path)["features"]
        assert fresh.tobytes() == original.features.tobytes()
    view = result[5:40, :, :]
    expected = view.tobytes()
    del result
    gc.collect()
    assert view.tobytes() == expected  # A child keeps the full reservation alive.
    assert np.asarray(view).tobytes() == expected


def test_append_and_historical_correction_write_only_changed_chunks(tmp_path):
    cache, first, descriptor = save(tmp_path, panel())
    inodes = [(cache / chunk["file"]).stat().st_ino for chunk in descriptor["chunks"]]
    old = panel_cache.load_panel_cache_v2(tmp_path)["features"]
    old_bytes = old.tobytes()
    _, second, appended = save(tmp_path, panel(62), source="b")
    counters = second["storage_build"]
    assert counters["reused_chunks"] == len(descriptor["chunks"]) - 1
    assert counters["written_chunks"] == 1
    assert counters["written_chunk_bytes"] <= panel_chunks.CHUNK_BYTES
    assert [(cache / chunk["file"]).stat().st_ino for chunk in appended["chunks"][:-1]] == inodes[:-1]
    assert not (cache / "generations" / first["generation"]).exists()
    assert old.tobytes() == old_bytes  # Unlink of prior names cannot invalidate readers.
    changed = panel(62)
    changed.features[20, 4, 2] = -123
    _, third, corrected = save(tmp_path, changed, source="c")
    assert third["storage_build"]["written_chunks"] == 1
    assert third["storage_build"]["reused_chunks"] == len(corrected["chunks"]) - 1
    loaded = panel_cache.load_panel_cache_v2(tmp_path)["features"]
    assert loaded.tobytes() == changed.features.tobytes()


def test_variants_reuse_chunks_and_pinned_manifest_loads_them(tmp_path):
    cache, first, _ = save(tmp_path, panel(), backend="a")
    _, second, _ = save(tmp_path, panel(), backend="b")
    assert second["storage_build"]["written_chunks"] == 0
    assert second["storage_build"]["reused_chunks"] > 1
    receipt = panel_cache.panel_cache_v2_variant_meta_path(
        tmp_path, source_hash="a", backend_key="a", version=123,
    )
    loaded = panel_cache.load_panel_cache_v2_manifest(
        receipt, expected_generation=first["generation"], expected_version=123,
    )
    assert loaded["features"].tobytes() == panel().features.tobytes()
    assert panel_cache.panel_cache_v2_is_valid(
        tmp_path, source_hash="a", backend_key="b", version=123, source_paths=[],
    )


def test_shared_payload_mtime_does_not_make_new_commit_permanently_stale(tmp_path):
    source = tmp_path / "source.parquet"
    source.write_bytes(b"one")
    cache, first, _ = save(tmp_path, panel(), backend="a")
    old_time = (cache / first["arrays"]["features"]["file"]).stat().st_mtime_ns
    source.write_bytes(b"two")
    os.utime(source, ns=(old_time + 1_000_000, old_time + 1_000_000))
    save(tmp_path, panel(62), source="b", backend="a")
    assert panel_cache.panel_cache_v2_is_valid(
        tmp_path, source_hash="b", backend_key="a", version=123, source_paths=[source],
    )


@pytest.mark.parametrize("damage", ["missing", "truncate", "writable", "symlink"])
def test_damaged_or_redirected_chunk_is_rejected(tmp_path, damage):
    cache, _, descriptor = save(tmp_path, panel())
    path = cache / descriptor["chunks"][1]["file"]
    if damage in ("missing", "symlink"):
        path.unlink()
        if damage == "symlink":
            path.symlink_to(cache / descriptor["chunks"][0]["file"])
    else:
        path.chmod(0o600)
        if damage == "truncate":
            with path.open("r+b") as stream:
                stream.truncate(4)
            path.chmod(0o400)
    with pytest.raises((ValueError, FileNotFoundError)):
        panel_cache.load_panel_cache_v2(tmp_path)


@pytest.mark.parametrize("damage", ["offset", "shape", "path", "overrun", "dtype"])
def test_malformed_descriptor_rejected_before_mapping(tmp_path, damage, monkeypatch):
    cache, meta, descriptor = save(tmp_path, panel())
    if damage == "offset":
        descriptor["chunks"][1]["offset"] += 4096
    elif damage == "shape":
        descriptor["shape"][0] += 1
    elif damage == "path":
        descriptor["chunks"][0]["file"] = "../outside.bin"
    elif damage == "overrun":
        descriptor["chunks"][-1]["logical_bytes"] += 4096
        descriptor["chunks"][-1]["stored_bytes"] += 4096
    else:
        descriptor["dtype"] = "object"
    (cache / meta["arrays"]["features"]["file"]).write_text(json.dumps(descriptor))
    def unexpected(*args, **kwargs):
        raise AssertionError("mapping a malformed descriptor is forbidden")
    monkeypatch.setattr(panel_chunks, "_open_chunk", unexpected)
    with pytest.raises(ValueError):
        panel_cache.load_panel_cache_v2(tmp_path)


def test_rplus_rejected_and_noncontiguous_or_object_uses_existing_layout(tmp_path):
    save(tmp_path, panel())
    with pytest.raises(ValueError, match="immutable Panel chunks"):
        panel_cache.load_panel_cache_v2(tmp_path, mmap_mode="r+")
    value = panel()
    value = replace(value, features=np.asfortranarray(value.features))
    panel_cache.save_panel_cache_v2(tmp_path, value, source_hash="f", backend_key="fortran", version=123)
    meta = panel_cache.read_panel_cache_v2_meta(tmp_path)
    assert "storage" not in meta["arrays"]["features"]
    assert panel_cache.load_panel_cache_v2(tmp_path)["features"].tobytes() == value.features.tobytes()
    assert not panel_chunks.can_chunk(np.ones(10000, dtype=object))


def test_reused_donor_content_is_verified_not_trusted_from_metadata(tmp_path):
    cache, _, descriptor = save(tmp_path, panel())
    chunk = descriptor["chunks"][1]
    path = cache / chunk["file"]
    path.chmod(0o600)
    with path.open("r+b") as stream:
        stream.write(b"incorrect")
    path.chmod(0o400)
    assert hashlib.sha256(path.read_bytes()).hexdigest() != chunk["sha256"]
    _, second, _ = save(tmp_path, panel(), backend="new")
    assert second["storage_build"]["written_chunks"] == 1
    assert panel_cache.load_panel_cache_v2(tmp_path)["features"].tobytes() == panel().features.tobytes()


def test_writer_rollout_is_explicit_and_legacy_reader_stays_compatible(tmp_path, monkeypatch):
    monkeypatch.delenv("STOCKAGENT_PANEL_CHUNK_STORAGE")
    value = panel()
    panel_cache.save_panel_cache_v2(tmp_path, value, source_hash="a", backend_key="a", version=123)
    meta = panel_cache.read_panel_cache_v2_meta(tmp_path)
    item = meta["arrays"]["features"]
    assert "storage" not in item
    assert np.load(panel_cache.panel_cache_v2_dir(tmp_path) / item["file"]).tobytes() == value.features.tobytes()
    monkeypatch.setenv("STOCKAGENT_PANEL_CHUNK_STORAGE", "1")
    panel_cache.save_panel_cache_v2(tmp_path, value, source_hash="b", backend_key="tail",
                                   version=123, chunk_storage=False)
    assert "storage" not in panel_cache.read_panel_cache_v2_meta(tmp_path)["arrays"]["features"]


@pytest.mark.parametrize("mode", ["r", "c", None])
def test_portable_recovery_allocates_explicitly_and_retains_all_values(tmp_path, monkeypatch, mode):
    cache, meta, _ = save(tmp_path, panel())
    monkeypatch.setattr(panel_chunks.mmap, "PAGESIZE", 65536)
    result = panel_chunks.load_chunks(cache, meta["arrays"]["features"], mmap_mode=mode)
    assert type(result) is np.ndarray
    assert result.tobytes() == panel().features.tobytes()
    assert result.flags.writeable == (mode != "r")


@pytest.mark.parametrize("dtype", [">f4", "<f8", "i8", "bool"])
def test_dtype_and_cross_chunk_element_bits_preserved(tmp_path, dtype):
    value = panel(123)
    with np.errstate(invalid="ignore"):
        converted = np.nan_to_num(value.features, nan=1, posinf=2, neginf=-3).astype(dtype)
    value = replace(value, features=converted)
    save(tmp_path, value)
    result = panel_cache.load_panel_cache_v2(tmp_path)["features"]
    assert result.dtype == converted.dtype
    assert result.tobytes() == converted.tobytes()


def test_partial_mapping_failure_closes_all_regions_and_file_descriptors(tmp_path, monkeypatch):
    cache, meta, _ = save(tmp_path, panel())
    real_library = panel_chunks.ctypes.CDLL(None, use_errno=True)
    real_mmap = real_library.mmap
    real_mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                         ctypes.c_int, ctypes.c_int, ctypes.c_long]
    real_mmap.restype = ctypes.c_void_p
    calls = []

    class MapWithFailure:
        def __call__(self, address, size, protection, flags, descriptor, offset):
            calls.append(address)
            if len(calls) == 2:
                ctypes.set_errno(12)
                return ctypes.c_void_p(-1).value
            return real_mmap(address, size, protection, flags, descriptor, offset)

    class Library:
        mmap = MapWithFailure()

    monkeypatch.setattr(panel_chunks.ctypes, "CDLL", lambda *args, **kwargs: Library())
    descriptors = len(os.listdir("/proc/self/fd"))
    with pytest.raises(OSError, match="Cannot allocate memory"):
        panel_chunks.load_chunks(cache, meta["arrays"]["features"])
    gc.collect()
    assert len(calls) == 2
    assert len(os.listdir("/proc/self/fd")) == descriptors


def test_failure_before_manifest_commit_preserves_previous_complete_generation(tmp_path, monkeypatch):
    cache, meta, descriptor = save(tmp_path, panel())
    before = (cache / "meta.json").read_bytes()
    hashes = {chunk["file"]: hashlib.sha256((cache / chunk["file"]).read_bytes()).hexdigest()
              for chunk in descriptor["chunks"]}
    write_json = panel_cache._write_json
    def fail(path, payload, **options):
        if path.name == "features.chunks.json":
            raise OSError("injected descriptor publication failure")
        return write_json(path, payload, **options)
    monkeypatch.setattr(panel_cache, "_write_json", fail)
    with pytest.raises(OSError, match="publication failure"):
        save(tmp_path, panel(62), source="b")
    assert (cache / "meta.json").read_bytes() == before
    assert [path.name for path in (cache / "generations").iterdir()] == [meta["generation"]]
    assert hashes == {name: hashlib.sha256((cache / name).read_bytes()).hexdigest() for name in hashes}
    assert panel_cache.load_panel_cache_v2(tmp_path)["features"].tobytes() == panel().features.tobytes()


def test_minute_context_uses_the_shared_chunk_reader_and_previous_session_clock(tmp_path):
    from stockagent.data.tw_minute import _load_minute_daily_feature_context
    original = panel()
    original = replace(original, open_prices=original.close_prices.copy())
    cache, _, _ = save(tmp_path, original, backend="test|benchmark=0")
    context = _load_minute_daily_feature_context(
        cache / "meta.json", minute_symbols=("5", "0"),
        minute_dates=original.dates[-2:], requested_feature_names=("f2", "f4"),
        benchmark_name="0",
    )
    assert context.panel_features.tobytes() == original.features.tobytes()
    assert isinstance(context.panel_features, np.memmap)
    np.testing.assert_array_equal(context.previous_session_indices, [58, 59])
    np.testing.assert_array_equal(context.current_session_indices, [59, 60])
    np.testing.assert_array_equal(context.panel_symbol_indices, [5, 0])
    np.testing.assert_array_equal(context.selected_feature_indices, [2, 4])
