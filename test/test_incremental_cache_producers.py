"""Exercise canonical producers, not a periodic duplicate cleaner."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os

import numpy as np
import pytest

from stockagent.data import panel_cache
from stockagent.data import tw_day_trade_carry_source as physical
from stockagent.data.panel import PanelData
from stockagent.data_sync import physical_cache_reuse as reuse_module
from stockagent.data_sync.immutable_cache_files import link_immutable_payload
from test_day_trade_carry_source import _physical_source_inputs


def _build(panel, public, minute, cache):
    source = physical.build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=cache, allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close", corporate_action_mode="avoid",
    )
    path = source.audit_receipt["cache_manifest"]
    from pathlib import Path
    return source, Path(path), json.loads(Path(path).read_bytes())


def _change_receipt_only(minute):
    path = minute / "manifest.json"
    receipt = json.loads(path.read_bytes())
    receipt["audit_revision"] = receipt.get("audit_revision", 0) + 1
    path.write_text(json.dumps(receipt))


def test_global_receipt_revision_reuses_session_before_parse_or_serialization(tmp_path, monkeypatch):
    panel, public, minute = _physical_source_inputs(tmp_path)
    _, first_path, first = _build(panel, public, minute, tmp_path / "cache")
    _change_receipt_only(minute)
    def unexpected(*args, **kwargs):
        raise AssertionError("unchanged historical session must not be rebuilt or serialized")
    monkeypatch.setattr(physical, "_dense_bars", unexpected)
    monkeypatch.setattr(reuse_module.PhysicalSessionReuse, "write", unexpected)
    _, second_path, second = _build(panel, public, minute, tmp_path / "cache")
    name = "session-2020-03-02.npz"
    assert first["source_digest"] != second["source_digest"]
    assert os.path.samefile(first_path.parent / name, second_path.parent / name)
    assert second["storage_build"]["reused_input_sessions"] == 1
    assert second["storage_build"]["written_sessions"] == 0
    assert second["storage_build"]["written_payload_bytes"] == 0
    assert first["files"] == second["files"]
    for key in ("mode_counts", "source_gap_symbol_days", "unresolved_action_gap_symbol_days",
                "internal_unknown_valuation", "official_no_regular_execution"):
        assert first[key] == second[key]


def test_append_day_builds_only_new_session_and_keeps_exact_historical_payload(tmp_path, monkeypatch):
    panel, public, minute = _physical_source_inputs(tmp_path)
    _, first_path, first = _build(panel, public, minute, tmp_path / "cache")
    panel.dates = np.append(panel.dates, np.datetime64("2020-03-03", "D"))
    for name in ("open_prices", "close_prices", "daily_volumes", "corporate_action_avoidance_mask",
                 "unresolved_corporate_action_mask", "force_exit_mask"):
        value = getattr(panel, name)
        setattr(panel, name, np.concatenate([value, value[-1:]], axis=0))
    panel.open_prices[-1, 2] = panel.close_prices[-1, 2] = 121.0
    partition = minute / "trade_date=2020-03-03/data.parquet"
    partition.parent.mkdir()
    partition.write_bytes((minute / "trade_date=2020-03-02/data.parquet").read_bytes())
    path = minute / "manifest.json"
    receipt = json.loads(path.read_bytes())
    receipt["partitions"].append({
        "status": "ok", "trade_date": "2020-03-03",
        "output": "trade_date=2020-03-03/data.parquet",
        "output_sha256": hashlib.sha256(partition.read_bytes()).hexdigest(),
    })
    path.write_text(json.dumps(receipt))
    parsed = []
    original = physical._dense_bars
    def parse(path, symbols):
        parsed.append(path.parent.name)
        return original(path, symbols)
    monkeypatch.setattr(physical, "_dense_bars", parse)
    _, second_path, second = _build(panel, public, minute, tmp_path / "cache")
    assert parsed == ["trade_date=2020-03-03"]
    assert second["storage_build"]["reused_input_sessions"] == 1
    assert second["storage_build"]["written_sessions"] == 1
    name = "session-2020-03-02.npz"
    assert os.path.samefile(first_path.parent / name, second_path.parent / name)
    assert first["files"][name] == second["files"][name]


@pytest.mark.parametrize("change", ["opening_marks", "symbols", "partition"])
def test_changed_actual_session_dependency_never_uses_input_hit(tmp_path, monkeypatch, change):
    panel, public, minute = _physical_source_inputs(tmp_path)
    if change == "opening_marks":
        panel.open_prices[1, 0] = np.nan
    _build(panel, public, minute, tmp_path / "cache")
    if change == "opening_marks":
        panel.close_prices[0, 0] = 101.0
    elif change == "symbols":
        order = [1, 0, 2]
        panel.symbols = [panel.symbols[i] for i in order]
        for name in ("open_prices", "close_prices", "daily_volumes", "corporate_action_avoidance_mask",
                     "unresolved_corporate_action_mask", "force_exit_mask"):
            setattr(panel, name, getattr(panel, name)[:, order])
    else:
        import pyarrow.parquet as pq
        import pyarrow as pa
        partition = minute / "trade_date=2020-03-02/data.parquet"
        rows = pq.ParquetFile(partition).read().to_pylist()
        for row in rows:
            if row["symbol"] == "2330":
                for field in ("Open", "High", "Low", "Close"):
                    row[field] = 101.0
                row["Amount"] = row["Close"] * row["volume_shares"]
        pq.write_table(pa.Table.from_pylist(rows), partition)
        path = minute / "manifest.json"
        receipt = json.loads(path.read_bytes())
        receipt["partitions"][0]["output_sha256"] = hashlib.sha256(partition.read_bytes()).hexdigest()
        path.write_text(json.dumps(receipt))
    parsed = []
    original = physical._dense_bars
    monkeypatch.setattr(physical, "_dense_bars", lambda *args: (parsed.append(True), original(*args))[1])
    _, _, result = _build(panel, public, minute, tmp_path / "cache")
    assert parsed == [True]
    assert result["storage_build"]["reused_input_sessions"] == 0


def test_source_partition_sha_is_required_even_with_valid_reusable_projection(tmp_path):
    panel, public, minute = _physical_source_inputs(tmp_path)
    _build(panel, public, minute, tmp_path / "cache")
    _change_receipt_only(minute)
    path = minute / "trade_date=2020-03-02/data.parquet"
    path.write_bytes(path.read_bytes() + b"broken")
    with pytest.raises(RuntimeError, match="partition differs"):
        _build(panel, public, minute, tmp_path / "cache")


def test_reused_nonzero_gap_masks_and_global_diagnostics_remain_exact(tmp_path, monkeypatch):
    panel, public, minute = _physical_source_inputs(tmp_path)
    panel.daily_volumes[1, 0] = np.nan
    _, _, first = _build(panel, public, minute, tmp_path / "cache")
    assert first["source_gap_symbol_days"] == 1
    _change_receipt_only(minute)
    def unexpected(*args, **kwargs):
        raise AssertionError("unchanged missingness must reuse its verified diagnostics")
    monkeypatch.setattr(physical, "_dense_bars", unexpected)
    _, _, second = _build(panel, public, minute, tmp_path / "cache")
    assert first["files"] == second["files"]
    assert first["mode_counts"] == second["mode_counts"]
    assert first["internal_unknown_valuation"] == second["internal_unknown_valuation"]
    assert second["source_gap_symbol_days"] == 1


def test_busy_donor_falls_back_without_deadlock_or_modifying_donor(tmp_path):
    panel, public, minute = _physical_source_inputs(tmp_path)
    _, path, first = _build(panel, public, minute, tmp_path / "cache")
    _change_receipt_only(minute)
    from stockagent.data_sync.physical_cache_compaction import _writer_lock
    with _writer_lock(path.parent.parent / f".{path.parent.name}.lock"):
        _, _, result = _build(panel, public, minute, tmp_path / "cache")
    assert result["storage_build"]["written_sessions"] == 1
    assert first["files"] == result["files"]


def test_legacy_npz_content_reuse_has_identical_bytes_and_no_disk_payload_write(tmp_path, monkeypatch):
    from test_physical_cache_compaction import _generation
    root = tmp_path / "cache"
    donor = _generation(root, "a")
    source = donor / "session-2026-10-13.npz"
    source.chmod(0o400)
    target = root / ("physical-" + "b" * 64)
    target.mkdir()
    store = reuse_module.PhysicalSessionReuse(root, target, 1)
    def unexpected(*args, **kwargs):
        raise AssertionError("identical existing NPZ must not be written to disk")
    monkeypatch.setattr(reuse_module, "atomic_write_bytes", unexpected)
    path = target / source.name
    result = store.write(path, {"values": np.arange(1024) + 7})
    assert result["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert os.path.samefile(source, path)
    assert store.statistics()["reused_content_sessions"] == 1


def _panel():
    masks = np.ones((3, 2), dtype=bool)
    return PanelData(
        dates=np.arange(3).astype("datetime64[D]"), symbols=["A", "B"], feature_names=["f"],
        features=np.arange(6, dtype=np.float32).reshape(3, 2, 1),
        returns_1d=np.zeros((3, 2), dtype=np.float32), tradable_mask=masks,
        can_buy_mask=masks.copy(), can_sell_mask=masks.copy(), alive_mask=masks.copy(),
        benchmark_returns=np.zeros(3, dtype=np.float32), close_prices=np.ones((3, 2), dtype=np.float32),
    )


def test_panel_variants_share_unchanged_arrays_before_memmap_write(tmp_path, monkeypatch):
    panel = _panel()
    panel_cache.save_panel_cache_v2(tmp_path, panel, source_hash="a", backend_key="a", version=123)
    before = panel_cache.read_panel_cache_v2_meta(tmp_path)
    panel = replace(panel, features=panel.features + 1)
    calls = []
    original = np.lib.format.open_memmap
    def record(path, *args, **kwargs):
        if kwargs.get("mode") == "w+":
            calls.append(str(path))
        return original(path, *args, **kwargs)
    monkeypatch.setattr(np.lib.format, "open_memmap", record)
    panel_cache.save_panel_cache_v2(tmp_path, panel, source_hash="a", backend_key="b", version=123)
    after = panel_cache.read_panel_cache_v2_meta(tmp_path)
    assert len(calls) == 1 and "features.npy" in calls[0]
    root = panel_cache.panel_cache_v2_dir(tmp_path)
    for name in before["arrays"]:
        if name != "features":
            assert os.path.samefile(root / before["arrays"][name]["file"], root / after["arrays"][name]["file"])
    assert after["storage_build"]["written_arrays"] == 1
    assert after["storage_build"]["reused_arrays"] == len(after["arrays"]) - 1
    loaded = panel_cache.load_panel_cache_v2(tmp_path, source_hash="a", backend_key="a", version=123)
    np.testing.assert_array_equal(loaded["features"], panel.features - 1)


def test_shared_panel_survives_replacement_of_its_donor_generation(tmp_path):
    panel = _panel()
    for source, backend in (("a", "a"), ("a", "b"), ("new", "a")):
        panel_cache.save_panel_cache_v2(tmp_path, panel, source_hash=source, backend_key=backend, version=123)
    loaded = panel_cache.load_panel_cache_v2(tmp_path, source_hash="a", backend_key="b", version=123)
    np.testing.assert_array_equal(loaded["features"], panel.features)


@pytest.mark.parametrize("kind", ["writable", "symlink", "wrong-sha"])
def test_shared_link_never_modifies_an_unsupported_donor(tmp_path, kind):
    source = tmp_path / "source"
    source.write_bytes(b"same")
    source.chmod(0o600 if kind == "writable" else 0o400)
    path = source
    if kind == "symlink":
        path = tmp_path / "alias"
        path.symlink_to(source)
    digest = hashlib.sha256(b"wrong" if kind == "wrong-sha" else b"same").hexdigest()
    assert not link_immutable_payload(path, tmp_path / "destination", sha256=digest, size=4)
    assert source.read_bytes() == b"same"
    assert not (tmp_path / "destination").exists()


def test_failed_shared_publication_preserves_donor_and_leaves_no_partial_name(tmp_path, monkeypatch):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"unchanged")
    source.chmod(0o400)
    import stockagent.data_sync.immutable_cache_files as files
    def fail(*args, **kwargs):
        raise OSError("injected shared publication failure")
    monkeypatch.setattr(files.os, "replace", fail)
    with pytest.raises(OSError, match="publication failure"):
        link_immutable_payload(source, target, sha256=hashlib.sha256(b"unchanged").hexdigest(), size=9)
    assert source.read_bytes() == b"unchanged" and source.stat().st_nlink == 1
    assert not target.exists() and not list(tmp_path.glob("*.tmp"))
