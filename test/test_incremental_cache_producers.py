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
    monkeypatch.setattr(physical, "_price_limits", unexpected)
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


@pytest.mark.parametrize("change", ["opens", "volumes", "twse_receipt"])
def test_unconsumed_price_limit_dependency_reuses_complete_price_payload(tmp_path, monkeypatch, change):
    panel, public, minute = _physical_source_inputs(tmp_path)
    cache = tmp_path / "cache"
    _, _, first = _build(panel, public, minute, cache)
    price_root, = cache.glob("physical-price-limits-*")
    before = {path.name: (path.stat().st_ino, path.read_bytes())
              for path in price_root.iterdir()}
    if change == "opens":
        panel.open_prices = panel.open_prices.copy()
        panel.open_prices[0, 0] += 1.
    elif change == "volumes":
        panel.daily_volumes = panel.daily_volumes.copy()
        panel.daily_volumes[0, 0] += 1.
    else:
        import polars as pl
        path = public / "twse_daily_ohlcv.parquet"
        pl.read_parquet(path).with_columns(audit_revision=pl.lit(1)).write_parquet(path)

    def unexpected(*args, **kwargs):
        raise AssertionError("limits must reuse unchanged consumed inputs")

    monkeypatch.setattr(physical, "_price_limits", unexpected)
    _, _, current = _build(panel, public, minute, cache)
    assert current["source_digest"] != first["source_digest"]
    assert list(cache.glob("physical-price-limits-*")) == [price_root]
    assert before == {path.name: (path.stat().st_ino, path.read_bytes())
                      for path in price_root.iterdir()}


@pytest.mark.parametrize("change", ["tpex", "corporate", "closes", "dates", "symbols"])
def test_consumed_price_limit_dependency_rebuilds_and_matches_full_oracle(tmp_path, monkeypatch, change):
    panel, public, _ = _physical_source_inputs(tmp_path)
    dates, symbols = panel.dates.copy(), tuple(panel.symbols)
    closes = np.asarray(panel.close_prices, dtype=np.float64).copy()
    cache = tmp_path / "cache"

    def build():
        receipts = {name: {"sha256": physical._sha256(public / filename)}
                    for name, filename in (
                        ("tpex_daily", "tpex_daily_ohlcv.parquet"),
                        ("corporate_reference", "tw_corporate_action_reference.parquet"))}
        return physical._load_or_build_price_limit_cache(
            cache_dir=cache, public_root=public, dates=dates, symbols=symbols, closes=closes,
            input_digest=physical._price_limit_input_digest(
                receipts=receipts, dates=dates, symbols=symbols, closes=closes))

    original = build()
    if change in {"tpex", "corporate"}:
        import polars as pl
        path = public / ("tpex_daily_ohlcv.parquet" if change == "tpex"
                         else "tw_corporate_action_reference.parquet")
        column = "次日漲停價" if change == "tpex" else "reference_price"
        value = "56.0" if change == "tpex" else 80.
        pl.read_parquet(path).with_columns(pl.lit(value).alias(column)).write_parquet(path)
    elif change == "closes":
        closes[0, 0] += 20.
    elif change == "dates":
        dates[-1] += np.timedelta64(1, "D")
    else:
        symbols = symbols[::-1]
        closes = closes[:, ::-1]
    expected = physical._price_limits(public_root=public, dates=dates, symbols=symbols, closes=closes)
    calls = []
    price_limits = physical._price_limits

    def counted(**kwargs):
        calls.append(True)
        return price_limits(**kwargs)

    monkeypatch.setattr(physical, "_price_limits", counted)
    actual = build()
    assert calls == [True]
    assert len(list(cache.glob("physical-price-limits-*/READY.json"))) == 2
    for result, oracle in zip(actual[:3], expected[:3]):
        assert result.dtype == oracle.dtype and result.shape == oracle.shape
        assert result.tobytes() == oracle.tobytes()
    assert actual[3] == expected[3]
    if change in {"tpex", "corporate", "closes", "symbols"}:
        assert any(a.tobytes() != b.tobytes() for a, b in zip(original[:3], actual[:3]))
    build()
    assert calls == [True]


def test_consumed_input_cache_still_rejects_same_size_payload_corruption(tmp_path, monkeypatch):
    panel, public, minute = _physical_source_inputs(tmp_path)
    cache = tmp_path / "cache"
    _build(panel, public, minute, cache)
    price_root, = cache.glob("physical-price-limits-*")
    payload = price_root / "upper.npy"
    expected = payload.read_bytes()
    payload.write_bytes(expected[:-1] + bytes([expected[-1] ^ 1]))
    calls = []
    price_limits = physical._price_limits

    def counted(**kwargs):
        calls.append(True)
        return price_limits(**kwargs)

    monkeypatch.setattr(physical, "_price_limits", counted)
    _build(panel, public, minute, cache)
    assert calls == [True] and payload.read_bytes() == expected


def test_interrupted_build_resumes_verified_sessions_before_global_commit(tmp_path, monkeypatch):
    panel, public, minute = _physical_source_inputs(tmp_path)
    _, _, reference = _build(panel, public, minute, tmp_path / "reference")
    atomic_npy = physical._atomic_npy

    def interrupt(path, *args, **kwargs):
        if path.name == "source_gaps.npy":
            raise KeyboardInterrupt("interrupted after the durable session")
        return atomic_npy(path, *args, **kwargs)

    monkeypatch.setattr(physical, "_atomic_npy", interrupt)
    with pytest.raises(KeyboardInterrupt):
        _build(panel, public, minute, tmp_path / "resume")
    root = tmp_path / "resume" / ("physical-" + reference["source_digest"])
    payload = root / "session-2020-03-02.npz"
    before = payload.read_bytes()
    inode = payload.stat().st_ino
    assert not (root / "READY.json").exists()
    assert not (root / "manifest.json").exists()
    monkeypatch.setattr(physical, "_atomic_npy", atomic_npy)

    def unexpected(*args, **kwargs):
        raise AssertionError("verified interrupted session must not be parsed or serialized again")

    monkeypatch.setattr(physical, "_dense_bars", unexpected)
    monkeypatch.setattr(reuse_module.PhysicalSessionReuse, "write", unexpected)
    _, _, resumed = _build(panel, public, minute, tmp_path / "resume")
    assert payload.read_bytes() == before and payload.stat().st_ino == inode
    assert resumed["storage_build"]["resumed_input_sessions"] == 1
    assert resumed["storage_build"]["written_sessions"] == 0
    for key in ("files", "session_builds", "mode_counts", "source_gap_symbol_days",
                "unresolved_action_gap_symbol_days", "internal_unknown_valuation"):
        assert resumed[key] == reference[key]
    assert (root / "READY.json").is_file()


def _interrupted_session_store(tmp_path):
    current = tmp_path / ("physical-" + "a" * 64)
    current.mkdir()
    store = reuse_module.PhysicalSessionReuse(tmp_path, current, 1)
    path = current / "session-2020-03-02.npz"
    member = store.write(path, {"values": np.arange(1024)})
    build = {"input_key": "b" * 64,
             "diagnostics": reuse_module.session_diagnostics(
                 np.array([True]), np.array([False]),
                 {name: int(name == "minute_symbol_days") for name in reuse_module.COUNT_NAMES})}
    store.checkpoint(path, member, build)
    return current, path, store, member, build


def test_unrelated_generation_change_reuses_interrupted_session_before_parse(tmp_path, monkeypatch):
    panel, public, minute = _physical_source_inputs(tmp_path)
    _, _, reference = _build(panel, public, minute, tmp_path / "reference")
    atomic_npy = physical._atomic_npy

    def interrupt(path, *args, **kwargs):
        if path.name == "source_gaps.npy":
            raise KeyboardInterrupt("durable sessions precede the global commit")
        return atomic_npy(path, *args, **kwargs)

    monkeypatch.setattr(physical, "_atomic_npy", interrupt)
    with pytest.raises(KeyboardInterrupt):
        _build(panel, public, minute, tmp_path / "cache")
    donor = tmp_path / "cache" / ("physical-" + reference["source_digest"])
    payload = donor / "session-2020-03-02.npz"
    before = payload.read_bytes()
    journal = (donor / ".session-builds.jsonl").read_bytes()
    _change_receipt_only(minute)
    monkeypatch.setattr(physical, "_atomic_npy", atomic_npy)

    def unexpected(*args, **kwargs):
        raise AssertionError("unchanged interrupted input must not be rebuilt after metadata changes")

    monkeypatch.setattr(physical, "_dense_bars", unexpected)
    monkeypatch.setattr(physical, "_price_limits", unexpected)
    monkeypatch.setattr(reuse_module.PhysicalSessionReuse, "write", unexpected)
    _, current, resumed = _build(panel, public, minute, tmp_path / "cache")
    assert reference["source_digest"] != resumed["source_digest"]
    assert os.path.samefile(payload, current.parent / payload.name)
    assert payload.read_bytes() == before
    assert (donor / ".session-builds.jsonl").read_bytes() == journal
    assert not (donor / "READY.json").exists() and not (donor / "manifest.json").exists()
    assert resumed["files"] == reference["files"]
    assert resumed["session_builds"] == reference["session_builds"]
    assert resumed["storage_build"]["reused_checkpoint_sessions"] == 1
    assert resumed["storage_build"]["resumed_input_sessions"] == 0
    assert resumed["storage_build"]["written_sessions"] == 0


@pytest.mark.parametrize("damage", ["journal", "payload", "writable", "input_key", "generation", "symbols", "busy"])
def test_incomplete_donor_requires_unchanged_proof_payload_input_and_owner(tmp_path, damage):
    donor, payload, _, _, build = _interrupted_session_store(tmp_path)
    current = tmp_path / ("physical-" + "d" * 64)
    current.mkdir()
    journal = donor / ".session-builds.jsonl"
    if damage in {"input_key", "generation", "symbols"}:
        record = json.loads(journal.read_bytes())
        if damage == "input_key":
            record["build"]["input_key"] = "c" * 64
        elif damage == "generation":
            record["source_digest"] = "c" * 64
        else:
            record["symbols"] = 2
        journal.write_text(json.dumps(record) + "\n")
    store = reuse_module.PhysicalSessionReuse(tmp_path, current, 1)
    if damage == "journal":
        with journal.open("ab") as stream:
            stream.write(b"\n")
    elif damage in {"payload", "writable"}:
        payload.chmod(0o600)
        if damage == "payload":
            raw = payload.read_bytes()
            payload.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
            payload.chmod(0o400)
    destination = current / payload.name
    if damage == "busy":
        with reuse_module._writer_lock(tmp_path / f".{donor.name}.lock"):
            assert store.reuse_input(build["input_key"], destination) is None
    else:
        assert store.reuse_input(build["input_key"], destination) is None
    assert not destination.exists()
    assert not (donor / "READY.json").exists()


def test_checkpoint_truncated_tail_does_not_lose_completed_records_or_future_append(tmp_path):
    current, path, _, member, build = _interrupted_session_store(tmp_path)
    journal = current / ".session-builds.jsonl"
    with journal.open("ab") as stream:
        stream.write(b'{"contract":')
    resumed = reuse_module.PhysicalSessionReuse(tmp_path, current, 1)
    assert resumed.reuse_input(build["input_key"], path)["file"] == member
    other = current / "session-2020-03-03.npz"
    other_member = resumed.write(other, {"values": np.arange(1024) + 1})
    other_build = {**build, "input_key": "c" * 64}
    resumed.checkpoint(other, other_member, other_build)
    again = reuse_module.PhysicalSessionReuse(tmp_path, current, 1)
    assert again.reuse_input(build["input_key"], path)["file"] == member
    assert again.reuse_input(other_build["input_key"], other)["file"] == other_member
    assert not (current / "READY.json").exists()


@pytest.mark.parametrize("damage", ["payload", "writable", "key", "diagnostics", "generation"])
def test_interrupted_session_proof_rejects_changed_dependencies_or_payload(tmp_path, damage):
    current, path, _, _, build = _interrupted_session_store(tmp_path)
    if damage in {"payload", "writable"}:
        path.chmod(0o600)
        if damage == "payload":
            raw = path.read_bytes()
            path.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
            path.chmod(0o400)
    else:
        journal = current / ".session-builds.jsonl"
        record = json.loads(journal.read_text().strip())
        if damage == "key":
            record["build"]["input_key"] = "d" * 64
        elif damage == "diagnostics":
            record["build"]["diagnostics"]["gaps"] = "ff"
        else:
            record["source_digest"] = "f" * 64
        journal.write_text(json.dumps(record) + "\n")
    resumed = reuse_module.PhysicalSessionReuse(tmp_path, current, 1)
    assert resumed.reuse_input(build["input_key"], path) is None


def test_checkpoint_symlink_never_reads_or_overwrites_other_owner(tmp_path):
    current, path, _, member, build = _interrupted_session_store(tmp_path)
    journal = current / ".session-builds.jsonl"
    foreign = tmp_path / "another-owner"
    foreign.write_bytes(journal.read_bytes())
    journal.unlink()
    journal.symlink_to(foreign)
    before = foreign.read_bytes()
    resumed = reuse_module.PhysicalSessionReuse(tmp_path, current, 1)
    assert resumed.reuse_input(build["input_key"], path) is None
    with pytest.raises((ValueError, OSError)):
        resumed.checkpoint(path, member, build)
    assert foreign.read_bytes() == before


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
