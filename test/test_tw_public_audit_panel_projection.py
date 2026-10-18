"""Audit projection must not change the canonical panel's complete ABI."""
from __future__ import annotations

from dataclasses import asdict, fields
from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts import audit_tw_public_data_layer as audit
from scripts.audit_tw_public_data_layer import _panel_kwargs
from scripts import benchmark_tw_public_audit_projection as benchmark
from scripts import benchmark_panel_producer_projection as producer_benchmark
from stockagent.config import load_config
from stockagent.data import panel as panel_module
from stockagent.data.panel import build_panel


def _fixture(tmp_path: Path):
    stocks = tmp_path / "stocks"
    stocks.mkdir()
    days = [date(2024, 1, n) for n in (2, 3, 4, 5, 8)]
    for index, symbol in enumerate(("2330", "2317", "0050")):
        observed = days if index != 1 else days[:2] + days[3:]
        price = [100.0 + index + row for row in range(len(observed))]
        pl.DataFrame({
            "date": observed, "open": price, "max": [p + 1 for p in price],
            "min": [p - 1 for p in price], "close": price, "adjclose": price,
            "Trading_Volume": [1000.0] * len(observed),
        }).write_parquet(stocks / f"{symbol}_features.parquet")
    external = tmp_path / "public.parquet"
    pl.DataFrame({
        "date": days * 3, "symbol": [s for s in ("2330", "2317", "0050") for _ in days],
        "twpub_keep": [1.0, None, -0.0, 2.5, -3.0] * 3,
        "twpub_drop": [-9.0, 8.0, None, 6.0, 5.0] * 3,
        "twpub_other": [0.0, 0.5, -0.25, 1.0, 0.0] * 3,
        "_twpub_official_traded": [1.0, 1.0, 0.0, 1.0, 1.0] * 3,
        "_twpub_price_limit_upper": [130.123456789] * 15,
        "_twpub_price_limit_lower": [90.123456789] * 15,
        "_twpub_short_capacity_shares_next_session": [13.125678901] * 15,
    }).write_parquet(external)
    cfg = load_config("configs/markets/tw_public.yaml")
    cfg.data.feature_zero_fill = []
    cfg.data.feature_shift_next_session = []
    cfg.data.feature_availability_indicators = []
    cfg.data.panel_start_date = "2024-01-01"
    cfg.data.benchmark_name = "universe_average_return"
    cfg.data.security_filter = "none"
    cfg.data.tradable_mode = "tradable"
    cfg.data.trading_volume_policy = "required"
    cfg.data.panel_backend = "pyarrow"
    cfg.data.panel_load_workers = 0
    cfg.data.use_tw_public_features = True
    cfg.data.use_tw_public_rules = True
    return stocks, external, cfg


def _assert_panel_bits(left, right):
    for field in fields(left):
        first, second = getattr(left, field.name), getattr(right, field.name)
        if isinstance(first, np.ndarray):
            assert isinstance(second, np.ndarray), field.name
            assert first.shape == second.shape, field.name
            assert first.dtype == second.dtype, field.name
            assert first.tobytes() == second.tobytes(), field.name
        else:
            assert first == second, field.name


@pytest.mark.parametrize("include,exclude", [
    (["close_logret_1d", "twpub_keep", "twpub_drop"], ["twpub_drop"]),
    (["twpub_*"], ["twpub_drop", "twpub_other"]),
    (["twpub_keep", "close_logret_1d", "twpub_drop", "twpub_keep"], ["twpub_drop"]),
    ([], ["twpub_drop"]),
    (["*"], ["twpub_drop", "*_raw"]),
    (["open_raw", "close_raw", "twpub_*"], ["twpub_drop", "twpub_other"]),
    (["next_session_open_gap_logret", "twpub_*"], ["twpub_drop"]),
    (["twpub_*", "next_session_open_gap_logret"], ["twpub_drop", "next_session_open_gap_logret"]),
    (["*"], ["twpub_drop", "*gap*"]),
    (["next_session_open_gap_logret"], ["twpub_drop"]),
])
def test_resolved_projection_preserves_every_panel_field(tmp_path, include, exclude):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = include, exclude
    projected = _panel_kwargs(cfg, external)
    original = {**projected, "feature_include": include, "feature_exclude": exclude}
    control = build_panel(stocks, panel_cache_root=tmp_path / "control", **original)
    candidate = build_panel(stocks, panel_cache_root=tmp_path / "projected", **projected)
    _assert_panel_bits(control, candidate)
    assert projected["feature_exclude"] == ()
    assert "twpub_drop" not in projected["feature_include"]
    assert cfg.data.feature_include == include and cfg.data.feature_exclude == exclude


def test_projection_reduces_actual_reads_but_never_rule_columns(tmp_path, monkeypatch):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include = ["twpub_keep", "twpub_drop"]
    cfg.data.feature_exclude = ["twpub_drop"]
    reads = []
    original_read = pq.read_table

    def read(path, *args, **kwargs):
        if Path(path) == external:
            reads.append(kwargs["columns"])
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_table", read)
    build_panel(stocks, panel_cache_root=tmp_path / "cache", **_panel_kwargs(cfg, external))
    assert reads == [["date", "symbol", "twpub_keep", *[
        name for name in pq.read_schema(external).names if name.startswith("_twpub_")]]]


def test_canonical_full_builder_projects_exclusions_without_a_caller_workaround(tmp_path, monkeypatch):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include = ["twpub_*", "close_logret_1d"]
    cfg.data.feature_exclude = ["twpub_drop", "twpub_other"]
    reads = []
    original_read = pq.read_table

    def read(path, *args, **kwargs):
        if Path(path) == external:
            reads.append(kwargs["columns"])
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(pq, "read_table", read)
    build_panel(stocks, panel_cache_root=tmp_path / "canonical", **_panel_kwargs(cfg, external, resolve_projection=False))
    assert reads == [["date", "symbol", "twpub_keep", *[
        name for name in pq.read_schema(external).names if name.startswith("_twpub_")]]]


@pytest.mark.parametrize("private_cache", [False, True])
def test_audit_cache_miss_builds_the_original_canonical_key(tmp_path, monkeypatch, private_cache):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = ["twpub_keep", "twpub_drop"], ["twpub_drop"]
    observed = []
    sentinel = object()
    monkeypatch.setattr(audit, "load_cached_panel", lambda *a, **kw: None)
    monkeypatch.setattr(audit, "build_panel", lambda *a, **kw: (observed.append(kw), sentinel)[1])
    root = tmp_path / "private-cache" if private_cache else None
    actual, source = audit._load_or_build_panel(cfg, stocks, external, build_if_missing=True, panel_cache_root=root)
    assert actual is sentinel and source == "built"
    assert observed == [{**_panel_kwargs(cfg, external, resolve_projection=False), "panel_cache_root": root}]


@pytest.mark.parametrize("tail", [False, True])
@pytest.mark.parametrize("include,exclude", [
    (["close_logret_1d", "twpub_keep", "twpub_drop"], ["twpub_drop"]),
    (["twpub_*"], ["twpub_drop", "twpub_other"]),
    (["twpub_keep", "close_logret_1d", "twpub_drop", "twpub_keep"], ["twpub_drop"]),
    ([], ["twpub_drop"]),
    (["*"], ["twpub_drop", "*_raw"]),
    (["open_raw", "close_raw", "twpub_*"], ["twpub_drop", "twpub_other"]),
    (["next_session_open_gap_logret", "twpub_*"], ["twpub_drop"]),
    (["twpub_*", "next_session_open_gap_logret"], ["twpub_drop", "next_session_open_gap_logret"]),
    (["*"], ["twpub_drop", "*gap*"]),
    (["next_session_open_gap_logret"], ["twpub_drop"]),
])
def test_shared_builder_projection_matches_unprojected_every_field(tmp_path, monkeypatch, tail, include, exclude):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = include, exclude
    kwargs = _panel_kwargs(cfg, external, resolve_projection=False)
    kwargs.pop("panel_backend")
    if tail:
        builder = panel_module.build_tail_panel
        kwargs["tail_rows"] = 20
    else:
        builder = build_panel
        kwargs["panel_cache_root"] = tmp_path / "control"
    with monkeypatch.context() as unprojected:
        unprojected.setattr(panel_module, "_resolve_external_feature_projection", lambda *a, **kw: (None, None))
        control = builder(stocks, **kwargs)
    if not tail:
        kwargs["panel_cache_root"] = tmp_path / "candidate"
    candidate = builder(stocks, **kwargs)
    _assert_panel_bits(control, candidate)


@pytest.mark.parametrize("features,rules", [(True, False), (False, True), (True, True)])
@pytest.mark.parametrize("backend", ["pyarrow", "polars_lazy", "polars_streaming"])
def test_shared_projection_keeps_independent_rules_and_feature_transforms(tmp_path, monkeypatch, features, rules, backend):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include = ["twpub_keep", "body_ratio"] if features else ["body_ratio"]
    cfg.data.feature_exclude = ["twpub_drop"] if features else ["close_logret_1d"]
    cfg.data.use_tw_public_features, cfg.data.use_tw_public_rules = features, rules
    cfg.data.panel_backend = backend
    cfg.data.feature_zero_fill = ["body_ratio"]
    cfg.data.feature_availability_indicators = ["twpub_keep"] if features else ["body_ratio"]
    cfg.data.feature_shift_next_session = ["twpub_keep"] if features else []
    kwargs = _panel_kwargs(cfg, external, resolve_projection=False)
    with monkeypatch.context() as unprojected:
        unprojected.setattr(panel_module, "_resolve_external_feature_projection", lambda *a, **kw: (None, None))
        control = build_panel(stocks, panel_cache_root=tmp_path / "control", **kwargs)
    candidate = build_panel(stocks, panel_cache_root=tmp_path / "candidate", **kwargs)
    _assert_panel_bits(control, candidate)


@pytest.mark.parametrize("include,exclude", [
    (["missing_feature"], ["twpub_drop"]),
    (["twpub_keep"], ["missing_exclude"]),
    (["twpub_keep"], ["twpub_keep"]),
])
def test_canonical_invalid_selection_fails_before_symbol_or_external_payload(tmp_path, monkeypatch, include, exclude):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = include, exclude
    monkeypatch.setattr(pq, "read_table", lambda *a, **kw: pytest.fail("payload read before selection admission"))
    monkeypatch.setattr(panel_module, "_load_symbol_arrays_pyarrow", lambda *a, **kw: pytest.fail("symbol read before admission"))
    with pytest.raises(ValueError):
        build_panel(stocks, panel_cache_root=tmp_path / "cache", **_panel_kwargs(cfg, external, resolve_projection=False))


def test_original_key_cache_hit_does_not_reproject_or_rewrite(tmp_path, monkeypatch):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = ["twpub_*"], ["twpub_drop", "twpub_other"]
    kwargs = _panel_kwargs(cfg, external, resolve_projection=False)
    cache = tmp_path / "cache"
    expected = build_panel(stocks, panel_cache_root=cache, **kwargs)
    before = {p: (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
              for p in cache.rglob("*") if p.is_file()}
    monkeypatch.setattr(panel_module, "_resolve_external_feature_projection", lambda *a, **kw: pytest.fail("cache hit reprojected"))
    monkeypatch.setattr(panel_module, "_load_external_feature_arrays", lambda *a, **kw: pytest.fail("cache hit decoded source"))
    actual = build_panel(stocks, panel_cache_root=cache, **kwargs)
    _assert_panel_bits(expected, actual)
    assert before == {p: (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
                      for p in cache.rglob("*") if p.is_file()}


def test_external_symbol_views_reuse_original_values_and_date_buffers(tmp_path, monkeypatch):
    _, external, _ = _fixture(tmp_path)
    buffers = []
    original = panel_module._external_frame_to_arrays

    def arrays(*args, **kwargs):
        value = original(*args, **kwargs)
        buffers.append(value)
        return value

    monkeypatch.setattr(panel_module, "_external_frame_to_arrays", arrays)
    actual = panel_module._load_external_feature_arrays(external, selected_feature_names=("twpub_keep",))
    for payloads, (dates, values) in zip((actual.by_symbol, actual.by_symbol_rules), buffers, strict=True):
        assert payloads
        for member_dates, member_values in payloads.values():
            assert np.shares_memory(member_dates, dates)
            assert np.shares_memory(member_values, values)


@pytest.mark.parametrize("configured", [False, True])
def test_private_cache_readonly_audit_reuses_writer_root_without_rebuilding(tmp_path, monkeypatch, configured):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = ["twpub_*"], ["twpub_drop", "twpub_other"]
    cache = tmp_path / "writable-cache"
    expected = build_panel(stocks, panel_cache_root=cache, **_panel_kwargs(cfg, external, resolve_projection=False))
    if configured:
        cfg.data.panel_cache_root = str(cache)
    monkeypatch.setattr(audit, "build_panel", lambda *a, **kw: pytest.fail("valid private cache rebuilt"))
    monkeypatch.setattr(panel_module, "_load_external_feature_arrays", lambda *a, **kw: pytest.fail("source payload decoded"))
    actual, origin = audit._load_or_build_panel(cfg, stocks, external, build_if_missing=False,
                                              panel_cache_root=None if configured else cache)
    assert origin == "cache"
    _assert_panel_bits(expected, actual)
    assert not (stocks / "panel_cache_v2").exists()


def test_private_cache_does_not_accept_changed_source_generation(tmp_path):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = ["twpub_keep"], []
    cache = tmp_path / "cache"
    kwargs = _panel_kwargs(cfg, external, resolve_projection=False)
    build_panel(stocks, panel_cache_root=cache, **kwargs)
    assert panel_module.load_cached_panel(stocks, panel_cache_root=cache, **kwargs) is not None
    frame = pl.read_parquet(external).with_columns((pl.col("twpub_keep") + 0.25).alias("twpub_keep"))
    frame.write_parquet(external)
    assert panel_module.load_cached_panel(stocks, panel_cache_root=cache, **kwargs) is None
    with pytest.raises(RuntimeError, match="exact data/config fingerprint"):
        audit._load_or_build_panel(cfg, stocks, external, panel_cache_root=cache, build_if_missing=False)


@pytest.mark.parametrize("dtype", [np.float32, np.float64])
@pytest.mark.parametrize("symbols", [["AAA", "AAA", "MKT", "MKT", "ZZZ"], ["AAA", "ZZZ"], ["MKT"], []])
def test_zero_copy_symbol_partition_matches_boolean_reference_every_bit(dtype, symbols):
    names = np.asarray(symbols, dtype=str)
    dates = np.arange(len(names)).astype("datetime64[D]")
    values = np.asarray(([[-0.0, np.nan], [3.0, -2.0]] * 3)[:len(names)], dtype=dtype).reshape(len(names), 2)
    market_dates, market_values, stocks = panel_module._partition_external_symbol_arrays(names, dates, values, market_key="MKT")
    mask = names == "MKT"
    assert market_dates.tobytes() == dates[mask].tobytes()
    assert market_values.tobytes() == values[mask].tobytes()
    assert set(stocks) == set(symbols) - {"MKT"}
    for key, (member_dates, member_values) in stocks.items():
        assert member_dates.tobytes() == dates[names == key].tobytes()
        assert member_values.tobytes() == values[names == key].tobytes()
        assert np.shares_memory(member_values, values)


def test_zero_copy_partition_rejects_misaligned_or_unpartitioned_input():
    with pytest.raises(RuntimeError, match="misaligned"):
        panel_module._partition_external_symbol_arrays(np.asarray(["A"]), np.zeros(0), np.zeros((0, 1)), market_key="M")
    with pytest.raises(RuntimeError, match="not contiguous"):
        panel_module._partition_external_symbol_arrays(np.asarray(["A", "B", "A"]), np.arange(3), np.zeros((3, 1)), market_key="M")


def test_complete_benchmark_capture_keeps_configured_benchmark_even_outside_public_batch(tmp_path, monkeypatch):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.benchmark_name = "2330"
    pl.read_parquet(external).filter(pl.col("symbol") != "2330").write_parquet(external)
    (tmp_path / "fixture.json").write_text(json.dumps({
        "fixture_sha256": hashlib.sha256(external.read_bytes()).hexdigest(),
        "bounded_rows": pq.ParquetFile(external).metadata.num_rows,
    }))
    config, baseline = tmp_path / "config.yaml", tmp_path / "baseline.py"
    config.write_text("fixed config")
    baseline.write_text("fixed baseline")
    monkeypatch.setattr(producer_benchmark, "load_config", lambda _: cfg)
    output = tmp_path / "capture"
    receipt = producer_benchmark.capture(config=config, external=external, parquet_root=stocks, baseline=baseline, output=output)
    assert (output / "stocks/2330_features.parquet").is_file()
    assert next(iter(receipt["captured_sources"])) == str(stocks / "2330_features.parquet")
    assert receipt["stock_files"] == 3
    assert receipt["input_proofs"] == producer_benchmark._proofs([Path(p) for p in receipt["input_proofs"]])


@pytest.mark.parametrize("include,exclude", [
    (["missing_feature"], ["twpub_drop"]),
    (["twpub_keep"], ["missing_exclude"]),
    (["twpub_keep"], ["twpub_keep"]),
])
def test_invalid_selection_fails_before_any_data_read(tmp_path, monkeypatch, include, exclude):
    _, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = include, exclude
    monkeypatch.setattr(pq, "read_table", lambda *a, **kw: pytest.fail("data read before admission"))
    with pytest.raises(ValueError):
        _panel_kwargs(cfg, external)


def test_independent_rules_only_path_is_unchanged(tmp_path):
    _, external, cfg = _fixture(tmp_path)
    cfg.data.use_tw_public_features = False
    cfg.data.feature_include, cfg.data.feature_exclude = ["body_ratio"], ["close_logret_1d"]
    kwargs = _panel_kwargs(cfg, external)
    assert kwargs["feature_include"] == ["body_ratio"]
    assert kwargs["feature_exclude"] == ["close_logret_1d"]
    assert kwargs["external_include_features"] is False
    assert kwargs["external_include_rules"] is True


@pytest.mark.parametrize("already_projected", [False, True])
def test_valid_original_or_projected_cache_is_reused_without_a_build(tmp_path, monkeypatch, already_projected):
    stocks, external, cfg = _fixture(tmp_path)
    cfg.data.feature_include, cfg.data.feature_exclude = ["twpub_keep", "twpub_drop"], ["twpub_drop"]
    kwargs = _panel_kwargs(cfg, external, resolve_projection=already_projected)
    build_panel(stocks, **kwargs)
    expected = audit.load_cached_panel(stocks, **kwargs)
    assert expected is not None
    monkeypatch.setattr(audit, "build_panel", lambda *a, **k: pytest.fail("valid complete cache was rebuilt"))
    actual, source = audit._load_or_build_panel(cfg, stocks, external, build_if_missing=False)
    assert source == "cache"
    _assert_panel_bits(expected, actual)


@pytest.mark.parametrize("value", [
    np.asarray(["2024-01-02", "NaT"], dtype="datetime64[D]"),
    np.asarray([], dtype="datetime64[D]"),
    np.asarray([[-0.0, np.nan, 2.5]], dtype="float32"),
    np.zeros((0, 17), dtype="float64"),
    np.asarray([1, 2], dtype="int64"),
])
def test_benchmark_hash_preserves_all_supported_array_bits(value):
    actual = hashlib.sha256()
    benchmark._array(actual, "value", value)
    contiguous = np.ascontiguousarray(value)
    header = json.dumps(["value", contiguous.dtype.str, list(contiguous.shape)], ensure_ascii=False).encode()
    expected = hashlib.sha256(len(header).to_bytes(8, "big") + header + contiguous.tobytes())
    assert actual.hexdigest() == expected.hexdigest()


def test_benchmark_does_not_hash_object_pointers():
    with pytest.raises(ValueError, match="object arrays"):
        benchmark._array(hashlib.sha256(), "unsafe", np.asarray([object()]))


def _retry_fixture(tmp_path, monkeypatch):
    # Private engineering inputs only. No live source or complete DB copy.
    cfg = load_config("configs/markets/tw_public.yaml")
    root = tmp_path / "project"
    output = root / "artifacts/benchmarks/attempt"
    output.mkdir(parents=True)
    for relative in ("scripts/audit_tw_public_data_layer.py", "stockagent/config.py"):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixed-code-identity")
    target = output / "bounded_features.parquet"
    target.write_bytes(b"fixed-bounded-input")
    metadata = {
        "contract": benchmark.CONTRACT, "bounded_rows": 3, "batch_bytes": 32,
        "fixture_sha256": benchmark._sha(target), "fixture_bytes": target.stat().st_size,
        "resolved_config_sha256": hashlib.sha256(json.dumps(asdict(cfg), sort_keys=True, default=str).encode()).hexdigest(),
    }
    (output / "fixture.json").write_text(json.dumps(metadata))
    monkeypatch.setattr(benchmark, "ROOT", root)
    monkeypatch.setattr(benchmark, "load_config", lambda _: cfg)
    monkeypatch.setattr(benchmark.pl, "thread_pool_size", lambda: 2)
    monkeypatch.setattr(benchmark, "capture", lambda *a: pytest.fail("must reuse the fixed capture"))
    return output, target, metadata


def _run_retry(output):
    return benchmark.benchmark(config_path=Path("unused.yaml"), feature_path=None,
                               parquet_root=None, output=output, max_rows=65536,
                               threads=2, reuse_fixture=True)


def test_benchmark_retry_reuses_fixed_input_and_preserves_trial_receipts(tmp_path, monkeypatch):
    output, target, _ = _retry_fixture(tmp_path, monkeypatch)
    before = benchmark._generation(target)
    variants = []

    def child(command, **_):
        variants.append(command[command.index("--worker") + 1])
        return SimpleNamespace(returncode=0, stdout=json.dumps({
            "variant": variants[-1], "polars_threads": 2, "arrow_threads": 2,
            "output_sha256": "same-complete-output", "wall_seconds": 0.01,
            "cpu_seconds": 0.01, "rss_peak_bytes": 1024,
        }), stderr="")

    monkeypatch.setattr(benchmark.subprocess, "run", child)
    result = _run_retry(output)
    assert variants == ["baseline", "projected", "projected", "baseline"]
    assert before == benchmark._generation(target)
    assert result["reused_existing_bounded_fixture"] is True
    assert len(list(Path(result["attempt"]).glob("*.json"))) == 5
    accepted_before = (output / "abba-receipt.json").read_bytes()
    repeated = _run_retry(output)
    assert repeated["attempt"] != result["attempt"]
    assert before == benchmark._generation(target)
    assert (output / "abba-receipt.json").read_bytes() == accepted_before


def test_benchmark_child_failure_is_visible_and_durably_recorded(tmp_path, monkeypatch):
    output, _, _ = _retry_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *a, **k: SimpleNamespace(
        returncode=1, stdout="partial progress", stderr="actual reader failure"))
    with pytest.raises(RuntimeError, match="actual reader failure"):
        _run_retry(output)
    receipts = list(output.glob("attempt-*/*-failed.json"))
    assert len(receipts) == 1
    assert json.loads(receipts[0].read_bytes())["stderr"] == "actual reader failure"
    assert not (output / "abba-receipt.json").exists()


@pytest.mark.parametrize("change", ["config", "input"])
def test_benchmark_retry_rejects_changed_fixture_before_running(tmp_path, monkeypatch, change):
    output, target, metadata = _retry_fixture(tmp_path, monkeypatch)
    if change == "config":
        metadata["resolved_config_sha256"] = "changed"
        (output / "fixture.json").write_text(json.dumps(metadata))
    else:
        target.write_bytes(b"mutated-input")
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *a, **k: pytest.fail("bad fixture admitted"))
    with pytest.raises(ValueError, match="existing bounded fixture/config"):
        _run_retry(output)
