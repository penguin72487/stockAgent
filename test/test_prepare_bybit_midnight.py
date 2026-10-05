from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import polars as pl
import pytest

import scripts.prepare_bybit_midnight as midnight
import stockagent.data.crypto_exchange_scope as scope


def funding(symbol="BTCUSDT"):
    stamps = [datetime(2026, 1, day, tzinfo=timezone.utc) for day in (1, 2, 3)]
    return pl.DataFrame({
        "symbol": [symbol] * 3, "category": ["linear"] * 3,
        "bybit_funding_contract_version": [3] * 3,
        "funding_timestamp_ms": [int(t.timestamp() * 1000) for t in stamps],
        "funding_time_utc": [t.strftime("%Y-%m-%d %H:%M:%S") for t in stamps],
        "funding_rate": [0.01, 0.02, 0.03], "funding_mark_price": [100.] * 3,
        "funding_coverage_start_utc": ["2026-01-01 00:00:00"] * 3,
        "download_snapshot_utc": ["2026-01-04 00:00:00"] * 3,
        "funding_prefix_quarantined_events": [0] * 3,
    })


def test_retained_funding_contract_recovers_only_recorded_horizon():
    result = midnight.checked_funding(funding(), "BTCUSDT")
    assert result == {"head_complete": True, "coverage_start_utc": "2026-01-01 00:00:00",
                      "coverage_end_utc": "2026-01-04 00:00:00"}


@pytest.mark.parametrize("fault", ["symbol", "category", "contract", "duplicate", "rate", "mark", "time", "horizon", "schema", "empty"])
def test_bad_retained_funding_fails_closed(fault):
    frame = funding()
    if fault == "symbol": frame = frame.with_columns(pl.lit("ICXUSDT").alias("symbol"))
    elif fault == "category": frame = frame.with_columns(pl.lit("inverse").alias("category"))
    elif fault == "contract": frame = frame.with_columns(pl.lit(2).alias("bybit_funding_contract_version"))
    elif fault == "duplicate": frame = pl.concat([frame, frame.tail(1)])
    elif fault == "rate": frame = frame.with_columns(pl.lit(float("nan")).alias("funding_rate"))
    elif fault == "mark": frame = frame.with_columns(pl.lit(0.).alias("funding_mark_price"))
    elif fault == "time": frame = frame.with_columns(pl.lit("2026-01-01 00:00:00").alias("funding_time_utc"))
    elif fault == "horizon": frame = frame.with_columns(pl.lit("2025-01-01 00:00:00").alias("download_snapshot_utc"))
    elif fault == "schema": frame = frame.drop("funding_coverage_start_utc")
    else: frame = frame.head(0)
    with pytest.raises(ValueError): midnight.checked_funding(frame, "BTCUSDT")


def test_actual_midnight_prices_and_boundary_funding_ownership(tmp_path):
    (tmp_path / "1m").mkdir()
    (tmp_path / "funding").mkdir()
    start = datetime(2026, 1, 1)
    times = [start + timedelta(minutes=i) for i in range(3 * 1440 + 6)]
    prices = [100. if t.minute == 0 else 110. for t in times]
    pl.DataFrame({"date": times, "open": prices, "max": prices, "min": prices,
                  "close": prices, "Trading_Volume": [1.] * len(times),
                  "bybit_turnover": prices}).write_parquet(tmp_path / "1m/HFTUSDT_features.parquet")
    funding("HFTUSDT").write_parquet(tmp_path / "funding/HFTUSDT_funding.parquet")
    frame, detail = midnight.build_symbol(tmp_path, "HFTUSDT", {}, {}, {})
    assert frame["execution_price"].to_list() == [100., 100., 100.]
    # Jan 2's boundary charge belongs to the pre-existing position, while the
    # new Jan 2 target pays Jan 3's charge at the next boundary.
    assert frame["funding_cashflow_coefficient_to_next"][0] == pytest.approx(0.03)
    assert frame["funding_cashflow_coefficient_previous_session"][1] == pytest.approx(0.03)
    assert frame["funding_adjusted_simple_return_to_next"][0] == pytest.approx(-0.03)
    assert set(frame["daily_boundary_utc"]) == {"00:00"}
    assert set(frame["bybit_perpetual_contract_version"]) == {7}
    assert detail["coverage_origin"] == "retained_v3_metadata"
    assert frame["return_quarantined"][-1]


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    monkeypatch.setattr(midnight, "ROOT", tmp_path)
    monkeypatch.setattr(midnight, "EXPECTED_SYMBOL_COUNT", 4)
    monkeypatch.setattr(scope, "BYBIT_MIDNIGHT_SYMBOL_COUNT", 4)
    base = tmp_path / "immutable"
    for name in ("perpetual_daily", "1m", "1m/_hot_tail", "funding"):
        (base / name).mkdir(parents=True, exist_ok=True)
    symbols = ["BTCUSDT", "HFTUSDT", "ICXUSDT", "VINEUSDT"]
    for symbol in symbols:
        for directory, suffix in (("perpetual_daily", "features"), ("1m", "features"), ("funding", "funding")):
            (base / directory / f"{symbol}_{suffix}.parquet").write_bytes(b"retained")
    (base / "1m/_hot_tail/BTCUSDT_features.parquet").write_bytes(b"tail")
    pl.DataFrame({"symbol": symbols}).write_csv(base / "funding/funding_coverage.csv")
    probe = tmp_path / "probe.json"
    probe.write_text(json.dumps({"official_announcements": [{"metadata": {"symbol": s}}
                                                           for s in sorted(midnight.ANNOUNCED_SYMBOLS)]}))
    def fake_build(base, symbol, coverage, document, events):
        return pl.DataFrame({"date": ["2026-01-02"], "execution_price": [100.]}), {"rows": 1}
    monkeypatch.setattr(midnight, "build_symbol", fake_build)
    output = tmp_path / "artifacts/cache/midnight"
    receipt = midnight.prepare(base, probe, output, workers=2)
    return base, probe, output, receipt


def test_fixed_view_reuses_receipt_without_rebuilding(prepared, monkeypatch):
    base, probe, output, receipt = prepared
    before = {p: p.stat().st_mtime_ns for p in output.rglob("*") if p.is_file()}
    monkeypatch.setattr(midnight, "build_symbol", lambda *a: pytest.fail("should not rebuild"))
    assert midnight.prepare(base, probe, output) == receipt
    assert before == {p: p.stat().st_mtime_ns for p in before}
    assert len(receipt["derived_sha256"]) == 4
    assert len(receipt["sources_sha256"]) == 15
    scope.validate_bybit_midnight_view(output / "perpetual_daily", venue_root=base)


def test_evidence_is_hashed_before_in_memory_reads(prepared, monkeypatch):
    base, probe, output, _ = prepared
    seen = set()
    real_hash, real_read_csv, real_read_text = midnight.sha256_file, midnight.pl.read_csv, Path.read_text
    def observed_hash(path):
        seen.add(Path(path))
        return real_hash(path)
    def observed_read_text(path, *args, **kwargs):
        if path == probe:
            assert path in seen, "probe parsed before its initial hash"
        return real_read_text(path, *args, **kwargs)
    def observed_read_csv(path, *args, **kwargs):
        if Path(path) == base / "funding/funding_coverage.csv":
            assert Path(path) in seen, "coverage parsed before its initial hash"
        return real_read_csv(path, *args, **kwargs)
    monkeypatch.setattr(midnight, "sha256_file", observed_hash)
    monkeypatch.setattr(Path, "read_text", observed_read_text)
    monkeypatch.setattr(midnight.pl, "read_csv", observed_read_csv)
    midnight.prepare(base, probe, output.parent / "other_fixed_view")


@pytest.mark.parametrize("fault", ["raw", "tail", "funding", "probe", "derived", "extra_symbol", "missing_evidence", "clock", "base"])
def test_midnight_full_evidence_guard(prepared, fault):
    base, probe, output, receipt = prepared
    if fault == "raw": (base / "1m/BTCUSDT_features.parquet").write_bytes(b"changed")
    elif fault == "tail": (base / "1m/_hot_tail/BTCUSDT_features.parquet").write_bytes(b"changed")
    elif fault == "funding": (base / "funding/BTCUSDT_funding.parquet").write_bytes(b"changed")
    elif fault == "probe": probe.write_bytes(b"changed")
    elif fault == "derived": (output / "perpetual_daily/BTCUSDT_features.parquet").write_bytes(b"changed")
    elif fault == "extra_symbol": (output / "perpetual_daily/NEWUSDT_features.parquet").write_bytes(b"extra")
    elif fault == "missing_evidence": receipt["sources_sha256"].pop(str(base / "1m/_hot_tail/BTCUSDT_features.parquet"))
    elif fault == "clock": receipt["execution_boundary_utc"] = "00:05"
    else: receipt["base_root"] = str(base / "other")
    (output / "midnight_manifest.json").write_text(json.dumps(receipt))
    with pytest.raises(ValueError): scope.validate_bybit_midnight_view(output / "perpetual_daily", venue_root=base)


def test_partial_or_unsafe_output_never_overwritten(prepared):
    base, probe, output, _ = prepared
    with pytest.raises(ValueError, match="bounded"):
        midnight.prepare(base, probe, base / "new")
    (output / "midnight_manifest.json").unlink()
    with pytest.raises(ValueError, match="lacks completed"):
        midnight.prepare(base, probe, output)
    with pytest.raises(ValueError, match="workers"):
        midnight.prepare(base, probe, output, workers=5)


def test_change_during_build_never_installs_completion_receipt(prepared, monkeypatch):
    base, probe, output, _ = prepared
    build = midnight.build_symbol
    def changed_source(base, symbol, coverage, document, events):
        if symbol == "BTCUSDT":
            (base / "1m/BTCUSDT_features.parquet").write_bytes(b"source changed mid-build")
        return build(base, symbol, coverage, document, events)
    monkeypatch.setattr(midnight, "build_symbol", changed_source)
    candidate = output.parent / "incomplete_midnight"
    with pytest.raises(ValueError, match="source changed while building"):
        midnight.prepare(base, probe, candidate)
    assert not (candidate / "midnight_manifest.json").exists()


def test_midnight_public_receipt_rejects_later_information_clock(tmp_path, monkeypatch):
    directory = tmp_path / "data_bybit/public_features"
    directory.mkdir(parents=True)
    table = directory / "bybit_crypto_public_daily.parquet"
    pl.DataFrame({"date": ["2026-01-01"], "symbol": ["BTCUSDT"], "crypto_bybit_signal": [1.]}).write_parquet(table)
    table.with_name(table.stem + "_summary.json").write_text(json.dumps({"decision_boundary_utc": "00:05"}))
    monkeypatch.setattr(scope, "validate_bybit_midnight_view", lambda *args, **kwargs: None)
    data = {"crypto_exchange_scope": "bybit", "crypto_information_scope": "historical_public_pit",
            "parquet_root": scope.BYBIT_MIDNIGHT_VIEW, "use_external_features": True,
            "external_feature_path": str(table), "use_tw_public_features": False, "use_tw_public_rules": False,
            "feature_include": ["crypto_bybit_signal"], "feature_availability_indicators": ["crypto_bybit_signal"]}
    with pytest.raises(ValueError, match="midnight-cutoff public feature receipt"):
        scope.validate_crypto_exchange_scope(data, repo_root=tmp_path, check_schema=True)


def test_midnight_path_is_registered_without_weakening_external_scope(tmp_path):
    data = {"crypto_exchange_scope": "bybit", "crypto_information_scope": "venue_only",
            "parquet_root": scope.BYBIT_MIDNIGHT_VIEW, "use_external_features": False,
            "use_tw_public_features": False, "use_tw_public_rules": False,
            "feature_include": ["open_logret_1d"], "feature_availability_indicators": []}
    scope.validate_crypto_exchange_scope(data, repo_root=tmp_path)
    data["parquet_root"] = "artifacts/cache/unregistered/perpetual_daily"
    with pytest.raises(ValueError, match="inside"):
        scope.validate_crypto_exchange_scope(data, repo_root=tmp_path)
