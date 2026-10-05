from __future__ import annotations

from dataclasses import asdict, replace
import json
from pathlib import Path
import sys

import polars as pl
import pytest

DOWNLOADER_DIR = Path(__file__).resolve().parents[1] / "downloader"
if str(DOWNLOADER_DIR) not in sys.path:
    sys.path.insert(0, str(DOWNLOADER_DIR))

import download_bybit_funding_history as funding  # noqa: E402
import materialize_bybit_perpetual_daily as daily  # noqa: E402
from download_bybit_perp_daily import SymbolRecord  # noqa: E402


def _record(code="ICXUSDT", *, status="Trading", **changes):
    record = SymbolRecord(
        code=code, name=code, market="bybit_linear_perp", bybit_symbol=code,
        category="linear", base_coin=code[:-4], quote_coin="USDT", settle_coin="USDT",
        contract_type="LinearPerpetual", status=status, launch_time="2022-04-13 06:22:15",
        symbol_type="", funding_interval_minutes=480, is_pre_listing=False,
        price_tick_size=0.00001, minimum_order_quantity=1.0, quantity_step=1.0,
        minimum_notional_value=5.0, maximum_order_quantity=570000.0,
        maximum_market_order_quantity=110000.0, maximum_leverage=25.0,
        unified_margin_trade=True,
    )
    return replace(record, **changes)


def _csv(path, records):
    pl.DataFrame([asdict(record) for record in records]).write_csv(path)
    return path


@pytest.mark.parametrize("module", [funding, daily])
@pytest.mark.parametrize("symbols", [[], ["--symbols"], ["--symbols", " "]])
def test_historical_cli_requires_explicit_symbols(monkeypatch, module, symbols):
    monkeypatch.setattr(sys, "argv", ["script", "--historical-instruments", "old.csv", *symbols])
    with pytest.raises(SystemExit):
        module.parse_args()


@pytest.mark.parametrize("module", [funding, daily])
def test_historical_cli_accepts_named_symbols(monkeypatch, module):
    monkeypatch.setattr(sys, "argv", ["script", "--historical-instruments", "old.csv", "--symbols", "ICXUSDT"])
    args = module.parse_args()
    assert args.historical_instruments == Path("old.csv")
    assert args.symbols == ["ICXUSDT"]


@pytest.mark.parametrize("status", ["Trading", "Closed", "Settled"])
def test_dated_historical_identity_not_current_trading_status(tmp_path, status):
    old = _record(status=status)
    path = _csv(tmp_path / "instruments_20260823.csv", [old])
    before = path.read_bytes()
    selected = funding._select_funding_instruments([_record("BTCUSDT")], {old.code}, path)
    assert selected == [old]
    assert selected[0].status == status
    assert path.read_bytes() == before


def test_default_remains_current_trading_only(tmp_path):
    records = [_record("BTCUSDT"), _record(status="Closed"), _record("USDCPERP", quote_coin="USDC")]
    assert funding._select_funding_instruments(records, set(), None) == records[:1]
    with pytest.raises(ValueError, match="not standard"):
        funding._select_funding_instruments(records, {"ICXUSDT"}, None)
    with pytest.raises(ValueError, match="explicit"):
        funding._select_funding_instruments(records, set(), tmp_path / "old.csv")
    current = _csv(tmp_path / "current.csv", records)
    assert daily._select_materialize_instruments(current, set(), None)["code"].to_list() == ["BTCUSDT"]
    with pytest.raises(ValueError, match="explicit"):
        daily._select_materialize_instruments(current, set(), tmp_path / "old.csv")


def test_current_snapshot_wins_over_old_metadata(tmp_path):
    old = _record(funding_interval_minutes=480)
    new = replace(old, funding_interval_minutes=60)
    path = _csv(tmp_path / "old.csv", [old])
    assert funding._select_funding_instruments([new], {new.code}, path) == [new]
    current = _csv(tmp_path / "current.csv", [new])
    selected = daily._select_materialize_instruments(current, {new.code}, path)
    assert selected["funding_interval_minutes"].to_list() == [60]


def test_missing_explicit_historical_csv_fails_even_for_current_symbol(tmp_path):
    record = _record()
    with pytest.raises(FileNotFoundError, match="historical instrument"):
        funding._select_funding_instruments([record], {record.code}, tmp_path / "absent.csv")
    current = _csv(tmp_path / "current.csv", [record])
    with pytest.raises(FileNotFoundError, match="historical instrument"):
        daily._select_materialize_instruments(current, {record.code}, tmp_path / "absent.csv")


@pytest.mark.parametrize("changes", [
    {"category": "inverse"}, {"settle_coin": "USDC"}, {"quote_coin": "USD"},
    {"contract_type": "LinearFutures"}, {"symbol_type": "xstock"},
    {"is_pre_listing": True}, {"launch_time": None},
    {"funding_interval_minutes": None}, {"funding_interval_minutes": 0},
    {"bybit_symbol": "OTHERUSDT"},
])
def test_incompatible_or_incomplete_historical_identity_rejected(tmp_path, changes):
    path = _csv(tmp_path / "old.csv", [_record(**changes)])
    with pytest.raises(ValueError, match="incompatible or incomplete"):
        funding._historical_instrument_records(path, {"ICXUSDT"})


def test_missing_duplicate_or_partial_historical_schema_rejected(tmp_path):
    path = _csv(tmp_path / "old.csv", [_record()])
    with pytest.raises(ValueError, match="absent"):
        funding._historical_instrument_records(path, {"HFTUSDT"})
    _csv(path, [_record(), _record()])
    with pytest.raises(ValueError, match="duplicate"):
        funding._historical_instrument_records(path, {"ICXUSDT"})
    pl.DataFrame({"code": ["ICXUSDT"]}).write_csv(path)
    with pytest.raises(ValueError, match="missing columns"):
        funding._historical_instrument_records(path, {"ICXUSDT"})


def test_current_incompatible_identity_cannot_be_overridden(tmp_path):
    current_record = _record(settle_coin="USDC")
    history = _csv(tmp_path / "old.csv", [_record()])
    with pytest.raises(ValueError, match="identity conflicts"):
        funding._select_funding_instruments([current_record], {"ICXUSDT"}, history)
    current = _csv(tmp_path / "current.csv", [current_record])
    with pytest.raises(ValueError, match="identity conflicts"):
        daily._select_materialize_instruments(current, {"ICXUSDT"}, history)


def test_materializer_historical_selection_requires_explicit_path(tmp_path):
    current = _csv(tmp_path / "current.csv", [_record("BTCUSDT")])
    history = _csv(tmp_path / "old.csv", [_record(status="Closed")])
    with pytest.raises(ValueError, match="outside standard"):
        daily._select_materialize_instruments(current, {"ICXUSDT"}, None)
    selected = daily._select_materialize_instruments(current, {"ICXUSDT"}, history)
    assert selected["code"].to_list() == ["ICXUSDT"]
    assert selected["status"].to_list() == ["Closed"]


def _materialize_inputs(tmp_path, monkeypatch, *, include_coverage):
    minute_dir, funding_dir, output_dir = (tmp_path / name for name in ("minutes", "funding", "daily"))
    minute_dir.mkdir()
    funding_dir.mkdir()
    _csv(funding_dir / "instruments.csv", [_record("BTCUSDT")])
    history = _csv(tmp_path / "old.csv", [_record(status="Closed")])
    # The core calculation is mocked: these files establish only path routing.
    (minute_dir / "ICXUSDT_features.parquet").write_bytes(b"test minute fixture")
    (funding_dir / "ICXUSDT_funding.parquet").write_bytes(b"test funding fixture")
    row = {"symbol": "ICXUSDT" if include_coverage else "BTCUSDT", "head_complete": True,
           "coverage_start_utc": "2022-04-13T06:22:15Z", "coverage_end_utc": "2026-09-18T09:00:00Z"}
    pl.DataFrame([row]).write_csv(funding_dir / "funding_coverage.csv")
    monkeypatch.setattr(sys, "argv", [
        "script", "--input-dir", str(minute_dir), "--funding-dir", str(funding_dir),
        "--output-dir", str(output_dir), "--historical-instruments", str(history),
        "--symbols", "ICXUSDT", "--workers", "1", "--refresh",
    ])
    return funding_dir, output_dir, row


def test_materializer_routes_selected_historical_symbol_to_supplied_coverage(tmp_path, monkeypatch):
    funding_dir, output_dir, row = _materialize_inputs(tmp_path, monkeypatch, include_coverage=True)
    observed = []
    monkeypatch.setattr(daily, "_daily_bars", lambda *a, **kw: (pl.DataFrame(), 0, 0))

    def attach(frame, path, coverage, **kwargs):
        observed.append((path, coverage, kwargs))
        return pl.DataFrame({"date": ["2026-09-04"]}), 1, 3

    monkeypatch.setattr(daily, "_attach_funding_total_return", attach)
    daily.main()
    assert observed == [(funding_dir / "ICXUSDT_funding.parquet", row, {"execution_minutes_utc": 5})]
    assert pl.read_csv(output_dir / "symbols.csv")["status"].to_list() == ["Closed"]


def test_historical_identity_does_not_bypass_missing_funding_coverage(tmp_path, monkeypatch):
    _, output_dir, _ = _materialize_inputs(tmp_path, monkeypatch, include_coverage=False)
    monkeypatch.setattr(daily, "_daily_bars", lambda *a, **kw: pytest.fail("must reject before reading bars"))
    with pytest.raises(RuntimeError, match="daily materialization failed"):
        daily.main()
    report = pl.read_csv(output_dir / "materialize_report.csv")
    assert "missing complete funding source/receipt for ICXUSDT" in report["message"][0]
    assert not (output_dir / "ICXUSDT_features.parquet").exists()


def test_funding_main_never_relabels_history_as_current_snapshot(tmp_path, monkeypatch):
    historical = _record(status="Closed")
    current = _record("BTCUSDT")
    history_path = _csv(tmp_path / "old.csv", [historical])
    output_dir = tmp_path / "funding"
    monkeypatch.setattr(sys, "argv", [
        "script", "--historical-instruments", str(history_path), "--symbols", historical.code,
        "--output-dir", str(output_dir), "--workers", "1", "--refresh",
    ])
    monkeypatch.setattr(funding, "BybitClient", lambda *args: object())
    monkeypatch.setattr(funding, "_fetch_perp_symbols", lambda *args, **kwargs: [current])
    observed = []

    def download(client, record, **kwargs):
        observed.append(record)
        return funding.FundingResult(
            symbol=record.code, status="updated", rows=1, requested_start_utc="2019-01-01",
            coverage_start_utc=record.launch_time, coverage_end_utc="2026-09-18 09:00:00",
            first_funding_utc="2022-04-13 16:00:00", last_funding_utc="2026-09-18 08:00:00",
            head_complete=True, quarantined_prefix_events=0, output_path=None, sha256=None,
        )

    monkeypatch.setattr(funding, "_download_symbol", download)
    funding.main()
    assert observed == [historical]
    assert pl.read_csv(output_dir / "instruments.csv")["code"].to_list() == [current.code]
    summary = json.loads((output_dir / "funding_summary.json").read_text())
    assert summary["historical_instruments"]["role"] == "historical_product_identity_only_not_current_trading_eligibility"
    assert summary["historical_instruments"]["sha256"] == funding._sha256(history_path)
