from __future__ import annotations

from datetime import date
import json
import math
from pathlib import Path
import shutil
from types import SimpleNamespace

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

import scripts.build_tw_official_symbol_parquets as official_symbol_builder
from scripts.audit_tw_public_data_layer import audit_official_symbol_build
from scripts.build_tw_official_symbol_parquets import (
    MIXED_FALLBACK_SOURCE_NAME,
    PREVIOUS_SESSION_NEXT_REFERENCE_SOURCE_NAME,
    _official_frame,
    _legacy_official_frame,
    _normalized_reference_index,
    _normalize_yahoo_fallback_raw_ohlc,
    _receipt,
    _source_adjustment_factors,
    _validate_download_receipts,
    _validate_yahoo_fallback_archive,
    _write_symbol,
    _write_official_quote_parquet,
)
from scripts.build_tw_yahoo_fallback_archive import (
    YAHOO_ASSET_CLASS_METADATA_KEY,
    YAHOO_CHECKED_THROUGH_METADATA_KEY,
    YAHOO_REQUESTED_START_METADATA_KEY,
    YAHOO_SOURCE_METADATA_KEY,
    _manifest_records,
    _read_symbol_fallback,
    _whitelist_markets,
    main as build_yahoo_fallback_archive,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf


def _raw_scale_frame(
    *,
    dates: list[date],
    sources: list[str],
    closes: list[float],
    yahoo_closes: list[float | None],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": dates,
            "open": closes,
            "max": closes,
            "min": closes,
            "close": closes,
            "quote_source": sources,
            "_lifecycle_episode_id": [0] * len(dates),
            "_yahoo_fallback_close": yahoo_closes,
        }
    )


def test_yahoo_fallback_raw_ohlc_uses_latest_causal_official_overlap() -> None:
    frame = _raw_scale_frame(
        dates=[date(2007, 12, 31), date(2008, 1, 2), date(2008, 1, 3)],
        sources=["tpex_official", "yahoo_fallback", "tpex_official"],
        closes=[6.25, 4343.105469, 5.82],
        yahoo_closes=[4343.105469, 4343.105469, 4044.299561],
    )

    normalized, rows, unanchored, discontinuous = (
        _normalize_yahoo_fallback_raw_ohlc(frame)
    )

    assert rows == 1
    assert unanchored == 0
    assert discontinuous == 0
    fallback = normalized.filter(pl.col("quote_source") == "yahoo_fallback").row(
        0, named=True
    )
    assert fallback["close"] == pytest.approx(6.25)
    assert fallback["raw_ohlc_scale_factor"] == pytest.approx(
        6.25 / 4343.105469
    )
    assert fallback["raw_ohlc_scale_reference_date"] == date(2007, 12, 31)


def test_yahoo_fallback_raw_ohlc_does_not_backfill_future_anchor() -> None:
    frame = _raw_scale_frame(
        dates=[date(2000, 1, 4), date(2004, 2, 11)],
        sources=["yahoo_fallback", "twse_official"],
        closes=[999.0, 20.0],
        yahoo_closes=[999.0, 500.0],
    )

    normalized, rows, unanchored, discontinuous = (
        _normalize_yahoo_fallback_raw_ohlc(frame)
    )

    assert normalized["date"].to_list() == [date(2004, 2, 11)]
    assert rows == 0
    assert unanchored == 1
    assert discontinuous == 0


def test_yahoo_fallback_raw_ohlc_drops_post_anchor_scale_discontinuity() -> None:
    frame = _raw_scale_frame(
        dates=[date(2024, 1, 2), date(2024, 1, 3)],
        sources=["twse_official", "yahoo_fallback"],
        closes=[10.0, 5000.0],
        yahoo_closes=[100.0, 5000.0],
    )

    normalized, rows, unanchored, discontinuous = (
        _normalize_yahoo_fallback_raw_ohlc(frame)
    )

    assert normalized["date"].to_list() == [date(2024, 1, 2)]
    assert rows == 0
    assert unanchored == 0
    assert discontinuous == 1


def test_yahoo_fallback_raw_ohlc_drops_entire_stale_scale_regime() -> None:
    frame = _raw_scale_frame(
        dates=[
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
        ],
        sources=[
            "twse_official",
            "yahoo_fallback",
            "yahoo_fallback",
            "twse_official",
        ],
        closes=[10.0, 5000.0, 5100.0, 10.2],
        yahoo_closes=[100.0, 5000.0, 5100.0, 102.0],
    )

    normalized, rows, unanchored, discontinuous = (
        _normalize_yahoo_fallback_raw_ohlc(frame)
    )

    assert normalized["date"].to_list() == [date(2024, 1, 2), date(2024, 1, 5)]
    assert rows == 0
    assert unanchored == 0
    assert discontinuous == 2


def test_yahoo_fallback_raw_ohlc_drops_run_that_fails_resuming_official_boundary() -> None:
    frame = _raw_scale_frame(
        dates=[
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 5),
        ],
        sources=[
            "twse_official",
            "yahoo_fallback",
            "yahoo_fallback",
            "twse_official",
        ],
        closes=[10.0, 10.1, 10.2, 50.0],
        yahoo_closes=[100.0, 101.0, 102.0, 500.0],
    )

    normalized, rows, unanchored, discontinuous = (
        _normalize_yahoo_fallback_raw_ohlc(frame)
    )

    assert normalized["date"].to_list() == [date(2024, 1, 2), date(2024, 1, 5)]
    assert rows == 0
    assert unanchored == 0
    assert discontinuous == 2


def _write_yahoo_source_parquet(
    frame: pl.DataFrame,
    path: Path,
    *,
    source: str | None = "yahoo",
    asset_class: str | None = "tw_stocks",
    requested_start: str | None = "2000-01-01",
    checked_through: str | None = "2000-01-31",
) -> None:
    metadata = dict(frame.to_arrow().schema.metadata or {})
    for key, value in (
        (YAHOO_SOURCE_METADATA_KEY, source),
        (YAHOO_ASSET_CLASS_METADATA_KEY, asset_class),
        (YAHOO_REQUESTED_START_METADATA_KEY, requested_start),
        (YAHOO_CHECKED_THROUGH_METADATA_KEY, checked_through),
    ):
        if value is not None:
            metadata[key] = value.encode("utf-8")
    table = frame.to_arrow().replace_schema_metadata(metadata)
    pq.write_table(table, path)


def test_yahoo_manifest_rejects_unaccounted_raw_preferred_record(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "symbols.csv"
    pl.DataFrame(
        {
            "code": ["2330", "1101B"],
            "name": ["台積電", "台泥乙特"],
            "market": ["listed", "listed"],
            "yahoo_symbol": ["2330.TW", "1101B.TW"],
        }
    ).write_csv(manifest)

    with pytest.raises(RuntimeError, match="invalid=1"):
        _manifest_records(manifest)
    assert official_symbol_builder._yahoo_manifest_codes(manifest) is None


def _write_verified_taiex_calendar(
    input_dir: Path,
    sessions: list[date],
    *,
    effective_end: date | None = None,
) -> None:
    dates = sorted({date(1999, 1, 5), *sessions})
    path = input_dir / "twse_taiex_ohlc.parquet"
    pl.DataFrame(
        {
            "date": dates,
            "opening_index": [100.0] * len(dates),
            "highest_index": [101.0] * len(dates),
            "lowest_index": [99.0] * len(dates),
            "closing_index": [100.5] * len(dates),
            "_dataset": ["twse_taiex_ohlc"] * len(dates),
            "_source": ["TWSE"] * len(dates),
            "_source_product": ["indicesReport/MI_5MINS_HIST"] * len(dates),
            "_request_month": [value.strftime("%Y-%m") for value in dates],
            "_downloaded_at_utc": ["2026-07-12T00:00:00+00:00"] * len(dates),
            "_url": ["https://wwwc.twse.com.tw/indicesReport/MI_5MINS_HIST"]
            * len(dates),
        }
    ).write_parquet(path)
    (input_dir / "twse_taiex_ohlc.summary.json").write_text(
        json.dumps(
            {
                "dataset": "twse_taiex_ohlc",
                "source": "TWSE",
                "official_start_date": "1999-01-05",
                "effective_start_date": "1999-01-05",
                "effective_end_date": str(effective_end or dates[-1]),
                "coverage_complete": True,
                "baseline_established": True,
                "replacement_promoted": True,
                "failed_count": 0,
                "unresolved_month_count": 0,
                "output_rows": len(dates),
                "output_receipt": _receipt(path),
            }
        ),
        encoding="utf-8",
    )


def _write_empty_lifecycle_evidence(input_dir: Path) -> None:
    for name in ("twse_delisted_company", "tpex_delisted_company"):
        pl.DataFrame(
            schema={
                "date": pl.String,
                "market": pl.String,
                "symbol": pl.String,
                "company_name": pl.String,
                "delisting_reason": pl.String,
            }
        ).write_parquet(input_dir / f"{name}.parquet")
    pl.DataFrame(
        schema={
            "公司代號": pl.String,
            "公司簡稱": pl.String,
            "公司名稱": pl.String,
            "上市日期": pl.String,
        }
    ).write_parquet(input_dir / "twse_listed_company_basic.parquet")
    pl.DataFrame(
        schema={
            "SecuritiesCompanyCode": pl.String,
            "CompanyAbbreviation": pl.String,
            "CompanyName": pl.String,
            "DateOfListing": pl.String,
        }
    ).write_parquet(input_dir / "tpex_basic_company.parquet")
    pl.DataFrame(
        schema={
            "Code": pl.String,
            "Company": pl.String,
            "ListingDate": pl.String,
            "ApprovedListingDate": pl.String,
            "Note": pl.String,
        }
    ).write_parquet(input_dir / "twse_api_company_newlisting.parquet")


def _write_core_official_sources(
    input_dir: Path,
    *,
    twse: pl.DataFrame,
    tpex: pl.DataFrame,
    sessions: list[date],
) -> None:
    twse.write_parquet(input_dir / "twse_daily_ohlcv.parquet")
    tpex.write_parquet(input_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(input_dir / "tw_corporate_action_reference.parquet")
    _write_verified_taiex_calendar(input_dir, sessions)


def _empty_twse_quotes() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "date": pl.Date,
            "證券代號": pl.String,
            "證券名稱": pl.String,
            "開盤價": pl.Float64,
            "最高價": pl.Float64,
            "最低價": pl.Float64,
            "收盤價": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌(+/-)": pl.String,
            "漲跌價差": pl.Float64,
        }
    )


def _empty_tpex_quotes() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "date": pl.Date,
            "代號": pl.String,
            "名稱": pl.String,
            "開盤": pl.Float64,
            "最高": pl.Float64,
            "最低": pl.Float64,
            "收盤": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌": pl.String,
            "次日參考價": pl.Float64,
        }
    )


def test_official_frame_excludes_partial_rows_after_completed_cutoff(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    public_dir.mkdir()
    tpex = pl.DataFrame(
        {
            "date": [date(2026, 7, 15), date(2026, 7, 16)],
            "代號": ["2330", "2330"],
            "名稱": ["台積電", "台積電"],
            "開盤": [1000.0, 1010.0],
            "最高": [1020.0, 1030.0],
            "最低": [995.0, 1005.0],
            "收盤": [1015.0, 1025.0],
            "成交股數": [1000.0, 1200.0],
            "漲跌": ["+15", "除權息"],
            "次日參考價": [1015.0, None],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=_empty_twse_quotes(),
        tpex=tpex,
        sessions=[date(2026, 7, 15), date(2026, 7, 16)],
    )

    frame, _, _, _, _ = _official_frame(
        public_dir,
        end_date=date(2026, 7, 15),
    )

    assert frame.get_column("date").to_list() == [date(2026, 7, 15)]


def test_builder_summary_is_accepted_by_auditor_and_tampering_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public_dir = tmp_path / "data_tw_public"
    output_dir = public_dir / "stocks"
    public_dir.mkdir()
    session = date(2004, 2, 11)
    _write_core_official_sources(
        public_dir,
        twse=pl.DataFrame(
            {
                "date": [session],
                "證券代號": ["2330"],
                "證券名稱": ["台積電"],
                "開盤價": [100.0],
                "最高價": [101.0],
                "最低價": [99.0],
                "收盤價": [100.0],
                "成交股數": [1000.0],
                "漲跌(+/-)": ["+"],
                "漲跌價差": [0.0],
            }
        ),
        tpex=_empty_tpex_quotes(),
        sessions=[session],
    )
    _write_empty_lifecycle_evidence(public_dir)
    (public_dir / "download_summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "failed_count": 0,
                "end_date": str(session),
            }
        ),
        encoding="utf-8",
    )
    corporate_action_path = public_dir / "tw_corporate_action_reference.parquet"
    (public_dir / "tw_corporate_action_reference.summary.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "coverage_complete": True,
                "failure_count": 0,
                "output_receipt": _receipt(corporate_action_path),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        official_symbol_builder,
        "parse_args",
        lambda: SimpleNamespace(
            input_dir=public_dir,
            output_dir=output_dir,
            workers=1,
            legacy_official_ohlcv=[],
            legacy_source_name=None,
            fallback_ohlcv=[],
            fallback_source_name=None,
            summary_path=None,
            report_path=None,
            dry_run=False,
            allow_incomplete_source=False,
        ),
    )

    official_symbol_builder.main()

    summary_path = output_dir / "official_symbol_build_summary.json"
    original = json.loads(summary_path.read_text(encoding="utf-8"))
    result, findings = audit_official_symbol_build(output_dir, public_dir)
    assert result["valid"] is True
    assert findings == []
    assert len(original["source_receipts"]) == 8
    assert len(original["lifecycle_source_receipts"]) == 5

    quarantine_count_tamper = dict(original)
    quarantine_count_tamper["return_quarantined_rows"] = (
        int(original["return_quarantined_rows"]) + 1
    )
    summary_path.write_text(json.dumps(quarantine_count_tamper), encoding="utf-8")
    result, _ = audit_official_symbol_build(output_dir, public_dir)
    assert result["valid"] is False
    assert result["checks"]["quarantine_count_reconciliation"] is False

    method_tamper = dict(original)
    method_tamper["adjusted_price_method"] += "; tampered"
    summary_path.write_text(json.dumps(method_tamper), encoding="utf-8")
    result, _ = audit_official_symbol_build(output_dir, public_dir)
    assert result["valid"] is False
    assert result["checks"]["adjusted_method"] is False

    receipt_tamper = dict(original)
    receipt_tamper["source_receipts"] = original["source_receipts"][:-1]
    summary_path.write_text(json.dumps(receipt_tamper), encoding="utf-8")
    result, _ = audit_official_symbol_build(output_dir, public_dir)
    assert result["valid"] is False
    assert result["checks"]["source_receipts"] is False


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("2330", "stock"),
        ("9110", "stock"),
        ("0050", "etf"),
        ("00631L", "etf"),
        ("00400A", "etf"),
        ("0001", None),
        ("01001T", None),
        ("020000", None),
        ("03001P", None),
        ("2881A", None),
    ],
)
def test_tw_universe_contains_only_stocks_and_etfs(symbol: str, expected: str | None) -> None:
    assert classify_tw_stock_or_etf(symbol) == expected


def test_official_adjusted_index_starts_at_ten_and_chains_reference_returns() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([100.0, 96.0, 97.0, 90.0]),
        np.asarray([0.0, 1.0, 1.0, -5.0]),
        np.asarray([1000.0, 1000.0, 1000.0, 1000.0]),
    )

    assert missing == 0
    assert adjusted[0] == 10.0
    assert math.isclose(adjusted[1] / adjusted[0], 96.0 / 95.0)
    assert math.isclose(adjusted[2] / adjusted[1], 97.0 / 96.0)
    assert math.isclose(adjusted[3] / adjusted[2], 90.0 / 95.0)


def test_official_adjusted_index_freezes_zero_volume_unknown_reference() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([10.0, 10.0, 10.5]),
        np.asarray([np.nan, np.nan, 0.5]),
        np.asarray([1000.0, 0.0, 1000.0]),
    )

    assert missing == 0
    assert adjusted.tolist() == [10.0, 10.0, 10.5]


def test_official_adjusted_index_masks_positive_volume_unknown_reference() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([10.0, 10.2, 10.5]),
        np.asarray([0.0, np.nan, 0.3]),
        np.asarray([1000.0, 1000.0, 1000.0]),
    )

    assert missing == 1
    assert np.isnan(adjusted[1])
    assert math.isclose(adjusted[2], 10.0 * (10.5 / 10.2))


def test_corporate_action_reference_overrides_zero_change_marker() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([84.2, 80.0]),
        np.asarray([0.0, 0.0]),
        np.asarray([1000.0, 1000.0]),
        np.asarray([np.nan, 79.07]),
    )

    assert missing == 0
    assert math.isclose(adjusted[1] / adjusted[0], 80.0 / 79.07)


def test_explicit_official_adjusted_series_is_rebased_to_ten_by_return_factor() -> None:
    factors = _source_adjustment_factors(np.asarray([50.0, 51.0, 49.98]))
    adjusted, missing = _normalized_reference_index(
        np.asarray([100.0, 100.0, 100.0]),
        np.asarray([np.nan, np.nan, np.nan]),
        np.asarray([1000.0, 1000.0, 1000.0]),
        factor_override=factors,
    )

    assert missing == 0
    assert adjusted[0] == 10.0
    assert math.isclose(adjusted[1], 10.2)
    assert math.isclose(adjusted[2], 9.996)


def test_adjusted_ratio_does_not_bridge_two_archive_files() -> None:
    factors = _source_adjustment_factors(
        np.asarray([50.0, 51.0, 10.0, 10.2]),
        np.asarray([0, 0, 1, 1]),
    )

    assert np.isnan(factors[0])
    assert math.isclose(factors[1], 51.0 / 50.0)
    assert np.isnan(factors[2])
    assert math.isclose(factors[3], 10.2 / 10.0)


def test_legacy_official_archive_without_adjclose_keeps_reconstruction_inputs(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "twse_legacy.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5)],
            "symbol": ["2330", "2330"],
            "market": ["twse", "twse"],
            "open": [10.0, 10.5],
            "high": [10.5, 11.0],
            "low": [9.5, 10.0],
            "close": [10.0, 10.5],
            "volume": [1000.0, 1200.0],
            "signed_change": [0.0, 0.5],
        }
    ).write_parquet(archive)

    normalized = _legacy_official_frame(archive)

    assert normalized["source_adjclose"].null_count() == 2
    assert normalized["signed_change"].to_list() == [0.0, 0.5]
    assert normalized["max"].to_list() == [10.5, 11.0]


def test_official_unlimited_etf_return_above_two_x_is_preserved() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([17.95, 38.83]),
        np.asarray([0.0, 20.88]),
        np.asarray([1000.0, 1000.0]),
    )

    assert missing == 0
    assert math.isclose(adjusted[1] / adjusted[0], 38.83 / 17.95)


def test_official_parquet_metadata_has_no_yahoo_lineage(tmp_path: Path) -> None:
    output = tmp_path / "2330_features.parquet"
    frame = pl.DataFrame(
        {
            "date": [date(2026, 7, 10)],
            "open": [100.0],
            "max": [101.0],
            "min": [99.0],
            "close": [100.5],
            "Trading_Volume": [1000.0],
            "adjclose": [10.0],
        }
    )

    _write_official_quote_parquet(frame, output, checked_through="2026-07-10")

    metadata = pq.read_schema(output).metadata or {}
    assert metadata[b"stockagent.source"] == b"twse_tpex_official"
    assert metadata[b"stockagent.official_checked_through"] == b"2026-07-10"
    assert not any(b"yahoo" in key.lower() for key in metadata)


def test_yahoo_fallback_filters_invalid_bars_and_preserves_source_factor(
    tmp_path: Path,
) -> None:
    source = tmp_path / "2330_features.parquet"
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4), date(2000, 1, 5), date(2000, 1, 6)],
                "open": [100.0, 102.0, 104.0],
                "max": [101.0, 103.0, 103.0],
                "min": [99.0, 101.0, 102.0],
                "close": [100.0, 102.0, 104.0],
                "adjclose": [50.0, 51.0, 52.0],
                "Trading_Volume": [1000.0, 1200.0, 1300.0],
            }
        ),
        source,
    )

    result, frame = _read_symbol_fallback(
        "2330",
        [(source, None)],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "ok"
    assert frame is not None
    assert frame.height == 2
    assert frame["quote_source"].unique().to_list() == ["yahoo_fallback"]
    assert frame["source_factor"][0] is None
    assert math.isclose(frame["source_factor"][1], 51.0 / 50.0)


@pytest.mark.parametrize(
    ("metadata_overrides", "expected_message"),
    [
        ({"source": None}, "stockagent.source"),
        ({"requested_start": None}, "stockagent.yahoo_requested_start"),
        ({"checked_through": "2000-01-30"}, "checked_through=2000-01-30 is earlier"),
    ],
)
def test_yahoo_fallback_rejects_missing_or_stale_coverage_metadata(
    tmp_path: Path,
    metadata_overrides: dict[str, str | None],
    expected_message: str,
) -> None:
    source = tmp_path / "2330_features.parquet"
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4)],
                "open": [100.0],
                "max": [101.0],
                "min": [99.0],
                "close": [100.0],
                "adjclose": [50.0],
                "Trading_Volume": [1000.0],
            }
        ),
        source,
        **metadata_overrides,
    )

    result, frame = _read_symbol_fallback(
        "2330",
        [(source, None)],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "failed"
    assert frame is None
    assert expected_message in (result.message or "")


def test_same_market_yahoo_alias_uses_unsuffixed_canonical_deterministically(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "2330_features.parquet"
    alias = tmp_path / "2330_TW_features.parquet"
    dates = [date(2000, 1, 4), date(2000, 1, 5)]
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": dates,
                "open": [100.0, 110.0],
                "max": [101.0, 111.0],
                "min": [99.0, 109.0],
                "close": [100.0, 110.0],
                "adjclose": [50.0, 55.0],
                "Trading_Volume": [1000.0, 1200.0],
            }
        ),
        canonical,
    )
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [*dates, date(2000, 1, 6)],
                "open": [100.0, 110.0, 900.0],
                "max": [101.0, 111.0, 901.0],
                "min": [99.0, 109.0, 899.0],
                "close": [100.0, 110.0, 900.0],
                "adjclose": [100.0, 80.0, 70.0],
                "Trading_Volume": [1000.0, 1200.0, 9000.0],
            }
        ),
        alias,
    )

    for sources in (
        [(alias, "TW"), (canonical, None)],
        [(canonical, None), (alias, "TW")],
    ):
        result, frame = _read_symbol_fallback(
            "2330",
            sources,
            manifest={"2330": ("TSMC", "twse")},
            official_markets={},
            start=date(2000, 1, 1),
            end=date(2000, 1, 31),
        )

        assert result.status == "ok", result.message
        assert frame is not None
        assert frame["market"].to_list() == ["twse", "twse"]
        assert frame["close"].to_list() == [100.0, 110.0]
        assert frame["source_adjclose"].to_list() == [50.0, 55.0]
        assert frame["source_factor"][0] is None
        assert math.isclose(frame["source_factor"][1], 1.1)


def test_same_market_yahoo_alias_still_requires_alias_metadata(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "2330_features.parquet"
    alias = tmp_path / "2330_TW_features.parquet"
    frame = pl.DataFrame(
        {
            "date": [date(2000, 1, 4)],
            "open": [100.0],
            "max": [101.0],
            "min": [99.0],
            "close": [100.0],
            "adjclose": [50.0],
            "Trading_Volume": [1000.0],
        }
    )
    _write_yahoo_source_parquet(frame, canonical)
    _write_yahoo_source_parquet(frame, alias, source=None)

    result, output = _read_symbol_fallback(
        "2330",
        [(alias, "TW"), (canonical, None)],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "failed"
    assert output is None
    assert "stockagent.source" in (result.message or "")


def test_same_market_yahoo_alias_ohlcv_conflict_stays_fail_closed(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "2330_features.parquet"
    alias = tmp_path / "2330_TW_features.parquet"
    for path, close in ((canonical, 100.0), (alias, 101.0)):
        _write_yahoo_source_parquet(
            pl.DataFrame(
                {
                    "date": [date(2000, 1, 4)],
                    "open": [close],
                    "max": [close + 1.0],
                    "min": [close - 1.0],
                    "close": [close],
                    "adjclose": [close / 2.0],
                    "Trading_Volume": [1000.0],
                }
            ),
            path,
        )

    result, frame = _read_symbol_fallback(
        "2330",
        [(alias, "TW"), (canonical, None)],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "failed"
    assert frame is None
    assert "same-market Yahoo alias OHLCV conflict on 1" in (result.message or "")


def test_cross_market_yahoo_alias_keeps_close_conflict_fail_closed(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "2330_features.parquet"
    tpex_alias = tmp_path / "2330_TWO_features.parquet"
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4)],
                "open": [100.0],
                "max": [101.0],
                "min": [99.0],
                "close": [100.0],
                "adjclose": [50.0],
                "Trading_Volume": [1000.0],
            }
        ),
        canonical,
    )
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4)],
                "open": [200.0],
                "max": [201.0],
                "min": [199.0],
                "close": [200.0],
                "adjclose": [100.0],
                "Trading_Volume": [2000.0],
            }
        ),
        tpex_alias,
    )

    result, frame = _read_symbol_fallback(
        "2330",
        [(canonical, None), (tpex_alias, "TWO")],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "failed"
    assert frame is None
    assert "conflict on 1 date-symbol keys" in (result.message or "")


def test_mixed_market_yahoo_group_drops_only_same_market_alias(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "2330_features.parquet"
    twse_alias = tmp_path / "2330_TW_features.parquet"
    tpex_source = tmp_path / "2330_TWO_features.parquet"
    for path, row_date, close, adjclose in (
        (canonical, date(2000, 1, 4), 100.0, 50.0),
        (twse_alias, date(2000, 1, 4), 100.0, 40.0),
        (tpex_source, date(2000, 1, 5), 200.0, 100.0),
    ):
        _write_yahoo_source_parquet(
            pl.DataFrame(
                {
                    "date": [row_date],
                    "open": [close],
                    "max": [close + 1.0],
                    "min": [close - 1.0],
                    "close": [close],
                    "adjclose": [adjclose],
                    "Trading_Volume": [1000.0],
                }
            ),
            path,
        )

    result, frame = _read_symbol_fallback(
        "2330",
        [
            (twse_alias, "TW"),
            (canonical, None),
            (tpex_source, "TWO"),
        ],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "ok", result.message
    assert frame is not None
    assert frame.select("date", "market", "close").rows() == [
        (date(2000, 1, 4), "twse", 100.0),
        (date(2000, 1, 5), "tpex", 200.0),
    ]


def test_yahoo_archive_receipts_same_market_alias_but_uses_canonical_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "yahoo"
    official_dir = tmp_path / "official"
    output = tmp_path / "fallback" / "yahoo_tw_ohlcv.parquet"
    input_dir.mkdir()
    official_dir.mkdir()
    pl.DataFrame(
        {
            "code": ["2330", "2330_TW"],
            "name": ["TSMC", "TSMC exact TW archive"],
            "market": ["TWSE", "tw_delisted"],
            "yahoo_symbol": ["2330.TW", "2330.TW"],
        }
    ).write_csv(input_dir / "symbols.csv")
    dates = [date(2000, 1, 4), date(2000, 1, 5)]
    for path, adjclose in (
        (input_dir / "2330_features.parquet", [50.0, 55.0]),
        (input_dir / "2330_TW_features.parquet", [100.0, 80.0]),
    ):
        _write_yahoo_source_parquet(
            pl.DataFrame(
                {
                    "date": dates,
                    "open": [100.0, 110.0],
                    "max": [101.0, 111.0],
                    "min": [99.0, 109.0],
                    "close": [100.0, 110.0],
                    "adjclose": adjclose,
                    "Trading_Volume": [1000.0, 1200.0],
                }
            ),
            path,
        )
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_tw_yahoo_fallback_archive.py",
            "--input-dir",
            str(input_dir),
            "--official-input-dir",
            str(official_dir),
            "--output-path",
            str(output),
            "--start-date",
            "2000-01-01",
            "--end-date",
            "2000-01-31",
            "--workers",
            "1",
        ],
    )

    build_yahoo_fallback_archive()
    _validate_yahoo_fallback_archive(output)

    archive = pl.read_parquet(output)
    assert archive["source_adjclose"].to_list() == [50.0, 55.0]
    assert archive["source_factor"][0] is None
    assert math.isclose(archive["source_factor"][1], 1.1)
    summary = json.loads(output.with_suffix(".summary.json").read_text())
    inputs = json.loads(output.with_suffix(".inputs.json").read_text())
    assert summary["source_symbol_count"] == 1
    assert summary["source_file_count"] == 2
    assert summary["verified_source_file_count"] == 2
    assert summary["manifest_record_count"] == 2
    assert inputs["file_count"] == 2
    assert {item["venue"] for item in inputs["files"]} == {None, "TW"}


def test_yahoo_fallback_archive_cli_writes_a_verifiable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "yahoo"
    official_dir = tmp_path / "official"
    output = tmp_path / "fallback" / "yahoo_tw_ohlcv.parquet"
    input_dir.mkdir()
    official_dir.mkdir()
    pl.DataFrame(
        {
            "code": ["2330", "2330_TW", "2330_TWO"],
            "name": ["TSMC", "TSMC legacy TW record", "TSMC legacy OTC record"],
            "market": ["TWSE", "TWSE", "TPEx"],
            "yahoo_symbol": ["2330.TW", "2330.TW", "2330.TWO"],
        }
    ).write_csv(input_dir / "symbols.csv")
    pl.DataFrame(
        {
            "code": ["2330", "2330_TWO"],
            "yahoo_symbol": ["2330.TW", "2330.TWO"],
            "status": ["not_found", "delisted_no_history"],
        }
    ).write_csv(input_dir / "download_report.csv")
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4), date(2000, 1, 5)],
                "open": [100.0, 102.0],
                "max": [101.0, 103.0],
                "min": [99.0, 101.0],
                "close": [100.0, 102.0],
                "adjclose": [50.0, 51.0],
                "Trading_Volume": [1000.0, 1200.0],
            }
        ),
        input_dir / "2330_features.parquet",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_tw_yahoo_fallback_archive.py",
            "--input-dir",
            str(input_dir),
            "--official-input-dir",
            str(official_dir),
            "--output-path",
            str(output),
            "--start-date",
            "2000-01-01",
            "--end-date",
            "2000-01-31",
            "--workers",
            "1",
        ],
    )

    build_yahoo_fallback_archive()
    _validate_yahoo_fallback_archive(output)

    archive = pl.read_parquet(output)
    assert archive.height == 2
    assert output.with_suffix(".summary.json").exists()
    assert output.with_suffix(".inputs.json").exists()
    assert output.with_suffix(".report.csv").exists()

    summary_path = output.with_suffix(".summary.json")
    input_manifest_path = output.with_suffix(".inputs.json")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    input_manifest = json.loads(input_manifest_path.read_text(encoding="utf-8"))
    assert summary["manifest_symbol_count"] == 1
    assert summary["manifest_record_count"] == 3
    assert summary["terminal_unavailable_record_count"] == 3
    assert input_manifest["terminal_unavailable_codes"] == [
        "2330",
        "2330_TW",
        "2330_TWO",
    ]
    summary["coverage_receipts_complete"] = False
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(RuntimeError, match="coverage_receipts_complete"):
        _validate_yahoo_fallback_archive(output)

    summary["coverage_receipts_complete"] = True
    input_manifest["files"][0]["checked_through"] = "2000-01-30"
    input_manifest_path.write_text(json.dumps(input_manifest), encoding="utf-8")
    summary["input_manifest_receipt"] = _receipt(input_manifest_path)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(RuntimeError, match="input_manifest_files"):
        _validate_yahoo_fallback_archive(output)


def test_yahoo_fallback_archive_remains_verifiable_after_source_detaches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "yahoo"
    official_dir = tmp_path / "official"
    output = tmp_path / "fallback" / "yahoo_tw_ohlcv.parquet"
    input_dir.mkdir()
    official_dir.mkdir()
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["TSMC"],
            "market": ["TWSE"],
            "yahoo_symbol": ["2330.TW"],
        }
    ).write_csv(input_dir / "symbols.csv")
    _write_yahoo_source_parquet(
        pl.DataFrame(
            {
                "date": [date(2000, 1, 4), date(2000, 1, 5)],
                "open": [100.0, 102.0],
                "max": [101.0, 103.0],
                "min": [99.0, 101.0],
                "close": [100.0, 102.0],
                "adjclose": [50.0, 51.0],
                "Trading_Volume": [1000.0, 1200.0],
            }
        ),
        input_dir / "2330_features.parquet",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_tw_yahoo_fallback_archive.py",
            "--input-dir",
            str(input_dir),
            "--official-input-dir",
            str(official_dir),
            "--output-path",
            str(output),
            "--start-date",
            "2000-01-01",
            "--end-date",
            "2000-01-31",
            "--workers",
            "1",
        ],
    )

    build_yahoo_fallback_archive()
    shutil.rmtree(input_dir)

    _validate_yahoo_fallback_archive(output)


def test_yahoo_whitelist_resolves_a_single_successful_venue(tmp_path: Path) -> None:
    path = tmp_path / "yahoo_whitelist.txt"
    path.write_text("3697.TW\n00631L.TW\nBAD\n", encoding="utf-8")

    assert _whitelist_markets(path) == {"3697": "twse", "00631L": "twse"}


def test_official_row_wins_over_yahoo_and_mixed_output_keeps_row_lineage(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    pl.DataFrame(
        {
            "date": [date(2004, 2, 11)],
            "證券代號": ["2330"],
            "證券名稱": ["台積電"],
            "開盤價": [20.0],
            "最高價": [21.0],
            "最低價": [19.0],
            "收盤價": [20.0],
            "成交股數": [2000.0],
            "漲跌(+/-)": ["+"],
            "漲跌價差": [1.0],
        }
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "代號": pl.String,
            "名稱": pl.String,
            "開盤": pl.Float64,
            "最高": pl.Float64,
            "最低": pl.Float64,
            "收盤": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌": pl.String,
        }
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    _write_verified_taiex_calendar(
        public_dir,
        [date(2000, 1, 4), date(2004, 2, 11)],
    )
    _write_empty_lifecycle_evidence(public_dir)
    fallback = tmp_path / "yahoo_fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2004, 2, 11)],
            "symbol": ["2330", "2330"],
            "name": ["TSMC", "TSMC"],
            "market": ["twse", "twse"],
            "open": [10.0, 999.0],
            "max": [10.5, 999.0],
            "min": [9.5, 999.0],
            "close": [10.0, 999.0],
            "Trading_Volume": [1000.0, 999.0],
            "source_adjclose": [5.0, 500.0],
            "source_factor": [None, 100.0],
            "quote_source": ["yahoo_fallback", "yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, merge_stats = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )
    official_row = frame.filter(pl.col("date") == date(2004, 2, 11)).row(0, named=True)
    assert official_row["close"] == 20.0
    assert official_row["quote_source"] == "twse_official"
    assert merge_stats.official_unusable_ohlcv_rows == 0

    result = _write_symbol(
        output_dir,
        "2330",
        frame,
        requested_end_date="2004-02-11",
        dry_run=False,
    )
    assert result.status == "created", result.message
    assert result.input_fallback_rows == 1
    assert result.fallback_rows == 0
    assert result.dropped_unanchored_fallback_rows == 1
    assert result.missing_adjustment_rows == 0
    output = pl.read_parquet(output_dir / "2330_features.parquet")
    assert output["data_source"].to_list() == ["twse_official"]
    assert output["adjustment_source"].to_list() == ["twse_official"]
    metadata = pq.read_schema(output_dir / "2330_features.parquet").metadata or {}
    assert metadata[b"stockagent.source"] == b"twse_tpex_official"


def test_yahoo_holiday_row_is_dropped_by_verified_taiex_calendar(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    public_dir.mkdir()
    pl.DataFrame(
        {
            "date": [date(2026, 7, 9)],
            "證券代號": ["2330"],
            "證券名稱": ["台積電"],
            "開盤價": [2410.0],
            "最高價": [2430.0],
            "最低價": [2400.0],
            "收盤價": [2415.0],
            "成交股數": [1000.0],
            "漲跌(+/-)": ["-"],
            "漲跌價差": [50.0],
        }
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "代號": pl.String,
            "名稱": pl.String,
            "開盤": pl.Float64,
            "最高": pl.Float64,
            "最低": pl.Float64,
            "收盤": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌": pl.String,
        }
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    _write_verified_taiex_calendar(
        public_dir,
        [date(2026, 7, 9)],
        effective_end=date(2026, 7, 10),
    )
    _write_empty_lifecycle_evidence(public_dir)
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2026, 7, 9), date(2026, 7, 10)],
            "symbol": ["2330", "2330"],
            "name": ["TSMC", "TSMC"],
            "market": ["twse", "twse"],
            "open": [2415.0, 2415.0],
            "max": [2415.0, 2415.0],
            "min": [2415.0, 2415.0],
            "close": [2415.0, 2415.0],
            "Trading_Volume": [1000.0, 0.0],
            "quote_source": ["yahoo_fallback", "yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, stats = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )

    assert frame.get_column("date").to_list() == [date(2026, 7, 9)]
    assert frame.get_column("quote_source").to_list() == ["twse_official"]
    assert stats.dropped_off_calendar_fallback_rows == 1
    assert stats.dropped_off_calendar_fallback_examples == ["2026-07-10:2330"]
    assert stats.session_calendar_rows == 2
    assert stats.session_calendar_receipt["name"] == "twse_taiex_ohlc.parquet"


def test_unverified_extreme_adjusted_transition_quarantines_label_not_quote(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    _write_core_official_sources(
        public_dir,
        twse=_empty_twse_quotes(),
        tpex=_empty_tpex_quotes(),
        sessions=[date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
    )
    _write_empty_lifecycle_evidence(public_dir)
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
            "symbol": ["2330", "2330", "2330"],
            "name": ["TSMC", "TSMC", "TSMC"],
            "market": ["twse", "twse", "twse"],
            "open": [10.0, 10.0, 40.0],
            "max": [10.0, 10.0, 40.0],
            "min": [10.0, 10.0, 40.0],
            "close": [10.0, 10.0, 40.0],
            "Trading_Volume": [1000.0, 1000.0, 1000.0],
            "source_adjclose": [10.0, 10.0, 40.0],
            "quote_source": [
                "yahoo_fallback",
                "yahoo_fallback",
                "yahoo_fallback",
            ],
        }
    ).write_parquet(fallback)

    frame, _, _, _, _ = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )
    result = _write_symbol(
        output_dir,
        "2330",
        frame,
        requested_end_date="2024-01-04",
        dry_run=False,
    )

    assert result.status == "excluded_unverified_fallback", result.message
    assert result.dropped_unanchored_fallback_rows == 3
    assert not (output_dir / "2330_features.parquet").exists()


def test_unused_official_reference_does_not_verify_yahoo_extreme(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    sessions = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    _write_core_official_sources(
        public_dir,
        twse=_empty_twse_quotes(),
        tpex=_empty_tpex_quotes(),
        sessions=sessions,
    )
    _write_empty_lifecycle_evidence(public_dir)
    pl.DataFrame(
        {
            "date": [sessions[-1]],
            "symbol": ["2330"],
            "reference_price": [10.0],
            "opening_reference_price": [None],
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": sessions,
            "symbol": ["2330"] * 3,
            "name": ["TSMC"] * 3,
            "market": ["twse"] * 3,
            "open": [10.0, 10.0, 40.0],
            "max": [10.0, 10.0, 40.0],
            "min": [10.0, 10.0, 40.0],
            "close": [10.0, 10.0, 40.0],
            "Trading_Volume": [1000.0] * 3,
            "source_adjclose": [10.0, 10.0, 40.0],
            "source_factor": [1.0, 1.0, 4.0],
            "quote_source": ["yahoo_fallback"] * 3,
        }
    ).write_parquet(fallback)

    frame, _, _, _, _ = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )
    result = _write_symbol(
        output_dir,
        "2330",
        frame,
        requested_end_date="2024-01-04",
        dry_run=False,
    )

    assert result.status == "excluded_unverified_fallback", result.message
    assert result.dropped_unanchored_fallback_rows == 3
    assert not (output_dir / "2330_features.parquet").exists()


def test_official_listing_boundary_quarantines_prelisting_to_listing_label(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    twse = pl.DataFrame(
        {
            "date": [date(2024, 1, 3)],
            "證券代號": ["2330"],
            "證券名稱": ["台積電"],
            "開盤價": [40.0],
            "最高價": [40.0],
            "最低價": [40.0],
            "收盤價": [40.0],
            "成交股數": [1000.0],
            "漲跌(+/-)": ["+"],
            "漲跌價差": [30.0],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=twse,
        tpex=_empty_tpex_quotes(),
        sessions=[date(2024, 1, 2), date(2024, 1, 3)],
    )
    _write_empty_lifecycle_evidence(public_dir)
    pl.DataFrame(
        {
            "公司代號": ["2330"],
            "公司簡稱": ["台積電"],
            "公司名稱": ["台灣積體電路製造股份有限公司"],
            "上市日期": ["20240103"],
        }
    ).write_parquet(public_dir / "twse_listed_company_basic.parquet")
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 1, 2)],
            "symbol": ["2330"],
            "name": ["TSMC prelisting"],
            "market": ["twse"],
            "open": [10.0],
            "max": [10.0],
            "min": [10.0],
            "close": [10.0],
            "Trading_Volume": [1000.0],
            "source_adjclose": [10.0],
            "quote_source": ["yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, _ = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )
    assert frame.filter(pl.col("date") == date(2024, 1, 3)).select(
        "_official_listing_boundary"
    ).item() is True
    result = _write_symbol(
        output_dir,
        "2330",
        frame,
        requested_end_date="2024-01-03",
        dry_run=False,
    )

    assert result.status == "created", result.message
    assert result.listing_boundary_quarantined_rows == 0
    assert result.dropped_unanchored_fallback_rows == 1
    output = pl.read_parquet(output_dir / "2330_features.parquet").sort("date")
    assert output.get_column("close").to_list() == [40.0]
    assert output.get_column("return_quarantine_reason").to_list() == [None]


def test_yahoo_rows_after_terminal_delisting_are_excluded_without_relisting(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    twse = pl.DataFrame(
        {
            "date": [date(2007, 1, 4)],
            "證券代號": ["9801"],
            "證券名稱": ["中國力霸"],
            "開盤價": [2.5],
            "最高價": [2.6],
            "最低價": [2.4],
            "收盤價": [2.5],
            "成交股數": [1000.0],
            "漲跌(+/-)": ["-"],
            "漲跌價差": [0.1],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=twse,
        tpex=_empty_tpex_quotes(),
        sessions=[date(2007, 1, 4), date(2025, 1, 2), date(2025, 1, 3)],
    )
    _write_empty_lifecycle_evidence(public_dir)
    pl.DataFrame(
        {
            "date": ["2007-04-11"],
            "market": ["twse"],
            "symbol": ["9801"],
            "company_name": ["中國力霸"],
            "delisting_reason": ["終止上市"],
        }
    ).write_parquet(public_dir / "twse_delisted_company.parquet")
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2025, 1, 2), date(2025, 1, 3)],
            "symbol": ["9801", "9801"],
            "name": ["Synthetic 9801", "Synthetic 9801"],
            "market": ["twse", "twse"],
            "open": [20.0, 20.5],
            "max": [20.5, 21.0],
            "min": [19.5, 20.0],
            "close": [20.0, 20.5],
            "Trading_Volume": [1000.0, 1200.0],
            "quote_source": ["yahoo_fallback", "yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, stats = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )

    assert frame.select("date", "symbol", "quote_source").to_dicts() == [
        {
            "date": date(2007, 1, 4),
            "symbol": "9801",
            "quote_source": "twse_official",
        }
    ]
    assert stats.fallback_rows_before_lifecycle_filter == 2
    assert stats.fallback_rows_after_lifecycle_filter == 0
    assert stats.dropped_post_terminal_fallback_rows == 2
    assert stats.lifecycle_terminal_events == 1
    assert "2007-04-11" in stats.dropped_post_terminal_fallback_examples[0]
    result = _write_symbol(
        output_dir,
        "9801",
        frame,
        requested_end_date="2025-01-03",
        dry_run=False,
    )
    assert result.status == "created", result.message
    assert result.fallback_rows == 0
    assert pl.read_parquet(output_dir / "9801_features.parquet").get_column(
        "date"
    ).to_list() == [date(2007, 1, 4)]


def test_verified_relisting_reopens_new_episode_and_resets_adjusted_index(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    twse = pl.DataFrame(
        {
            "date": [date(2007, 1, 4)],
            "證券代號": ["9801"],
            "證券名稱": ["舊公司"],
            "開盤價": [2.5],
            "最高價": [2.6],
            "最低價": [2.4],
            "收盤價": [2.5],
            "成交股數": [1000.0],
            "漲跌(+/-)": ["-"],
            "漲跌價差": [0.1],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=twse,
        tpex=_empty_tpex_quotes(),
        sessions=[date(2007, 1, 4), date(2024, 12, 31), date(2025, 1, 2)],
    )
    _write_empty_lifecycle_evidence(public_dir)
    pl.DataFrame(
        {
            "date": ["2007-04-11"],
            "market": ["twse"],
            "symbol": ["9801"],
            "company_name": ["舊公司"],
            "delisting_reason": ["終止上市"],
        }
    ).write_parquet(public_dir / "twse_delisted_company.parquet")
    pl.DataFrame(
        {
            "公司代號": ["9801"],
            "公司簡稱": ["新公司"],
            "公司名稱": ["新公司股份有限公司"],
            "上市日期": ["20250102"],
        }
    ).write_parquet(public_dir / "twse_listed_company_basic.parquet")
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 12, 31), date(2025, 1, 2)],
            "symbol": ["9801", "9801"],
            "name": ["新公司", "新公司"],
            "market": ["twse", "twse"],
            "open": [19.0, 20.0],
            "max": [19.5, 20.5],
            "min": [18.5, 19.5],
            "close": [19.0, 20.0],
            "Trading_Volume": [1000.0, 1000.0],
            "quote_source": ["yahoo_fallback", "yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, stats = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )
    assert stats.dropped_post_terminal_fallback_rows == 1
    reopened = frame.filter(pl.col("date") == date(2025, 1, 2)).row(0, named=True)
    assert reopened["_lifecycle_episode_id"] == 1
    assert reopened["_lifecycle_evidence"].startswith("official_relisting:")
    result = _write_symbol(
        output_dir,
        "9801",
        frame,
        requested_end_date="2025-01-02",
        dry_run=False,
    )
    assert result.status == "created", result.message
    output = pl.read_parquet(output_dir / "9801_features.parquet").sort("date")
    assert output.get_column("adjclose").to_list() == [10.0]
    assert output.get_column("lifecycle_episode_id").to_list() == [0]
    assert output.get_column("return_quarantined").to_list() == [False]
    assert output.get_column("return_quarantine_reason").to_list() == [None]
    assert result.dropped_unanchored_fallback_rows == 1


def test_next_official_session_tpex_to_twse_transfer_is_not_terminal(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    public_dir.mkdir()
    twse = pl.DataFrame(
        {
            "date": [date(2012, 8, 20)],
            "證券代號": ["4722"],
            "證券名稱": ["國精化"],
            "開盤價": [20.0],
            "最高價": [20.5],
            "最低價": [19.5],
            "收盤價": [20.0],
            "成交股數": [1000.0],
            "漲跌(+/-)": ["+"],
            "漲跌價差": [0.1],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=twse,
        tpex=_empty_tpex_quotes(),
        sessions=[date(2012, 8, 17), date(2012, 8, 20), date(2012, 8, 21)],
    )
    _write_empty_lifecycle_evidence(public_dir)
    pl.DataFrame(
        {
            "date": ["2012-08-18"],
            "market": ["tpex"],
            "symbol": ["4722"],
            "company_name": ["國精化學"],
            "delisting_reason": ["櫃轉市"],
        }
    ).write_parquet(public_dir / "tpex_delisted_company.parquet")
    pl.DataFrame(
        {
            "公司代號": ["4722"],
            "公司簡稱": ["國精化"],
            "公司名稱": ["國精化學股份有限公司"],
            "上市日期": ["20120820"],
        }
    ).write_parquet(public_dir / "twse_listed_company_basic.parquet")
    pl.DataFrame(
        {
            "Code": ["4722"],
            "Company": ["國精化"],
            "ListingDate": [""],
            "ApprovedListingDate": ["1010820"],
            "Note": ["櫃轉市"],
        }
    ).write_parquet(public_dir / "twse_api_company_newlisting.parquet")
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2012, 8, 21)],
            "symbol": ["4722"],
            "name": ["國精化"],
            "market": ["twse"],
            "open": [20.2],
            "max": [20.5],
            "min": [20.0],
            "close": [20.2],
            "Trading_Volume": [1000.0],
            "quote_source": ["yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, stats = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )
    assert stats.lifecycle_nonterminal_transfer_events == 1
    assert stats.lifecycle_terminal_events == 0
    assert stats.dropped_post_terminal_fallback_rows == 0
    assert frame.filter(pl.col("date") == date(2012, 8, 21)).height == 1
    assert frame.filter(pl.col("date") == date(2012, 8, 20)).select(
        "_official_listing_boundary"
    ).item() is False


def test_previous_official_session_next_reference_repairs_exact_adjustment_gap(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    tpex = pl.DataFrame(
        {
            "date": [date(2004, 11, 2), date(2004, 11, 3)],
            "代號": ["6217", "6217"],
            "名稱": ["中國探針", "中國探針"],
            "開盤": [22.5, 19.5],
            "最高": [23.5, 20.0],
            "最低": [22.0, 19.2],
            "收盤": [23.2, 19.6],
            "成交股數": [1000.0, 1200.0],
            "漲跌": ["+0.9", "除權息"],
            # Only the previous row's next-session reference prices today.
            # Today's value is tomorrow's reference and must never be borrowed.
            "次日參考價": [19.9, 18.5],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=_empty_twse_quotes(),
        tpex=tpex,
        sessions=[date(2004, 11, 2), date(2004, 11, 3)],
    )

    frame, _, _, _, stats = _official_frame(public_dir)
    assert stats.previous_session_next_reference_candidate_rows == 1
    result = _write_symbol(
        output_dir,
        "6217",
        frame,
        requested_end_date="2004-11-03",
        dry_run=False,
    )
    assert result.status == "created", result.message
    assert result.missing_adjustment_rows == 0
    assert result.previous_session_next_reference_adjustment_rows == 1
    output = pl.read_parquet(output_dir / "6217_features.parquet").sort("date")
    assert math.isclose(output["adjclose"][1] / output["adjclose"][0], 19.6 / 19.9)
    assert output["adjustment_source"].to_list() == [
        "tpex_official",
        PREVIOUS_SESSION_NEXT_REFERENCE_SOURCE_NAME,
    ]
    assert output["adjustment_reference_date"].to_list() == [
        None,
        date(2004, 11, 2),
    ]
    assert output["adjustment_reference_price"].to_list() == [None, 19.9]
    assert output["adjustment_reference_kind"].to_list() == [
        None,
        "previous_official_session_next_reference",
    ]


def test_next_reference_is_not_used_across_a_missing_official_session(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    tpex = pl.DataFrame(
        {
            "date": [date(2004, 11, 2), date(2004, 11, 4)],
            "代號": ["6217", "6217"],
            "名稱": ["中國探針", "中國探針"],
            "開盤": [22.5, 19.5],
            "最高": [23.5, 20.0],
            "最低": [22.0, 19.2],
            "收盤": [23.2, 19.6],
            "成交股數": [1000.0, 1200.0],
            "漲跌": ["+0.9", "除權息"],
            "次日參考價": [19.9, 18.5],
        }
    )
    _write_core_official_sources(
        public_dir,
        twse=_empty_twse_quotes(),
        tpex=tpex,
        sessions=[
            date(2004, 11, 2),
            date(2004, 11, 3),
            date(2004, 11, 4),
        ],
    )

    frame, _, _, _, stats = _official_frame(public_dir)
    assert stats.previous_session_next_reference_candidate_rows == 0
    result = _write_symbol(
        output_dir,
        "6217",
        frame,
        requested_end_date="2004-11-04",
        dry_run=False,
    )
    assert result.status == "created", result.message
    assert result.previous_session_next_reference_adjustment_rows == 0
    assert result.missing_adjustment_rows == 1
    output = pl.read_parquet(output_dir / "6217_features.parquet").sort("date")
    assert output["adjustment_reference_date"].to_list() == [None, None]
    assert math.isnan(output["adjclose"][1])


def test_unusable_official_bar_uses_yahoo_with_explicit_reason(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "證券代號": pl.String,
            "證券名稱": pl.String,
            "開盤價": pl.Float64,
            "最高價": pl.Float64,
            "最低價": pl.Float64,
            "收盤價": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌(+/-)": pl.String,
            "漲跌價差": pl.Float64,
        }
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        {
            "date": [date(2007, 1, 2)],
            "代號": ["4801"],
            "名稱": ["碼斯特"],
            "開盤": [0.0],
            "最高": [0.0],
            "最低": [0.0],
            "收盤": [0.0],
            "成交股數": [240.0],
            "漲跌": ["---"],
        }
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    _write_verified_taiex_calendar(public_dir, [date(2007, 1, 2)])
    _write_empty_lifecycle_evidence(public_dir)
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2007, 1, 2)],
            "symbol": ["4801"],
            "name": ["Master"],
            "market": ["tpex"],
            "open": [26.7],
            "max": [26.7],
            "min": [26.7],
            "close": [26.7],
            "Trading_Volume": [240.0],
            "quote_source": ["yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _, stats = _official_frame(
        public_dir,
        fallback_paths=[fallback],
    )

    row = frame.row(0, named=True)
    assert row["quote_source"] == "yahoo_fallback"
    assert row["fallback_reason"] == "official_ohlcv_unusable"
    assert stats.official_unusable_ohlcv_rows == 1
    assert stats.fallback_replaced_unusable_official_rows == 1
    assert stats.unfilled_unusable_official_rows == 0

    result = _write_symbol(
        output_dir,
        "4801",
        frame,
        requested_end_date="2007-01-02",
        dry_run=False,
    )
    assert result.status == "excluded_unverified_fallback"
    assert result.dropped_unanchored_fallback_rows == 1
    assert not (output_dir / "4801_features.parquet").exists()


def test_unusable_official_bar_without_fallback_is_counted_and_omitted(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    public_dir.mkdir()
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "證券代號": pl.String,
            "證券名稱": pl.String,
            "開盤價": pl.Float64,
            "最高價": pl.Float64,
            "最低價": pl.Float64,
            "收盤價": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌(+/-)": pl.String,
            "漲跌價差": pl.Float64,
        }
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        {
            "date": [date(2007, 1, 2), date(2007, 1, 3)],
            "代號": ["4801", "4801"],
            "名稱": ["碼斯特", "碼斯特"],
            "開盤": [0.0, 26.8],
            "最高": [0.0, 27.0],
            "最低": [0.0, 26.5],
            "收盤": [0.0, 26.8],
            "成交股數": [240.0, 1000.0],
            "漲跌": ["---", "+0.1"],
        }
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    _write_verified_taiex_calendar(
        public_dir,
        [date(2007, 1, 2), date(2007, 1, 3)],
    )

    frame, _, _, _, stats = _official_frame(public_dir)

    assert frame.get_column("date").to_list() == [date(2007, 1, 3)]
    assert stats.official_unusable_ohlcv_rows == 1
    assert stats.fallback_replaced_unusable_official_rows == 0
    assert stats.unfilled_unusable_official_rows == 1
    assert stats.unfilled_unusable_official_examples == ["2007-01-02:4801"]


def test_legacy_official_zero_ohlc_sentinel_becomes_audited_flat_close_bar(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "證券代號": pl.String,
            "證券名稱": pl.String,
            "開盤價": pl.Float64,
            "最高價": pl.Float64,
            "最低價": pl.Float64,
            "收盤價": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌(+/-)": pl.String,
            "漲跌價差": pl.Float64,
        }
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        {
            "date": [date(2003, 9, 12)],
            "代號": ["4522"],
            "名稱": ["大寶"],
            "開盤": [0.0],
            "最高": [0.0],
            "最低": [0.0],
            "收盤": [16.0],
            "成交股數": [50.0],
            "漲跌": ["0"],
        }
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    _write_verified_taiex_calendar(public_dir, [date(2003, 9, 12)])

    frame, _, _, _, merge_stats = _official_frame(public_dir)
    row = frame.row(0, named=True)
    assert [row["open"], row["max"], row["min"], row["close"]] == [
        16.0,
        16.0,
        16.0,
        16.0,
    ]
    assert row["quote_source"] == "tpex_official"
    assert row["_official_zero_ohlc_normalized"] is True
    assert merge_stats.official_unusable_ohlcv_rows == 0

    result = _write_symbol(
        output_dir,
        "4522",
        frame,
        requested_end_date="2003-09-12",
        dry_run=False,
    )
    assert result.status == "created", result.message
    assert result.normalized_zero_ohlc_rows == 1
    output = pl.read_parquet(output_dir / "4522_features.parquet")
    assert output.select("open", "max", "min", "close").row(0) == (
        16.0,
        16.0,
        16.0,
        16.0,
    )
    assert output["data_source"].to_list() == ["tpex_official"]
    assert output["ohlc_normalization"].to_list() == [
        "official_close_flat_bar"
    ]


def test_official_ohlcv_can_use_yahoo_factor_only_when_official_factor_is_missing(
    tmp_path: Path,
) -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5)],
            "symbol": ["2330", "2330"],
            "name": ["TSMC", "TSMC"],
            "market": ["twse", "twse"],
            "open": [100.0, 105.0],
            "max": [101.0, 106.0],
            "min": [99.0, 104.0],
            "close": [100.0, 105.0],
            "Trading_Volume": [1000.0, 1200.0],
            "signed_change": [0.0, None],
            "source_reference": [None, None],
            "source_adjclose": [None, None],
            "source_factor": [1.0, None],
            "quote_source": ["twse_official", "twse_official"],
            "_legacy_source_id": [-1, -1],
            "_source_priority": [2, 2],
            "_yahoo_fallback_factor": [None, 1.05],
            "_yahoo_fallback_close": [100.0, 105.0],
            "reference_override": [None, None],
            "security_type": ["stock", "stock"],
        }
    )

    result = _write_symbol(
        tmp_path,
        "2330",
        frame,
        requested_end_date="2000-01-05",
        dry_run=False,
    )
    output = pl.read_parquet(tmp_path / "2330_features.parquet")

    assert result.missing_adjustment_rows == 0
    assert result.fallback_adjustment_rows == 1
    assert output["data_source"].to_list() == ["twse_official", "twse_official"]
    assert output["adjustment_source"].to_list() == [
        "twse_official",
        "yahoo_fallback",
    ]
    assert math.isclose(output["adjclose"][1] / output["adjclose"][0], 1.05)
def test_daily_close_receipt_allows_only_certified_optional_publication_lag(
    tmp_path: Path,
) -> None:
    corporate_path = tmp_path / "tw_corporate_action_reference.parquet"
    corporate_path.write_bytes(b"certified-corporate-reference")
    (tmp_path / "tw_corporate_action_reference.summary.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "coverage_complete": True,
                "failure_count": 0,
                "output_receipt": _receipt(corporate_path),
            }
        ),
        encoding="utf-8",
    )
    public_summary_path = tmp_path / "download_summary.json"
    public_summary = {
        "mode": "daily",
        "coverage_complete": False,
        "failed_count": 2,
        "blocking_failed_count": 0,
        "daily_close_ready": True,
        "publication_lag_datasets": [
            "tpex_daily_valuation",
            "tpex_margin_balance",
            "twse_day_trade_eligibility",
            "twse_institutional_trades",
            "twse_margin_balance",
        ],
    }
    public_summary_path.write_text(json.dumps(public_summary), encoding="utf-8")

    _validate_download_receipts(
        tmp_path,
        allow_incomplete=False,
        allow_daily_publication_lag=True,
    )

    public_summary["publication_lag_datasets"] = ["twse_daily_ohlcv"]
    public_summary_path.write_text(json.dumps(public_summary), encoding="utf-8")
    with pytest.raises(RuntimeError, match="complete source receipts"):
        _validate_download_receipts(
            tmp_path,
            allow_incomplete=False,
            allow_daily_publication_lag=True,
        )


def test_complete_receipt_accepts_only_declared_nonblocking_failures(
    tmp_path: Path,
) -> None:
    corporate_path = tmp_path / "tw_corporate_action_reference.parquet"
    corporate_path.write_bytes(b"certified-corporate-reference")
    (tmp_path / "tw_corporate_action_reference.summary.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "coverage_complete": True,
                "failure_count": 0,
                "output_receipt": _receipt(corporate_path),
            }
        ),
        encoding="utf-8",
    )
    public_summary_path = tmp_path / "download_summary.json"
    public_summary = {
        "mode": "daily",
        "coverage_complete": True,
        "failed_count": 1,
        "allowed_failed_count": 1,
        "blocking_failed_count": 0,
        "allowed_failed_datasets": ["mof_tax_revenue"],
        "configured_allowed_failed_datasets": ["mof_tax_revenue"],
    }
    public_summary_path.write_text(json.dumps(public_summary), encoding="utf-8")

    _validate_download_receipts(tmp_path, allow_incomplete=False)

    public_summary["blocking_failed_count"] = 1
    public_summary_path.write_text(json.dumps(public_summary), encoding="utf-8")
    with pytest.raises(RuntimeError, match="public failed_count=1"):
        _validate_download_receipts(tmp_path, allow_incomplete=False)
