from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from stockagent.data.tw_public_research_features import (
    _event_source_coverage,
    build_tw_public_research_features,
    research_source_columns,
)


def test_research_columns_remove_log_asinh_but_keep_ratios_and_rules() -> None:
    assert research_source_columns([
        "date", "symbol", "twpub_pe_log", "twpub_financial_net_income_asinh",
        "twpub_margin_buy_flow", "twpub_cbc_fx_reserves_chg",
        "twpub_official_turnover_ratio",
        "twpub_pe_raw", "_twpub_can_sell",
    ]) == [
        "date", "symbol", "twpub_official_turnover_ratio",
        "twpub_pe_raw", "_twpub_can_sell",
    ]


def test_research_builder_backdates_obtainable_values_only_to_estimated_release(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    base_path = source / "features" / "strict.parquet"
    base_path.parent.mkdir()
    pl.DataFrame({
        "date": [date(2014, 5, 15), date(2014, 5, 16), date(2014, 5, 16)],
        "symbol": ["__MARKET__", "__MARKET__", "2330"],
        "twpub_pe_log": [None, None, 1.5],
        "twpub_official_turnover_ratio": [None, None, 0.05],
        "twpub_cbc_m2_raw": [None, None, None],
        "_twpub_can_sell": [None, None, 1.0],
    }).write_parquet(base_path)
    pl.DataFrame({"date": [date(2014, 5, 15), date(2014, 5, 16)]}).write_parquet(
        source / "twse_taiex_ohlc.parquet"
    )
    macro_path = tmp_path / "macro.parquet"
    pl.DataFrame({
        "effective_session": [date(2014, 5, 16)],
        "feature": ["twpub_cbc_m2_raw"],
        "subject_period": ["2014-04"],
        "source_value": [123.0],
    }).write_parquet(macro_path)
    xbrl = source / "mops_xbrl"
    xbrl.mkdir()
    pl.DataFrame({
        "archive_sha256": ["archive"],
        "document_sha256": ["document"],
        "source_member": ["2330.xml"],
        "company_id": ["2330"],
        "period": ["2014Q1"],
        "report_period_end": ["2014-03-31"],
        "publication_date_taipei": ["2014-05-15"],
        "publication_time_basis": ["estimated_from_issuer_board_authorization"],
    }).write_parquet(xbrl / "publication_candidates.parquet")
    fact_path = xbrl / "normalized/ifrs/2014Q1/archive/facts.parquet"
    fact_path.parent.mkdir(parents=True)
    pl.DataFrame({
        "archive_sha256": ["archive"] * 5,
        "document_sha256": ["document"] * 5,
        "source_member": ["2330.xml"] * 5,
        "entity_identifier": ["2330"] * 5,
        "report_period_end": ["2014-03-31"] * 5,
        "concept": [
            "{http://xbrl.iasb.org/taxonomy/2013-03-28/ifrs-full}Assets",
            "{http://www.xbrl.org/tifrs/bsci/ci/2014-03-31}OperatingRevenue",
            "{http://www.xbrl.org/tifrs/bsci/ci/2014-03-31}GrossProfitLossFromOperationsNet",
            "{http://www.xbrl.org/tifrs/bsci/ci/2014-03-31}NetOperatingIncomeLoss",
            "{http://xbrl.iasb.org/taxonomy/2010-04-30/ifrs}BasicEarningsLossPerShare",
        ],
        "unit_ref": ["TWD"] * 5,
        "period_start": [None, "2014-01-01", "2014-01-01", "2014-01-01", "2014-01-01"],
        "period_end": [None, "2014-03-31", "2014-03-31", "2014-03-31", "2014-03-31"],
        "period_instant": ["2014-03-31", None, None, None, None],
        "dimensions_json": ["[]"] * 5,
        "decimal_value": ["1000000", "100000", "40000", "25000", "1.85"],
        "value_parse_status": ["parsed"] * 5,
    }).write_parquet(fact_path)
    output = source / "features/research.parquet"
    summary = build_tw_public_research_features(
        base_path=base_path, macro_events_path=macro_path, xbrl_root=xbrl,
        calendar_path=source / "twse_taiex_ohlc.parquet", output_path=output,
        minimum_year=2013,
    )
    frame = pl.read_parquet(output).sort(["date", "symbol"])
    assert "twpub_pe_log" not in frame.columns
    assert frame.filter(pl.col("date") == date(2014, 5, 15)).get_column(
        "twpub_cbc_m2_raw"
    ).to_list() == [None]
    assert frame.filter((pl.col("date") == date(2014, 5, 16)) & (
        pl.col("symbol") == "__MARKET__"
    )).get_column("twpub_cbc_m2_raw").to_list() == [123.0]
    assert frame.filter(pl.col("symbol") == "2330").get_column(
        "twpub_xbrl_assets_twd_raw"
    ).to_list() == [1_000_000.0]
    issuer = frame.filter(pl.col("symbol") == "2330")
    assert issuer.get_column("twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw").to_list() == [100000.0]
    assert issuer.get_column("twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw").to_list() == [100000.0]
    assert issuer.get_column("twpub_xbrl_tifrs_net_gross_margin").to_list() == [0.4]
    assert issuer.get_column("twpub_xbrl_basic_eps_quarter_raw").to_list() == [1.85]
    assert issuer.get_column("twpub_xbrl_basic_eps_ytd_raw").to_list() == [1.85]
    assert summary["xbrl"]["events"] == 9
    assert summary["macro"]["events"] == 1
    original_inode = output.stat().st_ino
    again = build_tw_public_research_features(
        base_path=base_path, macro_events_path=macro_path, xbrl_root=xbrl,
        calendar_path=source / "twse_taiex_ohlc.parquet", output_path=output,
        minimum_year=2013,
    )
    assert again["reused"] is True
    assert output.stat().st_ino == original_inode


def test_event_coverage_requires_both_venues_and_respects_open_boundary(
    tmp_path: Path,
) -> None:
    sessions = [date(2026, 9, 17), date(2026, 9, 18), date(2026, 9, 21)]
    # 00:50 UTC is 08:50 Taipei and can cover today's pre-open decision;
    # 01:10 UTC is 09:10 Taipei and first covers the next session.
    pl.DataFrame({"_downloaded_at_utc": [
        "2026-09-17T00:50:00+00:00", "2026-09-17T01:10:00+00:00",
    ]}).write_parquet(tmp_path / "twse_notice_stock.parquet")
    pl.DataFrame({"_downloaded_at_utc": [
        "2026-09-17T00:50:00+00:00",
    ]}).write_parquet(tmp_path / "tpex_attention_stock.parquet")
    pl.DataFrame({"_downloaded_at_utc": [
        "2026-09-17T00:50:00+00:00",
    ]}).write_parquet(tmp_path / "twse_disposal_stock.parquet")
    frame, summary = _event_source_coverage(tmp_path, sessions)
    assert frame.get_column("date").to_list() == [date(2026, 9, 17)]
    assert frame.get_column("twpub_attention_source_covered").to_list() == [1.0]
    assert frame.get_column("twpub_disposal_source_covered").to_list() == [None]
    assert summary["source_sessions"]["twse_notice_stock"] == 2
