#!/usr/bin/env python3
"""Read-only inventory of registered Taiwan public download products.

Presence is not historical completeness or point-in-time certification.  Use
the downloader's coverage/audit receipts for those stronger claims.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from downloader.download_tw_public_data import EXTENDED_DATASETS


OFFICIAL_RELEASE_ARCHIVES = (
    (
        "dgbas_release_vintages", "DGBAS",
        "https://www.stat.gov.tw/News.aspx?n=2668&sms=10980",
        "CPI, unemployment and GDP original dated releases",
    ),
    (
        "cbc_fx_reserve_release_vintages", "CBC",
        "https://www.cbc.gov.tw/tw/lp-302-1-1-20.html",
        "Original dated foreign-reserve press releases",
    ),
    (
        "cbc_money_release_vintages", "CBC",
        "https://www.cbc.gov.tw/tw/lp-302-1-1-20.html",
        "Original dated M1B/M2 year-on-year money-supply releases",
    ),
)


def inventory(root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in sorted(EXTENDED_DATASETS.values(), key=lambda item: item.name):
        path = root / f"{spec.name}.parquet"
        state_path = root / "state" / f"{spec.name}.json"
        state: dict[str, object] = {}
        if state_path.is_file():
            try:
                parsed = json.loads(state_path.read_text(encoding="utf-8"))
                if isinstance(parsed, dict):
                    state = parsed
            except (OSError, ValueError):
                pass
        unavailable = state.get("confirmed_source_unavailable_dates")
        unavailable_count = (
            len(unavailable) if isinstance(unavailable, list)
            else int(unavailable or 0) if isinstance(unavailable, (int, float))
            else 0
        )
        row_count: int | None = None
        size_bytes: int | None = None
        schema_error: str | None = None
        if path.is_file():
            size_bytes = path.stat().st_size
            try:
                row_count = int(pq.ParquetFile(path, memory_map=True).metadata.num_rows)
            except Exception as exc:
                schema_error = f"{type(exc).__name__}: {exc}"
        mode = {
            "historical_json_table": "official_date_query",
            "delisted_history": "official_history_listing",
            "snapshot_url": "current_snapshot_archive_from_first_capture",
            "data_gov": (
                "current_snapshot_archive_from_first_capture"
                if "snapshot" in spec.tags
                else "bulk_history_with_mutable_values_archive_from_first_capture"
            ),
            "gcis_catalog": "official_resource_catalog_with_versioned_csv",
            "fsc_catalog": "official_resource_catalog_with_versioned_csv",
        }.get(spec.kind, "unknown")
        rows.append(
            {
                "dataset": spec.name,
                "source": spec.source,
                "kind": spec.kind,
                "history_mode": mode,
                "declared_earliest": spec.start_date,
                "url": spec.url or spec.url_template or (
                    f"https://data.gov.tw/dataset/{spec.data_gov_id}"
                    if spec.data_gov_id else None
                ),
                "description": spec.description,
                "tags": list(spec.tags),
                "present": path.is_file(),
                "rows": row_count,
                "bytes": size_bytes,
                "schema_error": schema_error,
                "state_coverage_complete": (
                    all(item.get("complete") is True for item in state.get("datasets", {}).values())
                    and len(state.get("datasets", {})) >= int(state.get("catalog_entry_count") or 0)
                    and bool(state.get("catalog_entry_count"))
                    if spec.kind in {"gcis_catalog", "fsc_catalog"}
                    else state.get("coverage_complete")
                ),
                "state_checked_through": (
                    state.get("last_catalog_check_utc")
                    if spec.kind in {"gcis_catalog", "fsc_catalog"}
                    else state.get("checked_through")
                ),
                "state_missing_dates_after": state.get("missing_dates_after"),
                "state_source_unavailable_dates": unavailable_count,
                "historical_completeness": (
                    "requires_receipt_and_calendar_audit"
                    if spec.kind == "historical_json_table"
                    else "requires_per_resource_archive_audit"
                    if spec.kind in {"gcis_catalog", "fsc_catalog"}
                    else "requires_source_specific_audit"
                    if spec.kind == "delisted_history"
                    else "not_a_historical_archive"
                    if mode == "current_snapshot_archive_from_first_capture"
                    else "requires_original_release_audit"
                ),
                "historical_pit_values": (
                    "requires_original_vintages_and_release_receipts"
                    if spec.kind in {"snapshot_url", "data_gov", "gcis_catalog", "fsc_catalog"}
                    else "requires_source_timing_audit"
                ),
            }
        )
    for name, source, url, description in OFFICIAL_RELEASE_ARCHIVES:
        path = root / f"{name}.parquet"
        state_path = root / "state" / f"{name}.json"
        try:
            state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.is_file() else {}
            if not isinstance(state, dict):
                state = {}
        except (OSError, ValueError):
            state = {}
        row_count = None
        schema_error = None
        if path.is_file():
            try:
                row_count = int(pq.ParquetFile(path, memory_map=True).metadata.num_rows)
            except Exception as exc:
                schema_error = f"{type(exc).__name__}: {exc}"
        failures = state.get("failures")
        missing = state.get("missing_periods")
        missing_count = (
            sum(len(value) for value in missing.values() if isinstance(value, list))
            if isinstance(missing, dict)
            else len(missing) if isinstance(missing, list)
            else None
        )
        rows.append({
            "dataset": name, "source": source, "kind": "official_release_archive",
            "history_mode": "dated_original_release_archive",
            "declared_earliest": state.get("earliest_period"), "url": url,
            "description": description, "tags": ["macro", "original_release"],
            "present": path.is_file(), "rows": row_count,
            "bytes": path.stat().st_size if path.is_file() else None,
            "schema_error": schema_error,
            "state_coverage_complete": bool(state.get("complete")) and path.is_file(),
            "state_checked_through": state.get("generated_at_utc"),
            "state_missing_dates_after": missing_count,
            "state_source_unavailable_dates": 0,
            "state_failed_releases": len(failures) if isinstance(failures, list) else 0,
            "historical_completeness": "requires_receipt_and_period_audit",
            "historical_pit_values": "only_metrics_extracted_from_original_release_are_PIT",
        })
    annual_path = root / "supplemental/cbc_usdtwd_annual_pages.parquet"
    annual_state_path = root / "state/cbc_usdtwd_annual_pages.json"
    try:
        annual_state = json.loads(annual_state_path.read_text(encoding="utf-8"))
        if not isinstance(annual_state, dict):
            annual_state = {}
    except (OSError, ValueError):
        annual_state = {}
    rows.append({
        "dataset": "cbc_usdtwd_annual_pages", "source": "CBC",
        "kind": "official_annual_daily_history_page",
        "history_mode": "dated_official_annual_tables_current_values_not_release_vintages",
        "declared_earliest": None, "url": "https://www.cbc.gov.tw/tw/lp-2151-1.html",
        "description": "CBC annual daily USD/TWD closing rates; archived raw HTML",
        "tags": ["cbc", "macro", "fx", "daily", "official_page"],
        "present": annual_path.is_file(),
        "rows": int(pq.ParquetFile(annual_path).metadata.num_rows) if annual_path.is_file() else None,
        "bytes": annual_path.stat().st_size if annual_path.is_file() else None,
        "schema_error": None,
        "state_coverage_complete": annual_state.get("status") == "complete" and annual_path.is_file(),
        "state_checked_through": annual_state.get("generated_at_utc"),
        "state_missing_dates_after": None, "state_source_unavailable_dates": 0,
        "historical_completeness": "annual_page_coverage_only_not_day_one_versions",
        "historical_pit_values": "publication_clock_and_original_value_vintages_unverified",
    })
    overnight_path = root / "supplemental/cbc_overnight_official_pages.parquet"
    overnight_state_path = root / "state/cbc_overnight_official_pages.json"
    try:
        overnight_state = json.loads(overnight_state_path.read_text(encoding="utf-8"))
        if not isinstance(overnight_state, dict):
            overnight_state = {}
    except (OSError, ValueError):
        overnight_state = {}
    rows.append({
        "dataset": "cbc_overnight_official_pages", "source": "CBC",
        "kind": "official_daily_history_page",
        "history_mode": "dated_official_page_current_values_not_release_vintages",
        "declared_earliest": None, "url": "https://www.cbc.gov.tw/tw/lp-641-1.html",
        "description": "CBC dated overnight interbank rate pages; raw HTML and ambiguity receipts",
        "tags": ["cbc", "macro", "rate", "daily", "official_page"],
        "present": overnight_path.is_file(),
        "rows": int(pq.ParquetFile(overnight_path).metadata.num_rows) if overnight_path.is_file() else None,
        "bytes": overnight_path.stat().st_size if overnight_path.is_file() else None,
        "schema_error": None,
        "state_coverage_complete": bool(overnight_state.get("full_history_scanned")) and not overnight_state.get("ambiguous_dates"),
        "state_checked_through": overnight_state.get("generated_at_utc"),
        "state_missing_dates_after": len(overnight_state.get("ambiguous_dates") or []),
        "state_source_unavailable_dates": 0,
        "historical_completeness": "official_page_index_only_not_first_published_versions",
        "historical_pit_values": "publication_clock_and_original_value_vintages_unverified",
    })
    mof_index_path = root / "supplemental/mof_macro_release_dates.parquet"
    mof_state_path = root / "state/mof_macro_release_dates.json"
    try:
        mof_state = json.loads(mof_state_path.read_text(encoding="utf-8"))
        if not isinstance(mof_state, dict):
            mof_state = {}
    except (OSError, ValueError):
        mof_state = {}
    rows.append({
        "dataset": "mof_macro_release_dates", "source": "MOF",
        "kind": "official_trade_tax_release_index",
        "history_mode": "official_publication_date_not_original_value_vintage",
        "declared_earliest": None,
        "url": "https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT",
        "description": "MOF trade and tax news release dates with raw listing HTML",
        "tags": ["mof", "macro", "trade", "tax", "release_date"],
        "present": mof_index_path.is_file(),
        "rows": int(pq.ParquetFile(mof_index_path).metadata.num_rows) if mof_index_path.is_file() else None,
        "bytes": mof_index_path.stat().st_size if mof_index_path.is_file() else None,
        "schema_error": None,
        "state_coverage_complete": (
            bool(mof_state.get("full_history_scanned")) and mof_index_path.is_file()
            and isinstance(mof_state.get("tax_pdf_fallback"), dict)
            and not (mof_state["tax_pdf_fallback"].get("not_found") or [])
            and not (mof_state["tax_pdf_fallback"].get("failures") or [])
        ),
        "state_checked_through": mof_state.get("generated_at_utc"),
        "state_missing_dates_after": (
            len((mof_state.get("tax_pdf_fallback") or {}).get("not_found") or []) +
            len((mof_state.get("tax_pdf_fallback") or {}).get("failures") or [])
        ) if isinstance(mof_state.get("tax_pdf_fallback"), dict) else None,
        "state_source_unavailable_dates": 0,
        "historical_completeness": "listed_news_releases_only_not_all_bulk_periods",
        "historical_pit_values": "bulk_historical_value_vintages_unverified",
    })
    trade_pdf_path = root / "supplemental/mof_trade_release_values.parquet"
    trade_pdf_state_path = root / "state/mof_trade_release_values.json"
    try:
        trade_pdf_state = json.loads(trade_pdf_state_path.read_text(encoding="utf-8"))
        if not isinstance(trade_pdf_state, dict):
            trade_pdf_state = {}
    except (OSError, ValueError):
        trade_pdf_state = {}
    rows.append({
        "dataset": "mof_trade_release_values", "source": "MOF",
        "kind": "original_trade_press_release_rounded_values",
        "history_mode": "dated_pdf_rounded_values_only_for_bulk_missing_months",
        "declared_earliest": None,
        "url": "https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT_EXP",
        "description": "Initial trade PDF NTD values rounded to hundred-million dollars; not exact bulk precision",
        "tags": ["mof", "macro", "trade", "original_release", "rounded"],
        "present": trade_pdf_path.is_file(),
        "rows": int(pq.ParquetFile(trade_pdf_path).metadata.num_rows) if trade_pdf_path.is_file() else None,
        "bytes": trade_pdf_path.stat().st_size if trade_pdf_path.is_file() else None,
        "schema_error": None,
        "state_coverage_complete": trade_pdf_state.get("status") == "complete" and trade_pdf_path.is_file(),
        "state_checked_through": trade_pdf_state.get("generated_at_utc"),
        "state_missing_dates_after": len(trade_pdf_state.get("unresolved_periods") or []),
        "state_source_unavailable_dates": 0,
        "historical_completeness": "only_periods_missing_from_exact_bulk_csv",
        "historical_pit_values": "rounded_release_value_not_exact_thousand_twd",
    })
    original_path = root / "supplemental/mof_original_release_archive.parquet"
    original_state_path = root / "state/mof_original_release_archive.json"
    try:
        original_state = json.loads(original_state_path.read_text(encoding="utf-8"))
        if not isinstance(original_state, dict):
            original_state = {}
    except (OSError, ValueError):
        original_state = {}
    period_gaps = original_state.get("index_period_gaps") or {}
    gap_count = sum(len(value) for value in period_gaps.values() if isinstance(value, list))
    gap_count += len(original_state.get("uncaptured_releases") or [])
    rows.append({
        "dataset": "mof_original_release_archive", "source": "MOF",
        "kind": "official_original_release_archive",
        "history_mode": "indexed_press_PDF_currently_observed_not_first_vintage",
        "declared_earliest": original_state.get("earliest_period_by_series"),
        "url": "https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT",
        "description": "Dated original trade and tax release PDFs and source detail pages",
        "tags": ["mof", "macro", "trade", "tax", "original_release"],
        "present": original_path.is_file(),
        "rows": int(pq.ParquetFile(original_path).metadata.num_rows) if original_path.is_file() else None,
        "bytes": original_path.stat().st_size if original_path.is_file() else None,
        "schema_error": None,
        "state_coverage_complete": (
            original_path.is_file() and original_state.get("status") == "complete"
            and original_state.get("continuous_index_history") is True
        ),
        "state_checked_through": original_state.get("generated_at_utc"),
        "state_missing_dates_after": gap_count,
        "state_source_unavailable_dates": 0,
        "historical_completeness": "index_periods_and_raw_PDF_receipts_only",
        "historical_pit_values": "first_published_PDF_bytes_and_value_extraction_unverified",
    })
    xbrl_root = root / "mops_xbrl"
    xbrl_state_path = xbrl_root / "state.json"
    try:
        xbrl_state = json.loads(xbrl_state_path.read_text(encoding="utf-8")) if xbrl_state_path.is_file() else {}
        if not isinstance(xbrl_state, dict):
            xbrl_state = {}
    except (OSError, ValueError):
        xbrl_state = {}
    xbrl_files = sorted(xbrl_root.glob("normalized/*/*/*/facts.parquet"))
    rows.append({
        "dataset": "mops_xbrl_quarterly", "source": "MOPS",
        "kind": "quarterly_xbrl_bulk_archive", "history_mode": "official_quarterly_archive_not_filing_vintage",
        "declared_earliest": "2009Q4", "url": "https://mopsov.twse.com.tw/mops/web/t203sb02",
        "description": "GAAP / IFRS quarterly XBRL ZIP; raw versions and normalized facts",
        "tags": ["mops", "xbrl", "quarterly"], "present": bool(xbrl_files),
        "rows": xbrl_state.get("fact_rows") if xbrl_state else None,
        "bytes": sum(path.stat().st_size for path in xbrl_files), "schema_error": None,
        "state_coverage_complete": bool(xbrl_files) and bool(xbrl_state.get("discovered_periods")) and (
            xbrl_state.get("completed_periods") == xbrl_state.get("discovered_periods")
        ),
        "state_checked_through": xbrl_state.get("generated_at_utc"),
        "state_missing_dates_after": (
            len(xbrl_state.get("missing_periods", [])) if xbrl_state else None
        ),
        "state_source_unavailable_dates": 0,
        "historical_completeness": "quarterly_archive_coverage_only_not_company_or_revision_coverage",
        "historical_pit_values": "unproven_without_original_filing_timestamps",
    })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    args = parser.parse_args()
    rows = inventory(args.root)
    if args.format == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return
    print("| dataset | source | history mode | declared earliest | local rows | gaps / failed releases | source URL |")
    print("|---|---|---|---|---:|---:|---|")
    for row in rows:
        source = str(row["source"]).replace("|", "\\|")
        url = str(row["url"] or "").replace("|", "%7C")
        gaps = (
            f"{row['state_failed_releases']} releases"
            if row["kind"] == "official_release_archive"
            else str(row["state_missing_dates_after"] if row["state_missing_dates_after"] is not None else "—")
            if row["kind"] in {"quarterly_xbrl_bulk_archive", "official_original_release_archive"}
            else str(row["state_source_unavailable_dates"] or "—")
        )
        print(
            f"| {row['dataset']} | {source} | {row['history_mode']} | "
            f"{row['declared_earliest'] or '—'} | {row['rows'] if row['rows'] is not None else '—'} | "
            f"{gaps} | {url} |"
        )
    print(
        f"\nRegistered: {len(rows)} ({len(EXTENDED_DATASETS)} bulk/market plus "
        f"{len(OFFICIAL_RELEASE_ARCHIVES)} official release archives plus "
        "CBC FX / overnight, MOF release dates / trade PDFs / original PDFs and MOPS XBRL); "
        f"local files: {sum(bool(row['present']) for row in rows)}. "
        "Rows and declared start dates do not prove complete or causal history."
    )


if __name__ == "__main__":
    main()
