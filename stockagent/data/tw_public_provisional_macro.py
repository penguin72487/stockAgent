"""Research-only Taiwan macro vintages from currently available bulk values.

The bulk tables are useful for recovering *values*, but a row describing 2005
downloaded in 2026 is not a 2005 value vintage.  This module deliberately keeps
value provenance separate from publication-clock evidence.  Its output must
never replace the strict PIT feature table without original-value validation.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime, time
import hashlib
import json
import math
import os
from pathlib import Path
import re

import polars as pl


TRANSFORMED_FEATURES = (
    "twpub_usdtwd_log", "twpub_usdtwd_logret_1d",
    "twpub_cbc_overnight_rate", "twpub_cbc_overnight_rate_chg",
    "twpub_cbc_m1b_log", "twpub_cbc_m2_log",
    "twpub_dgbas_cpi_log", "twpub_dgbas_gdp_log",
    "twpub_mof_export_log", "twpub_mof_import_log",
    "twpub_mof_trade_balance_asinh", "twpub_mof_tax_total_log",
    "twpub_mof_securities_tax_log", "twpub_mof_futures_tax_log",
    "twpub_mof_business_tax_log",
)
RAW_FEATURE_BY_SERIES = {
    "fx": "twpub_usdtwd_raw",
    "m1b": "twpub_cbc_m1b_raw",
    "m2": "twpub_cbc_m2_raw",
    "cpi": "twpub_dgbas_cpi_raw",
    "gdp": "twpub_dgbas_gdp_raw",
    "exports": "twpub_mof_export_raw",
    "imports": "twpub_mof_import_raw",
    "balance": "twpub_mof_trade_balance_raw",
    "tax_total": "twpub_mof_tax_total_raw",
    "securities_tax": "twpub_mof_securities_tax_raw",
    "futures_tax": "twpub_mof_futures_tax_raw",
    "business_tax": "twpub_mof_business_tax_raw",
}
FEATURES = (*TRANSFORMED_FEATURES, *RAW_FEATURE_BY_SERIES.values())

_BULK_FILES = (
    "cbc_usdtwd_closing_rate", "cbc_overnight_rate", "cbc_money_aggregates",
    "dgbas_cpi_basic", "dgbas_gdp_expenditure_sa", "mof_customs_trade",
    "mof_tax_revenue",
)
_MONEY_COLUMNS = {
    "m1b": ("貨幣總計數-Ｍ１Ｂ-原始值", "貨幣總計數 -Ｍ１Ｂ-原始值"),
    "m2": ("貨幣總計數-Ｍ２-原始值", "貨幣總計數 -Ｍ２-原始值"),
}


def _number(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(str(value).strip().replace(",", ""))
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _first_number(row: dict, columns: tuple[str, ...]) -> float | None:
    for column in columns:
        value = _number(row.get(column))
        if value is not None:
            return value
    return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def _month_period(text: object) -> str | None:
    match = re.fullmatch(r"(\d{4})M(\d{1,2})", str(text or ""))
    if not match:
        return None
    year, month = map(int, match.groups())
    return f"{year:04d}-{month:02d}" if 1 <= month <= 12 else None


def _quarter_period(text: object) -> str | None:
    match = re.fullmatch(r"(\d{4})Q([1-4])", str(text or ""))
    return f"{match.group(1)}Q{match.group(2)}" if match else None


def _daily_period(text: object) -> str | None:
    value = str(text or "").strip()
    for pattern in ("%Y%m%d", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            pass
    return None


def _archive_dates(root: Path) -> dict[tuple[str, str], tuple[date, str | None, str, str | None]]:
    """First dated original for a period; it proves a date, not bulk value."""
    result: dict[tuple[str, str], tuple[date, str | None, str, str | None]] = {}
    for dataset in ("cbc_money_release_vintages", "dgbas_release_vintages",
                    "supplemental/mof_macro_release_dates"):
        path = root / f"{dataset}.parquet"
        if not path.is_file():
            continue
        for row in pl.read_parquet(path).to_dicts():
            period = str(row.get("period") or "")
            source = ("money" if dataset.startswith("cbc_") else
                      str(row.get("series") or "") if dataset.startswith("supplemental/") else
                      str(row.get("source") or ""))
            if source == "gdp":
                period = period.replace("-Q", "Q")
            try:
                published = date.fromisoformat(str(row.get("published_on")))
            except ValueError:
                continue
            if source not in {"money", "cpi", "gdp", "trade", "tax"} or not period:
                continue
            key = (source, period)
            clock = (
                str(row.get("published_clock_taipei"))
                if row.get("published_time_precision") == "official_document_time"
                and row.get("published_clock_taipei")
                else None
            )
            candidate = (published, clock, str(row.get("release_url") or ""), None)
            if key not in result or published < result[key][0] or (
                published == result[key][0] and clock and not result[key][1]
            ):
                result[key] = candidate
    state_path = root / "state/mof_macro_release_dates.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        state = {}
    fallback = state.get("tax_pdf_fallback") if isinstance(state, dict) else None
    schedules = fallback.get("scheduled_inferences") if isinstance(fallback, dict) else None
    for row in schedules if isinstance(schedules, list) else []:
        key = ("tax", str(row.get("period") or ""))
        if key in result:
            continue  # an actual release date always outranks a prior forecast
        try:
            scheduled = date.fromisoformat(str(row["scheduled_on"]))
            clock = time.fromisoformat(str(row["scheduled_clock_taipei"]))
        except (KeyError, ValueError):
            continue
        result[key] = (scheduled, clock.isoformat(), str(row.get("evidence_url") or ""),
                       "estimated_official_advance_schedule")
    return result


def _publication(
    series: str, period: str,
    archives: dict[tuple[str, str], tuple[date, str | None, str, str | None]],
) -> tuple[datetime, str, str]:
    """Resolve the best known clock before falling back to a labelled guess."""
    if series in {"fx", "overnight"}:
        day = date.fromisoformat(period)
        if series == "fx":
            # CBC says each working day's close is provided 16:00-17:00.
            return datetime.combine(day, time(17)), "official_publication_window", (
                "https://www.cbc.gov.tw/tw/lp-645-1-3-20.html"
            )
        # No individual daily release clock has been verified for this table.
        return datetime.combine(day, time(18)), "estimated_daily_after_close", (
            "https://www.cbc.gov.tw/tw/lp-641-1.html"
        )
    archive_key = (series, period)
    if archive_key in archives:
        day, clock, url, basis = archives[archive_key]
        if basis == "estimated_official_advance_schedule":
            return datetime.combine(day, time.fromisoformat(clock or "16:00:00")), basis, url
        if clock:
            try:
                return datetime.combine(day, time.fromisoformat(clock)), "official_document_clock", url
            except ValueError:
                pass
        # A dated article is exact to the day; the intra-day clock is inferred.
        inferred = time(8, 30) if series == "cpi" and date(2012, 5, 7) <= day < date(2017, 7, 1) else time(16, 20) if series == "money" else time(16)
        return datetime.combine(day, inferred), "official_release_date_inferred_clock", url
    if series == "gdp":
        year, quarter = int(period[:4]), int(period[-1])
        year, month = _next_month(year, quarter * 3)
        day = date(year, month, 28)
    else:
        year, month = map(int, period.split("-"))
        year, month = _next_month(year, month)
        day = date(year, month, {"money": 24, "cpi": 6, "trade": 8, "tax": 12}[series])
    return datetime.combine(day, time(16, 20) if series == "money" else time(16)), (
        "estimated_historical_pattern"
    ), ""


def _sessions(root: Path) -> list[date]:
    path = root / "twse_taiex_ohlc.parquet"
    if not path.is_file():
        raise FileNotFoundError("official TAIEX session calendar is required")
    rows = pl.read_parquet(path, columns=["date"]).get_column("date").to_list()
    result = sorted({date.fromisoformat(str(value)[:10]) for value in rows if value is not None})
    if not result:
        raise ValueError("official TAIEX session calendar is empty")
    return result


def _effective_session(clock: datetime, sessions: list[date]) -> date | None:
    index = (bisect_left if clock.time() < time(9) else bisect_right)(sessions, clock.date())
    return sessions[index] if index < len(sessions) else None


def build_provisional_macro_events(root: Path) -> tuple[pl.DataFrame, dict]:
    """Build one event per obtainable feature and subject period, without PIT promotion."""
    root = Path(root)
    archives = _archive_dates(root)
    sessions = _sessions(root)
    base: dict[tuple[str, str], dict] = {}
    ambiguous: dict[tuple[str, str], set[float]] = {}
    receipts = {}
    for name in ("cbc_money_release_vintages", "dgbas_release_vintages",
                 "supplemental/mof_macro_release_dates"):
        path = root / f"{name}.parquet"
        if path.is_file():
            receipts[name] = {"size": path.stat().st_size, "sha256": _sha256(path)}
    mof_state_path = root / "state/mof_macro_release_dates.json"
    if mof_state_path.is_file():
        receipts["state/mof_macro_release_dates"] = {
            "size": mof_state_path.stat().st_size, "sha256": _sha256(mof_state_path)
        }
    for name in _BULK_FILES:
        path = root / f"{name}.parquet"
        if not path.is_file():
            raise FileNotFoundError(path)
        receipts[name] = {"size": path.stat().st_size, "sha256": _sha256(path)}
        for row in pl.read_parquet(path).to_dicts():
            observed = str(row.get("_downloaded_at_utc") or "")
            values: list[tuple[str, str, float | None]] = []
            if name == "cbc_usdtwd_closing_rate":
                period = _daily_period(row.get("日期"))
                values = [("fx", period, _number(row.get("NTD/USD")))] if period else []
            elif name == "cbc_overnight_rate":
                period = _daily_period(row.get("日期"))
                value = _number(row.get("利率[%]"))
                values = [("overnight", period, value / 100 if value is not None else None)] if period else []
            elif name == "cbc_money_aggregates":
                period = _month_period(row.get("期間"))
                values = [(series, period, _first_number(row, columns)) for series, columns in _MONEY_COLUMNS.items()] if period else []
            elif name == "dgbas_cpi_basic":
                if str(row.get("Item") or "").startswith("總指數(") and row.get("TYPE") == "原始值":
                    period = _month_period(row.get("TIME_PERIOD"))
                    values = [("cpi", period, _number(row.get("Item_VALUE")))] if period else []
            elif name == "dgbas_gdp_expenditure_sa":
                if str(row.get("Item") or "").startswith("金額(新臺幣百萬元)、當期價格_1.國內生產毛額") and row.get("TYPE") == "原始值":
                    period = _quarter_period(row.get("TIME_PERIOD"))
                    values = [("gdp", period, _number(row.get("Item_VALUE")))] if period else []
            elif name == "mof_customs_trade":
                year, month = _number(row.get("年度")), _number(row.get("月份"))
                if year is not None and month is not None and 1 <= month <= 12:
                    period = f"{int(year) + 1911:04d}-{int(month):02d}"
                    values = [(series, period, _number(row.get(column))) for series, column in (
                        ("exports", "出口總值(新臺幣千元)"),
                        ("imports", "進口總值(新臺幣千元)"),
                        ("balance", "出入超(新臺幣千元)"),
                    )]
            elif name == "mof_tax_revenue":
                match = re.fullmatch(r"(\d{2,3})年\s*(\d{1,2})月", str(row.get("稅目別") or "").strip())
                if match and 1 <= int(match.group(2)) <= 12:
                    period = f"{int(match.group(1)) + 1911:04d}-{int(match.group(2)):02d}"
                    values = [(series, period, _number(row.get(column))) for series, column in (
                        ("tax_total", "總計"), ("securities_tax", "證券交易稅"),
                        ("futures_tax", "期貨交易稅"), ("business_tax", "營業稅"),
                    )]
            for series, period, value in values:
                if period is None or value is None:
                    continue
                key = (series, period)
                candidate = {"value": value, "observed": observed, "dataset": name,
                             "url": str(row.get("_url") or ""), "sha256": receipts[name]["sha256"],
                             "vintage_basis": "current_bulk_unverified_historical_revision"}
                prior = base.get(key)
                if prior is None or observed > prior["observed"]:
                    base[key] = candidate
                    ambiguous.pop(key, None)
                elif observed == prior["observed"] and not math.isclose(prior["value"], value, rel_tol=1e-10, abs_tol=1e-10):
                    ambiguous.setdefault(key, {prior["value"]}).add(value)

    annual_discrepancies: list[dict] = []
    annual_path = root / "supplemental/cbc_usdtwd_annual_pages.parquet"
    if annual_path.is_file():
        annual_sha = _sha256(annual_path)
        receipts["cbc_usdtwd_annual_pages"] = {
            "size": annual_path.stat().st_size, "sha256": annual_sha,
        }
        for row in pl.read_parquet(annual_path).to_dicts():
            period = _daily_period(row.get("subject_date"))
            rate = _number(row.get("ntd_per_usd"))
            if period is None or rate is None:
                continue
            key = ("fx", period)
            previous = base.get(key)
            if previous is not None:
                if not math.isclose(previous["value"], rate, rel_tol=1e-10, abs_tol=1e-10):
                    annual_discrepancies.append({"subject_period": period,
                                                 "bulk_value": previous["value"],
                                                 "annual_page_value": rate})
                continue
            base[key] = {"value": rate, "observed": str(row.get("observed_at_utc") or ""),
                         "dataset": "cbc_usdtwd_annual_pages",
                         "url": str(row.get("page_url") or ""), "sha256": annual_sha,
                         "vintage_basis": "current_official_annual_table_unverified_historical_revision"}

    # The independently archived official daily page can repair a stale bulk
    # tail or disambiguate a duplicate bulk date. It remains a *current page*
    # read, not proof of what the page contained years ago.
    daily_pages = root / "supplemental" / "cbc_overnight_official_pages.parquet"
    if daily_pages.is_file():
        daily_sha = _sha256(daily_pages)
        receipts["cbc_overnight_official_pages"] = {
            "size": daily_pages.stat().st_size, "sha256": daily_sha,
        }
        for row in pl.read_parquet(daily_pages).to_dicts():
            period = _daily_period(row.get("subject_date"))
            rate = _number(row.get("rate_pct"))
            if period is None or rate is None or row.get("status") != "ok":
                continue
            key = ("overnight", period)
            base[key] = {"value": rate / 100, "observed": str(row.get("observed_at_utc") or ""),
                         "dataset": "cbc_overnight_official_pages",
                         "url": str(row.get("page_url") or ""), "sha256": daily_sha,
                         "vintage_basis": "current_official_page_unverified_historical_revision"}
            ambiguous.pop(key, None)

    # Press PDFs can cover months that the precise customs CSV has not yet
    # published. Keep their rounded NTD values separate and never overwrite a
    # precise bulk month with a whole-hundred-million release figure.
    trade_pdf_path = root / "supplemental/mof_trade_release_values.parquet"
    if trade_pdf_path.is_file():
        trade_pdf_sha = _sha256(trade_pdf_path)
        receipts["mof_trade_release_values"] = {
            "size": trade_pdf_path.stat().st_size, "sha256": trade_pdf_sha,
        }
        for row in pl.read_parquet(trade_pdf_path).to_dicts():
            period = _month_period(str(row.get("period") or "").replace("-", "M"))
            if period is None:
                continue
            for series in ("exports", "imports", "balance"):
                value = _number(row.get(series))
                key = (series, period)
                if value is None or key in base:
                    continue
                base[key] = {
                    "value": value,
                    "observed": str(row.get("observed_at_utc") or ""),
                    "dataset": "mof_trade_release_values",
                    "url": str(row.get("pdf_url") or ""),
                    "sha256": trade_pdf_sha,
                    "vintage_basis": "current_dated_release_pdf_rounded_100m_twd_unverified_revision",
                    "precision": "nearest_100000_twd_thousand",
                }

    # A bulk file may contain two different observations under the same date.
    # Without an official row-level disambiguator, neither is safe to choose.
    for key in ambiguous:
        base.pop(key)

    events: list[dict] = []
    previous: dict[str, float] = {}
    previous_period: dict[str, str] = {}
    mapping = {
        "fx": ("twpub_usdtwd_log",),
        "overnight": ("twpub_cbc_overnight_rate",),
        "m1b": ("twpub_cbc_m1b_log",), "m2": ("twpub_cbc_m2_log",),
        "cpi": ("twpub_dgbas_cpi_log",), "gdp": ("twpub_dgbas_gdp_log",),
        "exports": ("twpub_mof_export_log",), "imports": ("twpub_mof_import_log",),
        "balance": ("twpub_mof_trade_balance_asinh",),
        "tax_total": ("twpub_mof_tax_total_log",),
        "securities_tax": ("twpub_mof_securities_tax_log",),
        "futures_tax": ("twpub_mof_futures_tax_log",),
        "business_tax": ("twpub_mof_business_tax_log",),
    }
    for (series, period), source in sorted(base.items()):
        raw = source["value"]
        if series in {"fx", "m1b", "m2", "cpi", "gdp"}:
            transformed = math.log(raw) if raw > 0 else None
        elif series in {"exports", "imports", "tax_total", "securities_tax", "futures_tax", "business_tax"}:
            transformed = math.log1p(raw) if raw >= 0 else None
        elif series == "balance":
            transformed = math.asinh(raw / 1_000_000)
        else:
            transformed = raw
        feature_values = [(mapping[series][0], transformed)]
        if series in RAW_FEATURE_BY_SERIES:
            feature_values.append((RAW_FEATURE_BY_SERIES[series], raw))
        crosses_ambiguous = any(
            bad_series == series and previous_period.get(series, "") < bad_period < period
            for bad_series, bad_period in ambiguous
        )
        if series == "fx" and previous.get(series, 0) > 0 and raw > 0 and not crosses_ambiguous:
            feature_values.append(("twpub_usdtwd_logret_1d", math.log(raw / previous[series])))
        elif series == "overnight" and series in previous and not crosses_ambiguous:
            feature_values.append(("twpub_cbc_overnight_rate_chg", raw - previous[series]))
        previous[series] = raw
        previous_period[series] = period
        release_series = "money" if series in {"m1b", "m2"} else "trade" if series in {"exports", "imports", "balance"} else "tax" if series.endswith("tax") or series == "tax_total" else series
        published, time_basis, release_url = _publication(release_series, period, archives)
        effective = _effective_session(published, sessions)
        for feature, value in feature_values:
            if value is not None and not math.isfinite(value):
                continue
            events.append({
                "feature": feature, "subject_period": period,
                "source_value": raw, "feature_value": value,
                "transform_status": (
                    "raw" if feature in RAW_FEATURE_BY_SERIES.values()
                    else "ok" if value is not None
                    else "outside_existing_log_domain"
                ),
                "source_dataset": source["dataset"], "source_url": source["url"],
                "source_parquet_sha256": source["sha256"],
                "source_observed_at_utc": source["observed"],
                "published_at_taipei": published.isoformat(timespec="seconds"),
                "publication_time_basis": time_basis,
                "publication_evidence_url": release_url,
                "effective_session": effective,
                "value_vintage_basis": source["vintage_basis"],
                "source_value_precision": source.get("precision"),
                "strict_pit_eligible": False,
            })
    if not events:
        raise ValueError("no provisional macro values were extracted")
    # The rounded-PDF precision appears only near the tail; sampling the first
    # 100 older rows would incorrectly infer a Null dtype for this column.
    frame = pl.DataFrame(events, infer_schema_length=None).sort(["feature", "subject_period"])
    duplicates = frame.group_by(["feature", "subject_period"]).len().filter(pl.col("len") != 1)
    if not duplicates.is_empty():
        raise ValueError("provisional macro feature-period keys are not unique")
    coverage = {}
    for feature in FEATURES:
        subset = frame.filter(pl.col("feature") == feature)
        coverage[feature] = {
            "rows": subset.height,
            "transformable_rows": subset.filter(pl.col("feature_value").is_not_null()).height,
            "raw_value_only_rows": subset.filter(pl.col("feature_value").is_null()).height,
            "first_subject_period": subset.get_column("subject_period").min() if subset.height else None,
            "last_subject_period": subset.get_column("subject_period").max() if subset.height else None,
            "official_date_or_clock_rows": subset.filter(
                pl.col("publication_time_basis").str.starts_with("official_release_date")
                | (pl.col("publication_time_basis") == "official_document_clock")
            ).height,
            "estimated_publication_date_rows": subset.filter(
                pl.col("publication_time_basis").str.starts_with("estimated")
            ).height,
            "unmapped_session_rows": subset.filter(pl.col("effective_session").is_null()).height,
        }
    return frame, {"schema_version": 2, "contract": "provisional_bulk_values_not_strict_pit",
                   "source_receipts": receipts, "feature_coverage": coverage,
                   "total_rows": frame.height, "strict_pit_eligible_rows": 0,
                   "annual_fx_discrepancies": annual_discrepancies,
                   "ambiguous_bulk_keys": [
                       {"series": series, "subject_period": period, "candidate_values": sorted(values)}
                       for (series, period), values in sorted(ambiguous.items())
                   ]}


def write_provisional_macro_events(root: Path, output: Path) -> dict:
    frame, summary = build_provisional_macro_events(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    frame.write_parquet(temporary, compression="zstd")
    os.replace(temporary, output)
    summary["output_sha256"] = _sha256(output)
    summary_path = output.with_suffix(".summary.json")
    temporary_summary = summary_path.with_suffix(summary_path.suffix + ".tmp")
    temporary_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary_summary, summary_path)
    return summary
