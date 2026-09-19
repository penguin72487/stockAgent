"""Opt-in, wide Taiwan research features with explicit estimated-vintage lineage.

The canonical TW public table remains the strict/live authority.  This module
adds obtainable historical bulk values and original-valued XBRL facts without
claiming that a current revision is the value displayed on its old release day.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.data.tw_public_provisional_macro import RAW_FEATURE_BY_SERIES


RESEARCH_CONTRACT_VERSION = 5
MARKET_SYMBOL = "__MARKET__"
_TRANSFORMED_WITHOUT_SUFFIX = {
    "twpub_cbc_fx_reserves_chg",  # despite the suffix this is a log ratio
    "twpub_margin_balance_chg", "twpub_short_balance_chg",
    "twpub_margin_buy_flow", "twpub_margin_sell_flow",
    "twpub_short_sell_flow", "twpub_short_buy_flow",
    "twpub_foreign_net_buy_flow", "twpub_investment_trust_net_buy_flow",
    "twpub_dealer_net_buy_flow", "twpub_institutional_net_buy_flow",
}
_XBRL_CONCEPTS = {
    "ifrs-full:Assets": ("twpub_xbrl_assets_twd_raw", "instant", "TWD"),
    "ifrs-full:Liabilities": ("twpub_xbrl_liabilities_twd_raw", "instant", "TWD"),
    "ifrs-full:Equity": ("twpub_xbrl_equity_twd_raw", "instant", "TWD"),
    "ifrs-full:CashAndCashEquivalents": ("twpub_xbrl_cash_twd_raw", "instant", "TWD"),
    "ifrs-full:CurrentAssets": ("twpub_xbrl_current_assets_twd_raw", "instant", "TWD"),
    "ifrs-full:CurrentLiabilities": ("twpub_xbrl_current_liabilities_twd_raw", "instant", "TWD"),
    "ifrs-full:Inventories": ("twpub_xbrl_inventories_twd_raw", "instant", "TWD"),
    "ifrs-full:Revenue": ("twpub_xbrl_revenue_twd", "duration", "TWD"),
    "ifrs-full:ProfitLoss": ("twpub_xbrl_profit_twd", "duration", "TWD"),
    "ifrs-full:GrossProfit": ("twpub_xbrl_gross_profit_twd", "duration", "TWD"),
    "ifrs-full:ProfitLossFromOperatingActivities": (
        "twpub_xbrl_operating_profit_twd", "duration", "TWD",
    ),
    "ifrs-full:BasicEarningsLossPerShare": (
        "twpub_xbrl_basic_eps", "duration", "EarningsPerShare",
    ),
    "ifrs-full:CashFlowsFromUsedInOperatingActivities": (
        "twpub_xbrl_operating_cash_flow_twd", "duration", "TWD",
    ),
    # Older TIFRS comprehensive-income statements use local concepts. Keep
    # them distinct from modern IFRS Revenue/GrossProfit so an apparently
    # similar label never silently changes a feature's accounting meaning.
    "tifrs-ci:OperatingRevenue": (
        "twpub_xbrl_tifrs_operating_revenue_twd", "duration", "TWD",
    ),
    "tifrs-ci:GrossProfitLossFromOperations": (
        "twpub_xbrl_tifrs_gross_profit_twd", "duration", "TWD",
    ),
    "tifrs-ci:GrossProfitLossFromOperationsNet": (
        "twpub_xbrl_tifrs_net_gross_profit_twd", "duration", "TWD",
    ),
    "tifrs-ci:NetOperatingIncomeLoss": (
        "twpub_xbrl_tifrs_net_operating_income_twd", "duration", "TWD",
    ),
}
_XBRL_RATIOS = {
    "twpub_xbrl_debt_ratio": (
        "twpub_xbrl_liabilities_twd_raw", "twpub_xbrl_assets_twd_raw",
    ),
    "twpub_xbrl_equity_ratio": (
        "twpub_xbrl_equity_twd_raw", "twpub_xbrl_assets_twd_raw",
    ),
    "twpub_xbrl_current_ratio": (
        "twpub_xbrl_current_assets_twd_raw", "twpub_xbrl_current_liabilities_twd_raw",
    ),
    "twpub_xbrl_inventory_to_assets_ratio": (
        "twpub_xbrl_inventories_twd_raw", "twpub_xbrl_assets_twd_raw",
    ),
    "twpub_xbrl_gross_margin": (
        "twpub_xbrl_gross_profit_twd_quarter_raw", "twpub_xbrl_revenue_twd_quarter_raw",
    ),
    "twpub_xbrl_operating_margin": (
        "twpub_xbrl_operating_profit_twd_quarter_raw", "twpub_xbrl_revenue_twd_quarter_raw",
    ),
    "twpub_xbrl_net_margin": (
        "twpub_xbrl_profit_twd_quarter_raw", "twpub_xbrl_revenue_twd_quarter_raw",
    ),
    "twpub_xbrl_tifrs_gross_margin": (
        "twpub_xbrl_tifrs_gross_profit_twd_quarter_raw",
        "twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw",
    ),
    "twpub_xbrl_tifrs_net_gross_margin": (
        "twpub_xbrl_tifrs_net_gross_profit_twd_quarter_raw",
        "twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw",
    ),
    "twpub_xbrl_tifrs_operating_margin": (
        "twpub_xbrl_tifrs_net_operating_income_twd_quarter_raw",
        "twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw",
    ),
}
_EVENT_SOURCE_GROUPS = {
    "attention": ("twse_notice_stock", "tpex_attention_stock"),
    "disposal": ("twse_disposal_stock", "tpex_disposal_stock"),
}


def research_source_columns(columns: list[str]) -> list[str]:
    """Retain untransformed source values, ratios and all execution rule facts."""

    selected = []
    for name in columns:
        if name in {"date", "symbol"} or name.startswith("_twpub_"):
            selected.append(name)
        elif name.startswith("twpub_") and not (
            "_log" in name or "_asinh" in name
            or name in _TRANSFORMED_WITHOUT_SUFFIX
        ):
            selected.append(name)
    return selected


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def research_input_receipts(
    base_path: Path, macro_events_path: Path, xbrl_root: Path,
    calendar_path: Path, minimum_year: int,
) -> dict:
    fact_root = xbrl_root / "normalized" / "ifrs"
    facts = sorted(fact_root.glob("*/*/facts.parquet"))
    if not facts:
        raise FileNotFoundError(f"no normalized IFRS XBRL facts: {fact_root}")
    paths = [
        base_path, macro_events_path, calendar_path,
        xbrl_root / "publication_candidates.parquet", *facts,
    ]
    paths.extend(
        path for group in _EVENT_SOURCE_GROUPS.values() for stem in group
        if (path := xbrl_root.parent / f"{stem}.parquet").is_file()
    )
    return {
        "contract_version": RESEARCH_CONTRACT_VERSION,
        "minimum_year": int(minimum_year),
        "files": {
            str(path.resolve()): {"bytes": path.stat().st_size, "sha256": _sha256(path)}
            for path in paths
        },
    }


def _macro_wide(events_path: Path, minimum_year: int) -> tuple[pl.DataFrame, dict]:
    events = pl.read_parquet(events_path).filter(
        pl.col("effective_session").is_not_null()
        & (pl.col("effective_session").dt.year() >= minimum_year)
    )
    raw = events.filter(
        pl.col("feature").is_in(list(RAW_FEATURE_BY_SERIES.values()))
    ).select(
        pl.col("effective_session").alias("date"),
        pl.lit(MARKET_SYMBOL).alias("symbol"),
        "feature", "subject_period",
        pl.col("source_value").cast(pl.Float64, strict=False).alias("value"),
    )
    overnight = events.filter(pl.col("feature") == "twpub_cbc_overnight_rate").select(
        pl.col("effective_session").alias("date"),
        pl.lit(MARKET_SYMBOL).alias("symbol"),
        pl.lit("twpub_cbc_overnight_pct_raw").alias("feature"),
        "subject_period",
        pl.col("source_value").cast(pl.Float64, strict=False).alias("value"),
    )
    raw = pl.concat([raw, overnight]).drop_nulls(["value"])
    # If multiple subject periods are released on the same day, the newest
    # completed period is the state visible at the next pre-open boundary.
    raw = raw.sort(["date", "feature", "subject_period"]).unique(
        ["date", "symbol", "feature"], keep="last"
    )
    wide = raw.pivot(
        index=["date", "symbol"], on="feature", values="value"
    )
    return wide, {
        "events": raw.height,
        "features": sorted(set(raw.get_column("feature").to_list())),
    }


def _xbrl_wide(
    root: Path, sessions: list[date], minimum_year: int,
) -> tuple[pl.DataFrame, dict]:
    candidates_path = root / "publication_candidates.parquet"
    fact_paths = sorted((root / "normalized" / "ifrs").glob("*/*/facts.parquet"))
    if not candidates_path.is_file() or not fact_paths:
        raise FileNotFoundError("MOPS XBRL facts and publication candidates are required")
    candidates = pl.scan_parquet(candidates_path).select(
        "archive_sha256", "document_sha256", "source_member", "company_id",
        "period", "report_period_end", "publication_date_taipei",
        "publication_time_basis",
    ).filter(
        pl.col("publication_date_taipei").is_not_null()
        & (pl.col("publication_date_taipei").str.slice(0, 4).cast(pl.Int32) >= minimum_year)
    )
    concepts = list(_XBRL_CONCEPTS)
    ifrs_local_concepts = [concept.split(":", 1)[1] for concept in concepts
                           if concept.startswith("ifrs-full:")]
    tifrs_local_concepts = [concept.split(":", 1)[1] for concept in concepts
                            if concept.startswith("tifrs-ci:")]
    facts = pl.scan_parquet(fact_paths).filter(
        (
            pl.col("concept").is_in(concepts)
            | (
                pl.col("concept").str.starts_with("{http://xbrl.iasb.org/taxonomy/")
                & pl.col("concept").str.extract(r"([^}]+)$", 1).is_in(ifrs_local_concepts)
            )
            | (
                pl.col("concept").str.starts_with("{http://www.xbrl.org/tifrs/bsci/ci/")
                & pl.col("concept").str.extract(r"([^}]+)$", 1).is_in(tifrs_local_concepts)
            )
        )
        & (pl.col("dimensions_json") == "[]")
        & (pl.col("value_parse_status") == "parsed")
        & pl.col("decimal_value").is_not_null()
    ).select(
        "archive_sha256", "document_sha256", "source_member", "entity_identifier",
        "report_period_end", "concept", "unit_ref", "period_start",
        "period_end", "period_instant", "decimal_value",
    ).with_columns(
        pl.when(pl.col("concept").str.starts_with("{http://xbrl.iasb.org/taxonomy/"))
        .then(pl.concat_str([
            pl.lit("ifrs-full:"),
            pl.col("concept").str.extract(r"([^}]+)$", 1),
        ]))
        .when(pl.col("concept").str.starts_with("{http://www.xbrl.org/tifrs/bsci/ci/"))
        .then(pl.concat_str([
            pl.lit("tifrs-ci:"),
            pl.col("concept").str.extract(r"([^}]+)$", 1),
        ]))
        .otherwise(pl.col("concept"))
        .alias("concept")
    )
    joined = facts.join(
        candidates,
        on=["archive_sha256", "document_sha256", "source_member", "report_period_end"],
        how="inner",
    ).filter(
        pl.col("entity_identifier") == pl.col("company_id")
    ).with_columns(
        pl.col("decimal_value").cast(pl.Float64, strict=False).alias("value"),
        pl.col("publication_date_taipei").str.to_date(strict=False).alias("published_on"),
        pl.col("report_period_end").str.to_date(strict=False).alias("period_end_date"),
    ).filter(
        pl.col("value").is_finite() & pl.col("published_on").is_not_null()
    )
    # Collect only selected, un-dimensioned concepts; never materialize the
    # full ~2 GB XBRL fact corpus or comparative prior-period facts.
    rows = joined.collect(engine="streaming")
    if rows.is_empty():
        raise ValueError("no eligible dimensionless XBRL facts were found")
    period_quarter = ((pl.col("period_end_date").dt.month() - 1) // 3) + 1
    quarter_start = pl.date(
        pl.col("period_end_date").dt.year(), (period_quarter - 1) * 3 + 1, 1
    )
    year_start = pl.date(pl.col("period_end_date").dt.year(), 1, 1)
    rows = rows.with_columns(
        pl.col("period_start").cast(pl.Utf8).str.to_date(strict=False).alias("start_date"),
        pl.col("period_end").cast(pl.Utf8).str.to_date(strict=False).alias("duration_end"),
        pl.col("period_instant").cast(pl.Utf8).str.to_date(strict=False).alias("instant_date"),
    ).with_columns(
        quarter_start.alias("quarter_start"), year_start.alias("year_start")
    )
    frames = []
    for concept, (name, grain, unit) in _XBRL_CONCEPTS.items():
        allowed_units = (
            [unit, "TWD"] if concept == "ifrs-full:BasicEarningsLossPerShare" else [unit]
        )
        subset = rows.filter(
            (pl.col("concept") == concept)
            & pl.col("unit_ref").is_in(allowed_units)
            & (pl.col("period_end_date") == pl.col("instant_date") if grain == "instant" else
               pl.col("period_end_date") == pl.col("duration_end"))
        )
        if grain == "duration":
            # Q1's start is both quarter-start and year-start. Emit both
            # feature grains; otherwise carrying prior Q4 YTD through Q1
            # would be a silent, financially incorrect stale value.
            for start_col, suffix in (
                ("quarter_start", "quarter_raw"), ("year_start", "ytd_raw"),
            ):
                frames.append(subset.filter(
                    pl.col("start_date") == pl.col(start_col)
                ).select(
                    pl.col("published_on"), pl.col("company_id").alias("symbol"),
                    "period", pl.lit(f"{name}_{suffix}").alias("feature"),
                    "value", "publication_time_basis",
                ))
        else:
            frames.append(subset.select(
                pl.col("published_on"), pl.col("company_id").alias("symbol"),
                "period", pl.lit(name).alias("feature"), "value",
                "publication_time_basis",
            ))
    events = pl.concat(frames).drop_nulls(["value"])
    if events.is_empty():
        raise ValueError("no XBRL facts match the declared current report period")
    # Candidate clocks are date-only/estimated. They are treated as post-open;
    # an exact filing clock can later supersede this mapping without changing
    # historical numeric values or the canonical strict table.
    unique_dates = events.get_column("published_on").unique().to_list()
    effective = {
        day: sessions[bisect_right(sessions, day)]
        if bisect_right(sessions, day) < len(sessions) else None
        for day in unique_dates
    }
    events = events.with_columns(
        pl.col("published_on").replace(effective).cast(pl.Date).alias("date")
    ).drop_nulls(["date"])
    # Contradictory documents at an indistinguishable estimated instant cannot
    # be ordered; omit that key instead of selecting a lucky file order.
    grouped = events.group_by(["date", "symbol", "period", "feature"]).agg(
        pl.col("value").n_unique().alias("distinct_values"),
        pl.col("value").first().alias("value"),
    )
    conflicting = grouped.filter(pl.col("distinct_values") > 1).height
    grouped = grouped.filter(pl.col("distinct_values") == 1).drop("distinct_values")
    wide = grouped.pivot(index=["date", "symbol", "period"], on="feature", values="value")
    ratio_exprs = []
    for name, (numerator, denominator) in _XBRL_RATIOS.items():
        if numerator not in wide.columns or denominator not in wide.columns:
            continue
        ratio_exprs.append(
            pl.when(pl.col(denominator).is_not_null() & (pl.col(denominator) != 0))
            .then(pl.col(numerator) / pl.col(denominator))
            .otherwise(None)
            .alias(name)
        )
    if ratio_exprs:
        wide = wide.with_columns(ratio_exprs)
    # Same-day multiple quarterly submissions are not merged into one fictive
    # statement. The latest reported period wins this date's update; earlier
    # state remains represented by its prior release and panel carry-forward.
    wide = wide.sort("period").unique(["date", "symbol"], keep="last").drop("period")
    return wide, {
        "fact_paths": len(fact_paths), "candidate_rows": rows.height,
        "events": grouped.height, "conflicting_same_clock_keys_omitted": conflicting,
        "features": sorted([*set(grouped.get_column("feature").to_list()), *(
            expr.meta.output_name() for expr in ratio_exprs
        )]),
        "publication_basis": "estimated candidate date, next official exchange session",
    }


def _event_source_coverage(
    input_dir: Path, sessions: list[date],
) -> tuple[pl.DataFrame, dict]:
    taipei = ZoneInfo("Asia/Taipei")
    coverage: dict[str, set[date]] = {}
    source_counts: dict[str, int] = {}
    for group, stems in _EVENT_SOURCE_GROUPS.items():
        by_source: list[set[date]] = []
        for stem in stems:
            path = input_dir / f"{stem}.parquet"
            if not path.is_file():
                source_counts[stem] = 0
                by_source.append(set())
                continue
            frame = pl.read_parquet(path, columns=["_downloaded_at_utc"])
            observed: set[date] = set()
            for value in frame.get_column("_downloaded_at_utc").drop_nulls().unique().to_list():
                try:
                    clock = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                    clock = clock.replace(tzinfo=timezone.utc) if clock.tzinfo is None else clock
                    local = clock.astimezone(taipei)
                except ValueError:
                    continue
                lookup = bisect_left if local.hour < 9 else bisect_right
                index = lookup(sessions, local.date())
                if index < len(sessions):
                    observed.add(sessions[index])
            source_counts[stem] = len(observed)
            by_source.append(observed)
        coverage[group] = set.intersection(*by_source) if by_source else set()
    rows = [
        {"date": day, "symbol": MARKET_SYMBOL,
         **{f"twpub_{group}_source_covered": 1.0 if day in days else None
            for group, days in coverage.items()}}
        for day in sorted(set.union(*coverage.values()))
    ] if any(coverage.values()) else []
    if rows:
        return pl.DataFrame(rows), {"source_sessions": source_counts}
    return pl.DataFrame(schema={
        "date": pl.Date, "symbol": pl.String,
        "twpub_attention_source_covered": pl.Float64,
        "twpub_disposal_source_covered": pl.Float64,
    }), {"source_sessions": source_counts}


def build_tw_public_research_features(
    *, base_path: Path, macro_events_path: Path, xbrl_root: Path,
    calendar_path: Path, output_path: Path, minimum_year: int = 2013,
) -> dict:
    base_path = Path(base_path)
    macro_events_path = Path(macro_events_path)
    xbrl_root = Path(xbrl_root)
    output_path = Path(output_path)
    if output_path.resolve() == base_path.resolve():
        raise ValueError("research output must not overwrite the canonical PIT feature table")
    inputs = research_input_receipts(
        base_path, macro_events_path, xbrl_root, calendar_path, minimum_year
    )
    receipt = output_path.with_suffix(".research.json")
    if output_path.is_file() and receipt.is_file():
        try:
            prior = json.loads(receipt.read_text())
        except (OSError, ValueError):
            prior = {}
        if prior.get("inputs") == inputs and prior.get("output_sha256") == _sha256(output_path):
            return {**prior, "reused": True}
    sessions = sorted(set(pl.read_parquet(calendar_path, columns=["date"]).get_column("date").to_list()))
    if not sessions:
        raise ValueError("official TAIEX session calendar is empty")
    macro, macro_summary = _macro_wide(macro_events_path, minimum_year)
    xbrl, xbrl_summary = _xbrl_wide(xbrl_root, sessions, minimum_year)
    coverage, coverage_summary = _event_source_coverage(xbrl_root.parent, sessions)
    macro_summary["source_sha256"] = inputs["files"][str(macro_events_path.resolve())]["sha256"]
    xbrl_summary["candidates_sha256"] = inputs["files"][
        str((xbrl_root / "publication_candidates.parquet").resolve())
    ]["sha256"]
    columns = research_source_columns(pl.scan_parquet(base_path).collect_schema().names())
    base = pl.scan_parquet(base_path).select(columns).with_columns(
        (pl.col("_twpub_attention_flag") if "_twpub_attention_flag" in columns else
         pl.lit(None, dtype=pl.Float64)).alias("twpub_attention_event_flag"),
        (pl.col("_twpub_disposal_flag") if "_twpub_disposal_flag" in columns else
         pl.lit(None, dtype=pl.Float64)).alias("twpub_disposal_event_flag"),
    )
    columns.extend(["twpub_attention_event_flag", "twpub_disposal_event_flag"])
    macro_cols = [column for column in macro.columns if column not in {"date", "symbol"}]
    xbrl_cols = [column for column in xbrl.columns if column not in {"date", "symbol"}]
    # A canonical original release beats a current revised bulk value at the
    # same key. Every other bulk value remains explicitly research-only.
    joined = base.join(macro.lazy(), on=["date", "symbol"], how="left", suffix="__bulk")
    for name in macro_cols:
        if name in columns:
            joined = joined.with_columns(
                pl.coalesce(pl.col(name), pl.col(f"{name}__bulk")).alias(name)
            ).drop(f"{name}__bulk")
    joined = joined.join(xbrl.lazy(), on=["date", "symbol"], how="left")
    joined = joined.join(coverage.lazy(), on=["date", "symbol"], how="left")
    # Market rows exist for every official session. Keep any issuer filing on
    # a no-quote day as a sparse row so the next panel session can carry it.
    base_keys = base.select("date", "symbol")
    missing_xbrl = xbrl.lazy().join(base_keys, on=["date", "symbol"], how="anti")
    new_columns = [name for name in macro_cols if name not in columns]
    coverage_cols = [name for name in coverage.columns if name not in {"date", "symbol"}]
    output_cols = [*columns, *new_columns, *xbrl_cols, *coverage_cols]
    missing_xbrl = missing_xbrl.select(
        [pl.col(name) if name in {"date", "symbol", *xbrl_cols}
         else pl.lit(None).alias(name) for name in output_cols]
    )
    combined = pl.concat([joined.select(output_cols), missing_xbrl], how="vertical_relaxed")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    combined.sink_parquet(temporary, compression="zstd", statistics=True)
    os.replace(temporary, output_path)
    summary = {
        "contract_version": RESEARCH_CONTRACT_VERSION,
        "output_path": str(output_path),
        "output_sha256": _sha256(output_path),
        "base_sha256": inputs["files"][str(base_path.resolve())]["sha256"],
        "inputs": inputs,
        "reused": False,
        "columns": output_cols,
        "macro": macro_summary,
        "xbrl": xbrl_summary,
        "sparse_events": coverage_summary,
        "value_vintage_policy": "original when available; otherwise current revision, research only",
        "publication_policy": "original timestamp/date first; otherwise labelled historical schedule estimate",
    }
    temp_receipt = receipt.with_suffix(receipt.suffix + ".tmp")
    temp_receipt.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    os.replace(temp_receipt, receipt)
    return summary
