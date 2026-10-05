"""Explicit opt-in research clocks, NOT reconstructed publication evidence.

Only timing is estimated. Existing observations/vintages are never rewritten.
The extra safety delay is exactly one calendar day. A reporting period is not
a publication date; statutory/provider release proxies remain explicit, and
known uploads replace (rather than wait for) a guessed deadline. Closed-market
days are recorded separately from the safety delay.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
import re

import polars as pl

CONTRACT = "tw_preopen_release_schedule_research_v9_native_quarter_families"
LAW = "https://twse-regulation.twse.com.tw/tw/law/DOC01_print.aspx?FLCODE=fl007009&FLNO=36"
SPECIAL = "https://twse-regulation.twse.com.tw/TW/law/DAT08_print.aspx?FLCODE=FL067432"
HOLIDAY = "https://investoredu.twse.com.tw/FileSystem/FileUpload/f506f947-0f96-48d2-8248-b28f5a431c7a.pdf"
FINLAB = "https://finlab.finance/blog/taiwan-financial-statement-date-index"
PMI = "https://www.ndc.gov.tw/nc_337_2268"
NDC = "https://data.gov.tw/dataset/27543"
TDCC = "https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock"
NMI_FIRST_RELEASE = date(2015, 2, 2)
NMI_LAUNCH_SOURCE = "https://www.ndc.gov.tw/nc_333_2125"
TDCC_CADENCE_SOURCE = "https://data.gov.tw/api/front/file/download?uuid=50c4fb19-c6d0-4d88-8304-dfdf7338b732"


@dataclass(frozen=True)
class ReleaseRule:
    name: str
    kind: str
    carry_days: int
    scope: str
    rationale: str
    urls: tuple[str, ...]
    cadence_change_on: str = ""
    older_carry_days: int = 0


RULES = {
    "quarter_date": ReleaseRule("quarter_provider_deadline_date_plus1d", "quarter_date", 200, "stock",
        "Provider revised statement index is its stated deadline date, not a fiscal period. "
        "Roll a non-session deadline then +1 calendar day; current revised values are NOT historical vintages",
        (LAW, FINLAB)),
    "quarter": ReleaseRule("quarter_upload_or_general_deadline_plus1d", "quarter", 200, "stock",
        "Known issuer upload takes precedence. Otherwise general-company deadline "
        "Q1 May15/Q2 Aug14/Q3 Nov14/Q4 Mar31 proxy, holiday extended; special issuer "
        "regimes remain unverified research estimates, not proven publication dates",
        (LAW, SPECIAL, HOLIDAY, FINLAB)),
    "revenue": ReleaseRule("monthly_revenue_release_plus1d", "revenue", 62, "stock",
        "Period month -> next month10; provider date already denotes release month. "
        "Use provider release-date index as supplied; no blanket insurance/day15 padding; "
        "actual known publication overrides estimates; special regimes unverified",
        (LAW, SPECIAL, FINLAB)),
    "daily": ReleaseRule("completed_daily_next_session", "daily", 0, "stock",
        "Completed daily provider observation after close -> next session; no daily gap filling",
        ("https://finlab.finance/docs/details/get_data/",)),
    "weekly": ReleaseRule("tdcc_weekly_estimated_release_plus1d", "weekly", 14, "stock",
        "TDCC monthly before May 2015, weekly thereafter. Provider as-of date "
        "is an estimated release date, +1 calendar day; bounded state carry, "
        "NOT verified historical publication evidence", (TDCC, TDCC_CADENCE_SOURCE),
        cadence_change_on="2015-05-01", older_carry_days=62),
    "pmi": ReleaseRule("pmi_release_schedule_plus1d", "pmi", 62, "market",
        "Provider index denotes release month; no earlier than third exchange session "
        "of that month as release estimate under official first-three-working-day rule; "
        "+1 calendar day safety, no further padding; not exact historical release", (PMI,)),
    "business": ReleaseRule("ndc_provider_release_plus1d", "business", 62, "market",
        "Provider release-date index, +1 calendar day; no month-end padding; "
        "not exact historical release and does not repair revisions", (NDC,)),
}
FAMILIES = {
    "financial_statement": "quarter", "monthly_revenue": "revenue",
    "fundamental_features": "quarter", "financial_statement_revised": "quarter_date",
    "security_lending": "daily", "foreign_investors_shareholding": "daily",
    "tw_total_pmi": "pmi", "tw_total_nmi": "pmi",
    "tw_business_indicators": "business",
}
EXACT = {
    "etl:inventory:大於四百張佔比": "weekly",
    "block_trade:成交金額": "daily", "tw_etf_nav_daily:折溢價(%)": "daily",
}


def rule_for(key: str) -> ReleaseRule | None:
    if key.startswith("etl:inventory:"):
        return RULES["weekly"]
    kind = EXACT.get(key, FAMILIES.get(key.split(":")[0]))
    return RULES.get(kind)


def feature_name(key: str) -> str:
    return "twfl_sched_" + re.sub(r"[^\w]+", "_", key).strip("_") + "_raw"


def feature_category(key: str) -> str:
    """Source family outranks ambiguous nouns (e.g. a balance-sheet loan)."""
    rule = rule_for(key)
    if rule is None:
        raise ValueError("unknown research source family")
    if rule.kind in {"quarter", "quarter_date", "revenue"}:
        return "fundamentals"
    if rule.kind in {"pmi", "business"}:
        return "macro"
    if rule.kind == "weekly" or key.startswith("foreign_investors_shareholding:"):
        return "shareholding"
    if key.startswith("security_lending:"):
        return "margin_lending"
    return "funds" if key.startswith("tw_etf_") else "liquidity"


def next_session(day: date, sessions: list[date], *, roll_deadline: bool = False) -> date | None:
    # A Sunday filing deadline can be extended through Monday evening. Making
    # it available Monday 09:00 would still leak; roll deadline, THEN shift.
    index = bisect_left(sessions, day) if roll_deadline else bisect_right(sessions, day)
    if roll_deadline:
        index += 1
    return sessions[index] if index < len(sessions) else None


def publication_proxy(value: object, rule: ReleaseRule, sessions: list[date]) -> date:
    text = str(value)
    if rule.kind == "quarter":
        m = re.fullmatch(r"(20\d\d)-?Q([1-4])", text)
        if not m:
            raise ValueError(f"quarter label required, not publication date: {text}")
        year, q = map(int, m.groups())
        return (date(year, 5, 15), date(year, 8, 14), date(year, 11, 14),
                date(year + 1, 3, 31))[q - 1]
    if rule.kind == "revenue" and (m := re.fullmatch(r"(20\d\d)-M(\d{2})", text)):
        year, month = map(int, m.groups())
        date(year, month, 1)  # validate, do not normalize malformed months
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
        return date(year, month, 10)
    try:
        observed = datetime.fromisoformat(text).date()
    except ValueError as exc:
        raise ValueError(f"unsupported {rule.kind} source index: {text}") from exc
    if rule.kind == "revenue":
        # Our pinned provider wide files use deadline dates, not month-end
        # accounting periods. Reject ambiguous shapes rather than add a month.
        if observed.day > 20:
            raise ValueError("monthly date looks like a period end; explicit period adapter required")
        return observed
    if rule.kind == "pmi":
        start = bisect_left(sessions, date(observed.year, observed.month, 1))
        candidates = sessions[start:start + 3]
        if len(candidates) < 3 or candidates[-1].month != observed.month:
            raise ValueError("calendar does not cover PMI release month")
        return max(observed, candidates[-1])
    return observed


def schedule_lookup(indexes: list[str], rule: ReleaseRule, sessions: list[date]) -> pl.DataFrame:
    if not sessions or sessions != sorted(set(sessions)):
        raise ValueError("official sessions must be nonempty, sorted and unique")
    rows = []
    for value in indexes:
        # Never squeeze older unbounded history onto the calendar's first day.
        year = str(value)[:4]
        period_year_grace = 1 if rule.kind in {"quarter", "revenue"} else 0
        if not year.isdigit() or int(year) < sessions[0].year - period_year_grace:
            continue
        if rule.kind == "pmi":
            observed = datetime.fromisoformat(str(value)).date()
            month = (observed.year, observed.month)
            first_month = (sessions[0].year, sessions[0].month)
            last_month = (sessions[-1].year, sessions[-1].month)
            # The source may already expose its newest release-month row.
            # A bounded historical calendar cannot invent the future third
            # session. Defer only outside/unfinished boundary windows; an
            # absent month INSIDE the archive still fails in publication_proxy.
            if month < first_month or month > last_month:
                continue
            if month == last_month:
                index = bisect_left(sessions, date(*month, 1))
                if len(sessions) - index < 3:
                    continue
        day = publication_proxy(value, rule, sessions)
        if day < sessions[0]:
            continue
        # Roll a legal deadline, not an observed/provider-mapped publication date.
        deadline = rule.kind in {"quarter", "quarter_date"} or (rule.kind == "revenue" and "-M" in str(value))
        if deadline:
            index = bisect_left(sessions, day)
            if index < len(sessions):
                day = sessions[index]
        ready = day + timedelta(days=1)
        usable = next_session(day, sessions)
        rows.append({"source_index": value, "estimated_published_on": day,
            "safety_ready_on": ready, "extra_delay_days": 1, "date": usable,
            "nontrading_wait_days": (usable - ready).days if usable else None})
    return pl.DataFrame(rows, schema={"source_index": pl.String,
        "estimated_published_on": pl.Date, "safety_ready_on": pl.Date,
        "extra_delay_days": pl.Int32, "date": pl.Date, "nontrading_wait_days": pl.Int32})


def apply_known_release_overrides(lookup: pl.DataFrame, key: str,
                                  sessions: list[date]) -> pl.DataFrame:
    """A survey's retrospective start is NOT its first public release.

    NDC's dated launch release explicitly says NMI was first published on
    2015-02-02, retrospectively to August 2014. Its January 2015 value was
    included at launch too; that KNOWN release overrides the third-day proxy.
    Keep +1 day safety, and collapse simultaneous versions chronologically.
    """
    if not key.startswith("tw_total_nmi:"):
        return lookup
    launch = pl.col("source_index").str.slice(0, 10).str.to_date() < date(2015, 3, 1)
    usable = next_session(NMI_FIRST_RELEASE, sessions)
    ready = NMI_FIRST_RELEASE + timedelta(days=1)
    return lookup.with_columns(
        pl.when(launch).then(pl.lit(NMI_FIRST_RELEASE)).otherwise(pl.col("estimated_published_on")).alias("estimated_published_on"),
        pl.when(launch).then(pl.lit(ready)).otherwise(pl.col("safety_ready_on")).alias("safety_ready_on"),
        pl.when(launch).then(pl.lit(usable, dtype=pl.Date)).otherwise(pl.col("date")).alias("date"),
        pl.when(launch).then(pl.lit((usable - ready).days if usable else None, dtype=pl.Int32))
        .otherwise(pl.col("nontrading_wait_days")).alias("nontrading_wait_days"))


def max_carry_days(rule: ReleaseRule) -> int:
    return max(rule.carry_days, rule.older_carry_days)


def carry_limit(rule: ReleaseRule, date_column: str = "date") -> pl.Expr:
    if rule.cadence_change_on:
        return pl.when(pl.col(date_column) < date.fromisoformat(rule.cadence_change_on)).then(
            rule.older_carry_days).otherwise(rule.carry_days)
    return pl.lit(rule.carry_days)


ALIGNMENT_CONTRACT = "tw_mixed_frequency_alignment_v2_scope_lifecycle"


@dataclass(frozen=True)
class ObservationKeys:
    """One validated/sorted target grain reused for many heterogeneous measures."""
    _left: pl.DataFrame
    rows: int


def prepare_observation_keys(keys: pl.DataFrame) -> ObservationKeys:
    if keys.select(pl.struct("date", "symbol").is_duplicated().any()).item():
        raise ValueError("ambiguous duplicate target key")
    if keys.select(pl.any_horizontal(pl.col("date").is_null(), pl.col("symbol").is_null()).any()).item():
        raise ValueError("null target key")
    return ObservationKeys(keys.with_row_index("_order").sort("date"), keys.height)


def align_observation_frame(keys: pl.DataFrame | ObservationKeys, observations: pl.DataFrame,
                            feature: str, rule: ReleaseRule) -> pl.DataFrame:
    """Causal state carry, bounded age; never interpolate values or fill events.

    Input order is preserved. A feature's observed 0 stays 0. No observation
    before the target date -> NULL. Retired concepts expire instead of leaking
    across taxonomy eras. A repeated report period cannot overwrite a newer one.
    """
    if rule.scope not in {"stock", "market"} or rule.carry_days < 0:
        raise ValueError("unsupported scope or negative state lifetime")
    prepared = keys if isinstance(keys, ObservationKeys) else prepare_observation_keys(keys)
    left = prepared._left
    by = [] if rule.scope == "market" else ["symbol"]
    right = observations.select("date", *by, feature).with_columns(
        pl.when(pl.col(feature).cast(pl.Float64, strict=False).is_finite())
        .then(pl.col(feature).cast(pl.Float64, strict=False)).otherwise(None).alias(feature)
    ).sort("date")
    if right.select(pl.any_horizontal(pl.col(c).is_null() for c in ["date", *by]).any()).item():
        raise ValueError("null observation key")
    if right.select(pl.struct("date", *by).is_duplicated().any()).item():
        raise ValueError("ambiguous duplicate observation key")
    right = right.with_columns(pl.col("date").alias("_state_date"))
    if rule.carry_days:
        joined = left.join_asof(right, on="date", by=by or None,
            strategy="backward", tolerance=f"{max_carry_days(rule)}d", check_sortedness=False)
        joined = joined.with_columns(pl.when(
            (pl.col("date") - pl.col("_state_date")).dt.total_days() <= carry_limit(rule))
            .then(pl.col(feature)).otherwise(None).alias(feature))
    else:
        joined = left.join(right, on=["date", *by], how="left", validate="m:1")
    if rule.scope == "stock" and "lifecycle_start" in left.columns:
        # A reused security code must not inherit the former issuer's state.
        joined = joined.with_columns(pl.when(
            pl.col("_state_date") >= pl.col("lifecycle_start")
        ).then(pl.col(feature)).otherwise(None).alias(feature))
    if joined.height != prepared.rows:
        raise ValueError("research alignment changed row count")
    available = pl.col(feature).is_not_null()
    return joined.sort("_order").select(
        "date", "symbol", pl.col(feature).cast(pl.Float32),
        available.alias(feature + "__available"),
        pl.when(available).then((pl.col("date") - pl.col("_state_date")).dt.total_days())
        .otherwise(None).cast(pl.Float32).alias(feature + "__age_days"),
        (pl.col("date") == pl.col("_state_date")).fill_null(False).alias(feature + "__updated"),
    )


def align_observations(keys: pl.DataFrame, observations: pl.DataFrame,
                       feature: str, rule: ReleaseRule) -> pl.Series:
    """Compatibility value-only API; clocks, masks and ages share one join."""
    return align_observation_frame(keys, observations, feature, rule).get_column(feature)


def rule_manifest() -> dict:
    return {"contract": CONTRACT, "decision_clock": "09:00 Asia/Taipei",
        "extra_conservative_delay_calendar_days": 1,
        "market_closed_wait_is_not_extra_safety_delay": True,
        "publication_time_estimated": True, "historical_point_in_time": False,
        "value_vintage": "current_provider_revision_not_original_release",
        "known_first_public_release": {"tw_total_nmi": {"date": str(NMI_FIRST_RELEASE),
            "source": NMI_LAUNCH_SOURCE, "note": "2014 observations were retrospective, not public then"}},
        "rules": {k: asdict(v) for k, v in RULES.items()}}
