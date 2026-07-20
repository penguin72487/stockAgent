from __future__ import annotations

import inspect
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, get_args, get_origin

import pyarrow as pa
import pyarrow.parquet as pq


PAGINATION_FIELD_NAMES = frozenset(
    {
        "all_pages",
        "cursor",
        "limit",
        "max_pages",
        "max_results",
        "offset",
        "page",
        "page_num",
        "page_number",
        "page_size",
        "page_token",
        "skip",
    }
)

# Page-based routes whose continuation is materialized as another manifest
# task.  Keep this next to the pagination proof strategies so the planner,
# startup reconciliation, and long-running monitor share one source of truth.
FMP_MANIFEST_PAGINATED_ENDPOINTS = frozenset(
    {
        "equity.discovery.filings",
        "equity.fundamental.filings",
        "equity.ownership.major_holders",
        "news.company",
        "news.world",
    }
)

# Manifest tasks whose provider fetcher has a non-pageable fixed cap.  A plan
# partition is only complete when its realized row count is strictly below the
# cap; the monitor enforces this runtime proof before declaring completion.
MANIFEST_SOURCE_CAP_LIMITS: dict[str, int] = {
    "etf.search": 10_000,
}

# Non-pageable routes where the configured provider entitlement explicitly
# limits how many historical rows the API exposes.  The archive stores every
# row accessible under that entitlement and records the cap as a coverage
# constraint; retrying date partitions cannot reveal older rows because these
# provider adapters apply dates only after receiving the same capped payload.
ENTITLEMENT_CAPPED_NONPAGEABLE_CONTRACTS = frozenset(
    {
        ("equity.fundamental.employee_count", "fmp"),
        ("equity.fundamental.metrics", "fmp"),
        ("equity.fundamental.ratios", "fmp"),
    }
)

# Historical time partitions are safe only when every fallback sends the
# partition boundary to its upstream API (or is handled by a provider-specific
# full-history planner).  This is deliberately a closed provider contract:
# OpenBB's merged command schema alone cannot prove request pushdown.
ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST: dict[str, frozenset[str]] = {
    "economy.calendar": frozenset({"fmp", "fred"}),
    "economy.central_bank_holdings": frozenset({"federal_reserve"}),
    "economy.pce": frozenset({"fred"}),
    "economy.survey.nonfarm_payrolls": frozenset({"fred"}),
    "equity.calendar.dividend": frozenset({"fmp"}),
    "equity.calendar.earnings": frozenset({"fmp"}),
    "equity.calendar.events": frozenset({"fmp"}),
    "equity.calendar.ipo": frozenset({"fmp", "intrinio"}),
    "equity.calendar.splits": frozenset({"fmp"}),
    "equity.discovery.filings": frozenset({"fmp"}),
    "equity.fundamental.filings": frozenset({"fmp", "intrinio", "sec"}),
    "fixedincome.corporate.hqm": frozenset({"fred"}),
    "fixedincome.government.treasury_prices": frozenset({"government_us"}),
    "fixedincome.government.yield_curve": frozenset(
        {"econdb", "federal_reserve", "fmp", "fred"}
    ),
    "news.company": frozenset({"benzinga", "fmp", "intrinio", "tiingo"}),
    "news.world": frozenset({"benzinga", "fmp", "intrinio", "tiingo"}),
}

LOCAL_ONLY_ARCHIVE_DATE_FILTERS = frozenset(
    {
        ("equity.fundamental.employee_count", "fmp"),
        ("news.company", "yfinance"),
    }
)

# Provider routes whose declared finite ``limit`` is a safety ceiling rather
# than a request to truncate the archive.  Their completeness proof requires
# the realized row count to remain strictly below that ceiling.  Keep this
# provider-specific: the same endpoint may use terminal-page continuation or
# an unbounded full-document request with another provider.
#
# This is intentionally a runtime contract, not only planner documentation.
# A provider/parser regression that starts returning exactly the advertised
# cap must make the final archive audit fail closed across every market.
DECLARED_LIMIT_STRICTLY_BELOW_CONTRACTS = frozenset(
    {
        ("economy.fred_series", "fred"),
        ("equity.estimates.forward_ebitda", "fmp"),
        ("equity.estimates.forward_eps", "fmp"),
        ("equity.estimates.historical", "fmp"),
        ("equity.fundamental.balance", "fmp"),
        ("equity.fundamental.balance_growth", "fmp"),
        ("equity.fundamental.cash", "fmp"),
        ("equity.fundamental.cash_growth", "fmp"),
        ("equity.fundamental.dividends", "fmp"),
        ("equity.fundamental.employee_count", "fmp"),
        ("equity.fundamental.historical_eps", "fmp"),
        ("equity.fundamental.income", "fmp"),
        ("equity.fundamental.income_growth", "fmp"),
        ("equity.fundamental.metrics", "fmp"),
        ("equity.fundamental.ratios", "fmp"),
    }
)
TEMPORAL_FIELD_NAMES = frozenset(
    {
        "date",
        "end_date",
        "end_year",
        "fiscal_year",
        "quarter",
        "start_date",
        "start_year",
        "year",
    }
)

# These parameters change representation or derive a lower-frequency/adjusted
# view from the same raw observations.  The archive stores the highest native
# frequency/raw value once; derived variants can be reproduced later without
# spending provider quota or duplicating data.
DERIVED_OR_PRESENTATION_FIELDS = frozenset(
    {
        "adjusted",
        "adjustment",
        "aggregation_method",
        "annual_average",
        "calculations",
        "chart",
        "columns",
        "display",
        "extended_hours",
        "frequency",
        "growth_rate",
        "interval",
        "order",
        "order_by",
        "sort",
        "sort_by",
        "sort_order",
        "theme",
        "transform",
        "unit",
        "use_cache",
    }
)

# A small number of provider choices are aliases/groups rather than disjoint
# datasets.  Each entry is a proof obligation: the observed task values must
# include every listed covering group.  This is deliberately declarative so an
# OpenBB schema change cannot silently inherit an unrelated default.
GROUP_COVER_CONTRACTS: dict[tuple[str, str], frozenset[Any]] = {
    ("economy.central_bank_holdings", "holding_type"): frozenset(
        {"all_treasury", "all_agency"}
    ),
    ("economy.primary_dealer_positioning", "category"): frozenset(
        {"treasuries", "mbs", "municipal", "corporate", "abs"}
    ),
    ("fixedincome.mortgage_indices", "index"): frozenset(
        {"primary", "ltv_lte_80", "ltv_gt_80"}
    ),
    ("fixedincome.rate.dpcredit", "parameter"): frozenset(
        {"daily_excl_weekend", "daily"}
    ),
}

# These choices are intentionally not separate raw datasets.  Annual and
# quarter include the provider's FY/Q1-Q4 aliases, while TTM and growth variants
# are derived from the archived statements.  Metrics/ratios explicitly retain
# both raw annual/quarter rows and provider TTM rows.
STATEMENT_PERIOD_ENDPOINTS = frozenset(
    {
        "equity.estimates.historical",
        "equity.fundamental.balance",
        "equity.fundamental.balance_growth",
        "equity.fundamental.cash",
        "equity.fundamental.cash_growth",
        "equity.fundamental.income",
        "equity.fundamental.income_growth",
        "equity.fundamental.metrics",
        "equity.fundamental.ratios",
        "equity.fundamental.revenue_per_geography",
        "equity.fundamental.revenue_per_segment",
    }
)

# Pagination mechanisms that are implemented outside the provider's exposed
# query fields.  Unknown page/offset contracts fail closed in the audit.
PAGINATION_STRATEGIES: dict[tuple[str, str], str] = {
    ("economy.fred_search", "fred"): "manifest offset continuation to short page",
    ("economy.fred_series", "fred"): "100000 bound exceeds daily rows in requested interval",
    ("economy.shipping.port_info", "imf"): "provider loops exceededTransferLimit",
    ("equity.calendar.ipo", "fmp"): "provider splits complete range into 90-day requests",
    ("equity.discovery.filings", "fmp"): "manifest page continuation to short page",
    ("equity.estimates.historical", "fmp"): "period cardinality below 1000-row page bound",
    ("equity.estimates.forward_ebitda", "fmp"): "forward horizon below 1000-row bound",
    ("equity.estimates.forward_eps", "fmp"): "forward horizon below 1000-row bound",
    ("equity.estimates.price_target", "fmp"): "custom page loop to short page",
    ("equity.fundamental.balance", "sec"): "SEC company facts are fetched in full",
    ("equity.fundamental.balance", "fmp"): "annual/quarter cardinality below 1000-row bound",
    ("equity.fundamental.balance", "yfinance"): "bounded fallback statement history",
    ("equity.fundamental.balance_growth", "sec"): "derived from complete SEC facts",
    ("equity.fundamental.balance_growth", "fmp"): "annual/quarter cardinality below 1000-row bound",
    ("equity.fundamental.cash", "sec"): "SEC company facts are fetched in full",
    ("equity.fundamental.cash", "fmp"): "annual/quarter cardinality below 1000-row bound",
    ("equity.fundamental.cash", "yfinance"): "bounded fallback statement history",
    ("equity.fundamental.cash_growth", "sec"): "derived from complete SEC facts",
    ("equity.fundamental.cash_growth", "fmp"): "annual/quarter cardinality below 1000-row bound",
    ("equity.fundamental.dividends", "fmp"): "explicit 10000-row bound exceeds daily archive maximum",
    ("equity.fundamental.employee_count", "fmp"): (
        "non-pageable current-entitlement cap; all exposed rows are retained"
    ),
    ("equity.fundamental.filings", "sec"): "limit=0 loads all SEC submission shards",
    ("equity.fundamental.filings", "fmp"): "manifest page continuation to short page",
    ("equity.fundamental.historical_eps", "fmp"): "annual cardinality below provider bound",
    ("equity.fundamental.income", "sec"): "SEC company facts are fetched in full",
    ("equity.fundamental.income", "fmp"): "annual/quarter cardinality below 1000-row bound",
    ("equity.fundamental.income", "yfinance"): "bounded fallback statement history",
    ("equity.fundamental.income_growth", "sec"): "derived from complete SEC facts",
    ("equity.fundamental.income_growth", "fmp"): "annual/quarter cardinality below 1000-row bound",
    ("equity.fundamental.metrics", "fmp"): (
        "non-pageable current-entitlement cap by annual/quarter/TTM query"
    ),
    ("equity.fundamental.ratios", "fmp"): (
        "non-pageable current-entitlement cap by annual/quarter/TTM query"
    ),
    ("equity.ownership.government_trades", "fmp"): "custom House/Senate page loops to short pages",
    ("equity.ownership.insider_trading", "fmp"): "custom transaction page loop to short page",
    ("equity.ownership.insider_trading", "sec"): (
        "official quarterly Form 3/4/5 bulk ZIPs from 2006 plus exact-range "
        "EDGAR submission shards for pre-2006 and the unpublished current quarter"
    ),
    ("equity.ownership.major_holders", "fmp"): "manifest page continuation to short page",
    ("equity.shorts.fails_to_deliver", "sec"): "official half-month bulk files since 2009",
    ("fixedincome.government.treasury_auctions", "government_us"): "TreasuryDirect unpaged full date/type query",
    ("news.company", "fmp"): "manifest page continuation to short page",
    ("news.company", "tiingo"): "manifest offset continuation to short page",
    ("news.world", "fmp"): "custom or manifest page continuation to short page",
    ("news.world", "tiingo"): "manifest offset continuation to short page",
    ("uscongress.amendments", "congress_gov"): "limit=0 provider loop to final offset",
    ("uscongress.bills", "congress_gov"): "limit=0 provider loop to final offset",
}

# Fixed request sizes found by scanning provider fetcher source even when the
# query schema does not expose a page/limit field.  The strategy must explain
# why the fixed size cannot truncate the archive.
SOURCE_CAP_STRATEGIES: dict[tuple[str, str], str] = {
    ("cftc.cot", "cftc"): (
        "one market-code/report-mode task has fewer than 1,000,000 weekly rows"
    ),
    ("cftc.cot_search", "cftc"): (
        "custom Socrata loop advances offset until a short 50,000-row page"
    ),
    ("commodity.short_term_energy_outlook", "eia"): (
        "provider chunks ten series and follows response.total in 5000-row offsets"
    ),
    ("equity.calendar.events", "fmp"): (
        "provider splits the requested range into three-day windows below 1000 rows"
    ),
    ("equity.historical_market_cap", "fmp"): (
        "provider splits explicit dates into five-year windows below 5000 daily rows"
    ),
    ("etf.search", "fmp"): (
        "planner enumerates every supported exchange; monitor rejects any "
        "partition reaching the fixed 10000-row cap"
    ),
    ("fixedincome.government.yield_curve", "econdb"): (
        "each country requests fewer than 50 declared maturity series"
    ),
}

# Current/latest date routes with a known native inception later than the user
# requested 2000 boundary.  The planner still starts at the first real release.
NATIVE_INCEPTION = {
    "economy.central_bank_holdings": "2003-07-09",
    "economy.shipping.port_volume": "2019-01-02",
    "equity.shorts.fails_to_deliver": "2009-01-01",
    "etf.nport_disclosure": "2019-01-01",
}


@dataclass(frozen=True)
class ContractAuditRow:
    endpoint: str
    provider: str
    provider_role: str
    axis: str
    field: str
    status: str
    strategy: str
    expected: str
    observed: str
    evidence: str


def _literal_values(annotation: Any) -> list[Any]:
    origin = get_origin(annotation)
    if str(origin).endswith("Literal"):
        return list(get_args(annotation))
    values: list[Any] = []
    for arg in get_args(annotation):
        values.extend(_literal_values(arg))
    return list(dict.fromkeys(values))


def _json_value(value: Any) -> str:
    if isinstance(value, (date,)):
        return value.isoformat()
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    except TypeError:
        return repr(value)


def _stable_values(values: Iterable[Any]) -> list[Any]:
    unique: dict[str, Any] = {}
    for value in values:
        unique.setdefault(_json_value(value), value)
    return [unique[key] for key in sorted(unique)]


def _field_is_filter(field_name: str, description: str, default: Any) -> bool:
    text = description.lower()
    return default is None and (
        field_name
        in {
            "category",
            "continent",
            "counterpart",
            "exchange",
            "filter_variable",
            "sentiment",
            "source",
            "transaction_type",
        }
        or "filter by" in text
        or "used to only return" in text
        or "if not provided, all" in text
        or "if none, retrieves all" in text
        or "defaults to all" in text
        or "default is all" in text
        or "default is 'all'" in text
        or "defaults to, 'all'" in text
        or "omitting will return all" in text
    )


class PlanContractAuditor:
    """Observe one complete planning pass and prove archive completeness axes.

    The observer stores only distinct values for schema-risk fields, so it adds
    bounded memory overhead even when the plan contains millions of tasks.
    """

    def __init__(
        self,
        obb: Any,
        *,
        start_date: str,
        end_date: str,
        excluded_literal_values: Iterable[Any] = ("crypto",),
    ) -> None:
        self.obb = obb
        self.start_date = start_date
        self.end_date = end_date
        self.excluded_literal_values = frozenset(excluded_literal_values)
        self._command_models: Mapping[str, str] = (
            obb.coverage._command_map.commands_model  # noqa: SLF001
        )
        registry_map = obb.coverage._provider_interface._registry_map  # noqa: SLF001
        self._original_models: Mapping[str, Mapping[str, Mapping[str, Any]]] = (
            registry_map.original_models
        )
        self._provider_map: Mapping[str, Mapping[str, Mapping[str, Any]]] = (
            obb.coverage._provider_interface.map  # noqa: SLF001
        )
        self._registry = registry_map.registry
        self._risk_fields: dict[str, set[str]] = {}
        self._values: dict[str, dict[str, dict[str, Any]]] = {}
        self._temporal_bounds: dict[str, dict[str, tuple[str, str]]] = {}
        self._task_counts: dict[str, int] = {}

    def _provider_fields(self, model: str, provider: str) -> Mapping[str, Any]:
        item = self._original_models.get(model, {}).get(provider)
        query = item.get("query") if item else None
        fields = getattr(query, "model_fields", {})
        if fields:
            return fields
        provider_item = self._provider_map.get(model, {}).get(provider, {})
        query_params = provider_item.get("QueryParams", {})
        raw_fields = query_params.get("fields", {})
        return raw_fields if isinstance(raw_fields, Mapping) else {}

    def register_endpoint(self, endpoint: str, providers: Sequence[str]) -> None:
        dot_endpoint = f".{endpoint}"
        model = self._command_models.get(dot_endpoint)
        if not model:
            return
        fields: set[str] = set()
        for provider in providers:
            for field_name, field in self._provider_fields(model, provider).items():
                literals = _literal_values(field.annotation)
                description = str(field.description or "")
                if (
                    field_name in PAGINATION_FIELD_NAMES
                    or field_name in TEMPORAL_FIELD_NAMES
                    or literals
                    or re.search(
                        r"(?i)latest|current|historical|all available|summary|monthly",
                        description,
                    )
                ):
                    fields.add(field_name)
        self._risk_fields[endpoint] = fields

    def observe_task(self, task: Any) -> None:
        endpoint = str(task.endpoint)
        self._task_counts[endpoint] = self._task_counts.get(endpoint, 0) + 1
        fields = self._risk_fields.get(endpoint, set())
        endpoint_values = self._values.setdefault(endpoint, {})
        for field_name in fields:
            value = task.kwargs.get(field_name, "<missing>")
            if field_name in TEMPORAL_FIELD_NAMES and value != "<missing>":
                # OpenBB's full-history adapters accept comma-separated dates
                # and apply them only after one upstream series download.  A
                # complete archive task must contribute the first and last
                # member of that grid to the temporal proof, not the first ten
                # characters of the entire comma-delimited string.
                if field_name == "date" and isinstance(value, str):
                    normalized_values = [
                        item.strip() for item in value.split(",") if item.strip()
                    ]
                elif field_name == "date" and isinstance(
                    value, (list, tuple, set, frozenset)
                ):
                    normalized_values = [
                        item.isoformat() if isinstance(item, date) else str(item)
                        for item in value
                    ]
                else:
                    normalized_values = [
                        value.isoformat() if isinstance(value, date) else str(value)
                    ]
                normalized_first = min(normalized_values)
                normalized_last = max(normalized_values)
                bounds = self._temporal_bounds.setdefault(endpoint, {})
                previous = bounds.get(field_name)
                bounds[field_name] = (
                    min(previous[0], normalized_first)
                    if previous
                    else normalized_first,
                    max(previous[1], normalized_last) if previous else normalized_last,
                )
            values = endpoint_values.setdefault(field_name, {})
            if field_name == "date" and value != "<missing>":
                # Boundary values are sufficient for the date-grid proof and
                # avoid retaining a roughly 76 KiB comma string in the audit.
                for normalized in (normalized_first, normalized_last):
                    if len(values) < 512:
                        values.setdefault(_json_value(normalized), normalized)
            elif len(values) < 512:
                values.setdefault(_json_value(value), value)

    def seed_discovery_contract(self, endpoint: str) -> None:
        """Record the deterministic kwargs emitted from successful catalogs."""
        values: dict[str, Sequence[Any]] = {
            "cftc.cot": {
                "start_date": (self.start_date,),
                "end_date": (self.end_date,),
                "report_type": (
                    "legacy",
                    "disaggregated",
                    "financial",
                    "supplemental",
                ),
                "futures_only": (False, True),
                "measure": ("all",),
                "limit": (0,),
            },
            "economy.fred_release_table": {"date": ("<missing>",)},
            "economy.fred_series": {
                "start_date": (self.start_date,),
                "end_date": (self.end_date,),
                "limit": (100000,),
                "frequency": ("<missing>",),
                "aggregation_method": ("<missing>",),
                "transform": ("<missing>",),
            },
            "economy.indicators": {
                "start_date": (self.start_date,),
                "end_date": (self.end_date,),
                "frequency": ("<catalog-native>",),
                "transform": ("<missing>",),
            },
            "economy.survey.bls_series": {
                "start_date": (self.start_date,),
                "end_date": (self.end_date,),
                "calculations": (True,),
                "annual_average": (False,),
                "aspects": (True,),
            },
        }.get(endpoint, {})
        endpoint_values = self._values.setdefault(endpoint, {})
        for field_name, field_values in values.items():
            target = endpoint_values.setdefault(field_name, {})
            for value in field_values:
                target.setdefault(_json_value(value), value)

    def _observed(self, endpoint: str, field_name: str) -> list[Any]:
        return _stable_values(
            self._values.get(endpoint, {}).get(field_name, {}).values()
        )

    def _fetcher_source(self, model: str, provider: str) -> str:
        try:
            fetcher = self._registry.providers[provider].fetcher_dict[model]
            return inspect.getsource(fetcher)
        except (AttributeError, KeyError, OSError, TypeError):
            return ""

    def _temporal_row(
        self,
        endpoint: str,
        provider: str,
        role: str,
        field_name: str,
        description: str,
    ) -> ContractAuditRow:
        observed = self._observed(endpoint, field_name)
        real_values = [value for value in observed if value != "<missing>"]
        # A source cannot be expected before its native inception, but a run
        # that explicitly starts later must not be required to backfill years
        # outside the requested archive boundary.
        expected_start = max(
            self.start_date, NATIVE_INCEPTION.get(endpoint, self.start_date)
        )
        status = "pass"
        strategy = "explicit temporal bounds"
        evidence = ""

        if field_name in {"start_date", "start_year"}:
            normalized = [str(value) for value in real_values]
            target = expected_start[:4] if field_name == "start_year" else expected_start
            if not normalized and endpoint in {
                "uscongress.amendments",
                "uscongress.bills",
            }:
                strategy = "Congress-number enumeration derived from requested years"
                evidence = "start bound is represented by first congress"
            elif not normalized or min(normalized) > target:
                status = "unresolved"
                evidence = f"earliest planned value does not reach {target}"
            else:
                evidence = f"earliest={min(normalized)}"
        elif field_name in {"end_date", "end_year"}:
            normalized = [str(value) for value in real_values]
            target = self.end_date[:4] if field_name == "end_year" else self.end_date
            if not normalized and endpoint in {
                "uscongress.amendments",
                "uscongress.bills",
            }:
                strategy = "Congress-number enumeration derived from requested years"
                evidence = "end bound is represented by last congress"
            elif not normalized or max(normalized) < target:
                status = "unresolved"
                evidence = f"latest planned value does not reach {target}"
            else:
                evidence = f"latest={max(normalized)}"
        elif field_name == "date":
            bounds = self._temporal_bounds.get(endpoint, {}).get(field_name)
            normalized = list(bounds) if bounds else [str(value) for value in real_values]
            all_history_mode = endpoint == "economy.central_bank_holdings" and any(
                value is True
                for mode in ("summary", "monthly")
                for value in self._observed(endpoint, mode)
            )
            fred_release_reconstructed = endpoint == "economy.fred_release_table"
            if fred_release_reconstructed:
                strategy = "latest release-table structure plus complete FRED series history"
                evidence = (
                    "release-table observation rows are reconstructed from every "
                    "discovered fred_series; date is not a separate raw series"
                )
            elif not normalized and not all_history_mode:
                status = "unresolved"
                evidence = "provider defaults to latest/current and no date grid exists"
            elif normalized:
                strategy = "explicit native-cadence date grid"
                evidence = f"grid={min(normalized)}..{max(normalized)}"
                expected_first = date.fromisoformat(expected_start)
                expected_last = date.fromisoformat(self.end_date)
                actual_first = date.fromisoformat(min(normalized)[:10])
                actual_last = date.fromisoformat(max(normalized)[:10])
                if actual_first > expected_first + timedelta(days=40):
                    status = "unresolved"
                    evidence += "; first grid point is too late"
                if actual_last < expected_last - timedelta(days=40):
                    status = "unresolved"
                    evidence += "; final grid point is too early"
            else:
                strategy = "provider-native all-history mode"
                evidence = "summary/monthly all-history task present"
        elif field_name in {"year", "fiscal_year", "quarter"}:
            all_history_description = bool(
                re.search(
                    r"(?i)if none,? all years|all years since|returns? all years|"
                    r"if none,? (?:returns?|retrieves?) all",
                    description,
                )
            )
            catalog_period_cover = endpoint in {
                "uscongress.amendments",
                "uscongress.bills",
            }
            company_facts_bulk = endpoint == "equity.compare.company_facts" and (
                "__all__" in self._observed(endpoint, "fact")
            )
            if not real_values and all_history_description:
                strategy = "provider-native all-years response"
                evidence = "omitted year explicitly returns all years"
            elif not real_values and catalog_period_cover:
                strategy = "Congress-number enumeration derived from requested years"
                evidence = "all congresses and bill/amendment types are planned"
            elif not real_values and company_facts_bulk:
                strategy = "SEC companyfacts bulk response"
                evidence = "year/fiscal filters are bypassed by fact=__all__"
            elif not real_values:
                status = "unresolved"
                evidence = "current/latest period default remains implicit"
            elif field_name in {"year", "fiscal_year"} and 0 in real_values:
                strategy = "provider all-years sentinel"
                evidence = "year=0"
            else:
                strategy = "explicit period enumeration"
                evidence = f"values={len(real_values)}"

        return ContractAuditRow(
            endpoint,
            provider,
            role,
            "temporal",
            field_name,
            status,
            strategy,
            f"{expected_start}..{self.end_date}",
            _json_value(observed),
            evidence or description[:240],
        )

    def _dimension_row(
        self,
        endpoint: str,
        provider: str,
        role: str,
        field_name: str,
        field: Any,
    ) -> ContractAuditRow:
        description = str(field.description or "")
        choices = _literal_values(field.annotation)
        expected_choices = [
            value
            for value in choices
            if value not in self.excluded_literal_values and value is not None
        ]
        observed = self._observed(endpoint, field_name)
        actual = {value for value in observed if value != "<missing>"}
        default = field.default
        status = "pass"
        strategy = "complete literal enumeration"
        evidence = ""

        group_cover = GROUP_COVER_CONTRACTS.get((endpoint, field_name))
        if group_cover is not None:
            strategy = "declared non-overlapping group cover"
            missing = group_cover - actual
            if missing:
                status = "unresolved"
                evidence = f"missing covering groups: {sorted(missing, key=str)}"
            else:
                evidence = f"cover={sorted(group_cover, key=str)}"
        elif endpoint == "commodity.petroleum_status_report" and field_name == "table":
            from openbb_us_eia.models.petroleum_status_report import WpsrTableMap
            from openbb_us_eia.utils.constants import WpsrTableChoices

            required = {"all", *map(str, WpsrTableMap["weekly_estimates"])}
            schema_mismatches = set(map(str, WpsrTableMap["weekly_estimates"])) - set(
                map(str, WpsrTableChoices)
            )
            strategy = (
                "all workbook tables plus direct-fetch fallback for provider "
                "schema/map mismatches"
            )
            missing = required - actual
            if missing:
                status = "unresolved"
                evidence = f"missing EIA tables: {sorted(missing)}"
            else:
                evidence = (
                    f"table cover={len(required)}; "
                    f"schema_map_bypass={sorted(schema_mismatches)}"
                )
        elif endpoint == "etf.search" and field_name == "exchange":
            required = set(expected_choices)
            strategy = "exchange partitions cover the provider fixed-size catalog cap"
            missing = required - actual
            if missing:
                status = "unresolved"
                evidence = f"missing capped-catalog partitions: {sorted(missing)}"
            else:
                evidence = f"exchange partitions={sorted(required)}"
        elif endpoint in STATEMENT_PERIOD_ENDPOINTS and field_name == "period":
            strategy = "annual plus quarter raw statements; aliases/TTM are derived"
            if not {"annual", "quarter"}.issubset(actual):
                status = "unresolved"
                evidence = "annual and quarter tasks are both required"
        elif endpoint in {
            "equity.fundamental.metrics",
            "equity.fundamental.ratios",
        } and field_name == "ttm":
            strategy = "raw periods exclude TTM; TTM is a separate task"
            if not {"only", "exclude"}.issubset(actual):
                status = "unresolved"
                evidence = "only and exclude tasks are both required"
        elif endpoint == "news.world" and field_name == "topic":
            required = {"fmp_articles", "general", "press_releases", "stocks", "forex"}
            strategy = "all non-crypto provider feeds"
            if provider == "fmp" and not required.issubset(actual):
                status = "unresolved"
                evidence = f"missing topics: {sorted(required - actual)}"
            elif provider != "fmp":
                evidence = "fallback provider has one unfiltered world feed"
        elif endpoint == "economy.fred_search" and field_name == "search_type":
            strategy = "release catalog discovery"
            if "release" not in actual:
                status = "unresolved"
                evidence = "release catalog task is missing"
            else:
                evidence = "full_text and series_id are search methods, not datasets"
        elif endpoint == "equity.compare.company_facts" and field_name == "fact":
            strategy = "SEC companyfacts bulk response"
            if "__all__" not in actual:
                status = "unresolved"
                evidence = "fact=__all__ bulk task is missing"
            else:
                evidence = "one companyfacts document contains every reported concept"
        elif (
            endpoint == "equity.compare.company_facts"
            and field_name == "fiscal_period"
            and "__all__" in self._observed(endpoint, "fact")
        ):
            strategy = "SEC companyfacts bulk response"
            evidence = "fiscal_period is ignored by the bulk companyfacts path"
        elif len(expected_choices) == 1 and (
            default == expected_choices[0] or "<missing>" in observed
        ):
            strategy = "single provider-supported value"
            evidence = f"only choice={expected_choices[0]}"
        elif field_name in DERIVED_OR_PRESENTATION_FIELDS:
            strategy = "highest native frequency/raw representation retained"
            evidence = "other choices are reproducible transforms or presentation"
        elif "all" in actual:
            strategy = "provider aggregate all value"
            evidence = "all explicitly planned"
        elif "all" in expected_choices and (
            default == "all" or "<missing>" in observed
        ):
            strategy = "provider aggregate all default"
            evidence = "unfiltered/default all covers every choice"
        elif expected_choices and set(expected_choices).issubset(actual):
            evidence = f"enumerated={len(expected_choices)}"
        elif _field_is_filter(field_name, description, default) and (
            "<missing>" in observed or default is None
        ):
            strategy = "omitted filter returns union"
            evidence = "provider query remains unfiltered"
        elif field_name == "historical" and True in actual:
            strategy = "historical mode includes current and past records"
            evidence = "historical=true"
        elif field_name == "latest" and False in actual:
            strategy = "all-history mode"
            evidence = "latest=false"
        elif field_name == "pit_mode" and True in actual:
            strategy = "point-in-time reconstruction"
            evidence = "pit_mode=true"
        elif field_name in {"futures_only", "harmonized"} and {False, True}.issubset(
            actual
        ):
            strategy = "boolean dataset enumeration"
            evidence = "false and true both planned"
        elif endpoint == "economy.central_bank_holdings" and field_name in {
            "summary",
            "monthly",
            "wam",
        }:
            strategy = "explicit SOMA dataset modes"
            if True not in actual:
                status = "unresolved"
                evidence = f"{field_name}=true task is missing"
        elif not expected_choices:
            # Non-literal descriptive fields are retained in the report but do
            # not become a finite enumeration obligation unless they select an
            # all-history mode covered above.
            strategy = "non-finite filter or operational flag"
            evidence = description[:240]
        else:
            status = "unresolved"
            strategy = "unclassified content dimension"
            missing = set(expected_choices) - actual
            evidence = f"missing choices: {sorted(missing, key=str)}"

        return ContractAuditRow(
            endpoint,
            provider,
            role,
            "dimension",
            field_name,
            status,
            strategy,
            _json_value(expected_choices),
            _json_value(observed),
            evidence,
        )

    def _pagination_row(
        self,
        endpoint: str,
        provider: str,
        role: str,
        model: str,
        fields: Mapping[str, Any],
    ) -> ContractAuditRow:
        names = sorted(set(fields) & PAGINATION_FIELD_NAMES)
        observed = {name: self._observed(endpoint, name) for name in names}
        strategy = PAGINATION_STRATEGIES.get((endpoint, provider))
        source = self._fetcher_source(model, provider)
        evidence = ""
        status = "pass"

        if strategy:
            evidence = strategy
        else:
            defaults = {name: fields[name].default for name in names}
            has_page_axis = bool(
                set(names)
                & {
                    "cursor",
                    "offset",
                    "page",
                    "page_num",
                    "page_number",
                    "page_size",
                    "page_token",
                    "skip",
                }
            )
            unbounded = any(
                value in {0, True}
                for values in observed.values()
                for value in values
                if value != "<missing>"
            )
            source_default_cap = bool(
                re.search(
                    r"query\.limit\s+if\s+query\.limit\s+else\s+\d+|"
                    r"query\.limit\s+or\s+\d+|\[:\s*query\.limit",
                    source,
                )
            )
            if unbounded and not has_page_axis:
                strategy = "provider unbounded/all-pages sentinel"
                evidence = "limit=0 or all_pages=true"
            elif not has_page_axis and not source_default_cap and all(
                value is None for value in defaults.values()
            ):
                strategy = "provider native unbounded response"
                evidence = "no finite planner/provider default detected"
            else:
                status = "warning" if role == "fallback" else "unresolved"
                strategy = "unclassified pagination contract"
                evidence = (
                    "page/offset or finite source cap exists without a declared "
                    "terminal short-page proof"
                )

        return ContractAuditRow(
            endpoint,
            provider,
            role,
            "pagination",
            ",".join(names),
            status,
            strategy or "unclassified",
            "all pages through terminal short/empty page",
            _json_value(observed),
            evidence,
        )

    def _source_cap_row(
        self,
        endpoint: str,
        provider: str,
        role: str,
        model: str,
    ) -> ContractAuditRow | None:
        source = self._fetcher_source(model, provider)
        matches = re.findall(
            r"(?i)(?:limit|length|page_size)=\d+|"
            r"\b(?:limit|length|page_size|max_results)\s*=\s*\d+|"
            r"[\"'](?:limit|length|page_size)[\"']\s*:\s*\d+|"
            r"\.head\(\s*\d+\s*\)",
            source,
        )
        if not matches:
            return None
        strategy = SOURCE_CAP_STRATEGIES.get(
            (endpoint, provider)
        ) or PAGINATION_STRATEGIES.get((endpoint, provider))
        status = "pass" if strategy else "warning" if role == "fallback" else "unresolved"
        return ContractAuditRow(
            endpoint,
            provider,
            role,
            "source_cap",
            "hardcoded_fetcher_limit",
            status,
            strategy or "unclassified provider source cap",
            "chunk/paginate/partition until the complete universe is covered",
            _json_value(sorted(set(matches))),
            "fixed provider source cap detected independently of query schema",
        )

    def _source_temporal_row(
        self,
        endpoint: str,
        provider: str,
        role: str,
        model: str,
        fields: Mapping[str, Any],
    ) -> ContractAuditRow | None:
        temporal = sorted(set(fields) & TEMPORAL_FIELD_NAMES)
        if not temporal:
            return None
        source = self._fetcher_source(model, provider)
        delegated_dump = "model_dump" in source or ".dict(" in source
        missing = [
            field_name
            for field_name in temporal
            if field_name not in source and not delegated_dump
        ]
        status = "pass" if not missing else "warning" if role == "fallback" else "unresolved"
        return ContractAuditRow(
            endpoint,
            provider,
            role,
            "source_temporal",
            ",".join(temporal),
            status,
            "static provider fetcher parameter-use scan",
            "every exposed temporal bound reaches the provider request/transform",
            _json_value({"missing_source_references": missing}),
            "fetcher source references all temporal fields"
            if not missing
            else f"fetcher source does not reference: {missing}",
        )

    def finalize(self, coverage: Sequence[Any]) -> tuple[list[ContractAuditRow], dict[str, Any]]:
        rows: list[ContractAuditRow] = []
        for decision in coverage:
            if str(decision.decision) not in {"included", "deferred"}:
                continue
            endpoint = str(decision.endpoint)
            providers = [
                value
                for value in str(decision.selected_providers or "").split(",")
                if value
            ]
            model = self._command_models.get(f".{endpoint}")
            if not model:
                rows.append(
                    ContractAuditRow(
                        endpoint,
                        "-",
                        "primary",
                        "schema",
                        "command_model",
                        "unresolved",
                        "OpenBB command model lookup",
                        "registered command model",
                        "missing",
                        "included endpoint has no OpenBB command model",
                    )
                )
                continue
            for index, provider in enumerate(providers):
                role = "primary" if index == 0 else "fallback"
                fields = self._provider_fields(model, provider)
                if not fields:
                    provider_registered = (
                        provider in self._original_models.get(model, {})
                        or provider in self._provider_map.get(model, {})
                    )
                    rows.append(
                        ContractAuditRow(
                            endpoint,
                            provider,
                            role,
                            "schema",
                            "query_model",
                            "pass" if provider_registered else "unresolved",
                            "parameterless provider query"
                            if provider_registered
                            else "provider query model lookup",
                            "registered query model",
                            "no query parameters"
                            if provider_registered
                            else "missing",
                            "provider route has no query parameters"
                            if provider_registered
                            else "selected provider has no query schema",
                        )
                    )
                    continue

                for field_name, field in fields.items():
                    description = str(field.description or "")
                    if field_name in TEMPORAL_FIELD_NAMES and re.search(
                        r"(?i)date|year|quarter|latest|current|recent", description
                    ):
                        rows.append(
                            self._temporal_row(
                                endpoint,
                                provider,
                                role,
                                field_name,
                                description,
                            )
                        )
                    literals = _literal_values(field.annotation)
                    if literals or re.search(
                        r"(?i)latest|historical|summary|monthly|all available",
                        description,
                    ):
                        rows.append(
                            self._dimension_row(
                                endpoint, provider, role, field_name, field
                            )
                        )

                if set(fields) & PAGINATION_FIELD_NAMES:
                    rows.append(
                        self._pagination_row(
                            endpoint, provider, role, model, fields
                        )
                    )
                source_cap_row = self._source_cap_row(
                    endpoint, provider, role, model
                )
                if source_cap_row is not None:
                    rows.append(source_cap_row)
                source_temporal_row = self._source_temporal_row(
                    endpoint, provider, role, model, fields
                )
                if source_temporal_row is not None:
                    rows.append(source_temporal_row)
                if endpoint in ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST:
                    allowed = ARCHIVE_TIME_SHARD_PROVIDER_ALLOWLIST[endpoint]
                    verified = provider in allowed
                    rows.append(
                        ContractAuditRow(
                            endpoint,
                            provider,
                            role,
                            "source_time_pushdown",
                            "historical_partition",
                            "pass" if verified else "unresolved",
                            "closed adapter request-pushdown review",
                            (
                                "provider-native bounded query or explicitly "
                                "planned full-history query"
                            ),
                            _json_value({"allowed_providers": sorted(allowed)}),
                            (
                                "provider is approved for historical partitions"
                                if verified
                                else "provider is not approved for historical partitions"
                            ),
                        )
                    )

        # A fallback warning is visible but does not invalidate a complete
        # primary+fallback plan.  Any unclassified primary/schema contract does.
        counts: dict[str, int] = {}
        axis_counts: dict[str, int] = {}
        for row in rows:
            counts[row.status] = counts.get(row.status, 0) + 1
            if row.status == "unresolved":
                axis_counts[row.axis] = axis_counts.get(row.axis, 0) + 1
        unresolved = counts.get("unresolved", 0)
        summary = {
            "schema_version": 1,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "included_endpoints": sum(
                str(item.decision) == "included" for item in coverage
            ),
            "deferred_catalog_endpoints": sum(
                str(item.decision) == "deferred" for item in coverage
            ),
            "observed_tasks": sum(self._task_counts.values()),
            "contract_rows": len(rows),
            "status_counts": dict(sorted(counts.items())),
            "unresolved_by_axis": dict(sorted(axis_counts.items())),
            "unresolved": unresolved,
            "passed": unresolved == 0,
        }
        return rows, summary


def write_contract_audit(
    output_dir: Path,
    rows: Sequence[ContractAuditRow],
    summary: Mapping[str, Any],
) -> None:
    catalog_dir = output_dir / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([asdict(row) for row in rows])
    target = catalog_dir / "completeness_contract_audit.parquet"
    temporary = target.with_name(f".{target.name}.tmp")
    try:
        pq.write_table(table, temporary, compression="zstd", compression_level=6)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    summary_target = catalog_dir / "completeness_contract_summary.json"
    summary_temporary = summary_target.with_name(f".{summary_target.name}.tmp")
    try:
        summary_temporary.write_text(
            json.dumps(dict(summary), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        summary_temporary.replace(summary_target)
    finally:
        summary_temporary.unlink(missing_ok=True)
