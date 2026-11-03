"""Post-decision entry adapters for the canonical whole-contract carry ledger.

Minutes are Taipei wall time, labelled at their right endpoint. A one-minute
policy latency precedes a one-minute trade-print VWAP window. This is an
explicit research execution proxy, not a reconstructed bid/ask fill.
"""
from __future__ import annotations

from dataclasses import fields, replace
from datetime import date
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.research.taifex_transaction_tax import futures_transaction_tax_rate

CLOCK_VERSION = 1
CLOCK_DATASET = "tw_stock_futures_carry_decision_clock_v1"
EQUITY_CLOCK_DATASET = "tw_twd_equity_futures_carry_decision_clock_v2"
WINDOW_CLOCK_DATASET = "tw_futures_carry_execution_window_v3"
EXTENDED_WINDOW_CLOCK_DATASET = "tw_futures_carry_extended_execution_window_v4"
ALL_TWD_CLOCK_DATASET = "tw_all_twd_futures_carry_execution_window_v5"
PARTIAL_TWD_CLOCK_DATASET = "tw_all_twd_futures_observed_partial_window_v6"
MAX_EXECUTION_WINDOW_MINUTES = 300
QUARANTINE_CONTRACT = "tw_futures_unknown_entry_quarantine_v1"
ENTRY_CAPACITY_POLICIES = ("previous_session", "single_contract_probe")
ASSET_SCOPES = {
    "stock_only": ("stock_future",),
    "twd_equity": ("stock_future", "etf_future", "index_future"),
    "all_twd": ("stock_future", "etf_future", "index_future", "commodity_future", "interest_rate_future"),
}
ACCEPTED_STATUSES = {"observed_minute", "verified_no_window_trade", "daily_not_executable"}
OPENING_REFERENCE_DATASET = "tw_futures_observed_open_or_last_trade_v1"
OPENING_REFERENCE_FEATURES = (
    "opening_reference_log_return_from_prior_trade",
    "opening_reference_available",
    "opening_reference_is_session_open",
    "opening_reference_age_days_log1p",
)


def clock_asset_classes(asset_scope: str) -> tuple[str, ...]:
    if asset_scope not in ASSET_SCOPES:
        raise ValueError("unsupported futures decision-clock asset scope")
    return ASSET_SCOPES[asset_scope]


def clock_dataset(asset_scope: str, *, window_minutes=1, quarantine_unknown=False,
                  allow_partial_observations=False) -> str:
    clock_asset_classes(asset_scope)
    _validate_window_options(window_minutes, quarantine_unknown)
    _validate_partial_options(asset_scope, quarantine_unknown, allow_partial_observations)
    if allow_partial_observations:
        return PARTIAL_TWD_CLOCK_DATASET
    if asset_scope == "all_twd":
        return ALL_TWD_CLOCK_DATASET
    if window_minutes > 60:
        return EXTENDED_WINDOW_CLOCK_DATASET
    if window_minutes != 1 or quarantine_unknown:
        return WINDOW_CLOCK_DATASET
    return CLOCK_DATASET if asset_scope == "stock_only" else EQUITY_CLOCK_DATASET


def _validate_window_options(window_minutes, quarantine_unknown):
    if (isinstance(window_minutes, bool) or not isinstance(window_minutes, int)
            or not 1 <= window_minutes <= MAX_EXECUTION_WINDOW_MINUTES
            or not isinstance(quarantine_unknown, bool)):
        raise ValueError("execution window must be 1..300 whole minutes with an explicit quarantine flag")


def _validate_partial_options(asset_scope, quarantine_unknown, allow_partial_observations):
    if (not isinstance(allow_partial_observations, bool)
            or allow_partial_observations and (asset_scope != 'all_twd' or not quarantine_unknown)):
        raise ValueError('partial observations require the explicit all-TWD quarantine research clock')


def decision_clock_contract(decision_time: str, *, asset_scope="stock_only",
                            window_minutes=1, quarantine_unknown=False,
                            allow_partial_observations=False) -> dict:
    classes = clock_asset_classes(asset_scope)
    _validate_window_options(window_minutes, quarantine_unknown)
    _validate_partial_options(asset_scope, quarantine_unknown, allow_partial_observations)
    if decision_time not in {"08:45", "09:00"}:
        raise ValueError("supported futures decision times are 08:45 and 09:00")
    hour, minute = map(int, decision_time.split(":"))
    decision = hour * 60 + minute
    contract = {
        "version": CLOCK_VERSION,
        "decision_time": decision_time,
        "timezone": "Asia/Taipei",
        "feature_clock": "completed_prior_session_zero_current_cash_open_gap",
        "latency_minutes": 1,
        "window_start_minute": decision + 1,
        "window_end_minute": decision + 2,
        "minute_label": "right_endpoint_[start,end)",
        "price": "observed_trade_print_minute_vwap_research_proxy",
        "capacity": "min_prior_session_capacity_floor_observed_window_volume_times_participation",
        "missing_source": "reject_admission_never_daily_open_fallback",
        "verified_no_entry": "zero_trade_capacity_prior_settlement_valuation_only",
        "holding": "carry_actual_contracts_and_official_settlement_no_daily_flatten",
    }
    # The original stock-only ABI remains byte-for-byte compatible. An expanded
    # action universe needs its own source, execution and checkpoint identity.
    if asset_scope != "stock_only":
        contract.update(version=2, asset_scope=asset_scope, asset_classes=list(classes),
            entry_value="dated_linear_quote_times_multiplier_plus_deliverable_cash",
            entry_tax="dated_asset_class_quote_notional_whole_twd_half_up")
    if window_minutes != 1 or quarantine_unknown:
        contract.update(version=3, asset_scope=asset_scope, asset_classes=list(classes),
            window_minutes=window_minutes, window_end_minute=decision + 1 + window_minutes,
            price="observed_trade_print_window_vwap_research_proxy",
            quarantine_unknown_execution=quarantine_unknown)
        if quarantine_unknown:
            contract["missing_source"] = "exact_receipt_allowlist_zero_order_capacity_preserve_official_carry_valuation"
    if window_minutes > 60:
        # Historical illiquid adjusted contracts can first trade hours after
        # either decision clock. Their observed prints remain usable by a
        # precommitted order; they never become information at decision time.
        contract.update(version=4,
            order_lifetime="precommitted_postdecision_window_no_intraday_feature_refresh",
            source_session="observed_dated_day_session_only_no_night_or_invented_minutes")
    if asset_scope == "all_twd":
        contract.update(version=5,
            entry_tax="dated_specification_quoted_full_or_face_value_whole_twd_half_up",
            entry_margin="dated_specification_quoted_full_or_face_value",
            source_session="dated_specification_regular_and_expiry_clocks",
            specification_source="local_content_pinned_dated_product_intervals")
    if allow_partial_observations:
        contract.update(version=6, allow_partial_observations=True,
            price="observed_available_prints_vwap_research_proxy_incomplete_source",
            missing_source="positive_bounded_observations_only_otherwise_unknown_zero_order_capacity",
            partial_observation_scope="exact_quarantined_physical_contract_day_and_window",
            partial_observation_capacity="observed_positive_volume_no_inferred_missing_prints",
            partial_observation_is_not="full_window_certificate_opening_reference_or_broker_fill")
    return contract


def clock_contract_from_manifest(manifest: dict, decision_time: str) -> dict:
    """Resolve a completed clock's actual scope and window without guessing."""
    error = "futures entry clock manifest is incomplete or incompatible"
    if not isinstance(manifest, dict) or not isinstance(manifest.get("clock"), dict):
        raise ValueError(error)
    declared = manifest["clock"]
    scope = declared.get("asset_scope", "stock_only")
    options = {"window_minutes": declared.get("window_minutes", 1),
               "quarantine_unknown": declared.get("quarantine_unknown_execution", False),
               "allow_partial_observations": declared.get("allow_partial_observations", False)}
    try:
        expected = decision_clock_contract(decision_time, asset_scope=scope, **options)
        dataset = clock_dataset(scope, **options)
    except (TypeError, ValueError) as exc:
        raise ValueError(error) from exc
    if (manifest.get("status") != "complete" or manifest.get("dataset") != dataset
            or declared != expected):
        raise ValueError(error)
    return expected


def validate_execution_quarantine(document, gaps, *, daily_sha256, rules_sha256):
    """Admit only explicitly inventoried unknown executions, never empty trades.

    The financial daily/rules release remains authoritative for valuation. This
    receipt permits zero order capacity on exact contract-days; it cannot add a
    price, clear inventory or broaden the allowlist when a new source gap occurs.
    """
    if (document.get("contract") != QUARANTINE_CONTRACT
            or document.get("source_daily_sha256") != daily_sha256
            or document.get("source_rules_sha256") != rules_sha256):
        raise ValueError("execution quarantine is not bound to the financial source")
    items = document.get("contract_days")
    if not isinstance(items, list) or not items:
        raise ValueError("execution quarantine requires a nonempty exact allowlist")
    allowed = [(str(item["date"]), str(item["physical_contract"])) for item in items]
    actual = [(str(d), str(p)) for d, p in gaps.select("date", "physical_contract").iter_rows()]
    if (len(set(allowed)) != len(allowed) or len(set(actual)) != len(actual)
            or set(actual) != set(allowed) or any(not item.get("reason") for item in items)):
        raise ValueError("execution source gaps differ from the exact quarantine allowlist")


def observed_window_entries(minutes, clock):
    """Reduce only observed prints in a fixed, strictly post-decision window.

    The one-minute branch preserves the original entry ABI and floating values.
    Longer windows share the authenticated minute tape; volume-weighted FP64
    reductions add capacity without turning an opening reference into a fill.
    """
    selected = minutes.filter((pl.col("minute") > clock["window_start_minute"])
        & (pl.col("minute") <= clock["window_end_minute"]))
    keys = ["date", "physical_contract"]
    if clock.get("window_minutes", 1) == 1:
        return selected.select(*keys, pl.col("vwap").alias("entry_quote"),
            pl.col("volume").alias("entry_volume"), "source_file_sha256")
    if selected.filter(pl.col("vwap").is_null() | ~pl.col("vwap").is_finite()
            | (pl.col("vwap") <= 0) | pl.col("volume").is_null()
            | ~pl.col("volume").is_finite() | (pl.col("volume") <= 0)
            | (pl.col("volume") != pl.col("volume").floor())
            | pl.col("source_file_sha256").is_null()).height:
        raise ValueError("execution window requires observed positive whole-contract prints")
    grouped = selected.group_by(keys).agg(
        (pl.col("vwap").cast(pl.Float64) * pl.col("volume").cast(pl.Float64)).sum().alias("_value"),
        pl.col("volume").cast(pl.Float64).sum().alias("entry_volume"),
        pl.col("source_file_sha256").first(),
        pl.col("source_file_sha256").n_unique().alias("_source_count"))
    if grouped.filter(pl.col("_source_count") != 1).height:
        raise ValueError("execution window cannot silently merge different source certificates")
    return grouped.with_columns((pl.col("_value") / pl.col("entry_volume")).alias("entry_quote")).select(
        *keys, "entry_quote", "entry_volume", "source_file_sha256")


def opening_reference_contract(decision_time: str, *, version: int = 1) -> dict:
    decision_clock_contract(decision_time)
    if isinstance(version, bool) or version not in (1, 2, 3):
        raise ValueError("unsupported opening reference contract version")
    contract = {
        "version": 1, "decision_time": decision_time, "timezone": "Asia/Taipei",
        "policy": "observed_session_open_else_last_observed_trade",
        "selection": "available_by_lte_decision_same_physical_contract",
        "session_open": "first_real_session_trade_only_after_observed",
        "fallback": "latest_known_real_trade_including_prior_session_or_preopen_trades",
        "unknown": "retain_known_trade_with_original_clock_never_claim_no_trade",
        "historical_clock": "completed_minute_or_completed_prior_session_availability_bound_not_receive_time",
        "features": list(OPENING_REFERENCE_FEATURES),
        "execution": "reference_only_no_fill_volume_margin_or_settlement_rewrite",
    }
    if version == 2:
        contract.update(version=2,
            fallback="latest_known_real_trade_including_prior_session_or_preopen_trades_fail_closed_on_unordered_last_second",
            last_trade_ties="different_prices_in_same_last_second_unavailable_not_arbitrary_source_order",
            fallback_evidence="explicit_per_event_ambiguity_required_unknown_is_not_assumed_false")
    if version == 3:
        contract.update(version=3,
            unknown="withhold_reference_across_unverified_contract_days_or_before_covered_daily_axis",
            session_open="unique_first_second_or_verified_single_price_first_minute_after_available",
            fallback="unavailable_until_cross_session_and_last_second_continuity_are_certified",
            prior_price="unavailable_until_cross_session_and_last_second_continuity_are_certified",
            gap_scope="authenticated_daily_axis_exact_executable_contract_day_quarantine",
            last_trade_ties="unproven_fallbacks_withheld_never_infer_source_order",
            night_session="regular_session_source_does_not_certify_intervening_night_trades")
    return contract


def opening_reference_contract_from_manifest(manifest: dict, decision_time: str) -> dict:
    """Read the actual reference ABI rather than assuming the legacy version."""
    error = 'opening reference source/clock/pair identity is incompatible'
    if not isinstance(manifest, dict) or not isinstance(manifest.get('contract'), dict):
        raise ValueError(error)
    declared = manifest['contract']
    try:
        expected = opening_reference_contract(decision_time, version=declared.get('version'))
    except (TypeError, ValueError) as exc:
        raise ValueError(error) from exc
    if (manifest.get('dataset') != OPENING_REFERENCE_DATASET or manifest.get('status') != 'complete'
            or declared != expected):
        raise ValueError(error)
    return expected


def resolve_opening_references(decisions, trades, *, decision_time, version=1):
    """Causal as-of input selection; never select by a future availability time.

    An event's `available_by` is separate from its original `trade_ts` (nullable
    for date-only daily observations). A pre-open event can be the last trade
    without being a session-opening print. No event is invented for missing
    prices or an incomplete source. Callers own source authentication.
    """
    opening_reference_contract(decision_time, version=version)
    if version == 3:
        raise ValueError('version 3 requires the gap-aware reference preparation path')
    if 'opening_ambiguous' not in trades.columns:
        # Legacy producers with already certified individual events remain readable.
        trades = trades.with_columns(pl.lit(False).alias('opening_ambiguous'))
    if version == 2 and 'fallback_ambiguous' not in trades.columns:
        raise ValueError('opening reference contract v2 requires explicit fallback ambiguity')
    if version == 1 and 'fallback_ambiguous' in trades.columns and trades.filter(
            pl.col('fallback_ambiguous').fill_null(True)).height:
        raise ValueError('ambiguous fallback trades require opening reference contract v2')
    keys = ["date", "physical_contract"]
    required = {"physical_contract", "price", "trade_date", "trade_ts", "available_by",
                "source_sha256", "is_session_open", "opening_ambiguous"}
    if version == 2:
        required.add('fallback_ambiguous')
    if required - set(trades.columns) or decisions.select(keys).is_duplicated().any():
        raise ValueError("opening references require unique decisions and timed real-trade events")
    if trades.filter(pl.col("price").is_null() | ~pl.col("price").is_finite() | (pl.col("price") <= 0)
            | pl.col("available_by").is_null() | pl.col("trade_date").is_null()
            | pl.col("is_session_open").is_null()
            | pl.col("opening_ambiguous").is_null()
            | (pl.col("opening_ambiguous") & ~pl.col("is_session_open"))
            | ((pl.col('fallback_ambiguous').is_null()
                | (pl.col('fallback_ambiguous') & pl.col('is_session_open'))) if version == 2 else pl.lit(False))
            | pl.col("source_sha256").is_null()
            | (pl.col("source_sha256").str.len_chars() != 64)
            | (pl.col("trade_ts").is_not_null() & (pl.col("trade_ts") > pl.col("available_by")))
            | (pl.col("trade_date") > pl.col("available_by").dt.date())).height:
        raise ValueError("invalid real-trade price, source, or availability clock")
    # Identical duplicates are harmless; contradictory events at one available
    # instant need a source/sequence resolution rather than arbitrary row order.
    trades = trades.unique()
    if trades.filter(pl.col("is_session_open")).select("physical_contract", "trade_date").is_duplicated().any():
        raise ValueError("multiple session-opening trades for one physical contract-day")
    if trades.select("physical_contract", "available_by", "is_session_open").is_duplicated().any():
        raise ValueError("ambiguous trade events at one availability boundary")
    hour, minute = map(int, decision_time.split(":"))
    left = decisions.with_columns((pl.col("date").cast(pl.Datetime("us"))
        + pl.duration(hours=hour, minutes=minute)).alias("decision_ts"))
    last_events = trades.sort("is_session_open", descending=True).unique(
        subset=["physical_contract", "available_by"], keep="last", maintain_order=True)
    prior = last_events.rename({
        name: "_last_" + name for name in required if name != "physical_contract"})
    left = left.sort("decision_ts").join_asof(prior.sort("_last_available_by"),
        left_on="decision_ts", right_on="_last_available_by", by="physical_contract",
        strategy="backward", check_sortedness=False)
    opens = trades.filter(pl.col("is_session_open")).rename({
        name: "_open_" + name for name in required if name != "physical_contract"})
    # The as-of predicate is evaluated before the session-date predicate.
    left = left.sort("decision_ts").join_asof(opens.sort("_open_available_by"),
        left_on="decision_ts", right_on="_open_available_by", by="physical_contract",
        strategy="backward", check_sortedness=False)
    has_open = pl.col("_open_trade_date") == pl.col("date")
    known_open = has_open & ~pl.col("_open_opening_ambiguous")
    fallback_unknown = pl.col('_last_fallback_ambiguous').fill_null(False) if version == 2 else pl.lit(False)
    for output, raw in (("reference_quote", "price"), ("reference_trade_date", "trade_date"),
                        ("reference_trade_ts", "trade_ts"), ("reference_available_by", "available_by"),
                        ("reference_source_sha256", "source_sha256")):
        left = left.with_columns(pl.when(known_open).then(pl.col("_open_" + raw))
                                 .when(has_open).then(None)
                                 .when(fallback_unknown).then(None)
                                 .otherwise(pl.col("_last_" + raw)).alias(output))
    # The denominator is a real prior-session trade, never a settlement mark.
    prior_left = decisions.with_columns(pl.col("date").cast(pl.Datetime("us")).alias("_prior_cutoff"))
    prior_left = prior_left.sort("_prior_cutoff").join_asof(prior.sort("_last_available_by"),
        left_on="_prior_cutoff", right_on="_last_available_by", by="physical_contract",
        strategy="backward", allow_exact_matches=False, check_sortedness=False)
    prior_price = (pl.when(pl.col('_last_fallback_ambiguous').fill_null(False)).then(None)
                   .otherwise(pl.col('_last_price')) if version == 2 else pl.col('_last_price'))
    left = left.join(prior_left.select(*keys, prior_price.alias("prior_trade_quote")),
                     on=keys, how="left", validate="1:1")
    left = left.with_columns(
        pl.when(known_open).then(pl.lit("observed_session_open"))
          .when(pl.col("reference_quote").is_not_null()).then(pl.lit("last_observed_trade"))
          .otherwise(pl.lit("unavailable")).alias("reference_kind"),
        (pl.col("date") - pl.col("reference_trade_date")).dt.total_days().alias("reference_age_days"))
    left = left.with_columns(
        pl.when(pl.col("reference_quote").is_not_null() & pl.col("prior_trade_quote").is_not_null())
          .then((pl.col("reference_quote") / pl.col("prior_trade_quote")).log())
          .otherwise(0.).alias(OPENING_REFERENCE_FEATURES[0]),
        pl.col("reference_quote").is_not_null().cast(pl.Float64).alias(OPENING_REFERENCE_FEATURES[1]),
        (pl.col("reference_kind") == "observed_session_open").cast(pl.Float64).alias(OPENING_REFERENCE_FEATURES[2]),
        pl.col("reference_age_days").cast(pl.Float64).log1p().fill_null(0.).alias(OPENING_REFERENCE_FEATURES[3]))
    return left.select(*decisions.columns, "decision_ts", "reference_quote", "reference_kind",
        "reference_trade_date", "reference_trade_ts", "reference_available_by", "reference_source_sha256",
        "reference_age_days", "prior_trade_quote", *OPENING_REFERENCE_FEATURES).sort(keys)


def validate_opening_reference_source(path, *, daily_path, decision_time, entry_manifest):
    path = Path(path)
    manifest = json.loads(path.with_name("opening_reference_manifest.json").read_text())
    opening_reference_contract_from_manifest(manifest, decision_time)
    if (manifest.get("data_sha256") != sha256_file(path)
            or manifest.get("source_daily_sha256") != sha256_file(daily_path)
            or manifest.get("pair_identity") != entry_manifest.get("pair_identity")):
        raise ValueError("opening reference source/clock/pair identity is incompatible")
    refs = pl.read_parquet(path)
    required = {"date", "physical_contract", "symbol", "decision_ts", "reference_quote", "reference_kind",
                "reference_available_by", "reference_trade_date", "reference_trade_ts", "reference_source_sha256",
                "prior_trade_quote", *OPENING_REFERENCE_FEATURES}
    if required - set(refs.columns):
        raise ValueError("opening reference schema is incomplete")
    hour, minute = map(int, decision_time.split(":"))
    expected_time = pl.col("date").cast(pl.Datetime("us")) + pl.duration(hours=hour, minutes=minute)
    available = pl.col("reference_quote").is_not_null()
    if (refs.select("date", "physical_contract").is_duplicated().any()
            or refs.height != manifest.get("rows")
            or refs.filter(pl.col("decision_ts").is_null() | (pl.col("decision_ts") != expected_time)).height
            or refs.filter(pl.col("reference_available_by") > pl.col("decision_ts")).height
            or refs.filter(available & (~pl.col("reference_quote").is_finite() | (pl.col("reference_quote") <= 0)
                | pl.col("reference_available_by").is_null() | pl.col("reference_trade_date").is_null()
                | (pl.col("reference_trade_date") > pl.col("date")) | pl.col("reference_source_sha256").is_null()
                | (pl.col("reference_source_sha256").str.len_chars() != 64)
                | (pl.col("reference_trade_ts") > pl.col("reference_available_by")))).height
            or refs.filter(~pl.col("reference_kind").is_in(
                ["observed_session_open", "last_observed_trade", "unavailable"])
                | ((pl.col("reference_kind") == "unavailable") != ~available)
                | ((pl.col("reference_kind") == "observed_session_open") &
                   (pl.col("reference_trade_date") != pl.col("date")))
                | (pl.col(OPENING_REFERENCE_FEATURES[1]) != available.cast(pl.Float64))).height
            or not refs.select(OPENING_REFERENCE_FEATURES).to_numpy().shape[0]
            or not np.isfinite(refs.select(OPENING_REFERENCE_FEATURES).to_numpy()).all()):
        raise ValueError("opening reference has invalid prices/features or future observations")
    return refs, manifest


def _validate_partial_observation_source(directory, manifest, entries, expected):
    """Bind every partial execution to the immutable diagnostic and readback."""
    item = manifest.get('partial_observations', {})
    names = {'data_file': 'partial_observations.parquet',
             'manifest_file': 'partial_observations_manifest.json',
             'readback_file': 'partial_observations_readback.json'}
    paths = {}
    for field, name in names.items():
        if item.get(field) != name:
            raise ValueError('partial observations require local immutable evidence')
        member = Path(directory) / name
        if member.is_symlink() or not member.is_file() or sha256_file(member) != item.get(field.replace('_file', '_sha256')):
            raise ValueError('partial observation evidence is missing or changed')
        paths[field] = member
    diagnostic = json.loads(paths['manifest_file'].read_text())
    readback = json.loads(paths['readback_file'].read_text())
    source = diagnostic.get('sources', {})
    if (diagnostic.get('contract') != 'tw_futures_partial_window_observations_diagnostic_v1'
            or diagnostic.get('research_only') is not True
            or diagnostic.get('training_ready') is not False
            or diagnostic.get('source_coverage_complete') is not False
            or diagnostic.get('candidate_file', {}).get('sha256') != item['data_sha256']
            or source.get('daily_sha256') != manifest['source_daily_sha256']
            or source.get('rules_sha256') != manifest['source_rules_sha256']
            or source.get('quarantine_sha256') != manifest['execution_quarantine']['sha256']
            or readback.get('contract') != 'tw_futures_partial_window_independent_arithmetic_v1'
            or readback.get('independent_implementation') is not True
            or readback.get('candidate_sha256') != item['data_sha256']
            or readback.get('diagnostic_manifest_sha256') != item['manifest_sha256']
            or readback.get('rows_checked') != diagnostic.get('candidate_rows')):
        raise ValueError('partial observation diagnostic or independent arithmetic is incompatible')
    candidates = pl.read_parquet(paths['data_file'])
    required = {'date', 'physical_contract', 'window_minutes', 'decision_time',
                'entry_quote', 'entry_volume', 'source_file_sha256'}
    if (required - set(candidates.columns)
            or candidates.select('date', 'physical_contract', 'window_minutes', 'decision_time').is_duplicated().any()):
        raise ValueError('invalid partial observation candidate identities')
    columns = ['date', 'physical_contract', 'entry_quote', 'entry_volume', 'source_file_sha256']
    observed = candidates.filter((pl.col('window_minutes') == expected.get('window_minutes', 1))
        & (pl.col('decision_time') == expected['decision_time'])).select(columns).sort(columns[:2])
    used = entries.filter(pl.col('status') == 'partial_observed_minute').select(columns).sort(columns[:2])
    if not observed.equals(used):
        raise ValueError('partial clock rows differ from the source-bound observed candidates')
    if item.get('contract_days') != used.height:
        raise ValueError('partial observed entry count differs')


def validate_clock_source(path, *, daily_path, rules_path, decision_time):
    path = Path(path)
    manifest_path = path.with_name("manifest.json")
    manifest = json.loads(manifest_path.read_text())
    expected = clock_contract_from_manifest(manifest, decision_time)
    scope = expected.get("asset_scope", "stock_only")
    options = dict(window_minutes=expected.get("window_minutes", 1),
                   quarantine_unknown=expected.get("quarantine_unknown_execution", False),
                   allow_partial_observations=expected.get('allow_partial_observations', False))
    if (manifest.get("entries_sha256") != sha256_file(path)
            or manifest.get("source_daily_sha256") != sha256_file(daily_path)
            or manifest.get("source_rules_sha256") != sha256_file(rules_path)):
        raise ValueError("futures entry clock/source identity is incomplete or incompatible")
    entries = pl.read_parquet(path)
    required = {"date", "physical_contract", "symbol", "entry_quote", "entry_volume", "status"}
    if required - set(entries.columns) or entries.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("invalid futures entry clock schema or duplicate physical contract-day")
    accepted = ACCEPTED_STATUSES | ({"unverified_execution_quarantined"} if options["quarantine_unknown"] else set())
    if options['allow_partial_observations']:
        accepted = accepted | {'partial_observed_minute'}
    if entries.filter(~pl.col("status").is_in(accepted) | pl.col("status").is_null()).height:
        raise ValueError("unknown entry coverage cannot become a no-trade observation")
    if options["quarantine_unknown"]:
        item = manifest.get("execution_quarantine", {})
        if item.get('file') != 'execution_quarantine.json':
            raise ValueError('execution quarantine must be a local immutable receipt')
        quarantine_path = path.parent / item.get("file", "")
        if not quarantine_path.is_file() or sha256_file(quarantine_path) != item.get("sha256"):
            raise ValueError("execution quarantine receipt is missing or changed")
        unknown_statuses = ['unverified_execution_quarantined']
        if options['allow_partial_observations']:
            unknown_statuses.append('partial_observed_minute')
        unknown = entries.filter(pl.col('status').is_in(unknown_statuses))
        validate_execution_quarantine(json.loads(quarantine_path.read_text()), unknown,
            daily_sha256=manifest["source_daily_sha256"], rules_sha256=manifest["source_rules_sha256"])
        count = unknown.height
        if (manifest.get('coverage_gaps') != count or item.get('contract_days') != count
                or manifest.get('source_coverage_complete') is not False):
            raise ValueError('quarantined execution must retain its unknown source coverage')
    positive_statuses = ['observed_minute']
    if options['allow_partial_observations']:
        _validate_partial_observation_source(path.parent, manifest, entries, expected)
        positive_statuses.append('partial_observed_minute')
    observed = entries.filter(pl.col('status').is_in(positive_statuses))
    if observed.filter(pl.col("entry_quote").is_null() | ~pl.col("entry_quote").is_finite()
            | (pl.col("entry_quote") <= 0) | pl.col("entry_volume").is_null()
            | ~pl.col("entry_volume").is_finite() | (pl.col("entry_volume") <= 0)
            | (pl.col("entry_volume") != pl.col("entry_volume").floor())).height:
        raise ValueError("observed clock entries require finite positive prices and whole-contract volume")
    if entries.filter(~pl.col('status').is_in(positive_statuses)
            & (pl.col("entry_quote").is_not_null() | (pl.col("entry_volume") != 0))).height:
        raise ValueError("non-executable clock rows cannot contain a synthetic fill")
    if (entries.height != manifest.get("rows") or not entries.height
            or str(entries["date"].min()) != manifest.get("start_date")
            or str(entries["date"].max()) != manifest.get("end_date")):
        raise ValueError("entry clock horizon/count differs from its receipt")
    if scope == "all_twd":
        load_entry_specifications(path.parent, manifest)
    return entries, manifest


def load_entry_specifications(directory, manifest):
    """Read the exact dated pricing specification carried by a v5 clock."""
    from stockagent.data.tw_futures_margin_release import read_bound_output
    directory = Path(directory)
    item = manifest.get("entry_specifications", {})
    if item.get("directory") != "entry_specifications":
        raise ValueError("all-TWD entry specifications require an immutable local source")
    root = directory / item["directory"]
    data, receipt = root / "specifications.parquet", root / "manifest.json"
    if (root.is_symlink() or data.is_symlink() or receipt.is_symlink()
            or sha256_file(receipt) != item.get("manifest_sha256")
            or sha256_file(data) != item.get("data_sha256")):
        raise ValueError("all-TWD entry specifications changed")
    specs, proof = read_bound_output(data)
    if proof.get("dataset") != "taifex_dated_product_specifications":
        raise ValueError("all-TWD entries require dated product specifications")
    return specs


def bind_entry_specifications(source, specifications):
    """Bind once per product/date; retain product-specific value, tax and clocks."""
    from stockagent.data.tw_futures_execution_terms import bind_product_specifications
    columns = ["specification_bound", "spec_contract_multiplier", "spec_margin_value_kind",
        "spec_tax_value_kind", "spec_tax_fixed_value_twd", "spec_tax_class",
        "spec_opening_time", "spec_settlement_time", "spec_expiry_time",
        "spec_tick_kind", "spec_tick_size", "spec_final_cash_accounting_policy"]
    if set(columns) & set(source.columns):
        raise ValueError("entry specifications must have one authoritative binding")
    bound = bind_product_specifications(source.select("date", "product"), specifications)
    if not bound["specification_bound"].all():
        raise ValueError("all-TWD entry has an unannounced, ended or missing specification")
    return source.join(bound.select("date", "product", *columns),
                       on=["date", "product"], how="left", validate="m:1", maintain_order="left")


def dated_entry_sessions(source, *, unknown_final_mode="intersection"):
    """Return a dated physical-contract session or a full-book audit envelope.

    With no independently established final date, the common part of the
    regular and final-day sessions is the only safe envelope. The caller must
    still reconcile its complete observed prints with the official daily book;
    a print missing outside this envelope is not an inferred zero-volume day.

    An authenticated exchange-wide original may instead use the union of the
    possible sessions solely as an audit candidate. It is not a proven session:
    the entire original book must subsequently match independent official
    day-session volume and high/low/close before any print is admitted.
    """
    if unknown_final_mode not in {"intersection", "union_for_full_book_verification"}:
        raise ValueError("unsupported unknown-final session mode")
    required={"date","product","contract","physical_contract","official_expiry",
              "specification_bound","spec_opening_time","spec_settlement_time","spec_expiry_time",
              "spec_final_cash_accounting_policy"}
    if required-set(source.columns) or not source["specification_bound"].fill_null(False).all():
        raise ValueError("dated source sessions require bound physical-contract specifications")
    if source.filter(pl.col("date").is_null() | pl.col("spec_opening_time").is_null()
            | pl.col("spec_settlement_time").is_null() | pl.col("spec_expiry_time").is_null()).height:
        raise ValueError("dated source session has an unknown date or session boundary")
    if source.filter(pl.col("spec_final_cash_accounting_policy").is_null()
            | ~pl.col("spec_final_cash_accounting_policy").is_in(
                ["expiry_event_time", "official_final_day_end_v1"])).height:
        raise ValueError("dated source session has an unknown final-day role")
    if source.filter(pl.col("official_expiry").is_not_null()
                     & (pl.col("date") > pl.col("official_expiry"))).height:
        raise ValueError("dated source session occurs after its recorded final date")
    # Explicit parsing also handles empty typed frames without inferring a
    # timestamp format from nonexistent observations.
    start = pl.col("spec_opening_time").str.to_time(format="%H:%M:%S")
    regular = pl.col("spec_settlement_time").str.to_time(format="%H:%M:%S")
    expiry = pl.col("spec_expiry_time").str.to_time(format="%H:%M:%S")
    # official_expiry is the recorded final-settlement date. For contracts
    # with next-day cash settlement it must not be mistaken for the last
    # trading date; until that date has separate evidence, use common hours.
    unknown_end = (pl.min_horizontal(regular, expiry) if unknown_final_mode == "intersection"
                   else pl.max_horizontal(regular, expiry))
    end = (pl.when(pl.col("official_expiry").is_null()
                   | (pl.col("spec_final_cash_accounting_policy") == "official_final_day_end_v1"))
           .then(unknown_end)
           .when(pl.col("date") == pl.col("official_expiry")).then(expiry)
           .otherwise(regular))
    result=source.select("date","product","contract","physical_contract",
        (start.cast(pl.Int64)//1_000_000_000).alias("_start"),
        (end.cast(pl.Int64)//1_000_000_000).alias("_end"))
    if result.select("date","physical_contract").is_duplicated().any() or result.filter(
            pl.col("_start").is_null()|pl.col("_end").is_null()|(pl.col("_start")>=pl.col("_end"))).height:
        raise ValueError("invalid or duplicate dated regular-session envelope")
    return result


def reprice_margin_entries(execution, source: pl.DataFrame, *, participation, broker_multiplier,
                           asset_scope="stock_only", capacity_policy="previous_session",
                           allow_partial_observations=False):
    """Change entry facts only; preserve prior, end, terminal and corporate state.

    `source` is aligned to the flattened execution rows. Dated stock-futures
    contract values are quote * shares + deliverable cash; repricing the quote
    must not discard that cash. Unsupported value bases fail rather than guess.

    A single-contract probe is an ex-ante order allowance, independent of today's
    outcomes. Its fill is still capped by the observed post-decision print
    volume. It does not round fractional participation up, invent an execution,
    change terminal capacity or discard existing inventory.
    """
    classes = clock_asset_classes(asset_scope)
    if capacity_policy not in ENTRY_CAPACITY_POLICIES:
        raise ValueError("unsupported futures entry capacity policy")
    if (asset_scope != "stock_only" and "asset_class" not in source.columns
            or "asset_class" in source.columns and source.filter(
                pl.col("asset_class").is_null() | ~pl.col("asset_class").is_in(classes)).height):
        raise ValueError("entry repricing requires supported dated asset classes")
    if not 0 <= participation <= 1 or broker_multiplier <= 0:
        raise ValueError("invalid execution participation or broker multiplier")
    ex = np.asarray(execution, dtype=np.float32).copy()
    if ex.ndim != 2 or ex.shape[0] != source.height or ex.shape[1] < 33:
        raise ValueError("decision clock requires aligned corporate whole-contract margin execution")
    get = lambda name: source[name].to_numpy().astype(np.float64)
    if (not isinstance(allow_partial_observations, bool)
            or allow_partial_observations and asset_scope != 'all_twd'):
        raise ValueError('partial observations require the explicit all-TWD research clock')
    statuses = source['status'].to_numpy()
    if not allow_partial_observations and np.any(statuses == 'partial_observed_minute'):
        raise ValueError('partial observations require the explicit research clock')
    observed = (statuses == 'observed_minute') | (
        (statuses == 'partial_observed_minute') & allow_partial_observations)
    quote, old_quote, multiplier = get("entry_quote"), get("open"), get("contract_multiplier")
    full, known = get("opening_contract_value_twd"), get("known_margin_contract_value_twd")
    base = get("opening_margin_value_twd")
    dated = asset_scope == "all_twd"
    if dated:
        required = {"specification_bound", "spec_contract_multiplier", "spec_margin_value_kind",
            "spec_tax_value_kind", "spec_tax_fixed_value_twd", "spec_tax_class"}
        if required-set(source.columns) or not source["specification_bound"].fill_null(False).all():
            raise ValueError("all-TWD repricing requires bound dated specifications")
        kinds = source.select("spec_margin_value_kind", "spec_tax_value_kind")
        if kinds.select(pl.any_horizontal(~pl.col(c).is_in(["quoted_value","full_value","face_value"])
                | pl.col(c).is_null() for c in kinds.columns).any()).item():
            raise ValueError("unsupported dated entry value basis")
        fixed = get("spec_tax_fixed_value_twd")
        needs_fixed = source.select(pl.any_horizontal(pl.col(c)=="face_value" for c in kinds.columns)).to_series().to_numpy()
        if np.any(needs_fixed & (~np.isfinite(fixed) | (fixed<=0))):
            raise ValueError("face-value entry requires its positive dated nominal amount")
        def dated_base(kind, quoted, total):
            return np.select([kind=="quoted_value",kind=="full_value"], [quoted,total], default=fixed)
        margin_kind = source["spec_margin_value_kind"].to_numpy()
        expected_base = dated_base(margin_kind, old_quote*multiplier, full)
        ordinary = ~source["asset_class"].is_in(["stock_future","etf_future"]).to_numpy()
        if (np.any(observed & ~np.isclose(base,expected_base,atol=.005,rtol=0))
                or np.any(ordinary & ~np.isclose(multiplier,get("spec_contract_multiplier"),atol=1e-8,rtol=0))):
            raise ValueError("dated entry specification disagrees with financial value units")
    if (np.any(~np.isfinite(multiplier) | (multiplier <= 0))
            or np.any(observed & (~np.isfinite(old_quote) | (old_quote <= 0)))
            or (not dated and np.any(observed & ~(np.isclose(base, old_quote * multiplier, atol=.005, rtol=0)
                                  | np.isclose(base, full, atol=.005, rtol=0))))):
        raise ValueError("unsupported dated linear futures entry value/margin base")
    # For no observed trade, the previous official value is a valuation only.
    # There is no entry capacity and no imputed executable price.
    delta = np.where(observed, (quote - old_quote) * multiplier, 0.)
    value = np.where(observed, full + delta, known)
    entry_base = dated_base(margin_kind,quote*multiplier,full+delta) if dated else base+delta
    margin_base = np.where(observed, entry_base, get("known_margin_value_twd"))
    if np.any(~np.isfinite(value) | (value <= 0) | ~np.isfinite(margin_base) | (margin_base <= 0)):
        raise ValueError("clock valuation requires positive dated prior/entry values")
    tax_classes = {"stock_future": "stock", "etf_future": "etf", "index_future": "stock_index"}
    assets = source["asset_class"].to_list() if "asset_class" in source.columns else ["stock_future"] * source.height
    if dated:
        # Resolve the date schedule once per product tax class and date; a wide
        # contract tape must not call a Python tax function for every row.
        tax_rows=source.select("date",pl.col("spec_tax_class").alias("tax_class"))
        unique_tax_days=tax_rows.unique()
        schedule=unique_tax_days.with_columns(pl.Series("_rate",[
            futures_transaction_tax_rate(d,tax_class=c) for d,c in unique_tax_days.iter_rows()]))
        rate=tax_rows.join(schedule,on=["date","tax_class"],how="left",validate="m:1",maintain_order="left")["_rate"].to_numpy()
        tax_kind=source["spec_tax_value_kind"].to_numpy()
        old_tax_base=dated_base(tax_kind,old_quote*multiplier,full)
        entry_tax_base=dated_base(tax_kind,quote*multiplier,full+delta)
    else:
        rate = np.asarray([futures_transaction_tax_rate(d, tax_class=tax_classes[a])
                           for d, a in zip(source["date"], assets)])
        old_tax_base,entry_tax_base=old_quote*multiplier,quote*multiplier
    old_tax = np.floor(old_tax_base * rate + .5)
    if np.any(observed & ~np.isclose(get("opening_tax_twd"), old_tax, atol=.005, rtol=0)):
        raise ValueError("unsupported futures tax base; explicit dated tax required")
    capacity = np.floor(np.nan_to_num(get("entry_volume"), nan=0.) * participation)
    order_capacity = ex[:, 8]
    if capacity_policy == "single_contract_probe":
        if np.any(~np.isfinite(order_capacity) | (order_capacity < 0)):
            raise ValueError("probe orders require finite nonnegative prior capacity")
        order_capacity = np.maximum(order_capacity, 1.)
    ex[:, 8] = np.minimum(order_capacity, np.where(observed, capacity, 0))
    ex[:, 1] *= observed
    ex[:, 3] = value
    ex[:, 0] = np.log(ex[:, 4].astype(np.float64) / value)
    ex[:, 6] = np.where(observed, np.floor(entry_tax_base * rate + .5), get("known_tax_twd"))
    is_rate = source["margin_kind"].to_numpy() == "notional_rate"
    for channel, name in ((11, "initial"), (12, "maintenance")):
        ex[:, channel] = np.floor(np.where(is_rate, get(name) * margin_base, get(name))
                                 * broker_multiplier + .5)
    from stockagent.data.tw_futures_price_limits import price_limit_permissions
    can_buy, can_sell = price_limit_permissions(source, quote)
    ex[:, 18] = observed & can_buy
    ex[:, 19] = observed & can_sell
    return ex


def attach_futures_decision_clock(panel, path, *, decision_time, participation=.5, broker_multiplier=1.,
                                  opening_reference_path=None, capacity_policy="previous_session",
                                  futures_account_calendar=None):
    from stockagent.data.panel import slice_panel_start

    daily = panel.stock_context_futures_portfolio_daily
    if (daily is None or daily.integer_execution is None or daily.intraday_execution is not None
            or daily.margin_rules_path is None):
        raise ValueError("entry clock must follow the canonical corporate margin carry adapter")
    if capacity_policy not in ENTRY_CAPACITY_POLICIES:
        raise ValueError("unsupported futures entry capacity policy")
    if capacity_policy == "single_contract_probe":
        import json
        from stockagent.data.tw_futures_margin_release import WHOLE_CONTRACT_PREFIX_CONTRACT
        daily_manifest = json.loads(Path(daily.manifest_path).read_text())
        if (daily.require_prior_capacity
                or daily_manifest.get("context_only_prefix_contract") == WHOLE_CONTRACT_PREFIX_CONTRACT):
            raise ValueError("probe orders require a source admitted without prior-volume prefix omissions")
    entries, manifest = validate_clock_source(path, daily_path=daily.source_path,
        rules_path=daily.margin_rules_path, decision_time=decision_time)
    start, end = np.datetime64(manifest["start_date"]), np.datetime64(manifest["end_date"])
    dates = np.asarray(panel.dates, dtype="datetime64[D]")
    if dates[-1] != end or start not in dates:
        raise ValueError("paired clock end must equal the benchmark-clipped stock panel end")
    if futures_account_calendar is not None:
        from stockagent.data.tw_futures_account_calendar import FuturesAccountCalendar
        if (not isinstance(futures_account_calendar, FuturesAccountCalendar)
                or not np.array_equal(futures_account_calendar.dates, dates)):
            raise ValueError("entry clock account calendar must match the exact source date axis")
    original_length = len(dates)
    selected = pl.col("date").is_between(date.fromisoformat(manifest["start_date"]),
                                        date.fromisoformat(manifest["end_date"]))
    scope = manifest["clock"].get("asset_scope", "stock_only")
    daily_scan = pl.scan_parquet(daily.source_path)
    if (futures_account_calendar is not None
            and "accounting_only_date" not in daily_scan.collect_schema().names()):
        raise ValueError("entry clock account calendar requires source-backed date flags")
    source = daily_scan.filter(selected).select(
        "date", "physical_contract", "symbol", "open", "contract_multiplier", "asset_class",
        *(["product"] if scope=="all_twd" else []),
        *(["accounting_only_date"] if futures_account_calendar is not None else [])).collect()
    if source.filter(pl.col("asset_class").is_null()
            | ~pl.col("asset_class").is_in(clock_asset_classes(scope))).height:
        raise ValueError("decision-clock source contains assets outside its declared scope")
    rules = pl.scan_parquet(daily.margin_rules_path).filter(selected).collect()
    # Both canonical releases own the units; don't let a join silently suffix
    # and substitute a different multiplier.
    source = source.join(rules.drop("contract_multiplier"), on=["date", "physical_contract"],
                         how="left", validate="1:1")
    source = source.join(entries.drop("symbol"),
                         on=["date", "physical_contract"], how="left", validate="1:1")
    if scope=="all_twd":
        source=bind_entry_specifications(source,load_entry_specifications(Path(path).parent,manifest))
    # A separately checked symbol join prevents physical-contract/slot aliasing.
    if not entries.select("date", "physical_contract", "symbol").sort("date", "physical_contract").equals(
            source.select("date", "physical_contract", "symbol").sort("date", "physical_contract")):
        raise ValueError("entry clock must cover exactly the dated physical-contract action axis")
    di = np.searchsorted(dates, source["date"].to_numpy().astype("datetime64[D]"))
    if (np.any(di >= len(dates))
            or not np.array_equal(dates[di], source["date"].to_numpy().astype("datetime64[D]"))):
        raise ValueError("entry clock source contains a date outside the exact account axis")
    si = source["symbol"].str.extract(r"(\d+)$").cast(pl.Int64).to_numpy() - 1
    account_only = (np.zeros(len(di), dtype=bool) if futures_account_calendar is None
                    else futures_account_calendar.account_only_mask[di])
    if futures_account_calendar is not None and (
            source.schema["accounting_only_date"] != pl.Boolean
            or source["accounting_only_date"].null_count()
            or not np.array_equal(source["accounting_only_date"].to_numpy(), account_only)):
        raise ValueError("entry clock account calendar conflicts with source date flags")
    if np.any(account_only):
        account_rows = source.filter(pl.Series(account_only))
        account_execution = daily.integer_execution[di[account_only], si[account_only]]
        capacity = account_execution[:, [1, 8]]
        if (account_rows.filter(pl.col("status") != "daily_not_executable").height
                or np.any(~np.isfinite(capacity)) or np.any(capacity != 0)):
            raise ValueError("account-only dates cannot contain an executable entry clock")
    active = ~account_only & (daily.integer_execution[di, si, 3] > 0)
    accepted = ACCEPTED_STATUSES | ({"unverified_execution_quarantined"}
        if manifest["clock"].get("quarantine_unknown_execution") else set())
    partial_observations = manifest['clock'].get('allow_partial_observations', False)
    if partial_observations:
        accepted = accepted | {'partial_observed_minute'}
    if source.filter(pl.Series(active) & ~pl.col("status").is_in(accepted)).height:
        raise ValueError("active carrying contracts have missing clock evidence")
    ex = daily.integer_execution.copy()
    ex[di[active], si[active]] = reprice_margin_entries(ex[di[active], si[active]],
        source.filter(pl.Series(active)), participation=participation, broker_multiplier=broker_multiplier,
        asset_scope=scope, capacity_policy=capacity_policy,
        allow_partial_observations=partial_observations)
    open_prices = daily.open_prices.copy()
    # No executable quote stays missing in reports, even though the ledger
    # uses a separately labelled official mark for carrying valuation.
    open_prices[di[~account_only], si[~account_only]] = (
        source.filter(pl.Series(~account_only))["entry_quote"].to_numpy()
    )
    candidate_features = daily.candidate_features
    clock_metadata = {**manifest["clock"], "entries_sha256": manifest["entries_sha256"],
                      "pair_identity": manifest["pair_identity"], "source_path": str(path)}
    if scope=="all_twd":
        clock_metadata["entry_specifications"]=manifest["entry_specifications"]
    if capacity_policy == "single_contract_probe":
        clock_metadata.update(runtime_capacity_contract="causal_single_contract_probe_v1",
            capacity="min_max_prior_session_capacity_one_floor_observed_window_volume_times_participation")
    if opening_reference_path is not None:
        refs, ref_manifest = validate_opening_reference_source(opening_reference_path,
            daily_path=daily.source_path, decision_time=decision_time, entry_manifest=manifest)
        axis = ["date", "physical_contract", "symbol"]
        if not refs.select(axis).sort(axis[:2]).equals(source.select(axis).sort(axis[:2])):
            raise ValueError("opening reference must cover the exact physical action axis")
        aligned = source.select(axis).join(refs, on=axis, how="left", validate="1:1")
        extra = np.zeros((*daily.candidate_mask.shape, len(OPENING_REFERENCE_FEATURES)), dtype=np.float32)
        extra[di, si] = aligned.select(OPENING_REFERENCE_FEATURES).to_numpy().astype(np.float32)
        candidate_features = np.concatenate((candidate_features, extra), axis=-1)
        clock_metadata["opening_reference"] = {**ref_manifest["contract"],
            "source_path": str(opening_reference_path), "data_sha256": ref_manifest["data_sha256"]}
    new_daily = replace(daily, integer_execution=ex, holding_log_returns=ex[..., 0],
        executable_mask=ex[..., 1].astype(bool), open_prices=open_prices,
        candidate_features=candidate_features, decision_clock_contract=clock_metadata)
    # Build full prior/corporate context first, then crop the shared research
    # horizon. This keeps source-calendar validation and factorized ABI intact.
    slc = slice(int(np.searchsorted(dates, start)), None)
    updates = {f.name: getattr(new_daily, f.name)[slc] for f in fields(new_daily)
               if isinstance(getattr(new_daily, f.name), np.ndarray)
               and getattr(new_daily, f.name).ndim > 0
               and len(getattr(new_daily, f.name)) == original_length}
    panel = slice_panel_start(panel, str(start))
    return replace(panel, stock_context_futures_portfolio_daily=replace(new_daily, **updates))
