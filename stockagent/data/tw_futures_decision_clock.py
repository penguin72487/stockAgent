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
ACCEPTED_STATUSES = {"observed_minute", "verified_no_window_trade", "daily_not_executable"}
OPENING_REFERENCE_DATASET = "tw_futures_observed_open_or_last_trade_v1"
OPENING_REFERENCE_FEATURES = (
    "opening_reference_log_return_from_prior_trade",
    "opening_reference_available",
    "opening_reference_is_session_open",
    "opening_reference_age_days_log1p",
)


def decision_clock_contract(decision_time: str) -> dict:
    if decision_time not in {"08:45", "09:00"}:
        raise ValueError("supported futures decision times are 08:45 and 09:00")
    hour, minute = map(int, decision_time.split(":"))
    decision = hour * 60 + minute
    return {
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


def opening_reference_contract(decision_time: str) -> dict:
    decision_clock_contract(decision_time)
    return {
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


def resolve_opening_references(decisions, trades, *, decision_time):
    """Causal as-of input selection; never select by a future availability time.

    An event's `available_by` is separate from its original `trade_ts` (nullable
    for date-only daily observations). A pre-open event can be the last trade
    without being a session-opening print. No event is invented for missing
    prices or an incomplete source. Callers own source authentication.
    """
    opening_reference_contract(decision_time)
    keys = ["date", "physical_contract"]
    required = {"physical_contract", "price", "trade_date", "trade_ts", "available_by",
                "source_sha256", "is_session_open"}
    if required - set(trades.columns) or decisions.select(keys).is_duplicated().any():
        raise ValueError("opening references require unique decisions and timed real-trade events")
    if trades.filter(pl.col("price").is_null() | ~pl.col("price").is_finite() | (pl.col("price") <= 0)
            | pl.col("available_by").is_null() | pl.col("trade_date").is_null()
            | pl.col("is_session_open").is_null()
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
    for output, raw in (("reference_quote", "price"), ("reference_trade_date", "trade_date"),
                        ("reference_trade_ts", "trade_ts"), ("reference_available_by", "available_by"),
                        ("reference_source_sha256", "source_sha256")):
        left = left.with_columns(pl.when(has_open).then(pl.col("_open_" + raw))
                                 .otherwise(pl.col("_last_" + raw)).alias(output))
    # The denominator is a real prior-session trade, never a settlement mark.
    prior_left = decisions.with_columns(pl.col("date").cast(pl.Datetime("us")).alias("_prior_cutoff"))
    prior_left = prior_left.sort("_prior_cutoff").join_asof(prior.sort("_last_available_by"),
        left_on="_prior_cutoff", right_on="_last_available_by", by="physical_contract",
        strategy="backward", allow_exact_matches=False, check_sortedness=False)
    left = left.join(prior_left.select(*keys, pl.col("_last_price").alias("prior_trade_quote")),
                     on=keys, how="left", validate="1:1")
    left = left.with_columns(
        pl.when(has_open).then(pl.lit("observed_session_open"))
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
    if (manifest.get("dataset") != OPENING_REFERENCE_DATASET or manifest.get("status") != "complete"
            or manifest.get("contract") != opening_reference_contract(decision_time)
            or manifest.get("data_sha256") != sha256_file(path)
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


def validate_clock_source(path, *, daily_path, rules_path, decision_time):
    path = Path(path)
    manifest_path = path.with_name("manifest.json")
    manifest = json.loads(manifest_path.read_text())
    expected = decision_clock_contract(decision_time)
    if (manifest.get("dataset") != CLOCK_DATASET or manifest.get("clock") != expected
            or manifest.get("status") != "complete"
            or manifest.get("entries_sha256") != sha256_file(path)
            or manifest.get("source_daily_sha256") != sha256_file(daily_path)
            or manifest.get("source_rules_sha256") != sha256_file(rules_path)):
        raise ValueError("futures entry clock/source identity is incomplete or incompatible")
    entries = pl.read_parquet(path)
    required = {"date", "physical_contract", "symbol", "entry_quote", "entry_volume", "status"}
    if required - set(entries.columns) or entries.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("invalid futures entry clock schema or duplicate physical contract-day")
    if entries.filter(~pl.col("status").is_in(ACCEPTED_STATUSES) | pl.col("status").is_null()).height:
        raise ValueError("unknown entry coverage cannot become a no-trade observation")
    observed = entries.filter(pl.col("status") == "observed_minute")
    if observed.filter(pl.col("entry_quote").is_null() | ~pl.col("entry_quote").is_finite()
            | (pl.col("entry_quote") <= 0) | pl.col("entry_volume").is_null()
            | ~pl.col("entry_volume").is_finite() | (pl.col("entry_volume") <= 0)
            | (pl.col("entry_volume") != pl.col("entry_volume").floor())).height:
        raise ValueError("observed clock entries require finite positive prices and whole-contract volume")
    if entries.filter((pl.col("status") != "observed_minute")
            & (pl.col("entry_quote").is_not_null() | (pl.col("entry_volume") != 0))).height:
        raise ValueError("non-executable clock rows cannot contain a synthetic fill")
    if (entries.height != manifest.get("rows") or not entries.height
            or str(entries["date"].min()) != manifest.get("start_date")
            or str(entries["date"].max()) != manifest.get("end_date")):
        raise ValueError("entry clock horizon/count differs from its receipt")
    return entries, manifest


def reprice_margin_entries(execution, source: pl.DataFrame, *, participation, broker_multiplier):
    """Change entry facts only; preserve prior, end, terminal and corporate state.

    `source` is aligned to the flattened execution rows. Dated stock-futures
    contract values are quote * shares + deliverable cash; repricing the quote
    must not discard that cash. Unsupported value bases fail rather than guess.
    """
    if not 0 <= participation <= 1 or broker_multiplier <= 0:
        raise ValueError("invalid execution participation or broker multiplier")
    ex = np.asarray(execution, dtype=np.float32).copy()
    if ex.ndim != 2 or ex.shape[0] != source.height or ex.shape[1] < 33:
        raise ValueError("decision clock requires aligned corporate whole-contract margin execution")
    get = lambda name: source[name].to_numpy().astype(np.float64)
    observed = source["status"].to_numpy() == "observed_minute"
    quote, old_quote, multiplier = get("entry_quote"), get("open"), get("contract_multiplier")
    full, known = get("opening_contract_value_twd"), get("known_margin_contract_value_twd")
    base = get("opening_margin_value_twd")
    if (np.any(~np.isfinite(multiplier) | (multiplier <= 0))
            or np.any(observed & (~np.isfinite(old_quote) | (old_quote <= 0)))
            or np.any(observed & ~(np.isclose(base, old_quote * multiplier, atol=.005, rtol=0)
                                  | np.isclose(base, full, atol=.005, rtol=0)))):
        raise ValueError("unsupported dated stock-futures entry value/margin base")
    # For no observed trade, the previous official value is a valuation only.
    # There is no entry capacity and no imputed executable price.
    delta = np.where(observed, (quote - old_quote) * multiplier, 0.)
    value = np.where(observed, full + delta, known)
    margin_base = np.where(observed, base + delta, get("known_margin_value_twd"))
    if np.any(~np.isfinite(value) | (value <= 0) | ~np.isfinite(margin_base) | (margin_base <= 0)):
        raise ValueError("clock valuation requires positive dated prior/entry values")
    rate = np.asarray([futures_transaction_tax_rate(d, tax_class="stock") for d in source["date"]])
    old_tax = np.floor(old_quote * multiplier * rate + .5)
    if np.any(observed & ~np.isclose(get("opening_tax_twd"), old_tax, atol=.005, rtol=0)):
        raise ValueError("unsupported stock-futures tax base; explicit dated quote tax required")
    capacity = np.floor(np.nan_to_num(get("entry_volume"), nan=0.) * participation)
    ex[:, 8] = np.minimum(ex[:, 8], np.where(observed, capacity, 0))
    ex[:, 1] *= observed
    ex[:, 3] = value
    ex[:, 0] = np.log(ex[:, 4].astype(np.float64) / value)
    ex[:, 6] = np.where(observed, np.floor(quote * multiplier * rate + .5), get("known_tax_twd"))
    is_rate = source["margin_kind"].to_numpy() == "notional_rate"
    for channel, name in ((11, "initial"), (12, "maintenance")):
        ex[:, channel] = np.floor(np.where(is_rate, get(name) * margin_base, get(name))
                                 * broker_multiplier + .5)
    ex[:, 18] = observed & (quote < get("upper_limit"))
    ex[:, 19] = observed & (quote > get("lower_limit"))
    return ex


def attach_futures_decision_clock(panel, path, *, decision_time, participation=.5, broker_multiplier=1.,
                                  opening_reference_path=None):
    from stockagent.data.panel import slice_panel_start

    daily = panel.stock_context_futures_portfolio_daily
    if (daily is None or daily.integer_execution is None or daily.intraday_execution is not None
            or daily.margin_rules_path is None):
        raise ValueError("entry clock must follow the canonical corporate margin carry adapter")
    entries, manifest = validate_clock_source(path, daily_path=daily.source_path,
        rules_path=daily.margin_rules_path, decision_time=decision_time)
    start, end = np.datetime64(manifest["start_date"]), np.datetime64(manifest["end_date"])
    dates = np.asarray(panel.dates, dtype="datetime64[D]")
    if dates[-1] != end or start not in dates:
        raise ValueError("paired clock end must equal the benchmark-clipped stock panel end")
    original_length = len(dates)
    selected = pl.col("date").is_between(date.fromisoformat(manifest["start_date"]),
                                        date.fromisoformat(manifest["end_date"]))
    source = pl.scan_parquet(daily.source_path).filter(selected).select(
        "date", "physical_contract", "symbol", "open", "contract_multiplier", "asset_class").collect()
    if source.filter(pl.col("asset_class") != "stock_future").height:
        raise ValueError("decision-clock v1 is scoped to dated stock futures")
    rules = pl.scan_parquet(daily.margin_rules_path).filter(selected).collect()
    # Both canonical releases own the units; don't let a join silently suffix
    # and substitute a different multiplier.
    source = source.join(rules.drop("contract_multiplier"), on=["date", "physical_contract"],
                         how="left", validate="1:1")
    source = source.join(entries.drop("symbol"),
                         on=["date", "physical_contract"], how="left", validate="1:1")
    # A separately checked symbol join prevents physical-contract/slot aliasing.
    if not entries.select("date", "physical_contract", "symbol").sort("date", "physical_contract").equals(
            source.select("date", "physical_contract", "symbol").sort("date", "physical_contract")):
        raise ValueError("entry clock must cover exactly the dated physical-contract action axis")
    di = np.searchsorted(dates, source["date"].to_numpy().astype("datetime64[D]"))
    si = source["symbol"].str.extract(r"(\d+)$").cast(pl.Int64).to_numpy() - 1
    active = daily.integer_execution[di, si, 3] > 0
    if source.filter(pl.Series(active) & ~pl.col("status").is_in(ACCEPTED_STATUSES)).height:
        raise ValueError("active carrying contracts have missing clock evidence")
    ex = daily.integer_execution.copy()
    ex[di[active], si[active]] = reprice_margin_entries(ex[di[active], si[active]],
        source.filter(pl.Series(active)), participation=participation, broker_multiplier=broker_multiplier)
    open_prices = daily.open_prices.copy()
    # No executable quote stays missing in reports, even though the ledger
    # uses a separately labelled official mark for carrying valuation.
    open_prices[di, si] = source["entry_quote"].to_numpy()
    candidate_features = daily.candidate_features
    clock_metadata = {**manifest["clock"], "entries_sha256": manifest["entries_sha256"],
                      "pair_identity": manifest["pair_identity"], "source_path": str(path)}
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
