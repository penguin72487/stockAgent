"""Dated TAIFEX rules for the existing whole-contract futures account."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate


MARGIN_CONTRACT_VERSION = 1
# The first eleven channels retain the canonical integer account ABI.
INITIAL, MAINTENANCE, END_INITIAL, END_MAINTENANCE = range(11, 15)
PREVIOUS_MARK, PREVIOUS_INITIAL, PREVIOUS_MAINTENANCE = range(15, 18)
CAN_BUY, CAN_SELL, POSITION_GROUP, POSITION_UNIT, POSITION_LIMIT = range(18, 23)
LIQUIDATION_RATIO, TERMINAL_MARK, TERMINAL_CAPACITY = range(23, 26)
TERMINAL_CAN_BUY, TERMINAL_CAN_SELL, CASH_SETTLEMENT = range(26, 29)
MARGIN_EXECUTION_WIDTH = 29
MARGIN_FEATURE_COLUMNS = (
    "known_initial_margin_to_prior_notional",
    "known_maintenance_to_initial_margin",
)
MARGIN_AUDIT_COLUMNS = (
    "equity_before_twd", "equity_at_open_twd", "equity_after_mark_twd",
    "initial_margin_used_twd", "settlement_initial_margin_twd",
    "settlement_maintenance_margin_twd", "free_collateral_twd",
    "gross_notional_to_equity", "settlement_margin_call",
    "opening_forced_liquidation", "unfilled_liquidation_contracts",
    "overnight_pnl_twd",
)


def validate_margin_rule_source(path: str | Path, daily_path: str | Path):
    """Validate a prepared rule release; a current quote is not history."""
    path = Path(path)
    if not path.is_file() or not path.with_name("manifest.json").is_file():
        raise FileNotFoundError(
            f"dated futures margin/position/price-limit rules are missing: {path}; "
            "current TAIFEX tables cannot be backfilled as historical rules"
        )
    manifest = json.loads(path.with_name("manifest.json").read_text())
    if (manifest.get("dataset") != "taifex_futures_margin_rules"
            or manifest.get("schema_version") != MARGIN_CONTRACT_VERSION
            or manifest.get("status") != "complete"
            or manifest.get("point_in_time_verified") is not True):
        raise ValueError("futures margin rule release must be PIT-verified and complete")
    if manifest.get("source_daily_sha256") != sha256_file(Path(daily_path)):
        raise ValueError("futures margin rules belong to a different daily release")
    if manifest.get("outputs", {}).get("rules", {}).get("sha256") != sha256_file(path):
        raise ValueError("futures margin rules SHA-256 mismatch")
    sources = manifest.get("sources", [])
    if not sources:
        raise ValueError("futures margin rules require official source receipts")
    for source in sources:
        relative = Path(source["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe futures rule receipt path")
        if sha256_file(path.parent / relative) != source["sha256"]:
            raise ValueError("futures rule source receipt SHA-256 mismatch")
    rules = pl.read_parquet(path)
    required = {
        "date", "physical_contract", "margin_kind", "initial", "maintenance",
        "settlement_initial", "settlement_maintenance", "known_at", "effective_at",
        "settlement_known_at", "settlement_effective_at", "settlement_time",
        "position_group", "position_unit", "position_limit", "upper_limit", "lower_limit",
    }
    if required - set(rules.columns):
        raise ValueError(f"missing dated futures rules: {sorted(required - set(rules.columns))}")
    if rules.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("duplicate futures margin contract-days")
    for name in ("date", "physical_contract", "margin_kind", "position_group", "settlement_time"):
        if rules[name].null_count() or (rules[name].cast(pl.String).str.len_chars() == 0).any():
            raise ValueError(f"empty futures rule field: {name}")
    for name in ("known_at", "effective_at", "settlement_known_at", "settlement_effective_at"):
        if rules[name].null_count() or not rules[name].str.contains(r"(Z|[+-]\d{2}:\d{2})$").all():
            raise ValueError("futures rule timestamps require an explicit timezone")
    for phase, clock in (("", pl.lit("08:45:00")), ("settlement_", pl.col("settlement_time"))):
        cutoff = (pl.col("date").cast(pl.String) + pl.lit("T") + clock + pl.lit("+08:00")).str.to_datetime(time_zone="UTC")
        for field in ("known_at", "effective_at"):
            if rules.filter(pl.col(phase + field).str.to_datetime(time_zone="UTC") > cutoff).height:
                raise ValueError(f"future/unannounced rules at {phase or 'opening'} clock")
    numeric = ("initial", "maintenance", "settlement_initial", "settlement_maintenance",
               "position_unit", "position_limit", "upper_limit", "lower_limit")
    if rules.select(pl.any_horizontal([~pl.col(c).is_finite() | (pl.col(c) <= 0) | pl.col(c).is_null() for c in numeric]).any()).item():
        raise ValueError("futures margins, limits and units must be finite and positive")
    if rules.filter((pl.col("initial") < pl.col("maintenance"))
                    | (pl.col("settlement_initial") < pl.col("settlement_maintenance"))
                    | (pl.col("upper_limit") <= pl.col("lower_limit"))
                    | ~pl.col("margin_kind").is_in(["fixed_twd", "notional_rate"])).height:
        raise ValueError("invalid margin hierarchy, price limits or margin units")
    if rules.filter((pl.col("margin_kind") == "notional_rate") & (
        (pl.col("initial") > 1) | (pl.col("settlement_initial") > 1)
    )).height:
        raise ValueError("margin ratios must use fractions, not percent integers")
    return rules, manifest


def attach_futures_margin_rules(panel, path, *, broker_multiplier=1.0, liquidation_ratio=0.25, participation=0.5):
    """Attach executor-only marks and prior-observable margin model context."""
    daily = panel.stock_context_futures_portfolio_daily
    if daily is None or daily.integer_execution is None or daily.intraday_execution is not None:
        raise ValueError("margin requires the canonical carrying whole-contract sidecar")
    rules, _ = validate_margin_rule_source(path, daily.source_path)
    frame = pl.read_parquet(daily.source_path, columns=[
        "date", "physical_contract", "symbol", "open", "close", "settlement",
        "previous_settlement", "contract_multiplier", "volume", "liquidation_reason",
    ]).join(rules, on=["date", "physical_contract"], how="left", validate="1:1").sort("physical_contract", "date")
    is_rate = pl.col("margin_kind") == "notional_rate"
    for output, value, price in (
        ("_im", "initial", "open"), ("_mm", "maintenance", "open"),
        ("_end_im", "settlement_initial", "settlement"),
        ("_end_mm", "settlement_maintenance", "settlement"),
        ("_known_im", "initial", "previous_settlement"),
        ("_known_mm", "maintenance", "previous_settlement"),
    ):
        frame = frame.with_columns(((pl.when(is_rate).then(
            pl.col(value) * pl.col(price) * pl.col("contract_multiplier")
        ).otherwise(pl.col(value)) * broker_multiplier + 0.5).floor()).alias(output))
    frame = frame.with_columns(
        pl.col("_end_im").shift(1).over("physical_contract").alias("_prior_im"),
        pl.col("_end_mm").shift(1).over("physical_contract").alias("_prior_mm"),
    ).filter(pl.col("date").is_in(np.asarray(panel.dates, dtype="datetime64[D]").tolist()))
    # Cached stock panels can expose day, millisecond or nanosecond precision;
    # the dated rule contract always uses calendar days, not timestamp strings.
    dates = {str(d): i for i, d in enumerate(np.asarray(panel.dates, dtype="datetime64[D]"))}
    di = np.array([dates[str(d)] for d in frame["date"]], dtype=np.int64)
    si = frame["symbol"].str.extract(r"(\d+)$").cast(pl.Int64).to_numpy() - 1
    base = daily.integer_execution
    active = np.isfinite(base[di, si, 3]) & (base[di, si, 3] > 0)
    missing = frame.filter(pl.Series(active) & pl.col("margin_kind").is_null())
    if missing.height:
        raise ValueError(f"dated margin rules miss {missing.height} active physical contract-days")
    frame = frame.filter(pl.Series(active))
    di, si = di[active], si[active]
    for field in ("settlement", "_im", "_mm", "_end_im", "_end_mm"):
        if frame.filter(pl.col(field).is_null() | ~pl.col(field).is_finite() | (pl.col(field) <= 0)).height:
            raise ValueError(f"margin account missing official positive {field}")
    # Margin and limit grouping spans delivery months and standard/mini codes.
    group_labels = sorted(frame["position_group"].unique().to_list())
    group_ids = {g: i for i, g in enumerate(group_labels)}
    if len(group_ids) > base.shape[1]:
        raise ValueError("margin position groups exceed the fixed action axis")
    if frame.group_by("date", "position_group").agg(pl.col("position_limit").n_unique().alias("n")).filter(pl.col("n") != 1).height:
        raise ValueError("inconsistent limits within one combined product group")
    ex = np.zeros((*base.shape[:2], MARGIN_EXECUTION_WIDTH), dtype=np.float32)
    ex[..., :11] = base
    extra_features = np.zeros((*base.shape[:2], 2), dtype=np.float32)
    session_mask = np.zeros(len(panel.dates), dtype=bool)
    session_mask[di] = True
    def values(name):
        return frame[name].to_numpy().astype(np.float64)
    multiplier = values("contract_multiplier")
    opening, closing = values("open"), values("close")
    settlement = values("settlement") * multiplier
    expiry = np.asarray(frame["liquidation_reason"] == "last_trade_date")
    terminal = np.where(expiry, base[di, si, 4], closing * multiplier)
    mark = np.where(expiry, terminal, settlement)
    ex[di, si, 4] = mark
    ex[di, si, 0] = np.log(mark / base[di, si, 3])
    tax = np.asarray([stock_index_futures_tax_rate(d) for d in frame["date"]])
    ex[di, si, 7] = np.floor(terminal * tax + 0.5)
    packed = np.column_stack((
        values("_im"), values("_mm"), values("_end_im"), values("_end_mm"),
        values("previous_settlement") * multiplier, values("_prior_im"), values("_prior_mm"),
        opening < values("upper_limit"), opening > values("lower_limit"),
        [group_ids[g] for g in frame["position_group"]], values("position_unit"), values("position_limit"),
        np.full(frame.height, liquidation_ratio), terminal,
        np.floor(values("volume") * participation),
        closing < values("upper_limit"), closing > values("lower_limit"), expiry,
    ))
    ex[di, si, 11:] = packed
    prior_notional = values("previous_settlement") * multiplier
    context_valid = np.isfinite(prior_notional) & (prior_notional > 0)
    extra_features[di[context_valid], si[context_valid]] = np.column_stack((
        values("_known_im")[context_valid] / prior_notional[context_valid],
        values("_known_mm")[context_valid] / values("_known_im")[context_valid],
    ))
    candidate_mask = daily.candidate_mask.copy()
    candidate_mask[di, si] &= context_valid
    return replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, integer_execution=ex,
        candidate_features=np.concatenate((daily.candidate_features, extra_features), axis=-1),
        candidate_mask=candidate_mask, holding_log_returns=ex[..., 0],
        margin_rules_path=str(path), margin_contract_version=MARGIN_CONTRACT_VERSION,
        margin_session_mask=session_mask,
        benchmark_log_returns=np.zeros(len(panel.dates), dtype=np.float32),
    ))
