"""Compile dated product rules into the canonical futures margin account tape.

This module owns preparation, not a second ledger.  Missing rule inputs remain
missing and produce a contract-day worklist.  Only a complete tape can be
published by :mod:`tw_futures_margin_release`.
"""
from __future__ import annotations

import numpy as np
import polars as pl

from stockagent.data.tw_futures_margin_preparation import bind_dated_corporate_terms
from stockagent.data.tw_price_rules import tick_size_numpy
from stockagent.research.taifex_transaction_tax import futures_transaction_tax_rate


EXECUTION_TERMS_COMPILER_VERSION = 8
KEYS = ["date", "physical_contract"]
QUOTE_KEYS = ["date", "product", "contract"]
POSITION_INPUT_FIELDS = ["position_unit", "position_limit", "monthly_position_limit", "position_root_product",
    "position_numeric_inputs_resolved", "known_at", "effective_date", "effective_phase"]
POSITION_INPUT_EXTENSIONS = {"independent_contract_limit": pl.Float64, "unit": pl.String}
MARGIN_INPUT_FIELDS = [prefix+c for prefix in ("opening_", "settlement_") for c in
    ("binding_status", "margin_interval_id", "margin_kind", "initial", "maintenance", "known_at")]
SPEC_FIELDS = {
    "product": pl.String, "effective_date": pl.Date, "valid_until_exclusive": pl.Date,
    "known_at": pl.String, "source_content_sha256s": pl.List(pl.String),
    "contract_multiplier": pl.Float64, "opening_time": pl.String,
    "settlement_time": pl.String, "expiry_time": pl.String,
    "tick_kind": pl.String, "tick_size": pl.Float64,
    "limit_up_ratio": pl.Float64, "limit_down_ratio": pl.Float64,
    "margin_value_kind": pl.String, "tax_value_kind": pl.String,
    "tax_fixed_value_twd": pl.Float64, "tax_class": pl.String,
    "settlement_method": pl.String, "position_grandfather_existing": pl.Boolean,
    "second_position_grandfather_existing": pl.Boolean,
}
# Existing specification releases explicitly describe percentage limits and
# unchanged quantities. New contracts may state an absolute quote-point band
# or split every outstanding month without changing the product code.
SPEC_EXTENSIONS = {
    "limit_kind": (pl.String, "ratio"),
    "limit_up_points": (pl.Float64, None),
    "limit_down_points": (pl.Float64, None),
    "carry_split_numerator": (pl.Int64, 1),
    "carry_split_denominator": (pl.Int64, 1),
}


def _positive(name):
    return (pl.col(name).is_not_null() & pl.col(name).is_finite() & (pl.col(name) > 0)).fill_null(False)


def _clock(day, clock):
    return (pl.col(day).cast(pl.String) + "T" + pl.col(clock) + "+08:00")


def _utc(value):
    return value.str.to_datetime(time_zone="UTC", strict=False)


def inventory_entry_reachability(frame: pl.DataFrame, rules: pl.DataFrame) -> pl.DataFrame:
    """Bound possible inventory from causal entry capacity and declared carry edges.

    This is an upper bound, independent of model actions. Never retire a possible
    position because open interest is zero or a later observation is missing.
    Unknown entry capacity/origins remain reachable. Only account rows enter the
    graph; the account starts empty and source warmup rows cannot place orders.
    """
    if rules.select(KEYS).is_duplicated().any() or frame.select(KEYS).is_duplicated().any():
        raise ValueError("duplicate physical day in inventory reachability")
    required={"executable", "volume"}
    proof_fields={"carry_from_date", "carry_from_physical_contract", "inventory_origin_unresolved"}
    if not required<=set(frame.columns) or not proof_fields<=set(rules.columns):
        return rules.select(KEYS).with_columns(pl.lit(True).alias("inventory_entry_reachable"))
    data=rules.select(*KEYS,*sorted(proof_fields)).join(
        frame.select(*KEYS,"executable","volume"),on=KEYS,how="left",validate="1:1")
    if rules.join(frame.select(KEYS),on=KEYS,how="anti").height:
        raise ValueError("inventory reachability lacks a source physical day")
    incoming=data.filter(pl.col("carry_from_physical_contract")!="")
    if incoming.filter(pl.col("carry_from_date").is_null()
                       |(pl.col("carry_from_date")>=pl.col("date"))).height:
        raise ValueError("inventory reachability requires strictly earlier carry dates")
    data=data.sort(KEYS).with_columns(
        (pl.col("inventory_origin_unresolved").fill_null(True)
         |pl.col("executable").fill_null(True)&(
             pl.col("volume").is_null()|~pl.col("volume").is_finite()|(pl.col("volume")>0)))
        .alias("_entry_seed"))
    def prefix(rows):
        return rows.sort(KEYS).with_columns(pl.col("_entry_seed").cast(pl.Int64).cum_sum()
            .over("physical_contract").gt(0).alias("inventory_entry_reachable"))
    data=prefix(data)
    cross=incoming.filter(pl.col("carry_from_physical_contract")!=pl.col("physical_contract"))
    missing_same=incoming.filter(pl.col("carry_from_physical_contract")==pl.col("physical_contract")).join(
        data.select(pl.col("date").alias("carry_from_date"),
                    pl.col("physical_contract").alias("carry_from_physical_contract")),
        on=["carry_from_date","carry_from_physical_contract"],how="anti")
    cross=pl.concat([cross,missing_same],how="vertical")
    # Same-identity carry is already bounded by the causal prefix. Cross-identity
    # events seed only their destination date, then propagate forward. Iteration
    # handles multi-step conversions without letting future nodes seed the past.
    for _ in range(cross.height+1):
        sources=data.select(pl.col("date").alias("carry_from_date"),
            pl.col("physical_contract").alias("carry_from_physical_contract"),
            pl.col("inventory_entry_reachable").alias("_source_reachable"))
        targets=cross.join(sources,on=["carry_from_date","carry_from_physical_contract"],
            how="left",validate="m:1").join(data.select(*KEYS,"inventory_entry_reachable"),
            on=KEYS,validate="1:1").filter(
                pl.col("_source_reachable").fill_null(True)&~pl.col("inventory_entry_reachable"))
        if targets.is_empty():
            return data.select(*KEYS,"inventory_entry_reachable").sort(KEYS)
        data=prefix(data.join(targets.select(KEYS).with_columns(pl.lit(True).alias("_incoming_seed")),
            on=KEYS,how="left",validate="1:1").with_columns(
                (pl.col("_entry_seed")|pl.col("_incoming_seed").fill_null(False)).alias("_entry_seed"))
            .drop("_incoming_seed"))
    raise ValueError("inventory reachability did not converge")


def bind_product_specifications(days: pl.DataFrame, specifications: pl.DataFrame) -> pl.DataFrame:
    """One dated specification per product-day, without current-master fallback."""
    missing = set(SPEC_FIELDS) - set(specifications.columns)
    if missing:
        raise ValueError(f"missing dated product specification fields: {sorted(missing)}")
    for column in ("carry_split_numerator", "carry_split_denominator"):
        if column in specifications.columns and specifications.filter(
                pl.col(column).is_null() | (pl.col(column).cast(pl.Float64)
                    != pl.col(column).cast(pl.Int64).cast(pl.Float64))).height:
            raise ValueError("product quantity splits require explicit integer ratios")
    specs = specifications.select(
        *[pl.col(c).cast(t) for c, t in SPEC_FIELDS.items()],
        *[(pl.col(c).cast(t) if c in specifications.columns else pl.lit(default, dtype=t)).alias(c)
          for c, (t, default) in SPEC_EXTENSIONS.items()])
    if specs.select("product", "effective_date").is_duplicated().any():
        raise ValueError("duplicate product specification boundary")
    required = (set(SPEC_FIELDS) - {"valid_until_exclusive", "tick_size", "tax_fixed_value_twd",
                                   "limit_up_ratio", "limit_down_ratio"}) | {
                                       "limit_kind", "carry_split_numerator", "carry_split_denominator"}
    if specs.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError("incomplete product specification")
    if specs.filter(~pl.col("known_at").str.contains(r"(Z|[+-]\d{2}:\d{2})$")
                    | (_utc(pl.col("known_at")).is_null())
                    | (pl.col("source_content_sha256s").list.len() == 0)).height:
        raise ValueError("specifications require dated source evidence")
    for field in ("opening_time", "settlement_time", "expiry_time"):
        if specs.filter(~pl.col(field).str.contains(r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$" )).height:
            raise ValueError("invalid dated product clock")
    if specs.filter(~_positive("contract_multiplier")
            | ~pl.col("tick_kind").is_in(["fixed", "stock_future", "etf_future"])
            | ((pl.col("tick_kind") == "fixed") & ~_positive("tick_size"))
            | ~pl.col("limit_kind").is_in(["ratio", "points"])
            | ((pl.col("limit_kind") == "ratio") & (~_positive("limit_down_ratio")
                | (pl.col("limit_down_ratio") >= 1) | ~_positive("limit_up_ratio")
                | (pl.col("limit_up_ratio") <= 1)))
            | ((pl.col("limit_kind") == "points") & (~_positive("limit_up_points")
                | ~_positive("limit_down_points") | pl.col("limit_up_ratio").is_not_null()
                | pl.col("limit_down_ratio").is_not_null()))
            | ((pl.col("limit_kind") == "ratio") & (pl.col("limit_up_points").is_not_null()
                | pl.col("limit_down_points").is_not_null()))
            | (pl.col("carry_split_numerator") <= 0) | (pl.col("carry_split_denominator") <= 0)
            | ~pl.col("margin_value_kind").is_in(["quoted_value", "full_value", "face_value"])
            | ~pl.col("tax_value_kind").is_in(["quoted_value", "full_value", "face_value"])
            | ((pl.col("tax_value_kind").eq("face_value") | pl.col("margin_value_kind").eq("face_value"))
               & ~_positive("tax_fixed_value_twd"))
            | ~pl.col("settlement_method").is_in(["cash_settlement", "physical_delivery"])).height:
        raise ValueError("unsupported or invalid dated product specification")
    specs = specs.sort("product", "effective_date").with_columns(
        pl.col("effective_date").shift(-1).over("product").alias("_next"))
    if specs.filter((pl.col("valid_until_exclusive") <= pl.col("effective_date"))
            | (pl.col("_next").is_not_null() & (pl.col("valid_until_exclusive").is_null()
              | (pl.col("valid_until_exclusive") > pl.col("_next"))))).height:
        raise ValueError("overlapping product specification intervals")
    result = days.select("date", "product").unique().sort("date").join_asof(
        specs.drop("_next").sort("effective_date"), left_on="date", right_on="effective_date",
        by="product", strategy="backward", check_sortedness=False)
    valid = (pl.col("effective_date").is_not_null()
        & (pl.col("valid_until_exclusive").is_null() | (pl.col("date") < pl.col("valid_until_exclusive"))))
    public = _utc(pl.col("known_at")) <= _utc(_clock("date", "opening_time"))
    valid = (valid & public).fill_null(False)
    # Ended and unannounced specifications never supply numerical fallback values.
    return result.select("date", "product", valid.alias("specification_bound"),
        *[pl.when(valid).then(pl.col(c)).otherwise(None).alias("spec_" + c)
          for c in [*SPEC_FIELDS, *SPEC_EXTENSIONS] if c != "product"])


def position_constraint_columns() -> list[pl.Expr]:
    """Canonical mapping from bound position inputs to the two account axes."""
    return [pl.col("pos_position_unit").alias("position_unit"),
        pl.col("pos_position_limit").alias("position_limit"),
        pl.col("pos_position_root_product").alias("position_group"),
        pl.when(pl.col("pos_independent_contract_limit").is_not_null())
          .then(pl.lit("POSITION_SINGLE:")+pl.col("product"))
          .when(pl.col("pos_monthly_position_limit").is_not_null())
          .then(pl.col("pos_position_root_product")+":"+pl.col("contract"))
          .otherwise(pl.col("pos_position_root_product")).alias("second_position_group"),
        pl.when(pl.col("pos_independent_contract_limit").is_not_null()).then(1.)
          .otherwise(pl.col("pos_position_unit")).alias("second_position_unit"),
        pl.coalesce("pos_independent_contract_limit","pos_monthly_position_limit","pos_position_limit")
          .alias("second_position_limit")]


def position_constraint_errors() -> tuple[pl.Expr, pl.Expr]:
    """Shared dated group/second-axis consistency gates, without valuation."""
    group_fields=["date","pos_position_root_product"]
    conflict=pl.any_horizontal([
        pl.when(pl.col("pos_position_numeric_inputs_resolved")).then(pl.col(c))
          .otherwise(None).drop_nulls().n_unique().over(group_fields)>1
        for c in ("pos_position_limit","pos_unit")])
    conflict=conflict | (pl.when(pl.col("pos_position_numeric_inputs_resolved"))
        .then(pl.col("second_position_limit")).otherwise(None).drop_nulls().n_unique()
        .over(["date","second_position_group"])>1)
    extra=pl.col("pos_independent_contract_limit").is_not_null()
    unsupported=extra & (~_positive("pos_independent_contract_limit")
        | pl.col("pos_monthly_position_limit").is_not_null())
    return conflict,unsupported


def compile_execution_terms(frame: pl.DataFrame, margins: pl.DataFrame,
        positions: pl.DataFrame, corporate_terms: pl.DataFrame,
        margin_intervals: pl.DataFrame, specifications: pl.DataFrame,
        terminal_values: pl.DataFrame, *,
        position_research_policy: dict | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Vectorized all-product compiler; return rules and explicit row blockers.

    Specification intervals establish historical units, clocks and price/tax
    policy.  Existing preparation binders supply margin/position/corporate
    facts.  Their candidate flags are not treated as admission receipts.
    """
    if frame.select(KEYS).is_duplicated().any():
        raise ValueError("duplicate physical account day")
    frame = frame.drop("contract_multiplier", strict=False)
    # Provenance is validated once on the small source tables and retained in
    # their SHA-bound manifests. Expanding lists of source hashes into every
    # contract-day costs GBs without contributing to the arithmetic.
    specs = bind_product_specifications(frame, specifications).drop(
        "spec_source_content_sha256s", "spec_valid_until_exclusive")
    f = frame.join(specs, on=["date", "product"], how="left", validate="m:1")
    for source, keys in ((margins, ["date", "product"]), (positions, QUOTE_KEYS), (terminal_values, QUOTE_KEYS)):
        if source.select(keys).is_duplicated().any():
            raise ValueError("duplicate execution-term input")
    positions=positions.with_columns(*[pl.lit(None,dtype=t).alias(c)
        for c,t in POSITION_INPUT_EXTENSIONS.items() if c not in positions.columns])
    f = f.join(positions.select(*QUOTE_KEYS,
        *[pl.col(c).alias("pos_" + c) for c in [*POSITION_INPUT_FIELDS,*POSITION_INPUT_EXTENSIONS]]),
        on=QUOTE_KEYS, how="left", validate="1:1")
    f = f.join(margins.select("date", "product", *MARGIN_INPUT_FIELDS),
        on=["date", "product"], how="left", validate="m:1")
    levels = margin_intervals.with_row_index("_interval")
    for prefix in ("opening_", "settlement_"):
        f = f.join(levels.select(pl.col("_interval").alias(prefix + "margin_interval_id"),
            *[pl.col(c).alias(prefix + "rule_" + c) for c in
              ("effective_date", "effective_phase", "known_at",
               "margin_kind", "initial", "maintenance")]),
            on=prefix + "margin_interval_id", how="left", validate="m:1")
    terms = bind_dated_corporate_terms(f.select(QUOTE_KEYS), corporate_terms)
    term_fields = [c for c in terms.columns if c not in QUOTE_KEYS + ["source_content_sha256s"]]
    f = f.join(terms.select(*QUOTE_KEYS, *[pl.col(c).alias("corp_" + c) for c in term_fields]),
               on=QUOTE_KEYS, how="left", validate="1:1")
    events = corporate_terms.with_row_index("corp_corporate_term_id")
    extras = ["from_product", "effective_date", "effective_at", "equity_cash_credit_twd",
              "has_equity_credit_fields", "carry_quantity_numerator", "carry_quantity_denominator"]
    f = f.join(events.select("corp_corporate_term_id", *[pl.col(c).alias("event_" + c) for c in extras]),
               on="corp_corporate_term_id", how="left", validate="m:1")
    f = f.join(terminal_values.select(*QUOTE_KEYS, "terminal_value_input_twd"),
               on=QUOTE_KEYS, how="left", validate="1:1")
    corporate = pl.col("corp_terms_binding_status").eq("bound_prior_publication").fill_null(False)
    adjusted = pl.col("product").str.contains(r"\d$")
    # Standard codes can also undergo cash credits or same-code quantity splits.
    f = f.with_columns(
        pl.when(corporate).then(pl.col("corp_contract_multiplier"))
          .when(~adjusted).then(pl.col("spec_contract_multiplier")).alias("contract_multiplier"),
        # Subscription rights enter the final deliverable, never the daily
        # futures value, even after their cash amount has become fixed. The
        # quoted futures price already prices that future entitlement.
        pl.when(corporate).then(pl.col("corp_deliverable_cash_twd"))
          .when(~adjusted).then(0.).alias("_deliverable_cash"),
        pl.col("spec_opening_time").alias("opening_time"),
        pl.when(pl.col("cash_settlement")).then(pl.col("spec_expiry_time"))
          .otherwise(pl.col("spec_settlement_time")).alias("settlement_time"),
        (corporate & (pl.col("event_effective_date").cast(pl.Date) <= pl.col("date"))
          & (pl.col("event_effective_date").cast(pl.Date) > pl.col("previous_market_date")))
          .fill_null(False).alias("_corporate_event_today"),
        ((pl.col("spec_carry_split_numerator") != pl.col("spec_carry_split_denominator"))
          & (pl.col("spec_effective_date") <= pl.col("date"))
          & (pl.col("spec_effective_date") > pl.col("previous_market_date"))
          & pl.col("previous_symbol_date").is_not_null()).fill_null(False).alias("_split_today"),
    )
    # The same published capacity can be represented in root contracts and
    # adjusted shares. Compare physical capacities, never raw unlike units.
    # This converts an exactly equivalent obligation; it does not choose a
    # smaller cap or apply a missing historical grade.
    share=(pl.col("pos_unit").is_in(["shares","beneficial_units"])
        & pl.col("pos_position_numeric_inputs_resolved")).fill_null(False)
    axes=["date","pos_position_root_product"]
    f=f.with_columns(
        pl.when(share).then(pl.col("pos_position_limit")).otherwise(None).max().over(axes).alias("_share_cap"),
        pl.when(share).then(pl.col("pos_unit")).otherwise(None).max().over(axes).alias("_share_measure"))
    factor=pl.col("contract_multiplier")/pl.col("pos_position_unit")
    equivalent=((pl.col("pos_unit")=="contracts") & pl.col("pos_position_numeric_inputs_resolved")
        & _positive("pos_position_unit") & _positive("contract_multiplier")
        & ((pl.col("pos_position_limit")*factor)==pl.col("_share_cap"))).fill_null(False)
    f=f.with_columns(
        pl.when(equivalent).then(pl.col("contract_multiplier")).otherwise(pl.col("pos_position_unit"))
          .alias("pos_position_unit"),
        pl.when(equivalent).then(pl.col("_share_cap")).otherwise(pl.col("pos_position_limit"))
          .alias("pos_position_limit"),
        pl.when(equivalent).then(pl.col("pos_monthly_position_limit")*factor)
          .otherwise(pl.col("pos_monthly_position_limit")).alias("pos_monthly_position_limit"),
        pl.when(equivalent).then(pl.col("_share_measure")).otherwise(pl.col("pos_unit")).alias("pos_unit"))
    if f.filter(pl.col("_corporate_event_today") & pl.col("_split_today")).height:
        raise ValueError("overlapping corporate and product-wide quantity transitions")
    f = f.with_columns((pl.col("_corporate_event_today") | pl.col("_split_today")).alias("_event_today"))
    # The numeric day's closing snapshot belongs to the regular margin phase.
    # Cash expiry can occur earlier. Its event clock does not move the later
    # legal margin boundary forward: before regular close, the admitted opening
    # phase remains in force under this two-phase source contract. Keep that
    # interval identity and its knowledge clock, including any unresolved gap.
    early_cash = (pl.col("cash_settlement") & pl.col("specification_bound")
        & (pl.col("spec_settlement_method") == "cash_settlement")
        & (pl.col("date") == pl.col("official_expiry"))
        & (pl.col("spec_expiry_time") < pl.col("spec_settlement_time"))).fill_null(False)
    opening_margin_fields = [c for c in MARGIN_INPUT_FIELDS if c.startswith("opening_")]
    opening_margin_fields += [c for c in f.columns if c.startswith("opening_rule_")]
    f = f.with_columns(*[
        pl.when(early_cash).then(pl.col(c))
          .otherwise(pl.col(c.replace("opening_", "settlement_", 1)))
          .alias(c.replace("opening_", "settlement_", 1)) for c in opening_margin_fields])
    # The numeric binder deliberately leaves same-day announcements undecided.
    # Now that the actual product clock is known, resolve only that case; an
    # ended interval or a later announcement cannot revive an earlier level.
    for prefix, clock in (("opening_", "opening_time"), ("settlement_", "settlement_time")):
        effect = (pl.col(prefix + "rule_effective_date") + "T"
            + pl.when(pl.col(prefix + "rule_effective_phase") == 0).then(pl.col("opening_time"))
              .otherwise(pl.col("spec_settlement_time")) + "+08:00")
        resolved = ((pl.col(prefix + "binding_status") == "same_day_clock_review")
            & (_utc(pl.col(prefix + "rule_known_at")) <= _utc(_clock("date", clock)))
            & (_utc(effect) <= _utc(_clock("date", clock)))).fill_null(False)
        f = f.with_columns(
            *[pl.when(resolved).then(pl.col(prefix + "rule_" + c)).otherwise(pl.col(prefix + c)).alias(prefix + c)
              for c in ("initial", "maintenance", "margin_kind", "known_at")],
            pl.when(resolved).then(pl.lit("bound_same_day_clock"))
                .otherwise(pl.col(prefix + "binding_status")).alias(prefix + "binding_status"))
    f = f.with_columns(
        pl.when(pl.col("_corporate_event_today")).then(pl.col("event_from_product"))
          .otherwise(pl.col("product")).alias("_origin_product"),
        pl.when(pl.col("_corporate_event_today")).then(pl.col("event_equity_cash_credit_twd").fill_null(0.))
          .otherwise(0.).alias("carry_cash_twd"),
        pl.when(pl.col("_split_today")).then(pl.col("spec_carry_split_numerator"))
          .when(pl.col("_corporate_event_today")).then(pl.col("event_carry_quantity_numerator").fill_null(1))
          .otherwise(1).alias("carry_quantity_numerator"),
        pl.when(pl.col("_split_today")).then(pl.col("spec_carry_split_denominator"))
          .when(pl.col("_corporate_event_today")).then(pl.col("event_carry_quantity_denominator").fill_null(1))
          .otherwise(1).alias("carry_quantity_denominator"),
        (pl.col("settlement") * pl.col("contract_multiplier") + pl.col("_deliverable_cash"))
          .alias("settlement_contract_value_twd"),
    )
    prior = f.select(pl.col("date").alias("previous_market_date"),
        pl.col("product").alias("_origin_product"), "contract",
        pl.col("physical_contract").alias("_origin_identity"),
        pl.col("settlement_contract_value_twd").alias("_prior_value"),
        pl.col("settlement").alias("_prior_quote"),
        pl.col("contract_multiplier").alias("_prior_multiplier"),
        pl.col("_deliverable_cash").alias("_prior_cash"),
        pl.col("opening_time").alias("_prior_opening_time"))
    f = f.join(prior, on=["previous_market_date", "_origin_product", "contract"], how="left", validate="m:1")
    same_generation = pl.col("_origin_identity").eq(pl.col("physical_contract"))
    valid_origin = (pl.col("_corporate_event_today") | same_generation).fill_null(False)
    f = f.with_columns(
        pl.when(valid_origin).then(pl.col("_origin_identity")).alias("_source_identity"),
        pl.when(valid_origin).then((pl.col("_prior_value") - pl.col("carry_cash_twd"))
            * pl.col("carry_quantity_denominator") / pl.col("carry_quantity_numerator"))
          .alias("known_margin_contract_value_twd"),
    ).with_columns(
        ((pl.col("known_margin_contract_value_twd") - pl.col("_deliverable_cash"))
         / pl.col("contract_multiplier")).alias("_reference_quote"),
        (pl.col("_source_identity").is_null() & pl.col("previous_symbol_date").is_null()
         & ~pl.col("_event_today")).alias("is_warmup"),
        pl.when(pl.col("executable")).then(pl.col("open") * pl.col("contract_multiplier") + pl.col("_deliverable_cash"))
          .otherwise(pl.col("known_margin_contract_value_twd")).alias("opening_contract_value_twd"),
        pl.when(pl.col("cash_settlement")).then(pl.when(adjusted).then(pl.col("terminal_value_input_twd"))
            .otherwise(pl.col("settlement_contract_value_twd")))
          .otherwise(pl.when(pl.col("executable")).then(pl.col("close") * pl.col("contract_multiplier")
            + pl.col("_deliverable_cash")).otherwise(pl.col("settlement_contract_value_twd")))
          .alias("terminal_contract_value_twd"),
        pl.when(pl.col("cash_settlement")).then(pl.lit("cash_settlement"))
          .when((pl.col("date") == pl.col("official_expiry"))
                & (pl.col("spec_settlement_method") == "physical_delivery"))
          .then(pl.lit("market_close_required")).otherwise(pl.lit("mark_only")).alias("terminal_event"),
    )
    for phase, value in (("opening", "opening_contract_value_twd"),
                          ("settlement", "settlement_contract_value_twd"),
                          ("known", "known_margin_contract_value_twd"),
                          ("terminal", "terminal_contract_value_twd")):
        quoted = pl.col(value) - pl.col("_deliverable_cash")
        if phase == "terminal":
            # A cash-settlement deliverable can include subscription rights.
            # A quoted-value tax/margin policy must still use the quote, not
            # that full deliverable less only its other cash component.
            quoted = (pl.when(pl.col("cash_settlement")).then(pl.col("settlement"))
                .when(pl.col("executable")).then(pl.col("close"))
                .otherwise(pl.col("settlement"))) * pl.col("contract_multiplier")
        for kind in (("margin",) if phase == "settlement" else ("margin", "tax")):
            f = f.with_columns(pl.when(pl.col("spec_" + kind + "_value_kind") == "face_value")
                .then(pl.col("spec_tax_fixed_value_twd"))
                .when(pl.col("spec_" + kind + "_value_kind") == "full_value").then(pl.col(value))
                .otherwise(quoted)
                .alias(phase + "_" + kind + "_value_twd"))
    # Compute rates once per tax-class/day rather than a Python callback for each
    # contract or duplicating the canonical dated tax schedule.
    tax_days = f.select("date", "spec_tax_class").unique().filter(pl.col("spec_tax_class").is_not_null())
    rates = []
    for day, kind in tax_days.iter_rows():
        try:
            rates.append(futures_transaction_tax_rate(day, tax_class=kind))
        except ValueError:
            rates.append(None)
    tax_days = tax_days.with_columns(pl.Series("_tax_rate", rates, dtype=pl.Float64))
    f = f.join(tax_days, on=["date", "spec_tax_class"], how="left", validate="m:1").with_columns(
        *[(pl.col(p + "_tax_value_twd") * pl.col("_tax_rate") + .5).floor().alias(p + "_tax_twd")
          for p in ("opening", "terminal", "known")],
        pl.col("opening_margin_kind").alias("margin_kind"),
        pl.col("opening_initial").alias("initial"), pl.col("opening_maintenance").alias("maintenance"),
        *position_constraint_columns(),
        *[pl.col("spec_" + c).alias(c) for c in ("position_grandfather_existing", "second_position_grandfather_existing")],
    )
    if position_research_policy is not None:
        from stockagent.data.tw_futures_position_research import apply_research_position_capacity
        conflict, unsupported = position_constraint_errors()
        pos_effective = _utc(pl.col("pos_effective_date") + "T"
            + pl.when(pl.col("pos_effective_phase") == 0).then(pl.col("opening_time"))
              .otherwise(pl.col("settlement_time")) + "+08:00")
        cutoff = _utc(_clock("date", "opening_time"))
        failed = (~pl.col("pos_position_numeric_inputs_resolved") | conflict | unsupported
            | ~_positive("pos_position_unit") | ~_positive("pos_position_limit")
            | pl.col("pos_position_root_product").is_null()
            | (_utc(pl.col("pos_known_at")) > cutoff)
            | _utc(pl.col("pos_known_at")).is_null() | pos_effective.is_null()
            | (pos_effective > cutoff)).fill_null(True)
        f = apply_research_position_capacity(f, position_research_policy, failed)
        f = f.with_columns(*position_constraint_columns())
    for prefix, clock in (("opening_", "opening_time"), ("settlement_", "settlement_time")):
        f = f.with_columns((pl.col(prefix + "rule_effective_date") + "T"
            + pl.when(pl.col(prefix + "rule_effective_phase") == 0).then(pl.col("opening_time"))
              .otherwise(pl.col("spec_settlement_time")) + "+08:00").alias(prefix + "effective_at"))
    f = f.with_columns(
        pl.max_horizontal(*[_utc(pl.col(c)) for c in
            ("opening_known_at", "pos_known_at", "spec_known_at", "corp_known_at")])
            .dt.to_string("%Y-%m-%dT%H:%M:%S%:z").alias("known_at"),
        pl.max_horizontal(_utc(pl.col("opening_effective_at")),
            _utc(pl.col("pos_effective_date") + "T" + pl.when(pl.col("pos_effective_phase")==0)
                 .then(pl.col("opening_time")).otherwise(pl.col("settlement_time")) + "+08:00"),
            _utc(pl.col("spec_effective_date").cast(pl.String) + "T" + pl.col("opening_time") + "+08:00"))
            .dt.to_string("%Y-%m-%dT%H:%M:%S%:z").alias("effective_at"),
    )
    # Price buckets at the LIMIT itself determine the last permitted tick.
    # Using the reference price's bucket is wrong at a bucket crossing.
    for output, ratio, rounding in (("upper_limit", "spec_limit_up_ratio", np.floor),
                                    ("lower_limit", "spec_limit_down_ratio", np.ceil)):
        point_field = "spec_limit_up_points" if output == "upper_limit" else "spec_limit_down_points"
        sign = 1 if output == "upper_limit" else -1
        values = f.select(pl.when(pl.col("spec_limit_kind") == "points")
            .then(pl.col("_reference_quote") + sign * pl.col(point_field))
            .otherwise(pl.col("_reference_quote") * pl.col(ratio))).to_series().to_numpy()
        ticks = f["spec_tick_size"].to_numpy().copy()
        for kind in ("stock_future", "etf_future"):
            mask = (f["spec_tick_kind"] == kind).fill_null(False).to_numpy()
            if mask.any():
                ticks[mask] = tick_size_numpy(values[mask], f["date"].to_numpy()[mask], security_types=kind)
        with np.errstate(invalid="ignore", divide="ignore"):
            result = rounding(values / ticks + (1e-8 if output == "upper_limit" else -1e-8)) * ticks
        f = f.with_columns(pl.Series(output, result).fill_nan(None))
    for name in ("initial", "maintenance"):
        f = f.with_columns((pl.when(pl.col("settlement_margin_kind") == "notional_rate")
            .then(pl.col("settlement_" + name) * pl.col("settlement_margin_value_twd"))
            .otherwise(pl.col("settlement_" + name)) + .5).floor().alias("_end_" + name))
    prior_account = f.select(pl.col("date").alias("previous_market_date"),
        pl.col("physical_contract").alias("_source_identity"),
        pl.col("is_warmup").alias("_source_warmup"),
        pl.col("_end_initial").alias("carry_previous_initial_twd"),
        pl.col("_end_maintenance").alias("carry_previous_maintenance_twd"))
    f = f.join(prior_account, on=["previous_market_date", "_source_identity"], how="left", validate="m:1")
    incoming = pl.col("_source_identity").is_not_null() & ~pl.col("_source_warmup").fill_null(True)
    f = f.with_columns(
        pl.when(incoming).then(pl.col("_source_identity")).otherwise(pl.lit("")).alias("carry_from_physical_contract"),
        pl.when(incoming).then(pl.col("previous_market_date")).alias("carry_from_date"),
        pl.col("_prior_value").alias("carry_previous_value_twd"),
        pl.when(pl.col("_split_today")).then(pl.col("spec_known_at"))
          .when(pl.col("_corporate_event_today")).then(pl.col("corp_known_at"))
          .otherwise(pl.col("previous_market_date").cast(pl.String) + "T23:59:59+08:00").alias("carry_known_at"),
        _clock("date", "opening_time").alias("carry_effective_at"),
        pl.when(incoming).then(pl.col("carry_cash_twd")).otherwise(0.).alias("carry_cash_twd"),
        pl.when(incoming).then(pl.col("carry_quantity_numerator")).otherwise(1).alias("carry_quantity_numerator"),
        pl.when(incoming).then(pl.col("carry_quantity_denominator")).otherwise(1).alias("carry_quantity_denominator"),
    )
    f=f.with_columns((~pl.col("is_warmup")&pl.col("_source_identity").is_null())
        .alias("inventory_origin_unresolved"))
    reachability=inventory_entry_reachability(f,f.filter(~pl.col("is_warmup")))
    f=f.join(reachability,on=KEYS,how="left",validate="1:1")
    owners = f.filter((pl.col("carry_from_physical_contract") != "") & ~pl.col("is_warmup")).group_by(
        "carry_from_date", "carry_from_physical_contract").agg(
            pl.len().alias("_next_owners"), pl.col("date").min().alias("_next_owner_date"))
    f = f.join(owners, left_on=KEYS, right_on=["carry_from_date", "carry_from_physical_contract"],
               how="left", validate="1:1")
    needs_owner = (~pl.col("is_warmup") & pl.col("inventory_entry_reachable").fill_null(True)
                   & (pl.col("terminal_event") == "mark_only")
                   & pl.col("next_market_date").is_not_null())
    admitted_margin = ["bound_prior_publication", "bound_same_security_rate", "bound_same_day_clock"]
    group_conflict,unsupported_extra=position_constraint_errors()
    reasons = {
        "missing_product_specification": ~pl.col("specification_bound"),
        "missing_opening_margin": ~pl.col("opening_binding_status").is_in(admitted_margin),
        "missing_settlement_margin": ~pl.col("settlement_binding_status").is_in(admitted_margin),
        "intraday_margin_kind_change": (pl.col("opening_margin_kind") != pl.col("settlement_margin_kind")).fill_null(False),
        "missing_position_limit": ~pl.col("pos_position_numeric_inputs_resolved") | group_conflict | unsupported_extra,
        "missing_contract_units": ~_positive("contract_multiplier"),
        "missing_adjusted_terms": adjusted & ~corporate,
        "unvalued_subscription_rights": (pl.col("cash_settlement")
            & pl.col("corp_subscription_rights_at_final_settlement").fill_null(False)
            & ~_positive("terminal_value_input_twd")),
        "missing_terminal_value": ~_positive("terminal_contract_value_twd"),
        "missing_settlement_value": ~_positive("settlement_contract_value_twd"),
        "missing_carry_origin": pl.col("_event_today") & pl.col("_source_identity").is_null(),
        "missing_prior_valuation": ~pl.col("is_warmup") & ~_positive("known_margin_contract_value_twd"),
        "missing_opening_value": ~pl.col("is_warmup") & ~_positive("opening_contract_value_twd"),
        "missing_corporate_cash": pl.col("_event_today") & pl.col("event_has_equity_credit_fields").fill_null(False)
                                  & pl.col("event_equity_cash_credit_twd").is_null(),
        "unexplained_multiplier_change": (same_generation & ~pl.col("_event_today")
            & (pl.col("contract_multiplier") != pl.col("_prior_multiplier"))).fill_null(False),
        "inconsistent_product_split": pl.col("_split_today") & (
            ((pl.col("_prior_multiplier") * pl.col("carry_quantity_denominator")
              - pl.col("contract_multiplier") * pl.col("carry_quantity_numerator")).abs() > 1e-8)
            | ~same_generation).fill_null(True),
        "unknown_tax_schedule": pl.col("_tax_rate").is_null(),
        "missing_price_limits": ~_positive("upper_limit") | ~_positive("lower_limit"),
        "opening_rule_clock": (_utc(pl.col("known_at")) > _utc(_clock("date", "opening_time")))
            | (_utc(pl.col("effective_at")) > _utc(_clock("date", "opening_time"))),
        "settlement_rule_clock": (_utc(pl.col("settlement_known_at")) > _utc(_clock("date", "settlement_time")))
            | (_utc(pl.col("settlement_effective_at")) > _utc(_clock("date", "settlement_time"))),
        "missing_position_clock": _utc(pl.col("pos_known_at")).is_null() | pl.col("pos_effective_date").is_null(),
        "lost_inventory_continuation": (needs_owner & ((pl.col("_next_owners").fill_null(0) != 1)
            | (pl.col("_next_owner_date") != pl.col("next_market_date")).fill_null(True))),
        "duplicated_inventory_origin": (pl.col("_next_owners") > 1).fill_null(False),
        "terminal_inventory_carried": ((pl.col("terminal_event") != "mark_only")
            & pl.col("_next_owners").is_not_null()),
    }
    blockers = f.select(*QUOTE_KEYS, "physical_contract", "is_warmup",
        *[expr.fill_null(True).alias(name) for name, expr in reasons.items()]).with_columns(
        pl.any_horizontal(pl.col(c) for c in reasons).alias("has_blocker"))
    columns = [*KEYS, "product", "contract", "contract_multiplier", "margin_kind", "initial", "maintenance",
        "settlement_initial", "settlement_maintenance", "known_at", "effective_at", "settlement_known_at",
        "settlement_effective_at", "opening_time", "settlement_time", "upper_limit", "lower_limit",
        "position_group", "position_unit", "position_limit", "second_position_group", "second_position_unit",
        "second_position_limit", "position_grandfather_existing", "second_position_grandfather_existing",
        "opening_contract_value_twd", "settlement_contract_value_twd", "terminal_contract_value_twd",
        "known_margin_contract_value_twd", "opening_margin_value_twd", "settlement_margin_value_twd",
        "known_margin_value_twd", "opening_tax_twd", "terminal_tax_twd", "known_tax_twd", "terminal_event",
        "carry_from_date", "carry_from_physical_contract", "carry_quantity_numerator", "carry_quantity_denominator",
        "carry_cash_twd", "carry_known_at", "carry_effective_at", "carry_previous_value_twd",
        "carry_previous_initial_twd", "carry_previous_maintenance_twd"]
    columns.extend(["inventory_origin_unresolved","inventory_entry_reachable"])
    if position_research_policy is not None:
        from stockagent.data.tw_futures_position_research import POSITION_RESEARCH_COLUMNS
        columns.extend(POSITION_RESEARCH_COLUMNS)
    return f.filter(~pl.col("is_warmup")).select(columns).sort(KEYS), blockers.sort(KEYS)
