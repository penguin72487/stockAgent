"""Explicit research book values, kept separate from exchange observations.

Missing, non-executable marks may carry the preceding inventory's FULL value.
Verified cash/quantity events transform that value once. Real observations and
legal/final marks always prevail; no quote, volume or market fill is invented.
"""
from __future__ import annotations

from datetime import date, timedelta
import json
import math
from pathlib import Path

import polars as pl

from downloader.artifact_io import sha256_file

VALUATION_RESEARCH_CONTRACT = "frozen_contract_value_research_v1"
VALUATION_RESEARCH_SCHEMA = 8
VALUATION_RESEARCH_COLUMNS = {
    "valuation_research_applied": pl.Boolean,
    "valuation_research_method": pl.String,
    "valuation_research_seed_date": pl.Date,
    "valuation_research_seed_identity": pl.String,
    "valuation_research_seed_value_twd": pl.Float64,
    "valuation_research_value_twd": pl.Float64,
    "valuation_research_owner_extension": pl.Boolean,
    "valuation_research_terminal_assumption": pl.Boolean,
}


def validate_valuation_research_policy(policy: dict) -> dict:
    if (policy.get("contract") != VALUATION_RESEARCH_CONTRACT
            or policy.get("research_only") is not True
            or policy.get("financial_point_in_time_verified") is not False
            or not isinstance(policy.get("products"), list) or not policy["products"]
            or not isinstance(policy.get("physical_instances"), list)
            or not isinstance(policy.get("continuation_transfers"), list)
            or policy.get("terminal_cash_conversion_authorized") not in (False, True)):
        raise ValueError("explicit frozen-contract-value research policy required")
    for field in ("source_manifest_sha256", "rule_delta_manifest_sha256",
                  "prior_replay_manifest_sha256", "prior_gap_worklist_sha256"):
        value = policy.get(field, "")
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("research valuation requires bound source and prior accounting identities")
    return policy


def validate_valuation_research_manifest(manifest: dict, root: Path) -> dict:
    if (manifest.get("schema_version") != VALUATION_RESEARCH_SCHEMA
            or manifest.get("research_only") is not True
            or manifest.get("point_in_time_verified") is not False
            or manifest.get("financial_point_in_time_verified") is not False
            or manifest.get("valuation_research_contract") != VALUATION_RESEARCH_CONTRACT):
        raise ValueError("frozen valuation release requires the separate research ABI")
    proof = manifest.get("valuation_research_policy", {})
    relative = Path(proof.get("path", ""))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts or relative.parts[0] != "sources":
        raise ValueError("unsafe research valuation policy receipt")
    if sha256_file(root / relative) != proof.get("sha256"):
        raise ValueError("research valuation policy SHA mismatch")
    if manifest.get("valuation_research_policy_sha256") != proof["sha256"]:
        raise ValueError("research valuation manifest and policy differ")
    policy = validate_valuation_research_policy(json.loads((root / relative).read_text()))
    # This ABI can compose the existing disclosed position-risk assumption,
    # without falsely asserting its estimated financial values are verified.
    from stockagent.data.tw_futures_position_research import validate_position_research_policy
    position = manifest.get("position_research_policy", {})
    relative = Path(position.get("path", ""))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts or relative.parts[0] != "sources":
        raise ValueError("unsafe research position policy receipt")
    if sha256_file(root / relative) != position.get("sha256"):
        raise ValueError("research position policy SHA mismatch")
    pos_policy = validate_position_research_policy(json.loads((root / relative).read_text()))
    if (manifest.get("position_research_contract") != pos_policy["contract"]
            or manifest.get("position_research_policy_sha256") != position["sha256"]):
        raise ValueError("research position manifest and policy differ")
    return policy


def _positive(value) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _extend_verified_destinations(physical: pl.DataFrame, calendar: pl.DataFrame,
                                 terms: pl.DataFrame, policy: dict, *, corporate_candidates=None,
                                 final_settlements=None) -> tuple[pl.DataFrame, set[str]]:
    """Start an observed destination on its VERIFIED conversion date.

Only identity metadata is copied from the later destination observation. Its
prices/volumes are cleared; the first valuation comes from its prior owner.
"""
    extra = []
    targets = set(policy["physical_instances"])
    scoped_products = set(physical['product'])
    market_dates = calendar["date"].sort().to_list()
    for item in policy["continuation_transfers"]:
        if item['product'] not in scoped_products:
            continue
        boundary = date.fromisoformat(item["corporate_boundary"])
        target, origin, month = item["corporate_transfer_target"], item["product"], item["contract"]
        event = terms.filter((pl.col("product") == target) & (pl.col("from_product") == origin)
            & (pl.col("effective_date").cast(pl.Date) == boundary)
            & (pl.col("contract") == month))
        if event.height != 1:
            raise ValueError("research carry extension lacks one dated, month-owned corporate event")
        event = event.row(0, named=True)
        own = physical.filter((pl.col("physical_instance") == item["physical_contract"])
            & (pl.col("date") == date.fromisoformat(item["date"])))
        if own.height != 1 or own["date"][0] >= boundary:
            raise ValueError("research carry extension lacks its previous physical owner")
        destinations = physical.filter((pl.col("product") == target) & (pl.col("contract") == month)
            & (pl.col("date") >= boundary)).sort("date")
        unobserved = destinations.is_empty()
        if unobserved:
            # A sourced conversion month may expire during the halt, with no
            # later quote at all. Its exact own final settlement is still real.
            if corporate_candidates is None:
                raise ValueError("research carry extension has no independently observed destination identity")
            from stockagent.data.tw_futures_margin_preparation import corporate_identity_boundaries
            boundaries, _ = corporate_identity_boundaries(corporate_candidates)
            generation = boundaries.filter((pl.col('product') == target) & (pl.col('contract') == month)
                & (pl.col('corporate_boundary') <= boundary)).sort('corporate_boundary').tail(1)
            if generation.height != 1:
                raise ValueError('unobserved inventory owner lacks its sourced corporate generation')
            final = (final_settlements.filter((pl.col('product') == target) & (pl.col('contract') == month)
                & (pl.col('settlement_date') >= boundary)).sort('settlement_date').head(1)
                if final_settlements is not None else pl.DataFrame())
            if final.is_empty() and (not policy['terminal_cash_conversion_authorized']
                    or item.get('source_open_interest') != 0.):
                raise ValueError('unobserved owner requires exact own final or authorized zero-OI research conversion')
            expiry = final['settlement_date'][0] if final.height else None
            first_session = next((day for day in market_dates if day >= boundary), None)
            if first_session is None:
                raise ValueError('research carry extension has no session after its effective date')
            first = dict(own.row(0, named=True), product=target, contract=month,
                physical_contract=target + ':' + month,
                physical_instance=f"{target}:{month}@{expiry or 'unsettled'}#generation={generation['corporate_generation'][0]}",
                corporate_generation=generation['corporate_generation'][0],
                first_observed_date=first_session, last_observed_date=expiry or first_session,
                calendar_end=expiry or first_session, official_expiry=expiry,
                final_settlement_price=final['final_settlement_price'][0] if final.height else None,
                final_settlement_value=final['final_settlement_value'][0] if final.height else None,
                settlement_method='cash_settlement',
                lifetime_status='official_final' if final.height else 'corporate_transfer_candidate',
                corporate_boundary=None, corporate_transfer_target=None,
                date=(expiry or first_session) + timedelta(days=1))
        else:
            first = destinations.row(0, named=True)
        # Never borrow a later corporate generation after the next conversion.
        later = terms.filter((pl.col("product") == target)
            & (pl.col("effective_date").cast(pl.Date) > boundary)
            & (pl.col("effective_date").cast(pl.Date) <= first["date"])
            & (pl.col("contract") == month))
        if later.height:
            raise ValueError("research carry destination crosses another corporate generation")
        targets.add(first["physical_instance"])
        for day in market_dates:
            if not boundary <= day < first["date"]:
                continue
            row = dict(first, date=day, first_observed_date=boundary)
            for field in physical.columns:
                if field.startswith("official_") and field not in ("official_expiry",):
                    row[field] = None
                if field in ("daily_mark", "valuation_price", "outright_volume", "spread_leg_volume",
                             "reported_volume", "open_interest", "source_sha256"):
                    row[field] = None
            row["cash_settlement"] = False
            row["valuation_research_owner_extension"] = True
            row['valuation_research_terminal_assumption'] = unobserved and first['official_expiry'] is None
            if unobserved and day == first['official_expiry']:
                row.update(cash_settlement=True, valuation_price=first['final_settlement_price'])
            extra.append(row)
    if extra:
        physical = pl.concat([physical, pl.DataFrame(extra, schema=physical.schema, strict=False)], how="vertical")
        if physical.select("date", "product", "contract").is_duplicated().any():
            raise ValueError("research carry extension creates overlapping physical owners")
    return physical, targets


def prepare_frozen_value_physical_history(physical: pl.DataFrame, calendar: pl.DataFrame,
        *, terms: pl.DataFrame, specifications: pl.DataFrame, policy: dict,
        corporate_candidates=None, final_settlements=None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Apply the authorized valuation assumption, leaving execution evidence intact."""
    validate_valuation_research_policy(policy)
    physical = physical.with_columns(*[
        pl.lit(False if dtype == pl.Boolean else None, dtype=dtype).alias(name)
        for name, dtype in VALUATION_RESEARCH_COLUMNS.items()])
    physical, targets = _extend_verified_destinations(physical, calendar, terms, policy,
        corporate_candidates=corporate_candidates, final_settlements=final_settlements)
    from stockagent.data.tw_futures_margin_preparation import bind_dated_corporate_terms
    from stockagent.data.tw_futures_execution_terms import bind_product_specifications
    keys = ["date", "product", "contract"]
    own = physical.filter(pl.col("physical_instance").is_in(sorted(targets)))
    corporate = bind_dated_corporate_terms(own.select(keys), terms)
    corporate = corporate.rename({c: "corp_" + c for c in corporate.columns if c not in keys})
    specs = bind_product_specifications(own.select(keys), specifications)
    inputs = own.join(corporate, on=keys, how="left", validate="1:1").join(specs,
        on=["date", "product"], how="left", validate="m:1")
    inputs = inputs.join(terms.with_row_index("corp_corporate_term_id").select(
        "corp_corporate_term_id", pl.col("effective_date").cast(pl.Date).alias("_event_date"),
        "from_product", "equity_cash_credit_twd", "carry_quantity_numerator", "carry_quantity_denominator"),
        on="corp_corporate_term_id", how="left", validate="m:1")
    market_dates = calendar["date"].sort().to_list()
    previous_day = dict(zip(market_dates[1:], market_dates[:-1]))
    values = {}
    estimates = []
    for row in inputs.sort("date", "product", "contract").iter_rows(named=True):
        day, product, month, identity = row["date"], row["product"], row["contract"], row["physical_instance"]
        corporate_bound = row["corp_terms_binding_status"] == "bound_prior_publication"
        units = row["corp_contract_multiplier"] if corporate_bound else row["spec_contract_multiplier"]
        cash = row["corp_deliverable_cash_twd"] if corporate_bound else 0.
        if not _positive(units) or cash is None or not math.isfinite(cash):
            continue
        prior_day = previous_day.get(day)
        event = (corporate_bound and prior_day is not None
            and prior_day < row["_event_date"] <= day)
        source_product = row["from_product"] if event else product
        previous = values.get((previous_day.get(day), source_product, month))
        if previous is not None and not event and previous["identity"] != identity:
            previous = None
        quote = row["valuation_price"]
        if _positive(quote):
            value = quote * units + cash
            if _positive(value):
                values[(day, product, month)] = dict(identity=identity, value=value,
                    seed_date=day, seed_identity=identity, seed_value=value)
            continue
        # A missing printable day's valuation is a different source failure.
        # Research marks must never turn it into a tradable source row.
        volume = row.get("outright_volume")
        if previous is None or _positive(volume) or row["cash_settlement"]:
            continue
        numerator = (row["carry_quantity_numerator"] or 1) if event else 1
        denominator = (row["carry_quantity_denominator"] or 1) if event else 1
        credit = (row["equity_cash_credit_twd"] or 0.) if event else 0.
        if (prior_day is not None and row.get("spec_effective_date") is not None
                and prior_day < row["spec_effective_date"] <= day
                and row.get("spec_carry_split_numerator") is not None
                and (row["spec_carry_split_numerator"], row["spec_carry_split_denominator"]) != (1, 1)):
            numerator, denominator = row["spec_carry_split_numerator"], row["spec_carry_split_denominator"]
        if not _positive(numerator) or not _positive(denominator):
            raise ValueError("research value cannot bypass an unresolved quantity conversion")
        value = (previous["value"] - credit) * denominator / numerator
        quote = (value - cash) / units
        if not _positive(value) or not _positive(quote):
            raise ValueError("frozen full value yields an invalid quote after verified cash/quantity conversion")
        record = {name: row[name] for name in ("date", "physical_instance")}
        record.update(valuation_price=quote, valuation_research_applied=True,
            valuation_research_method="frozen_previous_inventory_full_value",
            valuation_research_seed_date=previous["seed_date"],
            valuation_research_seed_identity=previous["seed_identity"],
            valuation_research_seed_value_twd=previous["seed_value"], valuation_research_value_twd=value,
            valuation_research_owner_extension=row["valuation_research_owner_extension"],
            valuation_research_terminal_assumption=row['valuation_research_terminal_assumption'])
        estimates.append(record)
        values[(day, product, month)] = dict(previous, identity=identity, value=value)
    schema = {"date": pl.Date, "physical_instance": pl.String, "valuation_price": pl.Float64,
              **VALUATION_RESEARCH_COLUMNS}
    audit = pl.DataFrame(estimates, schema=schema, strict=False)
    changes = ["valuation_price", *VALUATION_RESEARCH_COLUMNS]
    joined = physical.join(audit.rename({c: "_research_" + c for c in changes}),
        on=["date", "physical_instance"], how="left", validate="1:1")
    physical = joined.with_columns(*[pl.coalesce("_research_" + c, c).alias(c) for c in changes]).drop(
        ["_research_" + c for c in changes])
    return physical.sort("date", "physical_instance"), audit


def attach_research_valuation_provenance(frame: pl.DataFrame, rules: pl.DataFrame) -> pl.DataFrame:
    """Also disclose actual days whose opening reference uses a research mark."""
    keys = ["date", "physical_contract"]
    own = frame.select(*keys, *VALUATION_RESEARCH_COLUMNS)
    result = rules.join(own, on=keys, how="left", validate="1:1")
    prior = own.rename({"date": "carry_from_date", "physical_contract": "carry_from_physical_contract",
        **{c: "_prior_" + c for c in VALUATION_RESEARCH_COLUMNS}})
    result = result.join(prior, on=["carry_from_date", "carry_from_physical_contract"], how="left", validate="m:1")
    incoming = pl.col("_prior_valuation_research_applied").fill_null(False) & ~pl.col("valuation_research_applied")
    result = result.with_columns(
        (pl.col("valuation_research_applied") | incoming).alias("valuation_research_applied"),
        *[pl.when(incoming).then(pl.col("_prior_" + c)).otherwise(pl.col(c)).alias(c)
          for c in VALUATION_RESEARCH_COLUMNS if c not in ("valuation_research_applied", "valuation_research_method",
                                                        "valuation_research_owner_extension", "valuation_research_terminal_assumption")],
        pl.when(incoming).then(pl.lit("opening_reference_from_frozen_inventory_value"))
            .otherwise(pl.col("valuation_research_method")).alias("valuation_research_method"))
    return result.drop(["_prior_" + c for c in VALUATION_RESEARCH_COLUMNS])


def validate_research_valuation_rows(rules: pl.DataFrame) -> None:
    if set(VALUATION_RESEARCH_COLUMNS) - set(rules.columns):
        raise ValueError("research valuation release lacks per-row assumption provenance")
    for field in ("valuation_research_applied", "valuation_research_owner_extension", "valuation_research_terminal_assumption"):
        if rules[field].null_count():
            raise ValueError("null research valuation disclosure flag")
    assumed = rules.filter(pl.col("valuation_research_applied"))
    bad = (pl.col("valuation_research_seed_date").is_null()
        | (pl.col("valuation_research_seed_date") > pl.col("date"))
        | ((pl.col('valuation_research_seed_date') == pl.col('date')) & ~pl.col('valuation_research_terminal_assumption'))
        | pl.col("valuation_research_seed_identity").is_null()
        | ~pl.col("valuation_research_method").is_in([
            "frozen_previous_inventory_full_value", "opening_reference_from_frozen_inventory_value",
            "termination_cash_conversion_at_frozen_full_value"])
        | ~pl.col("valuation_research_seed_value_twd").is_finite()
        | (pl.col("valuation_research_seed_value_twd") <= 0)
        | ~pl.col("valuation_research_value_twd").is_finite()
        | (pl.col("valuation_research_value_twd") <= 0))
    if assumed.filter(bad.fill_null(True)).height:
        raise ValueError("invalid research valuation seed, value or method")
    terminal = rules.filter(pl.col('terminal_event') == 'research_cash_conversion') if 'terminal_event' in rules.columns else rules.head(0)
    if terminal.height and terminal.filter(~pl.col('valuation_research_terminal_assumption') | ~pl.col('valuation_research_applied')
        | (pl.col('terminal_contract_value_twd') - pl.col('settlement_contract_value_twd')).abs().gt(.005)
        | (pl.col('terminal_tax_twd') != 0)).height:
        raise ValueError('research cash conversion lacks its frozen-value assumption')


def apply_research_terminal_cash_conversions(frame, rules, flags, policy):
    """Convert unresolved inventory to cash only under the explicit research policy.

The event is deliberately distinct from official cash settlement and from a
market fill. Its value is the last book value, with no assumed trading tax.
"""
    keys = ['date', 'physical_contract']
    if not policy['terminal_cash_conversion_authorized']:
        return frame, rules, flags, rules.head(0)
    owners = rules.filter(pl.col('carry_from_physical_contract') != '').select(
        pl.col('carry_from_date').alias('date'),
        pl.col('carry_from_physical_contract').alias('physical_contract')).unique()
    broken = flags.filter(pl.col('lost_inventory_continuation')).select(keys)
    terminal = frame.join(broken, on=keys, how='semi').join(owners, on=keys, how='anti').filter(
        (pl.col('lifetime_status') == 'zero_open_interest_delisting') & (pl.col('open_interest') == 0)
        & pl.col('physical_contract').is_in(policy['physical_instances']))
    synthetic = frame.filter(pl.col('valuation_research_terminal_assumption')).join(owners, on=keys, how='anti')
    selected = pl.concat([terminal.select(keys), synthetic.select(keys)]).unique()
    audit = rules.join(selected, on=keys, how='semi')
    if audit.is_empty():
        return frame, rules, flags, audit
    if audit.filter(pl.col('terminal_event') != 'mark_only').height:
        raise ValueError('research cash conversion cannot overwrite an actual terminal event')
    updates = audit.select(*keys,
        pl.col('settlement_contract_value_twd').alias('_research_terminal_value'),
        pl.lit(True).alias('_research_terminal'))
    rules = rules.join(updates, on=keys, how='left', validate='1:1')
    applies = pl.col('_research_terminal').fill_null(False)
    rules = rules.with_columns(
        pl.when(applies).then(pl.lit('research_cash_conversion')).otherwise(pl.col('terminal_event')).alias('terminal_event'),
        pl.when(applies).then(pl.col('_research_terminal_value')).otherwise(pl.col('terminal_contract_value_twd')).alias('terminal_contract_value_twd'),
        pl.when(applies).then(0.).otherwise(pl.col('terminal_tax_twd')).alias('terminal_tax_twd'),
        (applies | pl.col('valuation_research_applied')).alias('valuation_research_applied'),
        (applies | pl.col('valuation_research_terminal_assumption')).alias('valuation_research_terminal_assumption'),
        pl.when(applies).then(pl.lit('termination_cash_conversion_at_frozen_full_value')).otherwise(pl.col('valuation_research_method')).alias('valuation_research_method'),
        pl.when(applies).then(pl.coalesce('valuation_research_seed_date', 'date')).otherwise(pl.col('valuation_research_seed_date')).alias('valuation_research_seed_date'),
        pl.when(applies).then(pl.coalesce('valuation_research_seed_identity', 'physical_contract')).otherwise(pl.col('valuation_research_seed_identity')).alias('valuation_research_seed_identity'),
        pl.when(applies).then(pl.coalesce('valuation_research_seed_value_twd', '_research_terminal_value')).otherwise(pl.col('valuation_research_seed_value_twd')).alias('valuation_research_seed_value_twd'),
        pl.when(applies).then(pl.col('_research_terminal_value')).otherwise(pl.col('valuation_research_value_twd')).alias('valuation_research_value_twd'),
    ).drop('_research_terminal_value','_research_terminal')
    audit = rules.join(selected,on=keys,how='semi')
    frame = frame.drop(list(VALUATION_RESEARCH_COLUMNS)).join(
        rules.select(*keys,*VALUATION_RESEARCH_COLUMNS),on=keys,how='left',validate='1:1').with_columns(
        *[pl.col(c).fill_null(False).alias(c) for c,t in VALUATION_RESEARCH_COLUMNS.items() if t==pl.Boolean])
    flags = flags.join(selected.with_columns(pl.lit(True).alias('_research_terminal')),on=keys,how='left',validate='1:1')
    flags = flags.with_columns(pl.when(pl.col('_research_terminal').fill_null(False)).then(False)
        .otherwise(pl.col('lost_inventory_continuation')).alias('lost_inventory_continuation')).drop('_research_terminal')
    return frame, rules, flags, audit
