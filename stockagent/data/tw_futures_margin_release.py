"""Compile the full TWD physical history into the canonical daily interface.

Market observations and executable accounting terms are different inputs. A
materialization retains *every* identity, including zero-print products. Only a
complete dated accounting tape can publish it as a training release.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import time

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_portfolio_daily import (
    FUTURES_MODEL_FEATURE_COLUMNS,
    TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
    _fixed_portfolio_slot_map,
    _stable_expiry_slot_map,
    add_futures_daily_market_features,
    calendar_expiry_date,
    futures_slot_layout_version,
)

MARGIN_MATERIALIZATION_VERSION = 2
CONTEXT_ONLY_PREFIX_CONTRACT = "proved_empty_inventory_context_prefix_v1"
WHOLE_CONTRACT_PREFIX_CONTRACT = "proved_whole_contract_empty_prefix_v2"
KEYS = ["date", "product", "contract"]


def omit_unreachable_account_prefix(frame: pl.DataFrame, rules: pl.DataFrame,
        blockers: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Keep source rows intact while omitting provably empty account prefixes.

    This changes account membership, never a market observation or a held
    contract value. Unknown origins and possible inventory remain admitted.
    Empty incoming edges are removed only with their exact source-day proof.
    The returned original rows form the reversible publication evidence.
    """
    from stockagent.data.tw_futures_execution_terms import inventory_entry_reachability
    keys = ["date", "physical_contract"]
    proof_fields = ["inventory_entry_reachable", "inventory_origin_unresolved"]
    for data in (frame, rules, blockers):
        if data.select(keys).is_duplicated().any():
            raise ValueError("context-only prefixes require unique physical days")
    for field in proof_fields:
        if field not in rules.columns or rules.schema[field] != pl.Boolean or rules[field].null_count():
            raise ValueError("context-only prefixes require complete Boolean inventory proof")
    if ("executable" not in frame.columns or frame.schema["executable"] != pl.Boolean
            or frame["executable"].null_count()):
        raise ValueError("context-only prefixes require explicit executable source masks")
    if (rules.join(frame.select(keys), on=keys, how="anti").height
            or rules.join(blockers.select(keys), on=keys, how="anti").height):
        raise ValueError("context-only proof lacks its source rows or diagnostics")
    expected = inventory_entry_reachability(frame, rules)
    if not expected.equals(rules.select(*keys, "inventory_entry_reachable").sort(keys)):
        raise ValueError("context-only inventory proof disagrees with causal entries and carry edges")
    checked = rules.join(frame.select(*keys, "executable"), on=keys, validate="1:1")
    empty = checked.filter(~pl.col("inventory_entry_reachable")
        & ~pl.col("inventory_origin_unresolved") & ~pl.col("executable")).select(rules.columns)
    # A held interval can never become an empty prefix again. Generation
    # identity is explicit, so reused quoted codes cannot erase prior owners.
    transitions = checked.sort(keys).with_columns(
        pl.col("inventory_entry_reachable").cast(pl.Int64).cum_sum()
        .over("physical_contract").alias("_ever_reachable"))
    if transitions.join(empty.select(keys), on=keys, how="semi").filter(
            pl.col("_ever_reachable") > 0).height:
        raise ValueError("context-only omission is not a pre-inventory prefix")
    kept = rules.join(empty.select(keys), on=keys, how="anti")
    origins = empty.select(pl.col("date").alias("carry_from_date"),
                           pl.col("physical_contract").alias("carry_from_physical_contract"))
    suppressed = kept.join(origins, on=["carry_from_date", "carry_from_physical_contract"], how="semi")
    marker = suppressed.select(keys).with_columns(pl.lit(True).alias("_empty_origin"))
    kept = kept.join(marker, on=keys, how="left", validate="1:1").with_columns(
        pl.when(pl.col("_empty_origin").fill_null(False)).then(pl.lit(""))
          .otherwise(pl.col("carry_from_physical_contract")).alias("carry_from_physical_contract"),
        pl.when(pl.col("_empty_origin").fill_null(False)).then(pl.lit(None, dtype=pl.Date))
          .otherwise(pl.col("carry_from_date")).alias("carry_from_date"),
        pl.when(pl.col("_empty_origin").fill_null(False)).then(0.)
          .otherwise(pl.col("carry_cash_twd")).alias("carry_cash_twd"),
        *[pl.when(pl.col("_empty_origin").fill_null(False)).then(pl.lit(1, dtype=rules.schema[c]))
          .otherwise(pl.col(c)).alias(c) for c in ("carry_quantity_numerator", "carry_quantity_denominator")],
    ).drop("_empty_origin").select(rules.columns).sort(keys)
    expected_kept = inventory_entry_reachability(frame, kept)
    if not expected_kept.equals(kept.select(*keys, "inventory_entry_reachable").sort(keys)):
        raise ValueError("empty-prefix omission changes possible retained inventory")
    marked = blockers.join(empty.select(keys).with_columns(pl.lit(True).alias("_empty_prefix")),
        on=keys, how="left", validate="1:1").with_columns(
            (pl.col("is_warmup") | pl.col("_empty_prefix").fill_null(False)).alias("is_warmup"))
    return kept, marked.drop("_empty_prefix"), empty.sort(keys), suppressed.sort(keys)


def validate_context_only_prefix_sources(frame: pl.DataFrame, rules: pl.DataFrame,
        manifest: dict, root: Path) -> pl.DataFrame:
    """Recompute reversible empty-prefix evidence before accepting omissions."""
    proof = manifest.get("context_only_prefix")
    if not isinstance(proof, dict) or proof.get("contract") != CONTEXT_ONLY_PREFIX_CONTRACT:
        raise ValueError("intermediate context rows require explicit empty-prefix admission")
    receipts = {r["path"]: r["sha256"] for r in manifest.get("sources", [])}
    loaded = []
    for key in ("excluded_rules", "suppressed_carries"):
        item = proof.get(key)
        if not isinstance(item, dict):
            raise ValueError("context-only admission lacks reversible source rules")
        path = Path(item["path"])
        if (path.is_absolute() or ".." in path.parts or receipts.get(str(path)) != item.get("sha256")
                or sha256_file(root / path) != item.get("sha256")):
            raise ValueError("context-only original rules must be receipt-bound")
        loaded.append(pl.read_parquet(root / path))
    empty, suppressed = loaded
    keys = ["date", "physical_contract"]
    unproved = frame.join(pl.concat([rules.select(keys), empty.select(keys)]),
                          on=keys, how="anti")
    if "previous_symbol_date" in frame.columns:
        intermediate = unproved.filter(pl.col("previous_symbol_date").is_not_null())
    else:
        first = frame.group_by("physical_contract").agg(pl.col("date").min().alias("_first_date"))
        intermediate = unproved.join(first, on="physical_contract", validate="m:1").filter(
            pl.col("date") != pl.col("_first_date"))
    if intermediate.height:
        raise ValueError("context-only evidence leaves an unproved intermediate account day")
    for data in loaded:
        if data.schema != rules.schema or data.select(keys).is_duplicated().any():
            raise ValueError("context-only original rules have an ambiguous schema or identity")
    if (empty.join(rules.select(keys), on=keys, how="semi").height
            or suppressed.join(rules.select(keys), on=keys, how="anti").height):
        raise ValueError("context-only source rules differ from admitted membership")
    original = pl.concat([rules.join(suppressed.select(keys), on=keys, how="anti"), suppressed, empty])
    diagnostics = original.select(keys).with_columns(pl.lit(False).alias("is_warmup"))
    recomputed, _, expected_empty, expected_suppressed = omit_unreachable_account_prefix(frame, original, diagnostics)
    if (not recomputed.sort(keys).equals(rules.sort(keys))
            or not expected_empty.equals(empty.sort(keys))
            or not expected_suppressed.equals(suppressed.sort(keys))
            or proof.get("rows") != empty.height):
        raise ValueError("context-only proof cannot reproduce the admitted account")
    return empty.select(keys)


def whole_contract_entry_frame(frame: pl.DataFrame, maximum_volume_participation: float) -> pl.DataFrame:
    """A proof-only view of possible entries, leaving the source frame intact."""
    from stockagent.data.tw_futures_entry_capacity import whole_contract_trade_capacity
    required = {"previous_volume", "same_contract_as_previous_session", "executable"}
    if required - set(frame.columns):
        raise ValueError("whole-contract proof requires preceding same-contract volume and identity")
    for field in ("same_contract_as_previous_session", "executable"):
        if frame.schema[field] != pl.Boolean or frame[field].null_count():
            raise ValueError("whole-contract entry identity and execution must be explicit Booleans")
    capacity = whole_contract_trade_capacity(frame["previous_volume"].to_numpy(),
        maximum_volume_participation).astype("float32")
    return frame.with_columns((pl.col("executable") & pl.col("same_contract_as_previous_session")
        & pl.Series("_whole_contract_capacity", capacity).ge(1.)).alias("executable"))


def omit_whole_contract_empty_prefix(frame: pl.DataFrame, rules: pl.DataFrame,
        blockers: pl.DataFrame, *, maximum_volume_participation: float = 1.):
    """Use the canonical entry cap as an upper bound, preserving all held days.

    The bound is part of the publication ABI. Orders above it must be rejected
    by the loader; this is not a price imputation or a zero-OI retirement rule.
    """
    from stockagent.data.tw_futures_execution_terms import inventory_entry_reachability
    shadow = whole_contract_entry_frame(frame, maximum_volume_participation)
    reachability = inventory_entry_reachability(shadow, rules)
    checked = rules.drop("inventory_entry_reachable").join(reachability,
        on=["date", "physical_contract"], validate="1:1").select(rules.columns)
    return omit_unreachable_account_prefix(shadow, checked, blockers)


def validate_whole_contract_prefix_sources(frame: pl.DataFrame, rules: pl.DataFrame,
        manifest: dict, root: Path) -> pl.DataFrame:
    """Recompute the admitted entry bound and the original carry-graph proof."""
    proof = manifest.get("context_only_prefix", {})
    maximum = proof.get("maximum_volume_participation")
    if (proof.get("contract") != WHOLE_CONTRACT_PREFIX_CONTRACT or isinstance(maximum, bool)
            or not isinstance(maximum, (int, float))):
        raise ValueError("whole-contract empty-prefix admission requires its numeric entry bound")
    shadow = whole_contract_entry_frame(frame, maximum)
    legacy_proof = dict(proof, contract=CONTEXT_ONLY_PREFIX_CONTRACT)
    return validate_context_only_prefix_sources(shadow, rules,
        dict(manifest, context_only_prefix=legacy_proof), root)


def validate_admitted_context_prefix(frame: pl.DataFrame, rules: pl.DataFrame,
        manifest: dict, root: Path) -> pl.DataFrame:
    if manifest.get("context_only_prefix", {}).get("contract") == WHOLE_CONTRACT_PREFIX_CONTRACT:
        return validate_whole_contract_prefix_sources(frame, rules, manifest, root)
    return validate_context_only_prefix_sources(frame, rules, manifest, root)


def select_complete_margin_components(frame: pl.DataFrame, rules: pl.DataFrame,
        blockers: pl.DataFrame, *, start, end, context_only_keys=None) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Select explicitly disclosed, complete inventory components for research.

    A corporate conversion joins its source and destination lifetimes. One bad
    accounting day excludes the entire component, including its predecessors.
    This is retrospective source-quality selection, not historical eligibility.
    Neither observations nor prices are repaired by this operation.
    """
    keys = ["date", "physical_contract"]
    for data in (frame, rules, blockers):
        if data.select(keys).is_duplicated().any():
            raise ValueError("component selection requires unique physical days")
    if frame.select(keys).join(blockers.select(keys), on=keys, how="anti").height:
        raise ValueError("component selection lacks accounting diagnostics")
    identities = sorted(frame["physical_contract"].unique())
    parents = {identity: identity for identity in identities}

    def root(identity):
        while parents[identity] != identity:
            parents[identity] = parents[parents[identity]]
            identity = parents[identity]
        return identity

    edges = rules.filter((pl.col("carry_from_physical_contract") != "")
        & (pl.col("carry_from_physical_contract") != pl.col("physical_contract"))).select(
        "physical_contract", "carry_from_physical_contract").unique()
    missing_origins = set()
    for destination, origin in edges.iter_rows():
        if destination not in parents:
            raise ValueError("inventory destination is outside the source history")
        if origin not in parents:
            missing_origins.add(destination)
        else:
            parents[root(destination)] = root(origin)
    mapping = pl.DataFrame({"physical_contract": identities,
        "inventory_component": [root(identity) for identity in identities]})
    bad = blockers.group_by("physical_contract").agg(
        (pl.col("has_blocker").fill_null(True) & ~pl.col("is_warmup")).any()
        .alias("unresolved_accounting"),
        (~pl.col("is_warmup")).any().alias("has_account_days"))
    coverage = frame.group_by("physical_contract", "product").agg(
        pl.col("date").min().alias("start"), pl.col("date").max().alias("end"),
        pl.len().alias("source_rows"), pl.col("lifetime_status").first())
    coverage = coverage.join(mapping, on="physical_contract", validate="1:1").join(
        bad, on="physical_contract", validate="1:1").with_columns(
        pl.col("physical_contract").is_in(sorted(missing_origins)).alias("missing_component_origin"),
        ((pl.col("start") >= start) & (pl.col("start") <= end)).alias("within_requested_start"))
    allowed_status = ["official_final", "observed_at_dataset_boundary", "corporate_transfer_candidate",
                      "derived_same_security_final", "zero_open_interest_delisting"]
    # Validity is closed over every member, not just the quoted destination.
    coverage = coverage.with_columns((~pl.col("unresolved_accounting")
        & ~pl.col("missing_component_origin") & pl.col("within_requested_start")
        & pl.col("has_account_days")
        & pl.col("lifetime_status").is_in(allowed_status)).all().over("inventory_component")
        .alias("selected"))
    selected = frame.join(coverage.filter(pl.col("selected")).select("physical_contract"),
        on="physical_contract", how="semi").filter(pl.col("date") <= end)
    if context_only_keys is not None:
        # A selected destination may have a suppressed carry from a proven
        # empty lifetime. Keep those source rows so admission can independently
        # reproduce the empty-inventory proof instead of losing its origin.
        context_only_keys = context_only_keys.select(keys)
        if (context_only_keys.is_duplicated().any()
                or context_only_keys.join(frame.select(keys),on=keys,how='anti').height
                or context_only_keys.join(blockers.filter(pl.col('is_warmup')).select(keys),
                    on=keys,how='anti').height):
            raise ValueError('component context requires uniquely proved warmup source days')
        context = frame.join(context_only_keys,on=keys,how='semi').filter(pl.col('date')<=end)
        selected = pl.concat([selected,context.join(selected.select(keys),on=keys,how='anti')])
        retained = context.group_by('physical_contract').agg(pl.len().alias('context_only_rows'))
        coverage = coverage.join(retained,on='physical_contract',how='left',validate='1:1').with_columns(
            pl.col('context_only_rows').fill_null(0))
    if selected.is_empty():
        raise ValueError("no complete inventory components in the requested source-quality scope")
    # The explicit dataset endpoint is marked, never an invented close fill.
    selected = selected.with_columns(pl.when(pl.col("next_market_date") > end).then(None)
        .otherwise(pl.col("next_market_date")).alias("next_market_date"))
    chosen = rules.join(selected.select(keys), on=keys, how="semi")
    validate_accounting_continuation(selected.join(chosen.select(keys), on=keys, how="semi"), chosen)
    return selected, chosen, coverage.sort("product", "start", "physical_contract")


def select_complete_standard_stock_lives(frame: pl.DataFrame, rules: pl.DataFrame,
        blockers: pl.DataFrame, *, start, end) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Explicit research scope, never an inferred historical tradability mask.

    Select whole observed lifetimes with complete accounting and no corporate
    transfers. An incomplete middle day removes the entire lifetime, not only
    its losing/unpriced day. End-of-data inventory remains marked. The source
    admission review is a separate required step before publication.
    """
    bad = blockers.group_by('physical_contract').agg(
        (pl.col('has_blocker') & ~pl.col('is_warmup')).any().alias('unresolved_rule'))
    risk = rules.group_by('physical_contract').agg(
        ((pl.col('carry_from_physical_contract') != '') &
         (pl.col('carry_from_physical_contract') != pl.col('physical_contract'))).any().alias('cross_identity'),
        (pl.col('carry_cash_twd') != 0).any().alias('corporate_cash'),
        (pl.col('carry_quantity_numerator') != pl.col('carry_quantity_denominator')).any().alias('quantity_change'))
    values = frame.select('date','physical_contract','open','settlement').join(
        rules.select('date','physical_contract','contract_multiplier','opening_contract_value_twd',
                     'settlement_contract_value_twd'), on=['date','physical_contract'], how='inner')
    values = values.group_by('physical_contract').agg(pl.any_horizontal(
        ((pl.col(phase+'_contract_value_twd')-pl.col(price)*pl.col('contract_multiplier')).abs()>.005)
        .fill_null(True) for phase,price in [('opening','open'),('settlement','settlement')]
    ).any().alias('nonstandard_value'))
    coverage = frame.group_by('physical_contract', 'product', 'asset_class').agg(
        pl.col('date').min().alias('start'), pl.col('date').max().alias('end'),
        pl.col('lifetime_status').first()).join(bad, on='physical_contract').join(risk, on='physical_contract', how='left')
    coverage = coverage.join(values, on='physical_contract', how='left')
    accepted = ((pl.col('asset_class') == 'stock_future') & ~pl.col('product').str.contains(r'\d$')
        & (pl.col('start') >= start) & (pl.col('start') <= end)
        & ~pl.col('unresolved_rule') & ~pl.col('cross_identity').fill_null(True)
        & ~pl.col('corporate_cash').fill_null(True) & ~pl.col('quantity_change').fill_null(True)
        & ~pl.col('nonstandard_value').fill_null(True)
        & pl.col('lifetime_status').is_in(['official_final', 'observed_at_dataset_boundary']))
    coverage = coverage.with_columns(accepted.alias('selected'))
    ids = coverage.filter(pl.col('selected')).select('physical_contract')
    selected = frame.join(ids, on='physical_contract', how='semi').filter(pl.col('date') <= end)
    if not selected.height:
        raise ValueError('no complete standard stock-futures lifetimes in requested scope')
    # The explicit endpoint is mark-only; do not pretend a market-close fill.
    selected = selected.with_columns(pl.when(pl.col('next_market_date') > end)
        .then(None).otherwise(pl.col('next_market_date')).alias('next_market_date'))
    selected_rules = rules.join(selected.select('date', 'physical_contract'),
                                on=['date', 'physical_contract'], how='semi')
    validate_accounting_continuation(selected.join(selected_rules.select('date', 'physical_contract'),
        on=['date', 'physical_contract'], how='semi'), selected_rules)
    return selected, selected_rules, coverage


def read_bound_output(path: Path, *, output_key: str | None = None,
                      columns: list[str] | None = None,
                      predicate: pl.Expr | None = None) -> tuple[pl.DataFrame, dict]:
    """Verify the whole source, then read the requested repair coordinates.

    The returned manifest still describes the complete immutable source.
    Filtering cannot hide corruption outside the selected repair rows.
    """
    manifest = json.loads(path.with_name("manifest.json").read_text())
    expected = manifest.get("outputs", {}).get(output_key or path.name, {}).get("sha256")
    if expected != sha256_file(path):
        raise ValueError(f"materialization input SHA mismatch: {path}")
    if path.suffix == ".csv":
        frame = pl.scan_csv(path, infer_schema=False)
    else:
        frame = pl.scan_parquet(path)
    if predicate is not None:
        frame = frame.filter(predicate)
    if columns is not None:
        frame = frame.select(columns)
    return frame.collect(), manifest


def materialize_margin_market_rows(
    physical: pl.DataFrame, observations: pl.DataFrame, universe: pl.DataFrame,
    *, slot_count: int = 2816, market_dates: pl.DataFrame | None = None,
    portfolio_lifetimes: pl.DataFrame | None = None,
    portfolio_universe: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Preserve identities and marks without inventing executable prices/rules.

    Null source prices are deliberately retained for admission to diagnose. In
    particular no forward-fill may conceal an intermediate missing settlement.
    A non-executable row uses only the immediately previous official mark as
    its opening valuation. Missing volume remains NULL, with no fill permission.
    """
    futures_slot_layout_version(slot_count)
    if universe["product"].is_duplicated().any() or universe["product"].null_count():
        raise ValueError("duplicate/null requested product identity")
    if set(universe["settlement_currency"]) != {"TWD"}:
        raise ValueError("the requested universe must be exclusively TWD")
    if set(physical["product"]) != set(universe["product"]):
        raise ValueError("physical history must cover exactly every requested TWD product")
    if physical.select(KEYS).is_duplicated().any():
        raise ValueError("ambiguous physical identity on one product/contract/day")
    if physical.select("date", "physical_instance").is_duplicated().any():
        raise ValueError("duplicate physical instance day")
    raw = observations.filter(
        (pl.col("session") == "一般") & pl.col("product").is_in(universe["product"].to_list())
        & pl.col("contract").str.contains(r"^\d{6}(?:W[1-5])?$")
    )
    if raw.select(KEYS).is_duplicated().any():
        raise ValueError("duplicate observed general-session contract-day")
    if raw.select(KEYS).join(physical.select(KEYS), on=KEYS, how="anti").height:
        raise ValueError("physical history drops requested observed contract-days")
    calendar_source = physical.select("date").unique() if market_dates is None else market_dates.select("date")
    if (calendar_source['date'].null_count() or calendar_source['date'].is_duplicated().any()
            or physical.select('date').unique().join(calendar_source,on='date',how='anti').height):
        raise ValueError('repair calendar must uniquely cover every physical date')
    calendar = calendar_source.sort("date").with_columns(
        pl.col("date").shift(1).alias("previous_market_date"),
        pl.col("date").shift(-1).alias("next_market_date"),
    )
    lives = physical.group_by("product", "contract", "physical_instance").agg(
        pl.col("date").min().alias("first_observed_date"),
        pl.col("date").max().alias("last_observed_date"),
    )
    full_universe = universe if portfolio_universe is None else portfolio_universe
    slot_lives = lives if portfolio_lifetimes is None else portfolio_lifetimes
    if (portfolio_lifetimes is None) != (portfolio_universe is None):
        raise ValueError("scoped slots require the complete portfolio universe and lifetimes together")
    if portfolio_lifetimes is not None:
        if (full_universe["product"].null_count() or full_universe["product"].is_duplicated().any()
                or set(full_universe["settlement_currency"]) != {"TWD"}
                or set(slot_lives["product"]) != set(full_universe["product"])):
            raise ValueError("scoped slots have an incomplete or ambiguous parent universe")
        order = ["product", "contract", "physical_instance"]
        own_lives = slot_lives.filter(pl.col("product").is_in(universe["product"].to_list())).select(lives.columns)
        if not own_lives.sort(order).equals(lives.sort(order)):
            raise ValueError("scoped slots do not describe the actual repaired lifetimes")
        if not universe.sort("product").equals(full_universe.filter(
                pl.col("product").is_in(universe["product"].to_list())).select(universe.columns).sort("product")):
            raise ValueError("scoped universe differs from its complete portfolio parent")
    slots = _fixed_portfolio_slot_map(slot_lives, calendar["date"].to_list(), fixed_slot_count=slot_count)
    slots = slots.filter(pl.col("product").is_in(universe["product"].to_list()))
    # The quoted product/month is not a lifetime identity: adjusted codes can
    # be reused. Do not shift previous marks across their separate generations.
    frame = physical.rename({"physical_contract": "quoted_physical_contract"}).with_columns(
        pl.col("physical_instance").alias("physical_contract")
    ).join(slots, on=["product", "contract", "physical_instance"], validate="m:1")
    optional = [c for c in ("open_interest", "last_bid", "last_ask") if c in raw.columns]
    frame = frame.join(raw.select(*KEYS, *optional, pl.lit(True).alias("official_row_observed")),
                       on=KEYS, how="left", validate="1:1")
    for column in ("last_bid", "last_ask", "open_interest"):
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    frame = frame.join(calendar, on="date", validate="m:1").join(
        universe.select("product", "underlying_symbol", "region"), on="product", validate="m:1",
    ).sort("physical_contract", "date").with_columns(
        pl.col("official_row_observed").fill_null(False),
        *[pl.col("official_" + c).cast(pl.String).str.replace_all(",", "")
          .cast(pl.Float64, strict=False).alias("observed_" + c) for c in ("open", "high", "low", "close")],
        pl.col("valuation_price").alias("settlement"),
        pl.col("outright_volume").cast(pl.Float64).alias("volume"),
        pl.col("spread_leg_volume").cast(pl.Float64).alias("spread_order_volume"),
        *[pl.col(c).cast(pl.Float64, strict=False) for c in ("last_bid", "last_ask", "open_interest")],
    ).with_columns(
        ((pl.col("volume") > 0) & pl.col("observed_open").is_finite() & (pl.col("observed_open") > 0)
         & pl.col("observed_close").is_finite() & (pl.col("observed_close") > 0)).fill_null(False).alias("executable"),
        pl.col("date").shift(1).over("physical_contract").alias("previous_symbol_date"),
        pl.col("settlement").shift(1).over("physical_contract").alias("previous_settlement"),
        pl.col("volume").shift(1).over("physical_contract").alias("previous_volume"),
        pl.col("open_interest").shift(1).over("physical_contract").alias("previous_open_interest"),
        pl.col("date").shift(-1).over("physical_contract").alias("next_symbol_date"),
    ).with_columns(
        pl.col("executable").alias("source_row_observed"),
        (pl.col("previous_symbol_date") == pl.col("previous_market_date")).fill_null(False)
        .alias("same_contract_as_previous_session"),
        # Missing quotes/volume prohibit fills, not valuation of old inventory.
        # Keep the missing source fields intact; never synthesize market volume.
        pl.when(pl.col("executable")).then(pl.col("observed_open"))
        .otherwise(pl.col("previous_settlement")).alias("open"),
        pl.when(pl.col("executable")).then(pl.col("observed_close"))
        .otherwise(pl.col("settlement")).alias("close"),
        pl.col("observed_high").alias("high"), pl.col("observed_low").alias("low"),
    )
    tenors = pl.DataFrame({"contract": sorted(frame["contract"].unique())}).with_columns(
        pl.col("contract").map_elements(calendar_expiry_date, return_dtype=pl.Date).alias("tenor_sort_date"),
        pl.col("contract").str.slice(4, 2).cast(pl.Int16).alias("delivery_month"),
        pl.col("contract").str.extract(r"W([1-5])$", 1).cast(pl.Int16).fill_null(0).alias("delivery_week"),
    )
    delivery = _stable_expiry_slot_map(lives.join(tenors.select("contract", "tenor_sort_date"),
        on="contract", validate="m:1").with_columns(pl.col("tenor_sort_date").alias("resolved_last_trade_date")))
    ids = {p: i for i, p in enumerate(sorted(full_universe["product"].to_list()), start=1)}
    frame = frame.join(tenors, on="contract", validate="m:1").join(
        delivery.select("product", "contract", "physical_instance", "expiry_slot_lane"),
        on=["product", "contract", "physical_instance"], validate="m:1",
    ).sort("date", "product", "tenor_sort_date", "contract").with_columns(
        pl.col("product").replace_strict(ids, return_dtype=pl.UInt16).alias("taifex_product_id"),
        pl.int_range(1, pl.len() + 1).over("date", "product").alias("tenor_rank"),
        pl.when(pl.col("delivery_week") > 0).then(pl.lit("weekly")).otherwise(pl.lit("monthly")).alias("series_type"),
        (~pl.col("same_contract_as_previous_session")).alias("lifecycle_reset"),
        (pl.col("tenor_sort_date") - pl.col("date")).dt.total_days().clip(lower_bound=0).alias("calendar_days_to_tenor_key"),
        pl.col("open").alias("valuation_open"), pl.col("settlement").alias("valuation_settlement"),
        (pl.col("next_symbol_date") == pl.col("next_market_date")).fill_null(False).alias("can_hold_overnight"),
        pl.col("cash_settlement").alias("must_liquidate"),
        pl.when(pl.col("cash_settlement")).then(pl.lit("last_trade_date"))
        .when(pl.col("next_market_date").is_null()).then(pl.lit("dataset_boundary_mark_only"))
        .when(pl.col("next_symbol_date").is_null()).then(pl.col("lifetime_status"))
        .otherwise(pl.lit("carry_same_contract")).alias("liquidation_reason"),
        (pl.col("settlement") / pl.col("open")).log().alias("holding_log_return"),
        # Full dated values come from the accounting tape, never today's master.
        pl.lit(None, dtype=pl.Float64).alias("contract_multiplier"),
        pl.lit("standard").alias("sinopac_network_fee_group"),
    )
    if frame.select("date", "symbol").is_duplicated().any():
        raise ValueError("fixed-slot collision between physical lifetimes")
    frame = add_futures_daily_market_features(frame)
    return frame.sort("date", "symbol"), slots


def margin_execution_dependency_rows(frame: pl.DataFrame, margins: pl.DataFrame,
                                     positions: pl.DataFrame, terms: pl.DataFrame,
                                     terminal: pl.DataFrame) -> pl.DataFrame:
    """One row per actual contract-day; counts never substitute for admission."""
    keys = [*KEYS, "physical_contract", "symbol"]
    accepted = ["bound_prior_publication", "bound_same_security_rate"]
    result = frame.select(*keys, "volume", "open", "close", "observed_open", "observed_close", "settlement", "lifetime_status",
                          "same_contract_as_previous_session", "previous_symbol_date", "previous_settlement",
                          "cash_settlement", "asset_class", "final_settlement_value").join(
        margins.select("date", "product", "opening_binding_status", "settlement_binding_status"),
        on=["date", "product"], how="left", validate="m:1",
    ).join(positions.select(*KEYS, "position_numeric_inputs_resolved"), on=KEYS, how="left", validate="1:1")
    result = result.join(terms.select(*KEYS, "terms_binding_status", "subscription_rights_at_final_settlement"),
                         on=KEYS, how="left", validate="1:1").join(
        terminal.select(*KEYS, "terminal_value_input_twd"), on=KEYS, how="left", validate="1:1",
    )
    positive = lambda name: (pl.col(name).is_finite() & (pl.col(name) > 0)).fill_null(False)
    adjusted = pl.col("product").str.contains(r"\d$")
    return result.with_columns(
        (~(pl.col("opening_binding_status").is_in(accepted)
           & pl.col("settlement_binding_status").is_in(accepted))).fill_null(True).alias("missing_margin"),
        (~pl.col("position_numeric_inputs_resolved")).fill_null(True).alias("missing_position"),
        (adjusted & (pl.col("terms_binding_status") != "bound_prior_publication").fill_null(True)).alias("missing_adjusted_terms"),
        (pl.col("cash_settlement") & adjusted & ~positive("terminal_value_input_twd")).alias("missing_terminal_value"),
        (~positive("settlement")).alias("missing_valuation"),
        ((pl.col("volume") > 0) & (~positive("observed_open") | ~positive("observed_close"))).fill_null(False).alias("unavailable_execution_price"),
        (pl.col("previous_symbol_date").is_not_null() & ~pl.col("same_contract_as_previous_session")).alias("intermediate_calendar_gap"),
        pl.col("volume").is_null().alias("unreported_volume_no_fill"),
        (~pl.col("lifetime_status").is_in(["official_final", "observed_at_dataset_boundary", "zero_open_interest_delisting",
                                         "corporate_transfer_candidate"])).alias("unresolved_lifetime"),
        # The remaining work is a joined execution tape, not another OCR pass.
        pl.lit(True).alias("requires_dated_execution_terms"),
        pl.lit(False).alias("training_admitted"),
    )


def materialize_all_twd_margin_inputs(*, physical_history: Path, raw_daily: Path,
        product_universe: Path, margin_inputs: Path, output: Path, slot_count: int = 2816) -> dict:
    started = time.monotonic()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("use a new empty materialization directory")
    builder_sha = sha256_file(Path(__file__))
    paths = [physical_history / "physical_daily_marks.parquet", raw_daily, product_universe]
    paths += [margin_inputs / (name + ".parquet") for name in (
        "product_day_margin_inputs", "contract_day_position_inputs",
        "adjusted_contract_day_terms", "adjusted_terminal_value_inputs")]
    frames = [read_bound_output(path)[0] for path in paths]
    physical, raw, universe, margins, positions, terms, terminal = frames
    input_manifest = json.loads((margin_inputs / 'manifest.json').read_text())
    bound_hashes = set(input_manifest.get('source_sha256s', {}).values())
    if any(sha256_file(path) not in bound_hashes for path in (paths[0], product_universe)):
        raise ValueError('numeric inputs belong to a different physical history or universe')
    print(json.dumps(dict(stage='sources_verified', products=universe.height, physical_rows=physical.height)), flush=True)
    frame, slots = materialize_margin_market_rows(physical, raw, universe, slot_count=slot_count)
    print(json.dumps(dict(stage='market_rows_compiled', rows=frame.height, slots=slots.height)), flush=True)
    dependencies = margin_execution_dependency_rows(frame, margins, positions, terms, terminal)
    flags = [c for c in dependencies.columns if c.startswith(("missing_", "unresolved_"))
             or c in ("intermediate_calendar_gap", "unavailable_execution_price", "unreported_volume_no_fill")]
    coverage = dependencies.group_by("product").agg(
        pl.len().alias("physical_contract_days"), pl.col("date").min().alias("start"),
        pl.col("date").max().alias("end"), (pl.col("volume") > 0).sum().alias("positive_print_days"),
        *[pl.col(c).sum() for c in flags],
    ).join(universe.select("product", "product_name", "asset_class"), on="product", validate="1:1").sort("product")
    output.mkdir(parents=True)
    atomic_write_parquet(output / "continuous_daily.parquet", frame)
    atomic_write_parquet(output / "execution_dependencies.parquet", dependencies)
    atomic_write_parquet(output / "slot_map.parquet", slots)
    coverage.write_csv(output / "product_execution_coverage.csv")
    universe.write_csv(output / "products.csv")
    outputs = {p.name: {"sha256": sha256_file(p), "bytes": p.stat().st_size} for p in output.iterdir() if p.is_file()}
    outputs["continuous_daily"] = outputs["continuous_daily.parquet"]
    summary = dict(dataset="taifex_futures_portfolio_daily", materialization_version=MARGIN_MATERIALIZATION_VERSION,
        status="accounting_admission_required", all_products_execution_ready=False,
        all_products_training_ready=False, point_in_time_verified=False,
        contract_version=futures_slot_layout_version(slot_count),
        feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        fixed_model_output_slots=slot_count, slot_reuse_cooldown_sessions=31,
        maximum_safe_lookback_sessions=32, products=universe.height, rows=frame.height,
        physical_lifetimes=slots.height, used_slots=int(slots["portfolio_slot"].max()),
        date_start=str(frame["date"].min()), date_end=str(frame["date"].max()),
        requested_products=sorted(universe["product"].to_list()),
        zero_print_products=coverage.filter(pl.col("positive_print_days") == 0)["product"].to_list(),
        dependency_counts={c: int(dependencies[c].sum()) for c in flags},
        model_feature_columns=list(FUTURES_MODEL_FEATURE_COLUMNS),
        execution_contract="prior_completed_features_official_daily_open_proxy_and_daily_settlement_marks",
        boundary_policy="snapshot_mark_only_never_synthetic_liquidation",
        identity_policy="physical_instance_not_reused_product_month",
        source_sha256s={str(p): sha256_file(p) for p in paths}, outputs=outputs,
        builder_sha256=builder_sha, elapsed_s=time.monotonic() - started)
    atomic_write_json(output / "manifest.json", summary)
    return summary


def build_all_twd_execution_terms(*, materialization: Path, margin_inputs: Path,
        rule_candidates: Path, specifications: Path | None, output: Path,
        position_research_policy: Path | None = None) -> dict:
    """Run the real compiler over every requested row and retain its worklist.

    A missing specification file is a diagnosable input gap, not permission to
    copy today's contract master into historical rows. Incomplete builds exit
    through the CLI with code 2 and cannot enter the release publisher.
    """
    from stockagent.data.tw_futures_execution_terms import (
        EXECUTION_TERMS_COMPILER_VERSION, SPEC_FIELDS, compile_execution_terms,
        MARGIN_INPUT_FIELDS, POSITION_INPUT_FIELDS, POSITION_INPUT_EXTENSIONS,
    )
    from stockagent.data.tw_futures_margin import (
        validate_margin_carry_rules, validate_margin_value_bases,
        validate_margin_second_position_limit, validate_margin_grandfather_rules,
    )
    started = time.monotonic()
    compiler_path = Path(__file__).with_name("tw_futures_execution_terms.py")
    compiler_sha = sha256_file(compiler_path)
    policy = None
    if position_research_policy is not None:
        from stockagent.data.tw_futures_position_research import validate_position_research_policy
        policy = validate_position_research_policy(json.loads(position_research_policy.read_text()))
        policy_sha = sha256_file(position_research_policy)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("use a new empty terms output directory")
    columns = ["date", "product", "contract", "physical_contract", "previous_market_date",
        "next_market_date", "previous_symbol_date", "open", "close", "settlement", "volume",
        "executable", "cash_settlement", "official_expiry", "lifetime_status"]
    daily_path = materialization / "continuous_daily.parquet"
    frame, parent = read_bound_output(daily_path, columns=columns)
    dependency_columns = ["date", "physical_contract", "missing_valuation", "intermediate_calendar_gap", "unresolved_lifetime"]
    dependencies, _ = read_bound_output(materialization / "execution_dependencies.parquet", columns=dependency_columns)
    wanted = set(parent["requested_products"])
    if set(frame["product"].unique()) != wanted:
        raise ValueError("terms compiler requires the complete declared universe")
    paths = [margin_inputs / (name + ".parquet") for name in (
        "product_day_margin_inputs", "contract_day_position_inputs", "adjusted_terminal_value_inputs")]
    paths += [rule_candidates / (name + ".parquet") for name in (
        "margin_level_intervals", "corporate_terms_intervals")]
    projections = [["date", "product", *MARGIN_INPUT_FIELDS],
                   [*KEYS, *POSITION_INPUT_FIELDS, *[c for c in POSITION_INPUT_EXTENSIONS
                       if c in pl.read_parquet_schema(paths[1])]],
                   [*KEYS, "terminal_value_input_twd"], None, None]
    inputs = [read_bound_output(p, columns=columns)[0] for p, columns in zip(paths, projections)]
    margin_manifest = json.loads((margin_inputs / "manifest.json").read_text())
    if any(margin_manifest["source_sha256s"].get(str(p)) != sha256_file(p) for p in paths[-2:]):
        raise ValueError("margin inputs bind a different rule-candidate release")
    for p in paths[:2] + paths[2:3]:
        if parent["source_sha256s"].get(str(p)) != sha256_file(p):
            raise ValueError("materialized market and numeric input releases differ")
    spec_proof = {}
    if specifications is None:
        specs = pl.DataFrame(schema=SPEC_FIELDS)
    else:
        specs, spec_proof = read_bound_output(specifications)
        if spec_proof.get("dataset") != "taifex_dated_product_specifications":
            raise ValueError("wrong dated specification dataset")
    print(json.dumps(dict(stage="execution_terms_inputs_verified", products=len(wanted),
                         rows=frame.height, specification_intervals=specs.height)), flush=True)
    rules, blockers = compile_execution_terms(frame, inputs[0], inputs[1], inputs[4], inputs[3], specs, inputs[2],
        position_research_policy=policy)
    deps = dependencies.select("date", "physical_contract", "missing_valuation", "intermediate_calendar_gap", "unresolved_lifetime")
    blockers = blockers.join(deps, on=["date", "physical_contract"], how="left", validate="1:1")
    flags = [c for c in blockers.columns if c not in KEYS + ["physical_contract", "is_warmup", "has_blocker"]]
    blockers = blockers.with_columns(pl.any_horizontal(pl.col(c).fill_null(True) for c in flags).alias("has_blocker"))
    required = blockers.filter(~pl.col("is_warmup"))
    coverage = blockers.group_by("product").agg(pl.len().alias("source_rows"),
        (~pl.col("is_warmup")).sum().alias("account_rows"),
        (pl.col("has_blocker") & ~pl.col("is_warmup")).sum().alias("blocked_account_rows"),
        *[(pl.col(c) & ~pl.col("is_warmup")).sum().alias(c) for c in flags]).sort("product")
    # Do not expand 5M rows into a >100M-row melted table just to count gaps.
    work = pl.concat([required.filter(pl.col(flag)).group_by("product").agg(
        pl.len().alias("contract_days"), pl.col("date").min().alias("first_date"),
        pl.col("date").max().alias("last_date"), pl.col("physical_contract").n_unique().alias("physical_lives"))
        .with_columns(pl.lit(flag).alias("reason")) for flag in flags])
    issues = []
    if not required["has_blocker"].any():
        for validate in (validate_margin_carry_rules, validate_margin_value_bases,
                         validate_margin_second_position_limit, validate_margin_grandfather_rules):
            try:
                validate(rules)
            except ValueError as exc:
                issues.append(str(exc))
        try:
            validate_accounting_continuation(frame.join(rules.select("date", "physical_contract"),
                on=["date", "physical_contract"], how="semi"), rules)
        except ValueError as exc:
            issues.append(str(exc))
    # A compiler result is not source admission. The complete source review
    # binds the dated product specification and every numeric/corporate parent.
    admitted = (spec_proof.get("status") == "complete" and spec_proof.get("point_in_time_verified") is True
        and spec_proof.get("admitted_rule_candidates_sha256") == sha256_file(rule_candidates / "manifest.json")
        and spec_proof.get("admitted_numeric_inputs_sha256") == sha256_file(margin_inputs / "manifest.json"))
    if not admitted:
        issues.append("dated specifications and rule-input admission evidence incomplete")
    complete = not required["has_blocker"].any() and not issues
    if compiler_sha != sha256_file(compiler_path):
        raise ValueError("execution-terms compiler changed during this build")
    output.mkdir(parents=True)
    atomic_write_parquet(output / "rules.parquet", rules)
    atomic_write_parquet(output / "contract_day_blockers.parquet", blockers)
    coverage.write_csv(output / "product_coverage.csv")
    work.sort("contract_days", descending=True).write_csv(output / "rule_worklist.csv")
    sources = []
    policy_proof = None
    if complete:
        for source in spec_proof.get("sources", []):
            relative = Path(source["path"])
            if relative.is_absolute() or ".." in relative.parts or not relative.parts or relative.parts[0] != "sources":
                raise ValueError("unsafe execution-terms source path")
            source_path = specifications.parent / relative
            if sha256_file(source_path) != source["sha256"]:
                raise ValueError("execution-terms source receipt mismatch")
            target = output / relative; target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target)
            sources.append(source)
        if not sources or not spec_proof.get("adjusted_terminal_components"):
            raise ValueError("admitted terms require original sources and terminal-component evidence")
        if policy is not None:
            if sha256_file(position_research_policy) != policy_sha:
                raise ValueError("research position policy changed during build")
            relative = Path("sources/position_research_policy.json")
            shutil.copyfile(position_research_policy, output / relative)
            policy_proof = dict(path=str(relative), sha256=policy_sha)
            sources.append(dict(**policy_proof, kind="operator_research_position_policy", url=""))
    outputs = {p.name: dict(sha256=sha256_file(p), bytes=p.stat().st_size)
               for p in output.iterdir() if p.is_file()}
    outputs["rules"] = outputs["rules.parquet"]
    summary = dict(dataset="taifex_futures_margin_execution_terms", schema_version=7 if policy else 6,
        compiler_version=EXECUTION_TERMS_COMPILER_VERSION,
        compiler_sha256=compiler_sha,
        status="complete" if complete else "blocked", point_in_time_verified=complete and policy is None,
        source_materialization_sha256=sha256_file(materialization / "manifest.json"),
        source_sha256s={str(p): sha256_file(p) for p in paths + ([specifications] if specifications else [])},
        products=len(wanted), requested_products=sorted(wanted), source_rows=frame.height, account_rows=rules.height,
        blocked_account_rows=int(required["has_blocker"].sum()), warmup_rows=int(blockers["is_warmup"].sum()),
        blocker_counts={c: int(required[c].sum()) for c in flags}, validation_issues=issues,
        outputs=outputs, sources=sources, adjusted_terminal_components=spec_proof.get("adjusted_terminal_components"),
        all_products_training_ready=False, elapsed_s=time.monotonic()-started)
    if policy is not None:
        summary.update(research_only=True, financial_point_in_time_verified=complete,
            position_research_policy=policy_proof,
            position_research_contract=policy["contract"],
            position_research_policy_sha256=policy_sha,
            position_research_rows=int(rules["position_research_applied"].sum()),
            official_position_history_complete=False)
    atomic_write_json(output / "manifest.json", summary)
    return summary


def bind_margin_carry_opening_valuation(frame: pl.DataFrame, rules: pl.DataFrame,
                                      corporate_terms: pl.DataFrame) -> pl.DataFrame:
    """Value an untraded conversion from prior inventory, never create a quote.

    The compiler already owns the full incoming value after cash and quantity
    conversion. Remove the dated cash deliverable to express its quotation
    reference. Original observed prices and all execution masks stay intact.
    """
    keys = ["date", "physical_contract"]
    missing = (pl.col("open").is_null() | ~pl.col("open").is_finite() | (pl.col("open") <= 0))
    witness_fields = ["inventory_entry_reachable", "inventory_origin_unresolved", "known_margin_contract_value_twd"]
    witnessed = set(witness_fields) <= set(rules.columns)
    candidates = frame.filter(missing & ~pl.col("executable")).drop("contract_multiplier", strict=False).join(rules.select(
        *keys, "contract_multiplier", "opening_time", "opening_contract_value_twd",
        "carry_from_physical_contract", "carry_previous_value_twd", "carry_cash_twd",
        "carry_quantity_numerator", "carry_quantity_denominator",
        *(witness_fields if witnessed else [])), on=keys, validate="1:1")
    # The compiler may prove the predecessor had no reachable inventory. Its
    # dated prior reference still values the new, untraded contract; it does
    # not create an incoming position or a fill.
    unowned = ((pl.col("carry_from_physical_contract") == "")
        & ~pl.col("inventory_entry_reachable") & ~pl.col("inventory_origin_unresolved")
        & ((pl.col("known_margin_contract_value_twd") - pl.col("opening_contract_value_twd")).abs() <= .005)
        ).fill_null(False) if witnessed else pl.lit(False)
    candidates = candidates.with_columns(unowned.alias("_unowned_prior_reference"))
    candidates = candidates.filter(pl.col("product").str.contains(r"\d$")
        & (((pl.col("carry_from_physical_contract") != "")
            & (pl.col("carry_from_physical_contract") != pl.col("physical_contract")))
           | pl.col("_unowned_prior_reference")))
    if candidates.is_empty():
        return frame
    from stockagent.data.tw_futures_margin_preparation import bind_dated_corporate_terms
    terms = bind_dated_corporate_terms(candidates.select("date", "product", "contract"), corporate_terms)
    candidates = candidates.join(terms.select("date", "product", "contract",
        "deliverable_cash_twd", "known_at", "source_content_sha256s",
        pl.col("contract_multiplier").alias("_term_multiplier")),
        on=["date", "product", "contract"], validate="1:1")
    opening = (pl.col("date").cast(pl.String) + "T" + pl.col("opening_time") + "+08:00").str.to_datetime(time_zone="UTC")
    expected = (pl.col("carry_previous_value_twd") - pl.col("carry_cash_twd")) * pl.col(
        "carry_quantity_denominator") / pl.col("carry_quantity_numerator")
    valid = ((pl.col("known_at").str.to_datetime(time_zone="UTC") <= opening)
        & ((pl.col("contract_multiplier") - pl.col("_term_multiplier")).abs() <= 1e-8)
        & (pl.col("_unowned_prior_reference")
           | ((pl.col("opening_contract_value_twd") - expected).abs() <= .005))).fill_null(False)
    if candidates.filter(~valid).height:
        raise ValueError("untraded conversion lacks a prior-known, matching inventory valuation")
    candidates = candidates.with_columns(((pl.col("opening_contract_value_twd")
        - pl.col("deliverable_cash_twd")) / pl.col("contract_multiplier")).alias("_carry_open_reference"))
    if candidates.filter(pl.col("_carry_open_reference").is_null()
        | ~pl.col("_carry_open_reference").is_finite() | (pl.col("_carry_open_reference") <= 0)).height:
        raise ValueError("invalid nonexecuting carry quotation reference")
    result = frame.join(candidates.select(*keys, "_carry_open_reference"), on=keys, how="left", validate="1:1")
    result = result.with_columns(pl.coalesce("_carry_open_reference", "open").alias("open"),
        pl.col("_carry_open_reference").is_not_null().alias("opening_is_carry_valuation"))
    if "valuation_open" in result.columns:
        result = result.with_columns(pl.coalesce("_carry_open_reference", "valuation_open").alias("valuation_open"))
    return result.drop("_carry_open_reference")


def publish_all_twd_margin_release(*, materialization: Path, execution_terms: Path,
                                   final_settlement: Path, output: Path) -> dict:
    """Publish a fully admitted accounting tape via existing validators.

    The terms manifest must bind this exact materialization, all rule rows,
    original sources and terminal component evidence. Partial releases cannot
    become an all-product release by setting a readiness boolean.
    """
    from stockagent.data.tw_futures_margin import validate_margin_rule_source, corporate_margin_base_values, load_margin_terminal_components
    frame, parent = read_bound_output(materialization / "continuous_daily.parquet")
    proof = json.loads(execution_terms.with_name("manifest.json").read_text())
    valuation_research = proof.get("schema_version") == 8
    research = proof.get("schema_version") in (7, 8)
    if valuation_research:
        from stockagent.data.tw_futures_valuation_research import validate_valuation_research_manifest
        validate_valuation_research_manifest(proof, execution_terms.parent)
    elif research:
        from stockagent.data.tw_futures_position_research import validate_position_research_manifest
        validate_position_research_manifest(proof, execution_terms.parent)
    if (proof.get("dataset") != "taifex_futures_margin_execution_terms"
            or proof.get("status") != "complete" or (not research and proof.get("point_in_time_verified") is not True)
            or proof.get("source_materialization_sha256") != sha256_file(materialization / "manifest.json")
            or proof.get("outputs", {}).get("rules", {}).get("sha256") != sha256_file(execution_terms)
            or proof.get("schema_version") not in (6, 7, 8)):
        raise ValueError("full, source-bound schema-6 execution terms are required; candidate inputs cannot be admitted")
    rules = pl.read_parquet(execution_terms)
    keys = ["date", "physical_contract"]
    warmup = frame.join(rules.select(keys), on=keys, how="anti")
    # Only unowned first observations may be context-only. A first adjusted
    # row with incoming old inventory must be retained, even though it has no
    # preceding row under the new physical identity.
    if "context_only_prefix" in proof:
        context_keys = validate_admitted_context_prefix(frame, rules, proof, execution_terms.parent)
        unproved = warmup.join(context_keys, on=keys, how="anti")
        if unproved.filter(pl.col("previous_symbol_date").is_not_null()).height:
            raise ValueError("execution terms omit an unproved intermediate account day")
    elif warmup.filter(pl.col("previous_symbol_date").is_not_null()).height:
        raise ValueError("execution terms omit an intermediate account day")
    frame = frame.join(rules.select(keys), on=keys, how="semi")
    if (rules.select(keys).is_duplicated().any()
            or frame.select(keys).join(rules.select(keys), on=keys, how="anti").height
            or rules.select(keys).join(frame.select(keys), on=keys, how="anti").height):
        raise ValueError("execution terms must cover every materialized contract-day exactly once")
    warmup_only = set(parent["requested_products"]) - set(frame["product"])
    whole_empty = (proof.get("context_only_prefix", {}).get("contract")
        == WHOLE_CONTRACT_PREFIX_CONTRACT)
    if warmup_only - set(parent["zero_print_products"]) and not whole_empty:
        raise ValueError("a requested positive-print product has no post-warmup account days")
    dependencies, _ = read_bound_output(materialization / "execution_dependencies.parquet")
    dependencies = dependencies.join(frame.select(keys), on=keys, how="semi")
    # Financial terms cannot repair missing market observations or calendar holes.
    blockers = ("missing_valuation", "intermediate_calendar_gap", "unresolved_lifetime")
    if dependencies.select(pl.any_horizontal(*[pl.col(c) for c in blockers]).any()).item():
        raise ValueError("unresolved physical market evidence; see execution_dependencies.parquet")
    validate_accounting_continuation(frame, rules)
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("use a new empty execution release directory")
    if frame.filter(~pl.col("executable") & pl.col("open").is_null()).height:
        terminal_terms, _ = load_margin_terminal_components(execution_terms, proof)
        frame = bind_margin_carry_opening_valuation(frame, rules, terminal_terms)
    # Both positive-print execution and zero-print valuations must be usable by
    # the shared account. No row is removed to make these checks pass.
    for column in ("open", "close", "settlement"):
        if frame.filter(pl.col(column).is_null() | ~pl.col(column).is_finite() | (pl.col(column) <= 0)).height:
            raise ValueError(f"unresolved {column} in full materialization")
    frame = frame.drop("contract_multiplier").join(
        rules.select(*keys, "contract_multiplier", "terminal_event"), on=keys, validate="1:1",
    ).with_columns(
        (pl.col("terminal_event") != "mark_only").alias("must_liquidate"),
        pl.when(pl.col("terminal_event") == "cash_settlement").then(pl.lit("last_trade_date"))
        .otherwise(pl.col("liquidation_reason")).alias("liquidation_reason"),
    )
    daily_dir, rules_dir = output / "daily", output / "rules"
    daily_dir.mkdir(parents=True); rules_dir.mkdir()
    atomic_write_parquet(daily_dir / "first_observation_warmup.parquet", warmup)
    daily_path, rules_path = daily_dir / "continuous_daily.parquet", rules_dir / "rules.parquet"
    atomic_write_parquet(daily_path, frame)
    shutil.copyfile(execution_terms, rules_path)
    for source in proof.get("sources", []):
        relative = Path(source["path"])
        if not relative.parts or relative.is_absolute() or ".." in relative.parts or relative.parts[0] in ("rules.parquet", "manifest.json"):
            raise ValueError("unsafe or reserved execution source path")
        path = execution_terms.parent / relative
        if sha256_file(path) != source["sha256"]:
            raise ValueError("execution terms source SHA mismatch")
        target = rules_dir / relative; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    rule_manifest = dict(proof, dataset="taifex_futures_margin_rules", source_daily_sha256=sha256_file(daily_path),
        scope=dict(products=parent['requested_products'], start=str(frame['date'].min()),
                   end=str(frame['date'].max()), rows=frame.height))
    atomic_write_json(rules_dir / "manifest.json", rule_manifest)
    try:
        validated, manifest = validate_margin_rule_source(rules_path, daily_path)
        terms, rights = load_margin_terminal_components(rules_path, manifest)
        corporate_margin_base_values(frame, validated, final_settlement,
            terminal_component_terms=terms, terminal_subscription_values=rights)
    except Exception:
        atomic_write_json(rules_dir / "manifest.json", dict(rule_manifest, status="failed", point_in_time_verified=False))
        raise
    # Publish the training-visible manifest last. A failed validation leaves no
    # loadable daily release, even if some files were already written.
    daily_manifest = dict(parent, status="complete", point_in_time_verified=not research,
        materialization_version=MARGIN_MATERIALIZATION_VERSION,
        nonexecuting_opening_carry_valuations=int(frame["opening_is_carry_valuation"].sum())
            if "opening_is_carry_valuation" in frame.columns else 0,
        all_products_execution_ready=True, all_products_training_ready=False,
        require_all_source_dates_in_stock_context=True,
        execution_rules_sha256=sha256_file(rules_path),
        rows=frame.height, warmup_rows=warmup.height, zero_print_warmup_only_products=sorted(warmup_only),
        official_final_settlement_sha256=sha256_file(final_settlement),
        outputs={"continuous_daily": {"sha256": sha256_file(daily_path), "rows": frame.height}},
        source_materialization_sha256=sha256_file(materialization / "manifest.json"))
    if "context_only_prefix" in proof:
        daily_manifest["context_only_prefix_contract"] = proof["context_only_prefix"]["contract"]
        daily_manifest["context_only_prefix_rows"] = proof["context_only_prefix"]["rows"]
        if whole_empty:
            daily_manifest["maximum_volume_participation"] = proof["context_only_prefix"]["maximum_volume_participation"]
    if research:
        daily_manifest.update(research_only=True, financial_point_in_time_verified=not valuation_research,
            position_research_contract=proof["position_research_contract"],
            position_research_policy_sha256=proof["position_research_policy_sha256"],
            official_position_history_complete=False)
        if valuation_research:
            daily_manifest.update(valuation_research_contract=proof['valuation_research_contract'],
                valuation_research_policy_sha256=proof['valuation_research_policy_sha256'],
                financial_values_inferred=True)
    atomic_write_json(daily_dir / "manifest.json", daily_manifest)
    summary = dict(status="data_release_validated_remote_training_not_yet_verified",
        products=parent["products"], rows=frame.height, daily=str(daily_path), rules=str(rules_path),
        daily_sha256=sha256_file(daily_path), rules_sha256=sha256_file(rules_path),
        runtime_training_verified=False)
    atomic_write_json(output / "build_summary.json", summary)
    return summary


def validate_accounting_continuation(frame: pl.DataFrame, rules: pl.DataFrame) -> None:
    """A marked position must have exactly one next-session inventory owner."""
    keys = ['date', 'physical_contract']
    proof_columns=['inventory_origin_unresolved','inventory_entry_reachable']
    if any(c in rules.columns for c in proof_columns):
        from stockagent.data.tw_futures_execution_terms import inventory_entry_reachability
        if any(c not in rules.columns or rules.schema[c]!=pl.Boolean
               or rules[c].null_count() for c in proof_columns):
            raise ValueError('invalid inventory reachability proof')
        expected=inventory_entry_reachability(frame,rules)
        if not expected.equals(rules.select(*keys,'inventory_entry_reachable').sort(keys)):
            raise ValueError('inventory reachability proof disagrees with source entries and carry edges')
    joined = frame.select(*keys, 'next_market_date', 'cash_settlement').join(
        rules.select(*keys, 'terminal_event',*[c for c in proof_columns if c in rules.columns]),
        on=keys, validate='1:1')
    if joined.filter(pl.col('cash_settlement') & (pl.col('terminal_event') != 'cash_settlement')).height:
        raise ValueError('an official cash settlement cannot become a mark-only/market-close event')
    origins = rules.filter(pl.col('carry_from_physical_contract') != '').select(
        pl.col('carry_from_date').alias('date'),
        pl.col('carry_from_physical_contract').alias('physical_contract'),
        pl.col('date').alias('_destination_date'))
    if origins.select(keys).is_duplicated().any():
        raise ValueError('one inventory owner is used by multiple carry destinations')
    if origins.join(frame.select(keys), on=keys, how='anti').height:
        raise ValueError('carry origin is outside the admitted account rows')
    continued = joined.join(origins, on=keys, how='left', validate='1:1')
    needs_owner = (pl.col('terminal_event') == 'mark_only') & pl.col('next_market_date').is_not_null()
    if 'inventory_entry_reachable' in rules.columns:
        needs_owner=needs_owner & pl.col('inventory_entry_reachable')
    if continued.filter(needs_owner & (pl.col('_destination_date').is_null()
                        | (pl.col('_destination_date') != pl.col('next_market_date')))).height:
        raise ValueError('marked inventory loses its next-session owner, including corporate transfers')
    if continued.filter((pl.col('terminal_event') != 'mark_only') & pl.col('_destination_date').is_not_null()).height:
        raise ValueError('terminal inventory cannot also be carried into a new contract')
