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

MARGIN_MATERIALIZATION_VERSION = 1
KEYS = ["date", "product", "contract"]


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
    slots = _fixed_portfolio_slot_map(lives, calendar["date"].to_list(), fixed_slot_count=slot_count)
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
    ids = {p: i for i, p in enumerate(sorted(universe["product"].to_list()), start=1)}
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
        rule_candidates: Path, specifications: Path | None, output: Path) -> dict:
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
    rules, blockers = compile_execution_terms(frame, inputs[0], inputs[1], inputs[4], inputs[3], specs, inputs[2])
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
    outputs = {p.name: dict(sha256=sha256_file(p), bytes=p.stat().st_size)
               for p in output.iterdir() if p.is_file()}
    outputs["rules"] = outputs["rules.parquet"]
    summary = dict(dataset="taifex_futures_margin_execution_terms", schema_version=6,
        compiler_version=EXECUTION_TERMS_COMPILER_VERSION,
        compiler_sha256=compiler_sha,
        status="complete" if complete else "blocked", point_in_time_verified=complete,
        source_materialization_sha256=sha256_file(materialization / "manifest.json"),
        source_sha256s={str(p): sha256_file(p) for p in paths + ([specifications] if specifications else [])},
        products=len(wanted), requested_products=sorted(wanted), source_rows=frame.height, account_rows=rules.height,
        blocked_account_rows=int(required["has_blocker"].sum()), warmup_rows=int(blockers["is_warmup"].sum()),
        blocker_counts={c: int(required[c].sum()) for c in flags}, validation_issues=issues,
        outputs=outputs, sources=sources, adjusted_terminal_components=spec_proof.get("adjusted_terminal_components"),
        all_products_training_ready=False, elapsed_s=time.monotonic()-started)
    atomic_write_json(output / "manifest.json", summary)
    return summary


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
    if (proof.get("dataset") != "taifex_futures_margin_execution_terms"
            or proof.get("status") != "complete" or proof.get("point_in_time_verified") is not True
            or proof.get("source_materialization_sha256") != sha256_file(materialization / "manifest.json")
            or proof.get("outputs", {}).get("rules", {}).get("sha256") != sha256_file(execution_terms)
            or proof.get("schema_version") != 6):
        raise ValueError("full, source-bound schema-6 execution terms are required; candidate inputs cannot be admitted")
    rules = pl.read_parquet(execution_terms)
    keys = ["date", "physical_contract"]
    warmup = frame.join(rules.select(keys), on=keys, how="anti")
    # Only unowned first observations may be context-only. A first adjusted
    # row with incoming old inventory must be retained, even though it has no
    # preceding row under the new physical identity.
    if warmup.filter(pl.col("previous_symbol_date").is_not_null()).height:
        raise ValueError("execution terms omit an intermediate account day")
    frame = frame.join(rules.select(keys), on=keys, how="semi")
    if (rules.select(keys).is_duplicated().any()
            or frame.select(keys).join(rules.select(keys), on=keys, how="anti").height
            or rules.select(keys).join(frame.select(keys), on=keys, how="anti").height):
        raise ValueError("execution terms must cover every materialized contract-day exactly once")
    warmup_only = set(parent["requested_products"]) - set(frame["product"])
    if warmup_only - set(parent["zero_print_products"]):
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
    daily_manifest = dict(parent, status="complete", point_in_time_verified=True,
        all_products_execution_ready=True, all_products_training_ready=False,
        require_all_source_dates_in_stock_context=True,
        execution_rules_sha256=sha256_file(rules_path),
        rows=frame.height, warmup_rows=warmup.height, zero_print_warmup_only_products=sorted(warmup_only),
        official_final_settlement_sha256=sha256_file(final_settlement),
        outputs={"continuous_daily": {"sha256": sha256_file(daily_path), "rows": frame.height}},
        source_materialization_sha256=sha256_file(materialization / "manifest.json"))
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
    joined = frame.select(*keys, 'next_market_date', 'cash_settlement').join(
        rules.select(*keys, 'terminal_event'), on=keys, validate='1:1')
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
    if continued.filter(needs_owner & (pl.col('_destination_date').is_null()
                        | (pl.col('_destination_date') != pl.col('next_market_date')))).height:
        raise ValueError('marked inventory loses its next-session owner, including corporate transfers')
    if continued.filter((pl.col('terminal_event') != 'mark_only') & pl.col('_destination_date').is_not_null()).height:
        raise ValueError('terminal inventory cannot also be carried into a new contract')
