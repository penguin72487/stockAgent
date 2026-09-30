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
MARGIN_CORPORATE_CONTRACT_VERSION = 2
MARGIN_MULTI_LIMIT_CONTRACT_VERSION = 3
MARGIN_VALUE_BASE_CONTRACT_VERSION = 4
MARGIN_GRANDFATHER_CONTRACT_VERSION = 5
MARGIN_TERMINAL_COMPONENT_CONTRACT_VERSION = 6
MARGIN_RULE_VERSIONS = (MARGIN_CONTRACT_VERSION, MARGIN_CORPORATE_CONTRACT_VERSION,
                        MARGIN_MULTI_LIMIT_CONTRACT_VERSION, MARGIN_VALUE_BASE_CONTRACT_VERSION,
                        MARGIN_GRANDFATHER_CONTRACT_VERSION, MARGIN_TERMINAL_COMPONENT_CONTRACT_VERSION)
# Executor policy version is independent of the immutable source-tape schema.
MARGIN_ACCOUNTING_CONTRACT_VERSION = 7
# Training-only: exact quantities, marks, fees and forward returns are unchanged.
MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION = 11
# The first eleven channels retain the canonical integer account ABI.
INITIAL, MAINTENANCE, END_INITIAL, END_MAINTENANCE = range(11, 15)
PREVIOUS_MARK, PREVIOUS_INITIAL, PREVIOUS_MAINTENANCE = range(15, 18)
CAN_BUY, CAN_SELL, POSITION_GROUP, POSITION_UNIT, POSITION_LIMIT = range(18, 23)
LIQUIDATION_RATIO, TERMINAL_MARK, TERMINAL_CAPACITY = range(23, 26)
TERMINAL_CAN_BUY, TERMINAL_CAN_SELL, CASH_SETTLEMENT = range(26, 29)
MARGIN_EXECUTION_WIDTH = 29
# Schema 2 appends a destination-to-origin map. Zero means no incoming position;
# otherwise the source is a one-based slot on the previous account date. Cash
# and the rational quantity conversion apply to OLD inventory exactly once.
CARRY_SOURCE_SLOT, CARRY_QUANTITY_NUMERATOR, CARRY_QUANTITY_DENOMINATOR, CARRY_CASH = range(29, 33)
MARGIN_CORPORATE_EXECUTION_WIDTH = 33
SECOND_POSITION_GROUP, SECOND_POSITION_UNIT, SECOND_POSITION_LIMIT = range(33, 36)
MARGIN_MULTI_LIMIT_EXECUTION_WIDTH = 36
POSITION_GRANDFATHER, SECOND_POSITION_GRANDFATHER = range(36, 38)
MARGIN_GRANDFATHER_EXECUTION_WIDTH = 38
MARGIN_EXECUTION_WIDTHS = (MARGIN_EXECUTION_WIDTH, MARGIN_CORPORATE_EXECUTION_WIDTH,
                         MARGIN_MULTI_LIMIT_EXECUTION_WIDTH, MARGIN_GRANDFATHER_EXECUTION_WIDTH)
MARGIN_FEATURE_COLUMNS = (
    "known_initial_margin_to_prior_notional",
    "known_maintenance_to_initial_margin",
)
MARGIN_AMOUNT_FEATURE_COLUMNS = ("known_initial_margin_twd", *MARGIN_FEATURE_COLUMNS)
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
            or manifest.get("schema_version") not in MARGIN_RULE_VERSIONS
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
    if manifest['schema_version'] >= MARGIN_TERMINAL_COMPONENT_CONTRACT_VERSION:
        load_margin_terminal_components(path, manifest)
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
    if manifest["schema_version"] >= MARGIN_CORPORATE_CONTRACT_VERSION:
        validate_margin_carry_rules(rules)
    if manifest["schema_version"] >= MARGIN_MULTI_LIMIT_CONTRACT_VERSION:
        validate_margin_second_position_limit(rules)
    if manifest["schema_version"] >= MARGIN_VALUE_BASE_CONTRACT_VERSION:
        validate_margin_value_bases(rules)
    if manifest["schema_version"] >= MARGIN_GRANDFATHER_CONTRACT_VERSION:
        validate_margin_grandfather_rules(rules)
    opening_clock = pl.col("opening_time") if manifest["schema_version"] >= MARGIN_CORPORATE_CONTRACT_VERSION else pl.lit("08:45:00")
    for phase, clock in (("", opening_clock), ("settlement_", pl.col("settlement_time"))):
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


def validate_margin_grandfather_rules(rules: pl.DataFrame) -> None:
    """Dated source permission preserves old inventory; never creates headroom."""
    for prefix in ('','second_'):
        name=prefix+'position_grandfather_existing'
        if name not in rules.columns or rules.schema[name]!=pl.Boolean or rules[name].null_count():
            raise ValueError('grandfather permission requires explicit dated boolean fields')
        if rules.group_by('date',prefix+'position_group').agg(
                pl.col(name).n_unique().alias('n')).filter(pl.col('n')!=1).height:
            raise ValueError('inconsistent grandfather permission within a dated position group')


def validate_margin_value_bases(rules: pl.DataFrame) -> None:
    """Require explicit margin bases independently of P&L deliverable values.

    Adjusted cash/rights cannot silently become a leveraged margin base. This
    validates the interface; dated source rules still establish each amount.
    """
    fields=('opening_margin_value_twd','settlement_margin_value_twd','known_margin_value_twd')
    if set(fields)-set(rules.columns):
        raise ValueError('separate dated margin bases are required')
    for field in fields:
        if rules.filter(pl.col(field).is_null() | ~pl.col(field).is_finite() | (pl.col(field)<=0)).height:
            raise ValueError('margin bases must be finite positive dated values')


def validate_margin_second_position_limit(rules: pl.DataFrame) -> None:
    required={'second_position_group','second_position_unit','second_position_limit'}
    if required-set(rules.columns):
        raise ValueError('separate monthly and aggregate position limits require both axes')
    if rules.filter(pl.col('second_position_group').is_null() | (pl.col('second_position_group')=='')).height:
        raise ValueError('second position group must be explicit')
    for name in ('second_position_unit','second_position_limit'):
        if rules.filter(pl.col(name).is_null() | ~pl.col(name).is_finite() | (pl.col(name)<=0)).height:
            raise ValueError('second position units and limits must be positive')
    for prefix in ('','second_'):
        if rules.group_by('date',prefix+'position_group').agg(
                pl.col(prefix+'position_limit').n_unique().alias('n')).filter(pl.col('n')!=1).height:
            raise ValueError('inconsistent limits within a dated position group')


def validate_margin_carry_rules(rules: pl.DataFrame) -> None:
    """Validate explicit contract values and causal, one-to-one carry events.

    A multiplier change is not a price return. Keeping full old/new contract
    values also supports cash deliverables without treating them as dividends.
    This validates an admitted release's ABI, not an unreviewed notice's facts.
    """
    required = {"opening_time", "carry_from_date", "carry_from_physical_contract",
        "carry_quantity_numerator", "carry_quantity_denominator", "carry_cash_twd",
        "carry_known_at", "carry_effective_at", "carry_previous_value_twd",
        "carry_previous_initial_twd", "carry_previous_maintenance_twd",
        "opening_contract_value_twd", "settlement_contract_value_twd",
        "terminal_contract_value_twd", "known_margin_contract_value_twd",
        "opening_tax_twd", "terminal_tax_twd", "terminal_event"}
    missing = required - set(rules.columns)
    if missing:
        raise ValueError(f"missing corporate margin rules: {sorted(missing)}")
    if rules.filter(pl.col("terminal_event").is_null() | ~pl.col("terminal_event").is_in(
            ["mark_only", "cash_settlement", "market_close_required"])).height:
        raise ValueError("explicit product-specific terminal event is required")
    if rules.filter(pl.col("carry_from_physical_contract").is_null()).height:
        raise ValueError("carry origin must be explicit; empty means no incoming inventory")
    incoming = rules.filter(pl.col("carry_from_physical_contract") != "")
    if incoming.select("date", "carry_from_physical_contract").is_duplicated().any():
        raise ValueError("one old physical contract cannot be duplicated across carry targets")
    if incoming.filter(pl.col("carry_from_date").is_null()
                       | (pl.col("carry_from_date") >= pl.col("date"))).height:
        raise ValueError("carry origin must precede its effective account date")
    for field in ("carry_quantity_numerator", "carry_quantity_denominator"):
        if rules.filter(pl.col(field).is_null() | ~pl.col(field).is_finite()
                        | (pl.col(field) <= 0) | (pl.col(field) > 1_000_000)
                        | (pl.col(field) != pl.col(field).floor())).height:
            raise ValueError("carry quantity conversion must be a positive bounded rational")
    for field in ("carry_cash_twd", "opening_tax_twd", "terminal_tax_twd"):
        if rules.filter(pl.col(field).is_null() | ~pl.col(field).is_finite()
                        | ((pl.col(field) < 0) if field != "carry_cash_twd" else pl.lit(False))).height:
            raise ValueError(f"invalid explicit corporate cash/tax: {field}")
    no_origin = rules.filter(pl.col("carry_from_physical_contract") == "")
    if no_origin.filter((pl.col("carry_cash_twd") != 0)
                         | (pl.col("carry_quantity_numerator") != pl.col("carry_quantity_denominator"))).height:
        raise ValueError("a corporate credit or split requires an identified old contract")
    for frame, names in ((rules, ("opening_contract_value_twd", "settlement_contract_value_twd",
                                  "terminal_contract_value_twd", "known_margin_contract_value_twd")),
                         (incoming, ("carry_previous_value_twd", "carry_previous_initial_twd",
                                     "carry_previous_maintenance_twd"))):
        for field in names:
            if frame.filter(pl.col(field).is_null() | ~pl.col(field).is_finite() | (pl.col(field) <= 0)).height:
                raise ValueError(f"missing positive full contract value or prior margin: {field}")
    if incoming.filter(pl.col("carry_previous_initial_twd") < pl.col("carry_previous_maintenance_twd")).height:
        raise ValueError("invalid carried margin hierarchy")
    for clock in ("opening_time", "settlement_time"):
        if rules.filter(pl.col(clock).is_null() | ~pl.col(clock).str.contains(r"^(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$")).height:
            raise ValueError("invalid dated product session clock")
    cutoff = (pl.col("date").cast(pl.String) + pl.lit("T")
              + pl.col("opening_time") + pl.lit("+08:00")).str.to_datetime(time_zone="UTC")
    for field in ("carry_known_at", "carry_effective_at"):
        if incoming.filter(pl.col(field).is_null()
                            | ~pl.col(field).str.contains(r"(Z|[+-]\d{2}:\d{2})$")).height:
            raise ValueError("carry event clocks require explicit timezones")
        if incoming.filter(pl.col(field).str.to_datetime(time_zone="UTC") > cutoff).height:
            raise ValueError("future/unannounced corporate carry event")


def load_margin_terminal_components(path, manifest):
    """Load a release's admitted deliverables, never a preparation candidate.

    Schema 6 keeps the original exchange full-value cells intact. Explicit,
    receipt-bound corporate terms and optional subscription fixings supply the
    components used to recompute missing terminal values in the same account.
    """
    root=Path(path).parent
    proof=manifest.get('adjusted_terminal_components')
    if not isinstance(proof, dict) or proof.get('status')!='admitted':
        raise ValueError('schema 6 requires admitted terminal component evidence')
    receipts={r['path']:r['sha256'] for r in manifest.get('sources', [])}
    def read(key, required=True):
        item=proof.get(key)
        if item is None and not required:return None
        if not isinstance(item, dict):raise ValueError('missing terminal component '+key)
        relative=Path(item['path'])
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('unsafe terminal component source path')
        expected=item.get('sha256')
        if receipts.get(item['path'])!=expected or sha256_file(root/relative)!=expected:
            raise ValueError('terminal components must belong to the release receipts')
        return pl.read_parquet(root/relative)
    terms=read('corporate_terms')
    if ('point_in_time_verified' not in terms.columns or terms['point_in_time_verified'].null_count()
            or not terms['point_in_time_verified'].all()):
        raise ValueError('candidate corporate terms cannot settle a training account')
    rights=read('subscription_values', required=False)
    if rights is not None:
        required={'date','product','contract','fixing_date','rights_twd','notice_content_sha256'}
        if required-set(rights.columns):raise ValueError('subscription evidence lacks a dated fixing')
        if rights.filter(pl.col('fixing_date').is_null() | (pl.col('fixing_date')>pl.col('date'))
                | pl.col('rights_twd').is_null() | ~pl.col('rights_twd').is_finite()
                | (pl.col('rights_twd')<0)).height:
            raise ValueError('subscription fixing must be known by terminal settlement')
    return terms,rights


def corporate_margin_base_values(frame, rules, final_settlement_path=None, *,
        terminal_component_terms=None, terminal_subscription_values=None):
    """Bind full dated values before the shared candidate/fee adapter runs.

    A validated schema-2 rule release, not a current product master, owns
    adjusted deliverables and product-specific tax. Physical delivery remains
    a real close obligation. A quoted final price alone is insufficient for an
    adjusted contract whose deliverable may contain cash or rights.
    """
    columns = ('contract_multiplier', 'opening_contract_value_twd',
               'settlement_contract_value_twd', 'terminal_contract_value_twd',
               'known_margin_contract_value_twd', 'opening_tax_twd', 'terminal_tax_twd',
               'known_tax_twd', 'terminal_event')
    if set(columns) - set(rules.columns):
        raise ValueError('dated corporate base values require explicit units and known tax')
    for field in ('contract_multiplier', 'known_tax_twd'):
        if rules.filter(pl.col(field).is_null() | ~pl.col(field).is_finite()
                        | ((pl.col(field) <= 0) if field == 'contract_multiplier' else (pl.col(field) < 0))).height:
            raise ValueError('invalid dated corporate units/known tax')
    keys = ['date', 'physical_contract']
    joined = frame.join(rules.select(*keys, *[pl.col(c).alias('_rule_' + c) for c in columns]),
                        on=keys, how='left', validate='1:1')
    if joined.filter(pl.col('_rule_contract_multiplier').is_null()
                     | ((pl.col('contract_multiplier') - pl.col('_rule_contract_multiplier')).abs() > 1e-8)).height:
        raise ValueError('daily contract units are missing or differ from the dated rule release')
    cash = joined.filter(pl.col('_rule_terminal_event') == 'cash_settlement')
    final=None
    if final_settlement_path is not None:
        from stockagent.data.tw_futures_margin_preparation import load_preparation_final_settlements
        final = load_preparation_final_settlements(Path(final_settlement_path))
    if cash.height:
        if final is None:
            raise ValueError('corporate cash settlement requires official final evidence')
        if 'settlement_method' not in final.columns:
            final = final.with_columns(pl.lit('cash_settlement').alias('settlement_method'))
        cash = cash.join(final.select(pl.col('settlement_date').alias('date'), 'product', 'contract',
                                     'final_settlement_price', 'final_settlement_value', 'settlement_method'),
                         on=['date', 'product', 'contract'], how='left', validate='m:1')
        adjusted=pl.col('product').str.contains(r'\d$')
        # Full exchange values always take precedence. Multiplying only the
        # quote by shares is permitted for standard contracts, never as an
        # implicit substitute for unknown adjusted cash/subscription rights.
        cash=cash.with_columns(pl.coalesce('final_settlement_value',
            pl.when(~adjusted).then(pl.col('final_settlement_price')*pl.col('contract_multiplier')))
            .alias('_verified_terminal_value'))
        missing=cash.filter(adjusted & pl.col('final_settlement_value').is_null())
        if missing.height and terminal_component_terms is not None:
            from stockagent.data.tw_futures_margin_preparation import bind_adjusted_terminal_values, bind_dated_corporate_terms
            terminal_keys=['date','product','contract']
            resolved=bind_adjusted_terminal_values(missing.select(*terminal_keys,
                'final_settlement_price','final_settlement_value'),terminal_component_terms,
                terminal_subscription_values)
            units=bind_dated_corporate_terms(missing.select(terminal_keys),terminal_component_terms)
            resolved=resolved.join(units.select(*terminal_keys,
                pl.col('contract_multiplier').alias('_component_multiplier')),on=terminal_keys,validate='1:1')
            cash=cash.join(resolved.select(*terminal_keys,'terminal_value_input_twd','_component_multiplier'),
                           on=terminal_keys,how='left',validate='m:1')
            if cash.filter(pl.col('terminal_value_input_twd').is_not_null()
                    & ((pl.col('_component_multiplier')-pl.col('contract_multiplier')).abs()>1e-8)).height:
                raise ValueError('terminal component multiplier differs from the account')
            cash=cash.with_columns(pl.coalesce('_verified_terminal_value','terminal_value_input_twd')
                                   .alias('_verified_terminal_value'))
        if cash.filter(pl.col('final_settlement_price').is_null()
                       | (pl.col('settlement_method') != 'cash_settlement')
                       | pl.col('_verified_terminal_value').is_null()
                       | ((pl.col('_rule_terminal_contract_value_twd') - pl.col('_verified_terminal_value')).abs() > .005)).height:
            raise ValueError('dated cash terminal value lacks matching official product settlement')
    return joined


def attach_futures_margin_rules(panel, path, *, broker_multiplier=1.0, liquidation_ratio=0.25, participation=0.5,
                                benchmark_mode="legacy_front_holding_return", include_margin_amount=False):
    """Attach executor-only marks and prior-observable margin model context."""
    if benchmark_mode not in {"legacy_front_holding_return", "tx_front_rolling_1x_gross"}:
        raise ValueError(f"unsupported futures margin benchmark mode: {benchmark_mode}")
    daily = panel.stock_context_futures_portfolio_daily
    if daily is None or daily.integer_execution is None or daily.intraday_execution is not None:
        raise ValueError("margin requires the canonical carrying whole-contract sidecar")
    rules, rule_manifest = validate_margin_rule_source(path, daily.source_path)
    corporate = rule_manifest["schema_version"] >= MARGIN_CORPORATE_CONTRACT_VERSION
    multiple_limits = rule_manifest["schema_version"] >= MARGIN_MULTI_LIMIT_CONTRACT_VERSION
    separate_margin_bases = rule_manifest["schema_version"] >= MARGIN_VALUE_BASE_CONTRACT_VERSION
    grandfather = rule_manifest["schema_version"] >= MARGIN_GRANDFATHER_CONTRACT_VERSION
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
        rate_notional = (pl.col({"open": "opening_contract_value_twd",
                               "settlement": "settlement_contract_value_twd",
                               "previous_settlement": "known_margin_contract_value_twd"}[price])
                         if corporate else pl.col(price) * pl.col("contract_multiplier"))
        if separate_margin_bases:
            rate_notional = pl.col({"open":"opening_margin_value_twd",
                                   "settlement":"settlement_margin_value_twd",
                                   "previous_settlement":"known_margin_value_twd"}[price])
        frame = frame.with_columns(((pl.when(is_rate).then(
            pl.col(value) * rate_notional
        ).otherwise(pl.col(value)) * broker_multiplier + 0.5).floor()).alias(output))
    if corporate:
        # Join the prior physical identity before slicing model dates. An old
        # code and a new standard code can share a delivery month on this date.
        prior = frame.select(
            pl.col("date").alias("carry_from_date"),
            pl.col("physical_contract").alias("carry_from_physical_contract"),
            pl.col("symbol").alias("_carry_symbol"),
            pl.col("settlement_contract_value_twd").alias("_source_prior_value"),
            pl.col("_end_im").alias("_source_prior_im"),
            pl.col("_end_mm").alias("_source_prior_mm"),
        )
        frame = frame.join(prior, on=["carry_from_date", "carry_from_physical_contract"], how="left", validate="m:1")
        incoming = frame.filter(pl.col("carry_from_physical_contract") != "")
        if incoming.filter(pl.col("_carry_symbol").is_null()
                           | ((pl.col("carry_previous_value_twd") - pl.col("_source_prior_value")).abs() > .005)).height:
            raise ValueError("corporate carry origin/value disagrees with previous source contract")
        for declared, actual in (("carry_previous_initial_twd", "_source_prior_im"),
                                 ("carry_previous_maintenance_twd", "_source_prior_mm")):
            expected = (pl.col(declared) * broker_multiplier + .5).floor()
            if incoming.filter((expected - pl.col(actual)).abs() > .005).height:
                raise ValueError("corporate carry margin differs from the prior settled account")
        calendar = frame.select("date").unique().sort("date").with_columns(pl.col("date").shift(1).alias("_prior_account_date"))
        checked = incoming.join(calendar, on="date", how="left")
        if checked.filter(pl.col("_prior_account_date").is_not_null()
                          & (pl.col("carry_from_date") != pl.col("_prior_account_date"))).height:
            raise ValueError("corporate carry skips an account date")
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
    width = MARGIN_CORPORATE_EXECUTION_WIDTH if corporate else MARGIN_EXECUTION_WIDTH
    if multiple_limits:
        width = MARGIN_MULTI_LIMIT_EXECUTION_WIDTH
    if grandfather:
        width = MARGIN_GRANDFATHER_EXECUTION_WIDTH
    ex = np.zeros((*base.shape[:2], width), dtype=np.float32)
    ex[..., :11] = base
    extra_features = np.zeros((*base.shape[:2], 2 + int(include_margin_amount)), dtype=np.float32)
    session_mask = np.zeros(len(panel.dates), dtype=bool)
    session_mask[di] = True
    def values(name):
        return frame[name].to_numpy().astype(np.float64)
    multiplier = values("contract_multiplier")
    opening, closing = values("open"), values("close")
    settlement = values("settlement_contract_value_twd") if corporate else values("settlement") * multiplier
    if corporate:
        # A last trading date is not evidence of cash delivery. In particular,
        # GBF cannot retire a bond obligation using a fictitious cash fill.
        # A pre-delivery close is a capacity-constrained research trade; an
        # unfilled close remains an explicit executor failure.
        expiry = np.asarray(frame["terminal_event"] == "cash_settlement")
        required_close = np.asarray(frame["terminal_event"] == "market_close_required")
        if frame.filter((pl.col("liquidation_reason") == "last_trade_date")
                        & (pl.col("terminal_event") == "mark_only")).height:
            raise ValueError("last trading date lacks an explicit settlement/close policy")
        ex[di, si, 2] = np.maximum(base[di, si, 2], expiry | required_close)
    else:
        expiry = np.asarray(frame["liquidation_reason"] == "last_trade_date")
    terminal = values("terminal_contract_value_twd") if corporate else np.where(expiry, base[di, si, 4], closing * multiplier)
    mark = np.where(expiry, terminal, settlement)
    ex[di, si, 4] = mark
    if corporate:
        ex[di, si, 3] = values("opening_contract_value_twd")
        ex[di, si, 6] = values("opening_tax_twd")
        ex[di, si, 7] = values("terminal_tax_twd")
    else:
        tax = np.asarray([stock_index_futures_tax_rate(d) for d in frame["date"]])
        ex[di, si, 7] = np.floor(terminal * tax + 0.5)
    ex[di, si, 0] = np.log(mark / ex[di, si, 3])
    ratio = values("carry_quantity_numerator") / values("carry_quantity_denominator") if corporate else None
    packed = np.column_stack((
        values("_im"), values("_mm"), values("_end_im"), values("_end_mm"),
        values("carry_previous_value_twd") / ratio if corporate else values("previous_settlement") * multiplier,
        values("_source_prior_im") / ratio if corporate else values("_prior_im"),
        values("_source_prior_mm") / ratio if corporate else values("_prior_mm"),
        opening < values("upper_limit"), opening > values("lower_limit"),
        [group_ids[g] for g in frame["position_group"]], values("position_unit"), values("position_limit"),
        np.full(frame.height, liquidation_ratio), terminal,
        # An official mark (or a block-trade quantity without an OPEN/CLOSE)
        # supplies no executable terminal capacity. Preserve NULL source
        # volume; do not convert it into a trade or an absorbing account error.
        np.where(daily.executable_mask[di, si],
                 np.floor(np.nan_to_num(values("volume"), nan=0., posinf=0., neginf=0.) * participation), 0.),
        closing < values("upper_limit"), closing > values("lower_limit"), expiry,
    ))
    ex[di, si, 11:MARGIN_EXECUTION_WIDTH] = packed
    if corporate:
        source_slots = frame["_carry_symbol"].str.extract(r"(\d+)$").cast(pl.Int64).fill_null(0).to_numpy()
        ex[di, si, MARGIN_EXECUTION_WIDTH:MARGIN_CORPORATE_EXECUTION_WIDTH] = np.column_stack((source_slots,
            values("carry_quantity_numerator"), values("carry_quantity_denominator"), values("carry_cash_twd")))
    if multiple_limits:
        # Contract-month identities span tens of thousands over full history,
        # while only that day's live contracts need simultaneous cap groups.
        second_ids=frame.select((pl.col('second_position_group').rank('dense').over('date')-1)
                               .alias('id'))['id'].to_numpy()
        if second_ids.size and second_ids.max()>=base.shape[1]:
            raise ValueError('second margin position groups exceed the fixed action axis')
        ex[di,si,MARGIN_CORPORATE_EXECUTION_WIDTH:MARGIN_MULTI_LIMIT_EXECUTION_WIDTH] = np.column_stack((
            second_ids,
            values('second_position_unit'),values('second_position_limit')))
    if grandfather:
        ex[di,si,POSITION_GRANDFATHER] = values('position_grandfather_existing')
        ex[di,si,SECOND_POSITION_GRANDFATHER] = values('second_position_grandfather_existing')
    prior_notional = values("known_margin_contract_value_twd") if corporate else values("previous_settlement") * multiplier
    context_valid = np.isfinite(prior_notional) & (prior_notional > 0)
    extra_features[di[context_valid], si[context_valid], -2:] = np.column_stack((
        values("_known_im")[context_valid] / prior_notional[context_valid],
        values("_known_mm")[context_valid] / values("_known_im")[context_valid],
    ))
    if include_margin_amount:
        # _known_im uses the rule announced before 08:45 and the previous
        # settlement for rate-based contracts. Never expose current OPEN/mark.
        extra_features[di[context_valid], si[context_valid], 0] = values("_known_im")[context_valid]
    candidate_mask = daily.candidate_mask.copy()
    candidate_mask[di, si] &= context_valid
    return replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, integer_execution=ex,
        candidate_features=np.concatenate((daily.candidate_features, extra_features), axis=-1),
        candidate_mask=candidate_mask, holding_log_returns=ex[..., 0],
        margin_rules_path=str(path), margin_contract_version=rule_manifest["schema_version"],
        margin_session_mask=session_mask,
        benchmark_log_returns=(
            daily.benchmark_log_returns
            if benchmark_mode == "tx_front_rolling_1x_gross"
            else np.zeros(len(panel.dates), dtype=np.float32)
        ),
    ))
