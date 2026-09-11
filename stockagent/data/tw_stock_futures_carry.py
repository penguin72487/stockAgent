"""Physical-contract continuation metadata for unfilled 13:30 orders.

This is executor evidence, never a model feature. Lanes may be recycled only
after the old physical contract's last date; the ledger checks its identity.
"""
from __future__ import annotations

import json
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS, EVENT_MINUTES, BAR_FIELDS
from stockagent.data.tw_stock_futures_quarantine import exclude_contract_days
from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate

CARRY_CONTRACT_VERSION = 4
CARRY_EXTRA_CHANNELS = (
    "physical_id", "candidate_tier", "target_selected", "settlement_notional",
    "final_settlement_notional", "expiry", "minute_source_verified",
    "cash_equity_adjustment_per_contract", "unresolved_corporate_transition",
)
CARRY_TAPE_FIELDS = TAPE_FIELDS + len(CARRY_EXTRA_CHANNELS)
CARRY_TRANSITION_CONTRACT_VERSION = 5
CARRY_TRANSITION_EXTRA_CHANNELS = (*CARRY_EXTRA_CHANNELS, 'transition_from_physical_id')
CARRY_TAPE_FIELDS_BY_VERSION = {CARRY_CONTRACT_VERSION: CARRY_TAPE_FIELDS,
    CARRY_TRANSITION_CONTRACT_VERSION: TAPE_FIELDS + len(CARRY_TRANSITION_EXTRA_CHANNELS)}
CARRY_QUARANTINE_CONTRACT_VERSION = 6
QUARANTINED_CARRY_POLICY = 'hold_official_settlement'
CARRY_QUARANTINE_EXTRA_CHANNELS = (*CARRY_TRANSITION_EXTRA_CHANNELS,
    'quarantine_hold', 'quarantine_official_settlement_notional')
CARRY_TAPE_FIELDS_BY_VERSION[CARRY_QUARANTINE_CONTRACT_VERSION] = TAPE_FIELDS + len(CARRY_QUARANTINE_EXTRA_CHANNELS)
CARRY_QUARANTINE_OFFSET = TAPE_FIELDS + len(CARRY_TRANSITION_EXTRA_CHANNELS)
CARRY_SUPPORTED_TAPE_FIELDS = tuple(CARRY_TAPE_FIELDS_BY_VERSION.values())
CARRY_STATE_FIELDS = 4  # signed quantity, last marked notional, physical ID, NAV weight
CARRY_LEDGER_UNIT = "futures_physical_carry_v1"


def cash_equity_adjustment(cash_per_share, multiplier) -> int:
    """TAIFEX stock-futures rule 21: discard sub-TWD amounts per contract."""
    return int((Decimal(str(cash_per_share)) * Decimal(str(multiplier))).to_integral_value(rounding=ROUND_DOWN))


def load_carry_corporate_actions(path: str | Path, dates=None) -> pl.DataFrame:
    """Receipt-backed cash events; other old-position transitions stay unknown."""
    from stockagent.data.panel import _CorporateActionReferencePaths, _load_corporate_action_reference
    path = Path(path)
    reference = path.with_name('tw_corporate_action_reference.parquet')
    _load_corporate_action_reference(_CorporateActionReferencePaths(
        parquet=reference, summary=reference.with_suffix('.summary.json'),
        entitlements_parquet=path, entitlements_summary=path.with_suffix('.summary.json')))
    receipt = json.loads(path.with_suffix('.summary.json').read_text())
    if dates is not None and len(dates):
        if (np.min(dates) < np.datetime64(receipt['coverage_start'])
                or np.max(dates) > np.datetime64(receipt['coverage_end'])):
            raise ValueError('carry corporate-action archive does not cover the decision calendar')
    events = pl.read_parquet(path)
    pure_cash = ((pl.col('handling') == 'exact_cash') & pl.col('event_type').is_in(['息', '除息'])
                 & pl.col('cash_dividend_per_share').is_finite() & (pl.col('cash_dividend_per_share') >= 0)
                 & pl.col('announcement_date').is_not_null() & (pl.col('announcement_date') <= pl.col('date')))
    return events.select('date', pl.col('symbol').alias('underlying_symbol'),
        pl.when(pure_cash).then(pl.col('cash_dividend_per_share')).otherwise(0.).alias('cash_adjustment_per_share'),
        (~pure_cash.fill_null(False)).alias('unresolved_transition'))


def load_final_settlements(path: str | Path) -> pl.DataFrame:
    path = Path(path)
    receipt = json.loads((path.parent / "manifest.json").read_text())
    if (receipt.get("schema_version") != 1 or receipt.get("status") != "complete"
            or receipt.get("outputs", {}).get("futures_final_settlement_history", {}).get("sha256") != sha256_file(path)):
        raise ValueError("official final settlement receipt/SHA mismatch")
    frame = pl.read_parquet(path).with_columns(
        pl.concat_str("product", pl.lit(":"), "contract").alias("physical_contract"),
        pl.col("settlement_date").alias("date"),
    )
    if frame.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("duplicate official physical final settlements")
    if frame.filter(~pl.col("final_settlement_price").is_finite() | (pl.col("final_settlement_price") <= 0)).height:
        raise ValueError("invalid official final settlement value")
    if 'final_settlement_value' not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias('final_settlement_value'))
    if frame.filter(pl.col('final_settlement_value').is_not_null()
                    & (~pl.col('final_settlement_value').is_finite()
                       | (pl.col('final_settlement_value') <= 0))).height:
        raise ValueError('invalid official final contract value')
    return frame.select("date", "physical_contract", "final_settlement_price", "final_settlement_value")


def carry_contract_rows(source, selected, dates, symbols, settlements, *, quarantine_contract_days=()):
    """One physical continuation calendar for tape building and source audits."""
    selected = exclude_contract_days(selected, quarantine_contract_days)
    date_map = pl.DataFrame({"date": dates, "di": np.arange(len(dates))}).with_columns(pl.col("date").cast(pl.Date))
    symbol_map = pl.DataFrame({"underlying_symbol": symbols, "si": np.arange(len(symbols))})
    selected = selected.join(date_map.select("date"), on="date", how="semi").join(symbol_map, on="underlying_symbol", how="inner")
    starts = selected.group_by("physical_contract", "underlying_symbol").agg(pl.col("date").min().alias("start"))
    rows = (source.join(starts, on=["physical_contract", "underlying_symbol"], how="inner")
            .filter(pl.col("date") >= pl.col("start"))
            .join(date_map, on="date", how="inner").join(symbol_map, on="underlying_symbol", how="inner"))
    if rows.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("carry daily source must have unique physical contract-days")
    # An inferred last *observed trade* is not the legal expiry. Keep the old
    # identity through the official final date (or the dataset boundary when
    # final settlement is not yet available). Unknown intervening facts stay
    # unknown and cannot be replaced by the next nearby contract's prices.
    final_dates = settlements.select("physical_contract", pl.col("date").alias("official_expiry"))
    if final_dates["physical_contract"].is_duplicated().any():
        raise ValueError("physical contract has multiple official final dates")
    lifetime = (rows.group_by("physical_contract", "underlying_symbol", "si")
                .agg(pl.col("date").min().alias("start"), pl.col("contract_multiplier").first())
                .join(final_dates, on="physical_contract", how="left"))
    calendar = (lifetime.join(date_map, how="cross")
                .filter((pl.col("date") >= pl.col("start"))
                        & (pl.col("official_expiry").is_null() | (pl.col("date") <= pl.col("official_expiry")))))
    rows = calendar.join(rows.select("date", "physical_contract", "valuation_settlement", "source_row_observed"),
                         on=["date", "physical_contract"], how="left", validate="1:1")
    return rows, selected


def _load_carry_official_evidence(path: str | Path) -> pl.DataFrame:
    """Verify the common immutable daily/spread archive evidence chain."""
    path = Path(path)
    receipt = json.loads(path.with_name('official_evidence_manifest.json').read_text())
    if (receipt.get('source') != 'taifex_complete_daily_and_spread_legs_v1'
            or receipt.get('sha256') != sha256_file(path)):
        raise ValueError('carry no-trade evidence SHA/source mismatch')
    for item in receipt['sources']:
        if sha256_file(Path(item['path'])) != item['sha256']:
            raise ValueError('carry no-trade raw archive SHA mismatch')
    frame = pl.read_parquet(path)
    if frame.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('duplicate carry no-trade evidence')
    return frame


def load_carry_no_trade_evidence(path: str | Path) -> pl.DataFrame:
    """Accept only independently audited zero outright volume, never a KBar fill."""
    frame = _load_carry_official_evidence(path)
    receipt = json.loads(Path(path).with_name('official_evidence_manifest.json').read_text())
    if 'no_trade_proof_eligible' in frame.columns:
        frame = frame.filter(pl.col('no_trade_proof_eligible'))
    proven = frame.filter((pl.col('outright_volume') == 0)
        & pl.col('official_reason').is_in(['zero_total_volume', 'spread_legs_only', 'absent_from_complete_day']))
    if proven.filter(pl.col('official_day_sources').is_null() | (pl.col('official_day_sources') == '[]')).height:
        raise ValueError('carry no-trade proof has no complete official day')
    source_hashes = {item['sha256'] for item in receipt['sources']}
    for encoded in proven['official_day_sources'].unique():
        hashes = set(json.loads(encoded))
        if not hashes or not hashes <= source_hashes:
            raise ValueError('carry no-trade day references an unverified archive')
    return proven.select('date', 'physical_contract', pl.lit('official_no_outright_trades').alias('status'))


def load_carry_daily_settlements(path: str | Path) -> pl.DataFrame:
    """A reported settlement remains a valuation even with zero outright fills."""
    frame = _load_carry_official_evidence(path)
    if 'official_settlement' not in frame.columns:
        return pl.DataFrame(schema={'date': pl.Date, 'physical_contract': pl.String,
                                    'official_daily_settlement': pl.Float64})
    frame = frame.with_columns(pl.col('official_settlement').str.replace_all(',', '')
                               .cast(pl.Float64, strict=False).alias('official_daily_settlement'))
    priced = frame.filter(pl.col('official_daily_settlement').is_finite()
                          & (pl.col('official_daily_settlement') > 0))
    receipt = json.loads(Path(path).with_name('official_evidence_manifest.json').read_text())
    hashes = {item['sha256'] for item in receipt['sources']}
    for r in priced.select('official_source_sha256', 'official_day_sources').unique().iter_rows(named=True):
        if (r['official_source_sha256'] not in hashes
                or r['official_source_sha256'] not in set(json.loads(r['official_day_sources']))):
            raise ValueError('carry daily settlement references an unverified official day')
    return priced.select('date', 'physical_contract', 'official_daily_settlement')


def apply_carry_daily_settlements(rows: pl.DataFrame, evidence: pl.DataFrame | None) -> pl.DataFrame:
    """Join by exact day/physical identity, without changing orders or minutes."""
    if evidence is None:
        return rows.with_columns(pl.lit(None, dtype=pl.Float64).alias('official_daily_settlement'))
    return (rows.join(evidence, on=['date', 'physical_contract'], how='left', validate='m:1')
            .with_columns(pl.coalesce('official_daily_settlement', 'valuation_settlement')
                          .alias('valuation_settlement')))


def add_carry_no_trade_evidence(coverage, minutes, proof):
    keys = ['date', 'physical_contract']
    if minutes.filter(pl.col('volume') > 0).join(proof.select(keys), on=keys, how='semi').height:
        raise ValueError('carry no-trade evidence contradicts observed minute volume')
    # Existing verified minutes own their full-day acceptance; do not overwrite
    # any row or resolve an existing disputed candidate through this sidecar.
    extra = proof.join(coverage.select(keys), on=keys, how='anti')
    return pl.concat([coverage.select(*keys, 'status'), extra], how='vertical')


def build_carry_tape(
    source: pl.DataFrame, selected: pl.DataFrame, minutes: pl.DataFrame,
    coverage: pl.DataFrame, dates: np.ndarray, symbols: tuple[str, ...],
    settlements: pl.DataFrame, *, fee: float, participation: float,
    capacity_rounding: str, quarantine_contract_days=(), corporate_actions: pl.DataFrame | None = None,
    daily_settlements: pl.DataFrame | None = None,
    transition_bundle: dict | None = None,
    quarantined_carry_policy: str = 'reject',
) -> tuple[np.ndarray, dict]:
    """Retain physical series through expiry; unknown evidence remains unknown."""
    rows, selected = carry_contract_rows(source, selected, dates, symbols, settlements,
        quarantine_contract_days=quarantine_contract_days)
    transitions=[]
    if transition_bundle is not None:
        from stockagent.data.tw_stock_futures_transition import append_transition_rows
        rows, transitions = append_transition_rows(rows, selected, dates, symbols, settlements, transition_bundle)
    rows = apply_carry_daily_settlements(rows, daily_settlements)
    if quarantined_carry_policy not in ('reject', QUARANTINED_CARRY_POLICY):
        raise ValueError('unknown quarantined carry policy')
    from stockagent.data.tw_stock_futures_quarantine import normalize_contract_days
    quarantine_keys = {(item['date'], item['physical_contract'])
                       for item in normalize_contract_days(quarantine_contract_days)}
    quarantine_rows = []
    spans = rows.group_by("si", "physical_contract").agg(
        pl.col("di").min().alias("first"), pl.col("di").max().alias("last"))
    assignments = []
    width = 2
    for (si,), frame in spans.partition_by("si", as_dict=True).items():
        ends = []
        for r in frame.sort("first", "physical_contract").iter_rows(named=True):
            lane = next((i for i, end in enumerate(ends) if end < r["first"]), len(ends))
            if lane == len(ends): ends.append(r["last"])
            else: ends[lane] = r["last"]
            assignments.append({"physical_contract": r["physical_contract"], "lane": lane})
        width = max(width, len(ends))
    identities = {c: i + 1 for i, c in enumerate(sorted(rows["physical_contract"].unique()))}
    if len(identities) >= 2**24:
        raise ValueError("physical identities exceed exact FP32 integer range")
    rows = (rows.join(pl.DataFrame(assignments), on="physical_contract", how="left", validate="m:1")
            .join(selected.select("date", "physical_contract", pl.lit(True).alias("selected")),
                  on=["date", "physical_contract"], how="left", validate="1:1")
            .join(coverage.select("date", "physical_contract", "status"),
                  on=["date", "physical_contract"], how="left", validate="1:1")
            .join(settlements, on=["date", "physical_contract"], how="left", validate="1:1"))
    if corporate_actions is not None:
        rows = rows.join(corporate_actions, on=['date', 'underlying_symbol'], how='left', validate='m:1')
    else:
        # Synthetic fixtures can contain no events. Production attachment
        # requires the independently receipt-verified corporate-action archive.
        rows = rows.with_columns(pl.lit(0.).alias('cash_adjustment_per_share'),
                                 pl.lit(False).alias('unresolved_transition'))
    version = CARRY_TRANSITION_CONTRACT_VERSION if transition_bundle is not None else CARRY_CONTRACT_VERSION
    if quarantined_carry_policy == QUARANTINED_CARRY_POLICY:
        version = CARRY_QUARANTINE_CONTRACT_VERSION
    tape = np.zeros((len(dates), len(symbols), width, CARRY_TAPE_FIELDS_BY_VERSION[version]), dtype=np.float32)
    transfer = {(r['date'],r['to_physical_contract']):r for r in transitions}
    for r in rows.iter_rows(named=True):
        cell = tape[r["di"], r["si"], r["lane"]]
        m = float(r["contract_multiplier"])
        if m not in (2000., 100.):
            raise ValueError(f"unsupported carry contract multiplier: {r['physical_contract']}")
        mark = r["valuation_settlement"]
        expiry = r["date"] == r["official_expiry"]
        quarantine = (quarantined_carry_policy == QUARANTINED_CARRY_POLICY
                      and (str(r['date']), r['physical_contract']) in quarantine_keys)
        if quarantine:
            official_mark = r['official_daily_settlement']
            if official_mark is None or not np.isfinite(official_mark) or official_mark <= 0:
                raise ValueError(f"quarantined carry requires verified same-day official settlement: {r['date']} / {r['physical_contract']}")
            if expiry:
                raise ValueError('quarantined carry cannot preserve inventory past official expiry')
            cell[CARRY_QUARANTINE_OFFSET:] = (1., m * official_mark)
            quarantine_rows.append({'date': str(r['date']), 'physical_contract': r['physical_contract'],
                                    'official_daily_settlement': float(official_mark)})
        cell[:3] = (m, fee, stock_index_futures_tax_rate(r["date"]))
        cell[TAPE_FIELDS:CARRY_TAPE_FIELDS] = (
            identities[r["physical_contract"]], int(m == 100), bool(r["selected"]),
            m * mark if mark is not None else float("nan"),
            (r['final_settlement_value'] if r.get('final_settlement_value') is not None else
             m * r["final_settlement_price"] if r["final_settlement_price"] is not None else 0.),
            expiry, r["status"] in ("minute_verified", "official_no_outright_trades"),
            cash_equity_adjustment(r['cash_adjustment_per_share'] or 0., m), bool(r['unresolved_transition']),
        )
        transition=transfer.get((str(r['date']),r['physical_contract']))
        if transition is not None:
            cell[CARRY_TAPE_FIELDS] = identities[transition['from_physical_contract']]
            # The shared ledger transfers inventory before applying its cash
            # channel. Credit/debit old signed holdings once, never a new order.
            cash = transition['cash_adjustment_per_contract']
            if cell[TAPE_FIELDS+7] not in (0., cash):
                raise ValueError('corporate transition cash contradicts entitlement source')
            cell[TAPE_FIELDS+7] = cash
            cell[TAPE_FIELDS+8] = 0  # Exact notice resolves only the transferred physical.
    keys = rows.select("date", "physical_contract", "di", "si", "lane")
    bars = minutes.join(keys, on=["date", "physical_contract"], how="inner", validate="m:1")
    round_capacity = np.ceil if capacity_rounding == "ceil" else np.floor
    events = {m: i for i, m in enumerate(EVENT_MINUTES)}
    for r in bars.iter_rows(named=True):
        offset = 3 + events[r["minute"]] * len(BAR_FIELDS)
        tape[r["di"], r["si"], r["lane"], offset:offset + 5] = (
            r["vwap"], r["high"], r["low"], r["close"], round_capacity(r["volume"] * participation))
    if version == CARRY_QUARANTINE_CONTRACT_VERSION:
        quarantined = tape[..., CARRY_QUARANTINE_OFFSET] > 0
        # The source remains untouched and unverified. These execution-only
        # cells forbid orders even if additional bars later become available.
        tape[..., 3:TAPE_FIELDS][quarantined] = 0
        tape[..., TAPE_FIELDS + 2][quarantined] = 0
    resolved_origins = {(r['date'], r['from_physical_contract']) for r in transitions}
    unsupported = [
        {'date': str(r['date']), 'physical_contract': r['physical_contract'],
         'underlying_symbol': r['underlying_symbol']}
        for r in rows.filter(pl.col('unresolved_transition').fill_null(False)
                             & (pl.col('start') < pl.col('date'))).sort('date', 'physical_contract').iter_rows(named=True)
        if (str(r['date']), r['physical_contract']) not in resolved_origins
    ]
    if unsupported:
        count = len({(r['date'], r['underlying_symbol']) for r in unsupported})
        print(f'[futures carry] {count} corporate events / {len(unsupported)} physical-contract-days still lack a supported prior-inventory transition; held positions at these events remain data errors. Exact list: futures_carry_physical_contracts.json / unsupported_pre_event_holding_contract_days', flush=True)
    return tape, {"physical_ids": identities, "lanes": width,
                  "unsupported_pre_event_holding_contract_days": unsupported,
                  "contract_days": rows.height, "contract_version": version,
                  **({'quarantined_carry_policy': quarantined_carry_policy,
                      'quarantined_carry_contract_days': quarantine_rows,
                      'quarantine_valuation_timing': 'end_of_day_only_never_opening_sizing',
                      'quarantine_execution_policy': 'no_orders_no_fills_no_fees_preserve_signed_inventory'}
                     if version == CARRY_QUARANTINE_CONTRACT_VERSION else {}),
                  **({'corporate_transitions': transitions,
                      'transition_manifest_sha256': transition_bundle['manifest_sha256'],
                      'transition_policy': transition_bundle['policy']} if transition_bundle is not None else {}),
                  "valuation_policy": "verified_same_day_official_settlement_else_canonical_close_or_last_known",
                  "valuation_is_official_daily_settlement_guaranteed": False,
                  "corporate_actions_verified": corporate_actions is not None,
                  "official_daily_settlement_contract_days": rows.filter(pl.col('official_daily_settlement').is_not_null()).height,
                  "cash_adjustment_contract_days": rows.filter(pl.col('cash_adjustment_per_share') > 0).height,
                  "unsupported_transition_contract_days": rows.filter(pl.col('unresolved_transition').fill_null(False)).height,
                  "carried_valuation_contract_days": rows.filter(
                      ~pl.col("source_row_observed").fill_null(False)
                      & pl.col('official_daily_settlement').is_null()
                      & pl.col("valuation_settlement").is_finite()
                      & (pl.col("valuation_settlement") > 0)).height}
