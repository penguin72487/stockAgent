"""Native exchange opening references for verified new standard generations.

These are dated market values, not reconstructed prior settlements, executions,
or inventory transfers. Transport observation clocks remain in source receipts.
"""
from __future__ import annotations

import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import polars as pl

CONTRACT = 'native_reissued_standard_opening_reference_v2'
LISTING_CONTRACT = 'native_newly_listed_standard_opening_reference_v1'
COLLECTION_CONTRACT = 'native_standard_opening_reference_collection_v1'
EVENT_BIRTH_CONTRACT = 'native_event_standard_birth_opening_reference_v1'
EVENT_COLLECTION_CONTRACT = 'native_standard_opening_reference_collection_v2'
STANDARD_SHARE_UNITS = (100., 1000., 2000., 10000.)
REISSUE_REASON = '契約調整重新推出標準契約'
LISTING_REASON = '上市'
EVENT_BIRTH_REASONS = ('除息', '減資', '除息上市', '現增', '除權', '除權除息')
KEYS = ['date', 'physical_contract']
SCHEMA = {
    'date': pl.Date, 'physical_contract': pl.String, 'product': pl.String, 'contract': pl.String,
    'opening_reference_quote': pl.Float64, 'reference_contract_multiplier': pl.Float64,
    'market_reference_asof': pl.String, 'reference_contract': pl.String,
    'native_adjustment_reason': pl.String, 'native_contract_issued': pl.String,
    'source_task_id': pl.String, 'source_raw_sha256': pl.String,
    'source_parquet_sha256': pl.String, 'source_receipt_sha256': pl.String,
    'definition_content_sha256': pl.String,
}
DISCLOSURE_COLUMNS = ['initial_reference_applied', 'initial_reference_quote',
    'initial_reference_market_asof', 'initial_reference_source_raw_sha256']
NATIVE_FIELDS = ['Reasons for Contract Adjustment', 'Stock Dividend per Share',
    'Cash Dividend per Share', 'Adjustment for Cash Dividend', 'Shares per Contract',
    'Cash Dividends per Unit', 'Interest of Cash Offering per Unit', 'Reference Price',
    'Failure to Contract Adjustment']


def _native_standard_reference(row: dict, reason: str) -> dict | None:
    """Interpret the original native monthly key and effective-day value.

    Return None for other event roles and absent references. A populated
    reference with a conflicting native/query identity is an error, never a
    candidate that can be matched by the query coordinate alone.
    """
    from downloader.tej_history import display_decimal
    if row['Reasons for Contract Adjustment'] != reason:
        return None
    if row['Failure to Contract Adjustment'] != 'Y':
        return None
    quote, units = row['Reference Price'], row['Shares per Contract']
    if quote in (None, '', '(null)') or units in (None, '', '(null)'):
        return None
    quote, units = display_decimal(quote), display_decimal(units)
    if (not quote.is_finite() or not units.is_finite() or quote <= 0
            or units not in tuple(Decimal(int(value)) for value in STANDARD_SHARE_UNITS)):
        return None
    key = re.match(r'^([A-Z]{2}[F0-9])(\d{4}(?:0[1-9]|1[0-2]))(?:\s|$)', row['Future Code'] or '')
    day = row['Adjusted Date']
    if (key is None or key[1] + key[2] != row['_query_symbol']
            or not isinstance(day, str) or not re.fullmatch(r'\d{4}[-/]\d{2}[-/]\d{2}', day)):
        raise ValueError('Native reissue reference requires its exact standard monthly contract and date')
    day = date.fromisoformat(day.replace('/', '-'))
    if day.isoformat() != row['_query_period']:
        raise ValueError('Native adjustment date differs from its query coordinate')
    # Historical exports can label an adjusted code as a standard relisting.
    # Its native identity stays adjusted even when its shares happen to be
    # 2000; this zero-origin standard-contract policy cannot admit that code.
    if not key[1].endswith('F'):
        return None
    return dict(date=day, product=key[1], contract=key[2], opening_reference_quote=float(quote),
        reference_contract_multiplier=float(units), native_adjustment_reason=reason,
        native_contract_issued='Y')


def native_reissue_reference(row: dict) -> dict | None:
    return _native_standard_reference(row, REISSUE_REASON)


def native_listing_reference(row: dict) -> dict | None:
    """Use a populated exchange-defined reference; listing alone proves no value."""
    return _native_standard_reference(row, LISTING_REASON)


def native_event_birth_reference(row: dict) -> dict | None:
    """Read an actual reference; a corporate label never proves empty inventory.

    The source builder and compiler independently require a new standard
    physical generation. Existing holders still follow corporate carry/cash
    accounting, including when the native event label is identical.
    """
    reason = row['Reasons for Contract Adjustment']
    if reason not in EVENT_BIRTH_REASONS:
        return None
    return _native_standard_reference(row, reason)


def read_initial_reference_source(source: Path) -> pl.DataFrame | None:
    path = source / 'initial_reference' / 'initial_references.parquet'
    if not path.exists():
        return None
    from stockagent.data.tw_futures_margin_release import read_bound_output
    manifest = json.loads(path.with_name('manifest.json').read_text())
    if (manifest.get('contract') not in (CONTRACT, COLLECTION_CONTRACT, EVENT_COLLECTION_CONTRACT)
            or manifest.get('reference_value_definition_verified') is not True
            or manifest.get('reference_units_bound_to_dated_specifications') is not True
            or manifest.get('snapshot_transport_clock_backdated') is not False
            or manifest.get('reference_is_execution_price') is not False):
        raise ValueError('Versioned source-defined native initial-reference contract required')
    rows = read_bound_output(path)[0]
    if manifest['contract'] == CONTRACT:
        if rows.filter(pl.col('reference_contract') != CONTRACT).height:
            raise ValueError('Legacy reference manifest cannot admit another event role')
    else:
        expected = {CONTRACT: REISSUE_REASON, LISTING_CONTRACT: LISTING_REASON}
        if manifest['contract'] == EVENT_COLLECTION_CONTRACT:
            expected[EVENT_BIRTH_CONTRACT] = list(EVENT_BIRTH_REASONS)
        if (manifest.get('reference_event_contracts') != expected
                or rows.filter(~pl.col('reference_contract').is_in(list(expected))).height):
            raise ValueError('Explicit native reference event contracts required')
    return rows


def bind_initial_references(frame: pl.DataFrame, references: pl.DataFrame) -> pl.DataFrame:
    """Bind only a new physical generation with proved-empty starting inventory.

    The exchange reference is effective by the regular opening instant, by
    its source definition. This does not assert an earlier historical TEJ
    delivery time or turn that reference into a tradable observation.
    """
    if references.schema != pl.Schema(SCHEMA) or references.select(KEYS).is_duplicated().any():
        raise ValueError('Unique typed native initial-reference rows required')
    required = [c for c in SCHEMA if c not in ('date', 'opening_reference_quote', 'reference_contract_multiplier')]
    event_contract = (
        ((pl.col('reference_contract') == CONTRACT)
            & (pl.col('native_adjustment_reason') == REISSUE_REASON))
        | ((pl.col('reference_contract') == LISTING_CONTRACT)
            & (pl.col('native_adjustment_reason') == LISTING_REASON)
            & pl.col('product').str.contains(r'^[A-Z]{2}F$'))
        | ((pl.col('reference_contract') == EVENT_BIRTH_CONTRACT)
            & pl.col('native_adjustment_reason').is_in(EVENT_BIRTH_REASONS)
            & pl.col('product').str.contains(r'^[A-Z]{2}F$')))
    if (references.select(pl.any_horizontal(pl.col(c).is_null() for c in SCHEMA).any()).item()
            or references.filter(pl.any_horizontal(pl.col(c).str.len_chars() == 0 for c in required)).height
            or references.filter(~event_contract
                | (pl.col('native_contract_issued') != 'Y')
                | ~pl.col('opening_reference_quote').is_finite() | (pl.col('opening_reference_quote') <= 0)
                | ~pl.col('reference_contract_multiplier').is_in(STANDARD_SHARE_UNITS)
                | pl.any_horizontal(~pl.col(c).str.contains(r'^[0-9a-f]{64}$') for c in (
                    'source_raw_sha256', 'source_parquet_sha256', 'source_receipt_sha256',
                    'definition_content_sha256'))).height):
        raise ValueError('Unmodified positive native reissued-standard reference evidence required')
    # A partial product rebuild reads the same complete source manifest. Bind
    # only its exact account-day coordinates; never broaden to a code family.
    own = references.join(frame.select(KEYS), on=KEYS, how='semi')
    aliases = {c: '_ref_' + c for c in SCHEMA if c not in KEYS}
    f = frame.join(own.rename(aliases), on=KEYS, how='left', validate='1:1')
    present = pl.col('_ref_opening_reference_quote').is_not_null()
    cutoff = (pl.col('date').cast(pl.String) + 'T' + pl.col('opening_time') + '+08:00')
    clock_format = '%Y-%m-%dT%H:%M:%S%:z'
    asof = pl.col('_ref_market_reference_asof').str.to_datetime(
        format=clock_format, time_zone='UTC', strict=False)
    valid = (pl.col('_ref_product').eq(pl.col('product'))
        & pl.col('_ref_contract').eq(pl.col('contract'))
        & pl.col('previous_symbol_date').is_null() & pl.col('_source_identity').is_null()
        & ~pl.col('_event_today') & pl.col('specification_bound')
        & (pl.col('contract_multiplier') == pl.col('_ref_reference_contract_multiplier'))
        & (pl.col('_ref_market_reference_asof').str.slice(0,10) == pl.col('date').cast(pl.String))
        & asof.is_not_null() & (asof == cutoff.str.to_datetime(
            format=clock_format, time_zone='UTC', strict=False)))
    if f.filter(present & ~valid.fill_null(False)).height:
        raise ValueError('Initial reference must match exact new generation, unit and opening clock')
    return f.with_columns(
        present.alias('initial_reference_applied'),
        pl.col('_ref_opening_reference_quote').alias('initial_reference_quote'),
        pl.col('_ref_market_reference_asof').alias('initial_reference_market_asof'),
        pl.col('_ref_source_raw_sha256').alias('initial_reference_source_raw_sha256'),
        pl.when(present).then(pl.col('_ref_opening_reference_quote') * pl.col('contract_multiplier')
                             + pl.col('_deliverable_cash'))
          .otherwise(pl.col('known_margin_contract_value_twd')).alias('known_margin_contract_value_twd'),
    ).drop(list(aliases.values()))
