"""Withhold opening features when an intervening contract-day is unverified."""
from __future__ import annotations

from datetime import date

import polars as pl


def quarantine_reference_gaps(references: pl.DataFrame, trades: pl.DataFrame,
                              gaps: pl.DataFrame, *, coverage_start: date) -> pl.DataFrame:
    """Apply exact source-gap evidence without changing executions or prices.

    `gaps` must be the complete set of unverified executable contract-days on
    the caller's authenticated daily axis. A missing day can hide an opening
    or a newer trade, so an older reference cannot cross it. Earlier history
    outside that axis has not been proved complete and is also withheld.
    A verified later opening remains usable, while its prior-price denominator
    is independently withheld if the previous trade crossed an unknown day.
    """
    keys = ['date', 'physical_contract']
    if (not isinstance(coverage_start, date) or set(keys) - set(references.columns)
            or set(keys) - set(gaps.columns)
            or {'physical_contract', 'available_by', 'trade_date', 'is_session_open'} - set(trades.columns)
            or references.select(keys).is_duplicated().any()
            or gaps.select(keys).is_duplicated().any()
            or gaps.filter(pl.col('date').is_null() | pl.col('physical_contract').is_null()).height):
        raise ValueError('reference gap protection needs unique dated physical source identities')
    original_columns = references.columns
    if not references.height:
        return references
    if references.filter(pl.col('date') < coverage_start).height:
        raise ValueError('reference decision lies before the certified daily axis')

    # Match the shared resolver's previous-session as-of event selection. The
    # latest price and its source date are used only to prove continuity; this
    # function never creates a new price or changes the original trade event.
    events = trades.unique().sort('is_session_open', descending=True).unique(
        subset=['physical_contract', 'available_by'], keep='last', maintain_order=True)
    prior = events.select('physical_contract',
        pl.col('available_by').alias('_prior_available_by'),
        pl.col('trade_date').alias('_prior_trade_date'))
    left = references.with_columns(pl.col('date').cast(pl.Datetime('us')).alias('_prior_cutoff'))
    left = left.sort('_prior_cutoff').join_asof(prior.sort('_prior_available_by'),
        left_on='_prior_cutoff', right_on='_prior_available_by', by='physical_contract',
        strategy='backward', allow_exact_matches=False, check_sortedness=False)

    if gaps.height:
        history = gaps.select('physical_contract', pl.col('date').alias('_gap_date')).sort('_gap_date')
        left = left.sort('date').join_asof(history, left_on='date', right_on='_gap_date',
            by='physical_contract', strategy='backward', check_sortedness=False)
        before = gaps.select('physical_contract', pl.col('date').alias('_prior_gap_date')).sort('_prior_gap_date')
        left = left.sort('date').join_asof(before, left_on='date', right_on='_prior_gap_date',
            by='physical_contract', strategy='backward', allow_exact_matches=False, check_sortedness=False)
    else:
        left = left.with_columns(pl.lit(None, dtype=pl.Date).alias('_gap_date'),
                                 pl.lit(None, dtype=pl.Date).alias('_prior_gap_date'))
    stale_reference = (pl.col('reference_kind') == 'last_observed_trade') & (
        (pl.col('reference_trade_date') < pl.lit(coverage_start))
        | (pl.col('_gap_date') >= pl.col('reference_trade_date')).fill_null(False))
    unverified_open = (pl.col('reference_kind') == 'observed_session_open') & (
        pl.col('_gap_date') == pl.col('date')).fill_null(False)
    bad_ref = (stale_reference | unverified_open).fill_null(False)
    bad_prior = (pl.col('_prior_trade_date').is_null()
        | (pl.col('_prior_trade_date') < pl.lit(coverage_start))
        | (pl.col('_prior_gap_date') >= pl.col('_prior_trade_date')).fill_null(False))
    reference_fields = ['reference_quote', 'reference_trade_date', 'reference_trade_ts',
                        'reference_available_by', 'reference_source_sha256', 'reference_age_days']
    result = left.with_columns(
        *[pl.when(bad_ref).then(None).otherwise(pl.col(name)).alias(name) for name in reference_fields],
        pl.when(bad_ref).then(pl.lit('unavailable')).otherwise(pl.col('reference_kind')).alias('reference_kind'),
        pl.when(bad_prior).then(None).otherwise(pl.col('prior_trade_quote')).alias('prior_trade_quote'),
        pl.when(bad_ref | bad_prior).then(0.).otherwise(
            pl.col('opening_reference_log_return_from_prior_trade')).alias('opening_reference_log_return_from_prior_trade'),
        *[pl.when(bad_ref).then(0.).otherwise(pl.col(name)).alias(name) for name in (
            'opening_reference_available', 'opening_reference_is_session_open', 'opening_reference_age_days_log1p')])
    return result.select(original_columns).sort(keys)


def withhold_uncertified_fallbacks(references: pl.DataFrame, trades: pl.DataFrame) -> pl.DataFrame:
    """Require a provable current opening and withhold uncertified fallbacks.

    The current prepared regular-session tape does not certify all intervening
    night sessions or the order of equal-second fallback prints. Until a dated
    continuity certificate is connected, fallback prices and prior-price
    denominators cannot be exposed. A current opening also needs a unique
    first-second print, or a verified single-price first minute; a later daily
    OPEN cannot resolve an earlier unordered tie as if it were known then.
    """
    original_columns = references.columns
    if 'opening_order_proven' in trades.columns:
        open_keys = ['physical_contract', 'reference_trade_date', 'reference_available_by',
                     'reference_quote', 'reference_source_sha256']
        proof = trades.filter(pl.col('is_session_open') & pl.col('opening_order_proven').fill_null(False)).select(
            'physical_contract', pl.col('trade_date').alias('reference_trade_date'),
            pl.col('available_by').alias('reference_available_by'), pl.col('price').alias('reference_quote'),
            pl.col('source_sha256').alias('reference_source_sha256')).unique().with_columns(
                pl.lit(True).alias('_opening_order_proven'))
        references = references.join(proof, on=open_keys, how='left', validate='m:1')
        proven = pl.col('_opening_order_proven').fill_null(False)
    else:
        proven = pl.lit(False)
    unavailable = (pl.col('reference_kind') == 'last_observed_trade') | (
        (pl.col('reference_kind') == 'observed_session_open') & ~proven)
    reference_fields = ['reference_quote', 'reference_trade_date', 'reference_trade_ts',
                        'reference_available_by', 'reference_source_sha256', 'reference_age_days']
    return references.with_columns(
        *[pl.when(unavailable).then(None).otherwise(pl.col(name)).alias(name) for name in reference_fields],
        pl.when(unavailable).then(pl.lit('unavailable')).otherwise(pl.col('reference_kind')).alias('reference_kind'),
        pl.lit(None, dtype=pl.Float64).alias('prior_trade_quote'),
        pl.lit(0.).alias('opening_reference_log_return_from_prior_trade'),
        *[pl.when(unavailable).then(0.).otherwise(pl.col(name)).alias(name) for name in (
            'opening_reference_available', 'opening_reference_is_session_open', 'opening_reference_age_days_log1p')]
        ).select(original_columns)


def verify_partial_current_openings(references: pl.DataFrame, trades: pl.DataFrame,
                                    coverage: pl.DataFrame, gaps: pl.DataFrame) -> None:
    """Accept only first-trade events backed by an exact verified contract-day.

    This validates usable rows in a partial source; it does not certify the
    remaining days, historical delivery time, or execution/accounting data.
    """
    refs = references.filter(pl.col('reference_quote').is_not_null())
    if (references.filter((pl.col('reference_kind') == 'last_observed_trade')
                          | pl.col('prior_trade_quote').is_not_null()
                          | (pl.col('opening_reference_log_return_from_prior_trade') != 0.)).height
            or refs.filter((pl.col('reference_kind') != 'observed_session_open')
                           | (pl.col('reference_trade_date') != pl.col('date'))
                           | (pl.col('reference_available_by') > pl.col('decision_ts'))
                           | pl.col('reference_available_by').is_null()
                           | pl.col('reference_age_days').is_null()
                           | (pl.col('reference_age_days') != 0)
                           | (pl.col('opening_reference_age_days_log1p') != 0.)
                           | (pl.col('opening_reference_available') != 1.)
                           | (pl.col('opening_reference_is_session_open') != 1.)).height
            or references.filter(pl.col('reference_quote').is_null()
                                 & ((pl.col('reference_kind') != 'unavailable')
                                    | (pl.col('opening_reference_available') != 0.)
                                    | (pl.col('opening_reference_is_session_open') != 0.)
                                    | (pl.col('opening_reference_age_days_log1p') != 0.))).height):
        raise ValueError('partial references contain an uncertified or noncausal value')
    proof = trades.filter(pl.col('is_session_open') & pl.col('opening_order_proven')).select(
        'physical_contract', pl.col('trade_date').alias('reference_trade_date'),
        pl.col('trade_ts').alias('reference_trade_ts'),
        pl.col('available_by').alias('reference_available_by'),
        pl.col('price').alias('reference_quote'),
        pl.col('source_sha256').alias('reference_source_sha256')).unique()
    covered = coverage.filter(pl.col('verified')).select('date', 'physical_contract',
        pl.col('source_file_sha256').alias('reference_source_sha256')).unique()
    if (refs.join(proof, on=proof.columns, how='anti').height
            or refs.join(covered, on=['date', 'physical_contract', 'reference_source_sha256'], how='anti').height
            or refs.join(gaps.select('date', 'physical_contract'),
                         on=['date', 'physical_contract'], how='inner').height):
        raise ValueError('partial reference lacks an unambiguous, source-covered opening event')
