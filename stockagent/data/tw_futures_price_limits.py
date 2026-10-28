"""Source-bound price bands and permissions without inventing a limit stage.

For a staged contract whose current stage is not observed, only quotes strictly
inside the smallest legal band receive either permission.  This is an explicit
restricted research contract; its inner band is never an observed upper/lower
exchange limit.  Marks, reference values and market observations are unchanged.
"""
from __future__ import annotations

import numpy as np
import polars as pl

from stockagent.data.tw_price_rules import tick_size_numpy


STAGED_INNER_BAND_CONTRACT = 'staged_inner_band_permissions_v1'
STAGED_FIXED_INNER_BAND_CONTRACT = 'staged_fixed_tick_inner_band_permissions_v2'
STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT = 'reviewed_fixed_ladder_inner_band_permissions_v3'
STAGED_LIMIT_FIELDS = {
    'price_limit_permission_contract': pl.String,
    'price_limit_reference_quote': pl.Float64,
    'price_limit_stage_up_ratios': pl.List(pl.Float64),
    'price_limit_stage_down_ratios': pl.List(pl.Float64),
    'price_limit_inner_upper': pl.Float64,
    'price_limit_inner_lower': pl.Float64,
}
STAGED_FIXED_LIMIT_FIELDS = {'price_limit_fixed_tick': pl.Float64}


def price_limit_release_contract(rules: pl.DataFrame) -> str | None:
    """Keep legacy releases byte-compatible; declare v2 only for fixed ticks."""
    if 'price_limit_permission_contract' not in rules.columns:
        return None
    contracts = set(rules['price_limit_permission_contract'].drop_nulls())
    if not contracts:
        return None
    if contracts - {STAGED_INNER_BAND_CONTRACT, STAGED_FIXED_INNER_BAND_CONTRACT,
                    STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT}:
        raise ValueError('unsupported price-limit permission contract')
    if STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT in contracts:
        return STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT
    return (STAGED_FIXED_INNER_BAND_CONTRACT if STAGED_FIXED_INNER_BAND_CONTRACT in contracts
            else STAGED_INNER_BAND_CONTRACT)


def fixed_ratio_band(reference, ratio, ticks, *, upper):
    """Round the reviewed fixed-tick band; never infer a stage from a quote."""
    value = np.asarray(reference, dtype=np.float64) * np.asarray(ratio, dtype=np.float64)
    ticks = np.asarray(ticks, dtype=np.float64)
    with np.errstate(invalid='ignore', divide='ignore'):
        rounding = np.floor if upper else np.ceil
        return rounding(value / ticks + (1e-8 if upper else -1e-8)) * ticks


def etf_ratio_band(reference, ratio, dates, *, upper):
    """Round in the tick bucket of the limit itself, as for static limits."""
    value = np.asarray(reference, dtype=np.float64) * np.asarray(ratio, dtype=np.float64)
    ticks = tick_size_numpy(value, dates, security_types='etf_future')
    with np.errstate(invalid='ignore', divide='ignore'):
        rounding = np.floor if upper else np.ceil
        return rounding(value / ticks + (1e-8 if upper else -1e-8)) * ticks


def _reviewed_fixed_ladder_valid(up, down):
    """Mathematical support for a source-reviewed three-stage fixed-tick rule."""
    import math
    return (isinstance(up, list) and isinstance(down, list) and len(up) == len(down) == 3
        and all(isinstance(v, (int, float)) and math.isfinite(v) for v in [*up, *down])
        and 1 < up[0] < up[1] < up[2] and 1 > down[0] > down[1] > down[2] > 0)


def staged_specification_validity(specs: pl.DataFrame) -> pl.Series:
    """Source-reviewed ladders and tick families, never arbitrary stage guesses.

    The v2 fixed ladder is the 2017-05-15 UDF/SPF launch rule. Other dated
    fixed ladders require the reviewed v3 contract; the source review owns
    their values, product, tick and effective interval.
    """
    up = specs['limit_stage_up_ratios'].to_list()
    down = specs['limit_stage_down_ratios'].to_list()
    known = [([1.05, 1.10, 1.15], [.95, .90, .85]),
             ([1.07, 1.10, 1.15], [.93, .90, .85])]
    if 'tick_kind' not in specs.columns:
        return pl.Series('_staged_spec_valid', [(u, d) in known for u, d in zip(up, down)], dtype=pl.Boolean)
    kinds = specs['tick_kind'].to_list()
    dates = specs['effective_date'].to_list()
    reviewed = (specs['fixed_staged_ladder_contract'].to_list()
        if 'fixed_staged_ladder_contract' in specs.columns else [None]*specs.height)
    from datetime import date
    return pl.Series('_staged_spec_valid', [
        (kind == 'etf_future' and day is not None and day >= date(2026, 7, 6) and (u, d) in known)
        or (kind == 'fixed' and day is not None and day >= date(2017, 5, 15)
            and review is None and (u, d) == ([1.07, 1.13, 1.20], [.93, .87, .80]))
        or (kind == 'fixed' and day is not None and review == 'reviewed_fixed_ladder_v3'
            and _reviewed_fixed_ladder_valid(u, d))
        for u, d, kind, day, review in zip(up, down, kinds, dates, reviewed)
    ], dtype=pl.Boolean)


def compile_staged_price_limit_fields(frame: pl.DataFrame) -> pl.DataFrame:
    """Preserve unknown exact limits; compile only the certified inner band."""
    keys = ['date', 'physical_contract']
    staged = frame.filter(pl.col('spec_limit_kind') == 'staged_ratio')
    if staged.is_empty():
        return pl.DataFrame(schema={**{k:frame.schema[k] for k in keys}, **STAGED_LIMIT_FIELDS})
    up = staged['spec_limit_stage_up_ratios'].list.first().to_numpy()
    down = staged['spec_limit_stage_down_ratios'].list.first().to_numpy()
    reference = staged['_reference_quote'].to_numpy()
    dates = staged['date'].to_numpy()
    fixed = (staged['spec_tick_kind'] == 'fixed').to_numpy()
    upper = etf_ratio_band(reference, up, dates, upper=True)
    lower = etf_ratio_band(reference, down, dates, upper=False)
    if fixed.any():
        ticks = staged['spec_tick_size'].to_numpy()
        upper[fixed] = fixed_ratio_band(reference[fixed], up[fixed], ticks[fixed], upper=True)
        lower[fixed] = fixed_ratio_band(reference[fixed], down[fixed], ticks[fixed], upper=False)
    result = staged.select(*keys,
        pl.when((pl.col('spec_tick_kind') == 'fixed')
                & (pl.col('spec_fixed_staged_ladder_contract') == 'reviewed_fixed_ladder_v3'))
          .then(pl.lit(STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT))
          .when(pl.col('spec_tick_kind') == 'fixed').then(pl.lit(STAGED_FIXED_INNER_BAND_CONTRACT))
          .otherwise(pl.lit(STAGED_INNER_BAND_CONTRACT)).alias('price_limit_permission_contract'),
        pl.col('_reference_quote').alias('price_limit_reference_quote'),
        pl.col('spec_limit_stage_up_ratios').alias('price_limit_stage_up_ratios'),
        pl.col('spec_limit_stage_down_ratios').alias('price_limit_stage_down_ratios'),
    ).with_columns(
        pl.Series('price_limit_inner_upper', upper).fill_nan(None),
        pl.Series('price_limit_inner_lower', lower).fill_nan(None))
    if fixed.any():
        result = result.with_columns(pl.Series('price_limit_fixed_tick',
            np.where(fixed, staged['spec_tick_size'].to_numpy(), np.nan)).fill_nan(None))
    return result


def validate_price_limit_rules(rules: pl.DataFrame, manifest: dict) -> None:
    """Reject invented exact limits, partial certificates and unbound policies."""
    field = 'price_limit_permission_contract'
    has_fields = set(STAGED_LIMIT_FIELDS) & set(rules.columns)
    if not has_fields and any(c in rules.columns and rules[c].is_not_null().any()
                              for c in STAGED_FIXED_LIMIT_FIELDS):
        raise ValueError('fixed staged tick without its certificate')
    if has_fields and has_fields != set(STAGED_LIMIT_FIELDS):
        raise ValueError('incomplete staged price-limit certificate')
    if not has_fields:
        staged_mask = pl.lit(False)
    else:
        release_contract = price_limit_release_contract(rules)
        staged_mask = pl.col(field).is_not_null()
        static = rules.filter(~staged_mask.fill_null(False))
        if static.select(pl.any_horizontal(pl.col(c).is_not_null()
                for c in (*STAGED_LIMIT_FIELDS, *[c for c in STAGED_FIXED_LIMIT_FIELDS if c in rules.columns])).any()).item():
            raise ValueError('staged certificate values without their contract')
        staged = rules.filter(staged_mask)
        if not staged.is_empty():
            if (manifest.get('price_limit_permission_contract') != release_contract
                    or manifest.get('research_only') is not True):
                raise ValueError('staged permissions require an explicit research release contract')
            if any(staged.schema[c] != dtype for c, dtype in STAGED_LIMIT_FIELDS.items()):
                raise ValueError('staged certificate field type mismatch')
            if staged.select(pl.any_horizontal(pl.col(c).is_null() for c in STAGED_LIMIT_FIELDS).any()).item():
                raise ValueError('staged permissions require complete dated reference evidence')
            if (staged['upper_limit'].is_not_null().any() or staged['lower_limit'].is_not_null().any()
                    or staged.filter((pl.col(field) == STAGED_INNER_BAND_CONTRACT)
                                     & (pl.col('date') < pl.date(2026, 7, 6))).height
                    or staged.filter((pl.col(field) == STAGED_FIXED_INNER_BAND_CONTRACT)
                                     & (pl.col('date') < pl.date(2017, 5, 15))).height):
                raise ValueError('unknown stage cannot be relabelled as an exact exchange limit')
            check = staged.rename({'price_limit_stage_up_ratios':'limit_stage_up_ratios',
                                  'price_limit_stage_down_ratios':'limit_stage_down_ratios'})
            fixed = staged[field].is_in([STAGED_FIXED_INNER_BAND_CONTRACT,
                STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT]).to_numpy()
            etf = check.filter(pl.col(field) == STAGED_INNER_BAND_CONTRACT)
            if not staged_specification_validity(etf).all():
                raise ValueError('unreviewed staged ETF ladder')
            ref = staged['price_limit_reference_quote'].to_numpy()
            upper = staged['price_limit_inner_upper'].to_numpy()
            lower = staged['price_limit_inner_lower'].to_numpy()
            expected_up = etf_ratio_band(ref, staged['price_limit_stage_up_ratios'].list.first().to_numpy(),
                staged['date'].to_numpy(), upper=True)
            expected_down = etf_ratio_band(ref, staged['price_limit_stage_down_ratios'].list.first().to_numpy(),
                staged['date'].to_numpy(), upper=False)
            if fixed.any():
                if ('price_limit_fixed_tick' not in staged.columns
                        or staged.schema['price_limit_fixed_tick'] != pl.Float64):
                    raise ValueError('fixed staged certificate requires its source tick')
                ticks = staged['price_limit_fixed_tick'].to_numpy()
                fixed_rows = check.filter(pl.col(field) == STAGED_FIXED_INNER_BAND_CONTRACT)
                if (any((u.to_list(), d.to_list()) != ([1.07, 1.13, 1.20], [.93, .87, .80])
                        for u, d in zip(fixed_rows['limit_stage_up_ratios'], fixed_rows['limit_stage_down_ratios']))
                        or np.any(~np.isfinite(ticks[fixed]) | (ticks[fixed] <= 0))):
                    raise ValueError('unreviewed fixed staged ladder or tick')
                reviewed_rows = check.filter(pl.col(field) == STAGED_REVIEWED_FIXED_INNER_BAND_CONTRACT)
                if any(not _reviewed_fixed_ladder_valid(u, d) for u, d in zip(
                        reviewed_rows['limit_stage_up_ratios'].to_list(),
                        reviewed_rows['limit_stage_down_ratios'].to_list())):
                    raise ValueError('invalid reviewed fixed ladder')
                expected_up[fixed] = fixed_ratio_band(ref[fixed],
                    staged['price_limit_stage_up_ratios'].list.first().to_numpy()[fixed], ticks[fixed], upper=True)
                expected_down[fixed] = fixed_ratio_band(ref[fixed],
                    staged['price_limit_stage_down_ratios'].list.first().to_numpy()[fixed], ticks[fixed], upper=False)
            if ('price_limit_fixed_tick' in staged.columns
                    and staged.filter(pl.col(field) == STAGED_INNER_BAND_CONTRACT)['price_limit_fixed_tick'].is_not_null().any()):
                raise ValueError('ETF certificate cannot carry a fixed-tick override')
            if (np.any(~np.isfinite(ref) | (ref <= 0) | ~np.isfinite(upper) | ~np.isfinite(lower)
                       | (lower <= 0) | (upper <= lower))
                    or not np.allclose(upper, expected_up, atol=1e-10, rtol=0)
                    or not np.allclose(lower, expected_down, atol=1e-10, rtol=0)):
                raise ValueError('staged inner band differs from its dated source reference')
    static = rules.filter(~staged_mask.fill_null(False))
    if static.filter(pl.any_horizontal(
            pl.col(c).is_null() | ~pl.col(c).is_finite() | (pl.col(c) <= 0)
            for c in ('upper_limit', 'lower_limit'))
            | (pl.col('upper_limit') <= pl.col('lower_limit'))).height:
        raise ValueError('static exchange limits must be finite, positive and ordered')


def price_limit_permissions(rules: pl.DataFrame, quotes) -> tuple[np.ndarray, np.ndarray]:
    """One owner for daily, terminal and decision-clock directional guards."""
    quote = np.asarray(quotes, dtype=np.float64)
    if quote.shape != (rules.height,):
        raise ValueError('one price-limit quote is required per physical day')
    upper = rules['upper_limit'].to_numpy()
    lower = rules['lower_limit'].to_numpy()
    # An observed quote outside either exchange boundary is a source/rule
    # contradiction, not permission to trade in the opposite direction. Float
    # representations of one legal tick (e.g. 22.45 vs 22.450000000000003)
    # must retain the same directional limit behavior.
    at_upper = np.isclose(quote, upper, rtol=0., atol=1e-9)
    at_lower = np.isclose(quote, lower, rtol=0., atol=1e-9)
    inside = (np.isfinite(quote) & np.isfinite(upper) & np.isfinite(lower)
              & ((quote > lower) | at_lower) & ((quote < upper) | at_upper))
    buy = inside & ~at_upper
    sell = inside & ~at_lower
    field = 'price_limit_permission_contract'
    if field in rules.columns:
        if set(STAGED_LIMIT_FIELDS) - set(rules.columns):
            raise ValueError('missing staged permission fields')
        staged = rules[field].is_not_null().to_numpy()
        price_limit_release_contract(rules)
        # We do not infer today's or last night's actual stage. Quotes outside
        # this certain interval receive no new execution capacity under this ABI.
        certain = (np.isfinite(quote) & (quote > rules['price_limit_inner_lower'].to_numpy())
                   & (quote < rules['price_limit_inner_upper'].to_numpy()))
        buy[staged] = certain[staged]
        sell[staged] = certain[staged]
    return buy, sell
