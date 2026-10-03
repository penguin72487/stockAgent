"""Prepare dated futures rules from the canonical official archive.

No downloading, guessed historical values, or alternative training pipeline.
All dates refer to the regular session. Publication-day end is a conservative
availability bound; a close boundary labels the post-close account phase.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone, timedelta
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import unicodedata

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.taifex_rule_parsing import margin_changes, temporal_mentions
from scripts.download_taifex_rule_history import read_verified_raw

TAIPEI = timezone(timedelta(hours=8))
PRODUCTS = ("TX", "MTX")
ROC_DATE = r"(\d{2,3})年(\d{1,2})月(\d{1,2})日"


def align_product_margin_intervals(days: pl.DataFrame, intervals: pl.DataFrame) -> pl.DataFrame:
    """Bind product-relative rules at both regular-session account phases.

    This is the common numeric input to admission, not a PIT approval. Legal
    intervals and publication clocks are independent: an ended interval never
    fills a gap, and a not-yet-public replacement never revives the old level.
    A publication on the query day needs a dated product clock; it is retained
    as a clock-review row instead of assuming all products opened at 08:45.
    Work on unique product-days before joining back to contract-days.
    """
    required = {'product', 'effective_date', 'effective_phase', 'known_at',
                'valid_until_date_exclusive', 'valid_until_phase_exclusive',
                'margin_kind', 'initial', 'maintenance', 'source_content_sha256s'}
    if required - set(intervals.columns):
        raise ValueError(f'missing margin interval fields: {sorted(required-set(intervals.columns))}')
    days = days.select(pl.col('date').cast(pl.Date), 'product').unique()
    if days['date'].null_count() or days['product'].null_count():
        raise ValueError('margin queries require explicit dates and products')
    if intervals.filter(~pl.col('effective_phase').is_in([0, 1])
                        | pl.col('effective_phase').is_null()).height:
        raise ValueError('unresolved effective phases must not enter margin intervals')
    if intervals.select('product', 'effective_date', 'effective_phase').is_duplicated().any():
        raise ValueError('duplicate margin interval boundary')
    if intervals.filter(pl.col('known_at').is_null()
            | ~pl.col('known_at').str.contains(r'(Z|[+-]\d{2}:\d{2})$')).height:
        raise ValueError('margin availability requires an explicit timezone')
    if intervals.filter(pl.col('source_content_sha256s').is_null()
            | (pl.col('source_content_sha256s').list.len() == 0)).height:
        raise ValueError('margin intervals require source identities')
    if intervals.filter(~pl.col('margin_kind').is_in(['fixed_twd', 'notional_rate'])
            | pl.col('margin_kind').is_null()
            | pl.any_horizontal([pl.col(c).is_null() | ~pl.col(c).is_finite() | (pl.col(c) <= 0)
                                 for c in ('initial', 'maintenance')])
            | (pl.col('initial') < pl.col('maintenance'))
            | ((pl.col('margin_kind') == 'notional_rate') & (pl.col('initial') > 1))).height:
        raise ValueError('invalid margin interval amounts or units')
    levels = intervals.with_row_index('margin_interval_id').with_columns(
        (pl.col('effective_date').str.to_date().cast(pl.Int32)*2
         + pl.col('effective_phase')).alias('_begin'),
        (pl.col('valid_until_date_exclusive').str.to_date().cast(pl.Int32)*2
         + pl.col('valid_until_phase_exclusive')).alias('_end'),
        pl.col('known_at').str.to_datetime(time_zone='UTC').dt.convert_time_zone(
            'Asia/Taipei').dt.date().alias('_publication_day'),
    ).sort('_begin')
    if levels.filter(pl.col('_end').is_not_null() & (pl.col('_end') <= pl.col('_begin'))).height:
        raise ValueError('margin interval end must follow its beginning')
    if levels.filter(pl.col('valid_until_date_exclusive').is_not_null()
                     & ~pl.col('valid_until_phase_exclusive').is_in([0, 1]).fill_null(False)).height:
        raise ValueError('margin interval end requires an explicit phase')
    adjacent = levels.with_columns(pl.col('_begin').shift(-1).over('product').alias('_next'))
    if adjacent.filter(pl.col('_next').is_not_null()
            & (pl.col('_end').is_null() | (pl.col('_end') > pl.col('_next')))).height:
        raise ValueError('overlapping margin intervals')
    result = days
    for phase, prefix in ((0, 'opening_'), (1, 'settlement_')):
        query = days.with_columns((pl.col('date').cast(pl.Int32)*2+phase).alias('_query')).sort('_query')
        bound = query.join_asof(levels, left_on='_query', right_on='_begin', by='product',
                               strategy='backward', check_sortedness=False)
        live = (pl.col('_begin').is_not_null()
                & (pl.col('_end').is_null() | (pl.col('_query') < pl.col('_end'))))
        state = (pl.when(pl.col('_begin').is_null()).then(pl.lit('no_prior_interval'))
                 .when(~live).then(pl.lit('interval_ended'))
                 .when(pl.col('_publication_day') > pl.col('date')).then(pl.lit('not_yet_public'))
                 .when(pl.col('_publication_day') == pl.col('date')).then(pl.lit('same_day_clock_review'))
                 .otherwise(pl.lit('bound_prior_publication')))
        bound = bound.with_columns(state.alias(prefix+'binding_status'))
        usable = pl.col(prefix+'binding_status') == 'bound_prior_publication'
        bound = bound.select('date', 'product', prefix+'binding_status',
            # Keep the candidate id even on a gap, so the exact terminating
            # source can be fixed without rereading the complete archive.
            pl.col('margin_interval_id').alias(prefix+'margin_interval_id'),
            *[pl.when(usable).then(pl.col(c)).otherwise(None).alias(prefix+c)
              for c in ('margin_kind', 'initial', 'maintenance', 'known_at')])
        result = result.join(bound, on=['date', 'product'], how='left', validate='1:1')
    return result.sort('date', 'product')


def bind_dated_corporate_terms(days: pl.DataFrame, terms: pl.DataFrame, *, unit_only: bool = False) -> pl.DataFrame:
    """Bind adjusted shares/cash to observed contract dates without bridging gaps.

    Contract codes can be reused. The product/month key alone never supplies
    the multiplier, and a superseded interval must not reappear after a gap.
    Rights remain terminal components, distinct from daily cash deliverables.
    """
    keys=['product','contract']
    query=days.select(pl.col('date').cast(pl.Date),*keys).unique()
    fields=['contract_multiplier','known_at','source_content_sha256s']
    if not unit_only:
        fields+=['deliverable_cash_twd','fixed_subscription_rights_twd','subscription_rights_at_final_settlement']
    required={*keys,'effective_date','valid_until_exclusive',*fields}-{'fixed_subscription_rights_twd'}
    if required-set(terms.columns):raise ValueError('incomplete dated corporate term schema')
    if terms.select(*keys,'effective_date').is_duplicated().any():
        raise ValueError('duplicate dated corporate boundary')
    if not unit_only and 'fixed_subscription_rights_twd' not in terms.columns:
        terms=terms.with_columns(pl.lit(0.).alias('fixed_subscription_rights_twd'))
    elif not unit_only:
        # Older candidate facts lack this optional *already-valued* component.
        # Unknown future rights remain represented by the separate rights flag.
        terms=terms.with_columns(pl.col('fixed_subscription_rights_twd').fill_null(0.))
    if query['date'].null_count() or any(query[c].null_count() for c in keys):
        raise ValueError('corporate queries require dates and physical contract keys')
    numeric=['contract_multiplier'] if unit_only else ['contract_multiplier','deliverable_cash_twd','fixed_subscription_rights_twd']
    if terms.filter(pl.col('known_at').is_null()
            | ~pl.col('known_at').str.contains(r'(Z|[+-]\d{2}:\d{2})$')
            | pl.col('source_content_sha256s').is_null()
            | (pl.col('source_content_sha256s').list.len()==0)
            | (pl.lit(False) if unit_only else pl.col('subscription_rights_at_final_settlement').is_null())
            | pl.any_horizontal([pl.col(c).is_null() | ~pl.col(c).is_finite()
                                 | (pl.col(c)<0) for c in numeric])
            | (pl.col('contract_multiplier')<=0)).height:
        raise ValueError('invalid dated corporate amounts or provenance')
    levels=terms.with_row_index('corporate_term_id').with_columns(
        pl.col('effective_date').str.to_date().alias('_begin'),
        pl.col('valid_until_exclusive').str.to_date().alias('_end'),
        pl.col('known_at').str.to_datetime(time_zone='UTC').dt.convert_time_zone(
            'Asia/Taipei').dt.date().alias('_published'),
    ).sort('_begin')
    if levels.filter(pl.col('_end').is_not_null() & (pl.col('_end')<=pl.col('_begin'))).height:
        raise ValueError('invalid dated corporate interval')
    adjacent=levels.with_columns(pl.col('_begin').shift(-1).over(keys).alias('_next'))
    if adjacent.filter(pl.col('_next').is_not_null() & (pl.col('_end').is_null()
            | (pl.col('_end')>pl.col('_next')))).height:
        raise ValueError('overlapping dated corporate intervals')
    result=query.sort('date').join_asof(levels,left_on='date',right_on='_begin',
        by=keys,strategy='backward',check_sortedness=False)
    result=result.with_columns(
        pl.when(pl.col('_begin').is_null()).then(pl.lit('no_prior_terms'))
        .when(pl.col('_end').is_not_null() & (pl.col('date')>=pl.col('_end'))).then(pl.lit('terms_ended'))
        .when(pl.col('_published').is_null() | (pl.col('_published')>=pl.col('date')))
        .then(pl.lit('publication_clock_unresolved'))
        .otherwise(pl.lit('bound_prior_publication')).alias('terms_binding_status'))
    usable=pl.col('terms_binding_status')=='bound_prior_publication'
    return result.select('date',*keys,'corporate_term_id','terms_binding_status',
        *[pl.when(usable).then(pl.col(c)).otherwise(None).alias(c) for c in fields]
    ).sort('date',*keys)


def terminal_contract_value_twd(price, multiplier, cash=0, rights=0) -> float:
    """Official terminal-value whole-dollar truncation, without binary drift.

    A caller must resolve every deliverable component first. None is a gap,
    including an unresolved subscription right; it is never interpreted as 0.
    """
    from decimal import Decimal, ROUND_FLOOR
    try:parts=[Decimal(str(v)) for v in (price,multiplier,cash,rights)]
    except Exception as exc:raise ValueError('unresolved terminal components') from exc
    if any(not v.is_finite() for v in parts) or min(parts[:2])<=0 or min(parts[2:])<0:
        raise ValueError('invalid terminal components')
    return float((parts[0]*parts[1]+parts[2]+parts[3]).to_integral_value(rounding=ROUND_FLOOR))


def subscription_right_value_twd(stock_close, subscription_price, entitlement_units) -> float:
    """Value rights at the notice's cash-stock fixing, not its futures average."""
    from decimal import Decimal, ROUND_FLOOR
    try:s,k,q=[Decimal(str(v)) for v in (stock_close,subscription_price,entitlement_units)]
    except Exception as exc:raise ValueError('unresolved subscription components') from exc
    if not all(v.is_finite() and v>0 for v in (s,k,q)):
        raise ValueError('invalid subscription components')
    return float((max(s-k,Decimal(0))*q).to_integral_value(rounding=ROUND_FLOOR))


def bind_adjusted_terminal_values(final: pl.DataFrame, terms: pl.DataFrame,
                                  rights: pl.DataFrame | None = None) -> pl.DataFrame:
    """Resolve terminal inputs while preserving original missing official cells.

    This does not promote a release or infer an expiry. Each query must be an
    existing official final-settlement event. Derived values are kept separate
    from the exchange's original full-value field for source inspection.
    """
    keys=['date','product','contract']
    required={*keys,'final_settlement_price','final_settlement_value'}
    if required-set(final.columns) or final.select(keys).is_duplicated().any():
        raise ValueError('terminal values require unique official settlement events')
    if final.filter(~pl.col('product').str.contains(r'\d$')).height:
        raise ValueError('adjusted terminal binder only accepts adjusted product codes')
    bound=bind_dated_corporate_terms(final.select(keys),terms)
    frame=final.join(bound,on=keys,how='left',validate='1:1')
    if rights is not None:
        if rights.select(keys).is_duplicated().any():raise ValueError('ambiguous terminal rights')
        frame=frame.join(rights.select(*keys,'rights_twd','notice_content_sha256'),on=keys,how='left',validate='1:1')
    else:
        frame=frame.with_columns(pl.lit(None,dtype=pl.Float64).alias('rights_twd'),
                                 pl.lit(None,dtype=pl.String).alias('notice_content_sha256'))
    results=[]
    for r in frame.iter_rows(named=True):
        official=r['final_settlement_value'];value=None;status='components_unresolved'
        if official is not None:
            if not np.isfinite(official) or official<=0:raise ValueError('invalid official full value')
            value=official;status='official_full_deliverable'
        elif r['terms_binding_status']=='bound_prior_publication':
            right=r['rights_twd']
            if right is not None:
                if not np.isfinite(right) or right<0:
                    raise ValueError('subscription value must be finite and nonnegative')
                if r['notice_content_sha256'] not in r['source_content_sha256s']:
                    raise ValueError('rights and deliverable terms belong to different notices')
                status='source_bound_subscription_components'
            elif not r['subscription_rights_at_final_settlement']:
                right=0.;status='dated_components_without_future_rights'
            if right is not None:
                value=terminal_contract_value_twd(r['final_settlement_price'],r['contract_multiplier'],
                    r['deliverable_cash_twd'],right+r['fixed_subscription_rights_twd'])
        results.append({**{k:r[k] for k in keys},'official_final_settlement_value':official,
            'terminal_value_input_twd':value,'terminal_value_binding_status':status,
            'corporate_term_id':r['corporate_term_id'],
            'component_source_sha256s':r['source_content_sha256s']})
    return pl.DataFrame(results, schema={
        'date': pl.Date, 'product': pl.String, 'contract': pl.String,
        'official_final_settlement_value': pl.Float64,
        'terminal_value_input_twd': pl.Float64,
        'terminal_value_binding_status': pl.String,
        'corporate_term_id': pl.UInt32,
        'component_source_sha256s': pl.List(pl.String),
    }).sort(keys)


def equity_contract_families(universe: pl.DataFrame) -> pl.DataFrame:
    """Resolve identity relationships, never numeric historical rule levels.

    Adjusted codes belong to their named root. Mini roots resolve through the
    same security, with ambiguity rejected instead of borrowing a neighbour.
    Dated unit, margin and limit rules must still authorize the relationship.
    """
    required={'product','product_name','asset_class','underlying_symbol'}
    if required-set(universe.columns) or universe['product'].n_unique()!=universe.height:
        raise ValueError('families require a unique named product universe')
    equities=universe.filter(pl.col('asset_class').is_in(['stock_future','etf_future']))
    standard=equities.filter(pl.col('product').str.ends_with('F')
                             & ~pl.col('product_name').str.starts_with('小型'))
    if standard['underlying_symbol'].null_count() or standard['underlying_symbol'].n_unique()!=standard.height:
        raise ValueError('ambiguous standard futures security relationship')
    roots={r['underlying_symbol']:r['product'] for r in standard.iter_rows(named=True)}
    lookup={r['product']:r for r in universe.iter_rows(named=True)}
    rows=[]
    for row in universe.iter_rows(named=True):
        product=row['product'];base=product;parent=product;relation='self'
        if row['asset_class'] in ('stock_future','etf_future'):
            if re.search(r'\d$',product):
                parent=product[:-1]+'F'
                if parent not in lookup or lookup[parent]['underlying_symbol']!=row['underlying_symbol']:
                    raise ValueError('adjusted futures root/security mismatch: '+product)
                relation='adjusted'
            if lookup[parent]['product_name'].startswith('小型'):
                base=roots.get(row['underlying_symbol'])
                if not base:raise ValueError('mini future lacks a unique standard parent: '+product)
                relation='adjusted_mini' if relation=='adjusted' else 'mini'
            else:base=parent
        rows.append(dict(product=product,root_product=parent,standard_product=base,
                         family_relation=relation))
    return pl.DataFrame(rows).sort('product')


def bind_equity_margin_families(days: pl.DataFrame, intervals: pl.DataFrame,
                               universe: pl.DataFrame, *, rule_effective_date: date,
                               rule_known_at: str, rule_source_sha256: str) -> pl.DataFrame:
    """Bind the dated same-underlying stock *rate* rule, never ETF fixed sums.

    Join unique product-days once. A source product and interval id accompany
    every inherited amount; adjusted cash and rights are not margin bases.
    Direct evidence has precedence, and a missing family rule stays a gap.
    """
    if not re.fullmatch('[a-f0-9]{64}',rule_source_sha256):
        raise ValueError('same-security margin inheritance requires a source identity')
    known=datetime.fromisoformat(rule_known_at)
    if known.tzinfo is None:raise ValueError('margin family rule requires a timezone')
    families=equity_contract_families(universe).join(universe.select('product','asset_class'),on='product')
    query=days.select('date','product').unique().join(families,on='product',how='left',validate='m:1')
    if query['standard_product'].null_count():raise ValueError('product outside the explicit universe')
    all_days=pl.concat([query.select('date','product'),
        query.select('date',pl.col('standard_product').alias('product'))]).unique()
    aligned=align_product_margin_intervals(all_days,intervals)
    result=query.join(aligned,on=['date','product'],how='left',validate='1:1')
    parent=aligned.rename({c:'parent_'+c for c in aligned.columns if c!='date'})
    result=result.join(parent,left_on=['date','standard_product'],right_on=['date','parent_product'],
                       how='left',validate='m:1')
    for prefix in ('opening_','settlement_'):
        inherited=((pl.col('asset_class')=='stock_future')
            & (pl.col('date')>=rule_effective_date)
            & (pl.col('date')>known.astimezone(TAIPEI).date())
            & (pl.col('product')!=pl.col('standard_product'))
            & (pl.col(prefix+'binding_status')=='no_prior_interval')
            & (pl.col('parent_'+prefix+'binding_status')=='bound_prior_publication')
            & (pl.col('parent_'+prefix+'margin_kind')=='notional_rate'))
        result=result.with_columns(inherited.alias('_inherited'))
        result=result.with_columns(
            pl.when('_inherited').then(pl.col('standard_product')).otherwise(pl.col('product')).alias(prefix+'source_product'),
            pl.when('_inherited').then(pl.lit(rule_source_sha256)).otherwise(None).alias(prefix+'family_rule_sha256'),
            pl.when('_inherited').then(pl.lit('bound_same_security_rate')).otherwise(pl.col(prefix+'binding_status')).alias(prefix+'binding_status'),
            *[pl.when('_inherited').then(pl.col('parent_'+prefix+c)).otherwise(pl.col(prefix+c)).alias(prefix+c)
              for c in ('margin_interval_id','margin_kind','initial','maintenance','known_at')])
    return result.drop([c for c in result.columns if c.startswith('parent_')]+['_inherited']).sort('date','product')


def bind_dated_position_limits(days: pl.DataFrame, intervals: pl.DataFrame) -> pl.DataFrame:
    """Bind explicit caps/conversion formulas at a prior-published boundary.

    The result deliberately retains lifecycle-relative ends and base joins;
    these are not interchangeable with a resolved account position limit.
    """
    required={'product','effective_date','valid_until_date_exclusive','known_at',
        'admission_not_before','source_content_sha256s'}
    if required-set(intervals.columns):raise ValueError('incomplete dated position schema')
    if intervals.select('product','effective_date').is_duplicated().any():
        raise ValueError('duplicate position boundary')
    if intervals.filter(pl.col('known_at').is_null()
            | ~pl.col('known_at').str.contains(r'(Z|[+-]\d{2}:\d{2})$')
            | pl.col('source_content_sha256s').is_null()
            | (pl.col('source_content_sha256s').list.len()==0)).height:
        raise ValueError('position intervals require prior publication and source identities')
    query=days.select(pl.col('date').cast(pl.Date),'product').unique().sort('date')
    if query['date'].null_count() or query['product'].null_count():
        raise ValueError('position queries require dates and products')
    levels=intervals.with_row_index('position_interval_id').with_columns(
        pl.col('effective_date').str.to_date().alias('_begin'),
        pl.col('valid_until_date_exclusive').str.to_date().alias('_end'),
        pl.max_horizontal(pl.col('known_at').str.to_datetime(time_zone='UTC'),
                          pl.col('admission_not_before').str.to_datetime(time_zone='UTC'))
          .dt.convert_time_zone('Asia/Taipei').dt.date().alias('_published')).sort('_begin')
    if levels.filter(pl.col('_end').is_not_null() & (pl.col('_end')<=pl.col('_begin'))).height:
        raise ValueError('invalid position interval end')
    adjacent=levels.with_columns(pl.col('_begin').shift(-1).over('product').alias('_next'))
    if adjacent.filter(pl.col('_next').is_not_null()
            & (pl.col('_end').is_null() | (pl.col('_end')>pl.col('_next')))).height:
        raise ValueError('overlapping position intervals')
    result=query.join_asof(levels,left_on='date',right_on='_begin',by='product',
                           check_sortedness=False,strategy='backward').with_columns(
        pl.when(pl.col('_begin').is_null()).then(pl.lit('no_prior_interval'))
          .when(pl.col('_end').is_not_null() & (pl.col('date')>=pl.col('_end'))).then(pl.lit('interval_ended'))
          .when(pl.col('_published').is_null() | (pl.col('_published')>=pl.col('date')))
          .then(pl.lit('publication_clock_unresolved'))
          .otherwise(pl.lit('bound_prior_publication')).alias('position_binding_status'))
    fields=[c for c in intervals.columns if c not in ('product',)]
    return result.select('date','product','position_interval_id','position_binding_status',
        *[pl.when(pl.col('position_binding_status')=='bound_prior_publication').then(pl.col(c))
            .otherwise(None).alias(c) for c in fields]).sort('date','product')


def bind_dated_position_combinations(days: pl.DataFrame, intervals: pl.DataFrame) -> pl.DataFrame:
    """Resolve explicit mini/adjusted formulas against that day's base cap.

    Query missing parent product-days too: a base need not trade on a day an
    adjusted contract trades. Never use an ended parent or invent an allowance.
    Delisting-relative caps remain labelled as requiring lifecycle admission.
    """
    keys=['date','product']
    wanted=days.select(pl.col('date').cast(pl.Date),'product').unique()
    if 'requires_base_limit_join' not in intervals.columns:
        raise ValueError('dated position combinations require explicit interval formulas')
    edges=intervals.filter(pl.col('requires_base_limit_join')).select(
        'product','combined_position_base_product').unique()
    graph={}
    for child,parent in edges.iter_rows():
        if not parent or child==parent:raise ValueError('invalid position formula parent')
        graph.setdefault(child,set()).add(parent)
    def check_cycle(node,stack):
        if node in stack:raise ValueError('cyclic dated position formula')
        for parent in graph.get(node,()):check_cycle(parent,stack|{node})
    for node in graph:check_cycle(node,set())
    queries=wanted
    for _ in range(len(graph)+1):
        parents=queries.join(edges,on='product',how='inner').select('date',
            pl.col('combined_position_base_product').alias('product'))
        expanded=pl.concat([queries,parents]).unique()
        if expanded.height==queries.height:break
        queries=expanded
    result=bind_dated_position_limits(queries,intervals).with_columns(
        ((pl.col('position_binding_status')=='bound_prior_publication')
            & ~pl.col('requires_base_limit_join').fill_null(True)
            & (pl.col('position_limit')>0)).fill_null(False).alias('_resolved'),
        pl.col('product').alias('position_root_product'),
        pl.lit(None,dtype=pl.UInt32).alias('position_base_interval_id'))
    for _ in range(len(graph)):
        parents=result.select('date',pl.col('product').alias('combined_position_base_product'),
            *[pl.col(c).alias('_base_'+c) for c in ('position_limit','monthly_position_limit',
                'position_unit','unit','position_root_product','position_interval_id','known_at',
                'source_content_sha256s','requires_delisting_clock','_resolved')])
        result=result.join(parents,on=['date','combined_position_base_product'],how='left',validate='m:1')
        usable=((pl.col('position_binding_status')=='bound_prior_publication')
            & pl.col('requires_base_limit_join') & ~pl.col('_resolved')
            & pl.col('_base__resolved').fill_null(False)
            & (pl.col('conversion_numerator')>0) & (pl.col('conversion_denominator')>0))
        count=result.select(usable.sum()).item()
        if not count:
            result=result.drop([c for c in result.columns if c.startswith('_base_')]);break
        result=result.with_columns(
            *[pl.when(usable).then(pl.col('_base_'+c)).otherwise(pl.col(c)).alias(c)
              for c in ('position_limit','monthly_position_limit','position_root_product','unit')],
            pl.when(usable).then(pl.col('_base_position_unit')*pl.col('conversion_numerator')
                /pl.col('conversion_denominator')).otherwise(pl.col('position_unit')).alias('position_unit'),
            pl.when(usable).then(pl.col('_base_position_interval_id')).otherwise(
                pl.col('position_base_interval_id')).alias('position_base_interval_id'),
            pl.when(usable).then(pl.max_horizontal(
                pl.col('known_at').str.to_datetime(time_zone='UTC'),
                pl.col('_base_known_at').str.to_datetime(time_zone='UTC'))
                .dt.to_string('%Y-%m-%dT%H:%M:%S%:z'))
                .otherwise(pl.col('known_at')).alias('known_at'),
            pl.when(usable).then(pl.concat_list('source_content_sha256s','_base_source_content_sha256s').list.unique().list.sort())
                .otherwise(pl.col('source_content_sha256s')).alias('source_content_sha256s'),
            pl.when(usable).then(pl.col('requires_delisting_clock')|pl.col('_base_requires_delisting_clock'))
                .otherwise(pl.col('requires_delisting_clock')).alias('requires_delisting_clock'),
            (pl.col('_resolved')|usable).alias('_resolved'),
        ).drop([c for c in result.columns if c.startswith('_base_')])
    return result.join(wanted,on=keys,how='semi').with_columns(
        pl.when(pl.col('requires_base_limit_join') & pl.col('_resolved'))
          .then(pl.lit('bound_dated_combination'))
          .when(pl.col('requires_base_limit_join') & ~pl.col('_resolved'))
          .then(pl.lit('base_position_limit_unresolved'))
          .otherwise(pl.col('position_binding_status')).alias('position_binding_status'),
        pl.col('_resolved').alias('position_numeric_inputs_resolved'),
    ).drop('_resolved').sort(keys)


def bind_equity_position_families(days: pl.DataFrame, intervals: pl.DataFrame,
        universe: pl.DataFrame, terms: pl.DataFrame, family_rules: list[dict]) -> pl.DataFrame:
    """Apply dated same-security grouping laws without inventing a cap.

    A changed-share adjusted contract may inherit a securities-unit cap, but
    never a plain contract-count cap. Its temporary enlarged limit must come
    from the actual notice. Direct ended intervals remain gaps.
    """
    keys=['date','product'];families=equity_contract_families(universe)
    wanted=days.select(pl.col('date').cast(pl.Date),'product').unique().join(
        families,on='product',how='left',validate='m:1')
    query=pl.concat([wanted.select(keys),wanted.select('date',
        pl.col('standard_product').alias('product'))]).unique()
    direct=bind_dated_position_combinations(query,intervals)
    parent=direct.rename({c:'_base_'+c for c in direct.columns if c!='date'}).rename(
        {'_base_product':'standard_product'})
    original=direct.join(wanted.select(keys),on=keys,how='semi')
    frame=wanted.join(direct,on=keys,how='left',validate='1:1').filter(
        (pl.col('family_relation')!='self') & (pl.col('position_binding_status')=='no_prior_interval')).join(
        parent,on=['date','standard_product'],how='left',validate='m:1').join(
        universe.select('product','asset_class'),on='product',how='left',validate='m:1')
    known_unit=pl.col('terms_binding_status')=='bound_prior_publication'
    unit_terms=terms.group_by(keys).agg(
        known_unit.any().alias('_terms_bound'),
        pl.col('contract_multiplier').filter(known_unit).n_unique().alias('_unit_count'),
        pl.col('contract_multiplier').filter(known_unit).first().alias('_units'),
        pl.col('source_content_sha256s').explode(empty_as_null=True, keep_nulls=True)
        .drop_nulls().unique().sort().alias('_term_sources'))
    frame=frame.join(unit_terms,on=keys,how='left',validate='1:1')
    for law in sorted(family_rules,key=lambda r:r['effective_date']):
        digest=law['source_content_sha256']
        if not re.fullmatch(r'[a-f0-9]{64}',digest):raise ValueError('position family law requires a source identity')
        beginning=date.fromisoformat(law['effective_date']);known=datetime.fromisoformat(law['known_at'])
        if known.tzinfo is None:raise ValueError('position family law clock requires a timezone')
        standard=float(law['standard_units']);mini=law.get('mini_units')
        if standard<=0 or (mini is not None and not 0<float(mini)<standard):
            raise ValueError('invalid dated standard/mini units')
        relation=pl.col('family_relation');adjusted=relation.is_in(['adjusted','adjusted_mini'])
        units=pl.when(adjusted).then(pl.col('_units')).otherwise(pl.lit(mini,dtype=pl.Float64))
        # A law without minis still applies to unchanged standard-size
        # adjusted contracts.
        family=((relation=='mini') & pl.lit(mini is not None)) | (
            adjusted & pl.col('_terms_bound').fill_null(False) & (pl.col('_unit_count')==1))
        applicable=((pl.col('asset_class')==law['asset_class']) & (pl.col('date')>=beginning)
            & (pl.col('date')>known.astimezone(TAIPEI).date()) & family
            & (pl.col('position_binding_status')=='no_prior_interval')
            & pl.col('_base_position_numeric_inputs_resolved').fill_null(False))
        shares=pl.col('_base_unit').is_in(['shares','beneficial_units'])
        unchanged=(units==standard) | ((units==float(mini)) if mini is not None else pl.lit(False))
        applicable=(applicable & (shares | (pl.col('_base_unit').eq('contracts') & unchanged))).fill_null(False)
        inherit=[c for c in direct.columns if c not in keys+['position_interval_id','position_binding_status',
            'position_unit','position_base_interval_id','known_at','source_content_sha256s','effective_date']]
        frame=frame.with_columns(
            *[pl.when(applicable).then(pl.col('_base_'+c)).otherwise(pl.col(c)).alias(c) for c in inherit],
            pl.when(applicable).then(pl.when(shares).then(units).otherwise(units/standard))
                .otherwise(pl.col('position_unit')).alias('position_unit'),
            pl.when(applicable).then(pl.col('_base_position_interval_id')).otherwise(pl.col('position_base_interval_id'))
                .alias('position_base_interval_id'),
            pl.when(applicable).then(pl.max_horizontal('_base_effective_date',pl.lit(str(beginning))))
                .otherwise(pl.col('effective_date')).alias('effective_date'),
            pl.when(applicable).then(pl.max_horizontal(pl.col('_base_known_at').str.to_datetime(time_zone='UTC'),
                pl.lit(known).cast(pl.Datetime('us','UTC'))).dt.to_string('%Y-%m-%dT%H:%M:%S%:z'))
                .otherwise(pl.col('known_at')).alias('known_at'),
            pl.when(applicable).then(pl.concat_list(pl.col('_base_source_content_sha256s'),
                pl.col('_term_sources').fill_null(pl.lit([],dtype=pl.List(pl.String))),pl.lit([digest])).list.unique().list.sort())
                .otherwise(pl.col('source_content_sha256s')).alias('source_content_sha256s'),
            pl.when(applicable).then(pl.lit('bound_same_security_position')).otherwise(pl.col('position_binding_status'))
                .alias('position_binding_status'))
    updated=frame.select(direct.columns)
    result=pl.concat([original.join(updated.select(keys),on=keys,how='anti'),updated],
                    how='vertical_relaxed')
    # A product code can be reused after another corporate adjustment. An old
    # one-for-one formula or share conversion must not survive a changed unit
    # merely because no later position table was successfully extracted.
    checked=result.join(families,on='product',how='left',validate='m:1').join(
        universe.select('product','asset_class'),on='product',how='left',validate='m:1').join(
        unit_terms,on=keys,how='left',validate='1:1')
    relevant=(pl.col('family_relation').is_in(['adjusted','adjusted_mini'])
        & pl.col('position_numeric_inputs_resolved')
        & (pl.col('unit').is_in(['shares','beneficial_units'])
           | (pl.col('event_type')=='combined_position_formula'))).fill_null(False)
    valid_units=pl.lit(False)
    for law in sorted(family_rules,key=lambda r:r['effective_date']):
        known=datetime.fromisoformat(law['known_at']).astimezone(TAIPEI).date()
        applies=((pl.col('asset_class')==law['asset_class'])
            & (pl.col('date')>=date.fromisoformat(law['effective_date']))
            & (pl.col('date')>known))
        expected=pl.when(pl.col('unit').is_in(['shares','beneficial_units'])).then(
            pl.col('_units')).otherwise(pl.col('_units')/float(law['standard_units']))
        valid_units=valid_units | (applies & pl.col('_terms_bound').fill_null(False)
            & (pl.col('_unit_count')==1) & ((pl.col('position_unit')-expected).abs()<1e-8))
    invalid=relevant & ~valid_units.fill_null(False)
    checked=checked.with_columns(
        pl.when(invalid).then(pl.lit('dated_position_units_unresolved'))
            .otherwise(pl.col('position_binding_status')).alias('position_binding_status'),
        (pl.col('position_numeric_inputs_resolved') & ~invalid).alias('position_numeric_inputs_resolved'),
        *[pl.when(invalid).then(None).otherwise(pl.col(c)).alias(c)
          for c in ('position_limit','monthly_position_limit')])
    # This is only a product-day input. A missing month must not hide another
    # month's explicit source units, and must not borrow that month's units.
    # The physical join below is mandatory before counting resolved inputs.
    return checked.select(result.columns).with_columns(
        pl.lit(True).alias('requires_contract_day_unit_check')).sort(keys)


def bind_physical_position_inputs(days: pl.DataFrame, positions: pl.DataFrame,
        units: pl.DataFrame, universe: pl.DataFrame, family_rules: list[dict]) -> pl.DataFrame:
    """Check every physical month's units independently against a dated cap.

    One missing month's terms cannot disable a fully observed peer or acquire
    its units by filling. This resolves numeric inputs only; legal lifecycle,
    grouping and full account admission are still separate.
    """
    keys=['date','product','contract']
    if units.select(keys).is_duplicated().any() or positions.select('date','product').is_duplicated().any():
        raise ValueError('duplicate position-unit input identity')
    frame=days.select(keys).unique().join(positions,on=['date','product'],how='left',validate='m:1').join(
        units.select(*keys,'terms_binding_status',pl.col('contract_multiplier').alias('_units')),
        on=keys,how='left',validate='1:1').join(
        universe.select('product','asset_class'),on='product',how='left',validate='m:1')
    adjusted=pl.col('product').str.contains(r'\d$') & pl.col('asset_class').is_in(['stock_future','etf_future'])
    relevant=(adjusted & (pl.col('unit').is_in(['shares','beneficial_units'])
                         | (pl.col('event_type')=='combined_position_formula'))).fill_null(False)
    valid=pl.lit(False)
    for law in family_rules:
        known=datetime.fromisoformat(law['known_at']).astimezone(TAIPEI).date()
        expected=pl.when(pl.col('unit').is_in(['shares','beneficial_units'])).then(
            pl.col('_units')).otherwise(pl.col('_units')/float(law['standard_units']))
        valid=valid | ((pl.col('asset_class')==law['asset_class'])
            & (pl.col('date')>=date.fromisoformat(law['effective_date'])) & (pl.col('date')>known)
            & (pl.col('terms_binding_status')=='bound_prior_publication')
            & ((pl.col('position_unit')-expected).abs()<1e-8))
    rejected=relevant & ~valid.fill_null(False)
    resolved=pl.col('position_numeric_inputs_resolved').fill_null(False) & ~rejected
    return frame.with_columns(
        resolved.alias('position_numeric_inputs_resolved'),
        pl.when(rejected).then(pl.lit('physical_position_units_unresolved'))
          .otherwise(pl.col('position_binding_status')).alias('position_binding_status'),
        *[pl.when(resolved).then(pl.col(c)).otherwise(None).alias(c)
          for c in ('position_limit','monthly_position_limit')],
        pl.lit(False).alias('training_admitted'),
    ).drop('_units').sort(keys)


def corporate_identity_boundaries(facts: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Locate code transfers, independently of unresolved cash/share amounts.

    These boundaries organize evidence only. They do not admit a conversion,
    resolve a cash amount, or create a fill. Conflicting destinations fail here.
    """
    rows=[]
    for fact in facts.iter_rows(named=True):
        origin=fact.get('from_product');target=fact.get('product');day=fact.get('effective_date')
        if not origin or origin==target or not day or not fact.get('contract_months'):continue
        try:day=date.fromisoformat(day)
        except (ValueError,TypeError):continue
        if not re.fullmatch('[a-f0-9]{64}',fact.get('source_content_sha256') or ''):
            raise ValueError('corporate identity boundary lacks a source hash')
        for month in fact['contract_months']:
            if not re.fullmatch(r'\d{6}',month):raise ValueError('invalid corporate boundary contract month')
            rows.append(dict(product=origin,contract=month,corporate_boundary=day,
                corporate_transfer_target=target,source_content_sha256=fact['source_content_sha256']))
    if not rows:raise ValueError('no source-bound corporate identity transfers')
    edges=pl.DataFrame(rows).unique()
    if edges.group_by('product','contract','corporate_boundary').agg(
            pl.col('corporate_transfer_target').n_unique().alias('targets')).filter(pl.col('targets')!=1).height:
        raise ValueError('conflicting corporate identity transfer destinations')
    edges=edges.group_by('product','contract','corporate_boundary','corporate_transfer_target').agg(
        pl.col('source_content_sha256').unique().sort().alias('corporate_boundary_sources'))
    boundaries=pl.concat([edges.select('product','contract','corporate_boundary'),
        edges.select(pl.col('corporate_transfer_target').alias('product'),'contract','corporate_boundary')]).unique()
    boundaries=boundaries.sort('product','contract','corporate_boundary').with_columns(
        pl.col('corporate_boundary').rank('dense').over('product','contract').cast(pl.UInt32).alias('corporate_generation'))
    return boundaries,edges


def load_same_security_final_fixing_rule(path: Path) -> dict:
    """Read a portable original rule plus its separate commencement notice."""
    review=json.loads(path.read_text())
    if review.get('review_kind')!='source_bound_same_security_final_fixing':
        raise ValueError('unsupported underlying fixing review')
    documents={};raw_hashes=set()
    for source in review['sources']:
        file=path.parent/source['path']
        if not file.resolve().is_relative_to(path.parent.resolve()) or sha256_file(file)!=source['sha256']:
            raise ValueError('underlying fixing source SHA/path mismatch')
        if source['kind']=='raw_gzip':raw_hashes.add(hashlib.sha256(gzip.decompress(file.read_bytes())).hexdigest())
        elif source['kind']=='parsed_v2':documents[source['url']]=json.loads(file.read_text())
    law=documents[review['law_source_url']];commencement=documents[review['commencement_source_url']]
    if any(doc['content_sha256'] not in raw_hashes for doc in (law,commencement)):
        raise ValueError('underlying fixing extraction lacks an original')
    body=compact(law['text']);clock=compact(commencement['text'])
    for clause in ('股票期貨契約之最後結算價,以最後結算日證券市場當日交易時間收盤前六十分鐘內標的證券之算術平均價訂之',
                   '未了結之部位於最後結算日依最後結算價計算約定標的物價值'):
        if clause not in body:raise ValueError('same-security fixing rule clause missing')
    beginning=date.fromisoformat(review['effective_date'])
    literal=f'自{beginning.year-1911}年{beginning.month}月{beginning.day}日起實施'
    if literal not in clock or '第09800110800號公告' not in clock:
        raise ValueError('underlying fixing commencement does not bind the original rule')
    published=sorted({r['date_iso'] for r in temporal_mentions(commencement['text']) if r['role']=='publication'})
    if published!=[review['known_at'][:10]]:
        raise ValueError('underlying fixing publication clock mismatch')
    return dict(law_effective_date=beginning,law_known_at=review['known_at'],
                law_source_sha256=law['content_sha256'])


def derive_adjusted_final_fixings(observed: pl.DataFrame, final: pl.DataFrame,
        terms: pl.DataFrame, universe: pl.DataFrame, *, law_effective_date: date,
        law_known_at: str, law_source_sha256: str) -> pl.DataFrame:
    """Derive missing adjusted fixings from the same security/month's final.

    The statutory fixing is an underlying-security observation, shared by the
    standard and adjusted contract. Cash/rights/quantity remain separate and
    do NOT become the standard contract's value. This is a derived input, not
    an original exchange row or permission to trade. Never borrow a daily
    close, another month, an index, or a fixing across a corporate boundary.
    """
    known=datetime.fromisoformat(law_known_at)
    if known.tzinfo is None or not re.fullmatch(r'[a-f0-9]{64}',law_source_sha256):
        raise ValueError('underlying fixing rule requires a dated source identity')
    if not {'settlement_method','reported_date_role','source_sha256'}<=set(final.columns):
        raise ValueError('underlying fixing requires semantic official settlement evidence')
    family=equity_contract_families(universe).join(
        universe.select('product','asset_class'),on='product',validate='1:1').filter(
        (pl.col('asset_class')=='stock_future') & pl.col('family_relation').is_in(['adjusted','adjusted_mini']))
    observed=observed.select('product','contract',pl.col('date').cast(pl.Date)).unique()
    scope=observed.select('product','contract').unique().join(family,on='product',how='inner')
    roots=final.filter((pl.col('settlement_method')=='cash_settlement')
        & (pl.col('reported_date_role')=='final_settlement_day')).rename({'product':'standard_product'})
    pending=scope.join(roots,on=['standard_product','contract'],how='inner').rename({'settlement_date':'date'})
    pending=pending.filter((pl.col('date')>=law_effective_date)
        & (pl.col('date')>known.astimezone(TAIPEI).date()) & (pl.col('date')<=observed['date'].max()))
    keys=['product','contract','date']
    pending=pending.join(final.select('product','contract',pl.col('settlement_date').alias('date')),
        on=keys,how='anti')
    if pending.select(keys).is_duplicated().any():
        raise ValueError('ambiguous same-security final fixing')
    bound=bind_dated_corporate_terms(pending.select(keys),terms).join(
        terms.with_row_index('corporate_term_id').select('corporate_term_id','effective_date'),
        on='corporate_term_id',how='left',validate='m:1')
    candidates=pending.join(bound,on=keys,how='inner',validate='1:1').filter(
        (pl.col('terms_binding_status')=='bound_prior_publication')
        & pl.col('final_settlement_price').is_finite() & (pl.col('final_settlement_price')>0))
    # A planned delivery month does not prove that an adjusted contract
    # survived to expiry. In particular, zero-OI contracts cease listing.
    # Require that very contract in the official report on the fixing day;
    # a standard contract's later expiry must never extend its sibling's life.
    candidates=candidates.join(observed,on=keys,how='semi')
    rows=[]
    for row in candidates.iter_rows(named=True):
        end=row['date']
        original={k:row[k] for k in final.columns if k not in ('product','settlement_date','final_settlement_value','source_kind')}
        original.update(product=row['product'],settlement_date=end,
            final_settlement_value=None,source_kind='derived_same_security_official_final_fixing',
            final_fixing_origin='derived_same_security_official_final_fixing',
            fixing_source_product=row['standard_product'],
            fixing_law_source_sha256=law_source_sha256,
            fixing_corporate_source_sha256s=row['source_content_sha256s'],
            fixing_term_id=row['corporate_term_id'],candidate_only=True)
        rows.append(original)
    schema=dict(final.schema,final_fixing_origin=pl.String,fixing_source_product=pl.String,
        fixing_law_source_sha256=pl.String,fixing_corporate_source_sha256s=pl.List(pl.String),
        fixing_term_id=pl.UInt32,candidate_only=pl.Boolean)
    return pl.DataFrame(rows,schema=schema,strict=False) if rows else pl.DataFrame(schema=schema)


def load_adjusted_zero_oi_rule(path: Path) -> dict:
    """Bind early delisting to the retained original law and commencement."""
    rule=load_same_security_final_fixing_rule(path)
    review=json.loads(path.read_text())
    source=next(s for s in review['sources']
                if s['url']==review['law_source_url'] and s['kind']=='parsed_v2')
    body=compact(json.loads((path.parent/source['path']).read_text())['text'])
    clause=('第二十九條本公司對經調整之契約,不再加掛新月份契約;其任一月份契約於任一營業日收盤後,'
            '未了結部位數為零者,自次一營業日起,終止該月份契約之掛牌。')
    if clause not in body:
        raise ValueError('adjusted zero-open-interest delisting clause missing')
    return rule


def physical_lifetime_calendar(observed: pl.DataFrame, final: pl.DataFrame,
                               market_dates: pl.DataFrame,
                               corporate: pl.DataFrame | None = None,
                               delisting_rule: dict | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Partition observed codes/months by official settlement events.

    A code/month can be reused after an early settlement (DJF in 2010).
    Separate those legal lives before computing lagged marks or assigning a
    tensor slot. Expand only each observed life, not contracts x all dates.
    Missing expired-contract evidence remains an explicit unresolved status.
    Corporate F-to-adjusted transfers and reissued base codes are not resolved
    by final settlements alone. These intervals therefore remain observation
    partitions, not verified legal identities or a training admission receipt.
    """
    keys = ['product', 'contract']
    dates = market_dates.select(pl.col('date').cast(pl.Date)).unique().sort('date')
    end = dates['date'].max()
    evidence=[]
    if delisting_rule is not None:
        evidence=['open_interest','asset_class','source_sha256']
        if set(evidence)-set(observed.columns):
            raise ValueError('zero-OI delisting requires sourced open interest and asset class')
        if (not re.fullmatch('[a-f0-9]{64}',delisting_rule['law_source_sha256'])
                or datetime.fromisoformat(delisting_rule['law_known_at']).tzinfo is None):
            raise ValueError('zero-OI rule requires a dated source identity')
    obs = observed.select('date', *keys,*evidence).unique().sort('date')
    if obs.select('date',*keys).is_duplicated().any():
        raise ValueError('conflicting observed contract-days')
    semantics = [name for name in ('reported_date_role','settlement_method','final_fixing_origin',
        'fixing_source_product','fixing_law_source_sha256','fixing_corporate_source_sha256s') if name in final.columns]
    final = final.select(*keys, pl.col('settlement_date').alias('official_expiry'),
                         'final_settlement_price', 'final_settlement_value', *semantics)
    if corporate is not None:
        boundaries,edges=corporate_identity_boundaries(corporate)
        def generation(frame,clock):
            return frame.sort(clock).join_asof(boundaries.sort('corporate_boundary'),left_on=clock,
                right_on='corporate_boundary',by=keys,strategy='backward',check_sortedness=False).drop(
                'corporate_boundary').with_columns(pl.col('corporate_generation').fill_null(0))
        obs=generation(obs,'date');final=generation(final,'official_expiry')
        keys=[*keys,'corporate_generation']
    if final.select(*keys, 'official_expiry').is_duplicated().any():
        raise ValueError('duplicate official final settlement event')
    if final.filter(~pl.col('final_settlement_price').is_finite()
                    | (pl.col('final_settlement_price') <= 0)).height:
        raise ValueError('invalid final settlement price')
    # Future effective settlement events partition observation time. They are
    # bookkeeping identities, never features available to the model in advance.
    tagged = obs.join_asof(final.sort('official_expiry'), left_on='date',
        right_on='official_expiry', by=keys, strategy='forward', check_sortedness=False)
    # Official source joins use product:month. Generation belongs only to the
    # legal-instance discriminator, never to the exchange's quote lookup key.
    tagged = tagged.with_columns(pl.concat_str('product','contract', separator=':').alias('physical_contract'))
    tagged = tagged.with_columns(pl.concat_str('physical_contract', pl.lit('@'),
        pl.col('official_expiry').cast(pl.String).fill_null('unsettled')).alias('physical_instance'))
    if corporate is not None:
        tagged=tagged.with_columns(pl.concat_str('physical_instance',pl.lit('#generation='),
            pl.col('corporate_generation').cast(pl.String)).alias('physical_instance'))
    lives = tagged.group_by(*keys, 'physical_contract', 'physical_instance', 'official_expiry',
                            'final_settlement_price', 'final_settlement_value', *semantics).agg(
        pl.col('date').min().alias('first_observed_date'),
        pl.col('date').max().alias('last_observed_date'),
        *([pl.col('open_interest').sort_by('date').last().alias('last_open_interest'),
           pl.col('source_sha256').sort_by('date').last().alias('last_observation_source_sha256'),
           pl.col('asset_class').first().alias('asset_class'),
           pl.col('date').filter(pl.col('open_interest')==0).min().alias('first_zero_oi_date')]
          if delisting_rule is not None else []))
    # A still-observed identity at EOF needs no invented cash settlement.
    # Other identities without an official event need further source evidence.
    lives = lives.with_columns(
        pl.when(pl.col('official_expiry').is_not_null()).then(pl.lit('official_final'))
        .when(pl.col('last_observed_date') == end).then(pl.lit('observed_at_dataset_boundary'))
        .otherwise(pl.lit('unresolved_expiry')).alias('lifetime_status'),
        pl.min_horizontal(pl.coalesce('official_expiry', 'last_observed_date'), pl.lit(end))
        .alias('calendar_end'))
    if 'final_fixing_origin' in lives.columns:
        lives=lives.with_columns(pl.when(pl.col('final_fixing_origin')=='derived_same_security_official_final_fixing')
            .then(pl.lit('derived_same_security_final')).otherwise(pl.col('lifetime_status')).alias('lifetime_status'))
    if corporate is not None:
        # A transfer happens before the new code's day session. Old identity
        # evidence ends on the preceding calendar day; the session-calendar
        # join below removes weekends/holidays. No liquidation is invented.
        lives=lives.sort('last_observed_date').join_asof(edges.sort('corporate_boundary'),
            left_on='last_observed_date',right_on='corporate_boundary',by=['product','contract'],
            strategy='forward',allow_exact_matches=False,check_sortedness=False)
        handoff=(pl.col('corporate_boundary').is_not_null() & (pl.col('corporate_boundary')<=pl.lit(end)) &
            (pl.col('official_expiry').is_null() | (pl.col('corporate_boundary')<=pl.col('official_expiry'))))
        lives=lives.with_columns(
            pl.when(handoff).then(pl.lit('corporate_transfer_candidate')).otherwise(pl.col('lifetime_status')).alias('lifetime_status'),
            pl.when(handoff).then(pl.min_horizontal(pl.col('corporate_boundary')-pl.duration(days=1),pl.lit(end)))
            .otherwise(pl.col('calendar_end')).alias('calendar_end'),
            pl.lit(True).alias('corporate_identity_candidate_only'))
    if delisting_rule is not None:
        beginning=delisting_rule['law_effective_date']
        known=datetime.fromisoformat(delisting_rule['law_known_at']).astimezone(TAIPEI).date()
        # This is a listing boundary, not a cash-settlement price or an
        # automatic liquidation of simulated inventory. Conflicting earlier
        # zero-OI observations remain unresolved rather than guessed away.
        eligible=((pl.col('lifetime_status')=='unresolved_expiry')
            & (pl.col('asset_class')=='stock_future') & pl.col('product').str.contains(r'\d$')
            & (pl.col('last_open_interest')==0)
            & (pl.col('first_zero_oi_date')==pl.col('last_observed_date'))
            & (pl.col('last_observed_date')>=beginning) & (pl.col('last_observed_date')>known)
            & pl.col('last_observation_source_sha256').str.contains(r'^[a-f0-9]{64}$')).fill_null(False)
        next_days=dates.select(pl.col('date').alias('last_observed_date'),
                              pl.col('date').shift(-1).alias('_next_session'))
        lives=lives.join(next_days,on='last_observed_date',how='left',validate='m:1').with_columns(
            pl.when(eligible & pl.col('_next_session').is_not_null())
              .then(pl.lit('zero_open_interest_delisting')).otherwise(pl.col('lifetime_status')).alias('lifetime_status'))
        delisted=pl.col('lifetime_status')=='zero_open_interest_delisting'
        lives=lives.with_columns(
            pl.when(delisted).then(pl.col('_next_session')).otherwise(None).alias('delisting_date'),
            pl.when(delisted).then(pl.lit(delisting_rule['law_source_sha256'])).otherwise(None).alias('delisting_law_sha256'),
        ).drop('_next_session')
    calendar = (lives.with_columns(pl.date_ranges('first_observed_date', 'calendar_end').alias('date'))
        .explode('date', empty_as_null=True, keep_nulls=True)
        .join(dates, on='date', how='semi').sort('date', 'physical_instance'))
    if calendar.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('officially partitioned physical lifetimes overlap')
    return calendar, lives.sort(*keys, 'first_observed_date')


def load_preparation_final_settlements(path: Path) -> pl.DataFrame:
    """Verify mixed asset-class evidence without admitting cash execution.

    Schema 2 carries a reported-date role and settlement method. A government
    bond's physical-delivery quote must not enter a stock cash-settlement API.
    Keep the existing schema-1 execution reader strict and unchanged.
    """
    from stockagent.data.tw_stock_futures_carry import load_final_settlements
    receipt=json.loads(path.with_name('manifest.json').read_text())
    if receipt.get('schema_version')==1:
        load_final_settlements(path)
        return pl.read_parquet(path)
    if (receipt.get('schema_version')!=2 or receipt.get('status')!='complete'
            or receipt.get('outputs',{}).get('futures_final_settlement_history',{}).get('sha256')!=sha256_file(path)
            or receipt.get('requires_product_specific_settlement_clock') is not True):
        raise ValueError('mixed final settlement receipt/SHA mismatch')
    sources=receipt.get('receipts',[])
    if not sources:
        raise ValueError('mixed final settlements require source receipts')
    bound=set()
    for source in sources:
        source_path=Path(source['path'])
        if not source_path.is_absolute(): source_path=path.parent/source_path
        if sha256_file(source_path)!=source['sha256']:
            raise ValueError('mixed final settlement raw source SHA mismatch')
        bound.add(source['sha256'])
    frame=pl.read_parquet(path)
    required={'product','contract','settlement_date','final_settlement_price','final_settlement_value',
              'reported_date_role','settlement_method','source_sha256'}
    if required-set(frame.columns):
        raise ValueError('mixed final settlements lack semantic/source fields')
    if frame.select('product','contract','settlement_date').is_duplicated().any():
        raise ValueError('duplicate mixed final settlement event')
    if frame.filter(pl.col('final_settlement_price').is_null() | ~pl.col('final_settlement_price').is_finite()
                    | (pl.col('final_settlement_price')<=0)).height:
        raise ValueError('invalid mixed final settlement price')
    if (frame['source_sha256'].null_count() or not set(frame['source_sha256'])<=bound
            or frame['reported_date_role'].null_count() or frame['settlement_method'].null_count()
            or not set(frame['reported_date_role']) <= {'last_trading_day','final_settlement_day'}
            or not set(frame['settlement_method']) <= {'cash_settlement','physical_delivery'}):
        raise ValueError('unbound/unsupported mixed final settlement semantics')
    if frame.filter((pl.col('settlement_method')=='physical_delivery')
                    & (pl.col('reported_date_role')!='last_trading_day')).height:
        raise ValueError('physical delivery source has an unsupported reported-date role')
    return frame


def compact(text: str) -> str:
    # OCR can emit simplified glyphs for the same Chinese label. This changes
    # labels only, never digits, punctuation, Latin contract codes or units.
    # In particular 2.000 stays 2.000; I/1 and O/0 are never corrected here.
    labels = str.maketrans('为数约买卖权减项标证现币调转价缴参与终业股',
                          '為數約買賣權減項標證現幣調轉價繳參與終業股')
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", text)).translate(labels)


def roc_date(match) -> date:
    return date(int(match[1]) + 1911, int(match[2]), int(match[3]))


def timestamp(day: date, clock: str) -> str:
    return datetime.combine(day, time.fromisoformat(clock), TAIPEI).isoformat()


def margin_effective(text: str) -> str:
    matches = list(re.finditer(r"自" + ROC_DATE + r"(一般)?交易時段結束後", compact(text)))
    if any(not m[4] and roc_date(m) >= date(2017, 5, 15) for m in matches):
        raise ValueError("post-night-launch notice must identify the regular close")
    values = {timestamp(roc_date(m), "13:45:00") for m in matches}
    if len(values) != 1:
        raise ValueError(f"ambiguous/missing margin effective close: {values}")
    return values.pop()


def check_publication(text: str, published: str) -> None:
    """Bind reusable attachment names to their historical issuing notice."""
    issued = {m['date_iso'] for m in temporal_mentions(text) if m['role'] == 'publication'}
    if len(issued) != 1:
        raise ValueError("margin/position attachment lacks issuing date")
    lag = (date.fromisoformat(published) - date.fromisoformat(issued.pop())).days
    if not 0 <= lag <= 7:
        raise ValueError("historical attachment issuing date disagrees with index")


def margin_pdf_values(text: str, product: str) -> tuple[int, ...]:
    # The layout is product-local: never take a unit from another table.
    text = unicodedata.normalize("NFKC", text)
    blocks = re.split(r"單位\s*[:：]\s*", text)
    found = []
    for block in blocks[1:]:
        if not re.match(r"新[臺台]幣元\s+" + re.escape(product) + r"(?:\s|$)", block):
            continue
        dense = compact(block)
        header = "調整後保證金金額調整前保證金金額"
        if header not in dense:
            raise ValueError("unverified PDF before/after column order")
        columns = "原始保證金金額維持保證金金額結算保證金金額" * 2
        if columns not in dense:
            raise ValueError("unverified PDF margin column semantics")
        tail = re.split(r"保證金\s+(?=[\d,]+\s)", block)[-1]
        values = re.findall(r"(?<!\d)\d[\d,]*(?!\d)", tail)
        if len(values) < 6:
            raise ValueError("missing six PDF margin amounts")
        found.append(tuple(int(x.replace(",", "")) for x in values[:6]))
    if len(set(found)) != 1:
        raise ValueError(f"missing/ambiguous {product} TWD PDF table")
    return found[0]


def margin_table_candidates(text: str) -> list[dict]:
    """Read product-local before/after tables; never infer units by magnitude.

    This is a candidate extractor. A caller must still bind the announcement,
    effective phases, extraction evidence and complete transition chain.
    Options and incomplete/OCR-ambiguous tables do not become futures facts.
    """
    from decimal import Decimal, InvalidOperation
    text = unicodedata.normalize('NFKC', text)
    blocks = re.split(r'單位\s*[:：]\s*', text)[1:]
    out = []
    for block in blocks:
        kind = ('fixed_twd' if re.match(r'新[臺台]幣元', block) else
                'notional_rate' if re.match(r'比例\s*\(%\)', block) else None)
        if kind is None:
            continue
        # Explicitly retain after/before and column semantics in each table.
        dense = compact(block)
        if not re.search(r'調整後保證金(?:金額|適用比例).*調整前保證金(?:金額|適用比例)', dense):
            continue
        prefix = re.split(r'^\s*(?:保證金|比例)\s+', block, maxsplit=1, flags=re.M)
        if len(prefix) != 2:
            continue
        # Both inline labels and a two-row header (原始 維持 結算 / 保證金)
        # encode the same six ordered columns. Read labels before numeric rows.
        if re.findall(r'原始|維持|結算', prefix[0]) != ['原始','維持','結算'] * 2:
            continue
        # Issuer names such as (TPK-KY期貨) contain English words, not extra
        # contract codes. Require a code cell/line or explicit code parentheses.
        # Never drop the legitimate JNF/EPF table because KY looks like a root.
        codes = set(re.findall(r'^\s*([A-Z][A-Z0-9]{1,2})(?=\s|$)', prefix[0], flags=re.M))
        codes.update(re.findall(r'\(([A-Z][A-Z0-9]{1,2})\)', prefix[0]))
        codes.update(re.findall(r'(?<![A-Z0-9])(?:[A-Z][A-Z0-9][FO]|MTX|TX|TE|TF)(?![A-Z0-9])',prefix[0]))
        codes -= {'ETF', 'TWD', 'KY', 'TPK'}
        if len(codes) != 1:
            continue
        code = codes.pop()
        if code.endswith('O'):
            continue
        # Read the six explicitly headed values, not the page number or the
        # next paragraph after the numeric row.
        numeric = r'\d[\d,]*(?:\.\d+)?[ \t]*%?'
        numeric_row = re.match(r'\s*('+numeric+r'(?:\s+'+numeric+r'){5})[ \t]*(?:\n|$)', prefix[1])
        if numeric_row is None:
            continue
        tokens = re.findall(numeric,numeric_row[1])
        if len(tokens) != 6:
            continue
        if any(('%' in t) != (kind == 'notional_rate') for t in tokens):
            continue
        try:
            numbers = tuple(Decimal(t.replace(',','').replace('%','').strip()) /
                            (100 if kind == 'notional_rate' else 1) for t in tokens)
        except InvalidOperation:
            continue
        if not all(n.is_finite() and n > 0 for n in numbers):
            continue
        if not (numbers[0] >= numbers[1] >= numbers[2] and numbers[3] >= numbers[4] >= numbers[5]):
            continue
        out.append(dict(product=code, margin_kind=kind,
                        after=[float(x) for x in numbers[:3]], before=[float(x) for x in numbers[3:]],
                        candidate_only=True))
    return out


def margin_legacy_word_levels(text: str) -> list[dict]:
    """Recover explicit level tables in native antiword pipe output.

    Legacy Word tables can put all rows on one line, so line-based PDF/OCR
    matching loses them. Require the exact three-column order and row-local
    percentages; never expand merged amounts into another product's row.
    """
    from decimal import Decimal
    dense = compact(text)
    # Scope to futures sections; an adjacent option table has six percentages.
    heading = re.compile(r'結算保證金適用比例\|維持保證金適用比例\|原始保證金適用比例\|')
    result = []
    for match in heading.finditer(dense):
        end = dense.find('註:', match.end())
        block = dense[match.end():end if end >= 0 else match.end()+50000]
        for row in re.split(r'\|\|', block):
            cells = row.strip('|').split('|')
            if len(cells) != 8 or not cells[0].isdigit():
                continue
            _,code,name,_underlying,grade,*rates = cells
            if not re.fullmatch(r'[A-Z][A-Z0-9][F12]',code) or '期貨' not in name:
                continue
            if not re.fullmatch(r'級距[1-4]',grade):
                continue
            if not all(re.fullmatch(r'\d+(?:\.\d+)?%',rate) for rate in rates):
                continue
            clearing,maintenance,initial = [Decimal(rate[:-1])/100 for rate in rates]
            if not 0 < clearing <= maintenance <= initial <= 1:
                raise ValueError('invalid explicit legacy margin rates')
            result.append(dict(product=code,margin_kind='notional_rate',
                after=list(map(float,[initial,maintenance,clearing])),before=None,
                event_type='absolute_level',margin_grade=int(grade[-1]),
                extraction_method='native_legacy_word_explicit_pipe_cells',candidate_only=True))
    # Old fixed-amount notices use antiword tables with merged phase headings
    # and Chinese or mixed numerals. Reconstruct only those explicit cells and
    # send them through the same grid parser as native PDF tables.
    for block in re.findall(r'(?:[ \t]*\|[^\n]*\|[ \t]*(?:\n|$))+', text):
        table=[line.strip()[1:-1].split('|') for line in block.splitlines() if line.strip()]
        if not table:
            continue
        # A one-cell currency heading scopes the following table only. Several
        # currencies can occur consecutively in one antiword pipe block.
        segments=[];segment=[];unit=''
        for row in table:
            if len(row)==1 and '單位' in row[0]:
                if segment:segments.append((unit,segment))
                unit=row[0];segment=[]
            elif any(compact(c) for c in row):
                segment.append(row)
        if segment:segments.append((unit,segment))
        for caption,segment in segments:
            widths={len(row) for row in segment}
            if widths=={3,7}:
                segment=[([r[0],r[1],'','',r[2],'',''] if len(r)==3 and
                          '調整後' in r[1] and '調整前' in r[2] else r) for r in segment]
            if len({len(row) for row in segment})!=1:
                continue
            merged=[]
            for row in segment:
                row=list(map(compact,row))
                # A wrapped amount belongs to the preceding named product.
                if (merged and not row[0] and re.fullmatch(r'[A-Z][A-Z0-9]{1,2}',merged[-1][0])
                        and all(not c or re.fullmatch(r'[0-9〇零一二三四五六七八九十百千萬兩,元]+',c)
                                for c in row[1:])):
                    merged[-1]=[a+b for a,b in zip(merged[-1],row)]
                else:
                    merged.append(row)
            for fact in margin_grid_candidates(merged,caption):
                fact['extraction_method']='native_legacy_word_explicit_pipe_cells'
                result.append(fact)
    # A product launch can use a vertical specification table with an explicit
    # English code and three separately labelled currency amounts.
    for block in re.findall(r'(?:[ \t]*\|[^\n]*\|[ \t]*(?:\n|$))+', text):
        rows=[list(map(compact,line.strip()[1:-1].split('|'))) for line in block.splitlines() if line.strip()]
        if not rows or any(len(row)!=2 for row in rows):continue
        fields=dict(rows);code=fields.get('英文代碼','')
        if not re.fullmatch(r'[A-Z][A-Z0-9]{1,2}',code) or code.endswith('O'):continue
        body=''.join(value for key,value in rows)
        amounts=[]
        for label in ('原始','維持','結算'):
            found=re.findall(label+r'保證金每(?:單位|口)新[臺台]幣([0-9〇零一二三四五六七八九十百千萬兩,]+)元',body)
            amounts.append(_legacy_integer_amount(found[0]) if len(found)==1 else None)
        if all(x is not None and x>0 for x in amounts) and amounts[0]>=amounts[1]>=amounts[2]:
            result.append(dict(product=code,margin_kind='fixed_twd',after=list(map(float,amounts)),
                before=None,event_type='absolute_level',extraction_method='native_legacy_word_launch_specification',
                candidate_only=True))
    return result


def _legacy_integer_amount(token: str) -> int:
    """An explicit whole amount; not a date, range, multiplier or default."""
    from downloader.taifex_rule_parsing import _integer
    token=token.replace(',','').removesuffix('元')
    if not re.fullmatch(r'[0-9〇零一二三四五六七八九十百千萬兩]+',token) or token.count('萬')>1:
        raise ValueError('not an explicit integer amount')
    def small(part):
        if not part:
            return 0
        if part.isascii() and part.isdigit():
            return int(part)
        # Mixed sources write 7萬5千; the existing Chinese numeral owner
        # handles each sub-ten-thousand component after digit translation.
        part=part.translate(str.maketrans('0123456789','零一二三四五六七八九'))
        return _integer(part)
    parts=token.split('萬')
    return small(parts[0])*10000+small(parts[1]) if len(parts)==2 else small(token)


def position_legacy_word_candidates(text: str) -> list[dict]:
    """Read explicit natural-person contract counts from native Word tables.

    These are undated candidates; listing amendments and adjusted/mini grouping
    still require review. In particular the cancelled COF launch is not admitted
    merely because it has a row in the original 35-product announcement.
    """
    dense=compact(text)
    result=[]
    # Older market-wide tables include interest futures with BOTH a monthly
    # and all-month cap. Read only the explicit natural-person cell even when
    # its amount spans a merged corporate column.
    if '單位:契約數' in dense and '自然人' in dense:
        dates={str(roc_date(m)) for m in re.finditer(r'自'+ROC_DATE+r'起',dense)}
        rows=[list(map(compact,line.strip()[1:-1].split('|'))) for line in text.splitlines()
              if line.strip().startswith('|') and line.strip().endswith('|')]
        natural=None;names={}
        for row in rows:
            named=re.fullmatch(r'(.+?期貨)\(([A-Z][A-Z0-9]{1,2})\)',row[0])
            if named:names[named[1]]=named[2]
        for row in rows:
            if row[0]=='商品別' and '自然人' in row:
                natural=row.index('自然人');continue
            if natural is None or len(row)<=natural:continue
            named=re.fullmatch(r'(.+?期貨)\(([A-Z][A-Z0-9]{1,2})\)',row[0])
            if not named:continue
            code=named[2];value=row[natural];monthly=None;aggregate=None;formula=None
            pair=re.fullmatch(r'單一月份([\d,]+)[,，]各月份合計([\d,]+)',value)
            if pair:
                monthly,aggregate=[int(v.replace(',','')) for v in pair.groups()]
                if not 0<monthly<=aggregate:continue
            elif re.fullmatch(r'\d[\d,]*',value):
                aggregate=int(value.replace(',',''))
                if aggregate<=0:continue
            else:
                formula=re.fullmatch(r'與(.+?期貨)合併計算\(依(\d+)口(.+?期貨)(?:契約)?等於1口(.+?期貨)(?:契約)?合併計算\)',value)
                if not formula or formula[1]!=formula[4] or formula[3]!=named[1]:continue
            result.append(dict(product=code,natural_person_limit=aggregate,
                natural_person_monthly_limit=monthly,unit='contracts',direction='absolute',
                event_type='combined_position_formula' if formula else 'absolute_level',
                combined_position_base_name=formula[1] if formula else None,
                combined_position_base_product=names.get(formula[1]) if formula else None,
                combined_position_ratio=f'1/{formula[2]}' if formula else None,
                combined_position_evidence=value if formula else None,
                effective_date=next(iter(dates)) if len(dates)==1 else None,
                effective_phase='product_regular_open',candidate_only=True,
                extraction_method='native_legacy_market_position_cells'))
    if '股票期貨' not in dense or '部位限制契約數' not in dense or '自然人' not in dense:
        return result
    listing_dates={str(roc_date(m)) for m in re.finditer(
        ROC_DATE+r'新上市\d+檔[「\"]股票期貨(?:契約)?[」\"]',
        '\n'.join(compact(line) for line in text.splitlines()))}
    listing_date=next(iter(listing_dates)) if len(listing_dates)==1 else None
    for row in re.split(r'\|[ \t]*\||\n',text):
        cells=[compact(c) for c in row.strip().strip('|').split('|')]
        code=None;amount=None
        if (len(cells)==5 and re.fullmatch(r'[A-Z]{2}F',cells[0])
                and '期貨' in cells[1] and re.fullmatch(r'\d+',cells[2])):
            code,amount=cells[0],cells[3]
        elif (len(cells)==9 and cells[0].isdigit() and re.fullmatch(r'[A-Z]{2}F',cells[2])
                and '期貨' in cells[3] and re.fullmatch(r'[A-Z]{2}O',cells[4])):
            code,amount=cells[2],cells[6]
        if code and re.fullmatch(r'\d[\d,]*',amount) and int(amount.replace(',',''))>0:
            result.append(dict(product=code,natural_person_limit=int(amount.replace(',','')),
                unit='contracts',direction='absolute',effective_date=listing_date,
                effective_phase='new_contract_listing' if listing_date else 'unresolved',
                requires_adjusted_contract_and_mini_group_review=True,candidate_only=True,
                extraction_method='native_legacy_word_explicit_natural_person_cells'))
    return result


def position_prose_candidates(text: str) -> list[dict]:
    """Read explicit natural-person amounts within their own notice clause.

    Product names identify contracts, not current limits. A date-only change
    stays date-only; its eventual admission must wait past that calendar day.
    Options and amounts belonging to another numbered clause are not borrowed.
    """
    dense=compact(text)
    if '部位限制' not in dense:return []
    sections=re.split(r'公告事項:|說明:',dense,maxsplit=1)
    if len(sections)!=2:return []
    header,body=sections[0],sections[1].split('正本:',1)[0]
    issued={str(roc_date(m)) for m in re.finditer(r'發文日期:(?:中華民國)?'+ROC_DATE,dense)}
    names={'臺股期貨':'TX','小型臺指期貨':'MTX','電子期貨':'TE','金融期貨':'TF',
        '非金電期貨':'XIF','櫃買期貨':'GTF','臺幣黃金期貨':'TGF','布蘭特原油期貨':'BRF',
        '微型臺指期貨':'TMF','臺灣證券交易所股價指數期貨契約':'TX',
        '臺灣證券交易所股價指數小型期貨契約':'MTX',
        '臺灣證券交易所電子類股價指數期貨契約':'TE',
        '臺灣證券交易所金融保險類股價指數期貨契約':'TF',
        '布蘭特原油期貨契約':'BRF','臺灣中型100指數期貨契約':'M1F',
        'FTSE4GOOD臺灣指數公司臺灣永續指數期貨契約':'E4F',
        '臺灣指數公司臺灣上市上櫃生技醫療股價指數期貨契約':'BTF',
        '臺灣生技指數期貨契約':'BTF','英國富時100指數期貨契約':'F1F',
        '美國費城半導體股價指數期貨契約':'SXF','臺灣半導體30指數期貨契約':'SOF',
        '臺灣證券交易所航運類股價指數期貨契約':'SHF',
        '美國那斯達克100股價指數期貨契約':'UNF','櫃買富櫃200指數期貨契約':'G2F',
        '東京證券交易所股價指數期貨契約':'TJF',
        '美國道瓊工業平均股價指數期貨契約':'UDF',
        '美國標準普爾500股價指數期貨契約':'SPF','印度50股價指數期貨契約':'I5F'}
    # Long/mini aggregation requires an explicit dated conversion, so such
    # prose is retained for review instead of producing two independent caps.
    result=[]
    # "旨揭契約" refers only to the explicitly named subjects of this
    # listing notice. Tables later in its attachments can mention many other
    # products; those names must never receive this amount.
    subject=header.split('主旨:',1)[-1].split('依據:',1)[0]
    labels=re.findall(r'「([^」]+)」',subject)
    if '上市' in subject and labels and all(label in names for label in labels):
        limits=re.findall(r'旨揭契約(?:之)?交易人部位限制數(?:分別)?為:自然人([\d,]+)個契約',body)
        if len(limits)==1:
            amount=int(limits[0].replace(',',''))
            if amount>0:
                for label in labels:
                    result.append(dict(product=names[label],natural_person_limit=amount,unit='contracts',
                        direction='absolute',event_type='absolute_level',effective_date=None,
                        effective_phase='new_contract_listing',source_period_text=subject,
                        requires_adjusted_contract_and_mini_group_review=True,candidate_only=True,
                        extraction_method='named_listing_position_clause'))
    clauses=re.split(r'(?:[一二三四五六七八九十]+、|(?<!\d)\d+\.(?!\d))',body)
    preamble=clauses[0]
    for clause in clauses:
        if '加計' in clause and '依合約規模' in clause:continue
        dates={str(roc_date(m)) for m in re.finditer(r'自'+ROC_DATE+r'(?:(一般交易時段)起生效|起)',clause)}
        immediate='即日起' in clause or (not dates and '即日起' in preamble)
        if immediate and len(issued)==1:dates=set(issued)
        day=next(iter(dates)) if len(dates)==1 else None
        phase='product_regular_open' if '一般交易時段起生效' in clause else 'date_only_requires_phase_review'
        pattern=(r'((?:「[^」]+」(?:、|及|與)?)+)([^「」;；]*?)自然人'
                 r'(?:部位限制數)?(?:為|:)?([\d,]+)(?:個契約|口)')
        for match in re.finditer(pattern,clause):
            labels=re.findall(r'「([^」]+)」',match[1])
            if '加計' in match[2] or '依合約規模' in match[2]:continue
            codes=[names[label] for label in labels if label in names]
            if len(codes)!=len(labels):continue
            amount=int(match[3].replace(',',''))
            if amount<=0:continue
            for code in codes:
                result.append(dict(product=code,natural_person_limit=amount,unit='contracts',
                    direction='absolute',event_type='absolute_level',effective_date=day,
                    effective_phase=phase,source_period_text=clause,
                    requires_adjusted_contract_and_mini_group_review=True,candidate_only=True,
                    extraction_method='explicit_natural_person_prose_clause'))
    return result


def margin_grid_candidates(cells: list[list], caption: str = '') -> list[dict]:
    """Extract explicitly headed native table cells, retaining candidate status.

    Merged phase headings are expanded horizontally only inside the header.
    Never forward-fill product names or amounts from another data row. Bare
    numerals need an explicit local TWD unit; percent cells supply their unit.
    """
    from decimal import Decimal, InvalidOperation
    rows = [[compact(str(v or '')) for v in row] for row in cells]
    if not rows or len({len(r) for r in rows}) != 1:
        return []
    fields = [('after','initial'),('after','maintenance'),('after','clearing'),
              ('before','initial'),('before','maintenance'),('before','clearing')]
    metrics = {'原始': 'initial', '維持': 'maintenance', '結算': 'clearing'}
    header_index = next((i for i,r in enumerate(rows[:4])
                         if any('調整後' in c for c in r) and any('調整前' in c for c in r)), None)
    absolute = header_index is None
    if absolute:
        header_index = 0
        fields = fields[:3]
        phases = ['after'] * len(rows[0])
        # Do not mistake an incomplete before/after header for a level table.
        if any('調整前' in c or '調整後' in c for row in rows[:4] for c in row):
            return []
    else:
        phase = None; phases = []
        for cell in rows[header_index]:
            if '調整後' in cell: phase = 'after'
            if '調整前' in cell: phase = 'before'
            phases.append(phase)
    columns = {}; end_header = None
    for i in range(header_index, min(len(rows),header_index+4)):
        candidate = {}
        for col, cell in enumerate(rows[i]):
            labels = [v for k,v in metrics.items() if k in cell]
            if len(labels) == 1 and phases[col]:
                key = (phases[col],labels[0])
                if key in candidate: return []
                candidate[key] = col
        if set(candidate) == set(fields):
            columns = candidate; end_header = i; break
    if end_header is None:
        return []
    local_units = compact(caption + ' '.join(' '.join(r) for r in rows[:end_header+1]))
    currencies={'TWD':r'新[臺台]幣(?:元)?','USD':r'美元','CNY':r'人民幣(?:元)?','JPY':r'日[圓元]'}
    global_currency={code for code,pattern in currencies.items() if re.search(pattern,local_units)}
    currency_columns={i for row in rows[:end_header+1] for i,c in enumerate(row) if c in ('幣別','單位')}
    header_codes={c for row in rows[:end_header+1] for c in row
                  if re.fullmatch(r'[A-Z][A-Z0-9]{1,2}',c) and c not in ('ETF','TWD')}
    result = []
    for i, row in enumerate(rows[end_header+1:],start=end_header+1):
        label = ' '.join(c for j,c in enumerate(row) if j not in columns.values())
        if '選擇權' in label and '期貨' not in label:
            continue
        codes = set(re.findall(r'\(([A-Z][A-Z0-9]{1,2})\)',label))
        codes.update(c for j,c in enumerate(row) if j not in columns.values()
                     and re.fullmatch(r'[A-Z][A-Z0-9]{1,2}',c))
        codes -= {'ETF','TWD'}
        if not codes and label.strip() == '保證金' and len(header_codes)==1:
            codes=set(header_codes)
        codes={c for c in codes if not (len(c)==3 and c.endswith('O'))}
        if not codes:
            continue
        tokens = [row[columns[f]] for f in fields]
        percentages = [t.endswith('%') for t in tokens]
        local_currency={code for code,pattern in currencies.items()
                        if any(re.fullmatch(pattern,row[j]) for j in currency_columns)}
        currency=local_currency if local_currency else global_currency
        if all(percentages): kind = 'notional_rate'
        elif not any(percentages) and len(currency)==1: kind = 'fixed_'+next(iter(currency)).lower()
        elif not any(percentages) and not currency and all(t.endswith('元') for t in tokens):
            # 元 alone is not a TWD currency proof. Keep the amount for the
            # dated product-currency join, never admit it as fixed_twd here.
            kind='fixed_unresolved_currency'
        else: continue
        try:
            if kind=='notional_rate':
                if not all(re.fullmatch(r'\d[\d,]*(?:\.\d+)?%',t) for t in tokens):
                    continue
                values=[Decimal(t.replace(',','').rstrip('%'))/100 for t in tokens]
            else:
                values=[Decimal(t.replace(',','')) if re.fullmatch(r'\d[\d,]*(?:\.\d+)?',t)
                        else Decimal(_legacy_integer_amount(t)) for t in tokens]
        except (InvalidOperation,ValueError,KeyError):
            continue
        if not all(v.is_finite() and v > 0 for v in values): continue
        if not values[0] >= values[1] >= values[2]: continue
        if not absolute and not values[3] >= values[4] >= values[5]: continue
        if kind == 'notional_rate' and max(values)>1: continue
        for code in sorted(codes):
            source_code = code
            code_policy = 'explicit_daily_product_code'
            if (len(code)==2 and '股票期貨' in local_units
                    and any('期貨' in c for c in row)):
                code += 'F'
                code_policy = 'dated_stock_futures_family_code_plus_F'
            result.append(dict(product=code,source_product_code=source_code,product_code_policy=code_policy,
                               margin_kind=kind,after=list(map(float,values[:3])),
                               before=None if absolute else list(map(float,values[3:])),row_index=i,
                               event_type='absolute_level' if absolute else 'before_after',candidate_only=True))
    return result


def verified_margin_values(rows: list[dict], pdf_text: str, product: str) -> tuple[int, ...]:
    expected = margin_pdf_values(pdf_text, product)
    facts = [f for f in margin_changes(rows, unit_hint="TWD") if f["contract_code"] == product]
    values = {(f["phase"], f["margin_kind"]): float(f["normalized_value"]) for f in facts}
    keys = [(phase, kind) for phase in ("after", "before")
            for kind in ("initial", "maintenance", "clearing")]
    if len(facts) != 6 or set(values) != set(keys):
        raise ValueError(f"duplicate/incomplete {product} CSV cells")
    actual = tuple(values[k] for k in keys)
    if actual != expected or not all(x > 0 for x in actual):
        raise ValueError(f"{product} CSV/PDF margin disagreement")
    return expected


def position_grid_candidates(cells: list[list], source_text: str = '', *, caption: str = '',
        published_date: str | None = None, market_dates: list[date] | None = None) -> list[dict]:
    """Keep per-row natural-person limits and the distinct raise/lower clocks.

    A grade is not a contract count. In particular, this extractor never uses
    today's grade table to manufacture the amount before an old amendment.
    Adjusted-contract share limits still require their own dated adjustment.
    """
    rows = [[compact(str(v or '')) for v in row] for row in cells]
    if not rows or len({len(r) for r in rows}) != 1:
        return []
    text = compact(source_text)
    if any(row[0]=='商品別' and '自然人' in row for row in rows):
        # Reuse the explicit market-wide table grammar for native cell grids.
        # Preserve monthly caps and mini conversion ratios as separate facts.
        context='\n'.join(line for line in source_text.splitlines() if not line.strip().startswith('|'))
        return position_legacy_word_candidates(context+'\n'+'\n'.join('|'+('|'.join(row))+'|' for row in rows))
    if '股票期貨' not in text or '單位:契約數' not in compact(str(cells)+caption+source_text):
        return []
    # Market-wide amendments can publish a complete roster, with a separate
    # yes/no futures column. Read each literal count; grade numbers and an
    # options-only tick are not futures allowances.
    header=''.join(''.join(row) for row in rows[:3])
    if '商品代號(前2碼)' in header:
        columns={}
        labels={'商品代號(前2碼)':'code','證券代號':'underlying',
                '股票期貨':'futures','自然人':'natural'}
        for row in rows[:3]:
            for i,cell in enumerate(row):
                if cell in labels:columns[labels[cell]]=i
        dates={str(roc_date(m)) for m in re.finditer(r'並自'+ROC_DATE+r'起實施',text)}
        if set(columns)!=set(labels.values()) or len(dates)!=1:return []
        facts=[]
        for index,row in enumerate(rows[3:],3):
            code=row[columns['code']];amount=row[columns['natural']]
            underlying=row[columns['underlying']]
            if (not re.fullmatch('[A-Z]{2}',code) or not re.fullmatch(r'\d{4}',underlying)
                    or row[columns['futures']] not in ('O','○','〇','◯')
                    or not re.fullmatch(r'\d{1,3}(?:,\d{3})*',amount)
                    or int(amount.replace(',',''))<=0):continue
            facts.append(dict(product=code+'F',source_product_code=code,
                product_code_policy='dated_stock_futures_family_code_plus_F',
                underlying_symbol=underlying,natural_person_limit=int(amount.replace(',','')),
                unit='contracts',event_type='absolute_level',direction='absolute',
                effective_date=next(iter(dates)),effective_phase='date_only_requires_phase_review',
                requires_adjusted_contract_and_mini_group_review=True,
                row_index=index,candidate_only=True))
        return facts
    direction_dates = {'raise': set(), 'lower': set()}
    for direction, words in [('raise', '(?:提高|調高|調升)'), ('lower', '(?:降低|調降)')]:
        for match in re.finditer(r'部位限制數(?:之)?' + words + r'(?:者)?[,，:：]?自'
                                r'(?:(\d{4})/(\d{1,2})/(\d{1,2})|'
                                r'(\d{2,3})年(\d{1,2})月(\d{1,2})日)起生效', text):
            if match[1]: day = date(*map(int, match.group(1, 2, 3)))
            else: day = date(int(match[4]) + 1911, int(match[5]), int(match[6]))
            direction_dates[direction].add(str(day))
    immediate_date=None
    if published_date:
        # Older quarterly notices state publication-relative raises and a
        # separate explicit date for reductions. Never lend TXO's date to a
        # stock-futures row, or guess the next business day as tomorrow.
        stock_section=text.rsplit('調整本公司',1)[-1]
        next_business=bool(re.search(r'部位限制數(?:為|之)?(?:調高|提高)者[,，]?自公告(?:之)?次一營業日(?:一般交易時段)?起',stock_section))
        if next_business and market_dates:
            following=[d for d in market_dates if str(d)>published_date]
            if following:direction_dates['raise'].add(str(min(following)))
        elif not next_business and re.search(r'(?:部位限制數(?:為|之)?(?:調高|提高)(?:者)?[,，]?|提高,皆)自公告日起生效',stock_section):
            direction_dates['raise'].add(published_date)
        for m in re.finditer(r'(?:為調降者|部位限制數(?:之)?降低)([^。；]{0,100}?)\(即'+ROC_DATE+r'\)',stock_section):
            if '除' not in m[1]:
                direction_dates['lower'].add(str(date(int(m[2])+1911,int(m[3]),int(m[4]))))
        for m in re.finditer(r'(?:[;；]|生效[;；])調降者,自'+ROC_DATE+r'起生效',stock_section):
            direction_dates['lower'].add(str(roc_date(m)))
        # Before split raise/lower timing was introduced, notices explicitly
        # applied the entire attached table immediately. Restrict the match
        # to the subject; a footnote cannot override a later opening date.
        subject=text.split('主旨:',1)[-1].split('依據:',1)[0].split('說明:',1)[0]
        if ('股票期貨' in subject and '部位限制數,並自即日起實施' in subject
                and not next_business and not any(direction_dates.values())):
            immediate_date=published_date
        if ('股票期貨' in subject and '本公告自即日起生效' in text
                and not next_business and not any(direction_dates.values())):
            immediate_date=published_date
    exception_dates={}
    # A named delayed reduction in the old all-contracts-expired regime is
    # distinct from the later next-nearby-month rule. Bind the literal name;
    # the other rows use only the explicit "all others" clause.
    m=re.search(r'除(.+?)期貨及選擇權為公告日已上市契約均到期後\(即'+ROC_DATE+
        r'\)生效外,其餘皆自公告日起生效',text)
    if m and published_date:
        exception_dates[m[1]+'期貨']=str(date(int(m[2])+1911,int(m[3]),int(m[4])))
        immediate_date=published_date
    # In 2012/2013, some named reductions waited for ALL listed months,
    # while the other rows changed now or after the next-nearby maturity.
    # The first date in that sentence must never be applied to the table.
    m=re.search(r'為調降者,除(.+?)(?:為|自)公告[^。；]*?\(即'+ROC_DATE+
        r'\)外,其餘(.+?)(?:。|$)',text)
    if m:
        names=re.findall(r'[\u4e00-\u9fffA-Za-z0-9-]+?期貨',
            m[1].replace('及選擇權','、').replace('與','、'))
        exceptional=str(date(int(m[2])+1911,int(m[3]),int(m[4])))
        exception_dates.update({name:exceptional for name in names})
        rest=m[5];remaining=re.search(r'\(即'+ROC_DATE+r'\)',rest)
        direction_dates['lower']=({str(roc_date(remaining))} if remaining else
            {published_date} if published_date and '自公告日起生效' in rest else set())
    result = []; columns = {}; direction = None
    headings=list(re.finditer(r'(提高|降低)部位限制',compact(caption)))
    if headings:direction={'提高':'raise','降低':'lower'}[headings[-1][1]]
    else:
        # The scanned section title can be unreadable while its footnote
        # marker survives. Follow only an explicit matching numbered clause;
        # never infer raise/lower from table order or the numerical size.
        markers=re.findall(r'[註注](\d+)[:：]?',compact(caption))
        if markers:
            clauses=re.findall(r'[註注]'+re.escape(markers[-1])+r'[:：]?部位限制數之(提高|降低)',text)
            if len(set(clauses))==1:direction={'提高':'raise','降低':'lower'}[clauses[0]]
    for index, row in enumerate(rows):
        dense = ''.join(row)
        if '提高部位限制' in dense: direction = 'raise'
        if '降低部位限制' in dense: direction = 'lower'
        for i, cell in enumerate(row):
            for field, label in [('code', '契約代碼'), ('name', '契約名稱'),
                                 ('underlying', '標的證券代號'), ('natural', '自然人'),
                                 ('before_grade', '調整前部位限制級距'),
                                 ('after_grade', '調整後部位限制級距')]:
                if cell == label: columns[field] = i
            if cell in ('契約英文代碼','股票期貨英文代碼'): columns['code']=i
            if cell in ('商品代碼','商品代號','契約代號'): columns['code']=i
            if cell in ('商品契約',):columns['name']=i
            if cell in ('中文簡稱','股票期貨中文簡稱'): columns['name']=i
            if cell in ('標的證券代碼',): columns['underlying']=i
            if cell in ('標準型證券股數/受益權單位',): columns['multiplier']=i
            if cell in ('調整後部位限制級數','調整後部位限制'):columns['after_grade']=i
            if cell in ('原適用部位限制級數','原適用部位限制'):columns['before_grade']=i
        if not {'code', 'name', 'natural'} <= columns.keys(): continue
        code, name, amount = (row[columns[k]] for k in ('code', 'name', 'natural'))
        if '期貨' not in name or not re.fullmatch(r'[A-Z]{2}(?:F)?', code):
            continue
        formula=re.fullmatch(r'與(.+?期貨)合併計算\(依(\d+)口(.+?期貨)等於1口(.+?期貨)合併計算\)',amount)
        if formula:
            if formula[1]!=formula[4] or formula[3]!=name or int(formula[2])<=1:
                continue
            natural=None
        elif re.fullmatch(r'\d[\d,]*',amount) and int(amount.replace(',',''))>0:
            natural=int(amount.replace(',',''))
        else:
            continue
        dates = direction_dates.get(direction, set())
        result.append(dict(product=code+'F' if len(code)==2 else code,
            source_product_code=code, product_code_policy='dated_stock_futures_family_code_plus_F',
            natural_person_limit=natural, unit='contracts',
            event_type='combined_position_formula' if formula else 'absolute_level',
            combined_position_base_name=formula[1] if formula else None,
            combined_position_ratio=f'1/{formula[2]}' if formula else None,
            combined_position_evidence=amount if formula else None,
            contract_multiplier=(float(row[columns['multiplier']].replace(',',''))
                if 'multiplier' in columns and re.fullmatch(r'\d[\d,]*',row[columns['multiplier']]) else None),
            before_grade=row[columns['before_grade']] if 'before_grade' in columns else None,
            after_grade=row[columns['after_grade']] if 'after_grade' in columns else None,
            underlying_symbol=row[columns['underlying']] if 'underlying' in columns else None,
            direction=direction, effective_date=next((d for n,d in exception_dates.items() if name.startswith(n)),
                next(iter(dates)) if len(dates)==1 else immediate_date),
            effective_phase='product_regular_open', row_index=index,
            limit_follows_applicable_grade=bool(re.search(
                r'本次部位限制數調整生效後,?則依其最新適用之部位限制級數計算',text)),
            requires_adjusted_contract_and_mini_group_review=True, candidate_only=True))
    return result


def _corporate_multi_product_cells(fields: dict[str, str]) -> list[list[list[str]]]:
    """Split shared legacy cells by explicit futures codes, never option units.

    Old notices put several adjustments in one two-column table. Each field
    can have its own product scope: months refer to the OLD code, deliverables
    and multipliers to the NEW code. A standard-contract table is not a source
    for any of these fields.
    """
    futures = r'[A-Z]{2}[F1-9]'
    any_code = r'(?:[A-Z]{3}|[A-Z]{2}[1-9])'
    code_text = fields.get('契約代號', fields.get('調整契約代號', ''))
    changes = re.findall('('+futures+r')調整為('+futures+')', code_text)
    if not changes or len(set(changes)) != len(changes):
        return []
    if re.search(futures+r'調整為', re.sub('('+futures+r')調整為('+futures+')', '', code_text)):
        return []

    def scoped(value, product, label):
        # Explicit grouped prefixes bind only the products they name. In
        # particular DLA's 履約價格乘數 cannot supply DL1/DL2's 契約乘數.
        prefix = '('+any_code+r'(?:(?:及|、|與|和)('+any_code+'))*)'
        pattern = prefix + label
        matches = list(re.finditer(pattern, value))
        if not matches:
            return value if not re.search(any_code, value) else None
        values = []
        for i, match in enumerate(matches):
            if product in re.findall(any_code, match[1]):
                end = matches[i+1].start() if i+1 < len(matches) else len(value)
                values.append(value[match.end():end])
        return values[0] if len(values) == 1 else None

    result = []
    for old, new in changes:
        multiplier = scoped(fields.get('契約乘數', ''), new, r'(?:契約乘數|履約價格乘數)')
        deliverable = scoped(fields.get('約定標的物', fields.get('調整約定標的物', '')),
                             new, r'約定標的物(?:調整)?(?:為|[:：])')
        months = scoped(fields.get('調整契約月份', fields.get('契約調整月份', '')),
                        old, r'[:：]')
        if multiplier is None or deliverable is None or months is None:
            continue
        # A multiplier clause naming an option still carries its own prefix,
        # so the value boundary above stops before that clause.
        row = [['契約代號', old+'調整為'+new], ['契約乘數', multiplier],
               ['約定標的物', deliverable], ['調整契約月份', months]]
        for key in ('調整生效日', '調整生效日及恢復交易日', '買方權益數加項', '賣方權益數減項'):
            if key in fields:
                value = scoped(fields[key], new, r'[:：]') if key.endswith('項') else fields[key]
                if value is not None:
                    row.append([key, value])
        result.append(row)
    return result


def corporate_deliverable_components(text: str, multiplier: float) -> dict:
    """Separate daily cash deliverables from terminal subscription rights.

    Parse only an explicit single-security quantity that agrees with the
    independently stated multiplier. Missing or corrupt OCR stays unresolved.
    This resolves numeric terms, not their publication or effective clocks.
    """
    from decimal import Decimal
    dense=compact(text)
    number=r'([\d,]+(?:\.\d+)?)'
    # A legacy field may name the issuer directly ("股台積電股票"). Its
    # table already binds the product. Do not treat a second securities leg
    # as the first; a separately named OLD rights base is not a deliverable.
    quantity_text=re.sub(r'及(?:原)?'+number+r'股標的證[券劵](?:可獲|所獲|可得)優先參與現金增資.*','',dense)
    shares=re.findall(number+r'(?:股(?:標的證[券劵]|[\u4e00-\u9fffA-Za-z-]{1,40}(?:普通股|股票))|(?:單位)?受益權單位標的證券|單位標的證券)',quantity_text)
    errors=[]
    quantity=Decimal(shares[0].replace(',','')) if len(shares)==1 else None
    if quantity is None or quantity!=Decimal(str(multiplier)):
        errors.append('deliverable_quantity_missing_or_disagrees_with_multiplier')
    rights='現金增資' in dense or '優先參與' in dense
    cash_terms=re.findall(r'(?:新[臺台]幣|現金(?:新[臺台]幣)?)'+number+r'元',dense)
    has_other_cash='現金' in dense.replace('現金增資','') or bool(re.search(r'新[臺台]幣',dense))
    if len(cash_terms)==1:
        cash=Decimal(cash_terms[0].replace(',',''))
    elif not cash_terms and not has_other_cash and quantity is not None:
        cash=Decimal(0)
    else:
        cash=None;errors.append('deliverable_cash_missing_or_ambiguous')
    if '相當價值' in dense and not rights:
        errors.append('other_deliverable_value_requires_dated_rule')
    return dict(deliverable_security_quantity=float(quantity) if quantity is not None else None,
        deliverable_cash_twd=float(cash) if cash is not None else None,
        subscription_rights_at_final_settlement=rights,
        deliverable_components_resolved=not errors,
        deliverable_component_errors=errors)


def superseded_notice_sources(facts: list[dict], effective_day: str) -> set[str]:
    """Apply explicit source-bound revisions known before a common boundary.

    A reviewed notice-number reference or an explicit named listing update
    establishes supersession. Both original sources are bound by the review
    reader. A later publication or differing number alone never does.
    Callers group by product/effective boundary before invoking this helper.
    """
    removed=set()
    cutoff=datetime.combine(date.fromisoformat(effective_day),time(),TAIPEI)
    for new in facts:
        old_hashes=new.get('supersedes_source_sha256s') or []
        if not old_hashes:continue
        if not re.fullmatch('[a-f0-9]{64}',new.get('supersession_review_sha256') or ''):
            raise ValueError('notice supersession lacks reviewed source evidence')
        try:new_known=datetime.fromisoformat(new.get('known_at') or '')
        except ValueError:continue
        if not new.get('issue_date_bound') or new_known.tzinfo is None or new_known>=cutoff:continue
        for digest in old_hashes:
            if not re.fullmatch('[a-f0-9]{64}',digest) or digest==new.get('source_content_sha256'):
                raise ValueError('invalid notice supersession source identity')
            old=[fact for fact in facts if fact.get('source_content_sha256')==digest]
            if not old:continue
            for fact in old:
                try:old_known=datetime.fromisoformat(fact.get('known_at') or '')
                except ValueError:raise ValueError('superseded notice publication is unbound')
                if (not fact.get('issue_date_bound') or old_known.tzinfo is None
                        or old_known>=new_known):
                    raise ValueError('notice revision publication order is not established')
            removed.add(digest)
    return removed


def corporate_terms_intervals(facts: list[dict], *, unit_only: bool = False) -> tuple[list[dict], list[dict]]:
    """Resolve product/month terms, retaining barriers at conflicting events.

    Independent text/grid views may agree; row order never resolves a numeric
    disagreement. This does not authorize trading or infer a margin phase.
    Position-unit extraction uses the same dated identity barriers but has no
    dependency on a subscription valuation or cash credit. Its output omits
    those financial fields, so it cannot pass as a complete deliverable.
    """
    from collections import defaultdict
    grouped=defaultdict(list);issues=[];outgoing=defaultdict(set)
    month_views=defaultdict(list)
    def month_key(fact):
        return tuple(fact.get(key) for key in
            ('source_content_sha256','from_product','product','effective_date'))
    for fact in facts:
        if fact.get('contract_months') and all(month_key(fact)):
            month_views[month_key(fact)].append(fact)
    for original in facts:
        fact=dict(original)
        # OCR can lose an entire cell while another view of the exact same
        # bytes retains it. Bind only an unambiguous scope of the same event;
        # never borrow a year/month from another notice or adjusted code.
        if not fact.get('contract_months') and all(month_key(fact)):
            peers=month_views[month_key(fact)]
            scopes={tuple(sorted(set(peer['contract_months']))) for peer in peers}
            if len(scopes)==1:
                fact['contract_months']=list(next(iter(scopes)))
                fact['contract_months_origin']='same_source_exact_event_views'
                fact['month_scope_provenance']=sorted({json.dumps({key:peer.get(key) for key in
                    ('source_content_sha256','extraction_method','page','table_index','contract_months_origin')},
                    ensure_ascii=False,sort_keys=True) for peer in peers})
        reasons=[]
        if not fact.get('effective_date') or not fact.get('contract_months'):
            reasons.append('date_or_month_scope_missing')
        if not fact.get('issue_date_bound'):
            reasons.append('publication_not_bound')
        known=None
        try:
            known=datetime.fromisoformat(fact.get('known_at') or '')
            if known.tzinfo is None:raise ValueError('timezone missing')
        except ValueError:
            reasons.append('publication_timestamp_missing_or_invalid')
        if (known is not None and known.tzinfo is not None and fact.get('effective_date') and
                known>=datetime.combine(date.fromisoformat(fact['effective_date']),time(),TAIPEI)):
            reasons.append('not_known_before_effective_day')
        if not re.fullmatch(r'[a-f0-9]{64}',fact.get('source_content_sha256') or ''):
            reasons.append('source_content_identity_missing_or_invalid')
        quantity=fact.get('contract_multiplier')
        if (not isinstance(quantity,(int,float)) or isinstance(quantity,bool)
                or not np.isfinite(quantity) or quantity<=0):
            reasons.append('contract_multiplier_missing_or_invalid')
        if unit_only and fact.get('deliverable_security_quantity')!=quantity:
            reasons.append('independent_deliverable_quantity_missing_or_disagrees')
        if not unit_only and not fact.get('deliverable_components_resolved'):
            reasons.append('deliverable_components_unresolved')
        if not unit_only and fact.get('has_equity_credit_fields') and not fact.get('cash_equity_pair_agrees'):
            reasons.append('equity_cash_credit_unresolved')
        if reasons:
            issues.append(dict(product=fact['product'],source_content_sha256=fact['source_content_sha256'],
                source_url=fact['source_url'],effective_date=fact.get('effective_date'),
                reasons=';'.join(reasons)))
        if not fact.get('effective_date') or not fact.get('contract_months'):continue
        for month in fact['contract_months']:
            grouped[(fact['product'],month,fact['effective_date'])].append((fact,reasons))
            origin=fact.get('from_product')
            if origin and origin!=fact['product']:
                # Even an unresolved destination deliverable ends the old
                # code's term interval. It cannot carry its old multiplier
                # through a known outgoing corporate transfer.
                outgoing[(origin,month)].add(fact['effective_date'])
    identities=defaultdict(list)
    for (product,month,day),views in grouped.items():
        superseded=superseded_notice_sources([fact for fact,reasons in views],day)
        views=[(fact,reasons) for fact,reasons in views if fact['source_content_sha256'] not in superseded]
        valid=[dict(fact) for fact,reasons in views if not reasons]
        # An absent field in a text view is missing extraction, not an
        # explicit zero credit. Enrich only from the SAME source bytes and
        # matching physical terms. Different stated numbers remain conflicts.
        core=('from_product','contract_multiplier','deliverable_cash_twd',
              'subscription_rights_at_final_settlement')
        for fact in ([] if unit_only else valid):
            if fact.get('has_equity_credit_fields'):continue
            peers=[other for other in valid
                   if other['source_content_sha256']==fact['source_content_sha256']
                   and all(other.get(key)==fact.get(key) for key in core)
                   and other.get('cash_equity_pair_agrees')]
            amounts={other.get('equity_credit_long_per_contract') for other in peers}
            if len(amounts)==1:
                amount=next(iter(amounts))
                fact.update(has_equity_credit_fields=True,cash_equity_pair_agrees=True,
                    equity_credit_long_per_contract=amount,equity_debit_short_per_contract=amount)
        signatures=defaultdict(list)
        for fact in valid:
            signature=((fact['from_product'],fact['contract_multiplier']) if unit_only else
                (fact['from_product'],fact['contract_multiplier'],fact['deliverable_cash_twd'],
                fact['subscription_rights_at_final_settlement'],fact.get('equity_credit_long_per_contract'),
                fact.get('has_equity_credit_fields'),fact.get('fixed_subscription_rights_twd') or 0.))
            signatures[signature].append(fact)
        state=None
        if len(signatures)==1:
            signature,agreeing=next(iter(signatures.items()))
            # A different document with unresolved terms is still a barrier;
            # an incomplete view of the same verified document is not.
            resolved_hashes={f['source_content_sha256'] for f in agreeing}
            unresolved_hashes={f['source_content_sha256'] for f,reasons in views if reasons}
            if not unresolved_hashes-resolved_hashes:
                financial_fields={} if unit_only else dict(deliverable_cash_twd=signature[2],
                    subscription_rights_at_final_settlement=signature[3],equity_cash_credit_twd=signature[4],
                    has_equity_credit_fields=signature[5],fixed_subscription_rights_twd=signature[6])
                state=dict(product=product,contract=month,effective_date=day,
                    from_product=signature[0],contract_multiplier=signature[1],**financial_fields,
                    known_at=max(f['known_at'] for f in agreeing),
                    source_content_sha256s=sorted(resolved_hashes),
                    source_urls=sorted({f['source_url'] for f in agreeing}),
                    fixed_subscription_rights_evidence=sorted({json.dumps(f['fixed_subscription_rights_evidence'],
                        sort_keys=True,ensure_ascii=False) for f in agreeing if f.get('fixed_subscription_rights_evidence')}),
                    month_scope_provenance=sorted({proof for f in agreeing
                        for proof in f.get('month_scope_provenance',[])}),
                    superseded_source_sha256s=sorted(superseded),
                    supersession_review_sha256s=sorted({f['supersession_review_sha256'] for f in agreeing
                        if f.get('supersession_review_sha256')}),
                    point_in_time_verified=False,requires_phase_and_lifecycle_admission=True)
        if state is None:
            issues.append(dict(product=product,contract=month,effective_date=day,
                source_content_sha256='|'.join(sorted({f['source_content_sha256'] for f,_ in views})),
                source_url='|'.join(sorted({f['source_url'] for f,_ in views})),
                reasons='conflicting_or_unresolved_effective_terms'))
        identities[(product,month)].append((day,state))
    for key,days in outgoing.items():
        existing={day for day,state in identities[key]}
        identities[key].extend((day,None) for day in days-existing)
    intervals=[]
    for key,boundaries in identities.items():
        boundaries.sort(key=lambda x:x[0])
        for i,(day,state) in enumerate(boundaries):
            if state is not None:
                end=boundaries[i+1][0] if i+1<len(boundaries) else None
                intervals.append(dict(state,valid_until_exclusive=end,
                    end_includes_outgoing_transfer=end in outgoing.get(key,set())))
    return sorted(intervals,key=lambda r:(r['product'],r['contract'],r['effective_date'])),issues


def stock_futures_cash_reform_intervals(intervals: list[dict], reform: dict,
                                        cash_origins: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """Apply the 2011 cash-dividend reform to already resolved old terms.

    On May 3 the embedded dividend/capital-return cash leaves the deliverable
    and credits OLD long inventory (debits shorts) once, without changing the
    code or the number of contracts. Other benefits are not automatically cash
    dividends. Unknown origins stop the old interval; they are never zeroed.
    Contract-month filtering is only a necessary condition: legal lifetime and
    prior inventory still have to be admitted by the execution materializer.
    """
    boundary='2011-05-03'
    if (reform.get('effective_date')!=boundary or
            reform.get('effective_at')!=boundary+'T08:30:00+08:00' or
            not re.fullmatch(r'[a-f0-9]{64}',reform.get('source_content_sha256',''))):
        raise ValueError('invalid historical cash-reform source or phase')
    known=datetime.fromisoformat(reform['known_at'])
    if known.tzinfo is None or known>=datetime.fromisoformat(reform['effective_at']):
        raise ValueError('cash reform must be public before its effective phase')
    output=[];issues=[]
    for original in intervals:
        row=dict(original)
        cash=row.get('deliverable_cash_twd')
        end=row.get('valid_until_exclusive')
        crosses=(row['effective_date']<boundary and (end is None or end>boundary))
        if not crosses or cash is None or cash<=0 or row['contract']<'201105':
            output.append(row);continue
        # A pre-reform term must not silently carry its old embedded cash
        # past a known regime boundary, even when extraction is incomplete.
        row['valid_until_exclusive']=boundary
        output.append(row)
        origins={cash_origins.get(sha) for sha in row['source_content_sha256s']}
        if not origins or not origins <= {'cash_dividend','cash_capital_return'}:
            issues.append(dict(product=row['product'],contract=row['contract'],
                effective_date=boundary,reasons='cash_reform_origin_requires_source_review',
                source_content_sha256='|'.join(row['source_content_sha256s'])))
            continue
        changed=dict(original,from_product=row['product'],effective_date=boundary,
            effective_at=reform['effective_at'],deliverable_cash_twd=0.,
            equity_cash_credit_twd=float(cash),has_equity_credit_fields=True,
            cash_reform_prior_cash_twd=float(cash),event_family='cash_dividend_regime_20110503',
            carry_quantity_numerator=1,carry_quantity_denominator=1,
            known_at=max(row['known_at'],reform['known_at']),
            source_content_sha256s=sorted(set(row['source_content_sha256s'])|
                                          set(reform['source_content_sha256s'])),
            source_urls=sorted(set(row['source_urls'])|set(reform['source_urls'])))
        output.append(changed)
    return sorted(output,key=lambda r:(r['product'],r['contract'],r['effective_date'])),issues


def corporate_grid_candidates(cells: list[list], source_text: str = '', caption: str = '',
                              *, page_text: str = '') -> list[dict]:
    """Extract a single explicit futures adjustment without guessing cash flow.

    Deliverable cash and an equity credit are different financial quantities.
    Multi-product, multi-security and conditional rules remain source text for
    review; neither an absent credit nor an unreadable quantity becomes zero.
    """
    from decimal import Decimal
    rows = [[compact(str(v or '')) for v in row] for row in cells]
    if not rows or any(len(row) != 2 for row in rows): return []
    rows = [['契約乘數' if key == '契約乘數不調整' else key, value] for key,value in rows]
    # Older notices use 調整契約代號/調整約定標的物 instead of the
    # modern headers. Normalize labels only; do not repair OCR digits.
    aliases = {'調整契約代號': '契約代號', '調整約定標的物': '約定標的物'}
    rows = [[aliases.get(key, key), value] for key, value in rows]
    fields = {}
    for key,value in rows:
        # A second different value under one canonical label is ambiguous.
        # Dict last-write-wins would silently alter a financial contract.
        if key and key in fields and fields[key]!=value:
            return []
        if key:fields[key]=value
    month_fields=[(key,value) for key,value in rows
                  if re.fullmatch(r'(?:調整(?:契約)?|契約調整|契約)月份(?:註\d*)?',key)]
    code_text = fields.get('契約代號', fields.get('調整契約代號', ''))
    if not code_text: return []
    change = re.fullmatch(r'([A-Z]{2}[F1-9])調整為([A-Z]{2}[F1-9])', code_text)
    same = re.fullmatch(r'不調整\(仍為([A-Z]{2}[F1-9])\)', code_text)
    if change: before, after = change.groups()
    elif same: before = after = same[1]
    else:
        result = []
        for scoped_cells in _corporate_multi_product_cells(fields):
            for candidate in corporate_grid_candidates(scoped_cells, source_text, caption, page_text=page_text):
                candidate['field_scope'] = 'explicit_legacy_product_codes'
                result.append(candidate)
        return result
    # Some ruled scans lose the left label but retain a complete, explicitly
    # code-bound sentence in the right cell. The sentence itself identifies
    # the field; no character or number is corrected.
    for key,value in rows:
        field=None
        if re.fullmatch(re.escape(after)+r'契約乘數(?:調整為[\d,]+(?:\.\d+)?|不調整\(仍為[\d,]+(?:\.\d+)?\))[。.]?',value):
            field='契約乘數'
        for side,label in (('買方','買方權益數加項'),('賣方','賣方權益數減項')):
            if re.fullmatch(r'每口'+side+r'未沖銷部位調整'+label+r'新[臺台]幣[\d,]+元[。.]?',value):
                field=label
        if field:
            if field in fields and fields[field] and fields[field]!=value:
                return []
            fields[field]=value
    deliverable = fields.get('約定標的物', fields.get('調整約定標的物', ''))
    multiplier = fields.get('契約乘數', '')
    # Explicit constant multipliers only. A rights-value formula is not a
    # numeric multiplier, and numeric suffixes in the product are not values.
    quantity = re.search(r'(?:調整為|仍為)([\d,]+(?:\.\d+)?)(?:\)|。|$)', multiplier)
    if quantity is None: return []
    qty = Decimal(quantity[1].replace(',', ''))
    if not qty.is_finite() or qty <= 0: return []
    months = month_fields[0][1] if len(month_fields)==1 else ''
    month_footnote = re.search(r'註\d*$',month_fields[0][0]) if len(month_fields)==1 else None
    months_origin = 'table_cell'
    # Restrict the value to the date-list alphabet. A missing 到期契約 suffix
    # must not consume prices, footnote dates or the next contract's table.
    scope_pattern=r'(?:調整(?:契約)?|契約調整|契約)月份(?:註\d*|\d+)?[:：]((?:[\d年月、及與]|到期契約)+)'
    if not months and not month_fields:
        # The month list often sits immediately above this exact table. Never
        # borrow a different product's list from the whole document.
        captions = set(re.findall(scope_pattern, compact(caption)))
        if len(captions) == 1:
            months = captions.pop()
            months_origin = 'same_table_caption'
        elif not captions:
            page_scopes=set(re.findall(scope_pattern,compact(page_text)))
            if len(page_scopes)==1:
                months=page_scopes.pop()
                months_origin='same_page_single_scope'
    explicit_months = []; year = None; months_error = None
    for item in re.finditer(r'(?:(\d{2,3})年)?(\d{1,2})月', months):
        if item[1]: year = int(item[1]) + 1911
        if year is None or not 1 <= int(item[2]) <= 12:
            months_error = 'missing_year_or_invalid_month'; break
        explicit_months.append(f'{year:04d}{int(item[2]):02d}')
    if ('週' in months or not explicit_months or
            re.search(r'\d', re.sub(r'(?:(\d{2,3})年)?(\d{1,2})月', '', months))):
        months_error = 'unresolved_contract_months'
    cash = []
    for name in ('買方權益數加項', '賣方權益數減項'):
        found = re.findall(r'新[臺台]幣([\d,]+)元', fields.get(name, ''))
        cash.append(int(found[0].replace(',', '')) if len(found)==1 else None)
    cash_verified_pair = cash[0] is not None and cash[0] == cash[1]
    text = compact(source_text)
    date_pattern = r'調整生效日(?:及恢復交易日)?[:：]?' + ROC_DATE
    dates=set();date_error=None
    local_date = ''.join(key+':'+value for key,value in rows if key in
                         ('調整生效日', '調整生效日及恢復交易日'))
    for scope in (local_date,compact(caption),compact(page_text),text):
        mentions=list(re.finditer(date_pattern,scope))
        if not mentions:continue
        for match in mentions:
            try:dates.add(str(roc_date(match)))
            except ValueError:date_error='invalid_source_effective_date'
        break
    # This clause freezes the contract at the announced terms. It is not a
    # condition which makes today's adjustment optional. Keep other conditions.
    frozen_pattern = r'若(?:該)?標的公司於調整生效日後\(含當日\)變更股利[,，]則本契約不予調整[。.]?'
    frozen_terms = bool(re.search(frozen_pattern, text))
    condition_text = re.sub(frozen_pattern, '', text)
    conditional = any(word in condition_text for word in ('若', '倘', '順延', '變更'))
    return [dict(product=after, from_product=before, to_product=after,
        contract_multiplier=float(qty), deliverable_text=deliverable,
        contract_months=explicit_months if not months_error else [], contract_months_text=months,
        contract_months_origin=months_origin,
        contract_months_footnote_reference=month_footnote[0] if month_footnote else None,
        contract_months_error=months_error, effective_date=next(iter(dates)) if len(dates)==1 and not date_error else None,
        effective_date_error=date_error,
        equity_credit_long_per_contract=cash[0] if cash_verified_pair else None,
        equity_debit_short_per_contract=cash[1] if cash_verified_pair else None,
        cash_equity_pair_agrees=cash_verified_pair,
        has_equity_credit_fields=any(name in fields for name in ('買方權益數加項','賣方權益數減項')),
        **corporate_deliverable_components(deliverable,float(qty)),
        includes_deliverable_cash=('現金' in deliverable or '新臺幣' in deliverable),
        requires_rights_valuation=('現金增資' in deliverable or '相當價值' in deliverable),
        phase_evidence=caption, requires_phase_and_condition_review=True,
        has_conditional_language=conditional, later_dividend_revisions_do_not_restate=frozen_terms,
        candidate_only=True)]


def corporate_native_table_candidates(pages: list[dict]) -> list[dict]:
    """Bind a split table only to its explicit next-page continuation.

    A multiplier on a later options or standard-contract table is never a
    continuation. Require the same named destination, an empty continuation
    cell, and no intervening table or caption.
    """
    text='\n'.join(p.get('ocr_text') or p['native_text'] for p in pages)
    entries=[(p,i,t) for p in pages for i,t in enumerate(p['tables'])]
    result=[]
    for index,(page,ti,table) in enumerate(entries):
        cells=table['cells'];segments=[[page['page'],ti]]
        if cells and all(len(r)==2 for r in cells):
            fields={compact(str(k or '')):compact(str(v or '')) for k,v in cells}
            change=re.fullmatch(r'([A-Z]{2}[F1-9])調整為([A-Z]{2}[F1-9])',fields.get('契約代號',''))
            if change and not fields.get('契約乘數') and index+1<len(entries):
                npg,nti,nxt=entries[index+1];following=nxt['cells']
                continuation=(npg['page']==page['page']+1 and nti==0 and not compact(nxt.get('caption',''))
                    and following and all(len(r)==2 for r in following)
                    and not compact(str(following[0][0] or '')))
                if continuation:
                    extra={compact(str(k or '')):compact(str(v or '')) for k,v in following if k}
                    multiplier=extra.get('契約乘數','')
                    if (set(extra)=={'契約乘數'} and re.match(re.escape(change[2])+r'(?:契約|履約價格)乘數',multiplier)):
                        cells=[[str(k or ''),str(v or '')] for k,v in cells]
                        for key,value in following:
                            if not compact(str(key or '')):cells[-1][1]+='\n'+str(value or '')
                            else:cells.append([key,value])
                        segments.append([npg['page'],nti])
        for fact in corporate_grid_candidates(cells,text,table.get('caption',''),
                page_text=page.get('ocr_text') or page['native_text']):
            result.append(dict(fact,page=page['page'],table_index=ti,table_segments=segments,
                               extraction_method=table.get('extraction_method','native_cell_grid')))
    return result


def corporate_position_table_candidates(pages: list[dict], *, corporate: list[dict] | None = None) -> list[dict]:
    """Preserve explicit share caps and conversions from corporate notices.

    Adjusted and standard futures share a dated securities-unit cap. It is not
    a number of standard contracts. Delisting-relative ends stay unresolved
    until a legal lifecycle is supplied by the release builder.
    """
    text=compact('\n'.join(p.get('ocr_text') or p['native_text'] for p in pages))
    identities=corporate_native_table_candidates(pages) if corporate is None else corporate
    changes={(r['from_product'],r.get('to_product',r['product'])) for r in identities
             if r['from_product']!=r.get('to_product',r['product'])}
    conversions={};caps=[];shared_table_groups=set()
    for page in pages:
        for ti,table in enumerate(page['tables']):
            cells=[[compact(str(v or '')) for v in row] for row in table['cells']]
            if len(cells)<2 or len({len(row) for row in cells})!=1:continue
            if cells[0][0]=='持有部位' and cells[1][0] in ('每口折算股數','每口折算單位數'):
                for code,amount in zip(cells[0][1:],cells[1][1:]):
                    if re.fullmatch('[A-Z]{2}[F1-9]',code) and re.fullmatch(r'\d[\d,]*(?:\.\d+)?',amount):
                        conversions.setdefault(code,set()).add(float(amount.replace(',','')))
            natural=[r for r in cells[1:] if r[0]=='自然人']
            if cells[0][0]=='持有部位' and len(natural)==1:
                values=[v for v in natural[0][1:] if v]
                members=cells[0][1:]
                if (len(values)==1 and re.fullmatch(r'[\d,]+(?:\.\d+)?(?:股|受益權單位)',values[0])
                        and all(re.fullmatch('[A-Z]{2}[F1-9]',c) for c in members)):
                    # A single merged cap cell spans the named contracts.
                    # This is an explicit table grouping, not a family guess.
                    shared_table_groups.add(tuple(sorted(set(members))))
            if len(natural)==1:
                cap_cells=list(zip(cells[0][1:],natural[0][1:]))
            elif cells[0].count('自然人')==1 and len(cells)==2:
                # Older notices transpose person categories and put the
                # dated period immediately above each grid, not in its row.
                caption=compact(table.get('caption',''))
                header=caption.rsplit('適用期間:',1)[-1]
                if not header.startswith('自'):
                    # A single constant cap can have its period at the start
                    # of this page's position section, above the unit grid.
                    local=compact(page.get('ocr_text') or page['native_text'])
                    section=local.split('部位限制:',1)
                    scopes=re.findall(r'自\d{2,3}年\d{1,2}月\d{1,2}日[^。；;]{0,120}?契約終止掛牌前一營業日止',
                                      section[1] if len(section)==2 else '')
                    if len(scopes)!=1:continue
                    header=scopes[0]
                cap_cells=[(header,cells[1][cells[0].index('自然人')])]
            else:continue
            for header,amount in cap_cells:
                number=re.fullmatch(r'([\d,]+(?:\.\d+)?)(股|受益權單位|單位)',amount)
                dates=list(re.finditer(r'(?<!\d)(\d{2,3})(?:[./]|年)(\d{1,2})(?:[./]|月)(\d{1,2})(?:日)?',header))
                if not number or not dates or not header.startswith('自'):continue
                try:days=[str(date(int(m[1])+1911,int(m[2]),int(m[3]))) for m in dates]
                except ValueError:continue
                if len(days)==2 and days[1]>=days[0]:
                    end=days[1];end_rule='explicit_date_inclusive'
                elif len(days)==1 and re.search(r'契約(?:皆|均)?終止掛牌前一營業日',header):
                    end=None;end_rule=('previous_business_day_before_all_named_contracts_delisting'
                        if re.search(r'契約(?:皆|均)終止掛牌',header)
                        else 'previous_business_day_before_contract_delisting')
                elif len(days)==1 and '契約均到期或撤銷掛牌之日' in header:
                    end=None;end_rule='all_contracts_expiry_or_delisting_inclusive'
                elif len(days)==1 and re.fullmatch(r'自'+ROC_DATE+r'起改按標的證券股數計算[:：]?',header):
                    end=None;end_rule='until_superseding_rule'
                else:continue
                caps.append(dict(effective_date=days[0],valid_until_date_inclusive=end,
                    end_rule=end_rule,natural_person_limit=float(number[1].replace(',','')),
                    unit='shares' if number[2]=='股' else 'beneficial_units',
                    source_period_text=header,page=page['page'],table_index=ti,
                    extraction_method=table.get('extraction_method','native_cell_grid')))
    result=[];code=r'[A-Z]{2}[F1-9]'
    groups={tuple(sorted(set(re.findall(code,m[1])))) for m in re.finditer(
        r'('+code+r'(?:(?:、|與|及|暨)'+code+r')+)部位合併計算',text)}
    groups.update(shared_table_groups)
    if not groups:
        groups={(after,) for before,after in changes if set(conversions)=={after}}
    for group in sorted(groups):
        members=list(group)
        if (not any(before in members or after in members for before,after in changes)
                or any(len(conversions.get(c,set()))!=1 for c in members)):continue
        if corporate is not None:
            # A reviewed position page may reuse independently resolved terms
            # from the same notice. Its digits must still agree exactly.
            adjusted=[c for c in members if re.fullmatch(r'[A-Z]{2}[1-9]',c)]
            if any({f['contract_multiplier'] for f in corporate if f['product']==c}
                   !=conversions[c] for c in adjusted):continue
        for cap in caps:
            # More than one adjustment pair on a page must have a matching
            # period caption; do not lend another issuer's allowance.
            if len(groups)>1 and not all(member in cap['source_period_text'] for member in members):
                continue
            for code in members:
                result.append(dict(cap,product=code,position_unit=next(iter(conversions[code])),
                    combined_products=members,effective_phase='product_regular_open',
                    event_type='corporate_securities_unit_limit',direction='requires_dated_same_direction_rule',
                    limit_follows_applicable_grade='部位限制數應依本契約最新適用部位限制級數計算' in text,
                    requires_adjusted_contract_and_mini_group_review=True,
                    requires_delisting_clock=cap['end_rule'] not in ('explicit_date_inclusive','until_superseding_rule'),candidate_only=True))
    return result


def corporate_position_flat_table_candidates(source_text: str, corporate: list[dict]) -> list[dict]:
    """Read a retained table whose cells survived as whitespace-separated text.

    Codes, every conversion unit, person headings and period boundaries must
    survive. Unlike OCR digit repair, this only restores the table's two axes.
    Multiple adjusted codes and merger origins are valid when explicitly named.
    """
    text=unicodedata.normalize('NFKC',source_text).translate(str.maketrans('数约标证终业计准营','數約標證終業計準營'))
    text=re.sub(r'\[PAGE\s+\d+\]', '', text)
    label=lambda value:r'\s*'.join(re.escape(c) for c in value)
    heading=re.search(label('部位限制')+r'\s*[:：]',text)
    if heading is None:return []
    section=text[heading.end():];dense=compact(section).replace('计','計')
    table=re.search(label('持有部位')+r'(.*?)'+label('每口折算股數')+r'(.*?)(?:'
                    +label('部位合併計算')+'|'+label('部位限制數')+')',
                    section,re.S)
    if table is None:return []
    codes=re.findall(r'[A-Z]{2}[A-Z1-9]',compact(table[1]))
    # Page/section furniture is not a numeric cell. Reject any other residue.
    numeric=re.sub(r'第\s*\d+\s*頁\s*[,，]?\s*共\s*\d+\s*頁|[()一二:：|]', ' ',table[2])
    tokens=numeric.split()
    number=r'(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?'
    if not codes or len(tokens)!=len(codes) or any(not re.fullmatch(number,x) for x in tokens):return []
    units=dict(zip(codes,[float(x.replace(',','')) for x in tokens]))
    if len(units)!=len(codes) or any(v<=0 for v in units.values()):return []
    facts={}
    for f in corporate:
        product=f.get('product')
        if product in units and f.get('issue_date_bound'):
            identity=(f.get('effective_date'),f.get('contract_multiplier'))
            facts.setdefault(product,set()).add(identity)
    if not facts or any(len(v)!=1 for v in facts.values()):return []
    if any(next(iter(v))[1]!=units[k] for k,v in facts.items()):return []
    days={next(iter(v))[0] for v in facts.values()}
    if len(days)!=1 or None in days:return []
    day=next(iter(days))
    groups=[]
    for match in re.finditer(r'((?:[A-Z]{2}[F1-9][、與及和])+[A-Z]{2}[F1-9])部位合併計算',dense):
        group=re.findall(r'[A-Z]{2}[F1-9]',match[1])
        if len(set(group))!=len(group) or any(c not in units for c in group):continue
        if not set(group)&set(facts):continue
        if any(c[-1].isdigit() and c not in facts for c in group):continue
        groups.append(group)
    if not groups and len(codes)==1 and codes[0] in facts:
        groups=[codes]  # A delisted standard may have no newly issued sibling.
    if len(groups)!=1:return []
    group=groups[0]
    natural=re.search(r'自然人('+number+r')股('+number+r')股法人',dense)
    if natural:
        header=dense[:natural.start()];dates=[]
        for match in re.finditer(r'(?<!\d)(\d{2,3})(?:[./]|年)(\d{1,2})(?:[./]|月)(\d{1,2})(?:日)?',header):
            try:value=date(int(match[1])+1911,int(match[2]),int(match[3]))
            except ValueError:return []
            if value not in dates:dates.append(value)
        if (len(dates)!=3 or str(dates[0])!=day or dates[1]<dates[0]
                or dates[2]<=dates[1] or '終止掛牌前一營業日' not in header):return []
        periods=[(str(dates[0]),str(dates[1]),float(natural[1].replace(',',''))),
                 (str(dates[2]),None,float(natural[2].replace(',','')))]
    else:
        natural=re.search(r'自然人(?:法人(?:機構|/期貨自營商))造市者('+number+r')股',dense)
        if natural is None:return []
        header=dense[:natural.start()]
        dates=[]
        for match in re.finditer(ROC_DATE,header):
            value=roc_date(match)
            if value not in dates:dates.append(value)
        if not dates and len(group)==1 and group[0] in facts:
            dates=[date.fromisoformat(day)]
        elif (len(dates)!=1 or str(dates[0])!=day or '終止掛牌前一營業日' not in header):return []
        periods=[(day,None,float(natural[1].replace(',','')))]
    rows=[]
    for start,end,cap in periods:
        for code in group:
            rows.append(dict(product=code,position_unit=units[code],combined_products=group,
                unit='shares',natural_person_limit=cap,effective_date=start,
                valid_until_date_inclusive=end,requires_delisting_clock=end is None,
                end_rule='explicit_date_inclusive' if end else 'previous_business_day_before_contract_delisting',
                effective_phase='product_regular_open',candidate_only=True,
                requires_adjusted_contract_and_mini_group_review=True,
                limit_follows_applicable_grade='最新適用部位限制級數' in dense,
                event_type='corporate_securities_unit_limit',
                extraction_method='explicit_whitespace_corporate_cap_table'))
    return rows


def corporate_position_text_candidates(source_text: str, corporate: list[dict]) -> list[dict]:
    """Read explicit old notice caps using already source-bound identities.

    Flattened scans often have no cell grid. Do not repair their digits or
    codes: one unambiguous adjustment and its separately parsed units must
    agree with the standard-contract section. Missing/ambiguous dates or cap
    cells remain unresolved. The result is still an unadmitted candidate.
    """
    flat=corporate_position_flat_table_candidates(source_text,corporate)
    if flat:return flat
    text=compact(source_text).replace('|','').replace('標准','標準').replace('计','計').replace('营','營')
    identities={(f.get('from_product'),f.get('product'),f.get('effective_date'),
                 f.get('contract_multiplier')) for f in corporate
                if f.get('from_product')!=f.get('product') and f.get('issue_date_bound')}
    if len(identities)!=1:return []
    before,after,day,units=next(iter(identities))
    if (not before or not re.fullmatch(r'[A-Z]{2}[F1-9]',before)
            or not after or not re.fullmatch(r'[A-Z]{2}[1-9]',after)
            or not day or units is None or units<=0):return []
    start=re.search(r'(?:加掛|推出)(?:標準(?:型)?|新)(?:股票期貨)?契約',text)
    if start is None:return []
    section=text[start.end():]
    split=section.find('部位限制')
    if split<0:return []
    standard=section[:split];limits=section[split:]
    # A second adjustment can originate in AA1 and create AA2. The notice's
    # separately named standard contract (AAF), not AA1, owns the group cap.
    standard_code=after[:2]+'F'
    if before!=standard_code:
        if standard_code not in standard:return []
        before=standard_code
    sizes={float(m[1].replace(',','')) for m in re.finditer(r'約定標的物([\d,]+)(?:股|單位)標的證券',standard)}
    if len(sizes)!=1 or next(iter(sizes)) not in (100.,2000.):return []
    base_units=next(iter(sizes))
    named=(after+'與'+before+'部位合併計算' in limits
           or before+'與'+after+'部位合併計算' in limits)
    generic='標準契約與調整契約部位合併計算' in limits
    if not (named or generic):return []
    common=dict(effective_phase='product_regular_open',candidate_only=True,
        requires_adjusted_contract_and_mini_group_review=True)
    if ('每口折算' not in limits and '改按' not in limits and units==base_units):
        return [dict(common,product=after,effective_date=day,unit='contracts',
            event_type='combined_position_formula',natural_person_limit=None,
            combined_position_base_product=before,combined_position_ratio='1/1',
            combined_position_evidence=limits[:300],
            extraction_method='explicit_unchanged_unit_corporate_combination')]
    if '每口折算股數' not in limits or not named:return []
    natural=re.search(r'自然人([\d,]+)股([\d,]+)股法人',limits)
    if natural is None:return []
    header=limits[:natural.start()]
    dates=[]
    for match in re.finditer(r'(?<!\d)(\d{2,3})(?:[./]|年)(\d{1,2})(?:[./]|月)(\d{1,2})(?:日)?',header):
        try:parsed=date(int(match[1])+1911,int(match[2]),int(match[3]))
        except ValueError:return []
        if parsed not in dates:dates.append(parsed)
    if (len(dates)!=3 or str(dates[0])!=day or dates[1]<dates[0]
            or dates[2]<=dates[1]
            or '終止掛牌前一營業日' not in header):return []
    amounts=[int(natural[i].replace(',','')) for i in (1,2)]
    if any(v<=0 for v in amounts):return []
    result=[]
    for i,amount in enumerate(amounts):
        for code,weight in ((before,base_units),(after,units)):
            result.append(dict(common,product=code,position_unit=weight,
                combined_products=[before,after],unit='shares',natural_person_limit=amount,
                effective_date=str(dates[0] if i==0 else dates[2]),
                valid_until_date_inclusive=str(dates[1]) if i==0 else None,
                end_rule='explicit_date_inclusive' if i==0 else 'previous_business_day_before_contract_delisting',
                requires_delisting_clock=bool(i),event_type='corporate_securities_unit_limit',
                extraction_method='explicit_flattened_corporate_cap_cells'))
    return result


def corporate_text_candidates(source_text: str) -> list[dict]:
    """Recover explicit table fields from retained Word/OCR text as candidates.

    Never correct O/0, I/1, years or digits from expectations. A missed label
    remains missing. Options sections and newly listed standard tables cannot
    supply a futures adjustment's multiplier or cash credit.
    """
    text = compact(source_text)
    code = r'[A-Z]{2}[F1-9]'
    pattern = re.compile(r'(?:(?P<old>'+code+r')調整為(?P<new>'+code+r')|不調整\(仍為(?P<same>'+code+r')\))')
    changes = list(pattern.finditer(text))
    result=[]
    standard_section = r'(?:加掛|推出)(?:新|標準(?:型)?)(?:股票期貨)?契約'
    labels = r'契約代號|調整契約月份|契約調整月份|約定標的物|契約乘數|買方權益數加項|賣方權益數減項|註\d*[:：]|'+standard_section+r'|選擇權調整'
    def field(body, label):
        matches=list(re.finditer(label,body))
        if not matches:return ''
        if len(matches) != 1:
            # A row header is repeated literally inside the value in official
            # cash-equity clauses. Accept exactly that syntax, not two tables.
            if (len(matches)==2 and label in ('買方權益數加項','賣方權益數減項')
                    and re.fullmatch(r'[|:：]?每口'+('買方' if label.startswith('買') else '賣方')+
                                     r'未沖銷部位調整',body[matches[0].end():matches[1].start()])):
                matches=matches[1:]
            elif (len(matches)==2 and label=='契約乘數'
                    and re.fullmatch(r'[|:：]?'+code,body[matches[0].end():matches[1].start()])):
                matches=matches[1:]
            else:
                return ''
        start=matches[0].end()
        end=re.search(labels,body[start:])
        return body[start:start+end.start()] if end else body[start:]
    for i, match in enumerate(changes):
        old,new=(match['old'],match['new']) if match['old'] else (match['same'],match['same'])
        end=changes[i+1].start() if i+1<len(changes) else len(text)
        body=text[match.end():end]
        stop=re.search(r'選擇權調整|'+standard_section,body)
        if stop:body=body[:stop.start()]
        multiplier=field(body,r'契約乘數')
        if not re.search(r'(?:調整為|仍為)[\d,]+(?:\.\d+)?(?:\)|。|$)',multiplier):
            continue
        cells=[['契約代號',f'{old}調整為{new}' if old!=new else f'不調整(仍為{old})'],
               ['契約乘數',multiplier],['約定標的物',field(body,r'約定標的物')]]
        months=field(body,r'(?:調整契約|契約調整|契約)月份')
        # In flattened text the next row's product prefix precedes its label,
        # e.g. "到期契約HN2約定標的物". Remove only the bound code at that
        # exact boundary. A stray OCR digit/year remains an extraction error.
        months=re.sub(r'(?<=到期契約)'+re.escape(new)+r'$', '',months)
        if months:cells.append(['調整契約月份',months])
        for label in ('買方權益數加項','賣方權益數減項'):
            value=field(body,label)
            if value:cells.append([label,value])
        # Only the immediately preceding adjustment section can lend a caption.
        previous=text[:match.start()]
        boundary=max(previous.rfind('調整生效日'),previous.rfind('調整契約月份'),
                     previous.rfind('契約調整月份'))
        caption=previous[boundary:] if boundary>=0 else ''
        for fact in corporate_grid_candidates(cells,source_text,caption):
            fact.update(text_offset=match.start(),extraction_method='explicit_text_fields_requires_source_review')
            result.append(fact)
    return result


def position_change(text: str) -> dict | None:
    text = compact(text)
    try:
        clause = text.split("二、", 1)[1].split("三、", 1)[0]
    except IndexError as error:
        raise ValueError("position notice clause structure changed") from error
    if not clause.startswith("「臺股期貨」"):
        raise ValueError("position notice does not isolate TX")
    tx = clause.split("「臺指選擇權」", 1)[0]
    if "自然人" not in tx:
        if "法人部位限制數由" in tx:
            return None
        raise ValueError("unrecognized TX position amendment")
    number = r"([\d,]+)(?:個契約|口)"
    combined = re.search(r"自然人與法人部位限制數分別由" + number + "與" + number
                         + r"調[升高降]至" + number + "與" + number, tx)
    separate = re.search(r"自然人部位限制數由" + number + r"調[升高降]至" + number, tx)
    if combined:
        before, after = combined[1], combined[3]
    elif separate:
        before, after = separate[1], separate[2]
    else:
        raise ValueError("unrecognized natural-person position amounts")
    # First effective date in clause II applies to TX, including amendments
    # where TX and TXO share a trailing date. Never take a date from clause III.
    m = re.search("自" + ROC_DATE + "一般交易時段起生效", clause)
    if m is None:
        raise ValueError("missing TX position effective opening")
    return dict(before=int(before.replace(",", "")), after=int(after.replace(",", "")),
                effective_at=timestamp(roc_date(m), "08:45:00"))


def margin_candidate_intervals(facts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Bound explicit levels and leave unresolved/restoration periods empty.

    These are product-relative phases, not executor timestamps. In particular,
    a temporary notice proves its raised level only BEFORE the earliest stated
    restoration boundary; it never proves that a conditional restoration took
    place. A new absolute level can re-anchor after an intervening gap.
    """
    from collections import defaultdict
    import math
    phases={'product_regular_open':0,'new_contract_listing':0,'after_product_regular_close':1}
    grouped=defaultdict(list);issues=[]
    for fact in facts:
        reasons=[];day=fact.get('effective_date');phase=fact.get('effective_phase')
        try:
            if day:date.fromisoformat(day)
            else:raise ValueError('effective date missing')
        except (ValueError, TypeError):
            reasons.append('effective_date_unresolved');day=fact.get('published_date')
            try:
                date.fromisoformat(day)
            except (ValueError, TypeError):
                day=None
        if not day:
            issues.append(dict(product=fact['product'],reasons='unlocatable_source_boundary',
                               source_content_sha256=fact.get('source_content_sha256')))
            continue
        if phase not in phases:reasons.append('product_session_phase_unresolved')
        if not fact.get('issue_date_bound'):reasons.append('publication_not_bound')
        try:
            known=datetime.fromisoformat(fact.get('known_at') or '')
            if known.tzinfo is None:raise ValueError('timezone missing')
        except ValueError:
            reasons.append('publication_timestamp_missing_or_invalid')
        if not re.fullmatch('[a-f0-9]{64}',fact.get('source_content_sha256') or ''):
            reasons.append('source_content_identity_missing_or_invalid')
        kind=fact.get('margin_kind');after=fact.get('after') or []
        if kind not in ('fixed_twd','notional_rate'):reasons.append('dated_twd_or_rate_unit_required')
        if (len(after)!=3 or not all(isinstance(v,(int,float)) and not isinstance(v,bool)
                and math.isfinite(v) and v>0 for v in after)
                or (len(after)==3 and not after[0]>=after[1]>=after[2])
                or (kind=='notional_rate' and after and after[0]>1)):
            reasons.append('margin_level_or_hierarchy_invalid')
        ends=[]
        try:
            for end in json.loads(fact.get('temporary_end_evidence') or '[]'):
                if end['boundary']!='after_regular_session':raise ValueError('restoration phase')
                date.fromisoformat(end['date_iso'])
                ends.append((end['date_iso'],1))
        except (ValueError,KeyError,TypeError):
            reasons.append('restoration_boundary_unresolved')
        if fact.get('requires_reversion_review') and not ends:
            reasons.append('conditional_effective_or_restoration_rule_unresolved')
        boundary=(day,phases.get(phase,0))
        if ends and min(ends)<=boundary:reasons.append('restoration_does_not_follow_start')
        if reasons:
            issues.append(dict(product=fact['product'],effective_date=day,
                source_content_sha256=fact.get('source_content_sha256'),reasons=';'.join(reasons)))
        grouped[(fact['product'],boundary)].append((fact,reasons,min(ends) if ends else None))
    by_product=defaultdict(list)
    for (product,boundary),views in grouped.items():
        valid=[(fact,end) for fact,reasons,end in views if not reasons]
        signatures={(fact['margin_kind'],tuple(fact['after'])) for fact,end in valid}
        good_hashes={fact['source_content_sha256'] for fact,end in valid}
        bad_hashes={fact.get('source_content_sha256') for fact,reasons,end in views if reasons}
        state=None
        if len(signatures)==1 and not bad_hashes-good_hashes:
            kind,amount=next(iter(signatures))
            before={tuple(fact['before']) for fact,end in valid if fact.get('before') is not None}
            ends=[end for fact,end in valid if end is not None]
            state=dict(product=product,margin_kind=kind,initial=amount[0],maintenance=amount[1],
                clearing=amount[2],effective_date=boundary[0],effective_phase=boundary[1],
                known_at=max(fact['known_at'] for fact,end in valid),
                # A same-day publication cannot authorize that day's opening,
                # but it can identify a level for subsequent decisions. Keep
                # legal effectiveness and conservative knowledge time separate.
                requires_delayed_admission=any(datetime.fromisoformat(fact['known_at']) >=
                    datetime.combine(date.fromisoformat(boundary[0]),time(),TAIPEI)
                    for fact,end in valid),
                source_content_sha256s=sorted(good_hashes),source_urls=sorted({f['source_url'] for f,e in valid}),
                _before=before,_restoration=min(ends) if ends else None,
                point_in_time_verified=False,requires_dated_session_and_archive_admission=True)
        else:
            issues.append(dict(product=product,effective_date=boundary[0],
                reasons='conflicting_or_unresolved_margin_boundary',
                source_content_sha256='|'.join(sorted(str(f.get('source_content_sha256')) for f,_,_ in views))))
        by_product[product].append((boundary,state))
    intervals=[]
    for product,boundaries in by_product.items():
        boundaries.sort(key=lambda x:x[0])
        for i,(start,state) in enumerate(boundaries):
            if state is None:continue
            end=boundaries[i+1][0] if i+1<len(boundaries) else None
            next_state=boundaries[i+1][1] if end is not None else None
            restore=state.pop('_restoration')
            state.pop('_before')
            if next_state is not None and (restore is None or restore>=end):
                expected={(state['initial'],state['maintenance'],state['clearing'])}
                declared=next_state['_before']
                if declared and (declared!=expected or next_state['margin_kind']!=state['margin_kind']):
                    issues.append(dict(product=product,effective_date=start[0],
                        next_effective_date=end[0],reasons='prior_interval_before_after_chain_disagrees',
                        source_content_sha256='|'.join(state['source_content_sha256s'])))
                    continue
            if restore is not None and (end is None or restore<end):end=restore
            if end is not None and datetime.fromisoformat(state['known_at']).astimezone(TAIPEI).date()>date.fromisoformat(end[0]):
                issues.append(dict(product=product,effective_date=start[0],
                    reasons='rule_became_known_after_interval_ended',
                    source_content_sha256='|'.join(state['source_content_sha256s'])))
                continue
            intervals.append(dict(state,valid_until_date_exclusive=end[0] if end else None,
                valid_until_phase_exclusive=end[1] if end else None,
                temporary_level=restore is not None,
                restoration_applied=False))
    return sorted(intervals,key=lambda r:(r['product'],r['effective_date'],r['effective_phase'])),issues


def disposal_margin_restorations(facts: list[dict], universe: pl.DataFrame,
                                dispositions: pl.DataFrame, observations: pl.DataFrame):
    """Resolve conditional restorations using actual completed cash sessions.

    A provider disposition row alone is not authority for the margin amount.
    The TAIFEX notice supplies both amount and boundary. A matching disposition
    supplies its explicit session count, and positive official cash prints prove
    that those sessions elapsed without a full-day suspension. Missing prices,
    ambiguous counts, extensions and intervening margin changes remain gaps.
    """
    from downloader.taifex_rule_parsing import _integer
    securities=dict(universe.select('product','underlying_symbol').iter_rows())
    disposition_fields={'date','stock_id','period_start','period_end','measure','source_sha256'}
    if disposition_fields-set(dispositions.columns):
        raise ValueError('restorations require source-bound disposition periods')
    if dispositions.filter(~pl.col('source_sha256').str.contains(r'^[a-f0-9]{64}$').fill_null(False)).height:
        raise ValueError('invalid disposition source identities')
    needed={'date','symbol','volume','source_sha256'}
    if needed-set(observations.columns):raise ValueError('restorations require source-bound cash observations')
    valid=observations.filter(pl.col('volume').is_finite() & (pl.col('volume')>0)
        & pl.col('source_sha256').str.contains(r'^[a-f0-9]{64}$'))
    # Duplicate provider/exchange views are not additional trading sessions.
    traded={}
    for row in valid.select('date','symbol','source_sha256').unique().iter_rows(named=True):
        traded.setdefault(row['symbol'],{}).setdefault(str(row['date']),set()).add(row['source_sha256'])
    by_security={}
    for row in dispositions.iter_rows(named=True):
        by_security.setdefault(row['stock_id'],[]).append(row)
    resolved=[];issues=[];seen=set()
    for fact in facts:
        if (not fact.get('requires_reversion_review') or not fact.get('before')
                or not fact.get('effective_date') or not fact.get('published_date')
                or not fact.get('issue_date_bound') or fact.get('restoration_rule')!='return_to_declared_before'):
            continue
        ends=json.loads(fact.get('temporary_end_evidence') or '[]')
        candidates={(r['date_iso'],r['boundary']) for r in ends}
        if len(candidates)!=1:continue
        end,boundary=next(iter(candidates));product=fact['product'];security=securities.get(product)
        key=(product,fact.get('effective_date'),end,fact.get('source_content_sha256'))
        if key in seen:continue
        seen.add(key)
        if boundary not in ('after_regular_session','after_trading_session') or not security:continue
        matches=[r for r in by_security.get(security,[]) if r['period_end']==end
                 and r['period_start']<=fact['effective_date'] and r['date']<=fact['published_date']]
        signatures={(r['period_start'],r['period_end'],r['measure']) for r in matches}
        reason=None
        if len(signatures)!=1:reason='disposition_period_missing_or_ambiguous'
        else:
            start,_,measure=next(iter(signatures))
            counts={_integer(m[1]) for m in re.finditer(r'起([0-9零〇一二三四五六七八九十]+)個營業日',compact(measure))}
            # TWSE uses a period followed by a parenthesized Chinese duration.
            if not counts:
                counts={_integer(m[1]) for m in re.finditer(r'[﹝〔（(\[]([0-9零〇一二三四五六七八九十]+)個營業日',compact(measure))}
            days=sorted(d for d in traded.get(security,{}) if start<=d<=end)
            if len(counts)!=1 or not days or days[-1]!=end or len(days)!=next(iter(counts),-1):
                reason='completed_cash_session_count_does_not_prove_announced_restoration'
            elif any(r['period_start']<=end<r['period_end'] for r in by_security.get(security,[])):
                reason='overlapping_extended_disposition'
            elif any(r['product']==product and fact['effective_date']<str(r.get('effective_date') or '')<=end
                     for r in facts):
                reason='intervening_margin_event_requires_composed_restoration'
        if reason:
            issues.append(dict(product=product,effective_date=fact['effective_date'],restore_date=end,reasons=reason))
            continue
        evidence=dict(stock_id=security,disposition_start=start,disposition_end=end,
            completed_cash_dates=days,required_sessions=next(iter(counts)),
            disposition_source_sha256s=sorted({r['source_sha256'] for r in matches}),
            cash_source_sha256s=sorted({s for d in days for s in traded[security][d]}),
            confirmation_scope='completed_cash_sessions_only_not_a_provider_completeness_claim')
        resolved.append(dict(fact,after=list(fact['before']),before=list(fact['after']),
            effective_date=end,effective_phase='after_product_regular_close',
            known_at=timestamp(date.fromisoformat(end),'13:35:00'),
            requires_reversion_review=False,temporary_end_evidence='[]',event_type='before_after',
            extraction='observed_disposition_conditional_restoration',
            original_rule_known_at=fact['known_at'],restoration_evidence=json.dumps(evidence,ensure_ascii=False),
            requires_dated_session_rule=True,point_in_time_verified=False,candidate_only=True))
    return resolved,issues


def position_candidate_intervals(facts: list[dict]) -> tuple[list[dict], list[dict]]:
    """Bound dated caps without inventing missing conversions or delist dates.

    Separate all-month, single-month and securities-unit limits. Formula-only
    records remain joins to a dated base product, never an unlimited position.
    The result is evidence for the release builder, not an executable rule ABI.
    """
    from collections import defaultdict
    from fractions import Fraction
    import math
    grouped=defaultdict(list);issues=[]
    def positive(value):
        return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and value>0
    for fact in facts:
        reasons=[];day=fact.get('effective_date');product=fact['product']
        revoked=fact.get('notice_revoked_effective_date')
        if revoked:
            date.fromisoformat(revoked)
            if (not re.fullmatch('[a-f0-9]{64}',fact.get('notice_revocation_source_sha256') or '')
                    or not re.fullmatch('[a-f0-9]{64}',fact.get('notice_revocation_review_sha256') or '')):
                raise ValueError('position notice revocation lacks source-bound review')
            if day and day>=revoked:
                issues.append(dict(product=product,effective_date=day,
                    reasons='notice_revoked_before_scheduled_effective_date',
                    source_content_sha256=fact.get('source_content_sha256')))
                continue
        try:date.fromisoformat(day)
        except (ValueError,TypeError):
            reasons.append('effective_date_unresolved');day=fact.get('published_date')
            try:date.fromisoformat(day)
            except (ValueError,TypeError):
                issues.append(dict(product=product,reasons='unlocatable_position_boundary',
                    source_content_sha256=fact.get('source_content_sha256')));continue
        phase=fact.get('effective_phase')
        if phase not in ('product_regular_open','new_contract_listing','date_only_requires_phase_review'):
            reasons.append('product_session_phase_unresolved')
        if not fact.get('issue_date_bound'):reasons.append('publication_not_bound')
        try:
            known=datetime.fromisoformat(fact.get('known_at') or '')
            if known.tzinfo is None:raise ValueError('timezone missing')
        except ValueError:reasons.append('publication_timestamp_missing_or_invalid')
        if not re.fullmatch('[a-f0-9]{64}',fact.get('source_content_sha256') or ''):
            reasons.append('source_content_identity_missing_or_invalid')
        kind=fact.get('event_type') or 'absolute_level';unit=fact.get('unit')
        amount=fact.get('natural_person_limit');monthly=fact.get('natural_person_monthly_limit')
        position_unit=1.;members=None;base=None;ratio=None
        if kind=='combined_position_formula':
            base=fact.get('combined_position_base_product')
            if not base:reasons.append('dated_base_product_identity_unresolved')
            try:
                ratio=Fraction(fact.get('combined_position_ratio') or '')
                # A dated unchanged-unit adjusted code shares its root's cap
                # one-for-one. Mini conversions remain strictly fractional.
                unchanged=(fact.get('extraction_method')=='explicit_unchanged_unit_corporate_combination'
                           and re.fullmatch(r'[A-Z]{2}[1-9]',product)
                           and base==product[:-1]+'F')
                if not (0<ratio<1 or (ratio==1 and unchanged)):raise ValueError('conversion ratio')
            except (ValueError,ZeroDivisionError):reasons.append('dated_position_conversion_invalid')
            if amount is not None or monthly is not None:reasons.append('formula_has_independent_amount')
        elif kind in ('absolute_level','corporate_securities_unit_limit'):
            if not positive(amount):reasons.append('position_limit_invalid')
            if monthly is not None and (not positive(monthly) or not positive(amount) or monthly>amount):
                reasons.append('monthly_position_limit_invalid')
            if kind=='corporate_securities_unit_limit':
                position_unit=fact.get('position_unit');members=sorted(set(fact.get('combined_products') or []))
                if unit not in ('shares','beneficial_units') or not positive(position_unit):
                    reasons.append('dated_securities_position_unit_invalid')
                if not members or product not in members:reasons.append('dated_combined_group_invalid')
            elif unit!='contracts':reasons.append('absolute_position_unit_unresolved')
        else:reasons.append('position_event_type_unsupported')
        inclusive=fact.get('valid_until_date_inclusive')
        end=None
        if inclusive:
            try:
                end=str(date.fromisoformat(inclusive)+timedelta(days=1))
                if end<=day:raise ValueError('invalid position period')
            except ValueError:reasons.append('explicit_position_end_invalid')
        if revoked:end=min([value for value in (end,revoked) if value])
        if reasons:
            issues.append(dict(product=product,effective_date=day,reasons=';'.join(reasons),
                               source_content_sha256=fact.get('source_content_sha256')))
        state=dict(product=product,effective_date=day,effective_phase=0,
            legal_date_only=phase=='date_only_requires_phase_review',known_at=fact.get('known_at'),
            # The legal boundary remains the declared date. Only admission is
            # delayed; do not keep a superseded cap through an uncertain day.
            admission_not_before=(timestamp(date.fromisoformat(day),'23:59:59')
                if phase=='date_only_requires_phase_review' else None),
            event_type=kind,unit=unit,
            position_unit=float(position_unit) if positive(position_unit) else None,
            position_limit=float(amount) if positive(amount) else None,
            monthly_position_limit=float(monthly) if positive(monthly) else None,combined_products=members,
            combined_position_base_product=base,
            conversion_numerator=ratio.numerator if ratio and not reasons else None,
            conversion_denominator=ratio.denominator if ratio and not reasons else None,
            requires_base_limit_join=kind=='combined_position_formula',
            requires_delisting_clock=bool(fact.get('requires_delisting_clock')),
            explicit_end=end,end_rule=fact.get('end_rule'),
            notice_revoked_effective_date=revoked,
            notice_revocation_source_sha256=fact.get('notice_revocation_source_sha256'),
            point_in_time_verified=False,requires_dated_group_and_lifecycle_admission=True)
        grouped[(product,day)].append((fact,state,reasons))
    products=defaultdict(list)
    for (product,day),views in grouped.items():
        superseded=superseded_notice_sources([fact for fact,state,reasons in views],day)
        views=[(fact,state,reasons) for fact,state,reasons in views if fact['source_content_sha256'] not in superseded]
        valid=[(fact,state) for fact,state,reasons in views if not reasons]
        good={f['source_content_sha256'] for f,s in valid}
        bad={f.get('source_content_sha256') for f,s,reasons in views if reasons}
        economic=lambda s:{k:v for k,v in s.items() if k not in ('known_at','admission_not_before','legal_date_only')}
        # A new standard contract can have a standalone contract-count cap
        # and an explicitly combined securities-unit cap on the same day.
        # Retain the combined constraint when it implies the standalone one;
        # matching numbers never justify discarding its other members or its
        # pending lifecycle. Intersect explicit validity periods as well.
        securities=[s for f,s in valid if s['event_type']=='corporate_securities_unit_limit']
        standalone=[s for f,s in valid if s['event_type']=='absolute_level']
        if (securities and standalone and len(securities)+len(standalone)==len(valid)
                and len({json.dumps(economic(s),sort_keys=True) for s in securities})==1):
            combined=securities[0]
            def same_capacity(s):
                if s['unit']!='contracts':return False
                if Fraction(str(s['position_limit']))*Fraction(str(combined['position_unit'])) != Fraction(str(combined['position_limit'])):
                    return False
                a,b=s['monthly_position_limit'],combined['monthly_position_limit']
                return ((a is None and b is None) or (a is not None and b is not None
                    and Fraction(str(a))*Fraction(str(combined['position_unit']))==Fraction(str(b))))
            if all(same_capacity(s) for s in standalone):
                ends=[s['explicit_end'] for f,s in valid if s['explicit_end']]
                shared=dict(combined,explicit_end=min(ends) if ends else None)
                valid=[(f,dict(shared,known_at=s['known_at'],legal_date_only=s['legal_date_only'],
                    admission_not_before=s['admission_not_before'])) for f,s in valid]
        signatures={json.dumps(economic(s),sort_keys=True) for f,s in valid}
        state=None
        if len(signatures)==1 and not bad-good:
            state=dict(valid[0][1],known_at=max(f['known_at'] for f,s in valid),
                legal_date_only=any(s['legal_date_only'] for f,s in valid),
                admission_not_before=max((s['admission_not_before'] for f,s in valid if s['admission_not_before']),default=None),
                source_content_sha256s=sorted(good),source_urls=sorted({f['source_url'] for f,s in valid}),
                superseded_source_sha256s=sorted(superseded),
                supersession_review_sha256s=sorted({f['supersession_review_sha256'] for f,s in valid
                    if f.get('supersession_review_sha256')}),
                limit_follows_applicable_grade=any(f.get('limit_follows_applicable_grade') for f,s in valid))
        else:
            issues.append(dict(product=product,effective_date=day,
                reasons='conflicting_or_unresolved_position_boundary',
                source_content_sha256='|'.join(sorted(str(f.get('source_content_sha256')) for f,s,r in views))))
        products[product].append((day,state))
    intervals=[]
    for product,events in products.items():
        events.sort(key=lambda e:e[0])
        for i,(day,state) in enumerate(events):
            if state is None:continue
            ends=[e for e in (state.pop('explicit_end'),events[i+1][0] if i+1<len(events) else None) if e]
            end=min(ends) if ends else None
            after=max(filter(None,(state['known_at'],state['admission_not_before'])))
            if end and datetime.fromisoformat(after).astimezone(TAIPEI).date()>=date.fromisoformat(end):
                issues.append(dict(product=product,effective_date=day,
                    reasons='position_became_known_after_interval_ended',
                    source_content_sha256='|'.join(state['source_content_sha256s'])));continue
            intervals.append(dict(state,valid_until_date_exclusive=end))
    return sorted(intervals,key=lambda r:(r['product'],r['effective_date'])),issues


def validate_event_chain(events: list[dict], fields: tuple[str, ...]) -> list[dict]:
    unique = {}
    for event in events:
        key = event["effective_at"]
        if event["known_at"] > key:
            raise ValueError("event was not conservatively public before taking effect")
        if key in unique:
            if any(unique[key][name] != event[name] for name in fields):
                raise ValueError("conflicting simultaneous rule events")
        else:
            unique[key] = event
    ordered = sorted(unique.values(), key=lambda e: e["effective_at"])
    if not ordered:
        raise ValueError("empty rule event chain")
    for left, right in zip(ordered, ordered[1:]):
        if right.get("absolute_level_notice") and right["before"] is None:
            # Old official notices sometimes state a complete replacement level
            # without quoting the previous amount. Do not invent a quoted cell.
            continue
        if left["after"] != right["before"]:
            raise ValueError(f"missing rule transition: {left['effective_at']} -> {right['effective_at']}")
    return ordered


class RuleArchive:
    def __init__(self, root: Path, bundle: Path, reviews: Path | None = None):
        self.root, self.bundle = root, bundle
        self.conn = sqlite3.connect(f"file:{root / 'state/queue.sqlite3'}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("BEGIN")
        self.sources: dict[str, dict] = {}
        self.cache: dict[str, dict] = {}
        self.reviews = {}
        self.legacy = None
        if reviews is not None:
            review_data = json.loads(reviews.read_text())
            if review_data["schema_version"] != 1:
                raise ValueError("unsupported visual review schema")
            self.reviews = {r["url"]: r for r in review_data["reviews"]}
            self.legacy = review_data.get("legacy")
            self.copy(reviews, sha256_file(reviews), url="", kind="visual_transcription_review")
            for source in review_data.get("supplemental_sources", []):
                path = Path(source["path"])
                if hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest() != source["content_sha256"]:
                    raise ValueError("supplemental official content hash mismatch")
                self.copy(path, source["sha256"], url=source["url"], kind="supplemental_official_raw_gzip")
            self.supplemental = {s["url"]: s for s in review_data.get("supplemental_sources", [])}

    def copy(self, path: Path, digest: str, *, url: str, kind: str) -> str:
        if sha256_file(path) != digest:
            raise ValueError(f"archive source hash mismatch: {path}")
        relative = Path("sources") / (digest + "".join(path.suffixes))
        dest = self.bundle / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            shutil.copyfile(path, dest)
        if sha256_file(dest) != digest:
            raise ValueError("copied source hash mismatch")
        self.sources[str(relative)] = dict(path=str(relative), sha256=digest, url=url, kind=kind)
        return str(relative)

    def document(self, url: str) -> dict:
        if url in self.cache:
            return self.cache[url]
        row = self.conn.execute("SELECT * FROM documents WHERE url=?", (url,)).fetchone()
        if row is None or not row["parsed_path"]:
            raise ValueError(f"uncaptured rule document: {url}")
        row = dict(row)
        raw = read_verified_raw(self.root, row)
        version = self.conn.execute(
            "SELECT * FROM parse_versions WHERE url=? AND content_sha256=? AND parser_version=?",
            (url, row["content_sha256"], row["parser_version"])).fetchone()
        if version is None:
            raise ValueError("missing immutable parse version")
        path = self.root / version["parsed_path"]
        if sha256_file(path) != version["parsed_sha256"]:
            raise ValueError("parsed rule evidence hash mismatch")
        parsed = json.loads(path.read_text())
        if parsed["content_sha256"] != hashlib.sha256(raw).hexdigest():
            raise ValueError("parsed/raw document mismatch")
        if url in self.reviews:
            review = self.reviews[url]
            if review["content_sha256"] != parsed["content_sha256"]:
                raise ValueError("visual transcription applies to different PDF bytes")
            parsed = dict(parsed, text=parsed["text"] + "\n" + review["clock_text"])
        # Preserve exact raw and interpreted source bytes in the small rule bundle.
        self.copy(self.root / row["raw_path"], row["raw_sha256"], url=url, kind="raw_gzip")
        self.copy(path, version["parsed_sha256"], url=url, kind="parsed_v2")
        self.cache[url] = parsed
        return parsed

    def children(self, url: str) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT child FROM links WHERE parent=?", (url,))]


def reviewed_legacy_events(archive: RuleArchive, start: date) -> tuple[dict, list, dict]:
    """Accept explicit, byte-bound transcriptions, never unchecked OCR output."""
    margins, positions, notices = {p: [] for p in PRODUCTS}, [], {}
    scope = archive.legacy
    if scope is None:
        return margins, positions, notices
    if start < date.fromisoformat(scope["verified_start"]):
        raise ValueError("requested start predates the reviewed historical rule scope")
    for review in scope["notices"]:
        ann = archive.conn.execute("SELECT * FROM announcements WHERE url=? AND published_date=?",
            (review["announcement_url"], review["published_date"])).fetchone()
        if ann is None or review["url"] not in [ann["url"], *archive.children(ann["url"])]:
            raise ValueError("legacy source is not bound to its announcement")
        doc = archive.document(review["url"])
        if doc["content_sha256"] != review["content_sha256"] or not review["pages_one_based"]:
            raise ValueError("legacy transcription applies to different source bytes or lacks pages")
        if review["announcement_url"] in notices:
            raise ValueError("duplicate legacy notice review")
        notices[review["announcement_url"]] = review
        if review["kind"] == "excluded":
            if not review["reason"]:
                raise ValueError("legacy exclusion lacks a reviewed reason")
            continue
        published = date.fromisoformat(review["published_date"])
        issued = date.fromisoformat(review["issued_date"])
        if not 0 <= (published-issued).days <= 7:
            raise ValueError("legacy issuing date disagrees with index")
        known = timestamp(published, "23:59:59")
        proof = review.get("publication_date_confirmation")
        if proof:
            source = archive.supplemental.get(proof["url"])
            if source is None or source["content_sha256"] != proof["content_sha256"] or not proof["pages_one_based"]:
                raise ValueError("missing independent official publication-date confirmation")
            known = timestamp(issued, "23:59:59")
        event = dict(known_at=known, published_date=str(published), source_url=review["url"],
                     content_sha256=review["content_sha256"], pages_one_based=review["pages_one_based"])
        effective_day = date.fromisoformat(review["effective_date"])
        if review["kind"] == "margin":
            if review["currency"] != "TWD" or review["column_order"] != ["after", "before"]:
                raise ValueError("unverified legacy currency or before/after order")
            if set(review["values"]) != set(PRODUCTS):
                raise ValueError("TX and MTX must each have independently transcribed cells")
            for product, six in review["values"].items():
                if len(six) != 6 or any(type(v) is not int or v <= 0 for v in six):
                    raise ValueError("legacy margin requires six positive integer cells")
                if not (six[0] >= six[1] >= six[2] and six[3] >= six[4] >= six[5]):
                    raise ValueError("legacy margin column order is inconsistent")
                margins[product].append(dict(event, product=product, after=six[:3], before=six[3:],
                    effective_at=timestamp(effective_day, "13:45:00")))
        elif review["kind"] == "position":
            after = review["after"]
            if type(after) is not int or after <= 0:
                raise ValueError("invalid reviewed position limit")
            exchange = timestamp(effective_day, "00:00:00" if review["immediate"] else "08:45:00")
            positions.append(dict(event, before=review.get("before"), after=after, absolute_level_notice=True,
                exchange_effective_at=exchange, effective_at=max(exchange, known),
                delayed_relaxation=known > exchange))
        else:
            raise ValueError("unknown reviewed legacy notice kind")
    positions.sort(key=lambda e: e["effective_at"])
    for i, event in enumerate(positions):
        if event["delayed_relaxation"] and (i == 0 or event["after"] < positions[i-1]["after"]):
            raise ValueError("late-known position tightening cannot be deferred")
    return margins, positions, notices


def build_rule_events(archive: RuleArchive, start: date, end: date) -> tuple[dict, list, list]:
    margins, positions, reviewed = reviewed_legacy_events(archive, start)
    audit = []
    floor = archive.legacy["index_start"] if archive.legacy else "2021-04-01"
    if start < date(2021, 5, 20) and not archive.legacy:
        raise ValueError("earlier scope requires byte-bound legacy source reviews")
    announcements = [dict(r) for r in archive.conn.execute(
        "SELECT * FROM announcements WHERE published_date>=? AND published_date<=? ORDER BY published_date,url",
        (floor, str(end)))]
    # Index captures establish the bounded announcement inventory, independently
    # of the presence of numeric tables or current queue completeness.
    for row in archive.conn.execute("SELECT * FROM index_windows WHERE end>=? AND start<=?",
                                    (floor, str(end))):
        archive.copy(archive.root / row["raw_path"], row["raw_sha256"],
                     url="https://www.taifex.com.tw/cht/11/hisNews", kind="announcement_index")
    for ann in announcements:
        title, url, pub = ann["title"], ann["url"], ann["published_date"]
        if url in reviewed:
            review = reviewed[url]
            audit.append(dict(url=url, published_date=pub, kind=review["kind"],
                status="hash_bound_visual_review", reason=review.get("reason")))
            continue
        is_position = "部位限制" in title and "臺股" in title
        is_margin = "保證金" in title and "抵繳" not in title
        if not (is_position or is_margin):
            continue
        relevant_title = any(x in compact(title) for x in ("臺股期貨", "台股期貨", "小型臺指", "小型台指", "(TX)", "(MTX)"))
        children = archive.children(url)
        csv_urls = [u for u in children if u.lower().endswith(".csv")]
        if not relevant_title and not csv_urls and not is_position:
            continue
        if archive.legacy and pub < archive.legacy["until_exclusive"]:
            if relevant_title or is_position:
                # A separately indexed press release may duplicate a bound
                # primary notice, but it may not silently introduce a new date.
                same = [r for r in reviewed.values() if r["published_date"] == pub
                        and r["kind"] == ("position" if is_position else "margin")]
                if not same:
                    raise ValueError(f"unreviewed historical TX/MTX notice: {url}")
                audit.append(dict(url=url, published_date=pub, kind="position" if is_position else "margin",
                                  status="same_day_notice_duplicate_not_used"))
            continue
        known = timestamp(date.fromisoformat(pub), "23:59:59")
        if is_position:
            urls = [url] if url.lower().endswith(".pdf") else [u for u in children if u.lower().endswith(".pdf")]
            candidates = []
            for u in urls:
                doc = archive.document(u)
                if "「臺股期貨」" in compact(doc["text"]) and "發文日期" in compact(doc["text"]):
                    candidates.append((u, doc))
            if len(candidates) != 1:
                raise ValueError(f"position attachment not uniquely identified: {url}")
            u, doc = candidates[0]
            check_publication(doc["text"], pub)
            change = position_change(doc["text"])
            audit.append(dict(url=url, published_date=pub, kind="position", status="natural_change" if change else "legal_person_only"))
            if change:
                positions.append(dict(**change, known_at=known, published_date=pub, source_url=u))
            continue
        found = False
        for csv_url in csv_urls:
            state = archive.conn.execute("SELECT parsed_path FROM documents WHERE url=?", (csv_url,)).fetchone()
            if (state is None or not state[0]) and not relevant_title:
                audit.append(dict(url=url, published_date=pub, kind="margin",
                                  status="out_of_scope_notice_missing_attachment"))
                continue
            csv_doc = archive.document(csv_url)
            rows = [r for table in csv_doc["tables"] for r in table["rows"]]
            products = [p for p in PRODUCTS if any(r.get("契約代碼") == p for r in rows)]
            if not products:
                continue
            html = archive.document(url)
            effective = margin_effective(html["text"])
            # First explicit new level, never extend the 'before' value backwards
            # beyond an observed announcement to lengthen the sample.
            if not archive.legacy and effective < timestamp(start - timedelta(days=1), "00:00:00"):
                continue
            pdfs = [u for u in children if u.lower().endswith(".pdf")]
            matched = []
            errors = []
            for pdf_url in pdfs:
                doc = archive.document(pdf_url)
                try:
                    check_publication(doc["text"], pub)
                    if margin_effective(doc["text"]) != effective:
                        raise ValueError("HTML/PDF effective date mismatch")
                    values = {p: verified_margin_values(rows, doc["text"], p) for p in products}
                except ValueError as error:
                    errors.append(f"{pdf_url}: {error}")
                    continue
                matched.append((pdf_url, values))
            if len(matched) != 1:
                if not relevant_title and not matched:
                    audit.append(dict(url=url, published_date=pub, kind="margin",
                                      status="out_of_scope_csv_not_bound_to_notice"))
                    continue
                raise ValueError(f"CSV/PDF binding failed: {url}; {errors}")
            pdf_url, values = matched[0]
            for p, six in values.items():
                margins[p].append(dict(product=p, after=list(six[:3]), before=list(six[3:]),
                    effective_at=effective, known_at=known, published_date=pub,
                    source_url=url, csv_url=csv_url, pdf_url=pdf_url))
            found = True
        if relevant_title and timestamp(date.fromisoformat(pub), "23:59:59") >= timestamp(start - timedelta(days=2), "00:00:00"):
            audit.append(dict(url=url, published_date=pub, kind="margin", status="bound_csv_pdf" if found else "requires_duplicate_check"))
    for p in PRODUCTS:
        margins[p] = validate_event_chain(margins[p], ("after", "before"))
    positions = validate_event_chain(positions, ("after", "before"))
    covered_dates = {e["published_date"] for events in margins.values() for e in events}
    for row in audit:
        if row["status"] == "requires_duplicate_check":
            if row["published_date"] not in covered_dates:
                raise ValueError(f"unresolved TX/MTX announcement: {row['url']}")
            # A press-release duplicate is not authority for a numeric event.
            row["status"] = "same_day_notice_duplicate_not_used"
    return margins, positions, audit


def prepare_daily(daily_path: Path, evidence_path: Path, final_path: Path,
                  output: Path, start: date, end: date) -> tuple[pl.DataFrame, dict]:
    """Repair marks by physical identity, keeping the existing fixed-slot ABI."""
    original_manifest = json.loads(daily_path.with_name("manifest.json").read_text())
    parent_sha = sha256_file(daily_path)
    if original_manifest["outputs"]["continuous_daily"]["sha256"] != parent_sha:
        raise ValueError("daily parent release hash mismatch")
    proof_manifest = json.loads(evidence_path.with_name("official_evidence_manifest.json").read_text())
    if proof_manifest["sha256"] != sha256_file(evidence_path):
        raise ValueError("official daily proof hash mismatch")
    # The canonical evidence loader re-verifies the original archive sources.
    from stockagent.data.tw_stock_futures_carry import _load_carry_official_evidence
    evidence = _load_carry_official_evidence(evidence_path)
    hashes = {item["sha256"] for item in proof_manifest["sources"]}
    if not set(evidence["official_source_sha256"].drop_nulls()) <= hashes:
        raise ValueError("daily settlement row references an unverified official archive")
    final_manifest = json.loads(final_path.with_name("manifest.json").read_text())
    if final_manifest["outputs"]["futures_final_settlement_history"]["sha256"] != sha256_file(final_path):
        raise ValueError("final settlement source hash mismatch")
    raw = pl.read_parquet(daily_path)
    original_columns = raw.columns
    frame = raw.filter(pl.col("product").is_in(PRODUCTS) & pl.col("contract").str.contains(r"^\d{6}$")
                       & (pl.col("date") >= evidence["date"].min()) & (pl.col("date") <= end))
    frame = frame.join(evidence.select("date", "physical_contract", "official_settlement", "official_source_sha256"),
                       on=["date", "physical_contract"], how="left", validate="1:1")
    final = pl.read_parquet(final_path).select(
        pl.col("settlement_date").alias("date"), "product", "contract",
        pl.col("final_settlement_price").alias("_official_final"))
    frame = frame.join(final, on=["date", "product", "contract"], how="left", validate="m:1")
    frame = frame.with_columns(
        pl.when(pl.col("liquidation_reason") == "last_trade_date")
        .then(pl.col("_official_final")).otherwise(pl.col("official_settlement").cast(pl.Float64, strict=False))
        .alias("_verified_settlement"))
    # A file boundary is not a broker liquidation instruction. Keep the open
    # account marked at its final observed official settlement, with residual
    # contracts auditable. Otherwise a capacity-limited, solvent position is
    # spuriously assigned the executor's absorbing failure return at EOF.
    boundary = pl.col("liquidation_reason") == "dataset_terminal"
    legal_expiry = pl.col("date") == pl.col("resolved_last_trade_date")
    if frame.filter(boundary & (pl.col("date") != end)).height:
        raise ValueError("source terminal boundary does not match selected end")
    frame = frame.with_columns(
        pl.when(boundary & legal_expiry).then(pl.lit("last_trade_date"))
        .when(boundary).then(pl.lit("dataset_boundary_mark_only"))
        .otherwise(pl.col("liquidation_reason")).alias("liquidation_reason"),
        pl.when(boundary).then(legal_expiry).otherwise(pl.col("must_liquidate")).alias("must_liquidate"),
        pl.when(boundary & legal_expiry).then(pl.col("_official_final"))
        .otherwise(pl.col("_verified_settlement")).alias("_verified_settlement"))
    if frame.filter(pl.col("_verified_settlement").is_null() | ~pl.col("_verified_settlement").is_finite()
                    | (pl.col("_verified_settlement") <= 0)).height:
        raise ValueError("missing same-day official daily/final settlement")
    repaired = frame.filter(pl.col("settlement") != pl.col("_verified_settlement")).height
    frame = frame.with_columns(pl.col("_verified_settlement").alias("settlement")).sort("physical_contract", "date")
    frame = frame.with_columns(*[
        pl.col(column).shift(1).over("physical_contract").alias(name)
        for column, name in (("settlement", "previous_settlement"), ("date", "previous_symbol_date"),
                             ("product", "previous_product"), ("contract", "previous_contract"),
                             ("volume", "previous_volume"), ("open_interest", "previous_open_interest"))])
    same = (pl.col("previous_symbol_date") == pl.col("previous_market_date")).fill_null(False)
    # On a proven no-print day the previous official settlement is an opening
    # valuation only, with executable=False and zero capacity. Never a fill.
    frame = frame.with_columns(
        pl.when(~pl.col("source_row_observed")).then(pl.col("previous_settlement"))
        .otherwise(pl.col("open")).alias("open"),
        pl.when(~pl.col("source_row_observed")).then(pl.col("settlement"))
        .otherwise(pl.col("close")).alias("close"),
        same.alias("same_contract_as_previous_session"), (~same).alias("lifecycle_reset"),
        pl.when(same).then((pl.col("settlement") / pl.col("previous_settlement")).log())
        .otherwise(None).alias("taifex_settlement_logret_1d"))
    frame = frame.with_columns(
        pl.col("open").shift(-1).over("physical_contract").alias("next_open"),
        pl.col("settlement").alias("valuation_settlement"), pl.col("open").alias("valuation_open"))
    frame = frame.with_columns(pl.when(pl.col("can_hold_overnight"))
        .then((pl.col("next_open") / pl.col("open")).log())
        .otherwise((pl.col("close") / pl.col("open")).log()).alias("holding_log_return"))
    scoped = frame.filter(pl.col("date") >= start)
    excluded = scoped.filter(~pl.col("same_contract_as_previous_session"))
    # Only first physical observations may be excluded. An intermediate hole
    # would strand held positions and must fail rather than shorten a trajectory.
    if excluded.filter(pl.col("previous_symbol_date").is_not_null()).height:
        raise ValueError("intermediate physical-contract calendar gap")
    scoped = scoped.filter(pl.col("same_contract_as_previous_session"))
    if scoped.filter((~pl.col("source_row_observed")) &
                     (pl.col("executable") | (pl.col("volume") > 0))).height:
        raise ValueError("a no-print valuation became an executable quote")
    if scoped.select("date", "symbol").is_duplicated().any():
        raise ValueError("physical scope collides on a fixed output slot")
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(output / "excluded_first_observations.parquet",
                         excluded.select("date", "physical_contract", "previous_symbol_date"))
    outpath = output / "continuous_daily.parquet"
    scoped = scoped.select(original_columns).sort("date", "symbol")
    atomic_write_parquet(outpath, scoped)
    summary = dict(products=list(PRODUCTS), contract_series="monthly_only", start=str(scoped["date"].min()),
        end=str(scoped["date"].max()), rows=scoped.height, physical_contracts=scoped["physical_contract"].n_unique(),
        trading_dates=scoped["date"].n_unique(), excluded_first_observations=excluded.height,
        official_settlement_replacements=repaired, source_daily_sha256=parent_sha,
        no_print_valuation_days=scoped.filter(~pl.col("source_row_observed")).height,
        snapshot_boundary_mark_only_rows=scoped.filter(pl.col("liquidation_reason") == "dataset_boundary_mark_only").height,
        selection_reason="verified_historical_rules_and_prior_same_physical_official_settlement_not_performance")
    manifest = dict(original_manifest, verified_margin_scope=summary,
        outputs={"continuous_daily": {"path": str(outpath), "sha256": sha256_file(outpath), "rows": scoped.height}},
        parent_daily_sha256=parent_sha, official_daily_evidence_sha256=sha256_file(evidence_path),
        official_final_settlement_sha256=sha256_file(final_path))
    manifest.update(scope="verified_TX_MTX_monthly_margin_research", rows=scoped.height,
        products=2, fixed_fee_research_products=2, date_start=summary["start"], date_end=summary["end"],
        observed_transaction_rows=scoped.filter(pl.col("source_row_observed")).height,
        carry_forward_valuation_rows=summary["no_print_valuation_days"], weekly_observed_rows=0,
        logical_symbols=scoped["symbol"].n_unique(),
        maximum_simultaneous_active_contracts=scoped.group_by("date").len()["len"].max(),
        maximum_simultaneous_executable_contracts=scoped.filter(pl.col("executable")).group_by("date").len()["len"].max(),
        execution_contract="prior_completed_features_regular_open_adjustments_official_daily_and_final_settlement_marks",
        fixed_fee_contract="whole_contract_margin_account_fee_40_TWD_per_side_plus_dated_transaction_tax")
    manifest["mandatory_liquidation_boundary"] = "legal_expiry_and_account_risk_only_dataset_EOF_is_official_mark_with_open_positions"
    atomic_write_json(output / "manifest.json", manifest)
    exclusions = raw.group_by("product").agg(pl.len().alias("available_contract_days"),
        pl.col("date").min().alias("available_start"), pl.col("date").max().alias("available_end"))
    exclusions = exclusions.join(scoped.group_by("product").agg(pl.len().alias("selected_contract_days")),
                                  on="product", how="left").with_columns(pl.col("selected_contract_days").fill_null(0))
    exclusions.sort("product").write_csv(output / "all_product_scope.csv")
    return scoped, summary


def index_margin_corporate_execution_rules(frame, rules):
    """Express a verified, unchanged-unit TX/MTX release in carry schema 2.

    This is an algebraic regression fixture for the canonical full trainer,
    not an admission shortcut for equities, commodities or adjusted products.
    The caller must first validate the complete schema-1 release and retain
    that release's source receipts. No additional observations are created.
    """
    from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate
    if set(frame['product'].unique()) - {'TX', 'MTX'}:
        raise ValueError('plain index carry conversion supports verified TX/MTX only')
    if frame.filter(((pl.col('product') == 'TX') & (pl.col('contract_multiplier') != 200.))
                    | ((pl.col('product') == 'MTX') & (pl.col('contract_multiplier') != 50.))).height:
        raise ValueError('index carry conversion requires unchanged historical contract units')
    if set(rules['margin_kind'].unique()) != {'fixed_twd'}:
        raise ValueError('index carry conversion requires fixed TWD margins')
    keys = ['date', 'physical_contract']
    if frame.select(keys).is_duplicated().any() or rules.select(keys).is_duplicated().any():
        raise ValueError('duplicate index contract-days')
    extra = frame.select(*keys, 'open', 'close', 'settlement', 'previous_settlement',
                         'contract_multiplier', 'liquidation_reason')
    joined = rules.join(extra, on=keys, how='left', validate='1:1').sort('physical_contract', 'date')
    if joined.height != frame.height or joined['open'].null_count():
        raise ValueError('rules and index daily dates differ')
    expiry = pl.col('liquidation_reason') == 'last_trade_date'
    joined = joined.with_columns(
        pl.lit('08:45:00').alias('opening_time'),
        (pl.col('open') * pl.col('contract_multiplier')).alias('opening_contract_value_twd'),
        (pl.col('settlement') * pl.col('contract_multiplier')).alias('settlement_contract_value_twd'),
        (pl.when(expiry).then(pl.col('settlement')).otherwise(pl.col('close'))
         * pl.col('contract_multiplier')).alias('terminal_contract_value_twd'),
        (pl.col('previous_settlement') * pl.col('contract_multiplier')).alias('known_margin_contract_value_twd'),
        pl.when(expiry).then(pl.lit('cash_settlement')).otherwise(pl.lit('mark_only')).alias('terminal_event'),
        pl.col('date').shift(1).over('physical_contract').alias('carry_from_date'),
        pl.col('physical_contract').shift(1).over('physical_contract').fill_null('').alias('carry_from_physical_contract'),
        pl.lit(1).alias('carry_quantity_numerator'), pl.lit(1).alias('carry_quantity_denominator'),
        pl.lit(0.).alias('carry_cash_twd'),
        pl.col('settlement_initial').shift(1).over('physical_contract').alias('carry_previous_initial_twd'),
        pl.col('settlement_maintenance').shift(1).over('physical_contract').alias('carry_previous_maintenance_twd'),
    ).with_columns(
        pl.col('settlement_contract_value_twd').shift(1).over('physical_contract').alias('carry_previous_value_twd'),
        (pl.col('carry_from_date').cast(pl.String) + pl.lit('T23:59:59+08:00')).alias('carry_known_at'),
        (pl.col('date').cast(pl.String) + pl.lit('T08:45:00+08:00')).alias('carry_effective_at'),
        pl.Series('_tax_rate', [stock_index_futures_tax_rate(d) for d in joined['date']]),
    ).with_columns(
        (pl.col('opening_contract_value_twd') * pl.col('_tax_rate') + .5).floor().alias('opening_tax_twd'),
        (pl.col('terminal_contract_value_twd') * pl.col('_tax_rate') + .5).floor().alias('terminal_tax_twd'),
        (pl.col('known_margin_contract_value_twd') * pl.col('_tax_rate') + .5).floor().alias('known_tax_twd'),
    )
    calendar = frame.select('date').unique().sort('date').with_columns(pl.col('date').shift(1).alias('_previous_date'))
    check = joined.join(calendar, on='date', how='left')
    if check.filter(pl.col('carry_from_date').is_not_null()
                    & (pl.col('carry_from_date') != pl.col('_previous_date'))).height:
        raise ValueError('verified index inventory skips a source account date')
    return joined.drop('open', 'close', 'settlement', 'previous_settlement',
                       'liquidation_reason', '_tax_rate').sort(keys)


def event_at(events: list[dict], cutoff: str) -> dict:
    eligible = [e for e in events if e["known_at"] <= cutoff and e["effective_at"] <= cutoff]
    if not eligible:
        raise ValueError(f"no prior public effective rule at {cutoff}")
    return max(eligible, key=lambda e: e["effective_at"])


def align_rules(frame: pl.DataFrame, margins: dict, positions: list) -> pl.DataFrame:
    from stockagent.data.tw_price_rules import taifex_futures_tick_size_numpy, dated_limit_ratio
    prior = frame["previous_settlement"].to_numpy()
    ticks = taifex_futures_tick_size_numpy(prior, frame["date"].to_numpy(),
        product_codes=frame["product"].to_numpy(), asset_classes=frame["asset_class"].to_numpy())
    if not np.isfinite(prior).all() or not (prior > 0).all() or not (ticks == 1).all():
        raise ValueError("unverified prior reference or TX/MTX dated price grid")
    # Use the historical interval, then retain executable grid points inside it.
    upper = np.floor(prior * dated_limit_ratio(1.10, frame["date"].to_numpy(), prior.shape) / ticks + 1e-8) * ticks
    lower = np.ceil(prior * dated_limit_ratio(.90, frame["date"].to_numpy(), prior.shape) / ticks - 1e-8) * ticks
    rows = []
    for i, row in enumerate(frame.select("date", "physical_contract", "product", "liquidation_reason").to_dicts()):
        day, product = row["date"], row["product"]
        opening = event_at(margins[product], timestamp(day, "08:45:00"))
        clock = "13:30:00" if row["liquidation_reason"] == "last_trade_date" else "13:45:00"
        ending = event_at(margins[product], timestamp(day, clock))
        pos = event_at(positions, timestamp(day, "08:45:00"))
        rows.append(dict(date=day, physical_contract=row["physical_contract"], margin_kind="fixed_twd",
            initial=opening["after"][0], maintenance=opening["after"][1],
            settlement_initial=ending["after"][0], settlement_maintenance=ending["after"][1],
            known_at=max(opening["known_at"], pos["known_at"]),
            effective_at=max(opening["effective_at"], pos["effective_at"]),
            settlement_known_at=ending["known_at"], settlement_effective_at=ending["effective_at"],
            settlement_time=clock, position_group="TAIEX_NATURAL_PERSON", position_unit=1. if product=="TX" else .25,
            position_limit=pos["after"], upper_limit=upper[i], lower_limit=lower[i],
            opening_margin_source_url=opening["source_url"], settlement_margin_source_url=ending["source_url"],
            position_source_url=pos["source_url"]))
    return pl.DataFrame(rows).sort("date", "physical_contract")
