#!/usr/bin/env python3
"""Build source-bound physical calendars/marks before margin-rule admission."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_margin_preparation import (physical_lifetime_calendar,
    load_preparation_final_settlements, derive_adjusted_final_fixings, load_same_security_final_fixing_rule,
    load_adjusted_zero_oi_rule, load_loss_reduction_halt_review, apply_loss_reduction_halt_values)
from stockagent.data.tw_stock_futures_repair import official_day_evidence


def raw_physical_observations(raw: pl.DataFrame, universe: pl.DataFrame) -> pl.DataFrame:
    """Select the declared universe without the old fixed-multiplier filter."""
    if universe['product'].n_unique()!=universe.height:
        raise ValueError('duplicate raw universe product')
    selected=raw.filter(pl.col('session')=='一般').join(
        universe.select('product','asset_class'),on='product',how='inner',validate='m:1')
    if selected.select('date','product','contract').is_duplicated().any():
        raise ValueError('duplicate normalized general-session observations')
    if not selected['contract'].str.contains(r'^\d{6}(?:W[1-5])?$').all():
        raise ValueError('non-outright series in physical observations')
    return selected.select('date','product','contract','asset_class',
        *[c for c in ('open_interest','source_sha256') if c in selected.columns]).with_columns(
        pl.concat_str('product','contract',separator=':').alias('physical_contract'),
        pl.lit(True).alias('source_row_observed'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    source=p.add_mutually_exclusive_group(required=True)
    source.add_argument('--parent-daily',type=Path)
    source.add_argument('--raw-daily',type=Path,
                        help='Use the full normalized source snapshot, including adjusted codes.')
    p.add_argument('--product-universe',type=Path,
                   help='SHA-bound products.csv from the raw universe inventory; required with --raw-daily')
    p.add_argument('--final-settlement',type=Path,required=True)
    p.add_argument('--official-daily-manifest',type=Path,required=True)
    p.add_argument('--corporate-candidates',type=Path,
                   help='SHA-bound candidate events used only to separate reused contract identities')
    p.add_argument('--underlying-fixing-review',type=Path,
                   help='Dated original stock fixing rule; derive a separate missing-adjusted-price input, never an exchange row')
    p.add_argument('--adjusted-lifecycle-review',type=Path,
                   help='Original rule and commencement evidence for zero-open-interest adjusted contract delisting')
    p.add_argument('--loss-reduction-halt-review',type=Path,action='append',default=[],
                   help='Source-bound pure loss-reduction halt: legal nonexpiry values only; no executable prices')
    p.add_argument('--output-dir',type=Path,required=True)
    a=p.parse_args(); started=time.monotonic()
    a.output_dir.mkdir(parents=True,exist_ok=True)
    if (a.output_dir/'manifest.json').exists():
        raise FileExistsError('completed outputs are immutable; select a new directory')
    if a.raw_daily:
        if not a.product_universe:p.error('--raw-daily requires --product-universe')
        manifest=json.loads(a.raw_daily.with_name('manifest.json').read_text())
        if manifest['outputs'][a.raw_daily.name]['sha256']!=sha256_file(a.raw_daily):
            raise ValueError('raw daily SHA mismatch')
        inventory=json.loads(a.product_universe.with_name('manifest.json').read_text())
        if inventory['products_sha256']!=sha256_file(a.product_universe):
            raise ValueError('raw universe SHA mismatch')
        parent=raw_physical_observations(pl.read_parquet(a.raw_daily),pl.read_csv(
            a.product_universe,schema_overrides={'underlying_symbol':pl.String}))
    else:
        manifest=json.loads(a.parent_daily.with_name('manifest.json').read_text())
        if manifest['outputs']['continuous_daily']['sha256']!=sha256_file(a.parent_daily):
            raise ValueError('parent daily SHA mismatch')
        parent=pl.read_parquet(a.parent_daily,columns=['date','product','contract',
            'physical_contract','asset_class','source_row_observed'])
    # Verify price/value and manifest at the canonical reader, then preserve
    # multiple official events for a reused product/month in the preparation.
    final=load_preparation_final_settlements(a.final_settlement)
    mixed_semantics='settlement_method' in final.columns
    corporate=None
    if a.corporate_candidates:
        proof=json.loads(a.corporate_candidates.with_name('manifest.json').read_text())
        if (proof.get('point_in_time_verified') is not False or
                proof.get('outputs',{}).get(a.corporate_candidates.name,{}).get('sha256')!=sha256_file(a.corporate_candidates)):
            raise ValueError('corporate candidate source SHA/scope mismatch')
        corporate=pl.read_parquet(a.corporate_candidates)
    derived=None
    if a.underlying_fixing_review:
        if not a.corporate_candidates or not a.product_universe:
            p.error('underlying fixings require corporate candidates and the explicit universe')
        import shutil
        terms_path=a.corporate_candidates.with_name('corporate_terms_intervals.parquet')
        if proof['outputs'][terms_path.name]['sha256']!=sha256_file(terms_path):
            raise ValueError('derived fixing corporate terms SHA mismatch')
        rule=load_same_security_final_fixing_rule(a.underlying_fixing_review)
        derived=derive_adjusted_final_fixings(parent,final,pl.read_parquet(terms_path),
            pl.read_csv(a.product_universe,infer_schema=False),**rule)
        atomic_write_parquet(a.output_dir/'derived_final_fixings.parquet',derived)
        final=pl.concat([final.with_columns(pl.lit('official_product_final').alias('final_fixing_origin')),
                         derived],how='diagonal_relaxed')
        folder=a.output_dir/'underlying_final_rule';folder.mkdir(exist_ok=True)
        shutil.copyfile(a.underlying_fixing_review,folder/'review.json')
        for item in json.loads(a.underlying_fixing_review.read_text())['sources']:
            target=folder/item['path'];target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(a.underlying_fixing_review.parent/item['path'],target)
    delisting_rule=None
    if a.adjusted_lifecycle_review:
        import shutil
        if not a.raw_daily:p.error('adjusted lifecycle requires raw sourced open interest')
        delisting_rule=load_adjusted_zero_oi_rule(a.adjusted_lifecycle_review)
        folder=a.output_dir/'adjusted_lifecycle_rule';folder.mkdir(exist_ok=True)
        shutil.copyfile(a.adjusted_lifecycle_review,folder/'review.json')
        for item in json.loads(a.adjusted_lifecycle_review.read_text())['sources']:
            target=folder/item['path'];target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(a.adjusted_lifecycle_review.parent/item['path'],target)
    calendar,lives=physical_lifetime_calendar(parent.filter(pl.col('source_row_observed')),
        final,parent.select('date'),corporate=corporate,delisting_rule=delisting_rule)
    atomic_write_parquet(a.output_dir/'physical_lifetimes.parquet',lives)
    atomic_write_parquet(a.output_dir/'physical_calendar.parquet',calendar)
    print(json.dumps(dict(stage='calendar',rows=calendar.height,physical_lives=lives.height,
        statuses=lives.group_by('lifetime_status').len().to_dicts(),elapsed_s=time.monotonic()-started)),flush=True)
    proof=official_day_evidence(a.official_daily_manifest,
        calendar.select('date','physical_contract'),a.output_dir/'official_daily',allow_unreported_volume=True)
    joined=calendar.join(proof,on=['date','physical_contract'],how='left',validate='1:1').with_columns(
        pl.col('official_settlement').str.replace_all(',','').cast(pl.Float64,strict=False).alias('daily_mark'))
    cash_event=(pl.col('date')==pl.col('official_expiry'))
    if mixed_semantics:
        cash_event=cash_event & (pl.col('settlement_method')=='cash_settlement')
    joined=joined.with_columns(
        pl.when(cash_event).then(pl.col('final_settlement_price'))
        .otherwise(pl.col('daily_mark')).alias('valuation_price'),
        cash_event.fill_null(False).alias('cash_settlement'))
    halt_values=None
    if a.loss_reduction_halt_review:
        import shutil
        if corporate is None:p.error('halt value review requires --corporate-candidates')
        all_halt_values=[]
        for i,review_path in enumerate(a.loss_reduction_halt_review):
            if a.output_dir.resolve().is_relative_to(review_path.parent.resolve()):
                raise ValueError('halt review/output directories must not be nested')
            episode=load_loss_reduction_halt_review(review_path)
            own=corporate.filter((pl.col('source_content_sha256')==episode['source_content_sha256'])
                &(pl.col('from_product')==episode['product'])
                &(pl.col('effective_date')==episode['resumption_date']))
            if own.height!=1 or own['contract_multiplier'][0]!=float(episode['after_units']):
                raise ValueError('halt review is not owned by the selected corporate source')
            joined,values=apply_loss_reduction_halt_values(joined,parent.select('date').unique(),episode)
            all_halt_values.append(values)
            folder='loss_reduction_halt_rule' if len(a.loss_reduction_halt_review)==1 else f'loss_reduction_halt_rules/{i}'
            shutil.copytree(review_path.parent,a.output_dir/folder)
        halt_values=pl.concat(all_halt_values,how='vertical')
        atomic_write_parquet(a.output_dir/'legal_halt_valuation_inputs.parquet',halt_values)
    bad=joined.filter(pl.col('valuation_price').is_null()|~pl.col('valuation_price').is_finite()
                      |(pl.col('valuation_price')<=0))
    atomic_write_parquet(a.output_dir/'physical_daily_marks.parquet',joined)
    atomic_write_parquet(a.output_dir/'missing_valuation_rows.parquet',bad)
    derived_cash=(pl.col('cash_settlement') & (pl.col('final_fixing_origin')=='derived_same_security_official_final_fixing')) if derived is not None else pl.lit(False)
    products=parent.select('product','asset_class').unique().join(joined.group_by('product').agg(
        pl.len().alias('calendar_rows'),pl.col('date').min().alias('first_date'),
        pl.col('date').max().alias('last_date'),
        (pl.col('cash_settlement') & ~derived_cash.fill_null(False)).sum().alias('official_expiries'),
        derived_cash.fill_null(False).sum().alias('derived_expiries'),
        ((pl.col('valuation_price')>0)&pl.col('valuation_price').is_finite()).sum().alias('priced_rows')),
        on='product',how='left').sort('product')
    products.write_csv(a.output_dir/'product_physical_coverage.csv')
    result=dict(schema_version=7 if halt_values is not None else (5 if delisting_rule is not None else (4 if derived is not None else (3 if corporate is not None else (2 if mixed_semantics else 1)))),status='prepared_physical_evidence_requires_rule_and_accounting_admission',
        builder_source_sha256=sha256_file(Path(__file__)),
        preparation_source_sha256=sha256_file(Path(__file__).resolve().parents[1]/'stockagent/data/tw_futures_margin_preparation.py'),
        all_products_training_ready=False,created_at_utc=datetime.now(timezone.utc).isoformat(),
        corporate_transitions_verified=False,legal_physical_identities_verified=False,
        final_date_roles_preserved=mixed_semantics,
        cash_settlement_clock_verified=False if mixed_semantics else None,
        derived_final_fixing_events=derived.height if derived is not None else 0,
        underlying_final_fixing_rule_sha256=sha256_file(a.underlying_fixing_review) if a.underlying_fixing_review else None,
        derived_fixing_corporate_terms_sha256=sha256_file(terms_path) if derived is not None else None,
        adjusted_lifecycle_rule_sha256=sha256_file(a.adjusted_lifecycle_review) if a.adjusted_lifecycle_review else None,
        loss_reduction_halt_review_sha256=sha256_file(a.loss_reduction_halt_review[0]) if len(a.loss_reduction_halt_review)==1 else None,
        loss_reduction_halt_review_sha256s=[sha256_file(p) for p in a.loss_reduction_halt_review],
        legal_halt_value_contract='source_bound_loss_reduction_halt_value_v2' if halt_values is not None else None,
        legal_halt_valuation_rows=halt_values.height if halt_values is not None else 0,
        legal_halt_values_are_observed_quotes=False,
        zero_open_interest_delisted_lives=lives.filter(pl.col('lifetime_status')=='zero_open_interest_delisting').height,
        products=products.height,calendar_rows=calendar.height,physical_lives=lives.height,
        missing_valuation_rows=bad.height,elapsed_s=time.monotonic()-started,
        parent_daily_sha256=sha256_file(a.parent_daily) if a.parent_daily else None,
        raw_daily_sha256=sha256_file(a.raw_daily) if a.raw_daily else None,
        product_universe_sha256=sha256_file(a.product_universe) if a.product_universe else None,
        corporate_candidates_sha256=sha256_file(a.corporate_candidates) if a.corporate_candidates else None,
        corporate_identity_generation_applied=corporate is not None,
        corporate_transfer_candidate_lives=(lives.filter(pl.col('lifetime_status')=='corporate_transfer_candidate').height
                                           if corporate is not None else 0),
        final_settlement_sha256=sha256_file(a.final_settlement),
        official_daily_manifest_sha256=sha256_file(a.official_daily_manifest),
        outputs={str(f.relative_to(a.output_dir)):dict(sha256=sha256_file(f),bytes=f.stat().st_size)
            for f in sorted(a.output_dir.rglob('*')) if f.is_file()},
        boundary_policy='official_early_or_regular_settlement_separates_lifetimes; EOF_is_not_liquidation',
        unresolved_lives=lives.filter(pl.col('lifetime_status')=='unresolved_expiry').height)
    atomic_write_json(a.output_dir/'manifest.json',result)
    print(json.dumps({k:v for k,v in result.items() if k!='outputs'}),flush=True)


if __name__=='__main__': main()
