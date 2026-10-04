#!/usr/bin/env python3
"""Bind margin inputs, materialize all TWD contracts, or publish a checked release."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
import time

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_margin import validate_margin_rule_source
from stockagent.data.tw_futures_margin_preparation import (
    RuleArchive, align_rules, align_product_margin_intervals, build_rule_events, compact, prepare_daily,
    bind_equity_margin_families, bind_dated_position_combinations, bind_dated_corporate_terms,
    bind_equity_position_families,
    bind_physical_position_inputs,
    load_unchanged_position_member_scopes,
    bind_adjusted_terminal_values,
)


def prepare_all_twd_margin_inputs(args):
    """Resolve actual contract-day dependencies without promoting candidates.

    Reuse immutable physical evidence and numeric rule intervals. This stage
    neither redownloads/OCRs sources nor starts a GPU job. Its dated output is
    consumed by subsequent product-specification/accounting admission.
    """
    started = time.monotonic()
    required = ('physical_history', 'rule_candidates', 'product_universe')
    if any(getattr(args, key) is None for key in required):
        raise ValueError('all-twd requires --physical-history, --rule-candidates and --product-universe')
    member_path=getattr(args,'position_generation_proof',None)
    if member_path and not args.position_family_review:
        raise ValueError('position generation proof requires a dated position family law')
    out = args.output_dir
    if out.exists() and any(out.iterdir()):
        raise FileExistsError('use a new empty output directory')
    physical = args.physical_history / 'physical_daily_marks.parquet'
    intervals_path = args.rule_candidates / 'margin_level_intervals.parquet'
    sources = {}
    for path, manifest_path, output_key in (
        (physical, args.physical_history / 'manifest.json', physical.name),
        (intervals_path, args.rule_candidates / 'manifest.json', intervals_path.name),
        (args.rule_candidates/'position_level_intervals.parquet', args.rule_candidates/'manifest.json', 'position_level_intervals.parquet'),
        (args.rule_candidates/'corporate_terms_intervals.parquet', args.rule_candidates/'manifest.json', 'corporate_terms_intervals.parquet'),
    ):
        manifest = json.loads(manifest_path.read_text())
        digest = sha256_file(path)
        if manifest.get('outputs', {}).get(output_key, {}).get('sha256') != digest:
            raise ValueError(f'input SHA mismatch: {path}')
        sources[str(path)] = digest
        sources[str(manifest_path)] = sha256_file(manifest_path)
    universe_manifest = args.product_universe.with_name('manifest.json')
    if json.loads(universe_manifest.read_text()).get('products_sha256') != sha256_file(args.product_universe):
        raise ValueError('universe SHA mismatch')
    sources[str(args.product_universe)] = sha256_file(args.product_universe)
    sources[str(universe_manifest)] = sha256_file(universe_manifest)
    universe = pl.read_csv(args.product_universe, infer_schema=False)
    if universe['product'].n_unique() != universe.height or not universe['settlement_currency'].eq('TWD').all():
        raise ValueError('all-twd requires a unique explicit TWD universe')
    physical_query = pl.scan_parquet(physical).select('date', 'product', 'contract', 'physical_instance',
        'valuation_price', 'outright_volume', 'lifetime_status','official_expiry',
        'final_settlement_price','final_settlement_value')
    if args.start:
        physical_query = physical_query.filter(pl.col('date') >= args.start)
    if args.end:
        physical_query = physical_query.filter(pl.col('date') <= args.end)
    frame = physical_query.collect()
    if frame.is_empty():
        raise ValueError('empty selected physical history')
    if not set(frame['product'].unique()) <= set(universe['product']):
        raise ValueError('physical history includes a product outside the TWD universe')
    intervals = pl.read_parquet(intervals_path)
    if args.equity_margin_family_review:
        import gzip,hashlib
        review=json.loads(args.equity_margin_family_review.read_text())
        parent=json.loads((args.rule_candidates/'manifest.json').read_text())
        proof=review['source_content_sha256']
        raw=[s for s in parent['sources'] if s['url']==review['source_url'] and s['kind']=='raw_gzip']
        texts=[s for s in parent['sources'] if s['url']==review['source_url'] and s['path'].endswith('.txt')]
        if not raw or not texts:raise ValueError('same-security margin rule source missing')
        for s in raw+texts:
            path=args.rule_candidates/s['path']
            if sha256_file(path)!=s['sha256']:raise ValueError('margin family source SHA mismatch')
            sources[str(path)]=s['sha256']
        if not any(hashlib.sha256(gzip.decompress((args.rule_candidates/s['path']).read_bytes())).hexdigest()==proof for s in raw):
            raise ValueError('margin family review does not bind the retained source')
        phrase='同標的證券之股票期貨契約適用相同保證金所屬級距及適用比例'
        if not any(phrase in compact((args.rule_candidates/s['path']).read_text()) for s in texts):
            raise ValueError('same-security margin source clause missing')
        sources[str(args.equity_margin_family_review)]=sha256_file(args.equity_margin_family_review)
        aligned=bind_equity_margin_families(frame.select('date','product'),intervals,universe,
            rule_effective_date=date.fromisoformat(review['effective_date']),
            rule_known_at=review['known_at'],rule_source_sha256=proof)
    else:
        aligned = align_product_margin_intervals(frame.select('date', 'product'), intervals)
    adjusted=frame.filter(pl.col('product').str.contains(r'\d$'))
    corporate_unit_levels=pl.read_parquet(args.rule_candidates/'corporate_terms_intervals.parquet')
    terms=bind_dated_corporate_terms(adjusted.select('date','product','contract'),corporate_unit_levels)
    units=terms
    unit_path=args.rule_candidates/'corporate_unit_intervals.parquet'
    if unit_path.exists():
        parent=json.loads((args.rule_candidates/'manifest.json').read_text())
        digest=sha256_file(unit_path)
        if parent.get('outputs',{}).get(unit_path.name,{}).get('sha256')!=digest:
            raise ValueError('corporate position-unit input SHA mismatch')
        sources[str(unit_path)]=digest
        corporate_unit_levels=pl.read_parquet(unit_path)
        units=bind_dated_corporate_terms(adjusted.select('date','product','contract'),
            corporate_unit_levels,unit_only=True)
    position_intervals=pl.read_parquet(args.rule_candidates/'position_level_intervals.parquet')
    if args.position_family_review:
        import gzip,hashlib
        review=json.loads(args.position_family_review.read_text())
        if review.get('review_kind')!='source_bound_position_family_rules':
            raise ValueError('unsupported dated position family review')
        raw_hashes=set()
        for source in review['sources']:
            path=args.position_family_review.parent/source['path']
            if not path.resolve().is_relative_to(args.position_family_review.parent.resolve()):
                raise ValueError('unsafe position family source path')
            if sha256_file(path)!=source['sha256']:raise ValueError('position family source SHA mismatch')
            sources[str(path)]=source['sha256']
            if source['kind']=='raw_gzip':raw_hashes.add(hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest())
        if any(law[key] not in raw_hashes for law in review['rules']
               for key in ('source_content_sha256','effectiveness_source_content_sha256')):
            raise ValueError('position family law has no retained original')
        sources[str(args.position_family_review)]=sha256_file(args.position_family_review)
        position=bind_equity_position_families(frame.select('date','product'),position_intervals,
            universe,units,review['rules'])
    else:
        position=bind_dated_position_combinations(frame.select('date','product'),position_intervals)
    physical_position=None
    member_scopes=None
    if member_path:
        member_scopes,member_sources=load_unchanged_position_member_scopes(member_path)
        sources.update(member_sources)
    if args.position_family_review:
        physical_position=bind_physical_position_inputs(frame.select('date','product','contract'),
            position,units,universe,review['rules'],corporate_unit_intervals=corporate_unit_levels,
            unchanged_member_scopes=member_scopes)
    rights=None
    if args.terminal_subscription_values:
        receipt=json.loads(args.terminal_subscription_values.with_name('manifest.json').read_text())
        digest=sha256_file(args.terminal_subscription_values)
        if receipt['outputs'][args.terminal_subscription_values.name]['sha256']!=digest:
            raise ValueError('terminal subscription component SHA mismatch')
        if receipt.get('schema_version')==2:
            for source in receipt['sources']:
                parent=args.terminal_subscription_values.parent/source['path']
                if not parent.resolve().is_relative_to(args.terminal_subscription_values.parent.resolve()):
                    raise ValueError('unsafe terminal source path')
                if sha256_file(parent)!=source['sha256']:raise ValueError('terminal component parent source SHA mismatch')
                sources[str(parent)]=source['sha256']
        else:
            for relative,expected in receipt['source_sha256s'].items():
                if sha256_file(Path(relative))!=expected:raise ValueError('terminal component parent source SHA mismatch')
        sources[str(args.terminal_subscription_values)]=digest
        sources[str(args.terminal_subscription_values.with_name('manifest.json'))]=sha256_file(args.terminal_subscription_values.with_name('manifest.json'))
        rights=pl.read_parquet(args.terminal_subscription_values)
    finals=adjusted.filter(pl.col('date')==pl.col('official_expiry')).select(
        'date','product','contract','final_settlement_price','final_settlement_value').unique()
    terminal=bind_adjusted_terminal_values(finals,
        pl.read_parquet(args.rule_candidates/'corporate_terms_intervals.parquet'),rights)
    annotated = frame.join(aligned.select('date', 'product', 'opening_binding_status',
        'settlement_binding_status'), on=['date', 'product'], how='left', validate='m:1')
    accepted=['bound_prior_publication','bound_same_security_rate']
    bound = (pl.col('opening_binding_status').is_in(accepted)
             & pl.col('settlement_binding_status').is_in(accepted))
    direct=((pl.col('opening_binding_status')=='bound_prior_publication')
            & (pl.col('settlement_binding_status')=='bound_prior_publication'))
    coverage = annotated.group_by('product').agg(
        pl.len().alias('physical_contract_days'),
        pl.col('date').min().alias('selected_start'), pl.col('date').max().alias('selected_end'),
        direct.sum().alias('direct_margin_bound_contract_days'),
        bound.sum().alias('margin_bound_contract_days'),
        ((pl.col('outright_volume') > 0) & bound).sum().alias('positive_volume_margin_bound_days'),
        (pl.col('valuation_price').is_null() | ~pl.col('valuation_price').is_finite()
         | (pl.col('valuation_price') <= 0)).sum().alias('missing_valuation_contract_days'),
    )
    coverage = universe.select('product', 'product_name', 'asset_class').join(
        coverage, on='product', how='left', validate='1:1').with_columns(
            pl.col('physical_contract_days', 'direct_margin_bound_contract_days', 'margin_bound_contract_days',
                'positive_volume_margin_bound_days', 'missing_valuation_contract_days').fill_null(0),
            pl.lit(False).alias('training_admitted'),
        ).sort('product')
    position_days=(physical_position.group_by('date','product').agg(
        pl.col('position_numeric_inputs_resolved').any()) if physical_position is not None else position)
    dependencies=coverage.join(position_days.group_by('product').agg(
        pl.col('position_numeric_inputs_resolved').sum().alias('position_bound_product_days')),
        on='product',how='left').join(terms.group_by('product').agg(
        (pl.col('terms_binding_status')=='bound_prior_publication').sum().alias('adjusted_terms_bound_contract_days')),
        on='product',how='left').join(terminal.group_by('product').agg(
        pl.len().alias('official_terminal_events'),
        pl.col('terminal_value_input_twd').is_not_null().sum().alias('resolved_terminal_value_inputs')),
        on='product',how='left').join(frame.group_by('product').agg(
        (pl.col('outright_volume')>0).sum().alias('positive_volume_contract_days')),
        on='product',how='left').with_columns(
            pl.col('position_bound_product_days','adjusted_terms_bound_contract_days',
                'official_terminal_events','resolved_terminal_value_inputs').fill_null(0),
            pl.lit('requires_dated_execution_release_and_full_training_acceptance').alias('training_status'))
    # Count unresolved intervals where the model would actually see a physical
    # contract. Candidate-event counts alone cannot establish their impact.
    gaps = annotated.filter(~bound).group_by('product', 'opening_binding_status',
        'settlement_binding_status').agg(pl.len().alias('physical_contract_days'),
        pl.col('date').min().alias('first_date'), pl.col('date').max().alias('last_date'),
        (pl.col('outright_volume') > 0).sum().alias('positive_volume_days')).sort(
            'physical_contract_days', descending=True)
    out.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(out / 'product_day_margin_inputs.parquet', aligned)
    atomic_write_parquet(out / 'product_day_position_inputs.parquet', position)
    if physical_position is not None:
        atomic_write_parquet(out / 'contract_day_position_inputs.parquet', physical_position)
    atomic_write_parquet(out / 'adjusted_contract_day_terms.parquet', terms)
    atomic_write_parquet(out / 'adjusted_contract_day_units.parquet', units.select(
        'date','product','contract','terms_binding_status','contract_multiplier','known_at','source_content_sha256s'))
    atomic_write_parquet(out / 'adjusted_terminal_value_inputs.parquet', terminal)
    coverage.write_csv(out / 'product_margin_coverage.csv')
    dependencies.write_csv(out / 'product_accounting_input_coverage.csv')
    gaps.write_csv(out / 'margin_binding_gaps.csv')
    outputs = {path.name: dict(sha256=sha256_file(path), bytes=path.stat().st_size)
               for path in sorted(out.iterdir()) if path.is_file()}
    summary = dict(dataset='taifex_all_twd_margin_inputs', schema_version=5 if member_scopes is not None else 4,
        position_source_scope_contract='direct_corporate_securities_cap_verified_member_v4'
            if member_scopes is not None else 'direct_corporate_securities_cap_original_month_v2',
        status='margin_inputs_bound_requires_product_accounting_admission',
        point_in_time_verified=False, all_products_training_ready=False,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        products=coverage.height, products_observed=frame['product'].n_unique(),
        physical_contract_days=frame.height, product_days=aligned.height,
        direct_margin_bound_contract_days=int(annotated.select(direct.sum()).item()),
        margin_bound_contract_days=int(annotated.select(bound.sum()).item()),
        products_with_direct_margin_bound_days=coverage.filter(pl.col('direct_margin_bound_contract_days') > 0).height,
        products_with_margin_bound_days=coverage.filter(pl.col('margin_bound_contract_days') > 0).height,
        position_input_rows=position.height, adjusted_terms_input_rows=terms.height,
        products_with_numeric_position_inputs=dependencies.filter(pl.col('position_bound_product_days')>0).height,
        adjusted_products_with_dated_terms=terms.filter(pl.col('terms_binding_status')=='bound_prior_publication')['product'].n_unique(),
        adjusted_terminal_events=terminal.height,
        adjusted_terminal_events_with_value=terminal['terminal_value_input_twd'].is_not_null().sum(),
        selected_start=str(frame['date'].min()), selected_end=str(frame['date'].max()),
        source_sha256s=sources, outputs=outputs, elapsed_s=time.monotonic()-started,
        builder_sha256=sha256_file(Path(__file__)),
        preparation_sha256=sha256_file(REPO_ROOT / 'stockagent/data/tw_futures_margin_preparation.py'),
        unresolved_contracts=['dated_product_specifications_and_clocks',
            'margin_inheritance_for_adjusted_and_mini_contracts', 'dated_combined_position_limits',
            'corporate_deliverables_and_cash_transfers', 'terminal_accounting_and_valuation'],
        no_implicit_admission=True)
    atomic_write_json(out / 'manifest.json', summary)
    print(json.dumps({k: v for k, v in summary.items() if k not in ('source_sha256s', 'outputs')}, indent=2))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', choices=['verified-tx-mtx', 'all-twd'], default='verified-tx-mtx')
    parser.add_argument('--stage', choices=['inputs', 'materialize', 'terms', 'release'], default='inputs',
                        help='all-twd: join rule inputs, compile daily rows, or publish admitted execution terms')
    parser.add_argument('--raw-daily', type=Path)
    parser.add_argument('--margin-inputs', type=Path)
    parser.add_argument('--materialization', type=Path)
    parser.add_argument('--execution-terms', type=Path)
    parser.add_argument('--specifications', type=Path,
                        help='Source-bound dated product specifications; missing values remain explicit blockers.')
    parser.add_argument('--slot-count', type=int, default=2816)
    parser.add_argument('--physical-history', type=Path)
    parser.add_argument('--rule-candidates', type=Path)
    parser.add_argument('--product-universe', type=Path)
    parser.add_argument('--equity-margin-family-review',type=Path,
                        help='Source-bound dated same-security stock margin rate rule; never substitutes fixed ETF amounts.')
    parser.add_argument('--terminal-subscription-values',type=Path,
                        help='Source-bound subscription terminal components; never inserted into daily cash/features.')
    parser.add_argument('--position-family-review',type=Path,
                        help='Dated same-security position laws; changed shares still require an explicit cap.')
    parser.add_argument('--position-generation-proof',type=Path,
                        help='Immutable source-verified physical member scopes; never supplies a numeric cap.')
    parser.add_argument("--archive", type=Path, default=Path("data_taifex_public_history/rules"))
    parser.add_argument("--daily", type=Path)
    parser.add_argument("--official-evidence", type=Path)
    parser.add_argument("--final-settlement", type=Path)
    parser.add_argument("--source-reviews", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args()
    if args.scope == 'all-twd':
        if args.stage == 'inputs':
            prepare_all_twd_margin_inputs(args)
        elif args.stage == 'materialize':
            from stockagent.data.tw_futures_margin_release import materialize_all_twd_margin_inputs
            required = ('physical_history', 'raw_daily', 'product_universe', 'margin_inputs')
            if any(getattr(args, key) is None for key in required):
                parser.error('materialize requires --physical-history, --raw-daily, --product-universe and --margin-inputs')
            if args.start or args.end:
                parser.error('full TWD materialization cannot silently truncate requested history')
            summary = materialize_all_twd_margin_inputs(physical_history=args.physical_history,
                raw_daily=args.raw_daily, product_universe=args.product_universe,
                margin_inputs=args.margin_inputs, output=args.output_dir, slot_count=args.slot_count)
            print(json.dumps({k: v for k, v in summary.items()
                              if k not in ('source_sha256s', 'outputs', 'requested_products')}, indent=2))
        elif args.stage == 'terms':
            from stockagent.data.tw_futures_margin_release import build_all_twd_execution_terms
            if any(getattr(args, key) is None for key in ('materialization', 'margin_inputs', 'rule_candidates')):
                parser.error('terms requires --materialization, --margin-inputs and --rule-candidates')
            if args.start or args.end:
                parser.error('full TWD terms cannot silently truncate requested history')
            result = build_all_twd_execution_terms(materialization=args.materialization,
                margin_inputs=args.margin_inputs, rule_candidates=args.rule_candidates,
                specifications=args.specifications, output=args.output_dir)
            print(json.dumps({k: v for k, v in result.items() if k not in ('outputs', 'sources', 'source_sha256s', 'requested_products')}, indent=2))
            if result['status'] != 'complete':
                raise SystemExit(2)
        else:
            from stockagent.data.tw_futures_margin_release import publish_all_twd_margin_release
            if any(getattr(args, key) is None for key in ('materialization', 'execution_terms', 'final_settlement')):
                parser.error('release requires --materialization, --execution-terms and --final-settlement')
            print(json.dumps(publish_all_twd_margin_release(materialization=args.materialization,
                execution_terms=args.execution_terms, final_settlement=args.final_settlement,
                output=args.output_dir), indent=2))
        return
    if args.stage != 'inputs':
        parser.error('--stage applies to --scope all-twd only')
    if any(getattr(args, key) is None for key in ('daily', 'official_evidence', 'final_settlement', 'source_reviews')):
        parser.error('verified-tx-mtx requires --daily, --official-evidence, --final-settlement and --source-reviews')
    args.start = args.start or date(2021, 5, 20)
    args.end = args.end or date(2026, 9, 4)
    rules_dir = args.output_dir / "rules"
    if (rules_dir / "manifest.json").exists():
        raise FileExistsError("use a new output directory; completed releases are immutable")
    archive = RuleArchive(args.archive, rules_dir, args.source_reviews)
    margins, positions, audit = build_rule_events(archive, args.start, args.end)
    # The 2017 dated contract amendment establishes the common session clock,
    # +/-10% reference and combined 4:1 limit. Only the TX/MTX pages (11-24)
    # are relied on; the archive's 80-page extraction limit is not waived.
    specs_url = "https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/網站公告(5).pdf"
    specs = archive.document(specs_url)
    text = compact(specs["text"])
    for phrase in ("上午八時四十五分至下午一時四十五分", "四比一", "上下各百分之十"):
        if phrase not in text:
            raise ValueError(f"historical contract evidence lost: {phrase}")
    implementation_url = "https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/1060418上線新聞稿_修正NCP時間.pdf"
    implementation = archive.document(implementation_url)
    if "5月15日" not in compact(implementation["text"]):
        raise ValueError("historical session implementation date missing")
    frame, summary = prepare_daily(args.daily, args.official_evidence, args.final_settlement,
                                    args.output_dir / "daily", args.start, args.end)
    rules = align_rules(frame, margins, positions)
    daily = args.output_dir / "daily/continuous_daily.parquet"
    rules_path = rules_dir / "rules.parquet"
    atomic_write_parquet(rules_path, rules)
    events_path = rules_dir / "events.json"
    events = dict(margins=margins, positions=positions, announcement_audit=audit)
    atomic_write_json(events_path, events)
    # Include portable derivative proofs. Raw daily source bytes are verified
    # locally by the canonical loader and remain in the original source store.
    for path, kind in ((args.official_evidence, "verified_official_daily_rows"),
                       (args.official_evidence.with_name("official_evidence_manifest.json"), "daily_source_verification"),
                       (args.final_settlement.with_name("manifest.json"), "final_settlement_manifest"),
                       (events_path, "verified_rule_events"),
                       (Path("stockagent/data/tw_price_rules.py"), "dated_price_grid_implementation"),
                       (Path("docs/tw_futures_price_grid_evidence_2026-09-27.md"), "dated_price_grid_evidence")):
        archive.copy(path, sha256_file(path), url="", kind=kind)
    manifest = dict(dataset="taifex_futures_margin_rules", schema_version=1,
        status="complete", point_in_time_verified=True,
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        source_daily_sha256=sha256_file(daily), outputs={"rules": {"sha256": sha256_file(rules_path)}},
        sources=list(archive.sources.values()), scope=summary,
        historical_spec_source=specs_url, historical_spec_pages_one_based=[11, 24],
        historical_spec_content_sha256=specs["content_sha256"],
        session_implementation_source=implementation_url,
        rules_known_at_policy="publication_date_235959_upper_bound_with_hash_bound_official_date_confirmation",
        legacy_rule_review=bool(archive.legacy),
        delayed_position_relaxation_policy="retain_previous_stricter_cap_until_publication_day_end; never defer tightening",
        price_limit_policy="TX_MTX_7_percent_before_2015_06_01_then_10_percent",
        close_effective_policy="post_regular_close_account_phase_1345_expiry_cash_settlement_1330",
        position_policy="natural_person_absolute_gross_TX_equivalent_conservative_no_offsets",
        zero_print_policy="official_marks_for_valuation_only_no_new_fills",
        omissions=["all_other_products", "weekly_contracts", "before_" + summary["start"], "first_observation_without_prior_physical_settlement"],
        not_verified=["intraday_and_night_margin_calls", "broker_specific_surcharges_and_notification_times", "SPAN_offsets"],
        builder_sha256=sha256_file(Path("stockagent/data/tw_futures_margin_preparation.py")))
    atomic_write_json(rules_dir / "manifest.json", manifest)
    try:
        validate_margin_rule_source(rules_path, daily)
    except Exception:
        atomic_write_json(rules_dir / "manifest.json", dict(manifest, status="failed", point_in_time_verified=False))
        raise
    summary.update(margin_event_counts={p: len(e) for p, e in margins.items()},
                   position_events=len(positions), official_source_files=len(archive.sources),
                   validated_rules=rules.height, status="data_prepared_remote_training_not_yet_verified")
    atomic_write_json(args.output_dir / "build_summary.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
