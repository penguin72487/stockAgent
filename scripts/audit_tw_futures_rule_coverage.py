#!/usr/bin/env python3
"""Audit the full requested futures universe without promoting extracted text.

Only a hash-validated canonical margin release counts as verified contract-days.
Document mentions and final settlements are independent evidence inventories.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_margin import validate_margin_rule_source
from scripts.download_taifex_rule_history import read_verified_raw


def coverage_table(daily: pl.DataFrame, verified: pl.DataFrame, final: pl.DataFrame) -> pl.DataFrame:
    """Join physical identities and dates; a current product snapshot is no proof."""
    keys = ["date", "physical_contract"]
    if verified.select(keys).is_duplicated().any():
        raise ValueError("duplicate verified physical contract-days")
    admitted = daily.join(verified.select(keys).with_columns(pl.lit(True).alias("verified")),
                          on=keys, how="left", validate="m:1")
    products = admitted.group_by("product", "asset_class").agg(
        pl.col("product_name").first().alias("product_name"),
        pl.col("date").min().alias("source_start"), pl.col("date").max().alias("source_end"),
        pl.len().alias("source_contract_days"),
        pl.col("physical_contract").n_unique().alias("physical_contracts"),
        pl.col("date").n_unique().alias("observed_dates"),
        pl.col("verified").fill_null(False).sum().alias("verified_contract_days"),
        pl.col("date").filter(pl.col("verified").fill_null(False)).min().alias("verified_start"),
        pl.col("date").filter(pl.col("verified").fill_null(False)).max().alias("verified_end"),
        pl.col("contract_multiplier").n_unique().alias("observed_multiplier_values"),
    )
    fk = ["settlement_date", "product", "contract"]
    if final.select(fk).is_duplicated().any():
        raise ValueError("duplicate official settlement keys")
    terminal = daily.filter(pl.col("liquidation_reason") == "last_trade_date").join(
        final.select(fk + ["final_settlement_price"]).rename({"settlement_date": "date"}),
        on=["date", "product", "contract"], how="left", validate="m:1")
    expiry = terminal.group_by("product").agg(
        pl.len().alias("parent_expiry_rows"),
        pl.col("final_settlement_price").is_not_null().sum().alias("official_expiry_matches"),
        pl.col("final_settlement_price").is_null().sum().alias("unmatched_parent_expiry_rows"),
        ((pl.col("settlement") - pl.col("final_settlement_price")).abs() > 1e-8)
        .fill_null(False).sum().alias("parent_expiry_prices_needing_repair"),
    )
    return products.join(expiry, on="product", how="left", validate="1:1").with_columns(
        pl.col("product").str.contains(r"\d$").alias("adjusted_product_code"),
        (pl.col("source_contract_days") - pl.col("verified_contract_days")).alias("unverified_contract_days"),
        pl.when(pl.col("verified_contract_days") > 0).then(pl.lit("verified_subset_only"))
        .otherwise(pl.lit("historical_rules_not_yet_verified")).alias("training_admission"),
    ).sort("asset_class", "product")


def expiry_worklist(daily: pl.DataFrame, final: pl.DataFrame) -> pl.DataFrame:
    """Preserve full exchange contract value; never infer an adjusted multiplier."""
    return daily.filter(pl.col('liquidation_reason') == 'last_trade_date').select(
        'date', 'product', 'contract', 'physical_contract', 'asset_class',
        'settlement', 'contract_multiplier', 'resolved_last_trade_date',
    ).join(final.rename({'settlement_date': 'date'}),
           on=['date', 'product', 'contract'], how='left', validate='m:1').with_columns(
        (pl.col('final_settlement_value') - pl.col('final_settlement_price') * pl.col('contract_multiplier'))
        .alias('official_value_minus_parent_price_times_multiplier'),
        pl.when(pl.col('final_settlement_price').is_null()).then(pl.lit('resolve_official_expiry_date_and_identity'))
        .when((pl.col('settlement') - pl.col('final_settlement_price')).abs() > 1e-8)
        .then(pl.lit('replace_expiry_mark_with_official_value_in_validated_builder'))
        .otherwise(pl.lit('matched_price_still_requires_historical_contract_rules')).alias('next_action'),
    ).sort('product', 'date', 'contract')


def corporate_final_value_diagnostics(terms: pl.DataFrame, final: pl.DataFrame) -> pl.DataFrame:
    """Independent terminal-value checks catch OCR and reused-code mistakes.

    A one-TWD rounding envelope is a diagnostic tolerance, not a historical
    rounding rule. Neither matching outcomes nor this audit may infer a past
    multiplier, fill absent full values, or promote an executable release.
    """
    keys=['product','contract']
    if terms.select(*keys,'effective_date').is_duplicated().any():
        raise ValueError('duplicate corporate term boundaries')
    if final.select(*keys,'settlement_date').is_duplicated().any():
        raise ValueError('duplicate official settlement event')
    dated=terms.with_columns(pl.col('effective_date').cast(pl.Date),
                            pl.col('valid_until_exclusive').cast(pl.Date))
    dated=dated.with_columns((pl.col('fixed_subscription_rights_twd').fill_null(0.)
        if 'fixed_subscription_rights_twd' in dated.columns else pl.lit(0.)).alias('fixed_subscription_rights_twd'))
    selected=final.filter(pl.col('product').str.contains(r'\d$'))
    joined=selected.sort('settlement_date').join_asof(dated.sort('effective_date'),
        left_on='settlement_date',right_on='effective_date',by=keys,
        strategy='backward',check_sortedness=False)
    joined=joined.with_columns((pl.col('final_settlement_price')*pl.col('contract_multiplier')+
        pl.col('deliverable_cash_twd')+pl.col('fixed_subscription_rights_twd')).alias('candidate_formula_value_twd'))
    joined=joined.with_columns((pl.col('final_settlement_value')-pl.col('candidate_formula_value_twd'))
        .alias('official_minus_candidate_twd'))
    return joined.with_columns(
        pl.when(pl.col('effective_date').is_null()).then(pl.lit('no_candidate_term'))
        .when(pl.col('valid_until_exclusive').is_not_null() &
              (pl.col('settlement_date')>=pl.col('valid_until_exclusive')))
        .then(pl.lit('candidate_term_ended_before_expiry'))
        .when(pl.col('final_settlement_value').is_null()).then(pl.lit('official_full_value_missing'))
        .when(pl.col('subscription_rights_at_final_settlement')).then(pl.lit('rights_value_requires_separate_evidence'))
        .when(~pl.col('candidate_formula_value_twd').is_finite() | (pl.col('candidate_formula_value_twd')<=0))
        .then(pl.lit('candidate_value_invalid'))
        .when((pl.col('official_minus_candidate_twd') < -1.001) | (pl.col('official_minus_candidate_twd') > .001))
        .then(pl.lit('economic_value_conflict'))
        .otherwise(pl.lit('within_rounding_envelope_not_rule_approval')).alias('diagnostic_status'),
        pl.lit(False).alias('training_admitted')).sort('product','contract','settlement_date')


def expand_raw_universe(coverage: pl.DataFrame, raw: pl.DataFrame,
                        events: pl.DataFrame | None = None) -> pl.DataFrame:
    """The old model's supported-code filter must not define 'all products'."""
    if raw['product'].is_duplicated().any():raise ValueError('duplicate raw universe product')
    if set(coverage['product'])-set(raw['product']):raise ValueError('raw universe loses parent products')
    outer=raw.select('product','product_name','asset_class',
        pl.col('source_start').alias('raw_start'),pl.col('source_end').alias('raw_end'),
        pl.col('source_rows').alias('raw_session_rows'),'needs_dated_deliverables',
        *[name for name in ('settlement_currency','currency_evidence_key',
                            'currency_source_url','currency_source_content_sha256') if name in raw.columns])
    combined=outer.join(coverage.drop('product_name','asset_class'),on='product',how='left',validate='1:1')
    combined=combined.with_columns(pl.col('source_contract_days').is_not_null().alias('in_original_parent'),
        pl.col('verified_contract_days').fill_null(0),
        pl.col('adjusted_product_code').fill_null(pl.col('needs_dated_deliverables')),
        pl.col('training_admission').fill_null('missing_from_parent_requires_physical_and_rule_preparation'))
    if events is not None and events.height:
        facts=events.group_by('product').agg(pl.len().alias('candidate_margin_events'),
            (pl.col('issue_date_bound').fill_null(False)&pl.col('chronological').fill_null(False))
            .sum().alias('dated_candidate_margin_events'),
            pl.col('requires_reversion_review').fill_null(False).sum().alias('candidate_temporary_margin_events'))
        combined=combined.join(facts,on='product',how='left',validate='1:1').with_columns(
            pl.col('candidate_margin_events','dated_candidate_margin_events','candidate_temporary_margin_events').fill_null(0))
    return combined.sort('asset_class','product')


def margin_event_worklist(events: pl.DataFrame) -> pl.DataFrame:
    """Review event clocks and before/after links without backdating a level.

    An equal pair is useful evidence, not proof that no intervening notice was
    missed. Unknown and temporary events interrupt continuity checking.
    """
    rows=[];previous={};seen=set()
    for row in events.sort('product','effective_date','known_at',nulls_last=True).to_dicts():
        identity=(row['product'],row['source_content_sha256'],row.get('effective_date'),
                  row['margin_kind'],tuple(row.get('after') or []),tuple(row.get('before') or []))
        if identity in seen:
            continue
        seen.add(identity)
        reasons=[]
        if not row.get('issue_date_bound'): reasons.append('publication_not_bound')
        if not row.get('chronological'): reasons.append('effective_clock_not_after_known_date')
        if row.get('effective_phase') not in ('product_regular_open','new_contract_listing',
                                               'after_product_regular_close'):
            reasons.append('session_phase_unresolved')
        if row.get('requires_reversion_review'): reasons.append('conditional_or_reversion_review')
        if row['margin_kind'] not in ('notional_rate','fixed_twd'): reasons.append('dated_currency_required')
        key=(row['product'],row['margin_kind'])
        prior=previous.get(key)
        if (not reasons and prior and prior['effective_date']==row['effective_date']
                and prior.get('effective_phase')==row.get('effective_phase')
                and tuple(prior['after'])!=tuple(row.get('after') or [])):
            reasons.append('same_boundary_conflicting_values')
        if (not reasons and prior and row.get('before') is not None
                and prior['effective_date']!=row['effective_date']
                and tuple(prior['after'])!=tuple(row['before'])):
            reasons.append('before_after_chain_disagrees')
        if reasons:
            rows.append(dict(product=row['product'],published_date=row['published_date'],
                effective_date=row.get('effective_date'),effective_phase=row.get('effective_phase'),
                source_url=row['source_url'],source_content_sha256=row['source_content_sha256'],
                reasons=';'.join(reasons),before=json.dumps(row.get('before')),
                after=json.dumps(row.get('after')),prior_source_url=prior['source_url'] if prior else None,
                prior_after=json.dumps(prior['after']) if prior else None,training_admitted=False))
        # Never carry an old candidate past an unresolved intervening event.
        if reasons: previous.pop(key,None)
        else: previous[key]=row
    return pl.DataFrame(rows,infer_schema_length=None) if rows else pl.DataFrame(schema={
        'product':pl.String,'reasons':pl.String,'training_admitted':pl.Boolean})


def prepared_scope_coverage(universe, lives, missing, tables):
    """One output row per requested code, including inactive and unpriced ones."""
    if universe['product'].is_duplicated().any():
        raise ValueError('duplicate prepared product universe')
    if universe['settlement_currency'].null_count() or set(universe['settlement_currency'])!={'TWD'}:
        raise ValueError('prepared scope requires explicit TWD currency evidence')
    if set(lives['product'])!=set(universe['product']):
        raise ValueError('physical evidence differs from requested product universe')
    result=universe.select('product','product_name','asset_class','settlement_currency',
        'underlying_symbol','source_start','source_end','source_rows','needs_dated_deliverables')
    physical=lives.group_by('product').agg(pl.len().alias('observed_physical_lives'),
        (pl.col('lifetime_status')=='unresolved_expiry').sum().alias('unresolved_lives'),
        (pl.col('settlement_method')=='physical_delivery').sum().alias('physical_delivery_lives'))
    result=result.join(physical,on='product',validate='1:1').join(
        missing.group_by('product').len().rename({'len':'missing_valuation_rows'}),on='product',how='left')
    for kind,table in tables.items():
        counts=table.group_by('product').len().rename({'len':'candidate_'+kind+'_events'})
        result=result.join(counts,on='product',how='left',validate='1:1')
    return result.with_columns(
        pl.col('missing_valuation_rows',*[f'candidate_{k}_events' for k in tables]).fill_null(0),
        pl.lit(False).alias('complete_rule_chain_verified'),
        pl.lit(False).alias('all_product_accounting_verified'),
        pl.lit('prepared_evidence_requires_dated_rule_and_accounting_release').alias('training_admission'),
    ).sort('asset_class','product')


def audit_prepared_scope(a):
    if not all((a.raw_universe,a.candidate_events,a.daily,a.final_settlement,a.official_daily_manifest)):
        raise ValueError('physical audit requires raw universe, candidate events, daily, final settlement and raw receipts')
    evidence={};sources_checked=0
    def manifest(root):
        path=root/'manifest.json';m=json.loads(path.read_text());evidence[str(path)]=sha256_file(path)
        for relative,proof in m.get('outputs',{}).items():
            f=root/relative
            if not f.resolve().is_relative_to(root.resolve()) or sha256_file(f)!=proof['sha256']:
                raise ValueError('prepared output SHA mismatch: '+str(f))
            evidence[str(f)]=proof['sha256']
        return m
    physical=manifest(a.physical_history)
    scope=json.loads(a.raw_universe.with_name('manifest.json').read_text())
    evidence[str(a.raw_universe.with_name('manifest.json'))]=sha256_file(a.raw_universe.with_name('manifest.json'))
    if sha256_file(a.raw_universe)!=scope['products_sha256']:
        raise ValueError('prepared universe SHA mismatch')
    if physical['product_universe_sha256']!=scope['products_sha256']:
        raise ValueError('physical history has a different product universe')
    if physical['raw_daily_sha256']!=sha256_file(a.daily):
        raise ValueError('physical history has a different daily source')
    if (physical['official_daily_manifest_sha256']!=sha256_file(a.official_daily_manifest)
            or physical['final_settlement_sha256']!=sha256_file(a.final_settlement)):
        raise ValueError('physical evidence has different raw/final sources')
    for proof in scope['sources']:
        source=a.raw_universe.parent/proof['path']
        if not source.resolve().is_relative_to(a.raw_universe.parent.resolve()) or sha256_file(source)!=proof['sha256']:
            raise ValueError('universe currency source SHA mismatch')
        sources_checked+=1
    raw_receipts=json.loads(a.official_daily_manifest.read_text())['receipts']
    for proof in raw_receipts:
        source=Path(proof['path'])
        if not source.is_absolute(): source=a.official_daily_manifest.parent/source
        if sha256_file(source)!=proof['sha256']:
            raise ValueError('official daily raw receipt SHA mismatch')
        sources_checked+=1
    from stockagent.data.tw_futures_margin_preparation import load_preparation_final_settlements
    load_preparation_final_settlements(a.final_settlement)
    sources_checked+=len(json.loads(a.final_settlement.with_name('manifest.json').read_text())['receipts'])
    evidence[str(a.final_settlement)]=sha256_file(a.final_settlement)
    evidence[str(a.final_settlement.with_name('manifest.json'))]=sha256_file(a.final_settlement.with_name('manifest.json'))
    evidence[str(a.official_daily_manifest)]=sha256_file(a.official_daily_manifest)
    candidates=manifest(a.candidate_events.parent)
    if candidates.get('point_in_time_verified') is not False:
        raise ValueError('expected unapproved event candidates')
    if physical.get('corporate_candidates_sha256') and physical['corporate_candidates_sha256']!=sha256_file(
            a.candidate_events.with_name('corporate_event_candidates.parquet')):
        raise ValueError('physical identities use a different corporate candidate source')
    for source in candidates['sources']:
        p=a.candidate_events.parent/source['path']
        if not p.resolve().is_relative_to(a.candidate_events.parent.resolve()) or sha256_file(p)!=source['sha256']:
            raise ValueError('candidate raw provenance SHA mismatch')
        sources_checked+=1
    tables={k:pl.read_parquet(a.candidate_events.with_name(k+'_event_candidates.parquet'))
            for k in ('margin','position','corporate')}
    universe=pl.read_csv(a.raw_universe,schema_overrides={'underlying_symbol':pl.String})
    archive_counts={k:t.height for k,t in tables.items()}
    requested=universe['product'].to_list()
    tables={k:t.filter(pl.col('product').is_in(requested)) for k,t in tables.items()}
    lives=pl.read_parquet(a.physical_history/'physical_lifetimes.parquet')
    missing=pl.read_parquet(a.physical_history/'missing_valuation_rows.parquet')
    coverage=prepared_scope_coverage(universe,lives,missing,tables)
    work=margin_event_worklist(tables['margin'])
    a.output_dir.mkdir(parents=True,exist_ok=True)
    coverage.write_csv(a.output_dir/'product_coverage.csv')
    atomic_write_parquet(a.output_dir/'product_coverage.parquet',coverage)
    work.write_csv(a.output_dir/'margin_event_worklist.csv')
    tables['margin'].group_by('effective_phase','issue_date_bound','chronological').len().sort('len',descending=True).write_csv(
        a.output_dir/'margin_clock_coverage.csv')
    corporate=tables['corporate']
    corporate_work=corporate.filter(
        pl.col('contract_months').list.len().fill_null(0).eq(0)
        | pl.col('effective_date').is_null()
        | ~pl.col('issue_date_bound').fill_null(False))
    corporate_work.select('product','source_url','source_content_sha256','published_date',
        'effective_date','issue_date_bound','contract_months_text','contract_months_error',
        'phase_evidence').write_csv(a.output_dir/'corporate_extraction_worklist.csv')
    extraction_quality=dict(
        margin_publication_unbound=tables['margin'].filter(~pl.col('issue_date_bound').fill_null(False)).height,
        position_publication_unbound=tables['position'].filter(~pl.col('issue_date_bound').fill_null(False)).height,
        position_effective_date_missing=tables['position']['effective_date'].null_count(),
        corporate_publication_unbound=corporate.filter(~pl.col('issue_date_bound').fill_null(False)).height,
        corporate_contract_months_missing=corporate.filter(pl.col('contract_months').list.len().fill_null(0).eq(0)).height,
        corporate_effective_date_missing=corporate['effective_date'].null_count(),
        corporate_requires_rights_valuation=corporate['requires_rights_valuation'].fill_null(False).sum(),
        all_products_training_ready=False)
    atomic_write_json(a.output_dir/'rule_extraction_quality.json',extraction_quality)
    terminal_counts={}
    terms_path=a.candidate_events.with_name('corporate_terms_intervals.parquet')
    if terms_path.exists():
        terminal=corporate_final_value_diagnostics(pl.read_parquet(terms_path),
            pl.read_parquet(a.final_settlement).filter(pl.col('product').is_in(requested)))
        atomic_write_parquet(a.output_dir/'corporate_terminal_value_diagnostics.parquet',terminal)
        terminal_counts=dict(terminal.group_by('diagnostic_status').len().iter_rows())
        terminal.select('product','contract','settlement_date','diagnostic_status',
            'contract_multiplier','deliverable_cash_twd','candidate_formula_value_twd',
            'final_settlement_price','final_settlement_value','official_minus_candidate_twd',
            pl.col('source_content_sha256s').list.join(';'),'training_admitted').write_csv(
            a.output_dir/'corporate_terminal_value_diagnostics.csv')
    standard=coverage.filter(~pl.col('needs_dated_deliverables'))
    summary=dict(schema_version=2,status='prepared_evidence_requires_dated_rule_and_accounting_release',
        all_products_training_ready=False,point_in_time_verified=False,
        created_at_utc=datetime.now(timezone.utc).isoformat(),settlement_currency='TWD',
        products=coverage.height,standard_products=standard.height,
        adjusted_products=coverage.filter(pl.col('needs_dated_deliverables')).height,
        source_start=coverage['source_start'].min(),source_end=coverage['source_end'].max(),
        raw_session_rows=coverage['source_rows'].sum(),calendar_rows=physical['calendar_rows'],
        observed_physical_lives=lives.height,unresolved_lives=physical['unresolved_lives'],
        missing_valuation_rows=missing.height,source_files_verified=sources_checked,
        standard_products_without_direct_margin_facts=standard.filter(pl.col('candidate_margin_events')==0)['product'].to_list(),
        products_without_direct_position_facts=coverage.filter(pl.col('candidate_position_events')==0).height,
        products_without_direct_margin_facts=coverage.filter(pl.col('candidate_margin_events')==0).height,
        margin_event_review_rows=work.height,
        corporate_terminal_value_diagnostics=terminal_counts,
        corporate_identity_generation_applied=physical.get('corporate_identity_generation_applied',False),
        corporate_transfer_candidate_lives=physical.get('corporate_transfer_candidate_lives',0),
        candidate_counts={k:t.height for k,t in tables.items()},
        extraction_quality=extraction_quality,
        source_archive_candidate_counts=archive_counts,
        notes=['A direct candidate is not a verified date chain.',
               'Missing direct facts can require a dated same-underlying inheritance rule.',
               'Physical delivery and corporate quantity/cash transitions need the matching account ABI.',
               'Existing TX/MTX releases are separate; this audit does not approve a new training release.'],
        evidence_sha256={**evidence,str(a.raw_universe):sha256_file(a.raw_universe),str(a.daily):sha256_file(a.daily)},
        outputs={p.name:{'sha256':sha256_file(p)} for p in a.output_dir.iterdir() if p.is_file()})
    atomic_write_json(a.output_dir/'manifest.json',summary)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('outputs','evidence_sha256')},ensure_ascii=False))
    return 0


def document_inventory(root: Path, products: set[str], reviews: Path | None = None) -> tuple[list, list, list, dict]:
    conn = sqlite3.connect(f"file:{root / 'state/queue.sqlite3'}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("BEGIN")
    announcements = [dict(r) for r in conn.execute("SELECT * FROM announcements ORDER BY published_date,url")]
    by_url = {r["url"]: r for r in announcements}
    parents = defaultdict(list)
    for row in conn.execute("SELECT parent,child FROM links"):
        if row["parent"] in by_url:
            parents[row["child"]].append(by_url[row["parent"]])
    documents, mentions, unresolved = [], [], []
    for record in conn.execute("SELECT * FROM documents ORDER BY url"):
        r = dict(record)
        associated = parents[r["url"]] + ([by_url[r["url"]]] if r["url"] in by_url else [])
        categories = sorted({a["category"] for a in associated})
        item = dict(url=r["url"], state=r["state"], parsing_status=r["parsing_status"],
                    parser_version=r["parser_version"], content_sha256=r["content_sha256"],
                    raw_path=r["raw_path"], raw_sha256=r["raw_sha256"], parsed_path=r["parsed_path"],
                    categories="|".join(categories),
                    announcement_dates="|".join(sorted({a["published_date"] for a in associated})),
                    candidate_only=True, verified_rule=False)
        parsed = {}
        try:
            if r["raw_path"]:
                read_verified_raw(root, r)
                item["raw_integrity"] = "verified"
            else:
                item["raw_integrity"] = "not_captured"
            if r["parsed_path"]:
                version = conn.execute("SELECT parsed_sha256 FROM parse_versions WHERE url=? AND content_sha256=? AND parser_version=?",
                                       (r["url"], r["content_sha256"], r["parser_version"])).fetchone()
                path = root / r["parsed_path"]
                if version is None or sha256_file(path) != version["parsed_sha256"]:
                    raise ValueError("parsed_evidence_hash_mismatch")
                parsed = json.loads(path.read_text())
                if parsed["content_sha256"] != r["content_sha256"]:
                    raise ValueError("parsed_raw_source_binding_mismatch")
                item["parsed_sha256"] = version["parsed_sha256"]
        except (ValueError, OSError) as exc:
            item["raw_integrity"] = "failed"
            item["error"] = str(exc)
            parsed = {}
        text = parsed.get("text", "")
        item['review_extraction_status']='not_reviewed'
        if reviews and r['content_sha256']:
            dest=reviews/'documents'/r['content_sha256']
            if (dest/'receipt.json').exists():
                review=json.loads((dest/'receipt.json').read_text())
                if review['content_sha256']!=r['content_sha256']:raise ValueError('review/raw SHA mismatch')
                for f in review['files']:
                    relative=Path(f['path'])
                    if relative.is_absolute() or '..' in relative.parts:raise ValueError('unsafe review path')
                    if sha256_file(dest/relative)!=f['sha256']:raise ValueError('review file SHA mismatch')
                item['review_extraction_status']=review['status']
                if review['status']=='complete':text=(dest/'candidate.txt').read_text()
        explicit = set(re.findall(r"(?<![A-Z0-9])[A-Z][A-Z0-9]{1,2}(?![A-Z0-9])", text)) & products
        for product in sorted(explicit):
            mentions.append(dict(product=product, url=r["url"], content_sha256=r["content_sha256"],
                                 categories=item["categories"], announcement_dates=item["announcement_dates"],
                                 candidate_only=True))
        publications = {t["date_iso"] for t in parsed.get("temporal_mentions", []) if t["role"] == "publication"}
        if publications and associated and not publications.intersection(a["published_date"] for a in associated):
            item["publication_review"] = "attachment_date_disagrees_with_index"
        else:
            item["publication_review"] = "requires_semantic_review"
        item["explicit_product_mentions"] = "|".join(sorted(explicit))
        documents.append(item)
        if (item["raw_integrity"] != "verified" or
                (r["parsing_status"] != 'parsed' and item['review_extraction_status']!='complete') or
                item["publication_review"] == "attachment_date_disagrees_with_index"):
            unresolved.append(item)
    counts = dict(announcements=len(announcements), documents=len(documents),
                  parsing_status_counts=dict(Counter(r["parsing_status"] or "missing" for r in documents)),
                  raw_integrity_counts=dict(Counter(r["raw_integrity"] for r in documents)),
                  review_extraction_counts=dict(Counter(r['review_extraction_status'] for r in documents)),
                  index_windows=[dict(r) for r in conn.execute('SELECT "start","end",rows FROM index_windows ORDER BY "start"')])
    conn.close()
    return documents, mentions, unresolved, counts


def audit_numeric_rules(root: Path, output: Path) -> int:
    """Check numeric semantics without building any contract-day account.

    Thresholds nominate source review; they do not overwrite or approve facts.
    Candidate errors and a suspicious number in an interval are separate counts.
    """
    import math
    from scripts.repair_tw_futures_margin_source_intervals import read_bound_output
    from scripts.build_tw_futures_margin_event_candidates import corporate_numeric_anomalies,reviewed_small_corporate_units
    names=('corporate_event_candidates','position_event_candidates','margin_event_candidates',
           'corporate_unit_intervals','corporate_terms_intervals','position_level_intervals','margin_level_intervals')
    frames={name:read_bound_output(root/(name+'.parquet'))[0] for name in names}
    inputs={name:sha256_file(root/(name+'.parquet')) for name in names}
    corporate=frames['corporate_event_candidates'].to_dicts()
    # A matching native/inspected multiplier AND deliverable supports a true
    # small unit. A same-value pair of old OCR readings alone does not.
    small_native=reviewed_small_corporate_units(corporate)
    diagnostics=[]
    for row in corporate_numeric_anomalies(corporate):
        key=(row['product'],row['effective_date'],row['contract_multiplier'],row['source_content_sha256'])
        supported=key in small_native and row['contract_multiplier']==row['deliverable_security_quantity']
        diagnostics.append(dict(row,table='corporate_event_candidates',field='contract_multiplier_or_deliverable',
            value=row['contract_multiplier'],in_interval=False,
            review_status='small_native_or_inspected_unit' if supported else 'source_review_required'))
    def check(name,row,field,reason):
        amount=row.get(field)
        if amount is None:return
        digests=row.get('source_content_sha256s') or [row.get('source_content_sha256')]
        supported=(field in ('contract_multiplier','position_unit') and any((row.get('product'),row.get('effective_date'),amount,d)
                    in small_native for d in digests))
        diagnostics.append(dict(table=name,product=row.get('product'),effective_date=row.get('effective_date'),
            contract_months=[row['contract']] if row.get('contract') else None,
            source_url=row.get('source_url'),source_content_sha256=digests[0] if digests else None,
            field=field,value=amount,unit=row.get('unit') or row.get('margin_kind'),reasons=[reason],
            in_interval=name.endswith('intervals'),
            review_status='small_native_or_inspected_unit' if supported else 'source_review_required'))
    def suspicious(v):return isinstance(v,(int,float)) and (not math.isfinite(v) or v<=0 or v<100)
    for name in ('corporate_unit_intervals','corporate_terms_intervals'):
        for row in frames[name].iter_rows(named=True):
            if suspicious(row.get('contract_multiplier')):check(name,row,'contract_multiplier','small_or_invalid_contract_unit')
    for name in ('position_event_candidates','position_level_intervals'):
        fields=('natural_person_limit','natural_person_monthly_limit','contract_multiplier') if name.endswith('candidates') else ('position_limit','monthly_position_limit','independent_contract_limit')
        for row in frames[name].iter_rows(named=True):
            for field in fields:
                if suspicious(row.get(field)):check(name,row,field,'small_or_invalid_position_count_or_security_cap')
            if (row.get('event_type')=='corporate_securities_unit_limit'
                    and suspicious(row.get('position_unit'))):
                check(name,row,'position_unit','small_or_invalid_actual_security_conversion_unit')
    for name in ('margin_event_candidates','margin_level_intervals'):
        for row in frames[name].iter_rows(named=True):
            kind=row.get('margin_kind')
            if name.endswith('candidates'):
                vectors=[(side,row.get(side)) for side in ('before','after') if row.get(side)]
            else:vectors=[('level',[row.get(k) for k in ('initial','maintenance','clearing')])]
            for side,values in vectors:
                valid=all(isinstance(v,(int,float)) and math.isfinite(v) and v>0 for v in values)
                ordered=valid and len(values)==3 and values[0]>=values[1]>=values[2]
                for i,value in enumerate(values):
                    invalid=(not valid or not ordered or (kind=='notional_rate' and value>1)
                             or (kind in ('fixed_twd','fixed_usd') and value<100))
                    if invalid:check(name,dict(row,**{side+'_'+str(i):value}),side+'_'+str(i),'margin_amount_unit_or_order_review')
    unresolved=[r for r in diagnostics if r['review_status']=='source_review_required']
    interval=[r for r in unresolved if r['in_interval']]
    output.mkdir(parents=True,exist_ok=True)
    atomic_write_json(output/'numeric_rule_diagnostics.json',dict(
        diagnostics=diagnostics,threshold_is_diagnostic_only=True,financial_values_inferred=False))
    if diagnostics:
        atomic_write_parquet(output/'numeric_rule_diagnostics.parquet',pl.from_dicts(diagnostics,infer_schema_length=None))
    manifest=dict(schema_version=1,contract='source_bound_numeric_rule_audit_v1',
        status='numeric_intervals_source_review_required' if interval else 'numeric_intervals_checked_candidates_preserved',
        input_manifest_sha256=sha256_file(root/'manifest.json'),inputs=inputs,
        input_rows={name:frame.height for name,frame in frames.items()},
        diagnostic_rows=len(diagnostics),source_review_candidate_rows=len(unresolved)-len(interval),
        source_review_interval_rows=len(interval),supported_small_value_rows=len(diagnostics)-len(unresolved),
        threshold_is_diagnostic_only=True,new_ocr_jobs=0,new_accounting_builds=0,
        training_release_approved=False,builder_sha256=sha256_file(Path(__file__)),
        outputs={p.name:dict(sha256=sha256_file(p)) for p in output.iterdir() if p.name.startswith('numeric_rule_diagnostics.')})
    if inputs!={name:sha256_file(root/(name+'.parquet')) for name in names}:
        raise ValueError('numeric audit input changed during reading')
    atomic_write_json(output/'manifest.json',manifest)
    print(json.dumps({k:manifest[k] for k in ('status','diagnostic_rows','source_review_candidate_rows',
        'source_review_interval_rows','supported_small_value_rows')},ensure_ascii=False))
    return 2 if interval else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--daily", type=Path)
    p.add_argument("--rules", type=Path)
    p.add_argument("--verified-daily", type=Path)
    p.add_argument("--final-settlement", type=Path)
    p.add_argument('--official-daily-manifest',type=Path,help='Source receipts for a prepared physical audit')
    p.add_argument('--physical-history',type=Path,help='Audit a full prepared physical-evidence bundle')
    p.add_argument("--archive", type=Path, default=Path("data_taifex_public_history/rules"))
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument('--raw-universe',type=Path,help='SHA-bound complete raw product inventory, including excluded model codes')
    p.add_argument('--candidate-events',type=Path)
    p.add_argument('--reviews',type=Path)
    p.add_argument('--numeric-rules',type=Path,help='Audit bound candidate/interval numeric values without an accounting rebuild')
    a = p.parse_args()
    if (a.output_dir / "manifest.json").exists():
        raise FileExistsError("use a new immutable audit output directory")
    if a.numeric_rules:
        return audit_numeric_rules(a.numeric_rules,a.output_dir)
    if a.physical_history:
        return audit_prepared_scope(a)
    if not all((a.daily,a.rules,a.verified_daily,a.final_settlement)):
        p.error('legacy release audit requires --daily --rules --verified-daily --final-settlement')
    rules, rule_manifest = validate_margin_rule_source(a.rules, a.verified_daily)
    parent_sha = sha256_file(a.daily)
    daily_manifest = json.loads(a.verified_daily.with_name('manifest.json').read_text())
    if daily_manifest.get('source_daily_sha256') != parent_sha:
        # The canonical preparer records the parent digest in its build summary.
        summary = json.loads((a.verified_daily.parent.parent / 'build_summary.json').read_text())
        if summary['source_daily_sha256'] != parent_sha:
            raise ValueError('verified subset is from a different full-universe parent')
    daily = pl.read_parquet(a.daily)
    final_manifest = json.loads(a.final_settlement.with_name('manifest.json').read_text())
    if final_manifest['outputs']['futures_final_settlement_history']['sha256'] != sha256_file(a.final_settlement):
        raise ValueError('official final-settlement output hash mismatch')
    locations=a.final_settlement.with_name('portable_location_map.json')
    mapped={}
    if locations.exists():
        for entry in json.loads(locations.read_text())['entries']:
            rel=Path(entry['path'])
            if rel.is_absolute() or '..' in rel.parts:raise ValueError('unsafe portable source path')
            mapped[(entry['original_path'],entry['sha256'])]=a.final_settlement.parent/rel
    for receipt in final_manifest['receipts']:
        source=Path(receipt['path'])
        if not source.exists():source=mapped.get((receipt['path'],receipt['sha256']),source)
        if sha256_file(source) != receipt['sha256']:
            raise ValueError('official final-settlement raw receipt mismatch')
    final = pl.read_parquet(a.final_settlement)
    coverage = coverage_table(daily, rules, final)
    extra_inputs={}
    events=None
    if a.candidate_events:
        cm=json.loads(a.candidate_events.with_name('manifest.json').read_text())
        if cm['outputs'][a.candidate_events.name]['sha256']!=sha256_file(a.candidate_events):
            raise ValueError('candidate event SHA mismatch')
        events=pl.read_parquet(a.candidate_events)
        extra_inputs[str(a.candidate_events)]=sha256_file(a.candidate_events)
    if a.raw_universe:
        rm=json.loads(a.raw_universe.with_name('manifest.json').read_text())
        if rm['products_sha256']!=sha256_file(a.raw_universe):raise ValueError('raw universe SHA mismatch')
        raw=pl.read_csv(a.raw_universe,schema_overrides={'underlying_symbol':pl.String})
        coverage=expand_raw_universe(coverage,raw,events)
        extra_inputs[str(a.raw_universe)]=sha256_file(a.raw_universe)
    # New candidate bundles can expose independent position/corporate tables.
    # These counts are evidence-work coverage, never execution admission.
    other_counts={}
    if a.candidate_events:
        for kind in ('position','corporate'):
            candidate=a.candidate_events.with_name(kind+'_event_candidates.parquet')
            if not candidate.exists():continue
            if cm['outputs'][candidate.name]['sha256']!=sha256_file(candidate):
                raise ValueError(kind+' candidate SHA mismatch')
            table=pl.read_parquet(candidate)
            counts_name='candidate_'+kind+'_events'
            counts_frame=table.group_by('product').len().rename({'len':counts_name})
            coverage=coverage.join(counts_frame,on='product',how='left').with_columns(
                pl.col(counts_name).fill_null(0))
            other_counts['products_with_'+counts_name]=coverage.filter(pl.col(counts_name)>0).height
            extra_inputs[str(candidate)]=sha256_file(candidate)
    documents, mentions, unresolved, counts = document_inventory(a.archive, set(coverage['product']),a.reviews)
    mention_count = Counter(m['product'] for m in mentions)
    rows = coverage.to_dicts()
    for row in rows:
        row['candidate_document_mentions'] = mention_count[row['product']]
        row['unverified_rule_requirements'] = (
            'margin_amount_or_ratio_and_temporary_surcharges;known_at_and_effective_session;'
            'combined_position_caps;dated_multiplier_tick_limits_and_session;'
            'contract_adjustment_cash_and_quantity;expiry_and_suspension_lifecycle'
            if row['unverified_contract_days'] is None or row['unverified_contract_days']>0 else '')
    coverage = pl.DataFrame(rows)
    a.output_dir.mkdir(parents=True, exist_ok=True)
    coverage.write_csv(a.output_dir / 'product_coverage.csv')
    atomic_write_parquet(a.output_dir / 'product_coverage.parquet', coverage)
    expiry = expiry_worklist(daily, final)
    expiry.write_csv(a.output_dir / 'expiry_worklist.csv')
    for name, content in [('documents', documents), ('product_document_candidates', mentions),
                          ('document_worklist', unresolved)]:
        frame = pl.DataFrame(content, infer_schema_length=None)
        frame.write_csv(a.output_dir / f'{name}.csv')
    # This is an audit, deliberately never named the executable margin manifest.
    manifest = dict(schema_version=1, status='partial', all_products_training_ready=False,
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        scope=(rm.get('scope') if a.raw_universe else None) or
            'all observed index, ETF and stock futures product codes, including adjusted and delisted codes',
        settlement_currency=rm.get('settlement_currency') if a.raw_universe else None,
        scope_version=rm.get('scope_version') if a.raw_universe else None,
        products=coverage.height, source_contract_days=daily.height,
        parent_products=daily['product'].n_unique(),
        raw_session_rows=int(coverage['raw_session_rows'].sum()) if a.raw_universe else None,
        raw_start=str(coverage['raw_start'].min()) if a.raw_universe else None,
        raw_end=str(coverage['raw_end'].max()) if a.raw_universe else None,
        products_missing_from_parent=coverage.filter(~pl.col('in_original_parent')).height if a.raw_universe else 0,
        products_with_candidate_margin_events=coverage.filter(pl.col('candidate_margin_events')>0).height
            if a.raw_universe and events is not None and events.height else None,
        **other_counts,
        source_start=str(daily['date'].min()), source_end=str(daily['date'].max()),
        verified_products=coverage.filter(pl.col('verified_contract_days') > 0)['product'].to_list(),
        verified_contract_days=int(coverage['verified_contract_days'].sum()),
        unverified_contract_days=int(coverage['unverified_contract_days'].sum()),
        source_daily_sha256=parent_sha, verified_rule_scope=rule_manifest['scope'],
        expiry_rows=expiry.height,
        unmatched_parent_expiry_rows=expiry['final_settlement_price'].null_count(),
        parent_expiry_prices_needing_repair=expiry.filter(pl.col('next_action') == 'replace_expiry_mark_with_official_value_in_validated_builder').height,
        nonstandard_final_contract_value_rows=expiry.filter(pl.col('official_value_minus_parent_price_times_multiplier').abs() > .01).height,
        archive=counts, parser_mentions_are_not_verified_rules=True,
        inputs={**{str(path):sha256_file(path) for path in [a.daily,a.rules,a.verified_daily,a.final_settlement]},**extra_inputs},
        outputs={p.name:sha256_file(p) for p in a.output_dir.iterdir() if p.is_file()})
    atomic_write_json(a.output_dir / 'manifest.json', manifest)
    print(json.dumps({k:v for k,v in manifest.items() if k not in {'inputs','outputs','archive'}},ensure_ascii=False,indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
