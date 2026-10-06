#!/usr/bin/env python3
"""Complete selected-feature inventory plus exact native-grain gap worklists.

This audits a fixed source release, not every NULL in a date/security cube.
It uses the existing verified union receipts to avoid re-downloading filled
keys. Provider rechecks are finite queue intents, not fabricated observations.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources
from stockagent.data.tw_expected_observation_gaps import CONTRACT, interior_period_gaps, annotate_gap_candidates, mandatory_trading_price_gaps
from stockagent.data.tw_public_cross_source_fill import MAPPINGS, normalize_finmind, missing_only


def json_blocks(path):
    return [json.loads(p.split("\n```", 1)[0]) for p in path.read_text().split("```json\n")[1:]]


def audit(source, report, out, *, start, as_of, fresh_source_unions=False, priority_status=None):
    if out.exists():
        raise FileExistsError("use a new immutable audit directory")
    manifest = verify_sources(source)
    source_sha = sha256(source / "source_manifest.json")
    owner_checks={}
    if priority_status is not None:
        status=json.loads(priority_status.read_text())
        status_csv=Path(status['status_csv'])
        if sha256(status_csv)!=status['status_csv_sha256']:
            raise ValueError('priority check status CSV changed')
        records=pl.read_csv(status_csv).to_dicts()
        if len(records)!=status['total_intents'] or len({r['dataset'] for r in records})!=len(records):
            raise ValueError('incomplete or duplicate owner check status')
        owner_checks={r['dataset']:r for r in records}
    proof = json.loads((report / "report_receipt.json").read_text())
    definitions = [d for p in sorted([*report.glob("stock_*.md"), *report.glob("market_*.md")]) for d in json_blocks(p)]
    if len(definitions) != proof["selected_quantities"] or len({d['feature'] for d in definitions}) != len(definitions):
        raise ValueError("incomplete or duplicated selected-feature report")
    aliases = json_blocks(report / "aliases.md")
    accepted_sources = {a['source'] for a in aliases if a.get('comparison', {}).get('accepted') is True}
    coverage = {r['feature']: r for r in pl.read_csv(report.parent / 'feature_coverage.csv').to_dicts()}
    out.mkdir(parents=True); (out / 'gaps').mkdir(); (out / 'features').mkdir()
    native = {}
    for adapter in manifest.get('native_adapters', []):
        if adapter['kind'] != 'finmind_financial_facts':
            continue
        for dataset in sorted({r['dataset'] for r in adapter['members']}):
            paths = [source / r['path'] for r in adapter['members'] if r['dataset'] == dataset]
            chunks = [pl.scan_parquet(paths[i:i+128]).collect(engine='streaming') for i in range(0, len(paths), 128)]
            native[dataset] = pl.concat(chunks, how='diagonal_relaxed')
    source_rows, selected_rows, totals, comparisons = [], [], Counter(), []
    selected_names = {d['feature'] for d in definitions}
    specs = {s.get('dataset'): s for s in manifest['feature_specs'] if s.get('dataset')}
    for number, spec in enumerate(manifest['feature_specs']):
        if spec['source'] != 'FinLab':
            continue
        key, kind = spec['dataset'], spec['rule']['kind']
        table = pl.read_parquet(source / spec['path'])
        fields = [n for n in table.columns if n != 'source_index']
        finite = int(table.select(pl.sum_horizontal(pl.col(fields).is_finite().fill_null(False)).sum()).item())
        raw_nulls = table.height * len(fields) - finite
        detail = {'dataset': key, 'feature': spec['feature'], 'source': 'FinLab', 'kind': kind,
                  'scope': spec['rule']['scope'], 'selected': spec['feature'] in selected_names,
                  'source_observation_slots': table.height * len(fields), 'finite_observations': finite,
                  'raw_null_or_nonfinite_slots': raw_nulls,
                  'expectedness': 'daily_or_optional_or_not_applicable; no_universal_grid',
                  'gap_candidates': 0, 'already_resolved_by_source_union': 0, 'unresolved_recheck_keys': 0}
        if key in owner_checks:
            check=owner_checks[key]
            detail.update(provider_check_state=check['state'],
                provider_checked_at_utc=check.get('source_checked_at_utc'),
                provider_check_source_sha256=check.get('source_sha256'))
        if kind in {'quarter', 'quarter_date', 'revenue'} and spec['rule']['scope'] == 'stock':
            gaps, primary = interior_period_gaps(table, kind, start=start, as_of=as_of)
            candidate, accepted = None, False
            mapping = MAPPINGS.get(key)
            if mapping and mapping.dataset in native and (fresh_source_unions or mapping.dataset + ':' + mapping.field in accepted_sources):
                candidate, _ = normalize_finmind(native[mapping.dataset], mapping)
                if fresh_source_unions:
                    candidate, comparison, _ = missing_only(primary, candidate)
                    comparisons.append({'dataset': key, 'alternate': mapping.dataset + ':' + mapping.field, **comparison})
                    accepted = comparison['accepted']
                else:
                    accepted = True
            annotated = annotate_gap_candidates(gaps, candidate, accepted=accepted)
            # Accepted revised values are also already unioned by the panel.
            revised = specs.get(key.replace('financial_statement:', 'financial_statement_revised:', 1)) if key.startswith('financial_statement:') else None
            if revised and (fresh_source_unions or revised['dataset'] in accepted_sources) and annotated.height:
                _, revised_values = interior_period_gaps(pl.read_parquet(source / revised['path']),
                    'quarter_date', start=start, as_of=as_of)
                if fresh_source_unions:
                    revised_values, comparison, _ = missing_only(primary, revised_values.filter(pl.col('value').is_finite()))
                    comparisons.append({'dataset': key, 'alternate': revised['dataset'], **comparison})
                revised_keys = revised_values.filter(pl.col('value').is_finite()).select('period', 'symbol').unique().with_columns(pl.lit(True).alias('_revised'))
                annotated = annotated.join(revised_keys, on=['period', 'symbol'], how='left', validate='1:1').with_columns(
                    pl.when(pl.col('_revised').fill_null(False)).then(pl.lit('already_resolved_by_verified_source_union'))
                    .otherwise(pl.col('state')).alias('state')).drop('_revised')
            if annotated.height:
                name = spec['feature'] + '.parquet'
                annotated.with_columns(pl.lit(key).alias('dataset'), pl.lit(spec['feature']).alias('feature')) \
                    .write_parquet(out / 'gaps' / name, compression='zstd')
                detail['gap_file'] = 'gaps/' + name
            states = Counter(annotated['state'].to_list())
            detail.update(expectedness='finite_before_and_after_at_native_period; optional_account_applicability_unproven',
                gap_candidates=annotated.height,
                already_resolved_by_source_union=states['already_resolved_by_verified_source_union'],
                unresolved_recheck_keys=states['interior_period_omission_requires_provider_recheck'])
            totals.update(states)
        source_rows.append(detail)
        if number % 40 == 0:
            print(f"[expected-gaps] FinLab {number}/{len(manifest['feature_specs'])}; candidates={sum(x['gap_candidates'] for x in source_rows):,}", flush=True)
    by_feature = {r['feature']: r for r in source_rows}
    for d in definitions:
        row = {'feature': d['feature'], 'source': d['source'], 'label': d.get('dataset') or ', '.join(d.get('labels', d.get('original_concepts', []))) or str(d.get('identity', {})),
               'scope': d.get('scope', d['rule']['scope']), 'kind': d['rule']['kind'],
               'available_panel_cells': coverage.get(d['feature'], {}).get('available_panel_cells'),
               'source_status': 'observed_selected_feature', 'gap_candidates': None, 'unresolved_recheck_keys': None,
               'expectedness': 'no_universal_security_x_date_expectation', 'next_action': 'no_automatic_fill'}
        if d['source'] == 'MOPS':
            row.update(expectedness='optional_report_concept; issuer_report_basis_unit_fiscal_grain_required',
                       next_action='compare_exact_report_context_before_claiming_missing; no_blanket_account_grid')
        elif d['feature'] in by_feature:
            row.update({k: v for k, v in by_feature[d['feature']].items() if k in {'gap_candidates', 'unresolved_recheck_keys', 'expectedness',
                'provider_check_state','provider_checked_at_utc','provider_check_source_sha256'}})
            row['next_action'] = 'finite_owner_priority_recheck' if row['unresolved_recheck_keys'] else 'no_unresolved_interior_period_hint'
            if row['unresolved_recheck_keys'] and row.get('provider_check_state')=='true_upstream_checked_not_gap_fill_proof':
                row['next_action']='owner_rechecked; inspect_native_context_and_source_response; no_infinite_requery'
        elif d['source'] == 'FinMind':
            row['expectedness'] = 'optional_native_financial_code; complete_period_file_not_every_issuer_account'
        elif row['scope'] == 'market':
            row['expectedness'] = 'native_program_release_only; foreign_holidays_and_low_frequency_are_not_daily_gaps'
        selected_rows.append(row)
    # Mandatory execution observations: positive actual volume proves trading.
    # Do not infer sessions across a halt or demand quotes before an IPO.
    stock_paths = sorted((source / 'stocks').glob('*_features.parquet'))
    price = pl.scan_parquet(stock_paths, include_file_paths='_file').select(
        pl.col('date').cast(pl.Date), 'open', 'max', 'min', 'close', 'Trading_Volume',
        pl.col('_file').str.extract(r'/([^/]+)_features\.parquet$', 1).alias('symbol'))
    price = price.filter(pl.col('date').is_between(start, as_of) & (pl.col('Trading_Volume') > 0))
    hard_gaps = mandatory_trading_price_gaps(price, start=start, as_of=as_of)
    hard_gaps.write_parquet(out / 'mandatory_price_gaps.parquet', compression='zstd')
    priorities = []
    for row in source_rows:
        if not row['unresolved_recheck_keys']:
            continue
        core = row['dataset'] in {'monthly_revenue:當月營收', 'financial_statement:資產總額', 'financial_statement:負債總額',
                                  'financial_statement:營業收入淨額', 'financial_statement:每股盈餘'}
        priorities.append({**row, 'priority': 1 if core else 10,
            'reason': 'interior_observation_gap_recheck', 'severity': 'candidate_not_source_corruption_proof',
            'query_shape': 'canonical_sdk_incremental_source_recheck', 'worker': 'existing_finlab_local_refresh'})
    priorities.sort(key=lambda r: (r['priority'], -r['unresolved_recheck_keys'], r['dataset']))
    write_csv(out / 'selected_feature_status.csv', selected_rows)
    write_csv(out / 'source_period_status.csv', source_rows)
    write_csv(out / 'provider_priority_worklist.csv', priorities)
    write_csv(out / 'fresh_source_union_checks.csv', comparisons)
    for page, begin in enumerate(range(0, len(selected_rows), 100), 1):
        lines = ['# 已選特徵逐項缺值判定', '', '| 特徵 | 來源／頻率 | 原生期別候選缺口 | 尚待重查 | 判定 |', '| --- | --- | ---: | ---: | --- |']
        for r in selected_rows[begin:begin + 100]:
            label = r['label'].replace('|', '\\|').replace('\n', ' ')
            lines.append(f"| {label} (`{r['feature']}`) | {r['source']} / {r['kind']} | {r['gap_candidates']} | {r['unresolved_recheck_keys']} | {r['expectedness']} |")
        atomic_write_text(out / 'features' / f'{page:04d}.md', '\n'.join(lines) + '\n')
    result = {'contract': CONTRACT, 'created_at_utc': datetime.now(UTC).isoformat(),
        'source_manifest_sha256': source_sha, 'panel_report_receipt_sha256': sha256(report / 'report_receipt.json'),
        'implementation_sha256': sha256(Path(__file__)),
        'source_union_mode': 'fresh_source_missing_only_qa; not_materialized_panel_claim' if fresh_source_unions else 'prior_fixed_panel_verified_aliases',
        'panel_coverage_is_prior_pinned_view_not_new_source_projection': fresh_source_unions,
        'start': str(start), 'as_of': str(as_of), 'selected_quantities': len(definitions),
        'source_specs_checked': len(source_rows), 'mandatory_price_gaps': hard_gaps.height,
        'candidate_period_keys': sum(x['gap_candidates'] for x in source_rows),
        'already_resolved_in_fixed_panel_keys': totals['already_resolved_by_verified_source_union'],
        'unverified_period_keys_for_recheck': totals['interior_period_omission_requires_provider_recheck'],
        'priority_dataset_requests': len(priorities), 'all_missing_resolved': False,
        'limits': ['Not every financial account is mandatory for every issuer',
                  'Before-first, after-last, ETF and not-yet-released slots are not interior gap claims',
                  'Native MOPS optional concept omissions are not automatically downloaded or filled',
                  'Dataset source/clock/unwired inventories remain distinct from provider corruption'],
        'outputs': {str(p.relative_to(out)): {'sha256': sha256(p), 'bytes': p.stat().st_size}
                    for p in sorted(out.rglob('*')) if p.is_file()}}
    if sha256(source / 'source_manifest.json') != source_sha:
        raise ValueError('pinned source manifest changed during audit')
    if priority_status is not None:
        result['priority_status_sha256']=sha256(priority_status)
        result['provider_rechecked_null_remains_not_download_corruption_proof']=True
    atomic_write_json(out / 'audit_manifest.json', result)
    lines = ['# 原生期別應有值缺口清單', '',
             f"- 固定來源 SHA：`{source_sha}`；期間 {start}–{as_of}。",
             f"- 已選特徵完整列出 {len(definitions):,} 項：[逐項清單](selected_feature_status.csv)。",
             f"- 有前後真實值的期別缺口提示 {result['candidate_period_keys']:,}；通過驗證的來源聯集可解決 {result['already_resolved_in_fixed_panel_keys']:,}。",
             f"- 適用性／缺值原因仍待核對 {result['unverified_period_keys_for_recheck']:,} 個來源期別座標，{len(priorities)} 個資料 key；這不是同數量的已證實漏下載。",
             f"- 已交易但缺正確 OHLC 必要價：{hard_gaps.height:,} 筆。", '',
             '低頻未公布、商品不適用、科目未披露與發布時鐘未接線，不靠造值補齊。原生期別逐筆清單在 gaps/ Parquet；以下是優先 provider key。', '',
             '| 優先級 | 資料 | 尚待核對座標 | Provider 檢查狀態 |', '| ---: | --- | ---: | --- |']
    for row in priorities:
        state='已真實重查；NULL 原因仍待核對' if row.get('provider_check_state')=='true_upstream_checked_not_gap_fill_proof' else row.get('provider_check_state','尚未附檢查狀態')
        lines.append(f"| {row['priority']} | {row['dataset']} | {row['unresolved_recheck_keys']:,} | {state} |")
    lines.extend(['', '## 全特徵 Markdown', ''])
    lines.extend(f"- [逐項 {n:04d}](features/{n:04d}.md)" for n in range(1, (len(selected_rows) + 99)//100 + 1))
    atomic_write_text(out / 'index.md', '\n'.join(lines) + '\n')
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-root', type=Path, required=True)
    p.add_argument('--feature-report', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--start-date', type=date.fromisoformat, default=date(2014, 1, 1))
    p.add_argument('--as-of', type=date.fromisoformat, required=True)
    p.add_argument('--fresh-source-unions', action='store_true', help='Revalidate missing-only agreement on this new source; never reuse old alias acceptance')
    p.add_argument('--priority-status',type=Path,help='bind a verified owner-check status receipt; do not confuse checked NULL with pending dispatch')
    a = p.parse_args()
    result = audit(a.source_root, a.feature_report, a.output_dir, start=a.start_date, as_of=a.as_of,
                   fresh_source_unions=a.fresh_source_unions,priority_status=a.priority_status)
    print(json.dumps({k: v for k, v in result.items() if k != 'outputs'}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
