#!/usr/bin/env python3
"""Exact before/after native source slots; no training carry or fabricated cells."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources


def compare_wide(original, current, *, include_changes=False):
    for table in (original, current):
        if table['source_index'].null_count() or table['source_index'].n_unique() != table.height:
            raise ValueError('duplicate or NULL observation axis')
    fields = sorted(set(original.columns + current.columns) - {'source_index'})
    axis = pl.DataFrame({'source_index': sorted(set(original['source_index']) | set(current['source_index']))})
    arrays = []
    for table in (original, current):
        frame = axis.join(table, on='source_index', how='left', validate='1:1')
        absent = [n for n in fields if n not in frame.columns]
        if absent:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(n) for n in absent)
        arrays.append(frame.select(fields).cast(pl.Float64).to_numpy())
    old, new = arrays; old_mask, new_mask = np.isfinite(old), np.isfinite(new)
    additions = new_mask & ~old_mask
    finite_changed = old_mask & new_mask & (old != new)
    removed = old_mask & ~new_mask
    r, c = np.nonzero(additions)
    exact = pl.DataFrame({'source_index': np.asarray(axis['source_index'].to_list())[r].tolist(),
                         'symbol_or_dimension': np.asarray(fields)[c].tolist(), 'value': new[r, c]})
    counts={'old_finite': int(old_mask.sum()), 'new_finite': int(new_mask.sum()),
        'previous_null_now_real_value': int(additions.sum()), 'finite_source_revisions': int(finite_changed.sum()),
        'finite_no_longer_present_in_current_provider': int(removed.sum())}
    if not include_changes:return exact,counts
    changes={'additions':exact}
    for name,mask in [('revisions',finite_changed),('removed_finite',removed)]:
        r,c=np.nonzero(mask)
        changes[name]=pl.DataFrame({'source_index':np.asarray(axis['source_index'].to_list())[r].tolist(),
            'symbol_or_dimension':np.asarray(fields)[c].tolist(),'previous_value':old[r,c],
            'current_value':new[r,c]})
    return changes,counts


def compare_gap_keys(original, current):
    """Separate repaired old slots from holes newly exposed by a wider envelope."""
    keys = ['period', 'symbol']
    old, new = original.select(keys).unique(), current.select(keys).unique()
    return {'previous_candidates': old.height, 'current_candidates': new.height,
            'previous_gaps_no_longer_missing_in_source': old.join(new, on=keys, how='anti').height,
            'newly_visible_bracketed_candidates': new.join(old, on=keys, how='anti').height,
            'still_missing_source_candidates': old.join(new, on=keys, how='inner', validate='1:1').height}


def gap_transitions(before, after, out):
    if out.exists(): raise FileExistsError('new immutable gap transition report required')
    out.mkdir(parents=True)
    specs, receipts = [], []
    for root in (before, after):
        receipt = json.loads((root / 'audit_manifest.json').read_text())
        receipts.append(receipt)
        csv_path = root / 'source_period_status.csv'
        if sha256(csv_path) != receipt['outputs']['source_period_status.csv']['sha256']:
            raise ValueError('source gap audit inventory changed')
        specs.append({r['dataset']: r for r in pl.read_csv(csv_path).to_dicts()})
    records = []
    for key in sorted(set(specs[0]) | set(specs[1])):
        frames = []
        for root, inventory, receipt in zip((before, after), specs, receipts):
            relative = inventory.get(key, {}).get('gap_file')
            if not relative:
                frames.append(pl.DataFrame(schema={'period': pl.String, 'symbol': pl.String}))
                continue
            path = root / relative
            proof = receipt['outputs'][relative]
            if sha256(path) != proof['sha256']:
                raise ValueError('source gap key evidence changed')
            frames.append(pl.read_parquet(path))
        records.append({'dataset': key, **compare_gap_keys(*frames)})
    write_csv(out / 'gap_transitions.csv', records)
    totals = {n: sum(r[n] for r in records) for n in records[0] if n != 'dataset'}
    result = {'created_at_utc': datetime.now(UTC).isoformat(), **totals,
        'old_audit_sha256': sha256(before / 'audit_manifest.json'),
        'new_audit_sha256': sha256(after / 'audit_manifest.json'),
        'same_expectedness_policy_required': True, 'source_null_not_mandatory_account_claim': True,
        'gap_transitions_sha256': sha256(out / 'gap_transitions.csv')}
    atomic_write_json(out / 'gap_transition_manifest.json', result)
    atomic_write_text(out / 'index.md', '# 原生來源缺口前後比較\n\n'
        f"- 原有候選 {totals['previous_candidates']:,}，目前 {totals['current_candidates']:,}。\n"
        f"- 舊來源缺口已不再缺值 {totals['previous_gaps_no_longer_missing_in_source']:,}；仍缺 {totals['still_missing_source_candidates']:,}。\n"
        f"- 新資料擴大前後有值的範圍，新增可核對的中間缺值提示 {totals['newly_visible_bracketed_candidates']:,}；不是來源掉值。\n"
        '- [逐項變化](gap_transitions.csv)。這不是對 optional 科目強制有值的宣稱。\n')
    return result


def compare(before, after, out):
    if out.exists(): raise FileExistsError('new immutable source comparison required')
    old, new = verify_sources(before), verify_sources(after)
    out.mkdir(parents=True); (out / 'new_observations').mkdir()
    old_specs = {s['dataset']: s for s in old['feature_specs'] if s.get('source') == 'FinLab'}
    records = []
    for i, spec in enumerate(s for s in new['feature_specs'] if s.get('source') == 'FinLab'):
        key = spec['dataset']; previous = old_specs.get(key)
        if previous is None:
            records.append({'dataset': key, 'state': 'new_source_quantity; not_old_missing_slot'}); continue
        old_proof, new_proof = old['files'][previous['path']], new['files'][spec['path']]
        if old_proof['sha256'] == new_proof['sha256']:
            records.append({'dataset': key, 'state': 'identical_observation_bytes', 'previous_null_now_real_value': 0,
                            'finite_source_revisions': 0, 'finite_no_longer_present_in_current_provider': 0}); continue
        changes, counts = compare_wide(pl.read_parquet(before / previous['path']), pl.read_parquet(after / spec['path']),include_changes=True)
        fills=changes['additions']
        record = {'dataset': key, 'state': 'verified_immutable_source_comparison', **counts,
                  'old_sha256': old_proof['sha256'], 'new_sha256': new_proof['sha256']}
        if fills.height:
            target = out / 'new_observations' / (spec['feature'] + '.parquet')
            fills.write_parquet(target, compression='zstd')
            record.update(new_observations_path=str(target.relative_to(out)), new_observations_sha256=sha256(target))
        for name in ('revisions','removed_finite'):
            if not changes[name].height:continue
            folder=out/name;folder.mkdir(exist_ok=True)
            target=folder/(spec['feature']+'.parquet')
            changes[name].write_parquet(target,compression='zstd')
            record[name+'_path']=str(target.relative_to(out));record[name+'_sha256']=sha256(target)
        records.append(record)
        if i % 40 == 0: print(f'[source-comparison] {i}: new source slots={sum(r.get("previous_null_now_real_value",0) for r in records):,}', flush=True)
    write_csv(out / 'finlab_source_changes.csv', records)
    result = {'created_at_utc': datetime.now(UTC).isoformat(), 'source_only': True,
        'report_contract':'tw_exact_source_observation_changes_v2_with_revisions_and_removed_keys',
        'before_source_manifest_sha256': sha256(before / 'source_manifest.json'),
        'after_source_manifest_sha256': sha256(after / 'source_manifest.json'), 'finlab_quantities_checked': len(records),
        'previous_null_now_real_value': sum(r.get('previous_null_now_real_value', 0) for r in records),
        'finite_source_revisions': sum(r.get('finite_source_revisions', 0) for r in records),
        'finite_no_longer_present_in_current_provider': sum(r.get('finite_no_longer_present_in_current_provider', 0) for r in records),
        'training_usable_new_cells': 'not_a_claim; publication_clock_lifecycle_and_fold_still_apply',
        'changes_sha256': sha256(out / 'finlab_source_changes.csv')}
    atomic_write_json(out / 'comparison_manifest.json', result)
    atomic_write_text(out / 'index.md', '# 新版真實觀測補值驗證\n\n'
        f"- 逐項核對 {len(records)} 個 FinLab 來源數量；舊 NULL／不存在座標有新真實值 {result['previous_null_now_real_value']:,}。\n"
        f"- 原已存在值的來源修訂 {result['finite_source_revisions']:,}；新來源不再有值 {result['finite_no_longer_present_in_current_provider']:,}。舊版完整保留，沒有就地覆蓋。\n"
        '- [每項數量的 before/after 清單](finlab_source_changes.csv)；新增觀測的完整鍵和值在 new_observations/。\n'
        '- 此為來源期別座標，不等於全數是已公布、已上市或訓練期可用的股票×日期值。\n')
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--before', type=Path, required=True); p.add_argument('--after', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--gap-audits', action='store_true', help='Compare immutable native gap audits rather than source tables')
    a=p.parse_args(); function=gap_transitions if a.gap_audits else compare
    print(json.dumps(function(a.before,a.after,a.output_dir),ensure_ascii=False,indent=2))


if __name__ == '__main__': main()
