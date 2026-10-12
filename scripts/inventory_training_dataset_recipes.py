"""Inventory training data combinations from actual manifests and resolved configs.

Metadata only: no provider calls, cold hydration, model loading or persistent
assembly. Missing provenance remains a gap, rather than a guessed recipe.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.config import load_config
from stockagent.data_sync.desync_snapshots import atomic_write_json as _atomic_json, atomic_write_bytes, sha256_file
from stockagent.data_sync.training_recipes import digest
from scripts.audit_training_source_inventory import footer, specs
import csv
import io


def atomic_write_json(path, value):
    if path.is_file() and json.loads(path.read_text()) == value:
        return
    _atomic_json(path, value)


def dump_csv(path, rows):
    keys = sorted({key for row in rows for key in row})
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=keys)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: json.dumps(value, ensure_ascii=False)
                         if isinstance(value, (dict, list)) else value
                         for key, value in row.items()})
    content = stream.getvalue().encode('utf-8-sig')
    if not path.is_file() or path.read_bytes() != content:
        atomic_write_bytes(path, content)


def family(config: dict) -> str:
    data = config.get('data', {})
    mode = config.get('trading', {}).get('execution_mode', '')
    if data.get('factorized_feature_manifest'):
        return 'tw-factorized-value-or-four-channel-confirm-manifest'
    if mode == 'tw_minute':
        return 'tw-stock-minute-plus-oof-guidance' if data.get('minute_daily_guidance_path') else 'tw-stock-minute'
    if 'futures' in mode:
        return 'tw-physical-futures-rules-and-context'
    if 'tick' in mode or 'option' in mode:
        return 'tw-derivatives-tick-or-bidask'
    public = str(data.get('tw_public_feature_path', ''))
    if 'finlab' in public:
        return 'tw-private-finlab-overlay'
    if any(x in public for x in ('research', 'schedule', 'cross_source', 'mixed_frequency')):
        return 'tw-release-schedule-or-wide-research'
    if data.get('use_tw_public_features'):
        return 'tw-public-core-plus-selected-execution-tape'
    return 'canonical-daily-panel-or-provider-native'


def combination(config: dict) -> dict:
    # Full selected data settings, including feature list, temporal clock,
    # cadence flags and source path. Defaults are evidence, not active inputs.
    return {k: config.get(k, {}) for k in ('data', 'trading', 'walk_forward')}


def declared_release_bindings(config: dict) -> list[dict]:
    """Record literal immutable IDs in paths, without inventing a SHA proof.

    Many older manifests lack structured source_releases but already name a
    materialized release. This is partial provenance, not complete recovery.
    """
    rows = []
    for section in ('data', 'trading'):
        for field, value in config.get(section, {}).items():
            if not isinstance(value, str):
                continue
            parts = Path(value).parts
            if 'stockagent-packed-materialized' not in parts:
                continue
            index = parts.index('stockagent-packed-materialized')
            if len(parts) < index + 3:
                continue
            dataset, snapshot_id = parts[index+1:index+3]
            if not snapshot_id.startswith(dataset[:36]) or '-l' not in snapshot_id:
                continue
            rows.append({'field': f'{section}.{field}', 'dataset': dataset,
                         'snapshot_id': snapshot_id, 'path': value,
                         'manifest_sha256_verified': False,
                         'scope': 'literal_path_binding_not_all_active_source_proof'})
    return rows


def inventory(repo: Path, output: Path, *, additional_run_roots=(), remote_reports=(),
              include_minutes=True) -> dict:
    records, errors, definitions = [], [], []
    for path in sorted((repo/'configs').rglob('*.yaml')):
        try:
            declared = yaml.safe_load(path.read_text())
            if not isinstance(declared, dict):
                continue
            # Experiment matrices and GPU job launchers have their own canonical
            # loaders. They are methods, not standalone ExperimentConfig YAMLs;
            # do not manufacture errors or assert their baseline is every variant.
            if 'matrix' in declared or 'base_spec' in declared or isinstance(declared.get('jobs'), list):
                definitions.append({'evidence': str(path.relative_to(repo)),
                    'evidence_sha256': sha256_file(path),
                    'kind': 'gpu_job_spec' if isinstance(declared.get('jobs'), list) else 'ablation_spec',
                    'declared_method': declared,
                    'is_training_execution_evidence': False})
                continue
            cfg = asdict(load_config(path))
            combo = combination(cfg)
            records.append({'evidence': str(path.relative_to(repo)), 'evidence_sha256': sha256_file(path),
                'evidence_kind': 'configured_not_proof_it_was_trained', 'family': family(cfg),
                'combination_id': digest(combo), 'combination': combo,
                'exact_historical_sources_bound': False})
        except Exception as exc:
            errors.append({'path': str(path), 'error': type(exc).__name__+': '+str(exc)})
    for report in remote_reports:
        remote = json.loads(Path(report).read_text())
        for raw in remote['training_manifests']:
            cfg = raw.get('configuration', {})
            combo = combination(cfg)
            records.append({'evidence': f"{remote['node']}:{raw['path']}",
                'evidence_sha256': raw['sha256'], 'evidence_kind': 'training_run_manifest',
                'family': family(cfg) if cfg else 'missing_resolved_config',
                'combination_id': digest(combo) if cfg else '', 'combination': combo,
                'dataset_fingerprint': raw.get('dataset_fingerprint'),
                'selected_fold_ids': raw.get('selected_fold_ids'),
                'execution_clock': raw.get('execution_clock'), 'decision_clock': raw.get('decision_clock'),
                'source_releases': raw.get('source_releases', {}),
                'exact_historical_sources_bound': bool(raw.get('source_releases')),
                'missing_provenance': ([] if cfg else ['resolved_configuration']) +
                    ([] if raw.get('source_releases') else ['exact_source_release_binding_not_in_manifest'])})
        errors.extend(remote.get('errors', []))
    roots = [repo/'artifacts'/v for v in ('markets', 'ablations', 'benchmarks', 'operations')]
    roots.extend(Path(p) for p in additional_run_roots)
    paths = set()
    for root in roots:
        if root.is_dir():
            # Do not follow aliases into raw/source trees or cold mounts.
            for directory, _, names in os.walk(root, followlinks=False):
                if 'run_manifest.json' in names:
                    paths.add(Path(directory)/'run_manifest.json')
    for path in sorted(paths):
        try:
            raw = json.loads(path.read_text())
            cfg = raw.get('configuration', raw.get('resolved_config', {}))
            combo = combination(cfg)
            row = {'evidence': str(path), 'evidence_sha256': sha256_file(path),
                   'evidence_kind': 'training_run_manifest',
                   'family': family(cfg) if cfg else 'missing_resolved_config',
                   'combination_id': digest(combo) if cfg else '', 'combination': combo,
                   'dataset_fingerprint': raw.get('dataset_fingerprint'),
                   'selected_fold_ids': raw.get('selected_fold_ids'),
                   'execution_clock': raw.get('execution_clock'),
                   'decision_clock': raw.get('decision_clock'),
                   'exact_historical_sources_bound': bool(raw.get('source_releases')),
                   'source_releases': raw.get('source_releases', {}),
                   'missing_provenance': []}
            if not cfg:
                row['missing_provenance'].append('resolved_configuration')
            if not row['exact_historical_sources_bound']:
                row['missing_provenance'].append('exact_source_release_binding_not_in_manifest')
            records.append(row)
        except Exception as exc:
            errors.append({'path': str(path), 'error': type(exc).__name__+': '+str(exc)})
    output.mkdir(parents=True, exist_ok=True)
    for row in records:
        row['declared_release_bindings'] = declared_release_bindings(row['combination'])
    atomic_write_json(output/'training_combinations.json', records)
    atomic_write_json(output/'experiment_definitions.json', definitions)
    dump_csv(output/'training_combinations.csv', [{k:v for k,v in r.items() if k!='combination'} for r in records])
    minute_rows, file_rows = [], []
    minute_specs = [s for s in specs() if 'minute' in s[0] or s[0].endswith('-1m')] if include_minutes else []
    # This collector is distinct from stock kbars: physical futures, index and
    # latest option contracts, with separately dated source-empty/gap receipts.
    if include_minutes:
        minute_specs.append(('tw-shioaji-contract-minute', '永豐期貨／指數／選擇權原始分鐘',
            'data_tw_shioaji_history', 'contracts/*/*/kbars/*/data.parquet',
            ['contract', 'ts'], 'raw-contract-minutes-not-stock-panel'))
    for name, title, relative, pattern, grain, role in minute_specs:
        if 'minute' not in name and not name.endswith('-1m'):
            continue
        root = repo/relative
        paths = sorted(root.glob(pattern)) if pattern and root.exists() else ([root] if root.is_file() else [])
        total = 0
        first, last = [], []
        failures = []
        for path in paths:
            try:
                values, columns = footer(path)
                values.update(dataset=name, grain=str(grain), validation='footer_only_not_full_semantic_audit')
                file_rows.append(values)
                total += values['rows']
                if values['first']: first.append(values['first'])
                if values['last']: last.append(values['last'])
            except Exception as exc:
                failures.append({'path': str(path), 'error': str(exc)})
        minute_rows.append({'dataset':name,'title':title,'source':str(root.resolve()),
                            'files':len(paths),'rows':total,'first':min(first) if first else None,
                            'last':max(last) if last else None,'role':role,
                            'validation':'footer_only_not_complete_coverage','failures':failures})
    atomic_write_json(output/'minute_inventory.json', minute_rows)
    dump_csv(output/'minute_files.csv', file_rows)
    atomic_write_json(output/'errors.json', errors)
    used = [r for r in records if r['evidence_kind']=='training_run_manifest']
    summary = {'configured_combinations':len(records)-len(used),'training_manifests':len(used),
               'experiment_definitions': len(definitions),
               'distinct_combinations':len({r['combination_id'] for r in records}),
               'families':dict(Counter(r['family'] for r in records)), 'errors':len(errors),
               'minute_datasets':len(minute_rows), 'minute_inventory_requested': include_minutes,
               'scope':'available local configs/run manifests + supplied exact evidence roots and remote metadata reports',
               'archived_payloads_not_opened':True, 'all_historical_training_sources_recoverable_claim':False,
               'data_deleted':False,'assemblies_retained':False}
    summary['records_with_literal_release_paths'] = sum(bool(r['declared_release_bindings']) for r in records)
    atomic_write_json(output/'summary.json', summary)
    lines = ['# 訓練資料組合逐項清冊', '', '此表分開已執行 run 與僅存在設定的實驗；未證明的來源版本不會猜填。', '',
             '| 證據 | 類型 | 組合 | 來源版本缺口／既有固定路徑 |', '| --- | --- | --- | --- |']
    for row in records:
        gap = ', '.join(row.get('missing_provenance', [])) or ('未固定歷史來源' if not row['exact_historical_sources_bound'] else '已記錄，仍需還原驗證')
        if row['declared_release_bindings']:
            gap += '; 已保留固定路徑 IDs（未驗證全来源 SHA）'
        lines.append(f"| `{row['evidence']}` | {row['evidence_kind']} | {row['family']} / {row['combination_id'][:12]} | {gap} |")
    lines += ['', '## 消融矩陣與 GPU 工作定義', '',
              '這些由各自的 canonical runner 解讀，不能當成曾執行訓練。完整參數保留於 `experiment_definitions.json`。', '',
              '| 證據 | 方法類型 |', '| --- | --- |']
    lines += [f"| `{row['evidence']}` | {row['kind']} |" for row in definitions]
    content = ('\n'.join(lines)+'\n').encode('utf-8')
    path = output/'training_combinations.md'
    if not path.is_file() or path.read_bytes() != content:
        atomic_write_bytes(path, content)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,default=ROOT)
    parser.add_argument('--output-root',type=Path,default=ROOT/'artifacts/data_quality/minute_recipes_20261007')
    parser.add_argument('--additional-run-root',type=Path,action='append',default=[])
    parser.add_argument('--remote-run-report', type=Path, action='append', default=[])
    parser.add_argument('--methods-only', action='store_true', help='Metadata only; do not scan minute payloads/footers.')
    args=parser.parse_args()
    print(json.dumps(inventory(args.repo,args.output_root,additional_run_roots=args.additional_run_root,
                               remote_reports=args.remote_run_report,
                               include_minutes=not args.methods_only),ensure_ascii=False))


if __name__=='__main__':main()
