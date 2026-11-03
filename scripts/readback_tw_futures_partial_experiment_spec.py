#!/usr/bin/env python3
"""Check the resolved strict/partial ablations before any worker is started."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file

ALLOWED_DIFFERENCES = {'experiment_name', 'runner.output_dir',
                       'trading.tw_futures_portfolio_entry_clock_path',
                       'trading.tw_futures_portfolio_opening_reference_path'}


def differences(a, b, prefix='') -> set[str]:
    if isinstance(a, dict) and isinstance(b, dict):
        return set().union(*(differences(a.get(k), b.get(k), f'{prefix}.{k}' if prefix else k)
                             for k in set(a) | set(b)))
    return set() if a == b else {prefix}


def readback(*, spec_path: Path, configs: Path, output: Path) -> dict:
    if output.exists():
        raise ValueError('config readback must not overwrite an existing report')
    spec = yaml.safe_load(spec_path.read_text())
    if spec['expected_fold_count'] != 14:
        raise ValueError('expected full-history 14-fold experiment')
    variants = spec['matrix']['dimensions'][0]['variants']
    if len(variants) != 16:
        raise ValueError('expected eight strict/partial pairs')
    results = []
    for strict_variant, partial_variant in zip(variants[::2], variants[1::2]):
        old_name, new_name = strict_variant['name'], partial_variant['name']
        if not old_name.startswith('strict__') or new_name != old_name.replace('strict__', 'partial__', 1):
            raise ValueError('experiment variant pairing differs')
        old_path = configs / ('all_twd__' + old_name + '.yaml')
        new_path = configs / ('all_twd__' + new_name + '.yaml')
        old, new = (yaml.safe_load(path.read_text()) for path in (old_path, new_path))
        changes = differences(old, new)
        if changes != ALLOWED_DIFFERENCES:
            raise ValueError(f'unexpected paired config difference: {sorted(changes)}')
        training, walk = new['training'], new['walk_forward']
        if (training['epochs'] != 10000 or training['early_stopping_reference_epochs'] != 1000
                or training['early_stopping_no_improve_ratio'] != .1
                or training.get('pretrained_initialization_root') is not None
                or training.get('warm_start_from_previous_fold') is not False
                or walk['expected_first_year'] != 2011 or walk['year_boundary_mode'] != 'calendar'):
            raise ValueError('full-history training or scratch contract differs')
        def clock(config):
            path = Path(config['trading']['tw_futures_portfolio_entry_clock_path'])
            manifest = json.loads(path.with_name('manifest.json').read_text())
            if manifest['entries_sha256'] != sha256_file(path):
                raise ValueError('experiment clock differs from source manifest')
            return manifest
        before, after = clock(old), clock(new)
        if (before['clock']['version'] != 5 or after['clock']['version'] != 6
                or after.get('training_ready') is not False or after.get('research_only') is not True
                or not after['clock'].get('allow_partial_observations')
                or before['clock']['decision_time'] != after['clock']['decision_time']
                or before['clock'].get('window_minutes', 1) != after['clock'].get('window_minutes', 1)):
            raise ValueError('strict and partial source contracts differ in scope or state')
        results.append(dict(strict=old_name, partial=new_name, config_changes=sorted(changes),
            decision_time=after['clock']['decision_time'], window_minutes=after['clock'].get('window_minutes', 1),
            partial_contract_days=after['partial_observations']['contract_days'],
            strict_config_sha256=sha256_file(old_path), partial_config_sha256=sha256_file(new_path)))
    proof = dict(contract='tw_futures_partial_paired_config_readback_v1', spec_sha256=sha256_file(spec_path),
        expected_fold_count=14, experiment_pairs=len(results), training_started=False,
        financial_sources_admitted=False, results=results)
    atomic_write_json(output, proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec-path', type=Path, required=True)
    parser.add_argument('--configs', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(readback(**vars(args)), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
