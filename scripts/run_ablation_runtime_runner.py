#!/usr/bin/env python3
"""Apply measured per-case transport settings, then exec the canonical runner."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


TRANSPORT_KEYS = frozenset({
    'STOCKAGENT_ANNUAL_BATCH_DEVICE_CACHE_BYTES',
    'STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH',
    'STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH_ROWS',
})


def case_for_config(config: Path, output_root: Path, cases: list[str]) -> str:
    parts = config.resolve(strict=True).relative_to(output_root.resolve(strict=True)).parts
    if len(parts) == 2 and parts[0] == 'generated_configs':
        name = Path(parts[-1]).stem
    elif len(parts) == 4 and parts[0] == '_capacity' and parts[-1] == 'config.yaml':
        name = parts[1]
    else:
        raise ValueError('config is outside the canonical ablation config layouts')
    if name not in cases:
        raise ValueError('config does not belong to this experiment matrix')
    return name


def environment_for_case(context: dict, name: str, inherited: dict[str, str],
                         *, config_sha256: str | None = None) -> dict[str, str]:
    if context.get('contract') != 'ablation_case_transport_environment_v1':
        raise ValueError('unknown case transport contract')
    if name not in context['cases'] or context['first_case'] not in context['cases']:
        raise ValueError('case is outside the measured experiment matrix')
    measured = name == context['first_case'] and (
        context.get('first_config_sha256') is None or config_sha256 == context['first_config_sha256'])
    changes = context['first_environment'] if measured else context['default_environment']
    if set(changes) != TRANSPORT_KEYS:
        raise ValueError('case overrides may contain only the measured transport controls')
    if (changes['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH'] not in ('0', '1')
            or not 1 <= int(changes['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH_ROWS']) <= 64
            or int(changes['STOCKAGENT_ANNUAL_BATCH_DEVICE_CACHE_BYTES']) < 0
            or not all(isinstance(value, str) for value in changes.values())):
        raise ValueError('invalid measured transport setting')
    return {**inherited, **changes}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', type=Path, required=True)
    parser.add_argument('runner_args', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    forwarded = args.runner_args[1:] if args.runner_args[:1] == ['--'] else args.runner_args
    base_args = forwarded[:forwarded.index('--')] if '--' in forwarded else forwarded
    runner_parser = argparse.ArgumentParser(add_help=False)
    runner_parser.add_argument('-c', '--config', type=Path, required=True)
    parsed, _ = runner_parser.parse_known_args(base_args)
    raw = args.context.read_bytes()
    context = json.loads(raw)
    name = case_for_config(parsed.config, Path(context['output_root']), context['cases'])
    config_sha = hashlib.sha256(parsed.config.read_bytes()).hexdigest()
    env = environment_for_case(context, name, os.environ, config_sha256=config_sha)
    runner = Path(context['canonical_runner']).resolve(strict=True)
    if runner.name != 'coda_runner.sh' or not runner.is_file():
        raise ValueError('context must delegate to the canonical coda runner')
    receipt_dir = Path(context['receipt_root']) / name
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt = dict(contract=context['contract'], case=name,
        context_sha256=hashlib.sha256(raw).hexdigest(), config_sha256=config_sha,
        canonical_runner=str(runner), forwarded_args=forwarded,
        transport={key:env[key] for key in sorted(TRANSPORT_KEYS)})
    target = receipt_dir / (config_sha + '.json')
    temporary = target.with_suffix('.json.'+str(os.getpid())+'.tmp')
    temporary.write_text(json.dumps(receipt, indent=2)+'\n')
    os.replace(temporary, target)
    os.execvpe(str(runner), [str(runner), *forwarded], env)


if __name__ == '__main__':
    main()
