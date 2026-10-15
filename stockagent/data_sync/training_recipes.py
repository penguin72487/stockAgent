"""Rebuild recipes bind immutable inputs, code and transformations, not tensors."""
from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import os
import re
import subprocess
import sys
import tempfile

from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file, SnapshotError
from stockagent.data_sync.packed_snapshots import (
    resolve_latest_packed, resolve_packed_snapshot_id, verify_packed_snapshot,
)

CONTRACT = 'source_pinned_training_recipes_v1'


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                   separators=(',', ':')).encode()).hexdigest()


def load_templates(path: Path) -> list[dict]:
    value = json.loads(path.read_text())
    if value.get('schema_version') != 1 or value.get('contract') != CONTRACT:
        raise SnapshotError('unsupported recipe catalog')
    rows = value['recipes']
    ids = [r['recipe'] for r in rows]
    if len(ids) != len(set(ids)) or any(not re.fullmatch(r'[a-z0-9-]+', v) for v in ids):
        raise SnapshotError('duplicate or unsafe recipe id')
    for row in rows:
        for step in row['steps']:
            path = Path(step[0])
            if (path.is_absolute() or '..' in path.parts or path.parts[0] != 'scripts'
                    or path.suffix != '.py'):
                raise SnapshotError('recipe must use a repository-owned builder argv')
        for rule in row.get('source_range_requirements', []):
            if rule['input'] not in row['inputs'] or rule['at_least'] not in row['inputs']:
                raise SnapshotError('recipe date compatibility rule uses unknown inputs')
    return rows


def code_identity(repo: Path) -> dict[str, str]:
    # Include shared implementations, not only the tiny CLI wrapper. No secrets,
    # environment files, checkpoints, private code bytes or GPU cache are copied.
    paths = {repo/'train.py', repo/'scripts/runtime_env.sh'}
    for root in ('stockagent', 'scripts', 'downloader'):
        paths.update((repo/root).rglob('*.py'))
    return {str(p.relative_to(repo)): sha256_file(p) for p in sorted(paths) if p.is_file()}


def refresh_recipes(*, repo: Path, sync_root: Path, output_root: Path,
                    templates_path: Path | None = None, catalog_path: Path | None = None) -> dict:
    templates_path = templates_path or repo/'configs/data_sync/training_dataset_recipes.json'
    catalog_path = catalog_path or repo/'configs/data_sync/packed_datasets.json'
    templates = load_templates(templates_path)
    entries = json.loads(catalog_path.read_text())['datasets']
    available: dict[str, dict] = {}
    errors = []
    for entry in entries:
        if not entry['publish']:
            continue
        try:
            release = resolve_latest_packed(sync_root, entry['dataset'], require_objects=False)
            available[entry['dataset']] = {
                'dataset': entry['dataset'], 'snapshot_id': release.manifest['snapshot_id'],
                'manifest_sha256': release.manifest_sha256,
                'inventory_sha256': release.manifest['archive']['inventory']['sha256'],
                'metadata': release.manifest.get('metadata', {}),
                'hlc': release.manifest['hlc'],
                'objects_present_not_checked': True,
            }
        except (OSError, ValueError, SnapshotError) as exc:
            errors.append({'dataset': entry['dataset'], 'reason': str(exc)})
    code = code_identity(repo)
    statuses = []
    for template in templates:
        inputs, missing = {}, []
        for slot, selector in template['inputs'].items():
            if isinstance(selector, str):
                candidates = [available[selector]] if selector in available else []
            else:
                candidates = [v for k, v in available.items() if k.startswith(selector['prefix'])]
            if not candidates:
                missing.append(slot)
                continue
            # Resolve the moving head only here. Every resulting immutable recipe
            # contains exact IDs; a running assembly never follows a newer head.
            chosen = max(candidates, key=lambda v: (
                str(v['metadata'].get('freshness_value', '')),
                int(v['hlc']['physical_ns']), int(v['hlc']['logical']),
                str(v['hlc']['node_id']), v['snapshot_id']))
            inputs[slot] = chosen
        if missing:
            statuses.append({'recipe': template['recipe'], 'state': 'waiting_source',
                             'missing_input_slots': missing})
            continue
        incompatible = []
        for rule in template.get('source_range_requirements', []):
            a, b = inputs[rule['input']]['metadata'], inputs[rule['at_least']]['metadata']
            actual = a.get('freshness_value', a.get('source_validation_last_date'))
            required = b.get('freshness_value', b.get('source_validation_last_date'))
            if not actual or not required or str(actual) < str(required):
                incompatible.append({'input': rule['input'], 'actual': actual,
                                     'required_by': rule['at_least'], 'required': required})
        if incompatible:
            statuses.append({'recipe': template['recipe'], 'state': 'waiting_compatible_source',
                             'source_range_mismatches': incompatible})
            continue
        payload = {'contract': CONTRACT, 'schema_version': 1, 'template': template,
                   'inputs': inputs, 'code_files': code,
                   'recipe_catalog_sha256': sha256_file(templates_path),
                   'builds_on_arrival': False, 'training_ready_claim': False}
        identity = digest(payload)
        destination = output_root/'versions'/template['recipe']/f'{identity}.json'
        if destination.exists():
            if json.loads(destination.read_text()) != payload:
                raise SnapshotError('immutable recipe collision')
        else:
            atomic_write_json(destination, payload)
        head = {'recipe': template['recipe'], 'recipe_id': identity,
                'path': str(destination.relative_to(output_root)),
                'state': 'pinned_method_ready_not_training_acceptance'}
        head_path = output_root/'heads'/f"{template['recipe']}.json"
        if not head_path.is_file() or json.loads(head_path.read_text()) != head:
            atomic_write_json(head_path, head)
        statuses.append(head)
    result = {'contract': CONTRACT, 'recipes': statuses, 'source_errors': errors,
              'generated_at_utc': datetime.now(UTC).isoformat(),
              'stores_assembled_data': False, 'all_history_complete_claim': False}
    atomic_write_json(output_root/'status.json', result)
    return result


def build_recipe(recipe_path: Path, *, repo: Path, sync_root: Path,
                 input_paths: dict[str, str], parameters: dict[str, str],
                 receipt_path: Path, output_root: Path | None = None,
                 trial: bool = True) -> dict:
    recipe = json.loads(recipe_path.read_text())
    if recipe.get('contract') != CONTRACT:
        raise SnapshotError('unsupported pinned recipe')
    if (recipe_path.parent.name != recipe['template']['recipe']
            or recipe_path.stem != digest(recipe)):
        raise SnapshotError('pinned recipe payload differs from its immutable recipe id/path')
    if code_identity(repo) != recipe['code_files']:
        raise SnapshotError('builder/shared code changed; generate a new pinned recipe')
    template = recipe['template']
    if set(input_paths) != set(recipe['inputs']) or set(parameters) != set(template['parameters']):
        raise SnapshotError('recipe inputs/parameters do not match its contract')
    if not trial and output_root is None:
        raise SnapshotError('persistent assembly needs explicit output root')
    inputs = {}
    for slot, binding in recipe['inputs'].items():
        release = resolve_packed_snapshot_id(sync_root, binding['dataset'], binding['snapshot_id'])
        if release.manifest_sha256 != binding['manifest_sha256']:
            raise SnapshotError('pinned source manifest changed')
        path = Path(input_paths[slot]).resolve(strict=True)
        proof = verify_packed_snapshot(sync_root, release, materialized_path=path)
        if proof.get('materialized_verified') is not True:
            raise SnapshotError('input has not passed exact cold/materialization verification')
        inputs[slot] = str(path)
        inputs[slot+'_snapshot_id'] = binding['snapshot_id']
    param_sha = {}
    if 'base_config' in parameters:
        path = Path(parameters['base_config']).resolve(strict=True)
        from stockagent.config import load_config
        from dataclasses import asdict
        param_sha['base_config'] = digest(asdict(load_config(path)))
    result = {'contract': CONTRACT, 'recipe_sha256': sha256_file(recipe_path),
              'input_bindings': recipe['inputs'], 'parameters': parameters,
              'resolved_parameter_sha256': param_sha, 'trial': trial,
              'assembled_data_retained': not trial, 'state': 'running', 'steps': []}
    from importlib.metadata import version, PackageNotFoundError
    result['runtime'] = {'python': sys.version, 'packages': {}}
    for package in ('polars', 'pyarrow', 'numpy', 'torch'):
        try:
            result['runtime']['packages'][package] = version(package)
        except PackageNotFoundError:
            result['runtime']['packages'][package] = 'missing'
    atomic_write_json(receipt_path, result)
    temporary = tempfile.TemporaryDirectory(prefix='stockagent-recipe-') if trial else None
    out = Path(temporary.name)/'assembled' if temporary else output_root.resolve()
    try:
        if out.exists():
            raise SnapshotError('recipe never overwrites an existing assembly')
        values = {**inputs, **parameters, 'output': str(out)}
        for step in template['steps']:
            module = '.'.join(Path(step[0]).with_suffix('').parts)
            argv = [sys.executable, '-m', module, *[v.format_map(values) for v in step[1:]]]
            if trial and step[0] == 'scripts/prepare_tw_day_trade_factorized_panel.py':
                argv.extend(['--runtime-cache-root', str(out.parent/'runtime-cache')])
            subprocess.run(argv, cwd=repo, check=True)
            result['steps'].append({'builder': step[0], 'argv': argv[2:], 'exit_code': 0})
            atomic_write_json(receipt_path, result)
        for name in ('factorized_manifest.json', 'dataset_manifest.json', 'manifest.json', 'audit.json'):
            if (out/name).is_file():
                result.setdefault('output_receipts', {})[name] = sha256_file(out/name)
        result['state'] = 'verified_builder_completed'
        return result
    except BaseException as exc:
        result.update(state='failed', error=type(exc).__name__+': '+str(exc))
        raise
    finally:
        if temporary:
            temporary.cleanup()  # Only this freshly created, exactly owned trial.
            result['trial_output_removed'] = not out.exists()
        atomic_write_json(receipt_path, result)


def materialize_recipe_inputs(recipe_path: Path, *, repo: Path, sync_root: Path,
                              materialized_root: Path,
                              edge_state: Path | None = None) -> dict[str, str]:
    """Explicit on-demand use of the existing managed cache, never on arrival.

    Index-only Vast uses its established hydration/peer/lease owner. Retain the
    exact job's payload under that owner's seven-day lease until reconstruction
    verifies it; do not bypass edge ignores or fetch unrelated datasets.
    """
    recipe = json.loads(recipe_path.read_text())
    if recipe.get('contract') != CONTRACT or code_identity(repo) != recipe['code_files']:
        raise SnapshotError('unsupported recipe or builder/shared code changed before hydration')
    if recipe_path.stem != digest(recipe) or recipe_path.parent.name != recipe['template']['recipe']:
        raise SnapshotError('pinned recipe payload differs from its immutable recipe id/path')
    edge_state = edge_state or Path(os.environ.get(
        'STOCKAGENT_PACKED_EDGE_STATE', '/var/lib/stockagent-packed-edge/state.json'))
    inputs = {}
    for slot, binding in recipe['inputs'].items():
        release = resolve_packed_snapshot_id(sync_root, binding['dataset'],
            binding['snapshot_id'], require_objects=False)
        if release.manifest_sha256 != binding['manifest_sha256']:
            raise SnapshotError('pinned source manifest changed before hydration')
        if edge_state.is_file():
            response = subprocess.run([sys.executable, '-m', 'scripts.manage_packed_edge',
                '--sync-root', str(sync_root), '--materialized-root', str(materialized_root),
                '--state-root', str(edge_state.parent), 'use', binding['dataset'],
                '--snapshot-id', binding['snapshot_id'], '--retain-payload'],
                cwd=repo, capture_output=True, text=True, check=True)
            lease = json.loads(response.stdout)['lease']
        else:
            from stockagent.data_sync.materialized_cache import use_materialized_snapshot
            lease = use_materialized_snapshot(sync_root, materialized_root,
                binding['dataset'], snapshot_id=binding['snapshot_id'], verify_existing=True)
        if lease['snapshot_id'] != binding['snapshot_id'] or lease['manifest_sha256'] != binding['manifest_sha256']:
            raise SnapshotError('managed materialization differs from the pinned source')
        inputs[slot] = lease['target']
    return inputs
