"""Maintain source-pinned methods; never auto-hydrate panels or start training."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from stockagent.data_sync.training_recipes import refresh_recipes, build_recipe, materialize_recipe_inputs
from stockagent.data_sync.desync_snapshots import atomic_write_bytes


def packaged_cookbook(content: str) -> str:
    """The exported method inventory lives in catalog/, not the source audit dir."""
    content = re.sub(r'\(\.\./artifacts/data_quality/[^/]+/inventory/([^/)]+)\)',
                     r'(catalog/\1)', content)
    return re.sub(
        r'\[[^]]+\]\((minute_validation_and_recipes_[^)#]+\.md)(?:#[^)]*)?\)',
        r'來源 repo 的 `docs/\1`（日期驗收報告不隨方法包複製）', content)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('refresh', 'trial', 'build'))
    parser.add_argument('--sync-root', type=Path, default=Path('/srv/stockagent-packed'))
    parser.add_argument('--output-root', type=Path,
                        default=ROOT/'artifacts/operations/training_dataset_recipes')
    parser.add_argument('--recipe', type=Path)
    parser.add_argument('--inputs-json', type=Path)
    parser.add_argument('--materialize-inputs', action='store_true',
                        help='Explicitly hydrate only exact recipe inputs through the existing managed cache.')
    parser.add_argument('--materialized-root', type=Path, default=Path('/srv/stockagent-packed-materialized'))
    parser.add_argument('--parameters-json', type=Path)
    parser.add_argument('--receipt', type=Path)
    parser.add_argument('--build-output', type=Path)
    parser.add_argument('--remote-run-report', type=Path, action='append', default=[])
    parser.add_argument('--source-lock-fd', type=int,
                        help='Borrow the canonical metadata owner lock from the shell wrapper.')
    args = parser.parse_args()
    if args.mode == 'refresh':
        if args.materialize_inputs:
            parser.error('refresh is metadata only; hydration requires an explicit trial/build')
        from scripts.inventory_training_dataset_recipes import inventory
        from scripts.publish_data_releases import _source_coordination_lock
        output = args.output_root.resolve()
        entry = {'dataset': 'training-dataset-recipes',
                 'source': str(output), 'source_coordination_lock_scope': 'source_parent',
                 'source_coordination_lock': output.name+'/refresh.lock'}
        with _source_coordination_lock(entry, inherited_fd=args.source_lock_fd):
            inventory(ROOT, args.output_root/'catalog', remote_reports=args.remote_run_report,
                      include_minutes=False)
            cookbook = ROOT/'docs/training_dataset_rebuild_methods.md'
            if cookbook.is_file():
                content = packaged_cookbook(cookbook.read_text()).encode()
                dest = args.output_root/'training_dataset_rebuild_methods.md'
                if not dest.is_file() or dest.read_bytes() != content:
                    atomic_write_bytes(dest, content)
            result = refresh_recipes(repo=ROOT, sync_root=args.sync_root, output_root=args.output_root)
    else:
        if args.source_lock_fd is not None:
            parser.error('source-lock-fd belongs to metadata refresh, not trial/build')
        if not all((args.recipe, args.receipt)) or bool(args.inputs_json) == args.materialize_inputs:
            parser.error('recipe and receipt plus exactly one of inputs-json/materialize-inputs are required')
        inputs = (materialize_recipe_inputs(args.recipe, repo=ROOT, sync_root=args.sync_root,
                    materialized_root=args.materialized_root) if args.materialize_inputs
                  else json.loads(args.inputs_json.read_text()))
        result = build_recipe(args.recipe, repo=ROOT, sync_root=args.sync_root,
            input_paths=inputs,
            parameters=json.loads(args.parameters_json.read_text()) if args.parameters_json else {},
            receipt_path=args.receipt, output_root=args.build_output, trial=args.mode=='trial')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
