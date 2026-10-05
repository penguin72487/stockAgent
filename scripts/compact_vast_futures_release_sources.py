#!/usr/bin/env python3
"""Remove only proven duplicate raw attachments from the recovered current build.

Keep canonical source bundles untouched. A data-only immutable reference view
shares their inodes; original artifact receipt paths become directory aliases.
Derived admission/valuation tables remain artifacts. No cold payload is removed.
"""
from __future__ import annotations

import argparse
import inspect
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.deliver_vast_futures_preparation import rename_no_replace
from scripts.restore_vast_futures_raw_sources import remote, WORK, RECEIPTS
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.materialized_cache import process_references
from stockagent.data_sync.remote_legacy_return import metadata_tree, real, signature

GROUPS = ('rules', 'specifications', 'terminal', 'rule_delta', 'terminal_delta')


def compact(root: Path, work: Path, expected: dict, *, apply: bool) -> dict:
    root, work = real(root), real(work)
    build = real(root / 'artifacts/markets/tw_futures_v8_margin_preparation/margin_components_current')
    source_root = real(root / 'data_tw_index_futures/preparation_sources')
    for key, relative in [('daily_sha256', 'daily/continuous_daily.parquet'),
                          ('rules_sha256', 'rules/rules.parquet')]:
        if sha256_file(build / 'release' / relative) != expected[key]:
            raise SnapshotError('current core release is not byte-identical to the original')
    manifest = source_root / 'margin_sources/source_manifest.json'
    if sha256_file(manifest) != expected['source_manifest_sha256']:
        raise SnapshotError('canonical source manifest changed')
    origins = {}
    for relative, row in sorted(json.loads(manifest.read_text())['files'].items()):
        if Path(relative).is_absolute() or '..' in Path(relative).parts:
            raise SnapshotError('unsafe canonical source inventory path')
        path = real(source_root / 'margin_sources' / relative)
        if not path.is_relative_to(source_root / 'margin_sources'):
            raise SnapshotError('canonical source receipt escapes its root')
        origins.setdefault(row['sha256'], path)
    for name in ['margin_repair_pending', 'margin_repair_inputs']:
        source = real(source_root / name)
        for path in sorted(source.rglob('*')):
            real(path)
            if path.is_file():
                origins.setdefault(sha256_file(path), path)
    views = source_root / 'current_release_source_refs' / expected['rules_sha256']
    plans, inventories, source_signatures, hashes = [], {}, {}, {}
    for area in [build / 'terms', build / 'release/rules']:
        sources = json.loads((area / 'manifest.json').read_text())['sources']
        for group in GROUPS:
            prefix = 'sources/' + group + '/'
            wanted = {r['path'][len(prefix):]: r['sha256'] for r in sources if r['path'].startswith(prefix)}
            if not wanted:
                continue
            path = area / 'sources' / group
            view = views / group
            if path.is_symlink():
                if path.resolve() != view or not view.is_dir():
                    raise SnapshotError('raw group has an unknown redirect')
                if any(sha256_file(view / name) != digest for name, digest in wanted.items()):
                    raise SnapshotError('existing canonical references no longer match raw receipts')
                continue
            real(path)
            tree = metadata_tree(path)
            if (any(r['kind'] == 'unsupported' or r.get('cross_filesystem') for r in tree['rows'])
                    or {r['path'] for r in tree['rows'] if r['kind'] == 'file'} != set(wanted)):
                raise SnapshotError('raw duplicate group has extra, missing or redirected names')
            if process_references(path):
                raise SnapshotError('raw duplicate group has an active reader/writer')
            blocks = 0
            for row in tree['rows']:
                if row['kind'] != 'file':
                    continue
                duplicate = path / row['path']
                actual = duplicate.lstat()
                origin = origins.get(wanted[row['path']])
                if origin is None or actual.st_nlink != 1:
                    raise SnapshotError('raw duplicate lacks retained source or has unknown inode aliases')
                real(origin)
                before = origin.lstat()
                if not stat.S_ISREG(before.st_mode) or before.st_dev != actual.st_dev:
                    raise SnapshotError('source alias would cross filesystem or use unsupported bytes')
                if origin not in hashes:
                    hashes[origin] = sha256_file(origin)
                    source_signatures[origin] = signature(before)
                if (hashes[origin] != wanted[row['path']] or sha256_file(duplicate) != wanted[row['path']]
                        or signature(origin.lstat()) != source_signatures[origin]
                        or signature(duplicate.lstat()) != row['signature']):
                    raise SnapshotError('original and duplicate full SHA/identity differ')
                blocks += actual.st_blocks * 512
            if group in inventories and inventories[group] != wanted:
                raise SnapshotError('release and intermediate raw source names/content differ')
            inventories[group] = wanted
            plans.append({'path': str(path), 'group': group, 'tree': tree,
                          'private_duplicate': str(work / 'current-raw-duplicates' / area.relative_to(build) / group),
                          'would_remove_duplicate_allocated_bytes': blocks})
    summary = {'state': 'exact_raw_duplicate_cleanup_plan', 'groups': len(plans),
               'duplicate_files': sum(len(p['tree']['rows']) - sum(r['kind'] != 'file' for r in p['tree']['rows']) for p in plans),
               'would_remove_duplicate_allocated_bytes': sum(p['would_remove_duplicate_allocated_bytes'] for p in plans),
               'canonical_source_payloads_deleted': 0, 'cold_deleted': False,
               'raw_view': str(views), 'expected_core': expected}
    if not apply:
        return summary
    work.mkdir(parents=True, mode=0o700, exist_ok=True)
    atomic_write_json(work / 'raw-duplicate-cleanup-intent.json', summary)
    if not plans:
        summary['state'] = 'raw_sources_already_canonical_references'
        return summary
    real(views)
    if views.exists() or views.is_symlink():
        raise FileExistsError('previous source reference view exists; preserve and inspect it')
    views.mkdir(parents=True, mode=0o700)
    for group, wanted in inventories.items():
        for name, digest in sorted(wanted.items()):
            origin = origins[digest]
            # Controlled link additions may alter nlink/ctime, never payload or
            # mtime/size. Check the complete source again before linking.
            observed = signature(origin.lstat())
            previous = source_signatures[origin]
            if observed != previous:
                raise SnapshotError('canonical source changed while creating references')
            target = views / group / name
            target.parent.mkdir(parents=True, exist_ok=True)
            os.link(origin, target, follow_symlinks=False)
            source_signatures[origin] = signature(origin.lstat())
    removed_files, removed_blocks = 0, 0
    for plan in plans:
        original, private = Path(plan['path']), Path(plan['private_duplicate'])
        if process_references(original) or metadata_tree(original) != plan['tree']:
            raise SnapshotError('duplicate group changed; preserve original and reference view')
        private.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        rename_no_replace(original, private)
        os.symlink(str(views / plan['group']), original, target_is_directory=True)
        for name, digest in inventories[plan['group']].items():
            if sha256_file(original / name) != digest:
                raise SnapshotError('canonical reference cannot reconstruct original raw bytes')
        if process_references(private) or metadata_tree(private) != plan['tree']:
            raise SnapshotError('private duplicate changed/is in use; preserve it')
        for row in plan['tree']['rows']:
            if row['kind'] != 'file':
                continue
            path = private / row['path']
            if signature(path.lstat()) != row['signature']:
                raise SnapshotError('duplicate changed immediately before removal; preserve remainder')
            removed_blocks += path.lstat().st_blocks * 512
            path.unlink()
            removed_files += 1
        for row in sorted((r for r in plan['tree']['rows'] if r['kind'] == 'directory'),
                          key=lambda r: len(Path(r['path']).parts), reverse=True):
            (private / row['path']).rmdir()
        private.rmdir()
        atomic_write_json(work / 'raw-duplicate-cleanup-progress.json',
                          {**summary, 'duplicate_files_removed': removed_files,
                           'duplicate_allocated_bytes_removed': removed_blocks})
    for key, relative in [('daily_sha256', 'daily/continuous_daily.parquet'),
                          ('rules_sha256', 'rules/rules.parquet')]:
        if sha256_file(build / 'release' / relative) != expected[key]:
            raise SnapshotError('core release changed during raw reference migration')
    summary.update(state='raw_sources_only_in_canonical_data_with_artifact_reference_aliases',
                   duplicate_files_removed=removed_files, duplicate_allocated_bytes_removed=removed_blocks,
                   new_payload_copies_created=0)
    atomic_write_json(work / 'raw-duplicate-cleanup-verified.json', summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    original = json.loads((ROOT / 'artifacts/operations/futures_margin_prepare_20261004/remote_build_acceptance.json').read_text())
    expected = {k: original[k] for k in ['daily_sha256', 'rules_sha256', 'source_manifest_sha256']}
    code = '''
import os,stat,ctypes
from pathlib import Path
from stockagent.data_sync.desync_snapshots import SnapshotError,atomic_write_json,sha256_file
from stockagent.data_sync.materialized_cache import process_references
'''
    code += inspect.getsource(rename_no_replace) + '\n' + inspect.getsource(compact)
    code += f'\nGROUPS={GROUPS!r}\nprint(json.dumps(compact(Path({str(ROOT)!r}),Path({str(WORK)!r}),{expected!r},apply={args.apply!r})))\n'
    result = remote(code)
    atomic_write_json(RECEIPTS / ('remote-raw-duplicate-cleanup-verified.json' if args.apply else 'remote-raw-duplicate-cleanup-plan.json'), result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
