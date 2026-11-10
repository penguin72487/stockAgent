#!/usr/bin/env python3
"""Record precise hash-scope corrections without rewriting older receipts."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import packed_snapshots as p, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, write_immutable_json

CONTRACT = 'stockagent.v3_retirement_scope_correction.v1'


def audit(workspace: Path, *, apply=False) -> list[dict]:
    if type(apply) is not bool:
        raise SnapshotError('scope correction apply must be explicit')
    root = Path(workspace).absolute()/'migration/source-local-exact-repack'
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('scope correction receipt root is unavailable')
    paths = list(root.glob('*/*.json'))
    if len(paths) > 100_000:
        raise SnapshotError('scope correction receipt count exceeds its bound')
    output = root/'v3-retirement-scope-corrections'
    if output.exists() or output.is_symlink():
        if output.is_symlink() or not output.is_dir() or output.resolve(strict=True) != output:
            raise SnapshotError('scope correction output is redirected')
    rows = []
    for path in paths:
        if (path.parent == output or path.is_symlink() or path.parent.is_symlink() or not path.is_file()
                or not re.fullmatch(r'[0-9a-f]{64}\.json', path.name) or path.stat().st_size > 1024*1024):
            continue
        try:
            name = p.validate_slug(path.parent.name, 'dataset')
            before = path.stat()
            raw = path.read_bytes()
            after = path.stat()
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                continue
            receipt = json.loads(raw)
            if not isinstance(receipt, dict):
                continue
            retirement = receipt.get('retirement')
            if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
                    or receipt.get('dataset') != name or receipt.get('new_manifest_sha256') != path.stem
                    or receipt.get('packed_format') != 'content-alias-v3'
                    or receipt.get('unique_content_sha_streamed') is not True
                    or receipt.get('all_original_occurrences_reread') is not False
                    or receipt.get('old_release_removed') is not True or not isinstance(retirement, dict)
                    or retirement.get('state') != 'completed'
                    or retirement.get('old_source_member_hashes_checked_while_packing') is not True):
                continue
            old = p._validate_hash(receipt.get('old_manifest_sha256'), 'old manifest SHA')
            original_sha = hashlib.sha256(raw).hexdigest()
            correction = {'contract': CONTRACT, 'dataset': name, 'old_manifest_sha256': old,
                          'new_manifest_sha256': path.stem, 'original_receipt_sha256': original_sha,
                          'original_retirement_observed_at_epoch': retirement.get('observed_at_epoch'),
                          'corrected_old_source_member_hashes_checked_while_packing': False,
                          'unique_content_sha_streamed': True, 'all_original_occurrences_reread': False,
                          'original_receipt_modified': False, 'independent_D_readback': False,
                          'NAS_accepted': False}
            destination = output/(name+'-'+original_sha+'.json')
        except (OSError, ValueError, TypeError, KeyError, SnapshotError):
            continue
        if apply:
            write_immutable_json(destination, correction)
        rows.append(correction)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    rows = audit(Path(config['mount_point']), apply=args.apply)
    print(json.dumps({'contract': CONTRACT, 'apply': args.apply, 'corrections': rows,
                      'original_receipts_modified': False}, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
