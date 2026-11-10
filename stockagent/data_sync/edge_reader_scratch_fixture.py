"""Create a small isolated edge-owner fixture under an exact temporary root."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shutil
import sys

TMP = re.compile(r'^/tmp/stockagent-edge-recovery\.[A-Za-z0-9]{8}$')


def make(request: dict) -> dict:
    if (not isinstance(request, dict) or set(request) != {'temporary', 'canonical_repo'}
            or not isinstance(request['temporary'], str) or not TMP.fullmatch(request['temporary'])
            or not isinstance(request['canonical_repo'], str) or not request['canonical_repo'].startswith('/')):
        raise ValueError('scratch fixture scope differs')
    temporary, canonical = Path(request['temporary']), Path(request['canonical_repo'])
    if (temporary.is_symlink() or temporary.resolve(strict=True) != temporary or not temporary.is_dir()
            or canonical.is_symlink() or canonical.resolve(strict=True) != canonical):
        raise ValueError('scratch fixture root is unavailable')
    config = canonical/'configs/data_sync/cache_capacity.json'
    if config.is_symlink() or not config.is_file() or config.stat().st_size > 1024*1024:
        raise ValueError('canonical capacity policy is unavailable')
    repo, state = temporary/'scratch-repo', temporary/'scratch-state'
    repo.mkdir(mode=0o700); state.mkdir(mode=0o700)
    for path in (repo/'configs/data_sync', repo/'scripts', repo/'artifacts/code_releases'):
        path.mkdir(mode=0o700, parents=True)
    shutil.copyfile(config, repo/'configs/data_sync/cache_capacity.json')
    (repo/'scripts/runtime_env.sh').write_text('run_fintech_python() { python3 "$@"; }\n')
    baseline = b'#!/usr/bin/env bash\nexit 0\n'
    (repo/'scripts/run_data_cache.sh').write_bytes(baseline)
    (repo/'scripts/run_data_cache.sh').chmod(0o700)
    (state/'state.json').write_text('{}\n')
    (state/'operation.lock').touch(mode=0o600)
    return {'contract': 'stockagent.edge_reader_scratch_fixture.v1', 'repo': str(repo),
            'state_root': str(state), 'baseline_sha256': hashlib.sha256(baseline).hexdigest(),
            'canonical_owner_changed': False}


if __name__ == '__main__':
    try:
        print(json.dumps(make(json.load(sys.stdin)), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError):
        print('edge reader scratch fixture failed', file=sys.stderr)
        raise SystemExit(1)
