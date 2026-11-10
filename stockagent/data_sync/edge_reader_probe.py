"""Bounded read-only probe of one installed compute-node edge reader."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

CONTRACT = 'stockagent.edge_reader_probe.v1'
HASH = re.compile(r'^[0-9a-f]{64}$')
SLUG = re.compile(r'^[a-z0-9][a-z0-9-]{0,127}$')


def _file(path: Path, maximum=64*1024) -> bytes:
    if path.is_symlink() or not path.is_file() or path.resolve(strict=True) != path:
        raise ValueError('edge reader metadata is unavailable or redirected')
    before = path.stat()
    if before.st_size > maximum:
        raise ValueError('edge reader metadata exceeds its bound')
    body = path.read_bytes()
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('edge reader metadata changed')
    return body


def probe(request: dict, *, control_path=Path('/var/lib/stockagent-packed-edge/reader-release.control'),
          runner=subprocess.run) -> dict:
    if not isinstance(request, dict) or set(request) != {'repo', 'manifest_sha256', 'wrapper_sha256', 'dataset'}:
        raise ValueError('edge reader probe request differs')
    repo, digest, wrapper_digest, dataset = (request[k] for k in ('repo', 'manifest_sha256', 'wrapper_sha256', 'dataset'))
    if (not isinstance(repo, str) or not Path(repo).is_absolute() or Path(repo).resolve(strict=True) != Path(repo)
            or not isinstance(digest, str) or not HASH.fullmatch(digest)
            or not isinstance(wrapper_digest, str) or not HASH.fullmatch(wrapper_digest)
            or not isinstance(dataset, str) or not SLUG.fullmatch(dataset)):
        raise ValueError('edge reader probe identity is invalid')
    repo = Path(repo)
    wrapper = repo/'scripts/run_data_cache.sh'
    if hashlib.sha256(_file(wrapper)).hexdigest() != wrapper_digest:
        raise ValueError('edge reader wrapper identity changed')
    control = Path(control_path).absolute()
    data = _file(control, maximum=4096)
    info = control.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError('edge reader control permissions differ')
    release = repo/'artifacts/code_releases'/('stockagent-edge-reader-v3-'+digest[:16])
    if data != f'{release} {digest} {repo}\n'.encode():
        raise ValueError('edge reader control identity differs')
    if hashlib.sha256(_file(release/'reader-manifest.sha256')).hexdigest() != digest:
        raise ValueError('edge reader manifest identity differs')
    result = runner(['bash', str(wrapper), 'reader-status', dataset], cwd=repo,
                    capture_output=True, text=True, timeout=25, check=True)
    value = json.loads(result.stdout)
    if (not isinstance(value, dict) or value.get('mode') != 'index-only-reader-status'
            or value.get('dataset') != dataset or value.get('schema_version') != 3
            or not isinstance(value.get('manifest_sha256'), str) or not HASH.fullmatch(value['manifest_sha256'])
            or value.get('payload_fetched') is not False or value.get('payload_verified') is not False):
        raise ValueError('edge reader metadata status differs')
    return {'contract': CONTRACT, 'state': 'ready', 'reader_manifest_sha256': digest,
            'wrapper_sha256': wrapper_digest, 'probe_dataset': dataset,
            'probe_dataset_manifest_sha256': value['manifest_sha256'], 'schema_version': 3,
            'payload_fetched': False, 'payload_verified': False}


if __name__ == '__main__':
    try:
        print(json.dumps(probe(json.load(sys.stdin)), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
