#!/usr/bin/env python3
"""Capture existing Conda roles or create/rebuild isolated roles with mamba.

The active native interpreter is observed, never updated. A fresh role is
created from a declaration or exact explicit package URLs and hashes. Explicit
locks are platform-specific; observations of pip overlays are separate evidence.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock


def mamba_binary() -> str:
    candidate = os.environ.get('FINTECH_MAMBA_BIN') or shutil.which('mamba')
    if not candidate or not Path(candidate).is_file():
        raise ValueError('Miniforge mamba is required; select FINTECH_MAMBA_BIN')
    if Path(candidate).name != 'mamba':
        raise ValueError('this workflow requires mamba')
    return str(Path(candidate).absolute())


def validate_fresh_role(prefix: Path) -> None:
    if prefix.resolve() == Path(sys.prefix).resolve():
        raise ValueError('a role cannot replace the active native runtime')
    if prefix.exists() or prefix.is_symlink():
        raise ValueError('role output must be a new environment; existing environments remain their owner')
    if prefix.parent.resolve() != prefix.parent.absolute():
        raise ValueError('role parent is redirected')


def run(argv: list[str], *, timeout: int = 900) -> str:
    completed = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if completed.returncode:
        # Package-manager diagnostics contain no database credentials; retain
        # their actual failure so unavailable pins are distinguishable.
        raise RuntimeError(f'{Path(argv[0]).name} exit {completed.returncode}: {completed.stderr[-3000:]}')
    return completed.stdout


def conda_inventory(prefix: Path) -> list[dict]:
    metadata = prefix / 'conda-meta'
    if not metadata.is_dir():
        raise ValueError('selected prefix is not managed by Conda/mamba')
    rows = []
    for path in sorted(metadata.glob('*.json')):
        package = json.loads(path.read_bytes())
        rows.append({key: package.get(key) for key in
                     ('name', 'version', 'build', 'subdir', 'url', 'sha256', 'md5')})
    return rows


def snapshot(prefix: Path, output: Path, manager: str) -> dict:
    inventory = conda_inventory(prefix)
    # Mamba 2.x list output is human-readable and omits @EXPLICIT. Construct
    # the standard explicit file from installed records, including hashes,
    # rather than treating a display banner as a package requirement.
    explicit = explicit_lock(inventory)
    atomic_write_text(output / 'conda-explicit.txt', explicit)
    packages = json.loads(run([str(prefix / 'bin/python'), '-c',
        'import importlib.metadata as m,json;print(json.dumps(sorted([{\"name\":d.metadata[\"Name\"],\"version\":d.version,\"installer\":(d.read_text(\"INSTALLER\") or \"\").strip(),\"direct_url\":d.read_text(\"direct_url.json\")} for d in m.distributions()],key=lambda x:(x[\"name\"] or \"\").lower())))']))
    atomic_write_json(output / 'conda-packages.json', inventory)
    atomic_write_json(output / 'python-packages.json', packages)
    return {'conda_packages': len(inventory), 'python_distributions': len(packages),
            'conda_explicit_sha256': hashlib.sha256(explicit.encode()).hexdigest(),
            'pip_overlay_distributions': sum(row['installer'] == 'pip' for row in packages),
            'all_conda_packages_have_content_hash': all(row['sha256'] or row['md5'] for row in inventory)}


def explicit_lock(inventory: list[dict]) -> str:
    if not inventory:
        raise ValueError('empty Conda package inventory')
    rows = ['# Platform-specific installed Conda packages; pip overlays are separate.', '@EXPLICIT']
    for package in inventory:
        url = package.get('url') or ''
        checksum = package.get('sha256') or package.get('md5') or ''
        if not url.startswith('https://conda.anaconda.org/') or len(checksum) not in (32, 64):
            raise ValueError('installed package lacks a public URL/content hash')
        rows.append(url.split('#', 1)[0] + '#' + checksum)
    return '\n'.join(rows) + '\n'


def create_role(prefix: Path, specification: Path, output: Path, *, explicit: bool = False) -> dict:
    validate_fresh_role(prefix)
    specification = specification.resolve(strict=True)
    body = specification.read_bytes()
    if explicit and b'@EXPLICIT' not in body:
        raise ValueError('rebuild requires an explicit platform lock')
    if output.exists():
        raise ValueError('keep the previous environment receipt')
    manager = mamba_binary()
    before = runtime_identity()
    output.mkdir(parents=True, mode=0o700)
    atomic_write_json(output / 'native-before.json', before)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    argv = [manager, 'create'] if explicit else [manager, 'env', 'create']
    argv += ['--yes', '--prefix', str(prefix), '--file', str(specification)]
    if not explicit:
        argv += ['--strict-channel-priority']
    started = time.perf_counter()
    log = run(argv)
    atomic_write_text(output / 'creation.log', log)
    if specification.read_bytes() != body:
        raise ValueError('environment specification changed during creation')
    proof = snapshot(prefix, output, manager)
    after = runtime_identity()
    if validate_runtime_lock(before, after):
        raise ValueError('native runtime changed during isolated role creation')
    atomic_write_json(output / 'native-after.json', after)
    receipt = {'schema_version': 1, 'state': 'accepted', 'manager': manager,
               'mamba_version': run([manager, '--version']).strip(),
               'prefix': str(prefix), 'specification': str(specification),
               'specification_sha256': hashlib.sha256(body).hexdigest(),
               'explicit_rebuild': explicit, 'native_runtime_unchanged': True,
               'complete_wall_seconds': time.perf_counter() - started,
               'observed_at_utc': datetime.now(timezone.utc).isoformat(), **proof,
               'scope': 'isolated mamba role creation and package capture; import/workload acceptance is separate'}
    atomic_write_json(output / 'acceptance.json', receipt)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('capture', 'create', 'rebuild'))
    parser.add_argument('--prefix', type=Path)
    parser.add_argument('--specification', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.operation == 'capture':
        prefix = (args.prefix or Path(sys.prefix)).absolute()
        args.output.mkdir(parents=True, mode=0o700, exist_ok=False)
        receipt = {'state': 'captured', 'prefix': str(prefix),
                   'runtime': runtime_identity(), **snapshot(prefix, args.output, mamba_binary()),
                   'scope': 'package inventory; does not prove pip overlays or compiled CUDA artifacts can be rebuilt'}
        atomic_write_json(args.output / 'capture.json', receipt)
    else:
        if args.prefix is None or args.specification is None:
            parser.error('role creation requires --prefix and --specification')
        receipt = create_role(args.prefix.absolute(), args.specification, args.output,
                              explicit=args.operation == 'rebuild')
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
