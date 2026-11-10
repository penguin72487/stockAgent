"""Freeze a tiny, source-pinned edge reader for compute-node recovery.

The bundle contains code and a pinned dependency only, never credentials or
market payload. Building it does not change the compute node or any D head.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

from scripts import install_isolated_edge_reader as installer
from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = 'stockagent.edge_reader_recovery_policy.v1'
BUNDLE = 'stockagent.edge_reader_recovery_bundle.v1'
POLICY_KEYS = {'contract', 'enabled', 'source_identity_sha256', 'reader_manifest_sha256',
               'wrapper_sha256', 'installer_sha256', 'baseline_wrapper_sha256', 'dataset'}


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def policy(path: Path, activation_path: Path) -> dict:
    value = json.loads(installer._regular(path, maximum=16384))
    activation = json.loads(installer._regular(activation_path, maximum=16384))
    fields = POLICY_KEYS - {'contract', 'enabled', 'dataset'}
    if (not isinstance(value, dict) or set(value) != POLICY_KEYS or value.get('contract') != CONTRACT
            or type(value.get('enabled')) is not bool
            or any(not isinstance(value.get(key), str) or not installer.HASH_PATTERN.fullmatch(value[key])
                   for key in fields)
            or not isinstance(value.get('dataset'), str)
            or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', value['dataset'])
            or not isinstance(activation, dict)
            or activation.get('contract') != 'stockagent.edge_reader_activation_policy.v1'
            or type(activation.get('enabled')) is not bool
            or activation.get('remote_reader_manifest_sha256') != value['reader_manifest_sha256']
            or activation.get('remote_wrapper_sha256') != value['wrapper_sha256']
            or activation.get('probe_dataset') != value['dataset']):
        raise SnapshotError('edge reader recovery policy does not match the enrolled reader')
    return value


def _metadata(value: dict, files: int) -> dict:
    return {'contract': BUNDLE, 'source_identity_sha256': value['source_identity_sha256'],
            'reader_manifest_sha256': value['reader_manifest_sha256'], 'wrapper_sha256': value['wrapper_sha256'],
            'installer_sha256': value['installer_sha256'], 'baseline_wrapper_sha256': value['baseline_wrapper_sha256'],
            'dataset': value['dataset'], 'reader_file_count': files,
            'market_payload_included': False, 'credentials_included': False}


def verify(bundle: Path, value: dict) -> dict:
    bundle = Path(bundle).absolute()
    if (bundle.is_symlink() or not bundle.is_dir() or bundle.resolve(strict=True) != bundle
            or bundle.stat().st_mode & 0o077):
        raise SnapshotError('edge reader recovery bundle is unavailable or not private')
    files = installer._map(bundle/'source', value['source_identity_sha256'])
    if digest(installer._manifest(files)) != value['reader_manifest_sha256']:
        raise SnapshotError('edge reader recovery members differ from the pinned release')
    if (digest(installer._regular(bundle/'installer.py')) != value['installer_sha256']
            or digest(installer._regular(bundle/'wrapper.sh')) != value['wrapper_sha256']):
        raise SnapshotError('edge reader recovery installer or wrapper differs')
    metadata = json.loads(installer._regular(bundle/'bundle.json', maximum=16384))
    if metadata != _metadata(value, len(files)):
        raise SnapshotError('edge reader recovery metadata differs')
    return metadata


def build(repo: Path, workspace: Path, site: Path, value: dict) -> dict:
    repo, workspace, site = (Path(p).absolute() for p in (repo, workspace, site))
    for path in (repo, workspace, site):
        if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
            raise SnapshotError('edge reader recovery input root is unavailable or redirected')
    workspace_stat = workspace.stat()

    def workspace_unchanged():
        current = workspace.stat()
        if (workspace.is_symlink() or workspace.resolve(strict=True) != workspace
                or (current.st_dev, current.st_ino) != (workspace_stat.st_dev, workspace_stat.st_ino)):
            raise SnapshotError('edge reader recovery workspace changed during build')

    source_bytes = installer._regular(repo/'configs/data_sync/edge_reader_v3_source_sha.json', maximum=256*1024)
    source_map = json.loads(source_bytes)
    if (not isinstance(source_map, dict) or not source_map
            or digest(json.dumps(source_map, sort_keys=True, separators=(',', ':')).encode()) != value['source_identity_sha256']):
        raise SnapshotError('edge reader recovery source identity differs')
    manifest = installer._regular(repo/'configs/data_sync/edge_reader_v3_manifest.sha256', maximum=256*1024)
    if digest(manifest) != value['reader_manifest_sha256']:
        raise SnapshotError('edge reader recovery release manifest differs')
    members: dict[str, bytes] = {}
    for line in manifest.decode('ascii').splitlines():
        checksum, name = line.split('  ', 1)
        if (not installer.HASH_PATTERN.fullmatch(checksum) or not installer._safe_name(name)
                or name in members or name.startswith('vendor/') and not name.startswith(
                    ('vendor/ijson/', 'vendor/ijson.libs/', 'vendor/ijson-3.6.0.dist-info/'))):
            raise SnapshotError('edge reader recovery release member is invalid')
        path = site/name[len('vendor/'):] if name.startswith('vendor/') else repo/name
        body = installer._regular(path)
        if digest(body) != checksum or (not name.startswith('vendor/') and source_map.get(name) != checksum):
            raise SnapshotError('edge reader recovery input changed since enrollment')
        members[name] = body
    if set(source_map) != {name for name in members if not name.startswith('vendor/')}:
        raise SnapshotError('edge reader recovery source manifest is incomplete')
    if len(members) > 1024 or sum(map(len, members.values())) > 64*1024**2:
        raise SnapshotError('edge reader recovery bundle exceeds its bound')
    wrapper = installer._regular(repo/'scripts/run_data_cache.sh')
    deployer = installer._regular(repo/'scripts/install_isolated_edge_reader.py')
    if digest(wrapper) != value['wrapper_sha256'] or digest(deployer) != value['installer_sha256']:
        raise SnapshotError('edge reader recovery deployment tools changed')
    root = workspace/'migration/edge-reader-recovery-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    workspace_unchanged()
    if (root.is_symlink() or root.resolve(strict=True) != root or root.stat().st_mode & 0o077
            or root.stat().st_dev != workspace_stat.st_dev):
        raise SnapshotError('edge reader recovery destination is unavailable or not private')
    destination = root/value['reader_manifest_sha256']
    if destination.exists() or destination.is_symlink():
        return {'state': 'already_built', 'bundle': str(destination), **verify(destination, value)}
    temporary = Path(tempfile.mkdtemp(prefix='.reader-bundle-', dir=root))
    temporary_stat = temporary.stat()

    def temporary_unchanged():
        workspace_unchanged()
        current = temporary.stat()
        if (temporary.is_symlink() or temporary.resolve(strict=True) != temporary
                or (current.st_dev, current.st_ino) != (temporary_stat.st_dev, temporary_stat.st_ino)):
            raise SnapshotError('edge reader recovery temporary generation changed')

    try:
        for name, body in members.items():
            temporary_unchanged()
            output = temporary/'source'/name
            output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            output.write_bytes(body)
        temporary_unchanged()
        (temporary/'source/edge-source-sha.json').write_bytes(source_bytes)
        (temporary/'installer.py').write_bytes(deployer)
        (temporary/'wrapper.sh').write_bytes(wrapper)
        metadata = _metadata(value, len(members))
        (temporary/'bundle.json').write_text(json.dumps(metadata, sort_keys=True)+'\n')
        verify(temporary, value)
        temporary_unchanged()
        os.rename(temporary, destination)
        installer._fsync_dir(root)
    finally:
        try:
            if temporary.exists():
                temporary_unchanged()
                shutil.rmtree(temporary)
        except (OSError, SnapshotError):
            pass  # Never remove a name after its filesystem generation changed.
    return {'state': 'built', 'bundle': str(destination), **verify(destination, value)}


def default_site() -> Path:
    try:
        distribution = importlib.metadata.distribution('ijson')
    except importlib.metadata.PackageNotFoundError as error:
        raise SnapshotError('the enrolled ijson release is unavailable locally') from error
    if distribution.version != '3.6.0':
        raise SnapshotError('the enrolled ijson release is unavailable locally')
    return Path(distribution.locate_file('')).resolve(strict=True)
