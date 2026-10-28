"""Run a single futures-partition SDK call under the existing FinLab owner.

The isolated SDK shares the owner's native authenticated session; it never
logs in or takes an account lock. Callers retain quota, timing and disk gates.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

from downloader.artifact_io import atomic_write_json

SDK_NAME = 'finlab-2.2.18-cp312-cp312-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl'
SDK_SHA256 = '70fdd2e3fcee81332510bf81abd00d9703f1ed799e1d9446fe4a577133a10a22'


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def validated_overlay(root: Path) -> Path | None:
    """Reject missing, modified or extra importable SDK files before any call."""
    location = root / 'tooling/finlab_futures_sdk_2_2_18_v1'
    wheel, overlay = location / SDK_NAME, location / 'overlay'
    try:
        if wheel.is_symlink() or _sha256(wheel.read_bytes()) != SDK_SHA256 or not overlay.is_dir():
            return None
        with zipfile.ZipFile(wheel) as archive:
            names = [n for n in archive.namelist() if n.startswith('finlab/') and not n.endswith('/')]
            names.append('finlab-2.2.18.dist-info/METADATA')
            if len(names) != len(set(names)):
                return None
            expected = set(names)
            for name in names:
                path = overlay / name
                if path.is_symlink() or not path.is_file() or _sha256(path.read_bytes()) != _sha256(archive.read(name)):
                    return None
        for path in (overlay / 'finlab').rglob('*'):
            if path.is_symlink():
                return None
            if path.is_file() and (path.suffix in {'.py', '.pyc', '.so', '.pyd', '.pth'}):
                if path.relative_to(overlay).as_posix() not in expected:
                    return None
        return overlay
    except (OSError, ValueError, KeyError, zipfile.BadZipFile):
        return None


class PartitionFailure(Exception):
    def __init__(self, category: str):
        super().__init__(category)
        self.category = category


def fetch_with_owner_session(root: Path, key: str, day: date, *, overlay: Path) -> dict:
    """Copy the live owner's native session through a private stdin pipe only."""
    from finlab import get_token
    from finlab.auth import get_session
    from scripts.download_finlab_intraday import _partition_paths, _stored_receipt, record_failed_partition

    get_token()  # Same refresh path used by the stock-minute worker pool.
    session = get_session()
    if not session:
        record_failed_partition(root, key, day, RuntimeError('unauthenticated: owner session unavailable'))
        raise PartitionFailure('authentication_failed')
    repo = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(filter(None, (str(overlay), str(repo), env.get('PYTHONPATH', ''))))
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    request = json.dumps({'root': str(root.resolve()), 'key': key, 'day': day.isoformat(), 'session': session})
    try:
        child = subprocess.run([sys.executable, '-B', '-m', 'scripts.finlab_futures_sdk_bridge'],
            input=request, text=True, capture_output=True, env=env, cwd=repo, timeout=170, check=False)
        payload = json.loads(child.stdout)
        if child.returncode == 0 and payload.get('sdk_version') == '2.2.18':
            if payload.get('failure'):
                raise PartitionFailure(payload['failure'])
            receipt = _stored_receipt(_partition_paths(root, key, day)[0], root, key, day, verify_hash=True)
            if receipt is not None:
                return receipt
    except PartitionFailure:
        raise
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError, TypeError, ValueError):
        pass
    # A child can exit after persisting data but before returning its reply.
    receipt_path, attempt_path = _partition_paths(root, key, day)
    receipt = _stored_receipt(receipt_path, root, key, day, verify_hash=True)
    if receipt is not None:
        return receipt
    atomic_write_json(attempt_path, {'dataset': key, 'trade_date': day.isoformat(),
        'status': 'source_outcome_unknown', 'attempted_at_utc': datetime.now(UTC).isoformat(),
        'exception_class': 'FuturesSdkBridgeIncomplete', 'message_retained': False})
    raise PartitionFailure('source_outcome_unknown')
