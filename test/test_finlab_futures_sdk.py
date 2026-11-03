from datetime import date
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data import finlab_futures_sdk as adapter


def test_overlay_must_match_wheel_and_cannot_add_importable_code(tmp_path, monkeypatch):
    folder = tmp_path / 'tooling/finlab_futures_sdk_2_2_18_v1'
    folder.mkdir(parents=True)
    wheel = folder / 'test.whl'
    members = {'finlab/__init__.py': b'pass\n',
               'finlab-2.2.18.dist-info/METADATA': b'Version: 2.2.18\n'}
    with zipfile.ZipFile(wheel, 'w') as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    overlay = folder / 'overlay'
    for name, data in members.items():
        path = overlay / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    monkeypatch.setattr(adapter, 'SDK_NAME', 'test.whl')
    monkeypatch.setattr(adapter, 'SDK_SHA256', hashlib.sha256(wheel.read_bytes()).hexdigest())
    assert adapter.validated_overlay(tmp_path) == overlay
    (overlay / 'finlab/extra.py').write_text('pass\n')
    assert adapter.validated_overlay(tmp_path) is None
    (overlay / 'finlab/extra.py').unlink()
    (overlay / 'finlab/__init__.py').write_text('changed\n')
    assert adapter.validated_overlay(tmp_path) is None


def test_private_bridge_reuses_owner_session_and_reads_native_receipt(tmp_path, monkeypatch):
    from scripts.download_finlab_intraday import _partition_paths
    import finlab
    import finlab.auth

    key, day = 'tw_futures_minute:I5F_201611', date(2016, 11, 8)
    calls = []
    monkeypatch.setattr(finlab, 'get_token', lambda: None)
    monkeypatch.setattr(finlab.auth, 'get_session', lambda: {'session_id': 'private-fixture'})

    def child(argv, **kwargs):
        calls.append((argv, kwargs))
        payload = json.loads(kwargs['input'])
        assert payload['session']['session_id'] == 'private-fixture'
        assert 'private-fixture' not in repr(argv) and 'private-fixture' not in repr(kwargs['env'])
        receipt = {'dataset': key, 'trade_date': day.isoformat(), 'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(tmp_path, key, day)[0], receipt)
        return type('Result', (), {'returncode': 0, 'stdout': '{"sdk_version":"2.2.18"}'})()

    monkeypatch.setattr(adapter.subprocess, 'run', child)
    receipt = adapter.fetch_with_owner_session(tmp_path, key, day, overlay=tmp_path / 'overlay')
    assert receipt['status'] == 'verified_no_trade' and len(calls) == 1
    assert calls[0][1]['timeout'] == 170


def test_unknown_bridge_outcome_is_evidence_not_a_fake_empty_partition(tmp_path, monkeypatch):
    import finlab
    import finlab.auth
    from scripts.download_finlab_intraday import _checked_this_cycle, _partition_paths
    from datetime import UTC, datetime

    key, day = 'tw_futures_minute:I5F_201611', date(2016, 11, 8)
    monkeypatch.setattr(finlab, 'get_token', lambda: None)
    monkeypatch.setattr(finlab.auth, 'get_session', lambda: {'session_id': 'private-fixture'})
    monkeypatch.setattr(adapter.subprocess, 'run', lambda *a, **kw:
        type('Result', (), {'returncode': 1, 'stdout': ''})())
    with pytest.raises(adapter.PartitionFailure, match='source_outcome_unknown'):
        adapter.fetch_with_owner_session(tmp_path, key, day, overlay=tmp_path / 'overlay')
    receipt, attempt = _partition_paths(tmp_path, key, day)
    assert not receipt.exists()
    assert json.loads(attempt.read_text())['status'] == 'source_outcome_unknown'
    assert _checked_this_cycle(attempt, 'attempted_at_utc', datetime.now(UTC))


@pytest.mark.parametrize('workspace_mode', [False, True])
def test_isolated_sdk_bridge_writes_a_native_receipt_without_login(tmp_path, workspace_mode):
    """Exercise the real versioned bridge; the provider response is a local fixture."""
    overlay = adapter.validated_overlay(Path('data_finlab'))
    if overlay is None:
        pytest.skip('verified optional FinLab futures SDK overlay is not installed')
    repo = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join((str(overlay.resolve()), str(repo)))
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env['FINLAB_TEST_WORKSPACE'] = str(tmp_path / 'intraday/objects/futures-workspace-v1') if workspace_mode else ''
    if workspace_mode:
        Path(env['FINLAB_TEST_WORKSPACE']).mkdir(parents=True)
    fixture = r'''
import os
from pathlib import Path
import pandas as pd
from finlab import data
from finlab.data import context
from scripts import finlab_futures_sdk_bridge as bridge
if os.environ['FINLAB_TEST_WORKSPACE']:
    from stockagent.data_sync import workspace_volume
    workspace_volume.FINLAB_FUTURES_ALIAS = Path(os.environ['FINLAB_TEST_WORKSPACE'])
    workspace_volume.check_finlab_futures = lambda: {'target': os.environ['FINLAB_TEST_WORKSPACE']}
def get(key, **kwargs):
    assert key == 'tw_futures_minute:I5F_201611'
    assert kwargs['start'] == kwargs['end'] == '2016-11-08'
    assert kwargs['force_download'] is False
    # The canonical Parquet plus receipt is the retained object. The isolated
    # child must not make a second copy in the SDK's default disk cache.
    assert isinstance(context._default_context._storage, data.CacheStorage)
    result = pd.DataFrame({
        'stock_id': ['I5F_201611'], 'trade_date': ['2016-11-08'],
        'timestamp': pd.to_datetime(['2016-11-08T08:45:00+08:00']),
        'session': ['regular'], 'open': [100.], 'high': [100.],
        'low': [100.], 'close': [100.], 'volume': [1],
    })
    result.attrs['volume_scope'] = 'outright_only'
    return result
data.get = get
raise SystemExit(bridge.main())
'''
    payload = {'root': str(tmp_path), 'key': 'tw_futures_minute:I5F_201611',
               'day': '2016-11-08', 'session': {'session_id': 'private-fixture'},
               'futures_workspace': workspace_mode}
    result = subprocess.run([sys.executable, '-B', '-c', fixture], input=json.dumps(payload),
                            capture_output=True, text=True, cwd=repo, env=env, timeout=30)
    assert result.returncode == 0, result.stderr
    assert 'private-fixture' not in result.stdout + result.stderr
    assert json.loads(result.stdout) == {'sdk_version': '2.2.18', 'status': 'downloaded_unverified_for_pit'}
    from scripts.download_finlab_intraday import _partition_paths
    receipt = json.loads(_partition_paths(tmp_path, payload['key'], date(2016, 11, 8))[0].read_text())
    assert receipt['rows'] == 1 and receipt['source_sdk_version'] == '2.2.18'
    assert receipt['publication_time_status'] == 'not_verified_for_training'
    assert (tmp_path / receipt['parquet_path']).exists()
    if workspace_mode:
        assert receipt['storage_contract'] == 'finlab_futures_workspace_objects_v1'
        assert (tmp_path / receipt['parquet_path']).parent == Path(env['FINLAB_TEST_WORKSPACE'])


@pytest.mark.parametrize('failure_phase', ['before_source', 'after_source'])
def test_isolated_bridge_distinguishes_mount_loss_from_provider_results(tmp_path, failure_phase):
    overlay = adapter.validated_overlay(Path('data_finlab'))
    if overlay is None:
        pytest.skip('verified optional FinLab futures SDK overlay is not installed')
    repo = Path(__file__).resolve().parents[1]
    alias = tmp_path / 'intraday/objects/futures-workspace-v1'
    alias.mkdir(parents=True)
    env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(overlay.resolve()), str(repo))),
               PYTHONDONTWRITEBYTECODE='1', FINLAB_TEST_WORKSPACE=str(alias),
               FINLAB_TEST_FAILURE_PHASE=failure_phase)
    fixture = r'''
import os
from pathlib import Path
import pandas as pd
from finlab import data
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts import finlab_futures_sdk_bridge as bridge
workspace_volume.FINLAB_FUTURES_ALIAS = Path(os.environ['FINLAB_TEST_WORKSPACE'])
checks = []
def check():
    checks.append(1)
    if (os.environ['FINLAB_TEST_FAILURE_PHASE'] == 'before_source' or len(checks) == 3):
        raise SnapshotError('fixture mount disappeared')
workspace_volume.check_finlab_futures = check
def get(key, **kwargs):
    assert os.environ['FINLAB_TEST_FAILURE_PHASE'] == 'after_source'
    result = pd.DataFrame({'stock_id': ['I5F_201611'], 'trade_date': ['2016-11-08'],
        'timestamp': pd.to_datetime(['2016-11-08T08:45:00+08:00']), 'session': ['regular'],
        'open': [100.], 'high': [100.], 'low': [100.], 'close': [100.], 'volume': [1]})
    result.attrs['volume_scope'] = 'outright_only'
    return result
data.get = get
raise SystemExit(bridge.main())
'''
    payload = {'root': str(tmp_path), 'key': 'tw_futures_minute:I5F_201611',
               'day': '2016-11-08', 'session': {'session_id': 'private-fixture'},
               'futures_workspace': True}
    result = subprocess.run([sys.executable, '-B', '-c', fixture], input=json.dumps(payload),
                            capture_output=True, text=True, cwd=repo, env=env, timeout=30)
    assert result.returncode == 0 and 'private-fixture' not in result.stdout + result.stderr
    receipt, attempt = __import__('scripts.download_finlab_intraday', fromlist=['_partition_paths'])._partition_paths(
        tmp_path, payload['key'], date(2016, 11, 8))
    assert not receipt.exists()
    expected = 'workspace_unavailable_before_source' if failure_phase == 'before_source' else 'source_outcome_unknown'
    assert json.loads(result.stdout)['failure'] == expected
    assert attempt.exists() is (failure_phase == 'after_source')
    if attempt.exists():
        assert json.loads(attempt.read_text())['status'] == 'source_outcome_unknown'
