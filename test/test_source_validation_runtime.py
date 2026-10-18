"""The locked storage role can inspect validators without numerical imports."""
import json
import subprocess
import sys

import pytest

from scripts import validate_training_sources as gates
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_validator_fingerprints_require_no_numerical_imports():
    program = '''from scripts import validate_training_sources as gates
import sys
from stockagent.data_sync.source_validation import validator_identity
for kind in ('stock_minute_source', 'stock_minute_dataset'):
    assert len(validator_identity(*gates._validator_dependencies(kind))) == 64
assert not any(name in sys.modules for name in ('numpy', 'polars', 'torch'))
'''
    subprocess.run([sys.executable, '-c', program], cwd=gates.ROOT, check=True)


def test_source_identity_and_unchanged_guard_require_no_numerical_runtime(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'download_report.csv').write_text(
        'symbol,status,source_minute_rows\n2330,contract_unavailable,0\n')
    (source / 'download_summary.json').write_text('{"source":"shioaji_kbars_1m"}')
    raw = source / 'raw.bin'
    raw.write_bytes(b'unchanged source')
    entry = {'dataset': 'fixture', 'source': str(source),
             'validation_gate': {'kind': 'stock_minute_source'}}
    expected = gates._identity(entry, tmp_path)
    program = '''import importlib.abc, json, sys
from pathlib import Path
class NoNumericalImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in ('numpy', 'polars', 'torch'):
            raise ModuleNotFoundError('numerical import forbidden: ' + fullname)
sys.meta_path.insert(0, NoNumericalImports())
from scripts import validate_training_sources as gates
entry = json.loads(sys.argv[1])
root = Path(sys.argv[2])
value = gates._identity(entry, root)
gates.verify_unchanged(entry, {'identity':value}, repo_root=root)
(Path(entry['source'])/'raw.bin').write_bytes(b'changed source')
try:
    gates.verify_unchanged(entry, {'identity':value}, repo_root=root)
except gates.SnapshotError:
    pass
else:
    raise AssertionError('changed raw source was accepted')
assert not any(name in sys.modules for name in ('numpy','polars','torch'))
print(value)
'''
    result = subprocess.run([sys.executable, '-c', program, json.dumps(entry), str(tmp_path)],
                            cwd=gates.ROOT, check=True, text=True, capture_output=True)
    assert result.stdout.strip() == expected


@pytest.mark.parametrize('defect', ['child_failure', 'unrelated_result'])
def test_runtime_bridge_fails_closed_and_does_not_mutate_parent_environment(tmp_path, monkeypatch, defect):
    monkeypatch.setattr(gates.importlib.util, 'find_spec', lambda _: None)
    monkeypatch.setenv('PYTHON_BIN', 'locked-control-python')
    monkeypatch.setenv('FINTECH_ENV_PATH', 'locked-control-prefix')
    def run(command, **kwargs):
        assert command[0] == 'bash' and 'run_fintech_python' in command[2]
        assert 'PYTHON_BIN' not in kwargs['env'] and 'FINTECH_ENV_PATH' not in kwargs['env']
        assert json.loads(kwargs['input'])['dataset'] == 'fixture'
        if defect == 'child_failure':
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, json.dumps({
            'contract': 'source_semantic_audit_result_v1', 'dataset': 'unrelated', 'result': {}}))
    monkeypatch.setattr(gates.subprocess, 'run', run)
    with pytest.raises(SnapshotError):
        gates._audit_with_runtime({'dataset': 'fixture'}, tmp_path, tmp_path)
    assert gates.os.environ['PYTHON_BIN'] == 'locked-control-python'
    assert gates.os.environ['FINTECH_ENV_PATH'] == 'locked-control-prefix'
