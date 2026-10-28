import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import audit_wsl_storage_usage as census
from scripts import render_wsl_storage_census as renderer


def test_render_existing_complete_census_preserves_scope_and_hardlink_accounting(tmp_path):
    root = tmp_path / 'wsl'
    (root / 'one').mkdir(parents=True)
    (root / 'two').mkdir()
    (root / 'one' / 'payload').write_bytes(b'x' * 9000)
    (root / 'two' / 'linked').hardlink_to(root / 'one' / 'payload')
    output = tmp_path / 'census'
    assert census.scan(root, output, splits=('.',), mounts=[]) == 0
    result = renderer.render(output, observed_run_state='succeeded')
    assert result['state'] == 'complete'
    summary = (output / 'inventory_summary.md').read_text()
    csv = (output / 'path_usage.csv').read_text()
    assert '不是原子快照' in summary and '不代表可刪除空間' in summary
    assert str(root / 'one') in summary and str(root / 'two') in csv
    assert 'accounted_roots' in csv
    assert json.loads((output / 'result.json').read_text())['authorizes_retirement'] is False


def test_render_running_census_marks_unobserved_roots_without_guessing(tmp_path):
    root = tmp_path / 'wsl'
    root.mkdir()
    (root / 'waiting').write_text('waiting')
    plan = census.make_plan(root, splits=('.',), mounts=[])
    output = tmp_path / 'census'
    output.mkdir()
    (output / 'plan.json').write_text(json.dumps(plan))
    (output / 'records.nul').write_bytes(b'')
    result = renderer.render(output, observed_run_state='running')
    assert result['state'] == 'running'
    summary = (output / 'inventory_summary.md').read_text()
    assert '不是完成百分比或 ETA' in summary
    assert str(root / 'waiting') in summary and '尚未量測' in summary
    assert f'| `{root}` |' not in summary
    csv = (output / 'path_usage.csv').read_text().splitlines()
    assert next(row for row in csv if row.startswith(str(root / 'waiting') + ',')).endswith(',,1,0,0,False')
    root_row = next(row for row in csv
                    if row.startswith(str(root) + ','))
    assert root_row.endswith(',1,0,0,False')
    (output / 'result.json').write_text('{"state":')
    assert renderer.render(output, observed_run_state='running')['state'] == 'running'


def test_report_never_replaces_a_symlink(tmp_path):
    target, victim = tmp_path / 'report.md', tmp_path / 'victim'
    victim.write_text('preserve')
    target.symlink_to(victim)
    with pytest.raises(ValueError, match='redirected'):
        renderer._replace_generated(target, 'new')
    assert victim.read_text() == 'preserve'


def test_root_does_not_list_itself_as_its_own_child():
    rows = [{'path': '/'}, {'path': '/srv'}, {'path': '/srv/deeper'}]
    assert renderer._children(rows, Path('/')) == [{'path': '/srv'}]


def test_watcher_requires_exact_supervised_census_output(tmp_path, monkeypatch):
    output = tmp_path / 'census'
    run = {'run_id': 'scan-1', 'repo_root': str(renderer.ROOT), 'state': 'running',
           'argv': ['run_fintech_python', 'scripts/audit_wsl_storage_usage.py',
                    'scan', '--output', str(output)]}
    monkeypatch.setattr(renderer.subprocess, 'run', lambda *args, **kwargs:
                        SimpleNamespace(stdout=json.dumps(run)))
    assert renderer._run_state('scan-1', output) == 'running'
    run['argv'][-1] = str(tmp_path / 'another')
    with pytest.raises(ValueError, match='exact census'):
        renderer._run_state('scan-1', output)
