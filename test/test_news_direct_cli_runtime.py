"""Direct repository scripts use their checkout without changing module mode."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _environment(pythonpath=None):
    env = dict(os.environ, PYTHON_BIN=sys.executable, FINTECH_ENV_PATH=sys.prefix)
    env.pop('PYTHONPATH', None)
    if pythonpath is not None:
        env['PYTHONPATH'] = pythonpath
    return env


@pytest.mark.parametrize('script', [
    'download_news_fulltext.py',
    'audit_news_cnyes_alias_candidates.py',
    'review_news_ocr_candidates.py',
    'review_news_pdf_page_text.py',
])
def test_actual_news_cli_help_works_without_pythonpath_after_chdir(tmp_path, script):
    command = r'''
set -e
source "$1/scripts/runtime_env.sh"
cd "$2"
run_fintech_python "$1/scripts/$3" --help
test "${PYTHONPATH+x}" != x
'''
    result = subprocess.run(['bash', '-c', command, 'bash', str(ROOT), str(tmp_path), script],
                            env=_environment(), text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert 'usage:' in result.stdout


def _checkout(tmp_path):
    root = tmp_path / 'checkout with spaces'
    scripts = root / 'scripts'
    scripts.mkdir(parents=True)
    (scripts / 'runtime_env.sh').write_bytes((ROOT / 'scripts/runtime_env.sh').read_bytes())
    (root / 'unique_checkout_marker.py').write_text('VALUE = "checkout"\n')
    (scripts / 'entry.py').write_text(
        'import json, os, unique_checkout_marker\n'
        'print(json.dumps({"pid": os.getpid(), "path": os.environ.get("PYTHONPATH"), '
        '"marker": unique_checkout_marker.VALUE}))\n'
    )
    return root


@pytest.mark.parametrize('placement', ['absent', 'first', 'later'])
def test_direct_run_preserves_caller_path_and_does_not_mutate_shell(tmp_path, placement):
    root = _checkout(tmp_path)
    extra = tmp_path / 'other packages'
    extra.mkdir()
    (extra / 'unique_checkout_marker.py').write_text('VALUE = "other checkout"\n')
    original = {'absent': str(extra), 'first': f'{root}:{extra}', 'later': f'{extra}:{root}'}[placement]
    command = r'''
set -e
source "$1/scripts/runtime_env.sh"
cd "$2"
run_fintech_python "$1/scripts/entry.py"
printf '%s\n' "$PYTHONPATH"
'''
    result = subprocess.run(['bash', '-c', command, 'bash', str(root), str(tmp_path)],
                            env=_environment(original), text=True, capture_output=True, check=True)
    payload, parent_path = result.stdout.splitlines()
    value = json.loads(payload)
    assert value['marker'] == 'checkout'
    assert value['path'] == (original if placement == 'first' else f'{root}:{original}')
    assert parent_path == original


def test_external_script_and_module_mode_keep_original_import_environment(tmp_path):
    root = _checkout(tmp_path)
    extra = tmp_path / 'external packages'
    extra.mkdir()
    original = str(extra)
    external = tmp_path / 'outside.py'
    external.write_text('import os\nprint(os.environ.get("PYTHONPATH", ""))\n')
    (extra / 'external_module.py').write_text('import os\nprint(os.environ.get("PYTHONPATH", ""))\n')
    command = r'''
set -e
source "$1/scripts/runtime_env.sh"
run_fintech_python "$2"
run_fintech_python -c 'import os; print(os.environ.get("PYTHONPATH", ""))'
run_fintech_python -m external_module
'''
    result = subprocess.run(['bash', '-c', command, 'bash', str(root), str(external)],
                            env=_environment(original), text=True, capture_output=True, check=True)
    assert result.stdout.splitlines() == [original, original, original]


def test_script_symlinks_are_scoped_to_their_real_checkout_target(tmp_path):
    root = _checkout(tmp_path)
    external = tmp_path / 'outside.py'
    external.write_text('import os\nprint(os.environ.get("PYTHONPATH", ""))\n')
    project_link = tmp_path / 'project-entry.py'
    project_link.symlink_to(root / 'scripts/entry.py')
    external_link = root / 'scripts/external.py'
    external_link.symlink_to(external)
    original = str(tmp_path / 'existing path')
    command = r'''
set -e
source "$1/scripts/runtime_env.sh"
run_fintech_python "$2"
run_fintech_python "$3"
'''
    result = subprocess.run(['bash', '-c', command, 'bash', str(root), str(project_link), str(external_link)],
                            env=_environment(original), text=True, capture_output=True, check=True)
    project, outside_path = result.stdout.splitlines()
    assert json.loads(project)['path'] == f'{root}:{original}'
    assert outside_path == original


def test_supervised_direct_script_replaces_shell_and_uses_checkout(tmp_path):
    root = _checkout(tmp_path)
    command = r'''
set -e
source "$1/scripts/runtime_env.sh"
printf '%s\n' "$$"
exec_fintech_python "$1/scripts/entry.py"
'''
    result = subprocess.run(['bash', '-c', command, 'bash', str(root)],
                            env=_environment(), text=True, capture_output=True, check=True)
    shell_pid, payload = result.stdout.splitlines()
    value = json.loads(payload)
    assert str(value['pid']) == shell_pid
    assert value['marker'] == 'checkout'
    assert value['path'] == str(root)
