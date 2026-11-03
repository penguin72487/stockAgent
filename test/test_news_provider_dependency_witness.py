from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest

from scripts import audit_news_provider_dependencies as witness


def test_dependency_witness_rejects_changed_file_or_package(tmp_path, monkeypatch):
    source = tmp_path / 'dep.py'
    source.write_text('source = 1\n')
    run = tmp_path / 'run.json'
    run.write_text(json.dumps({'run_id': 'fixture', 'started_at_utc':
                              datetime.now(timezone.utc).isoformat()}))
    monkeypatch.setattr(witness, 'version', lambda name: '1')
    initial = witness.observe(run, repo=tmp_path, files=('dep.py',), packages=('library',))
    assert witness.verify(initial, witness.observe(run, repo=tmp_path, files=('dep.py',), packages=('library',)))['accepted']
    monkeypatch.setattr(witness, 'version', lambda name: '2')
    with pytest.raises(ValueError, match='dependency_witness_changed'):
        witness.verify(initial, witness.observe(run, repo=tmp_path, files=('dep.py',), packages=('library',)))
    monkeypatch.setattr(witness, 'version', lambda name: '1')
    altered = deepcopy(initial)
    altered['files']['dep.py']['sha256'] = '0' * 64
    with pytest.raises(ValueError, match='dependency_witness_changed'):
        witness.verify(initial, altered)
    source.write_text('source = 2\n')
    with pytest.raises(ValueError, match='dependency_modified_after_run_start'):
        witness.observe(run, repo=tmp_path, files=('dep.py',), packages=('library',))


def test_dependency_witness_refuses_retroactive_file_change(tmp_path):
    source = tmp_path / 'dep.py'
    source.write_text('source = 1\n')
    run = tmp_path / 'run.json'
    run.write_text(json.dumps({'run_id': 'fixture', 'started_at_utc':
                              (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}))
    with pytest.raises(ValueError, match='dependency_modified_after_run_start'):
        witness.observe(run, repo=tmp_path, files=('dep.py',), packages=())


def test_dependency_witness_rejects_future_run_start(tmp_path):
    run = tmp_path / 'run.json'
    run.write_text(json.dumps({'run_id': 'fixture', 'started_at_utc':
                              (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}))
    with pytest.raises(ValueError, match='run_start_is_in_future'):
        witness.observe(run, repo=tmp_path, files=(), packages=())
