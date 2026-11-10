from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from scripts import enroll_fixed_source_handoff as enrollment
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def fixed_copy(tmp_path, monkeypatch):
    repo, workspace = tmp_path/'repo', tmp_path/'workspace'
    source, destination = repo/'data_yahoo', workspace/'data_yahoo'
    evidence, control = workspace/'evidence', tmp_path/'control'
    for directory in (source, destination, evidence, control):
        directory.mkdir(parents=True)
    (control/'owner.lock').touch()
    (source/'download_summary.json').write_text('{}')
    (destination/'download_summary.json').write_text('{}')
    (repo/'writer.py').write_text('guarded')
    (evidence/'intent.json').write_text(json.dumps({'source_root_signature':relocation.signature(source)}))
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute('CREATE TABLE files (path TEXT, kind TEXT)')
        db.execute("INSERT INTO files VALUES ('download_summary.json','file')")
    item = {'name':handoff.YAHOO_NAME, 'source':'data_yahoo'}
    report = {'files':1, 'logical_bytes':2}
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    monkeypatch.setattr(handoff, 'YAHOO_WRITER_GUARD_FILES', ('writer.py',))
    monkeypatch.setattr(handoff, 'enrolled', lambda: (item, workspace, destination, evidence, workspace/'report'))
    monkeypatch.setattr(enrollment.workspace_volume, 'check', lambda: {'state':'mounted'})
    monkeypatch.setattr(enrollment.staging, 'verified_report', lambda *_args: report)
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args:False)
    return {'repo':repo, 'source':source, 'destination':destination, 'evidence':evidence, 'control':control}


def test_enrollment_only_records_metadata_without_reading_source_payload(fixed_copy, monkeypatch):
    original_digest = relocation.digest
    def guarded_digest(path, **kwargs):
        path = Path(path)
        assert not path.is_relative_to(fixed_copy['source'])
        assert not path.is_relative_to(fixed_copy['destination'])
        return original_digest(path, **kwargs)
    monkeypatch.setattr(relocation, 'digest', guarded_digest)
    assert enrollment.enroll()['state'] == 'ready'
    result = enrollment.enroll(apply=True)
    assert result['state'] == 'enrolled_for_existing_owner'
    assert not result['new_payload_readback'] and not result['source_files_deleted']
    handoff._validate_yahoo_enrollment()
    assert (fixed_copy['source']/'download_summary.json').is_file()
    assert not (fixed_copy['repo']/'.data_yahoo.pre-e-20261108-v1').exists()


def test_enrollment_rejects_unreviewed_symlink(fixed_copy):
    with sqlite3.connect(fixed_copy['evidence']/'files.sqlite') as db:
        db.execute("INSERT INTO files VALUES ('redirect','symlink')")
    with pytest.raises(SnapshotError, match='symbolic links'):
        enrollment.enroll(apply=True)
    assert not (fixed_copy['control']/'yahoo-enrollment.json').exists()


def test_minute_raw_enrollment_binds_guarded_writers_without_readback(tmp_path, monkeypatch):
    repo, workspace, control = tmp_path/'repo', tmp_path/'workspace', tmp_path/'control'
    source = repo/'data_tw_minute/shioaji_1m'
    destination, evidence = workspace/'minute', workspace/'evidence'
    for directory in (source, destination, evidence, control):
        directory.mkdir(parents=True)
    (control/'owner.lock').touch()
    (source/'download_summary.json').write_text('{}')
    (destination/'download_summary.json').write_text('{}')
    (repo/'writer.py').write_text('guarded')
    (evidence/'intent.json').write_text(json.dumps({'source_root_signature': relocation.signature(source)}))
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute('CREATE TABLE files (path TEXT, kind TEXT)')
        db.execute("INSERT INTO files VALUES ('download_summary.json','file')")
    item = {'name': handoff.MINUTE_RAW_NAME, 'source': 'data_tw_minute/shioaji_1m'}
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    monkeypatch.setattr(handoff, 'MINUTE_RAW_WRITER_GUARD_FILES', ('writer.py',))
    monkeypatch.setattr(handoff, 'enrolled', lambda: (item, workspace, destination, evidence, workspace/'report'))
    monkeypatch.setattr(enrollment.workspace_volume, 'check', lambda: {'state': 'mounted'})
    monkeypatch.setattr(enrollment.staging, 'verified_report', lambda *_args: {'files': 1, 'logical_bytes': 2})
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args: False)
    original_digest = relocation.digest
    def guarded_digest(path, **kwargs):
        assert not Path(path).is_relative_to(source) and not Path(path).is_relative_to(destination)
        return original_digest(path, **kwargs)
    monkeypatch.setattr(relocation, 'digest', guarded_digest)
    result = enrollment.enroll(handoff.MINUTE_RAW_NAME, apply=True)
    assert result['state'] == 'enrolled_for_existing_owner' and not result['new_payload_readback']
    assert source.is_dir() and not (repo/'.data_tw_minute_shioaji_1m.pre-e-20261108-v1').exists()
    handoff._validate_minute_raw_enrollment()
    (repo/'writer.py').write_text('changed')
    with pytest.raises(SnapshotError, match='writer guard differs'):
        handoff._validate_minute_raw_enrollment()


def test_pepperstone_enrollment_binds_its_writer_code_and_exact_scope(tmp_path, monkeypatch):
    repo, workspace, control = tmp_path/'repo', tmp_path/'workspace', tmp_path/'control'
    source, destination, evidence = repo/'data_peperstone', workspace/'data_peperstone', workspace/'evidence'
    for directory in (source, destination, evidence, control):
        directory.mkdir(parents=True)
    for root in (source, destination):
        (root/'download_summary.json').write_text('{}')
    (control/'owner.lock').touch()
    (repo/'writer.py').write_text('guarded')
    (evidence/'intent.json').write_text(json.dumps({'source_root_signature':relocation.signature(source)}))
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute('CREATE TABLE files (path TEXT, kind TEXT)')
        db.execute("INSERT INTO files VALUES ('download_summary.json','file')")
    item = {'name':handoff.PEPPERSTONE_NAME, 'source':'data_peperstone'}
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    monkeypatch.setattr(handoff, 'PEPPERSTONE_WRITER_GUARD_FILES', ('writer.py',))
    monkeypatch.setattr(handoff, 'enrolled', lambda: (item, workspace, destination, evidence, workspace/'report'))
    monkeypatch.setattr(enrollment.workspace_volume, 'check', lambda: {'state':'mounted'})
    monkeypatch.setattr(enrollment.staging, 'verified_report', lambda *_args: {'files':1, 'logical_bytes':2})
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args:False)
    assert enrollment.enroll(handoff.PEPPERSTONE_NAME)['state'] == 'ready'
    assert enrollment.enroll(handoff.PEPPERSTONE_NAME, apply=True)['state'] == 'enrolled_for_existing_owner'
    handoff._validate_pepperstone_enrollment()
    (repo/'writer.py').write_text('unguarded change')
    with pytest.raises(SnapshotError, match='writer guard differs'):
        handoff._validate_pepperstone_enrollment()
    assert (source/'download_summary.json').is_file()


@pytest.mark.parametrize('name', [handoff.CFTC_NAME, handoff.FRED_NAME, handoff.TAIFEX_PUBLIC_NAME,
                                  handoff.OPTIONS_RAW_NAME])
def test_retained_collector_enrollment_checks_both_actual_writer_locks(tmp_path, monkeypatch, name):
    repo, workspace, control = tmp_path/'repo', tmp_path/'workspace', tmp_path/'control'
    spec = {**handoff.RETAINED_LOCK_SOURCES[name], 'guard_files':('writer.py',)}
    monkeypatch.setattr(handoff, 'RETAINED_LOCK_SOURCES', {name:spec})
    source, destination, evidence = repo/spec['directory'], workspace/spec['directory'], workspace/'evidence'
    for directory in (source/'state', destination/'state', evidence, control):
        directory.mkdir(parents=True)
    for root in (source, destination):
        for relative in spec.get('native_locks', ('state/collector.lock',)):
            lock = root/relative
            lock.parent.mkdir(parents=True, exist_ok=True)
            lock.touch()
        (root/spec['marker']).parent.mkdir(parents=True, exist_ok=True)
        (root/spec['marker']).write_text('{}')
    for relative in spec.get('external_native_locks', ()):
        lock = repo/relative
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.touch()
    (control/'owner.lock').touch()
    (repo/'writer.py').write_text('guarded')
    (evidence/'intent.json').write_text(json.dumps({'source_root_signature':relocation.signature(source)}))
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute('CREATE TABLE files (path TEXT, kind TEXT)')
        db.execute('INSERT INTO files VALUES (?,?)', (spec['marker'], 'file'))
    item, report = {'name':name, 'source':spec['directory']}, {'files':2, 'logical_bytes':2}
    monkeypatch.setattr(handoff, 'ROOT', repo)
    monkeypatch.setattr(handoff, 'CONTROL', control)
    monkeypatch.setattr(handoff, 'enrolled', lambda: (item, workspace, destination, evidence, workspace/'report'))
    monkeypatch.setattr(enrollment.workspace_volume, 'check', lambda: {'state':'mounted'})
    monkeypatch.setattr(enrollment.staging, 'verified_report', lambda *_args: report)
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args:False)
    marker = control/(spec['label']+'-enrollment.json')
    for root in (source, destination):
        for relative in spec.get('native_locks', ('state/collector.lock',)):
            with handoff.lock_file(root/relative):
                with pytest.raises(BlockingIOError):
                    enrollment.enroll(name, apply=True)
    for relative in spec.get('external_native_locks', ()):
        with handoff.lock_file(repo/relative):
            with pytest.raises(BlockingIOError):
                enrollment.enroll(name, apply=True)
    assert not marker.exists()
    result = enrollment.enroll(name, apply=True)
    assert result['state'] == 'enrolled_for_existing_owner' and not result['source_files_deleted']
    handoff._validate_retained_enrollment(name)
