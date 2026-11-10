from __future__ import annotations

import hashlib
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_symlink_closure as closure
from stockagent.data_sync.desync_snapshots import SnapshotError


def make_index(db, source):
    db.execute('CREATE TABLE files(path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,state TEXT)')
    for name, kind, signature in relocation.walk(source, preserve_symlinks=True):
        sha = hashlib.sha256(os.fsencode(os.readlink(source/name))).hexdigest() if kind == 'symlink' else None
        db.execute('INSERT INTO files VALUES (?,?,?,?,?)',
                   (name, kind, json.dumps(signature), sha, 'verified' if kind != 'directory' else 'pending'))


def test_internal_target_is_mapped_to_fixed_destination(tmp_path):
    source = tmp_path / 'source'; source.mkdir()
    (source / 'data').mkdir(); (source / 'data' / 'part').write_bytes(b'data')
    (source / 'alias').symlink_to('data', target_is_directory=True)
    destination = tmp_path / 'destination'
    shutil.copytree(source, destination, symlinks=True)
    with sqlite3.connect(':memory:') as db:
        make_index(db, source)
        assert closure.inspect_links(db, source, destination) == {
            'links': 1, 'internal_targets_verified': 1, 'unresolved_targets': 0}


@pytest.mark.parametrize('target', ['../external', 'missing'])
def test_external_and_dangling_targets_do_not_become_completion(tmp_path, target):
    source = tmp_path / 'source'; source.mkdir()
    (tmp_path / 'external').write_text('outside')
    (source / 'alias').symlink_to(target)
    destination = tmp_path / 'destination'
    shutil.copytree(source, destination, symlinks=True)
    with sqlite3.connect(':memory:') as db:
        make_index(db, source)
        assert closure.inspect_links(db, source, destination)['unresolved_targets'] == 1


def test_changed_link_or_destination_fails_closed(tmp_path):
    source = tmp_path / 'source'; source.mkdir()
    (source / 'data').write_bytes(b'data')
    (source / 'alias').symlink_to('data')
    destination = tmp_path / 'destination'
    shutil.copytree(source, destination, symlinks=True)
    with sqlite3.connect(':memory:') as db:
        make_index(db, source)
        (destination / 'alias').unlink()
        (destination / 'alias').symlink_to('elsewhere')
        with pytest.raises(SnapshotError, match='text differs'):
            closure.inspect_links(db, source, destination)


@pytest.mark.parametrize('change', ['none', 'link', 'missing', 'target_metadata'])
def test_mounted_reviewed_child_link_stays_inside_fixed_index(tmp_path, change):
    root = tmp_path/'root'; root.mkdir()
    child = root/closure.FUTURES_765_PART; child.mkdir()
    (child/'data').mkdir()
    (child/'data'/'part').write_bytes(b'data')
    (child/'alias').symlink_to('data', target_is_directory=True)
    with sqlite3.connect(':memory:') as db:
        make_index(db, root)
        if change == 'link':
            (child/'alias').unlink(); (child/'alias').symlink_to('../outside')
        elif change == 'missing':
            shutil.rmtree(child/'data')
        elif change == 'target_metadata':
            info = (child/'data').stat()
            os.utime(child/'data', ns=(info.st_atime_ns, info.st_mtime_ns+1_000_000))
        if change == 'none':
            assert closure.inspect_mounted_765_links(db, root, root) == {
                'links':1, 'internal_targets_verified':1, 'unresolved_targets':0}
        else:
            with pytest.raises((SnapshotError, OSError)):
                closure.inspect_mounted_765_links(db, root, root)


def test_links_accept_only_one_guarded_device_renumber(tmp_path):
    source = tmp_path/'source'; source.mkdir()
    (source/'data').write_bytes(b'data')
    (source/'alias').symlink_to('data')
    destination = tmp_path/'destination'
    shutil.copytree(source, destination, symlinks=True)
    actual_device = source.stat().st_dev
    old_device = actual_device + 1
    with sqlite3.connect(':memory:') as db:
        make_index(db, source)
        for name, text in list(db.execute('SELECT path,sig FROM files')):
            value = json.loads(text)
            value[0] = old_device
            db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(value), name))
        assert closure.inspect_links(db, source, destination,
                                    source_device_mapping=(old_device, actual_device))['internal_targets_verified'] == 1
        with pytest.raises(SnapshotError, match='generation differs'):
            closure.inspect_links(db, source, destination, source_device_mapping=(old_device+1, actual_device))
        name, text = db.execute("SELECT path,sig FROM files WHERE kind='symlink'").fetchone()
        changed = json.loads(text); changed[1] += 1
        db.execute('UPDATE files SET sig=? WHERE path=?', (json.dumps(changed), name))
        with pytest.raises(SnapshotError, match='generation differs'):
            closure.inspect_links(db, source, destination, source_device_mapping=(old_device, actual_device))


def test_monitor_requires_fixed_receipt_and_does_not_claim_backup(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue
    from stockagent.data_sync import storage_progress as monitor
    repo = tmp_path / 'repo'; repo.mkdir()
    workspace = tmp_path / 'workspace'; workspace.mkdir()
    evidence = workspace / 'evidence'; evidence.mkdir()
    report_path = workspace / 'copy-report.json'; report_path.write_text('{"fixed":true}')
    item = {'name': 'tw-futures-prepared-e-primary-20261104-v1', 'preserve_symlinks': True}
    monkeypatch.setattr(queue, 'ROOT', repo)
    monkeypatch.setattr(queue, 'load_sources', lambda path: [item])
    monkeypatch.setattr(queue, 'paths', lambda item, workspace: (repo/'s', workspace/'d', evidence, report_path))
    monkeypatch.setattr(queue, 'verified_report', lambda item, workspace: {'relocation_evidence_sha256': 'a'*64})
    config = repo / 'config.json'
    row, = monitor.workspace_symlink_closure_tasks(repo, config, workspace)
    assert row['state'] == 'waiting' and not row['complete_evidence']
    receipt = {'contract': closure.CONTRACT, 'name': item['name'], 'state': 'verified_internal',
               'source_copy_report_sha256': hashlib.sha256(report_path.read_bytes()).hexdigest(),
               'copy_result_sha256': 'a'*64, 'links': 1, 'internal_targets_verified': 1,
               'unresolved_targets': 0, 'source_files_deleted': 0, 'source_alias_changed': False,
               'D_or_NAS_acceptance': False, 'retirement_authorized': False, 'observed_at_epoch': 1000}
    (evidence / 'symlink-closure-result.json').write_text(json.dumps(receipt))
    row, = monitor.workspace_symlink_closure_tasks(repo, config, workspace)
    assert row['state'] == 'completed' and row['complete_evidence']
    status = workspace/'migration/source-symlink-closure-status.json'
    status.parent.mkdir()
    status.write_text(json.dumps({'contract': closure.CONTRACT, 'observed_at_epoch': 1001,
                                  'sources': [{'name': item['name'], 'state': 'blocked'}]}))
    row, = monitor.workspace_symlink_closure_tasks(repo, config, workspace)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    assert row['observed_at_utc'] == monitor.iso(1001)
    status.write_text(json.dumps({'contract': closure.CONTRACT, 'observed_at_epoch': 999,
                                  'sources': [{'name': item['name'], 'state': 'blocked'}]}))
    row, = monitor.workspace_symlink_closure_tasks(repo, config, workspace)
    assert row['state'] == 'completed' and row['complete_evidence']
    receipt['D_or_NAS_acceptance'] = True
    (evidence / 'symlink-closure-result.json').write_text(json.dumps(receipt))
    row, = monitor.workspace_symlink_closure_tasks(repo, config, workspace)
    assert row['state'] == 'blocked' and not row['complete_evidence']


def test_one_symlink_verifier_owner_at_a_time(tmp_path, monkeypatch):
    from scripts import check_workspace_symlink_closure as runner
    monkeypatch.setattr(runner.volume, 'ENROLLMENT', tmp_path / 'workspace-enrollment.json')
    called = []
    monkeypatch.setattr(runner, 'check_registered_sources', lambda: called.append(True) or 0)
    lock_dir = tmp_path / 'source-symlink-closure-v1'; lock_dir.mkdir()
    with (lock_dir / 'owner.lock').open('w+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert runner.main() == 75 and not called
    assert runner.main() == 0 and called == [True]


def test_completed_fixed_copy_writes_bound_closure_receipt(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue
    source = tmp_path / 'source'; source.mkdir()
    (source / 'marker').write_text('fixed')
    (source / 'inside').write_bytes(b'data')
    (source / 'alias').symlink_to('inside')
    destination = tmp_path / 'destination'
    shutil.copytree(source, destination, symlinks=True)
    evidence = tmp_path / 'evidence'; evidence.mkdir()
    with sqlite3.connect(evidence / 'files.sqlite') as db:
        make_index(db, source)
    (evidence / 'intent.json').write_text(json.dumps({
        'contract': 'workspace_scoped_relocation_v1', 'preserve_symlinks': True,
        'excluded_subtrees': [], 'source': str(source), 'destination': str(destination),
        'source_root_signature': relocation.signature(source)}))
    report_path = tmp_path / 'report.json'; report_path.write_text('{"fixed":true}')
    report = {'relocation_evidence_sha256': 'a'*64, 'symlink_names_preserved': 1,
              'source_marker_sha256': relocation.digest(source / 'marker')}
    item = {'name': 'test', 'marker': 'marker'}
    monkeypatch.setattr(queue, 'paths', lambda item, workspace: (source, destination, evidence, report_path))
    monkeypatch.setattr(queue, 'verified_report', lambda item, workspace: report)
    monkeypatch.setattr(closure.volume, 'check', lambda: {'state': 'mounted'})
    result = closure.check_item(item, tmp_path)
    assert result['state'] == 'verified_internal' and result['internal_targets_verified'] == 1
    assert result['new_content_readback'] is False and result['D_or_NAS_acceptance'] is False
    assert result['retirement_authorized'] is False and not result['source_alias_changed']
    assert closure.check_item(item, tmp_path) == result
    # An unchanged fixed generation can acquire a new st_dev after a WSL
    # remount. The source must still be on the checked repository device.
    monkeypatch.setattr(queue, 'ROOT', tmp_path)
    intent_path = evidence/'intent.json'
    intent = json.loads(intent_path.read_text())
    intent['source_root_signature'][0] += 1
    intent_path.write_text(json.dumps(intent))
    assert closure.check_item(item, tmp_path) == result
    intent['source_root_signature'][1] += 1
    intent_path.write_text(json.dumps(intent))
    with pytest.raises(SnapshotError, match='source root generation changed'):
        closure.check_item(item, tmp_path)


def test_taifex_zero_link_copy_after_handoff_is_not_reported_as_blocked(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue
    from stockagent.data_sync import storage_progress as monitor, workspace_source_handoff as handoff
    repo = tmp_path/'repo'; repo.mkdir()
    source = repo/'data_taifex_public_history'; source.mkdir()
    (source/'marker').write_text('fixed')
    destination = tmp_path/'destination'; shutil.copytree(source, destination)
    evidence = tmp_path/'evidence'; evidence.mkdir()
    with sqlite3.connect(evidence/'files.sqlite') as db:
        make_index(db, source)
    (evidence/'intent.json').write_text(json.dumps({'contract':'workspace_scoped_relocation_v1',
        'preserve_symlinks':True, 'excluded_subtrees':[], 'source':str(source), 'destination':str(destination),
        'source_root_signature':relocation.signature(source)}))
    report_path = tmp_path/'report.json'; report_path.write_text('{"fixed":true}')
    report = {'relocation_evidence_sha256':'a'*64, 'symlink_names_preserved':0,
              'source_marker_sha256':relocation.digest(source/'marker')}
    item = {'name':handoff.TAIFEX_PUBLIC_NAME, 'marker':'marker', 'preserve_symlinks':True}
    journal = {'state':'active', 'source':str(source), 'backup':str(repo/'retained'),
               'destination':str(destination), 'evidence':str(evidence), 'workspace':str(tmp_path),
               'original_inode':1, 'original_lock_inode':2, 'destination_inode':3,
               'copy_result_sha256':'a'*64, 'report_sha256':relocation.digest(report_path)}
    monkeypatch.setattr(queue, 'ROOT', repo)
    monkeypatch.setattr(queue, 'load_sources', lambda _path: [item])
    monkeypatch.setattr(queue, 'paths', lambda *_: (source, destination, evidence, report_path))
    monkeypatch.setattr(queue, 'verified_report', lambda *_: report)
    monkeypatch.setattr(closure.volume, 'check', lambda: {'state':'mounted'})
    monkeypatch.setattr(handoff, 'selected_source', lambda _name: nullcontext())
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(source=source))
    monkeypatch.setattr(handoff, 'read_journal', lambda: journal)
    monkeypatch.setattr(handoff, '_assert_retained', lambda _value: destination)
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_: True)
    result = closure.check_item(item, tmp_path)
    assert result['state'] == 'no_links' and result['links'] == 0
    status = tmp_path/'migration/source-symlink-closure-status.json'
    status.parent.mkdir()
    status.write_text(json.dumps({'contract':closure.CONTRACT, 'observed_at_epoch':time.time(),
                                  'sources':[{'name':item['name'], **result}]}))
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'completed' and row['phase'] == 'workspace_link_no_links'
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_: False)
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_: True)
    with sqlite3.connect(evidence/'files.sqlite') as db:
        db.execute("INSERT INTO files VALUES ('link','symlink','[]','x','verified')")
    with pytest.raises(SnapshotError, match='contains a symbolic link'):
        closure.check_item(item, tmp_path)


def test_futures_parent_links_rechecked_after_reviewed_child_handoff(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue
    from stockagent.data_sync import storage_progress as monitor
    repo = tmp_path/'repo'; repo.mkdir()
    source = repo/'training_prepared'; source.mkdir()
    (source/'marker').write_text('fixed')
    (source/'sibling').mkdir()
    (source/'sibling'/'data').write_bytes(b'data')
    (source/'sibling'/'alias').symlink_to('data')
    destination = tmp_path/'destination'
    shutil.copytree(source, destination, symlinks=True)
    evidence = tmp_path/'evidence'; evidence.mkdir()
    with sqlite3.connect(evidence/'files.sqlite') as db:
        make_index(db, source)
    (evidence/'intent.json').write_text(json.dumps({'contract':'workspace_scoped_relocation_v1',
        'preserve_symlinks':True, 'excluded_subtrees':[], 'source':str(source), 'destination':str(destination),
        'source_root_signature':relocation.signature(source)}))
    report_path = tmp_path/'report.json'; report_path.write_text('{"fixed":true}')
    report = {'relocation_evidence_sha256':'a'*64, 'symlink_names_preserved':1,
              'source_marker_sha256':relocation.digest(source/'marker')}
    item = {'name':'tw-futures-prepared-e-primary-20261104-v1', 'marker':'marker', 'preserve_symlinks':True}
    monkeypatch.setattr(queue, 'ROOT', repo)
    monkeypatch.setattr(queue, 'load_sources', lambda _path: [item])
    monkeypatch.setattr(queue, 'paths', lambda _item, _workspace: (source, destination, evidence, report_path))
    monkeypatch.setattr(queue, 'verified_report', lambda _item, _workspace: report)
    monkeypatch.setattr(closure.volume, 'check', lambda: {'state':'mounted'})
    original = closure.check_item(item, tmp_path)
    old_receipt = (evidence/'symlink-closure-result.json').read_bytes()
    root_stat = source.stat()
    os.utime(source, ns=(root_stat.st_atime_ns, root_stat.st_mtime_ns+1_000_000))
    binding = {'source':'fixed-child', 'original_inode':123}
    monkeypatch.setattr(closure, '_active_futures_725_boundary', lambda *_args: (None, binding))
    monkeypatch.setattr(closure, '_active_futures_765_boundary', lambda *_args: None)
    partial = closure.check_item(item, tmp_path)
    assert partial['state'] == 'verified_internal' and partial['source_alias_changed'] is True
    assert partial['scope'] == 'indexed_links_outside_725_after_child_handoff'
    assert partial['child_handoff'] == binding and partial['links'] == 1
    assert (evidence/'symlink-closure-result.json').read_bytes() == old_receipt
    assert original['source_alias_changed'] is False
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'completed' and row['complete_evidence']
    monkeypatch.setattr(closure, '_active_futures_725_boundary', lambda *_args: (None, {'different':True}))
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'blocked' and not row['complete_evidence']


def test_parent_receipt_rechecks_both_child_handoffs(tmp_path, monkeypatch):
    from scripts import run_workspace_source_staging as queue
    from stockagent.data_sync import storage_progress as monitor
    repo = tmp_path/'repo'; repo.mkdir()
    source = repo/'training_prepared'; source.mkdir()
    (source/'marker').write_text('fixed')
    child = source/closure.FUTURES_765_PART; child.mkdir()
    (child/'data').mkdir(); (child/'alias').symlink_to('data', target_is_directory=True)
    evidence = tmp_path/'evidence'; evidence.mkdir()
    with sqlite3.connect(evidence/'files.sqlite') as db:
        make_index(db, source)
    (evidence/'intent.json').write_text(json.dumps({'contract':'workspace_scoped_relocation_v1',
        'preserve_symlinks':True, 'excluded_subtrees':[], 'source':str(source), 'destination':str(source),
        'source_root_signature':relocation.signature(source)}))
    report_path = tmp_path/'report.json'; report_path.write_text('{"fixed":true}')
    report = {'relocation_evidence_sha256':'a'*64, 'symlink_names_preserved':1,
              'source_marker_sha256':relocation.digest(source/'marker')}
    item = {'name':'tw-futures-prepared-e-primary-20261104-v1', 'marker':'marker', 'preserve_symlinks':True}
    monkeypatch.setattr(queue, 'ROOT', repo)
    monkeypatch.setattr(queue, 'load_sources', lambda _path: [item])
    monkeypatch.setattr(queue, 'paths', lambda _item, _workspace: (source, source, evidence, report_path))
    monkeypatch.setattr(queue, 'verified_report', lambda _item, _workspace: report)
    monkeypatch.setattr(closure.volume, 'check', lambda: {'state':'mounted'})
    closure.check_item(item, tmp_path)
    root_stat = source.stat()
    os.utime(source, ns=(root_stat.st_atime_ns, root_stat.st_mtime_ns+1_000_000))
    first, second = {'source':'725'}, {'source':'765'}
    monkeypatch.setattr(closure, '_active_futures_725_boundary', lambda *_args: (None, first))
    monkeypatch.setattr(closure, '_active_futures_765_boundary', lambda *_args: None)
    closure.check_item(item, tmp_path)
    monkeypatch.setattr(closure, '_active_futures_765_boundary', lambda *_args: second)
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    result = closure.check_item(item, tmp_path)
    assert result['scope'] == 'indexed_links_after_725_765_child_handoffs'
    assert result['child_handoff'] == first and result['second_child_handoff'] == second
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'completed' and row['complete_evidence']
    monkeypatch.setattr(closure, '_active_futures_765_boundary', lambda *_args: {'source':'other'})
    row, = monitor.workspace_symlink_closure_tasks(repo, repo/'config.json', tmp_path)
    assert row['state'] == 'blocked' and not row['complete_evidence']
