import json
import os
from pathlib import Path
import sqlite3
import threading

import pytest

from stockagent.data_sync import transport_relocation_audit as audit
from stockagent.data_sync import transport_relocation_delta as delta
from stockagent.data_sync import transport_relocation_retirement as retirement
from stockagent.data_sync import stream_repack
from stockagent.data_sync import workspace_relocation as relocation


def fixture_copy(tmp_path):
    source = tmp_path / 'D'
    source.mkdir()
    (source / 'same').write_bytes(b'unchanged')
    (source / 'changed').write_bytes(b'old version')
    (source / 'missing').write_bytes(b'preserve this')
    target = tmp_path / 'E'
    evidence = tmp_path / 'evidence'
    relocation.copy(source, target, evidence, workers=2)
    return source, target, evidence


def test_fixed_copy_comparison_reports_differences_without_deleting(tmp_path):
    source, target, evidence = fixture_copy(tmp_path)
    (target / 'changed').write_bytes(b'new version')
    (target / 'missing').unlink()
    result = audit.compare_fixed_copy(evidence, target, workers=2)
    assert result['state'] == 'completed'
    assert (result['files_completed'], result['files_total']) == (3, 3)
    assert (result['same_files'], result['different_files'], result['missing_files']) == (1, 1, 1)
    assert not result['retirement_authorized'] and not result['NAS_acceptance_claimed']
    assert (source / 'changed').read_bytes() == b'old version'
    assert (source / 'missing').read_bytes() == b'preserve this'
    status = json.loads((evidence / 'comparison-status.json').read_text())
    assert status['logical_bytes_completed'] == status['logical_bytes_total']
    again = audit.compare_fixed_copy(evidence, target, workers=2)
    assert again['observed_at_epoch'] == result['observed_at_epoch']


def test_interrupted_comparison_resumes_same_fixed_inventory(tmp_path):
    _, target, evidence = fixture_copy(tmp_path)
    first = audit.compare_fixed_copy(evidence, target, workers=1)
    db = sqlite3.connect(evidence / 'comparison.sqlite')
    db.execute("UPDATE members SET outcome=NULL WHERE path='missing'")
    db.commit()
    db.close()
    # Previously observed comparisons are historical observations, not fresh
    # deletion evidence. Only the interrupted member is reread on resumption.
    (target / 'same').write_bytes(b'changed after previous observation')
    (target / 'missing').unlink()
    second = audit.compare_fixed_copy(evidence, target, workers=2)
    assert second['identity_sha256'] == first['identity_sha256']
    assert (second['same_files'], second['missing_files']) == (2, 1)
    copy = json.loads((evidence / 'copy-result.json').read_text())
    copy['files'] = 99
    (evidence / 'copy-result.json').write_text(json.dumps(copy))
    with pytest.raises(ValueError, match='identity'):
        audit.compare_fixed_copy(evidence, target)


def test_symlink_and_path_escape_are_never_followed(tmp_path):
    destination = tmp_path / 'E'
    destination.mkdir()
    outside = tmp_path / 'outside'
    outside.write_bytes(b'private')
    (destination / 'link').symlink_to(outside)
    assert audit._compare(destination, ('link', '0' * 64))[1] == 'unavailable'
    assert audit._compare(destination, ('../outside', '0' * 64))[1] == 'unavailable'


def test_difference_archive_preserves_originals_with_independent_readback(tmp_path, monkeypatch):
    source, target, evidence = fixture_copy(tmp_path)
    (target / 'changed').write_bytes(b'new version')
    (target / 'missing').unlink()
    audit.compare_fixed_copy(evidence, target, workers=2)
    monkeypatch.setattr(delta, '_validate_native_source', lambda root, intent: None)
    admissions = []
    out = tmp_path / 'E-archive'
    result = delta.preserve_differences(evidence, source, out, admit=admissions.append)
    assert result['state'] == 'verified' and result['files'] == 2
    assert result['logical_bytes'] == len(b'old version') + len(b'preserve this')
    assert admissions[0] > result['logical_bytes']
    assert not result['retirement_authorized'] and not result['NAS_acceptance_claimed']
    assert (source / 'changed').read_bytes() == b'old version'
    assert (source / 'missing').read_bytes() == b'preserve this'
    assert delta.preserve_differences(evidence, source, out, admit=admissions.append) == result
    with open(result['archive'], 'r+b') as file:
        file.seek(100)
        file.write(b'changed')
    with pytest.raises(ValueError, match='changed'):
        delta.preserve_differences(evidence, source, out, admit=admissions.append)


def test_verified_delta_and_restore_receipts_survive_only_guarded_device_renumbering(tmp_path, monkeypatch):
    source, target, evidence = fixture_copy(tmp_path)
    (target/'changed').write_bytes(b'new version')
    audit.compare_fixed_copy(evidence, target, workers=1)
    monkeypatch.setattr(delta, '_validate_native_source', lambda *_args: None)
    out = tmp_path/'delta'
    receipt = delta.preserve_differences(evidence, source, out, admit=lambda _bytes: None)
    restored = retirement.rehearse_delta_restore(evidence, tmp_path/'restore', admit=lambda _bytes: None)
    previous_device = receipt['archive_signature'][0] + 1
    for name in ('delta-receipt.json', 'delta-restore-receipt.json'):
        path = evidence/name
        value = json.loads(path.read_text())
        value['archive_signature'][0] = previous_device
        path.write_text(json.dumps(value))
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda _path, _device: True)
    def no_write(_bytes):
        pytest.fail('reusing a fixed receipt must not require a new archive or restore')
    reused = delta.preserve_differences(evidence, source, out, admit=no_write)
    same_restore = retirement.rehearse_delta_restore(evidence, tmp_path/'restore', admit=no_write)
    assert reused['archive_sha256'] == receipt['archive_sha256']
    assert same_restore['archive_sha256'] == restored['archive_sha256']
    changed = json.loads((evidence/'delta-receipt.json').read_text())
    changed['archive_signature'][1] += 1
    (evidence/'delta-receipt.json').write_text(json.dumps(changed))
    with pytest.raises(ValueError, match='changed'):
        delta.preserve_differences(evidence, source, out, admit=no_write)


def test_device_renumbering_requires_the_enrolled_E_volume(tmp_path, monkeypatch):
    path = tmp_path/'archive'
    path.write_bytes(b'fixed')
    expected = relocation.signature(path)
    expected[0] += 1
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda *_args: False)
    assert not delta.same_E_generation(path, expected)


def test_difference_archive_fails_closed_if_original_generation_changed(tmp_path, monkeypatch):
    source, target, evidence = fixture_copy(tmp_path)
    (target / 'changed').write_bytes(b'new version')
    audit.compare_fixed_copy(evidence, target, workers=1)
    monkeypatch.setattr(delta, '_validate_native_source', lambda root, intent: None)
    (source / 'changed').write_bytes(b'changed after copy')
    with pytest.raises(ValueError, match='generation'):
        delta.preserve_differences(evidence, source, tmp_path / 'E-archive', admit=lambda size: None)
    assert (source / 'changed').read_bytes() == b'changed after copy'
    assert not (evidence / 'delta-receipt.json').exists()


def test_native_mount_mode_projection_does_not_hide_content_identity_changes():
    original = [10, 123, 0o100600, 8, 30, 40, 0, 0, 1]
    same_file_other_mount = [20, 123, 0o100777, 8, 30, 40, 0, 0, 1]
    assert delta._same_native_file_generation(same_file_other_mount, original)
    same_file_other_mount[4] += 1
    assert not delta._same_native_file_generation(same_file_other_mount, original)
    same_file_other_mount[4] -= 1
    same_file_other_mount[2] = 0o120777
    assert not delta._same_native_file_generation(same_file_other_mount, original)


def test_retired_name_probe_is_one_nonfollowing_lookup_and_keeps_io_errors(tmp_path, monkeypatch):
    path = tmp_path / 'retired'
    original = Path.lstat
    reads = []

    def observed(p, *args, **kwargs):
        reads.append(p)
        return original(p, *args, **kwargs)

    monkeypatch.setattr(Path, 'lstat', observed)
    assert retirement._name_absent(path) is True and reads == [path]
    path.symlink_to(tmp_path / 'missing-target')
    reads.clear()
    assert retirement._name_absent(path) is False and reads == [path]
    monkeypatch.setattr(Path, 'lstat', lambda *_: (_ for _ in ()).throw(PermissionError('unreadable')))
    with pytest.raises(PermissionError):
        retirement._name_absent(path)


def setup_retirement(tmp_path, monkeypatch):
    source, target, evidence = fixture_copy(tmp_path)
    (target / 'changed').write_bytes(b'new version')
    (target / 'missing').unlink()
    audit.compare_fixed_copy(evidence, target, workers=2)
    monkeypatch.setattr(delta, '_validate_native_source', lambda root, intent: None)
    delta.preserve_differences(evidence, source, tmp_path / 'delta', admit=lambda size: None)
    restored = retirement.rehearse_delta_restore(evidence, tmp_path / 'restore', admit=lambda size: None)
    assert restored['state'] == 'member_restore_verified' and restored['files'] == 2
    assert not restored['full_tree_restore_claimed'] and not restored['NAS_acceptance_claimed']
    assert retirement.rehearse_delta_restore(evidence, tmp_path / 'restore', admit=lambda size: None) == restored
    monkeypatch.setattr(retirement, '_check_unreachable_namespaces', lambda: {'count': 0, 'reachable': 0})
    monkeypatch.setattr(retirement, '_check_references', lambda root, rows: None)
    return source, target, evidence


def test_retirement_holds_archived_only_members_until_same_name_is_verified(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=2)
    assert result['state'] == 'waiting_held_members'
    assert (result['files_completed'], result['files_held']) == (1, 2)
    assert (source / 'changed').read_bytes() == b'old version'
    assert (source / 'missing').read_bytes() == b'preserve this'
    assert (target / 'changed').read_bytes() == b'new version'
    # A real owner must reconcile these names. The fixture copies the old
    # versions only to verify resumption once their original names match.
    import shutil
    shutil.copy2(source / 'changed', target / 'changed')
    shutil.copy2(source / 'missing', target / 'missing')
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=2)
    assert result['state'] == 'registered_files_retired' and result['files_completed'] == 3
    assert result['old_root_and_empty_directories_retained']
    assert source.is_dir() and not list(source.iterdir())
    assert (target / 'changed').read_bytes() == b'old version'
    assert result['cold_objects_deleted'] == result['nas_objects_deleted'] == 0


def test_user_enrolled_archived_controls_retire_without_rehashing_archive(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    first = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert first['state'] == 'waiting_held_members' and first['files_held'] == 2
    (target/'missing').write_bytes(b'new live control')
    comparison = json.loads((evidence/'comparison-status.json').read_bytes())
    archive = json.loads((evidence/'delta-receipt.json').read_bytes())
    policy_path = tmp_path/'policy.json'
    policy_path.write_text(json.dumps({'contract': retirement.ARCHIVED_FALLBACK_CONTRACT,
        'comparison_identity_sha256': comparison['identity_sha256'],
        'archive_sha256': archive['archive_sha256'], 'members': ['changed', 'missing']}))
    policy = retirement.load_archived_controls_policy(policy_path, comparison, evidence)
    monkeypatch.setattr(retirement, 'digest', lambda *_: pytest.fail('archive payload was reread'))
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, archived_fallback=policy)
    assert result['state'] == 'registered_files_retired' and result['files_completed'] == 3
    assert result['archived_controls_retired'] == 2
    assert result['archived_controls_full_payload_reread'] is False
    assert not any(path.is_file() for path in source.iterdir())
    assert (target/'changed').read_bytes() == b'new version'
    assert (target/'missing').read_bytes() == b'new live control'
    assert Path(archive['archive']).exists()


def test_archived_control_fallback_keeps_missing_live_name_and_rejects_wrong_identity(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    comparison = json.loads((evidence/'comparison-status.json').read_bytes())
    archive = json.loads((evidence/'delta-receipt.json').read_bytes())
    policy_path = tmp_path/'policy.json'
    policy = {'contract': retirement.ARCHIVED_FALLBACK_CONTRACT,
        'comparison_identity_sha256': comparison['identity_sha256'],
        'archive_sha256': archive['archive_sha256'], 'members': ['changed', 'missing']}
    policy_path.write_text(json.dumps({**policy, 'archive_sha256': '0'*64}))
    with pytest.raises(ValueError, match='policy differs'):
        retirement.load_archived_controls_policy(policy_path, comparison, evidence)
    policy_path.write_text(json.dumps(policy))
    enrolled = retirement.load_archived_controls_policy(policy_path, comparison, evidence)
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, archived_fallback=enrolled)
    assert result['state'] == 'waiting_held_members' and result['files_held'] == 1
    assert (source/'missing').read_bytes() == b'preserve this'
    assert not (source/'changed').exists()


def test_archived_control_fallback_detects_changed_archive_and_reappearing_names(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    first = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert first['files_held'] == 2
    (target/'missing').write_bytes(b'new live control')
    comparison = json.loads((evidence/'comparison-status.json').read_bytes())
    archive = json.loads((evidence/'delta-receipt.json').read_bytes())
    policy = {'contract': retirement.ARCHIVED_FALLBACK_CONTRACT,
        'comparison_identity_sha256': comparison['identity_sha256'],
        'archive_sha256': archive['archive_sha256'], 'members': ['changed', 'missing']}
    with open(archive['archive'], 'r+b') as stream:
        stream.seek(4096)
        stream.write(b'X')
    with pytest.raises(ValueError, match='archive'):
        retirement.retire_fixed_copy(evidence, source, target, workers=2, archived_fallback=policy)
    assert (source/'changed').exists() and (source/'missing').exists()


def test_archived_control_fallback_does_not_claim_absence_for_recreated_names(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    first = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert first['files_held'] == 2
    (source/'same').write_bytes(b'recreated')
    (target/'missing').write_bytes(b'new live control')
    comparison = json.loads((evidence/'comparison-status.json').read_bytes())
    archive = json.loads((evidence/'delta-receipt.json').read_bytes())
    policy = {'contract': retirement.ARCHIVED_FALLBACK_CONTRACT,
        'comparison_identity_sha256': comparison['identity_sha256'],
        'archive_sha256': archive['archive_sha256'], 'members': ['changed', 'missing']}
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1,
                                          archived_fallback=policy)
    assert result['state'] == 'waiting_unexpected_members'
    assert (source/'same').read_bytes() == b'recreated'


def test_retirement_holds_newly_changed_E_and_can_resume(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    original_mtime = (target / 'same').stat().st_mtime_ns
    (target / 'same').write_bytes(b'changed since comparison')
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=2)
    assert result['state'] == 'waiting_held_members' and result['files_held'] == 3
    assert (source / 'same').read_bytes() == b'unchanged'
    assert (source / 'changed').exists() and (source / 'missing').exists()
    (target / 'same').write_bytes(b'unchanged')
    os.utime(target / 'same', ns=(original_mtime, original_mtime))
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=2)
    assert result['state'] == 'waiting_held_members' and result['files_held'] == 2
    import shutil
    shutil.copy2(source / 'changed', target / 'changed')
    shutil.copy2(source / 'missing', target / 'missing')
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=2)
    assert result['state'] == 'registered_files_retired' and result['files_completed'] == 3


def test_retirement_skips_process_rescan_for_finished_batches_but_detects_reappearance(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1)
    assert (result['files_completed'], result['files_held']) == (1, 2)
    checks = []
    # Exercise a full retry below the separately tested no-action hold shortcut.
    monkeypatch.setattr(retirement, '_waiting_held_copy', lambda *a: None)
    monkeypatch.setattr(retirement, '_check_references', lambda root, rows: checks.append(True))
    retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1)
    # One initial whole-inventory check and one per unfinished batch.
    assert len(checks) == 3
    (source / 'same').write_bytes(b'recreated')
    with pytest.raises(ValueError, match='reappeared'):
        retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1)
    assert (source / 'same').read_bytes() == b'recreated'


def test_retirement_does_not_unlink_when_E_delta_changed(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    receipt = json.loads((evidence / 'delta-receipt.json').read_text())
    with open(receipt['archive'], 'r+b') as file:
        file.seek(100)
        file.write(b'tamper')
    with pytest.raises(ValueError, match='archive'):
        retirement.retire_fixed_copy(evidence, source, target)
    assert all((source / name).exists() for name in ('same', 'changed', 'missing'))


def test_retirement_holds_E_member_with_metadata_only_drift(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    mode = (target / 'same').stat().st_mode
    os.chmod(target / 'same', 0o600 if mode & 0o777 != 0o600 else 0o644)
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert result['state'] == 'waiting_held_members' and result['files_held'] == 3
    assert (source / 'same').exists()


def test_retirement_does_not_unlink_with_live_reference(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    def blocked(root, rows):
        raise BlockingIOError('still referenced')
    monkeypatch.setattr(retirement, '_check_references', blocked)
    with pytest.raises(BlockingIOError, match='referenced'):
        retirement.retire_fixed_copy(evidence, source, target)
    assert all((source / name).exists() for name in ('same', 'changed', 'missing'))


def test_unreachable_but_still_mounted_namespace_blocks_retirement(monkeypatch):
    from stockagent.data_sync import transport_relocation as relocation
    from stockagent.data_sync import transport_placement as placement
    monkeypatch.setattr(relocation, 'stale_transport_namespaces', lambda: {
        'count': 1, 'reachable': 0, 'inspection_complete': True})
    monkeypatch.setattr(relocation, 'syncthing_alias_matches', lambda _: True)
    monkeypatch.setattr(placement, 'backing', lambda: Path('/fixed/E'))
    with pytest.raises(BlockingIOError, match='namespace'):
        retirement._check_unreachable_namespaces()


def test_retirement_resumes_after_unlink_before_journal_commit(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    import shutil
    shutil.copy2(source / 'changed', target / 'changed')
    shutil.copy2(source / 'missing', target / 'missing')
    original = Path.unlink
    interrupted = []

    def unlink_then_crash(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path.parent == source and not interrupted:
            interrupted.append(path)
            raise OSError('simulated interruption after unlink')
        return result

    monkeypatch.setattr(Path, 'unlink', unlink_then_crash)
    with pytest.raises(OSError, match='simulated interruption'):
        retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=3)
    assert len(interrupted) == 1 and not interrupted[0].exists()
    monkeypatch.setattr(Path, 'unlink', original)
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=3)
    assert result['state'] == 'registered_files_retired' and result['files_completed'] == 3


def test_retirement_overlaps_only_durably_prepared_independent_unlinks(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    import shutil
    shutil.copy2(source / 'changed', target / 'changed')
    shutil.copy2(source / 'missing', target / 'missing')
    original = Path.unlink
    overlap = threading.Barrier(2)
    lock = threading.Lock()
    checked = []

    def observed_unlink(path, *args, **kwargs):
        if path.parent == source:
            with sqlite3.connect(evidence / 'retirement.sqlite') as observer:
                assert observer.execute('SELECT state FROM members WHERE path=?',
                                        (path.name,)).fetchone() == ('prepared',)
            with lock:
                checked.append(path.name)
                first_pair = len(checked) <= 2
            if first_pair:
                overlap.wait(timeout=5)  # A serial implementation cannot pass.
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'unlink', observed_unlink)
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=3)
    assert result['state'] == 'registered_files_retired' and result['files_completed'] == 3
    assert set(checked) == {'same', 'changed', 'missing'}
    assert all((target / name).is_file() for name in checked)


def test_held_retry_does_not_rescan_retired_D_or_renew_the_progress_clock(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    first = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert first['state'] == 'waiting_held_members' and first['files_held'] == 2
    monkeypatch.setattr(retirement, '_delta_manifest', lambda *a: pytest.fail('unchanged hold reread archive'))
    monkeypatch.setattr(retirement, '_check_references', lambda *a: pytest.fail('unchanged hold repeated D census'))
    again = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert again == first  # No heartbeat masquerades as new retirement progress.
    wait = json.loads((evidence / 'retirement-wait-status.json').read_bytes())
    assert wait['held_files_checked'] == 2 and wait['D_census_repeated'] is False
    assert wait['deletion_attempted'] is False and wait['recovery_acceptance_renewed'] is False
    assert (source / 'changed').read_bytes() == b'old version'
    assert (source / 'missing').read_bytes() == b'preserve this'


def test_no_action_hold_observation_avoids_owner_but_never_approves_deletion(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    first = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert first['files_held'] == 2
    monkeypatch.setattr(retirement, '_delta_manifest', lambda *a: pytest.fail('must not reopen archive'))
    monkeypatch.setattr(retirement, '_check_references', lambda *a: pytest.fail('must not rescan D'))
    observed = retirement.observe_unchanged_held_copy(evidence, source, target, workers=2)
    assert observed == first
    wait = json.loads((evidence / 'retirement-wait-status.json').read_text())
    assert wait['deletion_attempted'] is False and wait['recovery_acceptance_renewed'] is False
    assert (source / 'changed').exists() and (source / 'missing').exists()
    assert not (evidence / 'retirement-receipt.json').exists()
    import shutil
    shutil.copy2(source / 'missing', target / 'missing')
    assert retirement.observe_unchanged_held_copy(evidence, source, target, workers=2) is None
    # Newly usable E data requires the caller to acquire every original lock
    # and run the complete destructive-path checks; observation does not act.
    assert (source / 'missing').exists()


def test_exact_archived_partials_restore_to_e_then_use_original_retirement(tmp_path, monkeypatch):
    import contextlib
    import fcntl
    import hashlib
    from stockagent.data_sync import transport_restore_held_partials as staged

    source, target, evidence = tmp_path / 'D', tmp_path / 'E', tmp_path / 'evidence'
    wave = 'd' * 64
    payload = source / '.staging' / ('lake-raw-' + wave) / 'payload'
    payload.mkdir(parents=True)
    first = b'data'; second = b'\0' * 4
    (payload / ('blobs-' + hashlib.sha256(first).hexdigest() + '.partial')).write_bytes(first)
    # An empty interrupted fragment can have a different full-object SHA.
    (payload / ('blobs-' + hashlib.sha256(second).hexdigest() + '.partial')).write_bytes(b'')
    relocation.copy(source, target, evidence, workers=1)
    for path in (target / '.staging' / ('lake-raw-' + wave) / 'payload').iterdir():
        path.unlink()
    (target / '.stignore').write_text('(?d).staging\n(?d).staging/**\n')
    comparison = audit.compare_fixed_copy(evidence, target, workers=1)
    monkeypatch.setattr(delta, '_validate_native_source', lambda root, intent: None)
    archived = delta.preserve_differences(evidence, source, tmp_path / 'delta', admit=lambda size: None)
    retirement.rehearse_delta_restore(evidence, tmp_path / 'restore', admit=lambda size: None)
    monkeypatch.setattr(retirement, '_check_unreachable_namespaces', lambda: {'count': 0, 'reachable': 0})
    monkeypatch.setattr(retirement, '_check_references', lambda *args: None)
    held = retirement.retire_fixed_copy(evidence, source, target, workers=1)
    assert held['files_held'] == 2
    for name, value in [('COMPARISON', comparison['identity_sha256']), ('ARCHIVE_SHA', archived['archive_sha256']),
                        ('WAVE', wave), ('FILES', 2), ('BYTES', 4), ('CHUNK_BYTES', 4)]:
        monkeypatch.setattr(staged, name, value)
    assert not staged.needs_restore(evidence, target, comparison)
    body = {'contract': staged.CONTRACT, 'comparison_identity_sha256': staged.COMPARISON,
            'archive_sha256': staged.ARCHIVE_SHA, 'wave_intent_sha256': staged.WAVE,
            'files': 2, 'logical_bytes': 4, 'restore_to_sender_staging': True,
            'retirement_authorized_by_restore': False}
    body['identity_sha256'] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    (evidence / 'partial-name-restore-enrollment.json').write_text(json.dumps(body))
    state, archive_state = tmp_path / 'backup-state', tmp_path / 'lake-state'
    state.mkdir(); archive_state.mkdir()
    (archive_state / 'source-wave-current.json').write_text(json.dumps({'state': 'enrolled', 'intent_sha256': 'c' * 64}))
    config = {'state_root': str(state), 'immutable_archive_state_root': str(archive_state)}
    paths = (state / 'cycle.lock', state / 'owner.lock', archive_state / 'source-replication-owner.lock')
    assert staged.needs_restore(evidence, target, comparison)
    with pytest.raises(ValueError, match='lock set'):
        staged.restore(evidence, target, config, (), admit=lambda size: None)
    with contextlib.ExitStack() as stack:
        locks = [stack.enter_context(p.open('a+')) for p in paths]
        for lock in locks:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current = archive_state / 'source-wave-current.json'
        current.write_text(json.dumps({'state': 'copying', 'intent_sha256': wave}))
        with pytest.raises(BlockingIOError, match='current or unknown'):
            staged.restore(evidence, target, config, locks, admit=lambda size: None)
        current.write_text(json.dumps({'state': 'enrolled', 'intent_sha256': 'c' * 64}))
        conflict = target / '.staging' / ('lake-raw-' + wave) / 'payload' / next(payload.iterdir()).name
        conflict.write_bytes(b'newer, do not overwrite')
        with pytest.raises(ValueError, match='never overwrite'):
            staged.restore(evidence, target, config, locks, admit=lambda size: None)
        assert conflict.read_bytes() == b'newer, do not overwrite'
        # If archive order placed the other file first, that valid file was
        # installed before the conflict was encountered. Reset the synthetic
        # E tree so the following test interrupts the second installation.
        for path in conflict.parent.iterdir():
            path.unlink()
        original = staged._link_anonymous
        calls = []
        def interrupt_once(fd, directory, name):
            calls.append(name)
            if len(calls) == 2:
                raise RuntimeError('simulated interruption')
            original(fd, directory, name)
        monkeypatch.setattr(staged, '_link_anonymous', interrupt_once)
        with pytest.raises(RuntimeError, match='simulated interruption'):
            staged.restore(evidence, target, config, locks, admit=lambda size: None)
        assert staged.needs_restore(evidence, target, comparison)
        monkeypatch.setattr(staged, '_link_anonymous', original)
        result = staged.restore(evidence, target, config, locks, admit=lambda size: None)
        assert result['state'] == 'restored' and result['files_completed'] == 2
        assert result['source_files_deleted'] == 0 and not result['retirement_authorized']
        assert not staged.needs_restore(evidence, target, comparison)
        status_path = evidence/'partial-name-restore-status.json'
        status = json.loads(status_path.read_text())
        original_status = json.dumps(status)
        for member in status['members']:
            member['signature'][0] += 1
        status_path.write_text(json.dumps(status))
        monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda *_args: True)
        assert not staged.needs_restore(evidence, target, comparison)
        status['members'][0]['signature'][1] += 1
        status_path.write_text(json.dumps(status))
        assert staged.needs_restore(evidence, target, comparison)
        status_path.write_text(original_status)
        assert all(path.exists() for path in payload.iterdir())
        # Only the established retirement owner deletes after every original gate.
        result = retirement.retire_fixed_copy(evidence, source, target, workers=1)
        assert result['state'] == 'registered_files_retired' and result['files_completed'] == 2


def test_changed_held_target_reenters_all_retirement_gates(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    retirement.retire_fixed_copy(evidence, source, target, workers=2)
    import shutil
    shutil.copy2(source / 'missing', target / 'missing')
    refs = []
    monkeypatch.setattr(retirement, '_check_references', lambda *a: refs.append('checked'))
    result = retirement.retire_fixed_copy(evidence, source, target, workers=2)
    assert refs and result['files_completed'] == 2 and result['files_held'] == 1
    assert not (source / 'missing').exists() and (source / 'changed').exists()


def test_mismatched_held_journal_cannot_enable_no_action_shortcut(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    retirement.retire_fixed_copy(evidence, source, target, workers=2)
    with sqlite3.connect(evidence / 'retirement.sqlite') as journal:
        journal.execute('UPDATE metadata SET identity=?', ('f' * 64,))
    with pytest.raises(ValueError, match='another inventory'):
        retirement.retire_fixed_copy(evidence, source, target, workers=2)


def test_no_action_hold_never_accepts_recreated_names_and_full_retry_detects_them(tmp_path, monkeypatch):
    source, target, evidence = setup_retirement(tmp_path, monkeypatch)
    first = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1)
    (source / 'same').write_bytes(b'recreated, preserve this')
    unchanged = retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1)
    assert unchanged == first and unchanged['state'] == 'waiting_held_members'
    assert not (evidence / 'retirement-receipt.json').exists()
    assert (source / 'same').read_bytes() == b'recreated, preserve this'
    import shutil
    shutil.copy2(source / 'missing', target / 'missing')
    with pytest.raises(ValueError, match='reappeared'):
        retirement.retire_fixed_copy(evidence, source, target, workers=2, batch_size=1)
    assert (source / 'same').read_bytes() == b'recreated, preserve this'
