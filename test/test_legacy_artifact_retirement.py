from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import stockagent.data_sync.legacy_artifact_retirement as retirement
import stockagent.data_sync.cold_primary as cold_primary
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, publish_archive, restore_archive
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, resolve_latest_packed
from dataclasses import replace


def test_binding_parallelism_only_for_exact_enrolled_d_stage():
    from stockagent.data_sync.legacy_artifact_archive import D_BENCHMARK_STAGE_ROOT

    spec = LegacyArchiveSpec('legacy-test', 'benchmarks/example', 7, D_BENCHMARK_STAGE_ROOT)
    archive = D_BENCHMARK_STAGE_ROOT / spec.dataset / 'archive'
    assert retirement._archive_binding_workers(spec, archive) == 8
    assert retirement._archive_binding_workers(spec, archive.parent / 'other') == 1
    assert retirement._archive_binding_workers(replace(spec, stage_root=Path('/tmp/other')), archive) == 1
    quarantine = D_BENCHMARK_STAGE_ROOT / f'.{spec.dataset}.pruning-{os.getpid()}' / 'archive'
    assert retirement._archive_binding_workers(spec, quarantine) == 8


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    repo_root = tmp_path / "repo"
    artifact_root = repo_root / "artifacts"
    source = artifact_root / "markets/crypto"
    source.mkdir(parents=True)
    original = source / "checkpoint.pt"
    original.write_bytes(b"old unfinished run; preserve bytes, not deployment permission")
    old = time.time() - 10 * 86_400
    os.utime(original, (old, old))
    hot_root = tmp_path / "hot"
    hot_tree = hot_root / "markets/crypto"
    hot_tree.mkdir(parents=True)
    os.link(original, hot_tree / original.name)
    spec = LegacyArchiveSpec("legacy-artifact-markets-crypto", "markets/crypto", 7, tmp_path / "stage")
    sync_root = tmp_path / "packed"
    initialize_packed_layout(sync_root, node_id="penguin")
    publish_archive(spec, artifact_root, sync_root, repo_root=repo_root)
    backup = tmp_path / "backup"
    shutil.copytree(sync_root, backup)
    monkeypatch.setattr(
        cold_primary.BackupConfig,
        "load",
        lambda _path: SimpleNamespace(source=sync_root, destination=backup),
    )
    monkeypatch.setattr(cold_primary.VolumeGuard, "check", lambda self: None)
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: [])
    monkeypatch.setattr(retirement, "process_references", lambda *args: [])
    options = {
        "repo_root": repo_root,
        "artifact_root": artifact_root,
        "hot_root": hot_root,
        "sync_root": sync_root,
        "materialized_root": tmp_path / "materialized",
        "state_root": tmp_path / "state",
        "activation_root": tmp_path / "activations",
        "backup_config": tmp_path / "backup-config.json",
        "peer_proof": {
            "ok": True,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "peers": [{"name": "vastai1T", "ok": True}],
        },
        "bridge_inactive": True,
    }
    return spec, source, hot_tree, options


@pytest.mark.parametrize('interrupted,defect', [
    (False, None), (True, None), (False, 'state-origin'), (True, 'state-origin'),
    (False, 'producer-receipt'), (True, 'producer-receipt'),
    (True, 'producer-receipt-after-audit'),
])
def test_adjacent_completion_survives_real_quarantine_and_exact_cold_recovery(
        tmp_path, monkeypatch, interrupted, defect):
    """Exercise actual prepare/publish/rename/unlink, not a mocked outer CLI."""
    import stockagent.data_sync.artifact_retirement as common
    from stockagent.data_sync.benchmark_fixtures import (
        FINMIND_GROUP_STATUS_CONTRACT, completed_benchmark_evidence,
    )
    from stockagent.data_sync.legacy_artifact_archive import MANUAL_WSL_CAPTURE_CONTRACT
    from test_benchmark_fixture_reuse import _finmind_group_case

    source, reports, _ = _finmind_group_case(tmp_path)
    artifact_root = tmp_path / 'artifacts'
    original = b'exact offline benchmark bytes, with external original completion metadata'
    (source / 'original.bin').write_bytes(original)
    old = time.time() - 2 * 86400
    os.utime(source / 'original.bin', (old, old))
    spec = LegacyArchiveSpec('legacy-wsl-group', source.relative_to(artifact_root).as_posix(),
        7, tmp_path / 'stage', manual_capture_min_stable_hours=12,
        capture_contract=MANUAL_WSL_CAPTURE_CONTRACT)
    saved = completed_benchmark_evidence(source, destination=tmp_path / 'retained-evidence')
    raw_reports = {p.name: p.read_bytes() for p in reports}
    sync_root = tmp_path / 'packed'
    release = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path, manual_capture=True)
    backup = tmp_path / 'backup'
    shutil.copytree(sync_root, backup)
    monkeypatch.setattr(cold_primary.BackupConfig, 'load',
                        lambda _: SimpleNamespace(source=sync_root, destination=backup))
    monkeypatch.setattr(cold_primary.VolumeGuard, 'check', lambda self: None)
    options = dict(repo_root=tmp_path, artifact_root=artifact_root, hot_root=tmp_path / 'hot',
        sync_root=sync_root, materialized_root=tmp_path / 'materialized',
        state_root=tmp_path / 'state', activation_root=tmp_path / 'activations',
        backup_config=tmp_path / 'backup-config.json', bridge_inactive=True,
        manual_immediate=True, manual_capture=True,
        peer_proof={'ok': True, 'checked_at': datetime.now(timezone.utc).isoformat(), 'peers': []})
    # The fixture has no external consumers; final-gate rejection remains tested
    # by the existing process/pin/peer/service regression cases below.
    monkeypatch.setattr(retirement, 'artifact_process_references', lambda *args: [])
    monkeypatch.setattr(retirement, 'process_references', lambda *args: [])
    monkeypatch.setattr(common, 'artifact_process_references', lambda *args: [])
    monkeypatch.setattr(common, 'process_references', lambda *args: [])
    plan = retirement.plan_legacy_retirement(spec, **options)
    state_path = retirement._state_path(options['state_root'], spec.dataset)
    if interrupted:
        original_gate = retirement._assert_unlink_gates
        def fail_after_rename(*args, **kwargs):
            raise SnapshotError('controlled final-gate interruption')
        monkeypatch.setattr(retirement, '_assert_unlink_gates', fail_after_rename)
        with pytest.raises(SnapshotError, match='controlled final-gate'):
            retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                              owned_verified_plan=plan, **options)
        monkeypatch.setattr(retirement, '_assert_unlink_gates', original_gate)
        state = json.loads(state_path.read_bytes())
        quarantine = Path(state['quarantine'])
        assert not source.exists() and (quarantine / 'source/original.bin').read_bytes() == original
        if defect == 'state-origin':
            state['relative_root'] = state['relative_root'].replace('20261015', '20261016')
            state_path.write_text(json.dumps(state))
        elif defect == 'producer-receipt':
            changed = json.loads(reports[1].read_bytes())
            changed['exact_output_parity'] = False
            reports[1].write_text(json.dumps(changed))
        if defect and defect != 'producer-receipt-after-audit':
            with pytest.raises(SnapshotError):
                retirement.plan_legacy_quarantine_resume(spec, plan, **options)
            assert (quarantine / 'source/original.bin').read_bytes() == original
            assert json.loads(state_path.read_bytes())['state'] == 'retiring'
            return
        plan = retirement.plan_legacy_quarantine_resume(spec, plan, **options)
        if defect == 'producer-receipt-after-audit':
            changed = json.loads(reports[1].read_bytes())
            # The completion ABI still passes, but its exact frozen bytes
            # changed after the full pre-lock audit. Never reuse that proof.
            changed['later_annotation'] = 'producer metadata changed after full audit'
            reports[1].write_text(json.dumps(changed))
            with pytest.raises(SnapshotError, match='generation changed'):
                retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                                  owned_verified_plan=plan, **options)
            assert (quarantine / 'source/original.bin').read_bytes() == original
            return
    elif defect:
        # Reject the same bad binding during the actual post-rename audit.
        original_verify = retirement.verify_archive_directory
        def verify(archive, current, **kwargs):
            if current.name == 'source':
                if defect == 'state-origin':
                    state = json.loads(state_path.read_bytes())
                    state['relative_root'] = state['relative_root'].replace('20261015', '20261016')
                    state_path.write_text(json.dumps(state))
                else:
                    changed = json.loads(reports[1].read_bytes())
                    changed['exact_output_parity'] = False
                    reports[1].write_text(json.dumps(changed))
            return original_verify(archive, current, **kwargs)
        monkeypatch.setattr(retirement, 'verify_archive_directory', verify)
        with pytest.raises(SnapshotError):
            retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                              owned_verified_plan=plan, **options)
        state = json.loads(state_path.read_bytes())
        assert state['state'] == 'retiring'
        assert (Path(state['quarantine']) / 'source/original.bin').read_bytes() == original
        return
    result = retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                               owned_verified_plan=plan, **options)
    assert result['deleted'] is True and not source.exists()
    assert result.get('resumed_quarantine', False) == interrupted
    assert json.loads(state_path.read_bytes())['state'] == 'cold-only'
    assert result['snapshot_id'] == release['snapshot_id']
    assert {p.name: p.read_bytes() for p in reports} == raw_reports
    assert completed_benchmark_evidence(tmp_path / 'retained-evidence', original_source=source,
        required_contract=FINMIND_GROUP_STATUS_CONTRACT) == saved
    destination = tmp_path / 'independent-restored'
    restore_archive(spec, sync_root, destination, materialized_root=tmp_path / 'recovery')
    assert (destination / 'original.bin').read_bytes() == original
    assert {p.name for p in destination.iterdir()} == {'original.bin', '.LEGACY_RESTORED.json'}


@pytest.mark.parametrize('late_change', [None, 'process', 'pin', 'stale', 'timeout', 'error', 'incomplete'])
def test_idle_scan_wait_rechecks_all_unlink_gates_and_never_accepts_failed_health(tmp_path, monkeypatch, late_change):
    import stockagent.data_sync.artifact_retirement as common
    from types import SimpleNamespace

    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    quarantine = tmp_path/'quarantine'
    quarantine.mkdir()
    state = {'elapsed': 0, 'healthy': False}
    sleeps, probes, process_checks = [], [], []
    def process_refs(*args):
        process_checks.append(state['elapsed'])
        return ['new-open-fd'] if state['elapsed'] and late_change == 'process' else []
    monkeypatch.setattr(common, 'artifact_process_references', process_refs)
    monkeypatch.setattr(common, 'process_references', lambda *args: [])
    monkeypatch.setattr(common, '_pinned_snapshot_ids', lambda *args:
                        {plan['snapshot_id']} if state['elapsed'] and late_change == 'pin' else set())
    def probe():
        probes.append(state['elapsed'])
        checks = dict(folder_idle=state['healthy'], need_bytes_zero=True, need_items_zero=True,
                      need_deletes_zero=True, errors_zero=True, pull_errors_zero=True,
                      watch_error_empty=True, folder_errors_empty=True, system_errors_empty=True)
        if late_change == 'error':
            checks['errors_zero'] = False
        if late_change == 'incomplete':
            checks.pop('watch_error_empty')
        return {'ok': all(checks.values()), 'local_checks': checks,
                'checked_at': ('2000-01-01T00:00:00+00:00' if state['healthy'] and late_change == 'stale'
                               else datetime.now(timezone.utc).isoformat()), 'peers': []}
    def sleep(seconds):
        sleeps.append(seconds)
        state['elapsed'] += seconds
        state['healthy'] = late_change != 'timeout'
    monkeypatch.setattr(retirement, 'time', SimpleNamespace(
        monotonic=lambda: state['elapsed'], sleep=sleep))
    options['peer_probe'] = probe
    if late_change is None:
        result = retirement._assert_legacy_unlink_gates(spec, plan, quarantine, **options)
        assert result['ok'] is True and sleeps == [10]
    else:
        with pytest.raises(SnapshotError):
            retirement._assert_legacy_unlink_gates(spec, plan, quarantine, **options)
        if late_change in ('error', 'incomplete'):
            assert not sleeps
        elif late_change == 'timeout':
            assert sum(sleeps) == 300 and state['elapsed'] == 300
        else:
            assert sleeps == [10]
    assert process_checks[0] == 0
    if sleeps:
        assert process_checks[-1] == state['elapsed']  # Usage is checked AFTER waiting.
    assert source.is_dir() and hot.is_dir() and quarantine.is_dir()
    assert probes and options['peer_proof']['ok'] is True  # Old health never grants deletion.


@pytest.mark.parametrize('change', ['quarantine-bytes', 'cold', 'expired'])
def test_final_scan_gate_cannot_extend_full_proof_or_hide_generation_changes(tmp_path, monkeypatch, change):
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    original_gate = retirement._assert_legacy_unlink_gates
    def late_change(spec, current_plan, quarantine, **kwargs):
        proof = original_gate(spec, current_plan, quarantine, **kwargs)
        if change == 'expired':
            current_plan['full_verification']['verified_at_epoch'] = time.time()-1501
        else:
            if change == 'quarantine-bytes':
                target = quarantine/'source/checkpoint.pt'
            else:
                resolved = resolve_latest_packed(options['sync_root'], spec.dataset)
                target = options['sync_root']/resolved.manifest['archive']['objects'][0]['relpath']
            info = target.stat()
            target.write_bytes(b'X'*info.st_size)
            os.utime(target, ns=(info.st_atime_ns, info.st_mtime_ns))
        return proof
    monkeypatch.setattr(retirement, '_assert_legacy_unlink_gates', late_change)
    with pytest.raises(SnapshotError, match='changed or full audit expired'):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                          owned_verified_plan=plan, **options)
    state = json.loads(retirement._state_path(options['state_root'], spec.dataset).read_text())
    assert state['state'] == 'retiring' and not source.exists() and not hot.exists()
    assert (Path(state['quarantine'])/'source/checkpoint.pt').is_file()


@pytest.mark.parametrize('defect', [None, 'source-bytes', 'cold-bytes', 'pin'])
def test_owned_post_rename_audit_runs_outside_shared_owner_and_preserves_gates(tmp_path, monkeypatch, defect):
    import fcntl
    import stockagent.data_sync.artifact_retirement as common
    from scripts import retire_local_offline_artifacts as cleanup

    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    original_verified_at = plan['full_verification']['verified_at_epoch']
    target = tmp_path / 'receipt'
    target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    monkeypatch.setattr(cleanup, '_wait_for_convergence', lambda *a: {'ok': True})
    apply = cleanup.apply_legacy_retirement
    verify = retirement.verify_packed_snapshot
    transitions, full_audits = [], []

    def after_rename(spec, **kwargs):
        try:
            return apply(spec, **kwargs)
        except retirement.LegacyQuarantineAuditRequired:
            state = json.loads(retirement._state_path(options['state_root'], spec.dataset).read_bytes())
            quarantine = Path(state['quarantine'])
            assert state['state'] == 'retiring' and not source.exists() and not hot.exists()
            assert (quarantine / 'source/checkpoint.pt').is_file()
            transitions.append(quarantine)
            if defect == 'source-bytes':
                path = quarantine / 'source/checkpoint.pt'
                info = path.stat()
                path.write_bytes(b'X' * info.st_size)
                os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
            elif defect == 'cold-bytes':
                release = resolve_latest_packed(options['sync_root'], spec.dataset)
                path = options['sync_root'] / release.manifest['archive']['objects'][0]['relpath']
                info = path.stat()
                path.write_bytes(b'X' * info.st_size)
                os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
            elif defect == 'pin':
                monkeypatch.setattr(common, '_pinned_snapshot_ids', lambda *a: {plan['snapshot_id']})
            raise

    def audited_outside_owner(*args, **kwargs):
        # The complete packed and original audit must not hold the global
        # mutation owner or permit a deletion before this full byte check.
        with cleanup.COMMON_OWNER.open('a') as probe:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert len(transitions) == 1 and (transitions[0] / 'source/checkpoint.pt').is_file()
        full_audits.append(True)
        return verify(*args, **kwargs)

    monkeypatch.setattr(cleanup, 'apply_legacy_retirement', after_rename)
    monkeypatch.setattr(retirement, 'verify_packed_snapshot', audited_outside_owner)
    call = lambda: cleanup.retire_after_convergence(
        spec, None, target, plan, options, separate_quarantine_audit=True)
    if defect:
        with pytest.raises(SnapshotError):
            call()
        assert transitions and (transitions[0] / 'source/checkpoint.pt').is_file()
        assert json.loads(retirement._state_path(options['state_root'], spec.dataset).read_bytes())['state'] == 'retiring'
    else:
        result = call()
        assert result['deleted'] and result['resumed_quarantine'] and full_audits == [True]
        assert not transitions[0].exists() and not source.exists() and not hot.exists()
        assert result['full_verification']['contract'] == 'owned-legacy-quarantine-resume-v2'
        restored = tmp_path / 'restored-after-staged-retirement'
        restore_archive(spec, options['sync_root'], restored, materialized_root=tmp_path / 'restore-cache')
        assert (restored / 'checkpoint.pt').read_bytes().startswith(b'old unfinished run')
    assert plan['full_verification']['verified_at_epoch'] == original_verified_at


def test_post_rename_deferral_rejects_automatic_or_unowned_plan_before_mutation(tmp_path, monkeypatch):
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    plan = retirement.plan_legacy_retirement(spec, **options)
    with pytest.raises(SnapshotError, match='exact owned manual plan'):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                           owned_verified_plan=plan, defer_owned_post_rename_audit=True,
                                           **options)
    assert source.is_dir() and hot.is_dir()


@pytest.mark.parametrize('late_change', [None, 'source', 'pin', 'process', 'expired'])
def test_owned_resume_wait_releases_common_owner_without_repeating_full_bytes(
        tmp_path, monkeypatch, late_change):
    import fcntl
    import stockagent.data_sync.artifact_retirement as common
    from scripts import retire_local_offline_artifacts as cleanup

    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    original = (source / 'checkpoint.pt').read_bytes()
    plan = retirement.plan_legacy_retirement(spec, **options)
    final_gate = retirement._assert_unlink_gates
    monkeypatch.setattr(retirement, '_assert_unlink_gates', lambda *a, **kw:
                        (_ for _ in ()).throw(SnapshotError('controlled interruption')))
    with pytest.raises(SnapshotError, match='controlled interruption'):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                          owned_verified_plan=plan, **options)
    monkeypatch.setattr(retirement, '_assert_unlink_gates', final_gate)
    state_path = retirement._state_path(options['state_root'], spec.dataset)
    quarantine = Path(json.loads(state_path.read_bytes())['quarantine'])
    current = {'healthy': False}

    def probe():
        checks = dict(folder_idle=current['healthy'], need_bytes_zero=True, need_items_zero=True,
                      need_deletes_zero=True, errors_zero=True, pull_errors_zero=True,
                      watch_error_empty=True, folder_errors_empty=True, system_errors_empty=True)
        return {'ok': all(checks.values()), 'local_checks': checks,
                'checked_at': datetime.now(timezone.utc).isoformat(), 'peers': []}

    options['peer_probe'] = probe
    monkeypatch.setattr(retirement.time, 'sleep', lambda seconds:
                        pytest.fail('owned resume must wait outside the common mutation owner'))
    # A healthy scan cannot discard the completed byte audit. It remains an
    # explicit blocker and cannot itself authorize any unlink.
    resumed = retirement.plan_legacy_quarantine_resume(spec, plan, **options)
    assert resumed['blockers'] == ['cold-peer-not-converged']
    verified_at = resumed['full_verification']['verified_at_epoch']
    before_state = state_path.read_bytes()
    with pytest.raises(retirement.LegacyQuarantineTransportPending):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
            owned_verified_plan=resumed, defer_quarantine_transport=True, **options)
    assert state_path.read_bytes() == before_state
    assert (quarantine / 'source/checkpoint.pt').read_bytes() == original

    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    waits = []

    def wait(cfg, seconds):
        with cleanup.COMMON_OWNER.open('a') as other_owner:
            fcntl.flock(other_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        waits.append(seconds)
        # First successful preflight races with a new scan at apply. The
        # second wait must happen outside both common and dataset locks.
        if len(waits) > 1:
            current['healthy'] = True
            if late_change == 'source':
                path = quarantine / 'source/checkpoint.pt'
                path.write_bytes(b'X' * len(original))
            elif late_change == 'pin':
                monkeypatch.setattr(common, '_pinned_snapshot_ids', lambda *a: {plan['snapshot_id']})
            elif late_change == 'process':
                monkeypatch.setattr(common, 'artifact_process_references', lambda *a: ['new-open-fd'])
            elif late_change == 'expired':
                monkeypatch.setattr(cleanup.time, 'time', lambda: verified_at + 1501)
        return {'ok': True}

    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(retirement, 'verify_archive_directory', lambda *a, **kw:
                        pytest.fail('do not repeat full byte decoding under the mutation owner'))
    if late_change in {'source', 'pin', 'process'}:
        with pytest.raises(SnapshotError):
            cleanup.retire_after_convergence(spec, None, tmp_path / 'receipt', resumed, options)
    else:
        result = cleanup.retire_after_convergence(spec, None, tmp_path / 'receipt', resumed, options)
        if late_change == 'expired':
            assert result is None
        else:
            assert result['deleted'] and result['resumed_quarantine']
            assert result['manifest_sha256'] == plan['manifest_sha256']
    assert waits and len(waits) == 2
    assert resumed['full_verification']['verified_at_epoch'] == verified_at
    if late_change:
        assert quarantine.exists() and state_path.read_bytes() == before_state
    else:
        assert not quarantine.exists() and not source.exists() and not hot.exists()


@pytest.mark.parametrize('failure', [None, 'source-bytes', 'mirror', 'cold', 'unknown',
    'original-reappeared', 'state', 'expired', 'service', 'pin', 'process', 'peer', 'redirect'])
def test_interrupted_quarantine_resume_reaudits_exact_bytes_and_final_gates(tmp_path, monkeypatch, failure):
    import stockagent.data_sync.artifact_retirement as common
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    original_bytes = (source/'checkpoint.pt').read_bytes()
    plan = retirement.plan_legacy_retirement(spec, **options)
    final_gate = retirement._assert_unlink_gates
    def interrupted(*args, **kwargs):
        raise SnapshotError('controlled transport failure after rename')
    monkeypatch.setattr(retirement, '_assert_unlink_gates', interrupted)
    with pytest.raises(SnapshotError, match='controlled transport'):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                          owned_verified_plan=plan, **options)
    state_path = retirement._state_path(options['state_root'], spec.dataset)
    state = json.loads(state_path.read_text())
    quarantine = Path(state['quarantine'])
    assert state['state']=='retiring' and not source.exists() and not hot.exists()
    assert (quarantine/'source/checkpoint.pt').read_bytes()==original_bytes
    monkeypatch.setattr(retirement, '_assert_unlink_gates', final_gate)
    before_audit = failure in {'source-bytes', 'mirror', 'unknown', 'original-reappeared', 'state', 'redirect'}
    if not before_audit:
        resumed = retirement.plan_legacy_quarantine_resume(spec, plan, **options)
        assert resumed['quarantine_resume'] and resumed['manifest_sha256']==plan['manifest_sha256']
    if failure=='source-bytes':
        file=quarantine/'source/checkpoint.pt'; info=file.stat()
        file.write_bytes(b'X'*info.st_size)
        os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    elif failure=='mirror':
        (quarantine/'hot/unknown.bin').write_bytes(b'unpreserved')
    elif failure=='cold':
        resolved=resolve_latest_packed(options['sync_root'],spec.dataset)
        file=options['sync_root']/resolved.manifest['archive']['objects'][0]['relpath']
        info=file.stat();file.write_bytes(b'X'*info.st_size)
        os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    elif failure=='unknown':
        (quarantine/'unknown-evidence').write_text('retain')
    elif failure=='original-reappeared':
        source.mkdir();(source/'new-generation').write_bytes(b'new bytes')
    elif failure=='state':
        state['plan_fingerprint']='changed';state_path.write_text(json.dumps(state))
    elif failure=='expired':
        resumed['full_verification']['verified_at_epoch']=time.time()-1501
    elif failure=='service':
        monkeypatch.setattr(common,'artifact_service_references',lambda paths,*args:
                            {str(p):['new-web-consumer'] for p in paths})
    elif failure=='pin':
        monkeypatch.setattr(common,'_pinned_snapshot_ids',lambda *args:{plan['snapshot_id']})
    elif failure=='process':
        monkeypatch.setattr(common,'process_references',lambda *args:['live-open-fd'])
    elif failure=='peer':
        options['peer_probe']=lambda:{'ok':False,'checked_at':datetime.now(timezone.utc).isoformat()}
    elif failure=='redirect':
        moved=quarantine.with_name(quarantine.name+'-moved');quarantine.rename(moved)
        quarantine.symlink_to(moved,target_is_directory=True)
    if failure:
        with pytest.raises(SnapshotError):
            if before_audit:
                retirement.plan_legacy_quarantine_resume(spec,plan,**options)
            else:
                retirement.apply_legacy_retirement(spec,expected_fingerprint=plan['plan_fingerprint'],
                                                  owned_verified_plan=resumed,**options)
        assert (quarantine/'source/checkpoint.pt').exists()
        assert json.loads(state_path.read_text())['state']=='retiring'
    else:
        result=retirement.apply_legacy_retirement(spec,expected_fingerprint=plan['plan_fingerprint'],
                                                 owned_verified_plan=resumed,**options)
        assert result['deleted'] and result['resumed_quarantine'] and not quarantine.exists()
        state=json.loads(state_path.read_text())
        assert state['state']=='cold-only' and state['retirement_receipt']==result
        restored=tmp_path/'restored-after-interruption'
        restore_archive(spec,options['sync_root'],restored,materialized_root=tmp_path/'restored-cache')
        assert (restored/'checkpoint.pt').read_bytes()==original_bytes


@pytest.mark.parametrize('defect', [None, 'old-contract', 'manifest-proof', 'file-count',
    'boolean-count', 'byte-count', 'boolean-bytes', 'foreign-pid', 'foreign-start',
    'foreign-boot', 'missing-owner'])
def test_owned_quarantine_resume_decodes_once_outside_mutation_and_binds_full_inventory(
        tmp_path, monkeypatch, defect):
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    expected = (source / 'checkpoint.pt').read_bytes()
    plan = retirement.plan_legacy_retirement(spec, **options)
    gate = retirement._assert_unlink_gates
    def interrupted(*args, **kwargs):
        raise SnapshotError('controlled final-gate interruption')
    monkeypatch.setattr(retirement, '_assert_unlink_gates', interrupted)
    with pytest.raises(SnapshotError, match='controlled final-gate'):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                          owned_verified_plan=plan, **options)
    monkeypatch.setattr(retirement, '_assert_unlink_gates', gate)
    quarantine = Path(json.loads(retirement._state_path(options['state_root'], spec.dataset).read_bytes())['quarantine'])
    calls = []
    mutation = False
    verify = retirement.verify_archive_directory
    def full_audit(*args, **kwargs):
        assert not mutation, 'bulk verification must complete before the mutation owner'
        calls.append(True)
        return verify(*args, **kwargs)
    monkeypatch.setattr(retirement, 'verify_archive_directory', full_audit)
    audit_progress = []
    resumed = retirement.plan_legacy_quarantine_resume(spec, plan, progress=audit_progress.append, **options)
    full = resumed['full_verification']
    assert calls == [True] and full['contract'] == 'owned-legacy-quarantine-resume-v2'
    assert audit_progress[0]['audit_step'] == 'bind-quarantine-and-current-generations'
    assert audit_progress[-1]['audit_step'] == 'full-audit-complete'
    assert any(p.get('stage') == 'decode-originals' for p in audit_progress)
    if defect == 'old-contract': full['contract'] = 'owned-legacy-quarantine-resume-v1'
    elif defect == 'manifest-proof': full['verified_archive_manifest_sha256'] = '0' * 64
    elif defect == 'file-count': full['original_files'] += 1
    elif defect == 'boolean-count': full['original_files'] = True
    elif defect == 'byte-count': full['original_bytes'] += 1
    elif defect == 'boolean-bytes': full['original_bytes'] = True
    elif defect == 'foreign-pid': full['owner_process']['pid'] += 1
    elif defect == 'foreign-start': full['owner_process']['start_ticks'] += 1
    elif defect == 'foreign-boot': full['owner_process']['boot_id'] = 'different-boot'
    elif defect == 'missing-owner': full.pop('owner_process')
    mutation = True
    apply = lambda: retirement.apply_legacy_retirement(spec, expected_fingerprint=plan['plan_fingerprint'],
                                                      owned_verified_plan=resumed, **options)
    if defect:
        with pytest.raises(SnapshotError):
            apply()
        assert (quarantine / 'source/checkpoint.pt').read_bytes() == expected
        assert json.loads(retirement._state_path(options['state_root'], spec.dataset).read_bytes())['state'] == 'retiring'
    else:
        result = apply()
        assert result['deleted'] and result['reused_same_owned_quarantine_full_verification']
        assert not quarantine.exists() and calls == [True]
        destination = tmp_path / 'restored-owned-proof'
        restore_archive(spec, options['sync_root'], destination, materialized_root=tmp_path / 'recovery')
        assert (destination / 'checkpoint.pt').read_bytes() == expected


@pytest.mark.parametrize('new_consumer', [False, True])
def test_unequal_retained_hot_version_is_recovered_and_retired_separately(tmp_path, monkeypatch, new_consumer):
    from stockagent.data_sync.legacy_artifact_archive import MANUAL_WSL_CAPTURE_CONTRACT, verify_cold_archive
    spec, authority, hot, options = _fixture(tmp_path, monkeypatch)
    original = (authority/'checkpoint.pt').read_bytes()
    (hot/'checkpoint.pt').unlink()
    (hot/'checkpoint.pt').write_bytes(b'previous unequal historical version')
    old=time.time()-10*86400;os.utime(hot/'checkpoint.pt',(old,old))
    spec=replace(spec,dataset='legacy-wsl-hot-version',manual_capture_min_stable_hours=12,
                 capture_contract=MANUAL_WSL_CAPTURE_CONTRACT)
    publish_archive(spec, options['hot_root'], options['sync_root'],
                    repo_root=options['repo_root'], manual_capture=True)
    proof=verify_cold_archive(spec,options['sync_root'])
    assert proof['decoded_originals_verified'] and authority.is_dir()
    shutil.copytree(options['sync_root'],tmp_path/'backup',dirs_exist_ok=True)
    options.update(artifact_root=options['hot_root'],hot_root=tmp_path/'no-secondary-mirror',
                   state_root=tmp_path/'hot-version-state',activation_root=tmp_path/'hot-version-activations',
                   manual_immediate=True,manual_capture=True)
    plan=retirement.plan_legacy_retirement(spec,**options)
    assert plan['source']==str(hot) and plan['hot_mirror']['files']==0
    if new_consumer:
        monkeypatch.setattr(retirement,'_active_service_references',lambda *a:['new-web-consumer'])
        with pytest.raises(SnapshotError,match='plan changed|blocked'):
            retirement.apply_legacy_retirement(spec,expected_fingerprint=plan['plan_fingerprint'],
                                               owned_verified_plan=plan,**options)
        assert hot.is_dir()
    else:
        result=retirement.apply_legacy_retirement(spec,expected_fingerprint=plan['plan_fingerprint'],
                                                 owned_verified_plan=plan,**options)
        assert result['deleted'] and not hot.exists()
        restored=tmp_path/'restored-hot-version'
        restore_archive(spec,options['sync_root'],restored,materialized_root=tmp_path/'restore-cache')
        assert (restored/'checkpoint.pt').read_bytes()==b'previous unequal historical version'
    assert (authority/'checkpoint.pt').read_bytes()==original


@pytest.mark.parametrize("failure", [None, "expired", "source", "mirror-directory", "cold", "service", "pin", "automatic"])
def test_owned_manual_full_plan_preserves_final_retirement_gates(tmp_path, monkeypatch, failure):
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    before = retirement.plan_legacy_retirement(spec, **options)
    if failure == "expired":
        before["full_verification"]["verified_at_epoch"] = time.time() - 1501
    elif failure == "source":
        file = source / "checkpoint.pt"
        info = file.stat()
        file.write_bytes(b"X" * info.st_size)
        os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns))
    elif failure == "mirror-directory":
        (hot / "new-empty").mkdir()
    elif failure == "cold":
        resolved = resolve_latest_packed(options["sync_root"], spec.dataset)
        file = options["sync_root"] / resolved.manifest["archive"]["objects"][0]["relpath"]
        info = file.stat()
        file.write_bytes(b"X" * info.st_size)
        os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns))
    elif failure == "service":
        monkeypatch.setattr(retirement, "_active_service_references", lambda *args: ["new-consumer"])
    elif failure == "pin":
        monkeypatch.setattr(retirement, "_pinned_snapshot_ids", lambda *args: {before["snapshot_id"]})
    elif failure == "automatic":
        options["manual_immediate"] = False
    monkeypatch.setattr(retirement, "verify_packed_snapshot", lambda *a, **kw: pytest.fail("full cold audit must run outside the mutation owner"))
    if failure:
        with pytest.raises(SnapshotError):
            retirement.apply_legacy_retirement(spec, expected_fingerprint=before["plan_fingerprint"],
                                               owned_verified_plan=before, **options)
        assert source.exists() and hot.exists()
    else:
        result = retirement.apply_legacy_retirement(spec, expected_fingerprint=before["plan_fingerprint"],
                                                   owned_verified_plan=before, **options)
        assert result["deleted"] and not source.exists() and not hot.exists()
        restored = tmp_path / "restored"
        restore_archive(spec, options["sync_root"], restored, materialized_root=tmp_path / "restore-cache")
        assert (restored / "checkpoint.pt").read_bytes() == b"old unfinished run; preserve bytes, not deployment permission"


def test_observation_enrollment_without_cold_release_deletes_nothing(tmp_path: Path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "markets/us/checkpoint.pt"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"unique legacy bytes")
    old = time.time() - 10 * 86_400
    os.utime(source, (old, old))
    spec = LegacyArchiveSpec("legacy-artifact-markets-us", "markets/us", 7, tmp_path / "stage")
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: [])
    before = time.time_ns()
    result = retirement.enroll_legacy_lease(spec, artifact_root=artifact_root,
                                          state_root=tmp_path / "state")
    assert result["action"] == "enrolled"
    assert result["deleted"] is False and result["cold_verified"] is False
    assert before <= result["last_used_ns"] <= time.time_ns()
    assert source.read_bytes() == b"unique legacy bytes"
    repeated = retirement.enroll_legacy_lease(spec, artifact_root=artifact_root,
                                            state_root=tmp_path / "state")
    assert repeated["action"] == "already-enrolled"
    assert repeated["last_used_ns"] == result["last_used_ns"]


def test_observation_enrollment_refuses_in_use_source(tmp_path: Path, monkeypatch):
    spec, source, _, options = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: ["pid=123:fd"])
    with pytest.raises(SnapshotError, match="in use"):
        retirement.enroll_legacy_lease(spec, artifact_root=options["artifact_root"],
                                      state_root=options["state_root"])
    assert not (options["state_root"] / "retirements" / f"{spec.dataset}.json").exists()


def test_legacy_retirement_enrolls_then_removes_both_names_after_seven_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = time.time_ns()
    options["now_ns"] = start
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["blockers"] == ["seven-day-use-lease-not-enrolled"]
    enrolled = retirement.apply_legacy_retirement(
        spec, expected_fingerprint=plan["plan_fingerprint"], **options
    )
    assert enrolled["action"] == "enrolled"
    assert source.is_dir() and hot_tree.is_dir()
    renewed = retirement.renew_legacy_lease(
        spec,
        artifact_root=options["artifact_root"],
        state_root=options["state_root"],
        now_ns=start + 2 * 86_400_000_000_000,
    )
    assert renewed["lease_expires_at"]
    options["now_ns"] = start + 8 * 86_400_000_000_000
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "seven-day-use-lease-active" in plan["blockers"]
    options["now_ns"] = start + 10 * 86_400_000_000_000
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["apply_ready"]
    retired = retirement.apply_legacy_retirement(
        spec, expected_fingerprint=plan["plan_fingerprint"], **options
    )
    assert retired["action"] == "retired"
    assert not source.exists() and not hot_tree.exists()
    assert (options["sync_root"] / "heads" / spec.dataset / "penguin.json").is_file()
    assert (options["hot_root"] / ".stignore-cold-local").is_file()
    state = json.loads((options["state_root"] / "retirements" / f"{spec.dataset}.json").read_text())
    assert state["state"] == "cold-only"


def test_legacy_retirement_accepts_verified_single_d_primary_without_backup_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    (options["sync_root"] / cold_primary.D_PRIMARY_MARKER).write_text(json.dumps({
        "schema_version": 1,
        "volume_id": cold_primary.D_PRIMARY_VOLUME_ID,
        "backing": cold_primary.D_PRIMARY_BACKING,
        "authority_node_id": "penguin",
        "resilience": "single_d_volume",
    }))
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda root: None)
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["cold_primary_verified"] is True
    assert plan["backup_verified"] is False
    assert plan["resilience"] == "single_d_volume"
    assert source.exists() and hot_tree.exists()


def test_legacy_retirement_blocks_enabled_service_and_changed_mirror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    service = options["repo_root"] / "services/discord_bot/markets/crypto.yaml"
    service.parent.mkdir(parents=True)
    service.write_text("enabled: true\noutput_dir: artifacts/markets/crypto\n")
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "enabled-service-references-artifact" in plan["blockers"]
    service.write_text("enabled: false\noutput_dir: artifacts/markets/crypto\n")
    runtime_state = options["repo_root"] / "artifacts/discord_bot/state.json"
    runtime_state.parent.mkdir(parents=True)
    runtime_state.write_text(json.dumps({"markets": {"crypto": {"enabled": True}}}))
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "enabled-service-references-artifact" in plan["blockers"]
    runtime_state.write_text(json.dumps({"markets": {"crypto": {"enabled": False}}}))
    (hot_tree / "checkpoint.pt").unlink()
    (hot_tree / "checkpoint.pt").write_bytes(b"unverified copy")
    with pytest.raises(SnapshotError, match="legacy source differs|hot mirror"):
        retirement.plan_legacy_retirement(spec, **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_legacy_retirement_refreshes_peer_proof_after_full_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, _source, _hot_tree, options = _fixture(tmp_path, monkeypatch)
    stale = {"ok": True, "checked_at": "2020-01-01T00:00:00+00:00"}
    options["peer_proof"] = stale
    refreshed = {"ok": True, "checked_at": datetime.now(timezone.utc).isoformat()}
    options["peer_probe"] = lambda: refreshed
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["blockers"] == ["seven-day-use-lease-not-enrolled"]


def test_legacy_retirement_keeps_pinned_release(tmp_path, monkeypatch):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    plan = retirement.plan_legacy_retirement(spec, **options)
    root = options["materialized_root"]
    root.mkdir()
    (root / "legacy.pin.json").write_text(json.dumps({
        "manifest": {"snapshot_id": plan["snapshot_id"]}
    }))
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "artifact-release-is-pinned" in plan["blockers"]
    assert source.is_dir() and hot_tree.is_dir()


def test_legacy_retirement_rejects_naive_peer_timestamp(tmp_path, monkeypatch):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["peer_proof"]["checked_at"] = datetime.now().isoformat()
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "peer-proof-missing-timestamp" in plan["blockers"]
    assert not plan["apply_ready"]
    assert source.is_dir() and hot_tree.is_dir()


@pytest.mark.parametrize("enrolled", [False, True])
def test_legacy_manual_immediate_preserves_restore_and_automatic_policy(tmp_path, monkeypatch, enrolled):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = time.time_ns()
    options["now_ns"] = start
    if enrolled:
        retirement.enroll_legacy_lease(spec, artifact_root=options["artifact_root"], state_root=options["state_root"])
    ordinary = retirement.plan_legacy_retirement(spec, **options)
    assert not ordinary["apply_ready"]
    options["manual_immediate"] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["apply_ready"]
    with pytest.raises(SnapshotError, match="plan changed"):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=ordinary["plan_fingerprint"], **options)
    allocated = (source / "checkpoint.pt").stat().st_blocks * 512
    result = retirement.apply_legacy_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    assert result["deleted"] and result["reclaimed_allocated_file_bytes"] == allocated
    assert not source.exists() and not hot_tree.exists()
    state = json.loads(retirement._state_path(options["state_root"], spec.dataset).read_text())
    assert state["manual_immediate"] is True
    if not enrolled:
        assert state["last_used_ns"] == start
    restore = tmp_path / "restore"
    restore_archive(spec, options["sync_root"], restore, materialized_root=tmp_path / "restore-cache")
    assert (restore / "checkpoint.pt").read_bytes() == b"old unfinished run; preserve bytes, not deployment permission"


@pytest.mark.parametrize("gate", ["process", "service", "pin", "bridge", "state", "quarantine", "peer"])
def test_legacy_manual_immediate_never_bypasses_other_gates(tmp_path, monkeypatch, gate):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    if gate == "process":
        monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: ["pid=123:fd"])
    elif gate == "service":
        monkeypatch.setattr(retirement, "_active_service_references", lambda *args: ["service"])
    elif gate == "pin":
        first = retirement.plan_legacy_retirement(spec, **options)
        monkeypatch.setattr(retirement, "_pinned_snapshot_ids", lambda *args: {first["snapshot_id"]})
    elif gate == "bridge":
        options["bridge_inactive"] = False
    elif gate == "state":
        path = retirement._state_path(options["state_root"], spec.dataset)
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"schema_version": 1, "dataset": spec.dataset,
                                   "relative_root": spec.relative_root,
                                   "state": "cold-only", "last_used_ns": time.time_ns()}))
    elif gate == "quarantine":
        path = options["state_root"] / "retirements/quarantine" / spec.dataset
        path.mkdir(parents=True)
        (path / "unknown-evidence").write_bytes(b"keep")
    else:
        options["peer_proof"]["ok"] = False
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert not plan["apply_ready"]
    with pytest.raises(SnapshotError, match="blocked"):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_legacy_manual_immediate_missing_cold_keeps_hot_sources(tmp_path, monkeypatch):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    resolved = resolve_latest_packed(options["sync_root"], spec.dataset)
    (options["sync_root"] / resolved.manifest["archive"]["objects"][0]["relpath"]).unlink()
    with pytest.raises(SnapshotError):
        retirement.plan_legacy_retirement(spec, **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_sequential_legacy_retirement_handles_shared_inodes_without_fake_reclaimed_bytes(tmp_path, monkeypatch):
    first, source, hot, options = _fixture(tmp_path, monkeypatch)
    second = LegacyArchiveSpec("legacy-artifact-markets-us", "markets/us", 7, tmp_path / "stage")
    second_source = options["artifact_root"] / second.relative_root
    second_hot = options["hot_root"] / second.relative_root
    second_source.mkdir(parents=True)
    second_hot.mkdir(parents=True)
    os.link(source / "checkpoint.pt", second_source / "checkpoint.pt")
    os.link(source / "checkpoint.pt", second_hot / "checkpoint.pt")
    publish_archive(second, options["artifact_root"], options["sync_root"], repo_root=options["repo_root"])
    shutil.copytree(options["sync_root"], tmp_path / "backup", dirs_exist_ok=True)
    allocated = (source / "checkpoint.pt").stat().st_blocks * 512
    options["manual_immediate"] = True
    first_plan = retirement.plan_legacy_retirement(first, **options)
    first_result = retirement.apply_legacy_retirement(first, expected_fingerprint=first_plan["plan_fingerprint"], **options)
    assert first_result["deleted"] and first_result["reclaimed_allocated_file_bytes"] == 0
    assert second_source.is_dir() and second_hot.is_dir()
    second_plan = retirement.plan_legacy_retirement(second, **options)
    assert second_plan["source_metadata_drift"]
    second_result = retirement.apply_legacy_retirement(second, expected_fingerprint=second_plan["plan_fingerprint"], **options)
    assert second_result["deleted"] and second_result["reclaimed_allocated_file_bytes"] == allocated
    for spec in (first, second):
        restored = tmp_path / "restore" / spec.dataset
        restore_archive(spec, options["sync_root"], restored, materialized_root=tmp_path / "restore-cache")
        assert (restored / "checkpoint.pt").read_bytes() == b"old unfinished run; preserve bytes, not deployment permission"
