#!/usr/bin/env python3
"""Observe and safely restore the enrolled isolated reader on Vast when absent.

This owner transfers only a fixed code bundle. It does not fetch market data,
change any training job, create the base edge owner, or replace another release.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_v3_reader_activation_queue as activation
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from stockagent.data_sync import edge_reader_recovery as recovery, packed_snapshots as p, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.remote_ssh import ssh_base, validate_remote_absolute, validate_ssh_target

CONTRACT = 'stockagent.edge_reader_recovery_queue.v1'
PREFLIGHT = ROOT/'stockagent/data_sync/edge_reader_recovery_probe.py'
BUNDLE_PREFLIGHT = ROOT/'stockagent/data_sync/edge_reader_bundle_preflight.py'
SCRATCH_FIXTURE = ROOT/'stockagent/data_sync/edge_reader_scratch_fixture.py'
TEMP_PATTERN = re.compile(r'^/tmp/stockagent-edge-recovery\.[A-Za-z0-9]{8}$')
TRADING_DEFERRED = 'Taiwan day-trade priority window; maintenance deferred'


def _remote(environment: dict) -> tuple[list[str], str, str]:
    target = validate_ssh_target(environment['COLD_ARTIFACT_INGRESS_SSH_TARGET'])
    port = int(environment['COLD_ARTIFACT_INGRESS_SSH_PORT'])
    identity = Path(environment['COLD_ARTIFACT_INGRESS_IDENTITY_FILE'])
    repo = validate_remote_absolute(environment['COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT'], 'remote repository')
    return [*ssh_base(identity, port), '-o', 'StrictHostKeyChecking=yes'], target, repo


def _ssh(ssh: list[str], target: str, command: str, *, body=None, timeout=35) -> str:
    result = subprocess.run([*ssh, target, command], input=body, text=True,
                            capture_output=True, timeout=timeout, check=True)
    return result.stdout.strip()


def remote_preflight(value: dict, *, environment=None) -> dict:
    env = os.environ if environment is None else environment
    ssh, target, repo = _remote(env)
    helper = PREFLIGHT.read_bytes()
    if not helper or len(helper) > 64*1024:
        raise SnapshotError('edge reader recovery preflight is unavailable')
    code = 'import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(helper).decode())+'),"edge_reader_recovery_probe.py","exec"))'
    request = {'repo': repo, 'baseline_wrapper_sha256': value['baseline_wrapper_sha256'],
               'wrapper_sha256': value['wrapper_sha256'], 'reader_manifest_sha256': value['reader_manifest_sha256']}
    result = json.loads(_ssh(ssh, target, 'python3 -c '+shlex.quote(code), body=json.dumps(request)))
    if (not isinstance(result, dict) or result.get('contract') != 'stockagent.edge_reader_recovery_probe.v1'
            or result.get('state') not in {'eligible', 'base_not_enrolled', 'control_mismatch', 'wrapper_mismatch',
                                           'reader_intact', 'release_mismatch'}):
        raise SnapshotError('edge reader recovery preflight response differs')
    return result


def _expected_dataset(cold: Path, dataset: str) -> str:
    _check_d_primary_mount(cold)
    with p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True):
        value = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if value.manifest.get('schema_version') != 3:
            raise SnapshotError('reader recovery probe dataset is not v3')
        return value.manifest_sha256


@contextmanager
def staged_bundle(bundle: Path, value: dict, *, environment=None):
    """Transport and verify fixed code without touching the remote owner."""
    env = os.environ if environment is None else environment
    recovery.verify(bundle, value)
    ssh, target, repo = _remote(env)
    remote_tmp = _ssh(ssh, target, 'mktemp -d /tmp/stockagent-edge-recovery.XXXXXXXX')
    if not TEMP_PATTERN.fullmatch(remote_tmp):
        raise SnapshotError('remote reader staging directory identity differs')
    try:
        rsh = shlex.join(ssh)
        subprocess.run(['rsync', '-r', '--chmod=Du=rwx,Dgo=,Fu=rw,Fgo=', '-e', rsh, '--',
                        str(bundle)+'/', target+':'+remote_tmp+'/'],
                       capture_output=True, text=True, timeout=90, check=True)
        # Verify the entire transferred source and dependency closure before
        # running *any* installer or smoke code from the temporary directory.
        helper = BUNDLE_PREFLIGHT.read_bytes()
        if not helper or len(helper) > 64*1024:
            raise SnapshotError('remote reader bundle preflight is unavailable')
        code = 'import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(helper).decode())+'),"edge_reader_bundle_preflight.py","exec"))'
        request = {'root': remote_tmp, 'source_identity_sha256': value['source_identity_sha256'],
                   'reader_manifest_sha256': value['reader_manifest_sha256'],
                   'wrapper_sha256': value['wrapper_sha256'], 'installer_sha256': value['installer_sha256']}
        verified = json.loads(_ssh(ssh, target, 'python3 -c '+shlex.quote(code),
                                   body=json.dumps(request), timeout=35))
        if (not isinstance(verified, dict) or verified.get('contract') != 'stockagent.edge_reader_bundle_preflight.v1'
                or verified.get('state') != 'verified'
                or verified.get('reader_manifest_sha256') != value['reader_manifest_sha256']):
            raise SnapshotError('remote reader bundle preflight response differs')
        yield ssh, target, repo, remote_tmp, verified
    finally:
        # Only the unique temporary directory created above is removed; the
        # installed immutable release, owner state and all payloads remain.
        try:
            command = 'rm -rf -- '+shlex.quote(remote_tmp)
            _ssh(ssh, target, command, timeout=15)
        except (OSError, ValueError, subprocess.SubprocessError):
            pass


def _stage_remote(ssh, target, repo, temporary, value) -> str:
    command = ['python3', temporary+'/installer.py', 'stage', '--source', temporary+'/source', '--repo', repo,
               '--expected-source', value['source_identity_sha256']]
    staged = json.loads(_ssh(ssh, target, shlex.join(command), timeout=90))
    release = repo+'/artifacts/code_releases/stockagent-edge-reader-v3-'+value['reader_manifest_sha256'][:16]
    if (not isinstance(staged, dict) or staged.get('contract') != 'stockagent.isolated_edge_reader_release.v1'
            or staged.get('state') != 'staged' or staged.get('release_root') != release
            or staged.get('source_identity_sha256') != value['source_identity_sha256']
            or staged.get('manifest_sha256') != value['reader_manifest_sha256']
            or staged.get('pointer_changed') is not False):
        raise SnapshotError('remote staged reader identity differs')
    return release


def _activate_remote(ssh, target, repo, state_root, temporary, value, baseline, expected_dataset) -> dict:
    release = repo+'/artifacts/code_releases/stockagent-edge-reader-v3-'+value['reader_manifest_sha256'][:16]
    command = ['python3', temporary+'/installer.py', 'activate', '--repo', repo,
               '--state', state_root, '--release', release, '--wrapper-source', temporary+'/wrapper.sh',
               '--expected-manifest', value['reader_manifest_sha256'], '--expected-wrapper', value['wrapper_sha256'],
               '--expected-baseline', baseline, '--dataset', value['dataset'], '--expected-dataset-sha', expected_dataset]
    result = json.loads(_ssh(ssh, target, shlex.join(command), timeout=90))
    if (not isinstance(result, dict) or result.get('contract') != 'stockagent.isolated_edge_reader_release.v1'
            or result.get('state') != 'active' or result.get('manifest_sha256') != value['reader_manifest_sha256']
            or result.get('wrapper_sha256') != value['wrapper_sha256'] or result.get('payload_fetched') is not False):
        raise SnapshotError('remote reader activation response differs')
    return result


def remote_deploy(bundle: Path, cold: Path, value: dict, *, environment=None,
                  mutation_guard=lambda: None) -> dict:
    """Stage fixed code and select it only under the original edge owner."""
    expected_dataset = _expected_dataset(cold, value['dataset'])
    mutation_guard()
    with staged_bundle(bundle, value, environment=environment) as (ssh, target, repo, remote_tmp, _verified):
        mutation_guard()
        _stage_remote(ssh, target, repo, remote_tmp, value)
        mutation_guard()
        _activate_remote(ssh, target, repo, '/var/lib/stockagent-packed-edge', remote_tmp, value,
                         value['baseline_wrapper_sha256'], expected_dataset)
    return {'state': 'installed', 'dataset_manifest_sha256': expected_dataset, 'payload_fetched': False}


def exercise_scratch(bundle: Path, cold: Path, value: dict, *, environment=None) -> dict:
    """Exercise the real remote installer against a disposable edge owner."""
    expected_dataset = _expected_dataset(cold, value['dataset'])
    with staged_bundle(bundle, value, environment=environment) as (ssh, target, repo, temporary, verified):
        helper = SCRATCH_FIXTURE.read_bytes()
        if not helper or len(helper) > 64*1024:
            raise SnapshotError('remote reader scratch fixture is unavailable')
        code = 'import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(helper).decode())+'),"edge_reader_scratch_fixture.py","exec"))'
        scratch = json.loads(_ssh(ssh, target, 'python3 -c '+shlex.quote(code),
                                  body=json.dumps({'temporary': temporary, 'canonical_repo': repo})))
        scratch_repo, scratch_state = temporary+'/scratch-repo', temporary+'/scratch-state'
        if (not isinstance(scratch, dict) or scratch.get('contract') != 'stockagent.edge_reader_scratch_fixture.v1'
                or scratch.get('repo') != scratch_repo or scratch.get('state_root') != scratch_state
                or scratch.get('canonical_owner_changed') is not False
                or not isinstance(scratch.get('baseline_sha256'), str)
                or not re.fullmatch(r'[a-f0-9]{64}', scratch['baseline_sha256'])):
            raise SnapshotError('remote reader scratch fixture differs')
        _stage_remote(ssh, target, scratch_repo, temporary, value)
        result = _activate_remote(ssh, target, scratch_repo, scratch_state, temporary, value,
                                  scratch['baseline_sha256'], expected_dataset)
        status = result.get('reader_status')
        if (not isinstance(status, dict) or status.get('manifest_sha256') != expected_dataset
                or status.get('payload_verified') is not False):
            raise SnapshotError('remote scratch reader metadata differs')
        return {'state': 'scratch_recovery_passed', 'reader_file_count': verified['reader_file_count'],
                'dataset_manifest_sha256': expected_dataset, 'canonical_owner_changed': False,
                'payload_fetched': False}


def run(workspace: Path, cold: Path, value: dict, activation_policy: dict, *, apply=False,
        probe=activation.remote_probe, preflight=remote_preflight, deploy=remote_deploy,
        rebuild=None, ensure_workspace=lambda: None, mutation_guard=lambda: None, now=time.time) -> dict:
    if type(apply) is not bool:
        raise SnapshotError('edge reader recovery apply flag must be boolean')
    workspace, cold = Path(workspace).absolute(), Path(cold).absolute()
    root = workspace/'migration/edge-reader-recovery-v1'
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('edge reader recovery workspace is unavailable')
    bundle = root/value['reader_manifest_sha256']
    result = {'contract': CONTRACT, 'observed_at_epoch': now(), 'state': 'waiting', 'apply': apply,
              'reader_manifest_sha256': value['reader_manifest_sha256'], 'remote_payload_fetched': False,
              'market_payload_deleted': False, 'NAS_accepted': False}
    previous = root/'status.json'
    try:
        if (apply and not previous.is_symlink() and previous.is_file() and previous.stat().st_size <= 16384):
            old = json.loads(previous.read_bytes())
            recovered = old.get('last_recovered_at_epoch') if isinstance(old, dict) else None
            if (isinstance(old, dict) and old.get('contract') == CONTRACT and old.get('apply') is True
                    and old.get('reader_manifest_sha256') == value['reader_manifest_sha256']
                    and type(recovered) in (int, float) and 0 <= recovered <= now()):
                result['last_recovered_at_epoch'] = recovered
    except (OSError, ValueError, TypeError):
        pass  # An old observation is not needed to observe the current owner.

    def save():
        result['observed_at_epoch'] = now()
        if apply:
            ensure_workspace()
            atomic_write_json(root/'status.json', result, durable=True)
        return result

    def mutation_allowed():
        try:
            mutation_guard()
        except SnapshotError as error:
            if str(error) != TRADING_DEFERRED:
                raise
            result['state'] = 'waiting_trading'
            return False
        return True

    if value['enabled'] is not True or activation_policy.get('enabled') is not True:
        result['state'] = 'disabled'
        return save()
    try:
        if apply and not os.path.lexists(bundle):
            # A lost E code bundle can be rebuilt only from the same pinned
            # source and dependency identities; changed inputs fail closed.
            if not mutation_allowed():
                return save()
            builder = rebuild or (lambda: recovery.build(ROOT, workspace, recovery.default_site(), value))
            builder()
        recovery.verify(bundle, value)
    except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
        result.update(state='bundle_invalid', error_type=type(error).__name__)
        return save()
    try:
        proof = probe(activation_policy)
        if (not isinstance(proof, dict) or proof.get('state') != 'ready'
                or proof.get('probe_dataset') != value['dataset']
                or not isinstance(proof.get('probe_dataset_manifest_sha256'), str)
                or not re.fullmatch(r'[a-f0-9]{64}', proof['probe_dataset_manifest_sha256'])
                or proof.get('reader_manifest_sha256') != value['reader_manifest_sha256']
                or proof.get('wrapper_sha256') != value['wrapper_sha256'] or proof.get('schema_version') != 3
                or proof.get('payload_fetched') is not False or proof.get('payload_verified') is not False):
            raise SnapshotError('remote reader observation differs')
        result.update(state='ready', probe_dataset_manifest_sha256=proof.get('probe_dataset_manifest_sha256'))
        return save()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
        result['initial_probe_error_type'] = type(error).__name__
    try:
        check = preflight(value)
        if not isinstance(check, dict) or check.get('state') not in {
                'eligible', 'base_not_enrolled', 'control_mismatch', 'wrapper_mismatch',
                'reader_intact', 'release_mismatch'}:
            raise SnapshotError('remote recovery preflight differs')
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
        result.update(state='remote_unavailable', error_type=type(error).__name__)
        return save()
    if check['state'] != 'eligible':
        result['state'] = check['state']
        return save()
    if not apply:
        result['state'] = 'eligible'
        return save()
    if not mutation_allowed():
        return save()
    try:
        completed = deploy(bundle, cold, value)
        if (not isinstance(completed, dict) or completed.get('state') != 'installed'
                or completed.get('payload_fetched') is not False):
            raise SnapshotError('remote recovery completion differs')
        proof = probe(activation_policy)
        if (not isinstance(proof, dict) or proof.get('state') != 'ready'
                or proof.get('probe_dataset') != value['dataset']
                or not isinstance(proof.get('probe_dataset_manifest_sha256'), str)
                or not re.fullmatch(r'[a-f0-9]{64}', proof['probe_dataset_manifest_sha256'])
                or proof.get('reader_manifest_sha256') != value['reader_manifest_sha256']
                or proof.get('wrapper_sha256') != value['wrapper_sha256'] or proof.get('schema_version') != 3
                or proof.get('payload_fetched') is not False or proof.get('payload_verified') is not False):
            raise SnapshotError('remote reader post-install observation differs')
        result.update(state='recovered', last_recovered_at_epoch=now(),
                      probe_dataset_manifest_sha256=proof.get('probe_dataset_manifest_sha256'))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
        result.update(state='waiting_trading' if isinstance(error, SnapshotError) and str(error) == TRADING_DEFERRED
                      else 'retrying', error_type=type(error).__name__)
    return save()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-transfer-only', action='store_true',
                        help='Verify a temporary code transfer without selecting a reader.')
    parser.add_argument('--exercise-scratch', action='store_true',
                        help='Exercise installation under a disposable remote owner.')
    args = parser.parse_args()
    if args.verify_transfer_only and args.exercise_scratch:
        parser.error('select one diagnostic')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    value = recovery.policy(ROOT/'configs/data_sync/edge_reader_recovery_v3.json',
                            ROOT/'configs/data_sync/edge_reader_activation.json')
    guard = TradingPriorityGuard()
    if args.verify_transfer_only:
        guard.check()
        bundle = Path(config['mount_point'])/'migration/edge-reader-recovery-v1'/value['reader_manifest_sha256']
        with staged_bundle(bundle, value) as (_ssh_command, _target, _repo, _temporary, verified):
            print(json.dumps({'state': 'transfer_verified', 'reader_file_count': verified['reader_file_count'],
                              'reader_manifest_sha256': verified['reader_manifest_sha256'],
                              'remote_owner_changed': False, 'payload_fetched': False}, sort_keys=True))
        return
    if args.exercise_scratch:
        guard.check()
        bundle = Path(config['mount_point'])/'migration/edge-reader-recovery-v1'/value['reader_manifest_sha256']
        print(json.dumps(exercise_scratch(bundle, Path(config['cold_root']), value), sort_keys=True))
        return
    root = Path(config['mount_point'])/'migration/edge-reader-recovery-v1'
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('edge reader recovery owner root is unavailable')
    with p._try_publication_lock(root/'owner.lock'):
        result = run(Path(config['mount_point']), Path(config['cold_root']), value,
                     activation.load_policy(), apply=True,
                     mutation_guard=guard.check,
                     deploy=lambda bundle, cold, policy: remote_deploy(
                         bundle, cold, policy, mutation_guard=guard.check),
                     ensure_workspace=lambda: workspace_volume.check(c=config))
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
        print(f'{type(error).__name__}: edge reader recovery failed', file=sys.stderr)
        raise SystemExit(1)
