#!/usr/bin/env python3
"""Activate sealed E dedup candidates after the pinned Vast reader is observable.

The marker enables the existing D installer; it does not install, retire, read
back or back up payloads. Every candidate retains the original writer locks.
"""
from __future__ import annotations

import argparse
import base64
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
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts.run_stream_packed_repack import _source_head_nodes
from stockagent.data_sync import (dedup_stream_candidate as candidate, packed_snapshots as p,
                                  source_repack_bloat, stream_repack, workspace_volume)
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.remote_ssh import ssh_base, validate_remote_absolute, validate_ssh_target

CONTRACT = 'stockagent.v3_reader_activation_queue.v1'
POLICY_CONTRACT = 'stockagent.edge_reader_activation_policy.v1'
POLICY = ROOT/'configs/data_sync/edge_reader_activation.json'
PROBE = ROOT/'stockagent/data_sync/edge_reader_probe.py'


def load_policy(path: Path = POLICY) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 16384:
        raise SnapshotError('edge reader activation policy is unavailable')
    value = json.loads(path.read_bytes())
    if (not isinstance(value, dict) or set(value) != {'contract', 'enabled', 'remote_reader_manifest_sha256',
                                                       'remote_wrapper_sha256', 'probe_dataset'}
            or value.get('contract') != POLICY_CONTRACT or type(value.get('enabled')) is not bool
            or not all(isinstance(value.get(k), str) and re.fullmatch(r'[0-9a-f]{64}', value[k])
                       for k in ('remote_reader_manifest_sha256', 'remote_wrapper_sha256'))):
        raise SnapshotError('edge reader activation policy differs')
    p.validate_slug(value.get('probe_dataset'), 'probe dataset')
    return value


def remote_probe(policy: dict, *, environment=None) -> dict:
    env = os.environ if environment is None else environment
    repo = validate_remote_absolute(env['COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT'], 'remote repository')
    target = validate_ssh_target(env['COLD_ARTIFACT_INGRESS_SSH_TARGET'])
    identity = Path(env['COLD_ARTIFACT_INGRESS_IDENTITY_FILE'])
    port = int(env['COLD_ARTIFACT_INGRESS_SSH_PORT'])
    helper = PROBE.read_bytes()
    if not helper or len(helper) > 64*1024:
        raise SnapshotError('edge reader probe code is unavailable')
    code = 'import base64;exec(compile(base64.b64decode('+repr(base64.b64encode(helper).decode())+'),"edge_reader_probe.py","exec"))'
    request = {'repo': repo, 'manifest_sha256': policy['remote_reader_manifest_sha256'],
               'wrapper_sha256': policy['remote_wrapper_sha256'], 'dataset': policy['probe_dataset']}
    result = subprocess.run([*ssh_base(identity, port), '-o', 'StrictHostKeyChecking=yes', target,
                             'python3 -c '+shlex.quote(code)], input=json.dumps(request), text=True,
                            capture_output=True, timeout=35, check=True)
    value = json.loads(result.stdout)
    if (not isinstance(value, dict) or value.get('contract') != 'stockagent.edge_reader_probe.v1'
            or value.get('state') != 'ready' or value.get('reader_manifest_sha256') != request['manifest_sha256']
            or value.get('wrapper_sha256') != request['wrapper_sha256'] or value.get('probe_dataset') != request['dataset']
            or value.get('schema_version') != 3 or value.get('payload_fetched') is not False
            or value.get('payload_verified') is not False):
        raise SnapshotError('edge reader probe result differs')
    return value


def _completed_candidate(workspace: Path, stage: Path, name: str, digest: str) -> bool:
    """Exclude an already published, E-cleaned generation from activation.

    The retained prepared metadata no longer has its E payloads after cleanup;
    consulting ``_read_prepared`` there would incorrectly call it pending. This
    is only queue classification and does not authorize deletion or readback.
    """
    try:
        prepared = stream_repack._prepared_metadata(stage, name, digest)
        if prepared.get('packed_format') != 'content-alias-v3':
            return False
        stream_repack.require_content_alias_install(stage, name, digest, prepared)
        records = []
        for path in (stage/'cleanup.json', stage/'install.json'):
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
                return False
            records.append(json.loads(path.read_bytes()))
        cleanup, install = records
        if (not isinstance(cleanup, dict) or not isinstance(install, dict)
                or cleanup.get('contract') != stream_repack.CONTRACT+'.cleanup'
                or install.get('contract') != stream_repack.CONTRACT+'.install'
                or cleanup.get('state') != 'completed' or install.get('state') != 'head_pending'):
            return False
        new_sha = p._validate_hash(install.get('new_manifest_sha256'), 'new manifest SHA')
        snapshot = p.validate_slug(install.get('snapshot_id'), 'snapshot id')
        for value in (cleanup, install):
            if (value.get('dataset') != name or value.get('old_manifest_sha256') != digest
                    or value.get('new_manifest_sha256') != new_sha
                    or value.get('prepared_sha256') != prepared['identity_sha256']):
                return False
        path = workspace/'migration/source-local-exact-repack'/name/(new_sha+'.json')
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
            return False
        receipt = json.loads(path.read_bytes())
        return (isinstance(receipt, dict)
                and receipt.get('contract') == 'stockagent.source_local_exact_repack.v1'
                and receipt.get('state') == 'new_D_head_committed_from_exact_E_source'
                and receipt.get('dataset') == name and receipt.get('old_manifest_sha256') == digest
                and receipt.get('new_manifest_sha256') == new_sha and receipt.get('new_snapshot_id') == snapshot
                and receipt.get('packed_format') == 'content-alias-v3'
                and isinstance(receipt.get('E_stage_cleanup'), dict)
                and receipt['E_stage_cleanup'].get('state') == 'completed')
    except (OSError, ValueError, KeyError, TypeError, SnapshotError):
        return False


def pending_candidates(workspace: Path) -> list[tuple[str, str]]:
    rows = []
    for name, digest in sorted(source_repack_bloat.auto_holds(workspace)):
        stage = workspace/'migration/stream-source-repack-v1'/name/digest
        status = workspace/'migration/source-local-dedup-candidate-v1'/(name+'.status.json')
        if (not (stage/'prepared.json').is_file() or status.is_symlink() or not status.is_file()
                or status.stat().st_size > 1024*1024):
            continue
        try:
            value = json.loads(status.read_bytes())
            if (isinstance(value, dict) and value.get('contract') == 'stockagent.dedup_candidate_queue.v1'
                    and value.get('dataset') == name and value.get('expected_manifest_sha256') == digest
                    and value.get('state') == 'v3_handed_off_waiting_activation'
                    and value.get('publishable') is False and value.get('retirement_authorized') is False
                    and not _completed_candidate(workspace, stage, name, digest)):
                rows.append((name, digest))
        except (OSError, ValueError, TypeError):
            continue
    if len(rows) > 4096:
        raise SnapshotError('edge reader activation queue exceeds its bound')
    return rows


def run(workspace: Path, cold: Path, policy: dict, *, apply=False, remote=remote_probe,
        guard=lambda: None, maximum=8, now=time.time) -> dict:
    if type(apply) is not bool or not 1 <= maximum <= 32:
        raise SnapshotError('edge reader activation request is invalid')
    workspace, cold = Path(workspace).absolute(), Path(cold).absolute()
    rows = pending_candidates(workspace) if policy.get('enabled') is True else []
    status_root = workspace/'migration/source-local-exact-repack'
    if status_root.is_symlink() or not status_root.is_dir() or status_root.resolve(strict=True) != status_root:
        raise SnapshotError('edge reader activation status root is unavailable or redirected')
    status_path = status_root/'v3-reader-activation-status.json'
    previous = {}
    if apply and status_path.is_file() and not status_path.is_symlink() and status_path.stat().st_size <= 1024*1024:
        try:
            value = json.loads(status_path.read_bytes())
            if isinstance(value, dict) and value.get('contract') == CONTRACT and value.get('apply') is True:
                previous = value
        except (OSError, ValueError, TypeError):
            pass
    result = {'contract': CONTRACT, 'state': 'idle', 'apply': apply, 'observed_at_epoch': now(),
              'candidate_count': len(rows), 'eligible_count': 0, 'activated_count': 0, 'blocked_count': 0,
              'remote_reader_observed': False, 'source_payload_deleted': False, 'D_head_changed': False,
              'independent_D_readback': False, 'NAS_accepted': False, 'entries': []}
    if (type(previous.get('last_activated_count')) is int and 0 < previous['last_activated_count'] <= 32
            and type(previous.get('last_activated_at_epoch')) in (int, float)
            and 0 <= previous['last_activated_at_epoch'] <= now()):
        result.update(last_activated_count=previous['last_activated_count'],
                      last_activated_at_epoch=previous['last_activated_at_epoch'])
    def save():
        result['observed_at_epoch'] = now()
        if apply:
            atomic_write_json(status_path, result, durable=True)
    if not rows:
        save()
        return result
    guard()
    try:
        proof = remote(policy)
        if (proof.get('state') != 'ready' or proof.get('reader_manifest_sha256') != policy['remote_reader_manifest_sha256']
                or proof.get('wrapper_sha256') != policy['remote_wrapper_sha256']
                or proof.get('schema_version') != 3 or proof.get('payload_fetched') is not False
                or proof.get('payload_verified') is not False):
            raise SnapshotError('edge reader capability is not the enrolled version')
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
        result.update(state='waiting_reader', error_type=type(error).__name__)
        save()
        return result
    result.update(state='reader_ready', remote_reader_observed=True,
                  reader_manifest_sha256=policy['remote_reader_manifest_sha256'],
                  probe_dataset_manifest_sha256=proof.get('probe_dataset_manifest_sha256'))
    for name, digest in rows[:maximum]:
        guard()
        item = {'dataset': name, 'old_manifest_sha256': digest, 'state': 'waiting'}
        stage = workspace/'migration/stream-source-repack-v1'/name/digest
        try:
            # This writes only an E-side eligibility marker. The dataset
            # owner prevents that fixed source or stage from advancing; the
            # eventual installer still acquires both of its normal D owners.
            with p._try_publication_lock(cold/'.local-state/locks'/f'publish-{name}.lock'):
                guard()
                old = p.resolve_latest_packed(cold, name, require_objects=False)
                prepared = stream_repack._read_prepared(stage, name, digest)
                sealed = candidate.read_sealed(stage, name, digest)
                if (old.manifest_sha256 != digest or _source_head_nodes(cold, name) != {'penguin': digest}
                        or prepared.get('packed_format') != 'content-alias-v3'
                        or prepared.get('candidate_identity_sha256') != sealed.get('identity_sha256')
                        or prepared.get('old_inventory_sha256') != old.manifest['archive']['inventory']['sha256']
                        or prepared.get('files') != old.manifest['source']['files']
                        or prepared.get('logical_bytes') != old.manifest['source']['logical_bytes']):
                    raise SnapshotError('edge reader activation fixed source differs')
                if apply:
                    stream_repack.activate_content_alias_install(stage, name, digest,
                                                                 reader_contract='packed-v3-content-alias')
                item['state'] = 'activated' if apply else 'eligible'
                result['eligible_count'] += 1
                result['activated_count'] += int(apply)
                if apply:
                    result.update(last_activated_count=result['activated_count'], last_activated_at_epoch=now())
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            item.update(state='waiting_retry', error_type=type(error).__name__)
            result['blocked_count'] += 1
        result['entries'].append(item)
        save()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = Path(config['cold_root'])
    _check_d_primary_mount(cold)
    guard = TradingPriorityGuard()
    result = run(Path(config['mount_point']), cold, load_policy(),
                 apply=args.apply or os.environ.get('STOCKAGENT_V3_READER_ACTIVATION_APPLY') == '1',
                 guard=guard.check)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
