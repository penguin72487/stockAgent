#!/usr/bin/env python3
"""Coordinate bounded Vast-owned head handoffs for already committed D repacks."""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import select
import shlex
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts import run_stream_packed_repack as stream_cli
from stockagent.data_sync import authoritative_head_handoff as authority, packed_snapshots as p, stream_repack, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.remote_ssh import ssh_base, validate_remote_absolute, validate_ssh_target

CONTRACT = 'stockagent.foreign_head_handoff_queue.v1'
HELPER = ROOT/'stockagent/data_sync/foreign_head_handoff.py'
MAX_PENDING_HANDOFFS = 8


def metadata(path: Path, *, maximum=32*1024**2) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise SnapshotError('foreign-head source metadata is unavailable')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise SnapshotError('foreign-head source metadata is invalid')
    return value


def plan(workspace: Path, cold: Path, row: dict) -> dict:
    if (not isinstance(row, dict) or row.get('mode') != 'direct'
            or row.get('private') is True or row.get('reason') != 'retirement_current_head'):
        raise SnapshotError('foreign-head source is not a waiting public retirement')
    name = p.validate_slug(row.get('dataset'), 'dataset')
    old_sha = p._validate_hash(row.get('old_manifest_sha256'), 'old manifest SHA')
    stage = workspace/'migration/stream-source-repack-v1'/name/old_sha
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('foreign-head source stage is redirected')
    prepared = stream_repack._prepared_metadata(stage, name, old_sha)
    if prepared.get('source_member_sha_streamed') is not True:
        raise SnapshotError('foreign-head source did not stream each original member')
    install = metadata(stage/'install.json')
    new_sha = p._validate_hash(install.get('new_manifest_sha256'), 'new manifest SHA')
    new_id = p.validate_slug(install.get('snapshot_id'), 'new snapshot')
    old_id = p.validate_slug(prepared.get('old_snapshot_id'), 'old snapshot')
    receipt = metadata(workspace/'migration/source-local-exact-repack'/name/(new_sha+'.json'))
    if (install.get('contract') != stream_repack.CONTRACT+'.install' or install.get('state') != 'head_pending'
            or install.get('dataset') != name or install.get('old_manifest_sha256') != old_sha
            or install.get('prepared_sha256') != prepared['identity_sha256']
            or receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
            or receipt.get('source_mode') != 'D_streamed_to_E_dataset_pack'
            or receipt.get('dataset') != name or receipt.get('private') is True
            or receipt.get('old_manifest_sha256') != old_sha or receipt.get('new_manifest_sha256') != new_sha
            or receipt.get('new_snapshot_id') != new_id or receipt.get('old_release_removed') is not False):
        raise SnapshotError('foreign-head source receipt does not match the fixed repack')
    retired_lab203 = (name == stream_cli._RETIRED_LAB203_DATASET
                      and old_sha == stream_cli._RETIRED_LAB203_MANIFEST)
    if retired_lab203 and not stream_cli._retired_lab203_successor_ready(
            cold, name, old_sha, 'penguin', prepared, with_successor=True):
        raise SnapshotError('retired lab203 packed topology is not ready for handoff')
    old_publisher = 'lab203' if retired_lab203 else 'vastai1T'
    old = p.resolve_packed_snapshot_id(cold, name, old_id, require_objects=False)
    latest = p.resolve_latest_packed(cold, name, require_objects=False)
    old_head = metadata(cold/'heads'/name/(old_publisher+'.json'), maximum=65536)
    new_head = metadata(cold/'heads'/name/'penguin.json', maximum=65536)
    if (old.manifest_sha256 != old_sha or latest.manifest_sha256 != new_sha
            or old_head.get('manifest_sha256') != old_sha or old_head.get('snapshot_id') != old_id
            or old_head.get('node_id') != old_publisher or new_head.get('manifest_sha256') != new_sha
            or new_head.get('snapshot_id') != new_id or new_head.get('node_id') != 'penguin'
            or latest.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256') != old_sha
            or old.manifest['source'] != latest.manifest['source']
            or latest.manifest.get('schema_version') not in (1, 2)):
        raise SnapshotError('foreign-head current D generations differ')
    request = {'dataset': name, 'old_manifest_sha256': old_sha, 'old_snapshot_id': old_id,
               'new_manifest_sha256': new_sha, 'new_snapshot_id': new_id}
    if retired_lab203:
        request['old_publisher'] = 'lab203'
    return request


def _peer_line(process, *, timeout: int) -> dict:
    ready, _, _ = select.select([process.stdout], [], [], timeout)
    if not ready:
        raise SnapshotError('foreign-head remote owner timed out')
    raw = process.stdout.readline(65537)
    if not raw or len(raw) > 65536:
        if not raw and process.poll() is None:
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        if not raw and process.poll() is not None and process.stderr is not None:
            detail = process.stderr.read(8192)
            for marker, reason in (
                ('synchronization is not current', 'foreign-head peer synchronization is not current'),
                ('edge owner is busy', 'foreign-head peer owner is busy'),
                ('old version is pinned', 'foreign-head peer version is pinned'),
                ('active hydration or retention', 'foreign-head peer dataset is active'),
                ('used by a running process', 'foreign-head peer version is used by a process'),
                ('lease or materialization', 'foreign-head peer version has a lease or materialization')):
                if marker in detail:
                    raise SnapshotError(reason)
        raise SnapshotError('foreign-head remote owner returned no bounded status')
    try:
        result = json.loads(raw)
    except (ValueError, TypeError):
        raise SnapshotError('foreign-head remote owner returned invalid status') from None
    if not isinstance(result, dict):
        raise SnapshotError('foreign-head remote owner returned invalid status')
    return result


class RemoteLease:
    def __init__(self, process, request):
        self.process, self.request, self.sent = process, request, False
        self.ready = None

    def await_scan(self):
        self.process.stdin.write('await-source-scan\n')
        self.process.stdin.flush()
        self.sent = True

    def confirm(self):
        return _peer_line(self.process, timeout=70)


@contextmanager
def remote_transport(helper: bytes, request: dict, *, environment=None):
    env = os.environ if environment is None else environment
    target = validate_ssh_target(env['COLD_ARTIFACT_INGRESS_SSH_TARGET'])
    port = int(env['COLD_ARTIFACT_INGRESS_SSH_PORT'])
    # This mutates the remote owner's head; use the canonical ingress identity,
    # never the dedicated read-only fleet-observer identity.
    identity = Path(env['COLD_ARTIFACT_INGRESS_IDENTITY_FILE'])
    repo = validate_remote_absolute(env['COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT'], 'remote repository')
    source = base64.b64encode(helper).decode('ascii')
    code = 'import base64;exec(compile(base64.b64decode('+repr(source)+'),"foreign_head_handoff.py","exec"))'
    command = 'cd '+shlex.quote(repo)+' && source scripts/runtime_env.sh && run_fintech_python -c '+shlex.quote(code)+' --hold'
    process = subprocess.Popen([*ssh_base(identity, port), '-o', 'StrictHostKeyChecking=yes', target, command],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, bufsize=1)
    lease = RemoteLease(process, request)
    try:
        process.stdin.write(json.dumps(request)+'\n')
        process.stdin.flush()
        result = _peer_line(process, timeout=30)
        if (result.get('contract') != 'stockagent.foreign_head_handoff.v1' or result.get('request') != request
                or result.get('state') != 'ready' or result.get('source_payload_deleted') is not False
                or result.get('D_old_objects_retired') is not False):
            raise SnapshotError('foreign-head remote owner status differs')
        lease.ready = result
        yield lease
    finally:
        if process.poll() is None and not lease.sent:
            try:
                process.stdin.write('abort\n')
                process.stdin.flush()
            except (OSError, ValueError):
                pass
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for pipe in (process.stdin, process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()


def remote_confirm(helper: bytes, request: dict, head_sha: str, *, environment=None):
    env = os.environ if environment is None else environment
    target = validate_ssh_target(env['COLD_ARTIFACT_INGRESS_SSH_TARGET'])
    port = int(env['COLD_ARTIFACT_INGRESS_SSH_PORT'])
    identity = Path(env['COLD_ARTIFACT_INGRESS_IDENTITY_FILE'])
    repo = validate_remote_absolute(env['COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT'], 'remote repository')
    digest = p._validate_hash(head_sha, 'old head SHA')
    source = base64.b64encode(helper).decode('ascii')
    code = 'import base64;exec(compile(base64.b64decode('+repr(source)+'),"foreign_head_handoff.py","exec"))'
    command = ('cd '+shlex.quote(repo)+' && source scripts/runtime_env.sh && run_fintech_python -c '
               +shlex.quote(code)+' --confirm --head-sha '+shlex.quote(digest))
    result = subprocess.run([*ssh_base(identity, port), '-o', 'StrictHostKeyChecking=yes', target, command],
                            input=json.dumps(request), text=True, capture_output=True, timeout=30)
    if result.returncode:
        raise SnapshotError('foreign-head peer has not yet confirmed synchronized handoff')
    try:
        response = json.loads(result.stdout)
    except (TypeError, ValueError):
        raise SnapshotError('foreign-head peer confirmation is malformed') from None
    if not isinstance(response, dict):
        raise SnapshotError('foreign-head peer confirmation is malformed')
    return response


def notify(cold: Path, request: dict):
    from scripts.configure_artifact_ingress_syncthing import credentials, request as syncthing_request
    from stockagent.data_sync.syncthing_scan import queue_after_publish
    if not queue_after_publish(cold, request['dataset'], new_manifest_paths=(
            f"manifests/{request['dataset']}/{request['new_snapshot_id']}.json",)):
        raise SnapshotError('authoritative-head scan intent was not recorded')
    # A busy full-folder scan can keep this HTTP request open. Queue the exact
    # small head subtree but do not mistake a timeout for peer confirmation;
    # the durable original scan owner retries and the peer must still confirm.
    try:
        url, key = credentials()
        syncthing_request(url, key, '/rest/db/scan',
            {'folder': 'stockagent-packed', 'sub': f"heads/{request['dataset']}"}, method='POST', timeout=3)
    except (OSError, ValueError, TimeoutError, SnapshotError):
        pass


def recover_interrupted(workspace: Path, cold: Path, *, guard=lambda: None, confirm_peer=remote_confirm,
                        materialized=Path('/srv/stockagent-packed-materialized'), notify_peer=None):
    root = workspace/'migration/authoritative-head-handoffs'
    if not root.exists():
        return []
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('authoritative-head recovery root is redirected')
    journals = sorted(root.glob('*/*/journal.json'))
    if len(journals) > 4096:
        raise SnapshotError('authoritative-head recovery exceeds its bound')
    recovered = []
    helper = HELPER.read_bytes()
    notify_peer = notify_peer or (lambda request: notify(cold, request))
    for path in journals:
        value = metadata(path, maximum=65536)
        if value.get('state') in ('completed', 'rolled_back'):
            continue
        request = value.get('request')
        if not isinstance(request, dict):
            raise SnapshotError('authoritative-head recovery request is missing')
        name = p.validate_slug(request.get('dataset'), 'dataset')
        digest = p._validate_hash(request.get('old_manifest_sha256'), 'old manifest SHA')
        if path.parent != root/name/digest:
            raise SnapshotError('authoritative-head recovery location differs')
        guard()
        with p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True), \
                p._try_publication_lock(cold/'.local-state/locks'/f'publish-{name}.lock'):
            stage, head, _captured = authority._paths(workspace, cold, request)
            value = authority._record(stage, request)
            pin_path = materialized/'.stream-repack-pins'/name/stream_repack._repack_pin_name(digest, stage, scoped=True)
            if value['state'] != 'moved' or os.path.lexists(head) or not os.path.lexists(pin_path):
                result = authority.rollback(workspace, cold, materialized, request, notify=notify_peer)
                recovered.append({'dataset': name, 'old_manifest_sha256': digest, 'state': result['state']})
                continue
            try:
                ack = confirm_peer(helper, request, value['old_head_sha256'])
                result = authority.finalize(workspace, cold, materialized, request, ack)
                recovered.append({'dataset': name, 'old_manifest_sha256': digest, 'state': result['state']})
            except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError, SnapshotError):
                notify_peer(request)
                recovered.append({'dataset': name, 'old_manifest_sha256': digest, 'state': 'waiting_peer'})
    return recovered


def pending_handoff_count(workspace: Path) -> int:
    root = workspace/'migration/authoritative-head-handoffs'
    if not root.exists():
        return 0
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('authoritative-head pending root is redirected')
    journals = list(root.glob('*/*/journal.json'))
    if len(journals) > 4096:
        raise SnapshotError('authoritative-head pending count exceeds its bound')
    count = 0
    for path in journals:
        value = metadata(path, maximum=65536)
        request = value.get('request')
        if not isinstance(request, dict):
            raise SnapshotError('authoritative-head pending request is invalid')
        name = p.validate_slug(request.get('dataset'), 'dataset')
        digest = p._validate_hash(request.get('old_manifest_sha256'), 'old manifest SHA')
        if path.parent != root/name/digest:
            raise SnapshotError('authoritative-head pending location differs')
        checked = authority._record(path.parent, request)
        count += int(checked['state'] not in ('completed', 'rolled_back'))
    return count


def run(workspace: Path, cold: Path, *, apply: bool, maximum=4, transport=remote_transport,
        guard=lambda: None) -> dict:
    root = workspace/'migration/source-local-exact-repack'
    status = metadata(root/'retirement-queue-status.json', maximum=4*1024**2)
    if status.get('contract') != 'stockagent.source_local_repack_queue.v1.retirement-status':
        raise SnapshotError('foreign-head upstream retirement status is invalid')
    entries = status.get('entries')
    if not isinstance(entries, list) or len(entries) > 4096 or not 1 <= maximum <= 16:
        raise SnapshotError('foreign-head queue size is invalid')
    rows = sorted((r for r in entries if isinstance(r, dict) and r.get('mode') == 'direct'
                   and r.get('private') is not True and r.get('reason') == 'retirement_current_head'),
                  key=lambda r: (str(r.get('dataset')), str(r.get('old_manifest_sha256'))))
    path = root/'foreign-head-handoff-status.json'
    try:
        previous = metadata(path, maximum=128*1024)
    except (OSError, ValueError, SnapshotError):
        previous = {}
    cursor = previous.get('cursor') if previous.get('contract') == CONTRACT else None
    if isinstance(cursor, str):
        index = next((i for i, row in enumerate(rows) if str(row.get('dataset')) > cursor), 0)
        rows = rows[index:]+rows[:index]
    helper = HELPER.read_bytes()
    if not helper or len(helper) > 128*1024:
        raise SnapshotError('foreign-head helper is missing or oversized')
    result = {'contract': CONTRACT, 'observed_at_epoch': time.time(), 'candidate_count': len(rows),
              'helper_sha256': hashlib.sha256(helper).hexdigest(), 'apply': apply,
              'state': 'idle', 'attempts': [], 'cursor': cursor,
              'source_payload_deleted': False, 'D_old_objects_retired': False}
    if apply:
        result['recovered'] = recover_interrupted(workspace, cold, guard=guard)
    pending = pending_handoff_count(workspace) if apply else 0
    result['pending_handoffs'] = pending
    available = max(0, MAX_PENDING_HANDOFFS-pending) if apply else maximum
    for row in rows[:min(maximum, available)]:
        guard()
        name = str(row.get('dataset', ''))
        result['cursor'] = name
        global_owner_busy = False
        try:
            if not p.validate_slug(name, 'dataset'):
                raise SnapshotError('foreign-head dataset invalid')
            with p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True), \
                    p._try_publication_lock(cold/'.local-state/locks'/f'publish-{name}.lock'):
                guard()
                request = plan(workspace, cold, row)
                with transport(helper, request) as peer:
                    if apply:
                        outcome = authority.commit(workspace, cold, Path('/srv/stockagent-packed-materialized'),
                            request, peer_head_sha=peer.ready['head_sha256'], notify=lambda item: notify(cold, item))
                    else:
                        outcome = {'state': 'ready'}
            result['attempts'].append({'dataset': name, 'state': outcome['state'],
                                       'old_manifest_sha256': request['old_manifest_sha256']})
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, SnapshotError) as error:
            result['attempts'].append({'dataset': name, 'state': 'waiting_retry',
                                       'error_type': type(error).__name__, 'error': str(error)[:256]})
            global_owner_busy = (isinstance(error, p.PackedPublicationBusy)
                                 and str(error) == 'publication owner busy: publish-retention-global.lock')
        result['state'] = 'waiting_owner' if global_owner_busy else 'active'
        result['observed_at_epoch'] = time.time()
        if apply:
            result['pending_handoffs'] = pending_handoff_count(workspace)
            atomic_write_json(path, result, durable=True)
        if global_owner_busy:
            break  # All remaining candidates require this same global owner.
    if apply and not result['attempts']:
        if pending >= MAX_PENDING_HANDOFFS:
            result['state'] = 'waiting_peer'
        atomic_write_json(path, result, durable=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--recover-only', action='store_true')
    parser.add_argument('--maximum', type=int, default=4)
    args = parser.parse_args()
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = Path(config['cold_root'])
    _check_d_primary_mount(cold)
    guard = TradingPriorityGuard()
    if args.recover_only:
        print(json.dumps({'contract': CONTRACT, 'state': 'recovery',
                          'recovered': recover_interrupted(Path(config['mount_point']), cold, guard=guard.check)},
                         sort_keys=True))
        return 0
    scheduled_apply = os.environ.get('STOCKAGENT_FOREIGN_HEAD_APPLY') == '1'
    print(json.dumps(run(Path(config['mount_point']), cold, apply=args.apply or scheduled_apply,
                         maximum=args.maximum, guard=guard.check), sort_keys=True))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
