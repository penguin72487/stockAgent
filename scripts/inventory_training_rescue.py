#!/usr/bin/env python3
"""Freeze current private recovery inputs for the existing encrypted NAS relay.

Only small mutable controls are captured under the rescue owner. Immutable D
census and control preservation run after releasing it, so checkpoint capture
can continue. This is an inventory, not a full-byte restore or deletion proof.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_training_rescue import (  # noqa: E402
    RESTORED_PLAN_V2, _closed_bytes, _restored_input_specs,
)
from stockagent.training_rescue import Rescue, load_policy  # noqa: E402
from stockagent.training_rescue_bootstrap import bootstrap_objects, validate_bootstrap_binding  # noqa: E402
from stockagent.training_rescue_worker import (  # noqa: E402
    HASH, _strict_json, absolute, digest, identifier, input_control_bindings,
    MAX_PREPARED_REFERENCE_BYTES, preparation_resource_bindings, signature, validate_spec,
)

MAX_FILES = 10000
MAX_BYTES = 32 * 1024**3
MAX_MEMBER_BYTES = 8 * 1024**3 - 2 * 1024**2
MAX_CONTROL_BYTES = 64 * 1024**2
NAMESPACES = ('commits', 'bootstrap', 'evidence', 'source-verification', 'input-recipes')


def freeze_inventory(owner: Rescue, *, owner_wait_seconds: int = 0) -> dict:
    owner.guard()
    frozen, control_bytes = [], 0
    with owner.lock(wait_seconds=owner_wait_seconds):
        folders = sorted((owner.state / 'runs').iterdir())
        if not 0 < len(folders) <= 128:
            raise ValueError('private recovery inventory needs 1..128 enrolled runs')
        for folder in folders:
            absolute(str(folder))
            if not folder.is_dir():
                raise ValueError('private run namespace contains a non-directory')
            run = identifier(folder.name)
            values = {name: _closed_bytes(folder / name) for name in ('spec.json', 'state.json')}
            pointer = folder / 'input-recipe.json'
            if pointer.exists() or pointer.is_symlink():
                values['input-recipe.json'] = _closed_bytes(pointer)
            state = _strict_json(values['state.json'])
            target = state.get('active_target')
            if target:
                key = target.get('plan_id')
                if not isinstance(key, str) or not HASH.fullmatch(key):
                    raise ValueError('active target lacks its immutable plan identity')
                values['target-plan.json'] = _closed_bytes(owner.state / 'plans' / (key + '.json'))
            control_bytes += sum(map(len, values.values()))
            if control_bytes > MAX_CONTROL_BYTES:
                raise ValueError('frozen mutable recovery controls exceed their memory bound')
            frozen.append((run, values))

    # Resolve only frozen pointers. Never consult a moving journal again.
    required_objects, bindings = {}, []

    def control(name):
        path = owner.cold / name
        raw = _closed_bytes(path)
        value = _strict_json(raw)
        if digest(value) != path.stem:
            raise ValueError('immutable recovery control differs from its name')
        return value

    def require_object(row):
        key, size = row.get('sha256'), row.get('bytes')
        if (not isinstance(key, str) or not HASH.fullmatch(key)
                or type(size) is not int or not 0 <= size <= MAX_MEMBER_BYTES
                or key in required_objects and required_objects[key] != size):
            raise ValueError('invalid or conflicting bound private recovery object')
        required_objects[key] = size

    for run, raw_values in frozen:
        values = {name: _strict_json(raw) for name, raw in raw_values.items()}
        spec = validate_spec(values['spec.json'])
        state = values['state.json']
        if spec['run_id'] != run or state.get('run_id') != run:
            raise ValueError('frozen run controls differ from their owner directory')
        recovery, execution = _restored_input_specs(spec, state, values, control)
        latest = state.get('latest')
        if not isinstance(latest, dict) or not HASH.fullmatch(str(latest.get('commit_id', ''))):
            raise ValueError('run has no committed checkpoint cohort to back up')
        commit = control(f"commits/{run}/{latest['commit_id']}.json")
        if commit.get('run_id') != run or commit.get('spec_sha256') not in {digest(spec), digest(execution)}:
            raise ValueError('frozen checkpoint commit differs from its execution enrollment')
        inventory = commit['inventory']
        if (inventory['context_sha256'] != latest.get('context_sha256')
                or digest(inventory['files']) != inventory['context_sha256']
                or inventory['checkpoints'] != latest.get('checkpoints')):
            raise ValueError('frozen checkpoint context differs from its journal')
        for row in inventory['files']:
            require_object(row)
        bootstrap = state.get('bootstrap')
        if bootstrap:
            boot = control(f"bootstrap/{run}/{bootstrap['id']}.json")
            validate_bootstrap_binding(spec, boot, read_object=owner._bootstrap_object,
                                       source_identity=state.get('source_identity'))
            for row in bootstrap_objects(boot):
                require_object(row)
        proof = state.get('input_verification')
        source_matches = False
        if proof:
            source_proof = control(f"source-verification/{run}/{proof['id']}.json")
            if source_proof['spec_sha256'] not in {digest(s) for s in (spec, recovery, execution)}:
                raise ValueError('frozen source proof differs from the recorded input version')
            source_matches = source_proof['spec_sha256'] == digest(recovery)
        for inputs in {digest(s): s for s in (spec, recovery, execution)}.values():
            for row in [*input_control_bindings(inputs), *preparation_resource_bindings(inputs)]:
                if 'bytes' in row:
                    require_object(row)
                else:
                    # Legacy original-reference bindings pin SHA, not length.
                    # Observe the bounded CAS length; this is still metadata
                    # admission, never a substitute for publication full SHA.
                    path = absolute(str(owner.object_path(row['sha256'])))
                    before = signature(path)
                    if not stat.S_ISREG(before[-1]) or not 0 < before[2] <= MAX_PREPARED_REFERENCE_BYTES:
                        raise ValueError('pinned original reference exceeds its control bound')
                    require_object({**row, 'bytes': before[2]})
            for capsule in inputs.get('data_capsules', []):
                for kind in ('archive', 'proof'):
                    require_object({'sha256': capsule[kind + '_sha256'], 'bytes': capsule[kind + '_bytes']})
        controls = []
        for name, raw in sorted(raw_values.items()):
            row = {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
            owner.put_stream(io.BytesIO(raw), row)
            require_object(row)
            controls.append({'relative': f'run-controls/{run}/{name}', **row})
        bindings.append({'run_id': run, 'spec_sha256': digest(spec), 'latest': latest,
            'controls': controls, 'recovery_spec_sha256': digest(recovery),
            'execution_spec_sha256': digest(execution),
            'recorded_source_proof_matches_recovery_spec': source_matches,
            'full_source_rebuild_qualified': False, 'new_GPU_resume_verified': False})

    rows, excluded = [], []
    for folder in sorted((owner.cold / 'objects').iterdir()):
        absolute(str(folder))
        if not folder.is_dir() or re.fullmatch('[0-9a-f]{2}', folder.name) is None:
            raise ValueError('private CAS directory differs from its enrolled namespace')
        for path in sorted(folder.iterdir()):
            absolute(str(path))
            before = signature(path)
            if not stat.S_ISREG(before[-1]) or path.stat().st_nlink != 1 or path.name[:2] != folder.name:
                raise ValueError('private CAS member is redirected or not an original regular file')
            if re.fullmatch('[0-9a-f]{64}\\.partial\\.[0-9a-f]{32}', path.name):
                excluded.append({'relative': path.relative_to(owner.cold).as_posix(), 'bytes': before[2],
                    'signature': list(before), 'reason': 'uncommitted_original_transfer_partial_preserved'})
                continue
            if not HASH.fullmatch(path.name) or not 0 <= before[2] <= MAX_MEMBER_BYTES:
                raise ValueError('invalid or oversized committed private CAS member')
            rows.append({'relative': path.relative_to(owner.cold).as_posix(), 'bytes': before[2],
                'sha256': path.name, 'role': 'private_rescue_object', 'signature': list(before)})
            if len(rows) > MAX_FILES:
                raise ValueError('private recovery inventory exceeds its file bound')
    census = {row['sha256']: row['bytes'] for row in rows}
    if any(census.get(key) != size for key, size in required_objects.items()):
        raise ValueError('bound recovery resource is absent from the fixed private CAS census')
    for namespace in NAMESPACES:
        root = owner.cold / namespace
        if not root.exists():
            continue
        for directory, dirs, files in os.walk(root, followlinks=False):
            absolute(str(Path(directory)))
            for name in dirs:
                absolute(str(Path(directory) / name))
            for name in sorted(files):
                path = Path(directory) / name
                if path.suffix != '.json':
                    raise ValueError('unknown immutable recovery metadata member')
                before = signature(path)
                raw = _closed_bytes(path)
                _strict_json(raw)
                rows.append({'relative': path.relative_to(owner.cold).as_posix(), 'bytes': len(raw),
                    'sha256': hashlib.sha256(raw).hexdigest(), 'role': 'private_rescue_control',
                    'signature': list(before)})
                if len(rows) > MAX_FILES:
                    raise ValueError('private recovery inventory exceeds its file bound')
    rows.sort(key=lambda row: row['relative'])
    total = sum(row['bytes'] for row in rows)
    if total > MAX_BYTES or any(signature(owner.cold / row['relative']) != tuple(row['signature']) for row in rows):
        raise ValueError('private recovery inventory exceeds capacity or changed during census')
    plan = {'contract': RESTORED_PLAN_V2, 'source_root': str(owner.cold),
        'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'members': rows, 'runs': bindings,
        'files': len(rows), 'source_bytes': total, 'excluded_original_partials': excluded,
        'route': 'existing_encrypted_Restic_relay', 'delivery_kind': 'private_vast_recovery_backup',
        'encryption_required': True, 'plaintext_lake_export_allowed': False,
        'source_files_deleted': 0, 'full_payload_SHA_verified_by_census': False,
        'NAS_backup_verified': False, 'restore_verified': False,
        'complete_independent_source_restore_verified': False,
        'publication_requires_full_copy_SHA_and_independent_destination_SHA': True,
        'maximum_staging_bytes': 24 * 1024**3, 'maximum_pending_bytes_unchanged': MAX_BYTES,
        'reserve_bytes_unchanged': 64 * 1024**3,
        'claim_boundary': 'frozen controls and immutable metadata census; publication and restore require full SHA'}
    plan['identity_sha256'] = digest(plan)
    if len(json.dumps(plan).encode()) > 1024**2:
        raise ValueError('private recovery plan exceeds its relay metadata bound')
    return plan


def write_inventory(owner: Rescue, output: Path, *, owner_wait_seconds: int = 0) -> dict:
    """Publish only a new private evidence file, through either CLI entrypoint."""
    output = absolute(str(output.absolute()))
    if output.exists():
        raise ValueError('preserve the previously frozen private plan')
    if output.is_relative_to(owner.cold) or output.is_relative_to(owner.state):
        raise ValueError('inventory evidence must not alter the original owner namespace')
    plan = freeze_inventory(owner, owner_wait_seconds=owner_wait_seconds)
    raw = (json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()
    if len(raw) > 1024**2:
        raise ValueError('encoded private plan exceeds the relay bound')
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return {k: plan[k] for k in ('contract', 'identity_sha256', 'files', 'source_bytes',
        'full_payload_SHA_verified_by_census', 'NAS_backup_verified')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', type=Path, default=ROOT / 'configs/training_rescue.json')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--owner-wait-seconds', type=int, default=0)
    args = parser.parse_args()
    os.umask(0o077)
    if os.readlink('/proc/self/ns/mnt') != os.readlink('/proc/1/ns/mnt'):
        raise ValueError('use the original PID 1 authority mount namespace')
    result = write_inventory(Rescue(load_policy(args.policy)), args.output,
                             owner_wait_seconds=args.owner_wait_seconds)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
