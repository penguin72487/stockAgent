"""Production enrollment and bounded execution of canonical code verification.

PostgreSQL owns immutable work/attempt state. A private binding registry only
maps each exact work to its local frozen files; it is not a second scheduler.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import time
from typing import Any

from downloader.artifact_io import atomic_write_json
from stockagent.control.contracts import WorkSpec, identifier, integer
from stockagent.control.postgres import ControlStore
from stockagent.control.worker import execute_one, node_observation
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock, verify_source_release, verify_release_bundles

POLICY_KEYS = frozenset({'schema_version', 'node_id', 'state_root', 'runtime_lock',
                         'lease_seconds', 'maximum_jobs_per_cycle'})
BINDING_KEYS = frozenset({'schema_version', 'receipt', 'source_root', 'work_spec'})


def private_json(path: Path) -> dict[str, Any]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as handle:
        before = os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                or before.st_mode & 0o077 or before.st_size > 1024 * 1024 or before.st_nlink != 1):
            raise ValueError('control registry requires a private bounded regular file')
        body = handle.read()
        after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError('control registry changed during observation')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate control registry key')
            result[key] = value
        return result
    value = json.loads(body, object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ValueError('control registry must be an object')
    return value


def load_policy(path: Path) -> dict[str, Any]:
    value = private_json(path)
    if set(value) != POLICY_KEYS or type(value['schema_version']) is not int or value['schema_version'] != 1:
        raise ValueError('unsupported control verification policy')
    identifier(value['node_id'])
    integer(value['lease_seconds'], 'control lease', minimum=30, maximum=600)
    integer(value['maximum_jobs_per_cycle'], 'cycle jobs', minimum=1, maximum=16)
    for name in ('state_root', 'runtime_lock'):
        p = Path(value[name])
        if not p.is_absolute() or p.resolve() != p:
            raise ValueError('control policy paths must be absolute and unredirected')
    root = Path(value['state_root'])
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ValueError('control state root must remain private and owned')
    if validate_runtime_lock(private_json(Path(value['runtime_lock'])), runtime_identity()):
        raise ValueError('control role runtime differs from accepted Mamba lock')
    return value


@contextmanager
def owner(policy: dict[str, Any]):
    root = Path(policy['state_root'])
    descriptor = os.open(root / 'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise ValueError('unsafe control owner lock')
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield root
    finally:
        os.close(descriptor)


def binding_spec(value: dict[str, Any]) -> WorkSpec:
    if set(value) != BINDING_KEYS or type(value['schema_version']) is not int or value['schema_version'] != 1:
        raise ValueError('unsupported frozen release binding')
    spec = WorkSpec.from_dict(value['work_spec'])
    if spec.key != 'code-release-' + spec.inputs['receipt_sha256']:
        raise ValueError('binding key must identify its exact immutable receipt')
    for name in ('receipt', 'source_root'):
        p = Path(value[name])
        if not p.is_absolute() or p.resolve() != p:
            raise ValueError('frozen release path redirected')
    return spec


def enroll(policy: dict[str, Any], dsn: str, receipt: Path, source_root: Path,
           *, schema: str = 'stockagent_control') -> dict[str, Any]:
    receipt = receipt.resolve(strict=True)
    source_root = source_root.resolve(strict=True)
    # Enrollment requires the existing full bundle/source verifier, not stat or
    # a filename. No controller may switch source bytes behind the binding.
    verify_release_bundles(receipt)
    proof = verify_source_release(receipt, source_root)
    spec = WorkSpec('code-release-' + proof['receipt_sha256'], {
        'receipt_sha256': proof['receipt_sha256'], 'source_sha256': proof['source_sha256'],
    }, max_attempts=3)
    value = {'schema_version': 1, 'receipt': str(receipt), 'source_root': str(source_root),
             'work_spec': spec.as_dict()}
    with owner(policy) as root:
        path = root / (spec.key + '.json')
        if path.exists() or path.is_symlink():
            existing = private_json(path)
            if existing != value:
                raise ValueError('preserve existing binding; local paths or work contract differ')
        else:
            atomic_write_json(path, value)
            path.chmod(0o600)
        with ControlStore(dsn, schema=schema) as store:
            job = store.submit(spec)
    return {'state': 'enrolled', 'work_key': spec.key, 'identity_sha256': spec.identity_sha256,
            'job_state': job['state'], 'source_sha256': proof['source_sha256']}


def cycle(policy: dict[str, Any], dsn: str, *, schema: str = 'stockagent_control') -> dict[str, Any]:
    started = time.perf_counter()
    with owner(policy) as root:
        bindings = []
        for path in sorted(root.glob('code-release-*.json')):
            value = private_json(path)
            spec = binding_spec(value)
            if path.name != spec.key + '.json':
                raise ValueError('binding filename differs from its immutable work')
            # Detect changed receipt before submitting/claiming so another
            # release's attempts cannot be consumed by this local binding.
            if hashlib.sha256(Path(value['receipt']).read_bytes()).hexdigest() != spec.inputs['receipt_sha256']:
                raise ValueError('frozen receipt changed; source retained')
            bindings.append((value, spec))
        with ControlStore(dsn, schema=schema) as store:
            store.register_node(policy['node_id'], **node_observation(root),
                                ttl_seconds=policy['lease_seconds'] * 2)
            store.expire_leases()
            for _, spec in bindings:
                store.submit(spec)
            before = {row['key']: row['state'] for row in store.snapshot()['jobs']}
        results = []
        for value, spec in bindings:
            if before.get(spec.key) != 'pending':
                continue
            if len(results) >= policy['maximum_jobs_per_cycle']:
                break
            results.append(execute_one(dsn, schema=schema, node_id=policy['node_id'],
                worker_id=policy['node_id'] + '-code-verifier', receipt=Path(value['receipt']),
                root=Path(value['source_root']), output=root / 'attempts',
                lease_seconds=policy['lease_seconds'], work_key=spec.key))
        with ControlStore(dsn, schema=schema) as store:
            keys = {spec.key for _, spec in bindings}
            states = {row['key']: row['state'] for row in store.snapshot()['jobs'] if row['key'] in keys}
        errors = [{'work_key': result['work_key'], 'state': result['state'],
                   'error_type': result.get('error_type')} for result in results
                  if result['state'] in {'attempt_failed', 'lease_lost'}]
        verified = 0
        for value, spec in bindings:
            if states.get(spec.key) != 'succeeded':
                continue
            try:
                current = verify_source_release(Path(value['receipt']), Path(value['source_root']))
                if any(current[name] != spec.inputs[name] for name in spec.inputs):
                    raise ValueError('completed release bytes changed')
                verified += 1
            except (OSError, ValueError, RuntimeError) as error:
                errors.append({'work_key': spec.key, 'state': 'current_source_invalid',
                               'error_type': type(error).__name__})
        state = ('waiting_registration' if not bindings else
                 'ready' if verified == len(bindings) and not errors else
                 'degraded' if errors or 'failed' in states.values() else 'working')
        report = {'schema_version': 1, 'state': state,
                  'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                  'node_id': policy['node_id'], 'registered_release_count': len(bindings),
                  'verified_release_count': verified, 'work_states': states,
                  'attempts_executed': sum(r['state'] != 'no_eligible_work' for r in results),
                  'complete_workflow_seconds': time.perf_counter() - started, 'errors': errors,
                  'scope': 'registered frozen code verification; no collector, GPU, data publication or execution ownership'}
        atomic_write_json(root / 'status.json', report)
        (root / 'status.json').chmod(0o600)
        return report
