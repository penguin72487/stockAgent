#!/usr/bin/env python3
"""Capture and resume enrolled finite FinLab work across a WSL shutdown.

The existing priority handoff remains the only provider executor. Unknown
transient agent jobs block the shutdown plan; they are never guessed or killed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
STATE = Path('/var/lib/stockagent/wsl-maintenance')
RUNS = ROOT / 'artifacts/operations/agent-workflow/runs'
CONTRACT = 'stockagent.wsl_maintenance_finite_resume.v1'
IDENTIFIER = re.compile(r'[0-9a-f]{64}\Z')
UNIT = re.compile(r'stockagent-agent-([a-zA-Z0-9_-]+)\.service\Z')
PLAN = re.compile(r'wsl-[a-zA-Z0-9-]+\.json\Z')
TASK = re.compile(r'[a-zA-Z0-9_-]+\Z')


def checked_json(path: Path) -> dict:
    if (path.is_symlink() or path.parent.resolve() != path.parent or not path.is_file()
            or path.stat().st_size > 2 * 1024**2):
        raise ValueError('untrusted or oversized maintenance record')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError('maintenance record must be an object')
    return value


def parse_finlab(argv: list[str]) -> tuple[list[str], list[str]]:
    if (not isinstance(argv, list) or not all(isinstance(v, str) for v in argv)
            or argv[:2] != ['run_fintech_python', 'scripts/run_finlab_finite_priority_handoff.py']):
        raise ValueError('unknown transient owner requires its own recovery plan')
    ids, preferred, seen = [], [], set()
    index = 2
    while index < len(argv):
        option = argv[index]
        if option == '--futures-workspace-only':
            if option in seen:
                raise ValueError('duplicate workspace flag')
            seen.add(option)
            index += 1
            continue
        if option not in {'--request-id', '--preferred-futures-request-id', '--predecessor-pid',
                          '--predecessor-start-ticks', '--progress'} or index + 1 >= len(argv):
            raise ValueError('unknown or incomplete FinLab handoff option')
        value = argv[index + 1]
        if option in {'--request-id', '--preferred-futures-request-id'}:
            if not IDENTIFIER.fullmatch(value):
                raise ValueError('invalid registered request identity')
            (ids if option == '--request-id' else preferred).append(value)
        else:
            if option in seen:
                raise ValueError('duplicate FinLab handoff option')
            seen.add(option)
            if option in {'--predecessor-pid', '--predecessor-start-ticks'}:
                if not value.isdecimal() or int(value) <= 0:
                    raise ValueError('invalid predecessor identity')
            else:
                path = Path(value)
                if path.is_absolute() or '..' in path.parts or not path.is_relative_to('artifacts/operations'):
                    raise ValueError('progress path is outside the canonical operations root')
        index += 2
    required = {'--futures-workspace-only', '--predecessor-pid', '--predecessor-start-ticks', '--progress'}
    if (not required <= seen or not ids or len(ids) != len(set(ids))
            or len(preferred) != len(set(preferred)) or not set(preferred) <= set(ids)):
        raise ValueError('incomplete or duplicate finite request scope')
    return ids, preferred


def active_agent_units() -> list[str]:
    result = subprocess.run(['systemctl', 'list-units', '--type=service', '--state=active,activating',
                             'stockagent-agent-*', '--plain', '--no-legend', '--no-pager'],
                            check=True, capture_output=True, text=True, timeout=15)
    units = [line.split()[0] for line in result.stdout.splitlines() if line.split()]
    if len(units) > 128 or any(UNIT.fullmatch(unit) is None for unit in units):
        raise ValueError('unbounded or unknown transient agent unit')
    return units


def capture_rows(units: list[str], runs: Path | None = None) -> list[dict]:
    from scripts.agent_workflow import process_identity
    runs = RUNS if runs is None else runs
    rows = []
    for unit in units:
        match = UNIT.fullmatch(unit)
        if match is None:
            raise ValueError('invalid transient unit identity')
        record = checked_json(runs / match[1] / 'run.json')
        identity = record.get('process')
        if (record.get('run_id') != match[1] or record.get('unit') != unit
                or record.get('state') != 'running' or not isinstance(identity, dict)
                or process_identity(identity.get('pid', -1)) != identity
                or not isinstance(record.get('task_id'), str) or not TASK.fullmatch(record['task_id'])):
            raise ValueError('transient run is not stably owned; retry capture')
        parse_finlab(record.get('argv'))
        rows.append({key: record[key] for key in ('run_id', 'unit', 'task_id', 'argv', 'process')})
    return rows


def plan_identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True).encode()).hexdigest()


def plan_path(name: str, state: Path | None = None) -> Path:
    if not PLAN.fullmatch(name):
        raise ValueError('invalid fixed maintenance plan name')
    state = STATE if state is None else state
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = state.lstat()
    if state.is_symlink() or state.resolve() != state or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise ValueError('untrusted maintenance state directory')
    return state / name


def atomic_new(path: Path, value: dict) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(value, output, sort_keys=True, ensure_ascii=True)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    except BaseException:
        # A partially written plan is evidence and blocks replay; do not delete it.
        raise


@contextmanager
def owner(state: Path):
    fd = os.open(state / 'resume.lock', os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held, named = os.fstat(fd), (state / 'resume.lock').lstat()
        if (held.st_dev, held.st_ino, held.st_uid, held.st_mode & 0o777, held.st_nlink) != (
                named.st_dev, named.st_ino, os.geteuid(), 0o600, 1):
            raise ValueError('untrusted maintenance owner lock')
        yield
    finally:
        os.close(fd)


def capture(name: str) -> dict:
    path = plan_path(name)
    with owner(path.parent):
        rows = capture_rows(active_agent_units())
        captured_ids = {identifier for row in rows for identifier in parse_finlab(row['argv'])[0]}
        if not live_finlab_requests() <= captured_ids:
            raise ValueError('an unmanaged FinLab finite owner needs its original recovery plan')
        payload = {'contract': CONTRACT, 'created_at_utc': datetime.now(UTC).isoformat(),
                   'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                   'script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'runs': rows}
        value = {**payload, 'identity_sha256': plan_identity(payload)}
        atomic_new(path, value)
    return {'state': 'captured', 'plan': str(path), 'identity_sha256': value['identity_sha256'],
            'run_count': len(rows), 'provider_calls': 0}


def resumed_argv(row: dict, plan_name: str, excluded: frozenset[str] = frozenset()) -> tuple[str, list[str]]:
    ids, preferred = parse_finlab(row['argv'])
    nonce = hashlib.sha256((plan_name + '\0' + row['run_id']).encode()).hexdigest()[:16]
    name = 'fl-wsl-resume-' + nonce
    argv = list(row['argv'][:2])
    index = 2
    while index < len(row['argv']):
        option = row['argv'][index]
        if option == '--futures-workspace-only':
            argv.append(option)
            index += 1
            continue
        value = row['argv'][index + 1]
        if option not in {'--request-id', '--preferred-futures-request-id'} or value not in excluded:
            argv.extend((option, value))
        index += 2
    progress = argv.index('--progress')
    argv[progress + 1] = 'artifacts/operations/' + name + '.json'
    # Retain the original predecessor and all original request identities.
    if parse_finlab(argv) != ([v for v in ids if v not in excluded],
                              [v for v in preferred if v not in excluded]):
        raise RuntimeError('finite request scope changed')
    return name, argv


def live_finlab_requests() -> set[str]:
    """Discover exact live request ownership without exposing process arguments."""
    owned = set()
    for process in Path('/proc').iterdir():
        if not process.name.isdecimal():
            continue
        try:
            with (process / 'cmdline').open('rb') as stream:
                raw = stream.read(65537)
            if len(raw) > 65536:
                continue
            argv = [value.decode('utf-8') for value in raw.rstrip(b'\0').split(b'\0')]
            if len(argv) < 2 or Path(argv[1]).name != 'run_finlab_finite_priority_handoff.py':
                continue
            ids, _ = parse_finlab(['run_fintech_python', 'scripts/run_finlab_finite_priority_handoff.py', *argv[2:]])
            owned.update(ids)
        except FileNotFoundError:
            continue
        except (OSError, UnicodeError) as error:
            raise BlockingIOError('cannot inspect a live finite owner') from error
    return owned


def verify_finlab_workspace() -> None:
    from stockagent.data_sync import provider_workspace
    try:
        entry = provider_workspace.load()['providers']['finlab']
        provider_workspace.check_entry('finlab', entry)
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        raise BlockingIOError('the enrolled FinLab workspace is not yet verified') from error


def restore(name: str, expected_identity: str) -> dict:
    from scripts.agent_workflow import process_identity, run_view
    path = plan_path(name)
    with owner(path.parent):
        plan = checked_json(path)
        payload = {k: v for k, v in plan.items() if k != 'identity_sha256'}
        if (plan.get('contract') != CONTRACT or plan_identity(payload) != expected_identity
                or plan.get('identity_sha256') != expected_identity
                or plan.get('script_sha256') != hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                or not isinstance(plan.get('runs'), list) or len(plan['runs']) > 128):
            raise ValueError('maintenance plan or recovery code identity differs')
        active = set(active_agent_units())
        already_owned = live_finlab_requests()
        results = []
        for row in plan['runs']:
            if (not isinstance(row, dict) or set(row) != {'run_id', 'unit', 'task_id', 'argv', 'process'}
                    or not isinstance(row['unit'], str) or UNIT.fullmatch(row['unit']) is None
                    or UNIT.fullmatch(row['unit'])[1] != row['run_id']
                    or not isinstance(row['task_id'], str) or TASK.fullmatch(row['task_id']) is None
                    or not isinstance(row['process'], dict) or type(row['process'].get('pid')) is not int):
                raise ValueError('invalid saved finite owner scope')
            original = checked_json(RUNS / row['run_id'] / 'run.json')
            if any(original.get(k) != row[k] for k in ('run_id', 'unit', 'task_id', 'argv', 'process')):
                raise ValueError('original finite run identity changed')
            if row['unit'] in active or process_identity(row['process']['pid']) == row['process']:
                raise BlockingIOError('original finite owner is still active')
            state = run_view(original)['state']
            if state == 'succeeded':
                results.append({'original_run': row['run_id'], 'state': 'already_completed'})
                continue
            if state not in {'cancelled', 'interrupted'}:
                raise ValueError('finite owner outcome requires review before resuming')
            ids, _ = parse_finlab(row['argv'])
            excluded = frozenset(ids) & already_owned
            if excluded == frozenset(ids):
                results.append({'original_run': row['run_id'], 'state': 'requests_already_owned'})
                continue
            verify_finlab_workspace()
            resume_name, argv = resumed_argv(row, name, excluded)
            previous = sorted(RUNS.glob(resume_name + '-*/run.json'))
            if len(previous) > 1:
                raise ValueError('more than one recovery run exists')
            if previous:
                prior = checked_json(previous[0])
                if prior.get('name') != resume_name or prior.get('task_id') != row['task_id'] or prior.get('argv') != argv:
                    raise ValueError('existing recovery run differs from plan')
                if run_view(prior)['state'] not in {'queued', 'running', 'succeeded'}:
                    raise ValueError('existing recovery outcome requires review')
                results.append({'original_run': row['run_id'], 'state': 'existing_recovery_owner',
                                'run_id': prior['run_id']})
                continue
            result = subprocess.run(['bash', str(ROOT / 'scripts/stockagent-agent.sh'), 'run', '--backend',
                                     'systemd', '--name', resume_name, row['task_id'], '--', *argv],
                                    cwd=ROOT, capture_output=True, text=True, check=True, timeout=30)
            launched = json.loads(result.stdout)
            results.append({'original_run': row['run_id'], 'state': 'requeued', 'run_id': launched['run_id']})
            already_owned.update(set(ids) - excluded)
        value = {'contract': CONTRACT, 'state': 'resumed', 'plan_identity_sha256': expected_identity,
                 'observed_at_utc': datetime.now(UTC).isoformat(), 'runs': results, 'provider_calls': 0}
        from downloader.artifact_io import atomic_write_json
        atomic_write_json(path.with_suffix('.result.json'), value)
        return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('capture', 'restore'))
    parser.add_argument('--name', required=True)
    parser.add_argument('--identity', default='')
    args = parser.parse_args()
    value = capture(args.name) if args.action == 'capture' else restore(args.name, args.identity)
    print(json.dumps(value, ensure_ascii=True))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except BlockingIOError as error:
        import sys
        print(json.dumps({'state': 'deferred', 'reason': str(error)}), file=sys.stderr)
        raise SystemExit(75) from error
