#!/usr/bin/env python3
"""Prepare the opt-in control role without modifying the selected native runtime.

PostgreSQL binaries are installed separately by the OS package manager. This
installer will only budget a fresh default cluster with no non-control data.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shlex
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text, durable_replace  # noqa: E402
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock  # noqa: E402
from scripts.manage_runtime_environments import create_role  # noqa: E402


def execute(argv, *, input=None, env=None):
    return subprocess.run(argv, input=input, env=env, cwd=ROOT, text=True, capture_output=True, check=True).stdout


def validate_control_role_root(env_root: Path):
    if env_root.resolve() == Path(sys.prefix).resolve():
        raise ValueError('control role cannot replace the selected native runtime')
    if env_root.exists() or env_root.is_symlink():
        raise ValueError('control role needs a fresh mamba prefix; preserve the existing environment')


def control_role_specification() -> tuple[Path, bool]:
    # This exact platform lock passed local and two-node workload acceptance.
    # Other platforms must resolve their own declaration and capture its builds.
    if sys.platform == 'linux' and platform.machine().lower() in {'x86_64', 'amd64'}:
        return ROOT/'configs/environments/locks/control-linux-64-20261003.explicit.txt', True
    return ROOT/'configs/environments/control.yml', False


def install_control_role(env_root: Path, evidence: Path):
    validate_control_role_root(env_root)
    specification, explicit = control_role_specification()
    installation = create_role(env_root.absolute(), specification, evidence.resolve(), explicit=explicit)
    role_lock = json.loads(execute([str(env_root/'bin/python'), '-c',
        'import json; from stockagent.runtime_identity import runtime_identity; print(json.dumps(runtime_identity()))']))
    atomic_write_json(evidence/'control-role-runtime-lock.json', role_lock)
    atomic_write_json(evidence/'control-role-installation.json', {
        'state':'locked_control_role_installed', 'manager':'mamba',
        'conda_explicit_sha256':installation['conda_explicit_sha256'],
        'specification_sha256':installation['specification_sha256'],
        'explicit_rebuild':installation['explicit_rebuild'],
        'role_runtime_sha256':role_lock['sha256'], 'native_runtime_unchanged':True,
        'database_restart':False, 'observed_at_utc':datetime.now(timezone.utc).isoformat(),
    })
    return role_lock


def control_environment_body(body: str, env_root: Path) -> tuple[str, str]:
    lines = body.splitlines(keepends=True)
    selected = [i for i, line in enumerate(lines) if line.startswith('CONTROL_PLANE_ENV_PATH=')]
    dsn_lines = [line for line in lines if line.startswith('CONTROL_PLANE_DSN=')]
    if len(selected) != 1 or len(dsn_lines) != 1:
        raise ValueError('private control environment needs one role and one DSN')
    values = shlex.split(dsn_lines[0].split('=', 1)[1], comments=True)
    if len(values) != 1 or not values[0]:
        raise ValueError('invalid private control DSN declaration')
    lines[selected[0]] = 'CONTROL_PLANE_ENV_PATH=' + shlex.quote(str(env_root.resolve())) + '\n'
    return ''.join(lines), values[0]


def activate_control_role(env_root: Path, env_file: Path, evidence: Path) -> dict:
    if (not env_file.is_file() or env_file.is_symlink()
            or env_file.stat().st_mode & 0o077 or env_file.stat().st_uid != os.geteuid()):
        raise ValueError('activation requires the existing private owned control environment')
    if not (env_root/'conda-meta').is_dir() or env_root.is_symlink():
        raise ValueError('activation requires the accepted mamba role')
    if (evidence/'control-role-activation.json').exists():
        raise ValueError('preserve the previous activation receipt')
    installation = json.loads((evidence/'control-role-installation.json').read_bytes())
    acceptance = json.loads((evidence/'acceptance.json').read_bytes())
    if (installation.get('state') != 'locked_control_role_installed'
            or installation.get('manager') != 'mamba'
            or acceptance.get('state') != 'accepted'
            or Path(acceptance['prefix']).resolve() != env_root.resolve()):
        raise ValueError('control role installation is not accepted for this prefix')
    role_lock = json.loads(execute([str(env_root/'bin/python'), '-c',
        'import json; from stockagent.runtime_identity import runtime_identity; print(json.dumps(runtime_identity()))']))
    if role_lock['sha256'] != installation['role_runtime_sha256']:
        raise ValueError('control role changed since installation')
    before = runtime_identity()
    original = env_file.read_bytes()
    updated, dsn = control_environment_body(original.decode(), env_root)
    # Exercise the new interpreter against the canonical read-only DB contract
    # before publishing its pointer. Credentials never enter argv or receipts.
    status_before = json.loads(execute(['/bin/bash', str(ROOT/'scripts/stockagent-control.sh'), 'status'],
        env={**os.environ, 'STOCKAGENT_CONTROL_ENV_FILE':str(env_file)}))
    status_role = json.loads(execute([str(env_root/'bin/python'), str(ROOT/'scripts/manage_control_work.py'), 'status'],
        env={**os.environ, 'CONTROL_PLANE_DSN':dsn}))
    if status_role != status_before:
        raise ValueError('new control role did not read the same canonical DB state')
    if env_file.read_bytes() != original:
        raise ValueError('private control environment changed during validation')
    descriptor, temporary_name = tempfile.mkstemp(prefix='.control-role-', dir=env_file.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(updated.encode()); handle.flush(); os.fsync(handle.fileno())
        durable_replace(temporary, env_file)
    finally:
        temporary.unlink(missing_ok=True)
    after = runtime_identity()
    if validate_runtime_lock(before, after):
        raise ValueError('native runtime changed while activating the control role')
    status_after = json.loads(execute(['/bin/bash', str(ROOT/'scripts/stockagent-control.sh'), 'status'],
        env={**os.environ, 'STOCKAGENT_CONTROL_ENV_FILE':str(env_file)}))
    if status_after != status_before:
        raise ValueError('control DB state changed during role activation')
    receipt = {'state':'mamba_control_role_activated', 'manager':'mamba',
        'prefix':str(env_root.resolve()), 'role_runtime_sha256':role_lock['sha256'],
        'private_environment_mode':oct(env_file.stat().st_mode & 0o777),
        'private_environment_before_sha256':hashlib.sha256(original).hexdigest(),
        'private_environment_after_sha256':hashlib.sha256(env_file.read_bytes()).hexdigest(),
        'canonical_db_state_unchanged':True, 'native_runtime_unchanged':True,
        'database_restart':False, 'observed_at_utc':datetime.now(timezone.utc).isoformat()}
    atomic_write_json(evidence/'control-role-activation.json', receipt)
    return receipt


def install_backup_units(env_file: Path):
    # systemd substitutions are paths, never SQL/DSN or shell text. Keep the
    # template tokens confined to the installer-owned unit files.
    if any(c in str(ROOT) + str(env_file) for c in '\n\r"\\'):
        raise ValueError('control unit paths contain unsupported syntax')
    for suffix in ('service', 'timer'):
        template = ROOT/f'deploy/systemd/stockagent-control-backup.{suffix}.in'
        text = template.read_text().replace('__REPO_ROOT__', str(ROOT)).replace('__CONTROL_ENV_FILE__', str(env_file.resolve()))
        atomic_write_text(f'/etc/systemd/system/stockagent-control-backup.{suffix}', text, durable=True)
    execute(['systemctl', 'daemon-reload'])
    execute(['systemctl', 'enable', '--now', 'stockagent-control-backup.timer'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster', default='18/main')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--env-root', type=Path, default=Path('/var/lib/stockagent/control-plane/mamba-role'))
    parser.add_argument('--env-file', type=Path, default=Path('/etc/stockagent/control-plane.env'))
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--install-backup-only', action='store_true',
                        help='install owned backup units without restarting PostgreSQL or installing packages')
    parser.add_argument('--install-role-only', action='store_true',
                        help='create a fresh isolated mamba control role without changing PostgreSQL or services')
    parser.add_argument('--activate-role-only', action='store_true',
                        help='validate an installed mamba role and publish its private pointer without restarting services')
    args = parser.parse_args()
    if os.geteuid() != 0 or not re.fullmatch(r'\d+/[a-zA-Z0-9_-]+', args.cluster):
        raise ValueError('root and an explicit safe local PostgreSQL cluster are required')
    if not 1 <= args.port <= 65535:
        raise ValueError('invalid PostgreSQL port')
    if sum((args.install_backup_only, args.install_role_only, args.activate_role_only)) > 1:
        raise ValueError('select one bounded installation action')
    if not args.install_backup_only and not args.activate_role_only:
        validate_control_role_root(args.env_root)
    if args.activate_role_only:
        print(json.dumps(activate_control_role(args.env_root, args.env_file, args.evidence)))
        return
    if args.install_role_only:
        install_control_role(args.env_root, args.evidence)
        print('locked_control_role_installed')
        return
    if args.install_backup_only:
        if not args.env_file.is_file() or args.env_file.stat().st_mode & 0o077:
            raise ValueError('backup needs the existing private control environment')
        install_backup_units(args.env_file)
        args.evidence.mkdir(parents=True, exist_ok=True)
        atomic_write_json(args.evidence/'control-backup-installation.json', {
            'state': 'backup_timer_installed', 'database_restart': False,
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'timer': execute(['systemctl', 'show', 'stockagent-control-backup.timer', '-p', 'ActiveState', '-p', 'NextElapseUSecRealtime'])})
        print('backup_timer_installed')
        return
    version, cluster = args.cluster.split('/')
    unit = f'postgresql@{version}-{cluster}.service'
    before = runtime_identity()
    pg = ['runuser', '-u', 'postgres', '--', 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
          '-p', str(args.port), '-At', '-d', 'postgres']
    databases = execute(pg, input="SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname;").splitlines()
    if set(databases) - {'postgres', 'stockagent_control'}:
        raise ValueError('cluster contains non-control databases; choose a dedicated cluster')
    role_exists = execute(pg, input="SELECT 1 FROM pg_roles WHERE rolname='stockagent_control';").strip() == '1'
    if role_exists and not args.env_file.is_file():
        raise ValueError('existing control role has no private environment; do not rotate unknown credentials')
    if args.env_file.exists() and not role_exists:
        raise ValueError('private environment exists without the expected role; reconcile explicitly')
    if not role_exists:
        password = secrets.token_urlsafe(36)
        # token_urlsafe alphabet has no SQL or shell metacharacters; the secret never enters argv/logs.
        execute(pg, input=f"CREATE ROLE stockagent_control LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{password}';")
        dsn = f'postgresql://stockagent_control:{password}@127.0.0.1:{args.port}/stockagent_control'
        args.env_file.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(args.env_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as file:
            file.write('CONTROL_PLANE_DSN=' + shlex.quote(dsn) + '\n')
            file.write('CONTROL_PLANE_ENV_PATH=' + shlex.quote(str(args.env_root.resolve())) + '\n')
            file.flush(); os.fsync(file.fileno())
    if args.env_file.stat().st_mode & 0o077:
        raise ValueError('control environment must remain private (0600)')
    if 'stockagent_control' not in databases:
        execute(pg, input='CREATE DATABASE stockagent_control OWNER stockagent_control;')
    for command in ["ALTER SYSTEM SET shared_buffers = '64MB';", "ALTER SYSTEM SET max_connections = '24';",
                    "ALTER SYSTEM SET work_mem = '4MB';", "ALTER SYSTEM SET maintenance_work_mem = '32MB';",
                    "ALTER SYSTEM SET listen_addresses = '127.0.0.1';"]:
        execute(pg, input=command)
    template = ROOT / 'deploy/systemd/stockagent-control.slice.in'
    atomic_write_text('/etc/systemd/system/stockagent-control.slice', template.read_text(), durable=True)
    dropin = Path('/etc/systemd/system') / f'{unit}.d/stockagent-control-budget.conf'
    atomic_write_text(dropin, (ROOT/'deploy/systemd/stockagent-control-db.conf.in').read_text(), durable=True)
    execute(['systemctl', 'daemon-reload'])
    execute(['systemctl', 'restart', unit])
    install_control_role(args.env_root, args.evidence)
    # The control interpreter is the configured role, not a replacement native environment.
    execute(['/bin/bash', str(ROOT/'scripts/stockagent-control.sh'), 'init'],
            env={**os.environ, 'STOCKAGENT_CONTROL_ENV_FILE': str(args.env_file)})
    activate_control_role(args.env_root, args.env_file, args.evidence)
    after = runtime_identity()
    if validate_runtime_lock(before, after):
        raise ValueError('native runtime changed while installing the separate control role')
    install_backup_units(args.env_file)
    role_lock = json.loads(execute([str(args.env_root/'bin/python'), '-c',
        'import json; from stockagent.runtime_identity import runtime_identity; print(json.dumps(runtime_identity()))']))
    args.evidence.mkdir(parents=True, exist_ok=True)
    atomic_write_json(args.evidence/'control-runtime-lock.json', role_lock)
    atomic_write_json(args.evidence/'native-runtime-before.json', before)
    atomic_write_json(args.evidence/'native-runtime-after.json', after)
    settings = execute(pg, input='SHOW server_version; SHOW shared_buffers; SHOW max_connections; SHOW listen_addresses;').splitlines()
    budget = execute(['systemctl', 'show', unit, '-p', 'ActiveState', '-p', 'Slice',
                      '-p', 'CPUWeight', '-p', 'MemoryHigh', '-p', 'MemoryMax'])
    result = {'schema_version':1,'observed_at_utc':datetime.now(timezone.utc).isoformat(),
              'state':'installed_pilot','cluster':args.cluster,'port':args.port,'settings':settings,
              'database':'stockagent_control','unit':unit,'resource_budget':budget,
              'private_env_sha256':hashlib.sha256(args.env_file.read_bytes()).hexdigest(),
              'private_env_mode':oct(args.env_file.stat().st_mode & 0o777),
              'native_runtime_unchanged':True,'role_runtime_sha256':role_lock['sha256'],
              'scope':'non-intraday read-only code verification pilot; no acquisition/training/ledger migration'}
    atomic_write_json(args.evidence/'control-installation.json', result)
    print(json.dumps({key:result[key] for key in ['state','cluster','database','native_runtime_unchanged']},ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'state':'installation_failed','error_type':type(error).__name__}),file=sys.stderr)
        raise SystemExit(1)
