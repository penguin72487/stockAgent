#!/usr/bin/env python3
"""Install bounded recovery on existing critical owners without restarting them."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / 'deploy/systemd/stockagent-boot-recovery.conf.in'
# Maximum retry delay and exponential steps. Existing first delay and restart
# semantics are preserved, except for owners that currently have no recovery.
PROFILES = {
    'stockagent-discord-bot.service': (30, 5),
    'stockagent-tw-day-trade-simulation.service': (30, 5),
    'stockagent-tw-overnight-simulation.service': (30, 5),
    'stockagent-public-dashboards.service': (30, 4),
    'stockagent-shioaji-taifex-dashboard.service': (60, 4),
    'stockagent-shioaji-taifex-bidask.service': (120, 4),
    'stockagent-shioaji-top200.service': (120, 4),
    'stockagent-tej-history.service': (120, 4),
    'stockagent-tw-public-source-events.service': (60, 4),
    'stockagent-finmind-free.service': (120, 4),
    'stockagent-finmind-sponsor.service': (120, 4),
    'stockagent-finmind-complement.service': (120, 4),
    'syncthing@root.service': (30, 5),
    'stockagent-d-cold-mount.service': (60, 4),
    'stockagent-backup-transport-mount.service': (60, 4),
    'postgresql@18-main.service': (60, 4),
}
NEW_RECOVERY = {
    'stockagent-backup-transport-mount.service': ('on-failure', '15s'),
    'postgresql@18-main.service': ('on-failure', '10s'),
}
POST_START = {
    'stockagent-d-cold-mount.service': 'stockagent-backup-transport-mount.service',
    'stockagent-backup-transport-mount.service': 'syncthing@root.service',
}
PROPERTIES = ('Id', 'LoadState', 'UnitFileState', 'FragmentPath', 'ActiveState',
              'MainPID', 'InvocationID', 'Restart', 'RestartUSec', 'RestartSteps',
              'RestartMaxDelayUSec', 'StartLimitIntervalUSec')


def control(*args: str) -> str:
    return subprocess.check_output(['systemctl', *args], text=True, timeout=30)


def snapshot() -> dict[str, dict[str, str]]:
    raw = control('show', *PROFILES, '--property=' + ','.join(PROPERTIES), '--no-pager')
    rows = [dict(line.split('=', 1) for line in block.splitlines() if '=' in line)
            for block in raw.strip().split('\n\n')]
    return {row['Id']: row for row in rows if 'Id' in row}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--install-audit-timer', action='store_true')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.install_audit_timer and not args.apply:
        parser.error('--install-audit-timer requires --apply')
    version = subprocess.check_output(['systemctl', '--version'], text=True).splitlines()[0]
    if int(version.split()[1]) < 254:
        raise SystemExit('systemd >=254 required; do not silently ignore retry backoff')
    if not re.fullmatch(r'[A-Za-z0-9/._-]+', str(ROOT)):
        raise SystemExit('Repository path cannot be rendered safely in systemd')
    before = snapshot()
    template = TEMPLATE.read_text()
    plan, skipped = {}, []
    for unit, (maximum, steps) in PROFILES.items():
        row = before[unit]
        if row.get('LoadState') != 'loaded' or row.get('UnitFileState') in {'masked', 'masked-runtime'}:
            skipped.append(unit)
            continue
        mode, delay = NEW_RECOVERY.get(unit, (row['Restart'], row['RestartUSec']))
        if mode not in {'always', 'on-failure'}:
            raise SystemExit(f'Unexpected existing restart policy: {unit}')
        post = (f'ExecStartPost=/usr/bin/bash {ROOT}/scripts/start_enabled_services.sh {POST_START[unit]}'
                if unit in POST_START else '')
        body = template
        for key, value in {'RESTART_MODE': mode, 'RESTART_SECONDS': delay,
                           'RESTART_STEPS': str(steps), 'RESTART_MAX_SECONDS': str(maximum) + 's',
                           'POST_START': post}.items():
            body = body.replace('__' + key + '__', value)
        plan[unit] = body
    report = {'observed_at_utc': datetime.now(UTC).isoformat(), 'systemd_version': version,
              'applied': False, 'before': before, 'planned_overrides': plan, 'skipped_units': skipped,
              'claim_boundary': 'Named installed owner configuration; no disabled owner is enabled or started, and no live service restart is requested.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.apply:
        if os.geteuid() != 0:
            raise SystemExit('Installing systemd recovery policy requires the existing root owner')
        backups = {}
        backup_dir = args.output.parent / ('policy-backups-' + datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ'))
        backup_dir.mkdir(mode=0o700)
        try:
            for unit, body in plan.items():
                path = Path('/etc/systemd/system') / (unit + '.d') / 'stockagent-boot-recovery.conf'
                path.parent.mkdir(exist_ok=True)
                previous = path.read_bytes() if path.exists() else None
                backups[path] = previous
                if previous is not None:
                    (backup_dir / (unit + '.conf')).write_bytes(previous)
                path.write_text(body)
                path.chmod(0o644)
            check = subprocess.run(['systemd-analyze', 'verify', '--man=no',
                                    *[before[u]['FragmentPath'] for u in plan]],
                                   capture_output=True, text=True, timeout=30)
            if check.returncode != 0:
                raise RuntimeError(f'Native systemd validation failed (exit {check.returncode}); overrides restored')
            control('daemon-reload')
        except Exception:
            for path, previous in backups.items():
                if previous is None:
                    path.unlink(missing_ok=True)
                else:
                    path.write_bytes(previous)
            control('daemon-reload')
            raise
        report['applied'] = True
        report['after'] = snapshot()
        report['override_sha256'] = {unit: hashlib.sha256(body.encode()).hexdigest() for unit, body in plan.items()}
        report['changed_running_identities'] = [
            unit for unit, row in before.items() if row.get('ActiveState') == 'active'
            and any(row.get(k) != report['after'][unit].get(k) for k in ('ActiveState', 'MainPID', 'InvocationID'))
        ]
        if args.install_audit_timer:
            names = ('stockagent-boot-recovery-audit.service', 'stockagent-boot-recovery-audit.timer')
            with tempfile.TemporaryDirectory(prefix='stockagent-boot-audit-') as directory:
                staged = []
                for name in names:
                    path = Path(directory) / name
                    path.write_text((ROOT / 'deploy/systemd' / (name + '.in')).read_text().replace('__REPO_ROOT__', str(ROOT)))
                    staged.append(path)
                subprocess.run(['systemd-analyze', 'verify', '--man=no', *map(str, staged)], check=True, capture_output=True, timeout=30)
                for path in staged:
                    target = Path('/etc/systemd/system') / path.name
                    if target.exists():
                        (backup_dir / path.name).write_bytes(target.read_bytes())
                    subprocess.run(['install', '-m', '0644', str(path), str(target)], check=True)
            control('daemon-reload')
            control('enable', '--now', 'stockagent-boot-recovery-audit.timer')
            report['audit_timer_installed'] = True
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'applied': report['applied'], 'planned_units': len(plan), 'skipped_units': skipped,
                      'changed_running_identities': report.get('changed_running_identities'), 'output': str(args.output)}))
    if report.get('changed_running_identities'):
        raise SystemExit('A running owner changed identity during policy installation; inspect receipt')


if __name__ == '__main__':
    main()
