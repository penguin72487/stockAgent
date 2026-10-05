#!/usr/bin/env python3
"""Keep automatic boot observations using the canonical service audit."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_service_latency_coverage import build_report
from scripts.install_boot_recovery_policies import PROFILES, snapshot
from stockagent.control._schema import SCHEMA_VERSION
from stockagent.data_sync.storage_automation import bounded_receipt, systemd_observations, lakehouse_readiness


def control_verification_readiness(
    timers: dict, *,
    policy: Path = Path('/etc/stockagent/control-release-verification.json'),
    status: Path = Path('/var/lib/stockagent/control-release-verification/status.json'),
    now: datetime | None = None,
    service: dict | None = None,
) -> dict:
    """Admit the selected role from a fresh semantic result, not timer activity."""
    if not policy.exists() and not policy.is_symlink():
        return {'required': False, 'ready': True}
    observed = now or datetime.now(UTC)
    receipt = bounded_receipt(status, ('state', 'observed_at_utc', 'registered_release_count',
                                      'verified_release_count', 'errors'), now=observed.timestamp())
    value = receipt.get('values', {})
    try:
        info = policy.lstat()
        policy_valid = (stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                        and not info.st_mode & 0o077)
        timestamp = datetime.fromisoformat(value.get('observed_at_utc', ''))
        age = (observed - timestamp).total_seconds()
        fresh = timestamp.tzinfo is not None and 0 <= age <= 300
    except (OSError, ValueError, TypeError):
        policy_valid, fresh = False, False
    timer = timers.get('stockagent-control-release-verification.timer', {})
    scheduler_ready = timer.get('UnitFileState') == 'enabled' and timer.get('ActiveState') == 'active'
    if service is None:
        service = systemd_observations({'code': 'stockagent-control-release-verification'})['code']['service']
    service_ready = (service.get('LoadState') == 'loaded' and service.get('Result') == 'success'
                     and service.get('ExecMainStatus') == '0')
    registered, verified = value.get('registered_release_count'), value.get('verified_release_count')
    result_ready = (type(registered) is int and registered > 0 and type(verified) is int
                    and verified == registered and value.get('state') == 'ready' and value.get('errors') == [])
    return {'required': True, 'ready': policy_valid and scheduler_ready and service_ready and fresh and result_ready,
            'policy_valid': policy_valid, 'scheduler_ready': scheduler_ready,
            'last_service_success': service_ready,
            'receipt_fresh': fresh, 'receipt': receipt}


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def first_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
        temporary.chmod(0o600)
        try:
            os.link(temporary, path)  # Publish complete JSON without replacing history.
        except FileExistsError:
            pass
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-root', type=Path, default=Path('/var/lib/stockagent/boot-recovery'))
    args = parser.parse_args()
    args.state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    report = build_report(sample_seconds=1)
    units = snapshot()
    unavailable = [name for name in PROFILES if units[name].get('ActiveState') != 'active']
    guards = {}
    for name in ('mount_packed_d_cold.sh', 'mount_backup_transport.sh'):
        result = subprocess.run(['bash', str(ROOT / 'scripts' / name), '--check'],
                                capture_output=True, timeout=30)
        guards[name] = result.returncode == 0
    pg = subprocess.run(['runuser', '-u', 'postgres', '--', 'psql', '-X', '-At', '-q',
                         '-v', 'ON_ERROR_STOP=1', '-d', 'stockagent_control',
                         '-c', 'BEGIN READ ONLY; SELECT version FROM stockagent_control.contract_version; COMMIT;'],
                        capture_output=True, text=True, timeout=10)
    database_ready = pg.returncode == 0 and pg.stdout.strip() == str(SCHEMA_VERSION)
    startup = report['startup']
    runtime = startup.get('windows_wsl_runtime') or {}
    observed_namespace = os.readlink('/proc/self/ns/mnt')
    owner_namespace = os.readlink('/proc/1/ns/mnt')
    inactive_timers = [name for name, row in report['timers'].items()
                       if row.get('UnitFileState') == 'enabled' and row.get('ActiveState') != 'active']
    control_verification = control_verification_readiness(report['timers'])
    lakehouse = lakehouse_readiness(repo_root=ROOT)
    local_ready = (not unavailable and all(guards.values()) and database_ready
                   and startup.get('gateway_health_http') == 200
                   and len(runtime.get('holders', [])) == 1
                   and observed_namespace == owner_namespace
                   and not inactive_timers and not report['timer_schedule_findings']
                   and control_verification['ready'] and lakehouse['ready'])
    report['boot_recovery'] = {
        'local_infrastructure_ready': local_ready,
        'unavailable_critical_owners': unavailable,
        'inactive_enabled_timers': inactive_timers,
        'mount_guards': guards,
        'postgres_contract_read_verified': database_ready,
        'control_release_verification': control_verification,
        'lakehouse_control': lakehouse,
        'observed_mount_namespace': observed_namespace,
        'owner_mount_namespace': owner_namespace,
        'production_owner_states': units,
        'claim_boundary': (
            'Current local infrastructure and canonical service/data projections. '
            'Closed markets, source gaps and blocked settlement remain visible. '
            'Collection success is not cold-boot, broker-fill or full-history backup acceptance.'
        ),
    }
    identity = startup.get('wsl_boot_id', 'unknown')
    userspace = startup.get('wsl_userspace_at_utc')
    stamp = datetime.fromisoformat(userspace).strftime('%Y%m%dT%H%M%S%fZ') if userspace else 'userspace-unknown'
    first = args.state_root / f'first-{identity}-{stamp}.json'
    # Keep the first observation for each userspace lifetime. WSL can restart
    # systemd repeatedly without changing its kernel boot_id.
    first_json(first, report)
    atomic_json(args.state_root / 'latest.json', report)
    print(json.dumps({'local_infrastructure_ready': local_ready, 'receipt': str(args.state_root / 'latest.json'),
                      'unavailable_critical_owners': unavailable, 'collection_completed': True}))
    raise SystemExit(0 if local_ready else 75)


if __name__ == '__main__':
    main()
