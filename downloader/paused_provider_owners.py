"""Identify explicitly disabled, inactive general download owners.

This is a dispatch observation, not permission to start a paused service or
discard its work. Unknown systemd/process state fails closed.
"""
from __future__ import annotations

from pathlib import Path
import subprocess


UNITS = {
    'finlab': ('stockagent-finlab-local-refresh.service', 'stockagent-finlab-local-refresh.timer'),
    'free': ('stockagent-finmind-free.service',),
    'sponsor': ('stockagent-finmind-sponsor.service',),
    'complement': ('stockagent-finmind-complement.service',),
}
MODULES = {
    'finlab': ('download_finlab_history', 'run_finlab_refresh'),
    'free': ('download_finmind_free', 'run_finmind_free'),
    'sponsor': ('download_finmind_sponsor', 'run_finmind_sponsor'),
    'complement': ('download_finmind_complement', 'run_finmind_complement'),
}


def parse_units(text: str) -> dict[str, dict[str, str]]:
    rows = {}
    for block in text.strip().split('\n\n'):
        values = {}
        for line in block.splitlines():
            if '=' not in line:
                raise ValueError('invalid systemd owner observation')
            key, value = line.split('=', 1)
            if key in values:
                raise ValueError('duplicate systemd owner field')
            values[key] = value
        if not {'Id', 'LoadState', 'ActiveState', 'UnitFileState'} <= values.keys():
            raise ValueError('incomplete systemd owner observation')
        if values['Id'] in rows:
            raise ValueError('duplicate systemd owner identity')
        rows[values['Id']] = values
    return rows


def _normal_owner_running(owner: str, proc_root: Path = Path('/proc')) -> bool:
    for proc in proc_root.iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            raw = (proc/'cmdline').read_bytes()
        except FileNotFoundError:
            continue
        args = [item.decode('utf-8', errors='surrogateescape') for item in raw.split(b'\0') if item]
        if not args:
            continue
        # Explicit finite checks are the intended consumers, not an inactive
        # general owner's process. Account/worker locks remain authoritative.
        if '--task-key' in args or '--priority-only' in args:
            continue
        for arg in args:
            # Compare only argv elements; do not match shell command strings.
            if ' ' in arg or '\n' in arg:
                continue
            stem = Path(arg).name.removesuffix('.py').removesuffix('.sh')
            if any(stem in {key, 'downloader.' + key} or stem.startswith(key + '_frozen')
                   for key in MODULES[owner]):
                return True
    return False


def explicitly_paused(owners: set[str]) -> frozenset[str]:
    """Return only named owners proven disabled, inactive and not running."""
    if not owners or set(owners) - UNITS.keys():
        return frozenset()
    units = [unit for owner in sorted(owners) for unit in UNITS[owner]]
    try:
        value = subprocess.run(['systemctl', 'show', *units, '--no-pager',
                                '--property=Id,LoadState,ActiveState,UnitFileState'],
                               capture_output=True, text=True, check=True, timeout=4)
        rows = parse_units(value.stdout)
        paused = set()
        for owner in owners:
            if all(unit in rows and rows[unit]['LoadState'] == 'loaded'
                   and rows[unit]['ActiveState'] == 'inactive'
                   and rows[unit]['UnitFileState'] == 'disabled' for unit in UNITS[owner]):
                if not _normal_owner_running(owner):
                    paused.add(owner)
        return frozenset(paused)
    except (OSError, ValueError, subprocess.SubprocessError):
        return frozenset()
