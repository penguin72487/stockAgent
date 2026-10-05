#!/usr/bin/env python3
"""Install the enrolled lifecycle owners; never restart an active owner."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.control.lakehouse import configuration, guard


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    c = configuration()
    guard(c)
    if os.geteuid() != 0 or not re.fullmatch(r'[A-Za-z0-9/._-]+', str(ROOT)):
        raise ValueError('local root owner and safely rendered repository required')
    names = ('stockagent-storage-lifecycle.service', 'stockagent-lifecycle-control-backup.service',
             'stockagent-lifecycle-control-backup.timer', 'stockagent-lake-transport-gc.service',
             'stockagent-lake-transport-gc.timer')
    bodies = {name: (ROOT / 'deploy/systemd' / (name + '.in')).read_text().replace('__REPO_ROOT__', str(ROOT)) for name in names}
    for name in names:
        path = Path('/etc/systemd/system') / name
        known_start = 'ExecStart=/bin/bash ' + str(ROOT) + '/scripts/run_lakehouse_control.sh '
        if path.is_symlink() or (path.exists() and not (known_start in path.read_text()
            or (name.endswith('.timer') and ('Description=Resume lifecycle SQL encrypted backup' in path.read_text()
                                             or 'Description=Periodic encrypted backup of lakehouse lifecycle controls' in path.read_text()
                                             or 'Description=Bounded retirement of exact NAS-accepted immutable transport waves' in path.read_text())))):
            raise ValueError('preserve an unknown installed lifecycle unit')
    result = {'applied': False, 'owners': list(names), 'active_owners_restarted': False}
    if args.apply:
        backup = Path(c['state_root']) / ('unit-backups-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
        backup.mkdir(mode=0o700)
        before = {}
        try:
            for name, body in bodies.items():
                path = Path('/etc/systemd/system') / name
                before[path] = path.read_bytes() if path.exists() else None
                if before[path] is not None:
                    (backup / name).write_bytes(before[path])
                atomic_write_text(path, body)
                path.chmod(0o644)
            subprocess.run(['systemd-analyze', 'verify', '--man=no', *map(str, before)], capture_output=True, check=True, timeout=30)
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
        except Exception:
            for path, data in before.items():
                if data is None:
                    path.unlink(missing_ok=True)
                else:
                    path.write_bytes(data)
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
            raise
        subprocess.run(['systemctl', 'enable', '--now', 'stockagent-storage-lifecycle.service',
                        'stockagent-lifecycle-control-backup.timer', 'stockagent-lake-transport-gc.timer'], check=True)
        result['applied'] = True
        atomic_write_json(Path(c['state_root']) / 'lifecycle-units.json', result)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
