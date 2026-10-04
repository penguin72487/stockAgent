#!/usr/bin/env python3
"""Install a hash-pinned official Temporal CLI into a fresh private trial root."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_bytes, atomic_write_json

VERSION = '1.9.1'
ARCHIVE_SHA256 = '09a0326a51db84d02735e53542b9ebd8c4758daf47482a9ab0abce15844e60d5'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise ValueError('this explicit CLI pin is Linux x86_64 only')
    output = args.output.absolute()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    started = time.perf_counter()
    base = f'https://github.com/temporalio/cli/releases/download/v{VERSION}/'
    name = f'temporal_cli_{VERSION}_linux_amd64.tar.gz'
    request = urllib.request.Request(base+name, headers={'User-Agent':'StockAgent-architecture-trial'})
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read(128*1024**2+1)
    if len(body) > 128*1024**2 or hashlib.sha256(body).hexdigest() != ARCHIVE_SHA256:
        raise ValueError('official CLI archive content differs from its pinned release digest')
    atomic_write_bytes(output/name, body)
    with tarfile.open(fileobj=io.BytesIO(body), mode='r:gz') as archive:
        members = [m for m in archive.getmembers() if m.name == 'temporal' and m.isfile()]
        if len(members) != 1:
            raise ValueError('official CLI archive must contain exactly one regular temporal executable')
        executable = archive.extractfile(members[0]).read()
    atomic_write_bytes(output/'temporal', executable)
    (output/'temporal').chmod(0o700)
    version = subprocess.run([str(output/'temporal'), '--version'], check=True, capture_output=True,
                             text=True, timeout=10).stdout.strip()
    receipt = {'state':'accepted', 'release_url':base+name, 'version_output':version,
               'archive_sha256':ARCHIVE_SHA256, 'executable_sha256':hashlib.sha256(executable).hexdigest(),
               'complete_wall_seconds':time.perf_counter()-started,
               'scope':'isolated trial CLI installation; production Temporal is not deployed'}
    atomic_write_json(output/'acceptance.json', receipt)
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
