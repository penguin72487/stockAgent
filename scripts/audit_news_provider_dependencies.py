"""Supplement a running local news audit with a limited dependency witness.

This is not a proof of every loaded library byte or of a frozen filesystem.
It records explicit repository files and package versions and rejects changed
files between observations. A file already modified after the supervised run
began cannot retroactively be certified as unchanged for the whole run.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import sys

from downloader.artifact_io import atomic_write_json

REPO = Path(__file__).resolve().parent.parent
EXTRA_FILES = (
    'downloader/news_fulltext_policy.py',
    'downloader/news_receipt_io.py',
    'stockagent/private_json.py',
)
PACKAGES = ('pyarrow', 'beautifulsoup4', 'lxml')


def _file_witness(path):
    with path.open('rb') as stream:
        before = path.stat()
        opened = os.fstat(stream.fileno())
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        after = path.stat()
    def identity(value):
        return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns]
    if identity(before) != identity(opened) or identity(before) != identity(after):
        raise ValueError('dependency_changed_during_observation:' + str(path))
    return {'stamp': identity(before), 'sha256': digest}


def observe(run_path, *, repo=REPO, files=EXTRA_FILES, packages=PACKAGES):
    run = json.loads(run_path.read_text())
    started = datetime.fromisoformat(run['started_at_utc'].replace('Z', '+00:00'))
    if started.tzinfo is None or started.utcoffset() is None:
        raise ValueError('run_start_requires_timezone')
    if started > datetime.now(timezone.utc):
        raise ValueError('run_start_is_in_future')
    elapsed = started.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    # The run receipt records microseconds; include the rest of that unit.
    start_ns = ((elapsed.days * 86400 + elapsed.seconds) * 1_000_000_000
                + elapsed.microseconds * 1000 + 999)
    observations = {name: _file_witness(repo / name) for name in files}
    if any(item['stamp'][3] > start_ns or item['stamp'][4] > start_ns for item in observations.values()):
        raise ValueError('dependency_modified_after_run_start')
    return {'run_id': run['run_id'], 'started_at_utc': run['started_at_utc'],
            'files': observations, 'package_versions': {name: version(name) for name in packages},
            'python': list(sys.version_info[:3]), 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'selected file content and filesystem stamps; package versions only; not all loaded library bytes'}


def verify(initial, current):
    keys = ('run_id', 'started_at_utc', 'files', 'package_versions', 'python', 'scope')
    if any(initial[key] != current[key] for key in keys):
        raise ValueError('dependency_witness_changed')
    return {'accepted': True, 'initial': initial, 'final_observed_at_utc': current['observed_at_utc']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--compare-to', type=Path)
    args = parser.parse_args(argv)
    current = observe(args.run)
    result = verify(json.loads(args.compare_to.read_text()), current) if args.compare_to else current
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
