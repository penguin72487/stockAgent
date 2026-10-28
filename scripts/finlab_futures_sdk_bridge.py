"""Private stdin-only adapter used by the existing FinLab minute owner."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, date, datetime
import importlib.metadata
import io
import json
from pathlib import Path
import sys


def main() -> int:
    # SDK output and provider exception text can contain private details; only
    # a sanitized status reaches the parent. Never persist the supplied session.
    captured = io.StringIO()
    with redirect_stdout(captured), redirect_stderr(captured):
        request = json.load(sys.stdin)
        from scripts.download_finlab_intraday import (
            fetch_partition, initialize_intraday_worker, record_failed_partition,
        )
        root, key = Path(request['root']), request['key']
        day = date.fromisoformat(request['day'])
        version = importlib.metadata.version('finlab')
        if version != '2.2.18' or not key.startswith('tw_futures_minute:'):
            return 2
        initialize_intraday_worker(request['session'], keepalive=False)
        try:
            receipt = fetch_partition(root, key, day, now=datetime.now(UTC), force_download=False)
            result = {'sdk_version': version, 'status': receipt['status']}
        except Exception as exc:
            result = {'sdk_version': version, 'failure': record_failed_partition(root, key, day, exc)}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
