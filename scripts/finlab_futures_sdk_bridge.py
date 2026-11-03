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
            FuturesWorkspaceUnavailableBeforeSource, FuturesWorkspacePersistenceUnknown,
        )
        root, key = Path(request['root']), request['key']
        day = date.fromisoformat(request['day'])
        version = importlib.metadata.version('finlab')
        if version != '2.2.18' or not key.startswith('tw_futures_minute:'):
            return 2
        initialize_intraday_worker(request['session'], keepalive=False)
        # This process handles just one partition. The shared owner writes the
        # validated immutable Parquet and receipt; the SDK's default
        # FileStorage would write an additional, untracked frame under its
        # home-directory cache, on the authority filesystem. Retain the SDK's
        # validation and per-request in-memory cache without a second payload.
        from finlab import data
        data.set_storage(data.CacheStorage())
        try:
            workspace = request.get('futures_workspace', False)
            if type(workspace) is not bool:
                raise ValueError('invalid futures workspace mode')
            receipt = fetch_partition(root, key, day, now=datetime.now(UTC), force_download=False,
                                      futures_workspace=workspace)
            result = {'sdk_version': version, 'status': receipt['status']}
        except FuturesWorkspaceUnavailableBeforeSource:
            result = {'sdk_version': version, 'failure': 'workspace_unavailable_before_source'}
        except FuturesWorkspacePersistenceUnknown:
            from downloader.artifact_io import atomic_write_json
            from scripts.download_finlab_intraday import _partition_paths
            _, attempt_path = _partition_paths(root, key, day)
            atomic_write_json(attempt_path, {'dataset': key, 'trade_date': day.isoformat(),
                'status': 'source_outcome_unknown', 'attempted_at_utc': datetime.now(UTC).isoformat(),
                'exception_class': 'FuturesWorkspacePersistenceUnknown', 'message_retained': False})
            result = {'sdk_version': version, 'failure': 'source_outcome_unknown'}
        except Exception as exc:
            result = {'sdk_version': version, 'failure': record_failed_partition(root, key, day, exc)}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
