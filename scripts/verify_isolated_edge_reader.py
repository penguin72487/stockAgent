#!/usr/bin/env python3
"""Exercise v2/v3 edge use against tiny temporary fixtures without network I/O.

This validates the selected Python and installed reader code. It does not read
the real cold store, contact Syncthing, or authorize a data migration.
"""
from __future__ import annotations

from contextlib import ExitStack, nullcontext, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import manage_packed_edge as edge
from stockagent.data_sync import (cache_capacity, dedup_stream_candidate as candidate,
                                  packed_dedup_plan as plan, packed_snapshots as packed,
                                  stream_repack, workspace_volume)


def verify() -> dict:
    with tempfile.TemporaryDirectory(prefix='stockagent-edge-reader-smoke-') as tmp:
        root = Path(tmp)
        cold, hot, state = (root/name for name in ('cold', 'hot', 'state'))
        state.mkdir()
        (state/'state.json').write_text(json.dumps({'schema_version': 1, 'mode': 'index-only',
                                                    'hydrating': {}, 'retained_payloads': {}}))
        with ExitStack() as mocks:
            mocks.enter_context(patch.object(packed, '_notify_packed_publication', lambda *_a, **_kw: None))
            mocks.enter_context(patch.object(workspace_volume, 'ENROLLMENT', root/'no-e-enrollment'))
            mocks.enter_context(patch.object(cache_capacity, 'reserve_cache_write', lambda *_a, **_kw: nullcontext()))
            mocks.enter_context(patch.object(edge, '_convergence', lambda *_a, **_kw: {'ok': True}))
            mocks.enter_context(patch.object(edge, '_scan', lambda *_a, **_kw: None))
            mocks.enter_context(patch.object(edge, 'process_references', lambda *_a: []))
            mocks.enter_context(patch.object(edge, '_prepare_ignored_payload_prune', lambda *_a, **_kw: {'ok': True}))
            mocks.enter_context(patch.dict(os.environ, {'STGUIAPIKEY': 'isolated-fixture'}))
            source = root/'source'
            (source/'one').mkdir(parents=True)
            (source/'two').mkdir()
            (source/'one/a').write_bytes(b'content')
            (source/'two/b').write_bytes(b'content')
            (source/'two/c').write_bytes(b'other')
            (source/'two/link').symlink_to('../one/a')
            options = {'node_id': 'test', 'loose_file_threshold_bytes': 8, 'file_chunk_bytes': 4, 'pack_buckets': 4}
            legacy = packed.publish_packed_snapshot(cold, 'legacy-fixture', source, **options)
            old = packed.publish_packed_snapshot(cold, 'alias-fixture', source, **options)
            reports, stage = root/'reports', root/'candidate'
            reports.mkdir()
            index = plan.build_plan(cold, 'alias-fixture', old.manifest_sha256, reports)['index_path']
            candidate.prepare(cold, old, stage, index)
            candidate.prepare_stream_adoption(cold, stage, 'alias-fixture', old.manifest_sha256)
            stream_repack.activate_content_alias_install(stage, 'alias-fixture', old.manifest_sha256,
                                                         reader_contract='packed-v3-content-alias')
            stream_repack.repack(cold, stage, 'alias-fixture', old.manifest_sha256, node_id='test')
            alias = packed.resolve_latest_packed(cold, 'alias-fixture')
            # Both fixtures share one synthetic cold store. Preserve the
            # second fixture's payload when the first edge-use prunes its
            # own now-unneeded payloads, just like concurrent retained leases.
            expires = time.time_ns() + 3600 * 10**9
            (state/'state.json').write_text(json.dumps({
                'schema_version': 1, 'mode': 'index-only', 'hydrating': {},
                'retained_payloads': {resolved.manifest['dataset']: {
                    'snapshot_id': resolved.manifest['snapshot_id'], 'expires_ns': expires}
                    for resolved in (legacy, alias)}}))
            results = []
            for resolved in (legacy, alias):
                name = resolved.manifest['dataset']
                check_stdout = io.StringIO()
                with patch.object(sys, 'argv', ['edge', '--sync-root', str(cold), 'reader-status', name]), \
                        redirect_stdout(check_stdout):
                    if edge.main() != 0:
                        raise RuntimeError('edge metadata status rejected isolated fixture')
                checked = json.loads(check_stdout.getvalue())
                if (checked.get('manifest_sha256') != resolved.manifest_sha256
                        or checked.get('payload_verified') is not False
                        or checked.get('payload_fetched') is not False):
                    raise RuntimeError('edge metadata status differs from fixture')
                args = ['edge', '--sync-root', str(cold), '--materialized-root', str(hot),
                        '--state-root', str(state), 'use', name, '--retain-payload', '--timeout-seconds', '3']
                stdout = io.StringIO()
                with patch.object(sys, 'argv', args), redirect_stdout(stdout):
                    if edge.main() != 0:
                        raise RuntimeError('edge use rejected isolated fixture')
                result = json.loads(stdout.getvalue())
                snapshot = resolved.manifest['snapshot_id']
                target = hot/name/snapshot
                if (result.get('mode') != 'index-only-use' or result.get('snapshot_id') != snapshot
                        or result.get('lease', {}).get('manifest_sha256') != resolved.manifest_sha256
                        or (target/'one/a').read_bytes() != b'content'
                        or (target/'two/b').read_bytes() != b'content'
                        or (target/'one/a').stat().st_ino == (target/'two/b').stat().st_ino
                        or not (target/'two/link').is_symlink()):
                    raise RuntimeError('edge fixture reconstruction differs')
                results.append({'schema_version': resolved.manifest['schema_version'],
                                'snapshot_id': snapshot, 'source_paths': 4})
    if {item['schema_version'] for item in results} != {2, 3}:
        raise RuntimeError('edge smoke must exercise both v2 and v3')
    return {'contract': 'stockagent.isolated_edge_reader_smoke.v1', 'state': 'passed',
            'network_used': False, 'real_data_accessed': False,
            'python_version': sys.version.split()[0], 'python_executable': sys.executable,
            'reader_root': str(ROOT), 'fixtures': results}


if __name__ == '__main__':
    print(json.dumps(verify(), ensure_ascii=False, sort_keys=True))
