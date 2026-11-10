"""V3 edge integration, kept separate from the previously frozen v1 handoff."""
import json
import sys
from contextlib import nullcontext

import pytest

from scripts import manage_packed_edge as edge
from stockagent.data_sync import (cache_capacity, dedup_stream_candidate as candidate,
                                  materialized_cache, packed_dedup_plan as plan,
                                  packed_edge_cache, packed_snapshots as packed,
                                  stream_repack, workspace_volume)


@pytest.fixture
def published_alias(tmp_path, monkeypatch):
    source, cold, reports, stage = (tmp_path/name for name in ('source', 'cold', 'reports', 'candidate'))
    (source/'one').mkdir(parents=True)
    (source/'two').mkdir()
    (source/'one/a').write_bytes(b'content')
    (source/'two/b').write_bytes(b'content')
    (source/'two/c').write_bytes(b'other')
    (source/'two/link').symlink_to('../one/a')
    reports.mkdir()
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', tmp_path/'no-e-enrollment')
    monkeypatch.setattr(cache_capacity, 'reserve_cache_write', lambda *_a, **_kw: nullcontext())
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
                                         loose_file_threshold_bytes=8, file_chunk_bytes=4, pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    candidate.prepare(cold, old, stage, index)
    candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    stream_repack.activate_content_alias_install(stage, 'fixture', old.manifest_sha256,
                                                 reader_contract='packed-v3-content-alias')
    stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    return cold, packed.resolve_latest_packed(cold, 'fixture')


def test_v3_release_materializes_through_the_edge_cache_contract(published_alias, tmp_path):
    cold, current = published_alias
    needed = packed_edge_cache.release_payload_relpaths(current)
    assert needed == {item['relpath'] for item in current.manifest['archive']['objects']}
    lease = materialized_cache.use_materialized_snapshot(cold, tmp_path/'hot', 'fixture',
                                                         snapshot_id=current.manifest['snapshot_id'])
    target = tmp_path/'hot'/'fixture'/current.manifest['snapshot_id']
    assert lease['manifest_sha256'] == current.manifest_sha256 and lease['verification'] == 'full'
    assert (target/'one/a').read_bytes() == (target/'two/b').read_bytes() == b'content'
    assert (target/'one/a').stat().st_ino != (target/'two/b').stat().st_ino


def test_reader_status_observes_v3_metadata_without_credentials_or_payload(published_alias, monkeypatch, capsys):
    cold, current = published_alias
    monkeypatch.delenv('STGUIAPIKEY', raising=False)
    monkeypatch.delenv('OPEN_BUTTON_TOKEN', raising=False)
    monkeypatch.setattr(edge, '_convergence', lambda *_a, **_kw: pytest.fail('unexpected network'))
    monkeypatch.setattr(sys, 'argv', ['edge', '--sync-root', str(cold), 'reader-status', 'fixture'])
    assert edge.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result['schema_version'] == 3 and result['manifest_sha256'] == current.manifest_sha256
    assert result['payload_fetched'] is False and result['payload_verified'] is False


def test_v3_release_materializes_through_real_edge_use_entrypoint(published_alias, tmp_path, monkeypatch, capsys):
    """Exercise the canonical edge CLI, not only the underlying reader."""
    cold, current = published_alias
    state = tmp_path/'edge-state'
    state.mkdir()
    (state/'state.json').write_text(json.dumps({'schema_version': 1, 'mode': 'index-only',
                                                'hydrating': {}, 'retained_payloads': {}}))
    monkeypatch.setenv('STGUIAPIKEY', 'fixture-only')
    monkeypatch.setattr(edge, '_convergence', lambda *_args, **_kwargs: {'ok': True})
    monkeypatch.setattr(edge, '_scan', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(edge, 'process_references', lambda *_args: [])
    monkeypatch.setattr(edge, '_prepare_ignored_payload_prune', lambda *_args, **_kwargs: {'ok': True})
    hot = tmp_path/'hot'
    monkeypatch.setattr(sys, 'argv', ['edge', '--sync-root', str(cold), '--materialized-root', str(hot),
                                     '--state-root', str(state), 'use', 'fixture', '--retain-payload'])
    assert edge.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result['mode'] == 'index-only-use' and result['snapshot_id'] == current.manifest['snapshot_id']
    assert result['lease']['manifest_sha256'] == current.manifest_sha256
    target = hot/'fixture'/current.manifest['snapshot_id']
    assert (target/'one/a').read_bytes() == (target/'two/b').read_bytes() == b'content'
    assert (target/'one/a').stat().st_ino != (target/'two/b').stat().st_ino
