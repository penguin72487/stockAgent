import gzip
import json
from types import SimpleNamespace

import pytest

from stockagent.data_sync import index_generation_diff as diff, packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


def setup_generations(tmp_path, monkeypatch):
    pub_source, priv_source = tmp_path/'public-source', tmp_path/'private-source'
    pub_source.mkdir()
    priv_source.mkdir()
    (pub_source/'same.parquet').write_bytes(b'unchanged')
    (priv_source/'same.parquet').write_bytes(b'unchanged')
    (pub_source/'conflict.parquet').write_bytes(b'old-value')
    (priv_source/'conflict.parquet').write_bytes(b'new-value')
    (pub_source/'public-only.json').write_bytes(b'old')
    (priv_source/'private-only.json').write_bytes(b'new')
    (priv_source/'restricted').mkdir()
    (priv_source/'restricted/secret.json').write_bytes(b'restricted')
    pub, priv = tmp_path/'public-D', tmp_path/'private-D'
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    first = packed.publish_packed_snapshot(pub, 'fixture', pub_source, node_id='test')
    second = packed.publish_packed_snapshot(priv, 'fixture', priv_source, node_id='test')
    return pub, priv, first, second, tmp_path/'E-worklist'


def test_generation_diff_uses_metadata_and_honors_private_only_scope(tmp_path, monkeypatch):
    public, private, first, second, output = setup_generations(tmp_path, monkeypatch)
    result = diff.compare(public, private, 'fixture', second.manifest['snapshot_id'], second.manifest_sha256,
                          ('restricted',), output)
    assert result['counters']['equal'] == 1
    assert result['counters']['conflict'] == 1 and result['conflict_categories'] == {'parquet': 1}
    assert result['counters']['public_only'] == result['counters']['private_only'] == 1
    assert result['counters']['private_excluded'] == 2 and result['counters']['unexpected_public_excluded'] == 0
    assert not result['source_payload_read'] and not result['public_head_changed']
    assert packed.resolve_latest_packed(public, 'fixture').manifest_sha256 == first.manifest_sha256
    with gzip.open(output/result['worklist']['name'], 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    assert {row['decision'] for row in rows} == {'private_only', 'public_only', 'private_excluded', 'conflict'}
    assert len(rows) == 5


def test_generation_diff_rejects_changed_current_head(tmp_path, monkeypatch):
    public, private, first, second, output = setup_generations(tmp_path, monkeypatch)
    original, calls = packed.resolve_latest_packed, 0
    def changed(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs) if calls == 1 else SimpleNamespace(manifest_sha256='a'*64)
    monkeypatch.setattr(packed, 'resolve_latest_packed', changed)
    with pytest.raises(SnapshotError, match='head changed'):
        diff.compare(public, private, 'fixture', second.manifest['snapshot_id'], second.manifest_sha256, (), output)
    assert not (output/'diff.json').exists()


def test_generation_diff_rejects_changed_private_identity(tmp_path, monkeypatch):
    public, private, first, second, output = setup_generations(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='private source generation changed'):
        diff.compare(public, private, 'fixture', second.manifest['snapshot_id'], 'a'*64, (), output)
    assert packed.resolve_latest_packed(public, 'fixture').manifest_sha256 == first.manifest_sha256


def test_fixed_old_comparison_is_preserved_without_replacing_moving_status(tmp_path, monkeypatch):
    public, private, first, second, output = setup_generations(tmp_path, monkeypatch)
    initial = diff.compare(public, private, 'fixture', second.manifest['snapshot_id'], second.manifest_sha256,
                           ('restricted',), output)
    moving = (output/'diff.json').read_bytes()
    source = tmp_path/'replacement'
    source.mkdir()
    (source/'fresh').write_bytes(b'new head')
    packed.publish_packed_snapshot(public, 'fixture', source, node_id='test')
    fixed = diff.compare(public, private, 'fixture', second.manifest['snapshot_id'], second.manifest_sha256,
        ('restricted',), output, public_snapshot_id=first.manifest['snapshot_id'],
        public_manifest_sha256=first.manifest_sha256)
    assert fixed['comparison_scope'] == 'fixed_public_snapshot'
    assert fixed['worklist']['sha256'] == initial['worklist']['sha256']
    assert (output/'diff.json').read_bytes() == moving
    assert (output/('diff-plan-'+initial['identity_sha256']+'.json')).is_file()
    assert (output/('diff-plan-'+fixed['identity_sha256']+'.json')).is_file()
    with pytest.raises(SnapshotError, match='fixed public source generation changed'):
        diff.compare(public, private, 'fixture', second.manifest['snapshot_id'], second.manifest_sha256,
            ('restricted',), output, public_snapshot_id=first.manifest['snapshot_id'], public_manifest_sha256='a'*64)
