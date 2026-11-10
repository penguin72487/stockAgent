from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import estimate_e_stage_backup_delta as estimate


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_hash_chunked_file_checks_full_content_and_rejects_links(tmp_path):
    root = tmp_path / 'root'
    root.mkdir()
    (root / 'file').write_bytes(b'aaaabbbbcc')
    observed = estimate.hash_chunked_file(root, 'file', sha(b'aaaabbbbcc'), 10, 4)
    assert observed['chunks'] == [(sha(b'aaaa'), 4), (sha(b'bbbb'), 4), (sha(b'cc'), 2)]
    with pytest.raises(ValueError, match='SHA'):
        estimate.hash_chunked_file(root, 'file', sha(b'wrong'), 10, 4)
    (root / 'link').symlink_to(root / 'file')
    with pytest.raises(OSError):
        estimate.hash_chunked_file(root, 'link', sha(b'aaaabbbbcc'), 10, 4)
    with pytest.raises(ValueError, match='path'):
        estimate.hash_chunked_file(root, '../file', sha(b'aaaabbbbcc'), 10, 4)


def test_catalog_matching_is_exact_and_streamable(tmp_path):
    a, b = sha(b'aaaa'), sha(b'bbbb')
    p = tmp_path / 'catalog.json'
    p.write_text(json.dumps({'releases': [{'unrelated': [1, 2, 3]}], 'files': [
        {'relative': f'objects/blobs/{a[:2]}/{a}.blob', 'sha256': a, 'bytes': 4},
        {'relative': f'objects/blobs/{b[:2]}/{b}.blob', 'sha256': b, 'bytes': 99},
        {'relative': f'objects/packs/{b[:2]}/{b}.zip', 'sha256': b, 'bytes': 4},
    ]}))
    with p.open('rb') as stream:
        assert estimate.find_catalog_matches(stream, {a: 4, b: 4}) == {a: 4}


def test_layout_rejects_routing_the_estimator_does_not_model(monkeypatch):
    monkeypatch.setattr(estimate, 'MIB', 1)
    entry = {'reader_contract': 'packed-v2-chunks', 'file_chunk_bytes': 4,
             'loose_threshold_mib': 4}
    assert estimate.check_fixed_chunk_layout(entry) == 4
    with pytest.raises(ValueError, match='loose-file threshold'):
        estimate.check_fixed_chunk_layout({**entry, 'loose_threshold_mib': 2})
    with pytest.raises(ValueError, match='loose-file threshold'):
        estimate.check_fixed_chunk_layout({**entry, 'file_chunking': 'fastcdc-v1'})
    assert estimate.is_chunked_change(5, 4)
    assert not estimate.is_chunked_change(3, 4)
    with pytest.raises(ValueError, match='exactly at the loose threshold'):
        estimate.is_chunked_change(4, 4)


def test_full_estimate_deduplicates_new_chunks_and_separates_small_packs(tmp_path, monkeypatch):
    workspace = tmp_path / 'workspace'
    verify, destination, evidence = (workspace / 'verify', workspace / 'data', workspace / 'evidence')
    for path in (verify, destination, evidence):
        path.mkdir(parents=True)
    payloads = {'u': b'U', 'a': b'aaaaBBBBCCCC', 'b': b'aaaaBBBBdddd', 'small': b'new'}
    db = sqlite3.connect(evidence / 'files.sqlite')
    db.execute('CREATE TABLE files (path TEXT,sha TEXT,bytes INTEGER,kind TEXT,state TEXT)')
    for name, content in payloads.items():
        (destination / name).write_bytes(content)
        db.execute('INSERT INTO files VALUES (?,?,?,?,?)', (name, sha(content), len(content), 'file', 'verified'))
    db.commit()
    db.close()
    counts = {'exact_files': 1, 'changed_files': 3, 'e_only_files': 0,
              'changed_bytes': 27, 'e_only_bytes': 0}
    comparison = {'contract': 'stockagent.single_e_stage_d_inventory_comparison.v1',
        'retirement_authorized': False, 'source': {'name': 'item', 'dataset': 'binance',
        'state': 'inventory_compared', 'staged_files': 4, 'staged_bytes': 28,
        'D_snapshot_id': 'fixed', 'D_inventory_sha256': sha(b'inventory'), 'counts': counts}}
    comparison_path = verify / 'comparison.json'
    comparison_path.write_text(json.dumps(comparison))
    output = verify / 'estimate.json'
    proof = {'files': 4, 'logical_bytes': 28}
    entry = {'dataset': 'binance', 'publish': True, 'reader_contract': 'packed-v2-chunks',
             'file_chunk_bytes': 4, 'loose_threshold_mib': 4}
    baseline = {'archive': {'inventory': {'sha256': sha(b'inventory')},
                            'objects': [{'kind': 'blob', 'sha256': sha(b'aaaa'), 'bytes': 4}]}}
    rows = [
        {'kind': 'file', 'path': 'u', 'sha256': sha(b'U'), 'size': 1},
        {'kind': 'file', 'path': 'a', 'sha256': sha(b'old-a'), 'size': 5},
        {'kind': 'file', 'path': 'b', 'sha256': sha(b'old-b'), 'size': 5},
        {'kind': 'file', 'path': 'small', 'sha256': sha(b'old'), 'size': 3},
    ]
    existing = sha(b'BBBB')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(json.dumps({'files': [{'relative': f'objects/blobs/{existing[:2]}/{existing}.blob',
                                               'sha256': existing, 'bytes': 4}]}))
    monkeypatch.setattr(estimate.volume, 'admit', lambda *_: None)
    monkeypatch.setattr(estimate, 'MIB', 1)
    monkeypatch.setattr(estimate.volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(estimate.volume, 'path', lambda name: verify)
    monkeypatch.setattr(estimate.queue, 'load_sources', lambda: [{'name': 'item'}])
    monkeypatch.setattr(estimate.queue, 'verified_report', lambda *_: proof)
    monkeypatch.setattr(estimate.queue, 'catalog_entries', lambda *_: [entry])
    monkeypatch.setattr(estimate.queue, 'paths', lambda *_: (tmp_path, destination, evidence, output))
    monkeypatch.setattr(estimate, '_check_d_primary_mount', lambda *_: None)
    monkeypatch.setattr(estimate, 'resolve_packed_snapshot_id',
                        lambda *_args, **_kw: SimpleNamespace(manifest=baseline, manifest_sha256=sha(b'manifest')))
    monkeypatch.setattr(estimate, '_iter_inventory', lambda *_: iter(rows))
    monkeypatch.setattr(estimate, 'BACKUP_CATALOG', catalog)
    monkeypatch.setattr(sys, 'argv', ['estimate', '--name', 'item', '--comparison', str(comparison_path),
                                     '--output', str(output), '--global-catalog'])
    estimate.main()
    result = json.loads(output.read_text())
    assert result['base_reused_extent_bytes'] == 8
    assert result['not_in_base_unique_chunk_bytes'] == 12
    assert result['known_in_older_global_catalog_unique_bytes'] == 4
    assert result['not_seen_in_base_or_old_catalog_unique_bytes'] == 8
    assert result['changed_small_files'] == 1 and result['changed_small_logical_bytes'] == 3
    assert result['E_changed_large_files_full_sha_checked']
    assert not result['D_object_contents_verified'] and not result['backup_or_retirement_authorized']
