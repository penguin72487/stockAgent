"""Accidental-deletion rescue never invents sources or overwrites existing data."""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import restore_vast_futures_raw_sources as recovery
from scripts.deliver_vast_futures_preparation import rename_no_replace
from scripts import relocate_futures_preparation_sources as layout
from scripts import compact_vast_futures_release_sources as compaction
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def raw(tmp_path, monkeypatch):
    legacy = tmp_path / 'old'
    monkeypatch.setattr(recovery, 'LEGACY', legacy)
    source = legacy / 'raw'
    source.mkdir(parents=True)
    path = source / 'index/1998.html'
    path.parent.mkdir()
    path.write_bytes(b'original official observations')
    receipt = {'path': str(path), 'bytes': path.stat().st_size,
               'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    return source, path, receipt


def test_exact_raw_source_receipt_passes(raw):
    source, path, receipt = raw
    rows = recovery.raw_inventory([receipt], source)
    assert rows[0]['relative'] == 'index/1998.html'
    assert rows[0]['sha256'] == receipt['sha256']
    assert path.read_bytes() == b'original official observations'


@pytest.mark.parametrize('field,value', [('bytes', 1), ('sha256', '0' * 64)])
def test_wrong_original_is_preserved_and_rejected(raw, field, value):
    source, path, receipt = raw
    before = path.read_bytes()
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([{**receipt, field: value}], source)
    assert path.read_bytes() == before


def test_unsafe_or_duplicate_receipt_is_rejected(raw):
    source, _, receipt = raw
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([receipt, receipt], source)
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([{**receipt, 'path': str(source / '../escape')}], source)
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([{**receipt, 'path': '/unrelated/source'}], source)


def test_source_redirect_is_rejected(raw):
    source, path, receipt = raw
    target = path.with_name('original.html')
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([receipt], source)
    assert target.read_bytes() == b'original official observations'


def test_source_mutating_during_hash_is_rejected(raw, monkeypatch):
    source, path, receipt = raw
    def changing_hash(p: Path):
        p.write_bytes(b'changed source during read')
        return receipt['sha256']
    monkeypatch.setattr(recovery, 'sha256_file', changing_hash)
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([receipt], source)
    assert path.exists()


def test_empty_recovery_is_not_success(raw):
    with pytest.raises(SnapshotError):
        recovery.raw_inventory([], raw[0])


def test_atomic_exposure_preserves_existing_destination(tmp_path):
    source = tmp_path / 'ours'
    destination = tmp_path / 'existing'
    source.mkdir()
    destination.mkdir()
    (destination / 'unique').write_text('preserve')
    with pytest.raises(OSError):
        rename_no_replace(source, destination)
    assert source.is_dir()
    assert (destination / 'unique').read_text() == 'preserve'


@pytest.fixture
def source_layout(tmp_path, monkeypatch):
    root = tmp_path / 'repo'
    (root / 'data_tw_index_futures').mkdir(parents=True)
    for name in layout.NAMES:
        source = root / 'data_tw_futures' / name
        source.mkdir(parents=True)
        (source / 'receipt').write_bytes(b'unchanged source evidence')
    monkeypatch.setattr(layout, 'process_references', lambda _: [])
    return root, tmp_path / 'private'


def test_layout_moves_same_inodes_without_copying_or_changing_receipts(source_layout):
    root, work = source_layout
    before = {name: (root / 'data_tw_futures' / name / 'receipt').stat().st_ino for name in layout.NAMES}
    plan = layout.relocate(root, work, apply=False)
    assert plan['state'] == 'source_layout_plan'
    assert not work.exists()
    result = layout.relocate(root, work, apply=True)
    assert result['payload_copies_created'] == 0
    for name in layout.NAMES:
        old = root / 'data_tw_futures' / name
        new = root / 'data_tw_index_futures/preparation_sources' / name
        assert old.is_symlink() and old.resolve() == new
        assert (new / 'receipt').stat().st_ino == before[name]
        assert (new / 'receipt').read_bytes() == b'unchanged source evidence'
    assert all(r['state'] == 'already_moved' for r in layout.relocate(root, work, apply=False)['moves'])


def test_layout_preserves_occupied_destinations(source_layout):
    root, work = source_layout
    occupied = root / 'data_tw_index_futures/preparation_sources' / layout.NAMES[0]
    occupied.mkdir(parents=True)
    (occupied / 'unique').write_text('preserve')
    with pytest.raises(SnapshotError):
        layout.relocate(root, work, apply=True)
    assert not work.exists()
    assert (occupied / 'unique').read_text() == 'preserve'
    assert (root / 'data_tw_futures' / layout.NAMES[0]).is_dir()


def test_layout_defers_active_source(source_layout, monkeypatch):
    root, work = source_layout
    monkeypatch.setattr(layout, 'process_references', lambda _: [{'pid': 1234}])
    with pytest.raises(SnapshotError):
        layout.relocate(root, work, apply=True)
    assert not work.exists()


@pytest.fixture
def duplicated_current(tmp_path, monkeypatch):
    root = tmp_path / 'repo'
    data = root / 'data_tw_index_futures/preparation_sources'
    original = data / 'margin_sources/rules/sources/original.html'
    original.parent.mkdir(parents=True)
    original.write_bytes(b'official original values')
    digest = hashlib.sha256(original.read_bytes()).hexdigest()
    source_manifest = data / 'margin_sources/source_manifest.json'
    source_manifest.write_text(json.dumps({'files': {'rules/sources/original.html': {'sha256': digest}}}))
    for name in ['margin_repair_pending', 'margin_repair_inputs']:
        (data / name).mkdir()
    build = root / 'artifacts/markets/tw_futures_v8_margin_preparation/margin_components_current'
    daily, rules = build / 'release/daily/continuous_daily.parquet', build / 'release/rules/rules.parquet'
    daily.parent.mkdir(parents=True)
    rules.parent.mkdir(parents=True)
    daily.write_bytes(b'exact original daily')
    rules.write_bytes(b'exact original compiled rules')
    for area in [build / 'terms', build / 'release/rules']:
        duplicate = area / 'sources/rules/sources/original.html'
        duplicate.parent.mkdir(parents=True)
        duplicate.write_bytes(original.read_bytes())
        (area / 'manifest.json').write_text(json.dumps({'sources': [
            {'path': 'sources/rules/sources/original.html', 'sha256': digest}]}))
    expected = {'daily_sha256': hashlib.sha256(daily.read_bytes()).hexdigest(),
                'rules_sha256': hashlib.sha256(rules.read_bytes()).hexdigest(),
                'source_manifest_sha256': hashlib.sha256(source_manifest.read_bytes()).hexdigest()}
    monkeypatch.setattr(compaction, 'process_references', lambda _: [])
    return root, tmp_path / 'private', build, original, expected


def test_raw_duplicate_compaction_preserves_exact_sources_and_core(duplicated_current):
    root, work, build, original, expected = duplicated_current
    original_inode = original.stat().st_ino
    plan = compaction.compact(root, work, expected, apply=False)
    assert plan['duplicate_files'] == 2 and not work.exists()
    result = compaction.compact(root, work, expected, apply=True)
    assert result['duplicate_files_removed'] == 2
    assert result['canonical_source_payloads_deleted'] == 0
    assert result['new_payload_copies_created'] == 0
    assert original.stat().st_ino == original_inode
    for area in [build / 'terms', build / 'release/rules']:
        assert (area / 'sources/rules').is_symlink()
        path = area / 'sources/rules/sources/original.html'
        assert path.read_bytes() == original.read_bytes()
        assert path.stat().st_ino == original_inode
    assert compaction.compact(root, work, expected, apply=True)['state'] == 'raw_sources_already_canonical_references'


def test_mismatched_raw_duplicate_is_never_removed(duplicated_current):
    root, work, build, _, expected = duplicated_current
    duplicate = build / 'terms/sources/rules/sources/original.html'
    duplicate.write_bytes(b'unique mismatched bytes')
    with pytest.raises(SnapshotError):
        compaction.compact(root, work, expected, apply=True)
    assert duplicate.read_bytes() == b'unique mismatched bytes'
    assert not work.exists()


def test_unknown_file_in_raw_group_is_never_removed(duplicated_current):
    root, work, build, _, expected = duplicated_current
    extra = build / 'terms/sources/rules/unique'
    extra.write_text('preserve unknown data')
    with pytest.raises(SnapshotError):
        compaction.compact(root, work, expected, apply=True)
    assert extra.read_text() == 'preserve unknown data'
    assert not work.exists()
