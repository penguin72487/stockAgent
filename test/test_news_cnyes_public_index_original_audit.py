"""Offline audit checks; fixtures are isolated, no publisher or production DB."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_cnyes_public_index_originals import audit
from test_cnyes_news_bulk import cfg, registry_http
from test_news_cnyes_public_index import accept, index_payload


def saved_page(cfg, root):
    registry, _ = registry_http(cfg, root)
    with closing(NewsQueue(root, registry)) as queue:
        accept(queue, index_payload())
    return registry.sources['cnyes']


def test_audit_checks_original_receipt_observation_without_writes(cfg, tmp_path):
    source = saved_page(cfg, tmp_path)
    db = tmp_path / 'queue.sqlite3'
    before = db.stat().st_mtime_ns, db.stat().st_size
    result = audit(tmp_path, source)
    assert (result['originals'], result['receipts'], result['observation_rows'], result['head_rows']) == (1, 1, 1, 1)
    assert result['outcomes']['verified_clock_matches'] == 1
    assert result['input_snapshot_stable'] and result['issue_count'] == result['dangling_heads'] == 0
    assert not result['article_bodies_verified'] and not result['all_history_complete']
    assert not result['historical_http_status_or_headers_proven']
    assert (db.stat().st_mtime_ns, db.stat().st_size) == before


@pytest.mark.parametrize('damage,reason', [
    ('bad_hash', 'receipt_identity_or_hash_mismatch'),
    ('bad_count', 'observation_identity_or_count_mismatch'),
    ('unindexed', 'valid_unindexed_original'),
    ('missing_original', 'receipt_without_original'),
    ('missing_receipt', 'missing_receipt'),
])
def test_audit_keeps_each_kind_of_damage_distinct(cfg, tmp_path, damage, reason):
    source = saved_page(cfg, tmp_path)
    original = next(tmp_path.glob('list_metadata/cnyes/**/*.parquet'))
    receipt = Path(str(original) + '.receipt.json')
    if damage == 'bad_hash':
        value = json.loads(receipt.read_text())
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
    elif damage == 'bad_count':
        with closing(sqlite3.connect(tmp_path / 'queue.sqlite3')) as db, db:
            db.execute('UPDATE cnyes_public_index_observations SET metadata_rows=99')
    elif damage == 'unindexed':
        with closing(sqlite3.connect(tmp_path / 'queue.sqlite3')) as db, db:
            db.execute('DELETE FROM cnyes_public_index_observations')
    elif damage == 'missing_original':
        original.unlink()
    else:
        receipt.unlink()
    result = audit(tmp_path, source)
    assert result['input_snapshot_stable']
    assert reason in {item['reason'] for item in result['issues']}
    if damage == 'unindexed':
        assert result['dangling_heads'] == 1
        assert result['outcomes']['valid_unindexed_original'] == 1


def test_audit_accepts_legacy_receipt_without_optional_fields(cfg, tmp_path):
    source = saved_page(cfg, tmp_path)
    receipt = next(tmp_path.glob('list_metadata/cnyes/**/*.parquet.receipt.json'))
    value = json.loads(receipt.read_text())
    value.pop('index_data_contract_version', None)
    value.pop('blank_title_metadata_rows', None)
    receipt.write_text(json.dumps(value))
    result = audit(tmp_path, source)
    assert result['issue_count'] == 0 and result['outcomes']['verified_clock_matches'] == 1


@pytest.mark.parametrize('changed_file,change_before_read', [
    ('receipt', True), ('receipt', False), ('original', False),
])
def test_audit_detects_in_place_changes_even_when_directory_is_unchanged(
        cfg, tmp_path, monkeypatch, changed_file, change_before_read):
    from scripts import audit_news_cnyes_public_index_originals as audit_module

    source = saved_page(cfg, tmp_path)
    original = next(tmp_path.glob('list_metadata/cnyes/**/*.parquet'))
    receipt = Path(str(original) + '.receipt.json')
    changed = original if changed_file == 'original' else receipt
    directory_before = original.parent.stat().st_mtime_ns
    inspect = audit_module._inspect_original

    def inspect_with_change(*args, **kwargs):
        if change_before_read:
            with changed.open('ab') as stream:
                stream.write(b' ')
        outcome = inspect(*args, **kwargs)
        if not change_before_read:
            with changed.open('ab') as stream:
                stream.write(b' ')
        return outcome

    monkeypatch.setattr(audit_module, '_inspect_original', inspect_with_change)
    result = audit_module.audit(tmp_path, source)
    assert original.parent.stat().st_mtime_ns == directory_before
    assert result['outcomes']['verified_clock_matches'] == 1
    assert not result['input_snapshot_stable']
    assert result['changed_files'] == 1
    assert any(issue['reason'] == 'file_changed_during_audit' for issue in result['issues'])


def test_audit_refuses_nonempty_wal_without_touching_it(cfg, tmp_path):
    source = saved_page(cfg, tmp_path)
    wal = tmp_path / 'queue.sqlite3-wal'
    wal.write_bytes(b'not-a-checkpointed-wal')
    before = wal.read_bytes()
    with pytest.raises(ValueError, match='nonempty_wal'):
        audit(tmp_path, source)
    assert wal.read_bytes() == before


def test_audit_refuses_nonempty_rollback_journal_without_touching_it(cfg, tmp_path):
    source = saved_page(cfg, tmp_path)
    journal = tmp_path / 'queue.sqlite3-journal'
    journal.write_bytes(b'unverified-rollback-journal')
    before = journal.read_bytes()
    with pytest.raises(ValueError, match='nonempty_journal'):
        audit(tmp_path, source)
    assert journal.read_bytes() == before


def test_audit_report_symlink_inside_source_root_is_refused(tmp_path, monkeypatch):
    from scripts import audit_news_cnyes_public_index_originals as audit_module

    root = tmp_path / 'source'
    root.mkdir()
    outside = tmp_path / 'outside.json'
    outside.write_text('keep')
    output = root / 'report.json'
    output.symlink_to(outside)
    monkeypatch.setattr(sys, 'argv', ['audit', '--root', str(root), '--output', str(output)])
    monkeypatch.setattr(audit_module, 'load_config', lambda _: (None, SimpleNamespace(sources={'cnyes': object()}), None))
    monkeypatch.setattr(audit_module, 'audit', lambda *_: {
        'input_snapshot_stable': True, 'issue_count': 0, 'dangling_heads': 0})

    with pytest.raises(SystemExit) as exc:
        audit_module.main()
    assert exc.value.code == 2
    assert output.is_symlink() and outside.read_text() == 'keep'
