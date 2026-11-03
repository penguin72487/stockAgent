"""Offline inventory tests use isolated SQLite and source envelopes only."""
from contextlib import closing
import json

import pytest

from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from downloader.news_fulltext_queue import NewsQueue
from scripts import audit_news_cnyes_history_originals as audit_module
from test_cnyes_news_bulk import BASE, cfg, registry_http
from test_news_cnyes_history_evidence import bounds, capture, source_payload


def saved_pages(cfg, root):
    registry, _ = registry_http(cfg, root)
    urls = [cnyes_url(BASE, start=start, end=start + 99)
            for start in (1388534400, 1388534600)]
    with closing(NewsQueue(root, registry)) as queue:
        for url in urls:
            capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    return registry.sources['cnyes'], urls


def test_history_inventory_verifies_retained_envelopes_without_queue_writes(cfg, tmp_path):
    source, _ = saved_pages(cfg, tmp_path)
    db = tmp_path / 'queue.sqlite3'
    before = db.stat().st_mtime_ns, db.stat().st_size
    result = audit_module.audit(tmp_path, source)
    assert result['head_rows'] == 2 and result['original_envelope_outcomes'] == {'verified': 2}
    assert result['referenced_paths'] == 4 and result['input_snapshot_stable']
    assert not result['owner_repair_scan_completed'] and not result['article_bodies_verified']
    assert not result['all_history_complete'] and result['publisher_requests'] == 0
    assert result['production_queue_writes'] == 0
    assert (db.stat().st_mtime_ns, db.stat().st_size) == before


def test_history_inventory_reports_alias_separately(cfg, tmp_path):
    import sqlite3

    source, urls = saved_pages(cfg, tmp_path)
    with closing(sqlite3.connect(tmp_path / 'queue.sqlite3')) as db, db:
        first = db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads WHERE scope_url=?',
                           (cnyes_scope(urls[0]),)).fetchone()[0]
        db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=?',
                   (first, cnyes_scope(urls[1])))
    result = audit_module.audit(tmp_path, source)
    assert result['original_envelope_outcomes'] == {'verified': 2}
    assert result['alias_head_outcomes'] == {'verified': 1}
    assert not result['alias_request_identity_proven']


def test_history_inventory_does_not_mark_a_missing_original_complete(cfg, tmp_path):
    source, _ = saved_pages(cfg, tmp_path)
    original = next(tmp_path.glob('discovery_responses/by_request/**/*.parquet'))
    original.unlink()
    result = audit_module.audit(tmp_path, source)
    assert result['original_envelope_outcomes'] == {'verified': 1, 'unavailable': 1}
    assert result['uninspectable_paths'] == 1 and not result['input_snapshot_stable']
    assert not result['owner_repair_scan_completed']


def test_history_inventory_classifies_a_stably_corrupt_receipt(cfg, tmp_path):
    source, _ = saved_pages(cfg, tmp_path)
    receipt = next(tmp_path.glob('discovery_responses/by_request/**/*.parquet.receipt.json'))
    value = json.loads(receipt.read_text())
    value['sha256'] = '0' * 64
    receipt.write_text(json.dumps(value))
    result = audit_module.audit(tmp_path, source)
    assert result['original_envelope_outcomes'] == {'verified': 1, 'invalid': 1}
    assert result['input_snapshot_stable'] and not result['owner_repair_scan_completed']


def test_history_inventory_detects_receipt_change_after_verification(cfg, tmp_path, monkeypatch):
    source, _ = saved_pages(cfg, tmp_path)
    receipt = next(tmp_path.glob('discovery_responses/by_request/**/*.parquet.receipt.json'))
    original_check = audit_module._OriginalEnvelopeAuditCache.check

    def change_receipt_after_check(self, root, artifact, *args):
        outcome = original_check(self, root, artifact, *args)
        if root / artifact['receipt_path'] == receipt:
            with receipt.open('ab') as stream:
                stream.write(b' ')
        return outcome

    monkeypatch.setattr(audit_module._OriginalEnvelopeAuditCache, 'check', change_receipt_after_check)
    result = audit_module.audit(tmp_path, source)
    assert result['changed_paths'] == 1 and not result['input_snapshot_stable']


@pytest.mark.parametrize('suffix', ['-wal', '-journal'])
def test_history_inventory_rejects_uncheckpointed_database(cfg, tmp_path, suffix):
    source, _ = saved_pages(cfg, tmp_path)
    (tmp_path / ('queue.sqlite3' + suffix)).write_bytes(b'not checkpointed')
    with pytest.raises(ValueError, match='nonempty_.*_requires_stopped_checkpointed_owner'):
        audit_module.audit(tmp_path, source)


def test_history_inventory_rejects_output_symlink_inside_source(tmp_path, monkeypatch):
    root = tmp_path / 'source'
    root.mkdir()
    report = root / 'report.json'
    report.symlink_to(tmp_path / 'outside.json')
    monkeypatch.setattr('sys.argv', ['audit', '--root', str(root), '--output', str(report)])
    with pytest.raises(SystemExit) as error:
        audit_module.main()
    assert error.value.code == 2 and report.is_symlink()
