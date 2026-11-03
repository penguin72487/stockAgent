"""A saved CMNews head must be tied to its own retained response."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import audit_news_cmnews_retained_access as auditor
from scripts import readback_news_cmnews_retained_access as reader
from scripts.audit_news_cmnews_retained_access import audit, inspect_original
from scripts.audit_news_provider_storage_readback import snapshot, stamp
from scripts.readback_news_cmnews_retained_access import readback


def page(url, *, mask=False, wrong_canonical=False):
    slug = url.rsplit('/', 1)[-1]
    uuid = slug[-36:]
    canonical = 'https://cmnews.com.tw/article/other-' + uuid if wrong_canonical else url
    vip = ('<vip-content class="vipWrapper"><div class="vipContent"><div class="utilsMask">'
           '<p class="utilsMask__title">請登入以查看完整文章</p></div></div></vip-content>') if mask else ''
    return (f'<html><head><meta charset="utf-8"><link rel="canonical" href="{canonical}"></head>'
            f'<body><div class="articleContent" data-article-path="/article/{slug}" '
            f'data-author-id-article-id="{slug}" id="ArticleContainer-{uuid}">'
            f'<h1 class="articleContent__title">標題</h1><div class="articleContent__textContent">'
            f'<p>可見內容</p>{vip}</div></div></body></html>').encode()


@pytest.fixture
def example(tmp_path):
    root = tmp_path / 'news'
    root.mkdir()
    (root / 'owner.lock').touch()
    (root / 'batches').mkdir()
    (root / 'receipts').mkdir()
    source = sqlite3.connect(root / 'queue.sqlite3')
    source.executescript('''CREATE TABLE articles(id BLOB PRIMARY KEY,url TEXT,provider TEXT,body_sha256 TEXT,
        body_bytes INTEGER,captured_at_utc TEXT,state TEXT,parquet_path TEXT);
        CREATE INDEX stored_bodies ON articles(provider,parquet_path,id) WHERE body_sha256 IS NOT NULL;
        CREATE TABLE accepted_batches(path TEXT PRIMARY KEY,receipt_path TEXT);''')
    jobs = []
    for batch in range(2):
        rows, records = [], []
        path, receipt = f'batches/{batch}.parquet', f'receipts/{batch}.json'
        for offset in range(2):
            number = batch * 2 + offset
            uuid = f'00000000-0000-0000-0000-{number:012x}'
            url = f'https://cmnews.com.tw/article/forumnews-{uuid}'
            key = hashlib.sha256(url.encode()).digest()
            digest = hashlib.sha256(f'body{number}'.encode()).hexdigest()
            captured = '2026-10-29T00:00:00+00:00'
            raw = page(url, mask=number in (0, 2), wrong_canonical=number == 2)
            if number == 3:
                raw = None
            raw_sha = hashlib.sha256(raw or b'unavailable').hexdigest()
            rows.append((key, url, 'cmnews', digest, 5, captured, 'complete', path))
            records.append({'article_id': key.hex(), 'provider': 'cmnews', 'requested_url': url,
                            'body_sha256': digest, 'body_bytes': 5, 'captured_at_utc': captured,
                            'raw_response_body': raw, 'response_sha256': raw_sha,
                            'source_response_article_id': key.hex()})
        source.executemany('INSERT INTO articles VALUES(?,?,?,?,?,?,?,?)', rows)
        source.execute('INSERT INTO accepted_batches VALUES(?,?)', (path, receipt))
        pq.write_table(pa.Table.from_pylist(records), root / path)
        (root / receipt).write_text('{}')
        digest = hashlib.sha256(b''.join(sorted(row[0] for row in rows))).hexdigest()
        jobs.append((path, receipt, 2, digest, json.dumps(stamp(root / path)),
                     json.dumps(stamp(root / receipt)), None))
    source.commit()
    source.close()
    source_snapshot = snapshot(root)
    reference = tmp_path / 'reference.sqlite3'
    witness = sqlite3.connect(reference)
    witness.executescript('''CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE jobs(path TEXT PRIMARY KEY,receipt_path TEXT,selected_heads INTEGER,
            selected_ids_sha256 TEXT,body_stamp TEXT,receipt_stamp TEXT,error TEXT);''')
    meta = {'provider': 'cmnews', 'snapshot': source_snapshot, 'target_batches': 2, 'stored_heads': 4}
    witness.executemany('INSERT INTO metadata VALUES(?,?)',
                        [(key, json.dumps(value)) for key, value in meta.items()])
    witness.executemany('INSERT INTO jobs VALUES(?,?,?,?,?,?,?)', jobs)
    witness.commit()
    witness.close()
    readback = tmp_path / 'readback.json'
    readback.write_text(json.dumps({'accepted': True, 'provider': 'cmnews', 'snapshot': source_snapshot,
        'scope_sha256': 'a' * 64, 'provider_head_pointer_consistency': {'accepted': True},
        'counts': {'processed_batches': 2, 'verified_stored_heads': 4}}))
    return root, reference, readback, tmp_path / 'checkpoint.sqlite3'


def test_mask_is_scoped_and_confirmed_by_native_identity():
    url = 'https://cmnews.com.tw/article/forumnews-00000000-0000-0000-0000-000000000000'
    assert inspect_original(page(url, mask=True), url) == ('confirmed_login_mask', 'paywall_or_login')
    assert inspect_original(page(url), url) == ('no_observed_login_mask', None)
    assert inspect_original(b'', url) == ('html_parse_error', None)
    assert inspect_original(page(url, mask=True, wrong_canonical=True), url) == (
        'native_mismatch', 'article_identity_not_verified')
    unrelated = page(url).replace(b'</body>', page(url, mask=True).split(b'<vip-content')[1]
                                  .split(b'</vip-content>')[0].join([b'<aside><vip-content', b'</vip-content></aside></body>']))
    assert inspect_original(unrelated, url) == ('no_observed_login_mask', None)


def test_resume_keeps_scope_unknowns_and_rejects_changed_batch(example):
    root, reference, readback, checkpoint = example
    initial = audit(root, reference, readback, checkpoint, max_batches=1)
    assert initial['processed_batches'] == 1 and not initial['completed_scope']
    final = audit(root, reference, readback, checkpoint)
    assert final['accepted'] and final['processed_heads'] == 4
    assert final['outcomes'] == {'confirmed_login_mask': 1, 'no_observed_login_mask': 1,
                                 'native_mismatch': 1, 'original_unavailable_or_digest_mismatch': 1}
    assert final['unresolved_heads'] == 2
    assert final['publisher_requests'] == final['production_queue_writes'] == 0
    with sqlite3.connect(checkpoint) as db:
        assert db.execute('SELECT count(*) FROM candidates').fetchone()[0] == 2
        assert db.execute('SELECT count(*) FROM unresolved').fetchone()[0] == 2
    with (root / 'batches/0.parquet').open('ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='checkpoint_artifact_changed'):
        audit(root, reference, readback, checkpoint)


def test_incomplete_reference_and_symlink_checkpoint_are_rejected(example, tmp_path):
    root, reference, readback, checkpoint = example
    data = json.loads(readback.read_text())
    data['accepted'] = False
    readback.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='reference_provider_audit_not_accepted'):
        audit(root, reference, readback, checkpoint)
    link = tmp_path / 'link'
    link.symlink_to(checkpoint)
    with pytest.raises(ValueError, match='checkpoint_must_not_be_symlink'):
        audit(root, reference, readback, link)


def test_independent_readback_replays_bound_originals_and_detects_tampering(example, tmp_path):
    root, reference, report, checkpoint = example
    finished = audit(root, reference, report, checkpoint)
    log = tmp_path / 'run.log'
    log.write_text(json.dumps(finished) + '\n')
    result = readback(root, checkpoint, log)
    assert result['accepted'] and result['processed_heads'] == 4
    assert result['candidates_independently_replayed'] == {
        'paywall_or_login': 1, 'article_identity_not_verified': 1}
    db = sqlite3.connect(checkpoint)
    with db:
        db.execute("UPDATE candidates SET response_sha256=? WHERE native_outcome='paywall_or_login'", ('0' * 64,))
    db.close()
    with pytest.raises(ValueError, match='candidate_original_binding_changed'):
        readback(root, checkpoint, log)


def test_retry_of_failed_batch_keeps_successful_checkpoint(example, monkeypatch):
    root, reference, report, checkpoint = example
    original = auditor.inspect_batch
    def fail_first(root, source, job, rows):
        if job[0].endswith('0.parquet'):
            raise OSError('temporary read failure')
        return original(root, source, job, rows)
    monkeypatch.setattr(auditor, 'inspect_batch', fail_first)
    failed = audit(root, reference, report, checkpoint)
    assert failed['completed_scope'] and not failed['accepted'] and failed['failed_batches'] == 1
    assert failed['outcomes']['batch_validation_error'] == 2
    monkeypatch.setattr(auditor, 'inspect_batch', original)
    retried = audit(root, reference, report, checkpoint)
    assert retried['accepted'] and retried['failed_batches'] == 0 and retried['processed_heads'] == 4
    assert retried['outcomes']['confirmed_login_mask'] == 1


def test_checkpoint_rejects_changed_implementation_or_reference(example, monkeypatch):
    root, reference, report, checkpoint = example
    audit(root, reference, report, checkpoint, max_batches=1)
    original = auditor.implementation
    monkeypatch.setattr(auditor, 'implementation', lambda: {'changed': True})
    with pytest.raises(ValueError, match='checkpoint_identity_changed'):
        audit(root, reference, report, checkpoint)
    monkeypatch.setattr(auditor, 'implementation', original)
    report.write_text(report.read_text() + ' ')
    with pytest.raises(ValueError, match='checkpoint_identity_changed'):
        audit(root, reference, report, checkpoint)


@pytest.mark.parametrize('action', ['missing', 'wrong_path'])
def test_streaming_scan_rejects_source_witness_scope_mismatch(example, action):
    root, reference, report, checkpoint = example
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        if action == 'missing':
            source.execute("DELETE FROM articles WHERE parquet_path='batches/0.parquet'")
        else:
            source.execute("UPDATE articles SET parquet_path='batches/z.parquet' "
                           "WHERE parquet_path='batches/0.parquet'")
    source_snapshot = snapshot(root)
    with closing(sqlite3.connect(reference)) as witness, witness:
        witness.execute("UPDATE metadata SET value=? WHERE key='snapshot'", (json.dumps(source_snapshot),))
    data = json.loads(report.read_text())
    data['snapshot'] = source_snapshot
    report.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='source_scope_(identity|count)_mismatch'):
        audit(root, reference, report, checkpoint)


def test_v2_archive_readback_checks_old_scanner_and_active_parser(example, tmp_path, monkeypatch):
    root, reference, report, checkpoint = example
    finished = audit(root, reference, report, checkpoint)
    archive = tmp_path / 'archived_implementation'
    expected = auditor.implementation()
    for name in expected['files']:
        target = archive / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(auditor.REPO / name, target)
    old_scanner = archive / reader.SCANNER_PATH
    old_scanner.write_bytes(old_scanner.read_bytes() + b'\n# archived v2 scanner\n')
    expected['files'][reader.SCANNER_PATH] = hashlib.sha256(old_scanner.read_bytes()).hexdigest()
    with closing(sqlite3.connect(checkpoint)) as db, db:
        db.execute("UPDATE metadata SET value=? WHERE key='contract'",
                   (json.dumps('cmnews_retained_login_mask_audit_v2'),))
        db.execute("UPDATE metadata SET value=? WHERE key='implementation'", (json.dumps(expected),))
    finished['contract'] = 'cmnews_retained_login_mask_audit_v2'
    log = tmp_path / 'run_v2.log'
    log.write_text(json.dumps(finished) + '\n')
    with pytest.raises(ValueError, match='audit_implementation_changed'):
        readback(root, checkpoint, log)
    assert readback(root, checkpoint, log, implementation_root=archive)['accepted']
    old_scanner.write_bytes(old_scanner.read_bytes() + b'changed')
    with pytest.raises(ValueError, match='audit_implementation_changed'):
        readback(root, checkpoint, log, implementation_root=archive)
    old_scanner.write_bytes(old_scanner.read_bytes()[:-7])
    active = reader.implementation()
    active['files']['downloader/news_fulltext_policy.py'] = '0' * 64
    monkeypatch.setattr(reader, 'implementation', lambda: active)
    with pytest.raises(ValueError, match='audit_implementation_changed'):
        readback(root, checkpoint, log, implementation_root=archive)
