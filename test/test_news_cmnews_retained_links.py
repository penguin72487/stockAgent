"""Retained link discovery must remain a witnessed, resumable local hint."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import audit_news_cmnews_retained_links as auditor
from scripts import readback_news_cmnews_retained_links as reader
from scripts import stage_news_cmnews_retained_links as stager
from scripts.audit_news_provider_storage_readback import stamp
from downloader.news_fulltext_url_metadata import _load, submit_urls
from downloader import news_fulltext_retained_handoff as handoff
from test_news_cmnews_retained_access_audit import example, page


@pytest.fixture
def link_example(example):
    root, reference, readback, checkpoint = example
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source:
        known = source.execute('SELECT url FROM articles ORDER BY url LIMIT 1').fetchone()[0]
    with closing(sqlite3.connect(reference)) as witness, witness:
        for path, receipt in witness.execute('SELECT path,receipt_path FROM jobs ORDER BY path').fetchall():
            records = pq.read_table(root / path).to_pylist()
            batch = path.split('/')[-1].split('.')[0]
            for rec in records:
                raw = page(rec['requested_url'])
                anchors = (f'<a href="/article/new-{batch}">new</a>'
                           f'<a href="/article/shared">shared</a>'
                           f'<a href="{known}">known</a>'
                           f'<a href="/tag/tag-{batch}">tag</a>').encode()
                raw = raw.replace(b'</body>', anchors + b'</body>')
                rec['raw_response_body'] = raw
                rec['response_sha256'] = hashlib.sha256(raw).hexdigest()
            pq.write_table(pa.Table.from_pylist(records), root / path)
            payload = (root / path).read_bytes()
            (root / receipt).write_text(json.dumps({'parquet_path': path, 'sha256': hashlib.sha256(payload).hexdigest()}))
            witness.execute('UPDATE jobs SET body_stamp=?,receipt_stamp=? WHERE path=?',
                            (json.dumps(stamp(root / path)), json.dumps(stamp(root / receipt)), path))
    return root, reference, readback, checkpoint


def test_links_are_exact_same_host_hints_only():
    base = 'https://cmnews.com.tw/article/parent'
    raw = b'''<html><body><a href="/article/new-1">yes</a>
      <a href="https://cmnews.com.tw/tag/market">tag</a>
      <a href="/article/new-1?x=1">query</a><a href="/article/new-1#part">fragment</a>
      <a href="http://cmnews.com.tw/article/no">http</a><a href="//evil.test/article/no">other</a>
      <a href="/onlynotecontent/new-1">other route</a><script>"/article/not-anchor"</script>
    </body></html>'''
    assert auditor.links_from_original(raw, base) == {
        'https://cmnews.com.tw/article/new-1': 'article',
        'https://cmnews.com.tw/tag/market': 'tag'}
    assert auditor.local_url(base, 'https://user@cmnews.com.tw/article/x') is None
    assert auditor.local_url(base, '/article/' + 'a' * 9000) is None


def test_resume_keeps_original_witnesses_and_deduplicates_targets(link_example, monkeypatch):
    root, reference, readback, checkpoint = link_example
    first = auditor.audit(root, reference, readback, checkpoint, max_batches=1)
    assert not first['completed_scope'] and first['processed_batches'] == 1
    result = auditor.audit(root, reference, readback, checkpoint)
    assert result['accepted'] and result['processed_heads'] == 4 and result['failed_batches'] == 0
    assert result['target_counts'] == {'article_unregistered': 3, 'article_known_with_body': 1,
                                       'tag_unregistered': 2}
    assert result['publisher_requests'] == result['production_queue_writes'] == 0
    with sqlite3.connect(checkpoint) as db:
        shared = db.execute("SELECT batches,parent_url,response_sha256 FROM targets WHERE url=?",
                            ('https://cmnews.com.tw/article/shared',)).fetchone()
        assert shared[0] == 2 and shared[1].startswith('https://cmnews.com.tw/article/')
        assert len(shared[2]) == 64
    monkeypatch.setattr(auditor, 'inspect_batch', lambda *a, **k: pytest.fail('successful batch was replayed'))
    again = auditor.audit(root, reference, readback, checkpoint)
    assert again['accepted'] and again['target_counts'] == result['target_counts']


def test_changed_sample_artifact_is_not_reused(link_example):
    root, reference, readback, checkpoint = link_example
    auditor.audit(root, reference, readback, checkpoint, max_batches=1)
    with (root / 'batches/0.parquet').open('ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='checkpoint_artifact_changed'):
        auditor.audit(root, reference, readback, checkpoint)


def test_failed_batch_does_not_make_scope_complete_and_can_retry(link_example, monkeypatch):
    root, reference, readback, checkpoint = link_example
    original = auditor.inspect_batch
    def fail_first(root, source, job, rows):
        if job[0].endswith('0.parquet'):
            raise OSError('temporary local read failure')
        return original(root, source, job, rows)
    monkeypatch.setattr(auditor, 'inspect_batch', fail_first)
    failed = auditor.audit(root, reference, readback, checkpoint)
    assert failed['completed_scope'] and not failed['accepted'] and failed['failed_batches'] == 1
    monkeypatch.setattr(auditor, 'inspect_batch', original)
    final = auditor.audit(root, reference, readback, checkpoint)
    assert final['accepted'] and final['failed_batches'] == 0 and final['target_counts']['article_unregistered'] == 3


def test_checkpoint_rejects_changed_reference(link_example):
    root, reference, readback, checkpoint = link_example
    auditor.audit(root, reference, readback, checkpoint, max_batches=1)
    readback.write_text(readback.read_text() + ' ')
    with pytest.raises(ValueError, match='checkpoint_identity_changed'):
        auditor.audit(root, reference, readback, checkpoint)


def completed_inventory(link_example, tmp_path):
    root, reference, report, checkpoint = link_example
    result = auditor.audit(root, reference, report, checkpoint)
    log = tmp_path / 'completed.log'
    log.write_text(json.dumps(result) + '\n')
    return root, checkpoint, log


def test_independent_readback_checks_every_new_target_and_existing_inbox(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    source = SimpleNamespace(provider='cmnews', allowed=True, data_fingerprint='data',
                             authorization_fingerprint='authorization')
    registry = SimpleNamespace(match=lambda _: source)
    submit_urls(root, registry, ['https://cmnews.com.tw/article/new-0'])
    result = reader.readback(root, checkpoint, log)
    assert result['accepted'] and result['unregistered_article_witnesses_verified'] == 3
    assert result['pending_inbox_overlap'] == 1 and result['unregistered_not_in_pending'] == 2
    assert result['publisher_requests'] == result['production_queue_writes'] == 0
    assert not result['target_identity_or_body_verified']


def test_independent_readback_rejects_changed_projection(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    with closing(sqlite3.connect(checkpoint)) as db, db:
        db.execute("UPDATE targets SET parent_url='https://cmnews.com.tw/article/wrong' "
                   "WHERE url='https://cmnews.com.tw/article/new-0'")
    with pytest.raises(ValueError, match='target_witness_projection_mismatch'):
        reader.readback(root, checkpoint, log)


def test_independent_readback_replays_anchor_instead_of_trusting_checkpoint(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    before = 'https://cmnews.com.tw/article/new-0'
    after = 'https://cmnews.com.tw/article/forged'
    with closing(sqlite3.connect(checkpoint)) as db, db:
        db.execute('UPDATE links SET target_url=? WHERE target_url=?', (after, before))
        db.execute('UPDATE targets SET url=? WHERE url=?', (after, before))
    with pytest.raises(ValueError, match='target_not_in_independently_parsed_original'):
        reader.readback(root, checkpoint, log)


def test_independent_readback_rejects_changed_receipt_and_unsafe_inbox(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    (root / 'reviewed_repairs').symlink_to(tmp_path / 'nonexistent')
    with pytest.raises(ValueError, match='unsafe_url_metadata_inbox'):
        reader.readback(root, checkpoint, log)
    (root / 'reviewed_repairs').unlink()
    with (root / 'receipts/0.json').open('a') as receipt:
        receipt.write(' ')
    with pytest.raises(ValueError, match='witness_batch_artifact_changed'):
        reader.readback(root, checkpoint, log)


def test_private_stage_uses_owner_request_format_and_rechecks_pending(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    result = reader.readback(root, checkpoint, log)
    result_path = tmp_path / 'independent.json'
    result_path.write_text(json.dumps(result))
    source = SimpleNamespace(provider='cmnews', allowed=True, data_fingerprint='data',
                             authorization_fingerprint='authorization',
                             article_path_pattern=r'^/article/[A-Za-z0-9_-]+$')
    registry = SimpleNamespace(match=lambda _: source)
    # Another offline request arrived after readback; it must not be restaged.
    submit_urls(root, registry, ['https://cmnews.com.tw/article/new-0'])
    source_before = stamp(root / 'queue.sqlite3')
    dest = tmp_path / 'private_stage'
    manifest = stager.stage(root, checkpoint, log, result_path, dest, tmp_path, registry)
    assert manifest['accepted'] and manifest['staged_candidates'] == 2
    assert manifest['already_in_owner_inbox'] == 1 and manifest['owner_admission'] is False
    assert manifest['publisher_requests'] == manifest['production_queue_writes'] == 0
    requests = [_load(dest / item['path']) for item in manifest['requests']]
    assert {target['url'] for request in requests for target in request['targets']} == {
        'https://cmnews.com.tw/article/new-1', 'https://cmnews.com.tw/article/shared'}
    assert stamp(root / 'queue.sqlite3') == source_before
    assert json.loads((dest / 'manifest.json').read_text()) == manifest
    with pytest.raises(ValueError, match='private_stage_must_be_new'):
        stager.stage(root, checkpoint, log, result_path, dest, tmp_path, registry)


def test_private_stage_refuses_changed_source_or_policy(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    result_path = tmp_path / 'independent.json'
    result_path.write_text(json.dumps(reader.readback(root, checkpoint, log)))
    source = SimpleNamespace(provider='cmnews', allowed=False, data_fingerprint='data',
                             authorization_fingerprint='authorization', article_path_pattern=r'^/article/.*$')
    registry = SimpleNamespace(match=lambda _: source)
    destination = tmp_path / 'private_stage'
    with pytest.raises(ValueError, match='target_outside_current_source_scope'):
        stager.stage(root, checkpoint, log, result_path, destination, tmp_path, registry)
    assert not destination.exists()
    with closing(sqlite3.connect(checkpoint)) as db, db:
        db.execute("UPDATE targets SET parent_url='changed' WHERE url='https://cmnews.com.tw/article/new-0'")
    with pytest.raises(ValueError, match='readback_inputs_changed'):
        stager.stage(root, checkpoint, log, result_path, destination, tmp_path, registry)
    with pytest.raises(ValueError, match='private_stage_outside_source_required'):
        stager.stage(root, checkpoint, log, result_path, root / 'bad', tmp_path, registry)


def sealed_handoff(link_example, tmp_path):
    root, checkpoint, log = completed_inventory(link_example, tmp_path)
    result_path = tmp_path / 'independent.json'
    result_path.write_text(json.dumps(reader.readback(root, checkpoint, log)))
    os.chmod(result_path, 0o600)
    source = SimpleNamespace(provider='cmnews', allowed=True, data_fingerprint='data',
                             authorization_fingerprint='authorization',
                             article_path_pattern=r'^/article/[A-Za-z0-9_-]+$')
    registry = SimpleNamespace(match=lambda _: source)
    stage = tmp_path / 'private_stage'
    stager.stage(root, checkpoint, log, result_path, stage, tmp_path, registry)
    os.chmod(stage / 'manifest.json', 0o600)
    certificate = tmp_path / 'certificate.json'
    result = stager.seal_handoff(root, checkpoint, log, result_path, stage, tmp_path, registry, certificate)
    assert result['accepted'] and result['candidates'] == 3 and result['publisher_requests'] == 0
    db = sqlite3.connect(root / 'queue.sqlite3')
    queue = SimpleNamespace(root=root, registry=registry, db=db)
    spec = {'path': str(certificate), 'sha256': result['certificate_sha256'], 'artifacts': str(tmp_path)}
    return queue, spec, source


def test_handoff_rechecks_scoped_source_and_policy_after_unrelated_database_writes(link_example, tmp_path):
    queue, spec, source = sealed_handoff(link_example, tmp_path)
    try:
        cert, requests = handoff._validate(queue, spec)
        assert cert['candidate_count'] == 3 and len(requests) == 1
        # The owner may have legitimately changed an unrelated queue head.
        with queue.db:
            queue.db.execute("UPDATE articles SET state='deferred' WHERE id NOT IN (" +
                             ','.join('?' for _ in cert['parents']) + ')',
                             tuple(bytes.fromhex(key) for key in cert['parents']))
        assert handoff._validate(queue, spec)[0]['candidate_count'] == 3
        source.authorization_fingerprint = 'changed'
        with pytest.raises(ValueError, match='handoff_target_or_policy_changed'):
            handoff._validate(queue, spec)
        source.authorization_fingerprint = 'authorization'
        parent = next(iter(cert['parents']))
        with queue.db:
            queue.db.execute("UPDATE articles SET body_sha256='changed' WHERE id=?", (bytes.fromhex(parent),))
        with pytest.raises(ValueError, match='handoff_parent_head_changed'):
            handoff._validate(queue, spec)
    finally:
        queue.db.close()


def test_handoff_retry_is_idempotent_and_keeps_body_unverified(link_example, tmp_path, monkeypatch):
    queue, spec, _ = sealed_handoff(link_example, tmp_path)
    try:
        write = handoff._write_once
        calls = []
        def transient(path, payload):
            calls.append(path)
            if len(calls) == 1:
                raise OSError('transient local error')
            write(path, payload)
        monkeypatch.setattr(handoff, '_write_once', transient)
        failed = handoff.pump(queue, spec, now=0)
        assert failed['state'] == 'deferred' and failed['reason'] == 'local_input_unavailable'
        assert handoff.pump(queue, spec, now=1) == failed
        submitted = handoff.pump(queue, spec, now=2)
        assert submitted['state'] == 'submitted' and submitted['candidates'] == 3
        assert submitted['submitted_requests'] == 1 and not submitted['body_verified']
        inbox = queue.root / 'reviewed_repairs/url_metadata/pending'
        files = list(inbox.glob('*.json'))
        assert len(files) == 1 and len(_load(files[0])['targets']) == 3
        files[0].unlink()  # Simulate a lost mailbox file in this isolated fixture only.
        assert handoff.pump(queue, spec, now=13)['state'] == 'submitted'
        assert files[0].exists()
        # A restarted owner sees the same content-addressed request.
        del queue._retained_link_handoff_status
        again = handoff.pump(queue, spec, now=14)
        assert again['state'] == 'submitted' and list(inbox.glob('*.json')) == files
    finally:
        queue.db.close()


def test_handoff_defers_changed_original_or_certificate_without_writing_inbox(link_example, tmp_path):
    queue, spec, _ = sealed_handoff(link_example, tmp_path)
    try:
        with (queue.root / 'batches/0.parquet').open('ab') as stream:
            stream.write(b'changed')
        result = handoff.pump(queue, spec, now=0)
        assert result['state'] == 'deferred' and result['reason'] == 'handoff_batch_changed'
        assert not list((queue.root / 'reviewed_repairs/url_metadata/pending').glob('*.json'))
    finally:
        queue.db.close()


def test_handoff_waits_for_inbox_capacity_and_retries_without_restarting(link_example, tmp_path):
    queue, spec, _ = sealed_handoff(link_example, tmp_path)
    try:
        inbox = handoff._directory(queue.root, 'url_metadata', 'pending')
        for index in range(handoff.MAX_PENDING):
            (inbox / f'{index:064x}.json').write_text('{}')
        waiting = handoff.pump(queue, spec, now=0)
        assert waiting['state'] == 'waiting_inbox' and waiting['reason'] == 'url_metadata_inbox_full'
        assert handoff.pump(queue, spec, now=1) == waiting
        next(inbox.glob('*.json')).unlink()
        assert handoff.pump(queue, spec, now=2)['state'] == 'submitted'
        assert len(list(inbox.glob('*.json'))) == handoff.MAX_PENDING
    finally:
        queue.db.close()


def test_handoff_restarts_through_real_owner_and_preserves_existing_head(link_example, tmp_path, monkeypatch):
    from collections import Counter
    from downloader.artifact_io import atomic_write_bytes
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_queue import NewsQueue
    from downloader.news_fulltext_url_metadata import consume_urls, has_pending

    original, old_spec, _ = sealed_handoff(link_example, tmp_path)
    cert = json.loads(Path(old_spec['path']).read_bytes())
    old_requests = [_load(Path(item['path'])) for item in cert['requests']]
    urls = [target['url'] for request in old_requests for target in request['targets']]
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    owner_root = tmp_path / 'isolated_owner'
    owner = NewsQueue(owner_root, registry)
    try:
        with owner.db:
            for parent in cert['parents'].values():
                owner.add_articles([{'url': parent[0]}])
                owner.db.execute('UPDATE articles SET provider=?,body_sha256=?,body_bytes=?,captured_at_utc=?,parquet_path=? WHERE url=?',
                                 (parent[1], parent[2], parent[3], parent[4], parent[5], parent[0]))
            owner.add_articles([{'url': urls[0]}])
            owner.db.execute("UPDATE articles SET state='robots_disallowed',attempts=7,due=12345 WHERE url=?", (urls[0],))
            for path, item in cert['batches'].items():
                for name in (path, item['receipt_path']):
                    target = owner_root / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(original.root / name, target)
                owner.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (path, item['receipt_path']))
                item['body_stamp'] = handoff._source_stamp(owner_root, path)
                item['receipt_stamp'] = handoff._source_stamp(owner_root, item['receipt_path'])
        before = dict(owner.db.execute('SELECT * FROM articles WHERE url=?', (urls[0],)).fetchone())
        staged = []
        for index, group in enumerate((urls[:2], urls[2:])):
            stage = tmp_path / f'owner_stage_{index}'
            stage.mkdir(mode=0o700)
            receipt = submit_urls(stage, registry, group)
            staged.append({'path': receipt['pending_path'], 'sha256': receipt['request_id'], 'targets': len(group)})
        cert['source_root'] = str(owner_root.resolve())
        cert['source_identity'] = handoff._source_identity(registry, urls[0])
        cert['requests'] = staged
        cert_path = tmp_path / 'isolated_certificate.json'
        raw = json.dumps(cert, sort_keys=True).encode()
        atomic_write_bytes(cert_path, raw, mode=0o600)
        spec = {'path': str(cert_path), 'sha256': hashlib.sha256(raw).hexdigest(), 'artifacts': str(tmp_path)}
        write, calls = handoff._write_once, []
        def interrupt_second(path, payload):
            calls.append(path)
            if len(calls) == 2:
                raise OSError('simulated power interruption')
            write(path, payload)
        monkeypatch.setattr(handoff, '_write_once', interrupt_second)
        assert handoff.pump(owner, spec, now=0)['state'] == 'deferred'
        assert len(list((owner_root / 'reviewed_repairs/url_metadata/pending').glob('*.json'))) == 1
        owner.close()
        owner = NewsQueue(owner_root, registry)
        monkeypatch.setattr(handoff, '_write_once', write)
        resumed = handoff.pump(owner, spec, now=61)
        assert resumed['state'] == 'submitted' and resumed['submitted_requests'] == 2
        outcomes = Counter()
        while has_pending(owner):
            result = consume_urls(owner, limit=512, max_seconds=.1)
            assert result['processed'] > 0
            outcomes.update(result['outcomes'])
        assert outcomes == {'metadata_registered': 2, 'existing_head_preserved': 1}
        assert dict(owner.db.execute('SELECT * FROM articles WHERE url=?', (urls[0],)).fetchone()) == before
        completed = owner_root / 'reviewed_repairs/url_metadata/completed'
        assert len(list(completed.glob('*.receipt.json'))) == 2
        assert all(not json.loads(p.read_bytes())['body_acceptance_proven']
                   for p in completed.glob('*.receipt.json'))
        owner.close()
        owner = NewsQueue(owner_root, registry)
        checked = handoff.pump(owner, spec, now=120)
        assert checked['state'] == 'owner_checked' and checked['outcomes'] == dict(outcomes)
        assert not list((owner_root / 'reviewed_repairs/url_metadata/pending').glob('*.json'))
    finally:
        original.db.close()
        owner.close()
