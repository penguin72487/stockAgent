"""Only an owner-reviewed exact body version leaves the fulltext corpus."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_repairs as repair
from stockagent.data import news_dataset as corpus
from stockagent.data.news_dataset_sources import freeze_inputs
from test_news_fulltext import article_html, config
from test_news_stored_quality import URL, head, legacy, submit


@pytest.fixture(autouse=True)
def _fixture_disk_admission(monkeypatch):
    class FilesystemSampler:
        def __init__(self, root, **kwargs):
            pass

        def sample(self, filesystem_free):
            return {"state": "not_required", "free_bytes": filesystem_free, "inflight": False}

        def wait(self, timeout):
            raise AssertionError("synthetic corpus must not wait for a backing probe")

    monkeypatch.setattr(corpus, "BackingDiskSampler", FilesystemSampler)


@pytest.fixture
def reviewed(tmp_path, config):
    root = tmp_path / "source"
    root.mkdir()
    registry = PolicyRegistry(config, base=root)
    with closing(NewsQueue(root, registry)) as queue:
        legacy(queue)
        with queue.db:
            queue.db.execute("UPDATE articles SET title='來源標題' WHERE url=?", (URL,))
        saved = head(queue)
        request = submit(queue)
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'stored_quality_reclassified': 1}
        assert head(queue)['state'] == 'paywall_or_login'
    return root, registry, saved, request['request_id']


def _read_snapshot(output, manifest, path):
    generation = output / manifest['generation']
    corpus.copy_snapshot(output, generation, manifest['source_snapshot'], path)
    return sqlite3.connect(path)


def test_owner_review_excludes_exact_body_preserves_source_and_headline(reviewed, tmp_path):
    source, _registry, old, request_id = reviewed
    body_path = source / old['parquet_path']
    before = sha256_file(body_path)
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / 'manifest.json').read_text())
    counts = manifest['source_snapshot']['counts']
    assert manifest['body_quality_scope_contract'] == corpus.BODY_QUALITY_SCOPE_CONTRACT
    assert counts['stored_body_heads'] == counts['reviewed_excluded_body_heads'] == 1
    assert counts['fulltext_articles'] == 0 and counts['reviewed_headline_articles'] == 1
    articles = list(corpus.iter_news_articles(output))
    assert len(articles) == 1 and articles[0]['text_kind'] == 'headline'
    assert articles[0]['title'] == '來源標題' and articles[0]['body'] is None
    assert sha256_file(body_path) == before
    with closing(_read_snapshot(output, manifest, tmp_path / 'readback.sqlite3')) as db:
        original = db.execute('SELECT body_sha256,parquet_path,captured_at_utc FROM heads WHERE url=?', (URL,)).fetchone()
        assert original == (old['body_sha256'], old['parquet_path'], old['captured_at_utc'])
        row = db.execute('SELECT request_id,body_sha256,parquet_path,captured_at_utc,error '
                         'FROM body_quality_exclusions').fetchone()
        assert row == (request_id, old['body_sha256'], old['parquet_path'], old['captured_at_utc'], 'paywall_or_login')
        payload = db.execute('SELECT payload FROM quality_review_requests WHERE request_id=?', (request_id,)).fetchone()[0]
        assert hashlib.sha256(payload).hexdigest() == request_id
    assert corpus.verify_news_dataset(output)['state'] == 'verified'


def test_later_valid_same_text_capture_is_not_rejected_by_old_review(reviewed, tmp_path):
    source, registry, old, _request_id = reviewed
    with closing(NewsQueue(source, registry)) as queue:
        http = SimpleNamespace(registry=registry,
            get=lambda *_a, **_kw: Response(URL, 200, {'content-type': 'text/html'}, article_html()))
        record = fetch_article(http, head(queue))
        assert record['body_sha256'] == old['body_sha256']
        queue.accept([record])
        current = head(queue)
        assert current['parquet_path'] != old['parquet_path']
        assert current['captured_at_utc'] != old['captured_at_utc']
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / 'manifest.json').read_text())
    assert manifest['source_snapshot']['counts']['reviewed_excluded_body_heads'] == 0
    article, = corpus.iter_news_articles(output)
    assert article['text_kind'] == 'fulltext' and article['body'] == record['body']


def test_later_login_state_alone_does_not_remove_an_older_body(tmp_path, config):
    source = tmp_path / 'source'
    source.mkdir()
    registry = PolicyRegistry(config, base=source)
    with closing(NewsQueue(source, registry)) as queue:
        legacy(queue, masked=False)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' "
                             "WHERE url=?", (URL,))
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    article, = corpus.iter_news_articles(output)
    assert article['text_kind'] == 'fulltext' and article['body']
    assert article['queue_state'] == 'paywall_or_login'


@pytest.mark.parametrize('corruption', ['missing', 'changed', 'ledger_index', 'parser', 'body', 'receipt'])
def test_missing_or_inconsistent_review_evidence_fails_closed(reviewed, tmp_path, corruption):
    source, _registry, old, request_id = reviewed
    path = source / 'reviewed_repairs' / 'processed' / (request_id + '.json')
    if corruption == 'missing':
        path.unlink()
    elif corruption == 'changed':
        path.write_bytes(path.read_bytes() + b' ')
    elif corruption == 'body':
        body = source / old['parquet_path']
        body.write_bytes(body.read_bytes() + b'changed')
    elif corruption == 'receipt':
        with closing(sqlite3.connect(source / 'queue.sqlite3')) as db:
            receipt_path = db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?',
                                      (old['parquet_path'],)).fetchone()[0]
        receipt = source / receipt_path
        receipt.write_bytes(receipt.read_bytes() + b' ')
    else:
        with closing(sqlite3.connect(source / 'queue.sqlite3')) as db, db:
            if corruption == 'ledger_index':
                db.execute('UPDATE reviewed_repair_items SET target_index=42 WHERE request_id=?', (request_id,))
            else:
                db.execute('UPDATE reviewed_repair_items SET parser_fingerprint=? WHERE request_id=?',
                           ('0' * 64, request_id))
    with pytest.raises((FileNotFoundError, ValueError)):
        freeze_inputs(source, tmp_path / 'unaccepted')
    assert not (tmp_path / 'unaccepted' / 'snapshot.json').exists()


def test_committed_pending_request_remains_readable_before_archive(reviewed, tmp_path):
    source, _registry, _old, request_id = reviewed
    processed = source / 'reviewed_repairs' / 'processed' / (request_id + '.json')
    pending = source / 'reviewed_repairs' / 'pending' / processed.name
    os.replace(processed, pending)
    result = freeze_inputs(source, tmp_path / 'snapshot')
    assert result['counts']['reviewed_excluded_body_heads'] == 1


def test_request_move_during_read_is_retried_without_losing_evidence(reviewed, tmp_path, monkeypatch):
    source, _registry, _old, request_id = reviewed
    processed = source / 'reviewed_repairs' / 'processed' / (request_id + '.json')
    pending = source / 'reviewed_repairs' / 'pending' / processed.name
    os.replace(processed, pending)
    original = repair._read_private_request
    moved = False
    def racing_read(path, **kwargs):
        nonlocal moved
        if path == pending and not moved:
            os.replace(pending, processed)
            moved = True
            raise FileNotFoundError(path)
        return original(path, **kwargs)
    monkeypatch.setattr(repair, '_read_private_request', racing_read)
    result = freeze_inputs(source, tmp_path / 'snapshot')
    assert moved and result['counts']['reviewed_excluded_body_heads'] == 1


def test_older_scope_requires_new_output_root(reviewed, tmp_path):
    source, _registry, _old, _request_id = reviewed
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    path = output / 'manifest.json'
    manifest = json.loads(path.read_text())
    manifest.pop('body_quality_scope_contract')
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='body quality scope changed'):
        corpus.build_news_dataset(source, output, refresh=True)


def test_older_snapshot_cannot_be_reused_for_new_quality_scope(reviewed, tmp_path):
    source, _registry, _old, _request_id = reviewed
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / 'manifest.json').read_text())
    frozen = output / manifest['generation']
    plan_path = frozen / 'snapshot.json'
    plan = json.loads(plan_path.read_text())
    plan.pop('body_quality_scope_contract')
    plan_path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match='incompatible body quality scope'):
        corpus.build_news_dataset(source, tmp_path / 'new-corpus', snapshot_from=frozen)


def test_reviewed_body_without_title_can_produce_an_empty_corpus(reviewed, tmp_path):
    source, _registry, _old, _request_id = reviewed
    with closing(sqlite3.connect(source / 'queue.sqlite3')) as db, db:
        db.execute('UPDATE articles SET title=NULL WHERE url=?', (URL,))
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / 'manifest.json').read_text())
    assert manifest['source_snapshot']['counts']['reviewed_excluded_body_heads'] == 1
    assert manifest['row_counts'].get('articles', 0) == 0
    assert list(corpus.iter_news_articles(output)) == []
    assert corpus.verify_news_dataset(output)['state'] == 'verified'


def test_render_failure_does_not_move_existing_manifest(reviewed, tmp_path, monkeypatch):
    source, _registry, _old, _request_id = reviewed
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    manifest_path = output / 'manifest.json'
    before = manifest_path.read_bytes()
    def fail_render(*_args, **_kwargs):
        raise RuntimeError('synthetic report failure')
    monkeypatch.setattr(corpus, 'render_news_dataset_readme', fail_render)
    with pytest.raises(RuntimeError, match='synthetic report failure'):
        corpus.build_news_dataset(source, output, refresh=True)
    assert manifest_path.read_bytes() == before


def test_report_write_failure_does_not_move_existing_manifest(reviewed, tmp_path, monkeypatch):
    source, _registry, _old, _request_id = reviewed
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    manifest_path = output / 'manifest.json'
    before = manifest_path.read_bytes()
    original = corpus.atomic_write_text
    def fail_readme(path, *args, **kwargs):
        if Path(path) == output / 'README.md':
            raise OSError('synthetic report write failure')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(corpus, 'atomic_write_text', fail_readme)
    with pytest.raises(OSError, match='synthetic report write failure'):
        corpus.build_news_dataset(source, output, refresh=True)
    assert manifest_path.read_bytes() == before


def test_interrupted_publication_readme_links_to_its_own_generation(reviewed, tmp_path, monkeypatch):
    source, _registry, _old, _request_id = reviewed
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    before = (output / 'manifest.json').read_bytes()
    previous = json.loads(before)['generation']
    original = corpus.atomic_write_json
    def interrupt_pointer(path, *args, **kwargs):
        if Path(path) == output / 'manifest.json':
            raise OSError('synthetic pointer interruption')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(corpus, 'atomic_write_json', interrupt_pointer)
    with pytest.raises(OSError, match='pointer interruption'):
        corpus.build_news_dataset(source, output, refresh=True)
    assert (output / 'manifest.json').read_bytes() == before
    candidate = json.loads((output / '.active-build.json').read_text())['generation']
    assert candidate != previous
    # The old root pointer and the newer root convenience receipt are different.
    assert json.loads((output / 'verification.json').read_text())['generation'] == candidate
    readme = (output / 'README.md').read_text()
    for name in ('manifest.json', 'verification.json', 'quality_report.json', 'schema.json'):
        assert f']({candidate}/{name})' in readme
        assert (output / candidate / name).is_file()
    assert '根目錄目前指標' in readme
    old_proof = json.loads((output / previous / 'verification.json').read_text())
    assert old_proof['generation'] == previous
    monkeypatch.setattr(corpus, 'atomic_write_json', original)
    recovered = corpus.finish_pending_news_dataset(output)
    assert recovered['generation'] == candidate
    assert json.loads((output / 'manifest.json').read_text())['generation'] == candidate
    assert json.loads((output / 'verification.json').read_text())['generation'] == candidate
    assert not (output / '.active-build.json').exists()
    assert json.loads((output / previous / 'verification.json').read_text()) == old_proof


@pytest.fixture
def old_corpus_with_review(tmp_path, config):
    """The source review happened after an earlier fulltext corpus was built."""
    source = tmp_path / 'source'
    source.mkdir()
    registry = PolicyRegistry(config, base=source)
    with closing(NewsQueue(source, registry)) as queue:
        legacy(queue)
    output = tmp_path / 'corpus'
    corpus.build_news_dataset(source, output)
    with closing(NewsQueue(source, registry)) as queue:
        result = submit(queue)
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'stored_quality_reclassified': 1}
    from scripts.audit_news_corpus_reviewed_versions import collect_targets
    with closing(sqlite3.connect(source / 'queue.sqlite3')) as db:
        db.row_factory = sqlite3.Row
        targets = collect_targets(source, db, registry, [])
    assert result['request_id'] and len(targets) == 1
    return output, targets


def test_reviewed_version_audit_finds_exact_body_in_old_corpus(old_corpus_with_review):
    from scripts.audit_news_corpus_reviewed_versions import scan_pinned_corpus
    output, targets = old_corpus_with_review
    manifest = json.loads((output / 'manifest.json').read_text())
    audit = scan_pinned_corpus(output, manifest, targets)
    assert audit['confirmed_exact_versions'] == 1
    assert audit['confirmed_by_origin'] == {'owner_admitted': 1}
    assert audit['positive_part_checksums_verified'] == 1
    assert not audit['all_corpus_parts_sha_verified'] and not audit['negative_matches_prove_absence']


def test_reviewed_version_audit_does_not_apply_old_capture_to_other_version(old_corpus_with_review):
    from scripts.audit_news_corpus_reviewed_versions import scan_pinned_corpus
    output, targets = old_corpus_with_review
    manifest = json.loads((output / 'manifest.json').read_text())
    changed = [{**targets[0], 'captured_at_utc': '2026-01-02T00:00:00+00:00'}]
    audit = scan_pinned_corpus(output, manifest, changed)
    assert audit['confirmed_exact_versions'] == 0 and audit['unconfirmed_targets'] == 1
    assert not audit['negative_matches_prove_absence']


@pytest.mark.parametrize('change', ['source_checksum', 'body_checksum', 'part_checksum', 'duplicate'])
def test_reviewed_version_audit_rejects_inconsistent_positive_evidence(old_corpus_with_review, change):
    from scripts.audit_news_corpus_reviewed_versions import scan_pinned_corpus
    output, targets = old_corpus_with_review
    manifest = json.loads((output / 'manifest.json').read_text())
    if change == 'source_checksum':
        targets[0]['source_sha256'] = '0' * 64
    elif change == 'body_checksum':
        targets[0]['body_sha256'] = '0' * 64
    elif change == 'part_checksum':
        next(part for part in manifest['parts'] if part['table_name'] == 'articles')['sha256'] = '0' * 64
    else:
        targets.append(dict(targets[0]))
    with pytest.raises(ValueError):
        scan_pinned_corpus(output, manifest, targets)


def test_reviewed_version_audit_can_revalidate_unsubmitted_private_stage(tmp_path, config):
    from downloader.news_fulltext_stored_quality import prepare_stored_quality_reviews
    from scripts.audit_news_corpus_reviewed_versions import collect_targets
    source = tmp_path / 'source'
    source.mkdir()
    registry = PolicyRegistry(config, base=source)
    with closing(NewsQueue(source, registry)) as queue:
        legacy(queue)
        prepared = prepare_stored_quality_reviews(source, registry, [URL], reason='test private stage',
                                                   expected_error='paywall_or_login')
    stage = tmp_path / 'stage'
    stage.mkdir(mode=0o700)
    (stage / (prepared['request_id'] + '.json')).write_bytes(prepared['payload'])
    (stage / (prepared['request_id'] + '.json')).chmod(0o600)
    (stage / 'manifest.json').write_text(json.dumps({'submitted': False, 'staged': [
        {'request_id': prepared['request_id'], 'targets': 1}]}))
    with closing(sqlite3.connect(source / 'queue.sqlite3')) as db:
        db.row_factory = sqlite3.Row
        targets = collect_targets(source, db, registry, [stage])
    assert len(targets) == 1 and targets[0]['origin'] == 'private_staged'
    assert not (source / 'reviewed_repairs' / 'pending').exists()


def test_reviewed_document_audit_finds_exact_deduplicated_text(old_corpus_with_review):
    from scripts.audit_news_corpus_reviewed_versions import scan_pinned_corpus, scan_pinned_documents
    output, targets = old_corpus_with_review
    manifest = json.loads((output / 'manifest.json').read_text())
    article = scan_pinned_corpus(output, manifest, targets)
    # Multiple article references to identical text still name one document.
    documents = scan_pinned_documents(output, manifest, article['matches'] * 2)
    assert documents['targets'] == documents['confirmed_document_ids'] == 1
    assert documents['unconfirmed_document_ids'] == 0
    assert documents['positive_part_checksums_verified'] == 1
    assert not documents['all_corpus_parts_sha_verified'] and not documents['negative_matches_prove_absence']


def test_reviewed_document_audit_does_not_claim_unseen_text_absent(old_corpus_with_review):
    from scripts.audit_news_corpus_reviewed_versions import scan_pinned_documents
    output, _targets = old_corpus_with_review
    manifest = json.loads((output / 'manifest.json').read_text())
    result = scan_pinned_documents(output, manifest, [{'document_id': 'body-' + '0' * 64}])
    assert result['confirmed_document_ids'] == 0 and result['unconfirmed_document_ids'] == 1
    assert not result['negative_matches_prove_absence']


@pytest.mark.parametrize('change', ['part_checksum', 'document_text', 'duplicate_part', 'invalid_identity'])
def test_reviewed_document_audit_rejects_inconsistent_evidence(old_corpus_with_review, change):
    from scripts.audit_news_corpus_reviewed_versions import scan_pinned_corpus, scan_pinned_documents
    output, targets = old_corpus_with_review
    manifest = json.loads((output / 'manifest.json').read_text())
    article = scan_pinned_corpus(output, manifest, targets)
    part = next(part for part in manifest['parts'] if part['table_name'] == 'documents')
    if change == 'part_checksum':
        part['sha256'] = '0' * 64
    elif change == 'document_text':
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = output / part['path']
        table = pq.read_table(path)
        index = table.schema.get_field_index('text')
        table = table.set_column(index, 'text', pa.array(['different text'] * table.num_rows))
        pq.write_table(table, path)
        part['bytes'], part['sha256'] = path.stat().st_size, sha256_file(path)
    elif change == 'duplicate_part':
        manifest['parts'].append(dict(part))
    else:
        article['matches'][0]['document_id'] = 'body-' + 'x' * 64
    with pytest.raises(ValueError):
        scan_pinned_documents(output, manifest, article['matches'])
