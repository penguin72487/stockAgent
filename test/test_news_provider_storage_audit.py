from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_provider_storage import audit
from scripts.benchmark_cnyes_acquisition import verify_batches
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


def stored_fixture(config, root, *, two_batches=True):
    urls = ['https://publisher.test/a', 'https://publisher.test/b']
    responses = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')}
    responses.update({url: FakeResponse(article_html()) for url in urls})
    http, session = http_fixture(config, root, responses)
    queue = NewsQueue(root, http.registry)
    try:
        queue.add_articles([{'url': url} for url in urls])
        records = [fetch_article(http, queue.claim('publisher', 300)) for _ in urls]
        if two_batches:
            for record in records:
                queue.accept([record])
        else:
            queue.accept(records)
        paths = [row[0] for row in queue.db.execute('SELECT path FROM accepted_batches ORDER BY path')]
        calls = len(session.calls)
    finally:
        queue.close()
        http.close()
    return urls, paths, calls


def mixed_provider_fixture(config, root):
    other = deepcopy(config['providers'][0])
    other.update({'id': 'other', 'domains': ['other.test']})
    config['providers'].append(other)
    urls = ['https://publisher.test/a', 'https://other.test/b', 'https://other.test/c']
    responses = {f'https://{domain}/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')
                 for domain in ('publisher.test', 'other.test')}
    responses.update({url: FakeResponse(article_html()) for url in urls})
    http, _ = http_fixture(config, root, responses)
    queue = NewsQueue(root, http.registry)
    try:
        queue.add_articles([{'url': url} for url in urls])
        first = fetch_article(http, queue.claim('publisher', 300))
        second = fetch_article(http, queue.claim('other', 300))
        third = fetch_article(http, queue.claim('other', 300))
        queue.accept([first, second])
        queue.accept([third])
    finally:
        queue.close()
        http.close()
    return urls


def reference_audit(root, state, log):
    result = audit(root, 'publisher', state)
    assert result['accepted']
    log.write_text(json.dumps(result) + '\n')
    return {'reference_state': state, 'reference_log': log,
            'reference_implementation_root': Path.cwd()}


def test_provider_audit_reuses_proven_batches_and_verifies_other_heads(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module
    from scripts.audit_news_provider_storage_readback import readback

    root = tmp_path / 'source'
    mixed_provider_fixture(config, root)
    reference = reference_audit(root, tmp_path / 'reference.sqlite3', tmp_path / 'reference.log')
    checked = []
    original = module.verify_batches

    def tracking(queue, paths, **kwargs):
        checked.extend(paths)
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', tracking)
    result = audit(root, 'other', tmp_path / 'target.sqlite3', **reference)
    assert result['accepted'] and result['verified_stored_heads'] == 2
    assert result['reused_previously_verified_batches'] == result['newly_verified_batches'] == 1
    assert len(checked) == 1
    assert result['reference_audit']['provider'] == 'publisher'
    assert result['publisher_requests'] == result['production_queue_writes'] == 0
    log = tmp_path / 'target.log'
    log.write_text(json.dumps(result) + '\n')
    check = readback(root, tmp_path / 'target.sqlite3', log)
    assert check['accepted'] and check['provider_head_pointer_consistency']['accepted']
    assert check['reference_audit']['reused_previously_verified_batches'] == 1
    assert check['reference_audit']['newly_verified_batches'] == 1
    result['reused_previously_verified_batches'] += 1
    log.write_text(json.dumps(result) + '\n')
    with pytest.raises(ValueError, match='reference_batch_count_mismatch'):
        readback(root, tmp_path / 'target.sqlite3', log)


def test_provider_audit_reference_resumes_without_repeating_success(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    root = tmp_path / 'source'
    mixed_provider_fixture(config, root)
    reference = reference_audit(root, tmp_path / 'reference.sqlite3', tmp_path / 'reference.log')
    checked = []
    original = module.verify_batches

    def tracking(queue, paths, **kwargs):
        checked.extend(paths)
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', tracking)
    state = tmp_path / 'target.sqlite3'
    partial = audit(root, 'other', state, max_batches=1, **reference)
    assert not partial['completed_scope']
    result = audit(root, 'other', state, **reference)
    assert result['accepted'] and result['verified_stored_heads'] == 2
    assert result['reused_previously_verified_batches'] == result['newly_verified_batches'] == 1
    assert len(checked) == 1


def test_provider_audit_rejects_other_provider_head_disagreement(config, tmp_path):
    root = tmp_path / 'source'
    urls = mixed_provider_fixture(config, root)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET provider=? WHERE url=?', ('incorrect', urls[1]))
    reference = reference_audit(root, tmp_path / 'reference.sqlite3', tmp_path / 'reference.log')
    result = audit(root, 'incorrect', tmp_path / 'target.sqlite3', **reference)
    assert result['completed_scope'] and not result['accepted']
    assert result['failed_batches'] == 1
    assert 'selected_head_provider_mismatch' in result['errors'][0]['error']


def test_provider_audit_rejects_other_provider_digest_disagreement(config, tmp_path):
    root = tmp_path / 'source'
    urls = mixed_provider_fixture(config, root)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('0' * 64, urls[1]))
    reference = reference_audit(root, tmp_path / 'reference.sqlite3', tmp_path / 'reference.log')
    result = audit(root, 'other', tmp_path / 'target.sqlite3', **reference)
    assert result['completed_scope'] and not result['accepted']
    assert result['failed_batches'] == 1
    assert 'selected_head_digest_mismatch' in result['errors'][0]['error']


def test_provider_audit_cannot_reuse_unfinished_or_changed_reference(config, tmp_path):
    root = tmp_path / 'source'
    _, paths, _ = stored_fixture(config, root)
    state, log = tmp_path / 'reference.sqlite3', tmp_path / 'reference.log'
    partial = audit(root, 'publisher', state, max_batches=1)
    log.write_text(json.dumps(partial) + '\n')
    options = {'reference_state': state, 'reference_log': log,
               'reference_implementation_root': Path.cwd()}
    with pytest.raises(ValueError, match='unfinished_audit'):
        audit(root, 'publisher', tmp_path / 'target.sqlite3', **options)
    complete = audit(root, 'publisher', state)
    log.write_text(json.dumps(complete) + '\n')
    (root / paths[0]).write_bytes(b'changed')
    with pytest.raises(ValueError, match='verified_artifact_changed'):
        audit(root, 'publisher', tmp_path / 'target.sqlite3', **options)


def test_provider_audit_reference_readback_rejects_modified_evidence(config, tmp_path):
    from scripts.audit_news_provider_storage_readback import readback

    root = tmp_path / 'source'
    mixed_provider_fixture(config, root)
    reference = reference_audit(root, tmp_path / 'reference.sqlite3', tmp_path / 'reference.log')
    state, log = tmp_path / 'target.sqlite3', tmp_path / 'target.log'
    result = audit(root, 'other', state, **reference)
    log.write_text(json.dumps(result) + '\n')
    Path(reference['reference_log']).write_text(Path(reference['reference_log']).read_text() + '\n')
    with pytest.raises(ValueError, match='reference_audit_changed'):
        readback(root, state, log)


def test_provider_audit_resumes_without_repeating_verified_batches(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, calls = stored_fixture(config, root)
    before = hashlib.sha256((root / 'queue.sqlite3').read_bytes()).hexdigest()
    checked = []
    original = module.verify_batches

    def tracking(queue, paths, **kwargs):
        checked.extend(paths)
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', tracking)
    first = audit(root, 'publisher', state, max_batches=1)
    assert not first['completed_scope'] and not first['accepted']
    assert first['stored_heads_in_snapshot'] == 2 and first['target_batches'] == 2
    assert first['processed_batches'] == first['verified_stored_heads'] == 1
    final = audit(root, 'publisher', state)
    assert final['completed_scope'] and final['accepted']
    assert final['scope_membership_verified']
    assert final['verified_stored_heads'] == final['stored_heads_in_snapshot'] == 2
    assert final['verified_head_metadata']['declared_publication_clocks'] == 2
    assert final['verified_head_metadata']['nonempty_author_lists'] == 2
    assert final['publisher_requests'] == final['production_queue_writes'] == 0
    assert final['publisher_history_complete'] is None and final['current_parser_replay_verified'] is None
    assert final['historical_completeness_verified'] is False
    assert checked == paths
    assert hashlib.sha256((root / 'queue.sqlite3').read_bytes()).hexdigest() == before


def test_provider_audit_checks_a_shared_batch_once(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root, two_batches=False)
    checked = []
    original = module.verify_batches

    def tracking(queue, paths, **kwargs):
        checked.append((paths, len(kwargs['head_ids'])))
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', tracking)
    result = audit(root, 'publisher', state)
    assert result['accepted'] and result['verified_stored_heads'] == 2
    assert result['target_batches'] == 1 and checked == [(paths, 2)]


def test_provider_audit_reuses_private_records_only_when_explicit(config, tmp_path):
    root = tmp_path / 'source'
    urls, paths, _ = stored_fixture(config, root, two_batches=False)
    with closing(sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        queue = SimpleNamespace(root=root, db=db)
        ordinary = verify_batches(queue, paths)
        assert 'private_verified_records' not in ordinary
        proof = verify_batches(queue, paths, include_records=True)
        assert len(proof['private_verified_records']) == 2
        with pytest.raises(ValueError, match='private_records_require_one_batch'):
            verify_batches(queue, [], include_records=True)


def test_provider_audit_counts_iso_and_mail_timezones_without_inventing_naive_values():
    from scripts.audit_news_provider_storage import _selected_metadata

    ids = [bytes([index]) * 32 for index in (1, 2, 3, 4)]
    clocks = ['2020-05-01T10:00:00Z', 'Fri, 01 May 2020 12:00:00 +0200',
              '2020-05-01T10:00:00', '0001-01-01T00:00:00+14:00']
    heads = [(key, f'https://publisher.test/{index}', 10, '2026-01-01T00:00:00+00:00')
             for index, key in enumerate(ids)]
    records = [{'article_id': key.hex(), 'requested_url': url, 'body_bytes': size,
                'captured_at_utc': captured, 'published_at_raw': clock, 'authors_json': '["author"]'}
               for (key, url, size, captured), clock in zip(heads, clocks)]
    value = _selected_metadata(records, heads)
    assert value['declared_publication_clocks'] == 4
    assert value['aware_iso_publication_clocks'] == value['aware_mail_publication_clocks'] == 1
    assert value['first_aware_utc'] == value['last_aware_utc'] == '2020-05-01T10:00:00+00:00'


def test_provider_audit_rejects_head_metadata_mismatch(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET body_bytes=body_bytes+1 WHERE url=?', (urls[0],))
    result = audit(root, 'publisher', state)
    assert result['completed_scope'] and not result['accepted']
    assert result['failed_batches'] == 1
    assert 'selected_head_metadata_mismatch' in result['errors'][0]['error']


def test_provider_audit_continues_after_bad_batch_without_claiming_completion(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    (root / paths[0]).write_bytes(b'corrupted parquet')
    result = audit(root, 'publisher', state)
    assert result['completed_scope'] and not result['accepted']
    assert result['processed_batches'] == 2 and result['failed_batches'] == 1
    assert result['verified_stored_heads'] == 1
    assert result['errors'][0]['path'] == paths[0]
    assert 'mismatch' in result['errors'][0]['error']


def test_provider_audit_rechecks_repaired_batch_but_not_unchanged_success(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    original_bytes = (root / paths[0]).read_bytes()
    (root / paths[0]).write_bytes(b'corrupt')
    first = audit(root, 'publisher', state)
    assert first['failed_batches'] == 1
    (root / paths[0]).write_bytes(original_bytes)
    original = module.verify_batches
    checked = []

    def tracking(queue, paths, **kwargs):
        checked.extend(paths)
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', tracking)
    result = audit(root, 'publisher', state)
    assert result['accepted'] and result['verified_stored_heads'] == 2
    assert checked == [paths[0]]


def test_provider_audit_does_not_repeat_an_unchanged_failed_batch(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    (root / paths[0]).write_bytes(b'corrupt')
    assert audit(root, 'publisher', state)['failed_batches'] == 1
    monkeypatch.setattr(module, 'verify_batches', lambda *args, **kwargs: pytest.fail('unchanged batch was retried'))
    repeated = audit(root, 'publisher', state)
    assert repeated['completed_scope'] and not repeated['accepted'] and repeated['failed_batches'] == 1


def test_provider_audit_recovers_after_interruption_at_checkpoint(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls = [f'https://publisher.test/story-{i}' for i in range(33)]
    responses = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')}
    responses.update({url: FakeResponse(article_html()) for url in urls})
    http, _ = http_fixture(config, root, responses)
    queue = NewsQueue(root, http.registry)
    try:
        queue.add_articles([{'url': url} for url in urls])
        for _ in urls:
            queue.accept([fetch_article(http, queue.claim('publisher', 300))])
    finally:
        queue.close()
        http.close()
    original, checked = module.verify_batches, []

    def interrupted(queue, paths, **kwargs):
        checked.extend(paths)
        if len(checked) == 33:
            raise KeyboardInterrupt()
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', interrupted)
    with pytest.raises(KeyboardInterrupt):
        audit(root, 'publisher', state)
    with closing(sqlite3.connect(state)) as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0] == 32
    resumed = []

    def tracking(queue, paths, **kwargs):
        resumed.extend(paths)
        return original(queue, paths, **kwargs)

    monkeypatch.setattr(module, 'verify_batches', tracking)
    result = audit(root, 'publisher', state)
    assert result['accepted'] and result['verified_stored_heads'] == 33
    assert resumed == checked[-1:]


def test_provider_audit_rejects_changed_source_snapshot_on_resume(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    assert not audit(root, 'publisher', state, max_batches=1)['completed_scope']
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET due=due+1 WHERE url=?', (urls[1],))
    with pytest.raises(ValueError, match='audit_checkpoint_identity_changed'):
        audit(root, 'publisher', state)


def test_provider_audit_pins_verifier_dependencies_and_rejects_changed_identity(config, tmp_path, monkeypatch):
    from scripts import audit_news_provider_storage as module

    identity = module._identity()
    assert {'downloader/news_fulltext_policy.py', 'downloader/news_receipt_io.py',
            'stockagent/private_json.py'} <= set(identity['files'])
    assert identity['beautifulsoup4'] and identity['lxml']
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    stored_fixture(config, root)
    audit(root, 'publisher', state, max_batches=1)
    changed = deepcopy(identity)
    changed['files']['downloader/news_fulltext_policy.py'] = '0' * 64
    monkeypatch.setattr(module, '_identity', lambda: changed)
    with pytest.raises(ValueError, match='audit_checkpoint_identity_changed'):
        audit(root, 'publisher', state)


def test_provider_audit_allows_recreated_empty_wal(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    stored_fixture(config, root)
    wal = root / 'queue.sqlite3-wal'
    wal.touch()
    audit(root, 'publisher', state, max_batches=1)
    wal.unlink()
    wal.touch()
    result = audit(root, 'publisher', state)
    assert result['accepted'] and result['scope_membership_verified']


def test_provider_audit_rejects_tampered_checkpoint_membership(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    stored_fixture(config, root)
    audit(root, 'publisher', state, max_batches=1)
    with closing(sqlite3.connect(state)) as db:
        with db:
            db.execute("UPDATE jobs SET selected_ids_sha256=?", ('0' * 64,))
    with pytest.raises(ValueError, match='audit_checkpoint_scope_mismatch'):
        audit(root, 'publisher', state)


def test_provider_audit_rechecks_changed_verified_artifact_on_resume(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    audit(root, 'publisher', state, max_batches=1)
    (root / paths[0]).write_bytes(b'changed after checkpoint')
    result = audit(root, 'publisher', state)
    assert result['completed_scope'] and not result['accepted']
    assert result['failed_batches'] == 1 and result['verified_stored_heads'] == 1


def test_provider_audit_reports_missing_body_path_without_inventing_success(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, paths, _ = stored_fixture(config, root)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET parquet_path=NULL WHERE url=?', (urls[0],))
    result = audit(root, 'publisher', state)
    assert result['completed_scope'] and not result['accepted']
    assert result['stored_heads_in_snapshot'] == 2 and result['missing_batch_path_heads'] == 1
    assert result['verified_stored_heads'] == 1


def test_provider_audit_rejects_state_inside_source_and_empty_scope(config, tmp_path):
    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    stored_fixture(config, root)
    with pytest.raises(ValueError, match='audit_state_must_be_outside_news_root'):
        audit(root, 'publisher', root / 'unsafe.sqlite3')
    result = audit(root, 'another-provider', state)
    assert result['completed_scope'] and not result['accepted'] and result['stored_heads_in_snapshot'] == 0


def test_independent_provider_readback_reconciles_membership_and_detects_tampering(config, tmp_path):
    path = Path('scripts/audit_news_provider_storage_readback.py')
    spec = importlib.util.spec_from_file_location('news_provider_readback_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root, state, log = tmp_path / 'source', tmp_path / 'audit.sqlite3', tmp_path / 'audit.log'
    stored_fixture(config, root)
    result = audit(root, 'publisher', state)
    log.write_text(json.dumps(result) + '\n')
    readback = module.readback(root, state, log)
    assert readback['accepted'] and readback['counts']['verified_stored_heads'] == 2
    assert readback['counts']['failed_batches'] == 0
    assert readback['provider_head_pointer_consistency']['accepted']
    assert readback['verified_head_metadata'] == result['verified_head_metadata']
    with closing(sqlite3.connect(state)) as db:
        with db:
            db.execute('UPDATE jobs SET selected_ids_sha256=?', ('0' * 64,))
    with pytest.raises(ValueError, match='scope_group_identity_mismatch'):
        module.readback(root, state, log)


def test_independent_provider_readback_reports_path_without_body_digest(config, tmp_path):
    path = Path('scripts/audit_news_provider_storage_readback.py')
    spec = importlib.util.spec_from_file_location('news_provider_readback_pointer', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root, state, log = tmp_path / 'source', tmp_path / 'audit.sqlite3', tmp_path / 'audit.log'
    urls, _, _ = stored_fixture(config, root)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET body_sha256=NULL WHERE url=?', (urls[0],))
    result = audit(root, 'publisher', state)
    assert result['accepted'] and result['verified_stored_heads'] == 1
    log.write_text(json.dumps(result) + '\n')
    readback = module.readback(root, state, log)
    consistency = readback['provider_head_pointer_consistency']
    assert consistency['known_heads'] == 2 and consistency['path_without_body_sha256'] == 1
    assert consistency['complete_heads_missing_body_pointer'] == 1 and not consistency['accepted']
    output = tmp_path / 'readback.json'
    assert module.main([str(root), str(state), str(log), str(output)]) == 1
    assert not json.loads(output.read_text())['provider_head_pointer_consistency']['accepted']


def test_independent_provider_readback_can_use_archived_verifier_files(config, tmp_path):
    path = Path('scripts/audit_news_provider_storage_readback.py')
    spec = importlib.util.spec_from_file_location('news_provider_readback_archive', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root, state, log, archive = (tmp_path / 'source', tmp_path / 'audit.sqlite3',
                                 tmp_path / 'audit.log', tmp_path / 'implementation')
    stored_fixture(config, root)
    result = audit(root, 'publisher', state)
    log.write_text(json.dumps(result) + '\n')
    for name in result['implementation']['files']:
        target = archive / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(Path(name).read_bytes())
    assert module.readback(root, state, log, implementation_root=archive)['accepted']
    (archive / 'downloader/artifact_io.py').write_bytes(b'altered verifier')
    with pytest.raises(ValueError, match='implementation_changed'):
        module.readback(root, state, log, implementation_root=archive)


def test_independent_provider_readback_accepts_legacy_iso_only_metadata(config, tmp_path):
    path = Path('scripts/audit_news_provider_storage_readback.py')
    spec = importlib.util.spec_from_file_location('news_provider_readback_v1', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root, state, log = tmp_path / 'source', tmp_path / 'audit.sqlite3', tmp_path / 'audit.log'
    stored_fixture(config, root)
    result = audit(root, 'publisher', state)
    result['contract_version'] = 1
    result['verified_head_metadata'].pop('aware_mail_publication_clocks')
    with closing(sqlite3.connect(state)) as db:
        with db:
            db.execute("UPDATE metadata SET value='1' WHERE key='contract_version'")
            for path, raw in db.execute('SELECT path,metadata_json FROM jobs').fetchall():
                metadata = json.loads(raw)
                metadata.pop('aware_mail_publication_clocks')
                db.execute('UPDATE jobs SET metadata_json=? WHERE path=?', (json.dumps(metadata), path))
    log.write_text(json.dumps(result) + '\n')
    assert module.readback(root, state, log)['accepted']


@pytest.mark.parametrize(('raw', 'expected'), [
    ('2020-05-01', 'iso_date_only'),
    ('2020-05-01T10:00:00', 'iso_naive'),
    ('2020-05-01T10:00:00Z', 'iso_aware'),
    ('Fri, 01 May 2020 10:00:00 +0000', 'mail_date_aware'),
    ('Fri, 01 May 2020 10:00:00', 'mail_date_naive'),
    ('2026-10-08 11:34 (更新：2026-10-08 11:36)', 'combined_naive_publication_and_update'),
    ('2026-10-08 11:34 (更新: 2026-10-08 11:33)', 'combined_naive_update_before_publication'),
    ('2026-02-30 11:34 (更新：2026-10-08 11:36)', 'combined_naive_invalid'),
    ('0001-01-01T00:00:00+14:00', 'other'),
    ('not a clock', 'other'),
    (None, 'missing_or_nonstring'),
])
def test_clock_sample_classifies_formats_without_inventing_timezone(raw, expected):
    from scripts.audit_news_provider_clock_formats import classify_clock
    assert classify_clock(raw) == expected


def test_clock_sample_reuses_verified_batches_and_rejects_changed_artifacts(config, tmp_path):
    from scripts.audit_news_provider_clock_formats import audit as sample_clocks

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    _, paths, _ = stored_fixture(config, root)
    audit(root, 'publisher', state)
    sample = sample_clocks(root, state)
    assert sample['sampled_heads'] == 2 and sample['clock_formats']['iso_date_only'] == 2
    assert not sample['representative_of_provider'] and not sample['changes_source_times']
    first = sample_clocks(root, state, limit=1)
    tail = sample_clocks(root, state, limit=1, skip_batches=1)
    assert first['sampled_batches'] + tail['sampled_batches'] == sample['eligible_batches'] == 2
    assert first['sampled_heads'] + tail['sampled_heads'] == sample['eligible_heads'] == 2
    with pytest.raises(ValueError, match='batch_offset_outside_scope'):
        sample_clocks(root, state, skip_batches=3)
    (root / paths[0]).write_bytes(b'changed after verification')
    with pytest.raises(ValueError, match='verified_artifact_changed'):
        sample_clocks(root, state)


def test_provider_batch_overlap_requires_completed_unchanged_audit(config, tmp_path):
    from scripts.audit_news_provider_batch_overlap import inspect

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, _, _ = stored_fixture(config, root)
    audit(root, 'publisher', state, max_batches=1)
    with pytest.raises(ValueError, match='completed_source_audit_required'):
        inspect(root, state, 'publisher')
    audit(root, 'publisher', state)
    result = inspect(root, state, 'publisher')
    assert result['known_stored_heads'] == result['heads_in_previously_verified_batches'] == 2
    assert result['distinct_batches'] == result['previously_verified_batches'] == 2
    assert result['additional_batches'] == 0
    empty = inspect(root, state, 'another-provider')
    assert empty['known_stored_heads'] == empty['additional_batches'] == 0
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET due=due+1 WHERE url=?', (urls[0],))
    with pytest.raises(ValueError, match='completed_source_audit_required'):
        inspect(root, state, 'publisher')


def test_provider_batch_overlap_is_only_a_hint_for_other_heads(config, tmp_path):
    from scripts.audit_news_provider_batch_overlap import inspect

    root, state = tmp_path / 'source', tmp_path / 'audit.sqlite3'
    urls, _, _ = stored_fixture(config, root, two_batches=False)
    # Deliberately make the other head's provider disagree with its retained row.
    # Batch overlap must not be presented as verifying that head's identity.
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as db:
        with db:
            db.execute('UPDATE articles SET provider=? WHERE url=?', ('another-provider', urls[0]))
    assert audit(root, 'publisher', state)['accepted']
    result = inspect(root, state, 'another-provider')
    assert result['heads_in_previously_verified_batches'] == result['previously_verified_batches'] == 1
    assert 'current head pointers not yet verified' in result['scope']


@pytest.mark.parametrize('mutation', ['partial_report', 'report_metadata', 'artifact_replaced', 'state_journal'])
def test_independent_provider_readback_rejects_incomplete_or_changed_evidence(config, tmp_path, mutation):
    path = Path('scripts/audit_news_provider_storage_readback.py')
    spec = importlib.util.spec_from_file_location('news_provider_readback_more', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root, state, log = tmp_path / 'source', tmp_path / 'audit.sqlite3', tmp_path / 'audit.log'
    _, paths, _ = stored_fixture(config, root)
    result = audit(root, 'publisher', state, max_batches=1 if mutation == 'partial_report' else None)
    if mutation == 'report_metadata':
        result['verified_head_metadata']['nonempty_author_lists'] += 1
    elif mutation == 'artifact_replaced':
        artifact = root / paths[0]
        contents = artifact.read_bytes()
        artifact.unlink()
        artifact.write_bytes(contents)
    elif mutation == 'state_journal':
        Path(str(state) + '-journal').write_bytes(b'uncommitted audit state')
    log.write_text(json.dumps(result) + '\n')
    expected = {'partial_report': 'unfinished_audit',
                'report_metadata': 'acceptance_or_metadata_mismatch',
                'artifact_replaced': 'verified_artifact_changed',
                'state_journal': 'audit_state_not_quiescent'}[mutation]
    with pytest.raises(ValueError, match=expected):
        module.readback(root, state, log)
