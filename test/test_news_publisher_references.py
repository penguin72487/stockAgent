"""Metadata reference admission, sparse progress and crash recovery; no HTTP."""
from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_references as module
from scripts.audit_news_publisher_aliases import audit_pocket_aliases
from test_news_pocket_original import settings, URL  # noqa: F401 -- shared fixture
from test_news_publisher_alias_audit import fixture, ALIASES, heads


def approval_fixture(tmp_path, settings):
    queue, observed, _ = fixture(tmp_path, settings)
    audit = audit_pocket_aliases(queue.root, queue.registry, observed, sha256_file(observed), list(ALIASES))
    audit_path = tmp_path / 'references.json'
    atomic_write_json(audit_path, audit, durable=True)
    acceptance = {'accepted': True, 'reference_report_sha256': sha256_file(audit_path),
        'formal_queue_alias_admission': False, 'head_writes': 0, 'new_body_records': 0,
        'verified_alias_references': len(ALIASES), 'at_utc': datetime.now(UTC).isoformat()}
    acceptance_path = tmp_path / 'accepted.json'
    atomic_write_json(acceptance_path, acceptance, durable=True)
    entries = []
    for reference in audit['verified_alias_references']:
        a = queue.db.execute('SELECT * FROM articles WHERE url=?', (reference['alias_url'],)).fetchone()
        t = queue.db.execute('SELECT * FROM articles WHERE url=?', (reference['existing_requested_url'],)).fetchone()
        entries.append({'alias_url': a['url'], 'alias_id': a['id'].hex(), 'target_id': t['id'].hex(),
            'target_authorization_fingerprint': t['authorization_fingerprint'],
            'alias_head_guard': {key: a[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS}, 'reference': reference})
    source = queue.registry.sources['pocket']
    approval = {'contract_version': 1, 'scope_kind': 'reviewed_publisher_body_references_v1',
        'provider': 'pocket', 'source_data_fingerprint': source.data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint,
        'publish': False, 'remote_training_export': False, 'entries': entries,
        'approved_at_utc': datetime.now(UTC).isoformat(), 'audit_path': audit_path.name,
        'audit_sha256': sha256_file(audit_path), 'acceptance_path': acceptance_path.name,
        'acceptance_sha256': sha256_file(acceptance_path)}
    path = tmp_path / 'approval.json'
    atomic_write_json(path, approval, durable=True)
    return queue, path, approval


def install(queue, path):
    return module.install_reviewed_references(queue, list(ALIASES), path)


def test_installed_references_have_no_body_head_or_http_mutation_and_are_idempotent(tmp_path, settings, monkeypatch):
    queue, path, approval = approval_fixture(tmp_path, settings)
    try:
        before = heads(queue)
        monkeypatch.setattr('pyarrow.parquet.ParquetFile', lambda *a, **kw: pytest.fail('completed body audit must not be repeated'))
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **kw: pytest.fail('no HTTP'))
        result = install(queue, path)
        assert result['installed_references'] == 2 and result['outcome'] == 'installed'
        assert heads(queue) == before
        status = queue.status(activity='idle')
        assert status['articles_with_stored_body'] == 1
        assert status['providers']['pocket']['body_state_counts']['missing'] == {'unsupported_article_path': 2}
        refs = status['providers']['pocket']['publisher_body_references']
        assert refs['registered_urls'] == refs['verified_reference_urls'] == 2 and refs['inactive_reference_urls'] == 0
        assert refs['new_article_bodies'] == 0 and refs['raw_alias_response_retained'] is False
        assert install(queue, path) == {'installed_references': 0, 'already_recorded': True, 'outcome': 'installed'}
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM publisher_reference_commits').fetchone()[0] == 1
        assert len(list((queue.root / 'publisher_url_references/commits').glob('*.json'))) == 1
        assert all(not p.stat().st_mode & 0o077 for p in (queue.root/'publisher_url_references').rglob('*.json'))
        statements = []
        queue.db.set_trace_callback(statements.append)
        module.progress(queue.db, queue.registry)
        assert not any('FROM articles' in sql and 'WHERE id=' not in sql for sql in statements)
        assert heads(queue) == before
    finally:
        queue.close()


@pytest.mark.parametrize('obstruction', ['alias_refusal', 'alias_saved', 'lease', 'future_due', 'new_http',
    'target_revision', 'target_auth', 'approval_hash', 'source', 'authorization', 'usage_scope',
    'audit_changed', 'acceptance_changed', 'observations_changed', 'body_receipt_changed', 'row_reference_changed'])
def test_unreviewed_changed_or_newer_frontier_never_gets_a_reference(tmp_path, settings, obstruction):
    queue, path, approval = approval_fixture(tmp_path, settings)
    try:
        with queue.db:
            if obstruction == 'alias_refusal': queue.db.execute("UPDATE articles SET state='http_403' WHERE url=?", (ALIASES[0],))
            elif obstruction == 'alias_saved': queue.db.execute("UPDATE articles SET body_sha256='new' WHERE url=?", (ALIASES[0],))
            elif obstruction == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other' WHERE url=?", (ALIASES[0],))
            elif obstruction == 'future_due': queue.db.execute('UPDATE articles SET due=1e12 WHERE url=?', (ALIASES[0],))
            elif obstruction == 'new_http': queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (datetime.now(UTC).isoformat(), ALIASES[0]))
            elif obstruction == 'target_revision': queue.db.execute("UPDATE articles SET body_sha256='new' WHERE url=?", (URL,))
            elif obstruction == 'target_auth': queue.db.execute("UPDATE articles SET authorization_fingerprint='new' WHERE url=?", (URL,))
        if obstruction == 'approval_hash': approval['audit_sha256'] = '0' * 64
        elif obstruction == 'source': approval['source_data_fingerprint'] = '0' * 64
        elif obstruction == 'authorization': approval['authorization_fingerprint'] = '0' * 64
        elif obstruction == 'usage_scope': approval['publish'] = True
        elif obstruction == 'audit_changed': atomic_write_json(tmp_path/approval['audit_path'], {}, durable=True)
        elif obstruction == 'acceptance_changed': atomic_write_json(tmp_path/approval['acceptance_path'], {}, durable=True)
        elif obstruction == 'observations_changed': atomic_write_json(tmp_path/'completed-observations.json', {}, durable=True)
        elif obstruction == 'body_receipt_changed':
            rp = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
            atomic_write_json(queue.root/rp, {}, durable=True)
        elif obstruction == 'row_reference_changed': approval['entries'][0]['reference']['existing_body_sha256'] = '0' * 64
        atomic_write_json(path, approval, durable=True)
        before = heads(queue)
        with pytest.raises(ValueError): install(queue, path)
        assert heads(queue) == before and queue.db.execute('SELECT count(*) FROM publisher_body_references').fetchone()[0] == 0
        assert not list((queue.root/'publisher_url_references/commits').glob('*.json'))
    finally:
        queue.close()


@pytest.mark.parametrize('after_journal', [None, 'refusal', 'new_target', 'new_policy'])
def test_canonical_restart_recovers_exact_journal_or_supersedes_changed_heads(tmp_path, settings, monkeypatch, after_journal):
    queue, path, _ = approval_fixture(tmp_path, settings)
    root, registry = queue.root, queue.registry
    real_apply = module._apply
    monkeypatch.setattr(module, '_apply', lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('crash after journal')))
    with pytest.raises(RuntimeError, match='crash after journal'): install(queue, path)
    assert len(list((root/'publisher_url_references/commits').glob('*.json'))) == 1
    with queue.db:
        if after_journal == 'refusal': queue.db.execute("UPDATE articles SET state='http_403' WHERE url=?", (ALIASES[0],))
        elif after_journal == 'new_target': queue.db.execute("UPDATE articles SET body_sha256='new' WHERE url=?", (URL,))
    if after_journal == 'new_policy':
        registry.sources['pocket'] = replace(registry.sources['pocket'], data_fingerprint='new')
    before = heads(queue)
    queue.close()
    monkeypatch.setattr(module, '_apply', real_apply)
    restored = NewsQueue(root, registry)
    try:
        # Normal owner policy reconciliation is independent; a changed source
        # cannot gain aliases just because its old commit survives on disk.
        outcome = restored.db.execute('SELECT outcome FROM publisher_reference_commits').fetchone()[0]
        assert outcome == ('installed' if after_journal is None else 'superseded_without_head_mutation')
        assert restored.db.execute('SELECT count(*) FROM publisher_body_references').fetchone()[0] == (2 if after_journal is None else 0)
        assert all(r['body_sha256'] == next(b for b in before if b['url'] == r['url'])['body_sha256'] for r in heads(restored))
    finally:
        restored.close()


@pytest.mark.parametrize('lost', ['evidence', 'journal'])
def test_missing_exact_proof_only_deactivates_references_and_restores_without_http(tmp_path, settings, lost):
    queue, path, _ = approval_fixture(tmp_path, settings)
    try:
        install(queue, path)
        if lost == 'journal': broken = next((queue.root/'publisher_url_references/commits').glob('*.json'))
        else: broken = next((queue.root/'publisher_url_references/evidence').glob('*.audit.json'))
        parked = broken.with_suffix('.parked')
        broken.rename(parked)
        before = heads(queue)
        module.initialize(queue)
        refs = module.progress(queue.db, queue.registry)['pocket']
        assert refs['verified_reference_urls'] == 0 and refs['inactive_reference_urls'] == 2
        assert heads(queue) == before
        parked.rename(broken)
        module.initialize(queue)
        assert module.progress(queue.db, queue.registry)['pocket']['verified_reference_urls'] == 2
        assert heads(queue) == before
    finally:
        queue.close()


@pytest.mark.parametrize('change', ['source', 'body', 'captured_clock', 'alias_lease', 'alias_body', 'alias_new_http'])
def test_progress_never_masks_changed_alias_or_target_and_does_not_hash_payloads(tmp_path, settings, monkeypatch, change):
    queue, path, _ = approval_fixture(tmp_path, settings)
    try:
        install(queue, path)
        with queue.db:
            if change == 'body': queue.db.execute("UPDATE articles SET body_sha256='new' WHERE url=?", (URL,))
            elif change == 'captured_clock': queue.db.execute('UPDATE articles SET captured_at_utc=? WHERE url=?', (datetime.now(UTC).isoformat(), URL))
            elif change == 'alias_lease': queue.db.execute("UPDATE articles SET lease_owner='other' WHERE url=?", (ALIASES[0],))
            elif change == 'alias_body': queue.db.execute("UPDATE articles SET body_sha256='new' WHERE url=?", (ALIASES[0],))
            elif change == 'alias_new_http': queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (datetime.now(UTC).isoformat(), ALIASES[0]))
        if change == 'source': queue.registry.sources['pocket'] = replace(queue.registry.sources['pocket'], authorization_fingerprint='other')
        monkeypatch.setattr(module, 'sha256_file', lambda *a: pytest.fail('no heartbeat payload hashing'))
        refs = module.progress(queue.db, queue.registry)['pocket']
        assert refs['verified_reference_urls'] == (1 if change.startswith('alias_') else 0)
        assert refs['verified_reference_urls'] + refs['inactive_reference_urls'] == 2
    finally:
        queue.close()


def test_public_reference_progress_keeps_stored_bodies_separate_and_excludes_private_proofs():
    from stockagent.live.news_dashboard import _publisher_reference_coverage
    raw = {'contract_version': 1, 'registered_urls': 11, 'verified_reference_urls': 11,
        'inactive_reference_urls': 0, 'new_article_bodies': 0, 'raw_alias_response_retained': False,
        'all_history_complete': False, 'private_approval': 'SECRET'}
    coverage = {'missing_body_states': {'unsupported_article_path': 26}}
    public = _publisher_reference_coverage(raw, 254, 228, coverage)
    assert public['stored_body_urls'] == 228 and public['resolved_url_heads'] == 239
    assert public['unresolved_url_heads'] == 15 and public['new_article_bodies'] == 0
    assert public['resolved_url_progress_ratio'] == 239/254 and 'SECRET' not in json.dumps(public)
    for bad in ({'registered_urls': True}, {'verified_reference_urls': 27}, {'inactive_reference_urls': 1},
                {'contract_version': True}, {'new_article_bodies': True}, {'raw_alias_response_retained': True}):
        assert _publisher_reference_coverage({**raw, **bad}, 254, 228, coverage) is None
    assert _publisher_reference_coverage(raw, 254, 228, None) is None


def test_cli_reference_admission_cannot_combine_http_flags_or_construct_a_writer(monkeypatch, tmp_path):
    from scripts.download_news_fulltext import main
    monkeypatch.setattr('scripts.download_news_fulltext.NewsQueue', lambda *a, **kw: pytest.fail('reject before writer'))
    for command, extras in [('fetch', []), ('repair', ['--reason', 'wrong']), ('enqueue', ['--refresh']),
                            ('enqueue', ['--reviewed-native-diagnostics', 'other.json'])]:
        with pytest.raises(SystemExit) as error:
            main([command, '--url', ALIASES[0], '--reviewed-url-references', str(tmp_path/'approved.json'), *extras])
        assert error.value.code == 2


def test_sparse_index_corruption_is_inactive_then_rebuilt_from_same_immutable_journal(tmp_path, settings):
    queue, path, _ = approval_fixture(tmp_path, settings)
    try:
        install(queue, path)
        before = heads(queue)
        with queue.db: queue.db.execute("UPDATE publisher_body_references SET item_json='not-json'")
        assert module.progress(queue.db, queue.registry)['pocket']['inactive_reference_urls'] == 2
        module.initialize(queue)
        assert module.progress(queue.db, queue.registry)['pocket']['verified_reference_urls'] == 2
        with queue.db: queue.db.execute('DELETE FROM publisher_body_references')
        module.initialize(queue)
        assert module.progress(queue.db, queue.registry)['pocket']['verified_reference_urls'] == 2
        assert heads(queue) == before
    finally:
        queue.close()


def test_real_canonical_cli_admits_without_constructing_http_or_replaying_bodies(tmp_path, settings, monkeypatch, capsys):
    from scripts import download_news_fulltext as cli
    from test_news_pocket_linked_figures import current
    queue, path, _ = approval_fixture(tmp_path, settings)
    registry, root, before = queue.registry, queue.root, heads(queue)
    queue.close()
    config = current(settings)
    # This fixture intentionally contains Pocket only, not the production
    # Cnyes diagnostic cohort inherited by the base config.
    config['failure_diagnostic_focus'] = None
    config['failure_diagnostic_metadata_identity_upgrade'] = False
    monkeypatch.setattr(cli, 'load_config', lambda p: (config, registry, tmp_path))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **kw: pytest.fail('no HTTP client'))
    assert cli.main(['enqueue', '--root', str(root), '--reviewed-url-references', str(path),
                     *[part for url in ALIASES for part in ('--url', url)]]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['reviewed_publisher_references']['installed_references'] == 2
    restored = NewsQueue(root, registry)
    try:
        assert heads(restored) == before
        assert module.progress(restored.db, registry)['pocket']['verified_reference_urls'] == 2
    finally:
        restored.close()
