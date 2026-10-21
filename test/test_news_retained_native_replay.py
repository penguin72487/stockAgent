"""Retained native evidence uses the canonical acceptance/recovery owner only."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, UTC
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_metrics import record_workload
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_retained_native import materialize_reviewed_native
from test_news_mitrade_original import URL, page, settings  # noqa: F401 -- shared synthetic fixture


def write_json(path, value):
    atomic_write_json(path, value, durable=True)


def approved(tmp_path, settings, *, urls=(URL,), raw_override=None, original_auth=None,
             provider='mitrade', raw_factory=None):
    # A standalone synthetic provider is not the deployed Cnyes focus cohort.
    settings.pop('failure_diagnostic_focus', None)
    settings['failure_diagnostic_metadata_identity_upgrade'] = False
    settings['retained_cnyes_failure_replay_enabled'] = False
    registry = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path/'news', registry)
    source = registry.sources[provider]
    with queue.db:
        queue.add_articles([{'url': url, 'title': 'Original discovery title'} for url in urls])
    observed = (datetime.now(UTC)-timedelta(seconds=20)).isoformat()
    report_path = tmp_path/'review'/'observation.json'
    entries, probes = [], []
    for url in sorted(urls):
        raw = raw_factory(url=url) if raw_factory else page(url=url).encode()
        article = extract_article(raw, source, page_url=url)
        auth = deepcopy(original_auth or source.authorization)
        if original_auth is None:
            auth['scope'] = 'Three public diagnostic originals only; not archive or training permission.'
        record = {'provider': provider, 'request_url': url, 'final_url': url,
            'raw_response_body': raw, 'response_sha256': hashlib.sha256(raw).hexdigest(),
            'captured_at_utc': observed,
            'headers_json': json.dumps({'content-type': 'text/html; charset=utf-8', 'etag': 'original-tag'}),
            'authorization_json': json.dumps(auth, ensure_ascii=False, sort_keys=True),
            'diagnostic_only': True, 'accepted_as_article': False, 'publish': False}
        if raw_override:
            record.update(raw_override)
        path = report_path.parent/'private-source-diagnostics'/(hashlib.sha256(url.encode()).hexdigest()+'.parquet')
        atomic_write_parquet(path, table_from_records([record]), durable=True)
        receipt_path = path.with_suffix('.receipt.json')
        write_json(receipt_path, {'path': str(path), 'sha256': sha256_file(path),
            'response_sha256': record['response_sha256'], 'diagnostic_only': True,
            'accepted_as_article': False, 'publish': False})
        probes.append({'url': url, 'provider': provider, 'status': 200,
            'response_sha256': record['response_sha256'], 'private_diagnostic_path': str(path)})
        entries.append({'url': url, 'diagnostic_path': str(path.relative_to(report_path.parent)),
            'diagnostic_sha256': sha256_file(path), 'diagnostic_receipt_sha256': sha256_file(receipt_path),
            'response_sha256': record['response_sha256'], 'captured_at_utc': record['captured_at_utc'],
            'original_authorization': auth, **{key: article[key] for key in
                ('body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw')}})
    write_json(report_path, {'probes': probes})
    manifest_path = report_path.parent/'approved.json'
    manifest = {'contract_version': 1, 'scope_kind': 'reviewed_native_diagnostic_materialization_v1',
        'provider': provider, 'data_fingerprint': source.data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint,
        'approved_at_utc': datetime.now(UTC).isoformat(), 'diagnostic_scope_rewording_reviewed': True,
        'publish': False, 'remote_training_export': False, 'observation_report': report_path.name,
        'observation_report_sha256': sha256_file(report_path), 'entries': entries}
    write_json(manifest_path, manifest)
    return queue, registry, manifest_path, manifest


def head(queue, url=URL):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


def replay(queue, manifest, urls=(URL,)):
    return materialize_reviewed_native(queue, urls, manifest, lease_seconds=300)


def cmnews_approval(tmp_path):
    from downloader.news_fulltext import load_config
    from test_news_cmnews_scope import ANNOUNCEMENT_URL, short_announcement_page
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config.pop('provider_catalog', None)
    config['providers'] = [entry for entry in config['providers'] if entry['id'] == 'cmnews']
    return approved(tmp_path, config, urls=(ANNOUNCEMENT_URL,), provider='cmnews',
        raw_factory=lambda **kwargs: short_announcement_page())


def test_discoverable_cmnews_native_original_reuses_same_private_approval_without_http(tmp_path, monkeypatch):
    from test_news_cmnews_scope import ANNOUNCEMENT_URL
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *args, **kwargs: pytest.fail('local replay cannot construct HTTP'))
    queue, registry, path, manifest = cmnews_approval(tmp_path)
    try:
        assert not registry.sources['cmnews'].known_url_only
        result = replay(queue, path, (ANNOUNCEMENT_URL,))
        assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
        current = head(queue, ANNOUNCEMENT_URL)
        assert current['state'] == 'complete'
        saved = pq.read_table(queue.root/current['parquet_path']).to_pylist()[0]
        assert saved['source_observed_at_utc'] == manifest['entries'][0]['captured_at_utc']
        assert saved['acquisition_transport'] == 'retained_raw_reparse'
        assert replay(queue, path, (ANNOUNCEMENT_URL,))['materialized_bodies'] == 0
        assert head(queue, ANNOUNCEMENT_URL) == current
    finally:
        queue.close()


@pytest.mark.parametrize('change', [
    {'provider': 'other'}, {'domains': ('other.invalid',)}, {'embedded_article_layout': 'other'},
    {'selectors': ('body',)}, {'require_configured_container': False}, {'require_author_attribution': False},
])
def test_discoverable_native_replay_is_not_a_generic_provider_or_container_grant(tmp_path, change):
    from test_news_cmnews_scope import ANNOUNCEMENT_URL
    queue, registry, path, manifest = cmnews_approval(tmp_path)
    try:
        registry.sources['cmnews'] = replace(registry.sources['cmnews'], **change)
        before = head(queue, ANNOUNCEMENT_URL)
        with pytest.raises((NewsError, ValueError), match='scope|unapproved'):
            replay(queue, path, (ANNOUNCEMENT_URL,))
        assert head(queue, ANNOUNCEMENT_URL) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    finally:
        queue.close()


def test_native_reuse_zero_http_preserves_original_and_both_review_scopes(settings, tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('local original reuse must never create a network owner')
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', forbidden)
    queue, registry, path, manifest = approved(tmp_path, settings)
    root = queue.root
    try:
        result = replay(queue, path)
        assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
        assert not result['all_publisher_history_complete'] and not result['remote_training_export']
        saved = head(queue)
        assert saved['state'] == 'complete' and saved['attempts'] == 1
        row = pq.read_table(root/saved['parquet_path']).to_pylist()[0]
        entry = manifest['entries'][0]
        assert row['captured_at_utc'] == row['source_observed_at_utc'] == entry['captured_at_utc']
        assert saved['last_checked_at_utc'] == entry['captured_at_utc']
        assert row['reparsed_at_utc'] != entry['captured_at_utc'] and row['http_duration_seconds'] is None
        assert row['etag'] == 'original-tag' and row['historical_point_in_time'] is False
        assert row['requested_url'] == row['final_url'] == URL
        assert record_workload(row) == 'cached_source_reparse'
        assert json.loads(row['original_diagnostic_authorization_json']) == entry['original_authorization']
        assert json.loads(row['authorization_json']) == registry.sources['mitrade'].authorization
        assert row['response_sha256'] == entry['response_sha256']
        assert sha256_file(root/row['replay_source_parquet']) == entry['diagnostic_sha256']
        assert sha256_file(root/row['reviewed_native_approval_path']) == result['scope_sha256']
        assert sha256_file(root/row['reviewed_native_observation_report_path']) == manifest['observation_report_sha256']
        assert replay(queue, path)['materialized_bodies'] == 0 and head(queue) == saved
    finally:
        queue.close()
    queue = NewsQueue(root, registry)
    try:
        assert head(queue)['body_sha256'] == entry['body_sha256']
    finally:
        queue.close()


@pytest.mark.parametrize('failure', ['fingerprint', 'authorization_fingerprint', 'publish', 'export',
    'missing_review', 'bad_report_hash', 'bad_report_path', 'bad_original_hash', 'bad_receipt_hash',
    'bad_extraction', 'bad_author', 'naive_approval', 'future_approval', 'bad_diagnostic_path',
    'not_known_only', 'withdrawn', 'finite', 'missing_frontier', 'missing_report_path'])
def test_invalid_review_never_claims_or_partially_accepts(settings, tmp_path, failure):
    queue, registry, path, manifest = approved(tmp_path, settings)
    try:
        if failure == 'fingerprint': manifest['data_fingerprint'] = 'other'
        elif failure == 'authorization_fingerprint': manifest['authorization_fingerprint'] = 'other'
        elif failure == 'publish': manifest['publish'] = True
        elif failure == 'export': manifest['remote_training_export'] = True
        elif failure == 'missing_review': manifest['diagnostic_scope_rewording_reviewed'] = False
        elif failure == 'bad_report_hash': manifest['observation_report_sha256'] = '0'*64
        elif failure == 'bad_report_path': manifest['observation_report'] = '../observation.json'
        elif failure == 'bad_original_hash': manifest['entries'][0]['diagnostic_sha256'] = '0'*64
        elif failure == 'bad_receipt_hash': manifest['entries'][0]['diagnostic_receipt_sha256'] = '0'*64
        elif failure == 'bad_extraction': manifest['entries'][0]['body_sha256'] = '0'*64
        elif failure == 'bad_author': manifest['entries'][0]['authors_json'] = '["unproved"]'
        elif failure == 'naive_approval': manifest['approved_at_utc'] = '2026-10-01T00:00:00'
        elif failure == 'future_approval': manifest['approved_at_utc'] = (datetime.now(UTC)+timedelta(days=1)).isoformat()
        elif failure == 'bad_diagnostic_path': manifest['entries'][0]['diagnostic_path'] = '../original.parquet'
        elif failure == 'missing_report_path': manifest.pop('observation_report')
        elif failure == 'not_known_only': registry.sources['mitrade'] = replace(registry.sources['mitrade'], known_url_only=False)
        elif failure == 'withdrawn': registry.sources['mitrade'] = replace(registry.sources['mitrade'], allowed=False)
        elif failure == 'finite': registry.sources['mitrade'] = replace(registry.sources['mitrade'], acquisition_mode='finite')
        elif failure == 'missing_frontier':
            with queue.db:
                queue.db.execute('DELETE FROM articles WHERE url=?', (URL,))
        write_json(path, manifest)
        before = queue.db.execute('SELECT state,attempts,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()
        with pytest.raises((ValueError, NewsError)):
            replay(queue, path)
        after = queue.db.execute('SELECT state,attempts,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()
        assert (tuple(after) if after else None) == (tuple(before) if before else None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert not (queue.root/'reviewed_native_sources').exists()
    finally:
        queue.close()


@pytest.mark.parametrize('overrides', [
    {'captured_at_utc': None}, {'captured_at_utc': '2026-10-01T00:00:00'},
    {'captured_at_utc': (datetime.now(UTC)+timedelta(days=1)).isoformat()},
    {'headers_json': '[]'}, {'headers_json': '{"content-type":"application/json"}'},
    {'final_url': URL+'0'}, {'diagnostic_only': False}, {'accepted_as_article': True},
    {'response_sha256': '0'*64}, {'raw_response_body': b'<html>not the approved original</html>'},
])
def test_real_source_transport_fields_are_required_not_invented(settings, tmp_path, overrides):
    queue, _, path, _ = approved(tmp_path, settings, raw_override=overrides)
    try:
        with pytest.raises((ValueError, NewsError)):
            replay(queue, path)
        assert head(queue)['state'] == 'pending' and head(queue)['attempts'] == 0
        assert not (queue.root/'reviewed_native_sources').exists()
    finally:
        queue.close()


def test_scope_wording_review_cannot_change_actual_authorization(settings, tmp_path):
    auth = deepcopy(settings['providers'][0]['authorization'])
    auth['kind'] = 'open_license'
    queue, _, path, _ = approved(tmp_path, settings, original_auth=auth)
    try:
        with pytest.raises(ValueError, match='authorization or HTTP clock'):
            replay(queue, path)
        assert head(queue)['attempts'] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('state,due,checked', [('http_403', 0, None), ('inflight', 0, None),
    ('retry', 9e12, None), ('retry_cooldown', 9e12, None),
    ('retry', 0, 'newer'), ('retry', 0, 'invalid-clock')])
def test_old_response_never_clears_refusal_deadline_lease_or_new_observation(settings, tmp_path, state, due, checked):
    queue, _, path, _ = approved(tmp_path, settings)
    try:
        if checked == 'newer':
            checked = (datetime.now(UTC)-timedelta(seconds=5)).isoformat()
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,due=?,last_checked_at_utc=?,error_code=? WHERE url=?',
                (state, due, checked, 'preserved-evidence', URL))
        before = head(queue)
        result = replay(queue, path)
        assert result['materialized_bodies'] == 0 and result['skipped_urls'] == [URL]
        assert head(queue) == before
    finally:
        queue.close()


def test_entire_batch_validates_before_first_claim(settings, tmp_path):
    other = URL+'0'
    queue, _, path, manifest = approved(tmp_path, settings, urls=(URL, other))
    try:
        manifest['entries'][-1]['body_sha256'] = '0'*64
        write_json(path, manifest)
        with pytest.raises(ValueError, match='extraction changed'):
            replay(queue, path, (URL, other))
        assert all(tuple(row) == ('pending', 0, None) for row in
            queue.db.execute('SELECT state,attempts,body_sha256 FROM articles'))
        assert not (queue.root/'reviewed_native_sources').exists()
    finally:
        queue.close()


@pytest.mark.parametrize('newer', ['none', 'refusal', 'body', 'metadata', 'source_withdrawn'])
def test_crash_after_durable_receipt_recovers_only_unchanged_head(settings, tmp_path, monkeypatch, newer):
    queue, registry, path, manifest = approved(tmp_path, settings)
    root = queue.root
    def fail_commit(*args, **kwargs):
        raise RuntimeError('crash after durable receipt')
    monkeypatch.setattr(queue, '_accept_records', fail_commit)
    try:
        with pytest.raises(RuntimeError, match='crash after durable receipt'):
            replay(queue, path)
        assert len(list((root/'receipts').glob('*.json'))) == 1
        with queue.db:
            if newer == 'refusal':
                queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403',lease_owner=NULL,lease_until=NULL WHERE url=?", (URL,))
            elif newer == 'body':
                queue.db.execute("UPDATE articles SET state='complete',body_sha256='newer-body',lease_owner=NULL,lease_until=NULL WHERE url=?", (URL,))
            elif newer == 'metadata':
                queue.db.execute("UPDATE articles SET title='newer discovery title' WHERE url=?", (URL,))
        if newer == 'source_withdrawn':
            registry.sources['mitrade'] = replace(registry.sources['mitrade'], allowed=False)
    finally:
        queue.close()
    queue = NewsQueue(root, registry)
    try:
        saved = head(queue)
        if newer == 'none':
            assert saved['state'] == 'complete' and saved['body_sha256'] == manifest['entries'][0]['body_sha256']
            assert saved['captured_at_utc'] == manifest['entries'][0]['captured_at_utc']
        elif newer == 'refusal':
            assert saved['state'] == saved['error_code'] == 'http_403' and saved['body_sha256'] is None
        elif newer == 'body':
            assert saved['body_sha256'] == 'newer-body'
        elif newer == 'metadata':
            assert saved['title'] == 'newer discovery title' and saved['body_sha256'] is None
        else:
            assert saved['body_sha256'] is None
    finally:
        queue.close()


def test_exact_missing_head_claim_does_not_consume_attempt_after_metadata_change(settings, tmp_path):
    queue, _, _, _ = approved(tmp_path, settings)
    try:
        witness = {key: head(queue)[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS}
        with queue.db:
            queue.db.execute("UPDATE articles SET title='new metadata' WHERE url=?", (URL,))
        before = head(queue)
        assert queue.claim_url(URL, 300, expected_missing_head=witness) is None
        assert head(queue) == before
        assert queue.claim_url(URL, 300) is not None
        assert head(queue)['attempts'] == 1
    finally:
        queue.close()


def test_claim_update_itself_fences_a_newer_observation_after_select(settings, tmp_path):
    queue, _, _, _ = approved(tmp_path, settings)
    original = queue.db
    witness = {key: head(queue)[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS}
    class InterveningObservation:
        def __enter__(self):
            original.__enter__()
            return self
        def __exit__(self, *args):
            return original.__exit__(*args)
        def execute(self, sql, binds=()):
            if sql.startswith("UPDATE articles SET state='inflight'"):
                original.execute("UPDATE articles SET state='http_403',error_code='http_403' WHERE url=?", (URL,))
            return original.execute(sql, binds)
    try:
        queue.db = InterveningObservation()
        assert queue.claim_url(URL, 300, expected_missing_head=witness) is None
        assert head(queue)['state'] == 'http_403' and head(queue)['attempts'] == 0
    finally:
        queue.db = original
        queue.close()


def test_superseded_head_is_rejected_before_writing_a_body_receipt(settings, tmp_path, monkeypatch):
    queue, _, path, _ = approved(tmp_path, settings)
    accept = queue.accept
    def newer_refusal(records):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403',lease_owner=NULL,lease_until=NULL WHERE url=?", (URL,))
        accept(records)
    monkeypatch.setattr(queue, 'accept', newer_refusal)
    try:
        with pytest.raises(ValueError, match='reviewed_native_head_changed_before_acceptance'):
            replay(queue, path)
        assert head(queue)['state'] == 'http_403' and head(queue)['body_sha256'] is None
        assert not list((queue.root/'receipts').glob('*.json'))
    finally:
        queue.close()


def test_cli_uses_canonical_lifecycle_without_http_or_inventory(settings, tmp_path, monkeypatch, capsys):
    from scripts.download_news_fulltext import main
    queue, registry, path, _ = approved(tmp_path, settings)
    root = queue.root
    queue.close()
    monkeypatch.setattr('scripts.download_news_fulltext.configuration_revision', lambda path: 'fixed-config')
    monkeypatch.setattr('scripts.download_news_fulltext.load_config', lambda path: (settings, registry, tmp_path))
    def forbidden(*args, **kwargs):
        pytest.fail('retained CLI must not invoke HTTP or metadata inventory')
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', forbidden)
    monkeypatch.setattr(NewsQueue, 'ingest_local', forbidden)
    assert main(['fetch', '--root', str(root), '--url', URL, '--reviewed-native-diagnostics', str(path)]) == 0
    report = json.loads(capsys.readouterr().out)['reviewed_native_materialization']
    assert report['materialized_bodies'] == 1 and report['publisher_requests_this_command'] == 0


@pytest.mark.parametrize('argv', [['enqueue', '--url', URL], ['fetch'],
    ['fetch', '--url', URL, '--refresh'], ['fetch', '--url', URL, '--reviewed-supplement', 'other.json'],
    ['fetch', '--url', URL, '--url-file', 'more.txt'], ['run', '--continuous']])
def test_cli_rejects_scope_broadening_and_ambiguous_mutations(argv):
    from scripts.download_news_fulltext import main
    with pytest.raises(SystemExit) as error:
        main([*argv, '--reviewed-native-diagnostics', 'approval.json'])
    assert error.value.code == 2
