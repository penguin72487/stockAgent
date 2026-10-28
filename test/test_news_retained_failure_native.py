"""Same-attempt failure originals retain receive and failure-commit clocks."""
from copy import deepcopy
from datetime import datetime, timedelta, UTC
import json

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_contentplatform as native
from downloader.artifact_io import atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS as FIELDS
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_retained_native import _article_digest
from test_news_contentplatform import URL
from test_news_contentplatform_written_geometry import entry, wrapped
from test_news_retained_native_replay import approved, head as stored_head, replay, write_json
from downloader.news_fulltext import load_config
from pathlib import Path


def head(queue):
    return stored_head(queue, URL)


def approval(tmp_path, monkeypatch):
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config.pop('provider_catalog', None)
    config['providers'] = [entry()]
    queue, registry, old_path, manifest = approved(tmp_path, config, urls=(URL,), provider='contentplatform',
        raw_factory=lambda **_: wrapped(), original_auth=deepcopy(entry()['authorization']))
    source = registry.sources['contentplatform']
    monkeypatch.setattr(native, 'CONTENTPLATFORM_V5_ORIGINAL_BINDING',
        (source.data_fingerprint, source.authorization_fingerprint, source.parser_repair_prior_capture_fingerprint))
    item = manifest['entries'][0]
    old = old_path.parent/item['diagnostic_path']
    record, = pq.ParquetFile(old).read().to_pylist()
    observed = datetime.fromisoformat(record['captured_at_utc'])
    before = {key: head(queue)[key] for key in FIELDS}
    before.update(state='article_identity_not_verified', error_code='article_identity_not_verified', attempts=3,
        last_checked_at_utc=(observed-timedelta(days=1)).isoformat())
    checked = (observed+timedelta(milliseconds=150)).isoformat()
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=4,last_checked_at_utc=? WHERE url=?", (checked, URL))
    failed = {key: head(queue)[key] for key in FIELDS}
    record.update(contract_version=1, scope_kind='contentplatform_native_failure_original_v1', owner_id='a'*32,
        error_code='article_identity_not_verified', data_fingerprint=source.parser_repair_prior_capture_fingerprint,
        response_bytes=len(record['raw_response_body']), raw_response_retained=True, remote_training_export=False,
        native_private_permit={'contract_version': 1, 'literal_native_identity_verified': True,
            'native_publisher_byline_and_explicit_clocks_verified': True,
            'body_geometry_accepted': False, 'article_accepted': False})
    root = queue.root/'failure_responses'
    directory = root/observed.strftime('%Y%m%d')/'contentplatform'
    directory.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    original = directory/'123456789012-1234abcd.parquet'
    atomic_write_parquet(original, table_from_records([record]), durable=True)
    original.chmod(0o600)
    receipt = original.with_suffix('.receipt.json')
    write_json(receipt, {key: value for key, value in record.items() if key != 'raw_response_body'} |
        {'path': str(original.relative_to(queue.root)), 'sha256': sha256_file(original)})
    receipt.chmod(0o600)
    path = queue.root/'failure-approved.json'
    report_path = queue.root/'failure-observation.json'
    write_json(report_path, {'probes': [{'url': URL, 'provider': 'contentplatform', 'status': 200,
        'response_sha256': record['response_sha256'], 'private_diagnostic_path': str(original)}]})
    from downloader.news_fulltext_policy import extract_article
    article = extract_article(record['raw_response_body'], source, page_url=URL)
    item.update(diagnostic_path=str(original.relative_to(queue.root)), diagnostic_sha256=sha256_file(original),
        diagnostic_receipt_sha256=sha256_file(receipt), original_owner_id=record['owner_id'],
        before_failed_attempt_head=before, expected_missing_head=failed,
        article_keys=sorted(article), article_fields_sha256=_article_digest(article))
    manifest.update(contract_version=5, scope_kind='reviewed_canonical_failure_materialization_v5',
        diagnostic_scope_unchanged=True, parser_transition_reviewed=True, same_failed_attempt_reviewed=True,
        diagnostic_directory='failure_responses', observation_report=report_path.name,
        observation_report_sha256=sha256_file(report_path))
    write_json(path, manifest)
    return queue, registry, path, manifest, original, record


def test_same_attempt_reuses_original_without_HTTP_or_clock_backdating(tmp_path, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('retained canonical original must not create HTTP'))
    queue, registry, path, manifest, original, raw = approval(tmp_path, monkeypatch)
    original_sha = sha256_file(original)
    failed = head(queue)
    try:
        result = replay(queue, path, (URL,))
        assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
        saved = head(queue)
        assert saved['last_checked_at_utc'] == failed['last_checked_at_utc'] > saved['captured_at_utc']
        assert saved['attempts'] == failed['attempts']+1
        record, = pq.ParquetFile(queue.root/saved['parquet_path']).read().to_pylist()
        assert record['captured_at_utc'] == record['source_observed_at_utc'] == raw['captured_at_utc']
        assert record['replay_source_parquet'] == str(original.relative_to(queue.root))
        assert not record['original_transport_redirects_stored']
        assert 'transport_redirects_json' not in record
        assert sha256_file(original) == original_sha
        assert replay(queue, path, (URL,))['materialized_bodies'] == 0
        assert not list((queue.root/'reviewed_native_sources').glob('*.parquet'))
    finally:
        queue.close()


@pytest.mark.parametrize('change', ['attempt', 'receive_clock', 'before_clock', 'unreviewed', 'authorization',
    'binding', 'raw_pin', 'owner', 'field_digest', 'http_403', 'future_due', 'new_check', 'leased'])
def test_canonical_failure_reuse_rejects_changed_evidence_or_newer_heads(tmp_path, monkeypatch, change):
    queue, registry, path, manifest, original, raw = approval(tmp_path, monkeypatch)
    item = manifest['entries'][0]
    if change == 'attempt':
        item['before_failed_attempt_head']['attempts'] += 1
    elif change == 'receive_clock':
        item['captured_at_utc'] = datetime.now(UTC).isoformat()
    elif change == 'before_clock':
        item['before_failed_attempt_head']['last_checked_at_utc'] = item['expected_missing_head']['last_checked_at_utc']
    elif change == 'unreviewed':
        manifest['same_failed_attempt_reviewed'] = False
    elif change == 'authorization':
        item['original_authorization']['scope'] += ' different scope'
    elif change == 'binding':
        monkeypatch.setattr(native, 'CONTENTPLATFORM_V5_ORIGINAL_BINDING', ('0'*64,)*3)
    elif change == 'raw_pin':
        item['diagnostic_sha256'] = '0'*64
    elif change == 'owner':
        item['original_owner_id'] = 'b'*32
    elif change == 'field_digest':
        item['article_fields_sha256'] = '0'*64
    else:
        sql, value = {
            'http_403': ("state='http_403',error_code='http_403'", None),
            'future_due': ('due=?', datetime.now(UTC).timestamp()+86400),
            'new_check': ('last_checked_at_utc=?,attempts=attempts+1', datetime.now(UTC).isoformat()),
            'leased': ("lease_owner='new-owner',lease_until=?", datetime.now(UTC).timestamp()+60),
        }[change]
        with queue.db:
            queue.db.execute('UPDATE articles SET '+sql+' WHERE url=?', (URL,) if value is None else (value, URL))
    write_json(path, manifest)
    before = head(queue)
    try:
        if change in {'http_403', 'future_due', 'new_check', 'leased'}:
            assert replay(queue, path, (URL,))['materialized_bodies'] == 0
        else:
            with pytest.raises(ValueError):
                replay(queue, path, (URL,))
        assert head(queue) == before
        assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    finally:
        queue.close()


@pytest.mark.parametrize('newer_refusal', [False, True])
def test_failure_original_durable_recovery_preserves_clocks_and_newer_refusals(tmp_path, monkeypatch, newer_refusal):
    queue, registry, path, manifest, original, raw = approval(tmp_path, monkeypatch)
    checked, root = head(queue)['last_checked_at_utc'], queue.root
    monkeypatch.setattr(queue, '_accept_records', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('durable crash')))
    try:
        with pytest.raises(RuntimeError, match='durable crash'):
            replay(queue, path, (URL,))
        if newer_refusal:
            with queue.db:
                queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403',lease_owner=NULL,lease_until=NULL WHERE url=?", (URL,))
    finally:
        queue.close()
    queue = NewsQueue(root, registry)
    try:
        saved = head(queue)
        if newer_refusal:
            assert saved['state'] == 'http_403' and saved['body_sha256'] is None
        else:
            assert saved['state'] == 'complete'
            assert saved['last_checked_at_utc'] == checked
            assert saved['captured_at_utc'] == raw['captured_at_utc']
    finally:
        queue.close()
