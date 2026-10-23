"""Empty envelopes are retryable; accepted bodies and refusal evidence survive."""
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext import _require_article_response, extract_article_response, fetch_article
from downloader.news_fulltext_diagnostics import FailureResponse
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metrics import ARTICLE_RESPONSE_ENVELOPE_CONTRACT_VERSION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_repairs as repairs
from test_cnyes_news_bulk import cfg as config
from test_news_cnyes_html import URL, short_page


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    yield value
    value.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def saved_body(queue):
    with queue.db:
        queue.add_articles([{'url': URL}])
    http = SimpleNamespace(registry=queue.registry, get=lambda *a, **k:
        Response(URL, 200, {'content-type': 'text/html', 'etag': '"saved"'}, short_page()))
    queue.accept([fetch_article(http, head(queue))])
    return head(queue)


def legacy_empty_failure(queue):
    saved_body(queue)
    claimed = queue.claim_url(URL, 300, refresh=True)
    source = queue.registry.sources['cnyes']
    error = NewsError('body_not_found')
    error.failure_response = FailureResponse(Response(URL, 200, {'content-type': 'text/html'}, b''),
        'cnyes', source.data_fingerprint, URL, datetime.now(timezone.utc).isoformat())
    queue.failed(claimed, error, 6)  # Synthetic legacy observation, no publisher HTTP.
    # Emulate the pre-recovery terminal outcome, not the current bounded lane.
    # The actual diagnostic and its observation clock remain untouched.
    with queue.db:
        queue.db.execute("UPDATE articles SET state='body_not_found',due=0 WHERE url=?", (URL,))
    receipt, = (queue.root / 'diagnostics').glob('*/cnyes/*.receipt.json')
    return {'receipt_path': str(receipt.relative_to(queue.root)), 'receipt_sha256': sha256_file(receipt)}


def submit(queue, reference):
    return repairs.submit_repairs(queue.root, queue.registry, [URL], reason='Verified legacy zero-byte revalidation',
                                  empty_response_witnesses={URL: reference})


@pytest.mark.parametrize('stored', [False, True])
def test_empty_live_response_uses_retry_cooldown_and_never_parses_or_discards_body(queue, monkeypatch, stored):
    if stored:
        saved_body(queue)
    else:
        with queue.db:
            queue.add_articles([{'url': URL}])
    before = head(queue)
    monkeypatch.setattr('downloader.news_fulltext.extract_article', lambda *a, **k: pytest.fail('empty envelope is not a DOM'))
    http = SimpleNamespace(registry=queue.registry, get=lambda *a, **k:
        Response(URL, 200, {'content-type': 'text/html'}, b''))
    with pytest.raises(NewsError) as caught:
        fetch_article(http, before)
    error = caught.value
    assert error.code == 'empty_http_response' and error.retry and error.delay == 30
    assert not hasattr(error, 'failure_response')  # No parser/raw budget spent.
    queue.failed(before, error, 6)
    after = head(queue)
    assert after['state'] == 'retry' and after['due'] >= time.time() + 29
    for key in repairs.STORED_HEAD_FIELDS:
        assert after[key] == before[key]
    queue.failed({**after, 'attempts': 6}, error, 6)
    exhausted = head(queue)
    assert exhausted['state'] == 'retry_cooldown' and exhausted['attempts'] == 0
    for key in repairs.STORED_HEAD_FIELDS:
        assert exhausted[key] == before[key]
    status = queue.status()
    assert status['article_response_envelope_contract_version'] == ARTICLE_RESPONSE_ENVELOPE_CONTRACT_VERSION == 2
    assert status['providers']['cnyes']['stored_body_urls'] == int(stored)


@pytest.mark.parametrize('status,mime,path,code', [
    (404, 'text/html', URL, 'source_not_found'), (410, 'text/html', URL, 'source_not_found'),
    (403, 'text/html', URL, 'http_403'), (429, 'text/html', URL, 'http_429'),
    (500, 'text/html', URL, 'http_500'), (204, 'text/html', URL, 'http_204'),
    (200, 'application/json', URL, 'unsupported_content_type'),
    (200, 'text/html', 'https://news.cnyes.com/', 'redirect_not_article'),
])
def test_zero_response_never_overrides_status_mime_or_actual_route(queue, status, mime, path, code):
    with pytest.raises(NewsError) as caught:
        _require_article_response(Response(path, status, {'content-type': mime}, b''), queue.registry.sources['cnyes'])
    assert caught.value.code == code


@pytest.mark.parametrize('page,code', [
    (lambda: short_page(body='(OTT) 2.39 :'), 'source_body_quote_only'),
    # Existing native validation rejects these before proving a body. This
    # transport change must preserve that outcome, not invent a new diagnosis.
    (lambda: short_page(change={'payment': 1}), 'body_not_found'),
    (lambda: short_page(canonical=URL.replace('/42', '/43')), 'body_not_found'),
])
def test_nonempty_source_outcomes_are_not_reclassified_as_transport_retries(queue, page, code):
    with pytest.raises(NewsError) as caught:
        extract_article_response(Response(URL, 200, {'content-type': 'text/html'}, page()), queue.registry.sources['cnyes'])
    assert caught.value.code == code and not caught.value.retry


def test_stored_empty_witness_admission_is_read_only_until_single_owner_rechecks(queue, monkeypatch):
    reference = legacy_empty_failure(queue)
    before, total = head(queue), queue.db.total_changes
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no new downloader'))
    result = submit(queue, reference)
    assert result['submitted'] and result['publisher_requests'] == 0 and not result['accepted_as_bodies']
    assert head(queue) == before and queue.db.total_changes == total
    assert repairs.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'revalidation_enqueued': 1}
    assert head(queue) == {**before, 'state': 'retry'}
    directory = queue.root / 'reviewed_repairs/processed'
    receipt = json.loads((directory / (result['request_id'] + '.outcome.json')).read_bytes())
    assert receipt['contract_version'] == 4 and receipt['admitted_targets'] == 1
    assert not receipt['source_freshness_verified'] and not receipt['stored_heads_attempts_deadlines_and_clocks_changed']
    assert receipt['body_acceptances'] == receipt['publisher_requests'] == 0
    assert receipt['empty_responses'][before['id'].hex()]['reference'] == reference
    assert repairs.consume_repairs(queue)['processed'] == 0
    # Replay of the same old observation after a restart cannot enqueue again.
    with queue.db:
        queue.db.execute("UPDATE articles SET state='body_not_found' WHERE url=?", (URL,))
    replay = submit(queue, reference)
    assert not replay['submitted'] and replay['excluded'][0]['reason'] == 'already_admitted_for_parser'


@pytest.mark.parametrize('changes,reason', [
    ({'state': 'retry', 'error_code': 'http_429', 'due': 9999999999}, 'not_semantic_parser_failure'),
    ({'state': 'source_not_found', 'error_code': 'source_not_found'}, 'not_semantic_parser_failure'),
    ({'state': 'source_article_empty_template', 'error_code': 'source_article_empty_template'}, 'not_semantic_parser_failure'),
    ({'state': 'paywall_or_login', 'error_code': 'paywall_or_login'}, 'not_semantic_parser_failure'),
    ({'lease_owner': 'another'}, 'leased_head_protected'),
    ({'due': 9999999999}, 'future_or_invalid_deadline'),
    ({'body_sha256': None}, 'stored_head_required'),
    ({'attempts': 2}, 'empty_response_observation_not_verified'),
])
def test_exact_review_cannot_override_refusals_leases_deadlines_or_another_check(queue, changes, reason):
    reference = legacy_empty_failure(queue)
    with queue.db:
        queue.db.execute('UPDATE articles SET ' + ','.join(key + '=?' for key in changes) + ' WHERE url=?',
                         (*changes.values(), URL))
    before = head(queue)
    result = submit(queue, reference)
    assert not result['submitted'] and result['excluded'][0]['reason'] == reason
    assert head(queue) == before


@pytest.mark.parametrize('field,value', [
    ('body_sha256', 'a' * 64), ('etag', '"new"'), ('last_modified', 'changed'),
    ('captured_at_utc', '2026-01-01T00:00:00+00:00'), ('validator_url', URL + '?changed=1'),
])
def test_saved_representation_changed_after_submission_cannot_be_overwritten(queue, field, value):
    reference = legacy_empty_failure(queue)
    assert submit(queue, reference)['submitted']
    with queue.db:
        queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, URL))
    before = head(queue)
    assert repairs.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'stored_head_changed_since_review': 1}
    assert head(queue) == before


@pytest.mark.parametrize('change', ['receipt_hash', 'parquet_hash', 'receipt_status', 'nonempty_bytes', 'stale_clock', 'scope', 'headers_type'])
def test_forged_or_damaged_or_nonempty_witness_is_not_repair_evidence(queue, change):
    reference = legacy_empty_failure(queue)
    path = queue.root / reference['receipt_path']
    receipt = json.loads(path.read_bytes())
    if change == 'receipt_hash':
        reference['receipt_sha256'] = '0' * 64
    elif change == 'parquet_hash':
        receipt['sha256'] = '0' * 64
        atomic_write_json(path, receipt)
        reference['receipt_sha256'] = sha256_file(path)
    else:
        import pyarrow.parquet as pq
        original = queue.root / receipt['path']
        record, = pq.ParquetFile(original).read().to_pylist()
        field, value = {'receipt_status': ('status', 403), 'nonempty_bytes': ('response_bytes', 1),
                        'stale_clock': ('captured_at_utc', '2026-01-01T00:00:00+00:00'),
                        'scope': ('data_fingerprint', '0' * 64),
                        'headers_type': ('headers_json', '{"content-type": 123}')}[change]
        receipt[field] = record[field] = value
        atomic_write_parquet(original, table_from_records([record]), durable=True)
        receipt['sha256'] = sha256_file(original)
        atomic_write_json(path, receipt)
        reference['receipt_sha256'] = sha256_file(path)
    before = head(queue)
    result = submit(queue, reference)
    assert not result['submitted'] and result['excluded'] and head(queue) == before


def test_unavailable_witness_does_not_kill_owner_or_consume_recheck(queue, monkeypatch):
    reference = legacy_empty_failure(queue)
    result = submit(queue, reference)
    before = head(queue)
    original = repairs._verify_empty_response
    monkeypatch.setattr(repairs, '_verify_empty_response', lambda *a, **k: (_ for _ in ()).throw(OSError('offline witness')))
    assert repairs.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'retained_outcome_io_deferred': 1}
    assert head(queue) == before and Path(result['request_path']).exists()
    monkeypatch.setattr(repairs, '_verify_empty_response', original)
    assert repairs.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'revalidation_enqueued': 1}


def test_default_missing_only_repair_still_protects_this_saved_head(queue):
    legacy_empty_failure(queue)
    result = repairs.submit_repairs(queue.root, queue.registry, [URL], reason='No zero-response scope supplied')
    assert not result['submitted'] and result['excluded'][0]['reason'] == 'stored_head_protected'


def test_cli_uses_existing_read_only_submission_without_constructing_owner(queue, config, monkeypatch):
    from scripts.download_news_fulltext import main
    reference = legacy_empty_failure(queue)
    config_path, refs_path = queue.root / 'synthetic-config.json', queue.root / 'synthetic-witnesses.json'
    config.pop('provider_catalog', None)
    atomic_write_json(config_path, config)
    atomic_write_json(refs_path, {URL: reference})
    before = head(queue)
    monkeypatch.setattr('scripts.download_news_fulltext.NewsQueue', lambda *a, **k: pytest.fail('owner already running'))
    assert main(['repair', '--config', str(config_path), '--root', str(queue.root), '--url', URL,
        '--reason', 'Synthetic exact zero-response review', '--reviewed-empty-responses', str(refs_path)]) == 0
    assert head(queue) == before
    assert repairs.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'revalidation_enqueued': 1}


def test_empty_response_manifest_cannot_select_a_downloader_command(tmp_path):
    from scripts.download_news_fulltext import main
    with pytest.raises(SystemExit) as caught:
        main(['run', '--reviewed-empty-responses', str(tmp_path / 'not-read.json')])
    assert caught.value.code == 2


def test_committed_admission_survives_archive_interruption_without_second_recheck(queue, monkeypatch):
    reference = legacy_empty_failure(queue)
    result = submit(queue, reference)
    original = repairs._archive

    def unavailable(*args, **kwargs):
        raise OSError('synthetic interrupted receipt archive')

    monkeypatch.setattr(repairs, '_archive', unavailable)
    assert repairs.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'revalidation_enqueued': 1}
    before = head(queue)
    assert before['state'] == 'retry' and Path(result['request_path']).exists()
    monkeypatch.setattr(repairs, '_archive', original)
    assert repairs.consume_repairs(queue, max_seconds=0.1)['processed'] == 0
    assert not Path(result['request_path']).exists() and head(queue) == before
    assert queue.db.execute("SELECT count(*) FROM reviewed_repair_items WHERE outcome='revalidation_enqueued'").fetchone()[0] == 1


def test_pending_stored_recheck_resumes_on_new_canonical_owner(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    first = NewsQueue(tmp_path, registry)
    try:
        reference = legacy_empty_failure(first)
        before = head(first)
        assert submit(first, reference)['submitted']
    finally:
        first.close()
    second = NewsQueue(tmp_path, registry)
    try:
        assert head(second) == before
        assert repairs.consume_repairs(second, max_seconds=0.1)['outcomes'] == {'revalidation_enqueued': 1}
        assert head(second) == {**before, 'state': 'retry'}
    finally:
        second.close()
