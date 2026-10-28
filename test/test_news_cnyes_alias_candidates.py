"""An apparent duplicate is a candidate, never automatic body acceptance."""
import hashlib
import json

import pytest
import requests

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_cnyes_alias_candidates import audit, audit_amp_declarations, _declared_amp_head
from test_news_fulltext import config


@pytest.fixture
def queue(config, tmp_path):
    config['providers'][0].update(id='cnyes', domains=['m.cnyes.com', 'news.cnyes.com', 'amp-news.cnyes.com'])
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        yield q
    finally:
        q.close()


def canonical_candidate(queue, story_id, *, state='complete', has_body=True):
    url = 'https://news.cnyes.com/news/id/' + str(story_id)
    queue.add_articles([{'url': url, 'title': 'original'}])
    # Metadata-only candidate fixture intentionally has NO receipt or file.
    queue.db.execute('UPDATE articles SET state=?,body_sha256=? WHERE url=?',
                     (state, 'a' * 64 if has_body else None, url))
    queue.db.commit()


def test_alias_inventory_is_bounded_readonly_and_never_proves_body_equivalence(queue, monkeypatch):
    monkeypatch.setattr(requests.Session, 'request', lambda *a, **k: pytest.fail('no publisher HTTP allowed'))
    canonical_candidate(queue, 1)
    queue.add_articles([{'url': 'https://m.cnyes.com/news/id/1?exp=a', 'title': 'original'},
                        {'url': 'https://m.cnyes.com/news/id/1?exp=b', 'title': 'different'},
                        {'url': 'https://m.cnyes.com/news/id/2', 'title': 'other'}])
    queue.db.commit()
    before = queue.db.total_changes
    result = audit(queue.root, 2)
    assert result['sampled_pending_mobile_heads'] == 2 and result['sample_limit_reached']
    assert result['queue_owner_id'] == queue.owner and queue.db.total_changes == before
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['alias_source_equivalence_verified'] and not result['canonical_receipts_verified']
    assert not result['historical_completeness_verified']
    assert result['counts']['existing_canonical_body_candidate'] == 2
    assert result['counts']['supported_current_mobile_view'] == 2
    assert result['counts'].get('unverified_query_view', 0) == 0
    assert result['counts']['exact_metadata_title_match'] == result['counts']['different_metadata_titles'] == 1
    assert any('host_state' in line for line in result['query_plan'])
    assert 'exp=b' not in str(result)  # No query values, credentials or bodies in the report.
    assert queue.db.execute("SELECT count(*) FROM articles WHERE host='m.cnyes.com' AND state='pending'").fetchone()[0] == 3


@pytest.mark.parametrize('state,has_body', [('complete', False), ('retry', True), ('source_access_denied', True)])
def test_incomplete_or_refused_canonical_head_is_not_a_candidate(queue, state, has_body):
    canonical_candidate(queue, 1, state=state, has_body=has_body)
    queue.add_articles([{'url': 'https://m.cnyes.com/news/id/1'}])
    queue.db.commit()
    result = audit(queue.root)
    assert result['counts']['no_complete_canonical_body_candidate'] == 1
    assert not result['candidates'][0]['existing_canonical_body_candidate']


@pytest.mark.parametrize('bound', [0, 4097, True, 1.5])
def test_invalid_sample_bounds_are_rejected_without_opening_the_database(tmp_path, bound):
    with pytest.raises(ValueError, match='bounded_integer_sample_required'):
        audit(tmp_path, bound)
    assert not (tmp_path / 'queue.sqlite3').exists()


def retained_amp_candidates(queue, *, count=1, api=False, mutation=None):
    first, records = None, []
    raw = (b'{"items":{"data":[{"newsId":1,"content":"private-body-canary"}]}}' if api else
        b'<html><head><link rel="canonical" href="https://news.cnyes.com/news/id/1">'
        b'<link rel="amphtml" href="https://amp-news.cnyes.com/news/id/1"></head>'
        b'<body>private-body-canary</body></html>')
    for number in range(1, count+1):
        canonical_candidate(queue, number)
        primary = 'https://news.cnyes.com/news/id/'+str(number)
        amp = 'https://amp-news.cnyes.com/news/id/'+str(number)
        row = queue.db.execute('SELECT id FROM articles WHERE url=?', (primary,)).fetchone()
        first = first or row['id'].hex()
        queue.add_articles([{'url': amp}])
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=? WHERE url=?', (row['id'], amp))
        records.append({'article_id': row['id'].hex(), 'body_sha256': 'a'*64,
            'requested_url': primary, 'final_url': primary,
            'source_request_url': 'https://api.cnyes.com/media/api/v1/newslist/category/anue_live' if api else primary,
            'response_sha256': hashlib.sha256(raw).hexdigest(), 'source_response_article_id': first,
            'raw_response_body': raw if number == 1 else None,
            'body': 'private-body-canary', 'extraction_method': 'cnyes_api_content_v1' if api else 'css:article'})
    if mutation == 'raw_hash': records[0]['response_sha256'] = 'b'*64
    if mutation == 'head_hash': records[0]['body_sha256'] = 'b'*64
    if mutation == 'duplicate': records.append(dict(records[0]))
    path = queue.root/'bodies'/'synthetic.parquet'
    atomic_write_parquet(path, table_from_records(records), durable=True)
    receipt = path.with_suffix('.receipt.json')
    atomic_write_json(receipt, {'sha256': sha256_file(path), 'rows': len(records)}, durable=True)
    if mutation == 'receipt': atomic_write_json(receipt, {'sha256': 'c'*64, 'rows': len(records)}, durable=True)
    queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)',
        (str(path.relative_to(queue.root)), str(receipt.relative_to(queue.root))))
    queue.db.execute("UPDATE articles SET parquet_path=? WHERE host='news.cnyes.com'",
        (str(path.relative_to(queue.root)),))
    queue.db.commit()
    return path, raw


def test_amp_native_head_evidence_is_readonly_not_article_acceptance(queue, monkeypatch):
    monkeypatch.setattr(requests.Session, 'request', lambda *args, **kwargs: pytest.fail('no HTTP'))
    monkeypatch.setattr('downloader.news_fulltext_policy.extract_article',
        lambda *args, **kwargs: pytest.fail('healthy article must not be reparsed'))
    retained_amp_candidates(queue)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = audit_amp_declarations(queue.root)
    assert result['selected_queued_amp_heads'] == 1 and result['entire_current_queued_amp_scope_selected']
    assert result['selected_inputs_unchanged'] and result['publisher_requests'] == result['body_extractors_called'] == 0
    assert result['counts'] == {'native_head_declares_exact_amp_counterpart': 1}
    assert result['findings'][0]['publisher_declared_counterpart']
    assert not result['alias_source_equivalence_verified'] and result['aliases_admitted'] == 0
    assert not result['accepted_body_text_retained'] and 'private-body-canary' not in json.dumps(result)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


def test_shared_api_batch_is_read_once_without_promoting_ids_or_content_to_amp_links(queue, monkeypatch):
    import pyarrow.parquet as pq
    _, raw = retained_amp_candidates(queue, count=2, api=True)
    original = pq.ParquetFile.read
    calls = []
    def projected_read(self, *args, **kwargs):
        calls.append(kwargs['columns'])
        assert 'body' not in kwargs['columns']
        return original(self, *args, **kwargs)
    monkeypatch.setattr(pq.ParquetFile, 'read', projected_read)
    result = audit_amp_declarations(queue.root)
    assert len(calls) == 1 and len(result['committed_batches']) == 1
    assert result['committed_batches'][0]['distinct_originals'] == 1
    assert result['inventory_cost']['distinct_original_response_bytes'] == len(raw)
    assert result['counts'] == {'api_content_not_reviewed_native_amp_declaration': 2}
    assert not any(row['publisher_declared_counterpart'] for row in result['findings'])


@pytest.mark.parametrize('change', ['body_link_only', 'wrong_canonical', 'wrong_amp', 'duplicate_amp', 'api_string'])
def test_related_body_links_scripts_other_pages_and_api_strings_are_not_native_amp_proof(change):
    canonical, amp = 'https://news.cnyes.com/news/id/1', 'https://amp-news.cnyes.com/news/id/1'
    declaration = f'<link rel="canonical" href="{canonical}"><link rel="amphtml" href="{amp}">'
    raw = ('<html><head>'+declaration+'</head><body></body></html>').encode()
    method = 'css:article'
    if change == 'body_link_only': raw = ('<html><head></head><body>'+declaration+'</body></html>').encode()
    elif change == 'wrong_canonical': raw = raw.replace(canonical.encode(), (canonical+'2').encode())
    elif change == 'wrong_amp': raw = raw.replace(amp.encode(), (amp+'2').encode())
    elif change == 'duplicate_amp': raw = raw.replace(b'</head>', f'<link rel="amphtml" href="{amp}"></head>'.encode())
    elif change == 'api_string': raw = json.dumps({'content': declaration}).encode(); method = 'cnyes_api_content_v1'
    assert not _declared_amp_head(raw, canonical, amp, extraction_method=method)['publisher_declared_counterpart']


def test_amp_scope_is_bounded_and_unstored_primary_is_not_read(queue):
    retained_amp_candidates(queue, count=2, api=True)
    result = audit_amp_declarations(queue.root, 1)
    assert result['sample_limit_reached'] and not result['entire_current_queued_amp_scope_selected']
    assert len(result['findings']) == 1
    queue.db.execute("UPDATE articles SET state='source_not_found',body_sha256=NULL,parquet_path=NULL "
                     "WHERE url='https://news.cnyes.com/news/id/1'")
    queue.db.commit()
    result = audit_amp_declarations(queue.root)
    assert result['counts']['no_complete_primary_candidate'] == 1


@pytest.mark.parametrize('mutation', ['raw_hash', 'head_hash', 'receipt', 'duplicate'])
def test_amp_candidate_corruption_fails_without_queue_mutation(queue, mutation):
    retained_amp_candidates(queue, mutation=mutation)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    with pytest.raises(ValueError, match='mismatch|duplicate'):
        audit_amp_declarations(queue.root)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


@pytest.mark.parametrize('budget', ['AMP_MAX_ENCODED_BYTES', 'AMP_MAX_DECODED_BYTES', 'AMP_MAX_BATCH_ROWS'])
def test_amp_candidate_reads_obey_aggregate_memory_io_and_row_bounds(queue, monkeypatch, budget):
    import scripts.audit_news_cnyes_alias_candidates as implementation
    retained_amp_candidates(queue)
    monkeypatch.setattr(implementation, budget, 0)
    with pytest.raises(ValueError, match='budget|rows'):
        audit_amp_declarations(queue.root)


@pytest.mark.parametrize('bound', [0, 4097, True, 1.5])
def test_amp_invalid_bounds_do_not_create_a_frontier(tmp_path, bound):
    with pytest.raises(ValueError, match='bounded_integer_sample_required'):
        audit_amp_declarations(tmp_path, bound)
    assert not (tmp_path/'queue.sqlite3').exists()


def retained_amp_mobile_candidate(queue, *, mutation=None):
    """A committed mobile original is a separate observation from the AMP URL."""
    primary = 'https://news.cnyes.com/news/id/1'
    mobile = 'https://m.cnyes.com/news/id/1'
    amp = 'https://amp-news.cnyes.com/news/id/1'
    body = 'private-mobile-body-canary'
    raw = (f'<html><head><link rel="canonical" href="{primary}">'
           f'<link rel="amphtml" href="{amp}"></head><body>{body}</body></html>').encode()
    queue.add_articles([{'url': mobile}, {'url': amp}])
    row = queue.db.execute('SELECT id FROM articles WHERE url=?', (mobile,)).fetchone()
    body_hash = hashlib.sha256(body.encode()).hexdigest()
    record = {'article_id': row['id'].hex(), 'body_sha256': body_hash, 'body': body,
              'body_bytes': len(body.encode()), 'requested_url': mobile, 'final_url': mobile,
              'source_request_url': mobile, 'response_sha256': hashlib.sha256(raw).hexdigest(),
              'source_response_article_id': row['id'].hex(), 'raw_response_body': raw,
              'extraction_method': 'cnyes_visible_canonical_alias_v1'}
    if mutation == 'body': record['body'] = 'different'
    if mutation == 'body_bytes': record['body_bytes'] += 1
    if mutation == 'raw_hash': record['response_sha256'] = 'b'*64
    if mutation == 'identity': record['final_url'] = primary
    path = queue.root/'bodies'/'mobile-synthetic.parquet'
    atomic_write_parquet(path, table_from_records([record]), durable=True)
    relative = str(path.relative_to(queue.root))
    receipt = path.with_suffix('.receipt.json')
    receipt_data = {'contract_version': 1, 'parquet_path': relative, 'publish': False,
                    'sha256': sha256_file(path), 'rows': 1}
    if mutation == 'receipt_path': receipt_data['parquet_path'] = 'other.parquet'
    if mutation == 'receipt_contract': receipt_data['contract_version'] = 2
    if mutation == 'receipt_publish': receipt_data['publish'] = True
    if mutation == 'receipt_hash': receipt_data['sha256'] = 'c'*64
    atomic_write_json(receipt, receipt_data, durable=True)
    queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)',
                     (relative, str(receipt.relative_to(queue.root))))
    queue.db.execute("UPDATE articles SET state='complete',body_sha256=?,body_bytes=?,parquet_path=? WHERE url=?",
                     (body_hash, len(body.encode()), relative, mobile))
    queue.db.commit()
    return path


def test_amp_mobile_originals_are_receipted_readonly_evidence_not_accepted_aliases(queue, monkeypatch):
    from scripts.audit_news_cnyes_alias_candidates import audit_amp_mobile_declarations
    monkeypatch.setattr(requests.Session, 'request', lambda *args, **kwargs: pytest.fail('no HTTP'))
    retained_amp_mobile_candidate(queue)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = audit_amp_mobile_declarations(queue.root)
    assert result['inspection_kind'] == 'retained_mobile_native_amp_declarations'
    assert result['selected_queued_amp_heads'] == 1 and result['selected_inputs_unchanged']
    assert result['counts'] == {'native_head_declares_exact_amp_counterpart': 1}
    assert result['findings'][0]['publisher_declared_counterpart']
    assert result['publisher_requests'] == result['body_extractors_called'] == 0
    assert not result['alias_source_equivalence_verified'] and result['aliases_admitted'] == 0
    assert not result['accepted_body_text_retained'] and 'private-mobile-body-canary' not in json.dumps(result)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


def test_amp_relative_links_resolve_against_the_observed_document_not_primary():
    primary = 'https://news.cnyes.com/news/id/1'
    mobile = 'https://m.cnyes.com/news/id/1'
    amp = 'https://amp-news.cnyes.com/news/id/1'
    raw = (f'<html><head><link rel="canonical" href="/news/id/1">'
           f'<link rel="amphtml" href="{amp}"></head></html>').encode()
    assert _declared_amp_head(raw, primary, amp, extraction_method='css:article')['publisher_declared_counterpart']
    assert not _declared_amp_head(raw, primary, amp, document_url=mobile,
                                  extraction_method='css:article')['publisher_declared_counterpart']


def test_amp_relative_links_honor_the_first_native_html_base():
    primary = 'https://news.cnyes.com/news/id/1'
    mobile = 'https://m.cnyes.com/news/id/1'
    amp = 'https://amp-news.cnyes.com/news/id/1'
    raw = (f'<html><head><base href="https://news.cnyes.com/">'
           f'<link rel="canonical" href="/news/id/1"><link rel="amphtml" href="{amp}">'
           '</head></html>').encode()
    assert _declared_amp_head(raw, primary, amp, document_url=mobile,
                             extraction_method='css:article')['publisher_declared_counterpart']
    invalid = raw.replace(b'https://news.cnyes.com/', b'http://[invalid/')
    assert not _declared_amp_head(invalid, primary, amp, document_url=mobile,
                                  extraction_method='css:article')['publisher_declared_counterpart']


@pytest.mark.parametrize('mutation', ['body', 'body_bytes', 'raw_hash', 'identity', 'receipt_path',
                                      'receipt_contract', 'receipt_publish', 'receipt_hash'])
def test_amp_mobile_corruption_fails_closed_without_mutating_queue(queue, mutation):
    from scripts.audit_news_cnyes_alias_candidates import audit_amp_mobile_declarations
    retained_amp_mobile_candidate(queue, mutation=mutation)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    with pytest.raises(ValueError, match='mismatch|invalid'):
        audit_amp_mobile_declarations(queue.root)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


@pytest.mark.parametrize('budget', ['AMP_MAX_ENCODED_BYTES', 'AMP_MOBILE_MAX_DECODED_BYTES',
                                     'AMP_MOBILE_MAX_BATCH_DECODED_BYTES', 'AMP_MAX_BATCH_ROWS'])
def test_amp_mobile_reads_have_encoded_and_decoded_bounds(queue, monkeypatch, budget):
    import scripts.audit_news_cnyes_alias_candidates as implementation
    retained_amp_mobile_candidate(queue)
    monkeypatch.setattr(implementation, budget, 0)
    with pytest.raises(ValueError, match='budget|rows'):
        implementation.audit_amp_mobile_declarations(queue.root)


def test_amp_mobile_missing_or_incomplete_head_does_not_attempt_artifact_read(queue, monkeypatch):
    from scripts.audit_news_cnyes_alias_candidates import audit_amp_mobile_declarations
    import downloader.news_fulltext_queue as implementation
    queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1'},
                        {'url': 'https://amp-news.cnyes.com/news/id/2'},
                        {'url': 'https://m.cnyes.com/news/id/2'}])
    queue.db.execute("UPDATE articles SET state='retry',body_sha256=?,parquet_path=? WHERE url=?",
                     ('a'*64, 'missing.parquet', 'https://m.cnyes.com/news/id/2'))
    queue.db.commit()
    monkeypatch.setattr(implementation, 'source_path', lambda *args: pytest.fail('no artifact is admissible'))
    result = audit_amp_mobile_declarations(queue.root)
    assert result['counts'] == {'no_exact_literal_mobile_candidate': 1, 'no_complete_mobile_candidate': 1}
    assert not any(row['publisher_declared_counterpart'] for row in result['findings'])


def test_amp_mobile_readback_detects_head_change(queue, monkeypatch):
    import scripts.audit_news_cnyes_alias_candidates as implementation
    retained_amp_mobile_candidate(queue)
    original = implementation._declared_amp_head
    def change_head(*args, **kwargs):
        result = original(*args, **kwargs)
        queue.db.execute("UPDATE articles SET state='retry' WHERE url='https://m.cnyes.com/news/id/1'")
        queue.db.commit()
        return result
    monkeypatch.setattr(implementation, '_declared_amp_head', change_head)
    result = implementation.audit_amp_mobile_declarations(queue.root)
    assert not result['selected_inputs_unchanged'] and result['aliases_admitted'] == 0


def test_amp_bounded_reader_allocates_for_actual_file_not_aggregate_budget(tmp_path, monkeypatch):
    from pathlib import Path
    from scripts.audit_news_cnyes_alias_candidates import _read_stable_bounded
    path = tmp_path/'small.bin'
    path.write_bytes(b'small-content')
    original = Path.open
    reads = []
    class Tracked:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def fileno(self): return self.stream.fileno()
        def read(self, size): reads.append(size); return self.stream.read(size)
    monkeypatch.setattr(Path, 'open', lambda self, *args, **kwargs: Tracked(original(self, *args, **kwargs)))
    payload, _ = _read_stable_bounded(path, 64 << 20)
    assert payload == b'small-content' and reads == [len(payload)+1]
