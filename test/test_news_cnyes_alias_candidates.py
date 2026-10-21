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
