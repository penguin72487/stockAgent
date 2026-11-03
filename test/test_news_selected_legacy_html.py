"""Retained legacy HTML may be replayed only with its own verifiable envelope."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import article_response_record, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, article_claim_witness, utc_now
from scripts.audit_news_selected_articles import _legacy_globalvoices_direct_html, audit
from test_news_globalvoices_small_body import URL, page


@pytest.fixture
def legacy_batch(tmp_path):
    settings = deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    settings['providers'] = [p for p in settings['providers'] if p['id'] == 'globalvoices']
    settings.pop('provider_catalog', None)
    settings.update(root=str(tmp_path), min_free_disk_bytes=1, exclusive_http_owner=False,
                    adaptive_rate={'enabled': False})
    raw = page().replace(b'</head>', f'<meta property="og:url" content="{URL}"></head>'.encode())
    registry = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    queue.add_articles([{'url': URL}])
    row = queue.claim('globalvoices', 300)
    source = registry.sources['globalvoices']
    article = extract_article(raw, source, page_url=URL)
    record = article_response_record(source, row, Response(URL, 200, {}, raw), article,
        captured_at_utc=utc_now(), http_duration_seconds=None, parse_duration_seconds=None, duration_seconds=None)
    record['article_claim_witness_json'] = article_claim_witness(row)
    queue.accept([record])
    batch, receipt = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
    parquet_path, receipt_path = tmp_path / batch, tmp_path / receipt
    table = pq.read_table(parquet_path).drop(['source_response_article_id', 'source_request_url'])

    def save(synthetic_table):
        # Only a newly generated test fixture is rewritten to model the old
        # on-disk schema. Its receipt is made consistent with that fixture.
        pq.write_table(synthetic_table, parquet_path)
        data = json.loads(receipt_path.read_bytes())
        data['sha256'] = sha256_file(parquet_path)
        receipt_path.write_text(json.dumps(data), encoding='utf-8')

    save(table)
    yield queue, registry, table, save
    queue.close()


def test_selected_legacy_original_is_replayed_without_inventing_request(legacy_batch, tmp_path):
    queue, registry, _, _ = legacy_batch
    before = queue.db.total_changes
    result = audit(tmp_path, [URL], registry=registry, include_retained_heads=True)
    assert result['retained_versions_verified'] and result['retained_current_parser_replay_verified']
    assert result['articles'][0]['current_source_replay_verified']
    assert result['articles'][0]['source_request_url'] is None
    assert result['articles'][0]['response_reference_basis'] == 'legacy_own_raw_publisher_identity_v1'
    assert result['articles'][0]['original_transport_request_verified'] is False
    assert result['current_source_identity_verified'] is False
    assert queue.db.total_changes == before


@pytest.mark.parametrize('change', ['canonical', 'og_url'])
def test_legacy_replay_rejects_wrong_publisher_identity_with_valid_hashes(legacy_batch, tmp_path, change):
    queue, registry, table, save = legacy_batch
    row = table.to_pylist()[0]
    target = URL.encode()
    wrong = URL.replace('synthetic-roundup', 'synthetic-rounduX').encode()
    if change == 'canonical':
        old, new = b'href="' + target + b'"', b'href="' + wrong + b'"'
    else:
        old, new = b'content="' + target + b'"', b'content="' + wrong + b'"'
    assert row['raw_response_body'].count(old) == 1 and len(old) == len(new)
    row['raw_response_body'] = row['raw_response_body'].replace(old, new)
    row['response_sha256'] = hashlib.sha256(row['raw_response_body']).hexdigest()
    save(pa.Table.from_pylist([row], schema=table.schema))
    result = audit(tmp_path, [URL], registry=registry, include_retained_heads=True)
    assert result['retained_versions_verified']
    assert not result['retained_current_parser_replay_verified']
    assert result['errors'][0]['error'] == 'legacy_html_publisher_identity_unverified'


def test_legacy_replay_rejects_raw_hash_mismatch(legacy_batch, tmp_path):
    queue, registry, table, save = legacy_batch
    row = table.to_pylist()[0]
    row['response_sha256'] = '0' * 64
    save(pa.Table.from_pylist([row], schema=table.schema))
    result = audit(tmp_path, [URL], registry=registry, include_retained_heads=True)
    assert not result['retained_versions_verified']
    assert not result['retained_current_parser_replay_verified']


@pytest.mark.parametrize('stored,expected', [
    (['Writer', 'Writer'], True),
    (['Writer', 'Another'], False),
])
def test_legacy_author_mismatch_is_diagnosed_without_accepting_or_rewriting(legacy_batch, tmp_path, stored, expected):
    _, registry, table, save = legacy_batch
    row = table.to_pylist()[0]
    row['authors_json'] = json.dumps(stored)
    save(pa.Table.from_pylist([row], schema=table.schema))
    result = audit(tmp_path, [URL], registry=registry, include_retained_heads=True)
    article = result['articles'][0]
    assert result['retained_versions_verified'] and not result['retained_current_parser_replay_verified']
    assert not article['current_source_replay_verified']
    assert article['current_source_replay_mismatch_fields'] == ['authors_json']
    assert article['legacy_authors_match_after_ordered_deduplication'] is expected
    assert json.loads(article['authors_json']) == stored


@pytest.mark.parametrize('change', [
    'other_provider', 'feed', 'new_reference_field', 'new_request_field',
    'other_final_url', 'other_article_id', 'duplicate_canonical', 'duplicate_og_url',
])
def test_legacy_envelope_rejects_unproven_identity_or_nonlegacy_schema(legacy_batch, change):
    _, _, table, _ = legacy_batch
    row = table.to_pylist()[0]
    if change == 'other_provider':
        row['provider'] = 'other'
    elif change == 'feed':
        row['extraction_method'] = 'rss_content_encoded_v1'
    elif change == 'new_reference_field':
        row['source_response_article_id'] = None
    elif change == 'new_request_field':
        row['source_request_url'] = None
    elif change == 'other_final_url':
        row['final_url'] = URL.replace('synthetic-roundup', 'synthetic-other')
    elif change == 'other_article_id':
        row['article_id'] = '0' * 64
    else:
        declaration = (f'<link rel="canonical" href="{URL}">' if change == 'duplicate_canonical'
                       else f'<meta property="og:url" content="{URL}">').encode()
        row['raw_response_body'] = row['raw_response_body'].replace(b'</head>', declaration + b'</head>')
        row['response_sha256'] = hashlib.sha256(row['raw_response_body']).hexdigest()
    with pytest.raises(ValueError):
        _legacy_globalvoices_direct_html(row, URL)
