"""A metadata inventory is not repaired data or complete source history."""
import sqlite3

import pytest

from downloader.artifact_io import atomic_write_parquet, table_from_records
from scripts.audit_cnyes_mobile_metadata import audit


def fixture(tmp_path, *, records=None):
    db = sqlite3.connect(tmp_path / 'queue.sqlite3')
    db.execute('CREATE TABLE articles(id BLOB,url TEXT,host TEXT,state TEXT,body_sha256 TEXT,'
               'parquet_path TEXT,captured_at_utc TEXT)')
    db.execute('INSERT INTO articles VALUES (?,?,?,?,?,?,?)',
               (bytes.fromhex('01'), 'https://m.cnyes.com/news/id/42', 'm.cnyes.com',
                'complete', 'head-sha', 'batch.parquet', '2026-10-10T00:00:00+00:00'))
    db.commit()
    db.close()
    default = {'article_id': '01', 'body_sha256': 'head-sha', 'published_at_raw': None,
               'authors_json': '[]', 'declared_canonical_url': 'https://news.cnyes.com/news/id/42',
               'final_url': 'https://m.cnyes.com/news/id/42', 'extraction_method': 'css:#article-container',
               'body_completeness': 'article_container_unverified', 'data_fingerprint': 'old',
               'raw_response_body': b'Private response bytes must never be read by this inventory.'}
    atomic_write_parquet(tmp_path / 'batch.parquet', table_from_records(records if records is not None else [default]))
    return default


def test_reads_only_metadata_once_and_preserves_missing_values(tmp_path, monkeypatch):
    import scripts.audit_cnyes_mobile_metadata as module
    original = module.pq.ParquetFile
    calls = []

    class SelectedColumns:
        def __init__(self, path):
            self.file = original(path)
            self.schema_arrow = self.file.schema_arrow

        def read(self, *, columns, use_threads):
            calls.append(columns)
            assert 'raw_response_body' not in columns and 'body' not in columns
            assert use_threads is False
            return self.file.read(columns=columns, use_threads=use_threads)

    fixture(tmp_path)
    monkeypatch.setattr(module.pq, 'ParquetFile', SelectedColumns)
    result = audit(tmp_path)
    assert len(calls) == 1 and result['metadata_inventory_accepted'] is True
    assert result['findings']['missing_publication_clock'] == 1
    assert result['findings']['canonical_alias_metadata_replay_candidates'] == 1
    assert result['source_aware_publication_min_utc'] is None
    assert result['capture_min_utc'] == '2026-10-10T00:00:00+00:00'
    assert result['queue_modified'] is False and result['http_requests'] == 0
    assert result['all_source_metadata_correct'] is False


@pytest.mark.parametrize('problem', ['duplicate', 'digest_mismatch', 'missing_head'])
def test_broken_head_metadata_join_is_explicitly_rejected(tmp_path, problem):
    record = fixture(tmp_path)
    records = [record, record] if problem == 'duplicate' else [
        {**record, 'body_sha256': 'wrong'} if problem == 'digest_mismatch' else {**record, 'article_id': 'ff'}]
    atomic_write_parquet(tmp_path / 'batch.parquet', table_from_records(records))
    result = audit(tmp_path)
    assert result['metadata_inventory_accepted'] is False
    assert result['errors'] and result['all_history_complete'] is False


def test_explicit_limit_never_silently_truncates_the_inventory(tmp_path):
    fixture(tmp_path)
    with pytest.raises(ValueError, match='exceeds_explicit_bound'):
        audit(tmp_path, maximum_heads=0)


def test_missing_timezone_is_not_reconstructed_from_capture_time(tmp_path):
    record = fixture(tmp_path)
    atomic_write_parquet(tmp_path / 'batch.parquet', table_from_records([
        {**record, 'published_at_raw': '2014-01-01T01:02:00', 'authors_json': '["來源作者"]',
         'body_completeness': 'declared_article_body'}]))
    result = audit(tmp_path)
    assert result['findings']['source_clock_without_declared_timezone'] == 1
    assert result['source_aware_publication_min_utc'] is None
    assert result['raw_response_integrity_verified'] is False
