"""Recover only a source-proven legacy publication basis, without backdating."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data import news_dataset as corpus
from stockagent.data.news_dataset_sources import reuse_snapshot, url_article_id
from test_news_training_dataset import CAPTURED, URLS, read_table, source_fixture


URL = "https://globalvoices.org/2026/10/01/an-article/"
PUBLISHED = "2026-10-01T07:34:18+0000"
HTML = (f'<html><head><meta property="article:published_time" content="{PUBLISHED}">'
        '</head><body><p>Original article.</p></body></html>').encode()


def legacy_record(**changes):
    body = "Original article."
    row = {"article_id": url_article_id(URL).hex(), "provider": "globalvoices",
           "requested_url": URL, "final_url": URL, "title": "Original",
           "body": body, "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
           "published_at_raw": PUBLISHED, "captured_at_utc": CAPTURED,
           "extraction_method": "css:.post-container .entry", "raw_response_body": HTML,
           "response_sha256": hashlib.sha256(HTML).hexdigest()}
    row.update(changes)
    return row


def materialize(root: Path, record):
    path = root / "bodies/old.parquet"
    atomic_write_parquet(path, pa.Table.from_pylist([record]))
    atomic_write_json(root / "receipts/old.json", {"parquet_path": "bodies/old.parquet",
        "sha256": sha256_file(path), "rows": 1})
    return {"id": bytes.fromhex(record["article_id"]), "provider": record["provider"],
        "url": record["requested_url"], "state": "complete", "captured_at_utc": CAPTURED,
        "body_sha256": record["body_sha256"], "parquet_path": "bodies/old.parquet"}


def test_legacy_basis_is_recovered_only_from_verified_source_batch(tmp_path):
    head = materialize(tmp_path, legacy_record())
    before = sha256_file(tmp_path / "bodies/old.parquet")
    ((article, document),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_time_basis"] == corpus.GLOBALVOICES_RETAINED_META_BASIS
    assert article["published_at_raw"] == PUBLISHED
    assert article["available_at_utc"].isoformat() == CAPTURED
    assert article["observed_at_utc"] == article["available_at_utc"]
    assert article["historical_point_in_time"] is False
    assert article["source_sha256"] == before
    assert document["text"] == "Original article."
    assert sha256_file(tmp_path / "bodies/old.parquet") == before


@pytest.mark.parametrize("changes", [
    {"provider": "other"},
    {"requested_url": "https://example.com/article", "final_url": "https://example.com/article"},
    {"requested_url": "https://globalvoices.org.evil.example/article", "final_url": "https://globalvoices.org.evil.example/article"},
    {"final_url": "https://globalvoices.org/another/"},
    {"source_request_url": "https://globalvoices.org/feed/"},
    {"source_response_article_id": "other"},
    {"extraction_method": "rss_content_encoded_v1"},
    {"published_at_raw": "2026-10-01T07:34:19+0000"},
    {"published_at_raw": "not-a-clock"},
    {"raw_response_body": None},
    {"response_sha256": "0" * 64},
    {"published_time_basis": ""},
    {"published_time_basis": "already_declared"},
])
def test_unproven_clock_basis_is_not_inferred(changes):
    assert corpus.legacy_globalvoices_publication_basis(legacy_record(**changes)) is None


@pytest.mark.parametrize("raw", [
    b"<html><head></head></html>",
    HTML.replace(b"2026-10-01T07:34:18+0000", b"2026-10-01T07:34:19+0000"),
    HTML.replace(b"</head>", b'<meta property="article:published_time" content="2026-10-01T07:34:18+0000"></head>'),
])
def test_missing_conflicting_or_ambiguous_original_declaration_is_not_inferred(raw):
    record = legacy_record(raw_response_body=raw, response_sha256=hashlib.sha256(raw).hexdigest())
    assert corpus.legacy_globalvoices_publication_basis(record) is None


def test_existing_basis_schema_is_preserved_even_when_null(tmp_path):
    head = materialize(tmp_path, legacy_record(published_time_basis=None))
    ((article, _),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_time_basis"] is None


def test_corrupt_retained_response_keeps_article_but_does_not_claim_basis(tmp_path):
    head = materialize(tmp_path, legacy_record(response_sha256="0" * 64))
    ((article, document),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_time_basis"] is None
    assert article["published_at_raw"] == PUBLISHED
    assert document["text"] == "Original article."


def test_invalid_batch_receipt_prevents_basis_or_article_materialization(tmp_path):
    head = materialize(tmp_path, legacy_record())
    atomic_write_json(tmp_path / "receipts/old.json", {
        "parquet_path": "bodies/old.parquet", "sha256": "0" * 64, "rows": 1})
    with pytest.raises(ValueError, match="source checksum mismatch"):
        corpus._load_body_input((tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))


def test_existing_basis_does_not_decode_raw_html(tmp_path, monkeypatch):
    head = materialize(tmp_path, legacy_record(published_time_basis="source_basis"))
    original = corpus.pq.ParquetFile
    projections = []

    class ObserveParquet:
        def __init__(self, path):
            self.original = original(path)

        def __getattr__(self, name):
            return getattr(self.original, name)

        def iter_batches(self, **kwargs):
            projections.append(kwargs["columns"])
            return self.original.iter_batches(**kwargs)

    monkeypatch.setattr(corpus.pq, "ParquetFile", ObserveParquet)
    ((article, _),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_time_basis"] == "source_basis"
    assert len(projections) == 1 and "raw_response_body" not in projections[0]


@pytest.mark.parametrize("changes", [
    {"extraction_method": "rss_content_encoded_v1"},
    {"final_url": "https://globalvoices.org/other/"},
])
def test_legacy_non_candidate_does_not_decode_raw_html(tmp_path, monkeypatch, changes):
    head = materialize(tmp_path, legacy_record(**changes))
    original = corpus.pq.ParquetFile
    projections = []

    class ObserveParquet:
        def __init__(self, path):
            self.original = original(path)

        def __getattr__(self, name):
            return getattr(self.original, name)

        def iter_batches(self, **kwargs):
            projections.append(kwargs["columns"])
            return self.original.iter_batches(**kwargs)

    monkeypatch.setattr(corpus.pq, "ParquetFile", ObserveParquet)
    ((article, _),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_time_basis"] is None
    assert len(projections) == 1 and "raw_response_body" not in projections[0]


def test_legacy_candidate_decodes_raw_separately_in_bounded_rows(tmp_path, monkeypatch):
    head = materialize(tmp_path, legacy_record())
    original = corpus.pq.ParquetFile
    projections = []

    class ObserveParquet:
        def __init__(self, path):
            self.original = original(path)

        def __getattr__(self, name):
            return getattr(self.original, name)

        def iter_batches(self, **kwargs):
            projections.append((kwargs["columns"], kwargs["batch_size"]))
            return self.original.iter_batches(**kwargs)

    monkeypatch.setattr(corpus.pq, "ParquetFile", ObserveParquet)
    ((article, _),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_time_basis"] == corpus.GLOBALVOICES_RETAINED_META_BASIS
    assert len(projections) == 2
    assert "raw_response_body" not in projections[0][0]
    assert projections[1] == (["raw_response_body"], 1)


def test_new_corpus_materializes_recovered_basis_without_backdating(tmp_path, monkeypatch):
    monkeypatch.setattr(corpus._NewsDatasetDiskGuard, "check", lambda self: None)
    source, output = source_fixture(tmp_path), tmp_path / "dataset"
    path = source / "bodies/current.parquet"
    old_rows = pq.read_table(path).to_pylist()
    record = legacy_record()
    atomic_write_parquet(path, pa.Table.from_pylist([record, old_rows[1]]))
    atomic_write_json(source / "receipts/current.json", {
        "parquet_path": "bodies/current.parquet", "sha256": sha256_file(path), "rows": 2})
    import sqlite3
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE articles SET id=?,url=?,provider=?,body_sha256=? WHERE id=?",
                   (url_article_id(URL), URL, "globalvoices", record["body_sha256"], url_article_id(URLS[0])))
    before = sha256_file(path)
    corpus.build_news_dataset(source, output, workers=2)
    articles = [row for row in read_table(output, "articles") if row["article_id"] == url_article_id(URL).hex()]
    assert len(articles) == 1
    assert articles[0]["published_time_basis"] == corpus.GLOBALVOICES_RETAINED_META_BASIS
    assert articles[0]["published_at_raw"] == PUBLISHED
    assert articles[0]["available_at_utc"].isoformat() == CAPTURED
    assert not articles[0]["historical_point_in_time"]
    assert sha256_file(path) == before


def test_existing_dataset_and_snapshot_require_a_separate_compatible_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(corpus._NewsDatasetDiskGuard, "check", lambda self: None)
    source, output = source_fixture(tmp_path), tmp_path / "dataset"
    corpus.build_news_dataset(source, output)
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["publication_basis_scope_contract"] == corpus.PUBLICATION_BASIS_SCOPE_CONTRACT
    frozen = output / manifest["generation"]
    snapshot_path = frozen / "snapshot.json"
    snapshot = json.loads(snapshot_path.read_text())
    snapshot.pop("publication_basis_scope_contract")
    atomic_write_json(snapshot_path, snapshot)
    with pytest.raises(ValueError, match="incompatible publication basis scope"):
        reuse_snapshot(source, tmp_path / "new_frozen", frozen)
    manifest.pop("publication_basis_scope_contract")
    atomic_write_json(manifest_path, manifest)
    before = sha256_file(manifest_path)
    # The new write contract must not make an old committed manifest unreadable.
    assert len(read_table(output, "articles")) == 3
    with pytest.raises(ValueError, match="separate output root"):
        corpus.build_news_dataset(source, output, refresh=True)
    assert sha256_file(manifest_path) == before
