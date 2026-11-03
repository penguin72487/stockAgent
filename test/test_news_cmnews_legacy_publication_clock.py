"""Recover a witnessed source declaration without backdating old news."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pyarrow as pa
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data import news_dataset as corpus
from stockagent.data.news_dataset_sources import url_article_id
from test_news_cmnews_scope import AUTHOR, TEXT, URL, page
from test_news_training_dataset import CAPTURED, read_table, source_fixture


OLD_CLOCK = "2026-10-09 14:20 (更新：2026-10-09 14:25)"
DECLARED = "2026-10-09T14:20:23+08:00"
MODIFIED = "2026-10-09T14:25:56+08:00"


def html(*, declared=DECLARED, modified=MODIFIED, **kwargs):
    soup = BeautifulSoup(page(clock=OLD_CLOCK, schema_clock=declared, **kwargs), "lxml")
    script = soup.select_one('script[type="application/ld+json"]')
    declaration = json.loads(script.string)
    declaration["dateModified"] = modified
    script.string = json.dumps(declaration, ensure_ascii=False)
    return str(soup).encode()


def old_record(**changes):
    raw = html()
    record = {"article_id": url_article_id(URL).hex(), "provider": "cmnews",
              "requested_url": URL, "final_url": URL, "title": "本篇標題",
              "body": TEXT, "body_sha256": hashlib.sha256(TEXT.encode()).hexdigest(),
              "authors_json": json.dumps([AUTHOR], ensure_ascii=False),
              "published_at_raw": OLD_CLOCK, "published_time_basis": "publisher_declared_clock_or_unknown",
              "captured_at_utc": CAPTURED, "extraction_method": "css:.articleContent__textContent",
              "raw_response_body": raw, "response_sha256": hashlib.sha256(raw).hexdigest()}
    record.update(changes)
    return record


def materialize(root: Path, record):
    path = root / "bodies/old.parquet"
    atomic_write_parquet(path, pa.Table.from_pylist([record]))
    atomic_write_json(root / "receipts/old.json", {"parquet_path": "bodies/old.parquet",
        "sha256": sha256_file(path), "rows": 1})
    return {"id": bytes.fromhex(record["article_id"]), "provider": record["provider"],
            "url": record["requested_url"], "state": "complete", "captured_at_utc": CAPTURED,
            "body_sha256": record["body_sha256"], "parquet_path": "bodies/old.parquet"}


def test_exact_retained_declaration_preserves_original_and_capture_clock(tmp_path):
    head = materialize(tmp_path, old_record())
    before = sha256_file(tmp_path / "bodies/old.parquet")
    ((article, document),) = corpus._load_body_input(
        (tmp_path, "bodies/old.parquet", "receipts/old.json", [head]))
    assert article["published_at_raw"] == OLD_CLOCK
    assert article["retained_published_declaration_raw"] == DECLARED
    assert article["retained_modified_declaration_raw"] == MODIFIED
    assert article["published_at_utc"] == datetime(2026, 10, 9, 6, 20, 23, tzinfo=timezone.utc)
    assert article["publication_clock_status"] == "source_timezone_aware_retained_declaration"
    assert article["published_time_basis"] == corpus.CMNEWS_RETAINED_DECLARATION_BASIS
    assert article["observed_at_utc"].isoformat() == CAPTURED
    assert article["available_at_utc"] == article["observed_at_utc"]
    assert article["historical_point_in_time"] is False
    assert article["source_sha256"] == before and document["text"] == TEXT
    assert sha256_file(tmp_path / "bodies/old.parquet") == before


@pytest.mark.parametrize("changes", [
    {"provider": "other"},
    {"final_url": "https://cmnews.com.tw/another"},
    {"source_request_url": "https://cmnews.com.tw/feed"},
    {"source_response_article_id": "another"},
    {"extraction_method": "different"},
    {"published_time_basis": "already_recorded"},
    {"published_at_raw": "2026-10-09 14:20"},
    {"published_at_raw": "2026-10-09 14:25 (更新：2026-10-09 14:20)"},
    {"title": "另一篇"},
    {"authors_json": json.dumps(["另一位作者"])},
    {"authors_json": "broken"},
    {"raw_response_body": b"changed without matching digest"},
    {"response_sha256": "0" * 64},
])
def test_unproven_identity_or_original_clock_is_not_inferred(changes):
    assert corpus.legacy_cmnews_publication_declaration(old_record(**changes)) is None


@pytest.mark.parametrize("raw", [
    html(declared="2026-10-09T14:20:23"),
    html(declared="2026-10-09T14:21:23+08:00"),
    html(modified="2026-10-09T14:26:00+08:00"),
    html(canonical="https://cmnews.com.tw/another"),
    html(paid=True),
])
def test_unmatched_clock_identity_or_access_is_not_promoted(raw):
    record = old_record(raw_response_body=raw, response_sha256=hashlib.sha256(raw).hexdigest())
    assert corpus.legacy_cmnews_publication_declaration(record) is None


@pytest.mark.parametrize("modified", [None, "2026-10-09T14:25:56"])
def test_missing_or_naive_update_does_not_prevent_publication_or_invent_timezone(modified):
    raw = html(modified=modified)
    record = old_record(raw_response_body=raw, response_sha256=hashlib.sha256(raw).hexdigest())
    assert corpus.legacy_cmnews_publication_declaration(record) == (DECLARED, None)


def test_non_candidate_never_decodes_original_response(tmp_path, monkeypatch):
    head = materialize(tmp_path, old_record(published_at_raw="2026-10-09 14:20"))
    original, projections = corpus.pq.ParquetFile, []

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
    assert article["retained_published_declaration_raw"] is None
    assert len(projections) == 1 and "raw_response_body" not in projections[0]


def test_new_corpus_and_old_schema_are_independently_readable_and_verifiable(tmp_path, monkeypatch):
    monkeypatch.setattr(corpus._NewsDatasetDiskGuard, "check", lambda self: None)
    source, output = source_fixture(tmp_path), tmp_path / "new_dataset"
    path = source / "bodies/current.parquet"
    old_rows = corpus.pq.read_table(path).to_pylist()
    record = old_record()
    atomic_write_parquet(path, pa.Table.from_pylist([record, old_rows[1]]))
    atomic_write_json(source / "receipts/current.json", {
        "parquet_path": "bodies/current.parquet", "sha256": sha256_file(path), "rows": 2})
    import sqlite3
    from test_news_training_dataset import URLS
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE articles SET id=?,url=?,provider=?,body_sha256=? WHERE id=?",
                   (url_article_id(URL), URL, "cmnews", record["body_sha256"], url_article_id(URLS[0])))
    before = sha256_file(path)
    corpus.build_news_dataset(source, output, workers=2)
    rows = [row for row in read_table(output, "articles") if row["article_id"] == url_article_id(URL).hex()]
    assert len(rows) == 1 and rows[0]["retained_published_declaration_raw"] == DECLARED
    assert rows[0]["retained_modified_declaration_raw"] == MODIFIED
    assert rows[0]["published_at_raw"] == OLD_CLOCK
    assert rows[0]["available_at_utc"].isoformat() == CAPTURED
    assert sha256_file(path) == before
    assert corpus.verify_news_dataset(output)["state"] == "verified"
    readme = (output / "README.md").read_text().splitlines()
    declaration_line = next(i for i, line in enumerate(readme) if line.startswith("| `retained_published_declaration_raw`"))
    assert readme[declaration_line + 1].startswith("| `publication_date`")

    legacy = tmp_path / "legacy_dataset"
    with monkeypatch.context() as context:
        context.setattr(corpus, "SCHEMAS", {**corpus.SCHEMAS, "articles": corpus.LEGACY_ARTICLE_SCHEMA})
        corpus.build_news_dataset(source, legacy, workers=2)
    manifest_path = legacy / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("article_schema_contract")
    atomic_write_json(manifest_path, manifest)
    verification = corpus.verify_news_dataset(legacy)
    assert verification["state"] == "verified"
    old_readme = corpus.render_news_dataset_readme(manifest, verification).splitlines()
    raw_line = next(i for i, line in enumerate(old_readme) if line.startswith("| `published_at_raw`"))
    assert old_readme[raw_line + 1].startswith("| `publication_date`")
    assert "retained_published_declaration_raw" not in next(corpus.iter_news_articles(legacy))


def test_unknown_article_schema_contract_is_rejected():
    with pytest.raises(ValueError, match="unrecognized news article schema contract"):
        corpus._schemas_for_manifest({"article_schema_contract": "not-a-supported-schema"})
