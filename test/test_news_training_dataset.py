"""Real SQLite/Parquet fixtures for the local news corpus, without collectors."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data import news_dataset as corpus
from stockagent.data.news_dataset_sources import source_metadata_record, url_article_id


CAPTURED = "2026-10-16T01:00:00+00:00"
URLS = [f"https://news.example.com/article/{i}" for i in range(4)]


def source_fixture(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    body = "原始新聞全文，保留空白及負值 -123。\n下一段。"
    digest = hashlib.sha256(body.encode()).hexdigest()
    records = [{"article_id": url_article_id(url).hex(), "provider": "example",
                "requested_url": url, "title": f"全文標題 {i}", "body": body,
                "body_sha256": digest, "published_at_raw": "2014-03-31T08:59:00+08:00",
                "captured_at_utc": CAPTURED, "rights_status": "not_cleared_for_reuse",
                "training_injection_enabled": False, "historical_point_in_time": False,
                "publish": False, "remote_training_export": False}
               for i, url in enumerate(URLS[:2])]
    parquet = root / "bodies/current.parquet"
    atomic_write_parquet(parquet, pa.Table.from_pylist(records))
    atomic_write_json(root / "receipts/current.json", {"parquet_path": "bodies/current.parquet",
                      "sha256": sha256_file(parquet), "rows": 2})
    # The accepted history contains an older revision too, which must not be
    # exported as another current article or read in place of the current head.
    old = {**records[0], "body": "舊版", "body_sha256": hashlib.sha256("舊版".encode()).hexdigest()}
    atomic_write_parquet(root / "bodies/old.parquet", pa.Table.from_pylist([old]))
    atomic_write_json(root / "receipts/old.json", {"parquet_path": "bodies/old.parquet",
                      "sha256": sha256_file(root / "bodies/old.parquet"), "rows": 1})
    db = sqlite3.connect(root / "queue.sqlite3")
    db.executescript("""
        CREATE TABLE articles(id BLOB PRIMARY KEY,url TEXT,provider TEXT,title TEXT,metadata_date_raw TEXT,
            first_seen_utc TEXT,last_seen_utc TEXT,captured_at_utc TEXT,body_sha256 TEXT,parquet_path TEXT,state TEXT);
        CREATE TABLE state_metrics(provider TEXT,state TEXT,articles INTEGER,body_bytes INTEGER);
        CREATE TABLE stored_body_metrics(provider TEXT,heads INTEGER);
        CREATE TABLE accepted_batches(path TEXT PRIMARY KEY,receipt_path TEXT);
        CREATE TABLE origins(id INTEGER PRIMARY KEY,provider TEXT,parquet_path TEXT,sha256 TEXT,rows INTEGER,
            receipt_path TEXT,imported_at_utc TEXT);
        CREATE TABLE metadata_ingest_progress(origin_id INTEGER PRIMARY KEY,state TEXT);
        CREATE TABLE metadata_feed_heads(provider TEXT,feed_url TEXT,receipt_path TEXT,rows INTEGER,checked_at_utc TEXT);
    """)
    db.executemany("INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
        (url_article_id(URLS[0]), URLS[0], "example", "全文標題 0", "2014-03-31", CAPTURED, CAPTURED,
         CAPTURED, digest, "bodies/current.parquet", "complete"),
        (url_article_id(URLS[1]), URLS[1], "example", "全文標題 1", "2014-03-31", CAPTURED, CAPTURED,
         CAPTURED, digest, "bodies/current.parquet", "retry"),
        (url_article_id(URLS[2]), URLS[2], "example", "只有標題", "2015-02-02", CAPTURED, CAPTURED,
         None, None, None, "pending"),
        (url_article_id(URLS[3]), URLS[3], "example", None, None, CAPTURED, CAPTURED,
         None, None, None, "pending"),
    ])
    db.execute("INSERT INTO state_metrics VALUES ('example','all',4,0)")
    db.execute("INSERT INTO stored_body_metrics VALUES ('example',2)")
    db.executemany("INSERT INTO accepted_batches VALUES (?,?)", [
        ("bodies/current.parquet", "receipts/current.json"), ("bodies/old.parquet", "receipts/old.json")])
    inputs = [
        (1, "finlab", [{"date": "2014-03-31", "url": URLS[0], "stock_ids": "0015,2330,2330",
                        "title": "來源標題", "key_date": "2026-02-26"}]),
        (2, "finmind", [{"date": "2014-03-31 08:59:00", "link": URLS[0], "stock_id": "2330", "title": "來源標題"},
                        {"date": "2014-03-31 08:59:00", "link": URLS[0], "stock_id": "2330", "title": "來源標題"},
                        {"date": "2015-02-02", "link": URLS[2], "stock_id": "0050", "title": "只有標題"},
                        {"date": "2015-02-02", "link": URLS[3], "stock_id": "9999", "title": None}]),
    ]
    for number, provider, rows in inputs:
        path = root / f"metadata/{provider}.parquet"
        atomic_write_parquet(path, pa.Table.from_pylist(rows))
        db.execute("INSERT INTO origins VALUES (?,?,?,?,?,?,?)", (number, provider, str(path), sha256_file(path),
                   len(rows), f"{provider}-receipt.json", CAPTURED))
        db.execute("INSERT INTO metadata_ingest_progress VALUES (?,'complete')", (number,))
    # A later, unfinished metadata revision is not the current complete input.
    db.execute("INSERT INTO origins VALUES (3,'finmind','not-readable','bad',100,'finmind-receipt.json',?)", (CAPTURED,))
    db.execute("INSERT INTO metadata_ingest_progress VALUES (3,'pending')")
    db.commit()
    db.close()
    return root


def read_table(root, name):
    manifest = json.loads((root / "manifest.json").read_text())
    result = []
    for part in manifest["parts"]:
        if part["table_name"] == name:
            result.extend(pq.read_table(corpus.news_part_path(root, manifest, part)).to_pylist())
    return result


def test_build_deduplicates_text_preserves_articles_links_clocks_and_raw_rows(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    before = sha256_file(source / "queue.sqlite3")
    result = corpus.build_news_dataset(source, output, workers=2, batch_rows=2)
    assert result["state"] == "verified"
    assert result["row_counts"] == {"articles": 3, "documents": 2, "metadata_observations": 5, "article_stocks": 4}
    assert sha256_file(source / "queue.sqlite3") == before
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["source_snapshot"]["counts"]["without_trainable_text"] == 1
    assert manifest["source_snapshot"]["counts"]["metadata_partitions"] == 2
    assert manifest["metadata_quality"]["stock_associations_without_text"] == 1
    assert manifest["training_injection_enabled"] is False
    articles = read_table(output, "articles")
    full = [r for r in articles if r["text_kind"] == "fulltext"]
    assert len(full) == 2 and len({r["document_id"] for r in full}) == 1
    assert any(r["queue_state"] == "retry" for r in full)
    assert all(r["available_at_utc"].year == 2026 and r["published_at_utc"].year == 2014 for r in full)
    assert all(r["rights_status"] == "not_cleared_for_reuse" and not r["historical_point_in_time"] for r in full)
    headline = next(r for r in articles if r["text_kind"] == "headline")
    assert headline["body"] is None
    assert all(r["body"] == "原始新聞全文，保留空白及負值 -123。\n下一段。" for r in full)
    assert headline["published_at_utc"] is None
    assert headline["publication_clock_status"] == "metadata_date_not_verified_publication"
    assert {r["stock_id"] for r in read_table(output, "article_stocks")} == {"0015", "2330", "0050"}
    observations = read_table(output, "metadata_observations")
    assert len([r for r in observations if r["metadata_provider"] == "finmind"]) == 4
    assert observations[-1]["resolved_article_id"] is None
    assert all(r["observed_at_utc"].year == 2026 for r in observations)
    assert len(list(corpus.iter_news_documents(output))) == 1
    assert len(list(corpus.iter_news_documents(output, text_kind="headline"))) == 1
    assert len(list(corpus.iter_news_articles(output))) == 3
    assert len(list(corpus.iter_news_articles(output, text_kind="fulltext"))) == 2
    assert next(corpus.iter_news_articles(output, text_kind="headline"))["body"] is None
    assert corpus.build_news_dataset(source, output)["generation"] == result["generation"]


def test_readme_reports_fixed_snapshot_counts_and_all_links_resolve(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    verified = corpus.build_news_dataset(source, output)
    manifest = json.loads((output / "manifest.json").read_text())
    readme = (output / "README.md").read_text()
    assert readme == corpus.render_news_dataset_readme(manifest, verified)
    assert manifest["generation"] in readme
    assert "| 新聞文章 | 3 |" in readme
    assert "| 有內文的文章 | 2 |" in readme
    assert "| 只有標題的文章 | 1 |" in readme
    assert "| 有標題的文章 | 3 |" in readme
    assert "| 精確去重文字 | 2 |" in readme
    assert "| 新聞與股票關聯 | 4 |" in readme
    assert "| 中繼資料觀測 | 5 |" in readme
    assert "| 有內文 | 2014-03-31 | 2014-03-31 | 1 |" in readme
    assert "| 只有標題 | 2015-02-02 | 2015-02-02 | 1 |" in readme
    assert "| `example` | 2 | 1 |" in readme
    assert "`example` 占 100.00%" in readme
    assert "| `finmind` | 4 |" in readme
    assert "**只讀 manifest 指向的版本與 `parts` 清單。" in readme
    assert "不要只選 `text_kind='headline'`" in readme
    assert "NULL`／Python `None`" in readme
    assert "沒有可用標題也沒有內文" in readme
    assert "<!--" not in readme
    assert readme.count("```") % 2 == 0
    for target in re.findall(r"\]\(([^)]+)\)", readme):
        assert (output / target).is_file(), target
    assert not any(line.rstrip() != line for line in readme.splitlines())


def test_readme_python_examples_read_titles_with_and_without_bodies(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output)
    readme = (output / "README.md").read_text()
    examples = re.findall(r"```python\n(.*?)\n```", readme, re.DOTALL)
    assert len(examples) == 3
    monkeypatch.chdir(tmp_path)
    namespaces = []
    for example in examples:
        namespace = {}
        exec(compile(example, "data_news/README.md", "exec"), namespace)
        namespaces.append(namespace)
    assert len(namespaces[1]["rows"]) == 3
    assert {row[2] for row in namespaces[1]["rows"]} == {"全文標題 0", "全文標題 1", "只有標題"}
    assert namespaces[2]["first_document"]["text_kind"] == "fulltext"


def test_refresh_failure_does_not_publish_unverified_generation(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    first = corpus.build_news_dataset(source, output)
    committed = (output / "manifest.json").read_bytes()
    verify = corpus._verify_manifest
    def reject(*args, **kwargs):
        raise ValueError("candidate rejected")
    monkeypatch.setattr(corpus, "_verify_manifest", reject)
    with pytest.raises(ValueError, match="candidate rejected"):
        corpus.build_news_dataset(source, output, refresh=True)
    assert (output / "manifest.json").read_bytes() == committed
    monkeypatch.setattr(corpus, "_verify_manifest", verify)
    resumed = corpus.build_news_dataset(source, output, refresh=True)
    assert resumed["generation"] != first["generation"]
    assert resumed["row_counts"] == first["row_counts"]


def test_verification_retry_keeps_original_data_identity_without_rebuilding(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    original_verify = corpus._verify_manifest
    def reject(*args, **kwargs):
        raise RuntimeError("verification resource failure")
    monkeypatch.setattr(corpus, "_verify_manifest", reject)
    with pytest.raises(RuntimeError, match="resource failure"):
        corpus.build_news_dataset(source, output)
    active = json.loads((output / ".active-build.json").read_text())
    generation = output / active["generation"]
    candidate = (generation / "manifest.json").read_bytes()
    manifest = json.loads(candidate)
    hashes = {p["path"]: sha256_file(corpus.news_part_path(output, manifest, p)) for p in manifest["parts"]}
    monkeypatch.setattr(corpus, "_verify_manifest", original_verify)
    monkeypatch.setattr(corpus, "builder_fingerprint", lambda: "new-verifier-not-data-builder")
    def cannot_rebuild(*args, **kwargs):
        raise AssertionError("verification retry must not rebuild source or data")
    monkeypatch.setattr(corpus, "_build_tables", cannot_rebuild)
    monkeypatch.setattr(corpus, "freeze_inputs", cannot_rebuild)
    with pytest.raises(ValueError, match="incompatible partial output"):
        corpus.build_news_dataset(source, output)
    result = corpus.finish_pending_news_dataset(output, verification_memory_mb=256)
    assert result["state"] == "verified"
    assert result["verifier_fingerprint"] == "new-verifier-not-data-builder"
    assert result["quality"]["memory_limit"] == "256MB"
    assert (output / "manifest.json").read_bytes() == candidate
    assert json.loads(candidate)["builder_fingerprint"] == active["builder_fingerprint"]
    assert hashes == {p["path"]: sha256_file(corpus.news_part_path(output, manifest, p)) for p in manifest["parts"]}
    assert not (output / ".active-build.json").exists()


def test_verification_retry_rejects_changed_parts_without_publishing(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    original_verify = corpus._verify_manifest
    def reject(*args, **kwargs):
        raise RuntimeError("verification resource failure")
    monkeypatch.setattr(corpus, "_verify_manifest", reject)
    with pytest.raises(RuntimeError, match="resource failure"):
        corpus.build_news_dataset(source, output)
    monkeypatch.setattr(corpus, "_verify_manifest", original_verify)
    active = json.loads((output / ".active-build.json").read_text())
    generation = output / active["generation"]
    manifest = json.loads((generation / "manifest.json").read_text())
    part = corpus.news_part_path(output, manifest, manifest["parts"][0])
    part.write_bytes(part.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="part checksum mismatch"):
        corpus.finish_pending_news_dataset(output)
    assert not (output / "manifest.json").exists()
    assert (output / ".active-build.json").exists()


def test_verification_memory_budget_rejected_before_any_build(tmp_path):
    output = tmp_path / "data_news"
    with pytest.raises(ValueError, match="at least 128 MB"):
        corpus.build_news_dataset(tmp_path / "source", output, verification_memory_mb=0)
    with pytest.raises(ValueError, match="at least 128 MB"):
        corpus.finish_pending_news_dataset(output, verification_memory_mb=-1)
    with pytest.raises(ValueError, match="at least 128 MB"):
        corpus.verify_news_dataset(output, verification_memory_mb=1)
    assert not output.exists()


def test_resume_replays_only_uncommitted_parts(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    original = corpus.CorpusWriter.checkpoint
    raised = []
    def fail_metadata(self, stage, cursor, rows):
        if stage == "metadata" and not raised:
            raised.append(True)
            raise RuntimeError("interrupted after part writes")
        return original(self, stage, cursor, rows)
    monkeypatch.setattr(corpus.CorpusWriter, "checkpoint", fail_metadata)
    with pytest.raises(RuntimeError, match="interrupted"):
        corpus.build_news_dataset(source, output, batch_rows=2)
    assert not (output / "manifest.json").exists()
    monkeypatch.setattr(corpus.CorpusWriter, "checkpoint", original)
    result = corpus.build_news_dataset(source, output, batch_rows=2)
    assert result["row_counts"]["metadata_observations"] == 5
    assert result["row_counts"]["article_stocks"] == 4


def test_source_or_output_corruption_is_rejected(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    receipt = source / "receipts/current.json"
    value = json.loads(receipt.read_text())
    receipt.write_text(json.dumps({**value, "sha256": "bad"}))
    with pytest.raises(ValueError, match="source checksum mismatch"):
        corpus.build_news_dataset(source, output)
    assert not (output / "manifest.json").exists()
    receipt.write_text(json.dumps(value))
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / "manifest.json").read_text())
    target = corpus.news_part_path(output, manifest, manifest["parts"][0])
    with target.open("ab") as stream:
        stream.write(b"corrupt")
    with pytest.raises(ValueError, match="part checksum mismatch"):
        corpus.verify_news_dataset(output)


def test_rss_description_remains_metadata_not_fulltext(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    items = [{"url": URLS[2], "title": "RSS 標題", "publication_raw": "2015-02-02T08:00:00+08:00",
              "description_html": "<p>這是摘要，不是新聞全文。</p>"}]
    path = source / "rss/latest.parquet"
    atomic_write_parquet(path, pa.Table.from_pylist([{"items_json": json.dumps(items, ensure_ascii=False)}]))
    atomic_write_json(source / "receipts/rss.json", {"parquet_path": "rss/latest.parquet", "sha256": sha256_file(path)})
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("INSERT INTO metadata_feed_heads VALUES ('example','https://news.example.com/rss', 'receipts/rss.json',1,?)",
                   (CAPTURED,))
    result = corpus.build_news_dataset(source, output)
    assert result["row_counts"]["metadata_observations"] == 6
    assert result["row_counts"]["documents"] == 2
    rss = next(r for r in read_table(output, "metadata_observations") if r["metadata_provider"] == "example_rss")
    assert rss["description_html"] == items[0]["description_html"]
    assert rss["resolved_article_id"] == url_article_id(URLS[2]).hex()
    assert not any("摘要" in r["text"] for r in read_table(output, "documents"))


def test_legacy_literal_google_metadata_links_to_retained_original_only(tmp_path):
    import base64
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    literal = URLS[0].encode()
    envelope = b"\x08\x13\x22" + bytes([len(literal)]) + literal
    alias = "https://news.google.com/rss/articles/" + base64.urlsafe_b64encode(envelope).decode().rstrip("=")
    path = source / "metadata/finmind.parquet"
    atomic_write_parquet(path, pa.Table.from_pylist([{"link": alias, "stock_id": "2330", "date": "2014-03-31",
                                                    "title": "原文連結"}]))
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE origins SET sha256=?,rows=1 WHERE id=2", (sha256_file(path),))
    corpus.build_news_dataset(source, output)
    observation = next(r for r in read_table(output, "metadata_observations") if r["metadata_provider"] == "finmind")
    assert observation["article_id"] == url_article_id(alias).hex()
    assert observation["resolved_article_id"] == url_article_id(URLS[0]).hex()
    association = next(r for r in read_table(output, "article_stocks") if r["metadata_provider"] == "finmind")
    assert association["article_id"] == url_article_id(URLS[0]).hex()


def test_inconsistent_source_counters_are_not_accepted_as_coverage(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE state_metrics SET articles=100")
    with pytest.raises(ValueError, match="counter disagrees"):
        corpus.build_news_dataset(source, output)
    assert not (output / "manifest.json").exists()


@pytest.mark.parametrize("field,value,message", [
    ("body", "被修改但未更新內容 SHA", "committed checksum"),
    ("provider", "another-provider", "provider/URL"),
    ("requested_url", "https://news.example.com/wrong", "provider/URL"),
    ("captured_at_utc", "2026-10-16 09:00:00", "observation clock"),
])
def test_invalid_body_evidence_is_not_silently_exported(tmp_path, field, value, message):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    path = source / "bodies/current.parquet"
    rows = pq.read_table(path).to_pylist()
    rows[0][field] = value
    atomic_write_parquet(path, pa.Table.from_pylist(rows))
    atomic_write_json(source / "receipts/current.json", {"parquet_path": "bodies/current.parquet",
                      "sha256": sha256_file(path), "rows": 2})
    with pytest.raises(ValueError, match=message):
        corpus.build_news_dataset(source, output)
    assert not (output / "manifest.json").exists()


def test_current_headline_and_body_title_do_not_backdate_later_metadata(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE articles SET first_seen_utc='2019-01-01T00:00:00+00:00'")
    path = source / "bodies/current.parquet"
    records = pq.read_table(path).to_pylist()
    records[0]["title"] = None
    atomic_write_parquet(path, pa.Table.from_pylist(records))
    atomic_write_json(source / "receipts/current.json", {"parquet_path": "bodies/current.parquet",
                      "sha256": sha256_file(path), "rows": 2})
    corpus.build_news_dataset(source, output)
    articles = read_table(output, "articles")
    headline = next(r for r in articles if r["text_kind"] == "headline")
    assert headline["available_at_utc"].year == 2026
    body = next(r for r in articles if r["article_id"] == url_article_id(URLS[0]).hex())
    assert body["title"] is None


def test_new_builder_can_reuse_only_immutable_snapshot_not_old_outputs(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / "manifest.json").read_text())
    frozen = output / manifest["generation"]
    # The new build must not acquire/copy a different production snapshot.
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE state_metrics SET articles=100")
    other = tmp_path / "other_corpus"
    result = corpus.build_news_dataset(source, other, snapshot_from=frozen)
    assert result["row_counts"] == manifest["row_counts"]
    reused = json.loads((other / "manifest.json").read_text())
    assert reused["source_snapshot"]["snapshot_sha256"] == manifest["source_snapshot"]["snapshot_sha256"]
    assert reused["source_snapshot"]["snapshot_reused_from"] == str(frozen)


def test_invalid_metadata_url_counts_the_unresolved_stock_association(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    path = source / "metadata/finmind.parquet"
    atomic_write_parquet(path, pa.Table.from_pylist([{"link": "not a URL", "stock_id": "0050", "title": "bad URL"}]))
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE origins SET sha256=?,rows=1 WHERE id=2", (sha256_file(path),))
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["metadata_quality"]["source_stock_associations"] == 3
    assert manifest["metadata_quality"]["stock_associations_without_text"] == 1
    assert manifest["metadata_quality"]["metadata_rows_invalid_or_missing_url"] == 1


def test_independent_readback_rejects_a_title_substituted_for_missing_body(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output)
    manifest = json.loads((output / "manifest.json").read_text())
    for part in manifest["parts"]:
        if part["table_name"] != "articles":
            continue
        path = corpus.news_part_path(output, manifest, part)
        rows = pq.read_table(path).to_pylist()
        if rows[0]["text_kind"] == "headline":
            rows[0]["body"] = rows[0]["title"]
            atomic_write_parquet(path, pa.Table.from_pylist(rows, schema=corpus.ARTICLE_SCHEMA))
            part["sha256"] = sha256_file(path)
            part["bytes"] = path.stat().st_size
            break
    atomic_write_json(output / "manifest.json", manifest)
    with pytest.raises(ValueError, match="headline_body_not_null"):
        corpus.verify_news_dataset(output)


@pytest.mark.parametrize("raw,status,aware", [
    ("2014-03-31T08:59:00+08:00", "source_timezone_aware", True),
    ("Mon, 31 Mar 2014 08:59:00 +0800", "source_timezone_aware", True),
    ("2014-03-31 08:59:00", "naive_timezone_unverified", False),
    ("2014-03-31", "date_only", False), (None, "missing", False), ("bad", "invalid", False),
])
def test_source_clock_does_not_invent_timezone(raw, status, aware):
    clock, _, result = corpus.source_clock(raw)
    assert result == status
    assert (clock is not None) is aware


def test_ids_preserve_leading_zeroes_and_text_kind_is_explicit():
    assert source_metadata_record({"url": URLS[0], "stock_ids": "0015,2330,2330"}, "finlab")[1] == ["0015", "2330"]
    with pytest.raises(ValueError, match="source strings"):
        source_metadata_record({"stock_id": 50}, "finmind")
    with pytest.raises(ValueError, match="text kind"):
        corpus.document_record("test", "summary")
    with pytest.raises(ValueError, match="empty news text"):
        corpus.document_record(" \n", "fulltext")


def test_incremental_unchanged_refresh_writes_no_parquet_and_reuses_exact_parts(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output, batch_rows=2)
    first = json.loads((output / "manifest.json").read_text())
    old_bytes = (output / first["generation"] / "manifest.json").read_bytes()
    old_paths = {p["path"] for p in first["parts"]}
    def cannot_write(*args, **kwargs):
        raise AssertionError("unchanged refresh must not serialize a complete Parquet copy")
    monkeypatch.setattr(corpus, "atomic_write_parquet", cannot_write)
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=3)
    second = json.loads((output / "manifest.json").read_text())
    assert second["generation"] != first["generation"]
    assert second["row_counts"] == first["row_counts"]
    assert {p["path"] for p in second["parts"]} == old_paths
    assert second["incremental_storage"]["written_parts"] == 0
    assert second["incremental_storage"]["reused_parts"] == len(first["parts"])
    assert (output / first["generation"] / "manifest.json").read_bytes() == old_bytes
    assert not list((output / second["generation"]).glob("*/*.parquet"))
    assert not (output / second["generation"] / "snapshot.sqlite3").exists()
    assert not (output / second["generation"] / "build.sqlite3").exists()
    assert second["source_snapshot"]["snapshot_storage"]["written_chunk_bytes"] == 0
    assert corpus.verify_news_dataset(output)["state"] == "verified"


def test_incremental_append_revise_remove_equals_a_fresh_complete_build(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output, batch_rows=2)
    first = json.loads((output / "manifest.json").read_text())
    old_article_paths = corpus.news_table_paths(output, "articles", manifest=first)
    old_hashes = {str(p): sha256_file(p) for p in old_article_paths}
    # Revise one title and introduce text for a URL that previously had none.
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE articles SET title='新的標題 -456',last_seen_utc='2026-10-16T02:00:00+00:00' WHERE id=?",
                   (url_article_id(URLS[2]),))
        db.execute("UPDATE articles SET title='新增可用標題' WHERE id=?", (url_article_id(URLS[3]),))
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=3)
    second = json.loads((output / "manifest.json").read_text())
    assert second["incremental_storage"]["reused_parts"] > 0
    assert second["incremental_storage"]["removed_or_revised_old_rows"] > 0
    assert second["row_counts"]["articles"] == 4
    assert old_hashes == {str(p): sha256_file(p) for p in old_article_paths}
    # A third revision removes the title-only article and its now-orphaned
    # document/link, while source metadata stays retained as an observation.
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE articles SET title=NULL WHERE id=?", (url_article_id(URLS[2]),))
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=1)
    reference = tmp_path / "fresh_reference"
    corpus.build_news_dataset(source, reference, batch_rows=4)
    from collections import Counter
    from stockagent.data.news_dataset_storage import row_digest
    for table in corpus.SCHEMAS:
        assert Counter(map(row_digest, read_table(output, table))) == Counter(map(row_digest, read_table(reference, table)))
    assert all(sha256_file(p) == old_hashes[str(p)] for p in old_article_paths)
    # Pinning the original manifest still exposes the original three articles.
    assert sum(pq.ParquetFile(p).metadata.num_rows for p in corpus.news_table_paths(output, "articles", manifest=first)) == 3


def test_incremental_bag_preserves_duplicate_source_observations(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output, batch_rows=2)
    metadata = source / "metadata/finmind.parquet"
    rows = pq.read_table(metadata).to_pylist()
    # Two identical source rows were present. Remove one, and re-add it in
    # another revision. No DISTINCT/last-write-wins is allowed on this table.
    atomic_write_parquet(metadata, pa.Table.from_pylist(rows[1:]))
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE origins SET sha256=?,rows=3 WHERE id=2", (sha256_file(metadata),))
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=3)
    assert len(read_table(output, "metadata_observations")) == 4
    atomic_write_parquet(metadata, pa.Table.from_pylist(rows))
    with sqlite3.connect(source / "queue.sqlite3") as db:
        db.execute("UPDATE origins SET sha256=?,rows=4 WHERE id=2", (sha256_file(metadata),))
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=1)
    assert len(read_table(output, "metadata_observations")) == 5
    assert len(read_table(output, "article_stocks")) == 4


def test_incremental_failure_resume_keeps_base_parts_unchanged(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output, batch_rows=2)
    before = (output / "manifest.json").read_bytes()
    original = corpus.CorpusWriter.checkpoint
    raised = []
    def fail(self, stage, cursor, rows):
        if stage == "metadata" and not raised:
            raised.append(True)
            raise RuntimeError("incremental uncommitted matching")
        return original(self, stage, cursor, rows)
    monkeypatch.setattr(corpus.CorpusWriter, "checkpoint", fail)
    with pytest.raises(RuntimeError, match="uncommitted matching"):
        corpus.build_news_dataset(source, output, refresh=True, batch_rows=1)
    assert (output / "manifest.json").read_bytes() == before
    monkeypatch.setattr(corpus.CorpusWriter, "checkpoint", original)
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=3)
    after = json.loads((output / "manifest.json").read_text())
    assert after["incremental_storage"]["written_part_bytes"] == 0
    assert after["row_counts"] == json.loads(before)["row_counts"]


def test_legacy_v1_manifest_is_a_valid_incremental_base_without_rewriting_it(tmp_path):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output, batch_rows=2)
    manifest = json.loads((output / "manifest.json").read_text())
    # Produce the precise legacy path convention, retaining original Parquet.
    from stockagent.data.news_dataset_snapshots import copy_snapshot
    generation = output / manifest["generation"]
    plan = manifest["source_snapshot"]
    copy_snapshot(output, generation, plan, generation / "snapshot.sqlite3")
    plan.pop("snapshot_storage")
    atomic_write_json(generation / "snapshot.json", plan)
    manifest["contract_version"] = 1
    manifest.pop("storage_layout")
    manifest.pop("incremental_storage")
    for part in manifest["parts"]:
        part["path"] = str(Path(part["path"]).relative_to(manifest["generation"]))
    atomic_write_json(output / "manifest.json", manifest)
    atomic_write_json(generation / "manifest.json", manifest)
    old = (output / "manifest.json").read_bytes()
    snapshot_sha = sha256_file(generation / "snapshot.sqlite3")
    assert corpus.verify_news_dataset(output)["state"] == "verified"
    corpus.build_news_dataset(source, output, refresh=True, batch_rows=1)
    current = json.loads((output / "manifest.json").read_text())
    assert current["contract_version"] == 2
    assert current["incremental_storage"]["written_parts"] == 0
    assert (generation / "manifest.json").read_bytes() == old
    assert sha256_file(generation / "snapshot.sqlite3") == snapshot_sha
    assert current["source_snapshot"]["snapshot_storage"]["written_chunk_bytes"] == 0
    assert all(p["path"] == f"{manifest['generation']}/snapshot.sqlite3"
               for p in current["source_snapshot"]["snapshot_storage"]["chunks"])
    assert current["row_counts"] == json.loads(old)["row_counts"]


def test_committed_cleanup_interruption_never_rebuilds_published_parts(tmp_path, monkeypatch):
    source, output = source_fixture(tmp_path), tmp_path / "data_news"
    corpus.build_news_dataset(source, output, batch_rows=2)
    cleanup = corpus.discard_new_build_work
    def interrupted(*args):
        cleanup(*args)
        raise RuntimeError("power loss after cleanup")
    monkeypatch.setattr(corpus, "discard_new_build_work", interrupted)
    with pytest.raises(RuntimeError, match="power loss"):
        corpus.build_news_dataset(source, output, refresh=True, batch_rows=1)
    published = json.loads((output / "manifest.json").read_text())
    hashes = {p["path"]: sha256_file(corpus.news_part_path(output, published, p)) for p in published["parts"]}
    assert (output / ".active-build.json").exists()
    assert not (output / published["generation"] / "build.sqlite3").exists()
    def cannot_build(*args, **kwargs):
        raise AssertionError("a committed generation cannot re-enter construction")
    monkeypatch.setattr(corpus, "_build_tables", cannot_build)
    monkeypatch.setattr(corpus, "discard_new_build_work", cleanup)
    result = corpus.build_news_dataset(source, output, refresh=True)
    assert result["generation"] == published["generation"]
    assert not (output / ".active-build.json").exists()
    assert hashes == {p["path"]: sha256_file(corpus.news_part_path(output, published, p)) for p in published["parts"]}
