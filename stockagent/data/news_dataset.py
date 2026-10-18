"""Resumable, source-bound, local-only news corpus in ordinary Arrow/Parquet.

No tokenization, invented labels, return targets, random temporal split, network
requests, collector writes, permission escalation or trainer injection occurs.
"""
from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, atomic_write_text, sha256_file
from stockagent.data.news_dataset_quality import audit_news_tables
from stockagent.data.news_dataset_storage import CopyOnWriteParts, LAYOUT, news_part_path, news_table_paths
from stockagent.data.news_dataset_snapshots import (
    copy_snapshot, discard_new_build_work, seal_snapshot, verify_snapshot,
)
from stockagent.data.news_dataset_sources import (
    checked_path, freeze_inputs, iter_body_groups, original_article_id, readonly_database,
    reuse_snapshot, source_metadata_record, url_article_id, utc_now,
)

CONTRACT_VERSION = 2
SUPPORTED_CONTRACTS = {1, CONTRACT_VERSION}
MIN_FREE_BYTES = 25 * 2**30
ARTICLE_SCHEMA = pa.schema([
    ("article_id", pa.string()), ("provider", pa.string()), ("url", pa.string()),
    ("title", pa.string()), ("body", pa.string()), ("document_id", pa.string()), ("text_kind", pa.string()),
    ("published_at_raw", pa.string()), ("published_at_utc", pa.timestamp("us", tz="UTC")),
    ("publication_date", pa.date32()), ("publication_clock_status", pa.string()),
    ("metadata_date_raw", pa.string()), ("published_time_basis", pa.string()),
    ("modified_at_raw", pa.string()), ("observed_at_utc", pa.timestamp("us", tz="UTC")),
    ("available_at_utc", pa.timestamp("us", tz="UTC")),
    ("authors_json", pa.string()), ("attribution_status", pa.string()),
    ("body_completeness", pa.string()), ("extraction_method", pa.string()),
    ("queue_state", pa.string()), ("source_parquet", pa.string()), ("source_row", pa.int64()),
    ("source_sha256", pa.string()), ("rights_status", pa.string()),
    ("collection_purpose", pa.string()), ("collection_basis", pa.string()),
    ("declared_license_urls_json", pa.string()), ("license_verification_basis", pa.string()),
    ("per_article_license_verified", pa.bool_()), ("fair_use_legally_determined", pa.bool_()),
    ("commercial_use_enabled", pa.bool_()),
    ("training_injection_enabled", pa.bool_()), ("historical_point_in_time", pa.bool_()),
    ("publish", pa.bool_()), ("remote_training_export", pa.bool_()),
])
DOCUMENT_SCHEMA = pa.schema([
    ("document_id", pa.string()), ("text_kind", pa.string()), ("text", pa.string()),
    ("text_sha256", pa.string()), ("text_bytes", pa.int64()),
    ("dedup_group_id", pa.string()),
])
LINK_SCHEMA = pa.schema([
    ("article_id", pa.string()), ("stock_id", pa.string()), ("metadata_provider", pa.string()),
])
OBSERVATION_SCHEMA = pa.schema([
    ("article_id", pa.string()), ("resolved_article_id", pa.string()),
    ("metadata_provider", pa.string()), ("source_parquet", pa.string()),
    ("source_sha256", pa.string()), ("source_row", pa.int64()), ("url_raw", pa.string()),
    ("title_raw", pa.string()), ("date_raw", pa.string()), ("stock_ids_raw", pa.string()),
    ("key_date_raw", pa.string()), ("description_html", pa.string()),
    ("observed_at_utc", pa.timestamp("us", tz="UTC")), ("observation_time_basis", pa.string()),
])
SCHEMAS = {"articles": ARTICLE_SCHEMA, "documents": DOCUMENT_SCHEMA,
           "article_stocks": LINK_SCHEMA, "metadata_observations": OBSERVATION_SCHEMA}
BODY_COLUMNS = {
    "article_id", "requested_url", "provider", "title", "body", "body_sha256",
    "published_at_raw", "modified_at_raw", "published_time_basis", "captured_at_utc",
    "authors_json", "attribution_status", "body_completeness", "extraction_method",
    "rights_status", "training_injection_enabled", "historical_point_in_time", "publish",
    "remote_training_export",
    "collection_purpose", "collection_basis", "declared_license_urls_json", "license_verification_basis",
    "per_article_license_verified", "fair_use_legally_determined", "commercial_use_enabled",
}


def builder_fingerprint() -> str:
    sources = [Path(__file__), Path(__file__).with_name("news_dataset_sources.py"),
               Path(__file__).with_name("news_dataset_quality.py"),
               Path(__file__).with_name("news_dataset_storage.py"),
               Path(__file__).with_name("news_dataset_snapshots.py")]
    return hashlib.sha256("".join(sha256_file(p) for p in sources).encode()).hexdigest()


def source_clock(value) -> tuple[datetime | None, date | None, str]:
    """A naive time is never silently assigned a timezone or market session."""
    if value is None or not str(value).strip():
        return None, None, "missing"
    raw = str(value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        try:
            return None, date.fromisoformat(raw), "date_only"
        except ValueError:
            return None, None, "invalid"
    try:
        stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            stamp = parsedate_to_datetime(raw)
        except (ValueError, TypeError, OverflowError):
            return None, None, "invalid"
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        return None, stamp.date(), "naive_timezone_unverified"
    return stamp.astimezone(timezone.utc), stamp.date(), "source_timezone_aware"


def _raw(value):
    return None if value is None else str(value)


def document_record(text: str, kind: str) -> dict:
    if kind not in {"fulltext", "headline"}:
        raise ValueError("news text kind must be fulltext or headline")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("empty news text must not become a training document")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return {"document_id": ("body-" if kind == "fulltext" else "title-") + digest,
            "text_kind": kind, "text": text, "text_sha256": digest,
            "text_bytes": len(text.encode("utf-8")), "dedup_group_id": digest}


def article_record(head: dict, document: dict, *, body=None, source_row=None, source_sha=None) -> dict:
    body = body or {}
    full = document["text_kind"] == "fulltext"
    published = body.get("published_at_raw")
    clock, day, status = source_clock(published)
    if not full:
        _, day, _ = source_clock(head.get("metadata_date_raw"))
        status = "metadata_date_not_verified_publication"
    observed_raw = body.get("captured_at_utc") or (head.get("captured_at_utc") if full else head.get("last_seen_utc"))
    observed, _, _ = source_clock(observed_raw)
    if observed is None:
        raise ValueError("an article requires a real, timezone-aware observation clock")
    return {
        "article_id": head["id"].hex(), "provider": head["provider"], "url": head["url"],
        # Do not mix a later queue title into an older captured body revision.
        "title": body.get("title") if full else head.get("title"),
        "body": document["text"] if full else None,
        "document_id": document["document_id"], "text_kind": document["text_kind"],
        "published_at_raw": _raw(published), "published_at_utc": clock, "publication_date": day,
        "publication_clock_status": status, "metadata_date_raw": head.get("metadata_date_raw"),
        "published_time_basis": body.get("published_time_basis"), "modified_at_raw": _raw(body.get("modified_at_raw")),
        "observed_at_utc": observed, "available_at_utc": observed,
        "authors_json": body.get("authors_json"), "attribution_status": body.get("attribution_status"),
        "body_completeness": body.get("body_completeness"), "extraction_method": body.get("extraction_method"),
        "queue_state": head["state"], "source_parquet": head.get("parquet_path") if full else "queue_snapshot",
        "source_row": source_row, "source_sha256": source_sha,
        "rights_status": body.get("rights_status") or "source_scope_not_attested_by_dataset",
        "collection_purpose": body.get("collection_purpose"), "collection_basis": body.get("collection_basis"),
        "declared_license_urls_json": body.get("declared_license_urls_json"),
        "license_verification_basis": body.get("license_verification_basis"),
        "per_article_license_verified": body.get("per_article_license_verified", False),
        "fair_use_legally_determined": body.get("fair_use_legally_determined", False),
        "commercial_use_enabled": body.get("commercial_use_enabled", False),
        "training_injection_enabled": body.get("training_injection_enabled", False),
        "historical_point_in_time": body.get("historical_point_in_time", False),
        "publish": body.get("publish", False), "remote_training_export": body.get("remote_training_export", False),
    }


def _load_body_input(item):
    root, relative, receipt_path, heads = item
    path = checked_path(root, relative)
    receipt = json.loads(checked_path(root, receipt_path).read_text())
    if receipt.get("parquet_path") != relative or sha256_file(path) != receipt.get("sha256"):
        raise ValueError(f"news source checksum mismatch: {relative}")
    original = pq.ParquetFile(path)
    if original.metadata.num_rows != receipt.get("rows"):
        raise ValueError(f"news source receipt row mismatch: {relative}")
    selected = {h["id"].hex(): h for h in heads}
    found, output, row_number = set(), [], 0
    columns = sorted(BODY_COLUMNS.intersection(original.schema_arrow.names))
    for batch in original.iter_batches(batch_size=2048, columns=columns, use_threads=False):
        for record in batch.to_pylist():
            head = selected.get(record.get("article_id"))
            if head is not None and record.get("body_sha256") == head["body_sha256"]:
                if record.get("provider") != head["provider"] or url_article_id(record["requested_url"]) != head["id"]:
                    raise ValueError("source body provider/URL disagrees with its frozen head")
                if record["article_id"] in found:
                    raise ValueError("duplicate current article in accepted source batch")
                document = document_record(record.get("body"), "fulltext")
                if document["text_sha256"] != head["body_sha256"]:
                    raise ValueError("source body text does not match its committed checksum")
                output.append((article_record(head, document, body=record, source_row=row_number,
                                              source_sha=receipt["sha256"]), document))
                found.add(record["article_id"])
            row_number += 1
    if found != set(selected):
        raise ValueError(f"current body heads absent from immutable batch: {relative}")
    return output


class CorpusWriter:
    """Commit durable Parquet parts and their indexes/cursors together.

    A crash before SQLite commit leaves only unaccepted deterministic part names;
    replay overwrites those parts, never an accepted generation or source file.
    """
    def __init__(self, root: Path, *, dataset_root: Path, parent=None, notify=None):
        self.root = root
        self.dataset_root = dataset_root
        self.db = sqlite3.connect(root / "build.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA cache_size=-65536")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS progress(stage TEXT PRIMARY KEY,cursor TEXT,rows INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS documents(id TEXT PRIMARY KEY,kind TEXT NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS article_ids(id BLOB PRIMARY KEY) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS links(article BLOB NOT NULL,stock TEXT NOT NULL,provider TEXT NOT NULL,
                PRIMARY KEY(article,stock,provider)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS parts(path TEXT PRIMARY KEY,table_name TEXT NOT NULL,rows INTEGER NOT NULL,
                sha256 TEXT NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS source_counts(provider TEXT NOT NULL,kind TEXT NOT NULL,rows INTEGER NOT NULL,
                PRIMARY KEY(provider,kind)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS metadata_counts(key TEXT PRIMARY KEY,rows INTEGER NOT NULL) WITHOUT ROWID;
        """)
        self.cow = (CopyOnWriteParts(self, dataset_root, parent, SCHEMAS, notify)
                    if parent is not None else None)

    def position(self, stage):
        row = self.db.execute("SELECT cursor,rows FROM progress WHERE stage=?", (stage,)).fetchone()
        return (row[0], row[1]) if row else ("", 0)

    def part(self, table, rows):
        if self.cow and rows:
            # Match typed, round-trippable values, not Python input spelling.
            rows = self.cow.fresh_rows(table, pa.Table.from_pylist(rows, schema=SCHEMAS[table]).to_pylist())
        self.write_part(table, rows)

    def write_part(self, table, rows):
        if not rows:
            return
        index = self.db.execute("SELECT count(*) FROM parts WHERE table_name=?", (table,)).fetchone()[0]
        relative = f"{table}/part-{index:06d}.parquet"
        path = self.root / relative
        atomic_write_parquet(path, pa.Table.from_pylist(rows, schema=SCHEMAS[table]),
                             compression="zstd", compression_level=6, row_group_size=8192, durable=True)
        os.chmod(path, 0o444)
        self.db.execute("INSERT INTO parts VALUES (?,?,?,?,?)", (str(path.relative_to(self.dataset_root)),
                        table, len(rows), sha256_file(path), path.stat().st_size))

    def records(self, records):
        articles, unique = [], {}
        for article, document in records:
            articles.append(article)
            unique.setdefault(document["document_id"], document)
        keys, existing = list(unique), set()
        for offset in range(0, len(keys), 500):
            chunk = keys[offset:offset + 500]
            existing.update(r[0] for r in self.db.execute(
                "SELECT id FROM documents WHERE id IN (" + ",".join("?" for _ in chunk) + ")", chunk))
        fresh = [v for key, v in unique.items() if key not in existing]
        self.part("documents", fresh)
        self.part("articles", articles)
        self.db.executemany("INSERT INTO documents VALUES (?,?,?)",
                            ((r["document_id"], r["text_kind"], r["text_bytes"]) for r in fresh))
        self.db.executemany("INSERT INTO article_ids VALUES (?)", ((bytes.fromhex(r["article_id"]),) for r in articles))
        counts = Counter((r["provider"], r["text_kind"]) for r in articles)
        self.db.executemany("INSERT INTO source_counts VALUES (?,?,?) ON CONFLICT(provider,kind) "
                            "DO UPDATE SET rows=rows+excluded.rows", ((p, k, n) for (p, k), n in counts.items()))

    def checkpoint(self, stage, cursor, rows):
        self.db.execute("INSERT INTO progress VALUES (?,?,?) ON CONFLICT(stage) "
                        "DO UPDATE SET cursor=excluded.cursor,rows=excluded.rows", (stage, str(cursor), rows))
        self.db.commit()

    def count(self, key, value=1):
        self.db.execute("INSERT INTO metadata_counts VALUES (?,?) ON CONFLICT(key) "
                        "DO UPDATE SET rows=rows+excluded.rows", (key, value))

    def close(self):
        self.db.close()


def _metadata_block(writer, snapshot, rows, origin):
    observations, links, absent, invalid = [], [], 0, 0
    from downloader.news_fulltext_policy import NewsError
    observed, _, _ = source_clock(origin["observed_at_utc"])
    if observed is None:
        raise ValueError("metadata requires a timezone-aware observation clock")
    # Resolve each distinct URL once per block, then use bounded indexed IN
    # queries. Stock-association rows frequently repeat the same article URL.
    identities = {}
    for _, row in rows:
        link, _ = source_metadata_record(row, origin["provider"])
        if link and link not in identities:
            try:
                aid = url_article_id(link)
                resolved = original_article_id(link) if "news.google.com/" in link else aid
                identities[link] = (aid, resolved)
            except NewsError:
                identities[link] = (None, None)
    candidates = list({identity for pair in identities.values() for identity in pair if identity})
    retained = set()
    for offset in range(0, len(candidates), 500):
        chunk = candidates[offset:offset + 500]
        retained.update(r[0] for r in snapshot.execute("SELECT id FROM heads WHERE id IN (" +
                        ",".join("?" for _ in chunk) + ") AND (body_sha256 IS NOT NULL OR "
                        "(title IS NOT NULL AND trim(title)!=''))", chunk))
    for number, row in rows:
        link, stocks = source_metadata_record(row, origin["provider"])
        aid, resolved = identities.get(link, (None, None))
        target = None
        if not aid:
            invalid += 1
            absent += len(stocks)
        if aid:
            # Original resolution is metadata identity only; require a retained
            # destination before associating its already accepted article.
            if resolved != aid and resolved in retained:
                target = resolved
            elif aid in retained:
                target = aid
            if target is not None:
                links.extend((target, stock, origin["provider"]) for stock in stocks)
            else:
                absent += len(stocks)
        observations.append({
            "article_id": aid.hex() if aid else None, "resolved_article_id": target.hex() if target else None,
            "metadata_provider": origin["provider"], "source_parquet": origin["parquet_path"],
            "source_sha256": origin["sha256"], "source_row": number, "url_raw": link,
            "title_raw": _raw(row.get("title")), "date_raw": _raw(row.get("date")),
            "stock_ids_raw": _raw(row.get("stock_ids") if origin["provider"] == "finlab" else row.get("stock_id")),
            "key_date_raw": _raw(row.get("key_date")), "description_html": None,
            "observed_at_utc": observed, "observation_time_basis": "source_metadata_imported_at_utc",
        })
    writer.part("metadata_observations", observations)
    writer.db.executemany("INSERT OR IGNORE INTO links VALUES (?,?,?)", links)
    writer.count("source_stock_associations", len(links) + absent)
    writer.count("stock_associations_without_text", absent)
    writer.count("metadata_rows_invalid_or_missing_url", invalid)


def _build_tables(root, source, snapshot_path, plan, *, workers, batch_rows, notify, parent=None):
    writer = CorpusWriter(root, dataset_root=root.parent.parent, parent=parent, notify=notify)
    try:
        with readonly_database(snapshot_path) as snapshot, ThreadPoolExecutor(max_workers=workers) as pool:
            after, done = writer.position("fulltext")
            for paths in iter_body_groups(snapshot, after=after, target_rows=batch_rows):
                inputs = []
                for relative in paths:
                    heads = [dict(r) for r in snapshot.execute("SELECT * FROM heads WHERE body_sha256 IS NOT NULL "
                                                              "AND parquet_path=? ORDER BY id", (relative,))]
                    receipt = snapshot.execute("SELECT receipt_path FROM batches WHERE path=?", (relative,)).fetchone()
                    if not receipt:
                        raise ValueError("body head has no accepted-batch receipt")
                    inputs.append((source, relative, receipt[0], heads))
                # map's unbounded submission would retain many decoded bodies.
                # Submit only one bounded wave of workers at a time.
                records = []
                for offset in range(0, len(inputs), workers):
                    for result in pool.map(_load_body_input, inputs[offset:offset + workers]):
                        records.extend(result)
                writer.records(records)
                done += len(records)
                writer.checkpoint("fulltext", paths[-1], done)
                notify("fulltext", done, plan["counts"]["fulltext_articles"])
            if done != plan["counts"]["fulltext_articles"]:
                raise ValueError("fulltext scope is incomplete")
            after, done = writer.position("headlines")
            cursor = snapshot.execute("SELECT * FROM heads WHERE body_sha256 IS NULL AND title IS NOT NULL "
                                      "AND trim(title)!='' AND id>? ORDER BY id", (bytes.fromhex(after) if after else b"",))
            while rows := cursor.fetchmany(batch_rows):
                records = []
                for head in rows:
                    head = dict(head)
                    document = document_record(head["title"], "headline")
                    records.append((article_record(head, document), document))
                writer.records(records)
                done += len(rows)
                writer.checkpoint("headlines", rows[-1]["id"].hex(), done)
                notify("headlines", done, plan["counts"]["headline_articles"])
            if done != plan["counts"]["headline_articles"]:
                raise ValueError("headline scope is incomplete")
            after, done = writer.position("metadata")
            for row in snapshot.execute("SELECT * FROM metadata_inputs WHERE origin_id>? ORDER BY origin_id", (int(after or 0),)):
                origin = dict(row)
                path = Path(origin["parquet_path"])
                if sha256_file(path) != origin["sha256"]:
                    raise ValueError("metadata input checksum mismatch")
                original = pq.ParquetFile(path)
                if original.metadata.num_rows != origin["rows"]:
                    raise ValueError("metadata input row mismatch")
                offset = 0
                for batch in original.iter_batches(batch_size=batch_rows, use_threads=False):
                    values = batch.to_pylist()
                    _metadata_block(writer, snapshot, list(enumerate(values, offset)), origin)
                    offset += len(values)
                done += offset
                writer.checkpoint("metadata", origin["origin_id"], done)
                notify("metadata", done, plan["counts"]["metadata_rows"])
            if done != plan["counts"]["metadata_rows"]:
                raise ValueError("metadata scope is incomplete")
            after, done = writer.position("rss")
            for row in snapshot.execute("SELECT * FROM feed_inputs ORDER BY provider,feed_url"):
                key = row["provider"] + "\0" + row["feed_url"]
                if key <= after:
                    continue
                receipt = json.loads(checked_path(source, row["receipt_path"]).read_text())
                path = checked_path(source, receipt["parquet_path"])
                if sha256_file(path) != receipt["sha256"]:
                    raise ValueError("RSS metadata checksum mismatch")
                table = pq.read_table(path)
                items = json.loads(table["items_json"][0].as_py())
                if len(items) != row["rows"]:
                    raise ValueError("RSS metadata row mismatch")
                observations = []
                observed, _, _ = source_clock(row["checked_at_utc"])
                if observed is None:
                    raise ValueError("RSS metadata requires an observation clock")
                for number, item in enumerate(items):
                    identity = url_article_id(item["url"])
                    retained = writer.db.execute("SELECT 1 FROM article_ids WHERE id=?", (identity,)).fetchone()
                    aid = identity.hex()
                    observations.append({"article_id": aid, "resolved_article_id": aid if retained else None,
                        "metadata_provider": row["provider"] + "_rss", "source_parquet": receipt["parquet_path"],
                        "source_sha256": receipt["sha256"], "source_row": number, "url_raw": item["url"],
                        "title_raw": item["title"], "date_raw": item["publication_raw"],
                        "stock_ids_raw": None, "key_date_raw": None, "description_html": item["description_html"],
                        "observed_at_utc": observed, "observation_time_basis": "rss_head_checked_at_utc"})
                writer.part("metadata_observations", observations)
                done += len(items)
                writer.checkpoint("rss", key, done)
                notify("rss", done, plan["counts"]["rss_items"])
            if done != plan["counts"]["rss_items"]:
                raise ValueError("RSS scope is incomplete")
            if not writer.position("links")[0]:
                cursor = writer.db.execute("SELECT article,stock,provider FROM links ORDER BY article,stock,provider")
                while rows := cursor.fetchmany(batch_rows):
                    writer.part("article_stocks", [{"article_id": r[0].hex(), "stock_id": r[1],
                                                    "metadata_provider": r[2]} for r in rows])
                writer.checkpoint("links", "complete", writer.db.execute("SELECT count(*) FROM links").fetchone()[0])
        article_count = writer.db.execute("SELECT count(*) FROM article_ids").fetchone()[0]
        orphan_links = writer.db.execute("SELECT count(*) FROM links l LEFT JOIN article_ids a ON a.id=l.article "
                                        "WHERE a.id IS NULL").fetchone()[0]
        if orphan_links:
            raise ValueError("stock association references an article without retained text")
        if writer.cow:
            writer.cow.finalize()
        parts = [dict(r) for r in writer.db.execute("SELECT * FROM parts ORDER BY table_name,path")]
        return {"parts": parts, "documents_by_kind": [dict(r) for r in writer.db.execute(
                    "SELECT kind,count(*) documents,sum(bytes) text_bytes FROM documents GROUP BY kind")],
                "sources": [dict(r) for r in writer.db.execute("SELECT * FROM source_counts ORDER BY provider,kind")],
                "metadata_quality": dict(writer.db.execute("SELECT key,rows FROM metadata_counts")),
                "incremental_storage": (writer.cow.statistics(parts) if writer.cow else {
                    "policy": LAYOUT, "base_generation": None, "reused_parts": 0, "reused_part_bytes": 0,
                    "written_parts": len(parts), "written_part_bytes": sum(p["bytes"] for p in parts)}),
                "build_checks": {"unique_article_ids": article_count, "orphan_stock_links": orphan_links,
                                 "document_identity": "sha256 of unmodified UTF-8 text and explicit kind"}}
    finally:
        writer.close()


def verify_news_dataset(root: Path, *, deep: bool = True, verification_memory_mb: int = 2048) -> dict:
    """Read the committed manifest, checksum every part and inspect actual rows."""
    if verification_memory_mb < 128:
        raise ValueError("news verification memory limit must be at least 128 MB")
    manifest = json.loads((root / "manifest.json").read_text())
    return _verify_manifest(root, manifest, deep=deep, verification_memory_mb=verification_memory_mb)


def _verify_manifest(root: Path, manifest: dict, *, deep: bool = True, notify=None,
                     verification_memory_mb: int = 2048) -> dict:
    """Validate an unpublished candidate without changing the readers' pointer."""
    if manifest["contract_version"] not in SUPPORTED_CONTRACTS or manifest["state"] != "ready":
        raise ValueError("unaccepted news dataset contract")
    generation = checked_path(root, manifest["generation"] + "/generation.json").parent
    verify_snapshot(root, generation, manifest["source_snapshot"])
    counts, checked_bytes, paths = Counter(), 0, set()
    for number, part in enumerate(manifest["parts"], 1):
        if part["path"] in paths:
            raise ValueError("duplicate news part in manifest")
        paths.add(part["path"])
        path = news_part_path(root, manifest, part)
        if sha256_file(path) != part["sha256"]:
            raise ValueError("news dataset part checksum mismatch")
        original = pq.ParquetFile(path)
        if original.metadata.num_rows != part["rows"] or original.schema_arrow != SCHEMAS[part["table_name"]]:
            raise ValueError("news dataset schema/row mismatch")
        if deep and part["table_name"] == "documents":
            for batch in original.iter_batches(batch_size=2048):
                for record in batch.to_pylist():
                    expected = document_record(record["text"], record["text_kind"])
                    if record != expected:
                        raise ValueError("news document content identity mismatch")
        counts[part["table_name"]] += part["rows"]
        checked_bytes += path.stat().st_size
        if notify and (number % 64 == 0 or number == len(manifest["parts"])):
            notify("verification", number, len(manifest["parts"]))
    if dict(counts) != manifest["row_counts"]:
        raise ValueError("news dataset manifest totals disagree with actual Parquet")
    if manifest["build_checks"]["unique_article_ids"] != counts["articles"] or manifest["build_checks"]["orphan_stock_links"]:
        raise ValueError("news article identity or stock-link check failed")
    if sum(r["documents"] for r in manifest["documents_by_kind"]) != counts["documents"]:
        raise ValueError("news unique document totals disagree")
    if notify:
        notify("relational_validation", 0, 1)
    quality = audit_news_tables(root, manifest, SCHEMAS, memory_limit_mb=verification_memory_mb,
                               notify=notify)
    if notify:
        notify("relational_validation", 1, 1)
    return {"state": "verified", "generation": manifest["generation"], "row_counts": dict(counts),
            "parts_verified": len(manifest["parts"]), "bytes_verified": checked_bytes,
            "document_hashes_verified": deep, "build_checks": manifest["build_checks"],
            "verifier_fingerprint": builder_fingerprint(),
            "quality": quality,
            "manifest_content_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
            "verified_at_utc": utc_now()}


def build_news_dataset(source: Path, output: Path, *, workers: int = 4, batch_rows: int = 16384,
                       refresh: bool = False, snapshot_from: Path | None = None, notify=None,
                       verification_memory_mb: int = 2048) -> dict:
    if workers < 1 or batch_rows < 1:
        raise ValueError("workers and batch_rows must be positive")
    if verification_memory_mb < 128:
        raise ValueError("news verification memory limit must be at least 128 MB")
    source, output = source.resolve(), output.resolve()
    if source == output or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("news dataset output must be separate from its source")
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output, 0o700)
    notify_callback = notify
    started = time.monotonic()
    def progress(stage, completed, total):
        if shutil.disk_usage(output).free < MIN_FREE_BYTES:
            raise RuntimeError("news dataset disk guard: retain 25 GiB free; resume after capacity recovers")
        payload = {"stage": stage, "completed": completed, "total": total,
                   "elapsed_seconds": round(time.monotonic() - started, 3), "updated_at_utc": utc_now()}
        atomic_write_json(output / "progress.json", payload)
        if notify_callback:
            notify_callback(payload)
    with (output / ".build.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (output / "manifest.json").is_file() and not refresh:
            return verify_news_dataset(output, verification_memory_mb=verification_memory_mb)
        fingerprint = builder_fingerprint()
        active_path = output / ".active-build.json"
        if active_path.exists():
            active = json.loads(active_path.read_text())
            # A crash after pointer publication/scratch cleanup must only retry
            # acceptance. Reconstructing tables here could overwrite parts that
            # readers already reference from this very generation.
            if (output / "manifest.json").is_file():
                published = json.loads((output / "manifest.json").read_text())
                if published.get("generation") == active["generation"]:
                    generation = checked_path(output, active["generation"] + "/generation.json").parent
                    if (json.loads((generation / "manifest.json").read_text()) != published
                            or any(published.get(key) != value for key, value in active.items())):
                        raise ValueError("committed news recovery binding mismatch")
                    verification = _verify_manifest(output, published, notify=progress,
                                                    verification_memory_mb=verification_memory_mb)
                    _publish_verified(output, generation, published, verification)
                    progress("ready", published["row_counts"]["articles"], published["row_counts"]["articles"])
                    return verification
            if active["builder_fingerprint"] != fingerprint:
                raise ValueError("news builder changed; do not resume incompatible partial output")
            generation = checked_path(output, active["generation"] + "/generation.json").parent
            plan = json.loads((generation / "snapshot.json").read_text())
            verify_snapshot(output, generation, plan)
            snapshot = generation / "snapshot.sqlite3"
            if not snapshot.exists():
                copy_snapshot(output, generation, plan, snapshot)
            if sha256_file(snapshot) != plan["snapshot_sha256"]:
                raise ValueError("frozen news input snapshot changed")
        else:
            name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
            relative = "versions/" + name
            generation = output / relative
            progress("snapshot", 0, 0)
            plan = (reuse_snapshot(source, generation, snapshot_from.resolve()) if snapshot_from is not None
                    else freeze_inputs(source, generation, progress=progress))
            active = {"generation": relative, "builder_fingerprint": fingerprint}
            if (output / "manifest.json").is_file():
                parent_bytes = (output / "manifest.json").read_bytes()
                parent = json.loads(parent_bytes)
                if parent.get("contract_version") not in SUPPORTED_CONTRACTS or parent.get("state") != "ready":
                    raise ValueError("incremental news base is not an accepted release")
                if parent["source_snapshot"]["source_root"] != str(source):
                    raise ValueError("incremental news base belongs to another source")
                atomic_write_text(generation / "base_manifest.json", parent_bytes.decode(), durable=True)
                active["base_manifest_sha256"] = hashlib.sha256(parent_bytes).hexdigest()
            atomic_write_json(generation / "generation.json", {**active, "contract_version": CONTRACT_VERSION})
            atomic_write_json(active_path, active)
        if plan["source_root"] != str(source):
            raise ValueError("cannot resume news build from a different source root")
        parent = None
        if active.get("base_manifest_sha256"):
            if sha256_file(generation / "base_manifest.json") != active["base_manifest_sha256"]:
                raise ValueError("incremental news base binding changed")
            parent = json.loads((generation / "base_manifest.json").read_text())
        result = _build_tables(generation, source, generation / "snapshot.sqlite3", plan,
                               workers=workers, batch_rows=batch_rows, notify=progress, parent=parent)
        plan = seal_snapshot(output, generation, plan, parent)
        atomic_write_json(generation / "snapshot.json", plan)
        counts = Counter()
        for part in result["parts"]:
            counts[part["table_name"]] += part["rows"]
        expected_articles = plan["counts"]["fulltext_articles"] + plan["counts"]["headline_articles"]
        if counts["articles"] != expected_articles:
            raise ValueError("not every retained trainable article was materialized")
        manifest = {"contract_version": CONTRACT_VERSION, "storage_layout": LAYOUT, "state": "ready", **active,
                    "built_at_utc": utc_now(), "source_snapshot": plan, "row_counts": dict(counts), **result,
                    "source_full_checksums_verified": True,
                    "source_checksum_scope": "selected immutable body/metadata/RSS Parquet files plus frozen queue projection; not the live SQLite DB",
                    "split_policy": "not_assigned; group by dedup_group_id and choose task-specific causal cutoff",
                    "labels": "not_generated; prediction target and horizon not specified",
                    "content_policy": "fulltext and headline remain separate; raw strings are not transformed",
                    "availability_policy": "current captured revision becomes available at its actual observation, not backdated publication",
                    "all_history_complete": False, "publish": False, "remote_training_export": False,
                    "training_injection_enabled": False, "scope": "local_research_corpus_not_license_attestation"}
        atomic_write_json(generation / "manifest.json", manifest)
        # Verification precedes the moving pointer: readers keep the old ready
        # generation if construction or complete independent readback fails.
        verification = _verify_manifest(output, manifest, notify=progress,
                                        verification_memory_mb=verification_memory_mb)
        _publish_verified(output, generation, manifest, verification)
        progress("ready", expected_articles, expected_articles)
        return verification


def _publish_verified(output, generation, manifest, verification):
    """Move the readers' pointer only after the complete independent readback."""
    atomic_write_json(generation / "verification.json", verification)
    atomic_write_json(generation / "quality_report.json", verification["quality"])
    atomic_write_json(output / "manifest.json", manifest)
    atomic_write_json(output / "verification.json", verification)
    atomic_write_json(output / "quality_report.json", verification["quality"])
    atomic_write_json(output / "schema.json", {name: [{"name": f.name, "type": str(f.type)} for f in schema]
                                               for name, schema in SCHEMAS.items()})
    atomic_write_text(output / "README.md", render_news_dataset_readme(manifest, verification), durable=True)
    discard_new_build_work(output, generation, manifest)
    (output / ".active-build.json").unlink()


def finish_pending_news_dataset(output: Path, *, verification_memory_mb: int = 2048, notify=None) -> dict:
    """Retry only verification of a fully written candidate, never data resume.

    A changed verifier may validate already immutable output. Keep the original
    data-builder fingerprint unchanged and separately record the new verifier;
    do not retag old parts or bypass the builder's incompatible-resume guard.
    """
    if verification_memory_mb < 128:
        raise ValueError("news verification memory limit must be at least 128 MB")
    output = output.resolve()
    started = time.monotonic()
    def progress(stage, completed, total):
        if shutil.disk_usage(output).free < MIN_FREE_BYTES:
            raise RuntimeError("news verification disk guard: retain 25 GiB free")
        payload = {"stage": stage, "completed": completed, "total": total,
                   "elapsed_seconds": round(time.monotonic() - started, 3), "updated_at_utc": utc_now()}
        atomic_write_json(output / "progress.json", payload)
        if notify:
            notify(payload)
    with (output / ".build.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        active_path = output / ".active-build.json"
        if not active_path.exists():
            return verify_news_dataset(output, verification_memory_mb=verification_memory_mb)
        active = json.loads(active_path.read_text())
        generation = checked_path(output, active["generation"] + "/generation.json").parent
        marker = json.loads((generation / "generation.json").read_text())
        manifest = json.loads((generation / "manifest.json").read_text())
        plan = json.loads((generation / "snapshot.json").read_text())
        if any(manifest.get(key) != value or marker.get(key) != value for key, value in active.items()):
            raise ValueError("news candidate does not match its original build identity")
        if manifest["source_snapshot"] != plan:
            raise ValueError("frozen news candidate source changed")
        verify_snapshot(output, generation, plan)
        expected = plan["counts"]["fulltext_articles"] + plan["counts"]["headline_articles"]
        if manifest["row_counts"].get("articles") != expected or manifest["row_counts"].get("metadata_observations", 0) != (
                plan["counts"]["metadata_rows"] + plan["counts"]["rss_items"]):
            raise ValueError("news candidate does not cover its frozen source scope")
        verification = _verify_manifest(output, manifest, notify=progress,
                                        verification_memory_mb=verification_memory_mb)
        _publish_verified(output, generation, manifest, verification)
        progress("ready", expected, expected)
        return verification


def iter_news_documents(root: Path, *, text_kind: str = "fulltext", batch_size: int = 1024):
    """Yield a deduplicated corpus without loading it all into RAM.

    Training permission and temporal eligibility are article-level evidence;
    this reader neither grants those rights nor injects a model's features.
    """
    if text_kind not in {"fulltext", "headline"}:
        raise ValueError("choose fulltext or headline explicitly")
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("state") != "ready" or manifest.get("contract_version") not in SUPPORTED_CONTRACTS:
        raise ValueError("news dataset is not ready")
    generation = checked_path(root, manifest["generation"] + "/generation.json").parent
    for part in manifest["parts"]:
        if part["table_name"] == "documents":
            original = pq.ParquetFile(news_part_path(root, manifest, part))
            for batch in original.iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    if row["text_kind"] == text_kind:
                        yield row


def iter_news_articles(root: Path, *, text_kind: str | None = None, batch_size: int = 1024):
    """Yield article examples with independent title and nullable body fields.

    No-body articles remain useful for title tasks. Never replace a missing
    body with a title, RSS description, empty string, or synthesized text.
    """
    if text_kind not in {None, "fulltext", "headline"}:
        raise ValueError("choose fulltext, headline, or all articles explicitly")
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("state") != "ready" or manifest.get("contract_version") not in SUPPORTED_CONTRACTS:
        raise ValueError("news dataset is not ready")
    generation = checked_path(root, manifest["generation"] + "/generation.json").parent
    for part in manifest["parts"]:
        if part["table_name"] == "articles":
            original = pq.ParquetFile(news_part_path(root, manifest, part))
            for batch in original.iter_batches(batch_size=batch_size):
                for row in batch.to_pylist():
                    if text_kind is None or row["text_kind"] == text_kind:
                        yield row


def render_news_dataset_readme(manifest: dict, verification: dict) -> str:
    """Explain a fixed snapshot using its existing receipts, without reading data."""
    def stamp(value):
        return datetime.fromisoformat(value).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    counts = manifest["row_counts"]
    source_counts = manifest["source_snapshot"]["counts"]
    profiles = verification["quality"]["profile"]
    kinds = {row["text_kind"]: row for row in profiles["text_kinds"]}
    missing_titles = sum(row["missing_title"] for row in kinds.values())
    snapshot = [
        f"- 版本：`{manifest['generation']}`。",
        f"- 來源快照時間：{stamp(manifest['source_snapshot']['captured_at_utc'])}。",
        f"- 資料建置完成：{stamp(manifest['built_at_utc'])}。",
        f"- 完整讀回驗證：{stamp(verification['verified_at_utc'])}；台灣時間為 UTC 加 8 小時。",
        "", "| 項目 | 數量 | 代表什麼 |", "| --- | ---: | --- |",
        f"| 新聞文章 | {counts['articles']:,} | 以網址辨識的文章，不是事件數 |",
        f"| 有內文的文章 | {source_counts['fulltext_articles']:,} | `body` 有值 |",
        f"| 只有標題的文章 | {source_counts['headline_articles']:,} | `body` 是真正的 NULL |",
        f"| 有標題的文章 | {counts['articles'] - missing_titles:,} | 非 NULL／非空字串；訓練時再排除純空白 |",
        f"| 精確去重文字 | {counts['documents']:,} | 全文或標題原字串的不同版本，不是文章數 |",
        f"| 新聞與股票關聯 | {counts['article_stocks']:,} | 同篇新聞可連到多檔股票及多個中繼資料來源 |",
        f"| 中繼資料觀測 | {counts['metadata_observations']:,} | 來源列，不可再加到文章總數 |",
        f"| Parquet 檔案 | {len(manifest['parts']):,} | 僅目前版本的四張資料表 |",
        f"| Parquet 大小 | {verification['bytes_verified'] / 10**9:.2f} GB / "
        f"{verification['bytes_verified'] / 2**30:.2f} GiB | 不含快照索引、舊版本與原始新聞庫 |",
        "", "| 文字類型 | 最早來源日期 | 最新來源日期 | 精確去重文字數 |",
        "| --- | --- | --- | ---: |",
    ]
    for kind, label in (("fulltext", "有內文"), ("headline", "只有標題")):
        row = kinds.get(kind, {})
        snapshot.append(f"| {label} | {row.get('earliest_source_date') or '無'} | "
                        f"{row.get('latest_source_date') or '無'} | {row.get('unique_documents', 0):,} |")

    providers = {}
    for row in manifest["sources"]:
        providers.setdefault(row["provider"], {})[row["kind"]] = row["rows"]
    body_providers = sum(bool(row.get("fulltext")) for row in providers.values())
    provider_rows = [f"本版共有 {len(providers):,} 個媒體／網址來源識別碼，其中 "
                     f"{body_providers:,} 個有已取得的內文。識別碼含 `unregistered:<host>`；",
                     "這不是相同數量的下載器，也不表示每個來源都有全文或完整歷史。", "",
                     "以下只列有內文數最多的 8 個來源及無內文數最多的 8 個來源，方便認識組成；",
                     "不是刪減資料範圍。完整來源與各類筆數在 `manifest.json` 的 `sources`。", "",
                     "| 來源識別碼 | 有內文文章 | 只有標題文章 |", "| --- | ---: | ---: |"]
    for has_body in (True, False):
        candidates = [(key, row) for key, row in providers.items() if bool(row.get("fulltext")) == has_body]
        for key, row in sorted(candidates, key=lambda item: (-item[1].get("fulltext" if has_body else "headline", 0), item[0]))[:8]:
            provider_rows.append(f"| `{key}` | {row.get('fulltext', 0):,} | {row.get('headline', 0):,} |")
    if body_providers:
        key, largest = max(providers.items(), key=lambda item: item[1].get("fulltext", 0))
        share = largest["fulltext"] / source_counts["fulltext_articles"]
        provider_rows.extend(["", f"有內文文章中，`{key}` 占 {share:.2%}；文字量並非跨媒體平均分布。",
                              "若評估跨媒體泛化能力，要另外檢查來源分布，不能只看整體樣本數。"])
    provider_rows.extend(["", "`cnyes` 是 Anue 鉅亨。FinMind、FinLab 則是新聞清單與股票關聯的匯入來源，",
                          "不代表文章由它們撰寫；來源網站與轉載作者也不一定相同。", "",
                          "| 中繼資料匯入來源 | 來源列數 |", "| --- | ---: |"])
    for row in profiles["unresolved_metadata"]:
        provider_rows.append(f"| `{row['metadata_provider']}` | {row['observation_rows']:,} |")

    unresolved = sum(row["without_retained_article"] for row in profiles["unresolved_metadata"])
    quality_notes = (
        f"- 已知網址共 {source_counts['known_urls']:,} 個；其中 {source_counts['without_trainable_text']:,} 個"
        "沒有可用標題也沒有內文，未建立文字樣本，仍留在來源快照。\n"
        f"- 有內文但標題缺少或為空字串共 {kinds.get('fulltext', {}).get('missing_title', 0):,} 篇；"
        "保留原樣，不拿較晚觀測到的標題補進舊內文版本。\n"
        f"- 有內文但缺少可用的含時區發布時間共 "
        f"{kinds.get('fulltext', {}).get('without_aware_publication', 0):,} 篇；"
        "文字仍保留，不能當作精確盤前／盤後樣本。\n"
        f"- 中繼資料有 {unresolved:,} 列無法連到保留的文章；仍保存來源列，"
        "`resolved_article_id` 為 NULL，不虛構文章或股票關聯。"
    )
    storage = manifest.get("incremental_storage")
    if storage:
        storage_notes = (
            f"本版採增量分區儲存，基準版本：`{storage.get('base_generation') or '首次基準'}`。\n\n"
            f"沿用 {storage['reused_parts']:,} 個分區、{storage['reused_part_bytes']:,} bytes；"
            f"本版新寫 {storage['written_parts']:,} 個分區、{storage['written_part_bytes']:,} bytes。\n\n"
            "上方 Parquet 大小是本版可見資料的邏輯總量，不是這次新增占用；"
            "來源 SQLite 投影也按固定 byte chunks 保存，只新增變動的塊。"
        )
    else:
        storage_notes = ("這是既有全量基準版；目前版本與數字沒有被改寫。新版建置程式的 `--refresh` "
                         "已改為共用不變分區、只寫新增與受影響分區；不是再複製一份全庫。")
    return (_DATASET_README.replace("<!-- snapshot -->", "\n".join(snapshot))
            .replace("<!-- providers -->", "\n".join(provider_rows))
            .replace("<!-- quality -->", quality_notes)
            .replace("<!-- storage -->", storage_notes))


_DATASET_README = r"""# data_news：新聞標題與全文研究資料集

這份資料集把本機已取得的新聞，整理成可以分批讀取、追溯來源的 Parquet 表格。
你可以只研究標題，也可以使用已取得的內文，或把新聞與股票關聯接起來研究。
同一筆文章同時保留 `title` 與 `body`；沒有內文時，`body` 就是空值，不會拿標題或摘要代替。

理解它只需要先分清楚三件事：**一個網址不等於一個獨立事件、知道發布日不等於當時已取得、
有資料不等於歷史已經抓齊。** 這三個差別會影響樣本數、去重方式與金融回測是否偷看未來。

## 目前有哪些資料？

以下數字來自這個固定版本的 [manifest](manifest.json) 與 [驗證收據](verification.json)，
不是下載服務的即時進度。新聞庫之後新增的資料，要建立新版本才會出現在這裡。

<!-- snapshot -->

日期是有值資料的最小／最大來源日期，**不代表中間每天都有資料，也不代表每家媒體都涵蓋這段期間**。
只有標題的日期來自新聞清單，未驗證為精確發布時間；不要把整份資料的最早年份套到所有來源。
各表的筆數有不同意義，不能相加當作新聞總篇數。

## 新聞從哪裡來？

<!-- providers -->

`unregistered:<host>` 只是依網址主機歸類，不表示已辨識原始媒體；例如 `unregistered:news.google.com`
不能直接視為由 Google 撰寫的新聞，還要回查網址與來源觀測。
`ltn_rss` 保留 RSS 的標題、日期與描述。RSS 描述即使有 HTML 或幾段文字，也不是已取得的新聞全文。
這份資料不限台股新聞，也含國際、總體及一般新聞；股票關聯只是部分資料，不能當作全體分類標籤。
內容不是單一語言、單一媒體或人工標註過的標準語料。

## 每筆資料到底代表什麼？

資料拆成四張表，是因為「文章」、「相同文字」、「股票關聯」與「來源觀測」不是同一件事。

| 資料表 | 一筆代表什麼 | 什麼時候使用 |
| --- | --- | --- |
| `articles` | 一個網址在快照中保留的文章版本 | 標題／內文樣本、來源、時間與使用範圍；通常從這張開始 |
| `documents` | 一份精確去重的全文或標題字串 | 避免同樣文字重複處理，或建立訓練切分的去重群組 |
| `article_stocks` | 一組文章、股票代碼與中繼資料來源的關聯 | 把文章接到股票；一篇可對應多檔股票 |
| `metadata_observations` | 所選來源分區中的一列新聞中繼資料 | 查原始標題、日期、股票清單與來源證據 |

例如同樣的內文出現在兩個網址：`articles` 保留兩篇，`documents` 只保留一份相同全文。
這不是語意去重；改了字、改寫或不同媒體描述同一事件，仍可能是不同文字。
內文相同也不代表兩篇的標題、發布時間或使用範圍相同。

### 標題與內文要怎麼選？

- **只做標題任務：** 使用所有非空的 `articles.title`，包括已有內文的文章。
  不要只選 `text_kind='headline'`，否則會漏掉已有內文文章的標題。
- **只做內文任務：** 選 `body IS NOT NULL`，或使用 `iter_news_articles(..., text_kind='fulltext')`。
- **標題加內文：** 保留兩個欄位，依模型需求組合；不要把缺內文的樣本偽裝成全文。

`text_kind` 表示文字取得狀態，不是任務設定：`fulltext` 表示已保留內文；`headline` 表示只有標題。
沒有內文就是 Parquet `NULL`／Python `None`，不是 `""`、`"None"`、零或抓取成功的空文章。
`fulltext` 不保證文章有標題，也不保證取得付費區、附件或原頁所有元素。
`documents` 的 `headline` 只收「沒有內文文章」的標題，不是所有文章標題的全集。

### 有做什麼前處理？

本整理步驟保留來源表已有的文字字串，不額外做 log、數值轉換、翻譯、摘要、改寫、分詞或 tokenization。
「原字串」指來源下載器已抽取並保存的標題／內文，不是原始網頁 HTML 的每個位元組。
頁面抽取方法與內文完整性另有欄位可查。原始 HTML、PDF、歷史版本與下載收據仍在 `data_news_fulltext`，
不會因為建立這份訓練用資料表就被取代或刪除。

這裡沒有自動生成情緒、報酬標籤、token、embedding，也沒有預先指定 train／validation／test。
它是可供後續建模的文字與來源資料，不是已決定預測目標、切分與績效的模型資料集。

## 主要欄位怎麼看？

完整欄名與 Arrow 型別請看 [schema.json](schema.json)。以下是 `articles` 的閱讀方式：

| 欄位 | 意義 |
| --- | --- |
| `article_id`、`url` | 文章識別碼與來源網址；識別碼不是發布順序 |
| `provider` | 媒體／網址來源識別碼，不是 FinMind／FinLab 匯入來源名稱 |
| `title`、`body` | 分開保留的標題與內文；兩欄可獨立缺值 |
| `text_kind`、`document_id` | 取得狀態及對應的精確去重文字 |
| `published_at_raw`、`published_at_utc` | 原發布時間字串、可確認時區後轉成的 UTC 時間；無法確認則 UTC 欄為 NULL |
| `publication_date`、`publication_clock_status` | 來源日期與解析／可信狀態；日期不能自動提升成精確時間 |
| `metadata_date_raw`、`published_time_basis`、`modified_at_raw` | 清單原日期、發布時間取自哪裡、原修改時間；保留原本語意 |
| `observed_at_utc`、`available_at_utc` | 本機實際觀測與本版本可用時間；與歷史發布時間分開 |
| `authors_json`、`attribution_status` | 作者原紀錄與轉載歸屬狀態；JSON 以字串儲存，缺值可為 NULL |
| `body_completeness`、`extraction_method` | 來源記錄的內文完整性與抽取方式，不是人工逐篇審核結果 |
| `queue_state` | 快照時的下載佇列狀態；不能單靠它判定有沒有保留內文，以 `body` 為準 |
| `source_parquet`、`source_row`、`source_sha256` | 對應來源檔、列位置及檔案校驗碼，供追溯使用；標題樣本來自固定佇列快照 |
| `rights_status`、`collection_purpose`、`collection_basis` | 來源使用範圍與蒐集依據紀錄 |
| `declared_license_urls_json`、`license_verification_basis`、`per_article_license_verified` | 來源宣告的授權連結、核對依據與是否逐篇核對；不能由「下載成功」推定 |
| `fair_use_legally_determined`、`commercial_use_enabled` | 保留的使用範圍旗標，不是這次整理對合理使用或商用作出的法律判定 |
| `training_injection_enabled`、`historical_point_in_time`、`publish`、`remote_training_export` | 是否已啟用訓練注入、是否有歷史當時版本證據、發布與遠端匯出狀態 |

其他三張表的重點：

- `documents`：`text` 是全文或標題；`text_sha256` 與 `dedup_group_id` 來自未改寫 UTF-8 字串的 SHA-256；
  `text_bytes` 是 UTF-8 位元組數，不是字數或 token 數。
- `article_stocks`：`stock_id` 是字串，`0050` 等前導零不能轉成整數丟掉。
  唯一鍵包含 `metadata_provider`；只需要文章與股票配對時，取 `DISTINCT article_id, stock_id`，
  避免 FinLab／FinMind 都提到同一配對而加倍計權。
- `metadata_observations`：`title_raw`、`date_raw`、`stock_ids_raw`、`key_date_raw`、`url_raw` 與
  `description_html` 保留來源值；`source_*` 可追到來源列。`resolved_article_id` 才是已成功連到
  `articles` 的鍵，NULL 表示尚無對應文章；不能把這張表的每列當作另一篇獨立新聞。

## 發布日期、取得日期與回測：最容易混淆的地方

一篇標示 2014 年發布、但在 2026 年才回補取得的新聞，仍可研究其文字；
但不能只憑發布日期，就聲稱本系統在 2014 年已持有現在這個版本。

| 時間欄位 | 回答的問題 |
| --- | --- |
| `publication_date`／`published_at_*` | 來源說這篇新聞什麼時候發布？是否有精確且含時區的時間？ |
| `observed_at_utc` | 本機什麼時候實際觀測到這個版本？ |
| `available_at_utc` | 以目前保存證據，這個版本何時才可視為已取得？ |

有內文時，觀測／可用時間採用本次保存內文版本的取得時間；只有標題時採用清單最後觀測時間，
不拿網址第一次被發現的時間替後來更新的標題背書。中繼資料另保留自身匯入／觀測依據。

缺少時區、只有日期、無效或缺少時間都明確標記，不擅自補成台灣時間、00:00 或開盤前。
`metadata_date_raw` 與 FinLab `key_date` 也不等於已驗證的精確發布時間。
目前是回補取得的現存版本，不是已驗證的歷史 point-in-time（當時可見）全文庫。

因此，文字分類或語言研究可以先使用文字；若要做盤前訊號或報酬回測，
要另行選擇發布時間證據、可用時點與修訂版本政策。若採推測發布時間，應另建估計欄與假設，
不要覆蓋這份資料的原時間，也不要把估計描述成已驗證事實。

## 怎麼讀取？

以下範例在 StockAgent 專案根目錄、已安裝專案相依套件的 Python 環境執行。
資料量大，先分批／投影欄位，不要為了看幾筆就把整個資料夾讀成一張 pandas DataFrame。

### 先看幾筆文章

```python
from itertools import islice
from pathlib import Path
from stockagent.data.news_dataset import iter_news_articles

for row in islice(iter_news_articles(Path("data_news")), 3):
    print(row["article_id"], row["title"], row["body"] is None)
```

這個迭代器一次固定一份 manifest，逐批讀取文章；不會因下載器繼續工作而混入另一個版本。
`iter_news_articles(..., text_kind="fulltext")` 可只取有內文文章。

### 只讀標題所需欄位（DuckDB）

```python
import json
from pathlib import Path
import duckdb
from stockagent.data.news_dataset import news_table_paths

root = Path("data_news").resolve()
manifest = json.loads((root / "manifest.json").read_text())
paths = [str(path) for path in news_table_paths(root, "articles", manifest=manifest)]

with duckdb.connect() as con:
    rows = con.execute('''
        SELECT article_id, provider, title, published_at_utc, available_at_utc
        FROM read_parquet(?)
        WHERE title IS NOT NULL AND trim(title) <> ''
        LIMIT 5
    ''', [paths]).fetchall()
```

這裡刻意不限制 `text_kind`，因為有內文文章的標題也要保留。
`LIMIT 5` 是閱讀示範，不是訓練資料的筆數上限；正式處理應分批輸出。
同樣的 manifest 檔案清單也可交給 Polars `scan_parquet(paths)`，先選欄位與篩選再計算。

### 只處理精確去重的全文

```python
from pathlib import Path
from stockagent.data.news_dataset import iter_news_documents

first_document = next(iter_news_documents(Path("data_news"), text_kind="fulltext"), None)
# first_document["text"] 是一份全文；同樣原字串不重複列出。
```

`documents` 本身沒有每篇文章的發布時間、媒體或使用範圍；需要這些資訊時，
以 `document_id` 接回 `articles`，不要只因文字去重就丟掉來源限制。

## 訓練前要處理哪些事？

先決定任務需要標題、內文或兩者，再決定標籤、預測時點與驗證方式。
不要把「來源缺內文」當作空字串訓練，也不要把「沒有股票關聯」當作與股票無關的人工標註。

切分資料時，以 `document_id` 接到 `documents.dedup_group_id`，避免相同原文字串跨越訓練與測試集合。
精確去重擋不住近似改寫、同事件新聞或所有重複標題；這些要依任務另做評估。
如果預測金融結果，還要按所選時間與標籤涵蓋期間留出隔離區間，不能只隨機切文章。
一篇新聞連到多檔股票時，也要明確決定樣本權重，避免關聯列數變成隱藏的重複樣本。

## 品質驗證與仍存在的限制

本版本已完成所有輸出 Parquet 的校驗碼、schema 與筆數讀回、原文字雜湊重算，
並以獨立 SQL 檢查主鍵重複、文章／文字／股票外鍵、內文一致性與空值規則。
細節、時間分布與可重跑 SQL 在 [quality_report.json](quality_report.json)。
這些檢查證明輸出符合資料契約，不等於每篇新聞事實正確、每次抽取都完美或所有歷史都完整。

<!-- quality -->

沒有內文可能是未抓到、來源不可用、權限或原頁限制等情況，不會一律當成不存在的文章。
文章有內文也不代表整個 provider 已抓完。`all_history_complete` 仍為 `false`，
尚未對全站、全日期或每個來源作出完整性保證；這份 README 不能替代下載進度面板。

## 檔案與版本怎麼找？

- [manifest.json](manifest.json)：目前可讀版本、四表筆數、完整來源清單及每個 Parquet 的路徑／校驗碼。
- [schema.json](schema.json)：完整欄位與型別。
- [verification.json](verification.json)、[quality_report.json](quality_report.json)：驗收結果與檢查明細。
- [progress.json](progress.json)：資料集建置的階段進度，不是新聞網站的下載進度。
- `versions/<版本>/`：該版 manifest、新增／重寫的 Parquet 與來源快照差異塊；未變動分區由 manifest 共用先前檔案。

<!-- storage -->

版本是「完整的邏輯視圖」，不是「再保存一份全量副本」。新增另寫，修訂或刪除採 copy-on-write，
只重建受影響分區；同一分區內仍保留的列可能隨分區重寫，並非每個 byte 都達到逐列最小差異。
未變動分區不重寫；建置用 SQLite 索引是暫存，成功後不隨每版永久保留。
來源投影的完整原 bytes 可由快照 chunks 及其 SHA 還原；不刪原始新聞或既有歷史版本。
每版都有完整 `parts` 清單，不須把長串版本依次 UNION，也不需把整庫再拼成一個檔案。
讀取統一用 `news_table_paths`，兼容舊版與新版路徑；舊版固定 manifest 仍可重現，
任何版本仍引用的分區及來源快照塊都不能回收。此格式沒有自動 GC 或自動全庫壓實。

這個原則也適用其他資料集；共用機制與所有 `data_*` 的部署界線見
專案的 `docs/dataset_incremental_storage.md`。新聞仍由既有 builder
與 `news_table_paths` 負責，不另開下載器或把兩種 manifest 當成可以混用。

**只讀 manifest 指向的版本與 `parts` 清單。不要使用 `versions/**/*.parquet` 把所有版本一起讀入**，
否則舊版、未發布候選版與新版會混在一起，造成重複或使用未驗證的資料。

## 資料更新與重新驗證

本資料集不會自己連線抓新聞，也未另外安裝自動重建排程；下載由既有新聞服務負責。
來源庫有新資料後，從專案根目錄執行下列指令，提交新的增量版本：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_news_dataset.py --refresh --verification-memory-mb 4096
```

新版本通過完整驗證後才切換 manifest；失敗時，原本可讀的版本繼續保留。
增量的是實際儲存，不代表略過來源核對與全版本驗證；目前仍會比較完整來源，不能宣稱建置 CPU／I/O 也只與新增列數成正比。
只想重新驗證目前版本，不重新整理資料：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_news_dataset.py --verify-only --verification-memory-mb 4096
```

若所有表格已寫完，只是最後驗證因資源等原因失敗，可用 `--retry-verification` 重驗候選版，
不重抓新聞或重建文字。驗證預設記憶體預算 2048 MB；上面示範使用本次已通過的 4096 MB，
單執行緒且允許落盤。完整驗證可能需要數分鐘及暫存空間，不是閱讀 README 的必要步驟。
這份說明與快照統計由同一個建置入口產生，後續成功重建時會一起更新。

## 使用範圍

這是本機個人研究用途的格式化語料，不是對任何來源的重新授權或法律判定。
整理步驟保留原有使用範圍旗標，不因文章可下載就自行開放公開、商用、遠端匯出或注入既有交易模型。
使用前請查看文章的 `rights_status`、授權依據與相關旗標；個別來源及轉載文章可能不同。
目前資料集的發布、遠端匯出與自動訓練注入均未啟用。
"""
