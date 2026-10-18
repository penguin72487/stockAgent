"""Read-only, pinned inputs for the local news corpus builder.

Never construct NewsQueue: its constructor acquires collector ownership and
modifies the live database. Only this module reads the collector's WAL snapshot.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def readonly_database(path: Path):
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA cache_size=-16384")
    try:
        connection.execute("BEGIN")
        yield connection
    finally:
        connection.close()


def checked_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError(f"missing or out-of-scope news input: {relative}")
    return path


HEAD_FIELDS = (
    "id", "url", "provider", "title", "metadata_date_raw", "first_seen_utc",
    "last_seen_utc", "captured_at_utc", "body_sha256", "parquet_path", "state",
)


def freeze_inputs(source: Path, destination: Path, *, progress=None) -> dict:
    """Copy only source heads/references, not raw bodies, in one coherent read.

    A complete snapshot is immutable. Interrupted copies remain unaccepted and
    can be retried into a new build directory; the production queue is untouched.
    """
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    target = destination / "snapshot.sqlite3"
    snapshot = sqlite3.connect(target)
    # This unpublished copy is never resumed halfway: avoid a rollback journal
    # for every random-key bulk insert. Acceptance follows close + explicit
    # fsync + SHA; a crash leaves an unreferenced, rejected staging directory.
    snapshot.execute("PRAGMA journal_mode=OFF")
    snapshot.execute("PRAGMA synchronous=OFF")
    snapshot.execute("PRAGMA cache_size=-65536")
    snapshot.executescript("""
        CREATE TABLE heads(
            id BLOB PRIMARY KEY, url TEXT NOT NULL, provider TEXT NOT NULL,
            title TEXT, metadata_date_raw TEXT, first_seen_utc TEXT,
            last_seen_utc TEXT, captured_at_utc TEXT, body_sha256 TEXT,
            parquet_path TEXT, state TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE batches(path TEXT PRIMARY KEY, receipt_path TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE metadata_inputs(
            origin_id INTEGER PRIMARY KEY, provider TEXT NOT NULL,
            parquet_path TEXT NOT NULL, sha256 TEXT NOT NULL, rows INTEGER NOT NULL,
            receipt_path TEXT NOT NULL, observed_at_utc TEXT NOT NULL
        );
        CREATE TABLE feed_inputs(
            provider TEXT NOT NULL, feed_url TEXT NOT NULL, receipt_path TEXT NOT NULL,
            rows INTEGER NOT NULL, checked_at_utc TEXT NOT NULL,
            PRIMARY KEY(provider,feed_url)
        ) WITHOUT ROWID;
    """)
    copied = 0
    try:
        with readonly_database(source / "queue.sqlite3") as original:
            captured = utc_now()
            expected = original.execute("SELECT sum(articles) FROM state_metrics").fetchone()[0] or 0
            body_count = original.execute("SELECT sum(heads) FROM stored_body_metrics").fetchone()[0] or 0
            cursor = original.execute("SELECT " + ",".join(HEAD_FIELDS) + " FROM articles NOT INDEXED")
            while rows := cursor.fetchmany(16384):
                snapshot.executemany("INSERT INTO heads VALUES (?,?,?,?,?,?,?,?,?,?,?)", (tuple(r) for r in rows))
                snapshot.commit()
                copied += len(rows)
                if progress:
                    progress("snapshot", copied, expected)
            if copied != expected:
                raise ValueError("source head counter disagrees with the pinned queue snapshot")
            snapshot.executemany("INSERT INTO batches VALUES (?,?)", original.execute(
                "SELECT path,receipt_path FROM accepted_batches ORDER BY path"))
            # Superseded/partial versions remain in the source, not in current totals.
            inputs = original.execute("""
                WITH complete AS (
                    SELECT o.* FROM origins o LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id
                    WHERE p.origin_id IS NULL OR p.state='complete'
                ), latest AS (SELECT receipt_path,max(id) id FROM complete GROUP BY receipt_path)
                SELECT c.id,c.provider,c.parquet_path,c.sha256,c.rows,c.receipt_path,c.imported_at_utc
                FROM complete c JOIN latest l ON l.id=c.id ORDER BY c.id
            """)
            snapshot.executemany("INSERT INTO metadata_inputs VALUES (?,?,?,?,?,?,?)", (tuple(r) for r in inputs))
            snapshot.executemany("INSERT INTO feed_inputs VALUES (?,?,?,?,?)", original.execute(
                "SELECT provider,feed_url,receipt_path,rows,checked_at_utc FROM metadata_feed_heads "
                "ORDER BY provider,feed_url"))
        snapshot.executescript("""
            CREATE INDEX body_path ON heads(parquet_path,id) WHERE body_sha256 IS NOT NULL;
            CREATE INDEX headline_heads ON heads(id) WHERE body_sha256 IS NULL AND title IS NOT NULL AND trim(title)!='';
        """)
        snapshot.commit()
        counts = {
            "known_urls": copied, "fulltext_articles": body_count,
            "headline_articles": snapshot.execute("SELECT count(*) FROM heads WHERE body_sha256 IS NULL "
                                                  "AND title IS NOT NULL AND trim(title)!=''").fetchone()[0],
            "metadata_rows": snapshot.execute("SELECT coalesce(sum(rows),0) FROM metadata_inputs").fetchone()[0],
            "metadata_partitions": snapshot.execute("SELECT count(*) FROM metadata_inputs").fetchone()[0],
            "rss_items": snapshot.execute("SELECT coalesce(sum(rows),0) FROM feed_inputs").fetchone()[0],
        }
        counts["without_trainable_text"] = copied - counts["fulltext_articles"] - counts["headline_articles"]
        snapshot.close()
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
        result = {"captured_at_utc": captured, "source_root": str(source.resolve()),
                  "snapshot_path": target.name, "snapshot_sha256": sha256_file(target), "counts": counts,
                  "production_queue_writes": False, "publisher_requests": 0}
        atomic_write_json(destination / "snapshot.json", result)
        return result
    finally:
        snapshot.close()


def iter_body_groups(snapshot: sqlite3.Connection, *, after: str = "", target_rows: int = 16384):
    """Bound memory and open each selected immutable Parquet only once."""
    paths, count = [], 0
    cursor = snapshot.execute(
        "SELECT parquet_path,count(*) rows FROM heads INDEXED BY body_path "
        "WHERE body_sha256 IS NOT NULL AND parquet_path>? GROUP BY parquet_path ORDER BY parquet_path", (after,))
    for row in cursor:
        paths.append(row[0])
        count += row[1]
        if count >= target_rows:
            yield paths
            paths, count = [], 0
    if paths:
        yield paths


def reuse_snapshot(source: Path, destination: Path, frozen: Path) -> dict:
    """Reuse only verified immutable inputs, never old builder state or parts.

    A new builder ABI gets a fresh generation and a private working projection.
    Packed bytes are reconstructed exactly; the accepted input is never writable
    through a hard link. Successful builds seal reusable chunks, then drop work.
    """
    plan = json.loads((frozen / "snapshot.json").read_text())
    if plan["source_root"] != str(source.resolve()):
        raise ValueError("reused news snapshot source/checksum mismatch")
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    target = destination / "snapshot.sqlite3"
    if "snapshot_storage" in plan:
        from stockagent.data.news_dataset_snapshots import copy_snapshot
        copy_snapshot(frozen.parent.parent, frozen, plan, target)
    else:
        original = checked_path(frozen, plan["snapshot_path"])
        if sha256_file(original) != plan["snapshot_sha256"]:
            raise ValueError("reused news snapshot source/checksum mismatch")
        shutil.copyfile(original, target)
    with target.open("rb") as stream:
        os.fsync(stream.fileno())
    # A separate output root must not inherit paths into another corpus.
    result = {**{key: value for key, value in plan.items() if key != "snapshot_storage"},
              "snapshot_reused_from": str(frozen.resolve())}
    atomic_write_json(destination / "snapshot.json", result)
    return result


def source_metadata_record(row: dict, provider: str) -> tuple[str | None, list[str]]:
    """Keep leading zeroes and split only the source's documented ID list."""
    link = row.get("url" if provider == "finlab" else "link")
    raw_ids = row.get("stock_ids" if provider == "finlab" else "stock_id")
    if raw_ids is None:
        return link, []
    if not isinstance(raw_ids, str):
        raise ValueError("news stock identifiers must remain source strings")
    ids = raw_ids.split(",") if provider == "finlab" else [raw_ids]
    return link, list(dict.fromkeys(value.strip() for value in ids if value.strip()))


def url_article_id(url: str) -> bytes:
    from downloader.news_fulltext_policy import normalize_url
    return hashlib.sha256(normalize_url(url).encode("utf-8")).digest()


def original_article_id(url: str) -> bytes:
    """Resolve reviewed literal Google envelopes locally; never request a URL."""
    from stockagent.data.news_source_urls import resolve_google_literal_url
    if url.startswith("https://news.google.com/") or url.startswith("http://news.google.com/"):
        resolution = resolve_google_literal_url(url)
        if resolution.outcome == "literal_original":
            return url_article_id(resolution.original_url)
    return url_article_id(url)
