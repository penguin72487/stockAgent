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
import re
import shutil
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_metadata_current import current_origins_cte, unresolved_alias_count


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

HEADLINE_SCOPE_CONTRACT = "cnyes_market_and_cmnews_tag_metadata_only_v2"
BODY_QUALITY_SCOPE_CONTRACT = "owner_reviewed_exact_body_exclusions_v1"
METADATA_SELECTION_SCOPE_CONTRACT = "explicit_current_origin_or_legacy_inference_alias_census_v2"
PUBLICATION_BASIS_SCOPE_CONTRACT = "globalvoices_and_cmnews_exact_retained_declarations_v2"
_CNYES_MARKET_REASONS = ("cnyes_market_route", "literal_google_cnyes_market_route")
_CMNEWS_TAG_REASONS = ("cmnews_tag_route_unverified", "literal_google_cmnews_tag_route_unverified")
_CMNEWS_TAG_PREFIXES = ("http://cmnews.com.tw/tag/", "https://cmnews.com.tw/tag/")


def headline_metadata_only_reason(url: str) -> str | None:
    """Keep unverified market/tag references as metadata, not news text.

    A literal Google envelope may point at either route. Decode only
    with the shared, strict local resolver: no network request, guessed URL,
    source-identity assertion or change to the acquisition queue is involved.
    A tag-route hint does not establish the content or identity of that page.
    """
    if not isinstance(url, str):
        return None
    if url.startswith(("http://www.cnyes.com/twstock/", "https://www.cnyes.com/twstock/")):
        return "cnyes_market_route"
    if url.startswith(_CMNEWS_TAG_PREFIXES):
        return "cmnews_tag_route_unverified"
    if url.startswith(("http://news.google.com/", "https://news.google.com/")):
        from stockagent.data.news_source_urls import resolve_google_literal_url
        result = resolve_google_literal_url(url)
        if (result.outcome == "literal_original" and result.original_url.startswith(
                ("http://www.cnyes.com/twstock/", "https://www.cnyes.com/twstock/"))):
            return "literal_google_cnyes_market_route"
        if result.outcome == "literal_original" and result.original_url.startswith(_CMNEWS_TAG_PREFIXES):
            return "literal_google_cmnews_tag_route_unverified"
    return None


def _quality_request(source: Path, request_id: str):
    """Read the owner's exact sealed request without making mailbox changes.

    The owner may commit the ledger before atomically moving pending to
    processed. Probe both places twice so that move does not lose evidence;
    if neither is available the snapshot must defer, not guess a body version.
    """
    from downloader.news_fulltext_repairs import _read_private_request
    from downloader.news_fulltext_stored_quality import validate_request
    if not isinstance(request_id, str) or re.fullmatch(r"[0-9a-f]{64}", request_id) is None:
        raise ValueError("invalid reviewed body quality request id")
    base = source / "reviewed_repairs"
    for _ in range(2):
        for location in ("processed", "pending"):
            directory = base / location
            for parent in (base, directory):
                if parent.is_symlink():
                    raise ValueError("reviewed body quality directory is a symlink")
                if parent.exists() and (not parent.is_dir() or parent.stat().st_mode & 0o077):
                    raise ValueError("reviewed body quality directory is not private")
            try:
                request, payload = _read_private_request(directory / (request_id + ".json"),
                                                         include_payload=True)
            except FileNotFoundError:
                continue
            return validate_request(request), payload
    raise FileNotFoundError("reviewed body quality request is unavailable")


def _freeze_reviewed_body_quality(source, original, snapshot):
    """Project successful owner reviews, fenced to their exact retained body."""
    if not original.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                            "AND name='reviewed_repair_items'").fetchone():
        return
    from downloader.news_fulltext_repairs import _clock, _unique
    from downloader.news_fulltext_stored_quality import MAX_BATCH_BYTES
    def identity(info):
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
    rows = original.execute("""
        SELECT r.request_id,r.target_index,r.article_id,r.provider,r.parser_fingerprint,
               r.recorded_at_utc,j.targets,j.next_index,
               a.url,a.provider AS current_provider,a.body_sha256,a.parquet_path,a.captured_at_utc
        FROM reviewed_repair_items r
        LEFT JOIN reviewed_repair_jobs j ON j.request_id=r.request_id
        JOIN articles a ON a.id=r.article_id
        WHERE r.outcome='stored_quality_reclassified' AND a.body_sha256 IS NOT NULL
        ORDER BY r.request_id,r.target_index
    """)
    current_id, request, payload = None, None, None
    verified_batches = {}
    for row in rows:
        request_id = row["request_id"]
        if request_id != current_id:
            request, payload = _quality_request(source, request_id)
            current_id = request_id
        index = row["target_index"]
        if (type(index) is not int or not 0 <= index < len(request["targets"])
                or row["targets"] != len(request["targets"])
                or type(row["next_index"]) is not int or not index < row["next_index"] <= row["targets"]):
            raise ValueError("reviewed body quality ledger is inconsistent")
        target = request["targets"][index]
        if (target["id"] != bytes(row["article_id"]).hex() or target["url"] != row["url"]
                or target["provider"] != row["provider"] or row["provider"] != row["current_provider"]
                or request["sources"][row["provider"]]["parser_fingerprint"] != row["parser_fingerprint"]
                or _clock(row["recorded_at_utc"]) < _clock(request["created_at_utc"])):
            raise ValueError("reviewed body quality identity is inconsistent")
        if (target["body_sha256"], target["parquet_path"], target["captured_at_utc"]) != (
                row["body_sha256"], row["parquet_path"], row["captured_at_utc"]):
            # A later body or observation has its own evidence and must remain.
            continue
        witness = request["originals"][target["id"]]
        accepted = original.execute("SELECT receipt_path FROM accepted_batches WHERE path=?",
                                    (target["parquet_path"],)).fetchone()
        if accepted is None or accepted[0] != witness["receipt_path"]:
            raise ValueError("reviewed body quality batch is no longer accepted")
        key = (target["parquet_path"], witness["receipt_path"])
        expected = (witness["parquet_sha256"], witness["receipt_sha256"])
        if key not in verified_batches:
            body, receipt = checked_path(source, key[0]), checked_path(source, key[1])
            body_before, receipt_before = body.stat(), receipt.stat()
            if body_before.st_size > MAX_BATCH_BYTES or receipt_before.st_size > 65536:
                raise ValueError("reviewed body quality artifact exceeds owner bounds")
            receipt_bytes = receipt.read_bytes()
            metadata = json.loads(receipt_bytes, object_pairs_hook=_unique)
            observed = (sha256_file(body), hashlib.sha256(receipt_bytes).hexdigest())
            if (identity(body.stat()) != identity(body_before) or identity(receipt.stat()) != identity(receipt_before)
                    or metadata.get("contract_version") != 1 or metadata.get("parquet_path") != key[0]
                    or metadata.get("sha256") != observed[0]):
                raise ValueError("reviewed body quality artifact changed or receipt mismatched")
            verified_batches[key] = observed
        if verified_batches[key] != expected:
            raise ValueError("reviewed body quality artifact digest disagrees with review")
        snapshot.execute("INSERT OR IGNORE INTO quality_review_requests VALUES (?,?)", (request_id, payload))
        snapshot.execute("INSERT INTO body_quality_exclusions VALUES (?,?,?,?,?,?,?,?,?)", (
            row["article_id"], request_id, index, target["body_sha256"], target["parquet_path"],
            target["captured_at_utc"], request["expected_error"], row["recorded_at_utc"],
            row["parser_fingerprint"]))


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
        CREATE TABLE metadata_only_heads(id BLOB PRIMARY KEY, reason TEXT NOT NULL) WITHOUT ROWID;
        CREATE TABLE quality_review_requests(request_id TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
        CREATE TABLE body_quality_exclusions(
            article_id BLOB NOT NULL,request_id TEXT NOT NULL,target_index INTEGER NOT NULL,
            body_sha256 TEXT NOT NULL,parquet_path TEXT NOT NULL,captured_at_utc TEXT NOT NULL,
            error TEXT NOT NULL,reviewed_at_utc TEXT NOT NULL,parser_fingerprint TEXT NOT NULL,
            PRIMARY KEY(article_id,request_id,target_index)
        ) WITHOUT ROWID;
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
            if unresolved_alias_count(original):
                raise ValueError('unresolved_metadata_receipt_alias_retarget')
            captured = utc_now()
            expected = original.execute("SELECT sum(articles) FROM state_metrics").fetchone()[0] or 0
            body_count = original.execute("SELECT sum(heads) FROM stored_body_metrics").fetchone()[0] or 0
            cursor = original.execute("SELECT " + ",".join(HEAD_FIELDS) + " FROM articles NOT INDEXED")
            while rows := cursor.fetchmany(16384):
                snapshot.executemany("INSERT INTO heads VALUES (?,?,?,?,?,?,?,?,?,?,?)", (tuple(r) for r in rows))
                metadata_only = []
                for row in rows:
                    if row["body_sha256"] is None and row["title"] and row["title"].strip():
                        reason = headline_metadata_only_reason(row["url"])
                        if reason:
                            metadata_only.append((row["id"], reason))
                snapshot.executemany("INSERT INTO metadata_only_heads VALUES (?,?)", metadata_only)
                snapshot.commit()
                copied += len(rows)
                if progress:
                    progress("snapshot", copied, expected)
            if copied != expected:
                raise ValueError("source head counter disagrees with the pinned queue snapshot")
            snapshot.executemany("INSERT INTO batches VALUES (?,?)", original.execute(
                "SELECT path,receipt_path FROM accepted_batches ORDER BY path"))
            _freeze_reviewed_body_quality(source, original, snapshot)
            for row in snapshot.execute("SELECT h.id,h.url,h.title FROM "
                    "(SELECT DISTINCT article_id FROM body_quality_exclusions) e "
                    "JOIN heads h ON h.id=e.article_id"):
                if row[2] and row[2].strip():
                    reason = headline_metadata_only_reason(row[1])
                    if reason:
                        snapshot.execute("INSERT OR IGNORE INTO metadata_only_heads VALUES (?,?)", (row[0], reason))
            # Superseded/partial versions remain in the source, not in current totals.
            inputs = original.execute(current_origins_cte(original) + """
                SELECT id,provider,parquet_path,sha256,rows,receipt_path,imported_at_utc
                FROM current_metadata ORDER BY id
            """)
            snapshot.executemany("INSERT INTO metadata_inputs VALUES (?,?,?,?,?,?,?)", (tuple(r) for r in inputs))
            has_selection = original.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                             "AND name='metadata_current_origins'").fetchone()
            explicit_selections = (original.execute(current_origins_cte(original) +
                " SELECT count(*) FROM current_metadata c JOIN metadata_current_origins h "
                "ON h.receipt_path=c.receipt_path AND h.origin_id=c.id").fetchone()[0] if has_selection else 0)
            snapshot.executemany("INSERT INTO feed_inputs VALUES (?,?,?,?,?)", original.execute(
                "SELECT provider,feed_url,receipt_path,rows,checked_at_utc FROM metadata_feed_heads "
                "ORDER BY provider,feed_url"))
        snapshot.executescript("""
            CREATE INDEX body_path ON heads(parquet_path,id) WHERE body_sha256 IS NOT NULL;
            CREATE INDEX headline_heads ON heads(id) WHERE body_sha256 IS NULL AND title IS NOT NULL AND trim(title)!='';
        """)
        snapshot.commit()
        metadata_only_counts = dict(snapshot.execute(
            "SELECT reason,count(*) FROM metadata_only_heads GROUP BY reason"))
        excluded = snapshot.execute("SELECT count(DISTINCT article_id) FROM body_quality_exclusions").fetchone()[0]
        ordinary_headlines = snapshot.execute("SELECT count(*) FROM heads WHERE body_sha256 IS NULL "
            "AND title IS NOT NULL AND trim(title)!='' AND NOT EXISTS "
            "(SELECT 1 FROM metadata_only_heads m WHERE m.id=heads.id)").fetchone()[0]
        reviewed_headlines = snapshot.execute("SELECT count(*) FROM "
            "(SELECT DISTINCT article_id FROM body_quality_exclusions) e JOIN heads h ON h.id=e.article_id "
            "WHERE h.title IS NOT NULL AND trim(h.title)!='' AND NOT EXISTS "
            "(SELECT 1 FROM metadata_only_heads m WHERE m.id=h.id)").fetchone()[0]
        counts = {
            "known_urls": copied, "stored_body_heads": body_count,
            "reviewed_excluded_body_heads": excluded, "fulltext_articles": body_count - excluded,
            "ordinary_headline_articles": ordinary_headlines, "reviewed_headline_articles": reviewed_headlines,
            "headline_articles": ordinary_headlines + reviewed_headlines,
            "metadata_only_heads": sum(metadata_only_counts.values()),
            "metadata_only_market_heads": sum(metadata_only_counts.get(reason, 0) for reason in _CNYES_MARKET_REASONS),
            "metadata_only_cmnews_tag_heads": sum(metadata_only_counts.get(reason, 0) for reason in _CMNEWS_TAG_REASONS),
            "metadata_rows": snapshot.execute("SELECT coalesce(sum(rows),0) FROM metadata_inputs").fetchone()[0],
            "metadata_partitions": snapshot.execute("SELECT count(*) FROM metadata_inputs").fetchone()[0],
            "rss_items": snapshot.execute("SELECT coalesce(sum(rows),0) FROM feed_inputs").fetchone()[0],
        }
        counts["without_trainable_text"] = copied - counts["fulltext_articles"] - counts["headline_articles"]
        snapshot.close()
        with target.open("rb") as stream:
            os.fsync(stream.fileno())
        result = {"captured_at_utc": captured, "source_root": str(source.resolve()),
                  "headline_scope_contract": HEADLINE_SCOPE_CONTRACT,
                  "body_quality_scope_contract": BODY_QUALITY_SCOPE_CONTRACT,
                  "metadata_selection_scope_contract": METADATA_SELECTION_SCOPE_CONTRACT,
                  "publication_basis_scope_contract": PUBLICATION_BASIS_SCOPE_CONTRACT,
                  "metadata_selection_evidence": {"explicit": explicit_selections,
                      "legacy_inferred": counts["metadata_partitions"] - explicit_selections},
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
        "WHERE body_sha256 IS NOT NULL AND parquet_path>? AND NOT EXISTS "
        "(SELECT 1 FROM body_quality_exclusions e WHERE e.article_id=heads.id) "
        "GROUP BY parquet_path ORDER BY parquet_path", (after,))
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
    if plan.get("headline_scope_contract") != HEADLINE_SCOPE_CONTRACT:
        raise ValueError("reused news snapshot has an incompatible headline scope; freeze a new source snapshot")
    if plan.get("body_quality_scope_contract") != BODY_QUALITY_SCOPE_CONTRACT:
        raise ValueError("reused news snapshot has an incompatible body quality scope; freeze a new source snapshot")
    if plan.get("metadata_selection_scope_contract") != METADATA_SELECTION_SCOPE_CONTRACT:
        raise ValueError("reused news snapshot has an incompatible metadata selection scope; freeze a new source snapshot")
    if plan.get("publication_basis_scope_contract") != PUBLICATION_BASIS_SCOPE_CONTRACT:
        raise ValueError("reused news snapshot has an incompatible publication basis scope; freeze a new source snapshot")
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
