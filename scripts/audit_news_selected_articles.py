"""Read-only evidence for explicitly selected committed news article heads.

Use one pinned WAL snapshot and the canonical immutable batch verifier. Never
fetch a publisher, acquire the writer lock, export text or infer completeness.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_feed import FEED_BODY_PROVENANCE_FIELDS, replay_feed_body_version
from downloader.news_fulltext_policy import NewsError, extract_article, normalize_url
from downloader.news_fulltext_queue import source_path, utc_now
from scripts.benchmark_cnyes_acquisition import verify_batches
from stockagent.data.news_source_catalog import configuration_paths


CONTRACT_VERSION = 11


def _replay_feed_envelope(payload, request_url, registry, source, *, body_contract_version=1):
    """Use the registered feed parser, with no transport or relaxed rights.

    A mixed restricted envelope is never a valid historical body source. A
    non-body neighbour need not invalidate an independently usable item, but
    an error for that selected identity must not be silently ignored.
    """
    from downloader.news_fulltext_bulk import BULK_RESTRICTED_ITEM_ERRORS
    from downloader.news_fulltext_feed import feed_records

    response = SimpleNamespace(url=request_url, body=payload,
                               headers={'content-type': 'application/rss+xml'})
    rows, errors = feed_records(response, SimpleNamespace(registry=registry), source,
                                body_contract_version=body_contract_version)
    if any(error['error'] in BULK_RESTRICTED_ITEM_ERRORS for error in errors):
        raise ValueError('selected_feed_contains_restricted_text')
    indexed = {row['article_id']: row for row in rows}
    if len(indexed) != len(rows):
        raise ValueError('selected_feed_item_not_replayable')
    return indexed, {error.get('url') for error in errors}


def audit(root: Path, urls: list[str], *, registry=None) -> dict:
    if not 1 <= len(urls) <= 16:
        raise ValueError("one to sixteen explicitly selected URLs required")
    urls = list(dict.fromkeys(normalize_url(url) for url in urls))
    db = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        result = {"contract_version": CONTRACT_VERSION, "database_snapshot_at_utc": utc_now(), "articles": [], "errors": [],
                  "publish": False, "remote_training_export": False,
                  "all_history_complete": False, "scope": "explicit selected durable heads, not source history",
                  "current_source_replay_requested": registry is not None}
        batches = defaultdict(list)
        for url in urls:
            row = db.execute("SELECT id,provider,state,body_sha256,parquet_path,captured_at_utc "
                             "FROM articles WHERE url=?", (url,)).fetchone()
            if row is None or row["state"] != "complete" or not row["body_sha256"]:
                result["errors"].append({"url": url, "error": "selected_body_not_complete",
                                         "state": row["state"] if row else "not_registered"})
                continue
            batches[row["parquet_path"]].append((url, row))
        for path, selected in batches.items():
            try:
                # Read once per distinct batch, not once per selected article.
                # Release its unselected raw envelopes before the next batch.
                records = pq.read_table(source_path(root, path)).to_pylist()
                indexed = {r["article_id"]: r for r in records}
                envelopes = {r["article_id"]: r["raw_response_body"] for r in records if r["raw_response_body"] is not None}
                feed_replays = {}  # At most the selected batch's shared envelopes; no global raw cache.
                for url, row in selected:
                    record = indexed.get(row["id"].hex())
                    if record is None or record["body_sha256"] != row["body_sha256"]:
                        result["errors"].append({"url": url, "error": "selected_head_record_mismatch"})
                        continue
                    item = {"url": url, "provider": row["provider"], "state": row["state"],
                        "body_sha256": row["body_sha256"], "captured_at_utc": row["captured_at_utc"],
                        "parquet_path": path, "body_characters": len(record["body"]),
                        **{k: record.get(k) for k in ("requested_url", "final_url", "source_request_url", "published_at_raw",
                            "published_time_basis", "modified_at_raw", "authors_json", "author_attribution_detail_json", "data_fingerprint",
                            "article_identity_basis", "declared_canonical_url", "extraction_method", "response_sha256", "license_verification_basis",
                            "per_article_license_verified", "declared_license_urls_json", *FEED_BODY_PROVENANCE_FIELDS)}}
                    if registry is not None:
                        try:
                            source = registry.require(record["final_url"])
                            if source.provider != row["provider"]:
                                raise ValueError("selected_head_source_mismatch")
                            payload = envelopes[record["source_response_article_id"]]
                            if record.get('extraction_method') == 'cnyes_api_content_v1':
                                # The retained source is a publisher JSON page, not
                                # HTML at final_url. Replay its registered parser;
                                # a fake transport contains no session or get().
                                from downloader.news_fulltext_bulk import cnyes_records
                                # Format adapter for the already-accepted JSON
                                # bytes, not a newly observed HTTP MIME/status.
                                response = SimpleNamespace(url=record['source_request_url'], body=payload,
                                                           headers={'content-type': 'application/json'})
                                rows, _ = cnyes_records(response, SimpleNamespace(registry=registry), source)
                                replay_matches = [r for r in rows if r['article_id'] == record['article_id']]
                                if len(replay_matches) != 1:
                                    raise ValueError('selected_api_item_not_replayable')
                                replay = replay_matches[0]
                            elif str(record.get('extraction_method', '')).startswith('rss_content_encoded_'):
                                version = replay_feed_body_version(record, source)
                                key = (record['source_response_article_id'], record['source_request_url'], source.provider, version)
                                if key not in feed_replays:
                                    feed_replays[key] = _replay_feed_envelope(
                                        payload, record['source_request_url'], registry, source, body_contract_version=version)
                                feed_rows, failed_urls = feed_replays[key]
                                replay = feed_rows.get(record['article_id'])
                                if replay is None or record['requested_url'] in failed_urls:
                                    raise ValueError('selected_feed_item_not_replayable')
                                if (any(replay.get(k) != record.get(k) for k in (
                                        'requested_url', 'final_url', 'published_time_basis', 'body_completeness',
                                        'license_verification_basis', 'per_article_license_verified', *FEED_BODY_PROVENANCE_FIELDS))
                                        or json.loads(replay['declared_license_urls_json'])
                                        != json.loads(record['declared_license_urls_json'])):
                                    raise ValueError('current_source_replay_changed_rss_license_or_scope')
                            elif record.get('extraction_method') == 'fed_pdf_original_text_v1':
                                from downloader.news_fulltext_pdf import extract_board_pdf
                                replay = extract_board_pdf(payload, source, page_url=record['final_url'])
                            else:
                                replay = extract_article(payload, source, page_url=record["final_url"])
                            if record.get("url_resolution_evidence_json"):
                                from downloader.news_fulltext_public_originals import ftv_declared_public_canonical, verify_resolution_response
                                verify_resolution_response(record)
                                evidence = json.loads(record["url_resolution_evidence_json"])
                                resolution = ftv_declared_public_canonical(record["url_resolution_response_body"], source,
                                                                         page_url=evidence["subsite_url"])
                                if any(evidence.get(key) != value for key, value in resolution.items()):
                                    raise ValueError("canonical_resolution_source_replay_mismatch")
                                item.update(canonical_resolution_source_replay_verified=True,
                                            canonical_resolution_evidence=evidence)
                            if (any(replay.get(k) != record.get(k) for k in ("body_sha256", "title", "published_at_raw", "modified_at_raw", "declared_canonical_url", "source_title_declarations_json"))
                                    or json.loads(replay["authors_json"]) != json.loads(record["authors_json"])):
                                raise ValueError("current_source_replay_changed_body_title_author_or_clock")
                            if replay.get('verified_article_notices_json') != record.get('verified_article_notices_json'):
                                raise ValueError('current_source_replay_changed_verified_article_notices')
                            replay_captions = json.loads(replay['verified_image_captions_json']) if replay.get('verified_image_captions_json') else []
                            saved_captions = json.loads(record['verified_image_captions_json']) if record.get('verified_image_captions_json') else []
                            # Caption order and image pairing are source data,
                            # even when the prose SHA and publication clock
                            # are unchanged. JSON object key order is not.
                            if replay_captions != saved_captions:
                                raise ValueError('current_source_replay_changed_verified_image_captions')
                            item['verified_image_caption_count'] = len(saved_captions)
                            item['verified_article_notice_count'] = len(json.loads(record['verified_article_notices_json'])) if record.get('verified_article_notices_json') else 0
                            item.update(current_source_replay_verified=True, current_source_fingerprint=source.data_fingerprint,
                                        capture_contract_matches_current_source=record.get("data_fingerprint") == source.data_fingerprint)
                        except (NewsError, ValueError, KeyError, TypeError) as error:
                            item["current_source_replay_verified"] = False
                            result["errors"].append({"url": url, "error": error.code if isinstance(error, NewsError) else str(error)[:160]})
                    result["articles"].append(item)
            except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
                result["errors"].append({"path": path, "error": "selected_batch_not_readable", "type": type(error).__name__})
        if batches:
            try:
                # Historical neighbours still need valid immutable hashes, but
                # only our selected heads must match today's URL head pointers.
                result["batch_proof"] = verify_batches(SimpleNamespace(root=root, db=db), sorted(batches),
                    head_ids={row["id"] for selected in batches.values() for _, row in selected})
            except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
                result["errors"].append({"error": "selected_batch_not_verified", "type": type(error).__name__})
        result["accepted"] = (not result["errors"] and len(result["articles"]) == len(urls)
                              and result.get("batch_proof", {}).get("accepted", False))
        result["verified_at_utc"] = utc_now()
        return result
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--reparse-current-source", action="store_true", help="Replay selected HTML/JSON/RSS/PDF source envelopes with their registered current parser; no HTTP or rewrite of historical capture fingerprints")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = audit(repo / config["root"], args.url, registry=registry if args.reparse_current_source else None)
    result["configuration_files_sha256"] = {str(p): sha256_file(p) for p in configuration_paths(args.config)}
    result["implementation_sha256"] = {str(p.relative_to(repo)): sha256_file(p) for p in [
        Path(__file__).resolve(), *(repo / "downloader").glob("news_fulltext*.py")]}
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
