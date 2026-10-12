"""Whole-cohort, read-only original-URL inventory; no publisher/Google calls.

Temporary SQLite deduplication keeps RAM bounded. It is not a second collector
or source database. Only durable aggregate receipts/CSV are published; retained
queue URLs and the resolver version reproduce the exact observations.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import io
import json
from pathlib import Path
import resource
import sqlite3
import tempfile
import time
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import order_day, utc_now
from stockagent.data.news_source_catalog import configuration_paths
from stockagent.data.news_source_urls import GOOGLE_LITERAL_URL_CONTRACT, resolve_google_literal_url


def inventory(config_path: Path, *, workspace: Path, root: Path | None = None, progress=False) -> dict:
    started, cpu = time.perf_counter(), time.process_time()
    config, registry, repo = load_config(config_path)
    root = root or repo / config["root"]
    source = sqlite3.connect(f"file:{(root / 'queue.sqlite3').resolve()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        source.execute("BEGIN")
        owner = source.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        expected = source.execute("SELECT coalesce(sum(articles),0) FROM state_metrics WHERE provider='unregistered:news.google.com'").fetchone()[0]
        result = {"contract_version": 1, "resolver_contract_version": GOOGLE_LITERAL_URL_CONTRACT,
                  "observed_at_utc": utc_now(), "queue_owner_id": owner[0] if owner else None,
                  "scope": "every retained URL in unregistered:news.google.com, all states/dates; no top-K or time cutoff",
                  "scope_expected_urls": expected, "input_queue": str(root / 'queue.sqlite3'),
                  "resolver_sha256": sha256_file(repo / 'stockagent/data/news_source_urls.py'),
                  "configuration_sha256": {str(p): sha256_file(p) for p in configuration_paths(config_path)},
                  "date_basis": "retained metadata sorting dates, not verified publisher publication or PIT evidence",
                  "resolution_basis": "literal field 4 in locally retained reviewed legacy envelope; no Google RPC/HTTP; field 26 is evidence only",
                  "publisher_access_or_rights_proven_by_resolution": False, "body_acceptance_proven_by_resolution": False,
                  "all_history_complete": False, "provider_counts": {}, "samples": {}}
        counts, errors, input_hash, examined = Counter(), Counter(), hashlib.sha256(), 0
        host_policy, last_progress = {}, time.monotonic()
        with tempfile.TemporaryDirectory(prefix="news-url-audit-", dir=workspace) as temp:
            targets = sqlite3.connect(Path(temp) / "dedupe.sqlite3")
            try:
                targets.execute("PRAGMA cache_size=-2048")
                targets.execute("PRAGMA synchronous=OFF")  # Reproducible temporary audit spill only.
                targets.execute("CREATE TABLE destinations(id BLOB PRIMARY KEY,url TEXT NOT NULL,host TEXT NOT NULL,"
                                "provider TEXT NOT NULL,enabled INTEGER NOT NULL,metadata_urls INTEGER NOT NULL,"
                                "earliest_day TEXT,latest_day TEXT,known_state TEXT,has_body INTEGER DEFAULT 0) WITHOUT ROWID")
                rows = source.execute("SELECT id,url,metadata_date_raw,metadata_order_day FROM articles "
                                      "WHERE provider='unregistered:news.google.com' ORDER BY state,due,metadata_date_raw DESC,rowid")
                while batch := rows.fetchmany(2048):
                    inserts = []
                    for row in batch:
                        examined += 1
                        raw_date = row['metadata_date_raw'] or ''
                        input_hash.update(row['id'])
                        stamp = raw_date.encode()
                        input_hash.update(len(stamp).to_bytes(4, 'big'))
                        input_hash.update(stamp)
                        resolution = resolve_google_literal_url(row['url'])
                        counts[resolution.outcome] += 1
                        if resolution.error_code:
                            errors[resolution.error_code] += 1
                        if not row['metadata_order_day'] and order_day(raw_date):
                            counts['missing_derived_sort_day_with_raw_date'] += 1
                        key = resolution.error_code or resolution.outcome
                        samples = result['samples'].setdefault(key, [])
                        if len(samples) < 3:
                            samples.append({"redirect_article_id": row['id'].hex(), "metadata_date_raw": raw_date,
                                            "original_url": resolution.original_url, "alternate_url_evidence_only": resolution.alternate_url,
                                            "envelope_sha256": resolution.envelope_sha256})
                        if resolution.original_url is None:
                            continue
                        url = resolution.original_url
                        host = urlsplit(url).hostname
                        if host not in host_policy:
                            policy = registry.match(url)
                            host_policy[host] = (policy.provider if policy else 'unregistered:' + host, bool(policy and policy.allowed))
                        provider, enabled = host_policy[host]
                        day = order_day(raw_date) or None
                        inserts.append((hashlib.sha256(url.encode()).digest(), url, host, provider, enabled, day, day))
                    targets.executemany("INSERT INTO destinations(id,url,host,provider,enabled,metadata_urls,earliest_day,latest_day) "
                                        "VALUES (?,?,?,?,?,1,?,?) ON CONFLICT(id) DO UPDATE SET metadata_urls=metadata_urls+1,"
                                        "earliest_day=CASE WHEN earliest_day IS NULL THEN excluded.earliest_day WHEN excluded.earliest_day IS NULL THEN earliest_day ELSE min(earliest_day,excluded.earliest_day) END,"
                                        "latest_day=CASE WHEN latest_day IS NULL THEN excluded.latest_day WHEN excluded.latest_day IS NULL THEN latest_day ELSE max(latest_day,excluded.latest_day) END", inserts)
                    targets.commit()
                    if progress and time.monotonic() - last_progress >= 20:
                        print(json.dumps({"examined_urls": examined, "expected_urls": expected, "literal_urls": counts['literal_original']}), flush=True)
                        last_progress = time.monotonic()
                cursor = targets.execute("SELECT id FROM destinations ORDER BY id")
                while ids := cursor.fetchmany(400):
                    placeholders = ','.join('?' for _ in ids)
                    known = source.execute(f"SELECT id,state,body_sha256 IS NOT NULL FROM articles WHERE id IN ({placeholders})", [r[0] for r in ids]).fetchall()
                    targets.executemany("UPDATE destinations SET known_state=?,has_body=? WHERE id=?", ((r[1], r[2], r[0]) for r in known))
                targets.commit()
                for row in targets.execute("SELECT provider,enabled,count(*) AS unique_urls,sum(metadata_urls) AS redirect_metadata_urls,"
                                           "sum(known_state IS NOT NULL) AS already_known_original_urls,sum(has_body) AS stored_original_bodies,"
                                           "sum(known_state IS NULL) AS unqueued_original_urls, min(earliest_day),max(latest_day) "
                                           "FROM destinations GROUP BY provider,enabled ORDER BY unique_urls DESC,provider"):
                    result['provider_counts'][row[0]] = dict(zip(("enabled", "unique_original_urls", "redirect_metadata_urls", "already_known_original_urls", "stored_original_bodies", "unqueued_original_urls", "earliest_metadata_day", "latest_metadata_day"), row[1:]))
                result['unique_original_urls'] = targets.execute("SELECT count(*) FROM destinations").fetchone()[0]
                result['literal_rows_accounted_for_by_destinations'] = targets.execute("SELECT coalesce(sum(metadata_urls),0) FROM destinations").fetchone()[0]
            finally:
                targets.close()
        result.update(examined_urls=examined, counts=dict(counts), errors=dict(errors), input_cohort_sha256=input_hash.hexdigest(),
                      input_cohort_hash_order="SQLite dispatch order: state,due,metadata_date_raw DESC,rowid; each article id and length-delimited raw metadata date",
                      whole_local_cohort_accounted_for=examined == expected and result['literal_rows_accounted_for_by_destinations'] == counts['literal_original'])
        result['cost'] = {"wall_seconds": time.perf_counter() - started, "cpu_seconds": time.process_time() - cpu,
                          "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          "publisher_requests": 0, "temporary_sqlite_cache_kib": 2048}
        return result
    finally:
        source.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = inventory(args.config, workspace=args.output.parent, progress=True)
    atomic_write_json(args.output, result)
    fields = ['provider', 'enabled', 'unique_original_urls', 'redirect_metadata_urls', 'already_known_original_urls',
              'stored_original_bodies', 'unqueued_original_urls', 'earliest_metadata_day', 'latest_metadata_day']
    buffer = io.StringIO(newline='')
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows({'provider': p, **v} for p, v in result['provider_counts'].items())
    atomic_write_text(args.output.with_suffix('.csv'), buffer.getvalue(), durable=True)
    print(json.dumps({"output": str(args.output), **{k: result[k] for k in ('examined_urls', 'counts', 'unique_original_urls', 'whole_local_cohort_accounted_for', 'cost')}}, ensure_ascii=False))
    return 0 if result['whole_local_cohort_accounted_for'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
