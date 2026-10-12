"""Read-only inventory of every configured and locally observed news source.

One coherent WAL snapshot; no publisher requests or queue mutations. Metadata
days are not publication evidence. Stored-body rechecks are not missing text.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_urls import original_source_inventory
from stockagent.data.news_source_catalog import adapter_summary


def inventory(config_path: Path) -> dict:
    wall_started, cpu_started = time.perf_counter(), time.process_time()
    config, registry, repo = load_config(config_path)
    root = repo / config["root"]
    db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("BEGIN")
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0]
        result = {"observed_at_utc": utc_now(), "queue_owner_id": owner,
                  "config_sha256": sha256_file(config_path), "providers": {},
                  "metadata_date_basis": "local sorting dates, not verified publisher clocks or historical completeness",
                  "article_grain": "normalized source URL; stock associations and body revisions are separate",
                  "unregistered_source_grain": "hostname keys; may include redirectors, duplicate publisher subdomains and aggregators, not unique publishers",
                  "official_rate_limit_verified": False, "all_history_complete": False}
        try:
            status = json.loads((root / "status.json").read_text())
        except (OSError, ValueError):
            status = {}
        result["status_owner_matches"] = status.get("owner_id") == owner
        controls = (status.get("http_capacity") or {}).get("providers", {}) if result["status_owner_matches"] else {}
        # One full frontier aggregation, not a repeated full scan for each
        # metric. Provider/host groups retain the original inventory grain.
        for row in db.execute("SELECT provider,host,count(*) AS articles,sum(body_sha256 IS NOT NULL) AS stored_bodies,"
                              "min(nullif(metadata_order_day,'')) AS earliest_metadata_day,"
                              "max(nullif(metadata_order_day,'')) AS latest_metadata_day,max(captured_at_utc) AS latest_body_capture_utc,"
                              "sum(state IN ('retry','retry_cooldown') AND body_sha256 IS NOT NULL) AS stored_body_rechecks,"
                              "sum(state IN ('pending','retry','retry_cooldown','inflight') AND body_sha256 IS NULL) AS missing_body_work "
                              "FROM articles GROUP BY provider,host"):
            item = result["providers"].setdefault(row["provider"], {"hosts": [], "states": {}, "errors": []})
            item["hosts"].append({"host": row["host"], "articles": row["articles"]})
            item["known_urls"] = item.get("known_urls", 0) + row["articles"]
            for key in ("stored_bodies", "stored_body_rechecks", "missing_body_work"):
                item[key] = item.get(key, 0) + (row[key] or 0)
            for key in ("earliest_metadata_day", "latest_metadata_day", "latest_body_capture_utc"):
                values = [v for v in (item.get(key), row[key]) if v is not None]
                item[key] = (min(values) if key == "earliest_metadata_day" else max(values)) if values else None
        for provider in registry.sources:
            result["providers"].setdefault(provider, {"hosts": [], "states": {}, "errors": []})
        original_sources = original_source_inventory(db, registry)
        result['original_source_inventory'] = {key: value for key, value in original_sources.items() if key != 'sources'}
        for provider, evidence in original_sources['sources'].items():
            item = result['providers'].setdefault(provider, {'hosts': [], 'states': {}, 'errors': []})
            item['original_metadata'] = evidence
        for row in db.execute("SELECT provider,state,articles FROM state_metrics WHERE articles>0"):
            result["providers"][row["provider"]]["states"][row["state"]] = row["articles"]
        for row in db.execute("SELECT provider,state,error_code,body_sha256 IS NOT NULL AS has_stored_body,count(*) AS articles "
                              "FROM articles WHERE error_code IS NOT NULL GROUP BY provider,state,error_code,has_stored_body"):
            result["providers"][row["provider"]]["errors"].append(dict(row))
        discoveries = {}
        for row in db.execute("SELECT provider,kind,count(*) AS jobs,sum(error_code IS NOT NULL) AS errors,"
                              "sum(superseded_by_windows_json IS NOT NULL) AS superseded FROM discovery GROUP BY provider,kind"):
            discoveries.setdefault(row["provider"], []).append(dict(row))
        entries = {p["id"]: p for p in config["providers"]}
        for provider, item in result["providers"].items():
            for metric in ('known_urls', 'stored_bodies', 'stored_body_rechecks', 'missing_body_work'):
                item.setdefault(metric, 0)
            for stamp in ('earliest_metadata_day', 'latest_metadata_day', 'latest_body_capture_utc'):
                item.setdefault(stamp, None)
            source = registry.sources.get(provider)
            item.update(configured=source is not None, enabled=bool(source and source.allowed),
                        acquisition_mode=source.acquisition_mode if source else "unreviewed",
                        name=source.name if source else provider,
                        client_interval_seconds=source.interval_seconds if source else None,
                        parallelism=source.parallelism if source else 0,
                        authorization=source.authorization if source else {"kind": "unreviewed"},
                        note=entries.get(provider, {}).get("note", "Local metadata only; source and article rights/access unreviewed"))
            if source and source.acquisition_mode == "finite" and db.execute("SELECT 1 FROM sqlite_master WHERE name='finite_cohorts'").fetchone():
                from downloader.news_fulltext_cohorts import cohort_progress
                item["finite_cohort"] = cohort_progress(db, provider)
            original_hosts = item.get('original_metadata', {}).get('hosts', [])
            item.update(adapter_summary(source, host=(item["hosts"][0]["host"] if item["hosts"] else
                                                      original_hosts[0]['host'] if original_hosts else "")))
            item["adapter_acceptance"] = "committed_body_heads_observed" if item.get("stored_bodies", 0) else "no_body_acceptance_observed"
            item["configured_article_selectors"] = list(source.selectors) if source else []
            control = controls.get(provider, {})
            item["configured_interval_seconds"] = item["client_interval_seconds"]
            item["configured_parallelism"] = item["parallelism"]
            item["client_interval_seconds"] = control.get("effective_interval_seconds", item["client_interval_seconds"])
            item["parallelism"] = control.get("parallelism", item["parallelism"])
            item["runtime_capacity_observed"] = bool(control)
            item["discovery"] = discoveries.get(provider, [])
            # The grouped error inventory already proves there is no sample
            # to read for most sources. Avoid ~830 scans of empty error sets.
            has_failure_samples = any(not e["has_stored_body"] and e["state"] != "awaiting_authorization" for e in item["errors"])
            item["failure_samples"] = [dict(r) for r in db.execute(
                "SELECT url,state,error_code,attempts FROM articles WHERE provider=? AND error_code IS NOT NULL "
                "AND body_sha256 IS NULL AND state NOT IN ('awaiting_authorization') LIMIT 8", (provider,))] if has_failure_samples else []
        result["inventory_totals"] = {
            "configured_providers": len(registry.sources), "configured_enabled": sum(s.allowed for s in registry.sources.values()),
            "unregistered_source_keys": sum(not p["configured"] for p in result["providers"].values()),
            "known_urls": sum(p.get("known_urls", 0) for p in result["providers"].values()),
            "stored_bodies": sum(p.get("stored_bodies", 0) for p in result["providers"].values()),
            "awaiting_authorization_urls": sum(p["states"].get("awaiting_authorization", 0) for p in result["providers"].values()),
            "metadata_only_source_keys": sum(bool(p.get('original_metadata')) and not p['known_urls']
                                              for p in result['providers'].values()),
        }
        result["status"] = status
        # Endpoint capability is distinct from discovered URL/page coverage.
        # Article path patterns are namespaces, never invented endpoint URLs.
        result["endpoints"] = []
        seen_endpoints = set()
        for p, source in registry.sources.items():
            counters = status.get("performance", {}).get("providers", {}).get(p, {}).get("counts", {}) if result["status_owner_matches"] else {}
            for kind, urls in (("cnyes_news", source.cnyes_apis), ("wordpress_posts", source.wordpress_apis),
                               ("rss_fulltext", source.fulltext_feeds), ("feed", source.feeds),
                               ("sitemap", source.sitemaps), ("index", source.index_pages)):
                for url in dict.fromkeys(urls):
                    if (p, url) in seen_endpoints:
                        continue
                    seen_endpoints.add((p, url))
                    family = NewsHTTP.endpoint_kind(source, url)
                    result["endpoints"].append({"provider": p, "kind": kind, "endpoint_or_pattern": url,
                        "enabled": source.allowed, "history_search_start": source.cnyes_history_start if kind == "cnyes_news" else None,
                        "rows_per_request_cap_observed": 30 if kind == "cnyes_news" else None,
                        "page_number_cap_observed": 30 if kind == "cnyes_news" else None,
                        "maximum_history_window_days_tested": 730 if kind == "cnyes_news" else None,
                        "refresh_seconds": source.discovery_interval_seconds,
                        "actual_client_interval_seconds": controls.get(p, {}).get("effective_interval_seconds", source.interval_seconds),
                        "hard_interval_floor_seconds": controls.get(p, {}).get("hard_interval_floor_seconds"),
                        "parallelism": controls.get(p, {}).get("parallelism", source.parallelism),
                        "owner_requests_in_endpoint_family": counters.get("endpoint_" + family + "_requests"),
                        "counter_grain": "endpoint family; index/article namespaces aggregate paths, not per-URL calls",
                        "official_rps_limit": None, "source_maximum_verified": False,
                        "source_evidence_url": source.authorization.get("evidence_url"), "history_complete": False})
            for kind, pattern in (("article_namespace", source.article_path_pattern), ("pdf_namespace", source.pdf_path_pattern)):
                if pattern:
                    result["endpoints"].append({"provider": p, "kind": kind, "endpoint_or_pattern": pattern,
                        "enabled": source.allowed, "actual_client_interval_seconds": controls.get(p, {}).get("effective_interval_seconds", source.interval_seconds),
                        "parallelism": controls.get(p, {}).get("parallelism", source.parallelism),
                        "official_rps_limit": None, "source_maximum_verified": False,
                        "history_complete": False, "source_evidence_url": source.authorization.get("evidence_url")})
        result["inventory_cost"] = {"wall_seconds": time.perf_counter() - wall_started,
                                    "cpu_seconds": time.process_time() - cpu_started,
                                    "scope": "single pinned read-only inventory; no publisher calls"}
        return result
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = inventory(args.config)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    fields = ["provider", "name", "configured", "enabled", "acquisition_mode", "known_urls", "stored_bodies", "stored_body_rechecks",
              "missing_body_work", "earliest_metadata_day", "latest_metadata_day", "latest_body_capture_utc",
              "source_role", "adapter", "adapter_registered", "adapter_verified", "adapter_acceptance", "next_action",
              "literal_metadata_links", "original_hosts_json",
              "client_interval_seconds", "parallelism", "authorization_kind", "source_evidence_url", "hosts_json",
              "states_json", "errors_json", "note"]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for provider, item in sorted(result["providers"].items(), key=lambda pair: (not pair[1]["configured"], -pair[1].get("known_urls", 0), pair[0])):
        writer.writerow({**{k: item.get(k) for k in fields if k in item}, "provider": provider,
                         "literal_metadata_links": item.get('original_metadata', {}).get('literal_metadata_links'),
                         "original_hosts_json": json.dumps(item.get('original_metadata', {}).get('hosts'), ensure_ascii=False),
                         "authorization_kind": item["authorization"].get("kind"),
                         "source_evidence_url": item["authorization"].get("evidence_url", ""),
                         **{k + "_json": json.dumps(item[k], ensure_ascii=False) for k in ("hosts", "states", "errors")}})
    atomic_write_text(args.output.with_suffix('.csv'), buffer.getvalue(), durable=True)
    buffer = io.StringIO(newline="")
    fields = list(dict.fromkeys(key for row in result["endpoints"] for key in row))
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(result["endpoints"])
    atomic_write_text(args.output.with_suffix('.endpoints.csv'), buffer.getvalue(), durable=True)
    print(json.dumps({"output": str(args.output), "sources": len(result["providers"]),
                      "providers": {p: {k: v.get(k) for k in ("configured", "enabled", "known_urls", "stored_bodies", "stored_body_rechecks", "missing_body_work")}
                                    for p, v in result["providers"].items() if v["configured"]}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
