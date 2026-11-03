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
import stat
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_urls import original_source_inventory
from stockagent.data.news_source_catalog import adapter_summary, configuration_paths


ENDPOINT_INVENTORY_CONTRACT_VERSION = 2
FULL_FRONTIER_SCAN_CONTRACT_VERSION = 1
INVENTORY_SCOPE_CONTRACT_VERSION = 1
LITERAL_TARGET_COVERAGE_CONTRACT_VERSION = 1
METADATA_SORT_DAY_QUALITY_CONTRACT_VERSION = 1
METADATA_SORT_DAY_QUALITY_FIELDS = (
    'metadata_sort_days_present', 'metadata_sort_days_invalid',
    'metadata_sort_days_pre_1900', 'metadata_sort_days_after_snapshot_utc',
)


def _provider_scope(providers):
    if providers is None:
        return None
    if (not isinstance(providers, (list, tuple)) or not providers
            or any(not isinstance(p, str) or not p or p != p.strip() for p in providers)):
        raise ValueError('provider selection must be a nonempty list of source keys')
    return tuple(sorted(set(providers)))


def _literal_target_coverage(db, original_metadata, resolution_version):
    """Deduplicate selected incoming links in the same read snapshot.

    An alias association is not a distinct target, body or upstream-publisher
    identity. Keep this optional, potentially large join out of heartbeats and
    ordinary full inventories. Target existence comes from current article
    heads, never the historical target_queued hint.
    """
    hosts = [row['host'] for row in original_metadata.get('hosts', [])]
    counts = dict(alias_associations=0, distinct_target_urls=0,
                  queued_target_urls=0, stored_body_target_urls=0)
    if hosts:
        placeholders = ','.join('?' for _ in hosts)
        counts = dict(db.execute(
            'SELECT count(*) AS alias_associations,count(DISTINCT r.original_id) AS distinct_target_urls,'
            'count(DISTINCT CASE WHEN a.id IS NOT NULL THEN r.original_id END) AS queued_target_urls,'
            'count(DISTINCT CASE WHEN a.body_sha256 IS NOT NULL THEN r.original_id END) AS stored_body_target_urls '
            'FROM original_url_resolutions r LEFT JOIN articles a ON a.id=r.original_id '
            'WHERE r.contract_version=? AND r.original_host IN (' + placeholders + ')',
            (resolution_version, *hosts)).fetchone())
    if counts['alias_associations'] != original_metadata.get('literal_metadata_links', 0):
        raise ValueError('selected literal target coverage does not reconcile with host summary')
    return {'contract_version': LITERAL_TARGET_COVERAGE_CONTRACT_VERSION, **counts,
            'unqueued_target_urls': counts['distinct_target_urls'] - counts['queued_target_urls'],
            'grain': 'distinct literal target URL IDs, not alias incidences or accepted body identities',
            'direction': 'local redirect metadata to literal target; not target to its upstream publisher',
            'metadata_identity_verified': False, 'capture_access_granted': False}


def endpoint_inventory(registry, *, status=None, owner_matches=False, providers=None):
    """Describe every registered capability without reading the URL frontier.

    The independent public list has metadata, not category-API bodies or its
    thirty-page cap. All counters are owner-observed endpoint-family counters,
    never a claim of complete history, access permission or official capacity.
    """
    status = status or {}
    controls = (status.get('http_capacity') or {}).get('providers', {}) if owner_matches else {}
    result, seen = [], set()
    for provider, source in registry.sources.items():
        if providers is not None and provider not in providers:
            continue
        counters = status.get('performance', {}).get('providers', {}).get(provider, {}).get('counts', {}) if owner_matches else {}
        groups = (
            ('cnyes_news', source.cnyes_apis), ('wordpress_posts', source.wordpress_apis),
            ('rss_fulltext', source.fulltext_feeds), ('feed', source.feeds),
            ('cnyes_public_index', (source.cnyes_index_api,) if source.cnyes_index_api else ()),
            ('sitemap', source.sitemaps), ('index', source.index_pages),
        )
        for kind, urls in groups:
            for url in dict.fromkeys(urls):
                if (provider, url) in seen:
                    continue
                seen.add((provider, url))
                family = NewsHTTP.endpoint_kind(source, url)
                metadata_only = kind in {'feed', 'sitemap', 'index', 'cnyes_public_index'}
                row = {'provider': provider, 'kind': kind, 'endpoint_or_pattern': url,
                    'enabled': source.allowed, 'metadata_only': metadata_only,
                    'history_search_start': source.cnyes_history_start if kind == 'cnyes_news' else None,
                    'rows_per_request_cap_observed': 30 if kind in {'cnyes_news', 'cnyes_public_index'} else None,
                    'page_number_cap_observed': 30 if kind == 'cnyes_news' else None,
                    'maximum_history_window_days_tested': 730 if kind == 'cnyes_news' else None,
                    'refresh_seconds': source.discovery_interval_seconds,
                    'actual_client_interval_seconds': controls.get(provider, {}).get('effective_interval_seconds', source.interval_seconds),
                    'hard_interval_floor_seconds': controls.get(provider, {}).get('hard_interval_floor_seconds'),
                    'parallelism': controls.get(provider, {}).get('parallelism', source.parallelism),
                    'runtime_capacity_observed': bool(controls.get(provider)),
                    'owner_requests_in_endpoint_family': counters.get('endpoint_' + family + '_requests'),
                    'counter_grain': 'endpoint family; index/article namespaces aggregate paths, not per-URL calls',
                    'official_rps_limit': None, 'source_maximum_verified': False,
                    'source_evidence_url': source.authorization.get('evidence_url'), 'history_complete': False}
                if kind == 'cnyes_public_index':
                    from downloader.news_fulltext_cnyes_index import MAXIMUM_PAGE
                    row.update(seed_scope='actual_single_source_day_category_gaps',
                        metadata_fingerprint=source.cnyes_index_data_fingerprint,
                        maximum_page_engineering_guard=MAXIMUM_PAGE,
                        engineering_guard_is_not_source_limit=True,
                        body_acceptance_by_contract=0,
                        owner_verified_metadata_pages=counters.get('public_index_pages_verified'))
                result.append(row)
        for kind, pattern in (('article_namespace', source.article_path_pattern), ('pdf_namespace', source.pdf_path_pattern)):
            if pattern:
                result.append({'provider': provider, 'kind': kind, 'endpoint_or_pattern': pattern,
                    'enabled': source.allowed, 'metadata_only': False,
                    'actual_client_interval_seconds': controls.get(provider, {}).get('effective_interval_seconds', source.interval_seconds),
                    'parallelism': controls.get(provider, {}).get('parallelism', source.parallelism),
                    'official_rps_limit': None, 'source_maximum_verified': False,
                    'history_complete': False, 'source_evidence_url': source.authorization.get('evidence_url')})
    return result


def inventory_endpoints_only(config_path: Path, *, providers=None) -> dict:
    """O(configured endpoints), not O(millions of article/alias rows)."""
    started = time.perf_counter()
    selected = _provider_scope(providers)
    config, registry, repo = load_config(config_path)
    if selected is not None and set(selected) - registry.sources.keys():
        raise ValueError('endpoint selection contains an unregistered source key')
    root = repo / config['root']
    with sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True) as db:
        owner_row = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
    owner = owner_row[0] if owner_row else None
    try:
        status = json.loads((root / 'status.json').read_bytes())
    except (OSError, ValueError):
        status = {}
    matches = owner is not None and status.get('owner_id') == owner
    return {'endpoint_inventory_contract_version': ENDPOINT_INVENTORY_CONTRACT_VERSION,
        'inventory_scope': 'registered_endpoint_capabilities_only', 'observed_at_utc': utc_now(),
        'inventory_scope_contract_version': INVENTORY_SCOPE_CONTRACT_VERSION,
        'selected_source_keys': list(selected) if selected is not None else None,
        'all_known_source_keys_included': False,
        'queue_owner_id': owner, 'status_owner_matches': matches,
        'metric_observed_at_utc': status.get('performance', {}).get('snapshot_at_utc') if matches else None,
        'config_sha256': sha256_file(config_path), 'provider_inventory_included': False,
        'endpoints': endpoint_inventory(registry, status=status, owner_matches=matches, providers=selected),
        'publisher_requests': 0, 'production_queue_writes': False,
        'body_count_or_history_coverage_claimed': False, 'all_history_complete': False,
        'inventory_cost': {'wall_seconds': time.perf_counter() - started,
                           'scope': 'one owner-key read and configured endpoint descriptors; no article-frontier scan'}}


def _offline_snapshot_signature(root: Path) -> dict:
    """Capture admission evidence, not an exclusive lock on the producer."""
    def file_signature(path, *, required=False):
        try:
            value = path.stat()
        except FileNotFoundError:
            if required:
                raise ValueError('offline database is missing') from None
            return None
        if not stat.S_ISREG(value.st_mode):
            raise ValueError('offline snapshot files must be regular files')
        return {'device': value.st_dev, 'inode': value.st_ino, 'size': value.st_size,
                'mtime_ns': value.st_mtime_ns, 'ctime_ns': value.st_ctime_ns}

    return {'database': file_signature(root / 'queue.sqlite3', required=True),
            'wal': file_signature(root / 'queue.sqlite3-wal'),
            'journal': file_signature(root / 'queue.sqlite3-journal'),
            'status': file_signature(root / 'status.json')}


def _offline_configuration_signature(config_path: Path) -> dict:
    return {str(path): sha256_file(path) for path in configuration_paths(config_path)}


def inventory(config_path: Path, *, providers=None, resolve_link_coverage=False,
              offline_immutable=False, sort_day_quality=False) -> dict:
    wall_started, cpu_started = time.perf_counter(), time.process_time()
    selected = _provider_scope(providers)
    if type(resolve_link_coverage) is not bool or (resolve_link_coverage and selected is None):
        raise ValueError('literal target coverage requires an explicit provider selection')
    if type(offline_immutable) is not bool:
        raise ValueError('offline immutable selection must be boolean')
    if type(sort_day_quality) is not bool:
        raise ValueError('sort-day quality selection must be boolean')
    predicate = 'provider IN (' + ','.join('?' for _ in selected) + ')' if selected is not None else None
    parameters = selected or ()
    frontier = 'FROM articles WHERE ' + predicate if predicate else 'FROM articles NOT INDEXED'
    configuration = _offline_configuration_signature(config_path) if offline_immutable else None
    config, registry, repo = load_config(config_path)
    root = repo / config["root"]
    signature = _offline_snapshot_signature(root) if offline_immutable else None
    if signature and signature['wal'] and signature['wal']['size']:
        raise ValueError('offline immutable snapshot requires an empty WAL')
    if signature and signature['journal'] and signature['journal']['size']:
        raise ValueError('offline immutable snapshot requires an empty rollback journal')
    uri = ((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro&immutable=1'
           if offline_immutable else f"file:{root / 'queue.sqlite3'}?mode=ro")
    db = sqlite3.connect(uri, uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute("BEGIN")
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0]
        result = {"observed_at_utc": utc_now(), "queue_owner_id": owner,
                  "inventory_scope_contract_version": INVENTORY_SCOPE_CONTRACT_VERSION,
                  "inventory_scope": 'selected_source_keys' if selected is not None else 'all_known_source_keys',
                  "selected_source_keys": list(selected) if selected is not None else None,
                  "all_known_source_keys_included": selected is None,
                  "full_frontier_scan_contract_version": FULL_FRONTIER_SCAN_CONTRACT_VERSION if selected is None else None,
                  "literal_target_coverage_requested": resolve_link_coverage,
                  "inventory_implementation_sha256": sha256_file(Path(__file__)),
                  "config_sha256": sha256_file(config_path), "providers": {},
                  "metadata_date_basis": "local sorting dates, not verified publisher clocks or historical completeness",
                  "metadata_sort_day_quality_requested": sort_day_quality,
                  "metadata_sort_day_quality_contract_version": (METADATA_SORT_DAY_QUALITY_CONTRACT_VERSION
                                                                   if sort_day_quality else None),
                  "metadata_sort_day_quality_basis": (
                      "calendar syntax and date range flags for sorting hints only; pre-1900 or after the "
                      "UTC observation day does not by itself prove an incorrect publisher date" if sort_day_quality else None),
                  "article_grain": "normalized source URL; stock associations and body revisions are separate",
                  "unregistered_source_grain": "hostname keys; may include redirectors, duplicate publisher subdomains and aggregators, not unique publishers",
                  "publisher_requests": 0, "production_queue_writes": False,
                  "official_rate_limit_verified": False, "all_history_complete": False}
        try:
            status = json.loads((root / "status.json").read_text())
        except (OSError, ValueError):
            status = {}
        result["status_owner_matches"] = status.get("owner_id") == owner
        controls = (status.get("http_capacity") or {}).get("providers", {}) if result["status_owner_matches"] else {}
        # One full frontier aggregation, not a repeated full scan for each
        # metric. Provider/host groups retain the original inventory grain.
        # Full inventories read every applicable row sequentially. A selected
        # inventory instead uses provider indexes, leaving other heads alone.
        # The optional checks share the existing frontier scan. Keep the more
        # expensive calendar parsing out of ordinary inventories and polling.
        valid_sort_day = ("metadata_order_day GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' "
                          "AND substr(metadata_order_day,1,4)!='0000' "
                          "AND date(metadata_order_day,'+0 days')=metadata_order_day")
        quality_sql = (",sum(nullif(metadata_order_day,'') IS NOT NULL) AS metadata_sort_days_present,"
                       "sum(CASE WHEN nullif(metadata_order_day,'') IS NOT NULL AND ("
                       + valid_sort_day + ") IS NOT 1 THEN 1 ELSE 0 END) AS metadata_sort_days_invalid,"
                       "sum(CASE WHEN metadata_order_day<'1900-01-01' AND (" + valid_sort_day
                       + ") THEN 1 ELSE 0 END) AS metadata_sort_days_pre_1900,"
                       "sum(CASE WHEN metadata_order_day>? AND (" + valid_sort_day
                       + ") THEN 1 ELSE 0 END) AS metadata_sort_days_after_snapshot_utc") if sort_day_quality else ''
        quality_parameters = (result['observed_at_utc'][:10],) if sort_day_quality else ()
        for row in db.execute("SELECT provider,host,count(*) AS articles,sum(body_sha256 IS NOT NULL) AS stored_bodies,"
                              "min(nullif(metadata_order_day,'')) AS earliest_metadata_day,"
                              "max(nullif(metadata_order_day,'')) AS latest_metadata_day,max(captured_at_utc) AS latest_body_capture_utc,"
                              "sum(state IN ('retry','retry_cooldown') AND body_sha256 IS NOT NULL) AS stored_body_rechecks,"
                              "sum(state IN ('pending','retry','retry_cooldown','inflight') AND body_sha256 IS NULL) AS missing_body_work"
                              + quality_sql + ' ' + frontier + " GROUP BY provider,host", quality_parameters + parameters):
            item = result["providers"].setdefault(row["provider"], {"hosts": [], "states": {}, "errors": []})
            item["hosts"].append({"host": row["host"], "articles": row["articles"]})
            item["known_urls"] = item.get("known_urls", 0) + row["articles"]
            for key in ("stored_bodies", "stored_body_rechecks", "missing_body_work"):
                item[key] = item.get(key, 0) + (row[key] or 0)
            if sort_day_quality:
                for key in METADATA_SORT_DAY_QUALITY_FIELDS:
                    item[key] = item.get(key, 0) + (row[key] or 0)
            for key in ("earliest_metadata_day", "latest_metadata_day", "latest_body_capture_utc"):
                values = [v for v in (item.get(key), row[key]) if v is not None]
                item[key] = (min(values) if key == "earliest_metadata_day" else max(values)) if values else None
        for provider in registry.sources:
            if selected is None or provider in selected:
                result["providers"].setdefault(provider, {"hosts": [], "states": {}, "errors": []})
        original_sources = original_source_inventory(db, registry)
        if selected is not None:
            original_sources['sources'] = {p: v for p, v in original_sources['sources'].items() if p in selected}
            if original_sources['available']:
                original_sources.update(source_keys=len(original_sources['sources']),
                    literal_metadata_links=sum(v['literal_metadata_links'] for v in original_sources['sources'].values()),
                    original_hosts=sum(len(v['hosts']) for v in original_sources['sources'].values()))
        result['original_source_inventory'] = {key: value for key, value in original_sources.items() if key != 'sources'}
        for provider, evidence in original_sources['sources'].items():
            item = result['providers'].setdefault(provider, {'hosts': [], 'states': {}, 'errors': []})
            item['original_metadata'] = evidence
        if selected is not None and set(selected) - result['providers'].keys():
            raise ValueError('provider selection contains an unknown source key')
        if resolve_link_coverage:
            if not original_sources['available']:
                raise ValueError('literal target coverage requires a reconciled original-source summary')
            for item in result['providers'].values():
                evidence = item.setdefault('original_metadata', {'literal_metadata_links': 0, 'hosts': []})
                evidence['target_url_coverage'] = _literal_target_coverage(
                    db, evidence, original_sources['resolution_contract_version'])
        for row in db.execute("SELECT provider,state,articles FROM state_metrics WHERE articles>0"
                              + (' AND ' + predicate if predicate else ''), parameters):
            result["providers"][row["provider"]]["states"][row["state"]] = row["articles"]
        for row in db.execute("SELECT provider,state,error_code,body_sha256 IS NOT NULL AS has_stored_body,count(*) AS articles "
                              + frontier + (' AND' if predicate else ' WHERE')
                              + " error_code IS NOT NULL GROUP BY provider,state,error_code,has_stored_body", parameters):
            result["providers"][row["provider"]]["errors"].append(dict(row))
        discoveries = {}
        for row in db.execute("SELECT provider,kind,count(*) AS jobs,sum(error_code IS NOT NULL) AS errors,"
                              "sum(superseded_by_windows_json IS NOT NULL) AS superseded FROM discovery"
                              + (' WHERE ' + predicate if predicate else '') + " GROUP BY provider,kind", parameters):
            discoveries.setdefault(row["provider"], []).append(dict(row))
        entries = {p["id"]: p for p in config["providers"]}
        for provider, item in result["providers"].items():
            for metric in ('known_urls', 'stored_bodies', 'stored_body_rechecks', 'missing_body_work'):
                item.setdefault(metric, 0)
            if sort_day_quality:
                for metric in METADATA_SORT_DAY_QUALITY_FIELDS:
                    item.setdefault(metric, 0)
            for stamp in ('earliest_metadata_day', 'latest_metadata_day', 'latest_body_capture_utc'):
                item.setdefault(stamp, None)
            source = registry.sources.get(provider)
            item.update(configured=source is not None, enabled=bool(source and source.allowed),
                        acquisition_mode=source.acquisition_mode if source else "unreviewed",
                        name=source.name if source else provider,
                        client_interval_seconds=source.interval_seconds if source else None,
                        parallelism=source.parallelism if source else 0,
                        source_data_fingerprint=source.data_fingerprint if source else None,
                        source_authorization_fingerprint=source.authorization_fingerprint if source else None,
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
            "configured_providers": sum(p['configured'] for p in result['providers'].values()),
            "configured_enabled": sum(p['enabled'] for p in result['providers'].values()),
            "configured_providers_overall": len(registry.sources),
            "unregistered_source_keys": sum(not p["configured"] for p in result["providers"].values()),
            "known_urls": sum(p.get("known_urls", 0) for p in result["providers"].values()),
            "stored_bodies": sum(p.get("stored_bodies", 0) for p in result["providers"].values()),
            "awaiting_authorization_urls": sum(p["states"].get("awaiting_authorization", 0) for p in result["providers"].values()),
            "metadata_only_source_keys": sum(bool(p.get('original_metadata')) and not p['known_urls']
                                              for p in result['providers'].values()),
        }
        result["status"] = status if selected is None else {
            'owner_id': status.get('owner_id'), 'updated_at_utc': status.get('updated_at_utc'),
            'providers': {p: v for p, v in status.get('providers', {}).items() if p in selected},
            'scope': 'selected provider runtime context only; no global acquisition totals'}
        # Endpoint capability is distinct from discovered URL/page coverage.
        # Article path patterns are namespaces, never invented endpoint URLs.
        result['endpoint_inventory_contract_version'] = ENDPOINT_INVENTORY_CONTRACT_VERSION
        result['endpoints'] = endpoint_inventory(registry, status=status, owner_matches=result['status_owner_matches'], providers=selected)
        result["inventory_cost"] = {"wall_seconds": time.perf_counter() - wall_started,
                                    "cpu_seconds": time.process_time() - cpu_started,
                                    "scope": "single pinned read-only inventory; no publisher calls"}
        if offline_immutable:
            if _offline_snapshot_signature(root) != signature:
                raise ValueError('offline immutable snapshot changed during inventory')
            if _offline_configuration_signature(config_path) != configuration:
                raise ValueError('offline inventory configuration changed during inventory')
            result['offline_snapshot'] = {'contract_version': 2, 'wal_empty': True,
                'rollback_journal_empty': True, 'file_signature': signature,
                'configuration_sha256': configuration, 'writer_exclusion_proven': False,
                'quiescence_must_be_established_externally': True,
                'status_observed_at_utc': status.get('updated_at_utc')}
        return result
    finally:
        db.close()


def sort_day_summary(result: dict, *, inventory_sha256: str) -> dict:
    """Compact one full inventory without rescanning the queue for a page view."""
    if (result.get('inventory_scope') != 'all_known_source_keys'
            or result.get('all_known_source_keys_included') is not True
            or result.get('metadata_sort_day_quality_requested') is not True
            or type(result.get('metadata_sort_day_quality_contract_version')) is not int
            or result['metadata_sort_day_quality_contract_version'] != METADATA_SORT_DAY_QUALITY_CONTRACT_VERSION
            or type(result.get('publisher_requests')) is not int or result['publisher_requests'] != 0
            or result.get('production_queue_writes') is not False
            or not isinstance(inventory_sha256, str) or len(inventory_sha256) != 64
            or any(char not in '0123456789abcdef' for char in inventory_sha256)):
        raise ValueError('sort-day summary requires a complete quality inventory')
    providers = result.get('providers')
    if not isinstance(providers, dict):
        raise ValueError('invalid sort-day inventory')
    fields = ('known_heads', 'present', 'invalid', 'pre_1900', 'after_snapshot_utc')
    source_fields = ('known_urls', *METADATA_SORT_DAY_QUALITY_FIELDS)
    rows, totals = [], dict.fromkeys(fields, 0)
    for provider, item in sorted(providers.items()):
        if not isinstance(provider, str) or not isinstance(item, dict):
            raise ValueError('invalid sort-day source')
        values = [item.get(field) for field in source_fields]
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError('invalid sort-day counts')
        known, present, invalid, early, future = values
        if present > known or invalid + early + future > present:
            raise ValueError('inconsistent sort-day counts')
        if not known:
            continue
        row = {'provider': provider, **dict(zip(fields, values))}
        rows.append(row)
        for field in fields:
            totals[field] += row[field]
    observed = result['observed_at_utc']
    return {'contract': 'news_sort_day_inventory_summary_v1', 'observed_at_utc': observed,
            'utc_comparison_day': observed[:10], 'source_inventory_sha256': inventory_sha256,
            'provider_keys_with_heads': len(rows), 'totals': totals,
            'providers': rows, 'flagged_providers': [row for row in rows if any(row[field] for field in fields[2:])],
            'publisher_requests': 0, 'production_queue_writes': False,
            'date_semantics': 'sorting hints only; flags do not establish publication times or errors'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument('--endpoints-only', action='store_true',
                        help='Refresh registered endpoint capabilities without scanning the article/alias frontier')
    parser.add_argument('--provider', action='append',
                        help='Only these exact configured or locally observed source keys; repeat for multiple sources')
    parser.add_argument('--resolve-link-coverage', action='store_true',
                        help='Deduplicate incoming literal targets for selected providers; no publisher requests')
    parser.add_argument('--offline-immutable', action='store_true',
                        help='For an externally quiesced queue only: require empty journals and reject changed inputs')
    parser.add_argument('--sort-day-quality', action='store_true',
                        help='Inspect sorting-day syntax and date-range flags during the existing frontier scan')
    parser.add_argument('--sort-day-summary', type=Path,
                        help='Write a compact full-inventory summary for the read-only dashboard')
    args = parser.parse_args(argv)
    if args.resolve_link_coverage and (not args.provider or args.endpoints_only):
        parser.error('--resolve-link-coverage requires --provider and a provider inventory')
    if args.offline_immutable and args.endpoints_only:
        parser.error('--offline-immutable requires a provider inventory')
    if args.sort_day_quality and args.endpoints_only:
        parser.error('--sort-day-quality requires a provider inventory')
    if args.sort_day_summary:
        if not args.sort_day_quality or args.provider or args.endpoints_only:
            parser.error('--sort-day-summary requires an all-source --sort-day-quality inventory')
        destinations = (args.output, args.output.with_suffix('.csv'), args.output.with_suffix('.endpoints.csv'))
        if args.sort_day_summary.resolve() in {path.resolve() for path in destinations}:
            parser.error('--sort-day-summary must be separate from inventory outputs')
    if args.endpoints_only:
        result = inventory_endpoints_only(args.config, providers=args.provider)
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_json(args.output, result)
        buffer = io.StringIO(newline='')
        fields = list(dict.fromkeys(key for row in result['endpoints'] for key in row))
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(result['endpoints'])
        atomic_write_text(args.output.with_suffix('.endpoints.csv'), buffer.getvalue(), durable=True)
        print(json.dumps({'output': str(args.output), 'endpoints': len(result['endpoints']),
                          'scope': result['inventory_scope'], 'publisher_requests': 0}))
        return 0
    result = inventory(args.config, providers=args.provider, resolve_link_coverage=args.resolve_link_coverage,
                       offline_immutable=args.offline_immutable, sort_day_quality=args.sort_day_quality)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    if args.sort_day_summary:
        atomic_write_json(args.sort_day_summary, sort_day_summary(result, inventory_sha256=sha256_file(args.output)))
    fields = ["provider", "name", "configured", "enabled", "acquisition_mode", "known_urls", "stored_bodies", "stored_body_rechecks",
              "missing_body_work", "earliest_metadata_day", "latest_metadata_day", "latest_body_capture_utc",
              *METADATA_SORT_DAY_QUALITY_FIELDS,
              "source_role", "adapter", "adapter_registered", "adapter_verified", "adapter_acceptance", "next_action",
              "literal_metadata_links", "original_hosts_json", "literal_target_coverage_json",
              "inventory_scope", "all_known_source_keys_included",
              "client_interval_seconds", "parallelism", "authorization_kind", "source_evidence_url", "hosts_json",
              "states_json", "errors_json", "note"]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for provider, item in sorted(result["providers"].items(), key=lambda pair: (not pair[1]["configured"], -pair[1].get("known_urls", 0), pair[0])):
        writer.writerow({**{k: item.get(k) for k in fields if k in item}, "provider": provider,
                         "literal_metadata_links": item.get('original_metadata', {}).get('literal_metadata_links'),
                         "original_hosts_json": json.dumps(item.get('original_metadata', {}).get('hosts'), ensure_ascii=False),
                         "literal_target_coverage_json": json.dumps(item.get('original_metadata', {}).get('target_url_coverage'), ensure_ascii=False),
                         "inventory_scope": result['inventory_scope'],
                         "all_known_source_keys_included": result['all_known_source_keys_included'],
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
