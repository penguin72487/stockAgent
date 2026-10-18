"""Independent, bounded DuckDB readback of committed news Parquet tables."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from stockagent.data.news_dataset_storage import news_table_paths


QUERIES = {
    "article_rows": "SELECT count(*) FROM articles",
    "duplicate_article_ids": "SELECT count(*)-count(DISTINCT article_id) FROM articles",
    "duplicate_document_ids": "SELECT count(*)-count(DISTINCT document_id) FROM documents",
    "duplicate_stock_links": "SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM article_stocks "
                             "GROUP BY article_id,stock_id,metadata_provider HAVING count(*)>1)",
    "invalid_article_keys": "SELECT count(*) FROM articles WHERE article_id IS NULL OR length(article_id)<>64 "
                            "OR document_id IS NULL OR url IS NULL OR provider IS NULL",
    "invalid_document_kinds": "SELECT count(*) FROM documents WHERE text_kind IS NULL "
                              "OR text_kind NOT IN ('fulltext','headline')",
    "headline_body_not_null": "SELECT count(*) FROM articles WHERE text_kind='headline' AND body IS NOT NULL",
    "fulltext_body_missing": "SELECT count(*) FROM articles WHERE text_kind='fulltext' AND (body IS NULL OR trim(body)='')",
    "orphan_article_documents": "SELECT count(*) FROM articles a LEFT JOIN documents d "
                                "ON d.document_id=a.document_id WHERE d.document_id IS NULL",
    "article_document_kind_mismatch": "SELECT count(*) FROM articles a JOIN documents d "
                                     "ON d.document_id=a.document_id WHERE a.text_kind<>d.text_kind OR a.text_kind IS NULL",
    "article_body_content_mismatch": "SELECT count(*) FROM articles a JOIN documents d ON d.document_id=a.document_id "
                                     "WHERE a.text_kind='fulltext' AND a.body IS DISTINCT FROM d.text",
    "orphan_stock_links": "SELECT count(*) FROM article_stocks s LEFT JOIN articles a "
                          "ON a.article_id=s.article_id WHERE a.article_id IS NULL",
    "invalid_stock_ids": "SELECT count(*) FROM article_stocks WHERE stock_id IS NULL OR trim(stock_id)=''",
    "missing_observation_clocks": "SELECT count(*) FROM articles WHERE observed_at_utc IS NULL OR available_at_utc IS NULL",
    "availability_before_observation": "SELECT count(*) FROM articles WHERE available_at_utc<observed_at_utc",
    "metadata_missing_observation_clocks": "SELECT count(*) FROM metadata_observations WHERE observed_at_utc IS NULL",
    "orphan_resolved_metadata": "SELECT count(*) FROM metadata_observations m LEFT JOIN articles a "
                               "ON a.article_id=m.resolved_article_id WHERE m.resolved_article_id IS NOT NULL AND a.article_id IS NULL",
}
PROFILE_QUERIES = {
    "text_kinds": "SELECT text_kind,count(*) articles,count(DISTINCT document_id) unique_documents,"
                  "count(*) FILTER (WHERE title IS NULL OR title='') missing_title,"
                  "count(*) FILTER (WHERE body IS NULL) missing_body,"
                  "count(*) FILTER (WHERE published_at_utc IS NULL) without_aware_publication,"
                  "CAST(min(publication_date) AS VARCHAR) earliest_source_date,"
                  "CAST(max(publication_date) AS VARCHAR) latest_source_date,"
                  "CAST(min(observed_at_utc) AS VARCHAR) earliest_observation_utc,"
                  "CAST(max(observed_at_utc) AS VARCHAR) latest_observation_utc FROM articles GROUP BY text_kind ORDER BY text_kind",
    "publication_clocks": "SELECT text_kind,publication_clock_status,count(*) articles FROM articles "
                          "GROUP BY text_kind,publication_clock_status ORDER BY text_kind,publication_clock_status",
    "source_time_anomalies": "SELECT count(*) FILTER (WHERE published_at_utc>observed_at_utc) publication_after_capture,"
                             "count(*) FILTER (WHERE publication_date<DATE '1900-01-01') source_date_before_1900 FROM articles",
    "unresolved_metadata": "SELECT metadata_provider,count(*) AS observation_rows,count(*) FILTER (WHERE article_id IS NULL) invalid_url_rows,"
                           "count(*) FILTER (WHERE resolved_article_id IS NULL) without_retained_article FROM metadata_observations "
                           "GROUP BY metadata_provider ORDER BY metadata_provider",
    "usage_scopes": "SELECT text_kind,rights_status,training_injection_enabled,remote_training_export,publish,count(*) articles "
                    "FROM articles GROUP BY ALL ORDER BY text_kind,rights_status",
}


def audit_news_tables(root: Path, manifest: dict, schemas: dict, *, memory_limit_mb: int = 2048,
                      notify=None) -> dict:
    """Check actual exported keys/joins, not the builder's SQLite index.

    Distinct URL articles are not distinct stories; repeated source observation
    rows are intentionally not deduplicated. Only the three declared entity/
    relation keys have uniqueness requirements. Unknown source clocks are
    profiled, not rewritten, dropped or assigned arbitrary timezones.
    """
    if memory_limit_mb < 128:
        raise ValueError("news verification memory limit must be at least 128 MB")
    connection = duckdb.connect()
    spill = root / ".verification-spill"
    spill.mkdir(exist_ok=True)
    try:
        connection.execute("SET threads=1")
        connection.execute("SET memory_limit=?", [f"{memory_limit_mb}MB"])
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("SET temp_directory=?", [str(spill.resolve())])
        for table, schema in schemas.items():
            paths = [str(path) for path in news_table_paths(root, table, manifest=manifest)]
            if paths:
                connection.read_parquet(paths, hive_partitioning=False).create_view(table)
            else:
                connection.register(table, pa.Table.from_pylist([], schema=schema))
        counts = {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in schemas}
        checks = {}
        total = len(QUERIES) + len(PROFILE_QUERIES)
        for key, query in QUERIES.items():
            if notify:
                notify("quality:" + key, len(checks), total)
            checks[key] = int(connection.execute(query).fetchone()[0])
        failures = {key: n for key, n in checks.items() if key != "article_rows" and n}
        if failures:
            raise ValueError(f"news relational validation failed: {failures}")
        if any(counts[table] != manifest["row_counts"].get(table, 0) for table in schemas):
            raise ValueError("independent news row counts disagree with the manifest")
        profile = {}
        for name, sql in PROFILE_QUERIES.items():
            if notify:
                notify("quality:" + name, len(checks) + len(profile), total)
            cursor = connection.execute(sql)
            names = [item[0] for item in cursor.description]
            profile[name] = [dict(zip(names, row)) for row in cursor.fetchall()]
        if notify:
            notify("quality:complete", total, total)
        return {"state": "verified", "engine": "duckdb", "memory_limit": f"{memory_limit_mb}MB", "threads": 1,
                "row_counts": counts, "checks": checks, "profile": profile,
                "queries": {**QUERIES, **PROFILE_QUERIES},
                "limitations": ["Source clocks/rights are retained, not attested by this formatting step.",
                                "Backfilled current bodies are not historical point-in-time versions.",
                                "Source metadata rows may lack a retained article; these remain observations only."]}
    finally:
        connection.close()
        # DuckDB removes its own spill files on close; only remove the now empty
        # directory created by this verifier, never any data/Parquet or input.
        try:
            spill.rmdir()
        except OSError:
            pass
