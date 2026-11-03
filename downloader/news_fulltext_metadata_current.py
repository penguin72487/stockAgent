"""Shared selection of complete news metadata receipt versions.

The single writer records explicit receipt selections. Databases created before
that contract retain the best available inference: highest complete origin id.
"""
from __future__ import annotations


def unresolved_alias_count(db):
    """Legacy databases have no alias observations, not a fabricated repair."""
    exists = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata_receipt_alias_pending'"
    ).fetchone()
    return db.execute('SELECT count(*) FROM metadata_receipt_alias_pending').fetchone()[0] if exists else 0


def current_origins_cte(db):
    """Return a CTE named current_metadata, rejecting inconsistent pointers."""
    has_selection = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata_current_origins'"
    ).fetchone() is not None
    if has_selection:
        invalid = db.execute(
            "SELECT 1 FROM metadata_current_origins h "
            "LEFT JOIN origins o ON o.id=h.origin_id "
            "LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id "
            "WHERE o.id IS NULL OR o.receipt_path!=h.receipt_path "
            "OR (p.origin_id IS NOT NULL AND p.state!='complete') LIMIT 1"
        ).fetchone()
        if invalid:
            raise ValueError('invalid_current_metadata_origin')
    selection_join = ("LEFT JOIN metadata_current_origins h ON h.receipt_path=l.receipt_path"
                      if has_selection else "")
    selected_id = "coalesce(h.origin_id,l.id)" if has_selection else "l.id"
    has_retired = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata_retired_receipt_targets'"
    ).fetchone() is not None
    active_only = ("WHERE NOT EXISTS (SELECT 1 FROM metadata_retired_receipt_targets r "
                   "WHERE r.receipt_path=l.receipt_path)" if has_retired else "")
    return f"""
        WITH latest AS (
            SELECT o.receipt_path,max(o.id) id FROM origins o
            LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id
            WHERE p.origin_id IS NULL OR p.state='complete'
            GROUP BY o.receipt_path
        ),
        current_metadata AS (
            SELECT o.* FROM latest l {selection_join}
            JOIN origins o ON o.id={selected_id}
            {active_only}
        )
    """
