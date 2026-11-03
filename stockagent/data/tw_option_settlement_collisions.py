"""Identify ambiguous product/month identities in official option records.

This is a retrospective source diagnostic, not a listing or execution rule.
In particular, a later final-settlement record is not point-in-time evidence
available to an earlier trading decision.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Iterable, Mapping


def settlement_collisions(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Retain every source row for keys with more than one settlement record."""
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        product, series = row.get('product_raw'), row.get('series_raw')
        day = row.get('settlement_date')
        source_month, source_row = row.get('query_month'), row.get('source_row')
        if (not isinstance(product, str) or not product or not isinstance(series, str)
                or not series or not isinstance(day, date)
                or not isinstance(source_month, str) or not source_month
                or isinstance(source_row, bool) or not isinstance(source_row, int)):
            raise ValueError('invalid settlement identity or source provenance')
        groups[(product, series)].append({
            'query_month': source_month, 'source_row': source_row,
            'settlement_date': day.isoformat(),
            'settlement_price_raw': row.get('settlement_price_raw'),
            'underlying_raw': row.get('underlying_raw'),
        })
    output = []
    for (product, series), records in sorted(groups.items()):
        if len(records) > 1:
            output.append({
                'product_raw': product, 'series_raw': series, 'records': records,
                'distinct_settlement_dates': sorted({r['settlement_date'] for r in records}),
                'state': 'generation_or_duplicate_ambiguous',
            })
    return output
