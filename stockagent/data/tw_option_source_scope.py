"""Provisional category annotations for the shared option daily source view.

This is a research-source annotation, not a dated listing, currency, execution
or training universe.  All rows, including newly observed or unclassified
products, stay in the returned frame; consumers can inspect the match flag.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
import io
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping, Sequence

import polars as pl


CONTRACT = 'tw_option_daily_provisional_source_scope_v1'
_CLASSES = frozenset({'index_option', 'security_option_candidate', 'etf_option_candidate',
                      'commodity_option', 'currency_option', 'unclassified'})
_SCOPES = {
    'index_options': frozenset({'index_option'}),
    'stock_options': frozenset({'security_option_candidate'}),
    'etf_options': frozenset({'etf_option_candidate'}),
    'equity_options': frozenset({'security_option_candidate', 'etf_option_candidate'}),
    'commodity_options': frozenset({'commodity_option'}),
    'currency_options': frozenset({'currency_option'}),
    'other_options': frozenset({'commodity_option', 'currency_option'}),
    'unclassified_options': frozenset({'unclassified'}),
    'all_options': _CLASSES,
}


@dataclass(frozen=True)
class OptionSourceScope:
    scopes: tuple[str, ...]
    inventory_sha256: str
    fingerprint: str
    classes_by_product: Mapping[str, str]
    selected_classes: frozenset[str]


def load_option_source_scope(inventory: str | Path, *, expected_sha256: str,
                             scopes: Sequence[str]) -> OptionSourceScope:
    """Load an explicitly pinned inventory and canonicalize an exact scope union."""
    path = Path(inventory)
    if path.is_symlink() or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256):
        raise ValueError('an explicitly pinned regular option inventory is required')
    if isinstance(scopes, str) or not scopes or any(scope not in _SCOPES for scope in scopes):
        raise ValueError('an explicit nonempty list of known option scopes is required')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('option inventory digest mismatch')
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig'), newline=''))
    if not reader.fieldnames or not {'product', 'provisional_class'}.issubset(reader.fieldnames):
        raise ValueError('option inventory columns are missing')
    classes = {}
    for row in reader:
        product, category = row['product'], row['provisional_class']
        if (not re.fullmatch(r'[A-Z0-9]{2,8}', product or '') or category not in _CLASSES
                or product in classes):
            raise ValueError('invalid or duplicate option inventory entry')
        classes[product] = category
    if not classes or path.read_bytes() != raw:
        raise ValueError('option inventory is empty or changed while loading')
    selected = tuple(sorted(set(scopes)))
    fingerprint = hashlib.sha256(json.dumps(
        {'contract': CONTRACT, 'inventory_sha256': expected_sha256, 'scopes': selected},
        sort_keys=True, separators=(',', ':'),
    ).encode()).hexdigest()
    return OptionSourceScope(selected, expected_sha256, fingerprint, MappingProxyType(classes),
                             frozenset().union(*(_SCOPES[scope] for scope in selected)))


def annotate_option_source_scope(frame: pl.DataFrame, scope: OptionSourceScope) -> pl.DataFrame:
    """Add candidate class and match without filtering or changing original rows."""
    if 'product' not in frame.columns or {'source_scope_class', 'source_scope_match'} & set(frame.columns):
        raise ValueError('expected original option source rows without scope annotations')
    return frame.with_columns(
        pl.col('product').replace_strict(dict(scope.classes_by_product), default='unclassified',
                                         return_dtype=pl.String).alias('source_scope_class'),
    ).with_columns(
        pl.col('source_scope_class').is_in(sorted(scope.selected_classes)).alias('source_scope_match'),
    )
