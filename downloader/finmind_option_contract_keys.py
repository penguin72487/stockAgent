"""Comparison-only identity normalization for FinMind option contract series.

Keep original provider values and source receipts unchanged. A six-digit
calendar month sometimes arrives with the exact numeric serialization suffix
``.0``. Weekly labels, spreads and other decimal suffixes are not rewritten.
"""
from __future__ import annotations

import polars as pl


def vendor_option_series(expr: pl.Expr) -> pl.Expr:
    return (expr.str.strip_chars().str.to_uppercase()
            .str.replace(r'^(\d{4}(?:0[1-9]|1[0-2]))\.0$', '${1}'))
