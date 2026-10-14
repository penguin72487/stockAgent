"""Lightweight dependency routing shared by TW validation and event scheduling.

These receipts describe inputs, not proof that a generation is valid. The
canonical validator still checks their content, coverage and causal dates.
"""

from __future__ import annotations

from typing import Any, Iterator, Mapping


DERIVED_INPUT_RECEIPTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "corporate_action_reference": (
        "tw_corporate_action_reference.summary.json",
        ("source_receipts",),
    ),
    "stock_panel": (
        "stocks/official_symbol_build_summary.json",
        (
            "source_receipts",
            "fallback_source_receipts",
            "legacy_source_receipts",
            "lifecycle_source_receipts",
            "session_calendar_receipt",
            "session_calendar_summary_receipt",
        ),
    ),
    "corporate_action_entitlements": (
        "tw_corporate_action_entitlements.summary.json",
        ("reference_receipt", "universe_receipt", "raw_receipt_manifest"),
    ),
    "public_features": (
        "features/tw_public_stock_daily.summary.json",
        ("source_receipts",),
    ),
}


def iter_input_receipts(
    summary: Mapping[str, Any], keys: tuple[str, ...]
) -> Iterator[tuple[str, Mapping[str, Any]]]:
    """Keep the existing validator's singleton/list and malformed-row rules."""

    for key in keys:
        payload = summary.get(key)
        if isinstance(payload, Mapping):
            yield key, payload
        elif isinstance(payload, list):
            for receipt in payload:
                if isinstance(receipt, Mapping):
                    yield key, receipt
