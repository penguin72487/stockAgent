"""Lossless bulk observation layout for the canonical opening-source journal.

This is not a quote cache, source, or execution contract. Small/legacy events
keep their object rows. Bulk rows share field names once; strict JSON encoding
and the journal's synchronous file fsync remain owned by the existing writer.
"""
from __future__ import annotations

import math

NO_FIRST_PRINT_PROOF_TTL_SECONDS = 15.0

OBSERVATION_FIELDS = ("symbol", "state", "source", "open_price", "available",
                      "market_timestamp_ms", "received_timestamp_ms", "simtrade")
OBSERVATION_ENCODING = "tw_opening_observation_row_values_v1"
PROOF_FIELDS = OBSERVATION_FIELDS + ("no_first_print_observed_timestamp_ms",)
PROOF_ENCODING = "tw_opening_observation_row_values_v2"
_LAYOUTS = {OBSERVATION_ENCODING: OBSERVATION_FIELDS, PROOF_ENCODING: PROOF_FIELDS}
_FIELD_SET = frozenset(PROOF_FIELDS)


def journal_absence_proof_ms(*, state, proof, source, available, market, received,
                             price, simtrade, gate_ms, elapsed):
    """Recover only original direct MIS evidence, without loading a provider."""
    if (state == "observed_no_first_print" and proof is None
            and source == "twse_tpex:mis" and available is True and simtrade not in (True, 1)
            and type(received) is int and type(market) is int
            and gate_ms <= market <= received <= gate_ms + elapsed
            and not (isinstance(price, (int, float)) and math.isfinite(price) and price > 0)):
        return received
    return proof


def pack_observation_details(details: dict) -> dict:
    """Do not drop extra/legacy fields or mutate the caller's evidence."""
    rows = details.get("observations")
    if (not isinstance(rows, list) or len(rows) < 128
            or "observation_encoding" in details or type(rows[0]) is not dict):
        return details
    fields = PROOF_FIELDS if rows[0].keys() == _FIELD_SET else OBSERVATION_FIELDS
    field_set = _FIELD_SET if fields == PROOF_FIELDS else frozenset(OBSERVATION_FIELDS)
    if any(type(row) is not dict or row.keys() != field_set for row in rows):
        return details
    return {**details, "observation_encoding": PROOF_ENCODING if fields == PROOF_FIELDS else OBSERVATION_ENCODING,
            "observation_columns": list(fields), "observation_row_count": len(rows),
            "observations": [[row[field] for field in fields] for row in rows]}


def observation_values(event: dict, fields=OBSERVATION_FIELDS, *, optional_fields=()):
    """Read old objects/new row-values without rebuilding unused full objects.

    Reject unknown/malformed layouts, never silently zip/truncate a broken row.
    Consumers may project just symbol/state; original values/clocks remain in
    the durable event and are available to an independent complete readback.
    """
    if any(field not in _FIELD_SET for field in (*fields, *optional_fields)):
        raise ValueError("unknown opening observation projection")
    rows = event.get("observations", [])
    if not isinstance(rows, list):
        raise ValueError("invalid opening observation rows")
    encoding = event.get("observation_encoding")
    if encoding is None:
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("invalid legacy opening observation")
            yield tuple(row[field] for field in fields) + tuple(row.get(field) for field in optional_fields)
        return
    layout = _LAYOUTS.get(encoding)
    if (layout is None or event.get("observation_columns") != list(layout)
            or type(event.get("observation_row_count")) is not int
            or event["observation_row_count"] != len(rows)):
        raise ValueError("unknown or malformed opening observation layout")
    if any(field not in layout for field in fields):
        raise ValueError("required opening observation field absent from layout")
    indices = tuple(layout.index(field) for field in fields)
    optional_indices = tuple(layout.index(field) if field in layout else None for field in optional_fields)
    for row in rows:
        if not isinstance(row, list) or len(row) != len(layout):
            raise ValueError("opening observation row width mismatch")
        yield tuple(row[index] for index in indices) + tuple(row[index] if index is not None else None for index in optional_indices)
