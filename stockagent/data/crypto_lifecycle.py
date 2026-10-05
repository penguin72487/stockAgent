"""Dated Bybit announcement policy, never inferred from missing labels."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import numpy as np

ANNOUNCEMENT_COLUMNS = (
    "crypto_delisting_announced_date",
    "crypto_delisting_time_utc",
    "crypto_delisting_source_url",
)


def announced_exit_mask(dates: np.ndarray, columns: dict) -> np.ndarray | None:
    """Request zero exposure from the first midnight after publication day.

    The announcement pages expose a date, not a verified intraday publication
    timestamp. Waiting until the following UTC day avoids trading earlier on
    the publication day. This is an explicit strategy exit, not the exchange's
    later compulsory settlement. The executor must still enforce all fills.
    """
    present = set(ANNOUNCEMENT_COLUMNS) & columns.keys()
    if not present:
        return None
    if present != set(ANNOUNCEMENT_COLUMNS):
        raise ValueError("incomplete crypto announcement provenance")
    versions = set(columns.get("bybit_perpetual_contract_version", []))
    if not versions or not versions.issubset({6, 7}):
        raise ValueError("announcement policy requires Bybit perpetual daily source")
    values = []
    for key in ANNOUNCEMENT_COLUMNS:
        unique = set(columns[key])
        if len(unique) != 1 or None in unique or "" in unique:
            raise ValueError(f"announcement must be one explicit event per symbol: {key}")
        values.append(next(iter(unique)))
    published = date.fromisoformat(str(values[0]))
    delisted = datetime.fromisoformat(str(values[1]).replace("Z", "+00:00"))
    if delisted.tzinfo is None or delisted.utcoffset() != timedelta(0):
        raise ValueError("crypto delisting time must be explicit UTC")
    source = urlparse(str(values[2]))
    if source.scheme != "https" or source.hostname != "announcements.bybit.com" or not source.path.startswith("/en/article/"):
        raise ValueError("crypto announcement must reference official Bybit article")
    available = datetime.combine(published + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    if available >= delisted:
        raise ValueError("no daily decision exists between known announcement and delisting")
    return np.asarray(dates, dtype="datetime64[D]") >= np.datetime64(available.date())
