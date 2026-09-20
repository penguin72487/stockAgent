"""Keep original-release Parquet byte-stable when only the polling clock moves."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import tempfile
from typing import Any

import polars as pl


def write_release_rows_if_changed(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    identity_columns: tuple[str, ...],
    allow_placeholder_upgrade: bool = False,
) -> tuple[str, bool]:
    """Preserve the first observation of unchanged bytes; write only real changes.

    ``observed_at_utc`` is a collector clock, not a new source vintage.  A
    repeated read of identical original HTML/attachment hashes must not make
    the feature builder treat the entire historical source as revised.
    """

    def keyed(items: list[dict[str, Any]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        result: dict[tuple[Any, ...], dict[str, Any]] = {}
        for item in items:
            key = tuple(item[column] for column in identity_columns)
            if key in result:
                raise ValueError(f"duplicate release identity: {key}")
            result[key] = item
        return result

    def without_poll_clock(item: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in item.items() if key != "observed_at_utc"}

    new_by_key = keyed(rows)
    prior_rows = pl.read_parquet(path).to_dicts() if path.is_file() else []
    prior_by_key = keyed(prior_rows)
    missing_prior = prior_by_key.keys() - new_by_key.keys()
    upgraded_placeholders: dict[str, dict[str, Any]] = {}
    if allow_placeholder_upgrade:
        if identity_columns != ("release_url", "metric"):
            raise ValueError("placeholder upgrade requires release URL and metric identity")
        for key in list(missing_prior):
            if key[1] is not None:
                continue
            url = str(key[0])
            upgrades = [row for row in rows if row["release_url"] == url]
            if ({row.get("metric") for row in upgrades} == {"m1b_yoy_pct", "m2_yoy_pct"}
                    and all(row.get("html_sha256") == prior_by_key[key].get("html_sha256")
                            for row in upgrades)):
                upgraded_placeholders[url] = prior_by_key[key]
                missing_prior.remove(key)
    if missing_prior:
        examples = sorted(map(str, missing_prior))[:5]
        raise ValueError(
            f"release identity regression: {len(missing_prior)} previously archived releases "
            f"are absent from this scan; examples={examples}"
        )
    stable_rows: list[dict[str, Any]] = []
    for row in rows:
        current = dict(row)
        old = prior_by_key.get(tuple(row[column] for column in identity_columns))
        if old is not None and without_poll_clock(old) == without_poll_clock(current):
            current["observed_at_utc"] = old.get("observed_at_utc")
        elif old is None and str(row.get("release_url")) in upgraded_placeholders:
            current["observed_at_utc"] = upgraded_placeholders[str(row["release_url"])].get("observed_at_utc")
        stable_rows.append(current)
    if path.is_file() and stable_rows == prior_rows:
        with path.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest(), False
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        pl.DataFrame(stable_rows, infer_schema_length=None).write_parquet(
            temporary, compression="zstd", statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest(), True
