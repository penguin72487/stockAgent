#!/usr/bin/env python3
"""Download receipt-backed official TXO historical final settlement prices."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import io
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Final
from urllib import parse, request

import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.taifex_daily_download_common import (  # noqa: E402
    atomic_write_json,
    parse_iso_date,
    sha256_path,
)


TAIFEX_FINAL_SETTLEMENT_PAGE: Final[str] = (
    "https://www.taifex.com.tw/cht/5/optIndxFSP"
)
TAIFEX_TXO_COMMODITY_ID: Final[str] = "2"
OUTPUT_SCHEMA_VERSION: Final[int] = 1


def _download_html(url: str, target: Path, *, attempts: int = 3) -> Path:
    if target.is_file() and target.stat().st_size > 1_000:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    body = b""
    content_type = ""
    for attempt in range(1, attempts + 1):
        try:
            http_request = request.Request(
                url,
                headers={
                    "User-Agent": "stockAgent/taifex-final-settlement-research"
                },
            )
            with request.urlopen(http_request, timeout=120) as response:
                body = response.read()
                content_type = str(response.headers.get("Content-Type") or "")
            break
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                raise RuntimeError(
                    f"failed to download TAIFEX final settlements after {attempts} attempts"
                ) from last_error
            time.sleep(float(attempt))
    if len(body) < 1_000 or "html" not in content_type.casefold():
        raise RuntimeError(
            "TAIFEX final-settlement response is not a non-empty HTML page"
        )
    with tempfile.NamedTemporaryFile(
        dir=target.parent,
        prefix=target.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(target)
    return target


def parse_taifex_txo_final_settlement_html(
    body: bytes,
    *,
    start_date: date,
    end_date: date,
    source_file: str,
    source_sha256: str,
    source_url: str,
) -> pl.DataFrame:
    """Parse the official HTML result table without relying on mojibake labels."""

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("TAIFEX final-settlement HTML is not UTF-8") from exc
    tables = pd.read_html(io.StringIO(text))
    candidates = [table for table in tables if table.shape[1] >= 3]
    if len(candidates) != 1:
        raise ValueError(
            "expected exactly one TAIFEX final-settlement result table, "
            f"found {len(candidates)}"
        )
    table = candidates[0].iloc[:, :3].copy()
    table.columns = ["settlement_date", "option_series", "final_settlement_price"]
    table["settlement_date"] = pd.to_datetime(
        table["settlement_date"], format="%Y/%m/%d", errors="coerce"
    ).dt.date
    table["option_series"] = (
        table["option_series"].astype(str).str.strip().str.upper()
    )
    table["final_settlement_price"] = pd.to_numeric(
        table["final_settlement_price"], errors="coerce"
    )
    valid = table.loc[
        table["settlement_date"].notna()
        & table["option_series"].str.fullmatch(r"[0-9]{6}(?:[WF][1-5])?")
        & table["final_settlement_price"].gt(0.0)
    ].copy()
    valid = valid.loc[
        valid["settlement_date"].map(
            lambda value: start_date <= value <= end_date
        )
    ]
    if valid.empty:
        raise ValueError("official TAIFEX page produced no valid TXO settlements")
    if valid.duplicated(["settlement_date", "option_series"]).any():
        raise ValueError("official TXO settlement table contains duplicate keys")
    valid = valid.sort_values(
        ["settlement_date", "option_series"], kind="stable"
    ).reset_index(drop=True)
    valid["product"] = "TXO"
    valid["source_file"] = source_file
    valid["source_sha256"] = source_sha256
    valid["source_url"] = source_url
    return pl.from_pandas(valid)


def _parse_date(value: str) -> date:
    try:
        return parse_iso_date(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected ISO date YYYY-MM-DD, got {value!r}"
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=_parse_date, default=date(2012, 11, 1))
    parser.add_argument(
        "--end-date", type=_parse_date, default=date.today() - timedelta(days=1)
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_tw_index_options_daily")
    )
    args = parser.parse_args()
    if args.end_date < args.start_date:
        parser.error("--end-date precedes --start-date")

    query = {
        "commodityIds": TAIFEX_TXO_COMMODITY_ID,
        "start_year": f"{args.start_date.year:04d}",
        "start_month": f"{args.start_date.month:02d}",
        "end_year": f"{args.end_date.year:04d}",
        "end_month": f"{args.end_date.month:02d}",
    }
    source_url = f"{TAIFEX_FINAL_SETTLEMENT_PAGE}?{parse.urlencode(query)}"
    output_dir = args.output_dir.expanduser().resolve()
    raw_path = output_dir / "raw" / "final_settlement" / (
        f"{args.start_date.strftime('%Y-%m')}_{args.end_date.strftime('%Y-%m')}_TXO.html"
    )
    _download_html(source_url, raw_path)
    source_sha256 = sha256_path(raw_path)
    frame = parse_taifex_txo_final_settlement_html(
        raw_path.read_bytes(),
        start_date=args.start_date,
        end_date=args.end_date,
        source_file=str(raw_path),
        source_sha256=source_sha256,
        source_url=source_url,
    )
    normalized_path = output_dir / "txo_final_settlement_history.parquet"
    normalized_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = normalized_path.with_suffix(".parquet.tmp")
    frame.write_parquet(temporary, compression="zstd")
    temporary.replace(normalized_path)
    manifest = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "product": "TXO",
        "official_page": TAIFEX_FINAL_SETTLEMENT_PAGE,
        "query_url": source_url,
        "requested_start_date": args.start_date.isoformat(),
        "requested_end_date": args.end_date.isoformat(),
        "rows": frame.height,
        "first_settlement_date": frame["settlement_date"].min().isoformat(),
        "last_settlement_date": frame["settlement_date"].max().isoformat(),
        "raw_receipt": {
            "path": str(raw_path),
            "bytes": raw_path.stat().st_size,
            "sha256": source_sha256,
        },
        "normalized": {
            "path": str(normalized_path),
            "bytes": normalized_path.stat().st_size,
            "sha256": sha256_path(normalized_path),
        },
    }
    atomic_write_json(output_dir / "manifest_final_settlement.json", manifest)
    print(
        f"built {normalized_path}: {frame.height:,} official TXO settlements, "
        f"{manifest['first_settlement_date']}..{manifest['last_settlement_date']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
