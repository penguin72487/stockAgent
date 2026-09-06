#!/usr/bin/env python3
"""Download the official TAIFEX product-code/name table with a receipt."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from io import StringIO
import json
from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import (  # noqa: E402
    atomic_write_json,
    atomic_write_text,
    sha256_file,
)
from downloader.http_transport import (  # noqa: E402
    HttpRequestPolicy,
    ResilientHttpTransport,
)
from stockagent.data.tw_stock_futures_catalog import (  # noqa: E402
    SOURCE_URL as STOCK_FUTURES_URL,
    parse_stock_futures_catalog,
    rows_digest,
)


DEFAULT_URL = "https://www.taifex.com.tw/cht/4/contractName"
DEFAULT_OUTPUT = "data_tw_futures/taifex_contract_codes.csv"


def download_stock_futures_catalog(transport: ResilientHttpTransport, output: Path) -> dict:
    body = transport.request_bytes(STOCK_FUTURES_URL).body
    rows = parse_stock_futures_catalog(body.decode("utf-8"))
    # Detect a truncated response even if a proxy supplied a matching subtotal.
    if len(rows) < 100:
        raise ValueError("unexpectedly small official stock futures catalog")
    previous_path = output
    if previous_path.exists():
        previous = json.loads(previous_path.read_text())
        if len(rows) < int(previous.get("row_count", 0)) * .9:
            raise ValueError("stock futures catalog shrank more than 10%; review required")
    import hashlib

    source_sha = hashlib.sha256(body).hexdigest()
    raw_path = output.parent / "stock_futures_catalog_sources" / f"{source_sha}.html"
    atomic_write_text(raw_path, body.decode("utf-8"), durable=True)
    payload = {
        "schema_version": 1, "source_url": STOCK_FUTURES_URL,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "latest_catalog_not_signal_date", "complete": True,
        "row_count": len(rows), "rows_sha256": rows_digest(rows),
        "source_sha256": source_sha, "rows": rows,
    }
    atomic_write_json(output, payload, durable=True)
    return {key: value for key, value in payload.items() if key != "rows"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--stock-futures-only", action="store_true", help="Refresh only the current stock-futures membership companion")
    args = parser.parse_args()

    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="taifex_public",
            timeout_seconds=args.timeout,
            max_retries=args.max_retries,
            retry_base_seconds=0.5,
        )
    )
    membership_output = Path(args.output).parent / "taifex_stock_futures_catalog.json"
    if args.stock_futures_only:
        print(json.dumps(download_stock_futures_catalog(transport, membership_output), ensure_ascii=False, indent=2))
        return
    body = transport.request_bytes(
        str(args.url),
        headers={"User-Agent": "stockAgent-taifex-product-master/1"},
    ).body
    tables = pd.read_html(StringIO(body.decode("utf-8")))
    if len(tables) != 1:
        raise RuntimeError(f"expected one TAIFEX code table, received {len(tables)}")
    table = tables[0]
    required = {"英文代碼", "中文簡稱"}
    if not required.issubset(table.columns):
        raise RuntimeError(f"TAIFEX code table missing columns: {sorted(required)}")
    output = (
        table.loc[:, ["英文代碼", "中文簡稱"]]
        .rename(columns={"英文代碼": "code", "中文簡稱": "product_name"})
        .dropna()
    )
    output["code"] = output["code"].astype(str).str.strip().str.upper()
    output["product_name"] = output["product_name"].astype(str).str.strip()
    output = output[(output["code"] != "") & (output["product_name"] != "")]
    output = output.drop_duplicates(subset=["code"], keep="last").sort_values("code")
    if len(output) < 1_000:
        raise RuntimeError(f"TAIFEX code table unexpectedly small: {len(output)}")

    path = Path(args.output)
    atomic_write_text(path, output.to_csv(index=False), durable=True)
    sha256 = sha256_file(path)
    receipt = {
        "dataset": "taifex_contract_codes",
        "source_url": str(args.url),
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows": int(len(output)),
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": sha256,
    }
    receipt_path = path.with_suffix(".manifest.json")
    atomic_write_json(receipt_path, receipt, durable=True)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
