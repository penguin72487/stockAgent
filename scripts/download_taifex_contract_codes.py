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


DEFAULT_URL = "https://www.taifex.com.tw/cht/4/contractName"
DEFAULT_OUTPUT = "data_tw_futures/taifex_contract_codes.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="taifex_public",
            timeout_seconds=args.timeout,
            max_retries=args.max_retries,
            retry_base_seconds=0.5,
        )
    )
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
