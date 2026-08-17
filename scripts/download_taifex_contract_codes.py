#!/usr/bin/env python3
"""Download the official TAIFEX product-code/name table with a receipt."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from io import StringIO
import json
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd


DEFAULT_URL = "https://www.taifex.com.tw/cht/4/contractName"
DEFAULT_OUTPUT = "data_tw_futures/taifex_contract_codes.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    request = Request(
        str(args.url),
        headers={"User-Agent": "stockAgent-taifex-product-master/1"},
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed HTTPS default
        body = response.read()
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
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    output.to_csv(temp, index=False, encoding="utf-8")
    temp.replace(path)
    sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
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
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
