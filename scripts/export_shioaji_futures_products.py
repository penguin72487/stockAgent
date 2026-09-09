#!/usr/bin/env python3
"""Export every Shioaji futures root and continuous-history alias."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_shioaji_tw_kbars import _atomic_write_json
from downloader.artifact_io import atomic_write_text


CORE_ROOT_PRIORITY = (
    "TXF", "MXF", "TMF", "EXF", "FXF", "XIF", "ZEF", "ZFF", "GTF",
    "TJF", "SPF", "UNF", "UDF", "SXF", "F1F", "BRF", "GDF", "TGF",
    "RHF", "RTF", "XAF", "XBF", "XEF", "XJF",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _product_name(value: str) -> str:
    return re.sub(r"\s+\d{6}(?:\s+W\d+)?$", "", value).strip()


def export_inventory(api, output: Path) -> dict:
    """Reuse the caller's login; retain old aliases and fail closed on discovery errors."""
    rows: list[dict[str, object]] = []
    for root_item in api.contracts.futures_roots():
        if isinstance(root_item, (tuple, list)):
            root = str(root_item[0])
            raw_name = str(root_item[1]) if len(root_item) > 1 else root
        else:
            root = str(getattr(root_item, "root", root_item))
            raw_name = str(getattr(root_item, "name", root))
        chain = list(api.contracts.futures(root))
        codes = sorted({str(item.base.code) for item in chain})
        continuous = sorted(code for code in codes if code.endswith(('R1', 'R2')))
        if not continuous:
            continue
        rows.append({'root': root, 'product_name': _product_name(raw_name),
                     'raw_root_name': raw_name, 'listed_contracts': len(codes),
                     'continuous_r1': next((code for code in continuous if code.endswith('R1')), ''),
                     'continuous_r2': next((code for code in continuous if code.endswith('R2')), ''),
                     'listed_codes_json': json.dumps(codes, ensure_ascii=False)})
    if not rows:
        raise RuntimeError('empty futures catalog; previous inventory preserved')

    priority = {root: index for index, root in enumerate(CORE_ROOT_PRIORITY)}
    rows.sort(key=lambda row: (priority.get(str(row["root"]), 10_000), str(row["root"])))
    output.mkdir(parents=True, exist_ok=True)
    products_path = output / "futures_products.csv"
    atomic_write_text(products_path, pl.DataFrame(rows).write_csv())
    markdown_path = output / "futures_products.md"
    markdown_lines = [
        "# Shioaji futures products",
        "",
        "| Root | Product | R1 | R2 | Listed contracts |",
        "|---|---|---|---|---:|",
    ]
    markdown_lines.extend(
        "| {root} | {product_name} | {continuous_r1} | {continuous_r2} | "
        "{listed_contracts} |".format(**row)
        for row in rows
    )
    atomic_write_text(markdown_path, "\n".join(markdown_lines) + "\n")

    aliases: list[dict[str, object]] = []
    for row in rows:
        for tenor in ("R1", "R2"):
            code = str(row[f"continuous_{tenor.lower()}"])
            if not code:
                continue
            aliases.append(
                {
                    "priority": (
                        priority.get(str(row["root"]), 10_000) * 2
                        + (0 if tenor == "R1" else 1)
                    ),
                    "root": row["root"],
                    "product_name": row["product_name"],
                    "tenor": tenor,
                    "contract": code,
                }
            )
    aliases.sort(key=lambda row: (int(row["priority"]), str(row["contract"])))
    aliases_path = output / "continuous_contracts.csv"
    current_path = output / 'current_continuous_contracts.csv'
    atomic_write_text(current_path, pl.DataFrame(aliases).write_csv())
    retained = {str(row['contract']): row for row in pl.read_csv(aliases_path).to_dicts()} if aliases_path.exists() else {}
    for row in retained.values():
        row['active_selection'] = False
    for row in aliases:
        retained[str(row['contract'])] = {**row, 'active_selection': True}
    union = sorted(retained.values(), key=lambda row: (int(row['priority']), str(row['contract'])))
    atomic_write_text(aliases_path, pl.DataFrame(union).write_csv())
    manifest = {
        "schema_version": 1,
        "source": "shioaji_contract_v2_futures",
        "generated_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
        "futures_roots": len(rows),
        "continuous_contracts": len(union),
        "current_continuous_contracts": len(aliases),
        "current_continuous_contracts_path": str(current_path),
        "current_continuous_contracts_sha256": _sha256(current_path),
        "r1_contracts": sum(row["tenor"] == "R1" for row in aliases),
        "r2_contracts": sum(row["tenor"] == "R2" for row in aliases),
        "products_path": str(products_path),
        "products_sha256": _sha256(products_path),
        "products_markdown_path": str(markdown_path),
        "products_markdown_sha256": _sha256(markdown_path),
        "continuous_contracts_path": str(aliases_path),
        "continuous_contracts_sha256": _sha256(aliases_path),
    }
    _atomic_write_json(output / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('data_tw_futures/shioaji_contracts'))
    parser.add_argument('--simulation', action='store_true')
    args = parser.parse_args()
    import shioaji as sj
    key, secret = os.getenv('SHIOAJI_API_KEY', '').strip(), os.getenv('SHIOAJI_SECRET_KEY', '').strip()
    if not key or not secret:
        raise RuntimeError('SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required')
    api = sj.Shioaji(simulation=args.simulation)
    try:
        api.set_event_callback(lambda *_: None)
        api.login(api_key=key, secret_key=secret, subscribe_trade=False)
        print(json.dumps(export_inventory(api, args.output_dir), ensure_ascii=False, sort_keys=True))
    finally:
        api.logout()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
