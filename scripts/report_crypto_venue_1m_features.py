#!/usr/bin/env python3
"""Metadata-only, separate OKX/Binance 1m source and model-input inventories."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.config import load_config

CONFIGS = {
    "okx": "okx_1m_venue_only_v1.yaml",
    "binance": "binance_1m_venue_only_v1.yaml",
}


def _metadata(path: Path) -> tuple[int, str | None, str | None, set[str]]:
    parquet = pq.ParquetFile(path)
    names = set(parquet.schema.names)
    if "date" not in names:
        raise ValueError(f"missing date in {path}")
    index = parquet.schema.names.index("date")
    lows: list[str] = []
    highs: list[str] = []
    for i in range(parquet.metadata.num_row_groups):
        stats = parquet.metadata.row_group(i).column(index).statistics
        if stats is not None and stats.has_min_max:
            lows.append(str(stats.min))
            highs.append(str(stats.max))
    return (
        parquet.metadata.num_rows,
        min(lows) if lows else None,
        max(highs) if highs else None,
        names,
    )


def inventory(venue: str, root: Path, output_dir: Path) -> dict[str, object]:
    config = load_config(root / "configs/markets" / CONFIGS[venue])
    if config.data.crypto_exchange_scope != venue or config.data.use_external_features:
        raise ValueError("venue-only price baseline config changed")
    source = root / config.data.parquet_root
    files = sorted(source.glob("*_features.parquet"))
    hot_files = sorted((source / "_hot_tail").glob("*_features.parquet"))
    if not files:
        raise FileNotFoundError(f"no 1m symbol files in {source}")
    seen: dict[str, int] = {}
    first: list[str] = []
    last: list[str] = []
    physical_base_rows = 0
    physical_hot_rows = 0
    without_date_stats = 0
    for path in (*files, *hot_files):
        rows, start, end, names = _metadata(path)
        if path.parent == source:
            physical_base_rows += rows
        else:
            physical_hot_rows += rows
        if start is None or end is None:
            without_date_stats += 1
        else:
            first.append(start)
            last.append(end)
        for name in names:
            if name != "date":
                seen[name] = seen.get(name, 0) + 1
    receipt_path = source / "download_summary.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.is_file() else {}
    catalog_path = source / f"{venue}_historical_feature_catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8")) if catalog_path.is_file() else {}
    selected = list(config.data.feature_include)
    rows = [{
        "feature": name,
        "kind": "derived_panel_feature",
        "selected_for_training": True,
        "source_files_with_column": "",
        "status": "derived_from_same_venue_completed_OHLCV",
        "note": "The panel derives this value; this footer audit does not count finite values.",
    } for name in selected]
    rows.extend({
        "feature": name,
        "kind": "raw_1m_column",
        "selected_for_training": False,
        "source_files_with_column": count,
        "status": "source_column_not_direct_model_input",
        "note": "Schema presence is not filled-value or PIT eligibility proof.",
    } for name, count in sorted(seen.items()))
    summary: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "exchange_scope": venue,
        "config_path": str(root / "configs/markets" / CONFIGS[venue]),
        "source_root": str(source),
        "base_symbol_files": len(files),
        "hot_tail_symbol_files": len(hot_files),
        "base_physical_rows": physical_base_rows,
        "hot_tail_physical_rows": physical_hot_rows,
        "download_receipt_logical_rows": receipt.get("row_count"),
        "first_metadata_timestamp": min(first) if first else None,
        "last_metadata_timestamp": max(last) if last else None,
        "files_without_date_statistics": without_date_stats,
        "selected_derived_features": len(selected),
        "source_column_count": len(seen),
        "historical_features_enabled_last_run": receipt.get("historical_features_enabled"),
        "historical_feature_report_is_current_run": receipt.get("historical_feature_report_is_current_run"),
        "source_catalog_entries": len(catalog.get("catalog", [])),
        "completeness_verified": False,
        "funding_adjusted_execution_verified": False,
        "row_count_note": "Base and hot-tail physical rows may overlap; do not add them as unique model rows.",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(output_dir / "feature_inventory.csv", buffer.getvalue())
    catalog_rows = catalog.get("catalog", [])
    if catalog_rows:
        keys = sorted({key for row in catalog_rows for key in row})
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=keys)
        writer.writeheader()
        writer.writerows(catalog_rows)
        atomic_write_text(output_dir / "source_catalog.csv", buffer.getvalue())
    atomic_write_json(output_dir / "summary.json", summary)
    lines = [
        f"# {venue.upper()} 1m 單交易所資料與訓練欄位",
        "",
        f"來源：`{config.data.parquet_root}`；模型設定：`{CONFIGS[venue]}`。",
        f"物理 base {len(files)} 檔、{physical_base_rows:,} 列；hot-tail {len(hot_files)} 檔、{physical_hot_rows:,} 列。兩層可能重疊，不能相加當成唯一列數。",
        f"Parquet metadata 時間範圍：{summary['first_metadata_timestamp']} 至 {summary['last_metadata_timestamp']}；缺統計檔數 {without_date_stats}。",
        f"模型選 {len(selected)} 個同交易所價量衍生欄；原始 schema 有 {len(seen)} 欄，未選入的輔助欄逐一列在 CSV。",
        "這是 schema／footer 盤點，不證明每欄有值、逐 symbol 連續、PIT 安全、或 funding 已進回測。",
        f"最新下載收據：歷史輔助擷取啟用 `{receipt.get('historical_features_enabled')}`，本輪報告有效 `{receipt.get('historical_feature_report_is_current_run')}`。",
        "",
        "| 訓練 feature | 狀態 |",
        "|---|---|",
        *(f"| {name} | 同交易所 1m OHLCV 衍生；待完整 panel 值稽核 |" for name in selected),
        "",
        "來源輔助欄與官方歷史限制詳見 `feature_inventory.csv`、`source_catalog.csv`；目前未直接輸入模型。",
    ]
    atomic_write_text(output_dir / "feature_inventory.md", "\n".join(lines) + "\n")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("venue", choices=sorted(CONFIGS))
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or ROOT / "artifacts/data_quality" / (
        f"crypto_venue_training_inventory_{datetime.now(timezone.utc).date().isoformat()}"
    ) / args.venue
    print(json.dumps(inventory(args.venue, ROOT, output), ensure_ascii=False))


if __name__ == "__main__":
    main()
