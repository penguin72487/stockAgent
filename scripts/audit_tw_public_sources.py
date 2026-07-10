from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

import polars as pl
import pyarrow.parquet as pq
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import DEFAULT_DATASETS


CATALOGS = {
    "TWSE": "https://openapi.twse.com.tw/v1/swagger.json",
    "TPEx": "https://www.tpex.org.tw/openapi/swagger.json",
}

# Exact OpenAPI routes already ingested by downloader/download_tw_public_data.py.
EXACT_COVERAGE = {
    ("TWSE", "/opendata/t187ap03_L"): "twse_listed_company_basic",
    ("TWSE", "/opendata/t187ap45_L"): "twse_listed_dividend",
    ("TWSE", "/opendata/t187ap04_L"): "twse_listed_material_info",
    ("TWSE", "/exchangeReport/TWT48U_ALL"): "twse_ex_dividend_preview",
    ("TWSE", "/announcement/notice"): "twse_notice_stock",
    ("TWSE", "/announcement/punish"): "twse_disposal_stock",
    ("TWSE", "/company/suspendListingCsvAndHtml"): "twse_delisted_company",
    ("TPEx", "/mopsfin_t187ap03_O"): "tpex_basic_company",
    ("TPEx", "/tpex_disposal_information"): "tpex_disposal_stock",
    ("TPEx", "/tpex_trading_warning_information"): "tpex_attention_stock",
}

# The project uses date-query endpoints rather than these current/snapshot OpenAPI
# routes, so content coverage exists but the endpoint itself is not ingested.
SEMANTIC_COVERAGE = {
    ("TWSE", "/exchangeReport/STOCK_DAY_ALL"): "twse_daily_ohlcv",
    ("TWSE", "/exchangeReport/BWIBBU_ALL"): "twse_daily_valuation",
    ("TWSE", "/fund/T86_ALL"): "twse_institutional_trades",
    ("TPEx", "/tpex_mainboard_daily_close_quotes"): "tpex_daily_ohlcv",
    ("TPEx", "/tpex_mainboard_quotes"): "tpex_daily_ohlcv",
    ("TPEx", "/tpex_mainboard_peratio_analysis"): "tpex_daily_valuation",
    ("TPEx", "/tpex_mainboard_margin_balance"): "tpex_margin_balance",
    ("TPEx", "/tpex_3insti_daily_trading"): "tpex_institutional_trades",
    ("TPEx", "/mopsfin_t187ap39_O"): "tpex_dividend",
}

HIGH_VALUE_TERMS = (
    "財務", "營收", "資產負債", "損益", "現金流量", "持股", "董監", "內部人",
    "借券", "融資融券", "三大法人", "除權", "除息", "重大訊息", "終止", "上市",
    "上櫃", "公司基本", "股利", "ESG", "注意", "處置", "停止買賣", "暫停交易",
    "行情", "本益比", "市值", "指數", "ETF", "申請上市", "申請上櫃",
)


@dataclass(frozen=True)
class CatalogRow:
    source: str
    category: str
    endpoint: str
    summary: str
    status: str
    dataset: str
    priority: str
    catalog_url: str


def _get_json(url: str) -> dict[str, Any]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected an object from {url}")
    return payload


def _priority(summary: str, category: str) -> str:
    text = f"{category} {summary}"
    return "high" if any(term.lower() in text.lower() for term in HIGH_VALUE_TERMS) else "normal"


def collect_rows() -> list[CatalogRow]:
    rows: list[CatalogRow] = []
    for source, catalog_url in CATALOGS.items():
        payload = _get_json(catalog_url)
        paths = payload.get("paths", {})
        for endpoint, operations in paths.items():
            if not isinstance(operations, dict):
                continue
            operation = operations.get("get")
            if not isinstance(operation, dict):
                continue
            summary = str(operation.get("summary", "")).strip()
            tags = operation.get("tags") or ["未分類"]
            category = " / ".join(str(tag) for tag in tags)
            key = (source, endpoint)
            if key in EXACT_COVERAGE:
                status, dataset = "captured", EXACT_COVERAGE[key]
            elif key in SEMANTIC_COVERAGE:
                status, dataset = "captured_via_other_official_endpoint", SEMANTIC_COVERAGE[key]
            else:
                status, dataset = "not_captured", ""
            rows.append(
                CatalogRow(
                    source=source,
                    category=category,
                    endpoint=endpoint,
                    summary=summary,
                    status=status,
                    dataset=dataset,
                    priority=_priority(summary, category),
                    catalog_url=catalog_url,
                )
            )
    return sorted(rows, key=lambda row: (row.source, row.category, row.endpoint))


def write_csv(path: Path, rows: list[CatalogRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CatalogRow.__dataclass_fields__))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def write_markdown(path: Path, rows: list[CatalogRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    status_counts = Counter(row.status for row in rows)
    by_source = defaultdict(Counter)
    for row in rows:
        by_source[row.source][row.status] += 1
    high_missing = [row for row in rows if row.status == "not_captured" and row.priority == "high"]
    lines = [
        "# Taiwan Public Data Source Inventory",
        "",
        "This inventory compares every GET endpoint in the current TWSE and TPEx official OpenAPI catalogs with `download_tw_public_data.py`.",
        "",
        f"- Catalog endpoints: {len(rows)}",
        f"- Captured exactly: {status_counts['captured']}",
        f"- Captured through another official historical endpoint: {status_counts['captured_via_other_official_endpoint']}",
        f"- Not captured: {status_counts['not_captured']}",
        f"- High-value not captured: {len(high_missing)}",
        "",
        "## Coverage by official catalog",
        "",
        "| Source | Total | Captured exact | Covered elsewhere | Not captured |",
        "|---|---:|---:|---:|---:|",
    ]
    for source in sorted(by_source):
        counts = by_source[source]
        lines.append(
            f"| {source} | {sum(counts.values())} | {counts['captured']} | "
            f"{counts['captured_via_other_official_endpoint']} | {counts['not_captured']} |"
        )
    lines.extend(["", "## High-value gaps", "", "| Source | Category | Public dataset | Endpoint |", "|---|---|---|---|"])
    for row in high_missing:
        lines.append(f"| {row.source} | {row.category} | {row.summary} | `{row.endpoint}` |")
    lines.extend(
        [
            "",
            "## Scope and limitations",
            "",
            "- The exhaustive comparison is for the TWSE and TPEx OpenAPI catalogs only.",
            "- Website-only historical query endpoints are represented as coverage mappings when the downloader already uses them.",
            "- TAIFEX, TDCC, CBC, DGBAS, MOF, and data.gov.tw do not expose one bounded catalog comparable to these two exchange Swagger catalogs; their currently registered datasets must be audited separately.",
            "- `captured` means an ingestion route exists, not that its local parquet is necessarily fresh or historically complete.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_local_inventory(path: Path, data_dir: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["dataset", "source", "kind", "file_exists", "rows", "min_date", "max_date", "origin"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for spec in DEFAULT_DATASETS.values():
            parquet_path = data_dir / f"{spec.name}.parquet"
            row_count = 0
            min_date = ""
            max_date = ""
            if parquet_path.exists():
                row_count = int(pq.ParquetFile(parquet_path).metadata.num_rows)
                columns = pl.scan_parquet(parquet_path).collect_schema().names()
                if "date" in columns:
                    dates = pl.scan_parquet(parquet_path).select(
                        pl.col("date").cast(pl.String).min().alias("min_date"),
                        pl.col("date").cast(pl.String).max().alias("max_date"),
                    ).collect().row(0)
                    min_date, max_date = (str(value or "") for value in dates)
            writer.writerow(
                {
                    "dataset": spec.name,
                    "source": spec.source,
                    "kind": spec.kind,
                    "file_exists": parquet_path.exists(),
                    "rows": row_count,
                    "min_date": min_date,
                    "max_date": max_date,
                    "origin": spec.url or spec.url_template or f"data.gov.tw:{spec.data_gov_id}",
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit TWSE/TPEx public OpenAPI coverage.")
    parser.add_argument("--output-dir", default="artifacts/data_audit")
    parser.add_argument("--data-dir", default="data_tw_public")
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    rows = collect_rows()
    write_csv(output_dir / "tw_public_source_inventory.csv", rows)
    write_markdown(output_dir / "tw_public_source_inventory.md", rows)
    write_local_inventory(output_dir / "tw_public_local_dataset_inventory.csv", Path(args.data_dir))
    print(f"rows={len(rows)} output_dir={output_dir}")


if __name__ == "__main__":
    main()
