"""Build an inspectable coverage and point-in-time audit for crypto data.

The report distinguishes old event dates from old *known-at-the-time* values.
It uses Parquet footer statistics for the very large exchange trees, and scans
only the compact public-source tables needed to validate availability clocks.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Iterable

import polars as pl
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER_DIR = REPO_ROOT / "downloader"
if str(DOWNLOADER_DIR) not in sys.path:
    sys.path.insert(0, str(DOWNLOADER_DIR))

from artifact_io import atomic_write_json, atomic_write_text  # noqa: E402


CUTOFF = date(2020, 1, 1)
OKX_PUBLIC_COLUMNS = {
    "okx_mark_open",
    "okx_mark_high",
    "okx_mark_low",
    "okx_mark_close",
    "okx_index_open",
    "okx_index_high",
    "okx_index_low",
    "okx_index_close",
}


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _date_value(value: object) -> date | None:
    if value is None:
        return None
    text = value.isoformat() if hasattr(value, "isoformat") else str(value)
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _footer_tree_profile(
    root: Path,
    pattern: str,
    time_column: str,
    *,
    required_columns: set[str] | None = None,
) -> dict[str, Any]:
    files = sorted(root.glob(pattern))
    rows = 0
    corrupt: list[str] = []
    missing_statistics = 0
    first: date | None = None
    last: date | None = None
    pre_cutoff_files = 0
    schema_complete_files = 0
    for path in files:
        try:
            parquet = pq.ParquetFile(path)
            rows += int(parquet.metadata.num_rows)
            names = parquet.schema_arrow.names
            if required_columns is None or required_columns <= set(names):
                schema_complete_files += 1
            if time_column not in names:
                missing_statistics += 1
                continue
            index = names.index(time_column)
            minima: list[date] = []
            maxima: list[date] = []
            for row_group in range(parquet.metadata.num_row_groups):
                statistics = parquet.metadata.row_group(row_group).column(index).statistics
                if statistics is None or not statistics.has_min_max:
                    continue
                minimum = _date_value(statistics.min)
                maximum = _date_value(statistics.max)
                if minimum is not None:
                    minima.append(minimum)
                if maximum is not None:
                    maxima.append(maximum)
            if not minima or not maxima:
                missing_statistics += 1
                continue
            file_first, file_last = min(minima), max(maxima)
            first = file_first if first is None else min(first, file_first)
            last = file_last if last is None else max(last, file_last)
            if file_first <= CUTOFF:
                pre_cutoff_files += 1
        except Exception as exc:  # report corruption instead of hiding it
            corrupt.append(f"{path}: {type(exc).__name__}: {exc}")
    return {
        "path": str(root),
        "files": len(files),
        "rows_from_base_parquet_footers": rows,
        "earliest_event_date": first.isoformat() if first else None,
        "latest_event_date": last.isoformat() if last else None,
        "files_reaching_2020_01_01_or_earlier": pre_cutoff_files,
        "schema_complete_files": schema_complete_files,
        "missing_time_statistics_files": missing_statistics,
        "corrupt_files": corrupt,
    }


def _scan_min_max(path: Path, column: str) -> tuple[str | None, str | None, int]:
    if not path.is_file():
        return None, None, 0
    result = (
        pl.scan_parquet(path)
        .select(
            pl.col(column).min().alias("first"),
            pl.col(column).max().alias("last"),
            pl.len().alias("rows"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    first = _date_value(result["first"])
    last = _date_value(result["last"])
    return (
        first.isoformat() if first else None,
        last.isoformat() if last else None,
        int(result["rows"]),
    )


def _sec_profile(root: Path) -> dict[str, Any]:
    files = sorted(root.glob("*/filings.parquet"))
    if not files:
        return {"files": 0, "rows": 0, "earliest": None, "latest": None}
    frame = (
        pl.scan_parquet([str(path) for path in files])
        .select(
            pl.col("acceptance_datetime").min().alias("first"),
            pl.col("acceptance_datetime").max().alias("last"),
            pl.len().alias("rows"),
            pl.struct(["cik", "accession_number"]).n_unique().alias("unique_keys"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return {
        "files": len(files),
        "rows": int(frame["rows"]),
        "unique_keys": int(frame["unique_keys"]),
        "duplicate_rows_beyond_first": int(frame["rows"] - frame["unique_keys"]),
        "earliest": str(frame["first"])[:10] if frame["first"] else None,
        "latest": str(frame["last"])[:10] if frame["last"] else None,
    }


def _free_public_profile(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return (
        pl.scan_parquet(path)
        .group_by(["source", "dataset"])
        .agg(
            pl.len().alias("rows"),
            pl.col("event_ts_utc").min().alias("earliest_event"),
            pl.col("event_ts_utc").max().alias("latest_event"),
            pl.col("available_at_utc").min().alias("earliest_available"),
            pl.col("point_in_time_state").drop_nulls().unique().sort().alias("pit_states"),
        )
        .sort(["source", "dataset"])
        .collect(engine="streaming")
        .to_dicts()
    )


def _meets(first: str | None) -> bool:
    return bool(first and date.fromisoformat(first[:10]) <= CUTOFF)


def _row(
    dataset: str,
    source: str,
    path: str,
    first: str | None,
    last: str | None,
    rows: int | None,
    pit_class: str,
    training_status: str,
    evidence: str,
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "source": source,
        "local_path": path,
        "earliest_event_date": first,
        "latest_event_date": last,
        "rows": rows,
        "reaches_2020_01_01_or_earlier": _meets(first),
        "point_in_time_class": pit_class,
        "historical_training_status": training_status,
        "evidence": evidence,
    }


def _markdown_table(rows: Iterable[dict[str, Any]]) -> str:
    lines = [
        "| 資料 | 最早事件日 | 至少到 2020-01-01 | PIT 類別 | 目前用途 |",
        "|---|---:|:---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {dataset} | {first} | {meets} | {pit} | {status} |".format(
                dataset=row["dataset"],
                first=row["earliest_event_date"] or "—",
                meets="是" if row["reaches_2020_01_01_or_earlier"] else "否",
                pit=row["point_in_time_class"],
                status=row["historical_training_status"],
            )
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = args.repo_root.resolve()
    today = datetime.now(timezone.utc).date().isoformat()
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else root / "artifacts/data_quality" / f"crypto_historical_coverage_{today}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    bybit_1m = _footer_tree_profile(root / "data_bybit/1m", "*_features.parquet", "date")
    bybit_funding = _footer_tree_profile(
        root / "data_bybit/funding", "*_funding.parquet", "funding_time_utc"
    )
    binance_1m = _footer_tree_profile(root / "data_binance/1m", "*_features.parquet", "date")
    okx_1m = _footer_tree_profile(
        root / "data_okx/1m",
        "*_features.parquet",
        "date",
        required_columns=OKX_PUBLIC_COLUMNS,
    )
    okx_15m = _footer_tree_profile(
        root / "data_okx",
        "*_features.parquet",
        "date",
        required_columns=OKX_PUBLIC_COLUMNS,
    )
    historical = _load_json(root / "data_crypto_historical_public/download_summary.json")
    cftc = historical.get("sources", {}).get("cftc", {})
    wiki = historical.get("sources", {}).get("wikimedia", {})
    fred_first, fred_last, fred_rows = _scan_min_max(
        root / "data_fred_crypto_macro/observations.parquet", "observation_date"
    )
    sec = _sec_profile(root / "data_crypto_etf/normalized/sec")
    cm_first, cm_last, _ = _scan_min_max(
        root / "data_coinmetrics_community/assets/btc_features.parquet", "date"
    )
    cm_vintage_first, _, _ = _scan_min_max(
        root / "data_coinmetrics_community/vintages/btc_vintages.parquet",
        "available_at_utc",
    )
    cm_summary = _load_json(root / "data_coinmetrics_community/download_summary.json")
    archive = _load_json(root / "data_binance_archive/download_summary.json")
    dune = _load_json(root / "data_dune_crypto/download_summary.json")
    reference = _load_json(root / "data_crypto_reference/download_summary.json")
    public_build = _load_json(
        root / "data_bybit/public_features/bybit_crypto_public_daily_summary.json"
    )
    free_rows = _free_public_profile(root / "data_free_public/observations.parquet")
    free_by_dataset = {str(item["dataset"]): item for item in free_rows}

    def free_dates(dataset: str) -> tuple[str | None, str | None, int]:
        item = free_by_dataset.get(dataset, {})
        return (
            str(item.get("earliest_event"))[:10] if item.get("earliest_event") else None,
            str(item.get("latest_event"))[:10] if item.get("latest_event") else None,
            int(item.get("rows", 0)),
        )

    fear_first, fear_last, fear_rows = free_dates("alternative_me_fear_greed")
    dex_first, dex_last, dex_rows = free_dates("defillama_dex_volume")
    fees_first, fees_last, fees_rows = free_dates("defillama_fees_revenue")

    rows = [
        _row("Binance USD-M 1m", "Binance", binance_1m["path"], binance_1m["earliest_event_date"], binance_1m["latest_event_date"], binance_1m["rows_from_base_parquet_footers"], "A: completed exchange bars", "可用；需保留歷史成分與上市時間 mask", f"{binance_1m['files_reaching_2020_01_01_or_earlier']}/{binance_1m['files']} files reach cutoff"),
        _row("OKX SWAP 1m core", "OKX", okx_1m["path"], okx_1m["earliest_event_date"], okx_1m["latest_event_date"], okx_1m["rows_from_base_parquet_footers"], "A: completed exchange bars", "核心 K 線可用；輔助 mark/index 不完整", f"public feature schema complete {okx_1m['schema_complete_files']}/{okx_1m['files']} files"),
        _row("OKX SWAP 15m enriched", "OKX", okx_15m["path"], okx_15m["earliest_event_date"], okx_15m["latest_event_date"], okx_15m["rows_from_base_parquet_footers"], "A: completed exchange bars", "可因果聚合到日頻；目前正式公開特徵 fallback", f"public feature schema complete {okx_15m['schema_complete_files']}/{okx_15m['files']} files"),
        _row("Bybit perpetual 1m", "Bybit", bybit_1m["path"], bybit_1m["earliest_event_date"], bybit_1m["latest_event_date"], bybit_1m["rows_from_base_parquet_footers"], "A: completed exchange bars", "可用，但交易產品史未延伸至 2020-01-01", f"{bybit_1m['files_reaching_2020_01_01_or_earlier']}/{bybit_1m['files']} files reach cutoff"),
        _row("Bybit funding settlements", "Bybit", bybit_funding["path"], bybit_funding["earliest_event_date"], bybit_funding["latest_event_date"], bybit_funding["rows_from_base_parquet_footers"], "A: official settlement events", "可用，但與合約上市日共同限制", f"{bybit_funding['files']} symbol files"),
        _row("Binance public archive", "Binance Vision", "data_binance_archive", "2020-01-01", None, None, "A/B: immutable bars with archive receipt clock", "可用已驗證物件；整體仍 partial", f"state={archive.get('state')}, source_invalid={archive.get('source_invalid_objects')}, quarantined={archive.get('quarantined_monthly_objects')}"),
        _row("FRED initial releases", "FRED/ALFRED", "data_fred_crypto_macro/observations.parquet", fred_first, fred_last, fred_rows, "A: initial-release vintage", "可用；next-UTC-day 保守延遲", "output_type=4; revisions excluded"),
        _row("SEC crypto-ETF filings", "SEC EDGAR", "data_crypto_etf/normalized/sec", sec.get("earliest"), sec.get("latest"), sec.get("rows"), "A: acceptance timestamp", "可用；current registry selection 有 survivorship risk", f"duplicates={sec.get('duplicate_rows_beyond_first')}; files={sec.get('files')}"),
        _row("CFTC digital-asset TFF", "CFTC", "data_crypto_historical_public/normalized/cftc_tff_digital_assets.parquet", cftc.get("quality", {}).get("first_event_utc"), cftc.get("quality", {}).get("last_event_utc"), cftc.get("quality", {}).get("rows"), "B: old events, incomplete release calendar", "研究側車；完成歷史發布日稽核前隔離", f"duplicates={cftc.get('quality', {}).get('duplicate_rows_beyond_first')}"),
        _row("Wikimedia crypto pageviews", "Wikimedia", "data_crypto_historical_public/normalized/wikimedia_crypto_pageviews_daily.parquet", wiki.get("quality", {}).get("first_event_utc"), wiki.get("quality", {}).get("last_event_utc"), wiki.get("quality", {}).get("rows"), "B: old events, no revision vintage", "研究側車；redirect/rename/revision 稽核前隔離", f"duplicates={wiki.get('quality', {}).get('duplicate_rows_beyond_first')}; articles={len(wiki.get('articles', []))}"),
        _row("Coin Metrics Community latest view", "Coin Metrics", "data_coinmetrics_community/assets", cm_first, cm_last, cm_summary.get("row_count"), "B: old observations first seen locally later", f"歷史值研究用；因果 vintage 只從 {cm_vintage_first or 'unknown'} 起", f"assets={cm_summary.get('asset_count')}; metrics={cm_summary.get('unique_metric_count')}"),
        _row("Alternative.me Fear & Greed", "Alternative.me", "data_free_public/observations.parquet", fear_first, fear_last, fear_rows, "B: historical archive first observed now", "研究側車；不可回填成當時已知", "attribution required; repeated retrieval vintages retained"),
        _row("DefiLlama DEX volume", "DefiLlama", "data_free_public/observations.parquet", dex_first, dex_last, dex_rows, "B: historical archive first observed now", "研究側車；不可回填成當時已知", "no historical revision clock"),
        _row("DefiLlama fees/revenue", "DefiLlama", "data_free_public/observations.parquet", fees_first, fees_last, fees_rows, "B: historical archive first observed now", "研究側車；不可回填成當時已知", "no historical revision clock"),
        _row("CoinGecko market snapshots", "CoinGecko", "data_crypto_reference", None, reference.get("end_date"), reference.get("row_count"), "C: prospective snapshots", "只可從 2026-08-16 本機 observed_at 後使用", "Demo historical reach is insufficient for a full 2020 market snapshot panel"),
        _row("Dune registered crypto queries", "Dune", "data_dune_crypto", None, None, dune.get("rows"), "D: blocked", "不可用", f"state={dune.get('state')}; completed_partitions={dune.get('completed_partitions')}; credit blocked"),
    ]

    critical_findings = [
        {
            "severity": "HIGH",
            "finding": "OKX 1m auxiliary history is incomplete",
            "evidence": f"{okx_1m['schema_complete_files']}/{okx_1m['files']} files contain required mark/index columns",
            "impact": "A direct 1m-source rebuild fails for mapped Bybit symbols; do not label missing fields as zero.",
            "action": "Keep the completed 15m enriched source for daily aggregation until a bounded 1m repair release passes all-symbol audit.",
        },
        {
            "severity": "HIGH",
            "finding": "Historical event date is not historical information availability",
            "evidence": "Coin Metrics, Alternative.me, DefiLlama, Wikimedia and CFTC release-calendar gaps retain local first-observed clocks.",
            "impact": "Backprojecting them would leak future revisions or today's archive contents into 2020 decisions.",
            "action": "Keep B-class data outside the v1 feature ABI until source-specific vintage tests pass.",
        },
        {
            "severity": "MEDIUM",
            "finding": "Two zero-byte crypto-reference Parquets were isolated",
            "evidence": "2026-08-31 CoinGecko global and Etherscan snapshots are under data_crypto_reference/quarantine/corrupt_zero_byte.",
            "impact": "They previously stopped full public-feature materialization.",
            "action": "Retain quarantine evidence; canonical latest pointers reference valid later snapshots.",
        },
        {
            "severity": "MEDIUM",
            "finding": "Binance archive is not globally complete",
            "evidence": f"state={archive.get('state')}; source_invalid={archive.get('source_invalid_objects')}; quarantined_monthly={archive.get('quarantined_monthly_objects')}",
            "impact": "Per-object verified data is usable, but completeness claims must remain partition-specific.",
            "action": "Resolve invalid object and keep daily-over-monthly precedence receipts.",
        },
        {
            "severity": "MEDIUM",
            "finding": "Dune history is unavailable",
            "evidence": f"state={dune.get('state')}; rows={dune.get('rows')}",
            "impact": "No CEX labelled-flow or registered Dune feature can enter training.",
            "action": "Leave excluded until credits and every partition receipt are complete.",
        },
    ]
    generated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": 1,
        "generated_at_utc": generated_at,
        "cutoff": CUTOFF.isoformat(),
        "class_definitions": {
            "A": "historical observation with an authoritative or conservative point-in-time availability contract",
            "B": "historical event values exist, but historical release/revision vintages are incomplete",
            "C": "current or prospectively collected snapshots only",
            "D": "blocked or no usable rows",
        },
        "datasets": rows,
        "quality_findings": critical_findings,
        "public_feature_build": {
            "path": "data_bybit/public_features/bybit_crypto_public_daily.parquet",
            "failed_symbols": public_build.get("failed_symbols"),
            "rows": public_build.get("output_rows"),
            "sha256": public_build.get("output_sha256"),
            "historical_event_backprojection": public_build.get("historical_event_backprojection"),
            "okx_input": public_build.get("input_receipts", {}).get("okx_symbols", {}).get("path"),
        },
        "footer_profiles": {
            "bybit_1m": bybit_1m,
            "bybit_funding": bybit_funding,
            "binance_1m": binance_1m,
            "okx_1m": okx_1m,
            "okx_15m": okx_15m,
        },
    }
    atomic_write_json(output_dir / "coverage_matrix.json", payload)
    atomic_write_text(output_dir / "coverage_matrix.csv", pl.DataFrame(rows).write_csv())
    atomic_write_json(output_dir / "quality_findings.json", critical_findings)
    report = f"""# 加密貨幣歷史公開資料覆蓋與因果性稽核

產生時間：{generated_at}

## 判定原則

事件發生日期不等於模型當時可取得日期。A 類才可依收據中的 availability clock 進歷史因果訓練；B 類可保存與研究，但在補齊發布／修訂 vintage 前不得回投；C 類只可從本機首次觀測後前瞻使用；D 類不可用。

## 覆蓋矩陣

{_markdown_table(rows)}

## 目前正式日頻公開特徵

- rows: {public_build.get('output_rows')}
- failed_symbols: {public_build.get('failed_symbols')}
- sha256: `{public_build.get('output_sha256')}`
- OKX input receipt: `{public_build.get('input_receipts', {}).get('okx_symbols', {}).get('path')}`
- historical_event_backprojection: `{public_build.get('historical_event_backprojection')}`

## 品質缺口

""" + "\n".join(
        f"- **{item['severity']} — {item['finding']}**：{item['evidence']} Impact: {item['impact']} Action: {item['action']}"
        for item in critical_findings
    ) + "\n"
    atomic_write_text(output_dir / "report.md", report)
    print(json.dumps({"output_dir": str(output_dir), "datasets": len(rows), "findings": len(critical_findings)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
