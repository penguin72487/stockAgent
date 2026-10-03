#!/usr/bin/env python3
"""Inventory every selected and available Bybit-only daily training feature.

Counts describe the materialized source table, not a claim that a model was
trained or that a cold release has been pinned. Historical and prospective
availability contracts remain separate.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
from fnmatch import fnmatchcase
import hashlib
import io
import json
from pathlib import Path
import sys

import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_bybit_crypto_public_daily_features import (  # noqa: E402
    COINGECKO_MARKET_FEATURES,
    FRED_FEATURES,
    FREE_LAST_SNAPSHOT_SERIES,
    FREE_MARKET_SERIES,
    FREE_SNAPSHOT_SERIES,
    FREE_SUM_SNAPSHOT_SERIES,
    SEC_MARKET_FEATURES,
)
from stockagent.config import load_config  # noqa: E402
from downloader.artifact_io import atomic_write_json, atomic_write_text  # noqa: E402


DEFAULT_CONFIG = ROOT / "configs/markets/bybit_perpetual_daily_0005_historical_pit_v1.yaml"
MARKET_FEATURES = {
    *(spec[0] for spec in FREE_MARKET_SERIES),
    *(spec[0] for spec in FREE_SNAPSHOT_SERIES),
    *(spec[0] for spec in FREE_SUM_SNAPSHOT_SERIES),
    *(spec[0] for spec in FREE_LAST_SNAPSHOT_SERIES),
    *FRED_FEATURES,
    *SEC_MARKET_FEATURES,
    *COINGECKO_MARKET_FEATURES,
    "crypto_public_market_available",
}
HISTORICAL_CONTRACTS = {
    "historical_event_time_aligned",
    "historical_completed_bar",
    "historical_initial_release_next_utc_day",
    "historical_acceptance_time_with_registry_selection_risk",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _selected(name: str, include: list[str], exclude: list[str], zero: list[str]) -> bool:
    return (
        any(fnmatchcase(name, pattern) for pattern in include)
        and not any(fnmatchcase(name, pattern) for pattern in exclude)
        and not any(fnmatchcase(name, pattern) for pattern in zero)
    )


def _history_band(first: str | None) -> str:
    if not first:
        return "no_observation"
    observed = date.fromisoformat(str(first)[:10])
    if observed >= date(2026, 1, 1):
        return "recent_only"
    if observed > date(2020, 12, 31):
        return "partial_2020s"
    return "reaches_2020"


def _read_quality(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _source_status(
    *,
    selected: bool,
    contract: str,
    finite_rows: int,
    last_date: str | None,
    daily_last: str | None,
) -> str:
    if finite_rows == 0:
        return "no_eligible_value"
    if contract not in HISTORICAL_CONTRACTS:
        return "prospective_selected" if selected else "prospective_only"
    if not selected:
        return "not_selected"
    if last_date and daily_last and (
        date.fromisoformat(daily_last[:10]) - date.fromisoformat(last_date[:10])
    ).days > 3 and contract == "historical_completed_bar":
        return "exchange_aux_tail_gap"
    return "historical_selected"


def _nonzero_counts(path: Path, names: list[str]) -> dict[str, int]:
    if not names:
        return {}
    values = (
        pl.scan_parquet(path)
        .select(
            [
                (
                    pl.col(name).cast(pl.Float64, strict=False)
                    .is_finite()
                    .fill_null(False)
                    & (pl.col(name) != 0)
                )
                .sum()
                .alias(name)
                for name in names
            ]
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return {name: int(value or 0) for name, value in values.items()}


def build_inventory(
    root: Path,
    config_path: Path,
    quality_path: Path,
    public_path: Path,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    config = load_config(config_path)
    public_summary_path = public_path.with_name(f"{public_path.stem}_summary.json")
    public_summary = json.loads(public_summary_path.read_text(encoding="utf-8"))
    daily_root = root / str(config.data.parquet_root)
    daily_summary = json.loads(
        (daily_root / "materialize_summary.json").read_text(encoding="utf-8")
    )
    if daily_summary.get("contract_version") != 6 or config.trading.crypto_execution_minute_utc != 5:
        raise ValueError("training config and physical daily tree disagree on 00:05/v6")
    if daily_summary.get("failed_symbols"):
        raise ValueError("daily materialization has failed symbols")
    if _sha256(public_path) != public_summary.get("output_sha256"):
        raise ValueError("public feature parquet differs from its build receipt")
    if _sha256(quality_path) != public_summary.get("quality_sha256"):
        raise ValueError("public feature quality CSV differs from its build receipt")

    daily_files = sorted(daily_root.glob("*_features.parquet"))
    dates = []
    daily_rows = 0
    candidate_complete_rows = 0
    quarantined_rows = 0
    for path in daily_files:
        daily = pl.read_parquet(
            path,
            columns=[
                "date", "minute_grid_complete", "execution_available",
                "policy_tradable", "return_quarantined",
            ],
        )
        daily_rows += daily.height
        if daily.height:
            if daily["date"].n_unique() != daily.height:
                raise ValueError(f"duplicate symbol-day rows: {path}")
            dates.extend((str(daily["date"].min()), str(daily["date"].max())))
            metrics = daily.select(
                pl.col("return_quarantined").fill_null(True).sum().alias("quarantined"),
                (
                    pl.col("minute_grid_complete").fill_null(False)
                    & pl.col("execution_available").fill_null(False)
                    & pl.col("policy_tradable").fill_null(False)
                    & ~pl.col("return_quarantined").fill_null(True)
                ).sum().alias("candidate"),
            ).row(0, named=True)
            quarantined_rows += int(metrics["quarantined"])
            candidate_complete_rows += int(metrics["candidate"])
    daily_first = min(dates) if dates else None
    daily_last = max(dates) if dates else None
    public_rows = pq.ParquetFile(public_path).metadata.num_rows
    market_rows = (
        pl.scan_parquet(public_path)
        .select((pl.col("symbol") == "__MARKET__").sum())
        .collect(engine="streaming")
        .item()
    )
    symbol_rows = public_rows - int(market_rows)
    quality = _read_quality(quality_path)
    public_names = set(pq.read_schema(public_path).names)
    if {row["feature"] for row in quality} != public_names - {"date", "symbol"}:
        raise ValueError("feature quality CSV does not cover the exact public schema")
    nonzero = _nonzero_counts(public_path, [row["feature"] for row in quality])
    included = list(config.data.feature_include)
    excluded = list(config.data.feature_exclude)
    zero = list(config.data.feature_zero_fill)
    rows: list[dict[str, object]] = []
    base = [name for name in included if not name.startswith("crypto_")]
    for name in base:
        rows.append(
            {
                "feature": name,
                "source_family": "bybit_1m_daily_panel",
                "grain": "symbol_day",
                "point_in_time_contract": "previous_complete_utc_day",
                "selected_for_training": True,
                "source_first_date": daily_first,
                "source_last_date": daily_last,
                "finite_rows": None,
                "denominator_rows": daily_rows,
                "nonzero_rows": None,
                "coverage_fraction_at_grain": None,
                "symbols_with_value": len(daily_files),
                "history_band": _history_band(daily_first),
                "status": "panel_derived_not_individually_counted",
                "note": "Derived from complete previous-day OHLCV; exact panel values require panel build.",
            }
        )
    for row in quality:
        name = row["feature"]
        finite = int(row["finite_rows"])
        denom = int(market_rows) if name in MARKET_FEATURES else symbol_rows
        selected = _selected(name, included, excluded, zero)
        contract = row["point_in_time_contract"]
        first, last = row["first_date"] or None, row["last_date"] or None
        status = _source_status(
            selected=selected,
            contract=contract,
            finite_rows=finite,
            last_date=last,
            daily_last=daily_last,
        )
        if name.startswith("crypto_public_hyperliquid_") and not selected:
            status = "exchange_out_of_scope"
        note = ""
        if contract == "historical_acceptance_time_with_registry_selection_risk":
            note = "Current ETF registry selection may omit historical entities."
        elif contract not in HISTORICAL_CONTRACTS:
            note = "Only first locally observed vintage is causal; older event dates are not backfilled."
        elif _history_band(first) == "recent_only":
            note = "Historical API rows exist only in the recent observed window."
        if name.endswith("_available") or name.endswith("_available_fraction"):
            note = (note + " " if note else "") + "Finite zero is unavailable, not a populated observation."
        rows.append(
            {
                "feature": name,
                "source_family": row["source_family"],
                "grain": "market_day" if name in MARKET_FEATURES else "symbol_day",
                "point_in_time_contract": contract,
                "selected_for_training": selected,
                "source_first_date": first,
                "source_last_date": last,
                "finite_rows": finite,
                "denominator_rows": denom,
                "nonzero_rows": nonzero[name],
                "coverage_fraction_at_grain": round(finite / max(1, denom), 6),
                "symbols_with_value": int(row["symbols"]),
                "history_band": _history_band(first),
                "status": status,
                "note": note,
            }
        )
    actual_selected = sum(bool(row["selected_for_training"]) for row in rows)
    indicator_patterns = list(config.data.feature_availability_indicators)
    indicator_count = sum(
        bool(row["selected_for_training"])
        and any(fnmatchcase(str(row["feature"]), pattern) for pattern in indicator_patterns)
        for row in rows
    )
    raw_latest = {}
    for provider, path in {
        "bybit_1m": root / "data_bybit/1m/download_summary.json",
    }.items():
        if path.is_file():
            raw_latest[provider] = json.loads(path.read_text(encoding="utf-8")).get("end_date")
    instrument_path = root / "data_bybit/funding/instruments.csv"
    funding_summary_path = root / "data_bybit/funding/funding_summary.json"
    instrument_snapshot_utc = (
        json.loads(funding_summary_path.read_text(encoding="utf-8")).get("snapshot_utc")
        if funding_summary_path.is_file() else None
    )
    innovation_symbols = 0
    if instrument_path.is_file():
        instruments = pl.read_csv(instrument_path, infer_schema_length=10_000)
        innovation_symbols = instruments.filter(
            (pl.col("category") == "linear")
            & (pl.col("quote_coin") == "USDT")
            & (pl.col("settle_coin") == "USDT")
            & (pl.col("contract_type") == "LinearPerpetual")
            & (pl.col("status") == "Trading")
            & (pl.col("symbol_type") == "innovation")
            & ~pl.col("is_pre_listing").fill_null(False)
        ).height
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    raw_bybit = raw_latest.get("bybit_1m")
    expected_complete_date = (
        min(yesterday, date.fromisoformat(str(raw_bybit)[:10]))
        if raw_bybit else None
    )
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": str(config_path),
        "public_path": str(public_path),
        "quality_path": str(quality_path),
        "daily_root": str(daily_root),
        "daily_contract_version": daily_summary["contract_version"],
        "execution_minute_utc": config.trading.crypto_execution_minute_utc,
        "daily_rows_all_physical_files": daily_rows,
        "daily_candidate_complete_executable_rows": candidate_complete_rows,
        "daily_return_quarantined_rows": quarantined_rows,
        "daily_physical_files": len(daily_files),
        "daily_registered_symbols": daily_summary.get("symbols"),
        "trading_innovation_symbols_outside_standard_universe": innovation_symbols,
        "instrument_snapshot_utc": instrument_snapshot_utc,
        "daily_first_date": daily_first,
        "daily_last_date": daily_last,
        "daily_receipt_last_date": str(daily_summary.get("ended_at_utc", ""))[:10],
        "public_rows": public_rows,
        "public_market_rows": int(market_rows),
        "public_symbol_rows": symbol_rows,
        "public_build_last_date": str(public_summary.get("ended_at_utc", ""))[:10],
        "raw_download_end_dates": raw_latest,
        "expected_last_complete_training_date": (
            expected_complete_date.isoformat() if expected_complete_date else None
        ),
        "daily_lag_completed_days": (
            max(0, (expected_complete_date - date.fromisoformat(daily_last[:10])).days)
            if expected_complete_date and daily_last else None
        ),
        "feature_rows": len(rows),
        "selected_feature_rows": actual_selected,
        "derived_availability_indicator_rows": indicator_count,
        "model_input_feature_rows": actual_selected + indicator_count,
        "historical_selected_rows": sum(
            row["status"] in {"historical_selected", "exchange_aux_tail_gap"}
            for row in rows
        ),
        "prospective_only_rows": sum(row["status"] == "prospective_only" for row in rows),
        "prospective_selected_rows": sum(row["status"] == "prospective_selected" for row in rows),
        "exchange_out_of_scope_rows": sum(row["status"] == "exchange_out_of_scope" for row in rows),
        "source_receipt_hashes_match": True,
        "materialization_current_to_raw": bool(
            expected_complete_date and daily_last
            and date.fromisoformat(daily_last[:10]) >= expected_complete_date
        ),
    }
    return rows, summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    config = args.config if args.config.is_absolute() else root / args.config
    selected_config = load_config(config)
    if selected_config.data.crypto_exchange_scope != "bybit":
        raise ValueError("this report describes only the Bybit venue-only contract")
    public = root / selected_config.data.external_feature_path
    quality = public.with_name(f"{public.stem}_quality.csv")
    output_dir = args.output_dir or root / "artifacts/data_quality" / (
        f"crypto_venue_training_inventory_{datetime.now(timezone.utc).date().isoformat()}"
    ) / "bybit"
    rows, summary = build_inventory(root, config, quality, public)
    output_dir.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0])
    csv_buffer = io.StringIO(newline="")
    writer = csv.DictWriter(csv_buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(output_dir / "feature_inventory.csv", csv_buffer.getvalue())
    atomic_write_json(output_dir / "summary.json", summary)
    lines = [
        "# 加密貨幣訓練資料逐 feature 清單",
        "",
        f"產生時間：{summary['generated_at_utc']}",
        "",
        f"- 主資料：Bybit 線性 USDT 永續，前一完整 UTC 日特徵，00:05 UTC 執行。",
        f"- 物理日資料：{summary['daily_physical_files']} 檔、{summary['daily_rows_all_physical_files']} 列，{summary['daily_first_date']} 至 {summary['daily_last_date']}。",
        f"- 同一日表通過完整分鐘格、可執行、可交易且未隔離的候選列：{summary['daily_candidate_complete_executable_rows']}；return 隔離列：{summary['daily_return_quarantined_rows']}。這不等於完整訓練樣本數。",
        f"- Universe 缺口：截至 {summary['instrument_snapshot_utc']} 的舊 instrument 快照，另有 {summary['trading_innovation_symbols_outside_standard_universe']} 個 Bybit innovation 線性 USDT 合約不在 standard 策略契約中；不可聲稱所有加密貨幣符號已入訓。",
        f"- Bybit 自有 funding 特徵：{summary['public_rows']} 列；新設定選用 {summary['selected_feature_rows']} / {summary['feature_rows']} 個欄位。",
        f"- 每欄缺值遮罩：另加 {summary['derived_availability_indicator_rows']} 欄，模型輸入共 {summary['model_input_feature_rows']} 欄。",
        f"- 原始 1m 下載摘要截止：{summary['raw_download_end_dates']}。資料到達不代表 funding／日表／公開特徵已同步。",
        "- `finite_rows` 是來源表有值筆數；`denominator_rows` 依 market-day 或 symbol-day 分開，不能把市場欄位除以全部幣別列。",
        "- `nonzero_rows` 辨認只有 0 的可用性旗標；0 在比率、報酬、事件計數等欄位本身仍可能是合法值。",
        "- `reaches_2020` 僅表示最早有值，不保證從那天到現在每一日、每一個 symbol 都完整。",
        "- 舊控制組將所有外部特徵整族歸零；本清單的 `selected_for_training` 指新設定，不代表模型已完成訓練。",
        "",
        "| Feature | 來源 | 粒度 | 首筆 | 末筆 | 歷史帶 | 有值/粒度列 | 非零 | 新訓練 | 狀態 |",
        "|---|---|---|---|---|---|---:|---:|:---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {feature} | {source_family} | {grain} | {source_first_date} | "
            "{source_last_date} | {history_band} | {finite_rows}/{denominator_rows} | {nonzero_rows} | "
            "{selected} | {status} |".format(
                **row,
                selected="是" if row["selected_for_training"] else "否",
            )
        )
    atomic_write_text(output_dir / "feature_inventory.md", "\n".join(lines) + "\n")
    print(json.dumps({"output_dir": str(output_dir), **summary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
