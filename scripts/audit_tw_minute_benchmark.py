#!/usr/bin/env python3
"""Reconcile a saved tw_minute benchmark with the configured total-return asset.

The minute strategy is intraday, but its reporting benchmark is not.  The
configured benchmark is one adjusted-close buy-and-hold path from the split's
first close through its final close.  This audit preserves the original
artifact, writes a corrected copy, and produces source-backed evidence.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stockagent.backtest.report import compute_metrics
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import load_config
from stockagent.data.walkforward import build_expanding_year_folds


BENCHMARK_CONTRACT = (
    "configured_symbol_adjusted_close_buy_hold_first_to_last_v1"
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default="configs/markets/tw_minute_dual_5090.yaml",
        help="Resolved tw_minute market config.",
    )
    parser.add_argument("--fold-id", type=int, default=4)
    parser.add_argument(
        "--artifact-root",
        default=(
            "artifacts/benchmarks/tw_minute_fold4/"
            "optimized_b64_c16_bf16_prefetch"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=(
            "artifacts/benchmarks/tw_minute_fold4/"
            "benchmark_reconciliation"
        ),
    )
    return parser.parse_args()


def _array_root(meta_path: Path, metadata: dict[str, Any]) -> Path:
    relative = Path(str(metadata["arrays"]["dates"]["file"]))
    for candidate in (meta_path.parent, *meta_path.parents):
        if (candidate / relative).is_file():
            return candidate
    raise RuntimeError(f"panel cache arrays are missing for {meta_path}")


def _array_path(
    *,
    root: Path,
    metadata: dict[str, Any],
    name: str,
) -> Path:
    entry = metadata.get("arrays", {}).get(name)
    if not isinstance(entry, dict) or not entry.get("file"):
        raise RuntimeError(f"panel cache lacks array {name!r}")
    path = (root / str(entry["file"])).resolve()
    if not path.is_file():
        raise RuntimeError(f"panel cache array is missing: {path}")
    return path


def _cumulative(log_returns: np.ndarray) -> float:
    return float(math.expm1(float(np.asarray(log_returns, dtype=np.float64).sum())))


def _metric_result(
    *,
    strategy_returns: np.ndarray,
    benchmark_returns: np.ndarray,
    turnovers: np.ndarray,
) -> BacktestResult:
    rows = int(strategy_returns.size)
    return BacktestResult(
        strategy_returns=np.asarray(strategy_returns, dtype=np.float64),
        benchmark_returns=np.asarray(benchmark_returns, dtype=np.float64),
        turnovers=np.asarray(turnovers, dtype=np.float64),
        weights_history=np.zeros((rows, 1), dtype=np.float64),
        execution_mode="tw_minute",
    )


def _plot(
    *,
    dates: np.ndarray,
    old_log_returns: np.ndarray,
    adjusted_close: np.ndarray,
    output_path: Path,
) -> None:
    old_curve = np.expm1(np.cumsum(old_log_returns, dtype=np.float64))
    corrected_curve = adjusted_close / adjusted_close[0] - 1.0

    fig, axis = plt.subplots(figsize=(17, 6), facecolor="white")
    axis.set_facecolor("white")
    axis.plot(
        dates.astype("datetime64[ms]").astype(object),
        corrected_curve,
        color="#2563EB",
        linewidth=2.4,
        label="Corrected: 2330 adjusted-close buy-and-hold",
    )
    axis.plot(
        dates.astype("datetime64[ms]").astype(object),
        old_curve,
        color="#4B5563",
        linewidth=1.8,
        linestyle="--",
        label="Stored artifact: daily open-to-close reset",
    )
    axis.axhline(0.0, color="#111827", linewidth=0.9, alpha=0.75)
    axis.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    axis.xaxis.set_major_locator(mdates.MonthLocator())
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axis.grid(axis="y", color="#D1D5DB", linewidth=0.8, alpha=0.65)
    axis.spines[["top", "right"]].set_visible(False)
    axis.spines[["left", "bottom"]].set_color("#6B7280")
    axis.tick_params(colors="#374151")
    axis.set_ylabel("Cumulative return", color="#111827")
    axis.set_xlabel("Trading date", color="#111827")
    axis.legend(loc="upper left", frameon=False)
    axis.set_title(
        "Fold 4 benchmark reconciliation",
        loc="left",
        fontsize=15,
        color="#111827",
        pad=25,
    )
    axis.text(
        0.0,
        1.025,
        (
            f"{str(dates[0])} to {str(dates[-1])}; "
            f"{dates.size} sessions; indexed from the first test close"
        ),
        transform=axis.transAxes,
        color="#4B5563",
        fontsize=10,
        va="bottom",
    )
    axis.annotate(
        f"{corrected_curve[-1]:+.2%}",
        xy=(dates[-1].astype("datetime64[ms]").astype(object), corrected_curve[-1]),
        xytext=(-8, 8),
        textcoords="offset points",
        ha="right",
        color="#1D4ED8",
        fontweight="bold",
    )
    axis.annotate(
        f"{old_curve[-1]:+.2%}",
        xy=(dates[-1].astype("datetime64[ms]").astype(object), old_curve[-1]),
        xytext=(-8, -16),
        textcoords="offset points",
        ha="right",
        color="#374151",
        fontweight="bold",
    )
    fig.text(
        0.01,
        0.01,
        "Source: 2330_features.parquet adjclose and saved Fold 4 test_backtest.npz.",
        color="#6B7280",
        fontsize=9,
    )
    fig.tight_layout(rect=(0.0, 0.035, 1.0, 1.0))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160, facecolor="white")
    plt.close(fig)


def _percent(value: float) -> str:
    return f"{value:+.4%}"


def _write_report(*, output_path: Path, audit: dict[str, Any]) -> None:
    values = audit["metrics"]
    quality = audit["data_quality"]
    report = f"""# Fold 4：2330 benchmark 對帳與修正

## 技術摘要

舊 benchmark 定義錯誤。儲存的 Fold 4 產物把 2330 做成每天開盤買、收盤賣並每日重置，因此得到 `{_percent(values['stored_benchmark_cumulative_return'])}`；正確定義應是從測試集第一個交易日收盤持有到最後一個交易日收盤的 2330 `adjclose` 含息路徑，結果為 `{_percent(values['corrected_benchmark_cumulative_return'])}`。

這次修正只改 reporting benchmark，不改模型、策略報酬、交易成本或成交邏輯。既有策略累積報酬仍為 `{_percent(values['strategy_cumulative_return'])}`，但相對 benchmark 的超額報酬由錯誤口徑下的 `{_percent(values['stored_excess_return'])}` 更正為 `{_percent(values['corrected_excess_return'])}`。

![Fold 4 benchmark reconciliation](benchmark_reconciliation.png)

## 主要發現

- 問題類型：metric lineage / 定義錯置；嚴重度 **High**，信心 **High**。
- 舊公式：每一交易日各自計算 `log(close / open)`，再跨日累加，等價於每天重置的 intraday 策略。
- 正確公式：`log(adjclose[last] / adjclose[first])`；panel 的 forward return 會放到價格變動結束的日期，並把 split 第一列設為 0，使路徑從第一個測試日收盤開始且不吃到前一個 split。
- Fold 4 測試範圍：`{audit['scope']['first_date']}` 至 `{audit['scope']['last_date']}`，共 `{audit['scope']['sessions']}` 個交易日。
- 2330 原始收盤價端點報酬為 `{_percent(values['raw_close_endpoint_return'])}`；adjusted-close 含息端點報酬為 `{_percent(values['adjusted_close_endpoint_return'])}`。
- 舊 benchmark 與正確 benchmark 相差 `{values['benchmark_gap_percentage_points']:+.4f}` 個百分點。

## 資料與口徑

| 項目 | 定義 / 結果 |
|---|---|
| Benchmark symbol | `{audit['benchmark_name']}` |
| 起點 | 第一個測試日的 adjusted close，`{values['first_adjusted_close']:.6f}` |
| 終點 | 最後一個測試日的 adjusted close，`{values['last_adjusted_close']:.6f}` |
| 累積 benchmark | `{_percent(values['corrected_benchmark_cumulative_return'])}` |
| Panel 與 parquet 最大單日 log-return 差 | `{quality['panel_vs_source_max_abs_log_return_error']:.3e}` |
| 日期覆蓋 | `{quality['source_rows']}/{quality['expected_rows']}` |
| adjclose 缺值 | `{quality['adjusted_close_nulls']}` |
| 日期重複 | `{quality['duplicate_dates']}` |
| adjustment source | `{', '.join(quality['adjustment_sources'])}` |

## 方法

1. 讀取既有 `test_backtest.npz`，保留策略 returns、turnover、holdings 等所有欄位。
2. 從設定的 daily panel 讀取 2330 `returns_1d`，逐日對齊 Fold 4 測試日期。
3. 用 `2330_features.parquet` 的 `adjclose` 獨立重算端點總報酬。
4. 驗證 panel 日報酬與 parquet 相鄰 adjusted-close 比值一致。
5. 另存修正版 NPZ；原始產物不覆寫。

## 限制與穩健性

- 2026 區間截至 `{audit['scope']['last_date']}`，是部分年度，不是完整 2026 年。
- 本口徑從第一日**收盤**開始，以符合 daily panel 的 adjusted-close forward-return 契約；不是第一日開盤買入。
- `adjclose` 是官方公司行動調整後的 total-return proxy；它同時承接現金股利與其他公司行動調整，不應再額外加一次股息。
- 既有 Fold 4 模型不用重訓；只有舊報告與舊 NPZ 中的 benchmark 欄位需要重算。

## 產物

- `benchmark_reconciliation.json`：完整稽核數值與檢查結果。
- `benchmark_daily_comparison.parquet`：逐日舊／新 benchmark 路徑。
- `test_backtest.corrected_benchmark.npz`：策略欄位不動，只替換 benchmark 的修正版副本。
- `benchmark_reconciliation.png`：舊／新 benchmark 累積曲線。

## 後續

新的訓練／測試報告會直接使用修正後 contract。既有產物若要比較績效，應引用本目錄的修正版，而不要再讀原始 NPZ 的 benchmark 欄位。
"""
    output_path.write_text(report, encoding="utf-8")


def main() -> None:
    args = _arguments()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    benchmark_name = str(config.data.benchmark_name).strip()
    if not benchmark_name:
        raise RuntimeError("config.data.benchmark_name is empty")

    artifact_root = Path(args.artifact_root).resolve()
    fold_dir = artifact_root / f"fold_{int(args.fold_id):02d}"
    backtest_path = fold_dir / "test_backtest.npz"
    if not backtest_path.is_file():
        raise RuntimeError(f"saved backtest is missing: {backtest_path}")

    with np.load(backtest_path, allow_pickle=False) as saved:
        saved_arrays = {name: np.array(saved[name], copy=True) for name in saved.files}
    dates = np.asarray(saved_arrays["dates"], dtype="datetime64[D]")
    strategy_returns = np.asarray(saved_arrays["strategy_returns"], dtype=np.float64)
    stored_benchmark = np.asarray(saved_arrays["benchmark_returns"], dtype=np.float64)
    turnovers = np.asarray(saved_arrays["turnovers"], dtype=np.float64)
    if not (
        dates.ndim == 1
        and dates.size > 1
        and strategy_returns.shape == dates.shape
        and stored_benchmark.shape == dates.shape
        and turnovers.shape == dates.shape
        and bool(np.all(dates[1:] > dates[:-1]))
    ):
        raise RuntimeError("saved backtest rows are malformed or misaligned")

    minute_manifest_path = Path(config.data.minute_parquet_root).resolve() / "manifest.json"
    minute_manifest = json.loads(minute_manifest_path.read_text(encoding="utf-8"))
    minute_dates = np.asarray(minute_manifest["dates"], dtype="datetime64[D]")
    folds = build_expanding_year_folds(
        minute_dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
    )
    matching = [fold for fold in folds if fold.fold_id == int(args.fold_id)]
    if len(matching) != 1:
        raise RuntimeError(f"fold {args.fold_id} is absent from current config")
    configured_test_dates = minute_dates[matching[0].test_indices]
    if not np.array_equal(configured_test_dates, dates):
        raise RuntimeError(
            "saved artifact dates disagree with the current Fold test contract"
        )

    meta_path = Path(config.data.minute_daily_context_panel_meta).resolve()
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    root = _array_root(meta_path, metadata)
    symbols_path = (root / str(metadata["symbols_file"])).resolve()
    panel_symbols = json.loads(symbols_path.read_text(encoding="utf-8"))
    if benchmark_name not in panel_symbols:
        raise RuntimeError(f"panel cache lacks benchmark {benchmark_name}")
    benchmark_symbol_index = panel_symbols.index(benchmark_name)
    panel_dates = np.load(
        _array_path(root=root, metadata=metadata, name="dates"), mmap_mode="r"
    ).astype("datetime64[D]")
    panel_returns = np.load(
        _array_path(root=root, metadata=metadata, name="returns_1d"), mmap_mode="r"
    )
    panel_rows = np.searchsorted(panel_dates, dates)
    if np.any(panel_rows >= panel_dates.size) or not np.array_equal(
        panel_dates[panel_rows], dates
    ):
        raise RuntimeError("daily panel does not cover every saved test date")
    corrected_benchmark = np.zeros(dates.shape, dtype=np.float64)
    # A report row belongs to the date where the close-to-close move ends.
    # The first split row is the buy-and-hold index origin and therefore zero.
    corrected_benchmark[1:] = np.asarray(
        panel_returns[panel_rows[:-1], benchmark_symbol_index], dtype=np.float64
    )

    source_path = Path(config.data.parquet_root).resolve() / (
        f"{benchmark_name}_features.parquet"
    )
    source = (
        pl.scan_parquet(source_path)
        .select("date", "close", "adjclose", "adjustment_source")
        .filter(
            pl.col("date").is_between(
                dates[0].astype(object), dates[-1].astype(object), closed="both"
            )
        )
        .sort("date")
        .collect()
    )
    source_dates = source.get_column("date").to_numpy().astype("datetime64[D]")
    duplicate_dates = int(source_dates.size - np.unique(source_dates).size)
    if not np.array_equal(source_dates, dates):
        raise RuntimeError("2330 source dates do not exactly cover the Fold test dates")
    raw_close = source.get_column("close").to_numpy().astype(np.float64)
    adjusted_close = source.get_column("adjclose").to_numpy().astype(np.float64)
    if not bool(np.all(np.isfinite(raw_close) & (raw_close > 0.0))):
        raise RuntimeError("2330 source has invalid raw close endpoints")
    if not bool(np.all(np.isfinite(adjusted_close) & (adjusted_close > 0.0))):
        raise RuntimeError("2330 source has invalid adjusted close endpoints")
    source_forward_returns = np.zeros_like(adjusted_close)
    source_forward_returns[1:] = np.diff(np.log(adjusted_close))
    panel_vs_source_error = float(
        np.max(np.abs(corrected_benchmark - source_forward_returns))
    )
    if panel_vs_source_error > 2e-6:
        raise RuntimeError(
            "daily panel returns disagree with source adjusted close: "
            f"max_abs_error={panel_vs_source_error:.3e}"
        )

    stored_result = _metric_result(
        strategy_returns=strategy_returns,
        benchmark_returns=stored_benchmark,
        turnovers=turnovers,
    )
    corrected_result = _metric_result(
        strategy_returns=strategy_returns,
        benchmark_returns=corrected_benchmark,
        turnovers=turnovers,
    )
    stored_metrics = compute_metrics(stored_result)
    corrected_metrics = compute_metrics(corrected_result)
    endpoint_return = float(adjusted_close[-1] / adjusted_close[0] - 1.0)
    panel_return = _cumulative(corrected_benchmark)
    if not math.isclose(panel_return, endpoint_return, rel_tol=0.0, abs_tol=2e-6):
        raise RuntimeError("panel cumulative return disagrees with adjusted-close endpoints")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    corrected_path = output_dir / "test_backtest.corrected_benchmark.npz"
    saved_arrays["benchmark_returns"] = corrected_benchmark
    np.savez_compressed(corrected_path, **saved_arrays)

    comparison = pl.DataFrame(
        {
            "date": dates,
            "raw_close": raw_close,
            "adjusted_close": adjusted_close,
            "stored_daily_reset_log_return": stored_benchmark,
            "corrected_buy_hold_log_return": corrected_benchmark,
            "stored_cumulative_return": np.expm1(np.cumsum(stored_benchmark)),
            "corrected_cumulative_return": adjusted_close / adjusted_close[0] - 1.0,
        }
    )
    comparison.write_parquet(output_dir / "benchmark_daily_comparison.parquet")

    adjustment_sources = sorted(
        str(value)
        for value in source.get_column("adjustment_source").drop_nulls().unique().to_list()
    )
    audit = {
        "schema_version": 1,
        "benchmark_contract": BENCHMARK_CONTRACT,
        "benchmark_name": benchmark_name,
        "config": str(config_path),
        "source_artifact": str(backtest_path),
        "corrected_artifact": str(corrected_path),
        "scope": {
            "fold_id": int(args.fold_id),
            "first_date": str(dates[0]),
            "last_date": str(dates[-1]),
            "sessions": int(dates.size),
        },
        "metrics": {
            "strategy_cumulative_return": float(corrected_metrics["cumulative_return"]),
            "stored_benchmark_cumulative_return": float(
                stored_metrics["cumulative_benchmark"]
            ),
            "corrected_benchmark_cumulative_return": float(
                corrected_metrics["cumulative_benchmark"]
            ),
            "stored_excess_return": float(stored_metrics["excess_return_vs_benchmark"]),
            "corrected_excess_return": float(
                corrected_metrics["excess_return_vs_benchmark"]
            ),
            "benchmark_gap_percentage_points": float(
                (corrected_metrics["cumulative_benchmark"] - stored_metrics["cumulative_benchmark"])
                * 100.0
            ),
            "raw_close_endpoint_return": float(raw_close[-1] / raw_close[0] - 1.0),
            "adjusted_close_endpoint_return": endpoint_return,
            "panel_return_sum_cumulative": panel_return,
            "first_raw_close": float(raw_close[0]),
            "last_raw_close": float(raw_close[-1]),
            "first_adjusted_close": float(adjusted_close[0]),
            "last_adjusted_close": float(adjusted_close[-1]),
            "stored_benchmark_sharpe": float(stored_metrics["benchmark_sharpe"]),
            "corrected_benchmark_sharpe": float(corrected_metrics["benchmark_sharpe"]),
        },
        "data_quality": {
            "expected_rows": int(dates.size),
            "source_rows": int(source.height),
            "duplicate_dates": duplicate_dates,
            "adjusted_close_nulls": int(source.get_column("adjclose").null_count()),
            "panel_vs_source_max_abs_log_return_error": panel_vs_source_error,
            "panel_cumulative_vs_endpoint_abs_error": abs(panel_return - endpoint_return),
            "adjustment_sources": adjustment_sources,
            "date_alignment_exact": True,
            "first_return_zeroed_at_buy_hold_origin": bool(
                corrected_benchmark[0] == 0.0
            ),
        },
        "finding": {
            "severity": "high",
            "confidence": "high",
            "root_cause": (
                "stored minute benchmark reset at each session and measured "
                "open-to-close instead of one adjusted-close buy-and-hold path"
            ),
            "strategy_returns_changed": False,
        },
    }
    (output_dir / "benchmark_reconciliation.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    _plot(
        dates=dates,
        old_log_returns=stored_benchmark,
        adjusted_close=adjusted_close,
        output_path=output_dir / "benchmark_reconciliation.png",
    )
    _write_report(output_path=output_dir / "benchmark_diagnostic.md", audit=audit)
    print(json.dumps(audit, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
