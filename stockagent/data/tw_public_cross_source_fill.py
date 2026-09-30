"""Missing-only public feature repair; no statistical imputation or downloads.

Mappings are economic contracts, not fuzzy-name matching. Values are compared
on the original report/as-of period BEFORE publication-clock alignment. Receipt
hashes, conflicts and per-cell provenance stay outside numeric model inputs.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3

import polars as pl

from downloader.parquet_integrity import parquet_receipt_error

CONTRACT = "tw_cross_source_missing_only_v1"


@dataclass(frozen=True)
class Mapping:
    dataset: str
    field: str
    kind: str = "quarter"
    factor: float = 0.001  # FinMind TWD -> FinLab thousand TWD; never learned.
    transform: str = "identity"


INCOME = {
    "營業收入淨額": "Revenue", "營業成本": "CostOfGoodsSold",
    "營業毛利": "GrossProfit", "營業費用": "OperatingExpenses",
    "營業利益": "OperatingIncome", "營業外收入及支出": "TotalNonoperatingIncomeAndExpense",
    "稅前淨利": "PreTaxIncome", "所得稅費用": "TAX", "本期淨利": "IncomeAfterTaxes",
    "其他收益及費損淨額": "OTHNOE", "停業單位損益": "IncomeLossFromDiscontinuedOperation",
    "本期綜合損益總額": "TotalConsolidatedProfitForThePeriod",
}
BALANCE = {
    "資產總額": "TotalAssets", "負債總額": "Liabilities", "權益總額": "Equity",
    "現金及約當現金": "CashAndCashEquivalents", "流動資產": "CurrentAssets",
    "非流動資產": "NoncurrentAssets", "流動負債": "CurrentLiabilities",
    "非流動負債": "NoncurrentLiabilities", "存貨": "Inventories",
    "短期借款": "ShorttermBorrowings", "其他應付款": "OtherPayables",
    "其他流動資產": "OtherCurrentAssets", "其他非流動資產": "OtherNoncurrentAssets",
    "股本": "CapitalStock", "普通股股本": "OrdinaryShare",
    "保留盈餘": "RetainedEarnings", "法定盈餘公積": "LegalReserve",
    "未分配盈餘": "UnappropriatedRetainedEarningsAaccumulatedDeficit",
    "非控制權益": "NoncontrollingInterests", "資本公積合計": "CapitalSurplus",
    "其他權益": "OtherEquityInterest", "當期所得稅負債": "CurrentTaxLiabilities",
    "遞延所得稅資產": "DeferredTaxAssets", "使用權資產": "RightOfUseAsset",
    "按攤銷後成本衡量之金融資產_流動": "FinancialAssetsAtAmortizedCost",
    "按攤銷後成本衡量之金融資產_非流動": "FinancialAssetsAtAmortizedCostNonCurrent",
    "透過其他綜合損益按公允價值衡量之金融資產_流動": "FinancialAssetsAtFairvalueThroughOtherComprehensiveIncome",
    "透過其他綜合損益按公允價值衡量之金融資產_非流動": "FinancialAssetsAtFairvalueThroughOtherComprehensiveIncomeNonCurrent",
    "透過損益按公允價值衡量之金融資產_流動": "CurrentFinancialAssetsAtFairvalueThroughProfitOrLoss",
    "透過損益按公允價值衡量之金融資產_非流動": "NonCurrentFinancialAssetsAtFairvalueThroughProfitOrLoss",
    "透過損益按公允價值衡量之金融負債_流動": "CurrentFinancialLiabilitiesAtFairValueThroughProfitOrLoss",
    "負債準備_流動": "CurrentProvisions", "避險之金融負債_流動": "CurrentDerivativeFinancialLiabilitiesForHedging",
    "避險之金融資產_流動": "HedgingAinancialAssets",
    "避險之金融資產_非流動": "HedgingAinancialAssetsNonCurrent",
}
CASH = {
    "營業活動之淨現金流入_流出": "CashFlowsFromOperatingActivities",
    "投資活動之淨現金流入_流出": "CashProvidedByInvestingActivities",
    "籌資活動之淨現金流入_流出": "CashFlowsProvidedFromFinancingActivities",
    "折舊費用": "Depreciation", "攤銷費用": "AmortizationExpense",
    "存貨_增加_減少": "InventoryIncrease", "應收帳款_增加_減少": "ReceivableIncrease",
    "應付帳款增加_減少": "AccountsPayable", "支付之利息": "PayTheInterest",
    "收益費損項目合計": "TotalIncomeLossItems", "舉借長期借款": "ProceedsFromLongTermDebt",
    "償還長期借款": "RepaymentOfLongTermDebt", "償還公司債": "RedemptionOfBonds",
    "其他投資活動": "OtherInvestingActivities", "本期稅前淨利_淨損": "NetIncomeBeforeTax",
    "本期現金及約當現金增加_減少_數": "CashBalancesIncrease",
    "營運產生之現金流入_流出": "CashReceivedThroughOperations",
    "未實現銷貨利益_損失": "UnrealizedGain", "已實現銷貨損失_利益": "RealizedGain",
}
SHAREHOLDING = {
    "全體外資及陸資持有股數": "ForeignInvestmentShares",
    "外資及陸資尚可投資股數": "ForeignInvestmentRemainingShares",
    "外資及陸資尚可投資比率": "ForeignInvestmentRemainRatio",
    "全體外資及陸資持股比率": "ForeignInvestmentSharesRatio",
    "外資及陸資共用法令投資上限比率": "ForeignInvestmentUpperLimitRatio",
    "陸資法令投資上限比率": "ChineseInvestmentUpperLimitRatio", "發行股數": "NumberOfSharesIssued",
}
MAPPINGS = {
    **{"financial_statement:" + k: Mapping("TaiwanStockFinancialStatements", v) for k, v in INCOME.items()},
    **{"financial_statement:" + k: Mapping("TaiwanStockBalanceSheet", v) for k, v in BALANCE.items()},
    **{"financial_statement:" + k: Mapping("TaiwanStockCashFlowsStatement", v, transform="ytd_to_quarter") for k, v in CASH.items()},
    "financial_statement:每股盈餘": Mapping("TaiwanStockFinancialStatements", "EPS", factor=1.),
    **{"foreign_investors_shareholding:" + k: Mapping("TaiwanStockShareholding", v, "daily", 1.) for k, v in SHAREHOLDING.items()},
    "monthly_revenue:當月營收": Mapping("TaiwanStockMonthRevenue", "revenue", "revenue"),
    "etl:inventory:大於四百張佔比": Mapping("TaiwanStockHoldingSharesPer", "percent", "weekly", 1., "large_holder_sum"),
    "block_trade:成交金額": Mapping("TaiwanStockBlockTrade", "trading_money", "daily", 1., "sum_trades"),
}


def read_finmind(root: Path, dataset: str, *, first="2012-01-01", symbols=None):
    """Pin the canonical current receipts, not every historic object version."""
    with sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        tasks = [dict(r) for r in conn.execute(
            "SELECT * FROM tasks WHERE dataset=? AND partition>=? ORDER BY partition,data_id", (dataset, first))]
    receipts, paths, states = [], [], {}
    for task in tasks:
        states[task["state"]] = states.get(task["state"], 0) + 1
        if task["state"] != "complete":
            continue
        path = root / task["receipt_path"]
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("unsafe receipt path")
        r = json.loads(path.read_text())
        if any(r.get(k) != task[k] for k in ("dataset", "data_id", "partition")) or r.get("status") != "complete":
            raise ValueError("receipt identity mismatch")
        error = parquet_receipt_error(root, r)
        if error:
            raise ValueError(f"{dataset}: {error}")
        paths.append(str(root / r["parquet_path"]))
        receipts.append({"receipt_path": str(path.resolve()), **r})
    if not paths:
        raise ValueError(f"no verified observations: {dataset}")
    # Provider schema additions are kept; absent fields remain NULL.
    # Thousands of UNION children make query-planning dominate the tiny-file
    # read. Bounded batches preserve schema union without a quadratic plan.
    def scan(path):
        table = pl.scan_parquet(path)
        return table.filter(pl.col("stock_id").is_in(symbols)) if symbols is not None else table
    batches = [pl.concat([scan(p) for p in paths[i:i + 128]],
        how="diagonal_relaxed").collect(engine="streaming") for i in range(0, len(paths), 128)]
    table = pl.concat(batches, how="diagonal_relaxed")
    return table, {"root": str(root.resolve()), "dataset": dataset,
        "states": states, "receipts": receipts, "downloaded_observations": table.height,
        "coverage_claim": "queried_partitions_not_proof_of_all_security_cells"}


def period_key(frame: pl.DataFrame, kind: str, column="source_index") -> pl.DataFrame:
    if kind == "quarter":
        return frame.with_columns(pl.col(column).cast(pl.String).str.replace_all("-", "").alias("period"))
    stamp = pl.col(column).cast(pl.String).str.slice(0, 10).str.to_date(strict=True)
    if kind == "revenue":
        # FinLab dates are already in the release month; never add a month twice.
        return frame.with_columns(stamp.dt.offset_by("-1mo").dt.strftime("%Y-%m").alias("period"))
    return frame.with_columns(stamp.dt.strftime("%Y-%m-%d").alias("period"))


def unique_values(table: pl.DataFrame, keys: list[str]):
    table = table.filter(pl.col("value").is_finite())
    grouped = table.group_by(keys).agg(pl.col("value").n_unique().alias("_n"), pl.col("value").first())
    return grouped.filter(pl.col("_n") == 1).drop("_n"), grouped.filter(pl.col("_n") > 1)


def normalize_finmind(frame: pl.DataFrame, spec: Mapping) -> tuple[pl.DataFrame, dict]:
    """Return period/symbol/value; an ambiguous duplicate can never win by sort."""
    if spec.kind == "quarter":
        table = frame.filter(pl.col("type") == spec.field).select(
            pl.col("date").str.to_date().alias("_day"),
            pl.col("stock_id").alias("symbol"), (pl.col("value") * spec.factor).alias("value"))
        table = table.with_columns(pl.concat_str(pl.col("_day").dt.year().cast(pl.String),
            pl.lit("Q"), pl.col("_day").dt.quarter().cast(pl.String)).alias("period"))
        table, bad = unique_values(table, ["period", "symbol"])
        if spec.transform == "ytd_to_quarter":
            table = table.with_columns((pl.col("period").str.slice(0, 4).cast(pl.Int32) * 4
                + pl.col("period").str.slice(-1).cast(pl.Int32)).alias("_q"))
            previous = table.select("symbol", (pl.col("_q") + 1).alias("_q"), pl.col("value").alias("_previous"))
            table = table.join(previous, on=["symbol", "_q"], how="left", validate="1:1").with_columns(
                pl.when(pl.col("period").str.ends_with("Q1")).then(pl.col("value"))
                .otherwise(pl.col("value") - pl.col("_previous")).alias("value")).drop("_q", "_previous")
        return table.filter(pl.col("value").is_finite()), {"conflicting_source_keys": bad.height}
    if spec.kind == "revenue":
        frame = frame.filter(pl.col("country") == "Taiwan")
        table = frame.select(pl.date(pl.col("revenue_year"), pl.col("revenue_month"), 1)
            .dt.strftime("%Y-%m").alias("period"), pl.col("stock_id").alias("symbol"),
            (pl.col(spec.field) * spec.factor).alias("value"))
    elif spec.transform == "large_holder_sum":
        # TDCC levels 12..15 only. Never include total or adjustments.
        levels = ["400,001-600,000", "600,001-800,000", "800,001-1,000,000", "more than 1,000,001"]
        cells, bad = unique_values(frame.filter(pl.col("HoldingSharesLevel").is_in(levels)).select(
            "date", "stock_id", "HoldingSharesLevel", pl.col("percent").alias("value")),
            ["date", "stock_id", "HoldingSharesLevel"])
        grouped = cells.group_by("date", "stock_id").agg(
            pl.col("HoldingSharesLevel").n_unique().alias("_levels"), pl.col("value").sum())
        table = grouped.filter((pl.col("_levels") == 4) & pl.col("value").is_between(0, 100)).select(
            pl.col("date").alias("period"), pl.col("stock_id").alias("symbol"), "value")
        return table, {"conflicting_source_keys": bad.height,
            "incomplete_or_invalid_tiers": grouped.height - table.height}
    elif spec.transform == "sum_trades":
        table = frame.group_by("date", "stock_id").agg(pl.col(spec.field).sum().alias("value")).select(
            pl.col("date").alias("period"), pl.col("stock_id").alias("symbol"), "value")
    else:
        table = frame.select(pl.col("date").alias("period"), pl.col("stock_id").alias("symbol"),
                             (pl.col(spec.field) * spec.factor).alias("value"))
    table, bad = unique_values(table, ["period", "symbol"])
    if "比率" in spec.field or "Ratio" in spec.field:
        table = table.filter(pl.col("value").is_between(0, 100))
    return table, {"conflicting_source_keys": bad.height}


def missing_only(primary: pl.DataFrame, candidate: pl.DataFrame, *, min_overlap=100,
                 min_agreement=0.995, absolute_tolerance=0.011, relative_tolerance=1e-6):
    """Reject incompatible mappings; never vote/overwrite an existing value.

    Candidate has a unique economic key. Primary may contain NULL slots. The
    whole-source agreement is a unit/definition QA gate, not a fitted scale.
    """
    keys = ["period", "symbol"]
    for table in (primary, candidate):
        if table.select(pl.struct(keys).is_duplicated().any()).item():
            raise ValueError("duplicate economic keys")
    overlap = primary.filter(pl.col("value").is_finite()).join(candidate, on=keys, suffix="_other")
    difference = (pl.col("value") - pl.col("value_other")).abs()
    overlap = overlap.with_columns((difference <= absolute_tolerance
        + relative_tolerance * pl.max_horizontal(pl.col("value").abs(), pl.col("value_other").abs())).alias("_agrees"))
    agreement = float(overlap["_agrees"].mean()) if overlap.height else 0.
    accepted = overlap.height >= min_overlap and agreement >= min_agreement
    missing = candidate.join(primary.filter(pl.col("value").is_finite()).select(keys), on=keys, how="anti")
    # A conflicting symbol-period isn't a missing observation and cannot be overwritten.
    fills = missing if accepted else missing.head(0)
    report = {"overlap": overlap.height, "agree": overlap.filter(pl.col("_agrees")).height,
        "conflicts": overlap.filter(~pl.col("_agrees")).height, "agreement": agreement,
        "accepted": accepted, "candidate_missing_keys": missing.height, "filled_keys": fills.height,
        "reason": "verified_mapping_missing_only" if accepted else "mapping_or_vintage_disagreement_requires_review"}
    return fills, report, overlap.filter(~pl.col("_agrees"))
