#!/usr/bin/env python3
"""Backtest a one-lot Classic Opening ATM TXO straddle from official daily data.

This is intentionally a daily-price proxy benchmark.  Each option leg enters
at its official daily open (first transaction) and exits at its official daily
close (last transaction).  Those two legs are not guaranteed to be
simultaneous and are not historical executable bid/ask quotes.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sys
from typing import Any, Final

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.taifex_daily_download_common import atomic_write_json  # noqa: E402
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    TAIFEX_OPTION_SERIES_SCOPES,
    TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
    TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
    TAIFEX_TXO_MULTIPLIER,
    load_taifex_opening_atm_straddles,
)


DEFAULT_INPUTS: Final[dict[str, Path]] = {
    "monthly": Path("data_tw_index_options_daily/monthly_opening_atm_pairs.parquet"),
    "weekly": Path(
        "data_tw_index_options_daily/weekly_nearest_expiry_opening_atm_pairs.parquet"
    ),
}
TAIFEX_OPTION_POSITION_SIDES: Final[tuple[str, ...]] = ("long", "short")
DEFAULT_OUTPUT_DIRS: Final[dict[tuple[str, str], Path]] = {
    ("monthly", "long"): Path(
        "artifacts/research/taifex_classic_opening_straddle_daily_long_history"
    ),
    ("weekly", "long"): Path(
        "artifacts/research/taifex_classic_opening_straddle_weekly_nearest_expiry"
    ),
    ("monthly", "short"): Path(
        "artifacts/research/taifex_classic_opening_short_straddle_daily_history"
    ),
    ("weekly", "short"): Path(
        "artifacts/research/taifex_classic_opening_short_straddle_weekly_nearest_expiry"
    ),
}
OUTPUT_SCHEMA_VERSION: Final[int] = 3


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False, encoding="utf-8")
    temporary.replace(path)


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(path)


def _safe_profit_factor(values: pd.Series) -> float | None:
    gains = float(values.clip(lower=0.0).sum())
    losses = float(-values.clip(upper=0.0).sum())
    return gains / losses if losses > 0.0 else None


def _longest_streak(flags: np.ndarray) -> int:
    best = 0
    current = 0
    for flag in flags:
        current = current + 1 if bool(flag) else 0
        best = max(best, current)
    return best


def _reasons(frame: pd.DataFrame) -> Counter[str]:
    counts: Counter[str] = Counter()
    for raw in frame.loc[~frame["executable"], "exclusion_reason"].dropna():
        counts.update(str(raw).split("|"))
    return counts


def _position_direction(position_side: str) -> int:
    normalized = str(position_side).strip().lower()
    if normalized not in TAIFEX_OPTION_POSITION_SIDES:
        raise ValueError(
            f"unsupported position_side={position_side!r}; "
            f"expected one of {TAIFEX_OPTION_POSITION_SIDES}"
        )
    return 1 if normalized == "long" else -1


def _build_daily(
    source: pd.DataFrame,
    *,
    fee_per_contract_side_twd: float,
    position_side: str = "long",
) -> pd.DataFrame:
    direction = _position_direction(position_side)
    normalized_side = "long" if direction == 1 else "short"
    daily = source.loc[source["executable"]].copy()
    required = ["call_open", "call_close", "put_open", "put_close"]
    if daily[required].isna().any().any():
        raise ValueError("executable option rows contain missing prices")
    if (daily[required] <= 0.0).any().any():
        raise ValueError("executable option rows contain nonpositive prices")
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date", kind="stable").reset_index(drop=True)
    if daily["date"].duplicated().any():
        raise ValueError("normalized option dataset contains duplicate dates")
    daily["year"] = daily["date"].dt.year.astype(int)
    daily["month"] = daily["date"].dt.to_period("M").astype(str)
    daily["position_side"] = normalized_side
    daily["opening_premium_points"] = daily["call_open"] + daily["put_open"]
    daily["closing_value_points"] = daily["call_close"] + daily["put_close"]
    daily["opening_premium_twd"] = (
        daily["opening_premium_points"] * TAIFEX_TXO_MULTIPLIER
    )
    daily["closing_value_twd"] = (
        daily["closing_value_points"] * TAIFEX_TXO_MULTIPLIER
    )
    daily["entry_option_cashflow_twd"] = -direction * daily["opening_premium_twd"]
    daily["exit_option_cashflow_twd"] = direction * daily["closing_value_twd"]
    daily["entry_call_contracts"] = direction
    daily["entry_put_contracts"] = direction
    daily["exit_call_contracts"] = -direction
    daily["exit_put_contracts"] = -direction
    daily["final_call_contracts"] = (
        daily["entry_call_contracts"] + daily["exit_call_contracts"]
    )
    daily["final_put_contracts"] = (
        daily["entry_put_contracts"] + daily["exit_put_contracts"]
    )
    daily["call_gross_pnl_twd"] = direction * (
        daily["call_close"] - daily["call_open"]
    ) * TAIFEX_TXO_MULTIPLIER
    daily["put_gross_pnl_twd"] = direction * (
        daily["put_close"] - daily["put_open"]
    ) * TAIFEX_TXO_MULTIPLIER
    daily["gross_pnl_twd"] = (
        daily["entry_option_cashflow_twd"] + daily["exit_option_cashflow_twd"]
    )
    daily["fee_twd"] = 4.0 * float(fee_per_contract_side_twd)
    daily["net_pnl_twd"] = daily["gross_pnl_twd"] - daily["fee_twd"]
    daily["net_pnl_over_opening_premium_reference"] = (
        daily["net_pnl_twd"] / daily["opening_premium_twd"]
    )
    daily["net_pnl_over_opening_premium"] = (
        daily["net_pnl_over_opening_premium_reference"]
        if direction == 1
        else np.nan
    )
    daily["cumulative_net_pnl_twd"] = daily["net_pnl_twd"].cumsum()
    peak = daily["cumulative_net_pnl_twd"].cummax().clip(lower=0.0)
    daily["drawdown_twd"] = daily["cumulative_net_pnl_twd"] - peak
    return daily


def _coverage_by_year(source: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    source = source.copy()
    source["date"] = pd.to_datetime(source["date"])
    source["year"] = source["date"].dt.year.astype(int)
    candidates = source.groupby("year", as_index=False).agg(
        candidate_sessions=("date", "count"),
        executable_sessions=("executable", "sum"),
    )
    candidates["executable_sessions"] = candidates["executable_sessions"].astype(int)
    candidates["coverage_share"] = (
        candidates["executable_sessions"] / candidates["candidate_sessions"]
    )
    if daily.empty:
        for column in (
            "gross_pnl_twd",
            "fee_twd",
            "net_pnl_twd",
            "winning_sessions",
            "win_rate",
            "average_net_pnl_twd",
            "median_net_pnl_twd",
            "year_end_cumulative_net_pnl_twd",
            "worst_drawdown_twd",
        ):
            candidates[column] = 0.0
        return candidates
    annual = daily.groupby("year", as_index=False).agg(
        gross_pnl_twd=("gross_pnl_twd", "sum"),
        fee_twd=("fee_twd", "sum"),
        net_pnl_twd=("net_pnl_twd", "sum"),
        winning_sessions=("net_pnl_twd", lambda values: int((values > 0.0).sum())),
        win_rate=("net_pnl_twd", lambda values: float((values > 0.0).mean())),
        average_net_pnl_twd=("net_pnl_twd", "mean"),
        median_net_pnl_twd=("net_pnl_twd", "median"),
        year_end_cumulative_net_pnl_twd=("cumulative_net_pnl_twd", "last"),
        worst_drawdown_twd=("drawdown_twd", "min"),
    )
    return candidates.merge(annual, on="year", how="left").fillna(0.0)


def _monthly(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "sessions",
                "net_pnl_twd",
                "month_end_cumulative_net_pnl_twd",
                "month_worst_drawdown_twd",
            ]
        )
    return daily.groupby("month", as_index=False).agg(
        sessions=("date", "count"),
        net_pnl_twd=("net_pnl_twd", "sum"),
        month_end_cumulative_net_pnl_twd=("cumulative_net_pnl_twd", "last"),
        month_worst_drawdown_twd=("drawdown_twd", "min"),
    )


def _assert_accounting(
    daily: pd.DataFrame,
    *,
    fee: float,
    position_side: str = "long",
) -> None:
    direction = _position_direction(position_side)
    normalized_side = "long" if direction == 1 else "short"
    expected_gross = direction * (
        (daily["call_close"] + daily["put_close"])
        - (daily["call_open"] + daily["put_open"])
    ) * TAIFEX_TXO_MULTIPLIER
    if not np.allclose(daily["gross_pnl_twd"], expected_gross, rtol=0.0, atol=1e-9):
        raise AssertionError("gross P&L accounting mismatch")
    if not np.allclose(daily["fee_twd"], 4.0 * fee, rtol=0.0, atol=1e-12):
        raise AssertionError("fee accounting mismatch")
    if not np.allclose(
        daily["entry_option_cashflow_twd"] + daily["exit_option_cashflow_twd"],
        daily["gross_pnl_twd"],
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("option premium cash-flow accounting mismatch")
    if not (daily["position_side"] == normalized_side).all():
        raise AssertionError("position-side accounting mismatch")
    if not (
        (daily["final_call_contracts"] == 0)
        & (daily["final_put_contracts"] == 0)
    ).all():
        raise AssertionError("option session did not finish flat")
    if not np.allclose(
        daily["net_pnl_twd"],
        daily["gross_pnl_twd"] - daily["fee_twd"],
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("net P&L accounting mismatch")
    if not np.allclose(
        daily["cumulative_net_pnl_twd"],
        daily["net_pnl_twd"].cumsum(),
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("cumulative P&L accounting mismatch")


def _summary(
    source: pd.DataFrame,
    daily: pd.DataFrame,
    annual: pd.DataFrame,
    monthly: pd.DataFrame,
    *,
    input_path: Path,
    manifest_path: Path | None,
    series_scope: str,
    position_side: str,
    data_contract_version: int,
    fee: float,
    artifact_paths: dict[str, Path],
) -> dict[str, Any]:
    net = daily["net_pnl_twd"]
    best_index = int(net.idxmax()) if len(net) else None
    worst_index = int(net.idxmin()) if len(net) else None
    profitable_years = int((annual["net_pnl_twd"] > 0.0).sum())
    losing_years = int((annual["net_pnl_twd"] < 0.0).sum())
    candidate_rows = int(len(source))
    executed = int(len(daily))
    total_net = float(net.sum())
    total_gross = float(daily["gross_pnl_twd"].sum())
    total_fees = float(daily["fee_twd"].sum())
    is_short = position_side == "short"
    opposite_position_side = "long" if is_short else "short"
    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": "classic_opening_atm_straddle",
        "profitability_after_fixed_fee": total_net > 0.0,
        "engineering_boundary": (
            "pre-margin naked-short daily-price P&L screen; option margin, tax, "
            "slippage, mark-to-market liquidation, and ruin are not modeled, so "
            "this is not an executable or capital-valid naked-short backtest"
            if is_short
            else "complete daily-price proxy backtest; not an executable historical "
            "bid/ask backtest and not an investment conclusion"
        ),
        "parameters": {
            "product": "TXO",
            "session": "day",
            "series_scope": series_scope,
            "position_side": position_side,
            "series": (
                "nearest unexpired monthly option only"
                if series_scope == "monthly"
                else "nearest-expiry listed weekly option only"
            ),
            "atm_reference": "official front-month TX day-session open",
            "entry": (
                "sell each selected Call/Put at official daily open (first trade)"
                if is_short
                else "buy each selected Call/Put at official daily open (first trade)"
            ),
            "exit": (
                "buy back each selected Call/Put at official daily close (last trade)"
                if is_short
                else "sell each selected Call/Put at official daily close (last trade)"
            ),
            "contracts_per_leg": 1,
            "multiplier_twd_per_point": TAIFEX_TXO_MULTIPLIER,
            "fee_per_contract_side_twd": fee,
            "fee_per_session_twd": 4.0 * fee,
            "slippage_points": 0.0,
            "transaction_tax_in_primary_result": False,
            "liquidity_or_depth_cap": False,
            "daily_flatten": True,
            "formal_naked_short_backtest_complete": not is_short,
            "capital_return_reportable": not is_short,
        },
        "data_contract": {
            "version": data_contract_version,
            "price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
            "input_path": str(input_path),
            "input_sha256": _sha256(input_path),
            "manifest_path": str(manifest_path) if manifest_path else None,
            "manifest_sha256": _sha256(manifest_path) if manifest_path else None,
        },
        "coverage": {
            "candidate_first_date": str(pd.to_datetime(source["date"]).min().date()),
            "candidate_last_date": str(pd.to_datetime(source["date"]).max().date()),
            "candidate_sessions": candidate_rows,
            "executable_first_date": str(daily["date"].min().date()) if executed else None,
            "executable_last_date": str(daily["date"].max().date()) if executed else None,
            "executable_sessions": executed,
            "excluded_sessions": candidate_rows - executed,
            "executable_share": executed / candidate_rows if candidate_rows else 0.0,
            "exclusion_reason_counts": dict(sorted(_reasons(source).items())),
        },
        "results": {
            "gross_pnl_twd": total_gross,
            "fees_twd": total_fees,
            "net_pnl_twd": total_net,
            "opposite_position_same_sample": {
                "position_side": opposite_position_side,
                "gross_pnl_twd": -total_gross,
                "fees_twd": total_fees,
                "net_pnl_twd": -total_gross - total_fees,
                "gross_pnl_is_exact_mirror": True,
            },
            "winning_sessions": int((net > 0.0).sum()),
            "losing_sessions": int((net < 0.0).sum()),
            "flat_sessions": int((net == 0.0).sum()),
            "win_rate": float((net > 0.0).mean()) if executed else 0.0,
            "average_net_pnl_twd": float(net.mean()) if executed else 0.0,
            "median_net_pnl_twd": float(net.median()) if executed else 0.0,
            "profit_factor": _safe_profit_factor(net),
            "maximum_drawdown_twd": float(daily["drawdown_twd"].min()) if executed else 0.0,
            "profitable_years": profitable_years,
            "losing_years": losing_years,
            "longest_winning_streak_sessions": _longest_streak((net > 0.0).to_numpy()),
            "longest_losing_streak_sessions": _longest_streak((net < 0.0).to_numpy()),
            "best_session": {
                "date": str(daily.loc[best_index, "date"].date()),
                "net_pnl_twd": float(daily.loc[best_index, "net_pnl_twd"]),
            } if best_index is not None else None,
            "worst_session": {
                "date": str(daily.loc[worst_index, "date"].date()),
                "net_pnl_twd": float(daily.loc[worst_index, "net_pnl_twd"]),
            } if worst_index is not None else None,
        },
        "limitations": [
            "Call/Put daily opens and closes can occur at different times.",
            "Daily open/close are transaction-price proxies, not executable ask/bid quotes.",
            "Historical order-book depth is unavailable in the official daily archive.",
            "Primary result applies the fixed commission only; no transaction tax is backfilled.",
            "No alternate strike is chosen when the theoretical ATM pair lacks valid prices.",
            *(
                [
                    "Naked-short option margin, intraday mark-to-market, forced liquidation, and ruin are not modeled.",
                    "Opening premium is a cash inflow, not the capital denominator; no short-straddle ROI is reported.",
                ]
                if is_short
                else []
            ),
        ],
        "artifacts": {
            key: {"path": str(path), "sha256": _sha256(path)}
            for key, path in artifact_paths.items()
        },
        "monthly_rows": int(len(monthly)),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--series-scope",
        choices=TAIFEX_OPTION_SERIES_SCOPES,
        default="monthly",
    )
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--position-side",
        choices=TAIFEX_OPTION_POSITION_SIDES,
        default="long",
    )
    parser.add_argument("--fee-per-contract-side-twd", type=float, default=22.0)
    args = parser.parse_args()
    if args.fee_per_contract_side_twd < 0.0:
        parser.error("--fee-per-contract-side-twd must be non-negative")

    input_path = (args.input or DEFAULT_INPUTS[args.series_scope]).expanduser().resolve()
    output_dir = (
        args.output_dir or DEFAULT_OUTPUT_DIRS[(args.series_scope, args.position_side)]
    ).expanduser().resolve()
    table = load_taifex_opening_atm_straddles(
        input_path,
        expected_series_scope=args.series_scope,
    )
    raw_version = (table.schema.metadata or {}).get(b"stockagent.contract_version")
    data_contract_version = (
        int(raw_version)
        if raw_version is not None
        else TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION
    )
    source = table.to_pandas()
    if source.empty:
        raise ValueError("normalized option dataset is empty")
    if source["date"].astype(str).duplicated().any():
        raise ValueError("normalized option dataset contains duplicate dates")
    daily = _build_daily(
        source,
        fee_per_contract_side_twd=args.fee_per_contract_side_twd,
        position_side=args.position_side,
    )
    _assert_accounting(
        daily,
        fee=args.fee_per_contract_side_twd,
        position_side=args.position_side,
    )
    annual = _coverage_by_year(source, daily)
    monthly = _monthly(daily)
    exclusions = (
        source.loc[~source["executable"], ["date", "exclusion_reason", "tx_open"]]
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "daily_results": output_dir / "daily_results.parquet",
        "annual_results": output_dir / "annual_results.csv",
        "monthly_results": output_dir / "monthly_results.csv",
        "excluded_sessions": output_dir / "excluded_sessions.csv",
    }
    _atomic_parquet(daily, artifact_paths["daily_results"])
    _atomic_csv(annual, artifact_paths["annual_results"])
    _atomic_csv(monthly, artifact_paths["monthly_results"])
    _atomic_csv(exclusions, artifact_paths["excluded_sessions"])

    manifest_path = input_path.parent / (
        "manifest.json" if args.series_scope == "monthly" else "manifest_weekly.json"
    )
    summary = _summary(
        source,
        daily,
        annual,
        monthly,
        input_path=input_path,
        manifest_path=manifest_path if manifest_path.is_file() else None,
        series_scope=args.series_scope,
        position_side=args.position_side,
        data_contract_version=data_contract_version,
        fee=args.fee_per_contract_side_twd,
        artifact_paths=artifact_paths,
    )
    atomic_write_json(output_dir / "summary.json", summary)
    print(
        f"Classic opening ATM {args.position_side} straddle ({args.series_scope}): "
        f"{len(daily):,} sessions, "
        f"net TWD {summary['results']['net_pnl_twd']:,.2f}, "
        f"MDD TWD {summary['results']['maximum_drawdown_twd']:,.2f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
