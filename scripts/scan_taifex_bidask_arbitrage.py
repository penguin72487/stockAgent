#!/usr/bin/env python3
"""Scan captured TX/TXO books for ideal one-package model-free arbitrage.

This is an executable-quote ceiling, not a transaction-print backtest.  At each
local one-second snapshot every buy uses displayed best ask and every sell uses
displayed best bid.  A package is eligible only when all legs are non-stale,
uncrossed, non-simtrade books and level-one displayed quantity covers the full
package.  No slippage or order latency is added.  Marketable limits and market
orders therefore have the same modeled fill price.

Positions are valued by their model-free expiry payoff bound.  Entry broker
fees and statutory transaction tax are exact for the displayed prices.  Since
the future final-settlement level is not yet known, expiry transaction tax is a
conservative estimate: current TX midpoint is used as the tax base and the
maximum simultaneously exercised option-leg count is charged.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Final, Iterable, Mapping, Sequence

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import (  # noqa: E402
    _sha256_path,
)
from scripts.backtest_taifex_option_benchmarks import (  # noqa: E402
    FIXED_FEES_PER_CONTRACT_SIDE,
    FUTURES_MULTIPLIERS,
    OPTION_MULTIPLIER,
    OPTION_PRODUCT,
)
from stockagent.data.shioaji_capture_parts import (  # noqa: E402
    read_capture_manifests,
    select_capture_part_paths,
    shared_capture_id,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    TAIPEI,
    _atomic_json,
    _atomic_parquet,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    TAIFEX_OPTION_PREMIUM_TAX_RATE,
    option_cash_settlement_transaction_tax_twd,
    option_premium_transaction_tax_twd,
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 1
DEFAULT_CAPTURE_ROOT: Final[Path] = Path(
    "data_tw_index_derivatives_ticks/shioaji_fop_captures"
)
DEFAULT_OUTPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_bidask_arbitrage"
)
VARIANT_IDS: Final[tuple[str, ...]] = (
    "put_call_parity_tx",
    "call_vertical_bounds",
    "put_vertical_bounds",
    "call_butterfly_bounds",
    "put_butterfly_bounds",
    "box_spread",
)


@dataclass(frozen=True, slots=True)
class ContractMeta:
    code: str
    security_type: str
    expiry: date
    delivery_month: str
    multiplier: float
    strike: float | None = None
    right: str | None = None


@dataclass(frozen=True, slots=True)
class Quote:
    code: str
    bid: float
    ask: float
    bid_qty: int
    ask_qty: int
    age_ms: float
    receive_ns: int
    depth_enforced: bool = True


@dataclass(frozen=True, slots=True)
class Leg:
    code: str
    instrument_type: str
    side: str
    quantity: int
    price: float
    available_qty: int
    strike: float | None = None
    right: str | None = None


@dataclass(frozen=True, slots=True)
class Candidate:
    variant_id: str
    direction: str
    expiry: date
    delivery_month: str
    strikes: tuple[float, ...]
    gross_locked_edge_twd: float
    entry_fixed_fees_twd: float
    entry_transaction_tax_twd: float
    estimated_settlement_tax_twd: float
    legs: tuple[Leg, ...]

    @property
    def net_before_settlement_tax_twd(self) -> float:
        return (
            self.gross_locked_edge_twd
            - self.entry_fixed_fees_twd
            - self.entry_transaction_tax_twd
        )

    @property
    def net_after_estimated_settlement_tax_twd(self) -> float:
        return (
            self.net_before_settlement_tax_twd
            - self.estimated_settlement_tax_twd
        )


def _parse_contracts(manifest: Mapping[str, Any]) -> dict[str, ContractMeta]:
    output: dict[str, ContractMeta] = {}
    for raw in manifest.get("contract_metadata", []):
        security_type = str(raw["security_type"]).upper()
        delivery_date = date.fromisoformat(str(raw["delivery_date"]))
        meta = ContractMeta(
            code=str(raw["code"]),
            security_type=security_type,
            expiry=delivery_date,
            delivery_month=str(raw["delivery_month"]),
            multiplier=float(raw["multiplier"]),
            strike=(
                float(raw["strike_price"])
                if security_type == "OPT"
                else None
            ),
            right=(str(raw["option_right"]).upper() if security_type == "OPT" else None),
        )
        output[meta.code] = meta
    if not output:
        raise RuntimeError("capture manifest has no contract metadata")
    return output


def _valid_quote(row: Mapping[str, Any]) -> bool:
    bid = float(row["bid_price_1"])
    ask = float(row["ask_price_1"])
    return bool(
        not row["stale"]
        and not row["suspend"]
        and not row["simtrade"]
        and math.isfinite(bid)
        and math.isfinite(ask)
        and bid > 0.0
        and ask > bid
        and int(row["bid_volume_1"]) > 0
        and int(row["ask_volume_1"]) > 0
        and int(row["book_receive_ts_ns"]) <= int(row["snapshot_ts_ns"])
    )


def _quote(row: Mapping[str, Any]) -> Quote:
    return Quote(
        code=str(row["code"]),
        bid=float(row["bid_price_1"]),
        ask=float(row["ask_price_1"]),
        bid_qty=int(row["bid_volume_1"]),
        ask_qty=int(row["ask_volume_1"]),
        age_ms=float(row["book_age_ms"]),
        receive_ns=int(row["book_receive_ts_ns"]),
    )


def _leg(
    meta: ContractMeta, quote: Quote, *, side: str, quantity: int
) -> Leg | None:
    if quantity <= 0:
        raise ValueError("leg quantity must be positive")
    normalized = side.lower()
    if normalized == "buy":
        price = quote.ask
        available = quote.ask_qty
    elif normalized == "sell":
        price = quote.bid
        available = quote.bid_qty
    else:
        raise ValueError(f"unsupported side: {side}")
    if quote.depth_enforced and available < quantity:
        return None
    return Leg(
        code=meta.code,
        instrument_type=("option" if meta.security_type == "OPT" else "future"),
        side=normalized,
        quantity=quantity,
        price=price,
        available_qty=available,
        strike=meta.strike,
        right=meta.right,
    )


def _entry_costs(legs: Sequence[Leg], *, trading_date: date) -> tuple[float, float]:
    fees = 0.0
    taxes = 0.0
    future_tax_rate = stock_index_futures_tax_rate(trading_date)
    for leg in legs:
        if leg.instrument_type == "option":
            fees += leg.quantity * FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT]
            taxes += leg.quantity * option_premium_transaction_tax_twd(
                leg.price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
                tax_rate=TAIFEX_OPTION_PREMIUM_TAX_RATE,
            )
        else:
            product = "TX"
            fees += leg.quantity * FIXED_FEES_PER_CONTRACT_SIDE[product]
            taxes += leg.quantity * taifex_tax_per_contract_twd(
                leg.price,
                multiplier_twd_per_point=FUTURES_MULTIPLIERS[product],
                tax_rate=future_tax_rate,
            )
    return fees, taxes


def _settlement_tax_estimate(
    *, variant_id: str, expiry: date, settlement_proxy: float
) -> float:
    option_counts = {
        "put_call_parity_tx": 4,
        "call_vertical_bounds": 2,
        "put_vertical_bounds": 2,
        "call_butterfly_bounds": 4,
        "put_butterfly_bounds": 4,
        "box_spread": 2,
    }
    option_tax = option_cash_settlement_transaction_tax_twd(
        settlement_proxy,
        settlement_date=expiry,
        multiplier_twd_per_point=OPTION_MULTIPLIER,
    )
    total = option_counts[variant_id] * option_tax
    if variant_id == "put_call_parity_tx":
        total += taifex_tax_per_contract_twd(
            settlement_proxy,
            multiplier_twd_per_point=FUTURES_MULTIPLIERS["TX"],
            tax_rate=stock_index_futures_tax_rate(expiry),
        )
    return float(total)


def _candidate(
    *,
    variant_id: str,
    direction: str,
    expiry: date,
    delivery_month: str,
    strikes: tuple[float, ...],
    gross_edge_points: float,
    legs: Sequence[Leg | None],
    trading_date: date,
    settlement_proxy: float,
) -> Candidate | None:
    if any(leg is None for leg in legs):
        return None
    complete = tuple(leg for leg in legs if leg is not None)
    fees, entry_tax = _entry_costs(complete, trading_date=trading_date)
    return Candidate(
        variant_id=variant_id,
        direction=direction,
        expiry=expiry,
        delivery_month=delivery_month,
        strikes=strikes,
        gross_locked_edge_twd=float(gross_edge_points * OPTION_MULTIPLIER),
        entry_fixed_fees_twd=fees,
        entry_transaction_tax_twd=entry_tax,
        estimated_settlement_tax_twd=_settlement_tax_estimate(
            variant_id=variant_id,
            expiry=expiry,
            settlement_proxy=settlement_proxy,
        ),
        legs=complete,
    )


def _objective_value(candidate: Candidate, objective: str) -> float:
    if objective == "gross":
        return candidate.gross_locked_edge_twd
    if objective == "after_fees":
        return candidate.gross_locked_edge_twd - candidate.entry_fixed_fees_twd
    if objective == "after_entry_tax":
        return candidate.net_before_settlement_tax_twd
    if objective == "after_settlement_tax":
        return candidate.net_after_estimated_settlement_tax_twd
    raise ValueError(f"unsupported candidate objective: {objective}")


def _best(
    candidates: Iterable[Candidate | None],
    *,
    objective: str = "after_settlement_tax",
) -> Candidate | None:
    eligible = [candidate for candidate in candidates if candidate is not None]
    return (
        max(eligible, key=lambda item: _objective_value(item, objective))
        if eligible
        else None
    )


def _execution_quote(
    quote: Quote, *, price_mode: str, enforce_depth: bool
) -> Quote:
    if price_mode == "active":
        bid = quote.bid
        ask = quote.ask
    elif price_mode == "midpoint":
        bid = ask = (quote.bid + quote.ask) / 2.0
    else:
        raise ValueError(f"unsupported price mode: {price_mode}")
    return Quote(
        code=quote.code,
        bid=bid,
        ask=ask,
        bid_qty=quote.bid_qty,
        ask_qty=quote.ask_qty,
        age_ms=quote.age_ms,
        receive_ns=quote.receive_ns,
        depth_enforced=enforce_depth,
    )


def _scan_chain(
    *,
    option_metas: Sequence[ContractMeta],
    quotes: Mapping[str, Quote],
    future_meta: ContractMeta,
    future_quote: Quote,
    trading_date: date,
    price_mode: str = "active",
    enforce_depth: bool = True,
    objective: str = "after_settlement_tax",
) -> dict[str, Candidate | None]:
    if not option_metas:
        return {variant_id: None for variant_id in VARIANT_IDS}
    expiry = option_metas[0].expiry
    delivery_month = option_metas[0].delivery_month
    quotes = {
        code: _execution_quote(
            quote, price_mode=price_mode, enforce_depth=enforce_depth
        )
        for code, quote in quotes.items()
    }
    future_quote = _execution_quote(
        future_quote, price_mode=price_mode, enforce_depth=enforce_depth
    )
    settlement_proxy = (future_quote.bid + future_quote.ask) / 2.0
    by: dict[tuple[float, str], tuple[ContractMeta, Quote]] = {}
    for meta in option_metas:
        quote = quotes.get(meta.code)
        if quote is not None and meta.strike is not None and meta.right is not None:
            by[(meta.strike, meta.right)] = (meta, quote)

    calls = sorted(strike for strike, right in by if right == "C")
    puts = sorted(strike for strike, right in by if right == "P")
    common = sorted(set(calls) & set(puts))

    parity: list[Candidate | None] = []
    if future_meta.expiry == expiry:
        for strike in common:
            call_meta, call_quote = by[(strike, "C")]
            put_meta, put_quote = by[(strike, "P")]
            # Four TXO synthetic forwards match one TX contract (4*50 = 200).
            parity.extend(
                (
                _candidate(
                    variant_id="put_call_parity_tx",
                    direction="sell_rich_synthetic_buy_tx",
                    expiry=expiry,
                    delivery_month=delivery_month,
                    strikes=(strike,),
                    gross_edge_points=4.0
                    * (
                        call_quote.bid
                        - put_quote.ask
                        + strike
                        - future_quote.ask
                    ),
                    legs=(
                        _leg(call_meta, call_quote, side="sell", quantity=4),
                        _leg(put_meta, put_quote, side="buy", quantity=4),
                        _leg(future_meta, future_quote, side="buy", quantity=1),
                    ),
                    trading_date=trading_date,
                    settlement_proxy=settlement_proxy,
                ),
                _candidate(
                    variant_id="put_call_parity_tx",
                    direction="buy_cheap_synthetic_sell_tx",
                    expiry=expiry,
                    delivery_month=delivery_month,
                    strikes=(strike,),
                    gross_edge_points=4.0
                    * (
                        future_quote.bid
                        - strike
                        - call_quote.ask
                        + put_quote.bid
                    ),
                    legs=(
                        _leg(call_meta, call_quote, side="buy", quantity=4),
                        _leg(put_meta, put_quote, side="sell", quantity=4),
                        _leg(future_meta, future_quote, side="sell", quantity=1),
                    ),
                    trading_date=trading_date,
                    settlement_proxy=settlement_proxy,
                ),
                )
            )

    output: dict[str, Candidate | None] = {
        "put_call_parity_tx": _best(parity, objective=objective)
    }
    for right, strikes, vertical_id, butterfly_id in (
        ("C", calls, "call_vertical_bounds", "call_butterfly_bounds"),
        ("P", puts, "put_vertical_bounds", "put_butterfly_bounds"),
    ):
        verticals: list[Candidate | None] = []
        for low, high in zip(strikes, strikes[1:]):
            low_meta, low_quote = by[(low, right)]
            high_meta, high_quote = by[(high, right)]
            width = high - low
            if right == "C":
                long_cost = low_quote.ask - high_quote.bid
                long_legs = (
                    _leg(low_meta, low_quote, side="buy", quantity=1),
                    _leg(high_meta, high_quote, side="sell", quantity=1),
                )
                short_credit = low_quote.bid - high_quote.ask
                short_legs = (
                    _leg(low_meta, low_quote, side="sell", quantity=1),
                    _leg(high_meta, high_quote, side="buy", quantity=1),
                )
            else:
                long_cost = high_quote.ask - low_quote.bid
                long_legs = (
                    _leg(high_meta, high_quote, side="buy", quantity=1),
                    _leg(low_meta, low_quote, side="sell", quantity=1),
                )
                short_credit = high_quote.bid - low_quote.ask
                short_legs = (
                    _leg(high_meta, high_quote, side="sell", quantity=1),
                    _leg(low_meta, low_quote, side="buy", quantity=1),
                )
            verticals.extend(
                (
                    _candidate(
                        variant_id=vertical_id,
                        direction="buy_negative_cost_vertical",
                        expiry=expiry,
                        delivery_month=delivery_month,
                        strikes=(low, high),
                        gross_edge_points=-long_cost,
                        legs=long_legs,
                        trading_date=trading_date,
                        settlement_proxy=settlement_proxy,
                    ),
                    _candidate(
                        variant_id=vertical_id,
                        direction="sell_above_max_payoff_vertical",
                        expiry=expiry,
                        delivery_month=delivery_month,
                        strikes=(low, high),
                        gross_edge_points=short_credit - width,
                        legs=short_legs,
                        trading_date=trading_date,
                        settlement_proxy=settlement_proxy,
                    ),
                )
            )
        output[vertical_id] = _best(verticals, objective=objective)

        butterflies: list[Candidate | None] = []
        for low, middle, high in zip(strikes, strikes[1:], strikes[2:]):
            width = middle - low
            if width <= 0.0 or not math.isclose(
                width, high - middle, abs_tol=1e-9
            ):
                continue
            low_meta, low_quote = by[(low, right)]
            middle_meta, middle_quote = by[(middle, right)]
            high_meta, high_quote = by[(high, right)]
            long_cost = (
                low_quote.ask - 2.0 * middle_quote.bid + high_quote.ask
            )
            short_credit = (
                low_quote.bid - 2.0 * middle_quote.ask + high_quote.bid
            )
            butterflies.extend(
                (
                    _candidate(
                        variant_id=butterfly_id,
                        direction="buy_negative_cost_butterfly",
                        expiry=expiry,
                        delivery_month=delivery_month,
                        strikes=(low, middle, high),
                        gross_edge_points=-long_cost,
                        legs=(
                            _leg(low_meta, low_quote, side="buy", quantity=1),
                            _leg(
                                middle_meta,
                                middle_quote,
                                side="sell",
                                quantity=2,
                            ),
                            _leg(high_meta, high_quote, side="buy", quantity=1),
                        ),
                        trading_date=trading_date,
                        settlement_proxy=settlement_proxy,
                    ),
                    _candidate(
                        variant_id=butterfly_id,
                        direction="sell_above_max_payoff_butterfly",
                        expiry=expiry,
                        delivery_month=delivery_month,
                        strikes=(low, middle, high),
                        gross_edge_points=short_credit - width,
                        legs=(
                            _leg(low_meta, low_quote, side="sell", quantity=1),
                            _leg(
                                middle_meta,
                                middle_quote,
                                side="buy",
                                quantity=2,
                            ),
                            _leg(high_meta, high_quote, side="sell", quantity=1),
                        ),
                        trading_date=trading_date,
                        settlement_proxy=settlement_proxy,
                    ),
                )
            )
        output[butterfly_id] = _best(butterflies, objective=objective)

    boxes: list[Candidate | None] = []
    for low, high in zip(common, common[1:]):
        width = high - low
        low_call_meta, low_call = by[(low, "C")]
        high_call_meta, high_call = by[(high, "C")]
        low_put_meta, low_put = by[(low, "P")]
        high_put_meta, high_put = by[(high, "P")]
        long_cost = (
            low_call.ask - high_call.bid + high_put.ask - low_put.bid
        )
        short_credit = (
            low_call.bid - high_call.ask + high_put.bid - low_put.ask
        )
        boxes.extend(
            (
                _candidate(
                    variant_id="box_spread",
                    direction="buy_underpriced_box",
                    expiry=expiry,
                    delivery_month=delivery_month,
                    strikes=(low, high),
                    gross_edge_points=width - long_cost,
                    legs=(
                        _leg(low_call_meta, low_call, side="buy", quantity=1),
                        _leg(high_call_meta, high_call, side="sell", quantity=1),
                        _leg(high_put_meta, high_put, side="buy", quantity=1),
                        _leg(low_put_meta, low_put, side="sell", quantity=1),
                    ),
                    trading_date=trading_date,
                    settlement_proxy=settlement_proxy,
                ),
                _candidate(
                    variant_id="box_spread",
                    direction="sell_overpriced_box",
                    expiry=expiry,
                    delivery_month=delivery_month,
                    strikes=(low, high),
                    gross_edge_points=short_credit - width,
                    legs=(
                        _leg(low_call_meta, low_call, side="sell", quantity=1),
                        _leg(high_call_meta, high_call, side="buy", quantity=1),
                        _leg(high_put_meta, high_put, side="sell", quantity=1),
                        _leg(low_put_meta, low_put, side="buy", quantity=1),
                    ),
                    trading_date=trading_date,
                    settlement_proxy=settlement_proxy,
                ),
            )
        )
    output["box_spread"] = _best(boxes, objective=objective)
    return output


def _iso_taipei(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(
        timestamp_ns / 1_000_000_000.0, tz=timezone.utc
    ).astimezone(TAIPEI).isoformat()


def _candidate_row(
    *, snapshot_ns: int, candidate: Candidate, future_quote: Quote
) -> dict[str, Any]:
    return {
        "snapshot_ts": _iso_taipei(snapshot_ns),
        "snapshot_ts_ns": snapshot_ns,
        "delivery_month": candidate.delivery_month,
        "expiry": candidate.expiry,
        "variant_id": candidate.variant_id,
        "direction": candidate.direction,
        "strikes_json": json.dumps(candidate.strikes),
        "gross_locked_edge_twd": candidate.gross_locked_edge_twd,
        "entry_fixed_fees_twd": candidate.entry_fixed_fees_twd,
        "entry_transaction_tax_twd": candidate.entry_transaction_tax_twd,
        "estimated_settlement_tax_twd": candidate.estimated_settlement_tax_twd,
        "net_before_settlement_tax_twd": candidate.net_before_settlement_tax_twd,
        "net_after_estimated_settlement_tax_twd": (
            candidate.net_after_estimated_settlement_tax_twd
        ),
        "positive_after_all_modeled_costs": (
            candidate.net_after_estimated_settlement_tax_twd > 0.0
        ),
        "future_bid": future_quote.bid,
        "future_ask": future_quote.ask,
        "max_book_age_ms": max(
            [future_quote.age_ms]
            + [
                # Quote age is recovered from the candidate's code below by
                # the caller; leg prices and available quantities remain the
                # immutable execution audit.
                0.0
                for _leg_item in candidate.legs
            ]
        ),
        "legs_json": json.dumps(
            [
                {
                    "code": leg.code,
                    "instrument_type": leg.instrument_type,
                    "side": leg.side,
                    "quantity": leg.quantity,
                    "price": leg.price,
                    "available_qty": leg.available_qty,
                    "strike": leg.strike,
                    "right": leg.right,
                }
                for leg in candidate.legs
            ],
            separators=(",", ":"),
        ),
    }


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.write_csv(temporary)
    temporary.replace(path)


def _render_report(
    *, summary: pl.DataFrame, data_quality: Mapping[str, Any], output_dir: Path
) -> None:
    rows = summary.sort(
        "max_net_after_estimated_settlement_tax_twd", descending=True
    ).to_dicts()
    lines = [
        "# TX/TXO Bid/Ask 主動成交理想套利上限",
        "",
        f"- 捕捉日期：{data_quality['trade_date']}",
        f"- 捕捉環境：{'模擬帳戶' if data_quality['simulation_account_environment'] else '正式帳戶'}",
        f"- 同步秒數：{data_quality['snapshot_seconds']:,}",
        f"- 有效報價率：{data_quality['valid_book_fraction']:.4%}",
        "- 買進價：最佳 Ask；賣出價：最佳 Bid；第一檔量必須覆蓋完整組合。",
        "- 未加入滑價或下單延遲；marketable limit 與一檔足量市價單使用相同價格。",
        "- 到期稅為估計值：以當秒 TX 中間價作稅基，並使用最大同時履約腿數。",
        "",
        "| 到期月 | 方法 | 正套利秒數 | 最佳毛利 | 進場費稅後 | 含估計到期稅 | 最佳時間 | 履約價 |",
        "|---|---|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {delivery_month} | {variant_id} | {positive_snapshot_seconds:,} | "
            "{best_gross_locked_edge_twd:,.0f} | "
            "{best_net_before_settlement_tax_twd:,.0f} | "
            "{best_net_after_estimated_settlement_tax_twd:,.0f} | "
            "{best_snapshot_ts} | `{best_strikes_json}` |".format(**row)
        )
    lines.extend(
        (
            "",
            "## 解讀限制",
            "",
            "這是已顯示 Bid/Ask 的零延遲理想上限，不是成交保證。捕捉來自模擬帳戶環境，且目前只有一天；因此不能計算有意義的日 Sharpe、Sortino、MDD 或 Calmar。",
            "",
            "TAIEX 現貨指數不能直接交易，且本捕捉沒有同步成分股籃子；MTX/TMF 也未被訂閱，所以本報告不把那些方法偽裝成可計算結果。",
            "",
        )
    )
    path = output_dir / "report.md"
    temporary = path.with_suffix(".md.tmp")
    temporary.write_text("\n".join(lines), encoding="utf-8")
    temporary.replace(path)


def scan_capture(
    *, capture_root: Path, trade_date: date, output_dir: Path
) -> dict[str, Any]:
    trade_date_text = trade_date.isoformat()
    manifests = read_capture_manifests(capture_root, trade_date_text)
    if not manifests:
        raise RuntimeError(f"no capture manifests for {trade_date_text}")
    capture_id = shared_capture_id(manifests)
    if capture_id is None:
        raise RuntimeError("BidAsk arbitrage scan requires capture schema >= 3")
    if any(str(item.get("status")) != "complete" for item in manifests):
        raise RuntimeError("capture manifest is not complete")
    if len(manifests) != 1:
        raise RuntimeError("multi-worker quote merge is not yet supported")
    manifest = manifests[0]
    contracts = _parse_contracts(manifest)
    futures = [meta for meta in contracts.values() if meta.security_type == "FUT"]
    if len(futures) != 1 or futures[0].multiplier != FUTURES_MULTIPLIERS["TX"]:
        raise RuntimeError("capture must contain exactly one TX-equivalent future")
    future_meta = futures[0]
    options = [meta for meta in contracts.values() if meta.security_type == "OPT"]
    if not options or any(meta.multiplier != OPTION_MULTIPLIER for meta in options):
        raise RuntimeError("capture contains no valid TXO contracts")
    option_by_month: dict[str, list[ContractMeta]] = {}
    for meta in options:
        option_by_month.setdefault(meta.delivery_month, []).append(meta)

    parts = select_capture_part_paths(
        capture_root=capture_root,
        kind="book_1s",
        trade_date=trade_date_text,
        manifests=manifests,
        verify_part_counts=True,
    )
    columns = (
        "snapshot_ts_ns",
        "code",
        "book_receive_ts_ns",
        "book_age_ms",
        "stale",
        "suspend",
        "simtrade",
        "bid_price_1",
        "ask_price_1",
        "bid_volume_1",
        "ask_volume_1",
    )
    frame = (
        pl.scan_parquet(parts)
        .select(columns)
        .sort(["snapshot_ts_ns", "code"])
        .collect()
    )
    duplicate_rows = frame.select(
        pl.struct(["snapshot_ts_ns", "code"]).is_duplicated().sum()
    ).item()
    if duplicate_rows:
        raise RuntimeError(f"duplicate snapshot/code rows: {duplicate_rows}")

    all_rows = frame.height
    valid_rows = 0
    snapshot_count = 0
    output_rows: list[dict[str, Any]] = []
    for (snapshot_ns_raw,), group in frame.group_by(
        "snapshot_ts_ns", maintain_order=True
    ):
        snapshot_ns = int(snapshot_ns_raw)
        snapshot_count += 1
        raw_rows = group.to_dicts()
        valid = [row for row in raw_rows if _valid_quote(row)]
        valid_rows += len(valid)
        quotes = {str(row["code"]): _quote(row) for row in valid}
        future_quote = quotes.get(future_meta.code)
        if future_quote is None:
            continue
        for delivery_month, chain in sorted(option_by_month.items()):
            candidates = _scan_chain(
                option_metas=chain,
                quotes=quotes,
                future_meta=future_meta,
                future_quote=future_quote,
                trading_date=trade_date,
            )
            for variant_id in VARIANT_IDS:
                candidate = candidates.get(variant_id)
                if candidate is None:
                    continue
                row = _candidate_row(
                    snapshot_ns=snapshot_ns,
                    candidate=candidate,
                    future_quote=future_quote,
                )
                row["max_book_age_ms"] = max(
                    [future_quote.age_ms]
                    + [quotes[leg.code].age_ms for leg in candidate.legs]
                )
                output_rows.append(row)

    if not output_rows:
        raise RuntimeError("no complete quote packages were evaluable")
    snapshot_best = pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["delivery_month", "variant_id", "snapshot_ts_ns"]
    )
    summary = (
        snapshot_best.group_by(["delivery_month", "expiry", "variant_id"])
        .agg(
            pl.len().alias("evaluable_snapshot_seconds"),
            pl.col("positive_after_all_modeled_costs")
            .sum()
            .alias("positive_snapshot_seconds"),
            pl.col("gross_locked_edge_twd").max().alias("max_gross_locked_edge_twd"),
            pl.col("net_before_settlement_tax_twd")
            .max()
            .alias("max_net_before_settlement_tax_twd"),
            pl.col("net_after_estimated_settlement_tax_twd")
            .max()
            .alias("max_net_after_estimated_settlement_tax_twd"),
            pl.col("net_after_estimated_settlement_tax_twd")
            .filter(pl.col("positive_after_all_modeled_costs"))
            .sum()
            .alias("sum_positive_second_edges_not_simultaneously_tradeable_twd"),
        )
        .with_columns(
            (
                pl.col("positive_snapshot_seconds")
                / pl.col("evaluable_snapshot_seconds")
            ).alias("positive_snapshot_fraction")
        )
    )
    best_rows = (
        snapshot_best.sort(
            "net_after_estimated_settlement_tax_twd", descending=True
        )
        .unique(["delivery_month", "variant_id"], keep="first")
        .select(
            "delivery_month",
            "variant_id",
            pl.col("snapshot_ts").alias("best_snapshot_ts"),
            pl.col("direction").alias("best_direction"),
            pl.col("strikes_json").alias("best_strikes_json"),
            pl.col("legs_json").alias("best_legs_json"),
            pl.col("max_book_age_ms").alias("best_max_book_age_ms"),
            pl.col("gross_locked_edge_twd").alias("best_gross_locked_edge_twd"),
            pl.col("entry_fixed_fees_twd").alias("best_entry_fixed_fees_twd"),
            pl.col("entry_transaction_tax_twd").alias(
                "best_entry_transaction_tax_twd"
            ),
            pl.col("estimated_settlement_tax_twd").alias(
                "best_estimated_settlement_tax_twd"
            ),
            pl.col("net_before_settlement_tax_twd").alias(
                "best_net_before_settlement_tax_twd"
            ),
            pl.col("net_after_estimated_settlement_tax_twd").alias(
                "best_net_after_estimated_settlement_tax_twd"
            ),
        )
    )
    summary = summary.join(
        best_rows, on=["delivery_month", "variant_id"], how="left"
    ).sort(["delivery_month", "variant_id"])

    output_dir.mkdir(parents=True, exist_ok=True)
    _atomic_parquet(snapshot_best, output_dir / "snapshot_best.parquet")
    _atomic_parquet(summary, output_dir / "summary.parquet")
    _write_csv(snapshot_best, output_dir / "snapshot_best.csv")
    _write_csv(summary, output_dir / "summary.csv")

    audit_path = capture_root / "audits" / f"{trade_date_text}.json"
    audit = (
        json.loads(audit_path.read_text(encoding="utf-8"))
        if audit_path.is_file()
        else {}
    )
    data_quality = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "usable_as_zero_latency_quote_ceiling_only",
        "trade_date": trade_date_text,
        "capture_id": capture_id,
        "simulation_account_environment": bool(manifest.get("simulation", True)),
        "capture_manifest_status": manifest.get("status"),
        "capture_started_at_utc": manifest.get("started_at_utc"),
        "capture_finished_at_utc": manifest.get("finished_at_utc"),
        "snapshot_seconds": snapshot_count,
        "book_rows": all_rows,
        "valid_book_rows": valid_rows,
        "valid_book_fraction": valid_rows / all_rows if all_rows else 0.0,
        "dropped_events": int(manifest.get("dropped_events", -1)),
        "missed_snapshot_seconds": int(manifest.get("missed_snapshot_seconds", -1)),
        "contract_count": len(contracts),
        "future_code": future_meta.code,
        "option_delivery_months": sorted(option_by_month),
        "selected_book_part_count": len(parts),
        "duplicate_snapshot_code_rows": int(duplicate_rows),
        "transport_delay_audit_ms_p50": audit.get("transport_delay_ms_p50"),
        "transport_delay_audit_ms_p99": audit.get("transport_delay_ms_p99"),
        "transport_clock_warning": (
            "exchange timestamps lead local receive timestamps in this capture; "
            "the scan therefore uses local receive-time snapshots only"
        ),
        "execution_contract": {
            "decision_clock": "local whole-second book snapshot",
            "buy_price": "best ask",
            "sell_price": "best bid",
            "quantity_gate": "full package must fit displayed level-one quantity",
            "stale_books_rejected": True,
            "crossed_or_locked_books_rejected": True,
            "simtrade_rows_rejected": True,
            "latency_added_ms": 0.0,
            "slippage_added_points": 0.0,
            "passive_fill_assumed": False,
            "position_exit": "model-free official-expiry payoff bound",
            "financing_interest_rate": 0.0,
        },
        "metric_availability": {
            "sharpe": "not meaningful with one captured trading date",
            "sortino": "not meaningful with one captured trading date",
            "maximum_drawdown": "not meaningful with one captured trading date",
            "calmar": "not meaningful with one captured trading date",
        },
    }
    _atomic_json(output_dir / "data_quality.json", data_quality)
    _render_report(summary=summary, data_quality=data_quality, output_dir=output_dir)

    summary_payload = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "capture_root": str(capture_root),
            "capture_id": capture_id,
            "manifest_paths": [
                str(
                    capture_root
                    / "manifests"
                    / f"trade_date={trade_date_text}"
                    / f"worker={int(item['worker_index']):02d}.json"
                )
                for item in manifests
            ],
            "manifest_sha256": [
                _sha256_path(
                    capture_root
                    / "manifests"
                    / f"trade_date={trade_date_text}"
                    / f"worker={int(item['worker_index']):02d}.json"
                )
                for item in manifests
            ],
            "book_part_count": len(parts),
            "book_part_total_bytes": sum(path.stat().st_size for path in parts),
        },
        "costs": {
            "fixed_fees_per_contract_side_twd": FIXED_FEES_PER_CONTRACT_SIDE,
            "option_premium_tax_rate": TAIFEX_OPTION_PREMIUM_TAX_RATE,
            "futures_tax_rate": stock_index_futures_tax_rate(trade_date),
            "settlement_tax_basis": (
                "current TX midpoint and maximum simultaneously exercised option legs"
            ),
        },
        "variants": [
            {
                **row,
                "expiry": (
                    row["expiry"].isoformat()
                    if isinstance(row.get("expiry"), date)
                    else row.get("expiry")
                ),
            }
            for row in summary.to_dicts()
        ],
        "unsupported": [
            {
                "method": "TAIEX cash-and-carry",
                "reason": "no synchronized tradeable constituent basket quotes",
            },
            {
                "method": "TX/MTX/TMF equivalent-future arbitrage",
                "reason": "capture subscribed TX only",
            },
            {
                "method": "calendar spread",
                "reason": "different expiries retain carry risk and are not model-free",
            },
        ],
        "data_quality": data_quality,
    }
    _atomic_json(output_dir / "summary.json", summary_payload)
    receipt_paths = [
        output_dir / "snapshot_best.parquet",
        output_dir / "snapshot_best.csv",
        output_dir / "summary.parquet",
        output_dir / "summary.csv",
        output_dir / "summary.json",
        output_dir / "data_quality.json",
        output_dir / "report.md",
    ]
    receipt = {
        "schema_version": 1,
        "status": "complete",
        "outputs": {
            path.name: {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in receipt_paths
        },
    }
    _atomic_json(output_dir / "receipt.json", receipt)
    return summary_payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-root", type=Path, default=DEFAULT_CAPTURE_ROOT)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = scan_capture(
        capture_root=args.capture_root,
        trade_date=args.trade_date,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "output_dir": str(args.output_dir),
                "variants": len(result["variants"]),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
