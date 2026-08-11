#!/usr/bin/env python3
"""Build a monotone TX/TXO ideal-arbitrage funnel from one capture.

The strict funnel changes exactly one assumption at a time:

L0 midpoint, no frictions or depth gate
L1 active best Bid/Ask, no depth gate
L2 active best Bid/Ask plus displayed level-one quantity
L3 L2 plus fixed broker commissions
L4 L3 plus entry transaction tax
L5 L4 plus estimated expiry settlement tax

Every layer re-optimizes time, strikes, direction, method, and expiry.  A second
waterfall freezes the globally best L5 package so the monetary effect of each
assumption remains additive.  Same-local-second transaction prints are emitted
as a separate observation control because prints do not prove simultaneous or
atomic multi-leg execution and therefore are not part of the monotone funnel.
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
from typing import Any, Final, Mapping, Sequence

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.scan_taifex_bidask_arbitrage import (  # noqa: E402
    DEFAULT_CAPTURE_ROOT,
    DEFAULT_OUTPUT_DIR,
    VARIANT_IDS,
    Candidate,
    ContractMeta,
    Leg,
    Quote,
    _entry_costs,
    _iso_taipei,
    _objective_value,
    _parse_contracts,
    _quote,
    _scan_chain,
    _valid_quote,
    _write_csv,
)
from scripts.backtest_taifex_option_benchmarks import (  # noqa: E402
    FUTURES_MULTIPLIERS,
    OPTION_MULTIPLIER,
)
from stockagent.data.shioaji_capture_parts import (  # noqa: E402
    read_capture_manifests,
    select_capture_part_paths,
    shared_capture_id,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    _atomic_json,
    _atomic_parquet,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 1


@dataclass(frozen=True, slots=True)
class LayerSpec:
    order: int
    layer_id: str
    label_zh: str
    added_factor_zh: str
    price_mode: str
    enforce_depth: bool
    objective: str


LAYERS: Final[tuple[LayerSpec, ...]] = (
    LayerSpec(
        0,
        "L0_midpoint_no_friction",
        "L0 中間價、完全無摩擦",
        "基準：所有腿可在同秒中間價成交",
        "midpoint",
        False,
        "gross",
    ),
    LayerSpec(
        1,
        "L1_active_bidask",
        "L1 主動 Bid/Ask",
        "買 Ask、賣 Bid（加入完整買賣價差）",
        "active",
        False,
        "gross",
    ),
    LayerSpec(
        2,
        "L2_level1_depth",
        "L2 第一檔量足夠",
        "完整 package 必須放得進第一檔量",
        "active",
        True,
        "gross",
    ),
    LayerSpec(
        3,
        "L3_fixed_fees",
        "L3 加手續費",
        "TX 60 元、TXO 22 元／口／單邊",
        "active",
        True,
        "after_fees",
    ),
    LayerSpec(
        4,
        "L4_entry_tax",
        "L4 加進場交易稅",
        "依各腿主動成交價計算進場稅",
        "active",
        True,
        "after_entry_tax",
    ),
    LayerSpec(
        5,
        "L5_settlement_tax",
        "L5 加估計到期稅",
        "以當秒 TX 中間價與最大同時履約腿數估計",
        "active",
        True,
        "after_settlement_tax",
    ),
)


def _candidate_layer_row(
    *,
    snapshot_ns: int,
    candidate: Candidate,
    layer: LayerSpec,
    quotes: Mapping[str, Quote],
) -> dict[str, Any]:
    value = _objective_value(candidate, layer.objective)
    return {
        "snapshot_ts": _iso_taipei(snapshot_ns),
        "snapshot_ts_ns": snapshot_ns,
        "layer_order": layer.order,
        "layer_id": layer.layer_id,
        "layer_label_zh": layer.label_zh,
        "added_factor_zh": layer.added_factor_zh,
        "price_mode": layer.price_mode,
        "depth_enforced": layer.enforce_depth,
        "objective": layer.objective,
        "delivery_month": candidate.delivery_month,
        "expiry": candidate.expiry,
        "variant_id": candidate.variant_id,
        "direction": candidate.direction,
        "strikes_json": json.dumps(candidate.strikes),
        "layer_value_twd": value,
        "positive_layer_value": value > 0.0,
        "gross_locked_edge_twd": candidate.gross_locked_edge_twd,
        "entry_fixed_fees_twd": candidate.entry_fixed_fees_twd,
        "entry_transaction_tax_twd": candidate.entry_transaction_tax_twd,
        "estimated_settlement_tax_twd": candidate.estimated_settlement_tax_twd,
        "max_book_age_ms": max(quotes[leg.code].age_ms for leg in candidate.legs),
        "legs_json": json.dumps(
            [
                {
                    "code": leg.code,
                    "instrument_type": leg.instrument_type,
                    "side": leg.side,
                    "quantity": leg.quantity,
                    "price": leg.price,
                    "source_bid": quotes[leg.code].bid,
                    "source_ask": quotes[leg.code].ask,
                    "available_qty": leg.available_qty,
                    "strike": leg.strike,
                    "right": leg.right,
                }
                for leg in candidate.legs
            ],
            separators=(",", ":"),
        ),
    }


def _summarize_layers(
    snapshot_best: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    group_keys = [
        "layer_order",
        "layer_id",
        "layer_label_zh",
        "added_factor_zh",
        "delivery_month",
        "expiry",
        "variant_id",
    ]
    method_summary = snapshot_best.group_by(group_keys).agg(
        pl.len().alias("evaluable_method_seconds"),
        pl.col("positive_layer_value").sum().alias("positive_method_seconds"),
        pl.col("layer_value_twd").max().alias("max_layer_value_twd"),
    )
    best_method_rows = (
        snapshot_best.sort("layer_value_twd", descending=True)
        .unique(["layer_id", "delivery_month", "variant_id"], keep="first")
        .select(
            "layer_id",
            "delivery_month",
            "variant_id",
            pl.col("snapshot_ts").alias("best_snapshot_ts"),
            pl.col("direction").alias("best_direction"),
            pl.col("strikes_json").alias("best_strikes_json"),
            pl.col("legs_json").alias("best_legs_json"),
            pl.col("gross_locked_edge_twd").alias("best_gross_edge_twd"),
            pl.col("entry_fixed_fees_twd").alias("best_fixed_fees_twd"),
            pl.col("entry_transaction_tax_twd").alias("best_entry_tax_twd"),
            pl.col("estimated_settlement_tax_twd").alias(
                "best_estimated_settlement_tax_twd"
            ),
            pl.col("max_book_age_ms").alias("best_max_book_age_ms"),
        )
    )
    method_summary = method_summary.join(
        best_method_rows,
        on=["layer_id", "delivery_month", "variant_id"],
        how="left",
    ).sort(["layer_order", "delivery_month", "variant_id"])

    overall = snapshot_best.group_by(
        ["layer_order", "layer_id", "layer_label_zh", "added_factor_zh"]
    ).agg(
        pl.len().alias("evaluable_method_seconds"),
        pl.col("positive_layer_value").sum().alias("positive_method_seconds"),
        pl.col("layer_value_twd").max().alias("max_layer_value_twd"),
    )
    best_overall_rows = (
        snapshot_best.sort("layer_value_twd", descending=True)
        .unique("layer_id", keep="first")
        .select(
            "layer_id",
            pl.col("delivery_month").alias("best_delivery_month"),
            pl.col("expiry").alias("best_expiry"),
            pl.col("variant_id").alias("best_variant_id"),
            pl.col("snapshot_ts").alias("best_snapshot_ts"),
            pl.col("direction").alias("best_direction"),
            pl.col("strikes_json").alias("best_strikes_json"),
            pl.col("legs_json").alias("best_legs_json"),
            pl.col("gross_locked_edge_twd").alias("best_gross_edge_twd"),
            pl.col("entry_fixed_fees_twd").alias("best_fixed_fees_twd"),
            pl.col("entry_transaction_tax_twd").alias("best_entry_tax_twd"),
            pl.col("estimated_settlement_tax_twd").alias(
                "best_estimated_settlement_tax_twd"
            ),
            pl.col("max_book_age_ms").alias("best_max_book_age_ms"),
        )
    )
    overall = overall.join(best_overall_rows, on="layer_id", how="left").sort(
        "layer_order"
    )
    values = overall["max_layer_value_twd"].to_list()
    previous: float | None = None
    changes: list[float] = []
    ratios: list[float | None] = []
    for value in values:
        changes.append(0.0 if previous is None else float(value - previous))
        ratios.append(
            None
            if previous is None or math.isclose(previous, 0.0)
            else float(value / previous)
        )
        previous = float(value)
    overall = overall.with_columns(
        pl.Series("change_from_previous_layer_twd", changes),
        pl.Series("ratio_to_previous_layer", ratios, dtype=pl.Float64),
    )
    return method_summary, overall


def _fixed_package_waterfall(
    *,
    snapshot_frame: pl.DataFrame,
    contracts: Mapping[str, ContractMeta],
    fully_modeled_best: Mapping[str, Any],
    trading_date: date,
) -> pl.DataFrame:
    snapshot_ns = int(fully_modeled_best["snapshot_ts_ns"])
    raw_rows = snapshot_frame.filter(pl.col("snapshot_ts_ns") == snapshot_ns).to_dicts()
    quotes = {
        str(row["code"]): _quote(row) for row in raw_rows if _valid_quote(row)
    }
    leg_audit = json.loads(str(fully_modeled_best["legs_json"]))
    active_legs: list[Leg] = []
    midpoint_legs: list[Leg] = []
    for raw_leg in leg_audit:
        code = str(raw_leg["code"])
        meta = contracts[code]
        quote = quotes[code]
        side = str(raw_leg["side"])
        quantity = int(raw_leg["quantity"])
        active_price = quote.ask if side == "buy" else quote.bid
        midpoint_price = (quote.bid + quote.ask) / 2.0
        common = {
            "code": code,
            "instrument_type": (
                "option" if meta.security_type == "OPT" else "future"
            ),
            "side": side,
            "quantity": quantity,
            "available_qty": quote.ask_qty if side == "buy" else quote.bid_qty,
            "strike": meta.strike,
            "right": meta.right,
        }
        active_legs.append(Leg(price=active_price, **common))
        midpoint_legs.append(Leg(price=midpoint_price, **common))

    def entry_cash(legs: Sequence[Leg]) -> float:
        total = 0.0
        for leg in legs:
            multiplier = (
                OPTION_MULTIPLIER
                if leg.instrument_type == "option"
                else FUTURES_MULTIPLIERS["TX"]
            )
            sign = 1.0 if leg.side == "sell" else -1.0
            total += sign * leg.quantity * leg.price * multiplier
        return total

    active_cash = entry_cash(active_legs)
    midpoint_cash = entry_cash(midpoint_legs)
    terminal_floor = float(fully_modeled_best["gross_locked_edge_twd"]) - active_cash
    gross_midpoint = terminal_floor + midpoint_cash
    gross_active = terminal_floor + active_cash
    fees, entry_tax = _entry_costs(active_legs, trading_date=trading_date)
    settlement_tax = float(fully_modeled_best["estimated_settlement_tax_twd"])
    values = (
        gross_midpoint,
        gross_active,
        gross_active,
        gross_active - fees,
        gross_active - fees - entry_tax,
        gross_active - fees - entry_tax - settlement_tax,
    )
    rows: list[dict[str, Any]] = []
    previous: float | None = None
    for layer, value in zip(LAYERS, values):
        rows.append(
            {
                "layer_order": layer.order,
                "layer_id": layer.layer_id,
                "layer_label_zh": layer.label_zh,
                "added_factor_zh": layer.added_factor_zh,
                "fixed_package_value_twd": value,
                "change_from_previous_layer_twd": (
                    0.0 if previous is None else value - previous
                ),
                "snapshot_ts": fully_modeled_best["snapshot_ts"],
                "delivery_month": fully_modeled_best["delivery_month"],
                "expiry": fully_modeled_best["expiry"],
                "variant_id": fully_modeled_best["variant_id"],
                "direction": fully_modeled_best["direction"],
                "strikes_json": fully_modeled_best["strikes_json"],
                "depth_sufficient": all(
                    leg.available_qty >= leg.quantity for leg in active_legs
                ),
                "fixed_fees_twd": fees,
                "entry_tax_twd": entry_tax,
                "estimated_settlement_tax_twd": settlement_tax,
                "terminal_payoff_floor_twd": terminal_floor,
                "active_legs_json": json.dumps(
                    [
                        {
                            "code": leg.code,
                            "side": leg.side,
                            "quantity": leg.quantity,
                            "price": leg.price,
                            "available_qty": leg.available_qty,
                        }
                        for leg in active_legs
                    ],
                    separators=(",", ":"),
                ),
            }
        )
        previous = value
    return pl.DataFrame(rows, infer_schema_length=None)


def _trade_print_control(
    *,
    capture_root: Path,
    trade_date: date,
    manifests: Sequence[Mapping[str, Any]],
    contracts: Mapping[str, ContractMeta],
    future_meta: ContractMeta,
    option_by_month: Mapping[str, Sequence[ContractMeta]],
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    parts = select_capture_part_paths(
        capture_root=capture_root,
        kind="ticks",
        trade_date=trade_date.isoformat(),
        manifests=manifests,
        verify_part_counts=True,
    )
    ticks = (
        pl.scan_parquet(parts)
        .select(
            "event_seq",
            "receive_ts_ns",
            "code",
            "close",
            "volume",
            "suspend",
            "simtrade",
        )
        .filter(
            pl.col("code").is_in(list(contracts))
            & pl.col("close").is_finite()
            & (pl.col("close") > 0.0)
            & ~pl.col("suspend")
            & ~pl.col("simtrade")
        )
        .with_columns(
            (
                pl.col("receive_ts_ns")
                // pl.lit(1_000_000_000, dtype=pl.Int64)
                * pl.lit(1_000_000_000, dtype=pl.Int64)
            ).alias("local_second_ns")
        )
        .sort(["local_second_ns", "code", "receive_ts_ns", "event_seq"])
        .group_by(["local_second_ns", "code"], maintain_order=True)
        .agg(
            pl.col("close").last().alias("trade_price"),
            pl.col("volume").last().alias("trade_volume"),
            pl.col("receive_ts_ns").last().alias("last_receive_ts_ns"),
        )
        .sort(["local_second_ns", "code"])
        .collect()
    )
    rows: list[dict[str, Any]] = []
    trade_seconds = 0
    seconds_with_future_print = 0
    for (second_raw,), group in ticks.group_by(
        "local_second_ns", maintain_order=True
    ):
        second_ns = int(second_raw)
        trade_seconds += 1
        quotes: dict[str, Quote] = {}
        for row in group.iter_rows(named=True):
            price = float(row["trade_price"])
            quotes[str(row["code"])] = Quote(
                code=str(row["code"]),
                bid=price,
                ask=price,
                bid_qty=max(int(row["trade_volume"] or 0), 1),
                ask_qty=max(int(row["trade_volume"] or 0), 1),
                age_ms=0.0,
                receive_ns=int(row["last_receive_ts_ns"]),
                depth_enforced=False,
            )
        future_quote = quotes.get(future_meta.code)
        if future_quote is None:
            continue
        seconds_with_future_print += 1
        for delivery_month, chain in sorted(option_by_month.items()):
            candidates = _scan_chain(
                option_metas=chain,
                quotes=quotes,
                future_meta=future_meta,
                future_quote=future_quote,
                trading_date=trade_date,
                price_mode="midpoint",
                enforce_depth=False,
                objective="gross",
            )
            for variant_id in VARIANT_IDS:
                candidate = candidates.get(variant_id)
                if candidate is None:
                    continue
                print_volume_sufficient = all(
                    quotes[leg.code].bid_qty >= leg.quantity
                    for leg in candidate.legs
                )
                rows.append(
                    {
                        "local_second": _iso_taipei(second_ns),
                        "local_second_ns": second_ns,
                        "delivery_month": delivery_month,
                        "expiry": candidate.expiry,
                        "variant_id": variant_id,
                        "direction": candidate.direction,
                        "strikes_json": json.dumps(candidate.strikes),
                        "gross_print_edge_twd": candidate.gross_locked_edge_twd,
                        "after_fixed_fees_twd": (
                            candidate.gross_locked_edge_twd
                            - candidate.entry_fixed_fees_twd
                        ),
                        "after_entry_tax_twd": (
                            candidate.net_before_settlement_tax_twd
                        ),
                        "after_estimated_settlement_tax_twd": (
                            candidate.net_after_estimated_settlement_tax_twd
                        ),
                        "entry_fixed_fees_twd": candidate.entry_fixed_fees_twd,
                        "entry_transaction_tax_twd": (
                            candidate.entry_transaction_tax_twd
                        ),
                        "estimated_settlement_tax_twd": (
                            candidate.estimated_settlement_tax_twd
                        ),
                        "positive_gross_print_edge": (
                            candidate.gross_locked_edge_twd > 0.0
                        ),
                        "positive_after_all_modeled_costs": (
                            candidate.net_after_estimated_settlement_tax_twd > 0.0
                        ),
                        "print_volume_sufficient_for_package": (
                            print_volume_sufficient
                        ),
                        "positive_after_costs_and_print_volume": (
                            candidate.net_after_estimated_settlement_tax_twd > 0.0
                            and print_volume_sufficient
                        ),
                        "legs_json": json.dumps(
                            [
                                {
                                    "code": leg.code,
                                    "side": leg.side,
                                    "quantity": leg.quantity,
                                    "trade_price": leg.price,
                                    "observed_trade_volume": quotes[
                                        leg.code
                                    ].bid_qty,
                                }
                                for leg in candidate.legs
                            ],
                            separators=(",", ":"),
                        ),
                    }
                )
    if rows:
        observations = pl.DataFrame(rows, infer_schema_length=None).sort(
            ["delivery_month", "variant_id", "local_second_ns"]
        )
        summary = observations.group_by(
            ["delivery_month", "expiry", "variant_id"]
        ).agg(
            pl.len().alias("evaluable_same_second_prints"),
            pl.col("positive_gross_print_edge")
            .sum()
            .alias("positive_same_second_prints"),
            pl.col("gross_print_edge_twd").max().alias("max_gross_print_edge_twd"),
            pl.col("after_fixed_fees_twd")
            .max()
            .alias("max_after_fixed_fees_twd"),
            pl.col("after_entry_tax_twd").max().alias("max_after_entry_tax_twd"),
            pl.col("after_estimated_settlement_tax_twd")
            .max()
            .alias("max_after_estimated_settlement_tax_twd"),
            pl.col("positive_after_all_modeled_costs")
            .sum()
            .alias("positive_after_all_modeled_costs"),
            pl.col("print_volume_sufficient_for_package")
            .sum()
            .alias("print_volume_sufficient_packages"),
            pl.col("positive_after_costs_and_print_volume")
            .sum()
            .alias("positive_after_costs_and_print_volume"),
        )
        best = (
            observations.sort("gross_print_edge_twd", descending=True)
            .unique(["delivery_month", "variant_id"], keep="first")
            .select(
                "delivery_month",
                "variant_id",
                pl.col("local_second").alias("best_local_second"),
                pl.col("direction").alias("best_direction"),
                pl.col("strikes_json").alias("best_strikes_json"),
                pl.col("legs_json").alias("best_legs_json"),
            )
        )
        summary = summary.join(
            best, on=["delivery_month", "variant_id"], how="left"
        ).sort(["delivery_month", "variant_id"])
    else:
        observations = pl.DataFrame(
            schema={
                "local_second": pl.String,
                "local_second_ns": pl.Int64,
                "delivery_month": pl.String,
                "expiry": pl.Date,
                "variant_id": pl.String,
                "direction": pl.String,
                "strikes_json": pl.String,
                "gross_print_edge_twd": pl.Float64,
                "after_fixed_fees_twd": pl.Float64,
                "after_entry_tax_twd": pl.Float64,
                "after_estimated_settlement_tax_twd": pl.Float64,
                "entry_fixed_fees_twd": pl.Float64,
                "entry_transaction_tax_twd": pl.Float64,
                "estimated_settlement_tax_twd": pl.Float64,
                "positive_gross_print_edge": pl.Boolean,
                "positive_after_all_modeled_costs": pl.Boolean,
                "print_volume_sufficient_for_package": pl.Boolean,
                "positive_after_costs_and_print_volume": pl.Boolean,
                "legs_json": pl.String,
            }
        )
        summary = pl.DataFrame(
            schema={
                "delivery_month": pl.String,
                "expiry": pl.Date,
                "variant_id": pl.String,
                "evaluable_same_second_prints": pl.UInt32,
                "positive_same_second_prints": pl.UInt32,
                "max_gross_print_edge_twd": pl.Float64,
                "max_after_fixed_fees_twd": pl.Float64,
                "max_after_entry_tax_twd": pl.Float64,
                "max_after_estimated_settlement_tax_twd": pl.Float64,
                "positive_after_all_modeled_costs": pl.UInt32,
                "print_volume_sufficient_packages": pl.UInt32,
                "positive_after_costs_and_print_volume": pl.UInt32,
                "best_local_second": pl.String,
                "best_direction": pl.String,
                "best_strikes_json": pl.String,
                "best_legs_json": pl.String,
            }
        )
    quality = {
        "tick_part_count": len(parts),
        "valid_contract_second_print_rows": ticks.height,
        "local_seconds_with_any_valid_print": trade_seconds,
        "local_seconds_with_tx_print": seconds_with_future_print,
        "evaluable_method_seconds": observations.height,
        "interpretation": (
            "same local receive second and last print per contract; observation "
            "only, not a simultaneous or atomic multi-leg fill claim"
        ),
    }
    return observations, summary, quality


def analyze_capture_layers(
    *, capture_root: Path, trade_date: date, output_dir: Path
) -> dict[str, Any]:
    trade_date_text = trade_date.isoformat()
    manifests = read_capture_manifests(capture_root, trade_date_text)
    if not manifests:
        raise RuntimeError(f"no capture manifests for {trade_date_text}")
    capture_id = shared_capture_id(manifests)
    if capture_id is None:
        raise RuntimeError("layer analysis requires capture schema >= 3")
    if len(manifests) != 1 or any(
        str(item.get("status")) != "complete" for item in manifests
    ):
        raise RuntimeError("layer analysis requires one complete worker manifest")
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

    book_parts = select_capture_part_paths(
        capture_root=capture_root,
        kind="book_1s",
        trade_date=trade_date_text,
        manifests=manifests,
        verify_part_counts=True,
    )
    book_columns = (
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
        pl.scan_parquet(book_parts)
        .select(book_columns)
        .sort(["snapshot_ts_ns", "code"])
        .collect()
    )
    duplicate_rows = frame.select(
        pl.struct(["snapshot_ts_ns", "code"]).is_duplicated().sum()
    ).item()
    if duplicate_rows:
        raise RuntimeError(f"duplicate snapshot/code rows: {duplicate_rows}")

    rows: list[dict[str, Any]] = []
    snapshot_seconds = 0
    valid_book_rows = 0
    for (snapshot_raw,), group in frame.group_by(
        "snapshot_ts_ns", maintain_order=True
    ):
        snapshot_ns = int(snapshot_raw)
        snapshot_seconds += 1
        raw_rows = group.to_dicts()
        valid_rows = [row for row in raw_rows if _valid_quote(row)]
        valid_book_rows += len(valid_rows)
        quotes = {str(row["code"]): _quote(row) for row in valid_rows}
        future_quote = quotes.get(future_meta.code)
        if future_quote is None:
            continue
        for _, chain in sorted(option_by_month.items()):
            for layer in LAYERS:
                candidates = _scan_chain(
                    option_metas=chain,
                    quotes=quotes,
                    future_meta=future_meta,
                    future_quote=future_quote,
                    trading_date=trade_date,
                    price_mode=layer.price_mode,
                    enforce_depth=layer.enforce_depth,
                    objective=layer.objective,
                )
                for variant_id in VARIANT_IDS:
                    candidate = candidates.get(variant_id)
                    if candidate is not None:
                        rows.append(
                            _candidate_layer_row(
                                snapshot_ns=snapshot_ns,
                                candidate=candidate,
                                layer=layer,
                                quotes=quotes,
                            )
                        )
    if not rows:
        raise RuntimeError("no complete quote packages were evaluable")
    snapshot_best = pl.DataFrame(rows, infer_schema_length=None).sort(
        ["layer_order", "delivery_month", "variant_id", "snapshot_ts_ns"]
    )
    method_summary, overall_summary = _summarize_layers(snapshot_best)
    monotone_values = overall_summary["max_layer_value_twd"].to_list()
    monotone = all(
        float(current) <= float(previous) + 1e-9
        for previous, current in zip(monotone_values, monotone_values[1:])
    )
    if not monotone:
        raise RuntimeError(
            f"strict funnel is not monotone: {monotone_values}"
        )

    fully_modeled_best = (
        snapshot_best.filter(pl.col("layer_id") == LAYERS[-1].layer_id)
        .sort("layer_value_twd", descending=True)
        .row(0, named=True)
    )
    fixed_waterfall = _fixed_package_waterfall(
        snapshot_frame=frame,
        contracts=contracts,
        fully_modeled_best=fully_modeled_best,
        trading_date=trade_date,
    )
    print_observations, print_summary, print_quality = _trade_print_control(
        capture_root=capture_root,
        trade_date=trade_date,
        manifests=manifests,
        contracts=contracts,
        future_meta=future_meta,
        option_by_month=option_by_month,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "layer_snapshot_best.parquet": snapshot_best,
        "layer_method_summary.parquet": method_summary,
        "layer_overall_summary.parquet": overall_summary,
        "layer_fixed_package_waterfall.parquet": fixed_waterfall,
        "same_second_trade_observations.parquet": print_observations,
        "same_second_trade_summary.parquet": print_summary,
    }
    for name, table in outputs.items():
        parquet_path = output_dir / name
        _atomic_parquet(table, parquet_path)
        _write_csv(table, output_dir / name.replace(".parquet", ".csv"))

    payload = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "trade_date": trade_date_text,
        "capture_id": capture_id,
        "simulation_account_environment": bool(manifest.get("simulation", True)),
        "strict_funnel_monotone": monotone,
        "book_quality": {
            "book_part_count": len(book_parts),
            "snapshot_seconds": snapshot_seconds,
            "book_rows": frame.height,
            "valid_book_rows": valid_book_rows,
            "valid_book_fraction": (
                valid_book_rows / frame.height if frame.height else 0.0
            ),
            "duplicate_snapshot_code_rows": int(duplicate_rows),
        },
        "strict_layers": [
            {
                **row,
                "best_expiry": (
                    row["best_expiry"].isoformat()
                    if isinstance(row.get("best_expiry"), date)
                    else row.get("best_expiry")
                ),
            }
            for row in overall_summary.to_dicts()
        ],
        "fixed_package_waterfall": [
            {
                **row,
                "expiry": (
                    row["expiry"].isoformat()
                    if isinstance(row.get("expiry"), date)
                    else row.get("expiry")
                ),
            }
            for row in fixed_waterfall.to_dicts()
        ],
        "same_second_trade_print_control": {
            **print_quality,
            "summary_rows": print_summary.height,
            "positive_observation_rows": (
                int(print_observations["positive_gross_print_edge"].sum())
                if print_observations.height
                else 0
            ),
            "max_gross_print_edge_twd": (
                float(print_observations["gross_print_edge_twd"].max())
                if print_observations.height
                else None
            ),
            "max_after_all_modeled_costs_twd": (
                float(
                    print_observations[
                        "after_estimated_settlement_tax_twd"
                    ].max()
                )
                if print_observations.height
                else None
            ),
            "positive_after_all_modeled_cost_rows": (
                int(print_observations["positive_after_all_modeled_costs"].sum())
                if print_observations.height
                else 0
            ),
            "positive_after_costs_and_print_volume_rows": (
                int(
                    print_observations[
                        "positive_after_costs_and_print_volume"
                    ].sum()
                )
                if print_observations.height
                else 0
            ),
        },
        "interpretation_contract": {
            "strict_funnel": (
                "each layer adds exactly one constraint and re-optimizes the "
                "best time, strikes, direction, method, and expiry"
            ),
            "fixed_package": (
                "freezes the globally best L5 package so changes are additive"
            ),
            "same_second_prints": (
                "parallel observation control only; not executable fill evidence"
            ),
            "slippage_added": False,
            "latency_added": False,
            "passive_fill_assumed": False,
        },
    }
    _atomic_json(output_dir / "layer_summary.json", payload)
    receipt_paths = [
        output_dir / name for name in outputs
    ] + [
        output_dir / name.replace(".parquet", ".csv") for name in outputs
    ] + [output_dir / "layer_summary.json"]
    receipt = {
        "schema_version": 1,
        "status": "complete",
        "capture_id": capture_id,
        "outputs": {
            path.name: {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in receipt_paths
        },
    }
    _atomic_json(output_dir / "layer_receipt.json", receipt)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-root", type=Path, default=DEFAULT_CAPTURE_ROOT)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = analyze_capture_layers(
        capture_root=args.capture_root,
        trade_date=args.trade_date,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "output_dir": str(args.output_dir),
                "layers": len(result["strict_layers"]),
                "strict_funnel_monotone": result["strict_funnel_monotone"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
