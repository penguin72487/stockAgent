from __future__ import annotations

import argparse
from datetime import date, datetime, time
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.shioaji_capture_parts import (
    read_capture_manifests,
    select_capture_part_paths,
    shared_capture_id,
)


TAIPEI = ZoneInfo("Asia/Taipei")
NS_PER_SECOND = 1_000_000_000
DEFAULT_HORIZONS = (1, 5, 30, 60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a causal, one-row-per-symbol-second HFT feature/label dataset "
            "from Shioaji Tick, BidAsk, and one-second as-of book captures."
        )
    )
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=Path("data_tw_microstructure/captures"),
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("data_tw_microstructure/universe/top_200.csv"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_tw_microstructure/hft_dataset"),
    )
    parser.add_argument(
        "--trade-date",
        action="append",
        dest="trade_dates",
        help="YYYY-MM-DD; repeat for multiple dates. Default: all captured dates.",
    )
    parser.add_argument("--session-start", default="09:00:00")
    parser.add_argument("--session-end", default="13:30:00")
    parser.add_argument("--max-book-age-ms", type=float, default=5_000.0)
    parser.add_argument(
        "--horizons",
        default=",".join(str(value) for value in DEFAULT_HORIZONS),
        help="Comma-separated future-label horizons in seconds.",
    )
    return parser.parse_args()


def _parse_horizons(raw: str) -> tuple[int, ...]:
    horizons = tuple(sorted({int(value.strip()) for value in raw.split(",")}))
    if not horizons or any(value <= 0 for value in horizons):
        raise ValueError("horizons must contain positive integers")
    return horizons


def _session_ns(trade_date: date, raw_time: str) -> int:
    local = datetime.combine(trade_date, time.fromisoformat(raw_time), tzinfo=TAIPEI)
    return int(local.timestamp() * NS_PER_SECOND)


def _ceil_receive_second(expr: pl.Expr) -> pl.Expr:
    return ((expr + NS_PER_SECOND - 1) // NS_PER_SECOND) * NS_PER_SECOND


def _scan_partition(
    capture_root: Path,
    kind: str,
    trade_date: date,
    manifests: list[dict[str, Any]],
) -> pl.LazyFrame:
    paths = select_capture_part_paths(
        capture_root=capture_root,
        kind=kind,
        trade_date=trade_date.isoformat(),
        manifests=manifests,
    )
    return pl.scan_parquet([str(path) for path in paths], missing_columns="raise")


def _load_universe(path: Path) -> pl.LazyFrame:
    frame = pl.read_csv(
        path,
        schema_overrides={"symbol": pl.String},
        infer_schema_length=0,
    )
    required = {"symbol", "market_cap_rank", "name", "market", "source_date"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"universe is missing columns: {sorted(missing)}")
    return (
        frame.select(
            pl.col("symbol").alias("code"),
            pl.col("market_cap_rank").cast(pl.Int32, strict=True),
            pl.col("name"),
            pl.col("market"),
            pl.col("source_date").str.to_date(strict=True).alias("universe_as_of_date"),
        )
        .lazy()
    )


def _tick_features(ticks: pl.LazyFrame) -> pl.LazyFrame:
    value = pl.col("close") * pl.col("volume")
    return (
        ticks.filter(~pl.col("simtrade") & ~pl.col("intraday_odd"))
        .with_columns(
            _ceil_receive_second(pl.col("receive_ts_ns")).alias("snapshot_ts_ns")
        )
        .sort(["code", "receive_ts_ns", "event_seq"])
        .group_by("code", "snapshot_ts_ns")
        .agg(
            pl.len().cast(pl.Int32).alias("trade_count_1s"),
            pl.col("volume").sum().alias("trade_volume_lots_1s"),
            pl.col("amount").sum().alias("trade_amount_ntd_1s"),
            pl.when(pl.col("tick_type") == 1)
            .then(pl.col("volume"))
            .otherwise(0)
            .sum()
            .alias("ask_trade_volume_lots_1s"),
            pl.when(pl.col("tick_type") == 2)
            .then(pl.col("volume"))
            .otherwise(0)
            .sum()
            .alias("bid_trade_volume_lots_1s"),
            value.sum().alias("trade_price_volume_1s"),
            pl.col("close").last().alias("last_trade_price"),
            pl.col("exchange_ts_ns").max().alias("last_trade_exchange_ts_ns"),
            pl.col("receive_ts_ns").max().alias("last_trade_receive_ts_ns"),
        )
        .with_columns(
            pl.when(pl.col("trade_volume_lots_1s") > 0)
            .then(pl.col("trade_price_volume_1s") / pl.col("trade_volume_lots_1s"))
            .otherwise(None)
            .alias("trade_vwap_1s"),
            (
                pl.col("ask_trade_volume_lots_1s")
                - pl.col("bid_trade_volume_lots_1s")
            ).alias("signed_trade_volume_lots_1s"),
        )
        .with_columns(
            pl.when(pl.col("trade_volume_lots_1s") > 0)
            .then(
                pl.col("signed_trade_volume_lots_1s")
                / pl.col("trade_volume_lots_1s")
            )
            .otherwise(0.0)
            .alias("trade_imbalance_1s")
        )
        .drop("trade_price_volume_1s")
    )


def _book_event_features(events: pl.LazyFrame) -> pl.LazyFrame:
    bid_abs = pl.sum_horizontal(
        [pl.col(f"diff_bid_vol_{level}").abs() for level in range(1, 6)]
    )
    ask_abs = pl.sum_horizontal(
        [pl.col(f"diff_ask_vol_{level}").abs() for level in range(1, 6)]
    )
    return (
        events.filter(~pl.col("simtrade") & ~pl.col("intraday_odd"))
        .with_columns(
            _ceil_receive_second(pl.col("receive_ts_ns")).alias("snapshot_ts_ns")
        )
        .group_by("code", "snapshot_ts_ns")
        .agg(
            pl.len().cast(pl.Int32).alias("book_update_count_1s"),
            pl.col("diff_bid_vol_1").sum().alias("bid_depth_delta_l1_1s"),
            pl.col("diff_ask_vol_1").sum().alias("ask_depth_delta_l1_1s"),
            bid_abs.sum().alias("bid_depth_churn_l5_1s"),
            ask_abs.sum().alias("ask_depth_churn_l5_1s"),
            pl.col("receive_ts_ns").max().alias("last_book_event_receive_ts_ns"),
        )
        .with_columns(
            (
                pl.col("bid_depth_delta_l1_1s")
                - pl.col("ask_depth_delta_l1_1s")
            ).alias("net_depth_delta_l1_1s")
        )
    )


def build_hft_frame(
    *,
    books: pl.LazyFrame,
    ticks: pl.LazyFrame,
    book_events: pl.LazyFrame,
    universe: pl.LazyFrame,
    session_start_ns: int,
    session_end_ns: int,
    max_book_age_ms: float,
    horizons: Iterable[int],
) -> pl.LazyFrame:
    price_columns = [
        f"{side}_price_{level}"
        for side in ("bid", "ask")
        for level in range(1, 6)
    ]
    volume_columns = [
        f"{side}_volume_{level}"
        for side in ("bid", "ask")
        for level in range(1, 6)
    ]
    base = (
        books.filter(
            (pl.col("snapshot_ts_ns") >= session_start_ns)
            & (pl.col("snapshot_ts_ns") < session_end_ns)
        )
        .select(
            "snapshot_ts_ns",
            "exchange",
            "code",
            "trade_date",
            "book_exchange_ts_ns",
            "book_receive_ts_ns",
            "book_age_ms",
            "stale",
            "suspend",
            *price_columns,
            *volume_columns,
        )
        .join(_tick_features(ticks), on=["code", "snapshot_ts_ns"], how="left")
        .join(
            _book_event_features(book_events),
            on=["code", "snapshot_ts_ns"],
            how="left",
        )
        .join(universe, on="code", how="left", validate="m:1")
    )
    zero_columns = [
        "trade_count_1s",
        "trade_volume_lots_1s",
        "trade_amount_ntd_1s",
        "ask_trade_volume_lots_1s",
        "bid_trade_volume_lots_1s",
        "signed_trade_volume_lots_1s",
        "trade_imbalance_1s",
        "book_update_count_1s",
        "bid_depth_delta_l1_1s",
        "ask_depth_delta_l1_1s",
        "bid_depth_churn_l5_1s",
        "ask_depth_churn_l5_1s",
        "net_depth_delta_l1_1s",
    ]
    bid_depth_l5 = pl.sum_horizontal(
        [pl.col(f"bid_volume_{level}") for level in range(1, 6)]
    )
    ask_depth_l5 = pl.sum_horizontal(
        [pl.col(f"ask_volume_{level}") for level in range(1, 6)]
    )
    base = (
        base.with_columns([pl.col(name).fill_null(0) for name in zero_columns])
        .with_columns(
            ((pl.col("bid_price_1") + pl.col("ask_price_1")) / 2.0).alias(
                "mid_price"
            ),
            (pl.col("ask_price_1") - pl.col("bid_price_1")).alias("spread"),
            bid_depth_l5.alias("bid_depth_l5"),
            ask_depth_l5.alias("ask_depth_l5"),
            (
                ~pl.col("stale")
                & ~pl.col("suspend")
                & (pl.col("book_age_ms") <= max_book_age_ms)
                & (pl.col("book_receive_ts_ns") <= pl.col("snapshot_ts_ns"))
                & (pl.col("bid_price_1") > 0)
                & (pl.col("ask_price_1") >= pl.col("bid_price_1"))
                & (pl.col("bid_volume_1") >= 0)
                & (pl.col("ask_volume_1") >= 0)
            ).fill_null(False).alias("feature_valid"),
        )
        .with_columns(
            (10_000.0 * pl.col("spread") / pl.col("mid_price")).alias(
                "spread_bps"
            ),
            pl.when((pl.col("bid_volume_1") + pl.col("ask_volume_1")) > 0)
            .then(
                (
                    pl.col("ask_price_1") * pl.col("bid_volume_1")
                    + pl.col("bid_price_1") * pl.col("ask_volume_1")
                )
                / (pl.col("bid_volume_1") + pl.col("ask_volume_1"))
            )
            .otherwise(pl.col("mid_price"))
            .alias("microprice"),
            pl.when((pl.col("bid_volume_1") + pl.col("ask_volume_1")) > 0)
            .then(
                (pl.col("bid_volume_1") - pl.col("ask_volume_1"))
                / (pl.col("bid_volume_1") + pl.col("ask_volume_1"))
            )
            .otherwise(0.0)
            .alias("book_imbalance_l1"),
            pl.when((pl.col("bid_depth_l5") + pl.col("ask_depth_l5")) > 0)
            .then(
                (pl.col("bid_depth_l5") - pl.col("ask_depth_l5"))
                / (pl.col("bid_depth_l5") + pl.col("ask_depth_l5"))
            )
            .otherwise(0.0)
            .alias("book_imbalance_l5"),
            ((pl.col("snapshot_ts_ns") - session_start_ns) // NS_PER_SECOND)
            .cast(pl.Int32)
            .alias("seconds_from_open"),
            pl.from_epoch("snapshot_ts_ns", time_unit="ns")
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("Asia/Taipei")
            .alias("snapshot_datetime"),
        )
        .with_columns(
            (10_000.0 * (pl.col("microprice") / pl.col("mid_price") - 1.0)).alias(
                "microprice_edge_bps"
            )
        )
        .sort(["code", "snapshot_ts_ns"])
    )

    for lag in (1, 5, 30):
        prior_ts = pl.col("snapshot_ts_ns").shift(lag).over("code")
        prior_mid = pl.col("mid_price").shift(lag).over("code")
        prior_valid = pl.col("feature_valid").shift(lag).over("code")
        base = base.with_columns(
            pl.when(
                pl.col("feature_valid")
                & prior_valid
                & (pl.col("snapshot_ts_ns") - prior_ts == lag * NS_PER_SECOND)
                & (prior_mid > 0)
            )
            .then((pl.col("mid_price") / prior_mid).log())
            .otherwise(None)
            .alias(f"mid_log_return_{lag}s")
        )

    base = base.with_columns(
        pl.col("mid_log_return_1s")
        .rolling_std(window_size=30, min_samples=30)
        .over("code")
        .alias("realized_volatility_30s"),
        pl.col("mid_log_return_1s")
        .rolling_std(window_size=60, min_samples=60)
        .over("code")
        .alias("realized_volatility_60s"),
        pl.col("signed_trade_volume_lots_1s")
        .rolling_sum(window_size=30, min_samples=1)
        .over("code")
        .alias("signed_trade_volume_lots_30s"),
        pl.col("trade_count_1s")
        .rolling_sum(window_size=30, min_samples=1)
        .over("code")
        .alias("trade_count_30s"),
        pl.col("book_update_count_1s")
        .rolling_sum(window_size=30, min_samples=1)
        .over("code")
        .alias("book_update_count_30s"),
    )

    for horizon in horizons:
        future_ts = pl.col("snapshot_ts_ns").shift(-horizon).over("code")
        future_mid = pl.col("mid_price").shift(-horizon).over("code")
        future_bid = pl.col("bid_price_1").shift(-horizon).over("code")
        future_ask = pl.col("ask_price_1").shift(-horizon).over("code")
        future_valid = pl.col("feature_valid").shift(-horizon).over("code")
        valid_name = f"label_valid_{horizon}s"
        valid_expr = (
            pl.col("feature_valid")
            & future_valid
            & (future_ts - pl.col("snapshot_ts_ns") == horizon * NS_PER_SECOND)
            & (future_mid > 0)
        ).fill_null(False)
        base = base.with_columns(
            valid_expr.alias(valid_name),
            pl.when(valid_expr)
            .then(future_mid)
            .otherwise(None)
            .alias(f"future_mid_price_{horizon}s"),
        ).with_columns(
            pl.when(pl.col(valid_name))
            .then((pl.col(f"future_mid_price_{horizon}s") / pl.col("mid_price")).log())
            .otherwise(None)
            .alias(f"future_mid_log_return_{horizon}s"),
            pl.when(pl.col(valid_name) & (pl.col("ask_price_1") > 0))
            .then(10_000.0 * (future_bid / pl.col("ask_price_1") - 1.0))
            .otherwise(None)
            .alias(f"long_cross_spread_markout_bps_{horizon}s"),
            pl.when(pl.col(valid_name) & (future_ask > 0))
            .then(10_000.0 * (pl.col("bid_price_1") / future_ask - 1.0))
            .otherwise(None)
            .alias(f"short_cross_spread_markout_bps_{horizon}s"),
        )

    return base.sort(["snapshot_ts_ns", "market_cap_rank"])


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_date(
    *,
    capture_root: Path,
    universe_path: Path,
    output_root: Path,
    trade_date: date,
    session_start: str,
    session_end: str,
    max_book_age_ms: float,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    start_ns = _session_ns(trade_date, session_start)
    end_ns = _session_ns(trade_date, session_end)
    if start_ns >= end_ns:
        raise ValueError("session start must precede session end")
    manifests = read_capture_manifests(capture_root, trade_date.isoformat())
    if not manifests:
        raise RuntimeError(f"no capture manifests for {trade_date}")
    capture_id = shared_capture_id(manifests)
    frame = build_hft_frame(
        books=_scan_partition(capture_root, "book_1s", trade_date, manifests),
        ticks=_scan_partition(capture_root, "ticks", trade_date, manifests),
        book_events=_scan_partition(
            capture_root, "book_events", trade_date, manifests
        ),
        universe=_load_universe(universe_path),
        session_start_ns=start_ns,
        session_end_ns=end_ns,
        max_book_age_ms=max_book_age_ms,
        horizons=horizons,
    ).collect(engine="streaming")
    if frame.is_empty():
        raise RuntimeError(f"no in-session rows for {trade_date}")
    duplicate_keys = int(
        frame.group_by("snapshot_ts_ns", "code")
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    future_feature_events = int(
        frame.filter(
            (pl.col("book_receive_ts_ns") > pl.col("snapshot_ts_ns"))
            | (
                pl.col("last_trade_receive_ts_ns").is_not_null()
                & (pl.col("last_trade_receive_ts_ns") > pl.col("snapshot_ts_ns"))
            )
            | (
                pl.col("last_book_event_receive_ts_ns").is_not_null()
                & (pl.col("last_book_event_receive_ts_ns") > pl.col("snapshot_ts_ns"))
            )
        ).height
    )
    unknown_universe_rows = int(frame["market_cap_rank"].null_count())
    if duplicate_keys or future_feature_events or unknown_universe_rows:
        raise RuntimeError(
            "HFT dataset audit failed: "
            f"duplicate_keys={duplicate_keys} "
            f"future_feature_events={future_feature_events} "
            f"unknown_universe_rows={unknown_universe_rows}"
        )

    partition = output_root / f"trade_date={trade_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    output_path = partition / "data.parquet"
    temporary = partition / "data.parquet.tmp"
    frame.write_parquet(temporary, compression="zstd", statistics=True)
    os.replace(temporary, output_path)
    valid_rows = int(frame["feature_valid"].sum())
    labels = {
        f"{horizon}s": int(frame[f"label_valid_{horizon}s"].sum())
        for horizon in horizons
    }
    summary: dict[str, Any] = {
        "schema_version": 1,
        "status": "ok",
        "source": "shioaji_microstructure_v1",
        "grain": ["trade_date", "snapshot_ts_ns", "code"],
        "trade_date": trade_date.isoformat(),
        "capture_id": capture_id,
        "session": {"start": session_start, "end_exclusive": session_end},
        "max_book_age_ms": max_book_age_ms,
        "horizons_seconds": list(horizons),
        "rows": frame.height,
        "columns": frame.width,
        "symbols": frame["code"].n_unique(),
        "first_snapshot_ts_ns": int(frame["snapshot_ts_ns"].min()),
        "last_snapshot_ts_ns": int(frame["snapshot_ts_ns"].max()),
        "feature_valid_rows": valid_rows,
        "feature_valid_rate": valid_rows / frame.height,
        "stale_rows": int(frame["stale"].sum()),
        "label_valid_rows": labels,
        "duplicate_keys": duplicate_keys,
        "future_feature_events": future_feature_events,
        "unknown_universe_rows": unknown_universe_rows,
        "universe_sha256": _sha256(universe_path),
        "output": str(output_path),
        "output_sha256": _sha256(output_path),
        "label_semantics": {
            "future_mid_log_return": "continuous log return; exact future second only",
            "long_cross_spread_markout_bps": "buy current ask, mark/sell future bid; gross of fees",
            "short_cross_spread_markout_bps": "sell current bid, mark/cover future ask; gross of fees",
        },
        "causality": (
            "Tick and BidAsk events are assigned by receive time rounded up to the "
            "next snapshot boundary; all feature event receive timestamps are <= "
            "snapshot_ts_ns."
        ),
    }
    _atomic_json(partition / "summary.json", summary)
    return summary


def _discover_dates(capture_root: Path) -> list[date]:
    dates: list[date] = []
    for path in sorted((capture_root / "book_1s").glob("trade_date=*")):
        if path.is_dir():
            dates.append(date.fromisoformat(path.name.split("=", 1)[1]))
    return dates


def main() -> None:
    args = parse_args()
    horizons = _parse_horizons(args.horizons)
    dates = (
        [date.fromisoformat(value) for value in args.trade_dates]
        if args.trade_dates
        else _discover_dates(args.capture_root)
    )
    if not dates:
        raise RuntimeError("no captured trade dates found")
    summaries = []
    for trade_date in dates:
        summary = build_date(
            capture_root=args.capture_root,
            universe_path=args.universe,
            output_root=args.output_root,
            trade_date=trade_date,
            session_start=args.session_start,
            session_end=args.session_end,
            max_book_age_ms=args.max_book_age_ms,
            horizons=horizons,
        )
        summaries.append(summary)
        print(
            f"[shioaji-hft] date={trade_date} rows={summary['rows']} "
            f"valid={summary['feature_valid_rows']} symbols={summary['symbols']} "
            f"output={summary['output']}",
            flush=True,
        )
    _atomic_json(
        args.output_root / "manifest.json",
        {
            "schema_version": 1,
            "status": "ok",
            "dates": [summary["trade_date"] for summary in summaries],
            "partitions": summaries,
        },
    )


if __name__ == "__main__":
    main()
