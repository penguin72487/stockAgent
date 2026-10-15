"""Normalize TW stock/index minutes without guessing volume or bar clocks."""
from __future__ import annotations

import polars as pl

from downloader.stock_volume_units import with_stock_share_volume
from downloader.finmind_volume_units import (
    FINMIND_CASH_INTRADAY_SHARE_MULTIPLIERS, FINMIND_CASH_INTRADAY_UNIT_SOURCE,
)


CONTRACT = "tw_raw_minute_provider_union_v2_dated_units"
KEYS = ["asset_class", "symbol", "ts", "phase"]
FIELDS = ["Open", "High", "Low", "Close", "volume_shares", "Amount"]
SEMANTICS = {
    "contract": CONTRACT, "grain": "one_minute_or_dated_auction",
    "timezone": "Asia/Taipei", "price": "unadjusted_native_quote_currency",
    "quantity": "shares_for_equity_null_for_index_or_unverified_unit",
    "clock": "completed_right_label_regular_and_actual_auction_timestamp",
    "mechanisms": "regular_closing_delayed_closing_afterhours_kept_distinct",
    "vintage": "current_provider_revision_not_historical_PIT",
}


def _reason(frame: pl.DataFrame, *, native_volume: pl.Expr) -> pl.DataFrame:
    prices = [pl.col(c) for c in ("Open", "High", "Low", "Close")]
    bad_prices = pl.any_horizontal(*[c.is_null() | ~c.is_finite() | (c <= 0) for c in prices])
    bad_range = (pl.col("High") < pl.max_horizontal(prices)) | (pl.col("Low") > pl.min_horizontal(prices))
    bad_quantity = native_volume.is_null() | ~native_volume.is_finite() | (native_volume < 0)
    bad_clock = pl.col("ts").is_null() | (pl.col("ts").dt.second() != 0) | (pl.col("ts").dt.nanosecond() != 0)
    bad_shares = pl.col("volume_shares").is_not_null() & (
        ~pl.col("volume_shares").is_finite() | (pl.col("volume_shares") < 0)
        | ((pl.col("volume_shares") - pl.col("volume_shares").round(0)).abs() > 1e-6))
    bad_amount = pl.col("Amount").is_not_null() & (~pl.col("Amount").is_finite() | (pl.col("Amount") < 0))
    return frame.with_columns(pl.when(bad_clock).then(pl.lit("invalid_native_clock"))
        .when(bad_prices | bad_range).then(pl.lit("invalid_ohlc"))
        .when(bad_quantity).then(pl.lit("invalid_native_quantity"))
        .when(bad_shares).then(pl.lit("invalid_whole_share_quantity"))
        .when(bad_amount).then(pl.lit("invalid_native_amount"))
        .otherwise(pl.lit("")).alias("_invalid_reason"))


def normalize_shioaji(frame: pl.DataFrame) -> pl.DataFrame:
    canonical = with_stock_share_volume(frame, tolerance=0.01).with_columns(
        pl.lit("equity").alias("asset_class"), pl.col("ts").cast(pl.Datetime("ns")),
        pl.when((pl.col("ts").dt.hour() == 13) & (pl.col("ts").dt.minute() == 30))
        .then(pl.lit("closing_auction")).otherwise(pl.lit("regular")).alias("phase"))
    return _reason(canonical, native_volume=pl.col("Volume")).with_columns(
        *[pl.col(c).cast(pl.Float64) for c in FIELDS]).select(KEYS + FIELDS + ["_invalid_reason"])


def normalize_finmind(frame: pl.DataFrame, *, share_proofs: pl.DataFrame | None = None) -> pl.DataFrame:
    stamp = pl.concat_str(pl.col("date"), pl.lit(" "), pl.col("minute")).str.to_datetime(time_unit="ns")
    # Polars hour/minute are Int8. Widen BEFORE arithmetic: 13*60 otherwise
    # wraps and misclassifies auctions, so equivalent rows never share a key.
    offset = stamp.dt.hour().cast(pl.Int16) * 60 + stamp.dt.minute().cast(pl.Int16)
    auction = offset.is_in([810, 813, 870])
    canonical = frame.with_columns(
        pl.when(pl.col("stock_id") == "TAIEX").then(pl.lit("index")).otherwise(pl.lit("equity")).alias("asset_class"),
        pl.col("stock_id").alias("symbol"),
        pl.when(auction).then(stamp).otherwise(stamp + pl.duration(minutes=1)).cast(pl.Datetime("ns")).alias("ts"),
        pl.when(offset == 810).then(pl.lit("closing_auction"))
        .when(offset == 813).then(pl.lit("delayed_closing_auction"))
        .when(offset == 870).then(pl.lit("afterhours_fixed_price"))
        .when(offset.is_between(540, 809)).then(pl.lit("regular"))
        .otherwise(pl.lit("outside_regular_session")).alias("phase"),
        *[pl.col(c).cast(pl.Float64).alias(c.title()) for c in ("open", "high", "low", "close")],
        pl.lit(None, dtype=pl.Float64).alias("Amount"))
    if share_proofs is not None:
        if share_proofs.select(pl.struct("symbol", "date").is_duplicated().any()).item():
            raise ValueError("ambiguous dated FinMind unit proof")
        if share_proofs.select((~pl.col("multiplier").is_in([1., 1000.])
                              | pl.col("multiplier").is_null()).any()).item():
            raise ValueError("undocumented FinMind unit multiplier")
        canonical = canonical.join(share_proofs.select("symbol", "date", "multiplier"),
                                   on=["symbol", "date"], how="left", validate="m:1")
        canonical = canonical.with_columns(pl.when(pl.col("asset_class") == "equity")
            .then(pl.col("volume") * pl.col("multiplier")).otherwise(None)
            .cast(pl.Float64).alias("volume_shares"))
    else:
        canonical = canonical.with_columns(pl.lit(None, dtype=pl.Float64).alias("volume_shares"))
    canonical = _reason(canonical, native_volume=pl.col("volume")).with_columns(
        pl.when((pl.col("asset_class") == "index") & (pl.col("volume") != 0))
        .then(pl.lit("index_quantity_not_structural_zero"))
        .when(pl.col("symbol") == "001").then(pl.lit("retired_unverified_index_alias"))
        .otherwise(pl.col("_invalid_reason")).alias("_invalid_reason"))
    return canonical.select(KEYS + FIELDS + ["_invalid_reason"])


def same_day_share_proofs(raw_finmind: pl.DataFrame, shioaji: pl.DataFrame,
                          *, min_matching_bars: int = 3) -> pl.DataFrame:
    """Verify a documented {1,1000} multiplier against same-day native evidence.

    This is not a fitted scale or a current-market label projected backwards.
    Never extend a unit proof to another symbol/day or accept disagreeing bars.
    """
    if min_matching_bars < 1:
        raise ValueError("unit evidence needs positive matching bars")
    # Normalization without unit proofs preserves row order and cardinality.
    # Carry native quantity positionally before checking key uniqueness: never
    # allocate a many-to-many join just to rediscover conflicting duplicates.
    f = normalize_finmind(raw_finmind).select(KEYS + FIELDS[:4] + ["_invalid_reason"]).with_columns(
        raw_finmind["date"], raw_finmind["volume"])
    # Duplicates must not create extra unit votes or a multiplicative join.
    f = f.filter(~pl.struct(KEYS).is_duplicated()).filter(pl.col("_invalid_reason") == "")
    s = shioaji.filter(~pl.struct(KEYS).is_duplicated()).filter(pl.col("_invalid_reason") == "")
    joined = f.join(s.select(KEYS + [pl.col(c).alias("peer_" + c) for c in FIELDS[:5]]),
                    on=KEYS, how="inner", validate="1:1")
    agrees = pl.all_horizontal(*[(pl.col(c) - pl.col("peer_" + c)).abs() <= 1e-8 for c in FIELDS[:4]])
    joined = joined.filter(agrees & (pl.col("volume") > 0) & (pl.col("peer_volume_shares") > 0))
    joined = joined.with_columns((pl.col("peer_volume_shares") / pl.col("volume")).alias("multiplier"))
    proof = joined.group_by("symbol", "date").agg(pl.col("multiplier").n_unique().alias("_scales"),
        pl.col("multiplier").first(), pl.len().alias("matching_bars"))
    return proof.filter((pl.col("_scales") == 1) & pl.col("multiplier").is_in([1., 1000.])
        & (pl.col("matching_bars") >= min_matching_bars)).drop("_scales")


def dated_market_share_proofs(evidence: pl.DataFrame) -> pl.DataFrame:
    """Apply the provider's documented unit only to independently dated venues.

    Callers verify the official raw response, date and SHA before supplying
    evidence. No current-market backcast, price fitting or cross-day carry.
    """
    if evidence.select(pl.any_horizontal(*[pl.col(c).is_null() | (pl.col(c) == "")
            for c in ("symbol", "date", "venue", "source_ref")]).any()).item():
        raise ValueError("missing dated FinMind market unit evidence")
    if evidence.select((~pl.col("venue").is_in(list(FINMIND_CASH_INTRADAY_SHARE_MULTIPLIERS))).any()).item():
        raise ValueError("undocumented FinMind market unit")
    return evidence.select("symbol", "date", "venue", "source_ref").unique().with_columns(
        pl.col("venue").replace_strict(FINMIND_CASH_INTRADAY_SHARE_MULTIPLIERS,
                                       return_dtype=pl.Float64).alias("multiplier"),
        pl.lit(0, dtype=pl.UInt32).alias("matching_bars"),
        pl.lit("documented_unit_and_dated_official_venue").alias("method"),
        pl.lit(FINMIND_CASH_INTRADAY_UNIT_SOURCE).alias("unit_source"))


def resolve_share_proofs(peer: pl.DataFrame, dated: pl.DataFrame,
                         *, peer_ref: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Independent proof agreement corroborates; differing units remain unknown."""
    columns = ["symbol", "date", "multiplier", "matching_bars", "method", "venue", "source_ref", "unit_source"]
    p = peer.with_columns(
        pl.lit("same_day_amount_ohlc_peer").alias("method"),
        pl.lit("").alias("venue"), pl.lit(peer_ref).alias("source_ref"),
        pl.lit(FINMIND_CASH_INTRADAY_UNIT_SOURCE).alias("unit_source"))
    all_proofs = pl.concat([dated.select(columns), p.select(columns)], how="vertical_relaxed")
    summary = all_proofs.group_by("symbol", "date").agg(
        pl.col("multiplier").n_unique().alias("_scales"), pl.col("multiplier").first(),
        pl.col("matching_bars").sum(),
        *[pl.col(c).filter(pl.col(c) != "").unique().sort().str.join("|")
          for c in ("method", "venue", "source_ref", "unit_source")])
    conflicts = all_proofs.join(summary.filter(pl.col("_scales") != 1).select("symbol", "date"),
                               on=["symbol", "date"], how="inner", validate="m:1")
    return summary.filter(pl.col("_scales") == 1).drop("_scales"), conflicts
