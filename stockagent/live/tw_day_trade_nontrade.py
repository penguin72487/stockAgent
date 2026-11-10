"""Pinned last-observation evidence for explicitly non-execution paper settlement.

The provider object is an observed sample, not proof of complete exchange
history.  Nothing in this module submits an order or asserts a market fill.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
import hashlib
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl


CONTRACT = "last_observed_trade_nonexecution_book_settlement_v1"
TAIPEI = ZoneInfo("Asia/Taipei")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def select_last_observations(frame: pl.DataFrame, *, day: date, symbols: set[str],
                             official_closes: dict[str, float] | None = None) -> dict[str, dict]:
    """Find unambiguous latest positive-volume observed ticks through 13:30.

    Equal latest timestamps with different prices are ambiguous, even if their
    physical order differs. A separately verified official close may select a
    matching price, but never establishes the order of the tied observations.
    Absence is an error rather than a synthetic price.
    """
    required = {"date", "Time", "stock_id", "deal_price", "volume"}
    if not required <= set(frame.columns):
        raise ValueError("tick source missing required fields")
    if not symbols or any(not isinstance(s, str) or not s for s in symbols):
        raise ValueError("settlement symbols must be nonempty")
    selected = (frame.with_row_index("source_row_index")
        .filter(pl.col("stock_id").is_in(sorted(symbols)))
        .with_columns(pl.col("Time").str.strptime(pl.Time, format="%H:%M:%S%.f", strict=False).alias("clock")))
    if selected.filter((pl.col("date") != str(day)) | pl.col("clock").is_null()).height:
        raise ValueError("tick date or observation clock invalid")
    selected = selected.filter(
        (pl.col("clock") >= time(9, 0)) & (pl.col("clock") <= time(13, 30))
        & (pl.col("volume") > 0) & (pl.col("deal_price") > 0)
        & pl.col("deal_price").is_finite())
    result = {}
    for symbol in sorted(symbols):
        rows = selected.filter(pl.col("stock_id") == symbol)
        if not rows.height:
            raise ValueError(f"no positive observed tick through 13:30: {symbol}")
        latest = rows["clock"].max()
        matching = rows.filter(pl.col("clock") == latest)
        tie = matching["deal_price"].n_unique() != 1
        if tie:
            official = (official_closes or {}).get(symbol)
            if official is None or not math.isfinite(float(official)) or float(official) <= 0:
                raise ValueError(f"ambiguous latest observed tick: {symbol}")
            matching = matching.filter((pl.col("deal_price") - float(official)).abs() <= 1e-9)
            if not matching.height:
                raise ValueError(f"official close does not resolve observed tick tie: {symbol}")
        row = matching.sort("source_row_index").row(-1, named=True)
        price = float(row["deal_price"])
        if not math.isfinite(price):
            raise ValueError(f"invalid observed price: {symbol}")
        observed_at = datetime.combine(day, latest, tzinfo=TAIPEI)
        result[symbol] = {
            "symbol": symbol, "reference_price": price,
            "reference_observed_at": observed_at.isoformat(timespec="microseconds"),
            "source_row_index": int(row["source_row_index"]),
            "source_volume_raw": int(row["volume"]),
            "same_clock_price_tie_resolved_by_official_close": tie,
        }
    return result


def load_pinned_finmind_ticks(receipt_path: Path, *, day: date, symbols: set[str],
                              official_closes: dict[str, float] | None = None) -> tuple[dict[str, dict], list[dict]]:
    """Verify native receipt and bytes; return the source's last observations."""
    receipt_path = receipt_path.resolve(strict=True)
    with receipt_path.open("r", encoding="utf-8") as stream:
        receipt = json.load(stream)
    if (receipt.get("schema_version") != 1 or receipt.get("dataset") != "TaiwanStockPriceTick"
            or receipt.get("status") != "complete" or receipt.get("partition") != str(day)
            or receipt.get("source_bytes_preserved") is not True
            or not isinstance(receipt.get("sha256"), str)):
        raise ValueError("FinMind tick receipt identity invalid")
    # Native receipt layout: complement/receipts/TaiwanStockPriceTick/all/day.json.
    if receipt_path.parent.name != "all" or receipt_path.parent.parent.name != "TaiwanStockPriceTick":
        raise ValueError("unexpected FinMind tick receipt namespace")
    root = receipt_path.parents[3]
    path = (root / str(receipt.get("parquet_path") or "")).resolve(strict=True)
    if not path.is_relative_to(root) or path.suffix != ".parquet":
        raise ValueError("tick parquet escapes native source namespace")
    if path.stat().st_size != receipt.get("parquet_size_bytes") or _sha256(path) != receipt["sha256"]:
        raise ValueError("FinMind tick payload differs from receipt")
    from downloader.finmind_storage_objects import validate_parquet
    validation = validate_parquet(path, "TaiwanStockPriceTick", str(day))
    if validation["rows"] != receipt.get("rows"):
        raise ValueError("FinMind tick row count differs from receipt")
    frame = pl.read_parquet(path, columns=["date", "Time", "stock_id", "deal_price", "volume"])
    observations = select_last_observations(frame, day=day, symbols=symbols, official_closes=official_closes)
    pins = [{"path": str(receipt_path), "sha256": _sha256(receipt_path)},
            {"path": str(path), "sha256": receipt["sha256"]}]
    for row in observations.values():
        row.update({"source": "finmind_TaiwanStockPriceTick",
                    "source_sha256": receipt["sha256"],
                    "source_path": str(path),
                    "receipt_path": str(receipt_path), "receipt_sha256": pins[0]["sha256"],
                    "provider_object_complete": False,
                    "source_clock_basis": "source_observation_not_own_execution"})
    return observations, pins


def select_pinned_last_minute_observations(*, day: date, symbols: set[str],
                                           bars: dict, coverage: dict) -> tuple[dict, list[dict]]:
    """Select a source-verified last *observed minute*, not an exact tick.

    Missing minutes do not establish zero trading. The right label bounds the
    trade's minute; it must not be described as the actual trade timestamp.
    Only a unique source row with explicit shares and an unchanged pinned file
    is accepted. Unresolved schemas or changed sources fail closed.
    """
    if not symbols or any(not isinstance(symbol, str) or not symbol for symbol in symbols):
        raise ValueError("settlement symbols must be nonempty")
    observations, pins = {}, {}
    for symbol in sorted(symbols):
        candidates = []
        for key, bar in (bars.get(symbol) or {}).items():
            stamp = datetime.fromisoformat(str(key))
            if (stamp.utcoffset() != timedelta(hours=8) or stamp.date() != day
                    or not time(9, 1) <= stamp.time() <= time(13, 30)):
                continue
            try:
                price, volume = float(bar["close"]), float(bar["volume_shares"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(price) and price > 0 and math.isfinite(volume) and volume > 0 and volume.is_integer():
                candidates.append((stamp, price, int(volume)))
        if not candidates:
            raise ValueError(f"no verified positive-volume minute observation: {day}/{symbol}")
        stamp, price, volume = max(candidates, key=lambda item: item[0])
        sources = [row for row in coverage.get("source_files", []) if symbol in row.get("symbols", [])]
        if len(sources) != 1 or not isinstance(sources[0].get("sha256"), str):
            raise ValueError(f"minute source ownership is ambiguous: {day}/{symbol}")
        source = sources[0]
        path = Path(source["path"]).resolve(strict=True)
        before = path.stat()
        if (before.st_size != source.get("size_bytes") or _sha256(path) != source["sha256"]):
            raise ValueError(f"minute source differs from pinned evidence: {day}/{symbol}")
        lazy = pl.scan_parquet(path)
        schema = lazy.collect_schema()
        if not {"symbol", "ts", "Close", "volume_shares"}.issubset(schema.names()):
            raise ValueError(f"minute source lacks explicit symbol, price or share volume: {day}/{symbol}")
        # The canonical retained source uses Taiwan wall time for naive ts.
        # Do not interpret it as UTC or fabricate an intra-minute timestamp.
        raw_ts = stamp.replace(tzinfo=None)
        rows = (lazy.filter((pl.col("symbol").cast(pl.String) == symbol)
                            & (pl.col("ts") == raw_ts))
                .select("Close", "volume_shares").collect())
        after = path.stat()
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                or _sha256(path) != source["sha256"]):
            raise ValueError(f"minute source changed while verifying: {day}/{symbol}")
        if rows.height != 1:
            raise ValueError(f"last minute source key is not unique: {day}/{symbol}")
        row = rows.row(0, named=True)
        try:
            observed_price, observed_volume = float(row["Close"]), float(row["volume_shares"])
        except (TypeError, ValueError):
            raise ValueError(f"last minute source row is invalid: {day}/{symbol}") from None
        if (not math.isfinite(observed_price) or not math.isfinite(observed_volume)
                or not math.isclose(observed_price, price, rel_tol=0.0, abs_tol=1e-9)
                or observed_volume != volume):
            raise ValueError(f"last minute source row differs from replay: {day}/{symbol}")
        pin = {"path": str(path), "sha256": source["sha256"]}
        pins[str(path)] = pin
        observations[symbol] = {
            "symbol": symbol, "reference_price": price,
            "reference_observed_at": stamp.isoformat(timespec="seconds"),
            "reference_interval_start_exclusive": (stamp - timedelta(minutes=1)).isoformat(timespec="seconds"),
            "reference_interval_end_inclusive": stamp.isoformat(timespec="seconds"),
            "exact_trade_time_known": False, "source_volume_shares": volume,
            "source": "retained_right_labelled_1m_ohlcv",
            "source_path": str(path), "source_sha256": source["sha256"],
            "provider_object_complete": False,
            "source_clock_basis": "minute_interval_end_not_own_execution_or_exact_trade_time",
        }
    return observations, list(pins.values())


def apply_nontrade_observation(engine, mode: dict, position: dict, *, day: date,
                               reference: dict, official_evidence: dict,
                               research_close_expense: dict | None = None) -> dict:
    """Apply a verified non-execution assumption without creating fill rows.

    Source selection and any model-specific expense verification belong to
    the caller. For a positive research short cost, a verified zero-cost
    expense is mandatory here; nonzero costs require their own audit contract.
    """
    at = datetime.combine(day, time(13, 30), tzinfo=TAIPEI)
    symbol = str(position.get("symbol") or "")
    if (not symbol or reference.get("symbol") != symbol
            or official_evidence.get("session_date") != str(day)
            or official_evidence.get("symbol") != symbol
            or official_evidence.get("price_basis") != "official_session_close"
            or official_evidence.get("source") not in {"twse_official_daily_close", "tpex_official_daily_close"}):
        raise ValueError("nontrade settlement source identity mismatch")
    path = Path(str(official_evidence.get("source_path") or ""))
    if not path.is_file() or _sha256(path) != official_evidence.get("source_sha256"):
        raise ValueError("nontrade official close source changed")
    daily_close_reference = reference.get("source_clock_basis") == "official_daily_close_last_trade_time_unknown"
    stamp = None if daily_close_reference else datetime.fromisoformat(str(reference.get("reference_observed_at") or ""))
    if daily_close_reference and not (
            reference.get("reference_observed_at") is None
            and reference.get("exact_trade_time_known") is False
            and reference.get("source_path") == official_evidence.get("source_path")
            and reference.get("source_sha256") == official_evidence.get("source_sha256")
            and reference.get("reference_price") == official_evidence.get("price")):
        raise ValueError("invalid official daily close reference disclosure")
    entry = datetime.fromisoformat(str(position.get("entry_at") or ""))
    try:
        price = float(reference["reference_price"])
    except (KeyError, TypeError, ValueError):
        raise ValueError("invalid nontrade reference price") from None
    quantity = abs(int(position.get("signed_shares") or 0))
    if (not math.isfinite(price) or price <= 0 or quantity <= 0
            or entry.utcoffset() != timedelta(hours=8) or entry.date() != day
            or (stamp is not None and (stamp.utcoffset() != timedelta(hours=8)
                                       or stamp.date() != day or stamp > at))):
        raise ValueError("invalid nontrade observation, inventory or clock")
    if float(mode.get("research_short_limit_up_close_penalty_rate") or 0) and int(position["signed_shares"]) < 0:
        cost = (research_close_expense or {}).get("cost") or {}
        if cost.get("amount_twd") != 0:
            raise ValueError("nontrade research short cost needs independently verified zero-cost evidence")
    actual_exit_fields = ("exit_at", "exit_quote_at", "exit_price", "last_exit_at", "last_exit_quote_at",
                          "last_exit_price", "last_exit_quantity", "exit_reason")
    previous = {field: (field in position, position.get(field)) for field in actual_exit_fields}
    order_rows, fill_rows = [], []
    engine._book_position_close(position, mode, price=price, quote={}, now=at,
        reason="nontrade_last_observed_accounting_settlement", order_type="NONTRADE_ACCOUNTING",
        quantity=quantity, ledger_rows=(order_rows, fill_rows))
    if len(order_rows) != 1 or len(fill_rows) != 1:
        raise RuntimeError("nontrade preview did not produce exactly one accounting reduction")
    calculated = fill_rows[0]
    for field, (present, value) in previous.items():
        if present:
            position[field] = value
        else:
            position.pop(field, None)
    details = {"contract": CONTRACT, "accounting_at": at.isoformat(), "reference": reference,
        "official_close_evidence": official_evidence,
        "reference_before_simulated_entry": stamp < entry if stamp is not None else None,
        "quantity": quantity, "assumed_exit_fee_and_tax_twd": calculated["fee_and_tax_twd"],
        "assumed_commission_rebate_twd": calculated["commission_rebate_accrued_twd"],
        "gross_pnl_twd": calculated["gross_pnl_twd"], "net_pnl_twd": calculated["net_pnl_twd"],
        "entry_fee_allocated_twd": calculated["entry_fee_allocated_twd"],
        "simulation_only": True, "exchange_fill": False, "broker_fill": False,
        "simulated_order": False, "simulated_fill": False, "source_completeness_verified": False}
    if research_close_expense is not None:
        details["research_close_expense"] = research_close_expense
    position["nontrade_settlement"] = details
    position["status"] = "nontrade_book_settled"
    position["closing_auction_order_status"] = "not_filled_nontrade_book_settled"
    mode["execution_evidence_complete"] = False
    mode["cumulative_nontrade_settlement_net_pnl_twd"] = (
        float(mode.get("cumulative_nontrade_settlement_net_pnl_twd") or 0) + float(details["net_pnl_twd"]))
    mode["nontrade_settlement_count"] = int(mode.get("nontrade_settlement_count") or 0) + 1
    mode["nontrade_settlement_shares"] = int(mode.get("nontrade_settlement_shares") or 0) + quantity
    engine._event("nontrade_last_observed_accounting_settlement", recorded_at=datetime.now(TAIPEI),
                  market=mode["market"], session_date=str(day), position_id=position["position_id"],
                  symbol=symbol, **details)
    return details
