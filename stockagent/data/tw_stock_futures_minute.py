"""Physical-contract minute facts for one 08:45 daily futures decision.

The dense tape is executor-only. Bars are right labelled: 08:46 covers
[08:45,08:46). A 13:20 limit can use only subsequent bars; replacing it at
13:24 first consumes the 13:25 bar. No stock auction or daily-close fallback.
"""
from __future__ import annotations

import json
from pathlib import Path
import shlex

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_stock_futures_quarantine import (
    CONTRACT_DAY_QUARANTINE_VERSION, CONTRACT_DAY_QUARANTINE_POLICY,
    normalize_contract_days, exclude_contract_days, validate_contract_day_quarantine,
)

MINUTE_MODE = "tw_stock_futures_day_trade_0845_minute"
MINUTE_DATASET = "taifex_stock_futures_minute_v1"
MINUTE_CONTRACT_VERSION = 1
EVENT_MINUTES = (526, 800, *range(801, 811))  # 08:46, 13:20..13:30
BAR_FIELDS = ("vwap", "high", "low", "close", "capacity")
TAPE_CHANNELS = (
    "multiplier", "fee_per_side", "tax_rate",
    *(f"{minute // 60:02d}{minute % 60:02d}_{field}"
      for minute in EVENT_MINUTES for field in BAR_FIELDS),
)
TAPE_FIELDS = len(TAPE_CHANNELS)
HYBRID_CONTRACT_VERSION = 2
HYBRID_TAPE_CHANNELS = (*TAPE_CHANNELS, "daily_open_close_regime", "daily_open", "daily_close", "daily_capacity")
HYBRID_TAPE_FIELDS = len(HYBRID_TAPE_CHANNELS)


def build_futures_minute_bars(transactions: pl.DataFrame) -> pl.DataFrame:
    """Aggregate matched outright prints, preserving physical identity."""
    return (
        transactions.filter(
            (pl.col("session") == "day")
            & (pl.col("event_date") == pl.col("trading_date"))
            & ~pl.col("delivery_month_week").str.contains("/", literal=True)
            & pl.col("price").is_finite() & (pl.col("price") > 0)
            & pl.col("matched_quantity").is_finite()
            & (pl.col("matched_quantity") > 0)
        )
        .with_columns(
            pl.col("event_time").cast(pl.Int32).alias("hhmmss"),
            pl.concat_str("product", pl.lit(":"), "delivery_month_week")
            .alias("physical_contract"),
        )
        .with_columns(
            ((pl.col("hhmmss") // 10000) * 60
             + (pl.col("hhmmss") // 100) % 100 + 1).alias("minute")
        )
        .filter(pl.col("minute").is_in(EVENT_MINUTES))
        .sort("event_ts", "source_row_number")
        .group_by("trading_date", "physical_contract", "minute", maintain_order=True)
        .agg(
            ((pl.col("price") * pl.col("matched_quantity")).sum()
             / pl.col("matched_quantity").sum()).alias("vwap"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("matched_quantity").sum().alias("volume"),
            pl.col("source_sha256").first().alias("source_file_sha256"),
        )
        .rename({"trading_date": "date"})
        .sort("date", "physical_contract", "minute")
    )


def validate_futures_minute_data(
    path: str | Path, *, daily_sha256: str, dates: np.ndarray | None = None,
    daily_proxy_before: str | None = None,
    participation: float | None = None,
    capacity_rounding: str = "floor",
    quarantine_dates: tuple[str, ...] | list[str] = (),
    quarantine_contract_days: tuple[dict, ...] | list[dict] = (),
) -> tuple[pl.DataFrame, dict]:
    """Validate source facts without allocating a dense execution tensor."""
    if capacity_rounding not in {"floor", "ceil"}:
        raise ValueError("futures minute capacity rounding must be floor or ceil")
    path = Path(path)
    manifest_path = path.parent / "manifest.json"
    if not path.is_file() or not manifest_path.is_file():
        missing = [str(p) for p in (path, manifest_path) if not p.is_file()]
        raise FileNotFoundError(
            f"08:45 futures minute data/receipt missing: {', '.join(missing)}. "
            "Use receipt-backed one-minute futures KBars with "
            "scripts/build_tw_stock_futures_0900_entries.py --execution-policy scheduled_0846 --minute-root PATH. "
            "Daily OHLC cannot reconstruct minute prices or capacity; "
            "see docs/tw_stock_futures_day_trade_minute.md."
        )
    receipt = json.loads(manifest_path.read_text())
    contract_days = normalize_contract_days(quarantine_contract_days)
    if normalize_contract_days(receipt.get('quarantined_contract_days', [])) != contract_days:
        raise ValueError('explicit contract-day quarantine differs from prepared source contract')
    if contract_days and daily_proxy_before is None:
        raise ValueError('contract-day quarantine requires historical minute evidence')
    if daily_proxy_before is not None:
        from stockagent.data.tw_stock_futures_history import HISTORY_DATASET, HISTORY_SOURCE, ACCEPTED
        from stockagent.data.tw_stock_futures_repair import REPAIR_SOURCE, NO_TRADE, NO_CAPACITY
        if (receipt.get("dataset") != HISTORY_DATASET or receipt.get("contract_version") != HYBRID_CONTRACT_VERSION
                or receipt.get("source_kind") not in {HISTORY_SOURCE, REPAIR_SOURCE} or receipt.get("status") not in {"complete", "partial", "complete_with_quarantine"}
                or receipt.get("daily_proxy_before") != daily_proxy_before
                or receipt.get("source_daily_sha256") != daily_sha256):
            raise ValueError("hybrid futures history source/cutoff contract mismatch")
        for key in ("minutes", "coverage"):
            item = receipt.get("outputs", {}).get(key, {})
            expected_name = f"{key}.parquet"
            if item.get("file") != expected_name or sha256_file(path.parent / expected_name) != item.get("sha256"):
                raise ValueError(f"hybrid {key} SHA mismatch")
        coverage = pl.read_parquet(path.parent / "coverage.parquet")
        if coverage.select("date", "physical_contract").is_duplicated().any():
            raise ValueError("duplicate historical contract-day evidence")
        no_trade = coverage.filter(pl.col('status') == NO_TRADE)
        no_capacity = coverage.filter(pl.col('status') == NO_CAPACITY)
        # A slice replay needs participation proofs only for its requested
        # dates. Full preflight passes dates=None and still checks all history.
        if dates is not None:
            no_capacity = no_capacity.filter(pl.col('date').cast(pl.String).is_in(list(map(str, dates))))
        if set(receipt.get('quarantined_dates', [])) != set(quarantine_dates):
            raise ValueError('explicit quarantine dates differ from prepared source contract')
        unresolved = validate_contract_day_quarantine(coverage, contract_days, accepted=ACCEPTED)
        covered_dates = set(receipt['covered_dates'])
        if contract_days:
            requested_dates = set(receipt['requested_dates'])
            if (receipt.get('contract_day_quarantine_version') != CONTRACT_DAY_QUARANTINE_VERSION
                    or receipt.get('contract_day_quarantine_policy') != CONTRACT_DAY_QUARANTINE_POLICY
                    or any(x['date'] not in requested_dates or x['date'] in quarantine_dates for x in contract_days)):
                raise ValueError('contract-day quarantine scope/version mismatch')
            raw_gaps = coverage.filter(~pl.col('status').is_in(ACCEPTED))
            if covered_dates != requested_dates - set(map(str, raw_gaps['date'].to_list())):
                raise ValueError('raw covered dates differ from unresolved evidence')
            covered_dates = requested_dates - set(quarantine_dates) - set(map(str, unresolved['date'].to_list()))
            if set(receipt.get('usable_dates', [])) != covered_dates:
                raise ValueError('usable dates differ from exact contract-day quarantine evidence')
        if receipt.get('source_kind') == REPAIR_SOURCE:
            item = receipt.get('outputs', {}).get('official_evidence', {})
            official_path = path.parent/'official_evidence.parquet'
            if (receipt.get('repair_contract_version') != 1 or item.get('file') != official_path.name
                    or sha256_file(official_path) != item.get('sha256')):
                raise ValueError('official no-trade evidence SHA/contract mismatch')
            official = pl.read_parquet(official_path)
            if official.select('date','physical_contract').is_duplicated().any():
                raise ValueError('duplicate official no-trade evidence')
            proven = official.filter((pl.col('outright_volume') == 0)
                                     & pl.col('official_reason').is_in(['zero_total_volume','spread_legs_only','spread_and_block_legs_only','absent_from_complete_day'])
                                     & (pl.col('official_day_sources') != '[]')).select('date','physical_contract')
            if no_trade.join(proven, on=['date','physical_contract'], how='anti').height:
                raise ValueError('no-trade claim lacks independent official evidence')
            if no_capacity.height:
                if participation is None or not 0 < participation <= 1:
                    raise ValueError('integer-capacity proof requires actual execution participation')
                if capacity_rounding == "ceil":
                    keys = [(str(d), c) for d, c in no_capacity.select('date', 'physical_contract').iter_rows()]
                    raise ValueError(
                        f"ceil minute capacity invalidates {len(keys)} floor-only zero integer capacity proofs: "
                        f"{keys[:10]}; supply verified one-minute bars for these contract-days; "
                        "positive daily volume cannot prove zero minute capacity under ceil"
                    )
                zero_capacity = official.filter((pl.col('outright_volume') > 0)
                    & (pl.col('outright_volume') * participation < 1)
                    & (pl.col('official_day_sources') != '[]')).select('date','physical_contract')
                if no_capacity.join(zero_capacity,on=['date','physical_contract'],how='anti').height:
                    raise ValueError('official volume cannot prove zero integer capacity at this participation')
        elif no_trade.height or no_capacity.height:
            raise ValueError('legacy history cannot claim verified official no-trade days')
        required_dates = (set(map(str, dates)) if dates is not None else set(receipt["requested_dates"])) - set(quarantine_dates)
        missing_dates = required_dates - covered_dates
        gaps = unresolved.filter(pl.col("date").cast(pl.String).is_in(required_dates))
        if missing_dates or gaps.height:
            raise ValueError(f"hybrid futures history has {gaps.height} unresolved contract-days across "
                             f"{len(missing_dates)} uncovered panel dates; first={sorted(missing_dates)[:5]}; "
                             "see gaps.parquet; post-cutoff missing ticks cannot become daily fills or zero-return labels")
        if coverage.filter((pl.col("status") == "daily_open_close_proxy")
                           & (pl.col("date").cast(pl.String) >= daily_proxy_before)).height:
            raise ValueError("daily OPEN/CLOSE approximation crosses the exclusive cutoff")
        frame = pl.read_parquet(path)
        verified = coverage.filter(pl.col("status") == "minute_verified").select("date", "physical_contract", "source_file_sha256")
        if frame.select("date", "physical_contract", "source_file_sha256").unique().join(
                verified, on=["date", "physical_contract", "source_file_sha256"], how="anti").height:
            raise ValueError("minute row lacks verified dated physical identity")
        if frame.select("date", "physical_contract", "minute").is_duplicated().any() or frame.filter(
            ~pl.col("minute").is_in(EVENT_MINUTES)
            | ~pl.all_horizontal(pl.col(c).is_finite() & (pl.col(c) > 0) for c in ("vwap", "high", "low", "close", "volume"))
            | (pl.col("high") < pl.col("low"))
            | ~pl.col("close").is_between(pl.col("low"), pl.col("high"))
            | ~pl.col("vwap").is_between(pl.col("low") - 1e-8, pl.col("high") + 1e-8)
        ).height:
            raise ValueError("invalid historical minute price/capacity")
        return frame, receipt
    if (receipt.get("dataset") != MINUTE_DATASET
            or receipt.get("contract_version") != MINUTE_CONTRACT_VERSION
            or receipt.get("status") != "complete"
            or receipt.get("source_daily_sha256") != daily_sha256
            or receipt.get("outputs", {}).get("minutes", {}).get("sha256") != sha256_file(path)):
        raise ValueError("futures minute identity, completeness or source/output SHA mismatch")
    covered = set(receipt.get("covered_dates", []))
    missing = sorted(set(map(str, dates)) - covered) if dates is not None else []
    if missing:
        raise ValueError(
            f"futures minute archive misses {len(missing)} panel dates: "
            f"{missing[0]}..{missing[-1]} (first five: {missing[:5]}); "
            f"receipt covers {min(covered) if covered else 'none'}.."
            f"{max(covered) if covered else 'none'}. "
            "A complete recent-date build is not full training-history coverage. "
            "Supply the missing one-minute futures history; do not truncate folds or substitute daily bars."
        )
    frame = pl.read_parquet(path)
    required = {"date", "physical_contract", "minute", "vwap", "high", "low",
                "close", "volume", "source_file_sha256"}
    if not required <= set(frame.columns):
        raise ValueError("futures minute schema is incomplete")
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError("futures minute facts cannot contain nulls")
    if frame.select("date", "physical_contract", "minute").is_duplicated().any():
        raise ValueError("duplicate physical-contract minute")
    sources = {item["date"]: item["sha256"] for item in receipt.get("sources", [])}
    if (set(sources) != covered or len(sources) != len(receipt.get("sources", []))
            or len(covered) != len(receipt.get("covered_dates", []))):
        raise ValueError("minute source inventory and covered dates disagree")
    from stockagent.data.tw_stock_futures_kbars import KBAR_SOURCE, contract_sources_digest, validate_kbar_completion

    if receipt.get("source_kind") == KBAR_SOURCE:
        contract_sources = {}
        for source in receipt["sources"]:
            contracts = source.get("contracts", [])
            if source["sha256"] != contract_sources_digest(contracts):
                raise ValueError("KBar dated contract inventory SHA mismatch")
            for item in contracts:
                validate_kbar_completion(item)
                key = (source["date"], item["physical_contract"])
                if (key in contract_sources or not item["start"] <= source["date"] <= item["end"]
                        or item["status"] not in {"complete", "source_empty"}):
                    raise ValueError("invalid or duplicate dated KBar contract evidence")
                contract_sources[key] = item["sha256"] if item["status"] == "complete" else None
        for row in frame.select("date", "physical_contract", "source_file_sha256").unique().iter_rows(named=True):
            if contract_sources.get((str(row["date"]), row["physical_contract"])) != row["source_file_sha256"]:
                raise ValueError("minute row is not bound to its dated KBar contract receipt")
    else:
        if receipt.get("source_kind") not in (None, "taifex_transactions"):
            raise ValueError("unknown futures minute source kind")
        if any(item.get("day_session_rows", 0) <= 0 or item.get("day_last_time", 0) < 133000
               for item in receipt.get("sources", [])):
            raise ValueError("futures minute source lacks completed day-session evidence")
        for row in frame.select("date", "source_file_sha256").unique().iter_rows(named=True):
            if sources.get(str(row["date"])) != row["source_file_sha256"]:
                raise ValueError("minute row is not bound to its dated source receipt")
    if frame.filter(
        ~pl.col("minute").is_in(EVENT_MINUTES)
        | ~pl.all_horizontal(pl.col(c).is_finite() & (pl.col(c) > 0)
                             for c in ("vwap", "high", "low", "close", "volume"))
        | (pl.col("high") < pl.col("low"))
        | ~pl.col("close").is_between(pl.col("low"), pl.col("high"))
        | ~pl.col("vwap").is_between(pl.col("low") - 1e-8, pl.col("high") + 1e-8)
    ).height:
        raise ValueError("invalid minute price, interval or capacity")
    return frame, receipt


def preflight_futures_minute_training(config, *, config_path: str | Path) -> dict:
    """Reject absent/invalid inputs before panel construction or DDP launch.

    This checks source integrity only. The loader still checks every exact
    stock-panel date, including receipt-backed sessions with zero fills.
    """
    from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION

    daily = Path(config.trading.tw_stock_futures_day_trade_data_path)
    minute = Path(config.trading.tw_stock_futures_day_trade_minute_data_path)
    manifest = daily.with_name("manifest.json")
    from scripts.publish_data_releases import DEFAULT_CATALOG, _load_catalog

    source = next(entry["source"] for entry in _load_catalog(DEFAULT_CATALOG)
                  if entry["dataset"] == "tw-futures")
    command = shlex.join([
        "run_fintech_python", "scripts/build_tw_stock_futures_0900_entries.py",
        "--config", str(config_path), "--minute-root",
        "data_tw_shioaji_history",
        "--output-dir", str(Path(source) / MINUTE_DATASET),
    ])
    daily_cutoff = config.trading.tw_stock_futures_day_trade_daily_proxy_before
    try:
        if config.trading.tw_stock_futures_day_trade_corporate_transition_path:
            from stockagent.data.tw_stock_futures_transition import load_transition_bundle
            load_transition_bundle(config.trading.tw_stock_futures_day_trade_corporate_transition_path)
        missing = [str(p) for p in (daily, manifest, minute, minute.with_name("manifest.json"))
                   if not p.is_file()]
        if missing:
            raise FileNotFoundError(f"missing required source/receipt: {', '.join(missing)}")
        daily_receipt = json.loads(manifest.read_text())
        digest = sha256_file(daily)
        if (daily_receipt.get("contract_version") != TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
                or daily_receipt.get("outputs", {}).get("continuous_daily", {}).get("sha256") != digest):
            raise ValueError("daily candidate source contract or SHA mismatch")
        _, receipt = validate_futures_minute_data(
            minute, daily_sha256=digest,
            daily_proxy_before=config.trading.tw_stock_futures_day_trade_daily_proxy_before,
            participation=config.trading.max_volume_participation,
            capacity_rounding=config.trading.tw_stock_futures_day_trade_minute_capacity_rounding,
            quarantine_dates=config.trading.tw_stock_futures_day_trade_quarantine_dates,
            quarantine_contract_days=config.trading.tw_stock_futures_day_trade_quarantine_contract_days,
        )
        first_year = config.walk_forward.expected_first_year
        covered = sorted(receipt.get('usable_dates', receipt['covered_dates']))
        if not covered:
            raise ValueError("minute receipt has no covered sessions")
        if first_year is not None and not any(d.startswith(f"{first_year}-") for d in covered):
            raise ValueError(
                f"minute receipt covers {covered[0]}..{covered[-1]} but has no sessions "
                f"in the configured first panel year {first_year}; "
                "a complete recent-date build cannot satisfy this walk-forward config"
            )
        if config.trading.tw_stock_futures_day_trade_residual_policy == "carry":
            from stockagent.data.tw_stock_futures_carry import load_final_settlements, load_carry_corporate_actions
            if not config.trading.tw_stock_futures_day_trade_corporate_action_path:
                raise ValueError('carry requires the audited corporate-action source in carry_v9; old optimizers cannot resume')
            events = load_carry_corporate_actions(config.trading.tw_stock_futures_day_trade_corporate_action_path)
            settlements = load_final_settlements(config.trading.tw_futures_portfolio_final_settlement_path)
            receipt = dict(receipt, residual_policy="carry", official_final_settlement_rows=settlements.height,
                           corporate_action_events=events.height,
                           carry_coverage_scope="selected_minutes_verified; held_physical_continuation_checked_by_ledger")
            if config.trading.tw_stock_futures_day_trade_carry_evidence_path:
                from stockagent.data.tw_stock_futures_carry import load_carry_no_trade_evidence, load_carry_daily_settlements
                proof = load_carry_no_trade_evidence(config.trading.tw_stock_futures_day_trade_carry_evidence_path)
                receipt['carry_proven_zero_outright_contract_days'] = proof.height
                marks = load_carry_daily_settlements(config.trading.tw_stock_futures_day_trade_carry_evidence_path)
                receipt['carry_official_daily_settlement_rows'] = marks.height
        return receipt
    except (OSError, ValueError) as exc:
        if daily_cutoff is not None:
            raise ValueError(
                f"[futures-minute preflight] {exc}\n"
                f"Historical daily OPEN/CLOSE approximation is authorized only before {daily_cutoff}.\n"
                "Supply verified one-minute futures KBars for unresolved contract-days, then rebuild a new "
                "receipt-backed minute snapshot. No tick download is required.\n"
                "Inspect coverage.parquet and gaps.parquet; capacity-rounding proof failures are listed above. "
                "Unresolved later observations block full-history training."
            ) from exc
        raise ValueError(
            f"[futures-minute preflight] {exc}\n"
            f"Configured panel_start_date={config.data.panel_start_date}; "
            f"expected_first_year={config.walk_forward.expected_first_year}; "
            f"epochs={config.training.epochs}.\n"
            "Inspect one-minute KBar coverage first (read-only; no ticks required):\n"
            f"  source scripts/runtime_env.sh\n  {command} --check-only\n"
            f"Build after the required one-minute history is available:\n  {command}\n"
            "Publish rebuilt data as a new release and update the config's pinned sources/output root.\n"
            "Restore an audited matching minute release or supply the historical one-minute KBars. "
            "Daily bars, fabricated receipts and automatic fold truncation are not valid repairs."
        ) from exc


def load_futures_minute_tape(
    path: str | Path, selected: pl.DataFrame, dates: np.ndarray,
    symbols: tuple[str, ...], *, daily_sha256: str, fee: float,
    participation: float,
    capacity_rounding: str = "floor",
    daily_proxy_before: str | None = None,
    quarantine_dates: tuple[str, ...] | list[str] = (),
    quarantine_contract_days: tuple[dict, ...] | list[dict] = (),
) -> tuple[np.ndarray, dict]:
    """Require coverage of every panel date; absent trades have zero capacity."""
    from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate

    if not np.isfinite(fee) or fee < 0:
        raise ValueError("fee must be finite and non-negative")
    if not np.isfinite(participation) or not 0 < participation <= 1:
        raise ValueError("participation must be in (0,1]")
    frame, receipt = validate_futures_minute_data(path, daily_sha256=daily_sha256, dates=dates,
                                                daily_proxy_before=daily_proxy_before, participation=participation,
                                                capacity_rounding=capacity_rounding,
                                                quarantine_dates=quarantine_dates,
                                                quarantine_contract_days=quarantine_contract_days)
    selected = exclude_contract_days(selected, quarantine_contract_days)
    from stockagent.data.tw_stock_futures_kbars import KBAR_SOURCE

    if receipt.get("source_kind") == KBAR_SOURCE:
        covered_contracts = {(s["date"], c["physical_contract"])
                             for s in receipt["sources"] for c in s["contracts"]}
        requested_dates = set(map(str, dates))
        required_contracts = {(str(d), c) for d, c in selected.select("date", "physical_contract").iter_rows()
                              if str(d) in requested_dates}
        if missing := sorted(required_contracts - covered_contracts):
            raise ValueError(f"one-minute KBar history misses {len(missing)} selected contract-days: {missing[:5]}")
    fields = HYBRID_TAPE_FIELDS if daily_proxy_before is not None else TAPE_FIELDS
    tape = np.zeros((len(dates), len(symbols), 2, fields), dtype=np.float32)
    di = {str(d): i for i, d in enumerate(dates)}
    si = {s: i for i, s in enumerate(symbols)}
    ei = {m: i for i, m in enumerate(EVENT_MINUTES)}
    daily_keys = set()
    if daily_proxy_before is not None:
        from stockagent.data.tw_stock_futures_history import ACCEPTED
        coverage = pl.read_parquet(Path(path).parent / "coverage.parquet")
        covered_keys = set(coverage.filter(pl.col("status").is_in(ACCEPTED))
                           .select("date", "physical_contract").iter_rows())
        required_keys = {(d, c) for d, c in selected.select("date", "physical_contract").iter_rows()
                         if str(d) in di and str(d) not in quarantine_dates}
        if missing_keys := required_keys - covered_keys:
            raise ValueError(f"hybrid history misses selected contract-days: {sorted(missing_keys)[:5]}")
        daily_keys = set(coverage.filter(pl.col("status") == "daily_open_close_proxy").select("date", "physical_contract").iter_rows())
    keys = selected.select("date", "physical_contract", "underlying_symbol",
                           "candidate_slot", "contract_multiplier")
    for row in keys.iter_rows(named=True):
        d, s = di.get(str(row["date"])), si.get(row["underlying_symbol"])
        if d is not None and s is not None:
            tape[d, s, row["candidate_slot"], :3] = (
                row["contract_multiplier"], fee, stock_index_futures_tax_rate(row["date"])
            )
    round_capacity = np.ceil if capacity_rounding == "ceil" else np.floor
    aligned = frame.join(keys, on=["date", "physical_contract"], how="inner", validate="m:1")
    for row in aligned.iter_rows(named=True):
        d, s = di.get(str(row["date"])), si.get(row["underlying_symbol"])
        if d is None or s is None:
            continue
        offset = 3 + ei[row["minute"]] * len(BAR_FIELDS)
        tape[d, s, row["candidate_slot"], offset:offset + 5] = (
            row["vwap"], row["high"], row["low"], row["close"],
            round_capacity(row["volume"] * participation),
        )
    if daily_keys:
        for row in selected.iter_rows(named=True):
            d, s = di.get(str(row["date"])), si.get(row["underlying_symbol"])
            if d is None or s is None or (row["date"], row["physical_contract"]) not in daily_keys:
                continue
            values = (row["open"], row["close"], row["previous_volume"])
            valid = (row["source_row_observed"] and row["executable"] and row["volume"] > 0
                     and all(v is not None and np.isfinite(v) and v > 0 for v in values))
            tape[d, s, row["candidate_slot"], TAPE_FIELDS:] = (
                1, row["open"] if valid else 0, row["close"] if valid else 0,
                np.floor(row["previous_volume"] * participation) if valid else 0,
            )
    return tape, receipt
