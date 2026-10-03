"""Receipt-bound continuous ticks -> dated physical contracts -> minute facts.

R1's current resolved target is never historical identity. The official dated
monthly calendar determines the mapping; observed OHLC independently checks it.
All prices remain Taipei wall time. Source gaps are evidence, never zero trades.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import hashlib
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_price_rules import (
    TW_DERIVATIVE_PRICE_CONTRACT_VERSION,
    TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION,
    price_on_tick_grid_numpy,
)
from stockagent.data.tw_stock_futures_minute import EVENT_MINUTES, select_futures_minute_candidates
from stockagent.data.tw_stock_futures_quarantine import (
    CONTRACT_DAY_QUARANTINE_VERSION, CONTRACT_DAY_QUARANTINE_POLICY,
    normalize_contract_days, validate_contract_day_quarantine,
)

HISTORY_SOURCE = "shioaji_continuous_ticks_dated_physical_v1"
MULTISOURCE_INTRADAY_SOURCE = "taifex_verified_multisource_intraday_minutes_v1"
HISTORY_DATASET = "taifex_stock_futures_minute_history_v2"
HISTORY_VERSION = 2
ALL_FUTURES_SOURCE_SCOPE_VERSION = 2
ACCEPTED = {"minute_verified", "daily_open_close_proxy", "official_no_outright_trades", "official_subcontract_capacity"}
MINUTE_SCHEMA = {"date": pl.Date, "physical_contract": pl.String, "minute": pl.Int32,
                 **{c: pl.Float64 for c in ("vwap", "high", "low", "close", "volume")},
                 "source_file_sha256": pl.String}


def _selection_sha256(selected: pl.DataFrame) -> str:
    keys = selected.select("date", "physical_contract").sort("date", "physical_contract")
    return hashlib.sha256(json.dumps(keys.rows(), default=str, separators=(",", ":")).encode()).hexdigest()


def _load_base_bundle(path: Path, *, selected: pl.DataFrame, daily_digest: str,
                      participation: float | None, capacity_rounding: str,
                      revalidate_contract_days: tuple[dict, ...] = (),
                      enforce_volume_bound: bool = False,
                      official_volume_bounds: pl.DataFrame | None = None):
    """Reuse independently validated facts, never a prior universe/quarantine."""
    from stockagent.data.tw_stock_futures_minute import validate_futures_minute_data

    path = path.parent if path.name == "minutes.parquet" else path
    manifest_path = path / "manifest.json"
    manifest_digest = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    minutes, _ = validate_futures_minute_data(
        path / "minutes.parquet", daily_sha256=daily_digest,
        daily_proxy_before=manifest.get("daily_proxy_before"),
        participation=participation, capacity_rounding=capacity_rounding,
        quarantine_dates=manifest.get("quarantined_dates", []),
        quarantine_contract_days=manifest.get("quarantined_contract_days", []),
        require_complete=False,
    )
    coverage_path = path / "coverage.parquet"
    if manifest.get("outputs", {}).get("coverage", {}).get("sha256") != sha256_file(coverage_path):
        raise ValueError("base minute coverage SHA mismatch")
    coverage = pl.read_parquet(coverage_path)
    if coverage.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("base minute bundle has duplicate physical contract-days")
    accepted = coverage.filter(pl.col("status").is_in(ACCEPTED - {"daily_open_close_proxy"})).join(
        selected.select("date", "physical_contract"), on=["date", "physical_contract"], how="semi",
    )
    if enforce_volume_bound and "tick_volume" in accepted.columns:
        accepted = accepted.join(selected.select("date", "physical_contract", pl.col("volume").alias("_daily_volume")),
                                 on=["date", "physical_contract"], how="left", validate="m:1")
        if official_volume_bounds is not None:
            accepted = accepted.join(official_volume_bounds.select("date", "physical_contract", pl.col("outright_volume").alias("_outright_bound")),
                                     on=["date", "physical_contract"], how="left", validate="m:1").with_columns(
                pl.min_horizontal("_daily_volume", "_outright_bound").alias("_daily_volume")).drop("_outright_bound")
        accepted = accepted.filter(pl.col("tick_volume").is_null() | (pl.col("tick_volume") <= pl.col("_daily_volume"))).drop("_daily_volume")
    if revalidate_contract_days:
        revalidate = pl.DataFrame(list(revalidate_contract_days)).with_columns(pl.col("date").str.to_date())
        accepted = accepted.join(revalidate, on=["date", "physical_contract"], how="anti")
    minutes = minutes.join(accepted.select("date", "physical_contract"),
                           on=["date", "physical_contract"], how="semi").select(*MINUTE_SCHEMA).cast(MINUTE_SCHEMA)
    # ZIP/KBar bundles bind per-day/per-contract receipts in their manifest;
    # adapt only coverage metadata to the shared history representation.
    # Event-minute volume cannot be labelled full-session tick volume.
    for name, dtype, value in (("alias", pl.String, ""), ("receipt_sha256", pl.String, manifest_digest),
                               ("tick_volume", pl.Int64, None), ("tick_rows", pl.Int64, None),
                               ("detail", pl.String, "verified_canonical_base_bundle")):
        if name not in accepted.columns:
            accepted = accepted.with_columns(pl.lit(value, dtype=dtype).alias(name))
    for name, source_name in (("official_volume", "volume"), ("source_row_observed", "source_row_observed")):
        if name not in accepted.columns:
            accepted = accepted.join(selected.select("date", "physical_contract", pl.col(source_name).alias(name)),
                                     on=["date", "physical_contract"], how="left", validate="m:1")
    if enforce_volume_bound:
        rejected_keys = _volume_bound_violations(accepted, minutes, selected, official_volume_bounds).select("date", "physical_contract")
        accepted = accepted.join(rejected_keys, on=["date", "physical_contract"], how="anti")
        minutes = minutes.join(accepted.select("date", "physical_contract"), on=["date", "physical_contract"], how="semi")
    official = None
    if "official_evidence" in manifest.get("outputs", {}):
        official_path = path / "official_evidence.parquet"
        if sha256_file(official_path) != manifest["outputs"]["official_evidence"]["sha256"]:
            raise ValueError("base official evidence SHA mismatch")
        official = pl.read_parquet(official_path).join(
            accepted.select("date", "physical_contract"), on=["date", "physical_contract"], how="semi",
        )
    if sha256_file(manifest_path) != manifest_digest:
        raise ValueError("base minute manifest changed during validation")
    return minutes, accepted, official, {
        "path": str(path), "manifest_sha256": manifest_digest,
        "reused_contract_days": accepted.height,
        "scope": "accepted physical contract-days only; prior quarantine is not inherited",
        "revalidate_contract_days": list(revalidate_contract_days),
        "outputs": manifest["outputs"],
    }


def normalize_continuous_ticks(frame: pl.DataFrame, *, day: date, alias: str,
                               physical: str, digest: str, product: str | None = None,
                               asset_class: str = "stock_future", query_dates: tuple[date, ...] | None = None,
                               is_last_trading_day: bool = False) -> tuple[pl.DataFrame, dict]:
    required = {"event_ts", "ts", "close", "volume", "trading_date", "query_contract", "source_row_index"}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError("missing tick schema or empty complete receipt")
    if frame.filter(~pl.col("trading_date").is_in(query_dates or (day,)) | (pl.col("query_contract") != alias)).height:
        raise ValueError("tick query identity differs from receipt")
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError("null tick identity/price")
    if frame.filter(pl.col("event_ts").cast(pl.Int64) != pl.col("ts")).height:
        raise ValueError("tick event timestamp differs from wall-clock ts")
    start_minute, end_minute = 525, 825
    if product is not None:
        from stockagent.data.tw_price_rules import taifex_futures_day_session_minutes
        start_minute, end_minute = taifex_futures_day_session_minutes(
            day, product_code=product, asset_class=asset_class,
            is_last_trading_day=is_last_trading_day,
        )
    session = frame.filter(
        (pl.col("event_ts").dt.date() == day)
        & (pl.col("event_ts").dt.hour().cast(pl.Int32) * 60
           + pl.col("event_ts").dt.minute().cast(pl.Int32)).is_between(start_minute, end_minute)
    ).sort(["event_ts", *(["_query_partition_order"] if "_query_partition_order" in frame.columns else []), "source_row_index"])
    if product is not None:
        day_ns = int(np.datetime64(day, "ns").astype(np.int64))
        session = session.filter(pl.col("ts").is_between(
            day_ns + start_minute * 60_000_000_000,
            day_ns + end_minute * 60_000_000_000,
        ))
    if session.is_empty():
        raise ValueError("no day-session trades")
    if session.filter(~pl.col("close").is_finite() | (pl.col("close") <= 0)
                      | (pl.col("volume") <= 0)).height:
        raise ValueError("invalid day-session price/volume")
    # A transaction is a grid-constrained outright price; the minute VWAP
    # calculated below is not. Use the session date, never today's FOP rule.
    if product is None:
        valid_grid = price_on_tick_grid_numpy(
            session["close"].to_numpy(), np.datetime64(day), security_types=asset_class,
        )
    else:
        from stockagent.data.tw_price_rules import price_on_taifex_futures_tick_grid_numpy
        valid_grid = price_on_taifex_futures_tick_grid_numpy(
            session["close"].to_numpy(), np.datetime64(day),
            product_codes=product, asset_classes=asset_class,
        )
    if not valid_grid.all():
        raise ValueError("off-grid dated futures transaction price")
    stats = dict(tick_open=float(session["close"][0]), tick_close=float(session["close"][-1]),
                 tick_high=float(session["close"].max()), tick_low=float(session["close"].min()),
                 tick_volume=int(session["volume"].sum()), tick_rows=session.height)
    bars = (session.with_columns(
        (pl.col("event_ts").dt.hour().cast(pl.Int32) * 60
         + pl.col("event_ts").dt.minute().cast(pl.Int32) + 1).alias("minute"))
        .group_by("minute", maintain_order=True).agg(
            ((pl.col("close") * pl.col("volume")).sum() / pl.col("volume").sum()).alias("vwap"),
            pl.col("close").max().alias("high"), pl.col("close").min().alias("low"),
            pl.col("close").last().alias("close"), pl.col("volume").sum().cast(pl.Float64),
        ).with_columns(pl.lit(day).alias("date"), pl.lit(physical).alias("physical_contract"),
                       pl.lit(digest).alias("source_file_sha256"))
        .select(*MINUTE_SCHEMA).cast(MINUTE_SCHEMA))
    return bars, stats


def _tick_partition_fingerprint(partitions: list[dict]) -> str:
    facts = [{key: item[key] for key in ("alias", "query_date", "data_sha256", "receipt_sha256")}
             for item in partitions]
    return hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _read_verified_tick_partition(root: Path, alias: str, query_date: date, *, allow_legacy_tx: bool = False):
    receipt_path = root / alias / "receipts" / f"trading_date={query_date}.json"
    path = root / alias / "ticks" / f"trading_date={query_date}" / "data.parquet"
    try:
        raw_receipt = receipt_path.read_bytes()
        receipt = json.loads(raw_receipt)
    except (OSError, ValueError):
        return None, {}, "missing_receipt"
    proof = {"alias": alias, "query_date": str(query_date), "receipt_path": str(receipt_path),
             "data_path": str(path), "receipt_sha256": hashlib.sha256(raw_receipt).hexdigest()}
    from downloader.download_shioaji_tx_futures_ticks import SOURCE, LEGACY_TX_SOURCE
    allowed_sources = {SOURCE}
    if alias == "TXFR1" and allow_legacy_tx:
        allowed_sources.add(LEGACY_TX_SOURCE)
    if (receipt.get("source") not in allowed_sources
            or receipt.get("schema_version") != 1 or receipt.get("contract") != alias
            or receipt.get("trading_date") != str(query_date) or receipt.get("session_finalized") is False):
        return None, proof, "invalid_receipt"
    if receipt.get("status") != "complete":
        return None, proof, "source_empty_unresolved" if receipt.get("status") == "source_empty" else "incomplete_receipt"
    digest = sha256_file(path)
    if digest != receipt.get("sha256"):
        raise ValueError("tick SHA differs from receipt")
    frame = pl.read_parquet(path)
    if frame.height != receipt.get("rows"):
        raise ValueError("tick rows differ from receipt")
    required = {"event_ts", "ts", "close", "volume", "trading_date", "query_contract", "source_row_index"}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError("missing tick schema or empty complete receipt")
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError("null tick identity/price")
    if frame.filter((pl.col("trading_date") != query_date) | (pl.col("query_contract") != alias)
                    | (pl.col("event_ts").cast(pl.Int64) != pl.col("ts"))).height:
        raise ValueError("tick query partition identity differs from receipt")
    if receipt_path.read_bytes() != raw_receipt or sha256_file(path) != digest:
        raise ValueError("source changed while reading tick partition")
    proof["data_sha256"] = digest
    return frame, proof, "complete"


def _contract_day_single_root(row: dict, root: Path, cutoff: date) -> tuple[pl.DataFrame, dict]:
    day, physical = row["date"], row["physical_contract"]
    evidence = {"date": day, "physical_contract": physical, "status": "missing_receipt",
                "alias": "", "source_file_sha256": "", "receipt_sha256": "",
                "official_volume": row["volume"], "tick_volume": None, "tick_rows": None,
                "detail": "", "source_row_observed": bool(row["source_row_observed"])}
    empty = pl.DataFrame(schema=MINUTE_SCHEMA)
    if day < cutoff:
        evidence["status"] = "daily_open_close_proxy"
        return empty, evidence
    if not row.get("calendar_physical") or row["calendar_physical"] != physical:
        evidence.update(status="unverified_calendar", detail="selected contract differs from observed monthly R1")
        return empty, evidence
    # The official calendar supplies identity before prices are consulted.
    for sj_root in str(row.get("shioaji_roots") or row["product"]).split(","):
        alias = sj_root.strip() + "R1"
        try:
            frame, proof, status = _read_verified_tick_partition(root, alias, day, allow_legacy_tx=row["product"] == "TX")
            evidence.update(alias=alias, receipt_sha256=proof.get("receipt_sha256", ""))
            if status != "complete":
                evidence["status"] = status
                continue
            partitions, query_dates = [proof], [day]
            product = (row["product"] if row.get("_source_scope") == "all_futures_intraday"
                       or row.get("asset_class") in {"etf_future", "index_future"} else None)
            # resolved_last_trade_date can be a last-observed-date inference,
            # not an official expiry notice. Keep the regular envelope unless
            # an independently authoritative expiry caller supplies a rule.
            is_last = False
            partition_detail = ""
            if row.get("_source_scope") == "all_futures_intraday":
                from stockagent.data.tw_price_rules import taifex_futures_day_session_minutes
                _, end_minute = taifex_futures_day_session_minutes(
                    day, product_code=row["product"], asset_class=row["asset_class"],
                    is_last_trading_day=is_last,
                )
                if end_minute > 870:
                    next_day = row.get("_next_query_date")
                    partition_detail = "; observed_query_only_not_full_volume"
                    if next_day is not None and row.get("_next_calendar_physical") == physical:
                        tail, tail_proof, tail_status = _read_verified_tick_partition(
                            root, alias, next_day, allow_legacy_tx=row["product"] == "TX",
                        )
                        if tail_status == "complete":
                            today = frame.filter(pl.col("event_ts").dt.date() == day)
                            tail_today = tail.filter(pl.col("event_ts").dt.date() == day)
                            if (today.height and tail_today.height
                                    and today["ts"].max() >= tail_today["ts"].min()):
                                raise ValueError("overlapping query partition timestamps; cannot deduplicate trade identities")
                            # Keep both original query dates and row orders.
                            # Only event_ts owns the reconstructed session date.
                            frame = pl.concat([
                                frame.with_columns(pl.lit(0).alias("_query_partition_order")),
                                tail.with_columns(pl.lit(1).alias("_query_partition_order")),
                            ], how="diagonal_relaxed")
                            partitions.append(tail_proof)
                            query_dates.append(next_day)
                            partition_detail = "; event_day_reconstructed_from_two_verified_query_partitions"
                        elif tail_status not in {"missing_receipt", "source_empty_unresolved"}:
                            raise ValueError(f"unverified next query partition: {tail_status}")
            digest = proof["data_sha256"] if len(partitions) == 1 else _tick_partition_fingerprint(partitions)
            bars, stats = normalize_continuous_ticks(
                frame, day=day, alias=alias, physical=physical, digest=digest,
                product=product, asset_class=row.get("asset_class", "stock_future"),
                query_dates=tuple(query_dates), is_last_trading_day=is_last,
            )
            evidence.update(source_file_sha256=digest, tick_volume=stats["tick_volume"], tick_rows=stats["tick_rows"])
            if (row.get("_source_scope") == "all_futures_intraday"
                    and stats["tick_volume"] > row["volume"]):
                raise ValueError(f"observed volume {stats['tick_volume']} exceeds official daily volume {row['volume']}")
            if not all(np.isclose(stats[f"tick_{field}"], row[field], rtol=1e-7, atol=.011)
                       for field in ("open", "high", "low", "close")):
                evidence.update(status="ohlc_identity_mismatch", detail=json.dumps(stats, sort_keys=True))
                continue
            for part in partitions:
                if (sha256_file(part["data_path"]) != part["data_sha256"]
                        or sha256_file(part["receipt_path"]) != part["receipt_sha256"]):
                    raise ValueError("source changed during aggregation; retry")
            if len(partitions) > 1:
                evidence["source_partitions"] = json.dumps(partitions, sort_keys=True)
                evidence["receipt_sha256"] = hashlib.sha256("".join(p["receipt_sha256"] for p in partitions).encode()).hexdigest()
            evidence.update(status="minute_verified", detail="calendar_and_ohlc_match; observed_tick_volume_only" + partition_detail)
            return bars, evidence
        except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
            evidence.update(status="invalid_source", detail=str(exc)[:300])
    return empty, evidence


def _enforce_observed_volume_bound(row: dict, bars: pl.DataFrame, evidence: dict):
    """An accepted observed subset cannot exceed its official full-day total."""
    if row.get("_source_scope") == "all_futures_intraday" and evidence.get("status") == "minute_verified":
        observed = max(evidence.get("tick_volume") or 0, bars["volume"].sum() or 0)
        upper_bound = min(row["volume"], row.get("_outright_volume", row["volume"]))
        if observed > upper_bound:
            evidence = dict(evidence, status="invalid_source",
                            detail=f"observed volume {observed} exceeds official volume bound {upper_bound}")
            return pl.DataFrame(schema=MINUTE_SCHEMA), evidence
    return bars, evidence


def _volume_bound_violations(coverage, bars, selected, official):
    """Final vector gate also covers frozen bundles and assemble-only caches."""
    quantities = bars.group_by("date", "physical_contract").agg(pl.col("volume").sum().alias("_bar_volume"))
    checked = coverage.filter(pl.col("status") == "minute_verified").join(
        quantities, on=["date", "physical_contract"], how="left", validate="1:1").join(
        selected.select("date", "physical_contract", pl.col("volume").alias("_volume_bound")),
        on=["date", "physical_contract"], how="left", validate="1:1")
    if official is not None:
        checked = checked.join(official.select("date", "physical_contract", pl.col("outright_volume").alias("_outright_bound")),
                               on=["date", "physical_contract"], how="left", validate="1:1").with_columns(
            pl.min_horizontal("_volume_bound", "_outright_bound").alias("_volume_bound"))
    return checked.filter(pl.max_horizontal("tick_volume", "_bar_volume") > pl.col("_volume_bound"))


def _load_official_recovery(args):
    if not (getattr(args, 'repair_root', None) or getattr(args, 'official_evidence_dir', None)):
        return None, None, None, None
    from stockagent.data.tw_stock_futures_repair import ExactMinuteRecovery, apply_block_evidence
    evidence_dir = Path(args.official_evidence_dir)
    evidence_receipt = json.loads((evidence_dir/'official_evidence_manifest.json').read_text())
    official_sha = sha256_file(evidence_dir/'official_evidence.parquet')
    if evidence_receipt.get('source') != 'taifex_complete_daily_and_spread_legs_v1' or evidence_receipt.get('sha256') != official_sha:
        raise ValueError('official evidence SHA/source mismatch')
    for item in evidence_receipt['sources']:
        if sha256_file(item['path']) != item['sha256']:
            raise ValueError('official archive changed since audit')
    official = pl.read_parquet(evidence_dir/'official_evidence.parquet')
    if official.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('duplicate official physical contract-day evidence')
    block_manifest = evidence_dir/'block_evidence_manifest.json'
    if block_manifest.exists():
        official, block_receipt = apply_block_evidence(official, block_manifest)
        evidence_receipt['block_evidence'] = block_receipt
    recovery = ExactMinuteRecovery(Path(args.repair_root) if getattr(args, 'repair_root', None) else None, official,
                                   participation=getattr(args, 'capacity_participation', None),
                                   capacity_rounding=getattr(args, 'capacity_rounding', 'floor'))
    return recovery, official, evidence_receipt, official_sha


def _contract_day(row: dict, root: Path | list[Path], cutoff: date) -> tuple[pl.DataFrame, dict]:
    roots = [root] if isinstance(root, (str, Path)) else list(root)
    if not roots:
        raise ValueError("at least one continuous tick root is required")
    result = None
    for directory in roots:
        bars, evidence = _contract_day_single_root(row, Path(directory), cutoff)
        if evidence["status"] in ACCEPTED:
            return bars, evidence
        if result is None or evidence["status"] != "missing_receipt":
            result = bars, evidence
    assert result is not None
    return result


def _reuse_verified_contract(root: Path, evidence: dict) -> bool:
    """Cached facts are reusable only while their exact raw inputs still match."""
    if evidence.get("status") != "minute_verified":
        return False
    if evidence.get("source_partitions"):
        try:
            partitions = json.loads(evidence["source_partitions"])
            return (len(partitions) >= 2
                    and _tick_partition_fingerprint(partitions) == evidence["source_file_sha256"]
                    and all(sha256_file(part["data_path"]) == part["data_sha256"]
                            and sha256_file(part["receipt_path"]) == part["receipt_sha256"]
                            for part in partitions))
        except (OSError, ValueError, KeyError, TypeError):
            return False
    if evidence.get('repair_kind') in {'exact_kbars', 'exact_ticks', 'finmind_ticks'}:
        try:
            return (sha256_file(evidence['repair_receipt_path']) == evidence['receipt_sha256']
                    and sha256_file(evidence['repair_data_path']) == evidence['source_file_sha256'])
        except OSError:
            return False
    alias, day = evidence["alias"], evidence["date"]
    for directory in ([root] if isinstance(root, (str, Path)) else root):
        receipt = Path(directory) / alias / "receipts" / f"trading_date={day}.json"
        ticks = Path(directory) / alias / "ticks" / f"trading_date={day}" / "data.parquet"
        try:
            if (sha256_file(receipt) == evidence["receipt_sha256"]
                    and sha256_file(ticks) == evidence["source_file_sha256"]):
                return True
        except OSError:
            pass
    return False


def build_continuous_history(args, source: pl.DataFrame, expected_dates: list[date], daily_digest: str) -> int:
    """Resumable dated shards live outside the published producer tree."""
    cutoff = date.fromisoformat(args.daily_proxy_before)
    scope = getattr(args, "scope", "stock_front")
    if scope == "all_futures_intraday" and (getattr(args, "quarantine_dates", None)
                                           or getattr(args, "quarantine_contract_days", None)):
        raise ValueError("all-futures source preparation cannot inherit stock-only quarantines")
    selected = select_futures_minute_candidates(source, scope=scope)
    if scope == "all_futures_intraday" and selected.filter(pl.col("date") < cutoff).height:
        raise ValueError("all-futures intraday requires minute evidence; daily proxies are not allowed")
    calendar_source = getattr(args, "source_calendar", source)
    calendar = (calendar_source.filter(pl.col("source_row_observed") & pl.col("contract").str.contains(r"^\d{6}$"))
                .sort("date", "product", "contract").group_by("date", "product", maintain_order=True)
                .agg(pl.col("physical_contract").first().alias("calendar_physical")))
    selected = selected.join(calendar, on=["date", "product"], how="left", validate="m:1")
    if scope == "all_futures_intraday":
        calendar_dates = calendar_source["date"].unique().sort().to_list()
        next_dates = getattr(args, "source_next_dates", dict(zip(calendar_dates[:-1], calendar_dates[1:])))
        selected = selected.with_columns(
            pl.col("date").replace_strict(next_dates, default=None, return_dtype=pl.Date).alias("_next_query_date"),
        ).join(calendar.rename({"date": "_next_query_date", "calendar_physical": "_next_calendar_physical"}),
               on=["_next_query_date", "product"], how="left", validate="m:1")
    if args.check_only:
        print(json.dumps({"status": "inventory_only", "candidates": selected.height,
                          "daily_proxy_before": str(cutoff), "source_daily_sha256": daily_digest}))
        return 0
    root = [Path(args.shioaji_ticks_root), *map(Path, getattr(args, "additional_shioaji_ticks_root", []) or [])]
    output = Path(args.output_dir)
    cache = Path(args.work_dir) / f"{daily_digest[:20]}-{cutoff}-v{HISTORY_VERSION}"
    recovery, official, evidence_receipt, official_sha = _load_official_recovery(args)
    if scope == "all_futures_intraday" and official is not None:
        complete_bounds = official.filter(pl.col("outright_volume").is_not_null()).select("date", "physical_contract")
        if selected.join(complete_bounds, on=["date", "physical_contract"], how="anti").height:
            raise ValueError("all-futures official volume evidence must cover every selected physical contract-day")
    base_bars, base_coverage, base_official, base_receipt = None, None, None, None
    revalidate_contract_days = normalize_contract_days(
        json.loads(Path(args.revalidate_contract_days).read_text()) if getattr(args, "revalidate_contract_days", None) else [],
    )
    if revalidate_contract_days:
        requested_revalidation = pl.DataFrame(list(revalidate_contract_days)).with_columns(pl.col("date").str.to_date())
        if requested_revalidation.join(selected.select("date", "physical_contract"),
                                       on=["date", "physical_contract"], how="anti").height:
            raise ValueError("revalidate contract-days must belong to the selected universe")
    base_paths = ([args.base_minute_bundle] if getattr(args, "base_minute_bundle", None) else [])
    base_paths += list(getattr(args, "supplemental_minute_bundle", []) or [])
    base_receipts = []
    for base_index, supplied_base_path in enumerate(base_paths):
        base_path = Path(supplied_base_path)
        base_path = base_path.parent if base_path.name == "minutes.parquet" else base_path
        if base_path.resolve() == output.resolve() or base_path.resolve() in output.resolve().parents:
            raise ValueError("incremental output must be separate from the immutable base minute bundle")
        remaining = (selected if base_coverage is None else selected.join(
            base_coverage.select("date", "physical_contract"), on=["date", "physical_contract"], how="anti"))
        new_bars, new_coverage, new_official, new_receipt = _load_base_bundle(
            base_path, selected=remaining, daily_digest=daily_digest,
            participation=getattr(args, "capacity_participation", None),
            capacity_rounding=getattr(args, "capacity_rounding", "floor"),
            revalidate_contract_days=revalidate_contract_days if base_index == 0 else (),
            enforce_volume_bound=scope == "all_futures_intraday",
            official_volume_bounds=official if scope == "all_futures_intraday" else None,
        )
        base_bars = new_bars if base_bars is None else pl.concat([base_bars, new_bars], how="vertical_relaxed")
        base_coverage = new_coverage if base_coverage is None else pl.concat([base_coverage, new_coverage], how="diagonal_relaxed")
        if new_official is not None:
            base_official = new_official if base_official is None else pl.concat([base_official, new_official], how="diagonal_relaxed")
        base_receipts.append(new_receipt)
    if base_receipts:
        base_receipt = (base_receipts[0] if len(base_receipts) == 1 else {
            "bundles": base_receipts,
            "manifest_sha256": hashlib.sha256(json.dumps(base_receipts, sort_keys=True).encode()).hexdigest(),
            "reused_contract_days": base_coverage.height,
            "scope": "first independently accepted physical key wins; conflicting later candidates do not overwrite it",
        })
    scope_fingerprint = None
    if scope != "stock_front" or base_receipt:
        scope_fingerprint = hashlib.sha256(json.dumps({
            "scope": scope, "version": ALL_FUTURES_SOURCE_SCOPE_VERSION, "selection_sha256": _selection_sha256(selected),
            "product_history_contract_version": TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION,
            "base_manifest_sha256": base_receipt["manifest_sha256"] if base_receipt else None,
            "revalidate_contract_days": list(revalidate_contract_days),
            "official_evidence": evidence_receipt,
        }, sort_keys=True).encode()).hexdigest()
        cache = cache.with_name(cache.name + f"-{scope}-{scope_fingerprint[:20]}")
    original_cache = cache
    if recovery:
        cache = cache.with_name(cache.name + '-exact-v1-' + official_sha[:16])
        if getattr(args, 'capacity_rounding', 'floor') != 'floor':
            cache = cache.with_name(cache.name + '-' + args.capacity_rounding)
    cache.mkdir(parents=True, exist_ok=True)
    base_bars_by_day = base_bars.partition_by("date", as_dict=True) if base_bars is not None else {}
    base_coverage_by_day = base_coverage.partition_by("date", as_dict=True) if base_coverage is not None else {}
    frames, inventories = [], []
    refresh_dates = set(getattr(args, 'refresh_dates', None) or [])
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index, day in enumerate(expected_dates, 1):
            day_selected = selected.filter(pl.col("date") == day)
            expected_key_digest = _selection_sha256(day_selected)
            cache_bars, cache_coverage, receipt_path = (cache / f"{day}.{suffix}" for suffix in ("parquet", "coverage.parquet", "json"))
            # Reuse verified contract facts individually, re-read gaps, and detect
            # corrected raw data even when a dated shard was previously complete.
            saved = json.loads(receipt_path.read_text()) if receipt_path.is_file() else {}
            cached = (cache_bars.is_file() and cache_coverage.is_file()
                    and saved.get("price_rule_contract_version") == TW_DERIVATIVE_PRICE_CONTRACT_VERSION
                    and saved.get("bars_sha256") == sha256_file(cache_bars)
                    and saved.get("coverage_sha256") == sha256_file(cache_coverage))
            if scope_fingerprint:
                cached = cached and saved.get("scope_fingerprint") == scope_fingerprint
                cached = cached and saved.get("expected_keys_sha256") == expected_key_digest
            prior_bars, prior_coverage = cache_bars, cache_coverage
            if recovery and not cached and not scope_fingerprint:
                prior_bars, prior_coverage, old_receipt = (original_cache / f'{day}.{suffix}' for suffix in ('parquet','coverage.parquet','json'))
                old = json.loads(old_receipt.read_text()) if old_receipt.is_file() else {}
                cached = (prior_bars.is_file() and prior_coverage.is_file()
                          and old.get("price_rule_contract_version") == TW_DERIVATIVE_PRICE_CONTRACT_VERSION
                          and sha256_file(prior_bars) == old.get('bars_sha256')
                          and sha256_file(prior_coverage) == old.get('coverage_sha256'))
            preserve_date = bool(refresh_dates) and str(day) not in refresh_dates
            if getattr(args, "assemble_cached", False) or preserve_date:
                if not cached or prior_bars != cache_bars:
                    raise ValueError(f"cannot assemble missing/corrupt dated shard: {day}")
            elif cached and day < cutoff:
                bars, coverage = pl.read_parquet(cache_bars), pl.read_parquet(cache_coverage)
            else:
                rows = day_selected.to_dicts()
                if scope == "all_futures_intraday":
                    for row in rows:
                        row["_source_scope"] = scope
                        fact = recovery.official.get((row['date'], row['physical_contract'])) if recovery else None
                        if fact and fact.get('outright_volume') is not None:
                            row['_outright_volume'] = fact['outright_volume']
                previous_evidence, previous_bars = {}, {}
                if cached:
                    previous_evidence = {r["physical_contract"]: r for r in pl.read_parquet(prior_coverage).to_dicts()}
                    previous_bars = pl.read_parquet(prior_bars).partition_by("physical_contract", as_dict=True)
                frozen_evidence = {r["physical_contract"]: r for r in base_coverage_by_day.get(
                    (day,), pl.DataFrame()).to_dicts()}
                frozen_bars = base_bars_by_day.get((day,), pl.DataFrame(schema=MINUTE_SCHEMA)).partition_by(
                    "physical_contract", as_dict=True)
                def read_or_reuse(row):
                    if row["physical_contract"] in frozen_evidence:
                        return (frozen_bars.get((row["physical_contract"],), pl.DataFrame(schema=MINUTE_SCHEMA)),
                                frozen_evidence[row["physical_contract"]])
                    prior = previous_evidence.get(row["physical_contract"], {})
                    if _reuse_verified_contract(root, prior):
                        result = previous_bars.get((row["physical_contract"],), pl.DataFrame(schema=MINUTE_SCHEMA)), prior
                    else:
                        result = _contract_day(row, root, cutoff)
                    result = _enforce_observed_volume_bound(row, *result)
                    if recovery:
                        bars, evidence = result
                        if evidence['status'] not in ACCEPTED:
                            bars, evidence = recovery.recover(row, bars, evidence)
                        bars, evidence = _enforce_observed_volume_bound(row, bars, evidence)
                        if evidence['status'] not in ACCEPTED and getattr(args, "finmind_ticks_root", None):
                            from stockagent.data.tw_futures_finmind_ticks import read_finmind_contract_day
                            bars, evidence = read_finmind_contract_day(
                                Path(args.finmind_ticks_root), row,
                                recovery.official.get((row["date"], row["physical_contract"])),
                            )
                            bars, evidence = _enforce_observed_volume_bound(row, bars, evidence)
                        if evidence.get('repair_kind') and evidence['status'] == 'minute_verified':
                            evidence['tick_volume'] = int(bars['volume'].sum())
                        for key in ('repair_kind', 'repair_data_path', 'repair_receipt_path', 'official_reason', 'non_execution_quarantine'):
                            evidence.setdefault(key, '')
                        if scope_fingerprint:
                            evidence.setdefault('source_partitions', '')
                        evidence.setdefault('outright_volume', None)
                        return bars, evidence
                    return result
                results = list(pool.map(read_or_reuse, rows))
                if recovery or base_receipt or scope_fingerprint:
                    for _, evidence in results:
                        for key in ('repair_kind', 'repair_data_path', 'repair_receipt_path', 'official_reason', 'non_execution_quarantine'):
                            evidence.setdefault(key, '')
                        if scope_fingerprint:
                            evidence.setdefault('source_partitions', '')
                        evidence.setdefault('outright_volume', None)
                bars = pl.concat([r[0] for r in results]) if results else pl.DataFrame(schema=MINUTE_SCHEMA)
                coverage = pl.DataFrame([r[1] for r in results], infer_schema_length=None).with_columns(
                    pl.col("tick_volume", "tick_rows", "official_volume").cast(pl.Int64)) if results else pl.DataFrame()
                if (recovery or base_receipt or scope_fingerprint) and coverage.height:
                    if scope_fingerprint:
                        coverage = coverage.with_columns(pl.col('source_partitions').cast(pl.String))
                    coverage = coverage.with_columns(pl.col('outright_volume').cast(pl.Int64)).select(
                        'date','physical_contract','status','alias','source_file_sha256','receipt_sha256',
                        'official_volume','tick_volume','tick_rows','detail','source_row_observed',
                        'repair_kind','repair_data_path','repair_receipt_path','official_reason',
                        'non_execution_quarantine','outright_volume',
                        *(['source_partitions'] if scope_fingerprint else []))
                if scope_fingerprint and not coverage.height:
                    coverage = pl.DataFrame(schema={
                        'date': pl.Date, 'physical_contract': pl.String, 'status': pl.String,
                        'alias': pl.String, 'source_file_sha256': pl.String, 'receipt_sha256': pl.String,
                        'official_volume': pl.Int64, 'tick_volume': pl.Int64, 'tick_rows': pl.Int64,
                        'detail': pl.String, 'source_row_observed': pl.Boolean,
                        **{k: pl.String for k in ('repair_kind', 'repair_data_path', 'repair_receipt_path', 'official_reason', 'non_execution_quarantine')},
                        'outright_volume': pl.Int64,
                        'source_partitions': pl.String,
                    })
                atomic_write_parquet(cache_bars, bars)
                atomic_write_parquet(cache_coverage, coverage)
                shard_receipt = {"complete": bool(rows) and coverage["status"].is_in(ACCEPTED).all(),
                                               "price_rule_contract_version": TW_DERIVATIVE_PRICE_CONTRACT_VERSION,
                                               "bars_sha256": sha256_file(cache_bars), "coverage_sha256": sha256_file(cache_coverage)}
                if scope_fingerprint:
                    shard_receipt.update(scope_fingerprint=scope_fingerprint, expected_keys_sha256=expected_key_digest)
                atomic_write_json(receipt_path, shard_receipt)
            if scope_fingerprint:
                actual_keys = pl.read_parquet(cache_coverage, columns=["date", "physical_contract"])
                if actual_keys.is_duplicated().any() or _selection_sha256(actual_keys) != expected_key_digest:
                    raise ValueError(f"cached coverage differs from requested physical contract-days: {day}")
            frames.append(cache_bars)
            inventories.append(cache_coverage)
            if index % 20 == 0 or index == len(expected_dates):
                progress = {"status": "building", "sessions_done": index, "sessions_total": len(expected_dates), "date": str(day)}
                atomic_write_json(output / "build_progress.json", progress)
                print(json.dumps(progress), flush=True)
    atomic_write_json(output / "build_progress.json", {
        "status": "assembling", "sessions_done": len(expected_dates), "sessions_total": len(expected_dates),
    })
    # One multi-file scan avoids thousands of nested union query plans.
    full = pl.scan_parquet(frames).collect(engine="streaming")
    coverage = pl.scan_parquet(inventories).collect(engine="streaming")
    if scope == "all_futures_intraday" and _volume_bound_violations(coverage, full, selected, official).height:
        raise ValueError("accepted minute source volume exceeds official capacity; revalidate contradictory source keys")
    if scope_fingerprint and (coverage.select("date", "physical_contract").is_duplicated().any()
                              or _selection_sha256(coverage) != _selection_sha256(selected)):
        raise ValueError("assembled coverage differs from the exact requested physical contract-days")
    gaps = coverage.filter(~pl.col("status").is_in(ACCEPTED))
    complete_dates = sorted(set(map(str, expected_dates)) - set(map(str, gaps["date"].to_list())))
    quarantine = sorted(set(getattr(args, 'quarantine_dates', None) or []))
    if set(quarantine) - set(map(str, expected_dates)):
        raise ValueError('quarantined dates must belong to the requested calendar')
    contract_days = normalize_contract_days(getattr(args, 'quarantine_contract_days', None) or [])
    if any(item['date'] not in set(map(str, expected_dates)) or item['date'] in quarantine for item in contract_days):
        raise ValueError('contract-day quarantine must belong to requested non-quarantined calendar')
    training_gaps = validate_contract_day_quarantine(coverage, contract_days, accepted=ACCEPTED).filter(
        ~pl.col('date').cast(pl.String).is_in(quarantine))
    output.mkdir(parents=True, exist_ok=True)
    outputs = {}
    for key, filename, frame in (("all_minutes", "all_minutes.parquet", full),
                                  ("minutes", "minutes.parquet", full.filter(pl.col("minute").is_in(EVENT_MINUTES))),
                                  ("coverage", "coverage.parquet", coverage), ("gaps", "gaps.parquet", gaps)):
        path = output / filename
        atomic_write_parquet(path, frame)
        outputs[key] = {"file": filename, "sha256": sha256_file(path), "rows": frame.height}
    if sha256_file(args.daily_data_path) != daily_digest:
        raise ValueError("daily source changed during build; outputs cannot be accepted")
    manifest = {"dataset": HISTORY_DATASET, "contract_version": HISTORY_VERSION, "source_kind": HISTORY_SOURCE,
                "price_rule_contract_version": TW_DERIVATIVE_PRICE_CONTRACT_VERSION,
                "status": ("partial" if training_gaps.height else "complete_with_quarantine" if quarantine or contract_days else "complete"), "source_daily_sha256": daily_digest,
                "quarantined_dates": quarantine,
                "daily_proxy_before": str(cutoff), "covered_dates": complete_dates,
                "requested_dates": list(map(str, expected_dates)), "outputs": outputs,
                "counts": dict(coverage.group_by("status").len().iter_rows()),
                "mapping": "dated_observed_monthly_R1_then_OHLC_verification_not_query_time_target",
                "capacity": "observed_tick_volume_no_scaling_to_official_daily_volume",
                "daily_proxy_caveat": "daily_session_open_to_close_not_0846_or_1330_executable_fills"}
    if contract_days:
        manifest.update(
            quarantined_contract_days=contract_days,
            contract_day_quarantine_version=CONTRACT_DAY_QUARANTINE_VERSION,
            contract_day_quarantine_policy=CONTRACT_DAY_QUARANTINE_POLICY,
            usable_dates=sorted(set(map(str, expected_dates)) - set(quarantine)
                                - set(map(str, training_gaps['date'].to_list()))),
        )
    if recovery or base_official is not None:
        from stockagent.data.tw_stock_futures_repair import REPAIR_SOURCE
        if base_official is not None:
            official = (base_official if official is None else pl.concat([
                official,
                base_official.join(official.select('date', 'physical_contract'),
                                   on=['date', 'physical_contract'], how='anti'),
            ], how='diagonal_relaxed'))
        atomic_write_parquet(output/'official_evidence.parquet', official)
        outputs['official_evidence'] = dict(file='official_evidence.parquet', sha256=sha256_file(output/'official_evidence.parquet'), rows=official.height)
        manifest.update(source_kind=REPAIR_SOURCE, repair_contract_version=1,
                        capacity_participation=getattr(args, 'capacity_participation', None),
                        official_evidence=evidence_receipt,
                        mapping='dated_R1_OHLC_or_exact_physical_month_with_official_outright_evidence',
                        capacity='observed_source_volume_only; official_spread_only_or_empty_days_have_zero_outright_capacity')
        if getattr(args, 'capacity_rounding', 'floor') != 'floor':
            manifest['capacity_rounding'] = args.capacity_rounding
    if scope_fingerprint:
        manifest.update(
            source_scope=scope, source_scope_version=ALL_FUTURES_SOURCE_SCOPE_VERSION, scope_fingerprint=scope_fingerprint,
            product_history_contract_version=TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION,
            expected_contract_days=selected.height, expected_keys_sha256=_selection_sha256(selected),
            continuous_tick_roots=list(map(str, root)),
        )
    if scope == "all_futures_intraday":
        manifest.update(source_kind=MULTISOURCE_INTRADAY_SOURCE,
                        mapping='dated_physical_calendar_then_product_session_grid_and_price_verification',
                        official_volume_bounds_complete=recovery is not None)
    if base_receipt:
        manifest.update(base_bundle=base_receipt,
                        all_minutes_scope='base_scheduled_event_minutes_plus_new_observed_full_session_minutes')
    manifest_path = output / "manifest.json"
    previous_manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else None
    # Preserve the exact manifest bytes for an identical inventory. JSON key
    # ordering must not invalidate checkpoints or advance a timestamp-only head.
    if previous_manifest != manifest:
        atomic_write_json(manifest_path, manifest, sort_keys=True)
    atomic_write_json(output / "build_progress.json", {
        "status": "assembled", "training_coverage_status": manifest["status"],
        "sessions_done": len(expected_dates), "sessions_total": len(expected_dates),
    })
    print(json.dumps({k: manifest[k] for k in ("status", "counts", "daily_proxy_before")}), flush=True)
    return 2 if training_gaps.height else 0
