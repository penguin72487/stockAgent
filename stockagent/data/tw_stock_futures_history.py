"""Receipt-bound continuous ticks -> dated physical contracts -> minute facts.

R1's current resolved target is never historical identity. The official dated
monthly calendar determines the mapping; observed OHLC independently checks it.
All prices remain Taipei wall time. Source gaps are evidence, never zero trades.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_stock_futures_day_trade import select_causal_front_stock_futures_candidates
from stockagent.data.tw_stock_futures_minute import EVENT_MINUTES
from stockagent.data.tw_stock_futures_quarantine import (
    CONTRACT_DAY_QUARANTINE_VERSION, CONTRACT_DAY_QUARANTINE_POLICY,
    normalize_contract_days, validate_contract_day_quarantine,
)

HISTORY_SOURCE = "shioaji_continuous_ticks_dated_physical_v1"
HISTORY_DATASET = "taifex_stock_futures_minute_history_v2"
HISTORY_VERSION = 2
ACCEPTED = {"minute_verified", "daily_open_close_proxy", "official_no_outright_trades", "official_subcontract_capacity"}
MINUTE_SCHEMA = {"date": pl.Date, "physical_contract": pl.String, "minute": pl.Int32,
                 **{c: pl.Float64 for c in ("vwap", "high", "low", "close", "volume")},
                 "source_file_sha256": pl.String}


def normalize_continuous_ticks(frame: pl.DataFrame, *, day: date, alias: str,
                               physical: str, digest: str) -> tuple[pl.DataFrame, dict]:
    required = {"event_ts", "ts", "close", "volume", "trading_date", "query_contract", "source_row_index"}
    if not required <= set(frame.columns) or frame.is_empty():
        raise ValueError("missing tick schema or empty complete receipt")
    if frame.filter((pl.col("trading_date") != day) | (pl.col("query_contract") != alias)).height:
        raise ValueError("tick query identity differs from receipt")
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError("null tick identity/price")
    if frame.filter(pl.col("event_ts").cast(pl.Int64) != pl.col("ts")).height:
        raise ValueError("tick event timestamp differs from wall-clock ts")
    session = frame.filter(
        (pl.col("event_ts").dt.date() == day)
        & (pl.col("event_ts").dt.hour().cast(pl.Int32) * 60
           + pl.col("event_ts").dt.minute().cast(pl.Int32)).is_between(525, 825)
    ).sort("event_ts", "source_row_index")
    if session.is_empty():
        raise ValueError("no day-session trades")
    if session.filter(~pl.col("close").is_finite() | (pl.col("close") <= 0)
                      | (pl.col("volume") <= 0)).height:
        raise ValueError("invalid day-session price/volume")
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


def _contract_day(row: dict, root: Path, cutoff: date) -> tuple[pl.DataFrame, dict]:
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
        receipt_path = root / alias / "receipts" / f"trading_date={day}.json"
        path = root / alias / "ticks" / f"trading_date={day}" / "data.parquet"
        try:
            raw_receipt = receipt_path.read_bytes()
            receipt = json.loads(raw_receipt)
        except (OSError, ValueError):
            continue
        import hashlib
        evidence.update(alias=alias, receipt_sha256=hashlib.sha256(raw_receipt).hexdigest())
        if (receipt.get("source") != "shioaji_continuous_futures_historical_ticks_v1"
                or receipt.get("schema_version") != 1 or receipt.get("contract") != alias
                or receipt.get("trading_date") != str(day) or receipt.get("session_finalized") is False):
            evidence["status"] = "invalid_receipt"
            continue
        if receipt.get("status") == "source_empty":
            evidence["status"] = "source_empty_unresolved"
            continue
        if receipt.get("status") != "complete":
            evidence["status"] = "incomplete_receipt"
            continue
        try:
            digest = sha256_file(path)
            if digest != receipt.get("sha256"):
                raise ValueError("tick SHA differs from receipt")
            frame = pl.read_parquet(path)
            if frame.height != receipt.get("rows"):
                raise ValueError("tick rows differ from receipt")
            bars, stats = normalize_continuous_ticks(frame, day=day, alias=alias,
                                                      physical=physical, digest=digest)
            evidence.update(source_file_sha256=digest, tick_volume=stats["tick_volume"], tick_rows=stats["tick_rows"])
            if not all(np.isclose(stats[f"tick_{field}"], row[field], rtol=1e-7, atol=.011)
                       for field in ("open", "high", "low", "close")):
                evidence.update(status="ohlc_identity_mismatch", detail=json.dumps(stats, sort_keys=True))
                continue
            if sha256_file(path) != digest or receipt_path.read_bytes() != raw_receipt:
                raise ValueError("source changed during aggregation; retry")
            evidence.update(status="minute_verified", detail="calendar_and_ohlc_match; observed_tick_volume_only")
            return bars, evidence
        except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
            evidence.update(status="invalid_source", detail=str(exc)[:300])
    return empty, evidence


def _reuse_verified_contract(root: Path, evidence: dict) -> bool:
    """Cached facts are reusable only while their exact raw inputs still match."""
    if evidence.get("status") != "minute_verified":
        return False
    if evidence.get('repair_kind') in {'exact_kbars', 'exact_ticks'}:
        try:
            return (sha256_file(evidence['repair_receipt_path']) == evidence['receipt_sha256']
                    and sha256_file(evidence['repair_data_path']) == evidence['source_file_sha256'])
        except OSError:
            return False
    alias, day = evidence["alias"], evidence["date"]
    receipt = root / alias / "receipts" / f"trading_date={day}.json"
    ticks = root / alias / "ticks" / f"trading_date={day}" / "data.parquet"
    try:
        return (sha256_file(receipt) == evidence["receipt_sha256"]
                and sha256_file(ticks) == evidence["source_file_sha256"])
    except OSError:
        return False


def build_continuous_history(args, source: pl.DataFrame, expected_dates: list[date], daily_digest: str) -> int:
    """Resumable dated shards live outside the published producer tree."""
    cutoff = date.fromisoformat(args.daily_proxy_before)
    selected = select_causal_front_stock_futures_candidates(source)
    calendar = (source.filter(pl.col("source_row_observed") & pl.col("contract").str.contains(r"^\d{6}$"))
                .sort("date", "product", "contract").group_by("date", "product", maintain_order=True)
                .agg(pl.col("physical_contract").first().alias("calendar_physical")))
    selected = selected.join(calendar, on=["date", "product"], how="left", validate="m:1")
    if args.check_only:
        print(json.dumps({"status": "inventory_only", "candidates": selected.height,
                          "daily_proxy_before": str(cutoff), "source_daily_sha256": daily_digest}))
        return 0
    root, output = Path(args.shioaji_ticks_root), Path(args.output_dir)
    cache = Path(args.work_dir) / f"{daily_digest[:20]}-{cutoff}-v{HISTORY_VERSION}"
    original_cache = cache
    recovery, official = None, None
    if getattr(args, 'repair_root', None):
        from stockagent.data.tw_stock_futures_repair import ExactMinuteRecovery, apply_block_evidence
        evidence_dir = Path(args.official_evidence_dir)
        evidence_receipt = json.loads((evidence_dir/'official_evidence_manifest.json').read_text())
        official_sha = sha256_file(evidence_dir/'official_evidence.parquet')
        if evidence_receipt.get('source') != 'taifex_complete_daily_and_spread_legs_v1' or evidence_receipt.get('sha256') != official_sha:
            raise ValueError('official gap evidence SHA/source mismatch')
        for item in evidence_receipt['sources']:
            if sha256_file(item['path']) != item['sha256']:
                raise ValueError('official gap archive changed since audit')
        official = pl.read_parquet(evidence_dir/'official_evidence.parquet')
        block_manifest = evidence_dir/'block_evidence_manifest.json'
        if block_manifest.exists():
            official, block_receipt = apply_block_evidence(official, block_manifest)
            evidence_receipt['block_evidence'] = block_receipt
        recovery = ExactMinuteRecovery(Path(args.repair_root), official,
                                       participation=getattr(args, 'capacity_participation', None))
        cache = cache.with_name(cache.name + '-exact-v1-' + official_sha[:16])
    cache.mkdir(parents=True, exist_ok=True)
    frames, inventories = [], []
    refresh_dates = set(getattr(args, 'refresh_dates', None) or [])
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index, day in enumerate(expected_dates, 1):
            cache_bars, cache_coverage, receipt_path = (cache / f"{day}.{suffix}" for suffix in ("parquet", "coverage.parquet", "json"))
            # Reuse verified contract facts individually, re-read gaps, and detect
            # corrected raw data even when a dated shard was previously complete.
            saved = json.loads(receipt_path.read_text()) if receipt_path.is_file() else {}
            cached = (cache_bars.is_file() and cache_coverage.is_file()
                    and saved.get("bars_sha256") == sha256_file(cache_bars)
                    and saved.get("coverage_sha256") == sha256_file(cache_coverage))
            prior_bars, prior_coverage = cache_bars, cache_coverage
            if recovery and not cached:
                prior_bars, prior_coverage, old_receipt = (original_cache / f'{day}.{suffix}' for suffix in ('parquet','coverage.parquet','json'))
                old = json.loads(old_receipt.read_text()) if old_receipt.is_file() else {}
                cached = (prior_bars.is_file() and prior_coverage.is_file()
                          and sha256_file(prior_bars) == old.get('bars_sha256')
                          and sha256_file(prior_coverage) == old.get('coverage_sha256'))
            preserve_date = bool(refresh_dates) and str(day) not in refresh_dates
            if getattr(args, "assemble_cached", False) or preserve_date:
                if not cached or prior_bars != cache_bars:
                    raise ValueError(f"cannot assemble missing/corrupt dated shard: {day}")
            elif cached and day < cutoff:
                bars, coverage = pl.read_parquet(cache_bars), pl.read_parquet(cache_coverage)
            else:
                rows = selected.filter(pl.col("date") == day).to_dicts()
                previous_evidence, previous_bars = {}, {}
                if cached:
                    previous_evidence = {r["physical_contract"]: r for r in pl.read_parquet(prior_coverage).to_dicts()}
                    previous_bars = pl.read_parquet(prior_bars).partition_by("physical_contract", as_dict=True)
                def read_or_reuse(row):
                    prior = previous_evidence.get(row["physical_contract"], {})
                    if _reuse_verified_contract(root, prior):
                        result = previous_bars.get((row["physical_contract"],), pl.DataFrame(schema=MINUTE_SCHEMA)), prior
                    else:
                        result = _contract_day(row, root, cutoff)
                    if recovery:
                        bars, evidence = result
                        if evidence['status'] not in ACCEPTED:
                            bars, evidence = recovery.recover(row, bars, evidence)
                        if evidence.get('repair_kind') and evidence['status'] == 'minute_verified':
                            evidence['tick_volume'] = int(bars['volume'].sum())
                        for key in ('repair_kind', 'repair_data_path', 'repair_receipt_path', 'official_reason', 'non_execution_quarantine'):
                            evidence.setdefault(key, '')
                        evidence.setdefault('outright_volume', None)
                        return bars, evidence
                    return result
                results = list(pool.map(read_or_reuse, rows))
                bars = pl.concat([r[0] for r in results]) if results else pl.DataFrame(schema=MINUTE_SCHEMA)
                coverage = pl.DataFrame([r[1] for r in results], infer_schema_length=None).with_columns(
                    pl.col("tick_volume", "tick_rows", "official_volume").cast(pl.Int64)) if results else pl.DataFrame()
                if recovery and coverage.height:
                    coverage = coverage.with_columns(pl.col('outright_volume').cast(pl.Int64)).select(
                        'date','physical_contract','status','alias','source_file_sha256','receipt_sha256',
                        'official_volume','tick_volume','tick_rows','detail','source_row_observed',
                        'repair_kind','repair_data_path','repair_receipt_path','official_reason',
                        'non_execution_quarantine','outright_volume')
                atomic_write_parquet(cache_bars, bars)
                atomic_write_parquet(cache_coverage, coverage)
                atomic_write_json(receipt_path, {"complete": bool(rows) and coverage["status"].is_in(ACCEPTED).all(),
                                               "bars_sha256": sha256_file(cache_bars), "coverage_sha256": sha256_file(cache_coverage)})
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
    if recovery:
        from stockagent.data.tw_stock_futures_repair import REPAIR_SOURCE
        atomic_write_parquet(output/'official_evidence.parquet', official)
        outputs['official_evidence'] = dict(file='official_evidence.parquet', sha256=sha256_file(output/'official_evidence.parquet'), rows=official.height)
        manifest.update(source_kind=REPAIR_SOURCE, repair_contract_version=1,
                        capacity_participation=getattr(args, 'capacity_participation', None),
                        official_evidence=evidence_receipt,
                        mapping='dated_R1_OHLC_or_exact_physical_month_with_official_outright_evidence',
                        capacity='observed_source_volume_only; official_spread_only_or_empty_days_have_zero_outright_capacity')
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
