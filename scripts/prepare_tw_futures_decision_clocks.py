#!/usr/bin/env python3
"""Prepare paired 08:45/09:00 carry entries without daily-OPEN substitution.

Reuse receipt-verified full-session minutes and exact-contract KBar readers.
Missing contract-days are emitted as repair work and block both experiments.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_decision_clock import (
    CLOCK_DATASET, OPENING_REFERENCE_DATASET, decision_clock_contract,
    opening_reference_contract, resolve_opening_references,
)
from stockagent.data.tw_stock_futures_history import _reuse_verified_contract
from stockagent.data.tw_stock_futures_kbars import read_futures_kbar_sources


def load_bulk_minutes(root, block_manifest, tick_roots, source, *, progress, workers=4):
    """Read each complete global object once instead of one file per contract.

    Full-session quantity/HLC certificates precede the small two-window view.
    Independently evidenced block-only days remain eligible but cannot fill.
    """
    from concurrent.futures import ThreadPoolExecutor
    import time
    from stockagent.data.tw_futures_finmind_ticks import (
        BULK_SOURCE, BULK_PRICE_POLICY, pin_bulk_tick_inventory, read_bulk_stock_tick_day, stock_print_views,
    )
    from stockagent.data.tw_stock_futures_repair import apply_block_evidence
    from stockagent.data.tw_stock_futures_history import _read_verified_tick_partition, normalize_continuous_ticks
    selected = source.filter(pl.col('executable'))
    block_sha = sha256_file(block_manifest)
    facts, block_proof = apply_block_evidence(selected, Path(block_manifest))
    by_day = {str(f['date'][0]): f for f in facts.partition_by('date')}
    sources = pin_bulk_tick_inventory(Path(root), sorted(facts['date'].unique().to_list()))
    started = time.monotonic()
    minutes, coverage, trades = [], [], []
    def inspect(item):
        bars, days, events = read_bulk_stock_tick_day(item, by_day[item['date']])
        return bars.filter(pl.col('minute').is_in([527, 542])), days, events
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, (bars, days, events) in enumerate(pool.map(inspect, sources), 1):
            minutes.append(bars); coverage.append(days); trades.append(events)
            if i == 1 or i % 50 == 0 or i == len(sources):
                progress(stage='bulk_tick_readback', dates_checked=i, dates_total=len(sources),
                    elapsed_s=round(time.monotonic()-started, 3))
    minutes, coverage, trades = [pl.concat(items, how='vertical_relaxed') for items in (minutes, coverage, trades)]
    complements = []
    pending = facts.join(coverage.filter(~pl.col('verified')).select('date', 'physical_contract'),
        on=['date', 'physical_contract'], how='semi')
    # R1 is admitted only for an independently mapped dated near contract, not
    # by comparing a current broker alias or stripping lifetime suffixes.
    for row in pending.iter_rows(named=True):
        if row['tenor_rank'] != 1 or row['outright_volume'] <= 0:
            continue
        alias = row['product'] + 'R1'
        for tick_root in tick_roots:
            raw, proof, status = _read_verified_tick_partition(Path(tick_root), alias, row['date'])
            if status != 'complete':
                continue
            bars, stats = normalize_continuous_ticks(raw, day=row['date'], alias=alias,
                physical=row['physical_contract'], digest=proof['data_sha256'],
                product=row['product'], asset_class=row['asset_class'])
            if (stats['tick_volume'] != row['outright_volume'] or any(
                    abs(stats['tick_'+c]-row[c]) > 1e-6 for c in ('high', 'low', 'close'))):
                continue
            prints = raw.filter(pl.col('event_ts').dt.date() == row['date']).with_columns(
                (pl.col('event_ts').dt.hour().cast(pl.Int32)*3600 + pl.col('event_ts').dt.minute().cast(pl.Int32)*60
                 + pl.col('event_ts').dt.second().cast(pl.Int32)).alias('_second')).filter(
                    pl.col('_second').is_between(525*60, 825*60)).with_columns(
                        pl.lit(row['physical_contract']).alias('physical_contract'), pl.col('close').alias('price'))
            _, _, events = stock_print_views(prints, day=row['date'], source_sha256=proof['data_sha256'])
            minutes = pl.concat([minutes, bars.filter(pl.col('minute').is_in([527,542]))], how='vertical_relaxed')
            trades = pl.concat([trades, events], how='vertical_relaxed')
            match = (pl.col('date') == row['date']) & (pl.col('physical_contract') == row['physical_contract'])
            updates = {'verified': True, 'source_file_sha256': proof['data_sha256'], **stats}
            coverage = coverage.with_columns(*[pl.when(match).then(pl.lit(v)).otherwise(pl.col(k)).alias(k)
                for k,v in updates.items() if k in coverage.columns])
            complements.append({**proof, 'date': str(row['date']), 'physical_contract': row['physical_contract'],
                'mapping': 'dated_official_unique_product_tenor1_then_exact_full_book_quantity_and_hlc',
                'official_expiry': str(row['official_expiry']), 'tick_volume': stats['tick_volume']})
            break
    if sha256_file(block_manifest) != block_sha:
        raise ValueError('official block inventory changed during preparation')
    verified = coverage.filter(pl.col('verified')).select('date','physical_contract')
    proof = dict(source=BULK_SOURCE, price_policy=BULK_PRICE_POLICY,
        quantity='native_even_double_sided_divided_by_two_no_dedup',
        session='dated_product_rules_inclusive_exact_close_no_night_or_spread',
        admission='exact_continuous_book_quantity_and_official_hlc_or_independent_official_zero_book',
        block_manifest=dict(path=str(block_manifest),sha256=block_sha,receipt=block_proof),
        objects=sources, complementary_ticks=complements)
    return minutes, verified, proof, coverage, trades


def paired_entries(source, minutes, verified_days):
    """Pure alignment: execution availability never changes policy eligibility."""
    keys = ["date", "physical_contract"]
    if source.select(keys).is_duplicated().any() or minutes.select(*keys, "minute").is_duplicated().any():
        raise ValueError("duplicate daily/minute identity")
    proof = verified_days.select(keys).unique().with_columns(pl.lit(True).alias("_verified"))
    result = {}
    for clock in ("08:45", "09:00"):
        window = decision_clock_contract(clock)
        observed = minutes.filter(pl.col("minute") == window["window_end_minute"]).select(
            *keys, pl.col("vwap").alias("entry_quote"), pl.col("volume").alias("entry_volume"),
            "source_file_sha256")
        rows = source.select(*keys, "symbol", "executable").join(proof, on=keys, how="left", validate="1:1")
        rows = rows.join(observed, on=keys, how="left", validate="1:1").with_columns(
            pl.when(~pl.col("executable")).then(pl.lit("daily_not_executable"))
            .when(~pl.col("_verified").fill_null(False)).then(pl.lit("missing_source"))
            .when(pl.col("entry_volume").fill_null(0) > 0).then(pl.lit("observed_minute"))
            .otherwise(pl.lit("verified_no_window_trade")).alias("status"))
        rows = rows.with_columns(
            pl.when(pl.col("status") == "observed_minute").then(pl.col("entry_quote"))
              .otherwise(None).cast(pl.Float64).alias("entry_quote"),
            pl.when(pl.col("status") == "observed_minute").then(pl.col("entry_volume"))
              .otherwise(0.).cast(pl.Float64).alias("entry_volume"))
        result[clock] = rows.drop("executable", "_verified").sort(keys)
    return result


def paired_opening_references(source, minutes, verified_days):
    """Use only real prints; official marks never become fallback trades.

    Daily closing prints have date precision, so retain a nullable trade_ts and
    a separately labelled completed-session availability bound. A current daily
    OPEN is model-visible only once its first verified minute is complete.
    """
    keys = ["date", "physical_contract"]
    closes = source.filter(pl.col("observed_close").is_finite()
        & (pl.col("observed_close") > 0) & (pl.col("volume") > 0)
        & pl.col("source_row_observed")).select(
        "physical_contract", pl.col("observed_close").alias("price"),
        pl.col("date").alias("trade_date"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("trade_ts"),
        (pl.col("date").cast(pl.Datetime("us")) + pl.duration(days=1)
            - pl.duration(microseconds=1)).alias("available_by"),
        pl.col("official_source_sha256").alias("source_sha256"),
        pl.lit(False).alias("is_session_open"))
    prefix = minutes.filter((pl.col("volume") > 0) & (pl.col("minute") <= 540)).join(
        verified_days.select(keys).unique(), on=keys, how="semi")
    minute_trades = prefix.select("physical_contract", pl.col("close").alias("price"),
        pl.col("date").alias("trade_date"),
        pl.lit(None, dtype=pl.Datetime("us")).alias("trade_ts"),
        (pl.col("date").cast(pl.Datetime("us")) + pl.duration(minutes=pl.col("minute"))).alias("available_by"),
        pl.col("source_file_sha256").alias("source_sha256"), pl.lit(False).alias("is_session_open"))
    first = prefix.sort("minute").group_by(keys, maintain_order=True).first().join(
        source.select(*keys, "observed_open"), on=keys, how="left", validate="1:1")
    # A daily opening outside the first observed minute contradicts the
    # alleged complete prefix. Do not expose it using a date-only OPEN clock.
    first = first.filter(pl.col("observed_open").is_finite() & (pl.col("observed_open") > 0)
        & (pl.col("observed_open") >= pl.col("low") - 1e-6)
        & (pl.col("observed_open") <= pl.col("high") + 1e-6))
    openings = first.select("physical_contract", pl.col("observed_open").alias("price"),
        pl.col("date").alias("trade_date"), pl.lit(None, dtype=pl.Datetime("us")).alias("trade_ts"),
        (pl.col("date").cast(pl.Datetime("us")) + pl.duration(minutes=pl.col("minute"))).alias("available_by"),
        pl.col("source_file_sha256").alias("source_sha256"), pl.lit(True).alias("is_session_open"))
    trades = pl.concat([closes, minute_trades, openings], how="vertical_relaxed")
    decisions = source.select(*keys, "symbol")
    return {clock: resolve_opening_references(decisions, trades, decision_time=clock)
            for clock in ("08:45", "09:00")}


def load_verified_history(history_root, tick_roots, source, *, progress=None, include_reference_minutes=False):
    """Rebind old minute facts to new official rows through explicit identity checks.

    The historical bundle may be partial and bound to an older daily release.
    Its accepted rows are reused only after raw receipt/SHA readback and matching
    the new physical contract's observed high/low/close and volume upper bound.
    No source digest is rewritten to pretend that the two releases are equal.
    """
    root = Path(history_root)
    manifest_path = root / "manifest.json"
    manifest_sha = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("dataset") != "taifex_stock_futures_minute_history_v2" or manifest.get("contract_version") != 2:
        raise ValueError("unsupported historical minute bundle")
    proofs = []
    member_stats = {}
    for key in ("all_minutes", "coverage"):
        item = manifest["outputs"][key]
        relative = Path(item["file"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe historical minute member")
        path = root / relative
        member_stats[path] = path.stat()
        digest = sha256_file(path)
        if digest != item["sha256"]:
            raise ValueError("historical minute member SHA mismatch")
        proofs.append({"path": str(path), "sha256": digest})
    coverage_path = Path(proofs[1]["path"])
    minute_path = Path(proofs[0]["path"])
    keys = ["date", "physical_contract"]
    # The new release has lifetime/generation IDs; the historical producer
    # used product:delivery. Join through explicit dated product/delivery cells,
    # and reject ambiguity instead of stripping a physical-identity suffix.
    mapping = source.select("date", "physical_contract",
        (pl.col("product") + pl.lit(":") + pl.col("contract")).alias("_minute_identity"))
    if mapping.select("date", "_minute_identity").is_duplicated().any():
        raise ValueError("ambiguous dated product/delivery to physical lifetime")
    coverage = pl.read_parquet(coverage_path).filter(pl.col("status") == "minute_verified").rename(
        {"physical_contract": "_minute_identity"}).join(mapping, on=["date", "_minute_identity"], how="inner")
    def selected_minutes():
        return pl.scan_parquet(minute_path).filter(pl.col("date").is_between(
            source["date"].min(), source["date"].max())).rename({"physical_contract": "_minute_identity"}).join(
                mapping.lazy(), on=["date", "_minute_identity"], how="inner").drop("_minute_identity")
    summaries = selected_minutes().group_by(keys).agg(
        pl.col("high").max().alias("_high"), pl.col("low").min().alias("_low"),
        pl.col("close").sort_by("minute").last().alias("_close"),
        pl.col("volume").sum().alias("_volume")).collect()
    coverage = coverage.join(summaries, on=keys, how="left", validate="1:1").join(
        source.select(*keys, "high", "low", "close", "volume"), on=keys, how="left", validate="1:1")
    coverage = coverage.filter((pl.col("_high") - pl.col("high")).abs().le(1e-6)
        & (pl.col("_low") - pl.col("low")).abs().le(1e-6)
        & (pl.col("_close") - pl.col("close")).abs().le(1e-6)
        & (pl.col("_volume") > 0) & (pl.col("_volume") <= pl.col("volume")))
    accepted = []
    for i, row in enumerate(coverage.iter_rows(named=True)):
        if _reuse_verified_contract(tick_roots, {**row, "physical_contract": row["_minute_identity"]}):
            accepted.append({k: row[k] for k in keys})
        if progress is not None and i % 1000 == 0:
            progress(stage="historical_raw_readback", checked=i, candidates=coverage.height, verified=len(accepted))
    verified = pl.DataFrame(accepted, schema={"date": pl.Date, "physical_contract": pl.String})
    wanted_minutes = [decision_clock_contract(c)["window_end_minute"] for c in ("08:45", "09:00")]
    minute_filter = pl.col("minute").is_in(wanted_minutes)
    if include_reference_minutes:
        minute_filter |= pl.col("minute") <= 540
    minutes = selected_minutes().filter(minute_filter).collect().join(
        verified, on=keys, how="semi")
    if sha256_file(manifest_path) != manifest_sha:
        raise ValueError("historical minute manifest changed during readback")
    for path, before in member_stats.items():
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("historical minute member changed during readback")
    return minutes, verified, {"path": str(manifest_path), "sha256": manifest_sha,
        "original_daily_sha256": manifest["source_daily_sha256"], "members": proofs,
        "rebind": "dated_product_delivery_to_unique_official_lifetime_observed_hlc_volume_bound_original_raw_sha"}


def build(args):
    from stockagent.data.tw_futures_margin import validate_margin_rule_source

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    def progress(**state):
        payload = {"contract": CLOCK_DATASET, **state}
        atomic_write_json(output / "progress.json", payload)
        print(json.dumps(payload, ensure_ascii=False), flush=True)
    progress(stage="validating_canonical_daily_and_rules")
    _, rule_manifest = validate_margin_rule_source(args.rules, args.daily)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    daily_scan = pl.scan_parquet(args.daily)
    alias_columns = ["shioaji_roots"] if "shioaji_roots" in daily_scan.collect_schema().names() else []
    bulk_columns = ['outright_volume','tenor_rank','official_expiry'] if args.finmind_bulk_root else []
    reference_columns = (["observed_open", "observed_close", "source_row_observed", "official_source_sha256"]
                         if args.with_opening_reference else [])
    all_source = daily_scan.filter(pl.col("date") <= end).select(
        "date", "product", "contract", "asset_class", "physical_contract", "symbol", "open", "high", "low", "close",
        "volume", "executable", *alias_columns, *reference_columns, *bulk_columns).collect()
    source = all_source.filter(pl.col("date") >= start)
    if not source.height or source.filter(pl.col("asset_class") != "stock_future").height:
        raise ValueError("paired clocks require a nonempty stock-only source release")
    daily_sha, rules_sha = sha256_file(args.daily), sha256_file(args.rules)
    reference_trades = None
    if args.finmind_bulk_root:
        if not args.block_manifest:
            raise ValueError('bulk print admission requires independent official block evidence')
        progress(stage='bulk_tick_inventory', contract_days=source.height)
        minutes, verified, history_proof, coverage, reference_trades = load_bulk_minutes(
            args.finmind_bulk_root, args.block_manifest, args.tick_root, source,
            progress=progress, workers=args.source_workers)
        atomic_write_parquet(output / 'source_coverage.parquet', coverage)
    else:
        progress(stage="historical_minute_validation", contract_days=source.height)
        minutes, verified, history_proof = load_verified_history(args.history_root, args.tick_root, source,
            progress=progress, include_reference_minutes=args.with_opening_reference)
    sources, invalid_source_days = [], []
    missing = source.filter(pl.col("executable")).join(verified, on=["date", "physical_contract"], how="anti")
    for root in args.exact_kbar_root:
        if not missing.height:
            break
        progress(stage="exact_contract_kbars", root=str(root), remaining=missing.height)
        new_minutes, receipts, gaps = read_futures_kbar_sources(Path(root), missing,
            sorted(source["date"].unique().to_list()), all_futures=True, full_session=True,
            invalid_source_days=invalid_source_days)
        atomic_write_json(output / "rejected_kbar_source_days.json", {
            "source_days": invalid_source_days, "count": len(invalid_source_days),
            "policy": "unmodified_raw_retained_invalid_days_not_admitted"})
        # A provider must not create more outright contracts than the official
        # session. Missing data is a repair requirement, never a zero volume.
        totals = new_minutes.group_by("date", "physical_contract").agg(
            pl.col("volume").sum().alias("_v"), pl.col("high").max().alias("_high"),
            pl.col("low").min().alias("_low"), pl.col("close").sort_by("minute").last().alias("_close"))
        days = missing.select("date", "physical_contract", "volume", "high", "low", "close").join(totals,
            on=["date", "physical_contract"], how="inner").filter((pl.col("_v") > 0)
                & (pl.col("_v") <= pl.col("volume"))
                & ((pl.col("_high") - pl.col("high")).abs() <= 1e-6)
                & ((pl.col("_low") - pl.col("low")).abs() <= 1e-6)
                & ((pl.col("_close") - pl.col("close")).abs() <= 1e-6))
        # Receipts establish completed same-day coverage, not merely the
        # presence of the requested entry minute.
        missing_keys = {(str(g["date"]), g["physical_contract"]) for g in gaps}
        days = days.filter(pl.Series([ (str(d), p) not in missing_keys
            for d, p in days.select("date", "physical_contract").iter_rows()])).select("date", "physical_contract")
        new_minutes = new_minutes.join(days, on=["date", "physical_contract"], how="semi").filter(
            pl.col("minute").is_in([527, 542]) | ((pl.col("minute") <= 540) & args.with_opening_reference))
        minutes = pl.concat([minutes, new_minutes], how="vertical_relaxed")
        verified = pl.concat([verified, days]).unique()
        sources.extend(receipts)
        missing = missing.join(days, on=["date", "physical_contract"], how="anti")
    clocks = paired_entries(source, minutes, verified)
    references = {}
    if args.with_opening_reference:
        references = ({clock: resolve_opening_references(source.select('date','physical_contract','symbol'),
            reference_trades, decision_time=clock) for clock in ('08:45','09:00')}
            if reference_trades is not None else paired_opening_references(all_source, minutes, verified))
    gaps = missing.select("date", "product", "contract", "asset_class", "physical_contract", "symbol", "volume").with_columns(
        pl.lit("source_evidence_missing").alias("status"), pl.col("volume").alias("outright_volume"))
    atomic_write_parquet(output / "coverage_gaps.parquet", gaps)
    identity = {"daily": daily_sha, "rules": rules_sha, "start": str(source["date"].min()),
        "end": str(source["date"].max()), "historical_source": history_proof,
        "exact_sources": sources, "clocks": [decision_clock_contract(c) for c in clocks]}
    if references:
        identity["opening_reference_contracts"] = [opening_reference_contract(c) for c in references]
        identity['reference_observations'] = ('verified_actual_tick_first_and_last_with_minute_completion_bounds'
            if reference_trades is not None else 'verified_minute_and_prior_official_observed_close')
    pair_identity = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
    for clock, entries in clocks.items():
        directory = output / clock.replace(":", "")
        directory.mkdir(exist_ok=True)
        path = directory / "entries.parquet"
        atomic_write_parquet(path, entries)
        atomic_write_json(directory / "manifest.json", {
            "dataset": CLOCK_DATASET, "status": "complete" if not gaps.height else "partial",
            "clock": decision_clock_contract(clock), "pair_identity": pair_identity,
            "entries_sha256": sha256_file(path), "rows": entries.height,
            "source_daily_sha256": daily_sha, "source_rules_sha256": rules_sha,
            "start_date": identity["start"], "end_date": identity["end"],
            "sources": identity, "coverage_gaps": gaps.height,
            "counts": dict(entries.group_by("status").len().iter_rows()),
        })
        if clock in references:
            ref_path = directory / "opening_reference.parquet"
            refs = references[clock].filter(pl.col("date").is_between(start, end))
            atomic_write_parquet(ref_path, refs)
            atomic_write_json(directory / "opening_reference_manifest.json", {
                "dataset": OPENING_REFERENCE_DATASET, "status": "complete" if not gaps.height else "partial",
                "contract": opening_reference_contract(clock), "pair_identity": pair_identity,
                "source_daily_sha256": daily_sha, "data_sha256": sha256_file(ref_path), "rows": refs.height,
                "counts": dict(refs.group_by("reference_kind").len().iter_rows()),
            })
    if sha256_file(args.daily) != daily_sha or sha256_file(args.rules) != rules_sha:
        raise ValueError("canonical daily/rules changed during entry preparation")
    progress(stage="complete" if not gaps.height else "blocked_source_coverage",
        contract_days=source.height, verified_intraday_contract_days=verified.height,
        missing_executable_contract_days=gaps.height, pair_identity=pair_identity)
    return 0 if not gaps.height else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--daily", required=True)
    p.add_argument("--rules", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--start", default="2020-03-23")
    p.add_argument("--end", default="2026-09-24")
    p.add_argument("--history-root", default="data_tw_futures/taifex_stock_futures_minute_history_v2")
    p.add_argument("--tick-root", type=Path, action="append", default=[])
    p.add_argument("--exact-kbar-root", type=Path, action="append", default=[])
    p.add_argument('--finmind-bulk-root', type=Path,
                   help='Use the canonical existing global tick collector, all delivery months')
    p.add_argument('--block-manifest', type=Path, help='Independent official continuous-book quantity evidence')
    p.add_argument('--source-workers', type=int, choices=range(1,9), default=4)
    p.add_argument("--with-opening-reference", action="store_true",
                   help="Add causal observed-open/last-real-trade inputs with separate clocks and source hashes")
    args = p.parse_args()
    if not args.tick_root:
        args.tick_root = [Path("data_tw_futures/shioaji_history")]
    return build(args)


if __name__ == "__main__":
    raise SystemExit(main())
