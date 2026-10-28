#!/usr/bin/env python3
"""Separate historical inventory, dated observations and executable window coverage.

This reads each projected daily source once. Counts are unions over explicitly
declared dates, never a claim that every historical code can trade concurrently.
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_decision_clock import validate_clock_source


def audit_source_inventory(requested, raw, daily, blockers, *, start, end):
    """Reconcile every requested code, preserving missing-volume semantics.

    A missing print is an evidence gap, not proof a product never traded.
    Financial flags and the historical order-capacity policy are independent
    columns. Neither can be silently relabelled as an external source limit.
    """
    start, end = map(date.fromisoformat, (start, end))
    if start > end or requested["product"].null_count() or requested["product"].is_duplicated().any():
        raise ValueError("source inventory requires unique requested codes and an ordered horizon")
    codes = requested["product"].to_list()
    raw = raw.filter(pl.col("product").is_in(codes) & pl.col("date").is_between(start, end)
        & (pl.col("session") == "一般") & pl.col("contract").str.contains(r"^\d{6}(?:W[1-5])?$"))
    keys = ["date", "product", "contract"]
    if raw.select(keys).is_duplicated().any():
        raise ValueError("raw inventory repeats a regular-session physical observation")
    positive = pl.col("volume").is_finite() & (pl.col("volume") > 0)
    complete = positive & pl.all_horizontal(
        pl.col(c).is_finite() & (pl.col(c) > 0) for c in ("open", "high", "low", "close"))
    original = raw.group_by("product").agg(
        pl.len().alias("raw_rows"), pl.col("date").min().alias("raw_first"),
        pl.col("date").max().alias("raw_last"),
        positive.fill_null(False).sum().alias("raw_positive_volume_rows"),
        complete.fill_null(False).sum().alias("raw_complete_trade_rows"),
        pl.col("volume").is_null().sum().alias("raw_null_volume_rows"),
        pl.col("volume").eq(0).sum().alias("raw_explicit_zero_volume_rows"),
        pl.col("settlement").is_finite().sum().alias("raw_settlement_rows"))
    admitted = daily.filter(pl.col("date").is_between(start, end)).group_by("product").agg(
        pl.len().alias("admitted_rows"),
        pl.col("executable").sum().alias("admitted_daily_trade_rows"))
    if blockers.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("financial inventory repeats an accounting coordinate")
    flags = [c for c, t in blockers.schema.items() if t == pl.Boolean
             and c not in {"is_warmup", "has_blocker"}]
    current = blockers.filter(pl.col("date").is_between(start, end) & ~pl.col("is_warmup"))
    counts = current.group_by("product").agg(pl.len().alias("financial_account_days"),
        pl.col("has_blocker").sum().alias("blocked_financial_days"),
        *[pl.col(c).sum().alias("blocked_" + c) for c in flags])
    result = requested.join(original, on="product", how="left", validate="1:1").join(
        admitted, on="product", how="left", validate="1:1").join(counts, on="product", how="left", validate="1:1")
    metrics = [c for c in result.columns if c not in requested.columns and c not in {"raw_first", "raw_last"}]
    result = result.with_columns(pl.col(c).fill_null(0) for c in metrics).with_columns(
        pl.when(pl.col("raw_rows") == 0).then(pl.lit("no_source_row_in_horizon"))
        .when(pl.col("raw_positive_volume_rows") == 0).then(pl.lit("no_positive_trade_evidence"))
        .when(pl.col("admitted_daily_trade_rows") == 0).then(pl.lit("positive_source_not_admitted"))
        .when(pl.col("blocked_financial_days") > 0).then(pl.lit("partially_admitted_financial_gaps"))
        .otherwise(pl.lit("admitted_trade_evidence")).alias("source_status"))
    summary = dict(requested_codes=requested.height, horizon_start=str(start), horizon_end=str(end),
        codes_with_source=result.filter(pl.col("raw_rows") > 0).height,
        codes_with_positive_trade_evidence=result.filter(pl.col("raw_positive_volume_rows") > 0).height,
        codes_with_admitted_daily_trades=result.filter(pl.col("admitted_daily_trade_rows") > 0).height,
        blocked_financial_days=int(result["blocked_financial_days"].sum()),
        unresolved_codes=result.filter(pl.col("source_status") != "admitted_trade_evidence")["product"].to_list(),
        missing_volume_interpreted_as_zero=False, external_limits_proven=False,
        note="An inventory diagnoses work; it neither fabricates fills nor proves provider exhaustion.")
    return summary, result.sort("product")


def audit_actual_membership(dates, candidate_mask, source, *, test_start, test_end, execution=None):
    """Count the canonical policy mask, with observed rows as its identity map.

    Never construct a period union and broadcast it back into policy decisions.
    Actual positions, account affordability and realized fills remain separate.
    """
    from stockagent.data.tw_stock_context_futures_portfolio import DATED_FUTURES_MEMBERSHIP_CONTRACT
    dates = np.asarray(dates, dtype="datetime64[D]")
    if (dates.ndim != 1 or not len(dates) or np.isnat(dates).any()
            or np.any(dates[1:] <= dates[:-1]) or candidate_mask.dtype != np.bool_
            or candidate_mask.ndim != 2 or candidate_mask.shape[0] != len(dates)):
        raise ValueError("membership audit requires strictly ordered dates and Boolean [T,S]")
    keys = ["date", "physical_contract", "symbol"]
    frame = source.select(*keys, "product").filter(pl.col("date").is_in(dates.tolist()))
    if frame.select("date", "symbol").is_duplicated().any():
        raise ValueError("membership source has colliding physical slots")
    di = np.searchsorted(dates, frame["date"].to_numpy().astype("datetime64[D]"))
    si = frame["symbol"].str.extract(r"TAIFEX_SLOT_(\d+)$").cast(pl.Int64)
    if si.null_count() or not si.is_between(1, candidate_mask.shape[1]).all():
        raise ValueError("membership source contains an invalid policy slot")
    si = si.to_numpy() - 1
    admitted = candidate_mask[di, si]
    if int(admitted.sum()) != int(np.count_nonzero(candidate_mask)):
        raise ValueError("candidate membership exists outside dated source support")
    frame = frame.with_columns(pl.Series("policy_candidate", admitted))
    if execution is not None:
        if execution.shape[:2] != candidate_mask.shape or execution.shape[-1] < 11:
            raise ValueError("membership execution must match canonical integer [T,S,C]")
        possible = admitted & (execution[di, si, 1] > 0) & (execution[di, si, 8] >= 1)
        frame = frame.with_columns(pl.Series("entry_capacity_proxy", possible))
    test_start, test_end = map(date.fromisoformat, (test_start, test_end))
    if test_start > test_end:
        raise ValueError("invalid membership test period")
    periods = {}
    for name, lo, hi in (("history", dates[0], dates[-1]), ("test", test_start, test_end)):
        in_period = (dates >= np.datetime64(lo)) & (dates <= np.datetime64(hi))
        total = int(in_period.sum())
        if not total:
            raise ValueError("membership audit period has no actual sessions")
        subset = frame.filter(pl.col("date").is_between(lo, hi))
        aggregate = subset.group_by("product").agg(
            pl.col("date").filter(pl.col("policy_candidate")).n_unique().alias("candidate_days"))
        counts = subset.filter(pl.col("policy_candidate")).group_by("date").agg(pl.col("product").n_unique())
        daily_counts = pl.DataFrame({"date": dates[in_period]}).with_columns(pl.col("date").cast(pl.Date)).join(
            counts, on="date", how="left").with_columns(pl.col("product").fill_null(0))["product"]
        union = aggregate.filter(pl.col("candidate_days") > 0).height
        intersection = aggregate.filter(pl.col("candidate_days") == total).height
        periods[name] = dict(sessions=total, candidate_product_union=union,
            candidate_product_intersection=intersection, partial_period_candidates=union - intersection,
            daily_minimum=int(daily_counts.min()), daily_maximum=int(daily_counts.max()))
        if execution is not None:
            periods[name]["entry_capacity_product_union"] = subset.filter(pl.col("entry_capacity_proxy"))["product"].n_unique()
    return dict(contract=DATED_FUTURES_MEMBERSHIP_CONTRACT, periods=periods,
        no_candidate_outside_dated_source=True, mask_cells=int(candidate_mask.size),
        candidate_cells=int(np.count_nonzero(candidate_mask)),
        policy_note="per-decision source membership; no full-period, full-lookback or train-survival requirement",
        execution_note="entry capacity proxy is not a realized position or fill; side limits, funding and holdings remain authoritative"), frame


def audit_universe(daily, clocks, *, train_start, test_start, test_end, participation=.5):
    """Use physical identities for joins; adjusted codes remain distinct inventory."""
    train_start, test_start, test_end = map(date.fromisoformat, (train_start, test_start, test_end))
    if not train_start <= test_start <= test_end or not 0 < participation <= 1:
        raise ValueError('invalid universe horizon or participation')
    keys = ['date', 'physical_contract', 'symbol']
    if daily.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('duplicate physical contract-day in daily inventory')
    test = pl.col('date').is_between(test_start, test_end)
    train = pl.col('date').is_between(train_start, test_end)
    products = daily.group_by('product').agg(
        pl.col('asset_class').unique().sort(), pl.col('underlying_symbol').drop_nulls().unique().sort(),
        pl.col('date').min().alias('first_source_date'), pl.col('date').max().alias('last_source_date'),
        pl.len().alias('source_rows'), pl.col('date').filter(pl.col('executable')).n_unique().alias('history_executable_days'),
        train.any().alias('in_training_horizon'), test.any().alias('in_test_horizon'),
        (train & pl.col('executable')).any().alias('training_daily_executable'),
        (test & pl.col('executable')).any().alias('test_daily_executable'))
    products = products.with_columns(
        pl.when(pl.col('last_source_date') < train_start).then(pl.lit('ended_before_training'))
        .when(pl.col('last_source_date') < test_start).then(pl.lit('ended_before_test'))
        .when(pl.col('first_source_date') > test_end).then(pl.lit('starts_after_test'))
        .when(~pl.col('test_daily_executable')).then(pl.lit('test_valuation_only'))
        .otherwise(pl.lit('test_daily_executable')).alias('coverage_partition'))
    per_day = daily.group_by('date').agg(pl.col('product').n_unique().alias('source_codes'),
        pl.col('product').filter(pl.col('executable')).n_unique().alias('daily_executable_codes'))
    clock_summary = {}
    for name, entries in clocks.items():
        if entries.select('date', 'physical_contract').is_duplicated().any():
            raise ValueError('duplicate clock contract-day')
        if entries.join(daily.select(keys), on=keys, how='anti').height:
            raise ValueError('clock contains an identity absent from the daily source')
        joined = entries.join(daily.select(*keys, 'product', 'previous_volume', 'same_contract_as_previous_session'),
            on=keys, how='left', validate='1:1')
        observed = pl.col('status') == 'observed_minute'
        capacity = ((pl.col('entry_volume') * participation).floor() >= 1)
        prior_capacity = ((pl.col('previous_volume') * participation).floor() >= 1)
        eligible = observed & capacity & prior_capacity & pl.col('same_contract_as_previous_session')
        counts = joined.group_by('product').agg(
            pl.col('date').filter(observed).n_unique().alias(name + '_observed_days'),
            (test & observed).any().alias(name + '_test_observed'),
            (test & eligible).any().alias(name + '_test_one_lot_capacity_proxy'),
            eligible.any().alias(name + '_history_one_lot_capacity_proxy'))
        products = products.join(counts, on='product', how='left', validate='1:1')
        daily_clock = joined.group_by('date').agg(
            pl.col('product').filter(observed).n_unique().alias(name + '_observed_codes'),
            pl.col('product').filter(eligible).n_unique().alias(name + '_one_lot_capacity_codes'))
        per_day = per_day.join(daily_clock, on='date', how='left', validate='1:1')
        clock_summary[name] = dict(rows=entries.height, start=str(entries['date'].min()), end=str(entries['date'].max()),
            observed_products=joined.filter(observed)['product'].n_unique(),
            historical_one_lot_capacity_products=joined.filter(eligible)['product'].n_unique(),
            test_observed_products=joined.filter(test & observed)['product'].n_unique(),
            test_one_lot_capacity_products=joined.filter(test & eligible)['product'].n_unique(),
            statuses=dict(entries.group_by('status').len().iter_rows()))
    partitions = dict(products.group_by('coverage_partition').len().iter_rows())
    if sum(partitions.values()) != products.height:
        raise RuntimeError('universe partition does not reconcile')
    summary = dict(contract='tw_futures_historical_universe_coverage_v1',
        train_start=str(train_start), test_start=str(test_start), test_end=str(test_end), participation=participation,
        historical_inventory_products=products.height,
        historical_daily_executable_products=products.filter(pl.col('history_executable_days') > 0).height,
        training_horizon_products=products.filter(pl.col('in_training_horizon')).height,
        training_daily_executable_products=products.filter(pl.col('training_daily_executable')).height,
        test_horizon_products=products.filter(pl.col('in_test_horizon')).height,
        test_daily_executable_products=products.filter(pl.col('test_daily_executable')).height,
        distinct_known_underlyings=daily['underlying_symbol'].drop_nulls().n_unique(),
        coverage_partition=partitions, clocks=clock_summary,
        capacity_note='one-lot proxy only; actual ledger also applies financial, limit, margin and portfolio constraints',
        scope_note='historical source codes include corporate adjustments; period unions are not simultaneous listings')
    return summary, products.sort('product'), per_day.sort('date')


def audit_configured_membership(config_path, output, *, test_start, test_end, benchmark_preparation=False):
    """Exercise the training owner using only its receipt-bound structural axes.

    No stock prices/features are invented or consumed: this is a data-policy
    audit, not a backtest, prediction, training run or CUDA performance claim.
    """
    import gc
    import hashlib
    import json
    import resource
    import time
    from stockagent.config import load_config
    from stockagent.data.panel import PanelData
    from stockagent.data.tw_futures_benchmark import resolve_tx_benchmark_path, tx_front_benchmark_source_end
    from stockagent.data.tw_stock_context_futures_portfolio import attach_configured_stock_context_futures_portfolio

    config = load_config(config_path)
    manifest_path = Path(config.data.factorized_feature_manifest)
    axes_sha256 = sha256_file(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    dates = np.asarray(manifest["dates"], dtype="datetime64[D]")
    dates = dates[dates >= np.datetime64(config.data.panel_start_date)]
    if config.trading.tw_futures_portfolio_benchmark_mode == "tx_front_rolling_1x_gross":
        dates = dates[dates <= np.datetime64(tx_front_benchmark_source_end(resolve_tx_benchmark_path(config.trading)))]
    symbols = manifest["symbols"]
    shape = (len(dates), len(symbols))
    # Zero-width stock features are explicitly unused; only axes route contracts.
    zero = np.broadcast_to(np.float32(0), shape)
    unused = np.broadcast_to(False, shape)

    def panel_axes():
        return PanelData(dates=dates, symbols=symbols, feature_names=[],
            features=np.empty((*shape, 0), dtype=np.float32), returns_1d=zero,
            tradable_mask=unused, alive_mask=unused,
            benchmark_returns=np.zeros(len(dates), dtype=np.float32), close_prices=zero)

    fields = ("candidate_mask", "candidate_features", "integer_execution", "holding_log_returns",
        "executable_mask", "must_liquidate_mask", "can_hold_overnight_mask", "benchmark_log_returns")
    trials, identity = [], None
    selected = None
    selected_dates = None
    for reuse in ([False, True, True, False] if benchmark_preparation else [True]):
        gc.collect()
        cpu = resource.getrusage(resource.RUSAGE_SELF)
        started = time.perf_counter()
        attached = attach_configured_stock_context_futures_portfolio(panel_axes(), config,
            reuse_validated_margin_source=reuse)
        elapsed = time.perf_counter() - started
        usage = resource.getrusage(resource.RUSAGE_SELF)
        selected = attached.stock_context_futures_portfolio_daily
        selected_dates = np.asarray(attached.dates, dtype="datetime64[D]")
        hashes = {name: hashlib.sha256(memoryview(np.ascontiguousarray(getattr(selected, name))).cast("B")).hexdigest()
                  for name in fields if getattr(selected, name) is not None}
        if identity is not None and identity != hashes:
            raise ValueError("shared financial validation changed canonical data-policy outputs")
        identity = hashes
        trials.append(dict(reuse_validated_margin_source=reuse, attachment_wall_s=elapsed,
            attachment_cpu_s=(usage.ru_utime + usage.ru_stime - cpu.ru_utime - cpu.ru_stime),
            process_peak_rss_kib=usage.ru_maxrss, canonical_array_sha256=hashes))
        if benchmark_preparation and len(trials) < 4:
            del attached, selected
            selected = None
    source = pl.read_parquet(selected.source_path, columns=["date", "physical_contract", "symbol", "product"])
    summary, rows = audit_actual_membership(selected_dates, selected.candidate_mask, source,
        test_start=test_start, test_end=test_end, execution=selected.integer_execution)
    summary.update(config=str(config_path), config_sha256=sha256_file(Path(config_path)),
        axes_manifest=str(manifest_path), axes_manifest_sha256=axes_sha256,
        source_daily_sha256=sha256_file(Path(selected.source_path)),
        source_rules_sha256=sha256_file(Path(config.trading.tw_futures_portfolio_margin_rules_path)),
        scope="canonical candidate/execution preparation on structural axes; excludes stock feature loading and all training",
        preparation_trials=trials)
    if axes_sha256 != sha256_file(manifest_path):
        raise ValueError("structural axes changed during membership audit")
    if benchmark_preparation:
        a = np.mean([r["attachment_wall_s"] for r in trials if not r["reuse_validated_margin_source"]])
        b = np.mean([r["attachment_wall_s"] for r in trials if r["reuse_validated_margin_source"]])
        summary["preparation_comparison"] = dict(order="ABBA", original_mean_s=float(a), shared_mean_s=float(b),
            speed_ratio=float(a / b), bitwise_policy_arrays_equal=True, complete_fold_speedup_measured=False)
    output.mkdir(parents=True, exist_ok=True)
    products = rows.group_by("product").agg(pl.col("date").min().alias("first_source_date"),
        pl.col("date").max().alias("last_source_date"),
        pl.col("date").filter(pl.col("policy_candidate")).n_unique().alias("candidate_days"),
        pl.col("date").filter(pl.col("entry_capacity_proxy")).n_unique().alias("entry_capacity_days"))
    per_day = rows.group_by("date").agg(pl.col("product").filter(pl.col("policy_candidate")).n_unique().alias("candidate_products"),
        pl.col("product").filter(pl.col("entry_capacity_proxy")).n_unique().alias("entry_capacity_products"))
    atomic_write_parquet(output / "actual_membership_products.parquet", products.sort("product"))
    atomic_write_parquet(output / "actual_membership_daily.parquet", per_day.sort("date"))
    atomic_write_json(output / "actual_membership.json", summary)
    print({k: v for k, v in summary.items() if k != "preparation_trials"})
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, help='Audit the actual canonical configured candidate mask')
    p.add_argument('--benchmark-preparation', action='store_true', help='CPU attachment ABBA; never a full-fold timing claim')
    p.add_argument('--daily', type=Path)
    p.add_argument('--rules', type=Path)
    p.add_argument('--raw-daily', type=Path, help='Reconcile original observations for every requested code')
    p.add_argument('--requested-universe', type=Path, help='Explicit product CSV; never derive it from test survivors')
    p.add_argument('--financial-blockers', type=Path, help='Canonical contract_day_blockers.parquet')
    p.add_argument('--clock', action='append', default=[], metavar='NAME=ENTRIES_PARQUET')
    p.add_argument('--train-start', default='2011-01-26')
    p.add_argument('--test-start', default='2026-02-26')
    p.add_argument('--test-end', default='2026-09-24')
    p.add_argument('--participation', type=float, default=.5)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    inventory_paths = (args.raw_daily, args.requested_universe, args.financial_blockers)
    if any(inventory_paths) and (not all(inventory_paths) or args.config is not None):
        p.error('source inventory requires all three explicit inputs and --daily/--rules')
    if args.config is not None:
        if args.daily or args.rules or args.clock:
            p.error('--config uses its pinned sources; do not mix explicit source arguments')
        audit_configured_membership(args.config, args.output, test_start=args.test_start,
            test_end=args.test_end, benchmark_preparation=args.benchmark_preparation)
        return 0
    if args.daily is None or args.rules is None or args.benchmark_preparation:
        p.error('source audit requires --daily/--rules; preparation benchmark requires --config')
    columns = ['date', 'physical_contract', 'symbol', 'product', 'underlying_symbol', 'asset_class',
        'executable', 'previous_volume', 'same_contract_as_previous_session']
    digest = sha256_file(args.daily)
    daily = pl.read_parquet(args.daily, columns=columns)
    clocks, receipts = {}, {}
    for item in args.clock:
        name, raw = item.split('=', 1)
        if not name.isidentifier() or name in clocks:
            raise ValueError('clock labels must be unique identifiers')
        path = Path(raw)
        import json
        clock = json.loads(path.with_name('manifest.json').read_text())['clock']['decision_time']
        clocks[name], receipts[name] = validate_clock_source(path, daily_path=args.daily,
            rules_path=args.rules, decision_time=clock)
    summary, products, per_day = audit_universe(daily, clocks, train_start=args.train_start,
        test_start=args.test_start, test_end=args.test_end, participation=args.participation)
    if sha256_file(args.daily) != digest:
        raise ValueError('daily source changed during universe audit')
    summary['source'] = dict(daily=str(args.daily), daily_sha256=digest, rules=str(args.rules),
        rules_sha256=sha256_file(args.rules), clocks={k:dict(pair_identity=v['pair_identity'],
        entries_sha256=v['entries_sha256'], contract=v['clock']) for k,v in receipts.items()})
    args.output.mkdir(parents=True, exist_ok=True)
    if all(inventory_paths):
        before = {str(path): sha256_file(path) for path in inventory_paths}
        raw_columns = ['date', 'product', 'contract', 'session', 'volume', 'open', 'high', 'low', 'close', 'settlement']
        inventory_summary, inventory = audit_source_inventory(
            pl.read_csv(args.requested_universe, infer_schema=False),
            pl.read_parquet(args.raw_daily, columns=raw_columns), daily,
            pl.read_parquet(args.financial_blockers), start=args.train_start, end=args.test_end)
        if before != {str(path): sha256_file(path) for path in inventory_paths}:
            raise ValueError('original/financial inventory changed during audit')
        inventory_summary['sources'] = before
        atomic_write_parquet(args.output/'source_inventory.parquet', inventory)
        inventory.write_csv(args.output/'source_inventory.csv')
        atomic_write_json(args.output/'source_inventory.json', inventory_summary)
        summary['source_inventory'] = inventory_summary
    atomic_write_parquet(args.output/'products.parquet', products)
    atomic_write_parquet(args.output/'daily_coverage.parquet', per_day)
    summary['products'] = products.with_columns(pl.col('first_source_date', 'last_source_date').cast(pl.String)).to_dicts()
    atomic_write_json(args.output/'universe_coverage.json', summary)
    print({k:v for k,v in summary.items() if k not in ('products', 'source')})
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
