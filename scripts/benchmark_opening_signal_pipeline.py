"""Replay deployed opening models against a pinned receipt, without live writes.

Measures warm application compute/cache/publication, NOT exchange/network delay.
All generated signals and copied quote receipts stay in a temporary directory.
No quotes are fetched, no Discord messages or orders are sent.
"""
from __future__ import annotations

import argparse
import cProfile
from contextlib import ExitStack
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.discord_bot import bot as bot_mod
from stockagent import config as config_module
from stockagent.live import quote_provider, signal_engine


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--feature-date", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--defer-reports", action="store_true")
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--legacy-control", action="store_true", help="Restore redundant receipt/feature work for paired comparison")
    parser.add_argument("--legacy-validation-control", action="store_true", help="Resolve static bool annotations again on every config load")
    parser.add_argument("--compare-validation", action="store_true", help="Compare cached/uncached schema validation in balanced order in the same process")
    parser.add_argument("--compare-layout", action="store_true", help="Compare prewarmed C-order/K-order inputs in balanced order; model outputs must match")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    raw = args.receipt.read_bytes()
    session = json.loads(raw)["session_date"]
    if args.feature_date >= session or args.repeats < 1:
        parser.error("feature-date must precede receipt session; repeats must be positive")
    if args.compare_validation and args.legacy_validation_control:
        parser.error("compare-validation cannot be combined with legacy-validation-control")
    if args.compare_validation and args.compare_layout:
        parser.error("choose one paired comparison at a time")
    observed = datetime.fromisoformat(session + "T09:00:00+08:00")

    class ReceiptClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed.astimezone(tz or ZoneInfo("Asia/Taipei"))

    cfgs = [bot_mod._resolve_market(m) for m in bot_mod._scheduled_markets()
            if bot_mod._resolve_market(m).day_trade_simulation_enabled]
    cfgs.sort(key=lambda cfg: (-bot_mod._preopen_market_symbol_count(cfg), cfg.market))
    records, contracts = [], {}
    canonical_window = signal_engine._cached_normalized_model_window
    contiguous_windows = {}
    legacy_windows = {}

    def contiguous_layout_window(panel, *, panel_idx, lookback):
        value = canonical_window(panel, panel_idx=panel_idx, lookback=lookback)
        key = (id(panel.features), panel_idx, lookback)
        previous = contiguous_windows.get(key)
        if previous is None or previous[0] is not value:
            previous = (value, np.ascontiguousarray(value))
            contiguous_windows[key] = previous
        return previous[1]

    normalized_window = contiguous_layout_window if args.compare_layout else canonical_window

    def legacy_layout_window(panel, *, panel_idx, lookback):
        # Values still come from the canonical normalizer. Only restore the
        # pre-fix K-order layout once; no input values or model work are omitted.
        value = normalized_window(panel, panel_idx=panel_idx, lookback=lookback)
        key = (id(panel.features), panel_idx, lookback)
        previous = legacy_windows.get(key)
        if previous is None or previous[0] is not panel.features:
            template = panel.features[panel_idx - lookback + 1:panel_idx + 1]
            old = np.empty_like(template, dtype=np.float32, order="K")
            np.copyto(old, value)
            previous = (panel.features, old)
            legacy_windows[key] = previous
        return previous[1]

    with tempfile.TemporaryDirectory(prefix="opening-pipeline-") as temporary, ExitStack() as stack:
        stack.enter_context(patch.object(signal_engine, "_cached_normalized_model_window", normalized_window))
        root = Path(temporary)
        receipts = root / "receipts"
        receipts.mkdir()
        shutil.copyfile(args.receipt, receipts / f"{session}.json")
        stack.enter_context(patch.dict(quote_provider.os.environ, {
            "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": str(receipts),
            "STOCKAGENT_BOT_PROGRESS": "0",
            "STOCKAGENT_LIVE_PANEL_CACHE_ROOT": str(root / "panels"),
        }))
        stack.enter_context(patch.object(quote_provider, "datetime", ReceiptClock))
        for module, name in (
            (signal_engine, "fetch_shared_day_trade_stock_snapshots"),
            (signal_engine, "fetch_shioaji_stock_snapshots"),
            (quote_provider, "fetch_tw_mis_last_prices"),
        ):
            stack.enter_context(patch.object(module, name, side_effect=AssertionError("offline benchmark forbids network")))
        kwargs_by_market = {}
        for cfg in cfgs:
            kwargs = cfg.signal_kwargs(
                write=False, price_source="tw", ensure_previous_signal=False,
                previous_signal_backfill_limit=0, panel_date=args.feature_date,
                asof_date=f"{session} 09:00:00", day_trade_model_observation="session_open",
            )
            result = signal_engine.generate_live_signal(**kwargs)
            kwargs_by_market[cfg.market] = kwargs
            contracts[cfg.market] = _contract_digest(result)
        # One further complete pass makes cross-model cache capacity visible.
        for kwargs in kwargs_by_market.values():
            signal_engine.generate_live_signal(**kwargs)
        if args.compare_layout:
            with patch.object(signal_engine, "_cached_normalized_model_window", legacy_layout_window):
                for kwargs in kwargs_by_market.values():
                    result = signal_engine.generate_live_signal(**kwargs)
                    if _contract_digest(result) != contracts[result.summary["market"]]:
                        raise RuntimeError("memory layout changed the canonical model contract")
        stack.enter_context(patch.object(signal_engine, "_cached_normalized_model_window", normalized_window))
        schema_resolver = config_module._strict_bool_config_fields
        stack.enter_context(patch.object(config_module, "_strict_bool_config_fields", schema_resolver))
        if args.legacy_control:
            fetch = signal_engine.fetch_tw_mis_opening_snapshot
            feature_summary = signal_engine._feature_driver_summary

            def reseed(*a, **kw):
                snapshot = fetch(*a, **kw)
                signal_engine.seed_tw_opening_snapshot_cache(a[0], snapshot, parquet_root=kw["parquet_root"])
                return snapshot

            def duplicate_features(*a, **kw):
                feature_summary(*a, top_n=8)
                return feature_summary(*a, **kw)

            stack.enter_context(patch.object(signal_engine, "fetch_tw_mis_opening_snapshot", reseed))
            stack.enter_context(patch.object(signal_engine, "_feature_driver_summary", duplicate_features))
            class PythonSafeLoader(yaml.SafeLoader):
                pass

            PythonSafeLoader.add_constructor(
                yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                config_module._construct_unique_mapping,
            )

            def scalar_numpy_finite(value):
                try:
                    number = float(value)
                except Exception:
                    return None
                return number if np.isfinite(number) else None

            stack.enter_context(patch.object(config_module, "_UniqueKeySafeLoader", PythonSafeLoader))
            stack.enter_context(patch.object(signal_engine, "_finite_float_or_none", scalar_numpy_finite))
        profiler = cProfile.Profile() if args.profile else None
        if profiler:
            profiler.enable()
        variants = (False, True) if args.compare_validation or args.compare_layout else (not args.legacy_validation_control,)
        repetitions = [(repetition, use_cached) for repetition in range(args.repeats)
                       for use_cached in (variants if repetition % 2 == 0 else tuple(reversed(variants)))]
        for repetition, use_cached in repetitions:
            config_module._strict_bool_config_fields = schema_resolver if use_cached or args.compare_layout else schema_resolver.__wrapped__
            signal_engine._cached_normalized_model_window = normalized_window if use_cached or not args.compare_layout else legacy_layout_window
            started = time.perf_counter()
            rows, results = [], []
            for cfg in cfgs:
                kwargs = dict(kwargs_by_market[cfg.market], write=True,
                              live_output_dir=root / f"{repetition}-{int(use_cached)}" / cfg.market)
                if args.defer_reports:
                    kwargs["defer_rich_artifacts"] = True
                step = time.perf_counter()
                result = signal_engine.generate_live_signal(**kwargs)
                publish_ms = (step - started) * 1000 + result.summary["live_latency"].get(
                    "input_to_execution_pointer_ms", result.summary["live_latency"]["input_to_publish_ms"])
                rows.append({"market": cfg.market, "return_ms": (time.perf_counter() - step) * 1000,
                             "batch_publish_ms": publish_ms,
                             "stages": result.summary["live_latency"]})
                results.append(result)
            published_ms = max(row["batch_publish_ms"] for row in rows)
            if args.defer_reports:
                for result in results:
                    signal_engine.finalize_live_signal_artifacts(result)
            artifacts_ms = (time.perf_counter() - started) * 1000
            for result in results:
                if _contract_digest(result) != contracts[result.summary["market"]]:
                    raise RuntimeError(f"{result.summary['market']}: model/price/explanation contract changed")
                directory = Path(result.output_dir)
                if not (directory / "target_weights.parquet").is_file():
                    raise RuntimeError("rich artifacts incomplete")
                pointer = json.loads((directory.parents[1] / "latest_signal.json").read_text())
                if not pointer["artifact_complete"]:
                    raise RuntimeError("completion pointer incomplete")
            variant = ("C_order" if use_cached else "K_order") if args.compare_layout else ("cached" if use_cached else "uncached")
            records.append({"variant": variant,
                            "schema_validation_cached": use_cached or args.compare_layout,
                            "prewarmed_c_layout": use_cached if args.compare_layout else None,
                            "all_published_ms": published_ms,
                            "all_artifacts_ms": artifacts_ms,
                            "markets": rows})
        if profiler:
            profiler.disable()
            args.profile.parent.mkdir(parents=True, exist_ok=True)
            profiler.dump_stats(args.profile)
        payload = {"scope": "offline_warm_receipt_replay_not_live_opening_latency",
                   "network_requests": 0, "production_writes": False,
                   "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                   "session": session, "feature_date": args.feature_date,
                   "deferred_reports": args.defer_reports, "contracts": contracts,
                   "legacy_redundant_work_control": args.legacy_control,
                   "legacy_validation_control": args.legacy_validation_control,
                   "comparison": "prewarmed_memory_layout" if args.compare_layout else "static_schema_validation",
                   "variant_median_ms": {
                       variant: statistics.median(row["all_published_ms"] for row in records if row["variant"] == variant)
                       for variant in sorted({row["variant"] for row in records})},
                   "median_all_published_ms": statistics.median(r["all_published_ms"] for r in records),
                   "measurement_summary": _measurement_summary(records),
                   "samples": records}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({k: v for k, v in payload.items() if k != "samples"}, ensure_ascii=False, indent=2))


def _measurement_summary(records) -> dict:
    """Retain every sample; separate cache rebuilds from fully warmed paths.

    Publication latency and rich-artifact completion are different boundaries.
    Warm-only statistics are diagnostic, never substitutes for all-sample data.
    """
    def is_warm(row):
        markets = row.get("markets") or []
        return bool(markets) and all(
            market.get("stages", {}).get(key) is True
            for market in markets
            for key in ("panel_cache_hit", "checkpoint_cache_hit",
                        "model_cache_hit", "alignment_cache_hit")
        )

    def durations(rows):
        return {
            "sample_count": len(rows),
            "median_all_published_ms": statistics.median(
                row["all_published_ms"] for row in rows) if rows else None,
            "median_all_artifacts_ms": statistics.median(
                row["all_artifacts_ms"] for row in rows) if rows else None,
        }

    variants = {}
    for variant in sorted({row["variant"] for row in records}):
        rows = [row for row in records if row["variant"] == variant]
        warm = [row for row in rows if is_warm(row)]
        variants[variant] = {
            "all_samples": durations(rows),
            "fully_warm_samples": durations(warm),
            "not_fully_warm_sample_count": len(rows) - len(warm),
        }
    return {"all_samples_retained": True, "variants": variants}


def _contract_digest(result) -> str:
    payload = {"weights": result.weights_rows, "decisions": result.decision_rows,
               "explanation": result.summary["model_explanation"]}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


if __name__ == "__main__":
    main()
