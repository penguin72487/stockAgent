"""Replay all deployed TW slash-command paths without Discord sends or paper writes.

Fetch real MIS rows once per distinct request, verify the requested historical
session during inference, then hold those exact rows fixed for a paired queue
benchmark. An explicit synthetic I/O delay separates scheduling performance
from variable external network latency. No generated signal is promoted.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.discord_bot import bot as bot_mod
from stockagent.live import signal_engine
from stockagent.live.quote_provider import PriceSnapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-session", required=True, help="Session actually returned by MIS, YYYY-MM-DD")
    parser.add_argument("--feature-date", required=True, help="Prior accepted daily feature session, YYYY-MM-DD")
    parser.add_argument("--io-delay", type=float, default=0.25)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--synthetic-quotes", action="store_true", help="Explicit compute/queue test fixture; no market-data network request")
    args = parser.parse_args()
    if args.feature_date >= args.quote_session:
        parser.error("feature-date must precede quote-session")
    cfgs = [replace(bot_mod._resolve_market(m), panel_date=args.feature_date)
            for m in bot_mod._scheduled_markets()
            if bot_mod._resolve_market(m).day_trade_simulation_enabled]
    original_kwargs = bot_mod._signal_kwargs
    original_snapshot = signal_engine._price_snapshot
    snapshots = {}
    source_samples = []
    artifact_checks = []

    def signal_kwargs(**kwargs):
        resolved = original_kwargs(**kwargs)
        resolved.update(write=False, asof_date=f"{args.quote_session} 13:30:00", panel_date=args.feature_date)
        return resolved

    def capture(**kwargs):
        key = signal_engine._live_quote_request_key(kwargs)
        started = time.perf_counter()
        # Explicit MIS source: this probe never creates a broker login.
        if args.synthetic_quotes:
            prices = np.asarray(kwargs["fallback_prices"], dtype=np.float64).copy()
            valid = np.asarray(kwargs["request_mask"], dtype=bool) & np.isfinite(prices) & (prices > 0)
            stamp = datetime.fromisoformat(f"{args.quote_session}T13:00:00").replace(tzinfo=ZoneInfo("Asia/Taipei"))
            result = PriceSnapshot(
                prices=prices, open_prices=prices.copy(), source="synthetic:latency_probe",
                timestamp=stamp.isoformat(), available_mask=valid, available_count=int(valid.sum()),
                requested_count=int(np.count_nonzero(kwargs["request_mask"])),
                timestamps_ms=np.where(valid, int(stamp.timestamp() * 1000), 0).astype(np.int64),
            )
        else:
            result = original_snapshot(**kwargs)
        snapshots[key] = result
        source_samples.append({"elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                               "requested": result.requested_count, "available": result.available_count,
                               "price_timestamp": result.timestamp, "transport": result.transport_timing})
        return result

    def replay(**kwargs):
        if args.io_delay > 0:
            time.sleep(args.io_delay)
        return snapshots[signal_engine._live_quote_request_key(kwargs)]

    records = []
    async def slash(cfg):
        started = time.perf_counter()
        messages = []

        class Response:
            async def defer(self, **_kwargs):
                pass

        class Interaction:
            id = time.time_ns()
            user = SimpleNamespace(id=0)
            response = Response()

            async def edit_original_response(self, *, content, **_kwargs):
                messages.append(content)

        await bot_mod._handle_signal_now_command(
            Interaction(), market=cfg.market, mode="signal", price_source="auto",
            top_n=20, min_abs_delta=cfg.min_abs_delta, refresh_data=False, debug=False,
        )
        if not messages:
            raise RuntimeError(f"{cfg.market}: no original-response delivery")
        return {"market": cfg.market, "command_ms": round((time.perf_counter() - started) * 1000, 3),
                "message_chars": len(messages[0])}

    async def batch():
        started = time.perf_counter()
        results = await asyncio.gather(*(slash(cfg) for cfg in cfgs))
        return {"batch_ms": round((time.perf_counter() - started) * 1000, 3), "markets": results}

    with tempfile.TemporaryDirectory(prefix="discord-latency-") as temp, ExitStack() as stack:
        configs = {cfg.market: cfg for cfg in cfgs}
        statuses = {}
        for cfg in cfgs:
            current = bot_mod._ensure_signal_ready(cfg)
            statuses[cfg.market] = replace(current, market_open=True, data=replace(current.data, fresh=True))
        stack.enter_context(patch.dict(bot_mod.os.environ, {"STOCKAGENT_BOT_PROGRESS": "0"}))
        stack.enter_context(patch.object(bot_mod, "_resolve_market", lambda market: configs[market]))
        stack.enter_context(patch.object(bot_mod, "_signal_kwargs", signal_kwargs))
        stack.enter_context(patch.object(bot_mod, "AUDIT_LOG_PATH", Path(temp) / "audit.jsonl"))
        stack.enter_context(patch.object(bot_mod, "_ensure_signal_ready_cached", lambda cfg: statuses[cfg.market]))
        stack.enter_context(patch.object(bot_mod, "_enqueue_signal_now_background_refresh", side_effect=AssertionError("probe must not enqueue production work")))
        stack.enter_context(patch.object(bot_mod, "_signal_now_cached_result", lambda *_a, **_kw: None))
        stack.enter_context(patch.object(bot_mod, "_postclose_fast_signal_context", lambda *_a, **_kw: None))
        stack.enter_context(patch.object(bot_mod, "_signal_now_detail_page_groups", lambda *_a, **_kw: []))
        stack.enter_context(patch.object(bot_mod, "_intraday_waits_for_opening", lambda: False))
        stack.enter_context(patch.object(signal_engine, "fetch_shared_day_trade_stock_snapshots", side_effect=RuntimeError("probe disables broker login")))
        stack.enter_context(patch.object(signal_engine, "fetch_shioaji_stock_snapshots", side_effect=RuntimeError("probe disables broker login")))
        capture_kwargs = []
        for cfg in cfgs:
            kwargs = cfg.signal_kwargs(write=False, price_source="panel", ensure_previous_signal=False,
                                       asof_date=f"{args.quote_session} 13:30:00")
            signal_engine.generate_live_signal(**kwargs)
            capture_kwargs.append({**kwargs, "price_source": "tw", "day_trade_model_observation": "latest_quote"})
        with patch.object(signal_engine, "_price_snapshot", capture), ThreadPoolExecutor(max_workers=len(cfgs)) as pool:
            futures = [pool.submit(signal_engine.prefetch_live_signal_prices, **kwargs) for kwargs in capture_kwargs]
            prepared = [future.result() for future in futures]
        with patch.object(signal_engine, "_price_snapshot", lambda **kwargs: snapshots[signal_engine._live_quote_request_key(kwargs)]):
            for cfg, kwargs, quote in zip(cfgs, capture_kwargs, prepared):
                isolated_root = Path(temp) / cfg.market
                isolated_root.mkdir()
                pointer = isolated_root / "latest_signal.json"
                pointer.write_text('{"scheduled_opening":"keep"}')
                result = signal_engine.generate_live_signal(**{**kwargs, "_prefetched_quote": quote,
                    "write": True, "publish_latest": False, "live_output_dir": isolated_root})
                if str(result.summary["price_timestamp"])[:10] != args.quote_session:
                    raise RuntimeError("MIS session differs from requested replay session")
                assert pointer.read_text() == '{"scheduled_opening":"keep"}'
                assert (Path(result.output_dir) / "summary.json").is_file()
                assert (Path(result.output_dir) / "target_weights.parquet").is_file()
                artifact_checks.append({"market": cfg.market, "private_signal_durable": True,
                                        "scheduled_pointer_preserved": True})
        original_run = bot_mod._run_market_signal_sync
        for name, prefetch in (("serialized_quote_control", False), ("prefetch_enabled", True)):
            def run(**kwargs):
                kwargs["_prefetch_prices"] = prefetch
                return original_run(**kwargs)

            with patch.object(signal_engine, "_price_snapshot", replay), \
                 patch.object(bot_mod, "_run_market_signal_sync", run):
                samples = [asyncio.run(batch()) for _ in range(args.repeats)]
            records.append({"case": name, "median_batch_ms": statistics.median(s["batch_ms"] for s in samples),
                            "samples": samples})
        audit = [json.loads(line) for line in (Path(temp) / "audit.jsonl").read_text().splitlines()]
        accepted = [row for row in audit if row.get("action") == "accepted"]
        delivered = [row for row in audit if row.get("action") == "delivered"]
        generated = [row for row in audit if row.get("action") == "generated"]
        if len(generated) != len(cfgs) * args.repeats * 2 or len(delivered) != len(accepted):
            raise RuntimeError("command audit does not prove every generated result was delivered")
        payload = {"simulation_only": True, "discord_network_send": False, "signal_artifact_write": False,
                   "quote_fixture": "synthetic_prior_close" if args.synthetic_quotes else "live_mis_rows",
                   "quote_session": args.quote_session, "feature_date": args.feature_date,
                   "injected_io_delay_seconds": args.io_delay, "source_probes": source_samples,
                   "isolated_artifact_checks": artifact_checks,
                   "cases": records, "audit": audit}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({"output": str(args.output), "cases": records, "source_probes": source_samples}, indent=2))


if __name__ == "__main__":
    main()
