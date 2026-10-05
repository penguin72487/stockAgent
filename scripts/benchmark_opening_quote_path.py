"""Measure the canonical Snapshot path with a frozen receipt and slow quota meter.

This is an offline fault-replay benchmark, NOT a live opening latency or broker
fill. Both variants retain every receipt symbol and run the real quote parsing,
legal-price resolution and durable traffic accounting. Only synchronous quota
observation is toggled. No SDK login, network, signal or production write occurs.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime
import hashlib
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live import quote_provider as quotes


class ReceiptApi:
    def __init__(self, rows, usage_delay_ms):
        self.rows = rows
        self.usage_delay_ms = usage_delay_ms
        self.usage_calls = 0
        self.batch_sizes = []
        self.contracts = SimpleNamespace(get=lambda code: self._contract(code))

    def _contract(self, code):
        row = self.rows[code]
        return SimpleNamespace(
            code=code, reference=row.get("reference_prices"),
            limit_up=row.get("upper_limit_prices"), limit_down=row.get("lower_limit_prices"),
        )

    def usage(self):
        self.usage_calls += 1
        time.sleep(self.usage_delay_ms / 1000.0)
        return SimpleNamespace(bytes=1000, limit_bytes=1_000_000)

    def snapshots(self, contracts, *, timeout, cb):
        if timeout != 0 or len(contracts) > 500:
            raise AssertionError("native nonblocking/500-contract boundary changed")
        self.batch_sizes.append(len(contracts))
        # This clock is only the fake SDK response envelope. The original
        # receipt bytes remain untouched and are fingerprinted in the output.
        exchange_ns = int(np.datetime64(datetime.now().replace(tzinfo=None), "ns").astype(np.int64))
        fields = {
            "open": "open_prices", "high": "high_prices", "low": "low_prices",
            "close": "price", "total_volume": "volumes", "buy_price": "bid_prices",
            "sell_price": "ask_prices", "buy_volume": "bid_volumes", "sell_volume": "ask_volumes",
        }
        cb([SimpleNamespace(code=contract.code, ts=exchange_ns, simtrade=0,
                            **{field: self.rows[contract.code].get(source) for field, source in fields.items()})
            for contract in contracts])
        return []


def snapshot_digest(snapshot):
    fields = ("prices", "available_mask", "simtrade_flags", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS)
    digest = hashlib.sha256()
    for field in fields:
        value = getattr(snapshot, field)
        digest.update(field.encode())
        if value is not None:
            array = np.ascontiguousarray(value)
            digest.update(array.dtype.str.encode())
            digest.update(array.tobytes())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--usage-delay-ms", type=float, default=25.0)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--compare-scalar", action="store_true", help="Also compare the original scalar parser with quota reads disabled")
    args = parser.parse_args()
    if not np.isfinite(args.usage_delay_ms) or not 0 <= args.usage_delay_ms <= 5000 or args.repeats < 1:
        parser.error("usage delay must be finite and between 0 and 5000 ms; repeats must be positive")
    raw = args.receipt.read_bytes()
    receipt = json.loads(raw)
    rows = receipt["rows"]
    if not isinstance(rows, dict) or not rows or len(rows) != receipt["row_count"]:
        parser.error("receipt must contain its complete nonempty symbol map")
    symbols = list(rows)
    fallback = np.array([float(row["price"]) if row.get("price") is not None else np.nan for row in rows.values()])
    samples = []
    contract_digest = None
    real_query = quotes.shioaji_query
    real_parse = quotes._float_or_none

    def legacy_parse(value):
        try:
            text = str(value).strip()
            if not text or text in {"-", "--", "null", "None"}:
                return None
            parsed = float(text.replace(",", ""))
        except Exception:
            return None
        return parsed if np.isfinite(parsed) and parsed > 0.0 else None

    def legacy_query(*a, **kw):
        kw["observe_usage"] = True
        return real_query(*a, **kw)

    with tempfile.TemporaryDirectory(prefix="opening-quote-benchmark-") as temporary, ExitStack() as stack:
        stack.enter_context(patch.dict(quotes.os.environ, {"STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT": temporary}))
        limits = {code: (row.get("reference_prices"), row.get("upper_limit_prices"), row.get("lower_limit_prices"))
                  for code, row in rows.items()}
        stack.enter_context(patch.object(quotes, "_load_prepared_tw_price_limits", return_value=(limits, None)))
        variants = ("legacy", "critical_scalar_control", "critical") if args.compare_scalar else ("legacy", "critical")
        # Balanced forward/reverse ordering; no warm price cache in any arm.
        for repetition in range(args.repeats):
            for variant in (variants if repetition % 2 == 0 else tuple(reversed(variants))):
                api = ReceiptApi(rows, args.usage_delay_ms)
                with patch.object(quotes, "_SHIOAJI_STOCK_CONTRACTS", {}), \
                     patch.object(quotes, "_SHIOAJI_STOCK_CACHE", {}), \
                     patch.object(quotes, "shioaji_query", legacy_query if variant == "legacy" else real_query), \
                     patch.object(quotes, "_float_or_none", legacy_parse if args.compare_scalar and variant != "critical" else real_parse):
                    started_ms = int(time.time() * 1000)
                    started = time.perf_counter()
                    snapshot = quotes._fetch_shioaji_stock_snapshots_once(symbols, fallback, cache_ttl_seconds=0, api=api)
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    finished_ms = int(time.time() * 1000)
                digest = snapshot_digest(snapshot)
                if contract_digest is not None and digest != contract_digest:
                    raise AssertionError("quote/price/volume/limit/simtrade contract changed")
                contract_digest = digest
                if not np.all((snapshot.timestamps_ms >= started_ms) & (snapshot.timestamps_ms <= finished_ms)):
                    raise AssertionError("callback receipt time no longer matches the actual local observation")
                if api.usage_calls != (2 if variant == "legacy" else 0):
                    raise AssertionError("quota observation did not stay outside the critical path")
                samples.append({"repetition": repetition, "variant": variant, "elapsed_ms": elapsed_ms,
                                "usage_calls": api.usage_calls, "batch_sizes": api.batch_sizes,
                                "available_count": snapshot.available_count, "transport": snapshot.transport_timing})
        events = [json.loads(line) for path in Path(temporary, "daily").glob("*.jsonl")
                  for line in path.read_text().splitlines()]
        if len(events) != len(samples) or any(event["rows"] != len(symbols) for event in events):
            raise AssertionError("durable query/row accounting incomplete")
        critical_events = [event for event in events if event.get("usage_observation") == "not_sampled_latency_critical"]
        if len(critical_events) != args.repeats * (len(variants) - 1) or any(event["usage_delta_bytes"] is not None for event in critical_events):
            raise AssertionError("unknown quota attribution was lost")

    medians = {variant: statistics.median(row["elapsed_ms"] for row in samples if row["variant"] == variant)
               for variant in variants}
    payload = {"scope": "offline_canonical_snapshot_fault_replay_not_live_opening_latency",
               "network_requests": 0, "production_writes": False, "receipt_sha256": hashlib.sha256(raw).hexdigest(),
               "symbols": len(symbols), "injected_usage_delay_ms_per_call": args.usage_delay_ms,
               "price_contract_sha256": contract_digest, "parity_verified": True,
               "durable_events": len(events), "median_ms": medians, "samples": samples}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in payload.items() if key != "samples"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
