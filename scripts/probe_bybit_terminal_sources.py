"""Record bounded official HFT/VINE terminal evidence without changing datasets.

This is a read-only market query. The only write is the explicitly requested,
fixed audit receipt. Minute index OHLC is not an official settlement-price
receipt and must not silently become a tick-TWAP or a synthetic execution.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "downloader"))
from common import atomic_write_text

OUTPUT = ROOT / "artifacts/markets/bybit_perpetual_daily_0000_v7_training_audit/terminal_source_probe.json"
SYMBOLS = ("HFTUSDT", "VINEUSDT")


def milliseconds(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def jobs() -> list[tuple[str, str, dict]]:
    queries = []
    for symbol in SYMBOLS:
        for name, endpoint, interval, start, end, limit in (
            ("trade_tail_1m", "kline", 1, "2026-08-20T22:00", "2026-08-21T09:00", 1000),
            ("mark_tail_1m", "mark-price-kline", 1, "2026-08-20T22:00", "2026-08-21T09:00", 1000),
            ("index_tail_1m", "index-price-kline", 1, "2026-08-20T22:00", "2026-08-21T09:00", 1000),
            ("funding_mark_1h", "mark-price-kline", 60, "2026-08-20T00:00", "2026-08-21T10:00", 200),
            ("settlement_index_1m", "index-price-kline", 1, "2026-08-21T08:30", "2026-08-21T09:00", 1000),
            ("prior_empty_probe_replay", "kline", 1, "2026-08-20T00:00", "2026-08-22T00:00", 3),
        ):
            queries.append((name, endpoint, {"category": "linear", "symbol": symbol,
                            "interval": interval, "start": milliseconds(start),
                            "end": milliseconds(end) - 1, "limit": limit}))
        queries.extend([
            ("funding_tail", "funding/history", {"category": "linear", "symbol": symbol,
                "startTime": milliseconds("2026-08-20T00:00"),
                "endTime": milliseconds("2026-08-21T10:00") - 1, "limit": 200}),
            ("closed_instrument", "instruments-info", {"category": "linear", "symbol": symbol, "status": "Closed"}),
            ("delivery_price_attempt", "delivery-price", {"category": "linear", "symbol": symbol, "limit": 50}),
        ])
    return queries


def fetch(job: tuple[str, str, dict]) -> dict:
    name, endpoint, params = job
    with requests.get("https://api.bybit.com/v5/market/" + endpoint,
                      params=params, timeout=30, stream=True) as response:
        chunks, size = [], 0
        for chunk in response.iter_content(65536):
            size += len(chunk)
            if size > 1_000_000:
                raise ValueError("bounded probe response exceeds one megabyte")
            chunks.append(chunk)
        raw = b"".join(chunks)
        payload = json.loads(raw)
        if response.status_code != 200:
            raise ValueError(f"official endpoint HTTP failure: {response.status_code}")
        if name != "delivery_price_attempt" and payload.get("retCode") != 0:
            raise ValueError(f"official market query failed: {name}")
        return {"name": name, "symbol": params["symbol"], "url": response.url,
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                "http_status": response.status_code, "response_bytes": len(raw),
                "response_sha256": hashlib.sha256(raw).hexdigest(),
                "response_text": raw.decode("utf-8"), "response_json": payload,
                "response_json_canonical_sha256": hashlib.sha256(json.dumps(payload, sort_keys=True,
                    separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    if OUTPUT.exists():
        raise FileExistsError("fixed terminal evidence already exists; preserve it and review explicitly")
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(fetch, jobs()))
    receipt = {"schema_version": 1, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded official API reads; no source/panel/checkpoint writes or training",
        "api_probes": results,
        "interpretation_limits": [
            "A late end with limit=3 can return an empty list despite retained earlier minute history.",
            "Trade-open midnight execution, funding-event mark valuation, and exchange settlement are different prices.",
            "The 30 minute index OHLC series does not prove the exchange's actual averaged settlement price.",
            "Delivery-price API may reject perpetual symbols; retain the failure, never replace it with a guessed price.",
            "Data sufficiency does not guarantee constrained exits liquidate every held position.",
        ],
        "official_rules_verified_via_web": {
            "HFTUSDT": "https://announcements.bybit.com/en/article/delisting-of-hftusdt-perpetual-contract--art5f92db89ae63/",
            "VINEUSDT": "https://announcements.bybit.com/en/article/delisting-of-vineusdt-perpetual-contract--art131027cca589/",
            "delisting_utc": "2026-08-21T09:00:00Z",
            "price_rule": "preceding 30-minute average index; no numerical final price quoted",
            "fee_url": "https://www.bybit.com/en/help-center/article/Futures-Contracts-Fees-Explained",
            "fee_article_updated_display": "2026-08-05 09:53:00 (page timezone not specified)",
            "delisting_fee_rate": 0.0005,
            "fee_caveat": "official generic USDT delisting rule; not a historical account transaction receipt",
            "verification_method": "web content checked separately; these annotations are not raw HTML hashes",
        }}
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(OUTPUT, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(OUTPUT), "probes": len(results), "bytes": OUTPUT.stat().st_size,
                      "response_bytes": sum(item["response_bytes"] for item in results)}, indent=2))


if __name__ == "__main__":
    main()
