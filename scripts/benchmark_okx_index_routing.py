"""Bounded read-only ABBA probe of one completed OKX index-candle window.

Both routes use the canonical paginator and native shared endpoint buckets.
Only a benchmark receipt is written; source Parquet, progress and run summaries
are untouched. This is not a complete registered-features service benchmark.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.common import atomic_write_text  # noqa: E402
from downloader.download_okx_perp_daily import OkxClient  # noqa: E402
from downloader.okx_historical_features import (  # noqa: E402
    CANDLE_INTERVAL_MS,
    INDEX_PRICE_HISTORY_ENDPOINT,
    INDEX_PRICE_PAGE_LIMIT,
    INDEX_PRICE_RECENT_ENDPOINT,
    _fetch_array_history,
    _normalize_price_candles,
    feature_acquisition_payload,
)


class ProbeBudgetExceeded(TimeoutError):
    pass


class _BudgetClient:
    def __init__(self, client: Any, deadline: float) -> None:
        self.client = client
        self.deadline = deadline

    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if time.monotonic() >= self.deadline:
            raise ProbeBudgetExceeded("benchmark page-admission budget exceeded")
        return self.client.get(path, params)


def _limiter_delta(before: dict, after: dict) -> dict[str, int]:
    return {
        name: int(value.get("grants_total", 0)) - int(before.get(name, {}).get("grants_total", 0))
        for name, value in after.items()
    }


def run_probe(
    client: Any,
    *,
    inst_id: str,
    start_ms: int,
    end_ms: int,
    now_ms: int,
    repetitions: int = 1,
    budget_seconds: float = 60.0,
) -> dict[str, Any]:
    if not re.fullmatch(r"[A-Z0-9]+(?:-[A-Z0-9]+){1,2}", inst_id):
        raise ValueError("invalid public index ID")
    if not isinstance(repetitions, int) or isinstance(repetitions, bool) or not 1 <= repetitions <= 3:
        raise ValueError("repetitions must be 1..3")
    if not math.isfinite(budget_seconds) or not 1 <= budget_seconds <= 300:
        raise ValueError("page-admission budget must be 1..300 seconds")
    if start_ms % CANDLE_INTERVAL_MS or end_ms % CANDLE_INTERVAL_MS or start_ms > end_ms:
        raise ValueError("invalid fixed one-minute window")
    if end_ms >= (now_ms // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS:
        raise ValueError("benchmark must exclude the unfinished current bar")
    expected_rows = (end_ms - start_ms) // CANDLE_INTERVAL_MS + 1
    if not 1 <= expected_rows <= 2880:
        raise ValueError("benchmark window must contain 1..2880 requested minutes")
    started = time.monotonic()
    bounded = _BudgetClient(client, started + budget_seconds)
    trials: list[dict[str, Any]] = []
    reference = None
    failure = None
    for variant in ("history_only", "hybrid", "hybrid", "history_only") * repetitions:
        audit: dict[str, Any] = {}
        before = client.limiter_activity()
        trial_started = time.perf_counter()
        try:
            rows = _fetch_array_history(
                bounded, INDEX_PRICE_HISTORY_ENDPOINT, {"instId": inst_id, "bar": "1m"},
                start_ms=start_ms, end_ms=end_ms, limit=INDEX_PRICE_PAGE_LIMIT,
                recent_path=INDEX_PRICE_RECENT_ENDPOINT if variant == "hybrid" else None,
                now_ms=now_ms, request_audit=audit,
            )
            frame = _normalize_price_candles(rows, prefix="okx_index", start_ms=start_ms, end_ms=end_ms)
            if reference is None:
                reference = frame
            trials.append({
                "variant": variant,
                "elapsed_seconds": time.perf_counter() - trial_started,
                "rows": frame.height,
                "first_date": frame["date"].min() if frame.height else None,
                "last_date": frame["date"].max() if frame.height else None,
                "normalized_sha256": hashlib.sha256(frame.write_json().encode()).hexdigest(),
                "exact_frame_and_dtype_parity": frame.equals(reference),
                "acquisition": audit,
                "transport_grants": _limiter_delta(before, client.limiter_activity()),
            })
        except Exception as exc:
            failure = {
                "variant": variant,
                "type": type(exc).__name__,
                "acquisition": audit,
                "elapsed_seconds": time.perf_counter() - trial_started,
                "transport_grants": _limiter_delta(before, client.limiter_activity()),
            }
            break
    complete = len(trials) == 4 * repetitions and failure is None
    parity = complete and all(trial["exact_frame_and_dtype_parity"] for trial in trials)
    coverage = complete and all(trial["rows"] == expected_rows for trial in trials)
    medians = {
        variant: statistics.median(trial["elapsed_seconds"] for trial in trials if trial["variant"] == variant)
        for variant in ("history_only", "hybrid")
        if any(trial["variant"] == variant for trial in trials)
    }
    return {
        "schema_version": 1,
        "state": "accepted" if parity and coverage else "budget_exceeded" if failure and failure["type"] == "ProbeBudgetExceeded" else "not_accepted",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "fixed_window": {"inst_id": inst_id, "start_ms": start_ms, "end_ms": end_ms, "now_ms": now_ms, "requested_minutes": expected_rows},
        "order": "ABBA",
        "repetitions": repetitions,
        "trials": trials,
        "failure": failure,
        "exact_parity": parity,
        "requested_coverage": coverage,
        "medians_seconds": medians,
        "elapsed_seconds": time.monotonic() - started,
        "page_admission_budget_seconds": budget_seconds,
        "acquisition_contract": feature_acquisition_payload(),
        "source_files_written": 0,
        "claim_boundary": "one public completed index window; fetch+normalize only; not full-universe pipeline, other features, local history, cold-start, investment or live-trading readiness",
        "budget_boundary": "checked before each page; an admitted native client request may take its configured timeout; production retry budgets are unchanged",
        "cache_boundary": "same fixed window and shared native endpoint buckets; provider/OS cache is not controlled",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inst-id", default="BTC-USDT")
    parser.add_argument("--minutes", type=int, default=1440)
    parser.add_argument("--lag-minutes", type=int, default=30)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--budget-seconds", type=float, default=60.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if not 1 <= args.lag_minutes <= 10080:
        parser.error("--lag-minutes must be 1..10080")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = args.output or ROOT / "artifacts" / "benchmarks" / f"okx-index-routing-{stamp}.json"
    if output.exists():
        parser.error("benchmark output already exists; choose a new receipt path")
    now_ms = int(time.time() * 1000)
    end_ms = (now_ms // CANDLE_INTERVAL_MS - args.lag_minutes) * CANDLE_INTERVAL_MS
    start_ms = end_ms - (args.minutes - 1) * CANDLE_INTERVAL_MS
    # A failed public page ends this bounded probe; service retry policy is not changed.
    client = OkxClient(request_interval=None, max_retries=0, retry_base=0.6)
    try:
        result = run_probe(client, inst_id=args.inst_id, start_ms=start_ms, end_ms=end_ms, now_ms=now_ms,
                           repetitions=args.repetitions, budget_seconds=args.budget_seconds)
    except ValueError as exc:
        parser.error(str(exc))
    result["source_sha256"] = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (Path(__file__).resolve(), ROOT / "downloader" / "okx_historical_features.py",
                     ROOT / "downloader" / "download_okx_perp_daily.py", ROOT / "downloader" / "common.py")
    }
    atomic_write_text(output, json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"receipt": str(output), "state": result["state"], "medians_seconds": result["medians_seconds"], "exact_parity": result["exact_parity"], "requested_coverage": result["requested_coverage"], "trials": len(result["trials"])}))
    return 0 if result["state"] == "accepted" else 1


if __name__ == "__main__":
    raise SystemExit(main())
