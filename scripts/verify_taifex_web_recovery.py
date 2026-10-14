#!/usr/bin/env python3
"""Read back TAIFEX paper recovery, HTTP ranges and native browser interactions.

Never logs in, submits a broker order, changes a ledger, or weakens CSP.
Run after the canonical service has resumed; a running process is not acceptance.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.probe_browser_runtime import browser_profile_launch_options


def prefix_hash(path: Path, length: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while length:
            block = handle.read(min(length, 4 * 1024 * 1024))
            if not block:
                raise ValueError(f"original ledger prefix was truncated: {path.name}")
            digest.update(block)
            length -= len(block)
    return digest.hexdigest()


def ledger_readback(state_dir: Path, backup_dir: Path, *, settlement_path: Path,
                    capture_root: Path, capture_id: str) -> dict:
    import polars as pl
    from stockagent.data.taifex_sessions import TAIPEI, taifex_trading_date
    from stockagent.live.taifex_volatility_simulation import _depth_swept_price

    original_state = json.loads((backup_dir / "state.json").read_text())
    original_status = json.loads((backup_dir / "status.json").read_text())
    expiry = datetime.fromisoformat(original_state["active_cycle"]["expiry_date"]).date()
    settlements = pl.read_parquet(settlement_path).filter(pl.col("settlement_date") == expiry)
    if settlements.height != 1:
        raise ValueError("official settlement evidence is missing or ambiguous")
    settlement = settlements.row(0, named=True)
    settlement_file = Path(settlement["source_file"])
    settlement_hash = prefix_hash(settlement_file, settlement_file.stat().st_size)
    if settlement_hash != settlement["source_sha256"]:
        raise ValueError("official settlement source hash changed")
    settlement_price = float(settlement["final_settlement_price"])
    if not math.isfinite(settlement_price) or settlement_price <= 0:
        raise ValueError("invalid official settlement price")
    prefixes = []
    for name in ("ideal_ledger.jsonl", "marks.jsonl", "events.jsonl", "capital_contributions.jsonl"):
        original = backup_dir / name
        length = original.stat().st_size
        sha = prefix_hash(original, length)
        if prefix_hash(state_dir / name, length) != sha:
            raise ValueError(f"original append-only prefix was modified: {name}")
        prefixes.append({"file": name, "original_bytes": length, "sha256": sha})
    quantities = defaultdict(int)
    option_quantities = defaultdict(int)
    repairs = []
    with (state_dir / "ideal_ledger.jsonl").open("rb") as handle:
        handle.seek((backup_dir / "ideal_ledger.jsonl").stat().st_size)
        # The active writer may be between append writes; ignore only its final
        # incomplete line, never a malformed completed record.
        for raw in handle:
            if not raw.endswith(b"\n"):
                break
            row = json.loads(raw)
            if row.get("paper_repair") is not True:
                continue
            repairs.append(row)
            if row["instrument_type"] == "future":
                code = original_status["hedge_contract"]
                if row["code"] != code or row["price_source"] != "causally_received_five_level_depth_vwap":
                    raise ValueError("recovery future is not the original contract/five-level price")
                if row["book_receive_ts_ns"] > row["decision_ts_ns"] or row["book_age_ms"] > 2000:
                    raise ValueError("recovery future quote is noncausal or stale")
                quantities[row["strategy_id"]] += row["delta_contracts"]
            elif row["instrument_type"] == "option":
                if row["price_source"] != "official_taifex_final_settlement_intrinsic_value":
                    raise ValueError("recovery option has no official intrinsic settlement")
                leg = original_state["strategies"][row["strategy_id"]]["option_position_metadata"][row["code"]]
                strike = float(leg["strike"])
                intrinsic = (max(settlement_price - strike, 0.0) if leg["option_right"] == "C"
                             else max(strike - settlement_price, 0.0))
                if float(row["price_points"]) != intrinsic:
                    raise ValueError("recorded option price differs from official settlement intrinsic")
                option_quantities[(row["strategy_id"], row["code"])] += row["delta_contracts"]
    held = {sid: ledger["futures_position"] for sid, ledger in original_state["strategies"].items()
            if ledger.get("futures_position")}
    for sid, quantity in held.items():
        if quantities[sid] != -quantity:
            raise ValueError(f"original futures reduction is incomplete: {sid}")
    # Bind ledger fills to the original captured raw callbacks, not just their
    # self-reported price_source. No login, snapshot substitution or new orders.
    future_repairs = [row for row in repairs if row["instrument_type"] == "future"]
    partitions = set()
    for row in future_repairs:
        received = datetime.fromtimestamp(row["book_receive_ts_ns"] / 1e9, UTC).astimezone(TAIPEI)
        partitions.add((taifex_trading_date(received).isoformat(), received.strftime("%H")))
    raw_paths = [path for trading_date, hour in partitions for path in sorted(capture_root.glob(
        f"book_events/trade_date={trading_date}/hour={hour}/capture={capture_id}-worker=00-part=*.parquet"))]
    if future_repairs and not raw_paths:
        raise ValueError("original futures callback partitions are unavailable")
    raw_books = (pl.scan_parquet(raw_paths, include_file_paths="__source_file")
                 .filter(pl.col("receive_ts_ns").is_in([row["book_receive_ts_ns"] for row in future_repairs]))
                 .collect() if raw_paths else pl.DataFrame())
    used_paths = set()
    for row in future_repairs:
        matches = raw_books.filter((pl.col("code") == row["code"])
                                   & (pl.col("receive_ts_ns") == row["book_receive_ts_ns"]))
        if matches.height != 1:
            raise ValueError("original futures callback is missing or ambiguous")
        fill = _depth_swept_price(matches.row(0, named=True), delta_contracts=row["delta_contracts"],
                                  decision_ns=row["decision_ts_ns"], maximum_age_seconds=2.0)
        if fill is None or fill[0] != float(row["price_points"]):
            raise ValueError("recorded futures price/depth differs from captured executable book")
        used_paths.add(matches["__source_file"][0])
    option_leg_count = 0
    for sid, ledger in original_state["strategies"].items():
        for code, quantity in (ledger.get("option_positions") or {}).items():
            if quantity:
                option_leg_count += 1
                if option_quantities[(sid, code)] != -quantity:
                    raise ValueError(f"original option settlement is incomplete: {sid}/{code}")
    if len(repairs) != len(held) + option_leg_count:
        raise ValueError("repair rows include duplicates or changes outside the original cycle")
    state = json.loads((state_dir / "state.json").read_text())
    if state.get("broker_orders_enabled") or state.get("production_order_possible"):
        raise ValueError("paper repair enabled broker orders")
    old_cycle = original_state["active_cycle"]["cycle_id"]
    if (state.get("active_cycle") or {}).get("cycle_id") == old_cycle:
        raise ValueError("original expired cycle remains active")
    return {"original_cycle": old_cycle, "original_prefixes": prefixes,
            "repaired_futures_strategies": len(held), "settled_option_legs": option_leg_count,
            "repair_trade_rows": len(repairs), "broker_orders_enabled": False,
            "execution_contract_version": state["execution_contract_version"],
            "official_settlement": {"expiry": expiry.isoformat(), "price_points": settlement_price,
                                    "source_file": str(settlement_file), "source_sha256": settlement_hash},
            "raw_callback_price_matches": len(future_repairs), "capture_id": capture_id,
            "raw_callback_partitions_scanned": len(raw_paths),
            "raw_callback_sources": [{"path": path, "sha256": prefix_hash(Path(path), Path(path).stat().st_size)}
                                     for path in sorted(used_paths)]}


def validate_history_clock(status: dict, history: dict, *, maximum_lag_seconds: float = 180) -> float:
    """Reject healthy status + old curves, including a single forgotten strategy."""
    source = datetime.fromisoformat(status["source_updated_at_utc"])
    anchor = datetime.fromisoformat(history["anchor_at_utc"])
    end = datetime.fromisoformat(history["coverage_end_utc"])
    if any(value.tzinfo is None for value in (source, anchor, end)):
        raise ValueError("curve/source clocks have no timezone")
    lag = (source - end).total_seconds()
    if lag > maximum_lag_seconds or lag < -60 or end > anchor:
        raise ValueError("curve has not caught up to its source")
    last_times = {}
    for row in history.get("history") or []:
        sid, ts = row["strategy_id"], int(row["decision_ts_ns"])
        if ts < last_times.get(sid, 0):
            raise ValueError("history timestamps are not monotonic")
        last_times[sid] = ts
    required = int((source.timestamp() - maximum_lag_seconds) * 1e9)
    for strategy in status["strategies"]:
        if last_times.get(strategy["strategy_id"], 0) < required:
            raise ValueError(f"curve missing a current strategy endpoint: {strategy['strategy_id']}")
    return round(lag, 3)


def fetch_json(url: str) -> tuple[dict, float]:
    start = time.perf_counter()
    with urlopen(Request(url, headers={"Cache-Control": "no-cache"}), timeout=30) as response:
        data = json.load(response)
    return data, round((time.perf_counter() - start) * 1000, 3)


def api_readback(url: str) -> dict:
    status, status_ms = fetch_json(urljoin(url, "api/status"))
    if status.get("simulation_only") is not True or status.get("production_order_possible") is not False:
        raise ValueError("dashboard violated read-only simulation boundary")
    if status.get("blocked_reason") or status["source_age_seconds"] > status["max_source_age_seconds"]:
        raise ValueError("strategy snapshot remains blocked/stale")
    if not status.get("observed_trading_date") or not status.get("strategies"):
        raise ValueError("dashboard is missing current clock or strategy rows")
    ranges = []
    for key in ("1h", "1d", "1w", "1mo", "1q", "1y", "all"):
        start, attempts = time.perf_counter(), 0
        transient_failures = []
        while True:
            attempts += 1
            try:
                history, elapsed = fetch_json(urljoin(url, f"api/history?range={key}"))
            except HTTPError as exc:
                if exc.code != 503 or time.perf_counter() - start > 150:
                    raise
                transient_failures.append({"attempt": attempts, "http_status": exc.code})
                time.sleep(2)
                continue
            rows = history.get("history") or []
            if history.get("range") != key or len(rows) < 2:
                raise ValueError(f"history did not load: {key}")
            # SWR may briefly serve the previous verified generation. Acceptance
            # waits for real current endpoints; warmup time remains in the receipt.
            current, _ = fetch_json(urljoin(url, "api/status"))
            try:
                lag = validate_history_clock(current, history)
                break
            except ValueError:
                if time.perf_counter() - start > 150:
                    raise
                time.sleep(2)
        ranges.append({"range": key, "points": len(rows), "total_ms": elapsed,
                       "coverage_end_utc": history.get("coverage_end_utc"), "source_lag_seconds": lag,
                       "attempts": attempts, "transient_failures": transient_failures,
                       "ready_wait_ms": round((time.perf_counter() - start) * 1000, 3)})
        print(json.dumps({"phase": "history_validated", "url": url, **ranges[-1]}, ensure_ascii=False), flush=True)
    try:
        with urlopen(Request(urljoin(url, "api/status"), data=b"{}", method="POST"), timeout=10):
            raise ValueError("public API accepted a write")
    except HTTPError as exc:
        if exc.code != 405:
            raise
    return {"url": url, "source_health": status["health"], "engine_status": status["engine_status"],
            "strategy_count": len(status["strategies"]), "status_ms": status_ms,
            "source_age_seconds": status["source_age_seconds"], "ranges": ranges, "post_status": 405}


def browser_readback(urls: list[str], output_dir: Path, profile: str,
                     *, observe_minute_refresh: bool = False) -> list[dict]:
    from playwright.sync_api import sync_playwright

    results = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(**browser_profile_launch_options(profile), timeout=5000)
        try:
            for url in urls:
                for name, width, height in (("laptop", 1366, 768), ("tablet", 820, 1180), ("phone", 390, 844)):
                    context = browser.new_context(viewport={"width": width, "height": height})
                    try:
                        page = context.new_page()
                        errors = []
                        warmup_probe = None
                        page.on("pageerror", lambda exc: errors.append(str(exc)))
                        page.on("response", lambda r: errors.append(f"HTTP {r.status}")
                                if r.status >= 400 and r.headers.get("x-taifex-warmup-probe") != "1" else None)
                        start = time.perf_counter()
                        response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        if not response or response.status != 200:
                            raise ValueError("browser page failed to load")
                        # Function form works under the existing no-unsafe-eval CSP.
                        page.wait_for_function("() => document.querySelector('#strategy-select').options.length > 0", timeout=30000)
                        page.wait_for_function("() => document.querySelector('#equity-chart polyline') !== null", timeout=30000)
                        ready_ms = round((time.perf_counter() - start) * 1000, 3)
                        probe_started = None
                        probe_failures = []
                        probe_waiting_states = []

                        def one_warmup_failure(route):
                            if not probe_failures:
                                state = page.locator("#history-status").get_attribute("data-state")
                                if state != "waiting":
                                    raise ValueError("empty curve was incorrectly labeled ready")
                                probe_waiting_states.append(state)
                                probe_failures.append(503)
                                route.fulfill(status=503, body="Source projection warming",
                                              headers={"x-taifex-warmup-probe": "1"})
                            else:
                                route.continue_()

                        if url.startswith("https:") and name == "laptop":
                            # Browser-only failure injection; never alters the
                            # production cache, source or ledger. Subsequent retry
                            # must read the real API and retain the selected range.
                            page.route("**/api/history?range=all", one_warmup_failure)
                        for key in ("all", "1h", "1d"):
                            if key == "all" and url.startswith("https:") and name == "laptop":
                                probe_started = time.perf_counter()
                            if key != "1d":
                                with page.expect_response(lambda r: f"api/history?range={key}" in r.url, timeout=30000):
                                    page.locator(f'button[data-range="{key}"]').click(timeout=5000)
                            else:
                                # Initial 1d payload is an intentional client-cache hit.
                                page.locator(f'button[data-range="{key}"]').click(timeout=5000)
                            if key == "all" and probe_started is not None:
                                page.wait_for_function("() => document.querySelector('#history-status').dataset.state === 'degraded' && document.querySelector('#history-status').textContent.includes('自動重試')", timeout=1000)
                            page.wait_for_function("range => document.querySelector('#equity-chart').dataset.range === range && document.querySelector('#equity-chart polyline') !== null", arg=key, timeout=30000)
                            if key == "all" and probe_started is not None:
                                if probe_failures != [503]:
                                    raise ValueError("warmup retry probe did not inject exactly one 503")
                                warmup_probe = {"synthetic_http_failures": probe_failures,
                                                "state_before_failure": probe_waiting_states[0],
                                                "real_api_recovery_ms": round((time.perf_counter() - probe_started) * 1000, 3),
                                                "range_after_retry": page.locator("#equity-chart").get_attribute("data-range")}
                                page.unroute("**/api/history?range=all", one_warmup_failure)
                        page.locator("#strategy-select").select_option(index=1)
                        page.locator("#equity-chart").scroll_into_view_if_needed(timeout=5000)
                        minute_observation = None
                        if observe_minute_refresh and url.startswith("https:") and name == "laptop":
                            before = page.locator("#equity-chart").get_attribute("data-coverage-end-ns")
                            observed_start = time.perf_counter()
                            # Leave the actual minute scheduler running. No forced
                            # refresh, new navigation or test-only timer override.
                            page.wait_for_function("previous => Number(document.querySelector('#equity-chart').dataset.coverageEndNs) > Number(previous) && document.querySelector('#history-status').dataset.state === 'ready'", arg=before, timeout=125000)
                            minute_observation = {"before_ns": before,
                                                  "after_ns": page.locator("#equity-chart").get_attribute("data-coverage-end-ns"),
                                                  "observed_wait_ms": round((time.perf_counter() - observed_start) * 1000, 3)}
                        state = page.evaluate("""() => ({
                            health:document.querySelector('#live-label').textContent,
                            engine:document.querySelector('#engine-status').textContent,
                            trading_date:document.querySelector('#current-trading-date').textContent,
                            strategy_options:document.querySelector('#strategy-select').options.length,
                            curves:document.querySelectorAll('#curve-wall-grid .curve-card').length,
                            chart_lines:document.querySelectorAll('#equity-chart polyline').length,
                            chart_width:document.querySelector('#equity-chart').getBoundingClientRect().width,
                            chart_container_width:document.querySelector('.chart-scroll').clientWidth,
                            chart_scroll_width:document.querySelector('.chart-scroll').scrollWidth,
                            history_state:document.querySelector('#history-status').dataset.state,
                            history_status:document.querySelector('#history-status').textContent,
                            width:innerWidth, content_width:document.documentElement.scrollWidth,
                            navigation_count:performance.getEntriesByType('navigation').length,
                        })""")
                        if (errors or state["content_width"] > width + 1 or state["navigation_count"] != 1
                                or state["chart_scroll_width"] > state["chart_container_width"] + 1
                                or state["history_state"] != "ready"):
                            raise ValueError(f"browser errors/overflow/reload: {errors}/{state}")
                        image = output_dir / f"{'public' if url.startswith('https:') else 'local'}_{name}.png"
                        page.screenshot(path=str(image))
                        results.append({"url": url, "viewport": name, "ready_ms": ready_ms,
                                        "state": state, "errors": errors, "screenshot": str(image),
                                        "warmup_retry": warmup_probe,
                                        "automatic_minute_update": minute_observation})
                    finally:
                        context.close()
        finally:
            browser.close()
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("artifacts/live/shioaji_taifex_volatility_simulation"))
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--settlement-path", type=Path, default=Path("data_tw_index_options_daily/txo_final_settlement_history.parquet"))
    parser.add_argument("--capture-root", type=Path, default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"))
    parser.add_argument("--capture-id", required=True, help="Original authorized reduction capture; not a replacement snapshot")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--browser-profile", choices=("default", "cpu-2d"), default="default")
    parser.add_argument("--observe-minute-refresh", action="store_true",
                        help="During an active session, observe one unforced public browser minute update")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    receipt = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(), "state": "running",
               "scope": "TAIFEX web service and authorized original-cycle paper recovery; not full historical replay",
               "browser_profile": args.browser_profile, "gpu_webgl_verified": False, "broker_execution_verified": False}
    try:
        if not args.capture_id.isalnum():
            raise ValueError("capture ID must be alphanumeric")
        receipt["ledger"] = ledger_readback(args.state_dir, args.backup_dir, settlement_path=args.settlement_path,
                                          capture_root=args.capture_root, capture_id=args.capture_id)
        receipt["api"] = [api_readback(url) for url in args.url]
        receipt["browser"] = browser_readback(args.url, args.output_dir, args.browser_profile,
                                               observe_minute_refresh=args.observe_minute_refresh)
        receipt["final_status"] = []
        for url in args.url:
            status, elapsed = fetch_json(urljoin(url, "api/status"))
            if status.get("blocked_reason") or status["source_age_seconds"] > status["max_source_age_seconds"]:
                raise ValueError("source became blocked/stale during acceptance")
            receipt["final_status"].append({"url": url, "total_ms": elapsed, "health": status["health"],
                                             "engine_status": status["engine_status"],
                                             "source_age_seconds": status["source_age_seconds"],
                                             "source_updated_at_utc": status["source_updated_at_utc"]})
        receipt["state"] = "accepted"
    except Exception as exc:
        receipt.update(state="failed", error_type=type(exc).__name__, error=str(exc))
    receipt["finished_at_utc"] = datetime.now(UTC).isoformat()
    atomic_write_json(args.output_dir / "acceptance.json", receipt)
    print(json.dumps(receipt, ensure_ascii=False))
    return 0 if receipt["state"] == "accepted" else 1


if __name__ == "__main__":
    raise SystemExit(main())
