#!/usr/bin/env python3
"""Check deployed public prices against canonical files and real browser views.

Read-only for market data/ledgers. Writes acceptance receipts/screenshots only.
No broker login or order is sent. Daily references do not prove strategy health.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sys
import subprocess
import time
from urllib.request import ProxyHandler, build_opener
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.probe_browser_runtime import browser_profile_launch_options


def snapshot_contract_digest(payload: dict) -> str:
    """Archive writer updates only this envelope clock every minute.

    Retain every other field, including every row, raw price, quote/decision
    clock, fee, share count, P&L and deployment identity. This is not a blanket
    timestamp exclusion or a relaxed financial comparison.
    """
    data = {key: value for key, value in payload.items() if key != "archived_at"}
    return hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def position_identity(row: dict) -> tuple:
    return tuple(row.get(key) for key in ("market", "session_date", "symbol", "entry_at"))


def verify_transaction_report(row: dict, original: dict) -> int:
    """Compare public fill reports with an independent unsanitized ledger read."""
    reports = 0
    proofs = row.get("public_price_display", {}).get("visible_fields", {})
    for field in ("entry_price", "inventory_basis_price", "exit_price", "last_exit_price"):
        if row.get(field) is None:
            continue
        assert row[field] == original.get(field), f"published {field} changed recorded accounting price"
        proof = proofs[field]
        if proof.get("publication_basis") == "owner_paper_transaction_report_v1":
            assert proof["evidence_kind"] == "paper_fill" and proof["broker_execution_verified"] is False
            clock_field = "exit_at" if field == "exit_price" else "last_exit_at" if field == "last_exit_price" else "entry_at"
            assert proof["recorded_at"] == original[clock_field], "transaction report changed its recorded clock"
            assert "license_reference" not in proof, "an owner report cannot invent a quote license"
            reports += 1
        else:
            assert proof.get("grain") == "daily", "a price was disclosed with neither owner-report nor daily-source evidence"
    for field in ("entry_at", "exit_at", "filled_shares", "signed_shares", "entry_fee_twd", "net_pnl_twd",
                  "realized_net_pnl_twd", "unrealized_net_pnl_twd", "reconciled_total_net_pnl_twd"):
        if field in row:
            assert row[field] == original.get(field), f"publication changed {field}"
    return reports


def main() -> None:
    import pyarrow.parquet as pq
    from playwright.sync_api import expect, sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://penguin72487.ddnsgeek.com")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--history-date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--browser-profile", choices=("default", "cpu-2d"), default="default")
    parser.add_argument("--address-family", choices=("auto", "ipv4"), default="auto")
    parser.add_argument("--private-url", default="http://127.0.0.1:8766")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    opener = build_opener(ProxyHandler({}))
    def fetch_json(url):
        if args.address_family == "ipv4":
            result = subprocess.run(["curl", "-4", "--silent", "--show-error", "--fail",
                                     "--max-time", "20", url], capture_output=True, check=True, timeout=25)
            return json.loads(result.stdout)
        with opener.open(url, timeout=20) as response:
            return json.load(response)
    receipt = {"observed_at_utc": datetime.now(UTC).isoformat(), "url": args.url,
               "status": "running", "api": [], "browser": [],
               "broker_execution_verified": False, "all_minute_prices_connected": False,
               "browser_profile": args.browser_profile, "gpu_webgl_verified": False}
    receipt["api_address_family"] = args.address_family
    receipt["api_timing_scope"] = "request including DNS/TCP/TLS and new connection"
    protected = [*sorted((ROOT / "artifacts/live/tw_day_trade_simulation/position_history" / args.session_date).glob("*.json")),
                 ROOT / "data_tw_public/twse_daily_ohlcv.parquet",
                 ROOT / "data_tw_public/tpex_daily_ohlcv.parquet"]
    def hashes():
        result = {}
        for path in protected:
            with path.open("rb") as stream:
                result[str(path.relative_to(ROOT))] = hashlib.file_digest(stream, "sha256").hexdigest()
        return result
    before = hashes()
    receipt["protected_files_before"] = before
    def snapshot_contracts():
        return {str(path.relative_to(ROOT)): snapshot_contract_digest(json.loads(path.read_text()))
                for path in protected if path.suffix == ".json"}
    before_contracts = snapshot_contracts()
    expected = {}
    def fact(day, symbol, venue):
        key = (day, symbol, venue)
        if key not in expected:
            dataset, code, columns = (("twse_daily_ohlcv", "證券代號", {"open": "開盤價", "close": "收盤價"})
                                     if venue == "TWSE" else
                                     ("tpex_daily_ohlcv", "代號", {"open": "開盤", "close": "收盤"}))
            rows = pq.read_table(ROOT / f"data_tw_public/{dataset}.parquet",
                                 columns=list(columns.values()),
                                 filters=[("date", "==", day), (code, "==", symbol)]).to_pylist()
            if len(rows) != 1:
                raise ValueError("canonical quote is missing or ambiguous")
            expected[key] = {k: float(str(rows[0][v]).replace(",", "")) for k, v in columns.items()}
        return expected[key]
    try:
        originals = {}
        for day in (args.session_date, args.history_date):
            raw_rows = []
            for offset in range(0, 100_000, 250):
                data = fetch_json(f"{args.private_url.rstrip('/')}/api/positions?date={day}&offset={offset}&limit=250")
                assert data["simulation_only"] is True and data["production_order_possible"] is False
                raw_rows.extend(data.get("rows", []))
                if not data.get("has_more"):
                    break
            assert len(raw_rows) == data["total"], "independent private ledger pagination is incomplete"
            index = {position_identity(row): row for row in raw_rows}
            assert len(index) == len(raw_rows), "ambiguous independent transaction identities"
            originals[day] = index
        for product, endpoint, day in (
            ("tw-day-trade", "status", args.session_date),
            ("tw-overnight", "status", args.session_date),
            ("tw-day-trade", "positions", args.session_date),
            ("tw-day-trade", "positions", args.history_date),
            ("tw-day-trade", "signals", args.session_date),
            ("tw-overnight", "signals", args.session_date),
        ):
            url = f"{args.url.rstrip('/')}/{product}/api/{endpoint}?date={day}"
            if endpoint != "status":
                url += "&limit=100"
            timings = []
            for _ in range(3):
                start = time.perf_counter()
                data = fetch_json(url)
                timings.append((time.perf_counter() - start) * 1000)
            assert data["simulation_only"] is True and data["production_order_possible"] is False
            rows = (data.get("official_price_reference") or {}).get("quotes", []) if endpoint == "status" else data.get("rows", [])
            quotes = [row["official_daily_quote"] for row in rows if row.get("official_daily_quote")]
            if product == "tw-day-trade" or endpoint == "status":
                assert quotes, "expected public official quotes are absent"
            # Independent direct Arrow read, not the new source adapter.
            for quote in quotes[:3]:
                ref = fact(quote["session_date"], quote["symbol"], quote["venue"])
                for field in ("open", "close"):
                    assert quote[field] == ref[field], "public price differs from canonical official value"
            receipt["api"].append({"product": product, "endpoint": endpoint, "date": day,
                "timings_ms": timings, "returned": len(rows), "official_daily_quotes": len(quotes),
                "visible_entry_prices": sum(row.get("entry_price") is not None for row in rows),
                "visible_exit_prices": sum(row.get("exit_price") is not None for row in rows),
                "health": data.get("health"), "independent_price_checks": min(3, len(quotes))})
            if endpoint == "status":
                policy = data["price_display_policy"]
                assert policy["broker_public_display_permission"] == "unverified"
                assert policy["paper_transaction_reports"] is True
                assert "https://www.twse.com.tw/zh/products/information/qa.html" in policy["usage_rules_references"]
                verified_origins = []
                for row in data.get("benchmarks", []):
                    if row.get("instrument_type") != "stock_buy_and_hold" or row.get("entry_price") is None:
                        continue
                    proof = row["public_price_display"]["visible_fields"]["entry_price"]
                    assert proof["grain"] == "daily" and proof["source_field"] == "close"
                    reference = fact(proof["session_date"], row["symbol"], "TWSE")
                    assert row["entry_price"] == reference["close"]
                    verified_origins.append({"symbol": row["symbol"], "price": row["entry_price"],
                                             "session_date": proof["session_date"]})
                receipt["api"][-1]["independent_buy_hold_origins"] = verified_origins
            if product == "tw-day-trade" and endpoint == "positions":
                receipt["api"][-1]["independent_transaction_report_fields"] = sum(
                    verify_transaction_report(row, originals[day][position_identity(row)]) for row in rows
                )

        # Enumerate the entire selected day's bounded public pages, not merely
        # one screenshot or the first hundred positions.
        all_rows = []
        for offset in range(0, 100_000, 250):
            data = fetch_json(f"{args.url.rstrip('/')}/tw-day-trade/api/positions?date={args.session_date}&offset={offset}&limit=250")
            all_rows.extend(data.get("rows", []))
            if not data.get("has_more"):
                break
        assert len(all_rows) == data["total"], "selected-day pagination is incomplete"
        assert {position_identity(row) for row in all_rows} == set(originals[args.session_date]), "public and private ledger scope differs"
        report_fields = sum(verify_transaction_report(row, originals[args.session_date][position_identity(row)]) for row in all_rows)
        assert report_fields > 0, "requested owner transaction reports are missing"
        missing = sorted({row["symbol"] for row in all_rows if not row.get("official_daily_quote")})
        receipt["full_selected_day"] = {
            "date": args.session_date, "positions": len(all_rows),
            "official_daily_quotes": len(all_rows) - sum(not row.get("official_daily_quote") for row in all_rows),
            "missing_official_symbols": missing,
            "visible_exit_prices": sum(row.get("exit_price") is not None for row in all_rows),
            "visible_entry_prices": sum(row.get("entry_price") is not None for row in all_rows),
            "independent_transaction_report_fields": report_fields,
            "transaction_accounting_exact": True,
        }

        with sync_playwright() as pw:
            browser = pw.chromium.launch(**browser_profile_launch_options(args.browser_profile))
            try:
                for product in ("tw-day-trade", "tw-overnight"):
                    for width, height in ((1366, 768), (1920, 1080), (2560, 1440), (768, 1024), (390, 844)):
                        page = browser.new_page(viewport={"width": width, "height": height})
                        errors = []
                        assets = []
                        page.on("pageerror", lambda error: errors.append(str(error)))
                        page.on("response", lambda response: assets.append(response.url)
                                if any(name in response.url for name in ("/app.js?", "/detail-components.js?")) else None)
                        start = time.perf_counter()
                        page.goto(f"{args.url.rstrip('/')}/{product}/", wait_until="domcontentloaded", timeout=20_000)
                        page.locator("#quote-display-notice").filter(has_text="0050 開").wait_for(state="visible", timeout=20_000)
                        visible_ms = (time.perf_counter() - start) * 1000
                        notice = page.locator("#quote-display-notice")
                        assert notice.is_visible()
                        assert args.session_date in notice.inner_text()
                        reference = fact(args.session_date, "0050", "TWSE")
                        price_text = lambda value: f"{value:,.2f}".rstrip("0").rstrip(".")
                        expect(notice).to_contain_text(
                            f"0050 開 {price_text(reference['open'])}／收 {price_text(reference['close'])}")
                        expect(notice).to_contain_text("公開展示授權待確認")
                        expect(notice).to_contain_text("模擬帳本成交，非券商成交")
                        for asset, minimum in (("app.js", 101), ("detail-components.js", 13)):
                            versions = [int(parse_qs(urlparse(url).query).get("v", ["0"])[0])
                                        for url in assets if urlparse(url).path.endswith("/" + asset)]
                            assert versions and max(versions) >= minimum, "old immutable frontend asset is still referenced"
                        screenshot = args.output_dir / f"{product}-{width}.png"
                        page.screenshot(path=str(screenshot))
                        if product == "tw-day-trade":
                            quote = page.locator("#position-body .public-daily-quote").first
                            quote.wait_for(timeout=20_000)
                            # Shared content-visibility:auto intentionally skips
                            # off-screen text layout. Inspect actual visible rows.
                            quote.scroll_into_view_if_needed()
                            expect(quote).to_be_in_viewport(timeout=10_000)
                            expect(quote).to_contain_text("官方日行情", timeout=20_000)
                            expect(quote.locator("..")).to_contain_text("（臺北）", timeout=10_000)
                            expect(quote.locator("..")).to_contain_text("模擬帳本成交", timeout=10_000)
                            expect(quote.locator("..")).to_contain_text("非券商成交", timeout=10_000)
                        dimensions = page.evaluate("() => ({viewport: innerWidth, document: document.documentElement.scrollWidth})")
                        assert dimensions["document"] <= dimensions["viewport"], "page-level horizontal overflow"
                        if product == "tw-day-trade":
                            detail = args.output_dir / f"{product}-prices-{width}.png"
                            page.screenshot(path=str(detail))
                        assert not errors, errors
                        receipt["browser"].append({"product": product, "viewport": [width, height],
                            "official_notice_visible_ms": visible_ms, "notice": notice.inner_text(),
                            "page_errors": errors, "dimensions": dimensions, "screenshot": str(screenshot),
                            "frontend_assets": assets})
                        page.close()
            finally:
                browser.close()
        after = hashes()
        receipt["protected_files_unchanged"] = before == after
        receipt["protected_files"] = after
        receipt["snapshot_envelope_updates"] = [path for path in after if before[path] != after[path]]
        receipt["accounting_contracts_before"] = before_contracts
        receipt["accounting_contracts_after"] = snapshot_contracts()
        receipt["accounting_unchanged"] = before_contracts == receipt["accounting_contracts_after"]
        receipt["source_files_unchanged"] = all(before[path] == after[path] for path in after if path.endswith(".parquet"))
        assert receipt["source_files_unchanged"], "canonical source changed during acceptance; independently recheck new generation"
        assert receipt["accounting_unchanged"], "financial snapshot fields changed during acceptance; investigate owner"
        receipt["status"] = "passed"
    except Exception as exc:
        receipt["status"] = "failed"
        receipt["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        (args.output_dir / "receipt.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": receipt["status"], "receipt": str(args.output_dir / "receipt.json")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
