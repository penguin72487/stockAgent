#!/usr/bin/env python3
"""Check deployed public prices against canonical files and real browser views.

Read-only for market data/ledgers. Writes acceptance receipts/screenshots only.
No broker login or order is sent. Daily references do not prove strategy health.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sys
import subprocess
import time
from urllib.request import ProxyHandler, build_opener
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.probe_browser_runtime import browser_profile_launch_options


def verify_official_reference_fields(quote: dict, canonical: dict) -> int:
    """Check every published daily field against an independent raw-file read."""
    assert quote["projection_contract"] == "official_daily_quote_v2"
    proofs = quote.get("public_price_display", {}).get("visible_fields", {})
    measures = {"volume_shares", "turnover", "trade_count"}
    count = 0
    for field, value in canonical.items():
        assert quote.get(field) == value, f"official reference differs from canonical field {field}"
        if value is not None and field not in measures:
            proof = proofs.get(field, {})
            assert proof.get("grain") == "daily"
            assert proof.get("session_date") == quote["session_date"]
            assert proof.get("source_field") == field
            expected_license = "11370" if quote["venue"] == "TPEX" else "11549"
            assert proof.get("license_reference") == f"https://data.gov.tw/dataset/{expected_license}"
        count += 1
    if quote["venue"] == "TWSE":
        assert "last_bid" not in quote and "last_ask" not in quote
    elif "next_upper_limit" in canonical or "next_lower_limit" in canonical:
        assert quote["limit_scope"] == "next_exchange_session_published_bounds"
    return count


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


def verify_entry_distances(row: dict) -> int:
    """Independent Decimal price-axis oracle, not the production metric helper."""
    metrics = row.get("entry_price_distances", {})
    proofs = row.get("public_price_display", {}).get("visible_fields", {})
    withheld = row.get("public_price_display", {}).get("withheld_fields", [])
    if row.get("entry_price") is None or "entry_price" in withheld:
        assert not metrics, "a derived metric disclosed a withheld entry price"
        return 0
    entry = Decimal(str(row["entry_price"]))
    count = 0
    for field in ("last_mark_price", "exit_price", "last_exit_price"):
        if row.get(field) is None or field in withheld or field not in proofs:
            assert field not in metrics, "a derived metric disclosed a withheld current price"
            continue
        metric = metrics[field]
        assert metric["contract"] == "public_entry_price_distance_v1"
        price = Decimal(str(row[field]))
        percent = float((price / entry - 1) * 100)
        assert math.isclose(metric["change_pct"], percent, rel_tol=1e-10, abs_tol=1e-10)
        if metric["ticks"] is not None:
            kind = row.get("security_type") or ("etf" if str(row.get("symbol", "")).startswith("00") else "stock")
            if kind == "etf":
                bounds, sizes = ("50",), ("0.01", "0.05")
            elif metric["tick_session_date"] >= "2005-03-01":
                bounds, sizes = ("10", "50", "100", "500", "1000"), ("0.01", "0.05", "0.1", "0.5", "1", "5")
            else:
                bounds, sizes = ("5", "15", "50", "150", "1000"), ("0.01", "0.05", "0.1", "0.5", "1", "5")
            def ordinal(value):
                lower, total = Decimal(0), Decimal(0)
                for upper, size in zip((*map(Decimal, bounds), Decimal("Infinity")), map(Decimal, sizes)):
                    total += (min(value, upper) - lower) / size
                    if value <= upper:
                        return total
                    lower = upper
                raise AssertionError("unreachable price band")
            ticks = float(ordinal(price) - ordinal(entry))
            assert math.isclose(metric["ticks"], ticks, rel_tol=1e-10, abs_tol=1e-7), "cross-band tick distance is wrong"
        count += 1
    return count


def verify_strategy_report(row: dict, original: dict) -> int:
    """Independent ledger comparison, not a call back into the new classifier."""
    reports = 0
    clocks = {"take_profit_price": "entry_at", "stop_trigger_price": "entry_at",
              "eod_limit_price": "eod_limit_submitted_at",
              "closing_auction_limit_price": "closing_auction_order_submitted_at",
              "opening_exit_limit_price": "opening_exit_submitted_at"}
    proofs = row.get("public_price_display", {}).get("visible_fields", {})
    for field, clock_key in clocks.items():
        raw_value = original.get(field)
        if field in {"take_profit_price", "stop_trigger_price"} and (
            type(raw_value) in (float, int) and math.isfinite(raw_value) and raw_value > 0
        ):
            assert row.get(field) == raw_value, f"owner's valid {field} remains withheld or changed"
        if row.get(field) is None:
            continue
        assert row[field] == raw_value, f"published plan changed {field}"
        proof = proofs[field]
        assert proof["publication_basis"] == "owner_paper_strategy_report_v1"
        assert proof["evidence_kind"] == "strategy_plan" and proof["broker_execution_verified"] is False
        assert proof["basis_at"] == original[clock_key], "plan invented its clock"
        assert "license_reference" not in proof and "recorded_at" not in proof
        reports += 1
    return reports


def main() -> None:
    import pyarrow.parquet as pq
    from playwright.sync_api import expect, sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://penguin72487.dpdns.org")
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
        key = (day, venue)
        if key not in expected:
            dataset, code, columns = (("twse_daily_ohlcv", "證券代號", {
                "open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價",
                "volume_shares": "成交股數", "turnover": "成交金額", "trade_count": "成交筆數"})
                                     if venue == "TWSE" else
                                     ("tpex_daily_ohlcv", "代號", {
                "open": "開盤", "high": "最高", "low": "最低", "close": "收盤", "average_price": "均價",
                "last_bid": "最後買價", "last_ask": "最後賣價", "next_reference_price": "次日參考價",
                "next_upper_limit": "次日漲停價", "next_lower_limit": "次日跌停價",
                "volume_shares": "成交股數", "turnover": "成交金額(元)", "trade_count": "成交筆數"}))
            path = ROOT / f"data_tw_public/{dataset}.parquet"
            available = set(pq.read_schema(path).names)
            columns = {key: value for key, value in columns.items() if value in available}
            rows = pq.read_table(path, columns=[code, *columns.values()], filters=[("date", "==", day)]).to_pylist()
            facts, duplicates = {}, set()
            for row in rows:
                values = {}
                for field, column in columns.items():
                    try:
                        number = float(str(row[column]).replace(",", "").strip())
                        valid = (number >= 0 if field in {"volume_shares", "turnover", "trade_count"} else number > 0)
                        values[field] = number if math.isfinite(number) and valid else None
                    except (TypeError, ValueError):
                        values[field] = None
                if row[code] in facts:
                    duplicates.add(row[code])
                facts[row[code]] = values
            expected[key] = (facts, duplicates)
        facts, duplicates = expected[key]
        if symbol in duplicates or symbol not in facts:
            raise ValueError("canonical quote is missing or ambiguous")
        return facts[symbol]
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
                # Use the same calendar RANGE contract as the frontend. A
                # single-date selector correctly rejects a date absent from
                # this product's retained sessions; a range reports that gap.
                url = f"{args.url.rstrip('/')}/{product}/api/{endpoint}?start_date={day}&end_date={day}&limit=100"
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
            checked_fields = 0
            for quote in quotes:
                ref = fact(quote["session_date"], quote["symbol"], quote["venue"])
                checked_fields += verify_official_reference_fields(quote, ref)
            receipt["api"].append({"product": product, "endpoint": endpoint, "date": day,
                "timings_ms": timings, "returned": len(rows), "official_daily_quotes": len(quotes),
                "visible_entry_prices": sum(row.get("entry_price") is not None for row in rows),
                "visible_exit_prices": sum(row.get("exit_price") is not None for row in rows),
                "health": data.get("health"), "independent_price_checks": len(quotes),
                "independent_reference_field_checks": checked_fields,
                "selected_ledger_sessions": data.get("session_dates"),
                "selected_session_recorded": bool(data.get("session_dates")) if endpoint != "status" else None})
            if endpoint == "status":
                policy = data["price_display_policy"]
                assert policy["broker_public_display_permission"] == "unverified"
                assert policy["paper_transaction_reports"] is True
                assert policy["paper_strategy_reports"] is True
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
                receipt["api"][-1]["independent_strategy_report_fields"] = sum(
                    verify_strategy_report(row, originals[day][position_identity(row)]) for row in rows
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
        strategy_fields = sum(verify_strategy_report(row, originals[args.session_date][position_identity(row)]) for row in all_rows)
        distance_fields = sum(verify_entry_distances(row) for row in all_rows)
        assert report_fields > 0, "requested owner transaction reports are missing"
        missing = sorted({row["symbol"] for row in all_rows if not row.get("official_daily_quote")})
        receipt["full_selected_day"] = {
            "date": args.session_date, "positions": len(all_rows),
            "official_daily_quotes": len(all_rows) - sum(not row.get("official_daily_quote") for row in all_rows),
            "missing_official_symbols": missing,
            "visible_exit_prices": sum(row.get("exit_price") is not None for row in all_rows),
            "visible_entry_prices": sum(row.get("entry_price") is not None for row in all_rows),
            "independent_transaction_report_fields": report_fields,
            "independent_strategy_report_fields": strategy_fields,
            "visible_take_profit_prices": sum(row.get("take_profit_price") is not None for row in all_rows),
            "visible_stop_trigger_prices": sum(row.get("stop_trigger_price") is not None for row in all_rows),
            "transaction_accounting_exact": True,
            "strategy_plan_exact": True,
            "independent_entry_distance_fields": distance_fields,
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
                        page.locator('a[href="#price-sources"]').click()
                        page.locator("#quote-display-notice").filter(has_text="0050 開").wait_for(state="visible", timeout=20_000)
                        visible_ms = (time.perf_counter() - start) * 1000
                        notice = page.locator("#quote-display-notice")
                        # Async tables can change page height after the initial
                        # anchor jump. content-visibility skips offscreen text;
                        # inspect a genuinely painted footer, not a DOM shortcut.
                        notice.scroll_into_view_if_needed()
                        expect(notice).to_be_in_viewport(timeout=10_000)
                        expect(notice).to_contain_text(args.session_date, timeout=10_000)
                        assert notice.is_visible()
                        assert args.session_date in notice.inner_text()
                        live_status = fetch_json(f"{args.url.rstrip('/')}/{product}/api/status")
                        reference_session = live_status["official_price_reference"]["session_date"]
                        reference = fact(reference_session, "0050", "TWSE")
                        price_text = lambda value: f"{value:,.2f}".rstrip("0").rstrip(".")
                        expect(notice).to_contain_text(
                            f"0050 開 {price_text(reference['open'])}／收 {price_text(reference['close'])}")
                        expect(notice).to_contain_text("公開展示授權待確認")
                        expect(notice).to_contain_text("模擬帳本成交，非券商成交")
                        expect(notice).to_contain_text("TP／SL 與委託價是使用者公開的策略設定")
                        assert page.locator("main > section").last.get_attribute("id") == "price-sources"
                        assert page.locator("#price-sources #quote-display-notice").count() == 1
                        expect(page.locator("#price-source-list")).to_contain_text("官方日資料")
                        for asset, minimum in (("app.js", 103), ("detail-components.js", 19)):
                            versions = [int(parse_qs(urlparse(url).query).get("v", ["0"])[0])
                                        for url in assets if urlparse(url).path.endswith("/" + asset)]
                            assert versions and max(versions) >= minimum, "old immutable frontend asset is still referenced"
                        screenshot = args.output_dir / f"{product}-{width}.png"
                        page.screenshot(path=str(screenshot))
                        if product == "tw-day-trade":
                            reference_toggle = page.locator("#price-reference-details > summary")
                            reference_toggle.scroll_into_view_if_needed()
                            reference_toggle.click()
                            quote = page.locator("#price-reference-list .public-daily-quote").first
                            quote.wait_for(timeout=20_000)
                            # Shared content-visibility:auto intentionally skips
                            # off-screen text layout. Inspect actual visible rows.
                            quote.scroll_into_view_if_needed()
                            expect(quote).to_be_in_viewport(timeout=10_000)
                            expect(quote).to_contain_text("官方日行情", timeout=20_000)
                            expect(quote.locator("..")).to_contain_text("（臺北）", timeout=10_000)
                            expect(page.locator("#price-source-list")).to_contain_text("模擬帳本成交 · 非券商成交")
                            expect(page.locator("#price-source-list")).to_contain_text("策略設定 · 非行情／非成交")
                            reference_summary = page.locator("#price-reference-list .daily-reference-details summary").filter(has_text="次日界線").first
                            reference_summary.wait_for(state="visible", timeout=20_000)
                            reference_summary.scroll_into_view_if_needed()
                            reference_summary.click()
                            reference_details = reference_summary.locator("..")
                            expect(reference_details).to_have_attribute("open", "")
                            expect(reference_details).to_contain_text("次一交易日申報上界")
                            expect(reference_details).to_contain_text("盤後最後買")
                            expect(reference_details).to_contain_text("盤後最後賣")
                            reference_screenshot = args.output_dir / f"{product}-official-details-{width}.png"
                            page.screenshot(path=str(reference_screenshot))
                            metric = page.locator("#position-body .price-entry-distance").first
                            metric.wait_for(timeout=20_000)
                            metric.scroll_into_view_if_needed()
                            expect(metric).to_contain_text("距進場")
                            expect(metric).to_contain_text("%")
                            expect(metric).to_contain_text("tick")
                            expect(metric.locator("..")).to_contain_text("清算（已平倉）")
                            assert page.locator("#position-body .public-daily-quote, #position-body .owner-strategy-price").count() == 0
                            inline_text = page.locator("#position-body").inner_text()
                            assert "官方日行情" not in inline_text and "來源 臺灣證券交易所" not in inline_text
                            assert "模擬帳本成交 ·" not in inline_text and "策略設定 · 非行情／非成交" not in inline_text
                        dimensions = page.evaluate("() => ({viewport: innerWidth, document: document.documentElement.scrollWidth})")
                        assert dimensions["document"] <= dimensions["viewport"], "page-level horizontal overflow"
                        if product == "tw-day-trade":
                            detail = args.output_dir / f"{product}-prices-{width}.png"
                            page.screenshot(path=str(detail))
                        page.locator('a[href="#price-sources"]').click()
                        notice.scroll_into_view_if_needed()
                        expect(notice).to_be_in_viewport(timeout=10_000)
                        footer_screenshot = args.output_dir / f"{product}-footer-{width}.png"
                        page.screenshot(path=str(footer_screenshot))
                        if product == "tw-overnight":
                            assert page.locator(".timeline-panel .source-list").count() == 0
                            assert page.locator("#price-sources .source-list").count() == 1
                        refresh_preserved_source_details = False
                        if width in (1366, 390) and product == "tw-day-trade":
                            # Repaint through the normal read-only refresh control;
                            # expanded details and the current document must survive.
                            reference_key = reference_summary.locator("../..").get_attribute("data-reference-key")
                            navigations = []
                            page.on("request", lambda request: navigations.append(request.url)
                                    if request.is_navigation_request() and request.resource_type == "document" else None)
                            with page.expect_response(lambda response: "/api/status" in response.url and response.status == 200):
                                page.locator("#force-refresh").click()
                            page.locator('a[href="#price-sources"]').click()
                            expect(page.locator("#price-reference-details")).to_have_attribute("open", "")
                            # Select by the safe DOM attribute rather than text;
                            # source titles may be identical on different dates.
                            reference_item = page.locator(f'#price-reference-list li[data-reference-key="{reference_key}"]')
                            expect(reference_item.locator(".daily-reference-details")).to_have_attribute("open", "")
                            assert not navigations, "refresh reloaded the whole document"
                            refresh_preserved_source_details = True
                        assert not errors, errors
                        receipt["browser"].append({"product": product, "viewport": [width, height],
                            "official_notice_visible_ms": visible_ms, "notice": notice.inner_text(),
                            "page_errors": errors, "dimensions": dimensions, "screenshot": str(screenshot),
                            "frontend_assets": assets,
                            "strategy_plan_visible": product == "tw-day-trade",
                            "source_disclosures_at_page_end": True,
                            "entry_distance_visible": product == "tw-day-trade",
                            "footer_screenshot": str(footer_screenshot),
                            "refresh_preserved_source_details": refresh_preserved_source_details,
                            "reference_details_click_verified": product == "tw-day-trade"})
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
