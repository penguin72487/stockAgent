#!/usr/bin/env python3
"""Verify default/manual/latest date controls in native TW dashboard browsers.

Read-only to the server. Uses the existing explicit CPU-2D browser profile;
does not claim GPU/WebGL, ledger promotion or missing-history acceptance.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.promote_tw_day_trade_replay import _atomic_json


def _check_mode_visibility(observed, expected):
    if expected and sorted(observed["markets"]) != sorted(expected):
        raise ValueError("visible dashboard mode roster differs from requested markets")
    if observed["headings"] != observed["labels"]:
        raise ValueError("rendered mode cards differ from the loaded strategy labels")


def _check_counterfactual_disclosure(observed, session_date):
    rows, clocks = observed["rows"], observed["rendered_clocks"]
    if (not rows or len(rows) != len(clocks)
            or any(row.get("session_date") != session_date
                   or row.get("counterfactual_open_replay") is not True
                   or row.get("replay_signal_origin") != "current_model_counterfactual"
                   or row.get("has_private_signal_id") is True
                   for row in rows)):
        raise ValueError("loaded signals are not the requested counterfactual prefix")
    if any("目前模型反事實回放（非當時訊號）" not in text or "原始訊號" in text for text in clocks):
        raise ValueError("rendered prefix clocks misrepresent historical signal provenance")


def main():
    from playwright.sync_api import sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--expected-first-session")
    parser.add_argument("--verify-counterfactual-first-session", action="store_true")
    parser.add_argument("--expected-market", action="append", default=[])
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if len(args.expected_market) != len(set(args.expected_market)):
        parser.error("expected markets must be unique")
    if args.verify_counterfactual_first_session and not args.expected_first_session:
        parser.error("counterfactual disclosure check requires --expected-first-session")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    receipt = {"observed_at_utc": datetime.now(UTC).isoformat(), "status": "running",
               "browser_profile": "cpu-2d", "gpu_webgl_verified": False,
               "missing_history_accepted": False, "checks": []}
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(**browser_profile_launch_options("cpu-2d"))
            try:
                for url_index, url in enumerate(args.url):
                    for name, width, height in (("laptop", 1366, 768), ("phone", 390, 844)):
                        context = browser.new_context(viewport={"width": width, "height": height})
                        try:
                            page = context.new_page()
                            errors, navigations, failed_requests = [], [], []
                            page.on("pageerror", lambda exc: errors.append(str(exc)))
                            page.on("requestfailed", lambda request: failed_requests.append(
                                {"url": request.url, "failure": request.failure})
                                if "/api/" in request.url else None)
                            page.on("framenavigated", lambda frame: navigations.append(frame.url)
                                    if frame == page.main_frame else None)
                            response = page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                            if not response or not response.ok:
                                raise ValueError("dashboard document unavailable")
                            page.wait_for_function("() => typeof snapshot !== 'undefined' && snapshot && availableDetailDates.length > 0",
                                                   timeout=60_000)
                            latest = page.evaluate("snapshot.session_date")
                            start = page.locator("#detail-start-date")
                            end = page.locator("#detail-end-date")
                            if start.input_value() != args.start_date or end.input_value() != latest:
                                raise ValueError(f"default requested performance window differs: {page.url} "
                                                 f"{start.input_value()}..{end.input_value()} expected {args.start_date}..{latest}")
                            page.wait_for_function("() => chartHistory && !historyInFlight", timeout=90_000)
                            default_history = page.evaluate("({start:chartHistory.start_date,end:chartHistory.end_date,"
                                "points:chartHistory.returned_points,actualEarliest:availableDetailDates.at(-1),"
                                "rangeSummary:chartHistory.range_summary,loadError:historyLoadError})")
                            if (default_history["start"] != args.start_date or default_history["end"] != latest
                                    or default_history["loadError"]):
                                raise ValueError("loaded history does not match requested controls")
                            if (args.expected_first_session is not None
                                    and default_history["actualEarliest"] != args.expected_first_session):
                                raise ValueError("visible history does not start at the requested observed session")
                            visibility = page.evaluate("({markets:snapshot.modes.map(m=>m.market),"
                                "labels:snapshot.modes.map(m=>strategyLabel(m)),"
                                "headings:Array.from(document.querySelectorAll('#mode-cards .mode-card h3'),"
                                "e=>e.textContent.trim())})")
                            _check_mode_visibility(visibility, args.expected_market)
                            page.locator("#follow-latest-session").click(timeout=10_000)
                            page.wait_for_function("() => followLatestSession && !followPerformanceRange && "
                                "document.getElementById('detail-start-date').value === snapshot.session_date && "
                                "document.getElementById('detail-end-date').value === snapshot.session_date")
                            manual = page.evaluate("availableDetailDates[Math.min(1, availableDetailDates.length-1)]")
                            start.fill(manual)
                            start.press("Tab")
                            page.wait_for_function("() => !followLatestSession && !followPerformanceRange")
                            if start.input_value() != manual or end.input_value() != latest:
                                raise ValueError("manual calendar selection was overwritten")
                            page.locator("#performance-range").click(timeout=10_000)
                            page.wait_for_function("() => followPerformanceRange && !followLatestSession")
                            page.wait_for_function("() => chartHistory && !historyInFlight", timeout=90_000)
                            page.wait_for_function("() => !positionLoading && !signalLoading && positionDataRevision && signalDataRevision",
                                                   timeout=90_000)
                            if start.input_value() != args.start_date or end.input_value() != latest:
                                raise ValueError("year-to-date button did not restore requested window")
                            after = page.evaluate("chartHistory.range_summary")
                            if after != default_history["rangeSummary"]:
                                raise ValueError("unchanged full-history range changed absolute accounting")
                            overflow = page.evaluate("Math.max(0, document.documentElement.scrollWidth-innerWidth)")
                            if errors or len(navigations) != 1 or overflow > 1:
                                raise ValueError(f"browser errors/reload/overflow: {errors}, {len(navigations)}, {overflow}")
                            image = args.output_dir / f"page-{url_index}-{name}.png"
                            page.screenshot(path=str(image), full_page=True)
                            cards_image = args.output_dir / f"mode-cards-{url_index}-{name}.png"
                            page.locator("#mode-cards").screenshot(path=str(cards_image))
                            disclosure = None
                            if args.verify_counterfactual_first_session:
                                first = args.expected_first_session
                                start.fill(first)
                                start.press("Tab")
                                end.fill(first)
                                end.press("Tab")
                                page.wait_for_function("expected => !signalLoading && signalRows.length > 0 && "
                                    "signalDataRevision === detailDataRevision('signals') && "
                                    "signalSelectionKey === detailSelectionKey('signals') && "
                                    "signalRows.every(row => row.session_date === expected)", arg=first, timeout=90_000)
                                observed = page.evaluate("({rows:signalRows.map(row=>({session_date:row.session_date,"
                                    "replay_signal_origin:row.replay_signal_origin,has_private_signal_id:Object.hasOwn(row,'signal_id'),"
                                    "counterfactual_open_replay:row.counterfactual_open_replay})),"
                                    "rendered_clocks:Array.from(document.querySelectorAll('#signal-body tr'),"
                                    "row=>row.querySelector('td:first-child small')?.textContent || '')})")
                                _check_counterfactual_disclosure(observed, first)
                                disclosure = {"session_date": first, "rendered_rows": len(observed["rows"]),
                                              "marked_current_model_counterfactual": True}
                                if errors or len(navigations) != 1:
                                    raise ValueError("counterfactual selection caused browser errors or document reload")
                            receipt["checks"].append({"url": url, "viewport": name, "width": width,
                                "default_history": default_history, "latest_single_day": "passed",
                                "mode_visibility": visibility,
                                "mode_cards_screenshot": str(cards_image), "counterfactual_disclosure": disclosure,
                                "manual_start": manual, "restored_ytd": "passed", "document_navigations": len(navigations),
                                "horizontal_overflow_px": overflow, "page_errors": errors, "screenshot": str(image)})
                            _atomic_json(args.output_dir / "receipt.json", receipt)
                        except Exception:
                            failed = {"url": url, "viewport": name, "page_errors": errors,
                                      "document_navigations": navigations,
                                      "failed_requests": failed_requests[-20:]}
                            try:
                                failed["state"] = page.evaluate("({signalLoading,signalLoadError,signalLoadNotice,"
                                    "signalDataRevision,expectedSignalRevision:detailDataRevision('signals'),"
                                    "signalRows:signalRows.length,positionLoading,positionLoadError,"
                                    "positionDataRevision,expectedPositionRevision:detailDataRevision('positions'),"
                                    "historyInFlight,historyLoadError,start:document.getElementById('detail-start-date').value,"
                                    "end:document.getElementById('detail-end-date').value})")
                                failed_image = args.output_dir / f"failed-{url_index}-{name}.png"
                                page.screenshot(path=str(failed_image), full_page=True, timeout=5_000)
                                failed["screenshot"] = str(failed_image)
                            except Exception as diagnostic_error:
                                failed["diagnostic_error"] = str(diagnostic_error)
                            receipt["failed_case"] = failed
                            raise
                        finally:
                            context.close()
            finally:
                browser.close()
        receipt["status"] = ("accepted_date_controls_and_requested_visibility"
            if args.expected_market or args.expected_first_session else "accepted_date_controls_only")
    except Exception as exc:
        receipt.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        _atomic_json(args.output_dir / "receipt.json", receipt)
    print(f"{receipt['status']}: {len(receipt['checks'])} native browser cases")


if __name__ == "__main__":
    main()
