"""Clock validation is actionable, not a body, permanent absence or private error."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from playwright.sync_api import expect, sync_playwright

from scripts.probe_browser_runtime import browser_profile_launch_options
from stockagent.data.news_source_catalog import (
    NEWS_OUTCOME_GROUPS, NEWS_OUTCOME_STATES, classify_news_outcomes,
)
from stockagent.live.news_dashboard import _STATES, build_news_public_status
from test_news_dashboard import _seed, _save


CLOCK = "invalid_bulk_publication_clock"
PRIVATE_ERROR = "invalid_bulk_publication_clock_PRIVATE_TOKEN"


@pytest.mark.parametrize("registered,enabled", [(True, True), (True, False), (False, True), (False, False)])
def test_clock_failure_never_changes_kind_with_registration_or_permission(registered, enabled):
    states = {CLOCK: 3, "complete": 2, PRIVATE_ERROR: 1}
    original = deepcopy(states)
    value = classify_news_outcomes(states, registered=registered, body_enabled=enabled)
    assert value["contract_version"] == 2
    assert value["categories"]["repair_required"] == 3
    assert value["categories"]["unknown_outcome"] == 1
    assert value["categories"]["complete_state"] == 2
    assert value["known_urls"] == sum(value["categories"].values()) == 6
    assert value["definitively_unavailable_urls"] is value["closure_progress_ratio"] is None
    assert not value["all_history_complete"] and not value["queue_mutation_possible"]
    assert states == original


def test_public_vocabulary_is_shared_exact_and_does_not_accept_private_prefixes():
    assert _STATES is NEWS_OUTCOME_STATES
    assert NEWS_OUTCOME_STATES == {"complete"}.union(*NEWS_OUTCOME_GROUPS.values())
    assert CLOCK in _STATES and PRIVATE_ERROR not in _STATES
    assert sum(map(len, NEWS_OUTCOME_GROUPS.values())) == len(NEWS_OUTCOME_STATES) - 1


def _clock_payload(root, now):
    status = _seed(root, now=now)
    counts = {"complete": 2, CLOCK: 3, PRIVATE_ERROR: 1}
    status["providers"]["cnyes"].update(states=counts, stored_body_urls=2)
    status["providers"]["unregistered:example.org"]["stored_body_urls"] = 0
    status.update(states={**counts, "awaiting_authorization": 4},
                  provider_stored_body_counter_contract_version=1)
    _save(root, status)
    return build_news_public_status(root, now=now)


@pytest.mark.parametrize("age_seconds", [0, 181])
def test_readonly_projection_preserves_clock_counts_but_never_invents_body_or_eta(tmp_path, age_seconds):
    now = datetime.now(UTC)
    _clock_payload(tmp_path, now)
    value = build_news_public_status(tmp_path, now=now + timedelta(seconds=age_seconds))
    assert value["read_only"] and value["snapshot_consistent"]
    assert value["news_outcome_classification_contract_version"] == 2
    row = next(source for source in value["providers"] if source["id"] == "cnyes")
    assert row["states"] == {"complete": 2, CLOCK: 3, "other": 1}
    assert row["known_urls"] == 6 and row["stored_body_urls"] == row["complete_urls"] == 2
    assert row["stored_body_progress_ratio"] == 2 / 6
    assert row["outcome_progress"]["categories"]["repair_required"] == 3
    assert row["outcome_progress"]["categories"]["unknown_outcome"] == 1
    groups = value["outcome_progress"]["categories"]
    assert sum(groups.values()) == value["acquisition"]["known_urls"] == 10
    assert groups["repair_required"] == 3 and groups["registration_required"] == 4
    assert value["acquisition"]["stored_body_articles"] == 2
    assert row["conditional_eta_seconds"] is row["conditional_finish_at_utc"] is None
    assert value["outcome_progress"]["closure_progress_ratio"] is None
    encoded = json.dumps(value)
    assert all(secret not in encoded for secret in ("PRIVATE_TOKEN", "PRIVATE_BODY", str(tmp_path)))


@pytest.mark.parametrize("width,height", [(1440, 1000), (390, 844)])
def test_rendered_clock_label_and_group_keep_real_body_progress(tmp_path, width, height):
    payload = _clock_payload(tmp_path, datetime.now(UTC))
    services = Path(__file__).resolve().parents[1] / "services"
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"), timeout=10000)
        try:
            page = browser.new_page(viewport={"width": width, "height": height})
            errors, forbidden = [], []
            page.on("pageerror", lambda error: errors.append(str(error)))

            def route(request):
                parts = urlsplit(request.request.url)
                if parts.netloc != "news-preview.test":
                    forbidden.append(parts.netloc)
                    request.abort()
                elif parts.path == "/news/api/status":
                    request.fulfill(content_type="application/json", body=json.dumps(payload))
                else:
                    mounts = {"news": "news_dashboard", "shioaji": "shioaji_api_dashboard",
                              "finlab": "finlab_dashboard", "finmind": "finmind_dashboard"}
                    prefix, _, suffix = parts.path.removeprefix("/").partition("/")
                    relative = (mounts[prefix] + "/" + (suffix or "index.html") if prefix in mounts
                                else "public_dashboards/" + parts.path.removeprefix("/"))
                    target = (services / relative).resolve()
                    if not target.is_relative_to(services) or not target.is_file():
                        request.fulfill(status=404, body="")
                    else:
                        mime = {".js": "text/javascript", ".css": "text/css", ".html": "text/html", ".svg": "image/svg+xml"}.get(target.suffix, "application/octet-stream")
                        request.fulfill(content_type=mime, body=target.read_bytes())

            page.route("**/*", route)
            page.goto("http://news-preview.test/news/", wait_until="networkidle", timeout=15000)
            expect(page.locator("#body-count")).to_have_text("2")
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            card = page.locator("#pipeline-grid .pipeline-card").filter(
                has=page.locator(".news-provider-identity small").filter(has_text="cnyes"))
            expect(card).to_have_count(1)
            expect(card).to_contain_text("發布／異動時間未驗證 × 3")
            expect(card.locator(".mini-progress-track")).to_have_attribute("value", str(2 / 6))
            card.locator('[data-news-outcome-provider="cnyes"] > summary').click()
            expect(card).to_contain_text("解析／時間／端點待修")
            expect(card.locator('[data-news-outcome="repair_required"]')).to_have_text("3")
            expect(card.locator('[data-news-outcome="unknown_outcome"]')).to_have_text("1")
            page.locator('a[href="#acq-progress"]').click()
            page.locator("#outcome-panel > summary").click()
            expect(page.locator('[data-news-outcome-total="repair_required"]')).to_have_text("3")
            expect(page.locator('[data-news-outcome-total="unknown_outcome"]')).to_have_text("1")
            expect(page.locator('[data-news-outcome-total="complete_state"]')).to_have_text("2")
            assert page.evaluate("document.documentElement.scrollWidth") <= width
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()
