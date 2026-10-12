"""Review observations never become source permissions or collector revisions."""
from copy import deepcopy
from datetime import timedelta
import json

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data.news_source_catalog import (
    configuration_revision, load_news_configuration, load_source_review_ledger,
    planning_review_path,
)
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision
from test_news_dashboard import NOW, _seed


def _ledger():
    return {"contract_version": 1, "providers": {
        "moneydj": {"capture_decision": "permission_required",
                    "evidence_urls": ["https://example.org/terms"],
                    "observation": "PRIVATE_REVIEW_BODY_AND_KEY",
                    "reviewed_at_utc": NOW.isoformat()}},
        "provider_aliases": {"unregistered:example.org": "moneydj"}}


def _configure(root, *, ledger=None):
    _seed(root)
    config_path = root / "configs/news_fulltext.json"
    config = json.loads(config_path.read_text())
    atomic_write_json(config_path, {**config, "provider_catalog": "news/catalog.json"})
    atomic_write_json(root / "configs/news/catalog.json", {
        "contract_version": 1, "providers": [], "planning_review_ledger": "reviews.json"})
    path = root / "configs/news/reviews.json"
    atomic_write_json(path, ledger or _ledger())
    return config_path, path


def test_private_ledger_preserves_evidence_aliases_and_exact_review_precedence(tmp_path):
    value = _ledger()
    exact = {**value["providers"]["moneydj"], "capture_decision": "review_incomplete"}
    value["providers"]["unregistered:example.org"] = exact
    path = tmp_path / "reviews.json"
    atomic_write_json(path, value)
    reviews = load_source_review_ledger(path)
    assert reviews["moneydj"]["observation"] == "PRIVATE_REVIEW_BODY_AND_KEY"
    assert reviews["unregistered:example.org"] == exact


@pytest.mark.parametrize("field,bad", [
    ("contract_version", True), ("contract_version", 2), ("providers", []),
    ("providers", {"bad\nname": {}}), ("providers", {"moneydj": {"capture_decision": []}}),
    ("provider_aliases", []), ("provider_aliases", {"x": "missing"}),
])
def test_bad_review_ledger_is_rejected_without_raw_error_text(tmp_path, field, bad):
    path = tmp_path / "reviews.json"
    atomic_write_json(path, {**_ledger(), field: bad})
    with pytest.raises(ValueError, match="invalid source review"):
        load_source_review_ledger(path)


@pytest.mark.parametrize("relative", ["/absolute.json", "../../outside.json", ".env", 1, ""])
def test_review_path_cannot_escape_catalog_or_read_env(tmp_path, relative):
    config_path, _ = _configure(tmp_path)
    catalog = tmp_path / "configs/news/catalog.json"
    value = json.loads(catalog.read_text())
    atomic_write_json(catalog, {**value, "planning_review_ledger": relative})
    with pytest.raises(ValueError):
        planning_review_path(config_path)


def test_review_symlink_escape_is_rejected(tmp_path):
    config_path, path = _configure(tmp_path)
    outside = tmp_path / "outside.json"
    atomic_write_json(outside, _ledger())
    link = path.parent / "symlink.json"
    link.symlink_to(outside)
    catalog = tmp_path / "configs/news/catalog.json"
    value = json.loads(catalog.read_text())
    atomic_write_json(catalog, {**value, "planning_review_ledger": "symlink.json"})
    with pytest.raises(ValueError, match="catalog-local"):
        planning_review_path(config_path)


def test_malformed_catalog_does_not_create_attribute_error(tmp_path):
    config_path, _ = _configure(tmp_path)
    atomic_write_json(tmp_path / "configs/news/catalog.json", [])
    with pytest.raises(ValueError, match="invalid news source catalog"):
        planning_review_path(config_path)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["source_reviews_state"] == "unavailable"


def test_oversized_review_is_not_read_or_projected(tmp_path):
    config_path, path = _configure(tmp_path)
    atomic_write_json(path, {**_ledger(), "large_private_note": "x" * (2 << 20)})
    with pytest.raises(ValueError, match="oversized"):
        load_source_review_ledger(path)
    with pytest.raises(ValueError, match="bounded"):
        planning_review_path(config_path)
    assert build_news_public_status(tmp_path, now=NOW)["source_reviews_state"] == "unavailable"


def test_review_edit_refreshes_only_dashboard_not_owner_or_capture_configuration(tmp_path):
    config_path, path = _configure(tmp_path)
    before = configuration_revision(config_path), news_snapshot_revision(tmp_path), load_news_configuration(config_path)
    result = build_news_public_status(tmp_path, now=NOW)
    rows = {s["id"]: s for s in result["providers"]}
    assert rows["unregistered:example.org"]["source_policy_review"] == "permission_required"
    assert rows["unregistered:example.org"]["source_reviewed_at_utc"] == NOW.isoformat()
    assert not rows["unregistered:example.org"]["enabled"]
    assert rows["unregistered:example.org"]["conditional_eta_seconds"] is None
    assert rows["cnyes"]["source_policy_review"] == "active_existing_policy"
    assert rows["cnyes"]["enabled"]
    assert result["source_policy_review_counts"] == {"permission_required": 1, "active_existing_policy": 1}
    assert sum(result["source_policy_review_counts"].values()) == len(result["providers"])
    assert not result["scope"]["source_reviews_grant_capture_permission"]
    assert "PRIVATE_REVIEW" not in json.dumps(result)
    assert "https://example.org/terms" not in json.dumps(result)
    value = _ledger()
    value["providers"]["moneydj"]["capture_decision"] = "review_incomplete"
    atomic_write_json(path, value)
    assert before[0] == configuration_revision(config_path)
    assert before[1] != news_snapshot_revision(tmp_path)
    assert before[2] == load_news_configuration(config_path)
    rows = {s["id"]: s for s in build_news_public_status(tmp_path, now=NOW)["providers"]}
    assert rows["unregistered:example.org"]["source_policy_review"] == "review_incomplete"
    assert not rows["unregistered:example.org"]["enabled"]


def test_review_does_not_revoke_an_active_runtime_license(tmp_path):
    value = _ledger()
    value["providers"]["cnyes"] = deepcopy(value["providers"]["moneydj"])
    _configure(tmp_path, ledger=value)
    result = build_news_public_status(tmp_path, now=NOW)
    source = next(s for s in result["providers"] if s["id"] == "cnyes")
    assert source["enabled"] and source["source_policy_review"] == "permission_required"
    # Pending work without an inflight request is backfilling, not running.
    assert source["state"] == "backfilling" and source["known_urls"] == 6


@pytest.mark.parametrize("clock", [True, "not-a-clock", "2026-10-08T15:00:00", (NOW + timedelta(seconds=6)).isoformat()])
def test_review_with_unverified_clock_is_explicit_not_falsely_reviewed(tmp_path, clock):
    value = _ledger()
    value["providers"]["moneydj"]["reviewed_at_utc"] = clock
    _configure(tmp_path, ledger=value)
    result = build_news_public_status(tmp_path, now=NOW)
    row = next(s for s in result["providers"] if s["id"] == "unregistered:example.org")
    assert row["source_policy_review"] == "review_unverified"
    assert row["source_reviewed_at_utc"] is None and not row["enabled"]


def test_bad_review_file_preserves_successful_capture_counts_and_state(tmp_path):
    _, path = _configure(tmp_path)
    atomic_write_json(path, {"contract_version": True})
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["health"] == "running" and result["snapshot_consistent"]
    assert result["acquisition"]["stored_body_articles"] == 2
    assert result["source_reviews_state"] == "unavailable"
    assert result["source_policy_review_counts"] == {"active_existing_policy": 1, "not_reviewed": 1}


def test_missing_review_configuration_is_explicit_and_does_not_call_publishers(tmp_path):
    _seed(tmp_path)
    assert planning_review_path(tmp_path / "configs/news_fulltext.json") is None
    result = build_news_public_status(tmp_path, now=NOW)
    assert result["source_reviews_state"] == "not_configured"
    assert not result["scope"]["publisher_calls_on_page_view"]
