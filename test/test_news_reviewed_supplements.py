"""Diagnostic-verified additions never replace the frozen source scope."""
import hashlib
import json

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, table_from_records, sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_cohorts import add_reviewed_supplement, validate_supplements
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import FakeResponse, article_html, config, http_fixture

URL = "https://publisher.test/new-original"


def supplement_config(config):
    config["providers"][0].update(acquisition_mode="finite", allow_reviewed_supplements=True)
    return config


def diagnostic(root, *, url=URL, body=None, status=200, provider="publisher"):
    body = body or article_html()
    raw_sha = hashlib.sha256(body).hexdigest()
    path = root / "private-source-diagnostics" / (raw_sha + ".parquet")
    atomic_write_parquet(path, table_from_records([{
        "request_url": url, "final_url": url, "provider": provider, "response_sha256": raw_sha,
        "raw_response_body": body, "diagnostic_only": True, "accepted_as_article": False, "publish": False,
    }]))
    atomic_write_json(path.with_suffix(".receipt.json"), {"sha256": sha256_file(path), "diagnostic_only": True,
        "accepted_as_article": False, "publish": False})
    report = root / "probe.json"
    atomic_write_json(report, {"probes": [{"url": url, "provider": provider, "status": status,
        "private_diagnostic_path": str(path), "response_sha256": raw_sha}]})
    return report, path


def freeze_then_add(queue):
    with queue.db:
        queue.add_articles([{"url": "https://publisher.test/old-original"}])
    queue.sync_finite_cohorts()
    original = dict(queue.db.execute("SELECT * FROM finite_cohorts").fetchone())
    with queue.db:
        queue.add_articles([{"url": URL}, {"url": "https://publisher.test/unreviewed-growth"}])
    return original


def test_supplement_is_immutable_explicit_and_recovers_with_original_scope_unchanged(config, tmp_path):
    config = supplement_config(config)
    http, session = http_fixture(config, tmp_path, {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
                                                   URL: FakeResponse(article_html())})
    q = NewsQueue(tmp_path, http.registry)
    try:
        original = freeze_then_add(q)
        assert q.claim_url(URL, 300) is None
        report, path = diagnostic(tmp_path)
        scopes = add_reviewed_supplement(q, [URL], report)
        assert len(scopes) == 1 and scopes[0]["urls"] == 1
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
        assert add_reviewed_supplement(q, [URL], report) == []
        # A refreshed diagnostic cannot alter the immutable approved source.
        atomic_write_json(path.with_suffix(".receipt.json"), {"obsolete": True})
        assert validate_supplements(tmp_path, q.db)[0]["integrity_verified"]
        q.close()
        q = NewsQueue(tmp_path, http.registry)
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
        assert q.claim_url("https://publisher.test/unreviewed-growth", 300) is None
        row = q.claim_url(URL, 300)
        assert row is not None
        q.accept([fetch_article(http, row)])
        assert q.status()["providers"]["publisher"]["finite_supplements"]["stored_bodies"] == 1
        assert len(session.calls) == 2
    finally:
        q.close()
        http.close()


def test_report_beside_quality_artifacts_is_not_confused_with_collector_data_root(config, tmp_path):
    root = tmp_path / "data_news"
    q = NewsQueue(root, PolicyRegistry(supplement_config(config), base=tmp_path))
    try:
        original = freeze_then_add(q)
        report, _ = diagnostic(tmp_path / "artifacts/data_quality/review")
        scopes = add_reviewed_supplement(q, [URL], report)
        assert len(scopes) == 1 and validate_supplements(root, q.db)[0]["integrity_verified"]
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
        assert q.claim_url(URL, 300) is not None
    finally:
        q.close()


@pytest.mark.parametrize("failure", ["source_not_opted_in", "source_refused", "paid_body", "wrong_source", "tampered_file", "outside_root", "too_many"])
def test_supplement_cannot_release_unverified_or_unpermitted_work(config, tmp_path, failure):
    config = supplement_config(config)
    if failure == "source_not_opted_in":
        config["providers"][0]["allow_reviewed_supplements"] = False
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = freeze_then_add(q)
        report, path = diagnostic(tmp_path, status=403 if failure == "source_refused" else 200,
                                  body=article_html(free=False) if failure == "paid_body" else None,
                                  provider="other" if failure == "wrong_source" else "publisher")
        if failure == "tampered_file":
            atomic_write_json(path.with_suffix(".receipt.json"), {"sha256": "0" * 64})
        if failure == "outside_root":
            data = json.loads(report.read_text())
            data["probes"][0]["private_diagnostic_path"] = str(tmp_path.parent / "outside.parquet")
            atomic_write_json(report, data)
        urls = [f"https://publisher.test/{n}" for n in range(17)] if failure == "too_many" else [URL]
        with pytest.raises((ValueError, NewsError)):
            add_reviewed_supplement(q, urls, report)
        assert q.claim_url(URL, 300) is None
        assert q.db.execute("SELECT count(*) FROM finite_capture_supplements").fetchone()[0] == 0
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
    finally:
        q.close()


def test_reviewed_approval_does_not_reset_retry_deadline_or_source_cooldown(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(supplement_config(config), base=tmp_path))
    try:
        freeze_then_add(q)
        with q.db:
            q.db.execute("UPDATE articles SET state='retry_cooldown',due=9999999999,error_code='access_denied',attempts=3 WHERE url=?", (URL,))
        cooldown = tmp_path / "cooldowns/publisher.json"
        atomic_write_json(cooldown, {"provider": "publisher", "not_before_epoch": 9999999999})
        original_sha = sha256_file(cooldown)
        report, _ = diagnostic(tmp_path)
        add_reviewed_supplement(q, [URL], report)
        assert tuple(q.db.execute("SELECT state,due,error_code,attempts FROM articles WHERE url=?", (URL,)).fetchone()) == (
            "retry_cooldown", 9999999999, "access_denied", 3)
        assert q.claim_url(URL, 300) is None and sha256_file(cooldown) == original_sha
    finally:
        q.close()


def test_bad_second_selection_does_not_release_the_valid_first_one(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(supplement_config(config), base=tmp_path))
    try:
        freeze_then_add(q)
        report, _ = diagnostic(tmp_path)
        with pytest.raises(ValueError, match="lacks retained"):
            add_reviewed_supplement(q, [URL, "https://publisher.test/unreviewed-growth"], report)
        assert q.db.execute("SELECT count(*) FROM finite_supplement_members").fetchone()[0] == 0
        assert q.claim_url(URL, 300) is None
    finally:
        q.close()


def test_corrupt_approved_evidence_is_rejected_on_owner_restart(config, tmp_path):
    registry = PolicyRegistry(supplement_config(config), base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    try:
        freeze_then_add(q)
        report, _ = diagnostic(tmp_path)
        scopes = add_reviewed_supplement(q, [URL], report)
        receipt = json.loads((tmp_path / scopes[0]["receipt_path"]).read_text())
        private = tmp_path / receipt["source_evidence"][0]["private_source_path"]
        from downloader.artifact_io import atomic_write_bytes
        atomic_write_bytes(private, b"corrupt")
    finally:
        q.close()
    with pytest.raises(ValueError, match="immutable source digest"):
        NewsQueue(tmp_path, registry)


def test_supplement_opt_in_cannot_enable_continuous_growth(config, tmp_path):
    config["providers"][0]["allow_reviewed_supplements"] = True
    with pytest.raises(ValueError, match="explicit finite"):
        PolicyRegistry(config, base=tmp_path)


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_supplement_opt_in_is_boolean_and_finite(config, tmp_path, value):
    config = supplement_config(config)
    config["providers"][0]["allow_reviewed_supplements"] = value
    with pytest.raises(ValueError, match="explicit finite"):
        PolicyRegistry(config, base=tmp_path)
