"""Approved private diagnostics save requests, never invent a new observation."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_cohorts import add_reviewed_supplement, materialize_reviewed_supplements
from downloader.news_fulltext_metrics import record_workload
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_reviewed_supplements import URL, diagnostic, freeze_then_add, supplement_config

OBSERVED = "2026-10-09T03:00:00+00:00"


def real_diagnostic(root, registry, *, url=URL, **overrides):
    report, path = diagnostic(root, url=url)
    raw = pq.read_table(path).to_pylist()[0]
    raw.update(headers_json=json.dumps({"content-type": "text/html; charset=utf-8", "etag": "original-etag"}),
               authorization_json=json.dumps(registry.require(url).authorization), captured_at_utc=OBSERVED)
    raw.update(overrides)
    atomic_write_parquet(path, table_from_records([raw]))
    atomic_write_json(path.with_suffix(".receipt.json"), {"sha256": sha256_file(path), "diagnostic_only": True,
        "accepted_as_article": False, "publish": False})
    return report


def approved(config, tmp_path, **overrides):
    registry = PolicyRegistry(supplement_config(config), base=tmp_path)
    q = NewsQueue(tmp_path / "news", registry)
    original = freeze_then_add(q)
    report = real_diagnostic(tmp_path / "review", registry, **overrides)
    scopes = add_reviewed_supplement(q, [URL], report)
    return q, registry, original, scopes


def test_retained_original_uses_zero_http_preserves_clock_and_recovers(config, tmp_path, monkeypatch):
    def forbidden_http(*args, **kwargs):
        raise AssertionError("retained replay must not create an HTTP owner")
    monkeypatch.setattr("downloader.news_fulltext_http.NewsHTTP.__init__", forbidden_http)
    q, registry, original, scopes = approved(config, tmp_path)
    try:
        result = materialize_reviewed_supplements(q, [URL], lease_seconds=300)
        assert result["materialized_bodies"] == 1
        assert result["publisher_requests_this_command"] == 0
        head = dict(q.db.execute("SELECT * FROM articles WHERE url=?", (URL,)).fetchone())
        row = pq.read_table(q.root / head["parquet_path"]).to_pylist()[0]
        assert row["captured_at_utc"] == row["source_observed_at_utc"] == OBSERVED
        assert head["captured_at_utc"] == head["last_checked_at_utc"] == OBSERVED
        assert row["reparsed_at_utc"] != OBSERVED
        assert row["http_duration_seconds"] is None
        assert row["etag"] == "original-etag"
        assert row["requested_url"] == row["final_url"] == URL
        assert row["historical_point_in_time"] is False
        assert record_workload(row) == "cached_source_reparse"
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
        attempts = head["attempts"]
        assert materialize_reviewed_supplements(q, [URL], lease_seconds=300)["materialized_bodies"] == 0
        assert q.db.execute("SELECT attempts FROM articles WHERE url=?", (URL,)).fetchone()[0] == attempts
        root = q.root
        q.close()
        q = NewsQueue(root, registry)
        assert dict(q.db.execute("SELECT * FROM finite_cohorts").fetchone()) == original
        assert dict(q.db.execute("SELECT * FROM articles WHERE url=?", (URL,)).fetchone())["body_sha256"] == row["body_sha256"]
        assert q.status()["providers"]["publisher"]["finite_supplements"]["stored_bodies"] == 1
    finally:
        q.close()


@pytest.mark.parametrize("failure", ["missing_clock", "naive_clock", "future_clock", "wrong_authorization",
    "invalid_headers", "wrong_mime", "tampered_source", "changed_contract", "needs_canonical_resolution"])
def test_invalid_original_never_claims_or_falls_back_to_http(config, tmp_path, failure):
    overrides = {
        "missing_clock": {"captured_at_utc": None},
        "naive_clock": {"captured_at_utc": "2026-10-09T03:00:00"},
        "future_clock": {"captured_at_utc": (datetime.now(timezone.utc) + timedelta(days=366)).isoformat()},
        "wrong_authorization": {"authorization_json": "{}"},
        "invalid_headers": {"headers_json": "[]"},
        "wrong_mime": {"headers_json": json.dumps({"content-type": "application/json"})},
    }.get(failure, {})
    q, registry, _, scopes = approved(config, tmp_path, **overrides)
    try:
        if failure == "tampered_source":
            scope = json.loads((q.root / scopes[0]["receipt_path"]).read_text())
            path = q.root / scope["source_evidence"][0]["private_source_path"]
            path.write_bytes(path.read_bytes() + b"tampered")
        if failure == "changed_contract":
            registry.sources["publisher"] = replace(registry.sources["publisher"], data_fingerprint="new-contract")
        if failure == "needs_canonical_resolution":
            registry.sources["publisher"] = replace(registry.sources["publisher"], embedded_article_layout="ftv_visible_article_v1")
        with pytest.raises((ValueError, NewsError)):
            materialize_reviewed_supplements(q, [URL], lease_seconds=300)
        head = q.db.execute("SELECT state,attempts,body_sha256 FROM articles WHERE url=?", (URL,)).fetchone()
        assert tuple(head) == ("pending", 0, None)
        assert q.db.execute("SELECT count(*) FROM accepted_batches").fetchone()[0] == 0
    finally:
        q.close()


@pytest.mark.parametrize("state,due", [("retry", 9e12), ("http_403", 0), ("inflight", 0)])
def test_retained_original_does_not_clear_retry_refusal_or_lease(config, tmp_path, state, due):
    q, _, _, _ = approved(config, tmp_path)
    try:
        with q.db:
            q.db.execute("UPDATE articles SET state=?,due=?,error_code='persisted_source_evidence' WHERE url=?", (state, due, URL))
        before = dict(q.db.execute("SELECT * FROM articles WHERE url=?", (URL,)).fetchone())
        result = materialize_reviewed_supplements(q, [URL], lease_seconds=300)
        assert result["materialized_bodies"] == 0 and result["skipped_urls"] == [URL]
        assert dict(q.db.execute("SELECT * FROM articles WHERE url=?", (URL,)).fetchone()) == before
    finally:
        q.close()


def test_native_day_conflict_is_preserved_not_backdated(config, tmp_path):
    q, _, _, _ = approved(config, tmp_path)
    try:
        with q.db:
            q.db.execute("UPDATE articles SET title='older unrelated headline',metadata_date_raw='2019-01-28 08:00:00' WHERE url=?", (URL,))
        result = materialize_reviewed_supplements(q, [URL], lease_seconds=300)
        assert result["metadata_source_publication_day_conflicts"] == 1
        head = q.db.execute("SELECT parquet_path FROM articles WHERE url=?", (URL,)).fetchone()
        row = pq.read_table(q.root / head[0]).to_pylist()[0]
        assert row["metadata_date_raw"] == "2019-01-28 08:00:00"
        assert row["metadata_title_raw"] == "older unrelated headline"
        assert row["metadata_source_publication_day_conflict"] is True
        assert row["published_at_raw"] == "2014-02-03"
    finally:
        q.close()


@pytest.mark.parametrize("raw,expected", [("日期 : 2023/8/22", "2023-08-22"),
    ("2019-01-28 08:00:00", "2019-01-28"), ("2020-01-01T00:00:00+08:00", "2020-01-01"),
    ("2020-02-30", None), ("undated", None), (None, None)])
def test_day_comparison_never_assigns_a_timezone_or_intraday_clock(raw, expected):
    from downloader.news_fulltext_cohorts import _declared_calendar_day
    assert _declared_calendar_day(raw) == expected


def test_complete_batch_validation_precedes_any_claim(config, tmp_path):
    q, registry, _, _ = approved(config, tmp_path)
    other = "https://publisher.test/second-original"
    try:
        with q.db:
            q.add_articles([{"url": other}])
        q.sync_finite_cohorts()
        report = real_diagnostic(tmp_path / "second_review", registry, url=other, captured_at_utc=None)
        add_reviewed_supplement(q, [other], report)
        with pytest.raises(ValueError, match="real HTTP evidence missing"):
            materialize_reviewed_supplements(q, [URL, other], lease_seconds=300)
        rows = q.db.execute("SELECT state,attempts FROM articles WHERE url IN (?,?)", (URL, other)).fetchall()
        assert len(rows) == 2 and all(tuple(row) == ("pending", 0) for row in rows)
    finally:
        q.close()


def test_legacy_review_without_real_http_fields_is_not_fabricated(config, tmp_path):
    registry = PolicyRegistry(supplement_config(config), base=tmp_path)
    q = NewsQueue(tmp_path / "news", registry)
    try:
        freeze_then_add(q)
        report, _ = diagnostic(tmp_path / "legacy_review")
        add_reviewed_supplement(q, [URL], report)
        with pytest.raises(ValueError, match="real HTTP evidence missing"):
            materialize_reviewed_supplements(q, [URL], lease_seconds=300)
        assert q.db.execute("SELECT attempts FROM articles WHERE url=?", (URL,)).fetchone()[0] == 0
    finally:
        q.close()


def test_cli_materializes_without_constructing_http_or_changing_observation(config, tmp_path, monkeypatch, capsys):
    from scripts.download_news_fulltext import main
    q, registry, _, _ = approved(config, tmp_path)
    root = q.root
    report = tmp_path / "review/probe.json"
    q.close()
    monkeypatch.setattr("scripts.download_news_fulltext.configuration_revision", lambda path: "fixed-test-config")
    monkeypatch.setattr("scripts.download_news_fulltext.load_config", lambda path: (config, registry, tmp_path))
    def forbidden_http(*args, **kwargs):
        raise AssertionError("CLI constructed an unnecessary HTTP owner")
    monkeypatch.setattr("downloader.news_fulltext_http.NewsHTTP.__init__", forbidden_http)
    assert main(["fetch", "--root", str(root), "--url", URL, "--reviewed-supplement", str(report),
                 "--materialize-reviewed-response"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["reviewed_retained_materialization"]["materialized_bodies"] == 1
    assert result["reviewed_retained_materialization"]["publisher_requests_this_command"] == 0
    q = NewsQueue(root, registry)
    try:
        assert q.db.execute("SELECT captured_at_utc FROM articles WHERE url=?", (URL,)).fetchone()[0] == OBSERVED
    finally:
        q.close()


@pytest.mark.parametrize("newer_head", [False, True])
def test_durable_receipt_recovers_without_overwriting_later_source_check(config, tmp_path, monkeypatch, newer_head):
    q, registry, _, _ = approved(config, tmp_path)
    root = q.root
    def failed_commit(*args):
        raise RuntimeError("crash after immutable body/receipt before SQLite commit")
    monkeypatch.setattr(q, "_accept_records", failed_commit)
    try:
        with pytest.raises(RuntimeError, match="crash after immutable"):
            materialize_reviewed_supplements(q, [URL], lease_seconds=300)
        assert len(list((root / "receipts").glob("*.json"))) == 1
        if newer_head:
            with q.db:
                q.db.execute("UPDATE articles SET state='http_403',last_checked_at_utc='2026-10-11T00:00:00+00:00',"
                    "error_code='http_403',lease_owner=NULL,lease_until=NULL WHERE url=?", (URL,))
    finally:
        q.close()
    q = NewsQueue(root, registry)
    try:
        row = q.db.execute("SELECT * FROM articles WHERE url=?", (URL,)).fetchone()
        if newer_head:
            assert row["state"] == "http_403" and row["body_sha256"] is None
            assert row["last_checked_at_utc"] == "2026-10-11T00:00:00+00:00"
        else:
            assert row["state"] == "complete" and row["captured_at_utc"] == OBSERVED
            assert row["body_sha256"]
    finally:
        q.close()


@pytest.mark.parametrize("argv", [["enqueue", "--materialize-reviewed-response"],
    ["fetch", "--url", URL, "--materialize-reviewed-response"],
    ["fetch", "--url", URL, "--reviewed-supplement", "proof.json", "--materialize-reviewed-response", "--refresh"]])
def test_cli_rejects_ambiguous_or_refresh_replay(argv):
    from scripts.download_news_fulltext import main
    with pytest.raises(SystemExit) as error:
        main(argv)
    assert error.value.code == 2
