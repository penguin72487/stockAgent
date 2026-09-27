from datetime import UTC, datetime
import json
from pathlib import Path

import pytest
import requests

from downloader import finmind_announcements as notices


FIXTURE = Path(__file__).parent / "fixtures" / "finmind_announcements" / "notices.html"
NOW = datetime(2026, 9, 27, tzinfo=UTC)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unit tests must not access the network")
    monkeypatch.setattr(requests.Session, "send", forbidden)


@pytest.fixture
def html():
    return FIXTURE.read_text(encoding="utf-8")


class Response:
    def __init__(self, body=b"", status=200, headers=None, url=notices.SOURCE_URL):
        self.body = body
        self.status_code = status
        self.headers = {"Content-Type": "text/html; charset=utf-8", **(headers or {})}
        self.url = url
        self.history = []
        self.closed = False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]

    def close(self):
        self.closed = True


class Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {"Authorization": "Bearer MUST_NOT_LEAK"}
        self.auth = ("user", "MUST_NOT_LEAK")
        self.cookies = {"token": "MUST_NOT_LEAK"}

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def accepted(root, html):
    return notices.fetch_announcements(root, NOW, Session(Response(html.encode(), headers={
        "ETag": '"sample-etag"', "Last-Modified": "Sat, 26 Sep 2026 01:37:44 GMT"})))


def test_nested_notices_are_single_entries_with_full_scope_and_no_navigation(html):
    entries = notices.parse_announcements(html)
    assert len(entries) == 3
    first = entries[0]
    assert first["datasets"] == ["TaiwanStockPrice"]
    assert first["classification"] == "mixed"
    assert first["is_correction"] and first["known_unavailable"]
    assert first["local_repair_status"] == "not_asserted"
    assert len(first["nested_items"]) == 2
    assert first["nested_items"][1]["nested_items"][0]["text"] == "僅保留原有列,不能宣稱完整。"
    assert "2020-03-09" in first["text"] and "2019-02-20" in first["text"]
    assert first["source_url"] == notices.SOURCE_URL + "#2026-09-26"
    assert first["dataset_links"][0]["datasets"] == ["TaiwanStockPrice"]
    assert entries[1]["classification"] == "informational"
    assert all("TaiwanStockNews" not in e["datasets"] for e in entries)


def test_identity_does_not_use_list_position_or_cosmetic_whitespace(html):
    original = notices.parse_announcements(html)
    shuffled = html.replace("<li>新增 <code>TaiwanStockInfo</code> 欄位。</li>", "")
    shuffled = shuffled.replace("<ul>\n<li>", "<ul>\n<li>新增 <code>TaiwanStockInfo</code> 欄位。</li>\n<li>", 1)
    assert {e["entry_id"] for e in notices.parse_announcements(shuffled)} == {e["entry_id"] for e in original}
    spaced = html.replace("\n", "\n  \t").replace("2020-03-09 已修復", "2020-03-09   已修復")
    assert [e["entry_id"] for e in notices.parse_announcements(spaced)] == [e["entry_id"] for e in original]


def test_nested_revision_changes_identity_but_retains_family(html):
    old = notices.parse_announcements(html)[0]
    new = notices.parse_announcements(html.replace("2020-03-09", "2020-03-10"))[0]
    assert new["entry_id"] != old["entry_id"]
    assert new["revision_family_id"] == old["revision_family_id"]


def test_identical_top_level_items_remain_visible_with_same_semantic_id():
    entries = notices.parse_announcements(
        '<article><h4 id="d">2026-09-26</h4><ul><li>資訊</li><li>資訊</li></ul></article>')
    assert len(entries) == 2 and entries[0]["entry_id"] == entries[1]["entry_id"]


def test_overdeep_nesting_fails_closed(tmp_path):
    body = ('<article><h4 id="d">2026-09-26</h4>' + '<ul><li>x' * 35
            + '</li></ul>' * 35 + '</article>')
    result = notices.fetch_announcements(tmp_path, NOW, Session(Response(body.encode())))
    assert result["state"] == "degraded" and result["error_code"] == "notice_nesting_too_deep"
    assert not (tmp_path / "head.json").exists()


def test_old_version_and_slash_headings_and_legacy_section():
    html = ('<article><h4 id="release">FinMind 1.0.52 (2019-04-06)</h4>'
            '<h5>New Data</h5><ul><li>BalanceSheet</li></ul>'
            '<h4 id="new-function">New Function</h4><ul><li>transpose(data)</li></ul>'
            '<h4 id="201885">2018/8/5</h4><ol><li>資訊</li></ol></article>')
    entries = notices.parse_announcements(html)
    assert [e["notice_date"] for e in entries] == ["2019-04-06", "2019-04-06", "2018-08-05"]
    assert entries[1]["notice_date_basis"] == "preceding_dated_release_h4"


@pytest.mark.parametrize("html, code", [
    ("<nav><h4>2026-09-26</h4></nav>", "article_missing"),
    ("<article><h4 id='x'>Release notes</h4><ul><li>x</li></ul></article>", "notice_heading_date_missing"),
    ("<article><h4 id='x'>2026-09-26</h4></article>", "notice_items_missing"),
    ("<article><h4 id='x'>2026-02-30</h4><ul><li>x</li></ul></article>", "notice_date_invalid"),
    ("<article><h4 id='x'>2026-09-26</h4><p>Changed format</p></article>", "notice_body_shape_changed"),
    ("<article><div><h4 id='x'>2026-09-26</h4></div></article>", "notice_heading_shape_changed"),
])
def test_invalid_page_format_fails_closed(html, code):
    with pytest.raises(notices.AnnouncementError, match=code):
        notices.parse_announcements(html)


def test_untrusted_links_are_not_copied_or_inferred_as_dataset_ids(html):
    html = html.replace('https://finmind.github.io/tutor/TaiwanMarket/Technical/#taiwanstockprice',
                        'https://evil.example/?token=MUST_NOT_LEAK')
    entry = notices.parse_announcements(html)[0]
    assert entry["datasets"] == ["TaiwanStockPrice"]
    assert not entry["dataset_links"]
    assert "MUST_NOT_LEAK" not in json.dumps(entry)


def test_200_persists_hash_snapshot_and_head_without_credentials(tmp_path, html):
    response = Response(html.encode(), headers={"ETag": '"etag"'})
    session = Session(response)
    result = notices.fetch_announcements(tmp_path, NOW, session)
    assert result["state"] == "ok" and result["fetch_status"] == "updated"
    digest = result["snapshot_sha256"]
    assert (tmp_path / "snapshots" / f"{digest}.html").read_bytes() == html.encode()
    assert json.loads((tmp_path / "head.json").read_text())["snapshot_sha256"] == digest
    assert result["latest_notice_date"] == "2026-09-26"
    request, kwargs = session.calls[0]
    assert request.url == notices.SOURCE_URL and request.method == "GET"
    assert not {"Authorization", "Cookie", "Proxy-Authorization"}.intersection(request.headers)
    assert "MUST_NOT_LEAK" not in str(request.headers)
    assert kwargs == {"allow_redirects": False, "stream": True, "timeout": notices.HTTP_TIMEOUT}
    assert response.closed
    assert len(session.calls) == 1


def test_304_requires_hash_and_parse_verified_cache_and_sends_conditionals(tmp_path, html):
    first = accepted(tmp_path, html)
    head_before = (tmp_path / "head.json").read_bytes()
    session = Session(Response(status=304))
    result = notices.fetch_announcements(tmp_path, NOW, session)
    assert result["fetch_status"] == "not_modified" and result["state"] == "ok"
    assert result["snapshot_sha256"] == first["snapshot_sha256"]
    assert result["entries"] == first["entries"] and result["cache_reused"]
    assert session.calls[0][0].headers["If-None-Match"] == '"sample-etag"'
    assert "If-Modified-Since" in session.calls[0][0].headers
    assert (tmp_path / "head.json").read_bytes() == head_before


def test_tampered_cache_304_cannot_succeed_or_rewrite_head(tmp_path, html):
    first = accepted(tmp_path, html)
    snapshot = tmp_path / "snapshots" / f'{first["snapshot_sha256"]}.html'
    snapshot.write_bytes(b"corrupted")
    head_before = (tmp_path / "head.json").read_bytes()
    session = Session(Response(status=304))
    result = notices.fetch_announcements(tmp_path, NOW, session)
    assert result["state"] == "degraded" and result["entries"] == []
    assert result["cache_error"] == "cache_hash_mismatch"
    assert result["error_code"] == "not_modified_without_valid_cache"
    assert "If-None-Match" not in session.calls[0][0].headers
    assert (tmp_path / "head.json").read_bytes() == head_before


def test_valid_hash_but_invalid_cached_format_cannot_accept_304(tmp_path):
    payload = b"<main>Maintenance</main>"
    digest = notices.sha256_bytes(payload)
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    (snapshots / f"{digest}.html").write_bytes(payload)
    (tmp_path / "head.json").write_text(json.dumps({"snapshot_sha256": digest}))
    result = notices.fetch_announcements(tmp_path, NOW, Session(Response(status=304)))
    assert result["state"] == "degraded" and result["entries"] == []
    assert result["cache_error"] == "notice_heading_shape_changed"
    assert result["error_code"] == "not_modified_without_valid_cache"


def test_existing_digest_path_is_immutable_even_if_corrupted(tmp_path, html):
    first = accepted(tmp_path, html)
    snapshot = tmp_path / "snapshots" / f'{first["snapshot_sha256"]}.html'
    snapshot.write_bytes(b"preserve_corruption_for_audit")
    head_before = (tmp_path / "head.json").read_bytes()
    result = accepted(tmp_path, html)
    assert result["state"] == "degraded" and result["error_code"] == "immutable_snapshot_conflict"
    assert snapshot.read_bytes() == b"preserve_corruption_for_audit"
    assert (tmp_path / "head.json").read_bytes() == head_before


@pytest.mark.parametrize("change, error", [
    (lambda html: "<html><main>Maintenance</main></html>", "notice_heading_shape_changed"),
    (lambda html: html.replace("2026-09-26", "2099-01-01"), "future_notice_date"),
    (lambda html: html.replace("2026-09-24", "2026-09-23"), "historical_notice_sections_disappeared"),
])
def test_invalid_new_page_preserves_last_good_and_reports_degraded(tmp_path, html, change, error):
    first = accepted(tmp_path, html)
    head_before = (tmp_path / "head.json").read_bytes()
    result = notices.fetch_announcements(tmp_path, NOW, Session(Response(change(html).encode())))
    assert result["state"] == "degraded" and result["fetch_status"] == "failed"
    assert result["error_code"] == error
    assert result["snapshot_sha256"] == first["snapshot_sha256"]
    assert result["entries"] == first["entries"]
    assert (tmp_path / "head.json").read_bytes() == head_before
    assert json.loads((tmp_path / "status.json").read_text())["state"] == "degraded"


@pytest.mark.parametrize("response, code", [
    (Response(status=302, headers={"Location": "https://evil.example/"}), "redirect_rejected"),
    (Response(status=200, url="https://evil.example/"), "response_url_not_allowlisted"),
    (Response(status=304), "not_modified_without_valid_cache"),
    (Response(status=500), "http_status_rejected"),
    (Response(headers={"Content-Type": "application/json"}), "content_type_rejected"),
    (Response(b"x" * (notices.MAX_RESPONSE_BYTES + 1)), "response_too_large"),
    (Response(b"\xff\xfe"), "html_encoding_invalid"),
])
def test_http_limits_reject_without_head_or_retry(tmp_path, response, code):
    session = Session(response)
    result = notices.fetch_announcements(tmp_path, NOW, session)
    assert result["state"] == "degraded" and result["error_code"] == code
    assert not (tmp_path / "head.json").exists()
    assert len(session.calls) == 1 and response.closed


def test_network_exception_does_not_expose_token_or_destroy_cache(tmp_path, html):
    first = accepted(tmp_path, html)
    head_before = (tmp_path / "head.json").read_bytes()
    session = Session(requests.ConnectionError("https://bad/?token=MUST_NOT_LEAK"))
    result = notices.fetch_announcements(tmp_path, NOW, session)
    assert result["error_code"] == "network_error" and result["state"] == "degraded"
    assert result["snapshot_sha256"] == first["snapshot_sha256"]
    assert "MUST_NOT_LEAK" not in (tmp_path / "status.json").read_text()
    assert (tmp_path / "head.json").read_bytes() == head_before


def test_bad_conditional_header_is_not_sent(tmp_path, html):
    accepted(tmp_path, html)
    head = json.loads((tmp_path / "head.json").read_text())
    head["etag"] = 'etag\r\nAuthorization: Bearer MUST_NOT_LEAK'
    (tmp_path / "head.json").write_text(json.dumps(head))
    session = Session(Response(status=304))
    assert notices.fetch_announcements(tmp_path, NOW, session)["state"] == "ok"
    assert "If-None-Match" not in session.calls[0][0].headers


def test_notice_dates_use_taipei_calendar_and_aware_now(tmp_path, html):
    taipei_day = datetime(2026, 9, 25, 16, 0, tzinfo=UTC)
    assert notices.fetch_announcements(tmp_path, taipei_day, Session(Response(html.encode())))["state"] == "ok"
    with pytest.raises(ValueError, match="timezone_aware"):
        notices.fetch_announcements(tmp_path, datetime(2026, 9, 27), Session())
