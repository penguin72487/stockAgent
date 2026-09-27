"""Offline archive contracts; no provider calls, timers, or production writes."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from email.utils import format_datetime
import gzip
import json
from pathlib import Path
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from scripts import download_taifex_rule_history as collector


def index_html(*rows: tuple[str, str, str]) -> bytes:
    body = "".join(
        f"<tr><td>{day}</td><td><a href='{url}'>{title}</a></td></tr>"
        for day, title, url in rows
    )
    return ("<html><main id='content'><table><tr><th>日期</th><th>標題</th></tr>"
            + body + "</table></main></html>").encode()


@pytest.fixture
def queue(tmp_path):
    conn = collector.open_queue(tmp_path)
    try:
        yield conn
    finally:
        conn.close()


def test_index_keeps_publication_date_without_inventing_timestamp():
    result = collector.parse_index(index_html(("2026/09/23", "保證金調整", "newsDetail?newsType=1&idx=17377")), date(2026, 1, 1), date(2026, 9, 27))
    assert len(result) == 1
    assert result[0]["published_date"] == "2026-09-23"
    assert result[0]["published_at"] is None
    assert result[0]["publication_precision"] == "date_only"
    assert result[0]["category"] == "margins"
    assert result[0]["url"] == "https://www.taifex.com.tw/cht/11/newsDetail?newsType=1&idx=17377"


def test_index_validates_each_row_against_requested_range():
    with pytest.raises(ValueError, match="server_ignored_requested_date_range"):
        collector.parse_index(index_html(("2026/09/23", "公告", "newsDetail?idx=1")), date(2004, 1, 1), date(2004, 12, 31))


@pytest.mark.parametrize("html", [
    b"<html><title>Request Rejected</title>FOR SECURITY REASONS</html>",
    b"<div id='content'>FOR SECURITY REASONS</div>",
    b"<div id='content'>login required</div>",
])
def test_index_waf_and_unrecognized_empty_pages_are_not_complete(html):
    with pytest.raises(ValueError):
        collector.parse_index(html, date(2004, 1, 1), date(2004, 12, 31))


def test_index_official_header_only_empty_result_is_valid():
    assert collector.parse_index(index_html(), date(1997, 1, 1), date(1997, 12, 31)) == []


def test_index_rejects_malformed_dates_and_missing_links():
    with pytest.raises(ValueError):
        collector.parse_index(index_html(("2026/02/30", "公告", "newsDetail?idx=1")), date(2026, 1, 1), date(2026, 12, 31))
    missing_link = "<div id='content'><table><tr><th>日期</th><th>標題</th></tr><tr><td>2026/09/23</td><td>missing link</td></tr></table></div>".encode()
    with pytest.raises(ValueError, match="invalid_announcement_date_or_link"):
        collector.parse_index(missing_link, date(2026, 1, 1), date(2026, 12, 31))


def test_index_deduplicates_identical_rows_but_preserves_distinct_announcements():
    same = ("2026/09/23", "保證金調整", "/file/taifex/CHINESE/11/attach/margintable.csv")
    older = ("2026/09/03", "保證金調整", same[2])
    rows = collector.parse_index(index_html(same, same, older), date(2026, 1, 1), date(2026, 12, 31))
    assert len(rows) == 2
    assert len({row["url"] for row in rows}) == 1


@pytest.mark.parametrize("url", [
    "https://example.com/file.pdf", "https://www.taifex.com.tw.example.com/file.pdf",
    "http://127.0.0.1/file.pdf", "file:///etc/passwd", "javascript:alert(1)",
    "https://user:password@www.taifex.com.tw/file.pdf", "https://www.taifex.com.tw:8443/file.pdf",
])
def test_only_official_public_host_is_fetchable(url):
    assert collector.official_url(url) is None


def test_official_url_strips_fragment_and_normalizes_http():
    assert collector.official_url("http://www.taifex.com.tw/file.pdf#page=2") == "https://www.taifex.com.tw/file.pdf"


def test_external_index_rows_are_visible_but_not_download_allowed():
    rows = collector.parse_index(index_html(("2026/09/23", "公告", "https://external.example/notice.pdf")), date(2026, 1, 1), date(2026, 12, 31))
    assert rows[0]["download_allowed"] is False
    assert rows[0]["url"] == "https://external.example/notice.pdf"


def test_document_links_deduplicate_and_do_not_crawl_ordinary_navigation():
    raw = b"""<html><div id='content'>
      <a href='/file/doc.pdf#page=1'>one</a><a href='/file/doc.pdf'>duplicate</a>
      <a href='https://external.example/doc.pdf'>external</a>
      <a href='/cht/11/hisNews'>history navigation</a>
      <a href='/cht/2/sTF'>specification</a></div></html>"""
    ordinary = collector.document_links(raw, collector.INDEX_URL)
    assert ordinary == [("https://www.taifex.com.tw/file/doc.pdf", "duplicate", "attachment")]
    expanded = collector.document_links(raw, collector.INDEX_URL, discover_specs=True)
    assert {row[2] for row in expanded} == {"attachment", "specification"}


def test_queue_one_url_owns_shared_attachment_work(queue):
    url = "https://www.taifex.com.tw/file/margintable.csv"
    collector.enqueue(queue, url, "attachment", "margins", 5)
    collector.enqueue(queue, url, "attachment", "contract_specs", 0)
    assert queue.execute("SELECT count(*) FROM documents").fetchone()[0] == 1
    assert queue.execute("SELECT priority FROM documents").fetchone()[0] == 0
    queue.execute("UPDATE documents SET state='complete'")
    collector.enqueue(queue, url, "attachment", "margins", 2)
    assert queue.execute("SELECT state FROM documents").fetchone()[0] == "complete"


def test_archive_is_deterministic_content_addressed_and_reconstructible(tmp_path):
    body = "契約代碼,調整後原始保證金\nGUF,0.405\n".encode()
    one = collector.archive_raw(tmp_path, body, "csv")
    two = collector.archive_raw(tmp_path, body, "csv")
    assert one == two
    raw_path, compressed_hash, content_hash = one
    stored = (tmp_path / raw_path).read_bytes()
    assert gzip.decompress(stored) == body
    assert collector.digest(body) == content_hash
    assert collector.digest(stored) == compressed_hash


def test_archive_never_overwrites_damaged_immutable_object(tmp_path):
    path, _, _ = collector.archive_raw(tmp_path, b"first version", "text")
    (tmp_path / path).write_bytes(b"damaged")
    with pytest.raises(ValueError, match="immutable_raw_hash_mismatch"):
        collector.archive_raw(tmp_path, b"first version", "text")
    assert (tmp_path / path).read_bytes() == b"damaged"


class FakeLimiter:
    def __init__(self):
        self.waits = 0
        self.deferrals = []

    def wait(self):
        self.waits += 1

    def defer(self, seconds):
        self.deferrals.append(seconds)


class FakeResponse:
    def __init__(self, body=b"valid body", status=200, headers=None):
        self.body, self.status_code = body, status
        self.headers = {"Content-Type": "text/plain", **(headers or {})}
        self.is_redirect = status in {301, 302, 303, 307, 308}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("test provider rejection", response=self)

    def iter_content(self, _):
        yield self.body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def test_fetch_does_not_request_foreign_host_even_after_official_redirect():
    session = FakeSession([FakeResponse(status=302, headers={"Location": "https://external.example/file.pdf"})])
    with pytest.raises(ValueError, match="off_domain_redirect"):
        collector.fetch(session, FakeLimiter(), collector.INDEX_URL)
    assert len(session.calls) == 1
    assert session.calls[0][1] == collector.INDEX_URL


def test_fetch_respects_shared_limiter_and_429_cooldown():
    limiter = FakeLimiter()
    session = FakeSession([FakeResponse(status=429, headers={"Retry-After": "125"})])
    with pytest.raises(requests.HTTPError):
        collector.fetch(session, limiter, collector.INDEX_URL)
    assert limiter.waits == 1
    assert limiter.deferrals == [125]


@pytest.fixture
def frozen_now(monkeypatch):
    current = datetime(2026, 9, 27, 0, 0, tzinfo=UTC)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return current.astimezone(tz) if tz is not None else current.replace(tzinfo=None)

    monkeypatch.setattr(collector, 'datetime', FixedDateTime)
    return current


@pytest.mark.parametrize('kind', ['seconds', 'http_date'])
def test_fetch_retry_after_longer_than_one_day_is_not_shortened(kind, frozen_now):
    retry_after = ('172800' if kind == 'seconds'
                   else format_datetime(frozen_now + timedelta(days=2), usegmt=True))
    limiter = FakeLimiter()
    session = FakeSession([FakeResponse(status=429, headers={'Retry-After': retry_after})])
    with pytest.raises(requests.HTTPError):
        collector.fetch(session, limiter, collector.INDEX_URL)
    assert limiter.deferrals == [172800]


@pytest.mark.parametrize('error,expected', [
    (collector.ProviderBlockedError('WAF'), True),
    (requests.HTTPError('limited', response=FakeResponse(status=429)), True),
    (requests.HTTPError('missing', response=FakeResponse(status=404)), False),
    (requests.HTTPError('no response'), False),
    (requests.Timeout('timeout'), False),
    (ValueError('parse error'), False),
])
def test_provider_deferral_only_stops_shared_provider_failures(error, expected):
    assert collector.is_provider_deferral(error) is expected


@pytest.mark.parametrize('stage', ['index', 'documents'])
@pytest.mark.parametrize('kind', ['seconds', 'http_date'])
def test_429_stops_batch_before_next_limiter_wait_and_keeps_work_queued(tmp_path, monkeypatch, stage, kind, frozen_now):
    seed_urls = (collector.BASE + '/file/first.csv', collector.BASE + '/file/second.csv')
    monkeypatch.setattr(collector, 'SEEDS', tuple(
        (f'test_{index}', f'test {index}', url) for index, url in enumerate(seed_urls)
    ))
    deadline = frozen_now + timedelta(days=2)
    retry_after = '172800' if kind == 'seconds' else format_datetime(deadline, usegmt=True)
    responses = [FakeResponse(status=429, headers={'Retry-After': retry_after})]
    # The index-failure case leaves an older year unrequested. The document
    # case completes its one index and then fails its first queued document.
    end = '1998-12-31' if stage == 'index' else '1997-12-31'
    if stage == 'documents':
        responses.insert(0, FakeResponse(index_html(), headers={'Content-Type': 'text/html'}))
    session = FakeSession(responses)
    limiter = FakeLimiter()
    original_wait = limiter.wait

    def no_wait_after_deferral():
        if limiter.deferrals:
            pytest.fail('provider deferral must finish the batch before another limiter wait')
        original_wait()

    monkeypatch.setattr(limiter, 'wait', no_wait_after_deferral)
    monkeypatch.setattr(collector, 'SharedRateLimiter', lambda *args, **kwargs: limiter)
    monkeypatch.setattr(collector.requests, 'Session', lambda: session)
    args = ['--output-dir', str(tmp_path), '--start-year', '1997', '--end', end]
    result = collector.main(args)

    assert result == (1 if stage == 'index' else 0)
    assert limiter.waits == len(session.calls) == (1 if stage == 'index' else 2)
    assert limiter.deferrals == [172800]
    assert session.calls[0][1] == collector.INDEX_URL
    report = json.loads((tmp_path / 'manifest.json').read_text())
    assert report['status'] == 'partial'
    assert report['provider_deferred'] is True
    assert datetime.fromisoformat(report['provider_cooldown_until_utc']) == deadline
    assert report['coverage']['all_documents_fetched'] is False
    assert report['errors'] == [{
        'stage': stage, 'error_type': 'HTTPError', 'reason': 'test provider rejection',
        **({'year': 1998} if stage == 'index' else {}),
    }]
    conn = collector.open_queue(tmp_path)
    try:
        tasks = conn.execute('SELECT * FROM documents ORDER BY url').fetchall()
        assert len(tasks) == 2
        if stage == 'index':
            assert all(task['state'] == 'pending' and task['attempts'] == 0 for task in tasks)
            assert report['counts']['index_years_complete'] == 0
        else:
            assert {task['state'] for task in tasks} == {'pending', 'failed'}
            failed = next(task for task in tasks if task['state'] == 'failed')
            assert failed['http_status'] == 429
            assert failed['attempts'] == 1
            assert datetime.fromisoformat(failed['next_retry']) == deadline
            assert report['counts']['index_years_complete'] == 1
    finally:
        conn.close()
    cooldown = json.loads((tmp_path / 'state/provider_cooldown.json').read_text())
    assert datetime.fromisoformat(cooldown['deadline_utc']) == deadline
    assert cooldown['retry_after'] == retry_after

    def forbidden(*args, **kwargs):
        pytest.fail('a later process must defer before creating a limiter or HTTP session')

    monkeypatch.setattr(collector, 'SharedRateLimiter', forbidden)
    monkeypatch.setattr(collector.requests, 'Session', forbidden)
    assert collector.main(args) == 0
    second = json.loads((tmp_path / 'manifest.json').read_text())
    assert second['status'] == 'partial' and second['provider_deferred'] is True
    assert second['provider_cooldown_until_utc'] == report['provider_cooldown_until_utc']
    assert second['counts'] == report['counts']
    assert second['errors'][0]['stage'] == 'provider_cooldown'
    assert second['errors'][0]['error_type'] == 'HTTPError'
    # Offline inspection/export remains usable during the provider cooldown.
    assert collector.main([*args, '--offline-audit']) == 0
    offline = json.loads((tmp_path / 'manifest.json').read_text())
    assert offline['provider_deferred'] is True
    assert offline['counts'] == report['counts']
    assert 'documents' in offline['outputs']


def test_new_short_deferral_does_not_shorten_persisted_provider_deadline(tmp_path, frozen_now):
    long_error = requests.HTTPError('limited', response=FakeResponse(status=429, headers={'Retry-After': '172800'}))
    first = collector.record_provider_cooldown(tmp_path, long_error)
    shorter = collector.record_provider_cooldown(tmp_path, collector.ProviderBlockedError('WAF'))
    assert shorter == first == frozen_now + timedelta(days=2)
    assert collector.provider_cooldown_active(collector.read_provider_cooldown(tmp_path)) is True


def test_expired_provider_cooldown_allows_new_index_request(tmp_path, monkeypatch, frozen_now):
    collector.atomic_write_json(tmp_path / 'state/provider_cooldown.json', {
        'deadline_utc': (frozen_now - timedelta(seconds=1)).isoformat(),
        'error_type': 'HTTPError', 'reason': 'previous 429',
    })
    monkeypatch.setattr(collector, 'SEEDS', ())
    session = FakeSession([FakeResponse(index_html(), headers={'Content-Type': 'text/html'})])
    limiter = FakeLimiter()
    monkeypatch.setattr(collector, 'SharedRateLimiter', lambda *args, **kwargs: limiter)
    monkeypatch.setattr(collector.requests, 'Session', lambda: session)
    assert collector.main(['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31']) == 0
    assert len(session.calls) == limiter.waits == 1
    report = json.loads((tmp_path / 'manifest.json').read_text())
    assert report['status'] == 'current' and report['provider_deferred'] is False


def test_fetch_bounds_response_size_and_rejects_soft_errors(monkeypatch):
    monkeypatch.setattr(collector, "MAX_RESPONSE_BYTES", 4)
    with pytest.raises(ValueError, match="bounded_archive"):
        collector.fetch(FakeSession([FakeResponse(b"12345")]), FakeLimiter(), collector.INDEX_URL)
    monkeypatch.setattr(collector, "MAX_RESPONSE_BYTES", 1024)
    with pytest.raises(ValueError):
        collector.fetch(FakeSession([FakeResponse(b"<html><title>404</title></html>")]), FakeLimiter(), collector.INDEX_URL)


def test_index_cache_reuses_verified_bytes_and_rechecks_corruption(tmp_path, queue, monkeypatch):
    calls = []
    body = index_html(("2025/01/02", "公告調整保證金", "/file/a.pdf"))

    def fake_fetch(*args, **kwargs):
        calls.append(kwargs["payload"])
        return body, "text/html"

    monkeypatch.setattr(collector, "fetch", fake_fetch)
    assert collector.index_year(queue, tmp_path, None, None, 2025, date(2026, 9, 27)) == 1
    assert calls[0]["newsType"] == calls[0]["queryKeyWord"] == ""
    assert calls[0]["queryStartDate"] == "2025/01/01"
    assert calls[0]["queryEndDate"] == "2025/12/31"
    assert collector.index_year(queue, tmp_path, None, None, 2025, date(2026, 9, 27)) == 0
    assert len(calls) == 1
    row = queue.execute("SELECT raw_path FROM index_windows").fetchone()
    # The cache must not call this a valid checkpoint. A same-byte download then
    # refuses to overwrite the corrupt immutable object until separately repaired.
    (tmp_path / row["raw_path"]).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="immutable_raw_hash_mismatch"):
        collector.index_year(queue, tmp_path, None, None, 2025, date(2026, 9, 27))
    assert len(calls) == 2


def test_document_archive_preserves_versions_without_claiming_pit(tmp_path, queue, monkeypatch):
    url = "https://www.taifex.com.tw/file/margintable.csv"
    bodies = [b"contract,value\nTX,100\n", b"contract,value\nTX,120\n"]
    monkeypatch.setattr(collector, "fetch", lambda *args, **kwargs: (bodies.pop(0), "text/csv"))
    collector.enqueue(queue, url, "attachment", "margins", 0)
    task = queue.execute("SELECT * FROM documents WHERE url=?", (url,)).fetchone()
    collector.process_document(queue, tmp_path, task, None, None)
    collector.process_document(queue, tmp_path, task, None, None)
    assert queue.execute("SELECT count(*) FROM versions").fetchone()[0] == 2
    versions = queue.execute("SELECT * FROM versions ORDER BY captured_at").fetchall()
    assert {gzip.decompress((tmp_path / v["raw_path"]).read_bytes()) for v in versions} == {b"contract,value\nTX,100\n", b"contract,value\nTX,120\n"}
    for version in versions:
        parsed = json.loads((tmp_path / version["parsed_path"]).read_text())
        assert parsed["point_in_time_verified"] is False
        assert parsed["historical_attachment_binding_verified"] is False
        shard = tmp_path / parsed['shards']['table_cells']['path']
        assert pq.read_table(shard).num_rows == 1
        assert collector.sha256_path(shard) == parsed['shards']['table_cells']['sha256']


def test_unsupported_document_is_archived_and_reported_as_parse_gap(tmp_path, queue, monkeypatch):
    body = b"\xd0\xcf\x11\xe0legacy word document"
    monkeypatch.setattr(collector, "fetch", lambda *args, **kwargs: (body, "application/msword"))
    collector.enqueue(queue, "https://www.taifex.com.tw/file/old.doc", "attachment", "margins", 0)
    task = queue.execute("SELECT * FROM documents").fetchone()
    collector.process_document(queue, tmp_path, task, None, None)
    report = collector.publish_status(queue, tmp_path, start_year=1997, end=date(2026, 9, 27), running=False, errors=[])
    assert report["counts"]["documents_complete"] == 1
    assert report["counts"]["parse_gaps"] == 1
    assert report["coverage"]["point_in_time_verified"] is False
    assert report["coverage"]["historical_values_complete"] is False
    assert report["coverage"]["all_documents_fetched"] is False


def test_status_missing_index_years_never_reports_all_archive_current(tmp_path, queue):
    report = collector.publish_status(queue, tmp_path, start_year=1997, end=date(2026, 9, 27), running=False, errors=[])
    assert report["coverage"]["all_documents_fetched"] is False
    assert report["status"] != "current"


def test_same_content_on_two_urls_does_not_misattribute_source(tmp_path, queue, monkeypatch):
    monkeypatch.setattr(collector, "fetch", lambda *args, **kwargs: (b"code,value\nTX,100\n", "text/csv"))
    for name in ("a", "b"):
        url = f"https://www.taifex.com.tw/file/{name}.csv"
        collector.enqueue(queue, url, "attachment", "margins", 0)
        task = queue.execute("SELECT * FROM documents WHERE url=?", (url,)).fetchone()
        collector.process_document(queue, tmp_path, task, None, None)
        row = queue.execute("SELECT parsed_path FROM documents WHERE url=?", (url,)).fetchone()
        parsed = json.loads((tmp_path / row["parsed_path"]).read_text())
        # Content-addressed parses may be source-neutral; a per-URL parse must
        # identify this response, not whichever identical URL was fetched first.
        assert parsed.get("source_url") in {None, url}


def test_escaped_legacy_attachment_link_is_discovered():
    body = "<main id='content'><div id='news_content'>調整表&lt;a href='/file/taifex/CHINESE/11/attach/margin.pdf'&gt;附件&lt;/a&gt;</div></main>".encode()
    assert collector.document_links(body, collector.INDEX_URL) == [
        ("https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/margin.pdf", "附件", "attachment"),
    ]


def test_invalid_index_response_is_preserved_before_rejecting_scope(tmp_path, queue, monkeypatch):
    wrong_body = index_html(("2026/09/23", "server returned wrong year", "/file/doc.pdf"))
    monkeypatch.setattr(collector, "fetch", lambda *args, **kwargs: (wrong_body, "text/html"))
    with pytest.raises(ValueError, match="server_ignored_requested_date_range"):
        collector.index_year(queue, tmp_path, None, None, 2004, date(2026, 9, 27))
    assert queue.execute("SELECT count(*) FROM index_windows").fetchone()[0] == 0
    assert queue.execute("SELECT count(*) FROM documents").fetchone()[0] == 0
    assert any(gzip.decompress(path.read_bytes()) == wrong_body for path in tmp_path.rglob("*.gz"))


def test_old_partial_index_window_does_not_count_as_complete_year(tmp_path, queue):
    for year, end in ((1997, "1997-06-30"), (1998, "1998-12-31")):
        queue.execute("INSERT INTO index_windows VALUES(?,?,?,?,?,?,?)", (str(year), f"{year}-01-01", end, collector.now_iso(), 0, "missing", "missing"))
    queue.commit()
    report = collector.publish_status(queue, tmp_path, start_year=1997, end=date(1998, 12, 31), running=False, errors=[])
    assert report["counts"]["index_years_complete"] <= 1
    assert report["coverage"]["all_documents_fetched"] is False
    assert report["status"] != "current"


def test_parser_exception_cannot_discard_downloaded_bytes(tmp_path, queue, monkeypatch):
    from downloader import taifex_rule_parsing

    body = b"unknown future official format"
    monkeypatch.setattr(collector, "fetch", lambda *args, **kwargs: (body, "application/octet-stream"))

    def reject(*args, **kwargs):
        raise RuntimeError("synthetic parser failure")

    monkeypatch.setattr(taifex_rule_parsing, "extract_document", reject)
    collector.enqueue(queue, "https://www.taifex.com.tw/file/unknown.bin", "attachment", "margins", 0)
    task = queue.execute("SELECT * FROM documents").fetchone()
    collector.process_document(queue, tmp_path, task, None, None)
    document = queue.execute("SELECT * FROM documents").fetchone()
    assert document["parsing_status"] == "failed"
    assert gzip.decompress((tmp_path / document["raw_path"]).read_bytes()) == body


def test_external_unfetched_link_prevents_all_documents_claim(tmp_path, queue):
    queue.execute("INSERT INTO index_windows VALUES(?,?,?,?,?,?,?)", ("1997", "1997-01-01", "1997-12-31", collector.now_iso(), 1, "missing", "missing"))
    queue.execute("INSERT INTO announcements VALUES(?,?,?,?,?,?,?)", ("id", "1997-01-02", "external notice", "https://external.example/a.pdf", "margins", 0, "date_only"))
    report = collector.publish_status(queue, tmp_path, start_year=1997, end=date(1997, 12, 31), running=False, errors=[])
    assert report["counts"]["external_links_not_crawled"] == 1
    assert report["coverage"]["all_documents_fetched"] is False


@pytest.mark.parametrize("response", [
    FakeResponse(status=403),
    FakeResponse(b"<html><title>Forbidden</title>FOR SECURITY REASONS</html>"),
])
def test_waf_defers_shared_provider_instead_of_only_failing_one_task(response):
    limiter = FakeLimiter()
    with pytest.raises((ValueError, requests.HTTPError)):
        collector.fetch(FakeSession([response]), limiter, collector.INDEX_URL)
    assert limiter.deferrals and limiter.deferrals[0] >= 60


def test_unitless_margin_shard_has_nullable_string_schema(tmp_path, queue, monkeypatch):
    body = "契約代碼,調整後原始保證金\nGUF,0.405\n".encode()
    monkeypatch.setattr(collector, "fetch", lambda *args, **kwargs: (body, "text/csv"))
    collector.enqueue(queue, "https://www.taifex.com.tw/file/margin.csv", "attachment", "margins", 0)
    task = queue.execute("SELECT * FROM documents").fetchone()
    collector.process_document(queue, tmp_path, task, None, None)
    document = queue.execute('SELECT parsed_path FROM documents').fetchone()
    parsed = json.loads((tmp_path / document['parsed_path']).read_text())
    shard = tmp_path / parsed['shards']['margin_changes']['path']
    table = pq.read_table(shard)
    assert table.schema.field("normalized_value").type == pa.string()
    assert table["normalized_value"].to_pylist() == [None]


def legacy_capture(conn, root, url='https://www.taifex.com.tw/file/legacy.csv', *,
                   body=b'contract,value\nTX,100\n', format='csv', parsing_status='parsed'):
    captured = '2026-09-20T08:00:00+00:00'
    raw, raw_hash, content_hash = collector.archive_raw(root, body, 'body')
    parsed_path = root / 'parsed' / 'legacy' / f'{content_hash}.json'
    collector.atomic_write_json(parsed_path, {
        'format': format, 'parsing_status': parsing_status, 'content_sha256': content_hash,
        'captured_at_utc': captured, 'source_url': url,
    })
    old_shard = root / 'shards' / 'table_cells' / f'{content_hash}.parquet'
    collector.atomic_write_parquet(old_shard, pa.table({'legacy_value': ['untouched']}))
    collector.enqueue(conn, url, 'attachment', 'margins', 0)
    conn.execute('''UPDATE documents SET state='complete',checked_at=?,raw_path=?,raw_sha256=?,
        content_sha256=?,parsed_path=?,parsing_status=?,http_status=200 WHERE url=?''',
        (captured, raw, raw_hash, content_hash, str(parsed_path.relative_to(root)), parsing_status, url))
    conn.execute('INSERT INTO versions VALUES(?,?,?,?,?,?)',
                 (url, content_hash, captured, raw, raw_hash, str(parsed_path.relative_to(root))))
    conn.commit()
    return dict(conn.execute('SELECT * FROM documents WHERE url=?', (url,)).fetchone())


def forbid_http_and_limiter(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('offline reparse must not construct HTTP, limiter, or fetch')
    monkeypatch.setattr(collector.requests, 'Session', forbidden)
    monkeypatch.setattr(collector, 'SharedRateLimiter', forbidden)
    monkeypatch.setattr(collector, 'fetch', forbidden)


def test_offline_reparse_versions_preserve_capture_and_noop_on_current_parser(tmp_path, queue, monkeypatch, frozen_now):
    task = legacy_capture(queue, tmp_path)
    capture = dict(queue.execute('SELECT * FROM versions').fetchone())
    old_files = {path: path.read_bytes() for path in tmp_path.rglob('*') if path.is_file() and 'state' not in path.parts}
    collector.record_provider_cooldown(tmp_path, collector.ProviderBlockedError('test active cooldown'))
    forbid_http_and_limiter(monkeypatch)
    monkeypatch.setattr(collector, 'now_iso', lambda: '2026-09-27T09:00:00+00:00')
    args = ['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31', '--reparse-only']
    assert collector.main(args) == 0
    current = dict(queue.execute('SELECT * FROM documents').fetchone())
    version = collector.current_parser_version()
    assert current['parser_version'] == version
    assert current['checked_at'] == task['checked_at']
    assert current['parsed_at'] == '2026-09-27T09:00:00+00:00'
    assert current['raw_path'] == task['raw_path']
    assert current['raw_integrity_status'] == 'verified'
    assert current['parsed_path'].startswith(f'parsed/v{version}/')
    parsed = json.loads((tmp_path / current['parsed_path']).read_text())
    assert parsed['captured_at_utc'] == capture['captured_at']
    assert parsed['content_type_evidence'] == 'legacy_parsed_format_hint'
    assert parsed['content_type'] == 'text/csv'
    assert parsed['parsed_at_utc'] != parsed['captured_at_utc']
    assert parsed['parser_version'] == version
    for artifact in parsed['shards'].values():
        path = tmp_path / artifact['path']
        assert artifact['path'].startswith(f'shards/v{version}/')
        assert artifact['bytes'] == path.stat().st_size
        assert artifact['sha256'] == collector.sha256_path(path)
        assert artifact['rows'] == pq.read_metadata(path).num_rows
    assert dict(queue.execute('SELECT * FROM versions').fetchone()) == capture
    assert all(path.read_bytes() == content for path, content in old_files.items())
    parse_version = dict(queue.execute('SELECT * FROM parse_versions').fetchone())
    assert parse_version['captured_at'] == capture['captured_at']
    assert parse_version['parsed_sha256'] == collector.sha256_path(tmp_path / current['parsed_path'])
    report = json.loads((tmp_path / 'manifest.json').read_text())
    assert report['provider_deferred'] is True
    assert report['quality']['offline_reparsed_count'] == 1
    assert report['quality']['parser_version_counts'] == {str(version): 1}
    assert report['quality']['parsing_status_counts'] == {'parsed': 1}
    assert collector.main(args) == 0
    assert dict(queue.execute('SELECT * FROM documents').fetchone()) == current
    assert dict(queue.execute('SELECT * FROM parse_versions').fetchone()) == parse_version
    assert json.loads((tmp_path / 'manifest.json').read_text())['quality']['offline_reparsed_count'] == 0
    with pytest.raises(sqlite3.IntegrityError, match='append_only'):
        queue.execute("UPDATE parse_versions SET parsing_status='parsed'")
    queue.rollback()
    with pytest.raises(sqlite3.IntegrityError, match='append_only'):
        queue.execute('DELETE FROM parse_versions')
    queue.rollback()


@pytest.mark.parametrize('damage', ['compressed_hash', 'content_hash', 'missing', 'gzip', 'oversize', 'outside', 'symlink'])
def test_offline_reparse_integrity_failure_is_visible_without_http_or_overwrite(tmp_path, monkeypatch, damage):
    root = tmp_path / 'archive'
    with collector.closing(collector.open_queue(root)) as conn:
        task = legacy_capture(conn, root)
        raw = root / task['raw_path']
        if damage == 'compressed_hash':
            raw.write_bytes(b'damaged immutable evidence')
        elif damage == 'content_hash':
            conn.execute('UPDATE documents SET content_sha256=?', ('0' * 64,))
        elif damage == 'missing':
            raw.unlink()
        elif damage == 'gzip':
            raw.write_bytes(b'not a gzip stream')
            conn.execute('UPDATE documents SET raw_sha256=?', (collector.sha256_path(raw),))
        elif damage == 'oversize':
            monkeypatch.setattr(collector, 'MAX_RESPONSE_BYTES', 4)
        else:
            outside = tmp_path / 'outside.gz'
            outside.write_bytes(raw.read_bytes())
            if damage == 'outside':
                conn.execute('UPDATE documents SET raw_path=?', (str(outside),))
            else:
                raw.unlink()
                raw.symlink_to(outside)
        conn.commit()
        before = raw.read_bytes() if raw.exists() else None
    forbid_http_and_limiter(monkeypatch)
    assert collector.main(['--output-dir', str(root), '--start-year', '1997', '--end', '1997-12-31', '--reparse-only']) == 1
    with collector.closing(collector.open_queue(root)) as conn:
        row = conn.execute('SELECT * FROM documents').fetchone()
        assert row['state'] == 'integrity_failed'
        assert row['raw_integrity_status'] == 'failed'
        assert row['reparse_error']
        assert row['checked_at'] == task['checked_at']
        assert row['parser_version'] == 0
        assert conn.execute('SELECT count(*) FROM parse_versions').fetchone()[0] == 0
    assert (raw.read_bytes() if raw.exists() else None) == before
    report = json.loads((root / 'manifest.json').read_text())
    assert report['status'] == 'partial'
    assert report['quality']['offline_reparsed_count'] == 0
    assert report['quality']['reparse_errors'][0]['error_type'] == 'ArchiveIntegrityError'


def test_reparse_unsupported_remains_preserved_not_parsed_or_values_complete(tmp_path, queue, monkeypatch):
    task = legacy_capture(queue, tmp_path, collector.BASE + '/file/legacy.doc',
                          body=b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1legacy',
                          format='office_or_archive', parsing_status='unsupported')
    forbid_http_and_limiter(monkeypatch)
    assert collector.main(['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31', '--reparse-only']) == 0
    row = queue.execute('SELECT * FROM documents').fetchone()
    assert row['state'] == 'complete'  # only the capture is complete
    assert row['parsing_status'] == 'unsupported'
    assert row['checked_at'] == task['checked_at']
    report = json.loads((tmp_path / 'manifest.json').read_text())
    assert report['status'] == 'partial'
    assert report['counts']['parse_gaps'] == 1
    assert report['quality']['parsing_status_counts'] == {'unsupported': 1}
    assert report['coverage']['historical_values_complete'] is False


@pytest.mark.parametrize('offline', [True, False])
def test_reparse_document_budget_is_shared_and_never_creates_http_when_exhausted(tmp_path, queue, monkeypatch, offline):
    for suffix in ('a', 'b', 'c'):
        legacy_capture(queue, tmp_path, collector.BASE + f'/file/{suffix}.csv')
    forbid_http_and_limiter(monkeypatch)
    args = ['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31', '--max-documents', '1']
    if offline:
        args.append('--reparse-only')
    assert collector.main(args) == 0
    assert queue.execute('SELECT count(*) FROM parse_versions').fetchone()[0] == 1
    assert queue.execute('SELECT count(*) FROM documents WHERE parser_version=0').fetchone()[0] == 2


def test_reparse_only_obeys_time_budget_before_next_document(tmp_path, queue, monkeypatch):
    legacy_capture(queue, tmp_path)
    forbid_http_and_limiter(monkeypatch)
    clock_values = iter([0.0, 2.0])
    monkeypatch.setattr(collector.time, 'monotonic', lambda: next(clock_values))
    assert collector.main(['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31',
                           '--reparse-only', '--max-seconds', '1']) == 0
    assert queue.execute('SELECT count(*) FROM parse_versions').fetchone()[0] == 0


def test_reparse_does_not_reschedule_preserved_children_but_discovers_new_links(tmp_path, queue):
    old_url, new_url = collector.BASE + '/file/old.pdf', collector.BASE + '/file/new.pdf'
    body = f"<main id='content'><a href='{old_url}'>old</a><a href='{new_url}'>new</a></main>".encode()
    parent = legacy_capture(queue, tmp_path, collector.BASE + '/cht/11/newsDetail?idx=test', body=body, format='html')
    collector.enqueue(queue, old_url, 'attachment', 'margins', 0)
    queue.execute("UPDATE documents SET state='complete',checked_at='2004-01-01T00:00:00+00:00' WHERE url=?", (old_url,))
    queue.commit()
    collector.reparse_document(queue, tmp_path, parent)
    assert queue.execute('SELECT state FROM documents WHERE url=?', (old_url,)).fetchone()[0] == 'complete'
    assert queue.execute('SELECT state FROM documents WHERE url=?', (new_url,)).fetchone()[0] == 'pending'
    assert queue.execute('SELECT count(*) FROM links WHERE parent=?', (parent['url'],)).fetchone()[0] == 2


def test_old_queue_schema_migrates_additively_without_rewriting_capture(tmp_path):
    state = tmp_path / 'state'
    state.mkdir()
    original = ('https://www.taifex.com.tw/file/a.csv', 'hash', '2004-07-01T01:00:00+00:00', 'raw/a.gz', 'rawhash', 'parsed/a.json')
    with sqlite3.connect(state / 'queue.sqlite3') as conn:
        conn.execute('''CREATE TABLE documents (url TEXT PRIMARY KEY,kind TEXT,source_id TEXT,priority INTEGER,
            state TEXT DEFAULT 'pending',checked_at TEXT,next_retry TEXT,attempts INTEGER DEFAULT 0,
            raw_path TEXT,raw_sha256 TEXT,content_sha256 TEXT,parsed_path TEXT,parsing_status TEXT,
            table_rows INTEGER DEFAULT 0,temporal_rows INTEGER DEFAULT 0,http_status INTEGER,error TEXT)''')
        conn.execute('''CREATE TABLE versions (url TEXT,content_sha256 TEXT,captured_at TEXT,raw_path TEXT,
            raw_sha256 TEXT,parsed_path TEXT,PRIMARY KEY(url,content_sha256))''')
        conn.execute('INSERT INTO versions VALUES(?,?,?,?,?,?)', original)
        conn.execute("INSERT INTO documents(url,checked_at) VALUES(?,?)", (original[0], original[2]))
    with collector.closing(collector.open_queue(tmp_path)) as conn:
        row = conn.execute('SELECT * FROM documents').fetchone()
        assert row['parser_version'] == 0 and row['parsed_at'] is None
        assert row['checked_at'] == original[2]
        assert tuple(conn.execute('SELECT * FROM versions').fetchone()) == original
        assert conn.execute('SELECT count(*) FROM parse_versions').fetchone()[0] == 0


def test_fetch_accepts_legitimate_4047_filename_but_rejects_error_pages():
    content = b'%PDF-1.7\nlegitimate official attachment'
    url = collector.BASE + '/file/taifex/CHINESE/11/attach/4047_001.pdf'
    body, mime = collector.fetch(FakeSession([FakeResponse(content, headers={'Content-Type': 'application/pdf'})]), FakeLimiter(), url)
    assert body == content and mime == 'application/pdf'
    for bad_url in (collector.BASE + '/file/taifex/404.htm', collector.BASE + '/404'):
        with pytest.raises(ValueError, match='empty_or_error_response'):
            collector.fetch(FakeSession([FakeResponse(b'<html><title>404</title></html>')]), FakeLimiter(), bad_url)
    with pytest.raises(ValueError, match='empty_or_error_response'):
        collector.fetch(FakeSession([FakeResponse(b'<html><TITLE> 404 Not Found</TITLE></html>')]), FakeLimiter(), url)


def test_crash_before_sqlite_commit_reuses_orphan_parse_and_original_capture(tmp_path, queue, monkeypatch):
    url = collector.BASE + '/file/orphan.csv'
    body = b'contract,value\nTX,100\n'
    monkeypatch.setattr(collector, 'fetch', lambda *args, **kwargs: (body, 'text/csv'))
    collector.enqueue(queue, url, 'attachment', 'margins', 0)
    queue.commit()
    task = queue.execute('SELECT * FROM documents').fetchone()
    original_discover = collector.discover_preserved_links
    calls = []
    def crash_once(*args, **kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError('simulated crash after immutable parse, before SQLite commit')
        return original_discover(*args, **kwargs)
    monkeypatch.setattr(collector, 'discover_preserved_links', crash_once)
    first = '2026-09-20T08:00:00+00:00'
    monkeypatch.setattr(collector, 'now_iso', lambda: first)
    with pytest.raises(RuntimeError, match='simulated crash'):
        collector.process_document(queue, tmp_path, task, None, None)
    queue.rollback()
    assert queue.execute('SELECT count(*) FROM versions').fetchone()[0] == 0
    assert queue.execute('SELECT count(*) FROM parse_versions').fetchone()[0] == 0
    path = next((tmp_path / 'parsed').rglob('*.json'))
    original = path.read_bytes()
    later = '2026-09-27T08:00:00+00:00'
    monkeypatch.setattr(collector, 'now_iso', lambda: later)
    collector.process_document(queue, tmp_path, task, None, None)
    assert path.read_bytes() == original
    document = queue.execute('SELECT * FROM documents').fetchone()
    assert document['checked_at'] == later
    assert document['parsed_at'] == first
    assert queue.execute('SELECT captured_at FROM versions').fetchone()[0] == first
    assert queue.execute('SELECT captured_at FROM parse_versions').fetchone()[0] == first


def test_index_only_never_upgrades_parser_and_normal_batch_shares_remaining_budget(tmp_path, queue, monkeypatch):
    old = legacy_capture(queue, tmp_path)
    pending = collector.BASE + '/file/new.csv'
    collector.enqueue(queue, pending, 'attachment', 'margins', 0)
    queue.commit()
    monkeypatch.setattr(collector, 'SEEDS', ())
    monkeypatch.setattr(collector, 'index_year', lambda *args, **kwargs: 0)
    session = FakeSession([FakeResponse(b'code,value\nTX,123\n', headers={'Content-Type': 'text/csv'})])
    monkeypatch.setattr(collector.requests, 'Session', lambda: session)
    monkeypatch.setattr(collector, 'SharedRateLimiter', lambda *args, **kwargs: FakeLimiter())
    args = ['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31']
    collector.main([*args, '--index-only'])
    assert queue.execute('SELECT parser_version FROM documents WHERE url=?', (old['url'],)).fetchone()[0] == 0
    assert not session.calls
    collector.main([*args, '--max-documents', '2'])
    assert len(session.calls) == 1
    assert session.calls[0][1] == pending
    assert queue.execute('SELECT count(*) FROM parse_versions').fetchone()[0] == 2
    report = json.loads((tmp_path / 'manifest.json').read_text())
    assert report['quality']['offline_reparsed_count'] == 1


def test_failed_raw_integrity_is_not_automatically_repaired_over_http(tmp_path, queue, monkeypatch):
    task = legacy_capture(queue, tmp_path)
    raw = tmp_path / task['raw_path']
    raw.write_bytes(b'corrupt evidence, preserve for investigation')
    monkeypatch.setattr(collector, 'SEEDS', ())
    monkeypatch.setattr(collector, 'index_year', lambda *args, **kwargs: 0)
    session = FakeSession([])
    monkeypatch.setattr(collector.requests, 'Session', lambda: session)
    monkeypatch.setattr(collector, 'SharedRateLimiter', lambda *args, **kwargs: FakeLimiter())
    for _ in range(2):
        collector.main(['--output-dir', str(tmp_path), '--start-year', '1997', '--end', '1997-12-31'])
        assert not session.calls
        assert raw.read_bytes() == b'corrupt evidence, preserve for investigation'
        assert queue.execute('SELECT state FROM documents').fetchone()[0] == 'integrity_failed'
