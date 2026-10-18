from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import errno
from pathlib import Path
import sqlite3
import time

from bs4 import BeautifulSoup
import pytest
import requests

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import accept_discovery, fetch_discovery, load_config, run_cycle, run_service
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, _plain_text
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config as config, http_fixture


def test_daemon_handoff_log_is_bounded_without_changing_full_status(capsys):
    from scripts.download_news_fulltext import print_status

    result = {
        "activity": "configuration_reload_requested", "owner_id": "owner-fixture",
        "updated_at_utc": "2026-10-09T04:00:00+00:00", "unique_articles": 10000,
        "articles_with_stored_body": 2000, "http_response_bytes_this_run": 500000,
        "providers": {f"provider_{number}": {"private_capture": "PRIVATE_CAPTURE", "states": {"pending": 10}}
                      for number in range(738)},
    }
    original = json.dumps(result, sort_keys=True)
    print_status(result, compact=True)
    output = capsys.readouterr().out
    logged = json.loads(output)
    assert len(output.splitlines()) == 1 and len(output.encode()) < 1024
    assert "PRIVATE_CAPTURE" not in output and "providers" not in logged
    assert logged["activity"] == "configuration_reload_requested"
    assert logged["configured_or_observed_provider_count"] == 738
    assert logged["articles_with_stored_body"] == 2000
    assert logged["full_status_snapshot"] == "status.json"
    assert json.dumps(result, sort_keys=True) == original
    print_status(result)
    assert json.loads(capsys.readouterr().out) == result


@pytest.mark.parametrize('seconds_per_receipt,expected_status_calls', [(0.001, 1), (0.2, 14)])
def test_full_inventory_uses_time_not_receipt_count_for_status(config, tmp_path, monkeypatch,
                                                            seconds_per_receipt, expected_status_calls):
    root = tmp_path / 'finmind'
    root.mkdir()
    source = sqlite3.connect(root / 'queue.sqlite3')
    source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
    for index in range(1005):
        path = root / f'{index:04}.json'
        path.write_text('{}')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', path.name, index))
    source.commit()
    source.close()
    queue = NewsQueue(tmp_path / 'queue', PolicyRegistry(config, base=tmp_path))
    clock, visited, statuses = [0.0], [], []

    def receipt(_root, path, **kwargs):
        visited.append(path.name)
        clock[0] += seconds_per_receipt
        return path.name in {'0007.json', '0800.json'}

    monkeypatch.setattr('downloader.news_fulltext_queue.time.monotonic', lambda: clock[0])
    monkeypatch.setattr(queue, 'ingest_receipt', receipt)
    monkeypatch.setattr(queue, 'status', lambda **kwargs: statuses.append((clock[0], kwargs['activity'])))
    try:
        assert queue.ingest_local(tmp_path / 'finlab', root) == 2
        assert visited == [f'{i:04}.json' for i in reversed(range(1005))]
        assert len(statuses) == expected_status_calls
        assert all(activity == 'inventorying' for _, activity in statuses)
        assert queue.db.execute("SELECT value FROM counters WHERE key='latest_inventory_finmind_receipts_checked'").fetchone()[0] == 1005
        assert queue.metrics.snapshot()['stages']['metadata_inventory_status']['count'] == expected_status_calls
    finally:
        queue.close()


class SlowResponse(FakeResponse):
    def iter_content(self, **kwargs):
        time.sleep(0.08)
        yield self.body


class FailedResponse(FakeResponse):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def __enter__(self):
        raise self.error


class UnreadErrorResponse(FakeResponse):
    def iter_content(self, **kwargs):
        raise AssertionError("source status must be honored before reading an error body")


def test_isolated_gateway_error_is_article_retry_not_global_freeze(config, tmp_path):
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/bad": UnreadErrorResponse(status=502),
              "https://publisher.test/good": FakeResponse(article_html())}
    http, _ = http_fixture(config, tmp_path, routes)
    try:
        for _ in range(2):
            with pytest.raises(NewsError, match="http_502") as caught:
                http.get("https://publisher.test/bad")
            assert caught.value.retry and caught.value.delay >= 30
            assert "publisher" not in http.provider_not_before
            assert http.get("https://publisher.test/good").status == 200
            assert http.transport_failures["publisher"] == 0
        assert http.metrics.snapshot()["providers"]["publisher"]["counts"]["http_status_502"] == 2
    finally:
        http.close()


@pytest.mark.parametrize("status,delay", [(429, 120), (503, 120), (502, 120), (403, 900), (401, 900), (302, 120)])
def test_source_signals_pause_before_error_body(config, tmp_path, status, delay):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        "https://publisher.test/bad": UnreadErrorResponse(status=status, headers={"Retry-After": "120", "Location": "/other"})})
    try:
        with pytest.raises(NewsError) as caught:
            http.get("https://publisher.test/bad")
        assert caught.value.retry and caught.value.delay == delay
        assert http.provider_not_before["publisher"] >= time.time() + delay - 1
        assert http.limiters["publisher"].delays == [delay]
        assert http.metrics.snapshot()["providers"]["publisher"]["counts"]["http_inflight"] == 0
        with pytest.raises(NewsError, match="provider_cooldown"):
            http.get("https://publisher.test/other")
        assert len(session.calls) == 2
    finally:
        http.close()


def test_repeated_http_and_transport_failures_share_outage_circuit(config, tmp_path):
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        "https://publisher.test/a": UnreadErrorResponse(status=502),
        "https://publisher.test/b": FailedResponse(requests.exceptions.ReadTimeout()),
        "https://publisher.test/c": UnreadErrorResponse(status=504)})
    try:
        for url in ("a", "b", "c"):
            with pytest.raises(NewsError):
                http.get("https://publisher.test/" + url)
        assert http.provider_not_before["publisher"] >= time.time() + 29
        assert len(session.calls) == 4
    finally:
        http.close()


def test_rate_refusal_learns_slower_publisher_pacing_and_survives_expired_cooldown(config, tmp_path):
    config["providers"][0]["interval_seconds"] = 0.5
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/bad": UnreadErrorResponse(status=429)}
    http, _ = http_fixture(config, tmp_path, routes)
    try:
        with pytest.raises(NewsError, match="http_429"):
            http._response_status("publisher", 429, {})
        assert http.rate_refusal_intervals["publisher"] == 1.0
        assert http.limiters["publisher"].interval_seconds == 1.0
        checkpoint_path = tmp_path / "cooldowns/publisher.json"
        checkpoint = json.loads(checkpoint_path.read_text())
        assert checkpoint["rate_refusal_interval_seconds"] == 1.0
        checkpoint["not_before_epoch"] = time.time() - 1
        from downloader.artifact_io import atomic_write_json
        atomic_write_json(checkpoint_path, checkpoint)
    finally:
        http.close()
    resumed, _ = http_fixture(config, tmp_path, routes)
    try:
        assert "publisher" not in resumed.provider_not_before
        assert resumed.limiters["publisher"].interval_seconds == 1.0
        assert resumed.rate_refusal_intervals["publisher"] == 1.0
        resumed._robots("https://publisher.test/good")
        assert resumed.limiters["publisher"].interval_seconds == 1.0  # robots refresh cannot undo learned pacing.
        with pytest.raises(NewsError, match="http_429"):
            resumed._response_status("publisher", 429, {})
        assert resumed.rate_refusal_intervals["publisher"] == 2.0
    finally:
        resumed.close()


def test_benchmark_provider_subset_does_not_change_queue_scope(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
                                            "https://publisher.test/good": FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/good"}, {"url": "https://unknown.test/keep"}])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0, provider_ids={"publisher"})
        assert result["states"] == {"complete": 1, "awaiting_authorization": 1}
        assert result["performance"]["providers"]["publisher"]["counts"]["frontier_attempts_started"] == 1
        assert verify_bodies(tmp_path)["accepted"]
        for selection in ({"unknown"}, set()):
            with pytest.raises(ValueError, match="cycle provider selection"):
                run_cycle(queue, http, config, max_articles=1, provider_ids=selection)
    finally:
        queue.close()
        http.close()


def test_single_read_timeout_does_not_freeze_healthy_publisher(config, tmp_path):
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/fail": FailedResponse(requests.exceptions.ReadTimeout()),
              "https://publisher.test/good": FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    try:
        with pytest.raises(NewsError, match="transport_error"):
            http.get("https://publisher.test/fail")
        assert "publisher" not in http.provider_not_before
        assert http.get("https://publisher.test/good").status == 200
        assert http.transport_failures["publisher"] == 0
        with pytest.raises(NewsError, match="transport_error"):
            http.get("https://publisher.test/fail")
        assert "publisher" not in http.provider_not_before
        counts = http.metrics.snapshot()["providers"]["publisher"]["counts"]
        assert counts["failed_sessions_reset"] == 2
        assert counts["transport_error_ReadTimeout"] == 2
        assert len(session.calls) == 4
    finally:
        http.close()


def test_repeated_transport_failure_opens_persisted_source_circuit(config, tmp_path):
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              **{f"https://publisher.test/{i}": FailedResponse(requests.exceptions.ReadTimeout()) for i in range(4)}}
    http, session = http_fixture(config, tmp_path, routes)
    try:
        for index in range(3):
            with pytest.raises(NewsError, match="transport_error"):
                http.get(f"https://publisher.test/{index}")
        assert http.provider_not_before["publisher"] > time.time() + 28
        assert (tmp_path / "cooldowns/publisher.json").is_file()
        with pytest.raises(NewsError, match="provider_cooldown"):
            http.get("https://publisher.test/3")
        assert len(session.calls) == 4
    finally:
        http.close()


@pytest.mark.parametrize("kind", ["route", "tls"])
def test_confirmed_route_or_tls_failure_trips_immediately(config, tmp_path, kind):
    error = requests.exceptions.SSLError() if kind == "tls" else requests.exceptions.ConnectionError()
    if kind == "route":
        error.__cause__ = OSError(errno.ENETUNREACH, "unreachable")
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/fail": FailedResponse(error)}
    http, _ = http_fixture(config, tmp_path, routes)
    try:
        with pytest.raises(NewsError, match="transport_error"):
            http.get("https://publisher.test/fail")
        assert http.provider_not_before["publisher"] > time.time() + 28
    finally:
        http.close()


def test_parallel_responses_keep_actual_start_spacing_and_robots(config, tmp_path):
    config["providers"][0].update(interval_seconds=0.015, parallelism=2)
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nCrawl-delay: 0.03\n"),
              **{f"https://publisher.test/story/{i}": SlowResponse(article_html()) for i in range(2)}}
    http, _ = http_fixture(config, tmp_path, routes)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert all(r.status == 200 for r in pool.map(http.get, list(routes)[1:]))
        measured = http.metrics.snapshot()["providers"]["publisher"]
        assert measured["minimum_request_start_gap_seconds"] >= 0.029
        assert measured["counts"]["peak_http_inflight"] == 2
        assert measured["counts"]["http_inflight"] == 0
        assert measured["counts"]["http_requests_started"] == 3  # includes robots
    finally:
        http.close()


@pytest.mark.parametrize('telemetry_delay', [0.003, 0.2])
def test_pacer_and_metrics_share_one_dispatch_clock_under_scheduler_delay(config, tmp_path, monkeypatch, telemetry_delay):
    config['providers'][0].update(interval_seconds=10, parallelism=2)
    http, session = http_fixture(config, tmp_path, {})
    clock = [10000.0]
    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    original = http.metrics.request_started
    events = []

    def delayed_metric(provider, endpoint):
        if not events:
            clock[0] += telemetry_delay  # No real sleep or publisher HTTP.
        stamp = original(provider, endpoint)
        events.append(stamp)
        return stamp

    def simulated_wait(seconds):
        clock[0] += seconds
        return False

    monkeypatch.setattr(http.metrics, 'request_started', delayed_metric)
    monkeypatch.setattr(http.stop, 'wait', simulated_wait)
    try:
        source = http.registry.sources['publisher']
        for _ in range(2):
            http._reserve_request(source, 'https://publisher.test/a')
            assert http.last_calls['publisher'] == events[-1]
        assert events[1] - events[0] == pytest.approx(10)
        snapshot = http.metrics.snapshot()
        assert snapshot['request_dispatch_clock_contract_version'] == 2
        assert snapshot['providers']['publisher']['minimum_request_start_gap_seconds'] >= 10 - 1e-9
        assert not session.calls
    finally:
        http.close()


def test_racing_refusal_stops_waiting_worker_before_network(config, tmp_path):
    config["providers"][0].update(interval_seconds=0.12, parallelism=2)
    first, second = "https://publisher.test/one", "https://publisher.test/two"
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              first: SlowResponse(status=403), second: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    http._robots(first)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            one = pool.submit(http.get, first)
            time.sleep(0.01)
            two = pool.submit(http.get, second)
            with pytest.raises(NewsError, match="source_access_denied"):
                one.result()
            with pytest.raises(NewsError, match="provider_cooldown"):
                two.result()
        assert [url for url, _ in session.calls] == ["https://publisher.test/robots.txt", first]
    finally:
        http.close()


def test_stop_drains_inflight_response_and_durable_batch(config, tmp_path):
    class StopResponse(FakeResponse):
        def iter_content(self, **kwargs):
            http.stop.set()
            yield self.body

    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              "https://publisher.test/one": StopResponse(article_html())}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/one"}, {"url": "https://publisher.test/two"}])
        status = run_cycle(queue, http, config, max_articles=64, max_discovery_requests=0)
        assert status["activity"] == "stopped"
        assert status["states"] == {"complete": 1, "pending": 1}
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        http.close()
        queue.close()


def test_continuous_owner_reuses_workers_and_skips_batch_idle(config, tmp_path):
    urls = [f"https://publisher.test/story/{i}" for i in range(4)]
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              **{url: FakeResponse(article_html()) for url in urls}}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    calls = []
    try:
        with queue.db:
            queue.add_articles([{"url": url} for url in urls])
        result = run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                             refresh_metadata=lambda: calls.append(1), duration_seconds=0.2)
        assert result["activity"] == "service_duration_complete"
        assert result["states"] == {"complete": 4}
        assert len(http._sessions) == 1
        assert len(calls) <= 6  # no busy polling after the frontier is drained
        assert len(session.calls) == 5
        measured = result["performance"]
        assert measured["stages"]["idle_wait"]["wall_seconds"] > 0
        assert measured["providers"]["publisher"]["counts"]["durably_accepted_versions"] == 4
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        http.close()
        queue.close()


def test_empty_diagnostic_has_bounded_duration_not_busy_loop(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    calls, started = [], time.monotonic()
    try:
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                    refresh_metadata=lambda: calls.append(1), duration_seconds=0.03)
        assert time.monotonic() - started < 0.5
        assert len(calls) == 1
    finally:
        http.close()
        queue.close()


def test_first_inventory_status_keeps_loaded_http_deadline_and_capacity(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    snapshots = []
    deadline = time.time() + 900
    http.provider_not_before['publisher'] = deadline
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/story'}])
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                    refresh_metadata=lambda: snapshots.append(queue.status(activity='inventorying')),
                    duration_seconds=0.02)
        first = snapshots[0]
        assert first['activity'] == 'inventorying'
        assert first['provider_next_attempt_utc']['publisher']
        assert first['http_capacity']['providers']['publisher']['parallelism']
        assert http.cooldown_snapshot()['publisher'] == deadline
        assert queue.status(provider_cooldowns={})['provider_next_attempt_utc'] == {}
        assert queue.status()['provider_next_attempt_utc']['publisher']
        http.provider_not_before['publisher'] = time.time() - 1
        assert queue.status()['provider_next_attempt_utc'] == {}
    finally:
        http.close()
        queue.close()


def test_idle_wake_uses_nearest_deadline_and_ignores_disabled_bulk(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/story"}])
            queue.db.execute("UPDATE articles SET state='retry',due=?", (time.time() + 10,))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,0)",
                             ("https://publisher.test/wp-json/wp/v2/posts?page=1", "wordpress_posts", "publisher"))
        delay = queue.next_wake_delay({"publisher": time.time() + 20})
        assert 19 <= delay <= 20
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete'")
        assert queue.next_wake_delay({}) == 60
    finally:
        queue.close()


def test_raw_response_bytes_bound_batches_even_when_text_is_small(config, tmp_path, monkeypatch):
    config["batch_raw_response_bytes"] = 2048
    urls = [f"https://publisher.test/{i}" for i in range(5)]
    payload = article_html() + b"<!--" + b"x" * 4096 + b"-->"
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              **{url: FakeResponse(payload) for url in urls}}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    sizes, original = [], queue.accept

    def accept(rows):
        sizes.append(len(rows))
        original(rows)

    monkeypatch.setattr(queue, "accept", accept)
    try:
        with queue.db:
            queue.add_articles([{"url": url} for url in urls])
        run_cycle(queue, http, config, max_articles=5, max_discovery_requests=0)
        assert sizes == [1] * 5
        assert verify_bodies(tmp_path)["body_versions_verified"] == 5
    finally:
        http.close()
        queue.close()


def test_unchanged_policy_is_no_scan_but_changed_permission_reconciles(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    with queue.db:
        queue.add_articles([{"url": "https://publisher.test/one"}])
    queue.close()
    queue = NewsQueue(tmp_path, registry)
    statements = []
    queue.db.set_trace_callback(statements.append)
    try:
        queue.reconcile_policy()
        assert not any("UPDATE articles" in statement for statement in statements)
        config["providers"][0]["enabled"] = False
        queue.registry = PolicyRegistry(config, base=tmp_path)
        queue.reconcile_policy()
        assert queue.db.execute("SELECT state FROM articles").fetchone()[0] == "awaiting_authorization"
        assert any("UPDATE articles" in statement for statement in statements)
    finally:
        queue.close()


@pytest.mark.parametrize("text", [" 第一行 \n\n 第二行 \r\n", "x &amp; y\n z", "<p>hello</p>world", "\t台灣\u2028新聞  ", "\x00value\ntext"])
def test_plain_text_shortcut_preserves_original_whitespace_and_entities(text):
    expected = BeautifulSoup(text, "html.parser").get_text("\n", strip=True)
    expected = "\n".join(line.strip() for line in expected.splitlines() if line.strip())
    assert _plain_text(text) == expected


@pytest.mark.parametrize("key,value", [("workers", 1.5), ("workers", True), ("service_cycle_seconds", float("inf")),
                                      ("batch_raw_response_bytes", float("nan")), ("batch_flush_seconds", 0)])
def test_resource_limits_reject_unbounded_or_noninteger_settings(config, tmp_path, key, value):
    config[key] = value
    path = tmp_path / "config.json"
    atomic_write_json(path, config)
    with pytest.raises(ValueError):
        load_config(path, repo=tmp_path)


def test_parser_selection_is_fingerprinted_and_saved(config, tmp_path):
    config["providers"][0]["html_parser"] = "html.parser"
    original = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    config["providers"][0]["html_parser"] = "lxml"
    updated = PolicyRegistry(config, base=tmp_path).sources["publisher"]
    assert updated.html_parser == "lxml"
    assert updated.data_fingerprint != original.data_fingerprint
    config["providers"][0]["html_parser"] = "unknown"
    with pytest.raises(ValueError, match="unsupported article parser"):
        PolicyRegistry(config, base=tmp_path)


def test_sitemap_backlog_never_starves_known_bodies(config, tmp_path):
    config["sitemap_article_ratio"] = 8
    maps = [f"https://publisher.test/map{i}.xml" for i in range(3)]
    config["providers"][0]["sitemaps"] = maps
    urls = [f"https://publisher.test/story/{i}" for i in range(17)]
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              **{url: FakeResponse(article_html()) for url in urls},
              **{url: FakeResponse(b"<urlset/>") for url in maps}}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": url} for url in urls])
        status = run_cycle(queue, http, config, max_articles=17, max_discovery_requests=64)
        assert status["states"] == {"complete": 17}
        calls = [url for url, _ in session.calls if not url.endswith("robots.txt")]
        assert [index for index, url in enumerate(calls) if url in maps] == [8, 17]
        assert queue.db.execute("SELECT count(*) FROM discovery WHERE due=0").fetchone()[0] == 1
    finally:
        http.close()
        queue.close()


def test_rss_keeps_release_priority_over_bodies_and_historical_maps(config, tmp_path):
    feed, body, site = "https://publisher.test/feed", "https://publisher.test/article", "https://publisher.test/map"
    config["providers"][0].update(feeds=[feed], sitemaps=[site])
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              feed: FakeResponse(b"<rss><channel/></rss>"), body: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": body}])
        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=64)
        assert [url for url, _ in session.calls] == ["https://publisher.test/robots.txt", feed, body]
    finally:
        http.close()
        queue.close()


def test_identical_xml_response_is_no_parse_no_metadata_rewrite(config, tmp_path, monkeypatch):
    url = "https://publisher.test/feed"
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
              url: FakeResponse(b"<rss><channel><item><link>https://publisher.test/article</link></item></channel></rss>")}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')", (url,))
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        accept_discovery(queue, task, *fetch_discovery(http, task))
        previous = queue.db.execute("SELECT last_seen_utc FROM articles").fetchone()[0]
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())

        def unexpected_parse(*args):
            raise AssertionError("unchanged metadata must not rebuild a DOM")

        monkeypatch.setattr("downloader.news_fulltext.discovery_links", unexpected_parse)
        accept_discovery(queue, task, *fetch_discovery(http, task))
        assert queue.db.execute("SELECT last_seen_utc FROM articles").fetchone()[0] == previous
        assert queue.db.execute("SELECT last_checked_at_utc FROM discovery").fetchone()[0] is not None
        assert http.metrics.snapshot()["providers"]["publisher"]["counts"]["unchanged_discovery_responses"] == 1
    finally:
        http.close()
        queue.close()


def test_legacy_discovery_validator_cannot_hide_unversioned_parser_results(config, tmp_path):
    url = "https://publisher.test/feed"
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"), url: FakeResponse(status=304)}
    http, session = http_fixture(config, tmp_path, routes)
    try:
        with pytest.raises(NewsError, match="unverifiable_discovery_not_modified"):
            fetch_discovery(http, {"url": url, "kind": "feed", "provider": "publisher", "etag": "old", "last_modified": "old"})
        assert "If-None-Match" not in session.calls[-1][1]["headers"]
        assert "If-Modified-Since" not in session.calls[-1][1]["headers"]
    finally:
        http.close()


def test_config_change_hands_off_after_drained_cycle(config, tmp_path, monkeypatch, capsys):
    from scripts import download_news_fulltext as cli

    path = tmp_path / "config.json"
    atomic_write_json(path, config)
    http, session = http_fixture(config, tmp_path, {})
    monkeypatch.setattr(cli, "NewsHTTP", lambda *args: http)

    def service(queue, transport, effective, **kwargs):
        updated = dict(config)
        updated["batch_articles"] = config["batch_articles"] + 1
        atomic_write_json(path, updated)
        kwargs["refresh_metadata"]()
        assert transport.stop.is_set()
        return queue.status(activity="stopped")

    monkeypatch.setattr(cli, "run_service", service)
    assert cli.main(["run", "--continuous", "--config", str(path), "--root", str(tmp_path / "queue"), "--skip-local-ingest"]) == 75
    assert json.loads(capsys.readouterr().out)["activity"] == "configuration_reload_requested"
    assert not session.calls


@pytest.mark.parametrize('skip_ingest', [False, True])
def test_config_change_during_initial_recovery_hands_off_without_http_or_ingest(config, tmp_path, monkeypatch, capsys, skip_ingest):
    from scripts import download_news_fulltext as cli

    path, root = tmp_path / 'config.json', tmp_path / 'queue'
    atomic_write_json(path, config)
    owners = []

    def recovered_queue(*args, **kwargs):
        queue = NewsQueue(*args, **kwargs)
        owners.append(queue.owner)
        updated = {**config, 'batch_articles': config['batch_articles'] + 1}
        atomic_write_json(path, updated)
        return queue

    def forbidden(*args, **kwargs):
        pytest.fail('stale startup must not construct HTTP, ingest metadata, or run work')

    monkeypatch.setattr(cli, 'NewsQueue', recovered_queue)
    monkeypatch.setattr(cli, 'NewsHTTP', forbidden)
    monkeypatch.setattr(cli, 'run_service', forbidden)
    monkeypatch.setattr(NewsQueue, 'ingest_local_slice', forbidden)
    args = ['run', '--continuous', '--config', str(path), '--root', str(root)]
    if skip_ingest:
        args.append('--skip-local-ingest')
    assert cli.main(args) == 75
    result = json.loads(capsys.readouterr().out)
    persisted = json.loads((root / 'status.json').read_bytes())
    assert result['activity'] == persisted['activity'] == 'configuration_reload_requested'
    assert result['owner_id'] == persisted['owner_id'] == owners[0]
    # A second owner must acquire the released canonical lock and find no
    # changed attempt/deadline or invented completion after the handoff.
    with closing(NewsQueue(root, PolicyRegistry(config, base=tmp_path))) as queue:
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        assert queue.db.execute("SELECT value FROM counters WHERE key='last_metadata_ingest_epoch'").fetchone() is None


@pytest.mark.parametrize("value", [float("inf"), float("nan")])
def test_publisher_pacing_cannot_be_unbounded(config, tmp_path, value):
    config["providers"][0]["interval_seconds"] = value
    with pytest.raises(ValueError, match="publisher resource limits"):
        PolicyRegistry(config, base=tmp_path)


def test_service_entrypoint_execs_canonical_runtime_without_unconditional_timer_gap():
    script = Path("scripts/run_news_fulltext.sh").read_text()
    unit = Path("deploy/systemd/stockagent-news-fulltext.service.in").read_text()
    assert "exec_fintech_python" in script and "--continuous" in script
    assert "Type=simple" in unit and "Restart=on-failure" in unit
    assert "MemoryMax=2G" in unit
    json.loads(Path("configs/news_fulltext.json").read_text())
