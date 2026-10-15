from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from downloader.news_fulltext import accept_discovery, fetch_article, fetch_discovery, run_cycle, seed_discovery
from downloader.news_fulltext_bulk import (cnyes_history_priority, cnyes_pagination, cnyes_records, cnyes_scope,
                                          cnyes_url, cnyes_window_partitions)
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, http_fixture


BASE = "https://api.cnyes.com/media/api/v1/newslist/category/headline"


@pytest.fixture
def cfg():
    cfg = json.loads(Path("configs/news_fulltext.json").read_text())
    entry = deepcopy(next(p for p in cfg["providers"] if p["id"] == "cnyes"))
    entry.pop("cnyes_endpoint_inventory", None)
    entry["cnyes_history_partition_policy"] = "monthly_v1"
    entry.update(cnyes_apis=[BASE], cnyes_history_start="2010-01-01", article_path_pattern=r"^/news/(id|print)/[0-9]+$",
                 bulk_text_permission_basis="explicit local personal research, no license inference", interval_seconds=0.001)
    cfg["providers"] = [entry]
    cfg["min_free_disk_bytes"] = 1
    cfg["exclusive_http_owner"] = False
    return cfg


def payload(*, ids=(1, 2), page=1, total=None, content=None, **item_overrides):
    size = 30
    total = len(ids) if total is None else total
    posts = [{"newsId": i, "title": "測試標題", "payment": 0, "source": "原始來源署名", "columnists": None,
              "publishAt": 1388534400, "content": content or "&lt;p&gt;保留正文和來源，不是新聞摘要。" * 12 + "&lt;/p&gt;"} for i in ids]
    return {"statusCode": 200, "items": {"total": total, "per_page": size, "current_page": page,
            "last_page": max(1, (total + size - 1) // size), "data": posts, **item_overrides}}


def response(data, url=None):
    return Response(url or cnyes_url(BASE), 200, {"content-type": "application/json"}, json.dumps(data, ensure_ascii=False).encode())


def registry_http(cfg, tmp_path):
    registry = PolicyRegistry(cfg, base=tmp_path)
    return registry, SimpleNamespace(registry=registry)


def test_bulk_decodes_body_without_inventing_author(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    r = response(payload())
    rows, errors = cnyes_records(r, http, reg.sources["cnyes"])
    assert not errors and len(rows) == 2
    row = rows[0]
    assert "<p>" not in row["body"] and "保留正文" in row["body"]
    assert row["body_sha256"] == hashlib.sha256(row["body"].encode()).hexdigest()
    assert row["authors_json"] == "[]" and row["attribution_status"] == "source_byline_not_found"
    assert row["publisher_source_raw"] == "原始來源署名"
    assert row["published_at_epoch_raw"] == 1388534400
    assert row["historical_point_in_time"] is False and row["training_injection_enabled"] is False
    assert row["raw_response_body"] is None


def test_actual_navigation_registers_65_reviewed_endpoints_only():
    from downloader.news_fulltext import load_config
    _, registry, _ = load_config(Path("configs/news_fulltext.json"))
    source = registry.sources["cnyes"]
    assert len(source.cnyes_apis) == 65
    assert BASE.replace("headline", "tw_stock") in source.cnyes_apis
    assert all(url.rsplit("/", 1)[-1] not in {"hotai", "news24h", "headline_ai"} for url in source.cnyes_apis)


def test_parser_circuit_blocks_only_failed_registered_endpoint_and_survives_restart(cfg, tmp_path):
    from downloader.news_fulltext import fail_discovery
    other = BASE.replace('headline', 'stock_report')
    cfg['providers'][0]['cnyes_apis'] = [BASE, other]
    reg, _ = registry_http(cfg, tmp_path)
    q = NewsQueue(tmp_path, reg)
    source = reg.sources['cnyes']
    bad, good = cnyes_url(BASE), cnyes_url(other)
    stale = cnyes_url(BASE.replace('headline', 'not_registered'))
    try:
        with q.db:
            q.db.executemany("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", [(bad,), (good,), (stale,)])
            q.db.execute("INSERT INTO discovery(url,kind,provider) VALUES ('https://news.cnyes.com/rss','feed','cnyes')")
        fail_discovery(q, dict(q.db.execute('SELECT * FROM discovery WHERE url=?', (bad,)).fetchone()),
                       NewsError('bulk_all_items_unusable', retry=True, delay=3600))
        with q.db:
            q.db.execute('UPDATE discovery SET due=0 WHERE url=?', (bad,))
        clause, params = q.discovery_dispatch_filter(source)
        assert {r[0] for r in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)} == {good, 'https://news.cnyes.com/rss'}
        assert q.bulk_allowed(source)
        assert len(q.status()['bulk_endpoint_cooldowns']) == 1
        assert q.status()['bulk_adapter_cooldowns'] == []
        assert q.status()['bulk_api_pages_waiting'] == 1
        assert q.status()['bulk_api_pages_disabled'] == 1
        assert q.status()['bulk_api_pages_temporarily_blocked'] == 1
        dispatch_sql, dispatch_binds = q.discovery_candidates_query(source, 9)
        plan = [r[3] for r in q.db.execute('EXPLAIN QUERY PLAN ' + dispatch_sql, dispatch_binds)]
        assert any('discovery_ready_order' in item for item in plan)
        assert not any('TEMP B-TREE FOR ORDER BY' in item for item in plan)
        q.close()
        q = NewsQueue(tmp_path, reg)
        clause, params = q.discovery_dispatch_filter(source)
        assert good in {r[0] for r in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)}
        assert bad not in {r[0] for r in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)}
        with q.db:
            q.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=0')
        clause, params = q.discovery_dispatch_filter(source)
        assert bad in {r[0] for r in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)}
    finally:
        q.close()


def test_old_global_parser_circuit_migration_preserves_failed_job_deadline(cfg, tmp_path):
    other = BASE.replace('headline', 'stock_report')
    cfg['providers'][0]['cnyes_apis'] = [BASE, other]
    reg, _ = registry_http(cfg, tmp_path)
    q = NewsQueue(tmp_path, reg)
    bad, good, deadline = cnyes_url(BASE), cnyes_url(other), time.time() + 1200
    with q.db:
        q.db.execute("DELETE FROM queue_settings WHERE key='cnyes_endpoint_circuits_v1'")
        q.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code) VALUES (?,'cnyes_news','cnyes',?,'bulk_all_items_unusable')", (bad, deadline))
        q.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (good,))
        q.db.execute("INSERT INTO bulk_adapter_cooldowns VALUES ('cnyes','old-fingerprint',?,'bulk_all_items_unusable')", (deadline,))
    q.close()
    q = NewsQueue(tmp_path, reg)
    try:
        circuit = q.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone()
        assert circuit['not_before'] == deadline and circuit['request_url'] == bad
        assert q.db.execute('SELECT due,error_code FROM discovery WHERE url=?', (bad,)).fetchone()[:] == (deadline, 'bulk_all_items_unusable')
        assert q.db.execute('SELECT count(*) FROM bulk_adapter_cooldowns').fetchone()[0] == 1
        clause, params = q.discovery_dispatch_filter(reg.sources['cnyes'])
        assert [r[0] for r in q.db.execute('SELECT url FROM discovery WHERE ' + clause, params)] == [good]
        # No runnable blocked endpoint -> bounded sleep rather than a busy loop.
        with q.db:
            q.db.execute('UPDATE discovery SET due=? WHERE url=?', (time.time() + 300, good))
        assert q.next_wake_delay({}) == 60
        assert q.next_wake_delay({'cnyes': time.time() + 300}) == 60
    finally:
        q.close()


def test_changed_parser_requeues_only_exact_failed_job_preserving_refusals_and_evidence(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    q = NewsQueue(tmp_path, reg)
    bad, denied = cnyes_url(BASE), cnyes_url(BASE, page=2)
    deadline = time.time() + 3600
    try:
        with q.db:
            q.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code) VALUES (?,'cnyes_news','cnyes',?,'bulk_all_items_unusable')", (bad, deadline))
            q.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code) VALUES (?,'cnyes_news','cnyes',?,'http_429')", (denied, deadline))
            q.db.execute("INSERT INTO bulk_endpoint_cooldowns VALUES ('cnyes',?,'old-source-contract',?,'bulk_all_items_unusable',?)", (BASE, deadline, bad))
        q.recheck_changed_cnyes_parser('old-source-contract')
        assert q.db.execute('SELECT due,error_code FROM discovery WHERE url=?', (bad,)).fetchone()[:] == (0, 'bulk_all_items_unusable')
        assert q.db.execute('SELECT due,error_code FROM discovery WHERE url=?', (denied,)).fetchone()[:] == (deadline, 'http_429')
        assert q.db.execute('SELECT not_before,data_fingerprint FROM bulk_endpoint_cooldowns').fetchone()[:] == (deadline, 'old-source-contract')
        with q.db:
            q.db.execute('UPDATE discovery SET due=? WHERE url=?', (deadline, bad))
        q.recheck_changed_cnyes_parser(reg.sources['cnyes'].data_fingerprint)
        assert q.db.execute('SELECT due FROM discovery WHERE url=?', (bad,)).fetchone()[0] == deadline
    finally:
        q.close()


def test_adaptive_seed_reuses_old_jobs_and_keeps_exact_unplanned_gaps():
    from downloader.news_fulltext_bulk import cnyes_unseeded_intervals
    assert cnyes_unseeded_intervals(0, 100, []) == [(0, 100)]
    assert cnyes_unseeded_intervals(0, 100, [(10, 20), (18, 50), (60, 150), (-5, 2)]) == [(3, 9), (51, 59)]
    assert cnyes_unseeded_intervals(1, 100, [(1, 100)]) == []
    assert cnyes_unseeded_intervals(101, 100, []) == []
    assert cnyes_unseeded_intervals(0, 731 * 86400 - 1, []) == [(0, 730 * 86400 - 1), (730 * 86400, 731 * 86400 - 1)]


def test_adaptive_roots_keep_old_month_jobs_and_reseed_only_new_daily_bounds(cfg, tmp_path):
    from datetime import datetime, timezone
    cfg["providers"][0]["cnyes_history_partition_policy"] = "adaptive_days_v1"
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    old = cnyes_url(BASE, start=1262304000, end=1264982399)
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code) VALUES (?,'cnyes_news','cnyes',900,'saved_outcome')", (old,))
        seed_discovery(queue)
        jobs = [r[0] for r in queue.db.execute("SELECT url FROM discovery WHERE kind='cnyes_news'")]
        assert len(jobs) < 40
        assert queue.db.execute("SELECT due,error_code FROM discovery WHERE url=?", (old,)).fetchone()[:] == (900, "saved_outcome")
        for url in jobs:
            args = parse_qs(urlsplit(url).query)
            if "startAt" in args:
                assert int(args["endAt"][0]) - int(args["startAt"][0]) + 1 <= 730 * 86400
                if url != old:
                    assert int(args["startAt"][0]) > 1264982399
        before = queue.db.total_changes
        seed_discovery(queue)
        assert queue.db.total_changes == before
        assert registry.sources["cnyes"].cnyes_history_start <= datetime.now(timezone.utc).strftime("%Y-%m-%d")
    finally:
        queue.close()


def test_broad_category_precedes_leaf_without_excluding_leaf():
    assert cnyes_history_priority(cnyes_url(BASE)) < cnyes_history_priority(cnyes_url(BASE.replace("headline", "tw_revenue")))


def test_raw_html_literal_code_entities_are_not_double_decoded(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    r = response(payload(content="<p>" + "研究程式 &lt;symbol&gt; 的原始標示必須保留。" * 12 + "</p>"))
    rows, errors = cnyes_records(r, http, reg.sources["cnyes"])
    assert not errors and "<symbol>" in rows[0]["body"]


def test_hao_body_container_does_not_capture_comments_or_navigation():
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_policy import extract_article
    _, reg, _ = load_config(Path("configs/news_fulltext.json"))
    source = reg.sources["cnyes"]
    schema = {"@type": "Article", "headline": "社群文章", "author": {"name": "原作者"}, "datePublished": "2026-10-01"}
    article = ("<script type='application/ld+json'>" + json.dumps(schema) + "</script>"
               "<nav>不得收錄的選單</nav><div class='bodyContent fullContent rendered'>" + "這是創作者公開的完整文章。" * 12 +
               "</div><div class='comments'>不得收錄的留言</div>")
    result = extract_article(article.encode(), source, page_url="https://hao.cnyes.com/post/1")
    assert "原作者" in result["authors_json"] and "不得收錄" not in result["body"]
    assert reg.require("https://hao.cnyes.com/post/1").provider == "cnyes"


@pytest.mark.parametrize("start", ["0001-01-01", "2040-01-01", "not-a-date"])
def test_history_settings_are_bounded(cfg, tmp_path, start):
    cfg["providers"][0]["cnyes_history_start"] = start
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=tmp_path)


def test_failed_bulk_conditional_cannot_hide_new_parser_contract(cfg, tmp_path):
    from downloader.news_fulltext import discovery_fingerprint
    url = cnyes_url(BASE)
    http, session = http_fixture(cfg, tmp_path, {
        "https://api.cnyes.com/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        url: FakeResponse(status=304)})
    try:
        task = {"url": url, "kind": "cnyes_news", "etag": "old", "last_modified": None, "parser_fingerprint": "legacy"}
        with pytest.raises(NewsError, match="unverifiable_discovery_not_modified"):
            fetch_discovery(http, task)
        assert "If-None-Match" not in session.calls[-1][1]["headers"]
    finally:
        http.close()


@pytest.mark.parametrize("change", [{"current_page": 2}, {"last_page": 4}, {"total": -1}, {"per_page": 100},
                                     {"total": 3}, {"total": True}, {"per_page": "30"}])
def test_pagination_rejects_inconsistency(change):
    with pytest.raises(NewsError, match="bulk_pagination_totals_invalid"):
        cnyes_pagination(response(payload(**change)))


def test_pagination_rejects_duplicates_and_ignored_dates():
    with pytest.raises(NewsError):
        cnyes_pagination(response(payload(ids=(1, 1))))
    with pytest.raises(NewsError):
        cnyes_pagination(response(payload(), cnyes_url(BASE, start=1609459200, end=1612137599)))
    assert cnyes_pagination(response(payload(ids=())))[0]["total"] == 0


def test_source_observed_empty_zero_page_window_is_not_a_retry():
    page, rows = cnyes_pagination(response(payload(ids=(), last_page=0)))
    assert not rows and page["total"] == 0
    assert page["pages"] == 1 and page["publisher_last_page_raw"] == 0
    with pytest.raises(NewsError, match="bulk_pagination_totals_invalid"):
        cnyes_pagination(response(payload(ids=(1,), last_page=0)))


def test_verified_publisher_join_duplicates_preserve_raw_span_and_unique_bodies(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    data = payload(ids=tuple(range(1, 30)) + (1, 1, 1), total=61, **{'from': 1, 'to': 32})
    page, posts = cnyes_pagination(response(data))
    assert len(posts) == page['returned_rows'] == 32
    assert page['unique_rows'] == 29 and page['duplicate_rows'] == 3
    rows, errors = cnyes_records(response(data), http, reg.sources['cnyes'])
    assert len(rows) == 29 and len(errors) == 3
    assert {e['error'] for e in errors} == {'bulk_exact_duplicate_row'}
    assert all(r['source_page_duplicate_rows'] == 3 for r in rows)
    data['items']['data'][-1]['content'] = '這是衝突版本，不得靜默任選一個。'
    with pytest.raises(NewsError, match='bulk_pagination_totals_invalid'):
        cnyes_pagination(response(data))


@pytest.mark.parametrize('change', [
    {'from': 2, 'to': 33}, {'from': 1, 'to': 31}, {'from': None, 'to': None},
    {'total': 0, 'last_page': 1},
])
def test_excess_join_rows_need_exact_span_and_nonzero_total(change):
    data = payload(ids=tuple(range(1, 30)) + (1, 1, 1), total=61, **{'from': 1, 'to': 32})
    data['items'].update(change)
    with pytest.raises(NewsError, match='bulk_pagination_totals_invalid'):
        cnyes_pagination(response(data))


@pytest.mark.parametrize('key,value', [('payment', False), ('publishAt', 1388534400.0), ('hasCoverPhoto', True)])
def test_duplicate_equivalence_preserves_json_permission_and_clock_types(key, value):
    data = payload(ids=(1, 1), **{'from': 1, 'to': 2})
    if key == 'hasCoverPhoto':
        data['items']['data'][0][key] = 1
    data['items']['data'][1][key] = value
    with pytest.raises(NewsError, match='bulk_pagination_totals_invalid'):
        cnyes_pagination(response(data))


@pytest.mark.parametrize('body', ['公開來源的完整快訊，長度不到一百字，但數值與發布時間仍應原樣保留。',
                                 'This is the complete short published flash report with original values.'])
def test_short_publisher_full_content_is_not_a_missing_body(cfg, tmp_path, body):
    reg, http = registry_http(cfg, tmp_path)
    rows, errors = cnyes_records(response(payload(ids=(1,), content=body)), http, reg.sources['cnyes'])
    assert not errors and rows[0]['body'] == body
    assert rows[0]['short_publisher_content'] and rows[0]['authors_json'] == '[]'


@pytest.mark.parametrize('content', ['', None, '測試標題', '<script>hidden</script>', '&lt;p&gt;null&lt;/p&gt;', '<p>null</p>'])
def test_missing_body_cannot_borrow_title_or_summary(cfg, tmp_path, content):
    reg, http = registry_http(cfg, tmp_path)
    data = payload(ids=(1,))
    data['items']['data'][0].update(content=content, summary='摘要不得取代完整正文。' * 20)
    rows, errors = cnyes_records(response(data), http, reg.sources['cnyes'])
    assert not rows and errors[0]['error'] in {'bulk_body_not_provided', 'body_not_found'}


@pytest.mark.parametrize('missing_field', [False, True, 'encoded_null', 'empty_string'])
def test_valid_metadata_only_page_advances_without_false_parser_circuit(cfg, tmp_path, missing_field):
    from downloader.news_fulltext_metrics import NewsMetrics
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    url = cnyes_url(BASE, start=1388505600, end=1391212799)
    data = payload(ids=tuple(range(1, 31)), total=31)
    for post in data['items']['data']:
        post['content'] = None
        if missing_field is True:
            del post['content']
        elif missing_field == 'encoded_null':
            post['content'] = '&lt;p&gt;null&lt;/p&gt;'
        elif missing_field == 'empty_string':
            post['content'] = ''
    r = response(data, url)
    http.get = lambda *a, **k: r
    http.metrics = NewsMetrics()
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute('SELECT * FROM discovery').fetchone())
        parsed, links, records, errors = fetch_discovery(http, task)
        assert len(links) == 30 and all(title and date for _, title, date in links)
        assert accept_discovery(queue, task, parsed, links, records, errors) == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_endpoint_cooldowns').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM discovery WHERE url=?',
                                (cnyes_url(BASE, page=2, start=1388505600, end=1391212799),)).fetchone()[0] == 1
        assert queue.status()['states'] == {'pending': 30}
        counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert counts['bulk_metadata_only_html_fallback_pages'] == 1
        assert counts.get('bulk_valid_body_pages_with_item_errors', 0) == 0
        assert queue.db.execute('SELECT parser_fingerprint FROM discovery WHERE url=?', (url,)).fetchone()[0]
    finally:
        queue.close()


@pytest.mark.parametrize('item_error', ['missing_body', 'exact_duplicate'])
def test_mixed_valid_body_page_is_not_counted_as_metadata_only(cfg, tmp_path, item_error):
    registry, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        data = payload(ids=(1, 2) if item_error == 'missing_body' else (1, 1))
        if item_error == 'missing_body':
            data['items']['data'][1]['content'] = None
        else:
            data['items'].update({'from': 1, 'to': 2})
        raw = response(data)
        task = {'url': raw.url, 'provider': 'cnyes', 'kind': 'cnyes_news'}
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,provider,kind) VALUES (?,?,?)',
                             (raw.url, 'cnyes', 'cnyes_news'))
        records, errors = cnyes_records(raw, http, registry.sources['cnyes'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        links.extend((e['url'], e['title'], e['metadata_date_raw']) for e in errors if e['error'] != 'bulk_exact_duplicate_row')
        assert accept_discovery(queue, task, raw, links, records, errors) == 1
        metrics = queue.metrics.snapshot()
        counts = metrics['providers']['cnyes']['counts']
        assert metrics['cnyes_bulk_page_classification_version'] == 2
        assert counts['bulk_valid_body_pages_with_item_errors'] == 1
        assert counts.get('bulk_metadata_only_html_fallback_pages', 0) == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        queue.close()


def test_actual_soft_404_is_distinct_from_short_or_missing_article(cfg, tmp_path):
    from downloader.news_fulltext_policy import extract_article
    reg, _ = registry_http(cfg, tmp_path)
    source = reg.sources['cnyes']
    with pytest.raises(NewsError, match='source_not_found'):
        extract_article('<html><title>404 Not Found</title><body>\u200c \u200c</body></html>'.encode(), source)
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(b'<html><title>404 Not Found</title><body><div id="article-container">real incomplete article</div></body></html>', source)


def test_only_observed_reviewed_cnyes_category_endpoints_are_registered(cfg, tmp_path):
    cfg["providers"][0]["cnyes_apis"].append(BASE.replace("headline", "stock_report"))
    assert len(PolicyRegistry(cfg, base=tmp_path).sources["cnyes"].cnyes_apis) == 2
    cfg["providers"][0]["cnyes_apis"].append(BASE.replace("headline", "hotai"))
    with pytest.raises(ValueError, match="explicitly registered"):
        PolicyRegistry(cfg, base=tmp_path)


def test_full_acceptance_shared_raw_and_print_alias_no_second_request(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        url = cnyes_url(BASE, start=1388505600, end=1391212799)
        with queue.db:
            queue.add_articles([{"url": "https://news.cnyes.com/news/print/1"}])
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        r = response(payload(), url)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        links = [(x["requested_url"], x["title"], x["metadata_date_raw"]) for x in records]
        assert accept_discovery(queue, task, r, links, records, errors) == 3
        assert queue.status()["states"] == {"complete": 3}
        verified = verify_bodies(tmp_path)
        assert verified["accepted"] and verified["raw_response_bytes"] == len(r.body)
        # Already current bodies do not get downloaded/rewritten on replay.
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        assert accept_discovery(queue, task, r, links, records, errors) == 0
        assert verify_bodies(tmp_path)["body_versions_verified"] == 3
    finally:
        queue.close()


def test_next_page_is_checkpointed_only_after_durable_acceptance(cfg, tmp_path, monkeypatch):
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    url = cnyes_url(BASE, start=1388505600, end=1391212799)
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        r = response(payload(ids=tuple(range(1, 31)), total=31), url)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        links = [(x["requested_url"], x["title"], x["metadata_date_raw"]) for x in records]
        original = queue.accept
        monkeypatch.setattr(queue, "accept", lambda _: (_ for _ in ()).throw(OSError("fsync failed")))
        with pytest.raises(OSError):
            accept_discovery(queue, task, r, links, records, errors)
        assert queue.db.execute("SELECT count(*) FROM discovery").fetchone()[0] == 1
        assert queue.db.execute("SELECT count(*) FROM bulk_page_evidence").fetchone()[0] == 0
        monkeypatch.setattr(queue, "accept", original)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        assert accept_discovery(queue, task, r, links, records, errors) == 30
        next_url = cnyes_url(BASE, page=2, start=1388505600, end=1391212799)
        assert queue.db.execute("SELECT kind FROM discovery WHERE url=?", (next_url,)).fetchone()[0] == "cnyes_news"
        assert verify_bodies(tmp_path)["accepted"]
        # A repeated ID page claiming a different page is not forward progress.
        second = dict(queue.db.execute("SELECT * FROM discovery WHERE url=?", (next_url,)).fetchone())
        r2 = response(payload(ids=tuple(range(1, 31)), page=2, total=60), next_url)
        with queue.db:
            queue.db.execute("UPDATE bulk_page_evidence SET total=60")
        records, errors = cnyes_records(r2, http, reg.sources["cnyes"])
        with pytest.raises(NewsError, match="bulk_pagination_no_progress"):
            accept_discovery(queue, second, r2, links, records, errors)
    finally:
        queue.close()


def test_live_verifier_pins_heads_before_a_concurrent_acceptance(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_queue as module
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://news.cnyes.com/news/id/1"}, {"url": "https://news.cnyes.com/news/id/2"}])
        def record(i):
            r = response(payload(ids=(i,)))
            rows, _ = cnyes_records(r, http, reg.sources["cnyes"])
            rows[0]["raw_response_body"] = r.body
            rows[0]["source_response_article_id"] = rows[0]["article_id"]
            return rows[0]
        queue.accept([record(1)])
        original = module.source_path
        committed = False
        def accept_after_snapshot(root, relative):
            nonlocal committed
            if not committed:
                committed = True
                queue.accept([record(2)])
            return original(root, relative)
        monkeypatch.setattr(module, "source_path", accept_after_snapshot)
        result = verify_bodies(tmp_path)
        assert result["accepted"] and result["complete_queue_heads_verified"] == 1
        assert queue.status()["states"] == {"complete": 2}
        assert verify_bodies(tmp_path)["complete_queue_heads_verified"] == 2
    finally:
        queue.close()


def test_status_and_queue_identify_the_same_owner_generation_on_restart(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    first = queue.status()["owner_id"]
    assert queue.db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0] == first
    queue.close()
    resumed = NewsQueue(tmp_path, reg)
    try:
        assert resumed.status()["owner_id"] == resumed.owner and resumed.owner != first
        assert resumed.db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()[0] == resumed.owner
    finally:
        resumed.close()


def test_readonly_runtime_audit_rejects_old_status_generation(cfg, tmp_path, monkeypatch, capsys):
    from downloader.artifact_io import atomic_write_json
    from scripts import audit_cnyes_runtime as module
    cfg["root"] = "lake"
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path / "lake", reg)
    monkeypatch.setattr(module, "load_config", lambda _: (cfg, reg, tmp_path))
    monkeypatch.setattr(module.subprocess, "check_output", lambda *a, **k: "MainPID=22\nActiveState=active\nExecMainStartTimestamp=fixture\n")
    try:
        queue.status()
        first, final, verification = (tmp_path / name for name in ("first.json", "final.json", "verify.json"))
        assert module.main(["--output", str(first)]) == 0
        r = response(payload(ids=(1,)))
        rows, _ = cnyes_records(r, http, reg.sources["cnyes"])
        with queue.db:
            queue.add_articles([{"url": rows[0]["requested_url"]}])
        rows[0]["raw_response_body"] = r.body
        rows[0]["source_response_article_id"] = rows[0]["article_id"]
        queue.accept(rows)
        current_status = queue.status()
        atomic_write_json(verification, verify_bodies(queue.root))
        command = ["--output", str(final), "--previous", str(first), "--body-verification", str(verification)]
        assert module.main(command) == 0
        accepted = json.loads(final.read_text())
        assert accepted["accepted"] and accepted["production_progress"]["new_stored_url_heads"] == 1
        assert accepted["production_progress"]["http_counts_delta"] is not None
        current_status["owner_id"] = "old-generation"
        atomic_write_json(queue.root / "status.json", current_status)
        assert module.main(command) == 1
        rejected = json.loads(final.read_text())
        assert not rejected["accepted"] and not rejected["engineering_checks"]["status_database_owner_consistent"]
        assert rejected["production_progress"]["http_counts_delta"] is None
    finally:
        queue.close()


def test_paid_content_cannot_enter_private_shared_batch(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        url = cnyes_url(BASE)
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        data = payload()
        data["items"]["data"][0]["payment"] = 1
        r = response(data)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        assert errors[0]["error"] == "bulk_post_not_public"
        assert accept_discovery(queue, task, r, [(x["requested_url"], x["title"], x["metadata_date_raw"]) for x in records], records, errors) == 0
        assert queue.status()["states"] == {"pending": 1}
        assert queue.db.execute("SELECT error_code FROM discovery").fetchone()[0] == "bulk_response_contains_restricted_text"
        assert not (tmp_path / "bodies").exists() and not (tmp_path / "quarantine").exists()
        unchanged_task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        unchanged = Response(url, 304, {}, b"")
        assert accept_discovery(queue, unchanged_task, unchanged, [], [], []) == 0
        checkpoint = queue.db.execute("SELECT error_code,due FROM discovery").fetchone()
        assert checkpoint[0] == "bulk_response_contains_restricted_text"
        assert 86390 < checkpoint[1] - time.time() <= 86400
        assert queue.db.execute("SELECT count(*) FROM bulk_item_errors").fetchone()[0] == 1
    finally:
        queue.close()


def test_mixed_restricted_page_keeps_public_html_fallback_and_advances(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        url = cnyes_url(BASE, start=1388534400, end=1391212799)
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        data = payload(ids=tuple(range(1, 31)), total=31)
        data["items"]["data"][0]["payment"] = 1
        r = response(data, url)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        links = [(x["requested_url"], x["title"], x["metadata_date_raw"]) for x in records]
        assert accept_discovery(queue, task, r, links, records, errors) == 0
        assert queue.status()["states"] == {"pending": 29}
        assert queue.db.execute("SELECT count(*) FROM articles WHERE url='https://news.cnyes.com/news/id/1'").fetchone()[0] == 0
        assert queue.db.execute("SELECT error_code FROM bulk_item_errors").fetchone()[0] == "bulk_post_not_public"
        next_url = cnyes_url(BASE, page=2, start=1388534400, end=1391212799)
        assert queue.db.execute("SELECT url FROM discovery WHERE url=?", (next_url,)).fetchone()
        assert not (tmp_path / "bodies").exists() and not (tmp_path / "quarantine").exists()
        assert queue.metrics.snapshot()["providers"]["cnyes"]["counts"]["restricted_pages_metadata_only"] == 1
    finally:
        queue.close()


@pytest.mark.parametrize("pages,days", [(31, 31), (125, 30), (300, 2), (900, 31)])
def test_adaptive_windows_cover_exact_bounds_and_respect_date_density(pages, days):
    low, high = 1388534400, 1388534400 + days * 86400 - 1
    parent = cnyes_url(BASE, start=low, end=high)
    children = cnyes_window_partitions(parent, pages)
    # A UTC-aligned range intersects an extra partial publication date.
    assert len(children) == min(days + 1, (pages + 29) // 30)
    bounds = [(int(parse_qs(urlsplit(u).query)["startAt"][0]), int(parse_qs(urlsplit(u).query)["endAt"][0])) for u in children]
    assert bounds[0][0] == low and bounds[-1][1] == high
    assert all(a <= b for a, b in bounds)
    assert all(left[1] + 1 == right[0] for left, right in zip(bounds, bounds[1:]))
    assert cnyes_history_priority(cnyes_url(BASE)) < cnyes_history_priority(children[-1]) < cnyes_history_priority(children[0])
    assert not cnyes_window_partitions(cnyes_url(BASE), pages)
    assert not cnyes_window_partitions(parent, 30)
    with pytest.raises(ValueError):
        cnyes_url(BASE, 31)


def test_oversized_window_subdivides_before_page_31_retains_error_and_receipts(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        url = cnyes_url(BASE, start=1388534400, end=1391212799)
        old_page31 = url.replace("page=1&", "page=31&")
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,error_code,attempts) VALUES (?,'cnyes_news','cnyes','discovery_http_422',1)", (old_page31,))
        task = dict(queue.db.execute("SELECT * FROM discovery WHERE url=?", (url,)).fetchone())
        r = response(payload(ids=tuple(range(1, 31)), total=1000), url)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        links = [(x["requested_url"], x["title"], x["metadata_date_raw"]) for x in records]
        assert accept_discovery(queue, task, r, links, records, errors) == 30
        children = cnyes_window_partitions(url, 34)
        old = queue.db.execute("SELECT * FROM discovery WHERE url=?", (old_page31,)).fetchone()
        assert old["error_code"] == "discovery_http_422" and old["attempts"] == 1
        assert json.loads(old["superseded_by_windows_json"]) == children
        assert queue.status()["discovery_errors"] == 0 and queue.status()["discovery_archived_errors"] == 1
        assert queue.status()["bulk_api_pages_waiting"] == 2
        assert queue.db.execute("SELECT count(*) FROM discovery WHERE url LIKE '%page=2&%'").fetchone()[0] == 0
        assert verify_bodies(tmp_path)["accepted"]
        seed_discovery(queue)
        assert queue.db.execute("SELECT superseded_by_windows_json FROM discovery WHERE url=?", (url,)).fetchone()[0]
    finally:
        queue.close()


def test_existing_page_cap_failure_is_reobserved_only_on_contract_migration(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    url = cnyes_url(BASE, start=1388534400, end=1391212799)
    old31 = url.replace("page=1&", "page=31&")
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'cnyes_news','cnyes',?)", (url, time.time() + 86400))
        queue.db.execute("INSERT INTO discovery(url,kind,provider,error_code) VALUES (?,'cnyes_news','cnyes','discovery_http_422')", (old31,))
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',1000,34,'2026-10-09')", (cnyes_scope(url),))
        queue.db.execute("UPDATE queue_settings SET value='legacy-monthly-contract' WHERE key='policy_fingerprint'")
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_window_dispatch_v3:cnyes'")
    queue.close()
    queue = NewsQueue(tmp_path, reg)
    try:
        assert queue.db.execute("SELECT due FROM discovery WHERE url=?", (url,)).fetchone()[0] == 0
        assert queue.db.execute("SELECT error_code,superseded_by_windows_json FROM discovery WHERE url=?", (old31,)).fetchone()[0] == "discovery_http_422"
        assert queue.db.execute("SELECT superseded_by_windows_json FROM discovery WHERE url=?", (old31,)).fetchone()[0]
        assert queue.status()["discovery_errors"] == 0
    finally:
        queue.close()


def test_dense_day_migration_restores_permitted_pages_without_resetting_refusal_or_cache(cfg, tmp_path):
    from downloader.news_fulltext import discovery_fingerprint
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    low, high = 1388534400 - 28800, 1388620799 - 28800
    first, second, third = (cnyes_url(BASE, page, start=low, end=high) for page in (1, 2, 3))
    scope = cnyes_scope(first)
    future = time.time() + 86400
    retirement = json.dumps({'pending_partition_scope': scope})
    try:
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=\'cnyes_window_dispatch_v3:cnyes\'')
            queue.db.execute('INSERT INTO bulk_api_coverage VALUES (?,\'cnyes\',1000,34,\'original-observation\')', (scope,))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,1,1000,?,\'original-observation\')', (scope, 'a' * 64))
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due,etag,parser_fingerprint) '
                             'VALUES (?,\'cnyes_news\',\'cnyes\',?,\'old-etag\',?)',
                             (first, future, discovery_fingerprint(reg.sources['cnyes'])))
            queue.db.executemany('INSERT INTO discovery(url,kind,provider,due,error_code,superseded_by_windows_json) '
                'VALUES (?,\'cnyes_news\',\'cnyes\',?,?,?)',
                [(second, future, 'discovery_http_403', retirement), (third, 0, None, retirement)])
        queue.migrate_cnyes_window_dispatch()
        assert tuple(queue.db.execute('SELECT due,etag FROM discovery WHERE url=?', (first,)).fetchone()) == (future, 'old-etag')
        assert tuple(queue.db.execute('SELECT due,error_code,superseded_by_windows_json FROM discovery WHERE url=?',
                                      (second,)).fetchone()) == (future, 'discovery_http_403', None)
        assert queue.db.execute('SELECT superseded_by_windows_json FROM discovery WHERE url=?', (third,)).fetchone()[0] is None
        assert queue.db.execute('SELECT error_code FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()[0] == 'cnyes_single_day_page_cap'
        # A different site's configuration must not revalidate every old day.
        cfg['providers'].append({'id': 'other', 'domains': ['other.test'], 'enabled': False})
        queue.registry = PolicyRegistry(cfg, base=tmp_path)
        queue.reconcile_policy()
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (first,)).fetchone()[0] == future
        assert queue.db.execute('SELECT value FROM counters WHERE key=\'cnyes_window_v3_permitted_jobs_restored\'').fetchone()[0] == 2
    finally:
        queue.close()


def test_dense_day_validated_pagination_seeds_first_missing_page_not_all_or_page31(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    low, high = 1388534400 - 28800, 1388620799 - 28800
    first = cnyes_url(BASE, start=low, end=high)
    scope = cnyes_scope(first)
    try:
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=\'cnyes_window_dispatch_v3:cnyes\'')
            queue.db.execute('INSERT INTO bulk_api_coverage VALUES (?,\'cnyes\',1000,34,\'original-observation\')', (scope,))
            queue.db.executemany('INSERT INTO bulk_page_evidence VALUES (?,?,1000,?,\'original-observation\')',
                                [(scope, 1, 'a' * 64), (scope, 2, 'b' * 64)])
        queue.migrate_cnyes_window_dispatch()
        jobs = queue.db.execute('SELECT url FROM discovery').fetchall()
        assert [r[0] for r in jobs] == [cnyes_url(BASE, 3, start=low, end=high)]
        queue.migrate_cnyes_window_dispatch()
        assert queue.db.execute('SELECT count(*) FROM discovery').fetchone()[0] == 1
    finally:
        queue.close()


def test_dense_window_migration_does_not_guess_pages_without_source_page_evidence(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=\'cnyes_window_dispatch_v3:cnyes\'')
            queue.db.execute('INSERT INTO bulk_api_coverage VALUES (?,\'cnyes\',1000,34,\'original-observation\')',
                             (cnyes_scope(cnyes_url(BASE, start=1388534400 - 28800, end=1388620799 - 28800)),))
        queue.migrate_cnyes_window_dispatch()
        assert queue.db.execute('SELECT count(*) FROM discovery').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 1
    finally:
        queue.close()


def test_multiday_migration_requests_one_actual_envelope_not_a_conditional_304(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    url = cnyes_url(BASE, start=1388534400, end=1391212799)
    try:
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=\'cnyes_window_dispatch_v3:cnyes\'')
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due,etag,last_modified) '
                             'VALUES (?,\'cnyes_news\',\'cnyes\',1234,\'cache\',\'old date\')', (url,))
            queue.db.execute('INSERT INTO bulk_api_coverage VALUES (?,\'cnyes\',1000,34,\'original-observation\')', (cnyes_scope(url),))
        queue.migrate_cnyes_window_dispatch()
        assert tuple(queue.db.execute('SELECT due,etag,last_modified FROM discovery WHERE url=?', (url,)).fetchone()) == (0, None, None)
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=987654321 WHERE url=?', (url,))
        queue.migrate_cnyes_window_dispatch()
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (url,)).fetchone()[0] == 987654321
    finally:
        queue.close()


def test_dense_single_day_gap_does_not_invent_hourly_coverage_or_request_page31(cfg, tmp_path):
    reg, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        url = cnyes_url(BASE, page=30, start=1388534400 - 28800, end=1388620799 - 28800)
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute("SELECT * FROM discovery").fetchone())
        r = response(payload(ids=tuple(range(871, 901)), page=30, total=901), url)
        records, errors = cnyes_records(r, http, reg.sources["cnyes"])
        assert accept_discovery(queue, task, r, [(x["requested_url"], x["title"], x["metadata_date_raw"]) for x in records], records, errors) == 30
        assert queue.status()["bulk_historical_window_gaps"][0]["error_code"] == "cnyes_single_day_page_cap"
        assert queue.db.execute("SELECT count(*) FROM discovery").fetchone()[0] == 1
        assert queue.status()["historical_completeness_verified"] is False
    finally:
        queue.close()


@pytest.mark.parametrize("host", ["news.cnyes.com", "m.cnyes.com"])
def test_print_fallback_uses_verified_permalink_preserves_requested_identity(cfg, tmp_path, host):
    url, permalink = f"https://{host}/news/print/1", "https://news.cnyes.com/news/id/1"
    http, session = http_fixture(cfg, tmp_path, {
        "https://news.cnyes.com/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        permalink: FakeResponse(article_html())})
    try:
        row = {"url": url, "id": hashlib.sha256(url.encode()).digest()}
        record = fetch_article(http, row)
        assert record["requested_url"] == url and record["final_url"] == permalink
        assert record["article_id"] == row["id"].hex()
        assert [u for u, _ in session.calls] == ["https://news.cnyes.com/robots.txt", permalink]
    finally:
        http.close()


def test_discovery_cycle_cap_rolls_over_bulk_instead_of_forcing_slow_html(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext as module
    monkeypatch.setattr(module, "seed_discovery", lambda _: None)
    url = cnyes_url(BASE, start=1388505600, end=1391212799)
    r = response(payload(ids=tuple(range(1, 31)), total=31), url)
    http, _ = http_fixture(cfg, tmp_path, {
        "https://api.cnyes.com/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        url: FakeResponse(r.body, headers=r.headers)})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://news.cnyes.com/news/id/9999"}])
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        result = run_cycle(queue, http, cfg, max_articles=512, max_discovery_requests=1)
        assert result["cycle"]["bulk_backlog_rollover"] is True
        assert result["cycle"]["article_attempts_started"] == 0
        assert result["states"] == {"complete": 30, "pending": 1}
        assert verify_bodies(tmp_path)["accepted"]
    finally:
        http.close()
        queue.close()


def test_seed_closed_windows_and_one_latest_page_not_unbounded_pages(cfg, tmp_path):
    reg, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, reg)
    try:
        seed_discovery(queue)
        initial = queue.db.execute("SELECT count(*) FROM discovery").fetchone()[0]
        assert 200 < initial < 300
        assert all(parse_qs(urlsplit(r[0]).query)["page"] == ["1"] for r in queue.db.execute("SELECT url FROM discovery"))
        seed_discovery(queue)
        assert initial == queue.db.execute("SELECT count(*) FROM discovery").fetchone()[0]
        assert queue.status()["bulk_api_pages_waiting"] == initial
    finally:
        queue.close()
