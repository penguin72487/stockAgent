"""Source routing, URL decoding and body capture have independent completion."""
from copy import deepcopy
from datetime import UTC, datetime
import json
import sqlite3
import threading

import pytest

from downloader import news_fulltext_routing as plan
from downloader import news_fulltext_urls as urls
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import _seed, _save, _server, _resolution_snapshot, NOW
from test_news_fulltext import config
from test_news_routing_host_plan import change, finish, retained


def checkpoint(queue):
    return json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (urls._ROUTING,)).fetchone()[0])


def test_decoded_links_can_be_complete_while_host_routing_is_pending(config, tmp_path, monkeypatch):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(queue, ['https://publisher.test/a', 'https://publisher.test/b', 'https://unregistered.test/c'])
        monkeypatch.setattr(urls.time, 'monotonic', lambda: 0.)
        urls.sync_original_url_routes(queue, limit=1, max_seconds=.1)
        summary = urls.original_url_progress(queue)
        assert summary['progress_ratio'] == 1 and summary['remaining_urls'] == 0
        work = summary['routing_work']
        assert work['snapshot_consistent'] and work['state'] == 'pending'
        assert (work['known_hosts'], work['completed_hosts'], work['pending_hosts']) == (2, 0, 2)
        assert work['host_progress_ratio'] == 0 and not work['completion_proven']
        assert work['conditional_eta_seconds'] is None and work['publisher_requests'] == 0
        assert not work['body_completion_claimed']
        finish(queue, limit=1, max_seconds=.1)
        work = urls.original_url_progress(queue)['routing_work']
        assert work['state'] == 'complete' and work['completion_proven']
        assert (work['known_hosts'], work['completed_hosts'], work['pending_hosts']) == (2, 2, 0)
        assert work['host_progress_ratio'] == 1 and work['conditional_eta_seconds'] is None
    finally:
        queue.close()


def test_host_progress_reads_only_small_plan_and_never_repairs_or_decodes(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(queue, ['https://publisher.test/a', 'https://unregistered.test/b'])
        finish(queue, max_seconds=.1)
        marker = checkpoint(queue)
        changes, statements = queue.db.total_changes, []
        queue.db.set_trace_callback(statements.append)
        forbidden = {'articles', 'original_url_resolutions', 'source_links', 'original_url_checkpoint_recoveries'}
        queue.db.set_authorizer(lambda op, table, *_: sqlite3.SQLITE_DENY
                                if op == sqlite3.SQLITE_READ and table in forbidden else sqlite3.SQLITE_OK)
        work = plan.route_plan_progress(queue.db, marker, literal_version=1)
        queue.db.set_authorizer(None)
        queue.db.set_trace_callback(None)
        assert work['state'] == 'complete' and queue.db.total_changes == changes
        assert not any(s.lstrip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE')) for s in statements)
        assert 'publisher.test' not in json.dumps(work)
    finally:
        queue.close()


def test_source_change_retains_other_proven_host_and_reports_only_reset_scope(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(queue, ['https://publisher.test/a', 'https://unregistered.test/b'])
        finish(queue, max_seconds=.1)
        change(queue, authorization_fingerprint='b' * 64)
        work = urls.original_url_progress(queue)['routing_work']
        assert work['state'] == 'pending' and work['host_progress_ratio'] == .5
        assert (work['known_hosts'], work['completed_hosts'], work['pending_hosts']) == (2, 1, 1)
    finally:
        queue.close()


def test_empty_current_host_plan_is_measured_zero_but_has_no_ratio_or_eta(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        finish(queue, max_seconds=.1)
        work = urls.original_url_progress(queue)['routing_work']
        assert work['snapshot_consistent'] and work['state'] == 'empty'
        assert (work['known_hosts'], work['completed_hosts'], work['pending_hosts']) == (0, 0, 0)
        assert work['completion_proven'] and work['host_progress_ratio'] is work['conditional_eta_seconds'] is None
    finally:
        queue.close()


@pytest.mark.parametrize('fault', ['legacy', 'dirty', 'manifest', 'geometry', 'missing', 'signature', 'cursor', 'marker'])
def test_old_dirty_or_inconsistent_plan_is_unknown_without_mutation(config, tmp_path, fault):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        retained(queue, ['https://publisher.test/a'])
        finish(queue, max_seconds=.1)
        marker = checkpoint(queue)
        with queue.db:
            if fault == 'legacy':
                marker = {'policy_fingerprint': 'legacy', 'after': 'done'}
            elif fault in {'dirty', 'manifest', 'geometry'}:
                key, value = {'dirty': (plan.DIRTY, '1'), 'manifest': (plan.INPUTS, '{broken'),
                              'geometry': (plan.GEOMETRY, 'unexpected')}[fault]
                queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (value, key))
            elif fault == 'missing':
                queue.db.execute('DELETE FROM ' + plan.TABLE)
            elif fault == 'signature':
                queue.db.execute('UPDATE ' + plan.TABLE + ' SET source_fingerprint=?', ('f' * 64,))
            elif fault == 'cursor':
                queue.db.execute('UPDATE ' + plan.TABLE + " SET after='not-a-blob'")
            else:
                marker['after'] = ''
        changes = queue.db.total_changes
        work = plan.route_plan_progress(queue.db, marker, literal_version=1)
        assert work['state'] == 'unavailable' and not work['snapshot_consistent']
        assert work['known_hosts'] is work['pending_hosts'] is work['host_progress_ratio'] is None
        assert not work['completion_proven'] and queue.db.total_changes == changes
    finally:
        queue.close()


def work_snapshot(total=616, completed=611):
    return {'contract_version': 1, 'snapshot_consistent': True,
            'state': ('empty' if total == 0 else 'complete') if completed == total else 'pending',
            'known_hosts': total, 'completed_hosts': completed, 'pending_hosts': total - completed,
            'completion_proven': completed == total, 'host_progress_ratio': 9999,
            'conditional_eta_seconds': 0, 'publisher_requests': 0, 'body_completion_claimed': False,
            'url': 'https://PRIVATE_HOST/?token=PRIVATE_TOKEN', 'body': 'PRIVATE_BODY'}


def test_public_route_progress_recomputes_ratio_and_does_not_trust_eta_or_private_fields(tmp_path):
    status = _seed(tmp_path)
    status['original_url_resolution'] = {**_resolution_snapshot(), 'routing_work': work_snapshot()}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    work = result['original_url_resolution']['routing_work']
    assert work['snapshot_consistent'] and work['host_progress_ratio'] == 611 / 616
    assert work['pending_hosts'] == 5 and not work['completion_proven']
    assert work['conditional_eta_seconds'] is None and work['body_completion_claimed'] is False
    assert result['original_url_resolution']['progress_ratio'] == .5
    assert result['acquisition']['stored_body_articles'] == 2
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [{'known_hosts': True}, {'completed_hosts': -1}, {'pending_hosts': 0},
                                 {'completion_proven': True}, {'state': 'complete'}, {'snapshot_consistent': False},
                                 {'body_completion_claimed': True}, {'publisher_requests': 1},
                                 {'publisher_requests': False}])
def test_public_inconsistent_routing_is_unknown_not_finished(tmp_path, patch):
    status = _seed(tmp_path)
    status['original_url_resolution'] = {**_resolution_snapshot(), 'routing_work': {**work_snapshot(), **patch}}
    _save(tmp_path, status)
    work = build_news_public_status(tmp_path, now=NOW)['original_url_resolution']['routing_work']
    assert not work['snapshot_consistent'] and not work['completion_proven']
    assert work['known_hosts'] is work['pending_hosts'] is work['host_progress_ratio'] is None
    assert work['conditional_eta_seconds'] is None


@pytest.mark.parametrize('width', [1440, 390, 320])
def test_real_page_separates_decode_routing_and_body_and_handles_unknown(tmp_path, width):
    from playwright.sync_api import expect, sync_playwright
    from scripts.probe_browser_runtime import browser_profile_launch_options

    status = _seed(tmp_path, now=datetime.now(UTC))
    resolution = _resolution_snapshot()
    resolution.update(known_redirect_urls=500, examined_urls=500,
                      counts={'literal_original': 100, 'opaque_original': 400})
    status['original_url_resolution'] = {**resolution, 'routing_work': work_snapshot()}
    _save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_address[1]}'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
            try:
                page = browser.new_page(viewport={'width': width, 'height': 1000})
                errors, remote_calls = [], []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('request', lambda request: remote_calls.append(request.url)
                        if not request.url.startswith(base + '/') else None)
                for raw, expected_count, expected_state, value in (
                        (work_snapshot(), '611 / 616 個主機', '基線尚未完成', 611 / 616),
                        (work_snapshot(completed=616), '616 / 616 個主機', '目前基線已完成', 1),
                        (None, '未知', '等待有效快照', None),
                        ({**work_snapshot(), 'pending_hosts': True}, '未知', '等待有效快照', None),
                        (work_snapshot(total=0, completed=0), '0 / 0 個主機', '無既有原站主機', None)):
                    current = deepcopy(status)
                    current['original_url_resolution']['routing_work'] = raw
                    _save(tmp_path, current)
                    page.goto(base + '/news/', wait_until='networkidle', timeout=15000)
                    page.locator('a[href="#acq-progress"]').click()
                    page.locator('#original-url-panel > summary').click()
                    expect(page.locator('#original-routing-panel')).to_be_visible()
                    expect(page.locator('#original-routing-count')).to_have_text(expected_count)
                    expect(page.locator('#original-routing-state')).to_have_text(expected_state)
                    expect(page.locator('#original-url-progress')).to_have_attribute('value', '1')
                    expect(page.locator('#download-progress')).to_have_attribute('value', '0.2')
                    bar = page.locator('#original-routing-progress').get_attribute('value')
                    assert bar is None if value is None else abs(float(bar) - value) < 1e-12
                    expect(page.locator('#original-routing-panel')).to_contain_text('不能由主機比例推算剩餘時間')
                    assert page.evaluate('document.documentElement.scrollWidth') <= width
                assert not errors and not remote_calls
            finally:
                browser.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)


def test_full_page_verifier_opens_deferred_tables_through_native_navigation(tmp_path):
    from scripts.verify_news_dashboard import verify

    _seed(tmp_path, now=datetime.now(UTC))
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = verify(f'http://127.0.0.1:{server.server_address[1]}/news/', tmp_path / 'rendered')
        assert result['state'] == 'accepted' and len(result['viewports']) == 4
        for view in result['viewports']:
            assert view['catalog']['stages'] == view['catalog']['expected_stages'] == 2
            assert view['catalog']['traffic_rows'] == view['catalog']['configured'] == 1
            assert not view['javascript_errors'] and not view['publisher_requests']
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=2)
