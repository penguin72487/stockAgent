"""Exact RSS failure facts without raw capture, permission or early retries."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading
import time

import pytest

from downloader import news_fulltext_metadata_feeds as feeds
from downloader.news_fulltext import accept_discovery, fail_discovery, fetch_discovery, run_cycle
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from test_news_metadata_feed_scope import config, setup, task, xml
from test_news_fulltext import FakeResponse

PREFIX='metadata_feed_failure_observation:v1:'


def observations(queue):
    return [json.loads(r[0]) for r in queue.db.execute(
        'SELECT value FROM queue_settings WHERE key LIKE ?', (PREFIX+'%',))]


def rejected(queue, response):
    current=task(queue)
    with pytest.raises(NewsError) as caught:
        accept_discovery(queue,current,response,[],[],[])
    return current,caught.value


@pytest.mark.parametrize('body,mime,stage,retry,delay', [
    (b'<html>DO_NOT_COPY_BODY</html>', 'text/html', 'response_envelope', False, 0),
    (b'<!DOCTYPE rss><rss version="2.0"><channel/></rss>', 'text/xml', 'response_envelope', False, 0),
    (b'<rss><channel>', 'text/xml', 'xml_syntax', True, 300),
    (b'<rss version="2.0"><channel>\x01</channel></rss>', 'text/xml', 'xml_syntax', True, 300),
    (b'<feed/>', 'text/xml', 'rss_root_or_version', False, 0),
    (b'<rss version="1.0"><channel/></rss>', 'text/xml', 'rss_root_or_version', False, 0),
    (b'<rss version="2.0"/>', 'text/xml', 'channel_cardinality', False, 0),
    (b'<rss version="2.0"><channel/><channel/></rss>', 'text/xml', 'channel_cardinality', False, 0),
])
def test_exact_original_error_and_retry_typed_witness(config,tmp_path,body,mime,stage,retry,delay):
    q,http,_=setup(config,tmp_path,{})
    try:
        current,error=rejected(q,Response(feeds.LTN_FEEDS[0],200,{'content-type':mime},body))
        assert (error.code,error.retry,error.delay)==('invalid_metadata_feed_representation',retry,delay)
        assert error.metadata_feed_failure_details['stage']==stage
        if stage=='xml_syntax':
            assert error.metadata_feed_failure_details['xml10_forbidden_ascii_control_count']==int(b'\x01' in body)
        assert 'response_sha256' not in error.metadata_feed_failure_details
        before=datetime.now(timezone.utc).timestamp()
        fail_discovery(q,current,error)
        row=q.db.execute('SELECT * FROM discovery WHERE url=?',(current['url'],)).fetchone()
        leaf,=observations(q)
        assert leaf['failure_details']==error.metadata_feed_failure_details
        assert leaf['attempts']==1 and row['attempts']==1
        assert leaf['checked_at_utc']==row['last_checked_at_utc']
        assert before+(300 if retry else 86400)<=row['due']
        assert leaf['retry_due_epoch']==row['due']
        assert leaf['raw_response_retained'] is False and leaf['accepted_body_count']==0
        assert leaf['permission_granted'] is False and leaf['publish'] is False
        assert leaf['remote_training_export'] is False
        assert len(json.dumps(leaf,separators=(',',':')).encode())<=4096
        assert 'DO_NOT_COPY_BODY' not in json.dumps(leaf)
        assert not (tmp_path/'metadata_feeds').exists() and not (tmp_path/'diagnostics').exists()
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0]==0
    finally:
        http.close();q.close()


def test_xml_code_and_position_only_no_header_or_content_copy(config,tmp_path):
    q,http,_=setup(config,tmp_path,{})
    try:
        response=Response(feeds.LTN_FEEDS[0],200,{'content-type':'text/xml; SECRET_HEADER=abc'},
            b'<rss version="2.0">\n<channel>DO_NOT_COPY_TITLE & broken</channel></rss>')
        current,error=rejected(q,response)
        fail_discovery(q,current,error)
        leaf,=observations(q)
        detail=leaf['failure_details']
        assert detail['stage']=='xml_syntax'
        assert type(detail['xml_error_code']) is int
        assert detail['xml_error_position'][0]==2
        assert detail['xml_error_position'][1]>0
        assert detail['xml10_forbidden_ascii_control_count']==0
        assert 'DO_NOT_COPY_TITLE' not in json.dumps(leaf) and 'SECRET_HEADER' not in json.dumps(leaf)
    finally:
        http.close();q.close()


@pytest.mark.parametrize('mode', ['missing_context','fingerprint','request','response','naive_clock',
                                 'future_clock','details','scope_disabled','valid_response','provider'])
def test_unproved_failure_cannot_write_witness(config,tmp_path,mode):
    q,http,_=setup(config,tmp_path,{})
    try:
        current,error=rejected(q,Response(feeds.LTN_FEEDS[0],200,{'content-type':'text/xml'},b'<rss><channel>'))
        if mode=='missing_context':
            del error.failure_response
        elif mode=='fingerprint':
            error.failure_response=replace(error.failure_response,source_fingerprint='another_source')
        elif mode=='request':
            error.failure_response=replace(error.failure_response,requested_url=feeds.LTN_FEEDS[1])
        elif mode=='response':
            error.failure_response=replace(error.failure_response,response=Response('https://outside.test/a',200,{'content-type':'text/xml'},b'<rss>'))
        elif mode=='naive_clock':
            error.failure_response=replace(error.failure_response,received_at_utc='2026-10-13T20:00:00')
        elif mode=='future_clock':
            error.failure_response=replace(error.failure_response,received_at_utc=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat())
        elif mode=='details':
            error.metadata_feed_failure_details={'stage':'invented'}
        elif mode=='scope_disabled':
            q.registry.sources['ltn']=replace(q.registry.sources['ltn'],metadata_feeds=())
        elif mode=='valid_response':
            error.failure_response=replace(error.failure_response,response=Response(feeds.LTN_FEEDS[0],200,{'content-type':'text/xml'},xml()))
        else:
            current['provider']='different_provider'
        fail_discovery(q,current,error)
        assert observations(q)==[]
        assert q.db.execute('SELECT error_code FROM discovery WHERE url=?',(current['url'],)).fetchone()[0]==error.code
        assert not (tmp_path/'metadata_feeds').exists()
    finally:
        http.close();q.close()


def test_no_raw_reservations_and_one_latest_key_per_declared_feed(config,tmp_path):
    q,http,_=setup(config,tmp_path,{})
    try:
        keys={'failure_diagnostic_attempts:v1:ltn:20261013':'2',
              'failure_diagnostic_hourly_attempts:v1:ltn:20261013T21:provider_general':'1'}
        q.db.executemany('INSERT INTO queue_settings VALUES (?,?)',keys.items());q.db.commit()
        for _ in range(2):
            current,error=rejected(q,Response(feeds.LTN_FEEDS[0],200,{'content-type':'text/xml'},b'<rss><channel>'))
            fail_discovery(q,current,error)
        leaf,=observations(q)
        assert leaf['attempts']==2
        assert {k:q.db.execute('SELECT value FROM queue_settings WHERE key=?',(k,)).fetchone()[0] for k in keys}==keys
        assert q.db.execute('SELECT count(*) FROM discovery').fetchone()[0]==14
        assert not (tmp_path/'diagnostics').exists()
    finally:
        http.close();q.close()


def test_diagnostic_insert_failure_does_not_undo_source_failure(config,tmp_path):
    q,http,_=setup(config,tmp_path,{})
    original=q.db
    class DiagnosticFault:
        def __getattr__(self,name): return getattr(original,name)
        def __enter__(self): original.__enter__();return self
        def __exit__(self,*args): return original.__exit__(*args)
        def execute(self,sql,args=()):
            if sql.startswith('INSERT INTO queue_settings'):
                raise sqlite3.OperationalError('diagnostic_write_failure')
            return original.execute(sql,args)
    try:
        current,error=rejected(q,Response(feeds.LTN_FEEDS[0],200,{'content-type':'text/xml'},b'<rss><channel>'))
        q.db=DiagnosticFault()
        fail_discovery(q,current,error)
        assert observations(q)==[]
        row=q.db.execute('SELECT * FROM discovery WHERE url=?',(current['url'],)).fetchone()
        assert row['attempts']==1 and row['error_code']==error.code and row['due']>0
    finally:
        q.db=original;http.close();q.close()


def test_normal_worker_only_existing_requests_queue_writer_and_zero_bodies(config,tmp_path,monkeypatch):
    url=feeds.LTN_FEEDS[0]
    routes={'https://news.ltn.com.tw/robots.txt':FakeResponse(b'User-agent: *\nAllow: /\n'),
            url:FakeResponse(b'<rss><channel>',headers={'Content-Type':'text/xml'})}
    q,http,session=setup(config,tmp_path,routes)
    writer=threading.get_ident();observed=[]
    original=feeds.record_failure_observation
    def record(*args):
        observed.append(threading.get_ident());return original(*args)
    monkeypatch.setattr(feeds,'record_failure_observation',record)
    try:
        run_cycle(q,http,config,max_articles=1,max_discovery_requests=1,provider_ids={'ltn'})
        assert observed==[writer]
        assert len(observations(q))==1
        assert len(session.calls)==2
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0]==0
        assert not (tmp_path/'metadata_feeds').exists()
    finally:
        http.close();q.close()


def test_deferred_feed_resumes_after_real_gate_without_false_failure_observation(config, tmp_path):
    url = feeds.LTN_FEEDS[0]
    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(xml(), headers={'Content-Type': 'application/rss+xml'})}
    q, http, session = setup(config, tmp_path, routes)
    try:
        with q.db:
            q.db.execute("UPDATE discovery SET attempts=5,last_checked_at_utc='2026-01-01T00:00:00+00:00' "
                         "WHERE url=?", (url,))
        original = task(q)
        http.defer('ltn', .3)
        with pytest.raises(NewsError) as caught:
            fetch_discovery(http, original)
        assert caught.value.code == 'provider_cooldown' and caught.value.request_deferred
        assert not session.calls
        fail_discovery(q, original, caught.value)
        deferred = task(q)
        assert deferred['attempts'] == 5
        assert deferred['last_checked_at_utc'] == original['last_checked_at_utc']
        assert not observations(q)
        remaining = max(0, deferred['due'] - time.time())
        time.sleep(remaining + .02)
        assert deferred['due'] <= time.time()
        result = fetch_discovery(http, task(q))
        assert accept_discovery(q, task(q), *result) == 0
        completed = task(q)
        assert completed['attempts'] == 0 and completed['error_code'] is None
        assert completed['last_checked_at_utc'] > original['last_checked_at_utc']
        assert [call[0] for call in session.calls] == ['https://news.ltn.com.tw/robots.txt', url]
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 1
        assert q.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
    finally:
        http.close(); q.close()


def test_owner_cycle_recovers_from_a_gate_opened_after_dispatch(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as news

    url = feeds.LTN_FEEDS[0]
    config['providers'][0]['metadata_feed_policy']['urls'] = [url]
    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(xml(), headers={'Content-Type': 'application/rss+xml'})}
    q, http, session = setup(config, tmp_path, routes)
    original_fetch = news.fetch_discovery
    calls = []
    def racing_gate(client, current):
        calls.append(current['url'])
        if len(calls) == 1:
            client.defer('ltn', .3)
        return original_fetch(client, current)
    monkeypatch.setattr(news, 'fetch_discovery', racing_gate)
    try:
        with q.db:
            q.db.execute('UPDATE discovery SET attempts=5 WHERE url=?', (url,))
        run_cycle(q, http, config, max_articles=1, max_discovery_requests=1, provider_ids={'ltn'})
        deferred = task(q)
        assert calls == [url] and not session.calls
        assert deferred['attempts'] == 5 and deferred['last_checked_at_utc'] is None
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        time.sleep(max(0, deferred['due'] - time.time()) + .02)
        run_cycle(q, http, config, max_articles=1, max_discovery_requests=1, provider_ids={'ltn'})
        assert calls == [url, url]
        assert [call[0] for call in session.calls] == ['https://news.ltn.com.tw/robots.txt', url]
        assert task(q)['attempts'] == 0 and task(q)['error_code'] is None
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 1
        assert q.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
    finally:
        http.close(); q.close()


def test_resident_owner_wakes_and_recovers_after_deferred_discovery(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as news

    url = feeds.LTN_FEEDS[0]
    config['providers'][0]['metadata_feed_policy']['urls'] = [url]
    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(xml(), headers={'Content-Type': 'application/rss+xml'})}
    q, http, session = setup(config, tmp_path, routes)
    original_fetch, original_accept = news.fetch_discovery, news.accept_discovery
    calls = []
    def racing_gate(client, current):
        calls.append(current['url'])
        if len(calls) == 1:
            client.defer('ltn', .3)
        return original_fetch(client, current)
    def stop_after_accepted(*args):
        result = original_accept(*args)
        http.stop.set()
        return result
    monkeypatch.setattr(news, 'fetch_discovery', racing_gate)
    monkeypatch.setattr(news, 'accept_discovery', stop_after_accepted)
    try:
        news.run_service(q, http, config, max_articles=1, max_discovery_requests=1, duration_seconds=8)
        assert calls == [url, url]
        assert [call[0] for call in session.calls] == ['https://news.ltn.com.tw/robots.txt', url]
        assert task(q)['attempts'] == 0 and task(q)['error_code'] is None
        assert q.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 1
        assert q.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
    finally:
        http.close(); q.close()


def test_healthy_parser_does_not_add_failure_context_or_witness(config,tmp_path):
    q,http,_=setup(config,tmp_path,{})
    try:
        rows=feeds.parse(Response(feeds.LTN_FEEDS[0],200,{'content-type':'text/xml'},xml()),q.registry.sources['ltn'],feeds.LTN_FEEDS[0])
        assert len(rows)==1 and rows[0]['publication_raw']=='Tue, 13 Oct 2026 20:00:00 +0800'
        assert rows[0]['content_kind']=='rss_information_not_fulltext'
        assert observations(q)==[]
    finally:
        http.close();q.close()
