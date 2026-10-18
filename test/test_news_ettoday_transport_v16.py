"""Literal ETtoday transport identity and the single observed finance query.

Synthetic fixtures are not source articles. Historical query witnesses and
all retained-head metadata checks have separate private evidence receipts.
"""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import ETTODAY_LAYOUT_VERSIONS, NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies, utc_now, ettoday_finance_query_parser_repair
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks

URL = native.URLS[0]
QUERY = URL + '?redirect=1'


@pytest.mark.parametrize('version', sorted(ETTODAY_LAYOUT_VERSIONS.values()))
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_every_registered_version_binds_requested_identity_even_if_dom_is_self_consistent(version, host):
    final = URL + '1' if host == 'finance' else native.URLS[1]
    raw = str(native.page(host)).replace(URL, final).encode()
    http = SimpleNamespace(registry=clocks.registry(version),
        get=lambda *a, **kw: Response(final, 200, {'content-type':'text/html'}, raw))
    with pytest.raises(NewsError, match='article_identity_not_verified') as caught:
        fetch_article(http, {'url': URL, 'id': b'1'*16})
    assert caught.value.failure_response.response.url == final


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('requested,final', [(QUERY,QUERY),(QUERY,URL),(URL,QUERY),
    (QUERY.replace('https:','http:'), QUERY)])
def test_single_literal_parameter_never_rewrites_request_or_changes_original_values(parser, requested, final):
    cfg = clocks.config(16); cfg['providers'][0]['html_parser'] = parser
    registry = clocks.PolicyRegistry(cfg,base=Path.cwd())
    raw = str(native.page(parser=parser)).encode(); calls = []
    def get(url, **kwargs):
        calls.append(url)
        return Response(final,200,{'content-type':'text/html'},raw)
    record = fetch_article(SimpleNamespace(registry=registry,get=get),{'url':requested,'id':b'1'*16})
    old = extract_article(raw,replace(clocks.registry(15).sources['ettoday'],html_parser=parser),page_url=URL)
    assert extract_article(raw,registry.sources['ettoday'],page_url=final) == old
    assert record['body_sha256'] == old['body_sha256'] and record['raw_response_body'] == raw
    assert calls == [requested] and record['requested_url'] == requested and record['final_url'] == final
    assert record['source_request_url'] == final and record['declared_canonical_url'] == URL
    assert record['url_resolution_basis'] == 'verified_ettoday_finance_redirect_parameter_v16; literal_request_retained'
    assert not record['publish'] and not record['remote_training_export'] and not record['historical_point_in_time']


@pytest.mark.parametrize('query', ['redirect=0','redirect=2','redirect','redirect=01','REDIRECT=1',
    'redirect=1&redirect=1','redirect=1&extra=2','amp;redirect=1','from=1','redirect=%31'])
def test_unknown_query_is_rejected_before_transport(query):
    calls = []
    http = SimpleNamespace(registry=clocks.registry(16),get=lambda *a,**kw:calls.append(a))
    with pytest.raises(NewsError,match='unsupported_article_path'):
        fetch_article(http,{'url':URL+'?'+query,'id':b'1'*16})
    assert calls == []


@pytest.mark.parametrize('version', range(1,16))
def test_old_extraction_contracts_still_do_not_admit_query_urls(version):
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(str(native.page()).encode(),clocks.registry(version).sources['ettoday'],page_url=QUERY)


@pytest.mark.parametrize('final', [URL+'1?redirect=1', native.URLS[1], native.URLS[1]+'?redirect=1',
    URL+'?redirect=2','https://house.ettoday.net/news/3254747'])
def test_query_admission_does_not_waive_transport_identity_or_host_scope(final):
    http = SimpleNamespace(registry=clocks.registry(16),get=lambda *a,**kw:
        Response(final,200,{'content-type':'text/html'},str(native.page()).encode()))
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        fetch_article(http,{'url':QUERY,'id':b'1'*16})


@pytest.mark.parametrize('change', ['canonical','og','schema','author','paid','body'])
def test_parameter_admission_never_substitutes_for_native_article_validation(change):
    soup = native.page()
    if change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '1'
    elif change == 'og': soup.select_one('meta[property="og:url"]')['content'] += '1'
    elif change == 'schema': native.update_schema(soup,url=URL+'1')
    elif change == 'author': soup.select_one('#credit').decompose()
    elif change == 'body': soup.select_one('.story').decompose()
    else: native.update_schema(soup,isAccessibleForFree=False)
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(),clocks.registry(16).sources['ettoday'],page_url=QUERY)


def test_www_query_scope_and_unsupported_future_version_remain_closed():
    calls=[]
    http=SimpleNamespace(registry=clocks.registry(16),get=lambda *a,**kw:calls.append(a))
    with pytest.raises(NewsError,match='unsupported_article_path'):
        fetch_article(http,{'url':native.URLS[1]+'?redirect=1','id':b'1'*16})
    assert not calls
    with pytest.raises(ValueError,match='unreviewed embedded article parser'):
        clocks.registry(clocks.future_version())


def test_capture_abi_changes_without_resetting_permission_capacity_or_previous_heads(tmp_path):
    old, new = (clocks.registry(v).sources['ettoday'] for v in (15,16))
    assert new.data_fingerprint != old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    with closing(NewsQueue(tmp_path,clocks.registry(15))) as queue:
        queue.add_articles([{'url':QUERY}])
        queue.db.execute("UPDATE articles SET state='unsupported_article_path',attempts=3,due=99 WHERE url=?",(QUERY,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path,clocks.registry(16))) as queue:
        row=queue.db.execute('SELECT state,attempts,due FROM articles WHERE url=?',(QUERY,)).fetchone()
        assert tuple(row)==('unsupported_article_path',3,99)


@pytest.mark.parametrize('excluded', ['none','future_due','unobserved','future_observation','naive_observation',
    'leased','has_body','has_partition','other_query','other_failure'])
def test_only_observed_due_missing_query_heads_retry_once_without_changing_evidence(tmp_path,excluded):
    url=URL+('?redirect=2' if excluded=='other_query' else '?redirect=1')
    old,new=clocks.registry(15),clocks.registry(16)
    assert ettoday_finance_query_parser_repair(new.sources['ettoday'],old.sources['ettoday'].data_fingerprint)
    assert not ettoday_finance_query_parser_repair(new.sources['ettoday'],clocks.registry(14).sources['ettoday'].data_fingerprint)
    with closing(NewsQueue(tmp_path,old)) as queue:
        queue.add_articles([{'url':url}])
        values={'state':'unsupported_article_path','error_code':'unsupported_article_path','attempts':3,
            'due':99,'last_checked_at_utc':utc_now()}
        if excluded=='future_due':values['due']=(datetime.now(timezone.utc)+timedelta(days=1)).timestamp()
        elif excluded=='unobserved':values['last_checked_at_utc']=None
        elif excluded=='future_observation':values['last_checked_at_utc']=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat()
        elif excluded=='naive_observation':values['last_checked_at_utc']='2026-10-17T12:00:00'
        elif excluded=='leased':values.update(lease_owner='another-owner',lease_until=9999999999)
        elif excluded=='has_body':values['body_sha256']='a'*64
        elif excluded=='has_partition':values['parquet_path']='bodies/synthetic.parquet'
        elif excluded=='other_failure':values.update(state='article_identity_not_verified',error_code='article_identity_not_verified')
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',(*values.values(),url))
        queue.db.commit()
        before=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_transport_synthetic','unchanged')")
        queue.db.commit()
    with closing(NewsQueue(tmp_path,new)) as queue:
        after=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        expected={**before,'state':'retry'} if excluded=='none' else before
        assert after==expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_transport_synthetic'").fetchone()[0]=='unchanged'
        marker=queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'ettoday_finance_query_recheck_v16:%'").fetchone()
        assert len(json.loads(marker[0])['rows'])==(excluded=='none')
        queue.db.execute("UPDATE articles SET state='unsupported_article_path' WHERE url=?",(url,));queue.db.commit()
        queue.recheck_ettoday_finance_query(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?',(url,)).fetchone()[0]=='unsupported_article_path'


@pytest.mark.parametrize('parser',['lxml','html.parser'])
def test_formal_durable_receipt_and_independent_replay_preserve_literal_url(tmp_path,parser):
    cfg=clocks.config(16);cfg['providers'][0]['html_parser']=parser
    raw=str(native.page(parser=parser)).encode()
    http,session=http_fixture(cfg,tmp_path,{'https://finance.ettoday.net/robots.txt':FakeResponse(b'User-agent: *\nAllow: /'),
        QUERY:FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path,http.registry)) as queue:
            queue.add_articles([{'url':QUERY}])
            row=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(QUERY,)).fetchone())
            record=fetch_article(http,row);queue.accept([record])
            assert verify_bodies(tmp_path)['accepted'] and audit(tmp_path,[QUERY],registry=http.registry)['accepted']
            assert [call[0] for call in session.calls]==['https://finance.ettoday.net/robots.txt',QUERY]
    finally:
        http.close()
