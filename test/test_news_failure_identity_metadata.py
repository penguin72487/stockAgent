"""Failed identities yield bounded metadata, never a body or another request."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.artifact_io import sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_failure_diagnostics import fail, reports
from test_news_fulltext import config as cfg
from test_news_taiwanhot_native import URL, page
from test_news_taiwanhot_routes import registry, OLD
from test_news_max_original import settings as max_settings, schema_only_post, URL as MAX_URL


def evidence(root):
    paths=sorted((root/'identity_metadata').rglob('*.json'))
    return paths,[json.loads(p.read_bytes()) for p in paths]


@pytest.mark.parametrize('code', ['article_identity_not_verified', 'invalid_bulk_publication_clock'])
def test_max_received_clock_metadata_observes_missing_tag_and_hidden_template_without_refetch(max_settings, tmp_path, code):
    policy = PolicyRegistry(max_settings, base=tmp_path)
    raw = Response(MAX_URL, 200, {'content-type': 'text/html'}, schema_only_post().encode())
    with closing(NewsQueue(tmp_path / 'store', policy)) as queue:
        fail(queue, raw, code=code, received_at_utc='2026-10-20T10:00:00+00:00')
        paths, rows = evidence(queue.root)
        assert len(rows) == 1
        shape = rows[0]['facts']['max_native_clock_metadata']
        assert shape['contract_version'] == 2 and shape['head_clock_count'] == 1
        assert shape['header_time_count'] == 2 and shape['schema_clock_count'] == 2
        assert shape['header_times'][0]['in_native_hidden_update_template']
        assert not shape['body_text_exported'] and not shape['public_visibility_inferred']
        assert not shape['source_clock_inferred'] and not shape['article_durably_accepted']
        assert '完整合成研究細節' not in paths[0].read_text()
        assert not reports(queue.root)[0]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_failed_identity_only_retains_redacted_declarations_hashes_and_container_counts(cfg,tmp_path):
    policy=PolicyRegistry(cfg,base=tmp_path)
    url='https://publisher.test/article/42?token=private-query'
    html=b'<html><head><link rel="canonical" href="https://publisher.test/article/41?token=private-declaration#private-fragment"><meta property="og:url" content="https://user:private-password@publisher.test/42"></head><body><h1>private-title</h1><article>private-body</article></body></html>'
    with closing(NewsQueue(tmp_path/'store',policy)) as queue:
        fail(queue,Response(url,200,{'content-type':'text/html','set-cookie':'private-cookie'},html),code='article_identity_not_verified',received_at_utc='2026-10-14T15:00:00+00:00')
        paths,rows=evidence(queue.root)
        assert len(rows)==1
        r=rows[0]
        assert r['error_code']=='article_identity_not_verified' and r['response_sha256']==hashlib.sha256(html).hexdigest()
        assert r['facts']['canonical']['values'][0]['display_url']=='https://publisher.test/article/41'
        assert r['facts']['og_url']['values'][0]['display_url'] is None
        assert r['facts']['title_sha256']==[hashlib.sha256(b'private-title').hexdigest()]
        assert not r['raw_response_retained'] and not r['accepted_as_article'] and not r['publish'] and not r['remote_training_export']
        assert 'private-' not in paths[0].read_text() and paths[0].stat().st_mode & 0o777==0o600
        assert queue.db.execute('SELECT state,error_code,body_sha256 FROM articles').fetchone()[:]==('article_identity_not_verified','article_identity_not_verified',None)
        assert not reports(queue.root)[0] and queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]==0


def test_budget_survives_owner_source_identity_change_and_failed_writes(cfg,tmp_path,monkeypatch):
    policy=PolicyRegistry(cfg,base=tmp_path)
    root=tmp_path/'store'
    raw=Response('https://publisher.test/article/42',200,{'content-type':'text/html'},b'<h1>owned metadata</h1>')
    calls=[]
    def broken(*a,**kw):
        calls.append(a)
        raise OSError('owned storage failure')
    monkeypatch.setattr(diagnostics,'atomic_write_json',broken)
    with closing(NewsQueue(root,policy)) as queue:
        for _ in range(4):
            fail(queue,raw,code='article_identity_not_verified',received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(calls)==2 and not evidence(root)[0]
    provider=policy.match(raw.url).provider
    policy.sources[provider]=replace(policy.sources[provider],data_fingerprint='different-parser')
    with closing(NewsQueue(root,policy)) as queue:
        fail(queue,raw,code='article_identity_not_verified',received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(calls)==2
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0]=='2'


@pytest.mark.parametrize('status,mime,clock',[(403,'text/html','2026-10-14T15:00:00+00:00'),
    (429,'text/html','2026-10-14T15:00:00+00:00'),(200,'application/json','2026-10-14T15:00:00+00:00'),
    (200,'text/html','2026-10-14T15:00:00')])
def test_non_public_html_or_naive_observation_never_reserves_a_metadata_slot(cfg,tmp_path,status,mime,clock):
    with closing(NewsQueue(tmp_path,PolicyRegistry(cfg,base=tmp_path))) as queue:
        fail(queue,Response('https://publisher.test/article/42',status,{'content-type':mime},b'<h1>owned</h1>'),
             code='article_identity_not_verified',received_at_utc=clock)
        assert not evidence(tmp_path)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()


def test_worker_transfers_already_received_identity_failure_to_owner_without_io(tmp_path,monkeypatch):
    calls=[]
    raw=Response(URL,200,{'content-type':'text/html'},page(og_url=URL.replace('907503','42')))
    monkeypatch.setattr(diagnostics,'atomic_write_json',lambda *_a,**_k: pytest.fail('worker wrote'))
    def get(url,**kw):
        calls.append(url)
        return raw
    with pytest.raises(NewsError,match='article_identity_not_verified') as caught:
        fetch_article(SimpleNamespace(registry=registry(),get=get),{'url':OLD,'id':b'z'*32})
    assert calls==[OLD] and caught.value.failure_response.response is raw
    assert caught.value.failure_response.requested_url==OLD
    assert not (tmp_path/'identity_metadata').exists()


@pytest.mark.parametrize('backend',['html.parser','lxml'])
@pytest.mark.parametrize('missing_layout',[False,True])
def test_typed_taiwanhot_metadata_can_prove_private_received_raw_without_body_layout(backend,missing_layout):
    source=registry(backend=backend).sources['taiwanhot']
    raw=page(body='短文')
    if missing_layout:
        raw=raw.replace(b'class="news_content"',b'class="unknown-body-layout"')
    ctx=diagnostics.FailureResponse(Response(URL,200,{'content-type':'text/html'},raw),source.provider,
        source.data_fingerprint,OLD,'2026-10-14T15:00:00+00:00')
    assert diagnostics._public_taiwanhot_scope(ctx,source)


@pytest.mark.parametrize('change',['paid','rights','author','clock','og','title','challenge'])
def test_taiwanhot_diagnostic_scope_does_not_waive_paid_rights_clock_or_identity(change):
    source=registry().sources['taiwanhot']
    options={'paid':{'extra_body':'<div class="paywall">paid</div>'},'rights':{'body':'本文禁止轉載。'},
        'author':{'author':''},'clock':{'clock':'2026-10-14'},'og':{'og_url':URL.replace('907503','42')},
        'title':{'title':'另外的新聞'}}
    raw=page(**options.get(change,{}))
    if change=='challenge':
        raw=raw.replace('本篇新聞 | 台灣好新聞'.encode(),b'Just a moment')
    ctx=diagnostics.FailureResponse(Response(URL,200,{'content-type':'text/html'},raw),source.provider,
        source.data_fingerprint,OLD,'2026-10-14T15:00:00+00:00')
    assert not diagnostics._public_taiwanhot_scope(ctx,source)


def test_hourly_body_raw_budget_and_identity_metadata_are_separate_and_persistent(tmp_path):
    root=tmp_path/'store'
    policy=registry()
    raw=Response(URL,200,{'content-type':'text/html'},page(body='短文'))
    with closing(NewsQueue(root,policy)) as queue:
        for _ in range(4):
            fail(queue,raw,received_at_utc='2026-10-14T15:00:00+00:00')
        paths,rows=reports(root)
        assert len(paths)==3 and all(r['raw_response_body']==raw.body and r['diagnostic_only'] and not r['accepted_as_article'] for r in rows)
        assert sum(r['budget_period_kind']=='utc_hour' for r in rows)==1
        assert sum(r['budget_period_kind']=='utc_day' for r in rows)==2
        for p in paths:
            assert json.loads(p.with_suffix('.receipt.json').read_bytes())['sha256']==sha256_file(p)
        for _ in range(3):
            fail(queue,raw,code='article_identity_not_verified',received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(evidence(root)[0])==2 and len(reports(root)[0])==3
    with closing(NewsQueue(root,policy)) as queue:
        fail(queue,raw,received_at_utc='2026-10-14T15:00:00+00:00')
        fail(queue,raw,code='article_identity_not_verified',received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(evidence(root)[0])==2 and len(reports(root)[0])==3
