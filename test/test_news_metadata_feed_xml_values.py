"""Lossless declared-RSS values, not recovery mode or fulltext permission."""
from dataclasses import replace
from datetime import datetime, timezone
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_metadata_feeds as feeds
from downloader.news_fulltext import accept_discovery, fetch_discovery, seed_discovery
from downloader.news_fulltext_capacity import PublisherCapacity
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from test_news_metadata_feed_scope import config, setup, task, xml, FakeResponse

CONTROLS=tuple(range(9))+(11,12)+tuple(range(14,32))


def response(body):
    return Response(feeds.LTN_FEEDS[0],200,{'content-type':'application/rss+xml'},body)


def field_body(field,code):
    value=('原始'+chr(code)+'值 & 字元').encode()
    if field=='title':
        return xml().replace('<title>標題</title>'.encode(),b'<title><![CDATA['+value+b']]></title>'),value.decode()
    return xml().replace('<p>摘要，不是全文</p>'.encode(),value),value.decode()


@pytest.mark.parametrize('code',CONTROLS)
@pytest.mark.parametrize('field',['title','description'])
def test_every_forbidden_ascii_control_restores_native_declared_value(config,tmp_path,field,code):
    q,http,_=setup(config,tmp_path,{})
    try:
        body,expected=field_body(field,code)
        rows=feeds.parse(response(body),q.registry.sources['ltn'],feeds.LTN_FEEDS[0])
        assert rows[0]['title' if field=='title' else 'description_html']==expected
        assert rows[0]['publication_raw']=='Tue, 13 Oct 2026 20:00:00 +0800'
        assert rows[0]['publication_aware']=='2026-10-13T20:00:00+08:00'
        assert rows[0]['content_kind']=='rss_information_not_fulltext'
        assert not q.registry.sources['ltn'].allowed
    finally:
        http.close();q.close()


@pytest.mark.parametrize('encoding',['utf-8','utf-16'])
def test_healthy_xml_encoding_and_standard_whitespace_are_not_controls(config,tmp_path,encoding):
    q,http,_=setup(config,tmp_path,{})
    try:
        body=xml().decode().replace('標題','原始\t\r\n標題').encode(encoding)
        rows=feeds.parse(response(body),q.registry.sources['ltn'],feeds.LTN_FEEDS[0])
        assert rows[0]['title']=='原始\t\n標題'  # XML's existing CRLF rule, not new repair.
    finally:
        http.close();q.close()


@pytest.mark.parametrize('body',[
    xml().replace('<title>自由時報</title>'.encode(),b'<title>channel\x01</title>'),
    xml().replace(b'/123</link>',b'/123\x01</link>'),
    xml().replace(b'20:00:00',b'20:\x0100:00'),
    xml().replace(b'</title><description>',b'</title>\x01<description>'),
    xml().replace(b'<title>',b'<ti\x01tle>',1),
    xml().replace(b'<channel>',b'<channel x="\x01">'),
    xml().replace(b'<item>',b'<item><!--\x01-->'),
    xml().replace(b'<item>',b'<item><?comment \x01?>'),
    xml().replace(b'</rss>',b'\x01</rss>'),
    xml(extra='<author>\x01UNREVIEWED</author>'),
])
def test_control_outside_exact_declared_item_values_is_original_syntax_failure(config,tmp_path,body):
    q,http,_=setup(config,tmp_path,{})
    try:
        with pytest.raises(NewsError) as caught:
            accept_discovery(q,task(q),response(body),[],[],[])
        assert caught.value.code=='invalid_metadata_feed_representation'
        assert caught.value.metadata_feed_failure_details['stage']=='xml_syntax'
        assert not (tmp_path/'metadata_feeds').exists()
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0]==0
    finally:
        http.close();q.close()


@pytest.mark.parametrize('variant',['mismatch','entity','extra_field','invalid_utf8','excessive_controls','unsupported_declared_encoding_with_BOM'])
def test_other_invalid_representation_or_unreviewed_field_cannot_be_repaired(config,tmp_path,variant):
    q,http,_=setup(config,tmp_path,{})
    try:
        body,_=field_body('title',1)
        if variant=='mismatch':body=body.replace(b'</item>',b'</wrong>')
        elif variant=='entity':body=b'<!DOCTYPE rss [<!ENTITY x SYSTEM "file:///private">]>'+body
        elif variant=='extra_field':body=body.replace(b'</item>',b'<secret>NO_BODY</secret></item>')
        elif variant=='invalid_utf8':body=body.replace(b'\x01',b'\x01\xff')
        elif variant=='excessive_controls':body=body.replace(b'\x01',b'\x01'*257)
        else:body=b'\xef\xbb\xbf'+body.replace(b'<?xml version="1.0"?>',b'<?xml version="1.0" encoding="ISO-8859-1"?>')
        with pytest.raises(NewsError):accept_discovery(q,task(q),response(body),[],[],[])
        assert not (tmp_path/'metadata_feeds').exists()
        assert q.db.execute('SELECT count(*) FROM articles').fetchone()[0]==0
    finally:
        http.close();q.close()


def test_durable_raw_receipt_field_values_clocks_and_metadata_only_are_preserved(config,tmp_path):
    q,http,_=setup(config,tmp_path,{})
    try:
        body,title=field_body('title',0)
        body=body.replace('<p>摘要，不是全文</p>'.encode(),b'original\x01\x02summary')
        assert accept_discovery(q,task(q),response(body),[],[],[])==0
        head=q.db.execute('SELECT * FROM metadata_feed_heads').fetchone()
        receipt=json.loads((tmp_path/head['receipt_path']).read_bytes())
        retained=pq.read_table(tmp_path/receipt['parquet_path'],use_threads=False).to_pylist()[0]
        items=json.loads(retained['items_json'])
        assert retained['raw_response_body']==body
        assert items[0]['title']==title and items[0]['description_html']=='original\x01\x02summary'
        repair=receipt['xml_value_preservation']
        assert repair['control_count']==3 and repair['original_values_restored'] is True
        assert {x['field'] for x in repair['controls']}=={'title','description'}
        assert receipt['contract_version']==2
        assert receipt['article_bodies_accepted']==retained['article_bodies_accepted']==0
        article=q.db.execute('SELECT * FROM articles').fetchone()
        assert article['state']=='awaiting_authorization' and article['body_sha256'] is None
        assert article['title']==title and article['metadata_date_raw']=='Tue, 13 Oct 2026 20:00:00 +0800'
        assert not receipt['publish'] and not receipt['remote_training_export']
        assert (tmp_path/receipt['parquet_path']).stat().st_mode & 0o777==0o600
    finally:
        http.close();q.close()


@pytest.mark.parametrize('error',[None,'invalid_metadata_feed_representation','source_access_denied'])
def test_strict_v1_compatible_job_keeps_due_attempts_headers_and_original_error(config,tmp_path,error):
    q,http,_=setup(config,tmp_path,{})
    try:
        current,legacy=feeds.compatible_fingerprints(q.registry.sources['ltn'])
        assert current!=legacy
        q.db.execute('UPDATE discovery SET parser_fingerprint=?,due=?,attempts=7,error_code=?,etag=?,last_modified=? WHERE url=?',
            (legacy,time.time()+3600,error,'native-tag','native-last',feeds.LTN_FEEDS[0]));q.db.commit()
        original=task(q)
        seed_discovery(q)
        assert task(q)==original
    finally:
        http.close();q.close()


def test_parser_upgrade_does_not_reset_HTTP_capacity_fingerprint_or_refusal(config,tmp_path):
    q,http,_=setup(config,tmp_path,{})
    try:
        source=q.registry.sources['ltn'];current,legacy=feeds.compatible_fingerprints(source)
        upgraded=PublisherCapacity(http,config)
        q.registry.sources['ltn']=replace(source,metadata_feed_fingerprint=legacy)
        original=PublisherCapacity(http,config)
        assert upgraded.provider_fingerprints==original.provider_fingerprints
        assert upgraded.fingerprint==original.fingerprint
        q.registry.sources['ltn']=replace(source,metadata_feed_authorization={**source.metadata_feed_authorization,'reviewed_on':'DIFFERENT'})
        # A fake/new policy fingerprint must not borrow strict-v1 compatibility.
        assert feeds.compatible_fingerprints(q.registry.sources['ltn'])==(current,)
    finally:
        http.close();q.close()


def legacy_fixture(queue):
    """Synthetic protocol-v1 fixture; production originals are never retagged."""
    source=queue.registry.sources['ltn'];current,legacy=feeds.compatible_fingerprints(source)
    accept_discovery(queue,task(queue),response(xml()),[],[],[])
    head=queue.db.execute('SELECT * FROM metadata_feed_heads').fetchone()
    receipt=json.loads((queue.root/head['receipt_path']).read_bytes())
    native=pq.read_table(queue.root/receipt['parquet_path'],use_threads=False).to_pylist()[0]
    native['metadata_fingerprint']=legacy
    path=receipt['parquet_path'].replace(current,legacy)
    atomic_write_parquet(queue.root/path,table_from_records([native]),compression='zstd',durable=True)
    receipt.update(contract_version=1,parquet_path=path,metadata_fingerprint=legacy,
                   sha256=sha256_file(queue.root/path))
    receipt.pop('xml_value_preservation')
    receipt_path=path+'.receipt.json'
    atomic_write_json(queue.root/receipt_path,receipt,durable=True)
    queue.db.execute('UPDATE metadata_feed_heads SET parser_fingerprint=?,receipt_path=?',(legacy,receipt_path))
    queue.db.execute('UPDATE discovery SET parser_fingerprint=?,etag=? WHERE url=?',
                     (legacy,'original-v1',feeds.LTN_FEEDS[0]));queue.db.commit()
    return path,receipt_path,receipt


def test_exact_old_native_receipt_304_is_reused_not_retagged_or_refetched(config,tmp_path):
    routes={'https://news.ltn.com.tw/robots.txt':FakeResponse(b'User-agent: *\nAllow: /'),
            feeds.LTN_FEEDS[0]:FakeResponse(status=304)}
    q,http,session=setup(config,tmp_path,routes)
    try:
        path,receipt_path,native=legacy_fixture(q)
        raw_before=(tmp_path/path).read_bytes();receipt_before=(tmp_path/receipt_path).read_bytes()
        result=fetch_discovery(http,task(q))
        assert session.calls[-1][1]['headers']['If-None-Match']=='original-v1'
        assert accept_discovery(q,task(q),*result)==0
        assert (tmp_path/path).read_bytes()==raw_before
        assert (tmp_path/receipt_path).read_bytes()==receipt_before
        assert task(q)['parser_fingerprint']==native['metadata_fingerprint']
        assert feeds.progress(q.db,q.registry.sources['ltn'])['verified_feeds']==1
        assert q.db.execute('SELECT state FROM articles').fetchone()[0]=='awaiting_authorization'
    finally:
        http.close();q.close()


@pytest.mark.parametrize('mode',['policy','raw_sha','receipt_version','native_fingerprint','body_grant'])
def test_old_304_needs_exact_native_receipt_and_same_current_policy(config,tmp_path,mode):
    q,http,_=setup(config,tmp_path,{})
    try:
        path,receipt_path,receipt=legacy_fixture(q)
        if mode=='policy':
            source=q.registry.sources['ltn']
            q.registry.sources['ltn']=replace(source,metadata_feed_authorization={**source.metadata_feed_authorization,'reviewed_on':'OTHER'})
        elif mode=='raw_sha':receipt['sha256']='0'*64
        elif mode=='receipt_version':receipt['contract_version']=2
        elif mode=='native_fingerprint':receipt['metadata_fingerprint']='NOT_NATIVE'
        else:receipt['article_bodies_accepted']=1
        atomic_write_json(tmp_path/receipt_path,receipt,durable=True)
        with pytest.raises(NewsError):
            accept_discovery(q,task(q),Response(feeds.LTN_FEEDS[0],304,{},b''),[],[],[])
        assert q.db.execute('SELECT state FROM articles').fetchone()[0]=='awaiting_authorization'
    finally:
        http.close();q.close()
