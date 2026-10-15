"""A thin list title is not a body, identity, publication or access grant."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_cnyes_index as index
from downloader.news_fulltext import fail_discovery, fetch_discovery, seed_discovery
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, cnyes_bulk_fingerprint
from test_cnyes_news_bulk import response
from test_news_cnyes_public_index import DAY, accept, cfg, index_payload, queue, register


def legacy(source):
    old=index.compatible_fingerprints(source)[1]
    return replace(source,cnyes_index_data_fingerprint=old,cnyes_index_compatibility=())


def legacy_head(queue, *, page=1, total=2):
    current=queue.registry.sources['cnyes'];old=legacy(current)
    queue.registry.sources['cnyes']=old
    task,received,_=accept(queue,index_payload(page=page,total=total),page=page)
    queue.registry.sources['cnyes']=current
    return dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(task['url'],)).fetchone()),received


@pytest.mark.parametrize('title',['',' ','\n\t','&#32;'])
def test_native_string_title_can_be_thin_without_blocking_real_IDs_or_source_clocks(queue,title):
    data=index_payload();data['items']['data'][0]['title']=title
    received=response(data,index.request_url(queue.registry.sources['cnyes'],DAY))
    info,posts,links=index.parse_index(received,queue.registry.sources['cnyes'])
    assert posts==data['items']['data']
    assert links[0][0]=='https://news.cnyes.com/news/id/1'
    assert links[0][2]==links[1][2]
    assert links[0][1]==index.html.unescape(title)
    assert info['blank_title_metadata_rows']==int(not title.strip())
    assert info['article_bodies_accepted']==0 and len(links)==2


@pytest.mark.parametrize('title',[None,0,False,[],{},1.0])
def test_non_string_title_is_not_coerced_or_guessed(queue,title):
    data=index_payload();data['items']['data'][0]['title']=title
    with pytest.raises(NewsError,match='cnyes_index_unverified_metadata'):
        accept(queue,data)
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()
    assert not list(queue.root.glob('list_metadata/**/*.parquet'))


def test_missing_title_is_not_an_explicit_native_empty_string(queue):
    data=index_payload();del data['items']['data'][0]['title']
    with pytest.raises(NewsError,match='cnyes_index_unverified_metadata'):
        accept(queue,data)


@pytest.mark.parametrize('field,value',[('payment',True),('payment',2),('payment',None),
    ('publishAt',DAY*86400-28800-1),('categoryId',0),('categoryId',True)])
def test_blank_title_does_not_weaken_free_flag_clock_or_category_guards(queue,field,value):
    data=index_payload();data['items']['data'][0].update(title='',**{field:value})
    with pytest.raises(NewsError,match='cnyes_index_unverified_metadata'):
        accept(queue,data)
    assert not queue.db.execute('SELECT 1 FROM articles').fetchone()


def test_paid_blank_title_is_not_dispatched_and_unknown_fields_still_reject_entire_page(queue):
    data=index_payload();data['items']['data'][0].update(title='',payment=1)
    accept(queue,data)
    assert [r[0] for r in queue.db.execute('SELECT url FROM articles')]==['https://news.cnyes.com/news/id/2']
    other=index_payload(ids=(3,4));other['items']['data'][0]['title']=''
    other['items']['data'][1]['content']='not reviewed body field'
    with pytest.raises(NewsError,match='cnyes_index_unreviewed_metadata_fields'):
        accept(queue,other)
    assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0]==1


def test_original_empty_title_raw_receipt_and_next_page_are_durable_but_body_is_not(queue):
    data=index_payload(ids=tuple(range(1,31)),total=60);data['items']['data'][0]['title']=''
    task,received,count=accept(queue,data)
    row=queue.db.execute('SELECT * FROM cnyes_public_index_observations').fetchone()
    original=pq.read_table(queue.root/row['evidence_path']).to_pylist()[0]
    receipt=json.loads((queue.root/(row['evidence_path']+'.receipt.json')).read_bytes())
    assert original['raw_response_body']==received.body
    assert json.loads(original['raw_response_body'])['items']['data'][0]['title']==''
    assert receipt['blank_title_metadata_rows']==1 and receipt['index_data_contract_version']==2
    assert count==0 and original['accepted_body_count']==0 and receipt['accepted_body_count']==0
    assert original['publish'] is False and receipt['publish'] is False
    article=queue.db.execute("SELECT * FROM articles WHERE url='https://news.cnyes.com/news/id/1'").fetchone()
    assert article['title']=='' and article['state']=='pending' and article['body_sha256'] is None
    assert queue.db.execute('SELECT 1 FROM discovery WHERE url=?',(index.request_url(queue.registry.sources['cnyes'],DAY,2),)).fetchone()


def test_only_metadata_ABI_changes_and_previous_fingerprint_is_derived_from_same_review(cfg,tmp_path):
    registry=PolicyRegistry(cfg,base=tmp_path);source=registry.sources['cnyes']
    original={'version':1,'config':cfg['providers'][0]['cnyes_public_index'],
        'authorization':source.authorization,
        'purpose':'private_public_index_metadata_not_article_or_training_acceptance'}
    old=hashlib.sha256(json.dumps(original,sort_keys=True).encode()).hexdigest()
    assert index.INDEX_DATA_CONTRACT_VERSION==2
    assert index.compatible_fingerprints(source)==(source.cnyes_index_data_fingerprint,old)
    assert source.cnyes_index_data_fingerprint!=old
    without=deepcopy(cfg);without['providers'][0].pop('cnyes_public_index')
    bare=PolicyRegistry(without,base=tmp_path).sources['cnyes']
    assert source.data_fingerprint==bare.data_fingerprint
    assert source.authorization_fingerprint==bare.authorization_fingerprint
    assert cnyes_bulk_fingerprint(source)==cnyes_bulk_fingerprint(bare)
    assert index.compatible_fingerprints(replace(source,cnyes_index_data_fingerprint='f'*64))==('f'*64,)


def test_compatible_older_heads_keep_original_fingerprint_clocks_and_due_without_re_fetching(queue):
    task,received=legacy_head(queue)
    source=replace(queue.registry.sources['cnyes'],cnyes_apis=(),feeds=(),sitemaps=(),index_pages=())
    queue.registry.sources['cnyes']=source
    old=queue.db.execute('SELECT * FROM cnyes_public_index_heads').fetchone()
    progress=index.progress(queue.db,source)
    assert progress['verified_pages']==1 and progress['metadata_rows']==2
    for _ in range(3):seed_discovery(queue)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(task['url'],)).fetchone())==task
    assert tuple(queue.db.execute('SELECT * FROM cnyes_public_index_heads').fetchone())==tuple(old)
    assert not queue.metrics.snapshot()['providers'].get('cnyes',{}).get('counts',{}).get('public_index_metadata_legacy_retagged',0)


def test_new_and_compatible_previous_head_for_same_page_are_counted_once(queue):
    legacy_head(queue)
    accept(queue,index_payload())
    source=queue.registry.sources['cnyes'];result=index.progress(queue.db,source)
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0]==2
    assert result['verified_pages']==1 and result['metadata_rows']==2
    assert result['public_ID_row_observations']==2
    assert result['index_data_contract_version']==2


def test_legacy_head_cannot_cross_authorization_or_arbitrary_parser_contract(queue):
    legacy_head(queue);source=queue.registry.sources['cnyes']
    assert index.progress(queue.db,replace(source,authorization_fingerprint='a'*64))['verified_pages']==0
    assert index.progress(queue.db,replace(source,cnyes_index_data_fingerprint='b'*64))['verified_pages']==0


def test_conditional_request_keeps_exact_legacy_evidence_identity_and_sends_one_request(queue):
    task,received=legacy_head(queue);task['etag']='original-etag'
    source=queue.registry.sources['cnyes'];calls=[]
    def get(url,**kwargs):
        calls.append((url,kwargs));return replace(received,status=304,body=b'')
    http=type('HTTP',(),{'registry':queue.registry,'get':staticmethod(get),'metrics':queue.metrics})()
    reply,links,_,_=fetch_discovery(http,task)
    assert len(calls)==1 and calls[0][1]['headers']=={'If-None-Match':'original-etag'}
    assert index.accept_index(queue,task,reply,links,source)==0
    assert queue.db.execute('SELECT parser_fingerprint FROM discovery WHERE url=?',(task['url'],)).fetchone()[0]==task['parser_fingerprint']
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0]==1


@pytest.mark.parametrize('error',['robots_disallowed','discovery_http_429','discovery_http_403',
    'transport_error','http_server_error','cnyes_index_unreviewed_metadata_fields'])
def test_compatible_metadata_version_never_bypasses_prior_global_circuit_or_source_due(queue,error):
    source=queue.registry.sources['cnyes'];queue.registry.sources['cnyes']=legacy(source)
    bad=register(queue);neighbor=register(queue,page=2)
    fail_discovery(queue,bad,NewsError(error,retry=True,delay=3600))
    old=dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    before=dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(bad['url'],)).fetchone())
    queue.registry.sources['cnyes']=source;index.initialize(queue)
    assert queue.discovery_candidates(source,20)==[]
    assert dict(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())==old
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(bad['url'],)).fetchone())==before


def test_original_proved_request_scope_survives_read_compatibility_without_globalizing_or_erasing(queue):
    source=queue.registry.sources['cnyes'];queue.registry.sources['cnyes']=legacy(source)
    bad=register(queue);neighbor=register(queue,page=2)
    fail_discovery(queue,bad,NewsError('cnyes_index_unverified_metadata',retry=True,delay=3600))
    old_scopes=dict(queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'cnyes_index_request_failure_scope_v1:%'"))
    old_job=dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(bad['url'],)).fetchone())
    queue.registry.sources['cnyes']=source;index.initialize(queue)
    selected=[row['url'] for row in queue.discovery_candidates(source,20)]
    assert selected==[neighbor['url']]
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(bad['url'],)).fetchone())==old_job
    now=dict(queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'cnyes_index_request_failure_scope_v1:%'"))
    assert all(now[k]==v for k,v in old_scopes.items())
    assert not queue.db.execute('SELECT 1 FROM cnyes_public_index_heads').fetchone()
