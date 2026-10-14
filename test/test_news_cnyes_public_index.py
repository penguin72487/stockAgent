"""Public all-index metadata never loosens the article/category contracts."""
from copy import deepcopy
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest
import pyarrow.parquet as pq

from downloader.news_fulltext import accept_discovery, fail_discovery, fetch_discovery
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_scope, cnyes_url
from downloader.news_fulltext_cnyes_index import (
    ENDPOINT, MAXIMUM_PAGE, parse_index, progress, request_url, seed_gap_dates, validate_request,
)
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE, cfg, payload, registry_http, response


DAY = 16323  # Exact observed 2014-09-10 source day, not a synthetic release.


@pytest.fixture
def queue(cfg, tmp_path):
    registry, _ = registry_http(cfg,tmp_path)
    owner = NewsQueue(tmp_path,registry)
    try:
        yield owner
    finally:
        owner.close()


def index_payload(ids=(1,2), *, page=1, total=None):
    data = payload(ids=ids,page=page,total=total)
    for row in data['items']['data']:
        row.pop('content')
        row.pop('source')
        row.update(categoryId=830,publishAt=DAY*86400-28800+42,summary='這是摘要，不是全文。')
    return data


def register(queue, page=1, day=DAY):
    source = queue.registry.sources['cnyes']
    url = request_url(source,day,page)
    with queue.db:
        queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'index','cnyes')",(url,))
    return dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(url,)).fetchone())


def accept(queue, data, page=1, day=DAY):
    task = register(queue,page,day)
    source = queue.registry.sources['cnyes']
    r = response(data,task['url'])
    links = parse_index(r,source)[2]
    return task,r,accept_discovery(queue,task,r,links,[],[])


def test_metadata_registration_leaves_all_previous_capture_and_bulk_ABIs_exact(cfg,tmp_path):
    old = deepcopy(cfg)
    old['providers'][0].pop('cnyes_public_index')
    old_source = PolicyRegistry(old,base=tmp_path).sources['cnyes']
    source = PolicyRegistry(cfg,base=tmp_path).sources['cnyes']
    assert source.data_fingerprint == old_source.data_fingerprint
    assert source.authorization_fingerprint == old_source.authorization_fingerprint
    assert cnyes_bulk_fingerprint(source) == cnyes_bulk_fingerprint(old_source)
    assert source.cnyes_index_api == ENDPOINT and source.cnyes_index_data_fingerprint
    assert not old_source.cnyes_index_api


def test_public_index_cannot_leak_to_a_different_provider(cfg,tmp_path):
    cfg['providers'][0].update(id='other',domains=['example.test'],cnyes_apis=[])
    with pytest.raises(ValueError,match='unreviewed Cnyes public index'):
        PolicyRegistry(cfg,base=tmp_path)


@pytest.mark.parametrize('change', ['endpoint','navigation','page_order','scope','size'])
def test_unreviewed_index_registration_rejected(cfg,tmp_path,change):
    entry = cfg['providers'][0]['cnyes_public_index']
    if change == 'endpoint':
        entry['endpoint'] = 'https://api.cnyes.com/media/api/v2/search'
    elif change == 'navigation':
        entry['publisher_navigation']['url'] = 'https://foreign.example/client.js'
    elif change == 'page_order':
        entry['observed_pages'].reverse()
    elif change == 'scope':
        entry['seed_scope'] = 'all_world_history'
    else:
        entry['observed_page_size'] = 100
    with pytest.raises(ValueError,match='unreviewed Cnyes public index'):
        PolicyRegistry(cfg,base=tmp_path)


def test_all_metadata_page31_does_not_change_category_page30_rule(queue):
    source = queue.registry.sources['cnyes']
    data = index_payload(ids=range(901,931),page=31,total=10844)
    info,_,links = parse_index(response(data,request_url(source,DAY,31)),source)
    assert info['page'] == 31 and info['pages'] == 362 and len(links) == 30
    with pytest.raises(NewsError,match='bulk_pagination_totals_invalid'):
        cnyes_pagination(response(data,request_url(source,DAY,31)))
    with pytest.raises(ValueError,match='invalid bounded Cnyes page'):
        cnyes_url(BASE,31)


@pytest.mark.parametrize('mutation', ['limit','page','start','end','extra','duplicate','encoding','foreign'])
def test_reject_bad_source_request_before_network(queue,mutation):
    source = queue.registry.sources['cnyes']
    url = request_url(source,DAY)
    if mutation == 'limit': url = url.replace('limit=30','limit=100')
    elif mutation == 'page': url = url.replace('page=1','page='+str(MAXIMUM_PAGE+1))
    elif mutation == 'start': url = url.replace('startAt=1410278400','startAt=1410278401')
    elif mutation == 'end': url = url.replace('endAt=1410364799','endAt=1410364800')
    elif mutation == 'extra': url += '&q=hidden'
    elif mutation == 'duplicate': url += '&page=1'
    elif mutation == 'encoding': url = url.replace('page=1','page=%31')
    else: url = url.replace('api.cnyes.com','news.cnyes.com')
    with pytest.raises(NewsError,match='cnyes_index_request_unregistered'):
        validate_request(url,source)


@pytest.mark.parametrize('field,value', [
    ('payment',False),('payment',0.0),('payment',2),('publishAt',1410278400.0),
    ('publishAt',1410278399),('publishAt',1410364800),('categoryId',False),
    ('categoryId',0),('title',[]),('title',None),
])
def test_metadata_clock_permission_and_field_types_not_guessed(queue,field,value):
    data = index_payload()
    data['items']['data'][0][field] = value
    expected = 'bulk_pagination_totals_invalid' if field == 'publishAt' and type(value) is float else 'cnyes_index_unverified_metadata'
    with pytest.raises(NewsError,match=expected):
        parse_index(response(data,request_url(queue.registry.sources['cnyes'],DAY)),queue.registry.sources['cnyes'])


@pytest.mark.parametrize('field',['content','body','text','description','new_unknown_field'])
def test_unreviewed_possible_body_field_not_persisted(queue,field):
    data = index_payload()
    data['items']['data'][0][field] = '不能把未審查全文存成清單。'
    with pytest.raises(NewsError,match='cnyes_index_unreviewed_metadata_fields'):
        accept(queue,data)
    assert not list(queue.root.glob('list_metadata/**/*.parquet'))
    assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0


def test_metadata_has_zero_body_acceptance_paid_ID_excluded_and_original_receipt_readback(queue):
    data = index_payload()
    data['items']['data'][1]['payment'] = 1
    task,r,count = accept(queue,data)
    assert count == 0
    heads = queue.db.execute('SELECT url,state,body_sha256,parquet_path FROM articles').fetchall()
    assert [tuple(row) for row in heads] == [('https://news.cnyes.com/news/id/1','pending',None,None)]
    evidence = queue.db.execute('SELECT * FROM cnyes_public_index_observations').fetchone()
    original = pq.read_table(queue.root/evidence['evidence_path']).to_pylist()[0]
    assert original['raw_response_body'] == r.body and original['metadata_only']
    assert original['accepted_body_count'] == 0 and original['publish'] is False
    receipt = json.loads((queue.root/(evidence['evidence_path']+'.receipt.json')).read_bytes())
    assert receipt['accepted_body_count'] == 0 and receipt['metadata_rows'] == 2
    summary = progress(queue.db,queue.registry.sources['cnyes'])
    assert summary['verified_pages'] == 1 and summary['dates_all_reported_pages_observed'] == 1
    assert summary['article_bodies_accepted_by_index'] == 0 and not summary['all_history_complete']


def test_missing_receipt_recovered_from_exact_immutable_original_without_clock_rewrite(queue):
    task,r,_ = accept(queue,index_payload())
    evidence = queue.db.execute('SELECT evidence_path FROM cnyes_public_index_observations').fetchone()[0]
    target = queue.root/evidence
    original = target.read_bytes()
    receipt = target.with_suffix('.parquet.receipt.json')
    receipt.unlink()
    links = parse_index(r,queue.registry.sources['cnyes'])[2]
    assert accept_discovery(queue,task,r,links,[],[]) == 0
    assert target.read_bytes() == original and receipt.is_file()


def test_wrong_existing_receipt_not_overwritten(queue):
    task,r,_ = accept(queue,index_payload())
    evidence = queue.db.execute('SELECT evidence_path FROM cnyes_public_index_observations').fetchone()[0]
    receipt = queue.root/(evidence+'.receipt.json')
    data = json.loads(receipt.read_bytes())
    data['sha256'] = '0'*64
    from downloader.artifact_io import atomic_write_json
    atomic_write_json(receipt,data)
    original = receipt.read_bytes()
    with pytest.raises(NewsError,match='cnyes_index_original_receipt_unverified'):
        accept_discovery(queue,task,r,parse_index(r,queue.registry.sources['cnyes'])[2],[],[])
    assert receipt.read_bytes() == original


def test_source_metadata_cannot_reset_article_retry_or_fake_complete(queue):
    url = 'https://news.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url':url}])
        queue.db.execute("UPDATE articles SET state='retry',error_code='provider_cooldown',due=9999999999,attempts=7 WHERE url=?",(url,))
    accept(queue,index_payload(ids=(1,)))
    row = queue.db.execute('SELECT state,error_code,due,attempts,body_sha256 FROM articles WHERE url=?',(url,)).fetchone()
    assert tuple(row) == ('retry','provider_cooldown',9999999999,7,None)


def test_one_date_seed_deduplicates_categories_aliases_and_preserves_gap_rows(queue):
    source = queue.registry.sources['cnyes']
    low,high = 1410278400,1410364799
    scopes = [cnyes_scope(cnyes_url(BASE,start=low,end=high)),
              cnyes_scope(cnyes_url(BASE,start=low+12,end=high-12))]
    with queue.db:
        queue.db.executemany("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','real-source-clock')",[(scope,) for scope in scopes])
        seed_gap_dates(queue,source)
        seed_gap_dates(queue,source)
    assert queue.db.execute("SELECT count(*) FROM discovery WHERE kind='index'").fetchone()[0] == 1
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_only_source_advertised_next_page_queued(queue):
    accept(queue,index_payload(ids=range(1,31),total=61))
    urls = [row[0] for row in queue.db.execute("SELECT url FROM discovery WHERE kind='index' ORDER BY url")]
    assert len(urls) == 2 and any('page=2&' in url for url in urls)
    assert not any('page=3&' in url for url in urls)
    assert progress(queue.db,queue.registry.sources['cnyes'])['dates_all_reported_pages_observed'] == 0


def test_repeated_page_does_not_advance_or_discover_fake_bodies(queue):
    data = index_payload(ids=range(1,31),total=61)
    accept(queue,data)
    data['items']['current_page'] = 2
    with pytest.raises(NewsError,match='cnyes_index_pagination_no_progress'):
        accept(queue,data,page=2)
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0] == 1
    assert not any('page=3&' in row[0] for row in queue.db.execute('SELECT url FROM discovery'))


def test_index_circuit_independent_of_body_category_lane_and_disabled_registration(queue,cfg,tmp_path):
    task = register(queue)
    source = queue.registry.sources['cnyes']
    fail_discovery(queue,task,NewsError('cnyes_index_unverified_metadata',retry=True,delay=3600))
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?',(task['url'],))
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')",(cnyes_url(BASE),))
    candidates = queue.discovery_candidates(source,10)
    assert [row['kind'] for row in candidates] == ['cnyes_news']
    assert queue.db.execute('SELECT count(*) FROM bulk_adapter_cooldowns').fetchone()[0] == 0
    old = deepcopy(cfg)
    old['providers'][0].pop('cnyes_public_index')
    disabled = PolicyRegistry(old,base=tmp_path).sources['cnyes']
    assert all(row['url'] != task['url'] for row in queue.discovery_candidates(disabled,10))


def test_unregistered_public_index_fetch_rejected_before_HTTP(cfg,tmp_path):
    reg,_ = registry_http(cfg,tmp_path)
    enabled = reg.sources['cnyes']
    task = {'kind':'index','url':request_url(enabled,DAY),'etag':None,'last_modified':None,'parser_fingerprint':None}
    disabled = deepcopy(cfg)
    disabled['providers'][0].pop('cnyes_public_index')
    registry = PolicyRegistry(disabled,base=tmp_path)
    http = SimpleNamespace(registry=registry,get=lambda *a,**k:pytest.fail('unregistered index reached network'),metrics=NewsMetrics())
    with pytest.raises(NewsError,match='cnyes_index_request_unregistered'):
        fetch_discovery(http,task)


def test_request_response_date_scope_mismatch_cannot_be_accepted(queue):
    task = register(queue)
    r = response(index_payload(),request_url(queue.registry.sources['cnyes'],DAY+1))
    with pytest.raises(NewsError,match='cnyes_index_request_response_scope_mismatch'):
        accept_discovery(queue,task,r,[],[],[])


def test_304_without_same_scope_original_not_success(queue):
    task = register(queue)
    task['parser_fingerprint'] = queue.registry.sources['cnyes'].cnyes_index_data_fingerprint
    r = replace(response(index_payload(),task['url']),status=304,body=b'')
    with pytest.raises(NewsError,match='cnyes_index_not_modified_without_evidence'):
        accept_discovery(queue,task,r,[],[],[])
