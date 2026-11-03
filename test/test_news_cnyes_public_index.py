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
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch
    prepare_dispatch(queue, task)
    task['_cnyes_revision_scope_idle'] = True  # This fixture dispatches one response at a time.
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


@pytest.mark.parametrize('crash_point', ['before_receipt', 'before_database_commit'])
def test_public_index_owner_recovers_interrupted_original_after_restart(cfg, tmp_path, monkeypatch, crash_point):
    import sqlite3
    import downloader.news_fulltext_cnyes_index as module
    from downloader.news_fulltext_cnyes_checkpoint import recover
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch

    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    try:
        task = register(owner)
        prepare_dispatch(owner, task)
        task['_cnyes_revision_scope_idle'] = True
        source = registry.sources['cnyes']
        result = response(index_payload(ids=range(1, 31), total=31), task['url'])
        links = parse_index(result, source)[2]
        with monkeypatch.context() as patch:
            if crash_point == 'before_receipt':
                patch.setattr(module, 'atomic_write_json',
                              lambda *a, **kw: (_ for _ in ()).throw(OSError('simulated receipt interruption')))
                expected = OSError
            else:
                with owner.db:
                    owner.db.execute("CREATE TEMP TRIGGER reject_index_schedule BEFORE UPDATE ON discovery "
                                     "BEGIN SELECT RAISE(ABORT,'simulated schedule interruption'); END")
                expected = sqlite3.IntegrityError
            with pytest.raises(expected):
                accept_discovery(owner, task, result, links, [], [])
        assert owner.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 0
        assert len(list(tmp_path.glob('list_metadata/cnyes/**/*.parquet'))) == 1
    finally:
        owner.close()

    restored = NewsQueue(tmp_path, registry)
    try:
        pending = progress(restored.db, source)
        assert pending['checkpoint_contract_version'] == 1
        assert pending['pending_local_originals'] == 1 and pending['local_recovery_ready']
        assert pending['next_local_recovery_at_utc'] is not None
        assert recover(restored, limit=4, max_seconds=1)['outcomes'] == ['recovered']
        assert restored.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 1
        assert restored.db.execute('SELECT count(*) FROM articles').fetchone()[0] == len(links)
        assert restored.db.execute('SELECT 1 FROM discovery WHERE url=?', (request_url(source, DAY, 2),)).fetchone()
        assert not restored.db.execute("SELECT 1 FROM cnyes_interrupted_page_checkpoints WHERE state='pending'").fetchone()
        completed = progress(restored.db, source)
        assert completed['pending_local_originals'] == 0 and not completed['local_recovery_ready']
        assert completed['next_local_recovery_at_utc'] is None
    finally:
        restored.close()


@pytest.mark.parametrize('case', ['missing_original', 'corrupt_original', 'invalid_clock', 'policy_changed', 'newer_head', 'revision_changed'])
def test_public_index_checkpoint_preserves_scope_and_newer_source(cfg, tmp_path, monkeypatch, case):
    import downloader.news_fulltext_cnyes_index as module
    from downloader.news_fulltext_cnyes_checkpoint import recover
    from downloader.news_fulltext_cnyes_index_checkpoint import defer_live_failure
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch

    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    try:
        task = register(owner)
        prepare_dispatch(owner, task)
        task['_cnyes_revision_scope_idle'] = True
        source = registry.sources['cnyes']
        result = response(index_payload(), task['url'])
        links = parse_index(result, source)[2]
        with monkeypatch.context() as patch:
            name = 'atomic_write_parquet' if case == 'missing_original' else 'atomic_write_json'
            patch.setattr(module, name, lambda *a, **kw: (_ for _ in ()).throw(OSError('interrupted')))
            with pytest.raises(OSError):
                accept_discovery(owner, task, result, links, [], [])
        from downloader.news_fulltext_http import Response
        other = Response(result.url, 200, {'content-type': 'application/json', 'etag': '"different"'}, result.body)
        assert not defer_live_failure(owner, task, other, OSError('local disk'))
        assert defer_live_failure(owner, task, result, OSError('local disk'))
        neighbor = register(owner, 2)
        clause, params = owner.discovery_dispatch_filter(source)
        assert not owner.db.execute('SELECT 1 FROM discovery WHERE url=? AND ' + clause,
                                    (task['url'], *params)).fetchone()
        assert owner.db.execute('SELECT 1 FROM discovery WHERE url=? AND ' + clause,
                                (neighbor['url'], *params)).fetchone()
        if case == 'corrupt_original':
            next(tmp_path.glob('list_metadata/cnyes/**/*.parquet')).write_bytes(b'bad parquet')
        if case == 'invalid_clock':
            row = owner.db.execute('SELECT parquet_path,witness_json FROM cnyes_interrupted_page_checkpoints').fetchone()
            witness = json.loads(row['witness_json'])
            witness['observed_at_utc'] = '9999-12-31T23:59:59-23:59'
            with owner.db:
                owner.db.execute('UPDATE cnyes_interrupted_page_checkpoints SET witness_json=? WHERE parquet_path=?',
                                 (json.dumps(witness), row['parquet_path']))
        if case == 'policy_changed':
            registry.sources['cnyes'] = replace(source, authorization_fingerprint='f' * 64)
        if case in {'newer_head', 'revision_changed'}:
            with owner.db:
                owner.add_articles([{'url': links[0][0], 'title': 'newer independent title'}])
                if case == 'newer_head':
                    owner.db.execute("UPDATE discovery SET error_code='http_429',attempts=4,due=1234567890 WHERE url=?",
                                     (task['url'],))
                else:
                    from downloader.news_fulltext_cnyes_revision import start_round
                    start_round(owner, source, cnyes_scope(task['url']), reason='synthetic_newer_round')
            before = dict(owner.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
        expected = {'missing_original': 'original_missing', 'corrupt_original': 'invalid_original', 'invalid_clock': 'invalid_original',
                    'policy_changed': 'policy_changed', 'newer_head': 'recovered', 'revision_changed': 'recovered'}[case]
        assert recover(owner, limit=4, max_seconds=1)['outcomes'] == [expected]
        if case in {'newer_head', 'revision_changed'}:
            assert dict(owner.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
            assert owner.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 1
            assert owner.db.execute('SELECT count(*) FROM cnyes_public_index_heads').fetchone()[0] == 0
            assert owner.db.execute('SELECT count(*) FROM articles').fetchone()[0] == len(links)
            assert owner.db.execute('SELECT title FROM articles WHERE url=?', (links[0][0],)).fetchone()[0] == 'newer independent title'
        else:
            assert owner.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 0
            assert owner.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
            assert owner.db.execute('SELECT state,outcome FROM cnyes_interrupted_page_checkpoints').fetchone()[:] == ('skipped', expected)
    finally:
        owner.close()


def test_public_index_live_write_failure_wakes_local_recovery_without_second_http(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext as main
    import downloader.news_fulltext_cnyes_index as module
    from test_news_fulltext import FakeResponse, http_fixture

    cfg.update(min_free_disk_bytes=1, exclusive_http_owner=False)
    registry, _ = registry_http(cfg, tmp_path)
    url = request_url(registry.sources['cnyes'], DAY)
    body = response(index_payload(), url).body
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(body, headers={'Content-Type': 'application/json'}),
    })
    owner = NewsQueue(tmp_path, http.registry)
    writer, calls = module.atomic_write_json, []
    def fail_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OSError('simulated local receipt failure')
        return writer(*args, **kwargs)
    try:
        register(owner)
        monkeypatch.setattr(main, 'seed_discovery', lambda *_: None)
        monkeypatch.setattr(module, 'atomic_write_json', fail_once)
        main.run_cycle(owner, http, cfg, max_articles=0, max_discovery_requests=1, max_run_seconds=5)
        assert owner.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 1
        assert not owner.db.execute("SELECT 1 FROM cnyes_interrupted_page_checkpoints WHERE state='pending'").fetchone()
        assert [request for request, _ in session.calls].count(url) == 1
        assert len(calls) == 2
    finally:
        http.close(); owner.close()


def test_public_index_checkpoint_survives_abrupt_process_exit(cfg, tmp_path):
    from pathlib import Path
    import subprocess
    import sys
    from downloader.news_fulltext_cnyes_checkpoint import recover

    (tmp_path / 'synthetic_config.json').write_text(json.dumps(cfg), encoding='utf-8')
    script = '''
import json, os, sys
from pathlib import Path
sys.path.insert(0, 'test')
from test_cnyes_news_bulk import registry_http, response
from test_news_cnyes_public_index import register, index_payload
from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_cnyes_index import parse_index
from downloader.news_fulltext_cnyes_revision import prepare_dispatch
from downloader.news_fulltext_queue import NewsQueue
import downloader.news_fulltext_cnyes_index as module
root = Path(sys.argv[1])
config = json.loads((root / 'synthetic_config.json').read_text())
registry, _ = registry_http(config, root)
owner = NewsQueue(root, registry)
task = register(owner)
prepare_dispatch(owner, task)
task['_cnyes_revision_scope_idle'] = True
result = response(index_payload(), task['url'])
links = parse_index(result, registry.sources['cnyes'])[2]
module.atomic_write_json = lambda *args, **kwargs: os._exit(73)
accept_discovery(owner, task, result, links, [], [])
os._exit(74)
'''
    child = subprocess.run([sys.executable, '-c', script, str(tmp_path)], cwd=Path.cwd(), timeout=30,
                           capture_output=True, text=True, check=False)
    assert child.returncode == 73, child.stderr
    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    try:
        assert recover(owner, limit=4, max_seconds=1)['outcomes'] == ['recovered']
        assert owner.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 1
        assert owner.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
    finally:
        owner.close()


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


@pytest.mark.parametrize('damage', ['missing_original', 'missing_receipt', 'corrupt_original',
                                    'receipt_rows', 'observation_rows'])
def test_304_requires_verified_index_original_and_receipt(queue, damage):
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch

    accept(queue, index_payload())
    task = register(queue)
    prepare_dispatch(queue, task)
    observation = dict(queue.db.execute('SELECT * FROM cnyes_public_index_observations').fetchone())
    original = queue.root / observation['evidence_path']
    receipt = original.with_suffix('.parquet.receipt.json')
    if damage == 'missing_original':
        original.unlink()
    elif damage == 'missing_receipt':
        receipt.unlink()
    elif damage == 'corrupt_original':
        original.write_bytes(b'corrupt isolated fixture')
    elif damage == 'receipt_rows':
        value = json.loads(receipt.read_bytes())
        value['metadata_rows'] += 1
        receipt.write_text(json.dumps(value))
    else:
        with queue.db:
            queue.db.execute('UPDATE cnyes_public_index_observations SET metadata_rows=metadata_rows+1')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    head = tuple(queue.db.execute('SELECT * FROM cnyes_public_index_heads').fetchone())
    r = replace(response(index_payload(), task['url']), status=304, body=b'')
    with pytest.raises(NewsError, match='cnyes_index_not_modified_without_evidence'):
        accept_discovery(queue, task, r, [], [], [])
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before
    assert tuple(queue.db.execute('SELECT * FROM cnyes_public_index_heads').fetchone()) == head


@pytest.mark.parametrize('damage', ['invalid_clock', 'missing_original'])
def test_200_cannot_seal_invalid_or_replace_missing_index_evidence(queue, damage):
    from downloader.artifact_io import atomic_write_parquet, table_from_records
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch

    _, raw, _ = accept(queue, index_payload())
    task = register(queue)
    prepare_dispatch(queue, task)
    observation = dict(queue.db.execute('SELECT * FROM cnyes_public_index_observations').fetchone())
    original = queue.root / observation['evidence_path']
    receipt = original.with_suffix('.parquet.receipt.json')
    if damage == 'invalid_clock':
        row = pq.read_table(original).to_pylist()[0]
        row['source_observed_at_utc'] = 'not-a-time'
        receipt.unlink()
        atomic_write_parquet(original, table_from_records([row]), durable=True)
        evidence_before = original.read_bytes()
    else:
        original.unlink()
        evidence_before = receipt.read_bytes()
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    with pytest.raises(NewsError, match='cnyes_index_original_evidence_unverified'):
        accept_discovery(queue, task, raw, parse_index(raw, queue.registry.sources['cnyes'])[2], [], [])
    if damage == 'invalid_clock':
        assert not receipt.exists() and original.read_bytes() == evidence_before
    else:
        assert not original.exists() and receipt.read_bytes() == evidence_before
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()) == before


def test_missing_index_receipt_recovers_unconditionally_without_blocking_other_pages(queue):
    from downloader.news_fulltext import _discovery_head_fields
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch

    _, raw, _ = accept(queue, index_payload())
    task = register(queue)
    other = register(queue, day=DAY + 1)
    with queue.db:
        queue.db.execute("UPDATE discovery SET etag='old-tag',last_modified='old-clock' WHERE url=?", (task['url'],))
    task = register(queue)
    prepare_dispatch(queue, task)
    evidence = queue.db.execute('SELECT evidence_path FROM cnyes_public_index_observations').fetchone()[0]
    original = queue.root / evidence
    original_bytes = original.read_bytes()
    receipt = original.with_suffix('.parquet.receipt.json')
    receipt.unlink()
    with pytest.raises(NewsError, match='cnyes_index_not_modified_without_evidence') as caught:
        accept_discovery(queue, task, replace(raw, status=304, body=b''), [], [], [])
    fail_discovery(queue, task, caught.value, expected_head_fields=_discovery_head_fields(task))
    retry = register(queue)
    assert retry['etag'] is None and retry['last_modified'] is None
    assert queue.db.execute('SELECT count(*) FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=?',
                            ('cnyes', ENDPOINT)).fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (other['url'],)).fetchone()) == other
    prepare_dispatch(queue, retry)
    calls = []
    def fake_get(url, *, headers):
        calls.append((url, headers))
        return raw
    http = SimpleNamespace(registry=queue.registry, get=fake_get, metrics=NewsMetrics())
    result = fetch_discovery(http, retry)
    assert calls == [(retry['url'], {})]  # Fake HTTP; no publisher request.
    accept_discovery(queue, retry, *result)
    assert receipt.exists() and original.read_bytes() == original_bytes
    assert register(queue)['error_code'] is None


def test_local_index_evidence_failure_does_not_relax_existing_source_cooldown(queue):
    import time
    from downloader.news_fulltext import _discovery_head_fields

    task = register(queue)
    source = queue.registry.sources['cnyes']
    deadline = time.time() + 3600
    with queue.db:
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                         ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint,
                          deadline, 'http_429', task['url']))
    before = tuple(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    fail_discovery(queue, task, NewsError('cnyes_index_not_modified_without_evidence', retry=True, delay=60),
                   expected_head_fields=_discovery_head_fields(task))
    assert tuple(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone()) == before
