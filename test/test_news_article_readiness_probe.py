"""Conservative empty-work pruning, not a new dispatch or source contract."""
from copy import deepcopy
from contextlib import contextmanager
import math
import sqlite3

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, ARTICLE_READY_PROBE_SQL
from test_news_fulltext import config


@contextmanager
def queue_fixture(config, tmp_path):
    queue=NewsQueue(tmp_path, PolicyRegistry(config,base=tmp_path))
    try:
        yield queue
    finally:
        queue.close()


def test_empty_selection_uses_one_existing_covering_query(config,tmp_path):
    with queue_fixture(config,tmp_path) as q:
        statements=[]
        q.db.set_trace_callback(statements.append)
        assert q.claim('publisher',300) is None
        q.db.set_trace_callback(None)
        assert len(statements)==1 and 'INDEXED BY dispatch WHERE' in statements[0]
        plan=q.db.execute('EXPLAIN QUERY PLAN '+ARTICLE_READY_PROBE_SQL,('publisher',1000)).fetchall()
        assert any('COVERING INDEX dispatch' in row[3] for row in plan)
        assert not any('TEMP B-TREE' in row[3] for row in plan)
        counts=q.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_no_due_work_selections']==1
        assert counts.get('frontier_attempts_started',0)==0


@pytest.mark.parametrize('state',['pending','retry','retry_cooldown'])
@pytest.mark.parametrize('position',[-1,0,1])
def test_exact_float_deadline_boundary_is_not_rounded_or_cached(config,tmp_path,monkeypatch,state,position):
    now=1000.0
    deadline=math.nextafter(now,-math.inf if position<0 else math.inf) if position else now
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time',lambda:now)
    with queue_fixture(config,tmp_path) as q:
        q.add_articles([{'url':'https://publisher.test/source','date':'2020-01-01'}])
        with q.db:
            q.db.execute('UPDATE articles SET state=?,due=?',(state,deadline))
        row=q.claim('publisher',300)
        assert (row is not None)==(position<=0)
        if position>0:
            monkeypatch.setattr('downloader.news_fulltext_queue.time.time',lambda:deadline)
            assert q.claim('publisher',300)['due']==deadline


@pytest.mark.parametrize('state',['complete','inflight','waiting_cohort','awaiting_authorization',
                                  'source_not_found','article_rights_or_scope_excluded'])
def test_non_dispatch_states_do_not_become_ready(config,tmp_path,state):
    with queue_fixture(config,tmp_path) as q:
        q.add_articles([{'url':'https://publisher.test/source'}])
        with q.db:
            q.db.execute('UPDATE articles SET state=?,due=0,lease_owner=?,lease_until=?',
                         (state,'other-worker',1e20))
        assert q.claim('publisher',300) is None
        assert q.db.execute('SELECT state,due,lease_owner,lease_until,attempts FROM articles').fetchone()[:]==(
            state,0,'other-worker',1e20,0)


def test_negative_probe_does_not_delay_inserts_or_consume_fairness_credit(config,tmp_path):
    with queue_fixture(config,tmp_path) as q:
        attributes=('_missing_body_dispatch_bursts','_dated_body_dispatch_bursts',
                    '_pending_body_dispatch_bursts','_stored_retry_dispatch_bursts')
        for name in attributes:setattr(q,name,{'publisher':8})
        q._cnyes_primary_body_burst=8
        assert q.claim('publisher',300) is None
        assert all(getattr(q,name)=={'publisher':8} for name in attributes)
        assert q._cnyes_primary_body_burst==8
        q.add_articles([{'url':'https://publisher.test/first'}])
        assert q.claim('publisher',300)['url'].endswith('/first')
        assert q.claim('publisher',300) is None
        q.add_articles([{'url':'https://publisher.test/next'}])
        assert q.claim('publisher',300)['url'].endswith('/next')


def test_rollback_is_visible_and_no_positive_or_negative_cache_survives(config,tmp_path):
    with queue_fixture(config,tmp_path) as q:
        q.add_articles([{'url':'https://publisher.test/source'}])
        q.db.execute('SAVEPOINT test_rollback')
        q.db.execute('UPDATE articles SET due=1e20')
        assert q.claim('publisher',300) is None
        # claim's normal context commits; use a local context adapter solely
        # to exercise a rollback inside the same source transaction below.
        q.db.execute('UPDATE articles SET due=0')
        q.db.commit()
        raw=q.db
        class TransactionDB:
            def execute(self,*args,**kwargs):return raw.execute(*args,**kwargs)
            def __enter__(self):return self
            def __exit__(self,*args):return False
        q.db=TransactionDB()
        try:
            raw.execute('SAVEPOINT source_transaction')
            raw.execute('UPDATE articles SET due=1e20')
            assert q.claim('publisher',300) is None
            raw.execute('ROLLBACK TO source_transaction')
            raw.execute('RELEASE source_transaction')
            assert q.claim('publisher',300)['url'].endswith('/source')
        finally:
            q.db=raw


def test_positive_probe_never_bypasses_blocked_hosts(config,tmp_path):
    with queue_fixture(config,tmp_path) as q:
        q.add_articles([{'url':'https://publisher.test/source'}])
        assert q.claim('publisher',300,excluded_hosts=('publisher.test',)) is None
        assert q.db.execute('SELECT state,attempts,lease_owner FROM articles').fetchone()[:]==('pending',0,None)
        assert q.claim('publisher',300)['url'].endswith('/source')


@pytest.mark.parametrize('bursts',[(1,1,1,1),(8,8,8,8),(3,5,2,4)])
def test_full_peer_fairness_and_lease_sequence_matches_ungated_selector(config,tmp_path,monkeypatch,bursts):
    """Independent oracle bypasses only the optimization, not any old lane."""
    config['providers'].append({**deepcopy(config['providers'][0]),'id':'peer','domains':['peer.test']})
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time',lambda:2000.0)
    monkeypatch.setattr('downloader.news_fulltext_queue.utc_now',lambda:'2026-10-14T00:00:00+00:00')
    with queue_fixture(config,tmp_path/'gated') as gated, queue_fixture(config,tmp_path/'oracle') as oracle:
        for q in (gated,oracle):
            rows=[{'url':f'https://{host}/{i}','date':f'2020-01-{i%28+1:02d}' if i%7 else ''}
                  for host in ('publisher.test','peer.test') for i in range(64)]
            q.add_articles(rows)
            with q.db:
                q.db.execute("UPDATE articles SET state='retry',error_code='transport_error' WHERE url LIKE '%/1%' ")
                q.db.execute("UPDATE articles SET state='retry_cooldown',due=2001 WHERE url LIKE '%/2%'")
                q.db.execute("UPDATE articles SET body_sha256=?,body_bytes=200 WHERE url LIKE '%/3%'",('a'*64,))
                q.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=1e20 WHERE url LIKE '%/4%'")
        raw=oracle.db
        class AlwaysPossibleCursor:
            def fetchone(self):return (1,)
        class OracleDB:
            def execute(self,sql,*args,**kwargs):
                return AlwaysPossibleCursor() if sql==ARTICLE_READY_PROBE_SQL else raw.execute(sql,*args,**kwargs)
            def __enter__(self):raw.__enter__();return self
            def __exit__(self,*args):return raw.__exit__(*args)
        oracle.db=OracleDB()
        options=dict(zip(('missing_body_burst','dated_body_burst','pending_body_burst','stored_body_retry_burst'),bursts))
        try:
            for i in range(160):
                provider='peer' if i%2 else 'publisher'
                assert gated.claim(provider,300,**options)==oracle.claim(provider,300,**options)
            fields='url,state,due,attempts,lease_until,body_sha256,metadata_order_day,error_code'
            actual=[tuple(r) for r in gated.db.execute('SELECT '+fields+' FROM articles ORDER BY url')]
            expected=[tuple(r) for r in raw.execute('SELECT '+fields+' FROM articles ORDER BY url')]
            assert actual==expected
        finally:
            oracle.db=raw


def test_legacy_nullable_due_remains_not_ready():
    with sqlite3.connect(':memory:') as db:
        db.execute('CREATE TABLE articles(provider TEXT,state TEXT,due REAL,metadata_date_raw TEXT)')
        db.execute('CREATE INDEX dispatch ON articles(provider,state,due,metadata_date_raw DESC)')
        db.execute("INSERT INTO articles VALUES ('publisher','pending',NULL,'')")
        assert db.execute(ARTICLE_READY_PROBE_SQL,('publisher',1000)).fetchone() is None
