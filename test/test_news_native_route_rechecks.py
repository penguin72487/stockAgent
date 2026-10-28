"""New route retries use the existing mailbox and immutable closed witnesses."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostics
from downloader import news_fulltext_repairs as repairs
from downloader import news_fulltext_route_rechecks as routes
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_moneyweekly import ROUTE_METADATA_LAYOUT, LEGACY_ARTICLE_PATTERN
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_moneyweekly_native_routes import DESTINATION, LEGACY, MAGAZINE, magazine_page, native_config
from test_news_reviewed_author_scope import reviewed_moneyweekly_config


def head(queue, url=LEGACY):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


@pytest.fixture
def setup(tmp_path):
    config = reviewed_moneyweekly_config()
    next(p for p in config['providers'] if p['id'] == 'moneyweekly')['article_path_pattern'] = LEGACY_ARTICLE_PATTERN
    config.update(root=str(tmp_path), exclusive_http_owner=False, min_free_disk_bytes=1)
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=Path.cwd()))
    queue.add_articles([{'url':LEGACY}, {'url':MAGAZINE}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='unsupported_article_path',error_code='unsupported_article_path',"
            'attempts=1,due=0,last_checked_at_utc=NULL')
    witnesses = {}
    for family, url in [('magazine',MAGAZINE), ('article_redirect',LEGACY)]:
        result = diagnostics.submit_candidate(tmp_path, config, queue.registry, url, ROUTE_METADATA_LAYOUT,
            reason='Once-only public route question', evidence_contract_version=64)
        diagnostics.pump_candidates(queue)
        payload, _ = magazine_page()
        response = (Response(url,200,{'content-type':'text/html'},payload) if url == MAGAZINE
            else Response(url,302,{'location':DESTINATION},b'Literal source redirect'))
        transport = SimpleNamespace(registry=queue.registry, get=lambda u, **k: response,
            provider_not_before={}, blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())
        job = diagnostics.claim_candidate(queue,transport,config,'moneyweekly')
        assert job is not None
        diagnostics.finish_candidate(queue,job,result=diagnostics.fetch_candidate(transport,job))
        row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(result['request_id'],)).fetchone())
        witnesses[family] = dict(request_id=result['request_id'],job_sha256=hashlib.sha256(repairs._bytes(row)).hexdigest())
    heads = [head(queue,u) for u in (LEGACY,MAGAZINE)]
    queue.close()
    candidate = native_config()
    candidate.update(root=str(tmp_path), exclusive_http_owner=False, min_free_disk_bytes=1)
    queue = NewsQueue(tmp_path, PolicyRegistry(candidate, base=Path.cwd()))
    assert [head(queue,u) for u in (LEGACY,MAGAZINE)] == heads
    yield queue, witnesses
    queue.close()


def submit(queue, witnesses, urls=(LEGACY,MAGAZINE)):
    return repairs.submit_repairs(queue.root,queue.registry,iter(urls),
        reason='The closed native route observations support one normal missing-body attempt',
        native_route_witnesses=witnesses)


def test_route_submission_is_private_bounded_and_does_not_change_heads_or_call_http(setup, monkeypatch):
    queue, witnesses = setup
    before = [head(queue,u) for u in (LEGACY,MAGAZINE)]
    changes = queue.db.total_changes
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a,**k: pytest.fail('no HTTP'))
    result = submit(queue,witnesses)
    assert result['submitted_targets'] == 2 and result['publisher_requests'] == 0
    assert not result['queue_mutations'] and not result['accepted_as_bodies']
    assert queue.db.total_changes == changes and [head(queue,u) for u in (LEGACY,MAGAZINE)] == before
    path = Path(result['request_path'])
    assert path.stat().st_mode & 0o777 == 0o600
    assert repairs._load(path)['contract_version'] == 9


def test_common_owner_admits_only_state_and_archives_witnesses_without_fabricating_check_time(setup):
    queue, witnesses = setup
    before = [head(queue,u) for u in (LEGACY,MAGAZINE)]
    result = submit(queue,witnesses)
    receipt = repairs.consume_repairs(queue,max_seconds=0.1)
    assert receipt['outcomes'] == {routes.OUTCOME:2}
    assert [head(queue,u) for u in (LEGACY,MAGAZINE)] == [dict(r,state='retry') for r in before]
    assert not Path(result['request_path']).exists()
    completed = json.loads((queue.root/'reviewed_repairs/processed'/(result['request_id']+'.outcome.json')).read_bytes())
    assert completed['contract_version'] == 9 and completed['witnesses'] == witnesses
    assert completed['admitted_targets'] == 2 and completed['publisher_requests'] == completed['body_acceptances'] == 0
    assert not completed['attempts_deadlines_errors_checks_and_saved_bodies_changed']
    assert repairs.consume_repairs(queue)['processed'] == 0


@pytest.mark.parametrize('changes', [
    {'body_sha256':'a'*64}, {'body_bytes':1}, {'parquet_path':'existing.parquet'},
    {'captured_at_utc':'2026-01-01T00:00:00+00:00'}, {'lease_owner':'other'},
    {'lease_until':time.time()+1000}, {'due':time.time()+1000},
    {'last_checked_at_utc':'not-a-clock'}, {'last_checked_at_utc':'2099-01-01T00:00:00+00:00'},
    {'state':'complete'}, {'error_code':'body_not_found'},
])
def test_stored_leased_future_or_other_failed_heads_are_not_requeued(setup, changes):
    queue, witnesses = setup
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in changes)+' WHERE url=?',(*changes.values(),LEGACY))
    before = head(queue)
    result = submit(queue,witnesses,(LEGACY,))
    assert not result['submitted'] and result['excluded'] and head(queue) == before


def test_head_changed_after_review_is_not_admitted(setup):
    queue, witnesses = setup
    submit(queue,witnesses,(LEGACY,))
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?',(LEGACY,))
    before = head(queue)
    assert repairs.consume_repairs(queue,max_seconds=0.1)['outcomes'] == {'head_changed_since_review':1}
    assert head(queue) == before


@pytest.mark.parametrize('field,value', [('state','uncertain_outcome'), ('owner_id','altered-owner'),
    ('result_json','{}'), ('request_json','{}')])
def test_changed_closed_witness_is_not_repeated_or_treated_as_permission(setup, field, value):
    queue, witnesses = setup
    with queue.db:
        queue.db.execute('UPDATE native_candidate_jobs SET '+field+'=? WHERE request_id=?',
            (value,witnesses['article_redirect']['request_id']))
    before = head(queue)
    result = submit(queue,witnesses,(LEGACY,))
    assert not result['submitted'] and head(queue) == before


def test_witness_changed_after_submission_blocks_admission(setup):
    queue, witnesses = setup
    submit(queue,witnesses,(LEGACY,))
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET result_json='{}' WHERE request_id=?",
            (witnesses['article_redirect']['request_id'],))
    before = head(queue)
    assert repairs.consume_repairs(queue,max_seconds=0.1)['outcomes'] == {'native_route_evidence_invalid':1}
    assert head(queue) == before


def test_an_admitted_route_cannot_be_reset_by_a_new_reason_or_restart(setup):
    queue, witnesses = setup
    before = head(queue)
    submit(queue,witnesses,(LEGACY,))
    assert repairs.consume_repairs(queue,max_seconds=0.1)['outcomes'] == {routes.OUTCOME:1}
    with queue.db:
        queue.db.execute("UPDATE articles SET state='unsupported_article_path' WHERE url=?",(LEGACY,))
    queue.owner = 'a-different-owner'
    queue.reviewed_repair_schema_ready = False
    result = submit(queue,witnesses,(LEGACY,))
    assert not result['submitted'] and result['excluded'][0]['reason'] == 'already_admitted_for_native_route'
    assert head(queue) == before


def test_default_repair_does_not_gain_access_to_unsupported_routes(setup):
    queue, _ = setup
    result = repairs.submit_repairs(queue.root,queue.registry,[LEGACY],reason='Ordinary repair remains unchanged')
    assert not result['submitted'] and result['excluded']


def test_unknown_or_unobserved_namespaces_are_not_added_to_the_queue(setup):
    queue, witnesses = setup
    count = queue.db.execute('SELECT count(*) FROM articles').fetchone()[0]
    result = submit(queue,witnesses,[LEGACY.replace('246465','9999999'),
        'https://www.moneyweekly.com.tw/ArticleData/Info/%E7%90%86%E5%91%A8%E6%96%B0%E8%81%9E/1'])
    assert not result['submitted'] and len(result['excluded']) == 2
    assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == count
