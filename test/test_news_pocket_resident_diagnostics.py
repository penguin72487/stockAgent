"""One current-source question shares owner/pacing, never a parser upgrade."""
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
import time

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_candidates import candidate_response_metadata, native_candidate_source
from downloader.news_fulltext_http import Response, RedirectHop
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_pocket import pocket_identity_metadata, pocket_clock_metadata
from downloader.news_fulltext_queue import NewsQueue
from test_news_pocket_original import settings, page, versioned, URL


LAYOUT = 'pocket_visible_article_v3'


@pytest.fixture
def config(settings, tmp_path):
    result = versioned(settings, 3)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, workers=2, exclusive_http_owner=False)
    result['adaptive_rate'] = {'enabled': False}
    return result


@pytest.fixture
def queue(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    queue.add_articles([{'url': URL}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified',attempts=2,due=0,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (URL,))
    yield queue
    queue.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def submit(queue, config, **changes):
    arguments = {'reason': 'Current native boundary only; no body or candidate acceptance',
                 'evidence_contract_version': 3}
    arguments.update(changes)
    return diagnostic.submit_candidate(queue.root, config, queue.registry, URL, LAYOUT, **arguments)


def http(queue, *, body=None, redirect=False):
    calls = []
    def get(url):
        calls.append(url)
        hops = (RedirectHop(url, 308, url, url, '2026-01-01T00:00:00Z', 'a'*64, 22),) if redirect else ()
        return Response(url, 200, {'content-type': 'text/html'},
            body or page().replace('第2段', '<font>第2段</font>').encode(), redirects=hops)
    return SimpleNamespace(registry=queue.registry, get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def test_current_source_evidence_has_no_fictional_candidate_or_extra_parse(config, queue, monkeypatch):
    import downloader.news_fulltext_candidates as candidates
    candidate = native_candidate_source(config, queue.registry, queue.registry.base, LAYOUT)
    assert candidate is queue.registry.sources['pocket']
    monkeypatch.setattr(candidates, 'compare_native_candidate',
        lambda *a, **k: pytest.fail('do not reparse the same source as its own candidate'))
    client = http(queue)
    facts = candidate_response_metadata(client.get(URL), candidate, candidate, evidence_contract_version=3)
    assert facts['question'] == 'pocket_native_identity_boundary_v1' and facts['current_source_not_candidate']
    assert facts['current_error'] == 'article_identity_not_verified'
    assert facts['pocket_native_validation']['body_boundary']['tag_counts']['font'] == 1
    assert facts['pocket_native_validation']['native_credit']['known_organisation_visible']
    assert 'candidate_parser' not in facts and not facts['accepted_as_article']
    assert not facts['raw_or_names_or_body_retained'] and client.calls == [URL]
    assert '第2段' not in json.dumps(facts, ensure_ascii=False)


def test_native_shape_records_schema_agreement_without_exporting_the_title():
    facts = pocket_identity_metadata(BeautifulSoup(page(), 'lxml'), page_url=URL)
    schema = facts['clock_shapes']
    assert facts['heading_schema_headline_hash_agrees'] and schema['schema_mainEntity_agrees']
    assert schema['schema_publisher_is_known_Organization'] and not schema['declared_articleBody_supplied']
    assert schema['schema_headline_sha256'] == facts['page_heading_sha256'][0]
    assert '合成資料的原生研究' not in json.dumps(facts, ensure_ascii=False)
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('main h1').string = 'different heading'
    assert not pocket_identity_metadata(soup, page_url=URL)['heading_schema_headline_hash_agrees']


def test_canonical_question_cannot_submit_a_report_alias(config, queue):
    alias = URL.replace('/articles/', '/report/SCHOOL/')
    queue.add_articles([{'url': alias}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified',due=0 WHERE url=?", (alias,))
    with pytest.raises(ValueError, match='literal_current_pocket_canonical_route_required'):
        diagnostic.submit_candidate(queue.root, config, queue.registry, alias, LAYOUT,
            reason='Not a canonical sample', evidence_contract_version=3)
    assert not (queue.root/'reviewed_repairs').exists()


def test_owner_reserves_once_and_finishes_metadata_without_mutating_head(config, queue):
    before = head(queue)
    submitted = submit(queue, config)
    assert submitted['submitted'] and head(queue) == before
    diagnostic.pump_candidates(queue)
    client = http(queue)
    job = diagnostic.claim_candidate(queue, client, config, 'pocket')
    assert job['evidence_contract_version'] == 3 and head(queue) == before
    assert diagnostic.claim_candidate(queue, client, config, 'pocket') is None
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(client, job))
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
    assert head(queue) == before and client.calls == [URL]
    assert not submit(queue, config, reason='new reason cannot buy another request')['submitted']
    assert diagnostic.claim_candidate(queue, client, config, 'pocket') is None
    assert not (queue.root/'bodies').exists()


@pytest.mark.parametrize('version', [1, 2, 4, True, '3'])
def test_wrong_question_version_rejects_before_mailbox_or_http(config, queue, version):
    before = head(queue)
    with pytest.raises(ValueError, match='reviewed_candidate_evidence_contract_required'):
        submit(queue, config, evidence_contract_version=version)
    assert head(queue) == before and not (queue.root/'reviewed_repairs').exists()


@pytest.mark.parametrize('state', ['complete', 'http_403', 'http_429', 'robots_denied', 'body_not_found',
    'invalid_bulk_publication_clock', 'article_attribution_not_verified', 'article_rights_or_scope_excluded'])
def test_only_actual_missing_identity_failures_may_submit(config, queue, state):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
    before = head(queue)
    with pytest.raises((NewsError, ValueError)):
        submit(queue, config)
    assert head(queue) == before and not (queue.root/'reviewed_repairs').exists()


@pytest.mark.parametrize('change', ['source', 'head', 'origin_pause', 'provider_pause'])
def test_owner_rechecks_eligibility_and_source_controls(config, queue, change):
    submit(queue, config)
    diagnostic.pump_candidates(queue)
    client = http(queue)
    if change == 'source':
        queue.registry.sources['pocket'] = replace(queue.registry.sources['pocket'], authorization_fingerprint='f'*64)
    elif change == 'head':
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    elif change == 'origin_pause':
        client.blocked_robots_origins = lambda p: ('https://www.pocket.tw',)
    else:
        client.provider_not_before['pocket'] = time.time()+3600
    before = head(queue)
    assert diagnostic.claim_candidate(queue, client, config, 'pocket') is None
    assert head(queue) == before and client.calls == []


def test_unknown_outcome_is_never_replayed(config, queue):
    submitted = submit(queue, config)
    diagnostic.pump_candidates(queue)
    assert diagnostic.claim_candidate(queue, http(queue), config, 'pocket')
    before = head(queue)
    queue.close()
    restarted = NewsQueue(queue.root, queue.registry)
    try:
        diagnostic.pump_candidates(restarted)
        assert diagnostic.candidate_status(restarted.root, submitted['request_id'])['state'] == 'uncertain_outcome'
        assert diagnostic.claim_candidate(restarted, http(restarted), config, 'pocket') is None
        assert not submit(restarted, config)['submitted'] and head(restarted) == before
    finally:
        restarted.close()


def test_diagnostic_does_not_admit_even_a_source_redirect(config, queue):
    submit(queue, config)
    diagnostic.pump_candidates(queue)
    client = http(queue, redirect=True)
    with pytest.raises(NewsError, match='candidate_literal_response_not_verified'):
        diagnostic.fetch_candidate(client, diagnostic.claim_candidate(queue, client, config, 'pocket'))
    assert client.calls == [URL]


def test_request_version_cannot_be_downgraded_and_rehashed(config, queue):
    submitted = submit(queue, config)
    path = queue.root/'reviewed_repairs/candidate_diagnostics/pending'/(submitted['request_id']+'.json')
    request = json.loads(path.read_bytes())
    request['contract_version'] = 1
    request.pop('evidence_contract_version')
    payload = diagnostic._bytes(request)
    target = path.with_name(hashlib.sha256(payload).hexdigest()+'.json')
    atomic_write_bytes(target, payload, mode=0o600)
    with pytest.raises(ValueError, match='invalid_native_candidate_target'):
        diagnostic._load(target)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_finite_native_metadata_does_not_leak_text_or_claim_a_projected_container(engine):
    raw = page().replace('合成資料', 'PRIVATE TITLE').replace('第2段', 'PRIVATE BODY')
    facts = pocket_identity_metadata(BeautifulSoup(raw, engine), page_url=URL)
    assert facts['native_scope_count'] == facts['native_scope_under_heading_parent_count'] == 1
    assert facts['native_credit']['known_organisation_visible']
    assert facts['body_boundary']['text_characters'] > 0 and facts['body_boundary']['tag_counts']['table'] == 1
    assert not facts['accepted_as_article'] and not facts['raw_names_or_body_retained']
    text = json.dumps(facts, ensure_ascii=False)
    assert 'PRIVATE' not in text and '保留負值' not in text


@pytest.mark.parametrize('label', ['市場分析師', '更新於市場分析師', '市場市場分析分析市場'])
def test_clock_metadata_requires_a_whole_date_grammar_not_a_character_allowlist(label):
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('.article-meta').string = label
    shape = pocket_clock_metadata(soup, page_url=URL)['inner_update_labels'][0]
    assert 'literal_clock_or_known_label' not in shape and shape['sha256']
