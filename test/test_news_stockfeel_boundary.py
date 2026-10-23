"""Unanswered native declarations use one owner, not another download path."""
from dataclasses import replace
import hashlib
import json
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import (
    candidate_evidence_allowed, candidate_response_metadata, native_candidate_source,
)
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_stockfeel import STOCKFEEL_BOUNDARY_QUESTION
from test_news_stockfeel_original import AUTHOR, CANONICAL, PROFILE, TITLE, URL, html, settings
from test_news_candidate_diagnostics import http_stub
from test_news_fulltext import config


LAYOUT = 'stockfeel_visible_original_v1'


def inspect(settings, html, **changes):
    source = PolicyRegistry(settings, base='.').sources['stockfeel']
    response = Response(URL, 200, {'content-type': 'text/html'}, html.encode())
    return candidate_response_metadata(replace(response, **changes), source, source,
        evidence_contract_version=14, requested_url=URL)


def test_native_metadata_is_bounded_not_an_article_or_new_scope(settings, html):
    result = inspect(settings, html)
    native = result['stockfeel_native_boundary']
    assert result['question'] == STOCKFEEL_BOUNDARY_QUESTION and result['current_native_error'] is None
    assert not result['accepted_as_article'] and not result['body_completeness_verified']
    assert not result['raw_response_retained'] and not result['body_text_retained']
    assert native['native_original_region_count_within_bound'] == 1
    assert native['written_tag_counts']['table'] == 1 and native['credit_count_within_bound'] == 2
    assert all(row['recognized_public_group'] == 'stockfeel_news_team' and row['initial_visibility_hidden']
        for row in native['credits'])
    assert native['copyright_exact_known_declaration'] and native['additional_requests'] == 0
    assert native['source_clocks']['published_time']['declarations'][0]['seconds_and_offset_raw'] == '2026-10-15T07:07:51+00:00'
    serialized = json.dumps(result, ensure_ascii=False)
    assert TITLE not in serialized and AUTHOR not in serialized and '原始合成段落' not in serialized and '-1,234.50' not in serialized
    assert len(serialized.encode()) < 24576


def test_canonical_difference_is_explicit_source_metadata_not_a_body_or_accepted_alias(settings, html):
    changed_url = 'https://www.stockfeel.com.tw/synthetic-current-slug/'
    result = inspect(settings, html.replace(CANONICAL, changed_url))
    assert result['current_native_error'] == 'article_identity_not_verified'
    native = result['stockfeel_native_boundary']
    assert native['contract_version'] == 2
    identity = native['url_identity']
    assert identity['contract_version'] == 1 and identity['declaration_is_not_a_verified_alias_or_body_reference']
    assert not identity['additional_URLs_enqueued_or_requested'] and not result['identity_or_alias_accepted']
    for field in ('canonical', 'open_graph'):
        declaration, = identity[field]['declarations']
        assert identity[field]['node_count'] == 1 and not identity[field]['truncated']
        assert declaration['literal_same_origin_article_url'] == changed_url
        assert declaration['sha256'] == hashlib.sha256(changed_url.encode()).hexdigest()
        assert not declaration['matches_requested_page_key']
    assert not result['raw_response_retained'] and not result['accepted_as_article']


@pytest.mark.parametrize('value', ['https://secret.invalid/?key=never-copy',
    'https://www.stockfeel.com.tw/private/?token=never-copy',
    'https://www.stockfeel.com.tw/invalid%2fprivate/', 'never-copy'+('x'*3000)],
    ids=['foreign-origin', 'query-secret', 'escaped-separator', 'oversized'])
def test_unreviewed_canonical_declarations_are_hashes_not_new_fetch_targets(settings, html, value):
    result = inspect(settings, html.replace(CANONICAL, value))
    identity = result['stockfeel_native_boundary']['url_identity']
    for field in ('canonical', 'open_graph'):
        declaration, = identity[field]['declarations']
        assert declaration['literal_same_origin_article_url'] is None
        assert declaration['sha256'] == hashlib.sha256(value.encode()).hexdigest()
    encoded = json.dumps(result)
    assert 'never-copy' not in encoded and 'secret.invalid' not in encoded and 'token=' not in encoded
    assert not result['identity_or_alias_accepted']


def test_duplicate_and_many_canonical_tags_are_counted_but_metadata_is_bounded(settings, html):
    doc = html.replace('</head>', (f'<link rel="canonical" href="{CANONICAL}">' * 12) + '</head>')
    result = inspect(settings, doc)
    canonical = result['stockfeel_native_boundary']['url_identity']['canonical']
    assert canonical['node_count'] == 13 and len(canonical['declarations']) == 2 and canonical['truncated']
    assert result['current_native_error'] == 'article_identity_not_verified' and not result['identity_or_alias_accepted']


def test_unknown_person_label_and_profile_are_hashes_not_names(settings, html):
    unknown = 'SyntheticPrivatePersonNotForMetadata'
    changed = html.replace(AUTHOR, unknown).replace(PROFILE, 'https://example.invalid/private?secret=value')
    result = inspect(settings, changed)
    assert result['current_native_error'] == 'article_rights_or_scope_excluded'
    serialized = json.dumps(result)
    assert unknown not in serialized and 'secret=value' not in serialized and 'example.invalid' not in serialized
    assert all(row['recognized_public_group'] is None and row['literal_profile_url'] is None
        and row['sha256'] == hashlib.sha256(unknown.encode()).hexdigest()
        for row in result['stockfeel_native_boundary']['credits'])
    assert result['failure_location']['sites'][-1]['module'] == 'news_fulltext_stockfeel.py'


def test_unreviewed_native_aid_is_an_observation_not_permission(settings, html):
    result = inspect(settings, html.replace('data-aid="427"', 'data-aid="1234"'))
    assert result['current_native_error'] == 'article_rights_or_scope_excluded'
    assert all(row['data_aid'] == '1234' for row in result['stockfeel_native_boundary']['credits'])
    assert not result['stockfeel_native_boundary']['article_or_rights_acceptance_granted']


def test_only_one_dom_is_constructed_and_current_native_contract_is_not_replaced(settings, html, monkeypatch):
    from downloader import news_fulltext_policy as policy
    original, calls = policy.BeautifulSoup, []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'BeautifulSoup', counted)
    result = inspect(settings, html.replace('<table>', '<iframe src="https://example.invalid"></iframe><table>'))
    assert len(calls) == 1 and result['current_native_error'] == 'paywall_or_login'
    assert result['stockfeel_native_boundary']['unreviewed_tag_counts'] == {'iframe': 1}
    assert not result['accepted_as_article']


@pytest.mark.parametrize('title', ['Just a moment', 'Access denied', 'Verify you are human'])
def test_source_refusal_gate_precedes_native_metadata(settings, html, title):
    changed = html.replace('<title>'+TITLE+' - StockFeel 股感</title>', '<title>'+title+'</title>')
    result = inspect(settings, changed)
    assert result['current_native_error'] == 'access_challenge'
    assert 'stockfeel_native_boundary' not in result


@pytest.mark.parametrize('changes', [
    {'status': 403}, {'status': 429}, {'headers': {'content-type': 'application/json'}},
    {'url': URL+'other/'}, {'body': b'x' * ((2 << 20) + 1)},
])
def test_transport_and_bounds_are_not_a_diagnostic_bypass(settings, html, changes):
    with pytest.raises(NewsError):
        inspect(settings, html, **changes)


@pytest.mark.parametrize('version', [True, 1, 2, 3, 6, 12, 13, 16])
def test_only_fixed_evidence14_selects_stockfeel(settings, version):
    source = PolicyRegistry(settings, base='.').sources['stockfeel']
    assert not candidate_evidence_allowed(source, source, version)


@pytest.mark.parametrize('changes', [
    {'provider': 'other'}, {'allowed': False}, {'acquisition_mode': 'metadata_only'},
    {'domains': ('stockfeel.com.tw',)}, {'acquisition_mode': 'finite'}, {'known_url_only': False},
    {'embedded_article_layout': 'stockfeel_visible_original_v8'},
    {'authorization': {'kind': 'api'}},
])
def test_current_source_and_scope_cannot_be_substituted(settings, changes):
    source = PolicyRegistry(settings, base='.').sources['stockfeel']
    changed = replace(source, **changes)
    assert not candidate_evidence_allowed(source, changed, 14)
    assert not candidate_evidence_allowed(changed, changed, 14)


@pytest.fixture
def queue(settings, tmp_path):
    settings.update(root=str(tmp_path), min_free_disk_bytes=1, workers=1, exclusive_http_owner=False)
    settings['adaptive_rate'] = {'enabled': False}
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    with owner.db:
        owner.add_articles([{'url': URL}])
        owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=? WHERE url=?',
            ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded', time.time()-60,
             '2026-01-01T00:00:00+00:00', URL))
    yield owner
    owner.close()


def submit(queue, settings, **changes):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=changes.pop('reason', 'Review unanswered native credit/body boundary only'),
        evidence_contract_version=changes.pop('evidence_contract_version', 14), **changes)


def test_canonical_lifecycle_keeps_heads_budgets_and_once_key(queue, settings, html, monkeypatch):
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    budgets = list(queue.db.execute('SELECT * FROM queue_settings'))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('a diagnostic must not create a second HTTP owner'))
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'},
        html.replace('data-aid="427"', 'data-aid="1234"').encode()))
    job = diagnostic.claim_candidate(queue, http, settings, 'stockfeel')
    assert job and diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'reserved'
    result = diagnostic.fetch_candidate(http, job)
    assert result['current_native_error'] == 'article_rights_or_scope_excluded'
    diagnostic.finish_candidate(queue, job, result=result)
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    assert list(queue.db.execute('SELECT * FROM queue_settings')) == budgets
    assert not submit(queue, settings, reason='Changing reason does not rearm')['submitted']
    assert diagnostic.claim_candidate(queue, http, settings, 'stockfeel') is None and http.calls == [URL]
    key = diagnostic._key({'provider': 'stockfeel', 'id': before['id'].hex()}, 'a'*64, evidence_contract_version=14)
    assert key == diagnostic._key({'provider': 'stockfeel', 'id': before['id'].hex()}, 'b'*64, evidence_contract_version=14)
    assert not list(queue.root.glob('bodies/**/*.parquet'))


@pytest.mark.parametrize('state', ['complete', 'pending', 'paywall_or_login', 'awaiting_authorization',
    'http_403', 'http_429', 'robots_denied', 'body_not_found'])
def test_nonreviewed_states_do_not_enter_the_question(queue, settings, state):
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
    with pytest.raises(ValueError):
        submit(queue, settings)


def test_source_factory_reuses_current_source(settings, tmp_path):
    registry = PolicyRegistry(settings, base=tmp_path)
    assert native_candidate_source(settings, registry, tmp_path, LAYOUT) is registry.sources['stockfeel']


def test_fixed_key_rejects_other_provider_and_other_evidence():
    with pytest.raises(ValueError):
        diagnostic._key({'provider': 'cmnews', 'id': 'a'*64}, 'b'*64, evidence_contract_version=14)
    with pytest.raises(ValueError):
        diagnostic._key({'provider': 'stockfeel', 'id': 'a'*64}, 'b'*64, evidence_contract_version=1)


def test_reserved_unknown_outcome_is_not_repeated_after_restart(queue, settings):
    submitted = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    assert diagnostic.claim_candidate(queue, http_stub(queue), settings, 'stockfeel')
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    root, registry = queue.root, queue.registry
    queue.close()
    reopened = NewsQueue(root, registry)
    try:
        diagnostic.pump_candidates(reopened)
        assert diagnostic.candidate_status(root, submitted['request_id'])['state'] == 'uncertain_outcome'
        assert diagnostic.claim_candidate(reopened, http_stub(reopened), settings, 'stockfeel') is None
        assert not submit(reopened, settings, reason='A restart does not buy another question')['submitted']
        assert dict(reopened.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
    finally:
        reopened.close()
