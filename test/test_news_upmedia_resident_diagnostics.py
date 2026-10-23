"""Diagnose one unfinished modern head through the existing resident owner."""
from dataclasses import replace
from contextlib import closing
import json
from unittest.mock import patch

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader import news_fulltext_candidates as candidates
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_upmedia import UPMEDIA_BOUNDARY_QUESTION, UPMEDIA_HEADER_QUESTION, UPMEDIA_LAYOUT
from test_news_candidate_diagnostics import http_stub
from test_news_fulltext import config
from test_news_upmedia_native import URL, TEXT, entry, page, schema, v2_entry, v3_entry, single_byline_page


@pytest.fixture
def queue(config, tmp_path):
    config.update(root=str(tmp_path), providers=[entry()])
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified',attempts=1,due=0,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00'")
    yield queue
    queue.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def submit(queue, config, **options):
    return jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT,
        reason=options.pop('reason', 'Locate the new current native identity/credit failure'),
        evidence_contract_version=options.pop('evidence_contract_version', 20), **options)


def pump(queue):
    with patch.object(jobs.time, 'monotonic', return_value=0.0): jobs.pump_candidates(queue)


def current_queue(config, tmp_path, *, version=2):
    config.update(root=str(tmp_path), providers=[{1: entry, 2: v2_entry, 3: v3_entry}[version]()])
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
            "error_code='article_identity_not_verified',attempts=1,due=0,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00'")
    return queue


def observe_current_boundary(queue, config):
    """Completed question20 fixture; source and accepting parser stay v2."""
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V2
    request = jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V2,
        evidence_contract_version=20, reason='Unobserved current boundary')
    pump(queue)
    client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(single_byline_page()).encode()))
    job = jobs.claim_candidate(queue, client, config, 'upmedia')
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job))
    return request


def submit_editor_geometry(queue, config):
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V2
    return jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V2,
        evidence_contract_version=22, reason='Missing ad-script boundaries, label whitespace and profile encoding only')


def prepared_remaining_context(config, tmp_path, monkeypatch):
    """Real canonical question20/22 followed by a later normal-v3 marker."""
    from downloader import news_fulltext_upmedia as native
    from downloader.news_fulltext_repairs import _schema, _source
    from downloader.news_fulltext_queue import utc_now
    with closing(current_queue(config, tmp_path)) as queue:
        observe_current_boundary(queue, config)
        old_source = queue.registry.sources['upmedia']
        parent = submit_editor_geometry(queue, config); pump(queue)
        client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(single_byline_page()).encode()))
        job = jobs.claim_candidate(queue, client, config, 'upmedia')
        jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job))
    current = current_queue(config, tmp_path, version=3)
    new_source = current.registry.sources['upmedia']
    monkeypatch.setattr(native, 'UPMEDIA_V3_HTTP_BINDING', (
        old_source.data_fingerprint, new_source.data_fingerprint, new_source.authorization_fingerprint, 'a'*64, 'b'*64))
    assert old_source.authorization_fingerprint == new_source.authorization_fingerprint
    _schema(current)
    with current.db:
        _, identity = _source(current.db, current.registry, URL)
        stamp = utc_now()
        current.db.execute('INSERT INTO reviewed_repair_items VALUES (?,?,?,?,?,?,?,?)',
            ('c'*64, 0, bytes(head(current)['id']), 'upmedia', identity['parser_fingerprint'], 'enqueued', stamp, current.owner))
        current.db.execute('UPDATE articles SET attempts=3,last_checked_at_utc=?', (utc_now(),))
    return current, parent


def submit_remaining(queue, config):
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V3
    return jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V3,
        evidence_contract_version=23, reason='Only missing original JSON field, no-au2 profile and navigation punctuation')


def test_remaining_context_is_fixed_once_only_keeps_old_evidence_and_runs_parser_only_once(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_upmedia as native
    current, parent = prepared_remaining_context(config, tmp_path, monkeypatch)
    with closing(current) as queue:
        before = head(queue)
        old = [dict(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')]
        request = submit_remaining(queue, config); pump(queue)
        soup = single_byline_page('上報快訊')
        soup.select_one('.publish-text > a')['href'] = '#'
        soup.select_one('.author')['class'] = ['author']
        payload = str(soup).encode()
        client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, payload))
        original, calls = native.upmedia_public_modern_article, []
        def once(*args, **kwargs):
            calls.append(1)
            return original(*args, **kwargs)
        monkeypatch.setattr(native, 'upmedia_public_modern_article', once)
        job = jobs.claim_candidate(queue, client, config, 'upmedia')
        result = jobs.fetch_candidate(client, job)
        assert calls == [1] and result['question'] == native.UPMEDIA_REMAINING_QUESTION
        assert result['upmedia_remaining_context']['profiles'][0]['href_kind'] == 'fragment_only'
        assert result['current_native_error'] == 'article_attribution_not_verified'
        assert not result['accepted_as_article'] and not result['identity_or_alias_accepted']
        assert not result['raw_or_names_or_body_retained'] and not result['body_completeness_verified']
        jobs.finish_candidate(queue, job, result=result)
        assert head(queue) == before and client.calls == [URL]
        assert [dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
            (row['request_id'],)).fetchone()) for row in old] == old
        assert not submit_remaining(queue, config)['submitted']
        target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        assert jobs._key(target, 'a'*64, evidence_contract_version=23) == jobs._key(
            dict(target, attempts=999), 'b'*64, evidence_contract_version=23)
        assert not (queue.root/'bodies').exists()


@pytest.mark.parametrize('change', ['no_marker', 'wrong_parser', 'unknown_parent', 'refused_parent',
    'field_already_retained', 'future_parent', 'altered_request', 'array_request', 'array_result', 'unreviewed_current_source'])
def test_remaining_context_requires_distinct_field_proof_and_later_normal_repair(config, tmp_path, monkeypatch, change):
    from downloader import news_fulltext_upmedia as native
    current, parent = prepared_remaining_context(config, tmp_path, monkeypatch)
    with closing(current) as queue:
        before = head(queue)
        with queue.db:
            if change == 'no_marker': queue.db.execute('DELETE FROM reviewed_repair_items')
            elif change == 'wrong_parser': queue.db.execute('UPDATE reviewed_repair_items SET parser_fingerprint=?', ('a'*64,))
            elif change == 'unknown_parent': queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (parent['request_id'],))
            elif change == 'future_parent': queue.db.execute('UPDATE native_candidate_jobs SET completed_at_utc=? WHERE request_id=?', ('2999-01-01T00:00:00+00:00', parent['request_id']))
            elif change == 'altered_request': queue.db.execute("UPDATE native_candidate_jobs SET request_json='{}' WHERE request_id=?", (parent['request_id'],))
            elif change in {'array_request', 'array_result'}:
                field = 'request_json' if change == 'array_request' else 'result_json'
                queue.db.execute(f"UPDATE native_candidate_jobs SET {field}='[]' WHERE request_id=?", (parent['request_id'],))
            elif change == 'unreviewed_current_source': monkeypatch.setattr(native, 'UPMEDIA_V3_HTTP_BINDING', ('a'*64,)*5)
            else:
                result = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?', (parent['request_id'],)).fetchone()[0])
                if change == 'refused_parent': result['status'] = 403
                else: result['upmedia_remaining_context'] = {}
                queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?', (json.dumps(result), parent['request_id']))
        with pytest.raises(ValueError, match='completed_upmedia_missing_remaining_context_proof_required'):
            submit_remaining(queue, config)
        assert head(queue) == before


def test_remaining_context_prerequisite_is_rechecked_before_HTTP(config, tmp_path, monkeypatch):
    current, _ = prepared_remaining_context(config, tmp_path, monkeypatch)
    with closing(current) as queue:
        request = submit_remaining(queue, config); pump(queue)
        with queue.db: queue.db.execute('DELETE FROM reviewed_repair_items')
        client = http_stub(queue)
        assert jobs.claim_candidate(queue, client, config, 'upmedia') is None
        assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'skipped' and not client.calls


@pytest.mark.parametrize('control', ['\n', '\r', '\t'])
def test_remaining_context_preserves_original_JSON_control_identity_without_retaining_values(control):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_upmedia import upmedia_remaining_metadata
    soup = single_byline_page('上報快訊')
    soup.select_one('.author')['class'] = ['author']
    soup.select_one('.publish-text > a')['href'] = '#'
    schema(soup, lambda a: a.update(description='PRIVATE_DESCRIPTION'))
    payload = str(soup).encode().replace(b'PRIVATE_DESCRIPTION', ('PRIVATE'+control+'DESCRIPTION').encode())
    actual = BeautifulSoup(payload, 'lxml')
    result = upmedia_remaining_metadata(actual, raw_payload=payload)
    assert result['original_JSON_available'] and result['original_JSON_matches_DOM']
    assert result['profiles'][0]['href_kind'] == 'fragment_only'
    syntax, = result['declaration_syntax']
    assert syntax['control_codepoint_at_error'] == ord(control)
    context = syntax['original_field_context']
    assert context['proved'] and context['field'] == 'description' and context['nesting_depth'] == 1
    assert syntax['existing_description_LF_repair']['candidate_decoded'] is (control == '\n')
    encoded = json.dumps(result, ensure_ascii=False)
    assert all(value not in encoded for value in ('PRIVATE', 'DESCRIPTION', '上報快訊', TEXT, '合成標題'))


def test_remaining_context_separates_unlinked_punctuation_and_future_failure_uses_same_response(config, tmp_path):
    from downloader.news_fulltext_upmedia import upmedia_remaining_metadata
    with closing(current_queue(config, tmp_path, version=3)) as queue:
        soup = single_byline_page()
        soup.select_one('.rss_close strong').contents[0].replace_with('（延伸閱讀：')
        soup.select_one('.rss_close strong').contents[-1].replace_with(')')
        payload = str(soup).encode()
        source = queue.registry.sources['upmedia']
        result = candidates.candidate_response_metadata(Response(URL, 200, {'content-type': 'text/html'}, payload),
            source, source, evidence_contract_version=20, requested_url=URL)
        context = result['upmedia_native_boundary']['remaining_context']
        related, = context['related']
        assert related['punctuation_codepoints'] == [ord('（'), ord('：'), ord(')')]
        assert related['known_reading_labels'] == ['延伸閱讀'] and not related['v3_label_grammar_valid']
        assert context['original_JSON_matches_DOM'] and result['current_native_error'] == 'article_identity_not_verified'
        assert not result['accepted_as_article']
        without_original = upmedia_remaining_metadata(soup)
        assert not without_original['original_JSON_available'] and not without_original['declaration_syntax']


def test_editor_question_is_distinct_bounded_once_only_and_does_not_run_accepting_parser(config, tmp_path, monkeypatch):
    from urllib.parse import quote
    from downloader.news_fulltext_upmedia import UPMEDIA_EDITOR_QUESTION
    with closing(current_queue(config, tmp_path)) as queue:
        old = observe_current_boundary(queue, config)
        old_row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
        before = head(queue)
        soup = single_byline_page()
        link = soup.select_one('.publish-text > a')
        link['href'] = '/author/' + quote(link.get_text(' ', strip=True), safe='')
        soup.select_one('.rss_close strong').contents[0].replace_with('（ 延伸閱讀 ： ')
        script = soup.new_tag('script', type='text/javascript')
        script.string = 'window.syntheticAdOnly = true;'
        soup.select_one('.ad-detail').append(script)
        request = submit_editor_geometry(queue, config); pump(queue)
        client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()))
        monkeypatch.setattr('downloader.news_fulltext_upmedia.upmedia_public_modern_article',
            lambda *a, **k: pytest.fail('Question22 must not run accepting parser'))
        job = jobs.claim_candidate(queue, client, config, 'upmedia')
        facts = jobs.fetch_candidate(client, job)
        assert facts['question'] == UPMEDIA_EDITOR_QUESTION and facts['evidence_contract_version'] == 22
        native = facts['upmedia_editor_transport']
        assert native['profiles'][0]['decoded_once_matches_text'] and not native['profiles'][0]['literal_matches_text']
        related = next(r for r in native['exclusions'] if 'rss_close' in r['classes'])
        assert related['reading_label'] == '延伸閱讀' and related['normalized_known_prefix']
        assert not related['literal_known_prefix']
        ad = next(r for r in native['exclusions'] if 'ad-detail' in r['classes'])
        assert ad['text_characters'] == 0 and ad['scripts'][0]['within_ad_detail']
        assert ad['scripts'][0]['type'] == 'text/javascript'
        encoded = json.dumps(facts, ensure_ascii=False)
        assert TEXT not in encoded and '合成署名' not in encoded and script.string not in encoded
        assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
        jobs.finish_candidate(queue, job, result=facts)
        assert head(queue) == before and client.calls == [URL]
        assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
            (old['request_id'],)).fetchone()) == old_row
        assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
        assert not submit_editor_geometry(queue, config)['submitted']
        assert not (queue.root / 'bodies').exists()


def test_editor_question_requires_prior_current_boundary_without_replaying_it(config, tmp_path):
    with closing(current_queue(config, tmp_path)) as queue:
        before = head(queue)
        with pytest.raises(ValueError, match='completed_upmedia_missing_editor_geometry_proof_required'):
            submit_editor_geometry(queue, config)
        assert head(queue) == before


def test_editor_question_metadata_handles_malformed_profile_and_json_without_retaining_text():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_upmedia import upmedia_editor_metadata
    soup = single_byline_page()
    soup.select_one('.publish-text > a')['href'] = '//[invalid'
    script = soup.select_one('script[type="application/ld+json"]')
    script.string = '{"headline":"synthetic\nraw"}'
    # Match the received-HTML path: replacing .string creates a different BS4
    # text subtype than parsing a literal script from an actual response.
    soup = BeautifulSoup(str(soup), 'lxml')
    facts = upmedia_editor_metadata(soup)
    assert not facts['profiles'][0]['url_syntax_valid'] and not facts['profiles'][0]['relative_author_path']
    assert not facts['declaration_syntax'][0]['valid_json']
    assert facts['declaration_syntax'][0]['invalid_control_at_error']
    encoded = json.dumps(facts, ensure_ascii=False)
    assert 'synthetic' not in encoded and '合成署名' not in encoded and TEXT not in encoded


@pytest.mark.parametrize('field,value', [('state', 'uncertain_outcome'), ('state', 'reserved'),
    ('state', 'observed_error'), ('result_json', '{}'), ('request_json', '{}'),
    ('result_json', '[]'), ('result_json', 'x' * 131073)])
def test_editor_question_requires_terminal_bounded_same_source_proof(config, tmp_path, field, value):
    with closing(current_queue(config, tmp_path)) as queue:
        observe_current_boundary(queue, config)
        with queue.db: queue.db.execute(f'UPDATE native_candidate_jobs SET {field}=?', (value,))
        with pytest.raises(ValueError, match='completed_upmedia_missing_editor_geometry_proof_required'):
            submit_editor_geometry(queue, config)


@pytest.mark.parametrize('field,value', [('question', 'different'), ('status', 403),
    ('final_url', 'https://example.org/elsewhere'), ('accepted_as_article', True),
    ('raw_or_names_or_body_retained', True), ('upmedia_native_boundary', {'contract_version': 1}),
    ('upmedia_editor_transport', {})])
def test_editor_question_refuses_wrong_or_already_retained_boundary(config, tmp_path, field, value):
    with closing(current_queue(config, tmp_path)) as queue:
        observe_current_boundary(queue, config)
        result = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs').fetchone()[0])
        result[field] = value
        with queue.db: queue.db.execute('UPDATE native_candidate_jobs SET result_json=?', (json.dumps(result),))
        with pytest.raises(ValueError, match='completed_upmedia_missing_editor_geometry_proof_required'):
            submit_editor_geometry(queue, config)


def test_editor_question_allows_later_normal_failure_but_rechecks_proof_at_reservation(config, tmp_path):
    with closing(current_queue(config, tmp_path)) as queue:
        old = observe_current_boundary(queue, config)
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=attempts+1, last_checked_at_utc=?', ('2026-02-01T00:00:00+00:00',))
        request = submit_editor_geometry(queue, config); pump(queue)
        with queue.db:
            queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?', ('uncertain_outcome', old['request_id']))
        client = http_stub(queue, response=Response(URL, 200, {}, b'unused'))
        assert jobs.claim_candidate(queue, client, config, 'upmedia') is None and client.calls == []
        assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'skipped'


@pytest.mark.parametrize('unsupported', [False, True])
def test_current_v2_uses_same_fixed_question_correct_adapter_and_existing_owner(config, tmp_path, unsupported):
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V2
    with closing(current_queue(config, tmp_path)) as queue:
        soup = single_byline_page()
        if unsupported: soup.select_one('.news-box-text').append(soup.new_tag('hr'))
        before = head(queue)
        response = Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode())
        request = jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V2,
            reason='Inspect only the unresolved current boundary', evidence_contract_version=20)
        pump(queue)
        client = http_stub(queue, response=response)
        job = jobs.claim_candidate(queue, client, config, 'upmedia')
        assert job and job['source'] is job['candidate']
        result = jobs.fetch_candidate(client, job)
        assert result['question'] == UPMEDIA_BOUNDARY_QUESTION
        assert result['current_native_error'] == ('article_identity_not_verified' if unsupported else None)
        facts = result['upmedia_native_boundary']
        assert facts['contract_version'] == 2 and facts['byline_links'] == 1
        assert facts['schema_author_matches_visible']
        assert facts['publication_header']['schema_author_is_person']
        assert not result['accepted_as_article'] and not result['raw_or_names_or_body_retained']
        assert TEXT not in json.dumps(result, ensure_ascii=False) and '合成署名' not in json.dumps(result, ensure_ascii=False)
        jobs.finish_candidate(queue, job, result=result)
        assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
        assert head(queue) == before and client.calls == [URL]
        assert not (queue.root / 'bodies').exists()
        assert not candidates.candidate_evidence_allowed(job['source'], job['candidate'], 21)
        assert not jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V2,
            reason='Changed reason cannot repeat the old question', evidence_contract_version=20)['submitted']


def test_completed_boundary_question_cannot_rearm_by_upgrading_source_to_v2(config, tmp_path):
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V2
    with closing(current_queue(config, tmp_path, version=1)) as queue:
        request = submit(queue, config); pump(queue)
        client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode()))
        job = jobs.claim_candidate(queue, client, config, 'upmedia')
        jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(client, job))
        old = dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
    with closing(current_queue(config, tmp_path)) as queue:
        # The source/authorization and configured selector really changed, but
        # the URL/question key and completed durable evidence did not.
        assert json.loads(old['request_json'])['source']['data_fingerprint'] != queue.registry.sources['upmedia'].data_fingerprint
        assert not jobs.submit_candidate(queue.root, config, queue.registry, URL, UPMEDIA_LAYOUT_V2,
            reason='Parser upgrade is not a new observation allowance', evidence_contract_version=20)['submitted']
        assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone()) == old
        assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'


@pytest.mark.parametrize('change', ['author_selectors', 'domains', 'article_path_pattern', 'embedded_article_layout'])
def test_v2_diagnostic_support_does_not_allow_an_unreviewed_scope(config, tmp_path, change):
    with closing(current_queue(config, tmp_path)) as queue:
        source = queue.registry.sources['upmedia']
        replacement = () if change in {'author_selectors', 'domains'} else 'unreviewed'
        assert not candidates.candidate_evidence_allowed(replace(source, **{change: replacement}),
            replace(source, **{change: replacement}), 20)


def test_same_owner_observation_keeps_exact_head_and_cannot_rearm(queue, config, monkeypatch):
    soup = page(); soup.select_one('.news-box-text').append(soup.new_tag('hr'))
    response = Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode())
    before = head(queue)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *a, **k: pytest.fail('Cannot create another HTTP owner'))
    request = submit(queue, config); pump(queue)
    client = http_stub(queue, response=response)
    job = jobs.claim_candidate(queue, client, config, 'upmedia')
    assert job and job['source'] is job['candidate']
    result = jobs.fetch_candidate(client, job)
    assert result['question'] == UPMEDIA_BOUNDARY_QUESTION
    assert result['current_native_error'] == 'article_identity_not_verified'
    assert result['upmedia_native_boundary']['unsupported_editor_tags'] == ['hr']
    assert result['failure_location']['sites'][-1]['function'] == '_whole_editor'
    assert not result['accepted_as_article'] and not result['body_completeness_verified']
    assert not result['raw_or_names_or_body_retained']
    encoded = json.dumps(result, ensure_ascii=False)
    assert TEXT not in encoded and '合成測試' not in encoded
    jobs.finish_candidate(queue, job, result=result); pump(queue)
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert client.calls == [URL] and head(queue) == before
    assert not (queue.root/'bodies').exists()
    assert not submit(queue, config, reason='Changing a reason is not a new question')['submitted']
    assert jobs.claim_candidate(queue, client, config, 'upmedia') is None


def test_single_desk_credit_can_be_observed_without_relaxing_attribution(queue):
    soup = page(); soup.select('.author > a')[1].decompose()
    profile = 'https://www.upmedia.mg/author/上報快訊'
    schema(soup, lambda a: a['author'].update(name='上報快訊', url=profile))
    soup.select_one('meta[property="article:author"]')['content'] = profile
    source = queue.registry.sources['upmedia']
    result = candidates.candidate_response_metadata(
        Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()), source, source,
        evidence_contract_version=20, requested_url=URL)
    facts = result['upmedia_native_boundary']
    assert facts['single_house_desk'] and facts['schema_author_matches_visible'] and facts['schema_profile_matches']
    assert result['current_native_error'] == 'article_attribution_not_verified'
    assert not result['accepted_as_article'] and not result['identity_or_alias_accepted']


@pytest.mark.parametrize('state', ['complete', 'pending', 'retry', 'inflight', 'http_403', 'http_429',
                                  'paywall_or_login', 'source_not_found', 'unsupported_article_path'])
def test_completed_transport_refusal_and_nonarticle_heads_are_not_new_questions(queue, config, state):
    with queue.db: queue.db.execute('UPDATE articles SET state=?,error_code=?', (state, state))
    before = head(queue)
    with pytest.raises(ValueError): submit(queue, config)
    assert head(queue) == before


def test_evidence_scope_and_question_are_closed_and_version_independent(queue, config):
    source = queue.registry.sources['upmedia']
    assert candidates.native_candidate_source(config, queue.registry, queue.registry.base, UPMEDIA_LAYOUT) is source
    assert candidates.candidate_evidence_allowed(source, source, 20)
    assert candidates.candidate_evidence_allowed(source, source, 21)
    assert all(not candidates.candidate_evidence_allowed(source, source, v) for v in [True, 1, 13, 15, 18, 19, 22])
    assert not candidates.candidate_evidence_allowed(source, replace(source, data_fingerprint='f'*64), 20)
    target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    assert jobs._key(target, 'a'*64, evidence_contract_version=20) == jobs._key(
        dict(target, attempts=99, last_checked_at_utc='2026-02-01T00:00:00+00:00'), 'b'*64, evidence_contract_version=20)


def test_unknown_delivery_stays_uncertain_without_new_http(queue, config):
    request = submit(queue, config); pump(queue)
    client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode()))
    assert jobs.claim_candidate(queue, client, config, 'upmedia')
    root, registry = queue.root, queue.registry; queue.close()
    replacement = NewsQueue(root, registry)
    try:
        pump(replacement)
        assert jobs.candidate_status(root, request['request_id'])['state'] == 'uncertain_outcome'
        assert not submit(replacement, config)['submitted']
        assert jobs.claim_candidate(replacement, client, config, 'upmedia') is None and not client.calls
    finally: replacement.close()


@pytest.mark.parametrize('changes', [{'url': URL+'0'}, {'body': b'x' * ((2 << 20) + 1)}, {'status': 403}])
def test_redirect_large_or_denied_response_cannot_be_used_as_native_metadata(queue, changes):
    source = queue.registry.sources['upmedia']
    response = replace(Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode()), **changes)
    with pytest.raises(NewsError):
        candidates.candidate_response_metadata(response, source, source,
            evidence_contract_version=20, requested_url=URL)


def direct_credit():
    """Synthetic layout for metadata tests, not proof of a publisher layout."""
    soup = page()
    credit = soup.new_tag('a', attrs={'class': ['synthetic-byline'], 'href': '/author/上報快訊'})
    credit.string = '上報快訊'
    soup.select_one('.author').replace_with(credit)
    profile = 'https://www.upmedia.mg/author/上報快訊'
    schema(soup, lambda a: a['author'].update(name='上報快訊', url=profile))
    soup.select_one('meta[property="article:author"]')['content'] = profile
    return soup


def complete_missing_field_parent(queue, config):
    soup = direct_credit()
    request = submit(queue, config)
    pump(queue)
    client = http_stub(queue, response=Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()))
    job = jobs.claim_candidate(queue, client, config, 'upmedia')
    result = jobs.fetch_candidate(client, job)
    assert result['upmedia_native_boundary']['byline_links'] == 0
    assert result['current_native_error'] == 'article_attribution_not_verified'
    jobs.finish_candidate(queue, job, result=result)
    pump(queue)
    return request, client


def test_new_fixed_header_question_keeps_completed_parent_and_article_head(queue, config):
    before = head(queue)
    parent, client = complete_missing_field_parent(queue, config)
    parent_row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (parent['request_id'],)).fetchone())
    request = submit(queue, config, evidence_contract_version=21)
    pump(queue)
    job = jobs.claim_candidate(queue, client, config, 'upmedia')
    result = jobs.fetch_candidate(client, job)
    assert result['question'] == UPMEDIA_HEADER_QUESTION
    facts = result['upmedia_publication_header']
    node, = [n for n in facts['nodes'] if n['tag'] == 'a']
    assert node['classes'] == ['synthetic-byline'] and node['parent'] == 0
    assert node['schema_name_matches'] and node['schema_profile_matches_link']
    assert node['literal_author_profile_matches_text'] and node['house_desk']
    assert facts['head_profile_matches_schema'] and not facts['nodes_truncated']
    assert not result['accepted_as_article'] and not result['raw_or_names_or_body_retained']
    encoded = json.dumps(result, ensure_ascii=False)
    assert '上報快訊' not in encoded and TEXT not in encoded and '<a' not in encoded
    jobs.finish_candidate(queue, job, result=result)
    pump(queue)
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (parent['request_id'],)).fetchone()) == parent_row
    assert head(queue) == before and client.calls == [URL, URL]
    assert not submit(queue, config, evidence_contract_version=21, reason='No rearm by reason')['submitted']
    assert not submit(queue, config)['submitted']
    target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    assert jobs._key(target, 'a'*64, evidence_contract_version=21) == jobs._key(
        dict(target, attempts=99), 'b'*64, evidence_contract_version=21)
    assert jobs._key(target, 'a'*64, evidence_contract_version=21) != jobs._key(target, 'a'*64, evidence_contract_version=20)


def test_header_question_needs_completed_missing_field_proof(queue, config):
    before = head(queue)
    with pytest.raises(ValueError, match='completed_upmedia_missing_header_proof_required'):
        submit(queue, config, evidence_contract_version=21)
    assert head(queue) == before and not (queue.root/'bodies').exists()


@pytest.mark.parametrize('change', ['parent_uncertain', 'parent_has_links', 'head_changed', 'parent_wrong_question'])
def test_header_parent_change_blocks_new_observation(queue, config, change):
    parent, client = complete_missing_field_parent(queue, config)
    with queue.db:
        if change == 'parent_uncertain':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (parent['request_id'],))
        elif change == 'head_changed':
            queue.db.execute('UPDATE articles SET attempts=attempts+1')
        else:
            value = json.loads(queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?', (parent['request_id'],)).fetchone()[0])
            if change == 'parent_has_links':
                value['upmedia_native_boundary']['byline_links'] = 1
            else:
                value['question'] = 'not_the_completed_question'
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?', (json.dumps(value), parent['request_id']))
    with pytest.raises(ValueError, match='completed_upmedia_missing_header_proof_required'):
        submit(queue, config, evidence_contract_version=21)
    assert client.calls == [URL]


def test_resident_rechecks_header_parent_before_network(queue, config):
    parent, client = complete_missing_field_parent(queue, config)
    request = submit(queue, config, evidence_contract_version=21)
    pump(queue)
    with queue.db:
        queue.db.execute("UPDATE native_candidate_jobs SET state='observed_error' WHERE request_id=?", (parent['request_id'],))
    assert jobs.claim_candidate(queue, client, config, 'upmedia') is None
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'skipped'
    assert client.calls == [URL]


def test_header_geometry_is_bounded_and_contains_no_attribute_payload(queue):
    soup = direct_credit()
    for _ in range(70):
        node = soup.new_tag('span', attrs={'class': ['私有文字', 'x'*200], 'data-extra': '不可保留'})
        node.string = '不可保留'
        soup.select_one('.publish-text').append(node)
    source = queue.registry.sources['upmedia']
    result = candidates.candidate_response_metadata(
        Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()), source, source,
        evidence_contract_version=21, requested_url=URL)
    facts = result['upmedia_publication_header']
    assert facts['nodes_truncated'] and len(facts['nodes']) == 64
    encoded = json.dumps(result, ensure_ascii=False)
    assert not any(s in encoded for s in ['私有文字', '不可保留', 'x'*200, TEXT])
