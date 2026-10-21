"""New credit/clock evidence cannot retry old work or admit an article."""
from datetime import datetime
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_response_metadata, videoland_candidate_source
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_videoland import _clock_shape, videoland_credit_clock_metadata
from test_news_videoland_original import (
    URL, TITLE, CREDIT, PROSE, CAPTION, RELEASE, REVISION, changed, change_schema,
    description_defect_page, versioned_config,
)
from test_news_candidate_diagnostics import queue, settings, head, http_stub, request_path, seal_request


def submit(queue, settings, version=2):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL,
        'videoland_visible_article_v4', reason='New native credit and clock question', evidence_contract_version=version)


def nonanchor(soup):
    node = soup.select_one('a#author')
    node.name = 'span'
    del node['href']


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_literal_LF_envelope_reports_nonanchor_credit_without_raw_values_or_admission(engine):
    raw = description_defect_page(nonanchor, absent_body=True)
    facts = videoland_credit_clock_metadata(BeautifulSoup(raw, engine), page_url=URL)
    credit = facts['credit']
    assert facts['reason'] == 'native_credit_clock_shapes' and facts['literal_URL_agrees']
    assert credit['tag'] == 'span' and credit['visible'] and not credit['has_href']
    assert credit['native_equals_schema_name'] and credit['reporter_signature'] and not credit['plain_han_name']
    assert not credit['nonoriginal_credit_token'] and credit['schema_is_single_Person']
    assert credit['prefix_class'] == '記者'
    clocks = facts['clocks']
    for key in ('datePublished', 'dateModified'):
        row = clocks[key]
        assert row['schema_literal_equals_meta'] and row['schema_meta_same_aware_instant']
        assert row['schema_wall_equals_native_at_native_precision']
        assert row['schema_at_UTC_plus_08_equals_native_at_native_precision']
        assert row['native_visible'] and row['native_count'] == row['meta_count'] == 1
        assert row['native']['fields'][-2:] == [None, None]
        assert row['schema']['offset_seconds'] == 28800 and row['schema']['fractional_digits'] is None
    assert not any(facts[k] for k in ('accepted_as_article', 'raw_names_or_body_retained', 'source_requests'))
    output = json.dumps(facts, ensure_ascii=False)
    assert all(value not in output for value in (URL, TITLE, CREDIT, PROSE, CAPTION, RELEASE, REVISION))


@pytest.mark.parametrize('text,format,fields,offset', [
    ('2026/10/16 17:53', 'slash_space', [2026,10,16,17,53,None,None], None),
    ('2026/10/16 17:53:29', 'slash_space', [2026,10,16,17,53,29,None], None),
    ('2026-10-16 17:53', 'dash_space', [2026,10,16,17,53,None,None], None),
    ('2026年10月16日 17:53', 'chinese_ymd_space', [2026,10,16,17,53,None,None], None),
    ('2026-10-16T17:53:29.000001+08:00', 'iso_T', [2026,10,16,17,53,29,1], 28800),
    ('2026-10-16T09:53:29Z', 'iso_T', [2026,10,16,9,53,29,None], 0),
    ('2026-10-16T17:53:29+0800', 'iso_T', [2026,10,16,17,53,29,None], 28800),
    ('2026-10-16T17:53:29', 'iso_T', [2026,10,16,17,53,29,None], None),
    ('2026/10/16', 'date_only', [2026,10,16,None,None,None,None], None),
])
def test_clock_shape_preserves_source_precision_and_never_invents_timezone(text, format, fields, offset):
    facts, parsed = _clock_shape(text)
    assert facts['format'] == format and facts['fields'] == fields and facts['offset_seconds'] == offset
    assert facts['value_sha256'] == hashlib.sha256(text.encode()).hexdigest()
    assert text not in json.dumps(facts, ensure_ascii=False)
    assert parsed is None if format == 'date_only' else isinstance(parsed, datetime)


@pytest.mark.parametrize('value', [None, 17, [], 'PRIVATE SOURCE TEXT', 'x'*97,
    '2026/99/16 17:53', '2026-10-16T27:53:29+08:00', '2026-10-16T17:53:29+99:00',
    '2026/10-16 17:53', '2026-10-16 17:53 trailing text'])
def test_unknown_clock_is_a_shape_not_a_date_guess(value):
    facts, parsed = _clock_shape(value)
    assert parsed is None and 'fields' not in facts
    assert facts['format'] in {'unknown', 'invalid_calendar_or_offset'}
    assert 'PRIVATE' not in json.dumps(facts)


def test_escaped_nonscalar_clock_cannot_crash_metadata_diagnostic():
    facts, parsed = _clock_shape('\ud800')
    assert parsed is None and facts['format'] == 'invalid_unicode_scalar'
    assert 'value_sha256' not in facts


def test_UTC_display_hypothesis_is_separate_from_same_wall_time_and_literal_meta():
    def utc(soup):
        change_schema(soup, datePublished='2026-10-16T09:53:00Z')
        soup.select_one('meta[property="article:published_time"]')['content'] = '2026-10-16T17:53:00+08:00'
    soup = BeautifulSoup(changed(utc), 'lxml')
    facts = videoland_credit_clock_metadata(soup, page_url=URL)['clocks']['datePublished']
    assert facts['schema_meta_same_aware_instant'] and not facts['schema_literal_equals_meta']
    assert not facts['schema_wall_equals_native_at_native_precision']
    assert facts['schema_at_UTC_plus_08_equals_native_at_native_precision']


def test_clock_diagnostic_bounds_timezone_conversion_without_crashing_owner():
    def end_of_calendar(soup):
        change_schema(soup, datePublished='9999-12-31T23:53:00-12:00')
    facts = videoland_credit_clock_metadata(BeautifulSoup(changed(end_of_calendar), 'lxml'), page_url=URL)
    row = facts['clocks']['datePublished']
    assert row['UTC_plus_08_conversion_out_of_range'] is True
    assert row['schema_at_UTC_plus_08_equals_native_at_native_precision'] is None
    assert facts['accepted_as_article'] is False


@pytest.mark.parametrize('version', [True, False, 1.0, 2.0, '2', None, 0, 3])
def test_fixed_question_keys_reject_noninteger_or_unknown_versions(version):
    with pytest.raises(ValueError, match='reviewed_candidate_evidence_contract_required'):
        diagnostic._key({'provider':'videoland', 'id':'f'*64}, 'a'*64, evidence_contract_version=version)


@pytest.mark.parametrize('change,expected', [
    (lambda soup: soup.select_one('#author').__setitem__('hidden',''), 'hidden'),
    (lambda soup: soup.select_one('header').append(BeautifulSoup('<span id="author">duplicate</span>', 'lxml')), 'duplicate'),
    (lambda soup: change_schema(soup, author={'@type':'Organization', 'name':CREDIT}), 'organization'),
    (lambda soup: change_schema(soup, datePublished='2026-10-16T17:53:00'), 'naive'),
    (lambda soup: soup.select_one('#NewsRealDate').decompose(), 'missing_native'),
    (lambda soup: soup.select_one('meta[property="article:published_time"]').decompose(), 'missing_meta'),
])
def test_missing_conflicting_hidden_or_naive_fields_remain_explicit(change, expected):
    facts = videoland_credit_clock_metadata(BeautifulSoup(changed(change), 'lxml'), page_url=URL)
    if expected == 'hidden':
        assert facts['credit']['visible'] is False
    elif expected == 'duplicate':
        assert facts['credit']['node_count'] == 2 and 'tag' not in facts['credit']
    elif expected == 'organization':
        assert facts['credit']['schema_is_single_Person'] is False
    else:
        row = facts['clocks']['datePublished']
        if expected == 'naive':
            assert row['schema']['offset_seconds'] is None
            assert row['schema_at_UTC_plus_08_equals_native_at_native_precision'] is None
            assert row['schema_meta_same_aware_instant'] is None
        elif expected == 'missing_native':
            assert row['native_count'] == 0 and row['native']['type'] == 'missing'
        else:
            assert row['meta_count'] == 0 and row['meta']['type'] == 'missing'
    assert facts['accepted_as_article'] is False


def test_plain_name_and_wire_marker_are_not_silently_promoted_to_own_reporter():
    def plain(soup):
        nonanchor(soup)
        soup.select_one('#author').string = '合成姓名'
        change_schema(soup, author={'@type':'Person', 'name':'合成姓名'})
    facts = videoland_credit_clock_metadata(BeautifulSoup(changed(plain), 'lxml'), page_url=URL)
    assert facts['credit']['plain_han_name'] and not facts['credit']['reporter_signature']
    assert facts['credit']['native_equals_schema_name'] and not facts['accepted_as_article']
    def wire(soup):
        soup.select_one('#author').string = '中央社'
    wire_facts = videoland_credit_clock_metadata(BeautifulSoup(changed(wire), 'lxml'), page_url=URL)
    assert wire_facts['credit']['nonoriginal_credit_token']
    assert '合成姓名' not in json.dumps(facts, ensure_ascii=False)


def test_v1_response_contract_is_unchanged_and_v2_is_explicit_metadata_only(tmp_path):
    settings = versioned_config(3)
    registry = PolicyRegistry(settings, base=tmp_path)
    source = registry.sources['videoland']
    candidate = videoland_candidate_source(settings, registry, tmp_path, 'videoland_visible_article_v4')
    response = Response(URL, 200, {'content-type':'text/html'}, description_defect_page(nonanchor))
    v1 = candidate_response_metadata(response, source, candidate)
    v2 = candidate_response_metadata(response, source, candidate, evidence_contract_version=2)
    assert 'videoland_credit_clock_evidence' not in v1 and 'evidence_contract_version' not in v1
    assert v2['evidence_contract_version'] == 2
    assert v2['candidate_parser']['error'] == 'article_attribution_not_verified'
    assert v2['videoland_credit_clock_evidence']['credit']['native_equals_schema_name']
    for result in (v1, v2):
        assert result['accepted_as_article'] is False and result['raw_or_names_or_body_retained'] is False
        assert all(value not in json.dumps(result, ensure_ascii=False) for value in (TITLE, CREDIT, PROSE, CAPTION))


def test_explicit_new_evidence_cannot_rearm_or_mutate_completed_v1(queue, settings):
    before, owner = head(queue), queue.owner
    v1 = submit(queue, settings, 1)
    diagnostic.pump_candidates(queue)
    http = http_stub(queue)
    old_job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
    diagnostic.finish_candidate(queue, old_job, result=diagnostic.fetch_candidate(http, old_job))
    diagnostic.pump_candidates(queue)
    old_row = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (v1['request_id'],)).fetchone())
    old_archive = request_path(queue, v1).parent.parent/'processed'/(v1['request_id']+'.outcome.json')
    old_bytes = old_archive.read_bytes()
    v2 = submit(queue, settings)
    path = request_path(queue, v2)
    assert diagnostic._load(path)['evidence_contract_version'] == 2
    diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
    assert job['evidence_contract_version'] == 2 and job['request_id'] != old_job['request_id']
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    diagnostic.pump_candidates(queue)
    assert http.calls == [URL, URL]  # One v1 question and one genuinely new v2 question.
    assert not submit(queue, settings)['submitted'] and not submit(queue, settings, 1)['submitted']
    assert old_bytes == old_archive.read_bytes()
    assert old_row == dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (v1['request_id'],)).fetchone())
    assert head(queue) == before and queue.owner == owner
    assert diagnostic.candidate_status(queue.root, v2['request_id'])['result_json']['evidence_contract_version'] == 2


@pytest.mark.parametrize('state', ['ready','reserved','uncertain_outcome'])
def test_v2_never_bypasses_pending_or_unknown_v1_outcome(queue, settings, state):
    v1 = submit(queue, settings, 1)
    diagnostic.pump_candidates(queue)
    if state != 'ready':
        with queue.db:
            queue.db.execute('UPDATE native_candidate_jobs SET state=? WHERE request_id=?', (state,v1['request_id']))
    with pytest.raises(ValueError, match='prior_observation_not_terminal'):
        submit(queue, settings)
    assert queue.db.execute('SELECT COUNT(*) FROM native_candidate_jobs').fetchone()[0] == 1


@pytest.mark.parametrize('mutation', ['version','missing','mismatched_key','downgrade','provider'])
def test_v2_mailbox_cannot_smuggle_an_arbitrary_new_once_key(queue, settings, mutation):
    request = submit(queue, settings)
    path = request_path(queue, request)
    value = json.loads(path.read_bytes())
    if mutation == 'version':
        value['evidence_contract_version'] = 3
    elif mutation == 'missing':
        del value['evidence_contract_version']
    elif mutation == 'mismatched_key':
        value['observation_key'] = diagnostic._key(value['target'], value['candidate_fingerprint'])
    elif mutation == 'downgrade':
        value['contract_version'] = 1
    else:
        value['target']['provider'] = 'ettoday'
        value['observation_key'] = diagnostic._key(value['target'], value['candidate_fingerprint'], evidence_contract_version=2)
    path = seal_request(path, value)
    with pytest.raises(ValueError):
        diagnostic._load(path)


@pytest.mark.parametrize('version', [0,3,True,'2',None])
def test_unreviewed_evidence_is_rejected_before_any_new_request(queue, settings, version):
    with pytest.raises(ValueError, match='reviewed_candidate_evidence_contract_required'):
        submit(queue, settings, version)
    assert not (queue.root/'reviewed_repairs').exists()


def test_CLI_new_evidence_is_resident_only_and_never_constructs_HTTP(queue, settings, tmp_path, monkeypatch):
    from scripts import probe_news_sources as cli
    monkeypatch.setattr(cli, 'load_config', lambda p:(settings, queue.registry,tmp_path))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a,**k:pytest.fail('no second HTTP owner'))
    output = tmp_path/'private/v2.json'
    args = ['--current-failure-provider','videoland','--maximum-failure-probes','1',
        '--failure-code','article_identity_not_verified','--candidate-embedded-layout','videoland_visible_article_v4',
        '--candidate-evidence-contract','2','--enqueue-candidate','--output',str(output)]
    assert cli.main(args) == 0
    submitted = json.loads(output.read_bytes())
    value = diagnostic._load(request_path(queue, submitted))
    assert value['contract_version'] == value['evidence_contract_version'] == 2
    assert submitted['publisher_requests'] == 0
    with pytest.raises(SystemExit):
        cli.main([x for x in args if x != '--enqueue-candidate'])
