"""Complete written highlights and literal malformed signatures keep raw values."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader import news_fulltext_knews as native
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_knews_known_urls import URL, authored
from test_news_knews_written_blocks import sparse_entry


def table_entry():
    value = sparse_entry()
    value.update(embedded_article_layout=native.KNEWS_TABLE_NATIVE_LAYOUT,
        adapter_verification=native.KNEWS_V8_VERIFICATION)
    return value


def table_page():
    soup = authored()
    root = soup.new_tag('div', style='overflow: hidden; border-radius: 0.5em; margin: 0.75em 0;')
    table = soup.new_tag('table', style='border-collapse: collapse; width: 100%;')
    body = soup.new_tag('tbody')
    row = soup.new_tag('tr')
    accent = soup.new_tag('td', style='width: 6px; background-color: #ffc107;')
    value = soup.new_tag('td', style='background-color: #fff8e1; padding: 0.75em;')
    value.string = '合成重點：價格12.34、股數1000、比率-5.2%、年度2020。'
    row.extend([accent, value]); body.append(row); table.append(body); root.append(table)
    soup.select_one('.article-content-inner > p').insert_after(root)
    return soup


def source(entry=None):
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    settings['providers'] = [entry or table_entry()]
    return PolicyRegistry(settings, base=repo).sources['knews']


def extract(soup, *, entry=None):
    return extract_article(str(soup).encode(), source(entry), page_url=URL)


def test_whole_highlight_keeps_values_once_and_both_parsers_match():
    soup = table_page()
    result = extract(soup)
    highlight = soup.select_one('table').get_text(' ', strip=True)
    assert result['body'].count(highlight) == 1
    assert result == extract_article(str(soup).encode(), replace(source(), html_parser='html.parser'), page_url=URL)
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_written_table_highlights'] == 1
    assert notices['native_written_table_characters'] == len(highlight)
    assert notices['native_written_table_values_retained']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, entry=sparse_entry())


@pytest.mark.parametrize('case', ['extra_prose', 'written_accent', 'active', 'resource', 'nested_text',
    'hidden', 'event', 'extra_row', 'empty', 'unknown_attribute', 'wrong_wrapper'])
def test_highlight_cannot_discard_unverified_content_or_controls(case):
    soup = table_page(); root = soup.select_one('.article-content-inner > div:not([class])')
    accent, value = root.select('td')
    if case == 'extra_prose':
        paragraph = soup.new_tag('p'); paragraph.string = '不能悄悄略掉的旁支文字。'; root.append(paragraph)
    elif case == 'written_accent': accent.string = '不是裝飾'
    elif case in {'active', 'resource', 'nested_text'}:
        child = soup.new_tag({'active':'script','resource':'iframe','nested_text':'span'}[case])
        child.string = '未知內容'; value.append(child)
    elif case == 'hidden': value['style'] = 'display:none'
    elif case == 'event': root['onclick'] = 'neverExecuted()'
    elif case == 'extra_row': root.tbody.append(deepcopy(root.tr))
    elif case == 'empty': value.clear()
    elif case == 'unknown_attribute': value['data-source'] = 'unverified'
    else: root.name = 'section'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


def test_missing_opening_bracket_keeps_exact_signature_and_distinct_names():
    soup = authored()
    original = soup.select_one('.post-intro').get_text(' ', strip=True)
    soup.select_one('.post-intro').string = original.removeprefix('【')
    result = extract(soup)
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['printed_byline_raw'] == '記者合成作者／台北報導】'
    assert not detail['printed_signature_opening_bracket_present']
    assert not detail['source_signature_text_rewritten'] and not detail['different_names_are_verified_aliases']
    assert result['body'].count(original.removeprefix('【')) == 1
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup, entry=sparse_entry())


@pytest.mark.parametrize('intro', ['合成作者／台北報導】文字。', '記者／台北報導】文字。',
    '記者合成作者／台北報導文字。', '記者合成作者報導】文字。', '【中央社／台北報導】文字。'])
def test_partial_signature_and_external_credit_stay_unaccepted(intro):
    soup = authored(); soup.select_one('.post-intro').string = intro
    with pytest.raises(NewsError): extract(soup)


def test_installed_candidate_preserves_other_sources_and_original_http_identity(monkeypatch):
    import test_news_fulltext as shared
    settings, registry, repo = shared.load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings); candidate.pop('provider_catalog', None)
    next(p for p in candidate['providers'] if p['id']=='knews').update(
        embedded_article_layout=native.KNEWS_TABLE_NATIVE_LAYOUT, adapter_verification=native.KNEWS_V8_VERIFICATION)
    approved = PolicyRegistry(candidate, base=repo)
    for provider, old in registry.sources.items():
        current = approved.sources[provider]
        for field in ('authorization_fingerprint','http_capacity_source_fingerprint','http_capacity_authorization_fingerprint'):
            assert getattr(current,field)==getattr(old,field)
        if provider!='knews': assert current.data_fingerprint==old.data_fingerprint
    monkeypatch.setattr(shared, 'load_config', lambda _: (candidate, approved, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.fixture(params=['article_identity_not_verified', 'article_attribution_not_verified'])
def retained_original(request, tmp_path):
    soup = table_page() if request.param == 'article_identity_not_verified' else authored()
    if request.param == 'article_attribution_not_verified':
        intro = soup.select_one('.post-intro')
        intro.string = intro.get_text(' ', strip=True).removeprefix('【')
    yield from retained_native_fixture(tmp_path, request.param, soup)


def retained_native_fixture(tmp_path, failure, soup, *,
        prior_layout=native.KNEWS_SPARSE_NATIVE_LAYOUT, prior_verification=native.KNEWS_V7_VERIFICATION,
        next_layout=native.KNEWS_TABLE_NATIVE_LAYOUT, next_verification=native.KNEWS_V8_VERIFICATION):
    import pyarrow.parquet as pq
    from downloader.artifact_io import atomic_write_json, sha256_file
    from downloader.news_fulltext_candidate_diagnostics import submit_candidate, pump_candidates, claim_candidate, fetch_candidate, finish_candidate
    from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    from downloader.news_fulltext_retained_native import _article_digest
    from types import SimpleNamespace
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    entry = deepcopy(next(p for p in settings['providers'] if p['id']=='knews'))
    entry.update(embedded_article_layout=prior_layout, adapter_verification=prior_verification)
    settings.update(providers=[entry], root=str(tmp_path/'news'), min_free_disk_bytes=1, exclusive_http_owner=False)
    queue = NewsQueue(tmp_path/'news', PolicyRegistry(settings, base=repo))
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=6,due=0,last_checked_at_utc=?',
            (failure, failure, '2026-10-24T10:00:00+00:00'))
    response = Response(URL,200,{'content-type':'text/html'},str(soup).encode())
    requested = submit_candidate(queue.root,settings,queue.registry,URL,prior_layout,
        evidence_contract_version=56,reason='Test a previously unobserved failed native original once')
    pump_candidates(queue)
    http = SimpleNamespace(get=lambda *a, **k:response,provider_not_before={},
        blocked_gateway_hosts=lambda _:(),blocked_robots_origins=lambda _:())
    job = claim_candidate(queue,http,settings,'knews')
    finish_candidate(queue,job,result=fetch_candidate(http,job))
    completed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())
    reference = json.loads(completed['result_json'])['private_original']
    raw, = pq.ParquetFile(queue.root/reference['path']).read().to_pylist()
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    queue.close()
    entry.update(embedded_article_layout=next_layout,adapter_verification=next_verification)
    queue = NewsQueue(tmp_path/'news',PolicyRegistry(settings,base=repo))
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone()) == before
    current = queue.registry.sources['knews']
    article = extract_article(raw['raw_response_body'],current,page_url=URL)
    item = dict(url=URL,request_id=requested['request_id'],diagnostic_path=reference['path'],
        diagnostic_receipt_path=reference['receipt_path'],diagnostic_sha256=reference['sha256'],
        diagnostic_receipt_sha256=sha256_file(queue.root/reference['receipt_path']),
        response_sha256=reference['response_sha256'],response_bytes=reference['response_bytes'],
        captured_at_utc=raw['observed_at_utc'],candidate_job_sha256=_article_digest(completed),
        article_keys=sorted(article),article_fields_sha256=_article_digest(article),
        expected_missing_head={k:before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{key:article[key] for key in ('body_sha256','authors_json','published_at_raw','modified_at_raw')})
    report = tmp_path/'review'/'observed.json'
    atomic_write_json(report,dict(private_originals=[dict(request_id=requested['request_id'],url=URL,final_url=URL,
        reference=reference,receipt_sha256=item['diagnostic_receipt_sha256'],accepted_as_body=False)]),durable=True)
    manifest = dict(contract_version=2,scope_kind='reviewed_resident_native_materialization_v2',provider='knews',
        data_fingerprint=current.data_fingerprint,authorization_fingerprint=current.authorization_fingerprint,
        approved_at_utc=utc_now(),diagnostic_scope_unchanged=True,parser_transition_reviewed=True,
        publish=False,remote_training_export=False,observation_report=report.name,
        observation_report_sha256=sha256_file(report),entries=[item])
    approval = report.parent/'approved.json'; atomic_write_json(approval,manifest,durable=True)
    try:
        yield queue,approval,manifest,reference,raw,completed
    finally:
        queue.close()


def test_reuse_keeps_original_clock_and_does_not_repeat_http_or_completed_body(retained_original, monkeypatch):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue,approval,manifest,reference,raw,closed = retained_original
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a,**k:pytest.fail('reuse must not create another HTTP owner'))
    first = materialize_reviewed_native(queue,[URL],approval,lease_seconds=300)
    assert first['materialized_bodies']==1 and first['publisher_requests_this_command']==0, first
    head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    record, = pq.ParquetFile(queue.root/head['parquet_path']).read().to_pylist()
    assert record['raw_response_body']==raw['raw_response_body']
    assert record['captured_at_utc']==record['source_observed_at_utc']==raw['observed_at_utc']
    assert not record['historical_point_in_time'] and record['original_diagnostic_authorization_json'] is None
    assert record['body_sha256']==manifest['entries'][0]['body_sha256']
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone())==closed
    assert materialize_reviewed_native(queue,[URL],approval,lease_seconds=300)['materialized_bodies']==0
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())==head


@pytest.mark.parametrize('case',['head','receipt','source'])
def test_reuse_rejects_changed_missing_heads_receipts_and_source_scope(retained_original, case):
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    queue,approval,_,reference,_,_ = retained_original
    if case=='head':
        with queue.db: queue.db.execute("UPDATE articles SET lease_owner='other' WHERE url=?",(URL,))
    elif case=='receipt':
        (queue.root/reference['receipt_path']).write_bytes(b'{}')
    else:
        queue.registry.sources['knews'] = replace(queue.registry.sources['knews'],authorization_fingerprint='a'*64)
    if case=='head':
        result = materialize_reviewed_native(queue,[URL],approval,lease_seconds=300)
        assert result['materialized_bodies']==0 and result['skipped_urls']==[URL]
    else:
        with pytest.raises(ValueError): materialize_reviewed_native(queue,[URL],approval,lease_seconds=300)
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
