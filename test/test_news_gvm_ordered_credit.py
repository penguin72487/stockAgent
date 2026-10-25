"""Witnessed ordered two-author headers and zero-request original adoption."""
from copy import deepcopy
import json

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_gvm import GVM_ORDERED_CREDIT_LAYOUT, GVM_V11_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
from downloader.news_fulltext_retained_native import _article_digest, materialize_reviewed_native
from test_news_fulltext import config
from test_news_gvm_header_evidence import header_owner
from test_news_gvm_media import media_page
from test_news_gvm_multiple_credit_evidence import multiple_owner, multiple_page, submit
from test_news_gvm_native import ORIGIN, URL, policy, schema
from test_news_gvm_originals import diagnostic, head, http_stub, owner, response
from test_news_gvm_presentation import presentation_entry, special_page, title_page


def ordered_entry():
    return {**presentation_entry(), 'embedded_article_layout': GVM_ORDERED_CREDIT_LAYOUT,
        'adapter_verification': GVM_V11_VERIFICATION}


def ordered_page():
    soup = multiple_page()
    soup.select_one('.article-head')['author'] = ','.join(n.get_text(strip=True) for n in soup.select('.article-head_author'))
    return soup


def test_two_ordered_credits_preserve_each_name_profile_native_declaration_and_body():
    raw = str(ordered_page()).encode()
    with pytest.raises(NewsError):
        extract_article(raw, policy(presentation_entry()), page_url=URL)
    article = extract_article(raw, policy(ordered_entry()), page_url=URL)
    credits = ordered_page().select('.article-head_author')
    names, profiles = [n.get_text(strip=True) for n in credits], [n['href'] for n in credits]
    assert json.loads(article['authors_json']) == names
    detail = json.loads(article['author_attribution_detail_json'])
    assert detail['source_visible_credits'] == names and detail['literal_profile_urls'] == profiles
    assert detail['source_native_author_raw'] == ','.join(names) and detail['source_native_author_ID_raw'] == '6813,777'
    assert detail['ordered_visible_native_and_schema_credits_agree']
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert 'source_visible_credit' not in detail and 'literal_profile_url' not in detail
    assert json.loads(article['verified_article_notices_json'])['native_two_ordered_comma_joined_credit_header_schema_verified']
    assert article['body'] == extract_article(str(media_page('')).encode(), policy(presentation_entry()), page_url=URL)['body']


@pytest.mark.parametrize('change', ['name_separator', 'ID_separator', 'native_name_order', 'native_ID_order',
    'schema_order', 'schema_name', 'missing', 'third', 'extra_credit_node', 'hidden', 'hidden_parent',
    'duplicate_profile', 'duplicate_name', 'profile', 'advertorial', 'external', 'reprint', 'clock', 'nonfree'])
def test_two_author_support_keeps_scope_visibility_identity_order_and_clocks(change):
    soup = ordered_page(); node = soup.select_one('.article-head'); credits = soup.select('.article-head_author')
    if change == 'name_separator': node['author'] = node['author'].replace(',', '、')
    elif change == 'ID_separator': node['author_id'] = '6813, 777'
    elif change == 'native_name_order': node['author'] = ','.join(n.get_text(strip=True) for n in reversed(credits))
    elif change == 'native_ID_order': node['author_id'] = '777,6813'
    elif change == 'schema_order': schema(soup, lambda a: a['author'].reverse())
    elif change == 'schema_name': schema(soup, lambda a: a['author'][1].update(name='另一個宣告'))
    elif change == 'missing': credits[1].decompose()
    elif change == 'third': credits[1].parent.append(deepcopy(credits[1]))
    elif change == 'extra_credit_node':
        extra = soup.new_tag('span'); extra['class'] = 'article-head_author'; extra.string = '未證明署名'; node.append(extra)
    elif change == 'hidden': credits[1]['hidden'] = ''
    elif change == 'hidden_parent': credits[1].parent['style'] = 'display:none'
    elif change == 'duplicate_profile': credits[1]['href'] = credits[0]['href']
    elif change == 'duplicate_name': credits[1].string = credits[0].get_text(strip=True)
    elif change == 'profile': credits[1]['href'] = 'https://example.invalid/author/777'
    elif change == 'advertorial': node['article_type'] = '廣編'
    elif change == 'external':
        credits[1].string = '中央社'; node['author'] = credits[0].get_text(strip=True) + ',中央社'
        schema(soup, lambda a: a['author'][1].update(name='中央社'))
    elif change == 'reprint': soup.select_one('.article-content').append('本文轉載自中央社')
    elif change == 'clock': soup.select_one('meta[property="article:published_time"]')['content'] = 'invalid'
    elif change == 'nonfree': schema(soup, lambda a: a.update(isAccessibleForFree=False))
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), policy(ordered_entry()), page_url=URL)


@pytest.mark.parametrize('soup', [media_page(''), title_page(), special_page()])
def test_existing_single_credit_articles_keep_every_field_except_named_parser_version(soup):
    raw = str(soup).encode()
    old = extract_article(raw, policy(presentation_entry()), page_url=URL)
    new = extract_article(raw, policy(ordered_entry()), page_url=URL)
    assert {k: v for k, v in old.items() if k != 'article_identity_basis'} == {k: v for k, v in new.items() if k != 'article_identity_basis'}


def test_source_transition_keeps_exact_predecessor_authorization_and_HTTP_capacity():
    old, new = policy(presentation_entry()), policy(ordered_entry())
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint and new.authorization == old.authorization
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.interval_seconds == old.interval_seconds and new.parallelism == old.parallelism


def table_page():
    soup = ordered_page()
    table = soup.new_tag('table'); row = soup.new_tag('tr'); table.append(row)
    for value in ('合成欄位', '合成數值'):
        cell = soup.new_tag('td', valign='top', width='192'); cell.string = value; row.append(cell)
    soup.select_one('.article-content').append(table)
    return soup


def test_observed_top_aligned_table_cells_retain_all_text_and_order():
    soup = table_page(); article = extract_article(str(soup).encode(), policy(ordered_entry()), page_url=URL)
    assert article['body'].endswith('合成欄位\n合成數值')
    assert json.loads(article['verified_article_notices_json'])['native_visible_table_cell_top_alignment_verified']


@pytest.mark.parametrize('change', ['unknown_alignment', 'hidden_cell', 'hidden_table', 'event', 'bad_width', 'unknown_node'])
def test_cell_presentation_does_not_skip_hidden_or_unknown_table_content(change):
    soup = table_page(); cell = soup.select_one('td')
    if change == 'unknown_alignment': cell['valign'] = 'middle'
    elif change == 'hidden_cell': cell['hidden'] = ''
    elif change == 'hidden_table': soup.select_one('table')['style'] = 'display:none'
    elif change == 'event': cell['onclick'] = 'run()'
    elif change == 'bad_width': cell['width'] = 'unverified'
    else: cell.append(soup.new_tag('object'))
    with pytest.raises(NewsError): extract_article(str(soup).encode(), policy(ordered_entry()), page_url=URL)


def test_startup_does_not_bulk_rearm_other_failed_or_delayed_heads(config, tmp_path):
    settings = deepcopy(config); settings['providers'] = [presentation_entry()]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        for i, state in enumerate(('body_not_found', 'article_identity_not_verified', 'http_429', 'paywall_or_login', 'article_rights_or_scope_excluded')):
            url = ORIGIN + '/article/' + str(82000 + i); queue.add_articles([{'url': url}])
            with queue.db:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=8,due=9999999999 WHERE url=?', (state, state, url))
        before = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    finally: queue.close()
    settings['providers'] = [ordered_entry()]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try: assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
    finally: queue.close()


@pytest.fixture
def retained_ordered(multiple_owner, tmp_path, monkeypatch):
    queue, settings, _, _ = multiple_owner
    old = queue.registry.sources['gvm']
    submitted = submit(queue, settings); diagnostic.pump_candidates(queue)
    http = http_stub(response(ordered_page())); job = diagnostic.claim_candidate(queue, http, settings, 'gvm')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    completed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (submitted['request_id'],)).fetchone())
    ref = json.loads(completed['result_json'])['private_original']; before = head(queue)
    raw, = pq.ParquetFile(queue.root/ref['path']).read(use_threads=False).to_pylist()
    settings['providers'] = [ordered_entry()]; queue.registry = PolicyRegistry(settings, base=tmp_path)
    source = queue.registry.sources['gvm']
    monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V10_ORIGINAL_BINDING', (old.data_fingerprint, old.authorization_fingerprint))
    article = extract_article(raw['raw_response_body'], source, page_url=URL)
    entry = {'url': URL, 'request_id': submitted['request_id'], 'diagnostic_path': ref['path'],
        'diagnostic_receipt_path': ref['receipt_path'], 'diagnostic_sha256': ref['sha256'],
        'diagnostic_receipt_sha256': sha256_file(queue.root/ref['receipt_path']),
        'response_sha256': ref['response_sha256'], 'response_bytes': ref['response_bytes'],
        'captured_at_utc': raw['observed_at_utc'], 'candidate_job_sha256': _article_digest(completed),
        'article_keys': sorted(article), 'article_fields_sha256': _article_digest(article),
        'expected_missing_head': {k: before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{k: article[k] for k in ('body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw')}}
    report = tmp_path/'review'/'observed.json'
    atomic_write_json(report, {'private_originals': [{'request_id': submitted['request_id'], 'url': URL, 'final_url': URL,
        'reference': ref, 'receipt_sha256': entry['diagnostic_receipt_sha256'], 'accepted_as_body': False}]})
    approval = {'contract_version': 2, 'scope_kind': 'reviewed_resident_native_materialization_v2', 'provider': 'gvm',
        'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
        'approved_at_utc': utc_now(), 'diagnostic_scope_unchanged': True, 'parser_transition_reviewed': True,
        'publish': False, 'remote_training_export': False, 'observation_report': report.name,
        'observation_report_sha256': sha256_file(report), 'entries': [entry]}
    path = report.parent/'approval.json'; atomic_write_json(path, approval)
    return queue, path, raw


def test_two_author_original_adoption_is_durable_idempotent_and_keeps_transport_and_clock(retained_ordered, monkeypatch):
    queue, path, raw = retained_ordered
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('reuse observed original'))
    before = [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies'] == 1
    saved, = pq.ParquetFile(queue.root/head(queue)['parquet_path']).read(use_threads=False).to_pylist()
    assert len(json.loads(saved['authors_json'])) == 2 and saved['raw_response_body'] == raw['raw_response_body']
    assert saved['captured_at_utc'] == saved['source_observed_at_utc'] == raw['observed_at_utc']
    assert saved['original_transport_redirects_stored'] is False and saved['http_duration_seconds'] is None
    assert not saved['historical_point_in_time'] and not saved['publish'] and not saved['remote_training_export']
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies'] == 0
    assert before == [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    root, registry = queue.root, queue.registry; queue.close()
    reopened = NewsQueue(root, registry)
    try: assert head(reopened)['state'] == 'complete' and head(reopened)['body_sha256'] == saved['body_sha256']
    finally: reopened.close()


@pytest.mark.parametrize('change', ['binding', 'auth', 'newer_head', 'lease'])
def test_two_author_replay_does_not_accept_a_changed_capture_scope_or_head(retained_ordered, monkeypatch, change):
    queue, path, _ = retained_ordered
    if change == 'binding': monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V10_ORIGINAL_BINDING', ('a'*64, 'b'*64))
    elif change == 'auth':
        from dataclasses import replace
        source = queue.registry.sources['gvm']; queue.registry.sources['gvm'] = replace(source, authorization_fingerprint='b'*64)
    elif change == 'newer_head':
        with queue.db: queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    else:
        with queue.db: queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=9999999999 WHERE url=?", (URL,))
    try: result = materialize_reviewed_native(queue, [URL], path, lease_seconds=300)
    except ValueError: pass
    else: assert result['materialized_bodies'] == 0
    assert not head(queue)['body_sha256']
