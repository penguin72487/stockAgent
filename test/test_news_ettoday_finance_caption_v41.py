"""Source-proved finance prelude, preserving raw prose and once-only originals."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
from downloader.news_fulltext_retained_native import _article_digest, materialize_reviewed_native
from downloader.news_fulltext_ettoday import ETTODAY_V41_HTTP_BINDING
import test_news_ettoday as native
import test_news_ettoday_caption_original as original


def source(parser='lxml', version=41):
    entry = native.recipe(parser); entry['embedded_article_layout'] = f'ettoday_visible_article_v{version}'
    return PolicyRegistry(dict(contract_version=1, collection_purpose='personal_research',
        publish=False, remote_training_export=False, providers=[entry]), base='.').sources['ettoday']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_full_prose_raw_values_caption_and_reporter_are_separate(parser):
    soup = original.page(); unchanged = str(soup); raw = unchanged.encode()
    article = extract_article(raw, source(parser), page_url=original.URL)
    assert article['body'] == native.PROSE+'\n原始小標題\n'+native.NOTICE
    assert article['authors_json'] == json.dumps(['研究記者'], ensure_ascii=False)
    assert article['published_at_raw'] == native.PUB and article['modified_at_raw'] == native.MOD
    assert json.loads(article['verified_image_captions_json']) == [original.CAPTION]
    assert original.CAPTION not in article['body'] and original.SIGNATURE not in article['body']
    notice, = [n for n in json.loads(article['verified_article_notices_json'])
        if n['kind'] == 'native_finance_image_caption_only_before_separate_reporter']
    assert notice['reporter_is_following_plain_native_paragraph'] and not notice['source_values_rewritten']
    assert not article['historical_point_in_time'] and not article['per_article_license_verified']
    assert str(soup) == unchanged
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(raw, source(parser, 40), page_url=original.URL)


def test_new_finance_complete_result_is_identical_under_both_html_engines():
    raw = original.response().body
    assert extract_article(raw, source(), page_url=original.URL) == extract_article(
        raw, source('html.parser'), page_url=original.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('mutation', ['extra_prose', 'missing_image', 'hidden_credit', 'caption_markup',
    'missing_reporter', 'foreign_person', 'schema_headline', 'foreign_canonical', 'duplicate_schema', 'no_prose', 'unknown_script'])
def test_new_caption_geometry_never_discards_unmarked_prose_or_fills_missing_identity(parser, mutation):
    soup = original.page(); first = soup.select_one('.story > p')
    if mutation == 'extra_prose': first.append('這是正文，不能刪除')
    elif mutation == 'missing_image': first.img.decompose()
    elif mutation == 'hidden_credit': soup.select_one('#credit')['hidden'] = ''
    elif mutation == 'caption_markup': first.strong.string.wrap(soup.new_tag('span'))
    elif mutation == 'missing_reporter': soup.select_one('#credit').decompose()
    elif mutation == 'foreign_person': native.update_schema(soup, author={'@type': 'Person', 'name': '其他作者'})
    elif mutation == 'schema_headline': native.update_schema(soup, headline='其他新聞標題')
    elif mutation == 'foreign_canonical': soup.select_one('link[rel=canonical]')['href'] = original.URL+'0'
    elif mutation == 'duplicate_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif mutation == 'no_prose':
        for node in list(soup.select_one('#credit').find_next_siblings()): node.decompose()
    else:
        script = soup.new_tag('script'); script.string = 'unknown'; first.append(script)
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), source(parser), page_url=original.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_already_supported_originals_keep_every_parsed_field(parser, host):
    raw = str(native.page(host, parser)).encode()
    url = native.URLS[host == 'www']
    assert extract_article(raw, source(parser), page_url=url) == extract_article(
        raw, source(parser, 40), page_url=url)


def test_finance_prelude_does_not_enable_www_or_house_newsroom_fallbacks():
    soup = original.page()
    native.update_schema(soup, url=native.URLS[1], mainEntityOfPage=native.URLS[1])
    for selector, attribute in [('link[rel=canonical]', 'href'), ('meta[property="og:url"]', 'content')]:
        soup.select_one(selector)[attribute] = native.URLS[1]
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), source(), page_url=native.URLS[1])
    soup = original.page(); soup.select_one('#credit').string = '房產中心／綜合報導'
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), source(), page_url=original.URL)


@pytest.fixture
def approved(tmp_path):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    cfg['providers'] = [deepcopy(p) for p in cfg['providers'] if p['id'] == 'ettoday']
    cfg['providers'][0]['embedded_article_layout'] = 'ettoday_visible_article_v40'
    cfg.update(root=str(tmp_path/'news'), min_free_disk_bytes=1, exclusive_http_owner=False)
    cfg['adaptive_rate'] = {'enabled': False}
    old = NewsQueue(tmp_path/'news', PolicyRegistry(cfg, base='.'))
    old.add_articles([dict(url=original.URL)])
    with old.db:
        old.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=0,last_checked_at_utc=?',
            ('article_attribution_not_verified', 'article_attribution_not_verified', '2026-01-01T00:00:00+00:00'))
    handle = original.submit(old, cfg)
    http = original.http_stub(old); job = original.claim(old, cfg, http)
    original.diagnostic.finish_candidate(old, job, result=original.diagnostic.fetch_candidate(http, job))
    completed = dict(old.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (handle['request_id'],)).fetchone())
    reference = json.loads(completed['result_json'])['private_original']
    raw, = pq.ParquetFile(old.root/reference['path']).read().to_pylist()
    before = original.head(old); old.close()
    cfg['providers'][0]['embedded_article_layout'] = 'ettoday_visible_article_v41'
    queue = NewsQueue(tmp_path/'news', PolicyRegistry(cfg, base='.'))
    current = queue.registry.sources['ettoday']
    binding = ETTODAY_V41_HTTP_BINDING
    assert (current.parser_repair_prior_capture_fingerprint, current.data_fingerprint,
        current.authorization_fingerprint, current.http_capacity_source_fingerprint,
        current.http_capacity_authorization_fingerprint) == binding
    assert original.head(queue) == before
    article = extract_article(raw['raw_response_body'], current, page_url=original.URL)
    item = dict(url=original.URL, request_id=handle['request_id'], diagnostic_path=reference['path'],
        diagnostic_receipt_path=reference['receipt_path'], diagnostic_sha256=reference['sha256'],
        diagnostic_receipt_sha256=sha256_file(queue.root/reference['receipt_path']),
        response_sha256=reference['response_sha256'], response_bytes=reference['response_bytes'],
        captured_at_utc=raw['observed_at_utc'], candidate_job_sha256=_article_digest(completed),
        article_keys=sorted(article), article_fields_sha256=_article_digest(article),
        expected_missing_head={k:before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{k:article[k] for k in ('body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw')})
    folder = tmp_path/'review'; folder.mkdir()
    report = folder/'original.json'
    atomic_write_json(report, dict(private_originals=[dict(request_id=handle['request_id'], url=original.URL,
        final_url=original.URL, reference=reference, receipt_sha256=item['diagnostic_receipt_sha256'], accepted_as_body=False)]))
    approval = dict(contract_version=2, scope_kind='reviewed_resident_native_materialization_v2',
        provider='ettoday', data_fingerprint=current.data_fingerprint, authorization_fingerprint=current.authorization_fingerprint,
        approved_at_utc=utc_now(), diagnostic_scope_unchanged=True, parser_transition_reviewed=True,
        publish=False, remote_training_export=False, observation_report=report.name,
        observation_report_sha256=sha256_file(report), entries=[item])
    path = folder/'approved.json'; atomic_write_json(path, approval)
    yield queue, path, approval, reference, raw, completed
    queue.close()


def test_same_once_only_original_enters_canonical_acceptance_with_no_new_http(approved, monkeypatch):
    queue, path, approval, reference, raw, completed = approved
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('Retained acceptance has no HTTP owner'))
    proof = materialize_reviewed_native(queue, [original.URL], path, lease_seconds=60)
    assert proof['materialized_bodies'] == 1 and proof['publisher_requests_this_command'] == 0
    head = original.head(queue)
    assert head['body_sha256'] == approval['entries'][0]['body_sha256']
    record, = pq.ParquetFile(queue.root/head['parquet_path']).read().to_pylist()
    assert record['raw_response_body'] == raw['raw_response_body']
    assert record['captured_at_utc'] == raw['observed_at_utc']
    assert record['original_transport_redirects_stored'] is True
    assert record['published_at_raw'] == native.PUB and record['modified_at_raw'] == native.MOD
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone()) == completed
    assert sha256_file(queue.root/reference['path']) == reference['sha256']


@pytest.mark.parametrize('mutation', ['head', 'permission', 'receipt', 'raw'])
def test_retained_materialization_cannot_bypass_changed_head_scope_or_original(approved, mutation):
    queue, path, approval, reference, raw, completed = approved
    if mutation == 'head':
        with queue.db: queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403'")
    elif mutation == 'permission':
        approval['remote_training_export'] = True; atomic_write_json(path, approval)
    elif mutation == 'receipt':
        receipt = queue.root/reference['receipt_path']; payload = json.loads(receipt.read_bytes())
        payload['permit']['printed_signature_sha256'] = '0'*64; atomic_write_json(receipt, payload)
    else:
        original_path = queue.root/reference['path']
        original_path.write_bytes(original_path.read_bytes()+b'changed')
    if mutation == 'head':
        proof = materialize_reviewed_native(queue, [original.URL], path, lease_seconds=60)
        assert proof['materialized_bodies'] == 0 and proof['skipped_urls']
        assert original.head(queue)['state'] == 'http_403'
    else:
        with pytest.raises(ValueError): materialize_reviewed_native(queue, [original.URL], path, lease_seconds=60)
    assert original.head(queue)['body_sha256'] is None
