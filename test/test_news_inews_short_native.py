"""Complete native prose, rather than a length cutoff, defines a short story."""
import json
import time

import pytest
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_retained_native import materialize_reviewed_native, _article_digest
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_queue import utc_now
from test_news_inews_short_body import (
    SHORT, short_page, credit_page, owner, complete_short_question, submit_credit, response, head, http_stub,
)
from test_news_setn_inews import URL, NAME, CLOCK, CAPTION, TEXT, page, policy, v8_entry


def v9_entry():
    entry = v8_entry()
    entry['embedded_article_layout'] = 'setn_inews_native_article_v9'
    return entry


def extract(soup):
    return extract_article(str(soup).encode(),policy(v9_entry()),page_url=URL)


@pytest.mark.parametrize('text', [SHORT, '原始值-5與2.3%保留。', '文'*99, '文'*100])
def test_literal_complete_native_short_prose_keeps_all_values_and_unresolved_clock(text):
    soup = short_page(text)
    article = extract(soup)
    assert article['body']==text and article['title'] and json.loads(article['authors_json'])==[NAME]
    assert article['published_at_raw']==CLOCK and article['modified_at_raw'] is None
    notices = json.loads(article['verified_article_notices_json'])
    assert notices['exact_PIT_eligible'] is False
    if len(text)<100:
        assert notices['verified_native_short_written_prose']['characters']==len(text)
        assert notices['verified_native_short_written_prose']['printed_signature_or_photo_captions_counted_as_prose'] is False
    else:
        assert 'verified_native_short_written_prose' not in notices


def test_short_native_proof_preserves_the_nontrivial_text_check():
    with pytest.raises(NewsError,match='body_not_found'):
        extract(short_page('x'*99))


def test_v8_minimum_remains_versioned_and_short_configured_body_is_not_a_blanket_switch():
    soup = short_page()
    with pytest.raises(NewsError,match='body_not_found'):
        extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)
    source = policy(v9_entry())
    assert not source.allow_short_configured_body and source.require_author_attribution
    assert extract(soup)['body']==SHORT


def test_printed_signature_is_preserved_but_cannot_substitute_for_actual_news_prose():
    soup = credit_page('printed')
    article = extract(soup)
    assert article['body']=='財經中心／合成人報導\n\n'+SHORT
    notices = json.loads(article['verified_article_notices_json'])
    assert notices['verified_native_short_written_prose']['characters']==len(SHORT)
    soup.select_one('[itemprop=articleBody] > p:last-child').decompose()
    with pytest.raises(NewsError,match='body_not_found'):
        extract(soup)


@pytest.mark.parametrize('kind', ['empty','photo_only','credit_and_photo_only'])
def test_empty_or_caption_only_editors_are_not_promoted_to_short_articles(kind):
    soup = page()
    body = soup.select_one('[itemprop=articleBody]')
    if kind=='empty':
        body.clear()
    elif kind=='photo_only':
        for paragraph in body.find_all('p',recursive=False): paragraph.decompose()
    else:
        soup.select_one('.author_wrap').clear()
        for paragraph in body.find_all('p',recursive=False): paragraph.decompose()
        paragraph = soup.new_tag('p'); paragraph.string='財經中心／合成人報導'; body.insert(0,paragraph)
    with pytest.raises(NewsError,match='body_not_found'):
        extract(soup)


@pytest.mark.parametrize('mutation', ['credit','clock','canonical','hidden','unknown_child','editor_without_review'])
def test_short_prose_still_needs_native_identity_credit_clock_and_whole_editor(mutation):
    soup = short_page()
    if mutation=='credit': soup.select_one('.author_wrap').clear()
    elif mutation=='clock': soup.select_one('time').string='2026/02/30 25:00'
    elif mutation=='canonical': soup.select_one('link')['href']='/news/999999'
    elif mutation=='hidden': soup.select_one('[itemprop=articleBody]')['hidden']=''
    elif mutation=='unknown_child': soup.select_one('[itemprop=articleBody]').append(soup.new_tag('table'))
    else: soup.select_one('.author_wrap').contents[0].replace_with('編輯 ')
    with pytest.raises(NewsError): extract(soup)


def test_existing_long_native_body_attribution_and_clocks_match_v8():
    soup = page()
    old = extract_article(str(soup).encode(),policy(v8_entry()),page_url=URL)
    new = extract(soup)
    for key in ('body','title','authors_json','author_attribution_detail_json','verified_article_notices_json',
                'published_at_raw','published_time_basis','modified_at_raw','declared_canonical_url'):
        assert new.get(key)==old.get(key),key
    assert TEXT in new['body'] and CAPTION in new['body']


def v10_entry():
    return {**v9_entry(),'embedded_article_layout':'setn_inews_native_article_v10'}


def v11_entry():
    return {**v10_entry(),'embedded_article_layout':'setn_inews_native_article_v11'}


@pytest.mark.parametrize('suffix',['','】'])
def test_collective_desk_credit_preserves_the_source_typo_without_inventing_a_person(suffix):
    soup=page();soup.select_one('.author_wrap').clear()
    signature='財經中心／綜合報導'+suffix
    soup.select_one('[itemprop=articleBody] > p').string=signature
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract_article(str(soup).encode(),policy(v10_entry()),page_url=URL)
    article=extract_article(str(soup).encode(),policy(v11_entry()),page_url=URL)
    detail=json.loads(article['author_attribution_detail_json'])
    assert json.loads(article['authors_json'])==['財經中心']
    assert article['body'].startswith(signature) and article['body'].count(signature)==1
    assert detail['source_declared_author_type']=='Organization'
    assert detail['source_declared_person_name'] is None and detail['source_declared_profile_url'] is None
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert detail['source_visible_signature_raw']==signature
    assert article['published_at_raw']==CLOCK


@pytest.mark.parametrize('signature',['圖、文／CTWANT','圖、文／鏡週刊','財經中心／整理報導',
    '財經中心／綜合報導 extra prose','國際中心／綜合報導','財經中心／綜合報導】】'])
def test_reviewed_collective_desk_does_not_admit_third_party_or_unknown_credits(signature):
    soup=page();soup.select_one('.author_wrap').clear()
    soup.select_one('[itemprop=articleBody] > p').string=signature
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract_article(str(soup).encode(),policy(v11_entry()),page_url=URL)


def threads_page():
    from bs4 import BeautifulSoup
    soup=page();body=soup.select_one('[itemprop=articleBody]')
    widget=BeautifulSoup('''<div class="embedFrame"><blockquote class="text-post-media"
      data-text-post-permalink="https://www.threads.com/@synthetic/post/AbCdEf12345"
      data-text-post-version="0" id="ig-tp-AbCdEf12345" style="background:white">
      <div style="padding:40px"><div style="height:32px">&nbsp;</div><div style="font-size:15px">
      <a href="https://www.threads.com/@synthetic/post/AbCdEf12345" style="background:white"
      target="_blank">在 Threads 查看</a></div></div></blockquote>
      <script async="" src="https://www.threads.com/embed.js"></script></div>''','lxml').select_one('.embedFrame')
    body.append(widget)
    return soup


def test_static_threads_link_keeps_the_complete_written_story_without_a_transcript():
    soup=threads_page();before=str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(before.encode(),policy(v10_entry()),page_url=URL)
    article=extract_article(before.encode(),policy(v11_entry()),page_url=URL)
    ordinary=extract_article(str(page()).encode(),policy(v10_entry()),page_url=URL)
    assert article['body']==ordinary['body'] and article['authors_json']==ordinary['authors_json']
    assert article['published_at_raw']==ordinary['published_at_raw']
    assert '在 Threads 查看' not in article['body'] and str(soup)==before
    marker,=json.loads(article['verified_article_notices_json'])['verified_native_static_social_placeholders']
    assert marker['source_permalink']=='https://www.threads.com/@synthetic/post/AbCdEf12345'
    assert not marker['written_post_content_present'] and not marker['post_transcript_invented']
    assert not marker['media_or_script_requested_or_executed']


@pytest.mark.parametrize('mutation',['extra_prose','hidden','script_inline','script_host','quote_id',
    'unmatched_link','unknown_label','additional_link','iframe','wrapper_event'])
def test_changed_social_widget_never_silently_discards_written_or_unknown_content(mutation):
    soup=threads_page();widget=soup.select_one('.embedFrame');link=widget.select_one('a')
    if mutation=='extra_prose':widget.select_one('blockquote > div > div').string='不能漏掉的原始貼文'
    elif mutation=='hidden':widget['hidden']=''
    elif mutation=='script_inline':widget.select_one('script').string='unknown()'
    elif mutation=='script_host':widget.select_one('script')['src']='https://example.invalid/embed.js'
    elif mutation=='quote_id':widget.select_one('blockquote')['id']='ig-tp-Different123'
    elif mutation=='unmatched_link':link['href']='https://www.threads.com/@synthetic/post/Different123'
    elif mutation=='unknown_label':link.string='真正的貼文內文'
    elif mutation=='additional_link':link.insert_after(soup.new_tag('a'))
    elif mutation=='iframe':widget.append(soup.new_tag('iframe'))
    else:widget['onclick']='unknown()'
    with pytest.raises(NewsError):extract_article(str(soup).encode(),policy(v11_entry()),page_url=URL)


@pytest.mark.parametrize('factory',[page,short_page,lambda:credit_page('printed'),lambda:credit_page('editor')])
def test_v11_preserves_all_prior_v10_article_fields_except_the_named_version(factory):
    soup=factory();payload=str(soup).encode()
    old=extract_article(payload,policy(v10_entry()),page_url=URL)
    new=extract_article(payload,policy(v11_entry()),page_url=URL)
    assert {k:v for k,v in old.items() if k!='article_identity_basis'}=={k:v for k,v in new.items() if k!='article_identity_basis'}


def test_collective_parser_keeps_only_the_exact_reviewed_http_identity(monkeypatch):
    entry=v11_entry();source=policy(entry)
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V11_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    assert policy(entry).http_capacity_source_fingerprint=='a'*64
    assert policy(entry).http_capacity_authorization_fingerprint=='b'*64
    assert not policy({**entry,'adapter_verification':'unreviewed transition'}).http_capacity_source_fingerprint


def test_editor_parser_transition_preserves_only_the_exact_reviewed_http_bucket(monkeypatch):
    entry=v10_entry();source=policy(entry)
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V10_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    bound=policy(entry)
    assert bound.http_capacity_source_fingerprint=='a'*64
    assert bound.http_capacity_authorization_fingerprint=='b'*64
    changed={**entry,'adapter_verification':'different_unreviewed_parser'}
    assert not policy(changed).http_capacity_source_fingerprint
    assert not policy(changed).http_capacity_authorization_fingerprint


@pytest.mark.parametrize('short', [True,False])
def test_native_named_editor_is_a_source_declared_role_not_a_reporter_alias(short):
    soup=short_page() if short else page()
    soup.select_one('.author_wrap').contents[0].replace_with('編輯 ')
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract(soup)
    article=extract_article(str(soup).encode(),policy(v10_entry()),page_url=URL)
    detail=json.loads(article['author_attribution_detail_json'])
    assert json.loads(article['authors_json'])==[NAME]
    assert detail['source_visible_signature_raw'].startswith('編輯 '+NAME+' ')
    assert detail['source_credit_role']=='native_visible_named_editor_signature'
    assert detail['source_declared_credit_role']=='編輯' and not detail['human_editor_identity_verified']
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert article['published_at_raw']==CLOCK and article['modified_at_raw'] is None
    if short:assert article['body']==SHORT


@pytest.mark.parametrize('mutation',['role','name','profile','hidden','nested','title','clock'])
def test_editor_role_does_not_bypass_native_credit_identity_or_clock(mutation):
    soup=credit_page('editor');link=soup.select_one('.author_wrap > a')
    if mutation=='role':soup.select_one('.author_wrap').contents[0].replace_with('來源 ')
    elif mutation=='name':link.string='Unknown'
    elif mutation=='profile':link['href']='https://example.invalid/author/unknown'
    elif mutation=='hidden':link['hidden']=''
    elif mutation=='nested':link.append(soup.new_tag('span'))
    elif mutation=='title':soup.select_one('[itemprop=headline]').string='另一個標題'
    else:soup.select_one('time').string='2026/02/30 25:00'
    with pytest.raises(NewsError):extract_article(str(soup).encode(),policy(v10_entry()),page_url=URL)


@pytest.mark.parametrize('soup_factory',[page,short_page,lambda:credit_page('printed')])
def test_reporter_and_printed_short_articles_preserve_every_existing_v9_field(soup_factory):
    soup=soup_factory();old=extract(soup)
    new=extract_article(str(soup).encode(),policy(v10_entry()),page_url=URL)
    assert {k:v for k,v in old.items() if k!='article_identity_basis'}=={k:v for k,v in new.items() if k!='article_identity_basis'}


@pytest.fixture
def retained(owner, tmp_path, monkeypatch):
    queue, config = owner
    soup = credit_page('printed')
    complete_short_question(queue, config, soup)
    submitted = submit_credit(queue, config); diagnostic.pump_candidates(queue)
    http = http_stub(queue, response(soup))
    job = diagnostic.claim_candidate(queue, http, config, 'setn_inews_scope')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    completed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (submitted['request_id'],)).fetchone())
    ref = json.loads(completed['result_json'])['private_original']
    raw, = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    before = head(queue)
    old_source = queue.registry.sources['setn_inews_scope']
    config['providers'][0]['embedded_article_layout'] = 'setn_inews_native_article_v9'
    from downloader.news_fulltext_policy import PolicyRegistry
    queue.registry = PolicyRegistry(config, base=tmp_path)
    source = queue.registry.sources['setn_inews_scope']
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V8_HTTP_BINDING',
        (old_source.data_fingerprint, old_source.authorization_fingerprint))
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V9_HTTP_BINDING',
        (source.data_fingerprint, source.authorization_fingerprint))
    article = extract_article(raw['raw_response_body'], source, page_url=URL)
    entry = {'url': URL, 'request_id': submitted['request_id'],
        'diagnostic_path': ref['path'], 'diagnostic_receipt_path': ref['receipt_path'],
        'diagnostic_sha256': ref['sha256'], 'diagnostic_receipt_sha256': sha256_file(queue.root/ref['receipt_path']),
        'response_sha256': ref['response_sha256'], 'response_bytes': ref['response_bytes'],
        'captured_at_utc': raw['observed_at_utc'], 'candidate_job_sha256': _article_digest(completed),
        'article_keys': sorted(article), 'article_fields_sha256': _article_digest(article),
        'expected_missing_head': {k: before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{k: article[k] for k in ('body_sha256','authors_json','published_at_raw','modified_at_raw')}}
    report = tmp_path/'review'/'observed.json'
    atomic_write_json(report, {'private_originals':[{'request_id':submitted['request_id'], 'url':URL,
        'final_url':URL, 'reference':ref, 'receipt_sha256':entry['diagnostic_receipt_sha256'], 'accepted_as_body':False}]})
    approval = {'contract_version':2, 'scope_kind':'reviewed_resident_native_materialization_v2',
        'provider':source.provider, 'data_fingerprint':source.data_fingerprint,
        'authorization_fingerprint':source.authorization_fingerprint, 'approved_at_utc':utc_now(),
        'diagnostic_scope_unchanged':True, 'parser_transition_reviewed':True, 'publish':False,
        'remote_training_export':False, 'observation_report':report.name,
        'observation_report_sha256':sha256_file(report), 'entries':[entry]}
    path = report.parent/'approval.json'; atomic_write_json(path, approval)
    return queue, path, approval, ref, raw


def test_retained_short_original_is_accepted_once_without_get_or_synthesized_auth(retained, monkeypatch):
    queue, path, approval, ref, raw = retained
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k:pytest.fail('A retained repair cannot start HTTP'))
    jobs = [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')]
    result = materialize_reviewed_native(queue, [URL], path, lease_seconds=300)
    assert result['materialized_bodies']==1 and result['publisher_requests_this_command']==0
    saved, = pq.ParquetFile(queue.root/head(queue)['parquet_path']).read().to_pylist()
    assert saved['body']=='財經中心／合成人報導\n\n'+SHORT
    assert saved['raw_response_body']==raw['raw_response_body'] and saved['replay_source_parquet']==ref['path']
    assert saved['captured_at_utc']==raw['observed_at_utc'] and saved['http_duration_seconds'] is None
    assert saved['original_diagnostic_authorization_json'] is None and not saved['historical_point_in_time']
    assert saved['original_diagnostic_authorization_fingerprint']==raw['authorization_fingerprint']
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')]==jobs
    before = head(queue)
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies']==0
    assert head(queue)==before


@pytest.mark.parametrize('change', [
    {'state':'http_403','error_code':'http_403'}, {'state':'http_429','error_code':'http_429'},
    {'due':time.time()+3600}, {'lease_owner':'other','lease_until':time.time()+3600}, {'attempts':99},
])
def test_retained_short_original_preserves_newer_or_refused_heads(retained, change):
    queue, path, *_ = retained
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?',(*change.values(),URL))
    before = head(queue)
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies']==0
    assert head(queue)==before


@pytest.mark.parametrize('field', ['candidate_job_sha256','diagnostic_receipt_sha256','response_sha256','article_fields_sha256'])
def test_retained_short_original_revalidates_entire_approval_before_claim(retained, field):
    queue, path, approval, *_ = retained
    approval['entries'][0][field]='0'*64; atomic_write_json(path,approval)
    before = head(queue)
    with pytest.raises(ValueError):
        materialize_reviewed_native(queue,[URL],path,lease_seconds=300)
    assert head(queue)==before and not (queue.root/'reviewed_native_sources').exists()
