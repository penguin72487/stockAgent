"""Static GVM writing and exact resident-original reuse; synthetic HTML only."""
import json
import time
from urllib.parse import urlencode

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_gvm import GVM_STATIC_EDITOR_LAYOUT, GVM_V9_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_retained_native import _article_digest, materialize_reviewed_native
from test_news_fulltext import config
from test_news_gvm_known_urls import extract
from test_news_gvm_media import media_page
from test_news_gvm_native import TEXT, URL, policy
from test_news_gvm_originals import diagnostic, head, http_stub, owner, response, submit
from test_news_gvm_written_formats import CHART, chart_page, written_entry


def static_entry():
    return {**written_entry(), 'embedded_article_layout':GVM_STATIC_EDITOR_LAYOUT,
        'adapter_verification':GVM_V9_VERIFICATION}


def page(markup):
    soup = media_page('')
    fragment = BeautifulSoup('<section>'+markup+'</section>', 'lxml')
    for node in list(fragment.section.contents): soup.select_one('.article-content').append(node)
    return soup


TABLE = ('<table border="1" cellpadding="0" cellspacing="0" data-sheets-baot="1" '
    'data-sheets-root="1" dir="ltr" style="width:100%">'
    '<thead><tr><th scope="col">年度</th></tr></thead>'
    '<tbody><tr height="23"><td align="center" height="23" width="80">合成表格數值</td></tr></tbody>'
    '<tfoot><tr><td>合成表格頁尾</td></tr></tfoot></table>')
BLOCK = ('<div style="width:100%;max-width:860px;margin:24px auto;line-height:1.75">'
    '<div style="border:1px solid #dbe3f3"><div>第一段<strong>完整文字</strong></div>'
    '<div style="font-size:15.5px;color:#374151">第二段原始文字</div></div></div>')
TURN_ID = 'request-WEB:12345678-1234-1234-1234-123456789abc-5'
TURN = (f'<section data-testid="conversation-turn-12" data-turn="assistant" data-turn-id="{TURN_ID}" '
    f'data-turn-id-container="{TURN_ID}" dir="auto"><p>來源原有編輯文字</p><p>第二段編輯文字</p></section>')
DOT = ('<div style="position:absolute;left:-31px;top:20px;width:15px;height:15px;'
    'border-radius:50%;background:#2563eb;border:4px solid #dbeafe;"><br/></div>')
YOUTUBE = ('<div class="embed-responsive embed-responsive-16by9"><iframe class="embed-responsive-item" '
    'alt="合成影片標題" src="//www.youtube.com/embed/EXAMPLE12345?wmode=opaque" '
    'frameborder="0" allowfullscreen=""></iframe></div>')
FACEBOOK_SRC = 'https://www.facebook.com/plugins/post.php?'+urlencode({
    'href':'https://www.facebook.com/example/posts/pfbidSYNTHETIC123', 'show_text':'true', 'width':'500'})
FACEBOOK = (f'<iframe src="{FACEBOOK_SRC}" width="100%" height="233" style="border:none;overflow:hidden" '
    'scrolling="no" frameborder="0" allowfullscreen="true" '
    'allow="autoplay; clipboard-write; encrypted-media; picture-in-picture; web-share"></iframe>')


@pytest.mark.parametrize('markup,expected', [
    (TABLE, ['年度','合成表格數值','合成表格頁尾']),
    (TABLE.replace('data-sheets-baot="1" data-sheets-root="1"', 'data-path-to-node="41"'), ['合成表格頁尾']),
    (BLOCK, ['第一段','完整文字','第二段原始文字']),
    ('<section><p>來源段落</p>'+BLOCK+TABLE+'</section>', ['來源段落','合成表格頁尾','第二段原始文字']),
    (TURN, ['來源原有編輯文字','第二段編輯文字']),
    ('<div style="position:relative">'+DOT+'時間軸正文</div>', ['時間軸正文']),
])
def test_static_blocks_and_table_footers_retain_every_written_segment(markup, expected):
    soup = page(markup)
    with pytest.raises(NewsError): extract(soup, policy(written_entry()))
    result = extract(soup, policy(static_entry()))
    assert TEXT in result['body']
    assert all(text in result['body'] for text in expected)
    assert not result['historical_point_in_time']
    assert not json.loads(result['verified_article_notices_json'])['media_or_scripts_requested_or_executed']


@pytest.mark.parametrize('attrs', [
    {'onclick':'bad()'}, {'hidden':''}, {'aria-hidden':'true'}, {'data-loader':'unreviewed'},
    {'style':'display:none'}, {'style':'opacity:0'}, {'style':'position:fixed'},
    {'style':'position:absolute;left:-10000px'}, {'style':'width:0px'},
    {'style':'font-size:0'}, {'style':'background:url(https://example.invalid/)'},
    {'style':r'display:n\6f ne'}, {'style':'color:var(--hidden)'},
])
def test_unknown_hidden_or_executing_block_is_not_silently_omitted(attrs):
    soup = page(BLOCK)
    node = soup.select_one('.article-content > div[style^="width:100%"]')
    node.attrs.update(attrs)
    with pytest.raises(NewsError): extract(soup, policy(static_entry()))


@pytest.mark.parametrize('tag,attrs', [
    ('table', {'data-sheets-root':'0'}), ('table', {'data-path-to-node':'unknown'}),
    ('table', {'onclick':'run()'}), ('table', {'width':'0'}), ('table', {'border':'-1'}),
    ('tfoot', {'hidden':''}), ('tfoot', {'data-loader':'unknown'}),
    ('tr', {'height':'0'}), ('td', {'align':'justify'}), ('td', {'width':'bad'}),
    ('td', {'style':'display:none'}),
])
def test_table_presentation_does_not_erase_cells_or_authorize_unknown_data(tag, attrs):
    soup = page(TABLE); soup.select_one('.article-content '+tag).attrs.update(attrs)
    with pytest.raises(NewsError): extract(soup, policy(static_entry()))


@pytest.mark.parametrize('kind', ['table','cards'])
def test_empty_flourish_variants_capture_writing_without_inventing_dynamic_values(kind):
    soup = chart_page(CHART.replace('flourish-chart','flourish-'+kind))
    with pytest.raises(NewsError): extract(soup, policy(written_entry()))
    result = extract(soup, policy(static_entry()))
    assert result['body'] == extract(media_page(''), policy(written_entry()))['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_empty_flourish_table_or_cards_loader_verified']
    assert notices['dynamic_embed_values_captured'] is False
    assert 'empty_Flourish_table_or_cards_values_not_captured' in result['body_completeness']


@pytest.mark.parametrize('change', ['fallback','comment','nested','loader','src','hidden','event'])
def test_flourish_is_only_a_reviewed_empty_mount(change):
    soup = chart_page(CHART.replace('flourish-chart','flourish-table'))
    node = soup.select_one('.flourish-embed')
    if change=='fallback': node.append('不能丟掉的原始文字')
    elif change=='comment': node.append(BeautifulSoup('<!--不能忽略-->', 'lxml').find(string=lambda v:'不能忽略' in v))
    elif change=='nested': node.append(soup.new_tag('span'))
    elif change=='loader': node.script['src']='https://example.invalid/embed.js'
    elif change=='src': node['data-src']='visualisation/0'
    elif change=='hidden': node['hidden']=''
    elif change=='event': node['onclick']='run()'
    with pytest.raises(NewsError): extract(soup, policy(static_entry()))


@pytest.mark.parametrize('markup,notice', [
    (YOUTUBE, 'native_empty_protocol_relative_youtube_player_verified'),
    (FACEBOOK, 'native_empty_facebook_post_player_verified'),
])
def test_empty_media_variants_never_request_or_promote_external_content(markup, notice):
    soup = page(markup)
    with pytest.raises(NewsError): extract(soup, policy(written_entry()))
    result = extract(soup, policy(static_entry()))
    assert result['body'] == extract(media_page(''), policy(written_entry()))['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[notice] and not notices['media_or_scripts_requested_or_executed']
    if markup==FACEBOOK: assert notices['external_social_post_text_captured'] is False


@pytest.mark.parametrize('markup', [
    YOUTUBE.replace('//www.youtube.com/', 'http://www.youtube.com/'),
    YOUTUBE.replace('//www.youtube.com/', '//example.invalid/'),
    YOUTUBE.replace('</iframe>', '來源影音附文</iframe>'),
    FACEBOOK.replace('width="100%"','width="0"'),
    FACEBOOK.replace('www.facebook.com%2Fexample','example.invalid%2Fexample'),
    FACEBOOK.replace('scrolling="no"','scrolling="no" onload="run()"'),
    FACEBOOK.replace('</iframe>', '應保留的貼文文字</iframe>'),
])
def test_unknown_external_players_or_fallback_text_remain_failures(markup):
    with pytest.raises(NewsError): extract(page(markup), policy(static_entry()))


@pytest.mark.parametrize('markup', [
    '<div id="article_further[]">應保留原文</div>',
    '<div id="article_further[]"><!--未知節點--></div>',
    '<div id="article_further[]" onclick="run()"></div>',
    TURN.replace('data-turn="assistant"','data-turn="unreviewed"'),
    TURN.replace('dir="auto"','dir="auto" onload="run()"'),
    '<div style="position:relative">'+DOT.replace('<br/>','不應忽略的文字')+'</div>',
])
def test_empty_placeholders_and_editor_wrappers_do_not_authorize_unreviewed_text_or_attributes(markup):
    with pytest.raises(NewsError): extract(page(markup), policy(static_entry()))


def test_empty_recommendation_placeholder_has_no_effect_on_body():
    result = extract(page('<div id="article_further[]"></div>'), policy(static_entry()))
    assert result['body'] == extract(media_page(''), policy(written_entry()))['body']


@pytest.mark.parametrize('soup_factory', [lambda:media_page(''), chart_page])
def test_existing_written_article_fields_remain_identical_except_named_parser_version(soup_factory):
    soup = soup_factory()
    old = extract(soup, policy(written_entry())); new = extract(soup, policy(static_entry()))
    assert {k:v for k,v in old.items() if k!='article_identity_basis'} == {
        k:v for k,v in new.items() if k!='article_identity_basis'}


@pytest.fixture
def retained(owner, tmp_path, monkeypatch):
    queue, config = owner
    submitted = submit(queue, config); diagnostic.pump_candidates(queue)
    job = diagnostic.claim_candidate(queue, http_stub(response(page(BLOCK+TABLE))), config, 'gvm')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http_stub(response(page(BLOCK+TABLE))), job))
    completed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (submitted['request_id'],)).fetchone())
    ref = json.loads(completed['result_json'])['private_original']
    raw, = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    before = head(queue); old = queue.registry.sources['gvm']
    config['providers'][0] = static_entry()
    queue.registry = PolicyRegistry(config, base=tmp_path)
    source = queue.registry.sources['gvm']
    assert source.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V8_ORIGINAL_BINDING',
        (old.data_fingerprint, old.authorization_fingerprint))
    article = extract_article(raw['raw_response_body'], source, page_url=URL)
    entry = {'url':URL, 'request_id':submitted['request_id'], 'diagnostic_path':ref['path'],
        'diagnostic_receipt_path':ref['receipt_path'], 'diagnostic_sha256':ref['sha256'],
        'diagnostic_receipt_sha256':sha256_file(queue.root/ref['receipt_path']),
        'response_sha256':ref['response_sha256'], 'response_bytes':ref['response_bytes'],
        'captured_at_utc':raw['observed_at_utc'], 'candidate_job_sha256':_article_digest(completed),
        'article_keys':sorted(article), 'article_fields_sha256':_article_digest(article),
        'expected_missing_head':{k:before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{k:article[k] for k in ('body_sha256','authors_json','published_at_raw','modified_at_raw')}}
    report = tmp_path/'review'/'observed.json'
    atomic_write_json(report, {'private_originals':[{'request_id':submitted['request_id'], 'url':URL,
        'final_url':URL, 'reference':ref, 'receipt_sha256':entry['diagnostic_receipt_sha256'], 'accepted_as_body':False}]})
    approval = {'contract_version':2, 'scope_kind':'reviewed_resident_native_materialization_v2',
        'provider':'gvm', 'data_fingerprint':source.data_fingerprint,
        'authorization_fingerprint':source.authorization_fingerprint, 'approved_at_utc':utc_now(),
        'diagnostic_scope_unchanged':True, 'parser_transition_reviewed':True,
        'publish':False, 'remote_training_export':False, 'observation_report':report.name,
        'observation_report_sha256':sha256_file(report), 'entries':[entry]}
    path = report.parent/'approval.json'; atomic_write_json(path, approval)
    return queue, path, approval, ref, raw


def test_retained_static_original_is_adopted_once_and_reopened_without_HTTP(retained, monkeypatch):
    queue, path, approval, ref, raw = retained
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a,**k:pytest.fail('No HTTP for retained originals'))
    jobs = [tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    first = materialize_reviewed_native(queue, [URL], path, lease_seconds=300)
    assert first['materialized_bodies']==1 and first['publisher_requests_this_command']==0
    saved, = pq.ParquetFile(queue.root/head(queue)['parquet_path']).read().to_pylist()
    assert saved['raw_response_body']==raw['raw_response_body']
    assert saved['captured_at_utc']==saved['source_observed_at_utc']==raw['observed_at_utc']
    assert saved['http_duration_seconds'] is None and saved['original_diagnostic_authorization_json'] is None
    assert saved['original_transport_redirects_stored'] is False
    assert saved['original_diagnostic_authorization_fingerprint']==raw['authorization_fingerprint']
    assert not saved['historical_point_in_time'] and not saved['publish'] and not saved['remote_training_export']
    assert '合成表格頁尾' in saved['body'] and '第二段原始文字' in saved['body']
    before = head(queue)
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies']==0
    assert head(queue)==before and jobs==[tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]


@pytest.mark.parametrize('change', [
    {'state':'http_403','error_code':'http_403'}, {'state':'http_429','error_code':'http_429'},
    {'due':time.time()+3600}, {'lease_owner':'other','lease_until':time.time()+3600}, {'attempts':99},
])
def test_retained_original_does_not_override_newer_refusal_lease_or_retry_evidence(retained, change):
    queue, path, *_ = retained
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?', (*change.values(), URL))
    before = head(queue)
    assert materialize_reviewed_native(queue, [URL], path, lease_seconds=300)['materialized_bodies']==0
    assert head(queue)==before


@pytest.mark.parametrize('field', [
    'candidate_job_sha256','diagnostic_receipt_sha256','response_sha256','article_fields_sha256','captured_at_utc',
])
def test_original_integrity_and_approval_are_rechecked_before_any_write(retained, field):
    queue, path, approval, *_ = retained
    approval['entries'][0][field]='0'*64; atomic_write_json(path,approval)
    before = head(queue)
    with pytest.raises(ValueError): materialize_reviewed_native(queue,[URL],path,lease_seconds=300)
    assert head(queue)==before and not (queue.root/'reviewed_native_sources').exists()


def test_changed_source_scope_cannot_inherit_the_original_transition(retained):
    from dataclasses import replace
    from downloader.news_fulltext_retained_native import _resident_transition
    queue, *_ = retained; source = queue.registry.sources['gvm']
    for altered in (replace(source, authorization_fingerprint='0'*64),
            replace(source, parser_repair_prior_capture_fingerprint='0'*64),
            replace(source, known_url_only=False), replace(source, domains=('example.invalid',))):
        with pytest.raises(ValueError): _resident_transition(altered)


def test_durable_GVM_original_recovery_revalidates_its_exact_restart_witness(retained, monkeypatch):
    from downloader.news_fulltext_retained_native import reviewed_native_head_matches
    queue, path, *_ = retained
    original = queue._accept_records
    def restarted(records, relative, receipt):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,error_code='owner_restart',"
                "lease_owner=NULL,lease_until=NULL WHERE url=?", (URL,))
        assert reviewed_native_head_matches(queue, records[0])
        return original(records, relative, receipt)
    monkeypatch.setattr(queue, '_accept_records', restarted)
    assert materialize_reviewed_native(queue,[URL],path,lease_seconds=300)['materialized_bodies']==1
    assert head(queue)['state']=='complete'


def test_racing_refusal_before_GVM_claim_preserves_source_deadline(retained, monkeypatch):
    queue, path, *_ = retained
    original = queue.claim_url
    def refused(url, lease_seconds, **kwargs):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='http_429',error_code='http_429',due=? WHERE url=?",
                (time.time()+3600,URL))
        return original(url,lease_seconds,**kwargs)
    before = head(queue)
    monkeypatch.setattr(queue,'claim_url',refused)
    assert materialize_reviewed_native(queue,[URL],path,lease_seconds=300)['materialized_bodies']==0
    after = head(queue)
    assert after['state']=='http_429' and after['attempts']==before['attempts'] and not after['body_sha256']


def test_registered_v9_candidate_preserves_the_existing_complete_source_contract(monkeypatch):
    from copy import deepcopy
    from pathlib import Path
    from downloader.news_fulltext import load_config
    import test_news_fulltext as checks
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings); candidate.pop('provider_catalog',None)
    next(r for r in candidate['providers'] if r['id']=='gvm').update(
        embedded_article_layout=GVM_STATIC_EDITOR_LAYOUT, adapter_verification=GVM_V9_VERIFICATION)
    registry = PolicyRegistry(candidate,base=repo)
    monkeypatch.setattr(checks,'load_config',lambda _: (candidate,registry,repo))
    checks.test_default_configuration_separates_personal_capture_from_licenses()
