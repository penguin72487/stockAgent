"""Literal title padding and declared special issues; no fabricated metadata."""
import json
from copy import deepcopy

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
from downloader.news_fulltext_gvm import GVM_PRESENTATION_LAYOUT, GVM_V10_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
from downloader.news_fulltext_retained_native import _article_digest, materialize_reviewed_native
from test_news_fulltext import config
from test_news_gvm_header_evidence import header_owner, submit
from test_news_gvm_magazine import magazine_page
from test_news_gvm_media import media_page
from test_news_gvm_native import URL, ORIGIN, schema, policy
from test_news_gvm_originals import diagnostic, head, http_stub, owner, response
from test_news_gvm_static_editor import static_entry


def presentation_entry():
    return {**static_entry(),'embedded_article_layout':GVM_PRESENTATION_LAYOUT,
        'adapter_verification':GVM_V10_VERIFICATION}


def title_page(padding=' '):
    soup=media_page('')
    title=soup.select_one('.article-head')['title']+padding
    soup.select_one('.article-head')['title']=title
    schema(soup,lambda a:a.update(headline=title))
    return BeautifulSoup(str(soup),'lxml')


def special_page():
    soup=magazine_page(); url=ORIGIN+'/magazine/special/1096'
    soup.select_one('.article-head_magazine')['href']=url
    def special(article):
        article['isPartOf'].update(url=url,**{'@id':url+'#issue'})
        article['isPartOf'].pop('issueNumber')
    schema(soup,special)
    return BeautifulSoup(str(soup),'lxml')


@pytest.mark.parametrize('padding',[' ','  '])
def test_only_ASCII_title_padding_is_presentation_and_raw_declarations_are_kept(padding):
    soup=title_page(padding)
    with pytest.raises(NewsError): extract_article(str(soup).encode(),policy(static_entry()),page_url=URL)
    article=extract_article(str(soup).encode(),policy(presentation_entry()),page_url=URL)
    notices=json.loads(article['verified_article_notices_json'])
    assert notices['source_native_title_raw']==notices['source_schema_headline_raw']==soup.select_one('.article-head')['title']
    assert notices['source_visible_title_raw']+padding==notices['source_native_title_raw']
    assert notices['native_title_trailing_ASCII_space_presentation'] and not notices['semantic_title_conflict_accepted']


@pytest.mark.parametrize('padding',['\t','\n','\u00a0','另一個標題'])
def test_other_title_differences_are_not_implicitly_normalized(padding):
    with pytest.raises(NewsError):
        extract_article(str(title_page(padding)).encode(),policy(presentation_entry()),page_url=URL)


def test_disagreeing_title_declarations_are_not_padding():
    soup=title_page(); schema(soup,lambda a:a.update(headline='不同宣告'))
    with pytest.raises(NewsError): extract_article(str(soup).encode(),policy(presentation_entry()),page_url=URL)


def test_special_issue_preserves_absent_number_and_literal_special_identity():
    soup=special_page()
    with pytest.raises(NewsError): extract_article(str(soup).encode(),policy(static_entry()),page_url=URL)
    article=extract_article(str(soup).encode(),policy(presentation_entry()),page_url=URL)
    issue=json.loads(article['verified_article_notices_json'])['source_declared_periodical_issue']
    assert issue['source_issue_url']==ORIGIN+'/magazine/special/1096'
    assert issue['source_issue_number'] is None and issue['native_special_ID_is_not_issue_number']
    assert issue['source_issue_kind']=='special' and not issue['issue_requested']


@pytest.mark.parametrize('change',['URL','ID','name','periodical','number','publisher','hidden'])
def test_special_issue_still_requires_same_page_native_and_schema_agreement(change):
    soup=special_page()
    if change=='hidden': soup.select_one('.article-head_magazine')['hidden']=''
    else:
        def alter(article):
            issue=article['isPartOf']
            if change=='URL': issue['url'] += '9'
            elif change=='ID': issue['@id'] += '9'
            elif change=='name': issue['name']='其他期刊'
            elif change=='number': issue['issueNumber']='1096'
            elif change=='publisher': issue['isPartOf']['publisher']={'@id':'https://example.invalid/'}
            else: issue['isPartOf']['@type']='WebPage'
        schema(soup,alter)
    with pytest.raises(NewsError): extract_article(str(soup).encode(),policy(presentation_entry()),page_url=URL)


@pytest.mark.parametrize('page',[media_page,lambda _:magazine_page()])
def test_existing_articles_are_identical_except_named_version(page):
    raw=str(page('')).encode()
    old=extract_article(raw,policy(static_entry()),page_url=URL)
    new=extract_article(raw,policy(presentation_entry()),page_url=URL)
    assert {k:v for k,v in old.items() if k!='article_identity_basis'}=={k:v for k,v in new.items() if k!='article_identity_basis'}


def test_registered_upgrade_preserves_every_unselected_refusal_and_deadline(config,tmp_path,monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('offline upgrade'))
    settings=deepcopy(config); settings['providers']=[static_entry()]
    states=('body_not_found','article_attribution_not_verified','article_title_not_verified',
        'article_identity_not_verified','http_429','paywall_or_login','article_rights_or_scope_excluded')
    queue=NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    try:
        for index,state in enumerate(states):
            url=ORIGIN+'/article/'+str(81402+index); queue.add_articles([{'url':url}])
            with queue.db:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=8,due=9999999999,last_checked_at_utc=? WHERE url=?',
                    (state,state,'2026-01-01T00:00:00+00:00',url))
        before=[dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    finally: queue.close()
    settings['providers']=[presentation_entry()]
    queue=NewsQueue(tmp_path,PolicyRegistry(settings,base=tmp_path))
    try:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]==before
    finally: queue.close()


@pytest.fixture(params=['title','special'])
def retained_presentation(header_owner,tmp_path,monkeypatch,request):
    queue,settings,_,_=header_owner
    soup=title_page() if request.param=='title' else special_page()
    old=queue.registry.sources['gvm']
    submitted=submit(queue,settings); diagnostic.pump_candidates(queue)
    http=http_stub(response(soup)); job=diagnostic.claim_candidate(queue,http,settings,'gvm')
    diagnostic.finish_candidate(queue,job,result=diagnostic.fetch_candidate(http,job))
    completed=dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(submitted['request_id'],)).fetchone())
    ref=json.loads(completed['result_json'])['private_original']; before=head(queue)
    raw,=pq.ParquetFile(queue.root/ref['path']).read(use_threads=False).to_pylist()
    settings['providers']=[presentation_entry()]; queue.registry=PolicyRegistry(settings,base=tmp_path)
    source=queue.registry.sources['gvm']
    assert source.parser_repair_prior_capture_fingerprint==old.data_fingerprint
    monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V9_ORIGINAL_BINDING',(old.data_fingerprint,old.authorization_fingerprint))
    article=extract_article(raw['raw_response_body'],source,page_url=URL)
    entry={'url':URL,'request_id':submitted['request_id'],'diagnostic_path':ref['path'],'diagnostic_receipt_path':ref['receipt_path'],
        'diagnostic_sha256':ref['sha256'],'diagnostic_receipt_sha256':sha256_file(queue.root/ref['receipt_path']),
        'response_sha256':ref['response_sha256'],'response_bytes':ref['response_bytes'],'captured_at_utc':raw['observed_at_utc'],
        'candidate_job_sha256':_article_digest(completed),'article_keys':sorted(article),'article_fields_sha256':_article_digest(article),
        'expected_missing_head':{k:before[k] for k in RETAINED_SUPPLEMENT_HEAD_FIELDS},
        **{k:article[k] for k in ('body_sha256','authors_json','published_at_raw','modified_at_raw')}}
    report=tmp_path/'review'/'observed.json'
    atomic_write_json(report,{'private_originals':[{'request_id':submitted['request_id'],'url':URL,'final_url':URL,
        'reference':ref,'receipt_sha256':entry['diagnostic_receipt_sha256'],'accepted_as_body':False}]})
    approval={'contract_version':2,'scope_kind':'reviewed_resident_native_materialization_v2','provider':'gvm',
        'data_fingerprint':source.data_fingerprint,'authorization_fingerprint':source.authorization_fingerprint,'approved_at_utc':utc_now(),
        'diagnostic_scope_unchanged':True,'parser_transition_reviewed':True,'publish':False,'remote_training_export':False,
        'observation_report':report.name,'observation_report_sha256':sha256_file(report),'entries':[entry]}
    path=report.parent/'approval.json'; atomic_write_json(path,approval)
    return queue,path,approval,ref,raw


def test_retained_header_original_is_adopted_once_without_HTTP(retained_presentation,monkeypatch):
    queue,path,approval,ref,raw=retained_presentation
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',lambda *a,**k:pytest.fail('reuse sealed original'))
    jobs=[tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]
    result=materialize_reviewed_native(queue,[URL],path,lease_seconds=300)
    assert result['materialized_bodies']==1 and result['publisher_requests_this_command']==0
    saved,=pq.ParquetFile(queue.root/head(queue)['parquet_path']).read(use_threads=False).to_pylist()
    assert saved['raw_response_body']==raw['raw_response_body'] and saved['captured_at_utc']==raw['observed_at_utc']
    assert saved['source_observed_at_utc']==raw['observed_at_utc'] and saved['http_duration_seconds'] is None
    assert saved['original_transport_redirects_stored'] is False
    assert saved['original_transport_redirect_basis']=='original_private_permit_requires_no_redirect_and_literal_request_final_URL'
    assert not saved['historical_point_in_time'] and not saved['remote_training_export']
    assert materialize_reviewed_native(queue,[URL],path,lease_seconds=300)['materialized_bodies']==0
    assert jobs==[tuple(r) for r in queue.db.execute('SELECT * FROM native_candidate_jobs')]


@pytest.mark.parametrize('change',['binding','auth','newer_head','lease'])
def test_replay_does_not_accept_changed_capture_authorization_head_or_lease(retained_presentation,monkeypatch,change):
    queue,path,_,_,_=retained_presentation
    if change=='binding': monkeypatch.setattr('downloader.news_fulltext_gvm.GVM_V9_ORIGINAL_BINDING',('a'*64,'b'*64))
    elif change=='auth':
        from dataclasses import replace
        source=queue.registry.sources['gvm']; queue.registry.sources['gvm']=replace(source,authorization_fingerprint='b'*64)
    elif change=='newer_head':
        with queue.db: queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?',(URL,))
    else:
        with queue.db: queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=9999999999 WHERE url=?",(URL,))
    try:
        result=materialize_reviewed_native(queue,[URL],path,lease_seconds=300)
    except ValueError:
        pass
    else:
        assert result['materialized_bodies']==0
    assert not head(queue)['body_sha256']
