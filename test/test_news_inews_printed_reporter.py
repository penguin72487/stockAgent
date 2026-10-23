"""Printed reporter/location role; synthetic prose, no retained news fixture."""
from contextlib import closing
import json

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_setn_inews import (
    CLOCK, NAME, URL, INEWS_AUTHOR_SELECTOR, page, page_with_outline, policy, v4_entry, v5_entry,
)


def entry():
    return v5_entry()


def printed(signature=None):
    soup=page(); soup.select_one('.author_wrap').clear()
    soup.select_one('[itemprop=articleBody] > p').string=signature or f'記者{NAME}／台北報導'
    return soup


def extract(soup): return extract_article(str(soup).encode(),policy(entry()),page_url=URL)


def test_literal_first_printed_reporter_preserves_role_body_clock_and_absent_profile():
    soup=printed(); before=str(soup)
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract_article(before.encode(),policy(v4_entry()),page_url=URL)
    row=extract(soup); detail=json.loads(row['author_attribution_detail_json'])
    assert detail['source_declared_person_name']==NAME
    assert detail['source_credit_role']=='native_visible_printed_named_reporter_signature'
    assert detail['source_declared_report_location']=='台北' and 'source_declared_desk' not in detail
    assert detail['source_declared_profile_url'] is None and detail['source_profile_absent']
    assert not detail['profile_requested'] and not detail['human_reporter_identity_verified']
    assert not detail['original_ownership_verified']
    assert detail['source_visible_signature_raw'] in row['body']
    assert row['published_at_raw']==CLOCK and row['modified_at_raw'] is None
    assert not json.loads(row['verified_article_notices_json'])['exact_PIT_eligible']
    assert str(soup)==before and row['article_identity_basis'].endswith('_v5')


@pytest.mark.parametrize('signature', ['記者綜合／台北報導','記者整理／台北報導',
    '記者編輯／台北報導','記者記者／台北報導',f'記者{NAME}/台北報導',
    f'記者{NAME}／報導', f'記者{NAME}／Taipei報導', f'記者{NAME}／台北報導 trailing',
    '財經中心／綜合報導','中央社／合成記者報導','國際中心／合成記者報導'])
def test_unwitnessed_or_unnamed_credit_does_not_become_a_person(signature):
    with pytest.raises(NewsError,match='article_attribution_not_verified'):
        extract(printed(signature))


@pytest.mark.parametrize('mutation',['header','late','nested','hidden','reprint','unknown_body'])
def test_no_late_credit_or_ambiguous_header_body_and_no_third_party_fallback(mutation):
    soup=printed(); first=soup.select_one('[itemprop=articleBody] > p')
    if mutation=='header': soup.select_one('.author_wrap').string='unknown header'
    elif mutation=='late':
        node=soup.new_tag('p');node.string='正文先於署名，不得從後面補';first.insert_before(node)
    elif mutation=='nested':
        text=first.get_text();first.clear();node=soup.new_tag('span');node.string=text;first.append(node)
    elif mutation=='hidden': first['hidden']=''
    else:
        node=soup.new_tag('p' if mutation=='reprint' else 'ul')
        node.string='本文轉載自其他來源';first.insert_after(node)
    with pytest.raises(NewsError): extract(soup)


def test_existing_header_outline_and_financial_desk_roles_keep_all_values(monkeypatch):
    desk=printed(f'財經中心／{NAME}報導')
    for soup in (page(),page_with_outline(monkeypatch),desk):
        old=extract_article(str(soup).encode(),policy(v4_entry()),page_url=URL)
        new=extract(soup)
        assert {k:v for k,v in old.items() if k!='article_identity_basis'}=={
            k:v for k,v in new.items() if k!='article_identity_basis'}


def test_http_control_binding_is_exact_not_a_changed_scope_or_rate(monkeypatch):
    source=policy(entry())
    monkeypatch.setattr('downloader.news_fulltext_setn.INEWS_V5_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'a'*64,'b'*64))
    assert policy(entry()).http_capacity_source_fingerprint=='a'*64
    for mutate in ('scope','rate'):
        changed=entry()
        if mutate=='scope': changed['authorization']['scope']+=' changed'
        else: changed['parallelism']=2
        assert not policy(changed).http_capacity_source_fingerprint
    changed=entry();changed['author_selectors']=[INEWS_AUTHOR_SELECTOR]
    with pytest.raises(ValueError): policy(changed)


def test_parser_upgrade_preserves_old_complete_and_failed_heads(tmp_path):
    from downloader.news_fulltext_queue import NewsQueue, utc_now
    from downloader.news_fulltext_http import Response
    from test_news_failure_diagnostics import fail
    def registry(value):
        return PolicyRegistry({'contract_version':1,'collection_purpose':'personal_research',
            'publish':False,'remote_training_export':False,'providers':[value]},base=tmp_path)
    root=tmp_path/'store';saved=URL+'1'
    with closing(NewsQueue(root,registry(v4_entry()))) as queue:
        fail(queue,Response(URL,200,{'content-type':'text/html'},str(printed()).encode()),
            code='article_attribution_not_verified',received_at_utc=utc_now())
        queue.add_articles([{'url':saved}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,body_sha256=?,body_bytes=?,parquet_path=?,captured_at_utc=?,due=? WHERE url=?',
                ('complete','a'*64,123,'batches/existing.parquet',utc_now(),10000000000,saved))
        before={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    value=entry();value['authorization']['scope']+=' Reviewed literal first printed reporter; no profile.'
    with closing(NewsQueue(root,registry(value))) as queue:
        assert before=={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
