"""Static article prose is independent of executable code and empty markers."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_contentplatform as native
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_contentplatform import URL, RAW, page, source
from test_news_contentplatform_written_geometry import written_html_entry


def entry():
    row=written_html_entry()
    row.update(embedded_article_layout=native.CONTENTPLATFORM_STATIC_DOCUMENT_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V8_VERIFICATION)
    return row


@pytest.mark.parametrize('parser',['lxml','html.parser'])
@pytest.mark.parametrize('extra',[
    '<div><p>前文 -2.5股<script>window.SECRET_CODE="不能當正文";</script>後文 0.23%</p></div>',
    '<p>原始文字<script src="https://never-fetched.invalid/script.js"></script>持續保留</p>',
    '<div><div><div><empty-marker class="source-marker"></empty-marker></div></div></div>',
    '<div><font color="red" size="2"></font><p>完整原始值</p></div>',
    '<div><unknown> </unknown><p>空白之外的文字<br>仍要換行<hr>保留原值</p></div>',
    '<p><script type="application/json">{"body":"不推論動態產生正文"}</script>保留真正書寫值</p>',
])
def test_v8_preserves_entire_static_written_DOM_without_executing_or_decoding(extra,parser):
    raw=page(extra=extra)
    with pytest.raises(NewsError):
        extract_article(raw,source(written_html_entry()),page_url=URL)
    parsed=extract_article(raw,replace(source(entry()),html_parser=parser),page_url=URL)
    soup=BeautifulSoup(raw,'html.parser')
    body=soup.select_one('article .tdb_single_content > .tdb-block-inner')
    for node in list(body.select('figure, img, script')):
        if node.parent is not None:
            node.decompose()
    assert ''.join(''.join(body.stripped_strings).split())==''.join(parsed['body'].split())
    assert RAW in parsed['body'] and 'SECRET_CODE' not in parsed['body']
    assert 'static_HTML_only' in parsed['body_completeness']
    assert parsed['published_at_raw']=='2026-10-14T10:43:17+08:00'
    assert parsed['modified_at_raw']=='2026-10-14T10:43:19+08:00'
    assert not parsed['historical_point_in_time'] and not parsed['per_article_license_verified']


@pytest.mark.parametrize('extra',[
    '<unknown>未知文字不得丟棄</unknown>',
    '<unknown><span></span></unknown>',
    '<unknown src="/not-requested"></unknown>',
    '<unknown data-body="尚未可見的正文"></unknown>',
    '<unknown onclick="unknown()"></unknown>',
    '<unknown hidden></unknown>',
    '<unknown style="display:none"></unknown>',
    '<div hidden><script>unknown()</script></div>',
    '<iframe></iframe>','<embed>','<object></object>','<canvas></canvas>',
    '<svg></svg>','<noscript></noscript>','<template></template>',
    '<button></button>','<input>','<textarea></textarea>',
    '<figure><img src="/not-requested"><script>unknown()</script></figure>',
])
def test_empty_leaf_and_nonrendered_script_do_not_hide_unsupported_or_hidden_content(extra):
    with pytest.raises(NewsError):
        extract_article(page(extra=extra),source(entry()),page_url=URL)


@pytest.mark.parametrize('changes,error',[
    ({'canonical':URL.replace('533649','429527')},'article_identity_not_verified'),
    ({'entity':URL.replace('533649','429527')},'article_identity_not_verified'),
    ({'profile':'https://example.org/author/'},'article_attribution_not_verified'),
    ({'modified':'2026-10-13T10:43:19+08:00'},'invalid_bulk_publication_clock'),
    ({'credit':'中央社記者'},'article_rights_or_scope_excluded'),
    ({'extra':'<div class="paywall">登入閱讀</div><script>unknown()</script>'},'paywall_or_login'),
])
def test_v8_keeps_identity_clock_byline_and_access_gates(changes,error):
    with pytest.raises(NewsError) as caught:
        extract_article(page(**changes),source(entry()),page_url=URL)
    assert caught.value.code==error


def test_v8_only_exact_v7_policy_inherits_original_HTTP_and_refusal_identity():
    config,_,repo=load_config(Path('configs/news_fulltext.json'))
    config=deepcopy(config)
    row=next(p for p in config['providers'] if p['id']=='contentplatform')
    row.update(embedded_article_layout=native.CONTENTPLATFORM_STATIC_DOCUMENT_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V8_VERIFICATION)
    current=PolicyRegistry(config,base=repo).sources['contentplatform']
    assert native.contentplatform_known_native_source(current)
    assert current.parser_repair_prior_capture_fingerprint==native.CONTENTPLATFORM_V7_CAPTURE
    assert current.authorization_fingerprint==native.CONTENTPLATFORM_V4_HTTP_BINDING[1]
    assert (current.http_capacity_source_fingerprint,current.http_capacity_authorization_fingerprint)==native.CONTENTPLATFORM_V4_HTTP_BINDING[2:4]
    for key,value in [('adapter_verification',native.CONTENTPLATFORM_V8_VERIFICATION+'_unknown'),
            ('interval_seconds',row['interval_seconds']+1)]:
        changed_config=deepcopy(config)
        next(p for p in changed_config['providers'] if p['id']=='contentplatform')[key]=value
        changed=PolicyRegistry(changed_config,base=repo).sources['contentplatform']
        assert not changed.parser_repair_prior_capture_fingerprint
        assert changed.http_capacity_source_fingerprint!=current.http_capacity_source_fingerprint
