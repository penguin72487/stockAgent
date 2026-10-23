"""Title typography is a comparison key, never a source-text correction."""
from copy import deepcopy
import json
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_stockfeel import (
    STOCKFEEL_V5_CREDIT_NOTE, STOCKFEEL_V6_CREDIT_NOTE, STOCKFEEL_V6_VERIFICATION,
    STOCKFEEL_V6_HTTP_BINDING, _title_comparison_key,
)
from test_news_fulltext import config
from test_news_stockfeel_original import CANONICAL, TITLE, URL, html, settings
from test_news_stockfeel_roles import extract, role_html
from test_news_stockfeel_written_roles import writer_html, with_head, chart_html, v5_settings


NATIVE = '合成公司 ( 2330 ) – 財務分析 -123.45'
DOCUMENT = '合成公司 ( 2330 ) - 財務分析 -123.45 - StockFeel 股感'


def titles(html, *, native=NATIVE, document=DOCUMENT, og=None):
    soup = BeautifulSoup(html, 'lxml')
    soup.select_one('.sin-content-header > h1').string = native
    soup.select_one('head > title').string = document
    for node in soup.select('head > meta[property="og:title"]'):
        node['content'] = document if og is None else og
    return str(soup)


def test_literal_title_variants_and_signed_values_are_retained(settings, html):
    raw = titles(writer_html(html))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(settings, raw, 5)
    row = extract(settings, raw, 6)
    assert row['title'] == NATIVE and '-1,234.50' in row['body']
    notices = json.loads(row['verified_article_notices_json'])
    notice, = [item for item in notices if item['kind'] == 'source_declared_typographic_title_variants']
    assert notice['native_heading'] == NATIVE and notice['document_title_raw'] == DOCUMENT
    assert notice['OpenGraph_titles_raw'] == [DOCUMENT, DOCUMENT]
    assert not notice['raw_titles_rewritten'] and not notice['historical_alias_or_identity_inferred']
    assert 'parenthesized_stock_code_separator_v6' in row['article_identity_basis']
    assert 'same_native_h1_head_and_all_OG_titles' not in row['article_identity_basis']
    assert row['published_at_raw'] == '2026-10-15T07:07:51+00:00'
    assert row['modified_at_raw'] == '2026-10-15T07:07:55+00:00'


@pytest.mark.parametrize('native,document', [
    (NATIVE.replace('合成公司 ', '合成公司  '), DOCUMENT),
    (NATIVE.replace('合成公司 ', '合成公司  '), DOCUMENT.replace(' - 財務', ' – 財務')),
    (NATIVE.replace(' ( 2330 ) ', '(2330) '), DOCUMENT.replace(' ( 2330 ) ', '(2330) ')),
    (NATIVE.replace(' ( 2330 ) ', '(123456) '), DOCUMENT.replace(' ( 2330 ) ', '(123456) ')),
])
def test_only_comparison_changes_not_original_headline(settings, html, native, document):
    row = extract(settings, titles(html, native=native, document=document), 6)
    assert row['title'] == native
    notice, = [item for item in json.loads(row['verified_article_notices_json'])
        if item['kind']=='source_declared_typographic_title_variants']
    assert notice['document_title_raw'] == document


@pytest.mark.parametrize('document', [
    DOCUMENT.replace('合成公司', '完全不同公司'), DOCUMENT.replace('2330', '2317'),
    DOCUMENT.replace('-123.45', '123.45'), DOCUMENT.replace('-123.45', '−123.45'),
    DOCUMENT.replace('-123.45', '–123.45'), DOCUMENT.replace('財務分析', '財務分折'),
    DOCUMENT.replace(' - 財務', ' — 財務'), DOCUMENT.replace(' - 財務', ' − 財務'),
    DOCUMENT.replace(' - 財務', '–財務'), DOCUMENT.replace('公司 ', '公司\u200b '),
    DOCUMENT.replace('公司 ', '公司\xa0'), 'x'*3000,
], ids=['company','symbol','lost-negative','unicode-minus','en-dash-sign','word',
    'em-dash','minus-separator','unspaced-separator','hidden-character','unicode-space','oversized'])
def test_meaning_number_sign_dash_and_hidden_changes_are_not_equivalence(settings, html, document):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(settings, titles(html, document=document), 6)


def test_OpenGraph_cannot_disagree_with_other_title_declarations(settings, html):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(settings, titles(html, og=DOCUMENT.replace('2330','2317')), 6)


def test_en_dash_is_not_globally_equivalent_to_hyphen(settings, html):
    doc = titles(html, native='市場分析 – 損益 -123.45', document='市場分析 - 損益 -123.45 - StockFeel 股感')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(settings, doc, 6)


@pytest.mark.parametrize('old,new,error', [
    (CANONICAL, 'https://www.stockfeel.com.tw/other-story/', 'article_identity_not_verified'),
    ('content="article"', 'content="website"', 'article_identity_not_verified'),
    ('<h1>', '<h1 hidden>', 'article_identity_not_verified'),
    ('</table>', '</table><iframe src="/never-fetched"></iframe>', 'paywall_or_login'),
    ('</table>', '</table><form></form>', 'paywall_or_login'),
    ('data-aid="427"', 'data-aid="999"', 'article_attribution_not_verified'),
    ('2026-10-15T07:07:51+00:00', '2026-10-15T07:07:51', 'invalid_bulk_publication_clock'),
    ('© Stockfeel. All rights reserved', 'unreviewed copyright', 'article_rights_not_verified'),
])
def test_title_comparison_does_not_relax_independent_gates(settings, html, old, new, error):
    with pytest.raises(NewsError, match=error):
        extract(settings, titles(html).replace(old,new), 6)


def test_unknown_article_source_still_refuses_after_a_valid_writer(settings, html):
    soup=BeautifulSoup(titles(writer_html(html)),'lxml')
    soup.select('.sin-content-header div[data-aid]')[1]['data-aid']='999'
    with pytest.raises(NewsError,match='article_rights_or_scope_excluded'):
        extract(settings,str(soup),6)


def test_all_exact_old_fields_stay_equal_without_an_extra_notice(settings, html):
    cases = (html, role_html(html), writer_html(html), with_head(html, '合成作者甲, 合成作者乙'),
        chart_html(writer_html(html)), html.replace('</table>', '</table><wbr>'))
    for raw in cases:
        assert extract(settings, raw, 6) == extract(settings, raw, 5)


@pytest.mark.parametrize('value', [None, '', True, 'x'*3000, 'source\u200btext'])
def test_comparison_key_rejects_unknown_or_unbounded_input(value):
    assert _title_comparison_key(value) is None


def v6_settings():
    settings, repo = v5_settings()
    entry = settings['providers'][0]
    entry['embedded_article_layout']='stockfeel_visible_original_v6'
    entry['adapter_verification']=STOCKFEEL_V6_VERIFICATION
    entry['note']=entry['note'].replace(STOCKFEEL_V5_CREDIT_NOTE,STOCKFEEL_V6_CREDIT_NOTE)
    return settings, repo


def test_reviewed_v6_capture_preserves_exact_HTTP_refusal_and_authorization():
    settings, repo = v6_settings()
    source = PolicyRegistry(settings,base=repo).sources['stockfeel']
    assert (source.data_fingerprint,source.authorization_fingerprint)==STOCKFEEL_V6_HTTP_BINDING[1:3]
    assert (source.http_capacity_source_fingerprint,source.http_capacity_authorization_fingerprint)==STOCKFEEL_V6_HTTP_BINDING[3:]
    assert source.parser_repair_prior_capture_fingerprint==STOCKFEEL_V6_HTTP_BINDING[0]
    for field,value in [('interval_seconds',3),('parallelism',2),('capacity_max_parallelism',2)]:
        changed=deepcopy(settings); changed['providers'][0][field]=value
        other=PolicyRegistry(changed,base=repo).sources['stockfeel']
        assert not other.http_capacity_source_fingerprint and not other.http_capacity_authorization_fingerprint


def test_v6_reconciliation_preserves_all_old_heads_budgets_and_once_keys(tmp_path):
    settings, repo = v5_settings()
    owner = NewsQueue(tmp_path,PolicyRegistry(settings,base=repo))
    try:
        states=['article_identity_not_verified','article_attribution_not_verified','article_rights_or_scope_excluded',
            'paywall_or_login','awaiting_authorization','http_403','http_429']
        for index,state in enumerate(states):
            url='https://www.stockfeel.com.tw/synthetic-'+str(index)+'/'
            with owner.db:
                owner.add_articles([{'url':url}])
                owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=? WHERE url=?',
                    (state,state,time.time()-30,'2026-01-01T00:00:00+00:00',url))
        old=[tuple(row) for row in owner.db.execute('SELECT * FROM articles ORDER BY url')]
        with owner.db:
            owner.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',('failure_attempts:stockfeel', '17'))
        current,_=v6_settings()
        owner.registry=PolicyRegistry(current,base=repo)
        owner.reconcile_policy()
        assert [tuple(row) for row in owner.db.execute('SELECT * FROM articles ORDER BY url')]==old
        assert owner.db.execute('SELECT value FROM queue_settings WHERE key=?',('failure_attempts:stockfeel',)).fetchone()[0]=='17'
        target={'provider':'stockfeel','id':'a'*64}
        for version in (14,15):
            assert diagnostic._key(target,'a'*64,evidence_contract_version=version)==diagnostic._key(target,'b'*64,evidence_contract_version=version)
    finally:
        owner.close()
