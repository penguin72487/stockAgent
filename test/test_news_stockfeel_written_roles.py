"""v5 keeps written text and all declared credit roles without running media."""
from copy import deepcopy
import json
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_stockfeel import (
    STOCKFEEL_V5_HTTP_BINDING, STOCKFEEL_V5_VERIFICATION, STOCKFEEL_V4_CREDIT_NOTE, STOCKFEEL_V5_CREDIT_NOTE,
)
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_candidate_diagnostics as diagnostic
from test_news_fulltext import config
from test_news_stockfeel_original import AUTHOR, PROFILE, URL, html, settings
from test_news_stockfeel_roles import extract, role_html, production_settings


def writer_html(html):
    soup = BeautifulSoup(role_html(html), 'lxml')
    author = soup.select('.sin-content-header div[data-aid]')[0]
    author['data-aid'] = '352'
    author.a.string = '合成作者甲'
    author.a['href'] = 'https://www.stockfeel.com.tw/author/synthetic.writer'
    soup.select_one('head > meta[name=author]')['content'] = '合成作者甲'
    soup.select_one('.user-avatar')['href'] = author.a['href']
    return str(soup)


def with_head(html, value):
    soup = BeautifulSoup(writer_html(html), 'lxml')
    soup.select_one('head > meta[name=author]')['content'] = value
    return str(soup)


def chart_html(html, config=None):
    values = {'width':'100%', 'height':500, 'symbol':'TSE:2330', 'interval':'1',
        'timezone':'Asia/Taipei', 'theme':'light', 'style':'1', 'locale':'zh_TW',
        'toolbar_bg':'#f1f3f6', 'enable_publishing':False, 'allow_symbol_change':True,
        'container_id':'tradingview_synthetic'}
    if config:
        values.update(config)
    return html.replace('</table>', '</table><div class="tradingview-widget-container">'
        '<script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>'
        '<script type="text/javascript">new TradingView.widget('+json.dumps(values)+');</script></div>', 1)


def test_v5_preserves_all_old427_and_independent_writer_fields(settings, html):
    for doc in (html, role_html(html), writer_html(html)):
        assert extract(settings, doc, 5) == extract(settings, doc, 4)


def test_all_source_declared_coauthors_are_retained_not_verified_people(settings, html):
    doc = with_head(html, '合成作者甲, 合成作者乙')
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(settings, doc, 4)
    row = extract(settings, doc, 5)
    assert json.loads(row['authors_json']) == ['合成作者甲', '合成作者乙']
    detail = json.loads(row['author_attribution_detail_json'])
    assert detail['meta_author_raw'] == '合成作者甲, 合成作者乙'
    assert detail['credit_relationship'] == 'native_primary_writer_and_additional_head_declared_authors'
    assert detail['head_declared_names'] == ['合成作者甲', '合成作者乙']
    assert not detail['human_reporter_identity_verified'] and not detail['additional_head_profile_or_person_identity_verified']
    assert row['attribution_status'] == 'source_declared_header_credit_not_render_verified'


def test_distinct_source_group_stays_distinct_from_the_native_writer(settings, html):
    row = extract(settings, with_head(html, '股感商品研究室'), 5)
    assert json.loads(row['authors_json']) == ['合成作者甲']
    detail = json.loads(row['author_attribution_detail_json'])
    assert detail['meta_author_raw'] == '股感商品研究室'
    assert detail['head_declared_names'] == ['股感商品研究室']
    assert detail['credit_relationship'] == 'native_writer_and_distinct_head_group_declaration'
    assert not detail['head_declaration_equals_primary_native_writer']
    assert '原始合成段落。' in row['body']


@pytest.mark.parametrize('value', ['', '未核實的另一人', '合成作者乙, 合成作者甲',
    '合成作者甲, 合成作者甲', '合成作者甲, ', '合成作者甲, 合成\n作者乙',
    '合成作者甲,合成作者乙', '合成作者甲, a, b, c, d', '合成作者甲, '+('X'*129)])
def test_unknown_mismatch_and_malformed_coauthor_lists_still_reject(settings, html, value):
    with pytest.raises(NewsError):
        extract(settings, with_head(html, value), 5)


def test_legacy427_cannot_acquire_different_head_credit(settings, html):
    soup = BeautifulSoup(html, 'lxml')
    soup.select_one('head > meta[name=author]')['content'] = '股感商品研究室'
    with pytest.raises(NewsError):
        extract(settings, str(soup), 5)


def test_empty_wbr_keeps_all_source_written_words_and_numbers(settings, html):
    doc = writer_html(html).replace('原始合成段落。', '原始<wbr>合成段落。', 1)
    with pytest.raises(NewsError):
        extract(settings, doc, 4)
    row = extract(settings, doc, 5)
    baseline = extract(settings, writer_html(html), 4)
    assert ''.join(row['body'].split()) == ''.join(baseline['body'].split())
    assert '-1,234.50' in row['body'] and 'reviewed_empty_wbr_written_boundary_v5' in row['body_completeness']


@pytest.mark.parametrize('tag', ['<wbr hidden>', '<wbr class="unknown">', '<wbr onclick="bad()">'])
def test_nonempty_or_styled_word_break_is_not_a_visibility_bypass(settings, html, tag):
    with pytest.raises(NewsError):
        extract(settings, writer_html(html).replace('</table>', tag+'</table>'), 5)


def test_closed_interactive_chart_is_not_article_text_or_requested_prices(settings, html):
    row = extract(settings, chart_html(writer_html(html)), 5)
    baseline = extract(settings, writer_html(html), 4)
    assert row['body'] == baseline['body'] and row['body_sha256'] == baseline['body_sha256']
    assert 'TradingView.widget' not in row['body'] and 'TSE:2330' not in row['body']
    notice = json.loads(row['verified_article_notices_json'])[-1]
    assert notice['kind'] == 'unrequested_interactive_chart_configuration'
    assert json.loads(notice['source_configuration_json'])['symbol'] == 'TSE:2330'
    assert not notice['script_executed_or_external_media_requested'] and not notice['chart_values_in_written_article_body']
    assert notice['not_a_historical_price_observation']
    with pytest.raises(NewsError):
        extract(settings, chart_html(writer_html(html)), 4)


def mounted_chart_html(html):
    return chart_html(writer_html(html)).replace('<script type="text/javascript" src=',
        '<div id="tradingview_synthetic"></div><div class="tradingview-widget-copyright">'
        '<a href="https://tw.tradingview.com/symbols/TSE-2330/" rel="noopener" target="_blank">'
        '<span class="blue-text">2330圖表</span></a>由TradingView提供</div>'
        '<script type="text/javascript" src=', 1)


def test_empty_chart_mount_and_static_provider_credit_preserve_every_written_character(settings, html):
    doc = mounted_chart_html(html)
    row = extract(settings, doc, 5)
    static = BeautifulSoup(doc, 'lxml')
    for script in static.select('.tradingview-widget-container > script'):
        script.decompose()
    baseline = extract(settings, str(static), 4)
    assert row['body'] == baseline['body'] and row['body_sha256'] == baseline['body_sha256']
    assert '2330圖表' in row['body'] and '由TradingView提供' in row['body']
    assert json.loads(row['verified_article_notices_json'])[-1]['static_widget_credit_retained_in_written_body']
    assert 'TradingView.widget' not in row['body']


@pytest.mark.parametrize('old,new', [
    ('id="tradingview_synthetic"', 'id="tradingview_other"'),
    ('id="tradingview_synthetic"></div>', 'id="tradingview_synthetic">不可丟失正文</div>'),
    ('TSE-2330/', 'TSE-9999/'), ('2330圖表', '另一個圖表'),
    ('由TradingView提供', '由TradingView提供額外未審查文字'),
    ('class="blue-text"', 'class="blue-text" hidden'),
    ('tw.tradingview.com/symbols/', 'unknown.invalid/symbols/'),
])
def test_unknown_mount_or_credit_cannot_be_silently_omitted(settings, html, old, new):
    with pytest.raises(NewsError):
        extract(settings, mounted_chart_html(html).replace(old, new), 5)


@pytest.mark.parametrize('changes', [
    {'height':True}, {'height':100000}, {'symbol':'https://other.invalid'},
    {'enable_publishing':True}, {'datafeed':'not allowed'}, {'locale':'unknown'},
    {'container_id':'<script>'}, {'theme':'unknown'}, {'width':None},
])
def test_unreviewed_constructor_fields_are_not_executed_or_silently_dropped(settings, html, changes):
    with pytest.raises(NewsError):
        extract(settings, chart_html(writer_html(html), changes), 5)


@pytest.mark.parametrize('old,new', [
    ('https://s3.tradingview.com/tv.js', 'https://other.invalid/tv.js'),
    ('new TradingView.widget(', 'eval('),
    ('new TradingView.widget(', 'document.write('),
    ('"height": 500', '"height": 500, "height": 501'),
    (');</script>', '); alert(1);</script>'),
    ('class="tradingview-widget-container"', 'class="tradingview-widget-container other"'),
    ('<script type="text/javascript" src=', '正文不可丟失<script type="text/javascript" src='),
])
def test_other_script_or_container_prose_cannot_be_mistaken_for_a_chart(settings, html, old, new):
    with pytest.raises(NewsError):
        extract(settings, chart_html(writer_html(html)).replace(old, new), 5)


def test_oversized_constructor_is_rejected_before_its_json_or_expressions(settings, html):
    doc = chart_html(writer_html(html)).replace('new TradingView.widget(', ' ' * 4097 + 'new TradingView.widget(')
    with pytest.raises(NewsError):
        extract(settings, doc, 5)


def v5_settings():
    settings, repo = production_settings(4)
    entry = settings['providers'][0]
    entry['embedded_article_layout'] = 'stockfeel_visible_original_v5'
    entry['adapter_verification'] = STOCKFEEL_V5_VERIFICATION
    entry['note'] = entry['note'].replace(STOCKFEEL_V4_CREDIT_NOTE, STOCKFEEL_V5_CREDIT_NOTE)
    return settings, repo


def test_exact_v5_capture_keeps_the_same_source_refusals_and_authorization():
    settings, repo = v5_settings()
    source = PolicyRegistry(settings, base=repo).sources['stockfeel']
    assert (source.data_fingerprint, source.authorization_fingerprint) == STOCKFEEL_V5_HTTP_BINDING[1:3]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == STOCKFEEL_V5_HTTP_BINDING[3:]
    assert source.parser_repair_prior_capture_fingerprint == STOCKFEEL_V5_HTTP_BINDING[0]
    for field, value in [('interval_seconds',3), ('parallelism',2), ('capacity_max_parallelism',2)]:
        changed = deepcopy(settings); changed['providers'][0][field] = value
        other = PolicyRegistry(changed, base=repo).sources['stockfeel']
        assert not other.http_capacity_source_fingerprint and not other.http_capacity_authorization_fingerprint


def test_v5_upgrade_never_requeues_all_old_missing_or_completed_heads(tmp_path):
    settings, repo = production_settings(4)
    before_registry = PolicyRegistry(settings, base=repo)
    owner = NewsQueue(tmp_path, before_registry)
    try:
        states = ['article_identity_not_verified','article_attribution_not_verified',
            'article_rights_or_scope_excluded','paywall_or_login','awaiting_authorization','http_403','http_429']
        for index, state in enumerate(states):
            url = 'https://www.stockfeel.com.tw/synthetic-'+str(index)+'/'
            with owner.db:
                owner.add_articles([{'url':url}])
                owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=? WHERE url=?',
                    (state,state,time.time()-30,'2026-01-01T00:00:00+00:00',url))
        old = [tuple(r) for r in owner.db.execute('SELECT * FROM articles ORDER BY url')]
        after_settings, _ = v5_settings()
        owner.registry = PolicyRegistry(after_settings, base=repo)
        owner.reconcile_policy()
        assert [tuple(r) for r in owner.db.execute('SELECT * FROM articles ORDER BY url')] == old
        key = {'provider':'stockfeel','id':'a'*64}
        for version in (14,15):
            assert diagnostic._key(key, 'a'*64, evidence_contract_version=version) == diagnostic._key(key, 'b'*64, evidence_contract_version=version)
    finally:
        owner.close()
