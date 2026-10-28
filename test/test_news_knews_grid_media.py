"""Whole written grids and inert native nodes keep source values and receipts."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup, Comment
import pytest

from downloader import news_fulltext_knews as native
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_fulltext import config
from test_news_knews_known_urls import URL, authored
from test_news_knews_tables import table_entry, source, retained_native_fixture
from test_news_knews_written_blocks import cards_page


def grid_entry():
    return dict(table_entry(), embedded_article_layout=native.KNEWS_GRID_NATIVE_LAYOUT,
        adapter_verification=native.KNEWS_V9_VERIFICATION)


def extract(soup):
    return extract_article(str(soup).encode(), source(grid_entry()), page_url=URL)


def grid_page():
    soup = authored()
    table = soup.new_tag('table', border='1', style='width:100%;border-collapse:collapse')
    columns = soup.new_tag('colgroup')
    for _ in range(3):
        columns.append(soup.new_tag('col', style='width:33.3%'))
    rows = soup.new_tag('tbody')
    for values in [('合成名稱', '金額', '日期'), ('合成標的甲', '-12.345元', '4/20'),
                   ('合成標的乙', '1000股', '2020/3/2')]:
        row = soup.new_tag('tr', style='height:22px')
        for value in values:
            cell = soup.new_tag('td', style='height:22px'); cell.string = value; row.append(cell)
        rows.append(row)
    table.extend([columns, rows])
    soup.select_one('.article-content-inner > p').insert_after(table)
    return soup


def video_page():
    soup = authored()
    prefix = 'https://static.kmedia.com.tw/prd/2025/1203/' + URL.rsplit('/', 1)[-1] + '/'
    video = soup.new_tag('video', attrs={'id':'article-video-hls',
        'class':['vjs-matrix','video-js','vjs-16-9'], 'playsinline':'', 'controls':'', 'autoplay':'',
        'muted':'', 'preload':'auto', 'width':'1920', 'height':'1080',
        'poster':prefix+'a'*32+'.webp', 'data-setup':'{"fluid": true,"disablePictureInPicture":true}'})
    video.append(soup.new_tag('source', src=prefix+'vdo/'+'b'*32+'elitemedia.m3u8', type='application/x-mpegURL'))
    soup.select_one('h1').insert_before(video)
    return soup


def test_complete_grid_preserves_literal_values_in_source_order():
    soup = grid_page(); result = extract(soup)
    assert ''.join(result['body'].split()).count(''.join(soup.table.get_text().split())) == 1
    assert result == extract_article(str(soup).encode(), replace(source(grid_entry()), html_parser='html.parser'), page_url=URL)
    notices = json.loads(result['verified_article_notices_json'])
    assert (notices['native_written_data_grids'],notices['native_written_data_rows'],notices['native_written_data_cells']) == (1,3,9)
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(),source(table_entry()),page_url=URL)


@pytest.mark.parametrize('case', ['short_row','extra_column','nested_text','hidden','event','resource',
    'written_column','active','extra_caption','unowned_text','empty_row','unknown_attribute'])
def test_grid_cannot_discard_cells_content_or_unknown_controls(case):
    soup = grid_page(); table = soup.table; cell = table.select('td')[3]
    if case == 'short_row': cell.decompose()
    elif case == 'extra_column': table.colgroup.append(soup.new_tag('col'))
    elif case == 'nested_text': cell.append(soup.new_tag('span'))
    elif case == 'hidden': cell['style'] = 'display:none'
    elif case == 'event': table.tr['onclick'] = 'noExecution()'
    elif case == 'resource': cell.append(soup.new_tag('iframe',src='https://example.invalid/'))
    elif case == 'written_column': table.col.string = '不可略過'
    elif case == 'active': table.append(soup.new_tag('script'))
    elif case == 'extra_caption': table.insert(0,soup.new_tag('caption'))
    elif case == 'unowned_text': table.tr.append('不能略過的裸文字')
    elif case == 'empty_row':
        for n in table.tr.select('td'): n.clear()
    else: cell['data-unverified'] = 'unknown'
    with pytest.raises(NewsError): extract(soup)


def test_only_inert_card_comment_is_omitted_from_written_values():
    soup = cards_page(); root = soup.select_one('.my-3')
    root.insert(0,Comment('<h5>非顯示標題，不得冒充正文。</h5>'))
    result = extract(soup)
    assert '非顯示標題' not in result['body']
    assert ''.join(root.get_text().split()) in ''.join(result['body'].split())
    assert json.loads(result['verified_article_notices_json'])['native_inert_card_comments_excluded'] == 1
    root.append('旁支裸文字不能略掉')
    with pytest.raises(NewsError): extract(soup)


def test_native_hls_remains_unrequested_media_and_body_clocks_match():
    before, after = extract(authored()), extract(video_page())
    for field in ('body','body_sha256','authors_json','published_at_raw','modified_at_raw'):
        assert after[field] == before[field]
    notices = json.loads(after['verified_article_notices_json'])
    assert notices['native_nontext_HLS_videos_verified'] == 1 and not notices['HLS_or_poster_requested_or_transcribed']


def test_main_caption_after_intro_retains_its_literal_document_position():
    soup = video_page()
    article = soup.select_one('article#post-detail')
    figure = article.select_one(':scope > figure').extract()
    article.append(figure)
    result = extract(soup)
    assert result['body'].endswith(figure.figcaption.get_text(' ',strip=True))
    assert result['body'].index(soup.select_one('.post-intro').get_text(' ',strip=True)) < result['body'].index(figure.figcaption.get_text(' ',strip=True))


@pytest.mark.parametrize('case',['external','different_article','poster_scope','query','fallback','track','event','hidden','data_code','type'])
def test_native_media_rejects_unverified_scope_fallbacks_and_execution(case):
    soup = video_page(); video = soup.video
    if case == 'external': video.source['src'] = 'https://example.invalid/video.m3u8'
    elif case == 'different_article': video.source['src'] = video.source['src'].replace(URL.rsplit('/',1)[-1],'F'*32)
    elif case == 'poster_scope': video['poster'] = 'https://example.invalid/poster.webp'
    elif case == 'query': video.source['src'] += '?unverified=1'
    elif case == 'fallback': video.append('不可丟掉的影片替代文字')
    elif case == 'track': video.append(soup.new_tag('track',src='https://example.invalid/captions.vtt'))
    elif case == 'event': video['onerror'] = 'noExecution()'
    elif case == 'hidden': video['style'] = 'display:none'
    elif case == 'data_code': video['data-setup'] = '{"execute":"never"}'
    else: video.source['type'] = 'text/html'
    with pytest.raises(NewsError): extract(soup)


def test_whole_installed_config_changes_only_parser_and_keeps_http_capacity(monkeypatch):
    settings, old, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings); candidate.pop('provider_catalog',None)
    next(p for p in candidate['providers'] if p['id']=='knews').update(
        embedded_article_layout=native.KNEWS_GRID_NATIVE_LAYOUT,adapter_verification=native.KNEWS_V9_VERIFICATION)
    current = PolicyRegistry(candidate,base=repo)
    for p,source_old in old.sources.items():
        new = current.sources[p]
        if p!='knews': assert new == source_old
        else:
            assert new.parser_repair_prior_capture_fingerprint == native.KNEWS_TABLE_HTTP_BINDING[0]
            assert (new.authorization_fingerprint,new.http_capacity_source_fingerprint,new.http_capacity_authorization_fingerprint) == native.KNEWS_TABLE_HTTP_BINDING[1:]
    import test_news_fulltext as defaults
    monkeypatch.setattr(defaults,'load_config',lambda _: (candidate,current,repo))
    defaults.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.fixture
def retained_grid(tmp_path):
    yield from retained_native_fixture(tmp_path,'article_identity_not_verified',grid_page(),
        prior_layout=native.KNEWS_TABLE_NATIVE_LAYOUT,prior_verification=native.KNEWS_V8_VERIFICATION,
        next_layout=native.KNEWS_GRID_NATIVE_LAYOUT,next_verification=native.KNEWS_V9_VERIFICATION)


def test_grid_reuses_only_exact_original_and_preserves_head_on_startup(retained_grid,monkeypatch):
    from test_news_knews_tables import test_reuse_keeps_original_clock_and_does_not_repeat_http_or_completed_body
    test_reuse_keeps_original_clock_and_does_not_repeat_http_or_completed_body(retained_grid,monkeypatch)


def multi_reporter_entry():
    return dict(grid_entry(), embedded_article_layout=native.KNEWS_MULTI_REPORTER_LAYOUT,
        adapter_verification=native.KNEWS_V10_VERIFICATION)


def multi_reporter_page(*, separator='、'):
    from test_news_knews_native import schema
    # Reset BeautifulSoup's script-string type after the first schema edit.
    soup = BeautifulSoup(str(authored('合成記者甲')), 'lxml')
    byline = soup.select_one('.abc-byline')
    separator_node = soup.new_tag('span', attrs={'class':['separator']});separator_node.string = separator
    other = soup.new_tag('a', href='https://www.knews.com.tw/reporter/second-2');other.string = '合成記者乙'
    byline.extend([separator_node,other])
    soup.select_one('meta[name=author]')['content'] = separator.join(['合成記者甲','合成記者乙'])
    schema(soup,lambda value:value['author'].append({'@type':'Person','name':'合成記者乙','url':other['href']}))
    return BeautifulSoup(str(soup), 'lxml')


@pytest.mark.parametrize('separator',['、',',','，','/','／'])
def test_multiple_reporters_keep_every_person_profile_and_literal_order(separator):
    soup = multi_reporter_page(separator=separator)
    result = extract_article(str(soup).encode(),source(multi_reporter_entry()),page_url=URL)
    assert json.loads(result['authors_json']) == ['合成記者甲','合成記者乙']
    detail = json.loads(result['author_attribution_detail_json'])
    assert [p['name'] for p in detail['source_header_credits']] == ['合成記者甲','合成記者乙']
    assert [p['url'] for p in detail['source_header_credits']] == ['https://www.knews.com.tw/reporter/renee','https://www.knews.com.tw/reporter/second-2']
    assert detail['source_meta_author_raw'] == separator.join(['合成記者甲','合成記者乙'])
    assert detail['source_header_separators_raw'] == [separator]
    assert detail['source_header_credit'] is None and detail['literal_profile_url'] is None
    assert not detail['different_names_are_verified_aliases'] and detail['printed_reporter_name'] == '合成作者'
    assert result == extract_article(str(soup).encode(),replace(source(multi_reporter_entry()),html_parser='html.parser'),page_url=URL)
    with pytest.raises(NewsError): extract(soup)


@pytest.mark.parametrize('case',['schema_order','schema_missing','meta_order','meta_missing','profile','profile_duplicate',
    'external','hidden','unknown_separator','extra_text','resource','event','extra_author'])
def test_multi_reporter_admission_cannot_drop_or_alias_an_author(case):
    from test_news_knews_native import schema
    soup = multi_reporter_page();links = soup.select('.abc-byline > a')
    if case == 'schema_order': schema(soup,lambda obj:obj['author'].reverse())
    elif case == 'schema_missing': schema(soup,lambda obj:obj['author'].pop())
    elif case == 'meta_order': soup.select_one('meta[name=author]')['content']='合成記者乙、合成記者甲'
    elif case == 'meta_missing': soup.select_one('meta[name=author]')['content']='合成記者甲'
    elif case == 'profile': links[1]['href']='https://example.org/reporter/second-2'
    elif case == 'profile_duplicate':
        links[1]['href']=links[0]['href']
        schema(soup,lambda obj:obj['author'][1].update(url=links[0]['href']))
    elif case == 'external':
        links[1].string='中央社'
        soup.select_one('meta[name=author]')['content']='合成記者甲、中央社'
        schema(soup,lambda obj:obj['author'][1].update(name='中央社'))
    elif case == 'hidden': links[1]['hidden']=''
    elif case == 'unknown_separator': soup.select_one('.separator').string='不得丟棄的來源聲明'
    elif case == 'extra_text': soup.select_one('.abc-byline').append('不能省略的旁支文字')
    elif case == 'resource': links[1].append(soup.new_tag('iframe',src='https://example.org/'))
    elif case == 'event': links[1]['onclick']='notExecuted()'
    else: soup.select_one('.abc-byline').append(soup.new_tag('a',href=links[0]['href']))
    with pytest.raises(NewsError): extract_article(str(soup).encode(),source(multi_reporter_entry()),page_url=URL)


def test_multi_reporter_config_keeps_other_sources_and_shared_capacity(monkeypatch):
    settings,_,repo = load_config(Path('configs/news_fulltext.json'))
    previous = deepcopy(settings);previous.pop('provider_catalog',None)
    next(p for p in previous['providers'] if p['id']=='knews').update(
        embedded_article_layout=native.KNEWS_GRID_NATIVE_LAYOUT,adapter_verification=native.KNEWS_V9_VERIFICATION)
    old = PolicyRegistry(previous,base=repo)
    candidate = deepcopy(previous)
    next(p for p in candidate['providers'] if p['id']=='knews').update(
        embedded_article_layout=native.KNEWS_MULTI_REPORTER_LAYOUT,adapter_verification=native.KNEWS_V10_VERIFICATION)
    new = PolicyRegistry(candidate,base=repo)
    assert [p for p,s in old.sources.items() if s!=new.sources[p]] == ['knews']
    before,after = old.sources['knews'],new.sources['knews']
    assert after.allowed and after.authorization_fingerprint == before.authorization_fingerprint
    assert after.parser_repair_prior_capture_fingerprint == before.data_fingerprint
    assert (after.http_capacity_source_fingerprint,after.http_capacity_authorization_fingerprint) == native.KNEWS_GRID_HTTP_BINDING[2:]
    import test_news_fulltext as defaults
    monkeypatch.setattr(defaults,'load_config',lambda _: (candidate,new,repo))
    defaults.test_default_configuration_separates_personal_capture_from_licenses()


def test_single_reporter_values_do_not_change_in_multi_reporter_version():
    soup = authored()
    before = extract(soup)
    after = extract_article(str(soup).encode(),source(multi_reporter_entry()),page_url=URL)
    for key in ('body','body_sha256','title','authors_json','published_at_raw','modified_at_raw','published_time_basis'):
        assert before[key] == after[key]


def test_multi_reporter_startup_keeps_failed_head_attempts_and_budget(config,tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    from downloader.news_fulltext_repairs import HEAD_FIELDS
    previous = deepcopy(config);previous.update(root=str(tmp_path),providers=[grid_entry()])
    queue = NewsQueue(tmp_path,PolicyRegistry(previous,base=tmp_path))
    with queue.db:
        queue.add_articles([{'url':URL}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            "attempts=6,due=0,last_checked_at_utc='2026-10-24T10:00:00+00:00' WHERE url=?",(URL,))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',('failure_diagnostic_attempts:v1:knews:20261026','2'))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
    budgets = list(map(tuple,queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'failure_%'")))
    queue.close()
    current = deepcopy(previous);current['providers']=[multi_reporter_entry()]
    queue = NewsQueue(tmp_path,PolicyRegistry(current,base=tmp_path))
    try:
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(URL,)).fetchone())
        assert all(before[k]==after[k] for k in (*HEAD_FIELDS,'body_sha256','parquet_path','lease_owner','lease_until'))
        assert list(map(tuple,queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'failure_%'"))) == budgets
    finally: queue.close()
