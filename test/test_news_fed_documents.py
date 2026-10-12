"""Reviewed Board documents: exact native text, source clocks and attachments."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from downloader.news_fulltext import discovery_links, fetch_article, load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, http_fixture

BASE = 'https://www.federalreserve.gov'
TEXT = 'Original monetary policy text with exact values -12.5, 3.90 percent and 12345. ' * 8


@pytest.fixture
def board():
    return load_config(Path('configs/news_fulltext.json'))[1].sources['fed_board']


@pytest.mark.parametrize('first,second,stamp', [
    ('For Immediate Release<br>July 17, 2002', 'CONTACT: agency', 'July 17, 2002'),
    ('For Immediate Release', 'October 14 , 2004', 'October 14 , 2004'),
    ('Para diseminación inmediata', '13 de abril del  2004', '13 de abril del  2004'),
])
def test_declared_joint_release_cells_exclude_masthead_and_footer(board, first, second, stamp):
    payload = f'''<html><title>Original title</title><body><table width="600">
    <tr><td>Agency navigation</td></tr><tr><td>{first}</td><td>{second}</td></tr>
    <tr><td colspan="2"><div align="center">Original title</div><p>{TEXT}</p></td></tr></table>
    <font size="-1">Last update: December 31, 2099</font></body></html>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/boarddocs/press/bcreg/2002/20020717/default.htm')
    assert record['published_at_raw'] == stamp
    assert record['authors_json'] == '[]' and record['historical_point_in_time'] is False
    assert 'Agency navigation' not in record['body'] and 'Last update:' not in record['body']
    assert record['body'].count('-12.5') == 8


def test_spanish_fecha_is_the_source_clock_not_the_url_or_footer(board):
    payload = f'''<html><title>Spanish announcement</title><body>
    <font size="+1">Fecha: 8 de marzo de 2001</font><table width="560"><tr><td>Press release header</td></tr></table>
    <table width="600"><tr><td><p>{TEXT}</p></td></tr></table><hr><font size="-1">Last update: 2099</font></body></html>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/boarddocs/press/general/2001/200103084/default.htm')
    assert record['published_at_raw'] == '8 de marzo de 2001' and '2099' not in record['body']
    assert 'Press release header' not in record['body']


def test_nested_policy_release_keeps_all_source_instructions(board):
    payload = f'''<html><title>Original policy statement</title><body><table width="600"><tr><td>
    <font><i>Release Date: September 20, 2005</i></font><p>For immediate release</p><p>{TEXT}</p>
    <ul><li>Voting instructions and exact 3-3/4 percent.</li></ul></td></tr></table></body></html>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/boarddocs/press/monetary/2005/20050920/')
    assert record['published_at_raw'] == 'September 20, 2005'
    assert '3-3/4 percent' in record['body'] and TEXT.strip() in record['body']


@pytest.mark.parametrize('kind', ['joint_minutes', 'separate_intro'])
def test_minutes_containers_do_not_promote_meeting_or_footer_clocks(board, kind):
    intro = 'A joint meeting of the Federal Open Market Committee and the Board of Governors was held today.'
    left = '<p>PRESENT: committee participants.</p><p>'+TEXT+'</p><a href="#pagetop">Return to top</a>'
    columns = '<div id="leftText">'+intro+left+'</div>' if kind == 'joint_minutes' else '<p>'+intro+'</p><div id="leftText">'+left+'</div>'
    payload = f'''<div id="content"><div id="printThis"><h1>Minutes of the Federal Open Market Committee</h1>
    <p>April 28-29, 2009</p><div id="TwoColumns">{columns}<div id="rightText">Related sidebar</div></div></div>
    <div id="lastUpdate">Last Update: June 1, 2099</div></div>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/monetarypolicy/fomcminutes20090429.htm')
    assert record['published_at_raw'] is None and 'meeting_date' in record['published_time_basis']
    assert intro in record['body'] and 'PRESENT:' in record['body']
    assert all(t not in record['body'] for t in ['April 28-29', 'Related sidebar', '2099', 'Return to top'])


def projection(clock='For release at 2:00 p.m., EDT, September 16, 2026'):
    return f'''<div id="content"><div class="row"><div class="page-header">Navigation</div></div>
    <div class="row"><div class="col-xs-12 col-sm-8 col-md-8"><h3>September 16, 2026: FOMC Projections materials, accessible version</h3>
    <h3>Accessible version</h3><p>{clock}</p><h4>Summary of Economic Projections</h4><p>{TEXT}</p>
    <div class="data-table"><table><tr><th>Measure</th><th>Value</th></tr><tr><td>GDP</td><td>-12.5</td></tr></table></div>
    <div class="footnotes">Original qualifications must be preserved.</div></div></div>
    <div id="lastUpdate">Last Update: September 17, 2026</div></div>'''.encode()


def test_projection_accessible_tables_keep_numbers_notes_and_declared_release_clock(board):
    record = extract_article(projection(), board, page_url=BASE+'/monetarypolicy/fomcprojtabl20260916.htm')
    assert record['published_at_raw'] == 'For release at 2:00 p.m., EDT, September 16, 2026'
    assert 'timezone_abbreviation_preserved' in record['published_time_basis']
    assert 'GDP' in record['body'] and '-12.5' in record['body'] and 'qualifications' in record['body']
    assert 'Navigation' not in record['body'] and 'Last Update:' not in record['body']


@pytest.mark.parametrize('clock', ['', 'September 16, 2026', 'Last Update: September 16, 2026'])
def test_projection_without_actual_release_declaration_is_not_backdated(board, clock):
    record = extract_article(projection(clock), board, page_url=BASE+'/monetarypolicy/fomcprojtabl20260916.htm')
    assert record['published_at_raw'] is None and 'meeting_date' in record['published_time_basis']
    assert 'GDP' in record['body'] and 'qualifications' in record['body']
    # Source-contract v8 remains unchanged; v9's clock-independent reviewed
    # body scope is a new capture identity rather than a silent ABI rewrite.
    old = replace(board, legacy_release_layout='fed_table_release_v8')
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(projection(clock), old, page_url=BASE+'/monetarypolicy/fomcprojtabl20260916.htm')


@pytest.mark.parametrize('declaration,stamp', [
    ('For release at 12:30 p.m. EDT', 'July 28, 2003'),
    ('For Release at 4 p.m. EST', 'December 19, 2005'),
])
def test_joint_timed_release_keeps_both_native_cells_and_empty_heading_fallback(board, declaration, stamp):
    payload = f'''<html><title>Native Board title</title><body><table width="600">
    <tr><td>Agency masthead</td></tr><tr><td>{declaration}</td><td>{stamp}</td></tr>
    <tr><td colspan="2"><div align="center"> </div><p>{TEXT}</p></td></tr></table>
    <font size="-1">Last update: July 29, 2099</font></body></html>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/boarddocs/press/enforcement/2003/200307282/default.htm')
    assert record['published_at_raw'] == declaration+'\n'+stamp and record['title'] == 'Native Board title'
    assert 'timezone_abbreviation_preserved' in record['published_time_basis']
    assert 'Agency masthead' not in record['body'] and 'Last update:' not in record['body']


@pytest.mark.parametrize('declaration,link,stamp', [
    ('FOR IMMEDIATE RELEASE<br>March 8, 2001', 'Español', 'March 8, 2001'),
    ('Comunicado de prensa<br>8 de marzo de 2001', 'English', '8 de marzo de 2001'),
    ('<i>For Release: December 5, 2005 at 1 p.m. Eastern Time</i>', '', 'December 5, 2005 at 1 p.m. Eastern Time'),
])
def test_direct_declared_clock_excludes_only_translation_link_not_release_time(board, declaration, link, stamp):
    payload = f'''<html><title>Native Board title</title><body><table width="564"><tr><td>Agency masthead</td></tr></table>
    <font size="+1">{declaration}<a href="../translation.htm">{link}</a></font>
    <table width="600"><tr><td><p>{TEXT}</p></td></tr></table><hr>
    <font size="-1">Last update: July 29, 2099</font></body></html>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/boarddocs/press/general/2001/20010308/default.htm')
    assert record['published_at_raw'] == stamp and record['title'] == 'Native Board title'
    assert 'Agency masthead' not in record['body'] and '2099' not in record['body']


def test_pre_2013_projection_values_are_kept_without_promoting_title_date(board):
    payload = projection('').replace(b'September 16, 2026: FOMC Projections materials, accessible version', b'Accessible Version')
    payload = (b'<html><title>The Fed - January 25, 2012 Projection materials, accessible version</title><body>'
        + payload.replace(b'Summary of Economic Projections', b'Figure 1. Central tendencies and ranges of economic projections, 2012-14 and over the longer run')
        + b'</body></html>')
    record = extract_article(payload, board, page_url=BASE+'/monetarypolicy/fomcprojtabl20120125.htm')
    assert 'January 25, 2012' in record['title'] and record['published_at_raw'] is None
    assert 'GDP' in record['body'] and '-12.5' in record['body'] and 'qualifications' in record['body']
    assert 'Last Update:' not in record['body']


def test_accessible_only_heading_uses_actual_page_title_not_calendar_breadcrumb(board):
    payload = (b'<html><title>The Fed - March 20, 2013: FOMC Projections materials, accessible version</title><body>'
        + projection('').replace(b'September 16, 2026: FOMC Projections materials, accessible version', b'Accessible Version')
        + b'</body></html>')
    record = extract_article(payload, board, page_url=BASE+'/monetarypolicy/fomcprojtabl20130320.htm')
    assert record['published_at_raw'] is None and 'March 20, 2013:' in record['title']
    assert '-12.5' in record['body'] and 'Last Update:' not in record['body']


def test_native_h5_projection_summary_keeps_declared_timezone_even_if_unexpected(board):
    payload = projection('For release at 2:00 p.m., EST, March 21, 2018').replace(
        b'<h4>Summary of Economic Projections</h4>',
        b'<h5 class="tablehead" id="t1p1">Advance release of table 1 of the Summary of Economic Projections to be released with the FOMC minutes</h5>')
    record = extract_article(payload, board, page_url=BASE+'/monetarypolicy/fomcprojtabl20180321.htm')
    # Do not silently correct a historical source's EST to contemporary EDT.
    assert record['published_at_raw'] == 'For release at 2:00 p.m., EST, March 21, 2018'
    assert 'GDP' in record['body'] and 'qualifications' in record['body']
    previous = replace(board, legacy_release_layout='fed_table_release_v9')
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(payload, previous, page_url=BASE+'/monetarypolicy/fomcprojtabl20180321.htm')


@pytest.mark.parametrize('declared,title_day,accepted', [('May 04, 2022', 'May 4, 2022', True),
    ('May 04, 2022', 'May 5, 2022', False), ('February 31, 2022', 'February 30, 2022', False),
    ('February 31, 2022', 'February 31, 2022', False)])
@pytest.mark.parametrize('layout', ['full_width', 'two_columns'])
def test_implementation_clock_display_padding_is_not_a_different_calendar_day(board, declared, title_day, accepted, layout):
    main_classes = 'col-xs-12 col-sm-8' if layout == 'two_columns' else 'col-xs-12'
    sidebar = '<div class="col-xs-12 col-sm-4">Related Information</div>' if layout == 'two_columns' else ''
    payload = f'''<div id="content"><div class="row"><div class="heading {main_classes}">
    <p class="article__time">{declared}</p><h3>Implementation Note issued {title_day}</h3></div></div>
    <div class="row"><div class="{main_classes}"><p>Decisions Regarding Monetary Policy Implementation</p><p>{TEXT}</p></div>
    {sidebar}</div></div>'''.encode()
    url = BASE+'/newsevents/pressreleases/monetary20220504a1.htm'
    if accepted:
        record = extract_article(payload, board, page_url=url)
        assert record['published_at_raw'] == declared and 'Related Information' not in record['body']
    else:
        with pytest.raises(NewsError, match='body_not_found'):
            extract_article(payload, board, page_url=url)


def test_implementation_note_selects_main_column_not_the_related_sidebar(board):
    payload = f'''<div id="content"><div class="row"><div class="heading col-xs-12 col-sm-8">
    <p class="article__time">March 19, 2025</p><h3>Implementation Note issued March 19, 2025</h3></div></div>
    <div class="row"><div class="col-xs-12 col-sm-8"><p>Decisions Regarding Monetary Policy Implementation</p><p>{TEXT}</p></div>
    <div class="col-xs-12 col-sm-4">Related Information</div></div><div id="lastUpdate">2099</div></div>'''.encode()
    record = extract_article(payload, board, page_url=BASE+'/newsevents/pressreleases/monetary20250319a1.htm')
    assert record['published_at_raw'] == 'March 19, 2025' and TEXT.strip() in record['body']
    assert 'Related Information' not in record['body'] and '2099' not in record['body']


def test_video_landing_discovers_real_transcript_but_is_not_a_complete_article(board):
    url = BASE+'/monetarypolicy/fomcpresconf20110427.htm'
    pdf = '/mediacenter/files/FOMCpresconf20110427.pdf'
    payload = f'''<div id="content"><div id="printThis"><h1>FOMC: Press Conference on April 27, 2011</h1>
    <div id="leftText3"><video></video></div><a href="{pdf}">Press Conference Transcript (75 KB PDF)</a></div></div>'''.encode()
    with pytest.raises(NewsError, match='source_document_landing_page'):
        extract_article(payload, board, page_url=url)
    assert discovery_links(payload, 'index', board, page_url=url) == [(BASE+pdf, 'Press Conference Transcript (75 KB PDF)', '')]


def test_native_pdf_selected_head_replays_without_invented_time_or_byline(tmp_path):
    import pymupdf
    cfg = deepcopy(load_config(Path('configs/news_fulltext.json'))[0])
    cfg['providers'] = [p for p in cfg['providers'] if p['id'] == 'fed_board']
    cfg.pop('provider_catalog', None)
    cfg.update(exclusive_http_owner=False, adaptive_rate={'enabled': False})
    url = BASE+'/mediacenter/files/FOMCpresconf20110427.pdf'
    with pymupdf.open() as doc:
        doc.new_page().insert_text((60, 80), TEXT[:420])
        payload = doc.tobytes()
    http, _ = http_fixture(cfg, tmp_path, {
        BASE+'/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(payload, headers={'Content-Type': 'application/pdf'}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        queue.accept([fetch_article(http, queue.claim('fed_board', 300))])
        before = queue.db.total_changes
        proof = audit(tmp_path, [url], registry=http.registry)
        assert proof['accepted'] and proof['articles'][0]['current_source_replay_verified']
        assert proof['articles'][0]['published_at_raw'] is None and proof['articles'][0]['authors_json'] == '[]'
        assert queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('previous', ['fed_table_release_v7', 'fed_table_release_v8', 'fed_table_release_v9'])
def test_extended_contract_does_not_change_other_provider_capture_identities(board, previous):
    config, registry, repo = load_config(Path('configs/news_fulltext.json'))
    old = deepcopy(config)
    old.pop('provider_catalog', None)
    source = next(p for p in old['providers'] if p['id'] == 'fed_board')
    source['legacy_release_layout'] = previous
    old_registry = PolicyRegistry(old, base=repo)
    assert old_registry.sources['fed_board'].data_fingerprint != board.data_fingerprint
    for key in set(registry.sources)-{'fed_board'}:
        assert registry.sources[key].data_fingerprint == old_registry.sources[key].data_fingerprint
