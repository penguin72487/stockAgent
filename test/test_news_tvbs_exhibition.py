"""Two native segmented TVBS reports: all content, real credit, date-only."""
from copy import deepcopy
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_tvbs import TVBS_EXHIBITION_LAYOUTS, tvbs_request_identity
from test_news_fulltext import FakeResponse, http_fixture
from test_news_tvbs import config, extract


URLS = tuple('https://news.tvbs.com.tw' + path for path in TVBS_EXHIBITION_LAYOUTS)
TITLE = '原生專題：原始股數123456、負值-12.5%與比率0.75'
COPYRIGHT = '© TVBS Media Inc. All Rights Reserved.｜聯利媒體股份有限公司'
NOTICE = '本篇原始風險提示：實際值不代表未來報酬，請自行查證。'


def page(url, parser='lxml'):
    path = url.removeprefix('https://news.tvbs.com.tw')
    sections = []
    for section in TVBS_EXHIBITION_LAYOUTS[path]:
        if section.startswith('menu'):
            css, text = 'menu', '<a>導覽不是本文</a>'
        elif section.startswith('article01'):
            css = 'article01'
            text = '<div><h2 class="mbr-section-title">' + TITLE + '</h2>'
            text += '<p class="mbr-text display-4">◎ 報導／研究記者<br>◎ 發布日期／2026.06.03</p></div>'
        elif section.startswith('image3'):
            css, text = 'image3', '<img src="assets/images/original.jpg"><p class="mbr-description display-4">原始圖片署名，不是本文</p>'
        elif section.startswith('content01'):
            css = 'content1'
            text = '<h3>原始分段小標題' + section + '</h3><p class="mbr-text display-7">'
            text += '原始全文分段' + section + '：股數123456，負值-12.5%，比率0.75。<br>'
            text += '第二段<strong>原始強調文字</strong>不變，並非推薦新聞或摘要。' + NOTICE + '</p>'
        elif section.startswith('slider2'):
            css, text = 'slider2', '<p>輪播鄰篇不是本文</p>'
        else:
            css = 'footer7'
            text = '<p class="card-text display-4">新聞部 網路新聞中心 其他記者・研究記者・另一記者 編播中心 其他名單</p>'
            text += '<p class="mbr-text display-4">' + COPYRIGHT + '</p>'
        sections.append('<section id="' + section + '" class="' + css + '">' + text + '</section>')
    return BeautifulSoup('<html><head><title>' + TITLE + '｜TVBS新聞網</title>'
        '<link rel="canonical" href="' + url + '"><meta property="og:image" content="assets/images/original-meta.jpg">'
        '</head><body>' + ''.join(sections) + '<script>摘要不得冒充原文</script></body></html>', parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', URLS)
def test_all_native_segments_preserved_real_reporter_day_and_no_invented_clock(url, parser):
    soup = page(url, parser)
    original = str(soup)
    result = extract(soup, url, parser)
    ids = [section for section in TVBS_EXHIBITION_LAYOUTS[url.removeprefix('https://news.tvbs.com.tw')]
        if section.startswith('content01')]
    for section in ids:
        assert result['body'].count('原始全文分段' + section) == 1
        assert '原始分段小標題' + section in result['body']
    assert result['body'].count('股數123456，負值-12.5%，比率0.75。') == len(ids)
    assert result['body'].count(NOTICE) == len(ids)
    assert result['published_at_raw'] == '2026.06.03' and result['modified_at_raw'] is None
    assert 'publisher_visible_date_only' in result['published_time_basis']
    assert 'intraday_time_unknown' in result['published_time_basis'] and not result['historical_point_in_time']
    assert result['authors_json'] == '["研究記者"]' and result['title'] == TITLE
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert not any(s in result['body'] for s in ('导览', '導覽', '輪播鄰篇', '摘要不得', '◎ 報導', '新聞部', '原始圖片署名'))
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[0]['text'] == '◎ 報導／研究記者 ◎ 發布日期／2026.06.03'
    assert notices[2]['text'] == COPYRIGHT and not result['per_article_license_verified']
    assert len(json.loads(result['verified_image_captions_json'])) == (1 if url == URLS[0] else 4)
    assert str(soup) == original


@pytest.mark.parametrize('url', URLS)
@pytest.mark.parametrize('change', ['canonical', 'title', 'other_heading', 'other_author', 'wire_author',
    'missing_staff', 'duplicate_staff', 'wrong_staff_class', 'wrong_copyright', 'missing_credit', 'invalid_day', 'exact_time_injected',
    'schema_injected', 'hidden_credit', 'hidden_prose', 'hidden_caption', 'missing_segment',
    'duplicate_segment', 'unknown_segment', 'segment_reordered', 'wrong_class', 'missing_paragraph',
    'hidden_strong_prose', 'script_in_prose', 'wire_credit'])
def test_neighboring_incomplete_hidden_foreign_or_manufactured_evidence_rejected(url, change):
    soup = page(url)
    header = soup.select_one('section.article01')
    prose = soup.select('section.content1')
    credit = header.select_one('p')
    if change == 'canonical':
        soup.select_one('link')['href'] = URLS[1 if url == URLS[0] else 0]
    elif change == 'title':
        soup.title.string = '另一篇｜TVBS新聞網'
    elif change == 'other_heading':
        header.select_one('h2').string = '另一篇'
    elif change in {'other_author', 'wire_author', 'invalid_day', 'exact_time_injected'}:
        credit.clear()
        credit.string = {'other_author': '◎ 報導／別人 ◎ 發布日期／2026.06.03',
            'wire_author': '◎ 報導／中央社 ◎ 發布日期／2026.06.03',
            'invalid_day': '◎ 報導／研究記者 ◎ 發布日期／2026.02.30',
            'exact_time_injected': '◎ 報導／研究記者 ◎ 發布日期／2026.06.03 09:00:00'}[change]
    elif change == 'missing_staff':
        soup.select_one('section.footer7 p').string = '未核實原出處，沒有新聞部名單'
    elif change == 'duplicate_staff':
        node = soup.select_one('section.footer7 p'); node.insert_after(deepcopy(node))
    elif change == 'wrong_staff_class':
        soup.select_one('section.footer7 p')['class'] = ['unverified']
    elif change == 'wrong_copyright':
        soup.select('section.footer7 p')[1].string = '其他出版者'
    elif change == 'missing_credit':
        credit.decompose()
    elif change == 'schema_injected':
        script = soup.new_tag('script', type='application/ld+json'); script.string = '{}'; soup.head.append(script)
    elif change == 'hidden_credit':
        credit['aria-hidden'] = 'true'
    elif change == 'hidden_prose':
        prose[0]['style'] = 'display:none'
    elif change == 'hidden_caption':
        soup.select_one('section.image3 p')['hidden'] = ''
    elif change == 'missing_segment':
        prose[0].decompose()
    elif change == 'duplicate_segment':
        prose[0].insert_after(deepcopy(prose[0]))
    elif change == 'unknown_segment':
        added = deepcopy(prose[0]); added['id'] = 'content01-unknown'; prose[0].insert_after(added)
    elif change == 'segment_reordered':
        prose[0].insert_before(prose[-1].extract())
    elif change == 'wrong_class':
        prose[0]['class'] = ['unverified']
    elif change == 'missing_paragraph':
        prose[0].select_one('p').decompose()
    elif change == 'hidden_strong_prose':
        prose[0].select_one('strong')['aria-hidden'] = 'true'
    elif change == 'script_in_prose':
        prose[0].append(soup.new_tag('script'))
    else:
        prose[0].select_one('p').string = '（中央社台北3日電）未核實轉載'
    with pytest.raises(NewsError):
        extract(soup, url)


@pytest.mark.parametrize('url', ['https://news.tvbs.com.tw/exhibition/ai-trends-2026/article_2-3.html',
    URLS[0].replace('ai-trends-2026', 'ai-trends-2027'), URLS[0].replace('news.tvbs', 'cars.tvbs'),
    URLS[1].replace('news.tvbs', 'supertaste.tvbs'), URLS[0] + '?article=other'])
def test_two_reviewed_exhibition_paths_are_not_a_wildcard_or_alternate_host(url):
    assert tvbs_request_identity(url) is None


@pytest.mark.parametrize('url', URLS)
def test_normal_http_queue_receipt_restart_accepts_no_manufactured_timestamp(url, tmp_path):
    http, session = http_fixture(config(), tmp_path, {url: FakeResponse(str(page(url)).encode()),
        'https://news.tvbs.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': url}])
        row = queue.claim_url(url, 300)
        article = fetch_article(http, row)
        assert article['requested_url'] == article['final_url'] == url
        assert article['published_at_raw'] == '2026.06.03' and article['modified_at_raw'] is None
        assert [request[0] for request in session.calls if not request[0].endswith('/robots.txt')] == [url]
        queue.accept([article]); queue.close(); queue = NewsQueue(tmp_path, http.registry)
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert verify_bodies(tmp_path)['accepted'] and not queue.db.execute('SELECT 1 FROM discovery').fetchone()
    finally:
        queue.close(); http.close()
