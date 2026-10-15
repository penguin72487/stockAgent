"""Explicit article-local syndication is not a missing reporter or a body."""
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_public_originals import ftv_declared_public_canonical
from downloader.news_fulltext_queue import NewsQueue
from test_news_public_originals import FTV_URL, TEXT, ftv_page, ftv_subsite_page


@pytest.fixture
def source():
    return load_config(Path('configs/news_fulltext.json'))[1].sources['ftvnews']


@pytest.mark.parametrize('credit', ['（中央社）', '(中央社)', '<span>（中央社）</span>'])
@pytest.mark.parametrize('own_credit', [True, False])
def test_ftv_explicit_native_syndication_is_scope_not_missing_author(source, credit, own_credit):
    payload = ftv_page().decode()
    if not own_credit:
        payload = payload.replace('<p>即時中心／綜合報導</p>', '')
    payload = payload.replace('<p class="aphorism">', f'<p>{credit}</p><p class="aphorism">')
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$') as outcome:
        extract_article(payload.encode(), source, page_url=FTV_URL)
    assert outcome.value.ftv_native_scope_witness == 'standalone_visible_CNA_credit_v1'


@pytest.mark.parametrize('context', [
    '<p>根據中央社報導，這是原文引用，不是轉載署名。</p>',
    '<p>（中央社）只是本段討論的引文。</p>',
    '<p>（中央社）<br>這是同段正文而非獨立來源標籤。</p>',
    '<blockquote><p>（中央社）</p></blockquote>',
    '<figure><p>（中央社）</p></figure>',
    '<figcaption><p>（中央社）</p></figcaption>',
    '<aside><p>（中央社）</p></aside>',
    '<nav><p>（中央社）</p></nav>',
    '<footer><p>（中央社）</p></footer>',
    '<p><img alt="圖片來源"><span>（中央社）</span></p>',
])
def test_ftv_article_mentions_quotes_captions_and_nonbody_roles_are_not_syndication(source, context):
    payload = ftv_page().replace(b'<p class="aphorism">', (context + '<p class="aphorism">').encode())
    result = extract_article(payload, source, page_url=FTV_URL)
    assert TEXT in result['body']
    assert json.loads(result['author_attribution_detail_json'])['visible_newsroom_credit_raw'] == '即時中心／綜合報導'


def test_ftv_pagewide_footer_never_decides_this_article_scope(source):
    result = extract_article(ftv_page(extra='<footer><p>（中央社）</p></footer>'), source, page_url=FTV_URL)
    assert TEXT in result['body']


@pytest.mark.parametrize('hidden', ['<p hidden>（中央社）</p>', '<p><span hidden>（中央社）</span></p>'])
def test_ftv_hidden_syndication_marker_is_not_verified_provenance(source, hidden):
    payload = ftv_page().replace(b'<p class="aphorism">', (hidden + '<p class="aphorism">').encode())
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(payload, source, page_url=FTV_URL)


def test_ftv_wrong_current_identity_is_not_reclassified_from_unrelated_footer(source):
    payload = ftv_page(canonical=FTV_URL + 'OTHER').replace(b'<p class="aphorism">',
                         '<p>（中央社）</p><p class="aphorism">'.encode())
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(payload, source, page_url=FTV_URL)


def test_ftv_current_paywall_is_not_reclassified_as_public_syndication(source):
    payload = ftv_page(free=False).replace(b'<p class="aphorism">',
                         '<p>（中央社）</p><p class="aphorism">'.encode())
    with pytest.raises(NewsError, match='^paywall_or_login$'):
        extract_article(payload, source, page_url=FTV_URL)


def test_ftv_syndicated_subsite_does_not_authorize_following_public_canonical(source):
    url = FTV_URL.replace('www.', 'finance.')
    payload = ftv_subsite_page(url, credit='（中央社）')
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        ftv_declared_public_canonical(payload, source, page_url=url)


@pytest.mark.parametrize('credit,expected', [('（中央社）', 1), ('CNEWS匯流新聞網記者／台北報導', 0),
                                           ('圖、文／CTWANT', 0)])
def test_ftv_normal_failure_owner_counts_only_the_exact_nonprose_witness(tmp_path, source, credit, expected):
    registry = load_config(Path('configs/news_fulltext.json'))[1]
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': FTV_URL}])
        queue.sync_finite_cohorts()
        row = queue.claim_url(FTV_URL, 300)
        assert row is not None
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$') as outcome:
            extract_article(ftv_page(credit=credit), source, page_url=FTV_URL)
        queue.failed(row, outcome.value, 6)
        counts = queue.metrics.snapshot()['providers']['ftvnews']['counts']
        assert counts.get('native_standalone_CNA_credit_exclusions_v1', 0) == expected
        current = queue.db.execute('SELECT state,body_sha256,last_checked_at_utc FROM articles WHERE url=?',
                                   (FTV_URL,)).fetchone()
        assert current['state'] == 'article_rights_or_scope_excluded'
        assert current['body_sha256'] is None and current['last_checked_at_utc']
    finally:
        queue.close()
