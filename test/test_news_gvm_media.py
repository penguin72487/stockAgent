"""Native empty media grammar; all article prose is synthetic."""
from copy import deepcopy
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_gvm import (
    GVM_MEDIA_LAYOUT, GVM_UNTITLED_MEDIA_LAYOUT, GVM_V7_VERIFICATION,
    gvm_editor_grammar_metadata,
)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_known_urls import authored, extract, known_entry
from test_news_gvm_native import MODIFIED, PUBLISHED, TEXT, URL, policy, schema


FIRSTORY = ('<iframe title="Firstory" src="https://open.firstory.fm/embed/story/abc123" '
            'frameborder="0" width="100%" height="180" scrolling="no"></iframe>')
YOUTUBE = ('<div class="video-container">\n<iframe class="embed-responsive-item" alt="" '
           'src="https://www.youtube.com/embed/Abc123_xyz-9?rel=0" frameborder="0" '
           'allowfullscreen=""></iframe>\n</div>')


def media_entry():
    return {**known_entry(), 'embedded_article_layout': GVM_MEDIA_LAYOUT}


def media_page(markup=FIRSTORY + YOUTUBE):
    soup = authored('合成作者')
    fragment = BeautifulSoup('<section>' + markup + '</section>', 'lxml')
    for node in list(fragment.section.contents):
        soup.select_one('.article-content').append(node)
    return soup


def untitled_media_entry():
    from test_news_gvm_magazine import magazine_entry
    return {**magazine_entry(), 'embedded_article_layout': GVM_UNTITLED_MEDIA_LAYOUT,
        'adapter_verification': GVM_V7_VERIFICATION}


@pytest.mark.parametrize('periodical', [False, True])
@pytest.mark.parametrize('host', ['open.firstory.me', 'open.firstory.fm'])
def test_missing_player_title_is_not_missing_written_article(periodical, host):
    from test_news_gvm_magazine import magazine_entry, magazine_page
    soup = magazine_page() if periodical else media_page('')
    baseline = extract(soup, policy(magazine_entry()))
    markup = FIRSTORY.replace(' title="Firstory"', '').replace('open.firstory.fm', host)
    soup.select_one('.article-content').append(BeautifulSoup(markup, 'lxml').iframe)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(magazine_entry()))
    result = extract(soup, policy(untitled_media_entry()))
    for field in ('body', 'body_sha256', 'authors_json', 'title', 'published_at_raw', 'modified_at_raw'):
        assert result[field] == baseline[field]
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_empty_untitled_firstory_story_player_verified']
    assert not notices['media_or_scripts_requested_or_executed']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('replacement', [
    ('scrolling="no"', 'scrolling="no" title="Other player"'),
    ('scrolling="no"', 'scrolling="no" title=""'),
    ('scrolling="no"', 'scrolling="no" onload="anything()"'),
    ('scrolling="no"', 'scrolling="no" hidden'),
    ('scrolling="no"', 'scrolling="no" style="display:none"'),
    ('open.firstory.fm', 'other.example'), ('https:', 'http:'),
    ('/embed/story/', '/other/'), ('/abc123', '/abc123?token=1'),
    ('/abc123', '/abc123#other'), ('</iframe>', 'fallback prose</iframe>'),
    ('</iframe>', '<!-- fallback --></iframe>'),
    ('</iframe>', '<p>native prose</p></iframe>'),
])
def test_untitled_player_cannot_hide_fallback_prose_or_relax_origin_visibility(replacement):
    markup = FIRSTORY.replace(' title="Firstory"', '').replace(*replacement)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(media_page(markup), policy(untitled_media_entry()))


def test_v7_preserves_every_v6_result_except_named_identity_without_new_player():
    from test_news_gvm_magazine import magazine_entry, magazine_page
    for soup in (media_page(), magazine_page()):
        old, new = extract(soup, policy(magazine_entry())), extract(soup, policy(untitled_media_entry()))
        assert {k: v for k, v in old.items() if k != 'article_identity_basis'} == {
            k: v for k, v in new.items() if k != 'article_identity_basis'}


def test_v7_keeps_exact_http_capacity_and_v6_repair_predecessor():
    from test_news_gvm_magazine import magazine_entry
    old, new = policy(magazine_entry()), policy(untitled_media_entry())
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for key, value in [('interval_seconds', 5), ('parallelism', 2)]:
        different = policy({**untitled_media_entry(), key: value})
        assert different.http_capacity_source_fingerprint != old.http_capacity_source_fingerprint
        assert different.parser_repair_prior_capture_fingerprint != old.data_fingerprint


def test_registered_default_source_contract_with_v7_candidate(monkeypatch):
    from pathlib import Path
    from downloader.news_fulltext import load_config
    from downloader.news_fulltext_policy import PolicyRegistry
    import test_news_fulltext as checks
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings)
    candidate.pop('provider_catalog', None)
    entry = next(p for p in candidate['providers'] if p['id'] == 'gvm')
    entry.update(embedded_article_layout=GVM_UNTITLED_MEDIA_LAYOUT, adapter_verification=GVM_V7_VERIFICATION)
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(checks, 'load_config', lambda _: (candidate, registry, repo))
    checks.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('markup', [FIRSTORY, YOUTUBE, FIRSTORY + YOUTUBE,
    FIRSTORY.replace('open.firstory.fm', 'open.firstory.me').replace('title="Firstory"', 'title="Firstory" style="border:0"')])
def test_empty_players_preserve_exact_written_body_credit_and_clocks(markup):
    base = extract(authored('合成作者'))
    result = extract(media_page(markup), policy(media_entry()))
    for key in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert result[key] == base[key]
    assert TEXT in result['body']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert json.loads(result['author_attribution_detail_json'])['contract_version'] == 3
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_editor_media_grammar_version'] == 3
    assert not notices['media_or_scripts_requested_or_executed']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    # Closed ABI35 and V2 still describe the original refusal, not V3 acceptance.
    if 'open.firstory.fm' in markup or 'www.youtube.com' in markup:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract(media_page(markup))
        assert gvm_editor_grammar_metadata(media_page(markup), page_url=URL)['observed']


@pytest.mark.parametrize('markup', [
    FIRSTORY.replace('</iframe>', 'fallback prose</iframe>'),
    FIRSTORY.replace('</iframe>', '<!-- fallback --></iframe>'),
    FIRSTORY.replace('title="Firstory"', 'title="Firstory" onload="anything()"'),
    FIRSTORY.replace('open.firstory.fm', 'foreign.example'),
    FIRSTORY.replace('open.firstory.fm', '[malformed'),
    FIRSTORY.replace('https:', 'http:'),
    FIRSTORY.replace('/abc123', '/abc123?token=1'),
    FIRSTORY.replace('/abc123', '/abc123#fragment'),
    FIRSTORY.replace('open.firstory.fm', 'open.firstory.fm@foreign.example'),
    FIRSTORY.replace('title="Firstory"', 'title="Firstory" hidden'),
    FIRSTORY.replace('title="Firstory"', 'title="Firstory" style="display:none"'),
    YOUTUBE.replace('<div ', '<div id="other" '),
    YOUTUBE.replace('<iframe ', '<p>visible prose</p><iframe '),
    YOUTUBE.replace('<iframe ', '<!-- fallback --><iframe '),
    YOUTUBE.replace('</iframe>', 'fallback prose</iframe>'),
    YOUTUBE.replace('</iframe>', '<!-- fallback --></iframe>'),
    YOUTUBE.replace('www.youtube.com', 'foreign.example'),
    YOUTUBE.replace('www.youtube.com', '[malformed'),
    YOUTUBE.replace('https:', 'http:'),
    YOUTUBE.replace('/embed/', '/watch/'),
    YOUTUBE.replace('?rel=0', '?rel=0#fragment'),
    YOUTUBE.replace('alt=""', 'alt="" onload="anything()"'),
    YOUTUBE.replace('class="video-container"', 'class="video-container" aria-hidden="true"'),
    YOUTUBE.replace('alt=""', 'alt="" hidden'),
    '<iframe src="https://www.youtube.com/embed/Abc123_xyz-9"></iframe>',
    '<div class="video-container"><div>' + YOUTUBE + '</div></div>',
    '<div class="video-container"><script>anything()</script></div>',
])
def test_unreviewed_players_and_nonempty_wrappers_remain_errors(markup):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(media_page(markup), policy(media_entry()))


@pytest.mark.parametrize('change,error', [
    (lambda s: s.select_one('.article-head').__setitem__('data-is-accessible', '0'), 'paywall_or_login'),
    (lambda s: schema(s, lambda a: a['author'][0].update(name='different')), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('.article-content > p').append('本文轉載自別家媒體'), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('meta[property="article:published_time"]').__setitem__('content', '2020-12-10'), 'invalid_bulk_publication_clock'),
    (lambda s: s.select_one('link[rel=canonical]').__setitem__('href', URL + '9'), 'article_identity_not_verified'),
])
def test_media_grammar_cannot_override_article_access_identity_or_attribution(change, error):
    soup = media_page()
    change(soup)
    with pytest.raises(NewsError, match=error):
        extract(soup, policy(media_entry()))


@pytest.mark.parametrize('field,value', [
    ('feeds', ['https://www.gvm.com.tw/feed']), ('sitemaps', ['https://www.gvm.com.tw/sitemap.xml']),
    ('domains', ['esg.gvm.com.tw']), ('require_author_attribution', False),
    ('finite_article_urls', [URL]), ('private_known_url_scope', ''),
])
def test_media_revision_does_not_expand_source_scope(field, value):
    value_entry = media_entry()
    value_entry[field] = value
    with pytest.raises(ValueError):
        policy(value_entry)


def test_media_repair_preserves_only_unchanged_http_capacity_identity():
    old, new = policy(known_entry()), policy(media_entry())
    assert new.data_fingerprint != old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for field, value in [('interval_seconds', 5), ('parallelism', 2)]:
        changed = policy({**media_entry(), field: value})
        assert changed.http_capacity_source_fingerprint != old.data_fingerprint
    entry = media_entry()
    entry['authorization']['scope'] += ' Changed evidence.'
    changed = policy(entry)
    assert changed.http_capacity_source_fingerprint != old.data_fingerprint
    assert changed.authorization_fingerprint != old.authorization_fingerprint


def test_media_reconciliation_preserves_all_existing_heads(config, tmp_path):
    from downloader.news_fulltext_policy import PolicyRegistry
    settings = deepcopy(config)
    settings['providers'] = [known_entry()]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    states = ['complete', 'article_identity_not_verified', 'article_rights_or_scope_excluded',
              'paywall_or_login', 'retry', 'body_not_found', 'article_attribution_not_verified']
    with queue.db:
        for i, state in enumerate(states):
            url = URL + str(i)
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=? WHERE url=?',
                             (state, 'http_429' if state == 'retry' else state, 10**10, url))
    before = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    queue.close()
    settings['providers'] = [media_entry()]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
    finally:
        queue.close()
