"""Known Knews identities grow without replaying completed or failed work."""
from copy import deepcopy
import json
import time

import pytest

from downloader.news_fulltext_knews import KNEWS_KNOWN_URL_LAYOUT, KNEWS_V2_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_knews_native import INTRO, TEXT, entry, page, schema, policy, KNEWS_REVIEWED_URLS

URL = 'https://www.knews.com.tw/news/' + 'A' * 32


def known_entry():
    value = deepcopy(entry())
    value.pop('finite_article_urls')
    value.pop('permitted_author_names')
    value.update(acquisition_mode='continuous', private_known_url_scope='personal_noncommercial_originals_v1',
        embedded_article_layout=KNEWS_KNOWN_URL_LAYOUT, adapter_verification=KNEWS_V2_VERIFICATION)
    value['authorization'].pop('original_author_scope_evidence_urls')
    return value


def authored(name='合成新記者', url=URL):
    soup = page()
    soup.select_one('link[rel=canonical]')['href'] = url
    soup.select_one('meta[property="og:url"]')['content'] = url
    soup.select_one('meta[name=author]')['content'] = name
    soup.select_one('.abc-byline a').string = name
    schema(soup, lambda value: value.update(mainEntityOfPage={'@type': 'WebPage', '@id': url},
        author=[{'@type': 'Person', 'name': name, 'url': 'https://www.knews.com.tw/reporter/renee'}]))
    return soup


def extract(soup):
    return extract_article(str(soup).encode(), policy(known_entry()), page_url=URL)


def test_unseen_exact_identity_and_author_preserve_complete_values_and_distinct_roles():
    result = extract(authored())
    assert INTRO in result['body'] and TEXT in result['body']
    assert json.loads(result['authors_json']) == ['合成新記者']
    roles = json.loads(result['author_attribution_detail_json'])
    assert roles['contract_version'] == 2 and roles['printed_reporter_name'] == '合成作者'
    assert roles['source_header_credit'] == '合成新記者'
    assert not roles['different_names_are_verified_aliases'] and not roles['original_ownership_verified']
    assert not result['per_article_license_verified'] and not result['historical_point_in_time']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(authored()).encode(), policy(), page_url=URL)


@pytest.mark.parametrize('selector,attribute,value', [
    ('meta[name=author]', 'content', '不同署名'),
    ('.abc-byline a', 'href', 'https://unreviewed.example/reporter/renee'),
    ('link[rel=canonical]', 'href', KNEWS_REVIEWED_URLS[0]),
    ('meta[property="og:url"]', 'content', KNEWS_REVIEWED_URLS[0]),
    ('.article-content-inner', 'hidden', ''),
])
def test_new_authors_cannot_relax_identity_attribution_or_visibility(selector, attribute, value):
    soup = authored()
    soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('url', [URL + '?id=1', URL + '#body', URL.replace('www.', ''),
    URL.replace('/news/', '/reporter/'), URL.lower(), URL.replace('https:', 'http:')])
def test_no_query_fragment_other_origin_or_guessed_route(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(authored(url=url)).encode(), policy(known_entry()), page_url=url)


@pytest.mark.parametrize('name', ['中央社', 'Reuters', 'AFP'])
def test_matching_three_credit_fields_are_not_third_party_ownership(name):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(authored(name))


def test_explicit_reprint_notice_is_preserved_as_scope_failure():
    soup = authored()
    soup.select_one('.article-content-inner > p').string = '本文轉載自第三方媒體。' + TEXT
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


@pytest.mark.parametrize('field,value', [
    ('feeds', ['https://www.knews.com.tw/feed']), ('domains', ['event.knews.com.tw']),
    ('finite_article_urls', [URL]), ('permitted_author_names', ['任意樣本姓名']),
    ('article_selectors', ['article']), ('private_known_url_scope', ''),
    ('require_author_attribution', False), ('allow_reviewed_supplements', True),
    ('adapter_verification', 'unreviewed'),
])
def test_known_urls_do_not_enable_discovery_or_weaken_capture_contract(field, value):
    candidate = known_entry()
    candidate[field] = value
    with pytest.raises(ValueError):
        policy(candidate)


def test_http_owner_binding_requires_exact_capture_authorization_and_pacing(monkeypatch):
    source = policy(known_entry())
    binding = (source.data_fingerprint, source.authorization_fingerprint, '1' * 64, '2' * 64)
    monkeypatch.setattr('downloader.news_fulltext_knews.KNEWS_KNOWN_HTTP_BINDING', binding)
    assert policy(known_entry()).http_capacity_source_fingerprint == '1' * 64
    assert policy(known_entry()).http_capacity_authorization_fingerprint == '2' * 64
    for field, value in [('interval_seconds', 11), ('parallelism', 2)]:
        changed = known_entry()
        changed[field] = value
        assert not policy(changed).http_capacity_source_fingerprint
    changed = known_entry()
    changed['authorization']['scope'] += ' changed'
    assert not policy(changed).http_capacity_authorization_fingerprint


@pytest.mark.parametrize('state', ['body_not_found', 'article_attribution_not_verified',
    'article_title_not_verified', 'article_identity_not_verified', 'retry', 'paywall_or_login'])
def test_only_unrequested_missing_metadata_activates_and_prior_evidence_is_immutable(config, tmp_path, monkeypatch, state):
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('Queue reconciliation must not issue HTTP'))
    settings = {**config, 'providers': [entry()]}
    old = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    failed_url = 'https://www.knews.com.tw/news/' + 'B' * 32
    with old.db:
        old.add_articles([{'url': url} for url in [KNEWS_REVIEWED_URLS[0], URL, failed_url]])
        # Reproduce the deployed pristine, never-requested metadata head.
        old.db.execute("UPDATE articles SET state='awaiting_authorization' WHERE url=?", (URL,))
        old.db.execute("UPDATE articles SET state='complete',body_sha256=?,body_bytes=100 WHERE url=?",
            ('a' * 64, KNEWS_REVIEWED_URLS[0]))
        old.db.execute("UPDATE articles SET state=?,error_code=?,attempts=3,due=? WHERE url=?",
            (state, state, time.time() + 3600, failed_url))
    before = {row['url']: dict(row) for row in old.db.execute('SELECT * FROM articles')}
    assert before[URL]['state'] == 'awaiting_authorization'
    old.close()
    revised = NewsQueue(tmp_path, PolicyRegistry({**settings, 'providers': [known_entry()]}, base=tmp_path))
    try:
        revised.reconcile_policy()
        after = {row['url']: dict(row) for row in revised.db.execute('SELECT * FROM articles')}
        assert after[KNEWS_REVIEWED_URLS[0]] == before[KNEWS_REVIEWED_URLS[0]]
        assert after[failed_url] == before[failed_url]
        assert after[URL]['state'] == 'pending' and after[URL]['attempts'] == 0
        assert all(after[URL][key] == value for key, value in before[URL].items()
            if key not in {'state', 'due', 'error_code'})
    finally:
        revised.close()
