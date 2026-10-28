"""Attribution format must not replace the reviewed personal-download scope."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader import news_fulltext_contentplatform as native
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from downloader import news_fulltext_repairs as repair
from test_news_contentplatform import URL, RAW, page, source, v3_entry
from test_news_reviewed_repairs import failure, head


def entry():
    candidate = v3_entry()
    candidate.update(embedded_article_layout=native.CONTENTPLATFORM_KNOWN_NATIVE_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V4_VERIFICATION, note=native.CONTENTPLATFORM_V4_NOTE)
    candidate['authorization'].update(scope=native.CONTENTPLATFORM_V4_AUTH_SCOPE, reviewed_on='2026-10-26')
    return candidate


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('credit', [
    '【許家源記者 / 綜合報導】', '記者許家源／台北報導', '產業研究編輯部',
    '【別人記者 / 綜合報導】', '新聞來源：原公司／數據資料', '',
])
def test_native_account_and_body_credits_are_independent_original_declarations(parser, credit):
    raw = page(credit=credit)
    if credit != '【許家源記者 / 綜合報導】':
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
            extract_article(raw, source(v3_entry()), page_url=URL)
    record = extract_article(raw, replace(source(entry()), html_parser=parser), page_url=URL)
    assert record['body'] == '\n'.join(x for x in (credit, RAW, '新聞來源:原公司') if x)
    assert json.loads(record['authors_json']) == ['許 家源']
    assert record['author_attribution_detail_json'] is None
    assert record['published_at_raw'] == '2026-10-14T10:43:17+08:00'
    assert record['modified_at_raw'] == '2026-10-14T10:43:19+08:00'
    assert record['historical_point_in_time'] is False
    assert not record['per_article_license_verified']


@pytest.mark.parametrize('changes,error', [
    ({'canonical': URL.replace('533649', '429527')}, 'article_identity_not_verified'),
    ({'entity': URL.replace('533649', '429527')}, 'article_identity_not_verified'),
    ({'profile': 'https://example.org/articles/author/native/'}, 'article_attribution_not_verified'),
    ({'published': '2026-10-14T10:43:17'}, 'invalid_bulk_publication_clock'),
    ({'modified': '2026-10-13T10:43:19+08:00'}, 'invalid_bulk_publication_clock'),
    ({'visible': '2026-10-15'}, 'invalid_bulk_publication_clock'),
    ({'body_attrs': 'hidden'}, 'article_identity_not_verified'),
    ({'extra': '<p style="display:none">隱藏原文</p>'}, 'article_identity_not_verified'),
    ({'extra': '<iframe src="/not-fetched"></iframe>'}, 'article_identity_not_verified'),
    ({'extra': '<!--unknown content-->'}, 'article_identity_not_verified'),
    ({'extra': '<div>未經驗證的正文結構</div>'}, 'article_identity_not_verified'),
    ({'extra': '<div class="paywall">付費</div>'}, 'paywall_or_login'),
    ({'credit': '中央社記者'}, 'article_rights_or_scope_excluded'),
    ({'author': '中央社'}, 'article_rights_or_scope_excluded'),
])
def test_scope_change_preserves_identity_visibility_clocks_and_deferred_CNA(changes, error):
    with pytest.raises(NewsError) as caught:
        extract_article(page(**changes), source(entry()), page_url=URL)
    assert caught.value.code == error


def test_native_author_must_still_match_visible_and_microdata():
    raw = page().replace(b'content="'+ '許 家源'.encode()+b'"', b'content="different-author"')
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(raw, source(entry()), page_url=URL)


def test_v4_scope_requires_exact_review_not_just_layout_label():
    candidate = entry()
    candidate['authorization']['scope'] += ' extra permission'
    with pytest.raises(ValueError):
        source(candidate)


def test_only_exact_reviewed_production_change_inherits_old_HTTP_scope():
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    candidate = next(p for p in config['providers'] if p['id'] == 'contentplatform')
    candidate.update(embedded_article_layout=native.CONTENTPLATFORM_KNOWN_NATIVE_LAYOUT,
        adapter_verification=native.CONTENTPLATFORM_V4_VERIFICATION, note=native.CONTENTPLATFORM_V4_NOTE)
    candidate['authorization'].update(scope=native.CONTENTPLATFORM_V4_AUTH_SCOPE, reviewed_on='2026-10-26')
    policy = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert native.contentplatform_known_native_source(policy)
    assert (policy.data_fingerprint, policy.authorization_fingerprint,
        policy.http_capacity_source_fingerprint, policy.http_capacity_authorization_fingerprint,
        policy.parser_repair_prior_capture_fingerprint) == native.CONTENTPLATFORM_V4_HTTP_BINDING
    candidate['adapter_verification'] += '_unreviewed'
    changed = PolicyRegistry(config, base=repo).sources['contentplatform']
    assert changed.data_fingerprint != policy.data_fingerprint
    assert changed.http_capacity_source_fingerprint != policy.http_capacity_source_fingerprint
    assert not changed.parser_repair_prior_capture_fingerprint


def test_retained_unknown_geometry_is_diagnostic_only_and_uses_current_scope():
    raw = page(credit='編輯部原始署名', extra='<div><p>未知結構</p></div>')
    response = Response(URL, 200, {'content-type': 'text/html'}, raw)
    permit = native.contentplatform_private_failure_permit(response, source(entry()))
    assert permit['native_publisher_byline_and_explicit_clocks_verified']
    assert not permit['body_geometry_accepted'] and not permit['article_accepted']
    assert 'signed_reporter_and_explicit_clocks_verified' not in permit
    with pytest.raises(NewsError):
        extract_article(raw, source(entry()), page_url=URL)


def test_scope_repair_keeps_old_heads_and_once_marker_across_restart(tmp_path, monkeypatch):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [entry()]
    policy = PolicyRegistry(config, base=repo)
    cp = policy.sources['contentplatform']
    # A test-local reviewed binding cannot grant production admission.
    monkeypatch.setattr(native, 'CONTENTPLATFORM_V4_HTTP_BINDING',
        (cp.data_fingerprint, cp.authorization_fingerprint, 'a'*64, 'b'*64, 'c'*64))
    policy = PolicyRegistry(config, base=repo)
    root = tmp_path/'scope'
    with closing(NewsQueue(root, policy)) as queue:
        before = failure(queue, URL, state='article_rights_or_scope_excluded', error_code='article_rights_or_scope_excluded')
        refused = failure(queue, URL.replace('533649', '533650'), state='http_403', error_code='http_403')
        future = failure(queue, URL.replace('533649', '533651'), state='article_rights_or_scope_excluded',
            error_code='article_rights_or_scope_excluded', due=9000000000)
        recent = failure(queue, URL.replace('533649', '533652'), state='article_rights_or_scope_excluded',
            error_code='article_rights_or_scope_excluded', last_checked_at_utc='2026-10-26T10:00:00+00:00')
        stored = failure(queue, URL.replace('533649', '533653'), body_sha256='d'*64, body_bytes=12, parquet_path='retained.parquet')
        urls = [before['url'], refused['url'], future['url'], recent['url'], stored['url']]
        submitted = repair.submit_repairs(root, policy, urls, reason='Reviewed personal native written scope')
        assert submitted['submitted_targets'] == 1
        assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 1}
        assert head(queue, URL) == dict(before, state='retry')
        for row in (refused, future, recent, stored):
            assert head(queue, row['url']) == row
        queue.db.execute('UPDATE articles SET state=? WHERE url=?', ('article_rights_or_scope_excluded', URL))
        queue.db.commit()
    with closing(NewsQueue(root, policy)) as queue:
        retry = repair.submit_repairs(root, policy, [URL], reason='Must not replay the same repair')
        assert not retry['submitted']
        assert retry['excluded'][0]['reason'] == 'already_admitted_for_parser'
