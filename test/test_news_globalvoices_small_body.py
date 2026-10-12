"""Synthetic native-layout contracts; no publisher body is saved here."""
from dataclasses import replace
from pathlib import Path

import pytest

from downloader.news_fulltext import discovery_fingerprint, load_config, seed_discovery
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue


URL = 'https://globalvoices.org/2007/02/13/synthetic-roundup/'
CLOCK = '2007-02-13T12:17:00+0000'
BODY = 'A synthetic roundup links the complete public history of the original event.'
REMOVED = '[Pictochart.com embed removed because it no longer worked]'


def page(body=BODY):
    return ('<html><head><link rel="canonical" href="' + URL + '">'
            '<meta property="article:published_time" content="' + CLOCK + '"></head><body>'
            '<div class="post-header"><h2 class="post-title">Synthetic roundup</h2>'
            '<div id="post-header-credit"><a class="user-link" href="/author/writer/">Writer</a></div></div>'
            '<div class="post-container"><div class="entry"><p>' + body + '</p></div>'
            '<div class="postfooter-credits post-credit-container"><span class="contributor-name">'
            '<a class="user-link" href="/author/writer/">Writer</a></span>'
            '<a rel="license" href="https://creativecommons.org/licenses/by/3.0/">CC</a></div>'
            '</div></body></html>').encode()


def policy():
    return load_config(Path('configs/news_fulltext.json'))[1].sources['globalvoices']


def test_current_complete_micro_post_preserves_clock_byline_license_and_body():
    source = policy()
    result = extract_article(page(), source, page_url=URL)
    assert result['body'] == BODY
    assert result['authors_json'] == '["Writer"]'
    assert result['published_at_raw'] == CLOCK
    assert result['per_article_license_verified']
    assert result['body_completeness'].startswith('verified_publisher_short_body;')
    assert not source.allow_short_configured_body


@pytest.mark.parametrize('body', ['A complete native update.', 'Confirmed.'])
def test_verified_complete_micro_post_does_not_need_eight_english_words(body):
    result = extract_article(page(body), policy(), page_url=URL)
    assert result['body'] == body
    assert result['authors_json'] == '["Writer"]'
    assert result['published_at_raw'] == CLOCK
    assert result['per_article_license_verified']
    assert result['body_completeness'].startswith('verified_publisher_short_body;')


@pytest.mark.parametrize('markup', ['<p hidden>', '<p style="display:none">', '<p><span hidden>'])
def test_verified_short_body_identity_never_exempts_hidden_native_text(markup):
    raw = page('A complete source answer.').replace(b'<p>', markup.encode())
    if '<span' in markup:
        raw = raw.replace(b'</p>', b'</span></p>')
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(raw, policy(), page_url=URL)


@pytest.mark.parametrize('change', [
    'no_request_url', 'wrong_canonical', 'multiple_canonical', 'multiple_body',
    'multiple_title', 'missing_header_credit', 'different_footer_author',
    'foreign_author_profile', 'unknown_clock', 'naive_clock', 'multiple_clocks',
    'missing_license', 'site_footer_license_only', 'media_body', 'other_provider',
])
def test_no_short_exception_without_bound_current_article_evidence(change):
    raw, source, url = page(), policy(), URL
    if change == 'no_request_url':
        url = None
    elif change == 'wrong_canonical':
        raw = raw.replace(URL.encode(), b'https://globalvoices.org/2007/02/13/another/')
    elif change == 'multiple_canonical':
        raw = raw.replace(b'</head>', ('<link rel="canonical" href="' + URL + '"></head>').encode())
    elif change == 'multiple_body':
        raw = raw.replace(b'<div class="entry">', b'<div class="entry"><p>Related short text</p></div><div class="entry">')
    elif change == 'multiple_title':
        raw = raw.replace(b'<h2 class="post-title">', b'<h2 class="post-title">Another title</h2><h2 class="post-title">')
    elif change == 'missing_header_credit':
        raw = raw.replace(b'id="post-header-credit"', b'id="other-credit"')
    elif change == 'different_footer_author':
        raw = raw.replace(b'<span class="contributor-name"><a class="user-link" href="/author/writer/">Writer',
                          b'<span class="contributor-name"><a class="user-link" href="/author/other/">Other')
    elif change == 'foreign_author_profile':
        raw = raw.replace(b'/author/writer/', b'https://foreign.test/author/writer/')
    elif change == 'unknown_clock':
        raw = raw.replace(CLOCK.encode(), b'unknown')
    elif change == 'naive_clock':
        raw = raw.replace(CLOCK.encode(), b'2007-02-13T12:17:00')
    elif change == 'multiple_clocks':
        raw = raw.replace(b'</head>', b'<meta property="article:published_time" content="2007-02-14T00:00:00Z"></head>')
    elif change == 'missing_license':
        raw = raw.replace(b'https://creativecommons.org/licenses/by/3.0/', b'https://example.test/license')
    elif change == 'site_footer_license_only':
        license_node = b'<a rel="license" href="https://creativecommons.org/licenses/by/3.0/">CC</a>'
        raw = raw.replace(license_node, b'').replace(b'</body>', b'<footer class="post-credit-container">' + license_node + b'</footer></body>')
    elif change == 'media_body':
        raw = raw.replace(b'<div class="entry">', b'<div class="entry"><img src="diagram">')
    elif change == 'other_provider':
        source = replace(source, provider='other')
    with pytest.raises(NewsError, match='body_not_found|article_license_not_verified'):
        extract_article(raw, source, page_url=url)


def test_exact_removed_embed_is_not_an_accepted_short_article():
    with pytest.raises(NewsError, match='^source_body_removed$'):
        extract_article(page(REMOVED), policy(), page_url=URL)


def test_quoted_removed_embed_notice_does_not_remove_a_real_article():
    result = extract_article(page(BODY * 3 + REMOVED), policy(), page_url=URL)
    assert result['body'].endswith(REMOVED)


def test_short_body_never_exempts_paywall():
    with pytest.raises(NewsError, match='^paywall_or_login$'):
        extract_article(page().replace(b'class="entry"', b'class="entry paywall"'), policy(), page_url=URL)


def test_long_body_does_not_run_small_body_verification(monkeypatch):
    import downloader.news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, 'globalvoices_verified_small_body',
                        lambda *a, **k: pytest.fail('normal body did redundant short-body verification'))
    assert len(extract_article(page(BODY * 3), policy(), page_url=URL)['body']) > 100


IMAGE = '<div class="wp-caption"><img src="https://media.test/photo"><p class="wp-caption-text">Third-party caption must not become the body.</p></div>'
INFOGRAPHIC = '<iframe src="https://magic.piktochart.com/embed/123-synthetic-infographic"></iframe>'
QUIZ = ('<script src="//cdn.playbuzz.com/widget/feed.js"></script>'
        '<div class="pb_feed" data-game="/writer/synthetic-quiz" '
        'data-embed-by="6f3ac085-fc7a-44ca-ace1-6052838f852e"></div>')
PREVIOUS_REPORT = ('<p>More information see GV\'s <a href="https://globalvoicesonline.org/'
                   '2014/05/26/synthetic-previous-report/">previous report</a>. </p>')
TRANSLATION_CREDIT = '<div class="contributors">譯者：Translator<br/>校對：Reviewer</div>'
REVIEW_CREDIT = '<div class="contributors">校對：Reviewer</div>'
STATIC_VIDEO = '<iframe src="https://player.vimeo.com/video/81968940?dnt=1&amp;app_id=122963"></iframe>'
NONTEXT_YOUTUBE = '<iframe src="https://www.youtube.com/embed/BeDb26MeJKc?feature=oembed"></iframe>'


def nontext_page(markup=IMAGE):
    return page('').replace(b'<p></p>', markup.encode())


@pytest.mark.parametrize('markup,outcome', [
    (IMAGE, 'source_body_image_only'),
    (INFOGRAPHIC, 'source_body_embedded_only'),
    (QUIZ, 'source_body_embedded_only'),
])
def test_verified_nontext_source_outcome_is_not_article_acceptance(markup, outcome):
    with pytest.raises(NewsError, match='^' + outcome + '$'):
        extract_article(nontext_page(markup), policy(), page_url=URL)


@pytest.mark.parametrize('context', [PREVIOUS_REPORT, TRANSLATION_CREDIT, REVIEW_CREDIT,
                                   PREVIOUS_REPORT + TRANSLATION_CREDIT])
def test_image_page_navigation_and_translation_credit_do_not_become_or_hide_body(context):
    with pytest.raises(NewsError, match='^source_body_image_only$'):
        extract_article(nontext_page(IMAGE + context), policy(), page_url=URL)


@pytest.mark.parametrize('body', ['A complete source answer.', BODY])
def test_complete_authored_short_text_beside_captioned_static_photo_is_preserved(body):
    result = extract_article(nontext_page('<p>' + body + '</p>' + IMAGE), policy(), page_url=URL)
    assert result['body'] == body
    assert 'Third-party caption' not in result['body']
    assert result['authors_json'] == '["Writer"]'
    assert result['published_at_raw'] == CLOCK
    assert result['per_article_license_verified']
    assert 'embedded_images_not_downloaded_or_OCRed' in result['body_completeness']


@pytest.mark.parametrize('media', [
    '<img src="https://media.test/unknown">',
    IMAGE.replace('class="wp-caption"', 'class="unknown"'),
    IMAGE.replace('<img ', '<img hidden '),
    IMAGE.replace('class="wp-caption"', 'class="wp-caption" hidden'),
    IMAGE + '<iframe src="https://unknown.test/body"></iframe>',
    IMAGE + '<video src="https://unknown.test/body"></video>',
    IMAGE + '<script src="https://unknown.test/body.js"></script>',
])
def test_short_photo_post_never_exempts_unverified_media_or_hidden_content(media):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page('<p>A complete source answer.</p>' + media), policy(), page_url=URL)


def test_complete_native_text_beside_reviewed_static_video_is_preserved_without_media_calls():
    result = extract_article(nontext_page('<p>' + BODY + '</p><p>' + STATIC_VIDEO + '</p>'), policy(), page_url=URL)
    assert result['body'] == BODY
    assert result['authors_json'] == '["Writer"]'
    assert result['per_article_license_verified']
    assert 'external_static_video_not_requested_or_transcribed' in result['body_completeness']


def test_reviewed_video_without_source_authored_text_is_not_a_body():
    with pytest.raises(NewsError, match='^source_body_embedded_only$'):
        extract_article(nontext_page(STATIC_VIDEO), policy(), page_url=URL)


@pytest.mark.parametrize('media', [NONTEXT_YOUTUBE, IMAGE + '<p><span><br>' + NONTEXT_YOUTUBE + '</span></p>'])
def test_native_youtube_only_or_captioned_photo_with_video_is_not_text_success(media):
    with pytest.raises(NewsError, match='^source_body_embedded_only$'):
        extract_article(nontext_page(media), policy(), page_url=URL)


@pytest.mark.parametrize('media', [
    NONTEXT_YOUTUBE.replace('www.youtube.com', 'www.youtube.com.evil.test'),
    NONTEXT_YOUTUBE.replace('https://', 'http://'),
    NONTEXT_YOUTUBE.replace('feature=oembed', 'feature=unknown'),
    NONTEXT_YOUTUBE.replace('feature=oembed', 'feature=oembed#fragment'),
    NONTEXT_YOUTUBE.replace('/embed/BeDb26MeJKc', '/embed/unknown'),
    NONTEXT_YOUTUBE.replace('<iframe ', '<iframe hidden '),
    NONTEXT_YOUTUBE.replace('<iframe ', '<iframe srcdoc="unknown" '),
    NONTEXT_YOUTUBE + NONTEXT_YOUTUBE,
    NONTEXT_YOUTUBE + '<script src="https://unknown.test/body.js"></script>',
    NONTEXT_YOUTUBE + '<img src="https://unknown.test/unmarked.jpg">',
    NONTEXT_YOUTUBE + IMAGE.replace('<img ', '<img hidden '),
    NONTEXT_YOUTUBE + IMAGE.replace('class="wp-caption"', 'class="unknown"'),
])
def test_unknown_or_hidden_mixed_video_keeps_unresolved_body_evidence(media):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page(media), policy(), page_url=URL)


def test_nontext_youtube_declaration_does_not_expand_short_body_acceptance():
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page('<p>' + BODY + '</p>' + NONTEXT_YOUTUBE), policy(), page_url=URL)


@pytest.mark.parametrize('media', [
    STATIC_VIDEO.replace('player.vimeo.com', 'player.vimeo.com.evil.test'),
    STATIC_VIDEO.replace('/video/81968940', '/video/unknown'),
    STATIC_VIDEO.replace('dnt=1&amp;app_id=122963', 'private=unknown'),
    STATIC_VIDEO.replace('dnt=1&amp;app_id=122963', 'dnt=1&amp;app_id=122963#unknown'),
    STATIC_VIDEO.replace('<iframe ', '<iframe hidden '),
    STATIC_VIDEO.replace('<iframe ', '<iframe srcdoc="unknown" '),
    STATIC_VIDEO + STATIC_VIDEO,
    STATIC_VIDEO + IMAGE,
    STATIC_VIDEO + '<script src="https://unknown.test/another.js"></script>',
])
def test_unknown_video_markup_keeps_missing_body_evidence(media):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page('<p>' + BODY + '</p>' + media), policy(), page_url=URL)


@pytest.mark.parametrize('change', [
    'unknown_link_host', 'current_link', 'unknown_link_path', 'extra_text',
    'extra_link', 'hidden_reference', 'hidden_image', 'extra_script',
    'unknown_credit_label', 'credit_with_narrative', 'credit_with_link',
    'hidden_credit', 'unmarked_credit',
])
def test_nontext_context_classification_never_swallows_unverified_or_hidden_text(change):
    context = PREVIOUS_REPORT + TRANSLATION_CREDIT
    image = IMAGE
    if change == 'unknown_link_host':
        context = context.replace('globalvoicesonline.org', 'unknown.test')
    elif change == 'current_link':
        context = context.replace('https://globalvoicesonline.org/2014/05/26/synthetic-previous-report/', URL)
    elif change == 'unknown_link_path':
        context = context.replace('/2014/05/26/synthetic-previous-report/', '/search/')
    elif change == 'extra_text':
        context = context.replace('. </p>', '. Additional source text.</p>')
    elif change == 'extra_link':
        context = context.replace('. </p>', '. <a href="https://globalvoices.org/2014/05/25/another/">another</a></p>')
    elif change == 'hidden_reference':
        context = context.replace('<p>', '<p hidden>')
    elif change == 'hidden_image':
        image = image.replace('<img ', '<img hidden ')
    elif change == 'extra_script':
        context += '<script src="https://unknown.test/body.js"></script>'
    elif change == 'unknown_credit_label':
        context = context.replace('校對：Reviewer', '正文：News')
    elif change == 'credit_with_narrative':
        context = context.replace('</div>', '<p>Real text.</p></div>')
    elif change == 'credit_with_link':
        context = context.replace('Reviewer', '<a href="https://unknown.test/">Reviewer</a>')
    elif change == 'hidden_credit':
        context = context.replace('class="contributors"', 'class="contributors" hidden')
    elif change == 'unmarked_credit':
        context = context.replace('class="contributors"', 'class="unmarked"')
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page(image + context), policy(), page_url=URL)


def test_nontext_context_exception_never_exempts_paywall_or_no_media():
    with pytest.raises(NewsError, match='^paywall_or_login$'):
        extract_article(nontext_page(IMAGE + PREVIOUS_REPORT).replace(
            b'class="entry"', b'class="entry paywall"'), policy(), page_url=URL)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page(TRANSLATION_CREDIT), policy(), page_url=URL)


@pytest.mark.parametrize('markup,outcome', [(IMAGE, 'source_body_image_only'),
                                          (INFOGRAPHIC, 'source_body_embedded_only'),
                                          (QUIZ, 'source_body_embedded_only')])
def test_native_aria_hidden_empty_avatar_is_not_a_missing_or_hidden_named_author(markup, outcome):
    raw = nontext_page(markup).replace(b'<div id="post-header-credit">',
        b'<div id="post-header-credit"><a class="user-link" href="/author/writer/" aria-hidden="true"><img src="avatar"></a>')
    with pytest.raises(NewsError, match='^' + outcome + '$'):
        extract_article(raw, policy(), page_url=URL)


@pytest.mark.parametrize('change', [
    'no_request_url', 'wrong_canonical', 'multiple_canonical', 'multiple_body',
    'missing_header_credit', 'different_footer_author', 'unknown_clock',
    'naive_clock', 'site_footer_license_only', 'other_provider',
    'hidden_entry', 'hidden_title', 'hidden_author', 'hidden_license',
])
@pytest.mark.parametrize('markup', [IMAGE, INFOGRAPHIC, QUIZ])
def test_nontext_outcome_requires_current_article_identity_clock_authors_and_license(change, markup):
    raw, source, url = nontext_page(markup), policy(), URL
    if change == 'no_request_url':
        url = None
    elif change == 'wrong_canonical':
        raw = raw.replace(URL.encode(), b'https://globalvoices.org/2007/02/13/another/')
    elif change == 'multiple_canonical':
        raw = raw.replace(b'</head>', ('<link rel="canonical" href="' + URL + '"></head>').encode())
    elif change == 'multiple_body':
        raw = raw.replace(b'<div class="entry">', b'<div class="entry"></div><div class="entry">')
    elif change == 'missing_header_credit':
        raw = raw.replace(b'id="post-header-credit"', b'id="other-credit"')
    elif change == 'different_footer_author':
        raw = raw.replace(b'<span class="contributor-name"><a class="user-link" href="/author/writer/">Writer',
                          b'<span class="contributor-name"><a class="user-link" href="/author/other/">Other')
    elif change == 'unknown_clock':
        raw = raw.replace(CLOCK.encode(), b'unknown')
    elif change == 'naive_clock':
        raw = raw.replace(CLOCK.encode(), b'2007-02-13T12:17:00')
    elif change == 'site_footer_license_only':
        license_node = b'<a rel="license" href="https://creativecommons.org/licenses/by/3.0/">CC</a>'
        raw = raw.replace(license_node, b'').replace(b'</body>', b'<footer class="post-credit-container">' + license_node + b'</footer></body>')
    elif change == 'other_provider':
        source = replace(source, provider='other')
    elif change == 'hidden_entry':
        raw = raw.replace(b'class="entry"', b'class="entry" hidden')
    elif change == 'hidden_title':
        raw = raw.replace(b'class="post-title"', b'class="post-title" style="display:none"')
    elif change == 'hidden_author':
        raw = raw.replace(b'class="user-link"', b'class="user-link" aria-hidden="true"')
    elif change == 'hidden_license':
        raw = raw.replace(b'rel="license"', b'rel="license" hidden')
    with pytest.raises(NewsError, match='body_not_found|article_license_not_verified'):
        extract_article(raw, source, page_url=url)


@pytest.mark.parametrize('markup', [
    '', '<script src="https://unknown.test/load.js"></script>',
    '<iframe src="https://unknown.test/embed/123-title"></iframe>',
    '<iframe src="https://magic.piktochart.com.evil.test/embed/123-title"></iframe>',
    '<iframe src="https://[malformed"></iframe>',
    '<iframe src="https://magic.piktochart.com/embed/123-title?token=unknown"></iframe>',
    INFOGRAPHIC + '<iframe src="https://unknown.test/another"></iframe>',
    QUIZ.replace('/writer/synthetic-quiz', ''),
    QUIZ.replace('6f3ac085-fc7a-44ca-ace1-6052838f852e', 'unknown'),
    QUIZ.replace('//cdn.playbuzz.com/widget/feed.js', '//cdn.playbuzz.com.evil.test/widget/feed.js'),
    IMAGE + '<script src="https://unknown.test/load-body.js"></script>',
])
def test_unknown_empty_or_loading_representation_remains_a_parser_gap(markup):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(nontext_page(markup), policy(), page_url=URL)


@pytest.mark.parametrize('markup', [IMAGE, INFOGRAPHIC, QUIZ])
def test_real_native_text_beside_media_is_preserved_and_paywall_never_exempted(markup):
    raw = nontext_page('<p>' + BODY * 3 + '</p>' + markup)
    article = extract_article(raw, policy(), page_url=URL)
    assert article['body'] == BODY * 3
    with pytest.raises(NewsError, match='^paywall_or_login$'):
        extract_article(raw.replace(b'class="entry"', b'class="entry paywall"'), policy(), page_url=URL)


def test_html_fingerprint_change_preserves_healthy_feed_due_and_capture_scope(tmp_path):
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = {**config, 'providers': [p for p in config['providers'] if p['id'] == 'globalvoices']}
    registry = PolicyRegistry(config, base=tmp_path)
    source = registry.sources['globalvoices']
    # Exactly the previous payload before the new HTML-only key.
    previous = replace(source, data_fingerprint=source.fulltext_feed_data_fingerprint)
    registry.sources['globalvoices'] = previous
    queue = NewsQueue(tmp_path, registry)
    try:
        seed_discovery(queue)
        with queue.db:
            queue.db.execute("UPDATE discovery SET due=12345678900,parser_fingerprint=? WHERE kind='rss_fulltext'",
                             (discovery_fingerprint(previous),))
        registry.sources['globalvoices'] = source
        queue.reconcile_policy()
        seed_discovery(queue)
        assert source.data_fingerprint != previous.data_fingerprint
        assert discovery_fingerprint(source) == discovery_fingerprint(previous)
        assert all(r[0] == 12345678900 for r in queue.db.execute("SELECT due FROM discovery WHERE kind='rss_fulltext'"))
    finally:
        queue.close()
