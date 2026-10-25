"""Native card values and written/media boundaries; no source payload fixtures."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from downloader import news_fulltext_knews as native
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_knews_credits import credits_entry
from test_news_knews_editor import editor_page
from test_news_knews_known_urls import URL, authored
from test_news_knews_native import TEXT, policy


def blocks_entry():
    value = credits_entry()
    value.update(embedded_article_layout=native.KNEWS_WRITTEN_BLOCKS_LAYOUT,
        adapter_verification=native.KNEWS_V6_VERIFICATION)
    return value


def sparse_entry():
    value = blocks_entry()
    value.update(embedded_article_layout=native.KNEWS_SPARSE_NATIVE_LAYOUT,
        adapter_verification=native.KNEWS_V7_VERIFICATION)
    return value


def extract(soup, *, version=6):
    return extract_article(str(soup).encode(), policy(sparse_entry() if version == 7 else blocks_entry()), page_url=URL)


def video_page():
    soup = authored()
    mount = soup.new_tag('div', attrs={'class': 'video-container'})
    frame = soup.new_tag('iframe', attrs={'id': 'article-video-youtube',
        'src': 'https://www.youtube.com/embed/abc123DEF_0?autoplay=1&mute=1&playsinline=1&enablejsapi=1',
        'title': soup.select_one('h1').get_text(' ', strip=True), 'width': '560', 'height': '315',
        'allow': 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share',
        'allowfullscreen': '', 'referrerpolicy': 'strict-origin-when-cross-origin', 'frameborder': '0'})
    mount.append(frame)
    soup.select_one('h1').insert_before(mount)
    return soup


def cards_page():
    soup = authored()
    root = soup.new_tag('div', attrs={'class': 'my-3'})
    wrapper = soup.new_tag('div', attrs={'id': 'norss', 'style': 'width: 100%; box-sizing: border-box;'})
    for heading, value in [('合成第1筆', '-5'), ('合成第2筆', '1000 股')]:
        card = soup.new_tag('div', style='border: 1px solid #ddd;')
        header = soup.new_tag('div', style='font-weight: 500;')
        header.string = heading
        card.append(header)
        row = soup.new_tag('div', style='display: flex;')
        for text in ('原始數值', value):
            cell = soup.new_tag('span', style='color: #212529;')
            cell.string = text
            row.append(cell)
        card.append(row)
        wrapper.append(card)
    root.append(wrapper)
    soup.select_one('.article-content-inner > p').insert_after(root)
    return soup


@pytest.mark.parametrize('separator', ['／', '╱'])
def test_finance_desk_separators_keep_original_role_and_literal_signature(separator):
    soup = authored('合成編輯')
    signature = '【財經中心' + separator + '台北報導】'
    soup.select_one('p.post-intro').string = signature + '合成導言，原值-5、1000股不變。'
    result = extract(soup)
    role = json.loads(result['author_attribution_detail_json'])
    assert role['printed_byline_raw'] == signature and signature in result['body']
    assert role['printed_credit_role'] == 'editorial_desk' and role['printed_editorial_desk_name'] == '財經中心'
    assert role['printed_reporter_name'] is None and not role['different_names_are_verified_aliases']
    assert not role['human_reporter_identity_verified'] and not role['original_ownership_verified']
    if separator == '／':
        with pytest.raises(NewsError, match='article_attribution_not_verified'):
            extract_article(str(soup).encode(), policy(credits_entry()), page_url=URL)


def test_nontext_video_mount_keeps_body_and_does_not_become_a_media_transcript():
    before, after = extract(authored()), extract(video_page())
    for field in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'authors_json'):
        assert before[field] == after[field]
    notices = json.loads(after['verified_article_notices_json'])
    assert notices['native_nontext_video_mounts_verified'] == 1
    assert not notices['video_requested_or_transcribed'] and not notices['media_or_scripts_requested_or_executed']
    assert 'youtube' not in after['body'] and TEXT in after['body']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(video_page()).encode(), policy(credits_entry()), page_url=URL)


@pytest.mark.parametrize('version', [6, 7])
@pytest.mark.parametrize('case', ['text', 'extra_paragraph', 'hidden', 'event', 'srcdoc', 'wrong_title',
    'wrong_host', 'wrong_id', 'malformed_url', 'duplicate_parameter', 'extra_parameter', 'missing_parameter'])
def test_video_mount_cannot_hide_written_content_or_unknown_dynamic_nodes(case, version):
    soup = video_page(); mount = soup.select_one('.video-container'); frame = mount.iframe
    if case == 'text':
        frame.string = '不能遺失的影片文字。'
    elif case == 'extra_paragraph':
        node = soup.new_tag('p'); node.string = '不能遺失的段落。'; mount.append(node)
    elif case in {'hidden', 'event', 'srcdoc', 'wrong_title'}:
        key, value = {'hidden': ('style', 'display:none'), 'event': ('onload', 'unknown()'),
            'srcdoc': ('srcdoc', '<p>文字</p>'), 'wrong_title': ('title', '另一篇文章')}[case]
        frame[key] = value
    else:
        frame['src'] = {'wrong_host': frame['src'].replace('www.youtube.com', 'unreviewed.example'),
            'wrong_id': frame['src'].replace('abc123DEF_0', 'short'),
            'malformed_url': 'https://[bad/', 'duplicate_parameter': frame['src'] + '&mute=1',
            'extra_parameter': frame['src'] + '&unknown=1',
            'missing_parameter': frame['src'].replace('&mute=1', '')}[case]
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, version=version)


def test_card_headers_labels_values_and_order_are_kept_without_numeric_conversion():
    result = extract(cards_page())
    body = result['body']
    block = '合成第1筆\n原始數值\n-5\n合成第2筆\n原始數值\n1000 股'
    assert block in body and body.count(block) == 1
    assert body.index(TEXT) < body.index(block) < body.index('合成結尾。')
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_written_cards'] == 2 and notices['native_written_card_labels_and_values_retained']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(cards_page()).encode(), policy(credits_entry()), page_url=URL)


@pytest.mark.parametrize('version', [6, 7])
@pytest.mark.parametrize('case', ['hidden', 'event', 'script', 'extra_text', 'iframe', 'missing_value',
    'extra_value', 'nested_card', 'loader_class', 'empty_header', 'wrong_wrapper', 'extra_root_attribute'])
def test_unknown_or_partial_cards_are_rejected_instead_of_dropping_their_text(case, version):
    soup = cards_page(); root = soup.select_one('.my-3'); card = root.div.div; row = card.select_one('div + div')
    if case == 'hidden':
        row.span['style'] = 'visibility:hidden'
    elif case == 'event':
        row.span['onclick'] = 'unknown()'
    elif case in {'script', 'iframe', 'nested_card'}:
        row.span.append(soup.new_tag({'nested_card': 'div'}.get(case, case)))
    elif case == 'extra_text':
        row.append('不能吞掉的文字。')
    elif case == 'missing_value':
        row.find_all('span')[-1].decompose()
    elif case == 'extra_value':
        row.append(deepcopy(row.span))
    elif case == 'loader_class':
        card['class'] = ['loader']
    elif case == 'empty_header':
        card.div.clear()
    elif case == 'wrong_wrapper':
        root.div['id'] = 'unknown'
    else:
        root['data-loader'] = 'unknown'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, version=version)


def test_old_v5_values_and_credit_clock_notices_have_exact_parity():
    soup = editor_page()
    before = extract_article(str(soup).encode(), policy(credits_entry()), page_url=URL)
    after = extract(soup)
    for field in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert before[field] == after[field]
    for field in ('author_attribution_detail_json', 'verified_article_notices_json'):
        prior, current = json.loads(before[field]), json.loads(after[field])
        prior['contract_version'] = 6
        assert prior == current


@pytest.mark.parametrize('old_entry,new_entry,binding_name', [
    (credits_entry, blocks_entry, 'KNEWS_CREDITS_HTTP_BINDING'),
    (blocks_entry, sparse_entry, 'KNEWS_WRITTEN_BLOCKS_HTTP_BINDING')])
def test_exact_predecessor_keeps_http_owner_while_changed_rights_or_pacing_do_not(monkeypatch, old_entry, new_entry, binding_name):
    old = policy(old_entry())
    monkeypatch.setattr(native, binding_name,
        (old.data_fingerprint, old.authorization_fingerprint, '1' * 64, '2' * 64))
    current = policy(new_entry())
    assert current.data_fingerprint != old.data_fingerprint
    assert current.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert current.http_capacity_source_fingerprint == '1' * 64
    for field in ('authorization', 'interval_seconds'):
        entry = new_entry()
        if field == 'authorization': entry[field]['scope'] += ' changed purpose'
        else: entry[field] = 11
        assert not policy(entry).http_capacity_source_fingerprint


@pytest.mark.parametrize('old_entry,new_entry', [(credits_entry, blocks_entry), (blocks_entry, sparse_entry)])
def test_upgrade_keeps_old_attempts_deadlines_bodies_and_failure_states(tmp_path, old_entry, new_entry):
    config = {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'providers': [old_entry()]}
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=?',
                ('article_attribution_not_verified', 'article_attribution_not_verified', time.time() + 3600))
        before = [dict(r) for r in queue.db.execute('SELECT * FROM articles')]
    with closing(NewsQueue(tmp_path, PolicyRegistry({**config, 'providers': [new_entry()]}, base=tmp_path))) as queue:
        queue.reconcile_policy()
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles')] == before


@pytest.mark.parametrize('version', [6, 7])
def test_installed_candidate_keeps_every_other_provider_and_unchanged_http_scope(monkeypatch, version):
    import test_news_fulltext as shared
    config, registry, repo = shared.load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config); candidate.pop('provider_catalog', None)
    next(p for p in candidate['providers'] if p['id'] == 'knews').update(
        embedded_article_layout=native.KNEWS_SPARSE_NATIVE_LAYOUT if version == 7 else native.KNEWS_WRITTEN_BLOCKS_LAYOUT,
        adapter_verification=native.KNEWS_V7_VERIFICATION if version == 7 else native.KNEWS_V6_VERIFICATION)
    approved = PolicyRegistry(candidate, base=repo)
    for name, old in registry.sources.items():
        current = approved.sources[name]
        for field in ('authorization_fingerprint', 'http_capacity_source_fingerprint', 'http_capacity_authorization_fingerprint'):
            assert getattr(current, field) == getattr(old, field)
        if name != 'knews': assert current.data_fingerprint == old.data_fingerprint
    monkeypatch.setattr(shared, 'load_config', lambda _: (candidate, approved, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def captionless_page():
    soup = authored()
    figure = soup.new_tag('figure'); link = soup.new_tag('a', href='https://static.kmedia.com.tw/example.webp')
    link.append(soup.new_tag('img', src='https://static.kmedia.com.tw/example.webp', alt=''))
    figure.append(link); figure.append(soup.new_tag('figcaption'))
    soup.select_one('.article-content-inner > p').insert_after(figure)
    return soup


def test_verified_captionless_figure_keeps_written_body_and_remains_untranscribed():
    before, after = extract(authored(), version=7), extract(captionless_page(), version=7)
    for field in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert before[field] == after[field]
    notices = json.loads(after['verified_article_notices_json'])
    assert notices['native_captionless_figures_verified'] == 1
    assert not notices['media_or_scripts_requested_or_executed']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(captionless_page())


@pytest.mark.parametrize('case', ['written_child', 'script', 'iframe', 'extra_caption', 'missing_caption',
    'hidden_caption', 'hidden_image', 'event', 'figure_attributes'])
def test_captionless_media_cannot_hide_text_unknown_nodes_or_dynamic_behavior(case):
    soup = captionless_page(); figure = soup.select_one('.article-content-inner > figure')
    if case in {'written_child', 'script', 'iframe'}:
        child = soup.new_tag('p' if case == 'written_child' else case)
        if case == 'written_child': child.string = '必須保留的原始文字。'
        figure.a.append(child)
    elif case == 'extra_caption': figure.append(deepcopy(figure.figcaption))
    elif case == 'missing_caption': figure.figcaption.decompose()
    elif case == 'hidden_caption': figure.figcaption['hidden'] = ''
    elif case == 'hidden_image': figure.img['style'] = 'display:none'
    elif case == 'event': figure.img['onload'] = 'unknown()'
    else: figure['data-loader'] = 'unknown'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, version=7)


def test_empty_media_does_not_turn_the_introduction_into_a_complete_story():
    soup = captionless_page(); body = soup.select_one('.article-content-inner')
    figure = body.figure.extract(); body.clear(); body.append(figure)
    body.parent.select_one('#article-left-toc').decompose()
    with pytest.raises(NewsError, match='body_not_found'):
        extract(soup, version=7)


def test_optional_card_id_keeps_all_original_labels_values_and_order():
    soup = cards_page(); expected = extract(soup, version=7)
    del soup.select_one('.my-3 > div')['id']
    actual = extract(soup, version=7)
    assert actual['body'] == expected['body'] and actual['body_sha256'] == expected['body_sha256']
    assert json.loads(actual['verified_article_notices_json'])['native_written_cards'] == 2
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('signature,role,name', [
    ('【合成作者／綜合報導】', 'unspecified', '合成作者'),
    ('【記者／台北報導】', 'unnamed_reporter', None)])
def test_literal_sparse_signatures_preserve_missing_name_or_role(signature, role, name):
    soup = authored('合成編輯')
    soup.select_one('p.post-intro').string = signature + '合成原文-5、1000股。'
    result = extract(soup, version=7); detail = json.loads(result['author_attribution_detail_json'])
    assert signature in result['body'] and detail['printed_byline_raw'] == signature
    assert detail['printed_reporter_name'] is None and detail['printed_credit_role'] == role
    assert detail['source_header_credit'] == '合成編輯'
    assert not detail['different_names_are_verified_aliases'] and not detail['human_reporter_identity_verified']
    if role == 'unspecified': assert detail['printed_credit_name'] == name
    else: assert not detail['printed_reporter_name_declared']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup)


@pytest.mark.parametrize('signature', ['【記者 ／台北報導】', '【記者／報導】', '【／台北報導】', '【沒有分隔】'])
def test_sparse_signature_does_not_invent_unprinted_name_location_or_separator(signature):
    soup = authored(); soup.select_one('p.post-intro').string = signature + '原文。'
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup, version=7)


def test_sparse_signature_still_requires_agreeing_header_credit_and_excludes_external_reprints():
    soup = authored(); soup.select_one('p.post-intro').string = '【記者／台北報導】原文。'
    soup.select_one('meta[name=author]')['content'] = '另一個名字'
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup, version=7)
    soup = authored(); soup.select_one('p.post-intro').string = '【中央社／台北報導】原文。'
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, version=7)


@pytest.mark.parametrize('make_page', [authored, video_page, cards_page])
def test_old_v6_accepted_values_credits_and_notices_keep_exact_parity(make_page):
    soup = make_page(); before, after = extract(soup), extract(soup, version=7)
    for field in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert before[field] == after[field]
    for field in ('author_attribution_detail_json', 'verified_article_notices_json'):
        prior, current = json.loads(before[field]), json.loads(after[field]); prior['contract_version'] = 7
        assert prior == current
