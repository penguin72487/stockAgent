"""Independent clocks, declared category identity and ordered native TVBS roles."""
from contextlib import closing
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, tvbs_native_v3_parser_repair, utc_now
from downloader.news_fulltext_tvbs import TVBS_V3_PROVED_FAILURES, tvbs_visible_article
import test_news_tvbs as native
import test_news_tvbs_native_v2 as v2


def registry(version=3, parser='lxml'):
    return v2.registry(version, parser=parser)


def extract(soup, *, version=3, parser='lxml', url=native.URLS[0]):
    return extract_article(str(soup).encode(), registry(version, parser).sources['tvbs'], page_url=url)


def people(soup, layout='photo'):
    names = ['研究記者', '另位記者', '原始攝影'] if layout == 'photo' else ['研究記者', '責任編輯名', '實習編輯名']
    v2.update_schema(soup, author=[{'@type': 'Person', 'name': name} for name in names])
    text = (f'記者：{names[0]}、{names[1]}｜攝影：{names[2]}' if layout == 'photo' else
        f'記者：{names[0]}｜責任編輯：{names[1]}｜實習編輯：{names[2]}')
    for label in soup.select('header ul > li:first-child'):
        label.string = text
    return names


def clocks(soup, publication, modification):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    v2.update_schema(soup, datePublished=publication, dateModified=modification)
    for field, value in [('published', publication), ('modified', modification)]:
        soup.select_one(f'meta[property="article:{field}_time"]')['content'] = value
        prefix = '發佈時間' if field == 'published' else '最後更新時間'
        minute = datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(ZoneInfo('Asia/Taipei')).strftime('%Y.%m.%d %H:%M')
        for label in soup.select('header li'):
            if label.get_text().startswith(prefix):
                label.string = prefix + '：' + minute


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('layout,roles', [('photo', ['reporter', 'reporter', 'photographer']),
    ('intern', ['reporter', 'responsible_editor', 'intern_editor'])])
def test_visible_three_person_roles_preserve_order_not_just_schema_names(parser, layout, roles):
    soup = native.page(parser=parser)
    names = people(soup, layout)
    result = extract(soup, parser=parser)
    assert json.loads(result['authors_json']) == names
    details = json.loads(result['author_attribution_detail_json'])['publisher_ordered_people_with_visible_roles']
    assert [d['name'] for d in details] == names
    assert [d['role'] for d in details] == roles
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert not result['historical_point_in_time']
    for old in (1, 2):
        with pytest.raises(NewsError):
            extract(soup, version=old, parser=parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['wrong_person', 'schema_order', 'missing_role', 'unknown_role',
    'duplicate_person', 'organization', 'external_person', 'delimiter_name', 'hidden',
    'responsive_role_conflict', 'four_people', 'only_schema'])
def test_three_person_scope_does_not_accept_unknown_conflicting_or_hidden_credit(parser, change):
    soup = native.page(parser=parser)
    names = people(soup)
    labels = soup.select('header ul > li:first-child')
    if change == 'wrong_person':
        for label in labels:
            label.string = label.get_text().replace(names[2], '別位攝影')
    elif change == 'schema_order':
        v2.update_schema(soup, author=[{'@type': 'Person', 'name': n} for n in names[::-1]])
    elif change == 'missing_role':
        for label in labels:
            label.string = '記者：' + '、'.join(names)
    elif change == 'unknown_role':
        for label in labels:
            label.string = label.get_text().replace('攝影', '來源')
    elif change == 'duplicate_person':
        v2.update_schema(soup, author=[{'@type': 'Person', 'name': n} for n in [names[0], names[0], names[2]]])
    elif change == 'organization':
        v2.update_schema(soup, author=[{'@type': 'Organization' if i == 2 else 'Person', 'name': n} for i, n in enumerate(names)])
    elif change == 'external_person':
        v2.update_schema(soup, author=[{'@type': 'Person', 'name': n} for n in names[:2] + ['中央社']])
    elif change == 'delimiter_name':
        v2.update_schema(soup, author=[{'@type': 'Person', 'name': n} for n in [names[0] + '、另人', *names[1:]]])
    elif change == 'hidden':
        labels[0]['hidden'] = ''
    elif change == 'responsive_role_conflict':
        labels[1].string = f'記者：{names[0]}｜責任編輯：{names[1]}｜實習編輯：{names[2]}'
    elif change == 'four_people':
        v2.update_schema(soup, author=[{'@type': 'Person', 'name': n} for n in names + ['額外記者']])
    else:
        for label in labels:
            label.decompose()
    with pytest.raises(NewsError):
        extract(soup, parser=parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('publication,modification', [
    ('2026-10-14T08:29:00.000Z', '2026-10-14T08:28:59.000Z'),
    ('2026-10-14T08:29:26.000Z', '2026-10-10T09:26:22.000Z')])
def test_independently_witnessed_inverse_clocks_are_not_repaired_or_backdated(parser, publication, modification):
    soup = native.page(parser=parser)
    clocks(soup, publication, modification)
    result = extract(soup, parser=parser)
    assert result['published_at_raw'] == publication and result['modified_at_raw'] == modification
    assert 'independent_source_declared_revision_precedes_release' in result['published_time_basis']
    notice = next(n for n in json.loads(result['verified_article_notices_json']) if n['kind'] == 'source_modification_precedes_publication')
    assert not notice['availability_backdated_to_modification'] and not notice['source_reason_inferred']
    assert not result['historical_point_in_time']
    for old in (1, 2):
        with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
            extract(soup, version=old, parser=parser)


@pytest.mark.parametrize('change', ['schema_meta_conflict', 'visible_conflict', 'missing_publication',
    'missing_modification', 'naive', 'hidden', 'cars', 'supertaste'])
def test_inverse_clock_admission_still_requires_each_own_visible_clock(change):
    host = {'cars': native.TVBS_HOSTS[1], 'supertaste': native.TVBS_HOSTS[2]}.get(change, native.TVBS_HOSTS[0])
    soup = native.page(host)
    if change in {'cars', 'supertaste'}:
        v2.skew(soup, '2026-01-01T08:29:25.000Z')
    else:
        clocks(soup, '2026-10-14T08:29:00.000Z', '2026-10-14T08:28:59.000Z')
        if change == 'schema_meta_conflict':
            soup.select_one('meta[property="article:modified_time"]')['content'] = '2026-10-14T08:28:58.000Z'
        elif change == 'visible_conflict':
            soup.select('header li')[2].string = '最後更新時間：2026.10.14 16:27'
        elif change in {'missing_publication', 'missing_modification'}:
            v2.update_schema(soup, **{'datePublished' if change.endswith('publication') else 'dateModified': None})
        elif change == 'naive':
            v2.update_schema(soup, dateModified='2026-10-14T08:28:59')
        else:
            soup.select('header li')[2]['hidden'] = ''
    with pytest.raises(NewsError):
        extract(soup, url=native.URLS[native.TVBS_HOSTS.index(host)])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_old_category_binds_only_actual_same_story_canonical_without_extra_fetch(parser):
    soup = native.page(parser=parser)
    url = native.URLS[0].replace('/money/', '/life/')
    result = extract(soup, parser=parser, url=url)
    assert native.PROSE in result['body']
    notice = next(n for n in json.loads(result['verified_article_notices_json']) if n['kind'] == 'native_same_story_category_canonical')
    assert notice['response_url'] == url and notice['declared_canonical'] == native.URLS[0]
    assert not notice['alternate_url_requested']
    for old in (1, 2):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract(soup, version=old, url=url)


@pytest.mark.parametrize('change', ['different_id', 'foreign_host', 'canonical_og_conflict',
    'canonical_schema_conflict', 'http_canonical', 'query_canonical', 'duplicate_canonical', 'cars'])
def test_category_alias_never_loosens_other_identity_boundaries(change):
    soup = native.page()
    url = native.URLS[0].replace('/money/', '/life/')
    canonical = soup.select_one('link[rel~="canonical"]')
    if change == 'different_id':
        url = url.replace('4035845', '4035846')
    elif change == 'foreign_host':
        canonical['href'] = canonical['href'].replace('news.tvbs.com.tw', 'foreign.invalid')
    elif change == 'canonical_og_conflict':
        soup.select_one('meta[property="og:url"]')['content'] = url
    elif change == 'canonical_schema_conflict':
        v2.update_schema(soup, mainEntityOfPage={'@type': 'WebPage', '@id': url})
    elif change == 'http_canonical':
        canonical['href'] = canonical['href'].replace('https:', 'http:')
    elif change == 'query_canonical':
        canonical['href'] += '?from=known'
    elif change == 'duplicate_canonical':
        soup.head.append(BeautifulSoup(str(canonical), 'lxml').link)
    else:
        soup = native.page(native.TVBS_HOSTS[1])
        url = native.URLS[1].replace('/car-news/', '/money/')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('span', [False, True])
def test_short_native_prose_keeps_all_text_not_a_summary_threshold(parser, span):
    soup = native.page(parser=parser)
    body = soup.select_one('.article-editor-content')
    body.clear()
    prose = '公司公告今日下午五時舉行重大訊息說明會，會議將由董事長說明，原始數值123456股與-0.75不變。'
    paragraph = soup.new_tag('p')
    if span:
        inline = soup.new_tag('span', attrs={'style': 'font-size:18px'})
        inline.string = prose
        paragraph.append(inline)
    else:
        paragraph.string = prose
    body.append(paragraph)
    result = extract(soup, parser=parser)
    assert result['body'] == prose and len(result['body']) < 100
    assert 'native_short_plain_prose_verified' in result['body_completeness']
    for old in (1, 2):
        with pytest.raises(NewsError, match='body_not_found'):
            extract(soup, version=old, parser=parser)


@pytest.mark.parametrize('content', ['<p>原始報導：-12.5%、123456股與比率0.75</p>',
    '<p>▲獨立圖片說明</p>', '<p><a href="/money/1">只有連結摘要</a></p>',
    '<figure><figcaption>圖片不是正文</figcaption></figure>', '<p>1234</p>',
    '<p>記者：研究記者</p>', '<p hidden>不可見的正文</p>', '<iframe src="https://invalid.example"></iframe>',
    '<p><span hidden>隱藏內容不是公開正文</span></p>',
    '<p><span><a href="/money/1">連結摘要不是正文</a></span></p>',
    '<p><span>▲圖片說明仍非正文</span></p>'])
def test_short_title_caption_byline_media_teaser_or_hidden_content_is_not_prose(content):
    soup = native.page()
    body = soup.select_one('.article-editor-content')
    body.clear()
    body.append(BeautifulSoup(content, 'html.parser'))
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', native.TVBS_HOSTS)
def test_healthy_older_contract_outputs_are_unchanged_in_v3(parser, host):
    soup = native.page(host, parser=parser)
    url = native.URLS[native.TVBS_HOSTS.index(host)]
    assert extract(soup, version=1, parser=parser, url=url) == extract(soup, version=2, parser=parser, url=url) == extract(soup, parser=parser, url=url)


def test_exact_v2_ancestry_and_original_v1_http_refusal_identity():
    old, new = registry(2).sources['tvbs'], registry().sources['tvbs']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == registry(1).sources['tvbs'].data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert tvbs_native_v3_parser_repair(new, old.data_fingerprint)
    assert not tvbs_native_v3_parser_repair(new, registry(1).sources['tvbs'].data_fingerprint)
    assert not tvbs_native_v3_parser_repair(new, 'unknown')


@pytest.mark.parametrize('changed', ['authorization', 'interval_seconds', 'parallelism', 'domains'])
def test_changed_source_or_permission_cannot_inherit_previous_capture(changed):
    entry = native.recipe()
    entry['embedded_article_layout'] = 'tvbs_visible_article_v3'
    if changed == 'authorization':
        entry['authorization'] = {**entry['authorization'], 'reviewed_on': '2026-10-16'}
    elif changed == 'domains':
        entry['domains'] = ['news.tvbs.com.tw']
        with pytest.raises(ValueError):
            PolicyRegistry(v2.config(entry), base=Path.cwd())
        return
    else:
        entry[changed] += 1
    new = PolicyRegistry(v2.config(entry), base=Path.cwd()).sources['tvbs']
    assert not tvbs_native_v3_parser_repair(new, registry(2).sources['tvbs'].data_fingerprint)


def test_exact_missing_heads_only_retry_once_preserving_complete_rows_and_budgets(tmp_path):
    old, new = registry(2), registry()
    cases = list(TVBS_V3_PROVED_FAILURES.items()) + [
        ('https://news.tvbs.com.tw/money/11', 'article_attribution_not_verified'),
        ('https://news.tvbs.com.tw/money/12', 'body_not_found'),
        ('https://news.tvbs.com.tw/money/13', 'invalid_bulk_publication_clock'),
        ('https://news.tvbs.com.tw/money/14', 'http_403'),
        ('https://news.tvbs.com.tw/money/15', 'complete')]
    with closing(NewsQueue(tmp_path, old)) as queue:
        for url, code in cases:
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,last_checked_at_utc=? WHERE url=?',
                (code, code, utc_now(), url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('diagnostic-test-budget','spent')")
        queue.db.commit()
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert after == {url: {**row, 'state': 'retry'} if url in TVBS_V3_PROVED_FAILURES else row for url, row in before.items()}
        marker = 'tvbs_native_remaining_recheck_v3:' + new.sources['tvbs'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert len(proof['rows']) == 6 and not proof['accepted_as_bodies']
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='diagnostic-test-budget'").fetchone()[0] == 'spent'
        first = next(iter(TVBS_V3_PROVED_FAILURES))
        queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (first,))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (first,)).fetchone()) == before[first]
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('boundary', ['saved_body', 'saved_parquet', 'lease_owner', 'lease_until',
    'different_state', 'different_error', 'missing_clock', 'naive_clock', 'future_clock', 'bad_clock', 'unknown_prior'])
def test_proved_url_is_not_permission_to_replay_changed_or_unobserved_heads(tmp_path, boundary):
    old, new = registry(2), registry()
    url, code = next(iter(TVBS_V3_PROVED_FAILURES.items()))
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=9000000000,last_checked_at_utc=? WHERE url=?', (code, code, utc_now(), url))
        updates = {'saved_body': ('body_sha256', 'b' * 64), 'saved_parquet': ('parquet_path', 'immutable.parquet'),
            'lease_owner': ('lease_owner', 'another-owner'), 'lease_until': ('lease_until', 9000000000),
            'different_state': ('state', 'http_403'), 'different_error': ('error_code', 'http_429'),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-14T00:00:00'),
            'future_clock': ('last_checked_at_utc', '2299-01-01T00:00:00+00:00'), 'bad_clock': ('last_checked_at_utc', 'invalid')}
        if boundary == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:tvbs'", ('a' * 64,))
        else:
            column, value = updates[boundary]
            queue.db.execute(f'UPDATE articles SET {column}=? WHERE url=?', (value, url))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('version', [True, False, 0, 5, '3'])
def test_adapter_versions_are_closed_and_typed(version):
    with pytest.raises(ValueError, match='reviewed_tvbs_adapter_version_required'):
        tvbs_visible_article(native.page(), page_url=native.URLS[0], adapter_contract_version=version)
