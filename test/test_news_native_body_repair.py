"""Native complete flash reports and editor table cells are not missing bodies."""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
import test_news_ettoday as et
import test_news_ettoday_clock_v2 as clocks
import test_news_cmnews_scope as cm


SHORT = '本篇完整快訊保留原始-12.5%、123456股、比率0.75與發布時刻，不補摘要或鄰篇。'


def small_page(parser='lxml', host='finance', text=SHORT):
    soup = et.page(host, parser)
    soup.select_one('#prose').string = text
    soup.select_one('.story > h2').decompose()
    soup.select_one('.story > p:last-child').decompose()
    return soup


def registry(provider, base=Path.cwd()):
    if provider == 'ettoday':
        return clocks.registry(6, base)
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    cfg.pop('provider_catalog', None)
    cfg['providers'] = [entry for entry in cfg['providers'] if entry['id'] == 'cmnews']
    return PolicyRegistry(cfg, base=base)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_complete_native_short_report_preserves_all_original_values_clocks_and_credits(parser, host):
    source = replace(registry('ettoday').sources['ettoday'], html_parser=parser)
    raw = str(small_page(parser, host)).encode()
    before = raw
    result = extract_article(raw, source, page_url=et.URLS[host == 'www'])
    assert result['body'] == SHORT and raw == before
    assert result['published_at_raw'] == et.PUB and result['modified_at_raw'] == et.MOD
    assert result['body_completeness'].startswith('verified_publisher_short_body;')
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert len(json.loads(result['verified_image_captions_json'])) == 1
    for version in range(1, 6):
        with pytest.raises(NewsError, match='body_not_found'):
            extract_article(raw, replace(clocks.registry(version).sources['ettoday'], html_parser=parser),
                page_url=et.URLS[host == 'www'])


@pytest.mark.parametrize('text', ['', '12345678', 'A short English label', '---'])
def test_native_short_repair_does_not_promote_empty_caption_numeric_or_nonprose_bodies(text):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(str(small_page(text=text)).encode(), registry('ettoday').sources['ettoday'], page_url=et.URLS[0])


@pytest.mark.parametrize('change', ['link_only', 'caption_only', 'hidden', 'schema_paid', 'foreign_credit', 'wrong_url'])
def test_short_body_does_not_waive_the_native_article_semantics(change):
    soup = small_page()
    prose = soup.select_one('#prose')
    if change == 'link_only':
        prose.clear()
        link = soup.new_tag('a', href='/related')
        link.string = SHORT
        prose.append(link)
    elif change == 'caption_only': prose.decompose()
    elif change == 'hidden': prose['hidden'] = ''
    elif change == 'schema_paid': et.update_schema(soup, isAccessibleForFree=False)
    elif change == 'foreign_credit': soup.select_one('#credit').string = '中央社／綜合報導'
    else: soup.select_one('link[rel=canonical]')['href'] = et.URLS[1]
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), registry('ettoday').sources['ettoday'], page_url=et.URLS[0])


def cell_markup(text, *, classes='ck-heading--viewHeading ck-heading--description', attributes=''):
    return ('<figure class="table"><table><tbody><tr><td><aside class="' + classes + '" ' + attributes + '>'
        '<span style="color:red"><strong>' + text + '</strong></span></aside></td></tr></tbody></table></figure>')


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_editor_table_cell_aside_is_original_data_not_sidebar(parser):
    source = replace(registry('cmnews').sources['cmnews'], html_parser=parser)
    text = cm.TEXT + '；原始股數123456、負值-12.5與比率0.75。'
    raw = cm.page(body=cell_markup(text))
    result = extract_article(raw, source, page_url=cm.URL)
    assert result['body'] == text
    assert result['authors_json'] == json.dumps([cm.AUTHOR], ensure_ascii=False)
    assert result['published_at_raw'] == '2026-10-09T14:20:23+08:00'


@pytest.mark.parametrize('mutation', ['hidden', 'child_hidden', 'script', 'iframe', 'foreign_class', 'outside_table', 'image_figure', 'paid'])
def test_editor_cell_repair_cannot_admit_hidden_unknown_member_or_neighbor_content(mutation):
    markup = cell_markup(cm.TEXT)
    if mutation == 'hidden': markup = cell_markup(cm.TEXT, attributes='hidden')
    elif mutation == 'child_hidden': markup = markup.replace('<strong>', '<strong style="display:none">')
    elif mutation in {'script', 'iframe'}: markup = markup.replace('</strong>', '</strong><' + mutation + '>unreviewed</' + mutation + '>')
    elif mutation == 'foreign_class': markup = cell_markup(cm.TEXT, classes='sidebar')
    elif mutation == 'outside_table': markup = '<aside class="ck-heading--viewHeading ck-heading--description">' + cm.TEXT + '</aside>'
    elif mutation == 'image_figure': markup = markup.replace('class="table"', 'class="image"')
    with pytest.raises(NewsError):
        extract_article(cm.page(body=markup, paid=mutation == 'paid'), registry('cmnews').sources['cmnews'], page_url=cm.URL)


def old_registry(provider, base):
    if provider == 'ettoday':
        return clocks.registry(5, base)
    new = registry(provider, base)
    source = new.sources[provider]
    # Recreate the attested exact prior capture; this is a queue-state fixture,
    # not an alternative current authorization/parser policy.
    new.sources[provider] = replace(source, data_fingerprint=source.http_capacity_source_fingerprint,
        parser_repair_capture_fingerprint=source.http_capacity_source_fingerprint,
        parser_repair_fingerprint='a' * 64, http_capacity_source_fingerprint='')
    return new


@pytest.mark.parametrize('provider', ['ettoday', 'cmnews'])
def test_body_only_repair_preserves_attempts_deadlines_evidence_and_never_reopens_completed_or_denied(tmp_path, provider):
    new = registry(provider, tmp_path)
    old = old_registry(provider, tmp_path)
    url = et.URLS[0] if provider == 'ettoday' else cm.URL
    protected = ['complete', 'http_403', 'http_429', 'paywall_or_login', 'article_identity_not_verified',
        'article_attribution_not_verified', 'invalid_bulk_publication_clock', 'source_not_found']
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for index, state in enumerate(['body_not_found', *protected]):
            target = url + str(index)
            queue.add_articles([{'url': target}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=?,last_checked_at_utc=? WHERE url=?',
                (state, state, 9000000000, utc_now(), target))
            before[target] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (target,)).fetchone())
        queue.db.commit()
    marker = 'verified_native_body_recheck_v1:' + provider + ':' + new.sources[provider].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for target, previous in before.items():
            expected = {**previous, 'state': 'retry'} if previous['state'] == 'body_not_found' else previous
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (target,)).fetchone()) == expected
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert len(proof['rows']) == 1 and proof['rows'][0]['attempts'] == 4
        queue.db.execute("UPDATE articles SET state='body_not_found' WHERE url=?", (url + '0',))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url + '0',)).fetchone()[0] == 'body_not_found'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('provider', ['ettoday', 'cmnews'])
@pytest.mark.parametrize('protection', ['body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'last_checked_at_utc', 'error_code', 'unknown_previous'])
def test_body_repair_never_changes_stored_leased_unobserved_or_unproved_heads(tmp_path, provider, protection):
    old, new = old_registry(provider, tmp_path), registry(provider, tmp_path)
    url = et.URLS[0] if provider == 'ettoday' else cm.URL
    values = {'body_sha256': 'b' * 64, 'parquet_path': 'bodies/prior.parquet', 'lease_owner': 'another-owner',
        'lease_until': 9000000000, 'last_checked_at_utc': None, 'error_code': 'http_403'}
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=4,"
            'due=9000000000,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        if protection == 'unknown_previous':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('f' * 64, 'source_capture_fingerprint:' + provider))
        else:
            queue.db.execute('UPDATE articles SET ' + protection + '=? WHERE url=?', (values[protection], url))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before
