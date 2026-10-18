"""Native house image/caption/reporter prefixes, plus bounded failure witnesses."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_metrics import ETTODAY_ATTRIBUTION_FAILURE_STAGES
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_newsroom_v26 as desk


def registry(version=27):
    return PolicyRegistry(house.config(version), base=Path.cwd())


def page(parser='lxml'):
    soup = desk.page(parser, 'combined')
    prefix = soup.select_one('#desk-prelude')
    prefix.contents[-1].replace_with('記者研究記者／台北報導')
    native.update_schema(soup, creator=['1940-帳號不是原文記者', '1940'])
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=27, url=house.URL):
    return extract_article(str(soup).encode(), replace(registry(version).sources['ettoday'], html_parser=parser),
        page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_whole_native_prose_values_clocks_captions_reporter_and_source_dom_preserved(parser):
    soup = page(parser)
    before = str(soup)
    result = extract(soup, parser)
    old = extract(house.page(parser), parser, version=26)
    for key in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[key] == old[key], key
    assert json.loads(result['authors_json']) == ['研究記者']
    detail, = json.loads(result['author_attribution_detail_json'])
    assert detail['role'] == 'reporter' and detail['native_credit'] == '記者研究記者／台北報導'
    assert '帳號不是原文記者' not in result['authors_json']
    notes = json.loads(result['verified_article_notices_json'])
    prefix, = [n for n in notes if n['kind'] == 'native_house_image_caption_reporter_prefix']
    assert prefix['caption_and_reporter_only_no_prose_in_removed_paragraph']
    assert prefix['following_prose_preserved_in_native_paragraphs'] and not prefix['source_values_rewritten']
    assert not any(n['kind'] == 'native_publisher_newsroom_signature' for n in notes)
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert str(soup) == before
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup, parser, version=26)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_image', 'hidden_caption', 'hidden_credit', 'nested_caption',
    'extra_caption_prose', 'extra_prose', 'before_caption', 'no_image', 'duplicate_image',
    'no_following_prose', 'tagged_credit', 'embedded_credit', 'caption_reporter', 'missing_credit',
    'duplicate_credit', 'foreign_credit', 'duplicate_reporter', 'source_override', 'schema_author',
    'no_schema', 'schema_url', 'schema_clock', 'paid', 'canonical', 'rights', 'extra_script'])
def test_prefix_never_slices_prose_or_waives_identity_attribution_access_or_scope(parser, change):
    soup = page(parser)
    prefix = soup.select_one('#desk-prelude')
    caption = prefix.find('strong')
    if change == 'hidden_image': prefix.find('img')['hidden'] = ''
    elif change == 'hidden_caption': caption['hidden'] = ''
    elif change == 'hidden_credit': prefix['style'] = 'display:none'
    elif change == 'nested_caption': caption.append(soup.new_tag('span'))
    elif change == 'extra_caption_prose': caption.append('不能刪除的額外正文。')
    elif change == 'extra_prose': prefix.insert(2, '不能刪除的實際正文。')
    elif change == 'before_caption': prefix.insert(0, '署名前的實際正文。')
    elif change == 'no_image': prefix.find('img').decompose()
    elif change == 'duplicate_image': prefix.insert(1, deepcopy(prefix.find('img')))
    elif change == 'no_following_prose':
        for node in list(prefix.find_next_siblings()): node.decompose()
    elif change == 'tagged_credit':
        tagged = soup.new_tag('strong')
        tagged.string = str(prefix.contents[-1])
        prefix.contents[-1].replace_with(tagged)
    elif change == 'embedded_credit': prefix.contents[-1].replace_with('記者研究記者／台北報導 不可併入署名的正文。')
    elif change == 'caption_reporter': caption.string = '▲圖說 記者其他記者／台北報導'
    elif change == 'missing_credit': prefix.contents[-1].extract()
    elif change == 'duplicate_credit':
        duplicate = soup.new_tag('p')
        duplicate.string = '記者研究記者／台北報導'
        prefix.insert_after(duplicate)
    elif change == 'foreign_credit': prefix.contents[-1].replace_with('中央社／台北報導')
    elif change == 'duplicate_reporter': prefix.contents[-1].replace_with('記者研究記者、研究記者／台北報導')
    elif change == 'source_override':
        override = soup.new_tag('p')
        override.string = '來源：中央社'
        soup.select_one('.story').append(override)
    elif change == 'schema_author': native.update_schema(soup, author={'@type': 'Person', 'name': '不同記者'})
    elif change == 'no_schema': soup.select_one('#report').decompose()
    elif change == 'schema_url': native.update_schema(soup, url=house.URL + '0')
    elif change == 'schema_clock': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '0'
    elif change == 'rights': soup.select_one('p.note').decompose()
    elif change == 'extra_script': soup.select_one('.story').append(soup.new_tag('script', src='/unknown.js'))
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('style', ['standalone', 'combined', 'named'])
def test_existing_verified_output_unchanged(parser, style):
    soup = house.page(parser) if style == 'named' else desk.page(parser, style)
    assert extract(soup, parser) == extract(soup, parser, version=26)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_static_failure_stage_is_no_body_or_source_derived_metric(parser):
    soup = house.page(parser)
    soup.select_one('#credit').string = '沒有證據的署名。'
    with pytest.raises(NewsError, match='article_attribution_not_verified') as error:
        extract(soup, parser)
    assert error.value.ettoday_attribution_failure_stage == 'unrecognized_precredit_paragraph'
    assert error.value.ettoday_attribution_failure_stage in ETTODAY_ATTRIBUTION_FAILURE_STAGES
    assert not hasattr(error.value, 'failure_response')


@pytest.mark.parametrize('stage', sorted(ETTODAY_ATTRIBUTION_FAILURE_STAGES))
def test_queue_records_static_failure_without_changing_error_deadline_or_diagnostic_budget(tmp_path, stage):
    with closing(NewsQueue(tmp_path / 'store', registry())) as queue:
        with queue.db: queue.add_articles([{'url': house.URL}])
        row = queue.claim_url(house.URL, 300, refresh=True)
        error = NewsError('article_attribution_not_verified')
        error.ettoday_attribution_failure_stage = stage
        queue.failed(row, error, 6)
        result = queue.db.execute('SELECT state,error_code,body_sha256,due FROM articles WHERE url=?', (house.URL,)).fetchone()
        assert tuple(result) == ('article_attribution_not_verified', 'article_attribution_not_verified', None, 0)
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_%'").fetchone()
        assert not list(queue.root.glob('diagnostics/**/*')) and not list(queue.root.glob('identity_metadata/**/*'))
        snapshot = queue.metrics.snapshot()
        assert snapshot['ettoday_attribution_failure_observation_contract_version'] == 1
        assert snapshot['providers']['ettoday']['counts']['native_attribution_failure_stage_' + stage] == 1


@pytest.mark.parametrize('stage', ['source-controlled', 'https://private.example/story', '', None, [], 1])
def test_unknown_or_source_controlled_failure_stage_cannot_create_metric(tmp_path, stage):
    with closing(NewsQueue(tmp_path / 'store', registry())) as queue:
        with queue.db: queue.add_articles([{'url': house.URL}])
        row = queue.claim_url(house.URL, 300, refresh=True)
        error = NewsError('article_attribution_not_verified')
        error.ettoday_attribution_failure_stage = stage
        queue.failed(row, error, 6)
        assert not any(k.startswith('native_attribution_failure_stage_')
            for k in queue.metrics.snapshot()['providers']['ettoday']['counts'])


def test_parser_ancestry_does_not_expand_authorization_or_reset_http_capacity():
    old, new = registry(26).sources['ettoday'], registry().sources['ettoday']
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.data_fingerprint != old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
