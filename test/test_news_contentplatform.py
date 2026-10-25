"""Native article identity, complete prose and clocks through the shared reader."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_contentplatform import (
    CONTENTPLATFORM_BODY, CONTENTPLATFORM_PATH, CONTENTPLATFORM_TITLE,
    contentplatform_private_failure_permit,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article

URL = 'https://www.contentplatform.info/articles/533649/jaya533649/'
PROFILE = 'https://www.contentplatform.info/articles/author/native-reporter/'
RAW = '原始值-12.5、5434、21家、3.25%、2.12及2020年都應保留。' * 5


def entry():
    return dict(id='contentplatform', domains=['www.contentplatform.info'], enabled=True,
        acquisition_mode='continuous', private_known_url_scope='personal_noncommercial_originals_v1',
        interval_seconds=10, parallelism=1, html_parser='lxml',
        embedded_article_layout='contentplatform_native_article_v1',
        article_path_pattern=CONTENTPLATFORM_PATH, article_selectors=[CONTENTPLATFORM_BODY],
        title_selector=CONTENTPLATFORM_TITLE, require_configured_container=True,
        require_author_attribution=True, exclude_selectors=[], authorization=dict(
            kind='personal_research', evidence_url='https://www.contentplatform.info/copyright/',
            scope='Private noncommercial signed native public reporting; no discovery or export.',
            reviewed_on='2026-10-24', automated_access_review='no_explicit_prohibition_identified'))


def source(candidate=None):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [candidate or entry()]
    return PolicyRegistry(config, base=repo).sources['contentplatform']


def test_registered_source_keeps_shared_installed_scope_contract(monkeypatch):
    import test_news_fulltext as shared
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [row for row in config['providers'] if row['id'] != 'contentplatform'] + [entry()]
    registry = PolicyRegistry(config, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda _: (config, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def page(*, canonical=URL, entity=URL, author='許 家源', profile=PROFILE,
         published='2026-10-14T10:43:17+08:00', modified='2026-10-14T10:43:19+08:00',
         visible='2026-10-14', credit='【許家源記者 / 綜合報導】', extra='', body_attrs=''):
    return f'''<html><head><meta charset="utf-8"><link rel="canonical" href="{canonical}"></head><body>
    <article id="template-id-429527" itemtype="https://schema.org/Article">
      <h1 class="tdb-title-text">完整原文</h1>
      <meta itemprop="headline" content="完整原文">
      <meta itemprop="mainEntityOfPage" itemid="{entity}">
      <span itemprop="publisher"><meta itemprop="name" content="報新聞 Mega News"></span>
      <span itemprop="author" itemtype="https://schema.org/Person"><meta itemprop="name" content="{author}"><meta itemprop="url" content="{profile}"></span>
      <div class="tdb_single_author"><a class="tdb-author-name" href="{profile}">{author}</a></div>
      <meta itemprop="datePublished" content="{published}"><meta itemprop="dateModified" content="{modified}">
      <div class="tdb_single_date"><time datetime="{published}">{visible}</time></div>
      <div class="tdb_single_content"><style>irrelevant-css</style><div class="tdb-block-inner" {body_attrs}>
        <p>{credit}</p><p>{RAW}</p><figure class="wp-caption"><img src="/not-requested.jpg"><figcaption>照片說明</figcaption></figure>
        <p>新聞來源:原公司</p>{extra}
      </div></div>
    </article><aside><time>2099-01-01</time>推薦文不能混入</aside></body></html>'''.encode()


def test_raw_prose_names_clocks_and_template_identity():
    record = extract_article(page(), source(), page_url=URL)
    assert record['body'] == f'【許家源記者 / 綜合報導】\n{RAW}\n新聞來源:原公司'
    assert json.loads(record['authors_json']) == ['許 家源']
    assert record['published_at_raw'] == '2026-10-14T10:43:17+08:00'
    assert record['modified_at_raw'] == '2026-10-14T10:43:19+08:00'
    assert record['historical_point_in_time'] is False
    assert '照片說明' not in record['body'] and '推薦' not in record['body']


@pytest.mark.parametrize('changes,error', [
    ({'canonical': URL.replace('533649', '429527')}, 'article_identity_not_verified'),
    ({'entity': URL.replace('jaya533649', 'another-slug')}, 'article_identity_not_verified'),
    ({'profile': 'https://example.org/articles/author/native/'}, 'article_attribution_not_verified'),
    ({'credit': '中央社記者'}, 'article_rights_or_scope_excluded'),
    ({'credit': '【別人記者 / 綜合報導】'}, 'article_rights_or_scope_excluded'),
    ({'published': '2026-10-14T10:43:17'}, 'invalid_bulk_publication_clock'),
    ({'modified': '2026-10-13T10:43:19+08:00'}, 'invalid_bulk_publication_clock'),
    ({'visible': '2026-10-15'}, 'invalid_bulk_publication_clock'),
    ({'body_attrs': 'hidden'}, 'article_identity_not_verified'),
    ({'extra': '<p style="display:none">隱藏原文</p>'}, 'article_identity_not_verified'),
    ({'extra': '<iframe src="/not-fetched"></iframe>'}, 'article_identity_not_verified'),
    ({'extra': '<!--unreviewed content-->'}, 'article_identity_not_verified'),
    ({'extra': '<div>未驗證外框不可默默刪除</div>'}, 'article_identity_not_verified'),
    ({'extra': '<div class="paywall">付費</div>'}, 'paywall_or_login'),
])
def test_inconsistent_or_unreviewed_article_rejected(changes, error):
    with pytest.raises(NewsError) as caught:
        extract_article(page(**changes), source(), page_url=URL)
    assert caught.value.code == error


def test_complete_table_values_preserved():
    record = extract_article(page(extra='<table><tr><th>單位</th><td>-12.5股</td></tr></table>'), source(), page_url=URL)
    assert '-12.5股' in record['body'] and RAW in record['body']


def v2_entry():
    candidate=entry()
    candidate['embedded_article_layout']='contentplatform_native_article_v2'
    return candidate


def v3_entry():
    candidate = v2_entry()
    candidate['embedded_article_layout'] = 'contentplatform_native_article_v3'
    return candidate


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v3_retained_image_only_heading_preserves_all_words_numbers_and_clocks(parser):
    original = page(extra='<h3>原始章節與-12.5股</h3>')
    raw = original.replace('<p>新聞來源'.encode(),
        b'<h3 data-start="421" data-end="442"><img loading="lazy" class="alignnone size-full wp-image-378186" '
        b'src="/never-fetched.png" alt="" width="786" height="524"></h3><p>'+ '新聞來源'.encode(), 1)
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(raw, source(v2_entry()), page_url=URL)
    policy = replace(source(v3_entry()), html_parser=parser)
    expected = extract_article(original, source(v2_entry()), page_url=URL)
    assert extract_article(raw, policy, page_url=URL) == expected


@pytest.mark.parametrize('media', [
    '<h3>不可刪除的標題<img class="wp-image-42" src="/image.png"></h3>',
    '<h3><img src="/unverified.png"></h3>',
    '<h3><a href="/another"><img class="wp-image-42" src="/image.png"></a></h3>',
    '<h3><img class="wp-image-42" src="/1.png"><img class="wp-image-43" src="/2.png"></h3>',
    '<h3 hidden><img class="wp-image-42" src="/image.png"></h3>',
    '<h2><img class="wp-image-42" src="/image.png"></h2>',
    '<div><h3><img class="wp-image-42" src="/image.png"></h3></div>',
])
def test_v3_rejects_unobserved_wrappers_and_does_not_drop_written_prose(media):
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(page(extra=media), source(v3_entry()), page_url=URL)


def test_v2_observed_image_only_paragraph_preserves_all_written_values():
    original=page()
    image=b'<p data-path-to-node="12"><img class="alignnone size-full wp-image-491974" src="/never-fetched.jpg"></p>'
    marker='<p>新聞來源'.encode()
    raw=original.replace(marker,image+marker,1)
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        extract_article(raw,source(),page_url=URL)
    expected=extract_article(original,source(),page_url=URL)
    assert extract_article(raw,source(v2_entry()),page_url=URL)==expected
    assert extract_article(original,source(v2_entry()),page_url=URL)==expected


@pytest.mark.parametrize('image', [
    '<p>不可丟棄的內文<img class="wp-image-42" src="/image.jpg"></p>',
    '<p><img src="/unknown-image.jpg"></p>',
    '<p><a href="/another"><img class="wp-image-42" src="/image.jpg"></a></p>',
    '<p hidden><img class="wp-image-42" src="/image.jpg"></p>',
])
def test_v2_does_not_silently_drop_unreviewed_media_or_prose(image):
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        extract_article(page(extra=image),source(v2_entry()),page_url=URL)


@pytest.mark.parametrize('old_entry,new_entry', [(entry, v2_entry), (v2_entry, v3_entry)])
def test_media_upgrade_preserves_completed_and_unreviewed_failures_without_changing_due(tmp_path, old_entry, new_entry):
    from contextlib import closing
    from downloader.news_fulltext_queue import NewsQueue,utc_now,native_text_parser_repair
    config,_,repo=load_config(Path('configs/news_fulltext.json'))
    config['providers']=[old_entry()];config.pop('failure_diagnostic_focus',None)
    old=PolicyRegistry(config,base=repo)
    changed=deepcopy(config);changed['providers']=[new_entry()]
    new=PolicyRegistry(changed,base=repo)
    before_source,after_source=old.sources['contentplatform'],new.sources['contentplatform']
    assert native_text_parser_repair(after_source,before_source.data_fingerprint)
    assert before_source.authorization_fingerprint==after_source.authorization_fingerprint
    assert after_source.http_capacity_source_fingerprint==(before_source.http_capacity_source_fingerprint or before_source.data_fingerprint)
    states=['article_identity_not_verified','article_rights_or_scope_excluded','http_403','http_429','complete']
    root=tmp_path/'queue';before={}
    with closing(NewsQueue(root,old)) as queue:
        for n,state in enumerate(states,1):
            url=URL.replace('533649',str(n))
            queue.add_articles([{'url':url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,last_checked_at_utc=? WHERE url=?',
                (state,state,utc_now(),url))
            before[url]=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        queue.db.commit()
    with closing(NewsQueue(root,new)) as queue:
        for url,old_head in before.items():
            current=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
            expected=({**old_head,'state':'retry'} if new_entry is v2_entry
                and old_head['state']=='article_identity_not_verified' else old_head)
            assert current==expected
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE error_code='article_identity_not_verified'")
        queue.db.commit()
    with closing(NewsQueue(root,new)) as queue:
        assert queue.db.execute("SELECT state FROM articles WHERE error_code='article_identity_not_verified'").fetchone()[0]=='article_identity_not_verified'


def test_private_layout_witness_keeps_failure_and_persistent_budget(tmp_path):
    from contextlib import closing
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail
    import pyarrow.parquet as pq

    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config['providers'] = [entry()]
    config.pop('failure_diagnostic_focus', None)
    registry = PolicyRegistry(config, base=repo)
    raw = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'secret'},
        page(extra='<div>未知排版仍不可當成完成</div>'))
    root = tmp_path/'queue'
    with closing(NewsQueue(root, registry)) as queue:
        fail(queue, raw, code='article_identity_not_verified')
        paths = list((root/'failure_responses').rglob('*.parquet'))
        assert len(paths) == 1
        saved, = pq.ParquetFile(paths[0]).read().to_pylist()
        assert saved['raw_response_body'] == raw.body and saved['diagnostic_only']
        assert not saved['accepted_as_article'] and not saved['publish']
        assert 'secret' not in saved['headers_json']
        assert not saved['native_private_permit']['body_geometry_accepted']
        assert paths[0].stat().st_mode & 0o077 == 0
        row = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?',(URL,)).fetchone()
        assert tuple(row) == ('article_identity_not_verified','article_identity_not_verified',None)
        assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
        fail(queue, raw, code='article_identity_not_verified')
    with closing(NewsQueue(root, registry)) as queue:
        fail(queue, raw, code='article_identity_not_verified')
        assert len(list((root/'failure_responses').rglob('*.parquet'))) == 2


@pytest.mark.parametrize('failure,expected,stage,reason', [
    ('scope', 'scope_rejected', 'scope', 'article_rights_or_scope_excluded'),
    ('storage', 'storage_error', 'payload', 'io_error'),
    ('internal', 'internal_error', 'scope', 'unexpected_exception'),
])
def test_private_diagnostic_outcomes_are_durable_distinct_and_never_refill(
        tmp_path, monkeypatch, failure, expected, stage, reason):
    from contextlib import closing
    from downloader import news_fulltext_contentplatform as native
    from downloader import news_fulltext_diagnostics as diagnostics
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail

    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    config['providers'] = [entry()]
    config.pop('failure_diagnostic_focus', None)
    registry = PolicyRegistry(config, base=repo)
    raw = Response(URL, 200, {'content-type': 'text/html'},
        page(credit='中央社記者') if failure == 'scope' else page(extra='<div>未知排版</div>'))
    def broken(*args, **kwargs):
        raise (PermissionError('SECRET storage path') if failure == 'storage'
               else RuntimeError('SECRET exception text'))
    if failure == 'storage':
        monkeypatch.setattr(diagnostics, 'atomic_write_parquet', broken)
    elif failure == 'internal':
        monkeypatch.setattr(native, 'contentplatform_private_failure_permit', broken)
    root = tmp_path/'queue'
    stamp = '2026-10-24T10:00:00+00:00'
    def outcomes(queue):
        return [json.loads(row[0]) for row in queue.db.execute(
            "SELECT value FROM queue_settings WHERE key LIKE 'failure_contentplatform_original_outcome:v1:%' ORDER BY key")]
    with closing(NewsQueue(root, registry)) as queue:
        for _ in range(2):
            fail(queue, raw, code='article_identity_not_verified', received_at_utc=stamp)
        before = outcomes(queue)
        assert [row['budget_attempt'] for row in before] == [1, 2]
        for row in before:
            assert (row['outcome'], row['stage'], row['reason']) == (expected, stage, reason)
            assert not row['accepted_as_article'] and not row['publish']
            assert not row['remote_training_export'] and row['budget_attempt_limit'] == 2
            assert 'SECRET' not in json.dumps(row) and '中央社' not in json.dumps(row)
        counts = queue.metrics.snapshot()['providers']['contentplatform']['counts']
        assert counts.get('failure_contentplatform_original_scope_rejections', 0) == (2 if failure == 'scope' else 0)
        assert counts.get('failure_contentplatform_original_write_errors', 0) == (2 if failure == 'storage' else 0)
        assert counts.get('failure_contentplatform_original_internal_errors', 0) == (2 if failure == 'internal' else 0)
    with closing(NewsQueue(root, registry)) as queue:
        fail(queue, raw, code='article_identity_not_verified', received_at_utc=stamp)
        assert outcomes(queue) == before
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key=?",
            ('failure_diagnostic_attempts:v1:contentplatform:20261024',)).fetchone()[0] == '2'
        row = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()
        assert tuple(row) == ('article_identity_not_verified', 'article_identity_not_verified', None)
        assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
        assert not list((root/'failure_responses').rglob('*.parquet'))


@pytest.mark.parametrize('changes', [
    {'canonical':URL+'?alias=1'}, {'credit':'中央社記者'},
    {'credit':'【另一位記者 / 綜合報導】'}, {'body_attrs':'hidden'},
    {'extra':'<p hidden>隱藏文字</p>'}, {'extra':'<div class="paywall">會員</div>'},
    {'published':'2026-10-14T10:43:17'}, {'extra':'<iframe src="/unknown"></iframe>'},
])
def test_private_witness_does_not_relax_identity_access_or_attribution(changes):
    from downloader.news_fulltext_http import Response
    with pytest.raises(NewsError):
        contentplatform_private_failure_permit(
            Response(URL,200,{'content-type':'text/html'},page(**changes)), source())


def test_failed_native_cli_probe_is_exact_current_scope_and_never_accepts(tmp_path, monkeypatch):
    from contextlib import closing
    from scripts import probe_news_sources as probes
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_failure_diagnostics import fail
    from test_news_fulltext import FakeResponse, http_fixture

    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config['providers'] = [entry()]
    config.pop('provider_catalog', None)
    config.pop('failure_diagnostic_focus', None)
    root = tmp_path/'queue'
    config['root'] = str(root)
    registry = PolicyRegistry(config, base=tmp_path)
    raw = page(extra='<div>尚未驗證的正文結構</div>')
    with closing(NewsQueue(root,registry)) as queue:
        fail(queue,Response(URL,200,{'content-type':'text/html'},raw),code='article_identity_not_verified')
    http, session = http_fixture(config,root,{
        'https://www.contentplatform.info/robots.txt':FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL:FakeResponse(raw,headers={'Content-Type':'text/html'})})
    monkeypatch.setattr(probes,'load_config',lambda _: (config,registry,tmp_path))
    monkeypatch.setattr(probes,'NewsHTTP',lambda *a,**k: http)
    output=tmp_path/'private-review'/'probe.json'
    assert probes.main(['--probe',URL,'--failure-code','article_identity_not_verified',
        '--require-current-missing-body-failure','--retain-reviewed-response','--output',str(output)])==0
    result=json.loads(output.read_bytes());probe,=result['probes']
    assert probe['extraction_error']=='article_identity_not_verified'
    assert probe['native_private_permit']['article_accepted'] is False
    assert Path(probe['private_diagnostic_path']).is_file()
    assert len(session.calls)==2
    with closing(NewsQueue(root,registry)) as queue:
        assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?',(URL,)).fetchone()[0] is None


@pytest.mark.parametrize('change', [
    {'domains': ['example.org']}, {'article_selectors': ['body']},
    {'acquisition_mode': 'finite'}, {'feeds': ['https://www.contentplatform.info/feed/']},
    {'require_author_attribution': False}, {'exclude_selectors': ['table']},
])
def test_scope_cannot_silently_expand(change):
    candidate = entry()
    candidate.update(change)
    with pytest.raises(ValueError):
        source(candidate)
