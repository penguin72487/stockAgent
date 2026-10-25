"""First written credit after media is a new finite diagnostic, not admission."""
import json

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_contentplatform import CONTENTPLATFORM_IMAGE_QUESTION
from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_contentplatform import URL, RAW
from test_news_contentplatform_originals import owner, head, submit, response, http_stub
from test_news_contentplatform_roots import nested_page, root_submit
from test_news_fulltext import config


def image_page(inner=None):
    return nested_page(inner=inner if inner is not None else
        f'<img src="/never-fetched.jpg"><p>【許家源記者 / 綜合報導】</p><p>{RAW}</p>')


def image_submit(queue, settings, reason='Inspect the first written credit after leading media'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL,
        queue.registry.sources['contentplatform'].embedded_article_layout,
        reason=reason, evidence_contract_version=45)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version,inner,permitted', [
    (44, '<p>【許家源記者 / 綜合報導】</p><p>SECRET prose</p>', True),
    (44, '<img src="/never.jpg"><p>【許家源記者 / 綜合報導】</p>', False),
    (45, '<img src="/never.jpg"><p>【許家源記者 / 綜合報導】</p>', True),
    (45, '<img src="/never.jpg"><p>SECRET other writer</p>', False),
    (44, '<p>【許家源記者 / 綜合報導】</p><p hidden>SECRET hidden</p>', False),
    (45, '<img src="/never.jpg"><p>【許家源記者 / 綜合報導】</p><p hidden>SECRET hidden</p>', False),
])
def test_same_response_scope_and_shape_use_one_parse(owner, monkeypatch, parser, version, inner, permitted):
    from dataclasses import replace
    from downloader import news_fulltext_policy as policy
    queue, _ = owner
    source = replace(queue.registry.sources['contentplatform'], html_parser=parser)
    original, calls = policy.public_article_document, []
    def document(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'public_article_document', document)
    raw = image_page(inner)
    result = candidate_response_metadata(response(body=raw), source, source,
        evidence_contract_version=version, requested_url=URL)
    assert calls == [raw]
    assert ('private_original_permit' in result) == permitted
    key = {44: 'native_root_shape', 45: 'native_image_credit_shape'}[version]
    shape = result[key]
    assert not shape['article_accepted'] and not shape['body_geometry_accepted']
    assert not shape['source_text_exported']
    assert 'SECRET' not in json.dumps(result) and '許' not in json.dumps(result, ensure_ascii=False)
    if permitted:
        assert result['private_original_permit']['native_permit'] == {
            **shape, 'signed_reporter_and_explicit_clocks_verified': True}


@pytest.mark.parametrize('inner,prefix,path', [
    ('<img src="/never.jpg"><p>【許家源記者 / 綜合報導】 SECRET original prose</p>', True, ['div', 'p']),
    ('<p>SECRET original prose</p><p>【許家源記者 / 綜合報導】</p>', False, ['div', 'p']),
    ('<secret-source-words>SECRET text</secret-source-words>', False, ['div', '#other']),
    ('<div><figure><img src="/never.jpg"></figure></div>', False, ['div', 'div', 'figure']),
])
def test_first_header_refusal_keeps_bounded_same_dom_evidence_without_a_new_question(owner, monkeypatch, inner, prefix, path):
    from downloader import news_fulltext_policy as policy
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    original, calls = policy.public_article_document, []
    def document(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)
    monkeypatch.setattr(policy, 'public_article_document', document)
    raw = image_page(inner)
    result = candidate_response_metadata(response(body=raw), source, source,
        evidence_contract_version=43, requested_url=URL)
    assert calls == [raw] and 'private_original_permit' not in result
    assert result['private_original_scope_error'] == 'article_identity_not_verified'
    context = result['native_header_failure_context']
    assert context['first_written_candidate_path'] == path
    assert context['signed_prefix_matches_declared_author'] is prefix
    assert context['signed_prefix_followed_by_prose'] is prefix
    assert context['whole_paragraph_credit_matches_declared_author'] is False
    assert not context['article_accepted'] and not context['body_geometry_accepted']
    assert not context['source_text_exported']
    exported = json.dumps(result, ensure_ascii=False)
    assert all(word not in exported for word in ('SECRET', '許家源', 'secret-source-words'))


def test_nonvisible_leading_credit_cannot_become_positive_failure_evidence(owner):
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    result = candidate_response_metadata(response(body=image_page(
        '<p hidden>【許家源記者 / 綜合報導】 SECRET hidden</p>')), source, source,
        evidence_contract_version=43, requested_url=URL)
    assert result['private_original_scope_error'] == 'article_identity_not_verified'
    assert result['native_header_failure_context']['leading_shape_visible'] is False
    assert 'signed_prefix_matches_declared_author' not in result['native_header_failure_context']
    assert 'private_original_permit' not in result


def completed_prior(queue, settings):
    for create in (submit, root_submit):
        requested = create(queue, settings)
        diagnostic.pump_candidates(queue)
        http = http_stub(response(body=image_page()))
        job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
        result = diagnostic.fetch_candidate(http, job)
        assert not isinstance(result, PendingOriginal)
        diagnostic.finish_candidate(queue, job, result=result)
    return requested


def test_retains_one_replayable_original_without_accepting_or_reopening_old_work(owner, monkeypatch):
    queue, settings = owner
    before = head(queue)
    charges = list(map(tuple, queue.db.execute('SELECT * FROM queue_settings')))
    completed_prior(queue, settings)
    closed = list(map(tuple, queue.db.execute('SELECT * FROM native_candidate_jobs ORDER BY request_id')))
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('No new HTTP owner'))
    requested = image_submit(queue, settings)
    assert image_submit(queue, settings, reason='Other wording')['request_id'] == requested['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=image_page()))
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    original = diagnostic.fetch_candidate(http, job)
    assert isinstance(original, PendingOriginal)
    assert original.permit['question'] == CONTENTPLATFORM_IMAGE_QUESTION
    assert http.calls == [URL]
    diagnostic.finish_candidate(queue, job, result=original)
    status = diagnostic.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained']
    shape = status['result_json']['native_image_credit_shape']
    assert shape['leading_image_paths'] == [['div', 'img']]
    assert shape['first_written_candidate_path'] == ['div', 'p']
    assert shape['first_credit_matches_declared_author']
    assert not shape['article_accepted'] and not shape['body_geometry_accepted']
    assert RAW not in json.dumps(shape) and '許' not in json.dumps(shape, ensure_ascii=False)
    ref = status['result_json']['private_original']
    raw, = pq.ParquetFile(queue.root/ref['path']).read().to_pylist()
    assert raw['raw_response_body'] == image_page() and not raw['accepted_as_article']
    assert head(queue) == before
    assert list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == charges
    assert list(map(tuple, queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id<>? ORDER BY request_id',
        (requested['request_id'],)))) == closed
    assert not image_submit(queue, settings)['submitted']
    assert not root_submit(queue, settings)['submitted'] and not submit(queue, settings)['submitted']
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(raw['raw_response_body'], queue.registry.sources['contentplatform'], page_url=URL)


@pytest.mark.parametrize('inner', [
    '<img src="/never.jpg"><p>其他人的原文</p><p>【許家源記者 / 綜合報導】</p>',
    '<img src="/never.jpg">第一個書寫文字<p>【許家源記者 / 綜合報導】</p>',
    '<a href="/never"><img src="/never.jpg"></a><p>【許家源記者 / 綜合報導】</p>',
    '<p>【許家源記者 / 綜合報導】</p>',
    '<img src="/1.jpg">'*5+'<p>【許家源記者 / 綜合報導】</p>',
    '<div>'*6+'<img src="/never.jpg"><p>【許家源記者 / 綜合報導】</p>'+'</div>'*6,
    '<img src="/never.jpg">'+' '*100+'<h3>在先的標題</h3><p>【許家源記者 / 綜合報導】</p>',
])
def test_stops_at_first_written_or_unknown_node_instead_of_searching_for_a_name(owner, inner):
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    facts = candidate_response_metadata(response(body=image_page(inner)), source, source,
        evidence_contract_version=45, requested_url=URL)
    assert facts['native_image_credit_shape']['first_credit_matches_declared_author'] is False
    assert facts['private_original_scope_error'] == 'private_contentplatform_post_image_credit_not_verified'
    assert 'private_original_permit' not in facts


@pytest.mark.parametrize('mutation', ['retained', 'root', 'nonterminal', 'future'])
def test_rechecks_the_closed_leading_image_refusal_before_new_reservation(owner, mutation):
    queue, settings = owner
    prior = completed_prior(queue, settings)
    requested = image_submit(queue, settings)
    with queue.db:
        row = queue.db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?',
            (prior['request_id'],)).fetchone()
        if mutation == 'nonterminal':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",
                (prior['request_id'],))
        elif mutation == 'future':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",
                (prior['request_id'],))
        else:
            result = json.loads(row[0])
            if mutation == 'retained':
                result['private_original'] = {'path': 'previous-original-cannot-buy-another-get'}
            else:
                result['native_root_shape']['first_written_candidate_path'] = ['div', 'p']
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',
                (json.dumps(result), prior['request_id']))
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=image_page()))
    assert diagnostic.claim_candidate(queue, http, settings, 'contentplatform') is None
    assert not http.calls
    assert diagnostic.candidate_status(queue.root, requested['request_id'])['state'] != 'observed'
