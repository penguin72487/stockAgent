"""A new root observation uses the old closed refusal, never re-arms it."""
from dataclasses import replace
import json

from bs4 import BeautifulSoup
import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import PendingOriginal
from downloader.news_fulltext_candidates import candidate_response_metadata
from downloader.news_fulltext_contentplatform import (
    CONTENTPLATFORM_ORIGINAL_LAYOUT, CONTENTPLATFORM_ROOT_QUESTION,
    contentplatform_root_probe,
)
from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_contentplatform import URL, RAW, page
from test_news_contentplatform_originals import owner, head, submit, response, http_stub
from test_news_fulltext import config


def nested_page(*, inner=None, leading=''):
    soup = BeautifulSoup(page(), 'lxml')
    body = soup.select_one('.tdb_single_content > .tdb-block-inner')
    wrapper = soup.new_tag('div')
    if inner is None:
        for child in list(body.contents):
            wrapper.append(child.extract())
    else:
        body.clear()
        fragment = BeautifulSoup(inner, 'html.parser')
        for child in list(fragment.contents):
            wrapper.append(child.extract())
    body.append(wrapper)
    if leading:
        fragment = BeautifulSoup(leading, 'html.parser')
        for child in reversed(list(fragment.contents)):
            body.insert(0, child.extract())
    return soup.encode()


def root_submit(queue, settings, reason='Inspect the leading nested signed root'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL,
        CONTENTPLATFORM_ORIGINAL_LAYOUT, reason=reason, evidence_contract_version=44)


def close_header_refusal(queue, settings):
    requested = submit(queue, settings)
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=nested_page()))
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    result = diagnostic.fetch_candidate(http, job)
    assert not isinstance(result, PendingOriginal)
    assert result['private_original_scope_error'] == 'article_identity_not_verified'
    diagnostic.finish_candidate(queue, job, result=result)
    closed = dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (requested['request_id'],)).fetchone())
    return requested, closed


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_root_facts_preserve_uncertainty_and_do_not_accept_prose(owner, parser):
    queue, _ = owner
    source = replace(queue.registry.sources['contentplatform'], html_parser=parser)
    original = response(body=nested_page())
    facts = contentplatform_root_probe(original, source)
    assert facts['direct_tag_counts'] == {'div': 1}
    assert facts['first_written_candidate_path'] == ['div', 'p']
    assert facts['first_candidate_is_nested_paragraph']
    assert facts['first_credit_matches_declared_author']
    assert not facts['body_geometry_accepted'] and not facts['source_text_exported']
    assert RAW not in json.dumps(facts, ensure_ascii=False) and '許' not in json.dumps(facts, ensure_ascii=False)
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract_article(original.body, source, page_url=URL)


@pytest.mark.parametrize('inner,leading', [
    ('<p>其他記者</p><p>【許家源記者 / 綜合報導】</p>', ''),
    ('文字在段落之前<p>【許家源記者 / 綜合報導】</p>', ''),
    ('<p>【許家源記者 / 綜合報導】</p>', '<!--unknown root-->'),
    ('<p>【許家源記者 / 綜合報導】</p>', '<h3>先出現的文字</h3>'),
    ('<section><p>【許家源記者 / 綜合報導】</p></section>', ''),
])
def test_does_not_search_later_nodes_for_matching_signature(owner, inner, leading):
    queue, _ = owner
    source = queue.registry.sources['contentplatform']
    facts = candidate_response_metadata(response(body=nested_page(inner=inner, leading=leading)),
        source, source, evidence_contract_version=44, requested_url=URL)
    assert not facts['native_root_shape']['first_credit_matches_declared_author']
    assert facts['private_original_scope_error'] == 'private_contentplatform_nested_credit_not_verified'
    assert 'private_original_permit' not in facts


def test_closed_refusal_allows_one_distinct_observation_and_preserves_old_work(owner, monkeypatch):
    queue, settings = owner
    before = head(queue)
    charges = list(map(tuple, queue.db.execute('SELECT * FROM queue_settings')))
    old, closed = close_header_refusal(queue, settings)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *a, **k: pytest.fail('No additional HTTP owner'))
    requested = root_submit(queue, settings)
    assert requested['request_id'] != old['request_id']
    assert root_submit(queue, settings, reason='Changed wording')['request_id'] == requested['request_id']
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=nested_page()))
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    original = diagnostic.fetch_candidate(http, job)
    assert isinstance(original, PendingOriginal) and http.calls == [URL]
    assert original.permit['question'] == CONTENTPLATFORM_ROOT_QUESTION
    diagnostic.finish_candidate(queue, job, result=original)
    status = diagnostic.candidate_status(queue.root, requested['request_id'])
    assert status['state'] == 'observed' and status['raw_or_names_or_body_retained']
    ref = status['result_json']['private_original']
    record, = pq.ParquetFile(queue.root / ref['path']).read().to_pylist()
    assert record['raw_response_body'] == nested_page()
    assert not record['accepted_as_article'] and not record['historical_point_in_time']
    assert not root_submit(queue, settings)['submitted']
    assert not submit(queue, settings)['submitted']
    assert dict(queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',
        (old['request_id'],)).fetchone()) == closed
    assert head(queue) == before
    assert list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == charges
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()


def test_root_request_requires_closed_matching_header_refusal(owner):
    queue, settings = owner
    with pytest.raises(ValueError, match='completed_contentplatform_unretained_header_refusal_required'):
        root_submit(queue, settings)
    submit(queue, settings)
    diagnostic.pump_candidates(queue)
    with pytest.raises(ValueError, match='completed_contentplatform_unretained_header_refusal_required'):
        root_submit(queue, settings)


def test_frame_identity_refusal_does_not_authorize_nested_root_question(owner):
    queue, settings = owner
    submit(queue, settings)
    diagnostic.pump_candidates(queue)
    raw = nested_page().replace(f'href="{URL}"'.encode(),
        f'href="{URL.replace("533649", "533650")}"'.encode(), 1)
    http = http_stub(response(body=raw))
    job = diagnostic.claim_candidate(queue, http, settings, 'contentplatform')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    with pytest.raises(ValueError, match='completed_contentplatform_unretained_header_refusal_required'):
        root_submit(queue, settings)
    assert http.calls == [URL]


@pytest.mark.parametrize('mutation', ['retained', 'question', 'url', 'future', 'nonterminal', 'head'])
def test_revalidates_original_refusal_before_sending_new_question(owner, mutation):
    queue, settings = owner
    _, closed = close_header_refusal(queue, settings)
    requested = root_submit(queue, settings)
    with queue.db:
        if mutation == 'head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
        elif mutation == 'future':
            queue.db.execute("UPDATE native_candidate_jobs SET completed_at_utc='2099-01-01T00:00:00+00:00' WHERE request_id=?",
                (closed['request_id'],))
        elif mutation == 'nonterminal':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?",
                (closed['request_id'],))
        else:
            result = json.loads(closed['result_json'])
            if mutation == 'retained':
                result['private_original'] = {'path': 'cannot-authorize-a-new-get'}
            elif mutation == 'question':
                result['question'] = 'different-question'
            else:
                result['final_url'] = URL.replace('533649', '533650')
            queue.db.execute('UPDATE native_candidate_jobs SET result_json=? WHERE request_id=?',
                (json.dumps(result), closed['request_id']))
    diagnostic.pump_candidates(queue)
    http = http_stub(response(body=nested_page()))
    assert diagnostic.claim_candidate(queue, http, settings, 'contentplatform') is None
    assert not http.calls
    assert diagnostic.candidate_status(queue.root, requested['request_id'])['state'] != 'observed'
