"""Current-source finance caption witnesses stay separate from article admission."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidate_originals import (
    ETTODAY_CAPTION_ORIGINAL_QUESTION, PendingOriginal, private_ettoday_caption_original_permit,
    private_ettoday_credit_original_permit, retain_original,
)
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday as native

URL, LAYOUT = native.URLS[0], 'ettoday_visible_article_v40'
CAPTION, SIGNATURE = '▲原始圖片-12.5%、123456股。（圖／原始攝影）', '記者研究記者／台北報導'


def page():
    soup = native.page('finance')
    first, caption, credit = soup.select('.story > p')[:3]
    caption.decompose()
    first.append(soup.new_tag('br'))
    strong = soup.new_tag('strong'); strong.string = CAPTION
    first.append(strong)
    assert credit.get_text(' ', strip=True) == SIGNATURE
    return soup


def response(soup=None, **changes):
    return replace(Response(URL, 200, {'content-type': 'text/html; charset=utf-8'},
        str(page() if soup is None else soup).encode()), **changes)


@pytest.fixture
def settings(tmp_path):
    entry = native.recipe(); entry['embedded_article_layout'] = LAYOUT
    return dict(contract_version=1, root=str(tmp_path), collection_purpose='personal_research',
        publish=False, remote_training_export=False, providers=[entry], min_free_disk_bytes=1,
        workers=2, exclusive_http_owner=False, adaptive_rate={'enabled': False})


@pytest.fixture
def queue(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    owner.add_articles([dict(url=URL)])
    with owner.db:
        owner.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=?',
            ('article_attribution_not_verified', 'article_attribution_not_verified', time.time()-60,
                '2026-01-01T00:00:00+00:00'))
    yield owner
    owner.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def submit(queue, settings, **changes):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT,
        reason=changes.pop('reason', 'Unanswered image/caption and following reporter geometry'),
        evidence_contract_version=60, **changes)


def http_stub(queue, envelope=None):
    calls = []
    def get(url, *, follow_redirects):
        assert follow_redirects is False
        calls.append(url)
        return response() if envelope is None else envelope
    return SimpleNamespace(registry=queue.registry, calls=calls, get=get, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def claim(queue, settings, http):
    deadline = time.monotonic()+3
    while time.monotonic() < deadline:
        diagnostic.pump_candidates(queue)
        job = diagnostic.claim_candidate(queue, http, settings, 'ettoday')
        if job is not None:
            return job
        time.sleep(.005)
    pytest.fail('The same submitted handle did not become ready in bounded owner ticks')


def test_caption_question_is_private_and_does_not_enable_house_parser(queue, monkeypatch):
    source = queue.registry.sources['ettoday']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(response().body, source, page_url=URL)
    monkeypatch.setattr('downloader.news_fulltext_ettoday.ettoday_visible_article',
        lambda *a, **k: pytest.fail('A private witness cannot accept an article'))
    facts = candidate_response_metadata(response(), source, source, evidence_contract_version=60, requested_url=URL)
    permit = facts['private_original_permit']
    assert permit['question'] == ETTODAY_CAPTION_ORIGINAL_QUESTION
    assert permit['printed_signature_sha256'] == hashlib.sha256(SIGNATURE.encode()).hexdigest()
    assert facts['native_caption_structure']['first_paragraph_children'] == ['img', 'br', 'strong']
    assert not facts['accepted_as_article'] and not facts['raw_or_names_or_body_retained']
    assert all(text not in json.dumps(facts, ensure_ascii=False) for text in (CAPTION, SIGNATURE, native.PROSE))


@pytest.mark.parametrize('mutation', [
    'no_image', 'no_break', 'caption_markup', 'hidden_caption', 'hidden_credit', 'embedded_credit',
    'extra_prose', 'missing_credit', 'no_prose', 'canonical', 'schema_identity', 'wrong_person',
    'foreign_publisher', 'schema_headline', 'clock', 'paid', 'reprint', 'iframe', 'duplicate_story',
])
def test_geometry_identity_credit_clocks_and_source_scope_must_all_be_proved(queue, mutation):
    soup = page(); first = soup.select_one('.story > p'); credit = soup.select_one('#credit')
    if mutation == 'no_image': first.img.decompose()
    elif mutation == 'no_break': first.br.decompose()
    elif mutation == 'caption_markup': first.strong.string.wrap(soup.new_tag('span'))
    elif mutation == 'hidden_caption': first['hidden'] = ''
    elif mutation == 'hidden_credit': credit['style'] = 'display:none'
    elif mutation == 'embedded_credit': credit.string = '未驗證前言'+SIGNATURE
    elif mutation == 'extra_prose': first.append('此為正文，不能當作圖說刪除')
    elif mutation == 'missing_credit': credit.decompose()
    elif mutation == 'no_prose':
        for node in list(credit.find_next_siblings()): node.decompose()
    elif mutation == 'canonical': soup.select_one('link[rel=canonical]')['href'] = URL+'0'
    elif mutation == 'schema_identity': native.update_schema(soup, url=URL+'0')
    elif mutation == 'wrong_person': native.update_schema(soup, author={'@type': 'Person', 'name': '其他記者'})
    elif mutation == 'foreign_publisher': native.update_schema(soup, publisher={'@type': 'Organization', 'name': '其他來源'})
    elif mutation == 'schema_headline': native.update_schema(soup, headline='其他新聞標題')
    elif mutation == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif mutation == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif mutation == 'reprint': soup.select_one('#prose').string = '本文轉載自未驗證來源'
    elif mutation == 'iframe': soup.select_one('.story').append(soup.new_tag('iframe'))
    else: soup.select_one('.subject_article').append(deepcopy(soup.select_one('.story')))
    with pytest.raises(NewsError):
        private_ettoday_caption_original_permit(response(soup), queue.registry.sources['ettoday'], URL)


@pytest.mark.parametrize('changes', [
    {'status': 403}, {'status': 429}, {'status': 500}, {'body': b''}, {'body': b'x'*((2 << 20)+1)},
    {'url': URL+'0'}, {'headers': {'content-type': 'application/json'}},
])
def test_transport_refusal_never_grants_original(queue, changes):
    with pytest.raises(NewsError):
        private_ettoday_caption_original_permit(response(**changes), queue.registry.sources['ettoday'], URL)


def test_old_questions_and_other_hosts_are_not_rearmed(queue, settings):
    source = queue.registry.sources['ettoday']
    for version in (27, 28, 29, 30, 31, 32, 33, 51, 52):
        assert not candidate_evidence_allowed(source, source, version)
    old = replace(source, embedded_article_layout='ettoday_visible_article_v39')
    assert not candidate_evidence_allowed(old, old, 60)
    with pytest.raises(NewsError):
        private_ettoday_credit_original_permit(response(), source, URL)
    for url in (native.URLS[1], URL+'?route=1', URL+'#credit', URL.replace('https:', 'http:')):
        with pytest.raises(NewsError):
            private_ettoday_caption_original_permit(response(url=url), source, url)
    assert diagnostic._key({'provider': 'ettoday', 'id': 'a'*64}, 'b'*64, evidence_contract_version=60) == diagnostic._key(
        {'provider': 'ettoday', 'id': 'a'*64}, 'c'*64, evidence_contract_version=60)


def test_owner_reserves_once_retains_exact_raw_and_preserves_article_head(queue, settings):
    before = head(queue)
    request = submit(queue, settings)
    http = http_stub(queue); job = claim(queue, settings, http)
    reserved = queue.db.execute('SELECT state,owner_id FROM native_candidate_jobs WHERE request_id=?',
        (request['request_id'],)).fetchone()
    assert tuple(reserved) == ('reserved', queue.owner)
    pending = diagnostic.fetch_candidate(http, job)
    assert isinstance(pending, PendingOriginal)
    diagnostic.finish_candidate(queue, job, result=pending)
    result = diagnostic.candidate_status(queue.root, request['request_id'])
    assert result['state'] == 'observed' and result['raw_retention_state'] == 'retained_verified'
    reference = result['result_json']['private_original']
    original = pq.ParquetFile(queue.root/reference['path']).read().to_pylist()[0]
    assert original['raw_response_body'] == pending.response.body
    assert original['source_document_url'] == URL and original['transport_redirects_json'] == '[]'
    assert not original['accepted_as_article'] and not original['publish'] and not original['remote_training_export']
    assert (queue.root/reference['path']).stat().st_mode & 0o777 == 0o600
    assert head(queue) == before and http.calls == [URL]
    assert not submit(queue, settings, reason='Changing the reason cannot rearm')['submitted']
    with queue.db: queue.db.execute('UPDATE articles SET attempts=attempts+1')
    assert not submit(queue, settings)['submitted']
    assert diagnostic.claim_candidate(queue, http, settings, 'ettoday') is None
    assert http.calls == [URL]


def test_failed_shape_keeps_only_finite_facts_and_does_not_stall_owner(queue, settings):
    soup = page(); soup.select_one('.story > p').append('不可刪除的額外正文')
    before = head(queue); request = submit(queue, settings)
    http = http_stub(queue, response(soup)); job = claim(queue, settings, http)
    result = diagnostic.fetch_candidate(http, job)
    assert isinstance(result, dict) and 'private_original_scope_error' in result
    assert '不可刪除的額外正文' not in json.dumps(result, ensure_ascii=False)
    diagnostic.finish_candidate(queue, job, result=result)
    status = diagnostic.candidate_status(queue.root, request['request_id'])
    assert status['state'] == 'observed' and not status['result_json']['raw_or_names_or_body_retained']
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
    assert head(queue) == before and http.calls == [URL]


@pytest.mark.parametrize('change', ['lease', 'due', 'body', 'head'])
def test_claim_rechecks_protection_after_submission(queue, settings, change):
    submit(queue, settings); diagnostic.pump_candidates(queue)
    with queue.db:
        if change == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=?", (time.time()+3600,))
        elif change == 'due': queue.db.execute('UPDATE articles SET due=?', (time.time()+3600,))
        elif change == 'body': queue.db.execute("UPDATE articles SET body_sha256=?", ('c'*64,))
        else: queue.db.execute('UPDATE articles SET attempts=attempts+1')
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue, http, settings, 'ettoday') is None and not http.calls


def test_uncertain_reservation_stays_closed_across_owner_restart(queue, settings):
    request = submit(queue, settings)
    http = http_stub(queue); assert claim(queue, settings, http)
    queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome'"); queue.db.commit()
    queue.close()
    restarted = NewsQueue(queue.root, queue.registry)
    try:
        assert not submit(restarted, settings)['submitted']
        diagnostic.pump_candidates(restarted)
        assert diagnostic.claim_candidate(restarted, http, settings, 'ettoday') is None
        assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'uncertain_outcome'
        assert not http.calls
    finally:
        restarted.close()


def test_writer_independently_rechecks_raw_and_permit(queue, settings):
    submit(queue, settings); job = claim(queue, settings, http_stub(queue))
    envelope = response()
    permit = private_ettoday_caption_original_permit(envelope, job['source'], URL)
    changed = replace(envelope, body=envelope.body.replace(SIGNATURE.encode(), b'unknown'))
    with pytest.raises((NewsError, ValueError)):
        retain_original(queue, job, PendingOriginal(changed, {}, permit))
    assert not list(queue.root.glob('reviewed_repairs/candidate_diagnostics/originals/*'))
