"""Recovery of distinct source evidence for a shared page or older work."""
from datetime import datetime, timezone
import hashlib
import json

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from test_cnyes_news_bulk import BASE, cfg, response  # noqa: F401
from test_news_cnyes_history_evidence import bounds, capture, head, queue, seed_equivalent, source_payload  # noqa: F401


@pytest.mark.parametrize('older_wrong_expectation', [False, True])
def test_alias_with_own_original_recovers_while_exemplar_is_still_unproven(queue, monkeypatch,
                                                                          older_wrong_expectation):
    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    source = queue.registry.sources['cnyes']
    alias_response = response(first, alias)
    exact = history.retain_response(queue, alias_response, source, accepted=[], restricted=False)
    wrong_url = cnyes_url(BASE, start=low + 301, end=low + 400)
    wrong = history.retain_response(queue, response(first, wrong_url), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=low, end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute("UPDATE discovery SET response_sha256=?,last_checked_at_utc=?,error_code='http_429',"
                         'attempts=3,due=9e9 WHERE url=?',
                         (hashlib.sha256(alias_response.body).hexdigest(), '2020-01-01T00:00:00+00:00', alias))
        if older_wrong_expectation:
            queue.db.execute('INSERT INTO cnyes_history_retained_work '
                '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                'VALUES (?,?,?,?,?,?,?)', (alias, cnyes_scope(alias), 1,
                    hashlib.sha256(alias_response.body).hexdigest(), 1, '0' * 64,
                    '2020-01-01T00:00:00+00:00'))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30)['mismatched'] >= 1
    alias_work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (alias,)).fetchone()
    assert alias_work is not None and alias_work['state'] == 'pending'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=30)
        if head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence':
            break
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    assert head(queue, exemplar)['state'] == 'waiting_original_page_recheck'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()[0])
    assert observation['artifact'] == exact
    if older_wrong_expectation:
        archived = json.loads(queue.db.execute('SELECT original_work_json FROM cnyes_history_retained_revision_audit '
                                               'WHERE request_url=?', (alias,)).fetchone()[0])
        assert archived['work']['expected_total'] == 1 and archived['work']['state'] == 'retained_invalid'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


def test_same_response_with_older_incorrect_expectation_can_recover_current_head(queue, monkeypatch):
    first, second = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    wrong = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    data = source_payload([], epoch=bounds(second)[0])
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, second, data)
    source = queue.registry.sources['cnyes']
    current_response = response(data, second)
    exact = history.retain_response(queue, current_response, source, accepted=[], restricted=False)
    sha = hashlib.sha256(current_response.body).hexdigest()
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (second,))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (second, cnyes_scope(second), 1, sha, 1, '0' * 64,
                                       datetime.now(timezone.utc).isoformat()))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30)['mismatched'] == 1
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=30)
        if head(queue, second)['state'] == 'list_complete':
            break
    assert head(queue, second)['state'] == 'list_complete'
    archived = json.loads(queue.db.execute('SELECT original_work_json FROM cnyes_history_retained_revision_audit '
                                           'WHERE request_url=?', (second,)).fetchone()[0])
    assert archived['work']['response_sha256'] == sha
    assert archived['work']['expected_total'] == 1
    assert archived['work']['state'] == 'retained_invalid'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=?', (cnyes_scope(second),)).fetchone()[0])
    assert observation['artifact'] == exact
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
