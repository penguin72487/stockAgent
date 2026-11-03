"""Historical originals keep their own request/hash identity across revisions."""
from contextlib import closing
import hashlib
import json

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE, cfg, response  # noqa: F401
from test_news_cnyes_history_evidence import (  # noqa: F401
    bounds, capture, head, queue, seed_accepted_batch_for_history_page, source_payload,
)


def _older_head_and_batch(queue, monkeypatch):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    other = cnyes_url(BASE, start=1388534600, end=1388534700)
    data = source_payload([1, 2], epoch=bounds(url)[0])
    source = queue.registry.sources['cnyes']
    wrong = history.retain_response(queue, response(data, other), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, url, data)
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
    relative, receipt = seed_accepted_batch_for_history_page(queue, url, data)
    newer = response(source_payload([3, 4], epoch=bounds(url)[0]), url)
    with queue.db:
        queue.db.execute("UPDATE discovery SET response_sha256=?,error_code='http_429',attempts=3,due=9e9 "
                         'WHERE url=?', (hashlib.sha256(newer.body).hexdigest(), url))
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30)['mismatched'] == 1
    return url, data, relative, receipt


def test_old_original_is_indexed_and_repaired_after_current_job_hash_changes(queue, monkeypatch):
    url, data, relative, receipt = _older_head_and_batch(queue, monkeypatch)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    old_hash = hashlib.sha256(response(data, url).body).hexdigest()
    assert old_hash != before['response_sha256']

    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=30)
    assert result['indexed'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
                            'WHERE request_url=? AND response_sha256=? AND parquet_path=?',
                            (url, old_hash, relative)).fetchone()
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(12):
            history.backfill(resumed, limit=20, max_seconds=30)
            if head(resumed, url)['state'] == 'list_complete':
                break
        assert head(resumed, url)['state'] == 'list_complete'
        row = resumed.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=1', (cnyes_scope(url),)).fetchone()
        obs = json.loads(row[0])
        assert obs['request_url'] == url and obs['response_sha256'] == old_hash
        assert obs['artifact'] == {'kind': 'accepted_body_batch', 'parquet_path': relative, 'receipt_path': receipt}
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_prior_completed_scan_cannot_hide_new_revision_rule(queue, monkeypatch):
    url, data, relative, _ = _older_head_and_batch(queue, monkeypatch)
    source = queue.registry.sources['cnyes']
    old_key = 'cnyes_history_accepted_batch_scan_v1:' + history.digest((
        source.provider, history.cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    old_state = {'version': 1, 'state': 'complete', 'path': relative, 'end_path': relative,
                 'checked': 1, 'indexed': 0, 'retry_after': 0, 'attempts': 0}
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                         (old_key, json.dumps(old_state)))
    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=30)
    assert history.accepted_batch_scan_key(source) != old_key
    assert result['indexed'] == 1
    assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (old_key,)).fetchone()[0]) == old_state
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE request_url=? AND response_sha256=?',
                            (url, hashlib.sha256(response(data, url).body).hexdigest())).fetchone()


def test_old_revision_candidate_and_wakeup_roll_back_together_on_local_failure(queue, monkeypatch):
    url, _, relative, _ = _older_head_and_batch(queue, monkeypatch)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? "
                         'WHERE request_url=?', ('{"previous":"preserve"}', url))

    def fail_after_registration(*args, **kwargs):
        raise OSError('simulated interrupted batch wake')

    with monkeypatch.context() as patch:
        patch.setattr(history, '_queue_late_accepted_candidate', fail_after_registration)
        result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=30)
    assert not result['complete']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                                (relative,)).fetchone()
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'retained_invalid'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries WHERE parquet_path=?',
                            (relative,)).fetchone()
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=30)['indexed'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'pending'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (url,)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_different_request_with_identical_bytes_cannot_index_as_old_original(queue, monkeypatch):
    url, data, relative, _ = _older_head_and_batch(queue, monkeypatch)
    wrong = cnyes_url(BASE, start=1388534700, end=1388534800)
    with queue.db:
        queue.db.execute('DELETE FROM accepted_batches WHERE path=?', (relative,))
    seed_accepted_batch_for_history_page(queue, url, data, original_url=wrong, batch_tag='wrong')
    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=30)
    assert result['indexed'] == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE request_url=?', (url,)).fetchone()


@pytest.mark.parametrize('field,value', [
    ('authorization_fingerprint', 'f' * 64),
    ('source_bulk_fingerprint', 'e' * 64),
    ('contract_version', -1),
    ('corrupt_json', None),
])
def test_old_original_candidate_does_not_cross_source_contract_or_bad_observation(queue, monkeypatch, field, value):
    url, _, relative, _ = _older_head_and_batch(queue, monkeypatch)
    row = queue.db.execute('SELECT o.* FROM cnyes_history_page_heads h '
        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page=1', (cnyes_scope(url),)).fetchone()
    obs = json.loads(row['observation_json'])
    if field == 'corrupt_json':
        with queue.db:
            queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=? WHERE observation_sha256=?',
                             ('{invalid', row['observation_sha256']))
    else:
        obs[field] = value
        new_sha = history.digest(obs)
        column = 'evidence_contract_version' if field == 'contract_version' else field
        with queue.db:
            queue.db.execute(f'UPDATE cnyes_history_page_observations SET {column}=?,observation_sha256=?,observation_json=? '
                             'WHERE observation_sha256=?', (value, new_sha, history.encoded(obs), row['observation_sha256']))
            queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=? AND page=1',
                             (new_sha, cnyes_scope(url)))
    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=30)
    assert result['indexed'] == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                                (relative,)).fetchone()
