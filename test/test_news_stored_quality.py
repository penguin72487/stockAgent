"""Stored-body quality correction preserves source evidence and live work."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader import news_fulltext_repairs as repair
from downloader.news_fulltext_stored_quality import submit_stored_quality_reviews
from test_news_fulltext import article_html, config


URL = 'https://publisher.test/story/legacy'
OBSERVED = '2026-01-01T00:00:00+00:00'


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def legacy(queue, *, masked=True):
    queue.add_articles([{'url': URL}, {'url': 'https://publisher.test/story/other'}])
    http = SimpleNamespace(registry=queue.registry,
        get=lambda *_a, **_kw: Response(URL, 200, {'content-type': 'text/html'}, article_html()))
    record = fetch_article(http, head(queue))
    raw = article_html(free=not masked)
    # A synthetic legacy acceptance: today's parser would reject this mask.
    record.update(raw_response_body=raw, response_sha256=hashlib.sha256(raw).hexdigest(),
                  http_response_bytes=len(raw), captured_at_utc=OBSERVED)
    queue.accept([record])
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=4,due=? WHERE url=?', (time.time() + 3600, URL))
    return head(queue)


def submit(queue):
    return submit_stored_quality_reviews(queue.root, queue.registry, [URL],
        reason='Review the retained original member mask', expected_error='paywall_or_login')


def test_owner_reclassifies_only_reviewed_head_with_no_http_or_payload_change(config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        before = legacy(queue)
        files = {p: sha256_file(p) for p in (*tmp_path.glob('bodies/*/*.parquet'), *tmp_path.glob('receipts/*.json'))}
        neighbor = dict(queue.db.execute('SELECT * FROM articles WHERE url!=?', (URL,)).fetchone())
        counters = dict(queue.db.execute('SELECT key,value FROM counters'))
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no HTTP'))
        changes = queue.db.total_changes
        result = submit(queue)
        assert result['publisher_requests'] == 0 and not result['queue_mutations']
        assert queue.db.total_changes == changes and head(queue) == before
        assert Path(result['request_path']).stat().st_mode & 0o777 == 0o600
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'stored_quality_reclassified': 1}
        assert head(queue) == dict(before, state='paywall_or_login', error_code='paywall_or_login')
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url!=?', (URL,)).fetchone()) == neighbor
        assert all(sha256_file(p) == digest for p, digest in files.items())
        assert dict(queue.db.execute('SELECT key,value FROM counters'))['article_attempts_finished'] == counters['article_attempts_finished']
        assert queue.db.execute('SELECT recorded_at_utc FROM reviewed_repair_items').fetchone()[0] > OBSERVED
        receipt_path = tmp_path / 'reviewed_repairs/processed' / (result['request_id'] + '.outcome.json')
        receipt = json.loads(receipt_path.read_bytes())
        assert receipt['reclassified_targets'] == 1 and receipt['publisher_requests'] == 0
        assert not receipt['source_observation_clocks_changed'] and not receipt['source_freshness_verified']
        status = queue.status()['providers']['publisher']
        assert status['stored_body_urls'] == 1 and status['states']['paywall_or_login'] == 1
        assert status['body_state_counts']['stored'] == {'paywall_or_login': 1}
        assert verify_bodies(tmp_path)['accepted']
        assert repair.consume_repairs(queue)['processed'] == 0
        with pytest.raises(ValueError, match='requires_complete_head'):
            submit(queue)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as restarted:
        assert head(restarted)['state'] == 'paywall_or_login'
        assert head(restarted)['last_checked_at_utc'] == OBSERVED
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('change', [
    {'last_checked_at_utc': '2026-01-02T00:00:00+00:00'}, {'attempts': 5},
    {'due': 12345}, {'state': 'http_403', 'error_code': 'http_403'},
    {'lease_owner': 'another-owner', 'lease_until': 9999999999},
    {'body_sha256': 'a' * 64}, {'validator_url': 'https://publisher.test/story/changed'},
])
def test_superseded_observation_refusal_lease_or_saved_head_is_never_overwritten(config, tmp_path, change):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        legacy(queue)
        submit(queue)
        with queue.db:
            queue.db.execute('UPDATE articles SET ' + ','.join(key + '=?' for key in change) + ' WHERE url=?',
                             (*change.values(), URL))
        before = head(queue)
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'quality_review_head_changed': 1}
        assert head(queue) == before


def test_unmasked_current_original_cannot_be_downgraded(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        before = legacy(queue, masked=False)
        with pytest.raises(ValueError, match='native_validation_succeeded'):
            submit(queue)
        assert head(queue) == before and not (tmp_path / 'reviewed_repairs/pending').exists()


def test_corrupt_accepted_receipt_rejects_review_without_changing_head(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        before = legacy(queue)
        result = submit(queue)
        path = next(tmp_path.glob('receipts/*.json'))
        value = json.loads(path.read_bytes())
        value['sha256'] = '0' * 64
        path.write_text(json.dumps(value))
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'quality_review_evidence_invalid': 1}
        assert head(queue) == before
        assert not Path(result['request_path']).exists()


def test_temporarily_unavailable_payload_does_not_consume_review_or_stop_other_work(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        before = legacy(queue)
        result = submit(queue)
        path = tmp_path / before['parquet_path']
        parked = path.with_suffix('.unavailable')
        path.rename(parked)
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'retained_outcome_io_deferred': 1}
        assert head(queue) == before and Path(result['request_path']).exists()
        parked.rename(path)
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'stored_quality_reclassified': 1}


def test_committed_review_recovers_archive_without_second_source_attempt(config, tmp_path, monkeypatch):
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        before = legacy(queue)
        result = submit(queue)
        with monkeypatch.context() as patch:
            patch.setattr(repair, '_archive', lambda *a, **k: (_ for _ in ()).throw(OSError('synthetic archive interruption')))
            assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'stored_quality_reclassified': 1}
        assert Path(result['request_path']).exists()
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert repair.consume_repairs(queue, max_seconds=0.1)['processed'] == 0
        assert not Path(result['request_path']).exists()
        assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0] == 1
        assert head(queue) == dict(before, state='paywall_or_login', error_code='paywall_or_login')


def test_source_identity_change_and_malformed_request_are_rejected(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        before = legacy(queue)
        result = submit(queue)
        path = Path(result['request_path'])
        request = json.loads(path.read_bytes())
        request['sources']['publisher']['data_fingerprint'] = 'a' * 64
        payload = repair._bytes(request)
        changed = path.with_name(hashlib.sha256(payload).hexdigest() + '.json')
        path.rename(changed)
        changed.write_bytes(payload)
        assert repair.consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'quality_review_source_changed': 1}
        assert head(queue) == before
        request['expected_error'] = 'http_429'
        from downloader.news_fulltext_stored_quality import validate_request
        with pytest.raises(ValueError, match='invalid_stored_quality_request'):
            validate_request(request)
