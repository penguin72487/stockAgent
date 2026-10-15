"""Read-only diagnostic acceptance guards, with synthetic source responses."""
import hashlib
import json

import pytest

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_cnyes_problem_pages import audit
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, short_page


@pytest.mark.parametrize('mode,accepted', [('coherent', True), ('newer_head', False), ('missing_row', False)])
def test_body_proof_must_match_the_observed_native_head(cnyes_config, tmp_path, monkeypatch, mode, accepted):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    payload = short_page()
    article = extract_article(payload, registry.sources['cnyes'], page_url=URL)
    root, diagnostic = tmp_path / 'news', tmp_path / 'probe.json'
    raw = tmp_path / 'probe.raw'
    atomic_write_bytes(raw, payload)
    atomic_write_json(diagnostic, {'observed_at_utc': '2026-10-10T00:00:00+00:00', 'probes': [{
        'url': URL, 'status': 200, 'response_sha256': hashlib.sha256(payload).hexdigest(),
        'private_raw_path': str(raw)}]})
    queue = NewsQueue(root, registry)
    try:
        if mode != 'missing_row':
            with queue.db:
                queue.add_articles([{'url': URL}])
                queue.db.execute("UPDATE articles SET state='complete',body_sha256=?,last_checked_at_utc=?",
                                 (article['body_sha256'], '2026-10-10T00:01:00+00:00'))
    finally:
        queue.close()
    proof_hash = 'different-head-after-reader' if mode == 'newer_head' else article['body_sha256']
    monkeypatch.setattr('scripts.audit_cnyes_problem_pages.audit_bodies', lambda *a, **k: {
        'accepted': mode != 'missing_row', 'articles': [] if mode == 'missing_row' else [{
            'url': URL, 'body_sha256': proof_hash}]})
    result = audit(root, registry, diagnostic)
    assert result['accepted'] is accepted
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['historical_completeness_verified']
    assert not result['publish'] and not result['training_injection_enabled']
    if mode == 'newer_head':
        assert any(e['error'] == 'native_head_changed_since_outcome_snapshot' for e in result['errors'])
    if mode == 'missing_row':
        assert any(e['error'] == 'native_outcome_or_check_clock_mismatch' for e in result['errors'])


def test_raw_response_changed_is_not_an_accepted_source_proof(cnyes_config, tmp_path):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    root, diagnostic, raw = tmp_path / 'news', tmp_path / 'probe.json', tmp_path / 'probe.raw'
    queue = NewsQueue(root, registry)
    queue.close()
    atomic_write_bytes(raw, short_page())
    atomic_write_json(diagnostic, {'observed_at_utc': '2026-10-10T00:00:00+00:00', 'probes': [{
        'url': URL, 'status': 200, 'response_sha256': 'wrong-response', 'private_raw_path': str(raw)}]})
    with pytest.raises(ValueError, match='diagnostic_raw_response_digest_mismatch'):
        audit(root, registry, diagnostic)
