"""A finite response audit is bounded by its own observations, not the archive."""
import hashlib
import sqlite3

from scripts.audit_news_cnyes_retained_missing import audit
from test_cnyes_news_bulk import cfg, response  # noqa: F401
from test_news_cached_replay import _request_scoped_history_fixture


def test_selected_response_survives_more_than_100000_unrelated_observations(cfg, tmp_path, monkeypatch):
    registry, data, _, _, _, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    selected_sha = hashlib.sha256(response(data).body).hexdigest()
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as db:
        db.executemany('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
            ((f'noise-{number:06}', 1, '0' * 64, 1, 'a' * 64, 'b' * 64, f'{number:064x}', '{}')
             for number in range(100_001)))
    db.close()  # Checkpoint the isolated WAL before the immutable read.
    result = audit(tmp_path, registry, {}, only_responses=[selected_sha])
    assert result['candidate_source_envelopes'] == result['source_envelopes_verified'] == 2
    assert result['candidate_pairs'] == 2 and result['observations_scanned'] == 2
    assert result['observation_scan_scope'] == 'selected_responses'
    assert not result['response_failures'] and not result['unresolved_matching_observations']
    absent = audit(tmp_path, registry, {}, only_responses=['f' * 64])
    assert absent['selected_responses_without_candidate'] == ['f' * 64]
    assert absent.get('observations_scanned', 0) == 0
    mixed = audit(tmp_path, registry, {}, only_responses=['f' * 64, selected_sha])
    assert mixed['selected_responses_requested'] == 2
    assert mixed['observations_scanned'] == 2
    assert mixed['source_envelopes_verified'] == 2
    assert mixed['selected_responses_without_candidate'] == ['f' * 64]
