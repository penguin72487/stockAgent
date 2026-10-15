"""An apparent duplicate is a candidate, never automatic body acceptance."""
import pytest
import requests

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_cnyes_alias_candidates import audit
from test_news_fulltext import config


@pytest.fixture
def queue(config, tmp_path):
    config['providers'][0].update(id='cnyes', domains=['m.cnyes.com', 'news.cnyes.com'])
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        yield q
    finally:
        q.close()


def canonical_candidate(queue, story_id, *, state='complete', has_body=True):
    url = 'https://news.cnyes.com/news/id/' + str(story_id)
    queue.add_articles([{'url': url, 'title': 'original'}])
    # Metadata-only candidate fixture intentionally has NO receipt or file.
    queue.db.execute('UPDATE articles SET state=?,body_sha256=? WHERE url=?',
                     (state, 'a' * 64 if has_body else None, url))
    queue.db.commit()


def test_alias_inventory_is_bounded_readonly_and_never_proves_body_equivalence(queue, monkeypatch):
    monkeypatch.setattr(requests.Session, 'request', lambda *a, **k: pytest.fail('no publisher HTTP allowed'))
    canonical_candidate(queue, 1)
    queue.add_articles([{'url': 'https://m.cnyes.com/news/id/1?exp=a', 'title': 'original'},
                        {'url': 'https://m.cnyes.com/news/id/1?exp=b', 'title': 'different'},
                        {'url': 'https://m.cnyes.com/news/id/2', 'title': 'other'}])
    queue.db.commit()
    before = queue.db.total_changes
    result = audit(queue.root, 2)
    assert result['sampled_pending_mobile_heads'] == 2 and result['sample_limit_reached']
    assert result['queue_owner_id'] == queue.owner and queue.db.total_changes == before
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['alias_source_equivalence_verified'] and not result['canonical_receipts_verified']
    assert not result['historical_completeness_verified']
    assert result['counts']['existing_canonical_body_candidate'] == 2
    assert result['counts']['supported_current_mobile_view'] == 2
    assert result['counts'].get('unverified_query_view', 0) == 0
    assert result['counts']['exact_metadata_title_match'] == result['counts']['different_metadata_titles'] == 1
    assert any('host_state' in line for line in result['query_plan'])
    assert 'exp=b' not in str(result)  # No query values, credentials or bodies in the report.
    assert queue.db.execute("SELECT count(*) FROM articles WHERE host='m.cnyes.com' AND state='pending'").fetchone()[0] == 3


@pytest.mark.parametrize('state,has_body', [('complete', False), ('retry', True), ('source_access_denied', True)])
def test_incomplete_or_refused_canonical_head_is_not_a_candidate(queue, state, has_body):
    canonical_candidate(queue, 1, state=state, has_body=has_body)
    queue.add_articles([{'url': 'https://m.cnyes.com/news/id/1'}])
    queue.db.commit()
    result = audit(queue.root)
    assert result['counts']['no_complete_canonical_body_candidate'] == 1
    assert not result['candidates'][0]['existing_canonical_body_candidate']


@pytest.mark.parametrize('bound', [0, 4097, True, 1.5])
def test_invalid_sample_bounds_are_rejected_without_opening_the_database(tmp_path, bound):
    with pytest.raises(ValueError, match='bounded_integer_sample_required'):
        audit(tmp_path, bound)
    assert not (tmp_path / 'queue.sqlite3').exists()
