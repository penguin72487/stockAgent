"""Selected URL-shape inventory remains bounded, private and read-only."""
import json
import sqlite3

import pytest

from scripts.audit_news_route_shapes import audit, main


@pytest.fixture
def database(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE articles(url TEXT,provider TEXT,state TEXT,metadata_date_raw TEXT,body_sha256 TEXT)')
    db.execute('CREATE INDEX articles_provider ON articles(provider)')
    rows = [
        (f'https://example.test/livenews/{i}?secret=private', 'selected', 'pending',
         f'2021-01-0{i + 1} 10:00:00', None)
        for i in range(5)
    ]
    rows += [
        ('https://example.test/private-slug/1', 'selected', 'complete', 'not-a-date', 'a' * 64),
        ('https://example.test/2345#private', 'selected', 'pending', None, None),
        ('https://other.test/notselected', 'other', 'pending', '2000-01-01', None),
    ]
    db.executemany('INSERT INTO articles VALUES (?,?,?,?,?)', rows)
    db.commit()
    db.close()
    return path


def test_selected_shapes_retain_head_states_and_do_not_export_urls(database):
    before = database.read_bytes()
    result = audit(database, 'selected', max_urls=7)
    assert result['known_url_heads'] == 7
    assert result['stored_body_heads'] == 1
    assert result['states'] == {'complete': 1, 'pending': 6}
    assert result['scan_complete_for_provider_snapshot']
    assert 'selection' not in result and 'scan_complete_for_selection' not in result
    assert result['first_path_segment_counts'] == {'<other>': 1, '<rare>': 1, 'livenews': 5}
    assert result['with_query_string'] == 5 and result['with_fragment'] == 1
    assert result['metadata_date']['year_counts'] == {'2021': 5}
    assert result['metadata_date']['earliest_day'] == '2021-01-01'
    assert result['metadata_date']['latest_day'] == '2021-01-05'
    assert result['metadata_date']['absent'] == result['metadata_date']['invalid'] == 1
    assert not result['publisher_requests'] and not result['production_queue_writes']
    assert not result['article_identity_verified'] and not result['all_publisher_history_complete']
    assert 'private' not in json.dumps(result) and database.read_bytes() == before


def test_limit_exceeded_fails_before_writing_any_output(database, tmp_path):
    out = tmp_path / 'not-created.json'
    with pytest.raises(ValueError, match='exceeds scan limit'):
        main(['--database', str(database), '--provider', 'selected', '--max-urls', '6', '--output', str(out)])
    assert not out.exists()


def test_state_selection_is_complete_only_for_the_selection_and_keeps_body_state(database):
    before = database.read_bytes()
    result = audit(database, 'selected', max_urls=1, state='complete')
    assert result['known_url_heads'] == result['stored_body_heads'] == 1
    assert result['states'] == {'complete': 1}
    assert result['selection'] == {'state': 'complete'}
    assert result['scan_complete_for_selection']
    assert not result['scan_complete_for_provider_snapshot']
    assert not result['publisher_requests'] and not result['production_queue_writes']
    assert database.read_bytes() == before


def test_empty_provider_and_empty_state_keep_their_requested_scope(database):
    provider = audit(database, 'missing')
    assert provider['known_url_heads'] == 0
    assert provider['scan_complete_for_provider_snapshot']
    assert 'selection' not in provider and 'scan_complete_for_selection' not in provider
    selected = audit(database, 'selected', state='unsupported_article_path')
    assert selected['known_url_heads'] == 0
    assert selected['selection'] == {'state': 'unsupported_article_path'}
    assert selected['scan_complete_for_selection']
    assert not selected['scan_complete_for_provider_snapshot']


def test_state_selection_enforces_limit_before_output(database, tmp_path):
    out = tmp_path / 'partial-state.json'
    with pytest.raises(ValueError, match='exceeds scan limit'):
        main(['--database', str(database), '--provider', 'selected', '--state', 'pending',
              '--max-urls', '5', '--output', str(out)])
    assert not out.exists()


@pytest.mark.parametrize('state', ['', ' pending', 'pending\n', "x' OR 1=1", 'x' * 65])
def test_invalid_state_is_rejected(database, state):
    with pytest.raises(ValueError, match='invalid state'):
        audit(database, 'selected', state=state)


@pytest.mark.parametrize('provider', ['', ' selected', 'selected\n', 'x' * 301])
def test_invalid_provider_is_rejected(database, provider):
    with pytest.raises(ValueError, match='invalid provider'):
        audit(database, provider)


@pytest.mark.parametrize('limit', [0, -1, True, 1_000_001])
def test_invalid_limits_are_rejected(database, limit):
    with pytest.raises(ValueError, match='invalid URL scan limit'):
        audit(database, 'selected', max_urls=limit)


def test_turnnews_followup_keeps_prior_policy_clock_and_literal_target_grains():
    from pathlib import Path
    from stockagent.data.news_source_catalog import load_source_review_ledger, recorded_review_questions

    review = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))[
        'unregistered:turnnewsapp.com']
    scope = review['current_scope_review']
    assert review['capture_decision'] == 'review_incomplete'
    assert review['reviewed_at_utc'] == '2026-10-20T15:19:58.796031+00:00'
    assert scope['recorded_at_utc'] == '2026-10-27T15:18:12.113121+00:00'
    assert sum(scope['route_counts'].values()) == scope['known_URL_heads'] == 2528
    assert scope['stored_body_heads'] == 0
    assert scope['literal_targets_already_queued'] + scope['literal_targets_not_queued'] == scope['distinct_literal_redirect_targets']
    assert scope['literal_redirect_associations'] > scope['distinct_literal_redirect_targets']
    assert not scope['metadata_date_is_publisher_publication_time']
    assert not scope['article_identity_verified'] and not scope['source_authorization_granted']
    assert not scope['article_HTTP_requests_by_review'] and not scope['production_queue_mutations_by_review']
    completed, remaining = recorded_review_questions(review)
    assert completed and remaining
