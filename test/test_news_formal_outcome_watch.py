"""Normal nonbody observations cannot become synthetic full-article progress."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from scripts import watch_news_formal_outcomes as watch

URL = 'https://news.cnyes.com/news/id/42'
OUTCOME = 'source_body_image_only'
STARTED = 1388534400


def row(**changes):
    return {'url': URL, 'state': OUTCOME, 'error_code': OUTCOME, 'body_sha256': None,
        'lease_owner': None, 'lease_until': None, 'due': 0,
        'last_checked_at_utc': '2014-01-01T00:00:01+00:00', **changes}


@pytest.mark.parametrize('outcome', sorted(watch.NONBODY_OUTCOMES))
def test_every_explicit_nonbody_outcome_can_be_verified_not_called_full_article(outcome):
    expected = watch.expected_outcomes({URL: outcome})
    assert watch.ready({URL: row(state=outcome, error_code=outcome)}, expected, STARTED)


@pytest.mark.parametrize('changes', [
    {'state': 'complete'}, {'state': 'body_not_found'}, {'state': 'inflight'},
    {'error_code': 'body_not_found'}, {'body_sha256': 'a' * 64},
    {'lease_owner': 'another-owner'}, {'lease_owner': ''}, {'lease_until': 0},
    {'due': 1}, {'due': False}, {'due': float('nan')},
    {'last_checked_at_utc': '2014-01-01T00:00:01'},
    {'last_checked_at_utc': '2013-12-31T23:59:59+00:00'},
    {'last_checked_at_utc': None}, {'last_checked_at_utc': 'invalid'},
    {'url': URL.replace('/42', '/43')},
])
def test_old_wrong_leased_or_body_rows_not_counted(changes):
    assert not watch.ready({URL: row(**changes)}, {URL: OUTCOME}, STARTED)


@pytest.mark.parametrize('value', ['2014-01-01T00:00:01+00:00',
    '2014-01-01T08:00:01+08:00', '2014-01-01T00:00:00+00:00'])
def test_aware_clocks_match_owner_epoch_without_rewriting_source_clock(value):
    sample = row(last_checked_at_utc=value)
    before = deepcopy(sample)
    assert watch.ready({URL: sample}, {URL: OUTCOME}, STARTED)
    assert sample == before


@pytest.mark.parametrize('sample', [None, {}, [], 'body-not-found'])
def test_missing_or_malformed_sample_stays_unverified(sample):
    assert not watch.ready({URL: sample}, {URL: OUTCOME}, STARTED)


@pytest.mark.parametrize('field', ['state', 'error_code', 'body_sha256',
    'lease_owner', 'lease_until', 'due', 'last_checked_at_utc'])
def test_missing_contract_field_not_accepted(field):
    sample = row()
    del sample[field]
    assert not watch.ready({URL: sample}, {URL: OUTCOME}, STARTED)


def test_membership_and_outcome_bindings_not_inferred():
    expected = {URL: OUTCOME}
    assert not watch.ready({}, expected, STARTED)
    assert not watch.ready({URL: row(), URL + '0': row()}, expected, STARTED)
    assert not watch.ready({URL: row()}, {URL: 'source_body_quote_only'}, STARTED)


@pytest.mark.parametrize('payload', [None, [], {}, {URL: 'complete'}, {URL: 'body_not_found'},
    {URL: None}, {URL: True}, {URL: ['source_body_image_only']},
    {'https://news.cnyes.com/news/id/42#fragment': OUTCOME},
    {f'https://news.cnyes.com/news/id/{n}': OUTCOME for n in range(17)}])
def test_manifest_requires_normalized_bounded_nonbody_heads(payload):
    with pytest.raises(ValueError):
        watch.expected_outcomes(payload)


def args(tmp_path):
    return ['--input', str(tmp_path / 'input.json'), '--runtime-baseline', str(tmp_path / 'baseline.json'),
        '--output', str(tmp_path / 'out.json')]


def test_existing_intent_does_not_duplicate_or_construct_downloader(tmp_path, monkeypatch):
    (tmp_path / 'out.intent.json').write_text('{}')
    monkeypatch.setattr(watch, 'load_config', lambda *_: pytest.fail('duplicate observer loaded source config'))
    with pytest.raises(ValueError, match='existing_watch_intent'):
        watch.main(args(tmp_path))


@pytest.mark.parametrize('extra', [['--maximum-seconds', '0'], ['--maximum-seconds', 'nan'],
    ['--maximum-seconds', '43201'], ['--poll-seconds', '0'], ['--poll-seconds', '61']])
def test_unbounded_observation_rejected_before_owner_read_or_transport(tmp_path, monkeypatch, extra):
    monkeypatch.setattr(watch, 'load_config', lambda *_: pytest.fail('invalid observer loaded source config'))
    with pytest.raises(SystemExit) as caught:
        watch.main(args(tmp_path) + extra)
    assert caught.value.code == 2
