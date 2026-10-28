"""CPU consumption must remain separate from elapsed and synchronization time."""
from pathlib import Path

import pytest

from scripts import profile_workflow_speeds as profile


def capture(payload):
    ledger = profile.Ledger()
    profile.collect_benchmark(ledger, payload, 'original-receipt-sha', Path('bench/full.json'))
    return ledger


def test_direct_consumption_and_elapsed_keep_original_values_and_scopes():
    ledger = capture(dict(scope='complete collect and fresh reader', trials=[dict(
        mode='candidate', rows=7, wall_seconds=1.25, process_seconds=1.5,
        descendant_cpu_seconds=0.875, process_cpu_seconds=-0.0,
        child_cpu_user_seconds=0.625, child_cpu_system_seconds=0.25)]))
    actual = {row['stage']: row for row in ledger.samples}
    assert actual['wall_seconds']['metric'] == actual['process_seconds']['metric'] == 'wall_seconds'
    assert actual['wall_seconds']['seconds'] == 1.25 and actual['process_seconds']['seconds'] == 1.5
    for name, expected in [('descendant_cpu_seconds', .875), ('process_cpu_seconds', -0.0),
                           ('child_cpu_user_seconds', .625), ('child_cpu_system_seconds', .25)]:
        row = actual[name]
        assert row['metric'] == 'cpu_seconds' and row['seconds'].hex() == expected.hex()
        assert row['locator'] == '/trials/0/' + name
        assert row['source_sha256'] == 'original-receipt-sha'
        assert row['context']['scope'] == 'complete collect and fresh reader'
        assert row['context']['variant'] == 'candidate'
    assert profile.bind_source_stages(ledger) == 0


@pytest.mark.parametrize('container', ['phases', 'writes'])
def test_nested_consumption_keeps_cpu_and_cpu_sync_wait_keeps_elapsed(container):
    ledger = capture(dict(samples=[{container: [dict(stage='reconstruct', file='state.json',
        cpu_seconds=.5, process_cpu_seconds=.375, reaped_child_cpu_seconds=.125,
        wall_seconds=2.0, cpu_sync_seconds=1.5, elapsed_ms=250)]}]))
    by_locator = {row['locator']: row for row in ledger.samples}
    prefix = '/samples/0/' + container + '/0/'
    assert len(by_locator) == 6
    for name in ['cpu_seconds', 'process_cpu_seconds', 'reaped_child_cpu_seconds']:
        assert by_locator[prefix + name]['metric'] == 'cpu_seconds'
    for name, expected in [('wall_seconds', 2.0), ('cpu_sync_seconds', 1.5), ('elapsed_ms', .25)]:
        row = by_locator[prefix + name]
        assert row['metric'] == 'wall_seconds' and row['seconds'] == expected
    assert len(ledger.summaries()) == 6


@pytest.mark.parametrize('invalid', [None, True, -1.0, float('nan'), float('inf'), '0.5'])
@pytest.mark.parametrize('nested', [False, True])
def test_invalid_consumption_is_not_a_zero_or_an_elapsed_sample(invalid, nested):
    row = dict(wall_seconds=2.0, descendant_cpu_seconds=invalid)
    if nested:
        row = dict(phases=[dict(stage='native', **row)])
    ledger = capture(dict(trials=[row]))
    assert len(ledger.samples) == 1
    assert ledger.samples[0]['metric'] == 'wall_seconds' and ledger.samples[0]['seconds'] == 2.0


def test_alias_containers_and_failed_outcomes_keep_distinct_raw_intervals():
    payload = {name: [dict(mode='failed_attempt', wall_seconds=2.0,
        descendant_cpu_seconds=.75)] for name in ['trials', 'samples', 'results']}
    ledger = capture(payload)
    assert len(ledger.samples) == 6
    assert {row['locator'] for row in ledger.samples if row['metric'] == 'cpu_seconds'} == {
        '/' + name + '/0/descendant_cpu_seconds' for name in payload}
    assert all(row['context']['variant'] == 'failed_attempt' for row in ledger.samples)
