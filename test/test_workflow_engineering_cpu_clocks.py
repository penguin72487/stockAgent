"""Only canonical reaped-child resource deltas may become engineering CPU time."""
import hashlib
import json
from pathlib import Path
import socket

import pytest

from scripts import profile_workflow_speeds as profile


def observe(tmp_path, monkeypatch, *, performance, state='succeeded', foreign=False):
    logical = tmp_path / 'repo'
    logical.mkdir()
    monkeypatch.setattr(profile, 'ROOT', logical)
    run = logical / 'runs' / 'original-command'
    run.mkdir(parents=True)
    payload = dict(run_id=run.name, name='complete-fixture', task_id='fixture-task',
        host='different-host' if foreign else socket.gethostname(), repo_root=str(logical),
        state=state, exit_code=0 if state == 'succeeded' else 1,
        started_at_utc='2026-10-25T00:00:00+00:00', finished_at_utc='2026-10-25T00:00:03+00:00',
        workflow_code_sha256='workflow-source-sha', performance=performance,
        argv=['private arguments must not enter projection'], logs='not public timing evidence')
    receipt = run / 'run.json'
    receipt.write_text(json.dumps(payload))
    output = tmp_path / 'observation'
    output.mkdir()
    evidence = profile.Evidence(output, max_file_bytes=1 << 20, max_total_bytes=2 << 20)
    ledger = profile.Ledger()
    try:
        profile.collect_engineering_runs(ledger, evidence, run.parent, limit=1)
    finally:
        evidence.close_snapshot_store()
    return ledger, receipt, json.loads((output / 'engineering_timing_snapshot.json').read_bytes())


def native():
    return dict(contract='waited_command_monotonic_resources_v1', wall_seconds=.5,
        child_cpu_user_seconds=.625, child_cpu_system_seconds=.375,
        scope='child launch through wait; excludes queue, final receipt and detached workers')


@pytest.mark.parametrize('state', ['succeeded', 'failed', 'cancelled'])
def test_exact_cpu_components_can_exceed_wall_and_keep_outcomes(tmp_path, monkeypatch, state):
    ledger, receipt, snapshot = observe(tmp_path, monkeypatch, performance=native(), state=state)
    assert len(ledger.samples) == 2
    wall, cpu = ledger.samples
    assert wall['metric'] == 'wall_seconds' and wall['stage'] == 'complete_command'
    assert wall['seconds'].hex() == .5.hex() and wall['locator'] == 'original-command'
    assert cpu['metric'] == 'cpu_seconds' and cpu['stage'] == 'complete_command_reaped_children'
    assert cpu['seconds'].hex() == (1.0).hex() and cpu['outcome'] == wall['outcome'] == state
    assert cpu['source_sha256'] == wall['source_sha256']
    assert cpu['context']['native_cpu_user_seconds'].hex() == .625.hex()
    assert cpu['context']['native_cpu_system_seconds'].hex() == .375.hex()
    assert cpu['context']['original_receipt_sha256'] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert cpu['locator'] == 'original-command/performance'
    assert profile.bind_source_stages(ledger) == 0
    assert 'argv' not in snapshot['rows'][0] and 'logs' not in snapshot['rows'][0]


@pytest.mark.parametrize('field', ['child_cpu_user_seconds', 'child_cpu_system_seconds'])
@pytest.mark.parametrize('invalid', [None, True, -1.0, float('nan'), float('inf'), '0.5'])
def test_missing_or_invalid_component_does_not_fabricate_cpu(tmp_path, monkeypatch, field, invalid):
    performance = native()
    performance[field] = invalid
    ledger, _, _ = observe(tmp_path, monkeypatch, performance=performance)
    assert len(ledger.samples) == 1 and ledger.samples[0]['metric'] == 'wall_seconds'


@pytest.mark.parametrize('contract', [None, 'unrecognized', 'legacy_wall_clock'])
def test_legacy_or_unknown_clock_has_no_cpu_projection(tmp_path, monkeypatch, contract):
    performance = native()
    performance['contract'] = contract
    ledger, _, _ = observe(tmp_path, monkeypatch, performance=performance)
    assert len(ledger.samples) == 1
    assert ledger.samples[0]['seconds'] == 3.0
    assert ledger.samples[0]['context']['clock_basis'] == 'legacy_wall_clock_estimate_not_monotonic'


def test_missing_monotonic_wall_cannot_attach_cpu_to_a_legacy_interval(tmp_path, monkeypatch):
    performance = native()
    del performance['wall_seconds']
    ledger, _, _ = observe(tmp_path, monkeypatch, performance=performance)
    assert len(ledger.samples) == 1 and ledger.samples[0]['seconds'] == 3.0


def test_zero_cpu_is_a_real_resource_observation(tmp_path, monkeypatch):
    performance = native()
    performance.update(child_cpu_user_seconds=0.0, child_cpu_system_seconds=-0.0)
    ledger, _, _ = observe(tmp_path, monkeypatch, performance=performance)
    assert len(ledger.samples) == 2 and ledger.samples[1]['seconds'] == 0.0
    assert ledger.samples[1]['context']['native_cpu_system_seconds'].hex() == (-0.0).hex()


def test_finite_components_with_overflow_do_not_create_an_infinite_sample(tmp_path, monkeypatch):
    performance = native()
    performance.update(child_cpu_user_seconds=1e308, child_cpu_system_seconds=1e308)
    ledger, _, _ = observe(tmp_path, monkeypatch, performance=performance)
    assert len(ledger.samples) == 1


@pytest.mark.parametrize('state,foreign', [('running', False), ('succeeded', True)])
def test_unfinished_or_foreign_runs_remain_excluded(tmp_path, monkeypatch, state, foreign):
    ledger, _, snapshot = observe(tmp_path, monkeypatch, performance=native(), state=state, foreign=foreign)
    assert not ledger.samples and not snapshot['rows']
