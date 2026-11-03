"""Persistent dispatch must defer safely and require the original acceptance."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path

import pytest

from scripts import run_wsl_retirement_waves as original
from scripts import retire_local_offline_artifacts as owner
from stockagent.data_sync import retirement_queue as queue


class OriginalOwner:
    SchedulerBusy = original.SchedulerBusy
    _accepted = staticmethod(original._accepted)

    def __init__(self, root, config):
        self.root, self.config = root, config
        self.calls, self.inspections, self.outcomes = [], [], {}
        self.busy, self.waits, self.capacity = False, {}, {}

    @contextmanager
    def _scheduler_lock(self):
        if self.busy:
            raise self.SchedulerBusy('original scheduler busy')
        yield

    @contextmanager
    def _scheduler_read_lock(self):
        if self.busy:
            raise self.SchedulerBusy('original scheduler busy')
        yield

    def _inspect(self, pairs, parallelism, **options):
        self.inspections.append(pairs)
        cohorts = []
        for inventory, receipt in pairs:
            row = next(r for r in self.config['cohorts'] if self.root / r['inventory'] == inventory)
            source = self.root / 'artifacts/datasets' / row['id']
            cohorts.append({'inventory': str(inventory), 'receipt_directory': str(receipt),
                'inventory_sha256': hashlib.sha256(inventory.read_bytes()).hexdigest(),
                'fixed_d_publication': {'manifest_sha256': 'a' * 64} if source.exists() else None,
                'historical_progress': 'retired_and_source_absent' if not source.exists() else 'absent',
                'sources': [{'relative_root': 'datasets/' + row['id'], 'path': str(source),
                             'resolved_path': str(source)}],
                'preflight_deferrals': self.waits.get(row['id'], [])})
        return {'cohorts': cohorts, 'waves': [{'cohorts': list(range(len(pairs))),
            'capacity_deferrals': sorted({v for inv, _ in pairs for v in self.capacity.get(inv.name, [])})}],
            'overlap_conflicts': []}

    def execute(self, pairs, parallelism, *, expected_handoff_identity, **options):
        current = self._inspect(pairs, parallelism)
        assert expected_handoff_identity == [(c['inventory_sha256'], c['fixed_d_publication'])
                                             for c in current['cohorts']]
        durable = json.loads((self.root / self.config['state_root'] / 'status.json').read_text())
        for inventory, receipt in pairs:
            row = next(r for r in self.config['cohorts'] if self.root / r['inventory'] == inventory)
            assert durable['cohorts'][row['id']]['state'] == 'running'
            self.calls.append(row['id'])
            outcome = self.outcomes.get(row['id'], 'accepted')
            if outcome == 'crash':
                raise KeyboardInterrupt
            if outcome == 'scheduler_race':
                raise self.SchedulerBusy('another executor acquired the wave')
            if outcome == 'capacity_race':
                self.capacity[inventory.name] = ['windows_c']
                continue
            if outcome != 'accepted':
                continue
            source = self.root / 'artifacts/datasets' / row['id']
            source.unlink()
            item = {'relative_root': 'datasets/' + row['id'], 'dataset': row['id'],
                    'retired': True, 'snapshot_id': 'fixed-snapshot', 'manifest_sha256': 'a' * 64,
                    'reclaimed_allocated_bytes': 4096}
            (receipt / row['id']).mkdir(parents=True, exist_ok=True)
            (receipt / row['id'] / 'acceptance.json').write_text(json.dumps(item))
            (receipt / 'progress.json').write_text(json.dumps({
                'contract': owner.MANUAL_WSL_CAPTURE_CONTRACT,
                'inventory_sha256': row['inventory_sha256'], 'all_selected_retired': True,
                'items': [item], 'reclaimed_allocated_bytes': 4096}))
        # A successful process is deliberately insufficient by itself.
        return 0


@pytest.fixture
def enrolled(tmp_path):
    base = tmp_path / 'artifacts/operations/approved'
    base.mkdir(parents=True)
    (tmp_path / 'artifacts/datasets').mkdir()
    rows = []
    for name in ['one', 'two']:
        inventory = base / (name + '.json')
        inventory.write_text(json.dumps({'approved': name}))
        (tmp_path / 'artifacts/datasets' / name).write_text('exact fixture source')
        rows.append({'id': name, 'inventory': str(inventory.relative_to(tmp_path)),
            'inventory_sha256': hashlib.sha256(inventory.read_bytes()).hexdigest(),
            'receipt_directory': str((base / (name + '-receipts')).relative_to(tmp_path)),
            'resource_mode': 'fixed_d_stage_only'})
    config = {'schema_version': 1, 'contract': queue.CONTRACT, 'authority_node_id': 'penguin',
              'enabled': True, 'user_instruction': 'Automatically continue these frozen cohorts.',
              'state_root': 'service_data/storage-retirement', 'parallelism': 2,
              'maximum_error_attempts': 2, 'retry_delays_seconds': [60, 300], 'cohorts': rows}
    path = tmp_path / 'queue.json'
    path.write_text(json.dumps(config))
    runner = OriginalOwner(tmp_path, config)
    return tmp_path, path, config, runner


def turn(enrolled, *, apply=True):
    root, path, _, runner = enrolled
    return queue.cycle(path, root=root, runner=runner, apply=apply)


def test_backup_reserve_policy_is_explicit_and_reaches_every_original_preflight(enrolled, monkeypatch):
    root, path, config, runner = enrolled
    config.update(capacity_reserve_policy='use_backup_reserve',
                  backup_reserve_instruction='User earmarked this pool for backup.')
    path.write_text(json.dumps(config))
    seen = []
    inspect = runner._inspect
    execute = runner.execute
    def observe_inspect(*args, **kwargs):
        seen.append(('inspect', kwargs.copy()))
        return inspect(*args, **kwargs)
    def observe_execute(*args, **kwargs):
        seen.append(('execute', kwargs.copy()))
        return execute(*args, **kwargs)
    monkeypatch.setattr(runner, '_inspect', observe_inspect)
    monkeypatch.setattr(runner, 'execute', observe_execute)
    state = turn(enrolled)
    assert state['all_enrolled_accepted'] is True
    assert state['capacity_reserve_policy'] == 'use_backup_reserve'
    # The fake owner performs an additional internal inspection, without
    # options. All queue-originated calls must retain the maintenance policy.
    dispatched = [kw for kind, kw in seen if kind == 'execute']
    assert dispatched and all(kw['use_backup_reserve'] is True for kw in dispatched)
    assert sum(kw.get('use_backup_reserve') is True for kind, kw in seen if kind == 'inspect') >= 4
    del config['backup_reserve_instruction']
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match='purpose instruction'):
        queue.load_configuration(path, root)


def test_dry_plan_has_no_state_or_original_execution(enrolled):
    state = turn(enrolled, apply=False)
    assert not state['all_enrolled_accepted'] and state['state'] == 'planned'
    assert not (enrolled[0] / 'service_data').exists()
    assert not enrolled[3].calls


def test_busy_wave_skips_metadata_census_and_does_not_claim_acceptance(enrolled):
    enrolled[3].busy = True
    state = turn(enrolled)
    assert state['state'] == 'waiting_existing_wave'
    assert not state['all_enrolled_accepted'] and not enrolled[3].inspections
    assert set(state['cohorts']) == {'one', 'two'}


def test_busy_cycle_never_projects_previous_acceptance_as_a_fresh_full_check(enrolled):
    assert turn(enrolled)['all_enrolled_accepted']
    enrolled[3].busy = True
    state = turn(enrolled)
    assert state['state'] == 'waiting_existing_wave' and not state['all_enrolled_accepted']
    assert state['cohorts']['one']['acceptance']  # Preserve original evidence.


def test_accepted_proofs_can_be_rechecked_during_a_shared_read_census(enrolled, monkeypatch):
    assert turn(enrolled)['all_enrolled_accepted']
    runner = enrolled[3]
    def exclusive_would_block():
        raise AssertionError('read-only acceptance must not request the exclusive lease')
    monkeypatch.setattr(runner, '_scheduler_lock', exclusive_would_block)
    result = turn(enrolled)
    assert result['all_enrolled_accepted'] and runner.calls == ['one', 'two']


def test_changed_accepted_proof_does_not_use_shared_lease_to_run_retirement(enrolled):
    root, _, config, runner = enrolled
    assert turn(enrolled)['all_enrolled_accepted']
    source = root/'artifacts/datasets/one'
    source.write_text('reappeared')
    result = turn(enrolled)
    assert not result['all_enrolled_accepted'] and source.read_text() == 'reappeared'
    assert runner.calls == ['one', 'two']


def test_restart_rechecks_original_acceptance_without_resubmitting(enrolled):
    first = turn(enrolled)
    assert first['all_enrolled_accepted'] and enrolled[3].calls == ['one', 'two']
    second = turn(enrolled)
    assert second['all_enrolled_accepted'] and enrolled[3].calls == ['one', 'two']
    assert sum(r['acceptance']['reclaimed_allocated_bytes'] for r in second['cohorts'].values()) == 8192


def test_completed_pinned_cohort_no_longer_requires_its_retired_stage(enrolled):
    root, path, config, runner = enrolled
    config['cohorts'][0]['manifest_sha256'] = 'a' * 64
    path.write_text(json.dumps(config))
    assert turn(enrolled)['all_enrolled_accepted']
    result = turn(enrolled)
    assert result['all_enrolled_accepted'] and runner.calls == ['one', 'two']
    assert result['cohorts']['one']['acceptance']['manifest_sha256'] == 'a' * 64


def test_completed_cohort_still_requires_original_manifest_pin(enrolled):
    root, path, config, runner = enrolled
    config['cohorts'][0]['manifest_sha256'] = 'a' * 64
    path.write_text(json.dumps(config))
    assert turn(enrolled)['all_enrolled_accepted']
    receipt = root / config['cohorts'][0]['receipt_directory']
    progress = json.loads((receipt/'progress.json').read_text())
    progress['items'][0]['manifest_sha256'] = 'b' * 64
    (receipt/'progress.json').write_text(json.dumps(progress))
    (receipt/'one'/'acceptance.json').write_text(json.dumps(progress['items'][0]))
    result = turn(enrolled)
    assert result['cohorts']['one']['state'] == 'blocked_identity_or_proof'
    assert result['cohorts']['two']['state'] == 'accepted' and not result['all_enrolled_accepted']
    assert runner.calls == ['one', 'two']


def test_unfinished_cohort_requires_the_enrolled_existing_stage(enrolled):
    root, path, config, runner = enrolled
    config['cohorts'][0]['manifest_sha256'] = 'b' * 64
    path.write_text(json.dumps(config))
    result = turn(enrolled)
    assert result['cohorts']['one']['state'] == 'blocked_identity_or_proof'
    assert (root/'artifacts/datasets/one').read_text() == 'exact fixture source'
    assert runner.calls == ['two']


def test_reappeared_source_is_never_retired_using_old_completed_entry(enrolled):
    turn(enrolled)
    restored = enrolled[0] / 'artifacts/datasets/one'
    restored.write_text('new active materialization')
    for _ in range(2):
        state = turn(enrolled)
        assert state['cohorts']['one']['state'] == 'blocked_identity_or_proof'
        assert not state['all_enrolled_accepted'] and restored.read_text() == 'new active materialization'
    assert enrolled[3].calls == ['one', 'two']


@pytest.mark.parametrize('wait', ['owner', 'capacity', 'scheduler_race', 'capacity_race'])
def test_transient_waits_do_not_consume_failure_budget(enrolled, wait):
    runner = enrolled[3]
    if wait == 'owner':
        runner.waits['one'] = ['receipt_owner_busy']
    elif wait == 'capacity':
        runner.capacity['one.json'] = ['windows_c']
    else:
        runner.outcomes['one'] = wait
    state = turn(enrolled)
    assert state['cohorts']['one']['error_attempts'] == 0
    assert state['cohorts']['one']['state'].startswith('waiting_')


def test_exit_zero_without_acceptance_is_bounded_and_does_not_block_other_cohorts(enrolled, monkeypatch):
    epoch = [100.0]
    monkeypatch.setattr(queue.time, 'time', lambda: epoch[0])
    runner = enrolled[3]
    runner.outcomes['one'] = 'false_success'
    first = turn(enrolled)
    assert first['cohorts']['one']['state'] == 'waiting_retry'
    assert first['cohorts']['two']['state'] == 'accepted'
    assert not first['all_enrolled_accepted']
    turn(enrolled)
    assert runner.calls.count('one') == 1
    epoch[0] += 61
    second = turn(enrolled)
    assert second['cohorts']['one']['state'] == 'needs_attention'
    epoch[0] += 1000
    turn(enrolled)
    assert runner.calls.count('one') == 2
    assert len(list((enrolled[0] / enrolled[2]['state_root'] / 'failed-attempts').glob('*.json'))) == 2


def test_owner_fix_automatically_resumes_exhausted_work_without_resetting_old_evidence(enrolled, monkeypatch):
    epoch, generation = [100.0], ['owner-v1']
    monkeypatch.setattr(queue.time, 'time', lambda: epoch[0])
    monkeypatch.setattr(queue, '_execution_generation', lambda: generation[0])
    runner = enrolled[3]
    runner.outcomes['one'] = 'false_success'
    turn(enrolled)
    epoch[0] += 61
    assert turn(enrolled)['cohorts']['one']['state'] == 'needs_attention'
    generation[0] = 'owner-v2'
    runner.outcomes.clear()
    assert turn(enrolled)['all_enrolled_accepted']
    receipts = list((enrolled[0] / enrolled[2]['state_root'] / 'failed-attempts').glob('*.json'))
    assert len(receipts) == 2 and all(not json.loads(p.read_text())['acceptance_claimed'] for p in receipts)


def test_changed_inventory_does_not_dispatch_but_independent_cohort_continues(enrolled):
    root, _, config, runner = enrolled
    (root / config['cohorts'][0]['inventory']).write_text('changed bytes')
    result = turn(enrolled)
    assert result['cohorts']['one']['state'] == 'blocked_identity_or_proof'
    assert result['cohorts']['two']['state'] == 'accepted'
    assert runner.calls == ['two']


def test_interrupted_dispatch_has_durable_intent_and_resumes_with_original_owner(enrolled):
    runner = enrolled[3]
    runner.outcomes['one'] = 'crash'
    with pytest.raises(KeyboardInterrupt):
        turn(enrolled)
    prior = json.loads((enrolled[0] / enrolled[2]['state_root'] / 'status.json').read_text())
    assert prior['cohorts']['one']['state'] == 'running' and not prior['all_enrolled_accepted']
    runner.outcomes.clear()
    assert turn(enrolled)['all_enrolled_accepted']


@pytest.mark.parametrize('change', ['foreign_authority', 'external_inventory', 'overlap_receipts', 'state_symlink'])
def test_invalid_enrollment_never_dispatches(enrolled, change):
    root, path, config, runner = enrolled
    if change == 'foreign_authority':
        config['authority_node_id'] = 'vast'
    elif change == 'external_inventory':
        config['cohorts'][0]['inventory'] = '/tmp/unapproved.json'
    elif change == 'overlap_receipts':
        config['cohorts'][1]['receipt_directory'] = config['cohorts'][0]['receipt_directory'] + '/child'
    else:
        external = root / 'external'
        external.mkdir()
        (root / 'service_data').symlink_to(external, target_is_directory=True)
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        turn(enrolled)
    assert not runner.calls


def test_source_overlap_across_resource_modes_blocks_all_execution(enrolled, monkeypatch):
    runner = enrolled[3]
    inspect = runner._inspect
    def overlapping(*args, **kwargs):
        plan = inspect(*args, **kwargs)
        for cohort in plan['cohorts']:
            cohort['sources'][0]['resolved_path'] = str(enrolled[0] / 'artifacts/datasets/one')
        return plan
    monkeypatch.setattr(runner, '_inspect', overlapping)
    state = turn(enrolled)
    assert state['state'] == 'blocked_overlapping_sources'
    assert not state['all_enrolled_accepted'] and not runner.calls
