import pytest

from scripts import run_finlab_finite_priority_drain as finite_drain

from scripts.run_finlab_finite_priority_drain import drain, wait_for_headroom


def test_waiter_and_each_source_request_use_the_same_extra_headroom():
    from scripts.download_finlab_market_intraday import CAPACITY_DEFERRED_PRIORITY_HEADROOM_BYTES
    from scripts.run_finlab_finite_priority_drain import HEADROOM_BYTES
    assert HEADROOM_BYTES == CAPACITY_DEFERRED_PRIORITY_HEADROOM_BYTES


def _pass(attempted, successful=0, state='finite_priority_pass_complete', kind='tw_minute'):
    return {'contract': 'finlab_finite_intraday_priority_pass_v1',
            'selection_scope': 'finite_registered_priority', 'kind': kind,
            'state': state, 'attempted_this_run': attempted, 'successes_this_run': successful}


def test_drain_stops_when_the_existing_owner_has_no_more_actionable_work():
    responses = iter([(0, _pass(4, 1)), (0, _pass(2, 1))])
    code, result = drain(5, run_batch=lambda: next(responses), owner_digest=lambda: 'stable')
    assert code == 0
    assert (result['state'], result['passes'], result['attempted'], result['successful']) == (
        'no_more_actionable_in_pass', 2, 6, 2)
    assert result['full_history_completion_claim'] is False


def test_workspace_drain_accepts_only_a_confirmed_workspace_pass():
    code, result = drain(1, futures_workspace_only=True, run_batch=lambda: (0, _pass(0)),
                         owner_digest=lambda: 'stable')
    assert code == 1 and result['state'] == 'unexpected_child_result'
    response = {**_pass(0), 'futures_workspace_only': True}
    code, result = drain(1, futures_workspace_only=True, run_batch=lambda: (0, response),
                         owner_digest=lambda: 'stable')
    assert code == 0 and result['state'] == 'no_more_actionable_in_pass'
    with pytest.raises(ValueError):
        drain(1, kind='tw_tick', futures_workspace_only=True)


def test_drain_preserves_partial_attempts_and_yields_on_real_owner_defer():
    responses = iter([(0, _pass(4)), (75, _pass(1, state='quota_margin_reached'))])
    code, result = drain(5, run_batch=lambda: next(responses), owner_digest=lambda: 'stable')
    assert code == 75
    assert result['state'] == 'child_deferred'
    assert result['child_state'] == 'quota_margin_reached'
    assert result['attempted'] == 5


def test_drain_never_treats_busy_lock_as_a_completed_pass():
    code, result = drain(1, run_batch=lambda: (0, {'state': 'account_sync_active'}),
                         owner_digest=lambda: 'stable')
    assert code == 75
    assert result['state'] == 'account_busy'
    assert result['attempted'] == 0


def test_drain_retries_the_shared_account_lock_when_capacity_wait_is_enabled():
    responses = iter([(0, {'state': 'account_sync_active'}),
                      (0, _pass(0, kind='tw_tick'))])
    sleeps = []
    code, result = drain(2, kind='tw_tick', run_batch=lambda: next(responses),
                         owner_digest=lambda: 'stable', capacity_wait=lambda: True,
                         busy_sleep=sleeps.append)
    assert code == 0
    assert result['state'] == 'no_more_actionable_in_pass'
    assert result['busy_retries'] == 1 and result['passes'] == 1
    assert sleeps == [finite_drain.BUSY_RETRY_SECONDS]


def test_tick_drain_accepts_only_one_registered_partition_per_pass():
    code, result = drain(1, kind='tw_tick', run_batch=lambda: (0, _pass(2, kind='tw_tick')),
                         owner_digest=lambda: 'stable')
    assert code == 1
    assert result['state'] == 'unexpected_child_result'
    assert result['attempted'] == 0
    with pytest.raises(ValueError):
        drain(1, kind='other')


def test_drain_stops_before_using_changed_owner_code():
    fingerprints = iter(['initial', 'initial', 'changed'])
    code, result = drain(5, run_batch=lambda: (0, _pass(4)), owner_digest=lambda: next(fingerprints))
    assert code == 75
    assert result['state'] == 'owner_changed'
    assert result['passes'] == 1


def test_drain_stops_before_using_a_changed_capacity_guard():
    fingerprints = iter(['initial', 'initial', 'changed'])
    code, result = drain(5, run_batch=lambda: (0, _pass(4)), owner_digest=lambda: 'stable',
                         guard_digest=lambda: next(fingerprints))
    assert code == 75
    assert result['state'] == 'capacity_guard_changed'
    assert result['passes'] == 1


@pytest.mark.parametrize('summary', [_pass(True), _pass(5), {**_pass(4), 'kind': 'tw_tick'},
                                     {**_pass(4), 'state': 'unrecognized'}])
def test_drain_fails_closed_on_unverified_child_summary(summary):
    code, result = drain(2, run_batch=lambda: (0, summary), owner_digest=lambda: 'stable')
    assert code == 1
    assert result['state'] == 'unexpected_child_result'


def test_drain_limits_number_of_serial_passes():
    code, result = drain(2, run_batch=lambda: (0, _pass(4)), owner_digest=lambda: 'stable')
    assert code == 0
    assert result['state'] == 'pass_limit_reached'
    assert result['attempted'] == 8
    with pytest.raises(ValueError):
        drain(33)


def test_capacity_wait_retries_unknown_or_insufficient_space_without_dispatch():
    now = [0.0]
    values = iter([{'state': 'deferred', 'reason': 'physical_backing_reserve'},
                   {'state': 'deferred', 'reason': 'capacity_probe_failed'},
                   {'state': 'admitted'}])
    assert wait_for_headroom(180, sample=lambda: next(values), clock=lambda: now[0],
                             sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))
    assert now[0] == 120


def test_capacity_wait_times_out_without_admitting_unknown_capacity():
    now = [0.0]
    assert not wait_for_headroom(90, sample=lambda: {'state': 'not_enrolled'},
                                 clock=lambda: now[0],
                                 sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))


def test_capacity_wait_detects_superseded_owner_before_next_probe():
    now = [0.0]
    valid = [True]
    samples = []

    def sleep(seconds):
        now[0] += seconds
        valid[0] = False

    assert not wait_for_headroom(3600, sample=lambda: samples.append(True) or {'state': 'deferred'},
                                 clock=lambda: now[0], sleep=sleep,
                                 still_valid=lambda: valid[0])
    assert now[0] == 60 and len(samples) == 1


def test_capacity_wait_rechecks_owner_after_an_admitted_probe():
    valid = [True]

    def sample():
        valid[0] = False
        return {'state': 'admitted'}

    assert not wait_for_headroom(3600, sample=sample, clock=lambda: 0,
                                 still_valid=lambda: valid[0])


def test_cli_wait_duration_is_bounded_and_reaches_waiter(monkeypatch):
    monkeypatch.setattr(finite_drain.time, 'monotonic', lambda: 100.0)
    observed = []
    monkeypatch.setattr(finite_drain, 'wait_for_headroom', lambda deadline: observed.append(deadline) or True)
    def execute(_passes, *, kind, capacity_wait):
        assert kind == 'tw_minute'
        assert capacity_wait()
        return 0, {'state': 'verified'}
    monkeypatch.setattr(finite_drain, 'drain', execute)
    assert finite_drain.main(['--wait-for-capacity', '--capacity-wait-hours', '156']) == 0
    assert observed == [100.0 + 156 * 3600]
    with pytest.raises(SystemExit):
        finite_drain.main(['--wait-for-capacity', '--capacity-wait-hours', '169'])


def test_drain_waits_after_capacity_race_but_preserves_partial_attempts():
    calls = []
    responses = iter([(75, _pass(1, state='capacity_deferred')), (0, _pass(2))])
    code, result = drain(3, run_batch=lambda: next(responses), owner_digest=lambda: 'stable',
                         capacity_wait=lambda: calls.append('wait') or True)
    assert code == 0
    assert calls == ['wait', 'wait']
    assert result['attempted'] == 3
    assert result['state'] == 'no_more_actionable_in_pass'


def test_drain_retries_a_measured_prelock_capacity_race():
    calls = []
    responses = iter([(75, {'contract': 'acquisition_dispatch_capacity_v1', 'state': 'deferred',
                            'reason': 'physical_backing_reserve'}), (0, _pass(0, kind='tw_tick'))])
    code, result = drain(3, kind='tw_tick', run_batch=lambda: next(responses), owner_digest=lambda: 'stable',
                         capacity_wait=lambda: calls.append('wait') or True)
    assert code == 0
    assert calls == ['wait', 'wait']
    assert result['passes'] == 2 and result['attempted'] == 0
    assert result['state'] == 'no_more_actionable_in_pass'


def test_drain_rechecks_a_transient_unconfirmed_priority_backing_probe():
    waits, sleeps = [], []
    response = {'contract': 'acquisition_dispatch_capacity_v1', 'state': 'deferred',
                'reason': 'physical_backing_headroom_unconfirmed', 'retryable': True,
                'allocation_admitted': False, 'error_type': 'SnapshotError'}
    responses = iter([(75, response), (0, _pass(0))])
    code, result = drain(1, run_batch=lambda: next(responses), owner_digest=lambda: 'stable',
                         capacity_wait=lambda: waits.append('wait') or True, busy_sleep=sleeps.append)
    assert code == 0 and result['state'] == 'no_more_actionable_in_pass'
    assert result['passes'] == 1 and result['attempted'] == 0
    assert result['admission_waits'] == 1
    assert waits == ['wait', 'wait'] and sleeps == [finite_drain.ADMISSION_RETRY_SECONDS]


@pytest.mark.parametrize('overrides', [{'retryable': False}, {'allocation_admitted': True}])
def test_drain_does_not_retry_malformed_or_nonretryable_backing_probe(overrides):
    response = {'contract': 'acquisition_dispatch_capacity_v1', 'state': 'deferred',
                'reason': 'physical_backing_headroom_unconfirmed', 'retryable': True,
                'allocation_admitted': False} | overrides
    code, result = drain(1, run_batch=lambda: (75, response), owner_digest=lambda: 'stable',
                         capacity_wait=lambda: True, busy_sleep=lambda _: pytest.fail('must stop'))
    assert code == 75 and result['state'] == 'child_deferred'


def test_drain_does_not_retry_unknown_prelock_deferrals():
    response = {'contract': 'acquisition_dispatch_capacity_v1', 'state': 'deferred',
                'reason': 'priority_backing_unverified'}
    code, result = drain(3, run_batch=lambda: (75, response), owner_digest=lambda: 'stable',
                         capacity_wait=lambda: True)
    assert code == 75
    assert result['passes'] == 1 and result['attempted'] == 0
    assert result['state'] == 'child_deferred'


@pytest.mark.parametrize('waiting', ['waiting_required_general_data', 'waiting_required_minutes',
                                    'opening_window_yield'])
def test_drain_rechecks_owner_admission_without_losing_partial_attempts(waiting):
    responses = iter([(75, _pass(1, state=waiting)), (0, _pass(2))])
    sleeps = []
    code, result = drain(1, run_batch=lambda: next(responses), owner_digest=lambda: 'stable',
                         capacity_wait=lambda: True, busy_sleep=sleeps.append)
    assert code == 0 and result['state'] == 'no_more_actionable_in_pass'
    assert result['attempted'] == 3 and result['passes'] == 1
    assert result['admission_waits'] == 1
    assert sleeps == [finite_drain.ADMISSION_RETRY_SECONDS]


def test_drain_admission_wait_remains_bounded_by_capacity_deadline():
    waits = iter([True, False])
    sleeps = []
    code, result = drain(1, run_batch=lambda: (75, _pass(0, state='waiting_required_general_data')),
                         owner_digest=lambda: 'stable', capacity_wait=lambda: next(waits),
                         busy_sleep=sleeps.append)
    assert code == 75 and result['state'] == 'capacity_wait_timeout'
    assert result['passes'] == 0 and result['admission_waits'] == 1
    assert sleeps == [finite_drain.ADMISSION_RETRY_SECONDS]
