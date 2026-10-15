import json

from scripts.observe_finmind_protected_wait import counters, status_clock


def test_observer_keeps_only_operational_fields(tmp_path):
    (tmp_path / 'status.json').write_text(json.dumps({'state': 'protected_opening', 'account': 'secret',
        'observed_at_utc': 'cycle', 'last_task': {'rows': 2}, 'active_task': None}))
    (tmp_path / 'worker_status.json').write_text(json.dumps({'phase': 'waiting', 'observed_at_utc': 'heart',
        'reason': 'protected_opening', 'next_check_at_utc': 'deadline', 'token': 'secret'}))
    result = status_clock(tmp_path)
    assert 'secret' not in json.dumps(result)
    assert result['cycle_observed_at_utc'] == 'cycle'
    assert result['heartbeat_at_utc'] == 'heart'


def test_native_counter_reader_does_not_substitute_zeros(tmp_path):
    (tmp_path / 'cpu.stat').write_text('usage_usec 1234567\nuser_usec 12\n')
    (tmp_path / 'memory.current').write_text('4096\n')
    (tmp_path / 'memory.swap.current').write_text('5\n')
    assert counters(tmp_path) == {'cpu_usage_usec': 1234567, 'memory_current_bytes': 4096,
                                  'swap_current_bytes': 5}
