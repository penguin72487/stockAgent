import json

from scripts.run_tej_finite_discovery_wait import classify_owner_result


def test_only_proven_prequery_dataset_lock_timeout_is_retried():
    event = json.dumps({'event': 'dataset_lock', 'provider': 'tej_smart_wizard',
                        'state': 'timeout', 'reason': 'dataset_writer_lock_timeout'})
    traceback = 'downloader.dataset_lock.DatasetLockTimeout: owned dataset lock busy'
    assert classify_owner_result(1, event, traceback) == 'lock_busy'
    assert classify_owner_result(1, '', event + '\n' + traceback) == 'lock_busy'
    assert classify_owner_result(1, event, 'a different error') == 'error'
    assert classify_owner_result(1, '{}', traceback) == 'error'
    assert classify_owner_result(2, event, traceback) == 'error'
    assert classify_owner_result(75, '', '') == 'retry'
    assert classify_owner_result(0, '', '') == 'retry'
