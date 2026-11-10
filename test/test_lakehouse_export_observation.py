from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scripts import observe_lakehouse_export as observer


class Timestamp:
    def ToDatetime(self, *, tzinfo):
        return datetime(2026, 11, 5, 0, 36, tzinfo=tzinfo)


class Pending:
    activity_type = SimpleNamespace(name='export_version')
    activity_id = '30'
    state = 2
    attempt = 32
    last_failure = SimpleNamespace(message='Native batch failed (native-command-rejected, /private/path)')
    last_attempt_complete_time = Timestamp()
    last_started_time = Timestamp()

    def HasField(self, name):
        return name in {'last_failure', 'last_attempt_complete_time', 'last_started_time'}


def test_project_preserves_attempt_and_failure_time_without_private_error():
    result = observer.project({'state': 'exporting_version', 'snapshot_id': 1061}, [Pending()],
                              now='2026-11-05T00:40:00+00:00', run_id='fixed-run')
    assert result['read_only'] and result['activity'] == {
        'id': '30', 'state': 'started', 'attempt': 32, 'failure': 'native_command_rejected',
        'failure_at_utc': '2026-11-05T00:36:00+00:00', 'started_at_utc': '2026-11-05T00:36:00+00:00'}
    assert '/private/path' not in str(result)


def test_absent_export_never_becomes_a_success_receipt():
    result = observer.project({'state': 'registering'}, [], now='2026-11-05T00:40:00+00:00', run_id='fixed-run')
    assert result['phase'] == 'other' and result['activity'] is None
    assert 'success' not in str(result)


def test_ambiguous_or_invalid_activity_is_rejected():
    with pytest.raises(ValueError, match='more than one'):
        observer.project({}, [Pending(), Pending()], now='2026-11-05T00:40:00+00:00', run_id='fixed-run')
    pending = Pending()
    pending.state = 99
    with pytest.raises(ValueError, match='identity'):
        observer.project({}, [pending], now='2026-11-05T00:40:00+00:00', run_id='fixed-run')
