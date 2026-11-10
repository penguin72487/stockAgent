"""Rsync progress is bounded telemetry, never a copy or acceptance proof."""
import json
import sqlite3

from stockagent.data_sync import workspace_seed_observation as seed


def test_parse_only_matching_rsync_file_list_and_numeric_payload():
    message = '\r304,049,431,056  84%   52.62MB/s    1:31:50 (xfr#3046827, to-chk=494546/3542736)'
    expected = {'entries_checked': 3048190, 'entries_total': 3542736,
                'transferred_bytes': 304049431056}
    assert seed.parse_report(message, entries=3542735, logical_bytes=357963014044) == expected
    assert seed.parse_report(list(message.encode()), entries=3542735, logical_bytes=357963014044) == expected
    assert seed.parse_report(message, entries=3542734, logical_bytes=357963014044) is None
    assert seed.parse_report(message, entries=3542735, logical_bytes=100) is None
    assert seed.parse_report('filename\n' + message + 'x' * 5000, entries=3542735, logical_bytes=357963014044) is None


def test_sample_requires_same_source_generation_live_owner_and_fresh_clock(tmp_path, monkeypatch):
    value = {'contract': seed.CONTRACT, 'name': 'example', 'intent_started_at_epoch': 1000,
             'owner_pid': 1, 'owner_start_ticks': '10', 'client_pid': 2, 'client_start_ticks': '20',
             'observed_at_epoch': 1010, 'entries_checked': 4, 'entries_total': 11,
             'transferred_bytes': 200}
    (tmp_path / seed.RECEIPT).write_text(json.dumps(value))
    monkeypatch.setattr(seed, 'owner_for', lambda name: 1)
    monkeypatch.setattr(seed, 'process_identity', lambda pid: (0, '10') if pid == 1 else (1, '20'))
    intent = {'entries': 10, 'logical_bytes': 1000, 'started_at_epoch': 1000}
    assert seed.bound_sample('example', tmp_path, intent, now=1020) == value
    assert seed.bound_sample('other', tmp_path, intent, now=1020) is None
    assert seed.bound_sample('example', tmp_path, {**intent, 'started_at_epoch': 1001}, now=1020) is None
    assert seed.bound_sample('example', tmp_path, intent, now=1400) is None
    monkeypatch.setattr(seed, 'process_identity', lambda pid: (0, '10') if pid == 1 else (1, 'other'))
    assert seed.bound_sample('example', tmp_path, intent, now=1020) is None


def test_journal_requires_matching_unit_pid_and_original_observation_clock(monkeypatch):
    message = '12  50%  1MB/s  0:01 (xfr#1, to-chk=3/11)'
    rows = [
        {'_PID': '4', '_SYSTEMD_UNIT': 'work.service', '__REALTIME_TIMESTAMP': '1010000000', 'MESSAGE': message},
        {'_PID': '5', '_SYSTEMD_UNIT': 'work.service', '__REALTIME_TIMESTAMP': '1020000000', 'MESSAGE': message},
        {'_PID': '4', '_SYSTEMD_UNIT': 'wrong.service', '__REALTIME_TIMESTAMP': '1030000000', 'MESSAGE': message},
    ]
    class Result:
        stdout = '\n'.join(json.dumps(row) for row in rows)
    monkeypatch.setattr(seed.subprocess, 'run', lambda *a, **k: Result())
    result = seed.journal_sample(4, 'work.service', entries=10, logical_bytes=50, earliest=1000, now=1040)
    assert result == {'entries_checked': 8, 'entries_total': 11, 'transferred_bytes': 12,
                      'observed_at_epoch': 1010}
    assert seed.journal_sample(4, 'work.service', entries=10, logical_bytes=50, earliest=1011, now=1040) is None


def test_seed_checkpoint_is_historical_bound_to_live_owner_and_read_only(tmp_path, monkeypatch):
    index = tmp_path/'files.sqlite'
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE files (kind TEXT, state TEXT, bytes INTEGER)')
        db.executemany('INSERT INTO files VALUES (?, ?, ?)', [
            ('file', 'verified', 100), ('file', 'verified', 250), ('file', 'pending', 20),
            ('directory', 'verified', 0)])
    (tmp_path/'progress.json').write_text(json.dumps({'state': 'seeding_hardlinked_copy'}))
    monkeypatch.setattr(seed, 'boot_id', lambda: 'test-boot')
    monkeypatch.setattr(seed, 'owner_for', lambda _: 1)
    monkeypatch.setattr(seed, 'process_identity', lambda pid: (0, '10') if pid == 1 else (1, '20'))
    monkeypatch.setattr(seed.time, 'time', lambda: 1020)
    intent = {'files': 3, 'logical_bytes': 370, 'started_at_epoch': 1000}
    before = index.read_bytes()
    assert seed.capture_checkpoint('example', tmp_path, intent, owner=1, owner_start='10',
                                   pid=2, started='20', now=1020)
    result = seed.bound_checkpoint('example', tmp_path, intent, now=1020)
    assert result['verified_files'] == 2 and result['verified_bytes'] == 350
    assert index.read_bytes() == before
    assert seed.bound_checkpoint('other', tmp_path, intent, now=1020) is None
    assert seed.bound_checkpoint('example', tmp_path, {**intent, 'started_at_epoch': 999}, now=1020) is None
    monkeypatch.setattr(seed, 'boot_id', lambda: 'different-boot')
    assert seed.bound_checkpoint('example', tmp_path, intent, now=1020) is None
