import fcntl
import json
import os
from types import SimpleNamespace

from scripts import run_handoff_index_archive_queue as main_queue
from scripts import run_handoff_index_prepack_queue as prepack


def test_main_owner_requires_live_exact_flock_and_fresh_status(tmp_path, monkeypatch):
    monkeypatch.setattr(prepack.cli, 'ENROLLED', {'source':('dataset', [], False)})
    lock = tmp_path/'queue-owner.lock'
    fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        status = {'contract':main_queue.CONTRACT, 'owner_pid':os.getpid(), 'observed_at_epoch':1000,
                  'entries':[{'source':'source', 'dataset':'dataset', 'state':'running'}]}
        (tmp_path/'queue-status.json').write_text(json.dumps(status))
        assert prepack.main_owner(tmp_path, now=1001) == (os.getpid(), 'source', 'dataset')
        assert prepack.main_owner(tmp_path, now=1181) is None
        status['owner_pid'] += 1
        (tmp_path/'queue-status.json').write_text(json.dumps(status))
        assert prepack.main_owner(tmp_path, now=1001) is None
    finally:
        os.close(fd)
    assert prepack.main_owner(tmp_path, now=1001) is None


def test_prepack_only_prepares_another_ready_dataset(tmp_path, monkeypatch):
    monkeypatch.setattr(prepack.cli, 'ENROLLED', {'a-source':('a', [], False), 'b-source':('b', [], False)})
    def runner(command, on_heartbeat, timeout):
        assert command[-2:] == ['--source', 'b-source'] and '--retire' not in command
        on_heartbeat()
        (tmp_path/'b.status.json').write_text(json.dumps({'state':'prepared'}))
        return SimpleNamespace(returncode=0, stderr='')
    value = prepack.run_once(tmp_path, now=lambda:1000, ready=lambda _source:True, runner=runner,
        parent=lambda _root, **_kw:(111, 'a-source', 'a'),
        is_prepared=lambda _root, _dataset, value:value.get('state') == 'prepared')
    assert value['state'] == 'completed' and value['dataset'] == 'b'
    assert value['main_dataset'] == 'a'
    assert not value.get('C_payload_deleted', False) and not value.get('D_archive_committed', False)


def test_main_queue_skips_a_dataset_reserved_by_prepack(tmp_path, monkeypatch):
    monkeypatch.setattr(main_queue.cli, 'ENROLLED', {'a-source':('a', [], False), 'b-source':('b', [], False)})
    reservation = main_queue.reserve_dataset(tmp_path, 'a')
    try:
        def runner(command, on_heartbeat, timeout):
            assert command[-2:] == ['b-source', '--retire']
            from test_handoff_index_archive_queue import completed
            (tmp_path/'b.status.json').write_text(json.dumps(completed('b')))
            return SimpleNamespace(returncode=0, stderr='')
        rows = main_queue.run_once(tmp_path, now=lambda:1000, runner=runner, code_sha='x', ready=lambda _s:True)
        assert [row['state'] for row in rows] == ['waiting_prepack', 'completed']
    finally:
        os.close(reservation)


def test_prepack_does_not_start_after_main_owner_changes(tmp_path, monkeypatch):
    monkeypatch.setattr(prepack.cli, 'ENROLLED', {'a-source':('a', [], False), 'b-source':('b', [], False)})
    checks = iter([(111, 'a-source', 'a'), None])
    value = prepack.run_once(tmp_path, now=lambda:1000, ready=lambda _s:True,
        parent=lambda _root, **_kw:next(checks), is_prepared=lambda *_args:False,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('must not run')))
    assert value['state'] == 'waiting' and value['reason'] == 'main_queue_changed'
    reservation = main_queue.reserve_dataset(tmp_path, 'b')
    assert reservation is not None
    os.close(reservation)


def test_prepack_never_pretends_existing_minute_d_needs_new_e_packages(tmp_path, monkeypatch):
    raw = prepack.cli.h.MINUTE_RAW_NAME
    monkeypatch.setattr(prepack.cli, 'ENROLLED', {'a-source':('a', [], False),
        raw:('tw-minute-source-workspace', ['.download.lock'], True)})
    value = prepack.run_once(tmp_path, now=lambda:1000, ready=lambda _s:True,
        parent=lambda _root, **_kw:(111, 'a-source', 'a'), is_prepared=lambda *_a:False,
        runner=lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError('no new pack')))
    assert value['state'] == 'waiting' and value['reason'] == 'no_other_ready_source'
