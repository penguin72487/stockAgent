import json
from types import SimpleNamespace

from scripts import run_handoff_index_archive_queue as queue


def completed(dataset):
    return {'contract': queue.cli.CONTRACT, 'dataset': dataset,
            'state': 'C_payload_retired_root_and_locks_preserved', 'C_payload_deleted': True,
            'prepared_sha256': 'a'*64, 'C_retirement': {'state': 'C_payload_retired_root_and_lock_preserved',
                                                         'prepared_sha256': 'a'*64}}


def test_index_queue_seals_one_source_and_does_not_claim_D_completion(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False),
                                                 'b-source': ('b', ['.download.lock'], True)})
    def runner(command, on_heartbeat, timeout):
        assert command[-2:] == ['a-source', '--retire']
        on_heartbeat()
        (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='a', ready=lambda _: True)
    assert [row['state'] for row in rows] == ['completed', 'waiting']
    assert not json.loads((tmp_path/'a.status.json').read_text()).get('D_archive_committed', False)


def test_index_queue_skips_failed_source_until_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False),
                                                 'b-source': ('b', ['.download.lock'], False)})
    fail = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='lock busy')
    assert queue.run_once(tmp_path, now=lambda: 1000, runner=fail, code_sha='a', ready=lambda _: True)[0]['state'] == 'retrying'
    def runner(command, **_kw):
        assert command[-2:] == ['b-source', '--retire']
        (tmp_path/'b.status.json').write_text(json.dumps(completed('b')))
        return SimpleNamespace(returncode=0, stderr='')
    assert [row['state'] for row in queue.run_once(tmp_path, now=lambda: 1001, runner=runner, code_sha='a',
                                                    ready=lambda _: True)] == ['retrying', 'completed']


def test_handoff_dependency_waits_without_cooldown_and_does_not_block_other_sources(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False),
                                                 'b-source': ('b', ['.download.lock'], False)})
    active = {'b-source'}
    def runner(command, **_kw):
        dataset = command[-2][0]
        (tmp_path / (dataset + '.status.json')).write_text(json.dumps(completed(dataset)))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='a', ready=active.__contains__)
    assert [row['state'] for row in rows] == ['waiting_input', 'completed']
    active.add('a-source')
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=runner, code_sha='a', ready=active.__contains__)
    assert [row['state'] for row in rows] == ['completed', 'completed']


def test_transient_lock_contention_gets_short_retry_without_repacking(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False)})
    busy = lambda *_a, **_kw: SimpleNamespace(
        returncode=1, stderr='BlockingIOError: [Errno 11] Resource temporarily unavailable\n')
    row, = queue.run_once(tmp_path, now=lambda:1000, runner=busy, code_sha='a', ready=lambda _:True)
    assert row['state'] == 'retrying' and row['error'] == 'owner_busy'
    assert row['retry_after_epoch'] == 1005
    def resume(_command, **_kwargs):
        (tmp_path/'a.status.json').write_text(json.dumps(completed('a')))
        return SimpleNamespace(returncode=0, stderr='')
    row, = queue.run_once(tmp_path, now=lambda:1005, runner=resume, code_sha='a', ready=lambda _:True)
    assert row['state'] == 'completed'


def test_external_hardlinks_have_distinct_bounded_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.cli, 'ENROLLED', {'a-source': ('a', ['.download.lock'], False)})
    failed = lambda *_a, **_kw: SimpleNamespace(
        returncode=1, stderr='SnapshotError: retained source hardlinks extend outside the retirement scope\n')
    row, = queue.run_once(tmp_path, now=lambda:1000, runner=failed, code_sha='a', ready=lambda _:True)
    assert row['error'] == 'external_hardlinks' and row['retry_after_epoch'] == 1300


def test_progress_tracker_ignores_timestamps_regressions_and_other_generation(tmp_path):
    path = tmp_path/'a.status.json'
    def update(count, *, stage='streaming_source_into_E_packs', identity='a'*64, clock=1):
        path.write_text(json.dumps({'contract':queue.cli.CONTRACT, 'dataset':'a', 'state':'running',
            'source_identity':identity, 'observed_at_epoch':clock,
            'progress':{'stage':stage, 'files_completed':count}}))
    probe = queue.progress_tracker(tmp_path, 'a')
    assert probe() is None
    update(1); assert probe() == 0
    update(1, clock=2); assert probe() == 0
    update(0, clock=3); assert probe() == 0
    update(2, identity='b'*64); assert probe() == 0
    update(2); assert probe() == 1
    update(0, stage='rechecking_source_namespace'); assert probe() == 2
    update(100, stage='not_a_real_stage'); assert probe() is None


def test_progress_tracker_recognizes_existing_d_comparison(tmp_path):
    path = tmp_path/'a.status.json'
    def update(count):
        path.write_text(json.dumps({'contract':queue.cli.CONTRACT, 'dataset':'a', 'state':'running',
            'source_identity':'a'*64, 'progress':{'stage':'comparing_existing_D_inventory',
                                                   'files_completed':count}}))
    probe = queue.progress_tracker(tmp_path, 'a')
    update(1); assert probe() == 0
    update(2); assert probe() == 1
