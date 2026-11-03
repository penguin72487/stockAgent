import fcntl
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from stockagent.data_sync import storage_monitor as storage

_trading_opening_census_reason = storage.trading_opening_census_reason


@pytest.fixture(autouse=True)
def independent_of_live_trading_clock(monkeypatch):
    """Only explicit guardian tests use a clock, never the machine's current time."""
    monkeypatch.setattr(storage, 'trading_opening_census_reason', lambda *_: None)


@pytest.mark.parametrize(('session', 'protected', 'age', 'expected'), [
    ('open', True, 30, 'tw_opening_guard_active'),
    ('closed', False, 30, None),
    ('unknown', False, 30, 'tw_opening_guard_unknown'),
    ('open', False, 30, 'tw_opening_guard_unknown'),
    ('open', True, 181, 'tw_opening_guard_unknown'),
    ('open', True, -121, 'tw_opening_guard_unknown'),
])
def test_trading_opening_guard_uses_fresh_canonical_receipt(tmp_path, session, protected, age, expected):
    now = datetime(2026, 11, 3, 0, 30, tzinfo=timezone.utc)
    path = tmp_path / 'artifacts/operations/tw_day_trade_guardian/latest.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'schema_version': 1, 'session_date': '2026-11-03',
        'observed_at_taipei': (now - timedelta(seconds=age)).isoformat(), 'session_state': session,
        'components': {'opening_resource_guard': {'protected': protected}}}))
    assert _trading_opening_census_reason(tmp_path, now=now) == expected


def test_trading_opening_guard_missing_receipt_defers_only_protected_window(tmp_path):
    now = datetime(2026, 11, 3, 0, 30, tzinfo=timezone.utc)
    assert _trading_opening_census_reason(tmp_path, now=now) == 'tw_opening_guard_unknown'
    assert _trading_opening_census_reason(tmp_path, now=now - timedelta(minutes=11)) is None
    assert _trading_opening_census_reason(tmp_path, now=now + timedelta(minutes=40)) is None
    with pytest.raises(ValueError, match='requires_timezone'):
        _trading_opening_census_reason(tmp_path, now=now.replace(tzinfo=None))


def test_opening_guard_defers_optional_census_before_owner_observation(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, 'trading_opening_census_reason', lambda _: 'tw_opening_guard_active')
    def forbidden(*_):
        pytest.fail('opening guard must short-circuit optional scans')
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', forbidden)
    assert storage.local_census_priority_reason({'id': 'penguin-wsl'}, tmp_path) == 'tw_opening_guard_active'


def test_opening_guard_interrupts_owned_scan_after_it_starts(tmp_path, monkeypatch):
    import sys
    source = tmp_path / 'source'
    source.mkdir()
    lock = tmp_path / 'scheduler.lock'
    lock.touch()
    child_pid = tmp_path / 'child.pid'
    now = datetime(2026, 11, 3, 0, 30, tzinfo=timezone.utc)
    def guardian(_):
        active = child_pid.exists()
        return {'schema_version': 1, 'session_date': '2026-11-03',
                'observed_at_taipei': now.isoformat(), 'session_state': 'open' if active else 'closed',
                'components': {'opening_resource_guard': {'protected': active}}}
    monkeypatch.setattr(storage, 'operation_metadata', guardian)
    monkeypatch.setattr(storage, 'trading_opening_census_reason',
                        lambda repo: _trading_opening_census_reason(repo, now=now))
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    monkeypatch.setattr(storage, 'DATASET_VERSION_OWNER_DIRECTORY', tmp_path)
    monkeypatch.setattr(storage, 'capacity', lambda *_: measured())
    monkeypatch.setattr(storage, 'resolve_roots', lambda *_: [
        {'path': str(source), 'label': 'project', 'kind': 'source'}])
    code = f"import os,time;open({str(child_pid)!r},'w').write(str(os.getpid()));time.sleep(30)"
    monkeypatch.setattr(storage, 'du_command', lambda *_: [sys.executable, '-c', code])
    monkeypatch.setattr(storage, 'CENSUS_PRIORITY_POLL_SECONDS', 0.05)
    result = storage.census({'id': 'penguin-wsl', 'path': str(tmp_path)}, tmp_path,
                            depth=1, timeout=5, wave_lock=lock)
    assert result['state'] == 'deferred' and result['reason'] == 'tw_opening_guard_active'
    with pytest.raises(ProcessLookupError):
        os.kill(int(child_pid.read_text()), 0)


def measured(**change):
    return {"state": "measured", "observed_at_utc": "2026-10-25T07:00:00+00:00", "source": "statvfs",
            "total_bytes": 10000, "used_bytes": 8000, "free_bytes": 2000, "available_bytes": 1800,
            "reserved_free_bytes": 200, **change}


def fixture_status(cap=None):
    return {"contract": storage.CONTRACT, "volumes": [{"id": "penguin-wsl", "capacity": cap or measured()}]}


def test_cold_tier_projection_separates_schedule_poll_and_last_real_work():
    now = storage.epoch("2026-10-25T07:01:00+00:00")
    raw = fixture_status()
    raw["cold_tiers"] = {"contract": "stockagent.cold_tiers.v1", "observed_at_epoch": now - 1,
        "state": "outside_daily_window", "path": "/private/cold-root", "private_reason": "secret",
        "schedule": {"daily_window_timezone": "Asia/Taipei", "daily_window_start_hour": 3,
                     "daily_window_end_hour": 7, "stable_after_hours": 6},
        "last_work_cycle": {"contract": "stockagent.cold_tiers.v1", "apply": True,
            "observed_at_epoch": now - 3600, "objects_completed": 2,
            "logical_bytes_processed": 1234, "allocation_bytes_saved": 456, "failed_intents": 0,
            "private_diagnostics": "secret"}}
    value = storage.public_status(raw, now=now)["cold_tiers"]
    assert value["state"] == "outside_daily_window" and value["stale"] is False
    assert value["last_work_cycle"]["allocation_bytes_saved"] == 456
    assert value["scope"] == "last_bounded_cycle_not_whole_lake_or_NAS"
    assert value["schedule"] == {"timezone": "Asia/Taipei", "start_hour": 3, "end_hour": 7, "stable_after_hours": 6}
    assert "private" not in json.dumps(value) and "secret" not in json.dumps(value)
    raw["cold_tiers"]["observed_at_epoch"] = now - 1800
    assert storage.public_status(raw, now=now)["cold_tiers"]["stale"] is True


@pytest.mark.parametrize("bad", [None, [], {"contract": "unknown"},
    {"contract": "stockagent.cold_tiers.v1", "observed_at_epoch": True},
    {"contract": "stockagent.cold_tiers.v1", "observed_at_epoch": float("nan")},
    {"contract": "stockagent.cold_tiers.v1", "observed_at_epoch": float("inf")}])
def test_bad_tier_metadata_does_not_hide_valid_disk_capacity(bad):
    raw = fixture_status(); raw["cold_tiers"] = bad
    value = storage.public_status(raw, now=storage.epoch("2026-10-25T07:01:00+00:00"))
    assert value["cold_tiers"] is None
    assert volume(value, "penguin-wsl")["capacity"]["used_bytes"] == 8000


def volume(value, identity):
    return next(row for node in value["nodes"] for row in node["volumes"] if row["id"] == identity)


def test_storage_capacity_keeps_reserve_distinct_and_never_sums_nested_disks():
    raw = fixture_status()
    raw["volumes"][0]["contained_in"] = "penguin-c"
    public = storage.public_status(raw, now=storage.epoch("2026-10-25T07:01:00+00:00"))
    assert public["aggregate_capacity_bytes"] is None
    row = volume(public, "penguin-wsl")
    assert row["capacity"]["used_bytes"] == 8000
    assert row["capacity"]["available_bytes"] == 1800
    assert row["capacity"]["reserved_free_bytes"] == 200
    assert row["contained_in"] == "penguin-c"
    assert row["capacity"]["state"] == "measured"


@pytest.mark.parametrize('defect', ['none', 'stale', 'partial', 'overstated', 'ambiguous', 'negative'])
def test_host_accounting_separates_wsl_allocation_from_growable_free_and_hides_paths(defect):
    raw = fixture_status()
    raw['volumes'][0]['contained_in'] = 'penguin-c'
    allocation = {'complete': True, 'distribution_count': 2, 'current_distribution_count': 1,
                  'current_distribution_allocated_bytes': 6000,
                  'all_distributions_allocated_bytes': 6500,
                  'path': '/private/VHD', 'token': 'never-publish'}
    cap = measured(source='native_windows', wsl_allocation=allocation)
    if defect == 'stale':
        cap['observed_at_utc'] = '2026-10-24T07:00:00+00:00'
    elif defect == 'partial':
        allocation['complete'] = False
    elif defect == 'overstated':
        allocation['all_distributions_allocated_bytes'] = 9000
    elif defect == 'ambiguous':
        allocation['current_distribution_count'] = 2
    elif defect == 'negative':
        allocation['current_distribution_allocated_bytes'] = -1
    raw['volumes'].append({'id': 'penguin-c', 'capacity': cap})
    result = storage.public_status(raw, now=storage.epoch('2026-10-25T07:01:00+00:00'))
    wsl = volume(result, 'penguin-wsl')
    account = wsl['host_storage_accounting']
    assert account['state'] == ('measured' if defect == 'none' else 'unknown')
    if defect == 'none':
        assert account['current_wsl_allocated_bytes'] == 6000
        assert account['other_wsl_allocated_bytes'] == 500
        assert account['windows_and_other_used_bytes'] == 1500
        assert account['current_wsl_capacity_ceiling_bytes'] == 7800
        assert wsl['capacity']['available_bytes'] == 1800
        assert wsl['writable_headroom']['available_bytes'] == 1800
    assert 'private/VHD' not in json.dumps(result) and 'never-publish' not in json.dumps(result)


def test_daily_capacity_history_records_each_fresh_volume_once_per_day(tmp_path):
    path = tmp_path / 'history.json'
    now = datetime(2026, 10, 25, 7, 1, tzinfo=timezone.utc)
    rows = [{'id': 'penguin-wsl', 'capacity': measured()},
            {'id': 'penguin-c', 'capacity': measured(source='native_windows')},
            {'id': 'nas-disk', 'capacity': measured(state='partial')}]
    assert storage.record_daily_capacity(path, rows, now=now)
    first = path.read_bytes()
    history = json.loads(first)
    assert set(history['volumes']) == {'penguin-wsl', 'penguin-c'}
    assert 'not source ingestion' in history['definition']
    assert not storage.record_daily_capacity(path, rows, now=now + timedelta(seconds=10))
    assert path.read_bytes() == first
    tomorrow = now + timedelta(days=1)
    rows[0]['capacity'] = measured(observed_at_utc=(tomorrow - timedelta(seconds=10)).isoformat(),
                                    used_bytes=7900, free_bytes=2100, available_bytes=1900)
    assert storage.record_daily_capacity(path, rows, now=tomorrow)
    samples = json.loads(path.read_bytes())['volumes']['penguin-wsl']
    assert [row['date'] for row in samples] == ['2026-10-25', '2026-10-26']
    assert samples[1]['used_bytes'] - samples[0]['used_bytes'] == -100
    assert len(json.loads(path.read_bytes())['volumes']['penguin-c']) == 1  # stale, not zero


def test_daily_capacity_history_rejects_redirected_or_invalid_existing_history(tmp_path):
    now = datetime(2026, 10, 25, 7, 1, tzinfo=timezone.utc)
    rows = [{'id': 'penguin-wsl', 'capacity': measured()}]
    path = tmp_path / 'history.json'
    path.write_text('{"contract":"wrong"}')
    with pytest.raises(ValueError, match='invalid_capacity_history'):
        storage.record_daily_capacity(path, rows, now=now)
    assert path.read_text() == '{"contract":"wrong"}'
    path.unlink()
    target = tmp_path / 'target.json'
    target.write_text('{}')
    path.symlink_to(target)
    with pytest.raises(ValueError, match='redirected_capacity_history'):
        storage.record_daily_capacity(path, rows, now=now)
    assert target.read_text() == '{}'


def test_storage_partial_nas_does_not_invent_total_used_or_occupancy():
    raw = {"contract": storage.CONTRACT, "volumes": [{"id": "nas-disk", "capacity": {
        "state": "partial", "source": "paired_backup_readiness", "available_bytes": 1234,
        "observed_at_utc": "2026-10-25T07:00:00+00:00"}}]}
    row = volume(storage.public_status(raw, now=storage.epoch("2026-10-25T07:01:00+00:00")), "nas-disk")
    assert row["capacity"]["available_bytes"] == 1234
    assert row["capacity"]["total_bytes"] is None
    assert row["capacity"]["used_bytes"] is None
    assert row["capacity"]["complete"] is False
    assert row["classification"]["allocated_bytes"] is None


@pytest.mark.parametrize("cap", [[], 2, {"total_bytes": -1}, measured(used_bytes=9000), measured(total_bytes=True)])
def test_invalid_capacity_is_unknown_instead_of_false_zero(cap):
    raw = fixture_status()
    raw["volumes"][0]["capacity"] = cap
    row = volume(storage.public_status(raw), "penguin-wsl")
    assert row["capacity"]["complete"] is False
    assert row["capacity"]["total_bytes"] is None
    assert row["capacity"]["available_bytes"] is None


def test_stale_and_failure_preserve_actual_measurement_clock():
    before = measured()
    failed = {"state": "unavailable", "error_code": "TimeoutExpired", "attempted_at_utc": "2026-10-25T08:00:00+00:00"}
    retained = storage.retain_observation(before, failed)
    row = volume(storage.public_status(fixture_status(retained), now=storage.epoch("2026-10-25T08:00:00+00:00")), "penguin-wsl")
    assert row["capacity"]["observed_at_utc"] == before["observed_at_utc"]
    assert row["capacity"]["state"] == "stale"
    assert row["capacity"]["used_bytes"] == 8000


def test_future_observation_cannot_be_current():
    row = volume(storage.public_status(fixture_status(), now=storage.epoch("2026-10-25T06:00:00+00:00")), "penguin-wsl")
    assert row["capacity"]["state"] == "stale"


def test_public_storage_projection_drops_private_connection_and_extra_fields():
    raw = fixture_status()
    raw["password"] = "private-secret"
    raw["volumes"][0].update(ssh_target="private-host", path="/root/private-account")
    raw["volumes"][0]["capacity"]["stderr"] = "private-secret"
    raw["volumes"][0]["classification"] = {"state": "complete", "allocated_bytes": 40, "tree": [
        {"id": "r0/data", "label": "資料", "allocated_bytes": 40, "kind": "source", "children": [], "path": "/root/private-account"}]}
    body = json.dumps(storage.public_status(raw), ensure_ascii=False)
    assert "private-" not in body
    assert "password" not in body
    assert json.loads(body)["read_only"] is True


def test_real_du_deduplicates_hardlinks_across_roots_without_reading_payload(tmp_path):
    roots = [tmp_path / "source", tmp_path / "view"]
    for root in roots:
        root.mkdir()
    (roots[0] / "part.bin").write_bytes(b"x" * 8192)
    os.link(roots[0] / "part.bin", roots[1] / "same.bin")
    specs = [{"path": str(root), "label": root.name, "kind": "source"} for root in roots]
    result = subprocess.run(storage.du_command(specs, 2), capture_output=True, check=True)
    tree = storage.parse_du(result.stdout, specs)
    expected = (roots[0] / "part.bin").stat().st_blocks * 512 + sum(root.stat().st_blocks * 512 for root in roots)
    assert tree["allocated_bytes"] == expected
    assert tree["reclaimable_bytes"] is None


def test_real_single_du_publishes_progress_and_reorders_without_changing_root_ids(tmp_path, monkeypatch):
    roots = [tmp_path / name for name in ('giant', 'unmeasured', 'small')]
    for root in roots:
        root.mkdir()
    (roots[0] / 'part').write_bytes(b'x' * 8192)
    os.link(roots[0] / 'part', roots[1] / 'shared')
    (roots[2] / 'part').write_bytes(b'y' * 4096)
    specs = [{'path': str(path), 'label': path.name, 'kind': 'source'} for path in roots]
    prior = {'tree': [{'label': 'giant', 'allocated_bytes': 10**12},
                      {'label': 'small', 'allocated_bytes': 1}]}
    monkeypatch.setattr(storage, 'capacity', lambda *_: measured())
    monkeypatch.setattr(storage, 'resolve_roots', lambda *_: specs)
    progress = storage.CensusProgress()
    native_run, commands, updates = subprocess.run, [], []

    def run(command, **options):
        commands.append(command)
        result = native_run(command, **options)
        position = options['stdout'].tell()
        updates.append(progress.snapshot())
        assert options['stdout'].tell() == position, 'progress moved the shared writer offset'
        return result

    monkeypatch.setattr(storage.subprocess, 'run', run)
    final = storage.census({'id': 'penguin-wsl', 'path': str(tmp_path)}, tmp_path,
                           depth=2, timeout=10, previous=prior, progress=progress)
    assert len(commands) == 1
    assert commands[0][-3:] == [str(roots[1]), str(roots[2]), str(roots[0])]
    expected = sum(path.stat().st_blocks * 512 for path in roots)
    expected += (roots[0] / 'part').stat().st_blocks * 512 + (roots[2] / 'part').stat().st_blocks * 512
    assert final['state'] == 'complete' and final['allocated_bytes'] == expected
    assert updates[0]['state'] == 'scanning' and updates[0]['provisional']
    assert updates[0]['allocated_bytes'] == expected and updates[0]['allocated_bytes_is_lower_bound']
    assert final['roots_completed'] == final['roots_total'] == 3
    assert {row['label']: row['id'] for row in final['tree']} == {
        'giant': 'r0/.', 'unmeasured': 'r1/.', 'small': 'r2/.'}
    assert progress.snapshot() is None
    assert (roots[0] / 'part').read_bytes() == b'x' * 8192


def test_census_progress_preserves_partial_records_offset_and_descriptor_lifetime(tmp_path):
    import tempfile
    roots = [{'path': str(tmp_path), 'label': 'project'}]
    progress = storage.CensusProgress()
    with tempfile.TemporaryFile() as stream, progress.capture(stream, roots, storage.utc(), lambda: None):
        os.write(stream.fileno(), f'12\t{tmp_path / "a"}\0'.encode())
        os.write(stream.fileno(), f'30\t{tmp_path / "b"}'.encode())
        position = stream.tell()
        first = progress.snapshot()
        assert stream.tell() == position
        assert first['allocated_bytes'] == 12 and first['roots_completed'] == 0
        assert first['roots_total'] == 1 and first['allocated_bytes_is_lower_bound']
        assert progress.snapshot() is None, 'unchanged records must not refresh observation time'
        os.write(stream.fileno(), b'\0')
        second = progress.snapshot()
        assert second['allocated_bytes'] == 42 and second['roots_completed'] == 0
        os.write(stream.fileno(), f'100\t{tmp_path}\0'.encode())
        third = progress.snapshot()
        assert third['allocated_bytes'] == 100 and third['roots_completed'] == 1
    assert progress.snapshot() is None, 'closed/reused FD must never remain visible'


@pytest.mark.parametrize('failure', ['mount', 'invalid', 'oversized'])
def test_census_progress_cannot_publish_untrusted_or_unbounded_metadata(tmp_path, failure):
    import tempfile
    progress = storage.CensusProgress()

    def guard():
        if failure == 'mount':
            raise ValueError('classification_mount_changed')

    with tempfile.TemporaryFile() as stream, progress.capture(
            stream, [{'path': str(tmp_path), 'label': 'project'}], storage.utc(), guard):
        os.write(stream.fileno(), b'not-a-record\0' if failure == 'invalid' else f'12\t{tmp_path}\0'.encode())
        if failure == 'oversized':
            stream.truncate(storage.MAX_SNAPSHOT_BYTES + 1)
        assert progress.snapshot() is None
    assert progress.snapshot() is None


def test_root_order_ignores_ambiguous_invalid_or_absent_measurements():
    roots = [{'path': '/a', 'label': 'same'}, {'path': '/b', 'label': 'new'},
             {'path': '/c', 'label': 'small'}, {'path': '/d', 'label': 'giant'}]
    previous = {'tree': [{'label': 'same', 'allocated_bytes': 999},
                         {'label': 'same', 'allocated_bytes': 3},
                         {'label': 'small', 'allocated_bytes': 1},
                         {'label': 'giant', 'allocated_bytes': 9999}, None]}
    assert storage.census_root_order(roots, previous) == roots
    assert storage.census_root_order(roots, {'tree': 'malformed'}) == roots


@pytest.mark.parametrize('completed,total,expected', [(1, 2, 1), (3, 2, None), (True, 2, None), (0, 0, 0)])
def test_public_root_coverage_never_invents_a_valid_denominator(completed, total, expected):
    raw = fixture_status()
    raw['volumes'][0]['classification'] = {'state': 'scanning', 'roots_total': total,
                                          'roots_completed': completed}
    group = volume(storage.public_status(raw), 'penguin-wsl')['classification']
    assert group['roots_completed'] == expected


def test_du_newlines_tabs_and_hierarchy_keep_accounting(tmp_path):
    path = tmp_path / "data_finmind"
    child = path / "x\ny\tz"
    root = {"path": str(path), "label": "FinMind", "kind": "source"}
    raw = f"20\t{child}\0".encode() + f"30\t{path}\0".encode()
    result = storage.parse_du(raw, [root])
    assert result["allocated_bytes"] == 30
    assert result["tree"][0]["direct_or_unexpanded_bytes"] == 10
    assert "\n" not in result["tree"][0]["children"][0]["label"]


def test_du_folded_categories_preserve_bytes_and_concurrent_change_is_partial(tmp_path):
    root = {"path": str(tmp_path), "label": "專案", "kind": "project"}
    raw = b"".join(f"10\t{tmp_path / str(i)}\0".encode() for i in range(20)) + f"190\t{tmp_path}\0".encode()
    tree = storage.parse_du(raw, [root], max_children=12)
    assert len(tree["tree"][0]["children"]) == 13
    assert sum(row["allocated_bytes"] for row in tree["tree"][0]["children"]) == 200
    assert tree["concurrent_change_detected"] is True


@pytest.mark.parametrize("raw", [b"100\t/unregistered\0", b"nonsense\0", b"-1\t/root\0"])
def test_du_rejects_ambiguous_or_unregistered_records(tmp_path, raw):
    with pytest.raises(ValueError):
        storage.parse_du(raw, [{"path": str(tmp_path), "label": "a"}])


@pytest.mark.parametrize("paths", [("a", "a"), ("a", "a/nested")])
def test_du_rejects_ambiguous_root_scope_even_without_nested_record(tmp_path, paths):
    roots = [{"path": str(tmp_path / path), "label": path} for path in paths]
    with pytest.raises(ValueError, match="ambiguous_du_root"):
        storage.parse_du(f"10\t{tmp_path / 'a'}\0".encode(), roots, allow_missing=True)


def test_du_root_prefixes_remain_disjoint_and_reordered_ids_follow_registered_scope(tmp_path):
    roots = [{"path": str(tmp_path / name), "label": name} for name in ("aa", "a")]
    raw = (f"9\t{tmp_path / 'a' / 'data' / 'leaf'}\0" +
           f"4\t{tmp_path / 'aa'}\0").encode()
    parsed = storage.parse_du(raw, roots, allow_missing=True, partial_output=True)
    assert parsed["allocated_bytes"] == 13 and parsed["roots_completed"] == 1
    assert [(row["label"], row["id"], row["allocated_bytes"]) for row in parsed["tree"]] == [
        ("a", "r1/.", 9), ("aa", "r0/.", 4)]
    assert parsed["tree"][0]["children"][0]["children"][0]["id"] == "r1/data/leaf"


def test_timeout_partial_subtrees_are_disjoint_measured_lower_bounds(tmp_path):
    root = {"path": str(tmp_path), "label":"project"}
    raw = (f"10\t{tmp_path/'data'/'a'}\0" + f"30\t{tmp_path/'data'/'b'}\0" +
           f"5\t{tmp_path/'data'/'a'/'deep'}\0").encode()
    result = storage.parse_du(raw, [root], allow_missing=True, partial_output=True)
    assert result["allocated_bytes"] == 40 and result["allocated_bytes_is_lower_bound"] is True
    project = result["tree"][0]
    assert project["allocated_bytes_is_lower_bound"] is True
    data = project["children"][0]
    assert data["allocated_bytes"] == 40 and data["allocated_bytes_is_lower_bound"] is True
    assert sum(c["allocated_bytes"] for c in data["children"]) == 40
    public = storage.public_status({"contract":storage.CONTRACT, "volumes":[{
        "id":"penguin-wsl", "classification":{**result, "state":"partial", "observed_at_utc":storage.utc()}}]})
    group = volume(public, "penguin-wsl")["classification"]
    assert group["allocated_bytes_is_lower_bound"] is True and group["tree"][0]["allocated_bytes_is_lower_bound"] is True


def test_census_timeout_keeps_complete_records_and_discards_partial_record(tmp_path, monkeypatch):
    root = {"path":str(tmp_path), "label":"project"}
    monkeypatch.setattr(storage, "capacity", lambda *a:measured())
    monkeypatch.setattr(storage, "resolve_roots", lambda *a:[root])
    def interrupted(*args, **kwargs):
        kwargs["stdout"].write(f"12\t{tmp_path/'done'}\0".encode() + b"900\tunfinished")
        raise subprocess.TimeoutExpired("private-command", kwargs["timeout"])
    monkeypatch.setattr(storage.subprocess, "run", interrupted)
    result = storage.census({"id":"penguin-wsl", "path":str(tmp_path)}, tmp_path, depth=3, timeout=1)
    assert result["state"] == "partial" and result["timed_out"] is True
    assert result["allocated_bytes"] == 12 and result["allocated_bytes_is_lower_bound"] is True
    assert "private-command" not in json.dumps(result)


def test_deep_long_ids_do_not_collide_and_pruned_children_stay_accounted():
    rows = [{"id": "x" * 150 + suffix, "label": suffix, "allocated_bytes": 100, "kind": "source", "children": [
        {"id": "child", "label": "資料", "allocated_bytes": 70, "children": []}]} for suffix in ("a", "b")]
    tree = storage.sanitize_tree(rows)
    assert tree[0]["id"] != tree[1]["id"]
    assert tree[0]["children"][0]["id"] != tree[1]["children"][0]["id"]
    pruned = storage.sanitize_tree(rows, budget=[1])
    assert pruned[0]["direct_or_unexpanded_bytes"] == 100


def test_efficiency_projection_whitelists_actual_retirement_instead_of_claiming_global_savings():
    assert storage.public_efficiency({}) is None
    value = storage.public_efficiency({"contract": "stockagent.storage_efficiency.v1", "observed_at_utc": storage.utc(),
        "reclaimed_allocated_bytes": 1024, "protected_active_bytes": 2048, "compression_sample_files": 3,
        "source_files_deleted": 0, "cold_objects_deleted": 0, "private_path": "/root/secret"})
    assert value["reclaimed_allocated_bytes"] == 1024
    assert "/root/secret" not in json.dumps(value)


def test_root_discovery_excludes_symlink_interfaces_and_overlapping_roots(tmp_path):
    repo = tmp_path / "stockAgent"
    repo.mkdir()
    data = repo / "data_x"
    data.mkdir()
    (tmp_path / "stockAgent-other").symlink_to(repo, target_is_directory=True)
    specs = storage.resolve_roots({"roots": [{"path": "{repo}", "label": "a"}, {"path": str(data), "label": "b"}],
                                   "discover_worktrees": True}, repo)
    assert len(specs) == 1
    assert specs[0]["path"] == str(repo)


def test_remote_failure_is_bounded_and_never_exposes_command_details(monkeypatch):
    monkeypatch.setattr(storage, "ssh_prefix", lambda _config: ["ssh", "private-host"])
    def timeout(*args, **kwargs):
        assert 14 < kwargs["timeout"] <= 15
        raise subprocess.TimeoutExpired("private-secret", 15, stderr="private-secret")
    monkeypatch.setattr(storage.subprocess, "run", timeout)
    result = storage.remote_capacity({})
    assert result["state"] == "unavailable"
    assert "private" not in json.dumps(result)


def test_remote_probe_retries_one_transient_reset_within_original_budget(monkeypatch):
    monkeypatch.setattr(storage, 'ssh_prefix', lambda _: ['ssh', 'fixed-enrollment'])
    results = iter([subprocess.CompletedProcess([], 255, '', 'Connection reset by peer'),
                    subprocess.CompletedProcess([], 0, json.dumps(measured()), '')])
    timeouts = []
    def probe(_command, **kwargs):
        timeouts.append(kwargs['timeout'])
        return next(results)
    monkeypatch.setattr(storage.subprocess, 'run', probe)
    assert storage.remote_capacity({}) == measured()
    assert len(timeouts) == 2 and 0 < timeouts[1] < timeouts[0] <= 15


@pytest.mark.parametrize('results, attempts', [
    ([subprocess.CompletedProcess([], 255, '', 'Permission denied')], 1),
    ([subprocess.CompletedProcess([], 255, '', 'REMOTE HOST IDENTIFICATION HAS CHANGED')], 1),
    ([subprocess.CompletedProcess([], 0, '{"private":"invalid capacity"}', '')], 1),
    ([subprocess.CompletedProcess([], 255, '', 'Connection reset by peer')] * 2, 2)])
def test_remote_probe_never_retries_auth_bad_data_or_repeats_forever(monkeypatch, results, attempts):
    monkeypatch.setattr(storage, 'ssh_prefix', lambda _: ['ssh', 'private-host'])
    remaining = iter(results)
    calls = []
    def probe(*args, **kwargs):
        calls.append(kwargs['timeout'])
        return next(remaining)
    monkeypatch.setattr(storage.subprocess, 'run', probe)
    result = storage.remote_capacity({})
    assert result['state'] == 'unavailable' and len(calls) == attempts
    assert 'private' not in json.dumps(result) and 'observed_at_utc' not in result


def test_failed_windows_bridge_does_not_abort_other_volume_observations(monkeypatch):
    from stockagent.data_sync import windows_cold_io
    from stockagent.data_sync.desync_snapshots import SnapshotError

    def unavailable():
        raise SnapshotError("private bridge details")

    monkeypatch.setattr(windows_cold_io, "_windows_process_environment", unavailable)
    monkeypatch.setattr(storage, "capacity", lambda *_: measured())
    observed = storage.observe_capacities({"observer_node_id": "penguin", "volumes": [
        {"id": "penguin-wsl", "path": "/", "filesystem": "ext4"}]})
    assert observed["penguin-wsl"]["used_bytes"] == 8000
    assert "penguin-c" not in observed
    assert "private" not in json.dumps(observed)


def test_missing_or_redirected_snapshot_does_not_follow_link(tmp_path):
    real = tmp_path / "real.json"
    real.write_text("{}")
    link = tmp_path / "link.json"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="redirected"):
        storage.read_snapshot(link)


def test_capacity_only_cycle_never_starts_census(tmp_path, monkeypatch):
    monkeypatch.setenv("WSL_DISTRO_NAME", "test-distro")
    config = {"schema_version": 1, "observer_node_id": "penguin", "capacity_interval_seconds": 60,
        "classification_interval_seconds": 21600, "classification_timeout_seconds": 60, "classification_depth": 2,
        "volumes": [{"id": "penguin-wsl", "path": str(tmp_path), "roots": []}]}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    monkeypatch.setattr(storage, "observe_capacities", lambda _config: {"penguin-wsl": measured()})
    monkeypatch.setattr(storage, "census", lambda *_a, **_k: pytest.fail("capacity tick performed census"))
    output = tmp_path / "status.json"
    storage.monitor(path, output, tmp_path)
    value = json.loads(output.read_text())
    assert value["volumes"][0]["capacity"]["used_bytes"] == 8000


def test_capacity_ticks_publish_same_scan_progress_and_resume_an_interrupted_census(tmp_path, monkeypatch):
    import tempfile
    from threading import Event
    from stockagent.data_sync import storage_runtime

    monkeypatch.setenv('WSL_DISTRO_NAME', 'test-distro')
    monkeypatch.setattr(storage, 'DATASET_VERSION_OWNER_DIRECTORY', tmp_path)
    spec = {'id': 'penguin-wsl', 'path': str(tmp_path), 'roots': []}
    config = {'schema_version': 1, 'observer_node_id': 'penguin', 'capacity_interval_seconds': 60,
              'classification_interval_seconds': 21600, 'classification_timeout_seconds': 60,
              'classification_depth': 2, 'volumes': [spec]}
    config_path, output = tmp_path / 'config.json', tmp_path / 'status.json'
    config_path.write_text(json.dumps(config))
    # A recent progress timestamp cannot defer a scan lost in a prior process.
    output.write_text(json.dumps({'contract': storage.CONTRACT, 'volumes': [{'id': spec['id'],
        'classification': {'state': 'scanning', 'observed_at_utc': storage.utc(),
                           'fingerprint': storage.census_fingerprint(config, spec, None)}}]}))
    started, finish = Event(), Event()
    ticks, snapshots, calls = [], [], []

    def capacities(_):
        ticks.append(True)
        return {'penguin-wsl': measured(observed_at_utc=storage.utc())}

    def census(volume, repo, *, progress, previous, **options):
        calls.append(previous)
        with tempfile.TemporaryFile() as stream, progress.capture(stream,
                [{'path': str(tmp_path), 'label': 'project'}], storage.utc(), lambda: None):
            os.write(stream.fileno(), f'12\t{tmp_path / "done"}\0'.encode())
            started.set()
            assert finish.wait(5), 'test did not release the sole census worker'
        return {'state': 'partial', 'observed_at_utc': storage.utc(), 'tree': []}

    class Finished(Exception):
        pass

    def next_tick(_):
        assert started.wait(2)
        snapshots.append(json.loads(output.read_bytes()))
        if len(snapshots) == 3:
            finish.set()
            raise Finished

    monkeypatch.setattr(storage, 'observe_capacities', capacities)
    # This test owns a fake clock for capacity/census turns. A real systemctl
    # probe's subprocess timeout also calls time.sleep and would accidentally
    # advance that clock before the next capacity tick.
    monkeypatch.setattr(storage_runtime, 'collect', lambda *_a, **_k: {})
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    monkeypatch.setattr(storage, 'census', census)
    monkeypatch.setattr(storage.time, 'sleep', next_tick)
    try:
        with pytest.raises(Finished):
            storage.monitor(config_path, output, tmp_path, watch=True)
    finally:
        finish.set()
    assert len(ticks) == 3 and len(calls) == 1
    assert calls[0]['state'] == 'not_scanned'
    assert calls[0]['reason'] == 'previous_census_interrupted'
    second = snapshots[1]['volumes'][0]['classification']
    third = snapshots[2]['volumes'][0]['classification']
    assert second['state'] == 'scanning' and second['allocated_bytes'] == 12
    assert second['allocated_bytes_is_lower_bound'] and second['roots_total'] == 1
    assert second == third, 'capacity ticks without new records must retain the prior data clock'


def test_wsl_census_defers_while_exact_retirement_wave_owns_lock(tmp_path, monkeypatch):
    lock = tmp_path / 'scheduler.lock'
    lock.touch()
    monkeypatch.setattr(storage, '_census', lambda *a, **k: pytest.fail('must not scan'))
    with lock.open('r+') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = storage.census({'id': 'penguin-wsl'}, tmp_path, depth=1, timeout=30, wave_lock=lock)
    assert result == {'state': 'deferred', 'reason': 'retirement_wave_active'}


@pytest.mark.parametrize('emit_record', [True, False])
def test_waiting_retirement_preempts_owned_scan_and_releases_lock(tmp_path, monkeypatch, emit_record):
    import sys
    import time

    monkeypatch.setattr(storage, 'DATASET_VERSION_OWNER_DIRECTORY', tmp_path)
    source = tmp_path / 'source'
    source.mkdir()
    lock = tmp_path / 'scheduler.lock'
    lock.touch()
    child_pid = tmp_path / 'child.pid'
    roots = [{'path': str(source), 'label': 'project', 'kind': 'source'}]
    record = ('12\t' + str(source / 'done') + chr(0) + '900\tunfinished').encode()
    code = ("import os,time\n"
            f"open({str(child_pid)!r},'w').write(str(os.getpid()))\n"
            + (f"os.write(1, {record!r})\n" if emit_record else '')
            + "time.sleep(30)\n")
    monkeypatch.setattr(storage, 'capacity', lambda *_: measured())
    monkeypatch.setattr(storage, 'resolve_roots', lambda *_: roots)
    monkeypatch.setattr(storage, 'du_command', lambda *_: [sys.executable, '-c', code])
    monkeypatch.setattr(storage, 'CENSUS_PRIORITY_POLL_SECONDS', 0.1)
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: child_pid.exists())

    started = time.monotonic()
    result = storage.census({'id': 'penguin-wsl', 'path': str(tmp_path)}, tmp_path,
                            depth=1, timeout=10, wave_lock=lock)
    assert time.monotonic() - started < 8
    if emit_record:
        assert result['state'] == 'partial' and result['preempted'] is True
        assert result['reason'] == 'retirement_wave_pending'
        assert result['allocated_bytes'] == 12 and result['allocated_bytes_is_lower_bound']
    else:
        assert result == {'state': 'deferred', 'reason': 'retirement_wave_pending'}
    with pytest.raises(ProcessLookupError):
        os.kill(int(child_pid.read_text()), 0)
    with lock.open('r+') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_guarded_census_finishes_normally_without_waiting_retirement(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, 'DATASET_VERSION_OWNER_DIRECTORY', tmp_path)
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'data').write_bytes(b'data')
    lock = tmp_path / 'scheduler.lock'
    lock.touch()
    monkeypatch.setattr(storage, 'capacity', lambda *_: measured())
    monkeypatch.setattr(storage, 'resolve_roots', lambda *_: [
        {'path': str(source), 'label': 'project', 'kind': 'source'}])
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    result = storage.census({'id': 'penguin-wsl', 'path': str(tmp_path)}, tmp_path,
                            depth=1, timeout=10, wave_lock=lock)
    assert result['state'] == 'complete' and result['preempted'] is False
    assert result['roots_completed'] == 1 and result['reclaimable_bytes'] is None


def test_transport_retirement_preempts_optional_census_but_not_capacity(tmp_path, monkeypatch):
    import time
    config = tmp_path/'configs/data_sync'
    config.mkdir(parents=True)
    owner = tmp_path/'owner'
    owner.mkdir()
    (config/'storage_monitor.json').write_text(json.dumps({'operations': {
        'workspace_relocation_state_root': str(owner)}}))
    (owner/'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader', 'observed_at_epoch': time.time()}))
    monkeypatch.setattr(storage, 'trading_opening_census_reason', lambda _: None)
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    for volume in ('penguin-wsl', 'penguin-d', 'penguin-workspace'):
        assert storage.local_census_priority_reason({'id': volume}, tmp_path) == 'transport_relocation_owner_active'
    assert storage.local_census_priority_reason({'id': 'penguin-c'}, tmp_path) is None
    (owner/'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader', 'observed_at_epoch': time.time() - 121}))
    assert storage.local_census_priority_reason({'id': 'penguin-workspace'}, tmp_path) is None


def test_stale_retirement_counter_still_defers_optional_census_while_original_lock_is_held(tmp_path, monkeypatch):
    import time
    config = tmp_path / 'configs/data_sync'
    config.mkdir(parents=True)
    owner, backup = tmp_path / 'owner', tmp_path / 'backup'
    owner.mkdir()
    backup.mkdir()
    (config / 'storage_monitor.json').write_text(json.dumps({'operations': {
        'workspace_relocation_state_root': str(owner), 'backup_state_root': str(backup)}}))
    (owner / 'retirement-status.json').write_text(json.dumps({
        'state': 'verifying_and_retiring', 'observed_at_epoch': time.time() - 3600}))
    monkeypatch.setattr(storage, 'trading_opening_census_reason', lambda _: None)
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    with (backup / 'cycle.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert storage.local_census_priority_reason({'id': 'penguin-workspace'}, tmp_path) == 'transport_relocation_owner_active'
        # The read-only observation did not release or replace the original lock.
        with (backup / 'cycle.lock').open('r') as probe, pytest.raises(BlockingIOError):
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert storage.local_census_priority_reason({'id': 'penguin-workspace'}, tmp_path) is None


def test_local_census_observes_existing_dataset_owner_lock_without_disturbing_it(tmp_path, monkeypatch):
    lock = tmp_path / 'owner.lock'
    lock.touch()
    monkeypatch.setattr(storage, 'DATASET_VERSION_OWNER_DIRECTORY', tmp_path)
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    with lock.open('r+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert storage.local_census_priority_reason({'id': 'penguin-d'}, tmp_path) == 'dataset_version_owner_active'
        assert storage.local_census_priority_reason({'id': 'penguin-wsl'}, tmp_path) == 'dataset_version_owner_active'
        assert storage.local_census_priority_reason({'id': 'penguin-c'}, tmp_path) is None
        with lock.open('r+') as contender, pytest.raises(BlockingIOError):
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert storage.local_census_priority_reason({'id': 'penguin-d'}, tmp_path) is None
    lock.unlink()
    assert storage.local_census_priority_reason({'id': 'penguin-d'}, tmp_path) is None
    assert not lock.exists(), 'a monitoring probe must not create an owner lock'


def test_d_census_defers_if_dataset_owner_cannot_be_observed(tmp_path, monkeypatch):
    from stockagent.data_sync import dataset_migration
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    def inaccessible(*_):
        raise PermissionError('owner cannot be read')
    monkeypatch.setattr(dataset_migration, 'migration_owner_observation', inaccessible)
    assert storage.local_census_priority_reason({'id': 'penguin-d'}, tmp_path) == 'dataset_version_owner_unobservable'
    assert storage.local_census_priority_reason({'id': 'penguin-wsl'}, tmp_path) == 'dataset_version_owner_unobservable'
    assert storage.local_census_priority_reason({'id': 'penguin-c'}, tmp_path) is None


@pytest.mark.parametrize('emit_record', [True, False])
def test_d_scan_yields_to_new_publication_preserving_partial_observation(tmp_path, monkeypatch, emit_record):
    import sys
    source = tmp_path / 'source'
    source.mkdir()
    wave_lock = tmp_path / 'scheduler.lock'
    wave_lock.touch()
    child_pid = tmp_path / 'child.pid'
    roots = [{'path': str(source), 'label': 'project', 'kind': 'source'}]
    record = ('12\t' + str(source / 'done') + chr(0) + '900\tunfinished').encode()
    code = ("import os,time\n" + f"open({str(child_pid)!r},'w').write(str(os.getpid()))\n"
            + (f"os.write(1, {record!r})\n" if emit_record else '') + "time.sleep(30)\n")
    monkeypatch.setattr(storage, 'capacity', lambda *_: measured())
    monkeypatch.setattr(storage, 'resolve_roots', lambda *_: roots)
    monkeypatch.setattr(storage, 'du_command', lambda *_: [sys.executable, '-c', code])
    monkeypatch.setattr(storage, 'CENSUS_PRIORITY_POLL_SECONDS', 0.1)
    monkeypatch.setattr(storage, 'local_census_priority_reason',
                        lambda *_: 'dataset_version_owner_active' if child_pid.exists() else None)
    result = storage.census({'id': 'penguin-d', 'path': str(tmp_path)}, tmp_path,
                            depth=1, timeout=10, wave_lock=wave_lock)
    assert result['reason'] == 'dataset_version_owner_active'
    if emit_record:
        assert result['state'] == 'partial' and result['preempted'] is True
        assert result['allocated_bytes'] == 12 and result['allocated_bytes_is_lower_bound']
    else:
        assert result['state'] == 'deferred'
    with pytest.raises(ProcessLookupError):
        os.kill(int(child_pid.read_text()), 0)


def test_public_classification_only_exposes_known_deferral_reasons():
    raw = fixture_status()
    raw['volumes'][0]['classification'] = {'state': 'partial', 'tree': [],
                                            'deferred_reason': 'dataset_version_owner_active'}
    result = volume(storage.public_status(raw), 'penguin-wsl')['classification']
    assert result['deferred_reason'] == 'dataset_version_owner_active'
    for reason in ('tw_opening_guard_active', 'tw_opening_guard_unknown'):
        raw['volumes'][0]['classification']['deferred_reason'] = reason
        result = volume(storage.public_status(raw), 'penguin-wsl')['classification']
        assert result['deferred_reason'] == reason
    raw['volumes'][0]['classification']['deferred_reason'] = '/private/secret'
    result = volume(storage.public_status(raw), 'penguin-wsl')['classification']
    assert result['deferred_reason'] is None
    raw['volumes'][0]['classification']['deferred_reason'] = ['unexpected']
    result = volume(storage.public_status(raw), 'penguin-wsl')['classification']
    assert result['deferred_reason'] is None


def test_monitor_keeps_capacity_and_resumes_d_census_after_publication(tmp_path, monkeypatch):
    import time
    from threading import Event
    monkeypatch.setenv('WSL_DISTRO_NAME', 'test-distro')
    lock = tmp_path / 'owner.lock'
    lock.touch()
    owner = lock.open('r+')
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    monkeypatch.setattr(storage, 'DATASET_VERSION_OWNER_DIRECTORY', tmp_path)
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: False)
    config = {'schema_version': 1, 'observer_node_id': 'penguin', 'capacity_interval_seconds': 60,
              'classification_interval_seconds': 21600, 'classification_timeout_seconds': 60,
              'classification_depth': 2, 'volumes': [{'id': 'penguin-d', 'path': str(tmp_path), 'roots': []}]}
    config_path, output = tmp_path / 'config.json', tmp_path / 'status.json'
    config_path.write_text(json.dumps(config))
    output.write_text(json.dumps({'contract': storage.CONTRACT, 'volumes': [{'id': 'penguin-d',
        'classification': {'state': 'partial', 'allocated_bytes': 123, 'tree': [],
            'reason': 'previous_census_interrupted', 'observed_at_utc': storage.utc(),
            'fingerprint': storage.census_fingerprint(config, config['volumes'][0], None)}}]}))
    monkeypatch.setattr(storage, 'observe_capacities', lambda _: {'penguin-d': measured(observed_at_utc=storage.utc())})
    calls, snapshots, started = [], [], Event()
    def run_census(*args, **kwargs):
        calls.append(kwargs['previous'])
        started.set()
        return {'state': 'complete', 'tree': [], 'observed_at_utc': storage.utc()}
    monkeypatch.setattr(storage, 'census', run_census)
    clock = [time.time()]
    class Finished(Exception):
        pass
    def tick(_):
        snapshots.append(json.loads(output.read_text()))
        if len(snapshots) == 1:
            owner.close()
            clock[0] += 61
        else:
            assert started.wait(2)
            raise Finished
    monkeypatch.setattr(storage, 'time', SimpleNamespace(time=lambda: clock[0], monotonic=time.monotonic, sleep=tick))
    try:
        with pytest.raises(Finished):
            storage.monitor(config_path, output, tmp_path, watch=True)
    finally:
        owner.close()
    first = snapshots[0]['volumes'][0]
    assert first['capacity']['state'] == 'measured'
    assert first['classification']['deferred_reason'] == 'dataset_version_owner_active'
    assert first['classification']['allocated_bytes'] == 123
    assert snapshots[1]['volumes'][0]['classification']['state'] == 'scanning'
    assert 'deferred_reason' not in snapshots[1]['volumes'][0]['classification']
    assert len(calls) == 1


def test_runnable_cleanup_defers_census_but_capacity_wait_does_not(tmp_path, monkeypatch):
    from scripts import plan_wsl_retirement_waves as planner
    run_id = 'safe-wave'
    path = tmp_path / 'artifacts/operations/agent-workflow/runs' / run_id / 'run.log'
    path.parent.mkdir(parents=True)
    run = {'run_id': run_id, 'argv': ['scripts/run_wsl_retirement_waves.py', '--apply']}
    monkeypatch.setattr(planner, 'observe_active_supervisors', lambda: [run])
    path.write_text('{"event":"waiting_for_capacity"}\n')
    assert not storage.priority_retirement_wave_pending(tmp_path)
    path.write_text('{"event":"waiting_for_scheduler"}\n')
    assert storage.priority_retirement_wave_pending(tmp_path)
    path.unlink()
    assert storage.priority_retirement_wave_pending(tmp_path)


def test_timer_queue_waiting_for_wave_preempts_census_without_an_agent_run(tmp_path, monkeypatch):
    from scripts import plan_wsl_retirement_waves as planner
    monkeypatch.setattr(planner, 'observe_active_supervisors', lambda: [])
    config = tmp_path/'configs/data_sync/wsl_retirement_queue.json'
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({'contract': 'stockagent.frozen_retirement_queue.v1', 'enabled': True,
        'state_root': 'service_data/storage-retirement'}))
    status = tmp_path/'service_data/storage-retirement/status.json'
    status.parent.mkdir(parents=True)
    current = {'contract': 'stockagent.frozen_retirement_queue.v1', 'all_enrolled_accepted': False,
        'observed_at_utc': storage.utc(), 'state': 'waiting_existing_wave'}
    status.write_text(json.dumps(current))
    assert storage.priority_retirement_wave_pending(tmp_path)
    status.write_text(json.dumps({**current, 'state': 'waiting_capacity'}))
    assert not storage.priority_retirement_wave_pending(tmp_path)
    status.write_text(json.dumps({**current, 'all_enrolled_accepted': True}))
    assert not storage.priority_retirement_wave_pending(tmp_path)
    status.write_text(json.dumps({**current, 'observed_at_utc': '2026-10-25T07:00:00+00:00'}))
    assert not storage.priority_retirement_wave_pending(tmp_path)


def test_capacity_continues_while_wsl_census_is_deferred(tmp_path, monkeypatch):
    # This capacity/census test must not invoke live WSL probes whose retry
    # sleeps would be intercepted by the controlled one-tick loop below.
    import stockagent.data_sync.storage_runtime as runtime
    monkeypatch.setattr(runtime,'collect',lambda previous,*a,**k:previous)
    monkeypatch.setenv('WSL_DISTRO_NAME', 'test-distro')
    config = {'schema_version': 1, 'observer_node_id': 'penguin', 'capacity_interval_seconds': 60,
              'classification_interval_seconds': 21600, 'classification_timeout_seconds': 60,
              'classification_depth': 2, 'volumes': [{'id': 'penguin-wsl', 'path': str(tmp_path), 'roots': []}]}
    config_path, output = tmp_path / 'config.json', tmp_path / 'status.json'
    config_path.write_text(json.dumps(config))
    output.write_text(json.dumps({'contract': storage.CONTRACT, 'volumes': [{'id': 'penguin-wsl',
        'classification': {'state': 'scanning', 'allocated_bytes': 123, 'tree': [],
                           'observed_at_utc': '2026-10-25T07:00:00+00:00'}}]}))
    monkeypatch.setattr(storage, 'observe_capacities', lambda _: {'penguin-wsl': measured(observed_at_utc=storage.utc())})
    monkeypatch.setattr(storage, 'priority_retirement_wave_pending', lambda _: True)
    monkeypatch.setattr(storage, 'census', lambda *a, **k: pytest.fail('must defer the WSL census'))
    class Finished(Exception): pass
    monkeypatch.setattr(storage.time, 'sleep', lambda _: (_ for _ in ()).throw(Finished()))
    with pytest.raises(Finished):
        storage.monitor(config_path, output, tmp_path, watch=True)
    row = json.loads(output.read_text())['volumes'][0]
    assert row['capacity']['used_bytes'] == 8000
    assert row['classification']['state'] == 'partial'
    assert row['classification']['allocated_bytes'] == 123
    assert row['classification']['observed_at_utc'] == '2026-10-25T07:00:00+00:00'


def test_api_is_snapshot_only_and_member_boundary_is_preserved(tmp_path, monkeypatch):
    from test_public_dashboards import _test_server
    from stockagent.live.dashboard_access import permitted_path
    server = _test_server()
    server.repo_root = tmp_path
    snapshot = tmp_path / "artifacts/live/data_monitor/storage_status.json"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_text(json.dumps(fixture_status()))
    monkeypatch.setattr(storage, "capacity", lambda *_a, **_k: pytest.fail("API sampled a disk"))
    monkeypatch.setattr(storage, "remote_capacity", lambda *_a, **_k: pytest.fail("API opened SSH"))
    try:
        data = json.loads(server.data_monitor_storage().body)
        assert volume(data, "penguin-wsl")["capacity"]["used_bytes"] == 8000
        assert not permitted_path("/data-monitor/api/storage", audience="public")
        assert permitted_path("/data-monitor/api/storage", audience="member")
    finally:
        server.server_close()


@pytest.mark.parametrize("attempt", ["missing", "malformed", "current"])
def test_capacity_tick_keeps_last_tier_clock_when_auxiliary_probe_fails(tmp_path, monkeypatch, attempt):
    monkeypatch.setenv("WSL_DISTRO_NAME", "test-distro")
    config = {"schema_version": 1, "observer_node_id": "penguin", "capacity_interval_seconds": 60,
        "classification_interval_seconds": 21600, "classification_timeout_seconds": 60, "classification_depth": 2,
        "volumes": [{"id": "penguin-wsl", "path": str(tmp_path), "roots": []}]}
    path, output = tmp_path / "config.json", tmp_path / "status.json"
    path.write_text(json.dumps(config))
    previous = {"contract": "stockagent.cold_tiers.v1", "observed_at_epoch": 1000,
                "state": "outside_daily_window"}
    output.write_text(json.dumps({"contract": storage.CONTRACT, "volumes": [], "cold_tiers": previous}))
    original = storage.read_snapshot
    def read(selected):
        if selected == Path("/var/lib/stockagent-cold-tiers/status.json"):
            if attempt == "missing":
                raise FileNotFoundError(selected)
            return {**previous, "observed_at_epoch": 2000} if attempt == "current" else {"invalid": True}
        return original(selected)
    monkeypatch.setattr(storage, "read_snapshot", read)
    monkeypatch.setattr(storage, "observe_capacities", lambda _: {"penguin-wsl": measured()})
    storage.monitor(path, output, tmp_path)
    value = json.loads(output.read_bytes())
    assert value["volumes"][0]["capacity"]["used_bytes"] == 8000
    assert value["cold_tiers"]["observed_at_epoch"] == (2000 if attempt == "current" else 1000)


def test_receiver_storage_receipt_must_match_paired_readiness_identity(tmp_path):
    from scripts.backup_delivery_receipt import signed, READY
    pairing = {"producer_device_id": "penguin", "receiver_device_id": "lab203", "repository_id": "a" * 64}
    ready = signed({"contract": READY, **pairing, "ingress_free_bytes": 1000, "nas_free_bytes": 9000,
        "reserve_bytes": 20, "maximum_batch_bytes": 200, "repository_check_mode": "full_read_data",
        "single_owner_verified": True, "nas_mount_guard_verified": True, "runtime_lock_verified": True,
        "automatic_pruning": False, "automatic_batch_deletion": False, "observed_at_utc": storage.utc()})
    (tmp_path / "readiness.json").write_text(json.dumps(ready))
    cap = measured(source="guarded_nas_statvfs")
    group = {"state": "complete", "observed_at_utc": storage.utc(), "allocated_bytes": 500, "tree": []}
    telemetry = {"contract": storage.CONTRACT, "observer_node_id": "lab203", "pairing": pairing,
                 "volumes": [{"id": "nas-disk", "capacity": cap, "classification": group}]}
    snapshot = tmp_path / "storage-status.json"
    snapshot.write_text(json.dumps(signed(telemetry)))
    assert storage.receipt_capacities(tmp_path)["nas-disk"]["total_bytes"] == 10000
    assert storage.receipt_classifications(tmp_path)["nas-disk"]["allocated_bytes"] == 500
    telemetry["pairing"] = {**pairing, "receiver_device_id": "another-receiver"}
    snapshot.write_text(json.dumps(signed(telemetry)))
    assert storage.receipt_capacities(tmp_path)["nas-disk"]["state"] == "partial"
    assert storage.receipt_classifications(tmp_path) == {}


def test_receiver_capacity_cycle_signs_host_metrics_in_same_paired_receipt(tmp_path, monkeypatch):
    from scripts.backup_delivery_receipt import valid_signature
    from stockagent.data_sync import storage_runtime
    pairing = {"producer_device_id": "penguin", "receiver_device_id": "lab203", "repository_id": "a" * 64}
    config = {"schema_version": 1, "observer_node_id": "lab203", "capacity_interval_seconds": 60,
        "classification_interval_seconds": 21600, "classification_timeout_seconds": 60,
        "classification_depth": 2, "pairing": pairing,
        "volumes": [{"id": "lab203-disk", "path": str(tmp_path), "roots": []}]}
    path, output = tmp_path / "config.json", tmp_path / "storage-status.json"
    path.write_text(json.dumps(config))
    monkeypatch.setattr(storage, "observe_capacities", lambda _: {"lab203-disk": measured()})
    monkeypatch.setattr(storage_runtime, "receiver_host_observation", lambda *_a, **_k: (
        {"cpu_sample": {"boot": "private"}}, {"contract": storage_runtime.HOST_RUNTIME_CONTRACT,
         "observed_at_utc": storage.utc(), "metrics": {"cpu_count": 4}}))
    storage.monitor(path, output, tmp_path)
    receipt = json.loads(output.read_text())
    valid_signature(receipt, storage.CONTRACT)
    assert receipt["pairing"] == pairing and receipt["host_runtime"]["metrics"]["cpu_count"] == 4
    assert "private" not in json.dumps(receipt)


def test_bad_readiness_checksum_cannot_claim_nas_free_bytes(tmp_path):
    (tmp_path / "readiness.json").write_text(json.dumps({"contract": "nas_backup_relay_readiness_v1", "nas_free_bytes": 99999}))
    assert storage.receipt_capacities(tmp_path) == {}


def test_receiver_config_reuses_existing_roots_and_only_adds_read_only_service():
    from scripts.lab203_storage_monitor import receiver_config, unit_body
    row = {"producer_device_id": "a", "receiver_device_id": "b", "repository_id": "c", "ingress_root": "/srv/lab203-backup/ingress",
        "state_root": "/srv/lab203-backup/state", "scratch_root": "/var/lib/lab203-scratch", "nas_configuration": "/etc/stockagent/nas.json"}
    config = receiver_config(row)
    assert {r["id"] for r in config["volumes"]} == {"lab203-disk", "nas-disk"}
    body = unit_body(Path("/srv/receipts"), code=Path("/opt/stock agent"), python="/opt/runtime/python")
    assert "ProtectSystem=strict" in body
    assert "ReadWritePaths=\"/srv/receipts\"" in body
    assert "lab203-backup.service" not in body
    assert "--watch" in body
