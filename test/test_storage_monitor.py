import fcntl
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from stockagent.data_sync import storage_monitor as storage


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
        assert kwargs["timeout"] == 15
        raise subprocess.TimeoutExpired("private-secret", 15, stderr="private-secret")
    monkeypatch.setattr(storage.subprocess, "run", timeout)
    result = storage.remote_capacity({})
    assert result["state"] == "unavailable"
    assert "private" not in json.dumps(result)


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

    monkeypatch.setenv('WSL_DISTRO_NAME', 'test-distro')
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


def test_capacity_continues_while_wsl_census_is_deferred(tmp_path, monkeypatch):
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
