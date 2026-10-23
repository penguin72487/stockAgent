from __future__ import annotations

import importlib.util
import argparse
import fcntl
import json
import os
import shlex
import select
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts.bybit_refresh_inputs import materialization_input_signature
import stockagent.data_sync.packed_snapshots as packed_snapshots

from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot,
    initialize_packed_layout,
    publish_packed_snapshot,
    verify_packed_snapshot,
)


def test_writer_gate_uses_script_argv_not_waiting_shell_text():
    module = _module()
    entry = {"active_process_substrings": ["download_tw_corporate_action_entitlements.py"]}
    program = "python downloader/download_tw_corporate_action_entitlements.py --mode repair"
    commands = [(1, shlex.join(["bash", "-c", program])),
                (2, shlex.join(["python", "downloader/download_tw_corporate_action_entitlements.py", "--mode", "repair"]))]
    assert [r["pid"] for r in module._blockers(entry, commands)] == [2]


def test_catalog_root_selection_uses_original_publisher_without_whole_root_copy(tmp_path, monkeypatch, capsys):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    names = ["accepted.forward", "accepted.receipt.json"]
    for name in names:
        (source / name).write_bytes((name + " original bytes").encode())
    (source / "unique-unselected-source").write_bytes(b"must not publish implicitly")
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [{
        "dataset": "selected-results", "source": str(source), "role": "exact completed result fixture",
        "publish": True, "note": "selected source only, not whole namespace completeness",
        "pack_buckets": 1, "loose_threshold_mib": 8, "included_root_files": names,
        "source_coordination_lock": "source/.owner.lock",
    }]}))
    cold = tmp_path / "cold"
    assert module.main(["publish", "selected-results", "--catalog", str(catalog),
                        "--sync-root", str(cold), "--node-id", "test-node"]) == 0
    result = json.loads(capsys.readouterr().out)["published"][0]
    fixed = packed_snapshots.resolve_packed_snapshot_id(cold, result["dataset"], result["snapshot_id"])
    target = fetch_packed_snapshot(cold, tmp_path / "materialized", fixed)
    assert sorted(p.name for p in target.iterdir()) == names
    assert all((target / name).read_bytes() == (source / name).read_bytes() for name in names)
    status = module._status(module._load_catalog(catalog)[0], [])
    assert status["publish_ready"] and status["source_selection"]["scope"] == "explicit_root_files_only"
    assert (source / "unique-unselected-source").read_bytes() == b"must not publish implicitly"


@pytest.mark.parametrize("selection", [None, [], "one.forward", ["nested/file"], ["same", "same"]])
def test_catalog_rejects_implicit_or_ambiguous_root_selection(tmp_path, selection):
    module = _module()
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [{
        "dataset": "selected-results", "included_root_files": selection}]}))
    with pytest.raises(SnapshotError):
        module._load_catalog(catalog)


def test_catalog_status_refuses_missing_or_redirected_selected_members(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    entry = {"dataset": "selected-results", "source": str(source), "role": "fixture",
             "publish": True, "note": "fixture", "included_root_files": ["result.forward"]}
    status = module._status(entry, [])
    assert not status["publish_ready"] and status["selection_error"]
    (source / "other").write_bytes(b"not the selected file")
    (source / "result.forward").symlink_to("other")
    status = module._status(entry, [])
    assert not status["publish_ready"] and "not regular" in status["selection_error"]
    assert (source / "result.forward").is_symlink()


def test_writer_gate_also_recognizes_python_module_argv():
    module = _module()
    entry = {"active_process_substrings": ["build_shioaji_tw_minute_dataset.py"]}
    commands = [(1, "python -m scripts.build_shioaji_tw_minute_dataset --output-root data"),
                (2, "bash -c 'python -m scripts.build_shioaji_tw_minute_dataset'")]
    assert [r["pid"] for r in module._blockers(entry, commands)] == [1]


def test_all_ready_failure_is_isolated_from_other_datasets(tmp_path, monkeypatch, capsys):
    module = _module()
    entries = [{"dataset": "broken"}, {"dataset": "good"}]
    monkeypatch.setattr(module, "_load_catalog", lambda *_: entries)
    source = _source(tmp_path, "2026-10-06")
    release = publish_packed_snapshot(tmp_path/'cold', 'good', source, node_id='test-node')
    def publish(entry, *_args):
        if entry['dataset'] == 'broken':
            raise SnapshotError('source validation rejected')
        return {'publish_ready': True}, release
    monkeypatch.setattr(module, '_publish_entry', publish)
    assert module.main(['publish', '--all-ready', '--sync-root', str(tmp_path/'cold'),
                        '--node-id', 'test-node']) == 0
    result = json.loads(capsys.readouterr().out)
    assert [r['dataset'] for r in result['published']] == ['good']
    assert result['skipped'][0]['validation_or_publication_error'] == 'source validation rejected'


def test_all_ready_defers_busy_source_without_sleeping(tmp_path, monkeypatch, capsys):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = _source(tmp_path, "2026-10-16")
    good = {**_entry(source), "loose_threshold_mib": 8, "pack_buckets": 2}
    busy = {**good, "dataset": "busy", "source_coordination_lock": "busy.lock"}
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [busy, good])
    monkeypatch.setattr(module.time, "sleep", lambda *_: pytest.fail("batch must not wait"))
    with module._source_coordination_lock(busy, wait_seconds=0):
        assert module.main(["publish", "--all-ready", "--sync-root", str(tmp_path / "cold"),
                            "--node-id", "test-node"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert [item["dataset"] for item in result["published"]] == ["prices"]
    skipped = result["skipped"][0]
    assert skipped["retryable"] is True and skipped["dataset"] == "busy"
    assert skipped["committed_head_outcome"] == "not_started_source_lock_busy"
    assert not (tmp_path / "cold/heads/busy").exists()


def test_disabled_publication_never_acquires_source_lock(tmp_path, monkeypatch):
    module = _module()
    status = {"publish_ready": False, "publish": False}
    monkeypatch.setattr(module, "_status", lambda *_a, **_k: status)
    monkeypatch.setattr(module, "_running_commands", lambda: [])
    monkeypatch.setattr(module, "_source_coordination_lock",
                        lambda *_a, **_k: pytest.fail("disabled source has no transaction"))
    actual, result = module._publish_entry({"publish": False}, argparse.Namespace(), tmp_path / "cold")
    assert actual is status and result is None


def test_deferred_batch_checkpoints_each_attempt_without_scanning(tmp_path, monkeypatch, capsys):
    from stockagent.data_sync import syncthing_scan
    module = _module()
    source = _source(tmp_path, "2026-10-16")
    entries = [{**_entry(source), "dataset": name, "loose_threshold_mib": 8, "pack_buckets": 2}
               for name in ("first", "second")]
    monkeypatch.setattr(module, "_load_catalog", lambda *_: entries)
    cold = tmp_path / "cold"
    cold.mkdir()
    (cold / syncthing_scan.D_PRIMARY_MARKER).touch()
    monkeypatch.setattr(syncthing_scan, "CANONICAL_ROOT", cold)
    monkeypatch.setattr(syncthing_scan, "_scan_pending",
                        lambda *_a, **_k: pytest.fail("existing drain owns network scans"))
    receipt = tmp_path / "batch/progress.json"
    observations = []
    write_json = module.atomic_write_json

    def record(path, payload):
        observations.append(json.loads(json.dumps(payload)))
        write_json(path, payload)

    monkeypatch.setattr(module, "atomic_write_json", record)
    assert module.main(["publish", "--all-ready", "--sync-root", str(cold), "--node-id", "test-node",
                        "--defer-scan", "--batch-progress-receipt", str(receipt)]) == 0
    stdout = json.loads(capsys.readouterr().out)
    final = json.loads(receipt.read_text())
    assert final["published"] == stdout["published"] and final["skipped"] == []
    assert final["state"] == "finished" and final["completed_attempts"] == 2
    assert final["active_dataset"] is None and not final["all_dataset_migration_complete"]
    assert final["peer_convergence"] == final["release_restore_acceptance"] == "not_checked"
    assert [item["active_dataset"] for item in observations if item["active_dataset"]] == ["first", "second"]
    assert [item["completed_attempts"] for item in observations] == [0, 0, 1, 1, 2, 2]
    for name in ("first", "second"):
        assert (cold / f".local-state/scan-pending/{name}.json").is_file()


def test_batch_receipt_failure_retains_unknown_outcome_and_queued_commit(tmp_path, monkeypatch, capsys):
    from stockagent.data_sync import syncthing_scan
    module = _module()
    source = _source(tmp_path, "2026-10-16")
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [
        {**_entry(source), "loose_threshold_mib": 8, "pack_buckets": 2}])
    cold = tmp_path / "cold"
    cold.mkdir()
    (cold / syncthing_scan.D_PRIMARY_MARKER).touch()
    monkeypatch.setattr(syncthing_scan, "CANONICAL_ROOT", cold)
    receipt = tmp_path / "batch.json"
    write_json = module.atomic_write_json

    def fail_after_commit(path, payload):
        if payload["published"]:
            raise OSError("post-commit receipt failure")
        write_json(path, payload)

    monkeypatch.setattr(module, "atomic_write_json", fail_after_commit)
    assert module.main(["publish", "--all-ready", "--sync-root", str(cold), "--node-id", "test-node",
                        "--defer-scan", "--batch-progress-receipt", str(receipt)]) == 2
    assert "post-commit receipt failure" in capsys.readouterr().err
    pending = json.loads(receipt.read_text())
    assert pending["active_dataset"] == "prices" and pending["active_outcome"] == "unknown_check_current_head"
    assert pending["state"] == "running" and not pending["published"]
    assert packed_snapshots.resolve_latest_packed(cold, "prices").manifest["snapshot_id"]
    assert (cold / ".local-state/scan-pending/prices.json").is_file()


@pytest.mark.parametrize("args", [["status"], ["publish", "prices"], ["layout"]])
def test_batch_progress_rejects_non_batch_scope(tmp_path, monkeypatch, args):
    module = _module()
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("reject before catalog"))
    assert module.main([*args, "--batch-progress-receipt", str(tmp_path / "new.json")]) == 2


def test_batch_progress_cannot_overwrite_existing_receipt(tmp_path, monkeypatch):
    module = _module()
    receipt = tmp_path / "existing.json"
    original = '{"state":"previous evidence"}'
    receipt.write_text(original)
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("reject before catalog"))
    assert module.main(["publish", "--all-ready", "--batch-progress-receipt", str(receipt)]) == 2
    assert receipt.read_text() == original


@pytest.mark.parametrize("location", ["source", "cold"])
def test_batch_progress_cannot_mutate_selected_source_or_cold_tree(tmp_path, monkeypatch, capsys, location):
    module = _module()
    source = _source(tmp_path, "2026-10-16")
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [_entry(source)])
    monkeypatch.setattr(module, "initialize_packed_layout", lambda *_a, **_k: pytest.fail("reject before cold writes"))
    cold = tmp_path / "cold"
    receipt = (source if location == "source" else cold) / "progress.json"
    assert module.main(["publish", "--all-ready", "--sync-root", str(cold),
                        "--batch-progress-receipt", str(receipt)]) == 2
    assert "outside cold and selected source" in capsys.readouterr().err
    assert not receipt.exists() and not cold.exists()


@pytest.mark.parametrize("cursor,expected", [("first", ["second", "last"]), ("last", [])])
def test_batch_resume_cursor_bounds_attempts_without_claiming_previous_sources(tmp_path, monkeypatch, cursor, expected):
    module = _module()
    source = _source(tmp_path, "2026-10-16")
    entries = [{**_entry(source), "dataset": name} for name in ("first", "second", "last")]
    monkeypatch.setattr(module, "_load_catalog", lambda *_: entries)
    attempts = []

    def publish(entry, *_):
        attempts.append(entry["dataset"])
        return {"dataset": entry["dataset"], "publish_ready": False}, None

    monkeypatch.setattr(module, "_publish_entry", publish)
    receipt = tmp_path / "batch.json"
    assert module.main(["publish", "--all-ready", "--sync-root", str(tmp_path / "cold"), "--node-id", "test-node",
                        "--after-dataset", cursor, "--batch-progress-receipt", str(receipt)]) == 0
    progress = json.loads(receipt.read_text())
    assert attempts == progress["selected_datasets"] == expected
    assert progress["state"] == "finished" and progress["resume_after_dataset"] == cursor
    assert progress["prior_sources_accepted_by_this_receipt"] is False
    assert progress["all_dataset_migration_complete"] is False


def test_unknown_batch_resume_cursor_cannot_initialize_cold_store(tmp_path, monkeypatch, capsys):
    module = _module()
    source = _source(tmp_path, "2026-10-16")
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [_entry(source)])
    monkeypatch.setattr(module, "initialize_packed_layout", lambda *_a, **_k: pytest.fail("invalid cursor wrote state"))
    receipt = tmp_path / "batch.json"
    assert module.main(["publish", "--all-ready", "--sync-root", str(tmp_path / "cold"),
                        "--after-dataset", "absent", "--batch-progress-receipt", str(receipt)]) == 2
    assert "not in the selected catalog scope" in capsys.readouterr().err
    assert not receipt.exists()


def test_batch_resume_receipt_cannot_write_into_skipped_source(tmp_path, monkeypatch, capsys):
    module = _module()
    prior = _source(tmp_path, "2026-10-15")
    later = _source(tmp_path, "2026-10-16")
    entries = [{**_entry(prior), "dataset": "prior"}, {**_entry(later), "dataset": "later"}]
    monkeypatch.setattr(module, "_load_catalog", lambda *_: entries)
    monkeypatch.setattr(module, "initialize_packed_layout", lambda *_a, **_k: pytest.fail("receipt wrote source"))
    receipt = prior / "progress.json"
    assert module.main(["publish", "--all-ready", "--sync-root", str(tmp_path / "cold"),
                        "--after-dataset", "prior", "--batch-progress-receipt", str(receipt)]) == 2
    assert "outside cold and selected source" in capsys.readouterr().err
    assert not receipt.exists()


@pytest.mark.parametrize("args", [["status"], ["publish", "prices"], ["publish", "--all-ready"]])
def test_resume_cursor_requires_durable_batch_scope_before_any_io(args, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("invalid resume read catalog"))
    assert module.main([*args, "--after-dataset", "prior"]) == 2


def test_bybit_release_keeps_data_but_excludes_rebuildable_panel_cache(
    tmp_path: Path,
) -> None:
    module = _module()
    catalog = Path(__file__).resolve().parents[1] / "configs/data_sync/packed_datasets.json"
    entry = next(
        item for item in module._load_catalog(catalog) if item["dataset"] == "bybit"
    )
    assert entry["excluded_subtrees"] == ["perpetual_daily/panel_cache_v2"]
    assert entry["source_coordination_lock"] == (
        "artifacts/daily_downloader/bybit_source_publish.lock"
    )
    runner = (catalog.parents[2] / "downloader/run_daily_all_markets.sh").read_text()
    assert "artifacts/daily_downloader/bybit_source_publish.lock" in runner
    assert 'flock -w 180 "$bybit_lock_fd"' in runner

    source = tmp_path / "data_bybit"
    daily = source / "perpetual_daily"
    cache = daily / "panel_cache_v2"
    raw = source / "1m"
    cache.mkdir(parents=True)
    raw.mkdir()
    (raw / "BTCUSDT_features.parquet").write_bytes(b"raw source")
    (daily / "BTCUSDT_features.parquet").write_bytes(b"derived daily")
    (cache / "features.npy").write_bytes(b"rebuildable cache")
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")

    release = publish_packed_snapshot(
        cold,
        "bybit",
        source,
        excluded_subtrees=entry["excluded_subtrees"],
    )
    target = fetch_packed_snapshot(cold, tmp_path / "materialized", release)

    assert release.manifest["source"]["excluded_subtrees"] == entry["excluded_subtrees"]
    assert (target / "1m/BTCUSDT_features.parquet").read_bytes() == b"raw source"
    assert (target / "perpetual_daily/BTCUSDT_features.parquet").read_bytes() == b"derived daily"
    assert not (target / "perpetual_daily/panel_cache_v2").exists()
    assert verify_packed_snapshot(cold, release, materialized_path=target)[
        "materialized_verified"
    ]


def test_catalog_publish_holds_source_lock_through_snapshot(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "data_bybit"
    source.mkdir()
    (source / "source.txt").write_text("data")
    lock_relative = "artifacts/daily_downloader/bybit_source_publish.lock"
    entry = {
        "dataset": "bybit",
        "source": str(source),
        "role": "training",
        "publish": True,
        "note": "test",
        "active_process_substrings": [],
        "source_coordination_lock": lock_relative,
        "loose_threshold_mib": 8,
        "pack_buckets": 16,
    }
    observed = []

    def fake_publish(*_args, **_kwargs):
        lock_path = tmp_path / lock_relative
        with lock_path.open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed.append("published_under_lock")
        return object()

    monkeypatch.setattr(module, "publish_packed_snapshot", fake_publish)
    args = argparse.Namespace(node_id=None, recover_missing_base_objects=False)
    status, result = module._publish_entry(entry, args, tmp_path / "cold")
    assert status["publish_ready"] and result is not None
    assert observed == ["published_under_lock"]
    with (tmp_path / lock_relative).open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(other, fcntl.LOCK_UN)


@pytest.mark.parametrize("provider", ["okx", "bybit", "binance"])
def test_publication_borrows_native_collector_and_materializer_owner(tmp_path, monkeypatch, provider):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / ("data_" + provider)
    for grain in ("1m", "daily"):
        (source / grain).mkdir(parents=True)
    entry = {**_entry(source), "dataset": provider, "loose_threshold_mib": 8, "pack_buckets": 1}
    entry.pop("freshness")
    if provider == "bybit":
        entry["source_coordination_lock"] = "artifacts/daily_downloader/bybit_source_publish.lock"
    monkeypatch.setattr(module, "_running_commands", lambda: [])
    observed = []
    def pack(*_args, **_options):
        for grain in ("1m", "daily"):
            with (source / grain / ".download.lock").open("a+b") as other:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed.append(True)
        return object()
    monkeypatch.setattr(module, "publish_packed_snapshot", pack)
    args = argparse.Namespace(node_id=None, recover_missing_base_objects=False, all_ready=True)
    status, result = module._publish_entry(entry, args, tmp_path / "cold")
    assert status["publish_ready"] and result is not None and observed == [True]
    assert not (source / "15m").exists(), "publication must not invent another source grain"
    for grain in ("1m", "daily"):
        with (source / grain / ".download.lock").open("a+b") as other:
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_busy_native_writer_yields_without_packing_or_retaining_other_locks(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "data_okx"
    for grain in ("1m", "daily"):
        (source / grain).mkdir(parents=True)
    entry = {**_entry(source), "dataset": "okx", "source_coordination_lock": "outer.lock"}
    monkeypatch.setattr(module.time, "sleep", lambda *_: pytest.fail("all-ready admission slept"))
    with (source / "1m/.download.lock").open("a+b") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX)
        with pytest.raises(module.SourceCoordinationLockTimeout):
            with module._publication_source_locks(entry, wait_seconds=0):
                pytest.fail("admitted a live collector")
        with (tmp_path / "outer.lock").open("a+b") as other:
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_native_lock_mapping_cannot_capture_other_sources_or_redirected_grains(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "data_okx"
    source.mkdir()
    elsewhere = tmp_path / "another-source"
    elsewhere.mkdir()
    assert module._native_source_lock_entries({**_entry(elsewhere), "dataset": "okx"}) == []
    (source / "1m").symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(SnapshotError, match="redirected"):
        module._native_source_lock_entries({**_entry(source), "dataset": "okx"})


def test_native_lock_mapping_keeps_the_enrolled_whole_root_alias(tmp_path, monkeypatch):
    module = _module()
    repo, live = tmp_path / "repo", tmp_path / "live/data_bybit"
    repo.mkdir()
    (live / "1m").mkdir(parents=True)
    alias = repo / "data_bybit"
    alias.symlink_to(live, target_is_directory=True)
    monkeypatch.setattr(module, "REPO_ROOT", repo)
    entry = {**_entry(alias), "dataset": "bybit"}
    with module._publication_source_locks(entry, wait_seconds=0):
        with (live / "1m/.download.lock").open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize("provider", ["binance", "okx", "bybit"])
@pytest.mark.parametrize("writer_kind", ["original", "custom_output", "unknown_script"])
def test_native_writer_admission_requires_live_original_inode(
        tmp_path, monkeypatch, provider, writer_kind):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / ("data_" + provider)
    (source / "1m").mkdir(parents=True)
    script_dir = tmp_path / "downloader"
    script_dir.mkdir()
    known_name = "download_" + provider + "_perp_1m.py"
    script = script_dir / ("unknown_writer.py" if writer_kind == "unknown_script" else known_name)
    script.write_text("import fcntl,sys\nwith open(sys.argv[1], 'a+b') as lock:\n"
                      " print('opened-original-lock',flush=True)\n"
                      " fcntl.flock(lock,fcntl.LOCK_EX)\n")
    entry = {**_entry(source), "dataset": provider,
             "active_process_substrings": [known_name, "unknown_writer.py"]}
    entry.pop("freshness")
    with module._publication_source_locks(entry, wait_seconds=0) as owners:
        lock = source / ("custom-output.lock" if writer_kind == "custom_output" else "1m/.download.lock")
        argv = [sys.executable, str(script), str(lock)]
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, text=True)
        try:
            assert select.select([child.stdout], [], [], 5)[0], "collector did not reach its lock"
            assert child.stdout.readline().strip() == "opened-original-lock"
            commands = [(child.pid, shlex.join(argv))]
            ordinary = module._status(entry, commands)
            assert not ordinary["publish_ready"] and ordinary["active_blockers"]
            admitted = module._status(entry, commands, held_native_lock_fds=owners["native_lock_fds"])
            if writer_kind == "original":
                assert admitted["publish_ready"] and not admitted["active_blockers"]
                assert admitted["coordinated_writer_waiters"][0]["pid"] == child.pid
                assert child.poll() is None, "native waiter escaped the publisher lock"
                # Being a waiting writer never excuses a missing source proof.
                with_missing_receipt = {**entry, "freshness": {"receipt": "missing.json", "field": "date"}}
                rejected = module._status(with_missing_receipt, commands,
                                          held_native_lock_fds=owners["native_lock_fds"])
                assert not rejected["publish_ready"] and rejected["freshness_error"]
            else:
                assert not admitted["publish_ready"] and admitted["active_blockers"]
                assert not admitted.get("coordinated_writer_waiters")
        finally:
            if child.poll() is None:
                child.terminate()
            child.communicate(timeout=5)


def test_native_waiter_status_without_source_keeps_missing_source_status(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    entry = {**_entry(tmp_path / "data_okx"), "dataset": "okx"}
    entry.pop("freshness")
    status = module._status(entry, [])
    assert not status["publish_ready"] and not status["source_exists"]


def test_catalog_publish_rechecks_writer_after_acquiring_lock(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "data_bybit"
    source.mkdir()
    entry = {
        "dataset": "bybit",
        "source": str(source),
        "role": "training",
        "publish": True,
        "note": "test",
        "active_process_substrings": ["download_bybit_perp_1m.py"],
        "source_coordination_lock": "artifacts/bybit.lock",
    }
    monkeypatch.setattr(
        module,
        "_running_commands",
        lambda: [(123, "python downloader/download_bybit_perp_1m.py")],
    )
    monkeypatch.setattr(
        module,
        "publish_packed_snapshot",
        lambda *_args, **_kwargs: pytest.fail("must not publish an active writer"),
    )
    args = argparse.Namespace(node_id=None, recover_missing_base_objects=False)
    status, result = module._publish_entry(entry, args, tmp_path / "cold")
    assert result is None
    assert not status["publish_ready"]
    assert status["active_blockers"][0]["pid"] == 123


def test_source_parent_lock_binds_external_live_root_and_borrowed_descriptor(tmp_path, monkeypatch):
    module = _module()
    repo = tmp_path / "repo"
    repo.mkdir()
    source = tmp_path / "live/data_tw_public"
    source.mkdir(parents=True)
    monkeypatch.setattr(module, "REPO_ROOT", repo)
    entry = {"dataset": "tw-public", "source": str(source),
             "source_coordination_lock": ".locks/tw-public-refresh.lock",
             "source_coordination_lock_scope": "source_parent"}
    lock_path = source.parent / entry["source_coordination_lock"]
    with module._source_coordination_lock(entry) as fd:
        with module._source_coordination_lock(entry, inherited_fd=fd) as borrowed:
            assert borrowed == fd
            assert os.fstat(fd).st_ino == lock_path.stat().st_ino
        with lock_path.open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with lock_path.open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert not (repo / ".locks").exists()


def test_tw_public_shared_publication_admits_model_reader_but_blocks_writer(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = tmp_path / "live/data_tw_public"
    source.mkdir(parents=True)
    entry = {"dataset": "tw-public", "source": str(source),
             "source_coordination_lock": ".locks/tw-public-refresh.lock",
             "source_coordination_lock_scope": "source_parent",
             "source_coordination_lock_mode": "shared_read"}
    lock = source.parent / entry["source_coordination_lock"]
    with module._source_coordination_lock(entry) as fd:
        with lock.open("a+") as reader:
            fcntl.flock(reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
            with module._source_coordination_lock(entry, inherited_fd=fd) as borrowed:
                assert borrowed == fd
            with lock.open("a+") as writer:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Borrowing must not unlock the original open file description.
        with lock.open("a+") as writer:
            with pytest.raises(BlockingIOError):
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with lock.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_shared_source_borrow_never_downgrades_exclusive_parent_or_accepts_unlocked_fd(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    entry = {"dataset": "tw-public", "source_coordination_lock": "source.lock",
             "source_coordination_lock_mode": "shared_read"}
    lock = tmp_path / "source.lock"
    with lock.open("a+") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with module._source_coordination_lock(entry, inherited_fd=owner.fileno()):
            pass
        with lock.open("a+") as reader:
            with pytest.raises(BlockingIOError):
                fcntl.flock(reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
    with lock.open("a+") as unlocked:
        with pytest.raises(SnapshotError, match="no held flock"):
            with module._source_coordination_lock(entry, inherited_fd=unlocked.fileno()):
                pytest.fail("unlocked descriptor was admitted")


def test_tw_public_shared_source_lease_survives_real_child_inheritance(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    entry = {"dataset": "tw-public", "source_coordination_lock": "source.lock",
             "source_coordination_lock_mode": "shared_read"}
    program = """import json, sys
from pathlib import Path
from scripts import publish_data_releases as publisher
publisher.REPO_ROOT = Path(sys.argv[3])
with publisher._source_coordination_lock(json.loads(sys.argv[1]), inherited_fd=int(sys.argv[2])):
    print('inherited-shared-owner-verified', flush=True)
"""
    with module._source_coordination_lock(entry) as fd:
        child = subprocess.run([sys.executable, "-c", program, json.dumps(entry), str(fd), str(tmp_path)],
                               pass_fds=(fd,), capture_output=True, text=True, timeout=30)
        assert child.returncode == 0, child.stderr
        assert child.stdout.strip() == "inherited-shared-owner-verified"
        with (tmp_path / "source.lock").open("a+") as writer:
            with pytest.raises(BlockingIOError):
                fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize("dataset,mode", [("bybit", "shared_read"), ("tw-public", "unknown")])
def test_shared_source_lock_scope_is_explicit_not_a_global_policy(tmp_path, monkeypatch, dataset, mode):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    with pytest.raises(SnapshotError):
        with module._source_coordination_lock({"dataset": dataset, "source_coordination_lock": "source.lock",
                                               "source_coordination_lock_mode": mode}):
            pytest.fail("unsupported read lease scope was admitted")


def test_source_parent_lock_rejects_redirected_path_and_unknown_scope(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path / "repo")
    source = tmp_path / "live/data_tw_public"
    source.mkdir(parents=True)
    (source.parent / ".locks").symlink_to(tmp_path, target_is_directory=True)
    entry = {"dataset": "tw-public", "source": str(source),
             "source_coordination_lock": ".locks/tw-public-refresh.lock",
             "source_coordination_lock_scope": "source_parent"}
    with pytest.raises(SnapshotError, match="leaves source_parent"):
        with module._source_coordination_lock(entry):
            pytest.fail("redirected lock")
    with pytest.raises(SnapshotError, match="invalid.*scope"):
        with module._source_coordination_lock({**entry, "source_coordination_lock_scope": "unknown"}):
            pytest.fail("unknown lock scope")


def test_catalog_publish_uses_inherited_lock_without_reacquiring(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(module, "SOURCE_LOCK_WAIT_SECONDS", 0)
    source = _source(tmp_path, "2026-08-18")
    entry = {
        **_entry(source), "source_coordination_lock": "source.lock",
        "loose_threshold_mib": 8, "pack_buckets": 16,
    }
    result = object()
    monkeypatch.setattr(module, "publish_packed_snapshot", lambda *_a, **_kw: result)
    with module._source_coordination_lock(entry) as fd:
        args = argparse.Namespace(
            node_id=None, recover_missing_base_objects=False, source_lock_fd=fd,
        )
        status, actual = module._publish_entry(entry, args, tmp_path / "cold")
        assert actual is result and status["publish_ready"]
        with (tmp_path / "source.lock").open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_source_lock_timeout_fails_closed(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(module, "SOURCE_LOCK_WAIT_SECONDS", 0.01)
    entry = {"source_coordination_lock": "artifacts/lock"}
    lock_path = tmp_path / "artifacts/lock"
    lock_path.parent.mkdir()
    with lock_path.open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX)
        with pytest.raises(module.SourceCoordinationLockTimeout, match="timed out waiting"):
            with module._source_coordination_lock(entry):
                pytest.fail("must not enter while writer owns the lock")


def test_source_lock_rejects_redirect_even_inside_original_root(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    original = tmp_path / "original.lock"
    original.touch()
    (tmp_path / "source.lock").symlink_to(original.name)
    with pytest.raises(SnapshotError, match="redirected"):
        with module._source_coordination_lock({"source_coordination_lock": "source.lock"}):
            pytest.fail("a different lock inode cannot replace the original owner")


def test_source_lock_zero_wait_never_sleeps(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(module.time, "sleep", lambda *_: pytest.fail("must not wait"))
    entry = {"dataset": "bybit", "source_coordination_lock": "source.lock"}
    with (tmp_path / "source.lock").open("a+b") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        with pytest.raises(module.SourceCoordinationLockTimeout):
            with module._source_coordination_lock(entry, wait_seconds=0):
                pytest.fail("must defer immediately")


@pytest.mark.parametrize("fail_inside", [False, True])
def test_source_lock_subprocess_borrows_same_description_without_unlocking(
    tmp_path, monkeypatch, fail_inside
):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    entry = {"dataset": "bybit", "source_coordination_lock": "source.lock"}
    script = """
import os
from pathlib import Path
import sys
from scripts import publish_data_releases as publisher
publisher.REPO_ROOT = Path(sys.argv[1])
fd = int(sys.argv[2])
entry = {"dataset": "bybit", "source_coordination_lock": "source.lock"}
try:
    with publisher._source_coordination_lock(entry, inherited_fd=fd) as borrowed:
        assert borrowed == fd
        if sys.argv[3] == "True":
            raise ZeroDivisionError("body failed")
except ZeroDivisionError:
    pass
os.fstat(fd)
"""
    with module._source_coordination_lock(entry, wait_seconds=0) as fd:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path), str(fd), str(fail_inside)],
            cwd=Path(__file__).resolve().parents[1],
            pass_fds=(fd,), capture_output=True, text=True, timeout=10,
        )
        assert completed.returncode == 0, completed.stderr
        os.fstat(fd)
        with (tmp_path / "source.lock").open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with (tmp_path / "source.lock").open("a+b") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_source_lock_owner_exit_retains_lock_until_inherited_fd_closes(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    entry = {"dataset": "bybit", "source_coordination_lock": "source.lock"}
    with module._source_coordination_lock(entry, wait_seconds=0) as fd:
        inherited = os.dup(fd)
    try:
        with (tmp_path / "source.lock").open("a+b") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with module._source_coordination_lock(entry, inherited_fd=inherited):
            pass
        os.fstat(inherited)
    finally:
        os.close(inherited)
    with module._source_coordination_lock(entry, wait_seconds=0):
        pass


@pytest.mark.parametrize("fd", [True, False, -1, 0, 1, 2, 3.0, "3"])
def test_source_lock_rejects_invalid_inherited_descriptor(fd, tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    with pytest.raises(SnapshotError, match="integer >= 3"):
        with module._source_coordination_lock(
            {"source_coordination_lock": "source.lock"}, inherited_fd=fd,
        ):
            pytest.fail("invalid descriptor")


def test_source_lock_rejects_other_inode_and_contended_description(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    entry = {"dataset": "bybit", "source_coordination_lock": "source.lock"}
    with module._source_coordination_lock(entry) as owner_fd:
        with (tmp_path / "other.lock").open("a+b") as other_inode:
            with pytest.raises(SnapshotError, match="does not match"):
                with module._source_coordination_lock(entry, inherited_fd=other_inode.fileno()):
                    pytest.fail("different file must not prove ownership")
        with (tmp_path / "source.lock").open("a+b") as other_description:
            with pytest.raises(SnapshotError, match="cannot borrow"):
                with module._source_coordination_lock(
                    entry, inherited_fd=other_description.fileno(),
                ):
                    pytest.fail("same inode with different description is contended")
            os.fstat(other_description.fileno())
        os.fstat(owner_fd)


def test_source_lock_rejects_nonregular_or_uncatalogued_descriptor(tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    (tmp_path / "source.lock").touch()
    read_fd, write_fd = os.pipe()
    try:
        with pytest.raises(SnapshotError, match="regular file"):
            with module._source_coordination_lock(
                {"source_coordination_lock": "source.lock"}, inherited_fd=read_fd,
            ):
                pytest.fail("pipe is not a source lock")
        with pytest.raises(SnapshotError, match="catalogued lock"):
            with module._source_coordination_lock({}, inherited_fd=read_fd):
                pytest.fail("descriptor cannot bypass catalog")
    finally:
        os.close(read_fd)
        os.close(write_fd)


@pytest.mark.parametrize("wait", [-1, float("nan"), float("inf"), True])
def test_source_lock_rejects_unbounded_wait(wait, tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    with pytest.raises(SnapshotError, match="finite and nonnegative"):
        with module._source_coordination_lock(
            {"source_coordination_lock": "source.lock"}, wait_seconds=wait,
        ):
            pytest.fail("invalid wait")


@pytest.mark.parametrize("args", [
    ["status"], ["status", "bybit"], ["publish", "--all-ready"],
    ["publish"], ["publish", "bybit", "--all-ready"],
])
def test_source_lock_cli_rejects_non_single_publish(args, monkeypatch, capsys):
    module = _module()
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("reject before work"))
    assert module.main([*args, "--source-lock-fd", "7"]) == 2
    assert "one explicit publish dataset" in capsys.readouterr().err


def _bybit_proof_source(root: Path) -> Path:
    source = root / "data_bybit"
    (source / "1m").mkdir(parents=True)
    (source / "funding").mkdir()
    (source / "1m/BTCUSDT_features.parquet").write_bytes(b"raw-before")
    (source / "funding/instruments.csv").write_bytes(b"symbol\nBTCUSDT\n")
    (source / "funding/funding_coverage.csv").write_bytes(b"symbol\nBTCUSDT\n")
    return source


def test_bybit_input_signature_detects_in_place_write_with_restored_mtime(tmp_path):
    source = _bybit_proof_source(tmp_path) / "1m/BTCUSDT_features.parquet"
    before = materialization_input_signature(tmp_path)
    original = source.stat()
    source.write_bytes(b"raw-change")
    os.utime(source, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert source.stat().st_ino == original.st_ino
    assert source.stat().st_size == original.st_size
    assert source.stat().st_mtime_ns == original.st_mtime_ns
    assert materialization_input_signature(tmp_path) != before


@pytest.mark.parametrize("change_stage", ["before", "initial_collect", "final_collect", "pre_head"])
def test_bybit_build_input_change_vetoes_published_head(change_stage, tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = _bybit_proof_source(tmp_path)
    expected = materialization_input_signature(tmp_path)
    entry = {
        "dataset": "bybit", "source": str(source), "role": "training",
        "publish": True, "note": "fixture", "active_process_substrings": [],
        "source_coordination_lock": "bybit.lock", "loose_threshold_mib": 8,
        "pack_buckets": 2,
    }
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    raw = source / "1m/BTCUSDT_features.parquet"

    def change():
        before = raw.stat()
        raw.write_bytes(b"raw-change")
        os.utime(raw, ns=(before.st_atime_ns, before.st_mtime_ns))

    original_collect = packed_snapshots._collect_entries
    collections = 0

    def collect(*args, **kwargs):
        nonlocal collections
        result = original_collect(*args, **kwargs)
        collections += 1
        if (change_stage == "initial_collect" and collections == 1) or (
            change_stage == "final_collect" and collections == 2
        ):
            change()
        return result

    monkeypatch.setattr(packed_snapshots, "_collect_entries", collect)
    if change_stage == "before":
        change()
    elif change_stage == "pre_head":
        monkeypatch.setattr(packed_snapshots, "_archive_d_primary_head", lambda *_: change())
    with module._source_coordination_lock(entry) as fd:
        args = argparse.Namespace(
            node_id=None, recover_missing_base_objects=False, source_lock_fd=fd,
            expected_bybit_input_signature=expected,
        )
        with pytest.raises(SnapshotError, match="materialization inputs changed"):
            module._publish_entry(entry, args, cold)
    assert not list((cold / "heads/bybit").glob("*.json"))


@pytest.mark.parametrize("args", [
    ["status", "bybit"], ["publish", "bybit"],
    ["publish", "prices", "--source-lock-fd", "7"],
    ["publish", "bybit", "--all-ready", "--source-lock-fd", "7"],
])
def test_expected_bybit_proof_rejects_wrong_publication_scope(args, monkeypatch, capsys):
    module = _module()
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("reject before work"))
    proof = json.dumps({"files": 3, "metadata_sha256": "0" * 64})
    assert module.main([*args, "--expected-bybit-input-signature", proof]) == 2
    assert "explicit Bybit publish" in capsys.readouterr().err


@pytest.mark.parametrize("proof", [
    {}, [], {"files": True, "metadata_sha256": "0" * 64},
    {"files": -1, "metadata_sha256": "0" * 64},
    {"files": 3, "metadata_sha256": "not-a-sha"},
])
def test_expected_bybit_proof_rejects_invalid_shape(proof):
    with pytest.raises(SnapshotError, match="invalid expected Bybit"):
        _module()._expected_bybit_signature(proof)


def test_expected_bybit_cli_null_cannot_silently_disable_guard(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("must reject null proof"))
    with pytest.raises(SystemExit) as error:
        module.main(["publish", "bybit", "--source-lock-fd", "7", "--expected-bybit-input-signature", "null"])
    assert error.value.code == 2


@pytest.mark.parametrize("defer", [False, True])
def test_publish_result_receipt_matches_stdout_and_guarded_release(tmp_path, monkeypatch, capsys, defer):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = _bybit_proof_source(tmp_path)
    expected = materialization_input_signature(tmp_path)
    entry = {
        "dataset": "bybit", "source": str(source), "role": "training",
        "publish": True, "note": "fixture", "active_process_substrings": [],
        "source_coordination_lock": "bybit.lock", "loose_threshold_mib": 8,
        "pack_buckets": 2,
    }
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [entry])
    cold = tmp_path / "cold"
    result_path = tmp_path / "results/attempt.json"
    if defer:
        from stockagent.data_sync import syncthing_scan
        cold.mkdir()
        (cold / syncthing_scan.D_PRIMARY_MARKER).touch()
        monkeypatch.setattr(syncthing_scan, "CANONICAL_ROOT", cold)
    with module._source_coordination_lock(entry) as fd:
        assert module.main([
            "publish", "bybit", "--sync-root", str(cold), "--node-id", "node-a",
            "--source-lock-fd", str(fd), "--expected-bybit-input-signature", json.dumps(expected),
            "--result-receipt", str(result_path),
            *(["--defer-scan"] if defer else []),
        ]) == 0
    payload = json.loads(result_path.read_text())
    assert payload == json.loads(capsys.readouterr().out)
    release = packed_snapshots.resolve_latest_packed(cold, "bybit")
    assert payload["published"][0]["snapshot_id"] == release.manifest["snapshot_id"]
    assert payload["published"][0]["inventory_sha256"] == release.manifest["archive"]["inventory"]["sha256"]
    assert release.manifest["metadata"]["bybit_materialization_input_metadata_sha256"] == expected["metadata_sha256"]
    assert payload["skipped"] == []
    assert payload["published"][0]["scan_policy"] == ("durably_queued" if defer else "immediate")
    if defer:
        assert (cold / ".local-state/scan-pending/bybit.json").is_file()


@pytest.mark.parametrize("args", [["status"], ["publish", "bybit"], ["publish", "bybit", "--source-lock-fd", "4"]])
def test_deferred_scan_requires_explicit_owner_and_commit_receipt(args, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "_load_catalog", lambda *_a: pytest.fail("invalid phase contract"))
    assert module.main([*args, "--defer-scan"]) == 2


@pytest.mark.parametrize("native", [False, True])
def test_deferred_single_publish_can_acquire_original_owner_without_inherited_fd(
    tmp_path, monkeypatch, capsys, native,
):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    source = _source(tmp_path, "2026-10-19")
    lock_entry = {"dataset": "prices", "source_coordination_lock": "original-stage.lock"}
    entry = {**_entry(source), "loose_threshold_mib": 8, "pack_buckets": 2,
             **({} if native else lock_entry)}
    if native:
        monkeypatch.setattr(module, "_native_source_lock_entries", lambda _entry: [lock_entry])
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [entry])
    monkeypatch.setattr(module, "_running_commands", lambda: [])
    from stockagent.data_sync import syncthing_scan
    cold = tmp_path / "cold"
    cold.mkdir()
    (cold / syncthing_scan.D_PRIMARY_MARKER).touch()
    monkeypatch.setattr(syncthing_scan, "CANONICAL_ROOT", cold)
    monkeypatch.setattr(syncthing_scan, "_scan_pending",
                        lambda *a, **kw: pytest.fail("network scan during source transaction"))
    original_publish = module.publish_packed_snapshot
    observed_owners = []

    def publish(*args, **kwargs):
        with (tmp_path / "original-stage.lock").open("a+") as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed_owners.append(True)
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(module, "publish_packed_snapshot", publish)
    result_path = tmp_path / "commits/one.json"
    assert module.main(["publish", "prices", "--sync-root", str(cold), "--node-id", "test-node",
                        "--defer-scan", "--result-receipt", str(result_path)]) == 0
    assert observed_owners == [True]
    result = json.loads(result_path.read_text())
    assert result == json.loads(capsys.readouterr().out)
    assert result["published"][0]["scan_policy"] == "durably_queued"
    assert (cold / ".local-state/scan-pending/prices.json").is_file()
    with (tmp_path / "original-stage.lock").open("a+") as probe:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    release = packed_snapshots.resolve_latest_packed(cold, "prices")
    assert packed_snapshots.verify_packed_snapshot(cold, release)["snapshot_id"] == result["published"][0]["snapshot_id"]


def test_deferred_single_receipt_does_not_replace_missing_original_source_owner(tmp_path, monkeypatch, capsys):
    module = _module()
    source = _source(tmp_path, "2026-10-19")
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [_entry(source)])
    monkeypatch.setattr(module, "publish_packed_snapshot", lambda *a, **kw: pytest.fail("unlocked publish"))
    receipt = tmp_path / "commit.json"
    assert module.main(["publish", "prices", "--sync-root", str(tmp_path / "cold"),
                        "--defer-scan", "--result-receipt", str(receipt)]) == 2
    assert "held catalog/native source lock" in capsys.readouterr().err
    assert not receipt.exists()


def test_result_receipt_failure_can_follow_committed_publication(tmp_path, monkeypatch, capsys):
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    entry = {**_entry(source), "loose_threshold_mib": 8, "pack_buckets": 2}
    monkeypatch.setattr(module, "_load_catalog", lambda *_: [entry])
    monkeypatch.setattr(module, "atomic_write_json", lambda *_: (_ for _ in ()).throw(OSError("receipt failure")))
    cold = tmp_path / "cold"
    result_path = tmp_path / "attempt.json"
    assert module.main([
        "publish", "prices", "--sync-root", str(cold), "--node-id", "node-a",
        "--result-receipt", str(result_path),
    ]) == 2
    assert "receipt failure" in capsys.readouterr().err
    assert not result_path.exists()
    assert packed_snapshots.resolve_latest_packed(cold, "prices").manifest["snapshot_id"]


def test_publication_receipt_cannot_reuse_stale_path(tmp_path, monkeypatch, capsys):
    module = _module()
    result_path = tmp_path / "attempt.json"
    result_path.write_text('{"published": []}')
    monkeypatch.setattr(module, "_load_catalog", lambda *_: pytest.fail("reject before publication"))
    assert module.main(["publish", "prices", "--result-receipt", str(result_path)]) == 2
    assert "must be a new path" in capsys.readouterr().err
    assert json.loads(result_path.read_text()) == {"published": []}


@pytest.mark.parametrize("value", ["../escape", "/tmp/escape"])
def test_source_lock_rejects_out_of_repo_path(value, tmp_path, monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    with pytest.raises(SnapshotError, match="invalid source coordination lock"):
        with module._source_coordination_lock({"source_coordination_lock": value}):
            pytest.fail("invalid path must not acquire a lock")


def test_missing_cold_bytes_do_not_erase_freshness_non_regression(tmp_path):
    module = _module()
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    published = publish_packed_snapshot(cold, "prices", _source(tmp_path, "2026-08-18"),
        metadata={"freshness_value": "2026-08-18", "freshness_field": "end_date", "freshness_format": "iso-date"})
    for obj in published.manifest["archive"]["objects"]:
        (cold / obj["relpath"]).unlink()
    status = module._status(_entry(_source(tmp_path, "2026-08-17")), [], sync_root=cold)
    assert not status["freshness_non_regression"] and not status["publish_ready"]


def _module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts/publish_data_releases.py"
    spec = importlib.util.spec_from_file_location("publish_data_releases", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _entry(source: Path) -> dict[str, object]:
    return {
        "dataset": "prices",
        "source": str(source),
        "role": "training",
        "publish": True,
        "active_process_substrings": [],
        "note": "fixture",
        "freshness": {
            "receipt": "download_summary.json",
            "field": "end_date",
            "format": "iso-date",
            "required_values": {
                "coverage_complete": True,
                "failed_dates": 0,
            },
        },
    }


def _source(tmp_path: Path, end_date: str) -> Path:
    source = tmp_path / f"source-{end_date}"
    source.mkdir()
    (source / "prices.csv").write_text("price\n1\n", encoding="utf-8")
    (source / "download_summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "end_date": end_date,
                "failed_dates": 0,
            }
        ),
        encoding="utf-8",
    )
    return source


def test_publish_status_blocks_freshness_regression(tmp_path: Path) -> None:
    module = _module()
    cold_root = tmp_path / "cold"
    initialize_packed_layout(cold_root, node_id="node-a")
    newer = _source(tmp_path, "2026-08-18")
    publish_packed_snapshot(
        cold_root,
        "prices",
        newer,
        metadata={
            "freshness_field": "end_date",
            "freshness_format": "iso-date",
            "freshness_value": "2026-08-18",
        },
    )
    older = _source(tmp_path, "2026-08-17")

    status = module._status(_entry(older), [], sync_root=cold_root)

    assert status["source_freshness"]["value"] == "2026-08-17"
    assert status["latest_cold_freshness"]["value"] == "2026-08-18"
    assert status["freshness_non_regression"] is False
    assert status["publish_ready"] is False


def test_publish_status_requires_completion_receipt(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    receipt = json.loads((source / "download_summary.json").read_text())
    receipt["failed_dates"] = 1
    (source / "download_summary.json").write_text(
        json.dumps(receipt), encoding="utf-8"
    )

    status = module._status(_entry(source), [], sync_root=tmp_path / "cold")

    assert status["publish_ready"] is False
    assert "completion gate failed" in status["freshness_error"]


def test_publish_freshness_can_require_recent_catalog_check(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    entry = _entry(source)
    entry["freshness"]["timestamp_field"] = "catalog_checked_at_utc"
    entry["freshness"]["max_receipt_age_hours"] = 24
    path = source / "download_summary.json"
    receipt = json.loads(path.read_text())
    receipt["catalog_checked_at_utc"] = (datetime.now(UTC) - timedelta(hours=25)).isoformat()
    path.write_text(json.dumps(receipt))
    with pytest.raises(SnapshotError, match="outside its configured age window"):
        module._source_freshness(entry)
    receipt["catalog_checked_at_utc"] = datetime.now(UTC).isoformat()
    path.write_text(json.dumps(receipt))
    assert module._source_freshness(entry)["value"] == "2026-08-18"


@pytest.mark.parametrize("expired,wrong_scope,expected_exit", [(True, False, 75), (True, True, 2)])
def test_finlab_source_expiry_is_retryable_but_invalid_scope_is_not(
    tmp_path, monkeypatch, capsys, expired, wrong_scope, expected_exit,
):
    from scripts.finlab_release_gate import CONTRACT
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    entry = {**_entry(source), "dataset": "tw-public-research-finlab-2014-v4",
             "loose_threshold_mib": 8, "pack_buckets": 1}
    receipt = source / "download_summary.json"
    receipt.write_text(json.dumps({**json.loads(receipt.read_text()),
        "catalog_scope_contract": 'wrong' if wrong_scope else CONTRACT,
        "catalog_valid_until_utc": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
    }))
    monkeypatch.setattr(module, "_load_catalog", lambda *_a: [entry])
    status = module._status(entry, [])
    assert not status["publish_ready"]
    assert status.get("freshness_retryable", False) is (not wrong_scope)
    cold = tmp_path / "cold"
    result = tmp_path / "result.json"
    assert module.main(["publish", entry["dataset"], "--sync-root", str(cold),
                        "--node-id", "fixture", "--result-receipt", str(result)]) == expected_exit
    assert not result.exists()
    assert not list((cold / "heads").glob("**/*.json"))
    if expected_exit == 75:
        event = json.loads(capsys.readouterr().out)
        assert event["reason"] == "source_freshness_expired" and event["retryable"]


def test_finlab_expiry_at_final_commit_preserves_the_old_exact_head(tmp_path, monkeypatch, capsys):
    from scripts.finlab_release_gate import CONTRACT
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    entry = {**_entry(source), "dataset": "tw-public-research-finlab-2014-v4",
             "loose_threshold_mib": 8, "pack_buckets": 1}
    now = datetime.now(UTC)
    receipt = source / "download_summary.json"
    receipt.write_text(json.dumps({**json.loads(receipt.read_text()),
        "catalog_scope_contract": CONTRACT,
        "catalog_valid_until_utc": (now + timedelta(minutes=10)).isoformat(),
    }))
    cold = tmp_path / "cold"
    old = publish_packed_snapshot(cold, entry['dataset'], source, node_id='fixture')
    before = old.head_path.read_bytes()
    (source / "prices.csv").write_bytes(b"price\n-7\n")
    checks = []

    class AdvancingClock(datetime):
        @classmethod
        def now(cls, tz=None):
            checks.append(True)
            return now + timedelta(minutes=20 if len(checks) >= 4 else 0)

    monkeypatch.setattr(module, "datetime", AdvancingClock)
    monkeypatch.setattr(module, "_load_catalog", lambda *_a: [entry])
    assert module.main(["publish", entry['dataset'], "--sync-root", str(cold),
                        "--node-id", "fixture", "--result-receipt", str(tmp_path / "result.json")]) == 75
    assert len(checks) == 4
    assert old.head_path.read_bytes() == before
    assert not (tmp_path / "result.json").exists()
    assert (source / "prices.csv").read_bytes() == b"price\n-7\n"
    assert json.loads(capsys.readouterr().out)['reason'] == 'source_freshness_expired'
    assert verify_packed_snapshot(cold, old, reconstruct_all=True)['independently_reconstructed_files'] == 2


@pytest.mark.parametrize("root,dataset,chunked,enabled", [
    ('/srv/stockagent-packed', 'tw-public-research-finlab-2014-v4', True, True),
    ('/srv/stockagent-packed', 'prices', True, False),
    ('/srv/stockagent-packed/.local-state/other', 'tw-public-research-finlab-2014-v4', True, False),
    ('/srv/stockagent-packed', 'tw-public-research-finlab-2014-v4', False, False),
])
def test_finlab_native_reuse_dispatch_is_narrow_and_uses_the_original_publisher(
    tmp_path, monkeypatch, root, dataset, chunked, enabled,
):
    from contextlib import nullcontext
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    entry = {**_entry(source), 'dataset': dataset, 'loose_threshold_mib': 8, 'pack_buckets': 1}
    monkeypatch.setattr(module, '_publication_source_locks', lambda *_a, **_kw: nullcontext(
        {'primary_fd': None, 'native_lock_fds': ()}))
    monkeypatch.setattr(module, '_status', lambda *_a, **_kw: {'publish_ready': True, 'source_freshness': None})
    monkeypatch.setattr(module, '_version_options', lambda *_a: {'file_chunk_bytes': 8388608} if chunked else {})
    calls = []
    monkeypatch.setattr(module, 'publish_packed_snapshot', lambda *a, **kw: calls.append((a, kw)))
    args = module.build_parser().parse_args(['publish', dataset, '--sync-root', root, '--node-id', 'fixture'])
    module._publish_entry(entry, args, Path(root))
    assert len(calls) == 1 and calls[0][0][:3] == (Path(root), dataset, source)
    assert calls[0][1].get('d_primary_native_chunk_reuse', False) is enabled
    assert calls[0][1].get('d_primary_native_chunk_reuse_pipeline', False) is enabled
