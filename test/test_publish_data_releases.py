from __future__ import annotations

import importlib.util
import argparse
import fcntl
import json
import os
import shlex
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
