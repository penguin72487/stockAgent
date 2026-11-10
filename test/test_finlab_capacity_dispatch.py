"""Capacity admission yields between FinLab batches, without source failures."""
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import check_acquisition_capacity as dispatch
from scripts import download_finlab_history as history
from scripts import download_finlab_intraday as intraday
from scripts import download_finlab_market_intraday as market
from stockagent.data_sync import cache_capacity as capacity
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

ROOT = Path(__file__).resolve().parents[1]


def _inventory(root):
    return {str(path.relative_to(root)): (
        hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_ino,
        path.stat().st_mtime_ns, path.stat().st_ctime_ns,
    ) for path in root.rglob("*") if path.is_file()}


@pytest.fixture
def retained_source(tmp_path):
    root = tmp_path / "finlab"
    for name, raw in {
        "datasets/source.raw": b"original signed values: -7, NULL, shares=1000",
        "attempts/existing.json": b'{"failure_streak":2,"status":"provider_empty"}',
        "runs/latest.json": b'{"state":"partial","attempted":8,"attempt_id":"previous-group"}',
    }.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    return root


@pytest.fixture
def backing_refusal(monkeypatch):
    policy = capacity.load_capacity_policy()
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: {**policy, "nodes": ["capacity-test"]})
    monkeypatch.setattr(capacity, "load_acquisition_verification_reserve", lambda: 64 * 1024**3)
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "capacity-test")
    monkeypatch.setattr(capacity.os, "statvfs", lambda _path: SimpleNamespace(f_bavail=1024**4, f_frsize=1))
    calls = []

    def refuse(path, required):
        calls.append((path, required))
        raise CapacityDeferred("physical_backing_headroom_unconfirmed")

    monkeypatch.setattr(capacity, "_admit_backing_volume", refuse)
    return calls


@pytest.mark.parametrize("action", ["discover", "fetch", "sync", "market", "intraday"])
def test_virtual_space_cannot_admit_new_batch_or_consume_source_retry(
    retained_source, backing_refusal, monkeypatch, capsys, action,
):
    before = _inventory(retained_source)
    for module, names in (
        (history, ("provider_catalog", "sync_provider_catalog", "credential_available", "record_attempt")),
        (market, ("_account_sync_lock", "sync_market")), (intraday, ("sync",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, lambda *a, **kw: pytest.fail("source work before admission"))
    argv = ["--output-root", str(retained_source)]
    if action in {"market", "intraday"}:
        monkeypatch.setattr(sys, "argv", ["finlab", *argv])
        result = (market if action == "market" else intraday).main()
    else:
        result = history.main([action, *argv])
    assert result == 75
    report = json.loads(capsys.readouterr().out)
    assert report["state"] == "deferred"
    assert report["reason"] == "physical_backing_headroom_unconfirmed"
    assert report["available_bytes"] > report["required_bytes"]
    assert backing_refusal == [(retained_source, 64 * 1024**3)]
    assert _inventory(retained_source) == before


def test_first_download_admits_existing_parent_without_creating_output(tmp_path, backing_refusal):
    destination = tmp_path / "not-created" / "source"
    assert dispatch.main(["--path", str(destination)]) == 75
    assert backing_refusal[0][0] == tmp_path
    assert list(tmp_path.iterdir()) == []


def test_missing_registered_provider_path_does_not_fall_back_to_checkout(tmp_path, monkeypatch, capsys):
    from stockagent.data_sync import provider_workspace as placement
    monkeypatch.setattr(placement, 'ROOT', tmp_path)
    fence = tmp_path/'migration-fence'; fence.write_text('in progress')
    monkeypatch.setattr(placement, 'MIGRATION_FENCE', fence)
    monkeypatch.setattr(placement, 'load', lambda: {'providers': {'tej': {'state': 'copying'}}})
    monkeypatch.setattr(dispatch, 'acquisition_dispatch_status', lambda *_: pytest.fail('must not fall back to C'))
    target = tmp_path/'data_tej/api_trial_v1'
    assert dispatch.check_dispatch(target) == 75
    assert json.loads(capsys.readouterr().out)['reason'] == 'provider_workspace_unavailable'
    assert not target.exists()


@pytest.mark.parametrize("dangling,expected", [(False, 75), (True, 2)])
def test_redirected_or_dangling_workspace_does_not_admit_writes(
    tmp_path, backing_refusal, dangling, expected,
):
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path / "missing" if dangling else tmp_path, target_is_directory=True)
    assert dispatch.check_dispatch(alias / "new-source") == expected
    assert not backing_refusal
    assert alias.is_symlink()


@pytest.mark.parametrize("state", ["admitted", "not_enrolled"])
def test_space_recovery_retries_original_work_without_dataset_cooldown(
    retained_source, monkeypatch, state,
):
    calls = []
    results = iter([{"state": "deferred"}, {"state": state}])
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda path: next(results))
    monkeypatch.setattr(history, "sync_provider_catalog", lambda *a, **kw: calls.append("catalog") or ["price:close"])
    monkeypatch.setattr(history, "sync_catalog", lambda *a, **kw: calls.append("sync") or {"state": "pass_complete"})
    before = _inventory(retained_source)
    argv = ["sync", "--output-root", str(retained_source)]
    assert history.main(argv) == 75 and not calls
    assert _inventory(retained_source) == before
    assert history.main(argv) == 0 and calls == ["catalog", "sync"]
    assert _inventory(retained_source) == before


def test_probe_error_is_closed_and_does_not_print_private_error(monkeypatch, tmp_path, capsys):
    def fail(_path):
        raise OSError("private://account/token")
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", fail)
    assert dispatch.check_dispatch(tmp_path) == 2
    report = capsys.readouterr()
    assert "private://" not in report.out + report.err
    assert json.loads(report.out) == {"state": "error", "error_type": "OSError", "allocation_admitted": False}


def test_priority_may_use_verification_headroom_but_not_control_reserve(monkeypatch, tmp_path):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {
        "state": "deferred", "reason": "filesystem_reserve"})
    monkeypatch.setattr(dispatch, "load_capacity_policy", lambda: {
        "nodes": ["test"], "minimum_free_bytes": 25})
    monkeypatch.setattr(dispatch.socket, "gethostname", lambda: "test")
    free = [30]
    monkeypatch.setattr(dispatch.os, "statvfs", lambda _: SimpleNamespace(f_bavail=free[0], f_frsize=1))
    calls = []
    monkeypatch.setattr(dispatch, "_admit_backing_volume", lambda path, required: calls.append(required) or {"state": "ok"})
    assert dispatch.check_dispatch(tmp_path, report_admitted=False, priority=True) == 0
    assert calls == [25]
    free[0] = 24
    assert dispatch.check_dispatch(tmp_path, report_admitted=False, priority=True) == 75
    assert calls == [25]


def test_priority_cannot_bypass_unknown_physical_capacity(monkeypatch, tmp_path):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {
        "state": "deferred", "reason": "physical_backing_headroom_unconfirmed"})
    monkeypatch.setattr(dispatch, "_admit_backing_volume", lambda *_: pytest.fail("must not guess capacity"))
    assert dispatch.check_dispatch(tmp_path, report_admitted=False, priority=True) == 75


@pytest.mark.parametrize("state", ["not_enrolled", "unknown", "error"])
def test_explicit_extra_headroom_never_admits_unverified_capacity(monkeypatch, tmp_path, state, capsys):
    monkeypatch.setattr(dispatch, "priority_dispatch_status", lambda *a, **kw: {"state": state})
    assert dispatch.check_dispatch(tmp_path, report_admitted=False, priority=True,
                                   additional_priority_headroom_bytes=24) == 75
    assert json.loads(capsys.readouterr().out)["reason"] == "priority_backing_unverified"


def test_additional_priority_headroom_guards_both_filesystem_and_backing(monkeypatch, tmp_path):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {
        "state": "deferred", "reason": "physical_backing_reserve"})
    monkeypatch.setattr(dispatch, "load_capacity_policy", lambda: {
        "nodes": ["test"], "minimum_free_bytes": 25})
    monkeypatch.setattr(dispatch.socket, "gethostname", lambda: "test")
    free = [49]
    monkeypatch.setattr(dispatch.os, "statvfs", lambda _: SimpleNamespace(f_bavail=free[0], f_frsize=1))
    checked = []
    monkeypatch.setattr(dispatch, "_admit_backing_volume", lambda path, required:
                        checked.append(required) or {"physical_free_bytes": free[0]})
    assert dispatch.check_dispatch(tmp_path, report_admitted=False, priority=True,
                                   additional_priority_headroom_bytes=24) == 0
    assert checked == [49]
    free[0] = 48
    assert dispatch.check_dispatch(tmp_path, report_admitted=False, priority=True,
                                   additional_priority_headroom_bytes=24) == 75
    assert checked == [49]


@pytest.mark.parametrize("physical,expected", [(55, "admitted"), (48, "deferred")])
def test_priority_reuses_only_the_same_verified_physical_probe(monkeypatch, tmp_path, physical, expected):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {
        "state": "deferred", "reason": "physical_backing_reserve", "path": str(tmp_path),
        "physical_free_bytes": physical, "required_bytes": 64, "required_bytes_including_reserve": 64})
    monkeypatch.setattr(dispatch, "load_capacity_policy", lambda: {"nodes": ["test"], "minimum_free_bytes": 25})
    monkeypatch.setattr(dispatch.socket, "gethostname", lambda: "test")
    monkeypatch.setattr(dispatch.os, "statvfs", lambda _: SimpleNamespace(f_bavail=100, f_frsize=1))
    monkeypatch.setattr(dispatch, "_admit_backing_volume", lambda *_: pytest.fail("duplicate physical probe"))
    result = dispatch.priority_dispatch_status(tmp_path, additional_headroom_bytes=24)
    assert result["state"] == expected
    if expected == "admitted":
        assert result["backing_volume"] == {"physical_free_bytes": physical, "required_bytes": 49,
                                            "observation_basis": "same_dispatch_physical_probe"}
    else:
        assert result["physical_free_bytes"] == physical and result["required_bytes_including_reserve"] == 49


@pytest.mark.parametrize("field,value", [
    ("path", "/unrelated"), ("physical_free_bytes", True), ("physical_free_bytes", -1),
    ("required_bytes_including_reserve", None), ("required_bytes", 63),
    ("physical_free_bytes", 64),
])
def test_priority_does_not_reuse_an_inconsistent_probe(monkeypatch, tmp_path, field, value):
    normal = {"state": "deferred", "reason": "physical_backing_reserve", "path": str(tmp_path),
              "physical_free_bytes": 55, "required_bytes": 64, "required_bytes_including_reserve": 64}
    normal[field] = value
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: normal)
    monkeypatch.setattr(dispatch, "load_capacity_policy", lambda: {"nodes": ["test"], "minimum_free_bytes": 25})
    monkeypatch.setattr(dispatch.socket, "gethostname", lambda: "test")
    monkeypatch.setattr(dispatch.os, "statvfs", lambda _: SimpleNamespace(f_bavail=100, f_frsize=1))
    checked = []
    monkeypatch.setattr(dispatch, "_admit_backing_volume", lambda path, required:
                        checked.append(required) or {"physical_free_bytes": 55})
    assert dispatch.priority_dispatch_status(tmp_path, additional_headroom_bytes=24)["state"] == "admitted"
    assert checked == [49]


def test_priority_headroom_above_the_normal_reserve_is_not_silently_skipped(monkeypatch, tmp_path):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {
        "state": "admitted", "required_bytes": 64})
    monkeypatch.setattr(dispatch, "load_capacity_policy", lambda: {
        "nodes": ["test"], "minimum_free_bytes": 25})
    monkeypatch.setattr(dispatch.socket, "gethostname", lambda: "test")
    monkeypatch.setattr(dispatch.os, "statvfs", lambda _: SimpleNamespace(f_bavail=70, f_frsize=1))
    checked = []
    monkeypatch.setattr(dispatch, "_admit_backing_volume", lambda path, required:
                        checked.append(required) or {})
    assert dispatch.check_dispatch(tmp_path, priority=True, additional_priority_headroom_bytes=40) == 0
    assert checked == [65]


def test_finite_intraday_priority_uses_guard_before_lock_or_source(retained_source, monkeypatch):
    observed = []
    monkeypatch.setattr(market, 'check_dispatch', lambda path, **kw: observed.append((path, kw)) or 75)
    monkeypatch.setattr(market, '_account_sync_lock', lambda *a: pytest.fail('lock before capacity admission'))
    monkeypatch.setattr(market, 'sync_market', lambda *a, **kw: pytest.fail('source work before admission'))
    before = _inventory(retained_source)
    monkeypatch.setattr(sys, 'argv', ['finlab', '--output-root', str(retained_source), '--priority-only'])
    assert market.main() == 75
    assert observed == [(retained_source, {'report_admitted': False, 'priority': True})]
    assert _inventory(retained_source) == before


def test_capacity_deferred_tick_checks_extra_headroom_before_lock_or_source(retained_source, monkeypatch):
    observed = []
    monkeypatch.setattr(market, 'check_dispatch', lambda path, **kw: observed.append((path, kw)) or 75)
    monkeypatch.setattr(market, '_account_sync_lock', lambda *a: pytest.fail('lock before capacity admission'))
    monkeypatch.setattr(market, 'sync_market', lambda *a, **kw: pytest.fail('source work before admission'))
    before = _inventory(retained_source)
    monkeypatch.setattr(sys, 'argv', ['finlab', '--output-root', str(retained_source), '--priority-only',
                                     '--capacity-deferred-priority', '--kind', 'tw_tick', '--limit', '1'])
    assert market.main() == 75
    assert observed == [(retained_source, {'report_admitted': False, 'priority': True,
                          'additional_priority_headroom_bytes': 24 * 1024**3})]
    assert _inventory(retained_source) == before


@pytest.mark.parametrize("state", ["admitted", "not_enrolled"])
@pytest.mark.parametrize("kind", ["market", "intraday"])
def test_accepted_download_keeps_one_JSON_summary(retained_source, monkeypatch, capsys, state, kind):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {"state": state})
    monkeypatch.setattr(sys, "argv", ["finlab", "--output-root", str(retained_source)])
    expected = {"state": "pass_complete", "attempted_this_run": 1}
    if kind == "market":
        monkeypatch.setattr(market, "_account_sync_lock", lambda *a: nullcontext())
        monkeypatch.setattr(market, "sync_market", lambda *a, **kw: expected)
        assert market.main() == 0
    else:
        monkeypatch.setattr(intraday, "sync", lambda *a, **kw: expected)
        assert intraday.main() == 0
    assert json.loads(capsys.readouterr().out) == expected


def test_standalone_capacity_CLI_still_reports_accepted_state(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(dispatch, "acquisition_dispatch_status", lambda _: {"state": "admitted"})
    assert dispatch.main(["--path", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out) == {"state": "admitted"}


@pytest.mark.parametrize("action", ["catalog", "pending", "audit-local", "audit-volume-units", "record-timeout"])
def test_local_inspection_and_interruption_repair_do_not_need_new_capacity(
    retained_source, monkeypatch, action,
):
    monkeypatch.setattr(history, "check_dispatch", lambda *_: pytest.fail("local operation was capacity gated"))
    monkeypatch.setattr(history, "provider_catalog", lambda: pytest.fail("unexpected source query"))
    monkeypatch.setattr(history, "audit_local", lambda *a, **kw: (1, 0))
    monkeypatch.setattr(history, "record_timed_out_sync", lambda *a, **kw: {"state": "partial"})
    assert history.main([action, "--output-root", str(retained_source)]) == (2 if action == "pending" else 0)


def test_inventory_only_keeps_existing_account_lock_without_dispatch_probe(retained_source, monkeypatch):
    monkeypatch.setattr(market, "check_dispatch", lambda *_: pytest.fail("inventory was capacity gated"))
    monkeypatch.setattr(market, "_account_sync_lock", lambda *a: nullcontext())
    monkeypatch.setattr(market, "_inventory_only", lambda args: 0)
    monkeypatch.setattr(sys, "argv", ["finlab", "--status-only", "--output-root", str(retained_source)])
    assert market.main() == 0


def test_frozen_parent_yields_at_next_child_and_keeps_completed_batch(tmp_path):
    """Run the real frozen Bash loop and the new Python entrypoint, no API."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("run_finlab_refresh.sh", "run_finlab_refresh_frozen.sh"):
        shutil.copyfile(ROOT / "scripts" / name, scripts / name)
    output = tmp_path / "data_finlab"
    (output / "datasets").mkdir(parents=True)
    source = output / "datasets/original.raw"
    source.write_bytes(b"retained source from an earlier group")
    shim = scripts / "python-entry"
    shim.write_text("#!/bin/bash\nif [[ \"$1\" == \"-c\" ]]; then exit 0; fi\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
    shim.chmod(0o700)
    (scripts / "runtime_env.sh").write_text('''
resolve_fintech_python() { printf '%s\\n' "$FINLAB_REPO_ROOT/scripts/python-entry"; }
date() { if [[ "$*" == "+%H%M%S" ]]; then printf '120000\\n'; else /bin/date "$@"; fi; }
run_fintech_python() {
  case "$1" in
    */check_acquisition_capacity.py) return 0;;
    */finlab_refresh_policy.py) printf '%s\\n' '{"minute_partitions_per_pass":1,"maximum_intraday_passes":1,"catalog_current":true,"history_floor_mb":50,"general_batch_size":1,"processing_threads":1,"tick_partitions_per_pass":1}';;
    *) "$FINLAB_TEST_PYTHON" "$@";;
  esac
}
''')
    (scripts / "download_finlab_history.py").write_text('''
import json
from pathlib import Path
import sys
sys.path.insert(0, REPOSITORY)
from scripts import check_acquisition_capacity as dispatch
from scripts import download_finlab_history as history
root = Path("data_finlab")
latest = root / "runs/latest.json"
if not latest.exists():
    latest.parent.mkdir()
    latest.write_text('{"state":"partial","attempted":1,"attempt_id":"completed-first-group"}')
    raise SystemExit(0)
before = latest.read_bytes()
dispatch.acquisition_dispatch_status = lambda _: {"state":"deferred","reason":"physical_backing_headroom_unconfirmed"}
history.sync_provider_catalog = lambda *a, **kw: (_ for _ in ()).throw(AssertionError("SDK called after deferral"))
result = history.main(["sync", "--output-root", str(root)])
assert latest.read_bytes() == before
raise SystemExit(result)
'''.replace("REPOSITORY", repr(str(ROOT))))
    result = subprocess.run(["bash", str(scripts / "run_finlab_refresh_frozen.sh")], cwd=tmp_path,
        env={**os.environ, "FINLAB_REPO_ROOT": str(tmp_path), "FINLAB_TEST_PYTHON": sys.executable},
        text=True, capture_output=True, timeout=30)
    assert result.returncode == 75, result.stdout + result.stderr
    assert "new acquisition batch deferred" in result.stdout
    assert json.loads((output / "runs/latest.json").read_bytes())["attempt_id"] == "completed-first-group"
    assert source.read_bytes() == b"retained source from an earlier group"
    assert not (output / "attempts").exists()
