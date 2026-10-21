import json
from datetime import date
import os
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys
import pytest

from scripts import update_tw_day_trade_factorized_live as updater
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
from stockagent.live.market_status import TwStockDayDecision
from scripts import prepare_tw_day_trade_factorized_live as builder
from stockagent.data.annual_cache_admission import AnnualCacheBuildDeferred, CONTRACT
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred


def test_disk_capacity_deferral_does_not_repeat_the_entire_source_build(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(updater.time, "sleep", lambda *_: pytest.fail("blind capacity retry"))
    failure = CapacityDeferred("prepared_generation_byte_budget_exhausted", maximum_generation_bytes=1024)

    def operation():
        calls.append(1)
        raise failure

    receipt = tmp_path / "status.json"
    with pytest.raises(CapacityDeferred):
        updater._prepare_with_retry(operation, receipt_path=receipt, phase="completed",
                                    attempts=4, source_wait_seconds=600)
    assert len(calls) == 1
    status = json.loads(receipt.read_text())
    assert status["status"] == "capacity_deferred" and status["attempt"] == 1
    assert status["admission"] == failure.details
    assert status["last_good_generation_retained"] is True


@pytest.mark.skipif(os.name != "posix", reason="native WSL direct executable entrypoint")
def test_native_updater_is_executable_and_help_does_not_launch_the_data_owner():
    path = Path(updater.__file__).resolve()
    assert path.stat().st_mode & 0o111, "configured direct updater requires tracked executable mode"
    # A systemd namespace probe does not inherit the interactive Mamba PATH.
    # Bind env-python shebang execution to the runtime running this test; the
    # real bot likewise resolves source commands against its selected Python.
    result = subprocess.run([str(path), "--help"], capture_output=True, text=True, timeout=20,
        env={**os.environ, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")})
    assert result.returncode == 0, result.stderr
    assert "--phase {preopen,completed}" in result.stdout


@pytest.mark.parametrize("threads", [2, 3])
def test_fresh_source_entrypoint_configures_both_actual_pools_without_cuda(threads):
    script = """
import json, os, runpy, sys
sys.argv = [sys.argv[1], '--help']
try:
    runpy.run_path(sys.argv[0], run_name='__main__')
except SystemExit as exc:
    assert exc.code == 0
else:
    raise AssertionError('help did not exit before invoking the data owner')
import polars as pl, pyarrow as pa, torch
visible = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else (os.cpu_count() or 1)
expected = min(int(os.environ['STOCKAGENT_FACTORIZED_PREP_THREADS']), visible)
assert pl.thread_pool_size() == pa.cpu_count() == expected
assert os.environ['CUDA_VISIBLE_DEVICES'] == '' and not torch.cuda.is_initialized()
print(json.dumps({'polars': pl.thread_pool_size(), 'arrow': pa.cpu_count(), 'expected': expected}))
"""
    result = subprocess.run([sys.executable, "-c", script, str(Path(updater.__file__).resolve())],
        env={**os.environ, "STOCKAGENT_FACTORIZED_PREP_THREADS": str(threads),
             "POLARS_MAX_THREADS": "64", "OMP_NUM_THREADS": "64"},
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout.splitlines()[-1])
    assert observed["polars"] == observed["arrow"] == observed["expected"]


@pytest.mark.parametrize("phase", ["preopen", "completed"])
def test_scheduled_entrypoint_uses_the_canonical_cache_and_records_actual_pools(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(updater, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", [str(updater.__file__), "--phase", phase])
    monkeypatch.setattr(updater, "_projection_start_date", lambda *_: "2014-01-06")
    pools = {"polars_observed_threads": 2, "arrow_observed_threads": 2,
             "process_cpu_quota": False}
    commands, preparation = [], []

    def source_command(command, *, cwd, check):
        assert cwd == tmp_path and check is True
        commands.append(command)

    def prepare(**kwargs):
        preparation.append(kwargs)
        return {"status": "verified", "decision_session": "2026-10-14"}

    monkeypatch.setattr(updater.subprocess, "run", source_command)
    monkeypatch.setattr(updater, "prepare", prepare)
    monkeypatch.setattr(updater, "verify_preopen_projection", prepare)
    monkeypatch.setattr(updater, "_processing_pools", lambda: pools)
    updater.main()
    assert len(commands) == len(preparation) == 1
    expected_script = ("activate_tw_public_opening_data.py" if phase == "preopen"
                       else "finalize_tw_public_completed_session.py")
    assert commands[0][:2] == [sys.executable, str(tmp_path / "scripts" / expected_script)]
    if phase == "completed":
        assert preparation[0]["annual_cache_root"] == tmp_path / "artifacts/cache/tw_factorized_live/annual"
    else:
        assert "annual_cache_root" not in preparation[0] and "finlab" not in preparation[0]
    assert preparation[0]["start_date"] == "2014-01-06"
    receipt = tmp_path / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"
    status = json.loads(receipt.read_text())
    assert status["status"] == "verified" and status["phase"] == phase
    assert status["attempt"] == 1 and status["processing_pools"] == pools
    assert status["requested_start_date"] == "2014-01-06"
    assert status["stages_ms"]["canonical_activation_or_finalization"] >= 0


@pytest.fixture
def published_projection(tmp_path, monkeypatch):
    from contextlib import nullcontext
    import polars as pl
    from stockagent.live import tw_public_source_guard
    from stockagent.data.factorized_panel import file_sha256

    root = tmp_path / "projection"
    generation = root / "generations/accepted"
    generation.mkdir(parents=True)
    (root / "current").symlink_to(generation, target_is_directory=True)
    public = tmp_path / "public"
    (public / "features").mkdir(parents=True)
    summary = public / "features/tw_public_stock_daily.summary.json"
    summary.write_text(json.dumps({"requested_end_date": "2026-10-19"}))
    dictionary = generation / "feature_dictionary.json"
    dictionary.write_bytes(b"original trained dictionary")
    common = generation / "common.npy"
    common.write_bytes(b"common payload")
    block = generation / "part.npy.zst"
    block.write_bytes(b"original immutable projection payload")
    config = tmp_path / "config.yaml"
    config.write_text("selected original model configuration")
    parent = {"feature_dictionary_sha256": file_sha256(dictionary),
        "individual_channels": ["feature"], "common_channels": ["market"],
        "symbols": ["2330"], "base_feature_names": ["base"],
        "source_snapshot_id": "fixed", "logical_model_channels": 3}
    parent_path = tmp_path / "parent.json"
    parent_path.write_text(json.dumps(parent))
    manifest = {**parent, "contract": updater.VALUE_ONLY_CONTRACT, "status": "complete",
        "model_channel_policy": "value_only", "value_features": 2, "feature_lag": 1,
        "research_only": True, "historical_point_in_time": False,
        "dates": ["2026-10-16", "2026-10-19"],
        "blocks": [{"start": 0, "shape": [2, 1, 1], "path": block.name, "sha256": file_sha256(block)}],
        "common": {"path": common.name, "sha256": file_sha256(common)},
        "paper_inference": {"contract": "factorized_paper_projection_lag1_v1", "status": "verified",
            "raw_source_publication": False, "parent_manifest_sha256": file_sha256(parent_path),
            "completed_session": "2026-10-19", "decision_session": "2026-10-20",
            "built_at_utc": "2026-10-19T17:51:55+00:00",
            "source_identity": {"parent": file_sha256(parent_path), "config_sha": file_sha256(config),
                "start_date": "2026-10-16", "completed": "2026-10-19", "decision": "2026-10-20",
                "finlab": {"original_published_snapshot": "unchanged"}}}}
    path = generation / "factorized_manifest.json"
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(tw_public_source_guard, "live_tw_public_source_read_guard", lambda *_: nullcontext())
    monkeypatch.setattr(updater, "_next_projection_session", lambda *_: (date(2026, 10, 20), {}))
    monkeypatch.setattr(updater, "_exchange_session_dates", lambda *_: pl.DataFrame({
        "_session_date": [date(2026, 10, 16), date(2026, 10, 19)]}))
    return dict(config_path=config, parent_path=parent_path, public=public, output=root,
                start_date="2026-10-16", session_date=date(2026, 10, 20)), path, manifest


def test_preopen_consumes_the_complete_published_snapshot_without_raw_builds(published_projection, monkeypatch):
    kwargs, path, manifest = published_projection
    monkeypatch.setattr(updater, "prepare", lambda **_: pytest.fail("no full-history work at preopen"))
    original = path.read_bytes()
    result = updater.verify_preopen_projection(**kwargs)
    assert result["status"] == "already_current" and result["dates"] == 2
    assert result["full_history_rebuilt"] is False and result["latest_raw_heads_claimed"] is False
    assert result["source_identity"] == manifest["paper_inference"]["source_identity"]
    assert result["verified_members"] == 3 and path.read_bytes() == original
    assert result["provider_requests"] == 0 and result["model_executed"] is False


@pytest.mark.parametrize("fault", ["parent", "config", "dictionary", "member", "missing_member",
    "session", "completed", "horizon", "missing_date", "unsorted", "raw_publication", "lag",
    "model_policy", "logical_width", "block_gap", "symbol_gap", "pointer_escape", "missing_projection"])
def test_preopen_fails_closed_for_stale_or_corrupt_published_inputs(published_projection, fault):
    kwargs, path, manifest = published_projection
    if fault == "parent":
        kwargs["parent_path"].write_text("{}")
    elif fault == "config":
        kwargs["config_path"].write_text("different configuration")
    elif fault == "dictionary":
        path.with_name("feature_dictionary.json").write_bytes(b"changed")
    elif fault == "member":
        path.with_name("part.npy.zst").write_bytes(b"corrupt")
    elif fault == "missing_member":
        path.with_name("part.npy.zst").unlink()
    elif fault == "session":
        manifest["paper_inference"]["decision_session"] = "2026-10-19"
    elif fault == "completed":
        manifest["paper_inference"]["completed_session"] = "2026-10-16"
    elif fault == "horizon":
        kwargs["start_date"] = "2014-01-06"
    elif fault == "missing_date":
        manifest["dates"] = ["2026-10-19"]
    elif fault == "unsorted":
        manifest["dates"].reverse()
    elif fault == "raw_publication":
        manifest["paper_inference"]["raw_source_publication"] = True
    elif fault == "lag":
        manifest["feature_lag"] = 0
    elif fault == "model_policy":
        manifest["model_channel_policy"] = "other"
    elif fault == "logical_width":
        manifest["value_features"] = 200
    elif fault == "block_gap":
        manifest["blocks"][0]["start"] = 1
    elif fault == "symbol_gap":
        manifest["blocks"][0]["shape"][1] = 2
    elif fault == "pointer_escape":
        outside = kwargs["output"].parent / "outside"
        outside.mkdir()
        (outside / path.name).write_text(json.dumps(manifest))
        (kwargs["output"] / "current").unlink()
        (kwargs["output"] / "current").symlink_to(outside, target_is_directory=True)
    elif fault == "missing_projection":
        (kwargs["output"] / "current").unlink()
    path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, FileNotFoundError, updater.FactorizedPreopenProjectionPending)):
        updater.verify_preopen_projection(**kwargs)


def test_missing_published_projection_requests_existing_background_owner_without_repeating(tmp_path, monkeypatch):
    clock = _clock(monkeypatch)
    calls = []
    def operation():
        calls.append(clock[0])
        raise updater.FactorizedPreopenProjectionPending("completed calendar not prepared")
    receipt = tmp_path / "status.json"
    with pytest.raises(updater.FactorizedPreopenProjectionPending):
        updater._prepare_with_retry(operation, receipt_path=receipt, phase="preopen",
                                    attempts=4, source_wait_seconds=600)
    value = json.loads(receipt.read_bytes())
    assert calls == [0.] and value["status"] == "projection_refresh_required"
    assert value["retry_owner"] == "stockagent-discord-artifact-maintenance.service"
    assert value["model_executed"] is False


@pytest.mark.parametrize("configured,expected", [("2014-01-06", "2014-01-06"),
                                                  ("2026-01-01", "2026-01-01"),
                                                  (None, "2014-01-06")])
def test_projection_scope_follows_selected_full_horizon(tmp_path, monkeypatch, configured, expected):
    parent = tmp_path / "parent.json"
    parent.write_text(json.dumps({"dates": ["2014-01-06", "2026-10-16"]}))
    monkeypatch.setattr(updater, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date=configured)))
    assert updater._projection_start_date(tmp_path / "config.yaml", parent) == expected


def test_projection_rejects_an_uncovered_earlier_horizon(tmp_path, monkeypatch):
    parent = tmp_path / "parent.json"
    parent.write_text(json.dumps({"dates": ["2014-01-06", "2026-10-16"]}))
    monkeypatch.setattr(updater, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date="2013-01-01")))
    with pytest.raises(ValueError, match="precedes"):
        updater._projection_start_date(tmp_path / "config.yaml", parent)


@pytest.mark.parametrize("dates", [[], ["bad-date"], ["2026-10-16", "2014-01-06"],
                                    ["2014-01-06", "2014-01-06"], [None]])
def test_projection_rejects_invalid_parent_calendar(tmp_path, monkeypatch, dates):
    parent = tmp_path / "parent.json"
    parent.write_text(json.dumps({"dates": dates}))
    monkeypatch.setattr(updater, "load_config", lambda _: SimpleNamespace(
        data=SimpleNamespace(panel_start_date=None)))
    with pytest.raises(ValueError):
        updater._projection_start_date(tmp_path / "config.yaml", parent)


def _clock(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(updater.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updater.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    return clock


def test_writer_race_waits_without_consuming_build_attempts(tmp_path, monkeypatch):
    clock = _clock(monkeypatch)
    calls = []

    def operation():
        calls.append(clock[0])
        if len(calls) <= 3:
            raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock")
        return {"status": "already_current", "decision_session": "2026-10-14"}

    receipt = tmp_path / "status.json"
    result = updater._prepare_with_retry(operation, receipt_path=receipt, phase="completed",
                                        attempts=1, source_wait_seconds=10)
    assert result["status"] == "already_current"
    assert calls == [0, 2, 4, 6]
    assert json.loads(receipt.read_text())["attempt"] == 1


def test_writer_wait_expiry_fails_visibly_and_retains_last_good(tmp_path, monkeypatch):
    _clock(monkeypatch)
    receipt = tmp_path / "status.json"

    def operation():
        raise TWPublicSourceUpdatePending(tmp_path, tmp_path / "writer.lock")

    with pytest.raises(TWPublicSourceUpdatePending):
        updater._prepare_with_retry(operation, receipt_path=receipt, phase="preopen",
                                    attempts=2, source_wait_seconds=3)
    status = json.loads(receipt.read_text())
    assert status["status"] == "failed_retryable"
    assert status["source_wait_expired"] is True
    assert status["model_executed"] is False
    assert status["last_good_generation_retained"] is True


def test_corruption_is_not_treated_as_a_busy_writer(tmp_path, monkeypatch):
    _clock(monkeypatch)
    calls = []

    def operation():
        calls.append(True)
        raise ValueError("immutable source member changed")

    receipt = tmp_path / "status.json"
    with pytest.raises(ValueError, match="member changed"):
        updater._prepare_with_retry(operation, receipt_path=receipt, phase="completed",
                                    attempts=2, source_wait_seconds=600)
    assert len(calls) == 2
    status = json.loads(receipt.read_text())
    assert status["status"] == "failed_retryable"
    assert status["attempt"] == 2
    assert status["error_type"] == "ValueError"
    assert "source_wait_expired" not in status


@pytest.mark.parametrize("phase", ["preopen", "completed"])
def test_denied_cache_build_records_the_existing_owner_without_repeating(tmp_path, monkeypatch, phase):
    clock = _clock(monkeypatch)
    calls = []
    details = {"contract": CONTRACT, "reason": "insufficient_memory_headroom",
               "source": str(tmp_path / "2025.parquet"), "immutable_seed": True,
               "required_headroom_bytes": 26 << 30, "observed_headroom_bytes": 23 << 30}
    failure = AnnualCacheBuildDeferred(details)

    def operation():
        calls.append(clock[0])
        raise failure

    receipt = tmp_path / "status.json"
    with pytest.raises(AnnualCacheBuildDeferred) as error:
        updater._prepare_with_retry(operation, receipt_path=receipt, phase=phase,
                                    attempts=4, source_wait_seconds=600)
    assert error.value is failure
    assert calls == [0.0] and clock[0] == 0.0
    status = json.loads(receipt.read_text())
    assert status["status"] == "cache_warming_required"
    assert status["phase"] == phase and status["attempt"] == 1
    assert status["admission"] == details
    assert status["retry_owner"] == "stockagent-discord-artifact-maintenance.service"
    assert status["model_executed"] is False
    assert status["last_good_generation_retained"] is True
    assert "source_wait_expired" not in status


def test_projection_next_clock_reuses_official_calendar_not_future_eligibility(tmp_path, monkeypatch):
    decisions = {date(2026, 10, 9): TwStockDayDecision("closed", "official holiday"),
                 date(2026, 10, 10): TwStockDayDecision("closed", "official weekend"),
                 date(2026, 10, 11): TwStockDayDecision("closed", "official weekend"),
                 date(2026, 10, 12): TwStockDayDecision("scheduled_open", "official schedule")}
    checked = []

    def classify(day, *, parquet_root):
        assert parquet_root == tmp_path / "stocks"
        checked.append(day)
        return decisions[day]

    monkeypatch.setattr(builder, "tw_stock_day_decision", classify)
    day, proof = builder._next_projection_session(tmp_path, date(2026, 10, 8))
    assert day == date(2026, 10, 12)
    assert checked == list(decisions)
    assert proof == {"status": "scheduled_open", "reason": "official schedule",
                     "execution_or_eligibility_proof": False}


def test_projection_unknown_calendar_cannot_skip_to_a_weekday(tmp_path, monkeypatch):
    monkeypatch.setattr(builder, "tw_stock_day_decision", lambda *_args, **_kwargs:
                        TwStockDayDecision("unknown", "official schedule missing"))
    with pytest.raises(ValueError, match="calendar is unknown"):
        builder._next_projection_session(tmp_path, date(2026, 10, 13))
