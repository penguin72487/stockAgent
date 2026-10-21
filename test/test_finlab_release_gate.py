from __future__ import annotations

from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts.finlab_release_gate import CONTRACT, catalog_readiness
from stockagent.data.finlab_acquisition_contract import read_verified_empty_evidence, safe_stem


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _catalog(root: Path, now: datetime) -> dict:
    keys = ["monthly:a", "monthly:b", "events:empty", "tw_minute:2330", "tw_tick:2330"]
    _write(root / "catalog/discovery.json", {"keys": keys, "observed_at_utc": now.isoformat()})
    payload = root / "datasets/a.parquet"
    payload.parent.mkdir()
    payload.write_bytes(b"original signed values: -7, NULL, 2.5")
    alias = payload.with_name("b.parquet")
    os.link(payload, alias)
    for key, path in zip(keys[:2], (payload, alias)):
        _write(root / "receipts" / f"{safe_stem(key)}.json", {
            "dataset": key, "status": "downloaded_unverified_for_pit",
            "parquet_path": str(path.relative_to(root)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source_check_mode": "upstream_incremental",
            "source_checked_at_utc": (now - timedelta(days=2)).isoformat(),
            "next_source_check_at_utc": (now + timedelta(days=1)).isoformat(),
        })
    raw = root / "empty_evidence/source.feather"
    raw.parent.mkdir()
    raw.write_bytes(b"retained original all-null source")
    _write(root / "empty_evidence/proof.json", {
        "dataset": keys[2], "status": "source_all_null", "usable_observations": False,
        "raw_non_null_values": 0, "raw_path": "empty_evidence/source.feather",
        "raw_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
        "observed_at_utc": (now - timedelta(minutes=1)).isoformat(),
    })
    attempt = {"dataset": keys[2], "status": "provider_empty", "raw_non_null_values": 0,
               "empty_evidence_path": "empty_evidence/proof.json", "failure_streak": 1,
               "attempted_at_utc": (now - timedelta(minutes=1)).isoformat()}
    _write(root / "attempts" / f"{safe_stem(keys[2])}.json", attempt)
    return attempt


def test_general_catalog_uses_native_expiry_and_explicit_partition_scope(tmp_path):
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    result = catalog_readiness(tmp_path, now=now, verify_hashes=True)
    assert result["contract"] == CONTRACT
    assert result["ready"] and result["hashes_verified"]
    assert result["catalog_total"] == 5 and result["general_history_total"] == 3
    assert result["verified"] == 3 and result["missing"] == result["stale"] == result["invalid"] == 0
    assert result["partitioned_endpoints"] == ["tw_minute:2330", "tw_tick:2330"]
    assert result["partitioned_history_verified_by_this_gate"] is False
    assert result["verified_source_empty"] == ["events:empty"]
    assert result["physical_files_hashed"] == 1
    assert result["hash_bytes_read"] == (tmp_path / "datasets/a.parquet").stat().st_size
    assert datetime.fromisoformat(result["valid_until_utc"]) == now + timedelta(minutes=29)
    expired = catalog_readiness(tmp_path, now=now + timedelta(minutes=30))
    assert expired["missing"] == 1 and not expired["ready"]


def test_alias_mutation_is_detected_for_every_logical_key(tmp_path, monkeypatch):
    from stockagent.data_sync import immutable_cache_files
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    original = immutable_cache_files.stable_payload_hash

    def mutate(path, before):
        digest = original(path, before)
        path.write_bytes(b"modified during verification")
        return digest

    monkeypatch.setattr(immutable_cache_files, "stable_payload_hash", mutate)
    result = catalog_readiness(tmp_path, now=now, verify_hashes=True)
    assert not result["ready"] and not result["hashes_verified"]
    assert result["invalid"] == 2 and result["verified"] == 1


@pytest.mark.parametrize("replacement", ["symlink", "fifo", "list", "raw_changed"])
def test_source_empty_requires_regular_stable_backing_evidence(tmp_path, replacement):
    now = datetime.now(UTC)
    attempt = _catalog(tmp_path, now)
    proof = tmp_path / "empty_evidence/proof.json"
    if replacement == "symlink":
        moved = proof.with_suffix(".saved")
        proof.rename(moved)
        proof.symlink_to(moved.name)
    elif replacement == "fifo":
        proof.unlink()
        os.mkfifo(proof)
    elif replacement == "list":
        proof.write_text("[]")
    else:
        (tmp_path / "empty_evidence/source.feather").write_bytes(b"not the observed source")
    assert read_verified_empty_evidence("events:empty", tmp_path, attempt) is None
    result = catalog_readiness(tmp_path, now=now)
    assert result["missing"] == 1 and not result["ready"]


def test_source_check_due_still_blocks_release(tmp_path):
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    receipt_path = tmp_path / "receipts" / f"{safe_stem('monthly:a')}.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["next_source_check_at_utc"] = now.isoformat()
    _write(receipt_path, receipt)
    result = catalog_readiness(tmp_path, now=now)
    assert result["stale"] == 1 and not result["ready"]


def test_stage_refreshes_only_expired_full_hash_completion(tmp_path):
    from scripts.stage_tw_public_finlab_research_release import (
        catalog_completion_fields, refresh_unchanged_completion,
    )
    now = datetime.now(UTC)
    _catalog(tmp_path, now)
    readiness = catalog_readiness(tmp_path, now=now, verify_hashes=True)
    previous = {"status": "complete", "staged_files_sha256": {"feature": "fixed-sha"}}
    receipt = tmp_path / "release_receipt.json"
    _write(receipt, previous)
    assert refresh_unchanged_completion(receipt, previous, readiness)
    updated = json.loads(receipt.read_text())
    assert updated["catalog_latest_verified"] and updated["source_hashes_verified"]
    assert updated["staged_files_sha256"] == previous["staged_files_sha256"]
    assert updated["catalog_partitioned_history_verified"] is False
    stamp = receipt.stat().st_mtime_ns
    assert not refresh_unchanged_completion(receipt, updated, readiness)
    assert receipt.stat().st_mtime_ns == stamp
    with pytest.raises(RuntimeError, match="hash evidence"):
        catalog_completion_fields({**readiness, "hashes_verified": False})
    with pytest.raises(RuntimeError, match="elapsed"):
        catalog_completion_fields({**readiness, "valid_until_utc": now.isoformat()})


def test_finlab_stage_coexists_with_formal_reader_but_excludes_real_writers(tmp_path):
    from scripts.stage_tw_public_finlab_research_release import stage_source_guards
    from stockagent.data.finlab_acquisition_contract import FINLAB_RESEARCH_STAGE_LOCK
    from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
    public = tmp_path / "live/data_tw_public"
    public.mkdir(parents=True)
    (public / "twse_daily_ohlcv.parquet").touch()
    public_lock = public.parent / ".locks/tw-public-refresh.lock"
    public_lock.parent.mkdir()
    staged = public.with_name("data_tw_public_research_finlab_2014_v4")
    with public_lock.open("a+") as reader:
        fcntl.flock(reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
        with stage_source_guards(staged, public):
            with (public.parent / FINLAB_RESEARCH_STAGE_LOCK).open("a+") as writer:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(reader, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(TWPublicSourceUpdatePending):
            with stage_source_guards(staged, public):
                pytest.fail("formal writer lease was bypassed")


def test_canonical_base_refresh_reuses_original_builder_and_rejects_custom_inputs(tmp_path, monkeypatch):
    from scripts import build_finlab_research_overlay as builder
    calls = []
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: calls.append((a, kw)))
    builder.refresh_canonical_base(builder.CANONICAL_BASE)
    assert len(calls) == 3 and all(call[1]["check"] is True for call in calls)
    assert calls[0][0][0][1:] == [
        str(builder.ROOT / "scripts/build_tw_public_research_features.py"),
        "--source-update-lock-held",
    ]
    assert calls[1][0][0][1:] == [str(builder.ROOT / "scripts/build_tw_public_research_taifex.py")]
    command = calls[2][0][0]
    assert command[1] == str(builder.ROOT / "scripts/build_tw_public_research_all_features.py")
    assert command[command.index("--output-path") + 1] == str(builder.CANONICAL_BASE)
    with pytest.raises(ValueError, match="custom research input"):
        builder.refresh_canonical_base(tmp_path / "custom.parquet")
    assert len(calls) == 3


def test_canonical_base_dependency_failure_cannot_run_downstream_union(monkeypatch):
    from scripts import build_finlab_research_overlay as builder
    import subprocess
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if len(calls) == 2:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(builder.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        builder.refresh_canonical_base(builder.CANONICAL_BASE)
    assert len(calls) == 2


def test_finlab_dependency_refresh_uses_exclusive_then_shared_public_lease(tmp_path, monkeypatch):
    from scripts import build_finlab_research_overlay as builder
    import sys
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    public = tmp_path / "data_tw_public"
    public.mkdir()
    (public / "twse_daily_ohlcv.parquet").touch()
    lock = tmp_path / ".locks/tw-public-refresh.lock"
    calls = []

    def refresh(base):
        with lock.open("a+") as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
        calls.append("exclusive_refresh")

    def overlay(**kwargs):
        with lock.open("a+") as probe:
            fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
            fcntl.flock(probe, fcntl.LOCK_UN)
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append("shared_overlay")
        return {"rows": 1, "feature_columns": [], "inputs": {"sources": []},
                "reused": False, "output_path": "test.parquet"}

    monkeypatch.setattr(builder, "refresh_canonical_base", refresh)
    monkeypatch.setattr(builder, "build_finlab_research_overlay", overlay)
    monkeypatch.setattr(sys, "argv", ["build_finlab_research_overlay.py", "--refresh-canonical-base"])
    assert builder.main() == 0
    assert calls == ["exclusive_refresh", "shared_overlay"]


def test_finlab_busy_dependency_returns_retryable_without_overlay(tmp_path, monkeypatch, capsys):
    from scripts import build_finlab_research_overlay as builder
    import sys
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    public = tmp_path / "data_tw_public"
    public.mkdir()
    lock = tmp_path / ".locks/tw-public-refresh.lock"
    lock.parent.mkdir()
    monkeypatch.setattr(builder, "refresh_canonical_base", lambda _base: pytest.fail("busy writer bypassed"))
    monkeypatch.setattr(builder, "build_finlab_research_overlay", lambda **_kw: pytest.fail("busy input consumed"))
    monkeypatch.setattr(sys, "argv", ["build_finlab_research_overlay.py", "--refresh-canonical-base"])
    with lock.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert builder.main() == 75
    assert json.loads(capsys.readouterr().out) == {
        "event": "finlab_research_deferred", "reason": "public_source_writer_busy", "retryable": True,
    }
    assert not list((tmp_path / "artifacts").rglob("*.parquet"))


def test_finlab_unexpected_builder_error_is_not_dependency_deferral(tmp_path, monkeypatch):
    from scripts import build_finlab_research_overlay as builder
    import sys
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["build_finlab_research_overlay.py"])

    def fail(**_kwargs):
        raise RuntimeError("canonical Taiwan public source writer is active")

    monkeypatch.setattr(builder, "build_finlab_research_overlay", fail)
    with pytest.raises(RuntimeError, match="writer is active"):
        builder.main()


def test_finlab_stage_busy_dependency_does_not_hash_or_seal(tmp_path, monkeypatch, capsys):
    from scripts import stage_tw_public_finlab_research_release as stage
    public = tmp_path / "live/data_tw_public"
    public.mkdir(parents=True)
    (public / "twse_daily_ohlcv.parquet").touch()
    lock = public.parent / ".locks/tw-public-refresh.lock"
    lock.parent.mkdir()
    monkeypatch.setattr(stage, "catalog_readiness", lambda *_a, **_kw: pytest.fail("busy staging continued"))
    staged = public.with_name("staged")
    with lock.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert stage.main(["--live-root", str(public), "--staged-root", str(staged)]) == 75
    assert not staged.exists()
    assert json.loads(capsys.readouterr().out)["event"] == "finlab_research_stage_deferred"


@pytest.mark.parametrize("build_exit,stage_exit,expected_exit,minute_runs", [
    (75, 0, 0, True), (0, 75, 0, True), (1, 0, 1, False), (0, 1, 1, False),
])
def test_real_finlab_wrapper_keeps_independent_minutes_after_expected_deferral(
    tmp_path, build_exit, stage_exit, expected_exit, minute_runs,
):
    """Exercise actual set -e/function/command-substitution behavior, no API."""
    import subprocess
    import sys
    root = Path(__file__).resolve().parents[1]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    base = tmp_path / "artifacts/research_features/tw_public_research_all_2014_v3.parquet"
    base.parent.mkdir(parents=True)
    base.touch()
    state = tmp_path / "data_finlab/runs/latest.json"
    _write(state, {"state": "pass_complete"})
    (scripts / "runtime_env.sh").write_text(f'''
resolve_fintech_python() {{ printf '%s\\n' '{sys.executable}'; }}
date() {{ if [[ "$*" == "+%H%M%S" ]]; then printf '120000\\n'; else /bin/date "$@"; fi; }}
run_fintech_python() {{
  case "$1" in
    */finlab_refresh_policy.py) printf '%s\\n' '{{"minute_partitions_per_pass":1,"maximum_intraday_passes":1,"catalog_current":true,"history_floor_mb":50,"general_batch_size":1,"processing_threads":1,"tick_partitions_per_pass":1}}';;
    */finlab_release_gate.py) return 0;;
    */build_finlab_research_overlay.py) return {build_exit};;
    */stage_tw_public_finlab_research_release.py) return {stage_exit};;
    */check_finlab_refresh_due.py) return 0;;
    */download_finlab_history.py) printf '%s\\n' '{{"minute_residual_allowed":true,"scheduled_reserve_mb":50}}';;
    *) '{sys.executable}' "$@";;
  esac
}}
stockagent-data() {{ printf 'unexpected_publication\\n' >> unexpected.log; return 1; }}
''')
    (scripts / "download_finlab_history.py").write_text("# Fixture sweep: pass_complete already sealed locally.\n")
    (scripts / "download_finlab_market_intraday.py").write_text('''
import json
from pathlib import Path
Path("minute-called").touch()
state = Path("data_finlab/intraday/market_status.json")
state.parent.mkdir(exist_ok=True)
state.write_text(json.dumps({"state":"waiting_older_publication", "active_kind":"tw_minute",
                           "minute_priority":{"tick_residual_allowed":False}}))
''')
    result = subprocess.run(["bash", str(root / "scripts/run_finlab_refresh.sh")],
                            cwd=tmp_path, env={**os.environ, "FINLAB_REPO_ROOT": str(tmp_path)},
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == expected_exit, result.stdout + result.stderr
    assert (tmp_path / "minute-called").exists() is minute_runs
    assert not (tmp_path / "unexpected.log").exists()
