from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from scripts import benchmark_discord_artifact_retry_probe as benchmark
from scripts import check_outside_tw_opening_resource_window as window


@pytest.mark.parametrize(
    "kwargs", [{"rounds": 0}, {"rounds": 11}, {"timeout": 0}, {"timeout": 61}]
)
def test_invalid_budget_rejects_before_any_worker_or_file(
    tmp_path, monkeypatch, kwargs
):
    monkeypatch.setattr(
        benchmark.subprocess, "run", lambda *a, **kw: pytest.fail("unexpected worker")
    )
    with pytest.raises(ValueError):
        benchmark.benchmark(tmp_path / "missing.json", **kwargs)


@pytest.mark.parametrize("defect", [None, "parity", "model_import", "worker_failed"])
def test_abba_uses_equal_private_snapshot_and_keeps_real_receipt(
    tmp_path, monkeypatch, defect
):
    path = tmp_path / "status.json"
    source = b'{"jobs": {"fixture": {"status": "failed"}}}'
    path.write_bytes(source)
    monkeypatch.setattr(window, "evaluate", lambda *a, **kw: {"allowed": True})
    calls = []

    def worker(command, **kwargs):
        calls.append((command, kwargs))
        snapshot = Path(kwargs["env"]["STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH"])
        assert snapshot != path and snapshot.read_bytes() == source
        assert command[-1] in {"legacy_import", "receipt_probe"}
        if defect == "worker_failed":
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "benchmark_probe": {
                        "due": False,
                        "due_keys": [],
                        "reason": "changed"
                        if defect == "parity" and len(calls) == 2
                        else "idle",
                        "job_count": 1,
                        "ignored_invalid_jobs": 0,
                        "probe_seconds": 0.001,
                        "model_runtime_imported": command[-1] == "legacy_import"
                        or defect == "model_import",
                    },
                    "worker_peak_rss_bytes": 1000,
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(benchmark.subprocess, "run", worker)
    if defect:
        with pytest.raises((ValueError, subprocess.CalledProcessError)):
            benchmark.benchmark(path, rounds=1)
    else:
        result = benchmark.benchmark(path, rounds=1)
        assert result["decision_parity"] and result["source_unchanged"]
        assert result["samples_per_variant"] == 2
        assert result["dispatch_requests"] == 0
        assert [call[0][-1] for call in calls] == [
            "legacy_import",
            "receipt_probe",
            "receipt_probe",
            "legacy_import",
        ]
    assert path.read_bytes() == source
    snapshot = Path(calls[0][1]["env"]["STOCKAGENT_ARTIFACT_BACKFILL_STATUS_PATH"])
    assert not snapshot.parent.exists()


def test_market_runway_protects_legacy_import_benchmark(tmp_path, monkeypatch):
    monkeypatch.setattr(window, "evaluate", lambda *a, **kw: {"allowed": False})
    with pytest.raises(RuntimeError, match="safe market runway"):
        benchmark.benchmark(tmp_path / "never_read.json")
