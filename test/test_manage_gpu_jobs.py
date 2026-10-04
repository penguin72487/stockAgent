from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "manage_gpu_jobs.py"
SPEC = importlib.util.spec_from_file_location("manage_gpu_jobs", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "jobs.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_load_spec_merges_defaults_and_parses_gpu_indices(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
defaults:
  enabled: true
jobs:
  - name: crypto
    config: configs/markets/crypto.yaml
    gpus: [1, 2]
""",
    )
    _, jobs = MODULE._load_spec(path)
    assert jobs == [
        {
            "enabled": True,
            "name": "crypto",
            "config": "configs/markets/crypto.yaml",
            "gpus": [1, 2],
        }
    ]


def test_load_spec_rejects_gpu_collision(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
jobs:
  - {name: us, config: configs/markets/us.yaml, gpus: [0]}
  - {name: crypto, config: configs/markets/crypto.yaml, gpus: [0]}
""",
    )
    with pytest.raises(ValueError, match="assigned to both"):
        MODULE._load_spec(path)


def test_disabled_job_does_not_reserve_gpu(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
jobs:
  - {name: us, config: configs/markets/us.yaml, gpus: [0], enabled: false}
  - {name: crypto, config: configs/markets/crypto.yaml, gpus: [0]}
""",
    )
    _, jobs = MODULE._load_spec(path)
    assert len(jobs) == 2


def test_load_spec_normalizes_fold_range_and_output_dir(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
jobs:
  - name: tw_25
    config: configs/markets/tw.yaml
    gpus: [0]
    fold_range: [25, 26]
    output_dir: artifacts/tw_25_26
""",
    )
    _, jobs = MODULE._load_spec(path)
    assert jobs[0]["fold_range"] == [25, 26]
    assert jobs[0]["output_dir"] == "artifacts/tw_25_26"


def test_load_spec_rejects_shared_output_dir(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
allow_gpu_sharing: true
jobs:
  - {name: a, config: configs/markets/tw.yaml, gpus: [0], output_dir: artifacts/shared}
  - {name: b, config: configs/markets/tw.yaml, gpus: [1], output_dir: artifacts/shared}
""",
    )
    with pytest.raises(ValueError, match="distinct output directories"):
        MODULE._load_spec(path)


def test_selected_rejects_unknown_job() -> None:
    jobs = [{"name": "us"}]
    with pytest.raises(ValueError, match="unknown job"):
        MODULE._selected(jobs, ["crypto"])


def test_reused_pid_cannot_stop_unrelated_process(tmp_path, monkeypatch):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    from downloader.artifact_io import atomic_write_json
    atomic_write_json(state_dir / "test.json", {
        "pid": 123, "process": {"pid": 123, "start_ticks": "old", "boot_id": "same"},
    })
    monkeypatch.setattr(MODULE, "_alive", lambda pid: True)
    monkeypatch.setattr(MODULE, "_process_identity", lambda pid: {
        "pid": pid, "start_ticks": "new", "boot_id": "same",
    })
    signals = []
    monkeypatch.setattr(MODULE.os, "killpg", lambda *args: signals.append(args))
    with pytest.raises(ValueError, match="refusing signal"):
        MODULE._stop(tmp_path / "jobs.yaml", {"state_dir": str(state_dir)}, [{"name": "test"}], 0)
    assert signals == []


def test_independent_gpu_work_blocks_launch(tmp_path, monkeypatch):
    monkeypatch.setattr(MODULE, "_validate_files_and_gpus", lambda jobs: None)
    monkeypatch.setattr(MODULE, "_busy_gpu_indices", lambda: {1})
    launches = []
    monkeypatch.setattr(MODULE.subprocess, "Popen", lambda *args, **kwargs: launches.append(args))
    with pytest.raises(ValueError, match="independent process"):
        MODULE._start(tmp_path / "jobs.yaml", {"state_dir": str(tmp_path / "state")}, [
            {"name": "test", "config": "existing.yaml", "gpus": [1]},
        ])
    assert launches == []


def test_receipt_name_cannot_escape_state_directory(tmp_path):
    path = _write(tmp_path, "jobs:\n  - {name: ../foreign, config: configs/markets/us.yaml, gpus: [0]}\n")
    with pytest.raises(ValueError, match="safe receipt basename"):
        MODULE._load_spec(path)


def test_gpu_lease_covers_pre_cuda_work_and_releases_after_exit(tmp_path, monkeypatch):
    import subprocess
    import sys
    monkeypatch.setenv("STOCKAGENT_GPU_LEASE_ROOT", str(tmp_path / "leases"))
    marker = tmp_path / "started"
    code = "import pathlib,time; pathlib.Path(%r).write_text('started'); time.sleep(10)" % str(marker)
    command = MODULE._gpu_lease_command([sys.executable, "-c", code], [0, 1], sharing=False)
    process = subprocess.Popen(command, start_new_session=True)
    try:
        import time
        deadline = time.monotonic() + 3
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        contender = MODULE._gpu_lease_command([sys.executable, "-c", "pass"], [1], sharing=False)
        assert subprocess.run(contender).returncode != 0
    finally:
        import os
        import signal
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=3)
    assert subprocess.run(contender).returncode == 0


def test_job_environment_cannot_override_admitted_gpu(tmp_path):
    path = _write(tmp_path, """
jobs:
  - name: worker
    config: configs/markets/tw.yaml
    gpus: [0]
    env: {CUDA_VISIBLE_DEVICES: '1'}
""")
    with pytest.raises(ValueError, match="through gpus"):
        MODULE._load_spec(path)


def test_output_aliases_cannot_share_artifacts(tmp_path):
    path = _write(tmp_path, """
jobs:
  - {name: a, config: configs/markets/tw.yaml, gpus: [0], output_dir: artifacts/shared}
  - {name: b, config: configs/markets/tw.yaml, gpus: [1], output_dir: artifacts/nested/../shared}
""")
    with pytest.raises(ValueError, match="distinct output directories"):
        MODULE._load_spec(path)


def test_sharing_requires_boolean_and_disabled_output_does_not_reserve(tmp_path):
    path = _write(tmp_path, "allow_gpu_sharing: 'false'\njobs: []\n")
    with pytest.raises(ValueError, match="explicit boolean"):
        MODULE._load_spec(path)
    path = _write(tmp_path, """
jobs:
  - {name: a, config: configs/markets/tw.yaml, gpus: [0], output_dir: artifacts/shared, enabled: false}
  - {name: b, config: configs/markets/tw.yaml, gpus: [1], output_dir: artifacts/shared}
""")
    assert len(MODULE._load_spec(path)[1]) == 2
