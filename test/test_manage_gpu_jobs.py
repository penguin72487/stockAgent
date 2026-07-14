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
