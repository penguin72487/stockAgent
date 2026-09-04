from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

from stockagent.runtime_env import normalize_cuda_env, normalize_python_env


def test_coda_runner_shell_and_config_attribute_contract() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    runner = repo_root / "coda_runner.sh"

    subprocess.run(["bash", "-n", str(runner)], check=True)
    source = runner.read_text(encoding="utf-8")
    removed_attribute_patterns = (
        r"\.environment\.conda_env(?![A-Za-z0-9_])",
        r"\.training\.batch_size(?![A-Za-z0-9_])",
    )
    for pattern in removed_attribute_patterns:
        assert re.search(pattern, source) is None, f"removed config attribute remains: {pattern}"


def test_normalize_cuda_env_makes_cuda_path_and_home_match(monkeypatch, tmp_path: Path) -> None:
    cuda_root = tmp_path / "env" / "targets" / "x86_64-linux"
    include_dir = cuda_root / "include"
    include_dir.mkdir(parents=True)
    (include_dir / "cuda_runtime.h").write_text("", encoding="utf-8")
    other_root = tmp_path / "other"
    other_root.mkdir()

    monkeypatch.setenv("STOCKAGENT_CUDA_ROOT", str(cuda_root))
    monkeypatch.setenv("CUDA_PATH", str(cuda_root))
    monkeypatch.setenv("CUDA_HOME", str(other_root))

    selected = normalize_cuda_env()

    assert selected == cuda_root
    assert os.environ["CUDA_PATH"] == str(cuda_root)
    assert os.environ["CUDA_HOME"] == str(cuda_root)
    assert os.environ["CUDAToolkit_ROOT"] == str(cuda_root)


def test_normalize_python_env_removes_inherited_conda_prefix(monkeypatch, tmp_path: Path) -> None:
    selected = tmp_path / "fintech"
    inherited = tmp_path / "main"
    (selected / "bin").mkdir(parents=True)
    (selected / "conda-meta").mkdir()
    (inherited / "bin").mkdir(parents=True)

    monkeypatch.setattr(sys, "prefix", str(selected))
    monkeypatch.setattr(sys, "executable", str(selected / "bin" / "python"))
    monkeypatch.setenv("CONDA_PREFIX", str(inherited))
    monkeypatch.setenv("CONDA_DEFAULT_ENV", "main")
    monkeypatch.setenv("PATH", f"{inherited / 'bin'}:/usr/bin")

    prefix = normalize_python_env()

    assert prefix == selected.resolve()
    assert os.environ["CONDA_PREFIX"] == str(selected.resolve())
    assert os.environ["CONDA_DEFAULT_ENV"] == "fintech"
    assert os.environ["PATH"].split(os.pathsep)[0] == str(selected / "bin")
    assert str(inherited / "bin") not in os.environ["PATH"].split(os.pathsep)


def test_shell_runtime_uses_python_override_as_single_source_of_truth(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    selected = tmp_path / "selected" / "fintech"
    inherited = tmp_path / "inherited" / "main"
    other = tmp_path / "other" / "fintech"
    for prefix in (selected, inherited, other):
        (prefix / "bin").mkdir(parents=True)
    (selected / "bin" / "python").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (selected / "bin" / "python").chmod(0o755)
    (other / "bin" / "python").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (other / "bin" / "python").chmod(0o755)
    cuda_root = selected / "targets" / "x86_64-linux"
    (cuda_root / "include").mkdir(parents=True)
    (cuda_root / "include" / "cuda_runtime.h").write_text("", encoding="utf-8")

    command = r'''
source "$1/scripts/runtime_env.sh"
printf '%s\n%s\n%s\n%s\n%s\n' \
  "$PYTHON_BIN" "$FINTECH_ENV_PATH" "$CONDA_PREFIX" "$CUDA_HOME" "$PATH"
'''
    env = {
        **os.environ,
        "PYTHON_BIN": str(selected / "bin" / "python"),
        "FINTECH_ENV_PATH": str(other),
        "CONDA_PREFIX": str(inherited),
        "CONDA_DEFAULT_ENV": "main",
        "PATH": f"{inherited / 'bin'}:/usr/bin:/bin",
        "CUDA_HOME": "/does/not/exist",
        "CUDA_PATH": "/also/invalid",
    }
    result = subprocess.run(
        ["bash", "-c", command, "bash", str(repo_root)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    python, prefix, conda_prefix, cuda_home, path = result.stdout.splitlines()

    assert python == str(selected / "bin" / "python")
    assert prefix == str(selected)
    assert conda_prefix == str(selected)
    assert cuda_home == str(cuda_root)
    assert path.split(":")[0] == str(selected / "bin")
    assert str(inherited / "bin") not in path.split(":")


def test_shell_runtime_can_be_sourced_without_home() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    command = r'''
set -u
source "$1/scripts/runtime_env.sh"
printf 'runtime-ready\n'
'''
    result = subprocess.run(
        ["/bin/bash", "-c", command, "bash", str(repo_root)],
        check=True,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
    )

    assert result.stdout == "runtime-ready\n"


def test_cuml_umap_uses_random_init_to_avoid_spectral_fallback_warning(monkeypatch) -> None:
    from stockagent.backtest import gpu_plot

    captured: dict[str, object] = {}

    class FakeUMAP:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gpu_plot, "_import_cuml_umap", lambda: FakeUMAP)

    reducer = gpu_plot._new_cuml_umap(n_components=2, n_neighbors=3)

    assert isinstance(reducer, FakeUMAP)
    assert captured["init"] == "random"
    assert captured["n_components"] == 2
    assert captured["n_neighbors"] == 3
