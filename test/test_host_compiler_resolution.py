from pathlib import Path

import pytest

from stockagent.training import trainer
from stockagent.training import runtime_configuration


def _executable(path: Path) -> str:
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(0o700)
    return str(path)


@pytest.mark.parametrize("unusable", ["missing", "nonexecutable", "directory"])
def test_host_compilers_skip_unusable_conda_candidates(tmp_path, monkeypatch, unusable):
    env_bin = tmp_path / "venv" / "bin"
    env_bin.mkdir(parents=True)
    python = env_bin / "python"
    python.touch()
    monkeypatch.setattr(trainer.sys, "executable", str(python))
    system_bin = tmp_path / "system-bin"
    system_bin.mkdir()
    expected = (_executable(system_bin / "cc"), _executable(system_bin / "c++"))
    monkeypatch.setenv("PATH", str(system_bin))
    for variable, name in (("CC", "x86_64-conda-linux-gnu-gcc"),
                           ("CXX", "x86_64-conda-linux-gnu-g++")):
        candidate = env_bin / name
        if unusable == "nonexecutable":
            candidate.touch(mode=0o600)
        elif unusable == "directory":
            candidate.mkdir(mode=0o700)
        monkeypatch.setenv(variable, str(candidate))
    assert trainer._resolve_host_compilers() == expected
    monkeypatch.setenv("CONDA_PREFIX", str(env_bin.parent))
    monkeypatch.setattr(runtime_configuration, "_configure_compile_cache_paths", lambda: None)
    monkeypatch.setattr(runtime_configuration, "normalize_cuda_env", lambda: None)
    runtime_configuration._prepend_compile_toolchain_paths()
    # The financial runtime must export the choice even when the model's
    # separate torch.compile admission function is never called.
    assert (trainer.os.environ["CC"], trainer.os.environ["CXX"]) == expected


def test_host_compilers_respect_executable_explicit_paths(tmp_path, monkeypatch):
    expected = (_executable(tmp_path / "selected-cc"),
                _executable(tmp_path / "selected-cxx"))
    monkeypatch.setenv("CC", expected[0])
    monkeypatch.setenv("CXX", expected[1])
    assert trainer._resolve_host_compilers() == expected


def test_host_compilers_do_not_admit_nonexistent_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(trainer.sys, "executable", str(tmp_path / "missing-env" / "python"))
    monkeypatch.setenv("PATH", "")
    monkeypatch.delenv("CC", raising=False)
    monkeypatch.delenv("CXX", raising=False)
    assert trainer._resolve_host_compilers() == (None, None)
