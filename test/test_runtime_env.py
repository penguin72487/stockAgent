from __future__ import annotations

import os
from pathlib import Path

from stockagent.runtime_env import normalize_cuda_env


def test_normalize_cuda_env_makes_cuda_path_and_home_match(monkeypatch, tmp_path: Path) -> None:
    cuda_root = tmp_path / "env" / "targets" / "x86_64-linux"
    include_dir = cuda_root / "include"
    include_dir.mkdir(parents=True)
    (include_dir / "cuda_runtime.h").write_text("", encoding="utf-8")
    other_root = tmp_path / "other"
    other_root.mkdir()

    monkeypatch.setenv("CUDA_PATH", str(cuda_root))
    monkeypatch.setenv("CUDA_HOME", str(other_root))
    monkeypatch.delenv("STOCKAGENT_CUDA_ROOT", raising=False)

    selected = normalize_cuda_env()

    assert selected == cuda_root
    assert os.environ["CUDA_PATH"] == str(cuda_root)
    assert os.environ["CUDA_HOME"] == str(cuda_root)
    assert os.environ["CUDAToolkit_ROOT"] == str(cuda_root)


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
