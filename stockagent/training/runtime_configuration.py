"""Process-wide compile and backtest runtime configuration.

This module is safe for training, inference, live signals, and explainability; it
does not import trainer orchestration.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from stockagent.config import ExperimentConfig
from stockagent.runtime_env import normalize_cuda_env


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "on", "yes"}


def _runtime_cache_path(value: object) -> Path | None:
    raw = str(value or "").strip()
    if raw.lower() in {"", "0", "false", "off", "no", "none"}:
        return None
    return Path(os.path.expandvars(raw)).expanduser()


def _configure_compile_cache_paths(
    *,
    torchinductor_cache_dir: object = "~/.cache/torchinductor",
    triton_cache_dir: object = "~/.cache/triton",
    cuda_cache_path: object = "~/.cache/nv_cuda",
    force: bool = False,
) -> None:
    for env_name, raw_path in (
        ("TORCHINDUCTOR_CACHE_DIR", torchinductor_cache_dir),
        ("TRITON_CACHE_DIR", triton_cache_dir),
        ("CUDA_CACHE_PATH", cuda_cache_path),
    ):
        path = _runtime_cache_path(raw_path)
        if path is None:
            continue
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception:
            continue
        if force or not os.environ.get(env_name):
            os.environ[env_name] = str(path)


def _prepend_compile_toolchain_paths() -> None:
    _configure_compile_cache_paths()
    normalize_cuda_env()
    entries: list[str] = []
    env_bin = Path(sys.executable).resolve().parent
    env_root = env_bin.parent
    os.environ.setdefault("CONDA_PREFIX", str(env_root))
    ptxas_path = env_bin / "ptxas"
    if ptxas_path.exists():
        os.environ.setdefault("TRITON_PTXAS_PATH", str(ptxas_path))
        os.environ.setdefault("TRITON_PTXAS_BLACKWELL_PATH", str(ptxas_path))
    cc_path = env_bin / "x86_64-conda-linux-gnu-gcc"
    cxx_path = env_bin / "x86_64-conda-linux-gnu-g++"
    if cc_path.exists():
        os.environ.setdefault("CC", str(cc_path))
    if cxx_path.exists():
        os.environ.setdefault("CXX", str(cxx_path))
    entries.append(str(env_bin))
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        entries.append(str(Path(conda_prefix) / "bin"))
    try:
        import site

        for site_dir in site.getsitepackages():
            entries.append(str(Path(site_dir) / "nvidia" / "cuda_nvcc" / "bin"))
    except Exception:
        pass

    existing = os.environ.get("PATH", "")
    existing_parts = [part for part in existing.split(os.pathsep) if part]
    prepend = [
        part
        for part in entries
        if part and Path(part).exists() and part not in existing_parts
    ]
    if prepend:
        os.environ["PATH"] = os.pathsep.join([*prepend, existing])


def _configure_torch_compile_runtime() -> None:
    try:
        import torch._dynamo.config as dynamo_config

        dynamo_config.log_compilation_metrics = False
    except Exception:
        pass
    try:
        import torch._inductor.config as inductor_config

        # PyTorch CUDA 13 / RTX 5090 currently hits an Inductor scheduler
        # assertion in MixOrderReduction for some compiled backward shapes
        # such as S=1024, D=32. Keep Inductor/Triton compile enabled but avoid
        # this one unstable fusion pass unless explicitly re-enabled.
        inductor_config.triton.mix_order_reduction = _env_truthy(
            "STOCKAGENT_INDUCTOR_MIX_ORDER_REDUCTION",
            "0",
        )
    except Exception:
        pass
    try:
        from torch._dynamo.utils import CompileEventLogger

        CompileEventLogger.compilation_metric = staticmethod(
            lambda *args, **kwargs: None
        )
    except Exception:
        pass


def _configure_backtest_runtime_from_config(config: ExperimentConfig) -> None:
    training = config.training
    _configure_compile_cache_paths(
        torchinductor_cache_dir=getattr(
            training, "torchinductor_cache_dir", "~/.cache/torchinductor"
        ),
        triton_cache_dir=getattr(training, "triton_cache_dir", "~/.cache/triton"),
        cuda_cache_path=getattr(training, "cuda_cache_path", "~/.cache/nv_cuda"),
        force=True,
    )
    _prepend_compile_toolchain_paths()
    _configure_torch_compile_runtime()
    os.environ["STOCKAGENT_BACKTEST_AUTOTUNE"] = (
        "1" if bool(training.backtest_autotune) else "0"
    )
    os.environ["STOCKAGENT_BACKTEST_COMPILE"] = (
        "1" if bool(training.backtest_compile) else "0"
    )
    os.environ["STOCKAGENT_BACKTEST_COMPILE_STATEFUL"] = (
        "1" if bool(training.backtest_compile_stateful) else "0"
    )
    os.environ["STOCKAGENT_TW_DUAL_SESSION_CUDA_GRAPH"] = (
        "1" if bool(training.tw_dual_session_cuda_graph) else "0"
    )
    os.environ["STOCKAGENT_SYMBOL_SHARDED_PACK_METADATA"] = (
        "1" if bool(training.distributed_symbol_sharded_pack_metadata) else "0"
    )
    os.environ["STOCKAGENT_SYMBOL_SHARDED_PACK_SCALARS"] = (
        "1" if bool(training.distributed_symbol_sharded_pack_scalars) else "0"
    )
    os.environ["STOCKAGENT_SYMBOL_SHARDED_SKIP_NOOP_COLLECTIVES"] = (
        "1"
        if bool(training.distributed_symbol_sharded_skip_noop_collectives)
        else "0"
    )
    os.environ["STOCKAGENT_BACKTEST_COMPILE_DYNAMIC"] = (
        "1" if bool(getattr(training, "backtest_compile_dynamic", False)) else "0"
    )
    os.environ["STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS"] = str(
        max(0, int(getattr(training, "tw_continuous_compile_chunk_rows", 8)))
    )
    os.environ["STOCKAGENT_TW_CONTINUOUS_GRADIENT_HORIZON_ROWS"] = str(
        max(
            0,
            int(getattr(training, "tw_continuous_gradient_horizon_rows", 32)),
        )
    )
    eval_backtest_compile = getattr(training, "eval_backtest_compile", None)
    if eval_backtest_compile is not None:
        os.environ["STOCKAGENT_EVAL_BACKTEST_COMPILE"] = (
            "1" if bool(eval_backtest_compile) else "0"
        )
    os.environ["STOCKAGENT_BACKTEST_VERBOSE"] = (
        "1" if bool(training.backtest_verbose) else "0"
    )
    os.environ["STOCKAGENT_STRICT_NO_FALLBACK"] = (
        "1" if bool(training.strict_no_fallback) else "0"
    )
    os.environ["STOCKAGENT_BACKTEST_CHECKPOINT_CHUNK_ROWS"] = str(
        max(0, int(training.backtest_checkpoint_chunk_rows))
    )


def configure_inference_runtime(config: ExperimentConfig) -> None:
    """Apply the canonical backtest/compile environment for a config."""
    _configure_backtest_runtime_from_config(config)


__all__ = ["configure_inference_runtime"]
