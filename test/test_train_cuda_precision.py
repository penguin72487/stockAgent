from types import SimpleNamespace

import pytest

import train


@pytest.mark.parametrize("enabled", [True, False])
def test_cli_cuda_precision_honors_resolved_config(monkeypatch, enabled):
    precisions=[]
    matmul=SimpleNamespace(allow_tf32=not enabled,
        allow_fp16_reduced_precision_reduction=False,
        allow_bf16_reduced_precision_reduction=False)
    cudnn=SimpleNamespace(allow_tf32=not enabled,benchmark=True,deterministic=True)
    backend=SimpleNamespace(matmul=matmul)
    fake=SimpleNamespace(cuda=SimpleNamespace(is_available=lambda:True),
        set_float32_matmul_precision=precisions.append,
        backends=SimpleNamespace(cuda=backend,cudnn=cudnn))
    monkeypatch.setattr(train,"torch",fake)
    monkeypatch.setattr(train,"normalize_cuda_env",lambda:None)
    train._configure_cuda_runtime(cudnn_benchmark=False,use_tensor_cores=enabled)
    assert precisions==["high" if enabled else "highest"]
    assert matmul.allow_tf32 is enabled and cudnn.allow_tf32 is enabled
    assert matmul.allow_bf16_reduced_precision_reduction # BF16 remains separate
    assert not cudnn.benchmark and not cudnn.deterministic
