#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import torch
import yaml

from stockagent.models import build_model


@dataclass
class CheckResult:
    name: str
    available: bool
    ok: bool
    detail: str
    ms: float | None = None


def _gpu_capability() -> tuple[int, int] | None:
    if not torch.cuda.is_available():
        return None
    return torch.cuda.get_device_capability()


def _sm_code() -> int:
    cap = _gpu_capability()
    if cap is None:
        return 0
    return cap[0] * 10 + cap[1]


def _time_cuda(fn, warmup: int = 5, iters: int = 20) -> float:
    for _ in range(max(0, warmup)):
        fn()
    if not torch.cuda.is_available():
        t0 = time.perf_counter()
        for _ in range(max(1, iters)):
            fn()
        return (time.perf_counter() - t0) * 1000.0 / max(1, iters)

    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    torch.cuda.synchronize()
    start.record()
    for _ in range(max(1, iters)):
        fn()
    end.record()
    torch.cuda.synchronize()
    return float(start.elapsed_time(end) / max(1, iters))


def _check_xformers(dtype: torch.dtype, device: torch.device) -> CheckResult:
    try:
        import xformers.ops as xops  # type: ignore
    except Exception as exc:
        return CheckResult("xformers", available=False, ok=False, detail=f"import failed: {exc}")

    b, t, h, hd = 4, 64, 4, 32
    q = torch.randn(b, t, h, hd, device=device, dtype=dtype)
    k = torch.randn(b, t, h, hd, device=device, dtype=dtype)
    v = torch.randn(b, t, h, hd, device=device, dtype=dtype)

    try:
        def _run() -> None:
            _ = xops.memory_efficient_attention(q, k, v, p=0.0)

        ms = _time_cuda(_run)
        return CheckResult("xformers", available=True, ok=True, detail="memory_efficient_attention ok", ms=ms)
    except Exception as exc:
        return CheckResult(
            "xformers",
            available=True,
            ok=False,
            detail=f"installed but CUDA extension path unavailable: {exc}",
        )


def _check_flash_attn(dtype: torch.dtype, device: torch.device) -> CheckResult:
    try:
        from flash_attn import flash_attn_func  # type: ignore
    except Exception as exc:
        return CheckResult("flash_attn_2", available=False, ok=False, detail=f"import failed: {exc}")

    b, t, h, hd = 4, 64, 4, 32
    q = torch.randn(b, t, h, hd, device=device, dtype=dtype)
    k = torch.randn(b, t, h, hd, device=device, dtype=dtype)
    v = torch.randn(b, t, h, hd, device=device, dtype=dtype)

    try:
        def _run() -> None:
            _ = flash_attn_func(q, k, v, dropout_p=0.0, causal=False)

        ms = _time_cuda(_run)
        return CheckResult("flash_attn_2", available=True, ok=True, detail="flash_attn_func ok", ms=ms)
    except Exception as exc:
        return CheckResult("flash_attn_2", available=True, ok=False, detail=f"runtime failed: {exc}")


def _check_transformer_engine(device: torch.device) -> CheckResult:
    try:
        import transformer_engine.pytorch as te  # type: ignore
    except Exception as exc:
        return CheckResult("transformer_engine", available=False, ok=False, detail=f"import failed: {exc}")

    sm = _sm_code()
    fp8_capable = sm >= 89
    detail = f"import ok; SM={sm}; fp8_capable={fp8_capable}"

    try:
        lin = te.Linear(128, 128).to(device=device, dtype=torch.bfloat16)
        x = torch.randn(8, 128, device=device, dtype=torch.bfloat16)

        def _run() -> None:
            _ = lin(x)

        ms = _time_cuda(_run)
        return CheckResult("transformer_engine", available=True, ok=True, detail=detail, ms=ms)
    except Exception as exc:
        return CheckResult("transformer_engine", available=True, ok=False, detail=f"import ok but runtime failed: {exc}")


def _check_sdpa(dtype: torch.dtype, device: torch.device) -> CheckResult:
    b, h, t, hd = 4, 4, 64, 32
    q = torch.randn(b, h, t, hd, device=device, dtype=dtype)
    k = torch.randn(b, h, t, hd, device=device, dtype=dtype)
    v = torch.randn(b, h, t, hd, device=device, dtype=dtype)

    try:
        def _run() -> None:
            _ = torch.nn.functional.scaled_dot_product_attention(q, k, v, dropout_p=0.0, is_causal=False)

        ms = _time_cuda(_run)
        return CheckResult("sdpa", available=True, ok=True, detail="scaled_dot_product_attention ok", ms=ms)
    except Exception as exc:
        return CheckResult("sdpa", available=True, ok=False, detail=f"runtime failed: {exc}")


def _check_portfolio_backends(
    model_cfg: dict[str, Any],
    device: torch.device,
    symbols: int,
    features: int,
    lookback: int,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    x = torch.randn(2, lookback, symbols, features, device=device, dtype=torch.bfloat16)
    mask = torch.randint(0, 2, (2, symbols), device=device, dtype=torch.bool)

    for backend in ["sdpa", "xformers", "flash_attn_2", "auto"]:
        cfg = dict(model_cfg)
        cfg["attention_backend"] = backend
        try:
            model = build_model(
                model_name="portfolio_transformer",
                num_features=features,
                num_symbols=symbols,
                long_only=False,
                model_params=cfg,
            ).to(device=device, dtype=x.dtype)
            model = model.eval()

            def _run() -> None:
                with torch.inference_mode():
                    _ = model(x, mask)

            ms = _time_cuda(_run)
            results.append(
                CheckResult(
                    name=f"portfolio_backend:{backend}",
                    available=True,
                    ok=True,
                    detail="forward ok",
                    ms=ms,
                )
            )
        except Exception as exc:
            results.append(
                CheckResult(
                    name=f"portfolio_backend:{backend}",
                    available=True,
                    ok=False,
                    detail=f"forward failed: {exc}",
                )
            )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate acceleration stack and backend availability.")
    parser.add_argument("--config", default="configs/models/portfolio_transformer.yaml", help="Portfolio model config")
    parser.add_argument("--symbols", type=int, default=724)
    parser.add_argument("--features", type=int, default=7)
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--output", default="", help="Optional JSON output path")
    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    dtype = torch.bfloat16 if device.type == "cuda" else torch.float32

    info = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "torch": torch.__version__,
        "torch_cuda": torch.version.cuda,
        "cuda_available": torch.cuda.is_available(),
        "gpu_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        "gpu_capability": _gpu_capability(),
    }

    model_cfg_path = Path(args.config)
    with model_cfg_path.open("r", encoding="utf-8") as handle:
        model_cfg = yaml.safe_load(handle) or {}

    checks: list[CheckResult] = [
        _check_sdpa(dtype, device),
        _check_xformers(dtype, device),
        _check_flash_attn(dtype, device),
        _check_transformer_engine(device),
    ]
    checks.extend(
        _check_portfolio_backends(
            model_cfg=model_cfg,
            device=device,
            symbols=args.symbols,
            features=args.features,
            lookback=args.lookback,
        )
    )

    payload = {
        "env": info,
        "checks": [asdict(item) for item in checks],
    }

    print(json.dumps(payload, indent=2, ensure_ascii=False))

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved report: {out_path}")


if __name__ == "__main__":
    main()
