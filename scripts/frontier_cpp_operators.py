"""Lazy, isolated C++ tile probes with explicit autograd equations."""
from pathlib import Path
import os
import torch


def load_extension(backend):
    from torch.utils.cpp_extension import load
    root = Path(os.environ["STOCKAGENT_FRONTIER_PACKAGE_ROOT"])
    os.environ.setdefault("MAX_JOBS", "4")
    os.environ.setdefault("TORCH_CUDA_ARCH_LIST", "12.0a")
    if backend == "thunderkittens":
        source = Path(__file__).with_name("frontier_tk_swiglu.cu")
        includes = [root / "thunderkittens-source/include"]
        flags = ["-std=c++20", "-DKITTENS_SM120", "--expt-relaxed-constexpr", "--extended-lambda"]
    elif backend == "cublasdx":
        source = Path(__file__).with_name("frontier_cublasdx_gemm.cu")
        math = next((root / "mathdx-source").rglob("include/cublasdx.hpp")).parent
        includes = [math, math.parent / "external/cutlass/include"]
        flags = ["-std=c++20", "--expt-relaxed-constexpr"]
    else:
        raise ValueError(backend)
    # Keep CUDA 12 ahead of Conda's inherited CUDA 13 link search path.
    cuda = Path(os.environ["STOCKAGENT_CUDA_ROOT"])
    # PyTorch disables CUDA scalar operators by default; both header-only
    # libraries use those operators in their own half/BF16 type definitions.
    flags += ["-U__CUDA_NO_HALF_OPERATORS__", "-U__CUDA_NO_HALF_CONVERSIONS__",
        "-U__CUDA_NO_BFLOAT16_CONVERSIONS__", "-U__CUDA_NO_HALF2_OPERATORS__"]
    return load(name=f"frontier_{backend}_v1", sources=[str(source)],
        extra_include_paths=[str(p) for p in includes], extra_cflags=["-std=c++20"],
        extra_cuda_cflags=flags, extra_ldflags=[f"-L{cuda / 'lib64'}", f"-Wl,-rpath,{cuda / 'lib64'}"],
        verbose=True)


def cpp_operator(backend):
    extension = load_extension(backend)
    if backend == "thunderkittens":
        class SwiGLU(torch.autograd.Function):
            @staticmethod
            def forward(ctx, a, b):
                ctx.save_for_backward(a, b)
                return extension.run(a.float(), b.float(), a.float(), False)[0].to(a.dtype)
            @staticmethod
            def backward(ctx, g):
                a, b = ctx.saved_tensors
                da, db = extension.run(a.float(), b.float(), g.float().contiguous(), True)
                return da.to(a.dtype), db.to(b.dtype)
        return SwiGLU.apply
    class Linear(torch.autograd.Function):
        @staticmethod
        def forward(ctx, x, w, b):
            ctx.save_for_backward(x, w)
            return extension.mm(x.contiguous(), w.T.contiguous()) + b
        @staticmethod
        def backward(ctx, g):
            x, w = ctx.saved_tensors
            return (extension.mm(g.contiguous(), w.contiguous()),
                extension.mm(g.T.contiguous(), x.contiguous()), g.sum(0))
    return Linear.apply
