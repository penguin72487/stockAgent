"""Small, explicit SwiGLU forward/adjoint kernels for compiler comparisons.

Only diagnostic callers use these kernels until complete-fold acceptance. All
implementations differentiate sigmoid(x)*x*y; no input is detached or omitted.
"""
import torch


def swiglu_operator(backend: str):
    global hl
    if backend in {"triton", "gluon"}:
        import triton
        if backend == "triton":
            import triton.language as lang
            decorator = triton.jit
            layout = None
        else:
            from triton.experimental import gluon
            from triton.experimental.gluon import language as lang
            from triton.experimental.gluon.language import BlockedLayout
            decorator = gluon.jit
            layout = BlockedLayout([1], [32], [4], [0])

        @decorator
        def kernel(A, B, G, Y, DA, DB, N: lang.constexpr,
                   BACKWARD: lang.constexpr, BLOCK: lang.constexpr,
                   LAYOUT: lang.constexpr):
            if LAYOUT is None:
                index = lang.program_id(0) * BLOCK + lang.arange(0, BLOCK)
            else:
                index = lang.program_id(0) * BLOCK + lang.arange(0, BLOCK, layout=LAYOUT)
            a = lang.load(A + index, index < N, 0).to(lang.float32)
            b = lang.load(B + index, index < N, 0).to(lang.float32)
            s = 1.0 / (1.0 + lang.exp(-a))
            if BACKWARD:
                g = lang.load(G + index, index < N, 0).to(lang.float32)
                lang.store(DA + index, g * b * s * (1.0 + a * (1.0 - s)), index < N)
                lang.store(DB + index, g * a * s, index < N)
            else:
                lang.store(Y + index, a * s * b, index < N)

        def forward(a, b):
            out = torch.empty_like(a)
            kernel[(triton.cdiv(a.numel(), 1024),)](
                a, b, a, out, out, out, a.numel(), False, 1024, layout)
            return out

        def backward(a, b, g):
            da, db = torch.empty_like(a), torch.empty_like(b)
            kernel[(triton.cdiv(a.numel(), 1024),)](
                a, b, g, da, da, db, a.numel(), True, 1024, layout)
            return da, db
    elif backend == "tilelang":
        import tilelang
        import tilelang.language as T

        @tilelang.jit(out_idx=[2])
        def fwd(n: int, dtype: str):
            @T.prim_func
            def main(A: T.Tensor((n,), dtype), B: T.Tensor((n,), dtype),
                     Y: T.Tensor((n,), dtype)):
                with T.Kernel(T.ceildiv(n, 1024), threads=128) as block:
                    for offset in T.Parallel(1024):
                        index = block * 1024 + offset
                        if index < n:
                            a = T.cast(A[index], "float32")
                            b = T.cast(B[index], "float32")
                            Y[index] = a * b / (1.0 + T.exp(-a))
            return main

        @tilelang.jit(out_idx=[3, 4])
        def bwd(n: int, dtype: str):
            @T.prim_func
            def main(A: T.Tensor((n,), dtype), B: T.Tensor((n,), dtype),
                     G: T.Tensor((n,), dtype), DA: T.Tensor((n,), dtype),
                     DB: T.Tensor((n,), dtype)):
                with T.Kernel(T.ceildiv(n, 1024), threads=128) as block:
                    for offset in T.Parallel(1024):
                        index = block * 1024 + offset
                        if index < n:
                            a = T.cast(A[index], "float32")
                            b = T.cast(B[index], "float32")
                            g = T.cast(G[index], "float32")
                            s = 1.0 / (1.0 + T.exp(-a))
                            DA[index] = g * b * s * (1.0 + a * (1.0 - s))
                            DB[index] = g * a * s
            return main

        def forward(a, b):
            return fwd(a.numel(), str(a.dtype).split(".")[-1])(a.flatten(), b.flatten()).view_as(a)

        def backward(a, b, g):
            da, db = bwd(a.numel(), str(a.dtype).split(".")[-1])(
                a.flatten(), b.flatten(), g.flatten())
            return da.view_as(a), db.view_as(b)
    elif backend == "helion":
        import helion
        import helion.language as hl

        @helion.kernel(autotune_effort="none")
        def forward(a: torch.Tensor, b: torch.Tensor):
            out = torch.empty_like(a)
            for tile in hl.tile(a.shape):
                aa, bb = a[tile].float(), b[tile].float()
                out[tile] = aa * torch.sigmoid(aa) * bb
            return out

        @helion.kernel(autotune_effort="none")
        def backward(a: torch.Tensor, b: torch.Tensor, g: torch.Tensor):
            da, db = torch.empty_like(a), torch.empty_like(b)
            for tile in hl.tile(a.shape):
                aa, bb, gg = a[tile].float(), b[tile].float(), g[tile].float()
                s = torch.sigmoid(aa)
                da[tile] = gg * bb * s * (1.0 + aa * (1.0 - s))
                db[tile] = gg * aa * s
            return da, db
    else:
        raise ValueError(backend)

    class SwiGLU(torch.autograd.Function):
        @staticmethod
        def forward(ctx, a, b):
            a, b = a.contiguous(), b.contiguous()
            ctx.save_for_backward(a, b)
            return forward(a, b)

        @staticmethod
        def backward(ctx, gradient):
            return backward(*ctx.saved_tensors, gradient.contiguous())

    return SwiGLU.apply
