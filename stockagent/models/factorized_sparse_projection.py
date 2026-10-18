"""Full-schema affine projection from resident immutable CSR observations."""
from __future__ import annotations

from functools import lru_cache
import ctypes
import os
from pathlib import Path
import threading

import torch
from torch.nn import functional as F

from stockagent.data.factorized_sparse_rows import FactorizedSparseRowStore


def sparse_adjoint_contract():
    backend = os.environ.get('STOCKAGENT_SPARSE_ADJOINT_BACKEND', 'native')
    if backend == 'native':
        return None
    if backend != 'cusparse_csr_transpose':
        raise ValueError('sparse adjoint backend must be native or cusparse_csr_transpose')
    algorithm = int(os.environ.get('STOCKAGENT_SPARSE_ADJOINT_ALGORITHM', '6'))
    if algorithm not in (0, 6):
        raise ValueError('direct CSR transpose SpMM algorithm must be 0 or 6')
    return dict(version='immutable_csr_direct_transpose_fp32_spmm_v1', algorithm=algorithm)


class _DirectCSRTranspose:
    """Use op(A)=transpose on resident CSR instead of converting CSC to CSR.

    Descriptors borrow tensors only for their synchronous host submission.
    PyTorch owns every GPU allocation and its consuming-stream lifetime.
    The handle is thread/device-owned; no process-wide stream is changed.
    """
    def __init__(self):
        paths = {line.rsplit(' ', 1)[-1] for line in Path('/proc/self/maps').read_text().splitlines()
                 if 'libcusparse.so' in line and '/' in line}
        if len(paths) != 1:
            raise RuntimeError(f'cannot identify the cuSPARSE library already loaded by PyTorch: {paths}')
        self.lib = ctypes.CDLL(paths.pop())
        pointer, integer, size = ctypes.c_void_p, ctypes.c_int, ctypes.c_int64
        signatures = {
            'cusparseCreate': [ctypes.POINTER(pointer)], 'cusparseDestroy': [pointer],
            'cusparseSetStream': [pointer, pointer],
            'cusparseCreateCsr': [ctypes.POINTER(pointer), size, size, size,
                                 pointer, pointer, pointer, integer, integer, integer, integer],
            'cusparseDestroySpMat': [pointer],
            'cusparseCreateDnMat': [ctypes.POINTER(pointer), size, size, size, pointer, integer, integer],
            'cusparseDestroyDnMat': [pointer],
            'cusparseSpMM_bufferSize': [pointer, integer, integer, pointer, pointer, pointer,
                                       pointer, pointer, integer, integer, ctypes.POINTER(ctypes.c_size_t)],
            'cusparseSpMM': [pointer, integer, integer, pointer, pointer, pointer,
                             pointer, pointer, integer, integer, pointer],
        }
        for name, signature in signatures.items():
            function = getattr(self.lib, name)
            function.argtypes, function.restype = signature, integer
        self.handle = pointer()
        self.call('cusparseCreate', ctypes.byref(self.handle))

    def call(self, name, *arguments):
        status = getattr(self.lib, name)(*arguments)
        if status:
            raise RuntimeError(f'{name} rejected direct CSR adjoint with status {status}')

    def __del__(self):
        try:
            if self.handle.value:
                self.call('cusparseDestroy', self.handle)
        except Exception:
            pass  # The CUDA context may already have exited at interpreter teardown.

    def multiply(self, matrix, gradient, algorithm):
        dense = gradient.contiguous()
        output = dense.new_empty((matrix.size(1), dense.size(1)))
        if matrix._nnz() == 0:
            return output.zero_()
        stream = torch.cuda.current_stream(matrix.device)
        self.call('cusparseSetStream', self.handle, ctypes.c_void_p(stream.cuda_stream))
        crow, columns, values = matrix.crow_indices(), matrix.col_indices(), matrix.values()
        index_type = 2 if crow.dtype == torch.int32 else 3
        if columns.dtype != crow.dtype:
            raise ValueError('direct CSR adjoint requires matching row/column index types')
        sparse, right, result = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_void_p()
        try:
            self.call('cusparseCreateCsr', ctypes.byref(sparse), matrix.size(0), matrix.size(1), matrix._nnz(),
                ctypes.c_void_p(crow.data_ptr()), ctypes.c_void_p(columns.data_ptr()),
                ctypes.c_void_p(values.data_ptr()), index_type, index_type, 0, 0)
            self.call('cusparseCreateDnMat', ctypes.byref(right), dense.size(0), dense.size(1),
                dense.stride(0), ctypes.c_void_p(dense.data_ptr()), 0, 2)
            self.call('cusparseCreateDnMat', ctypes.byref(result), output.size(0), output.size(1),
                output.stride(0), ctypes.c_void_p(output.data_ptr()), 0, 2)
            alpha, beta = ctypes.c_float(1.), ctypes.c_float(0.)
            # CUSPARSE_OPERATION_TRANSPOSE=1, NON_TRANSPOSE=0,
            # CUDA_R_32F=0, CUSPARSE_ORDER_ROW=2. No transpose payload is built.
            arguments = (self.handle, 1, 0, ctypes.byref(alpha), sparse, right,
                         ctypes.byref(beta), result, 0, algorithm)
            size = ctypes.c_size_t()
            self.call('cusparseSpMM_bufferSize', *arguments, ctypes.byref(size))
            workspace = torch.empty(max(1, size.value), device=dense.device, dtype=torch.uint8)
            for tensor in (crow, columns, values, dense, output, workspace):
                tensor.record_stream(stream)
            self.call('cusparseSpMM', *arguments, ctypes.c_void_p(workspace.data_ptr()))
        finally:
            for name, descriptor in [('cusparseDestroyDnMat', result), ('cusparseDestroyDnMat', right),
                                     ('cusparseDestroySpMat', sparse)]:
                if descriptor.value:
                    self.call(name, descriptor)
        return output


_DIRECT_CSR_HANDLES = threading.local()


def _direct_csr_transpose_mm(matrix, gradient, algorithm):
    with torch.cuda.device(matrix.device):
        handles = getattr(_DIRECT_CSR_HANDLES, 'handles', None)
        if handles is None:
            handles = _DIRECT_CSR_HANDLES.handles = {}
        device = torch.cuda.current_device()
        if device not in handles:
            if torch.cuda.is_current_stream_capturing():
                raise RuntimeError('warm the direct CSR adjoint before CUDA Graph capture')
            handles[device] = _DirectCSRTranspose()
        return handles[device].multiply(matrix, gradient, algorithm)


@lru_cache(maxsize=1)
def _fixed_row_kernel():
    # Import only on CUDA. CPU accounting and checkpoint inspection do not
    # require Triton. Each row/channel has one writer and a fixed FP32 sum;
    # cuSPARSE's split-row atomic sum can change a fixed policy on replay.
    import triton
    import triton.language as tl

    @triton.jit
    def kernel(crow, columns, values, weight, output, WIDTH: tl.constexpr,
               K: tl.constexpr, N: tl.constexpr):
        row, tile = tl.program_id(0), tl.program_id(1)
        start, end = tl.load(crow + row), tl.load(crow + row + 1)
        k = tl.arange(0, K)
        n = tile * N + tl.arange(0, N)
        partial = tl.full((K, N), 0, tl.float32)
        for offset in range(start, end, K):
            present = offset + k < end
            col = tl.load(columns + offset + k, present, 0)
            value = tl.load(values + offset + k, present, 0.)
            dense = tl.load(weight + col[:, None] * WIDTH + n[None, :],
                            present[:, None] & (n[None, :] < WIDTH), 0.)
            partial += value[:, None] * dense
        tl.store(output + row * WIDTH + n, tl.sum(partial, 0), n < WIDTH)

    return kernel


class _FixedRowCSRProjection(torch.autograd.Function):
    @staticmethod
    def forward(ctx, matrix, weight):
        ctx.save_for_backward(matrix)
        ctx.adjoint_contract = sparse_adjoint_contract()
        rows, width = matrix.size(0), weight.size(1)
        output = weight.new_empty((rows, width))
        if rows:
            _fixed_row_kernel()[(rows, (width + 63) // 64)](
                matrix.crow_indices(), matrix.col_indices(), matrix.values(),
                weight, output, width, 64, 64, num_warps=4)
        return output

    @staticmethod
    def backward(ctx, gradient):
        matrix, = ctx.saved_tensors
        # Immutable observations have no gradient. All learned dense columns
        # participate in the exact affine adjoint, including absent features
        # through the separate centered-zero term below.
        adjoint = (torch.sparse.mm(matrix.transpose(0, 1), gradient.contiguous())
                   if ctx.adjoint_contract is None else
                   _direct_csr_transpose_mm(matrix, gradient, ctx.adjoint_contract['algorithm']))
        return None, adjoint


def fixed_row_csr_projection(matrix, weight):
    """Full-schema FP32 CSR affine term with reproducible CUDA forward sums."""
    sparse_adjoint_contract()  # Reject invalid selected backends on CPU as well.
    if (matrix.layout != torch.sparse_csr or matrix.ndim != 2 or weight.ndim != 2
            or matrix.size(1) != weight.size(0) or matrix.device != weight.device
            or matrix.dtype != torch.float32 or weight.dtype != torch.float32
            or matrix.requires_grad or not weight.is_contiguous()):
        raise ValueError("fixed-row projection requires immutable FP32 CSR and contiguous FP32 weights")
    if matrix.device.type != "cuda":
        return torch.sparse.mm(matrix, weight)
    return _FixedRowCSRProjection.apply(matrix, weight)


def project_factorized_sparse_rows(model, slab):
    candle = model.candle_encoder
    store = model.__dict__.get("_factorized_sparse_row_store")
    if store is None:
        store = FactorizedSparseRowStore(slab.source, candle, slab.device,
            budget_bytes=model.factorized_sparse_cache_bytes)
        object.__setattr__(model, "_factorized_sparse_row_store", store)
    store.validate(slab.source, candle)
    before = store.host_payload_bytes
    misses_before = store.misses
    host_hits_before = store.host_hits
    norm, projection = candle.input_norm, candle.joint_projection.proj
    with torch.autocast(slab.device.type, enabled=False):
        # Every logical parameter participates. Observed centered values use
        # CSR; omitted positive zeros retain their centered affine effect.
        weight = projection.weight.float()
        # CSR CUDA kernels require consecutive output channels for coalesced
        # weight reads; a transposed view caused the measured strided-read bottleneck.
        effective = (weight * norm.weight.float()).t().contiguous()
        total = effective.sum(dim=0)
        offset = (None if getattr(norm, "bias", None) is None
                  else F.linear(norm.bias.float(), weight))
        outputs = []
        for values, zero_value in store.slices(slab.rows):
            projected = fixed_row_csr_projection(values, effective)
            projected = projected + zero_value[:, None] * total
            if offset is not None:
                projected = projected + offset
            if projection.bias is not None:
                projected = projected + projection.bias.float()
            outputs.append(projected)
        projected = torch.cat(outputs, dim=0).reshape(len(slab.rows), slab.size(1), -1)
        result = candle._finish_embedding(candle._activate_projected(projected))
    if (slab.device.type == "cuda" and store.misses != misses_before
            and store.host_hits == host_hits_before):
        # Cold construction and CSR backward use differently sized transient
        # workspaces. Keep live source/autograd tensors, but return a large
        # inactive reservation before growing the persistent source cache.
        # Reusing verified host storage does not create cold construction
        # workspaces and must not synchronize the allocator on every eviction.
        inactive = (torch.cuda.memory_reserved(slab.device)
                    - torch.cuda.memory_allocated(slab.device))
        if inactive > 2 * 1024**3:
            torch.cuda.empty_cache()
            store.__dict__["cold_allocator_releases"] = (
                store.__dict__.get("cold_allocator_releases", 0) + 1)
    stats = slab.source.transfer_stats
    stats["host_payload_bytes"] += store.host_payload_bytes - before
    stats["sparse_resident_bytes"] = store.bytes
    stats["sparse_cache_hits"] = store.hits
    stats["sparse_cache_misses"] = store.misses
    stats["sparse_host_cache_bytes"] = store.host_bytes
    stats["sparse_host_cache_hits"] = store.host_hits
    stats["sparse_host_cache_misses"] = store.host_misses
    stats["sparse_host_cache_capacity_skips"] = store.host_capacity_skips
    stats["sparse_device_cache_evictions"] = store.device_evictions
    stats["sparse_device_cache_evicted_bytes"] = store.device_evicted_bytes
    if store.disk is not None:
        for key, value in store.disk.stats.items():
            stats[f"sparse_disk_{key}"] = value
    stats["sparse_cold_allocator_releases"] = store.__dict__.get("cold_allocator_releases", 0)
    return result

