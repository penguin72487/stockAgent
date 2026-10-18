"""Full-window native solvers with bounded, explicit memory ownership."""
from __future__ import annotations

import ctypes
import math
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from scipy import sparse


def lightgbm_dataset(matrix, labels, params, *, reference=None, groups=None, progress=None,
                     cache_root=None):
    """Stream exact numeric-zero CSR rows directly into native binned storage.

    This extends LightGBM's official sampled-column / PushRowsByCSR API; it
    avoids both the dense window expansion and a second full lagged CSR copy.
    Bin construction sees training rows only. Validation reuses those bins.
    """
    import lightgbm as lgb
    import lightgbm.basic as b
    if cache_root is not None:
        return _cached_lightgbm_dataset(matrix,labels,params,reference=reference,
            groups=groups,progress=progress,cache_root=cache_root)
    if params.get("device_type") == "cuda":
        # The sampled-column C API does not create CUDA metadata. Enter the
        # official binary loader, which initializes GPU columns and metadata,
        # after streaming the identical CPU bins. No dense window is created.
        import tempfile
        from pathlib import Path
        cpu = lightgbm_dataset(matrix, labels, {**params,"device_type":"cpu"},
            reference=reference,groups=groups,progress=progress)
        with tempfile.TemporaryDirectory(prefix="stockagent-lgbm-") as folder:
            path=Path(folder)/"dataset.bin"
            cpu.save_binary(str(path))
            cpu._free_handle()
            result=lgb.Dataset(str(path),params=params,reference=reference,free_raw_data=True).construct()
            result.set_label(np.asarray(labels,dtype=np.float32))
            if groups is not None:
                result.set_group(np.asarray(groups,dtype=np.int32))
            return result
    ds = lgb.Dataset(None, params=params, free_raw_data=True, reference=reference)
    if reference is None:
        indices = ds._create_sample_indices(total_nrow=matrix.shape[0])
        sample = matrix.rows(indices).tocsc()
        values = [sample.data[sample.indptr[i]:sample.indptr[i + 1]].astype(np.float64)
                  for i in range(matrix.shape[1])]
        rows = [sample.indices[sample.indptr[i]:sample.indptr[i + 1]].astype(np.int32, copy=False)
                for i in range(matrix.shape[1])]
        ds._init_from_sample(values, rows, len(indices), matrix.shape[0])
        del values, rows, sample
    else:
        ds._init_from_ref_dataset(matrix.shape[0], reference.construct()._handle)
    for start, stop, x in matrix.batches():
        ptr, ptr_type, keep_ptr = b._c_int_array(x.indptr)
        data, data_type, keep_data = b._c_float_array(x.data)
        indices = np.asarray(x.indices, dtype=np.int32)
        b._safe_call(b._LIB.LGBM_DatasetPushRowsByCSR(
            ds._handle, ptr, ctypes.c_int(ptr_type),
            indices.ctypes.data_as(ctypes.POINTER(ctypes.c_int32)),
            data, ctypes.c_int(data_type), ctypes.c_int64(len(x.indptr)),
            ctypes.c_int64(x.nnz), ctypes.c_int64(x.shape[1]), ctypes.c_int64(start)))
        if progress is not None:
            progress(stop, matrix.shape[0])
    ds.set_label(np.asarray(labels, dtype=np.float32))
    if groups is not None:
        ds.set_group(np.asarray(groups, dtype=np.int32))
    return ds


def _cached_lightgbm_dataset(matrix,labels,params,*,reference,groups,progress,cache_root):
    """Reuse verified train-only bins across objectives and CPU/CUDA backends."""
    import fcntl
    import hashlib
    import json
    import os
    from pathlib import Path
    import lightgbm as lgb
    from downloader.artifact_io import atomic_write_json
    from stockagent.runtime_identity import identity_sha256
    from stockagent.training.classical_data import _sha
    dataset_keys=("max_bin","min_data_in_bin","min_data_in_leaf","bin_construct_sample_cnt",
        "zero_as_missing","use_missing","feature_pre_filter","enable_bundle",
        "is_enable_sparse","data_random_seed","seed")
    identity=dict(contract="canonical_classical_native_bins_v1",version=lgb.__version__,
        input=matrix.store.identity,shape=matrix.shape,lookback=matrix.lookback,
        row_dates=hashlib.sha256(matrix.row_dates.tobytes()).hexdigest(),
        row_symbols=hashlib.sha256(matrix.row_symbols.tobytes()).hexdigest(),
        feature_end=hashlib.sha256(matrix.feature_end.tobytes()).hexdigest(),
        params={k:params[k] for k in dataset_keys if k in params},
        reference=None if reference is None else reference._stockagent_bin_key)
    key=identity_sha256(identity)
    folder=Path(cache_root)/key
    folder.mkdir(parents=True,exist_ok=True)
    binary=folder/'dataset.bin'
    receipt=folder/'complete.json'
    with (folder/'build.lock').open('a') as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX)
        if not receipt.exists():
            began=time.perf_counter()
            # Cache features/bins only. Every consumer sets its own labels and
            # daily groups, so a ranker never inherits regression labels.
            built=lightgbm_dataset(matrix,np.zeros(matrix.shape[0],dtype=np.float32),
                {**params,"device_type":"cpu"},reference=reference,progress=progress)
            temporary=folder/f'dataset.{os.getpid()}.tmp.bin'
            built.save_binary(str(temporary))
            built._free_handle()
            with temporary.open('rb') as stream:
                os.fsync(stream.fileno())
            temporary.replace(binary)
            atomic_write_json(receipt,dict(identity=identity,sha256=_sha(binary),
                bytes=binary.stat().st_size,build_s=time.perf_counter()-began))
        proof=json.loads(receipt.read_text())
        # JSON normalizes shape tuples to lists.
        if (identity_sha256(proof['identity'])!=key or proof['bytes']!=binary.stat().st_size
            or proof['sha256']!=_sha(binary)):
            raise ValueError("corrupt or incompatible classical native bin cache")
        result=lgb.Dataset(str(binary),params=params,reference=reference,
            free_raw_data=True).construct()
        result.set_label(np.asarray(labels,dtype=np.float32))
        if groups is not None:
            result.set_group(np.asarray(groups,dtype=np.int32))
        result._stockagent_bin_key=key
        if progress:
            progress(matrix.shape[0],matrix.shape[0])
        return result


def lightgbm_continue(params, datasets, *, previous=None, initial_scores=None):
    """Restore native trees and their train/validation score accumulators."""
    import lightgbm as lgb
    if previous is not None:
        import lightgbm.basic as b
        if initial_scores is None or len(initial_scores) != len(datasets):
            raise ValueError("LightGBM continuation needs every owned prediction accumulator")
        predictor = b._InnerPredictor.from_booster(previous, pred_parameter=params)
        for dataset, scores in zip(datasets, initial_scores):
            dataset.set_init_score(np.asarray(scores, dtype=np.float64))
            dataset._predictor = predictor
    booster = lgb.Booster(params=params, train_set=datasets[0])
    for index, dataset in enumerate(datasets[1:], 1):
        booster.add_valid(dataset, f"canonical_{index}")
    return booster


def lightgbm_scores(booster, dataset_index):
    """Read the native incremental score cache instead of re-predicting X."""
    return booster._Booster__inner_predict(data_idx=int(dataset_index))


class LaggedLinearOperator:
    """X and X.T without storing overlapping windows L times.

    A physical CSR block multiplies all L lag-specific coefficient vectors in
    one SpMM. The adjoint uses the identical sample map. Only a model's own
    training rows contribute to centering and scaling.
    """
    def __init__(self, matrix, *, backend="cpu", gpu_device_id=0,
                 device_budget_bytes=26 * 1024**3, progress=None, threads=1):
        import torch
        self.matrix, self.backend = matrix, backend
        self.shape = matrix.shape
        self.blocks = []
        self.shared = []
        self.threads = max(1,int(threads))
        self.pool = ThreadPoolExecutor(max_workers=self.threads) if backend=="cpu" and self.threads>1 else None
        self.map_seconds = 0.0
        self.device = torch.device(f"cuda:{gpu_device_id}" if backend == "cuda" else "cpu")
        if backend not in {"cpu", "cuda"}:
            raise ValueError("Ridge backend must be cpu or cuda")
        if backend == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("requested Ridge CUDA backend is unavailable")
        source = matrix.store
        lo = int(np.min(matrix.feature_end) - matrix.lookback)
        hi = int(np.max(matrix.feature_end))
        lookup = np.full(source.shape[:2], -1, dtype=np.int32)
        lookup[matrix.row_dates, matrix.row_symbols] = np.arange(self.shape[0], dtype=np.int32)
        from stockagent.training.dataset import execution_feature_lag
        delay = execution_feature_lag(matrix.dataset.execution_mode)
        admitted = 0
        total = (hi - 1) // source.block_rows - lo // source.block_rows + 1
        for count, bid in enumerate(range(lo // source.block_rows, (hi - 1) // source.block_rows + 1)):
            x = source.block(bid)
            dates = np.arange(bid * source.block_rows,
                              min((bid + 1) * source.block_rows, source.shape[0]))
            common = None
            if getattr(source.source,"_stockagent_factorized_features",False):
                common = np.nan_to_num(np.asarray(source.source.common[dates],dtype=np.float32),
                    nan=0.0,posinf=0.0,neginf=0.0)
                # Shared channels are evaluated once per physical date, then
                # broadcast algebraically; no stock's information is removed.
                x = x[:,:matrix.features-common.shape[-1]]
            target = dates[:, None] + matrix.lookback - np.arange(matrix.lookback) + delay - 1
            legal = (target >= 0) & (target < source.shape[0])
            mapping = lookup[np.clip(target, 0, source.shape[0] - 1)].transpose(0, 2, 1).copy()
            mapping[np.broadcast_to(~legal[:, None, :], mapping.shape)] = -1
            mapping = mapping.reshape(-1, matrix.lookback)
            admitted += x.data.nbytes + 4 * (x.indices.size + x.indptr.size) + mapping.size * 8
            if backend == "cuda":
                # Include transient SpMM output / adjoint / squared values.
                scratch = x.shape[0] * matrix.lookback * 4 * 3 + x.data.nbytes
                if admitted + scratch > device_budget_bytes:
                    raise MemoryError("Ridge resident full-input CSR exceeds declared GPU memory budget")
                tensor = torch.sparse_csr_tensor(
                    torch.as_tensor(x.indptr.astype(np.int32), device=self.device),
                    torch.as_tensor(x.indices.astype(np.int32), device=self.device),
                    torch.as_tensor(np.array(x.data), device=self.device),
                    size=x.shape, device=self.device, check_invariants=False)
                mapping = torch.as_tensor(mapping.astype(np.int64), device=self.device)
                self.blocks.append((tensor, mapping))
                self.shared.append(None if common is None else torch.as_tensor(common,device=self.device))
            else:
                self.blocks.append((x, mapping))
                self.shared.append(common)
            if progress:
                progress(count + 1, total)
        self.resident_bytes = admitted

    def close(self):
        if self.pool is not None:
            self.pool.shutdown(wait=True)
            self.pool = None
        self.blocks.clear()
        self.shared.clear()

    def _matvec(self, coefficient):
        if self.backend == "cpu":
            out = np.zeros(self.shape[0], dtype=np.float64)
            w = np.asarray(coefficient).reshape(self.matrix.lookback, self.matrix.features).T
            def product(block):
                (x,mapping),common = block
                values = x @ w[:x.shape[1]]
                if common is not None:
                    values += np.repeat(common @ w[x.shape[1]:],self.matrix.symbols,axis=0)
                valid = mapping >= 0
                indices=mapping[valid]
                if not len(indices):
                    return 0,np.empty(0)
                lo,hi=int(indices.min()),int(indices.max())+1
                return lo,np.bincount(indices-lo,weights=values[valid],minlength=hi-lo)
            from threadpoolctl import threadpool_limits
            with threadpool_limits(limits=1,user_api="blas"):
                parts=(self.pool.map(product,zip(self.blocks,self.shared)) if self.pool
                       else map(product,zip(self.blocks,self.shared)))
                for lo,part in parts:
                    out[lo:lo+len(part)] += part
            return out
        import torch
        w = torch.as_tensor(np.asarray(coefficient).reshape(
            self.matrix.lookback, self.matrix.features).T.copy(), device=self.device, dtype=torch.float32)
        out = torch.zeros(self.shape[0], device=self.device)
        for (x, mapping), common in zip(self.blocks,self.shared):
            values = torch.sparse.mm(x, w[:x.shape[1]])
            if common is not None:
                values.add_((common @ w[x.shape[1]:]).repeat_interleave(self.matrix.symbols,dim=0))
            # Avoid boolean indexing's dynamic-size synchronization on CUDA.
            out.scatter_add_(0, mapping.clamp_min(0).reshape(-1),
                             (values * (mapping >= 0)).reshape(-1))
        return out.cpu().numpy().astype(np.float64)

    def _rmatvec(self, residual, *, squared=False, stable=False):
        if self.backend == "cpu":
            out = np.zeros((self.matrix.features, self.matrix.lookback), dtype=np.float64)
            def product(block):
                (x,mapping),common = block
                values = np.asarray(residual)[mapping.clip(min=0)] * (mapping >= 0)
                result=(x.multiply(x) if squared else x).T @ values
                if common is not None:
                    c = common*common if squared else common
                    result=np.concatenate((result,c.T @ values.reshape(-1,self.matrix.symbols,self.matrix.lookback).sum(1)))
                return result
            from threadpoolctl import threadpool_limits
            with threadpool_limits(limits=1,user_api="blas"):
                parts=(self.pool.map(product,zip(self.blocks,self.shared)) if self.pool
                       else map(product,zip(self.blocks,self.shared)))
                for part in parts:
                    out+=part
            return out.T.reshape(-1)
        import torch
        dtype = torch.float64 if stable else torch.float32
        r = torch.as_tensor(np.asarray(residual), device=self.device, dtype=dtype)
        out = torch.zeros((self.matrix.features, self.matrix.lookback), device=self.device, dtype=dtype)
        for (x, mapping), common in zip(self.blocks,self.shared):
            values = r[mapping.clamp_min(0)] * (mapping >= 0)
            operand = x.to(dtype=dtype) if stable else x
            operand = (torch.sparse_csr_tensor(operand.crow_indices(), operand.col_indices(),
                       operand.values().square(), size=x.shape, device=self.device,
                       check_invariants=False) if squared else operand)
            out[:x.shape[1]].add_(torch.sparse.mm(operand.transpose(0, 1), values))
            if common is not None:
                c = common.to(dtype=dtype)
                c = c.square() if squared else c
                out[x.shape[1]:].add_(c.T @ values.reshape(-1,self.matrix.symbols,self.matrix.lookback).sum(1))
        return out.T.contiguous().reshape(-1).cpu().numpy().astype(np.float64)

    def moments(self):
        ones = np.ones(self.shape[0], dtype=np.float32)
        mean = self._rmatvec(ones, stable=True) / self.shape[0]
        second = self._rmatvec(ones, squared=True, stable=True) / self.shape[0]
        if not (np.isfinite(mean).all() and np.isfinite(second).all()):
            raise FloatingPointError("nonfinite train-only feature moments")
        scale = np.sqrt(np.maximum(second - mean * mean, 0.0))
        scale[scale < 1e-12] = 1.0
        return mean, scale

    def solve(self, y, *, alpha=1.0, tolerance=1e-5, max_iter=1000,
              callback=None, state=None):
        """Preconditioned CG; continuation stores the actual solver trajectory."""
        if alpha <= 0 or tolerance <= 0 or max_iter < 1:
            raise ValueError("Ridge alpha/tolerance/iteration limit must be positive")
        y = np.asarray(y, dtype=np.float64)
        mean, scale = self.moments() if state is None else (
            np.asarray(state["mean"]), np.asarray(state["scale"]))
        target_mean = float(np.mean(y))
        rhs = self._rmatvec(y - target_mean, stable=True) / scale
        def hessian(w):
            v = w / scale
            z = self._matvec(v) - np.dot(mean, v)
            return (self._rmatvec(z) - mean * z.sum()) / scale + alpha * w
        # Constant columns stay zero; each varying standardized column has N
        # diagonal mass. Scaling is a train-only preprocessing/model choice.
        diagonal = self.shape[0] + alpha
        w = np.zeros(self.shape[1]) if state is None else np.asarray(state["w"]).copy()
        r = rhs - hessian(w) if state is None else np.asarray(state["r"]).copy()
        p = r / diagonal if state is None else np.asarray(state["p"]).copy()
        rz = float(r @ (r / diagonal))
        norm = max(float(np.linalg.norm(rhs)), 1e-30)
        start = 0 if state is None else int(state["iteration"])
        if start >= max_iter or np.linalg.norm(r) <= tolerance*norm:
            coefficient=w/scale
            payload=dict(w=w.copy(),r=r.copy(),p=p.copy(),mean=mean,scale=scale,
                iteration=max(1,start),relative_residual=float(np.linalg.norm(r)/norm),
                converged=bool(np.linalg.norm(r)<=tolerance*norm),solve_s=0.0)
            if start==0 and callback:
                callback(1,coefficient,target_mean-float(mean@coefficient),payload)
            return coefficient,target_mean-float(mean@coefficient),payload
        converged = False
        for iteration in range(start + 1, max_iter + 1):
            began = time.perf_counter()
            hp = hessian(p)
            curvature = float(p @ hp)
            if curvature <= 0 or not math.isfinite(curvature):
                raise FloatingPointError("Ridge CG lost positive curvature; reject unstable solver update")
            step = rz / curvature
            w += step * p
            r -= step * hp
            new_rz = float(r @ (r / diagonal))
            p = r / diagonal + new_rz / max(rz, 1e-300) * p
            rz = new_rz
            relative_residual = float(np.linalg.norm(r) / norm)
            converged = relative_residual <= tolerance
            coefficient = w / scale
            payload = dict(w=w.copy(), r=r.copy(), p=p.copy(), mean=mean, scale=scale,
                iteration=iteration, relative_residual=relative_residual,
                converged=converged, solve_s=time.perf_counter()-began)
            stop = callback(iteration, coefficient, target_mean - float(mean @ coefficient), payload) if callback else False
            if converged or stop:
                break
        return coefficient, target_mean - float(mean @ coefficient), payload
