"""Measure supported native backends using unchanged, real causal windows."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import time
import traceback
import faulthandler

import numpy as np


def main():
    faulthandler.enable()
    faulthandler.dump_traceback_later(120, repeat=True)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--cache-root", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--samples", type=int, default=2048)
    p.add_argument("--only-model", choices=["lightgbm","ridge"])
    p.add_argument("--backend", choices=["cpu","cuda"])
    p.add_argument("--threads", type=int)
    a = p.parse_args()
    from downloader.artifact_io import atomic_write_json
    from stockagent.config import load_config
    from stockagent.data.panel import build_panel
    from stockagent.data.walkforward import build_expanding_year_folds, year_boundary_offset_sessions
    from stockagent.training.classical import _dataset
    from stockagent.training.classical_data import CausalWindowMatrix, PhysicalCSRStore
    from stockagent.training.classical_solvers import LaggedLinearOperator, lightgbm_dataset
    from train import _build_panel_rank_coordinated
    from threadpoolctl import threadpool_limits
    config = load_config(a.config)
    panel = _build_panel_rank_coordinated(build_panel, config, "none")
    folds = build_expanding_year_folds(dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
        split_start_year=config.walk_forward.split_start_year,
        year_boundary_offset_sessions=year_boundary_offset_sessions(config))
    store = PhysicalCSRStore(panel.features, a.cache_root, cache_bytes=24*1024**3)
    full = CausalWindowMatrix(store, _dataset(panel, folds[-1].train_indices, config), batch_rows=256)
    selected = np.sort(np.random.default_rng(42).choice(full.shape[0], min(a.samples, full.shape[0]), replace=False))
    class Sample:
        shape = (len(selected), full.shape[1])
        y = full.y[selected]
        def rows(self, selection):
            return full.rows(selected[selection])
        def batches(self):
            for start in range(0,len(selected),256):
                stop=min(start+256,len(selected))
                yield start,stop,self.rows(slice(start,stop))
    sample = Sample()
    records=[]
    def record(name, backend, fn):
        began=time.perf_counter()
        try:
            detail=fn()
            row=dict(model=name,backend=backend,success=True,wall_s=time.perf_counter()-began,**(detail or {}))
        except Exception as exc:
            row=dict(model=name,backend=backend,success=False,wall_s=time.perf_counter()-began,
                     error=f"{type(exc).__name__}: {exc}",traceback=traceback.format_exc())
        records.append(row)
        atomic_write_json(a.output,dict(complete=False,shape=sample.shape,full_shape=full.shape,records=records))
        print(json.dumps(row),flush=True)
    import lightgbm as lgb
    for threads,backend in [(16,"cpu"),(32,"cpu"),(64,"cpu"),(16,"cuda")]:
        if a.only_model and a.only_model != "lightgbm":
            continue
        if (a.backend and a.backend != backend) or (a.threads and a.threads != threads):
            continue
        def run(threads=threads,backend=backend):
            params=dict(objective="regression",num_threads=threads,max_bin=63,
                min_data_in_leaf=32,num_leaves=15,max_depth=4,verbosity=-1,
                device_type=backend,force_col_wise=True,feature_pre_filter=False,
                bin_construct_sample_cnt=len(selected),zero_as_missing=False,use_missing=False)
            ds=lightgbm_dataset(sample,sample.y,params)
            prepared=time.perf_counter()
            model=lgb.train(params,ds,num_boost_round=8)
            return dict(threads=threads,fit_s=time.perf_counter()-prepared,trees=model.current_iteration())
        record("lightgbm",f"{backend}:{threads}",run)
    # The same full physical training matrix, never a shortened lookback, is
    # used for the sparse linear backend comparison.
    for backend,threads in [("cpu",16),("cpu",32),("cuda",16)]:
        if a.only_model and a.only_model != "ridge":
            continue
        if (a.backend and a.backend != backend) or (a.threads and a.threads != threads):
            continue
        def run(backend=backend,threads=threads):
            with threadpool_limits(threads):
                op=LaggedLinearOperator(full,backend=backend,gpu_device_id=1,
                    device_budget_bytes=27*1024**3,threads=threads)
                w=np.random.default_rng(7).normal(size=full.shape[1])
                begin=time.perf_counter(); pred=op._matvec(w); out=op._rmatvec(pred)
                elapsed=time.perf_counter()-begin
                return dict(normal_product_s=elapsed,norm=float(np.linalg.norm(out)),resident_bytes=op.resident_bytes)
        record("ridge",f"{backend}:{threads}",run)
        import torch,gc
        gc.collect();torch.cuda.empty_cache()
    faulthandler.cancel_dump_traceback_later()
    atomic_write_json(a.output,dict(complete=True,shape=sample.shape,full_shape=full.shape,records=records,
        caveat="Tree timings are a bounded real-row probe; full-fold backend acceptance remains required."))


if __name__=="__main__":
    main()
