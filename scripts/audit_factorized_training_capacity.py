#!/usr/bin/env python3
"""Read-only parameter/checkpoint budget for the selected wide-panel contract.

This is not a data, CUDA, model-fit, or full-fold acceptance. Its shape-only PCA
bank is explicitly NOT training state; no checkpoint or optimizer is created.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import sys

import torch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.config import load_config
from stockagent.data.factorized_panel import file_sha256
from stockagent.models.factory import build_model
from stockagent.models.transformer_base_portfolio import _temporal_basis_matrix


def checkpoint_budget(parameter_bytes: int, *, report_reserve_bytes: int) -> dict:
    if parameter_bytes<0 or report_reserve_bytes<0:
        raise ValueError("nonnegative byte budgets required")
    # Canonical Adam group-last = parameters + two FP32 moment vectors.
    # While atomically replacing it, old + new remain. A model-only fold best
    # also remains. Its own old+new peak is smaller than the group-last peak.
    group=3*parameter_bytes
    return {"model_parameter_bytes":parameter_bytes,"adam_group_bytes":group,
        "group_atomic_peak_plus_fold_best_bytes":2*group+parameter_bytes,
        "report_reserve_bytes":report_reserve_bytes,
        "required_free_bytes":2*group+parameter_bytes+report_reserve_bytes}


def audit(config_path: Path, *, reserve_bytes: int) -> dict:
    import json
    config=load_config(config_path)
    manifest_path=Path(config.data.factorized_feature_manifest).resolve(strict=True)
    manifest=json.loads(manifest_path.read_text())
    if manifest.get("status")!="complete":raise ValueError("complete feature manifest required")
    if config.training.model_name!="financial_transformer":
        raise ValueError("parameter scaling is admitted only for the selected FinancialTransformer")
    settings=config.training.financial_transformer
    if settings.feature_bottleneck_dim or settings.categorical_feature_names:
        raise ValueError("audit requires the selected direct continuous-input architecture")
    overrides={}
    if "pca_klt" in settings.temporal_basis_families:
        overrides["pca_klt"]=_temporal_basis_matrix("dct",steps=config.training.lookback,
            components=config.training.lookback-1)
    counts=[];state_bytes=[]
    for width in (1,2,3):
        model=build_model(config=config,lookback=config.training.lookback,num_features=width,
            num_symbols=len(manifest["symbols"]),temporal_basis_overrides=overrides)
        counts.append(sum(p.numel() for p in model.parameters()))
        state_bytes.append(sum(v.numel()*v.element_size() for v in model.state_dict().values()))
    slope=counts[1]-counts[0]
    if counts[2]-counts[1]!=slope or state_bytes[2]-state_bytes[1]!=state_bytes[1]-state_bytes[0]:
        raise ValueError("architecture is not affine in continuous feature count; no extrapolation")
    width=int(manifest["logical_model_channels"])
    params=counts[0]+slope*(width-1)
    buffer_bytes=state_bytes[0]-4*counts[0]+(state_bytes[1]-state_bytes[0]-4*slope)*(width-1)
    budget=checkpoint_budget(4*params,report_reserve_bytes=reserve_bytes)
    # Retain persistent model buffers in each of the two group files and best.
    budget["persistent_buffer_bytes"]=buffer_bytes
    budget["required_free_bytes"]+=3*buffer_bytes
    free=shutil.disk_usage(manifest_path.parent).free
    return {"status":"read_only_capacity_audit","config_sha256":file_sha256(config_path),
        "manifest_sha256":file_sha256(manifest_path),"source_snapshot_id":manifest["source_snapshot_id"],
        "parameters_upper_estimate":params,"feature_channels":width,
        "parameter_count_oracle_widths":counts,"parameter_slope_per_feature":slope,
        "pca_budget_bank":"shape_only_DCT_non_DC_not_a_training_fit",
        "gpu_verified":False,"training_ready":False,"free_bytes":free,**budget,
        "checkpoint_disk_budget_passed":free>=budget["required_free_bytes"],
        "limitations":["upper estimate before train_union symbol compaction and fitted PCA",
            "GPU workspaces/DDP/Adam VRAM and host memory require separate measured acceptance",
            "budget covers one selected fold with model-only best and canonical Adam group-last"]}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--report-reserve-gib",type=float,default=4.)
    a=p.parse_args()
    if a.output.exists():raise FileExistsError("retain previous capacity receipt")
    result=audit(a.config, reserve_bytes=int(a.report_reserve_gib*1024**3))
    atomic_write_json(a.output,result)
    import json
    print(json.dumps(result,ensure_ascii=False))
    return 0 if result["checkpoint_disk_budget_passed"] else 2


if __name__=="__main__":raise SystemExit(main())
