#!/usr/bin/env python3
"""Read-only, bounded v6 output-head and conditioning diagnostics.

Run from the repository root with the fintech runtime. Only the bounded audit
JSON is replaced; use --stdout for no filesystem writes. This command never
trains, updates a model, rebuilds a panel, or writes a checkpoint. Each completed
fold gets two fixed calendar probes, not a full-fold explanation or return claim.
"""
import argparse
import dataclasses
import hashlib
import json
import platform
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import numpy as np
import torch
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.models.factory import build_model
from stockagent.models.temporal_basis_fit import temporal_basis_overrides_from_state_dict
from stockagent.models.normalization import masked_cash_entmax15_weights, masked_learned_cash_weights
from stockagent.training.dataset import CrossSectionalDataset, execution_feature_lag

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--config", type=Path,
    default=Path("configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6.yaml"),
)
parser.add_argument(
    "--output", type=Path,
    default=Path("artifacts/markets/bybit_perpetual_daily_0005_v6_training_audit/output_mechanism.json"),
)
parser.add_argument("--stdout", action="store_true", help="Print JSON without writing any artifact.")
args = parser.parse_args()
torch.set_num_threads(2)
config_path = args.config
cfg = load_config(config_path)
root = Path(cfg.runner.output_dir)
output = args.output.resolve()
protected_roots = [root.resolve(), Path(cfg.data.parquet_root).resolve().parent]
if cfg.data.panel_cache_root:
    protected_roots.append(Path(cfg.data.panel_cache_root).resolve())
if cfg.data.external_feature_path:
    protected_roots.append(Path(cfg.data.external_feature_path).resolve().parent)
if any(output.is_relative_to(protected) for protected in protected_roots):
    parser.error("--output must be outside training artifacts, source parents, and panel caches")
if (
    not output.is_relative_to((REPO_ROOT / "artifacts/markets").resolve())
    or not output.parent.name.endswith("_training_audit")
    or output.name != "output_mechanism.json"
):
    parser.error("--output must be artifacts/markets/*_training_audit/output_mechanism.json")
model_config = cfg.training.financial_transformer
if (
    cfg.training.model_name != "financial_transformer"
    or cfg.trading.execution_mode != "crypto_perpetual"
    or model_config.portfolio_output_mode != "learned_cash"
    or not model_config.causal_feature_rms_normalization
    or model_config.causal_feature_window_rms_normalization
):
    parser.error("this diagnostic requires a crypto FinancialTransformer learned_cash policy with fitted train-only RMS")
manifest_path = root / "run_manifest.json"
manifest = json.loads(manifest_path.read_text())
manifest_model = manifest.get("configuration", {}).get("training", {}).get("financial_transformer", {})
if (
    manifest.get("model_name") != "financial_transformer"
    or manifest.get("execution_mode") != "crypto_perpetual"
    or manifest_model.get("portfolio_output_mode") != "learned_cash"
):
    parser.error("artifact manifest does not describe the selected crypto learned_cash policy")
selected_ids = manifest.get("selected_fold_ids")
if (
    not isinstance(selected_ids, list) or not selected_ids
    or any(type(fold_id) is not int or fold_id <= 0 for fold_id in selected_ids)
    or len(set(selected_ids)) != len(selected_ids)
):
    parser.error("artifact manifest must declare nonempty unique positive selected_fold_ids")
selected_folds = [root / f"fold_{fold_id:02d}" for fold_id in sorted(selected_ids)]
required_files = ("checkpoint_best.pt", "causal_feature_rms_normalization.json", "temporal_basis_selection.json", "fold_complete.json")
missing = [str(fold / name) for fold in selected_folds for name in required_files if not (fold / name).is_file()]
if missing:
    parser.error("selected folds are incomplete; missing: " + ", ".join(missing))
cache = Path(cfg.data.panel_cache_root) / "panel_cache_v2"
meta = json.loads((cache / "meta.json").read_text())
names = json.loads((cache / meta["feature_names_file"]).read_text())
symbols = json.loads((cache / meta["symbols_file"]).read_text())
arrays = {key: np.load(cache / value["file"], mmap_mode="r") for key, value in meta["arrays"].items()}
dates = arrays["dates"].astype("datetime64[D]")
def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

known = {field.name for field in dataclasses.fields(PanelData)}
panel = PanelData(symbols=symbols, feature_names=names, **{k:v for k,v in arrays.items() if k in known})
ds = CrossSectionalDataset(panel, np.flatnonzero(dates.astype("datetime64[Y]").astype(int)+1970 == 2020), 32, execution_mode="crypto_perpetual", lookback_context="panel_history")
counts = []
target_dates = []
for i in range(len(ds)):
    row = ds[i]
    mask = row["tradable_mask"].bool()
    if "force_exit_mask" in row:
        mask = mask & ~row["force_exit_mask"].bool()
    counts.append(int(mask.sum()))
    target_dates.append(str(dates[int(ds.valid_indices[i])]) if hasattr(ds, "valid_indices") else str(i))

result = {
    "schema_version": 1,
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "purpose": "Bounded no-update diagnostics of six completed v6 checkpoints and exact output geometry, not formal training or performance attribution.",
    "runtime": {"python": platform.python_version(), "torch": torch.__version__, "device": "cpu", "dtype": "float32", "threads": 2},
    "config": str(config_path), "config_sha256": sha(config_path),
    "artifact_root": str(root),
    "run_manifest": {"path": str(manifest_path), "sha256": sha(manifest_path), "selected_fold_ids": sorted(selected_ids)},
    "source": {"panel_meta_path": str(cache/"meta.json"), "panel_meta_sha256":sha(cache/"meta.json"), "source_hash":meta["source_hash"], "generation":meta["generation"], "num_dates":len(dates), "num_symbols":len(symbols), "num_features":len(names), "array_fingerprints_declared_by_meta":{k:v.get("content_fingerprint") for k,v in meta["arrays"].items()}, "array_payload_rehashed":False},
    "method": {
        "checkpoint": "torch.load CPU, build_model with temporal_basis_overrides_from_state_dict, strict load_state_dict, eval, torch.no_grad; no optimizer or parameter updates.",
        "sample_selection": "January 15 and July 15 of each fold first test year; fold 1 additionally 2020-05-15, 2020-10-15, 2020-12-15 for singleton training-date geometry only.",
        "compaction": "Select tradable_mask AND NOT force_exit_mask at target row; extract 32-row inclusive feature window, all-true compact mask, pass original symbol indices. No symbol position embedding in config.",
        "normalized_input": "x / saved causal_feature_rms_scale * saved causal_feature_active_mask; per-column RMS over batch,time,symbol; no recentering or test-time fitting.",
        "caveat": "FP32 CPU bounded forwards are not byte-identical BF16 formal inference. Two fold first-test years overlap in 2026. Samples are not a full-fold causal attribution or OOS strategy estimate.",
        "legacy_formula": "w=(1-sigmoid(c))*s/sum(abs(s)); singleton nonzero score has dw/ds=0 and exact zero uses zero output/gradient.",
        "v2_formula": "w=entmax15(abs(s))*s/(1+abs(s)); direct signed quotient preserves exact-zero gradient; singleton derivative=1/(1+abs(s))^2.",
        "candidate_caveat": "Entmax sparse-support exclusion may have zero score gradients; support and conviction both depend on score magnitude. Engineering learnability does not imply positive returns.",
        "prior_measurement_correction": "Earlier feature_scaling.json was a v4 assumed-lag-1 measurement, not exact v6 fitted normalizer evidence. Crypto execution_feature_lag is 0; saved v6 fold receipts and checkpoint buffers are authoritative."
    },
    "first_train_2020": {"dataset_valid_targets":len(ds), "active_count_histogram":dict(sorted(Counter(counts).items())), "mean_active":float(np.mean(counts)), "single_active_fraction":float(np.mean(np.array(counts)==1)), "target_start":target_dates[0], "target_end":target_dates[-1], "feature_lag":execution_feature_lag("crypto_perpetual")},
    "folds": [],
}
for fold in selected_folds:
    checkpoint_path = fold/"checkpoint_best.pt"
    cp = torch.load(checkpoint_path, map_location="cpu", weights_only=False)
    if cp.get("fold_id") != int(fold.name.removeprefix("fold_")):
        raise ValueError(f"checkpoint fold_id disagrees with manifest: {checkpoint_path}")
    sd = cp["model_state_dict"]
    model = build_model(config=cfg, lookback=32, num_features=len(names), num_symbols=len(symbols), feature_names=names, temporal_basis_overrides=temporal_basis_overrides_from_state_dict(sd))
    model.load_state_dict(sd, strict=True)
    model.eval()
    receipt_path = fold/"causal_feature_rms_normalization.json"
    receipt = json.loads(receipt_path.read_text())
    basis_path = fold/"temporal_basis_selection.json"
    basis = json.loads(basis_path.read_text())
    scale = model.candle_encoder.causal_feature_rms_scale
    active = model.candle_encoder.causal_feature_active_mask
    receipt_scale = torch.tensor([f["rms_scale"] for f in receipt["features"]])
    receipt_active = torch.tensor([f["active"] for f in receipt["features"]])
    head_keys = ["score_head.1.weight","score_head.1.bias","learned_cash_score_head.weight","learned_cash_score_head.bias"]
    entry = {
        "fold": fold.name, "checkpoint":str(checkpoint_path), "checkpoint_sha256":sha(checkpoint_path),
        "best_epoch":int(cp["epoch"]), "best_val_loss":float(cp["best_val_loss"]),
        "train_years":cp["train_years"], "val_years":cp["val_years"], "test_years":cp["test_years"],
        "head_parameters":{k:{"shape":list(sd[k].shape), "min":float(sd[k].min()), "max":float(sd[k].max()), "l2":float(sd[k].norm())} for k in head_keys},
        "normalizer": {"receipt":str(receipt_path), "receipt_sha256":sha(receipt_path), **{k:v for k,v in receipt.items() if k!="features"}, "scales_equal_saved_receipt":bool(torch.equal(scale,receipt_scale)), "active_equal_saved_receipt":bool(torch.equal(active.bool(),receipt_active)), "scale_min":float(scale.min()), "scale_max":float(scale.max()), "inactive_features":[names[i] for i in range(len(names)) if not bool(active[i])]},
        "temporal_basis": {"receipt":str(basis_path), "receipt_sha256":sha(basis_path), **{k:basis[k] for k in ["actual_rank","selected_total","near_duplicate_count","families","components_per_family","pca_klt_training_only","selection_policy","selection_fingerprint"]}},
        "samples":[]
    }
    year=min(cp["test_years"])
    days=[str(year)+"-01-15",str(year)+"-07-15"]
    if fold.name=="fold_01": days=["2020-05-15","2020-10-15","2020-12-15"]+days
    for day in days:
        idx=int(np.flatnonzero(dates==np.datetime64(day))[0])
        ids=np.flatnonzero(arrays["tradable_mask"][idx] & ~arrays["force_exit_mask"][idx])
        x=torch.from_numpy(np.asarray(arrays["features"][idx-31:idx+1,ids,:]).copy()).unsqueeze(0)
        mask=torch.ones((1,len(ids)),dtype=torch.bool)
        with torch.no_grad():
            weights,_,aux=model(x,mask,return_aux=True,symbol_indices=torch.tensor(ids))
            scores=aux["score_logits"]
            cash=aux["cash_target_logits"]
            gate=torch.sigmoid(cash)
            norm=x/scale*active
            rms=norm.square().mean(dim=(0,1,2)).sqrt()
            absmax=norm.abs().amax(dim=(0,1,2))
        entry["samples"].append({
            "date":day, "sample_role":"training_date_mechanism_probe" if day.startswith("2020") else "first_test_year_probe",
            "active_candidates":len(ids), "score_min":float(scores.min()), "score_max":float(scores.max()), "score_mean":float(scores.mean()), "score_std":float(scores.std(unbiased=False)),
            "negative_score_count":int((scores<0).sum()), "gross":float(weights.abs().sum()), "net":float(weights.sum()),
            "cash_logit":float(cash.item()), "sigmoid_cash_derivative":float((gate*(1-gate)).item()),
            "normalized_input_rms":float(norm.square().mean().sqrt()), "normalized_input_abs_max":float(norm.abs().max()),
            "normalized_features":[{"name":names[j],"rms":float(rms[j]),"absmax":float(absmax[j]),"saved_train_scale":float(scale[j]),"active":bool(active[j])} for j in range(len(names))]
        })
    result["folds"].append(entry)
result["feature_names_in_model_order"]=names
result["singleton_jacobian_probes"]=[]
for score in [-1.0,-0.1,0.0,0.1,1.0]:
    s=torch.tensor([[score]],requires_grad=True)
    cash=torch.zeros(1,requires_grad=True)
    mask=torch.ones_like(s,dtype=torch.bool)
    old,_,_=masked_learned_cash_weights(s,cash,mask)
    old_grad=torch.autograd.grad(old.sum(),s)[0]
    new=masked_cash_entmax15_weights(s,mask,preserve_fp32_output=True,preserve_zero_score_gradient=True)
    new_grad=torch.autograd.grad(new.sum(),s)[0]
    result["singleton_jacobian_probes"].append({"score":score,"learned_cash_weight":float(old.detach()),"learned_cash_dw_ds":float(old_grad),"score_entmax_cash_v2_weight":float(new.detach()),"score_entmax_cash_v2_dw_ds":float(new_grad)})
result["validation"]={"test_path":"test/test_crypto_output_gradients.py","test_sha256":sha(Path("test/test_crypto_output_gradients.py")),"command":"source scripts/runtime_env.sh; run_fintech_python -m pytest -q -s test/test_crypto_output_gradients.py","tests_executed_by_this_reader":False,"formal_training_launched":False}
result["source_code_sha256_at_measurement"] = {
    path: sha(Path(path)) for path in (
        "scripts/audit_crypto_output_geometry.py",
        "stockagent/models/normalization.py",
        "stockagent/models/financial_transformer.py",
        "stockagent/models/transformer_base_portfolio.py",
        "stockagent/training/dataset.py",
        "stockagent/training/loss.py",
        "stockagent/backtest/crypto_perpetual.py",
    )
}
result["reproduction_command"] = (
    "source scripts/runtime_env.sh; run_fintech_python "
    "scripts/audit_crypto_output_geometry.py --config " + str(config_path)
)
rendered = json.dumps(result, sort_keys=True, allow_nan=False)
if args.stdout:
    print(rendered)
else:
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "folds": len(result["folds"])}))
