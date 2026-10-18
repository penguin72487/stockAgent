"""Explicitly install an independently qualified new-generation CPU forward.

No models, CUDA, provider requests, source edits, signal publication or service
restarts. Re-observe the canonical complete input under its source lease and
compare every output against the preserved old-generation control again.
The canonical cache owns locking, hash validation and durable commit.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.benchmark_live_inference import (
    capture_forward_identity, forward_control_wrapper, require_equal_forward_outputs,
)
from stockagent.config import load_config
from stockagent.live import completed_inference_cache as cache
from stockagent.live import signal_engine
from stockagent.live.market_config import load_market_config
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
import torch


def qualification(record: dict, *, market: str, panel_date: str) -> str:
    if (record.get("contract") != "canonical_live_inference_benchmark_v1"
            or record.get("market") != market or record.get("panel_date") != panel_date
            or record.get("exact_output_parity") is not True
            or record.get("production_writes") is not False
            or record.get("network_requests") != 0
            or record.get("completed_session_cache") is not True):
        raise ValueError("independent full-forward benchmark qualification is missing")
    controls = record.get("full_forward_controls")
    projection = bool(isinstance(controls, list) and controls
                      and controls[0].get("projection_contract") ==
                      signal_engine._COMPLETED_SIGNAL_PROJECTION)
    candle_policy = bool(projection and controls[0].get("candle_aux_policy") ==
                         signal_engine._COMPLETED_CANDLE_AUX_POLICY)
    model_equal = bool(isinstance(controls, list) and controls and
                       controls[0].get("full_model_output_bitwise_equal") is True)
    if candle_policy:
        model_equal = (
            controls[0].get("full_default_model_output_bitwise_equal") is True
            and controls[0].get("non_candle_output_bitwise_equal") is True
            and controls[0].get("oracle_model_calls") == 1
            and bool(controls[0].get("omitted_candle_fields"))
            and all(row.get("candle_aux_policy") == signal_engine._COMPLETED_CANDLE_AUX_POLICY
                    for row in controls)
            and all(row.get("oracle_model_calls") == 0 for row in controls[1:]))
    outputs_equal = (
        model_equal
        and all(row.get("projection_contract") == signal_engine._COMPLETED_SIGNAL_PROJECTION
                and row.get("complete_signal_projection_bitwise_equal") is True for row in controls)
        if projection else isinstance(controls, list)
        and all(row.get("whole_output_bitwise_equal") is True for row in controls))
    if (not isinstance(controls, list) or len(controls) < 2
            or controls[0].get("model_calls") != 1
            or any(row.get("model_calls") != 0 for row in controls[1:])
            or not outputs_equal
            or len({row.get("candidate_key") for row in controls}) != 1):
        raise ValueError("a complete new-code forward and independent warm-cache oracle are required")
    samples = record.get("samples")
    if (not isinstance(samples, list) or len(samples) != len(controls)
            or samples[0].get("gpu_allocated_after_release_bytes") != 0
            or samples[0].get("gpu_reserved_after_release_bytes") != 0):
        raise ValueError("qualified candidate did not release the entire GPU model cache")
    key = controls[0]["candidate_key"]
    for sample in samples:
        receipt = sample.get("stages", {}).get("completed_forward_cache", {})
        if receipt.get("key") != key or receipt.get("contract") != cache.CONTRACT:
            raise ValueError("benchmark does not refer to the qualified full-forward key")
    return key


def install(*, market_config: Path, panel_date: str, benchmark: Path,
            target_root: Path) -> dict:
    if torch.cuda.is_initialized():
        raise RuntimeError("installation requires a fresh CUDA-cold process")
    with benchmark.open("rb") as handle:
        raw = handle.read(16 * 1024**2 + 1)
    if len(raw) > 16 * 1024**2:
        raise ValueError("benchmark receipt exceeds bounded size")
    qualified = json.loads(raw)
    cfg = load_market_config(market_config)
    key = qualification(qualified, market=cfg.market, panel_date=panel_date)
    private_root = Path(qualified["cache_root"])
    if private_root.resolve() == target_root.resolve():
        raise ValueError("qualified private cache and installation target must be distinct")
    kwargs = cfg.signal_kwargs(write=False, publish_latest=False, ensure_previous_signal=False,
                               previous_signal_backfill_limit=0, panel_date=panel_date,
                               asof_date=panel_date + " 13:30:00", price_source="panel",
                               use_completed_session_cache=True)
    config = load_config(cfg.config_path)
    with live_tw_public_source_read_guard(config.data.parquet_root):
        observation = capture_forward_identity(kwargs)
        if observation["key"] != key:
            raise RuntimeError("canonical complete input/code changed after qualification")
        identity = observation["identity"]

        def validate():
            for proof in [identity["config"], identity["checkpoint"], *identity["code"]]:
                if cache.file_identity(proof["path"]) != proof:
                    raise RuntimeError("config/checkpoint/code changed during installation")
            if signal_engine._completed_panel_identity(
                    config, rule_data_dir=cfg.day_trade_rule_data_dir) != observation["source_generation"]:
                raise RuntimeError("source generation changed during installation")

        def no_compute():
            raise AssertionError("qualified private forward must exist; no model is allowed")

        controls = []
        wrapper = forward_control_wrapper(
            Path(qualified["full_forward_controls"][0]["control_receipt"]), controls)
        value, private_receipt = wrapper(private_root, identity, no_compute,
                                         validate_before_commit=validate)
        if private_receipt["state"] != "disk_hit" or controls[0]["model_calls"] != 0:
            raise RuntimeError("qualified private cache was not a complete verified hit")
        installed, receipt = cache.cached_forward(target_root, identity, lambda: value,
                                                  validate_before_commit=validate)
        require_equal_forward_outputs(value, installed)
        validate()
        if torch.cuda.is_initialized():
            raise RuntimeError("CPU cache installation initialized CUDA")
    return {"contract": "qualified_completed_forward_installation_v1", "state": "installed",
            "market": cfg.market, "panel_date": panel_date, "key": key,
            "source_generation": observation["source_generation"], "receipt": receipt,
            "benchmark_path": str(benchmark), "benchmark_sha256": hashlib.sha256(raw).hexdigest(),
            "independent_full_output_comparison": controls[0],
            "model_calls": 0, "provider_calls": 0, "cuda_initialized": False,
            "signal_publication": False, "source_edits": False,
            "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-config", type=Path, required=True)
    parser.add_argument("--panel-date", required=True)
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--target-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = install(market_config=args.market_config, panel_date=args.panel_date,
                     benchmark=args.benchmark, target_root=args.target_root)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ("state", "market", "key", "cuda_initialized")}))


if __name__ == "__main__":
    main()
