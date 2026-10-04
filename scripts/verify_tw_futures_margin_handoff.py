#!/usr/bin/env python3
"""Verify a pinned remote margin rebuild and its canonical training artifacts."""
from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.config import load_config
from stockagent.data.tw_futures_margin import validate_margin_rule_source, MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION
from stockagent.data_sync.materialized_cache import _lease_path, _ready_path
from stockagent.runtime_identity import verify_source_release
from stockagent.training.lifecycle import validate_completed_training_artifacts


def verify(config: Path, build: Path, code_receipt: Path, output: Path,
           *, run: Path | None = None, epochs: int | None = None, prior: Path | None = None,
           runtime_log: Path | None = None) -> dict:
    identity = verify_source_release(code_receipt, ROOT)
    c = load_config(config)
    t = c.trading
    assert c.runner.require_cuda and t.tw_futures_portfolio_capital_basis == "initial_margin"
    assert t.tw_futures_portfolio_holding_policy == "carry"
    assert t.tw_futures_portfolio_integer_initial_capital == 100000000
    assert c.training.pretrained_initialization_root is None
    accepted = json.loads((build / "build_acceptance.json").read_text())
    assert accepted["compiler_sha256"] == sha256_file(ROOT / "stockagent/data/tw_futures_execution_terms.py")
    assert accepted["builder_sha256"] == sha256_file(ROOT / "scripts/prepare_tw_futures_margin_training.py")
    daily, rules = Path(t.tw_futures_portfolio_data_path), Path(t.tw_futures_portfolio_margin_rules_path)
    assert daily.resolve() == (build / "release/daily/continuous_daily.parquet").resolve()
    assert rules.resolve() == (build / "release/rules/rules.parquet").resolve()
    assert sha256_file(daily) == accepted["daily_sha256"]
    assert sha256_file(rules) == accepted["rules_sha256"]
    _, proof = validate_margin_rule_source(rules, daily)
    assert set(proof["scope"]["products"]) == set(accepted["selected_products"])
    scope = pl.read_parquet(build / "lifetime_scope.parquet")
    assert sha256_file(build / "lifetime_scope.parquet") == accepted["lifetime_scope_sha256"]
    assert not scope.filter(pl.col("selected") & pl.col("unresolved_accounting")).height
    final = Path(t.tw_futures_portfolio_final_settlement_path)
    dm = json.loads(daily.with_name("manifest.json").read_text())
    assert dm["official_final_settlement_sha256"] == sha256_file(final)
    source = final.parent.parent
    assert sha256_file(source / "source_manifest.json") == accepted["source_manifest_sha256"]
    snapshot = Path(c.data.parquet_root).parent.name
    cache = Path("/srv/stockagent-packed-materialized")
    lease = json.loads(_lease_path(cache, "tw-public", snapshot).read_text())
    ready = json.loads(_ready_path(cache, "tw-public", snapshot).read_text())
    assert lease["state"] == "hot" and int(lease["expires_ns"]) > time.time_ns()
    assert lease["manifest_sha256"] == ready["manifest_sha256"]
    assert lease["inventory_sha256"] == ready["inventory_sha256"]
    result = dict(status="remote_source_preflight_passed", code=identity,
        config=str(config), config_sha256=sha256_file(config),
        requested_products=len(accepted["requested_products"]),
        trained_scope_products=len(accepted["selected_products"]),
        excluded_products=accepted["excluded_products"], rows=accepted["rows"],
        remaining_source_blockers=accepted["remaining_blocked_account_rows"],
        selected_account_blockers=0, selection_is_retrospective=True,
        full_history_training_ready=False, source_manifest_sha256=accepted["source_manifest_sha256"],
        stock_snapshot=snapshot, stock_manifest_sha256=ready["manifest_sha256"],
        gradient_contract_version=MARGIN_TRAINING_GRADIENT_CONTRACT_VERSION,
        initial_capital_twd=100000000, execution="margin_carry_daily_open_proxy",
        runtime_training_verified=False, profitability_validated=False)
    if run is not None:
        if epochs is None or runtime_log is None:
            raise ValueError("runtime verification requires --epochs and --runtime-log")
        ranks = {int(x) for x in re.findall(r"rank=([01])/2\b", runtime_log.read_text())}
        assert ranks == {0, 1}, "both initialized DDP ranks must be observed"
        groups = sorted(p.name for p in run.glob("train_*") if p.is_dir())
        assert len(groups) == 1
        gate = validate_completed_training_artifacts(run, fold_ids=[10], group_names=groups)
        gate.require()
        curve = [json.loads(line) for line in (run / groups[0] / "epoch_curve.jsonl").read_text().splitlines()]
        assert [int(row["epoch"]) for row in curve] == list(range(1, epochs + 1))
        for row in curve:
            assert int(row["train_optimizer_steps"]) >= 1
            assert math.isfinite(row["epoch_max_rank_s"]) and row["epoch_max_rank_s"] > 0
            assert math.isfinite(row["train_grad_norm_before_clip_mean"]) and row["train_grad_norm_before_clip_mean"] > 0
        if prior is not None:
            old = json.loads(prior.read_text())
            assert curve[:len(old["curves"])] == old["curves"], "resume rewrote earlier epochs"
        result.update(status="remote_margin_training_verified", runtime_training_verified=True,
            runtime_log_sha256=sha256_file(runtime_log), observed_ddp_ranks=sorted(ranks),
            run=str(run), epochs=epochs, curves=curve,
            artifact_gate=dict(ok=gate.ok, checked=len(gate.checked), missing=[], invalid=[]))
    atomic_write_json(output, result)
    print(json.dumps({k:v for k,v in result.items() if k not in ["code", "curves"]}), flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["config", "build", "code-receipt", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--run", type=Path)
    parser.add_argument("--epochs", type=int)
    parser.add_argument("--prior", type=Path)
    parser.add_argument("--runtime-log", type=Path)
    args = parser.parse_args()
    verify(args.config, args.build, args.code_receipt, args.output,
           run=args.run, epochs=args.epochs, prior=args.prior, runtime_log=args.runtime_log)
