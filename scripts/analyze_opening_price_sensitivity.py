#!/usr/bin/env python3
"""Offline opening coverage and price-input sensitivity; never promote signals.

Source analysis uses original journal clocks. Signal analysis reuses the deployed
inference path, holding one source lease across all scenarios. The optional
progressive probe writes ONLY temporary signals/a synthetic-book paper ledger.
Neither command fetches quotes, publishes live signals, or submits orders.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime
from dataclasses import replace
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json


def completion_percentiles(timeline: dict) -> dict:
    """Every whole percent uses ceil(N * p / 100), not sample quantiles.

    Unreached is null. Source event delivery is not broker fill or browser paint.
    Genuine first prints are monotone; fresh absence proofs are deliberately NOT
    accumulated into this curve because they can expire or be superseded.
    """
    rows = timeline["symbols"]
    size = timeline["requested_count"]
    if size <= 0 or len(rows) != size or len({row["symbol"] for row in rows}) != size:
        raise ValueError("completion requires the entire unique official axis")
    # Failed/zero-result providers belong in the inventory too, not only winners.
    provider_names = {row["provider"] for row in timeline.get("failure_jobs", [])}
    provider_names.update(provider for row in rows for deadline in row.get("deadlines", {}).values()
        for provider in deadline.get("source_states", {}))
    sources = {name: [] for name in {"merged", *provider_names}}
    for row in rows:
        values = row["first_valid_by_provider_ms"]
        if "merged" in values:
            raise ValueError("merged must be derived from independent providers")
        for provider, value in values.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("invalid first-print observation clock")
            sources.setdefault(provider, []).append(value)
        if values:
            sources["merged"].append(min(values.values()))
    result = {}
    for source, values in sorted(sources.items()):
        values.sort()
        result[source] = {"final_true_open_count": len(values),
            "final_true_open_percent": 100 * len(values) / size,
            "percentiles": [{"completion_percent": pct, "required_count": (size * pct + 99) // 100,
                "after_open_ms": values[(size * pct + 99) // 100 - 1]
                if len(values) >= (size * pct + 99) // 100 else None} for pct in range(1, 101)]}
    return result


def compare_weights(actual: list[dict], reference: list[dict], *, field="model_weight") -> dict:
    """Compare the SAME full symbol axis; keep raw-model and target separate."""
    def axis(rows):
        result = {}
        for row in rows:
            symbol, value = row["symbol"], float(row[field])
            if symbol in result or not math.isfinite(value):
                raise ValueError("duplicate symbol or nonfinite signal")
            result[symbol] = value
        return result
    left, right = axis(actual), axis(reference)
    if not left or set(left) != set(right):
        raise ValueError("signal comparison requires identical complete model axes")
    symbols = sorted(left)
    a, b = np.asarray([left[s] for s in symbols]), np.asarray([right[s] for s in symbols])
    active_a, active_b = a != 0, b != 0
    changed = a != b
    union = np.count_nonzero(active_a | active_b)
    return {"field": field, "symbol_count": len(symbols),
        "active_count": int(active_a.sum()), "reference_active_count": int(active_b.sum()),
        "changed_count": int(changed.sum()),
        "direction_flips": int(np.count_nonzero((a * b < 0) & active_a & active_b)),
        "only_scenario_active": int(np.count_nonzero(active_a & ~active_b)),
        "only_reference_active": int(np.count_nonzero(~active_a & active_b)),
        "active_jaccard": float(np.count_nonzero(active_a & active_b) / union) if union else 1.,
        "l1_difference_percentage_points": float(np.abs(a - b).sum() * 100),
        "max_difference_percentage_points": float(np.abs(a - b).max() * 100),
        "gross_weight": float(np.abs(a).sum()), "net_weight": float(a.sum()),
        "changed_symbols": [{"symbol": symbols[i], "scenario_weight": float(a[i]),
            "reference_weight": float(b[i]), "difference_percentage_points": float((a[i] - b[i]) * 100)}
            for i in np.argsort(-np.abs(a - b), kind="stable") if changed[i]]}


def _execution_stage_clocks(signal_complete_ms: float, verified_ms: float, *,
                            execution_published: bool) -> dict:
    """No execution is inapplicable, not an observed fast financial commit.

    The historical field keeps its original elapsed no-work boundary for old
    reports/consumers; only the distinct new clocks attest execution stages.
    A reused progressive decision also has no newly published execution here.
    """
    return {"all_execution_pointers_published_ms": signal_complete_ms if execution_published else None,
            "all_execution_verified_ms": verified_ms if execution_published else None,
            "all_execution_commits_ms": verified_ms}


def compare_complete_runs(left: dict, right: dict) -> dict:
    """All scenarios and symbols; output parity is separate from timed speed.

    This does not turn two unrelated runs into a controlled ABBA experiment.
    Complete generated-ledger readback is optional for historical reports, but
    a supplied proof must be complete; quantity aggregates alone prove no P&L.
    """
    for run in (left, right):
        if (run.get("status") != "completed_offline_sensitivity_not_live_acceptance"
                or not run.get("endpoint_parity") or not all(run["endpoint_parity"].values())):
            raise ValueError("comparison requires complete endpoint-verified runs")
    for key in ("session_date", "feature_date", "receipt_sha256", "journal_prefix",
                "runtime_profile", "opening_reference_policy", "source_roots",
                "observation_horizon_ms", "full_day_source_acceptance"):
        if left.get(key) != right.get(key):
            raise ValueError(f"comparison input identity differs: {key}")
    identities = bool(left.get("model_identities") and right.get("model_identities"))
    if identities and left["model_identities"] != right["model_identities"]:
        raise ValueError("comparison checkpoint/config identities differ")
    def scenarios(run):
        rows = {row["label"]: row for row in run["scenarios"]}
        if not rows or len(rows) != len(run["scenarios"]):
            raise ValueError("comparison requires unique complete scenarios")
        return rows
    a, b = scenarios(left), scenarios(right)
    if a.keys() != b.keys():
        raise ValueError("comparison scenarios differ")
    results = []
    compared_financial_probes, financial_proofs, financial_mismatches = 0, [], []
    quantities = ("status", "fixture_quote_count", "entry_fill_outcome", "filled_shares",
                  "requested_shares", "pending_source_count", "position_count")

    def valid_financial_oracle(value):
        from stockagent.live.tw_day_trade_service_sync import PAPER_JOURNAL_FILENAMES

        def digest(text):
            return isinstance(text, str) and len(text) == 64 and all(c in "0123456789abcdef" for c in text)

        if (not isinstance(value, dict)
                or value.get("contract") != "complete_generated_paper_state_and_six_journals_v1"
                or value.get("in_memory_and_disk_complete_state_equal") is not True
                or not digest(value.get("state_sha256"))
                or value.get("excluded_state_fields") != ["engine_run_id", "dashboard_content_fingerprint"]
                or value.get("path_normalization") != "exact_isolated_run_root_only"
                or value.get("unobserved_full_session_economics_proven") is not False):
            return False
        journals = value.get("journals")
        if not isinstance(journals, dict) or set(journals) != set(PAPER_JOURNAL_FILENAMES):
            return False
        return all(isinstance(row, dict) and type(row.get("present")) is bool
            and digest(row.get("sha256")) and type(row.get("records")) is int and row["records"] >= 0
            and type(row.get("physical_lines")) is int and row["physical_lines"] >= row["records"]
            for row in journals.values())

    for label, old in a.items():
        new = b[label]
        aa = {row["market"]: row for row in old["markets"]}
        bb = {row["market"]: row for row in new["markets"]}
        if not aa or aa.keys() != bb.keys() or len(aa) != len(old["markets"]) or len(bb) != len(new["markets"]):
            raise ValueError("comparison mode axes differ")
        parity = []
        for market, row in aa.items():
            weight_checks = {field: compare_weights(row["weights"], bb[market]["weights"], field=field)["changed_count"]
                             for field in ("model_weight", "target_weight")}
            pa, pb = row.get("execution_probe"), bb[market].get("execution_probe")
            probe_match = ((pa is None and pb is None) or (isinstance(pa, dict) and isinstance(pb, dict)
                and all(pa.get(key) == pb.get(key) for key in quantities)))
            ledger_equal = None
            if isinstance(pa, dict) or isinstance(pb, dict):
                compared_financial_probes += 1
                oa = pa.get("ledger_financial_oracle") if isinstance(pa, dict) else None
                ob = pb.get("ledger_financial_oracle") if isinstance(pb, dict) else None
                supplied = oa is not None or ob is not None
                present = valid_financial_oracle(oa) and valid_financial_oracle(ob)
                if supplied and not present:
                    raise ValueError("complete generated financial oracle is missing or invalid")
                if present:
                    ledger_equal = oa == ob
                    if not ledger_equal:
                        financial_mismatches.append({"scenario": label, "market": market})
                financial_proofs.append(present and ledger_equal)
            parity.append({"market": market, "changed_counts": weight_checks, "execution_quantities_equal": probe_match,
                           "complete_generated_ledger_values_equal": ledger_equal})
        results.append({"label": label, "markets": parity,
            "left_all_execution_commits_ms": old.get("all_execution_commits_ms"),
            "right_all_execution_commits_ms": new.get("all_execution_commits_ms"),
            "left_full_scenario_ms": old["all_models_elapsed_ms"],
            "right_full_scenario_ms": new["all_models_elapsed_ms"]})
    weights_quantities_equal = all(not any(row["changed_counts"].values()) and row["execution_quantities_equal"]
                for scenario in results for row in scenario["markets"])
    return {"status": "offline_outputs_equal" if weights_quantities_equal and not financial_mismatches else "rejected_output_parity",
            "session_date": left["session_date"], "model_identity_attested_in_both_reports": identities,
            "all_weights_and_execution_quantities_equal": weights_quantities_equal, "scenarios": results,
            "all_generated_ledger_values_equal": bool(compared_financial_probes and all(financial_proofs)),
            "compared_financial_probes": compared_financial_probes,
            "financial_mismatches": financial_mismatches,
            "native_opening_slo_proven": False, "all_ledger_economics_proven": False,
            "timing_boundary": "unpaired_until_independently_run_in_balanced_controlled_order"}


def compact_signal_summary(report: dict) -> dict:
    """Small inspection index; retain and hash-bind the complete evidence."""
    if (report.get("status") != "completed_offline_sensitivity_not_live_acceptance"
            or not report.get("endpoint_parity") or not all(report["endpoint_parity"].values())):
        raise ValueError("only completed endpoint-verified sensitivity can be summarized")
    result = {key: report[key] for key in ("contract", "status", "session_date", "feature_date",
        "receipt_sha256", "journal_prefix", "runtime_profile", "mounted_code_reference", "source_roots",
        "opening_reference_policy", "boundary", "full_price_reference_is_late_information_not_available_at_open",
        "endpoint_parity", "warmups", "network_requests", "production_writes", "broker_fill_claim")}
    result["scenarios"] = []
    result["input_scheduling"] = report.get("input_scheduling", "sequential")
    result["queued_refresh_policy"] = report.get("queued_refresh_policy")
    result["model_identities"] = report.get("model_identities")
    result["clock_contract"] = report.get("clock_contract")
    for scenario in report["scenarios"]:
        markets = []
        for market in scenario["markets"]:
            comparisons = {field: {key: value for key, value in comparison.items() if key != "changed_symbols"}
                for field, comparison in market["vs_full_available"].items()}
            # Diagnostic magnitude only; never prune/rewrite actual model rows.
            for field, comparison in market["vs_full_available"].items():
                comparisons[field]["changed_at_least_001_percentage_points"] = sum(
                    abs(row["difference_percentage_points"]) >= .01 for row in comparison["changed_symbols"])
            markets.append({"market": market["market"], "elapsed_ms": market["elapsed_ms"],
                "execution_publication_ms": market.get("execution_publication_ms"),
                "consumer_commit_from_batch_start_ms": market.get("consumer_commit_from_batch_start_ms"),
                "deferred_artifact_ms": market.get("deferred_artifact_ms"),
                "queued_quote_refresh_ms": market.get("queued_quote_refresh_ms"),
                "input_transport": {key: market["stages"][key] for key in
                    ("dense_input_transport", "factorized_input_transport") if key in market["stages"]},
                "prepare_clock_contract": market["stages"].get("prepare_clock_contract"),
                "post_quote_prepare_steps_ms": market["stages"].get("post_quote_prepare_steps_ms"),
                "input_changed": market.get("input_changed"),
                "signal_revision": market.get("signal_revision"),
                "execution_probe": market.get("execution_probe"),
                "input_reference": {key: value for key, value in market["input_reference"].items() if key != "rows"},
                "vs_full_available": comparisons,
                "stages_ms": {key: value for key, value in market["stages"].items() if key.endswith("_ms")
                    and (value is None or isinstance(value, (int, float)))}})
        result["scenarios"].append({"label": scenario["label"], "after_open_ms": scenario["after_open_ms"],
            "all_signal_calculations_complete_ms": scenario.get("all_signal_calculations_complete_ms"),
            "all_execution_pointers_published_ms": scenario.get("all_execution_pointers_published_ms"),
            "all_execution_verified_ms": scenario.get("all_execution_verified_ms"),
            "scenario_complete_including_reports_ms": scenario.get("scenario_complete_including_reports_ms"),
            "all_execution_commits_ms": scenario.get("all_execution_commits_ms"),
            "all_models_elapsed_ms": scenario["all_models_elapsed_ms"], "markets": markets})
    return result


def source_analysis(receipt: Path, journal: Path, *, cutoffs: tuple[int, ...],
                    observation_horizon_ms: int | None = None) -> dict:
    from scripts.audit_tw_opening_latency import opening_session_context
    from scripts.audit_tw_opening_symbol_status import journal_symbol_timeline, receipt_snapshot
    from scripts.benchmark_opening_signal_pipeline import _receipt_universe
    from stockagent.live import quote_provider
    from stockagent.live.market_config import load_market_configs

    raw = receipt.read_bytes()
    payload = json.loads(raw)
    gate = datetime.fromisoformat(payload["session_date"] + "T09:00:00+08:00")
    captured = datetime.fromisoformat(payload["captured_at_taipei"])
    captured_ms = final_ms = int((captured - gate).total_seconds() * 1000)
    if observation_horizon_ms is not None:
        if type(observation_horizon_ms) is not int or not 0 <= observation_horizon_ms <= final_ms:
            raise ValueError("opening observation horizon must be within the retained receipt clock")
        final_ms = observation_horizon_ms
    timeline = journal_symbol_timeline(journal, payload, _receipt_universe(payload),
        cutoffs=tuple(sorted({0, final_ms, *cutoffs})), include_reference_rows=True,
        observation_horizon_ms=observation_horizon_ms)
    names, snapshot = receipt_snapshot(payload)
    real = quote_provider.tw_session_open_observed_mask(snapshot, session_date=payload["session_date"])
    no_print = quote_provider.tw_session_no_open_print_mask(snapshot, session_date=payload["session_date"],
        asof_timestamp_ms=int(datetime.fromisoformat(snapshot.timestamp).timestamp() * 1000))
    cfgs = [cfg for cfg in load_market_configs(ROOT / "services/discord_bot/markets").values()
            if cfg.day_trade_simulation_enabled and cfg.enabled]
    context = opening_session_context(cfgs, payload["session_date"])
    unknown_reasons = {}
    for cutoff in timeline["milestones"]:
        key = str(cutoff["after_open_ms"])
        unknown_reasons[key] = dict(Counter(f"{provider}:{state}"
            for row in timeline["symbols"] if row["deadlines"][key]["state"] == "unknown"
            for provider, state in row["deadlines"][key]["source_states"].items()))
    if receipt.read_bytes() != raw:
        raise RuntimeError("receipt changed during offline analysis")
    return {"contract": "opening_all_percent_source_analysis_v1", "session_date": payload["session_date"],
        "receipt": str(receipt), "receipt_sha256": hashlib.sha256(raw).hexdigest(),
        "session_applicability": context, "source_timeline": timeline,
        "completion_by_provider": completion_percentiles(timeline), "unknown_source_reasons_at_cutoffs": unknown_reasons,
        "observation_horizon_ms": observation_horizon_ms,
        "full_day_source_acceptance": timeline["state"] == "observed" and observation_horizon_ms is None,
        "final_receipt": {"captured_at_taipei": payload["captured_at_taipei"], "after_open_ms": captured_ms,
            "official_count": len(names), "true_open_count": int(real.sum()),
            "fresh_no_first_print_count": int((no_print & ~real).sum()),
            "unknown_count": int((~real & ~no_print).sum())},
        "boundary": "retained_source_delivery_not_publication_fill_or_paint",
        "network_requests": 0, "production_writes": False}


def signal_analysis(analysis: dict, *, feature_date: str, runtime_profile: Path, output: Path,
                    progressive_execution: bool = False, profile_execution: bool = False,
                    source_wait_seconds: float = 0, parallel_inputs: bool = False,
                    repeat_prepare_control: bool = False, overlap_ledger: bool = False) -> dict:
    if parallel_inputs and not progressive_execution:
        raise ValueError("parallel inputs require the progressive execution probe")
    if repeat_prepare_control and not parallel_inputs:
        raise ValueError("repeat preparation control requires parallel inputs")
    if overlap_ledger and not (progressive_execution and parallel_inputs):
        raise ValueError("overlapped ledger requires progressive execution and parallel inputs")
    # Import canonical owners lazily: pure metric tests need no bot/CUDA setup.
    from scripts import benchmark_opening_signal_pipeline as owner
    from scripts.audit_tw_opening_symbol_status import journal_reference_receipt
    from stockagent.data import tw_current_trading_universe as registry
    from stockagent.live.opening_price_priority import model_identity_from_summary
    from stockagent.live.market_config import SIGNAL_UPDATE_ONCE, SIGNAL_UPDATE_PROGRESSIVE
    from stockagent.live.opening_signal_revision import opening_input_fingerprint
    import requests
    import torch

    profile = owner._benchmark_runtime_profile(runtime_profile)
    receipt = Path(analysis["receipt"])
    raw = receipt.read_bytes()
    if hashlib.sha256(raw).hexdigest() != analysis["receipt_sha256"]:
        raise ValueError("source analysis receipt changed")
    payload = json.loads(raw)
    session = payload["session_date"]
    if feature_date >= session:
        raise ValueError("completed features must precede session")
    if analysis["source_timeline"]["state"] != "observed":
        raise ValueError("degraded source timeline cannot establish sensitivity inputs")
    if not analysis["completion_by_provider"]["merged"]["final_true_open_count"]:
        raise ValueError("no true opening prices available for the full-price comparator")
    cfgs = [owner.bot_mod._effective_market_config(owner.bot_mod._resolve_market(m))
        for m in owner.bot_mod._scheduled_markets() if owner.bot_mod._resolve_market(m).day_trade_simulation_enabled]
    cfgs.sort(key=lambda cfg: (-owner.bot_mod._preopen_market_symbol_count(cfg), cfg.market))
    if progressive_execution:
        cfgs = [replace(cfg, day_trade_signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE) for cfg in cfgs]
    owner._require_selected_sparse_runtime(cfgs)
    if any(str(cfg.device).startswith("cuda") for cfg in cfgs) and not torch.cuda.is_available():
        raise RuntimeError("selected CUDA models may not fall back to CPU")
    code = None
    if release := os.environ.get("STOCKAGENT_SERVICE_CODE_RELEASE"):
        from stockagent.live.service_code_release import verify_mounted_source
        code = verify_mounted_source(Path(release), ROOT, os.getpid())
    clock = [datetime.fromisoformat(session + "T08:50:00+08:00")]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz or ZoneInfo("Asia/Taipei"))

    records, warmups, identities = [], [], {}
    report = {"contract": "single_generation_opening_price_sensitivity_v1", "session_date": session,
        "feature_date": feature_date, "receipt_sha256": analysis["receipt_sha256"],
        "journal_prefix": analysis["source_timeline"]["journal_prefix"], "runtime_profile": profile,
        "mounted_code_reference": code, "warmups": warmups, "scenarios": records,
        "model_identities": identities,
        "opening_reference_policy": "known_prior_trade_provisional_v1",
        "input_scheduling": ("parallel_prefetch_serial_model_independent_single_ledger_owner" if overlap_ledger
            else "parallel_prefetch_serial_model_and_ledger" if parallel_inputs else "sequential"),
        "queued_refresh_policy": "repeat_complete_preparation_control" if repeat_prepare_control else "cache_only_provisional_facts",
        "network_requests": 0, "production_writes": False, "broker_fill_claim": False,
        "boundary": "counterfactual_model_input_sensitivity_not_live_latency_or_execution",
        "observation_horizon_ms": analysis.get("observation_horizon_ms"),
        "full_day_source_acceptance": analysis.get("full_day_source_acceptance", False),
        "full_price_reference_is_late_information_not_available_at_open": True}
    if progressive_execution:
        report.update(progressive_execution=True,
            isolated_ledger_initial_clock=clock[0].isoformat(),
            isolated_ledger_clock_contract="explicit_empty_fixture_initialization_not_observation_rewrite_v1",
            isolated_signal_id_contract="explicit_fixture_signal_id_before_inference_v1",
            boundary="offline_original_input_clocks_with_synthetic_books_and_isolated_ledger_not_live_SLO",
            zero_fill_is_not_a_successful_entry=True)
    with tempfile.TemporaryDirectory(prefix="opening-sensitivity-") as temporary, ExitStack() as stack:
        root = Path(temporary)
        started = time.perf_counter()
        report["source_roots"] = owner._hold_source_read_leases(stack, cfgs, wait_seconds=source_wait_seconds)
        report["source_admission_ms"] = (time.perf_counter() - started) * 1000
        stack.enter_context(patch.object(requests.sessions.Session, "request",
            side_effect=AssertionError("offline sensitivity forbids network")))
        for module in (registry, owner.signal_engine, owner.quote_provider, owner.opening_price_acquisition):
            stack.enter_context(patch.object(module, "datetime", Clock))
        for module, name in ((owner.signal_engine, "fetch_shared_day_trade_stock_snapshots"),
                (owner.signal_engine, "fetch_shioaji_stock_snapshots"),
                (owner.quote_provider, "fetch_tw_mis_last_prices")):
            stack.enter_context(patch.object(module, name, side_effect=AssertionError("offline sensitivity forbids network")))
        stack.enter_context(patch.dict(os.environ, {"STOCKAGENT_BOT_PROGRESS": "0",
            "STOCKAGENT_LIVE_PANEL_CACHE_ROOT": str(root / "panels")}))
        for cfg in cfgs:
            kwargs = cfg.signal_kwargs(write=False, price_source="panel", ensure_previous_signal=False,
                previous_signal_backfill_limit=0, panel_date=feature_date,
                asof_date=clock[0].strftime("%Y-%m-%d %H:%M:%S"), day_trade_model_observation="session_open",
                day_trade_opening_reference_policy=report["opening_reference_policy"])
            started = time.perf_counter()
            result = owner.signal_engine.generate_live_signal(**kwargs)
            identities[cfg.market] = model_identity_from_summary(result.summary)
            warmups.append({"market": cfg.market, "elapsed_ms": (time.perf_counter() - started) * 1000})
            atomic_write_json(output, dict(report, status="preparing"))
        parents = {}
        engine, specs, ledger_paths, entry_worker = None, {}, {}, None
        if progressive_execution:
            from scripts.run_tw_day_trade_simulation import _mode_specs
            loaded, _, errors = _mode_specs(ROOT / "services/discord_bot/markets")
            if errors:
                raise ValueError(f"execution contracts unavailable: {errors}")
            specs = {spec.market: replace(spec, signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE,
                opening_reference_policy=report["opening_reference_policy"]) for spec in loaded}
            ledger_root = root / "isolated-paper-ledger"
            if overlap_ledger:
                # Match the real model/ledger ownership, never fork CUDA or
                # let two threads/processes mutate one engine. The same
                # consumer persists all scenarios/revisions in FIFO order.
                entry_worker = stack.enter_context(ProcessPoolExecutor(
                    max_workers=1, mp_context=multiprocessing.get_context("spawn")))
                ledger_paths = entry_worker.submit(owner._prepare_entry_worker, ledger_root, {},
                                                   created_at=clock[0]).result()
            else:
                engine = owner._new_isolated_entry_engine(ledger_root, created_at=clock[0])
                ledger_paths = {"state": str(engine.state_path), "signals": str(engine.signals_path),
                    "orders": str(engine.orders_path), "fills": str(engine.fills_path)}
        report["execution_profiler_enabled"] = profile_execution
        cutoffs = [row["after_open_ms"] for row in analysis["source_timeline"]["milestones"]]
        # Repeated endpoints detect stale receipt/model cache contamination.
        sequence = [(str(c), c) for c in cutoffs] + [("final_repeat", cutoffs[-1]), ("zero_repeat", 0)]
        for label, cutoff in sequence:
            projected = journal_reference_receipt(payload, analysis["source_timeline"], cutoff)
            clock[0] = datetime.fromisoformat(projected["captured_at_taipei"])
            receipts = root / ("receipts-" + label)
            receipts.mkdir()
            atomic_write_json(receipts / f"{session}.json", projected)
            scenario = {"label": label, "after_open_ms": cutoff, "markets": []}
            pending_artifacts = []
            entry_jobs = []
            started = time.perf_counter()
            with ExitStack() as scenario_stack:
                scenario_stack.enter_context(patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": str(receipts)}))
                # Cache keys intentionally omit diagnostic receipt roots; isolate
                # only this process's mutable quote cache between price scenarios.
                for key, value in (("_TW_MIS_OPENING_CACHE_KEY", None), ("_TW_MIS_OPENING_CACHE", {}),
                        ("_TW_MIS_OPENING_REQUESTED", set()), ("_TW_MIS_OPENING_DIRTY", False)):
                    scenario_stack.enter_context(patch.object(owner.quote_provider, key, value))
                actionable = (progressive_execution and label not in {"final_repeat", "zero_repeat"}
                              and clock[0].strftime("%H:%M") < "13:20")
                def arguments(cfg):
                    return cfg.signal_kwargs(write=False, price_source="tw", ensure_previous_signal=False,
                        previous_signal_backfill_limit=0, panel_date=feature_date,
                        asof_date=clock[0].strftime("%Y-%m-%d %H:%M:%S"),
                        day_trade_model_observation="session_open",
                        day_trade_opening_reference_policy=report["opening_reference_policy"])
                prepared = {}
                if parallel_inputs and actionable:
                    pool = scenario_stack.enter_context(ThreadPoolExecutor(max_workers=len(cfgs)))
                    prepared = {cfg.market: pool.submit(owner.signal_engine.prefetch_live_signal_prices,
                                                       **arguments(cfg)) for cfg in cfgs}
                for cfg in cfgs:
                    kwargs = arguments(cfg)
                    call_started = time.perf_counter()
                    input_changed, published_ms, entry_probe = True, None, None
                    refresh_ms = None
                    if actionable:
                        prefetched = (prepared[cfg.market].result() if prepared else
                                      owner.signal_engine.prefetch_live_signal_prices(**kwargs))
                        if prepared and time.perf_counter() - prefetched.quote_finished > .05:
                            refresh_started = time.perf_counter()
                            prefetched = (owner.signal_engine.prefetch_live_signal_prices(**kwargs)
                                if repeat_prepare_control else
                                owner.signal_engine.refresh_provisional_opening_prefetch(prefetched))
                            refresh_ms = (time.perf_counter() - refresh_started) * 1000
                        key = opening_input_fingerprint(prefetched.request_key, prefetched.snapshot, session_date=session)
                        parent = parents.get(cfg.market)
                        input_changed = parent is None or parent.summary["opening_signal_revision"]["input_fingerprint"] != key
                        if input_changed:
                            kwargs.update(write=True, defer_rich_artifacts=True,
                                live_output_dir=root / "signals" / cfg.market,
                                signal_id=owner._isolated_signal_id(
                                    market=cfg.market, observed=clock[0], scenario=label),
                                day_trade_signal_update_policy=SIGNAL_UPDATE_PROGRESSIVE,
                                _prefetched_quote=prefetched,
                                _previous_opening_signal=parent.summary if parent else None)
                            result = owner.signal_engine.generate_live_signal(**kwargs)
                            parents[cfg.market] = result
                            published_ms = (time.perf_counter() - call_started) * 1000
                            probe_profile = (output.parent / (output.stem + ".profiles") / label / (cfg.market + ".prof")
                                             if profile_execution else None)
                            # Use this scenario's synthetic books, not the final
                            # receipt. Isolate the child's output/clock exactly
                            # as for the sequential control.
                            probe_spec = replace(specs[cfg.market], live_output_dir=Path(kwargs["live_output_dir"]))
                            if entry_worker is not None:
                                future = entry_worker.submit(owner._timed_paper_entry_probe, started,
                                    probe_spec, dict(result.summary), result.weights_rows, clock[0],
                                    source_rows=projected.get("rows") or {}, profile_path=probe_profile)
                                entry_job = {"future": future, "observed": threading.Event()}

                                def receipt_completed(_future, job=entry_job, batch_start=started):
                                    # Parent-observed IPC completion is a
                                    # conservative bound; do not subtract an
                                    # unrelated child's monotonic clock.
                                    job["parent_observed_ms"] = (time.perf_counter() - batch_start) * 1000
                                    job["observed"].set()

                                future.add_done_callback(receipt_completed)
                                entry_jobs.append(entry_job)
                            else:
                                entry_probe = owner._isolated_paper_entry_probe(engine, probe_spec, result,
                                    clock[0], projected.get("rows") or {}, profile_path=probe_profile)
                        else:
                            result = parent
                    else:
                        # Late-information and repeated endpoint comparators
                        # are never replayed into the causal revision cohort.
                        kwargs["day_trade_signal_update_policy"] = SIGNAL_UPDATE_ONCE
                        result = owner.signal_engine.generate_live_signal(**kwargs)
                    if model_identity_from_summary(result.summary) != identities[cfg.market]:
                        raise RuntimeError("model/config/completed-source identity changed across price inputs")
                    scenario["markets"].append({"market": cfg.market,
                        "elapsed_ms": (time.perf_counter() - call_started) * 1000,
                        "input_reference": owner._input_reference_evidence(result),
                        "stages": result.summary["live_latency"],
                        "input_changed": input_changed,
                        "execution_publication_ms": published_ms,
                        "execution_probe": entry_probe,
                        "queued_quote_refresh_ms": refresh_ms,
                        "consumer_commit_from_batch_start_ms": (None if entry_worker is not None and actionable
                            and input_changed else (time.perf_counter() - started) * 1000),
                        "signal_revision": result.summary.get("opening_signal_revision"),
                        "weights": [{key: row.get(key) for key in ("symbol", "model_weight", "target_weight")}
                            for row in result.weights_rows]})
                    if actionable and input_changed:
                        pending_artifacts.append((result, scenario["markets"][-1]))
                        if entry_worker is not None:
                            entry_jobs[-1]["market_row"] = scenario["markets"][-1]
                scenario["all_signal_calculations_complete_ms"] = (time.perf_counter() - started) * 1000
                for job in entry_jobs:
                    probe, _child_relative_clock = job["future"].result()
                    job["observed"].wait()
                    job["market_row"]["execution_probe"] = probe
                    job["market_row"]["consumer_commit_from_batch_start_ms"] = job["parent_observed_ms"]
                scenario.update(_execution_stage_clocks(
                    scenario["all_signal_calculations_complete_ms"], (time.perf_counter() - started) * 1000,
                    execution_published=bool(pending_artifacts)))
                # Match the existing live critical path: rich reports must not
                # block the next mode's execution pointer. Still finish and
                # time every report before this full-workflow scenario ends.
                for result, market in pending_artifacts:
                    completed = time.perf_counter()
                    owner.signal_engine.finalize_live_signal_artifacts(result)
                    market["deferred_artifact_ms"] = (time.perf_counter() - completed) * 1000
            scenario["scenario_complete_including_reports_ms"] = (time.perf_counter() - started) * 1000
            scenario["all_models_elapsed_ms"] = scenario["scenario_complete_including_reports_ms"]
            records.append(scenario)
            atomic_write_json(output, dict(report, status="measuring"))
        report["clock_contract"] = {
            "all_signal_calculations_complete_ms": "same parent monotonic clock; six-mode loop including input waits, not pure GPU",
            "all_execution_pointers_published_ms": "new execution revisions only; all mode execution pointers published, not broker fills; otherwise null",
            "all_execution_verified_ms": "new execution revisions only; parent IPC acknowledgement after durable publication AND complete financial readback; conservative commit upper bound; otherwise null",
            "scenario_complete_including_reports_ms": "includes every deferred rich artifact after execution verification",
            "legacy_all_execution_commits_ms": "historical elapsed verification/no-work boundary; equals all_execution_verified_ms only when execution was published; not raw fsync or proof of a commit",
            "legacy_all_models_elapsed_ms": "alias of scenario_complete_including_reports_ms, not pure model time"}
        final = {row["market"]: row["weights"] for row in records[-3]["markets"]}
        baseline = {row["market"]: row["weights"] for row in records[0]["markets"]}
        for scenario in records:
            for market in scenario["markets"]:
                weights = market["weights"]
                market["vs_full_available"] = {field: compare_weights(weights, final[market["market"]], field=field)
                    for field in ("model_weight", "target_weight")}
                market["vs_all_missing"] = compare_weights(weights, baseline[market["market"]])
        report["endpoint_parity"] = {row["label"]: all(
            compare_weights(m["weights"], (baseline if row["label"] == "zero_repeat" else final)[m["market"]])
                ["changed_count"] == 0 for m in row["markets"]) for row in records[-2:]}
        if not all(report["endpoint_parity"].values()):
            raise RuntimeError("repeated endpoint weights changed; sensitivity comparison rejected")
        if ledger_paths:
            # Independent disk readback survives temporary-directory cleanup
            # in the private evidence, including exact quantities and fees.
            report["isolated_execution_readback"] = {name: {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None,
                "rows": [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []}
                for name, path in ((name, Path(ledger_paths[name])) for name in ("signals", "orders", "fills"))}
            report["isolated_execution_modes"] = {
                market: {key: mode.get(key) for key in ("signal_id", "opening_signal_revision",
                    "entry_fill_outcome", "pending_entry_shares", "pending_reduction_shares",
                    "cumulative_realized_net_pnl_twd", "total_equity_twd", "positions")}
                for market, mode in json.loads(Path(ledger_paths["state"]).read_text())["modes"].items()}
    report["model_identities"] = identities
    report["status"] = "completed_offline_sensitivity_not_live_acceptance"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("source")
    source.add_argument("--receipt", type=Path, required=True)
    source.add_argument("--journal", type=Path, required=True)
    source.add_argument("--cutoffs-ms", default="1000,5000,11000,20000,30000,60000")
    source.add_argument("--observation-horizon-ms", type=int,
        help="Explicit opening-only analysis window; later events remain hashed and excluded, never repaired or accepted as a full day")
    source.add_argument("--output", type=Path, required=True)
    signal = commands.add_parser("signals")
    signal.add_argument("--source-analysis", type=Path, required=True)
    signal.add_argument("--feature-date", required=True)
    signal.add_argument("--runtime-profile", type=Path, required=True)
    signal.add_argument("--output", type=Path, required=True)
    signal.add_argument("--progressive-execution", action="store_true",
        help="Measure actual canonical revision/delta code on an isolated synthetic-book ledger; never live fills")
    signal.add_argument("--profile-execution", action="store_true",
        help="Retain per-mode cProfile evidence; profiling adds overhead and is not an SLO timing")
    signal.add_argument("--parallel-inputs", action="store_true",
        help="Match live parallel CPU prefetch; keep canonical model/ledger ordering")
    signal.add_argument("--overlap-ledger", action="store_true",
        help="Overlap model N+1 with one independent ledger owner; never parallelize ledger writes")
    signal.add_argument("--repeat-prepare-control", action="store_true",
        help="With parallel-inputs, compare former full preparation after a model queue")
    signal.add_argument("--source-wait-seconds", type=float, default=0,
        help="Bounded wait for the canonical source lease; never bypass an active writer")
    compact = commands.add_parser("summary")
    compact.add_argument("--analysis", type=Path, required=True)
    compact.add_argument("--output", type=Path, required=True)
    compare = commands.add_parser("compare")
    compare.add_argument("--left", type=Path, required=True)
    compare.add_argument("--right", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve existing diagnostic evidence; choose a new output path")
    if args.command == "source":
        report = source_analysis(args.receipt, args.journal,
            cutoffs=tuple(int(value) for value in args.cutoffs_ms.split(",")),
            observation_horizon_ms=args.observation_horizon_ms)
    elif args.command == "signals":
        try:
            report = signal_analysis(json.loads(args.source_analysis.read_text()), feature_date=args.feature_date,
                runtime_profile=args.runtime_profile, output=args.output,
                progressive_execution=args.progressive_execution, profile_execution=args.profile_execution,
                source_wait_seconds=args.source_wait_seconds, parallel_inputs=args.parallel_inputs,
                repeat_prepare_control=args.repeat_prepare_control, overlap_ledger=args.overlap_ledger)
        except Exception as exc:
            partial = json.loads(args.output.read_text()) if args.output.exists() else {}
            atomic_write_json(args.output, dict(partial, status="failed_not_sensitivity_acceptance",
                error_type=type(exc).__name__, error=str(exc), production_writes=False))
            raise
    elif args.command == "compare":
        from stockagent.private_json import loads
        reports, proofs = [], []
        for path in (args.left, args.right):
            raw = path.read_bytes()
            reports.append(loads(raw))
            proofs.append({"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
        report = compare_complete_runs(*reports)
        report["complete_evidence"] = proofs
    else:
        raw = args.analysis.read_bytes()
        report = compact_signal_summary(json.loads(raw))
        report["complete_evidence"] = {"path": str(args.analysis), "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest()}
    atomic_write_json(args.output, report)
    print(json.dumps({"output": str(args.output), "session_date": report["session_date"],
        "status": report.get("status", "analyzed_original_journal"), "production_writes": False}))
    if report.get("status") == "rejected_output_parity":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
