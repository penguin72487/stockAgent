"""Replay deployed opening models against a pinned receipt, without live writes.

Measures warm application compute/cache/publication, NOT exchange/network delay.
All generated signals and copied quote receipts stay in a temporary directory.
No quotes are fetched, no Discord messages or orders are sent.
"""
from __future__ import annotations

import argparse
import cProfile
from contextlib import ExitStack, nullcontext
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timedelta
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
from types import SimpleNamespace
import shutil
import statistics
import sys
import tempfile
import threading
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.discord_bot import bot as bot_mod
from downloader.artifact_io import atomic_write_json
from stockagent import config as config_module
from stockagent.live import opening_price_acquisition, quote_provider, signal_engine


_RUNTIME_PROFILE_KEYS = frozenset({"STOCKAGENT_SPARSE_ADJOINT_BACKEND", "STOCKAGENT_SPARSE_ADJOINT_ALGORITHM"})


def _benchmark_runtime_profile(path: Path | None) -> dict:
    """Explicit non-secret deployment ABI, before expensive input building.

    Do not load broker/Discord credentials or execute an arbitrary shell file.
    Conflicting caller settings must be resolved, not silently overwritten.
    Checkpoint compatibility validation still runs unchanged afterwards.
    """
    proof = {"basis": "caller_environment_not_owner_attestation"}
    if path is not None:
        from dotenv import dotenv_values
        raw = path.read_bytes()
        from io import StringIO
        values = dict(dotenv_values(stream=StringIO(raw.decode()), interpolate=False))
        if (not values or set(values) - _RUNTIME_PROFILE_KEYS
                or any(not isinstance(value, str) or not value for value in values.values())):
            raise ValueError("benchmark runtime profile permits only explicit non-secret sparse-adjoint ABI keys")
        if any(key in os.environ and os.environ[key] != value for key, value in values.items()):
            raise ValueError("benchmark runtime profile conflicts with the caller environment")
        os.environ.update(values)
        proof = {"basis": "explicit_profile_not_owner_attestation", "path": str(path),
                 "sha256": hashlib.sha256(raw).hexdigest()}
    proof["values"] = {key: os.environ.get(key) for key in sorted(_RUNTIME_PROFILE_KEYS)}
    return proof


def _require_selected_sparse_runtime(cfgs) -> None:
    from stockagent.training.checkpoint_contract import _active_model_config
    sparse = [cfg.market for cfg in cfgs if _active_model_config(config_module.load_config(cfg.config_path))
              ["values"].get("factorized_sparse_projection")]
    if sparse and any(not os.environ.get(key) for key in _RUNTIME_PROFILE_KEYS):
        raise ValueError("Selected sparse model runtime ABI is unspecified for " + ",".join(sparse)
            + "; supply its exact --runtime-profile before benchmarking")


def _hold_source_read_leases(stack, configs, *, wait_seconds=0):
    """Keep paired samples on one generation, using the real producer's lease.

    This offline observer may delay a writer while reading. It never bypasses
    an existing writer or counts lock contention as successful inference.
    """
    roots = sorted({str(Path(config_module.load_config(cfg.config_path).data.parquet_root).resolve())
                    for cfg in configs})
    from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
    if not 0 <= wait_seconds <= 600:
        raise ValueError("source wait must be between 0 and 600 seconds")
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            # A failed attempt must release earlier roots before waiting, not
            # strand a reader and prevent the canonical writer from finishing.
            with ExitStack() as attempt:
                for root in roots:
                    attempt.enter_context(signal_engine.live_tw_public_source_read_guard(root))
                stack.enter_context(attempt.pop_all())
            return roots
        except TWPublicSourceUpdatePending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(.25, remaining))


def _candle_aux_forward(forward, *, materialize: bool):
    """Change diagnostic allocation only, retaining the full-aux math route."""
    def call(*args, **kwargs):
        return forward(*args, **{**kwargs, "materialize_candle_aux": materialize})
    return call


def _candle_aux_oracle(forward, evidence: list):
    """Full output oracle outside timing; only declared diagnostics may be None."""
    from scripts.benchmark_live_inference import (
        forward_tensor_inventory, require_equal_forward_outputs,
        require_equal_metadata_only_candle_outputs,
    )
    from stockagent.training.runtime import unwrap_model

    def call(model, *args, **kwargs):
        control = _candle_aux_forward(forward, materialize=True)(model, *args, **kwargs)
        candidate = _candle_aux_forward(forward, materialize=False)(model, *args, **kwargs)
        inventory = getattr(unwrap_model(model), "candle_diagnostic_aux_fields", None)
        fields = inventory() if inventory is not None else ()
        if fields:
            require_equal_metadata_only_candle_outputs(control, candidate, fields)
        else:
            require_equal_forward_outputs(control, candidate)
        evidence.append({"model_type": type(unwrap_model(model)).__name__,
                         "omitted_diagnostic_fields": list(fields),
                         "all_other_tensor_bytes_equal": True,
                         "control_tensor_inventory": forward_tensor_inventory(control),
                         "candidate_tensor_inventory": forward_tensor_inventory(candidate)})
        return candidate
    return call


def _full_inference_manifest(panel, config, *, checkpoint):
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest
    return build_checkpoint_manifest(panel, config, include_data_content=False)


def _model_contract_oracle(builder, evidence: list):
    """Verify the scoped builder against the complete canonical one off-clock."""
    from stockagent.training.checkpoint_contract import validate_checkpoint_manifest

    def build(panel, config, *, checkpoint):
        full = _full_inference_manifest(panel, config, checkpoint=checkpoint)
        candidate = builder(panel, config, checkpoint=checkpoint)
        for key in ("data_schema", "model"):
            if candidate["fingerprints"][key] != full["fingerprints"][key]:
                raise RuntimeError(f"model-scope {key} fingerprint differs from full builder")
        for key in ("schema_4_pre_external_feature_source",
                    "schema_4_pre_projection_l1_active_count_scale",
                    "schema_4_with_inactive_futures_fields",
                    "schema_4_with_inactive_causal_feature_fields"):
            relevant = full["compatibility_fingerprints"][key]
            if candidate.get("validation_scope") == "model":
                relevant = {name: value for name, value in relevant.items()
                            if name in {"data_schema", "model"}}
            if candidate["compatibility_fingerprints"][key] != relevant:
                raise RuntimeError(f"model-scope {key} compatibility differs from full builder")
        for manifest in (full, candidate):
            validate_checkpoint_manifest(checkpoint, manifest,
                                         checkpoint_path=Path("offline-oracle.pt"), scope="model")
        evidence.append({"model_name": candidate["contracts"]["model"]["model_name"],
                         "fingerprints": candidate["fingerprints"],
                         "current_and_compatible_fingerprints_equal": True,
                         "both_canonical_validations_passed": True})
        return candidate
    return build


def _publication_offset_ms(result, *, batch_started, call_started, prefetched):
    latency = result.summary["live_latency"]
    origin = call_started
    if latency.get("quote_prefetched_outside_model_lock") is True:
        if prefetched is None:
            raise RuntimeError("accepted quote prefetch has no measured origin")
        origin = prefetched.workflow_started
    return (origin - batch_started) * 1000 + latency.get(
        "input_to_execution_pointer_ms", latency["input_to_publish_ms"])


def _input_reference_evidence(result) -> dict:
    """New-standard model readiness, explicitly NOT executable fill coverage."""
    fields = ("symbol", "model_input_price_basis", "model_input_price", "model_input_price_is_carry",
              "model_input_price_source_date", "open_price", "opening_price_wait_reason")
    rows = [{key: row.get(key) for key in fields} for row in result.weights_rows]
    summary = result.summary
    return {"contract": "prepared_last_trade_no_first_print_model_reference_v1",
        "official_model_active_count": summary.get("opening_price_active_symbol_count"),
        "real_open_active_count": summary.get("opening_price_active_count"),
        "carried_reference_active_count": summary.get("opening_no_print_active_count"),
        "no_first_reference_unavailable_count": summary.get("opening_no_print_reference_unavailable_count"),
        "resolved_model_input_count": summary.get("opening_input_resolved_count"),
        "required_count": summary.get("opening_input_required_count"),
        "rows": rows, "boundary": "model_reference_not_trade_volume_or_execution"}


def _preopen_kwargs(kwargs, session):
    return dict(kwargs, price_source="panel", write=False, ensure_previous_signal=False,
                previous_signal_backfill_limit=0, asof_date=f"{session} 08:50:00")


def _receipt_universe(payload):
    """Keep the canonical registry checks on the replay's original clock."""
    from stockagent.data import tw_current_trading_universe as registry
    observed = datetime.fromisoformat(payload["captured_at_taipei"])
    if observed.tzinfo is None or observed.date().isoformat() != payload["session_date"]:
        raise ValueError("receipt registry clock/session mismatch")
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed.astimezone(tz or ZoneInfo("Asia/Taipei"))
    with patch.object(registry, "datetime", Clock):
        return registry.load_current_universe(payload["session_date"])


def _prefetch_with_stage_timing(kwargs, stages):
    """Trace the canonical progress boundaries, without changing its inputs."""
    clock, phase = time.perf_counter(), None
    def progress(event):
        nonlocal clock, phase
        current = str(event.get("message") or "")
        if current != phase:
            now = time.perf_counter()
            if phase is not None:
                stages[phase] = stages.get(phase, 0.) + (now - clock) * 1000
            clock, phase = now, current
    try:
        return signal_engine.prefetch_live_signal_prices(**dict(kwargs, progress_callback=progress))
    finally:
        progress({"message": "input_finished"})


def _write_benchmark_manifest(output: Path) -> Path:
    """Small, hash-bound index; never replace the complete per-symbol evidence.

    Keep first prepared inference distinct from repeats, and an executor
    rejection distinct from a fill. This index is outside all timed stages.
    """
    with output.open("rb") as stream:
        before = os.fstat(stream.fileno())
        raw = stream.read()
        digest = hashlib.sha256(raw).hexdigest()
        after = os.fstat(stream.fileno())
    def identity(st):
        return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns
    if identity(before) != identity(after) or identity(after) != identity(output.stat()):
        raise RuntimeError("benchmark evidence changed during manifest readback")
    payload = json.loads(raw)
    preparation = payload.get("preopen_preparation") or {}
    calls = preparation.get("reference_calls") or []
    inventories = preparation.get("reference_inventories") or []
    phase_calls = {}
    for phase in sorted({row["phase"] for row in calls}):
        rows = [row for row in calls if row["phase"] == phase]
        phase_calls[phase] = {"count": len(rows), "total_ms": sum(row["elapsed_ms"] for row in rows),
            "max_ms": max(row["elapsed_ms"] for row in rows),
            "same_reference_object_reused_count": sum(row["same_reference_object_reused"] is True for row in rows)}
    samples = []
    for sample in payload.get("samples") or []:
        entry = {key: sample.get(key) for key in ("variant", "sample_phase", "all_published_ms",
            "all_entry_probes_ms", "all_artifacts_ms", "entry_worker_prepare_ms_outside_warm_round")}
        entry["markets"] = [{"market": row["market"], "return_ms": row.get("return_ms"),
            "batch_publish_ms": row.get("batch_publish_ms"),
            "input_preparation_stages_ms": row.get("input_preparation_stages_ms"),
            "stages": {key: value for key, value in (row.get("stages") or {}).items()
                       if value is None or isinstance(value, (str, int, float, bool))},
            "input_reference_evidence": {key: value for key, value in
                (row.get("input_reference_evidence") or {}).items() if key != "rows"},
            "execution_probe": {key: (row.get("execution_probe") or {}).get(key) for key in
                ("scope", "status", "fixture_quote_count", "fixture_build_and_real_eligibility_ms",
                 "registration_and_ledger_persist_ms", "entry_fill_outcome", "filled_shares",
                 "requested_shares", "pending_source_count", "position_count", "production_ledger_modified")}}
            for row in sample.get("markets") or []]
        samples.append(entry)
    result = {"schema_version": 1,
        "source_artifact": {"path": str(output.resolve()), "bytes": after.st_size, "sha256": digest},
        "status": payload.get("status", "completed_offline_parity_run"),
        **{key: payload.get(key) for key in ("scope", "session", "feature_date", "input_observation_clock",
            "native_opening_to_completion_claim", "broker_fill_claim", "network_requests", "production_writes",
            "all_warmup_passes_ms", "measurement_summary", "contracts", "error_type", "error", "stage")},
        "preopen_preparation": {"panel_only": preparation.get("panel_only"),
            "opening_quotes_used_for_preparation": preparation.get("opening_quotes_used_for_preparation"),
            "reference_inventories": [{"prepared_during": row["prepared_during"],
                "source_cutoff": row["source_cutoff"], "symbol_count": len(row["symbols"]),
                "valid_reference_count": sum(value is not None and np.isfinite(value) and value > 0
                                             for value in row["prices"]),
                "source_dates": sorted({value for value in row["source_dates"] if value is not None})}
                for row in inventories], "reference_calls": phase_calls},
        "source_isolation": payload.get("source_isolation"), "samples": samples}
    manifest = output.with_name(output.stem + ".manifest.json")
    atomic_write_json(manifest, result)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--feature-date")
    parser.add_argument("--summarize-existing", type=Path,
                        help="Hash and index existing full benchmark evidence, without running models or changing it")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--defer-reports", action="store_true")
    parser.add_argument("--simulate-entry", action="store_true", help="Include isolated canonical ledger registration with explicit SYNTHETIC one-lot book fixtures; not real opening/fill evidence")
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--runtime-profile", type=Path,
                        help="Exact non-secret deployment runtime ABI file; never relaxes checkpoint validation")
    parser.add_argument("--preopen-panel-warmup", action="store_true",
                        help="Prepare only completed panels/models/references before the FIRST timed opening inference; no opening receipt warmup")
    parser.add_argument("--source-wait-seconds", type=float, default=0,
                        help="Bounded wait for the real source writer before preparation; never included as inference latency")
    parser.add_argument("--journal", type=Path, help="Optional original native acquisition journal for a private clock-pinned source projection")
    parser.add_argument("--journal-after-open-ms", type=int, help="Requires --journal; use only source facts originally observed by this deadline")
    parser.add_argument("--legacy-control", action="store_true", help="Restore redundant receipt/feature work for paired comparison")
    parser.add_argument("--legacy-validation-control", action="store_true", help="Resolve static bool annotations again on every config load")
    parser.add_argument("--compare-validation", action="store_true", help="Compare cached/uncached schema validation in balanced order in the same process")
    parser.add_argument("--compare-layout", action="store_true", help="Compare prewarmed C-order/K-order inputs in balanced order; model outputs must match")
    parser.add_argument("--compare-input-scheduling", action="store_true", help="Paired sequential/parallel read-only input scheduling, with serialized model compute and complete output parity")
    parser.add_argument("--parallel-inputs", action="store_true", help="Use production-style parallel read-only prefetch for both comparison variants; GPU execution remains serialized")
    parser.add_argument("--compare-publication", action="store_true", help="Paired compact/pretty complete JSON, with identical durable writes and full output parity")
    parser.add_argument("--compare-member-signatures", action="store_true", help="Paired original/current full member generation scans in the complete six-mode pipeline")
    parser.add_argument("--compare-candle-aux", action="store_true", help="Paired full/metadata-only unused candle diagnostics, with complete forward oracle outside timing")
    parser.add_argument("--compare-model-contract", action="store_true", help="Paired full/scoped canonical checkpoint contract construction; validation and signals must remain identical")
    parser.add_argument("--compare-prefetch-preparation", action="store_true", help="Paired unbounded/coordinated CPU metadata preparation; both retain parallel source waits and serialized model execution")
    parser.add_argument("--compare-source-scans", action="store_true", help="Paired fresh/overlapping-OS-lease stock member scans; requires parallel-inputs, preserves all sentinel checks")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.summarize_existing is not None:
        if args.output != args.summarize_existing:
            parser.error("summarize-existing output must name the unchanged full benchmark artifact")
        manifest = _write_benchmark_manifest(args.output)
        print(json.dumps({"manifest": str(manifest), "benchmark_not_rerun": True}))
        return
    if args.receipt is None or args.feature_date is None:
        parser.error("receipt and feature-date are required for a benchmark")
    runtime_profile = _benchmark_runtime_profile(args.runtime_profile)
    raw = args.receipt.read_bytes()
    source_payload = json.loads(raw)
    session = source_payload["session_date"]
    if bool(args.journal) != (args.journal_after_open_ms is not None):
        parser.error("journal and journal-after-open-ms must be supplied together")
    if args.journal is not None:
        from scripts.audit_tw_opening_symbol_status import journal_reference_receipt, journal_symbol_timeline
        timeline = journal_symbol_timeline(args.journal, source_payload, _receipt_universe(source_payload),
            cutoffs=(args.journal_after_open_ms,), include_reference_rows=True)
        source_payload = journal_reference_receipt(source_payload, timeline, args.journal_after_open_ms)
    source_rows = source_payload["rows"]
    if args.feature_date >= session or args.repeats < 1:
        parser.error("feature-date must precede receipt session; repeats must be positive")
    if args.compare_validation and args.legacy_validation_control:
        parser.error("compare-validation cannot be combined with legacy-validation-control")
    if args.compare_validation and args.compare_layout:
        parser.error("choose one paired comparison at a time")
    if args.compare_input_scheduling and (args.compare_validation or args.compare_layout or args.legacy_validation_control):
        parser.error("input scheduling is a separate paired comparison")
    if args.compare_input_scheduling and args.parallel_inputs:
        parser.error("parallel-inputs cannot change both sides of the input-scheduling comparison")
    if args.compare_publication and (args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.legacy_validation_control):
        parser.error("publication is a separate paired comparison")
    if args.compare_member_signatures and (args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.legacy_validation_control or args.legacy_control):
        parser.error("member generation scanning is a separate paired comparison")
    if args.compare_candle_aux and (args.compare_member_signatures or args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.legacy_validation_control or args.legacy_control):
        parser.error("candle diagnostics are a separate paired comparison")
    if args.compare_model_contract and (args.compare_candle_aux or args.compare_member_signatures or args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.legacy_validation_control or args.legacy_control):
        parser.error("model contract construction is a separate paired comparison")
    if args.compare_prefetch_preparation and (args.compare_model_contract or args.compare_candle_aux or args.compare_member_signatures or args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.legacy_validation_control or args.legacy_control):
        parser.error("CPU preparation is a separate paired comparison")
    if args.compare_source_scans and (not args.parallel_inputs or args.compare_prefetch_preparation or args.compare_model_contract or args.compare_candle_aux or args.compare_member_signatures or args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.legacy_validation_control or args.legacy_control):
        parser.error("source scans require parallel-inputs and a separate paired comparison")
    if args.preopen_panel_warmup and any((args.compare_layout, args.compare_model_contract, args.compare_candle_aux)):
        parser.error("preopen-only preparation cannot run an opening oracle before its first timed sample")
    observed = (datetime.fromisoformat(source_payload["captured_at_taipei"]) if args.preopen_panel_warmup
                else datetime.fromisoformat(session + "T09:00:00+08:00"))
    clock = [datetime.fromisoformat(session + "T08:50:00+08:00") if args.preopen_panel_warmup else observed]

    class ReceiptClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz or ZoneInfo("Asia/Taipei"))

    cfgs = [bot_mod._effective_market_config(bot_mod._resolve_market(m))
            for m in bot_mod._scheduled_markets()
            if bot_mod._resolve_market(m).day_trade_simulation_enabled]
    cfgs.sort(key=lambda cfg: (-bot_mod._preopen_market_symbol_count(cfg), cfg.market))
    _require_selected_sparse_runtime(cfgs)
    records, contracts, warmup, candle_oracle = [], {}, [], []
    model_contract_oracle = []
    canonical_window = signal_engine._cached_normalized_model_window
    contiguous_windows = {}
    legacy_windows = {}

    def contiguous_layout_window(panel, *, panel_idx, lookback):
        value = canonical_window(panel, panel_idx=panel_idx, lookback=lookback)
        key = (id(panel.features), panel_idx, lookback)
        previous = contiguous_windows.get(key)
        if previous is None or previous[0] is not value:
            previous = (value, np.ascontiguousarray(value))
            contiguous_windows[key] = previous
        return previous[1]

    normalized_window = contiguous_layout_window if args.compare_layout else canonical_window

    def legacy_layout_window(panel, *, panel_idx, lookback):
        # Values still come from the canonical normalizer. Only restore the
        # pre-fix K-order layout once; no input values or model work are omitted.
        value = normalized_window(panel, panel_idx=panel_idx, lookback=lookback)
        key = (id(panel.features), panel_idx, lookback)
        previous = legacy_windows.get(key)
        if previous is None or previous[0] is not panel.features:
            template = panel.features[panel_idx - lookback + 1:panel_idx + 1]
            old = np.empty_like(template, dtype=np.float32, order="K")
            np.copyto(old, value)
            previous = (panel.features, old)
            legacy_windows[key] = previous
        return previous[1]

    with tempfile.TemporaryDirectory(prefix="opening-pipeline-") as temporary, ExitStack() as stack:
        admission_started = time.perf_counter()
        source_roots = _hold_source_read_leases(stack, cfgs, wait_seconds=args.source_wait_seconds)
        source_admission_ms = (time.perf_counter() - admission_started) * 1000
        import requests
        stack.enter_context(patch.object(requests.sessions.Session, "request",
            side_effect=AssertionError("offline benchmark forbids network")))
        # The real engine is a separate process, not a thread sharing the
        # model owner's Python GIL. Never fork an initialized CUDA runtime.
        entry_worker = stack.enter_context(ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))) if args.simulate_entry else None
        stack.enter_context(patch.object(signal_engine, "_cached_normalized_model_window", normalized_window))
        root = Path(temporary)
        # Every date-sensitive source check uses the native receipt clock;
        # midnight during replay must not substitute another day's registry.
        from stockagent.data import tw_current_trading_universe as registry
        stack.enter_context(patch.object(registry, "datetime", ReceiptClock))
        stack.enter_context(patch.object(signal_engine, "datetime", ReceiptClock))
        specs = {}
        if args.simulate_entry:
            from scripts.run_tw_day_trade_simulation import _mode_specs
            loaded_specs, _, errors = _mode_specs(ROOT / "services/discord_bot/markets")
            if errors:
                raise RuntimeError(f"cannot resolve original execution contracts: {errors}")
            specs = {spec.market: spec for spec in loaded_specs}
        receipts = root / "receipts"
        receipts.mkdir()
        if args.journal is None:
            shutil.copyfile(args.receipt, receipts / f"{session}.json")
        else:
            atomic_write_json(receipts / f"{session}.json", source_payload)
        stack.enter_context(patch.dict(quote_provider.os.environ, {
            "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": str(receipts),
            "STOCKAGENT_BOT_PROGRESS": "0",
            "STOCKAGENT_LIVE_PANEL_CACHE_ROOT": str(root / "panels"),
        }))
        stack.enter_context(patch.object(quote_provider, "datetime", ReceiptClock))
        stack.enter_context(patch.object(opening_price_acquisition, "datetime", ReceiptClock))
        for module, name in (
            (signal_engine, "fetch_shared_day_trade_stock_snapshots"),
            (signal_engine, "fetch_shioaji_stock_snapshots"),
            (quote_provider, "fetch_tw_mis_last_prices"),
        ):
            stack.enter_context(patch.object(module, name, side_effect=AssertionError("offline benchmark forbids network")))
        kwargs_by_market = {}
        reference_inventories, reference_calls = {}, []
        reference_resolver = signal_engine._last_completed_trade_reference
        reference_phase = ["preopen" if args.preopen_panel_warmup else "legacy_opening_warmup"]

        def observed_references(panel, panel_idx):
            before = time.perf_counter()
            result = reference_resolver(panel, panel_idx)
            duration = (time.perf_counter() - before) * 1000
            key = (id(panel.close_prices), int(panel_idx))
            previous = reference_inventories.get(key)
            reused = previous is not None and previous[0] is result
            if previous is None:
                reference_inventories[key] = (result, {"symbols": list(map(str, panel.symbols)),
                    "source_cutoff": str(panel.dates[panel_idx]), "prepared_during": reference_phase[0],
                    "prices": [float(value) if np.isfinite(value) else None for value in result[0]],
                    "source_dates": [str(value) if not np.isnat(value) else None for value in result[1]]})
            reference_calls.append({"phase": reference_phase[0], "elapsed_ms": duration,
                                    "same_reference_object_reused": reused})
            return result

        stack.enter_context(patch.object(signal_engine, "_last_completed_trade_reference", observed_references))
        warmup_started = time.perf_counter()
        for cfg in cfgs:
            kwargs = cfg.signal_kwargs(
                write=False, price_source="tw", ensure_previous_signal=False,
                previous_signal_backfill_limit=0, panel_date=args.feature_date,
                asof_date=observed.strftime("%Y-%m-%d %H:%M:%S"), day_trade_model_observation="session_open",
            )
            step = time.perf_counter()
            try:
                result = signal_engine.generate_live_signal(**(_preopen_kwargs(kwargs, session) if args.preopen_panel_warmup else kwargs))
            except Exception as exc:
                atomic_write_json(args.output, {"scope": "offline_receipt_replay_failed_not_latency_success",
                    "status": "failed", "stage": "fresh_process_warmup", "market": cfg.market,
                    "failed_stage_elapsed_ms": round((time.perf_counter() - step) * 1000, 3),
                    "error_type": type(exc).__name__, "error": str(exc), "runtime_profile": runtime_profile,
                    "warmup_samples": warmup, "contracts": contracts,
                    "receipt_sha256": hashlib.sha256(raw).hexdigest(), "session": session,
                    "network_requests": 0, "production_writes": False, "broker_fill_claim": False})
                raise
            warmup.append({"market": cfg.market, "phase": "preopen_panel_only_first_use" if args.preopen_panel_warmup else "fresh_process_first_use",
                           "elapsed_ms": (time.perf_counter() - step) * 1000,
                           "stages": result.summary["live_latency"]})
            kwargs_by_market[cfg.market] = kwargs
            if not args.preopen_panel_warmup:
                contracts[cfg.market] = _contract_digest(result)
        # One further complete pass makes cross-model cache capacity visible.
        for kwargs in (() if args.preopen_panel_warmup else kwargs_by_market.values()):
            step = time.perf_counter()
            result = signal_engine.generate_live_signal(**kwargs)
            if _contract_digest(result) != contracts[result.summary["market"]]:
                raise RuntimeError("second warmup pass changed the canonical model contract")
            warmup.append({"market": result.summary["market"], "phase": "second_complete_pass",
                           "elapsed_ms": (time.perf_counter() - step) * 1000,
                           "stages": result.summary["live_latency"]})
        warmup_ms = (time.perf_counter() - warmup_started) * 1000
        clock[0] = observed
        reference_phase[0] = "opening_after_preparation" if args.preopen_panel_warmup else "opening_after_legacy_warmup"
        if args.compare_layout:
            with patch.object(signal_engine, "_cached_normalized_model_window", legacy_layout_window):
                for kwargs in kwargs_by_market.values():
                    result = signal_engine.generate_live_signal(**kwargs)
                    if _contract_digest(result) != contracts[result.summary["market"]]:
                        raise RuntimeError("memory layout changed the canonical model contract")
        stack.enter_context(patch.object(signal_engine, "_cached_normalized_model_window", normalized_window))
        schema_resolver = config_module._strict_bool_config_fields
        stack.enter_context(patch.object(config_module, "_strict_bool_config_fields", schema_resolver))
        canonical_json_writer = signal_engine._atomic_write_json
        stack.enter_context(patch.object(signal_engine, "_atomic_write_json", canonical_json_writer))
        canonical_signatures = signal_engine._factorized_member_signatures
        stack.enter_context(patch.object(signal_engine, "_factorized_member_signatures", canonical_signatures))
        canonical_model_call = signal_engine.call_model
        stack.enter_context(patch.object(signal_engine, "call_model", canonical_model_call))
        canonical_manifest_builder = signal_engine.build_inference_checkpoint_manifest
        stack.enter_context(patch.object(signal_engine, "build_inference_checkpoint_manifest", canonical_manifest_builder))
        canonical_guard = signal_engine.live_tw_public_source_read_guard
        stack.enter_context(patch.object(signal_engine, "live_tw_public_source_read_guard", canonical_guard))

        def unshared_guard(root, **options):
            return canonical_guard(root, **dict(options, share_generation=False))

        from stockagent.live import completed_inference_cache
        canonical_scan = completed_inference_cache._parquet_source_generations
        scan_samples = []

        def observed_scan(*paths, **options):
            before = time.perf_counter()
            result = canonical_scan(*paths, **options)
            scan_samples.append({"feature_scan": bool(options.get("feature_roots")),
                "member_count": len(result), "elapsed_ms": (time.perf_counter() - before) * 1000})
            return result

        if args.compare_source_scans:
            stack.enter_context(patch.object(completed_inference_cache, "_parquet_source_generations", observed_scan))
        # This is an experimental comparison, not the production default.
        preparation_slots = (threading.BoundedSemaphore(2) if args.compare_prefetch_preparation
                             else signal_engine._LIVE_QUOTE_PREPARATION_SLOTS)
        stack.enter_context(patch.object(signal_engine, "_LIVE_QUOTE_PREPARATION_SLOTS", preparation_slots))
        if args.compare_model_contract:
            with patch.object(signal_engine, "build_inference_checkpoint_manifest",
                              _model_contract_oracle(canonical_manifest_builder, model_contract_oracle)):
                for cfg in cfgs:
                    before = len(model_contract_oracle)
                    result = signal_engine.generate_live_signal(**kwargs_by_market[cfg.market])
                    if _contract_digest(result) != contracts[cfg.market] or before == len(model_contract_oracle):
                        raise RuntimeError("model contract oracle did not preserve the complete signal")
                    for entry in model_contract_oracle[before:]:
                        entry["market"] = cfg.market
        if args.compare_candle_aux:
            with patch.object(signal_engine, "call_model", _candle_aux_oracle(canonical_model_call, candle_oracle)):
                for cfg in cfgs:
                    before = len(candle_oracle)
                    result = signal_engine.generate_live_signal(**kwargs_by_market[cfg.market])
                    if _contract_digest(result) != contracts[cfg.market] or before == len(candle_oracle):
                        raise RuntimeError("candle oracle did not preserve the complete signal contract")
                    for entry in candle_oracle[before:]:
                        entry["market"] = cfg.market
        if args.compare_member_signatures:
            from scripts.benchmark_opening_preparation import _legacy_factorized_member_signatures

        def pretty_json_writer(path, payload):
            # Isolate encoding/whitespace cost, never skipped fsync/artifacts.
            signal_engine._atomic_publish_json(path, payload, allow_nan=True, indent=2)
        if args.legacy_control:
            fetch = signal_engine.fetch_tw_mis_opening_snapshot
            feature_summary = signal_engine._feature_driver_summary

            def reseed(*a, **kw):
                snapshot = fetch(*a, **kw)
                signal_engine.seed_tw_opening_snapshot_cache(a[0], snapshot, parquet_root=kw["parquet_root"])
                return snapshot

            def duplicate_features(*a, **kw):
                feature_summary(*a, top_n=8)
                return feature_summary(*a, **kw)

            stack.enter_context(patch.object(signal_engine, "fetch_tw_mis_opening_snapshot", reseed))
            stack.enter_context(patch.object(signal_engine, "_feature_driver_summary", duplicate_features))
            class PythonSafeLoader(yaml.SafeLoader):
                pass

            PythonSafeLoader.add_constructor(
                yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                config_module._construct_unique_mapping,
            )

            def scalar_numpy_finite(value):
                try:
                    number = float(value)
                except Exception:
                    return None
                return number if np.isfinite(number) else None

            stack.enter_context(patch.object(config_module, "_UniqueKeySafeLoader", PythonSafeLoader))
            stack.enter_context(patch.object(signal_engine, "_finite_float_or_none", scalar_numpy_finite))
        profiler = cProfile.Profile() if args.profile else None
        if profiler:
            profiler.enable()
        variants = (False, True) if args.compare_validation or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.compare_member_signatures or args.compare_candle_aux or args.compare_model_contract or args.compare_prefetch_preparation or args.compare_source_scans else (not args.legacy_validation_control,)
        repetitions = [(repetition, use_cached) for repetition in range(args.repeats)
                       for use_cached in (variants if repetition % 2 == 0 else tuple(reversed(variants)))]
        for repetition, use_cached in repetitions:
            config_module._strict_bool_config_fields = schema_resolver if use_cached or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.compare_member_signatures or args.compare_candle_aux or args.compare_model_contract or args.compare_prefetch_preparation or args.compare_source_scans else schema_resolver.__wrapped__
            signal_engine._cached_normalized_model_window = normalized_window if use_cached or not args.compare_layout else legacy_layout_window
            signal_engine._atomic_write_json = pretty_json_writer if args.compare_publication and not use_cached else canonical_json_writer
            signal_engine._factorized_member_signatures = (_legacy_factorized_member_signatures
                if args.compare_member_signatures and not use_cached else canonical_signatures)
            if args.compare_candle_aux:
                signal_engine.call_model = _candle_aux_forward(canonical_model_call, materialize=not use_cached)
            if args.compare_model_contract:
                signal_engine.build_inference_checkpoint_manifest = canonical_manifest_builder if use_cached else _full_inference_manifest
            if args.compare_prefetch_preparation:
                signal_engine._LIVE_QUOTE_PREPARATION_SLOTS = preparation_slots if use_cached else nullcontext()
            if args.compare_source_scans:
                signal_engine.live_tw_public_source_read_guard = canonical_guard if use_cached else unshared_guard
            scan_start = len(scan_samples)
            rows, results, entry_jobs = [], [], []
            entry_prepare_ms = None
            if args.simulate_entry:
                prepare_started = time.perf_counter()
                entry_worker.submit(_prepare_entry_worker, root / f"ledger-{repetition}-{int(use_cached)}", source_rows).result()
                entry_prepare_ms = (time.perf_counter() - prepare_started) * 1000
            started = time.perf_counter()
            prefetch_pool = None
            prefetched_by_market = {}
            prefetch_stages_by_market = {cfg.market: {} for cfg in cfgs}
            execution_order = cfgs
            if args.parallel_inputs or args.compare_prefetch_preparation or (args.compare_input_scheduling and use_cached):
                prefetch_pool = stack.enter_context(ThreadPoolExecutor(max_workers=len(cfgs)))
                prefetched_by_market = {
                    cfg.market: prefetch_pool.submit(_prefetch_with_stage_timing,
                        kwargs_by_market[cfg.market], prefetch_stages_by_market[cfg.market])
                    for cfg in cfgs
                }
                by_future = {prefetched_by_market[cfg.market]: cfg for cfg in cfgs}
                execution_order = (by_future[future] for future in as_completed(by_future))
            for cfg in execution_order:
                kwargs = dict(kwargs_by_market[cfg.market], write=True,
                              live_output_dir=root / f"{repetition}-{int(use_cached)}" / cfg.market)
                if args.defer_reports:
                    kwargs["defer_rich_artifacts"] = True
                step = time.perf_counter()
                try:
                    if cfg.market in prefetched_by_market:
                        kwargs["_prefetched_quote"] = prefetched_by_market[cfg.market].result()
                    result = signal_engine.generate_live_signal(**kwargs)
                except Exception as exc:
                    atomic_write_json(args.output, {
                        "scope": "offline_receipt_replay_failed_not_latency_success", "status": "failed",
                        "stage": "opening_inference", "market": cfg.market,
                        "error_type": type(exc).__name__, "error": str(exc),
                        "failed_batch_elapsed_ms": (time.perf_counter() - started) * 1000,
                        "sample_phase": "first_opening_after_panel_only_preparation" if not records and args.preopen_panel_warmup else "steady_opening_repeat",
                        "input_observation_clock": observed.isoformat(),
                        "private_source_projection": source_payload.get("diagnostic_projection"),
                        "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                        "runtime_profile": runtime_profile, "warmup_samples": warmup,
                        "all_warmup_passes_ms": warmup_ms,
                        "preopen_preparation": {"panel_only": args.preopen_panel_warmup,
                            "opening_quotes_used_for_preparation": not args.preopen_panel_warmup,
                            "reference_inventories": [entry[1] for entry in reference_inventories.values()],
                            "reference_calls": reference_calls},
                        "samples": records, "partial_batch_markets": rows,
                        "network_requests": 0, "production_writes": False, "broker_fill_claim": False,
                    })
                    raise
                publish_ms = _publication_offset_ms(result, batch_started=started, call_started=step,
                                                    prefetched=kwargs.get("_prefetched_quote"))
                rows.append({"market": cfg.market, "return_ms": (time.perf_counter() - step) * 1000,
                             "batch_publish_ms": publish_ms,
                             "input_preparation_stages_ms": {phase: round(ms, 3) for phase, ms in
                                                             prefetch_stages_by_market[cfg.market].items()},
                             "stages": result.summary["live_latency"],
                             "input_reference_evidence": _input_reference_evidence(result)})
                results.append(result)
                if entry_worker is not None:
                    # Mirror the real independent model and single ledger
                    # owners: model N+1 does not wait for N's ledger fsync.
                    entry_jobs.append((rows[-1], entry_worker.submit(
                        _timed_paper_entry_probe, started,
                        replace(specs[cfg.market], live_output_dir=Path(kwargs["live_output_dir"])),
                        dict(result.summary), result.weights_rows, observed,
                    )))
            published_ms = max(row["batch_publish_ms"] for row in rows)
            for row, future in entry_jobs:
                row["execution_probe"], row["batch_entry_probe_complete_ms"] = future.result()
            if args.defer_reports:
                for result in results:
                    signal_engine.finalize_live_signal_artifacts(result)
            artifacts_ms = (time.perf_counter() - started) * 1000
            for result in results:
                contracts.setdefault(result.summary["market"], _contract_digest(result))
                if _contract_digest(result) != contracts[result.summary["market"]]:
                    raise RuntimeError(f"{result.summary['market']}: model/price/explanation contract changed")
                directory = Path(result.output_dir)
                if not (directory / "target_weights.parquet").is_file():
                    raise RuntimeError("rich artifacts incomplete")
                pointer = json.loads((directory.parents[1] / "latest_signal.json").read_text())
                if not pointer["artifact_complete"]:
                    raise RuntimeError("completion pointer incomplete")
            if prefetch_pool is not None:
                prefetch_pool.shutdown(wait=True)
            variant = ("coordinated_cpu_preparation" if use_cached else "unbounded_cpu_preparation") if args.compare_prefetch_preparation else ("model_scope_contract" if use_cached else "full_resume_contract") if args.compare_model_contract else ("metadata_only_diagnostics" if use_cached else "full_diagnostics") if args.compare_candle_aux else ("per_scan_parent_resolve" if use_cached else "per_member_resolve") if args.compare_member_signatures else ("compact_durable_json" if use_cached else "pretty_durable_json") if args.compare_publication else ("parallel_inputs" if use_cached else "sequential_inputs") if args.compare_input_scheduling else ("C_order" if use_cached else "K_order") if args.compare_layout else ("cached" if use_cached else "uncached")
            if args.compare_source_scans:
                variant = "shared_read_generation_scans" if use_cached else "independent_source_scans"
            records.append({"variant": variant,
                            "sample_phase": "first_opening_after_panel_only_preparation" if args.preopen_panel_warmup and not records else "steady_opening_repeat",
                            "schema_validation_cached": use_cached or args.compare_layout or args.compare_input_scheduling or args.compare_publication or args.compare_member_signatures or args.compare_candle_aux or args.compare_model_contract or args.compare_prefetch_preparation or args.compare_source_scans,
                            "source_scan_stages": scan_samples[scan_start:],
                            "prewarmed_c_layout": use_cached if args.compare_layout else None,
                            "all_published_ms": published_ms,
                            "all_entry_probes_ms": max((row["batch_entry_probe_complete_ms"] for row in rows), default=None) if entry_jobs else None,
                            "entry_worker_prepare_ms_outside_warm_round": entry_prepare_ms,
                            "all_artifacts_ms": artifacts_ms,
                            "markets": rows})
        if profiler:
            profiler.disable()
            args.profile.parent.mkdir(parents=True, exist_ok=True)
            profiler.dump_stats(args.profile)
        payload = {"scope": "offline_warm_receipt_replay_not_live_opening_latency",
                   "network_requests": 0, "production_writes": False,
                   "source_isolation": {"policy": "canonical_shared_read_lease_for_complete_paired_run_v1",
                                        "parquet_roots": source_roots,
                                        "source_admission_ms_outside_preparation_and_samples": source_admission_ms,
                                        "may_defer_source_writer": True,
                                        "source_lock_contention_in_timed_samples": False},
                   "receipt_sha256": hashlib.sha256(raw).hexdigest(),
                   "runtime_profile": runtime_profile,
                   "input_observation_clock": observed.isoformat(),
                   "native_opening_to_completion_claim": False,
                   "private_source_projection": source_payload.get("diagnostic_projection"),
                   "preopen_preparation": {"panel_only": args.preopen_panel_warmup,
                       "opening_quotes_used_for_preparation": not args.preopen_panel_warmup,
                       "reference_inventories": [entry[1] for entry in reference_inventories.values()],
                       "reference_calls": reference_calls},
                   "session": session, "feature_date": args.feature_date,
                   "deferred_reports": args.defer_reports, "contracts": contracts,
                   "synthetic_entry_book_fixture": args.simulate_entry,
                   "execution_owner_layout": "independent_model_and_single_ledger_process" if args.simulate_entry else "model_only",
                   "input_scheduling": "parallel_readonly_inputs_serial_model_owner" if args.parallel_inputs or args.compare_prefetch_preparation else "paired_sequential_parallel" if args.compare_input_scheduling else "sequential_inputs",
                   "broker_fill_claim": False,
                   "legacy_redundant_work_control": args.legacy_control,
                   "legacy_validation_control": args.legacy_validation_control,
                   "comparison": "stock_member_scans_only_with_overlapping_canonical_OS_read_leases" if args.compare_source_scans else "cpu_metadata_preparation_not_source_concurrency" if args.compare_prefetch_preparation else "canonical_model_scope_contract_construction" if args.compare_model_contract else "unused_candle_diagnostic_materialization" if args.compare_candle_aux else "full_member_generation_scan" if args.compare_member_signatures else "compact_vs_pretty_same_durable_publication" if args.compare_publication else "parallel_readonly_inputs_serial_model_owner" if args.compare_input_scheduling else "prewarmed_memory_layout" if args.compare_layout else "static_schema_validation",
                   "candle_aux_oracle_outside_timing": candle_oracle,
                   "model_contract_oracle_outside_timing": model_contract_oracle,
                   "all_warmup_passes_ms": warmup_ms, "warmup_samples": warmup,
                   "variant_median_ms": {
                       variant: statistics.median(row["all_published_ms"] for row in records if row["variant"] == variant)
                       for variant in sorted({row["variant"] for row in records})},
                   "median_all_published_ms": statistics.median(r["all_published_ms"] for r in records),
                   "measurement_summary": _measurement_summary(records),
                   "samples": records}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(args.output, payload)
        manifest = _write_benchmark_manifest(args.output)
        print(json.dumps({"output": str(args.output), "manifest": str(manifest),
                          "measurement_summary": payload["measurement_summary"]}, ensure_ascii=False, indent=2))


def _isolated_paper_entry_probe(engine, spec, result, observed, source_rows):
    """Full weights + sizing/fees/budget/append/persist, never a broker fill.

    Each positive observed open gets an explicitly synthetic ONE-LOT book at
    that legal price. Missing opens stay missing; partial orders remain partial.
    This measures software processing, not exchange depth or a source SLA.
    """
    from stockagent.live.tw_day_trade_simulation import load_live_eligibility, resolve_day_trade_rule_data_dir
    started = time.perf_counter()
    rule_root = resolve_day_trade_rule_data_dir(None, parquet_root=spec.parquet_root, repo_root=ROOT)
    eligibility, coverage = load_live_eligibility(rule_data_dir=rule_root, parquet_root=spec.parquet_root, symbols=[str(row["symbol"]) for row in result.weights_rows], trading_date=observed.date(), require_latest=False)
    quotes_by_symbol = {}
    for row in result.weights_rows:
        opening = row.get("open_price")
        source_row = source_rows.get(str(row["symbol"]), {})
        upper, lower = source_row.get("upper_limit_prices"), source_row.get("lower_limit_prices")
        if opening is None or upper is None or lower is None:
            continue
        quotes_by_symbol[str(row["symbol"])] = {"open": opening, "last": opening, "bid": opening, "ask": opening, "bid_volume": 1.0, "ask_volume": 1.0, "upper_limit": upper, "lower_limit": lower, "simtrade": False, "source": "engineering_fixture:one_lot_book_not_market_observation", "quote_at": (observed + timedelta(milliseconds=2)).isoformat()}
    fixture_build_ms = (time.perf_counter() - started) * 1000
    register_started = time.perf_counter()
    status = engine.register_signal(spec=spec, summary=result.summary, signal_rows=result.weights_rows, quotes=quotes_by_symbol, eligibility=eligibility, eligibility_coverage=coverage, now=observed + timedelta(milliseconds=3))
    register_ms = (time.perf_counter() - register_started) * 1000
    mode = engine.state["modes"][spec.market]
    return {"scope": "isolated_engineering_synthetic_one_lot_books_not_live_fill", "status": status, "fixture_quote_count": len(quotes_by_symbol), "fixture_build_and_real_eligibility_ms": round(fixture_build_ms, 3), "registration_and_ledger_persist_ms": round(register_ms, 3), "entry_fill_outcome": mode.get("entry_fill_outcome"), "filled_shares": mode.get("entry_filled_shares"), "requested_shares": mode.get("entry_requested_shares"), "pending_source_count": mode.get("pending_entry_source_count"), "position_count": len(mode.get("positions") or {}), "production_ledger_modified": False}


def _measurement_summary(records) -> dict:
    """Retain every sample; separate cache rebuilds from fully warmed paths.

    Publication latency and rich-artifact completion are different boundaries.
    Warm-only statistics are diagnostic, never substitutes for all-sample data.
    """
    def is_warm(row):
        markets = row.get("markets") or []
        return bool(markets) and all(
            market.get("stages", {}).get(key) is True
            for market in markets
            for key in ("panel_cache_hit", "checkpoint_cache_hit",
                        "model_cache_hit", "alignment_cache_hit")
        )

    def durations(rows):
        return {
            "sample_count": len(rows),
            "median_all_published_ms": statistics.median(
                row["all_published_ms"] for row in rows) if rows else None,
            "median_all_artifacts_ms": statistics.median(
                row["all_artifacts_ms"] for row in rows) if rows else None,
            "min_all_published_ms": min(
                (row["all_published_ms"] for row in rows), default=None),
            "max_all_published_ms": max(
                (row["all_published_ms"] for row in rows), default=None),
            "min_all_artifacts_ms": min(
                (row["all_artifacts_ms"] for row in rows), default=None),
            "max_all_artifacts_ms": max(
                (row["all_artifacts_ms"] for row in rows), default=None),
        }

    variants = {}
    for variant in sorted({row["variant"] for row in records}):
        rows = [row for row in records if row["variant"] == variant]
        warm = [row for row in rows if is_warm(row)]
        prefetch = [market.get("stages", {}).get("quote_prefetched_outside_model_lock")
                    for row in rows for market in row.get("markets", [])]
        variants[variant] = {
            "all_samples": durations(rows),
            "fully_warm_samples": durations(warm),
            "first_opening_after_preparation": durations([row for row in rows if
                row.get("sample_phase") == "first_opening_after_panel_only_preparation"]),
            "steady_opening_repeats": durations([row for row in rows if
                row.get("sample_phase") == "steady_opening_repeat"]),
            "not_fully_warm_sample_count": len(rows) - len(warm),
            "prefetch_admission": {
                "adopted_model_count": sum(value is True for value in prefetch),
                "fallback_model_count": sum(value is False for value in prefetch),
                "unknown_model_count": sum(value is not True and value is not False
                                           for value in prefetch),
            },
        }
    return {"all_samples_retained": True, "variants": variants}


def _contract_digest(result) -> str:
    payload = {"weights": result.weights_rows, "decisions": result.decision_rows,
               "explanation": result.summary["model_explanation"]}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


_ENTRY_ENGINE = None
_ENTRY_SOURCE_ROWS = None


def _prepare_entry_worker(path, source_rows):
    global _ENTRY_ENGINE, _ENTRY_SOURCE_ROWS
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
    _ENTRY_ENGINE = TwDayTradeSimulationEngine(path)
    _ENTRY_SOURCE_ROWS = source_rows


def _timed_paper_entry_probe(started, spec, summary, weights_rows, observed):
    if _ENTRY_ENGINE is None:
        raise RuntimeError("isolated ledger owner was not prepared")
    result = _isolated_paper_entry_probe(_ENTRY_ENGINE, spec, SimpleNamespace(summary=summary, weights_rows=weights_rows), observed, _ENTRY_SOURCE_ROWS)
    return result, (time.perf_counter() - started) * 1000


if __name__ == "__main__":
    main()
