"""Measure complete paper-state publication on pinned data, never live ledgers.

Both variants use the current canonical writer and every durability barrier.
Compare stdlib versus guarded-native PRIVATE UI fingerprint encoding, not a
whitespace patch that the current ledger encoder no longer consumes. Validate
physical projection/journal generations and the post-fsync ACK before excluding
those identities from a complete decoded-state comparison. Retain all samples.
"""
from __future__ import annotations

import argparse
import cProfile
import copy
from contextlib import nullcontext
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live import tw_day_trade_simulation as simulation
from stockagent.private_json import loads
from stockagent import private_json
from stockagent.live.tw_day_trade_monitor_projection import validated_shioaji_monitor_projection
from stockagent.live.paper_mode_snapshot import read_paper_state, mode_object_generations
from stockagent.live.tw_day_trade_service_sync import (
    committed_paper_read_guard, load_service_sync,
    paper_journal_generations, paper_projection_generations,
)


def _publication_contract(engine, *, expected_fingerprint: str) -> str:
    with committed_paper_read_guard(engine.state_dir):
        payloads = {name: read_paper_state(engine.state_dir / name)
                    for name in ("state.json", "positions.json", "status.json", "service_sync.json")}
        receipt = load_service_sync(engine.state_dir)
        if receipt is None or receipt != payloads["service_sync.json"]:
            raise RuntimeError("publication lacks its post-fsync acknowledgement")
        if (receipt.get("projection_generations") != paper_projection_generations(engine.state_dir)
                or receipt.get("journal_generations") != paper_journal_generations(engine.state_dir)):
            raise RuntimeError("publication generations do not bind the physical files")
        receipt = payloads["service_sync.json"]
    status = payloads["status.json"]
    projection = validated_shioaji_monitor_projection(status, engine.state_path)
    if projection is None:
        raise RuntimeError("monitor projection is not bound to the committed state")
    for payload in payloads.values():
        if payload["state_revision"] != engine.state["state_revision"]:
            raise RuntimeError("publication revisions diverge")
        if payload["simulation_only"] is not True or payload["production_order_possible"] is not False:
            raise RuntimeError("publication violated the paper-only contract")
    # The physical file's inode/byte count/clocks necessarily differ. Validate
    # them above rather than ignoring a potentially stale monitor projection.
    del projection["state_signature"]
    # The codec changes this PRIVATE invalidation digest, not financial values.
    # Check the actual writer result first. The independently computed logical
    # input digest below must also agree across all variants and samples.
    if payloads["state.json"].get("dashboard_content_fingerprint") != expected_fingerprint:
        raise RuntimeError("published material fingerprint differs from the measured writer")
    del payloads["state.json"]["dashboard_content_fingerprint"]
    del receipt["projection_generations"]
    del receipt["journal_generations"]
    del receipt["durability_ack"]["commit_id"]
    if "mode_object_generations" in receipt:
        if receipt["mode_object_generations"] != mode_object_generations(engine.state_dir):
            raise RuntimeError("mode objects are not bound to the commit")
        del receipt["mode_object_generations"]
        del receipt["state_storage_contract"]
    prefix = str(engine.state_dir.resolve())
    def normalized(value):
        if isinstance(value, dict):
            return {key: normalized(item) for key, item in value.items()}
        if isinstance(value, list):
            return [normalized(item) for item in value]
        if isinstance(value, str) and (value == prefix or value.startswith(prefix + os.sep)):
            return "<isolated-paper-engine-root>" + value[len(prefix):]
        return value
    return hashlib.sha256(json.dumps(normalized(payloads), ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _stdlib_fingerprint(payload) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
        default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def _require_distinct_output(source: Path, output: Path) -> None:
    if source.resolve() == output.resolve() or (output.exists() and source.samefile(output)):
        raise ValueError("benchmark output must not overwrite its pinned source")


def benchmark(state_path: Path, *, repeats: int, output: Path, profile: bool = False,
              key_guard: bool = False, ledger_codec: bool = False) -> dict:
    if repeats < 1:
        raise ValueError("repeats must be positive")
    _require_distinct_output(state_path, output)
    raw = state_path.read_bytes()
    pinned = json.loads(raw)
    if pinned.get("simulation_only") is not True or pinned.get("production_order_possible") is not False:
        raise ValueError("only a paper simulation state can be benchmarked")
    clock = datetime.fromisoformat(pinned["updated_at"])
    output.parent.mkdir(parents=True, exist_ok=True)
    samples = []
    expected_contract = None
    expected_logical_input = None
    candidate_fingerprint = simulation._dashboard_material_fingerprint
    variants = {"stdlib_fingerprint": _stdlib_fingerprint,
                "guarded_native_fingerprint": candidate_fingerprint}
    legacy_keys = lambda value: all(type(key) is str for key in value)
    key_predicates = {"legacy_python_key_generator": legacy_keys,
                      "exact_type_identity_key_guard": private_json._plain_string_keys}
    if key_guard:
        variants = dict.fromkeys(key_predicates, candidate_fingerprint)
    if ledger_codec:
        if key_guard or private_json._native is None:
            raise ValueError("ledger codec comparison requires its own available native reference")
        variants = dict.fromkeys(("guarded_native_ledger", "stdlib_ledger"), candidate_fingerprint)
    native = private_json._native

    # Keep the same filesystem as the requested output. Do not use a ramdisk
    # and describe it as the live durable-publication cost.
    with tempfile.TemporaryDirectory(prefix="opening-persist-", dir=output.parent) as temporary:
        engine = simulation.TwDayTradeSimulationEngine(Path(temporary), publication_clock=lambda: clock)
        engine._engine_run_id = "pinned-offline-publication-benchmark"
        for repeat in range(repeats):
            order = tuple(variants) if repeat % 2 == 0 else tuple(reversed(variants))
            for variant in order:
                engine.state = copy.deepcopy(pinned)
                # Invalidate only the ephemeral UI digest, so both codecs must
                # publish exactly one material revision from the same state.
                # A v1/v2/native-availability transition may otherwise make one
                # variant a cache hit; no source, money or target is modified.
                engine.state["dashboard_content_fingerprint"] = ""
                fingerprint_calls = []

                def measured_fingerprint(value):
                    digest = variants[variant](value)
                    fingerprint_calls.append((value, digest))
                    return digest

                profiler = cProfile.Profile() if profile else None
                if profiler:
                    profiler.enable()
                cpu_started = time.thread_time()
                started = time.perf_counter()
                with (patch.object(simulation, "_dashboard_material_fingerprint", measured_fingerprint),
                      patch.object(private_json, "_plain_string_keys", key_predicates[variant]) if key_guard else nullcontext(),
                      patch.object(private_json, "_native", None if variant == "stdlib_ledger" else native)
                      if ledger_codec else nullcontext()):
                    engine._persist(clock)
                elapsed_ms = (time.perf_counter() - started) * 1000
                owner_cpu_ms = (time.thread_time() - cpu_started) * 1000
                if profiler:
                    profiler.disable()
                    profiler.dump_stats(str(output.with_suffix(f".{variant}.{repeat}.prof")))
                if len(fingerprint_calls) != 1:
                    raise RuntimeError("benchmark did not exercise exactly one material fingerprint")
                logical_input = _stdlib_fingerprint(fingerprint_calls[0][0])
                if expected_logical_input is None:
                    expected_logical_input = logical_input
                if logical_input != expected_logical_input:
                    raise RuntimeError("complete material fingerprint input changed between variants")
                contract = _publication_contract(engine, expected_fingerprint=fingerprint_calls[0][1])
                if expected_contract is None:
                    expected_contract = contract
                if contract != expected_contract:
                    raise RuntimeError("complete publication values changed between variants")
                if any(path.exists() for path in (engine.signals_path, engine.orders_path, engine.fills_path)):
                    raise RuntimeError("persistence benchmark unexpectedly wrote an execution ledger")
                writes = [dict(item, bytes=(engine.state_dir / item["file"]).stat().st_size)
                          for item in engine.last_persist_metrics["files"]]
                samples.append({"repeat": repeat, "variant": variant, "elapsed_ms": elapsed_ms,
                                "owner_cpu_ms": owner_cpu_ms,
                                "writes": writes, "state_publication": engine.last_persist_metrics,
                                "logical_fingerprint_input_sha256": logical_input,
                                "material_fingerprint": fingerprint_calls[0][1],
                                "contract_sha256": contract})
    payload = {
        "scope": "isolated_full_state_publication_not_live_opening_latency",
        "production_writes": False, "network_requests": 0, "ledger_writes": 0,
        "state_source": str(state_path.resolve()), "state_source_sha256": hashlib.sha256(raw).hexdigest(),
        "state_source_bytes": len(raw), "profiled": profile,
        "file_fsync_preserved": True, "directory_fsync_preserved": True,
        "full_publication_values_identical": True, "monitor_binding_verified_each_sample": True,
        "projection_and_journal_generations_verified_each_sample": True,
        "post_fsync_ack_verified_each_sample": True,
        "comparison": ("full_publication_guarded_native_vs_stdlib_ledger_v1" if ledger_codec
                       else "exact_ledger_key_guard_complete_publication_v1" if key_guard
                       else "private_ui_fingerprint_stdlib_vs_guarded_native_v2"),
        "key_guard_reference": "all(type(key) is str for key in mapping) at the same helper-call boundary" if key_guard else None,
        "ephemeral_digest_invalidated_for_equal_work": True,
        "logical_fingerprint_input_sha256": expected_logical_input,
        "contract_sha256": expected_contract,
        "variant_median_ms": {variant: statistics.median(row["elapsed_ms"] for row in samples
                                                         if row["variant"] == variant)
                              for variant in variants},
        "variant_median_owner_cpu_ms": {variant: statistics.median(row["owner_cpu_ms"] for row in samples
                                                                   if row["variant"] == variant) for variant in variants},
        "cpu_clock_scope": "current_owner_thread_not_filesystem_wait_or_other_workers",
        "samples": samples,
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _pinned_journal_bytes(journal_path: Path) -> tuple[bytes, tuple[int, ...]]:
    """Never block on a replaced FIFO or follow a substituted source symlink."""
    signature = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns)
    descriptor = os.open(journal_path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
                         | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(descriptor, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > 64 * 1024**2:
            raise ValueError("bounded regular journal source required")
        raw = handle.read(before.st_size + 1)
        captured = signature(before)
        if (signature(os.fstat(handle.fileno())) != captured
                or signature(journal_path.lstat()) != captured or len(raw) != before.st_size):
            raise RuntimeError("journal source changed during capture")
    return raw, captured


def benchmark_journal(journal_path: Path, *, repeats: int, output: Path) -> dict:
    """Same complete logical append/fsync, not complete state publication.

    Pin a bounded source before measuring, retain its exact row order/values,
    and write only new private temporary journals on the output filesystem.
    Compare the existing per-record codec with the grouped native codec using
    the SAME canonical append owner. Full source/prefix bytes must be identical.
    """
    if repeats < 1:
        raise ValueError("repeats must be positive")
    _require_distinct_output(journal_path, output)
    raw, before = _pinned_journal_bytes(journal_path)
    rows = [loads(line) for line in raw.split(b"\n") if line.strip()]
    if not rows or any(not isinstance(row, dict) for row in rows):
        raise ValueError("nonempty object-only journal required")
    baseline = lambda values: b"".join(simulation.ledger_bytes(value) + b"\n" for value in values)
    variants = {"per_record_encoding": baseline, "batch_encoding": simulation.ledger_lines_bytes}
    expected = baseline(rows)
    prefix = b'{"retained_prefix":true}\n'
    samples = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="opening-journal-", dir=output.parent) as temporary:
        for repeat in range(repeats):
            order = tuple(variants) if repeat % 2 == 0 else tuple(reversed(variants))
            for variant in order:
                target = Path(temporary) / f"{repeat}-{variant}.jsonl"
                target.write_bytes(prefix)
                with target.open("rb") as handle:
                    os.fsync(handle.fileno())
                encode_spans, syncs = [], []
                original_sync = os.fsync
                def measured_encode(values):
                    started = time.perf_counter()
                    result = variants[variant](values)
                    encode_spans.append((time.perf_counter() - started) * 1000)
                    return result
                def counted_sync(fd):
                    syncs.append(stat.S_ISREG(os.fstat(fd).st_mode))
                    original_sync(fd)
                with patch.object(simulation, "ledger_lines_bytes", measured_encode), patch.object(os, "fsync", counted_sync):
                    started = time.perf_counter()
                    simulation._append_jsonl_many(target, rows)
                    elapsed_ms = (time.perf_counter() - started) * 1000
                if target.read_bytes() != prefix + expected or syncs != [True] or len(encode_spans) != 1:
                    raise RuntimeError("journal bytes, prefix or batch durability changed")
                samples.append({"variant": variant, "repeat": repeat, "elapsed_ms": elapsed_ms,
                                "encode_ms": encode_spans[0], "file_fsync_count": 1})
    current_raw, current_signature = _pinned_journal_bytes(journal_path)
    if current_signature != before or current_raw != raw:
        raise RuntimeError("journal source changed; result not accepted")
    payload = {"scope": "isolated_complete_logical_journal_append_not_full_state_or_native_SLO",
        "production_writes": False, "network_requests": 0, "source_bytes": len(raw), "source_rows": len(rows),
        "source_sha256": hashlib.sha256(raw).hexdigest(), "encoded_bytes": len(expected),
        "encoded_sha256": hashlib.sha256(expected).hexdigest(), "exact_bytes_and_prefix_equal": True,
        "file_fsync_preserved": True, "state_commit_directory_barriers_not_measured": True,
        "variant_median_ms": {variant: statistics.median(row["elapsed_ms"] for row in samples if row["variant"] == variant)
                              for variant in variants},
        "variant_median_encode_ms": {variant: statistics.median(row["encode_ms"] for row in samples if row["variant"] == variant)
                                     for variant in variants}, "samples": samples}
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def benchmark_journal_snapshots(journal_path: Path, *, repeats: int, output: Path) -> dict:
    """Exact retained bytes -> durable delta -> verified dashboard tail.

    This is one bounded journal-stage experiment, NOT a complete native ledger,
    model, market, state-commit/ACK or public-paint benchmark. Initial sealing
    is measured separately; complete independent byte readback is not skipped.
    """
    from downloader.artifact_io import atomic_write_json
    from stockagent.live import paper_journal as store
    from stockagent.live import tw_day_trade_dashboard as dashboard
    if repeats < 1:
        raise ValueError("repeats must be positive")
    _require_distinct_output(journal_path, output)
    raw, generation = _pinned_journal_bytes(journal_path)
    if not raw or not raw.endswith(b"\n"):
        raise ValueError("journal snapshot comparison requires complete retained JSONL lines")
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < 8 * 1024**3 + 2 * len(raw) + 128 * 1024**2:
        raise RuntimeError("journal comparison must preserve the 8 GiB free-space reserve")
    variants = ("legacy_verified_prefix", "immutable_verified_parent")
    samples, cold, expected = [], {}, hashlib.sha256(raw)
    with tempfile.TemporaryDirectory(prefix="paper-journal-view-", dir=output.parent) as temporary:
        paths = {}
        for name in variants:
            root = Path(temporary) / name
            root.mkdir()
            path = paths[name] = root / "signals.jsonl"
            path.write_bytes(raw)
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            started = time.perf_counter()
            if name == "immutable_verified_parent":
                store.publish_snapshot(path, operation="seed")
            seal_ms = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            dashboard._tail(path, 100, verified_append=True)
            cold[name] = {"initial_seal_ms": seal_ms, "cold_verified_tail_ms": (time.perf_counter()-started)*1000,
                "retained_bytes": len(raw), "immutable_object_bytes": sum(
                    item.stat().st_size for item in (root / store.DIRECTORY).glob("*") if item.is_file())}
        for repeat in range(repeats):
            row = {"paper_journal_benchmark_revision": repeat}
            expected.update(simulation.ledger_lines_bytes([row]))
            order = variants if repeat % 2 == 0 else tuple(reversed(variants))
            tails = []
            for name in order:
                path = paths[name]
                original_sync, sync_count = os.fsync, [0]
                def sync(fd):
                    sync_count[0] += 1
                    original_sync(fd)
                with patch.object(os, "fsync", sync):
                    cpu_started, started = time.thread_time(), time.perf_counter()
                    simulation._append_jsonl(path, row)
                    appended = time.perf_counter()
                    tail = dashboard._tail(path, 100, verified_append=True)
                    done = time.perf_counter()
                    owner_cpu_ms = (time.thread_time()-cpu_started)*1000
                if not tail or tail[-1] != row or sync_count[0] < 1:
                    raise RuntimeError("journal append/tail/durability boundary was not exercised")
                tails.append(simulation.ledger_lines_bytes(tail))
                # Full byte parity, not just the visible rows or an inode/size
                # shortcut. It is deliberately outside the measured delta.
                for opener in (lambda: store.open_paper_journal(path), lambda: path.open("rb")):
                    with opener() as stream:
                        if hashlib.file_digest(stream, "sha256").hexdigest() != expected.hexdigest():
                            raise RuntimeError("complete journal source/delta bytes differ")
                samples.append({"repeat": repeat, "variant": name, "append_ms": (appended-started)*1000,
                    "verified_tail_ms": (done-appended)*1000, "append_to_tail_ms": (done-started)*1000,
                    "owner_cpu_ms": owner_cpu_ms, "fsync_count": sync_count[0],
                    "complete_raw_sha256": expected.hexdigest(), "tail_rows": len(tail)})
            if tails[0] != tails[1]:
                raise RuntimeError("complete dashboard tail values differ across representations")
    if _pinned_journal_bytes(journal_path) != (raw, generation):
        raise RuntimeError("journal source changed; snapshot comparison is not accepted")
    report = {"scope": "bounded_one_journal_append_to_verified_tail_not_full_state_or_native_opening_slo",
        "comparison": "legacy_full_prefix_hash_vs_immutable_verified_append_parent_v1",
        "production_writes": False, "network_requests": 0, "broker_orders": 0,
        "source_bytes": len(raw), "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source_generation": list(generation), "cold_and_reuse_separated": True, "initial_preparation": cold,
        "all_retained_bytes_and_deltas_equal": True, "complete_readback_each_sample_outside_delta_timing": True,
        "tail_values_equal_each_sample": True, "original_file_and_new_head_barriers_preserved": True,
        "state_commit_ack_and_public_transport_not_measured": True,
        "variant_median_ms": {name: {key: statistics.median(item[key] for item in samples if item["variant"]==name)
            for key in ("append_ms", "verified_tail_ms", "append_to_tail_ms", "owner_cpu_ms")} for name in variants},
        "samples": samples}
    atomic_write_json(output, report, durable=True)
    return report


def benchmark_mode_snapshots(state_path: Path, *, repeats: int, output: Path) -> dict:
    """Complete durable commits, equal controlled one-mode clock revisions.

    Each arm has its own cold store, then retains only its own prepared objects.
    First writes and reuse are separate; no prefilled snapshot is called cold.
    This is publication/recovery work, not model/source/fill or native paint SLO.
    """
    from stockagent.live.paper_mode_snapshot import expand_paper_state
    if repeats < 1:
        raise ValueError("repeats must be positive")
    _require_distinct_output(state_path, output)
    if output.parent.resolve() == (state_path.parent / "mode_snapshots").resolve():
        raise ValueError("benchmark output must not modify source mode objects")
    raw, captured = _pinned_journal_bytes(state_path)
    pinned = expand_paper_state(loads(raw), state_path.parent)
    if (pinned.get("simulation_only") is not True or pinned.get("production_order_possible") is not False
            or not isinstance(pinned.get("modes"), dict) or not pinned["modes"]):
        raise ValueError("complete paper simulation state required")
    clock = base_clock = datetime.fromisoformat(pinned["updated_at"])
    if clock.tzinfo is None:
        raise ValueError("publication benchmark requires an explicit timezone")
    output.parent.mkdir(parents=True, exist_ok=True)
    variants = {"inline_state": False, "immutable_mode_objects": True}
    samples, contracts = [], {}
    with tempfile.TemporaryDirectory(prefix="paper-mode-publication-", dir=output.parent) as temporary:
        engines = {name: simulation.TwDayTradeSimulationEngine(Path(temporary) / name,
            publication_clock=lambda: clock, immutable_mode_snapshots=enabled)
            for name, enabled in variants.items()}
        for engine in engines.values():
            engine._engine_run_id = "pinned-mode-publication-benchmark"
        modes = sorted(pinned["modes"])
        value = copy.deepcopy(pinned)
        for repeat in range(repeats):
            # A declared nonfinancial fixture clock moves one mode at a time;
            # every other source value remains byte/value identical.
            clock = base_clock + timedelta(milliseconds=repeat + 1)
            value["modes"][modes[repeat % len(modes)]]["last_mark_at"] = (
                clock.isoformat(timespec="microseconds"))
            value["state_revision"] = int(pinned.get("state_revision") or 0) + repeat
            value["dashboard_content_revision"] = int(pinned.get("dashboard_content_revision") or 0) + repeat
            value["dashboard_content_fingerprint"] = ""
            order = tuple(variants) if repeat % 2 == 0 else tuple(reversed(variants))
            for variant in order:
                engine = engines[variant]
                engine.state = copy.deepcopy(value)
                # A real owner restores this nonfinancial pointer at startup.
                # Give each isolated arm its own path BEFORE measuring, not
                # an old production artifact pointer or a post-hoc money mask.
                for mode in engine.state["modes"].values():
                    mode["executed_positions_path"] = str(engine.positions_path)
                cpu_started = time.thread_time()
                started = time.perf_counter()
                engine._persist(clock)
                elapsed = (time.perf_counter() - started) * 1000
                owner_cpu_ms = (time.thread_time() - cpu_started) * 1000
                contract = _publication_contract(engine,
                    expected_fingerprint=engine.state["dashboard_content_fingerprint"])
                if repeat in contracts and contracts[repeat] != contract:
                    raise RuntimeError("complete publication values differ across layouts")
                contracts[repeat] = contract
                for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl", "events.jsonl", "marks.jsonl"):
                    if (engine.state_dir / name).exists():
                        raise RuntimeError("publication benchmark unexpectedly wrote a financial journal")
                samples.append({"repeat": repeat, "variant": variant,
                    "first_write": repeat == 0, "elapsed_ms": elapsed,
                    "owner_cpu_ms": owner_cpu_ms,
                    "state_manifest_bytes": engine.state_path.stat().st_size,
                    "state_publication": copy.deepcopy(engine.last_persist_metrics),
                    "contract_sha256": contract})
                if repeat == repeats - 1:
                    restore_started = time.perf_counter()
                    restored = simulation.TwDayTradeSimulationEngine(engine.state_dir,
                        publication_clock=lambda: clock)
                    samples[-1]["restore_without_native_historical_journals_ms"] = (
                        time.perf_counter() - restore_started) * 1000
                    if simulation.ledger_bytes(restored.state) != simulation.ledger_bytes(engine.state):
                        raise RuntimeError("restored complete state differs from the published state")
    if _pinned_journal_bytes(state_path) != (raw, captured):
        raise RuntimeError("state source changed; mode comparison not accepted")
    report = {"scope": "complete_isolated_durable_publication_not_native_opening_slo",
        "comparison": "inline_vs_immutable_mode_objects_with_large_sections_v1",
        "production_writes": False, "network_requests": 0, "financial_journal_writes": 0,
        "source_sha256": hashlib.sha256(raw).hexdigest(), "source_bytes": len(raw),
        "mode_count": len(pinned["modes"]), "controlled_change": "one_mode_last_mark_at_fixture_clock_and_isolated_owner_path_only",
        "file_and_directory_barriers_preserved": True,
        "full_publication_values_identical": True, "post_fsync_ack_verified_each_sample": True,
        "complete_restore_values_identical": True,
        "restore_scope": "published_checkpoint_only_no_native_historical_journal_scan",
        "path_normalization": "exact_isolated_engine_root_only_not_source_or_financial_fields",
        "cold_and_reuse_separated": True,
        "variant_median_ms": {name: statistics.median(row["elapsed_ms"] for row in samples
            if row["variant"] == name) for name in variants},
        "variant_reuse_median_ms": {name: (statistics.median(row["elapsed_ms"] for row in samples
            if row["variant"] == name and not row["first_write"]) if repeats > 1 else None)
            for name in variants},
        "variant_median_owner_cpu_ms": {name: statistics.median(row["owner_cpu_ms"] for row in samples
            if row["variant"] == name) for name in variants},
        "cpu_clock_scope": "current_owner_thread_not_filesystem_wait_or_other_workers", "samples": samples}
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def benchmark_journal_transaction(state_path: Path, *, repeats: int, output: Path) -> dict:
    """All six retained journals + full state/commit/ACK + complete byte readback.

    Only private copies receive explicit nonfinancial fixture rows. Capture
    and initial immutable enrollment are charged separately, not called cold
    transaction speed. Models, real orders, network and native SLO are excluded.
    """
    from stockagent.live import paper_journal as store
    from downloader.artifact_io import atomic_write_json
    if repeats < 1:
        raise ValueError("repeats must be positive")
    state_path, output = Path(state_path), Path(output)
    _require_distinct_output(state_path, output)
    if output.resolve().is_relative_to(state_path.parent.resolve()):
        raise ValueError("transaction report must not modify its source book")
    raw, state_generation = _pinned_journal_bytes(state_path)
    pinned = read_paper_state(state_path, use_cache=False)
    if (pinned.get("simulation_only") is not True or pinned.get("production_order_possible") is not False
            or not isinstance(pinned.get("modes"), dict) or not pinned["modes"]):
        raise ValueError("complete paper simulation state required")
    root = state_path.parent
    generations = paper_journal_generations(root)
    total = sum((generations[name] or [0, 0, 0])[2] for name in store.FILENAMES)
    if any(generations[name] is None for name in store.FILENAMES):
        raise ValueError("complete six-journal source inventory required")
    output.parent.mkdir(parents=True, exist_ok=True)
    # Two raw copies plus a worst-case incompressible immutable object set.
    if shutil.disk_usage(output.parent).free < 3 * total + 8 * 1024**3:
        raise RuntimeError("transaction comparison must preserve its full-copy admission and 8 GiB reserve")
    from stockagent.data_sync.cache_capacity import acquisition_dispatch_status
    capacity = acquisition_dispatch_status(output.parent, maximum_growth_bytes=3 * total)
    if capacity.get("state") != "admitted":
        raise RuntimeError("transaction comparison physical backing capacity is not admitted")
    base_clock = datetime.fromisoformat(pinned["updated_at"])
    if base_clock.tzinfo is None:
        raise ValueError("explicit paper publication timezone required")
    samples, captures, oracle, expected_values = [], {}, {}, {}
    with tempfile.TemporaryDirectory(prefix="paper-full-transaction-", dir=output.parent) as temporary:
        clock = base_clock
        engines = {}
        for variant in ("legacy_projection", "expected_append_intent"):
            target = Path(temporary) / variant
            target.mkdir()
            engine = simulation.TwDayTradeSimulationEngine(target, publication_clock=lambda: clock)
            engine._engine_run_id = "pinned-six-journal-transaction-benchmark"
            engine.state = copy.deepcopy(pinned)
            for mode in engine.state["modes"].values():
                mode["executed_positions_path"] = str(engine.positions_path)
            engines[variant] = engine
            copied_at, source_hashes = time.perf_counter(), {}
            for name in store.FILENAMES:
                source, destination = root / name, target / name
                digest, size = hashlib.sha256(), 0
                with store.open_paper_journal(source) as incoming, destination.open("wb") as outgoing:
                    before = store._generation(os.fstat(incoming.fileno()))
                    if before != tuple(generations[name]) or not stat.S_ISREG(os.fstat(incoming.fileno()).st_mode):
                        raise RuntimeError("source journal changed before complete capture")
                    while block := incoming.read(4 * 1024**2):
                        outgoing.write(block)
                        digest.update(block)
                        size += len(block)
                    outgoing.flush()
                    os.fsync(outgoing.fileno())
                    if (before != store._generation(os.fstat(incoming.fileno()))
                            or before != store._generation(source.lstat()) or size != before[2]):
                        raise RuntimeError("source journal changed during complete capture")
                source_hashes[name] = {"sha256": digest.hexdigest(), "size_bytes": size}
                if variant == "legacy_projection":
                    oracle[name] = digest
                elif source_hashes[name] != captures["legacy_projection"]["sources"][name]:
                    raise RuntimeError("captured source bytes differ between transaction arms")
            copied_ms = (time.perf_counter() - copied_at) * 1000
            sealed_at = time.perf_counter()
            if variant == "expected_append_intent":
                for name in store.FILENAMES:
                    store.publish_snapshot(target / name, operation="seed")
            captures[variant] = {"copy_and_fsync_ms": copied_ms,
                "initial_enrollment_ms": (time.perf_counter() - sealed_at) * 1000, "sources": source_hashes}

        for repeat in range(repeats):
            clock = base_clock + timedelta(milliseconds=repeat + 1)
            row = {"paper_transaction_benchmark": repeat, "simulation_only": True,
                   "signed_zero": -0.0, "big_integer": 9007199254740993}
            delta = simulation.ledger_lines_bytes([row])
            for digest in oracle.values():
                digest.update(delta)
            variants = tuple(engines) if repeat % 2 == 0 else tuple(reversed(engines))
            for variant in variants:
                engine = engines[variant]
                started, cpu_started = time.perf_counter(), time.thread_time()
                for name in store.FILENAMES:
                    simulation._append_jsonl_many(engine.state_dir / name, [row])
                appended_at = time.perf_counter()
                engine._persist(clock)
                committed_at = time.perf_counter()
                contract = _publication_contract(engine,
                    expected_fingerprint=engine.state["dashboard_content_fingerprint"])
                if repeat in expected_values and expected_values[repeat] != contract:
                    raise RuntimeError("complete financial publication differs across transaction arms")
                expected_values[repeat] = contract
                with committed_paper_read_guard(engine.state_dir):
                    for name in store.FILENAMES:
                        with store.open_paper_journal(engine.state_dir / name) as stream:
                            actual = hashlib.file_digest(stream, "sha256").hexdigest()
                        if actual != oracle[name].hexdigest():
                            raise RuntimeError("complete retained journal/delta byte identity differs")
                completed = time.perf_counter()
                samples.append({"repeat": repeat, "variant": variant,
                    "append_ms": (appended_at - started) * 1000,
                    "state_commit_ack_ms": (committed_at - appended_at) * 1000,
                    "complete_readback_ms": (completed - committed_at) * 1000,
                    "complete_workflow_ms": (completed - started) * 1000,
                    "owner_cpu_ms": (time.thread_time() - cpu_started) * 1000,
                    "publication_sha256": contract, "state_publication": copy.deepcopy(engine.last_persist_metrics)})
        # Independent restore uses the same canonical audit path, including
        # retained historical journals, rather than merely re-reading JSON.
        for variant, engine in engines.items():
            restored_at = time.perf_counter()
            restored = simulation.TwDayTradeSimulationEngine(engine.state_dir, publication_clock=lambda: clock)
            if simulation.ledger_bytes(restored.state) != simulation.ledger_bytes(engine.state):
                raise RuntimeError("full-journal restore changed financial state")
            captures[variant]["restore_ms"] = (time.perf_counter() - restored_at) * 1000
        if (_pinned_journal_bytes(state_path) != (raw, state_generation)
                or paper_journal_generations(root) != generations):
            raise RuntimeError("source changed; complete transaction comparison not accepted")
    report = {"scope": "isolated_six_journal_append_full_state_commit_ack_and_full_readback_not_opening_slo",
        "contract": "paper_complete_transaction_benchmark_v1", "source_state_sha256": hashlib.sha256(raw).hexdigest(),
        "capacity_admission": capacity,
        "source_journal_bytes": total, "mode_count": len(pinned["modes"]), "repeats": repeats,
        "source_journal_generations": generations, "source_state_generation": list(state_generation),
        "captures": captures, "production_writes": False, "network_requests": 0, "broker_orders": 0,
        "fixture_delta": "declared_nonfinancial_benchmark_row_in_each_private_copy_only",
        "complete_journal_bytes_equal": True, "complete_financial_publication_equal": True,
        "post_fsync_ack_verified_each_sample": True, "complete_restore_equal": True,
        "restore_scope": "same_process_canonical_restore_with_full_historical_journals_not_machine_cold_boot",
        "median_ms": {variant: {key: statistics.median(row[key] for row in samples if row["variant"] == variant)
            for key in ("append_ms", "state_commit_ack_ms", "complete_readback_ms", "complete_workflow_ms")}
            for variant in engines}, "samples": samples}
    atomic_write_json(output, report, durable=True)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--state", type=Path)
    source.add_argument("--journal", type=Path, help="Paired exact logical JSONL batch append; not full state publication")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--profile", action="store_true", help="Diagnostic profiling; do not mix with unprofiled timings")
    parser.add_argument("--mode-snapshots", action="store_true", help="Compare complete inline versus immutable-mode durable publication")
    parser.add_argument("--journal-snapshots", action="store_true", help="Bounded journal append-to-verified-tail comparison; preserves full byte readback")
    parser.add_argument("--key-guard", action="store_true", help="Paired exact old/new private JSON key predicates in complete durable publication")
    parser.add_argument("--ledger-codec", action="store_true", help="Paired native-guard versus canonical stdlib ledger codec in complete durable publication")
    parser.add_argument("--journal-transaction", action="store_true", help="Complete six-journal append, state commit/ACK and full retained-byte readback")
    args = parser.parse_args()
    if args.journal is not None and args.profile:
        parser.error("journal comparison retains unprofiled timings only")
    if args.mode_snapshots and (args.journal is not None or args.profile):
        parser.error("mode snapshots require an unprofiled state comparison")
    if args.key_guard and (args.mode_snapshots or args.journal is not None):
        parser.error("key guard comparison requires its own complete state publication")
    if args.ledger_codec and (args.key_guard or args.mode_snapshots or args.journal is not None or args.journal_transaction):
        parser.error("ledger codec requires its own complete state publication")
    if args.journal_snapshots and (args.journal is None or args.profile or args.mode_snapshots or args.key_guard):
        parser.error("journal snapshots require their own unprofiled journal comparison")
    if args.journal_transaction and (args.state is None or args.profile or args.mode_snapshots or args.key_guard or args.journal_snapshots):
        parser.error("journal transaction requires its own unprofiled complete state comparison")
    payload = (benchmark_journal_transaction(args.state, repeats=args.repeats, output=args.output) if args.journal_transaction
               else benchmark_journal_snapshots(args.journal, repeats=args.repeats, output=args.output) if args.journal_snapshots
               else benchmark_mode_snapshots(args.state, repeats=args.repeats, output=args.output) if args.mode_snapshots
               else benchmark_journal(args.journal, repeats=args.repeats, output=args.output) if args.journal is not None
               else benchmark(args.state, repeats=args.repeats, output=args.output, profile=args.profile,
                              key_guard=args.key_guard, ledger_codec=args.ledger_codec))
    print(json.dumps({key: value for key, value in payload.items() if key != "samples"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
