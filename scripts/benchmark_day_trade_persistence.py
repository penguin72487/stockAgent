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
from datetime import datetime
import hashlib
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live import tw_day_trade_simulation as simulation
from stockagent.live.tw_day_trade_monitor_projection import validated_shioaji_monitor_projection
from stockagent.live.tw_day_trade_service_sync import (
    committed_paper_read_guard, load_service_sync,
    paper_journal_generations, paper_projection_generations,
)


def _publication_contract(engine, *, expected_fingerprint: str) -> str:
    with committed_paper_read_guard(engine.state_dir):
        payloads = {name: json.loads((engine.state_dir / name).read_text(encoding="utf-8"))
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
    return hashlib.sha256(json.dumps(payloads, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _stdlib_fingerprint(payload) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
        default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def benchmark(state_path: Path, *, repeats: int, output: Path, profile: bool = False) -> dict:
    if repeats < 1:
        raise ValueError("repeats must be positive")
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
                started = time.perf_counter()
                with patch.object(simulation, "_dashboard_material_fingerprint", measured_fingerprint):
                    engine._persist(clock)
                elapsed_ms = (time.perf_counter() - started) * 1000
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
        "comparison": "private_ui_fingerprint_stdlib_vs_guarded_native_v2",
        "ephemeral_digest_invalidated_for_equal_work": True,
        "logical_fingerprint_input_sha256": expected_logical_input,
        "contract_sha256": expected_contract,
        "variant_median_ms": {variant: statistics.median(row["elapsed_ms"] for row in samples
                                                         if row["variant"] == variant)
                              for variant in variants},
        "samples": samples,
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--profile", action="store_true", help="Diagnostic profiling; do not mix with unprofiled timings")
    args = parser.parse_args()
    payload = benchmark(args.state, repeats=args.repeats, output=args.output, profile=args.profile)
    print(json.dumps({key: value for key, value in payload.items() if key != "samples"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
