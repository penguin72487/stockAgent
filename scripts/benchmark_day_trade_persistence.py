"""Measure complete paper-state publication on pinned data, never live ledgers.

Both variants use the canonical writer, including file and directory fsyncs.
Only JSON whitespace differs. Four complete public views must agree, and the
monitor's file-identity binding is verified before excluding that physical
identity from the semantic comparison. Every measured sample is retained.
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


def _publication_contract(engine) -> str:
    payloads = {name: json.loads((engine.state_dir / name).read_text(encoding="utf-8"))
                for name in ("state.json", "positions.json", "status.json", "service_sync.json")}
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
    return hashlib.sha256(json.dumps(payloads, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


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
    original_dumps = json.dumps
    original_writer = simulation._atomic_json

    # Keep the same filesystem as the requested output. Do not use a ramdisk
    # and describe it as the live durable-publication cost.
    with tempfile.TemporaryDirectory(prefix="opening-persist-", dir=output.parent) as temporary:
        engine = simulation.TwDayTradeSimulationEngine(Path(temporary), publication_clock=lambda: clock)
        engine._engine_run_id = "pinned-offline-publication-benchmark"
        for repeat in range(repeats):
            for variant in (("indented", "compact") if repeat % 2 == 0 else ("compact", "indented")):
                engine.state = copy.deepcopy(pinned)
                writes = []

                def measured_writer(path, payload, **kwargs):
                    def publication_dumps(value, *args, **kwargs):
                        kwargs["indent"] = 2 if variant == "indented" else None
                        if variant == "compact":
                            kwargs["separators"] = (",", ":")
                        else:
                            kwargs.pop("separators", None)
                        return original_dumps(value, *args, **kwargs)

                    started = time.perf_counter()
                    with patch.object(simulation.json, "dumps", publication_dumps):
                        original_writer(path, payload, **kwargs)
                    writes.append({"file": path.name, "elapsed_ms": (time.perf_counter() - started) * 1000,
                                   "bytes": path.stat().st_size})

                profiler = cProfile.Profile() if profile else None
                if profiler:
                    profiler.enable()
                started = time.perf_counter()
                with patch.object(simulation, "_atomic_json", measured_writer):
                    engine._persist(clock)
                elapsed_ms = (time.perf_counter() - started) * 1000
                if profiler:
                    profiler.disable()
                    profiler.dump_stats(str(output.with_suffix(f".{variant}.{repeat}.prof")))
                contract = _publication_contract(engine)
                if expected_contract is None:
                    expected_contract = contract
                if contract != expected_contract:
                    raise RuntimeError("complete publication values changed between variants")
                if any(path.exists() for path in (engine.signals_path, engine.orders_path, engine.fills_path)):
                    raise RuntimeError("persistence benchmark unexpectedly wrote an execution ledger")
                samples.append({"repeat": repeat, "variant": variant, "elapsed_ms": elapsed_ms,
                                "writes": writes, "contract_sha256": contract})
    payload = {
        "scope": "isolated_full_state_publication_not_live_opening_latency",
        "production_writes": False, "network_requests": 0, "ledger_writes": 0,
        "state_source": str(state_path.resolve()), "state_source_sha256": hashlib.sha256(raw).hexdigest(),
        "state_source_bytes": len(raw), "profiled": profile,
        "file_fsync_preserved": True, "directory_fsync_preserved": True,
        "full_publication_values_identical": True, "monitor_binding_verified_each_sample": True,
        "contract_sha256": expected_contract,
        "variant_median_ms": {variant: statistics.median(row["elapsed_ms"] for row in samples
                                                         if row["variant"] == variant)
                              for variant in ("indented", "compact")},
        "samples": samples,
    }
    output.write_text(original_dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
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
