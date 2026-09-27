"""Bounded ABBA of the read-only compact status builder, never the live cache.

Fresh workers copy validated ledger indices into private caches and share frozen
small metadata plus read-only source links. No service/API call or cache flush is
performed. First-call timings are module-cold, NOT filesystem/source-cold.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REQUIRED_INDICES = (
    ("marks", True), ("benchmark_marks", True), ("events", True),
    ("orders", False), ("fills", False), ("latency", True),
)
MAX_METADATA_BYTES = 16 << 20
MAX_SESSION_BYTES = 32 << 20


def _signature(path: Path) -> list[int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def _digest(value: object) -> str:
    # Compare every field and preserve array order; only object key order differs.
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        allow_nan=False,
    ).encode()).hexdigest()


def _validated_index(source: Path, cache: Path, fallback: bool, session: str):
    identity = f"{source.resolve()}\0recorded_at_fallback={int(fallback)}"
    name = f"ledger-session-index-v1-{hashlib.sha256(identity.encode()).hexdigest()}.json"
    path = cache / name
    if path.stat().st_size > MAX_METADATA_BYTES:
        raise ValueError("oversized ledger index")
    data = path.read_bytes()
    meta = json.loads(data)
    signature = _signature(source)
    if (
        meta.get("schema_version") != 1
        or meta.get("source") != str(source.resolve())
        or meta.get("recorded_at_fallback") is not fallback
        or [meta.get(key) for key in ("device", "inode", "observed_size", "modified_ns")]
        != (signature[:4] if signature else None)
        or signature is None
        or meta.get("scanned_offset") != signature[2]
    ):
        raise ValueError(f"missing/stale index; benchmark will not scan {source.name}")
    spans = meta.get("spans", {}).get(session, [])
    if any(not 0 <= start < end <= signature[2] for start, end in spans):
        raise ValueError("invalid selected-session spans")
    if sum(end - start for start, end in spans) > MAX_SESSION_BYTES:
        raise ValueError("selected-session read exceeds benchmark bound")
    return name, data


def _worker(config: dict) -> dict:
    if config.get("baseline_module"):
        baseline = Path(config["baseline_module"])
        if hashlib.sha256(baseline.read_bytes()).hexdigest() != config["baseline_sha256"]:
            raise ValueError("baseline module SHA mismatch")
        name = "_tw_compact_status_baseline"
        spec = importlib.util.spec_from_file_location(name, baseline)
        dashboard = importlib.util.module_from_spec(spec)
        sys.modules[name] = dashboard
        spec.loader.exec_module(dashboard)
        if hashlib.sha256(baseline.read_bytes()).hexdigest() != config["baseline_sha256"]:
            raise ValueError("baseline module changed during import")
    else:
        from stockagent.live import tw_day_trade_dashboard as dashboard
    from stockagent.live.public_dashboards import sanitize_tw_status
    from scripts.serve_public_dashboards import _prepared

    os.environ["STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"] = config["cache"]
    dashboard.DEFAULT_OPENING_GATE_PATH = Path(config["gate"])

    def reject_scan(*_args, **_kwargs):
        raise RuntimeError("benchmark refuses a whole-ledger index/history scan")

    dashboard._ledger_line_session_date = reject_scan
    dashboard._benchmark_history_index = reject_scan
    reader_timings: list[dict] = []
    for name in ("_rows_for_sessions", "_columnar_ledger_frame", "_tail_for_session",
                 "_latest_contiguous_session_rows", "_object"):
        original = getattr(dashboard, name)

        def timed(*args, _name=name, _original=original, **kwargs):
            start, cpu = time.perf_counter_ns(), time.process_time_ns()
            try:
                return _original(*args, **kwargs)
            finally:
                reader_timings.append({
                    "function": _name, "source": Path(args[0]).name,
                    "wall_ns": time.perf_counter_ns() - start,
                    "cpu_ns": time.process_time_ns() - cpu,
                })

        setattr(dashboard, name, timed)

    def io():
        return {
            line.split(":")[0]: int(line.split(":")[1])
            for line in Path("/proc/self/io").read_text().splitlines()
        }

    samples = []
    for iteration in range(config["iterations"]):
        reader_timings.clear()
        phases = {}

        def phase(name, call):
            start, cpu = time.perf_counter_ns(), time.process_time_ns()
            result = call()
            phases[name] = {"wall_ns": time.perf_counter_ns() - start,
                            "cpu_ns": time.process_time_ns() - cpu}
            return result

        before = io()
        start, cpu = time.perf_counter_ns(), time.process_time_ns()
        raw = phase("snapshot", lambda: dashboard.build_dashboard_snapshot(
            state_dir=Path(config["root"]), session_date=config["session"],
            preopen_readiness_path=Path(config["preopen"]),
            discord_service_status_path=Path(config["discord"]),
            unattended_guardian_path=Path(config["guardian"]),
            now=datetime.fromisoformat(config["now"]), maximum_event_rows=500,
            maximum_mark_rows=32, include_position_rows=False,
            include_ledger_session_dates=False,
            include_order_fill_rows=config["include"],
        ))
        payload = phase("sanitize", lambda: sanitize_tw_status(raw))
        encoded = phase("json_encode", lambda: (json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False,
        ) + "\n").encode())
        response = phase("prepare", lambda: _prepared(
            encoded, content_type="application/json; charset=utf-8", cache_control="no-store",
        ))
        after = io()
        # End timing before parity hashes; evidence generation is not serving cost.
        sample = {
            "iteration": iteration,
            "variant": config.get("variant", "A" if config["include"] else "B"),
            "cache_class": "fresh_module_first_call" if iteration == 0 else "same_module_steady",
            "include_order_fill_rows": config["include"],
            "wall_ns": time.perf_counter_ns() - start,
            "cpu_ns": time.process_time_ns() - cpu,
            "phases": phases, "inclusive_reader_timings": list(reader_timings),
            "read_bytes_delta": after["read_bytes"] - before["read_bytes"],
            "rchar_delta": after["rchar"] - before["rchar"],
            "maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "raw_orders": len(raw["orders"]), "raw_fills": len(raw["fills"]),
            "payload_bytes": len(encoded), "gzip_bytes": len(response.gzip_body),
        }
        sample.update({
            "payload_sha256": hashlib.sha256(encoded).hexdigest(),
            "canonical_payload_sha256": _digest(payload),
            "top_level_field_sha256": {key: _digest(value) for key, value in payload.items()},
        })
        samples.append(sample)
    return {"measurements": samples}


def _summary(workers: list[dict]) -> dict:
    samples = [sample for worker in workers for sample in worker["measurements"]]
    first = samples[0]["top_level_field_sha256"]
    output = {
        "raw_json_byte_parity": len({sample["payload_sha256"] for sample in samples}) == 1,
        "canonical_payload_parity": len({sample["canonical_payload_sha256"] for sample in samples}) == 1,
        "differing_fields": sorted({
            key for sample in samples for key in first.keys() | sample["top_level_field_sha256"].keys()
            if first.get(key) != sample["top_level_field_sha256"].get(key)
        }),
        "timings": {},
    }
    for variant in ("A", "B"):
        output["timings"][variant] = {}
        for kind in ("fresh_module_first_call", "same_module_steady"):
            selected = [s for s in samples if s["variant"] == variant and s["cache_class"] == kind]
            output["timings"][variant][kind] = {
                "count": len(selected),
                **{f"{metric}_median_ms": statistics.median(s[f"{metric}_ns"] / 1e6 for s in selected)
                   for metric in ("wall", "cpu")},
            }
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=ROOT / "artifacts/live/tw_day_trade_simulation")
    parser.add_argument("--index-cache-dir", type=Path, default=Path("/var/cache/stockagent-public-dashboards"))
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--now", default=None, help="fixed timezone-aware ISO time; default is start time")
    parser.add_argument("--iterations", type=int, choices=range(2, 6), default=5)
    parser.add_argument("--baseline-module", type=Path, help="compare hash-pinned pre-change dashboard vs current; both omit order/fill display rows")
    parser.add_argument("--baseline-sha256", help="required with --baseline-module")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve previous evidence")
    if bool(args.baseline_module) != bool(args.baseline_sha256):
        parser.error("--baseline-module and --baseline-sha256 are required together")
    if args.baseline_module and hashlib.sha256(args.baseline_module.read_bytes()).hexdigest() != args.baseline_sha256:
        parser.error("baseline module SHA mismatch")
    try:
        datetime.strptime(args.session_date, "%Y-%m-%d")
    except ValueError:
        parser.error("--session-date must be YYYY-MM-DD")
    if len(args.session_date) != 10:
        parser.error("--session-date must be YYYY-MM-DD")
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    if now.tzinfo is None:
        parser.error("--now must include a timezone")
    source = args.state_dir.resolve()
    if any(args.output.resolve().is_relative_to(path.resolve())
           for path in (source, args.index_cache_dir)):
        parser.error("receipt must not be written into source or live index cache")
    os.nice(15)
    subprocess.run(["ionice", "-c", "3", "-p", str(os.getpid())], check=True)
    paths = [source / f"{name}.jsonl" for name, _ in REQUIRED_INDICES]
    paths += [source / "opening_signal_latency.jsonl", source / "signals.jsonl"]
    paths += [path for folder in ("position_history", "overnight_position_history")
              for path in (source / folder / args.session_date).glob("*.json")]
    before = {str(path): _signature(path) for path in paths}
    indices = [_validated_index(source / f"{name}.jsonl", args.index_cache_dir, fallback, args.session_date)
               for name, fallback in REQUIRED_INDICES]
    for name in ("latency.jsonl", "opening_signal_latency.jsonl"):
        signature = _signature(source / name)
        if signature and signature[2] > MAX_SESSION_BYTES:
            raise ValueError("latency source exceeds benchmark bound")
    code_paths = [Path(__file__).resolve(), ROOT / "stockagent/live/tw_day_trade_dashboard.py",
                  ROOT / "stockagent/live/public_dashboards.py", ROOT / "scripts/serve_public_dashboards.py"]
    if args.baseline_module:
        code_paths.append(args.baseline_module.resolve())
    code_hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    receipt = {
        "schema_version": 1, "kind": "isolated_read_only_tw_compact_status_abba",
        "production_enabled": False, "fixed_now_utc": now.isoformat(),
        "session_date": args.session_date, "argv": sys.argv,
        "method": {
            "order": ["A", "B", "B", "A"], "A_include_order_fill_rows": True,
            "B_include_order_fill_rows": False, "iterations_per_worker": args.iterations,
            "cold_first": "first builder call after module imports; not source/OS-cold",
            "steady": "same worker and variant; no cache flush",
            "metadata": "frozen small JSON; original ledgers/history linked read-only",
            "cache": "private copied validated indices; reject whole-ledger reindex",
            "scheduler": "nice 15; ionice idle", "reader_timings": "inclusive; do not sum nested times",
            "scope": "builder/sanitize/encode/prepare, excludes HTTP, revision route and cache wait",
        },
        "source_signatures_before": before, "source_code_sha256": code_hashes,
        "metadata_receipts": {}, "workers": [],
    }
    if args.baseline_module:
        receipt["method"].update({
            "comparison": "prechange_module_vs_complete_session_cache",
            "A_include_order_fill_rows": False,
            "baseline_module": str(args.baseline_module.resolve()),
            "baseline_sha256": args.baseline_sha256,
            "index_ctime_boundary": "legacy disk index lacks ctime; only in-process observed rewrites are proven; loss of both observations restores the legacy four-field boundary",
        })
    with tempfile.TemporaryDirectory(prefix="tw-compact-status-") as temporary:
        tmp = Path(temporary)
        frozen = tmp / "state"
        frozen.mkdir()

        def freeze(path: Path, target: Path):
            signature = _signature(path)
            if signature is None:
                receipt["metadata_receipts"][str(path)] = {"signature": None}
                return
            if signature[2] > MAX_METADATA_BYTES:
                raise ValueError(f"metadata exceeds benchmark bound: {path.name}")
            data = path.read_bytes()
            if _signature(path) != signature:
                raise ValueError("metadata changed during freeze")
            target.write_bytes(data)
            os.utime(target, ns=(signature[3], signature[3]))
            receipt["metadata_receipts"][str(path)] = {
                "signature": signature, "sha256": hashlib.sha256(data).hexdigest(),
            }

        for path in source.iterdir():
            target = frozen / path.name
            if path.name in {"state.json", "status.json", "service_sync.json", "preopen_readiness.json"} and path.is_file():
                freeze(path, target)
            else:
                target.symlink_to(path, target_is_directory=path.is_dir())
        ancillary = {
            "preopen": ROOT / "artifacts/discord_bot/preopen_readiness.json",
            "discord": ROOT / "artifacts/discord_bot/service_status.json",
            "guardian": ROOT / "artifacts/operations/tw_day_trade_guardian/latest.json",
            "gate": ROOT / "artifacts/data_refresh/tw_public/preopen_gate/latest.json",
        }
        for name, path in ancillary.items():
            freeze(path, tmp / f"{name}.json")
        for index, variant in enumerate(receipt["method"]["order"]):
            isolated = tmp / f"cache-{index}"
            isolated.mkdir()
            for name, data in indices:
                (isolated / name).write_bytes(data)
            config = {
                "root": str(frozen), "cache": str(isolated), "session": args.session_date,
                "now": now.isoformat(), "iterations": args.iterations, "include": variant == "A",
                "variant": variant,
                **{name: str(tmp / f"{name}.json") for name in ancillary},
            }
            if args.baseline_module:
                config["include"] = False
                if variant == "A":
                    config.update({"baseline_module": str(args.baseline_module.resolve()),
                                   "baseline_sha256": args.baseline_sha256})
            command = [sys.executable, str(Path(__file__).resolve()), "--worker", json.dumps(config)]
            start = time.perf_counter_ns()
            result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60)
            receipt["workers"].append({
                "worker_index": index, "variant": variant,
                "process_wall_ns_including_imports": time.perf_counter_ns() - start,
                **json.loads(result.stdout),
            })
    receipt["source_signatures_after"] = {str(path): _signature(path) for path in paths}
    receipt["sources_unchanged"] = receipt["source_signatures_after"] == before
    receipt["code_unchanged"] = code_hashes == {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths
    }
    receipt.update(_summary(receipt["workers"]))
    receipt["accepted"] = all(receipt[key] for key in ("sources_unchanged", "code_unchanged", "canonical_payload_parity"))
    receipt["finished_at_utc"] = datetime.now(UTC).isoformat()
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"receipt": str(args.output), "accepted": receipt["accepted"], "timings": receipt["timings"]}))
    return 0 if receipt["accepted"] else 1


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        print(json.dumps(_worker(json.loads(sys.argv[2]))))
    else:
        raise SystemExit(main())
