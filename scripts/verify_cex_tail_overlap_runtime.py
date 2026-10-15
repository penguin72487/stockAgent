"""Read-only admission of the exact benchmarked tail helper in native CEX jobs.

Observe the existing timer's next fresh owners, never start/restart collectors.
Short parallel samples prove actual helper execution, not an engine label.
An owner that completes before observation remains inconclusive, not failed data.
"""
from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.artifact_io import atomic_write_json
from scripts.benchmark_cex_tail_overlap import digest, load_reference, sha
from scripts.verify_finlab_minute_admission_runtime import unit_identity
from stockagent.data.finlab_acquisition_contract import process_owner

MODULES = {"bybit": bybit, "okx": okx, "binance": binance}
UNIT = "stockagent-registered-data-intraday.service"
PROTECTED = ("stockagent-tw-day-trade-simulation.service", "stockagent-shioaji-taifex-bidask.service",
             "stockagent-shioaji-historical-market-data.service", "stockagent-finmind-complement.service",
             "stockagent-shioaji-taifex-dashboard.service", "stockagent-public-dashboards.service")


def overlap_ast(source: str, *, provider: str, prototype_input: bool) -> str:
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Name) and node.func.id == "tail_newer_summary"]
    if len(calls) != 1 or len(calls[0].args) != 1:
        raise ValueError("one exact overlap helper call required")
    expected = ("_read_overlap_dates(tail_path)" if prototype_input else
                "pl.from_arrow(pq.read_table(tail_path, columns=['date'], memory_map=True))" if provider == "binance"
                else "_read_date_column(tail_path)")
    if ast.dump(calls[0].args[0]) != ast.dump(ast.parse(expected, mode="eval").body):
        raise ValueError("benchmark date projection differs from live collector")
    calls[0].args[0] = ast.Name(id="same_exact_date_projection", ctx=ast.Load())
    return ast.dump(tree)


def validate_code(comparison: Path) -> dict:
    comparison = comparison.resolve()
    receipt_sha = sha(comparison)
    receipt = json.loads(comparison.read_text())
    if (receipt.get("contract") != "complete_cex_tail_information_roundtrip_v1"
            or receipt.get("state") != "accepted_output_parity"
            or receipt.get("provider_calls") != 0 or receipt.get("native_source_writes") != 0
            or receipt.get("all_base_generations_checked") is not True):
        raise ValueError("matching complete accepted benchmark required")
    helper = ROOT / "downloader/ohlcv_hot_tail.py"
    expected_paths = {str(helper), str(ROOT / "scripts/benchmark_cex_tail_overlap.py"), *(
        str(comparison.parent.parent / f"reference_{provider}.py") for provider in MODULES)}
    if set(receipt.get("code_sha256", {})) != expected_paths:
        raise ValueError("exact complete benchmark code scope required")
    if any(sha(Path(path)) != expected for path, expected in receipt["code_sha256"].items()):
        raise ValueError("benchmark/oracle/helper generation changed")
    if sha(helper) != receipt["code_sha256"].get(str(helper)):
        raise ValueError("benchmarked helper generation changed")
    current = {str(helper): sha(helper)}
    functions = {}
    for provider, module in MODULES.items():
        path = comparison.parent.parent / f"reference_{provider}.py"
        if sha(path) != receipt["code_sha256"].get(str(path)):
            raise ValueError("preserved complete oracle changed")
        original = load_reference(path, provider)
        name = "_load_logical_existing_info" if provider == "binance" else "_load_logical_existing_candle_info"
        function = getattr(original, name)
        # Keep generated source bound to the already accepted original module.
        source = inspect.getsource(function)
        start, end = source.index("    dates = "), source.index("    contiguous = ")
        candidate = source[:start] + (
            "    newer_count, first_new = tail_newer_summary(\n"
            "        _read_overlap_dates(tail_path), base_latest_ms=base.latest_ms,\n"
            "        normalize=_normalize_date_frame,\n"
            "    )\n"
        ) + source[end:]
        candidate = candidate.replace("base.rows + newer.height", "base.rows + newer_count")
        actual = inspect.getsource(getattr(module, name))
        if overlap_ast(candidate, provider=provider, prototype_input=True) != overlap_ast(
                actual, provider=provider, prototype_input=False):
            raise ValueError("complete live overlap function differs from accepted prototype")
        # The shared fast path doesn't replace physical date/interval/normalizer rules.
        for physical in ("_normalize_date_frame", "_load_existing_info" if provider == "binance" else "_load_existing_candle_info"):
            if ast.dump(ast.parse(inspect.getsource(getattr(module, physical)))) != ast.dump(
                    ast.parse(inspect.getsource(getattr(original, physical)))):
                raise ValueError("physical source or legacy normalization rules changed")
        current[str(Path(module.__file__).resolve())] = sha(Path(module.__file__))
        functions[provider] = digest(actual)
    return {"benchmark_receipt_sha256": receipt_sha, "code_sha256": current,
            "functions_sha256": functions, "live_matches_complete_prototype": True}


def native_owners(unit: dict, code_not_before: float) -> dict:
    if unit["ActiveState"] != "activating" or not unit["ControlGroup"] or not int(unit["MainPID"]):
        return {}
    group = Path("/sys/fs/cgroup") / unit["ControlGroup"].lstrip("/")
    try:
        pids = (group / "cgroup.procs").read_text().split()
        boot_wall = time.time() - float(Path("/proc/uptime").read_text().split()[0])
    except OSError:
        return {}
    owners = {}
    for text in pids:
        proc = Path("/proc") / text
        try:
            command = [x.decode() for x in (proc / "cmdline").read_bytes().split(b"\0") if x]
            if len(command) < 2 or (proc / "exe").resolve() != Path(sys.executable).resolve() or (proc / "cwd").resolve() != ROOT:
                continue
            for provider in MODULES:
                if Path(command[1]).resolve() != ROOT / f"downloader/download_{provider}_perp_1m.py":
                    continue
                owner = process_owner(int(text))
                if not owner or boot_wall + int(owner["start_ticks"]) / os.sysconf("SC_CLK_TCK") <= code_not_before:
                    continue
                if provider in owners:
                    raise ValueError("duplicate formal native source owner")
                owners[provider] = owner
        except (OSError, UnicodeError):
            continue
    return owners


def helper_samples(body: dict) -> int:
    frames = body.get("shared", {}).get("frames", [])
    matches = {i for i, frame in enumerate(frames) if frame.get("name") == "tail_newer_summary"
               and frame.get("file") == str(ROOT / "downloader/ohlcv_hot_tail.py")}
    return sum(bool(matches.intersection(stack)) for profile in body.get("profiles", [])
               for stack in profile.get("samples", []))


def verify(comparison: Path, output: Path, *, wait_seconds: float = 180) -> dict:
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        raise ValueError("fresh private operational receipt required")
    if not 0 <= wait_seconds <= 180:
        raise ValueError("bounded wait required")
    admission = validate_code(comparison)
    code_not_before = max(Path(path).stat().st_mtime for path in admission["code_sha256"])
    protected = {unit: unit_identity(unit) for unit in PROTECTED}
    deadline = time.monotonic() + wait_seconds
    while True:
        unit = unit_identity(UNIT)
        owners = native_owners(unit, code_not_before)
        if len(owners) == 3 or time.monotonic() >= deadline:
            break
        time.sleep(2)  # Independent diagnostic process; no collector work is blocked.
    rows = []
    if len(owners) == 3:
        print(json.dumps({"stage": "native_before", "unit": unit, "owners": owners}), flush=True)

        def profile(item):
            provider, owner = item
            path = output.with_name(f"{output.stem}-{provider}-profile.json")
            if path.exists():
                raise ValueError("fresh native profile required")
            result = subprocess.run(["py-spy", "record", "--pid", str(owner["pid"]), "--nonblocking",
                                     "--format", "speedscope", "--rate", "49", "--duration", "5", "--output", str(path)],
                                    text=True, capture_output=True)
            body = json.loads(path.read_text()) if path.exists() else {}
            return {"provider": provider, "owner": owner, "profiler_exit": result.returncode,
                    "helper_samples": helper_samples(body), "profile": str(path), "log": result.stdout + result.stderr}

        with ThreadPoolExecutor(max_workers=3) as pool:
            rows = list(pool.map(profile, owners.items()))
    same = unit_identity(UNIT) == unit and native_owners(unit, code_not_before) == owners
    if (admission != validate_code(comparison)
            or protected != {unit: unit_identity(unit) for unit in PROTECTED}):
        raise ValueError("code or protected native owner changed during acceptance")
    accepted = len(rows) == 3 and same and all(row["profiler_exit"] == 0 and row["helper_samples"] > 0 for row in rows)
    torch_module = sys.modules.get("torch")
    cuda_initialized = bool(torch_module is not None and torch_module.cuda.is_initialized())
    receipt = {"contract": "three_cex_native_tail_helper_execution_v1",
               "state": "accepted_native_helper_execution" if accepted else "awaiting_native_helper_execution",
               "observed_at_utc": datetime.now(UTC).isoformat(), **admission,
               "unit": unit, "native_owners": owners, "same_native_owners": same, "rows": rows,
               "protected_owners_unchanged": True, "provider_calls": 0, "source_writes": 0,
               "services_restarted": False, "cuda_initialized": cuda_initialized,
               "all_history_complete_claim": False, "native_lifecycle_complete_claim": False}
    atomic_write_json(output, receipt, durable=True)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wait-seconds", type=float, default=180)
    args = parser.parse_args()
    receipt = verify(args.comparison.resolve(), args.output.absolute(), wait_seconds=args.wait_seconds)
    print(json.dumps({"state": receipt["state"], "same_native_owners": receipt["same_native_owners"],
                      "rows": [{k: row[k] for k in ("provider", "profiler_exit", "helper_samples")} for row in receipt["rows"]]}))
    return 0 if receipt["state"] == "accepted_native_helper_execution" else 75


if __name__ == "__main__":
    raise SystemExit(main())
