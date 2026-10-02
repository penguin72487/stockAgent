"""Fresh-interpreter ABBA for public readers against exact frozen source.

This measures import wall/CPU time and process peak RSS, NOT data freshness,
HTTP latency or a whole snapshot workflow. No source/ledger writes, provider
queries, service actions or OS-cache clearing. Imports cannot open sockets.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path
import resource
import socket
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402


FROZEN_MODULES = (
    "stockagent.live.data_monitor_dashboard",
    "stockagent.live.finlab_dashboard",
    "scripts.finlab_release_gate",
    "scripts.download_finlab_history",
    "downloader.download_finmind_complement",
)
TARGETS = (
    "scripts.snapshot_data_refresh_services",
    "stockagent.live.data_monitor_dashboard",
    "stockagent.live.finlab_dashboard",
)
NEW_CONTRACTS = (
    "stockagent/data/finlab_acquisition_contract.py",
    "downloader/finmind_catalog.py",
)


def _source_path(module: str) -> Path:
    return ROOT / (module.replace(".", "/") + ".py")


def _hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def capture_baseline(output: Path) -> None:
    if output.exists():
        raise ValueError("refuse to replace a captured baseline")
    modules = {}
    for name in FROZEN_MODULES:
        body = _source_path(name).read_bytes()
        modules[name] = {"sha256": _hash(body), "source": body.decode("utf-8")}
    atomic_write_json(output, {"schema_version": 1, "modules": modules})


def _read_baseline(path: Path) -> dict:
    body = path.read_bytes()
    if len(body) > 8 << 20:
        raise ValueError("baseline exceeds bounded size")
    payload = json.loads(body)
    modules = payload.get("modules")
    if payload.get("schema_version") != 1 or not isinstance(modules, dict):
        raise ValueError("invalid baseline")
    if set(modules) != set(FROZEN_MODULES):
        raise ValueError("baseline module set mismatch")
    for record in modules.values():
        if not isinstance(record, dict) or not isinstance(record.get("source"), str):
            raise ValueError("invalid frozen source")
        if _hash(record["source"].encode("utf-8")) != record.get("sha256"):
            raise ValueError("frozen source SHA mismatch")
    return payload


class _FrozenLoader(importlib.abc.Loader):
    def __init__(self, name: str, source: str):
        self.path, self.source = _source_path(name), source

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        module.__file__ = str(self.path)
        exec(compile(self.source, str(self.path), "exec"), module.__dict__)


class _FrozenFinder(importlib.abc.MetaPathFinder):
    def __init__(self, modules: dict):
        self.modules = modules

    def find_spec(self, fullname, path=None, target=None):
        record = self.modules.get(fullname)
        if record is None:
            return None
        return importlib.util.spec_from_loader(
            fullname, _FrozenLoader(fullname, record["source"]),
            origin=str(_source_path(fullname)),
        )


def _contract_projection() -> dict:
    # Use already-loaded rules, never import a worker to gather measurements.
    policy = sys.modules.get("stockagent.data.finlab_acquisition_contract")
    if policy is None:
        policy = sys.modules["scripts.download_finlab_history"]
    cases = []
    for status in (*policy.ATTEMPT_RETRY_SECONDS, "authentication_failed"):
        for streak in (1, 4, True, -1):
            for downloaded in (False, True):
                result = policy.attempt_retry_at({
                    "status": status, "failure_streak": streak,
                    "attempted_at_utc": "2026-10-01T00:00:00+00:00",
                }, downloaded=downloaded)
                cases.append(result.isoformat() if result else None)
    projection = {
        "deferred": policy.AUTOMATICALLY_DEFERRED_REASONS,
        "retry_seconds": policy.ATTEMPT_RETRY_SECONDS, "retry_cases": cases,
        "safe_stems": [policy.safe_stem(key) for key in (
            "price:收盤價", "../../trading_attention:測試", "tw_tick:2330", "extra:field",
        )],
    }
    dashboard = sys.modules.get("stockagent.live.data_monitor_dashboard")
    if dashboard is not None:
        catalog = sys.modules.get("downloader.finmind_catalog")
        if catalog is None:
            catalog = sys.modules["downloader.download_finmind_complement"]
        catalog_fields = {}
        for name in (
            "SNAPSHOTS", "GLOBAL_HISTORY", "PER_ID_REQUIRED", "CURRENCIES",
            "GLOBAL_START_YEAR", "BULK_GLOBAL_HISTORY", "GLOBAL_RELEASE_HOUR_TAIPEI",
            "TW_SYMBOL_HISTORY", "LONG_INSTITUTIONAL", "WIDE_INSTITUTIONAL",
            "INSTITUTIONAL_NAMES", "DERIVATIVE_HISTORY", "GLOBAL_EQUITY_HISTORY",
            "LIVE_SNAPSHOT_ENDPOINTS", "DERIVATIVE_SNAPSHOTS", "FIXED_ID_HISTORY", "ALL_DATASETS",
        ):
            value = getattr(catalog, name)
            catalog_fields[name] = sorted(value) if isinstance(value, frozenset) else value
        projection.update(
            complete_catalog=catalog_fields,
            complement=dashboard.FINMIND_COMPLEMENT_DATASETS,
            snapshots=dashboard.FINMIND_COMPLEMENT_SNAPSHOTS,
            wide=dashboard.FINMIND_DERIVED_WIDE,
            sponsor=[vars(source) for source in dashboard.FINMIND_SPONSOR_SOURCES],
            session_datasets=sorted(dashboard.FINMIND_SESSION_DAY_DATASETS),
        )
    return projection


def probe(target: str, *, baseline: Path | None) -> dict:
    if target not in TARGETS:
        raise ValueError("unknown public reader")
    # Both variants use the same source loader: an old uncached source compile
    # must not be compared with a current .pyc hit and called an import gain.
    modules = (
        _read_baseline(baseline)["modules"] if baseline is not None else
        {name: {"source": _source_path(name).read_text(encoding="utf-8")}
         for name in FROZEN_MODULES}
    )
    sys.meta_path.insert(0, _FrozenFinder(modules))

    def no_connect(*_args, **_kwargs):
        raise RuntimeError("public import attempted network I/O")

    socket.socket.connect = socket.socket.connect_ex = no_connect
    socket.create_connection = no_connect
    sys.dont_write_bytecode = True
    wall_start, cpu_start = time.perf_counter_ns(), time.process_time_ns()
    importlib.import_module(target)
    result = {
        "import_wall_ms": (time.perf_counter_ns() - wall_start) / 1e6,
        "import_cpu_ms": (time.process_time_ns() - cpu_start) / 1e6,
        "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "loaded_modules": {name: name in sys.modules for name in (
            "numpy", "pyarrow", "pandas", "requests", "finlab", "shioaji",
            "scripts.download_finlab_history", "downloader.download_finmind_complement",
            "downloader.download_finmind_sponsor",
        )},
    }
    result["contract_sha256"] = _hash(json.dumps(
        _contract_projection(), sort_keys=True, ensure_ascii=False, default=str,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8"))
    return result


def benchmark(baseline: Path, *, rounds: int = 1) -> dict:
    if not 1 <= rounds <= 4:
        raise ValueError("rounds must be between 1 and 4")
    payload = _read_baseline(baseline)
    sources = {_source_path(name) for name in (*FROZEN_MODULES, *TARGETS)}
    sources.update(ROOT / path for path in NEW_CONTRACTS)
    sources.add(Path(__file__))
    code_before = {str(path.relative_to(ROOT)): _hash(path.read_bytes()) for path in sources}
    baseline_before = _hash(baseline.read_bytes())
    trials, summaries = [], {}
    for target in TARGETS:
        for _ in range(rounds):
            for variant in ("baseline", "current", "current", "baseline"):
                command = [sys.executable, str(Path(__file__).resolve()), "--probe", target]
                if variant == "baseline":
                    command.extend(["--baseline", str(baseline.resolve())])
                result = subprocess.run(
                    command, check=True, cwd=ROOT, capture_output=True, text=True,
                    timeout=30, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                )
                trials.append({"target": target, "variant": variant, **json.loads(result.stdout)})
        cohort = [trial for trial in trials if trial["target"] == target]
        if len({trial["contract_sha256"] for trial in cohort}) != 1:
            raise ValueError("public source/retry contract parity failed")
        summaries[target] = {
            variant: {key: statistics.median(
                trial[key] for trial in cohort if trial["variant"] == variant
            ) for key in ("import_wall_ms", "import_cpu_ms", "process_peak_rss_kib")}
            for variant in ("baseline", "current")
        }
    if code_before != {str(path.relative_to(ROOT)): _hash(path.read_bytes()) for path in sources}:
        raise ValueError("measured code changed during ABBA")
    if _hash(baseline.read_bytes()) != baseline_before:
        raise ValueError("baseline changed during ABBA")
    return {
        "schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
        "scope": "public_reader_imports_only_not_whole_workflow",
        "fresh_interpreter": True, "os_cache_controlled": False,
        "frozen_module_source_loader_used_for_both_variants": True,
        "network_connects_prohibited": True, "production_actions": False,
        "baseline_sha256": baseline_before,
        "baseline_code_sha256": {name: record["sha256"] for name, record in payload["modules"].items()},
        "current_code_sha256": code_before, "trials": trials, "medians": summaries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-baseline", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--probe", choices=TARGETS)
    args = parser.parse_args()
    if args.capture_baseline is not None:
        capture_baseline(args.capture_baseline)
    elif args.probe:
        print(json.dumps(probe(args.probe, baseline=args.baseline), sort_keys=True))
    elif args.baseline is None or args.output is None:
        parser.error("ABBA requires --baseline and --output")
    else:
        result = benchmark(args.baseline, rounds=args.rounds)
        atomic_write_json(args.output, result)
        print(json.dumps(result["medians"], sort_keys=True))


if __name__ == "__main__":
    main()
