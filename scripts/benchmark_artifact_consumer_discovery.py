"""Compare complete service-path discovery in fresh, network-disabled workers.

The reference is the preserved original artifact_consumers module, not another
implementation of its rules. Every returned field and ordered path must match;
no source payload, model or service is changed by this benchmark.
"""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
import hashlib
from importlib.machinery import PathFinder
import importlib.util
import json
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

from downloader.artifact_io import atomic_write_json

PANEL_ROOT_REFERENCE = "panel-root-v1"
MINUTE_PATH_REFERENCE = "minute-paths-v1"
MINUTE_REFERENCE_FILES = {
    "stockagent/data_sync/artifact_consumers.py": "original_artifact_consumers.py",
    "scripts/rebuild_tw_day_trade_minute_curves.py": "original_minute_curves.py",
    "scripts/rebuild_tw_day_trade_open_price_replay.py": "original_open_price_replay.py",
    "scripts/recheck_tw_day_trade_0901_local_price_gaps.py": "original_price_gap_recheck.py",
}


def capture_minute_reference(reference_root: Path) -> dict:
    """Retain small original code files, never another data/database copy."""
    reference_root.mkdir(parents=True, exist_ok=False)
    sources = {name: (ROOT / name).read_bytes() for name in MINUTE_REFERENCE_FILES}
    for name, filename in MINUTE_REFERENCE_FILES.items():
        with (reference_root / filename).open("xb") as stream:
            stream.write(sources[name])
    proof = {"contract": MINUTE_PATH_REFERENCE, "captured_at_utc": datetime.now(UTC).isoformat(),
             "files": [{"path": name, "sha256": hashlib.sha256(raw).hexdigest()}
                       for name, raw in sources.items()]}
    atomic_write_json(reference_root / "original-code.json", proof)
    if sources != {name: (ROOT / name).read_bytes() for name in sources}:
        raise RuntimeError("source changed during reference capture")
    return {"reference_root": str(reference_root), "contract": MINUTE_PATH_REFERENCE,
            "source_files": len(sources), "source_bytes": sum(map(len, sources.values()))}


def verify_minute_path_relocation(reference_root: Path) -> None:
    """Full AST proof: fixed paths move once; every computation stays intact."""
    proof = json.loads((reference_root / "original-code.json").read_bytes())
    if proof.get("contract") != MINUTE_PATH_REFERENCE:
        raise ValueError("a minute-path reference capture is required")
    originals = {}
    for name, filename in MINUTE_REFERENCE_FILES.items():
        raw = (reference_root / filename).read_bytes()
        record = next(row for row in proof["files"] if row["path"] == name)
        if hashlib.sha256(raw).hexdigest() != record["sha256"]:
            raise ValueError("original minute-path reference changed")
        originals[name] = ast.parse(raw)
    module = "stockagent.live.minute_source_paths"
    declarations = []
    for name, names in (
        ("scripts/rebuild_tw_day_trade_minute_curves.py",
         ("DEFAULT_LOCAL_MINUTE_ROOTS", "DEFAULT_LOCAL_MINUTE_CACHE_ROOTS")),
        ("scripts/rebuild_tw_day_trade_open_price_replay.py", ("DEFAULT_MINUTE_DATA_ROOTS",)),
    ):
        original, current = originals[name], ast.parse((ROOT / name).read_bytes())
        moved = [node for node in original.body if isinstance(node, ast.Assign)
                 and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                 and node.targets[0].id in names]
        imports = [node for node in current.body if isinstance(node, ast.ImportFrom) and node.module == module]
        expected = ([ast.alias(name=key) for key in names] if len(names) == 2 else
                    [ast.alias(name="DEFAULT_LOCAL_MINUTE_ROOTS", asname="DEFAULT_MINUTE_DATA_ROOTS")])
        if (len(moved) != len(names) or len(imports) != 1
                or ast.dump(imports[0]) != ast.dump(ast.ImportFrom(module=module, names=expected, level=0))):
            raise ValueError("one exact minute-path declaration import is required")
        if len(names) == 2:
            declarations = moved
        elif ast.dump(moved[0].value) != ast.dump(declarations[0].value):
            raise ValueError("the original replay/minute defaults did not agree")
        for node in moved:
            original.body.remove(node)
        current.body.remove(imports[0])
        if ast.dump(original) != ast.dump(current):
            raise ValueError("minute computation changed beyond path declaration relocation")
    source = ast.parse((ROOT / "stockagent/live/minute_source_paths.py").read_bytes())
    source.body = [node for node in source.body if not (
        isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
        or isinstance(node, ast.ImportFrom) and node.module == "__future__")]
    expected = ast.Module(body=[ast.parse("from pathlib import Path").body[0], *declarations], type_ignores=[])
    if ast.dump(source) != ast.dump(expected):
        raise ValueError("shared minute declarations differ from the original producer defaults")
    for name, old, aliases in (
        ("stockagent/data_sync/artifact_consumers.py", "scripts.rebuild_tw_day_trade_minute_curves", None),
        ("scripts/recheck_tw_day_trade_0901_local_price_gaps.py",
         "scripts.rebuild_tw_day_trade_open_price_replay",
         [ast.alias(name="DEFAULT_LOCAL_MINUTE_ROOTS", asname="DEFAULT_MINUTE_DATA_ROOTS")]),
    ):
        original = originals[name]
        imports = [node for node in ast.walk(original) if isinstance(node, ast.ImportFrom) and node.module == old]
        if len(imports) != 1:
            raise ValueError("the original metadata path import is ambiguous")
        imports[0].module = module
        if aliases:
            imports[0].names = aliases
        if ast.dump(original) != ast.dump(ast.parse((ROOT / name).read_bytes())):
            raise ValueError("metadata reader changed beyond its canonical path import")


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def verify_relocation(reference_root: Path) -> None:
    """Prove the reference's loaded engine differs only by this extraction."""
    original = ast.parse((reference_root / "original_signal_engine.py").read_text())
    current = ast.parse((ROOT / "stockagent/live/signal_engine.py").read_text())
    helper = next(node for node in original.body if isinstance(node, ast.FunctionDef)
                  and node.name == "_live_panel_disk_cache_root")
    original.body.remove(helper)
    alias = ast.parse("from stockagent.live.market_config import "
                      "live_panel_disk_cache_root as _live_panel_disk_cache_root").body[0]
    aliases = [node for node in current.body if ast.dump(node) == ast.dump(alias)]
    if len(aliases) != 1:
        raise ValueError("one canonical compatibility import is required")
    current.body.remove(aliases[0])
    if ast.dump(original) != ast.dump(current):
        raise ValueError("the engine changed beyond the pure resolver extraction")
    original_config = ast.parse((reference_root / "original_market_config.py").read_text())
    current_config = ast.parse((ROOT / "stockagent/live/market_config.py").read_text())
    moved = next(node for node in current_config.body if isinstance(node, ast.FunctionDef)
                 and node.name == "live_panel_disk_cache_root")
    current_config.body.remove(moved)
    current_config.body.remove(next(node for node in current_config.body
                                    if ast.dump(node) == ast.dump(ast.parse("import os").body[0])))
    moved.body = moved.body[1:]  # Only the public docstring is new.
    moved.name = helper.name
    if (ast.dump(moved) != ast.dump(helper)
            or ast.dump(original_config) != ast.dump(current_config)):
        raise ValueError("the original resolver or live configuration semantics changed")


class MetadataOnlyImports:
    def __init__(self, extra_forbidden=()):
        self.forbidden = {"torch", "cupy", "shioaji", "stockagent.live.signal_engine", *extra_forbidden}

    def find_spec(self, fullname, path=None, target=None):
        if fullname in self.forbidden or fullname.split(".")[0] in self.forbidden:
            # Polars probes availability with find_spec without importing the
            # optional dependency. Preserve that probe; prohibit actual loading.
            spec = PathFinder.find_spec(fullname, path, target)
            if spec is not None:
                spec.loader = self
                return spec
        return None

    def create_module(self, spec):
        raise AssertionError(f"metadata lookup imported a compute/provider owner: {spec.name}")

    def exec_module(self, module):
        raise AssertionError(f"metadata lookup imported a compute/provider owner: {module.__name__}")


def run_case(reference_root: Path, label: str, reference_contract=PANEL_ROOT_REFERENCE) -> dict:
    def no_network(*args, **kwargs):
        raise AssertionError("metadata lookup must not connect to any provider")

    socket.socket.connect = no_network
    socket.socket.connect_ex = no_network
    socket.create_connection = no_network
    reference = reference_root / "original_artifact_consumers.py"
    proof = json.loads((reference_root / "original-code.json").read_bytes())
    record = next(row for row in proof["files"]
                  if row["path"] == "stockagent/data_sync/artifact_consumers.py")
    if hashlib.sha256(reference.read_bytes()).hexdigest() != record["sha256"]:
        raise ValueError("original consumer resolver changed")
    started, cpu = time.perf_counter(), time.process_time()
    if label == "baseline":
        spec = importlib.util.spec_from_file_location("original_artifact_consumers", reference)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    else:
        # Canonical config validation still uses NumPy/Polars. This change
        # removes accidental valuation/replay imports, not required validators.
        extra = {"scripts.rebuild_tw_day_trade_minute_curves",
                 "scripts.rebuild_tw_day_trade_open_price_replay",
                 "stockagent.live.tw_day_trade_simulation"} if reference_contract == MINUTE_PATH_REFERENCE else ()
        sys.meta_path.insert(0, MetadataOnlyImports(extra))
        from stockagent.data_sync import artifact_consumers as module
    paths = module.service_artifact_paths(ROOT)
    elapsed, spent = time.perf_counter() - started, time.process_time() - cpu
    torch = sys.modules.get("torch")
    cuda_initialized = bool(torch is not None and torch.cuda.is_initialized())
    if cuda_initialized or (label == "baseline" and torch is None and reference_contract == PANEL_ROOT_REFERENCE):
        raise RuntimeError("reference dependency boundary was not exercised CUDA-cold")
    return {"label": label, "wall_seconds": elapsed, "cpu_seconds": spent,
            "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            "torch_imported": torch is not None, "cuda_initialized": cuda_initialized,
            "numeric_modules_imported": sorted(name for name in ("numpy", "polars", "pandas") if name in sys.modules),
            "paths_count": len(paths), "paths_sha256": digest(paths), "paths": paths}


def compare(reference_root: Path, output: Path, reference_contract=PANEL_ROOT_REFERENCE) -> dict:
    if output.exists() or output.is_symlink():
        raise FileExistsError("preserve earlier benchmark evidence")
    if reference_contract == MINUTE_PATH_REFERENCE:
        verify_minute_path_relocation(reference_root)
    else:
        verify_relocation(reference_root)
    sources = [Path(__file__).resolve(), *[ROOT / "stockagent" / name for name in (
        "data_sync/artifact_consumers.py", "live/market_config.py", "live/signal_engine.py")]]
    if reference_contract == MINUTE_PATH_REFERENCE:
        sources += [ROOT / name for name in MINUTE_REFERENCE_FILES if name.startswith("scripts/")]
        sources.append(ROOT / "stockagent/live/minute_source_paths.py")
    before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    cases = []
    sequence = ("baseline", "candidate", "candidate", "baseline") * 2
    targets = [output.parent / f"{output.stem}-{index}-{label}.json"
               for index, label in enumerate(sequence)]
    if any(path.exists() or path.is_symlink() for path in targets):
        raise FileExistsError("preserve all earlier case evidence")
    for target, label in zip(targets, sequence):
        process = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--reference-root", str(reference_root),
             "--label", label, "--reference-contract", reference_contract], capture_output=True, text=True, timeout=90,
        )
        if process.returncode:
            atomic_write_json(target, {"state": "failed", "label": label,
                                       "returncode": process.returncode,
                                       "stderr": process.stderr[-16000:]})
            raise RuntimeError(f"{label} discovery failed: {process.stderr[-16000:]}")
        row = json.loads(process.stdout)
        if row["label"] != label or (cases and row["paths"] != cases[0]["paths"]):
            raise RuntimeError("complete ordered service dependencies changed")
        atomic_write_json(target, row)
        cases.append(row)
    if before != {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}:
        raise RuntimeError("shared source changed during the comparison")
    if reference_contract == MINUTE_PATH_REFERENCE:
        verify_minute_path_relocation(reference_root)
    means = {label: {metric: statistics.mean(row[metric] for row in cases if row["label"] == label)
                     for metric in ("wall_seconds", "cpu_seconds", "peak_rss_bytes")}
             for label in ("baseline", "candidate")}
    result = {"contract": ("complete_artifact_consumer_minute_paths_abba_v1"
                           if reference_contract == MINUTE_PATH_REFERENCE else "complete_artifact_consumer_discovery_abba_v1"),
              "reference_contract": reference_contract, "state": "accepted",
              "observed_at_utc": datetime.now(UTC).isoformat(), "source_sha256": before,
              "complete_paths_count": cases[0]["paths_count"],
              "complete_paths_sha256": cases[0]["paths_sha256"], "all_paths_equal": True,
              "full_ast_only_relocates_path_declarations": True,
              "engine_ast_only_relocates_original_path_helper": reference_contract == PANEL_ROOT_REFERENCE,
              "means": means, "fresh_processes": len(cases),
              "baseline_torch_imported": all(row["torch_imported"] for row in cases
                                             if row["label"] == "baseline"),
              "candidate_compute_and_provider_imports_forbidden": True,
              "candidate_valuation_engine_imports_forbidden": reference_contract == MINUTE_PATH_REFERENCE,
              "case_paths": [str(path) for path in targets],
              "cuda_initialized": any(row["cuda_initialized"] for row in cases),
              "network_connections_forbidden": True,
              "source_payload_writes": 0, "service_controls": 0,
              "scope": "complete metadata resolver; not an entire storage lifecycle",
              "caveats": ["Nonexclusive host; OS page cache not cleared.",
                          ("Reference imports the full minute tool through an AST-proven declaration-only relocation."
                           if reference_contract == MINUTE_PATH_REFERENCE else
                           "Reference loads current engine with the unchanged relocated path helper.")]}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--label", choices=("baseline", "candidate"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--capture", action="store_true", help="capture a small minute-path source reference before changing it")
    parser.add_argument("--reference-contract", choices=(PANEL_ROOT_REFERENCE, MINUTE_PATH_REFERENCE), default=PANEL_ROOT_REFERENCE)
    args = parser.parse_args()
    if args.capture:
        if args.label or args.output or args.reference_contract != MINUTE_PATH_REFERENCE:
            parser.error("capture requires minute-paths-v1 without a label/output")
        print(json.dumps(capture_minute_reference(args.reference_root), sort_keys=True))
        return
    if (args.label is None) == (args.output is None):
        parser.error("choose one fresh worker or a full comparison output")
    result = (run_case(args.reference_root, args.label, args.reference_contract) if args.label else
              compare(args.reference_root, args.output, args.reference_contract))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
