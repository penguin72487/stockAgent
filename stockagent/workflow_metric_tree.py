"""Optional, source-bound C++ walker for decoded benchmark metric trees.

Numerical values remain borrowed Python objects. The caller owns redaction,
time-field matching, finite-number validation and the complete Python fallback.
This module never installs packages or changes service/training state.
"""
from __future__ import annotations

import hashlib
import errno
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time

from downloader.artifact_io import atomic_write_json


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_backend(root: Path, mode: str = "auto"):
    """Return an exact source/ABI-bound module, or the recorded Python fallback.

    Auto only uses an already verified cache. Explicit ``cpp`` builds an absent
    cache with the runtime's compiler and Python headers; compile time belongs
    to the calling workflow. Publication is one atomic directory rename.
    """
    started = time.perf_counter()
    if mode not in {"auto", "python", "cpp"}:
        raise ValueError("unknown metric backend")
    info = {"requested": mode, "selected": "python", "scope": "decoded benchmark field extraction only"}
    if mode == "python":
        info["initialization_seconds"] = time.perf_counter() - started
        return None, info
    source = Path(__file__).with_suffix(".cpp")
    try:
        source_raw = source.read_bytes()
    except OSError:
        if mode == "cpp":
            raise
        info.update(reason="native_source_unavailable", initialization_seconds=time.perf_counter() - started)
        return None, info
    source_sha = hashlib.sha256(source_raw).hexdigest()
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    identity = {"source_sha256": source_sha, "python": sys.version,
                "cache_tag": sys.implementation.cache_tag,
                "soabi": sysconfig.get_config_var("SOABI"), "machine": platform.machine()}
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    parent = root / "artifacts/benchmarks/workflow_speed_metric_backend"
    cache = parent / key
    module_path = cache / ("_workflow_metric_tree" + suffix)
    if not cache.exists():
        if mode == "auto":
            info.update(reason="source_bound_cache_absent", initialization_seconds=time.perf_counter() - started)
            return None, info
        compiler = shutil.which("g++") or shutil.which("c++")
        if compiler is None:
            raise RuntimeError("explicit C++ metric backend requires a C++ compiler")
        parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".build-", dir=parent) as directory:
            stage = Path(directory)
            snapshot = stage / source.name
            snapshot.write_bytes(source_raw)
            target = stage / module_path.name
            command = [compiler, "-O3", "-std=c++17", "-shared", "-fPIC",
                       "-I" + sysconfig.get_path("include"), str(snapshot), "-o", str(target)]
            try:
                process = subprocess.run(command, capture_output=True, text=True, timeout=120)
                log, code = process.stdout + process.stderr, process.returncode
            except (OSError, subprocess.TimeoutExpired) as exc:
                log, code = type(exc).__name__ + ": " + str(exc) + "\n", None
                for partial in (getattr(exc, "stdout", None), getattr(exc, "stderr", None)):
                    if partial:
                        log += partial.decode(errors="replace") if isinstance(partial, bytes) else partial
            (stage / "compile.log").write_text(log)
            if code != 0:
                # Preserve failed build evidence, including source and log.
                atomic_write_json(stage / "failed_build_receipt.json", {
                    "contract": "source_bound_metric_tree_cpp_build_failure_v1", "identity": identity,
                    "command": command, "exit_code": code})
                failed = parent / ("failed-" + stage.name.removeprefix(".build-"))
                os.rename(stage, failed)
                raise RuntimeError("C++ metric backend compilation failed; retained at " + str(failed))
            receipt = {"contract": "source_bound_metric_tree_cpp_build_v1", "identity": identity,
                       "module_sha256": _sha(target), "compiler": subprocess.check_output(
                           [compiler, "--version"], text=True).splitlines()[0],
                       "command": command, "compiled_this_call": True}
            atomic_write_json(stage / "build_receipt.json", receipt)
            try:
                os.rename(stage, cache)
            except OSError as exc:
                # A concurrent same-identity builder may publish first. Its
                # receipt and module still undergo the checks below.
                if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
        info["compiled_this_call"] = True
    try:
        receipt = json.loads((cache / "build_receipt.json").read_bytes())
        if receipt["identity"] != identity or receipt["module_sha256"] != _sha(module_path):
            raise ValueError("native metric source/ABI/module identity mismatch")
    except (OSError, ValueError, KeyError) as exc:
        if mode == "cpp":
            raise RuntimeError("explicit native metric cache failed identity verification") from exc
        info.update(reason="source_bound_cache_rejected", error_type=type(exc).__name__,
                    initialization_seconds=time.perf_counter() - started)
        return None, info
    try:
        spec = importlib.util.spec_from_file_location("_workflow_metric_tree", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except ImportError:
        if mode == "cpp":
            raise
        info.update(reason="source_bound_module_unloadable", initialization_seconds=time.perf_counter() - started)
        return None, info
    info.update(selected="cpp", source_path=str(source), source_sha256=source_sha, module_sha256=receipt["module_sha256"],
                module_path=str(module_path),
                build_receipt=str(cache / "build_receipt.json"),
                build_receipt_sha256=_sha(cache / "build_receipt.json"),
                initialization_seconds=time.perf_counter() - started)
    return module, info
