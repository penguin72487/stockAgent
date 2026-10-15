"""Restore saved original tail callers only inside an isolated CPU benchmark.

Compile the actual saved function ASTs, not hand-copied finance equations.
Unmodified canonical helpers retain their current namespace. Every complete
panel field must independently match the saved full-workflow output oracle;
restoring these functions alone is not an equality or performance acceptance.
"""
from __future__ import annotations

import ast
import __future__
import json
from pathlib import Path
import stat

from downloader.artifact_io import sha256_file

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ("stockagent/data/panel.py", "stockagent/live/signal_engine.py")


def original_callers_identity(receipt_path: Path) -> dict:
    """Private immutable code snapshots, with exact source/path/hash fencing."""
    receipt_path = Path(receipt_path).resolve(strict=True)
    private = (ROOT / "artifacts/benchmarks").resolve()
    if not receipt_path.is_relative_to(private) or receipt_path.stat().st_size > 65536:
        raise ValueError("original caller receipt must be bounded private benchmark evidence")
    before = sha256_file(receipt_path)
    value = json.loads(receipt_path.read_bytes())
    if (value.get("contract") != "canonical_live_tail_callers_original_v1"
            or value.get("status") != "frozen"
            or len(value.get("proofs", [])) != len(SOURCES)):
        raise ValueError("exact frozen original caller receipt required")
    proofs = {}
    for proof in value["proofs"]:
        name = str(Path(proof["source"]).absolute())
        if name not in {str(ROOT / item) for item in SOURCES} or name in proofs:
            raise ValueError("original caller source is missing, repeated or unexpected")
        path = Path(proof["path"])
        if (path.is_symlink() or not path.resolve(strict=True).is_relative_to(receipt_path.parent)
                or not stat.S_ISREG(path.stat().st_mode) or path.stat().st_mode & 0o222
                or path.stat().st_size != proof["bytes"] or sha256_file(path) != proof["sha256"]):
            raise ValueError("original caller snapshot is mutable, escaped or changed")
        proofs[name] = {key: proof[key] for key in ("path", "source", "sha256", "bytes")}
    if sha256_file(receipt_path) != before:
        raise ValueError("original caller receipt changed while being read")
    return {"receipt": str(receipt_path), "receipt_sha256": before, "proofs": proofs,
            "scope": "saved original BASE tail callers; complete output oracle still required"}


def _function_namespace(path: Path, namespace: dict, names: tuple[str, ...]) -> dict:
    tree = ast.parse(path.read_bytes(), filename=str(path))
    selected = [node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name in names]
    if (len(selected) != len(names) or {node.name for node in selected} != set(names)
            or any(node.decorator_list for node in selected)):
        raise ValueError("saved original caller definitions are incomplete or decorated")
    # All nested helpers stay part of their original function AST. Other
    # top-level code must never execute while importing a saved reference.
    result = dict(namespace)
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, str(path), "exec", flags=__future__.annotations.compiler_flag), result)
    return result


def verify_unmodified_helpers(identity: dict) -> None:
    """The restored functions may reuse only unchanged surrounding helpers."""
    permitted = {
        SOURCES[0]: {"_load_symbol_arrays_pyarrow_tail", "build_tail_panel"},
        SOURCES[1]: {"_build_panel_from_stable_source", "clear_live_panel_memory_cache",
                     "_live_raw_tail_source_cache", "_LIVE_RAW_TAIL_SOURCE_CACHE",
                     "_LIVE_RAW_TAIL_SOURCE_CACHE_LOCK", "_LIVE_RAW_TAIL_SOURCE_CACHE_MAX_BYTES",
                     # This oracle covers complete BASE construction, not idle
                     # teardown. Both trials share the current teardown; all
                     # data/model builders and the saved full output stay fenced.
                     "clear_live_inference_memory_cache", "release_idle_live_cuda_cache",
                     "_live_inference_cpu_cache_retained", "_LIVE_CUDA_CACHE_RELEASE_PENDING"},
    }
    def rest(path, ignored):
        nodes = []
        for node in ast.parse(path.read_bytes(), filename=str(path)).body:
            name = getattr(node, "name", None)
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                name = node.target.id
            elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                name = node.targets[0].id
            if name not in ignored:
                nodes.append(ast.dump(node, include_attributes=False))
        return nodes
    for relative, ignored in permitted.items():
        current = ROOT / relative
        saved = Path(identity["proofs"][str(current)]["path"])
        if rest(current, ignored) != rest(saved, ignored):
            raise ValueError("unmodified original caller helpers changed outside the raw-cache hook")


def original_stable_tail_builder(receipt_path: Path):
    """Return the original engine caller bound to both original panel callers."""
    from stockagent.data import panel
    from stockagent.live import signal_engine

    before = original_callers_identity(receipt_path)
    verify_unmodified_helpers(before)
    saved_panel = Path(before["proofs"][str(ROOT / SOURCES[0])]["path"])
    saved_engine = Path(before["proofs"][str(ROOT / SOURCES[1])]["path"])
    namespace = _function_namespace(saved_panel, vars(panel), (
        "_load_symbol_arrays_pyarrow_tail", "build_tail_panel"))
    engine_namespace = {**vars(signal_engine), "build_tail_panel": namespace["build_tail_panel"]}
    original = _function_namespace(saved_engine, engine_namespace, ("_build_panel_from_stable_source",))
    if original_callers_identity(receipt_path) != before:
        raise ValueError("saved original callers changed while compiling references")
    return original["_build_panel_from_stable_source"]
