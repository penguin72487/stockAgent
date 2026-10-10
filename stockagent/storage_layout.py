"""Node-local training placement; immutable provenance stays byte-for-byte intact."""
from __future__ import annotations

from pathlib import Path


ROLE_DIRECTORIES = {
    "training": "artifacts/markets",
    "prepared": "artifacts/data_preparation",
    "cache": "artifacts/cache",
    "code": "artifacts/code_releases",
    "benchmark": "artifacts/benchmarks",
    "operations": "artifacts/operations",
    "source": "data_tw_index_futures/preparation_sources",
}


def node_root(root: Path) -> Path:
    """Frozen build-source trees use their owning checkout for mutable work."""
    root = Path(root).resolve()
    for parent in (root, *root.parents):
        if (parent / '.git').exists() and (parent / 'scripts/runtime_env.sh').is_file():
            return parent
    return root


def admit_output(path: Path, role: str) -> Path:
    """Reserve markets for results, also checking aliases into that namespace.

    Caller-selected paths elsewhere remain supported. This is placement
    admission, never permission to delete a cache or relocate an active run.
    """
    if role not in ROLE_DIRECTORIES:
        raise ValueError(f"unknown training storage role: {role}")
    path = Path(path).expanduser().absolute()
    for view in (path, path.resolve()):
        if role != "code" and any(a == 'artifacts' and b == 'code_releases'
                                  for a,b in zip(view.parts,view.parts[1:])):
            raise ValueError(f"code_releases contains frozen code; place {role} work under {ROLE_DIRECTORIES[role]}")
        if role != "training" and any(
            a == "artifacts" and b == "markets"
            for a, b in zip(view.parts, view.parts[1:])
        ):
            raise ValueError(
                f"artifacts/markets is reserved for training results; "
                f"place {role} output under {ROLE_DIRECTORIES[role]}: {path}"
            )
    return path


def runtime_cache_path(path: str | Path, root: Path) -> Path:
    """Localize a legacy data-cache setting in a new mutable launch copy."""
    owner = node_root(root)
    path = Path(path).expanduser()
    path = path if path.is_absolute() else owner / path
    for view in (path, path.resolve()):
        for index, (a, b) in enumerate(zip(view.parts, view.parts[1:])):
            if a == 'artifacts' and b == 'markets':
                suffix = view.parts[index + 2:]
                if not suffix:
                    raise ValueError('a runtime cache requires an experiment-specific path')
                return admit_output(owner / 'artifacts/cache' / Path(*suffix), 'cache')
    return admit_output(path, 'cache')


def relocate_configuration(value, moves: list[dict], root: Path):
    """Localize an audited *copy*, preserving source manifests/config bytes.

    Match path boundaries, never replace substrings or resolve moving latest.
    Training output stays at its original path; only explicitly enrolled
    storage inputs and code/config dependency locations can change.
    """
    if isinstance(value, dict):
        return {k: relocate_configuration(v, moves, root) for k, v in value.items()}
    if isinstance(value, list):
        return [relocate_configuration(v, moves, root) for v in value]
    if isinstance(value, str):
        for move in sorted(moves, key=lambda m: len(m["source"]), reverse=True):
            if move.get("action", "move") != "move":
                continue
            for old, new in ((move["source"], move["destination"]),
                             (str(root / move["source"]), str(root / move["destination"]))):
                if value == old or value.startswith(old + "/"):
                    return new + value[len(old):]
    return value
