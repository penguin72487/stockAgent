"""Node-local mutable service paths, separate from reproducible artifacts.

Legacy names remain usable during an inode-preserving placement migration.
Only the exact enrolled compatibility links are resolved here; arbitrary
redirects and symlinks below a service root are still checked by each reader.
"""
from __future__ import annotations

import os
from pathlib import Path

from stockagent.storage_layout import node_root


def service_moves(root: Path) -> dict[Path, Path]:
    root = node_root(root)
    destination = root / "service_data"
    return {
        root / "artifacts/live": destination / "live",
        root / "artifacts/cache": destination / "cache",
        root / "artifacts/operations": destination / "operations",
        root / "artifacts/discord_bot": destination / "discord_bot",
        Path("/srv/stockagent-live"): destination / "stockagent-live",
        Path("/var/lib/stockagent"): destination / "stockagent",
    }


def canonical_service_path(path: str | Path, root: Path | None = None) -> Path:
    """Resolve a deployed compatibility root without following other links.

    This never changes an immutable manifest or chooses between two copies.
    An unexpected target is an error, rather than an excuse to accept any
    symlink. Callers that forbid leaf/descendant links retain those checks.
    """
    path = Path(path).expanduser().absolute()
    if ".." in path.parts:
        raise ValueError("service path must not contain parent traversal")
    root = Path(__file__).resolve().parents[1] if root is None else root
    for legacy, destination in service_moves(root).items():
        if path == legacy or legacy in path.parents:
            if not legacy.is_symlink():
                if (destination.is_dir() and legacy.is_dir()
                        and os.path.samestat(legacy.stat(), destination.stat())):
                    if destination.resolve() != destination:
                        raise ValueError("redirected service bind target")
                    # Native control interfaces keep their original parent
                    # traversal semantics, including external relative links
                    # embedded in existing environments and signed journals.
                    if legacy in {Path("/var/lib/stockagent"), node_root(root) / "artifacts/operations"}:
                        return path
                    return destination / path.relative_to(legacy)
                return path
            if (os.readlink(legacy) != str(destination) or not destination.is_dir()
                    or destination.resolve() != destination):
                raise ValueError("unregistered service compatibility target")
            return destination / path.relative_to(legacy)
    return path


def service_path(relative: str | Path, *, area: str = "live", root: Path | None = None) -> Path:
    """Choose the existing single state owner; new nodes use service_data.

    Before a migration, keep using its real legacy directory. Never create a
    second empty ledger just because the new namespace has been introduced.
    """
    root = node_root(Path(__file__).resolve().parents[1] if root is None else root)
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("service suffix must be relative and stay inside its area")
    by_area = {target.name: (old, target) for old, target in service_moves(root).items()}
    if area not in by_area:
        raise ValueError(f"unknown service storage area: {area}")
    legacy, destination = by_area[area]
    if legacy.exists() or legacy.is_symlink():
        selected = canonical_service_path(legacy, root)
        if (destination.exists() and selected != destination
                and not os.path.samestat(selected.stat(), destination.stat())):
            raise ValueError("two independent service state roots require reconciliation")
    else:
        selected = destination
    return selected / relative
