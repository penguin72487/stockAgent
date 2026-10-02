"""Manually compact byte-identical immutable training-panel payloads only.

This is not source eviction, cold GC, or a new automatic cleanup daemon. Every
logical cache path remains. Reuse canonical SHA-256/signature rechecks and the
panel writer locks; reject service caches, active processes and redirected paths.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import replace
from datetime import UTC, datetime
import fcntl
import json
import os
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from stockagent.data_sync.artifact_dedup import (
    DEFAULT_EXCLUDED_SUFFIXES,
    apply_duplicate_groups,
    find_duplicate_groups,
    groups_as_json,
)
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many


# Explicitly inactive research namespaces, never arbitrary paths or the whole
# artifacts tree. New service use must remove a name from this list first.
RESEARCH_PANELS = frozenset(
    {
        "tw_public_preopen_raw_v1",
        "tw_public_preopen_raw_long_history_v1",
        "tw_public_preopen_pit_v7",
        "tw_public_preopen_pit_v7_649",
        "tw_public_preopen_pit_pinned_20260916",
        "tw_public_verified_features_20260917",
        "tw_public_no_unvintaged_macro_20260917",
    }
)


def checked_panel_roots(root: Path, names: list[str]) -> list[Path]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("cache parent must be a real directory")
    root = root.absolute()
    if root.resolve() != root:
        raise ValueError("redirected cache parent")
    selected = []
    for name in sorted(set(names)):
        if name not in RESEARCH_PANELS:
            raise ValueError(f"not an allowlisted inactive research panel: {name}")
        panel = root / name / "panel_cache_v2"
        if not panel.exists():
            continue
        if panel.resolve() != panel or not panel.is_dir():
            raise ValueError(f"redirected panel cache: {panel}")
        for directory, dirs, files in os.walk(panel, followlinks=False):
            for entry in dirs + files:
                if (Path(directory) / entry).is_symlink():
                    raise ValueError(f"symlink inside panel cache: {directory}/{entry}")
        meta = json.loads((panel / "meta.json").read_text())
        if not isinstance(meta.get("arrays"), dict) or "features" not in meta["arrays"]:
            raise ValueError(f"not a recognized generated panel: {panel}")
        selected.append(panel)
    return selected


def immutable_array_path(relative: str, selected: list[Path], root: Path) -> bool:
    path = root / relative
    # Restrict to generation .npy files, excluding metadata, original Parquet,
    # receipts, direct legacy files, partial output and arbitrary nested trees.
    return any(
        path.parent.parent == panel / "generations"
        and path.suffix == ".npy"
        and len(path.parent.name) == 32
        and all(char in "0123456789abcdef" for char in path.parent.name)
        for panel in selected
    )


def current_service_blockers(selected: list[Path], repo: Path) -> list[str]:
    import dataclasses
    import yaml
    from stockagent.config import load_config

    blockers = []
    registry = repo / "services/discord_bot/markets"
    if not registry.is_dir():
        return ["service registry missing"]
    for market in sorted(registry.glob("*.yaml")):
        settings = yaml.safe_load(market.read_text())
        # Overnight services are conservatively protected even when Discord is
        # disabled; enabled=false is not evidence that the engine is stopped.
        if not (
            settings.get("enabled") or settings.get("overnight_simulation_enabled")
        ):
            continue
        configs = {settings.get("config_path")}
        model_path = settings.get("model_selection_path")
        if model_path:
            model = yaml.safe_load((market.parent / model_path).read_text())
            configs.add(model.get("config_path"))
            configs.update(model.get("model_candidate_config_paths") or ())
        for config_path in configs - {None}:
            config = load_config(repo / config_path)
            for value in dataclasses.asdict(config.data).values():
                if not isinstance(value, str) or not value.startswith(
                    ("data_", "artifacts/", "/srv/", "/root/")
                ):
                    continue
                candidate = (repo / value).resolve()
                for panel in selected:
                    if (
                        candidate == panel
                        or candidate in panel.parents
                        or panel in candidate.parents
                    ):
                        blockers.append(f"service consumer {market.name}: {value}")
    return blockers


def compact(*, names, apply, receipt_dir, repo=REPO, min_age_hours=168.0):
    root = repo / "artifacts/cache"
    selected = checked_panel_roots(root, names)
    if not selected:
        raise ValueError("no selected research panel caches exist")
    blockers = current_service_blockers(selected, repo)
    blockers += process_references_many(selected)
    if blockers:
        raise ValueError("protected cache references: " + "; ".join(blockers))
    # The same per-panel lock used by save_panel_cache_v2 is held through audit
    # and apply. Nonblocking acquisition never interrupts an active builder.
    with ExitStack() as stack:
        for panel in selected:
            lock = stack.enter_context((panel / ".write.lock").open("a+b"))
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        all_names = {path.name for path in root.iterdir() if path.is_dir()}
        groups, counters = find_duplicate_groups(
            root,
            min_age_hours=min_age_hours,
            excluded_top=frozenset(
                all_names - {panel.parent.name for panel in selected}
            ),
            excluded_suffixes=DEFAULT_EXCLUDED_SUFFIXES | {".json", ".parquet", ".npz"},
        )
        groups = [
            replace(
                group,
                duplicates=tuple(
                    item
                    for item in group.duplicates
                    if immutable_array_path(item.relative, selected, root)
                    and (root / item.relative).stat().st_nlink == 1
                ),
            )
            for group in groups
            if immutable_array_path(group.canonical.relative, selected, root)
        ]
        groups = [group for group in groups if group.duplicates]
        started = datetime.now(UTC)
        receipt_dir.mkdir(parents=True, exist_ok=True)
        stamp = started.strftime("%Y%m%dT%H%M%S.%fZ")
        audit = {
            "scope": "immutable inactive research panel generation arrays; no source or service eviction",
            "apply": False,
            "observed_at_utc": started.isoformat(),
            "selected_roots": [str(path) for path in selected],
            "min_age_hours": min_age_hours,
            "counters": counters,
            "groups": groups_as_json(groups),
            "would_replace_files": sum(len(group.duplicates) for group in groups),
            "would_free_allocated_bytes": sum(
                group.reclaimable_allocated_bytes for group in groups
            ),
            "source_files_removed": 0,
            "logical_paths_removed": 0,
        }
        audit_path = receipt_dir / f"panel-dedup-audit-{stamp}.json"
        atomic_write_json(audit_path, audit)
        replaced, skipped = [], []
        if apply:
            blockers = current_service_blockers(
                selected, repo
            ) + process_references_many(selected)
            if blockers:
                raise ValueError(
                    "cache became protected before apply: " + "; ".join(blockers)
                )
            # Canonical routine independently rehashes each retained and removed
            # inode, checks signatures, then atomically replaces only that name.
            replaced, skipped = apply_duplicate_groups(root, groups)
        allocated_by_path = {
            item.relative: item.blocks * 512
            for group in groups
            for item in group.duplicates
        }
        result = {
            **audit,
            "apply": apply,
            "audit_receipt": str(audit_path),
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "replaced": replaced,
            "skipped": skipped,
            "reclaimed_allocated_bytes": sum(
                allocated_by_path[item["path"]] for item in replaced
            ),
        }
        result_path = (
            receipt_dir / f"panel-dedup-{'apply' if apply else 'result'}-{stamp}.json"
        )
        atomic_write_json(result_path, result)
    print(
        json.dumps(
            {
                "receipt": str(result_path),
                "apply": apply,
                "would_replace_files": audit["would_replace_files"],
                "would_free_allocated_bytes": audit["would_free_allocated_bytes"],
                "replaced_files": len(replaced),
                "skipped_files": len(skipped),
                "reclaimed_allocated_bytes": result["reclaimed_allocated_bytes"],
                "logical_paths_removed": 0,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--panel", action="append", required=True, choices=sorted(RESEARCH_PANELS)
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--receipt-dir",
        type=Path,
        default=REPO / "artifacts/operations/source_host_cleanup_20261001/panel_dedup",
    )
    args = parser.parse_args()
    compact(names=args.panel, apply=args.apply, receipt_dir=args.receipt_dir)
