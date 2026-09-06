#!/usr/bin/env python3
"""Read-only architecture inventory from the existing config/registry owners.

No imports of runners, downloads, service restarts, Git fetches, or data scans.
The optional systemd snapshot observes this host only; it is not a fleet or
release-validity proof. Output goes to stdout unless --output is specified.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.config import load_config
from stockagent.training.mode_adapter import TRAINING_MODE_SPECS


def _command(args: list[str]) -> str:
    result = subprocess.run(
        args, cwd=REPO_ROOT, capture_output=True, text=True, timeout=20
    )
    if result.returncode:
        raise RuntimeError(
            f"{args[0]} exited with {result.returncode}: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _unit_fields(path: Path) -> dict[str, list[str]]:
    fields: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(("#", ";", "[")) or "=" not in line:
            continue
        key, value = line.split("=", 1)
        # Deliberately exclude environment, credentials and command arguments.
        if key in {
            "Description",
            "Type",
            "After",
            "Requires",
            "Wants",
            "OnCalendar",
            "OnUnitActiveSec",
            "OnBootSec",
            "Persistent",
            "Restart",
            "WatchdogSec",
        }:
            fields.setdefault(key, []).append(value)
    return fields


def _systemd_snapshot(names: list[str]) -> list[dict[str, str]]:
    properties = [
        "Id",
        "LoadState",
        "ActiveState",
        "SubState",
        "Result",
        "MainPID",
        "ExecMainStartTimestamp",
        "FragmentPath",
        "UnitFileState",
        "NeedDaemonReload",
    ]
    text = _command(
        [
            "systemctl",
            "show",
            *names,
            "--no-pager",
            "--property=" + ",".join(properties),
        ]
    )
    return [
        dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        for block in text.split("\n\n")
        if block.strip()
    ]


def build_inventory(*, systemd: bool = False) -> dict:
    market_configs, errors = [], []
    for path in sorted((REPO_ROOT / "configs/markets").glob("*.yaml")):
        relative = str(path.relative_to(REPO_ROOT))
        try:
            cfg = load_config(path)
            market_configs.append(
                {
                    "path": relative,
                    "execution_mode": cfg.trading.execution_mode,
                    "model": cfg.training.model_name,
                }
            )
        except Exception as exc:
            errors.append({"path": relative, "error": f"{type(exc).__name__}: {exc}"})
    templates = sorted((REPO_ROOT / "deploy/systemd").glob("*.in"))
    units = [
        {
            "unit": path.name.removesuffix(".in"),
            "template": str(path.relative_to(REPO_ROOT)),
            "fields": _unit_fields(path),
        }
        for path in templates
    ]
    packages = [
        {
            "path": str(path.relative_to(REPO_ROOT)),
            "python_files": len(list(path.rglob("*.py"))),
        }
        for path in sorted((REPO_ROOT / "stockagent").iterdir())
        if path.is_dir() and (path / "__init__.py").is_file()
    ]
    catalog = json.loads(
        (REPO_ROOT / "configs/data_sync/packed_datasets.json").read_text()
    )
    result = {
        "schema_version": 1,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "current checkout, local Git refs, optional local systemd",
        "git": {
            "head": _command(["git", "rev-parse", "HEAD"]),
            "branch": _command(["git", "branch", "--show-current"]),
            "status": _command(["git", "status", "--short"]),
            "tracked_diff_sha256": hashlib.sha256(
                _command(["git", "diff", "--no-ext-diff", "HEAD"]).encode()
            ).hexdigest(),
            "refs": _command(
                [
                    "git",
                    "for-each-ref",
                    "--format=%(refname:short) %(objectname) %(upstream:short) %(symref)",
                    "refs/heads",
                    "refs/remotes",
                ]
            ).splitlines(),
            "remote_refs_live_verified": False,
        },
        "packages": packages,
        "training_modes": [asdict(spec) for spec in TRAINING_MODE_SPECS.values()],
        "market_configs": market_configs,
        "market_config_errors": errors,
        "dataset_catalog": catalog["datasets"],
        "systemd_templates": units,
        "evidence_limits": [
            "Mode registry defaults can be specialized by the resolved strategy configuration.",
            "Active service does not prove source freshness, public acceptance or loaded Git revision.",
            "Absent optional service templates do not authorize enabling publication or eviction.",
            "Dataset catalog describes ownership, not current completeness or peer convergence.",
        ],
    }
    if systemd:
        try:
            result["local_systemd"] = _systemd_snapshot(
                [unit["unit"] for unit in units]
            )
        except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
            result["local_systemd_error"] = str(exc)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--systemd", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = build_inventory(systemd=args.systemd)
    if args.output:
        atomic_write_json(args.output, result)
        print(
            json.dumps(
                {
                    "output": str(args.output),
                    "market_configs": len(result["market_configs"]),
                    "training_modes": len(result["training_modes"]),
                    "systemd_templates": len(result["systemd_templates"]),
                    "config_errors": len(result["market_config_errors"]),
                },
                ensure_ascii=False,
            )
        )
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["market_config_errors"] or result.get("local_systemd_error"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
