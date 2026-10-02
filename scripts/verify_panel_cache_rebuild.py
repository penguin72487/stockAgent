"""Prove a plain panel cache is exactly reconstructible without writing a cache.

One-off cleanup validation only: calls the canonical builder with its cache
load/save boundaries suppressed, then compares every logical array digest.
Does not train, download, publish, evict or change services.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
os.environ.setdefault("POLARS_MAX_THREADS", "2")

from stockagent.config import external_panel_data_kwargs, load_config
from stockagent.data import panel as panels
from stockagent.data.panel_cache import (
    array_content_fingerprint,
    load_panel_cache_v2_manifest,
)
from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file
from stockagent.data_sync.materialized_cache import process_references_many


def verify(config_path: Path, receipt: Path):
    config = load_config(config_path)
    data = config.data
    # This deliberately bounded validator cannot bypass TW rule/carry/execution
    # contracts or pin a deployed model. Additional families need their own
    # explicit canonical source set before becoming cleanup candidates.
    if not str(data.parquet_root).startswith("data_bybit/perpetual_daily"):
        raise ValueError("only the explicit Bybit daily source family is supported")
    source_root = Path(data.parquet_root).resolve()
    cache_base = (
        Path(data.panel_cache_root).resolve() if data.panel_cache_root else source_root
    )
    cache = cache_base / "panel_cache_v2"
    if process_references_many([cache]):
        raise ValueError("cache is currently in use")
    manifest = cache / "meta.json"
    manifest_hash = sha256_file(manifest)
    original = json.loads(manifest.read_text())
    if original.get("version") != panels.PANEL_CACHE_VERSION:
        raise ValueError("old panel ABI: current builder cannot prove the old contract")
    inputs = sorted(source_root.glob("*_features.parquet"))
    inputs += [
        p.parent / "_hot_tail" / p.name
        for p in inputs
        if (p.parent / "_hot_tail" / p.name).is_file()
    ]
    external = external_panel_data_kwargs(data)
    if external.get("external_feature_path"):
        inputs.append(Path(external["external_feature_path"]).resolve())
    identities = {str(p): sha256_file(p) for p in inputs}
    before_hash = panels._compute_source_hash(inputs)
    if before_hash != original.get("source_hash"):
        raise ValueError("retained inputs differ from cache's exact source identity")
    captured = {}

    def no_save(root, panel, source_hash, backend_key):
        captured.update(source_hash=source_hash, backend_key=backend_key)

    started = datetime.now(UTC)
    with (
        patch.object(panels, "_load_valid_panel_cache", return_value=None),
        patch.object(panels, "_save_panel_cache", side_effect=no_save),
    ):
        rebuilt = panels.build_panel(
            source_root,
            benchmark_name=data.benchmark_name,
            usd_only_trading_pairs=data.usd_only_trading_pairs,
            tradable_mode=data.tradable_mode,
            trading_volume_policy=data.trading_volume_policy,
            security_filter=data.security_filter,
            strict_no_fallback=True,
            panel_backend=data.panel_backend,
            panel_load_workers=2,
            panel_cache_root=cache_base,
            feature_include=data.feature_include,
            feature_exclude=data.feature_exclude,
            feature_zero_fill=data.feature_zero_fill,
            feature_availability_indicators=data.feature_availability_indicators,
            feature_shift_next_session=data.feature_shift_next_session,
            panel_start_date=data.panel_start_date,
            **external,
        )
    payload = load_panel_cache_v2_manifest(manifest, mmap_mode="r")
    comparisons = []
    for name in original["arrays"]:
        old = payload.get(name)
        new = getattr(rebuilt, name, None)
        old_digest = array_content_fingerprint(old)
        new_digest = array_content_fingerprint(new)
        comparisons.append(
            {
                "array": name,
                "old": old_digest,
                "rebuilt": new_digest,
                "equal": old_digest == new_digest,
            }
        )
    stable = (
        panels._compute_source_hash(inputs) == before_hash
        and sha256_file(manifest) == manifest_hash
    )
    verified = (
        stable
        and all(item["equal"] for item in comparisons)
        and payload["symbols"] == rebuilt.symbols
        and payload["feature_names"] == rebuilt.feature_names
        and captured.get("source_hash") == original.get("source_hash")
        and captured.get("backend_key") == original.get("backend_key")
    )
    result = {
        "schema_version": 1,
        "config": str(config_path.resolve()),
        "cache": str(cache),
        "started_at_utc": started.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "manifest_sha256": manifest_hash,
        "source_hash": before_hash,
        "source_file_sha256": identities,
        "captured_builder_contract": captured,
        "array_comparisons": comparisons,
        "inputs_and_manifest_stable": stable,
        "exact_rebuild_verified": verified,
        "source_files_changed": 0,
        "cache_files_written": 0,
        "eviction_authorized": False,
    }
    receipt.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(receipt, result)
    print(
        json.dumps(
            {"receipt": str(receipt), "exact_rebuild_verified": verified},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    try:
        verify(args.config, args.receipt)
    except (OSError, ValueError) as exc:
        result = {
            "schema_version": 1,
            "config": str(args.config.resolve()),
            "observed_at_utc": datetime.now(UTC).isoformat(),
            "exact_rebuild_verified": False,
            "eviction_authorized": False,
            "blocked_reason": str(exc),
        }
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(args.receipt, result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        raise SystemExit(2)
