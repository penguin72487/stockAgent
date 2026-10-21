#!/usr/bin/env python3
"""Package existing official evidence for the canonical remote margin builder.

No provider requests, OCR, physical ledgers, panels or training are performed.
The maintained parser workspace remains the only rule editing authority.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, sha256_file


def materialize_portable_final_settlements(source: Path, output: Path) -> dict:
    """Project retained receipt locations without editing original evidence.

    The source bundle's hash-bound original-location map owns the relocation.
    Original manifest, raw payloads and parquet bytes are retained verbatim;
    only the new runtime manifest's receipt paths become relative.
    """
    source, output = source.resolve(), output.absolute()
    source_manifest = source / "source_manifest.json"
    inventory = json.loads(source_manifest.read_text())
    locations_path = source / "original_locations.json"
    if inventory.get("files", {}).get("original_locations.json", {}).get("sha256") != sha256_file(locations_path):
        raise ValueError("portable final requires the retained original-location map")
    locations = json.loads(locations_path.read_text())
    original_manifest = source / "final/manifest.json"
    if inventory["files"]["final/manifest.json"]["sha256"] != sha256_file(original_manifest):
        raise ValueError("original final manifest differs from the source release")
    manifest = json.loads(original_manifest.read_text())
    if manifest.get("schema_version") != 2 or manifest.get("status") != "complete":
        raise ValueError("portable final requires the canonical complete mixed-asset receipt")
    reverse = {}
    for relative, original in locations.items():
        reverse.setdefault(original, []).append(relative)
    retained_by_sha = {}
    for relative, item in inventory["files"].items():
        if relative.startswith("final/raw/"):
            retained_by_sha.setdefault(item["sha256"], []).append(relative)
    plan = [(original_manifest, "original_manifest.json", sha256_file(original_manifest))]
    receipts = []
    for item in manifest["receipts"]:
        candidates = [r for r in reverse.get(item["path"], []) if r.startswith("final/raw/")
                      and inventory["files"].get(r, {}).get("sha256") == item["sha256"]]
        # Several dated empty responses can have identical original bytes.
        # The retained bundle deduplicates those by SHA, so its location map
        # records one original name. Resolve the same exact content, retaining
        # every original receipt and its date/URL in original_manifest.json.
        if not candidates:
            candidates = retained_by_sha.get(item["sha256"], [])
        if len(candidates) != 1:
            raise ValueError("final receipt has no unique retained raw source")
        relative = Path(candidates[0]); retained = source / relative
        if not retained.resolve().is_relative_to(source) or sha256_file(retained) != item["sha256"]:
            raise ValueError("retained final raw source SHA/path mismatch")
        destination = "raw/" + retained.name
        plan.append((retained, destination, item["sha256"]))
        receipts.append(dict(item, path=destination))
    data = source / "final/futures_final_settlement_history.parquet"
    expected = manifest["outputs"]["futures_final_settlement_history"]["sha256"]
    if sha256_file(data) != expected:
        raise ValueError("retained final data SHA mismatch")
    plan.append((data, data.name, expected))
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("portable final projection requires a new output root")
    output.mkdir(parents=True, exist_ok=True)
    for origin, relative, digest in plan:
        target = output / relative; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin, target)
        if sha256_file(target) != digest:
            raise ValueError("portable final readback mismatch")
    projected = dict(manifest, receipts=receipts,
        path_projection_contract="retained_original_location_map_relative_receipts_v1",
        original_manifest_sha256=sha256_file(original_manifest),
        source_bundle_manifest_sha256=sha256_file(source_manifest))
    atomic_write_json(output / "manifest.json", projected)
    return dict(path=str(output / data.name), data_sha256=expected,
                manifest_sha256=sha256_file(output / "manifest.json"),
                original_manifest_sha256=sha256_file(original_manifest), raw_sources=len(receipts))


def stage(preparation: Path, output: Path) -> dict:
    if (output / "source_manifest.json").exists():
        raise FileExistsError("source bundle exists; verify/reuse its exact manifest")
    output.mkdir(parents=True, exist_ok=True)
    inventory = {}
    origins = {}

    def retain(source: Path, relative: Path, expected: str | None = None):
        source = source.resolve()
        digest = sha256_file(source)
        if expected is not None and digest != expected:
            raise ValueError(f"source SHA mismatch: {source}")
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if sha256_file(target) != digest:
                raise ValueError(f"conflicting source path: {relative}")
        else:
            # Inputs are immutable originals/receipts. Never chmod these links.
            try:
                os.link(source, target)
            except OSError:
                shutil.copy2(source, target)
        inventory[str(relative)] = dict(sha256=digest, bytes=source.stat().st_size)
        origins[str(relative)] = str(source)

    def retained_bundle(source: Path, name: str, *, include_sources=True):
        proof = json.loads((source / "manifest.json").read_text())
        retain(source / "manifest.json", Path(name) / "manifest.json")
        for filename, row in proof.get("outputs", {}).items():
            path = source / filename
            if path.is_file():
                retain(path, Path(name) / filename, row["sha256"])
        for row in (proof.get("sources", []) if include_sources else []):
            relative = Path(row["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("source bundle requires relative receipt paths")
            retain(source / relative, Path(name) / relative, row["sha256"])

    current = preparation / "all_products_rule_facts"
    retained_bundle(current, "rules")
    retained_bundle(preparation / "dated_product_specifications_v5_20260930", "specifications")
    retained_bundle(preparation / "remaining_gap_repair_v3_20260930/cj_hw_terminal_components_v706", "terminal")
    retained_bundle(preparation / "all_reported_daily_source_v3_repaired_20260929", "observations", include_sources=False)
    raw = json.loads((output / "observations/manifest.json").read_text())
    daily_receipts = []
    for item in raw["sources"]:
        source = Path(item["path"])
        relative = Path("official_daily/raw") / source.name
        retain(source, relative, item["sha256"])
        daily_receipts.append(dict(path=str(relative), sha256=item["sha256"]))
    daily_manifest = output / "official_daily/manifest.json"
    atomic_write_json(daily_manifest, dict(receipts=daily_receipts,
        source_observations_manifest_sha256=sha256_file(output / "observations/manifest.json")))
    inventory[str(daily_manifest.relative_to(output))] = dict(sha256=sha256_file(daily_manifest),
        bytes=daily_manifest.stat().st_size)

    final = preparation / "official_final_settlement_all_asset_classes_20260929"
    proof = json.loads((final / "manifest.json").read_text())
    retain(final / "manifest.json", Path("final/manifest.json"))
    for filename in ["futures_final_settlement_history.parquet"]:
        row = proof["outputs"].get("futures_final_settlement_history", proof["outputs"].get(filename))
        retain(final / filename, Path("final") / filename, row["sha256"])
    for item in proof["receipts"]:
        retain(Path(item["path"]), Path("final/raw") / (item["sha256"] + Path(item["path"]).suffix), item["sha256"])

    retained_bundle(preparation / "all_twd_currency_scope_v3_20260929", "universe")
    # products.csv has its own catalog binding rather than an outputs entry.
    retain(preparation / "all_twd_currency_scope_v3_20260929/products.csv", Path("universe/products.csv"),
        json.loads((preparation / "all_twd_currency_scope_v3_20260929/manifest.json").read_text())["products_sha256"])
    retain(preparation / "scoped_stock_margin_20260930/deferred_54_products.csv", Path("deferred_54_products.csv"))
    retain(ROOT / "configs/markets/tw_futures_position_research_v1.json", Path("position_research_policy.json"))
    retain(preparation / "dated_equity_margin_family_20260930.json", Path("margin_family.json"))

    for source, name in [
        (preparation / "dated_position_family_review_20260930", "position_family"),
        (preparation / "all_twd_physical_history_v26_20261001/underlying_final_rule", "underlying_final_rule"),
        (preparation / "all_twd_physical_history_v26_20261001/adjusted_lifecycle_rule", "adjusted_lifecycle_rule"),
        (preparation / "remaining_gap_repair_v3_20260930/dated_cuf_halt_reviews_v1183/2015", "halt/2015"),
        (preparation / "remaining_gap_repair_v3_20260930/dated_cuf_halt_reviews_v1183/2019", "halt/2019"),
    ]:
        # These portable reviews bind their own nested original-byte receipts.
        for file in sorted(source.rglob("*")):
            if file.is_file():
                retain(file, Path(name) / file.relative_to(source))
    atomic_write_json(output / "original_locations.json", origins)
    inventory["original_locations.json"] = dict(sha256=sha256_file(output / "original_locations.json"),
        bytes=(output / "original_locations.json").stat().st_size)
    report = dict(schema_version=1, dataset="tw_futures_margin_sources",
        status="source_evidence_complete_training_scope_pending",
        created_at_utc=datetime.now(UTC).isoformat(), source_only=True,
        all_requested_history_training_ready=False,
        rule_workspace_manifest_sha256=sha256_file(current / "manifest.json"),
        builder_sha256=sha256_file(Path(__file__)), files=inventory,
        raw_originals_preserved=True, new_provider_requests=0, new_ocr_jobs=0,
        derived_training_views_in_bundle=False)
    atomic_write_json(output / "source_manifest.json", report)
    print(json.dumps(dict(status=report["status"], files=len(inventory),
        bytes=sum(v["bytes"] for v in inventory.values())), ensure_ascii=False), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation", type=Path,
        default=ROOT / "artifacts/markets/tw_futures_v8_margin_preparation")
    parser.add_argument("--output", type=Path,
        default=ROOT / "data_tw_index_futures/preparation_sources/margin_sources")
    parser.add_argument("--refresh-current", action="store_true",
        help="Atomically refresh source evidence; retain the exact prior bundle.")
    args = parser.parse_args()
    # Follow only the explicitly requested live producer alias, never create
    # a second bundle beside it during a refresh of an older CLI path.
    args.output = args.output.resolve()
    if args.refresh_current:
        from scripts.promote_tw_day_trade_replay import _exchange_directories
        pending=args.output.parent/'.margin-sources-pending'
        previous=args.output.parent/'.margin-sources-previous'
        if previous.exists():raise FileExistsError("prior source evidence awaits explicit retirement")
        if not (pending/'source_manifest.json').exists():stage(args.preparation,pending)
        _exchange_directories(args.output,pending)
        os.rename(pending,previous)
    else:
        stage(args.preparation, args.output)
