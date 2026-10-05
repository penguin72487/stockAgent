#!/usr/bin/env python3
"""Stage compact, private source observations; build training views on Vast.

No panel, normalization, fit, label, training matrix or broker calls. The raw
provider catalog/cache is never packaged. User-authorized projections retain
their original observation axis, values, exact source hashes and restrictions.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import sys

import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.stage_tw_public_research_release import _required_formal_members, _stage_copy, LOCK
from stockagent.data.tw_day_trade_feature_admission import classify_candidate
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT, PRIVATE_USE, public_spec
from stockagent.data.tw_public_release_schedule import rule_for, feature_name, feature_category, rule_manifest
from stockagent.data.tw_day_trade_carry_source import PHYSICAL_PUBLIC_RELATIVE_MEMBERS
from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source


def stage(*, public: Path, finlab_catalog: Path, out: Path, authorization: Path,
          cross_source_bundle: Path | None = None) -> dict:
    grant = json.loads(authorization.read_text())
    if grant.get("private_vastai1T_delivery_authorized") is not True:
        raise ValueError("explicit private delivery authorization is required")
    if out.exists():
        raise FileExistsError("stage at a fresh versioned source root")
    out.mkdir(parents=True, mode=0o700)
    proofs, specs, decisions, applied_repairs = {}, [], [], {}
    bundle, publication_bounds = None, None
    if cross_source_bundle is not None:
        bundle = json.loads((cross_source_bundle / 'bundle_manifest.json').read_text())
        if bundle.get('contract') != 'tw_cross_source_missing_only_v1':
            raise ValueError('unsupported missing-only source overlay')
        for name in ('source_receipts', 'mapping_audit'):
            if sha256(cross_source_bundle / (name + '.json')) != bundle[name + '_sha256']:
                raise ValueError('cross-source repair evidence changed')
        publication_bounds = bundle.get('publication_lower_bounds')

    def copy(relative: str):
        src = public / relative
        digest = sha256(src)
        _stage_copy(src, out / relative, digest)
        proofs[relative] = {"sha256": digest, "bytes": src.stat().st_size, "original_source": str(src)}

    summary = json.loads((public / "features/tw_public_stock_daily.summary.json").read_text())
    expected = summary["output_receipt"]["sha256"]
    copy("features/tw_public_stock_daily.parquet")
    if proofs["features/tw_public_stock_daily.parquet"]["sha256"] != expected:
        raise ValueError("canonical feature output receipt is stale")
    entitlements = json.loads((public / "tw_corporate_action_entitlements.summary.json").read_text())
    for relative in [*_required_formal_members(entitlements, public)[1:],
                     "features/tw_public_stock_daily.summary.json", "twse_taiex_ohlc.parquet",
                     "download_summary.json", "stocks/official_symbol_build_report.csv"]:
        copy(relative)
    # Model features alone do not make a receipt-bound physical FIFO source.
    # Keep the consumer's own dependency inventory in the source publication.
    for relative in PHYSICAL_PUBLIC_RELATIVE_MEMBERS:
        if relative not in proofs:copy(relative)
    stock_paths = sorted((public / "stocks").glob("*_features.parquet"))
    if not stock_paths:
        raise ValueError("no official stock source files")
    for path in stock_paths:
        copy(path.relative_to(public).as_posix())
    symbols = {p.name.removesuffix("_features.parquet") for p in stock_paths}
    for name in pq.read_schema(public / "features/tw_public_stock_daily.parquet").names:
        spec = public_spec(name) if name.startswith("twpub_") else None
        if spec:
            specs.append(spec)
    with finlab_catalog.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        decision = classify_candidate(row)
        key, rule = row["dataset_id"], rule_for(row["dataset_id"])
        if rule is None or decision["decision"] != "quarantined_pit":
            decisions.append(decision)
            continue
        receipt_path = Path(row['receipt_path'])
        src, receipt = read_receipt_bound_source(receipt_path.parent.parent, key, receipt_path)
        digest = sha256(src)
        if receipt.get("dataset") != key or digest != receipt.get("sha256"):
            raise ValueError(f"source receipt changed: {key}")
        schema = pq.read_schema(src)
        columns = [n for n in schema.names if n != "source_index" and (rule.scope == "market" or n in symbols)]
        if "source_index" not in schema.names or not columns:
            decision.update(decision="quarantined_research_adapter", reason="missing_period_axis_or_no_official_universe_columns")
            decisions.append(decision)
            continue
        feature = feature_name(key)
        # Project BEFORE reading/unpivoting: most stock-code columns can be
        # warrants or foreign dimensions. Never change observed value precision.
        source = pl.read_parquet(src, columns=["source_index", *columns])
        repair = bundle.get('overrides', {}).get(key) if bundle else None
        if repair:
            if repair['primary_sha256'] != digest or sha256(Path(repair['path'])) != repair['sha256']:
                raise ValueError('overlay does not belong to the current pinned primary')
            patched = pl.read_parquet(repair['path'])
            from stockagent.data.tw_public_cross_source_fill import verify_wide_missing_only
            fills_path = Path(repair['fills_path'])
            if sha256(fills_path) != repair['fills_sha256']:
                raise ValueError('actual source repair evidence changed')
            count = verify_wide_missing_only(source, patched, pl.read_parquet(fills_path))
            if count != repair['filled_observations']:
                raise ValueError('source repair count mismatch')
            columns = [n for n in patched.columns if n != 'source_index' and (rule.scope == 'market' or n in symbols)]
            source = patched.select('source_index', *columns)
        schema = source.schema
        if any(not (schema[n].is_numeric() or schema[n] == pl.Null) for n in columns):
            decision.update(decision="quarantined_research_adapter", reason="nonnumeric_measure_or_ambiguous_dimensions")
            decisions.append(decision)
            continue
        relative = f"finlab/observations/{feature}.parquet"
        target = out / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        source.write_parquet(target, compression="zstd", statistics=True)
        if sha256(src) != digest:
            raise ValueError(f"source changed while projecting: {key}")
        proofs[relative] = {"sha256": sha256(target), "bytes": target.stat().st_size,
                            "original_source_sha256": digest, "source_receipt": receipt,
                            "projection": "unchanged_original_source_index_and_values; official_historical_universe_only"}
        if repair:
            fill_relative = 'source_repairs/fills/' + feature + '.parquet'
            _stage_copy(fills_path, out / fill_relative, repair['fills_sha256'])
            proofs[fill_relative] = {'sha256': repair['fills_sha256'], 'bytes': fills_path.stat().st_size}
            proofs[relative]['source_repair'] = {**repair, 'relative_fills_path': fill_relative}
            proofs[relative]['projection'] = 'verified_missing_only_real_alternate_observations; finite_primary_unchanged'
            applied_repairs[key] = count
        specs.append({"feature": feature, "source": "FinLab", "dataset": key,
                      "path": relative, "rule": asdict(rule), "clock": "estimated_publication",
                      "category": feature_category(key), "value_vintage": "current_provider_revision",
                      "publication_time_estimated": True})
        decision.update(decision="selected_private_research_observation", selected_canonical_feature=feature,
                        reason=rule.name + "; current_revision_not_historical_vintage")
        decisions.append(decision)
        print(f"[mixed-sources] {key}: {source.height} periods / {len(columns)} dimensions", flush=True)
    # Filing dates are clock evidence, not model X.
    upload = next((r for r in rows if r["dataset_id"] == "financial_statements_upload_detail:upload_date"), None)
    if upload:
        receipt_path = Path(upload['receipt_path'])
        src, receipt = read_receipt_bound_source(receipt_path.parent.parent, upload['dataset_id'], receipt_path)
        digest = sha256(src)
        if digest != receipt["sha256"]:
            raise ValueError("filing-date source receipt is stale")
        columns = [n for n in pq.read_schema(src).names if n == "source_index" or n in symbols]
        target = out / "finlab/known_uploads.parquet"
        pl.read_parquet(src, columns=columns).write_parquet(target, compression="zstd")
        if sha256(src) != digest:
            raise ValueError("filing-date source changed")
        proofs["finlab/known_uploads.parquet"] = {"sha256": sha256(target), "bytes": target.stat().st_size,
            "original_source_sha256": digest, "source_receipt": receipt}
    for relative, item in proofs.items():
        if sha256(out / relative) != item["sha256"]:
            raise ValueError("staged source changed before commit")
    write_csv(out / "finlab_admission.csv", decisions)
    atomic_write_json(out / "publication_rules.json", rule_manifest())
    if bundle:
        if (set(applied_repairs) != set(bundle['overrides'])
                or sum(applied_repairs.values()) != bundle['filled_observations']):
            raise ValueError('declared source repairs were not actually staged')
        for name in ('bundle_manifest.json', 'source_receipts.json', 'mapping_audit.json'):
            src = cross_source_bundle / name; relative = 'source_repairs/' + name
            _stage_copy(src, out / relative, sha256(src))
            proofs[relative] = {'sha256': sha256(src), 'bytes': src.stat().st_size}
        if publication_bounds:
            src = Path(publication_bounds['path'])
            if sha256(src) != publication_bounds['sha256']:
                raise ValueError('source repair publication bound changed')
            relative = 'source_repairs/publication_lower_bounds.parquet'
            _stage_copy(src, out / relative, publication_bounds['sha256'])
            proofs[relative] = {'sha256': publication_bounds['sha256'], 'bytes': src.stat().st_size}
    manifest = {"contract": CONTRACT, "status": "complete", "source_only": True,
        "private_personal_research": True, "private_delivery_authorized": True,
        "use_restriction": PRIVATE_USE, "authorization_sha256": sha256(authorization),
        "created_at_utc": datetime.now(UTC).isoformat(), "end_date": summary["requested_end_date"],
        "historical_point_in_time": False, "research_only": True, "live_eligible": False,
        "training_ready": False, "files": proofs, "feature_specs": specs,
        "stock_files": len(stock_paths), "selected_value_features": len(specs),
        "source_bytes": sum(x["bytes"] for x in proofs.values()),
        "implementation_sha256": sha256(Path(__file__)),
        "limitations": ["current_revisions_and_estimated_release_clocks_not_historical_vintages",
                        "unadapted_and_licensed_unverified_sources_not_auto_injected",
                        "no_training_matrix_or_execution_readiness_claim"]}
    if bundle:
        manifest['source_repairs'] = {'contract': bundle['contract'], 'filled_observations': bundle['filled_observations'],
            'manifest': 'source_repairs/bundle_manifest.json',
            'publication_lower_bounds': 'source_repairs/publication_lower_bounds.parquet' if publication_bounds else None}
    from stockagent.data.tw_public_cross_source_fill import verify_staged_source_repairs
    verify_staged_source_repairs(out, manifest)
    atomic_write_json(out / "source_manifest.json", manifest)
    from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources
    verify_sources(out)
    return {k:v for k,v in manifest.items() if k not in {"files", "feature_specs"}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    parser.add_argument("--finlab-catalog", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--authorization", type=Path, required=True)
    parser.add_argument('--cross-source-bundle', type=Path)
    args = parser.parse_args()
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(json.dumps(stage(public=args.public_root, finlab_catalog=args.finlab_catalog,
                               out=args.output_root, authorization=args.authorization,
                               cross_source_bundle=args.cross_source_bundle), ensure_ascii=False))


if __name__ == "__main__":
    main()
