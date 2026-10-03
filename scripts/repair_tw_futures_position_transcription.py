#!/usr/bin/env python3
"""Reissue reviewed position terms through the existing margin release publisher."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_margin_release import read_bound_output, publish_all_twd_margin_release
from stockagent.data.tw_futures_position_review import apply_position_cap_review


def complete_training_envelope(prepared: Path, output: Path) -> dict:
    """Retain the parent's verified slot ABI when the publisher omits its envelope.

    The original preparation workflow also completes these three fields after
    publication. Only byte-identical market rows can reuse that loader envelope.
    """
    import polars as pl
    from stockagent.data.tw_futures_portfolio_daily import (
        futures_slot_layout_version, TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
    )
    old_daily = prepared / 'release/daily/continuous_daily.parquet'
    new_daily = output / 'release/daily/continuous_daily.parquet'
    parent = json.loads(old_daily.with_name('manifest.json').read_text())
    digest = sha256_file(old_daily)
    if digest != parent['outputs']['continuous_daily']['sha256'] or sha256_file(new_daily) != digest:
        raise ValueError('loader-envelope reuse requires identical verified market bytes')
    slots = int(parent['fixed_model_output_slots'])
    envelope = dict(contract_version=futures_slot_layout_version(slots),
                    feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
                    fixed_model_output_slots=slots)
    if any(parent.get(k) != v for k, v in envelope.items()):
        raise ValueError('parent loader envelope differs from current slot contract')
    coords = pl.scan_parquet(new_daily).select(
        pl.col('symbol').str.extract(r'TAIFEX_SLOT_(\d+)$', 1).cast(pl.Int64).alias('slot')
    ).select(pl.col('slot').min(), pl.col('slot').max().alias('maximum'),
             pl.col('slot').null_count().alias('nulls')).collect().row(0)
    if coords[2] or not 1 <= coords[0] <= coords[1] <= slots:
        raise ValueError('market coordinates violate the source-bound slot ABI')
    path = new_daily.with_name('manifest.json')
    manifest = json.loads(path.read_text())
    if any(k in manifest and manifest[k] != v for k, v in envelope.items()):
        raise ValueError('new loader envelope conflicts with its byte-identical parent')
    atomic_write_json(path, dict(manifest, **envelope))
    return dict(fields=envelope, daily_bytes_equal=True, daily_sha256=digest,
                parent_manifest_sha256=sha256_file(old_daily.with_name('manifest.json')),
                completed_manifest_sha256=sha256_file(path))


def repair(prepared: Path, review_path: Path, page_image: Path, final_settlement: Path, output: Path):
    review = json.loads(review_path.read_text())
    parent = prepared / "terms/rules.parquet"
    if sha256_file(parent) != review["parent_rules_sha256"]:
        raise ValueError("review belongs to a different immutable parent")
    if sha256_file(page_image) != review["page_image_sha256"]:
        raise ValueError("review page image SHA mismatch")
    rules, manifest = read_bound_output(parent, output_key="rules")
    raw = [r for r in manifest["sources"] if r.get("kind") == "raw_gzip"
           and r.get("url") == review["source_url"]]
    if len(raw) != 1:
        raise ValueError("review must bind one retained official original")
    source = parent.parent / raw[0]["path"]
    if sha256_file(source) != raw[0]["sha256"]:
        raise ValueError("official original SHA mismatch")
    if hashlib.sha256(gzip.decompress(source.read_bytes())).hexdigest() != review["pdf_sha256"]:
        raise ValueError("review belongs to another PDF")
    corrected, parity = apply_position_cap_review(rules, review)
    if output.exists():
        raise FileExistsError("position repair requires a fresh output directory")
    output.mkdir(parents=True)
    terms = output / "terms"
    # Source receipts are immutable, verified again by the common publisher.
    # Atomic writers replace the two changed names without editing shared inodes.
    shutil.copytree(parent.parent, terms, copy_function=os.link)
    atomic_write_parquet(terms / "rules.parquet", corrected)
    evidence = terms / "sources/position_transcription_review"
    evidence.mkdir()
    shutil.copyfile(review_path, evidence / "review.json")
    shutil.copyfile(page_image, evidence / "page.png")
    receipt = dict(parent_rules_sha256=review["parent_rules_sha256"],
                   review_sha256=sha256_file(review_path), **parity,
                   financial_values_inferred=False, training_started=False)
    atomic_write_json(evidence / "parity.json", receipt)
    manifest["sources"] = [*manifest["sources"], *[
        dict(path=str(p.relative_to(terms)), sha256=sha256_file(p),
             kind="source_bound_position_transcription_review", url=review["source_url"])
        for p in sorted(evidence.iterdir())
    ]]
    manifest["outputs"] = {**manifest["outputs"],
                           "rules": dict(sha256=sha256_file(terms / "rules.parquet"))}
    manifest["position_transcription_review"] = receipt
    atomic_write_json(terms / "manifest.json", manifest)
    # Reuse source admission, continuation, financial validation and manifests.
    result = publish_all_twd_margin_release(
        materialization=prepared / "materialization", execution_terms=terms / "rules.parquet",
        final_settlement=final_settlement, output=output / "release")
    result['loader_envelope'] = complete_training_envelope(prepared, output)
    if sha256_file(parent) != review["parent_rules_sha256"]:
        raise RuntimeError("parent changed during repair")
    result.update(position_transcription_review=receipt)
    atomic_write_json(output / "repair_acceptance.json", result)
    print(json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--review", type=Path, required=True)
    parser.add_argument("--page-image", type=Path, required=True)
    parser.add_argument("--final-settlement", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    a = parser.parse_args()
    repair(a.prepared, a.review, a.page_image, a.final_settlement, a.output)
