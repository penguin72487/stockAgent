#!/usr/bin/env python3
"""Compile a source-bound visual review against all retained corporate facts.

The output is a reviewed source candidate, not a promoted rules bundle,
customer-ledger transition or training admission.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from downloader.artifact_io import atomic_write_json
from scripts.audit_tw_futures_holiday_generation_paths import stable_digest
from scripts.build_tw_futures_margin_event_candidates import (
    corporate_source_review_candidates, replace_reviewed_corporate_facts,
)
from stockagent.data.tw_futures_margin_preparation import RuleArchive, corporate_terms_intervals, read_verified_raw


def verify_review_renders(pdf: bytes, review: dict) -> None:
    """Prove every reviewed image is the declared page of these exact bytes."""
    import pymupdf
    with pymupdf.open(stream=pdf, filetype='pdf') as document:
        for page in review['pages']:
            number, dpi = page['page'], page.get('render_dpi')
            if type(number) is not int or not 1 <= number <= len(document) or type(dpi) is not int or not 72 <= dpi <= 600:
                raise ValueError('invalid reviewed page or render resolution')
            image = document[number - 1].get_pixmap(dpi=dpi, alpha=False).tobytes('png')
            if hashlib.sha256(image).hexdigest() != page['image_sha256']:
                raise ValueError('review image is not the declared original page render')


def verify_reference_image(pdf: bytes, directory: Path, reference: dict) -> Path:
    """Bind an ancillary rounding reference to both original bytes and page."""
    if hashlib.sha256(pdf).hexdigest() != reference.get('content_sha256'):
        raise ValueError('reference PDF does not match the declared content hash')
    image = (directory / reference['image_path']).resolve()
    if not image.is_relative_to(directory.resolve()) or stable_digest(image) != reference['image_sha256']:
        raise ValueError('reference image differs from its declared source-bound receipt')
    verify_review_renders(pdf, {'pages': [reference]})
    return image


def audit(workspace: Path, current: Path, review: Path, output: Path) -> dict:
    workspace = workspace.resolve()
    current, review, output = (p.resolve() for p in (current, review, output))
    if (not current.is_relative_to(workspace) or not review.is_relative_to(workspace / 'artifacts/markets')
            or not output.is_relative_to(workspace / 'artifacts/markets') or output.exists()):
        raise ValueError('review and new output must use the canonical workspace')
    manifest_path = current / 'manifest.json'
    facts_path = current / 'corporate_event_candidates.parquet'
    input_paths = [manifest_path, facts_path, review, Path(__file__).resolve(),
                   workspace / 'stockagent/data/tw_futures_margin_preparation.py',
                   workspace / 'scripts/build_tw_futures_margin_event_candidates.py']
    review_pin = stable_digest(review)
    payload = json.loads(review.read_text())
    if stable_digest(review) != review_pin:
        raise ValueError('review changed during reading')
    reviews = payload.get('reviews', [payload])
    for item in reviews:
        for page in item['pages']:
            image = (review.parent / page['image_path']).resolve()
            if not image.is_relative_to(review.parent) or stable_digest(image) != page['image_sha256']:
                raise ValueError('visual review page differs from its source-bound receipt')
            input_paths.append(image)
    pins = {str(path.relative_to(workspace)): stable_digest(path) for path in input_paths}
    manifest = json.loads(manifest_path.read_text())
    if pins[str(facts_path.relative_to(workspace))] != manifest['outputs'][facts_path.name]['sha256']:
        raise ValueError('retained corporate facts differ from their manifest')
    facts = pl.read_parquet(facts_path).to_dicts()
    with tempfile.TemporaryDirectory(prefix='stockagent-corporate-review-', dir='/var/tmp') as temp:
        archive = RuleArchive(workspace / 'data_taifex_public_history/rules', Path(temp) / 'bundle')
        try:
            reviewed, positions, source_hashes = corporate_source_review_candidates(archive, review)
            reference_receipts = []
            for item in reviews:
                document = archive.conn.execute('SELECT * FROM documents WHERE url=?',
                                                (item['source_url'],)).fetchone()
                if document is None:
                    raise ValueError('reviewed source document is absent')
                verify_review_renders(read_verified_raw(archive.root, dict(document)), item)
                for component in item.get('fixed_subscription_rights', []):
                    reference = component.get('rounding_reference')
                    if reference is None:
                        continue
                    source = archive.conn.execute('SELECT * FROM documents WHERE url=?',
                                                  (reference['source_url'],)).fetchone()
                    if source is None or source['content_sha256'] != reference.get('content_sha256'):
                        raise ValueError('rounding reference document is absent or changed')
                    image = verify_reference_image(read_verified_raw(archive.root, dict(source)),
                                                   review.parent, reference)
                    pins[str(image.relative_to(workspace))] = stable_digest(image)
                    reference_receipts.append({
                        'url': reference['source_url'], 'content_sha256': source['content_sha256'],
                        'raw_sha256': source['raw_sha256'], 'page': reference['page'],
                        'image_sha256': reference['image_sha256'],
                    })
            merged = replace_reviewed_corporate_facts(facts, reviewed)
            terms, issues = corporate_terms_intervals(merged)
            source_receipts = sorted(({k: row[k] for k in ('kind', 'sha256', 'url')}
                                      for row in archive.sources.values()), key=lambda value: (value['kind'], value['sha256']))
        finally:
            archive.conn.close()
    targets = {(row['product'], row['from_product'], row['effective_date']) for row in reviewed}
    selected = [row for row in terms if (row['product'], row['from_product'], row['effective_date']) in targets
                and set(row['source_content_sha256s']) & source_hashes]
    if not reviewed or not selected:
        raise ValueError('review did not resolve any matching source terms')
    if any(stable_digest(workspace / name) != digest for name, digest in pins.items()):
        raise ValueError('review input changed during audit')
    result = {
        'contract': 'tw_futures_visual_corporate_review_readback_v1',
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'reviewed_facts': reviewed, 'reviewed_position_facts': positions,
        'resolved_candidate_terms': selected,
        'source_issues': [row for row in issues if row.get('source_content_sha256') in source_hashes],
        'source_content_sha256s': sorted(source_hashes), 'source_receipts': source_receipts,
        'rounding_reference_receipts': reference_receipts,
        'review_images_match_original_page_renders': True,
        'input_pins': pins, 'financial_or_training_auto_adopted': False,
        'limitations': ['Reviewed source terms are not a complete holding-transfer or settlement proof.',
                        'Historical point-in-time availability and the account lifecycle remain separately gated.'],
    }
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--current', type=Path, required=True)
    parser.add_argument('--review', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(Path(__file__).resolve().parents[1], args.current, args.review, args.output)
    print(json.dumps({'reviewed_facts': len(result['reviewed_facts']),
                      'resolved_candidate_terms': len(result['resolved_candidate_terms']),
                      'source_issues': len(result['source_issues'])}))


if __name__ == '__main__':
    main()
