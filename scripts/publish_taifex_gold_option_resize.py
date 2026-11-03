#!/usr/bin/env python3
"""Publish a retrospective, source-pinned TGO contract-resize event.

This does not create an executable options account or a PIT training rule.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import gzip
import hashlib
from io import BytesIO
import json
from pathlib import Path
import sqlite3

import fitz
import polars as pl
from pypdf import PdfReader

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.check_acquisition_capacity import priority_dispatch_status
from scripts.publish_taifex_gold_option_settlements import parse as parse_gold_settlement
from stockagent.data.tw_option_contract_terms import gold_option_2026_resize


ROOT = Path(__file__).resolve().parents[1]
SPEC = '908282d083b1ad96a91810d03ec26144de6e6aba8a251d9d4167c0162516dc6f'
NOTICE = '6ad93de304a421bef461d026acf9a4e3068af6b9c098c43d37d66ec37e9b85b5'


def verified_notice(db: sqlite3.Connection, root: Path, digest: str) -> tuple[bytes, dict]:
    rows = db.execute('SELECT url,state,raw_path,raw_sha256,content_sha256,parsed_path,'
                      'parsing_status,checked_at FROM documents WHERE content_sha256=?',
                      (digest,)).fetchall()
    if len(rows) != 1:
        raise ValueError('expected exactly one retained official notice')
    url, state, raw_name, raw_sha, content_sha, parsed_name, parsing, checked = rows[0]
    raw, parsed = root / raw_name, root / parsed_name
    if (state != 'complete' or parsing != 'parsed' or content_sha != digest
            or raw.is_symlink() or parsed.is_symlink()
            or not raw.resolve().is_relative_to(root.resolve())
            or not parsed.resolve().is_relative_to(root.resolve())
            or sha256_file(raw) != raw_sha):
        raise ValueError('official notice receipt or path differs')
    payload = gzip.decompress(raw.read_bytes())
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError('official notice decompressed digest differs')
    doc = json.loads(parsed.read_bytes())
    if (doc.get('content_sha256') != digest or doc.get('raw_sha256') != raw_sha
            or doc.get('source_url') != url or doc.get('point_in_time_verified') is not False):
        raise ValueError('parsed notice source identity differs')
    return payload, {'url': url, 'raw_path': str(raw.resolve()), 'raw_sha256': raw_sha,
                     'content_sha256': digest, 'parsed_path': str(parsed.resolve()),
                     'parsed_sha256': sha256_file(parsed), 'captured_at_utc': checked,
                     'point_in_time_verified': False}


def option_code_evidence(manifest: Path) -> dict:
    doc = json.loads(manifest.read_bytes())
    raw, data = Path(doc['raw_path']), Path(doc['data_path'])
    if (doc.get('contract') != 'taifex_gold_option_final_settlement_source_v1'
            or doc.get('training_ready') is not False or raw.is_symlink() or data.is_symlink()
            or sha256_file(raw) != doc['raw_sha256'] or sha256_file(data) != doc['data_sha256']):
        raise ValueError('gold option identity evidence differs')
    parsed = parse_gold_settlement(raw.read_bytes(), date.fromisoformat(doc['requested_start']),
                                   date.fromisoformat(doc['requested_end']))
    original = pl.read_parquet(data)
    if not parsed.equals(original) or set(parsed['product_raw']) != {'TGO'}:
        raise ValueError('retained official TGO source does not read back')
    return {'manifest_path': str(manifest.resolve()), 'manifest_sha256': sha256_file(manifest),
            'raw_path': str(raw), 'raw_sha256': doc['raw_sha256'],
            'data_path': str(data), 'data_sha256': doc['data_sha256'], 'rows': parsed.height}


def futures_release_check(release: Path) -> dict:
    manifest = release / 'rules/manifest.json'
    rules = release / 'rules/rules.parquet'
    meta = json.loads(manifest.read_bytes())
    if (meta.get('status') != 'complete' or meta.get('research_only') is not True
            or sha256_file(rules) != meta['outputs']['rules']['sha256']):
        raise ValueError('research futures rules or receipt differ')
    prior = date(2026, 7, 3)
    current = date(2026, 7, 6)
    columns = ['product', 'contract', 'date', 'contract_multiplier', 'carry_from_date',
               'carry_quantity_numerator', 'carry_quantity_denominator', 'carry_previous_value_twd',
               'settlement_contract_value_twd']
    frame = (pl.scan_parquet(rules).filter(pl.col('product').is_in(['TGF', 'XIF'])
             & pl.col('date').is_in([prior, current])).select(columns).collect())
    previous = {tuple(r[k] for k in ('product', 'contract')): r
                for r in frame.filter(pl.col('date') == prior).to_dicts()}
    today = frame.filter(pl.col('date') == current).to_dicts()
    if not today or {r['product'] for r in today} != {'TGF', 'XIF'}:
        raise ValueError('expected both futures product boundaries')
    counts: dict[str, int] = {}
    for row in today:
        p = previous.get((row['product'], row['contract']))
        if (not p or p['contract_multiplier'] != 100 or row['contract_multiplier'] != 10
                or row['carry_from_date'] != prior or row['carry_quantity_numerator'] != 10
                or row['carry_quantity_denominator'] != 1
                or row['carry_previous_value_twd'] != p['settlement_contract_value_twd']):
            raise ValueError('futures release boundary does not preserve prior contract value')
        counts[row['product']] = counts.get(row['product'], 0) + 1
    return {'manifest_path': str(manifest.resolve()), 'manifest_sha256': sha256_file(manifest),
            'rules_path': str(rules.resolve()), 'rules_sha256': meta['outputs']['rules']['sha256'],
            'previous_date': prior.isoformat(), 'effective_date': current.isoformat(),
            'matched_contracts': counts,
            'scope': 'readback of prepared rule rows only, not a training or account replay'}


def publish(root: Path, gold_manifest: Path, futures_release: Path, output: Path, report: Path) -> dict:
    if output.is_symlink() or report.is_symlink():
        raise ValueError('symlinked output paths are not accepted')
    root, gold_manifest, futures_release = (p.resolve() for p in (root, gold_manifest, futures_release))
    output, report = output.resolve(), report.resolve()
    if (not output.is_relative_to(ROOT / 'data_tw_index_options_daily/research')
            or not report.is_relative_to(ROOT / 'artifacts/markets')
            or output.exists() or report.exists() or output.is_symlink() or report.is_symlink()):
        raise ValueError('new research dataset and report locations required')
    ancestor = output.parent
    while not ancestor.exists():
        ancestor = ancestor.parent
    capacity = priority_dispatch_status(ancestor, additional_headroom_bytes=16 * 1024**2)
    if capacity.get('state') not in {'admitted', 'not_enrolled'}:
        raise RuntimeError('research event capacity not admitted: ' + str(capacity.get('reason')))
    code_paths = [Path(__file__).resolve(), ROOT / 'stockagent/data/tw_option_contract_terms.py',
                  ROOT / 'scripts/publish_taifex_gold_option_settlements.py']
    pins = {str(p): sha256_file(p) for p in code_paths}
    with sqlite3.connect(f'file:{root / "state/queue.sqlite3"}?mode=ro', uri=True) as db:
        spec_pdf, spec = verified_notice(db, root, SPEC)
        notice_pdf, notice = verified_notice(db, root, NOTICE)
    def pymupdf_pages(payload: bytes) -> list[str]:
        with fitz.open(stream=payload, filetype='pdf') as doc:
            return [page.get_text() for page in doc]
    def pypdf_pages(payload: bytes) -> list[str]:
        return [page.extract_text() for page in PdfReader(BytesIO(payload)).pages]
    event = gold_option_2026_resize(pymupdf_pages(spec_pdf), pymupdf_pages(notice_pdf))
    independent = gold_option_2026_resize(pypdf_pages(spec_pdf), pypdf_pages(notice_pdf))
    if event != independent:
        raise ValueError('independent PDF extractions disagree')
    gold = option_code_evidence(gold_manifest)
    futures = futures_release_check(futures_release)
    for p, h in pins.items():
        if sha256_file(Path(p)) != h:
            raise ValueError('implementation changed during preparation')
    source_paths = [spec['raw_path'], spec['parsed_path'], notice['raw_path'], notice['parsed_path'],
                    gold['manifest_path'], gold['raw_path'], gold['data_path'],
                    futures['manifest_path'], futures['rules_path']]
    source_pins = {p: sha256_file(Path(p)) for p in source_paths}
    event_doc = {'contract': 'taifex_gold_option_resize_candidate_v1',
                 'created_at_utc': datetime.now(timezone.utc).isoformat(), 'event': event,
                 'official_documents': [spec, notice], 'official_product_code_source': gold,
                 'implementation_sha256': pins, 'training_ready': False,
                 'limitations': ['Retrospective capture does not prove point-in-time availability.',
                     'Old and new are event-side values, not verified complete effective intervals.',
                     'The source states before opening, not an exact adjustment timestamp.',
                     'Margin, exercise method, fees, liquidity, execution and option account are separate.']}
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, event_doc)
    if json.loads(output.read_bytes()) != event_doc:
        raise ValueError('candidate event readback differs')
    if any(sha256_file(Path(p)) != h for p, h in source_pins.items()):
        raise ValueError('source changed during readback')
    result = {'contract': 'taifex_gold_option_resize_readback_v1',
              'event_path': str(output), 'event_sha256': sha256_file(output),
              'event': event, 'pdf_extraction_methods': ['PyMuPDF', 'pypdf'],
              'independent_pdf_results_equal': True, 'source_pins': source_pins,
              'futures_release_boundary': futures, 'training_ready': False}
    atomic_write_json(report, result)
    if json.loads(report.read_bytes()) != result:
        raise ValueError('event report readback differs')
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('rules-root', 'gold-manifest', 'futures-release', 'output', 'report'):
        p.add_argument('--' + name, type=Path, required=True)
    a = p.parse_args()
    result = publish(a.rules_root, a.gold_manifest, a.futures_release, a.output, a.report)
    print(json.dumps({'event': result['event'], 'futures_release_boundary':
                      result['futures_release_boundary'], 'report': str(a.report)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
