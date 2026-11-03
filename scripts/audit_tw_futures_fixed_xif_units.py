#!/usr/bin/env python3
"""Read back the legacy XIF fixed-unit rejection against retained official law."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import gzip
import hashlib
from io import BytesIO
import json
from pathlib import Path
import re
import unicodedata

import fitz
import polars as pl
import pyarrow.parquet as pq
from pypdf import PdfReader

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_portfolio_daily import _validate_fixed_contract_units


def _source_rule(pages: list[str]) -> bool:
    text = [re.sub(r'\s+', '', unicodedata.normalize('NFKC', page)) for page in pages]
    relevant = [p for p in text if '「臺灣證券交易所未含金融電子類股價指數期貨契約」規格修正對照表' in p]
    return (len(relevant) == 1 and '修正條文現行條文說明' in relevant[0]
            and '契約價值●非金電期貨指數乘上新臺幣10元'
                '契約價值●非金電期貨指數乘上新臺幣100元' in relevant[0])


def audit(daily: Path, event_report: Path) -> dict:
    daily, event_report = daily.resolve(), event_report.resolve()
    manifest = daily.parent / 'manifest.json'
    data_sha, manifest_sha, parent_sha = map(sha256_file, (daily, manifest, event_report))
    meta = json.loads(manifest.read_bytes())
    parent = json.loads(event_report.read_bytes())
    if (meta.get('contract_version') != 4
            or meta['outputs']['continuous_daily']['sha256'] != data_sha
            or parent.get('contract') != 'taifex_gold_option_resize_readback_v1'):
        raise ValueError('legacy or official evidence receipt differs')
    candidates = [Path(p) for p in parent['source_pins'] if Path(p).name.startswith(
        '908282d083b1ad96a91810d03ec26144de6e6aba8a251d9d4167c0162516dc6f.body')]
    if len(candidates) != 1 or sha256_file(candidates[0]) != parent['source_pins'][str(candidates[0])]:
        raise ValueError('official specification original differs')
    pdf = gzip.decompress(candidates[0].read_bytes())
    if hashlib.sha256(pdf).hexdigest() != '908282d083b1ad96a91810d03ec26144de6e6aba8a251d9d4167c0162516dc6f':
        raise ValueError('official specification PDF digest differs')
    with fitz.open(stream=pdf, filetype='pdf') as document:
        fitz_valid = _source_rule([page.get_text() for page in document])
    pypdf_valid = _source_rule([page.extract_text() for page in PdfReader(BytesIO(pdf)).pages])
    if not fitz_valid or not pypdf_valid:
        raise ValueError('two PDF extractors cannot corroborate the XIF change')
    frame = (pl.scan_parquet(daily).filter(pl.col('product') == 'XIF')
             .select('date', 'product', 'contract', 'contract_multiplier').collect())
    if frame.is_empty() or frame.filter(pl.col('date').is_null() | pl.col('contract_multiplier').is_null()).height:
        raise ValueError('nonempty dated legacy XIF rows required')
    arrow_rows = pq.read_table(daily, columns=['date', 'product', 'contract', 'contract_multiplier'],
                               filters=[('product', '=', 'XIF')]).to_pylist()
    expected = Counter((r['date'], r['contract'], r['contract_multiplier']) for r in arrow_rows)
    actual = Counter((r['date'], r['contract'], r['contract_multiplier']) for r in frame.to_dicts())
    if expected != actual:
        raise ValueError('independent parquet reader differs')
    before = frame.filter(pl.col('date') < date(2026, 7, 6))
    boundary = frame.filter(pl.col('date') == date(2026, 7, 6))
    later = frame.filter(pl.col('date') > date(2026, 7, 6))
    if before.is_empty() or boundary.is_empty() or later.is_empty():
        raise ValueError('legacy snapshot does not span the audited boundary')
    if frame['contract_multiplier'].unique().to_list() != [10.0]:
        raise ValueError('legacy snapshot is no longer a fixed 10 TWD XIF unit')
    rejected = []
    for label, scope in [('pre_boundary', before), ('boundary', boundary)]:
        try:
            _validate_fixed_contract_units(scope)
        except ValueError as error:
            if 'dated XIF multiplier and position split' not in str(error):
                raise
            rejected.append(label)
        else:
            raise ValueError('unsafe fixed-unit data were accepted')
    _validate_fixed_contract_units(later)
    pins = {str(daily): data_sha, str(manifest): manifest_sha, str(event_report): parent_sha,
            str(candidates[0]): parent['source_pins'][str(candidates[0])],
            str(Path(__file__).resolve()): sha256_file(Path(__file__)),
            str((Path.cwd() / 'stockagent/data/tw_futures_portfolio_daily.py').resolve()):
                sha256_file(Path('stockagent/data/tw_futures_portfolio_daily.py'))}
    if any(sha256_file(Path(p)) != h for p, h in pins.items()):
        raise ValueError('audit inputs changed')
    return {'contract': 'tw_legacy_xif_fixed_unit_rejection_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'input_pins': pins,
            'rows': frame.height, 'before_boundary_rows': before.height,
            'boundary_rows': boundary.height, 'later_rows': later.height,
            'earliest_legacy_date': str(frame['date'].min()), 'latest_legacy_date': str(frame['date'].max()),
            'legacy_multiplier_twd_per_point': 10,
            'official_immediately_before_multiplier': 100, 'official_new_multiplier': 10,
            'two_pdf_extractors_match_official_rule': True,
            'independent_parquet_rows_checked': len(arrow_rows), 'row_mismatches': 0,
            'rejected_scopes': rejected, 'post_boundary_gate_accepted': True,
            'limitations': ['The old notice gives the immediately preceding rule, not a full historical interval.',
                'The gate does not repair old artifacts or validate an account replay.',
                'The later gate acceptance does not prove lifecycle, price or training readiness.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('daily', 'event-report', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve previous readback')
    result = audit(args.daily, args.event_report)
    atomic_write_json(args.output, result)
    print(json.dumps({key: result[key] for key in ('rows', 'before_boundary_rows', 'boundary_rows',
          'later_rows', 'rejected_scopes', 'row_mismatches')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
