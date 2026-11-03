#!/usr/bin/env python3
"""Retain scoped source evidence for a TXO postponement, without admitting it to training."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
from html.parser import HTMLParser
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import unicodedata

import fitz
from pypdf import PdfReader

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.check_acquisition_capacity import priority_dispatch_status

ROOT = Path(__file__).resolve().parents[1]
TYPHOON_URL = 'https://www.taifex.com.tw/cht/11/newsDetail?newsType=1&idx=17441'
TYPHOON_SHA = 'af6622a8c4ee2e00818b109cf537a9c616a65a2812df6edb0312d3b7a98deba5'
HOLIDAY_URL = 'https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/台期交字第1150201963號函.pdf'
HOLIDAY_SHA = '9ce9701301323b76eddbe0b243ce9bfa48d542fb6e6fd5cc710a34d666e68b14'
DECISION = datetime.fromisoformat('2026-10-27T08:45:00+08:00')


class Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def compact(text: str) -> str:
    return ''.join(unicodedata.normalize('NFKC', text).split())


def notice_facts(html: bytes, pdf_text: str) -> dict:
    parsed = Text()
    parsed.feed(html.decode('utf-8', errors='strict'))
    typhoon, holiday = compact(''.join(parsed.parts)), compact(pdf_text)
    required = ('115年10月23日', '臺指選擇權週五到期契約',
                '最後交易日及最後結算日順延至最近之次一營業日', '2026/10/22')
    if any(term not in typhoon for term in required):
        raise ValueError('specific official typhoon notice is not established')
    holiday_required = ('1150201963', '10月26日(星期一)補假2日', '期貨集中交易市場配合休市2日')
    if any(term not in holiday for term in holiday_required):
        raise ValueError('specific official holiday notice is not established')
    return {'nominal_friday': '2026-10-23', 'relative_rule': 'next_business_day',
            'october_26_closed': True, 'candidate_if_no_additional_closure': '2026-10-27'}


def version(db: sqlite3.Connection, root: Path, url: str, digest: str) -> tuple[bytes, dict]:
    rows = db.execute('SELECT captured_at,raw_path,raw_sha256,parsed_path FROM versions '
                      'WHERE url=? AND content_sha256=?', (url, digest)).fetchall()
    if len(rows) != 1:
        raise ValueError('exact official version required')
    captured, raw_name, raw_sha, parsed_name = rows[0]
    timestamp = datetime.fromisoformat(captured)
    if timestamp.tzinfo is None or timestamp >= DECISION:
        raise ValueError('retained version must predate the decision')
    raw, parsed = root / raw_name, root / parsed_name
    if (raw.is_symlink() or parsed.is_symlink() or not raw.resolve().is_relative_to(root.resolve())
            or not parsed.resolve().is_relative_to(root.resolve()) or sha256_file(raw) != raw_sha):
        raise ValueError('official source path or compressed digest differs')
    payload = gzip.decompress(raw.read_bytes())
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError('official decompressed digest differs')
    doc = json.loads(parsed.read_bytes())
    if (doc.get('source_url') != url or doc.get('raw_sha256') != raw_sha
            or doc.get('content_sha256') != digest or doc.get('captured_at_utc') != captured
            or doc.get('point_in_time_verified') is not False):
        raise ValueError('captured version and parsed receipt differ')
    return payload, {'url': url, 'captured_at_utc': captured, 'raw_path': str(raw.resolve()),
                     'raw_sha256': raw_sha, 'content_sha256': digest,
                     'parsed_path': str(parsed.resolve()), 'parsed_sha256': sha256_file(parsed)}


def publish(root: Path, output: Path, report: Path) -> dict:
    root, output, report = root.resolve(), output.resolve(), report.resolve()
    if (not output.is_relative_to(ROOT / 'data_tw_index_options_daily/research')
            or not report.is_relative_to(ROOT / 'artifacts/markets')
            or output.exists() or report.exists()):
        raise ValueError('new research event and report paths required')
    ancestor = output.parent
    while not ancestor.exists():
        ancestor = ancestor.parent
    capacity = priority_dispatch_status(ancestor, additional_headroom_bytes=16 * 1024**2)
    if capacity.get('state') not in {'admitted', 'not_enrolled'}:
        raise RuntimeError('research event capacity not admitted: ' + str(capacity.get('reason')))
    script_hash = sha256_file(Path(__file__))
    with sqlite3.connect(f'file:{root / "state/queue.sqlite3"}?mode=ro', uri=True) as db:
        typhoon, typhoon_receipt = version(db, root, TYPHOON_URL, TYPHOON_SHA)
        holiday, holiday_receipt = version(db, root, HOLIDAY_URL, HOLIDAY_SHA)
    with fitz.open(stream=holiday, filetype='pdf') as pdf:
        fitz_text = '\n'.join(page.get_text() for page in pdf)
    pypdf_text = '\n'.join(page.extract_text() for page in PdfReader(BytesIO(holiday)).pages)
    facts, independent = notice_facts(typhoon, fitz_text), notice_facts(typhoon, pypdf_text)
    if facts != independent or script_hash != sha256_file(Path(__file__)):
        raise ValueError('independent extraction or script identity differs')
    evidence = [typhoon_receipt, holiday_receipt]
    source_pins = {d[k]: d[k.replace('_path', '_sha256')] for d in evidence
                   for k in ('raw_path', 'parsed_path')}
    event = {'contract': 'taifex_txo_friday_20261023_source_event_v1',
             'created_at_utc': datetime.now(timezone.utc).isoformat(),
             'facts': facts, 'official_documents': evidence, 'implementation_sha256': script_hash,
             'point_in_time_verified': False, 'training_ready': False,
             'limitations': ['Earlier retained source versions do not prove model delivery or completeness.',
                 'The relative rule does not explicitly name October 27; another closure cannot be excluded.',
                 'Product-series generation, listing, executable prices and accounting require separate evidence.']}
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, event)
    if json.loads(output.read_bytes()) != event or any(sha256_file(Path(p)) != h for p, h in source_pins.items()):
        raise ValueError('event or official source changed during readback')
    result = {'contract': 'taifex_txo_friday_20261023_source_readback_v1',
              'event_path': str(output), 'event_sha256': sha256_file(output),
              'source_pins': source_pins, 'pdf_extraction_methods': ['PyMuPDF', 'pypdf'],
              'independent_pdf_results_equal': True, 'facts': facts,
              'point_in_time_verified': False, 'training_ready': False}
    atomic_write_json(report, result)
    if json.loads(report.read_bytes()) != result:
        raise ValueError('report readback differs')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rules-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(publish(args.rules_root, args.output, args.report), ensure_ascii=False))


if __name__ == '__main__':
    main()
