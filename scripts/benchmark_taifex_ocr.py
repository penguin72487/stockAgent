"""Benchmark the complete canonical OCR extraction and verify source/token parity.

Fresh output is mandatory. Rendering, evidence PNGs, native text, OCR text,
coordinates, hashing, receipts and model startup all remain in the timed path.
Run independent variants sequentially; never benchmark receipt-cache hits.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_json, sha256_file


def verify_receipt(directory):
    receipt = json.loads((directory/'receipt.json').read_text())
    if receipt.get('status') != 'complete' or not receipt.get('candidate_only') or receipt.get('point_in_time_verified'):
        raise ValueError(f'invalid OCR candidate receipt: {directory}')
    if receipt['content_sha256'] != directory.name or sha256_file(directory/'source.pdf') != directory.name:
        raise ValueError(f'OCR source hash mismatch: {directory}')
    for entry in receipt['files']:
        path=(directory/entry['path']).resolve()
        if not path.is_relative_to(directory.resolve()) or sha256_file(path)!=entry['sha256']:
            raise ValueError(f'OCR evidence hash mismatch: {path}')
    return receipt


def compare_outputs(reference, candidate):
    """Exact text/order/digits/geometry comparison; confidence drift is separate."""
    before={p.parent.name:p.parent for p in (reference/'documents').glob('*/receipt.json')}
    after={p.parent.name:p.parent for p in (candidate/'documents').glob('*/receipt.json')}
    result=dict(reference=str(reference),candidate=str(candidate),documents=len(after),pages=0,tokens=0,
                missing_documents=sorted(before.keys()-after.keys()),
                extra_documents=sorted(after.keys()-before.keys()),mismatches=[],max_score_delta=0.0)
    for key in sorted(before.keys() & after.keys()):
        left,right=before[key],after[key]
        a_receipt,b_receipt=verify_receipt(left),verify_receipt(right)
        pages_a={p.name:p for p in left.glob('page_*_ocr.json')}
        pages_b={p.name:p for p in right.glob('page_*_ocr.json')}
        if (pages_a.keys()!=pages_b.keys() or a_receipt['document_pages']!=b_receipt['document_pages']):
            result['mismatches'].append(dict(document=key,kind='page_set'))
        for page in sorted(pages_a.keys() & pages_b.keys()):
            a,b=json.loads(pages_a[page].read_text()),json.loads(pages_b[page].read_text())
            for tokens in [a,b]:
                if any(not math.isfinite(x['score']) or not 0<=x['score']<=1
                       or any(not math.isfinite(v) for point in x['box'] for v in point) for x in tokens):
                    raise ValueError(f'nonfinite/invalid OCR score or coordinate: {key}/{page}')
            result['pages']+=1;result['tokens']+=len(a)
            tests={
                'text_order':[x['txt'] for x in a]==[x['txt'] for x in b],
                'geometry':[x['box'] for x in a]==[x['box'] for x in b],
                'numeric_tokens':[re.findall(r'[0-9０-９]+(?:[.,．，][0-9０-９]+)*',x['txt']) for x in a]
                    ==[re.findall(r'[0-9０-９]+(?:[.,．，][0-9０-９]+)*',x['txt']) for x in b],
                'evidence_png':sha256_file(left/page.replace('_ocr.json','.png'))
                    ==sha256_file(right/page.replace('_ocr.json','.png')),
                'native_text':sha256_file(left/page.replace('_ocr.json','_native.json'))
                    ==sha256_file(right/page.replace('_ocr.json','_native.json')),
            }
            for kind,equal in tests.items():
                if not equal:result['mismatches'].append(dict(document=key,page=page,kind=kind))
            if len(a)==len(b):
                result['max_score_delta']=max(result['max_score_delta'],max(
                    (abs(x['score']-y['score']) for x,y in zip(a,b)),default=0))
        if (left/'candidate.txt').read_bytes()!=(right/'candidate.txt').read_bytes():
            result['mismatches'].append(dict(document=key,kind='candidate_text'))
    result['exact_text_geometry_sources']=bool(before) and not any(result[k] for k in
        ['missing_documents','extra_documents','mismatches'])
    return result


def benchmark(args):
    output=args.output_dir.resolve()
    if output.exists():
        raise ValueError('benchmark requires a fresh output directory')
    output.mkdir(parents=True)
    command=[sys.executable,'scripts/extract_taifex_rule_review_candidates.py',
        '--archive',str(args.archive),'--output-dir',str(output/'extraction'),
        '--rapidocr-config',str(args.config),'--workers','1','--ocr-dpi',str(args.dpi),'--force-ocr',
        '--document-sha256-file',str(args.selection)]
    for category in args.category or ['contract_adjustments']:
        command+=['--category',category]
    if args.profile:
        command+=['--ocr-profile-dir',str(output/'profiles')]
    atomic_write_json(output/'command.json',dict(command=command,config_sha256=sha256_file(args.config),
        selection_sha256=sha256_file(args.selection),benchmark_sha256=sha256_file(Path(__file__))))
    monitor=None
    process=None
    with (output/'device.csv').open('w') as telemetry, (output/'run.log').open('w') as log:
        try:
            monitor=subprocess.Popen(['nvidia-smi','--query-gpu=timestamp,memory.used,utilization.gpu,power.draw',
                '--format=csv','--loop-ms=1000'],stdout=telemetry,stderr=subprocess.DEVNULL)
            started=time.perf_counter()
            process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
            _,status,usage=os.wait4(process.pid,0)
            process.returncode=os.waitstatus_to_exitcode(status)
            wall=time.perf_counter()-started
        finally:
            if process is not None and process.poll() is None:
                process.terminate();process.wait()
            if monitor is not None:
                monitor.terminate();monitor.wait()
    result=dict(command=command,returncode=process.returncode,wall_s=wall,
        user_s=usage.ru_utime,system_s=usage.ru_stime,max_rss_kb=usage.ru_maxrss,
        config_sha256=sha256_file(args.config),device_csv_sha256=sha256_file(output/'device.csv'))
    manifest=output/'extraction/manifest.json'
    if manifest.exists():
        data=json.loads(manifest.read_text())
        result.update(manifest_sha256=sha256_file(manifest),status=data['status'],
                      documents=len(data['documents']),failures=data['failures'])
    if args.reference and process.returncode==0:
        result['comparison']=compare_outputs(args.reference,output/'extraction')
    atomic_write_json(output/'benchmark.json',result)
    print(json.dumps({k:v for k,v in result.items() if k not in ['command','comparison']},indent=2))
    return process.returncode or (1 if args.reference and not result['comparison']['exact_text_geometry_sources'] else 0)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--archive',type=Path,default=Path('artifacts/markets/tw_futures_v8_margin_preparation/all_products_source_archive_20260929'))
    p.add_argument('--selection',type=Path,required=True)
    p.add_argument('--output-dir',type=Path,required=True)
    p.add_argument('--reference',type=Path)
    p.add_argument('--dpi',type=int,default=300)
    p.add_argument('--category',action='append')
    p.add_argument('--profile',action='store_true')
    args=p.parse_args()
    return benchmark(args)


if __name__=='__main__':
    raise SystemExit(main())
