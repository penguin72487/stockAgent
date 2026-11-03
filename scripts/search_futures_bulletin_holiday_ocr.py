#!/usr/bin/env python3
"""Search retained historical futures association bulletins for holiday leads.

January sources retain their existing digest pins. February sources are new
captures, not previously verified originals. An output directory preserves the
PDF, acquisition metadata and OCR for either engine.
OCR is a search aid, not authoritative historical-account or absence evidence.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import time
from uuid import uuid4


SOURCES = (
    ('monthly_201101_99.pdf', '8c43d0f64b3fd19a4b9231fa79d1bae20dfd43f1c70a56e92f2cfc9305ce4017'),
    ('monthly_201201_111.pdf', '27b895b3c2311bd048ff386a2e798eb5423a506402259786c38485f5f7cd8832'),
    ('monthly_201102_100.pdf', None),
    ('monthly_201202_112.pdf', None),
)
DEFAULT_BULLETINS = tuple(filename for filename, _ in SOURCES[:2])
MAX_SOURCE_BYTES = 32 * 1024 * 1024
BASE_URL = 'https://www.futures.org.tw/static/uploadFile/monthly/'
TERMS = ('春節', '農曆', '休市', '結算', '交割', '一月十九', '一月三十一')
CUDA_CONFIG = 'configs/ocr/futures_bulletin_rapidocr_cuda13_v1.json'
DEFAULT_OUTPUT_ROOT = Path('artifacts/ocr/futures_bulletin_search')


def matching_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if any(term in line for term in TERMS)]


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', choices=('tesseract', 'rapidocr-cuda'), default='rapidocr-cuda')
    parser.add_argument('--rapidocr-config', default=CUDA_CONFIG)
    parser.add_argument('--bulletin', action='append', choices=[name for name, _ in SOURCES],
                        help='explicit retained source selection; defaults to the two January bulletins')
    parser.add_argument('--output-dir', type=Path,
                        help='new directory for original PDFs and page evidence; generated for CUDA when omitted')
    args = parser.parse_args(argv)

    started = time.perf_counter()
    engine = None
    output_dir = args.output_dir
    if args.engine == 'rapidocr-cuda':
        if output_dir is None:
            run_stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            output_dir = DEFAULT_OUTPUT_ROOT / f'{run_stamp}-{uuid4().hex}'
        selected_config = json.loads(Path(args.rapidocr_config).read_text())
        execution = selected_config.get('execution')
        if not isinstance(execution, dict) or execution.get('device') != 'cuda':
            raise ValueError('rapidocr-cuda requires a CUDA execution profile')
        # Runtime bootstrap must precede imports that may initialize a different
        # NumPy/ONNX binary stack. The CUDA runner fails closed on CPU fallback.
        output_dir.mkdir(parents=True, exist_ok=False)
        print(json.dumps({'state': 'cuda_search_started', 'output_dir': str(output_dir),
                          'candidate_only': True, 'training_ready': False}), flush=True)
        from stockagent.ocr.rapidocr import rapidocr_engine
        engine = rapidocr_engine(args.rapidocr_config, profile_dir=output_dir / 'profiles')
        if engine.budget.device != 'cuda':
            engine.close()
            raise ValueError('rapidocr-cuda requires a CUDA execution profile')
    elif output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=False)

    pages = []
    sources = []
    try:
        import fitz
        import requests
        from downloader.artifact_io import atomic_write_bytes, atomic_write_json
        _search_sources(args, engine, output_dir, pages, fitz, requests,
                        atomic_write_bytes, atomic_write_json, sources=sources)
        if output_dir is not None:
            gpu_evidence = engine.finish_profiling() if engine is not None else None
            receipt = dict(state='completed_search', engine=args.engine,
                           candidate_only=True, point_in_time_verified=False,
                           training_ready=False, source_absence_proven=False,
                           observed_at_utc=datetime.now(timezone.utc).isoformat(),
                           source_script_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
                           elapsed_before_receipt_s=time.perf_counter() - started,
                           sources=sources, pages=pages, gpu_evidence=gpu_evidence)
            atomic_write_json(output_dir / 'receipt.json', receipt)
        print(json.dumps({'state': 'completed_search', 'training_ready': False}), flush=True)
    finally:
        if engine is not None:
            engine.close()


def _read_source(response):
    response.raise_for_status()
    chunks = []
    size = 0
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        size += len(chunk)
        if size > MAX_SOURCE_BYTES:
            raise ValueError('bulletin source exceeds bounded capture size')
        chunks.append(chunk)
    return b''.join(chunks)


def _search_sources(args, engine, output_dir, pages, fitz, requests, write_bytes, write_json,
                    *, sources=None):
    selected = tuple(dict.fromkeys(getattr(args, 'bulletin', None) or DEFAULT_BULLETINS))
    pins = dict(SOURCES)
    if any(name not in pins for name in selected):
        raise ValueError('unknown bulletin source')
    for filename in selected:
        expected = pins[filename]
        url = BASE_URL + filename
        download_started = time.perf_counter()
        with requests.get(url, timeout=(10, 90), stream=True) as response:
            data = _read_source(response)
            headers = {key: response.headers.get(key) for key in ('Last-Modified', 'ETag', 'Content-Type')}
            response_url = response.url
        digest = sha256(data).hexdigest()
        if (expected is not None and digest != expected) or not data.startswith(b'%PDF-'):
            raise ValueError(f'source content changed: {url}')
        download_s = time.perf_counter() - download_started
        source = dict(url=url, response_url=response_url, sha256=digest, bytes=len(data),
                      expected_sha256=expected, source_digest_previously_pinned=expected is not None,
                      acquired_at_utc=datetime.now(timezone.utc).isoformat(), http_headers=headers,
                      file=filename if output_dir is not None else None, training_ready=False)
        if output_dir is not None:
            write_bytes(output_dir / filename, data)
            if sha256((output_dir / filename).read_bytes()).hexdigest() != digest:
                raise ValueError('retained original differs from captured source')
            write_json(output_dir / f'{filename}.acquisition.json', source)
        if sources is not None:
            sources.append(source)
        doc = fitz.open(stream=data, filetype='pdf')
        print(json.dumps({'state': 'source_verified' if expected is not None else 'source_observed',
                          'url': url, 'sha256': digest,
                          'bytes': len(data), 'pages': len(doc), 'training_ready': False}), flush=True)
        for index, page in enumerate(doc):
            render_started = time.perf_counter()
            pixmap = page.get_pixmap(dpi=140, alpha=False)
            image = pixmap.tobytes('png')
            render_s = time.perf_counter() - render_started
            if engine is None:
                result = subprocess.run(['tesseract', 'stdin', 'stdout', '-l', 'chi_tra+eng'],
                                        input=image, capture_output=True, check=True)
                raw_text = result.stdout
                text = raw_text.decode('utf-8', errors='strict')
            else:
                import numpy as np
                if pixmap.n != 3:
                    raise ValueError(f'expected RGB PDF render, got {pixmap.n} channels')
                # PyMuPDF renders RGB; RapidOCR's ndarray input uses BGR.
                rgb = np.frombuffer(pixmap.samples_mv, dtype=np.uint8).reshape(
                    pixmap.height, pixmap.stride)[:, :pixmap.width * 3].reshape(
                        pixmap.height, pixmap.width, 3)
                result = engine(np.ascontiguousarray(rgb[:, :, ::-1]))
                if not result.txts:
                    raise ValueError(f'RapidOCR returned no text on page {index + 1} of {filename}')
                text = '\n'.join(result.txts)
                raw_text = text.encode('utf-8')
            if output_dir is not None:
                stem = f'{Path(filename).stem}_page_{index + 1:03d}'
                write_bytes(output_dir / f'{stem}.png', image)
                write_bytes(output_dir / f'{stem}.txt', raw_text)
                if engine is not None:
                    write_json(output_dir / f'{stem}.json', result.to_json())
            record = {'state': 'ocr_searched', 'url': url, 'source_sha256': digest,
                      'page': index + 1, 'ocr_sha256': sha256(raw_text).hexdigest(),
                      'ocr_characters': len(text), 'matching_lines': matching_lines(text),
                      'note': 'OCR search only; do not infer absence or trading/account rules'}
            if output_dir is not None:
                record.update(engine=args.engine, candidate_only=True,
                              point_in_time_verified=False, training_ready=False,
                              dpi=140, image_sha256=sha256(image).hexdigest(),
                              image=f'{stem}.png', text=f'{stem}.txt',
                              source_download_s=download_s, render_s=render_s)
                if engine is not None:
                    record.update(blocks=f'{stem}.json',
                                  blocks_sha256=sha256((output_dir / f'{stem}.json').read_bytes()).hexdigest(),
                                  ocr_execution=dict(engine.last_call))
                pages.append(record)
            print(json.dumps(record, ensure_ascii=False), flush=True)
        doc.close()


if __name__ == '__main__':
    main()
