#!/usr/bin/env python3
"""Recover source-bound PDF pages for rule review, never executable rules.

Uses the canonical verified archive and atomic writers. Independent documents
are decoded concurrently; source bytes and per-page TSV coordinates are retained.
This does not mutate the collector database or require another HTTP collector.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import cache
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
# Canonical collector imports can load NumPy. Select the isolated bounded OCR
# runtime before those imports, rather than claiming a new wheel's metadata
# while retaining a previously loaded binary module from the training runtime.
if __name__ == '__main__':
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument('--rapidocr-config', type=Path)
    bootstrap_args, _ = bootstrap.parse_known_args()
    if bootstrap_args.rapidocr_config:
        bootstrap_config = json.loads(bootstrap_args.rapidocr_config.read_text())
        if 'execution' in bootstrap_config:
            from stockagent.ocr.rapidocr import rapidocr_runtime
            rapidocr_runtime(bootstrap_args.rapidocr_config)
from downloader.artifact_io import atomic_write_json, atomic_write_bytes, sha256_file
from downloader.taifex_rule_parsing import temporal_mentions
from scripts.download_taifex_rule_history import read_verified_raw
from stockagent.ocr.rapidocr import rapidocr_runtime, rapidocr_engine


PROFILE = 'pdf_review_pymupdf_1.27.2.2_tesseract_chi_tra_eng_psm3_200dpi_process_v2'
TABLE_PROFILE = 'pdf_native_tables_pymupdf_1.27.2.2_v1'


@cache
def runtime_profile(tessdata_dir=None, languages='chi_tra+eng'):
    version=subprocess.run(['tesseract','--version'],capture_output=True,check=True).stdout.decode().splitlines()[0]
    langs=subprocess.run(['tesseract','--list-langs'],capture_output=True,check=True).stdout.decode()
    import re
    location=re.search(r'"([^"]+)"',langs)
    if location is None or not {'chi_tra','eng'} <= set(langs.splitlines()):
        raise ValueError('Tesseract chi_tra and eng language data are required')
    root=Path(tessdata_dir) if tessdata_dir is not None else Path(location[1])
    names=languages.split('+')
    if not names or set(names)-{'chi_tra','eng'}:
        raise ValueError('only the retained Chinese/English OCR models are supported')
    if tessdata_dir is not None and 'chi_tra' in names:
        # tessdata_best/chi_tra requests this sublanguage internally. Bind it
        # too rather than accepting a successful exit with a missing model.
        names.append('chi_tra_vert')
    return dict(tesseract=version,language_sha256={name:sha256_file(root/(name+'.traineddata'))
                                                for name in names})


def extraction_profile(dpi=200, psm=3, languages='chi_tra+eng', tessdata_dir=None, force_ocr=False):
    if (dpi,psm,languages,tessdata_dir,force_ocr)==(200,3,'chi_tra+eng',None,False):
        return PROFILE
    return f'pdf_review_pymupdf_1.27.2.2_tesseract_{languages}_psm{psm}_{dpi}dpi_force{int(force_ocr)}_v3'


def rapidocr_review_profile(config_path, dpi, force_ocr):
    config, runtime = rapidocr_runtime(config_path)
    digest = sha256_file(config_path)
    if 'execution' in config:
        runtime = dict(runtime, extractor_sha256=sha256_file(Path(__file__)))
        digest = hashlib.sha256(json.dumps(runtime, sort_keys=True).encode()).hexdigest()
    return f'pdf_review_rapidocr_{digest}_{dpi}dpi_force{int(force_ocr)}_v1'


def review_documents(conn, categories, pending_only=False):
    """Include direct-file notices as well as attachments of HTML notices."""
    placeholders = ','.join('?' for _ in categories)
    query = '''SELECT d.*,a.published_date,a.title,a.category,a.url announcement_url
        FROM documents d JOIN (
            SELECT url parent,url child FROM announcements
            UNION SELECT parent,child FROM links
        ) l ON l.child=d.url JOIN announcements a ON a.url=l.parent
        WHERE a.category IN (''' + placeholders + ''')
        AND d.state='complete' AND lower(d.url) LIKE '%.pdf'
        ORDER BY a.published_date,d.url'''
    items = {}
    for row in conn.execute(query, categories):
        r = dict(row)
        if pending_only and r['parsing_status'] not in {'pending_ocr', 'partial', 'failed'}:
            continue
        key = r['content_sha256']
        item = items.setdefault(key, dict(document={k:r[k] for k in r if k not in
            ['published_date', 'title', 'category', 'announcement_url']}, announcements=[]))
        item['announcements'].append(dict(published_date=r['published_date'], title=r['title'],
                                          category=r['category'], url=r['announcement_url']))
    priority = {name:i for i,name in enumerate(['margins','position_limits','contract_specs',
        'contract_adjustments','calendar','settlement','other_announcements'])}
    return dict(sorted(items.items(), key=lambda pair:min(
        (priority[a['category']], a['published_date'], a['url']) for a in pair[1]['announcements'])))


def is_blank_render(pixmap, native_text: str) -> bool:
    """An empty OCR result is valid only for an exactly white source render.

    No ink threshold: a single faint nonwhite pixel keeps the page unresolved.
    Native text also prevents treating invisible/clipped source text as blank.
    """
    if native_text.strip():
        return False
    import numpy as np
    return bool(np.all(np.frombuffer(pixmap.samples_mv, dtype=np.uint8) == 255))


def extract(root: Path, out: Path, item: dict, max_pages: int, *, dpi=200, psm=3,
            languages='chi_tra+eng', tessdata_dir=None, force_ocr=False, rapidocr_config=None,
            rapidocr_profile_dir=None) -> dict:
    import pymupdf
    source = item['document']
    profile=(rapidocr_review_profile(rapidocr_config, dpi, force_ocr)
             if rapidocr_config else extraction_profile(dpi,psm,languages,tessdata_dir,force_ocr))
    runtime=rapidocr_runtime(rapidocr_config)[1] if rapidocr_config else runtime_profile(tessdata_dir,languages)
    bounded = rapidocr_config and 'execution' in rapidocr_runtime(rapidocr_config)[0]
    if bounded:
        runtime = dict(rapidocr_engine(rapidocr_config, rapidocr_profile_dir).runtime,
                       extractor_sha256=sha256_file(Path(__file__)))
    body = read_verified_raw(root, source)
    if not body.startswith(b'%PDF-'):
        return dict(url=source['url'], status='not_pdf')
    dest = out / 'documents' / source['content_sha256']
    receipt_path = dest / 'receipt.json'
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
        if (receipt['profile'] != profile or receipt['content_sha256'] != source['content_sha256']
                or receipt.get('runtime') != runtime):
            raise ValueError('review receipt belongs to another extraction profile')
        for record in receipt['files']:
            if sha256_file(dest / record['path']) != record['sha256']:
                raise ValueError('review output hash mismatch')
        return receipt
    dest.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(dest / 'source.pdf', body)
    pages, chunks = [], []
    with pymupdf.open(stream=body, filetype='pdf') as pdf:
        count = len(pdf)
        for index in range(min(count, max_pages)):
            page = pdf[index]
            text = page.get_text(sort=True)
            words = page.get_text('words', sort=True)
            record = dict(page=index+1, extraction='native', width=page.rect.width,
                          height=page.rect.height, native_word_count=len(words))
            # A mixed text+scanned-table page still needs OCR. Preserve the
            # native extraction separately instead of discarding either view.
            images = page.get_images(full=True)
            image_area = sum(rect.get_area() for img in images for rect in page.get_image_rects(img[0]))
            needs_ocr = force_ocr or len(text.strip()) < 50 or image_area > page.rect.get_area() * .35
            atomic_write_json(dest / f'page_{index+1:03d}_native.json', {'text':text,'words':words})
            if needs_ocr:
                if page.rect.get_area() * (dpi/72)**2 > 25_000_000:
                    raise ValueError('OCR page pixel bound exceeded')
                png = dest / f'page_{index+1:03d}.png'
                render_started = time.perf_counter()
                pixmap = page.get_pixmap(dpi=dpi, colorspace=pymupdf.csGRAY, alpha=False)
                render_s = time.perf_counter() - render_started
                save_started = time.perf_counter()
                pixmap.save(png)
                save_s = time.perf_counter() - save_started
                if is_blank_render(pixmap, text):
                    record.update(extraction='verified_blank_render', blank_check='all_pixels_255_no_native_text')
                    chunks.append(f'\n[PAGE {index+1}]\n')
                    pages.append(record)
                    continue
                output = dest / f'page_{index+1:03d}_ocr'
                if rapidocr_config:
                    engine=rapidocr_engine(rapidocr_config, rapidocr_profile_dir)
                    if bounded and engine.optimizations.direct_pixels:
                        import numpy as np
                        # Keep the exact evidence PNG, but avoid rereading and
                        # decompressing it for inference. Retain pixmap until
                        # the engine finishes consuming its zero-copy view.
                        pixels = np.frombuffer(pixmap.samples_mv, dtype=np.uint8).reshape(
                            pixmap.height, pixmap.stride)[:, :pixmap.width]
                        result=engine(pixels)
                    else:
                        result=engine(str(png))
                    if not result.txts:
                        raise ValueError(f'RapidOCR returned no text on page {index+1}')
                    text='\n'.join(result.txts)
                    atomic_write_bytes(output.with_suffix('.txt'),text.encode())
                    atomic_write_json(output.with_suffix('.json'),result.to_json())
                    record['ocr_backend']='rapidocr'
                    record['extraction']='ocr_candidate'
                    if bounded:
                        record['ocr_execution']=dict(engine.last_call)
                        record['ocr_execution']['render_s']=render_s
                        record['ocr_execution']['png_write_s']=save_s
                else:
                    model_args=['--tessdata-dir',str(tessdata_dir)] if tessdata_dir is not None else []
                    proc = subprocess.run(['tesseract',str(png),str(output),*model_args,'-l',languages,
                                       '--psm',str(psm),'-c','tessedit_create_txt=1',
                                       '-c','tessedit_create_tsv=1'],
                                      env=dict(os.environ, OMP_THREAD_LIMIT='1'),
                                      capture_output=True, timeout=180, check=False)
                    if (proc.returncode or b'Failed loading language' in proc.stderr
                        or not output.with_suffix('.txt').is_file()
                        or not output.with_suffix('.tsv').is_file()):
                        raise ValueError(f'OCR failed on page {index+1}: exit {proc.returncode}')
                    text = output.with_suffix('.txt').read_text()
                    record['extraction'] = 'ocr_candidate'
                    record['ocr_stderr'] = proc.stderr.decode('utf-8', errors='replace')[:300]
            chunks.append(f'\n[PAGE {index+1}]\n{text}')
            pages.append(record)
    text = '\n'.join(chunks)
    atomic_write_bytes(dest / 'candidate.txt', text.encode())
    receipt = dict(profile=profile, runtime=runtime, url=source['url'], content_sha256=source['content_sha256'],
                   source_raw_sha256=source['raw_sha256'], status='complete' if count <= max_pages else 'partial',
                   document_pages=count, extracted_pages=len(pages), pages=pages,
                   candidate_only=True, point_in_time_verified=False,
                   temporal_mentions=temporal_mentions(text),
                   files=[dict(path=str(p.relative_to(dest)),sha256=sha256_file(p),bytes=p.stat().st_size)
                          for p in sorted(dest.iterdir()) if p.is_file() and p.name!='receipt.json'])
    atomic_write_json(receipt_path, receipt)
    return receipt


def ruled_ocr_tables(pixels, tokens):
    """Recover source cells with a bounded five-pixel scan-gap repair.

    Geometry improves reading order only. OCR numbers remain candidates.
    OCR polygons include whitespace around glyphs and can slightly cross a
    scanned rule; permit at most eight pixels, bounded by cell size, with a
    unique containing cell. A merged token spanning cells stays excluded.
    """
    import cv2
    import numpy as np
    if pixels.ndim!=2:raise ValueError('OCR grid requires a grayscale page')
    height,width=pixels.shape
    ink=cv2.threshold(pixels,0,255,cv2.THRESH_BINARY_INV|cv2.THRESH_OTSU)[1]
    # Scanned thin rules have JPEG/scan pinholes; opening a long line before
    # closing those pinholes deletes real source borders. Repair only <=4px
    # raster gaps, keeping the original page and OCR polygons untouched.
    ink=cv2.morphologyEx(ink,cv2.MORPH_CLOSE,
        cv2.getStructuringElement(cv2.MORPH_RECT,(5,5)))
    horizontal=cv2.morphologyEx(ink,cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_RECT,(max(30,width//35),1)))
    vertical=cv2.morphologyEx(ink,cv2.MORPH_OPEN,
        cv2.getStructuringElement(cv2.MORPH_RECT,(1,max(30,height//60))))
    grid=cv2.morphologyEx(cv2.bitwise_or(horizontal,vertical),cv2.MORPH_CLOSE,
        cv2.getStructuringElement(cv2.MORPH_RECT,(5,5)))
    contours,hierarchy=cv2.findContours(grid,cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)
    if hierarchy is None:return []
    groups={}
    for i,contour in enumerate(contours):
        parent=int(hierarchy[0,i,3])
        if parent<0:continue
        x,y,w,h=cv2.boundingRect(contour)
        if w<width*.035 or h<height*.006 or cv2.contourArea(contour)<w*h*.85:continue
        groups.setdefault(parent,[]).append((x,y,x+w,y+h))
    tables=[]
    def edges(values):
        clusters=[]
        for value in sorted(values):
            if not clusters or value-clusters[-1][-1]>8:clusters.append([value])
            else:clusters[-1].append(value)
        return [sum(c)/len(c) for c in clusters]
    for cells in groups.values():
        if len(cells)<4:continue
        xs=edges([edge for box in cells for edge in (box[0],box[2])])
        ys=edges([edge for box in cells for edge in (box[1],box[3])])
        if len(xs)<3 or len(ys)<3:continue
        rows=[[None]*(len(xs)-1) for _ in range(len(ys)-1)]
        assignments=[];excluded=[]
        for box in cells:
            x0,y0,x1,y1=box;inside=[]
            for index,token in enumerate(tokens):
                polygon=np.asarray(token['box'],dtype=float)
                left,top=polygon.min(axis=0);right,bottom=polygon.max(axis=0)
                center=polygon.mean(axis=0)
                if not (x0<=center[0]<=x1 and y0<=center[1]<=y1):continue
                overlap=max(0,min(x1,right)-max(x0,left))*max(0,min(y1,bottom)-max(y0,top))
                if overlap/max((right-left)*(bottom-top),1)<.95:
                    def contains_with_padding(cell):
                        a,b,c,d=cell;pad=min(8.,.12*min(c-a,d-b))
                        return a-pad<=left and right<=c+pad and b-pad<=top and bottom<=d+pad
                    candidates=[cell for cell in cells if contains_with_padding(cell)]
                    if candidates!=[box]:
                        excluded.append(index);continue
                inside.append((float(center[1]),float(left),index,token['txt']))
            inside.sort()
            row=min(range(len(ys)-1),key=lambda j:abs(ys[j]-y0))
            col=min(range(len(xs)-1),key=lambda j:abs(xs[j]-x0))
            if rows[row][col] is not None:raise ValueError('overlapping OCR grid cells')
            rows[row][col]='\n'.join(t[3] for t in inside)
            assignments.append(dict(bbox=list(box),row=row,column=col,token_indices=[t[2] for t in inside]))
        bbox=[min(x[0] for x in cells),min(x[1] for x in cells),max(x[2] for x in cells),max(x[3] for x in cells)]
        caption=[]
        for token in tokens:
            p=np.asarray(token['box']);top=float(p[:,1].min());bottom=float(p[:,1].max())
            if max(0,bbox[1]-height*.15)<=top and (top+bottom)/2<bbox[1]:
                caption.append((top,float(p[:,0].min()),token['txt']))
        tables.append(dict(bbox=bbox,cells=rows,caption='\n'.join(t[2] for t in sorted(caption)),
            cell_evidence=assignments,excluded_boundary_tokens=sorted(set(excluded)),
            raster_gap_repair_kernel=[5,5],
            token_whitespace_padding_max_pixels=8,
            token_whitespace_padding_cell_fraction=.12,
            extraction_method='source_ruled_ocr_cell_grid',candidate_only=True))
    return sorted(tables,key=lambda t:(t['bbox'][1],t['bbox'][0]))


def extract_ocr_tables(root: Path, out: Path, item: dict, max_pages: int, *, review_root: Path,
                       rapidocr_config: Path) -> dict:
    rapidocr_runtime(rapidocr_config)  # Isolated, version-bound cv2; no inference.
    import cv2
    source=item['document'];folder=review_root/'documents'/source['content_sha256']
    proof=json.loads((folder/'receipt.json').read_text())
    if proof['status']!='complete' or proof['content_sha256']!=source['content_sha256']:
        raise ValueError('OCR tables require a complete same-source page receipt')
    read_verified_raw(root,source)
    for record in proof['files']:
        path=folder/record['path']
        if not path.resolve().is_relative_to(folder.resolve()) or sha256_file(path)!=record['sha256']:
            raise ValueError('OCR table source page SHA mismatch')
    pages=[]
    for page in proof['pages']:
        index=page['page']
        native=json.loads((folder/f'page_{index:03d}_native.json').read_text())['text']
        if page.get('extraction') == 'verified_blank_render':
            import numpy as np
            pixels=cv2.imread(str(folder/f'page_{index:03d}.png'),cv2.IMREAD_GRAYSCALE)
            if pixels is None or native.strip() or not bool(np.all(pixels == 255)):
                raise ValueError('blank source page evidence differs from its receipt')
            pages.append(dict(page=index,native_text='',ocr_text='',tables=[],
                extraction_method='verified_blank_render'))
            continue
        if page.get('ocr_backend')!='rapidocr':
            raise ValueError('OCR grid expects retained RapidOCR polygons')
        tokens=json.loads((folder/f'page_{index:03d}_ocr.json').read_text())
        pixels=cv2.imread(str(folder/f'page_{index:03d}.png'),cv2.IMREAD_GRAYSCALE)
        if pixels is None:raise ValueError('unreadable OCR page raster')
        tables=ruled_ocr_tables(pixels,tokens)
        pages.append(dict(page=index,native_text=native,ocr_text='\n'.join(t['txt'] for t in tokens),
            tables=tables,extraction_method='source_ruled_ocr_cell_grid'))
    dest=out/'documents'/source['content_sha256'];dest.mkdir(parents=True,exist_ok=True)
    if (dest/'receipt.json').exists():raise FileExistsError('OCR grid receipts are immutable')
    atomic_write_json(dest/'tables.json',dict(pages=pages,candidate_only=True))
    result=dict(profile='source_ruled_ocr_cell_grid_v3',url=source['url'],
        content_sha256=source['content_sha256'],source_raw_sha256=source['raw_sha256'],
        status='complete',document_pages=len(pages),extracted_pages=len(pages),
        candidate_only=True,point_in_time_verified=False,table_count=sum(len(p['tables']) for p in pages),
        source_page_receipt_sha256=sha256_file(folder/'receipt.json'),
        extractor_sha256=sha256_file(Path(__file__)),
        files=[dict(path='tables.json',sha256=sha256_file(dest/'tables.json'),bytes=(dest/'tables.json').stat().st_size)])
    atomic_write_json(dest/'receipt.json',result)
    return result


def extract_tables(root: Path, out: Path, item: dict, max_pages: int) -> dict:
    """Keep cell geometry separate from OCR; a scan cannot become native data."""
    import pymupdf
    source = item['document']
    body = read_verified_raw(root, source)
    if not body.startswith(b'%PDF-'):
        raise ValueError('official PDF URL does not contain a PDF')
    dest = out / 'documents' / source['content_sha256']
    receipt_path = dest / 'receipt.json'
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
        if receipt['profile'] != TABLE_PROFILE or receipt['content_sha256'] != source['content_sha256']:
            raise ValueError('native table receipt profile/source mismatch')
        for record in receipt['files']:
            if sha256_file(dest / record['path']) != record['sha256']:
                raise ValueError('native table output SHA mismatch')
        return receipt
    pages = []
    with pymupdf.open(stream=body, filetype='pdf') as pdf:
        count = len(pdf)
        for index in range(min(count, max_pages)):
            page = pdf[index]
            text = page.get_text(sort=True)
            tables = []
            if len(text.strip()) >= 50:
                for table in page.find_tables().tables:
                    box = pymupdf.Rect(table.bbox)
                    caption = page.get_text(clip=pymupdf.Rect(
                        0, max(0, box.y0 - 100), page.rect.width, box.y0), sort=True)
                    tables.append(dict(bbox=list(table.bbox), cells=table.extract(), caption=caption))
            pages.append(dict(page=index+1, native_text=text, tables=tables,
                              has_images=bool(page.get_images(full=True))))
    dest.mkdir(parents=True, exist_ok=True)
    atomic_write_json(dest/'tables.json', dict(pages=pages, candidate_only=True))
    receipt = dict(profile=TABLE_PROFILE, url=source['url'], content_sha256=source['content_sha256'],
        source_raw_sha256=source['raw_sha256'], status='complete' if count <= max_pages else 'partial',
        document_pages=count, extracted_pages=len(pages), candidate_only=True,
        point_in_time_verified=False, table_count=sum(len(p['tables']) for p in pages),
        files=[dict(path='tables.json', sha256=sha256_file(dest/'tables.json'),
                    bytes=(dest/'tables.json').stat().st_size)])
    atomic_write_json(receipt_path, receipt)
    return receipt


def main():
    wall_started = time.monotonic()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive',type=Path,default=Path('data_taifex_public_history/rules'))
    p.add_argument('--output-dir',type=Path,required=True)
    p.add_argument('--category',action='append',choices=['margins','position_limits','contract_specs','contract_adjustments','calendar','settlement','other_announcements'])
    p.add_argument('--workers',type=int,default=None,
                   help='CPU legacy default: 4; bounded GPU/CPU runner requires 1')
    p.add_argument('--max-pages',type=int,default=120)
    p.add_argument('--pending-only',action='store_true')
    p.add_argument('--document-sha256-file',type=Path,
                   help='Revisit only these exact retained source hashes, one SHA-256 per line')
    p.add_argument('--ocr-dpi',type=int,choices=[200,300,400],default=200)
    p.add_argument('--ocr-psm',type=int,choices=[3,6],default=3)
    p.add_argument('--ocr-languages',choices=['chi_tra','chi_tra+eng'],default='chi_tra+eng')
    p.add_argument('--tessdata-dir',type=Path)
    p.add_argument('--force-ocr',action='store_true')
    p.add_argument('--rapidocr-config',type=Path,
                   help='Use independently retained local ONNX models and dependency versions')
    p.add_argument('--ocr-profile-dir',type=Path,
                   help='Retain actual ONNX node/device traces for the bounded OCR runner')
    p.add_argument('--native-tables-only',action='store_true',
                   help='Extract native cell grids into a separate immutable output; do not repeat OCR')
    p.add_argument('--ocr-tables-from',type=Path,
                   help='Reconstruct ruled cells from retained OCR polygons and source page images')
    args = p.parse_args()
    bounded = bool(args.rapidocr_config and 'execution' in json.loads(args.rapidocr_config.read_text())
                   and not args.native_tables_only and not args.ocr_tables_from)
    if args.workers is None:
        args.workers = 1 if bounded else 4
    if bounded and args.workers != 1:
        p.error('bounded OCR uses one GPU/session owner; use --workers 1')
    if args.ocr_profile_dir and not bounded:
        p.error('--ocr-profile-dir requires a bounded OCR execution configuration')
    if args.ocr_tables_from and (args.native_tables_only or not args.rapidocr_config):
        p.error('--ocr-tables-from requires --rapidocr-config and excludes --native-tables-only')
    if not 1 <= args.workers <= 48 or not 1 <= args.max_pages <= 400:
        p.error('workers 1..48 and pages 1..400 required')
    import pymupdf
    if pymupdf.VersionBind != '1.27.2.2':
        raise ValueError('review profile requires PyMuPDF 1.27.2.2')
    c=sqlite3.connect(f'file:{args.archive}/state/queue.sqlite3?mode=ro',uri=True);c.row_factory=sqlite3.Row
    categories=args.category or ['margins','position_limits','contract_specs','contract_adjustments','calendar']
    c.execute('BEGIN')
    items=review_documents(c,categories,args.pending_only)
    c.close()
    if args.document_sha256_file:
        selected=set(args.document_sha256_file.read_text().split())
        if not selected or any(not re.fullmatch('[0-9a-f]{64}',s) for s in selected):
            raise ValueError('non-empty exact SHA-256 selection required')
        if selected-set(items):
            raise ValueError('selected source hashes are outside the captured category: '+str(sorted(selected-set(items))))
        items={k:v for k,v in items.items() if k in selected}
    args.output_dir.mkdir(parents=True,exist_ok=True)
    profile = ('source_ruled_ocr_cell_grid_v3' if args.ocr_tables_from else TABLE_PROFILE if args.native_tables_only else
        rapidocr_review_profile(args.rapidocr_config, args.ocr_dpi, args.force_ocr)
        if args.rapidocr_config else extraction_profile(
            args.ocr_dpi,args.ocr_psm,args.ocr_languages,args.tessdata_dir,args.force_ocr))
    extractor = extract_ocr_tables if args.ocr_tables_from else extract_tables if args.native_tables_only else extract
    previous = args.output_dir/'worklist.json'
    if previous.exists() and json.loads(previous.read_text())['profile'] != profile:
        raise ValueError('use a separate output directory for each extraction profile')
    engine = None
    if args.rapidocr_config and not args.native_tables_only and not args.ocr_tables_from:
        engine = rapidocr_engine(args.rapidocr_config, args.ocr_profile_dir)
    atomic_write_json(previous,dict(profile=profile,categories=categories,documents=list(items.values())))
    completed=[];failed=[];start=time.monotonic()
    options=(dict(review_root=args.ocr_tables_from,rapidocr_config=args.rapidocr_config) if args.ocr_tables_from
                 else {} if args.native_tables_only else dict(dpi=args.ocr_dpi,psm=args.ocr_psm,
            languages=args.ocr_languages,tessdata_dir=args.tessdata_dir,force_ocr=args.force_ocr,
            rapidocr_config=args.rapidocr_config,rapidocr_profile_dir=args.ocr_profile_dir))
    def record_result(key, result):
        try:
            r=result();completed.append({k:r[k] for k in ['content_sha256','status','document_pages','extracted_pages']})
        except Exception as exc:
            failed.append(dict(content_sha256=key,error=f'{type(exc).__name__}: {exc}'))
        if (len(completed)+len(failed)) % 20 == 0:
            status=dict(total=len(items),completed=len(completed),failed=len(failed),elapsed_s=round(time.monotonic()-start,2),candidate_only=True)
            atomic_write_json(args.output_dir/'progress.json',status)
            print(json.dumps(status),flush=True)

    execution = None
    if bounded:
        # No fork of CUDA state, duplicate models, concurrent PyMuPDF access,
        # or unbounded page queue. RapidOCR batches text lines internally.
        try:
            for key,item in items.items():
                record_result(key,lambda item=item:extractor(args.archive,args.output_dir,item,args.max_pages,**options))
                # A source page with no recognized text is a document-level
                # failure, not poisoned CUDA state. Keep it failed and continue
                # independent documents. Runtime/device/hash failures still stop.
                if failed and any(not re.fullmatch(
                        r'ValueError: RapidOCR returned no text on page \d+', f['error']) for f in failed):
                    break
            execution=engine.finish_profiling()
            atomic_write_json(args.output_dir/'ocr_execution.json',execution)
        finally:
            engine.close()
    else:
        # PyMuPDF explicitly does not support shared-process multithreading.
        with ProcessPoolExecutor(max_workers=args.workers,
                                 mp_context=multiprocessing.get_context('spawn')) as pool:
            pending={pool.submit(extractor,args.archive,args.output_dir,item,args.max_pages,**options):key for key,item in items.items()}
            for future in as_completed(pending):
                record_result(pending[future],future.result)
    partial=sum(r['status']!='complete' for r in completed)
    atomic_write_json(args.output_dir/'manifest.json',dict(profile=profile,
        runtime=(execution['runtime'] if execution else {'pymupdf':pymupdf.VersionBind} if args.native_tables_only else
            rapidocr_runtime(args.rapidocr_config)[1] if args.rapidocr_config else
            runtime_profile(args.tessdata_dir,args.ocr_languages)),
        status='complete' if not failed and not partial else 'partial',
        candidate_only=True,point_in_time_verified=False,created_at_utc=datetime.now(timezone.utc).isoformat(),
        elapsed_s=time.monotonic()-start,documents=completed,failures=failed,partial_documents=partial,
        **(dict(wall_elapsed_s=time.monotonic()-wall_started,execution_receipt='ocr_execution.json',
                execution_receipt_sha256=sha256_file(args.output_dir/'ocr_execution.json'),
                unprocessed_documents=len(items)-len(completed)-len(failed)) if bounded else {}),
        worklist_sha256=sha256_file(args.output_dir/'worklist.json')))
    if bounded:
        atomic_write_json(args.output_dir/'progress.json',dict(total=len(items),completed=len(completed),
            failed=len(failed),elapsed_s=round(time.monotonic()-start,2),candidate_only=True))
    print(json.dumps(dict(completed=len(completed),failed=len(failed),elapsed_s=time.monotonic()-start)),flush=True)
    return int(bool(failed or partial))


if __name__=='__main__':
    raise SystemExit(main())
