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
import math
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
OCR_TABLE_PROFILE = 'source_ruled_ocr_cell_grid_v5'


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


def refine_position_ocr_cells(table, tokens):
    """Restore position-row ownership from already retained glyph polygons.

    A scanned rule may merge two rows or miss a label cell. Only literal
    labels and complete, uniquely aligned amount tokens can repair ownership.
    Original cells and token identities remain in the candidate's evidence.
    """
    from copy import deepcopy
    import math
    normal=lambda s:re.sub(r'\s+','',str(s or '')).translate(
        str.maketrans('数约标证终业计准营','數約標證終業計準營'))
    original=table['cells']
    if 'position_cell_refinement' in table:return table
    if (not original or not original[0] or len({len(r) for r in original})!=1
            or len(original[0])<2):return table
    unit_table=normal(original[0][0])=='持有部位'
    cap_table=normal(original[0][0]).startswith('適用期間')
    if not (unit_table or cap_table):return table
    evidence={(e['row'],e['column']):e for e in table.get('cell_evidence',[])}
    if len(evidence)!=len(table.get('cell_evidence',[])):return table
    def box(i):
        polygon=tokens[i]['box']
        if len(polygon)!=4 or any(len(p)!=2 or not all(math.isfinite(v) for v in p) for p in polygon):
            raise ValueError('invalid retained OCR polygon')
        return [min(p[0] for p in polygon),min(p[1] for p in polygon),
                max(p[0] for p in polygon),max(p[1] for p in polygon)]
    def aligned(a,b):
        return max(0,min(a[3],b[3])-max(a[1],b[1]))>=.7*min(a[3]-a[1],b[3]-b[1])>0
    def indices(r,c):
        item=evidence.get((r,c))
        if item is None:return []
        ids=item.get('token_indices',[])
        if len(ids)!=len(set(ids)) or any(i<0 or i>=len(tokens) for i in ids):
            raise ValueError('invalid retained OCR token identity')
        if '\n'.join(tokens[i]['txt'] for i in ids)!=(original[r][c] or ''):
            raise ValueError('retained OCR cell differs from its tokens')
        x0,y0,x1,y1=item['bbox'];pad=min(8.,.12*min(x1-x0,y1-y0))
        for i in ids:
            b=box(i)
            if not (x0-pad<=b[0] and b[2]<=x1+pad and y0-pad<=b[1] and b[3]<=y1+pad):
                return []
        return ids
    rows=deepcopy(original);changes=[]
    categories={'自然人','法人機構','造市者'}
    amount=r'[\d,]+(?:\.\d+)?(?:股|受益權單位)'
    for r in range(1,len(rows)):
        labels=(original[r][0] or '').splitlines()
        if len(labels)<2 or not all(normal(s) in categories for s in labels):continue
        label_ids=indices(r,0)
        if len(label_ids)!=len(labels) or len(set(map(normal,labels)))!=len(labels):continue
        label_boxes=[box(i) for i in label_ids]
        if any(a[3]>=b[1] for a,b in zip(label_boxes,label_boxes[1:])):continue
        columns=[]
        for c in range(1,len(rows[r])):
            ids=indices(r,c)
            if len(ids)!=len(label_ids) or not all(re.fullmatch(amount,normal(tokens[i]['txt'])) for i in ids):break
            matches=[[i for i in ids if aligned(label,box(i))] for label in label_boxes]
            if any(len(m)!=1 for m in matches) or len({m[0] for m in matches})!=len(ids):break
            columns.append([m[0] for m in matches])
        if len(columns)!=len(rows[r])-1:continue
        replacement=[[tokens[i]['txt'],*[tokens[ids[n]]['txt'] for ids in columns]]
                     for n,i in enumerate(label_ids)]
        changes.append(dict(kind='uniquely_aligned_category_rows',row=r,
            label_token_indices=label_ids,amount_token_indices=columns,replacement=replacement))
    # Missing labels can be retained in a spanning header cell. Numeric row
    # borders independently own the target row; a caption or neighbouring
    # label outside that row/column cannot supply it.
    if not changes:
        left=[e['bbox'] for (r,c),e in evidence.items() if c==0]
        if left:
            x0=max(b[0] for b in left);x1=min(b[2] for b in left)
            excluded=set(table.get('excluded_boundary_tokens',[]))
            for r in range(1,len(rows)):
                if normal(rows[r][0]):continue
                numeric=[evidence.get((r,c)) for c in range(1,len(rows[r]))]
                if any(e is None for e in numeric):continue
                y0=max(e['bbox'][1] for e in numeric);y1=min(e['bbox'][3] for e in numeric)
                if x0>=x1 or y0>=y1:continue
                expected={'每口折算股數','每口折算單位數'} if unit_table else categories
                found=[]
                for i,t in enumerate(tokens):
                    if i in excluded or normal(t['txt']) not in expected:continue
                    b=box(i);pad=min(8.,.12*min(x1-x0,y1-y0))
                    if x0-pad<=b[0] and b[2]<=x1+pad and y0-pad<=b[1] and b[3]<=y1+pad:
                        found.append(i)
                if len(found)!=1:continue
                i=found[0];owners=[(rr,c) for (rr,c),e in evidence.items() if i in e.get('token_indices',[])]
                if len(owners)>1 or any(c!=0 for rr,c in owners):continue
                if owners:
                    rr,c=owners[0];ids=indices(rr,c)
                    lines=(rows[rr][c] or '').splitlines()
                    if len(lines)!=len(ids) or rr==r:continue
                    rows[rr][c]='\n'.join(line for line,j in zip(lines,ids) if j!=i)
                rows[r][0]=tokens[i]['txt']
                changes.append(dict(kind='unique_label_in_numeric_row_bounds',row=r,
                    token_index=i,previous_cell=owners,own_row_bbox=[x0,y0,x1,y1]))
    if not changes:return table
    if any(c['kind']=='uniquely_aligned_category_rows' for c in changes):
        replacements={c['row']:c['replacement'] for c in changes}
        rows=[row for r,old in enumerate(rows) for row in replacements.get(r,[old])]
    result=dict(table,cells=rows,position_cell_refinement=dict(
        contract='retained_polygons_unique_position_rows_v1',original_cells=original,changes=changes))
    return result


def position_polygon_tables(tokens):
    """Recover two complete position tables from unique retained text axes.

    This narrow fallback handles missing scan rules, not missing glyphs. All
    member/quantity cells, three person rows and both period headers must be
    observed. Ambiguous rows or columns produce no additional candidates.
    """
    records=[]
    for i,token in enumerate(tokens):
        try:
            points=token['box']
            xs=[float(p[0]) for p in points];ys=[float(p[1]) for p in points]
            if len(points)!=4 or not all(math.isfinite(v) for v in xs+ys):return []
            box=[min(xs),min(ys),max(xs),max(ys)]
            if box[2]<=box[0] or box[3]<=box[1]:return []
            value=''.join(token['txt'].split()).translate(str.maketrans('数约','數約'))
        except (KeyError,TypeError,ValueError):return []
        records.append(dict(index=i,box=box,value=value,x=(box[0]+box[2])/2,
                            y=(box[1]+box[3])/2,h=box[3]-box[1]))
    labels=('持有部位','每口折算股數','適用期間','自然人','法人機構','造市者')
    matches={label:[r for r in records if r['value']==label] for label in labels}
    if any(len(matches[label])!=1 for label in labels):return []
    label={key:value[0] for key,value in matches.items()}
    if not all(label[a]['y']<label[b]['y'] for a,b in zip(labels,labels[1:])):return []
    number=r'\d[\d,]*(?:\.\d+)?'
    def same_row(anchor):
        return [r for r in records if r['x']>anchor['box'][2]
                and abs(r['y']-anchor['y'])<.45*(r['h']+anchor['h'])]
    codes=sorted(same_row(label['持有部位']),key=lambda r:r['x'])
    quantities=sorted(same_row(label['每口折算股數']),key=lambda r:r['x'])
    if (not 2<=len(codes)<=6 or len(quantities)!=len(codes)
            or len({r['value'] for r in codes})!=len(codes)
            or any(not re.fullmatch(r'[A-Z]{2}[A-Z0-9]',r['value']) for r in codes)
            or any(not re.fullmatch(number,r['value'])
                   or float(r['value'].replace(',',''))<=0 for r in quantities)):return []
    for code,quantity in zip(codes,quantities):
        # The numeric polygon must align with exactly this observed code.
        aligned=[c for c in codes if min(c['box'][2],quantity['box'][2])>
                                      max(c['box'][0],quantity['box'][0])]
        if aligned!=[code]:return []
    persons=[label[key] for key in labels[3:]]
    amounts=[]
    for row,person in enumerate(persons):
        values=sorted(same_row(person),key=lambda r:r['x'])
        if (len(values)!=2 or any(not re.fullmatch(number+'股',r['value'])
                or float(r['value'][:-1].replace(',',''))<=0 for r in values)):return []
        # Scanned rows may slope across a wide page. Every numeric polygon
        # must match exactly one observed person baseline and be no taller
        # than an ordinary single-row token; no synthetic horizontal rule.
        if any(r['h']>1.5*person['h'] or [p for p in persons
                if abs(r['y']-p['y'])<.45*(r['h']+p['h'])]!=[person] for r in values):return []
        amounts.append(values)
    header_low=label['適用期間']['box'][1]
    header_high=min(r['box'][1] for r in [persons[0],*amounts[0]])
    starts=sorted([r for r in records if re.match(r'自\d',r['value'])
                   and header_low<=r['y']<header_high],key=lambda r:r['x'])
    if len(starts)!=2:return []
    # All observed left/right numbers and start phrases leave one common
    # empty horizontal gap. It supplies a separator without choosing dates.
    left=max(starts[0]['box'][2],*(row[0]['box'][2] for row in amounts))
    right=min(starts[1]['box'][0],*(row[1]['box'][0] for row in amounts))
    if left>=right:return []
    split=(left+right)/2
    label_right=max(r['box'][2] for r in [label['適用期間'],*persons])
    if any(row[0]['box'][0]<=label_right for row in amounts):return []
    headers=[[],[]]
    for r in records:
        if not header_low<=r['y']<header_high or r['x']<=label_right:continue
        column=int(r['x']>split)
        pad=min(8.,.12*(r['box'][2]-r['box'][0]))
        if (column==0 and r['box'][2]>split+pad
                or column==1 and r['box'][0]<split-pad):return []
        headers[column].append(r)
    if any(not h or len([r for r in h if re.match(r'自\d',r['value'])])!=1
           for h in headers):return []
    for row in amounts:
        if row[0]['box'][2]>split or row[1]['box'][0]<split:return []
    def table(rows,kind):
        evidence=[];cells=[];used=[]
        for ri,row in enumerate(rows):
            values=[]
            for ci,parts in enumerate(row):
                parts=sorted(parts,key=lambda r:(r['y'],r['x']))
                box=[min(r['box'][0] for r in parts),min(r['box'][1] for r in parts),
                     max(r['box'][2] for r in parts),max(r['box'][3] for r in parts)]
                values.append('\n'.join(tokens[r['index']]['txt'] for r in parts))
                evidence.append(dict(row=ri,column=ci,bbox=box,
                    token_indices=[r['index'] for r in parts]))
                used.extend(r['index'] for r in parts)
            cells.append(values)
        if len(set(used))!=len(used):raise ValueError('position polygon table assigns a token twice')
        return dict(cells=cells,caption='',bbox=[min(e['bbox'][0] for e in evidence),
            min(e['bbox'][1] for e in evidence),max(e['bbox'][2] for e in evidence),
            max(e['bbox'][3] for e in evidence)],cell_evidence=evidence,
            position_polygon_structure=dict(contract='complete_position_token_axes_v1',kind=kind,
                token_indices=sorted(used),column_separator=split,
                original_tokens=[dict(index=i,**tokens[i]) for i in sorted(used)]),
            extraction_method='source_position_polygon_alignment',candidate_only=True)
    unit=table([[[label[labels[0]]],*[[r] for r in codes]],
                [[label[labels[1]]],*[[r] for r in quantities]]],'own_units')
    cap=table([[[label[labels[2]]],*headers],
               *[[[person],*[ [r] for r in row]] for person,row in zip(persons,amounts)]],
              'two_period_three_person_caps')
    return [unit,cap]


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
    fallback=position_polygon_tables(tokens)
    if hierarchy is None:return fallback
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
    refined=[refine_position_ocr_cells(t,tokens) for t in tables]
    normalized=lambda cells:[[''.join(str(v or '').split()).translate(str.maketrans('数约','數約'))
                              for v in row] for row in cells]
    for candidate in fallback:
        if not any(normalized(t['cells'])==normalized(candidate['cells']) for t in refined):
            refined.append(candidate)
    return sorted(refined,key=lambda t:(t['bbox'][1],t['bbox'][0]))


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
    result=dict(profile=OCR_TABLE_PROFILE,url=source['url'],
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
    profile = (OCR_TABLE_PROFILE if args.ocr_tables_from else TABLE_PROFILE if args.native_tables_only else
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
