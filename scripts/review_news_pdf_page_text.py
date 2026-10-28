#!/usr/bin/env python3
"""Compare retained PDF OCR with a separate, pixel-reviewed transcription.

This is a private diagnostic, not an article parser or a publication-clock
inference.  It reads existing extraction artifacts and never runs OCR or HTTP.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import struct

from downloader.artifact_io import atomic_write_bytes, sha256_file
from stockagent.ocr.review import review_grid

_MAX_IMAGE = 32 << 20
_MAX_TEXT = 4 << 20
_FIELDS = ('level', 'page_num', 'block_num', 'par_num', 'line_num', 'word_num',
    'left', 'top', 'width', 'height', 'conf', 'text')


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _json(path, limit):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('invalid or oversized review input')
    data = path.read_bytes()
    if len(data) > limit:
        raise ValueError('review input too large')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('invalid JSON number'))), _sha(data)


def _file(receipt, directory, name, limit):
    entries = [entry for entry in receipt['files'] if entry.get('path') == name]
    if len(entries) != 1:
        raise ValueError('missing or duplicate extraction file')
    path = directory / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('invalid extraction file')
    data = path.read_bytes()
    entry = entries[0]
    if len(data) != entry.get('bytes') or _sha(data) != entry.get('sha256'):
        raise ValueError('extraction file hash or size mismatch')
    return path, data, entry['sha256']


def _dimensions(png):
    if (len(png) < 24 or png[:8] != b'\x89PNG\r\n\x1a\n'
            or png[8:16] != b'\x00\x00\x00\rIHDR'):
        raise ValueError('invalid PNG header')
    width, height = struct.unpack('>II', png[16:24])
    if not 0 < width <= 30000 or not 0 < height <= 30000:
        raise ValueError('invalid image dimensions')
    return [width, height]


def _ocr_lines(tsv, dimensions):
    reader = csv.DictReader(io.StringIO(tsv.decode('utf-8'), newline=''), delimiter='\t')
    if tuple(reader.fieldnames or ()) != _FIELDS:
        raise ValueError('unexpected OCR TSV schema')
    lines, key_to_line, canvas = [], {}, None
    for row in reader:
        if None in row or any(row.get(key) is None for key in _FIELDS):
            raise ValueError('malformed OCR TSV')
        if row['level'] == '1':
            if canvas is not None:
                raise ValueError('multiple OCR page canvases')
            canvas = [int(row['width']), int(row['height'])]
        if row['level'] != '5' or not row['text'].strip():
            continue
        key = tuple(int(row[name]) for name in ('page_num', 'block_num', 'par_num', 'line_num'))
        word = int(row['word_num'])
        x, y, w, h = (int(row[name]) for name in ('left', 'top', 'width', 'height'))
        confidence = float(row['conf'])
        if (key[0] != 1 or word <= 0 or x < 0 or y < 0 or w <= 0 or h <= 0
                or x + w > dimensions[0] or y + h > dimensions[1]
                or not math.isfinite(confidence) or not 0 <= confidence <= 100):
            raise ValueError('invalid OCR word or geometry')
        if key not in key_to_line:
            key_to_line[key] = len(lines)
            lines.append(dict(key=key, words=[], bounds=[x, y, x + w, y + h]))
        line = lines[key_to_line[key]]
        if key_to_line[key] != len(lines) - 1 or line['words'] and word <= line['words'][-1][0]:
            raise ValueError('noncontiguous OCR line or unordered words')
        line['words'].append((word, row['text'].strip(), confidence))
        bounds = line['bounds']
        bounds[:] = [min(bounds[0], x), min(bounds[1], y), max(bounds[2], x + w), max(bounds[3], y + h)]
    if canvas != dimensions or not lines:
        raise ValueError('OCR canvas mismatch or no OCR lines')
    result = []
    for line in lines:
        left, top, right, bottom = line['bounds']
        result.append(dict(txt=' '.join(word[1] for word in line['words']),
            score=sum(word[2] for word in line['words']) / (100 * len(line['words'])),
            box=[[left, top], [right, top], [right, bottom], [left, bottom]]))
    return result


def _visual_rows(lines, visual, width, *, explicit_groups=None):
    if explicit_groups is not None:
        # A reviewer can bind a separate OCR column to visual rows explicitly;
        # never infer reading order or silently join source fragments.
        if (not isinstance(explicit_groups, list) or not explicit_groups
                or any(not isinstance(group, list) or not group for group in explicit_groups)):
            raise ValueError('invalid explicit OCR line groups')
        indices = [index for group in explicit_groups for index in group]
        if (any(type(index) is not int for index in indices)
                or sorted(indices) != list(range(len(lines)))):
            raise ValueError('explicit OCR groups must cover every line exactly once')
        groups = []
        for group in explicit_groups:
            boxes = [(index, lines[index]['box'][0][0], lines[index]['box'][0][1],
                      lines[index]['box'][2][0], lines[index]['box'][2][1]) for index in group]
            boxes.sort(key=lambda box: box[1])
            if any(a[3] > b[1] for a, b in zip(boxes, boxes[1:])):
                raise ValueError('explicit OCR cells overlap horizontally')
            top, bottom = min(box[2] for box in boxes), max(box[4] for box in boxes)
            common = min(box[4] for box in boxes) - max(box[2] for box in boxes)
            if len(boxes) > 1 and any(common < .7 * (box[4] - box[2]) for box in boxes):
                raise ValueError('explicit OCR cells do not share a visual line')
            if groups and top <= groups[-1][0]:
                raise ValueError('explicit OCR groups must follow visual vertical order')
            groups.append([top, bottom, boxes])
        for i in range(1, len(groups)):
            if groups[i][0] < groups[i - 1][1]:
                boundary = (groups[i][0] + groups[i - 1][1]) / 2
                groups[i - 1][1] = boundary
                groups[i][0] = boundary
        rows = []
        for top, bottom, boxes in groups:
            cells = []
            for i, (index, left, _, right, _) in enumerate(boxes):
                start = 0 if i == 0 else (boxes[i - 1][3] + left) / 2
                stop = width if i + 1 == len(boxes) else (right + boxes[i + 1][1]) / 2
                cells.append(dict(id=str(index), left=start, right=stop,
                                  kind='literal', text=visual[index]))
            rows.append(dict(id=','.join(str(box[0]) for box in boxes),
                             top=top, bottom=bottom, cells=cells))
        return rows
    # Adjacent OCR boxes can overlap by a few pixels even on distinct source
    # lines. Split only adjacent bands at their midpoint; the existing grid
    # review still checks material overlap and rejects ambiguous associations.
    bands = [[line['box'][0][1], line['box'][2][1]] for line in lines]
    for index in range(1, len(bands)):
        if bands[index][0] <= bands[index - 1][0]:
            raise ValueError('OCR lines are not in vertical reading order')
        if bands[index][0] < bands[index - 1][1]:
            boundary = (bands[index][0] + bands[index - 1][1]) / 2
            bands[index - 1][1] = boundary
            bands[index][0] = boundary
    return [dict(id=str(index), top=top, bottom=bottom, cells=[dict(id=str(index),
        left=0, right=width, kind='literal', text=expected)])
        for index, ((top, bottom), expected) in enumerate(zip(bands, visual))]


def review(receipt_path, annotation_path, output):
    if output.is_symlink():
        raise ValueError('review output cannot be a symlink')
    receipt, receipt_sha = _json(receipt_path, _MAX_TEXT)
    annotation, annotation_sha = _json(annotation_path, _MAX_TEXT)
    if (receipt.get('status') != 'complete' or receipt.get('candidate_only') is not True
            or receipt.get('point_in_time_verified') is not False
            or annotation.get('kind') != 'pdf_page_visual_text_review'
            or annotation.get('contract_version') != 1
            or annotation.get('method') != 'ocr_guided_visual_transcription'
            or annotation.get('receipt_sha256') != receipt_sha
            or annotation.get('content_sha256') != receipt.get('content_sha256')):
        raise ValueError('PDF review identity mismatch')
    pages = annotation.get('pages')
    if not isinstance(pages, list) or not 1 <= len(pages) <= 64:
        raise ValueError('one to 64 explicit pages required')
    results, used_pages, input_paths = [], set(), []
    for item in pages:
        page = item.get('page')
        if type(page) is not int or page in used_pages or not 1 <= page <= receipt['document_pages']:
            raise ValueError('invalid or duplicate physical page')
        used_pages.add(page)
        source_pages = [p for p in receipt['pages'] if p.get('page') == page]
        if len(source_pages) != 1 or source_pages[0].get('extraction') != 'ocr_candidate':
            raise ValueError('physical page is not a retained OCR candidate')
        prefix = f'page_{page:03d}'
        image_path, png, image_sha = _file(receipt, receipt_path.parent, prefix + '.png', _MAX_IMAGE)
        tsv_path, tsv, tsv_sha = _file(receipt, receipt_path.parent, prefix + '_ocr.tsv', _MAX_TEXT)
        text_path, _, text_sha = _file(receipt, receipt_path.parent, prefix + '_ocr.txt', _MAX_TEXT)
        if (item.get('image_sha256') != image_sha or item.get('ocr_tsv_sha256') != tsv_sha
                or item.get('ocr_text_sha256') != text_sha):
            raise ValueError('visual transcription does not match retained page')
        dimensions = _dimensions(png)
        lines = _ocr_lines(tsv, dimensions)
        visual = item.get('visual_lines')
        if (not isinstance(visual, list) or len(visual) != len(lines)
                or any(not isinstance(line, str) or not line for line in visual)):
            raise ValueError('all OCR lines require a visual transcription')
        explicit_groups = item.get('ocr_line_groups')
        if 'ocr_line_groups' in item and explicit_groups is None:
            raise ValueError('explicit OCR line groups cannot be null')
        rows = _visual_rows(lines, visual, dimensions[0], explicit_groups=explicit_groups)
        result = review_grid(lines, rows, dimensions)
        if (result['unmatched_line_indexes'] or result['cross_cell_line_indexes']
                or any(cell['line_indexes'] != [int(cell['id'])] for cell in result['cells'])):
            raise ValueError('OCR geometry is ambiguous')
        page_result = dict(page=page, image_sha256=image_sha, ocr_tsv_sha256=tsv_sha,
            ocr_text_sha256=text_sha, dimensions=dimensions, **result)
        if explicit_groups is not None:
            page_result['explicit_ocr_line_groups_used'] = True
        results.append(page_result)
        input_paths.extend(((image_path, image_sha), (tsv_path, tsv_sha), (text_path, text_sha)))
    if (sha256_file(receipt_path) != receipt_sha or sha256_file(annotation_path) != annotation_sha
            or any(sha256_file(path) != digest for path, digest in input_paths)):
        raise ValueError('review inputs changed during inspection')
    result = dict(kind='pdf_page_visual_text_comparison', contract_version=1,
        source_url=receipt['url'], content_sha256=receipt['content_sha256'],
        source_raw_sha256=receipt['source_raw_sha256'], extraction_receipt_sha256=receipt_sha,
        annotation_sha256=annotation_sha, implementation_sha256={
            'scripts/review_news_pdf_page_text.py': sha256_file(Path(__file__)),
            'stockagent/ocr/review.py': sha256_file(Path(review_grid.__code__.co_filename))},
        annotation_method=annotation['method'], pages=results, source_requests=0, ocr_inference_repeated=False,
        article_body_modified=False, accepted_as_article=False, historical_release_clock_inferred=False,
        all_source_text_accuracy_proven=False, omitted_source_lines_possible=True)
    reused = output.exists()
    if reused:
        if json.loads(output.read_bytes()) != result:
            raise ValueError('existing review belongs to different inputs or implementation')
    else:
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, allow_nan=False,
            separators=(',', ':')) + '\n').encode(), durable=True, mode=0o600)
    return result, reused


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--annotation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result, reused = review(args.receipt, args.annotation, args.output)
    print(json.dumps(dict(pages=len(result['pages']),
        lines=sum(page['total_ocr_lines'] for page in result['pages']),
        counts=[page['counts'] for page in result['pages']], source_requests=0,
        ocr_inference_repeated=False, accepted_as_article=False, reused=reused)))


if __name__ == '__main__':
    main()
