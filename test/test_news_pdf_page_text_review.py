"""An OCR review must not become a source article or silently change inputs."""
import copy
import hashlib
import json
import struct

import pytest

from scripts.review_news_pdf_page_text import _ocr_lines, _visual_rows, review
from stockagent.ocr.review import review_grid


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _save(path, value):
    data = json.dumps(value, ensure_ascii=False).encode()
    path.write_bytes(data)
    return _sha(data)


def _fixture(tmp_path):
    png = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR' + struct.pack('>II', 200, 300)
    tsv = ('level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n'
           '1\t1\t0\t0\t0\t0\t0\t0\t200\t300\t-1\t\n'
           '5\t1\t1\t1\t1\t1\t10\t30\t8\t10\t90\te\n'
           '5\t1\t1\t1\t1\t2\t25\t30\t60\t10\t95\tText\n'
           '5\t1\t1\t1\t2\t1\t10\t60\t70\t10\t98\t40 percent\n').encode()
    raw = {'page_001.png': png, 'page_001_ocr.tsv': tsv, 'page_001_ocr.txt': b'e Text\n40 percent\n'}
    for name, data in raw.items():
        (tmp_path / name).write_bytes(data)
    receipt = dict(url='https://example.org/file.pdf', content_sha256='a' * 64,
        source_raw_sha256='b' * 64, status='complete', candidate_only=True,
        point_in_time_verified=False, document_pages=1, pages=[dict(page=1, extraction='ocr_candidate')],
        files=[dict(path=name, bytes=len(data), sha256=_sha(data)) for name, data in raw.items()])
    receipt_path = tmp_path / 'receipt.json'
    receipt_sha = _save(receipt_path, receipt)
    annotation = dict(kind='pdf_page_visual_text_review', contract_version=1,
        method='ocr_guided_visual_transcription', receipt_sha256=receipt_sha, content_sha256='a' * 64,
        pages=[dict(page=1, image_sha256=_sha(png), ocr_tsv_sha256=_sha(tsv),
            ocr_text_sha256=_sha(raw['page_001_ocr.txt']), visual_lines=['• Text', '40 percent'])])
    annotation_path = tmp_path / 'annotation.json'
    _save(annotation_path, annotation)
    return receipt_path, annotation_path, tmp_path / 'result.json', annotation


def test_review_compares_source_literal_and_reuses_result_without_admission(tmp_path):
    receipt, annotation, output, _ = _fixture(tmp_path)
    result, reused = review(receipt, annotation, output)
    assert not reused
    assert result['annotation_method'] == 'ocr_guided_visual_transcription'
    assert result['pages'][0]['counts'] == {'agrees': 1, 'disagrees': 1}
    assert result['pages'][0]['cells'][0]['ocr_candidates'][0]['text'] == 'e Text'
    assert result['pages'][0]['cells'][0]['review_text'] == '• Text'
    assert result['source_requests'] == 0 and result['ocr_inference_repeated'] is False
    assert result['accepted_as_article'] is False and result['historical_release_clock_inferred'] is False
    assert result['all_source_text_accuracy_proven'] is False and result['omitted_source_lines_possible'] is True
    assert review(receipt, annotation, output)[1]


@pytest.mark.parametrize('name', ['page_001.png', 'page_001_ocr.tsv', 'page_001_ocr.txt'])
def test_changed_retained_input_rejected(tmp_path, name):
    receipt, annotation, output, _ = _fixture(tmp_path)
    (tmp_path / name).write_bytes(b'changed')
    with pytest.raises(ValueError, match='hash or size mismatch'):
        review(receipt, annotation, output)
    assert not output.exists()


@pytest.mark.parametrize('bad', ['receipt_hash', 'image_hash', 'wrong_page', 'duplicate_page', 'missing_line', 'method'])
def test_annotation_identity_and_coverage_fail_closed(tmp_path, bad):
    receipt, path, output, annotation = _fixture(tmp_path)
    if bad == 'receipt_hash':
        annotation['receipt_sha256'] = '0' * 64
    elif bad == 'image_hash':
        annotation['pages'][0]['image_sha256'] = '0' * 64
    elif bad == 'wrong_page':
        annotation['pages'][0]['page'] = 2
    elif bad == 'duplicate_page':
        annotation['pages'].append(copy.deepcopy(annotation['pages'][0]))
    elif bad == 'missing_line':
        annotation['pages'][0]['visual_lines'].pop()
    else:
        annotation['method'] = 'blind_independent_review'
    _save(path, annotation)
    with pytest.raises(ValueError):
        review(receipt, path, output)
    assert not output.exists()


def test_existing_output_is_never_overwritten_for_different_annotation(tmp_path):
    receipt, path, output, annotation = _fixture(tmp_path)
    review(receipt, path, output)
    before = output.read_bytes()
    annotation['pages'][0]['visual_lines'][1] = '41 percent'
    _save(path, annotation)
    with pytest.raises(ValueError, match='existing review'):
        review(receipt, path, output)
    assert output.read_bytes() == before


def test_invalid_ocr_geometry_and_canvas_are_rejected(tmp_path):
    _fixture(tmp_path)
    data = (tmp_path / 'page_001_ocr.tsv').read_bytes()
    with pytest.raises(ValueError, match='geometry'):
        _ocr_lines(data.replace(b'10\t60\t70\t10', b'190\t60\t70\t10'), [200, 300])
    with pytest.raises(ValueError, match='canvas'):
        _ocr_lines(data, [201, 300])


def test_duplicate_json_keys_are_not_silently_accepted(tmp_path):
    receipt, annotation, output, _ = _fixture(tmp_path)
    annotation.write_text('{"kind":"a","kind":"b"}')
    with pytest.raises(ValueError, match='duplicate JSON key'):
        review(receipt, annotation, output)


def test_symlink_inputs_and_output_are_not_followed(tmp_path):
    receipt, annotation, output, _ = _fixture(tmp_path)
    link = tmp_path / 'linked-receipt.json'
    link.symlink_to(receipt)
    with pytest.raises(ValueError, match='review input'):
        review(link, annotation, output)
    output.symlink_to(tmp_path / 'missing-target')
    with pytest.raises(ValueError, match='symlink'):
        review(receipt, annotation, output)
    assert not (tmp_path / 'missing-target').exists()


def test_adjacent_ocr_boxes_with_small_vertical_overlap_remain_separate():
    lines = [
        dict(txt='first', score=.9, box=[[10, 30], [80, 30], [80, 40], [10, 40]]),
        dict(txt='second', score=.9, box=[[10, 38], [80, 38], [80, 48], [10, 48]]),
    ]
    result = review_grid(lines, _visual_rows(lines, ['first', 'second'], 200), [200, 300])
    assert result['counts'] == {'agrees': 2}
    assert [cell['line_indexes'] for cell in result['cells']] == [[0], [1]]


def test_out_of_order_ocr_lines_are_not_silently_reassociated():
    lines = [
        dict(box=[[0, 40], [10, 40], [10, 50], [0, 50]]),
        dict(box=[[0, 30], [10, 30], [10, 40], [0, 40]]),
    ]
    with pytest.raises(ValueError, match='vertical reading order'):
        _visual_rows(lines, ['first', 'second'], 200)


def test_reviewer_explicitly_groups_separate_columns_without_reordering_candidates():
    lines = [
        dict(txt='Il.', score=.9, box=[[10, 51], [30, 51], [30, 70], [10, 70]]),
        dict(txt='heading', score=.9, box=[[50, 10], [130, 10], [130, 30], [50, 30]]),
        dict(txt='Domestic Operations', score=.9, box=[[50, 50], [180, 50], [180, 71], [50, 71]]),
    ]
    rows = _visual_rows(lines, ['II.', 'heading', 'Domestic Operations'], 200,
                        explicit_groups=[[1], [0, 2]])
    result = review_grid(lines, rows, [200, 300])
    assert result['counts'] == {'agrees': 2, 'disagrees': 1}
    assert [(cell['id'], cell['line_indexes']) for cell in result['cells']] == [
        ('1', [1]), ('0', [0]), ('2', [2])]
    assert not result['unmatched_line_indexes'] and not result['cross_cell_line_indexes']


@pytest.mark.parametrize('groups', [
    [], [[0], [0, 1]], [[0]], [[True], [1]], [[1], [0]], [[0, 1]],
])
def test_explicit_line_groups_reject_omissions_duplicates_and_wrong_geometry(groups):
    lines = [
        dict(box=[[10, 20], [70, 20], [70, 30], [10, 30]]),
        dict(box=[[20, 50], [80, 50], [80, 60], [20, 60]]),
    ]
    with pytest.raises(ValueError):
        _visual_rows(lines, ['one', 'two'], 200, explicit_groups=groups)


def test_explicit_same_line_overlapping_columns_are_rejected():
    lines = [
        dict(box=[[10, 20], [70, 20], [70, 30], [10, 30]]),
        dict(box=[[60, 20], [80, 20], [80, 30], [60, 30]]),
    ]
    with pytest.raises(ValueError, match='overlap horizontally'):
        _visual_rows(lines, ['one', 'two'], 200, explicit_groups=[[0, 1]])


def test_review_binds_explicit_column_groups_to_the_original_ocr_indices(tmp_path):
    receipt, annotation, output, saved = _fixture(tmp_path)
    tsv = ('level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n'
           '1\t1\t0\t0\t0\t0\t0\t0\t200\t300\t-1\t\n'
           '5\t1\t1\t1\t2\t1\t10\t60\t20\t10\t90\tIl.\n'
           '5\t1\t1\t1\t1\t1\t50\t30\t80\t10\t90\tHeading\n'
           '5\t1\t1\t1\t3\t1\t50\t60\t100\t10\t90\tOperations\n').encode()
    (tmp_path / 'page_001_ocr.tsv').write_bytes(tsv)
    record = json.loads(receipt.read_bytes())
    entry = next(item for item in record['files'] if item['path'] == 'page_001_ocr.tsv')
    entry.update(bytes=len(tsv), sha256=_sha(tsv))
    saved['receipt_sha256'] = _save(receipt, record)
    saved['pages'][0].update(ocr_tsv_sha256=_sha(tsv), visual_lines=['II.', 'Heading', 'Operations'],
                             ocr_line_groups=[[1], [0, 2]])
    _save(annotation, saved)
    result, reused = review(receipt, annotation, output)
    page = result['pages'][0]
    assert not reused and page['explicit_ocr_line_groups_used']
    assert [(cell['id'], cell['line_indexes']) for cell in page['cells']] == [
        ('1', [1]), ('0', [0]), ('2', [2])]
    assert page['counts'] == {'agrees': 2, 'disagrees': 1}
    assert result['omitted_source_lines_possible'] and not result['accepted_as_article']


def test_explicit_null_grouping_is_rejected(tmp_path):
    receipt, annotation, output, saved = _fixture(tmp_path)
    saved['pages'][0]['ocr_line_groups'] = None
    _save(annotation, saved)
    with pytest.raises(ValueError, match='cannot be null'):
        review(receipt, annotation, output)
    assert not output.exists()
