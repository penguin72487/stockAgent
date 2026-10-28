"""Separate OCR agreement from source truth, missingness and units."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.artifact_io import sha256_file
from scripts.review_news_ocr_candidates import review
from stockagent.ocr.review import review_grid, source_grid_review, source_label_review


def _line(text, left=12, right=48, top=12, bottom=28, score=0.99):
    return dict(txt=text, score=score, box=[[left, top], [right, top], [right, bottom], [left, bottom]])


def _rows(text='0.33', kind='decimal', unit='%'):
    return [dict(id='row1', top=10, bottom=30, cells=[
        dict(id='yield', left=10, right=50, text=text, kind=kind, unit=unit)])]


def _candidate(tmp_path):
    image = tmp_path / 'source.png'
    image.write_bytes(b'retained image fixture bytes; no decoder or inference needed')
    candidate = dict(kind='source_inline_image_ocr_candidate', contract_version=1,
        image_path=str(image), image_sha256=sha256_file(image), source_raw_sha256='a' * 64,
        article_url='https://example.org/retained-original', dimensions=[100, 100],
        financial_observation_admitted=False, article_body_modified=False,
        historical_release_clock_inferred=False, lines=[_line('300')])
    path = tmp_path / 'candidate.json'
    path.write_text(json.dumps(candidate))
    annotation = dict(kind='source_image_visual_grid_review', contract_version=1,
        image_sha256=candidate['image_sha256'], candidate_sha256=sha256_file(path),
        dimensions=candidate['dimensions'], rows=_rows())
    annotated = tmp_path / 'annotation.json'
    annotated.write_text(json.dumps(annotation))
    return candidate, annotation, path, annotated


@pytest.mark.parametrize('actual,expected,status', [
    ('300', '0.33', 'disagrees'), ('-1,122', '-1122', 'agrees'),
    ('1122', '-1122', 'disagrees'), ('−1122', '-1122', 'agrees'),
    ('0', '-0', 'disagrees'), ('0.3300', '0.33', 'agrees'),
    ('12,34', '1234', 'unreadable_number'), ('1e3', '1000', 'unreadable_number'),
    ('NaN', '0', 'unreadable_number'), ('3.3%', '3.3', 'unreadable_number'),
])
def test_decimal_comparison_preserves_sign_and_never_repairs_tokens(actual, expected, status):
    lines = [_line(actual)]
    original = deepcopy(lines)
    result = review_grid(lines, _rows(expected), [100, 100])
    assert result['cells'][0]['status'] == status
    assert result['cells'][0]['ocr_candidates'][0]['text'] == actual
    assert result['cells'][0]['review_text'] == expected
    assert result['cells'][0]['reviewed_unit'] == '%'
    assert not result['financial_observation_admitted']
    assert not result['historical_release_clock_inferred']
    assert lines == original


@pytest.mark.parametrize('kind,expected,actual', [('missing', 'N/A', '0'),
    ('identifier', '0050', '50'), ('literal', '閎康', '閑康')])
def test_missing_code_and_name_semantics_are_not_numeric_aliases(kind, expected, actual):
    cell = review_grid([_line(actual)], _rows(expected, kind, None), [100, 100])['cells'][0]
    assert cell['status'] == 'disagrees'
    assert 'candidate_decimal' not in cell


def test_units_remain_source_context_and_do_not_convert_lots_to_shares():
    cell = review_grid([_line('-1122')], _rows('-1122', unit='張'), [100, 100])['cells'][0]
    assert cell['candidate_decimal'] == '-1122'
    assert cell['reviewed_unit'] == '張' and 'shares' not in cell


def test_missing_ocr_is_not_filled_from_transcription():
    result = review_grid([], _rows(), [100, 100])
    assert result['counts'] == {'missing_candidate': 1}
    assert result['cells'][0]['ocr_candidates'] == []
    assert 'candidate_decimal' not in result['cells'][0]


def test_split_sign_and_digits_require_review_not_automatic_joining():
    result = review_grid([_line('-', 12, 18), _line('1122', 20, 48)], _rows('-1122'), [100, 100])
    assert result['counts'] == {'multiple_candidates': 1}
    assert len(result['cells'][0]['ocr_candidates']) == 2


def test_cross_cell_candidate_is_ambiguous_even_when_text_matches():
    rows = _rows('12')
    rows[0]['cells'].append(dict(id='other', left=50, right=90, text='12', kind='decimal'))
    result = review_grid([_line('12', 30, 70)], rows, [100, 100])
    assert result['counts'] == {'cross_cell_candidate': 2}
    assert result['cross_cell_line_indexes'] == [0]


def test_headers_outside_review_are_not_claimed_verified():
    result = review_grid([_line('0.33'), _line('heading', top=50, bottom=70)], _rows(), [100, 100])
    assert result['counts'] == {'agrees': 1}
    assert result['matched_line_indexes'] == [0] and result['unmatched_line_indexes'] == [1]
    assert not result['all_text_accuracy_proven']


@pytest.mark.parametrize('bad', [float('nan'), float('inf'), True, '12'])
def test_invalid_geometry_or_confidence_cannot_enter_a_review(bad):
    line = _line('0.33')
    line['box'][0][0] = bad
    with pytest.raises(ValueError):
        review_grid([line], _rows(), [100, 100])
    line = _line('0.33', score=bad)
    with pytest.raises(ValueError):
        review_grid([line], _rows(), [100, 100])


@pytest.mark.parametrize('problem', ['rows_overlap', 'cells_overlap', 'duplicate_id', 'outside_image'])
def test_ambiguous_manual_layout_is_rejected(problem):
    rows = _rows()
    if problem == 'rows_overlap':
        rows.append(dict(id='row2', top=20, bottom=40, cells=[
            dict(id='second', left=60, right=80, text='1', kind='decimal')]))
    elif problem in {'cells_overlap', 'duplicate_id'}:
        rows[0]['cells'].append(dict(id='yield' if problem == 'duplicate_id' else 'second',
            left=49 if problem == 'cells_overlap' else 60, right=80, text='1', kind='decimal'))
    else:
        rows[0]['bottom'] = 101
    with pytest.raises(ValueError):
        review_grid([_line('0.33')], rows, [100, 100])


def test_many_separated_rows_keep_their_own_cells():
    rows, lines = [], []
    for index in range(300):
        top = index * 30 + 10
        row = _rows(str(index))[0]
        row.update(id=str(index), top=top, bottom=top + 20)
        row['cells'][0]['id'] = 'value:' + str(index)
        rows.append(row)
        lines.append(_line(str(index), top=top + 2, bottom=top + 18))
    result = review_grid(lines[::-1], rows[::-1], [100, 9020])
    assert result['counts'] == {'agrees': 300}
    assert not result['unmatched_line_indexes'] and not result['cross_cell_line_indexes']


@pytest.mark.parametrize('field', ['image_sha256', 'candidate_sha256', 'dimensions'])
def test_transcription_cannot_be_applied_to_another_image_or_ocr(tmp_path, field):
    candidate, annotation, path, annotated = _candidate(tmp_path)
    annotation[field] = [101, 100] if field == 'dimensions' else 'b' * 64
    with pytest.raises(ValueError, match='identity mismatch'):
        source_grid_review(candidate, annotation,
            candidate_sha256=sha256_file(path), annotation_sha256=sha256_file(annotated))


@pytest.mark.parametrize('field', ['financial_observation_admitted', 'article_body_modified',
    'historical_release_clock_inferred'])
def test_review_does_not_reopen_an_admitted_or_modified_candidate(tmp_path, field):
    candidate, annotation, path, annotated = _candidate(tmp_path)
    candidate[field] = True
    with pytest.raises(ValueError, match='unsupported or admitted'):
        source_grid_review(candidate, annotation,
            candidate_sha256=sha256_file(path), annotation_sha256=sha256_file(annotated))


def test_review_output_reuses_exact_evidence_and_rejects_changed_image(tmp_path):
    candidate, _, path, annotated = _candidate(tmp_path)
    output = tmp_path / 'private-review.json'
    originals = path.read_bytes(), annotated.read_bytes()
    result, reused = review(path, annotated, output)
    before = output.read_bytes(), output.stat().st_ino, output.stat().st_mtime_ns
    assert not reused and result['counts'] == {'disagrees': 1}
    assert output.stat().st_mode & 0o777 == 0o600
    assert review(path, annotated, output) == (result, True)
    assert before == (output.read_bytes(), output.stat().st_ino, output.stat().st_mtime_ns)
    assert originals == (path.read_bytes(), annotated.read_bytes())
    assert result['source_requests'] == 0 and not result['ocr_inference_repeated']
    assert not result['article_body_modified'] and not result['financial_observation_admitted']
    assert sha256_file(Path(candidate['image_path'])) == candidate['image_sha256']
    with open(candidate['image_path'], 'ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='image hash mismatch'):
        review(path, annotated, output)
    assert before == (output.read_bytes(), output.stat().st_ino, output.stat().st_mtime_ns)


def _label_annotation(candidate, candidate_sha, text='300', kind='decimal', role='axis_tick'):
    return dict(kind='source_image_visual_label_review', contract_version=1,
        image_sha256=candidate['image_sha256'], candidate_sha256=candidate_sha,
        dimensions=candidate['dimensions'], labels=[dict(id='visible-axis', line_indexes=[0],
            role=role, kind=kind, text=text, unit='%', region=[10, 10, 60, 30],
            visual_basis='Read from retained source pixels, independent of OCR confidence')])


def _label_result(candidate, annotation, **kwargs):
    return source_label_review(candidate, annotation,
        candidate_sha256=annotation['candidate_sha256'], annotation_sha256='b' * 64, **kwargs)


@pytest.mark.parametrize('actual,expected,kind,status', [
    ('50', '-50', 'decimal', 'disagrees'), ('0', '-0', 'decimal', 'disagrees'),
    ('050.00', '50', 'decimal', 'agrees'), ('cUu', '200', 'decimal', 'unreadable_number'),
    ('外资買責超（张）', '外資買賣超(張)', 'literal', 'disagrees'),
    ('0050', '0050', 'identifier', 'agrees'), ('50', '0050', 'identifier', 'disagrees'),
])
def test_label_review_retains_raw_text_units_signs_and_separate_pixel_annotation(tmp_path, actual, expected, kind, status):
    candidate, _, path, _ = _candidate(tmp_path)
    candidate['lines'][0]['txt'] = actual
    annotation = _label_annotation(candidate, sha256_file(path), expected, kind)
    original = deepcopy((candidate, annotation))
    result = _label_result(candidate, annotation)
    label, = result['labels']
    assert label['status'] == status and label['review_text'] == expected
    assert label['ocr_candidates'][0]['text'] == actual and label['reviewed_unit'] == '%'
    assert result['remaining_scope_covered'] and not result['financial_observation_admitted']
    assert not result['historical_release_clock_inferred'] and not result['chart_curves_digitized']
    assert (candidate, annotation) == original


def test_rotated_date_fragments_are_recorded_without_joining_or_backdating(tmp_path):
    candidate, _, path, _ = _candidate(tmp_path)
    candidate['lines'] = [_line('82/20/1'), _line('120', top=26, bottom=29)]
    annotation = _label_annotation(candidate, sha256_file(path), '2021/07/28', 'identifier', 'axis_date')
    annotation['labels'][0]['line_indexes'] = [0, 1]
    result = _label_result(candidate, annotation)
    assert result['counts'] == {'fragmented_candidates': 1}
    assert [c['text'] for c in result['labels'][0]['ocr_candidates']] == ['82/20/1', '120']
    assert 'candidate_decimal' not in result['labels'][0]
    assert not result['historical_release_clock_inferred']


@pytest.mark.parametrize('kind,role,status', [('non_text', 'non_text', 'non_text_detection'),
    ('unreadable', 'axis_date', 'unreadable_source')])
def test_chart_strokes_and_unreadable_source_never_become_text(tmp_path, kind, role, status):
    candidate, _, path, _ = _candidate(tmp_path)
    annotation = _label_annotation(candidate, sha256_file(path), None, kind, role)
    result = _label_result(candidate, annotation)
    assert result['counts'] == {status: 1}
    assert result['labels'][0]['review_text'] is None
    assert result['labels'][0]['ocr_candidates'][0]['score'] == .99
    annotation['labels'][0]['text'] = 'invented prose'
    with pytest.raises(ValueError, match='cannot supply text'):
        _label_result(candidate, annotation)


def test_visually_observed_missing_tick_is_not_synthetic_ocr(tmp_path):
    candidate, _, path, _ = _candidate(tmp_path)
    annotation = _label_annotation(candidate, sha256_file(path))
    annotation['labels'].append(dict(annotation['labels'][0], id='missing-axis-zero',
        text='0', line_indexes=[], region=[10, 35, 60, 45]))
    result = _label_result(candidate, annotation)
    assert result['counts'] == {'agrees': 1, 'missing_candidate': 1}
    assert result['labels'][1]['ocr_candidates'] == []
    assert candidate['lines'][0]['txt'] == '300' and len(candidate['lines']) == 1


def _prior_grid(candidate, candidate_sha):
    return dict(kind='source_image_ocr_grid_comparison', contract_version=1,
        image_sha256=candidate['image_sha256'], candidate_sha256=candidate_sha,
        total_ocr_lines=len(candidate['lines']), matched_line_indexes=[0],
        financial_observation_admitted=False)


def test_remaining_labels_reuse_exact_numeric_grid_without_reviewing_it_again(tmp_path):
    candidate, _, path, _ = _candidate(tmp_path)
    candidate['lines'].append(_line('張'))
    annotation = _label_annotation(candidate, sha256_file(path), '張', 'literal', 'table_header')
    annotation['labels'][0]['line_indexes'] = [1]
    annotation['previous_comparison_sha256'] = 'c' * 64
    previous = _prior_grid(candidate, sha256_file(path))
    original = deepcopy(previous)
    result = _label_result(candidate, annotation, previous_comparison=previous,
        previous_comparison_sha256='c' * 64)
    assert result['previously_reviewed_line_indexes'] == [0] and result['reviewed_line_indexes'] == [1]
    assert result['total_review_cells'] == 1 and previous == original
    annotation['labels'][0]['line_indexes'] = [0]
    with pytest.raises(ValueError, match='overlapping'):
        _label_result(candidate, annotation, previous_comparison=previous, previous_comparison_sha256='c' * 64)


@pytest.mark.parametrize('case', ['hash', 'candidate', 'image', 'admitted', 'count', 'indexes', 'absent'])
def test_changed_or_missing_completed_numeric_review_cannot_hide_lines(tmp_path, case):
    candidate, _, path, _ = _candidate(tmp_path)
    candidate['lines'].append(_line('張'))
    annotation = _label_annotation(candidate, sha256_file(path), '張', 'literal', 'table_header')
    annotation['labels'][0]['line_indexes'] = [1]
    annotation['previous_comparison_sha256'] = 'c' * 64
    previous = _prior_grid(candidate, sha256_file(path))
    if case == 'hash': annotation['previous_comparison_sha256'] = 'd' * 64
    elif case == 'candidate': previous['candidate_sha256'] = 'd' * 64
    elif case == 'image': previous['image_sha256'] = 'd' * 64
    elif case == 'admitted': previous['financial_observation_admitted'] = True
    elif case == 'count': previous['total_ocr_lines'] = 1
    elif case == 'indexes': previous['matched_line_indexes'] = [0, 0]
    elif case == 'absent': previous = None
    with pytest.raises(ValueError):
        _label_result(candidate, annotation, previous_comparison=previous, previous_comparison_sha256='c' * 64)


@pytest.mark.parametrize('case', ['omitted', 'duplicate', 'bool', 'unhashable', 'outside', 'disjoint', 'empty_basis', 'duplicate_id'])
def test_remaining_review_requires_complete_unique_spatially_bound_explicit_labels(tmp_path, case):
    candidate, _, path, _ = _candidate(tmp_path)
    annotation = _label_annotation(candidate, sha256_file(path))
    item = annotation['labels'][0]
    if case == 'omitted': candidate['lines'].append(_line('unreviewed date'))
    elif case == 'duplicate': item['line_indexes'] = [0, 0]
    elif case == 'bool': item['line_indexes'] = [False]
    elif case == 'unhashable': item['line_indexes'] = [[]]
    elif case == 'outside': item['region'] = [10, 10, 101, 30]
    elif case == 'disjoint': item['region'] = [70, 70, 90, 90]
    elif case == 'empty_basis': item['visual_basis'] = ''
    elif case == 'duplicate_id': annotation['labels'].append(dict(item, line_indexes=[]))
    with pytest.raises(ValueError):
        _label_result(candidate, annotation)


def test_label_cli_reuses_outputs_and_rejects_changed_completed_evidence(tmp_path):
    candidate, _, path, annotated = _candidate(tmp_path)
    candidate['lines'].append(_line('張'))
    path.write_text(json.dumps(candidate))
    prior = tmp_path / 'prior-grid.json'
    prior.write_text(json.dumps(_prior_grid(candidate, sha256_file(path))))
    annotation = _label_annotation(candidate, sha256_file(path), '張', 'literal', 'table_header')
    annotation['labels'][0]['line_indexes'] = [1]
    annotation['previous_comparison_sha256'] = sha256_file(prior)
    annotated.write_text(json.dumps(annotation))
    output = tmp_path / 'remaining-labels.json'
    first = review(path, annotated, output, previous_comparison_path=prior)
    assert first[0]['counts'] == {'agrees': 1} and not first[1]
    before = output.read_bytes(), output.stat().st_ino, output.stat().st_mtime_ns
    assert review(path, annotated, output, previous_comparison_path=prior) == (first[0], True)
    assert before == (output.read_bytes(), output.stat().st_ino, output.stat().st_mtime_ns)
    prior.write_text(prior.read_text() + '\n')
    with pytest.raises(ValueError, match='identity mismatch'):
        review(path, annotated, output, previous_comparison_path=prior)
    assert before == (output.read_bytes(), output.stat().st_ino, output.stat().st_mtime_ns)
