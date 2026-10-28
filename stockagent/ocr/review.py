"""Compare retained OCR with an explicit visual grid; never admit source values.

The review is a separate annotation. Agreement does not prove either an OCR
line or a manual transcription correct, and does not establish release time.
"""
from bisect import bisect_left, bisect_right
from collections import Counter
from decimal import Decimal, InvalidOperation
import math
import re

_NUMBER = re.compile(r"[+-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?\Z")
_MINUS = str.maketrans({"−": "-", "－": "-"})
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _finite(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("review geometry must contain finite numbers")
    return value


def _number(text):
    token = text.strip().translate(_MINUS)
    if not _NUMBER.fullmatch(token):
        raise ValueError("not an explicit decimal token")
    try:
        return Decimal(token.replace(",", ""))
    except InvalidOperation as error:
        raise ValueError("invalid decimal token") from error


def _compare(actual, expected, kind):
    if kind != "decimal":
        return "agrees" if actual == expected else "disagrees", {}
    source = _number(expected)
    try:
        candidate = _number(actual)
    except ValueError:
        return "unreadable_number", {"review_decimal": str(source)}
    # Keep a lost minus even for zero visible; Decimal equality alone hides it.
    agrees = candidate == source and (source != 0 or candidate.is_signed() == source.is_signed())
    return ("agrees" if agrees else "disagrees"), {
        "review_decimal": str(source), "candidate_decimal": str(candidate)}


def review_grid(lines, rows, dimensions):
    """Match lines to nonoverlapping rows/cells in O(L log R + local overlaps).

    Explicit row bands permit binary search without comparing every OCR line
    with every cell. A line crossing multiple cells remains ambiguous; text
    fragments, missing values, signs and units are never guessed or joined.
    """
    if (not isinstance(dimensions, list) or len(dimensions) != 2
            or any(type(v) is not int or v <= 0 for v in dimensions)):
        raise ValueError("invalid image dimensions")
    width, height = dimensions
    if not isinstance(lines, list) or not isinstance(rows, list) or not rows:
        raise ValueError("review requires OCR lines and explicit rows")
    bands, cells, identifiers = [], [], set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("cells"), list) or not row["cells"]:
            raise ValueError("invalid review row")
        top, bottom = _finite(row["top"]), _finite(row["bottom"])
        if not 0 <= top < bottom <= height:
            raise ValueError("review row outside image")
        band = []
        for item in row["cells"]:
            if not isinstance(item, dict):
                raise ValueError("invalid review cell")
            key, expected, kind = item.get("id"), item.get("text"), item.get("kind")
            if (not isinstance(key, str) or not key or key in identifiers
                    or not isinstance(expected, str) or not expected
                    or kind not in {"literal", "identifier", "decimal", "missing"}):
                raise ValueError("invalid or duplicate review cell")
            if kind == "decimal":
                _number(expected)
            if kind == "missing" and expected != "N/A":
                raise ValueError("missing values require an explicit N/A transcription")
            unit = item.get("unit")
            if unit is not None and (not isinstance(unit, str) or not unit):
                raise ValueError("invalid review unit")
            left, right = _finite(item["left"]), _finite(item["right"])
            if not 0 <= left < right <= width:
                raise ValueError("review cell outside image")
            identifiers.add(key)
            cell = dict(id=key, row=row.get("id"), review_text=expected, kind=kind,
                reviewed_unit=unit, rect=[left, top, right, bottom], line_indexes=[])
            cells.append(cell)
            band.append(cell)
        band.sort(key=lambda c: c["rect"][0])
        if any(a["rect"][2] > b["rect"][0] for a, b in zip(band, band[1:])):
            raise ValueError("overlapping review cells")
        bands.append((top, bottom, band))
    bands.sort(key=lambda band: band[0])
    if any(a[1] > b[0] for a, b in zip(bands, bands[1:])):
        raise ValueError("overlapping review rows")
    tops, bottoms = [b[0] for b in bands], [b[1] for b in bands]
    cross_cell, matched = set(), set()
    for index, line in enumerate(lines):
        if (not isinstance(line, dict) or not isinstance(line.get("txt"), str)
                or not isinstance(line.get("box"), list) or len(line["box"]) != 4):
            raise ValueError("invalid OCR line")
        score = _finite(line.get("score"))
        if not 0 <= score <= 1:
            raise ValueError("invalid OCR confidence")
        box = line["box"]
        if any(not isinstance(point, list) or len(point) != 2 for point in box):
            raise ValueError("invalid OCR polygon")
        xs, ys = zip(*[tuple(_finite(v) for v in point) for point in box])
        left, top, right, bottom = min(xs), min(ys), max(xs), max(ys)
        if (left < -2 or top < -2 or right > width + 2 or bottom > height + 2
                or left >= right or top >= bottom):
            raise ValueError("OCR geometry outside image or degenerate")
        hits = []
        first, stop = bisect_right(bottoms, top), bisect_left(tops, bottom)
        for row_top, row_bottom, band in bands[first:stop]:
            for cell in band:
                x0, _, x1, _ = cell["rect"]
                intersection = max(0, min(right, x1) - max(left, x0)) * (
                    max(0, min(bottom, row_bottom) - max(top, row_top)))
                # Retain all material overlaps, including exactly half-cells.
                if intersection / ((right - left) * (bottom - top)) >= 0.45:
                    hits.append(cell)
        if hits:
            matched.add(index)
            for cell in hits:
                cell["line_indexes"].append(index)
            if len(hits) > 1:
                cross_cell.add(index)
    for cell in cells:
        indexes = cell["line_indexes"]
        cell["ocr_candidates"] = [dict(line_index=i, text=lines[i]["txt"],
            box=lines[i]["box"], score=lines[i].get("score")) for i in indexes]
        if any(i in cross_cell for i in indexes):
            cell["status"] = "cross_cell_candidate"
        elif not indexes:
            cell["status"] = "missing_candidate"
        elif len(indexes) != 1:
            cell["status"] = "multiple_candidates"
        else:
            cell["status"], extra = _compare(lines[indexes[0]]["txt"], cell["review_text"], cell["kind"])
            cell.update(extra)
    return dict(cells=cells, counts=dict(Counter(c["status"] for c in cells)),
        total_review_cells=len(cells), total_ocr_lines=len(lines),
        matched_line_indexes=sorted(matched), unmatched_line_indexes=sorted(set(range(len(lines))) - matched),
        cross_cell_line_indexes=sorted(cross_cell), all_text_accuracy_proven=False,
        confidence_is_not_accuracy=True, original_candidates_changed=False,
        financial_observation_admitted=False, historical_release_clock_inferred=False)


def _source_review_identity(candidate, annotation, candidate_sha256, annotation_sha256, annotation_kind):
    if (candidate.get("kind") != "source_inline_image_ocr_candidate"
            or type(candidate.get("contract_version")) is not int or candidate["contract_version"] != 1
            or candidate.get("financial_observation_admitted") is not False
            or candidate.get("article_body_modified") is not False
            or candidate.get("historical_release_clock_inferred") is not False):
        raise ValueError("unsupported or admitted OCR candidate")
    if (annotation.get("kind") != annotation_kind
            or type(annotation.get("contract_version")) is not int or annotation["contract_version"] != 1):
        raise ValueError("unsupported visual review contract")
    for name, value in (("candidate_sha256", candidate_sha256), ("annotation_sha256", annotation_sha256),
                        ("image_sha256", candidate.get("image_sha256"))):
        if not isinstance(value, str) or not _SHA.fullmatch(value):
            raise ValueError("invalid review hash: " + name)
    if (annotation.get("image_sha256") != candidate["image_sha256"]
            or annotation.get("candidate_sha256") != candidate_sha256
            or annotation.get("dimensions") != candidate.get("dimensions")):
        raise ValueError("visual review image/OCR identity mismatch")


def source_grid_review(candidate, annotation, *, candidate_sha256, annotation_sha256):
    """Bind a separate manual review to the exact original image and OCR file."""
    _source_review_identity(candidate, annotation, candidate_sha256, annotation_sha256,
        "source_image_visual_grid_review")
    result = review_grid(candidate.get("lines"), annotation.get("rows"), candidate.get("dimensions"))
    return dict(kind="source_image_ocr_grid_comparison", contract_version=1,
        image_sha256=candidate["image_sha256"], candidate_sha256=candidate_sha256,
        annotation_sha256=annotation_sha256, article_url=candidate.get("article_url"),
        source_raw_sha256=candidate.get("source_raw_sha256"),
        basis="explicit visual transcription, not authoritative financial observation",
        source_requests=0, ocr_inference_repeated=False, article_body_modified=False, **result)


_LABEL_ROLES = frozenset({"title", "table_header", "direction", "source_credit", "footnote",
    "axis_tick", "axis_date", "legend", "summary_label", "summary_value", "reported_period", "non_text"})


def source_label_review(candidate, annotation, *, candidate_sha256, annotation_sha256,
                        previous_comparison=None, previous_comparison_sha256=None):
    """Review remaining labels in O(L + annotations), preserving finished grids.

    Labels are independently read from source pixels. Explicit line membership
    handles rotated dates, split labels and chart strokes without OCR reruns,
    automatic fragment joins, curve digitization or inferred release clocks.
    Every remaining OCR line must appear exactly once, including unreadable or
    non-text detections; completed numeric cells require the pinned old review.
    """
    _source_review_identity(candidate, annotation, candidate_sha256, annotation_sha256,
        "source_image_visual_label_review")
    dimensions, lines = candidate.get("dimensions"), candidate.get("lines")
    if (not isinstance(dimensions, list) or len(dimensions) != 2
            or any(type(v) is not int or v <= 0 for v in dimensions) or not isinstance(lines, list)):
        raise ValueError("invalid image dimensions or OCR lines")
    excluded = set()
    if previous_comparison is not None:
        if (not isinstance(previous_comparison_sha256, str) or not _SHA.fullmatch(previous_comparison_sha256)
                or annotation.get("previous_comparison_sha256") != previous_comparison_sha256
                or previous_comparison.get("kind") != "source_image_ocr_grid_comparison"
                or type(previous_comparison.get("contract_version")) is not int
                or previous_comparison["contract_version"] != 1
                or previous_comparison.get("candidate_sha256") != candidate_sha256
                or previous_comparison.get("image_sha256") != candidate["image_sha256"]
                or previous_comparison.get("total_ocr_lines") != len(lines)
                or previous_comparison.get("financial_observation_admitted") is not False):
            raise ValueError("completed review identity mismatch")
        prior_indexes = previous_comparison.get("matched_line_indexes")
        if (not isinstance(prior_indexes, list)
                or any(type(i) is not int or not 0 <= i < len(lines) for i in prior_indexes)
                or len(set(prior_indexes)) != len(prior_indexes)):
            raise ValueError("invalid completed review indexes")
        excluded = set(prior_indexes)
    elif previous_comparison_sha256 is not None or annotation.get("previous_comparison_sha256") is not None:
        raise ValueError("completed review must be supplied and pinned")
    labels = annotation.get("labels")
    if not isinstance(labels, list) or not labels:
        raise ValueError("explicit visual labels required")
    reviewed, identifiers, seen = [], set(), set()
    for label in labels:
        if not isinstance(label, dict):
            raise ValueError("invalid visual label")
        key, indexes = label.get("id"), label.get("line_indexes")
        role, kind, text = label.get("role"), label.get("kind"), label.get("text")
        basis, unit = label.get("visual_basis"), label.get("unit")
        if (not isinstance(key, str) or not key or key in identifiers
                or not isinstance(indexes, list)
                or any(type(i) is not int or not 0 <= i < len(lines) or i in excluded or i in seen for i in indexes)
                or len(set(indexes)) != len(indexes)
                or not isinstance(role, str) or role not in _LABEL_ROLES
                or not isinstance(kind, str) or kind not in {"literal", "identifier", "decimal", "missing", "non_text", "unreadable"}
                or not isinstance(basis, str) or not basis
                or unit is not None and (not isinstance(unit, str) or not unit)):
            raise ValueError("invalid or overlapping visual label")
        if kind in {"non_text", "unreadable"}:
            if text is not None or not indexes or (kind == "non_text" and role != "non_text"):
                raise ValueError("unreadable or non-text labels cannot supply text")
        elif not isinstance(text, str) or not text or role == "non_text":
            raise ValueError("visible label text required")
        if kind == "decimal":
            _number(text)
        if kind == "missing" and text != "N/A":
            raise ValueError("missing values require an explicit N/A transcription")
        region = label.get("region")
        if not isinstance(region, list) or len(region) != 4:
            raise ValueError("explicit visual region required")
        left, top, right, bottom = map(_finite, region)
        if not 0 <= left < right <= dimensions[0] or not 0 <= top < bottom <= dimensions[1]:
            raise ValueError("visual label outside image")
        copies = []
        for index in indexes:
            line = lines[index]
            if not isinstance(line, dict) or not isinstance(line.get("txt"), str):
                raise ValueError("invalid OCR label")
            bounds = line.get("box")
            if (not isinstance(bounds, list) or len(bounds) != 4
                    or any(not isinstance(p, list) or len(p) != 2 for p in bounds)):
                raise ValueError("invalid OCR label geometry")
            xs, ys = [[_finite(p[axis]) for p in bounds] for axis in (0, 1)]
            if (not 0 <= min(xs) < max(xs) <= dimensions[0]
                    or not 0 <= min(ys) < max(ys) <= dimensions[1]
                    or max(xs) <= left or min(xs) >= right or max(ys) <= top or min(ys) >= bottom):
                raise ValueError("OCR label does not intersect its visual region")
            copies.append(dict(line_index=index, text=line["txt"], box=[list(p) for p in bounds], score=line.get("score")))
        seen.update(indexes)
        identifiers.add(key)
        extra = {}
        if kind == "non_text":
            status = "non_text_detection"
        elif kind == "unreadable":
            status = "unreadable_source"
        elif not indexes:
            status = "missing_candidate"
        elif len(indexes) != 1:
            status = "fragmented_candidates"
        else:
            status, extra = _compare(lines[indexes[0]]["txt"], text, kind)
        reviewed.append(dict(id=key, role=role, kind=kind, review_text=text, reviewed_unit=unit,
            region=list(region), visual_basis=basis, ocr_candidates=copies, status=status, **extra))
    if seen != set(range(len(lines))) - excluded:
        raise ValueError("every remaining OCR line requires explicit review")
    return dict(kind="source_image_ocr_label_comparison", contract_version=1,
        image_sha256=candidate["image_sha256"], candidate_sha256=candidate_sha256,
        annotation_sha256=annotation_sha256, previous_comparison_sha256=previous_comparison_sha256,
        article_url=candidate.get("article_url"), source_raw_sha256=candidate.get("source_raw_sha256"),
        basis="explicit visual transcription and classification, not authoritative financial observations",
        labels=reviewed, counts=dict(Counter(label["status"] for label in reviewed)),
        total_review_cells=len(reviewed), total_ocr_lines=len(lines), reviewed_line_indexes=sorted(seen),
        previously_reviewed_line_indexes=sorted(excluded), remaining_scope_covered=True,
        source_requests=0, ocr_inference_repeated=False, article_body_modified=False,
        original_candidates_changed=False, confidence_is_not_accuracy=True,
        all_text_accuracy_proven=False, financial_observation_admitted=False,
        historical_release_clock_inferred=False, chart_curves_digitized=False)
