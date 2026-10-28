#!/usr/bin/env python3
"""Audit retained OCR against private, image-bound visual grid annotations."""
import argparse
import json
from pathlib import Path
import time

from downloader.artifact_io import atomic_write_bytes, sha256_file
from stockagent.ocr.review import source_grid_review, source_label_review


def review(candidate_path, annotation_path, output, *, previous_comparison_path=None):
    candidate_sha, annotation_sha = sha256_file(candidate_path), sha256_file(annotation_path)
    candidate = json.loads(candidate_path.read_bytes())
    annotation = json.loads(annotation_path.read_bytes())
    prior_sha = sha256_file(previous_comparison_path) if previous_comparison_path is not None else None
    if annotation.get('kind') == 'source_image_visual_label_review':
        prior = json.loads(previous_comparison_path.read_bytes()) if previous_comparison_path is not None else None
        result = source_label_review(candidate, annotation, candidate_sha256=candidate_sha,
            annotation_sha256=annotation_sha, previous_comparison=prior, previous_comparison_sha256=prior_sha)
    else:
        if previous_comparison_path is not None:
            raise ValueError('previous comparison is only valid for remaining-label review')
        result = source_grid_review(candidate, annotation,
            candidate_sha256=candidate_sha, annotation_sha256=annotation_sha)
    root = Path(__file__).resolve().parents[1]
    result['implementation_sha256'] = {name: sha256_file(root / name) for name in (
        'stockagent/ocr/review.py', 'scripts/review_news_ocr_candidates.py')}
    image = Path(candidate['image_path'])
    if sha256_file(image) != candidate['image_sha256']:
        raise ValueError('retained source image hash mismatch')
    if (candidate_sha != sha256_file(candidate_path) or annotation_sha != sha256_file(annotation_path)
            or previous_comparison_path is not None and sha256_file(previous_comparison_path) != prior_sha):
        raise ValueError('review inputs changed during read')
    # Existing durable comparisons are reusable, never overwritten by a rerun.
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
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--annotation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--previous-comparison', type=Path,
        help='Pinned completed grid review; only its remaining labels may be reviewed')
    args = parser.parse_args()
    started = time.monotonic()
    result, reused = review(args.candidate, args.annotation, args.output,
        previous_comparison_path=args.previous_comparison)
    print(json.dumps(dict(review_cells=result['total_review_cells'], counts=result['counts'],
        original_candidates_changed=False, source_requests=0, ocr_inference_repeated=False,
        financial_observation_admitted=False, reused=reused,
        wall_seconds=time.monotonic() - started)), flush=True)


if __name__ == '__main__':
    main()
