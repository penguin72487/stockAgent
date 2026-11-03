"""Bind a private corpus audit and independent readback to the news dashboard.

This publishes only a local evidence reference. It does not edit either news
data root, submit a review, start a service or contact a publisher.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
import json
import os
from pathlib import Path

from downloader.artifact_io import atomic_write_bytes
from stockagent.live.news_corpus_quality import (
    ARTIFACTS, HEADLINE_REFERENCE, REFERENCE, QualityEvidenceError, build_reference,
    inspect_headline_reference, inspect_reference, public_headline_quality_status, public_quality_status,
)


REPO = Path(__file__).resolve().parents[1]


def publish(repo: Path, audit: Path, readback: Path, *, now: datetime | None = None,
            kind: str = 'body') -> dict:
    repo = repo.resolve(strict=True)
    now = now or datetime.now(UTC)
    if kind == 'body':
        pointer, inspect, public = REFERENCE, inspect_reference, public_quality_status
    elif kind == 'cmnews-tag-headlines':
        pointer, inspect, public = HEADLINE_REFERENCE, inspect_headline_reference, public_headline_quality_status
    else:
        raise ValueError('unsupported quality evidence kind')
    reference = build_reference(repo, audit, readback)
    directory = repo / pointer.parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    quality_root = (repo / ARTIFACTS).resolve(strict=True)
    if not directory.resolve(strict=True).is_relative_to(quality_root):
        raise QualityEvidenceError('reference_directory_outside_quality_root')
    lock_path = directory / 'latest.lock'
    lock = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        candidate = inspect(repo, reference, now=now)
        if candidate['state'] == 'superseded':
            raise QualityEvidenceError('audit_does_not_match_current_corpus')
        previous = public(repo, now=now)
        if (previous.get('observed_at_utc') and
                datetime.fromisoformat(previous['observed_at_utc']) > datetime.fromisoformat(candidate['observed_at_utc'])):
            raise QualityEvidenceError('newer_current_audit_already_published')
        payload = (json.dumps(reference, sort_keys=True) + '\n').encode()
        atomic_write_bytes(repo / pointer, payload, durable=True, mode=0o600)
        if public(repo, now=now) != candidate:
            raise QualityEvidenceError('published_reference_changed_or_unavailable')
        return candidate
    finally:
        os.close(lock)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, default=REPO)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--readback', type=Path, required=True)
    parser.add_argument('--kind', choices=('body', 'cmnews-tag-headlines'), default='body')
    args = parser.parse_args()
    print(json.dumps(publish(args.repo_root, args.audit, args.readback, kind=args.kind), ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
