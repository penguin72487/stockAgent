"""Publish a local historical sorting-hint audit reference; no queue or HTTP access."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
import json
import os
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes
from stockagent.live.news_corpus_quality import (
    ARTIFACTS, SORT_DAY_REFERENCE, QualityEvidenceError, build_sort_day_reference,
    inspect_sort_day_reference, public_sort_day_quality_status,
)


def publish(repo: Path, audit: Path, *, now: datetime | None = None) -> dict:
    repo = repo.resolve(strict=True)
    now = now or datetime.now(UTC)
    reference = build_sort_day_reference(repo, audit)
    directory = repo / SORT_DAY_REFERENCE.parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not directory.resolve(strict=True).is_relative_to((repo / ARTIFACTS).resolve(strict=True)):
        raise QualityEvidenceError('reference_directory_outside_quality_root')
    lock = os.open(directory / 'sort_day.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        candidate = inspect_sort_day_reference(repo, reference, now=now)
        previous = public_sort_day_quality_status(repo, now=now)
        if (previous.get('observed_at_utc') and
                datetime.fromisoformat(previous['observed_at_utc']) > datetime.fromisoformat(candidate['observed_at_utc'])):
            raise QualityEvidenceError('newer_sort_audit_already_published')
        payload = (json.dumps(reference, sort_keys=True) + '\n').encode()
        atomic_write_bytes(repo / SORT_DAY_REFERENCE, payload, durable=True, mode=0o600)
        if public_sort_day_quality_status(repo, now=now) != candidate:
            raise QualityEvidenceError('published_sort_audit_changed_or_unavailable')
        return candidate
    finally:
        os.close(lock)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--audit', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(publish(args.repo_root, args.audit), ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
