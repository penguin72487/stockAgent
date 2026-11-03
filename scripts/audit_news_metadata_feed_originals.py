"""Verify retained RSS information originals without writing the news queue.

This audits registered feed/fingerprint directories and their original/receipt
pairs using the collector's current reader. It makes no publisher request and
does not recover article bodies or prove that a publisher's history is complete.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace

import pyarrow as pa

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_metadata_checkpoint import MAX_BODY_BYTES, _original, _receipt
from downloader.news_fulltext_metadata_feeds import compatible_fingerprints, require_request
from downloader.news_fulltext_policy import NewsError


_ORIGINAL = re.compile(r'[0-9a-f]{64}\.parquet\Z')


def _stamp(path):
    try:
        value = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _error_code(error):
    # Never copy response content, exception paths, or untrusted parser text.
    if isinstance(error, NewsError):
        return error.code
    if type(error) is ValueError and re.fullmatch(r'[a-z0-9_]{1,96}', str(error)):
        return str(error)
    return type(error).__name__


def audit(root, registry, provider, *, max_files=None, progress=None):
    """Inspect the registered scope; compare all inspected input stamps again.

    A partial run is expressly marked incomplete. Identical response content
    in different feed/fingerprint directories remains distinct evidence.
    """
    if max_files is not None and (type(max_files) is not int or max_files < 1):
        raise ValueError('max_files must be a positive integer')
    root = Path(root).resolve(strict=True)
    source = registry.sources[provider]
    if not source.metadata_feeds:
        raise ValueError('provider has no registered metadata feeds')
    queue = SimpleNamespace(root=root)
    start = time.monotonic()
    files, directories, issues = {}, {}, []
    counts = Counter()
    complete = True
    stop = False
    for fingerprint in compatible_fingerprints(source):
        for feed in source.metadata_feeds:
            require_request(registry, feed, feed)
            directory = root / 'metadata_feeds' / fingerprint / hashlib.sha256(feed.encode()).hexdigest()
            relative_dir = directory.relative_to(root).as_posix()
            stamp = _stamp(directory)
            directories[directory] = stamp
            counts['registered_directories'] += 1
            if stamp is None:
                counts['absent_directories'] += 1
                continue
            if directory.is_symlink() or not directory.is_dir() or not directory.resolve().is_relative_to(root):
                issues.append({'path': relative_dir, 'reason': 'unsafe_directory'})
                complete = False
                continue
            counts['present_directories'] += 1
            originals, receipts = set(), set()
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        if not entry.is_file(follow_symlinks=False):
                            issues.append({'path': relative_dir + '/' + entry.name, 'reason': 'not_regular_file'})
                            complete = False
                        elif _ORIGINAL.fullmatch(entry.name):
                            originals.add(entry.name)
                        elif entry.name.endswith('.parquet.receipt.json') and _ORIGINAL.fullmatch(entry.name[:-len('.receipt.json')]):
                            receipts.add(entry.name[:-len('.receipt.json')])
                        else:
                            issues.append({'path': relative_dir + '/' + entry.name, 'reason': 'unexpected_file'})
                            complete = False
            except OSError:
                issues.append({'path': relative_dir, 'reason': 'directory_unreadable'})
                complete = False
                continue
            counts['original_paths'] += len(originals)
            counts['receipt_paths'] += len(receipts)
            if originals != receipts:
                complete = False
            for name in sorted(originals - receipts):
                issues.append({'path': relative_dir + '/' + name, 'reason': 'missing_receipt'})
            for name in sorted(receipts - originals):
                issues.append({'path': relative_dir + '/' + name, 'reason': 'missing_original'})
            for name in sorted(originals & receipts):
                if max_files is not None and counts['inspected_originals'] >= max_files:
                    complete = False
                    stop = True
                    break
                path = directory / name
                receipt_path = directory / (name + '.receipt.json')
                before = (_stamp(path), _stamp(receipt_path))
                files[path], files[receipt_path] = before
                counts['inspected_originals'] += 1
                try:
                    relative = path.relative_to(root).as_posix()
                    receipt = _receipt(queue, relative, required=True)
                    response_bytes = receipt.get('response_bytes') if isinstance(receipt, dict) else None
                    if type(response_bytes) is not int or not 0 <= response_bytes <= MAX_BODY_BYTES:
                        raise ValueError('invalid_metadata_legacy_response_size')
                    value = {'head': {'url': feed}, 'response_sha256': name[:-len('.parquet')],
                        'metadata_fingerprint': fingerprint, 'response_bytes': response_bytes,
                        'headers': {'content-type': 'application/rss+xml'}}
                    _, _, items = _original(queue, value, source, legacy=True)
                    counts['verified_originals'] += 1
                    counts['metadata_rows'] += len(items)
                except (OSError, ValueError, TypeError, KeyError, OverflowError, NewsError, pa.ArrowException) as error:
                    issues.append({'path': relative_dir + '/' + name, 'reason': _error_code(error)})
                if progress and counts['inspected_originals'] % 1000 == 0:
                    progress(dict(counts))
            if stop:
                break
        if stop:
            break
    changed_files = sum(_stamp(path) != old for path, old in files.items())
    changed_directories = sum(_stamp(path) != old for path, old in directories.items())
    if changed_files or changed_directories:
        complete = False
    return {'contract': 'read_only_metadata_feed_original_audit_v1',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(), 'provider': provider,
        'compatible_fingerprints': list(compatible_fingerprints(source)),
        'registered_feeds': len(source.metadata_feeds), 'expected_directories': len(source.metadata_feeds) * len(compatible_fingerprints(source)),
        'counts': dict(counts), 'issue_count': len(issues), 'issues': issues,
        'changed_files': changed_files, 'changed_directories': changed_directories,
        'scope_complete': complete, 'input_snapshot_stable': not changed_files and not changed_directories,
        'historical_http_status_or_headers_proven': False, 'article_bodies_verified': False,
        'all_history_complete': False, 'publisher_requests': 0, 'production_queue_writes': 0,
        'elapsed_seconds': round(time.monotonic() - start, 6)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_news_fulltext'))
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--provider', default='ltn')
    parser.add_argument('--max-files', type=int)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    # Atomic replacement writes the parent directory entry even if an existing
    # output symlink resolves outside the source root.
    if args.output.parent.resolve().is_relative_to(root) or args.output.resolve().is_relative_to(root):
        parser.error('output must be outside the source data root')
    _, registry, _ = load_config(args.config)
    def show_progress(counts):
        print(json.dumps({'progress': counts}, ensure_ascii=False, sort_keys=True), flush=True)
    result = audit(args.root, registry, args.provider, max_files=args.max_files, progress=show_progress)
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({key: value for key, value in result.items() if key != 'issues'}, ensure_ascii=False, sort_keys=True))
    return 0 if result['scope_complete'] and result['input_snapshot_stable'] and not result['issue_count'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
