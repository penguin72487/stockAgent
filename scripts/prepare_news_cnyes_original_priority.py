"""Recreate the configured local Cnyes audit hint from its offline report.

This writes only the configured private artifact. It does not open the news
queue, acquire the downloader owner, or make a publisher request.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext import load_config
from downloader.news_fulltext_cnyes_priority import make_plan, plan_bytes


def prepare(config_path, report_path):
    config, registry, _ = load_config(config_path)
    spec = config.get('cnyes_original_audit_priority_plan')
    if spec is None:
        raise ValueError('cnyes_priority_plan_not_configured')
    with report_path.open('rb') as handle:
        report = handle.read((8 << 20) + 1)
    if len(report) > 8 << 20:
        raise ValueError('source_report_too_large')
    plan = make_plan(report, registry.sources['cnyes'])
    raw = plan_bytes(plan)
    actual = hashlib.sha256(raw).hexdigest()
    if actual != spec['sha256']:
        raise ValueError('configured_priority_digest_does_not_match_report')
    path = Path(spec['path'])
    if path.is_symlink() or path.exists() and path.read_bytes() != raw:
        raise ValueError('existing_priority_artifact_conflict')
    if not path.exists():
        atomic_write_bytes(path, raw, durable=True, mode=0o600)
    return {'contract_version': 1, 'targets': len(plan['observations']), 'sha256': actual,
            'publisher_requests': 0, 'production_queue_writes': 0, 'accepted_as_originals': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.config, args.report), sort_keys=True))


if __name__ == '__main__':
    main()
