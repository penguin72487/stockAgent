"""Source-proven Cnyes story links, not permission to merge page observations.

Verify explicitly selected current heads with their immutable source receipts
and registered parsers. An API body and mobile HTML can declare the same story
while retaining different bodies, authors and capture vintages. No HTTP, queue
writer, field copying, query stripping, or all-history completeness claim.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_publishers import cnyes_supported_mobile_view
from scripts.audit_news_selected_articles import audit as audit_selected


def audit(root, mobile_urls, registry):
    if not 1 <= len(mobile_urls) <= 8 or len(set(mobile_urls)) != len(mobile_urls):
        raise ValueError('one_to_eight_distinct_mobile_views_required')
    started, cpu = time.monotonic(), time.process_time()
    pairs = []
    for url in mobile_urls:
        parts = urlsplit(url)
        match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
        if (not match or parts.scheme != 'https' or parts.netloc != 'm.cnyes.com'
                or parts.fragment or not cnyes_supported_mobile_view(parts)):
            raise ValueError('unsupported_mobile_view_not_normalized')
        pairs.append((url, 'https://news.cnyes.com/news/id/' + match[1]))
    urls = list(dict.fromkeys(u for pair in pairs for u in pair))
    proof = audit_selected(root, urls, registry=registry)
    heads = {item['url']: item for item in proof['articles']}
    links = []
    for mobile, canonical in pairs:
        left, right = heads.get(mobile), heads.get(canonical)
        identity = bool(left and right and left.get('current_source_replay_verified')
            and right.get('current_source_replay_verified')
            and left.get('extraction_method') == 'cnyes_visible_canonical_alias_v1'
            and left.get('declared_canonical_url') == right.get('declared_canonical_url') == canonical)
        equal_clock = None
        if identity:
            a = datetime.fromisoformat(left['published_at_raw'].replace('Z', '+00:00'))
            b = datetime.fromisoformat(right['published_at_raw'].replace('Z', '+00:00'))
            equal_clock = a.tzinfo is not None and b.tzinfo is not None and a == b
        links.append({'mobile_url': mobile, 'canonical_url': canonical,
            'publisher_declared_story_identity_verified': identity,
            'exact_body_digest_equal': left['body_sha256'] == right['body_sha256'] if identity else None,
            'publication_instant_equal': equal_clock,
            'original_authors_equal': left['authors_json'] == right['authors_json'] if identity else None,
            'captures_equal': left['captured_at_utc'] == right['captured_at_utc'] if identity else None,
            'observations_interchangeable': False})
    return {'contract_version': 1, 'selected_head_proof': proof, 'story_links': links,
        'accepted': proof['accepted'] and all(p['publisher_declared_story_identity_verified'] for p in links),
        'scope': 'explicit source-declared story links; separate original observations, not entire queue or interchangeable views',
        'publisher_requests': 0, 'source_states_mutated': False, 'fields_copied': False,
        'body_equivalence_assumed_from_numeric_id': False, 'historical_completeness_verified': False,
        'publish': False, 'remote_training_export': False,
        'inventory_cost': {'wall_seconds': time.monotonic() - started, 'cpu_seconds': time.process_time() - cpu}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--mobile-url', action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], args.mobile_url, registry)
    result['configuration_sha256'] = sha256_file(args.config)
    result['implementation_sha256'] = sha256_file(Path(__file__))
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print({key: result[key] for key in ('accepted', 'story_links', 'inventory_cost', 'publisher_requests')})
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
