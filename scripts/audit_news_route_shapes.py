"""Summarize one observed source's URL shapes without publisher requests.

The database remains the authority: this is a bounded, read-only snapshot of
URL heads, not an article-identity, original-publication or access proof.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json


_ROUTE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,31}\Z")
_DATE_PREFIX = re.compile(r"\d{4}-\d{2}-\d{2}")
_STATE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}\Z")
MAX_URL_LIMIT = 1_000_000


def audit(database: Path, provider: str, *, max_urls: int = 100_000, state: str | None = None) -> dict:
    """Read a complete provider or provider/state selection, never a partial scan."""
    if (not isinstance(provider, str) or not provider or len(provider) > 300
            or not provider.isprintable() or provider != provider.strip()):
        raise ValueError('invalid provider')
    if type(max_urls) is not int or not 1 <= max_urls <= MAX_URL_LIMIT:
        raise ValueError('invalid URL scan limit')
    if state is not None and (not isinstance(state, str) or not _STATE.fullmatch(state)):
        raise ValueError('invalid state')
    database = Path(database).resolve(strict=True)
    db = sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        # LIMIT + 1 ensures an incomplete scan cannot be labelled complete.
        if state is None:
            rows = db.execute(
                'SELECT url,state,metadata_date_raw,body_sha256 FROM articles '
                'WHERE provider=? LIMIT ?', (provider, max_urls + 1))
        else:
            rows = db.execute(
                'SELECT url,state,metadata_date_raw,body_sha256 FROM articles '
                'WHERE provider=? AND state=? LIMIT ?', (provider, state, max_urls + 1))
        routes, states, years = Counter(), Counter(), Counter()
        known = stored = with_query = with_fragment = absent_dates = invalid_dates = 0
        first_day = last_day = None
        for url, article_state, raw_date, body_sha in rows:
            known += 1
            if known > max_urls:
                raise ValueError('source URL count exceeds scan limit')
            states[article_state] += 1
            stored += body_sha is not None
            try:
                if not isinstance(url, str) or len(url) > 16384:
                    raise ValueError('invalid URL')
                parsed = urlsplit(url)
                route = next((part for part in parsed.path.split('/') if part), '')
                route = route.lower() if _ROUTE.fullmatch(route) else '<other>'
                routes[route] += 1
                with_query += bool(parsed.query)
                with_fragment += bool(parsed.fragment)
            except ValueError:
                routes['<invalid>'] += 1
            if raw_date is None or raw_date == '':
                absent_dates += 1
                continue
            prefix = _DATE_PREFIX.match(raw_date) if isinstance(raw_date, str) else None
            try:
                day = date.fromisoformat(prefix.group()) if prefix else None
            except ValueError:
                day = None
            if day is None:
                invalid_dates += 1
                continue
            years[str(day.year)] += 1
            first_day = min(first_day, day) if first_day else day
            last_day = max(last_day, day) if last_day else day
        # Sparse route prefixes may contain user-specific slugs. Keep their
        # aggregate, not their strings, in the reusable evidence file.
        rare = sum(count for name, count in routes.items()
                   if not name.startswith('<') and count < 5)
        visible_routes = {name: count for name, count in routes.items()
                          if name.startswith('<') or count >= 5}
        if rare:
            visible_routes['<rare>'] = rare
        result = {
            'contract_version': 1,
            'observed_at_utc': datetime.now(timezone.utc).isoformat(),
            'provider': provider,
            'known_url_heads': known,
            'stored_body_heads': stored,
            'states': dict(sorted(states.items())),
            'first_path_segment_counts': dict(sorted(visible_routes.items())),
            'with_query_string': with_query,
            'with_fragment': with_fragment,
            'metadata_date': {
                'earliest_day': first_day.isoformat() if first_day else None,
                'latest_day': last_day.isoformat() if last_day else None,
                'year_counts': dict(sorted(years.items())),
                'absent': absent_dates,
                'invalid': invalid_dates,
                'basis': 'local metadata date prefix; not verified publisher publication time',
            },
            'scan_limit': max_urls,
            'scan_complete_for_provider_snapshot': state is None,
            'publisher_requests': 0,
            'production_queue_writes': False,
            'article_identity_verified': False,
            'all_publisher_history_complete': False,
        }
        if state is not None:
            result['selection'] = {'state': state}
            result['scan_complete_for_selection'] = True
        return result
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=Path('data_news_fulltext/queue.sqlite3'))
    parser.add_argument('--provider', required=True)
    parser.add_argument('--state', help='Limit the complete scan to one exact queue state')
    parser.add_argument('--max-urls', type=int, default=100_000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    result = audit(args.database, args.provider, max_urls=args.max_urls, state=args.state)
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(f"{args.provider}: {result['known_url_heads']} URL heads; {result['stored_body_heads']} stored bodies")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
