"""Compare unfinished CNYES issuer names with receipt-backed same-day market lists.

The exchange list is later/independently observed data. A name match is not
proof of announcement identity, article text, or point-in-time availability.
All reads are local and no live queue or source is modified.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.parquet_integrity import signature


TAIPEI = ZoneInfo('Asia/Taipei')
HASH = re.compile(r'[0-9a-f]{64}')
JOURNAL_DATE = re.compile(rb'"date"\s*:\s*"(\d{4}-\d{2}-\d{2})"')
VENUES = {'twse': {'dataset': 'twse_daily_ohlcv', 'hosts': {'www.twse.com.tw', 'wwwc.twse.com.tw'},
    'path': '/rwd/zh/afterTrading/MI_INDEX', 'type': 'ALLBUT0999',
    'title': '每日收盤行情', 'symbol': '證券代號', 'name': '證券名稱'},
    'tpex': {'dataset': 'tpex_daily_ohlcv', 'hosts': {'www.tpex.org.tw'},
    'path': '/www/zh-tw/afterTrading/otc', 'type': 'EW',
    'title': '上櫃股票每日收盤行情', 'symbol': '代號', 'name': '名稱'}}


def _journal_url_ok(url, spec, day):
    if not isinstance(url, str):
        return False
    parsed = urlsplit(url)
    params = parse_qs(parsed.query)
    dates = params.get('date', [])
    return (parsed.scheme == 'https' and parsed.hostname in spec['hosts'] and parsed.path == spec['path']
            and len(dates) == 1 and dates[0].replace('/', '') == day.replace('-', '')
            and params.get('type') == [spec['type']])


def _journal(root, spec, days):
    """Scan the relevant canonical journal once, retaining exact source receipts."""
    path = root / 'state/journals' / (spec['dataset'] + '.jsonl')
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError('unsafe_journal')
    before = signature(path)
    records, digest = defaultdict(list), hashlib.sha256()
    with path.open('rb') as stream:
        for line in stream:
            digest.update(line)
            if len(line) > 2 << 20:
                raise ValueError('journal_line_oversized')
            if not any(match.group(1).decode() in days for match in JOURNAL_DATE.finditer(line)):
                continue
            item = json.loads(line)
            day = item.get('date')
            if day not in days:
                continue
            sha, size = item.get('raw_sha256'), item.get('raw_size')
            if (item.get('dataset') == spec['dataset'] and item.get('date') == day
                    and item.get('status') == 'data' and item.get('raw_path') == f"raw/{spec['dataset']}/{day}.json"
                    and isinstance(sha, str) and HASH.fullmatch(sha) and type(size) is int and size >= 0
                    and isinstance(item.get('recorded_at_utc'), str) and isinstance(item.get('source'), str)
                    and _journal_url_ok(item.get('url'), spec, day)):
                records[day].append({'sha256': sha, 'size': size, 'recorded_at_utc': item['recorded_at_utc'],
                                     'source': item['source'], 'url': item['url']})
    if signature(path) != before:
        raise ValueError('journal_changed_during_read')
    return records, {'dataset': spec['dataset'], 'sha256': digest.hexdigest(), 'path': str(path),
                     'size': before[2]}


def _daily_names(root, spec, day, symbols, journal_rows):
    path = root / 'raw' / spec['dataset'] / (day + '.json')
    if not path.exists():
        return {'state': 'raw_unavailable', 'records': []}
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        return {'state': 'unsafe_raw_path', 'records': []}
    try:
        before = signature(path)
        if before[2] > 16 << 20:
            return {'state': 'raw_oversized', 'records': []}
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        receipts = [item for item in journal_rows if item['sha256'] == sha and item['size'] == len(raw)]
        if not receipts:
            return {'state': 'raw_receipt_mismatch', 'raw_sha256': sha, 'records': []}
        payload = json.loads(raw)
        if (not isinstance(payload, dict) or str(payload.get('stat', '')).lower() != 'ok'
                or payload.get('date') != day.replace('-', '') or not isinstance(payload.get('tables'), list)):
            return {'state': 'raw_payload_invalid', 'raw_sha256': sha, 'records': []}
        tables = [table for table in payload['tables'] if isinstance(table, dict)
                  and spec['title'] in table.get('title', '') and isinstance(table.get('fields'), list)
                  and spec['symbol'] in table['fields'] and spec['name'] in table['fields']]
        if len(tables) != 1 or not isinstance(tables[0].get('data'), list):
            return {'state': 'raw_table_ambiguous', 'raw_sha256': sha, 'records': []}
        table = tables[0]
        symbol_index, name_index = (table['fields'].index(spec[field]) for field in ('symbol', 'name'))
        matches = []
        for row in table['data']:
            if (not isinstance(row, list) or len(row) <= max(symbol_index, name_index)
                    or not isinstance(row[symbol_index], str) or not isinstance(row[name_index], str)):
                return {'state': 'raw_row_invalid', 'raw_sha256': sha, 'records': []}
            if row[symbol_index] in symbols:
                matches.append({'symbol_literal': row[symbol_index], 'name_literal': row[name_index]})
        if signature(path) != before:
            return {'state': 'raw_changed_during_read', 'raw_sha256': sha, 'records': []}
        return {'state': 'verified_raw', 'raw_path': str(path), 'raw_sha256': sha,
                'raw_size': len(raw), 'table_title': table['title'], 'receipts': receipts, 'records': matches}
    except (OSError, ValueError, TypeError, KeyError) as error:
        return {'state': 'raw_read_error', 'error': type(error).__name__ + ':' + str(error)[:100], 'records': []}


def audit(report, root):
    root = root.resolve()
    rows, counts = report.get('rows'), report.get('counts', {})
    if (report.get('contract') != 'cnyes_unkeyed_tentative_issuer_v1' or report.get('accepted') is not True
            or report.get('source_failures') != [] or counts.get('source_envelopes_failed') != 0
            or counts.get('changed_heads') != 0 or counts.get('unverified_post_identities') != 0
            or not isinstance(rows, list) or len(rows) != counts.get('unkeyed_targets')
            or len(rows) != counts.get('verified_post_identities')):
        raise ValueError('unkeyed_audit_incomplete')
    selected = []
    for row in rows:
        key = row.get('tentative_finlab_key')
        if key is None:
            continue
        if (not isinstance(key, (list, tuple)) or len(key) != 3
                or not all(isinstance(v, str) and v for v in key)
                or tuple(key) != (row.get('stock_symbol_candidate'), row.get('publisher_day_taipei'), row.get('title_subject_literal'))
                or not isinstance(row.get('title_company_prefix_literal'), str)
                or not row['title_company_prefix_literal'] or type(row.get('publisher_epoch')) is not int):
            raise ValueError('tentative_candidate_invalid')
        try:
            day = date.fromisoformat(key[1])
            actual = datetime.fromtimestamp(row['publisher_epoch'], TAIPEI).date()
        except (ValueError, OverflowError, OSError) as error:
            raise ValueError('candidate_date_invalid') from error
        if day != actual or day.isoformat() != key[1]:
            raise ValueError('candidate_date_mismatch')
        selected.append(row)
    if len({(row['news_id'], row['source_post_sha256']) for row in selected}) != len(selected):
        raise ValueError('duplicate_candidate_identity')
    if len(selected) > 20_000:
        raise ValueError('candidate_bound_exceeded')
    by_day = defaultdict(set)
    for row in selected:
        by_day[row['publisher_day_taipei']].add(row['stock_symbol_candidate'])
    journals, day_evidence = {}, {}
    for venue, spec in VENUES.items():
        records, journal = _journal(root, spec, set(by_day))
        journals[venue] = journal
        for day, symbols in by_day.items():
            day_evidence.setdefault(day, {})[venue] = _daily_names(root, spec, day, symbols, records[day])
    out = []
    for row in selected:
        evidence = day_evidence[row['publisher_day_taipei']]
        by_venue = {venue: [item['name_literal'] for item in evidence[venue]['records']
                           if item['symbol_literal'] == row['stock_symbol_candidate']] for venue in VENUES}
        names = {name for values in by_venue.values() for name in values}
        complete = all(evidence[venue]['state'] == 'verified_raw' for venue in VENUES)
        name = row['title_company_prefix_literal']
        if len(names) > 1:
            state = 'conflicting_same_day_names'
        elif name in names:
            state = 'same_day_literal_name_match' if complete else 'same_day_literal_match_other_venue_unverified'
        elif names:
            state = 'same_day_different_name' if complete else 'same_day_different_name_other_venue_unverified'
        else:
            state = 'no_listed_name_in_both_verified_venues' if complete else 'daily_sources_incomplete'
        out.append({'news_id': row['news_id'], 'source_post_sha256': row['source_post_sha256'],
            'publisher_day_taipei': row['publisher_day_taipei'], 'stock_symbol_candidate': row['stock_symbol_candidate'],
            'title_company_prefix_literal': name, 'cnyes_market_names_for_symbol': row['market_names_for_symbol'],
            'exchange_names_by_venue': by_venue, 'state': state,
            'official_event_identity_verified': False, 'point_in_time_availability_verified': False,
            'article_body_found': False})
    invalid = {day: {venue: evidence['state'] for venue, evidence in venues.items()
                      if evidence['state'] not in {'verified_raw', 'raw_unavailable'}}
               for day, venues in day_evidence.items()}
    invalid = {day: venues for day, venues in invalid.items() if venues}
    return {'contract': 'cnyes_same_day_exchange_name_evidence_v1',
        'completed_at_utc': datetime.now(timezone.utc).isoformat(), 'counts': {'unkeyed_rows': len(rows),
            'tentative_rows_checked': len(selected), 'unkeyed_without_candidate': len(rows) - len(selected),
            'source_days': len(by_day), 'states': dict(Counter(row['state'] for row in out)),
            'invalid_source_day_venues': sum(len(venues) for venues in invalid.values())},
        'journals': journals, 'source_day_evidence': day_evidence, 'rows': out, 'invalid_sources': invalid,
        'accepted': not invalid, 'publisher_requests': 0, 'queue_writes': 0, 'article_bodies_copied': False,
        'cross_source_identity_verified': False, 'point_in_time_availability_verified': False,
        'all_history_complete': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--unkeyed-audit', type=Path, required=True)
    parser.add_argument('--tw-public-root', type=Path, default=Path('data_tw_public'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    if (args.output.is_symlink() or output.exists()
            or not output.is_relative_to((repo / 'artifacts/data_quality').resolve())):
        parser.error('output must be a new file under artifacts/data_quality')
    before = signature(args.unkeyed_audit)
    if before[2] > 128 << 20:
        parser.error('input audit oversized')
    raw = args.unkeyed_audit.read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    result = audit(json.loads(raw), args.tw_public_root)
    if signature(args.unkeyed_audit) != before or sha256_file(args.unkeyed_audit) != sha:
        raise ValueError('input_changed_during_audit')
    result['input_audit_sha256'] = sha
    result['implementation_sha256'] = sha256_file(Path(__file__).resolve())
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode(), durable=True, mode=0o600)
    print(json.dumps({'accepted': result['accepted'], 'counts': result['counts']}, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
