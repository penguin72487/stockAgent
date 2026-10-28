"""Private, bounded diagnosis of the canonical Cnyes news transport.

Read-only inventory by default. Probes share the production limiter, robots and
cooldowns; they never create another frontier owner or invent API permissions.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
import hashlib
import html
import json
from itertools import islice
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
import unicodedata
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import NewsHTTP, ACCESS_DIAGNOSTIC_HEADERS
from downloader.news_fulltext_policy import NewsError, _objects, _plain_text, _plain_text_node, extract_article, normalize_url
from downloader.news_fulltext_queue import source_path, utc_now


def cnyes_literal_route_hint(url):
    """Classify literal URL syntax, never article identity or source absence.

    Do not decode, normalize or generate a replacement URL. In particular,
    an old shtml path is still an unresolved article *candidate*, while a
    twstock route is a market-endpoint *candidate*, not downloaded news.
    """
    if not isinstance(url, str) or len(url) > 8192:
        return 'invalid_or_oversized_url'
    try:
        parts = urlsplit(url)
    except ValueError:
        return 'invalid_or_oversized_url'
    if parts.scheme not in {'http', 'https'} or not parts.netloc or parts.username or parts.password:
        return 'invalid_or_oversized_url'
    # Exact netlocs intentionally reject ports, lookalikes and transformed
    # identities. These hints confer no request, redirect or alias permission.
    if parts.netloc == 'www.cnyes.com' and parts.path.startswith('/twstock/'):
        return 'market_endpoint_route_candidate'
    if parts.netloc in {'news.cnyes.com', 'm.cnyes.com', 'amp-news.cnyes.com'}:
        if re.fullmatch(r'/news/id/[0-9]+/?', parts.path):
            return 'current_article_route_candidate'
        if re.fullmatch(r'(?:/(?:Content|content))?/[0-9]{8}/[A-Za-z0-9]+\.shtml', parts.path):
            return 'legacy_shtml_article_route_candidate'
        return 'other_news_host_route_unreviewed'
    if parts.netloc in {'www.cnyes.com', 'hao.cnyes.com', 'fund.cnyes.com', 'invest.cnyes.com'}:
        return 'other_cnyes_route_unreviewed'
    return 'foreign_origin_unreviewed'


def cnyes_corpus_market_head_audit(root: Path, *, notify=None):
    """Count this builder rule's effects in one pinned, existing corpus.

    Only URL and text-kind columns are read. An accepted manifest is not an
    independent re-verification of every Parquet byte: stat generations are
    checked around each read and the manifest is checked again at the end.
    This does not alter data, request publishers, or infer article equivalence.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from stockagent.data.news_dataset_sources import HEADLINE_SCOPE_CONTRACT, headline_metadata_only_reason
    from stockagent.data.news_dataset_storage import news_part_path

    root = root.resolve()
    manifest_path = root / 'manifest.json'
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest.get('state') != 'ready' or manifest.get('contract_version') not in {1, 2}:
        raise ValueError('news corpus manifest is not a supported ready version')
    parts = [p for p in manifest['parts'] if p['table_name'] == 'articles']
    if len({p['path'] for p in parts}) != len(parts):
        raise ValueError('duplicate article part in news corpus')
    reasons = defaultdict(int)
    headline_rows = fulltext_rows = projected_rows = pruned_rows = 0
    for number, part in enumerate(parts, 1):
        path = news_part_path(root, manifest, part)
        before = path.stat()
        signature = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        with pq.ParquetFile(path) as parquet:
            if parquet.metadata.num_rows != part['rows'] or before.st_size != part['bytes']:
                raise ValueError('news corpus article part size/row mismatch')
            kind_index = parquet.schema_arrow.get_field_index('text_kind')
            if kind_index < 0 or parquet.schema_arrow.get_field_index('url') < 0:
                raise ValueError('news corpus article projection is missing required columns')
            for group_number in range(parquet.metadata.num_row_groups):
                group = parquet.metadata.row_group(group_number)
                stats = group.column(kind_index).statistics
                # A fulltext-only row group cannot contain the no-body
                # headlines covered by this rule; skip its URL column.
                if (stats is not None and stats.has_min_max and stats.null_count == 0
                        and stats.min == 'fulltext' and stats.max == 'fulltext'):
                    fulltext_rows += group.num_rows
                    pruned_rows += group.num_rows
                    continue
                for batch in parquet.iter_batches(row_groups=[group_number],
                                                  columns=['url', 'text_kind'],
                                                  batch_size=16384, use_threads=False):
                    kinds = batch.column(batch.schema.get_field_index('text_kind'))
                    if kinds.null_count:
                        raise ValueError('news corpus article text kind is missing')
                    headline = pc.equal(kinds, 'headline')
                    full = pc.equal(kinds, 'fulltext')
                    headlines = int(pc.sum(pc.cast(headline, 'int64')).as_py() or 0)
                    fulltexts = int(pc.sum(pc.cast(full, 'int64')).as_py() or 0)
                    if headlines + fulltexts != batch.num_rows:
                        raise ValueError('news corpus article has an unknown text kind')
                    headline_rows += headlines
                    fulltext_rows += fulltexts
                    projected_rows += batch.num_rows
                    urls = pc.filter(batch.column(batch.schema.get_field_index('url')), headline)
                    if urls.null_count:
                        raise ValueError('news corpus headline URL is missing')
                    candidates = pc.or_(
                        pc.or_(pc.starts_with(urls, 'https://www.cnyes.com/twstock/'),
                               pc.starts_with(urls, 'http://www.cnyes.com/twstock/')),
                        pc.or_(pc.starts_with(urls, 'https://news.google.com/'),
                               pc.starts_with(urls, 'http://news.google.com/')))
                    for url in pc.filter(urls, candidates).to_pylist():
                        if reason := headline_metadata_only_reason(url):
                            reasons[reason] += 1
        after = path.stat()
        if signature != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError('news corpus article part changed during inspection')
        if notify and (number % 64 == 0 or number == len(parts)):
            notify(number, len(parts))
    if (manifest_path.read_bytes() != manifest_bytes
            or headline_rows + fulltext_rows != manifest['row_counts']['articles']
            or headline_rows != manifest['source_snapshot']['counts']['headline_articles']):
        raise ValueError('news corpus manifest changed or row counts disagree')
    return {
        'contract_version': 1,
        'headline_scope_contract': HEADLINE_SCOPE_CONTRACT,
        'dataset_root': str(root), 'source_root': manifest['source_snapshot']['source_root'],
        'generation': manifest['generation'],
        'manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
        'article_parts_examined': len(parts), 'headline_rows_examined': headline_rows,
        'fulltext_rows': fulltext_rows, 'projected_rows': projected_rows,
        'fulltext_rows_pruned_by_statistics': pruned_rows,
        'metadata_only_market_headlines': sum(reasons.values()),
        'metadata_only_by_reason': dict(sorted(reasons.items())),
        'scope': 'current corpus manifest headline rows under the new literal URL rule, not live queue or whole source history',
        'full_part_sha_reverified': False, 'article_identity_inferred': False,
        'source_or_corpus_writes': 0, 'publisher_requests': 0, 'body_columns_read': 0,
    }


def cnyes_unsupported_path_inventory(db, *, head_limit=4096):
    """One indexed, bounded read of unfinished heads; no completed-body scan.

    Keep URL-head counts and body presence separate. A syntax hint must not
    remove a head from any denominator, change a queue outcome or turn source
    content/rights/identity into a verified fact. Overflow remains explicit.
    """
    if type(head_limit) is not int or not 1 <= head_limit <= 4096:
        raise ValueError('unsupported path head limit must be 1..4096')
    rows = db.execute(
        "SELECT url,body_sha256 FROM articles INDEXED BY dispatch "
        "WHERE provider='cnyes' AND state='unsupported_article_path' "
        "ORDER BY due LIMIT ?", (head_limit + 1,)).fetchall()
    overflow = len(rows) > head_limit
    groups = defaultdict(lambda: {'url_heads': 0, 'missing_body_heads': 0,
                                  'stored_body_heads': 0, 'literal_url_samples': []})
    for url, body_sha256 in rows[:head_limit]:
        hint = cnyes_literal_route_hint(url)
        group = groups[hint]
        group['url_heads'] += 1
        group['stored_body_heads' if body_sha256 is not None else 'missing_body_heads'] += 1
        if len(group['literal_url_samples']) < 3:
            # This is the existing private inspection output, not a browser
            # projection. Do not retain oversized input or source prose.
            group['literal_url_samples'].append(url if hint not in {
                'invalid_or_oversized_url', 'foreign_origin_unreviewed'} else None)
    return {
        'contract_version': 1, 'provider': 'cnyes',
        'state': 'unsupported_article_path', 'scope': 'literal_unfinished_url_head_syntax_only',
        'head_limit': head_limit, 'heads_examined': min(len(rows), head_limit),
        'head_limit_reached': overflow, 'all_current_unsupported_heads_included': not overflow,
        'total_unsupported_url_heads': len(rows) if not overflow else None,
        'route_hint_counts': dict(sorted(groups.items())),
        'article_count_inferred': False, 'body_acceptance_granted': False,
        'source_absence_or_permission_inferred': False,
        'completion_denominator_changed': False, 'publisher_requests': 0,
        'production_queue_writes': 0, 'completed_body_rows_scanned': 0,
        'raw_or_body_payloads_read': 0,
    }


def cnyes_market_route_provenance(db, *, head_limit=4096):
    """Summarize bounded market-route hints and existing metadata pointers.

    A literal redirect target or a metadata title is not publisher identity,
    a publication clock, or proof that the destination is or is not an
    article. Retain every head and pointer; never follow URLs or load bodies.
    The caller should hold one read transaction for a coherent snapshot.
    """
    if type(head_limit) is not int or not 1 <= head_limit <= 4096:
        raise ValueError('unsupported path head limit must be 1..4096')
    from stockagent.data.news_source_urls import GOOGLE_LITERAL_URL_CONTRACT
    rows = db.execute(
        "SELECT substr(a.url,1,8193),substr(a.title,1,4097),a.body_sha256,"
        "EXISTS(SELECT 1 FROM source_links s WHERE s.article_id=a.id),"
        "EXISTS(SELECT 1 FROM original_url_resolutions r INDEXED BY original_url_target "
        "WHERE r.original_id=a.id AND r.contract_version=? AND r.outcome='literal_original') "
        "FROM articles a INDEXED BY dispatch WHERE a.provider='cnyes' "
        "AND a.state='unsupported_article_path' ORDER BY a.due LIMIT ?",
        (GOOGLE_LITERAL_URL_CONTRACT, head_limit + 1)).fetchall()
    overflow = len(rows) > head_limit
    paths, titles, provenance = defaultdict(int), defaultdict(int), defaultdict(int)
    missing, stored = 0, 0
    for url, title, body_sha, direct, literal in rows[:head_limit]:
        if cnyes_literal_route_hint(url) != 'market_endpoint_route_candidate':
            continue
        path = urlsplit(url).path
        if re.fullmatch(r'/twstock/[A-Za-z0-9]+/?', path):
            shape = 'symbol_overview_path'
        elif re.fullmatch(r'/twstock/[A-Za-z0-9]+/company(?:/.*)?', path):
            shape = 'company_path'
        else:
            shape = 'other_twstock_path'
        paths[shape] += 1
        if not isinstance(title, str) or not title:
            titles['missing'] += 1
        elif len(title) > 4096:
            titles['oversized'] += 1
        elif '總覽' in title:
            titles['mentions_overview'] += 1
        elif '簡介' in title:
            titles['mentions_profile'] += 1
        else:
            titles['other'] += 1
        provenance['both' if direct and literal else 'metadata_link_only' if direct
                   else 'literal_pointer_only' if literal else 'neither_recorded'] += 1
        if body_sha is None:
            missing += 1
        else:
            stored += 1
    return {
        'contract_version': 1, 'provider': 'cnyes',
        'scope': 'bounded_unsupported_heads_with_literal_twstock_route_hint',
        'head_limit': head_limit, 'heads_examined': min(len(rows), head_limit),
        'head_limit_reached': overflow,
        'all_current_unsupported_heads_included': not overflow,
        'market_route_heads_examined': missing + stored,
        'missing_body_heads': missing, 'stored_body_heads': stored,
        'literal_path_shape_counts': dict(sorted(paths.items())),
        'metadata_title_hint_counts': dict(sorted(titles.items())),
        'recorded_pointer_kind_counts': dict(sorted(provenance.items())),
        'pointer_grain': 'URL heads with at least one recorded pointer, not pointer count',
        'literal_resolution_contract_version': GOOGLE_LITERAL_URL_CONTRACT,
        'metadata_identity_verified': False, 'article_identity_inferred': False,
        'source_absence_or_permission_inferred': False, 'completion_denominator_changed': False,
        'publisher_requests': 0, 'production_queue_writes': 0,
        'completed_body_rows_scanned': 0, 'raw_or_body_payloads_read': 0,
    }


def _legacy_title_key(value):
    """Create a matching hint, never an identity or version proof."""
    if not isinstance(value, str) or len(value) > 4096:
        return ''
    value = re.split(r'\s*(?:[-|｜])\s*(?=Anue\s*鉅亨|鉅亨網)', value,
                     maxsplit=1, flags=re.IGNORECASE)[0]
    # Historical search metadata sometimes truncates only the publisher label.
    value = re.sub(r'\s*\|\s*鉅\s*(?:\.{3}|…)\s*$', '', value)
    prefix, suffix = '公告台股公告', '鉅亨網新聞中心※來源'
    if value.startswith(prefix) and value.endswith(suffix):
        value = value[len(prefix):-len(suffix)]
    value = unicodedata.normalize('NFKC', html.unescape(value)).casefold()
    return ''.join(character for character in value if not character.isspace())


def _legacy_candidate_days(url, metadata_day):
    """Use both recorded date hints, without assigning either a source clock."""
    match = re.search(r'/([0-9]{8})/', urlsplit(url).path)
    raw_days = [metadata_day]
    if match:
        raw = match.group(1)
        raw_days.append(f'{raw[:4]}-{raw[4:6]}-{raw[6:8]}')
    result = set()
    for raw in raw_days:
        try:
            if not isinstance(raw, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', raw):
                continue
            day = date.fromisoformat(raw)
            for offset in (-1, 0, 1):
                try:
                    result.add((day + timedelta(days=offset)).isoformat())
                except OverflowError:
                    pass
        except ValueError:
            continue
    return sorted(result)


def _read_verified_missing_titles(db, root, selected):
    """Read only verified title columns for a bounded set of stored heads."""
    import pyarrow.parquet as pq

    max_heads, max_encoded, max_decoded = 256, 16 << 20, 8 << 20
    selected = selected[:max_heads]
    grouped = defaultdict(list)
    for row in selected:
        grouped[row['parquet_path']].append(row)
    results, errors, encoded, decoded = [], 0, 0, 0
    for relative, heads in grouped.items():
        try:
            receipt_row = db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
            if not receipt_row:
                raise ValueError('missing_receipt')
            receipt_file = source_path(root, receipt_row[0])
            if receipt_file.stat().st_size > 65536:
                raise ValueError('receipt_too_large')
            receipt = json.loads(receipt_file.read_bytes())
            path = source_path(root, relative)
            size = path.stat().st_size
            if size > max_encoded - encoded:
                raise ValueError('encoded_budget')
            encoded += size
            if sha256_file(path) != receipt['sha256']:
                raise ValueError('batch_hash')
            parquet = pq.ParquetFile(path)
            if parquet.metadata.num_rows > 4096 or parquet.metadata.num_rows != receipt['rows']:
                raise ValueError('row_count')
            columns = {'article_id', 'title', 'body_sha256'}
            if not columns.issubset(set(parquet.schema_arrow.names)):
                raise ValueError('missing_columns')
            column_bytes = sum(
                parquet.metadata.row_group(i).column(j).total_uncompressed_size
                for i in range(parquet.metadata.num_row_groups)
                for j in range(parquet.metadata.row_group(i).num_columns)
                if parquet.metadata.row_group(i).column(j).path_in_schema in columns)
            if column_bytes > max_decoded - decoded:
                raise ValueError('decoded_budget')
            decoded += column_bytes
            records = parquet.read(columns=sorted(columns), use_threads=False).to_pylist()
            indexed = {record['article_id']: record for record in records}
            if len(indexed) != len(records):
                raise ValueError('duplicate_identity')
            for head in heads:
                record = indexed.get(head['article_id'].hex())
                if not record or record['body_sha256'] != head['declared_body_sha256']:
                    errors += 1
                    continue
                title = record['title']
                if not isinstance(title, str) or not title or len(title) > 4096:
                    errors += 1
                    continue
                results.append({**head, 'title': title, 'title_source': 'verified_parquet_title'})
        except (OSError, ValueError, KeyError, TypeError, IndexError, sqlite3.Error):
            errors += len(heads)
    return results, {'selected_heads': len(selected), 'verified_titles': len(results),
                     'unavailable_or_invalid_heads': errors, 'encoded_bytes_read': encoded,
                     'decoded_column_byte_bound': decoded, 'body_columns_decoded': 0}


def cnyes_legacy_reuse_candidates(db, *, head_limit=4096, day_limit=4096, root=None):
    """Find bounded title/date hints among already stored canonical heads.

    Each dated index range is read once and candidate titles are indexed in
    memory. This is a private, read-only review list: it never follows a URL,
    reads a body, queues a request, accepts an alias, or changes a denominator.
    A match cannot establish same-version identity; capped or missing date
    ranges and non-complete states are explicitly outside its search scope.
    """
    if type(head_limit) is not int or not 1 <= head_limit <= 4096:
        raise ValueError('unsupported path head limit must be 1..4096')
    if type(day_limit) is not int or not 1 <= day_limit <= 4096:
        raise ValueError('candidate day limit must be 1..4096')
    heads = db.execute(
        "SELECT url,title,metadata_date_raw,metadata_order_day FROM articles INDEXED BY dispatch "
        "WHERE provider='cnyes' AND state='unsupported_article_path' AND body_sha256 IS NULL "
        "ORDER BY due LIMIT ?", (head_limit + 1,)).fetchall()
    overflow = len(heads) > head_limit
    legacy = []
    title_keys = set()
    days = set()
    for url, title, metadata_raw, metadata_day in heads[:head_limit]:
        if cnyes_literal_route_hint(url) != 'legacy_shtml_article_route_candidate':
            continue
        title_key = _legacy_title_key(title)
        candidate_days = _legacy_candidate_days(url, metadata_day)
        # A short generic title would create misleading, high-collision hints.
        if len(title_key) < 4:
            title_key = ''
        if title_key:
            title_keys.add(title_key)
            days.update(candidate_days)
        legacy.append((url, title, metadata_raw, metadata_day, title_key, candidate_days))

    by_day_and_title = defaultdict(list)
    capped_days = set()
    scanned_rows = 0
    missing_title_heads = []
    examined_days, unsearched_days = [], []
    max_days, max_total_rows = 512, 262144
    for day in sorted(days):
        if len(examined_days) >= max_days or scanned_rows >= max_total_rows:
            unsearched_days.append(day)
            continue
        examined_days.append(day)
        current_limit = min(day_limit, max_total_rows - scanned_rows)
        rows = db.execute(
            "SELECT url,title,metadata_date_raw,metadata_order_day,body_sha256,id,parquet_path "
            "FROM articles INDEXED BY dispatch_latest WHERE provider='cnyes' "
            "AND state='complete' AND metadata_order_day=? LIMIT ?", (day, current_limit + 1)).fetchall()
        if len(rows) > current_limit:
            capped_days.add(day)
        scanned_rows += min(len(rows), current_limit)
        for url, title, raw, order_day, body_sha, article_id, parquet_path in rows[:current_limit]:
            if not isinstance(url, str) or not re.fullmatch(r'https://news\.cnyes\.com/news/id/[0-9]+', url):
                continue
            if not isinstance(body_sha, str) or not re.fullmatch(r'[0-9a-f]{64}', body_sha):
                continue
            title_key = _legacy_title_key(title)
            if title_key in title_keys:
                by_day_and_title[day, title_key].append({
                    'canonical_url': url, 'title': title, 'metadata_date_raw': raw,
                    'metadata_order_day': order_day, 'declared_body_sha256': body_sha,
                    'title_source': 'queue_metadata',
                })
            elif (root is not None and not title and isinstance(article_id, bytes)
                  and isinstance(parquet_path, str) and parquet_path):
                missing_title_heads.append({'canonical_url': url, 'metadata_date_raw': raw,
                    'metadata_order_day': order_day, 'declared_body_sha256': body_sha,
                    'article_id': article_id, 'parquet_path': parquet_path})

    fallback = {'selected_heads': 0, 'verified_titles': 0, 'unavailable_or_invalid_heads': 0,
                'encoded_bytes_read': 0, 'decoded_column_byte_bound': 0, 'body_columns_decoded': 0}
    if root is not None:
        recovered, fallback = _read_verified_missing_titles(db, root, missing_title_heads)
        for candidate in recovered:
            title_key = _legacy_title_key(candidate['title'])
            if title_key in title_keys:
                candidate.pop('article_id')
                candidate.pop('parquet_path')
                by_day_and_title[candidate['metadata_order_day'], title_key].append(candidate)
    fallback.update(requested=root is not None, head_limit=256,
                    head_limit_reached=len(missing_title_heads) > 256)

    items = []
    for url, title, metadata_raw, metadata_day, title_key, candidate_days in legacy:
        parts = urlsplit(url)
        found = {candidate['canonical_url']: candidate
                 for day in candidate_days
                 for candidate in by_day_and_title.get((day, title_key), [])}
        candidates = [found[key] for key in sorted(found)]
        items.append({
            'legacy_url_sha256': hashlib.sha256(url.encode()).hexdigest(),
            # Query values are intentionally omitted, including tracking or credentials.
            'legacy_origin_path': f'{parts.scheme}://{parts.netloc}{parts.path}',
            'legacy_query_present': bool(parts.query),
            'legacy_title': title, 'legacy_metadata_date_raw': metadata_raw,
            'legacy_metadata_order_day': metadata_day, 'candidate_days': candidate_days,
            'title_eligible_for_matching': bool(title_key),
            'candidate_days_capped': sorted(set(candidate_days) & capped_days),
            'candidate_days_unsearched': sorted(set(candidate_days) & set(unsearched_days)),
            'candidate_count': len(candidates), 'candidates': candidates,
            'same_article_or_version_verified': False,
            'old_url_body_present': False,
        })
    return {
        'contract_version': 1, 'scope': 'missing_legacy_shtml_heads_vs_complete_canonical_title_and_date_hints',
        'head_limit': head_limit, 'heads_examined': min(len(heads), head_limit),
        'head_limit_reached': overflow, 'legacy_heads_examined': len(items),
        'day_limit': day_limit, 'days_examined': len(examined_days), 'capped_days': sorted(capped_days),
        'max_days': max_days, 'max_total_candidate_rows': max_total_rows, 'unsearched_days': unsearched_days,
        'complete_head_rows_examined': scanned_rows,
        'legacy_heads_with_candidates': sum(item['candidate_count'] > 0 for item in items),
        'legacy_heads_with_multiple_candidates': sum(item['candidate_count'] > 1 for item in items),
        'legacy_heads_without_candidates': sum(item['candidate_count'] == 0 for item in items),
        'candidate_matching_is_identity_proof': False, 'absence_proved': False,
        'body_integrity_independently_verified': False, 'completion_denominator_changed': False,
        'missing_title_fallback': fallback,
        'publisher_requests': 0, 'production_queue_writes': 0, 'raw_or_body_payloads_decoded': 0,
        'items': items,
    }


def headline_comparison_metadata(visible, current):
    """Explain a title mismatch without persisting either title or body.

    These are diagnostic comparisons, not new acceptance/retention rules.
    In particular, a whitespace/entity comparison never authorizes raw bytes
    when the existing exact public-scope proof fails.
    """
    if not isinstance(visible, str) or not isinstance(current, str):
        return {'string_pair': False}
    compact = lambda value: re.sub(r'\s+', '', value)
    first = next((i for i, pair in enumerate(zip(visible, current)) if pair[0] != pair[1]),
                 min(len(visible), len(current)) if visible != current else None)
    return {'string_pair': True,
            'visible_characters': len(visible), 'current_characters': len(current),
            'visible_sha256': hashlib.sha256(visible.encode()).hexdigest(),
            'current_sha256': hashlib.sha256(current.encode()).hexdigest(),
            'exact_match': visible == current,
            'whitespace_equivalent': compact(visible) == compact(current),
            'entity_decoded_equivalent': visible == html.unescape(current),
            'first_different_character_offset': first,
            'first_different_codepoints': {
                key: f'U+{ord(value[first]):04X}' if first is not None and first < len(value) else None
                for key, value in (('visible', visible), ('current', current))}}


def cnyes_document_representation_shape(soup, source, *, article_id):
    """Explain missing native containers without exporting source prose.

    This is a bounded diagnostic, not an identity, visibility or permission
    witness. Field types/lengths are useful even when no body can be retained;
    never include values, bylines, URLs, scripts or schema excerpts here.
    """
    from downloader.news_fulltext_publishers import _cnyes_current_article_data

    def shape(value):
        result = {'type': type(value).__name__}
        if isinstance(value, str):
            result['characters'] = len(value)
        return result

    data_error = False
    try:
        data = _cnyes_current_article_data(soup, article_id)
    except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        data, data_error = None, True
    result = {'contract_version': 2, 'current_data_present': data is not None,
              'current_shape_malformed': data_error,
              'current_field_shapes': {key: shape(data.get(key)) for key in
                  ('title', 'content', 'payment', 'publishAt', 'url', 'author')} if data is not None else {},
              'current_payment_zero': bool(data is not None and type(data.get('payment')) is int
                                           and data['payment'] == 0),
              'matching_schema_shapes': [], 'schema_documents_examined': 0,
              'schema_objects_examined': 0, 'schema_invalid_documents': 0,
              'bounded_schema_scan': True, 'source_text_exported': False,
              'raw_scope_or_article_acceptance_granted': False}
    containers = soup.find_all(id='article-container', limit=5)
    result['native_container_limit_reached'] = len(containers) > 4
    result['native_container_shapes'] = []
    for container in containers[:4]:
        parent = container.find_parent('article')
        headers = parent.find_all('h1', limit=5) if parent is not None else []
        result['native_container_shapes'].append({
            'tag': container.name, 'has_article_parent': parent is not None,
            'article_header_count_capped_at_five': len(headers),
            'article_header_limit_reached': len(headers) > 4,
            'article_header_shapes': [{'tag': header.name, 'characters': len(header.get_text(' ', strip=True)),
                'matches_current_headline': bool(data is not None and isinstance(data.get('title'), str)
                    and headline_comparison_metadata(header.get_text(' ', strip=True), data['title'])['whitespace_equivalent'])}
                for header in headers[:4]],
            'ancestor_tags': [node.name for node in islice(container.parents, 8) if node.name],
        })
    scripts = soup.find_all('script', type='application/ld+json', limit=17)
    result['schema_document_limit_reached'] = len(scripts) > 16
    for script in scripts[:16]:
        result['schema_documents_examined'] += 1
        raw = script.get_text()
        if len(raw) > 1 << 20:
            result['schema_invalid_documents'] += 1
            continue
        try:
            nodes = list(islice(_objects(json.loads(raw)), 65))
        except (ValueError, TypeError, RecursionError):
            result['schema_invalid_documents'] += 1
            continue
        result['schema_objects_examined'] += min(len(nodes), 64)
        if len(nodes) > 64:
            result['schema_object_limit_reached'] = True
        for node in nodes[:64]:
            if node.get('@type') != 'NewsArticle':
                continue
            # A declared same-ID URL is diagnostic association only. The
            # canonical/native/current/free/author/clock proof below remains
            # wholly separate and still controls raw retention.
            try:
                parts = urlsplit(node.get('url', '')) if isinstance(node.get('url'), str) else None
            except ValueError:
                continue
            if (parts is None or parts.scheme != 'https' or parts.netloc not in
                    {'news.cnyes.com', 'm.cnyes.com'} or parts.path != f'/news/id/{article_id}'):
                continue
            if len(result['matching_schema_shapes']) == 4:
                result['schema_shape_limit_reached'] = True
                continue
            result['matching_schema_shapes'].append({key: shape(node.get(key)) for key in
                ('headline', 'articleBody', 'author', 'datePublished', 'isAccessibleForFree')})
    return result


def cnyes_html_structure(soup, source, *, page_url):
    """Bounded identity/clock/body comparison metadata, never body acceptance.

    Free current-ID declarations can permit private diagnosis even when the
    three body representations disagree. Missing/conflicting/hidden/paid
    evidence permits metadata only. No script execution, alternate requests,
    raw author/body text, credentials or inferred publication timestamps.
    """
    from downloader.news_fulltext_publishers import (
        _cnyes_completed_stream_wrapper, _cnyes_current_article_data,
        _cnyes_visible_after_completed_stream, _cnyes_visible_article_scope,
        cnyes_visible_headline_matches,
    )

    result = {"contract_version": 5, "raw_public_scope": False,
              "article_containers": len(soup.select('#article-container'))}
    parts = urlsplit(page_url)
    match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
    if (source.provider != 'cnyes' or not match or parts.scheme != 'https'
            or parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}):
        return result
    result['document_representation_shape'] = cnyes_document_representation_shape(
        soup, source, article_id=int(match[1]))
    canonical = soup.select('link[rel="canonical"][href]')
    result['canonical_count'] = len(canonical)
    scope = _cnyes_visible_article_scope(soup)
    result['unambiguous_native_scope'] = scope is not None
    if len(canonical) != 1 or scope is None:
        return result
    try:
        declared_url = normalize_url(canonical[0]['href'])
        declared_parts = urlsplit(declared_url)
        result['canonical_path_matches_request'] = declared_parts.path == parts.path
        result['canonical_public_origin'] = declared_parts.scheme == 'https' and declared_parts.netloc in {
            'news.cnyes.com', 'm.cnyes.com'}
        result['canonical_origin_kind'] = ({'news.cnyes.com': 'desktop_news', 'm.cnyes.com': 'mobile_news'}
                                           .get(declared_parts.netloc, 'other'))
        og = soup.select('meta[property="og:url"][content]')
        result['og_url_count'] = len(og)
        result['og_url_matches_canonical'] = len(og) == 1 and normalize_url(og[0]['content']) == declared_url
        if (declared_parts.scheme != 'https' or declared_parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}
                or declared_parts.path != parts.path or declared_parts.query or declared_parts.fragment):
            return result
        header, container = scope
        reveal = _cnyes_completed_stream_wrapper(soup, container.find_parent('article'))
        result['completed_native_stream'] = reveal is not None
        try:
            for node in (header, container):
                _cnyes_visible_after_completed_stream(node, reveal)
            result['native_scope_visible'] = True
        except NewsError:
            result['native_scope_visible'] = False
        declarations = []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                declarations.extend(node for node in _objects(json.loads(script.get_text()))
                                    if node.get('@type') == 'NewsArticle' and node.get('url') == declared_url)
            except (ValueError, TypeError, RecursionError):
                result['invalid_schema_declaration'] = True
                return result
        result['current_schema_count'] = len(declarations)
        if len(declarations) == 1:
            entity = declarations[0].get('mainEntityOfPage')
            entity = entity.get('@id') if isinstance(entity, dict) else entity
            result['schema_main_entity_matches_canonical'] = entity == declared_url
        data = _cnyes_current_article_data(soup, int(match[1]))
        result['current_data_present'] = data is not None
        result['current_author_unambiguous'] = _cnyes_current_article_data(
            soup, int(match[1]), verify_author=True) is not None
        if data is None:
            return result
        result['current_field_types'] = {key: type(data.get(key)).__name__
                                         for key in ('title', 'content', 'payment', 'publishAt', 'url', 'author')}
        result['current_payment_zero'] = type(data.get('payment')) is int and data['payment'] == 0
        result['current_url_matches_canonical'] = normalize_url(data.get('url', '')) == declared_url
        visible_title = header.get_text(' ', strip=True)
        result['visible_headline_matches_current'] = cnyes_visible_headline_matches(visible_title, data.get('title'))
        result['headline_comparison'] = headline_comparison_metadata(visible_title, data.get('title'))
        if len(declarations) != 1:
            return result
        declared = declarations[0]
        clock = datetime.fromisoformat(declared.get('datePublished', '').replace('Z', '+00:00'))
        result['declared_clock_raw'] = declared.get('datePublished')
        result['declared_clock_matches_current'] = (clock.tzinfo is not None
            and type(data.get('publishAt')) is int and clock.timestamp() == data['publishAt'])
        result['declared_headline_matches_current'] = declared.get('headline') == data.get('title')
        result['schema_declares_free'] = str(declared.get('isAccessibleForFree', 'true')).lower() == 'true'
        result['raw_public_scope'] = all(result.get(key, False) for key in (
            'native_scope_visible', 'current_payment_zero', 'current_url_matches_canonical',
            'visible_headline_matches_current', 'declared_clock_matches_current',
            'declared_headline_matches_current', 'schema_declares_free')) and bool(data.get('title'))
        if not result['raw_public_scope']:
            return result
        texts = {'visible': _plain_text_node(container,
            (*source.exclude_selectors, '[id^="div-gpt-ad-cnyes_news_article_"]'), parser=source.html_parser)}
        for label, value in (('current', data.get('content')), ('schema', declared.get('articleBody'))):
            if isinstance(value, str):
                texts[label] = _plain_text(html.unescape(value), parser=source.html_parser)
        result['body_representations'] = {label: {'characters': len(text),
            'whitespace_normalized_sha256': hashlib.sha256(re.sub(r'\s+', '', text).encode()).hexdigest()}
            for label, text in texts.items()}
    except (NewsError, ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        result['raw_public_scope'] = False
        result['identity_or_clock_malformed'] = True
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/news_fulltext.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe", action="append", default=[])
    parser.add_argument("--canonical-fetch", action="store_true", help="Probe the actual article entrypoint with its existing queue row; retain its received response, no queue writes")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--unsupported-paths-only", action="store_true", help="Read only bounded unfinished Cnyes URL syntax; no status/service scan, HTTP or source-content determination")
    modes.add_argument("--legacy-reuse-candidates-only", action="store_true", help="Find bounded title/date hints for missing old URLs among already stored canonical heads; do not accept aliases")
    modes.add_argument("--market-route-provenance-only", action="store_true", help="Summarize bounded market-route hints and recorded metadata pointers without requests or queue writes")
    modes.add_argument("--corpus-market-heads-only", action="store_true", help="Count this literal market-route headline rule in one existing corpus; read only URL/text-kind columns")
    parser.add_argument("--corpus-root", type=Path, default=Path("data_news"))
    parser.add_argument("--unsupported-head-limit", type=int, default=4096, help="Maximum unfinished route heads to inspect (1..4096); overflow is unknown, not complete")
    parser.add_argument("--legacy-day-limit", type=int, default=4096, help="Maximum completed heads to inspect per candidate date (1..4096)")
    args = parser.parse_args(argv)
    if len(args.probe) > 8:
        parser.error("at most eight explicitly selected probes; no saturation test")
    if not 1 <= args.unsupported_head_limit <= 4096:
        parser.error("unsupported head limit must be 1..4096")
    if not 1 <= args.legacy_day_limit <= 4096:
        parser.error("candidate day limit must be 1..4096")
    if (args.unsupported_paths_only or args.legacy_reuse_candidates_only or args.market_route_provenance_only or args.corpus_market_heads_only) and (args.probe or args.canonical_fetch):
        parser.error("read-only inventory modes cannot request publisher probes")
    if args.corpus_market_heads_only:
        corpus_root = args.corpus_root.resolve()
        preliminary = json.loads((corpus_root / 'manifest.json').read_bytes())
        source_root = Path(preliminary['source_snapshot']['source_root']).resolve()
        output = args.output.resolve()
        if output.is_relative_to(corpus_root) or output.is_relative_to(source_root):
            parser.error('corpus audit output must be outside the corpus and collector roots')
        result = cnyes_corpus_market_head_audit(corpus_root,
            notify=lambda done, total: print(json.dumps({'parts_examined': done, 'parts_total': total}), flush=True))
        if output.is_relative_to(Path(result['source_root']).resolve()):
            parser.error('corpus source root changed during inspection')
        result['observed_at_utc'] = utc_now()
        args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_write_json(args.output, result)
        print(json.dumps({'output': str(args.output), 'headline_rows_examined': result['headline_rows_examined'],
                          'metadata_only_market_headlines': result['metadata_only_market_headlines'],
                          'publisher_requests': 0}))
        return 0
    config, registry, repo = load_config(args.config)
    root = repo / config["root"]
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if args.unsupported_paths_only or args.legacy_reuse_candidates_only or args.market_route_provenance_only:
        db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
        try:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')  # One coherent read while the canonical owner runs.
            if args.legacy_reuse_candidates_only:
                result = cnyes_legacy_reuse_candidates(db, head_limit=args.unsupported_head_limit,
                                                       day_limit=args.legacy_day_limit, root=root)
            elif args.market_route_provenance_only:
                result = cnyes_market_route_provenance(db, head_limit=args.unsupported_head_limit)
            else:
                result = cnyes_unsupported_path_inventory(db, head_limit=args.unsupported_head_limit)
        finally:
            db.close()
        result['observed_at_utc'] = utc_now()
        atomic_write_json(args.output, result)
        summary = {'output': str(args.output), 'heads_examined': result['heads_examined'],
                   'head_limit_reached': result['head_limit_reached'], 'publisher_requests': 0}
        if args.legacy_reuse_candidates_only:
            summary.update(legacy_heads_examined=result['legacy_heads_examined'],
                           legacy_heads_with_candidates=result['legacy_heads_with_candidates'],
                           capped_days=len(result['capped_days']))
        elif args.market_route_provenance_only:
            summary['market_route_heads_examined'] = result['market_route_heads_examined']
        print(json.dumps(summary))
        return 0
    status = json.loads((root / "status.json").read_text())
    db = sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        states = [dict(r) for r in db.execute("SELECT * FROM state_metrics WHERE provider='cnyes'")]
        unsupported_paths = cnyes_unsupported_path_inventory(db, head_limit=args.unsupported_head_limit)
        failures = {state: [dict(r) for r in db.execute(
            "SELECT url,attempts,due,error_code,metadata_date_raw FROM articles WHERE provider='cnyes' AND state=? LIMIT 12", (state,))]
            for state in ("unsupported_article_path", "body_not_found", "retry", "retry_cooldown", "article_attribution_not_verified")}
        probe_rows = {url: dict(row) for url in args.probe
                      if (row := db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())}
    finally:
        db.close()
    units = subprocess.check_output(["systemctl", "list-units", "--all", "--no-legend", "--plain", "stockagent-*.service"], text=True)
    services = {}
    for line in units.splitlines():
        unit = line.split()[0]
        fields = subprocess.check_output(["systemctl", "show", unit, "-p", "MainPID", "-p", "ActiveState", "-p", "ExecMainStartTimestampMonotonic"], text=True)
        services[unit] = dict(line.split("=", 1) for line in fields.splitlines())
    result = {"observed_at_utc": utc_now(), "services": services, "status": status, "states": states,
              "unsupported_path_inventory": unsupported_paths,
              "failure_samples": failures, "probes": [], "official_numeric_rate_limit": None, "all_history_complete": False,
              "canonical_fetch_entrypoint_used": args.canonical_fetch}
    atomic_write_json(args.output, result)
    if not args.probe:
        # Inventory is genuinely read-only: no HTTP owner, persisted limiter,
        # robots refresh or publisher cooldown rewrite while production runs.
        print(json.dumps({"output": str(args.output), "probes": 0, "states": states}, ensure_ascii=False))
        return 0
    for url in args.probe:
        if registry.require(url).provider != 'cnyes':
            raise ValueError('only explicitly authorized cnyes URLs')
        if args.canonical_fetch and url not in probe_rows:
            raise ValueError('canonical_fetch_probe_requires_existing_queue_row')
    try:
        http = NewsHTTP(registry, config, root)
    except NewsError as error:
        if error.code != 'news_http_owner_already_running':
            raise
        # The live canonical owner keeps fetching under the shared limiter.
        # A diagnostic must not steal its lock, turn exclusivity off, stop it
        # or send duplicate requests. Preserve the inspection snapshot and
        # explicitly defer this optional probe instead of crashing or calling
        # the publisher unavailable. No requested URL is requeued here.
        result.update(diagnostic_status_contract_version=1,
                      state='deferred_active_canonical_http_owner',
                      reason=error.code, explicit_selected_urls=list(args.probe),
                      publisher_requests=0, source_states_mutated=False,
                      raw_response_retained=False, original_source_integrity_verified=False)
        atomic_write_json(args.output, result)
        print(json.dumps({'output': str(args.output), 'state': result['state'],
                          'probes': 0, 'publisher_requests': 0}, ensure_ascii=False))
        return 0
    try:
        for number, url in enumerate(args.probe):
            source = registry.require(url)
            if source.provider != "cnyes":
                raise ValueError("only explicitly authorized cnyes URLs")
            started = time.monotonic()
            probe = {"url": url}
            try:
                response, article, extraction_error = None, None, None
                if args.canonical_fetch:
                    if url not in probe_rows:
                        raise ValueError('canonical_fetch_probe_requires_existing_queue_row')
                    original_get = http.get

                    def capture_get(*get_args, **get_kwargs):
                        nonlocal response
                        response = original_get(*get_args, **get_kwargs)
                        return response

                    http.get = capture_get
                    try:
                        article = fetch_article(http, probe_rows[url])
                    except NewsError as error:
                        if response is None:
                            raise
                        extraction_error = error.code
                    finally:
                        http.get = original_get
                else:
                    response = http.get(url)
                probe.update(status=response.status, bytes=len(response.body),
                             headers={k: v for k, v in response.headers.items() if k in ACCESS_DIAGNOSTIC_HEADERS},
                             response_sha256=hashlib.sha256(response.body).hexdigest())
                retain_public_response = False
                if "json" in response.headers.get("content-type", ""):
                    from downloader.news_fulltext_bulk import cnyes_pagination, wordpress_base
                    payload = json.loads(response.body)
                    items = payload.get("items", {})
                    rows = items.get("data", []) if isinstance(items, dict) else []
                    probe["json_structure"] = {"keys": sorted(payload), "rows": len(rows),
                        "item_keys": sorted(rows[0]) if rows else [],
                        "content_lengths": [len(str(r.get("content", ""))) for r in rows[:8]],
                        "total": items.get("total") if isinstance(items, dict) else None}
                    if response.status == 200 and wordpress_base(response.url) in source.cnyes_apis:
                        _, posts = cnyes_pagination(response)
                        retain_public_response = bool(posts) and all(
                            type(post.get('payment')) is int and post['payment'] == 0 for post in posts)
                elif response.status == 200 and "html" in response.headers.get("content-type", ""):
                    soup = BeautifulSoup(response.body, "lxml")
                    probe['cnyes_structure'] = cnyes_html_structure(soup, source, page_url=response.url)
                    canonical = soup.select_one('link[rel="canonical"]')
                    probe["canonical_url"] = canonical.get("href") if canonical else None
                    probe["script_sources"] = [s.get("src") for s in soup.select("script[src]")][:30]
                    try:
                        if args.canonical_fetch:
                            if extraction_error:
                                raise NewsError(extraction_error)
                            if article is None:
                                raise NewsError('diagnostic_not_modified_without_body')
                        else:
                            article = extract_article(response.body, source, page_url=response.url)
                        probe["article"] = {k: article.get(k) for k in ("body_sha256", "extraction_method", "body_completeness", "attribution_status", "published_at_raw")}
                        probe["body_characters"] = len(article["body"])
                        retain_public_response = True
                    except NewsError as error:
                        probe["extraction_error"] = error.code
                        # Failed public text/identity diagnosis is not article
                        # acceptance. Restriction, access, rights and unknown
                        # failures never permit retaining a response body.
                        retain_public_response = probe['cnyes_structure']['raw_public_scope'] and error.code in {
                            'body_not_found', 'source_article_empty_template',
                            'source_not_found', 'source_body_image_only', 'source_body_quote_only', 'source_body_removed'}
                if retain_public_response:
                    raw_path = args.output.parent / (args.output.stem + f"-probe-{number}.raw")
                    atomic_write_bytes(raw_path, response.body, durable=True)
                    probe['private_raw_path'] = str(raw_path)
                probe['raw_response_retained'] = retain_public_response
            except NewsError as error:
                probe.update(error=error.code, retry=error.retry, delay=error.delay)
            probe["wall_seconds"] = time.monotonic() - started
            result["probes"].append(probe)
            result["probe_performance"] = http.metrics.snapshot()
            atomic_write_json(args.output, result)
            print(json.dumps({k: v for k, v in probe.items() if k not in {"headers", "script_sources"}}, ensure_ascii=False), flush=True)
            if http.provider_not_before.get("cnyes", 0) > time.time():
                break  # Do not probe through a source refusal/outage.
    finally:
        http.close()
    print(json.dumps({"output": str(args.output), "probes": len(result["probes"]), "states": states}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
