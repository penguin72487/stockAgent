#!/usr/bin/env python3
"""Resumable official announcement/rule archive, separate from executable rules.

Reuse the canonical TAIFEX workspace, shared limiter and atomic writers. Index
all announcement types: old contract adjustments were not consistently tagged.
An archived document is NOT proof of complete numeric/PIT rule reconstruction.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import fcntl
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
import zlib
from urllib.parse import urljoin, urlsplit, urlunsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bs4 import BeautifulSoup
import pyarrow as pa
import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet
from downloader.common import SharedRateLimiter, parse_retry_after_seconds
from scripts.taifex_daily_download_common import sha256_path


BASE = 'https://www.taifex.com.tw'
INDEX_URL = BASE + '/cht/11/hisNews'
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_COMPRESSED_BYTES = MAX_RESPONSE_BYTES + 1024 * 1024
SEEDS = (
    ('contract_adjustments', '契約調整近期表', BASE + '/cht/4/contractAdj'),
    ('position_limits_non_equity', '非個股部位限額', BASE + '/cht/4/traderPLNonEquity'),
    ('position_limits_equity', '個股部位限額', BASE + '/cht/4/traderPLEquity'),
    ('contract_specs', '全商品契約規格', BASE + '/cht/2/sTF'),
    ('margins', '歷史保證金查詢官方說明', BASE + '/cht/9/tradersQAClearing'),
    ('contract_adjustments_csv', '契約調整近期下載表', BASE + '/cht/4/contractAdjDown'),
    ('position_limits_equity_csv', '個股部位限額下載表', BASE + '/cht/4/traderPLEquityDown'),
    ('large_trader_history_definition', '大額交易人資料起日與欄位定義', BASE + '/cht/3/largeTraderOptQryDetail'),
    ('surveillance_history', '監視制度沿革與歷史資料首次發布日', BASE + '/cht/1/historyOfSurveillance'),
)
ATTACHMENT = re.compile(r'\.(?:pdf|csv|xls|xlsx|ods|odt|doc|docx|zip|txt)(?:$|\?)', re.I)
DATE = re.compile(r'^((?:19|20)\d{2})[/-](\d{1,2})[/-](\d{1,2})$')


class ProviderBlockedError(ValueError):
    """A shared provider deferral, not thousands of independent failed URLs."""


class ArchiveIntegrityError(ValueError):
    """Local immutable evidence failed validation; HTTP must not hide the failure."""


def is_provider_deferral(error: Exception) -> bool:
    """Keep HTTPError callers compatible while stopping provider-wide batches."""
    return isinstance(error, ProviderBlockedError) or (
        isinstance(error, requests.HTTPError)
        and getattr(error.response, 'status_code', None) == 429
    )


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def read_provider_cooldown(root: Path) -> dict | None:
    path = root / 'state' / 'provider_cooldown.json'
    if not path.exists():
        return None
    cooldown = json.loads(path.read_text(encoding='utf-8'))
    deadline = datetime.fromisoformat(cooldown['deadline_utc'])
    if deadline.tzinfo is None:
        raise ValueError('provider_cooldown_requires_utc_offset')
    return cooldown


def provider_cooldown_active(cooldown: dict | None) -> bool:
    return cooldown is not None and datetime.fromisoformat(cooldown['deadline_utc']) > datetime.now(UTC)


def record_provider_cooldown(root: Path, error: Exception) -> datetime:
    """Persist provider authority across timer processes, without shortening it."""
    now = datetime.now(UTC)
    response = getattr(error, 'response', None)
    retry_after = getattr(response, 'headers', {}).get('Retry-After')
    minimum = 600 if isinstance(error, ProviderBlockedError) else 60
    delay = max(minimum, parse_retry_after_seconds(retry_after, now=now) or 0)
    deadline = now + timedelta(seconds=delay)
    previous = read_provider_cooldown(root)
    if previous is not None:
        deadline = max(deadline, datetime.fromisoformat(previous['deadline_utc']))
    atomic_write_json(root / 'state' / 'provider_cooldown.json', {
        'schema_version': 1, 'observed_at_utc': now.isoformat(),
        'deadline_utc': deadline.astimezone(UTC).isoformat(),
        'error_type': type(error).__name__, 'reason': str(error)[:220],
        'http_status': getattr(response, 'status_code', None),
        'retry_after': retry_after,
    })
    return deadline


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def official_url(value: str, base: str = BASE) -> str | None:
    try:
        url = urlsplit(urljoin(base, value.strip()))
        port = url.port
    except ValueError:
        return None
    if url.scheme not in {'http', 'https'} or url.hostname != 'www.taifex.com.tw' or url.username or url.password:
        return None
    if port not in (None, 80, 443):
        return None
    return urlunsplit(('https', 'www.taifex.com.tw', url.path, url.query, ''))


def topic(title: str) -> str:
    for name, words in (
        ('margins', ('保證金', '擔保品', 'SPAN')),
        ('position_limits', ('部位', '限額')),
        ('contract_adjustments', ('契約調整', '除權', '除息', '減資', '合併')),
        ('contract_specs', ('漲跌', '升降', '契約規格', '交易規則', '升降單位', '價格穩定')),
        ('settlement', ('結算',)),
        ('calendar', ('休市', '交易日', '交易時間')),
    ):
        if any(word in title for word in words):
            return name
    return 'other_announcements'


def parse_index(content: bytes, start: date, end: date) -> list[dict]:
    soup = BeautifulSoup(content, 'html.parser')
    content_node = soup.select_one('#content')
    if content_node is None:
        raise ValueError('announcement_content_missing')
    tables = [table for table in content_node.select('table')
              if '日期' in table.get_text() and '標題' in table.get_text()]
    if not tables:
        # An empty result must be an explicit official no-data response, never
        # an HTTP-200 WAF/login/error template.
        if any(label in content_node.get_text() for label in ('查無資料', '無符合條件', '沒有符合')):
            return []
        raise ValueError('announcement_table_missing')
    rows = {}
    for tr in tables[0].select('tr'):
        cells = tr.find_all('td', recursive=False)
        if not cells:
            continue
        if len(cells) < 2:
            if '查無' in tr.get_text():
                continue
            raise ValueError('unexpected_announcement_row')
        match = DATE.fullmatch(cells[0].get_text(strip=True))
        link = cells[1].find('a', href=True)
        if not match or link is None:
            raise ValueError('invalid_announcement_date_or_link')
        published = date(*map(int, match.groups()))
        if not start <= published <= end:
            raise ValueError('server_ignored_requested_date_range')
        title = link.get_text(' ', strip=True)
        original_url = urljoin(INDEX_URL, link['href'])
        url = official_url(original_url)
        identity = digest(f'{published}|{title}|{original_url}'.encode())
        rows[identity] = {'id': identity, 'published_date': str(published), 'title': title,
                          'url': url or original_url, 'category': topic(title),
                          'download_allowed': bool(url), 'published_at': None,
                          'publication_precision': 'date_only'}
    return list(rows.values())


def document_links(content: bytes, url: str, *, discover_specs: bool = False) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(content, 'html.parser')
    node = soup.select_one('#content') or soup
    for legacy in node.select('.myContent,#news_content'):
        decoded = legacy.get_text()
        if re.search(r'<(?:p|br|a|table|div|span)\b', decoded, re.I):
            legacy.clear()
            legacy.append(BeautifulSoup(decoded, 'html.parser'))
    links = {}
    for link in node.find_all('a', href=True):
        target = official_url(link['href'], url)
        if not target:
            continue
        path = urlsplit(target).path
        if ATTACHMENT.search(target):
            kind = 'attachment'
        elif discover_specs and path.startswith('/cht/2/') and re.fullmatch(r'/cht/2/[a-zA-Z0-9]+', path):
            kind = 'specification'
        else:
            continue
        links[target] = (target, link.get_text(' ', strip=True), kind)
    return list(links.values())


def open_queue(root: Path) -> sqlite3.Connection:
    state = root / 'state'
    state.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(state / 'queue.sqlite3', timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS index_windows (
            key TEXT PRIMARY KEY, start TEXT, end TEXT, checked_at TEXT,
            rows INTEGER, raw_path TEXT, raw_sha256 TEXT);
        CREATE TABLE IF NOT EXISTS announcements (
            id TEXT PRIMARY KEY, published_date TEXT, title TEXT, url TEXT,
            category TEXT, download_allowed INTEGER, publication_precision TEXT);
        CREATE INDEX IF NOT EXISTS announcements_url ON announcements(url);
        CREATE TABLE IF NOT EXISTS documents (
            url TEXT PRIMARY KEY, kind TEXT, source_id TEXT, priority INTEGER,
            state TEXT DEFAULT 'pending', checked_at TEXT, next_retry TEXT,
            attempts INTEGER DEFAULT 0, raw_path TEXT, raw_sha256 TEXT,
            content_sha256 TEXT, parsed_path TEXT, parsing_status TEXT,
            table_rows INTEGER DEFAULT 0, temporal_rows INTEGER DEFAULT 0,
            http_status INTEGER, error TEXT);
        CREATE INDEX IF NOT EXISTS documents_due ON documents(state,next_retry,priority);
        CREATE TABLE IF NOT EXISTS links (
            parent TEXT, child TEXT, label TEXT, PRIMARY KEY(parent,child));
        CREATE TABLE IF NOT EXISTS versions (
            url TEXT, content_sha256 TEXT, captured_at TEXT, raw_path TEXT, raw_sha256 TEXT,
            parsed_path TEXT, PRIMARY KEY(url,content_sha256));
        CREATE TABLE IF NOT EXISTS parse_versions (
            url TEXT, content_sha256 TEXT, parser_version INTEGER, parsed_at TEXT,
            captured_at TEXT, parsed_path TEXT, parsed_sha256 TEXT, shards_json TEXT,
            parsing_status TEXT, table_rows INTEGER, temporal_rows INTEGER,
            raw_path TEXT, raw_sha256 TEXT,
            PRIMARY KEY(url,content_sha256,parser_version));
        CREATE TRIGGER IF NOT EXISTS parse_versions_no_update
            BEFORE UPDATE ON parse_versions BEGIN SELECT RAISE(ABORT,'parse_versions_append_only'); END;
        CREATE TRIGGER IF NOT EXISTS parse_versions_no_delete
            BEFORE DELETE ON parse_versions BEGIN SELECT RAISE(ABORT,'parse_versions_append_only'); END;
    ''')
    # Existing capture rows and the original versions table are evidence, not
    # migration scratch space.  Add nullable/defaulted projection fields only.
    columns = {row['name'] for row in conn.execute('PRAGMA table_info(documents)')}
    for name, declaration in (
        ('parser_version', 'INTEGER NOT NULL DEFAULT 0'), ('parsed_at', 'TEXT'),
        ('content_type', 'TEXT'), ('reparse_error', 'TEXT'),
        ('raw_integrity_status', "TEXT NOT NULL DEFAULT 'not_checked'"),
        ('last_reparse_attempt_version', 'INTEGER NOT NULL DEFAULT 0'),
    ):
        if name not in columns:
            conn.execute(f'ALTER TABLE documents ADD COLUMN {name} {declaration}')
    conn.commit()
    return conn


def current_parser_version() -> int:
    from downloader import taifex_rule_parsing
    version = getattr(taifex_rule_parsing, 'PARSER_VERSION', 1)
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ValueError('positive_integer_parser_version_required')
    return version


def local_evidence_path(root: Path, value: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise ArchiveIntegrityError('archive_path_must_be_relative')
    path = (root / value).resolve()
    if not path.is_relative_to(root.resolve()) or '..' in Path(value).parts:
        raise ArchiveIntegrityError('archive_path_outside_root')
    if not path.is_file():
        raise ArchiveIntegrityError(f'archive_file_missing:{value}')
    return path


def read_verified_raw(root: Path, task) -> bytes:
    path = local_evidence_path(root, task['raw_path'])
    if path.stat().st_size > MAX_COMPRESSED_BYTES:
        raise ArchiveIntegrityError('compressed_archive_exceeds_limit')
    if sha256_path(path) != task['raw_sha256']:
        raise ArchiveIntegrityError('compressed_archive_sha256_mismatch')
    try:
        with gzip.open(path, 'rb') as handle:
            body = handle.read(MAX_RESPONSE_BYTES + 1)
    except (OSError, EOFError, zlib.error) as error:
        raise ArchiveIntegrityError(f'compressed_archive_decode_failed:{type(error).__name__}') from error
    if len(body) > MAX_RESPONSE_BYTES:
        raise ArchiveIntegrityError('decoded_archive_exceeds_limit')
    if digest(body) != task['content_sha256']:
        raise ArchiveIntegrityError('decoded_archive_sha256_mismatch')
    return body


def stored_content_type(root: Path, task) -> tuple[str, str]:
    if task['content_type']:
        return task['content_type'], 'captured_http_content_type'
    # Old captures did not persist MIME.  A matching legacy parse provides a
    # format hint only; extract_document still gives magic bytes precedence.
    if task['parsed_path']:
        try:
            path = local_evidence_path(root, task['parsed_path'])
            if path.stat().st_size <= MAX_RESPONSE_BYTES:
                parsed = json.loads(path.read_text(encoding='utf-8'))
                mime = {'html': 'text/html', 'csv': 'text/csv', 'text': 'text/plain',
                        'pdf': 'application/pdf'}.get(parsed.get('format'))
                if mime and parsed.get('content_sha256') == task['content_sha256']:
                    return mime, 'legacy_parsed_format_hint'
        except (OSError, ValueError):
            pass
    return '', 'magic_and_url_only_no_verified_mime'


def enqueue(conn, url: str, kind: str, source_id: str, priority: int) -> None:
    conn.execute('INSERT INTO documents(url,kind,source_id,priority) VALUES(?,?,?,?) '
                 'ON CONFLICT(url) DO UPDATE SET priority=min(priority,excluded.priority)',
                 (url, kind, source_id, priority))


def refresh_if_stale(conn, url: str, cutoff: str) -> None:
    # Failed tasks retain their bounded retry schedule. Old completed captures
    # can be refreshed without losing the prior immutable version.
    conn.execute("UPDATE documents SET state='pending',next_retry=NULL WHERE url=? AND state='complete' AND checked_at<?",
                 (url, cutoff))


def archive_raw(root: Path, content: bytes, format: str) -> tuple[str, str, str]:
    content_hash = digest(content)
    path = root / 'raw' / content_hash[:2] / f'{content_hash}.{format}.gz'
    encoded = gzip.compress(content, compresslevel=6, mtime=0)
    if path.exists():
        if path.read_bytes() != encoded:
            raise ArchiveIntegrityError('immutable_raw_hash_mismatch')
    else:
        atomic_write_bytes(path, encoded)
    return str(path.relative_to(root)), digest(encoded), content_hash


def fetch(session: requests.Session, limiter: SharedRateLimiter, url: str, *, payload=None) -> tuple[bytes, str]:
    if official_url(url) is None:
        raise ValueError('off_domain_fetch_not_allowed')
    # Validate each redirect BEFORE requesting it; source HTML cannot send the
    # collector to credentials, another host or an unbounded redirect chain.
    for _ in range(5):
        limiter.wait()
        with session.request('POST' if payload is not None else 'GET', url, data=payload,
                             timeout=(10, 45), stream=True, allow_redirects=False) as response:
            if response.is_redirect:
                target = official_url(response.headers.get('Location', ''), url)
                if not target:
                    raise ValueError('off_domain_redirect')
                url, payload = target, None
                continue
            if response.status_code == 429:
                delay = max(60, parse_retry_after_seconds(
                    response.headers.get('Retry-After'), now=datetime.now(UTC)
                ) or 0)
                limiter.defer(delay)
            elif response.status_code == 403:
                limiter.defer(600)
                raise ProviderBlockedError('HTTP_403_provider_cooldown_600s')
            response.raise_for_status()
            chunks, length = [], 0
            for chunk in response.iter_content(65536):
                length += len(chunk)
                if length > MAX_RESPONSE_BYTES:
                    raise ValueError('response_exceeds_32MiB_bounded_archive')
                chunks.append(chunk)
            body = b''.join(chunks)
            content_type = response.headers.get('Content-Type', '')
            if any(marker in body.upper() for marker in (b'REQUEST REJECTED', b'FOR SECURITY REASONS', b'ACCESS DENIED')):
                limiter.defer(600)
                raise ProviderBlockedError('WAF_provider_cooldown_600s')
            error_page = urlsplit(url).path.rstrip('/').rsplit('/', 1)[-1].lower() in {
                '404', '404.html', '404.htm', '404.jsp', '404.aspx',
            }
            if not body or error_page or re.search(br'<title>\s*404\b', body[:65536], re.I):
                raise ValueError('empty_or_error_response')
            return body, content_type
    raise ValueError('too_many_redirects')


def index_year(conn, root: Path, session, limiter, year: int, end: date) -> int:
    start = date(year, 1, 1)
    stop = min(date(year, 12, 31), end)
    key = str(year)
    previous = conn.execute('SELECT * FROM index_windows WHERE key=?', (key,)).fetchone()
    if previous and previous['end'] == str(stop):
        stamp = datetime.fromisoformat(previous['checked_at'])
        ttl = timedelta(days=30) if year < end.year else timedelta(hours=6)
        if datetime.now(UTC) - stamp < ttl:
            path = root / previous['raw_path']
            if path.is_file() and sha256_path(path) == previous['raw_sha256']:
                return 0
    body, _ = fetch(session, limiter, INDEX_URL, payload={
        'isQuery': '1', 'queryStartDate': start.strftime('%Y/%m/%d'),
        'queryEndDate': stop.strftime('%Y/%m/%d'), 'newsType': '', 'queryKeyWord': '',
    })
    raw, raw_hash, _ = archive_raw(root, body, 'html')
    try:
        rows = parse_index(body, start, stop)
    except ValueError as error:
        raise ValueError(f'{error}; archived_response={raw}') from error
    for row in rows:
        conn.execute('INSERT OR IGNORE INTO announcements VALUES(?,?,?,?,?,?,?)',
                     tuple(row[key] for key in ('id', 'published_date', 'title', 'url', 'category',
                                                'download_allowed', 'publication_precision')))
        if row['download_allowed']:
            priority = 1 if row['category'] != 'other_announcements' else 5
            enqueue(conn, row['url'], 'announcement', row['category'], priority)
            if date.fromisoformat(row['published_date']) >= end - timedelta(days=14):
                refresh_if_stale(conn, row['url'], (datetime.now(UTC)-timedelta(hours=6)).isoformat())
    conn.execute('INSERT OR REPLACE INTO index_windows VALUES(?,?,?,?,?,?,?)',
                 (key, str(start), str(stop), now_iso(), len(rows), raw, raw_hash))
    conn.commit()
    return 1


def persist_parse_version(conn, root: Path, task, body: bytes, *, content_type: str,
                          content_type_evidence: str, captured: str | None,
                          raw: str, raw_hash: str, content_hash: str) -> dict:
    """Keep independent, immutable capture and parser-version provenance."""
    from downloader.taifex_rule_parsing import extract_document, margin_changes
    version = current_parser_version()
    previous = conn.execute('SELECT * FROM parse_versions WHERE url=? AND content_sha256=? AND parser_version=?',
                            (task['url'], content_hash, version)).fetchone()
    if previous is not None:
        path = local_evidence_path(root, previous['parsed_path'])
        if sha256_path(path) != previous['parsed_sha256']:
            raise ArchiveIntegrityError('immutable_parsed_sha256_mismatch')
        for artifact in json.loads(previous['shards_json']).values():
            shard = local_evidence_path(root, artifact['path'])
            if shard.stat().st_size != artifact['bytes'] or sha256_path(shard) != artifact['sha256']:
                raise ArchiveIntegrityError('immutable_shard_sha256_mismatch')
        return dict(previous)
    parsed_at = now_iso()
    try:
        parsed = extract_document(body, content_type, task['url'])
    except Exception as error:
        parsed = {'format': 'unknown', 'parsing_status': 'failed', 'tables': [],
                  'temporal_mentions': [], 'warnings': [f'parser_exception:{type(error).__name__}']}
    url_hash = digest(task['url'].encode())
    parsed_path = root / 'parsed' / f'v{version}' / url_hash / f'{content_hash}.json'
    table_rows = []
    margin_rows = []
    for table in parsed.get('tables', []):
        for index, row in enumerate(table.get('rows', [])):
            table_rows.append({'table_index': table.get('table_index', 0), 'row_index': index,
                               'fields_json': json.dumps(row, ensure_ascii=False)})
        margin_rows.extend(margin_changes(table.get('rows', [])))
    shards = {}
    for name, rows in (('table_cells', table_rows), ('temporal_mentions', parsed.get('temporal_mentions', [])),
                       ('margin_changes', margin_rows)):
        # URL/MIME can influence a parse.  Keep this projection source-specific
        # while raw bytes remain deduplicated solely by their content digest.
        target = root / 'shards' / f'v{version}' / url_hash / name / f'{content_hash}.parquet'
        if rows:
            # Complex cells stay lossless JSON, not float guesses or mixed units.
            data = [{str(k): json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                     for k, v in row.items()} for row in rows]
            for row in data:
                row['content_sha256'] = content_hash
            # Nullable string facts need a stable Arrow type even when every
            # unit/normalised value in this particular shard is unknown.
            integer_fields = {'table_index', 'row_index', 'source_row_index'}
            schema = pa.schema([(key, pa.int64() if key in integer_fields else pa.string())
                                for key in data[0]])
            table = pa.Table.from_pylist(data, schema=schema)
            if target.exists():
                import pyarrow.parquet as pq
                if not pq.read_table(target).equals(table):
                    raise ArchiveIntegrityError('immutable_parser_shard_conflict')
            else:
                atomic_write_parquet(target, table, compression='zstd')
            shards[name] = {'path': str(target.relative_to(root)), 'rows': len(rows),
                            'bytes': target.stat().st_size, 'sha256': sha256_path(target)}
    parsed.update(source_url=task['url'], content_sha256=content_hash,
                  raw_path=raw, raw_sha256=raw_hash, parser_version=version,
                  parsed_at_utc=parsed_at, captured_at_utc=captured,
                  content_type=content_type, content_type_evidence=content_type_evidence,
                  point_in_time_verified=False, historical_attachment_binding_verified=False,
                  shards=shards, artifact_selection='only_shards_referenced_by_this_parse_version')
    if parsed_path.exists():
        # A process may have died after writing metadata but before committing
        # SQLite.  Once all URL/content/parser/raw/shard evidence agrees, reuse
        # both its original capture and parse times, never overwrite a receipt
        # or mislabel the later retry as the first capture of these same bytes.
        existing = json.loads(parsed_path.read_text(encoding='utf-8'))
        time_fields = {'parsed_at_utc', 'captured_at_utc'}
        stable = {key: value for key, value in parsed.items() if key not in time_fields}
        if {key: value for key, value in existing.items() if key not in time_fields} != stable:
            raise ArchiveIntegrityError('immutable_parser_metadata_conflict')
        recorded = conn.execute('SELECT captured_at FROM versions WHERE url=? AND content_sha256=?',
                                (task['url'], content_hash)).fetchone()
        if recorded and existing.get('captured_at_utc') != recorded['captured_at']:
            raise ArchiveIntegrityError('immutable_parser_capture_conflict')
        captured = existing['captured_at_utc']
        parsed_at = existing['parsed_at_utc']
    else:
        atomic_write_json(parsed_path, parsed)
    result = {
        'url': task['url'], 'content_sha256': content_hash, 'parser_version': version,
        'parsed_at': parsed_at, 'captured_at': captured,
        'parsed_path': str(parsed_path.relative_to(root)), 'parsed_sha256': sha256_path(parsed_path),
        'shards_json': json.dumps(shards, ensure_ascii=False, sort_keys=True),
        'parsing_status': parsed['parsing_status'], 'table_rows': len(table_rows),
        'temporal_rows': len(parsed.get('temporal_mentions', [])), 'raw_path': raw, 'raw_sha256': raw_hash,
    }
    conn.execute('''INSERT INTO parse_versions(url,content_sha256,parser_version,parsed_at,captured_at,
        parsed_path,parsed_sha256,shards_json,parsing_status,table_rows,temporal_rows,raw_path,raw_sha256)
        VALUES(:url,:content_sha256,:parser_version,:parsed_at,:captured_at,:parsed_path,:parsed_sha256,
        :shards_json,:parsing_status,:table_rows,:temporal_rows,:raw_path,:raw_sha256)''', result)
    return result


def discover_preserved_links(conn, body: bytes, task, parsed_format: str, *, refresh_existing: bool = True) -> None:
    if parsed_format == 'html':
        for url, label, kind in document_links(body, task['url'], discover_specs=task['source_id'] == 'contract_specs'):
            enqueue(conn, url, kind, task['source_id'], 0)
            if refresh_existing:
                refresh_if_stale(conn, url, (datetime.now(UTC)-timedelta(hours=6)).isoformat())
            conn.execute('INSERT OR IGNORE INTO links VALUES(?,?,?)', (task['url'], url, label))


def process_document(conn, root: Path, task, session, limiter) -> None:
    payload = {} if task['url'].endswith('/traderPLEquityDown') else None
    body, content_type = fetch(session, limiter, task['url'], payload=payload)
    checked = now_iso()
    # Capture bytes before parsing; unsupported formats remain preserved.
    raw, raw_hash, content_hash = archive_raw(root, body, 'body')
    previous = conn.execute('SELECT captured_at FROM versions WHERE url=? AND content_sha256=?',
                            (task['url'], content_hash)).fetchone()
    captured = previous['captured_at'] if previous else checked
    result = persist_parse_version(conn, root, task, body, content_type=content_type,
                                   content_type_evidence='captured_http_content_type', captured=captured,
                                   raw=raw, raw_hash=raw_hash, content_hash=content_hash)
    parsed = json.loads((root / result['parsed_path']).read_text(encoding='utf-8'))
    discover_preserved_links(conn, body, task, parsed.get('format', 'unknown'))
    conn.execute('INSERT OR IGNORE INTO versions VALUES(?,?,?,?,?,?)',
                 (task['url'], content_hash, result['captured_at'], raw, raw_hash, result['parsed_path']))
    conn.execute('''UPDATE documents SET state='complete',checked_at=?,next_retry=NULL,
        raw_path=?,raw_sha256=?,content_sha256=?,parsed_path=?,parsing_status=?,
        table_rows=?,temporal_rows=?,http_status=200,error=NULL,attempts=0,
        parser_version=?,parsed_at=?,content_type=?,reparse_error=NULL,raw_integrity_status='write_time_verified'
        WHERE url=?''',
                 (checked, raw, raw_hash, content_hash, result['parsed_path'], result['parsing_status'],
                  result['table_rows'], result['temporal_rows'], result['parser_version'], result['parsed_at'],
                  content_type, task['url']))
    conn.commit()


def next_document(conn, *, prioritize_rules: bool = False):
    """Prioritize dated rules while retaining the full queue and retry clocks."""
    order = "(d.state='failed'),d.priority,d.url DESC"
    if prioritize_rules:
        order = """CASE
            WHEN d.kind='attachment' AND d.source_id IN
                ('margins','position_limits','contract_specs') THEN 0
            WHEN EXISTS (SELECT 1 FROM announcements a WHERE a.url=d.url
                AND a.category='margins' AND a.title NOT LIKE '%抵繳%'
                AND (a.title LIKE '%金金額%' OR a.title LIKE '%級距%'
                     OR a.title LIKE '%適用比例%' OR a.title LIKE '%金比例%'
                     OR a.title LIKE '%金計收%' OR a.title LIKE '%原始保證金%')) THEN 1
            WHEN d.source_id='position_limits' THEN 2
            WHEN d.source_id='contract_specs' THEN 3
            ELSE 4 END,(d.state='failed'),d.priority,
            COALESCE((SELECT MIN(a.published_date) FROM announcements a
                      WHERE a.url=d.url),'9999'),d.url"""
    return conn.execute(
        "SELECT d.* FROM documents d WHERE d.state IN ('pending','failed') "
        "AND (d.next_retry IS NULL OR d.next_retry<=?) ORDER BY " + order + " LIMIT 1",
        (now_iso(),),
    ).fetchone()


def reparse_document(conn, root: Path, task) -> dict:
    body = read_verified_raw(root, task)
    content_type, evidence = stored_content_type(root, task)
    capture = conn.execute('SELECT captured_at FROM versions WHERE url=? AND content_sha256=?',
                           (task['url'], task['content_sha256'])).fetchone()
    captured = capture['captured_at'] if capture else task['checked_at']
    result = persist_parse_version(conn, root, task, body, content_type=content_type,
                                   content_type_evidence=evidence, captured=captured,
                                   raw=task['raw_path'], raw_hash=task['raw_sha256'],
                                   content_hash=task['content_sha256'])
    parsed = json.loads((root / result['parsed_path']).read_text(encoding='utf-8'))
    discover_preserved_links(conn, body, task, parsed.get('format', 'unknown'), refresh_existing=False)
    # Do not touch checked_at, original capture versions, HTTP attempts/retry,
    # or a newer failed network refresh.  Parsing is not another HTTP capture.
    conn.execute('''UPDATE documents SET parser_version=?,parsed_at=?,parsed_path=?,parsing_status=?,
        table_rows=?,temporal_rows=?,reparse_error=NULL,raw_integrity_status='verified',
        last_reparse_attempt_version=?,state=CASE WHEN state IN ('integrity_failed','reparse_failed')
        THEN 'complete' ELSE state END WHERE url=?''',
                 (result['parser_version'], result['parsed_at'], result['parsed_path'], result['parsing_status'],
                  result['table_rows'], result['temporal_rows'], result['parser_version'], task['url']))
    conn.commit()
    return result


def publish_status(conn, root: Path, *, start_year: int, end: date, running: bool,
                   errors: list[dict], export: bool = False,
                   offline_reparsed_count: int = 0) -> dict:
    cooldown = read_provider_cooldown(root)
    provider_deferred = provider_cooldown_active(cooldown)
    state_counts = dict(conn.execute('SELECT state,count(*) FROM documents GROUP BY state'))
    first, last, announcements = conn.execute('SELECT min(published_date),max(published_date),count(*) FROM announcements').fetchone()
    index_count = conn.execute("SELECT count(*) FROM index_windows WHERE key>=? AND key<=? "
                               "AND start=key||'-01-01' AND end=CASE WHEN key=? THEN ? ELSE key||'-12-31' END",
                               (str(start_year), str(end.year), str(end.year), str(end))).fetchone()[0]
    index_total = end.year-start_year+1
    complete = state_counts.get('complete', 0)
    pending = sum(value for key, value in state_counts.items() if key != 'complete')
    tables, temporal = conn.execute('SELECT coalesce(sum(table_rows),0),coalesce(sum(temporal_rows),0) FROM documents').fetchone()
    parse_gaps = conn.execute("SELECT count(*) FROM documents WHERE state='complete' AND coalesce(parsing_status,'missing')!='parsed'").fetchone()[0]
    parsing_counts = dict(conn.execute("SELECT coalesce(parsing_status,'not_parsed'),count(*) FROM documents GROUP BY parsing_status"))
    version_counts = {str(version): count for version, count in conn.execute(
        'SELECT parser_version,count(*) FROM documents WHERE content_sha256 IS NOT NULL GROUP BY parser_version')}
    parser_version = current_parser_version()
    outdated = conn.execute('SELECT count(*) FROM documents WHERE content_sha256 IS NOT NULL AND parser_version<?',
                            (parser_version,)).fetchone()[0]
    artifacts = {}
    if export:
        for name, query in (
            ('announcements', 'SELECT * FROM announcements ORDER BY published_date,id'),
            ('documents', 'SELECT * FROM documents ORDER BY url'),
            ('document_links', 'SELECT * FROM links ORDER BY parent,child'),
            ('parse_versions', 'SELECT * FROM parse_versions ORDER BY url,content_sha256,parser_version'),
        ):
            rows = [dict(row) for row in conn.execute(query)]
            target = root / 'normalized' / f'{name}.parquet'
            if rows:
                atomic_write_parquet(target, pa.Table.from_pylist(rows), compression='zstd')
                artifacts[name] = {'path': str(target.relative_to(root)), 'rows': len(rows), 'sha256': sha256_path(target)}
    sources = [{'source_id': 'his_news', 'name': '全部類型歷史公告索引', 'url': INDEX_URL,
                'kind': 'historical_index', 'status': 'indexed' if index_count == end.year-start_year+1 else 'partial',
                'rows': announcements, 'first_date': first, 'last_date': last}]
    for source_id, name, url in SEEDS:
        row = conn.execute('SELECT state,table_rows,checked_at FROM documents WHERE url=?', (url,)).fetchone()
        sources.append({'source_id': source_id, 'name': name, 'url': url, 'kind': 'current_snapshot_not_history',
                        'status': row['state'] if row else 'missing', 'rows': row['table_rows'] if row else None,
                        'first_date': None, 'last_date': None, 'observed_at_utc': row['checked_at'] if row else None})
    external_links = conn.execute('SELECT count(*) FROM announcements WHERE download_allowed=0').fetchone()[0]
    payload = {
        'schema_version': 1, 'dataset': 'taifex_public_rule_history', 'observed_at_utc': now_iso(),
        'status': 'running' if running else 'partial' if pending or errors or parse_gaps or outdated or provider_deferred or index_count != index_total else 'current',
        'provider_deferred': provider_deferred,
        'provider_cooldown_until_utc': cooldown['deadline_utc'] if cooldown else None,
        'counts': {'index_years_complete': index_count, 'index_years_total': index_total,
                   'announcements': announcements, 'documents_complete': complete, 'documents_pending': pending,
                   'documents_failed': state_counts.get('failed', 0),
                   'documents_integrity_failed': state_counts.get('integrity_failed', 0),
                   'documents_reparse_failed': state_counts.get('reparse_failed', 0),
                   'documents_parser_outdated': outdated,
                   'attachments_complete': conn.execute("SELECT count(*) FROM documents WHERE kind='attachment' AND state='complete'").fetchone()[0],
                   'normalized_table_rows': tables, 'temporal_mentions': temporal,
                   'external_links_not_crawled': external_links,
                   'parse_gaps': parse_gaps},
        'quality': {'parsing_status_counts': parsing_counts, 'parser_version_counts': version_counts,
                    'current_parser_version': parser_version, 'offline_reparsed_count': offline_reparsed_count,
                    'reparse_errors': [item for item in errors if item.get('stage') == 'reparse'][-30:]},
        'coverage': {'first_published_date': first, 'last_published_date': last,
                     'index_start': f'{start_year}-01-01', 'index_end': str(end),
                     'all_documents_fetched': not pending and index_count == index_total and not errors and not external_links and not provider_deferred,
                     'official_scope_documents_fetched': not pending and index_count == index_total and not errors and not provider_deferred,
                     'point_in_time_verified': False, 'historical_values_complete': False},
        'sources': sources, 'outputs': artifacts, 'errors': errors[-30:],
        'integrity_scope': 'write_time_hashes_and_resume_existence_not_full_archive_audit',
        'parse_integrity_scope': 'reparsed_raw_compressed_and_content_hashes_verified_per_document',
        'artifact_selection': 'documents.parsed_path_then_exact_versioned_shards_metadata_never_glob_all_versions',
        'limitations': ['current_web_values_are_not_historical_values', 'publication_date_is_not_effective_time',
                       'session_boundary_is_not_midnight', 'reused_attachment_urls_may_have_changed_contents',
                       'raw_archive_not_executable_margin_rules', 'off_domain_links_listed_not_crawled',
                       'official_unavailable_old_links_remain_visible'],
    }
    atomic_write_json(root / 'manifest.json', payload)
    return payload


def reparse_batch(conn, root: Path, *, started: float, max_documents: int, max_seconds: float,
                  errors: list[dict], explicit: bool, start_year: int, end: date,
                  statuses: tuple[str, ...] = ()) -> tuple[int, int]:
    version = current_parser_version()
    status_filter = (' AND parsing_status IN (' + ','.join('?' for _ in statuses) + ')') if statuses else ''
    tasks = conn.execute('''SELECT * FROM documents WHERE content_sha256 IS NOT NULL
        AND parser_version<? AND (? OR last_reparse_attempt_version<?)
        ''' + status_filter + '''
        ORDER BY (parsing_status='unsupported' AND
            (lower(url) LIKE '%.docx' OR lower(url) LIKE '%.odt' OR lower(url) LIKE '%.ods')) DESC,
            priority,url LIMIT ?''', (version, int(explicit), version, *statuses, max_documents)).fetchall()
    attempted = succeeded = 0
    for task in tasks:
        if time.monotonic() - started >= max_seconds:
            break
        attempted += 1
        try:
            reparse_document(conn, root, task)
            succeeded += 1
        except Exception as error:
            conn.rollback()
            state = 'integrity_failed' if isinstance(error, ArchiveIntegrityError) else 'reparse_failed'
            message = f'{type(error).__name__}: {str(error)[:180]}'
            conn.execute('''UPDATE documents SET state=?,reparse_error=?,last_reparse_attempt_version=?,
                raw_integrity_status=CASE WHEN ?='integrity_failed' THEN 'failed' ELSE raw_integrity_status END
                WHERE url=?''', (state, message, version, state, task['url']))
            conn.commit()
            errors.append({'stage': 'reparse', 'url': task['url'], 'error_type': type(error).__name__,
                           'reason': str(error)[:220]})
        if attempted % 10 == 0:
            publish_status(conn, root, start_year=start_year, end=end, running=True, errors=errors,
                           offline_reparsed_count=succeeded)
    return attempted, succeeded


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('data_taifex_public_history/rules'))
    parser.add_argument('--start-year', type=int, default=1997)
    parser.add_argument('--end', type=date.fromisoformat, default=date.today())
    parser.add_argument('--max-documents', type=int, default=1200)
    parser.add_argument('--max-seconds', type=float, default=1800)
    parser.add_argument('--request-interval', type=float, default=1.0)
    parser.add_argument('--index-only', action='store_true')
    parser.add_argument('--prioritize-rules', action='store_true',
                        help='Collect dated margin, position and specification notices first; preserve the full queue')
    parser.add_argument('--download-only', action='store_true',
                        help='Prioritize missing raw documents without spending the HTTP budget on offline reparse')
    parser.add_argument('--reparse-only', action='store_true',
                        help='Verify and reparse old local captures under the newest parser; never create HTTP or limiter objects')
    parser.add_argument('--reparse-status', action='append', default=[],
                        choices=('unsupported', 'pending_ocr', 'failed', 'partial', 'empty', 'parsed'),
                        help='Restrict an offline reparse to these previous statuses; repeat to select multiple')
    parser.add_argument('--offline-audit', action='store_true',
                        help='Export local inventory without HTTP; not a full payload-hash audit')
    args = parser.parse_args(argv)
    if args.reparse_only and (args.index_only or args.offline_audit):
        parser.error('--reparse-only cannot be combined with --index-only or --offline-audit')
    if args.download_only and (args.reparse_only or args.index_only or args.offline_audit):
        parser.error('--download-only cannot be combined with offline or index-only modes')
    if args.reparse_status and not args.reparse_only:
        parser.error('--reparse-status requires --reparse-only')
    if not 1997 <= args.start_year <= args.end.year or args.end > date.today():
        parser.error('valid 1997..today date range required')
    if args.request_interval < 1 or args.max_documents < 1 or args.max_seconds <= 0:
        parser.error('positive budgets and at least 1 second shared pacing required')
    root = args.output_dir.resolve()
    if root.is_relative_to(Path('/srv/stockagent-packed')) or root.is_relative_to(Path('/srv/stockagent-packed-materialized')):
        parser.error('immutable cold/materialized stores cannot be downloader destinations')
    root.mkdir(parents=True, exist_ok=True)
    (root / 'state').mkdir(exist_ok=True)
    with (root / 'state' / 'collector.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('TAIFEX rule archive already running; no duplicate writer')
            return 0
        with closing(open_queue(root)) as conn:
            if args.offline_audit:
                report = publish_status(conn, root, start_year=args.start_year, end=args.end,
                                        running=False, errors=[], export=True)
                print(json.dumps(report['counts'], ensure_ascii=False))
                return 0
            started, attempted, reparsed = time.monotonic(), 0, 0
            errors = []
            if not args.index_only and not args.download_only:
                attempted, reparsed = reparse_batch(
                    conn, root, started=started, max_documents=args.max_documents,
                    max_seconds=args.max_seconds, errors=errors, explicit=args.reparse_only,
                    start_year=args.start_year, end=args.end,
                    statuses=tuple(args.reparse_status),
                )
            if args.reparse_only or attempted >= args.max_documents or time.monotonic() - started >= args.max_seconds:
                report = publish_status(conn, root, start_year=args.start_year, end=args.end,
                                        running=False, errors=errors, export=True,
                                        offline_reparsed_count=reparsed)
                print(json.dumps({'status': report['status'], 'offline_reparsed_count': reparsed,
                                  **report['counts']}, ensure_ascii=False), flush=True)
                return 1 if errors else 0
            cooldown = read_provider_cooldown(root)
            if provider_cooldown_active(cooldown):
                report = publish_status(
                    conn, root, start_year=args.start_year, end=args.end,
                    running=False, errors=[*errors, {
                        'stage': 'provider_cooldown', 'error_type': cooldown['error_type'],
                        'reason': cooldown['reason'], 'deadline_utc': cooldown['deadline_utc'],
                    }], offline_reparsed_count=reparsed,
                )
                print(json.dumps({
                    'status': report['status'], 'provider_deferred': True,
                    'provider_cooldown_until_utc': report['provider_cooldown_until_utc'],
                    **report['counts'],
                }, ensure_ascii=False), flush=True)
                return 0
            provider_deferred = False
            for source_id, _, url in SEEDS:
                enqueue(conn, url, 'snapshot', source_id, 0)
            # Current pages refresh independently from immutable historical
            # documents. Repeated URL bodies get new content-addressed versions.
            cutoff = (datetime.now(UTC)-timedelta(hours=6)).isoformat()
            # A dangling local capture is an integrity incident, not permission
            # to silently replace evidence from the network.  Parser upgrades
            # verified both hashes above; steady runs only check path/existence.
            for row in conn.execute("SELECT url,raw_path,parsed_path FROM documents WHERE state='complete'").fetchall():
                try:
                    for key in ('raw_path', 'parsed_path'):
                        local_evidence_path(root, row[key])
                except ArchiveIntegrityError as error:
                    conn.execute("UPDATE documents SET state='integrity_failed',raw_integrity_status='failed',reparse_error=? WHERE url=?",
                                 (str(error), row['url']))
            conn.execute("UPDATE documents SET state='pending' WHERE kind IN ('snapshot','specification') AND state='complete' AND checked_at<?", (cutoff,))
            conn.commit()
            limiter = SharedRateLimiter(args.request_interval, name='taifex_public_history')
            with requests.Session() as session:
                session.headers['User-Agent'] = 'stockAgent/taifex-official-rule-research (bounded archive)'
                for year in range(args.end.year, args.start_year-1, -1):
                    if time.monotonic()-started >= args.max_seconds:
                        break
                    try:
                        index_year(conn, root, session, limiter, year, args.end)
                    except Exception as error:
                        errors.append({'stage': 'index', 'year': year, 'error_type': type(error).__name__, 'reason': str(error)[:220]})
                        if is_provider_deferral(error):
                            record_provider_cooldown(root, error)
                            provider_deferred = True
                            break
                    publish_status(conn, root, start_year=args.start_year, end=args.end, running=True, errors=errors,
                                   offline_reparsed_count=reparsed)
                while not provider_deferred and not args.index_only and attempted < args.max_documents and time.monotonic()-started < args.max_seconds:
                    task = next_document(conn, prioritize_rules=args.prioritize_rules)
                    if task is None:
                        break
                    attempted += 1
                    try:
                        process_document(conn, root, task, session, limiter)
                    except Exception as error:
                        conn.rollback()
                        attempts = task['attempts'] + 1
                        next_retry = datetime.now(UTC) + timedelta(seconds=min(3600, 30 * 2**min(attempts, 7)))
                        if is_provider_deferral(error):
                            next_retry = max(next_retry, record_provider_cooldown(root, error))
                        status = getattr(getattr(error, 'response', None), 'status_code', None)
                        failed_state = 'integrity_failed' if isinstance(error, ArchiveIntegrityError) else 'failed'
                        conn.execute("UPDATE documents SET state=?,attempts=?,next_retry=?,http_status=?,error=? WHERE url=?",
                                     (failed_state, attempts, next_retry.isoformat(), status, f'{type(error).__name__}: {str(error)[:180]}', task['url']))
                        conn.commit()
                        if is_provider_deferral(error):
                            provider_deferred = True
                            errors.append({'stage': 'documents', 'error_type': type(error).__name__, 'reason': str(error)})
                            break
                    if attempted % 10 == 0:
                        report = publish_status(conn, root, start_year=args.start_year, end=args.end,
                                                running=True, errors=errors, export=attempted % 100 == 0,
                                                offline_reparsed_count=reparsed)
                        print(json.dumps({'attempted': attempted, **report['counts']}, ensure_ascii=False), flush=True)
            report = publish_status(conn, root, start_year=args.start_year, end=args.end,
                                    running=False, errors=errors, export=True, offline_reparsed_count=reparsed)
            print(json.dumps({'status': report['status'], **report['counts']}, ensure_ascii=False), flush=True)
            # A successfully audited resumable batch is not a whole-history
            # completion claim. Failed work stays queued; all-index failure fails.
            return 0 if report['counts']['index_years_complete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
