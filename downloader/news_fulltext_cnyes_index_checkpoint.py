"""Interrupted public-index pages, replayed by the existing CNYES owner."""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.news_fulltext_cnyes_checkpoint import HEAD_FIELDS, HEADER_FIELDS, _clock
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import replay_source_path


VERSION = 1
PREFIX = 'cnyes_index_intents/'
MAX_BODY_BYTES = 32 << 20
MAX_FILE_BYTES = 64 << 20
MAX_UNCOMPRESSED_BYTES = 128 << 20
MAX_RECEIPT_BYTES = 64 << 10
ORIGINAL_FIELDS = frozenset({'request_url', 'response_sha256', 'raw_response_body',
    'source_observed_at_utc', 'index_fingerprint', 'authorization_fingerprint',
    'metadata_only', 'accepted_body_count', 'publish'})
_ORIGINAL_NAME = re.compile(r'([0-9a-f]{64})-([0-9a-f]{64})\.parquet')


def _stable_stat(path):
    try:
        value = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _read_stable(path, limit):
    before = _stable_stat(path)
    if before is None or path.is_symlink() or not path.is_file():
        raise ValueError('missing_or_unsafe_file')
    if before[2] > limit:
        raise ValueError('file_size_exceeds_limit')
    with path.open('rb') as stream:
        data = stream.read(limit + 1)
    if len(data) > limit or len(data) != before[2] or _stable_stat(path) != before:
        raise ValueError('file_changed_or_exceeds_limit')
    return data


def read_retained_original(path, fingerprint, source, *, expected_url=None, expected_body=None):
    """Read a bounded immutable envelope and prove its identity and clock."""
    from downloader.news_fulltext_cnyes_index import compatible_fingerprints
    name = _ORIGINAL_NAME.fullmatch(path.name)
    if name is None or re.fullmatch(r'[0-9a-f]{64}', fingerprint) is None:
        raise ValueError('unexpected_original_path')
    raw_file = _read_stable(path, MAX_FILE_BYTES)
    try:
        parquet = pq.ParquetFile(pa.BufferReader(raw_file))
        names = parquet.schema_arrow.names
        if (parquet.metadata.num_rows != 1 or len(names) != len(ORIGINAL_FIELDS)
                or set(names) != ORIGINAL_FIELDS
                or sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
                   > MAX_UNCOMPRESSED_BYTES):
            raise ValueError('invalid_original_schema')
        original = parquet.read().to_pylist()[0]
    except pa.ArrowException as error:
        raise ValueError('invalid_original_parquet') from error
    body, url = original['raw_response_body'], original['request_url']
    if (not isinstance(body, bytes) or len(body) > MAX_BODY_BYTES or not isinstance(url, str)
            or hashlib.sha256(url.encode()).hexdigest() != name.group(1)
            or hashlib.sha256(body).hexdigest() != name.group(2)
            or original['response_sha256'] != name.group(2) or original['index_fingerprint'] != fingerprint
            or original['metadata_only'] is not True or original['publish'] is not False
            or type(original['accepted_body_count']) is not int or original['accepted_body_count'] != 0
            or expected_url is not None and url != expected_url
            or expected_body is not None and body != expected_body):
        raise ValueError('original_identity_or_hash_mismatch')
    _clock(original['source_observed_at_utc'])
    if (not source.allowed or fingerprint not in compatible_fingerprints(source)
            or original['authorization_fingerprint'] != source.authorization_fingerprint):
        raise ValueError('source_policy_not_current_or_compatible')
    return original, hashlib.sha256(raw_file).hexdigest()


def inspect_retained(path, relative, fingerprint, receipt_path, references, source):
    """Validate a retained index original, its receipt and optional DB row.

    Shared by conditional live acceptance and the offline inventory. Historical
    envelopes lack HTTP headers: synthetic parser inputs do not prove them.
    """
    from downloader.news_fulltext_cnyes_index import parse_index

    original, file_sha = read_retained_original(path, fingerprint, source)
    body, url = original['raw_response_body'], original['request_url']
    name = _ORIGINAL_NAME.fullmatch(path.name)
    original_clock = _clock(original['source_observed_at_utc'])
    try:
        receipt = json.loads(_read_stable(receipt_path, MAX_RECEIPT_BYTES))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError('invalid_receipt_json') from error
    if not isinstance(receipt, dict):
        raise ValueError('invalid_receipt_object')
    if (receipt.get('parquet_path') != relative or receipt.get('sha256') != file_sha
            or receipt.get('response_sha256') != name.group(2) or receipt.get('index_fingerprint') != fingerprint
            or receipt.get('metadata_only') is not True or receipt.get('publish') is not False
            or type(receipt.get('accepted_body_count')) is not int or receipt['accepted_body_count'] != 0
            or type(receipt.get('metadata_rows')) is not int or receipt['metadata_rows'] < 0):
        raise ValueError('receipt_identity_or_hash_mismatch')
    info, posts, links = parse_index(Response(url, 200, {'content-type': 'application/json'}, body), source)
    if receipt['metadata_rows'] != len(posts):
        raise ValueError('receipt_row_count_mismatch')
    if ('blank_title_metadata_rows' in receipt
            and (type(receipt['blank_title_metadata_rows']) is not int
                 or receipt['blank_title_metadata_rows'] != info['blank_title_metadata_rows'])):
        raise ValueError('receipt_blank_title_count_mismatch')
    if ('index_data_contract_version' in receipt
            and (type(receipt['index_data_contract_version']) is not int
                 or receipt['index_data_contract_version'] != 2)):
        raise ValueError('receipt_version_unrecognized')
    if not references:
        return 'valid_unindexed_original'
    if len(references) != 1:
        raise ValueError('multiple_observations_for_original')
    row = references[0]
    expected = {'scope_url': info['scope'], 'page': info['page'], 'response_sha256': name.group(2),
                'index_fingerprint': fingerprint, 'authorization_fingerprint': original['authorization_fingerprint'],
                'total': info['total'], 'pages': info['pages'], 'ids_sha256': info['ids_sha256'],
                'metadata_rows': len(posts), 'public_ids': len(links), 'evidence_path': relative}
    if any(row[key] != value for key, value in expected.items()):
        raise ValueError('observation_identity_or_count_mismatch')
    observed_clock = _clock(row['source_observed_at_utc'])
    return 'verified_clock_differs' if original_clock != observed_clock else 'verified_clock_matches'


def is_index_intent(row):
    path = row['parquet_path']
    return (isinstance(path, str) and row['receipt_path'] == path
            and re.fullmatch(r'cnyes_index_intents/[0-9a-f]{32}\.json', path) is not None)


def parse_witness(raw):
    if not isinstance(raw, str) or len(raw.encode()) > 32768:
        raise ValueError('invalid_index_checkpoint_witness')
    value = json.loads(raw)
    if (not isinstance(value, dict) or set(value) != {'version', 'head', 'headers', 'response_sha256',
            'response_bytes', 'observed_at_utc', 'request_started_at_utc', 'index_fingerprint',
            'authorization_fingerprint', 'revision_round', 'revision_phase'}
            or type(value['version']) is not int or value['version'] != VERSION
            or not isinstance(value['head'], dict) or set(value['head']) != set(HEAD_FIELDS)
            or not isinstance(value['headers'], dict) or set(value['headers']) != set(HEADER_FIELDS)):
        raise ValueError('invalid_index_checkpoint_witness')
    head = value['head']
    texts = set(HEAD_FIELDS) - {'due', 'attempts', 'priority'}
    if (head['kind'] != 'index' or head['provider'] != 'cnyes' or not isinstance(head['url'], str)
            or any(v is not None and (not isinstance(v, str) or len(v) > 8192)
                   for k, v in head.items() if k in texts)
            or type(head['attempts']) is not int or head['attempts'] < 0 or type(head['priority']) is not int
            or type(head['due']) not in (int, float) or not math.isfinite(head['due'])
            or type(value['response_bytes']) is not int or not 0 <= value['response_bytes'] <= MAX_BODY_BYTES
            or any(v is not None and (not isinstance(v, str) or len(v) > 4096) for v in value['headers'].values())
            or any(not isinstance(value[k], str) or not re.fullmatch(r'[0-9a-f]{64}', value[k])
                   for k in ('response_sha256', 'index_fingerprint', 'authorization_fingerprint'))):
        raise ValueError('invalid_index_checkpoint_witness')
    if ((value['revision_round'] is None) != (value['revision_phase'] is None)
            or value['revision_round'] is not None and (
                not isinstance(value['revision_round'], str) or not re.fullmatch(r'[0-9a-f]{64}', value['revision_round'])
                or value['revision_phase'] not in {'anchor', 'pages', 'verify'})):
        raise ValueError('invalid_index_checkpoint_revision')
    observed = _clock(value['observed_at_utc'])
    started = value['request_started_at_utc']
    if started is not None and _clock(started) > observed:
        raise ValueError('invalid_index_checkpoint_order')
    if (head['due'] > observed.timestamp() or head['last_checked_at_utc'] is not None
            and _clock(head['last_checked_at_utc']) >= observed):
        raise ValueError('invalid_index_checkpoint_head_order')
    return value


def _started(task, observed):
    value = task.get('_discovery_request_started_at_utc')
    try:
        return value if value is not None and _clock(value) <= _clock(observed) else None
    except (ValueError, TypeError, OverflowError):
        return None


def register(queue, task, response, source, observed):
    if (any(key not in task for key in (*HEAD_FIELDS, '_cnyes_revision_round', '_cnyes_revision_phase'))
            or task['url'] != response.url or response.status != 200 or not isinstance(response.body, bytes)
            or len(response.body) > MAX_BODY_BYTES):
        return None
    value = {'version': VERSION, 'head': {key: task[key] for key in HEAD_FIELDS},
        'headers': {key: response.headers.get(key) for key in HEADER_FIELDS},
        'response_sha256': hashlib.sha256(response.body).hexdigest(), 'response_bytes': len(response.body),
        'observed_at_utc': observed, 'request_started_at_utc': _started(task, observed),
        'index_fingerprint': source.cnyes_index_data_fingerprint,
        'authorization_fingerprint': source.authorization_fingerprint,
        'revision_round': task['_cnyes_revision_round'], 'revision_phase': task['_cnyes_revision_phase']}
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        parse_witness(raw)
    except (ValueError, TypeError, KeyError, OverflowError):
        return None
    path = PREFIX + uuid.uuid4().hex + '.json'
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_interrupted_page_checkpoints '
                         '(parquet_path,receipt_path,request_url,response_sha256,witness_json,state) '
                         "VALUES (?,?,?,?,?,'pending')", (path, path, task['url'], value['response_sha256'], raw))
    return path


def defer_live_failure(queue, task, response, error):
    if (not isinstance(error, (OSError, sqlite3.Error)) or not isinstance(response, Response)
            or response.status != 200 or response.url != task.get('url') or not isinstance(response.body, bytes)):
        return False
    source = queue.registry.sources.get('cnyes')
    if source is None or not source.allowed:
        return False
    digest = hashlib.sha256(response.body).hexdigest()
    for row in queue.db.execute("SELECT * FROM cnyes_interrupted_page_checkpoints "
                                "WHERE request_url=? AND response_sha256=? AND state='pending'",
                                (task['url'], digest)):
        if not is_index_intent(row):
            continue
        try:
            value = parse_witness(row['witness_json'])
            if (value['head'] != {key: task[key] for key in HEAD_FIELDS}
                    or value['headers'] != {key: response.headers.get(key) for key in HEADER_FIELDS}
                    or value['response_bytes'] != len(response.body)
                    or value['index_fingerprint'] != source.cnyes_index_data_fingerprint
                    or value['authorization_fingerprint'] != source.authorization_fingerprint
                    or value['request_started_at_utc'] != _started(task, value['observed_at_utc'])
                    or (value['revision_round'], value['revision_phase']) !=
                       (task['_cnyes_revision_round'], task['_cnyes_revision_phase'])):
                continue
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
        current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        if current is None or any(current[key] != value['head'][key] for key in HEAD_FIELDS):
            return False
        with queue.db:
            queue.db.execute("UPDATE cnyes_interrupted_page_checkpoints SET outcome=? "
                             "WHERE parquet_path=? AND state='pending'", ('live_commit_' + type(error).__name__, row['parquet_path']))
        queue.metrics.count('cnyes', 'public_index_local_commit_failures')
        return True
    return False


def recover_one(queue, row):
    from downloader.news_fulltext_cnyes_index import accept_index, parse_index, validate_request
    value = parse_witness(row['witness_json'])
    head, source = value['head'], queue.registry.sources.get('cnyes')
    if head['url'] != row['request_url'] or value['response_sha256'] != row['response_sha256']:
        raise ValueError('index_checkpoint_identity_mismatch')
    if (source is None or not source.allowed or source.cnyes_index_data_fingerprint != value['index_fingerprint']
            or source.authorization_fingerprint != value['authorization_fingerprint']):
        return 'policy_changed'
    try:
        validate_request(head['url'], source)
    except NewsError:
        return 'policy_changed'
    digest, url = value['response_sha256'], head['url']
    relative = ('list_metadata/cnyes/' + value['index_fingerprint'] + '/'
                + hashlib.sha256(url.encode()).hexdigest() + '-' + digest + '.parquet')
    path = replay_source_path(queue.root, relative)
    before = path.stat()
    if before.st_size > MAX_FILE_BYTES:
        raise ValueError('index_original_too_large')
    try:
        parquet = pq.ParquetFile(path)
        columns = {'request_url', 'response_sha256', 'raw_response_body', 'source_observed_at_utc',
                   'index_fingerprint', 'authorization_fingerprint', 'metadata_only', 'accepted_body_count', 'publish'}
        if (parquet.metadata.num_rows != 1 or set(parquet.schema_arrow.names) != columns
                or sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
                   > MAX_UNCOMPRESSED_BYTES):
            raise ValueError('invalid_index_original_schema')
        original = parquet.read().to_pylist()[0]
    except pa.ArrowException as error:
        raise ValueError('invalid_index_original_parquet') from error
    body = original['raw_response_body']
    if (not isinstance(body, bytes) or len(body) != value['response_bytes']
            or hashlib.sha256(body).hexdigest() != digest or original['request_url'] != url
            or original['response_sha256'] != digest or original['index_fingerprint'] != value['index_fingerprint']
            or original['authorization_fingerprint'] != value['authorization_fingerprint']
            or original['metadata_only'] is not True or type(original['accepted_body_count']) is not int
            or original['accepted_body_count'] != 0
            or original['publish'] is not False):
        raise ValueError('invalid_index_original_identity')
    _clock(original['source_observed_at_utc'])
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise OSError('index_original_changed_during_read')
    response = Response(url, 200, {key: val for key, val in value['headers'].items() if val is not None}, body)
    links = parse_index(response, source)[2]
    task = dict(head, _discovery_request_started_at_utc=value['request_started_at_utc'],
                _cnyes_revision_round=value['revision_round'], _cnyes_revision_phase=value['revision_phase'],
                _cnyes_revision_scope_idle=False)
    accept_index(queue, task, response, links, source, _checkpoint_id=row['parquet_path'],
                 _observed=value['observed_at_utc'])
    return 'recovered'
