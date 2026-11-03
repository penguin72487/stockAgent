"""Find exact reviewed body versions in a pinned news corpus, without writes.

Only positive matches in checksum-verified corpus parts are proven. Statistics
prune other row groups, whose full files are not hashed, so non-matches remain
unknown rather than proof of absence. This tool neither admits staged reviews
nor changes the source queue or the published corpus.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_repairs import _read_private_request, _source
from downloader.news_fulltext_stored_quality import _head, _native_error, _original, validate_request
from scripts.audit_news_provider_storage_readback import snapshot, stamp
from stockagent.data.news_dataset_sources import _freeze_reviewed_body_quality
from stockagent.data.news_dataset_storage import news_part_path

REPO = Path(__file__).resolve().parent.parent
CONTRACT = 'news_corpus_reviewed_body_positive_audit_v1'
FIELDS = ('article_id', 'provider', 'source_parquet', 'source_sha256', 'observed_at_utc',
          'text_kind', 'document_id', 'body')


def _possible(stats, values):
    """Use parquet statistics only to prune, never to prove a negative."""
    if stats is None or not stats.has_min_max or not isinstance(stats.min, str) or not isinstance(stats.max, str):
        return True
    index = bisect_left(values, stats.min)
    return index < len(values) and values[index] <= stats.max


def scan_pinned_corpus(root: Path, manifest: dict, targets: list[dict]) -> dict:
    """Find exact article/source/capture/text matches and verify positive parts."""
    if manifest.get('state') != 'ready' or manifest.get('contract_version') not in (1, 2):
        raise ValueError('unsupported_or_unready_corpus')
    keyed = {(row['article_id'], row['source_parquet']): row for row in targets}
    if len(keyed) != len(targets) or not targets:
        raise ValueError('empty_or_ambiguous_review_targets')
    ids = pa.array(sorted({row['article_id'] for row in targets}), type=pa.string())
    paths = sorted({row['source_parquet'] for row in targets})
    path_array = pa.array(paths, type=pa.string())
    parts = [part for part in manifest['parts'] if part['table_name'] == 'articles']
    if len({part['path'] for part in parts}) != len(parts):
        raise ValueError('duplicate_corpus_article_part')
    found, stamps, groups_read = {}, {}, 0
    for part in parts:
        path = news_part_path(root, manifest, part)
        before = stamp(path)
        if before is None or before[2] != part['bytes']:
            raise ValueError('corpus_part_size_changed')
        stamps[path] = before
        matched = []
        with pq.ParquetFile(path) as parquet:
            schema = parquet.schema_arrow
            if any(schema.get_field_index(name) < 0 for name in FIELDS):
                raise ValueError('corpus_article_schema_incomplete')
            if parquet.metadata.num_rows != part['rows']:
                raise ValueError('corpus_part_row_count_changed')
            index = schema.get_field_index('source_parquet')
            for group in range(parquet.metadata.num_row_groups):
                stats = parquet.metadata.row_group(group).column(index).statistics
                if not _possible(stats, paths):
                    continue
                groups_read += 1
                key_columns = parquet.read_row_group(group, columns=['article_id', 'source_parquet'], use_threads=False)
                mask = pc.and_(pc.is_in(key_columns['article_id'], value_set=ids),
                               pc.is_in(key_columns['source_parquet'], value_set=path_array))
                positions = pc.indices_nonzero(mask)
                if len(positions) == 0:
                    continue
                selected = parquet.read_row_group(group, columns=list(FIELDS), use_threads=False).take(positions)
                for row in selected.to_pylist():
                    target = keyed.get((row['article_id'], row['source_parquet']))
                    if target is None:
                        continue
                    captured = datetime.fromisoformat(target['captured_at_utc'])
                    if captured.tzinfo is None:
                        raise ValueError('review_capture_time_is_not_aware')
                    if row['observed_at_utc'] != captured:
                        continue  # A different capture is not this review's body version.
                    if (row['provider'] != target['provider'] or row['text_kind'] != 'fulltext'
                            or row['source_sha256'] != target['source_sha256']
                            or row['document_id'] != 'body-' + target['body_sha256']
                            or not isinstance(row['body'], str)
                            or hashlib.sha256(row['body'].encode('utf-8')).hexdigest() != target['body_sha256']):
                        raise ValueError('corpus_reviewed_body_version_mismatch')
                    key = (row['article_id'], row['source_parquet'])
                    if key in found:
                        raise ValueError('duplicate_corpus_reviewed_body_version')
                    found[key] = {'article_id': row['article_id'], 'provider': row['provider'],
                                  'origin': target['origin'], 'document_id': row['document_id'],
                                  'corpus_part': part['path'],
                                  'corpus_part_sha256': part['sha256']}
                    matched.append(key)
        if stamp(path) != before:
            raise ValueError('corpus_part_changed_during_inspection')
        if matched and sha256_file(path) != part['sha256']:
            raise ValueError('corpus_positive_part_checksum_mismatch')
        if matched and stamp(path) != before:
            raise ValueError('corpus_positive_part_changed_during_hash')
    if any(stamp(path) != before for path, before in stamps.items()):
        raise ValueError('corpus_part_changed_after_inspection')
    return {'article_parts_examined': len(parts), 'row_groups_projected': groups_read,
            'positive_part_checksums_verified': len({v['corpus_part'] for v in found.values()}),
            'targets': len(targets), 'confirmed_exact_versions': len(found),
            'unconfirmed_targets': len(targets) - len(found),
            'confirmed_by_origin': dict(Counter(v['origin'] for v in found.values())),
            'confirmed_by_provider': dict(Counter(v['provider'] for v in found.values())),
            'matches': list(found.values()), 'all_corpus_parts_sha_verified': False,
            'negative_matches_prove_absence': False}


def scan_pinned_documents(root: Path, manifest: dict, article_matches: list[dict]) -> dict:
    """Find referenced deduplicated texts; positive parts alone are hashed.

    A document may be referenced by other article versions. Finding the same
    text here does not authorize excluding it from those other versions.
    """
    identifiers = sorted({row['document_id'] for row in article_matches})
    if any(not isinstance(value, str) or re.fullmatch(r'body-[0-9a-f]{64}', value) is None
           for value in identifiers):
        raise ValueError('invalid_reviewed_document_id')
    parts = [part for part in manifest['parts'] if part['table_name'] == 'documents']
    if len({part['path'] for part in parts}) != len(parts):
        raise ValueError('duplicate_corpus_document_part')
    if not identifiers:
        return {'targets': 0, 'confirmed_document_ids': 0, 'unconfirmed_document_ids': 0,
                'document_parts_examined': 0, 'row_groups_projected': 0,
                'positive_part_checksums_verified': 0, 'matches': [],
                'all_corpus_parts_sha_verified': False, 'negative_matches_prove_absence': False}
    identifier_array = pa.array(identifiers, type=pa.string())
    fields = ('document_id', 'text_kind', 'text', 'text_sha256', 'text_bytes', 'dedup_group_id')
    found, stamps, groups_read = {}, {}, 0
    for part in parts:
        path = news_part_path(root, manifest, part)
        before = stamp(path)
        if before is None or before[2] != part['bytes']:
            raise ValueError('corpus_document_part_size_changed')
        stamps[path] = before
        matched = False
        with pq.ParquetFile(path) as parquet:
            schema = parquet.schema_arrow
            if any(schema.get_field_index(name) < 0 for name in fields):
                raise ValueError('corpus_document_schema_incomplete')
            if parquet.metadata.num_rows != part['rows']:
                raise ValueError('corpus_document_row_count_changed')
            index = schema.get_field_index('document_id')
            for group in range(parquet.metadata.num_row_groups):
                stats = parquet.metadata.row_group(group).column(index).statistics
                if not _possible(stats, identifiers):
                    continue
                groups_read += 1
                keys = parquet.read_row_group(group, columns=['document_id'], use_threads=False)
                positions = pc.indices_nonzero(pc.is_in(keys['document_id'], value_set=identifier_array))
                if not len(positions):
                    continue
                selected = parquet.read_row_group(group, columns=list(fields), use_threads=False).take(positions)
                for row in selected.to_pylist():
                    identity, value = row['document_id'], row['text']
                    if identity in found:
                        raise ValueError('duplicate_corpus_reviewed_document')
                    if not isinstance(value, str):
                        raise ValueError('corpus_reviewed_document_not_text')
                    raw = value.encode('utf-8')
                    digest = hashlib.sha256(raw).hexdigest()
                    if (row['text_kind'] != 'fulltext' or identity != 'body-' + digest
                            or row['text_sha256'] != digest or row['dedup_group_id'] != digest
                            or row['text_bytes'] != len(raw)):
                        raise ValueError('corpus_reviewed_document_mismatch')
                    found[identity] = {'document_id': identity, 'corpus_part': part['path'],
                                       'corpus_part_sha256': part['sha256']}
                    matched = True
        if stamp(path) != before:
            raise ValueError('corpus_document_part_changed_during_inspection')
        if matched and sha256_file(path) != part['sha256']:
            raise ValueError('corpus_positive_document_part_checksum_mismatch')
        if matched and stamp(path) != before:
            raise ValueError('corpus_positive_document_part_changed_during_hash')
    if any(stamp(path) != before for path, before in stamps.items()):
        raise ValueError('corpus_document_part_changed_after_inspection')
    return {'targets': len(identifiers), 'confirmed_document_ids': len(found),
            'unconfirmed_document_ids': len(identifiers) - len(found),
            'document_parts_examined': len(parts), 'row_groups_projected': groups_read,
            'positive_part_checksums_verified': len({v['corpus_part'] for v in found.values()}),
            'matches': list(found.values()), 'all_corpus_parts_sha_verified': False,
            'negative_matches_prove_absence': False}


def collect_targets(root: Path, original: sqlite3.Connection, registry, stages: list[Path]):
    """Revalidate owner-admitted and private, not-yet-admitted witnesses."""
    targets, request_ids = [], set()
    with closing(sqlite3.connect(':memory:')) as projection:
        projection.executescript('''CREATE TABLE quality_review_requests(request_id TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
            CREATE TABLE body_quality_exclusions(article_id BLOB NOT NULL,request_id TEXT NOT NULL,
              target_index INTEGER NOT NULL,body_sha256 TEXT NOT NULL,parquet_path TEXT NOT NULL,
              captured_at_utc TEXT NOT NULL,error TEXT NOT NULL,reviewed_at_utc TEXT NOT NULL,
              parser_fingerprint TEXT NOT NULL,PRIMARY KEY(article_id,request_id,target_index)) WITHOUT ROWID;''')
        _freeze_reviewed_body_quality(root, original, projection)
        for article_id, request_id, index in projection.execute(
                'SELECT article_id,request_id,target_index FROM body_quality_exclusions ORDER BY article_id'):
            payload = projection.execute('SELECT payload FROM quality_review_requests WHERE request_id=?', (request_id,)).fetchone()[0]
            request = validate_request(json.loads(payload))
            target = request['targets'][index]
            head = original.execute('SELECT * FROM articles WHERE id=?', (article_id,)).fetchone()
            record, witness = _original(root, original, head)
            if (witness != request['originals'][article_id.hex()]
                    or _native_error(registry.require(head['url']), head, record) != request['expected_error']):
                raise ValueError('owner_review_original_changed')
            targets.append(_target(target, witness, 'owner_admitted'))
            request_ids.add(request_id)
    for directory in stages:
        directory = directory.resolve(strict=True)
        manifest = json.loads((directory / 'manifest.json').read_bytes())
        if manifest.get('submitted') is not False:
            raise ValueError('private_stage_must_be_unsubmitted')
        for item in manifest['staged']:
            request_id = item['request_id']
            request = validate_request(_read_private_request(directory / (request_id + '.json')))
            if request_id in request_ids or item['targets'] != len(request['targets']):
                raise ValueError('duplicate_or_inconsistent_review_request')
            request_ids.add(request_id)
            for target in request['targets']:
                head = original.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(target['id']),)).fetchone()
                if head is None or _head(head) != target:
                    raise ValueError('staged_review_head_changed')
                source, identity = _source(original, registry, head['url'])
                record, witness = _original(root, original, head)
                if (identity != request['sources'][head['provider']]
                        or witness != request['originals'][target['id']]
                        or _native_error(source, head, record) != request['expected_error']):
                    raise ValueError('staged_review_original_changed')
                targets.append(_target(target, witness, 'private_staged'))
    return targets


def _target(target, witness, origin):
    return {'article_id': target['id'], 'provider': target['provider'], 'origin': origin,
            'body_sha256': target['body_sha256'], 'source_parquet': target['parquet_path'],
            'captured_at_utc': target['captured_at_utc'], 'source_sha256': witness['parquet_sha256']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--config', type=Path, default=REPO / 'configs/news_fulltext.json')
    parser.add_argument('--stage', type=Path, action='append', default=[])
    parser.add_argument('--include-documents', action='store_true',
                        help='also inspect deduplicated text rows referenced by confirmed article versions')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source, corpus = args.source.resolve(strict=True), args.corpus.resolve(strict=True)
    output = args.output.resolve()
    if output.is_relative_to(source) or output.is_relative_to(corpus) or output.exists():
        raise ValueError('audit_output_must_be_new_and_outside_data_roots')
    manifest_path = corpus / 'manifest.json'
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest['source_snapshot']['source_root'] != str(source):
        raise ValueError('corpus_belongs_to_another_source')
    _, registry, _ = load_config(args.config)
    owner = os.open(source / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = snapshot(source)
        with closing(sqlite3.connect((source / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            targets = collect_targets(source, db, registry, args.stage)
        result = scan_pinned_corpus(corpus, manifest, targets)
        if args.include_documents:
            result['document_audit'] = scan_pinned_documents(corpus, manifest, result['matches'])
        if snapshot(source) != before or manifest_path.read_bytes() != manifest_bytes:
            raise ValueError('source_or_corpus_manifest_changed')
        result.update(contract=CONTRACT, observed_at_utc=datetime.now(timezone.utc).isoformat(),
            corpus_generation=manifest['generation'], corpus_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            corpus_source_snapshot_utc=manifest['source_snapshot']['captured_at_utc'], source_snapshot=before,
            source_targets_by_origin=dict(Counter(t['origin'] for t in targets)),
            publisher_requests=0, production_queue_writes=0, production_corpus_writes=0,
            scope='exact validated versions only; staged requests remain unadmitted; negative results unproven')
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, sort_keys=True) + '\n').encode(),
                           durable=True, mode=0o600)
        summary = {key: value for key, value in result.items() if key != 'matches'}
        if 'document_audit' in summary:
            summary['document_audit'] = {key: value for key, value in summary['document_audit'].items() if key != 'matches'}
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    finally:
        os.close(owner)


if __name__ == '__main__':
    main()
