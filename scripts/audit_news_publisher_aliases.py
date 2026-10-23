"""Read-only Pocket redirect/body references, not alias body admission.

Reuse completed canonical transport observations and immutable accepted bodies.
Distinct response hashes/clocks stay distinct. This never fetches, constructs a
queue owner, rewrites a head, copies body rows or grants a new capture scope.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import urljoin, urlsplit

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_pocket import pocket_article_id
from downloader.news_fulltext_policy import extract_article


CONTRACT_VERSION = 1
_SHA = re.compile(r'[0-9a-f]{64}')
_PRIVATE_FLAGS = ('publish', 'historical_point_in_time', 'per_article_license_verified',
                  'commercial_use_enabled', 'training_injection_enabled',
                  'remote_training_export', 'fair_use_legally_determined')


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _clock(value):
    try:
        result = datetime.fromisoformat(value)
        if result.utcoffset() is None or result > datetime.now(UTC):
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ValueError('invalid alias observation clock') from None


def _json(path, limit):
    _require(path.stat().st_size <= limit, 'alias evidence exceeds bound')
    with path.open('rb') as stream:
        payload = stream.read(limit + 1)
    _require(len(payload) <= limit, 'alias evidence grew beyond bound')
    return json.loads(payload), hashlib.sha256(payload).hexdigest()


def _inside(root, relative):
    _require(isinstance(relative, str) and not Path(relative).is_absolute(), 'relative evidence path required')
    path = (root / relative).resolve()
    _require(path.is_relative_to(root), 'alias evidence outside canonical root')
    return path


def _literal(url):
    _require(isinstance(url, str), 'literal publisher URL required')
    parts = urlsplit(url)
    _require(parts.scheme == 'https' and parts.netloc == 'www.pocket.tw'
             and not parts.query and not parts.fragment, 'unreviewed alias URL')
    return url


def _redirect_reference(item, source):
    """Verify the already-observed literal chain, including redaction witnesses."""
    url, result, traces = item['url'], item['result'], item['trace']
    final = _literal(result['final_url'])
    _require(result.get('provider') == 'pocket' and result.get('url') == url
             and result.get('status') == 200 and result.get('article_durably_accepted') is False
             and result.get('source_probe_contract_version') == 2
             and source.article_path_pattern and re.search(source.article_path_pattern, urlsplit(url).path)
             and pocket_article_id(final) is not None, 'unreviewed alias article identity')
    hops = result.get('actual_redirects')
    _require(isinstance(hops, list) and 1 <= len(hops) <= 8
             and isinstance(traces, list) and len(traces) == len(hops) + 1, 'incomplete alias redirect trace')
    previous, prior_clock = _literal(url), None
    for hop, trace in zip(hops, traces):
        _require(hop['from_url'] == previous and hop['status'] in {301, 302, 303, 307, 308}
                 and isinstance(hop['location'], str)
                 and urljoin(previous, hop['location']) == _literal(hop['to_url']), 'broken alias redirect chain')
        # Display URLs may be redacted by the probe. They are literal evidence
        # only if all three original-string digests can be independently matched.
        for key in ('from_url', 'to_url', 'location'):
            _require(hashlib.sha256(hop[key].encode()).hexdigest() == hop[key + '_raw_sha256'],
                     'redacted alias strings are not original evidence')
        observed, traced = _clock(hop['observed_at_utc']), _clock(trace['observed_at_utc'])
        _require((prior_clock is None or observed >= prior_clock)
                 and abs((observed - traced).total_seconds()) <= 1
                 and trace.get('requested_url') == previous and trace.get('status') == hop['status']
                 and trace.get('body_read') is True
                 and isinstance(hop.get('response_sha256'), str) and _SHA.fullmatch(hop['response_sha256'])
                 and type(hop.get('response_bytes')) is int and 0 <= hop['response_bytes'] <= 8 << 20
                 and trace.get('response_sha256') == hop['response_sha256']
                 and trace.get('response_bytes') == hop['response_bytes']
                 and trace.get('headers', {}).get('location') == hop['location'], 'alias hop witness differs')
        previous, prior_clock = hop['to_url'], observed
    trace = traces[-1]
    observed = _clock(trace['observed_at_utc'])
    _require(previous == final == trace.get('requested_url') and trace.get('status') == 200
             and trace.get('body_read') is True and observed >= prior_clock
             and isinstance(result.get('response_sha256'), str) and _SHA.fullmatch(result['response_sha256'])
             and trace.get('response_sha256') == result['response_sha256']
             and type(result.get('bytes')) is int and 0 < result['bytes'] <= 8 << 20
             and trace.get('response_bytes') == result['bytes']
             and _clock(item['question_completed_at_utc']) >= observed, 'alias final witness differs')
    extracted = result.get('extracted', {})
    _require(isinstance(extracted, dict) and isinstance(extracted.get('body_sha256'), str)
             and _SHA.fullmatch(extracted['body_sha256']), 'alias has no witnessed body identity')
    return {'alias_url': url, 'observed_final_url': final, 'redirect_hops': len(hops),
            'alias_response_sha256': result['response_sha256'],
            'redirect_observed_at_utc': trace['observed_at_utc'], 'extracted': extracted}


def audit_pocket_aliases(root, registry, observations, observations_sha256, urls):
    """Return bounded, independently replayed references; zero live writes/HTTP.

    A reference binds one actual observed redirect to one already accepted
    canonical body. It does NOT assert that the alias response raw bytes were
    retained, that both captures were simultaneous, or that the alias head has
    acquired a body. Read-only WAL and immutable receipt hashes define scope.
    """
    root, observations = Path(root).resolve(), Path(observations).resolve()
    _require(isinstance(urls, (list, tuple)) and 1 <= len(urls) <= 16
             and len(set(urls)) == len(urls), 'one to sixteen distinct aliases required')
    urls = sorted(_literal(url) for url in urls)
    source = registry.sources['pocket']
    _require(source.allowed and source.known_url_only
             and source.authorization.get('kind') == 'personal_research', 'unreviewed Pocket reference scope')
    report, report_sha = _json(observations, 8 << 20)
    _require(report_sha == observations_sha256 and report.get('accepted') is True
             and report.get('accepted_as_article') is False and report.get('raw_body_retention') is False
             and report.get('article_head_writes') == 0, 'completed observation proof differs')
    identity = report['source_identities']['pocket']
    _require(isinstance(identity, list) and len(identity) == 4 and all(isinstance(v, str) for v in identity)
             and identity[1:] == [source.authorization_fingerprint, source.http_capacity_source_fingerprint,
                             source.http_capacity_authorization_fingerprint], 'observation source policy differs')
    observations_by_url = defaultdict(list)
    for item in report['observations']:
        observations_by_url[item['url']].append(item)
    references = []
    for url in urls:
        _require(len(observations_by_url[url]) == 1, 'missing or repeated alias observation')
        references.append(_redirect_reference(observations_by_url[url][0], source))
    groups = defaultdict(list)
    with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        heads = list(db.execute('SELECT * FROM articles WHERE provider=? LIMIT 4097', ('pocket',)))
        _require(len(heads) <= 4096, 'Pocket reference frontier exceeds bounded review')
        heads = {head['url']: dict(head) for head in heads}
        completed = {url: h['body_sha256'] for url, h in heads.items() if h['body_sha256']}
        for reference in references:
            url, digest = reference['alias_url'], reference['extracted']['body_sha256']
            head = heads.get(url)
            _require(head is not None and head['state'] == head['error_code'] == 'unsupported_article_path'
                     and not any(head.get(key) for key in ('body_sha256', 'parquet_path', 'captured_at_utc',
                                                         'lease_owner', 'lease_until'))
                     and head['due'] <= datetime.now(UTC).timestamp()
                     and _clock(head['last_checked_at_utc']) <= _clock(reference['redirect_observed_at_utc']),
                     'alias head is no longer the reviewed missing frontier')
            candidates = [h for h in heads.values() if h['body_sha256'] == digest and h['parquet_path']]
            _require(candidates, 'no accepted body for witnessed alias')
            # Prefer the literal canonical head, without inventing one when
            # the existing accepted owner used another actual request spelling.
            candidates.sort(key=lambda h: (h['url'] != reference['observed_final_url'], h['url']))
            groups[candidates[0]['parquet_path']].append((reference, candidates[0]))
        decoded = 0
        parts, verified = [], []
        for relative, selected in sorted(groups.items()):
            path = _inside(root, relative)
            _require(path.stat().st_size <= 64 << 20, 'body reference file exceeds bound')
            accepted = db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
            _require(accepted is not None, 'body reference lacks canonical acceptance')
            receipt_path = _inside(root, accepted['receipt_path'])
            receipt, receipt_sha = _json(receipt_path, 1 << 20)
            parquet_sha = sha256_file(path)
            _require(receipt.get('sha256') == parquet_sha and receipt.get('parquet_path') == relative
                     and receipt.get('publish') is False and receipt.get('historical_point_in_time') is False,
                     'body reference receipt differs')
            parquet = pq.ParquetFile(path)
            logical = sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
            decoded += logical
            _require(parquet.metadata.num_rows == receipt.get('rows') and parquet.metadata.num_rows <= 8192
                     and decoded <= 256 << 20, 'body reference logical grain exceeds bound')
            wanted = {head['url'] for _, head in selected}
            matches = defaultdict(list)
            for batch in parquet.iter_batches(batch_size=8, use_threads=False):
                for record in batch.to_pylist():
                    if record.get('requested_url') in wanted:
                        matches[record['requested_url']].append(record)
            for reference, head in selected:
                _require(len(matches[head['url']]) == 1, 'body reference row is missing or ambiguous')
                record = matches[head['url']][0]
                _require(record['provider'] == 'pocket' and record['article_id'] == head['id'].hex()
                         and record['final_url'] == reference['observed_final_url']
                         and record['body_sha256'] == head['body_sha256']
                         == hashlib.sha256(record['body'].encode()).hexdigest()
                         and record['response_sha256'] == hashlib.sha256(record['raw_response_body']).hexdigest()
                         and record['authorization_fingerprint'] == source.authorization_fingerprint
                         and json.loads(record['authorization_json']) == source.authorization
                         and all(record.get(key) is False for key in _PRIVATE_FLAGS), 'body reference identity differs')
                _clock(record['captured_at_utc'])
                outputs = [extract_article(record['raw_response_body'], replace(source, html_parser=engine),
                                           page_url=record['final_url']) for engine in ('lxml', 'html.parser')]
                _require(outputs[0] == outputs[1], 'body reference parser outputs differ')
                _require(all(record.get(key) == value for key, value in outputs[0].items()),
                         'retained native extraction no longer matches its accepted row')
                for key in ('body_sha256', 'title', 'authors_json', 'published_at_raw'):
                    _require(outputs[0].get(key) == record.get(key) == reference['extracted'].get(key),
                             'observed alias and retained native body differ')
                verified.append({key: value for key, value in reference.items() if key != 'extracted'} | {
                    'existing_requested_url': head['url'], 'existing_final_url': record['final_url'],
                    'existing_body_sha256': record['body_sha256'], 'existing_response_sha256': record['response_sha256'],
                    'existing_body_captured_at_utc': record['captured_at_utc'],
                    'existing_parquet_path': relative, 'existing_receipt_path': accepted['receipt_path'],
                    'raw_responses_identical': record['response_sha256'] == reference['alias_response_sha256'],
                    'native_body_replayed_both_engines': True, 'alias_body_admitted': False})
            parts.append({'parquet_path': relative, 'parquet_sha256': parquet_sha,
                          'receipt_path': accepted['receipt_path'], 'receipt_sha256': receipt_sha,
                          'physical_parquet_decodes': 1})
    return {'contract_version': CONTRACT_VERSION, 'scope_kind': 'observed_redirect_existing_body_reference_audit',
            'accepted': True, 'verified_at_utc': datetime.now(UTC).isoformat(), 'provider': 'pocket',
            'observation_report': str(observations), 'observation_report_sha256': report_sha,
            'source_data_fingerprint': source.data_fingerprint,
            'authorization_fingerprint': source.authorization_fingerprint,
            'completed_body_heads': completed, 'known_urls': len(heads), 'stored_body_urls': len(completed),
            'verified_alias_references': sorted(verified, key=lambda row: row['alias_url']),
            'physical_parts': parts, 'decoded_logical_bytes_upper_bound': decoded,
            'source_requests': 0, 'queue_writes': 0, 'new_article_bodies': 0,
            'formal_queue_alias_admission': False, 'raw_alias_response_retained': False,
            'publish': False, 'remote_training_export': False, 'all_history_complete': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--observations', type=Path, required=True)
    parser.add_argument('--observations-sha256', required=True)
    parser.add_argument('--url', action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    _require(not args.output.exists(), 'reuse completed audit; do not overwrite its original evidence')
    config, registry, repo = load_config(args.config)
    result = audit_pocket_aliases(repo / config['root'], registry, args.observations, args.observations_sha256, args.url)
    _require(not args.output.exists(), 'alias audit output appeared during verification')
    atomic_write_bytes(args.output, (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode(),
                       durable=True, mode=0o600)
    print(json.dumps({'accepted': True, 'verified_references': len(result['verified_alias_references']),
                      'physical_parquet_decodes': len(result['physical_parts']), 'source_requests': 0,
                      'queue_writes': 0, 'new_article_bodies': 0, 'formal_queue_alias_admission': False}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
