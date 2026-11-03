"""Plan unlisted CNYES disclosure candidates from already verified local audits.

This is an offline research worklist, not a downloader or a queue submission.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import unicodedata
from zoneinfo import ZoneInfo

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.parquet_integrity import signature


TAIPEI = ZoneInfo('Asia/Taipei')
SHA256 = re.compile(r'[a-f0-9]{64}')
ARTICLE = re.compile(r'(?:證券交易法|證交法)(?:第)?(?:43|四十三)(?:條)?(?:之|-)(?:1(?!\d)|一(?![〇零一二三四五六七八九十百千]))')


def is_43_1_candidate(title):
    """Recognize explicit title variants; retain the source title unchanged."""
    if not isinstance(title, str) or len(title) > 4096:
        return False
    normalized = re.sub(r'\s+', '', unicodedata.normalize('NFKC', title))
    return bool(ARTICLE.search(normalized))


def valid_request_scoped_provenance(observations, selected_sha, selected_path, *,
                                    selected_receipt=None, selected_request=None, require_details=False):
    """Keep independently captured equal response bytes and clocks distinct."""
    if (not isinstance(observations, list) or not observations
            or not isinstance(selected_sha, str) or not SHA256.fullmatch(selected_sha)
            or not isinstance(selected_path, str)):
        return False
    detailed = require_details or selected_receipt is not None or selected_request is not None
    if detailed and (not isinstance(selected_receipt, str) or not isinstance(selected_request, str)
                     or not SHA256.fullmatch(selected_request)):
        return False
    keys = set()
    for item in observations:
        if not isinstance(item, dict):
            return False
        sha, path, kind, clock = (item.get('source_response_sha256'), item.get('source_artifact_path'),
                                  item.get('source_artifact_kind'), item.get('source_observed_at_utc'))
        receipt, request = item.get('source_receipt_path'), item.get('source_request_sha256')
        if (not isinstance(sha, str) or not SHA256.fullmatch(sha) or not isinstance(path, str)
                or Path(path).is_absolute() or '..' in Path(path).parts or not isinstance(clock, str)
                or not (kind == 'canonical_quarantine' and path == f'quarantine/{sha}.parquet'
                        or kind == 'cnyes_history_source_envelope'
                        and path.startswith('discovery_responses/') and path.endswith('.parquet')
                        or detailed and kind == 'accepted_body_batch'
                        and path.startswith('bodies/') and path.endswith('.parquet'))):
            return False
        if detailed and (not isinstance(receipt, str) or Path(receipt).is_absolute()
                or '..' in Path(receipt).parts or not isinstance(request, str) or not SHA256.fullmatch(request)
                or kind == 'accepted_body_batch' and not (receipt.startswith('receipts/') and receipt.endswith('.json'))
                or kind != 'accepted_body_batch' and receipt != path + '.receipt.json'):
            return False
        try:
            if datetime.fromisoformat(clock).tzinfo is None:
                return False
        except ValueError:
            return False
        key = (sha, path, kind, receipt, request) if detailed else (sha, path, kind)
        if key in keys:
            return False
        keys.add(key)
    return any(key[0] == selected_sha and key[1] == selected_path
               and (not detailed or key[3:] == (selected_receipt, selected_request)) for key in keys)


def plan(report, prior):
    rows, counts = report.get('rows'), report.get('counts', {})
    inputs = report.get('input_audit_sha256')
    contract = report.get('contract')
    if (contract not in {'cnyes_finlab_issuer_overlap_v1', 'cnyes_finlab_issuer_overlap_v2',
                         'cnyes_finlab_issuer_overlap_v3'}
            or report.get('accepted') is not True
            or report.get('source_failures') != [] or counts.get('source_envelopes_failed') != 0
            or counts.get('changed_head_pairs', 0) or counts.get('unverified_post_identities') != 0
            or not isinstance(rows, list) or len(rows) != counts.get('novel_post_identities')
            or len(rows) != counts.get('verified_post_identities')
            or Counter(row.get('match_state') for row in rows) != Counter(counts.get('match_states', {}))):
        raise ValueError('issuer_audit_incomplete_or_inconsistent')
    if (prior.get('contract_version') != 1 or not isinstance(prior.get('groups'), list)
            or len(prior['groups']) != prior.get('candidate_days')
            or not isinstance(inputs, list) or len(inputs) != 2
            or prior.get('source_audit_sha256') != inputs[1]):
        raise ValueError('prior_worklist_identity_mismatch')
    old_rows = [item for group in prior['groups'] for item in group['candidates']]
    old_ids = {item['cnyes_news_id'] for item in old_rows}
    if (len(old_ids) != len(old_rows) or len(old_rows) != prior.get('candidate_heads')
            or any(type(number) is not int or number <= 0 for number in old_ids)):
        raise ValueError('prior_worklist_duplicate_or_invalid_head')
    ids = [row.get('news_id') for row in rows]
    if len(set(ids)) != len(ids) or any(type(number) is not int or number <= 0 for number in ids):
        raise ValueError('issuer_audit_duplicate_or_invalid_head')

    groups, skipped_old = defaultdict(list), 0
    for row in rows:
        if row['match_state'] != 'no_local_match' or not is_43_1_candidate(row.get('literal_subject')):
            continue
        news_id, sha = row['news_id'], row.get('source_post_sha256')
        observations, selected = row.get('source_observations'), row.get('selected_response_sha256')
        epoch, day, symbol = row.get('source_publish_epoch'), row.get('publisher_day_taipei'), row.get('symbol_candidate')
        if contract == 'cnyes_finlab_issuer_overlap_v1':
            provenance_valid = (isinstance(observations, dict) and bool(observations)
                and isinstance(selected, str) and selected in observations
                and all(isinstance(k, str) and SHA256.fullmatch(k) and isinstance(v, str)
                        for k, v in observations.items()))
        else:
            provenance_valid = valid_request_scoped_provenance(
                observations, selected, row.get('selected_source_artifact_path'),
                **({'selected_receipt': row.get('selected_source_receipt_path'),
                    'selected_request': row.get('selected_source_request_sha256'), 'require_details': True}
                   if contract == 'cnyes_finlab_issuer_overlap_v3' else {}))
        if (not isinstance(sha, str) or not SHA256.fullmatch(sha)
                or not provenance_valid
                or type(epoch) is not int or not isinstance(day, str) or not isinstance(symbol, str) or not symbol
                or row.get('local_record_candidates') != []):
            raise ValueError('invalid_candidate_provenance')
        try:
            actual_day = datetime.fromtimestamp(epoch, TAIPEI).date().isoformat()
        except (OSError, OverflowError, ValueError) as error:
            raise ValueError('invalid_candidate_epoch') from error
        if actual_day != day:
            raise ValueError('candidate_day_does_not_match_source_epoch')
        if news_id in old_ids:
            skipped_old += 1
            continue
        candidate = {'cnyes_news_id': news_id, 'cnyes_url': f'https://news.cnyes.com/news/id/{news_id}',
            'issuer_symbol_candidate': symbol, 'cnyes_subject_literal': row['literal_subject'],
            'cnyes_published_epoch': epoch, 'source_post_sha256': sha, 'selected_response_sha256': selected,
            'source_observations': observations, 'official_record_identity_verified': False,
            'official_release_clock_verified': False, 'body_found_in_audited_cnyes_bulk_post': False}
        if contract in {'cnyes_finlab_issuer_overlap_v2', 'cnyes_finlab_issuer_overlap_v3'}:
            candidate['selected_source_artifact_path'] = row['selected_source_artifact_path']
        if contract == 'cnyes_finlab_issuer_overlap_v3':
            candidate['selected_source_receipt_path'] = row['selected_source_receipt_path']
            candidate['selected_source_request_sha256'] = row['selected_source_request_sha256']
        groups[day].append(candidate)
    ordered = [{'cnyes_publication_day': day, 'candidates': sorted(items, key=lambda item: item['cnyes_news_id'])}
               for day, items in sorted(groups.items())]
    candidates = [item for group in ordered for item in group['candidates']]
    return {'contract': ('cnyes_43_1_incremental_candidates_v3' if contract == 'cnyes_finlab_issuer_overlap_v3'
                         else 'cnyes_43_1_incremental_candidates_v2' if contract == 'cnyes_finlab_issuer_overlap_v2'
                         else 'cnyes_43_1_incremental_candidates_v1'),
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'scope': 'explicit 43-1 title variants among new exact-key no-local-match CNYES posts; not all missing news',
        'candidate_heads': len(candidates), 'candidate_days': len(ordered),
        'unique_issuer_day_candidates': len({(group['cnyes_publication_day'], item['issuer_symbol_candidate'])
                                             for group in ordered for item in group['candidates']}),
        'already_in_prior_worklist': skipped_old, 'groups': ordered,
        'days_are_cnyes_publication_days_not_official_filing_times': True,
        'official_record_identity_verified': False, 'official_historical_access_verified': False,
        'current_queue_state_rechecked': False, 'source_requests': 0, 'production_queue_writes': 0,
        'article_bodies_copied': False, 'historical_gaps_retired': False}


def verify_readback(report, readback, report_sha):
    """Require proof of an exact title join without claiming titles were unread."""
    readback_inputs = readback.get('input_sha256')
    if (not isinstance(readback_inputs, list) or len(readback_inputs) != 4 or readback_inputs[3] != report_sha
            or readback.get('finlab_receipt_sha256') != report.get('finlab_receipt_sha256')
            or readback.get('unique_eligible_identities') != len(report.get('rows', []))
            or readback.get('polars_inner_join_source_rows') != 0
            or readback.get('source_text_columns_read') is not True
            or readback.get('source_title_column_read') is not True
            or readback.get('source_body_info_column_read') is not False
            or readback.get('publisher_http_requests') != 0 or readback.get('production_queue_writes') != 0):
        raise ValueError('independent_readback_mismatch')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('report', 'independent-readback', 'prior-worklist', 'output'):
        parser.add_argument('--' + option, type=Path, required=True)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    if (args.output.is_symlink() or output.exists()
            or not output.is_relative_to((root / 'artifacts/data_quality').resolve())):
        parser.error('output must be a new file under artifacts/data_quality')
    saved, contents = [], []
    for path in (args.report, args.independent_readback, args.prior_worklist):
        before = signature(path)
        if before[2] > 128 << 20:
            parser.error('input oversized')
        raw = path.read_bytes()
        saved.append((path, before, hashlib.sha256(raw).hexdigest()))
        contents.append(json.loads(raw))
    report, readback, prior = contents
    hashes = [entry[2] for entry in saved]
    verify_readback(report, readback, hashes[0])
    result = plan(report, prior)
    if any(signature(path) != before or sha256_file(path) != digest for path, before, digest in saved):
        raise ValueError('input_changed_during_planning')
    result['input_sha256'] = hashes
    result['implementation_sha256'] = sha256_file(Path(__file__).resolve())
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, indent=2) + '\n').encode(), durable=True, mode=0o600)
    print(json.dumps({key: result[key] for key in ('candidate_heads', 'candidate_days',
        'unique_issuer_day_candidates', 'already_in_prior_worklist')}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
