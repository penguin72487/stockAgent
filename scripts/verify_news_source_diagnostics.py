"""Verify reviewed private responses against source-scoped canonical parsers.

No HTTP or queue writes. Diagnostic acceptance is not durable article acceptance,
training permission, full-site completeness or a provider rate-limit proof.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, wordpress_base
from downloader.news_fulltext_http import Response
from types import SimpleNamespace
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import utc_now
from stockagent.data.news_source_catalog import configuration_paths


def cnyes_empty_item_outcome(post, error):
    """Recognize the observed empty publisher record, not arbitrary bad fields.

    This is diagnostic evidence only. It creates no body/queue state and does
    not waive missing IDs, dates, permissions or nonempty invalid values.
    """
    return (error.get('error') == 'invalid_bulk_post_fields'
            and type(post.get('newsId')) is int and post['newsId'] > 0
            and error.get('url') == f"https://news.cnyes.com/news/id/{post['newsId']}"
            and type(post.get('payment')) is int and post['payment'] == 0
            and type(post.get('publishAt')) is int
            and post.get('title') == '' and post.get('content') == ''
            and post.get('video') == '')


def verify(probes, registry):
    results = []
    for probe in probes:
        item = {k: probe.get(k) for k in ('provider', 'url', 'status', 'error', 'extraction_error', 'response_sha256')}
        path = probe.get('private_diagnostic_path') or probe.get('diagnostic_response_path')
        if not path:
            item.update(parser_verified=False, source_outcome_preserved=True)
            if probe.get('extraction_error'):
                item.update(source_outcome=probe['extraction_error'],
                            diagnostic_outcome='not_retained_under_source_body_scope; no raw integrity proof')
            results.append(item)
            continue
        try:
            path = Path(path)
            receipt = json.loads(path.with_suffix('.receipt.json').read_text())
            if (sha256_file(path) != receipt.get('sha256', receipt.get('parquet_sha256'))
                    or not receipt['diagnostic_only'] or receipt['accepted_as_article'] or receipt['publish']):
                raise ValueError('diagnostic_receipt_invalid')
            table = pq.read_table(path)
            if table.num_rows != 1:
                raise ValueError('diagnostic_grain_invalid')
            raw = table.to_pylist()[0]
            if (raw['provider'] != probe['provider'] or raw.get('request_url', raw.get('url')) != probe['url']
                    or hashlib.sha256(raw['raw_response_body']).hexdigest() != probe['response_sha256']
                    or raw['response_sha256'] != probe['response_sha256']):
                raise ValueError('diagnostic_source_identity_invalid')
            source = registry.require(raw['final_url'])
            item['diagnostic_integrity_verified'] = True
            if source.provider == 'cnyes' and wordpress_base(raw['final_url']) in source.cnyes_apis:
                response = Response(raw['final_url'], probe['status'], {'content-type': 'application/json'}, raw['raw_response_body'])
                page, posts = cnyes_pagination(response)
                if any(type(post.get('payment')) is not int or post['payment'] != 0 for post in posts):
                    raise ValueError('diagnostic_contains_restricted_bulk_row')
                rows, errors = cnyes_records(response, SimpleNamespace(registry=registry), source)
                valid_errors = {'bulk_body_not_provided', 'bulk_exact_duplicate_row'}
                empty_rows = [e for e in errors if type(e.get('item_number')) is int
                              and 0 <= e['item_number'] < len(posts)
                              and cnyes_empty_item_outcome(posts[e['item_number']], e)]
                empty_indices = {e['item_number'] for e in empty_rows}
                item.update(parser_verified=not any(e['error'] not in valid_errors
                            and e.get('item_number') not in empty_indices for e in errors),
                            publisher_pagination=page, validated_body_rows=len(rows),
                            verified_source_empty_rows=[{'url': e['url'], 'source_outcome': 'empty_publisher_title_and_content',
                                                        'accepted_as_article': False} for e in empty_rows],
                            short_complete_bodies=sum(r['short_publisher_content'] for r in rows),
                            metadata_only_html_fallback_rows=sum(e['error'] == 'bulk_body_not_provided' for e in errors),
                            item_error_counts={code: sum(e['error'] == code for e in errors) for code in {e['error'] for e in errors}},
                            attribution_basis='source-declared byline only; API may legitimately leave it unknown')
                results.append(item)
                continue
            if probe.get('status') == 200 and urlsplit(raw['final_url']).path in source.source_not_found_paths:
                item.update(parser_verified=False, source_outcome='source_not_found', source_outcome_verified=True)
                results.append(item)
                continue
            article = extract_article(raw['raw_response_body'], source, page_url=raw['final_url'])
            official_document = (source.provider == 'fed_board' and source.authorization.get('kind') == 'public_domain'
                                 and source.legacy_release_layout in {'fed_table_release_v8', 'fed_table_release_v9', 'fed_table_release_v10'})
            explicit_empty_name = (source.provider == 'cmnews' and source.embedded_article_layout == 'cmnews_visible_article_v1'
                                   and article['attribution_status'] == 'source_visible_byline_name_empty'
                                   and article['article_identity_basis'] == 'cmnews_canonical_current_uuid_profile_link_and_visible_empty_name_v1'
                                   and article['authors_json'] == '[]')
            if not article['title'] or not official_document and (not article['published_at_raw'] or
                    not json.loads(article['authors_json']) and not explicit_empty_name):
                raise ValueError('diagnostic_attribution_or_clock_missing')
            item.update(parser_verified=True, body_characters=len(article['body']),
                        body_bytes=len(article['body'].encode()),
                        **{k: article[k] for k in ('body_sha256', 'extraction_method', 'authors_json', 'published_at_raw', 'modified_at_raw', 'published_time_basis')},
                        attribution_status=article['attribution_status'],
                        attribution_basis=('official_Board_document; individual_byline_not_invented' if official_document else
                                           'source_visible_profile_link; displayed_author_name_explicitly_empty_not_invented'
                                           if explicit_empty_name else 'source_declared_byline'))
        except (ValueError, KeyError, OSError, NewsError) as error:
            item.update(parser_verified=False, parser_error=error.code if isinstance(error, NewsError) else str(error))
            if (isinstance(error, NewsError) and error.code in {'source_body_image_only', 'source_body_embedded_only', 'source_body_quote_only', 'source_article_test_only', 'source_body_removed', 'source_article_empty_template', 'source_not_found', 'source_document_landing_page', 'paywall_or_login'}
                    and item.get('diagnostic_integrity_verified')):
                # A source mask retained by an older misclassifying parser is
                # evidence of refusal, never accepted article text. New probes
                # do not retain member/paid responses; preserve the existing
                # private evidence and report the corrected outcome only.
                item.update(source_outcome=error.code, source_outcome_verified=True)
        results.append(item)
    parsed = [r for r in results if r.get('response_sha256')]
    return {'accepted': bool(parsed) and all(r['parser_verified'] or r.get('source_outcome_verified') for r in parsed), 'results': results,
            'source_refusals': [r for r in results if r.get('error')],
            'scope': 'private response integrity and parser/source-outcome validation, not durable article or full-site acceptance',
            'all_sites_downloaded': False, 'publish': False, 'training_injection_enabled': False,
            'observed_at_utc': utc_now()}


def verify_queue_outcomes(results, database, registry):
    """Bind reviewed non-body outcomes to current exact-URL queue states.

    This reads one WAL snapshot, not a second owner or a repair/reset path.
    A correct refusal/absence classification is not a saved article. Previously
    saved heads remain visible and are never deleted when a source changes.
    """
    selected = [r for r in results if r.get('source_outcome_verified')]
    rows = []
    db = sqlite3.connect(f'file:{database}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        for result in selected:
            row = db.execute('SELECT provider,state,error_code,body_sha256 FROM articles WHERE url=?',
                             (result['url'],)).fetchone()
            setting = db.execute('SELECT value FROM queue_settings WHERE key=?',
                                 ('source_capture_fingerprint:' + result['provider'],)).fetchone()
            policy_matches = bool(setting and setting[0] == registry.sources[result['provider']].data_fingerprint)
            outcome_matches = bool(row and row['provider'] == result['provider']
                                   and row['state'] == row['error_code'] == result['source_outcome'])
            rows.append({'provider': result['provider'], 'url': result['url'],
                         'expected_source_outcome': result['source_outcome'],
                         'observed_state': row['state'] if row else None,
                         'observed_error_code': row['error_code'] if row else None,
                         'has_previous_stored_body': bool(row and row['body_sha256']),
                         'source_policy_matches': policy_matches,
                         'accepted': outcome_matches and policy_matches})
    finally:
        db.close()
    return {'accepted': bool(rows) and all(r['accepted'] for r in rows),
            'queue_owner_id': owner[0] if owner else None, 'rows': rows,
            'database_snapshot_at_utc': utc_now(), 'read_only': True,
            'scope': 'current exact-URL refusal/absence states and loaded source fingerprint; not article or history acceptance'}


def verify_recorded_nonbody_outcomes(root, urls, registry):
    """Replay immutable non-body approvals and their exact current WAL rows.

    This proves an observed absence/namespace exclusion, not a retained 410
    body or article. A subsequent source/policy/head change invalidates current
    acceptance while all old observations remain immutable evidence.
    """
    from downloader.news_fulltext_cohorts import NONBODY_SCOPE_OUTCOME_CONTRACT_VERSION, review_nonbody_probes
    from downloader.news_fulltext_queue import source_path
    if not 1 <= len(urls) <= 16:
        raise ValueError('bounded exact non-body outcome verification required')
    db = sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    results = []
    try:
        db.execute('BEGIN')
        for url in urls:
            row = db.execute('SELECT o.*,a.url,a.state,a.error_code,a.body_sha256,a.last_checked_at_utc FROM '
                             'finite_source_outcomes o JOIN articles a ON a.id=o.article_id WHERE a.url=?', (url,)).fetchone()
            if row is None:
                raise ValueError('exact URL lacks recorded source outcome')
            path = source_path(root, row['receipt_path'])
            if path.stat().st_size > 65536:
                raise ValueError('non-body approval receipt integrity invalid')
            payload = path.read_bytes()
            if len(payload) > 65536 or hashlib.sha256(payload).hexdigest() != row['receipt_sha256']:
                raise ValueError('non-body approval receipt integrity invalid')
            receipt = json.loads(payload)
            if (receipt.get('contract_version') != NONBODY_SCOPE_OUTCOME_CONTRACT_VERSION
                    or receipt.get('scope_kind') != 'explicit_verified_nonbody_source_outcomes'
                    or receipt.get('original_and_supplement_membership_unchanged') is not True
                    or receipt.get('accepted_article_bodies') != 0 or receipt.get('publish') is not False
                    or receipt.get('all_publisher_history_complete') is not False
                    or receipt.get('observed_at_utc') != row['observed_at_utc']
                    or receipt.get('recorded_at_utc') != row['recorded_at_utc']):
                raise ValueError('non-body approval receipt contract invalid')
            matches = [r for r in receipt['outcomes'] if r.get('url') == url]
            if len(matches) != 1:
                raise ValueError('non-body approval exact membership invalid')
            approval = matches[0]
            probe_path = source_path(root, receipt['probe_path'])
            if probe_path.stat().st_size > 8 << 20:
                raise ValueError('non-body approval original probe integrity invalid')
            probe_payload = probe_path.read_bytes()
            if len(probe_payload) > 8 << 20 or hashlib.sha256(probe_payload).hexdigest() != receipt['probe_sha256']:
                raise ValueError('non-body approval original probe integrity invalid')
            report = json.loads(probe_payload)
            if report.get('observed_at_utc') != row['observed_at_utc']:
                raise ValueError('non-body approval source observation clock changed')
            if approval.get('private_source_path'):
                raw_path = source_path(root, approval['private_source_path'])
                if (sha256_file(raw_path) != approval['private_source_sha256']
                        or sha256_file(raw_path.with_suffix('.receipt.json')) != approval['private_source_receipt_sha256']):
                    raise ValueError('non-body approval immutable original changed')
                for probe in report['probes']:
                    if probe['url'] == url:
                        probe['private_diagnostic_path'] = str(raw_path)
            replay = review_nonbody_probes(registry, report, [url],
                                           diagnostic_root=root / 'capture_cohorts/supplement_sources')[0]
            if (any(replay[key] != approval[key] for key in ('url', 'provider', 'status', 'response_sha256',
                                                            'source_data_fingerprint', 'outcome', 'basis'))
                    or row['provider'] != replay['provider']
                    or row['source_data_fingerprint'] != replay['source_data_fingerprint']
                    or row['article_id'].hex() != approval['article_id']
                    or row['state'] != row['error_code'] or row['state'] != replay['outcome']
                    or row['body_sha256'] is not None or row['last_checked_at_utc'] != row['observed_at_utc']):
                raise ValueError('non-body approval no longer matches exact frontier outcome')
            results.append({'url': url, 'provider': replay['provider'], 'status': replay['status'],
                            'response_sha256': replay['response_sha256'], 'parser_verified': False,
                            'source_outcome': replay['outcome'], 'source_outcome_verified': True,
                            'source_observation_receipt_verified': True,
                            'diagnostic_integrity_verified': bool(approval.get('private_source_path')),
                            'evidence_basis': replay['basis'], 'accepted_as_article': False})
    finally:
        db.close()
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--probes', type=Path, action='append', required=True, help='Latest declared snapshot per request wins; original input reports remain')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--url', action='append', default=[], help='Explicit bounded subset; unselected source outcomes remain in the input report')
    parser.add_argument('--verify-queue-outcomes', action='store_true',
                        help='Read-only exact-URL queue/source-policy check for verified non-body outcomes')
    parser.add_argument('--verify-nonbody-scope-outcomes', action='store_true',
                        help='Replay recorded immutable finite source absence/namespace observations, not article bodies')
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    if len(args.probes) > 4:
        parser.error('at most four bounded diagnostic reports')
    inputs = {str(p): sha256_file(p) for p in [*args.probes, *configuration_paths(args.config)]}
    snapshots = sorted((json.loads(p.read_text()) for p in args.probes), key=lambda s: s['observed_at_utc'])
    latest = {probe['url']: probe for snapshot in snapshots for probe in snapshot['probes']}
    if len(latest) > 64:
        raise ValueError('unbounded_diagnostic_replay')
    if args.url:
        if len(args.url) > 16 or any(url not in latest for url in args.url):
            parser.error('select at most sixteen URLs present in the diagnostic inputs')
        latest = {url: latest[url] for url in dict.fromkeys(args.url)}
    result = verify(list(latest.values()), registry)
    if args.verify_nonbody_scope_outcomes:
        if not args.verify_queue_outcomes:
            parser.error('recorded non-body source verification also requires --verify-queue-outcomes')
        try:
            verified = verify_recorded_nonbody_outcomes(repo / config['root'], list(latest), registry)
            if any(item['response_sha256'] != latest[item['url']].get('response_sha256') for item in verified):
                raise ValueError('selected probe differs from the immutable recorded observation')
            result.update(accepted=True, results=verified,
                          scope='immutable recorded exact-URL absence/namespace outcomes; not article or historical body acceptance')
        except (ValueError, OSError, KeyError, sqlite3.Error, NewsError) as error:
            result.update(accepted=False, nonbody_verification_error=error.code if isinstance(error, NewsError) else str(error))
    result['observation_selection'] = 'latest declared probe snapshot per requested URL; input reports retained'
    result['explicit_selected_urls'] = list(dict.fromkeys(args.url)) if args.url else None
    if args.verify_queue_outcomes:
        result['queue_outcomes'] = verify_queue_outcomes(result['results'], repo / config['root'] / 'queue.sqlite3', registry)
        result['accepted'] = result['accepted'] and result['queue_outcomes']['accepted']
    if inputs != {p: sha256_file(Path(p)) for p in inputs}:
        raise ValueError('diagnostic_configuration_changed_during_validation')
    result['inputs_sha256'] = inputs
    result['implementation_sha256'] = {str(p.relative_to(repo)): sha256_file(p) for p in (
        repo / 'downloader/news_fulltext_policy.py', repo / 'downloader/news_fulltext_publishers.py',
        repo / 'downloader/news_fulltext_taiwan_pages.py',
        repo / 'downloader/news_fulltext_cnews_cmmedia.py',
        repo / 'downloader/news_fulltext_public_originals.py',
        repo / 'downloader/news_fulltext_cohorts.py', repo / 'downloader/news_fulltext_queue.py',
        repo / 'scripts/verify_news_source_diagnostics.py')}
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
