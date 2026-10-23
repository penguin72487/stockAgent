"""One missing-body recheck from an immutable, successful current native witness.

An actual source response can recover without a parser change. Never invent a
new parser fingerprint, replay a closed metadata question, or clear the first
repair marker to obtain that recheck. The regular HTTP owner and full accepting
validator still own the next request; this evidence is not an accepted body.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

VERSION = 5
KIND = 'reviewed_native_success_missing_body_recheck'
OUTCOME = 'native_success_recheck_enqueued'
_HASH = re.compile(r'[0-9a-f]{64}')


def validate_reference(value):
    if (not isinstance(value, dict) or set(value) != {'request_id', 'result_sha256'}
            or any(not isinstance(v, str) or not _HASH.fullmatch(v) for v in value.values())):
        raise ValueError('invalid_native_success_reference')


def reference(root, request_id):
    """Read a bounded immutable result reference; admission re-verifies it."""
    validate_reference({'request_id': request_id, 'result_sha256': '0' * 64})
    with closing(sqlite3.connect((Path(root).resolve() / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
        row = db.execute('SELECT result_json FROM native_candidate_jobs WHERE request_id=?', (request_id,)).fetchone()
        if not row or not isinstance(row[0], str) or not 0 < len(row[0]) <= 131072:
            raise ValueError('completed_native_success_required')
        return {'request_id': request_id, 'result_sha256': hashlib.sha256(row[0].encode()).hexdigest()}


def verify(db, source, row, value):
    """Exact same failed head, source, successful fixed question and clocks.

    Only the reviewed modern UpMedia question is covered. A similar title,
    canonical URL, later clock, changed parser or other provider is not proof.
    """
    from downloader.news_fulltext_candidate_diagnostics import _key
    from downloader.news_fulltext_queue import parser_repair_identity
    from downloader.news_fulltext_repairs import _bytes, _clock, _head, _unique
    from downloader.news_fulltext_upmedia import UPMEDIA_BOUNDARY_QUESTION, UPMEDIA_LAYOUT_V2, upmedia_evidence_source
    validate_reference(value)
    if (not upmedia_evidence_source(source, layout=UPMEDIA_LAYOUT_V2)
            or row['state'] not in {'article_identity_not_verified', 'article_attribution_not_verified'}
            or not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_candidate_jobs'").fetchone()):
        raise ValueError('completed_native_success_required')
    job = db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (value['request_id'],)).fetchone()
    if (not job or job['state'] != 'observed' or job['provider'] != source.provider
            or any(not isinstance(job[k], str) or not 0 < len(job[k]) <= 131072 for k in ('request_json', 'result_json'))
            or hashlib.sha256(job['result_json'].encode()).hexdigest() != value['result_sha256']):
        raise ValueError('completed_native_success_required')
    request = json.loads(job['request_json'], object_pairs_hook=_unique)
    facts = json.loads(job['result_json'], object_pairs_hook=_unique)
    capture_identity = parser_repair_identity(source)
    if capture_identity is None:
        raise ValueError('native_success_not_verified')
    identity = dict(capture_identity, authorization_fingerprint=source.authorization_fingerprint)
    if (not isinstance(request, dict) or not isinstance(facts, dict)
            or hashlib.sha256(_bytes(request)).hexdigest() != value['request_id']
            or request.get('contract_version') != 20 or request.get('kind') != 'reviewed_native_candidate_metadata'
            or type(request.get('evidence_contract_version')) is not int or request['evidence_contract_version'] != 20
            or request.get('target') != _head(row) or request.get('source') != identity
            or request.get('candidate_layout') != UPMEDIA_LAYOUT_V2
            or request.get('candidate_fingerprint') != source.data_fingerprint
            or job['observation_key'] != _key(_head(row), source.data_fingerprint, evidence_contract_version=20)
            or facts.get('evidence_contract_version') != 20 or facts.get('question') != UPMEDIA_BOUNDARY_QUESTION
            or facts.get('contract_version') != 1 or facts.get('current_source_not_candidate') is not True
            or facts.get('provider') != source.provider or facts.get('requested_url') != row['url']
            or facts.get('final_url') != row['url'] or type(facts.get('status')) is not int or facts['status'] != 200
            or type(facts.get('redirect_count')) is not int or facts['redirect_count'] != 0
            or type(facts.get('response_bytes')) is not int or not 0 < facts['response_bytes'] <= 2 << 20
            or not isinstance(facts.get('response_sha256'), str) or not _HASH.fullmatch(facts['response_sha256'])
            or 'current_native_error' not in facts or facts['current_native_error'] is not None
            or 'failure_location' in facts
            or any(facts.get(k) is not False for k in ('accepted_as_article', 'body_completeness_verified',
                'raw_or_names_or_body_retained', 'identity_or_alias_accepted'))):
        raise ValueError('native_success_not_verified')
    native = facts.get('upmedia_native_boundary')
    if (not isinstance(native, dict) or native.get('contract_version') != 2
            or any(type(native.get(k)) is not int or native[k] != 1 for k in
                ('scope_nodes', 'editor_nodes', 'heading_nodes', 'schema_article_nodes'))
            or any(native.get(k) is not True for k in ('headline_matches_visible', 'schema_author_matches_visible',
                'schema_author_is_person', 'schema_profile_matches', 'head_profile_matches'))
            or native.get('unsupported_editor_tags') != []
            or any(native.get(k) is not False for k in ('raw_response_retained', 'names_retained', 'article_prose_retained'))):
        raise ValueError('native_success_not_verified')
    if not (request.get('created_at_utc') == job['created_at_utc']
            and _clock(row['last_checked_at_utc']) <= _clock(job['created_at_utc'])
            <= _clock(job['reserved_at_utc']) <= _clock(job['completed_at_utc']) <= time.time()):
        raise ValueError('native_success_clock_not_verified')
    # This is recovery AFTER the original once-per-parser repair, not an
    # alternative first-repair path or permission to erase that old marker.
    if (not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reviewed_repair_items'").fetchone()
            or not db.execute("SELECT 1 FROM reviewed_repair_items WHERE provider=? AND parser_fingerprint=? "
                "AND article_id=? AND outcome='enqueued'", (source.provider, identity['parser_fingerprint'], row['id'])).fetchone()):
        raise ValueError('prior_current_parser_repair_required')
